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
	@brief Implementation of ChunkManager
*/

#include "chunk_manager.h"
#include "util/type.h"
#include "util/file.h"
#include "util/trace.h"
#include "gs_error.h"
#include <iomanip>
#include <iostream>
#include <sstream>
#include <cfloat>
#ifndef _WIN32
#include <fcntl.h> 
#endif
#include "picojson.h"


UTIL_TRACER_DECLARE(SIZE_MONITOR);
UTIL_TRACER_DECLARE(IO_MONITOR);

const uint32_t ChunkManager::LIMIT_CHUNK_NUM_LIST[12] = {
	64 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024,
	128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024,
	128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024,
	128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024 };

const double ChunkManager::MemoryLimitManager::CALC_EMA_HALF_LIFE_CONSTANT = 2.8854;

/*!
	@brief Constructor of ChunkManager.
*/
ChunkManager::ChunkManager(
		ConfigTable& configTable,
		ChunkCategoryId chunkCategoryNum,
		const ChunkCategoryAttribute* chunkCategoryAttributeList,
		bool isReadOnlyModeFile, bool isCreateModeFile)
: MAX_CHUNK_ID(calcMaxChunkId(
		configTable.get<int32_t>(CONFIG_TABLE_DS_STORE_BLOCK_SIZE))),
  configTable_(configTable),
  config_(configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY),
		  configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM),
		  chunkCategoryNum,
		  configTable.get<int32_t>(CONFIG_TABLE_DS_STORE_BLOCK_SIZE),
		  configTable.get<int32_t>(CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE),
		  configTable.get<bool>(CONFIG_TABLE_DS_STORE_WARM_START),
		  configTable.getUInt32(CONFIG_TABLE_DS_STORE_MEMORY_LIMIT),
		  configTable.getUInt32(CONFIG_TABLE_CP_CHECKPOINT_MEMORY_LIMIT),
		  MAX_ONCE_SWAP_SIZE_BYTE_ /
				configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE),
		  (configTable.get<int32_t>(CONFIG_TABLE_DS_STORE_COMPRESSION_MODE) ==
				  0 ? NO_BLOCK_COMPRESSION : BLOCK_COMPRESSION),
		  configTable,
		  configTable.get<int32_t>(
				CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE),
		  configTable.get<int32_t>(
				CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD)
		  , configTable.get<double>(
				CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE)
		  , configTable.getUInt64(CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE)
		  , configTable.get<bool>(CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE)
		  , configTable.get<double>(
				CONFIG_TABLE_DS_STORE_BUFFER_TABLE_SIZE_RATE)   
		  , configTable.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT)
		),
  storeMemoryPool_(
		util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "txnBlockPool"),
		(1UL << config_.getChunkExpSize())),
  checkpointMemoryPool_(
		util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "cpBlockPool"),
		(1UL << config_.getChunkExpSize())),
  atomicExistPartitionNum_(0),
  memoryLimitManager_(config_, partitionGroupData_, storeMemoryPool_),
  chunkManagerStats_()
  ,
  readOnly_(isReadOnlyModeFile)
{
	GS_TRACE_INFO(
			CHUNK_MANAGER, GS_TRACE_CHM_CONFIG, "Backup function is acceptable. ");

	config_.setUpConfigHandler(configTable);	
	chunkManagerStats_.setManager(*this);

	try {
		storeMemoryPool_.setLimit(util::AllocatorStats::STAT_STABLE_LIMIT,
				config_.getAtomicStoreMemoryLimit());
		checkpointMemoryPool_.setLimit(util::AllocatorStats::STAT_STABLE_LIMIT,
				config_.getAtomicCheckpointMemoryLimit());

		for (ChunkCategoryId categoryId = 0;
			 categoryId < getConfig().getChunkCategoryNum(); categoryId++) {
			categoryAttribute_.push_back(
				chunkCategoryAttributeList[categoryId]);
			if (categoryAttribute_[categoryId].freeMode_ == BATCH_FREE_MODE) {
				batchFreeCategoryId_.push_back(categoryId);
			}
		}

		PartitionGroupConfig pgConfig(
			getConfig().getPartitionNum(), getConfig().getPartitionGroupNum());
		for (PartitionId pId = 0; pId < getConfig().getPartitionNum(); pId++) {
			getConfig().appendPartitionGroupId(
					pgConfig.getPartitionGroupId(pId));
		}
		getConfig().isValid();

		for (PartitionGroupId pgId = 0;
			 pgId < getConfig().getPartitionGroupNum(); pgId++) {
			partitionGroupData_.push_back(
				UTIL_NEW PartitionGroupData(configTable, getConfig(), pgId,
					configTable_.get<const char8_t*>(CONFIG_TABLE_DS_DB_PATH),
					checkpointMemoryPool_, checkpointMemoryPoolMutex_,
					storeMemoryPool_, partitionList_));
			CheckpointFile& checkpointFile =
					partitionGroupData_[pgId]->checkpointFile_;
			checkpointFile.open(isReadOnlyModeFile, isCreateModeFile);
			checkpointFile.setIOWarningThresholdMillis(
					configTable.getUInt32(
							CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_TIME));
			if (checkpointFile.getFileSize() == 0) {
				util::NormalXArray<uint8_t> chunk;
				uint8_t temp = 0;
				chunk.assign(getConfig().getChunkSize(), temp);

				const PartitionId pId = pgConfig.getGroupBeginPartitionId(pgId);
				ChunkHeader::initialize(
						chunk.data(),
						getConfig().getChunkExpSize(), pgId, pId, -1, -1,
						categoryAttribute_[0], UNDEF_CHUNK_KEY,
						getConfig().getPartitionGroupNum(),
						getConfig().getPartitionNum(),
						getConfig().getChunkCategoryNum()
						, getConfig().getCpFileSplitCount()
						, getConfig().getCpFileSplitStripeSize()
					);

				int64_t writePos = 0;
				FileManager fileManager(getConfig(), checkpointFile);
				int64_t compTime; 
				uint32_t holeOffset = fileManager.compressChunk(
						chunk.data(),
						getConfig().getPartitionCompressionMode(pId), compTime);
				fileManager.updateCheckSum(chunk.data());

				uint64_t flushTime; 
				fileManager.writeHolePunchingChunk(
						chunk.data(), writePos, holeOffset, flushTime);
				checkpointFile.flush();
			}
		}

		for (PartitionId pId = 0; pId < getConfig().getPartitionNum(); pId++) {
			partitionList_.push_back(
					UTIL_NEW PartitionData(
							getConfig(), pId,
							getPartitionGroupData(pId).allocator_, this));
		}

		memoryLimitManager_.redistributeMemoryLimit(
				MemoryLimitManager::DISTRIBUTOR_PARTITION_GROUP_ID_);
	}
	catch (std::exception& e) {
		for (PartitionGroupId pgId = 0;
			 pgId < getConfig().getPartitionGroupNum(); pgId++) {
			if (pgId < partitionGroupData_.size()) {
				delete partitionGroupData_[pgId];
				partitionGroupData_[pgId] = NULL;
			}
		}

		for (PartitionId pId = 0; pId < getConfig().getPartitionNum(); pId++) {
			if (pId < partitionList_.size()) {
				delete partitionList_[pId];
				partitionList_[pId] = NULL;
			}
		}

		;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Destructor of ChunkManager.
*/
ChunkManager::~ChunkManager() {
	for (PartitionId pId = 0; pId < getConfig().getPartitionNum(); pId++) {
		if (pId < partitionList_.size()) {
			delete partitionList_[pId];
			partitionList_[pId] = NULL;
		}
	}

	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		if (pgId < partitionGroupData_.size()) {
			partitionGroupData_[pgId]->checkpointFile_.close();
			delete partitionGroupData_[pgId];
			partitionGroupData_[pgId] = NULL;
		}
	}

}


/*!
	@brief Return true if the partition exists.
*/
bool ChunkManager::existPartition(PartitionId pId) {
	assert(isValidId(pId));
	return exist(pId);
}

/*!
	@brief Drop the partition.
*/
void ChunkManager::dropPartition(PartitionId pId) {
	assert(!readOnly_);
	assert(isValidId(pId));

	try {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		BufferManager& bufferManager = getBufferManager(pId);
		ChunkCursor cursor;
		ChunkKey* chunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, chunkKey);
		bool isHead = false;
		while (!isHead) {
			if (!baseMetaChunk->isFree()) {
				int32_t unfixNum = bufferManager.getRefCounter(
						pId, cursor.categoryId_, cursor.cId_);
				for (int32_t i = 0; i < unfixNum; i++) {
					bufferManager.unfix(pId, cursor.categoryId_, cursor.cId_);
				}
				freeChunk(pId, cursor.categoryId_, cursor.cId_);
			}
			baseMetaChunk = metaChunkManager.next(pId, cursor, isHead, chunkKey);
		}

		assert(
			metaChunkManager
					.getEmptyScanCursor(pId, static_cast<ChunkCategoryId>(
									getConfig().getChunkCategoryNum() - 1))
					.cId_ == 0);
		assert(metaChunkManager.getBatchFreeCursor(pId).cId_ == 0);

		getAffinityManager(pId).drop(pId);
		drop(pId);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Unfix all chunks of the partition by reset reffering counter.
*/
void ChunkManager::resetRefCounter(PartitionId pId) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	bufferManager.resetRefCounter();
}

/*!
	@brief Fix the chunk.
*/
void ChunkManager::fix(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	bufferManager.fix(pId, categoryId, cId);
}

/*!
	@brief Unfix the chunk.
*/
void ChunkManager::unfix(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	bufferManager.unfix(pId, categoryId, cId);
}

/*!
	@brief Free the chunk.
*/
void ChunkManager::freeChunk(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(!readOnly_);
	assert(isValidId(pId, categoryId, cId));
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	ChunkKey *metaChunkKey = NULL;
	BaseMetaChunk* baseMetaChunk =
			metaChunkManager.getMetaChunk(pId, categoryId, cId, metaChunkKey);
	assert(!baseMetaChunk->isFree());
	int64_t checkpointPos = baseMetaChunk->getCheckpointPos();

	uint8_t* checkpointBuffer;
	BufferManager& bufferManager = getBufferManager(pId);
	bufferManager.free(pId, categoryId, cId, checkpointPos, checkpointBuffer);

	if (checkpointBuffer) {
		CheckpointManager& checkpointManager = getCheckpointManager(pId);
		checkpointManager.kickChunk(
				pId, categoryId, cId, checkpointBuffer, checkpointPos);
		bufferManager.freeCheckpointBuffer(checkpointBuffer);
	}

	if (metaChunkKey != NULL) {
		*metaChunkKey = UNDEF_CHUNK_KEY;
	}
	baseMetaChunk->resetUnoccupiedSize();
	baseMetaChunk->setAffinityUnused();

	metaChunkManager.free(pId, categoryId, cId);

	GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			getTraceInfo(pId, categoryId, cId).c_str());
	assert(metaChunkManager.isValid(pId, categoryId));
}

/*!
	@brief Return the chunk for updating.
*/
ChunkManager::MetaChunk* ChunkManager::getChunkForUpdate(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		ChunkKey chunkKey, bool doFix) {
	assert(!readOnly_);
	assert(isValidId(pId, categoryId, cId));


	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	ChunkKey *metaChunkKey;
	BaseMetaChunk* baseMetaChunk =
			metaChunkManager.getMetaChunk(pId, categoryId, cId, metaChunkKey);
	assert(!baseMetaChunk->isFree());
	int64_t checkpointPos = baseMetaChunk->getCheckpointPos();

	MetaChunk& metaChunk = getMetaChunk(pId);

	uint8_t* checkpointBuffer;
	BufferManager& bufferManager = getBufferManager(pId);
	uint8_t* buffer = bufferManager.getForUpdate(pId, categoryId, cId,
			checkpointPos, doFix, checkpointBuffer, metaChunk.bufferInfo_);

	if (checkpointBuffer) {
		CheckpointManager& checkpointManager = getCheckpointManager(pId);
		checkpointManager.kickChunk(
				pId, categoryId, cId, checkpointBuffer, checkpointPos);
		assert(buffer == checkpointBuffer);
		checkpointBuffer = NULL;
	}

	if (chunkKey != UNDEF_CHUNK_KEY &&
			metaChunkKey != NULL && *metaChunkKey < chunkKey) {
			assert(isBatchFreeMode(categoryId));
			*metaChunkKey = chunkKey;
			ChunkHeader::setChunkKey(buffer, chunkKey);
	}

	metaChunk.base_ = baseMetaChunk;
	metaChunk.buffer_ = buffer;
	assert(metaChunk.isValid());

	assert(baseMetaChunk->getUnoccupiedSize() != UNDEF_UNOCCUPIED_SIZE_);
	assert(0 <= getMetaChunk(pId).getOccupiedSize());
	assert(buffer && ChunkHeader::getPartitionId(buffer) == pId &&
			ChunkHeader::getChunkCategoryId(buffer) == categoryId &&
			ChunkHeader::getChunkId(buffer) == cId);

	return &metaChunk;
}

/*!
	@brief Return the chunk prohibited updating.
*/
ChunkManager::MetaChunk* ChunkManager::getChunk(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(isValidId(pId, categoryId, cId));

	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	ChunkKey* chunkKey = NULL;
	BaseMetaChunk* baseMetaChunk =
			metaChunkManager.getMetaChunk(pId, categoryId, cId, chunkKey);
	int64_t checkpointPos = baseMetaChunk->getCheckpointPos();

	MetaChunk& metaChunk = getMetaChunk(pId);

	BufferManager& bufferManager = getBufferManager(pId);
	uint8_t* buffer = bufferManager.get(
			pId, categoryId, cId, checkpointPos, metaChunk.bufferInfo_);

	metaChunk.base_ = baseMetaChunk;
	metaChunk.buffer_ = buffer;
	assert(metaChunk.isValid());

	assert(buffer && ChunkHeader::getPartitionId(buffer) == pId &&
			ChunkHeader::getChunkCategoryId(buffer) == categoryId &&
			ChunkHeader::getChunkId(buffer) == cId);

	return &metaChunk;
}

/*!
	@brief Return the chunk prohibited updating.
*/
ChunkManager::MetaChunk* ChunkManager::searchChunk(
		PartitionId pId, ChunkCategoryId chunkCategoryId,
		const DataAffinityInfo &affinityInfo, ChunkKey chunkKey,
		uint8_t powerSize, ChunkId& cId) {
	assert(isValidId(pId, chunkCategoryId));
	BaseMetaChunk* baseMetaChunk = NULL;

	try {
		AffinityManager& affinityManager = getAffinityManager(pId);
		cId = affinityManager.getAffinityChunkId(
				pId, chunkCategoryId, affinityInfo, chunkKey);

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		bool hasAffinity = false;
		if (cId != UNDEF_CHUNKID) {
			hasAffinity = true;
			ChunkKey *metaChunkKey;
			baseMetaChunk = metaChunkManager.getMetaChunk(
					pId, chunkCategoryId, cId, metaChunkKey);
			if (baseMetaChunk->isFree()) {
				ChunkId oldAffinityCId;
				affinityManager.setAffinityChunkId(
						pId, chunkCategoryId,
						affinityInfo, chunkKey, UNDEF_CHUNKID, oldAffinityCId);
				assert(!baseMetaChunk->isAffinityUse());
				assert(oldAffinityCId == cId);  
				cId = UNDEF_CHUNKID;  
			}
			else if (baseMetaChunk->getUnoccupiedSize() < powerSize) {

				cId = UNDEF_CHUNKID;
			}
			else {

				assert(powerSize <= baseMetaChunk->getUnoccupiedSize());
				assert(
					cId < metaChunkManager.getNextTail(pId, chunkCategoryId).cId_);
			}
		}

		if (cId == UNDEF_CHUNKID) {
			uint64_t searchNum = MAX_EMPTY_CHUNK_SEARCH_NUM_;

			const uint8_t BATCH_FREE_MODE_EMPTY_CHUNK_THRESHOLD =
					static_cast<uint8_t>(getConfig().getChunkExpSize() - 2);
			if (isBatchFreeMode(chunkCategoryId)) {
				if (powerSize < BATCH_FREE_MODE_EMPTY_CHUNK_THRESHOLD) {
					powerSize = BATCH_FREE_MODE_EMPTY_CHUNK_THRESHOLD;
				}
			}
			if (hasAffinity && isSmallSizeSearchSkip(chunkCategoryId)) {
				const uint8_t SPACE_SIZE_MODE_EMPTY_CHUNK_THRESHOLD = 10;
				if (powerSize < SPACE_SIZE_MODE_EMPTY_CHUNK_THRESHOLD) {
					searchNum = 0;
				}
			}


			cId = searchEmptyChunk(pId, chunkCategoryId, powerSize, chunkKey, searchNum, affinityInfo);

			ChunkKey *metaChunkKey;
			baseMetaChunk = metaChunkManager.getMetaChunk(
					pId, chunkCategoryId, cId, metaChunkKey);
			assert(cId != UNDEF_CHUNKID);
			assert(cId < metaChunkManager.getNextTail(pId, chunkCategoryId).cId_);

			if (baseMetaChunk->getAffinityId() == UNDEF_AFFINITY_GROUP_ID) {
				baseMetaChunk->setAffinityId(affinityInfo.dataAffinity_);
			}
		}

		if (!baseMetaChunk->isFree()) {
			assert(powerSize <= baseMetaChunk->getUnoccupiedSize());
			return getChunkForUpdate(pId, chunkCategoryId, cId, chunkKey, true);
		}
		else {
			return NULL;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, getTraceInfo(pId, chunkCategoryId, cId) <<
				", affinity(expire)=" << affinityInfo.expireCategory_ <<
				", affinity(update)=" << affinityInfo.dataAffinity_ <<
				", powerSize=" << (int32_t)powerSize <<
				")" << GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Return the chunk which is allocatable the new object of powerSize.
*/
ChunkManager::MetaChunk* ChunkManager::searchChunk(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		ChunkKey chunkKey, uint8_t powerSize) {
	assert(isValidId(pId, categoryId, cId));
	try {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk =
				metaChunkManager.getMetaChunk(pId, categoryId, cId, metaChunkKey);
		if (powerSize <= baseMetaChunk->getUnoccupiedSize()) {
			return getChunkForUpdate(pId, categoryId, cId, chunkKey, true);
		}
		else {
			return NULL;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, getTraceInfo(pId, categoryId, cId)
				<< ",chunkKey," << chunkKey
				<< ",powerSize," << (int32_t)powerSize
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Re-distribute storeMemoryLimit configuration among partitionGroups.
*/
void ChunkManager::redistributeMemoryLimit(PartitionId pId) {
	assert(isValidId(pId));
	try {
		memoryLimitManager_.redistributeMemoryLimit(getPartitionGroupId(pId));
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Adjust using store memory to the memoryLimit of the partitionGroup.
*/
void ChunkManager::adjustStoreMemory(PartitionId pId) {
	assert(isValidId(pId));
	try {
		BufferManager& bufferManager = getBufferManager(pId);
		bufferManager.adjustStoreMemory();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Re-construct AffinityTable according to the storeMemoryLimit.
*/
void ChunkManager::reconstructAffinityTable(PartitionId pId) {
	static_cast<void>(pId);
	assert(isValidId(pId));

	try {
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Free some chunks of the ChunkKey of smaller than chunkKey argument.
*/
bool ChunkManager::batchFreeChunk(
		PartitionId pId, ChunkKey chunkKey,
		uint64_t scanChunkNum, uint64_t& scanCount,
		util::NormalXArray<ChunkCategoryId> &freeCategoryList,
		util::NormalXArray<ChunkId> &freeChunkList,
		ChunkKey simulateChunkKey, uint64_t& simulateFreeNum) {
	assert(!readOnly_);
	assert(isValidId(pId));

	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);

	ChunkCursor& cursor = metaChunkManager.getBatchFreeCursor(pId);
	ChunkKey* metaChunkKey;
	BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
			pId, cursor, metaChunkKey);
	bool isHead = false;
	for (scanCount = 0; scanCount < scanChunkNum && !isHead; scanCount++) {
		if (isBatchFreeMode(cursor.categoryId_) && !baseMetaChunk->isFree()) {
			if (metaChunkKey && *metaChunkKey <= simulateChunkKey) {
				simulateFreeNum++;
			}
			if (metaChunkKey && *metaChunkKey <= chunkKey) {
				assert(!getBufferManager(pId).isFixed(
								pId, cursor.categoryId_, cursor.cId_));
				freeCategoryList.push_back(cursor.categoryId_);
				freeChunkList.push_back(cursor.cId_);
				freeChunk(pId, cursor.categoryId_, cursor.cId_);
				metaChunkManager.incrementPGBatchFreeCount();
			}
		}

		baseMetaChunk = metaChunkManager.next(pId, cursor, isHead, metaChunkKey);
		if (!isHead &&
			metaChunkManager.getNextTail(pId, cursor.categoryId_).cId_ <=
				cursor.cId_) {
			while (true) {
			cursor.categoryId_++;
			cursor.categoryId_ =
				static_cast<ChunkCategoryId>(
						cursor.categoryId_ % getConfig().getChunkCategoryNum());
				if (cursor.categoryId_ == 0
						|| isBatchFreeMode(cursor.categoryId_)) {
					break;
				}
			}
			cursor.cId_ = 0;
			if (cursor.categoryId_ == 0) {
				isHead = true;  
				break;
			}
			baseMetaChunk = metaChunkManager.begin(pId, cursor, metaChunkKey);
		}
	}


	return isHead;  
}

void ChunkManager::purgeDataAffinityInfo(PartitionId pId, Timestamp ts) {
	AffinityManager& affinityManager = getAffinityManager(pId);
	affinityManager.purge(pId, ts);
}



/*!
	@brief Validate if the header of the checkpoint block is valid.
*/
void ChunkManager::isValidFileHeader(PartitionGroupId pgId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	CheckpointManager& pgCheckpointManager = getPGCheckpointManager(pgId);

	if (pgCheckpointManager.getPGFileSize() == 0) {
		return;  
	}

	try {
		util::NormalXArray<uint8_t> chunk;
		uint8_t temp = 0;
		chunk.assign(getConfig().getChunkSize(), temp);

		bool isValidChunk = pgCheckpointManager.readRecoveryChunk(chunk.data());

		const uint32_t splitCount =
		  configTable_.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT);
		const bool splitMode = splitCount > 0;
		ChunkHeader::validateHeader(
				chunk.data(), getConfig().getPartitionNum(),
				getConfig().getChunkExpSize(),
				getConfig().getCpFileSplitCount(),
				getConfig().getCpFileSplitStripeSize(),
				splitMode,
				isValidChunk);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

void ChunkManager::resetCheckpointBit(PartitionGroupId pgId) {
	try {
		assert(pgId < getConfig().getPartitionGroupNum());
		CheckpointManager& pgCheckpointManager = getPGCheckpointManager(pgId);
		pgCheckpointManager.initializePGCheckpointBit();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

void ChunkManager::releaseUnusedFileBlocks(PartitionGroupId pgId) {
	try {
		assert(pgId < getConfig().getPartitionGroupNum());
		CheckpointManager& pgCheckpointManager = getPGCheckpointManager(pgId);
		pgCheckpointManager.releaseUnusedPGFileBlocks();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Recovery the chunk by information and CheckpointFile.
*/
void ChunkManager::recoveryChunk(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		ChunkKey chunkKey, uint8_t unoccupiedSize, uint64_t filePos) {
	if (!isValidId(pId, categoryId, cId)) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_PARTITION_ID,
				"invalid IDs. " << getTraceInfo(pId, categoryId, cId)
				<< ",chunkKey," << chunkKey << ",occupiedSize,"
				<< (int32_t)unoccupiedSize << ",filePos,"
				<< filePos);
	}
	CheckpointManager& checkpointManager = getCheckpointManager(pId);

	checkpointManager.setPGCheckpointBit(filePos);
	try {
		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				getTraceInfo(pId, categoryId, cId)
				<< ", chunkKey, " << chunkKey << ", unoccupiedSize, "
				<< unoccupiedSize << ", filePos, " << filePos);

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk =
				metaChunkManager.reallocate(pId, categoryId, cId, metaChunkKey);

		if (!exist(pId)) {
			initialize(pId);
		}

		if (metaChunkKey != NULL) {
			*metaChunkKey = chunkKey;
		}
		baseMetaChunk->setUnoccupiedSize(unoccupiedSize);

		baseMetaChunk->setCheckpointPos(filePos);
		checkpointManager.incrementCheckpointNum(pId);

		if (getConfig().isWarmStart()) {
			BufferManager& bufferManager = getBufferManager(pId);
			bufferManager.enwarm(
					pId, categoryId, cId, filePos);  
		}

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				getTraceInfo(pId, categoryId, cId)
				<< ",chunkKey," << chunkKey << ",occupiedSize,"
				<< (int32_t)unoccupiedSize << ",filePos," << filePos);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Recovery a chunk from the chunk image.
*/
void ChunkManager::recoveryChunk(
		PartitionId pId, const uint8_t* chunk,
		uint32_t size, bool forIncrementalBackup)
{
	if (!chunk) {
		GS_THROW_USER_ERROR(GS_ERROR_CHM_NO_CHUNK_IMAGE, "pId," << pId);
	}
	if (size != getConfig().getChunkSize()) {
		GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_CHUNK_SIZE, "pId," << pId);
	}
	ChunkHeader::validateHeader(
			chunk, getConfig().getPartitionNum(),
			getConfig().getChunkExpSize(),
			getConfig().getCpFileSplitCount(),
			getConfig().getCpFileSplitStripeSize(),
			false, 
			true);
	if (forIncrementalBackup) {
		pId = ChunkHeader::getPartitionId(chunk);
	}
	else {
		if (pId != ChunkHeader::getPartitionId(chunk)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CHM_INVALID_PARTITION_ID, "pId, " << pId);
		}
	}
	ChunkCategoryId categoryId = ChunkHeader::getChunkCategoryId(chunk);
	ChunkId cId = ChunkHeader::getChunkId(chunk);
	if (!isValidId(pId, categoryId, cId)) {
		GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_CHUNK_ID,
				"invalid chunk Id" << getTraceInfo(pId, categoryId, cId));
	}
	ChunkKey chunkKey = ChunkHeader::getChunkKey(chunk);
	uint8_t freeMode = ChunkHeader::getAttribute(chunk);
	uint8_t unoccupiedSize = ChunkHeader::getUnoccupiedSize(chunk);
	if (isBatchFreeMode(categoryId)) {
		if (freeMode != BATCH_FREE_MODE) {
			GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_CHUNK_ATTRIBUTE,
					"invalid chunk category attribute BATCH_FREE_MODE, expected "
					"SELF_FREE_MODE"
					<< getTraceInfo(pId, categoryId, cId));
		}
	}
	else {
		if (freeMode == BATCH_FREE_MODE) {
			GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_CHUNK_ATTRIBUTE,
					"invalid chunk category attribute SELF_FREE_MODE, expected "
					"BATCH_FREE_MODE"
					<< getTraceInfo(pId, categoryId, cId));
		}
	}
	try {
		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				getTraceInfo(pId, categoryId, cId).c_str()
				<< ", attribute = " << (int32_t)freeMode
				<< ", chunkKey = " << chunkKey);

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);

		if (!exist(pId)) {
			initialize(pId);
		}

		BaseMetaChunk* baseMetaChunk = NULL;
		if (forIncrementalBackup) {
			ChunkCursor& cursor = metaChunkManager.getNextTail(pId, categoryId);
			if (cId < cursor.cId_) {
				ChunkKey *metaChunkKey;
				baseMetaChunk = metaChunkManager.getMetaChunk(
						pId, categoryId, cId, metaChunkKey);
				if (!baseMetaChunk->isFree()) {
					freeChunk(pId, categoryId, cId);
				}
			}
		}
		ChunkKey *metaChunkKey;
		baseMetaChunk = metaChunkManager.reallocate(
				pId, categoryId, cId, metaChunkKey);
		BufferManager& bufferManager = getBufferManager(pId);
		BufferInfo* bufferInfo = NULL;
		uint8_t* buffer =
				bufferManager.allocate(pId, categoryId, cId, bufferInfo);
		memcpy(buffer, chunk, size);
		ChunkHeader::setCpFileSplitCount(buffer, getConfig().getCpFileSplitCount());
		ChunkHeader::setCpFileSplitStripeSize(buffer, getConfig().getCpFileSplitStripeSize());
		bufferManager.unfix(pId, categoryId, cId);  

		if (metaChunkKey != NULL) {
			*metaChunkKey = chunkKey;
		}
		baseMetaChunk->setUnoccupiedSize(unoccupiedSize);

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				getTraceInfo(pId, categoryId, cId) << ",chunkKey," << chunkKey
				<< ",occupiedSize,"
				<< (int32_t)unoccupiedSize);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Adjust using store memory to the memoryLimit of the partitionGroup.
*/
void ChunkManager::adjustPGStoreMemory(PartitionGroupId pgId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	try {
		BufferManager& bufferManager = getPGBufferManager(pgId);
		bufferManager.adjustStoreMemory();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}


/*!
	@brief Start checkpoint of partitionGroup by initializing checkpoint data.
*/
void ChunkManager::startCheckpoint(PartitionGroupId pgId, CheckpointId cpId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	try {
		CheckpointManager& pgCheckpointManager = getPGCheckpointManager(pgId);
		pgCheckpointManager.clearPG();
		pgCheckpointManager.setPGCheckpointId(cpId);
		pgCheckpointManager.clearPGBackupBit();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief End checkpoint of partitionGroup by switching the recent checkpoint
   blocks.
*/
void ChunkManager::endCheckpoint(PartitionGroupId pgId, CheckpointId cpId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	try {
		CheckpointManager& pgCheckpointManager = getPGCheckpointManager(pgId);
		uint64_t kickCount = pgCheckpointManager.getPGKickCount();
		uint64_t kickTime = pgCheckpointManager.getPGKickTime();
		if (kickCount > 0) {
			GS_TRACE_WARNING(
				IO_MONITOR, GS_TRACE_CHM_CHECKPOINT_MEMORY_LIMIT_EXCEEDED,
				"CheckpointMemoryLimit exceeded: pgId," << pgId <<
						  ",forceWriteCount," << kickCount <<
						  ",forceWriteTime," << kickTime);
		}
		pgCheckpointManager.switchPGCheckpointBit(cpId);
		pgCheckpointManager.clearPG();

	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Flush the checkpointFile of partitionGroup.
*/
void ChunkManager::flush(PartitionGroupId pgId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	try {
		EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);
		CheckpointManager& pgCheckpointManager = getPGCheckpointManager(pgId);

		pgCheckpointManager.flushPGFile();

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL,
				GS_TRACE_CHM_INTERNAL_INFO, "pgId," << pgId);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Return checkpoint bits of the CheckpointFile of partitionGroup.
*/
ChunkBitArray& ChunkManager::getCheckpointBit(PartitionGroupId pgId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	CheckpointManager& checkpointManager = getPGCheckpointManager(pgId);
	return checkpointManager.getPGCheckpointBit();
}

/*!
	@brief Clean the information for the checkpoint of partitionGroup.
*/
void ChunkManager::cleanCheckpointData(PartitionGroupId pgId) {
	assert(pgId < getConfig().getPartitionGroupNum());
	CheckpointManager& checkpointManager = getPGCheckpointManager(pgId);
	checkpointManager.clearPG();
}

void ChunkManager::checkChunkMeta(PartitionId pId) {
	assert(isValidId(pId));

	try {
		bool error = false;
		CheckpointManager& checkpointManager = getCheckpointManager(pId);
		checkpointManager.clearPartition();

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkCursor cursor;
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, metaChunkKey);
		bool isHead = false;
		while (!isHead) {
			if (baseMetaChunk->isFree()) {
				baseMetaChunk = metaChunkManager.next(
						pId, cursor, isHead, metaChunkKey);
				continue;
			}

			MetaChunk* metaChunk = getChunk(
					pId, cursor.categoryId_, cursor.cId_);


			if (metaChunk->buffer_) {
				PartitionId chunkPId = ChunkHeader::getPartitionId(metaChunk->buffer_);
				ChunkCategoryId chunkCategoryId = ChunkHeader::getChunkCategoryId(metaChunk->buffer_);
				ChunkId chunkCId = ChunkHeader::getChunkId(metaChunk->buffer_);
				if (chunkPId != pId ||
					chunkCategoryId != cursor.categoryId_ ||
					chunkCId != cursor.cId_) {
					GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_INTERNAL_INFO,
								   "ChunkHeader not match: expected=(" << pId <<
								   "," << (int32_t)cursor.categoryId_ <<
								   "," << cursor.cId_ <<
								   "), result=(" << chunkPId <<
								   "," << (int32_t)chunkCategoryId <<
								   "," << chunkCId << ")" <<
								   ",cpPos," << baseMetaChunk->getCheckpointPos());
					error = true;
				}
			}
			baseMetaChunk = metaChunkManager.next(
					pId, cursor, isHead, metaChunkKey);
		}
		if (error) {
			GS_THROW_SYSTEM_ERROR(
					GS_ERROR_CHM_RECOVERY_FAILED,
				"Chunk header info does not match with Chunk meta info");
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Start checkpoint of the partition by collectiong information of
   checkpoint blocks.
*/
void ChunkManager::startCheckpoint(PartitionId pId) {

	assert(isValidId(pId));

	try {
		CheckpointManager& checkpointManager = getCheckpointManager(pId);
		checkpointManager.clearPartition();

		BufferManager& bufferManager = getBufferManager(pId);

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkCursor cursor;
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, metaChunkKey);
		bool isHead = false;
		while (!isHead) {
			if (baseMetaChunk->isFree()) {
				if (baseMetaChunk->getCheckpointPos() == -1) {
					;  
				}
				else {
					checkpointManager.removeOldCheckpointChunkId(
						pId, baseMetaChunk->getCheckpointPos());
					baseMetaChunk->setCheckpointPos(-1);
				}

				baseMetaChunk = metaChunkManager.next(
						pId, cursor, isHead, metaChunkKey);
				continue;
			}


			int64_t swapPos;
			bool toWrite = bufferManager.startCheckpoint(
				pId, cursor.categoryId_, cursor.cId_, swapPos);

			int64_t oldCheckpointPos = baseMetaChunk->getCheckpointPos();
			int64_t newCheckpointPos = swapPos; 

			if (0 <= swapPos) {
				if (toWrite) {  
					checkpointManager.appendNewCheckpointChunkId(
							pId, cursor.categoryId_, cursor.cId_,
							oldCheckpointPos, newCheckpointPos, toWrite);
				}
				else {  
					checkpointManager.appendNewCheckpointChunkId(
							pId, cursor.categoryId_, cursor.cId_,
							oldCheckpointPos, newCheckpointPos);
				}
				baseMetaChunk = metaChunkManager.getMetaChunk(
						pId, cursor.categoryId_, cursor.cId_, metaChunkKey);
				baseMetaChunk->setCheckpointPos(newCheckpointPos);
				baseMetaChunk->setCumulativeDirtyFlag(true);
				baseMetaChunk->setDifferentialDirtyFlag(true);
			}
			else {
				assert(newCheckpointPos == -1);
				newCheckpointPos =
					oldCheckpointPos;  
				checkpointManager.appendNewCheckpointChunkId(
						pId, cursor.categoryId_, cursor.cId_,
						oldCheckpointPos, newCheckpointPos);
				assert(oldCheckpointPos == baseMetaChunk->getCheckpointPos());
			}

			GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
					getTraceInfo(pId, cursor.categoryId_, cursor.cId_));

			baseMetaChunk = metaChunkManager.next(
					pId, cursor, isHead, metaChunkKey);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief End checkpoint of the partition by clearing checkpoint data.
*/
void ChunkManager::endCheckpoint(PartitionId pId) {
	assert(isValidId(pId));
	try {
		CheckpointManager& checkpointManager = getCheckpointManager(pId);
		checkpointManager.clearPartition();
		BufferManager& bufferManager = getBufferManager(pId);
		bufferManager.endCheckpoint();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Return number of scaning chunks of the partition.
*/
uint64_t ChunkManager::getScanSize(PartitionId pId) {
	return getMetaChunkManager(pId).getTotalListElementNum(pId);
}

ChunkManager::MetaChunk* ChunkManager::begin(
		PartitionId pId, ChunkCategoryId& categoryId,
		ChunkId& cId, ChunkKey* &chunkKey) {
	assert(isValidId(pId));

	ChunkCursor& cursor = getSyncCursor(pId);
	cursor.categoryId_ = 0;
	cursor.cId_ = 0;
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(pId, cursor, chunkKey);
	categoryId = cursor.categoryId_;
	cId = cursor.cId_;
	if (baseMetaChunk->isFree()) {
		return NULL;
	}
	else {
		getMetaChunk(pId).base_ = baseMetaChunk;
		return &getMetaChunk(pId);
	}
}
ChunkManager::MetaChunk* ChunkManager::next(
		PartitionId pId, ChunkCategoryId& categoryId,
		ChunkId& cId, ChunkKey* &chunkKey) {
	assert(isValidId(pId));
	ChunkCursor& cursor = getSyncCursor(pId);
	bool isHead = false;
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	BaseMetaChunk* baseMetaChunk = metaChunkManager.next(
			pId, cursor, isHead, chunkKey);
	categoryId = cursor.categoryId_;
	cId = cursor.cId_;
	if (baseMetaChunk->isFree()) {
		return NULL;
	}
	else if (isHead) {
		categoryId = -1;
		cId = UNDEF_CHUNKID;
		return NULL;
	}
	else {
		getMetaChunk(pId).base_ = baseMetaChunk;
		return &getMetaChunk(pId);
	}
}

/*!
	@brief Check if the chunks to copy left.
*/
bool ChunkManager::isCopyLeft(PartitionId pId) {
	assert(isValidId(pId));
	CheckpointManager& checkpointManager = getCheckpointManager(pId);
	return checkpointManager.isCopyLeft(pId);
}

/*!
	@brief Copy a chunk to the buffer of the checkpoint thread writer.
*/
bool ChunkManager::kickChunk(PartitionId pId) {
	assert(isValidId(pId));

	try {
		CheckpointManager& checkpointManager = getCheckpointManager(pId);

		ChunkCategoryId categoryId;
		ChunkId cId;

		bool copied = false;
		bool existCopyChunkId = true;
		size_t scanCount = 0;

		while (!copied && scanCount < MAX_COPY_SCAN_NUM_) {
			scanCount++;
			existCopyChunkId =
					checkpointManager.getCopyChunkId(pId, categoryId, cId);

			if (existCopyChunkId) {

				BufferManager& bufferManager = getBufferManager(pId);
				uint8_t* checkpointBuffer = NULL;
				int64_t checkpointPos;
				bufferManager.getCheckpointBuffer(
						pId, categoryId, cId, checkpointPos, checkpointBuffer);

				if (checkpointBuffer) {
					copied = true;
				}

				checkpointManager.kickChunk(
						pId, categoryId, cId, checkpointBuffer, checkpointPos);

				GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
						getTraceInfo(pId, categoryId, cId));
			}
			else {
				break;
			}
		}

		return !existCopyChunkId;  
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Write a chunk to the checkpointFile by checkpoint thread writer.
*/
uint64_t ChunkManager::writeChunk(PartitionId pId) {
	assert(isValidId(pId));

	try {
		uint64_t wroteCount = 0;
		CheckpointManager& checkpointManager = getCheckpointManager(pId);
		while (checkpointManager.writeChunk(pId)) {
			wroteCount++;
		}
		return wroteCount;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Prepare the chunk list for synchronization between nodes.
*/
uint64_t ChunkManager::startSync(CheckpointId cpId, PartitionId pId) {
	assert(isValidId(pId));
	CheckpointManager& checkpointManager = getCheckpointManager(pId);

	try {
		if (checkpointManager.isExecutingCheckpoint(pId)) {
			GS_TRACE_WARNING(CHUNK_MANAGER, GS_TRACE_CHM_INTERNAL_INFO,
					"invalid executing checkpoint, cpId,"
					<< cpId << ",pId," << pId << ",innerCpId,"
					<< checkpointManager.getPGCompletedCheckpointId());
		}
		if (checkpointManager.getPGCompletedCheckpointId() != cpId) {
			GS_TRACE_WARNING(CHUNK_MANAGER, GS_TRACE_CHM_INTERNAL_INFO,
					"invalid cpId, "
					<< cpId << ",pId," << pId << ",innerCpId,"
					<< checkpointManager.getPGCompletedCheckpointId());
		}

		checkpointManager.clearCheckpointPosList();
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkCursor cursor;
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, metaChunkKey);
		bool isHead = false;
		while (!isHead) {
			if (baseMetaChunk->getCheckpointPos() != -1) {
				int64_t checkpointPos = baseMetaChunk->getCheckpointPos();
				checkpointManager.appendCheckpointPosList(pId, checkpointPos);
			}

			baseMetaChunk = metaChunkManager.next(
					pId, cursor, isHead, metaChunkKey);
		}

		assert(pId != checkpointManager.getSyncPId() ||
				checkpointManager.getSyncChunkNum() ==
				getPartitionData(pId).partitionInfo_.checkpointNum_);
		return checkpointManager.getSyncChunkNum();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e,
				"cpId," << cpId << ",pId," << pId << ",innerCpId,"
				<< checkpointManager.getPGCompletedCheckpointId()
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Return one of the chunk on the list for synchronization between
   nodes.
*/
bool ChunkManager::getCheckpointChunk(
		PartitionId pId, uint32_t size, uint8_t* buffer) {
	assert(isValidId(pId));

	CheckpointManager& checkpointManager = getCheckpointManager(pId);
	if (pId != checkpointManager.getSyncPId()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CHM_INVALID_PARTITION_ID, "invalid pId, " << pId);
	}
	if (size != getConfig().getChunkSize()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CHM_INVALID_CHUNK_SIZE, "invalid chunk size, " << size);
	}

	try {
		int64_t checkpointPos;
		bool isTail = checkpointManager.getCheckpointPosList(checkpointPos);
		if (0 <= checkpointPos) {
			checkpointManager.readSyncChunk(checkpointPos, buffer);
		}
		if (isTail) {
			checkpointManager.clearCheckpointPosList();
		}
		return isTail;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e,
				"pId," << pId << ",innerCpId,"
				<< checkpointManager.getPGCompletedCheckpointId()
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

void ChunkManager::getBackupChunk(
	PartitionGroupId pgId, uint64_t checkpointPos, uint8_t* buffer) {
	CheckpointManager& checkpointManager = getPGCheckpointManager(pgId);
	try {
		checkpointManager.readCheckpointChunk(checkpointPos, buffer);
	}
	catch (std::exception& e) {
		UTIL_RETHROW_SYSTEM_ERROR(GS_ERROR_CHM_GET_CHECKPOINT_CHUNK_FAILED, e,
			"pgId," << pgId << ",checkpointPos," << checkpointPos);
	}
}
ChunkBitArray& ChunkManager::getBackupBitArray(PartitionGroupId pgId) {
	CheckpointManager& checkpointManager = getPGCheckpointManager(pgId);
	return checkpointManager.getPGBackupBit();
}

uint64_t ChunkManager::backupCheckpointFile(
	PartitionGroupId pgId, CheckpointId cpId, const std::string& backupPath) {

	assert(pgId < getConfig().getPartitionGroupNum());

	uint64_t copySize = 0;
	Config& config = getConfig();
	if (config.getChunkExpSize() < 20 && config.cpFileSplitCount_ == 1) {
		copySize = backupCheckpointFileForSmallChunk(pgId, cpId, backupPath);
	}
	else {
		copySize = backupCheckpointFileForLargeChunk(pgId, cpId, backupPath);
	}
	return copySize;
}

void ChunkManager::makeBackupDirList(
		const std::string& backupPath, std::vector<std::string> &dirList) {

	dirList.clear();
	Config& config = getConfig();

	uint32_t fileSplitCount = 1;
	fileSplitCount = config.getCpFileSplitCount();
	for (uint32_t nth = 0; nth < fileSplitCount; ++nth) {
		util::NormalOStringStream oss;
		oss << nth;

		u8string path;
		util::FileSystem::createPath(backupPath.c_str(), oss.str().c_str(), path);

		dirList.push_back(path.c_str());
	}
}

uint64_t ChunkManager::backupCheckpointFileForLargeChunk(
		PartitionGroupId pgId, CheckpointId cpId, const std::string& backupPath) {
	try {
		Config& config = getConfig();

		ByteArray &byteArray = getPGBackupBuffer(pgId);

		CheckpointFile* cpFile = &getPGCheckpointFile(pgId);
		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"Backup started (backupName=" << backupPath << ")");

		std::vector<std::string> dirList;
		makeBackupDirList(backupPath, dirList);

		CheckpointFile backupFile_org(
			config.getChunkExpSize(), backupPath, pgId,
			config.getCpFileSplitCount(), config.getCpFileSplitStripeSize(),
			dirList);
		CheckpointFile* backupFile = &backupFile_org;
		backupFile->open();

		assert(cpFile);
		assert(backupFile);


		cpFile->flush();
		const uint32_t chunkSize = config.getChunkSize();
		const uint64_t chunkNum = cpFile->getBlockNum();

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"chunkSize=" << chunkSize << ", chunkNum=" << chunkNum);

		FileManager originalFileManager(config_, *cpFile);
		FileManager backupFileManager(config_, *backupFile);
		for (uint64_t blockNo = 0; blockNo < chunkNum; blockNo++) {

			bool isValid = cpFile->getValidBlockInfo(blockNo);
			if (blockNo == 0 || isValid) {
				originalFileManager.readChunk(byteArray.data(), 1, blockNo);
				if (isValid) {
					uint32_t chunkCheckSum =
							ChunkHeader::getCheckSum(byteArray.data());
					uint32_t calcCheckSum =
							ChunkHeader::calcCheckSum(byteArray.data());

					EXEC_FAILURE(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM);

					if (chunkCheckSum != calcCheckSum) {
						GS_THROW_SYSTEM_ERROR(
								GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM,
								"checkSum, " << chunkCheckSum << ", "
											 << calcCheckSum << ", pgId, "
											 << pgId << ",blockNo,"
											 << blockNo);
					}
				}
				backupFile->writeBlock(byteArray.data(), 1, blockNo);
			}
			else {
#ifdef _WIN32
				backupFile->writeBlock(byteArray.data(), 1, blockNo);
#else
				uint64_t offset = blockNo * chunkSize;
				uint32_t holeOffset = FILE_SYSTEM_BLOCK_SIZE;
				uint32_t holeSize = chunkSize - holeOffset;

				backupFile->writePartialBlock(byteArray.data(), holeOffset, offset);
				backupFile->punchHoleBlock(holeSize, offset + holeOffset);
#endif
			}
		}
		backupFile->flush();
		uint64_t totalFileSize = backupFile->getFileSize();
		backupFile->close();
		backupFile = NULL;

		return totalFileSize;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "pgId," << pgId << ",cpId," << cpId << GS_EXCEPTION_MESSAGE(e));
	}
}

uint64_t ChunkManager::backupCheckpointFileForSmallChunk(
	PartitionGroupId pgId, CheckpointId cpId, const std::string& backupPath) {
	try {
		ByteArray &byteArray = getPGBackupBuffer(pgId);
		const uint32_t copySize = 1 * 1024 * 1024;
		uint8_t temp = 0;
		byteArray.assign(copySize, temp);

		Config& config = getConfig();
		assert(config.getChunkSize() <= copySize);

		CheckpointFile* cpFile = &getPGCheckpointFile(pgId);
		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"Backup started (backupName=" << backupPath << ")");

		std::vector<std::string> dirList;
		makeBackupDirList(backupPath, dirList);

		CheckpointFile backupFile_org(
			config.getChunkExpSize(), backupPath, pgId,
			config.getCpFileSplitCount(), config.getCpFileSplitStripeSize(),
			dirList);
		CheckpointFile* backupFile = &backupFile_org;
		backupFile->open();

		assert(cpFile);
		assert(backupFile);


		cpFile->flush();
		const uint32_t chunkSize = config.getChunkSize();
		const uint64_t chunkNum = cpFile->getBlockNum();
		const uint32_t copyNum = copySize / chunkSize;

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"copySize=" << copySize << ", chunkNum=" << chunkNum
						<< ", copyNum=" << copyNum);
		uint64_t srcBlockNo = 0;
		uint64_t destBlockNo = 0;
		FileManager originalFileManager(config_, *cpFile);
		FileManager backupFileManager(config_, *backupFile);

		{
			for (; (srcBlockNo + copyNum) < chunkNum;
				 srcBlockNo += copyNum, destBlockNo += copyNum) {
				GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
					"srcBlockNo=" << srcBlockNo << ", chunkNum=" << chunkNum);

				originalFileManager.readChunk(
					byteArray.data(), copyNum, srcBlockNo);

				for (uint32_t i = 0; i < copyNum; ++i) {
					if (cpFile->getValidBlockInfo(srcBlockNo + i)) {
						uint32_t chunkCheckSum = ChunkHeader::getCheckSum(
							byteArray.data() + chunkSize * i);
						uint32_t calcCheckSum = ChunkHeader::calcCheckSum(
							byteArray.data() + chunkSize * i);

						EXEC_FAILURE(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM);
						if (chunkCheckSum != calcCheckSum) {
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM,
								"checkSum, " << chunkCheckSum << ", "
											 << calcCheckSum << ", pgId, "
											 << pgId << ",blockNo,"
											 << srcBlockNo + i);
						}
					}
				}
				backupFile->writeBlock(byteArray.data(), copyNum, destBlockNo);
			}
			if (srcBlockNo < chunkNum) {
				const uint32_t lastCopyNum =
					static_cast<uint32_t>(chunkNum - srcBlockNo);
				GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
					"srcBlockNo=" << srcBlockNo << ", chunkNum=" << chunkNum
								  << ", lastCopyNum=" << lastCopyNum);
				originalFileManager.readChunk(
					byteArray.data(), lastCopyNum, srcBlockNo);

				for (uint32_t i = 0; i < lastCopyNum; ++i) {
					if (cpFile->getValidBlockInfo(srcBlockNo + i)) {
						uint32_t chunkCheckSum = ChunkHeader::getCheckSum(
							byteArray.data() + chunkSize * i);
						uint32_t calcCheckSum = ChunkHeader::calcCheckSum(
							byteArray.data() + chunkSize * i);

						EXEC_FAILURE(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM);
						if (chunkCheckSum != calcCheckSum) {
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM,
								"checkSum, " << chunkCheckSum << ", "
											 << calcCheckSum << ", pgId, "
											 << pgId << ",blockNo,"
											 << srcBlockNo + i);
						}
					}
				}
				backupFile->writeBlock(byteArray.data(), lastCopyNum, destBlockNo);
			}
		}

		backupFile->flush();
		backupFile->close();
		backupFile = NULL;

		return chunkNum * config.getChunkSize();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "pgId," << pgId << ",cpId," << cpId << GS_EXCEPTION_MESSAGE(e));
	}
}
int64_t ChunkManager::getCheckpointChunkPos(
		PartitionId pId,
		std::map<int64_t, int64_t> &oldOffsetMap 
		) {
	assert(isValidId(pId));

	CheckpointManager& checkpointManager = getCheckpointManager(pId);
	try {
		oldOffsetMap.clear();

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkCursor cursor;
		int64_t chunkSeqNumber = 0;
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, metaChunkKey);
		bool isHead = false;
		while (!isHead) {
			int64_t checkpointPos = baseMetaChunk->getCheckpointPos();
			if (checkpointPos != -1) {
				assert(oldOffsetMap.find(checkpointPos) == oldOffsetMap.end());
				oldOffsetMap[checkpointPos] = chunkSeqNumber;
				++chunkSeqNumber;
			}
			baseMetaChunk = metaChunkManager.next(
					pId, cursor, isHead, metaChunkKey);
		}
		return chunkSeqNumber;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "pId," << pId << ",innerCpId,"
					  << checkpointManager.getPGCompletedCheckpointId()
					  << GS_EXCEPTION_MESSAGE(e));
	}
}

void ChunkManager::prepareMakeSyncTempCpFile(
		PartitionGroupId pgId, PartitionId pId, CheckpointId cpId,
		const std::string& syncTempPath, CheckpointFile &syncTempFile,
		MakeSyncTempCpContext &context) {

	Config& config = getConfig();

	assert(pgId < config.getPartitionGroupNum());

	context.pgId_ = pgId;
	context.pId_ = pId;
	context.cpId_ = cpId;

	context.syncTempPath_ = syncTempPath;
	context.syncTempFile_ = &syncTempFile;
	context.srcCpFile_ = &getPGCheckpointFile(pgId);

	assert(context.readBuffer_.size() == context.IO_SIZE);
	assert(context.writeBuffer_.size() == context.IO_SIZE);

	context.chunkSize_ = config.getChunkSize();
	context.chunkNum_ = context.srcCpFile_->getBlockNum();
	context.copyNum_ = static_cast<int32_t>(context.IO_SIZE / context.chunkSize_);

	GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"makeSyncTempCpFile started (syncTempName=" << context.syncTempPath_ << ")");

	GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"copySize=" << context.IO_SIZE <<
			", chunkNum=" << context.chunkNum_ <<
			", copyNum=" << context.copyNum_);
}


bool ChunkManager::makeSyncTempCpFile(
		MakeSyncTempCpContext &context,
		std::map<int64_t, int64_t> &oldOffsetMap,
		std::map<int64_t, int64_t> &newOffsetMap,
		uint64_t &srcFilePos, uint64_t &destFilePos,
		uint64_t &writeCount) {

	try {
		uint8_t *writeBufferTop = context.writeBuffer_.data();
		uint64_t writeBufferPos = 0;
		for (; (srcFilePos + context.copyNum_) <= context.chunkNum_; ) {

			context.srcCpFile_->readBlock(
					context.readBuffer_.data(), context.copyNum_, srcFilePos);

			uint64_t oldDestWritePos = destFilePos;
			makeSyncTempCpFileInternal(
					context, oldOffsetMap, newOffsetMap,
					srcFilePos, context.copyNum_,
					destFilePos, writeBufferPos, writeCount, false);

			if (oldDestWritePos != destFilePos) {
				context.syncTempFile_->flush();
				return false; 
			}
		}
		if (static_cast<uint64_t>(srcFilePos) < context.chunkNum_) {
			const uint32_t lastCopyNum =
					static_cast<uint32_t>(context.chunkNum_ - srcFilePos);
			assert(lastCopyNum < context.copyNum_);
			GS_TRACE_INFO(
					CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
					"srcFilePos=" << srcFilePos <<
					", chunkNum=" << context.chunkNum_ <<
					", lastCopyNum=" << lastCopyNum);

			context.srcCpFile_->readBlock(
					context.readBuffer_.data(), lastCopyNum, srcFilePos);

			makeSyncTempCpFileInternal(
					context, oldOffsetMap, newOffsetMap,
					srcFilePos, lastCopyNum,
					destFilePos, writeBufferPos, writeCount, true);
		}
		if (writeBufferPos > 0) {
			context.syncTempFile_->writeBlock(
					writeBufferTop, static_cast<uint32_t>(writeBufferPos),
					destFilePos);
			destFilePos += writeBufferPos;
			GS_TRACE_INFO(
					CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
					"makeSyncTempCpFile: last write: writeBuferPos=" << writeBufferPos <<
					", destFilePos=" << destFilePos);
			writeBufferPos = 0;
		}
		context.syncTempFile_->flush();

		GS_TRACE_INFO(
				CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				"makeSyncTempCpFile: end: copy chunkNum=" << newOffsetMap.size());

		if (!oldOffsetMap.empty()) {
		}
		return true; 
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "pgId," << context.pgId_ << ",cpId," << context.cpId_ <<
				GS_EXCEPTION_MESSAGE(e));
	}
}

void ChunkManager::makeSyncTempCpFileInternal(
	MakeSyncTempCpContext &context,
	std::map<int64_t, int64_t> &oldOffsetMap,
	std::map<int64_t, int64_t> &newOffsetMap,
	uint64_t &srcFilePos, uint64_t readCount,
	uint64_t &destFilePos, uint64_t &writeBufferPos,
	uint64_t &writeCount, bool isLast) {

	uint8_t *writeBufferTop = &context.writeBuffer_.front();
	for (uint32_t i = 0; i < readCount; ++i) {
		const uint8_t* chunkTop = context.readBuffer_.data() + context.chunkSize_ * i;
		if (!context.srcCpFile_->getValidBlockInfo(srcFilePos + i)) {
			continue;
		}
		PartitionId chunkPId = ChunkHeader::getPartitionId(chunkTop);
		if (chunkPId != context.pId_) {
			continue;
		}
		uint32_t chunkCheckSum = ChunkHeader::getCheckSum(chunkTop);
		uint32_t calcCheckSum = ChunkHeader::calcCheckSum(chunkTop);

		EXEC_FAILURE(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM);
		if (chunkCheckSum != calcCheckSum) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM,
				"checkSum, " << chunkCheckSum << ", " <<
				calcCheckSum << ", pgId, " << context.pgId_ <<
				", pId," << chunkPId <<
				",blockNo," << srcFilePos + i);
		}
		int64_t newOffsetMapKey = -1;
		std::map<int64_t, int64_t>::iterator mapItr;
		if ((mapItr = oldOffsetMap.find(srcFilePos + i))
				== oldOffsetMap.end()) {
			continue;
		}
		else {
			newOffsetMapKey = mapItr->second;
			oldOffsetMap.erase(mapItr);
		}
		uint8_t* writeTop = writeBufferTop + context.chunkSize_ * writeBufferPos;
		memcpy(writeTop, chunkTop, context.chunkSize_);

		assert(newOffsetMap.find(newOffsetMapKey) == newOffsetMap.end());
		newOffsetMap[newOffsetMapKey] = static_cast<ChunkId>(destFilePos + writeBufferPos);
		++writeBufferPos;
		++writeCount;
		if (writeBufferPos == context.copyNum_) {
			context.syncTempFile_->writeBlock(
					writeBufferTop, static_cast<uint32_t>(writeBufferPos),
					destFilePos);
			destFilePos += writeBufferPos;
			writeBufferPos = 0;
			if (!isLast) {
				srcFilePos += i;
				return;
			}
		}
	} 
	srcFilePos += readCount;
}

void ChunkManager::setSwapOutCounter(PartitionId pId, int64_t counter) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	bufferManager.setSwapOutCounter(static_cast<uint64_t>(counter));
}

int64_t ChunkManager::getSwapOutCounter(PartitionId pId) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	return static_cast<int64_t>(bufferManager.getSwapOutCounter());
}

void ChunkManager::setStoreMemoryAgingSwapRate(PartitionId pId, double ratio) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	bufferManager.setStoreMemoryAgingSwapRate(ratio);
}

double ChunkManager::getStoreMemoryAgingSwapRate(PartitionId pId) {
	assert(isValidId(pId));
	BufferManager& bufferManager = getBufferManager(pId);
	return bufferManager.getStoreMemoryAgingSwapRate();
}

void ChunkManager::updateStoreObjectUseStats(PartitionId pId) {
	assert(isValidId(pId));

	try {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkCursor cursor;
		ChunkKey* chunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, chunkKey);

		std::vector<uint64_t> storeObjectUseSizeList;
		storeObjectUseSizeList.resize(getConfig().getChunkCategoryNum(), 0);

		bool isHead = false;
		while (!isHead) {
			if (!baseMetaChunk->isFree()) {
				uint64_t objectUseSize = getConfig().getChunkSize();
				uint8_t unoccupiedPowerSize = baseMetaChunk->getUnoccupiedSize();
				if (unoccupiedPowerSize > 0) {
					objectUseSize -= (1 << unoccupiedPowerSize);
				}
				storeObjectUseSizeList[cursor.categoryId_] += objectUseSize;
			}
			baseMetaChunk = metaChunkManager.next(pId, cursor, isHead, chunkKey);
		}

		for (ChunkCategoryId categoryId = 0;
				categoryId < getConfig().getChunkCategoryNum(); categoryId++) {
			metaChunkManager.setStoreObjectUseSize(pId, categoryId, 
				storeObjectUseSizeList[categoryId]);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}


std::string ChunkManager::dump(PartitionId pId, ChunkCategoryId categoryId) {
	assert(isValidId(pId, categoryId));

	util::NormalOStringStream stream;
	stream << "ChunkManagerDump,pId," << pId << std::endl;

	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	ChunkCursor cursor;
	bool isHead = false;
	if (categoryId == UNDEF_CHUNK_CATEGORY_ID) {
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, metaChunkKey);
		while (!isHead) {
			stream << "categoryId," << (int32_t)cursor.categoryId_ << ",cId,"
				   << cursor.cId_ << "," << baseMetaChunk->dump()
				   << ",chunkKey," << (metaChunkKey ? *metaChunkKey : -1)
				   << std::endl;
			baseMetaChunk = metaChunkManager.next(
					pId, cursor, isHead, metaChunkKey);
		}
	}
	else {
		cursor.categoryId_ = categoryId;
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk = metaChunkManager.begin(
				pId, cursor, metaChunkKey);
		while (!isHead) {
			stream << "categoryId," << (int32_t)cursor.categoryId_ << ",cId,"
				   << cursor.cId_ << "," << baseMetaChunk->dump()
				   << ",chunkKey," << (metaChunkKey ? *metaChunkKey : -1)
				   << std::endl;
			baseMetaChunk = metaChunkManager.nextForCategory(
					pId, cursor, isHead, metaChunkKey);
		}
	}
	return stream.str();
}
std::string ChunkManager::dumpChunkCSVField() {
	util::NormalOStringStream stream;

	stream << ChunkHeader::dumpFieldName() << "validChecksum";
	return stream.str();
}
std::string ChunkManager::dumpChunkCSV(PartitionGroupId pgId, int64_t pos) {
	util::NormalOStringStream stream;

	uint8_t* buffer = NULL;
	try {
		buffer = static_cast<uint8_t*>(checkpointMemoryPool_.allocate());

		FileManager fileManager(config_, getPGCheckpointFile(pgId));
		fileManager.readChunk(buffer, 1, pos);
		fileManager.isValidCheckSum(buffer);
		fileManager.uncompressChunk(buffer);

		stream << ChunkHeader::dump(buffer);

		checkpointMemoryPool_.deallocate(buffer);
		return stream.str();
	}
	catch (std::exception& e) {
		checkpointMemoryPool_.deallocate(buffer);
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

ChunkManager::PartitionGroupData::PartitionGroupData(ConfigTable &configTable, Config& config,
	PartitionGroupId pgId, const std::string& dir,
	MemoryPool& checkpointMemoryPool, util::Mutex& cpMemPoolMutex,
	MemoryPool& storeMemoryPool, PartitionDataList& partitionDataList)
	: pIdList_(),
	  allocator_(util::AllocatorInfo(
		  ALLOCATOR_GROUP_STORE, "chunkPartitionGroupLocal")),
	  multiThreadAllocator_(util::AllocatorInfo(
		  ALLOCATOR_GROUP_STORE, "chunkPartitionGroupShared")),
	  checkpointFile_(config.getChunkExpSize(), dir, pgId,
			config.getCpFileSplitCount(), config.getCpFileSplitStripeSize(),
			ChunkManager::Config::parseCpFileDirList(configTable)),
	  metaChunkManager_(pgId, pIdList_, partitionDataList, config),
	  metaChunk_(&metaChunkManager_),
	  checkpointManager_(pgId, allocator_, multiThreadAllocator_,
		  partitionDataList, checkpointFile_, checkpointMemoryPool,
		  cpMemPoolMutex, config),
	  bufferManager_(
		  pgId, partitionDataList, checkpointFile_, storeMemoryPool, config),
	  affinityManager_(config.getChunkCategoryNum(),
			AffinityManager::MAX_AFFINITY_SIZE, config.getAffinitySize(),
			partitionDataList)
{
	int32_t expSize = config.getChunkExpSize();
	expSize = (expSize >= 20) ? expSize : 20; 
	uint8_t temp = 0;
	backupBuffer_.assign(1 << expSize, temp);
}
void ChunkManager::drop(PartitionId pId) {
	PartitionData& partitionData = getPartitionData(pId);
	partitionData.partitionExistance_ = NO_PARTITION;  
	PartitionGroupId pgId = getPartitionGroupId(pId);

	ActivePartitionList& activePartitionList = getPGActivePartitionList(pgId);
	ActivePartitionList::iterator pIdItr = activePartitionList.begin();
	for (; pIdItr != activePartitionList.end(); pIdItr++) {
		if (*pIdItr == pId) {
			activePartitionList.erase(pIdItr);
			assert(0 < atomicExistPartitionNum_);
			atomicExistPartitionNum_--;
			break;
		}
	}
}
void ChunkManager::initialize(PartitionId pId) {
	PartitionData& partitionData = getPartitionData(pId);
	partitionData.partitionExistance_ = EXIST_PARTITION;  
	PartitionGroupId pgId = getPartitionGroupId(pId);
	ActivePartitionList& activePartitionList = getPGActivePartitionList(pgId);
	activePartitionList.push_back(pId);
	atomicExistPartitionNum_++;
}
bool ChunkManager::exist(PartitionId pId) {
	PartitionData& partitionData = getPartitionData(pId);
	return (partitionData.partitionExistance_ == EXIST_PARTITION);  
}
uint32_t ChunkManager::getAtomicExistPartitionNum() {
	return atomicExistPartitionNum_;
}
ChunkId ChunkManager::searchEmptyChunk(
		PartitionId pId, ChunkCategoryId categoryId, uint8_t powerSize,
		ChunkKey chunkKey, uint64_t maxSearchNum, const DataAffinityInfo &affinityInfo) {
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	BufferManager& bufferManager = getBufferManager(pId);

	ChunkCursor& nextTail = metaChunkManager.getNextTail(pId, categoryId);
	uint64_t searchNum = nextTail.cId_;
	if (maxSearchNum < searchNum) {
		searchNum = maxSearchNum;
	}

	ChunkCursor* inMemoryCandidate = NULL;  
	ChunkCursor swappedCandidate(
		-1, UNDEF_CHUNKID);  
	ChunkCursor unusedCandidate(-1, UNDEF_CHUNKID);  

	ChunkCursor& cursor = metaChunkManager.getEmptyScanCursor(pId, categoryId);
	bool isHead = false;
	BaseMetaChunk* baseMetaChunk = NULL;
	if (nextTail.cId_ <= cursor.cId_) {
		cursor.cId_ = 0;  
	}
	ChunkKey *metaChunkKey = NULL;
	baseMetaChunk = metaChunkManager.begin(pId, cursor, metaChunkKey);
	uint64_t count = 0;
	for (; count < searchNum; count++) {
		assert(cursor.cId_ <
			   metaChunkManager.getNextTail(pId, cursor.categoryId_).cId_);

		if (!baseMetaChunk->isFree()) {								
			if (baseMetaChunk->getUnoccupiedSize() >= powerSize &&  
				!baseMetaChunk->isAffinityUse() &&  
				(baseMetaChunk->getAffinityId() == UNDEF_AFFINITY_GROUP_ID || 
				baseMetaChunk->getAffinityId() == affinityInfo.dataAffinity_) &&
				((chunkKey == UNDEF_CHUNK_KEY) || (metaChunkKey && chunkKey == *metaChunkKey)))
			{
				if (bufferManager.isOnBuffer(pId, categoryId, cursor.cId_)) {
					inMemoryCandidate = &cursor;
					break;
				}
				else {
					swappedCandidate =
						cursor;  
				}
			}
		}
		else {
			unusedCandidate = cursor;  
		}

		baseMetaChunk = metaChunkManager.nextForCategory(
				pId, cursor, isHead, metaChunkKey);
		if (!isHead && nextTail.cId_ <= cursor.cId_) {
			cursor.cId_ = 0;
			baseMetaChunk = metaChunkManager.begin(pId, cursor, metaChunkKey);
		}
	}

	if (inMemoryCandidate) {
#ifndef NDEBUG
		baseMetaChunk = metaChunkManager.getMetaChunk(
				pId, categoryId, cursor.cId_, metaChunkKey);
		assert(powerSize <= baseMetaChunk->getUnoccupiedSize());
#endif
		assert(inMemoryCandidate->cId_ == cursor.cId_);
		return cursor.cId_;
	}
	else if (swappedCandidate.cId_ != UNDEF_CHUNKID) {
		cursor = swappedCandidate;
#ifndef NDEBUG
		baseMetaChunk = metaChunkManager.getMetaChunk(
				pId, categoryId, swappedCandidate.cId_, metaChunkKey);
		assert(powerSize <= baseMetaChunk->getUnoccupiedSize());
#endif
		assert(swappedCandidate.cId_ == cursor.cId_);
		return swappedCandidate.cId_;
	}
	else if (unusedCandidate.cId_ != UNDEF_CHUNKID) {
		cursor = unusedCandidate;
#ifndef NDEBUG
		baseMetaChunk = metaChunkManager.getMetaChunk(
				pId, categoryId, unusedCandidate.cId_, metaChunkKey);
		assert(baseMetaChunk->isFree());
#endif
		assert(unusedCandidate.cId_ == cursor.cId_);
		return unusedCandidate.cId_;
	}
	else {
		metaChunkManager.expand(pId, nextTail);
		return nextTail.cId_ - 1;  
	}
}
std::string ChunkManager::getTraceInfo(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, bool enableMetaChunk,
	bool enableMetaChunkManager, bool enableCheckpointManager,
	bool enableMemoryLimitManager, bool enableBufferManager,
	bool enableFileManager, bool enableAffinityManager) {

	static_cast<void>(enableMetaChunkManager);
	static_cast<void>(enableCheckpointManager);
	static_cast<void>(enableMemoryLimitManager);
	static_cast<void>(enableBufferManager);
	static_cast<void>(enableFileManager);
	static_cast<void>(enableAffinityManager);

	util::NormalOStringStream stream;

	stream << ",pId," << pId << ",categoryId," << (int32_t)categoryId << ",cId,"
		   << cId << ",attribute,"
		   << (int32_t)getChunkCategoryAttribute(categoryId).freeMode_;

	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	if (enableMetaChunk &&
		cId < (ChunkId)metaChunkManager.getChunkList(pId, categoryId).size()) {
		ChunkKey *metaChunkKey;
		BaseMetaChunk* baseMetaChunk =
			metaChunkManager.getMetaChunk(pId, categoryId, cId, metaChunkKey);
		if (baseMetaChunk) {
			stream << baseMetaChunk->dump();
		}
	}
	return stream.str();
}

bool ChunkManager::isValidId(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	bool isValidPId = (pId < getConfig().getPartitionNum());
	bool isValidCategoryId = true;
	if (categoryId != UNDEF_CHUNK_CATEGORY_ID) {
		isValidCategoryId =
			(0 <= categoryId && categoryId < getConfig().getChunkCategoryNum());
	}
	bool isValidCId = true;
	if (cId != UNDEF_CHUNKID) {
		isValidCId = (0 <= cId &&
					  cId <= static_cast<ChunkId>(
								 getMetaChunkManager(pId).getMaxChunkId()));
	}
	return isValidPId && isValidCategoryId && isValidCId;
}

bool ChunkManager::isValid(PartitionId pId, ChunkCategoryId categoryId,
	bool enableMetaChunkManager, bool enableMemoryLimitManager) {
	bool isValidMetaChunkManager = true;
	bool isValidMemoryLimitManager = true;

	MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
	if (enableMetaChunkManager) {
		isValidMetaChunkManager = metaChunkManager.isValid(pId, categoryId);
	}
	if (enableMemoryLimitManager) {
		MemoryLimitManager& memoryLimitManager = memoryLimitManager_;
		isValidMemoryLimitManager = memoryLimitManager.isValid();
	}

	bool isValid = isValidMetaChunkManager && isValidMemoryLimitManager;
	return isValid;
}

bool ChunkManager::Config::isValid() {
	assert(pgIdList_.size() == static_cast<uint64_t>(getPartitionNum()));

	bool isValidChunkSize = ((1ULL << getChunkExpSize()) == getChunkSize());
	if (!isValidChunkSize) {
		GS_THROW_SYSTEM_ERROR(
			GS_ERROR_CHM_INVALID_CHUNK_SIZE, "chunk size must be 2^n.");
	}
	bool isValidMinimumChunkSize =
		(CHUNK_HEADER_FULL_SIZE * 2 <= static_cast<int32_t>(getChunkSize()));
	if (!isValidMinimumChunkSize) {
		GS_THROW_SYSTEM_ERROR(
			GS_ERROR_CHM_INVALID_CHUNK_SIZE, "chunk size must be bigger.");
	}
	bool isValidMaximumChunkSize = (getChunkExpSize() <= MAX_CHUNK_EXP_SIZE_);
	if (!isValidMaximumChunkSize) {
		GS_THROW_SYSTEM_ERROR(
			GS_ERROR_CHM_INVALID_CHUNK_SIZE, "chunk size must be bigger.");
	}
	bool isValidMaximumChunkCategoryNum =
		(getChunkCategoryNum() <= CHUNK_CATEGORY_NUM);
	if (!isValidMaximumChunkCategoryNum) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_CHUNK_CATEGORY_NUM,
			"chunk size must be bigger.");
	}
	return true;
}

void ChunkManager::Config::setUpConfigHandler(ConfigTable& configTable) {
	configTable.setParamHandler(CONFIG_TABLE_CP_CHECKPOINT_MEMORY_LIMIT, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_LIMIT, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE, *this); 
	configTable.setParamHandler(CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE, *this); 
}

void ChunkManager::Config::operator()(
	ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_CP_CHECKPOINT_MEMORY_LIMIT:
		setAtomicCheckpointMemoryLimit(ConfigTable::megaBytesToBytes(
			static_cast<uint32_t>(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_LIMIT:
		setAtomicStoreMemoryLimit(ConfigTable::megaBytesToBytes(
			static_cast<uint32_t>(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE:
		setAtomicShiftableMemRate(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD:
		setAtomicEMAHalfLifePeriod(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE:
		setAtomicStoreMemoryColdRate(value.get<double>());
		break;
	case CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE:
		setAtomicCpFileFlushSize(value.get<int64_t>());
		break;
	case CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE:
		setAtomicCpFileAutoClearCache(value.get<bool>());
		break;
	}
}

ChunkManager::Config::Config(
		PartitionGroupId partitionGroupNum, PartitionId partitionNum,
		ChunkCategoryId chunkCategoryNum, int32_t chunkSize, int32_t affinitySize,
		bool isWarmStart, uint64_t storeMemoryLimit, uint64_t checkpointMemoryLimit,
		uint32_t maxOnceSwapNum,
		CompressionMode compressionMode,
		ConfigTable &configTable,
		int32_t shiftableMemRate, int32_t emaHalfLifePeriod
		, double coldRate
		, uint64_t cpFileFlushSize
		, bool cpFileAutoClearCache
		, double bufferHashTableSizeRate   
		, uint32_t cpFileSplitCount   
		, uint32_t cpFileSplitStripeSize
	)
	: partitionGroupNum_(partitionGroupNum),
	  partitionNum_(partitionNum),
	  categoryNum_(chunkCategoryNum),
	  chunkExpSize_(static_cast<uint8_t>(util::nextPowerBitsOf2(static_cast<uint32_t>(chunkSize)))),
	  chunkSize_(chunkSize),
	  atomicAffinitySize_(affinitySize),
	  isWarmStart_(isWarmStart),
	  maxOnceSwapNum_(maxOnceSwapNum),
	  partitionCompressionMode_(partitionNum),
	  compressionMode_(initializetCompressionMode(compressionMode)),  
	  atomicShiftableMemRate_(shiftableMemRate),
	  atomicEMAHalfLifePeriod_(emaHalfLifePeriod)
	  , bufferHashTableSizeRate_(bufferHashTableSizeRate)  
	  , cpFileSplitCount_(cpFileSplitCount)   
	  , cpFileSplitStripeSize_(cpFileSplitStripeSize)   
{
	cpFileDirList_ = ChunkManager::Config::parseCpFileDirList(configTable);
	setAtomicStoreMemoryLimit(storeMemoryLimit * 1024 * 1024);
	setAtomicCheckpointMemoryLimit(checkpointMemoryLimit * 1024 * 1024);
	setAtomicStoreMemoryColdRate(coldRate);
	setAtomicCpFileFlushSize(cpFileFlushSize);
	setAtomicCpFileAutoClearCache(cpFileAutoClearCache);
}
ChunkManager::Config::~Config() {}
std::string ChunkManager::Config::dump() {
	util::NormalOStringStream stream;
	stream << "Config"
		   << ",allocatedCount," << getPartitionGroupNum() << ",partitionNum,"
		   << getPartitionNum() << ",isWarmStart," << isWarmStart()
		   << ",storePoolLimitNum," << getAtomicStoreMemoryLimitNum()
		   << ",checkpointPoolLimitNum," << getAtomicCheckpointMemoryLimitNum()
		   << ",chunkCategoryNum," << (uint32_t)getChunkCategoryNum() << ",chunkExpSize,"
		   << (uint32_t)getChunkExpSize() << ",storeMemoryLimit,"
		   << getAtomicStoreMemoryLimit() << ",checkpointMemoryLimit,"
		   << getAtomicCheckpointMemoryLimit() << ",affinitySize,"
		   << getAffinitySize();
	stream << ",shiftableMemRate," << getAtomicShiftableMemRate();
	stream << ",emaHalfLifePeriod," << getAtomicEMAHalfLifePeriod();
	stream << ",storeMemoryColdRate," << getAtomicStoreMemoryColdRate();
	stream << ",cpFileFlushSize," << getAtomicCpFileFlushSize();
	stream << ",cpFileAutoClearCache," << (getAtomicCpFileAutoClearCache() ? "true" : "false");
	return stream.str();
}

bool ChunkManager::Config::setAtomicStoreMemoryLimit(uint64_t memoryLimitByte) {
	assert((memoryLimitByte >> getChunkExpSize()) <= UINT32_MAX);

	int32_t nth = getChunkExpSize() - MIN_CHUNK_EXP_SIZE_;
	uint64_t limitMegaByte = memoryLimitByte / 1024 / 1024;
	if (limitMegaByte > LIMIT_CHUNK_NUM_LIST[nth]) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large value (source=gs_node.json, "
				<< "name=dataStore.storeMemoryLimit, "
				<< "value=" << limitMegaByte << "MB, "
				<< "max=" << LIMIT_CHUNK_NUM_LIST[nth] << "MB)");
	}
	atomicStorePoolLimit_ = memoryLimitByte;
	atomicStorePoolLimitNum_
			= static_cast<uint32_t>(memoryLimitByte >> getChunkExpSize());
	return true;
}

bool ChunkManager::Config::setAtomicCheckpointMemoryLimit(
		uint64_t memoryLimitByte) {
	assert((memoryLimitByte >> getChunkExpSize()) <= UINT32_MAX);

	int32_t nth = getChunkExpSize() - MIN_CHUNK_EXP_SIZE_;
	uint64_t limitMegaByte = memoryLimitByte / 1024 / 1024;
	if (limitMegaByte > LIMIT_CHUNK_NUM_LIST[nth]) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large value (source=gs_node.json, "
				<< "name=dataStore.storeMemoryLimit, "
				<< "value=" << limitMegaByte << "MB, "
				<< "max=" << LIMIT_CHUNK_NUM_LIST[nth] << "MB)");
	}
	atomicCheckpointPoolLimit_ = memoryLimitByte;
	atomicCheckpointPoolLimitNum_
			= static_cast<uint32_t>(memoryLimitByte >> getChunkExpSize());
	return true;
}

bool ChunkManager::Config::setPartitionCompressionMode(
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
					"partition " << pId << " compression mode is inactive. ");
			return false;
		} else {
			partitionCompressionMode_[pId] = BLOCK_COMPRESSION;
			GS_TRACE_INFO(CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
					"partition " << pId << " compression mode is active. ");
		}
	}
	return true;	
}

bool ChunkManager::Config::setAtomicStoreMemoryColdRate(double rate) {
	if (rate > 1.0) {
		rate = 1.0;
	}
	atomicStoreMemoryColdRate_ = static_cast<uint64_t>(rate * 1.0E6);
	return true;
}

bool ChunkManager::Config::setAtomicCpFileFlushSize(uint64_t size) {
	atomicCpFileFlushSize_ = size;
	atomicCpFileFlushInterval_ = size / chunkSize_;
	GS_TRACE_INFO(
			SIZE_MONITOR, GS_TRACE_CHM_CP_FILE_FLUSH_SIZE,
			"setAtomicCpFileFlushSize: size," << atomicCpFileFlushSize_ <<
			",interval," << atomicCpFileFlushInterval_);
	return true;
}
bool ChunkManager::Config::setAtomicCpFileAutoClearCache(bool flag) {
	atomicCpFileAutoClearCache_ = flag;
	return true;
}

std::vector<std::string> ChunkManager::Config::parseCpFileDirList(
		ConfigTable &configTable) {
	std::vector<std::string> dirList;
	try {
		picojson::value v = configTable.get<picojson::value>(
				CONFIG_TABLE_DS_DB_FILE_PATH_LIST);
		if (v.is<picojson::array>()) {
			const picojson::array &list = v.get<picojson::array>();
			for (picojson::array::const_iterator entryIt = list.begin();
				 entryIt != list.end(); ++entryIt) {
				if ((*entryIt).is<std::string>()) {
					const std::string &strVal = (*entryIt).get<std::string>();
					dirList.push_back(strVal);
				}
				else {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_CT_PARAMETER_TYPE_MISMATCH,
							"cpFileDirList must be string array.");
				}
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
	return dirList;
}


void ChunkManager::Config::setCpFileDirList(const char* jsonStr) {
	util::NormalOStringStream oss;
	oss << "{" << "\"dbFilePathList\":" << jsonStr << "}";
	picojson::value jsonValue;
	std::string in = oss.str();
	std::string error;
	picojson::parse(jsonValue, in.begin(), in.end(), &error);

	if (!error.empty()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CT_PARAMETER_INVALID, "Json parse error : " << error.c_str());
	}
	cpFileDirList_.clear();
}

ChunkManager::ChunkManagerStats::ChunkManagerStats() : manager_(NULL) {
}

ChunkManager::ChunkManagerStats::~ChunkManagerStats() {}


uint64_t ChunkManager::ChunkManagerStats::getStoreUse(
	ChunkCategoryId categoryId) const {
	uint64_t chunkNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
		chunkNumSum += metaChunkManager.getPGUseChunkNum(categoryId);
	}
	uint64_t chunkByteSize = chunkNumSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getStoreMemory(
	ChunkCategoryId categoryId) const {
	uint64_t chunkNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkNumSum += bufferManager.getPGUseBufferNum(categoryId);
	}
	uint64_t chunkByteSize = chunkNumSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapRead(
	ChunkCategoryId categoryId) const {
	uint64_t countSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		countSum += bufferManager.getPGSwapReadCount(categoryId);
	}
	return countSum;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapWrite(
	ChunkCategoryId categoryId) const {
	uint64_t countSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		countSum += bufferManager.getPGSwapWriteCount(categoryId);
	}
	return countSum;
}
uint64_t ChunkManager::ChunkManagerStats::getBufferHashCollisionCount() const {
	uint64_t countSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		countSum += bufferManager.getPGHashCollisionCount();
	}
	return countSum;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointFileSize(
	PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	uint64_t chunkNum = checkpointManager.getPGFileNum();
	uint64_t chunkByteSize = chunkNum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointFileAllocateSize(
	PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	uint64_t fileBlcokSizeSum = checkpointManager.getPGFileAllocateSize();
	return fileBlcokSizeSum;
}

uint64_t ChunkManager::ChunkManagerStats::getPGCheckpointChunkCount(PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	return checkpointManager.getPGCheckpointCount();
}
uint64_t ChunkManager::ChunkManagerStats::getPGCheckpointKickCount(PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	return checkpointManager.getPGCheckpointKickCount();
}
uint64_t ChunkManager::ChunkManagerStats::getPGCheckpointKickTime(PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	return checkpointManager.getPGCheckpointKickTime();
}
uint64_t ChunkManager::ChunkManagerStats::getPGCheckpointKickCompressTime(PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	return checkpointManager.getPGCheckpointKickCompressTime();
}
uint64_t ChunkManager::ChunkManagerStats::getPGCheckpointCopyCount(PartitionGroupId pgId) const {
	CheckpointManager& checkpointManager = getCheckpointManager(pgId);
	return checkpointManager.getPGCheckpointCopyCount();
}

uint64_t ChunkManager::ChunkManagerStats::getStoreUse() const {
	uint64_t chunkNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
		chunkNumSum += metaChunkManager.getAtomicPGUseChunkNum();
	}
	uint64_t chunkByteSize = chunkNumSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getBatchFree() const {
	uint64_t countSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
		countSum += metaChunkManager.getPGBatchFreeCount();
	}
	uint64_t chunkByteSize = countSum * getConfig().getChunkSize();
	return chunkByteSize;
}
int64_t ChunkManager::ChunkManagerStats::getStoreAllocateData() const {
	uint64_t sizeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
		sizeSum += metaChunkManager.getPGAllocatedObjectSize();
	}
	return sizeSum;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointMemory() const {
	uint64_t chunkNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		chunkNumSum += checkpointManager.getPGCheckpointBufferNum();
	}
	util::AllocatorStats stats;
	getCheckpointMemoryPool().getStats(stats);
	uint64_t chunkByteSize =
		chunkNumSum * getConfig().getChunkSize() +
		stats.values_[util::AllocatorStats::STAT_CACHE_SIZE];
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getAllocatedCheckpointBufferCount()
	const {
	util::AllocatorStats stats;
	getCheckpointMemoryPool().getStats(stats);
	uint64_t count = stats.values_[util::AllocatorStats::STAT_ALLOCATION_COUNT];
	return count;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointFileSize() const {
	uint64_t chunkNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		chunkNumSum += checkpointManager.getPGFileNum();
	}
	uint64_t chunkByteSize = chunkNumSum * getConfig().getChunkSize();
	return chunkByteSize;
}
double ChunkManager::ChunkManagerStats::getCheckpointFileUsageRate() const {
	uint64_t fileNumSum = 0;
	uint64_t fileUseNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		fileNumSum += checkpointManager.getPGFileNum();
		fileUseNumSum += checkpointManager.getPGFileUseNum();
	}
	double rate = 1.0;
	if (0 < fileNumSum && fileUseNumSum < fileNumSum) {
		rate = static_cast<double>(fileUseNumSum) /
				static_cast<double>(fileNumSum);
	}
	return rate;
}

const char8_t* ChunkManager::ChunkManagerStats::getActualCompressionMode()
	const {
		switch (FileManager::isEnableCompression()) {
			case NO_BLOCK_COMPRESSION:
				return "NO_BLOCK_COMPRESSION";
			case BLOCK_COMPRESSION:
				return "BLOCK_COMPRESSION";
			default:
				return "UNDEF_MODE";
		}
}

uint64_t ChunkManager::ChunkManagerStats::getCheckpointFileAllocateSize()
	const {
	uint64_t fileBlcokSizeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		fileBlcokSizeSum += checkpointManager.getPGFileAllocateSize();
	}
	return fileBlcokSizeSum;
}

uint64_t ChunkManager::ChunkManagerStats::getCheckpointWriteBufferSize() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		chunkCountSum += checkpointManager.getPGCheckpointBufferNum();
	}
	uint64_t chunkByteSize = chunkCountSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointWriteSize() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		chunkCountSum += checkpointManager.getPGCheckpointWriteCount();
	}
	uint64_t chunkByteSize = chunkCountSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getSyncReadSize() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		chunkCountSum += checkpointManager.getPGChunkSyncReadCount();
	}
	uint64_t chunkByteSize = chunkCountSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointWriteTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGCheckpointWriteTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getSyncReadTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGChunkSyncReadTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getStoreMemory() const {
	uint64_t chunkNumSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkNumSum += bufferManager.getPGUseBufferNum();
		chunkNumSum += bufferManager.getPGPoolBufferNum();
	}
	util::AllocatorStats stats;
	getStoreMemoryPool().getStats(stats);
	uint64_t chunkByteSize =
		chunkNumSum * getConfig().getChunkSize() +
		stats.values_[util::AllocatorStats::STAT_CACHE_SIZE];
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getGlobalPoolMemory() const {
	util::AllocatorStats stats;
	getStoreMemoryPool().getStats(stats);
	uint64_t chunkByteSize =
		stats.values_[util::AllocatorStats::STAT_CACHE_SIZE];
	return chunkByteSize;
}

uint64_t ChunkManager::ChunkManagerStats::getGlobalCheckpointPoolMemory()
	const {
	util::AllocatorStats stats;
	getCheckpointMemoryPool().getStats(stats);
	uint64_t chunkByteSize =
		stats.values_[util::AllocatorStats::STAT_CACHE_SIZE];
	return chunkByteSize;
}

uint64_t ChunkManager::ChunkManagerStats::getGlobalPoolAllocateMemory() const {
	util::AllocatorStats stats;
	getStoreMemoryPool().getStats(stats);
	uint64_t chunkByteSize =
		stats.values_[util::AllocatorStats::STAT_TOTAL_SIZE];
	return chunkByteSize;
}

uint64_t
ChunkManager::ChunkManagerStats::getGlobalCheckpointPoolAllocateMemory() const {
	util::AllocatorStats stats;
	getCheckpointMemoryPool().getStats(stats);
	uint64_t chunkByteSize =
		stats.values_[util::AllocatorStats::STAT_TOTAL_SIZE];
	return chunkByteSize;
}

uint64_t ChunkManager::ChunkManagerStats::getSwapRead() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkCountSum += bufferManager.getAtomicPGSwapReadCount();
	}
	return chunkCountSum;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapWrite() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkCountSum += bufferManager.getPGSwapWriteCount();
	}
	return chunkCountSum;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapReadSize() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkCountSum += bufferManager.getAtomicPGSwapReadCount();
	}
	uint64_t chunkByteSize = chunkCountSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapWriteSize() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkCountSum += bufferManager.getPGSwapWriteCount();
	}
	uint64_t chunkByteSize = chunkCountSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getRecoveryReadSize() const {
	uint64_t chunkCountSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkCountSum += bufferManager.getPGRecoveryReadCount();
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		chunkCountSum += checkpointManager.getPGRecoveryReadCount();
	}
	uint64_t chunkByteSize = chunkCountSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapReadTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		timeSum += bufferManager.getPGSwapReadTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapWriteTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		timeSum += bufferManager.getPGSwapWriteTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getRecoveryReadTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		timeSum += bufferManager.getPGRecoveryReadTime();
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGRecoveryReadTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getPGStoreMemoryLimit(
	PartitionGroupId pgId) const {
	BufferManager& bufferManager = getBufferManager(pgId);
	uint64_t limitNum = bufferManager.getAtomicPGMemoryLimitNum();
	uint64_t limitByteSize = limitNum * getConfig().getChunkSize();
	return limitByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getPGStoreMemory(
	PartitionGroupId pgId) const {
	BufferManager& bufferManager = getBufferManager(pgId);
	uint64_t bufferNum = bufferManager.getPGUseBufferNum();
	uint64_t bufferByteSize = bufferNum * getConfig().getChunkSize();
	return bufferByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getPGSwapRead(
		PartitionGroupId pgId) const {
	BufferManager& bufferManager = getBufferManager(pgId);
	return bufferManager.getAtomicPGSwapReadCount();
}
uint64_t ChunkManager::ChunkManagerStats::getPGNormalSwapRead(
		PartitionGroupId pgId) const {
	BufferManager& bufferManager = getBufferManager(pgId);
	return bufferManager.getPGSwapNormalReadCount();
}
uint64_t ChunkManager::ChunkManagerStats::getPGColdBufferingSwapRead(
		PartitionGroupId pgId) const {
	BufferManager& bufferManager = getBufferManager(pgId);
	return bufferManager.getPGSwapColdBufferingReadCount();
}
uint64_t ChunkManager::ChunkManagerStats::getPGStoreUse(
		PartitionGroupId pgId) const {
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
	uint64_t chunkNumSum = 0;
	for (ChunkCategoryId categoryId = 0;
			categoryId < getConfig().getChunkCategoryNum(); categoryId++) {
		chunkNumSum += metaChunkManager.getPGUseChunkNum(categoryId);
	}
	uint64_t chunkByteSize = chunkNumSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getPGCategoryStoreUse(
		PartitionGroupId pgId, ChunkCategoryId categoryId) const {
	uint64_t chunkByteSize = 0;
	if (0 <= categoryId && categoryId < getConfig().getChunkCategoryNum()) {
		MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
		uint64_t chunkNum = metaChunkManager.getPGUseChunkNum(categoryId);
		chunkByteSize = chunkNum * getConfig().getChunkSize();
	}
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getPGCategoryStoreMemory(
		PartitionGroupId pgId, ChunkCategoryId categoryId) const {
	uint64_t chunkNum = 0;
	if (0 <= categoryId && categoryId < getConfig().getChunkCategoryNum()) {
		BufferManager& bufferManager = getBufferManager(pgId);
		chunkNum = bufferManager.getPGUseBufferNum(categoryId);
	}
	uint64_t chunkByteSize = chunkNum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getPGCategorySwapRead(
		PartitionGroupId pgId, ChunkCategoryId categoryId) const {
	uint64_t countSum = 0;
	BufferManager& bufferManager = getBufferManager(pgId);
	if (0 <= categoryId && categoryId < getConfig().getChunkCategoryNum()) {
		countSum = bufferManager.getPGSwapReadCount(categoryId);
	}
	return countSum;
}
uint64_t ChunkManager::ChunkManagerStats::getPGCategorySwapWrite(
		PartitionGroupId pgId, ChunkCategoryId categoryId) const {
	uint64_t countSum = 0;
	BufferManager& bufferManager = getBufferManager(pgId);
	if (0 <= categoryId && categoryId < getConfig().getChunkCategoryNum()) {
		countSum = bufferManager.getPGSwapWriteCount(categoryId);
	}
	return countSum;
}
uint64_t ChunkManager::ChunkManagerStats::getFileFlushCount() const {
	uint64_t countSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		countSum += bufferManager.getPGCpFileFlushCount();
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		countSum += checkpointManager.getPGCpFileFlushCount();
	}
	return countSum;
}
uint64_t ChunkManager::ChunkManagerStats::getFileFlushTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		timeSum += bufferManager.getPGCpFileFlushTime();
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGCpFileFlushTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getCheckpointWriteCompressTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGCheckpointWriteCompressTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getSyncReadUncompressTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGChunkSyncReadUncompressTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getRecoveryReadUncompressTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		CheckpointManager& checkpointManager = getCheckpointManager(pgId);
		timeSum += checkpointManager.getPGRecoveryReadUncompressTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}

uint64_t ChunkManager::ChunkManagerStats::getSwapReadUncompressTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		timeSum += bufferManager.getPGSwapReadUncompressTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}
uint64_t ChunkManager::ChunkManagerStats::getSwapWriteCompressTime() const {
	uint64_t timeSum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		timeSum += bufferManager.getPGSwapWriteCompressTime();
	}
	uint64_t millSecTime = timeSum / 1000;
	return millSecTime;
}

uint64_t ChunkManager::ChunkManagerStats::getCpFileReadRetryCount() const {
	uint64_t sum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		sum += bufferManager.getPGCpFileReadRetryCount();
	}
	return sum;
}
uint64_t ChunkManager::ChunkManagerStats::getCpFileWriteRetryCount() const {
	uint64_t sum = 0;
	for (PartitionGroupId pgId = 0; pgId < getConfig().getPartitionGroupNum();
		 pgId++) {
		BufferManager& bufferManager = getBufferManager(pgId);
		sum += bufferManager.getPGCpFileWriteRetryCount();
	}
	return sum;
}

uint64_t ChunkManager::ChunkManagerStats::getStoreUse(PartitionId pId, ChunkCategoryId categoryId) const {
	PartitionGroupId pgId = manager_->getPartitionGroupId(pId);
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
	uint64_t chunkNumSum = metaChunkManager.getUseChunkNum(pId, categoryId);
	uint64_t chunkByteSize = chunkNumSum * getConfig().getChunkSize();
	return chunkByteSize;
}
uint64_t ChunkManager::ChunkManagerStats::getStoreObjectUse(PartitionId pId, ChunkCategoryId categoryId) const {
	PartitionGroupId pgId = manager_->getPartitionGroupId(pId);
	MetaChunkManager& metaChunkManager = getMetaChunkManager(pgId);
	uint64_t storeObjectUse = metaChunkManager.getStoreObjectUseSize(pId, categoryId);
	return storeObjectUse;
}

ChunkManager::ChunkManagerStats::StatSetUpHandler
	ChunkManager::ChunkManagerStats::statSetUpHandler_;

#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)
#define STAT_ADD_SUB_SUB(id) STAT_TABLE_ADD_PARAM_SUB_SUB(stat, parentId, id)

#define CHUNK_STAT_ADD_CATEGORY(id) \
	stat.resolveGroup(parentId, id, STAT_TABLE_EXTRACT_SYMBOL(id, 5));
#define CHUNK_STAT_ADD_PARAM(id)                           \
	stat.addParam(parentId, getStoreParamId(parentId, id), \
		STAT_TABLE_EXTRACT_SYMBOL(id, 5));

void ChunkManager::ChunkManagerStats::StatSetUpHandler::operator()(
	StatTable& stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_STORE_MEMORY_LIMIT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_STORE_MEMORY);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_STORE_TOTAL_USE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_ALLOCATE_DATA);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_BATCH_FREE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ_SIZE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE_SIZE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SYNC_READ_SIZE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SYNC_READ_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_RECOVERY_READ_SIZE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_RECOVERY_READ_TIME);

	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_DETAIL_POOL_BUFFER_MEMORY);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_DETAIL_POOL_CHECKPOINT_MEMORY);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_BUFFER_HASH_COLLISION_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ_UNCOMPRESS_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE_COMPRESS_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SYNC_READ_UNCOMPRESS_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_RECOVERY_READ_UNCOMPRESS_TIME);
	stat.resolveGroup(parentId, STAT_TABLE_PERF_DS_DETAIL, "storeDetail");

	parentId = STAT_TABLE_PERF_DS_DETAIL;
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_META_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_MAP_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_ROW_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_BATCH_FREE_MAP_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_BATCH_FREE_ROW_DATA);

	for (int32_t parentId = STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START + 1;
		 parentId < STAT_TABLE_PERF_DS_DETAIL_CATEGORY_END; parentId++) {
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_STORE_MEMORY);
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_STORE_USE);
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_SWAP_READ);
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_SWAP_WRITE);
	}
}

StatTableParamId ChunkManager::ChunkManagerStats::getStoreParamId(
	int32_t parentId, StatTableParamId id) {
	const int32_t categoryIndex =
		(parentId - STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START - 1);
	const int32_t categoryCount = (STAT_TABLE_PERF_DS_DETAIL_PARAM_END -
								   STAT_TABLE_PERF_DS_DETAIL_PARAM_START - 1);

	return static_cast<StatTableParamId>(
		(STAT_TABLE_PERF_DS_DETAIL_ALL_START + 1) +
		categoryCount * categoryIndex +
		(id - STAT_TABLE_PERF_DS_DETAIL_PARAM_START - 1));
}

bool ChunkManager::ChunkManagerStats::operator()(StatTable& stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	stat.set(STAT_TABLE_PERF_DS_STORE_MEMORY, getStoreMemory());

	stat.set(STAT_TABLE_PERF_DS_STORE_TOTAL_USE, getStoreUse());

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_DS)) {
		stat.set(STAT_TABLE_PERF_DS_ALLOCATE_DATA, getStoreAllocateData());
	}

	stat.set(STAT_TABLE_PERF_DS_BATCH_FREE, getBatchFree());

	stat.set(STAT_TABLE_PERF_DS_SWAP_READ, getSwapRead());

	stat.set(STAT_TABLE_PERF_DS_SWAP_WRITE, getSwapWrite());

	stat.set(STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_COUNT, getFileFlushCount());
	stat.set(STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_TIME, getFileFlushTime());
	stat.set(STAT_TABLE_PERF_DS_BUFFER_HASH_COLLISION_COUNT, getBufferHashCollisionCount());
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE)) {
		stat.set(STAT_TABLE_PERF_DS_SYNC_READ_SIZE, getSyncReadSize());

		stat.set(STAT_TABLE_PERF_DS_RECOVERY_READ_SIZE, getRecoveryReadSize());

		for (int32_t baseId = STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START + 1;
			 baseId < STAT_TABLE_PERF_DS_DETAIL_CATEGORY_END; baseId++) {
			const ChunkCategoryId categoryId =
				static_cast<ChunkCategoryId>(
					baseId - (STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START + 1));
			int32_t id =
				getStoreParamId(baseId, STAT_TABLE_PERF_DS_DETAIL_PARAM_START);
			stat.set(++id, getStoreMemory(categoryId));
			stat.set(++id, getStoreUse(categoryId));
			stat.set(++id, getSwapRead(categoryId));
			stat.set(++id, getSwapWrite(categoryId));
		}
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		stat.set(STAT_TABLE_PERF_DS_STORE_MEMORY_LIMIT,
			getConfig().getAtomicStoreMemoryLimit());

		stat.set(STAT_TABLE_PERF_DS_SWAP_READ_SIZE, getSwapReadSize());

		stat.set(STAT_TABLE_PERF_DS_SWAP_READ_TIME, getSwapReadTime());

		stat.set(STAT_TABLE_PERF_DS_SWAP_WRITE_SIZE, getSwapWriteSize());

		stat.set(STAT_TABLE_PERF_DS_SWAP_WRITE_TIME, getSwapWriteTime());

		stat.set(STAT_TABLE_PERF_DS_SYNC_READ_TIME, getSyncReadTime());

		stat.set(STAT_TABLE_PERF_DS_RECOVERY_READ_TIME, getRecoveryReadTime());

		stat.set(STAT_TABLE_PERF_DS_SWAP_READ_UNCOMPRESS_TIME, getSwapReadUncompressTime());
		stat.set(STAT_TABLE_PERF_DS_SWAP_WRITE_COMPRESS_TIME, getSwapWriteCompressTime());
		stat.set(STAT_TABLE_PERF_DS_SYNC_READ_UNCOMPRESS_TIME, getSyncReadUncompressTime());
		stat.set(STAT_TABLE_PERF_DS_RECOVERY_READ_UNCOMPRESS_TIME, getRecoveryReadUncompressTime());
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_DS)) {
		stat.set(STAT_TABLE_PERF_DS_DETAIL_POOL_BUFFER_MEMORY,
			getGlobalPoolAllocateMemory());

		stat.set(STAT_TABLE_PERF_DS_DETAIL_POOL_CHECKPOINT_MEMORY,
			getGlobalCheckpointPoolAllocateMemory());
	}

	return true;
}

ChunkManager::BufferInfo::BufferInfo(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId)
	: nextCollision_(NULL),
	  pId_(pId),
	  cId_(cId),
	  buffer_(NULL),
	  refCount_(0),
	  lastRefId_(0),
	  lruNext_(NULL),
	  lruPrev_(NULL),
	  latestPos_(-1),
	  newCheckpointPos_(-1),
	  isDirty_(false),
	  toCopy_(false),
	  onHotList_(false),
	  categoryId_(categoryId) {}
ChunkManager::BufferInfo::BufferInfo()
	: nextCollision_(NULL),
	  pId_(UNDEF_PARTITIONID),
	  cId_(UNDEF_CHUNKID),
	  buffer_(NULL),
	  refCount_(0),
	  lastRefId_(0),
	  lruNext_(NULL),
	  lruPrev_(NULL),
	  latestPos_(-1),
	  newCheckpointPos_(-1),
	  isDirty_(false),
	  toCopy_(false),
	  onHotList_(false),
	  categoryId_(-1) {}
ChunkManager::BufferInfoState ChunkManager::BufferInfo::getState() {
	if (isDirty_) {
		if (buffer_) {
			if (latestPos_ == -1) {
				return BUFFER_MANAGER_STATE_DIRTY_BUFFER;  
			}
			else {
				return BUFFER_MANAGER_STATE_DIRTY_BOTH;  
			}
		}
		else {
			return BUFFER_MANAGER_STATE_DIRTY_FILE;  
		}
	}
	else {
		if (toCopy_) {
			return BUFFER_MANAGER_STATE_RESERVED;  
		}
		else {
			return BUFFER_MANAGER_STATE_CLEAN_BUFFER;  
		}
	}
}
bool ChunkManager::BufferInfo::isValid(BufferInfoState type) {
	switch (type) {
	case BUFFER_MANAGER_STATE_DIRTY_BUFFER:
		return (isDirty_) && (buffer_) && (latestPos_ == -1) && (!toCopy_);
		break;
	case BUFFER_MANAGER_STATE_DIRTY_BOTH:
		return (isDirty_) && (buffer_) && (latestPos_ != -1) && (!toCopy_);
		break;
	case BUFFER_MANAGER_STATE_DIRTY_FILE:
		return (isDirty_) && (!buffer_) && (latestPos_ != -1) && (!toCopy_);
		break;
	case BUFFER_MANAGER_STATE_RESERVED:
		return (!isDirty_) && (buffer_) && (latestPos_ == -1) &&
			   (newCheckpointPos_ != -1) && (toCopy_);
		break;
	case BUFFER_MANAGER_STATE_CLEAN_BUFFER:
		return (!isDirty_) && (buffer_) && (latestPos_ == -1) && (!toCopy_);
		break;
	default:
		return false;
	}
	return true;
}
std::string ChunkManager::BufferInfo::dump() {
	util::NormalOStringStream stream;
	stream << "BufferInfo";
	if (nextCollision_) {
		stream << ",nextCollision," << nextCollision_;
	}
	else {
		stream << ",next,NULL";
	}
	stream << ",pId," << pId_ << ",categoryId," << (int32_t)categoryId_
		   << ",cId," << cId_;
	if (buffer_) {
		stream << ",buffer," << static_cast<void*>(buffer_);
	}
	else {
		stream << ",buffer,NULL";
	}
	stream << ",refCount_," << refCount_ << ",lastRefId_," << lastRefId_;
	if (lruNext_) {
		stream << ",lruNext," << lruNext_;
	}
	else {
		stream << ",lruNext,NULL";
	}
	if (lruPrev_) {
		stream << ",lruPrev," << lruPrev_;
	}
	else {
		stream << ",lruPrev,NULL";
	}
	stream << ",latestPos," << latestPos_ << ",newCheckpointPos,"
		   << newCheckpointPos_ << ",isDirty," << isDirty_ << ",toCopy,"
		   << toCopy_;

	stream << ",BufferInfoState";
	if (isValid(BUFFER_MANAGER_STATE_DIRTY_BUFFER)) {
		stream << ",BUFFER_MANAGER_STATE_DIRTY_BUFFER";
	}
	else if (isValid(BUFFER_MANAGER_STATE_DIRTY_BOTH)) {
		stream << ",BUFFER_MANAGER_STATE_DIRTY_BOTH";
	}
	else if (isValid(BUFFER_MANAGER_STATE_DIRTY_FILE)) {
		stream << ",BUFFER_MANAGER_STATE_DIRTY_FILE";
	}
	else if (isValid(BUFFER_MANAGER_STATE_RESERVED)) {
		stream << ",BUFFER_MANAGER_STATE_RESERVED";
	}
	else if (isValid(BUFFER_MANAGER_STATE_CLEAN_BUFFER)) {
		stream << ",BUFFER_MANAGER_STATE_CLEAN_BUFFER";
	}
	else {
		stream << ",BUFFER_MANAGER_STATE_UNKNOWN";
	}
	return stream.str();
}
ChunkManager::MetaChunk::MetaChunk()
	: base_(NULL), buffer_(NULL), bufferInfo_(NULL), metaChunkManager_(NULL) {}
ChunkManager::MetaChunk::~MetaChunk() {}
bool ChunkManager::MetaChunk::isValid() const {
	bool existBufferPtr = (buffer_ != NULL);
	bool existMetaChunkManager = (metaChunkManager_ != NULL);
	return existBufferPtr && existMetaChunkManager;
}
std::string ChunkManager::MetaChunk::dump(bool isFull) {
	util::NormalOStringStream stream;
	stream << base_->dump().c_str();
	if (isFull && buffer_) {
		stream << ChunkHeader::dump(buffer_).c_str();
	}
	else {
		stream << "buffer,NULL";
	}
	stream << ",allocatedObjectSize,"
		   << metaChunkManager_->getPGAllocatedObjectSize();
	return stream.str();
}

void ChunkManager::ChunkHeader::validateHeader(
		const uint8_t* data,
		PartitionId expectedPartitionNum, uint8_t expectedChunkExpSize,
		uint32_t expectedSplitCount, uint32_t expectedSplitStripeSize,
		bool checkSplitInfo,
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
	static_cast<void>(checkCheckSum);
	if (checkSplitInfo) {
		uint32_t splitCount = getCpFileSplitCount(data);
		if (expectedSplitCount != splitCount) {
			GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_SPLIT_COUNT,
					"splitCount=" << splitCount
					<< ", expected=" << expectedSplitCount);
		}
		uint32_t splitStripeSize = getCpFileSplitStripeSize(data);
		if (expectedSplitStripeSize != splitStripeSize) {
			GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_SPLIT_STRIPE_SIZE,
					"splitStripeSize=" << splitStripeSize
					<< ", expected=" << expectedSplitStripeSize);
		}
	}
}

std::string ChunkManager::ChunkHeader::dumpFieldName() {
	util::NormalOStringStream stream;

	stream << "checkSum,"
			  "checkMagic,"
			  "version ,"

			  "partitionGroupNum,"
			  "partitionNum,"
			  "partitionGroupId,"
			  "partitionId ,"

			  "chunkId,"
			  "chunkExpSize,"
			  "attribute,"
			  "categoryNum,"
			  "categoryId,"
			  "chunkKey ,"

			  "unoccupiedSize,"
			  "occupiedSize,"
			  "SwapCpId,"
			  "CPCpId, ";

	return stream.str();
}
std::string ChunkManager::ChunkHeader::dump(const uint8_t* chunk) {
	util::NormalOStringStream stream;

	stream << ChunkHeader::getCheckSum(chunk) << ","
		   << ChunkHeader::checkMagicNum(chunk) << ","
		   << ChunkHeader::getVersion(chunk) << ", " <<

		ChunkHeader::getPartitionGroupNum(chunk) << ","
		   << ChunkHeader::getPartitionNum(chunk) << ","
		   << ChunkHeader::getPartitionGroupId(chunk) << ","
		   << ChunkHeader::getPartitionId(chunk) << ", " <<

		ChunkHeader::getChunkId(chunk) << ","
		   << (int32_t)ChunkHeader::getChunkExpSize(chunk) << ","
		   << (int32_t)ChunkHeader::getAttribute(chunk) << ","
		   << (int32_t)ChunkHeader::getChunkCategoryNum(chunk) << ","
		   << (int32_t)ChunkHeader::getChunkCategoryId(chunk) << ","
		   << ChunkHeader::getChunkKey(chunk) << ", " <<

		(int32_t)ChunkHeader::getUnoccupiedSize(chunk) << ","
		   << ChunkHeader::getOccupiedSize(chunk) << ",";

	if (getSwapCpId(chunk) != UNDEF_CHECKPOINT_ID) {
		stream << ChunkHeader::getSwapCpId(chunk) << ",";
	}
	else {
		stream << "-1,";
	}
	if (ChunkHeader::getCPCpId(chunk) != UNDEF_CHECKPOINT_ID) {
		stream << ChunkHeader::getCPCpId(chunk) << ", ";
	}
	else {
		stream << "-1,";
	}

	return stream.str();
}

void ChunkManager::ChunkHeader::dumpHeader(
		const uint8_t* data, size_t dumpSize, bool hexDump) {
	std::cerr <<
		  "ChunkHeader: chunkExpSize," << (int32_t)ChunkManager::ChunkHeader::getChunkExpSize(data) <<
				  ",pgId," << ChunkManager::ChunkHeader::getPartitionGroupId(data) <<
				  ",pId," << ChunkManager::ChunkHeader::getPartitionId(data) <<
				  ",categoryId," << (int32_t)ChunkManager::ChunkHeader::getChunkCategoryId(data) <<
				  ",cId," << ChunkManager::ChunkHeader::getChunkId(data) <<
					std::endl;

	if (hexDump) {
		int64_t count = 0;

		std::cerr << "addr=0x" << std::setw(8) << std::setfill('0') << std::hex
		  << std::nouppercase << (uintptr_t)data << std::endl;
		const uint8_t* startAddr = data;
		const uint8_t* addr = startAddr;
		for (; addr < startAddr + dumpSize; ++addr) {
			if (count % 16 == 0) {
				std::cerr << std::setw(8) << std::setfill('0') << std::hex
				  << std::nouppercase << (uintptr_t)(addr - startAddr) << " ";
			}
			std::cerr << std::setw(2) << std::setfill('0') << std::hex
			  << std::nouppercase << (uint32_t)(*addr) << " ";
			
			if (count % 16 == 15) {
				std::cerr << std::endl;
			}
			++count;
		}
		std::cerr << std::dec << std::endl;
	}
}

std::string ChunkManager::BaseMetaChunk::dump() {
	util::NormalOStringStream stream;
	stream << "BaseMetaChunk"
		   << ",checkpointPos," << getCheckpointPos()
		   << ",unoccupiedSize," << (int32_t)getUnoccupiedSize()
		   << ",isAffinityUse," << isAffinityUse()
		   << ",cumulativeDirtyFlag," << getCumulativeDirtyFlag()
		   << ",differentialDirtyFlag," << getDifferentialDirtyFlag();
	return stream.str();
}

const char* const ChunkManager::ChunkOperationTypeString[] = {
	"ALLOCATE_CHUNK", "FREE_CHUNK",
	"GET_CHUNK_FOR_UPDATE", "GET_CHUNK", "SEARCH_CHUNK",
	"CHECKPOINT_CHUNK", "COPY_CHUNK"};

ChunkManager::PartitionMetaChunk::PartitionMetaChunk(
		ChunkCategoryId chunkCategoryNum, PartitionGroupAllocator& allocator,
		ChunkManager *chunkManager)
	: chunkCategoryNum_(chunkCategoryNum),
	chunkList_(), chunkKeyList_() {
	try {
		for (ChunkCategoryId categoryId = 0; categoryId < chunkCategoryNum_;
			 categoryId++) {
			categoryStats_.push_back(MetaChunkStats());
			chunkList_.push_back(UTIL_NEW ChunkList(allocator));
			chunkKeyList_.push_back(UTIL_NEW ChunkKeyList(allocator));
			assert(chunkManager);
			const bool isBatchFreeMode =
					(chunkManager ?
					 chunkManager->isBatchFreeMode(categoryId) : true);
			for (uint32_t i = 0; i < INIT_CHUNK_RESERVE_NUM; i++) {
				chunkList_[categoryId]->push_back(BaseMetaChunk());
				if (isBatchFreeMode) {
					chunkKeyList_[categoryId]->push_back(MIN_CHUNK_KEY);
				}
			}
			ChunkCursor chunkCursor(categoryId, 0);
			emptyScanCursor_.push_back(chunkCursor);
			nextTailCursor_.push_back(chunkCursor);
			checkpointTailCursor_.push_back(chunkCursor);
		}
	}
	catch (std::exception& e) {
		std::vector<ChunkList*>::iterator chunkItr = chunkList_.begin();
		for (; chunkItr != chunkList_.end(); ++chunkItr) {
			delete (*chunkItr);
		}
		chunkList_.clear();
		std::vector<ChunkKeyList*>::iterator chunkKeyItr = chunkKeyList_.begin();
		for (; chunkKeyItr != chunkKeyList_.end(); ++chunkKeyItr) {
			delete (*chunkKeyItr);
		}
		chunkKeyList_.clear();
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
ChunkManager::PartitionMetaChunk::~PartitionMetaChunk() {
	std::vector<ChunkList*>::iterator chunkItr = chunkList_.begin();
	for (; chunkItr != chunkList_.end(); ++chunkItr) {
		delete (*chunkItr);
	}
	chunkList_.clear();
	std::vector<ChunkKeyList*>::iterator chunkKeyItr = chunkKeyList_.begin();
	for (; chunkKeyItr != chunkKeyList_.end(); ++chunkKeyItr) {
		delete (*chunkKeyItr);
	}
	chunkKeyList_.clear();
}

ChunkManager::MetaChunkManager::MetaChunkManager(PartitionGroupId pgId,
	ActivePartitionList& pIdList, PartitionDataList& partitionList,
	Config& config)
	: MAX_CHUNK_ID(ChunkManager::calcMaxChunkId(config.getChunkSize())),
	  config_(config),
	  pgId_(pgId),
	  pgStats_(),
	  pIdList_(pIdList),
	  partitionList_(partitionList) {}

ChunkManager::MetaChunkManager::~MetaChunkManager() {
	return;
}
void ChunkManager::MetaChunkManager::expand(
	PartitionId pId, ChunkCursor& nextTail, ChunkId cId) {
	ChunkList& chunkList = getChunkList(pId, nextTail.categoryId_);
	ChunkKeyList& chunkKeyList = getChunkKeyList(pId, nextTail.categoryId_);
	ChunkId targetCId = cId;

	if (targetCId == UNDEF_CHUNKID) {
		targetCId = nextTail.cId_;
		nextTail.cId_++;
	}
	else if (nextTail.cId_ <= targetCId) {
		nextTail.cId_ = targetCId + 1;
	}
	assert(targetCId < nextTail.cId_);

	if (MAX_CHUNK_ID < targetCId) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_CID_LIMIT_OVER,
			"pId," << pId << ",categoryId, " << (int32_t)nextTail.categoryId_
				   << ",cId," << targetCId);
	}

	if (static_cast<size_t>(targetCId) < chunkList.size()) {
		assert(chunkList[targetCId].getUnoccupiedSize() == UNDEF_UNOCCUPIED_SIZE_);
		assert((chunkKeyList.size() > 0) ?
				static_cast<size_t>(targetCId) < chunkKeyList.size() : true);
		assert(isValid(pId, nextTail.categoryId_));
		assert(static_cast<size_t>(nextTail.cId_) <= chunkList.size());
		return;
	}

	uint32_t newSize = static_cast<uint32_t>(chunkList.size());
	uint32_t targetSize = static_cast<uint32_t>(targetCId) + 1;
	if (targetSize < ChunkManager::MAX_CHUNK_EXPAND_COUNT) {
		newSize = util::nextPowerOf2(targetSize);
	}
	else {
		uint32_t quotient = targetSize / ChunkManager::MAX_CHUNK_EXPAND_COUNT;
		newSize = (quotient + 1) * ChunkManager::MAX_CHUNK_EXPAND_COUNT;
	}
	assert(newSize >= (static_cast<uint32_t>(targetCId) + 1));
	if (static_cast<uint32_t>(MAX_CHUNK_ID + 1) < newSize) {
		newSize = MAX_CHUNK_ID + 1;
	}
	assert(static_cast<uint32_t>(chunkList.size()) < newSize);

	try {
		chunkList.resize(newSize);  
		if (chunkKeyList.size() > 0) {
			chunkKeyList.resize(newSize, MIN_CHUNK_KEY);  
		}
		assert(chunkList.size() == newSize);
		assert(chunkList[chunkList.size() - 1].getUnoccupiedSize() ==
			   UNDEF_UNOCCUPIED_SIZE_);
		assert(isValid(pId, nextTail.categoryId_));
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
void ChunkManager::MetaChunkManager::contract(
	PartitionId pId, ChunkCursor& nextTail) {
	if (getPartition(pId).categoryStats_[nextTail.categoryId_].useChunkNum_
			== 0) {
		nextTail.cId_ = 0;
	}
	else {
		uint64_t searchNum = nextTail.cId_;
		if (MAX_EMPTY_CHUNK_SEARCH_NUM_ < searchNum) {
			searchNum = MAX_EMPTY_CHUNK_SEARCH_NUM_;
		}

		ChunkList& chunkList = getChunkList(pId, nextTail.categoryId_);
		assert(
			static_cast<size_t>(nextTail.cId_) == chunkList.size() ||
			chunkList[nextTail.cId_].getUnoccupiedSize() == UNDEF_UNOCCUPIED_SIZE_);
		assert(0 < nextTail.cId_);
		assert(chunkList[nextTail.cId_ - 1].getUnoccupiedSize()
				== UNDEF_UNOCCUPIED_SIZE_);
		nextTail.cId_--;

		for (uint64_t count = 0; count < searchNum && 0 < nextTail.cId_;
			 count++) {
			if (chunkList[nextTail.cId_ - 1].getUnoccupiedSize()
					== UNDEF_UNOCCUPIED_SIZE_) {
				nextTail.cId_--;
			}
			else {
				break;  
			}
		}
	}

	ChunkCursor& emptyScanCursor =
		getEmptyScanCursor(pId, nextTail.categoryId_);
	if (nextTail.cId_ <= emptyScanCursor.cId_) {
		emptyScanCursor.cId_ = 0;
	}

	ChunkCursor& batchFreeCursor = getBatchFreeCursor(pId);
	if (nextTail.categoryId_ == batchFreeCursor.categoryId_ &&
		nextTail.cId_ <= batchFreeCursor.cId_) {
		batchFreeCursor.categoryId_++;
		batchFreeCursor.categoryId_ =
			static_cast<ChunkCategoryId>(
				batchFreeCursor.categoryId_ % config_.getChunkCategoryNum());
		batchFreeCursor.cId_ = 0;
	}
}
ChunkManager::BaseMetaChunk* ChunkManager::MetaChunkManager::allocate(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		ChunkKey*& metaChunkKey) {
	ChunkList& chunkList = getChunkList(pId, categoryId);

	assert(static_cast<size_t>(cId) < chunkList.size());

	pgStats_.atomicUseChunkNum_++;
	getPartition(pId).categoryStats_[categoryId].useChunkNum_++;
	chunkList[cId].reuse(static_cast<uint8_t>(config_.getChunkExpSize() - 0x01));
	metaChunkKey = NULL;
	ChunkKeyList& chunkKeyList = getChunkKeyList(pId, categoryId);
	if (cId < static_cast<ChunkId>(chunkKeyList.size())) {
		chunkKeyList[cId] = MIN_CHUNK_KEY;
		metaChunkKey = &chunkKeyList[cId];
	}
	assert(0 < getNextTail(pId, categoryId).cId_);
	return &chunkList[cId];
}
void ChunkManager::MetaChunkManager::free(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	ChunkList& chunkList = getChunkList(pId, categoryId);
	assert(static_cast<size_t>(cId) < chunkList.size());
	chunkList[cId].setUnoccupiedSize(UNDEF_UNOCCUPIED_SIZE_);  
	pgStats_.atomicUseChunkNum_--;
	assert(0 < getPartition(pId).categoryStats_[categoryId].useChunkNum_);
	getPartition(pId).categoryStats_[categoryId].useChunkNum_--;

	ChunkCursor& nextTail = getNextTail(pId, categoryId);
	if (cId + 1 == nextTail.cId_ ||
		getPartition(pId).categoryStats_[categoryId].useChunkNum_ == 0) {
		contract(pId, nextTail);
	}

	assert(static_cast<size_t>(getNextTail(pId, categoryId).cId_) <= chunkList.size());
	assert(static_cast<size_t>(getEmptyScanCursor(pId, categoryId).cId_) <= chunkList.size());
	assert(getPartition(pId).batchFreeCursor_.categoryId_ != categoryId ||
		   static_cast<size_t>(getPartition(pId).batchFreeCursor_.cId_) <= chunkList.size());
}
ChunkManager::BaseMetaChunk* ChunkManager::MetaChunkManager::reallocate(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		ChunkKey*& metaChunkKey) {
	assert(cId <= MAX_CHUNK_ID);

	ChunkCursor& nextTail = getNextTail(pId, categoryId);
	if (nextTail.cId_ <= cId) {
		expand(pId, nextTail, cId);
	}

	return allocate(pId, categoryId, cId, metaChunkKey);
}

uint64_t ChunkManager::MetaChunkManager::getMaxChunkId() {
	return MAX_CHUNK_ID;
}
uint64_t ChunkManager::MetaChunkManager::getPGUseChunkNum(
	ChunkCategoryId categoryId) {
	uint64_t sumNum = 0;
	ActivePartitionList::iterator pIdItr = pIdList_.begin();
	for (; pIdItr != pIdList_.end(); pIdItr++) {
		PartitionId pId = *pIdItr;
		sumNum += getPartition(pId).categoryStats_[categoryId].useChunkNum_;
	}
	return sumNum;
}
uint64_t ChunkManager::MetaChunkManager::getAtomicPGUseChunkNum() const {
	return pgStats_.atomicUseChunkNum_;
}
uint64_t ChunkManager::MetaChunkManager::getPGCheckpointChunkNum() const {
	return pgStats_.checkpointChunkNum_;
}
uint64_t ChunkManager::MetaChunkManager::getPGBatchFreeCount() const {
	return pgStats_.batchFreeCount_;
}
void ChunkManager::MetaChunkManager::addPGAllocatedObjectSize(int64_t size) {
	assert(0 < size);
	pgStats_.objectAllocateSize_ += size;
}
void ChunkManager::MetaChunkManager::subtractPGAllocatedObjectSize(
	int64_t size) {
	assert(0 < size);
	pgStats_.objectAllocateSize_ -= size;
}
uint64_t ChunkManager::MetaChunkManager::getPGAllocatedObjectSize() const {
	return pgStats_.objectAllocateSize_;
}
uint64_t ChunkManager::MetaChunkManager::getTotalListElementNum(
	PartitionId pId) {
	uint64_t num = 0;
	for (ChunkCategoryId categoryId = 0; categoryId < config_.getChunkCategoryNum();
		 categoryId++) {
		num += getPartition(pId).chunkList_[categoryId]->size();
	}
	return num;
}
uint64_t ChunkManager::MetaChunkManager::getUseChunkNum(PartitionId pId, ChunkCategoryId categoryId) {
	return getPartition(pId).categoryStats_[categoryId].useChunkNum_;
}
uint64_t ChunkManager::MetaChunkManager::getStoreObjectUseSize(PartitionId pId, ChunkCategoryId categoryId) {
	return getPartition(pId).categoryStats_[categoryId].storeObjectUseSize_;
}
void ChunkManager::MetaChunkManager::setStoreObjectUseSize(PartitionId pId, ChunkCategoryId categoryId, uint64_t size) {
	getPartition(pId).categoryStats_[categoryId].storeObjectUseSize_ = size;
}
ChunkManager::ChunkCursor& ChunkManager::MetaChunkManager::getBatchFreeCursor(
	PartitionId pId) {
	return getPartition(pId).batchFreeCursor_;
}
ChunkManager::BaseMetaChunk* ChunkManager::MetaChunkManager::begin(
		PartitionId pId, ChunkCursor& cursor,
		ChunkKey*& metaChunkKey) {
	ChunkList& chunkList = getChunkList(pId, cursor.categoryId_);
	ChunkKeyList& chunkKeyList =
			getChunkKeyList(pId, cursor.categoryId_);
	if (cursor.cId_ < static_cast<ChunkId>(chunkKeyList.size())) {
		metaChunkKey = &chunkKeyList[cursor.cId_];
	}
	else {
		metaChunkKey = NULL;
	}
	return &chunkList[cursor.cId_];
}
ChunkManager::BaseMetaChunk* ChunkManager::MetaChunkManager::next(
		PartitionId pId, ChunkCursor& cursor,
		bool& isHead, ChunkKey*& metaChunkKey) {
	isHead = false;
	cursor.cId_++;
	ChunkList& chunkList = getChunkList(pId, cursor.categoryId_);
	if (static_cast<ChunkId>(chunkList.size()) <= cursor.cId_) {
		cursor.cId_ = 0;
		cursor.categoryId_++;
		if (config_.getChunkCategoryNum() <= cursor.categoryId_) {
			cursor.categoryId_ = 0;
			isHead = true;
		}
		ChunkKeyList& chunkKeyList =
				getChunkKeyList(pId, cursor.categoryId_);
		if (cursor.cId_ < static_cast<ChunkId>(chunkKeyList.size())) {
			metaChunkKey = &chunkKeyList[cursor.cId_];
		}
		else {
			metaChunkKey = NULL;
		}
		ChunkList& nextForCategoryChunkList =
				getChunkList(pId, cursor.categoryId_);
		return &nextForCategoryChunkList[cursor.cId_];
	}
	else {
		ChunkKeyList& chunkKeyList =
				getChunkKeyList(pId, cursor.categoryId_);
		if (cursor.cId_ < static_cast<ChunkId>(chunkKeyList.size())) {
			metaChunkKey = &chunkKeyList[cursor.cId_];
		}
		else {
			metaChunkKey = NULL;
		}
		return &chunkList[cursor.cId_];
	}
}

ChunkManager::BaseMetaChunk* ChunkManager::MetaChunkManager::nextForCategory(
		PartitionId pId, ChunkCursor& cursor, bool& isHead, ChunkKey* &chunkKey) {
	isHead = false;
	cursor.cId_++;
	ChunkList& chunkList = getChunkList(pId, cursor.categoryId_);
	ChunkKeyList& chunkKeyList =
			getChunkKeyList(pId, cursor.categoryId_);
	if (static_cast<ChunkId>(chunkList.size()) <= cursor.cId_) {
		cursor.cId_ = 0;
		isHead = true;
		if (cursor.cId_ < static_cast<ChunkId>(chunkKeyList.size())) {
			chunkKey = &chunkKeyList[cursor.cId_];
		}
		else {
			chunkKey = NULL;
		}
		ChunkList& nextForCategoryChunkList =
			getChunkList(pId, cursor.categoryId_);
		return &nextForCategoryChunkList[cursor.cId_];
	}
	else {
		if (cursor.cId_ < static_cast<ChunkId>(chunkKeyList.size())) {
			chunkKey = &chunkKeyList[cursor.cId_];
		}
		else {
			chunkKey = NULL;
		}
		return &chunkList[cursor.cId_];
	}
}
bool ChunkManager::MetaChunkManager::isValid(
	PartitionId pId, ChunkCategoryId categoryId) {
	ChunkList& chunkList = getChunkList(pId, categoryId);
	ChunkCursor& nextTail = getNextTail(pId, categoryId);
	ChunkCursor& emptyScanCursor = getEmptyScanCursor(pId, categoryId);
	ChunkCursor& batchFreeCursor = getPartition(pId).batchFreeCursor_;
	bool isValidNextTailCursor = (nextTail.cId_ <= (ChunkId)chunkList.size());
	bool isValidEmptyScanCursor =
		((emptyScanCursor.cId_ < nextTail.cId_) ||
			(nextTail.cId_ == 0 && emptyScanCursor.cId_ == nextTail.cId_));
	bool isValidBatchFreeCursor =
		(batchFreeCursor.categoryId_ != categoryId ||
			(batchFreeCursor.cId_ < nextTail.cId_) ||
			(nextTail.cId_ == 0 && batchFreeCursor.cId_ == nextTail.cId_));

	return isValidNextTailCursor && isValidEmptyScanCursor &&
		   isValidBatchFreeCursor;
}



ChunkManager::Compressor::Compressor()
: isFirstTime_(true), compressionErrorCount_(0) {
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

uint64_t ChunkManager::Compressor::getCompressionErrorCount() const {
	return compressionErrorCount_;
}

void ChunkManager::Compressor::compressData(
	uint8_t* src, uint32_t srcSize, uint8_t* dest, 
	uint32_t &destSize) 
{
	int32_t flush = Z_FINISH;

	deflateStream_.zalloc = Z_NULL;
	deflateStream_.zfree = Z_NULL;
	deflateStream_.opaque = Z_NULL;

	EXEC_FAILURE(GS_ERROR_CHM_COMPRESSION_FAILED);
	int32_t ret = deflateInit(&deflateStream_, COMPRESS_LEVEL);
	if (ret != Z_OK) {
		if (ret == Z_MEM_ERROR) {
			if (isFirstTime_) {
				GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_COMPRESSION_FAILED,
							   "error occured in deflateInit.");
				isFirstTime_ = false;
			}
			destSize = srcSize;
			compressionErrorCount_++;
			return;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_COMPRESSION_FAILED, 
							  "deflateInit failed.");
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
			GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_COMPRESSION_FAILED,
						   "error occured in deflate.");
			isFirstTime_ = false;
		}
		destSize = srcSize;
		compressionErrorCount_++;
		return;
	} else {
		destSize = static_cast<uint32_t>(deflateStream_.total_out);
		if (srcSize < destSize) {
			destSize = srcSize;
		}
	}

	ret = deflateEnd(&deflateStream_);
	if (ret != Z_OK) {
		if (isFirstTime_) {
			GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_COMPRESSION_FAILED,
				"error occured in deflateEnd.");
		}
		destSize = srcSize;
		compressionErrorCount_++;
		return;
	}
}

void ChunkManager::Compressor::uncompressData(
	uint8_t* src, uint32_t srcSize, uint8_t* dest, 
	uint32_t &destSize) 
{
	int32_t flush = Z_FINISH;

	inflateStream_.zalloc = Z_NULL;
	inflateStream_.zfree = Z_NULL;
	inflateStream_.opaque = Z_NULL;

	EXEC_FAILURE(GS_ERROR_CHM_UNCOMPRESSION_FAILED);
	if (inflateInit(&inflateStream_) != Z_OK) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_UNCOMPRESSION_FAILED,
			"inflateInit failed.");
	}

	inflateStream_.avail_in = srcSize;
	inflateStream_.avail_out = destSize;
	inflateStream_.next_in = (Bytef*)src;
	inflateStream_.next_out = (Bytef*)dest;

	int ret = Z_OK;
	do {
		ret = inflate(&inflateStream_, flush);
	} while(ret == Z_OK);

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
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_UNCOMPRESSION_FAILED,
				"inflateEnd failed.");
	}
}


util::Atomic<uint32_t> ChunkManager::FileManager::atomicEnableCompression_(1);

ChunkManager::FileManager::FileManager(
		Config& config, CheckpointFile& checkpointFile)
: config_(config),
  checkpointFile_(checkpointFile),
  compressor_(),
  holeBlockExpSize_(config.getChunkExpSize()),
  invalidCompressionCount_(TRACE_COUNTER_UNIT_),
  invalidCompressionTraceCount_(1),
  ioTimer_(),
  writeChunkCount_(0),
  chunkExpSize_(config_.getChunkExpSize()),
  chunkSize_(static_cast<uint32_t>(1UL << config_.getChunkExpSize())),
  destBuffer_(UTIL_NEW uint8_t[chunkSize_ * 2]) {

	uint64_t fileSystemBlockSize =
			checkpointFile_.getFileSystemBlockSize();

	if (!isValidHoleSize(chunkSize_, fileSystemBlockSize)) {
		GS_TRACE_ERROR(CHUNK_MANAGER,
				GS_TRACE_CHM_INVALID_PARAMETER,
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

ChunkManager::FileManager::~FileManager() {
	delete[] destBuffer_;
	destBuffer_ = NULL;
}

uint32_t ChunkManager::FileManager::compressChunk(
		uint8_t* buffer, CompressionMode mode, int64_t &compTime) {

	compTime = 0;
	ioTimer_.reset();
	ioTimer_.start();
	if (atomicEnableCompression_ != 0 && mode == BLOCK_COMPRESSION) {
		uint32_t unitHoleSize =
				static_cast<uint32_t>(1UL << holeBlockExpSize_);
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
			uint32_t holeOffset = getHoleOffset(compressedSize, holeSize);
			memset(buffer + holeOffset, 0, holeSize);

			compTime = ioTimer_.elapsedNanos() / 1000;
			return holeOffset;
		} else {
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
	compTime = ioTimer_.elapsedNanos() / 1000;
	return chunkSize_;
}

int64_t ChunkManager::FileManager::uncompressChunk(uint8_t* buffer) {
	ioTimer_.reset();
	ioTimer_.start();
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
					"Invalid uncompressed block data size. (chunkSize_=" <<
					chunkSize_ << ", uncompressedSize=" <<
					CHUNK_HEADER_FULL_SIZE + destSize);
		}
		memcpy(buffer + CHUNK_HEADER_FULL_SIZE, destBuffer_, destSize);
		ChunkHeader::setCompressedDataSize(buffer, 0);
	}
	int64_t compTime = ioTimer_.elapsedNanos() / 1000;
	return compTime;
}

int64_t ChunkManager::FileManager::writeHolePunchingChunk(
		uint8_t* buffer, int64_t writePos, uint32_t holeOffset,
		uint64_t &flushTime
) {
	flushTime = 0;
	uint32_t size = 1;
	iotrace(buffer, size, writePos);

	ioTimer_.reset();
	ioTimer_.start();

	EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);	
	EXEC_FAILURE(GS_ERROR_CF_READ_CHUNK_FAILED);	
#ifdef _WIN32
	checkpointFile_.writePartialBlock(buffer, chunkSize_, (writePos << chunkExpSize_));
#else
	uint32_t holeSize = chunkSize_ - holeOffset;
	checkpointFile_.writePartialBlock(buffer, holeOffset, (writePos << chunkExpSize_));

	try {
		EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);	
		checkpointFile_.punchHoleBlock(
			holeSize, (writePos << chunkExpSize_) + holeOffset);  
	} 
	catch (std::exception& e) {
		GS_TRACE_ERROR(CHUNK_MANAGER,
				GS_TRACE_CHM_INVALID_PARAMETER,
				"Compression mode is inactive. "
				<< "because file hole punching failed.");

		atomicEnableCompression_ = 0;
		EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);	
		EXEC_FAILURE(GS_ERROR_CF_READ_CHUNK_FAILED);	
		checkpointFile_.writePartialBlock(
				buffer, holeSize, 
				(writePos << chunkExpSize_) + holeOffset);
	}
#endif
	int64_t ioTime = ioTimer_.elapsedNanos() / 1000;

	writeChunkCount_++;
	uint64_t cpFileFlushInterval = config_.getAtomicCpFileFlushInterval();
	if (cpFileFlushInterval > 0 &&
			(writeChunkCount_ % cpFileFlushInterval == 0)) {
		ioTimer_.reset();
		ioTimer_.start();
		checkpointFile_.flush();
#ifndef WIN32
		if (config_.getAtomicCpFileAutoClearCache()) {
			checkpointFile_.advise(POSIX_FADV_DONTNEED);
		}
#endif
		flushTime = ioTimer_.elapsedNanos() / 1000;
		if (flushTime == 0) {
			flushTime = 1;
		}
	}
	return ioTime;
}

int64_t ChunkManager::FileManager::writeChunk(
		uint8_t* buffer, uint32_t size, int64_t writePos, uint64_t &flushTime) {
	flushTime = 0;
	iotrace(buffer, size, writePos);

	EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);

	ioTimer_.reset();
	ioTimer_.start();

	checkpointFile_.writeBlock(buffer, size, writePos);  
	int64_t ioTime = ioTimer_.elapsedNanos() / 1000;

	writeChunkCount_++;
	uint64_t cpFileFlushInterval = config_.getAtomicCpFileFlushInterval();
	if (cpFileFlushInterval > 0 &&
			(writeChunkCount_ % cpFileFlushInterval == 0)) {
		ioTimer_.reset();
		ioTimer_.start();
		checkpointFile_.flush();
#ifndef WIN32
		if (config_.getAtomicCpFileAutoClearCache()) {
			checkpointFile_.advise(POSIX_FADV_DONTNEED);
		}
#endif
		flushTime = ioTimer_.elapsedNanos() / 1000;
		if (flushTime == 0) {
			flushTime = 1;
		}
	}
	return ioTime;
}

int64_t ChunkManager::FileManager::readChunk(
		uint8_t* buffer, uint32_t size, int64_t readPos) {
	iotrace(buffer, size, readPos);

	ioTimer_.reset();
	ioTimer_.start();

	EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);
	checkpointFile_.readBlock(buffer, size, readPos);

	int64_t ioTime = ioTimer_.elapsedNanos() / 1000;

	return ioTime;
}

void ChunkManager::FileManager::isValidCheckSum(uint8_t* buffer) {
	assert(buffer);
	uint32_t chunkCheckSum = ChunkManager::ChunkHeader::getCheckSum(buffer);
	uint32_t calcCheckSum = ChunkManager::ChunkHeader::calcCheckSum(buffer);

	EXEC_FAILURE(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM);

	if (chunkCheckSum != calcCheckSum) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM,
				"checkSum, "
				<< chunkCheckSum << ", " << calcCheckSum << ","
				<< "pId = " << ChunkManager::ChunkHeader::getPartitionId(buffer)
				<< ", categoryId = "
				<< (int32_t)ChunkManager::ChunkHeader::getChunkCategoryId(buffer)
				<< ", cId = " << ChunkManager::ChunkHeader::getChunkId(buffer)
				<< ", pgId = "
				<< ChunkManager::ChunkHeader::getPartitionGroupId(buffer));
	}
}


void ChunkManager::FileManager::iotrace(
		uint8_t* buffer, uint32_t size, int64_t pos) {
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

uint64_t ChunkManager::BufferManager::getHashTableSize(Config& config) {
	uint64_t hashTableSize1 = getHashTableSize1(
			config.getPartitionGroupNum(),
			config.getAtomicStoreMemoryLimitNum());
	uint64_t hashTableSize2 = getHashTableSize2(config);

	GS_TRACE_INFO(
			SIZE_MONITOR, GS_TRACE_CHM_CHUNK_HASH_TABLE_SIZE,
			"chunkExpSize," << config.getChunkExpSize() <<
			",pgCount," << config.getPartitionGroupNum() <<
			",storeMemoryLimitCount," << config.getAtomicStoreMemoryLimitNum() <<
			",rate," << config.getBufferHashTableSizeRate() <<
			",oldHashTableSize," << hashTableSize1 <<
			",newHashTableSize," << hashTableSize2 <<
			",max," << ((hashTableSize1 < hashTableSize2) ? hashTableSize2 : hashTableSize1)
	);
	return ((hashTableSize1 < hashTableSize2) ? hashTableSize2 : hashTableSize1);
}

uint64_t ChunkManager::BufferManager::getHashTableSize2(Config& config) {
	const PartitionGroupId partitionGroupNum = config.getPartitionGroupNum();
	const uint64_t totalAtomicMemoryLimitNum = config.getAtomicStoreMemoryLimitNum();
	const uint64_t chunkExpSize = config.getChunkExpSize();
	double rate = config.getBufferHashTableSizeRate();
	assert(0 < partitionGroupNum && partitionGroupNum <= 128);
	assert(0 < totalAtomicMemoryLimitNum);
	if (rate == 0.0) {
		rate = 0.01;
	}
	uint64_t memLimitByte = totalAtomicMemoryLimitNum << chunkExpSize;
	uint64_t base = static_cast<double>(memLimitByte) * rate
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

uint64_t ChunkManager::BufferManager::getHashTableSize1(
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

void ChunkManager::BufferManager::setStoreMemoryAgingSwapRate(double rate) {
	if (rate < 0.0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"");
	}
	pgStats_.storeMemoryAgingSwapRate_ = rate;

	const double totalLimitNum =
			static_cast<double>(config_.getAtomicStoreMemoryLimitNum());
	const double avgNum =
			totalLimitNum / static_cast<double>(config_.getPartitionGroupNum());

	storeMemoryAgingSwapCount_ = (rate < 1.0) ?
			static_cast<uint64_t>(avgNum * rate) : UINT64_MAX;

}

void ChunkManager::BufferManager::updateStoreMemoryAgingParams() {
	const double totalLimitNum =
			static_cast<double>(config_.getAtomicStoreMemoryLimitNum());
	const double avgNum =
			totalLimitNum / static_cast<double>(config_.getPartitionGroupNum());
	const double rate = pgStats_.storeMemoryAgingSwapRate_;

	storeMemoryAgingSwapCount_ = (rate < 100.0) ?
			static_cast<uint64_t>(avgNum * rate) : UINT64_MAX;

	setStoreMemoryColdRate(config_.getAtomicStoreMemoryColdRate());

}

UTIL_FORCEINLINE
uint64_t ChunkManager::BufferManager::getPGCpFileReadRetryCount() const {
	return checkpointFile_.getReadRetryCount();
}
UTIL_FORCEINLINE
uint64_t ChunkManager::BufferManager::getPGCpFileWriteRetryCount() const {
	return checkpointFile_.getWriteRetryCount();
}


ChunkManager::BufferManager::ChainingHashTable::ChainingHashTable(
		uint64_t size, ArrayAllocator& allocator)
: maxSize_(size),
  tableMask_(maxSize_ - 1),
  table_(UTIL_NEW BufferInfo*[size])
  , collisionCount_(0)
{
	static_cast<void>(allocator);
	memset(table_, 0, maxSize_ * sizeof(BufferInfo*));
}

ChunkManager::BufferManager::ChainingHashTable::~ChainingHashTable() {
	delete [] table_;
	table_ = NULL;
}





ChunkManager::MemoryLimitManager::Load::Load(PartitionGroupId partitionGroupNum)
: totalLoad_(0),
  avgLoad_(0),
  lastLoad_(NULL),
  load_(NULL),
  readCount_(NULL) {

	try {
		lastLoad_ = UTIL_NEW uint64_t[partitionGroupNum];
		load_ = UTIL_NEW uint64_t[partitionGroupNum];
		readCount_ = UTIL_NEW uint64_t[partitionGroupNum];
		lastEstimatedLoad_.assign(partitionGroupNum, 0.0);
		newEstimatedLoad_.assign(partitionGroupNum, 0.0);
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; pgId++) {
			lastLoad_[pgId] = 0;
			load_[pgId] = 0;
			readCount_[pgId] = 0;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "partitionGroupNum," << partitionGroupNum
			<< GS_EXCEPTION_MESSAGE(e));
	}
}

ChunkManager::MemoryLimitManager::Load::~Load() {
	delete[] lastLoad_;
	lastLoad_ = NULL;
	delete[] load_;
	load_ = NULL;
	delete[] readCount_;
	readCount_ = NULL;
}



ChunkManager::CheckpointManager::CPWriter::CPWriter(
	PartitionGroupAllocator& multiThreadAllocator)
	: checkpointBufferNum_(0),
	  ptLeftCopyNum_(0),
	  ptCheckpointBuffer_(multiThreadAllocator),
	  mutex_() {}
ChunkManager::CheckpointManager::CPWriter::~CPWriter() {
	CheckpointBuffer checkpointBuffer;
	assert(!atomicFrontQueue(checkpointBuffer));
	while (atomicFrontQueue(checkpointBuffer)) {
		delete[] checkpointBuffer.buffer_;
		checkpointBuffer.buffer_ = NULL;
		if (!atomicPopQueue(checkpointBuffer)) {
			break;
		}
	}
}
void ChunkManager::CheckpointManager::CPWriter::atomicIncrementCopySize() {
	util::LockGuard<util::Mutex> guard(mutex_);
	ptLeftCopyNum_++;
}
void ChunkManager::CheckpointManager::CPWriter::atomicDecrementCopySize() {
	util::LockGuard<util::Mutex> guard(mutex_);
	assert(0 < ptLeftCopyNum_);
	ptLeftCopyNum_--;
}
uint64_t ChunkManager::CheckpointManager::CPWriter::atomicGetSize() {
	util::LockGuard<util::Mutex> guard(mutex_);
	uint64_t leftWriteChunkNum = ptCheckpointBuffer_.size();
	uint64_t size = ptLeftCopyNum_ + leftWriteChunkNum;
	return size;
}
void ChunkManager::CheckpointManager::CPWriter::atomicResetCopySize() {
	EXEC_FAILURE(GS_ERROR_CM_INTERNAL_ERROR);  
	util::LockGuard<util::Mutex> guard(mutex_);
	ptLeftCopyNum_ = 0;
}
void ChunkManager::CheckpointManager::CPWriter::atomicPushQueue(
	CheckpointBuffer& checkpointBuffer) {
	util::LockGuard<util::Mutex> guard(mutex_);
	try {
		ptCheckpointBuffer_.push_back(checkpointBuffer);
		checkpointBufferNum_++;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
bool ChunkManager::CheckpointManager::CPWriter::atomicFrontQueue(
	CheckpointBuffer& checkpointBuffer) {
	util::LockGuard<util::Mutex> guard(mutex_);
	if (ptCheckpointBuffer_.empty()) {
		return false;
	}
	checkpointBuffer = ptCheckpointBuffer_.front();
	return true;
}
bool ChunkManager::CheckpointManager::CPWriter::atomicPopQueue(
		CheckpointBuffer& targetCheckpointBuffer) {
	static_cast<void>(targetCheckpointBuffer);
	util::LockGuard<util::Mutex> guard(mutex_);
	if (ptCheckpointBuffer_.empty()) {
		return false;
	}
	assert(targetCheckpointBuffer.checkpointPos_ ==
		   ptCheckpointBuffer_.front().checkpointPos_);
	ptCheckpointBuffer_.pop_front();
	checkpointBufferNum_--;
	return true;
}
uint64_t ChunkManager::CheckpointManager::CPWriter::getCheckpointBufferNum()
	const {
	return checkpointBufferNum_;
}

ChunkManager::CheckpointManager::CheckpointManager(PartitionGroupId pgId,
	PartitionGroupAllocator& allocator,
	PartitionGroupAllocator& multiThreadAllocator,
	PartitionInfoList& partitionInfo, CheckpointFile& checkpointFile,
	MemoryPool& memoryPool, util::Mutex& mutex, Config& config)
	: pgId_(pgId),
	  pgStats_(),
	  pgPool_(memoryPool),
	  cpMemPoolMutex_(mutex),
	  pgStartCpId_(UNDEF_CHECKPOINT_ID),
	  pgEndCpId_(UNDEF_CHECKPOINT_ID),
	  pgCheckpointBitArray_(100),
	  pgFreeBitArray_(100)
	  ,
	  pgBackupBitArray_(100)
	  , pgKickCount_(0), pgKickTime_(0)
	  ,
	  pId_(UNDEF_PARTITIONID),
	  partitionInfo_(partitionInfo),
	  categoryCursor_(UNDEF_CHUNK_CATEGORY_ID),
	  ptCopyList_(),
	  cpWriter_(multiThreadAllocator),
	  directWriteBuffer_(allocator),
	  syncPId_(UNDEF_PARTITIONID),
	  checkpointPosList_(allocator),
	  checkpointFile_(checkpointFile),
	  config_(config),
	  fileManagerCPThread_(config_, checkpointFile),
	  fileManagerTxnThread_(config_, checkpointFile) {
	try {
		for (ChunkCategoryId categoryId = 0;
			 categoryId < config_.getChunkCategoryNum(); categoryId++) {
			ptCopyList_.push_back(UTIL_NEW CopyList(allocator));
		}
		uint8_t temp = 0;
		directWriteBuffer_.assign(config_.getChunkSize(), temp);
	}
	catch (std::exception& e) {
		for (ChunkCategoryId categoryId = 0;
			 categoryId < config_.getChunkCategoryNum(); categoryId++) {
			delete ptCopyList_[categoryId];
		}
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
ChunkManager::CheckpointManager::~CheckpointManager() {
	clearPG();
	while (!ptCopyList_.empty()) {
		delete ptCopyList_[ptCopyList_.size() - 1];
		ptCopyList_.pop_back();
	}
}
void ChunkManager::CheckpointManager::setPGCheckpointId(CheckpointId cpId) {
	clearPG();
	assert(pgStartCpId_ == UNDEF_CHECKPOINT_ID || pgStartCpId_ < cpId);
	pgStartCpId_ = cpId;
}
CheckpointId ChunkManager::CheckpointManager::getPGCompletedCheckpointId()
	const {
	return pgEndCpId_;
}
void ChunkManager::CheckpointManager::switchPGCheckpointBit(CheckpointId cpId) {
	EXEC_FAILURE(GS_ERROR_CM_NO_MEMORY);
	CheckpointFile& checkpointFile = getCheckpointFile();
	for (uint64_t blockNo = 0; blockNo < pgCheckpointBitArray_.length();
		 ++blockNo) {
		if (pgCheckpointBitArray_.get(blockNo)) {
			checkpointFile.setValidBlockInfo(blockNo, true);
			assert(checkpointFile.getUsedBlockInfo(blockNo));
		}
	}
	for (uint64_t blockNo = 0; blockNo < pgFreeBitArray_.length(); ++blockNo) {
		if (pgFreeBitArray_.get(blockNo)) {
			assert(!pgCheckpointBitArray_.get(blockNo));
			checkpointFile.setValidBlockInfo(blockNo, false);
			checkpointFile.setUsedBlockInfo(blockNo, false);
		}
	}

	pgStats_.checkpointCount_ = pgCheckpointBitArray_.countNumOfBits();
	pgEndCpId_ = cpId;
	if (pgStartCpId_ != pgEndCpId_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_CHECKPOINT_ID,
			"invalid end checkpoint ID, should be same as start checkpoint ID. "
				<< "start checkpoint ID," << pgStartCpId_
				<< "end checkpoint ID," << pgEndCpId_);
	}

	for (PartitionId pId = 0; pId < config_.getPartitionNum(); pId++) {
		if (config_.getPartitionGroupId(pId) == pgId_) {
			PartitionInfo& partitionInfo = getPartitionInfo(pId);
			if (partitionInfo.startCpId_ == pgStartCpId_) {
				partitionInfo.lastCpId_ = pgStartCpId_;
			}
		}
	}
}

void ChunkManager::CheckpointManager::initializePGCheckpointBit() {
	CheckpointFile& checkpointFile = getCheckpointFile();
	checkpointFile.initializeValidBlockInfo();
	checkpointFile.initializeUsedBlockInfo();
}

void ChunkManager::CheckpointManager::setPGCheckpointBit(const uint64_t pos) {
	CheckpointFile& checkpointFile = getCheckpointFile();
	checkpointFile.setValidBlockInfo(pos, true);
	checkpointFile.setUsedBlockInfo(pos, true);
}

void ChunkManager::CheckpointManager::releaseUnusedPGFileBlocks() {
	CheckpointFile& checkpointFile = getCheckpointFile();
	checkpointFile.zerofillUnusedBlock();
}

void ChunkManager::CheckpointManager::flushPGFile() {
	return getCheckpointFile().flush();
}
ChunkBitArray& ChunkManager::CheckpointManager::getPGCheckpointBit() {
	return getCheckpointFile().getValidBitArray();
}
ChunkBitArray& ChunkManager::CheckpointManager::getPGBackupBit() {
	return pgBackupBitArray_;
}
void ChunkManager::CheckpointManager::clearPGBackupBit() {
	pgBackupBitArray_.clear();
}
void ChunkManager::CheckpointManager::clearPG() {
	pgCheckpointBitArray_.clear();
	pgFreeBitArray_.clear();
	pgKickCount_ = pgStats_.checkpointKickCount_;
	pgKickTime_ = pgStats_.checkpointKickTime_;
	clearPartition();
}

uint64_t ChunkManager::CheckpointManager::getPGKickCount() {
	if (pgStats_.checkpointKickCount_ >= pgKickCount_) {
		return (pgStats_.checkpointKickCount_ - pgKickCount_);
	}
	else {
		return 0;
	}
}

uint64_t ChunkManager::CheckpointManager::getPGKickTime() {
	if (pgStats_.checkpointKickTime_ >= pgKickTime_) {
		return (pgStats_.checkpointKickTime_ - pgKickTime_);
	}
	else {
		return 0;
	}
}
void ChunkManager::CheckpointManager::appendNewCheckpointChunkId(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId, int64_t oldPos,
	int64_t newPos, bool toWrite) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	assert(-1 <= oldPos);
	assert(0 <= newPos);

	if (pId_ == UNDEF_PARTITIONID) {
		pId_ =
			pId;  
	}

	PartitionInfo& partitionInfo = getPartitionInfo(pId);
	partitionInfo.startCpId_ = pgStartCpId_;

	if (toWrite) {
		EXEC_FAILURE(GS_ERROR_CM_NO_MEMORY);
		getCopyList(categoryId).push_back(cId);
		cpWriter_.atomicIncrementCopySize();  
		partitionInfo.copyNum_++;
		categoryCursor_ =
			0;  
	}

	assert(newPos != -1);
	pgCheckpointBitArray_.set(newPos, true);
	partitionInfo.checkpointNum_++;
	if (oldPos != -1) {
		if (oldPos != newPos) {
			pgFreeBitArray_.set(oldPos, true);
		}
		assert(0 < partitionInfo.checkpointNum_);
		partitionInfo.checkpointNum_--;
	}
}
void ChunkManager::CheckpointManager::removeOldCheckpointChunkId(
	PartitionId pId, int64_t oldPos) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	assert(0 <= oldPos);

	if (pId_ == UNDEF_PARTITIONID) {
		pId_ =
			pId;  
	}

	pgFreeBitArray_.set(oldPos, true);
	PartitionInfo& partitionInfo = getPartitionInfo(pId);
	assert(0 < partitionInfo.checkpointNum_);
	partitionInfo.checkpointNum_--;
}
bool ChunkManager::CheckpointManager::isCopyLeft(PartitionId pId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	return (pId == pId_) && (0 < cpWriter_.atomicGetSize());  
}
bool ChunkManager::CheckpointManager::getCopyChunkId(
	PartitionId pId, ChunkCategoryId& categoryId, ChunkId& cId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));

	if (pId_ != pId) {
		categoryId = UNDEF_CHUNK_CATEGORY_ID;
		cId = UNDEF_CHUNKID;
		return false;
	}

	if (categoryCursor_ == UNDEF_CHUNK_CATEGORY_ID ||
		config_.getChunkCategoryNum() <= categoryCursor_) {
		categoryId = UNDEF_CHUNK_CATEGORY_ID;
		cId = UNDEF_CHUNKID;
		return false;
	}
	CopyList* copyList = &getCopyList(categoryCursor_);
	if (copyList->empty()) {
		while (copyList->empty()) {
			categoryCursor_++;
			if (config_.getChunkCategoryNum() <= categoryCursor_) {
				categoryId = UNDEF_CHUNK_CATEGORY_ID;
				cId = UNDEF_CHUNKID;
				return false;
			}
			copyList = &getCopyList(categoryCursor_);
		}
	}
	assert(0 < copyList->size());
	categoryId = categoryCursor_;
	cId = copyList->front();
	return true;
}
void ChunkManager::CheckpointManager::directWriteChunk(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		uint8_t* buffer, CheckpointBuffer &checkpointBuffer) {
	if (checkpointBuffer.checkpointPos_ != -1) {
		memcpy(checkpointBuffer.buffer_, buffer, config_.getChunkSize());
		ChunkHeader::setCPCpId(checkpointBuffer.buffer_, getPartitionInfo(pId).lastCpId_);
		ChunkHeader::setSwapCpId(checkpointBuffer.buffer_, getPartitionInfo(pId).startCpId_);
		int64_t compTime = 0;
		uint32_t holeOffset = fileManagerTxnThread_.compressChunk(
				checkpointBuffer.buffer_,
				config_.getPartitionCompressionMode(pId), compTime);
		pgStats_.checkpointKickCompressTime_ += compTime;
		fileManagerTxnThread_.updateCheckSum(checkpointBuffer.buffer_);
		TEST_DIRTY_CHECK_SET(pId, categoryId, cId, checkpointBuffer.buffer_);
		uint64_t flushTime;
		uint64_t ioTime =
				fileManagerTxnThread_.writeHolePunchingChunk(
						checkpointBuffer.buffer_,
						checkpointBuffer.checkpointPos_,
						holeOffset, flushTime);

		pgStats_.checkpointKickCount_++;
		pgStats_.checkpointKickTime_ += ioTime;
		if (flushTime != 0) {
			pgStats_.cpFileFlushCount_++;
			pgStats_.cpFileFlushTime_ += flushTime;
		}
	}
	else {
		TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, true);
	}
}


void ChunkManager::CheckpointManager::copyChunk(PartitionId pId,
		ChunkCategoryId categoryId, ChunkId cId, uint8_t* buffer,
		CheckpointBuffer &checkpointBuffer) {
	if (checkpointBuffer.checkpointPos_ != -1) {

		memcpy(checkpointBuffer.buffer_, buffer, config_.getChunkSize());

		cpWriter_.atomicPushQueue(checkpointBuffer);  
		pgStats_.checkpointCopyCount_++;
	}
}

bool ChunkManager::CheckpointManager::kickChunk(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, uint8_t* buffer,
	uint64_t checkpointPos) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	assert(pId_ == pId);

	bool copied = false;
	if (buffer) {
		{

			CheckpointBuffer checkpointBuffer;
			checkpointBuffer.checkpointPos_ = checkpointPos;
			{
				util::LockGuard<util::Mutex> guard(cpMemPoolMutex_); 
				if (pgPool_.getFreeElementCount() == 0
						&& pgPool_.getTotalElementCount() >= config_.getAtomicCheckpointMemoryLimitNum()) {
					checkpointBuffer.buffer_ = directWriteBuffer_.data();
				}
				else {
					checkpointBuffer.buffer_ = pgPool_.allocate();
				}
			}
			if (checkpointBuffer.buffer_ == directWriteBuffer_.data()) {
				directWriteChunk(pId, categoryId, cId,
								 buffer, checkpointBuffer);
				copied = false;
			}
			else {
				copyChunk(pId, categoryId, cId,
						buffer, checkpointBuffer);
				copied = true;
			}
		}
	}

	if (categoryId == categoryCursor_) {
		CopyList& copyList = getCopyList(categoryCursor_);
		if (!copyList.empty() && cId == copyList.front()) {
			copyList.pop_front();
			cpWriter_.atomicDecrementCopySize();  
			assert(0 < getPartitionInfo(pId).copyNum_);
			getPartitionInfo(pId).copyNum_--;
		}
	}
	return copied;
}


bool ChunkManager::CheckpointManager::writeChunk(PartitionId pId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));

	if (pId_ != pId) {
		return false;
	}

	CheckpointBuffer checkpointBuffer;
	if (!cpWriter_.atomicFrontQueue(checkpointBuffer)) {  
		return false;
	}
	assert(checkpointBuffer.buffer_);
	if (checkpointBuffer.checkpointPos_ != -1) {
		int64_t compTime = 0;
		uint32_t holeOffset =
			fileManagerCPThread_.compressChunk(checkpointBuffer.buffer_,
				config_.getPartitionCompressionMode(pId), compTime);
		pgStats_.checkpointWriteCompressTime_ += compTime;
		fileManagerCPThread_.updateCheckSum(checkpointBuffer.buffer_);
		uint64_t flushTime;
		uint64_t ioTime = fileManagerCPThread_.writeHolePunchingChunk(
			checkpointBuffer.buffer_, checkpointBuffer.checkpointPos_, holeOffset, flushTime);
		pgStats_.checkpointWriteCount_++;
		pgStats_.checkpointWriteTime_ += ioTime;
		if (flushTime != 0) {
			pgStats_.cpFileFlushCount_++;
			pgStats_.cpFileFlushTime_ += flushTime;
		}
	}
	cpWriter_.atomicPopQueue(checkpointBuffer);  

	{
		util::LockGuard<util::Mutex> guard(cpMemPoolMutex_);
		pgPool_.free(checkpointBuffer.buffer_,
			static_cast<uint32_t>(config_.getAtomicCheckpointMemoryLimitNum()));
		checkpointBuffer.buffer_ = NULL;
	}

	return true;
}
void ChunkManager::CheckpointManager::clearPartition() {
	CheckpointBuffer checkpointBuffer;

	if (pId_ == UNDEF_PARTITIONID) {
		if (ptCopyList_.empty()) {
		}
		else {
			assert(getCopyList(0).empty());
			assert(
				config_.getChunkCategoryNum() <= 1 || getCopyList(1).empty());
			assert(
				config_.getChunkCategoryNum() <= 2 || getCopyList(2).empty());
			assert(
				config_.getChunkCategoryNum() <= 3 || getCopyList(3).empty());
			assert(
				config_.getChunkCategoryNum() <= 4 || getCopyList(4).empty());
		}
		assert(cpWriter_.atomicGetSize() == 0);
		assert(!cpWriter_.atomicFrontQueue(checkpointBuffer));
		return;
	}

	assert(pgId_ == config_.getPartitionGroupId(pId_));

	pId_ = UNDEF_PARTITIONID;
	categoryCursor_ = UNDEF_CHUNK_CATEGORY_ID;
	for (ChunkCategoryId categoryId = 0;
		 categoryId < config_.getChunkCategoryNum(); categoryId++) {
		getCopyList(categoryId).clear();
	}
	cpWriter_.atomicResetCopySize();  

	while (cpWriter_.atomicFrontQueue(checkpointBuffer)) {  
		{
			util::LockGuard<util::Mutex> guard(cpMemPoolMutex_);
			pgPool_.free(checkpointBuffer.buffer_,
				static_cast<uint32_t>(config_.getAtomicCheckpointMemoryLimitNum()));
		}
		if (!cpWriter_.atomicPopQueue(checkpointBuffer)) {  
			return;
		}
	}
	assert(!cpWriter_.atomicFrontQueue(checkpointBuffer));  
}
void ChunkManager::CheckpointManager::readSyncChunk(
	int64_t checkpointPos, uint8_t* buffer) {
	assert(0 <= checkpointPos);
	assert(buffer);

	try {
		int64_t ioTime = fileManagerTxnThread_.readChunk(buffer, 1, checkpointPos);
		fileManagerTxnThread_.isValidCheckSum(buffer);
		int64_t compTime = fileManagerTxnThread_.uncompressChunk(buffer);
		pgStats_.chunkSyncReadUncompressTime_ += compTime;
		pgStats_.chunkSyncReadCount_++;
		pgStats_.chunkSyncReadTime_ += ioTime;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "checkpointPos,"
									 << checkpointPos << ",fileSize,"
									 << checkpointFile_.getFileSize() << ", "
									 << GS_EXCEPTION_MESSAGE(e));
	}
}
bool ChunkManager::CheckpointManager::readRecoveryChunk(uint8_t* buffer) {
	assert(buffer);

	uint64_t checkBlockPos = 0;
	CheckpointFile& checkpointFile = getCheckpointFile();
	bool validBlockFound = false;
	try {
		int64_t ioTime = fileManagerTxnThread_.readChunk(buffer, 1, checkBlockPos);
		int64_t compTime = fileManagerTxnThread_.uncompressChunk(buffer);
		pgStats_.recoveryReadUncompressTime_ += compTime;
		pgStats_.recoveryReadCount_++;
		pgStats_.recoveryReadTime_ += ioTime;

		if (!ChunkHeader::checkMagicNum(buffer)) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_RECOVERY_FAILED,
				"Invalid magic number of block header: data corrupt."
					<< " (pgId=" << pgId_
					<< ", validate block position=" << checkBlockPos << ")");
		}
		return validBlockFound;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MESSAGE(e));
	}
}
void ChunkManager::CheckpointManager::readCheckpointChunk(
	uint64_t checkpointPos, uint8_t* buffer) {
	try {
		int64_t ioTime = fileManagerCPThread_.readChunk(buffer, 1, checkpointPos);
		fileManagerCPThread_.isValidCheckSum(buffer);
		int64_t compTime = fileManagerCPThread_.uncompressChunk(buffer);
		pgStats_.backupReadUncompressTime_ += compTime;
		pgStats_.backupReadCount_++;
		pgStats_.backupReadTime_ += ioTime;
	}
	catch (std::exception& e) {
		UTIL_RETHROW_USER_ERROR(GS_ERROR_CHM_GET_CHECKPOINT_CHUNK_FAILED, e,
			"checkpointPos," << checkpointPos);
	}
}

void ChunkManager::CheckpointManager::clearCheckpointPosList() {
	syncPId_ = UNDEF_PARTITIONID;
	checkpointPosList_.clear();
}
void ChunkManager::CheckpointManager::appendCheckpointPosList(
	PartitionId pId, int64_t checkpointPos) {
	assert(pgId_ == config_.getPartitionGroupId(pId));

	if (syncPId_ == UNDEF_PARTITIONID) {
		syncPId_ = pId;
	}
	if (syncPId_ == pId && checkpointPos != -1) {
		checkpointPosList_.push_back(checkpointPos);
	}
}
bool ChunkManager::CheckpointManager::getCheckpointPosList(
	int64_t& checkpointPos) {
	if (!checkpointPosList_.empty()) {
		checkpointPos = checkpointPosList_.front();
		checkpointPosList_.pop_front();
		if (checkpointPosList_.empty()) {
			syncPId_ = UNDEF_PARTITIONID;
			return true;  
		}
	}
	else {
		checkpointPos = -1;
	}
	return false;
}
ChunkManager::BufferManager::PGStats::PGStats(ChunkCategoryId categoryNum)
	: categoryStats_(NULL),
	  useBufferNum_(0)
	  ,
	  swapWriteCount_(0),
	  swapWriteTime_(0),
	  atomicSwapReadCount_(0),
	  swapReadTime_(0),
	  swapOutCount_(0),
	  storeMemoryAgingSwapRate_(0.02),
	  swapNormalReadCount_(0),
	  swapColdBufferingReadCount_(0),
	  cpFileFlushCount_(0),
	  cpFileFlushTime_(0),
	  swapWriteCompressTime_(0),
	  swapReadUncompressTime_(0),
	  enwarmReadUncompressTime_(0),
	  enwarmReadCount_(0),
	  enwarmReadTime_(0) {
	try {
		categoryStats_ = UTIL_NEW CategoryStats[categoryNum];
	}
	catch (std::exception& e) {
		delete[] categoryStats_;
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
ChunkManager::BufferManager::PGStats::~PGStats() {
	delete[] categoryStats_;
}

void ChunkManager::BufferManager::LinkedList::split(
		int64_t argPos, LinkedList &result) {

	assert(isValid());

	BufferInfo* start = NULL;
	BufferInfo* end = NULL;
	uint64_t count = 0;
	result.reset(NULL, NULL, 0);
	if (argPos == 0) {
		return;
	}
	if (argPos > 0) {
		uint64_t pos = static_cast<uint64_t>(argPos);
		start = head_;
		if (pos < elementCount_) {
			count = pos;
			BufferInfo* cursor = head_;
			for (uint64_t cursorPos = 0; cursorPos < count; ++cursorPos) {
				assert(cursor->lruNext_);
				cursor->onHotList_ = !(cursor->onHotList_);
				cursor = cursor->lruNext_;
			}
			end = cursor->lruPrev_;
			head_ = cursor;
			end->lruNext_ = NULL;
			head_->lruPrev_ = NULL;
			elementCount_ -= count;
		}
		else {
			count = elementCount_;
			end = tail_;

			BufferInfo* cursor = head_;
			while (cursor) {
				cursor->onHotList_ = !(cursor->onHotList_);
				cursor = cursor->lruNext_;
			}
			head_ = NULL;
			tail_ = NULL;
			elementCount_ = 0;
		}
	}
	else {
		uint64_t pos = static_cast<uint64_t>(0 - argPos);
		end = tail_;
		if (pos < elementCount_) {
			count = pos;
			BufferInfo* cursor = tail_;
			for (uint64_t cursorPos = 0; cursorPos < count; ++cursorPos) {
				assert(cursor->lruPrev_);
				cursor->onHotList_ = !(cursor->onHotList_);
				cursor = cursor->lruPrev_;
			}
			start = cursor->lruNext_;
			tail_ = cursor;
			start->lruPrev_ = NULL;
			tail_->lruNext_ = NULL;
			elementCount_ -= count;
		}
		else {
			count = elementCount_;
			start = head_;

			BufferInfo* cursor = head_;
			while (cursor) {
				cursor->onHotList_ = !(cursor->onHotList_);
				cursor = cursor->lruNext_;
			}
			head_ = NULL;
			tail_ = NULL;
			elementCount_ = 0;
		}
	}
	result.reset(start, end, count);
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
	assert(result.isValid());
}

bool ChunkManager::BufferManager::LinkedList::isValid() {
	return true;
}


ChunkManager::BufferManager::BufferList::BufferList()
: cursor_(NULL), cursorOnCold_(false), coldRate_(3.0 / 8.0)
{
}

ChunkManager::BufferManager::BufferList::~BufferList() {
}



ChunkManager::BufferInfo* ChunkManager::BufferManager::getSwapChunk(
	PartitionId& pId, ChunkCategoryId& categoryId, ChunkId& cId) {
	BufferInfo* bufferInfo = swapList_.back();
	if (bufferInfo) {
		assert(
			!bufferInfo->isFixed());  
		pId = bufferInfo->pId_;
		categoryId = bufferInfo->categoryId_;
		cId = bufferInfo->cId_;
		assert(bufferInfo->buffer_);
	}
	else {
		pId = UNDEF_PARTITIONID;
		categoryId = -1;
		cId = UNDEF_CHUNKID;
		bufferInfo = NULL;
	}

	return bufferInfo;
}
void ChunkManager::BufferManager::swapOut(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, BufferInfo* bufferInfo) {

	TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, false);

	assert(pgId_ == config_.getPartitionGroupId(pId));
	assert(bufferInfo);
	assert(bufferInfo->buffer_);

	int64_t writePos = -1;
	if (bufferInfo->newCheckpointPos_ != -1 && currentCPPId_ == pId) {
		assert(!bufferInfo->isDirty_);
		writePos = bufferInfo->newCheckpointPos_;
	}
	else {
		if (bufferInfo->isDirty_) {
			if (bufferInfo->latestPos_ == -1) {
				bufferInfo->latestPos_ = getCheckpointFile().allocateBlock();
				writePos = bufferInfo->latestPos_;
			}
			else {
				assert(writePos ==
					   -1);  
				TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, true);
			}
		}
		else {
			assert(
				writePos ==
				-1);  
		}
	}

	bufferInfo->newCheckpointPos_ = -1;
	if (bufferInfo->toCopy_) {
		bufferInfo->toCopy_ = false;
	}

	if (writePos != -1) {
		uint8_t* buffer = bufferInfo->buffer_;
		ChunkHeader::setCPCpId(buffer, getPartitionInfo(pId).lastCpId_);
		ChunkHeader::setSwapCpId(buffer, getPartitionInfo(pId).startCpId_);
		int64_t compTime = 0;
		uint32_t holeOffset = fileManager_.compressChunk(buffer,
			config_.getPartitionCompressionMode(pId), compTime);
		pgStats_.swapWriteCompressTime_ += compTime;
		fileManager_.updateCheckSum(buffer);
		TEST_DIRTY_CHECK_SET(pId, categoryId, cId, buffer);
		uint64_t flushTime;
		uint64_t ioTime =
			fileManager_.writeHolePunchingChunk(buffer, writePos, holeOffset, flushTime);
		pgStats_.categoryStats_[categoryId].swapWriteCount_++;
		pgStats_.swapWriteCount_++;
		pgStats_.swapWriteTime_ += ioTime;
		if (flushTime != 0) {
			pgStats_.cpFileFlushCount_++;
			pgStats_.cpFileFlushTime_ += flushTime;
		}
	}
	else {
		TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, true);
	}
	swapList_.remove(*bufferInfo);
	pgStats_.swapOutCount_++;
	pgPool_.free(bufferInfo->buffer_, atomicMemoryLimitNum_);
	assert(0 < pgStats_.categoryStats_[categoryId].useBufferNum_);
	pgStats_.categoryStats_[categoryId].useBufferNum_--;
	assert(0 < pgStats_.useBufferNum_);
	pgStats_.useBufferNum_--;
	bufferInfo->buffer_ = NULL;
	if (!bufferInfo->isDirty_) {
		assert(!bufferInfo->buffer_);  
		assert(bufferInfo->latestPos_ == -1);
		assert(bufferInfo->newCheckpointPos_ == -1);
		bufferInfoPool_.deallocate(
			bufferInfoList_.remove(getBufferId(pId, categoryId, cId)));
	}
	else {
		assert(bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_FILE));
	}

	TEST_DIRTY_CHECK_RESET(pId, categoryId, cId);
}

void ChunkManager::BufferManager::updateChunkCheckpointId(
	CheckpointId last, CheckpointId current, uint8_t* buffer) {
	ChunkHeader::setCPCpId(buffer, last);
	ChunkHeader::setSwapCpId(buffer, current);
	ChunkHeader::updateCheckSum(buffer);  
}
ChunkManager::BufferManager::BufferManager(PartitionGroupId pgId,
	PartitionInfoList& partitionInfo, CheckpointFile& checkpointFile,
	MemoryPool& memoryPool, Config& config)
	: pgId_(pgId),
	  config_(config),
	  atomicMemoryLimitNum_(
		  static_cast<int32_t>(config.getAtomicStoreMemoryLimitNum() /
							   config.getPartitionGroupNum())),
	  partitionInfo_(partitionInfo),
	  pgStats_(config.getChunkCategoryNum()),
	  pgPool_(memoryPool),
	  bufferInfoAllocator_(
		  util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "bufferInfo")),
	  bufferInfoList_(getHashTableSize(config_),
		  bufferInfoAllocator_),
	  bufferInfoPool_(
		  util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "bufferInfoPool")),
	  swapList_(),
	  refId_(0),
	  currentCPPId_(UNDEF_PARTITIONID),

	  storeMemoryAgingSwapCount_(UINT64_MAX),

	  checkpointFile_(checkpointFile),
	  fileManager_(config_, checkpointFile)
{
}
ChunkManager::BufferManager::~BufferManager() {

	ChainingHashTableCursor cursor(bufferInfoList_);
	BufferInfo* bufferInfo = NULL;
	while (cursor.next(bufferInfo)) {
		pgPool_.free(bufferInfo->buffer_, atomicMemoryLimitNum_);
		cursor.remove(bufferInfo);
		bufferInfoPool_.deallocate(bufferInfo);
	}
}
void ChunkManager::BufferManager::adjustStoreMemory(bool doReserve) {
	uint64_t swapNum =
		getSwapNum(getAtomicPGMemoryLimitNum(), getPGUseBufferNum());
	if (config_.getMaxOnceSwapNum() < swapNum) {
		swapNum = config_.getMaxOnceSwapNum();
	}
	if (swapNum == 0 && doReserve && pgPool_.getFreeElementCount() == 0
			&& pgPool_.getTotalElementCount() >= config_.getAtomicStoreMemoryLimitNum()) {
		swapNum = 1;
	}
	for (uint64_t count = 0; count < swapNum; count++) {
		PartitionId pId;
		ChunkCategoryId categoryId;
		ChunkId cId;
		BufferInfo* bufferInfo = getSwapChunk(pId, categoryId, cId);
		if (bufferInfo) {
			swapOut(pId, categoryId, cId, bufferInfo);
		}
		else {
			GS_TRACE_INFO(CHUNK_MANAGER, GS_TRACE_CHM_INTERNAL_INFO,
				"pgId = " << pgId_ << ", num of chunks over the limit = "
						  << (swapNum - count));
			return;
		}
	}
	updateStoreMemoryAgingParams();
	swapList_.rebalance(pgStats_.useBufferNum_);
}
void ChunkManager::BufferManager::resetRefCounter() {
	refId_++;
}
int32_t ChunkManager::BufferManager::getRefCounter(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));
	if (bufferInfo) {
		return bufferInfo->refCount_;
	}
	else {
		return 0;
	}
}
bool ChunkManager::BufferManager::fix(BufferInfo* bufferInfo) {
	if (bufferInfo) {
		bool isFirst = bufferInfo->fix(refId_);
		if (isFirst) {
			swapList_.remove(*bufferInfo);
		}
		return true;
	}
	else {
		return false;
	}
}
bool ChunkManager::BufferManager::fix(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	return fix(bufferInfoList_.get(getBufferId(pId, categoryId, cId)));
}
bool ChunkManager::BufferManager::unfix(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));
	if (bufferInfo) {
		assert(bufferInfo->buffer_);  

		bufferInfo->unfix();

		if (!bufferInfo->isFixed()) {
			if (pgStats_.swapOutCount_ < storeMemoryAgingSwapCount_
				|| bufferInfo->onHotList_) {
				swapList_.push(*bufferInfo);
			}
			else {
				swapList_.pushCold(*bufferInfo);
			}
		}
		return true;
	}
	else {
		return false;
	}
}
bool ChunkManager::BufferManager::isFixed(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));
	if (bufferInfo) {
		return bufferInfo->isFixed();
	}
	return false;
}

uint8_t* ChunkManager::BufferManager::allocate(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId,
	ChunkManager::BufferInfo*& bufferInfo) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	bufferInfo = bufferInfoPool_.allocate();
	bufferInfo->pId_ = pId;
	bufferInfo->categoryId_ = categoryId;
	bufferInfo->cId_ = cId;
	bufferInfoList_.append(getBufferId(pId, categoryId, cId), bufferInfo);

	adjustStoreMemory(true);

	bufferInfo->buffer_ =
		pgPool_.allocate();  
	pgStats_.categoryStats_[categoryId].useBufferNum_++;
	pgStats_.useBufferNum_++;
	bufferInfo->fixFirstTime(refId_);  
	bufferInfo->isDirty_ = true;
	assert(bufferInfo->isValid(
		BUFFER_MANAGER_STATE_DIRTY_BUFFER));  

	return bufferInfo->buffer_;
}
void ChunkManager::BufferManager::free(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, int64_t checkpointPos,
	uint8_t*& checkpointBuffer) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	checkpointBuffer = NULL;
	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));

	TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, false);

	if (bufferInfo) {
		assert(bufferInfo->buffer_ || 0 <= bufferInfo->latestPos_);
		assert(bufferInfo->refCount_ == 0);

		if (bufferInfo->newCheckpointPos_ != -1) {
			assert(bufferInfo->buffer_);
			assert(bufferInfo->latestPos_ == -1);
			assert(!bufferInfo->isDirty_);

			assert(0 <= bufferInfo->newCheckpointPos_ && bufferInfo->buffer_);
			if (bufferInfo->toCopy_) {
				swapList_.remove(*bufferInfo);
				checkpointBuffer = bufferInfo->buffer_;
				bufferInfo->buffer_ =
					NULL;  
			}
			bufferInfo->newCheckpointPos_ = -1;
			bufferInfo->toCopy_ = false;
		}

		if (bufferInfo->latestPos_ != -1) {
			if (bufferInfo->latestPos_ != checkpointPos) {
				getCheckpointFile().setUsedBlockInfo(
					bufferInfo->latestPos_, false);
			}
			bufferInfo->latestPos_ = -1;
		}

		if (bufferInfo->buffer_) {
			swapList_.remove(*bufferInfo);
			pgPool_.free(bufferInfo->buffer_, atomicMemoryLimitNum_);
			assert(0 < pgStats_.categoryStats_[categoryId].useBufferNum_);
			pgStats_.categoryStats_[categoryId].useBufferNum_--;
			assert(0 < pgStats_.useBufferNum_);
			pgStats_.useBufferNum_--;
			bufferInfo->buffer_ = NULL;
		}

		assert(!bufferInfo->buffer_);  
		assert(bufferInfo->latestPos_ == -1);
		assert(!bufferInfo->toCopy_);
		if (bufferInfo->isDirty_) {
			bufferInfo->isDirty_ = false;
		}

		EXEC_FAILURE(GS_ERROR_CM_INTERNAL_ERROR);  
		bufferInfoPool_.deallocate(
			bufferInfoList_.remove(getBufferId(pId, categoryId, cId)));
	}

	TEST_DIRTY_CHECK_RESET(pId, categoryId, cId);
}
uint8_t* ChunkManager::BufferManager::getForUpdate(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, int64_t checkpointPos, bool doFix,
	uint8_t*& checkpointBuffer, ChunkManager::BufferInfo*& bufferInfo) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	checkpointBuffer = NULL;
	bufferInfo = bufferInfoList_.get(getBufferId(pId, categoryId, cId));

	uint64_t readPos = -1;
	if (!bufferInfo) {
		bufferInfo = bufferInfoPool_.allocate();
		bufferInfo->pId_ = pId;
		bufferInfo->categoryId_ = categoryId;
		bufferInfo->cId_ = cId;
		bufferInfoList_.append(getBufferId(pId, categoryId, cId), bufferInfo);
		assert(bufferInfo->latestPos_ == -1);

		readPos = checkpointPos;
		assert(readPos != -1);
	}
	else {
		readPos = bufferInfo->latestPos_;

		if (bufferInfo->newCheckpointPos_ != -1) {
			assert(0 <= bufferInfo->newCheckpointPos_ && bufferInfo->buffer_);
			if (bufferInfo->toCopy_) {
				checkpointBuffer =
					bufferInfo->buffer_;  
			}
			bufferInfo->newCheckpointPos_ =
				-1;  
			bufferInfo->toCopy_ = false;
		}
	}

	if (bufferInfo->buffer_) {
		if (doFix) {
			bool isFirst = bufferInfo->fix(refId_);
			if (isFirst) {
				swapList_.remove(*bufferInfo);
			}
		}
	}
	else {
		assert(readPos != -1);

		adjustStoreMemory(true);

		bufferInfo->buffer_ =
			pgPool_.allocate();  
		pgStats_.categoryStats_[categoryId].useBufferNum_++;
		pgStats_.useBufferNum_++;

		if (doFix) {
			bufferInfo->fixFirstTime(refId_);  
		}
		else {
			if (pgStats_.swapOutCount_ < storeMemoryAgingSwapCount_) {
				swapList_.push(*bufferInfo);
			}
			else {
				swapList_.pushCold(*bufferInfo);
			}
		}
		EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);
		int64_t ioTime = fileManager_.readChunk(bufferInfo->buffer_, 1, readPos);
		fileManager_.isValidCheckSum(bufferInfo->buffer_);
		TEST_DIRTY_CHECK_SET(pId, categoryId, cId, bufferInfo->buffer_);
		int64_t compTime = fileManager_.uncompressChunk(bufferInfo->buffer_);
		pgStats_.swapReadUncompressTime_ += compTime;
		pgStats_.categoryStats_[categoryId].swapReadCount_++;
		pgStats_.atomicSwapReadCount_++;
		pgStats_.swapReadTime_ += ioTime;
		if (pgStats_.swapOutCount_ < storeMemoryAgingSwapCount_) {
			pgStats_.swapNormalReadCount_++;
		}
		else {
			pgStats_.swapColdBufferingReadCount_++;
		}
	}

	if (bufferInfo->latestPos_ != -1 &&
		bufferInfo->latestPos_ != checkpointPos) {
		getCheckpointFile().setUsedBlockInfo(bufferInfo->latestPos_, false);
	}

	bufferInfo->latestPos_ = -1;
	if (!bufferInfo->isDirty_) {
		bufferInfo->isDirty_ = true;
	}
	assert(bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_BUFFER));
	assert(bufferInfo->buffer_ &&
		   ChunkHeader::getPartitionId(bufferInfo->buffer_) == pId &&
		   ChunkHeader::getChunkCategoryId(bufferInfo->buffer_) == categoryId &&
		   ChunkHeader::getChunkId(bufferInfo->buffer_) == cId);
	assert(bufferInfoList_.get(getBufferId(pId, categoryId, cId))->latestPos_ ==
		   -1);

	return bufferInfo->buffer_;
}
uint8_t* ChunkManager::BufferManager::get(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, int64_t checkpointPos,
	ChunkManager::BufferInfo*& bufferInfo) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	bufferInfo = bufferInfoList_.get(getBufferId(pId, categoryId, cId));

	int64_t readPos = -1;
	if (!bufferInfo) {
		bufferInfo = bufferInfoPool_.allocate();
		bufferInfo->pId_ = pId;
		bufferInfo->categoryId_ = categoryId;
		bufferInfo->cId_ = cId;
		bufferInfoList_.append(getBufferId(pId, categoryId, cId), bufferInfo);
		assert(!bufferInfo->isDirty_);  
		assert(bufferInfo->latestPos_ == -1);
		readPos = checkpointPos;
	}
	else {
		readPos = bufferInfo->latestPos_;
	}

	if (bufferInfo->buffer_) {
		bool isFirst = bufferInfo->fix(refId_);
		if (isFirst) {
			swapList_.remove(*bufferInfo);
		}
	}
	else {
		assert(readPos != -1);

		adjustStoreMemory(true);

		bufferInfo->buffer_ = pgPool_.allocate();
		pgStats_.categoryStats_[categoryId].useBufferNum_++;
		pgStats_.useBufferNum_++;

		bufferInfo->fixFirstTime(refId_);  

		EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);
		int64_t ioTime = fileManager_.readChunk(bufferInfo->buffer_, 1, readPos);
		fileManager_.isValidCheckSum(bufferInfo->buffer_);
		TEST_DIRTY_CHECK_SET(pId, categoryId, cId, bufferInfo->buffer_);
		int64_t compTime = fileManager_.uncompressChunk(bufferInfo->buffer_);
		pgStats_.swapReadUncompressTime_ += compTime;
		pgStats_.categoryStats_[categoryId].swapReadCount_++;
		pgStats_.atomicSwapReadCount_++;
		pgStats_.swapReadTime_ += ioTime;
		if (pgStats_.swapOutCount_ < storeMemoryAgingSwapCount_) {
			pgStats_.swapNormalReadCount_++;
		}
		else {
			pgStats_.swapColdBufferingReadCount_++;
		}
	}

	assert(bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_BUFFER) ||
		   bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_BOTH) ||
		   bufferInfo->isValid(BUFFER_MANAGER_STATE_RESERVED) ||
		   bufferInfo->isValid(BUFFER_MANAGER_STATE_CLEAN_BUFFER));
	assert(bufferInfo->buffer_ &&
		   ChunkHeader::getPartitionId(bufferInfo->buffer_) == pId &&
		   ChunkHeader::getChunkCategoryId(bufferInfo->buffer_) == categoryId &&
		   ChunkHeader::getChunkId(bufferInfo->buffer_) == cId);

	return bufferInfo->buffer_;
}

void ChunkManager::BufferManager::freeCheckpointBuffer(
	uint8_t* checkpointBuffer) {
	if (checkpointBuffer) {
		ChunkCategoryId categoryId =
			ChunkHeader::getChunkCategoryId(checkpointBuffer);
		pgPool_.free(checkpointBuffer, atomicMemoryLimitNum_);
		checkpointBuffer = NULL;
		assert(0 < pgStats_.categoryStats_[categoryId].useBufferNum_);
		pgStats_.categoryStats_[categoryId].useBufferNum_--;
		assert(0 < pgStats_.useBufferNum_);
		pgStats_.useBufferNum_--;
	}
	assert(!checkpointBuffer);
}
bool ChunkManager::BufferManager::startCheckpoint(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, int64_t& swapPos) {
	assert(pgId_ == config_.getPartitionGroupId(pId));

	currentCPPId_ = pId;

	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));

	TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, false);

	if (bufferInfo) {
		swapPos = bufferInfo->latestPos_;
		bufferInfo->newCheckpointPos_ =
			-1;  
		if (bufferInfo->isDirty_) {
			if (bufferInfo->buffer_) {
				updateChunkCheckpointId(getPartitionInfo(pId).lastCpId_,
					getPartitionInfo(pId).startCpId_, bufferInfo->buffer_);
				TEST_DIRTY_CHECK_SET(pId, categoryId, cId, bufferInfo->buffer_);

				if (swapPos == -1) {
					assert(
						bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_BUFFER));
					bufferInfo->isDirty_ = false;
					bufferInfo->newCheckpointPos_ =
						getCheckpointFile().allocateBlock();  
					bufferInfo->toCopy_ = true;
					swapPos = bufferInfo->newCheckpointPos_;
					assert(bufferInfo->isValid(BUFFER_MANAGER_STATE_RESERVED));
					assert(-1 < swapPos);
					return true;  
				}
				else {
					assert(
						bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_BOTH));
					bufferInfo->isDirty_ = false;
					bufferInfo->latestPos_ = -1;
					assert(
						bufferInfo->isValid(BUFFER_MANAGER_STATE_CLEAN_BUFFER));
					assert(-1 < swapPos);
				}
			}
			else {
				assert(bufferInfo->isValid(BUFFER_MANAGER_STATE_DIRTY_FILE));
				bufferInfo->isDirty_ = false;
				bufferInfo->latestPos_ = -1;

				assert(!bufferInfo->buffer_);  
				assert(bufferInfo->latestPos_ == -1);
				assert(bufferInfo->newCheckpointPos_ == -1);

				bufferInfoPool_.deallocate(
					bufferInfoList_.remove(getBufferId(pId, categoryId, cId)));
				assert(-1 < swapPos);
			}
		}
	}
	else {
		swapPos = -1;
	}
	return false;  
}
void ChunkManager::BufferManager::endCheckpoint() {
	currentCPPId_ = UNDEF_PARTITIONID;
}
void ChunkManager::BufferManager::getCheckpointBuffer(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, int64_t& checkpointPos,
	uint8_t*& checkpointBuffer) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));

	if (bufferInfo && bufferInfo->newCheckpointPos_ != -1 &&
		bufferInfo->toCopy_) {
		checkpointPos = bufferInfo->newCheckpointPos_;
		checkpointBuffer = bufferInfo->buffer_;  
		bufferInfo->toCopy_ =
			false;  
		assert(
			checkpointBuffer &&
			ChunkHeader::getPartitionId(checkpointBuffer) == pId &&
			ChunkHeader::getChunkCategoryId(checkpointBuffer) == categoryId &&
			ChunkHeader::getChunkId(checkpointBuffer) == cId);
	}
	else {
		checkpointPos = -1;
		checkpointBuffer = NULL;  
	}
}
bool ChunkManager::BufferManager::enwarm(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, int64_t checkpointPos) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	if (getPGUseBufferNum() + 1 < getAtomicPGMemoryLimitNum()) {
		uint8_t* buffer = NULL;
		try {
			buffer = pgPool_.allocate();  
			assert(buffer);
			pgStats_.categoryStats_[categoryId].useBufferNum_++;
			pgStats_.useBufferNum_++;

			int64_t ioTime = fileManager_.readChunk(buffer, 1, checkpointPos);
			fileManager_.isValidCheckSum(buffer);
			TEST_DIRTY_CHECK_SET(pId, categoryId, cId, buffer);
			int64_t compTime = fileManager_.uncompressChunk(buffer);
			pgStats_.enwarmReadUncompressTime_ += compTime;
			pgStats_.enwarmReadCount_++;
			pgStats_.enwarmReadTime_ += ioTime;

			BufferInfo* bufferInfo = bufferInfoPool_.allocate();
			bufferInfo->pId_ = pId;
			bufferInfo->categoryId_ = categoryId;
			bufferInfo->cId_ = cId;
			bufferInfoList_.append(
				getBufferId(pId, categoryId, cId), bufferInfo);
			bufferInfo->buffer_ = buffer;
			assert(bufferInfoList_.get(getBufferId(pId, categoryId, cId))
					   ->isValid(BUFFER_MANAGER_STATE_CLEAN_BUFFER));
			assert(
				bufferInfoList_.get(getBufferId(pId, categoryId, cId))->cId_ ==
				cId);
			swapList_.push(*bufferInfo);

			assert(bufferInfo->buffer_ &&
				   ChunkHeader::getPartitionId(bufferInfo->buffer_) == pId &&
				   ChunkHeader::getChunkCategoryId(bufferInfo->buffer_) ==
					   categoryId &&
				   ChunkHeader::getChunkId(bufferInfo->buffer_) == cId);
		}
		catch (std::exception e) {
			pgPool_.free(buffer,
				static_cast<uint32_t>(config_.getAtomicStoreMemoryLimitNum()));
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		return true;
	}
	return false;
}
bool ChunkManager::BufferManager::isOnBuffer(
	PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));
	BufferInfo* bufferInfo =
		bufferInfoList_.get(getBufferId(pId, categoryId, cId));
	return (bufferInfo) && (bufferInfo->buffer_);
}
std::string ChunkManager::BufferManager::dumpDirtyState(PartitionId pId,
	ChunkCategoryId categoryId, ChunkId cId, BufferInfo* bufferInfo) {
	util::NormalOStringStream stream;
	stream << ",pId," << pId << ",categoryId," << (int32_t)categoryId << ",cId,"
		   << cId;
	if (bufferInfo) {
		stream << ",isDirty," << bufferInfo->isDirty_;
		if (bufferInfo->buffer_) {
			stream << ",checkSum,"
				   << ChunkHeader::calcCheckSum(bufferInfo->buffer_);
		}
		stream << ",lruPrev_," << bufferInfo->lruPrev_;
		stream << ",lruNext_," << bufferInfo->lruNext_;
		stream << ",refCount_," << bufferInfo->refCount_;
	}
	else {
		stream << ",isDirty,0";
	}
	return stream.str();
}


ChunkManager::MemoryLimitManager::MemoryLimitManager(Config& config,
	PartitionGroupDataList& partitionGroupData, MemoryPool& memoryPool)
	: partitionGroupNum_(config.getPartitionGroupNum()),
	  minTotalLimitNum_(partitionGroupNum_ * 2),
	  maxShiftNum_(config.getMaxOnceSwapNum()),
	  lastTotalLimitNum_(0),
	  load_(partitionGroupNum_),
	  limitNum_(NULL),
	  shiftableMemRate_(config.getAtomicShiftableMemRate()),
	  lastEMAHalfLifePeriod_(config.getAtomicEMAHalfLifePeriod()),
	  smoothingConstant_(0.003),
	  config_(config),
	  partitionGroupData_(partitionGroupData),
	  memoryPool_(memoryPool) {
	assert(0 < config.getMaxOnceSwapNum());
	assert(0 < maxShiftNum_);
	try {
		limitNum_ = UTIL_NEW uint64_t[partitionGroupNum_];
		memset(limitNum_, 0, sizeof(uint64_t) * partitionGroupNum_);
		lastBaseLimit_.assign(partitionGroupNum_, 0);
		smoothingConstant_ =
				2.0 / (static_cast<double>(lastEMAHalfLifePeriod_)
					   * CALC_EMA_HALF_LIFE_CONSTANT + 1.0);
		GS_TRACE_INFO(
				CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
				"MemoryLimitManger: EMAHalfLifePeriod initialize: emaHalfLifePeriod," <<
				lastEMAHalfLifePeriod_ << ",smoothingConstant," << smoothingConstant_ );
	}
	catch (std::exception& e) {
		delete[] limitNum_;
		limitNum_ = NULL;
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
ChunkManager::MemoryLimitManager::~MemoryLimitManager() {
	delete[] limitNum_;
	limitNum_ = NULL;
}

void ChunkManager::MemoryLimitManager::calcParameter(
		uint64_t &avgNum, uint64_t &minNum) {
	uint64_t totalLimitNum = config_.getAtomicStoreMemoryLimitNum();  
	avgNum = totalLimitNum / partitionGroupNum_;
	if (avgNum == 0) {
		avgNum = 1;
	}

	minNum = avgNum -
			(totalLimitNum * shiftableMemRate_) /
			(100 * partitionGroupNum_);

	if (minNum < 1) {
		minNum = 1;  
	}

}

void ChunkManager::MemoryLimitManager::redistributeByEMA(
		uint64_t avgNum, uint64_t minNum,
		uint64_t totalLimitNum) {
	static_cast<void>(avgNum);
	uint64_t limitTotal = 0;
	double estimatedTotal = 0.0;
	uint64_t lastBaseLimitTotal = 0;
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
		limitTotal += limitNum_[pgId];
		estimatedTotal += load_.newEstimatedLoad_[pgId];
		lastBaseLimitTotal += lastBaseLimit_[pgId];
	}
	assert(totalLimitNum >= limitTotal);
	uint64_t targetLimit = totalLimitNum;
	if (estimatedTotal <= DBL_EPSILON) {
		return;
	}
	PartitionGroupId maxPgId = 0;
	uint64_t maxLimit = 0;
	uint64_t subTotal = 0;
	assert(targetLimit >= minNum * partitionGroupNum_);
	targetLimit -= minNum * partitionGroupNum_;
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
		lastBaseLimit_[pgId] = static_cast<uint64_t>(
				static_cast<double>(targetLimit)
				* load_.newEstimatedLoad_[pgId] / estimatedTotal) + minNum;
		uint64_t pgUseChunkNum = getPGMetaChunkManager(pgId).getAtomicPGUseChunkNum();
		if (pgUseChunkNum < minNum) {
			pgUseChunkNum = minNum;
		}
		if (lastBaseLimit_[pgId] > pgUseChunkNum) {
			lastBaseLimit_[pgId] = pgUseChunkNum;
		}
		assert(lastBaseLimit_[pgId] >= minNum);
		subTotal += lastBaseLimit_[pgId];
		if (maxLimit < lastBaseLimit_[pgId]) {
			maxLimit = lastBaseLimit_[pgId];
			maxPgId = pgId;
		}
		limitNum_[pgId] = lastBaseLimit_[pgId];
	}

	if (subTotal != totalLimitNum) {
		assert(subTotal < totalLimitNum);
		int64_t diff = static_cast<int64_t>(totalLimitNum)
				- static_cast<int64_t>(subTotal);
		if (diff >= partitionGroupNum_) {
			int64_t addition = diff / partitionGroupNum_;
			for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
				lastBaseLimit_[pgId] += addition;
				limitNum_[pgId] = lastBaseLimit_[pgId];
			}
			diff -= addition * partitionGroupNum_;
		}
		if (diff >= 0) {
			lastBaseLimit_[maxPgId] += diff;
			limitNum_[maxPgId] = lastBaseLimit_[maxPgId];
		}
		else {
			assert(false);
			diff = -diff;
			if (lastBaseLimit_[maxPgId] > diff + minNum) {
				lastBaseLimit_[maxPgId] -= diff;
			}
			else {
				lastBaseLimit_[maxPgId] = minNum;
			}
			limitNum_[maxPgId] = lastBaseLimit_[maxPgId];
		}
	}
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
		getPGBufferManager(pgId).setAtomicPGMemoryLimitNum(limitNum_[pgId]);  
	}
}

void ChunkManager::MemoryLimitManager::redistributeMemoryLimit(
	PartitionGroupId executePgId) {
	if (executePgId != DISTRIBUTOR_PARTITION_GROUP_ID_) {
		return;
	}
	assert(0 < partitionGroupNum_);
	uint64_t totalLimitNum = config_.getAtomicStoreMemoryLimitNum();  
	uint64_t avgNum = totalLimitNum / partitionGroupNum_;

	int32_t emaHalfLifePeriod = config_.getAtomicEMAHalfLifePeriod();  
	if (lastEMAHalfLifePeriod_ != emaHalfLifePeriod) {
		smoothingConstant_ =
				2.0 / (static_cast<double>(emaHalfLifePeriod)
					   * CALC_EMA_HALF_LIFE_CONSTANT + 1.0);
		GS_TRACE_INFO(
				CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
				"MemoryLimitManger: EMAHalfLifePeriod change: emaHalfLifePeriod," <<
				emaHalfLifePeriod << ",smoothingConstant," << smoothingConstant_ );

		lastEMAHalfLifePeriod_ = emaHalfLifePeriod;
	}

	if (lastTotalLimitNum_ != totalLimitNum) {
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
			limitNum_[pgId] = avgNum;  
			getPGBufferManager(pgId).setAtomicPGMemoryLimitNum(
				limitNum_[pgId]);  
		}
	}
	else if (1 < partitionGroupNum_ && minTotalLimitNum_ <= totalLimitNum) {

		uint64_t minNum = avgNum -
						  (totalLimitNum * shiftableMemRate_) /
						  (100 * partitionGroupNum_);
		if (minNum < 1) {
			minNum = 1;  
		}

		getCurrentLoad(load_);

		redistributeByEMA(avgNum, minNum, totalLimitNum);
	}

	memoryPool_.setLimit(util::AllocatorStats::STAT_STABLE_LIMIT,
			config_.getAtomicStoreMemoryLimit());

	lastTotalLimitNum_ = totalLimitNum;
	assert(isValid());
}

void ChunkManager::MemoryLimitManager::getCurrentLoad(Load& load) {
	assert(1 < partitionGroupNum_);

	load.totalLoad_ = 0;  
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
		uint64_t lastReadCount = load.readCount_[pgId];
		load.readCount_[pgId] =
			getPGBufferManager(pgId).getAtomicPGSwapReadCount();  
		load.lastLoad_[pgId] = load.load_[pgId];
		load.load_[pgId] = load.readCount_[pgId] - lastReadCount;
		load.totalLoad_ += load.load_[pgId];

		load.lastEstimatedLoad_[pgId] = load.newEstimatedLoad_[pgId];
		load.newEstimatedLoad_[pgId] =
				smoothingConstant_ * static_cast<double>(load.load_[pgId]) +
				(1.0 - smoothingConstant_) * load.lastEstimatedLoad_[pgId];
	}
	load.avgLoad_ = load.totalLoad_ / partitionGroupNum_;
}

bool ChunkManager::MemoryLimitManager::isValid() const {
	assert(0 < partitionGroupNum_);

	bool isValidPGMemoryLimitNum = true;
	uint64_t totalLimitNum = 0;
	if (lastTotalLimitNum_ < minTotalLimitNum_) {
		uint64_t averageLimitNum = lastTotalLimitNum_ / partitionGroupNum_;

		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
			totalLimitNum += limitNum_[pgId];
			if (limitNum_[pgId] != averageLimitNum) {
				isValidPGMemoryLimitNum = false;
				break;
			}
		}
	}
	else {
		uint64_t pgMinLimitNum =
			lastTotalLimitNum_ / partitionGroupNum_
			- (static_cast<uint64_t>(
					static_cast<double>(lastTotalLimitNum_) * shiftableMemRate_)
				/ 100 / partitionGroupNum_);
		if (pgMinLimitNum < 1) {
			pgMinLimitNum = 1;  
		}

		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
			totalLimitNum += limitNum_[pgId];
			if (limitNum_[pgId] < pgMinLimitNum) {
				isValidPGMemoryLimitNum = false;
				break;
			}
		}
	}

	bool isValidMemoryLimitNum = (totalLimitNum <= lastTotalLimitNum_);

	bool isValid = isValidPGMemoryLimitNum && isValidMemoryLimitNum;
	if (!isValid) {
		std::cout << dump();
	}
	return isValid;
}

std::string ChunkManager::MemoryLimitManager::dump() const {
	util::NormalOStringStream stream;

	uint64_t minNum =
		lastTotalLimitNum_ / config_.getPartitionGroupNum() -
		static_cast<uint64_t>(
			static_cast<double>(lastTotalLimitNum_) * shiftableMemRate_) /
			100 / partitionGroupNum_;
	bool adjustable = (maxShiftNum_ <= minNum);

	stream << "MemoryLimitManager"
		   << ",total"
		   << ",partitionGroupNum_," << partitionGroupNum_ << ",poolLimitNum,"
		   << config_.getAtomicStoreMemoryLimitNum() << ",lastPoolLimitNum,"
		   << lastTotalLimitNum_ << ",minLimitNumForAdjust_," << maxShiftNum_;
	if (adjustable) {
		stream << ",adjustable" << std::endl;
	}
	else {
		stream << ",non-adjustable" << std::endl;
	}

	stream << "MemoryLimitManager"
		   << ",pgAverage"
		   << ",limitNum," << lastTotalLimitNum_ / partitionGroupNum_;
	if (adjustable) {
		uint64_t nonAdjustLimitRate =
			minNum * 100 / (lastTotalLimitNum_ / partitionGroupNum_);
		uint64_t adjustLimitNum =
			static_cast<uint64_t>(static_cast<double>(lastTotalLimitNum_) *
								  ONCE_SHIFTABLE_MEMORY_RATE / 100);
		if (adjustLimitNum < 1) {
			adjustLimitNum = 1;  
		}
		else if (maxShiftNum_ < adjustLimitNum) {
			adjustLimitNum =
				maxShiftNum_;  
		}
		uint64_t onceAdjustLimitRate =
			adjustLimitNum * 100 / (lastTotalLimitNum_ / partitionGroupNum_);
		stream << ",adjustLimitNum,"
			   << (lastTotalLimitNum_ / partitionGroupNum_) - minNum
			   << ",nonAdjustLimitNum," << minTotalLimitNum_
			   << ",nonAdjustLimitRate%," << nonAdjustLimitRate
			   << ",onceAdjustLimitNum," << adjustLimitNum
			   << ",onceAdjustLimitRate%," << onceAdjustLimitRate;
	}
	stream << std::endl;

	stream << "MemoryLimitManager"
		   << ",etc"
		   << ",maxShiftNum_," << maxShiftNum_ << std::endl;

	stream << "pgId,pgLimitNum,pgReadCount" << std::endl;
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
		stream << pgId << "," << limitNum_[pgId] << ","
			   << load_.readCount_[pgId] << std::endl;
	}

	return stream.str();
}

ChunkManager::PartitionAffinityChunkInfoTable::PartitionAffinityChunkInfoTable(
		PartitionGroupAllocator& alloc, PartitionId pId,
		ChunkCategoryId chunkCategoryNum, uint32_t expireCategoryNum,
		uint64_t affinitySize,
		ChunkManager* chunkManager) :
		alloc_(alloc), chunkManager_(chunkManager),
		pId_(pId), chunkCategoryNum_(chunkCategoryNum),
		expireCategoryNum_(expireCategoryNum),
		affinitySize_(affinitySize),
		infoTableList_(alloc) {
	infoTableList_.assign(chunkCategoryNum, NULL);
}

ChunkManager::PartitionAffinityChunkInfoTable::~PartitionAffinityChunkInfoTable() {
	for (ChunkCategoryId categoryId = 0;
			categoryId < chunkCategoryNum_;
		 	++categoryId) {
		if (infoTableList_[categoryId]) {
			if (chunkManager_->isBatchFreeMode(categoryId)) {
				BatchFreeChunkInfoTable* infoTable =
					static_cast<BatchFreeChunkInfoTable*>(infoTableList_[categoryId]);
				delete infoTable;
				infoTableList_[categoryId] = NULL;
			}
			else {
				NormalChunkInfoTable* infoTable =
						static_cast<NormalChunkInfoTable*>(infoTableList_[categoryId]);
				delete infoTable;
				infoTableList_[categoryId] = NULL;
			}
		}
	}
}

ChunkId ChunkManager::PartitionAffinityChunkInfoTable::getAffinityChunkId(
		ChunkCategoryId categoryId, const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey) const {
	assert(categoryId < chunkCategoryNum_);
	if (infoTableList_[categoryId]) {
		if (chunkManager_->isBatchFreeMode(categoryId)) {
			BatchFreeChunkInfoTable* infoTable =
					static_cast<BatchFreeChunkInfoTable*>(infoTableList_[categoryId]);
			return infoTable->getAffinityChunkId(
					affinityInfo, chunkKey);
		}
		else {
			NormalChunkInfoTable* infoTable =
					static_cast<NormalChunkInfoTable*>(infoTableList_[categoryId]);
			return infoTable->getAffinityChunkId(
					affinityInfo, chunkKey);
		}
	}
	else {
		return UNDEF_CHUNKID;
	}
}

bool ChunkManager::PartitionAffinityChunkInfoTable::setAffinityChunkId(
		ChunkCategoryId categoryId, const DataAffinityInfo &affinityInfo,
		ChunkKey chunkKey, ChunkId chunkId) {
	assert(categoryId < chunkCategoryNum_);
	if (infoTableList_[categoryId]) {
		if (chunkManager_->isBatchFreeMode(categoryId)) {
			BatchFreeChunkInfoTable* infoTable =
					static_cast<BatchFreeChunkInfoTable*>(infoTableList_[categoryId]);
			return infoTable->setAffinityChunkId(
					affinityInfo, chunkKey, chunkId);
		}
		else {
			NormalChunkInfoTable* infoTable =
					static_cast<NormalChunkInfoTable*>(infoTableList_[categoryId]);
			return infoTable->setAffinityChunkId(
					affinityInfo, chunkKey, chunkId);
		}
	}
	else {
		if (chunkManager_->isBatchFreeMode(categoryId)) {
			BatchFreeChunkInfoTable* infoTable = UTIL_NEW BatchFreeChunkInfoTable(
					alloc_, pId_, chunkCategoryNum_, expireCategoryNum_,
					affinitySize_);
			infoTableList_[categoryId] = infoTable;
			return infoTable->setAffinityChunkId(
					affinityInfo, chunkKey, chunkId);
		}
		else {
			NormalChunkInfoTable* infoTable = UTIL_NEW NormalChunkInfoTable(
					alloc_, pId_, chunkCategoryNum_, expireCategoryNum_,
					affinitySize_);
			infoTableList_[categoryId] = infoTable;
			return infoTable->setAffinityChunkId(
					affinityInfo, chunkKey, chunkId);
		}
	}
}

void ChunkManager::PartitionAffinityChunkInfoTable::purge(
		ChunkCategoryId categoryId, Timestamp baseTime){
	assert(categoryId < chunkCategoryNum_);
	if (infoTableList_[categoryId]) {
		if (chunkManager_->isBatchFreeMode(categoryId)) {
			BatchFreeChunkInfoTable* infoTable =
					static_cast<BatchFreeChunkInfoTable*>(infoTableList_[categoryId]);
			infoTable->purge(baseTime);
		}
		else {
		}
	}
	else {
	}
}

size_t ChunkManager::PartitionAffinityChunkInfoTable::size() const {
	size_t totalSize = 0;
	for (ChunkCategoryId categoryId = 0; categoryId < chunkCategoryNum_;
			++categoryId) {
		if (infoTableList_[categoryId]) {
			if (chunkManager_->isBatchFreeMode(categoryId)) {
				BatchFreeChunkInfoTable* infoTable =
				  static_cast<BatchFreeChunkInfoTable*>(infoTableList_[categoryId]);
				totalSize += infoTable->size();
			}
			else {
				NormalChunkInfoTable* infoTable =
				  static_cast<NormalChunkInfoTable*>(infoTableList_[categoryId]);
				totalSize += infoTable->size();
			}
		}
		else {
		}
	}
	return totalSize;
}

void ChunkManager::PartitionAffinityChunkInfoTable::clear() {
	for (ChunkCategoryId categoryId = 0; categoryId < chunkCategoryNum_;
			++categoryId) {
		if (infoTableList_[categoryId]) {
			if (chunkManager_->isBatchFreeMode(categoryId)) {
				BatchFreeChunkInfoTable* infoTable =
						static_cast<BatchFreeChunkInfoTable*>(infoTableList_[categoryId]);
				delete infoTable;
				infoTableList_[categoryId] = NULL;
			}
			else {
				NormalChunkInfoTable* infoTable =
						static_cast<NormalChunkInfoTable*>(infoTableList_[categoryId]);
				delete infoTable;
				infoTableList_[categoryId] = NULL;
			}
		}
	}
}

void ChunkManager::PartitionAffinityChunkInfoTable::initialize(Timestamp baseTime) {
	static_cast<void>(baseTime);
	clear();
}

ChunkManager::NormalChunkInfoTable::NormalChunkInfoTable(
		PartitionGroupAllocator& allocator, PartitionId pId,
		ChunkCategoryId chunkCategoryId, uint32_t expireCategoryNum,
		uint64_t affinitySize) :
		alloc_(allocator), chunkInfoTableList_(allocator), pId_(pId),
		chunkCategoryId_(chunkCategoryId),
		expireCategoryNum_(expireCategoryNum),
		affinitySize_(affinitySize) {
	initialize();
}

ChunkManager::NormalChunkInfoTable::~NormalChunkInfoTable() {
}

ChunkId ChunkManager::NormalChunkInfoTable::getAffinityChunkId(
		const DataAffinityInfo &affinityInfo,
		ChunkKey chunkKey) const {
	static_cast<void>(chunkKey);
	size_t pos = affinityInfo.dataAffinity_ % chunkInfoTableList_.size();
	return chunkInfoTableList_[pos];
}

bool ChunkManager::NormalChunkInfoTable::setAffinityChunkId(
		const DataAffinityInfo &affinityInfo,
		ChunkKey chunkKey, ChunkId chunkId) {
	static_cast<void>(chunkKey);
	size_t pos = affinityInfo.dataAffinity_ % chunkInfoTableList_.size();
	chunkInfoTableList_[pos] = chunkId;
	return true;
}

void ChunkManager::NormalChunkInfoTable::purge(Timestamp baseTime){
	static_cast<void>(baseTime);
}

void ChunkManager::NormalChunkInfoTable::clear() {
	chunkInfoTableList_.clear();
	chunkInfoTableList_.assign(affinitySize_, UNDEF_CHUNKID);
}

void ChunkManager::NormalChunkInfoTable::initialize() {
	chunkInfoTableList_.clear();
	chunkInfoTableList_.assign(affinitySize_, UNDEF_CHUNKID);
}

ChunkManager::BatchFreeChunkInfoList::BatchFreeChunkInfoList(
		PartitionGroupAllocator &alloc,
		ExpireIntervalCategoryId expireCategory,
		uint64_t startBaseTime) :
		chunkInfoDeque_(alloc), expireCategory_(expireCategory),
		roundingBitNum_(
				DataAffinityUtils::getExpireTimeRoundingBitNum(expireCategory)) {
	roundingMask_ = static_cast<ChunkKey>(
			UINT64_C(1) << (roundingBitNum_ - CHUNKKEY_BIT_NUM)) - 1;
	initialize(startBaseTime);
}

ChunkManager::BatchFreeChunkInfoList::~BatchFreeChunkInfoList() {
}

void ChunkManager::BatchFreeChunkInfoList::clear() {
	chunkInfoDeque_.clear();
}

void ChunkManager::BatchFreeChunkInfoList::initialize(Timestamp startBaseTime) {
	chunkInfoDeque_.clear();
	ChunkKey baseChunkKey = static_cast<ChunkKey>(
			startBaseTime >> CHUNKKEY_BIT_NUM);
	if ((startBaseTime & CHUNKKEY_BIT_NUM) != 0) {
		baseChunkKey++;
	}
	ChunkKey maxChunkKey =
			((baseChunkKey >> (roundingBitNum_ - CHUNKKEY_BIT_NUM)) + 1)
			<< (roundingBitNum_ - CHUNKKEY_BIT_NUM);
	minChunkKey_ = maxChunkKey - (KEEP_NUM << (roundingBitNum_ - CHUNKKEY_BIT_NUM));
	assert(((maxChunkKey - minChunkKey_) >> (roundingBitNum_ - CHUNKKEY_BIT_NUM)) == KEEP_NUM);
	ChunkKey pos = KEEP_NUM;
	for (ChunkKey i = 0; i <= pos; ++i) {
		AffinityChunkInfo chunkInfo;
		chunkInfo.first = minChunkKey_ + (i << (roundingBitNum_ - CHUNKKEY_BIT_NUM));
		chunkInfo.second = UNDEF_CHUNKID;
		chunkInfoDeque_.push_back(chunkInfo);
	}
	assert(chunkInfoDeque_[pos].first == maxChunkKey);
	assert(chunkInfoDeque_[pos].second == UNDEF_CHUNKID);
}

ChunkId ChunkManager::BatchFreeChunkInfoList::find(ChunkKey chunkKey) {
	assert((chunkKey & ~roundingMask_) == chunkKey);
	size_t pos = (chunkKey - minChunkKey_) >> (roundingBitNum_ - CHUNKKEY_BIT_NUM);
	if (pos < chunkInfoDeque_.size()) {
		assert(chunkInfoDeque_[pos].first == chunkKey);
		return chunkInfoDeque_[pos].second;
	}
	else {
		return UNDEF_CHUNKID;
	}
}

bool ChunkManager::BatchFreeChunkInfoList::set(ChunkKey chunkKey, ChunkId chunkId) {
	assert((chunkKey & ~roundingMask_) == chunkKey);
	if (chunkKey >= minChunkKey_) {
		size_t pos = (chunkKey - minChunkKey_) >> (roundingBitNum_ - CHUNKKEY_BIT_NUM);
		if (pos >= chunkInfoDeque_.size()) {
			expand(chunkKey);
		}
		assert(chunkInfoDeque_[pos].first == chunkKey);
		chunkInfoDeque_[pos].second = chunkId;
		return true;
	}
	else {
		return false;
	}
}

void ChunkManager::BatchFreeChunkInfoList::purge(Timestamp baseTime) {
	ChunkKey baseChunkKey = static_cast<ChunkKey>(
			baseTime >> CHUNKKEY_BIT_NUM);
	if ((baseTime & CHUNKKEY_BIT_NUM) != 0) {
		baseChunkKey++;
	}
	ChunkKey maxChunkKey =
			((baseChunkKey >> (roundingBitNum_ - CHUNKKEY_BIT_NUM)) + 1)
			<< (roundingBitNum_ - CHUNKKEY_BIT_NUM);
	ChunkKey newMinChunkKey = maxChunkKey - (KEEP_NUM << (roundingBitNum_ - CHUNKKEY_BIT_NUM));
	if (newMinChunkKey <= minChunkKey_) {
		return;
	}
	assert(((maxChunkKey - newMinChunkKey) >> (roundingBitNum_ - CHUNKKEY_BIT_NUM)) == KEEP_NUM);
	assert((newMinChunkKey & ~roundingMask_) == newMinChunkKey);
	if (newMinChunkKey > minChunkKey_) {
		size_t pos = (newMinChunkKey - minChunkKey_) >> (roundingBitNum_ - CHUNKKEY_BIT_NUM);
		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				"BatchFreeChunkInfoList::purge: baseTime," << baseTime <<
				",oldMinChunkKey," << minChunkKey_ <<
				",newMinChunkKey," << newMinChunkKey <<
				",newHeadPos," << pos);
		if (pos < chunkInfoDeque_.size()) {
			for (size_t i = 0; i < pos; ++i) {
				chunkInfoDeque_.pop_front();
			}
		}
		else {
			chunkInfoDeque_.clear();
			AffinityChunkInfo chunkInfo;
			chunkInfo.first = newMinChunkKey;
			chunkInfo.second = UNDEF_CHUNKID;
			chunkInfoDeque_.push_back(chunkInfo);
		}
		assert(chunkInfoDeque_[0].first == newMinChunkKey);
		minChunkKey_ = newMinChunkKey;
	}
	else {
	}
}

void ChunkManager::BatchFreeChunkInfoList::expand(ChunkKey chunkKey) {
	assert((chunkKey & ~roundingMask_) == chunkKey);
	size_t pos = (chunkKey - minChunkKey_) >> (roundingBitNum_ - CHUNKKEY_BIT_NUM);
	size_t startSize = chunkInfoDeque_.size();
	assert(pos >= startSize);
	for (size_t i = startSize; i <= pos; ++i) {
		AffinityChunkInfo chunkInfo;
		chunkInfo.first = static_cast<ChunkKey>(
				minChunkKey_ + (i << (roundingBitNum_ - CHUNKKEY_BIT_NUM)));
		chunkInfo.second = UNDEF_CHUNKID;
		chunkInfoDeque_.push_back(chunkInfo);
	}
	assert(chunkInfoDeque_[pos].first == chunkKey);
	assert(chunkInfoDeque_[pos].second == UNDEF_CHUNKID);
}

ChunkManager::BatchFreeChunkInfoTable::BatchFreeChunkInfoTable(
		PartitionGroupAllocator& allocator, PartitionId pId,
		ChunkCategoryId chunkCategoryId, uint32_t expireCategoryNum,
		uint64_t affinitySize) :
		alloc_(allocator), chunkInfoTableList_(allocator), pId_(pId),
		chunkCategoryId_(chunkCategoryId),
		expireCategoryNum_(expireCategoryNum),
		affinitySize_(affinitySize) {
	chunkInfoTableList_.clear();
	chunkInfoTableList_.assign(expireCategoryNum_ * affinitySize_, NULL);
}

ChunkManager::BatchFreeChunkInfoTable::~BatchFreeChunkInfoTable() {
	const size_t total = expireCategoryNum_ * affinitySize_;
	for (size_t pos = 0; pos < total; ++pos) {
		if (chunkInfoTableList_[pos]) {
			delete chunkInfoTableList_[pos];
			chunkInfoTableList_[pos] = NULL;
		}
	}
}

ChunkId ChunkManager::BatchFreeChunkInfoTable::getAffinityChunkId(
		const DataAffinityInfo &affinityInfo,
		ChunkKey chunkKey) const {
	assert(affinityInfo.expireCategory_ < expireCategoryNum_);
	size_t affinity = affinityInfo.dataAffinity_ % affinitySize_;
	const size_t pos = affinityInfo.expireCategory_ * affinitySize_ + affinity;
	assert(pos < chunkInfoTableList_.size());
	BatchFreeChunkInfoList* chunkInfoList = chunkInfoTableList_[pos];
	if (chunkInfoList) {
		return chunkInfoList->find(chunkKey);
	}
	else {
		return UNDEF_CHUNKID;
	}
}

bool ChunkManager::BatchFreeChunkInfoTable::setAffinityChunkId(
		const DataAffinityInfo &affinityInfo,
		ChunkKey chunkKey, ChunkId chunkId) {
	assert(affinityInfo.expireCategory_ < expireCategoryNum_);
	size_t affinity = affinityInfo.dataAffinity_ % affinitySize_;
	const size_t pos = affinityInfo.expireCategory_ * affinitySize_ + affinity;
	assert(pos < chunkInfoTableList_.size());
	BatchFreeChunkInfoList* chunkInfoList = chunkInfoTableList_[pos];
	if (!chunkInfoList) {
		uint64_t ts = util::DateTime::now(false).getUnixTime();
		chunkInfoList = UTIL_NEW BatchFreeChunkInfoList(
				alloc_, affinityInfo.expireCategory_, ts);
		chunkInfoTableList_[pos] = chunkInfoList;
	}
	return chunkInfoList->set(chunkKey, chunkId);
}

void ChunkManager::BatchFreeChunkInfoTable::purge(
		Timestamp baseTime){
	for (size_t pos = 0; pos < expireCategoryNum_ * affinitySize_; ++pos) {
		BatchFreeChunkInfoList* chunkInfoList = chunkInfoTableList_[pos];
		if (chunkInfoList) {
			chunkInfoList->purge(baseTime);
		}
		else {
		}
	}
}

size_t ChunkManager::BatchFreeChunkInfoTable::size() const {
	size_t total = 0;
	for (size_t pos = 0; pos < expireCategoryNum_ * affinitySize_; ++pos) {
		BatchFreeChunkInfoList* chunkInfoList = chunkInfoTableList_[pos];
		if (chunkInfoList) {
			total += chunkInfoList->size();
		}
		else {
		}
	}
	return total;
}

void ChunkManager::BatchFreeChunkInfoTable::clear() {
	for (size_t pos = 0; pos < expireCategoryNum_ * affinitySize_; ++pos) {
		BatchFreeChunkInfoList* chunkInfoList = chunkInfoTableList_[pos];
		if (chunkInfoList) {
			delete chunkInfoList;
			chunkInfoTableList_[pos] = NULL;
		}
		else {
		}
	}
	chunkInfoTableList_.clear();
	chunkInfoTableList_.assign(expireCategoryNum_ * affinitySize_, NULL);
}

void ChunkManager::BatchFreeChunkInfoTable::initialize() {
}

const uint64_t ChunkManager::AffinityManager::MAX_AFFINITY_SIZE = 10000;
ChunkManager::AffinityManager::AffinityManager(int32_t chunkCategoryNum,
		uint64_t maxAffinitySize, uint64_t userSpecifiedAffinitySize,
		PartitionDataList& partitionData)
	: chunkCategoryNum_(chunkCategoryNum),
	  expireCategoryNum_(EXPIRE_INTERVAL_CATEGORY_COUNT),
	  maxAffinitySize_(maxAffinitySize),
	  affinitySize_(userSpecifiedAffinitySize),
	  partitionData_(partitionData)
{
	if (affinitySize_ < 1) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"limit exceeded. (specified data affinity size="
				<< affinitySize_
				<< ", lower limit=1)");
	}
	if (affinitySize_ > maxAffinitySize_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"limit exceeded. (specified data affinity size="
				<< affinitySize_
				<< ", upper limit=" << maxAffinitySize_ << ")");
	}
}

ChunkManager::AffinityManager::~AffinityManager() {}

void ChunkManager::AffinityManager::drop(PartitionId pId) {
	try {
		PartitionAffinityChunkInfoTable& affinityChunkInfoTable =
				getAffinityChunkInfoTable(pId);
		affinityChunkInfoTable.clear();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "pId," << pId << GS_EXCEPTION_MESSAGE(e));
	}
}

ChunkId ChunkManager::AffinityManager::getAffinityChunkId(
		PartitionId pId, ChunkCategoryId chunkCategoryId,
		const DataAffinityInfo &affinityInfo, ChunkKey chunkKey) const {
	return getAffinityChunkInfoTable(pId).getAffinityChunkId(
		chunkCategoryId, affinityInfo, chunkKey);
}
bool ChunkManager::AffinityManager::setAffinityChunkId(
		PartitionId pId, ChunkCategoryId chunkCategoryId,
		const DataAffinityInfo &affinityInfo, ChunkKey chunkKey,
		ChunkId cId, ChunkId& oldAffinityCId) {

	PartitionAffinityChunkInfoTable& affinityChunkInfoTable = getAffinityChunkInfoTable(pId);
	oldAffinityCId = affinityChunkInfoTable.getAffinityChunkId(
			chunkCategoryId, affinityInfo, chunkKey);
	if (cId != UNDEF_CHUNKID) {
		bool result = affinityChunkInfoTable.setAffinityChunkId(
				chunkCategoryId, affinityInfo,
				chunkKey, cId);
		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			"pId," << pId <<
			",chunkCategoryId," << static_cast<uint32_t>(chunkCategoryId) <<
			",expireCategoryId," << affinityInfo.expireCategory_ <<
			",dataAffinityId," << affinityInfo.dataAffinity_ << ",cId," << cId);
		return result;
	}
	else {
		return false;
	}
}
void ChunkManager::AffinityManager::purge(
		PartitionId pId, Timestamp baseTime) {
	PartitionAffinityChunkInfoTable& affinityChunkInfoTable =
			getAffinityChunkInfoTable(pId);
	for (ChunkCategoryId id = 0; static_cast<uint32_t>(id) < chunkCategoryNum_; ++id) {
		affinityChunkInfoTable.purge(id, baseTime);
	}
}


