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
	@brief Definition of CheckpointFile
*/
#ifndef CHECKPOINT_FILE_H_
#define CHECKPOINT_FILE_H_

#include "util/allocator.h"  
#include "util/code.h"		 
#include "util/container.h"
#include "util/file.h"
#include "util/time.h"  
#include "util/trace.h"
#include "data_type.h"
#include "bit_array.h"
#include "gs_error.h"

UTIL_TRACER_DECLARE(CHECKPOINT_FILE);

/*!
	@brief Operates CheckpoinFile I/O for Chunks of store data.
*/
class CheckpointFile {
	friend class ObjectManagerTest;
	friend class ChunkManagerCheckpointTest;
	friend class ChunkManager;

public:
	static const int32_t ALLOCATE_BLOCK_SEARCH_LIMIT = 1024;
	static const uint32_t FILE_SPLIT_COUNT_LIMIT = 128;
	static const uint32_t FILE_SPLIT_STRIPE_SIZE_LIMIT = 1024;

	CheckpointFile(
		uint8_t chunkExpSize, const std::string &dir, PartitionGroupId pgId,
		uint32_t splitCount, uint32_t stripeSize,
		const std::vector<std::string> configDirList);
	~CheckpointFile();

	void punchHoleBlock(uint32_t size, uint64_t offset);

	void zerofillUnusedBlock();

	int64_t writeBlock(const uint8_t *buffer, uint32_t size, uint64_t blockNo);
	int64_t readBlock(uint8_t *buffer, uint32_t size, uint64_t blockNo);

	int64_t writePartialBlock(const uint8_t *buffer, uint32_t size, uint64_t offset);

	uint64_t getWriteBlockCount();
	uint64_t getReadBlockCount();
	void resetWriteBlockCount();
	void resetReadBlockCount();
	uint64_t getReadRetryCount();
	uint64_t getWriteRetryCount();
	void resetReadRetryCount();
	void resetWriteRetryCount();

	int64_t allocateBlock();
	void freeBlock(uint64_t blockNo);

	void setUsedBlockInfo(uint64_t blockNo, bool flag);
	bool getUsedBlockInfo(uint64_t blockNo);
	void initializeUsedBlockInfo();
	void setValidBlockInfo(uint64_t blockNo, bool flag);
	bool getValidBlockInfo(uint64_t blockNo);
	inline int64_t getValidBlockInfoSize() const {
		return validChunkInfo_.length();
	};
	void initializeValidBlockInfo();

public:
	ChunkBitArray &getValidBitArray() {
		return validChunkInfo_;
	}
	ChunkBitArray &getUsedBitArray() {
		return usedChunkInfo_;
	}

	bool open(bool checkOnly = false, bool createMode = true);
	void close();
	void flush();
	void truncate();
	void advise(int32_t advise);

	inline uint64_t getBlockNum() const {
		return blockNum_;
	}
	inline uint64_t getUseBitNum() const {
		if (freeUseBitNum_ <= usedChunkInfo_.length()) {
			return usedChunkInfo_.length() - freeUseBitNum_;
		}
		else {
			return 0;
		}
	}
	inline uint64_t getFreeUseBitNum() const {
		return freeUseBitNum_;
	}
	void getLastChunkImage(int64_t chunkNth, CheckpointId lastCompCpId,
		std::vector<CheckpointId> &completeList, uint8_t *&chunkImage,
		int64_t &fileOffset, int32_t &fileId, CheckpointId &readCompCpId);

	inline void setIOWarningThresholdMillis(uint32_t millis) {
		ioWarningThresholdMillis_ = millis;
	}
	inline uint32_t getIOWarningThresholdMillis() {
		return ioWarningThresholdMillis_;
	}

	static bool checkFileName(const std::string &name, PartitionGroupId &pgId, int32_t &splitId);

	std::string dumpUsedChunkInfo();
	std::string dumpValidChunkInfo();

	int64_t getFileSize();
	int64_t getFileAllocateSize();
	static size_t getFileSystemBlockSize(const std::string &dir);
	size_t getFileSystemBlockSize();
	std::string dump();

	int64_t getSplitFileSize(uint32_t splitId);

	inline size_t calcFileNth(uint64_t offset) {
		return static_cast<size_t>((offset / (BLOCK_SIZE_ * stripeSize_)) % splitCount_);
	}

	inline uint64_t calcFileOffset(uint64_t offset) {
		uint64_t unit = offset / (BLOCK_SIZE_ * stripeSize_ * splitCount_);
		return unit * (BLOCK_SIZE_ * stripeSize_) + offset % (BLOCK_SIZE_ * stripeSize_);
	}

private:
	static const char8_t *const gsCpFileBaseName;
	static const char8_t *const gsCpFileExtension;
	static const char8_t *const gsCpFileSeparator;

	const uint64_t BLOCK_EXP_SIZE_;
	const uint64_t BLOCK_SIZE_;

	ChunkBitArray usedChunkInfo_;
	ChunkBitArray validChunkInfo_;
	uint64_t blockNum_;
	uint64_t freeUseBitNum_;
	uint64_t freeBlockSearchCursor_;
	PartitionGroupId pgId_;
	util::Mutex mutex_;  

	std::string dir_;
	bool splitMode_;
	std::vector<std::string> fileNameList_;
	std::vector<util::NamedFile *>fileList_;
	std::vector<std::string> dirList_;
	std::vector<uint64_t> blockCountList_;
	uint32_t splitCount_;
	uint64_t stripeSize_;
	uint64_t readBlockCount_;
	uint64_t writeBlockCount_;
	uint64_t readRetryCount_;
	uint64_t writeRetryCount_;
	uint32_t ioWarningThresholdMillis_;
};

#endif
