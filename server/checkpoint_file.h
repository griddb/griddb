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
#include "bit_array.h"
#include "data_type.h"

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

	CheckpointFile(
		uint8_t chunkExpSize, const std::string &dir, PartitionGroupId pgId);
	~CheckpointFile();

	void initialize(const char8_t *dir, PartitionGroupId pgId);

	void punchHoleBlock(uint32_t size, uint64_t offset);
	int64_t writeBlock(const uint8_t *buffer, uint32_t size, uint64_t blockNo);
	int64_t readBlock(uint8_t *buffer, uint32_t size, uint64_t blockNo);

	int64_t writePartialBlock(
		const uint8_t *buffer, uint32_t size, uint64_t offset);

	uint64_t getWriteBlockCount();
	uint64_t getReadBlockCount();
	void resetWriteBlockCount();
	void resetReadBlockCount();

	int64_t allocateBlock();
	void freeBlock(uint64_t blockNo);

	void setUsedBlockInfo(uint64_t blockNo, bool flag);
	bool getUsedBlockInfo(uint64_t blockNo);
	void initializeUsedBlockInfo(const uint8_t *bitList, const uint64_t bitNum);

	void setValidBlockInfo(uint64_t blockNo, bool flag);
	bool getValidBlockInfo(uint64_t blockNo);
	uint64_t saveValidBlockInfo(util::XArray<uint8_t> &buf);
	inline int64_t getValidBlockInfoSize() const {
		return validChunkInfo_.length();
	};
	void initializeValidBlockInfo(
		const uint8_t *bitList, const uint64_t bitNum);

public:
	BitArray &getValidBitArray() {
		return validChunkInfo_;
	}

	bool open(bool checkOnly = false, bool createMode = true);
	void close();
	void flush();
	void truncate();

	inline uint64_t getBlockNum() const {
		return blockNum_;
	}
	inline uint64_t getUseBitNum() const {
		assert(freeUseBitNum_ <= usedChunkInfo_.length());
		return usedChunkInfo_.length() - freeUseBitNum_;
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

	static bool checkFileName(const std::string &name, PartitionGroupId &pgId);

	std::string dumpUsedChunkInfo();
	std::string dumpValidChunkInfo();

	int64_t getFileSize();
	int64_t getFileAllocateSize();
	static size_t getFileSystemBlockSize(const std::string &dir);
	size_t getFileSystemBlockSize();
	std::string dump();

private:
	static const char8_t *const gsCpFileBaseName;
	static const char8_t *const gsCpFileExtension;
	static const char8_t *const gsCpFileSeparator;

	const Size_t BLOCK_EXP_SIZE_;
	const Size_t BLOCK_SIZE_;

	util::NamedFile *file_;
	BitArray usedChunkInfo_;
	BitArray validChunkInfo_;
	uint64_t blockNum_;
	uint64_t freeUseBitNum_;
	uint64_t freeBlockSearchCursor_;
	PartitionGroupId pgId_;
	std::string dir_;
	util::Mutex mutex_;  
	uint64_t readBlockCount_;
	uint64_t writeBlockCount_;
	uint32_t ioWarningThresholdMillis_;
	std::string fileName_;
};

#endif
