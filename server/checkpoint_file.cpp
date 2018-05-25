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
	@brief Implementation of CheckpointFile
*/

#include "util/trace.h"
#include "gs_error.h"
#include "config_table.h"
#include "checkpoint_file.h"
#include <algorithm>
#ifndef _WIN32
#include <fcntl.h>
#include <linux/falloc.h>
#endif

UTIL_TRACER_DECLARE(IO_MONITOR);

const char8_t *const CheckpointFile::gsCpFileBaseName = "gs_cp_";
const char8_t *const CheckpointFile::gsCpFileExtension = ".dat";
const char8_t *const CheckpointFile::gsCpFileSeparator = "_";

#define EXEC_FAILURE(errorNo)

/*!
	@brief Constructore of CheckpointFile.
*/
CheckpointFile::CheckpointFile(
	uint8_t chunkExpSize, const std::string &dir, PartitionGroupId pgId)
	: BLOCK_EXP_SIZE_(chunkExpSize),
	  BLOCK_SIZE_(1UL << chunkExpSize),
	  file_(NULL),
	  usedChunkInfo_(10240),
	  validChunkInfo_(10240),
	  blockNum_(0),
	  freeUseBitNum_(0),
	  freeBlockSearchCursor_(0),
	  pgId_(pgId),
	  dir_(dir),
	  readBlockCount_(0),
	  writeBlockCount_(0),
	  ioWarningThresholdMillis_(IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS) {
	;
}

/*!
	@brief Destructor of CheckpointFile.
*/
CheckpointFile::~CheckpointFile() {
	if (file_ && !file_->isClosed()) {
		file_->unlock();
		file_->close();
	}
	delete file_;
	file_ = NULL;
}

/*!
	@brief Open a file.
*/
bool CheckpointFile::open(bool checkOnly, bool createMode) {
	try {
		util::NormalOStringStream ss;
		if (!dir_.empty()) {
			if (util::FileSystem::exists(dir_.c_str())) {
				if (!util::FileSystem::isDirectory(dir_.c_str())) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_CF_INVALID_DIRECTORY,
						"Not direcotry: directoryName=" << dir_.c_str());
				}
			}
			else {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CF_INVALID_DIRECTORY,
					"Directory not found: directoryName=" << dir_.c_str());
			}
			ss << dir_ << "/";
		}
		ss << CheckpointFile::gsCpFileBaseName << pgId_
		   << CheckpointFile::gsCpFileSeparator;

		util::NormalOStringStream ssFile;
		ssFile << ss.str() << "1" << CheckpointFile::gsCpFileExtension;
		fileName_.assign(ssFile.str());

		if (util::FileSystem::exists(fileName_.c_str())) {
			if (file_) {
				delete file_;
				file_ = NULL;
			}
			file_ = UTIL_NEW util::NamedFile();
			file_->open(fileName_.c_str(),
				(checkOnly ? util::FileFlag::TYPE_READ_ONLY
						   : util::FileFlag::TYPE_READ_WRITE));
			if (!checkOnly) {
				file_->lock();
			}
			util::FileStatus status;
			file_->getStatus(&status);
			int64_t chunkNum =
				(status.getSize() + BLOCK_SIZE_ - 1) / BLOCK_SIZE_;
			blockNum_ = chunkNum;
			usedChunkInfo_.reserve(chunkNum + 1);
			usedChunkInfo_.set(chunkNum + 1, false);
			validChunkInfo_.reserve(chunkNum + 1);
			validChunkInfo_.set(chunkNum + 1, false);
			freeUseBitNum_ = usedChunkInfo_.length();  
			freeBlockSearchCursor_ = 0;
			assert(freeUseBitNum_ <= usedChunkInfo_.length());
			return false;
		}
		else {
			if (checkOnly) {
				GS_THROW_USER_ERROR(GS_ERROR_CM_FILE_NOT_FOUND,
					"Checkpoint file not found despite check only.");
			}
			if (!createMode) {
				GS_THROW_USER_ERROR(
					GS_ERROR_CM_FILE_NOT_FOUND, "Checkpoint file not found.");
			}

			file_ = UTIL_NEW util::NamedFile();
			file_->open(fileName_.c_str(),
				util::FileFlag::TYPE_CREATE | util::FileFlag::TYPE_READ_WRITE);
			file_->lock();
			blockNum_ = 0;
			freeUseBitNum_ = 0;
			freeBlockSearchCursor_ = 0;
			usedChunkInfo_.reset();
			validChunkInfo_.reset();
			return true;
		}
	}
	catch (SystemException &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file open failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file open failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Truncate a file.
*/
void CheckpointFile::truncate() {
	try {
		if (file_) {
			delete file_;
		}
		file_ = UTIL_NEW util::NamedFile();
		file_->open(fileName_.c_str(), util::FileFlag::TYPE_CREATE |
										   util::FileFlag::TYPE_TRUNCATE |
										   util::FileFlag::TYPE_READ_WRITE);
		UTIL_TRACE_WARNING(CHECKPOINT_FILE, "file truncated.");
		file_->lock();
		blockNum_ = 0;
		freeUseBitNum_ = 0;
		freeBlockSearchCursor_ = 0;
		usedChunkInfo_.reset();
		validChunkInfo_.reset();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file truncate failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Allocate a chunkSize-block on the checkpoint file.
*/
int64_t CheckpointFile::allocateBlock() {
	int64_t allocatePos = -1;

	int32_t count = 0;
	uint64_t pos = freeBlockSearchCursor_;
	uint64_t usedChunkInfoSize = usedChunkInfo_.length();

	if (freeUseBitNum_ > 0) {
		uint64_t startPos = freeBlockSearchCursor_;
		for (pos = freeBlockSearchCursor_; pos < usedChunkInfoSize;
			 ++pos, ++count) {
			if (!usedChunkInfo_.get(pos)) {
				allocatePos = pos;
				break;
			}
			if (count > ALLOCATE_BLOCK_SEARCH_LIMIT) {
				break;
			}
		}
		if (allocatePos == -1 && count <= ALLOCATE_BLOCK_SEARCH_LIMIT) {
			for (pos = 0; pos < startPos; ++pos, ++count) {
				if (!usedChunkInfo_.get(pos)) {
					allocatePos = pos;
					break;
				}
				if (count > ALLOCATE_BLOCK_SEARCH_LIMIT) {
					break;
				}
			}
		}
		freeBlockSearchCursor_ = pos + 1;
		if (freeBlockSearchCursor_ >= usedChunkInfoSize) {
			freeBlockSearchCursor_ = 0;
		}
	}
	if (allocatePos == -1) {
		allocatePos = usedChunkInfo_.append(true);
		assert(allocatePos != -1);
		validChunkInfo_.set(allocatePos, false);
		UTIL_TRACE_INFO(CHECKPOINT_FILE, "allocateBlock(NEW): " << allocatePos);
	}
	else {
		UTIL_TRACE_INFO(
			CHECKPOINT_FILE, "allocateBlock(reuse): " << allocatePos);
	}
	setUsedBlockInfo(allocatePos, true);
	return allocatePos;
}

/*!
	@brief Free an allocated chunkSize-block on the checkpoint file.
*/
void CheckpointFile::freeBlock(uint64_t blockNo) {
	UTIL_TRACE_INFO(CHECKPOINT_FILE, "freeBlock: " << blockNo);
	assert(usedChunkInfo_.length() >= (blockNo));
	assert(usedChunkInfo_.get(blockNo));

	setUsedBlockInfo(blockNo, false);
	UTIL_TRACE_INFO(CHECKPOINT_FILE, "freeBlock: " << blockNo);
}

/*!
	@brief Set a flag of the used block.
*/
void CheckpointFile::setUsedBlockInfo(uint64_t blockNo, bool flag) {
	UTIL_TRACE_INFO(
		CHECKPOINT_FILE, "setUsedBlock: " << blockNo << ",val," << flag);

	bool oldFlag = usedChunkInfo_.get(blockNo);
	usedChunkInfo_.set(blockNo, flag);
	if (flag && (oldFlag != flag)) {
		--freeUseBitNum_;
		assert(freeUseBitNum_ >= 0);
	}
	else if (!flag && (oldFlag != flag)) {
		++freeUseBitNum_;
	}
	assert(freeUseBitNum_ <= usedChunkInfo_.length());
}

/*!
	@brief Return a flag of the used block.
*/
bool CheckpointFile::getUsedBlockInfo(uint64_t blockNo) {
	assert(usedChunkInfo_.length() >= blockNo);
	return usedChunkInfo_.get(blockNo);
}

/*!
	@brief Initialize a flag of the used block.
*/
void CheckpointFile::initializeUsedBlockInfo(
	const uint8_t *bitList, const uint64_t blockNum) {
	usedChunkInfo_.putAll(bitList, blockNum);
	freeUseBitNum_ = 0;
	for (uint64_t i = 0; i < blockNum; ++i) {
		if (!usedChunkInfo_.get(i)) {
			++freeUseBitNum_;
		}
	}
	assert(freeUseBitNum_ <= usedChunkInfo_.length());
	UTIL_TRACE_INFO(CHECKPOINT_FILE,
		"initializeUsedBlockInfo: freeBlockNum=" << freeUseBitNum_);
}

/*!
	@brief Set a flag of the recent checkpoint block.
*/
void CheckpointFile::setValidBlockInfo(uint64_t blockNo, bool flag) {
	validChunkInfo_.set(blockNo, flag);
}

/*!
	@brief Return a flag of the recent checkpoint block.
*/
bool CheckpointFile::getValidBlockInfo(uint64_t blockNo) {
	return validChunkInfo_.get(blockNo);
}

/*!
	@brief Return set of recent checkpoint block flags.
*/
uint64_t CheckpointFile::saveValidBlockInfo(util::XArray<uint8_t> &buf) {
	validChunkInfo_.copyAll(buf);
	return validChunkInfo_.length();
}

void CheckpointFile::initializeValidBlockInfo(
	const uint8_t *bitList, const uint64_t blockNum) {
	validChunkInfo_.putAll(bitList, blockNum);
}


/*!
	@brief Write chunkSize-block.
*/
void CheckpointFile::punchHoleBlock(uint32_t size, uint64_t offset) {

#ifdef _WIN32
#else
	const uint64_t startClock =
		util::Stopwatch::currentClock();  
	try {
		if (0 < size) {
			file_->preAllocate(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
				offset, size);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file fallocate failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << fileName_
									   << ",pgId," << pgId_ 
									   << ",offset," << offset
									   << ",size," << size << ")");
	}

	const uint32_t lap = util::Stopwatch::clockToMillis(
		util::Stopwatch::currentClock() - startClock);
	if (lap > ioWarningThresholdMillis_) {  
		UTIL_TRACE_WARNING(
			IO_MONITOR, "[LONG I/O] punching hole time,"
							<< lap << ",fileName," << fileName_ << ",pgId,"
							<< pgId_ << ",offset," << offset
							<< ",size," << size
							<< ",writeBlockCount_=" << writeBlockCount_);
	}
#endif
}

void CheckpointFile::zerofillUnusedBlock(const uint64_t blockNum) {
#ifdef _WIN32
#else


	uint64_t headBlockId = 0;
	size_t count = 0;
	off_t offset = 0;
	off_t length = 0;
	size_t punchCount = 0;
	size_t totalCount = 0;

	const uint64_t startClock = util::Stopwatch::currentClock();
	try {
		for (uint64_t i = 1; i < blockNum; ++i) {
			if (usedChunkInfo_.get(i)) {
				if (count > 0) {
					offset = headBlockId * BLOCK_SIZE_;
					length = count * BLOCK_SIZE_;
					file_->preAllocate(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
							offset, length);
					totalCount += count;
					++punchCount;
					count = 0;
					headBlockId = 0;
				}
				else {
				}
			}
			else {
				if (count > 0) {
					++count;
				}
				else {
					headBlockId = i;
					count = 1;
				}
			}
		}
		if (count > 0) {
			offset = headBlockId * BLOCK_SIZE_;
			length = count * BLOCK_SIZE_;
			file_->preAllocate(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
					offset, length);
			totalCount += count;
			++punchCount;
			count = 0;
			headBlockId = 0;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
				"Punching holes in checkpoint file has failed. (reason="
				<< GS_EXCEPTION_MESSAGE(e) << fileName_
				<< ",pgId," << pgId_
				<< ",offset," << offset
				<< ",size," << length << ")");
	}

	const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);
	UTIL_TRACE_INFO(
			IO_MONITOR, "Punching hole time," << lap
			<< ",fileName," << fileName_ << ",pgId,"
			<< pgId_ << ",holePunchCount," << punchCount
			<< ",holeBlockCount," << totalCount
			<< ",totalBlockCount," << blockNum);
#endif
}

/*!
	@brief Write chunkSize-block.
*/
int64_t CheckpointFile::writeBlock(
	const uint8_t *buffer, uint32_t size, uint64_t blockNo) {
	try {
		if (!file_) {
			file_ = UTIL_NEW util::NamedFile();
			file_->open(fileName_.c_str(),
				util::FileFlag::TYPE_CREATE | util::FileFlag::TYPE_READ_WRITE);
			file_->lock();
		}
		const uint64_t startClock =
			util::Stopwatch::currentClock();  
		ssize_t writtenSize = file_->write(buffer, (size << BLOCK_EXP_SIZE_), 
			(blockNo << BLOCK_EXP_SIZE_));
		if (writtenSize != (size << BLOCK_EXP_SIZE_)) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CF_WRITE_CHUNK_FAILED, 
				"invalid size, expected = " << (size << BLOCK_EXP_SIZE_) 
				<< ", actural = " << writtenSize);
		}

		const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);
		if (lap > ioWarningThresholdMillis_) {  
			UTIL_TRACE_WARNING(
				IO_MONITOR, "[LONG I/O] write time,"
								<< lap << ",fileName," << fileName_ << ",pgId,"
								<< pgId_ << ",chunkNth," << blockNo
								<< ",writeBlockCount_=" << writeBlockCount_);
		}
		if (blockNum_ < (size + blockNo)) {
			blockNum_ = (size + blockNo);
			UTIL_TRACE_INFO(
				CHECKPOINT_FILE, fileName_ + " extended. File size = "
									 << (blockNo + size) 
									 << ",blockNum_=" << blockNum_);
		}

		uint64_t writeBlockNum = (writtenSize >> BLOCK_EXP_SIZE_);
		writeBlockCount_ += writeBlockNum;
		UTIL_TRACE_INFO(CHECKPOINT_FILE,
			fileName_ << ",blockNo," << blockNo
					  << ",writeBlockCount=" << writeBlockCount_);
		return size;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file write failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << fileName_
									   << ",pgId," << pgId_ << ",blockNo,"
									   << blockNo << ")");
	}
}

/*!
	@brief Write byteSize-data.
*/
int64_t CheckpointFile::writePartialBlock(
	const uint8_t *buffer, uint32_t size, uint64_t offset) {
	assert(offset >= 0);
	try {
		if (!file_) {
			file_ = UTIL_NEW util::NamedFile();
			file_->open(fileName_.c_str(),
				util::FileFlag::TYPE_CREATE | util::FileFlag::TYPE_READ_WRITE);
			file_->lock();
		}

		const uint64_t startClock =
			util::Stopwatch::currentClock();  
		ssize_t writtenSize = file_->write(buffer, size, offset);

		if (writtenSize != size) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CF_WRITE_CHUNK_FAILED, 
				"invalid size, expected = " << size 
				<< ", actural = " << writtenSize);
		}

		const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);  
		if (lap > ioWarningThresholdMillis_) {  
			UTIL_TRACE_WARNING(IO_MONITOR, "[LONG I/O] write time,"
											   << lap << ",fileName,"
											   << fileName_ << ",pgId," << pgId_
											   << ",offset," << offset);
		}
		if ((blockNum_ << BLOCK_EXP_SIZE_) < size + offset) {
			blockNum_ = ((size + offset + BLOCK_SIZE_ - 1) >> BLOCK_EXP_SIZE_);
			UTIL_TRACE_INFO(
				CHECKPOINT_FILE, fileName_ + " extended. File size = "
									 << (size + offset) << "(Byte)"
									 << ",blockNum_=" << blockNum_);
		}

		UTIL_TRACE_INFO(CHECKPOINT_FILE,
			fileName_ << ",write, offset," << offset << ",size=" << size);

		return writtenSize;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file write failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}

}

/*!
	@brief Read chunkSize-block.
*/
int64_t CheckpointFile::readBlock(
	uint8_t *buffer, uint32_t size, uint64_t blockNo) {

	if (blockNum_ < size + blockNo - 1) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CF_READ_CHUNK_FAILED, 
			"Checkpoint file read failed. (reason= invalid parameter."
			<< " size = " << size << ", blockNo = " << blockNo
			<< ", blockNum = " << blockNum_ << ")");
	}

	try {
		if (!file_) {
			if (util::FileSystem::exists(fileName_.c_str())) {
				file_ = UTIL_NEW util::NamedFile();
				file_->open(fileName_.c_str(), util::FileFlag::TYPE_READ_WRITE);
				file_->lock();
			}
			else {
				return 0;
			}
		}
		const uint64_t startClock =
			util::Stopwatch::currentClock();  
		ssize_t readSize =
			file_->read(buffer, (size << BLOCK_EXP_SIZE_), (blockNo << BLOCK_EXP_SIZE_));

		if (readSize != (size << BLOCK_EXP_SIZE_)) {
			util::FileStatus status;
			file_->getStatus(&status);
			if ((readSize < (size << BLOCK_EXP_SIZE_)) 
				&& (static_cast<uint64_t>(status.getSize()) < ((blockNo + size) << BLOCK_EXP_SIZE_))) {
				uint32_t tailBlankSize = (size << BLOCK_EXP_SIZE_) - readSize;
				memset(buffer + readSize, 0, tailBlankSize);
				readSize += tailBlankSize;
			}
			else {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CF_READ_CHUNK_FAILED, 
					"invalid size, expected = " << (size << BLOCK_EXP_SIZE_) 
					<< ", actural = " << readSize);
			}
		}

		const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);  
		if (lap > ioWarningThresholdMillis_) {  
			UTIL_TRACE_WARNING(IO_MONITOR,
				"[LONG I/O] read time," << lap << ",fileName," << fileName_
										<< ",blockNo," << blockNo
										<< ",blockCount," << size);
		}
		int64_t readBlockNum = (readSize >> BLOCK_EXP_SIZE_);
		readBlockCount_ += readBlockNum;
		UTIL_TRACE_INFO(CHECKPOINT_FILE,
			fileName_ << ",blockNo," << blockNo
					  << ",readBlockCount_=" << readBlockCount_);
		return readBlockNum;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file read failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


/*!
	@brief Return file size.
*/
int64_t CheckpointFile::getFileSize() {
	int64_t writeOffset;
	try {
		util::FileStatus fileStatus;
		file_->getStatus(&fileStatus);
		writeOffset = fileStatus.getSize();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file read failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return writeOffset;
}

/*!
	@brief Close the file.
*/
int64_t CheckpointFile::getFileAllocateSize() {
	if (0 < blockNum_) {
#ifdef _WIN32
		return getFileSize();
#else
		int64_t blockSize;
		try {
			util::FileStatus fileStatus;
			file_->getStatus(&fileStatus);
			blockSize = fileStatus.getBlockCount() * 512;
		}
		catch (std::exception &e) {
			GS_RETHROW_SYSTEM_ERROR(e, "Checkpoint file read failed. (reason="
										   << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		return blockSize;
#endif
	}
	return 0;
}

size_t CheckpointFile::getFileSystemBlockSize(const std::string &dir) {
	size_t fileSystemBlockSize;
	try {
		util::FileSystemStatus status;
		util::FileSystem::getStatus(dir.c_str(), &status);
		fileSystemBlockSize = status.getBlockSize();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Directory access failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return fileSystemBlockSize;
}

size_t CheckpointFile::getFileSystemBlockSize() {
	return getFileSystemBlockSize(dir_);
}

void CheckpointFile::close() {
	if (file_) {
		file_->close();
		delete file_;
	}
	file_ = NULL;
}

/*!
	@brief Flush the file.
*/
void CheckpointFile::flush() {
	if (file_) {
		const uint64_t startClock =
			util::Stopwatch::currentClock();  
		file_->sync();
		const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);  
		if (lap > ioWarningThresholdMillis_) {				
			UTIL_TRACE_WARNING(IO_MONITOR,
				"[LONG I/O] sync time," << lap << ",fileName," << fileName_);
		}
	}
}

uint64_t CheckpointFile::getReadBlockCount() {
	return readBlockCount_;
}
uint64_t CheckpointFile::getWriteBlockCount() {
	return writeBlockCount_;
}
void CheckpointFile::resetReadBlockCount() {
	readBlockCount_ = 0;
}
void CheckpointFile::resetWriteBlockCount() {
	writeBlockCount_ = 0;
}

/*!
	@brief Return if fileName is of a checkpointFile.
*/
bool CheckpointFile::checkFileName(
	const std::string &fileName, PartitionGroupId &pgId) {
	pgId = UNDEF_PARTITIONGROUPID;

	std::string::size_type pos = fileName.find(gsCpFileBaseName);
	if (0 != pos) {
		return false;
	}
	pos = fileName.rfind(gsCpFileExtension);
	if (std::string::npos == pos) {
		return false;
	}
	else if ((pos + strlen(gsCpFileExtension)) != fileName.length()) {
		return false;
	}

	util::NormalIStringStream iss(fileName);
	iss.seekg(strlen(gsCpFileBaseName));
	uint32_t num32;
	uint64_t num64;
	char c;
	iss >> num32 >> c >> num64;

	if (iss.fail()) {
		return false;
	}
	if (c != '_') {
		return false;
	}
	if (num64 != 1) {
		return false;
	}
	if (static_cast<std::string::size_type>(iss.tellg()) != pos) {
		return false;
	}
	pgId = num32;  
	return true;
}

std::string CheckpointFile::dump() {
	return fileName_;
}

std::string CheckpointFile::dumpUsedChunkInfo() {
	return usedChunkInfo_.dumpUnit();
}

std::string CheckpointFile::dumpValidChunkInfo() {
	return validChunkInfo_.dumpUnit();
}
