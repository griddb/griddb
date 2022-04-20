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
#include "partition_file.h"
#include "chunk_manager.h"
#include "chunk.h"
#ifndef _WIN32
#include <fcntl.h>
#include <linux/falloc.h>
#endif

UTIL_TRACER_DECLARE(CHUNK_MANAGER);  



FileStatTable::FileStatTable(const util::StdAllocator<void, void> &alloc) :
		fileSet_(alloc) {
}

void FileStatTable::addFile(util::File *file) {
	assert(file != NULL && !file->isClosed());

	util::LockGuard<util::Mutex> guard(mutex_);
	assert(fileSet_.find(file) == fileSet_.end());
	fileSet_.insert(file);
}

void FileStatTable::removeFile(util::File *file) {
	assert(file != NULL && !file->isClosed());

	util::LockGuard<util::Mutex> guard(mutex_);
	assert(fileSet_.find(file) != fileSet_.end());
	fileSet_.erase(file);
}

void FileStatTable::get(BaseTable &table) {
	util::FileStatus status;

	util::LockGuard<util::Mutex> guard(mutex_);
	for (FileSet::const_iterator it = fileSet_.begin();
			it != fileSet_.end(); ++it) {
		util::File *file = *it;
		assert(!file->isClosed());
		try {
			file->getStatus(&status);
		}
		catch (util::PlatformException&) {
		}
		table(FILE_STAT_SIZE).add(status.getSize());
		table(FILE_STAT_ALLOCATED_SIZE).add(getAllocatedSize(status));
	}
}

uint64_t FileStatTable::getAllocatedSize(const util::FileStatus &status) {
#ifdef _WIN32
	return status.getSize();
#else
	return status.getBlockCount() * FILE_SYSTEM_STAT_BLOCK_SIZE;
#endif
}

/*!
	@brief コンストラクタ
*/
ChunkCompressorZip::ChunkCompressorZip(
		size_t chunkSize, size_t skipSize, size_t compressedSizeLimit)
: compressBuffer_(NULL), uncompressBuffer_(NULL),
  chunkSize_(chunkSize), skipSize_(skipSize),
  compressedSizeLimit_(compressedSizeLimit),
  bufferSize_(compressBound(chunkSize)),
  compressionErrorCount_(0), isFirstTime_(true)
{
	static const char* currentVersion = ZLIB_VERSION;
	if (currentVersion[0] != zlibVersion()[0]) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INCOMPATIBLE_ZLIB_VERSION,
			"the zlib library version (zlib_version) is incompatible"
				<< " with the version assumed, " << currentVersion
				<< ", but the library version, " << zlibVersion());
	}
	compressBuffer_ = UTIL_NEW uint8_t[bufferSize_];
	uncompressBuffer_ = UTIL_NEW uint8_t[bufferSize_];
}

ChunkCompressorZip::~ChunkCompressorZip() {
	delete [] compressBuffer_;
	compressBuffer_ = NULL;
	delete [] uncompressBuffer_;
	uncompressBuffer_ = NULL;
}

/*!
	@brief チャンクを圧縮
*/
size_t ChunkCompressorZip::compress(const void* area, void*& out) {
	assert(area);
	out = NULL;
	const uint8_t* top = static_cast<const uint8_t*>(area);
	size_t bodySize = chunkSize_ - skipSize_;
	uLongf compressedSize = bufferSize_;
	int32_t ret = compress2(
		reinterpret_cast<Bytef*>(compressBuffer_ + skipSize_), &compressedSize,
		reinterpret_cast<const Bytef*>(top + skipSize_), bodySize,
		COMPRESS_LEVEL);

	if (ret != Z_OK) {
		if (isFirstTime_) {
			GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_COMPRESSION_FAILED,
						   "error occured in compress.");
			isFirstTime_ = false;
		}
		compressionErrorCount_++;
		return 0;
	} else {
		if (skipSize_ + compressedSize <= compressedSizeLimit_) {
			assert(skipSize_ + compressedSize <= chunkSize_);
			memcpy(compressBuffer_, top , skipSize_);
			out = compressBuffer_;
			return compressedSize;
		} else {
			return 0;
		}
	}
}

/*!
	@brief 圧縮チャンクを展開
*/
bool ChunkCompressorZip::uncompress(void* chunkTop, size_t compressedSize) {
	assert(chunkTop);
	assert(compressedSize <= chunkSize_);
	uint8_t* top = static_cast<uint8_t*>(chunkTop);
	if (compressedSize < (chunkSize_ - skipSize_)) {
		uLongf destLen = bufferSize_;
		int32_t ret = ::uncompress(
			reinterpret_cast<Bytef*>(uncompressBuffer_), &destLen,
			reinterpret_cast<Bytef*>(top + skipSize_), compressedSize);
		if (ret != Z_OK) {
			GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_COMPRESSION_FAILED,
						   "error occured in uncompress.");
		}
		assert(destLen <= (chunkSize_ - skipSize_));
		memcpy(top + skipSize_, uncompressBuffer_, destLen);
		return true;
	}
	else {
		return false;
	}
}

/*!
	@brief コンストラクタ
*/
VirtualFileNIO::VirtualFileNIO(
		std::unique_ptr<Locker>&& locker, const char* path, PartitionId pId,
		int32_t stripeSize, int32_t numfiles, int32_t chunkSize,
		FileStatTable *stats) :
		locker_(std::move(locker)),
		path_(path),
		pId_(pId),
		stripeSize_(stripeSize),
		numfiles_(numfiles),
		chunkSize_(chunkSize), 
		stats_(stats) {
}

VirtualFileNIO::~VirtualFileNIO() {
	clearMonitoring();
}

/*!
	@brief オープン処理
*/
VirtualFileNIO& VirtualFileNIO::open() {
	if (!util::FileSystem::isDirectory(path_.c_str())) {
		util::FileSystem::createDirectoryTree(path_.c_str());
		assert(util::FileSystem::isDirectory(path_.c_str()));
	}
	clearMonitoring();
	files_.clear();
	for (int32_t index = 0; index < numfiles_; index++) {
		util::NormalOStringStream oss;
		oss << path_ << "/" << pId_ << "_part_" << index << ".dat";
		const std::string fileName = oss.str();

		std::unique_ptr<util::NamedFile> file(UTIL_NEW util::NamedFile());
		int32_t fileFlag = util::FileFlag::TYPE_READ_WRITE;
		if (!util::FileSystem::exists(fileName.c_str())) {
			fileFlag |= util::FileFlag::TYPE_CREATE;
		}
		file->open(fileName.c_str(), fileFlag);

		if (file->isClosed()) {
			std::cout << "failed to open " << fileName.c_str() << std::endl;
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
				"failed to open " << fileName.c_str());
		}
		files_.push_back(std::move(file));
	}
	startMonitoring();
	return *this;
}

/*!
	@brief トータルのファイル長取得
*/
size_t VirtualFileNIO::length() {
	size_t len = 0;
	util::FileStatus status;
	for (int32_t index = 0; index < numfiles_; index++) {
		files_[index]->getStatus(&status);
		len += status.getSize();
	}
	return len;
}

void VirtualFileNIO::read(
		int32_t index, void* buff, size_t buffLength, int64_t offset,
		StatStopwatch &ioWatch) {
	int32_t retryCount = 0;
	uint64_t filePos = static_cast<uint64_t>(offset * chunkSize_);
	ssize_t readRemain = buffLength;
	uint8_t* readAddr = static_cast<uint8_t*>(buff);
	ioWatch.start();
	while (readRemain > 0) {
		ssize_t readSize = files_[index]->read(readAddr, readRemain, filePos);
		if (readSize == readRemain) {
			break;
		}
		if (readSize == 0 && retryCount > 0) {
			util::FileStatus status;
			files_[index]->getStatus(&status);
			if (status.getSize() <= filePos) {
				memset(readAddr, 0, readRemain);
				readRemain = 0;
				break;
			}
		}
		assert(readSize < readRemain);
		readRemain -= readSize;
		readAddr += readSize;
		filePos += static_cast<uint64_t>(readSize);
		++retryCount;
	}
	const uint32_t lap = ioWatch.stop();

}

void VirtualFileNIO::write(
		int32_t index, void* buff, size_t buffLength, int32_t length,
		int64_t offset, StatStopwatch &ioWatch) {
	assert(length <= buffLength);
	int32_t retryCount = 0;
	uint64_t filePos = static_cast<uint64_t>(offset * chunkSize_);
	ssize_t writeRemain = length;
	const uint8_t* writeAddr = static_cast<const uint8_t*>(buff);
	ioWatch.start();
	while (writeRemain > 0) {
		ssize_t writtenSize =
			files_[index]->write(writeAddr, writeRemain, filePos);
		if (writtenSize == writeRemain) {
			break;
		}
		assert(writtenSize < writeRemain);
		writeRemain -= writtenSize;
		writeAddr += writtenSize;
		filePos += static_cast<uint64_t>(writtenSize);
		++retryCount;
	}
	const uint32_t lap = ioWatch.stop();

}

void VirtualFileNIO::punchHole(
		int32_t index, int64_t offset, int64_t size, StatStopwatch &ioWatch) {
	ioWatch.start();
#ifndef _WIN32
	files_[index]->preAllocate(
			FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, offset, size);
#endif
	ioWatch.stop();
}

void VirtualFileNIO::flush(int32_t index) {
	files_[index]->sync();
}

/*!
	@brief ファイルリード
*/
void VirtualFileNIO::seekAndRead(
		int64_t offset, uint8_t* buff, size_t buffLength,
		StatStopwatch &ioWatch) {
	int32_t index = (int32_t)((int64_t)(offset / stripeSize_) % numfiles_);
	int64_t suboffset = (int64_t)(offset / stripeSize_ / numfiles_) * stripeSize_ + (offset % stripeSize_);
	{
		read(index, buff, buffLength, suboffset, ioWatch);
	}
}

/*!
	@brief ファイルライト
*/
void VirtualFileNIO::seekAndWrite(
		int64_t offset, uint8_t* buff, size_t buffLength, int32_t length,
		StatStopwatch &ioWatch) {
	int32_t index = static_cast<int32_t>(
		static_cast<int64_t>(offset / stripeSize_) % numfiles_);
	int64_t suboffset = (int64_t)(offset / stripeSize_ / numfiles_) * stripeSize_ + (offset % stripeSize_);
	{
		assert(length > 0 && length <= chunkSize_);
#ifdef _WIN32
		write(index, buff, buffLength, chunkSize_, suboffset, ioWatch);
#else
		write(index, buff, buffLength, length, suboffset, ioWatch);
		if (length < chunkSize_) {
			punchHole(
					index,
					(suboffset * static_cast<int64_t>(chunkSize_)) + length,
					chunkSize_ - length,
					ioWatch);
		}
#endif
	}
}

/*!
	@brief ブロックまるごと領域解放(穴開け)
*/
void VirtualFileNIO::punchHoleBlock(
		int64_t offset, StatStopwatch &ioWatch) {
	int32_t index = static_cast<int32_t>(
			static_cast<int64_t>(offset / stripeSize_) % numfiles_);
	int64_t suboffset = static_cast<int64_t>(offset / stripeSize_ / numfiles_) * stripeSize_
			+ (offset % stripeSize_);
	{
#ifdef _WIN32
#else
		punchHole(
				index, suboffset * static_cast<int64_t>(chunkSize_),
				chunkSize_, ioWatch);
#endif
	}
}


/*!
	@brief 全ファイルフラッシュ
*/
void VirtualFileNIO::flush() {
	for (int32_t index = 0; index < numfiles_; index++) {
		flush(index);
	}
}

/*!
	@brief 全ファイルクローズ
*/
void VirtualFileNIO::close() {
	clearMonitoring();
	for (int32_t index = 0; index < numfiles_; index++) {
		files_[index]->close();
	}
}

/*!
	@brief 全ファイル削除
*/
void VirtualFileNIO::remove() {
	util::FileSystem::remove(path_.c_str(), true); 
}

void VirtualFileNIO::startMonitoring() {
	assert(monitoringFiles_.empty());
	clearMonitoring();

	if (stats_ == NULL) {
		return;
	}

	for (int32_t index = 0; index < numfiles_; index++) {
		if (index >= static_cast<int32_t>(files_.size())) {
			break;
		}

		util::File *file = files_[index].get();
		if (file != NULL && !file->isClosed()) {
			monitoringFiles_.resize(monitoringFiles_.size() + 1);
			stats_->addFile(file);
			monitoringFiles_.back() = file;
		}
	}
}

void VirtualFileNIO::clearMonitoring() {
	if (stats_ == NULL) {
		return;
	}

	while (!monitoringFiles_.empty()) {
		util::File *file = monitoringFiles_.back();
		monitoringFiles_.back() = NULL;

		if (file != NULL) {
			stats_->removeFile(file);
		}

		monitoringFiles_.pop_back();
	}
}

/*!
	@brief コンストラクタ
*/
SimpleFile::SimpleFile(const std::string& path)
: path_(path), file_(NULL), tailOffset_(0), flushedOffset_(0) {
	;
}

/*!
	@brief デストラクタ
*/
SimpleFile::~SimpleFile() {
	if (file_) {
		delete file_;
		file_ = NULL;
	}
}

std::string SimpleFile::getFileName() {
	return path_;
}

/*!
	@brief ファイルオープン
*/
SimpleFile& SimpleFile::open() {
	file_ = UTIL_NEW util::NamedFile();
	if (util::FileSystem::exists(path_.c_str())) {
		file_->open(path_.c_str(), util::FileFlag::TYPE_READ_WRITE);
		util::FileStatus status;
		file_->getStatus(&status);
		tailOffset_ = status.getSize();
		flushedOffset_ = tailOffset_;
	}
	else {
		file_->open(path_.c_str(), util::FileFlag::TYPE_CREATE | util::FileFlag::TYPE_READ_WRITE);
		tailOffset_ = 0;
		flushedOffset_ = tailOffset_;
	}
	return *this;
}

void SimpleFile::seek(int64_t offset) {
	file_->seek(offset);
}

void SimpleFile::copyAll(const char *dest, uint8_t* buffer, size_t bufferSize) {
	SimpleFile destFile(dest);
	destFile.open();

	uint64_t size = getSize();
	uint64_t readOffset = 0;
	uint64_t writeOffset = 0;
	uint64_t remain = size;
	while(remain > 0) {
		uint64_t requestSize = (remain > bufferSize) ? bufferSize : remain;
		ssize_t readSize = file_->read(buffer, requestSize, readOffset);
		readOffset += readSize;
		remain -= readSize;
		destFile.write(readSize, buffer);
	}
	destFile.flush();
	destFile.clearFileCache();
	destFile.close();
}

void SimpleFile::setSize(int64_t offset) {
	file_->setSize(offset);
}

/*!
	@brief int64値読み込み
*/
int64_t SimpleFile::readLong(int64_t& offset) {
	int64_t n = -1;
	try {
 		int32_t retryCount = 0;
		ssize_t readRemain = sizeof(n);
		const uint64_t startClock = util::Stopwatch::currentClock();
		while (readRemain > 0) {
			ssize_t readSize = file_->read(&n, sizeof(n), offset);
			if (readSize == sizeof(n)) {
				offset += readSize;
				break;
			}
			if (retryCount > 0) {
				util::FileStatus status;
				file_->getStatus(&status);
				if (status.getSize() <= (offset + sizeof(n))) {
					n = -1;
					readRemain = 0;
					break;
				}
			}
			if (retryCount > 5) {
				n = -1;
				readRemain = 0;
				break;
			}
			assert(readSize < readRemain);
			++retryCount;
		}
		const uint32_t lap = util::Stopwatch::clockToMillis(
				util::Stopwatch::currentClock() - startClock);
	}
	catch(util::PlatformException &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
				"Read error occred (fileName=" << path_.c_str() <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return n;
}

/*!
	@brief int32値書き込み
*/
void SimpleFile::writeLong(int64_t len) {
	ssize_t writeBytes = file_->write(&len, sizeof(len), tailOffset_);
	assert(writeBytes == sizeof(len));

	tailOffset_ += sizeof(len);
}

/*!
	@brief バイナリ読み込み
*/
int32_t SimpleFile::read(int64_t len, void* buff, int64_t& filePos) {
	try {
		int32_t retryCount = 0;
		ssize_t readRemain = static_cast<ssize_t>(len);
		uint8_t* readAddr = static_cast<uint8_t*>(buff);
		const uint64_t startClock = util::Stopwatch::currentClock();
		while (readRemain > 0) {
			ssize_t readSize = file_->read(readAddr, readRemain, filePos);
			if (readSize == readRemain) {
				filePos += static_cast<int64_t>(readSize);
				break;
			}
			if (readSize == 0 && retryCount > 0) {
				util::FileStatus status;
				file_->getStatus(&status);
				if (status.getSize() <= filePos) {
					memset(readAddr, 0, readRemain);
					readRemain = 0;
					break;
				}
			}
			assert(readSize < readRemain);
			readRemain -= readSize;
			readAddr += readSize;
			filePos += static_cast<int64_t>(readSize);
			++retryCount;
		}
		const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);
		return static_cast<int32_t>(lap);
	}
	catch(util::PlatformException &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
				"Read error occred (fileName=" << path_.c_str() <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief バイナリ書き込み
*/
void SimpleFile::write(int64_t len, const void* buff) {
	ssize_t remain = static_cast<ssize_t>(len);
	const char* addr = static_cast<const char*>(buff);
	while(remain > 0) {
		ssize_t writeBytes = file_->write(addr, remain, tailOffset_);
		tailOffset_ += writeBytes;
		addr += writeBytes;
		remain -= writeBytes;
	}
}

/*!
	@brief ファイルフラッシュ
*/
void SimpleFile::flush() {
	uint64_t currentTail = tailOffset_;
	if (flushedOffset_ != currentTail) {
		file_->sync();
		flushedOffset_ = currentTail;
	}
}

/*!
	@brief ファイルクローズ
*/
void SimpleFile::close() {
	file_->close();
}

/*!
	@brief ファイルキャッシュクリア
*/
void SimpleFile::clearFileCache() {
#ifndef WIN32
	if (file_) {
		try {
			int32_t ret = posix_fadvise(file_->getHandle(), 0, 0, POSIX_FADV_DONTNEED);
			if (ret > 0) {
				GS_TRACE_WARNING(LOG_MANAGER, GS_TRACE_LM_FADVISE_LOG_FILE_FAILED,
						"file fadvise failed. :" <<
						" fileName," << file_->getName() <<
						",returnCode," << ret);
			}
			GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_FADVISE_LOG_FILE_INFO,
					"advise(POSIX_FADV_DONTNEED) : fileName," << file_->getName() );
		} catch(...) {
			; 
		}
	}
#endif
}

/*!
	@brief オフセット取得
*/
uint64_t SimpleFile::getOffset() {
	if (file_) {
		return file_->tell();
	}
	else {
		return 0;
	}
}

/*!
	@brief ファイルサイズ取得
*/
uint64_t SimpleFile::getSize() {
	if (file_) {
		util::FileStatus status;
		file_->getStatus(&status);
		return status.getSize();
	}
	else {
		return 0;
	}
}


BufferedReader::BufferedReader(const std::string& path, uint32_t bufferSize)
: file_(NULL), bufferSize_(bufferSize), bufferUsed_(0), bufferedOffset_(0),
  readOffset_(0), bufferPos_(0) {
	try {
		buffer_.assign(bufferSize, 0);
		file_ = UTIL_NEW SimpleFile(path);
	} catch(std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

BufferedReader::~BufferedReader() {
	if (file_) {
		file_->close();
		delete file_;
		file_ = NULL;
	}
}

std::string BufferedReader::getFileName() {
	return file_ ? file_->getFileName() : "";
}

void BufferedReader::open() {
	if (file_) {
		file_->open();
	}
	bufferUsed_ = 0;
	bufferedOffset_ = 0;
	readOffset_ = 0;
	bufferPos_ = 0;
}

int64_t BufferedReader::readLong(int64_t& offset) {
	if (!file_) {
		return -1;
	}
	int64_t val = -1;
	int64_t readSize;
	read(sizeof(val), &val, offset, readSize);

	return (readSize > 0) ? val : -1;
}

int32_t BufferedReader::updateBuffer(int64_t& offset, int64_t& readSize) {
	assert(file_);
	int32_t readTime = 0;
	readSize = 0;
	uint64_t remain = bufferSize_;
	fileSize_ = file_->getSize();
	if (offset + remain > fileSize_) {
		remain = fileSize_ - offset;
	}
	if (remain == 0) {
		readSize = 0;
		return readTime;
	}
	bufferedOffset_ = offset;
	readTime += file_->read(remain, buffer_.data(), bufferedOffset_);
	readSize = remain;
	bufferPos_ = 0;
	bufferUsed_ = remain;
	readOffset_ = offset;
	return readTime;
}

int32_t BufferedReader::read(int64_t len, void* buff, int64_t& offset, int64_t& readSize) {
	assert(file_);
	int32_t readTime = 0;
	uint8_t* dest = static_cast<uint8_t*>(buff);
	readSize = 0;
	if (offset != readOffset_ || bufferUsed_ == 0) {
		readTime += updateBuffer(offset, readSize);
	}
	uint64_t remain = len;
	if (bufferPos_ + len <= bufferUsed_) {
		memcpy(dest, buffer_.data() + bufferPos_, len);
		readOffset_ += len;
		bufferPos_ += len;
		offset += len;
		readSize = len;
		return readTime;
	}
	if (readOffset_ + len > fileSize_) {
		fileSize_ = file_->getSize(); 
		if (readOffset_ + len > fileSize_) {
			readSize = 0;
			return readTime;
		}
	}
	if (bufferUsed_ > bufferPos_) {
		uint32_t copySize = bufferUsed_ - bufferPos_;
		memcpy(dest, buffer_.data() + bufferPos_, copySize);
		remain -= copySize;
		readOffset_ += copySize;
		bufferPos_ += copySize;
		dest += copySize;
		offset += copySize;
		readSize += copySize;
	}
	if (remain < bufferSize_) {
		bufferedOffset_ = readOffset_;
		int64_t readSize2;
		readTime += updateBuffer(bufferedOffset_, readSize2);
		assert(readSize2 >= remain);
		bufferUsed_ = readSize2;
		memcpy(dest, buffer_.data(), remain);
		readOffset_ += remain;
		bufferPos_ = remain;
		offset += remain;
		readSize += remain;
		return readTime;
	}
	else {
		uint64_t prev = readOffset_;
		readTime += file_->read(remain, dest, readOffset_);
		assert((readOffset_ - prev) == remain);
		bufferPos_ = 0;
		bufferUsed_ = 0;
		offset += remain;
		readSize += remain;
		return readTime;
	}
}

void BufferedReader::close() {
	if (file_) {
		file_ ->close();
	}
}

uint64_t BufferedReader::getOffset() {
	return readOffset_;
}

uint64_t BufferedReader::getSize() {
	return (file_ ? file_->getSize() : 0);
}

