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
	@brief Definition of Partition
*/
#ifndef PARTITION_FILE_H_
#define PARTITION_FILE_H_

#include "data_type.h"
#include "utility_v5.h"
#include "config_table.h"
#include "zlib.h"
#include <iostream>

class FileStatTable {
public:
	enum Param {
		FILE_STAT_SIZE,
		FILE_STAT_ALLOCATED_SIZE,
		FILE_STAT_END
	};

	typedef LocalStatTable<Param, FILE_STAT_END> BaseTable;
	typedef BaseTable::Mapper BaseMapper;

	explicit FileStatTable(const util::StdAllocator<void, void> &alloc);

	void addFile(util::File *file);
	void removeFile(util::File *file);

	void get(BaseTable &table);

private:
	typedef util::AllocSet<util::File*> FileSet;

	enum {
		FILE_SYSTEM_STAT_BLOCK_SIZE = 512
	};

	static uint64_t getAllocatedSize(const util::FileStatus &status);

	util::Mutex mutex_;
	FileSet fileSet_;
};

class Chunk;
/*!
	@brief チャンク圧縮基底クラス
*/
class ChunkCompressorBase {
public:
	virtual size_t compress(const void* in, void*& out) = 0;
	virtual bool uncompress(void* area, size_t compressedSize) = 0;
};


/*!
	@brief Zipによるチャンク圧縮
	@note Chunk層を対象
*/
class ChunkCompressorZip final : public ChunkCompressorBase {
private:
	static const int32_t COMPRESS_LEVEL = Z_BEST_SPEED;

	uint8_t* compressBuffer_;
	uint8_t* uncompressBuffer_;
	size_t chunkSize_;
	size_t skipSize_;
	size_t compressedSizeLimit_;
	size_t bufferSize_;
	uint64_t compressionErrorCount_;
	bool isFirstTime_;

public:
	ChunkCompressorZip(size_t chunkSize, size_t skipSize, size_t compressedSizeLimit);
	~ChunkCompressorZip();

	size_t compress(const void* in, void*& out);

	bool uncompress(void* area, size_t compressedSize);
};


/*!
	@brief 仮想ファイル基底クラス
*/
class VirtualFileBase {
public:
	virtual VirtualFileBase& open() = 0;
	virtual size_t length() = 0;
	virtual void seekAndRead(
			int64_t offset, uint8_t* buff, size_t buffLength,
			StatStopwatch &ioWatch) = 0;
	virtual void seekAndWrite(
			int64_t offset, uint8_t* buff, size_t buffLength, int32_t length,
			StatStopwatch &ioWatch) = 0;
	virtual void punchHoleBlock(int64_t offset, StatStopwatch &ioWatch) = 0;
	virtual void flush() = 0;
	virtual void close() = 0;
	virtual void remove() = 0;
};


/*!
	@brief 仮想ファイル
*/
class VirtualFileNIO final : public VirtualFileBase {
private:
	uint32_t timeThresholdMillis_;
	std::unique_ptr<Locker> locker_;
	std::string path_;
	int32_t stripeSize_;
	int32_t numfiles_;
	int32_t chunkSize_;
	PartitionId pId_;
	FileStatTable *stats_;

	std::vector< std::unique_ptr<util::NamedFile> > files_;
	std::vector<util::File*> monitoringFiles_;

public:
	VirtualFileNIO(
			std::unique_ptr<Locker>&& locker, const char* path, PartitionId pId,
			int32_t stripeSize, int32_t numfiles, int32_t chunkSize, 
			FileStatTable *stats);

	virtual ~VirtualFileNIO();

	VirtualFileNIO& open();

	size_t length();

	void seekAndRead(
			int64_t offset, uint8_t* buff, size_t buffLength,
			StatStopwatch &ioWatch);
	void seekAndWrite(
			int64_t offset, uint8_t* buff, size_t buffLength, int32_t length,
			StatStopwatch &ioWatch);

	void punchHoleBlock(int64_t offset, StatStopwatch &ioWatch);

	void flush();

	void close();

	void remove();

private:
	void seek(int32_t index, int64_t offset);

	void read(
			int32_t index, void* buff, size_t buffLength, int64_t offset,
			StatStopwatch &ioWatch);

	void write(
			int32_t index, void* buff, size_t buffLength, int32_t length,
			int64_t offset, StatStopwatch &ioWatch);

	void punchHole(
			int32_t index, int64_t offset, int64_t size,
			StatStopwatch &ioWatch);

	void flush(int32_t index);

	static void holeTest();

	void startMonitoring();
	void clearMonitoring();
};


/*!
	@brief 単純なファイル
	@note 書き込みは追記のみ
*/
class SimpleFile {
 private:
	uint32_t timeThresholdMillis_;
	std::string path_;
	util::NamedFile* file_;
	util::Atomic<uint64_t> tailOffset_;
	util::Atomic<uint64_t> flushedOffset_;
	int64_t writeOffset_;

 public:
	SimpleFile(const std::string& path);
	~SimpleFile();

	std::string getFileName();

	SimpleFile& open();

	void seek(int64_t offset);

	void setSize(int64_t offset);

	int64_t readLong(int64_t& offset);

	void writeLong(int64_t len);

	int32_t read(int64_t len, void* buff, int64_t& offset);

	void write(int64_t len, const void* buff);

	void flush();

	void copyAll(const char *dest, uint8_t* buffer, size_t bufferSize);

	void close();

	void clearFileCache();

	uint64_t getOffset();
	uint64_t getSize();
};

/*!
	@brief SimpleFileに読み込みバッファ機能を付与したもの
	@note 読み込みのみ
*/
class BufferedReader {
 private:
	SimpleFile* file_;
	int32_t bufferSize_;
	int32_t bufferUsed_;
	int64_t bufferedOffset_;
	int64_t readOffset_;
	int32_t bufferPos_;
	int64_t fileSize_;
	util::NormalXArray<uint8_t> buffer_;

 public:
	BufferedReader(const std::string& path, uint32_t readBufferSize);
	~BufferedReader();

	std::string getFileName();

	void open();

	void seek(int64_t offset);

	int64_t readLong(int64_t& offset);

	int32_t read(int64_t len, void* buff, int64_t& offset, int64_t& readSize);

	void close();

	uint64_t getOffset();
	uint64_t getSize();

private:
	int32_t updateBuffer(int64_t& offset, int64_t& readSize);
};

#endif 
