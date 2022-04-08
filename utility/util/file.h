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
/*
    Copyright (c) 2008, Yubin Lim(purewell@gmail.com).
    All rights reserved.

    Redistribution and use in source and binary forms, with or without 
    modification, are permitted provided that the following conditions 
    are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the 
      documentation and/or other materials provided with the distribution.
    * Neither the name of the Purewell nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/*
    Copyright (c) 2011 Minor Gordon
    All rights reserved

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
    * Neither the name of the Yield project nor the
    names of its contributors may be used to endorse or promote products
    derived from this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED. IN NO EVENT SHALL Minor Gordon BE LIABLE FOR ANY DIRECT,
    INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
    THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/*!
	@file
    @brief Definition of Utility of files
*/
#ifndef UTIL_FILE_H_
#define UTIL_FILE_H_

#include "util/type.h"
#include "util/time.h"
#include <memory>

namespace util {

namespace detail {
inline void checkOffsetSize() {
	int offsetSizeChecker[sizeof(off_t) == sizeof(uint64_t) ? 1 : -1];
	(void) offsetSizeChecker;
}
}

struct FileLib;
class FileStatus;

/*!
    @brief File open modes.
*/
class FileFlag {
public:
	static const int TYPE_READ_ONLY;
	static const int TYPE_WRITE_ONLY;
	static const int TYPE_READ_WRITE;
	static const int TYPE_CREATE;
	static const int TYPE_EXCLUSIVE;	
	static const int TYPE_TRUNCATE;	
	static const int TYPE_APPEND;
	static const int TYPE_NON_BLOCK;
	static const int TYPE_SYNC;
	static const int TYPE_ASYNC; 
	static const int TYPE_DIRECT;


	int getFlags() const;

	int setFlags(int flags);



	void setCreate(bool v);

	void setExclusive(bool v);

	void setTruncate(bool v);

	void setAppend(bool v);

	void setNonBlock(bool v);

	void setSync(bool v);

	void setAsync(bool v);

	void setDirect(bool v);

	bool isCreate() const;

	bool isExclusive() const;

	bool isTruncate() const;

	bool isAppend() const;

	bool isNonBlock() const;

	bool isSync() const;

	bool isAsync() const;

	bool isDirect() const;

	bool isReadOnly() const;

	bool isWriteOnly() const;

	bool isReadAndWrite() const;

	void swap(FileFlag &obj);

public:
	operator int() const;
	FileFlag& operator=(int flags);
	FileFlag& operator=(const FileFlag &flag);

	bool operator==(int flags) const;
	bool operator==(const FileFlag &flag) const;
	bool operator!=(int flags) const;
	bool operator!=(const FileFlag &flag) const;

public:
	FileFlag();
	FileFlag(int flags);
	FileFlag(const FileFlag &flag);
	virtual ~FileFlag();

private:
	int flags_;
};

/*!
    @brief File access modes.
*/
class FilePermission {
public:
	int getMode() const;

	int setMode(int mode);

	void setOwnerRead(bool v);

	void setOwnerWrite(bool v);

	void setOwnerExecute(bool v);

	void setGroupRead(bool v);

	void setGroupWrite(bool v);

	void setGroupExecute(bool v);

	void setGuestRead(bool v);

	void setGuestWrite(bool v);

	void setGuestExecute(bool v);

	bool isOwnerRead() const;

	bool isOwnerWrite() const;

	bool isOwnerExecute() const;

	bool isGroupRead() const;

	bool isGroupWrite() const;

	bool isGroupExecute() const;

	bool isGuestRead() const;

	bool isGuestWrite() const;

	bool isGuestExecute() const;

public:
	operator int() const;
	FilePermission& operator=(int mode);
	FilePermission& operator=(const FilePermission &obj);

	bool operator==(int mode) const;
	bool operator==(const FilePermission &obj) const;

	bool operator!=(int mode) const;
	bool operator!=(const FilePermission &obj) const;

public:
	FilePermission();
	FilePermission(int mode);
	FilePermission(const FilePermission &obj);
	virtual ~FilePermission();

public:
	int mode_;
};

class IOOperationQueue;

/*!
    @brief Operates asynchronous file I/O.
*/
class IOOperation {
public:
	IOOperation();
	~IOOperation();

	void setBuffer(void *buffer);
	void setRequestSize(size_t size);
	void setOffset(off_t offset);
	void* getBuffer();
	size_t getRequestSize();
	off_t getOffset();

	size_t getTransferredSize();
	bool isErrorOccurred();
	void confirmSuccess();
	void confirmAllTransferred();

private:
	friend class IOOperationQueue;
	friend class File;

	IOOperation(const IOOperation&);
	IOOperation& operator=(const IOOperation&);

	struct Data;
	Data *data_;
};

/*!
    @brief Queue of information of asynchronous file I/O.
*/
class IOQueue {
public:
	IOQueue();
	~IOQueue();

	void push(IOOperation &operation);
	IOOperation* pop();
	IOOperation* pop(uint32_t timeoutMillisec);
	void setInterruptible(bool enabled);
	void interrupt();

private:
	IOQueue(const IOQueue&);
	IOQueue& operator=(const IOQueue&);

	struct Data;
	Data *data_;
};

/*!
    @brief Operates basic file I/O.
*/
class File {
public:
#ifdef _WIN32
	typedef void *FD;
	static const FD INITIAL_FD;
#else
	typedef int FD;
	static const FD INITIAL_FD;
#endif

	static const FilePermission DEFAULT_PERMISSION;

	inline FD getHandle() const {
		return fd_;
	}

	virtual void close();

	virtual bool isClosed();

	virtual void attach(FD fd);

	virtual FD detach();

	virtual ssize_t read(void *buf, size_t blen);

	virtual ssize_t write(const void *buf, size_t blen);

	virtual ssize_t read(void *buf, size_t blen, off_t offset);

	virtual ssize_t write(const void *buf, size_t blen, off_t offset);

	virtual void read(IOOperation &operation);

	virtual void write(IOOperation &operation);

	virtual off_t tell();

	virtual void seek(int64_t offset);

	virtual void sync();

	File& getFile() { return *this; }

	void getStatus(FileStatus *status);

	void preAllocate(int mode, off_t offset, off_t len);

	void setSize(uint64_t size);

public:

	virtual void setBlockingMode(bool block);

	virtual void getBlockingMode(bool &block) const;

	virtual void setAsyncMode(bool async);

	virtual void getAsyncMode(bool &block) const;

	virtual void setCloseOnExecMode(bool coe);

	virtual void getCloseOnExecMode(bool &coe) const;

	virtual void setMode(int mode);

	virtual void getMode(int &mode) const;

	virtual void duplicate(FD srcfd);

	virtual void duplicate(FD srcfd, FD newfd);

public:
	File();
	virtual ~File();

private:
	File(const File&);
	File& operator=(const File&);

protected:
	FD fd_; 
};

/*!
    @brief Basic file associated with a path on the file system.
*/
class NamedFile : public File {
public:
	NamedFile();
	virtual ~NamedFile();

	virtual void open(const char8_t *name,
			FileFlag flags, FilePermission perm = DEFAULT_PERMISSION);

	const char8_t* getName() const;

	virtual FD detach();

	virtual bool unlink();

	virtual bool lock();

	virtual void unlock();

#if UTIL_FAILURE_SIMULATION_ENABLED
public:
	virtual ssize_t read(void *buf, size_t blen);

	virtual ssize_t write(const void *buf, size_t blen);

	virtual ssize_t read(void *buf, size_t blen, off_t offset);

	virtual ssize_t write(const void *buf, size_t blen, off_t offset);

	virtual void sync();
#endif	

private:
	NamedFile(const NamedFile&);
	NamedFile& operator=(const NamedFile&);

protected:
	u8string name_; 

	friend class FileUtil;
};

#if UTIL_FAILURE_SIMULATION_ENABLED
class NamedFileFailureSimulator {
public:
	typedef bool (*FilterHandler)(
			const NamedFile &file, int32_t targetType, int32_t operationType);

	static void set(int32_t targetType, uint64_t startCount, uint64_t endCount,
			FilterHandler handler);

	static void checkOperation(NamedFile &file, int32_t operationType);

	static uint64_t getLastOperationCount() { return lastOperationCount_; }

private:
	NamedFileFailureSimulator();
	~NamedFileFailureSimulator();

	static volatile bool enabled_;
	static volatile FilterHandler handler_;
	static volatile int32_t targetType_;
	static volatile uint64_t startCount_;
	static volatile uint64_t endCount_;
	static volatile uint64_t lastOperationCount_;
};
#endif	

/*!
    @brief Named pipe.
*/
class NamedPipe : public NamedFile {
public:
	virtual void open(const char8_t *name,
			FileFlag flags, FilePermission perm = DEFAULT_PERMISSION);

public:
	NamedPipe();
	virtual ~NamedPipe();

};


/*!
    @brief Manages a list of directory.
*/
class Directory {
public:
	explicit Directory(const char8_t *path);

	virtual ~Directory();

	bool nextEntry(u8string &name);

	void resetPosition();

	bool isSubDirectoryChecked();

	bool isParentOrSelfChecked();

private:
	Directory(const Directory&);
	Directory& operator=(const Directory&);

private:
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

/*!
    @brief File status.
*/
class FileStatus {
public:
	bool getStatus(const File &file);

	bool getStatus(int fd);

	bool getStatus(const char *path);

	bool getStatus2(const char *path);

	bool isSocket() const;

	bool isRegularFile() const;

	bool isDirectory() const;

	bool isCharacterDevice() const;

	bool isBlockDevice() const;

	bool isFIFO() const;

	bool isSymbolicLink() const;

public:
	dev_t getDevice() const;

	ino_t getINode() const;


	nlink_t getHardLinkCount() const;

	uid_t getUID() const;

	gid_t getGID() const;

	dev_t getRDevice() const;

	off_t getSize() const;

	blksize_t getBlockSize() const;

	blkcnt_t getBlockCount() const;

	DateTime getAccessTime() const;

	DateTime getModificationTime() const;

	DateTime getChangeTime() const;

	DateTime getCreationTime() const;

public:
	FileStatus();
	virtual ~FileStatus();

private:
	FileStatus(const FileStatus&);
	FileStatus& operator=(const FileStatus&);

private:
	friend struct FileLib;

	DateTime accessTime_;
	nlink_t hardLinkCount_;
	DateTime modificationTime_;
	uint64_t size_;

#ifdef _WIN32
	uint32_t attributes_;
	DateTime creationTime_;
#else
	blkcnt_t blockCount_;
	blksize_t blockSize_;
	DateTime changeTime_;
	dev_t device_;
	gid_t gid_;
	ino_t iNode_;
	int mode_;
	dev_t rDevice_;
	uid_t uid_;
#endif
};

/*!
    @brief File system status.
*/
class FileSystemStatus {
public:
	size_t getBlockSize() const;

	size_t getFragmentSize() const;

	uint64_t getBlocks() const;

	uint64_t getFreeBlocks() const;

	uint64_t getAvailableBlocks() const;

	uint64_t getINodes() const;

	uint64_t getFreeINodes() const;

	uint64_t getAvailableINodes() const;

	size_t getID() const;

	size_t getFlags() const;

	size_t getMaxFileNameSize() const;

	bool isReadOnly() const;

	bool isNoSUID() const;

public:
	FileSystemStatus();
	virtual ~FileSystemStatus();

private:
	FileSystemStatus(const FileSystemStatus&);
	FileSystemStatus& operator=(const FileSystemStatus&);

private:
	friend struct FileLib;

	size_t blockSize_;
	size_t fragmentSize_;
	uint64_t blockCount_;
	uint64_t freeBlockCount_;
	uint64_t availableBlockCount_;

#ifndef _WIN32
	uint64_t iNodeCount_;
	uint64_t freeINodeCount_;
	uint64_t availableINodeCount_;
	size_t id_;
	size_t flags_;
	size_t maxFileNameSize_;
#endif
};

/*!
    @brief Utility of the file system.
*/
class FileSystem {
public:

	static bool exists(const char8_t *path);

	static bool isDirectory(const char8_t *path);

	static bool isRegularFile(const char8_t *path);

	static void createDirectory(const char8_t *path);


	static void createDirectoryTree(const char8_t *path);


	static void createLink(
			const char8_t *sourcePath, const char8_t *targetPath);


	static void createPath(
			const char8_t *directoryName, const char8_t *baseName,
			u8string &path);

	static void getBaseName(const char8_t *path, u8string &name);

	static void getDirectoryName(const char8_t *path, u8string &directoryName);

	static void getFileStatus(const char8_t *path, FileStatus *status);

	static void getFileStatusNoFollow(const char8_t *path, FileStatus *status);

	static void getRealPath(const char8_t *path, u8string &realPath);


	static void getStatus(const char8_t *path, FileSystemStatus *status);

	static void move(const char8_t *sourcePath, const char8_t *targetPath);

	static void remove(const char8_t *path, bool recursive = true);

	static void removeDirectory(const char8_t *path);

	static void removeFile(const char8_t *path);

	static void touch(const char8_t *path);


	static void updateFileTime(const char8_t *path,
			const DateTime *accessTime = NULL,
			const DateTime *modifiedTime = NULL,
			const DateTime *creationTime = NULL);

	static bool getFDLimit(int32_t *cur, int32_t *max);

	static bool setFDLimit(int32_t cur, int32_t max);

	static void syncAll();

private:
	FileSystem();
	FileSystem(const FileSystem&);
	FileSystem& operator=(const FileSystem&);
};

/*!
    @brief Prevents of double execution by file.
*/
class PIdFile {
public:
	PIdFile();
	~PIdFile();

	void open(const char8_t *name);

	void close();

private:
	PIdFile(const PIdFile&);
	PIdFile& operator=(const PIdFile&);

	NamedFile base_;
	bool locked_;
};

} 

#endif
