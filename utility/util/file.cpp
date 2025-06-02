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
#include "util/file.h"
#include "util/code.h"
#include "util/system.h"
#include "util/os.h"
#include <vector>
#include <assert.h>

#if defined(UTIL_HAVE_DIRENT_H)
#	include <dirent.h>
#elif defined(UTIL_HAVE_SYS_DIRENT_H)
#	include <sys/dirent.h>
#endif

#ifdef UTIL_HAVE_SYS_RESOURCE_H
#include <limits>
#include <sys/resource.h>
#endif

#ifndef _WIN32
#include <sys/file.h>		
#endif

namespace util {


#ifdef _WIN32
const int FileFlag::TYPE_READ_ONLY = 00;
const int FileFlag::TYPE_WRITE_ONLY = 01;
const int FileFlag::TYPE_READ_WRITE = 02;
const int FileFlag::TYPE_CREATE = 0100;
const int FileFlag::TYPE_EXCLUSIVE = 0200;
const int FileFlag::TYPE_TRUNCATE = 01000;
const int FileFlag::TYPE_APPEND = 02000;
const int FileFlag::TYPE_NON_BLOCK = 04000;
const int FileFlag::TYPE_SYNC = 04010000;
const int FileFlag::TYPE_ASYNC = 020000;
const int FileFlag::TYPE_DIRECT = 040000;
#else
const int FileFlag::TYPE_READ_ONLY = O_RDONLY;
const int FileFlag::TYPE_WRITE_ONLY = O_WRONLY;
const int FileFlag::TYPE_READ_WRITE = O_RDWR;
const int FileFlag::TYPE_CREATE = O_CREAT;
const int FileFlag::TYPE_EXCLUSIVE = O_EXCL;
const int FileFlag::TYPE_TRUNCATE = O_TRUNC;
const int FileFlag::TYPE_APPEND = O_APPEND;
const int FileFlag::TYPE_NON_BLOCK = O_NONBLOCK;
const int FileFlag::TYPE_SYNC = O_SYNC;
const int FileFlag::TYPE_ASYNC = O_ASYNC;
const int FileFlag::TYPE_DIRECT = O_DIRECT;
#endif

FileFlag::FileFlag() :
		flags_(0) {
}

FileFlag::FileFlag(int flags) :
		flags_(flags) {
}

FileFlag::FileFlag(const FileFlag &obj) :
		flags_(obj.flags_) {
}

FileFlag::~FileFlag() {
}

int FileFlag::getFlags(void) const {
	return flags_;
}

int FileFlag::setFlags(int flags) {
	flags_ = flags;
	return flags_;
}

void FileFlag::setCreate(bool v) {
	FileLib::setFlag(flags_, TYPE_CREATE, v);
}

bool FileFlag::isCreate(void) const {
	return FileLib::getFlag(flags_, TYPE_CREATE);
}

void FileFlag::setExclusive(bool v) {
	FileLib::setFlag(flags_, TYPE_EXCLUSIVE, v);
}

bool FileFlag::isExclusive(void) const {
	return FileLib::getFlag(flags_, TYPE_EXCLUSIVE);
}

void FileFlag::setTruncate(bool v) {
	FileLib::setFlag(flags_, TYPE_TRUNCATE, v);
}

bool FileFlag::isTruncate(void) const {
	return FileLib::getFlag(flags_, TYPE_TRUNCATE);
}

void FileFlag::setAppend(bool v) {
	FileLib::setFlag(flags_, TYPE_APPEND, v);
}

bool FileFlag::isAppend(void) const {
	return FileLib::getFlag(flags_, TYPE_APPEND);
}

void FileFlag::setNonBlock(bool v) {
	FileLib::setFlag(flags_, TYPE_NON_BLOCK, v);
}

bool FileFlag::isNonBlock(void) const {
	return FileLib::getFlag(flags_, TYPE_NON_BLOCK);
}

void FileFlag::setSync(bool v) {
	FileLib::setFlag(flags_, TYPE_SYNC, v);
}

bool FileFlag::isSync(void) const {
	return FileLib::getFlag(flags_, TYPE_SYNC);
}

void FileFlag::setAsync(bool v) {
	FileLib::setFlag(flags_, TYPE_ASYNC, v);
}

bool FileFlag::isAsync(void) const {
	return FileLib::getFlag(flags_, TYPE_ASYNC);
}

void FileFlag::setDirect(bool v) {
	FileLib::setFlag(flags_, TYPE_DIRECT, v);
}

bool FileFlag::isDirect(void) const {
	return FileLib::getFlag(flags_, TYPE_DIRECT);
}

bool FileFlag::isReadOnly(void) const {
	return ((flags_ & (TYPE_WRITE_ONLY | TYPE_READ_WRITE)) ==
			TYPE_READ_ONLY);
}

bool FileFlag::isWriteOnly(void) const {
	return ((flags_ & TYPE_WRITE_ONLY) == TYPE_WRITE_ONLY);
}

bool FileFlag::isReadAndWrite(void) const {
	return ((flags_ & TYPE_READ_WRITE) == TYPE_READ_WRITE);
}



void FileFlag::swap(FileFlag &obj) {
	std::swap(flags_, obj.flags_);
}

FileFlag::operator int(void) const {
	return flags_;
}

FileFlag& FileFlag::operator=(int flags) {
	flags_ = flags;
	return *this;
}

FileFlag& FileFlag::operator=(const FileFlag &flag) {
	flags_ = flag.flags_;
	return *this;
}

bool FileFlag::operator==(int flags) const {
	return flags_ == flags;
}

bool FileFlag::operator==(const FileFlag &flags) const {
	return flags_ == flags.flags_;
}

bool FileFlag::operator!=(int flags) const {
	return flags_ != flags;
}

bool FileFlag::operator!=(const FileFlag &flags) const {
	return flags_ != flags.flags_;
}


FilePermission::FilePermission() :
		mode_(0) {
}

FilePermission::FilePermission(int mode) :
		mode_(mode) {
}

FilePermission::FilePermission(const FilePermission &mode) :
		mode_(mode.mode_) {
}

FilePermission::~FilePermission() {
}

int FilePermission::getMode(void) const {
	return mode_;
}

int FilePermission::setMode(int mode) {
	mode_ = mode;
	return mode_;
}

void FilePermission::setOwnerRead(bool v) {
	FileLib::setFlag(mode_, 00400, v);
}

bool FilePermission::isOwnerRead(void) const {
	return FileLib::getFlag(mode_, 00400);
}

void FilePermission::setOwnerWrite(bool v) {
	FileLib::setFlag(mode_, 00200, v);
}

bool FilePermission::isOwnerWrite(void) const {
	return FileLib::getFlag(mode_, 00200);
}

void FilePermission::setOwnerExecute(bool v) {
	FileLib::setFlag(mode_, 00100, v);
}

bool FilePermission::isOwnerExecute(void) const {
	return FileLib::getFlag(mode_, 00100);
}

void FilePermission::setGroupRead(bool v) {
	FileLib::setFlag(mode_, 00040, v);
}

bool FilePermission::isGroupRead(void) const {
	return FileLib::getFlag(mode_, 00040);
}

void FilePermission::setGroupWrite(bool v) {
	FileLib::setFlag(mode_, 00020, v);
}

bool FilePermission::isGroupWrite(void) const {
	return FileLib::getFlag(mode_, 00020);
}

void FilePermission::setGroupExecute(bool v) {
	FileLib::setFlag(mode_, 00010, v);
}

bool FilePermission::isGroupExecute(void) const {
	return FileLib::getFlag(mode_, 00010);
}

void FilePermission::setGuestRead(bool v) {
	FileLib::setFlag(mode_, 00004, v);
}

bool FilePermission::isGuestRead(void) const {
	return FileLib::getFlag(mode_, 00004);
}

void FilePermission::setGuestWrite(bool v) {
	FileLib::setFlag(mode_, 00002, v);
}

bool FilePermission::isGuestWrite(void) const {
	return FileLib::getFlag(mode_, 00002);
}

void FilePermission::setGuestExecute(bool v) {
	FileLib::setFlag(mode_, 00001, v);
}

bool FilePermission::isGuestExecute(void) const {
	return FileLib::getFlag(mode_, 00001);
}

FilePermission::operator int(void) const {
	return mode_;
}

FilePermission& FilePermission::operator=(int mode) {
	mode_ = mode;
	return *this;
}

FilePermission& FilePermission::operator=(const FilePermission &perm) {
	mode_ = perm.mode_;
	return *this;
}

bool FilePermission::operator==(int mode) const {
	return mode_ == mode;
}

bool FilePermission::operator==(const FilePermission &perm) const {
	return mode_ == perm.mode_;
}

bool FilePermission::operator!=(int mode) const {
	return mode_ != mode;
}

bool FilePermission::operator!=(const FilePermission &perm) const {
	return mode_ != perm.mode_;
}


#ifdef _WIN32
const File::FD File::INITIAL_FD = NULL;
#else
const File::FD File::INITIAL_FD = -1;
#endif

const FilePermission File::DEFAULT_PERMISSION = 0664;

File::File() : fd_(INITIAL_FD) {
}

File::~File() try {
	close();
}
catch (...) {
}

void File::close(void) {
	FD fd = detach();
	if (INITIAL_FD != fd) {
#ifdef _WIN32
		CloseHandle(fd);
#else
		::close(fd);
#endif
	}
}

bool File::isClosed(void) {
	return (INITIAL_FD == fd_);
}

void File::attach(FD fd) {
	assert (isClosed());
	fd_ = fd;
}

File::FD File::detach(void) {
	FD fd = fd_;
	fd_ = INITIAL_FD;
	return fd;
}

ssize_t File::read(void *buf, size_t blen) {
#ifdef _WIN32
	return read(buf, blen, tell());
#else
	const ssize_t result = ::read(fd_, static_cast<char*>(buf), blen);
	if (result < 0) {
		if (errno == EINTR) {
			return 0;
		}
		else {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	return result;
#endif
}

ssize_t File::write(const void *buf, size_t blen) {
#ifdef _WIN32
	return write(buf, blen, tell());
#else
	const ssize_t result = ::write(fd_, static_cast<const char*>(buf), blen);
	if (result < 0) {
		if (errno == EINTR) {
			return 0;
		}
		else {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	return result;
#endif
}

ssize_t File::read(void *buf, size_t blen, off_t offset) {
#ifdef _WIN32
	OVERLAPPED overlapped;

	overlapped.Offset = static_cast<DWORD>(offset);
	overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
	overlapped.hEvent = NULL;
	overlapped.Internal = 0;
	overlapped.InternalHigh = 0;

	DWORD readBytes;
	if (!ReadFile(fd_, buf,
			static_cast<DWORD>(blen), &readBytes, &overlapped)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return static_cast<ssize_t>(readBytes);
#else
	const ssize_t result = ::pread(fd_, buf, blen, offset);
	if (result < 0) {
		if (errno == EINTR) {
			return 0;
		}
		else {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	return result;
#endif
}

ssize_t File::write(const void *buf, size_t blen, off_t offset) {
#ifdef _WIN32
	OVERLAPPED overlapped;

	overlapped.Offset = static_cast<DWORD>(offset);
	overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
	overlapped.hEvent = NULL;
	overlapped.Internal = 0;
	overlapped.InternalHigh = 0;

	DWORD writeBytes;
	if (!WriteFile(fd_, buf,
			static_cast<DWORD>(blen), &writeBytes, &overlapped)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return static_cast<ssize_t>(writeBytes);
#else
	const ssize_t result = ::pwrite(fd_, buf, blen, offset);
	if (result < 0) {
		if (errno == EINTR) {
			return 0;
		}
		else {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	return result;
#endif
}

off_t File::tell() {
#ifdef _WIN32
	ULARGE_INTEGER fp;
	LONG fpHigh = 0;
	fp.LowPart = SetFilePointer(fd_, 0, &fpHigh, FILE_CURRENT);
	if (fp.LowPart == INVALID_SET_FILE_POINTER) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	fp.HighPart = fpHigh;
	return fp.QuadPart;
#else
	const off_t result = lseek64(fd_, 0, SEEK_CUR);
	if (result == static_cast<off_t>(-1)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return result;
#endif
}

void File::seek(int64_t offset) {
#ifdef _WIN32
	LARGE_INTEGER fp;
	LONG fpHigh = 0;
	if (offset >= 0) { 
		fp.QuadPart = offset;
		LONG result = SetFilePointer(fd_, fp.LowPart, &fp.HighPart, FILE_BEGIN);
		if (result == INVALID_SET_FILE_POINTER) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	else { 
		fp.LowPart = SetFilePointer(fd_, 0, &fpHigh, FILE_END);
		if (fp.LowPart == INVALID_SET_FILE_POINTER) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
#else
	if (offset >= 0) { 
		const off_t result = lseek64(fd_, offset, SEEK_SET);
		if (result == static_cast<off_t>(-1)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	else { 
		const off_t result = lseek64(fd_, 0, SEEK_END);
		if (result == static_cast<off_t>(-1)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
#endif
}

void File::sync() {
#ifdef _WIN32
	if (!FlushFileBuffers(fd_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	if (fsync(fd_) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void File::preAllocate(int mode, off_t offset, off_t len) {
#ifdef _WIN32
#else
	if (fallocate(fd_, mode, offset, len) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void File::getStatus(FileStatus *status) {
#ifdef _WIN32
	BY_HANDLE_FILE_INFORMATION info;
	if (!GetFileInformationByHandle(fd_, &info)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	FileLib::getFileStatus(info, status);
#else
	struct stat stBuf;
	if (fstat(fd_, &stBuf) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	FileLib::getFileStatus(stBuf, status);
#endif
}

void File::setSize(uint64_t size) {
#ifdef _WIN32
	LARGE_INTEGER fp;
	fp.QuadPart = static_cast<LONGLONG>(size);
	if (SetFilePointerEx(fd_, fp, NULL, FILE_BEGIN) ==
			INVALID_SET_FILE_POINTER) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (!SetEndOfFile(fd_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	if (ftruncate64(fd_, static_cast<off64_t>(size)) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void File::setBlockingMode(bool block) {
#ifdef O_NONBLOCK
	FileLib::setFlags(!block, fd_, O_NONBLOCK);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::getBlockingMode(bool &block) const {
#ifdef O_NONBLOCK
	FileLib::getFlags(block, fd_, O_NONBLOCK);

	block = !block;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::setAsyncMode(bool async) {
#ifdef O_ASYNC
	FileLib::setFlags(async, fd_, O_ASYNC);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::getAsyncMode(bool &async) const {
#ifdef O_ASYNC
	FileLib::getFlags(async, fd_, O_ASYNC);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::setCloseOnExecMode(bool coe) {
#ifdef FD_CLOEXEC
	FileLib::setFDFlags(coe, fd_, FD_CLOEXEC);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::getCloseOnExecMode(bool &coe) const {
#ifdef FD_CLOEXEC
	FileLib::getFDFlags(coe, fd_, FD_CLOEXEC);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::getMode(int &mode) const {
#ifdef UTIL_HAVE_FCNTL
	const int result = fcntl(fd_, F_GETFL);
	if (-1 == result) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	mode = result;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::setMode(int mode) {
#ifdef UTIL_HAVE_FCNTL
	if (0 != fcntl(fd_, F_SETFL, mode)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void File::duplicate(FD srcfd) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	const FD dfd = dup(srcfd);
	if (INITIAL_FD == dfd)
		UTIL_THROW_PLATFORM_ERROR(NULL);

	attach(dfd);
#endif
}

void File::duplicate(FD srcfd, FD newfd) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	const FD dfd = dup2(srcfd, newfd);
	if (INITIAL_FD == dfd)
		UTIL_THROW_PLATFORM_ERROR(NULL);

	attach(dfd);
#endif
}


NamedFile::NamedFile() {
}

NamedFile::~NamedFile() {
}

void NamedFile::open(const char8_t *name, FileFlag flags, FilePermission perm) {
	u8string nameStr(name);

#ifdef _WIN32
	DWORD dwDesiredAccess = 0;
	DWORD dwCreationDisposition = 0;
	DWORD dwFlagsAndAttributes = FILE_FLAG_SEQUENTIAL_SCAN;

	if (flags.isAppend()) {
		dwDesiredAccess |= FILE_APPEND_DATA;
	}
	else if (flags.isReadAndWrite()) {
		dwDesiredAccess |= GENERIC_READ | GENERIC_WRITE;
	}
	else if (flags.isWriteOnly()) {
		dwDesiredAccess |= GENERIC_WRITE;
	}
	else {
		dwDesiredAccess |= GENERIC_READ;
	}

	if (flags.isCreate()) {
		if (flags.isExclusive()) {
			dwCreationDisposition = CREATE_NEW;
		}
		else if (flags.isTruncate()) {
			dwCreationDisposition = CREATE_ALWAYS;
		}
		else {
			dwCreationDisposition = OPEN_ALWAYS;
		}
	} else {
		dwCreationDisposition = OPEN_EXISTING;
	}


	if (flags.isSync()) {
		dwFlagsAndAttributes |= FILE_FLAG_WRITE_THROUGH;
	}

	if (flags.isDirect()) {
		dwFlagsAndAttributes |= FILE_FLAG_NO_BUFFERING;
	}

	if (flags.isAsync()) {
		dwFlagsAndAttributes |= FILE_FLAG_OVERLAPPED;
	}

	std::wstring encodedName;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(nameStr, encodedName);

	const uint32_t MAX_RETRIES = 10;
	const DWORD RETRY_DELAY = 250;
	HANDLE fd;
	for (uint32_t retries = 0;;) {
		fd = CreateFileW(encodedName.c_str(), dwDesiredAccess,
				FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
				dwCreationDisposition, dwFlagsAndAttributes, NULL);

		if (fd == INVALID_HANDLE_VALUE) {
			if (GetLastError() == ERROR_SHARING_VIOLATION &&
					++retries <= MAX_RETRIES) {
				Sleep(RETRY_DELAY);
				continue;
			}
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		else {
			break;
		}
	}

	if (flags.isTruncate()/* && flags.isCreate()*/) {
		SetFilePointer(fd, 0, NULL, FILE_BEGIN);
		SetEndOfFile(fd);
	}

#else
	std::string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(nameStr, encodedName);
	const int fd = ::open(
			encodedName.c_str(), flags | O_LARGEFILE, static_cast<int>(perm));
	if (-1 == fd) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif	
	name_.swap(nameStr);
	fd_ = fd;
}

const char8_t* NamedFile::getName(void) const {
	return name_.c_str();
}

bool NamedFile::unlink(void) {
#ifdef _WIN32
	std::wstring encodedName;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(name_, encodedName);
	const bool res = (DeleteFileW(encodedName.c_str()) != 0);
#else
	std::string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(name_, encodedName);
	const bool res = (0 == ::unlink(encodedName.c_str()));
#endif
	if (res) {
		name_.clear();
	}

	return res;
}

bool NamedFile::lock() {
	if (isClosed()) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}
#ifdef _WIN32
	if (LockFile(fd_, 0, 0,
			std::numeric_limits<DWORD>::max(),
			std::numeric_limits<DWORD>::max()) == 0) {
		return false;
	}
#else
	if (flock(fd_, LOCK_EX | LOCK_NB) != 0) {
		if (errno == EINTR || errno == EWOULDBLOCK) {
			return false;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
	return true;
}

void NamedFile::unlock() {
	if (isClosed()) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

#ifdef _WIN32
	if (UnlockFile(fd_, 0, 0,
			std::numeric_limits<DWORD>::max(),
			std::numeric_limits<DWORD>::max()) == 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	if (flock(fd_, LOCK_UN | LOCK_NB) != 0) {
		if (errno != EINTR) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
#endif
}

NamedFile::FD NamedFile::detach(void) {
	const FD fd = fd_;
	name_.clear();
	fd_ = INITIAL_FD;
	return fd;
}

#if UTIL_FAILURE_SIMULATION_ENABLED
ssize_t NamedFile::read(void *buf, size_t blen) {
	NamedFileFailureSimulator::checkOperation(*this, 0);
	return File::read(buf, blen);
}

ssize_t NamedFile::write(const void *buf, size_t blen) {
	NamedFileFailureSimulator::checkOperation(*this, 0);
	return File::write(buf, blen);
}

ssize_t NamedFile::read(void *buf, size_t blen, off_t offset) {
	NamedFileFailureSimulator::checkOperation(*this, 0);
	return File::read(buf, blen, offset);
}

ssize_t NamedFile::write(const void *buf, size_t blen, off_t offset) {
	NamedFileFailureSimulator::checkOperation(*this, 0);
	return File::write(buf, blen, offset);
}

void NamedFile::sync() {
	NamedFileFailureSimulator::checkOperation(*this, 0);
	File::sync();
}
#endif	

#if UTIL_FAILURE_SIMULATION_ENABLED
volatile bool NamedFileFailureSimulator::enabled_ = false;
volatile NamedFileFailureSimulator::FilterHandler
		NamedFileFailureSimulator::handler_ = NULL;
volatile int32_t NamedFileFailureSimulator::targetType_ = 0;
volatile uint64_t NamedFileFailureSimulator::startCount_ = 0;
volatile uint64_t NamedFileFailureSimulator::endCount_ = 0;
volatile uint64_t NamedFileFailureSimulator::lastOperationCount_ = 0;

void NamedFileFailureSimulator::set(
		int32_t targetType, uint64_t startCount, uint64_t endCount,
		FilterHandler handler) {
	if (startCount < endCount) {
		if (handler == NULL) {
			UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
		}
		enabled_ = false;
		handler_ = handler;
		targetType_ = targetType;
		startCount_ = startCount;
		endCount_ = endCount;
		lastOperationCount_ = 0;
		enabled_ = true;
	}
	else {
		enabled_ = false;
		handler_ = NULL;
		targetType_ = 0;
		startCount_ = 0;
		endCount_ = 0;
	}
}

void NamedFileFailureSimulator::checkOperation(
		NamedFile &file, int32_t operationType) {
	FilterHandler handler = handler_;
	if (enabled_ && handler != NULL &&
			(*handler)(file, targetType_, operationType)) {
		const uint64_t count = lastOperationCount_;
		lastOperationCount_++;

		if (startCount_ <= count && count < endCount_) {
#ifdef _WIN32
			SetLastError(static_cast<DWORD>(-1));
#else
			errno = -1;
#endif
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
}

#endif	


NamedPipe::NamedPipe() {
}

NamedPipe::~NamedPipe() {
}


void NamedPipe::open(
		const char8_t *name, FileFlag flags, FilePermission perm) {
#ifdef _WIN32
	std::string nameStr(name);

	static const char *const prefix = "\\\\.\\pipe\\";
	if (nameStr.find(prefix) != 0) {
		nameStr.insert(0, prefix);
	}

	DWORD openMode = 0;
	if ((flags & FileFlag::TYPE_ASYNC) != 0) {
		openMode |= FILE_FLAG_OVERLAPPED;
	}
	if ((flags & FileFlag::TYPE_READ_WRITE) != 0) {
		openMode |= PIPE_ACCESS_DUPLEX;
	} else if ((flags & FileFlag::TYPE_WRITE_ONLY) != 0) {
		openMode |= PIPE_ACCESS_OUTBOUND;
	} else {
		openMode |= PIPE_ACCESS_INBOUND;
	}

	DWORD pipeMode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE;
	if ((flags & FileFlag::TYPE_NON_BLOCK) != 0) {
		pipeMode |= PIPE_NOWAIT;
	} else {
		pipeMode |= PIPE_WAIT;
	}

	std::wstring encodedName;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(nameStr, encodedName);
	const HANDLE handle = CreateNamedPipeW(encodedName.c_str(), openMode,
			pipeMode, PIPE_UNLIMITED_INSTANCES, 4096, 4096, 0, NULL);
	if (handle == INVALID_HANDLE_VALUE) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	fd_ = handle;
#else
	std::string nameStr(name);

	u8string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(nameStr, encodedName);

	int nflags = flags & ~(O_CREAT | O_EXCL);
	int fd = ::open(name, nflags, static_cast<int>(perm));

	if (-1 == fd) {
		if (flags & O_CREAT) {
			if (0 != mkfifo(encodedName.c_str(), perm)) {
				UTIL_THROW_PLATFORM_ERROR(NULL);
			}

			if (-1 == (fd = ::open(
					encodedName.c_str(), nflags, static_cast<int>(perm)))) {
				::unlink(encodedName.c_str());
				UTIL_THROW_PLATFORM_ERROR(NULL);
			}

			fd_ = fd;
			name_.swap(nameStr);
			return;
		}

		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (flags & O_EXCL) {
		::close(fd);
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	struct stat stat;
	if (0 != fstat(fd, &stat)) {
		::close(fd);
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (!S_ISFIFO(stat.st_mode)) {
		::close(fd);
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	fd_ = fd;
	name_.swap(nameStr);
#endif
}

#if UTIL_MINOR_MODULE_ENABLED


#ifndef _WIN32
UTIL_FLAG_TYPE UTIL_MEM_OMODE_EXEC = PROT_EXEC;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_READ = PROT_READ;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_WRITE = PROT_WRITE;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_NONE = PROT_NONE;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_DEFAULT = PROT_READ|PROT_WRITE;
#else
UTIL_FLAG_TYPE UTIL_MEM_OMODE_EXEC = 0;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_READ = 0;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_WRITE = 0;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_NONE = 0;
UTIL_FLAG_TYPE UTIL_MEM_OMODE_DEFAULT = 0;
#endif

#ifndef _WIN32
UTIL_FLAG_TYPE UTIL_MEM_FILE_FORCE = MAP_FIXED;
UTIL_FLAG_TYPE UTIL_MEM_FILE_SHARED = MAP_SHARED;
UTIL_FLAG_TYPE UTIL_MEM_FILE_PRIVATE = MAP_PRIVATE;
UTIL_FLAG_TYPE UTIL_MEM_FILE_DEFAULT = MAP_PRIVATE;
#else
UTIL_FLAG_TYPE UTIL_MEM_FILE_FORCE = 0;
UTIL_FLAG_TYPE UTIL_MEM_FILE_SHARED = 0;
UTIL_FLAG_TYPE UTIL_MEM_FILE_PRIVATE = 0;
UTIL_FLAG_TYPE UTIL_MEM_FILE_DEFAULT = 0;
#endif

struct Memory::Data {
public:
	struct Normal;
	struct FileMap;

	virtual Memory::MEM_TYPE getType(void) const = 0;

	virtual void close(void) = 0;

	inline size_t getSize(void) const {
		return size_;
	}

	inline void* getBlock(void) {
		return mem_;
	}

	inline const void* getBlock(void) const {
		return mem_;
	}

public:
	inline Data() :
			size_(0), mem_(NULL) {
	}
	virtual ~Data() {
	}

protected:
	size_t size_;
	void *mem_;
};

struct Memory::Data::Normal : public Memory::Data {
public:
	inline Memory::MEM_TYPE getType(void) const {
		return Memory::MEM_NORMAL;
	}

	inline void close(void) {
		if (NULL != mem_) {
			UTIL_FREE(mem_);
			mem_ = NULL;
			size_ = 0;
		}
	}

	Normal(size_t size) {
		void* ptr(UTIL_MALLOC(size));
		if (NULL == ptr) {
			UTIL_THROW_UTIL_ERROR_CODED(CODE_NO_MEMORY);
		}

		mem_ = ptr;
		size_ = size;
	}
};

struct Memory::Data::FileMap : public Memory::Data {
public:
	inline Memory::MEM_TYPE getType(void) const {
		return Memory::MEM_FILE;
	}

	inline void close(void) {
#ifdef _WIN32
		UTIL_THROW_NOIMPL_UTIL();
#else
		if (NULL != mem_) {
			munmap(mem_, size_);
			mem_ = NULL;
			size_ = 0;
		}
#endif
	}

#ifndef _WIN32
	FileMap(void *start, int fd, off_t offset, size_t size, int prot,
			int flag) :
			fd_(fd), offset_(offset), prot_(prot), flag_(flag) {
		void* ptr = mmap(start, size, prot, flag, fd, offset);
		if (MAP_FAILED == ptr) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		mem_ = ptr;
		size_ = size;
	}
#endif

protected:
#ifndef _WIN32
	int fd_;
	off_t offset_;
	int prot_;
	int flag_;
#endif
};

Memory::Memory(size_t size) : data_(new Data::Normal(size)) {
}

Memory::Memory(size_t size, File &file,
		off_t offset, int omode, int flags, void *startPtr) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	data_.reset(new Data::FileMap(
			startPtr, file.getHandle(), offset, size, omode, flags));
#endif
}

Memory::Memory(size_t size, File::FD fd,
		off_t offset, int omode, int flags, void *startPtr) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	data_.reset(new Data::FileMap(startPtr, fd, offset, size, omode, flags));
#endif
}

Memory::~Memory() {
}

Memory::MEM_TYPE Memory::getType(void) const {
	return data_->getType();
}

size_t Memory::getSize(void) const {
	return data_->getSize();
}

const void* Memory::operator()(void) const {
	return data_->getBlock();
}

void* Memory::operator()(void) {
	return data_->getBlock();
}

void* Memory::getMemory(void) {
	return data_->getBlock();
}

const void* Memory::getMemory(void) const {
	return data_->getBlock();
}


MessageQueue::MessageQueue() {
}

MessageQueue::~MessageQueue() {
	close();
}

void MessageQueue::close(void) {
#ifndef _WIN32
	if (-1 != fd_) {
		mq_close((mqd_t) fd_);
		fd_ = -1;
	}
#endif
}

void MessageQueue::open(
		const char8_t *name, FileFlag flags, FilePermission perm) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	u8string nameStr = name;
	std::string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(nameStr, encodedName);

	mqd_t fd = (mqd_t) -1;
	if (flags & O_CREAT) {
		fd = mq_open(
				encodedName.c_str(), flags, static_cast<int>(perm), NULL);
	} else {
		fd = mq_open(encodedName.c_str(), flags);
	}

	if ((mqd_t) -1 == fd) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	name_.swap(nameStr);
	fd_ = (int) fd;
#endif
}

void MessageQueue::open(const char8_t *name, FileFlag flags,
		FilePermission perm, size_t maxmsg, size_t msgsize) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	u8string nameStr = name;
	std::string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(nameStr, encodedName);

	mqd_t fd = (mqd_t) -1;
	if (flags & O_CREAT) {
		struct mq_attr attr;
		memset(&attr, 0x00, sizeof(attr));
		attr.mq_maxmsg = maxmsg;
		attr.mq_msgsize = msgsize;
		attr.mq_curmsgs = 0;
		fd = mq_open(
				encodedName.c_str(), flags, static_cast<int>(perm), &attr);
	} else {
		fd = mq_open(encodedName.c_str(), flags);
	}

	if ((mqd_t) -1 == fd) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	name_.swap(nameStr);
	fd_ = (int) fd;
#endif
}

bool MessageQueue::unlink(void) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	const bool res = (0 == (int) mq_unlink(name_.c_str()));
	name_.clear();
	return res;
#endif
}

ssize_t MessageQueue::send(const void *buf, size_t blen, size_t priority) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != mq_send((mqd_t) fd_, (const char*) buf, blen, priority)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return static_cast<ssize_t>(blen);
#endif
}

ssize_t MessageQueue::sendTimeLimit(const void *buf, size_t blen,
		size_t priority, size_t msec) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	struct timespec ts;
	ts.tv_sec = msec / 1000;
	ts.tv_nsec = (msec % 1000) * 1000;
	if (0 != mq_timedsend(
			(mqd_t) fd_, (const char*) buf, blen, priority, &ts)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return static_cast<ssize_t>(blen);
#endif
}

ssize_t MessageQueue::receive(void *buf, size_t blen, size_t *priority) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	unsigned int prior = 0;
	const ssize_t result = mq_receive((mqd_t) fd_, (char*) buf, blen, &prior);
	if (priority) {
		*priority = (size_t) prior;
	}
	if (result < 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return result;
#endif
}

ssize_t MessageQueue::receiveTimeLimit(void *buf, size_t blen,
		size_t *priority, size_t msec) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	struct timespec ts;
	ts.tv_sec = msec / 1000;
	ts.tv_nsec = (msec % 1000) * 1000;
	unsigned int prior = 0;
	const ssize_t result =
			mq_timedreceive((mqd_t) fd_, (char*) buf, blen, &prior, &ts);
	if (priority) {
		*priority = (size_t) prior;
	}
	if (result < 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return result;
#endif
}

size_t MessageQueue::getCurrentCount(void) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	size_t ret = (size_t) -1;
	getStatus(NULL, NULL, &ret);
	return ret;
#endif
}

size_t MessageQueue::getMaxCount(void) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	size_t ret = (size_t) -1;
	getStatus(&ret, NULL, NULL);
	return ret;
#endif
}

size_t MessageQueue::getMessageSize(void) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	size_t ret = (size_t) -1;
	getStatus(NULL, &ret, NULL);
	return ret;
#endif
}

#ifndef _WIN32
void MessageQueue::getStatus(size_t* __restrict__ maxcount,
		size_t* __restrict__ msgsize, size_t* __restrict__ curcount) const {
	struct mq_attr attr;
	if (-1 == mq_getattr(fd_, &attr))
		UTIL_THROW_PLATFORM_ERROR(NULL);

	if (maxcount)
		*maxcount = attr.mq_maxmsg;
	if (msgsize)
		*msgsize = attr.mq_msgsize;
	if (curcount)
		*curcount = attr.mq_curmsgs;
}
#else
void MessageQueue::getStatus(size_t *maxcount, size_t *msgsize,
		size_t *curcount) const {
	UTIL_THROW_NOIMPL_UTIL();
}
#endif

ssize_t MessageQueue::read(void *buf, size_t blen) {
	return receive(buf, blen, NULL);
}

ssize_t MessageQueue::write(const void *buf, size_t blen) {
	return send(buf, blen, 0);
}


const FilePermission SharedMemory::DEFAULT_PERMISSION = 0777;

SharedMemory::SharedMemory() {
}

SharedMemory::~SharedMemory() {
}

void SharedMemory::open(const char8_t *name,
		FileFlag flags, FilePermission perm) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	u8string nameStr = name;
	std::string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(nameStr, encodedName);

	const int fd = shm_open(encodedName.c_str(), flags, perm);
	if (-1 == fd) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	name_ = name;
	fd_ = fd;
#endif
}

bool SharedMemory::unlink(void) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	std::string encodedName;
	CodeConverter(Code::UTF8, Code::CHAR)(name_, encodedName);

	const bool res = (0 == shm_unlink(encodedName.c_str()));
	if (res) {
		name_.clear();
	}

	return res;
#endif
}

#endif 


namespace {
#ifdef UTIL_HAVE_OPENDIR
union DirEnt {
	struct dirent body_;
	char padding_[sizeof(struct dirent) + NAME_MAX + 1];
};
#endif
}

struct Directory::Data {
	bool subDir_;
	bool parentOrSelf_;
#ifdef UTIL_HAVE_OPENDIR
	DIR *dir_;
	std::string basePath_;
#else
	HANDLE handle_;
	std::wstring pathPattern_;
#endif
};

Directory::Directory(const char8_t *path) : data_(new Data()) {
#ifdef UTIL_HAVE_OPENDIR
	CodeConverter(Code::UTF8, Code::CHAR)(path, data_->basePath_);
	data_->dir_ = opendir(data_->basePath_.c_str());
	if (data_->dir_ == NULL) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	std::wstring &pattern = data_->pathPattern_;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pattern);
	if (!pattern.empty()) {
		const wchar_t tail = *(--pattern.end());
		if (tail == L'\\') {
			pattern += L'*';
		}
		else if (tail != L'*') {
			pattern += L"\\*";
		}

		WIN32_FIND_DATAW findData;
		data_->handle_ = FindFirstFileW(pattern.c_str(), &findData);
		if (data_->handle_ == INVALID_HANDLE_VALUE) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
#endif
	data_->subDir_ = false;
	data_->parentOrSelf_ = false;
}

Directory::~Directory() {
#ifdef UTIL_HAVE_OPENDIR
	closedir(data_->dir_);
#else
	FindClose(data_->handle_);
#endif
}

bool Directory::nextEntry(u8string &name) {
	name.clear();

#ifdef UTIL_HAVE_OPENDIR
	struct dirent *result;
	errno = 0;
	result = readdir(data_->dir_);
	if (result == NULL) {
		if (errno != 0) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		return false;
	}

	struct stat fileStat;
	if (stat((data_->basePath_ + "/" + result->d_name).c_str(), &fileStat) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	CodeConverter(Code::CHAR, Code::UTF8)(result->d_name, name);

	data_->subDir_ = ((fileStat.st_mode & S_IFDIR) != 0);
	data_->parentOrSelf_ =
			(strcmp(".", result->d_name) == 0 || strcmp("..", result->d_name) == 0);
#else
	WIN32_FIND_DATAW findData;
	if (!FindNextFileW(data_->handle_, &findData)) {
		const DWORD lastError = GetLastError();
		if (lastError == ERROR_NO_MORE_FILES) {
			return false;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	CodeConverter(Code::WCHAR_T, Code::UTF8)(findData.cFileName, name);
	data_->subDir_ =
			((findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0);
	data_->parentOrSelf_ =
			(wcscmp(L".", findData.cFileName) == 0 ||
			wcscmp(L"..", findData.cFileName) == 0);
#endif
	return true;
}

void Directory::resetPosition(void) {
	data_->subDir_ = false;
	data_->parentOrSelf_ = false;
#ifdef UTIL_HAVE_OPENDIR
	rewinddir(data_->dir_);
#else
	WIN32_FIND_DATAW findData;
	data_->handle_ =
			FindFirstFileW(data_->pathPattern_.c_str(), &findData);
	if (data_->handle_ == INVALID_HANDLE_VALUE) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

bool Directory::isSubDirectoryChecked() {
	return data_->subDir_;
}

bool Directory::isParentOrSelfChecked() {
	return data_->parentOrSelf_;
}


bool FileStatus::isSocket(void) const {
#if !defined(_WIN32) && defined(S_ISSOCK)
	return 0 != S_ISSOCK(mode_);
#elif !defined(_WIN32)
#error
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

bool FileStatus::isRegularFile(void) const {
#if !defined(_WIN32) && defined(S_ISREG)
	return 0 != S_ISREG(mode_);
#elif !defined(_WIN32) && defined(S_IFREG)
	return S_IFREG == ((S_IFMT & mode_) & S_IFREG);
#elif !defined(_WIN32)
#error
#else
	return ((attributes_ & FILE_ATTRIBUTE_DIRECTORY) == 0 &&
			(attributes_ & FILE_ATTRIBUTE_DEVICE));
#endif
}

bool FileStatus::isDirectory(void) const {
#if !defined(_WIN32) && defined(S_ISDIR)
	return 0 != S_ISDIR(mode_);
#elif !defined(_WIN32)
#error
#else
	return ((attributes_ & FILE_ATTRIBUTE_DIRECTORY) != 0);
#endif
}

bool FileStatus::isCharacterDevice(void) const {
#if !defined(_WIN32) && defined(S_ISCHR)
	return 0 != S_ISCHR(mode_);
#elif !defined(_WIN32) && defined(S_IFCHR)
	return S_IFCHR == ((S_IFMT & mode_) & S_IFCHR);
#elif !defined(_WIN32) && defined(_S_IFCHR)
	return 0 != _S_IFCHR(mode_);
#elif !defined(_WIN32)
#error
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

bool FileStatus::isBlockDevice(void) const {
#if !defined(_WIN32) && defined(S_ISBLK)
	return 0 != S_ISBLK(mode_);
#elif !defined(_WIN32)
#error
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

bool FileStatus::isFIFO(void) const {
#if !defined(_WIN32) && defined(S_ISFIFO)
	return 0 != S_ISFIFO(mode_);
#elif !defined(_WIN32)
#error
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

bool FileStatus::isSymbolicLink(void) const {
#if !defined(_WIN32) && defined(S_ISLINK)
	return 0 != S_ISLNK(mode_);
#elif !defined(_WIN32)
	UTIL_THROW_NOIMPL_UTIL();
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

FileStatus::FileStatus() :
		accessTime_(0),
		hardLinkCount_(0),
		modificationTime_(0),
		size_(0)
#ifdef _WIN32
		, attributes_(0),
		creationTime_(0)
#else
		, blockCount_(0),
		blockSize_(0),
		changeTime_(0),
		device_(0),
		gid_(0),
		iNode_(0),
		mode_(0),
		rDevice_(0),
		uid_(0)
#endif
{
}

FileStatus::~FileStatus() {
}

dev_t FileStatus::getDevice(void) const {
#ifdef UTIL_HAVE_LSTAT
	return device_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

ino_t FileStatus::getINode(void) const {
#ifdef UTIL_HAVE_LSTAT
	return iNode_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

nlink_t FileStatus::getHardLinkCount(void) const {
	return hardLinkCount_;
}

uid_t FileStatus::getUID(void) const {
#ifdef UTIL_HAVE_LSTAT
	return uid_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

gid_t FileStatus::getGID(void) const {
#ifdef UTIL_HAVE_LSTAT
	return gid_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

dev_t FileStatus::getRDevice(void) const {
#ifdef UTIL_HAVE_LSTAT
	return rDevice_;
#else
	UTIL_THROW_PLATFORM_ERROR(NULL);
#endif
}

off_t FileStatus::getSize(void) const {
	return size_;
}

blksize_t FileStatus::getBlockSize(void) const {
#ifdef UTIL_HAVE_MEM_STAT_ST_BLKSIZE
	return blockSize_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

blkcnt_t FileStatus::getBlockCount(void) const {
#ifdef UTIL_HAVE_MEM_STAT_ST_BLOCKS
	return blockCount_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

DateTime FileStatus::getAccessTime(void) const {
	return accessTime_;
}

DateTime FileStatus::getModificationTime(void) const {
	return modificationTime_;
}

DateTime FileStatus::getChangeTime(void) const {
#ifdef UTIL_HAVE_LSTAT
	return changeTime_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

DateTime FileStatus::getCreationTime(void) const {
#ifdef _WIN32
	return creationTime_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}


size_t FileSystemStatus::getBlockSize(void) const {
	return blockSize_;
}

size_t FileSystemStatus::getFragmentSize(void) const {
	return fragmentSize_;
}

uint64_t FileSystemStatus::getBlocks(void) const {
	return blockCount_;
}

uint64_t FileSystemStatus::getFreeBlocks(void) const {
	return freeBlockCount_;
}

uint64_t FileSystemStatus::getAvailableBlocks(void) const {
	return availableBlockCount_;
}

uint64_t FileSystemStatus::getINodes(void) const {
#if UTIL_HAVE_STATVFS_T
	return freeINodeCount_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

uint64_t FileSystemStatus::getFreeINodes(void) const {
#if UTIL_HAVE_STATVFS_T
	return freeINodeCount_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

uint64_t FileSystemStatus::getAvailableINodes(void) const {
#if UTIL_HAVE_STATVFS_T
	return availableINodeCount_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

size_t FileSystemStatus::getID(void) const {
#if UTIL_HAVE_STATVFS_T
	return id_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

size_t FileSystemStatus::getFlags(void) const {
#if UTIL_HAVE_STATVFS_T
	return flags_;
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

size_t FileSystemStatus::getMaxFileNameSize(void) const {
#if UTIL_HAVE_STATVFS_T
	return maxFileNameSize_;
#else
	return MAX_PATH;
#endif
}

FileSystemStatus::FileSystemStatus() :
		blockSize_(0),
		fragmentSize_(0),
		blockCount_(0),
		freeBlockCount_(0),
		availableBlockCount_(0)
#ifndef _WIN32
		, iNodeCount_(0),
		freeINodeCount_(0),
		availableINodeCount_(0),
		id_(0),
		flags_(0),
		maxFileNameSize_(0)
#endif
{
}

FileSystemStatus::~FileSystemStatus() {
}

bool FileSystemStatus::isNoSUID(void) const {
#if UTIL_HAVE_STATVFS_T
	return 0 != (flags_ & ST_NOSUID);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

bool FileSystemStatus::isReadOnly(void) const {
#if UTIL_HAVE_STATVFS_T
	return 0 != (flags_ & ST_RDONLY);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}


namespace {

#ifdef _WIN32
const char8_t UTIL_FILE_SYSTEM_SEPARATOR = '\\';
#else
const char8_t UTIL_FILE_SYSTEM_SEPARATOR = '/';
#endif

static bool isPathSeparator(char ch) {
#ifdef _WIN32
	return (ch == '\\' || ch == '/');
#else
	return (ch == '/');
#endif
}

static size_t findLastNonSeparatorPos(const u8string &path) {
	for (u8string::const_reverse_iterator it = path.rbegin(); it != path.rend(); ++it) {
		if (!isPathSeparator(*it)) {
			return (path.rend() - it - 1);
		}
	}

	return u8string::npos;
}

static size_t findLastSeparatorPos(const u8string &path) {
	for (u8string::const_reverse_iterator it = path.rbegin(); it != path.rend(); ++it) {
		if (isPathSeparator(*it)) {
			return (path.rend() - it - 1);
		}
	}

	return u8string::npos;
}

}	

bool FileSystem::exists(const char8_t *path) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);

	WIN32_FIND_DATAW findData;
	const HANDLE handle = FindFirstFileW(pathStr.c_str(), &findData);
	if (handle == INVALID_HANDLE_VALUE) {
		return false;
	}

	FindClose(handle);
	return true;
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	return (access(pathStr.c_str(), F_OK) == 0);
#endif
}

bool FileSystem::isDirectory(const char8_t *path) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);

	WIN32_FIND_DATAW findData;
	const HANDLE handle = FindFirstFileW(pathStr.c_str(), &findData);
	if (handle == INVALID_HANDLE_VALUE) {
		return false;
	}

	FindClose(handle);
	return (findData.dwFileAttributes != INVALID_FILE_ATTRIBUTES &&
			(findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0);
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	struct stat stBuf;
	return (::stat(pathStr.c_str(), &stBuf) == 0 && S_ISDIR(stBuf.st_mode));
#endif
}

bool FileSystem::isRegularFile(const char8_t *path) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);

	WIN32_FIND_DATAW findData;
	const HANDLE handle = FindFirstFileW(pathStr.c_str(), &findData);
	if (handle == INVALID_HANDLE_VALUE) {
		return false;
	}

	FindClose(handle);
	return (findData.dwFileAttributes != INVALID_FILE_ATTRIBUTES &&
			(findData.dwFileAttributes == FILE_ATTRIBUTE_NORMAL ||
			findData.dwFileAttributes == FILE_ATTRIBUTE_ARCHIVE));
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	struct stat stBuf;
	return (::stat(pathStr.c_str(), &stBuf) == 0 && S_ISREG(stBuf.st_mode));
#endif
}

void FileSystem::createDirectory(const char8_t *path) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);
	if (!CreateDirectoryW(pathStr.c_str(), NULL)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	if (::mkdir(pathStr.c_str(), S_IREAD | S_IWRITE | S_IEXEC) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void FileSystem::createDirectoryTree(const char8_t *path) {
	if (strcmp(path, "") == 0 || strcmp(path, "/") == 0 ||
			strcmp(path, ".") == 0 || strcmp(path, "..") == 0) {
		return;
	}

	u8string dirName;
	getDirectoryName(path, dirName);
	createDirectoryTree(dirName.c_str());

	if (!exists(path)) {
		createDirectory(path);
	}
}

void FileSystem::createLink(
		const char8_t *sourcePath, const char8_t *targetPath) {
#ifdef _WIN32
	std::wstring sourcePathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(sourcePath, sourcePathStr);
	std::wstring targetPathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(targetPath, targetPathStr);
	if (!CreateHardLinkW(sourcePathStr.c_str(), targetPathStr.c_str(), NULL)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	std::string sourcePathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(sourcePath, sourcePathStr);
	std::string targetPathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(targetPath, targetPathStr);
	if (::link(sourcePathStr.c_str(), targetPathStr.c_str()) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void FileSystem::createPath(
		const char8_t *directoryName, const char8_t *baseName,
		u8string &path) {

	const bool emptyDir =
			(directoryName == NULL || strlen(directoryName) == 0);
	const bool emptyBase = (baseName == NULL || strlen(baseName) == 0);

	path.clear();
	if (!emptyDir) {
		path += directoryName;
	}

	if (!emptyDir && !emptyBase &&
			*(--path.end()) != UTIL_FILE_SYSTEM_SEPARATOR) {
		path += UTIL_FILE_SYSTEM_SEPARATOR;
	}

	if (!emptyBase) {
		path += baseName;
	}
}

void FileSystem::getBaseName(const char8_t *path, u8string &baseName) {

	u8string buf;
	buf.swap(baseName);
	buf = path;

	const size_t tailNoSepPos = findLastNonSeparatorPos(buf);
	if (tailNoSepPos != u8string::npos && tailNoSepPos < buf.size()) {
		buf.erase(tailNoSepPos + 1);
	}

	const size_t sepPos = findLastSeparatorPos(buf);
	if (sepPos != u8string::npos) {
		buf.erase(0, sepPos + 1);
	}

	baseName.swap(buf);
}

void FileSystem::getDirectoryName(
		const char8_t *path, u8string &directoryName) {

	u8string buf;
	buf.swap(directoryName);
	buf = path;

	const size_t tailNoSepPos = findLastNonSeparatorPos(buf);
	if (tailNoSepPos != u8string::npos && tailNoSepPos < buf.size()) {
		buf.erase(tailNoSepPos + 1);
	}

	const size_t sepPos = findLastSeparatorPos(buf);
	if (sepPos == u8string::npos) {
		if (buf != "..") {
			buf = ".";
		}
	}
	else {
		buf.erase(sepPos + 1);

		const size_t noSepPos = findLastNonSeparatorPos(buf);
		if (noSepPos == u8string::npos) {
			buf = UTIL_FILE_SYSTEM_SEPARATOR;
		}
		else {
			buf.erase(noSepPos + 1);
		}
	}

	directoryName.swap(buf);
}

void FileSystem::getFileStatus(const char8_t *path, FileStatus *status) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);
	WIN32_FILE_ATTRIBUTE_DATA data;
	if (!GetFileAttributesExW(pathStr.c_str(), GetFileExInfoStandard, &data)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	FileLib::getFileStatus(data, status);
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	struct stat stBuf;
	if (stat(pathStr.c_str(), &stBuf) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	FileLib::getFileStatus(stBuf, status);
#endif
}

void FileSystem::getFileStatusNoFollow(
		const char8_t *path, FileStatus *status) {
#ifdef _WIN32
	getFileStatus(path, status);
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	struct stat stBuf;
	if (lstat(pathStr.c_str(), &stBuf) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	FileLib::getFileStatus(stBuf, status);
#endif
}

void FileSystem::getRealPath(const char8_t *path, u8string &realPath) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);
	std::vector<wchar_t> realPathWStr;
	realPathWStr.resize(MAX_PATH);
	for (DWORD size;;) {
		size = GetFullPathNameW(
				pathStr.c_str(), static_cast<DWORD>(realPathWStr.size()),
				&realPathWStr[0], NULL);
		if (size == 0) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		else if (size < realPathWStr.size()) {
			break;
		}
		realPathWStr.resize(size + 1);
	}
	CodeConverter(Code::WCHAR_T, Code::UTF8)(&realPathWStr[0], realPath);
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	char realPathChars[PATH_MAX];
	if (::realpath(pathStr.c_str(), realPathChars) == NULL) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	CodeConverter(Code::CHAR, Code::UTF8)(realPathChars, realPath);
#endif
}

void FileSystem::getStatus(const char8_t *path, FileSystemStatus *status) {
	FileLib::getFileSystemStatus(path, status);
}

void FileSystem::move(const char8_t *sourcePath, const char8_t *targetPath) {
#ifdef _WIN32
	std::wstring sourcePathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(sourcePath, sourcePathStr);
	std::wstring targetPathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(targetPath, targetPathStr);
	if (!MoveFileExW(sourcePathStr.c_str(), targetPathStr.c_str(),
			MOVEFILE_REPLACE_EXISTING)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	std::string sourcePathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(sourcePath, sourcePathStr);
	std::string targetPathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(targetPath, targetPathStr);
	if (::rename(sourcePathStr.c_str(), targetPathStr.c_str()) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void FileSystem::remove(const char8_t *path, bool recursive) {
	if (isDirectory(path)) {
		if (recursive) {
			Directory dir(path);
			u8string subPath;
			while (dir.nextEntry(subPath)) {
				if (!dir.isParentOrSelfChecked()) {
					u8string fullPath;
					createPath(path, subPath.c_str(), fullPath);
					remove(fullPath.c_str(), true);
				}
			}
		}
		removeDirectory(path);
	}
	else {
		removeFile(path);
	}
}

void FileSystem::removeDirectory(const char8_t *path) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);
	if (!RemoveDirectoryW(pathStr.c_str())) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	if (::rmdir(pathStr.c_str()) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void FileSystem::removeFile(const char8_t *path) {
#ifdef _WIN32
	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);
	if (!DeleteFileW(pathStr.c_str())) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	if (::unlink(pathStr.c_str()) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void FileSystem::touch(const char8_t *path) {
	NamedFile file;
	file.open(path, FileFlag::TYPE_CREATE |
			FileFlag::TYPE_WRITE_ONLY | FileFlag::TYPE_TRUNCATE);
}

void FileSystem::updateFileTime(const char8_t *path,
		const DateTime *accessTime,
		const DateTime *modifiedTime,
		const DateTime *creationTime) {
#ifdef _WIN32
	FILETIME accessFT, modifiedFT, creationFT;
	FILETIME *accessFTPtr = NULL, *modifiedFTPtr = NULL, *creationFTPtr = NULL;
	if (accessTime != NULL) {
		accessFT = FileLib::getFileTime(accessTime->getUnixTime());
		accessFTPtr = &accessFT;
	}
	if (modifiedTime != NULL) {
		modifiedFT = FileLib::getFileTime(modifiedTime->getUnixTime());
		modifiedFTPtr = &modifiedFT;
	}
	if (creationTime != NULL) {
		creationFT = FileLib::getFileTime(creationTime->getUnixTime());
		creationFTPtr = &creationFT;
	}

	if (isDirectory(path)) {
		std::wstring pathStr;
		CodeConverter(Code::UTF8, Code::WCHAR_T)(path, pathStr);
		const HANDLE handle = CreateFileW(pathStr.c_str(), GENERIC_WRITE,
				FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
				FILE_FLAG_BACKUP_SEMANTICS, NULL);
		if (handle == INVALID_HANDLE_VALUE) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		if (SetFileTime(handle, creationFTPtr, accessFTPtr, modifiedFTPtr) == 0) {
			CloseHandle(handle);
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		CloseHandle(handle);
	}
	else {
		NamedFile file;
		file.open(path, FileFlag::TYPE_WRITE_ONLY);
		if (SetFileTime(file.getHandle(), creationFTPtr, accessFTPtr, modifiedFTPtr) == 0) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
#else
	(void) creationTime;
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	timeval times[2];
	if (accessTime == NULL || modifiedTime == NULL) {
		struct stat stBuf;
		if (::stat(pathStr.c_str(), &stBuf) != 0) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		times[0].tv_sec = static_cast<long>(stBuf.st_atime);
		times[0].tv_usec = 0;
		times[1].tv_sec = static_cast<long>(stBuf.st_mtime);
		times[1].tv_usec = 0;
	}
	if (accessTime != NULL) {
		times[0] = FileLib::getTimeval(accessTime->getUnixTime());
	}
	if (modifiedTime != NULL) {
		times[1] = FileLib::getTimeval(modifiedTime->getUnixTime());
	}
	if (utimes(pathStr.c_str(), times) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

bool FileSystem::getFDLimit(int32_t *cur, int32_t *max) {
#ifdef UTIL_HAVE_SYS_RESOURCE_H
	struct rlimit rbuf;
	if (getrlimit(RLIMIT_NOFILE, &rbuf) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	if (cur != NULL) {
		assert (rbuf.rlim_cur <=
				static_cast<rlim_t>(std::numeric_limits<int32_t>::max()));
		*cur = static_cast<int32_t>(rbuf.rlim_cur);
	}
	if (max != NULL) {
		assert (rbuf.rlim_max <=
				static_cast<rlim_t>(std::numeric_limits<int32_t>::max()));
		*max = static_cast<int32_t>(rbuf.rlim_max);
	}
	return true;
#else
	if (cur != NULL) {
		*cur = 0;
	}
	if (cur != NULL) {
		*max = 0;
	}
	return false;
#endif
}

bool FileSystem::setFDLimit(int32_t cur, int32_t max) {
#ifdef UTIL_HAVE_SYS_RESOURCE_H
	struct rlimit rbuf;
	rbuf.rlim_cur = static_cast<rlim_t>(cur);
	rbuf.rlim_max = static_cast<rlim_t>(max);
	if (setrlimit(RLIMIT_NOFILE, &rbuf) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return true;
#else
	(void) cur;
	(void) max;
	return false;
#endif
}

void FileSystem::syncAll() {
#ifdef _WIN32
#else
	::sync();
#endif
}

PIdFile::PIdFile() : locked_(false) {
};

PIdFile::~PIdFile() {
	try {
		close();
	}
	catch (...) {
	}
};

void PIdFile::open(const char8_t *name) {
	assert(base_.isClosed());

	base_.open(name,
			util::FileFlag::TYPE_READ_WRITE | util::FileFlag::TYPE_CREATE);

	if (!base_.lock()) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	base_.setSize(0);

	try {
		const u8string &str = util::LexicalConverter<u8string>()(
				ProcessUtils::getCurrentProcessId());
		base_.write(str.c_str(), str.size());
	}
	catch (...) {
		try {
			base_.setSize(0);
		}
		catch (...) {
		}
		throw;
	}

	locked_ = true;
}

void PIdFile::close() {
	if (base_.isClosed()) {
		return;
	}
	if (locked_) {
		base_.setSize(0);
		base_.unlink();
		base_.unlock();
	}
	base_.close();
}

} 
