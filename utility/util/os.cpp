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
#include "util/os.h"
#include "util/file.h"
#include "util/code.h"
#include <vector>
#include <cassert>

namespace util {

PlatformException PlatformExceptionBuilder::operator()(
		const char8_t *message,
		const Exception::SourceSymbolChar *fileNameLiteral,
		const Exception::SourceSymbolChar *functionNameLiteral,
		int32_t lineNumber,
		Type type,
		int32_t specialErrorCode) {
	const char8_t *descriptionPtr;
#ifdef _WIN32
	const DWORD lastError = GetLastError();
	const int32_t errorCode = static_cast<int32_t>(lastError);
	const char8_t *name = "GetLastError";

	wchar_t descriptionW[1024];
	u8string descriptionStr;

	if (FormatMessageW(FORMAT_MESSAGE_FROM_SYSTEM, NULL, lastError,
			MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
			descriptionW,
			sizeof(descriptionW) / sizeof(descriptionW[0]), NULL) == 0) {
		descriptionPtr = "";
	}
	else {
		try {
			CodeConverter(Code::WCHAR_T, Code::UTF8)(
					descriptionW, descriptionStr);
			for (u8string::iterator it = descriptionStr.begin();
					it != descriptionStr.end();) {
				if (*it == '\r' || *it == '\n') {
					it = descriptionStr.erase(it);
				}
				else {
					++it;
				}
			}
			descriptionPtr = descriptionStr.c_str();
		}
		catch (...) {
			descriptionPtr = "";
		}
	}
#else
	const int32_t errorCode =
			(type == TYPE_NORMAL ? errno : specialErrorCode);
	const char8_t *name =
			(type == TYPE_NORMAL ? "errno" : "getaddrinfo");

	char8_t description[1024];
	descriptionPtr = (type == TYPE_NORMAL ?
			strerror_r(errorCode, description, sizeof(description)) :
			gai_strerror(errorCode));
#endif

	return PlatformException(errorCode,
			UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
					"Platform error (" <<
					(message == NULL ? "" : message) <<
					(message == NULL ? "" : ", ") <<
					name << "=" << errorCode <<
					", description=" << descriptionPtr << ")"),
			fileNameLiteral, functionNameLiteral, lineNumber,
			NULL, "util::PlatformException",
			Exception::STACK_TRACE_TOP);
}

namespace detail {


#ifdef _WIN32
OSLib::OSLib(LPCWSTR libFileName) :
		handle_(LoadLibraryW(libFileName)) {
}
#endif

OSLib::~OSLib() {
#ifdef _WIN32
	if (handle_ != NULL) {
		FreeLibrary(handle_);
	}
#endif
}

#ifdef _WIN32
void* OSLib::findFunctionAddress(LPCSTR funcName) {
	if (handle_ != NULL) {
		return GetProcAddress(handle_, funcName);
	}

	return NULL;
}
#endif


#ifdef _WIN32
OSFunction::OSFunction(LPCWSTR libFileName, LPCSTR funcName) :
		lib_(libFileName),
		address_(lib_.findFunctionAddress(funcName)) {
}
#endif

OSFunction::~OSFunction() {
}

} 


#ifdef _WIN32
detail::OSFunction FileLib::preciseSystemTimeFunc_(
		L"kernel32.dll", "GetSystemTimePreciseAsFileTime");
#endif

namespace {
const int64_t UNIXTIME_FILETIME_EPOC_DIFF =
		static_cast<int64_t>(10000000) * 60 * 60 * 24 * (365 * 369 + 89);
}

#ifdef _WIN32
int64_t FileLib::getUnixTime(const FILETIME &fileTime) {
	ULARGE_INTEGER value;
	value.LowPart = fileTime.dwLowDateTime;
	value.HighPart = fileTime.dwHighDateTime;
	return (static_cast<int64_t>(value.QuadPart) -
			UNIXTIME_FILETIME_EPOC_DIFF) / 10000;
}

FILETIME FileLib::getFileTime(
		SYSTEMTIME &systemTime, bool asLocalTimeZone, bool dstIgnored) {
	FILETIME fileTime;

	if (asLocalTimeZone) {
		TIME_ZONE_INFORMATION tzInfo = getTimeZoneInformation(dstIgnored);

		SYSTEMTIME utcSystemTime;
		if (!TzSpecificLocalTimeToSystemTime(
				&tzInfo, &systemTime, &utcSystemTime)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		if (!SystemTimeToFileTime(&utcSystemTime, &fileTime)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	else {
		if (!SystemTimeToFileTime(&systemTime, &fileTime)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}

	return fileTime;
}

FILETIME FileLib::getFileTime(int64_t unixTime) {
	if (unixTime < 0) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Negative unix time");
	}
	const int64_t value = (unixTime * 10000 + UNIXTIME_FILETIME_EPOC_DIFF);
	FILETIME fileTime;
	fileTime.dwLowDateTime = static_cast<DWORD>(value);
	fileTime.dwHighDateTime = static_cast<DWORD>(value >> 32);
	return fileTime;
}

SYSTEMTIME FileLib::getSystemTime(
		const FILETIME &fileTime, bool asLocalTimeZone, bool dstIgnored) {
	SYSTEMTIME utcSystemTime;
	if (!FileTimeToSystemTime(&fileTime, &utcSystemTime)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (asLocalTimeZone) {
		TIME_ZONE_INFORMATION tzInfo = getTimeZoneInformation(dstIgnored);

		SYSTEMTIME localSystemTime;
		if (!SystemTimeToTzSpecificLocalTime(
				&tzInfo, &utcSystemTime, &localSystemTime)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		return localSystemTime;
	}
	else {
		return utcSystemTime;
	}
}

TIME_ZONE_INFORMATION FileLib::getTimeZoneInformation(bool dstIgnored) {
	TIME_ZONE_INFORMATION rawTzInfo;
	const DWORD tzType = GetTimeZoneInformation(&rawTzInfo);
	if (tzType == TIME_ZONE_ID_INVALID) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (dstIgnored) {
		TIME_ZONE_INFORMATION tzInfo;
		ZeroMemory(&tzInfo, sizeof(tzInfo));
		tzInfo.Bias = rawTzInfo.Bias;
		tzInfo.StandardBias = rawTzInfo.StandardBias;
		tzInfo.DaylightBias = rawTzInfo.DaylightBias;
		return tzInfo;
	}
	else {
		return rawTzInfo;
	}
}
#else
int64_t FileLib::getUnixTime(
		tm &time, int32_t milliSecond, bool asLocalTimeZone) {
	if (milliSecond < 0 || milliSecond >= 1000) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Millisecond field out of range");
	}

	time_t unixTimeSec;
	if (asLocalTimeZone) {
		unixTimeSec = mktime(&time);
	}
	else {
		unixTimeSec = timegm(&time);
	}

	if (unixTimeSec == static_cast<time_t>(-1)) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Illegal time");
	}
	else {
		return static_cast<int64_t>(unixTimeSec) * 1000 +
				static_cast<int32_t>(milliSecond);
	}
}

int64_t FileLib::getUnixTime(const timeval &time) {
	return static_cast<int64_t>(time.tv_sec) * 1000 +
			static_cast<int64_t>(time.tv_usec) / 1000;
}

int64_t FileLib::getUnixTime(const timespec &time) {
	const int64_t nanosPerMilli = 1000 * 1000;
	return static_cast<int64_t>(time.tv_sec) * 1000 +
			static_cast<int64_t>(time.tv_nsec) / nanosPerMilli;
}

int64_t FileLib::getUnixTime(time_t time) {
	return static_cast<int64_t>(time) * 1000;
}

tm FileLib::getTM(int64_t unixTime, bool asLocalTimeZone) {
	if (unixTime < 0) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Negative unix time");
	}
	time_t unixTimeSec = static_cast<time_t>(unixTime / 1000);
	tm time;
	if (asLocalTimeZone) {
		if (localtime_r(&unixTimeSec, &time) == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	else {
		if (gmtime_r(&unixTimeSec, &time) == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	return time;
}

timeval FileLib::getTimeval(int64_t unixTime) {
	timeval tv;
	tv.tv_sec = static_cast<long>(unixTime / 1000);
	tv.tv_usec = static_cast<long>(unixTime % 1000) * 1000;
	return tv;
}

timespec FileLib::calculateTimeoutSpec(clockid_t clockId, uint32_t timeoutMillis) {
	timespec ts;
	if (0 != clock_gettime(clockId, &ts)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	const long nanosPerSec = 1000 * 1000 * 1000;
	const long nanosPerMilli = 1000 * 1000;

	ts.tv_sec += static_cast<time_t>(timeoutMillis / 1000);
	ts.tv_nsec += static_cast<long>(timeoutMillis % 1000) * nanosPerMilli;

	const long sec = ts.tv_nsec / nanosPerSec;
	assert(0 <= sec && sec <= 1);

	ts.tv_sec += static_cast<time_t>(sec);
	ts.tv_nsec -= sec * nanosPerSec;

	return ts;
}
#endif

#ifdef _WIN32
void FileLib::getFileStatus(
		const BY_HANDLE_FILE_INFORMATION &info, FileStatus *status) {
	status->accessTime_ = getUnixTime(info.ftLastAccessTime);
	status->attributes_ = info.dwFileAttributes;
	status->creationTime_ = getUnixTime(info.ftCreationTime);
	status->hardLinkCount_ = info.nNumberOfLinks;
	status->modificationTime_ = getUnixTime(info.ftLastWriteTime);
	ULARGE_INTEGER size;
	size.HighPart = info.nFileSizeHigh;
	size.LowPart = info.nFileSizeLow;
	status->size_ = size.QuadPart;
}

void FileLib::getFileStatus(
		const WIN32_FILE_ATTRIBUTE_DATA &data, FileStatus *status) {
	status->accessTime_ = getUnixTime(data.ftLastAccessTime);
	status->attributes_ = data.dwFileAttributes;
	status->creationTime_ = getUnixTime(data.ftCreationTime);
	status->hardLinkCount_ = 1;
	status->modificationTime_ = getUnixTime(data.ftLastWriteTime);
	ULARGE_INTEGER size;
	size.HighPart = data.nFileSizeHigh;
	size.LowPart = data.nFileSizeLow;
	status->size_ = size.QuadPart;
}

void FileLib::getFileStatus(
		const WIN32_FIND_DATAW &data, FileStatus *status) {
	status->accessTime_ = getUnixTime(data.ftLastAccessTime);
	status->attributes_ = data.dwFileAttributes;
	status->creationTime_ = getUnixTime(data.ftCreationTime);
	status->hardLinkCount_ = 1;
	status->modificationTime_ = getUnixTime(data.ftLastWriteTime);
	ULARGE_INTEGER size;
	size.HighPart = data.nFileSizeHigh;
	size.LowPart = data.nFileSizeLow;
	status->size_ = size.QuadPart;
}
#else
void FileLib::getFileStatus(const struct stat &stBuf, FileStatus *status) {
	status->accessTime_ = getUnixTime(stBuf.st_atime);
	status->blockCount_ = stBuf.st_blocks;
	status->blockSize_ = stBuf.st_blksize;
	status->changeTime_ = getUnixTime(stBuf.st_ctime);
	status->device_ = stBuf.st_dev;
	status->gid_ = stBuf.st_gid;
	status->hardLinkCount_ = stBuf.st_nlink;
	status->iNode_ = stBuf.st_ino;
	status->mode_ = stBuf.st_mode;
	status->modificationTime_ = getUnixTime(stBuf.st_mtime);
	status->rDevice_ = stBuf.st_rdev;
	status->size_ = stBuf.st_size;
	status->uid_ = stBuf.st_uid;
}
#endif

void FileLib::getFileSystemStatus(
		const char8_t *path, FileSystemStatus *status) {
#ifdef _WIN32
	u8string realPathDir;
	if (FileSystem::exists(path)) {
		u8string realPath;
		FileSystem::getRealPath(path, realPath);

		if (FileSystem::isDirectory(realPath.c_str())) {
			realPathDir = realPath;
		}
		else {
			FileSystem::getDirectoryName(realPath.c_str(), realPathDir);
		}
	}
	else {
		realPathDir = path;
	}

	std::wstring pathStr;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(realPathDir.c_str(), pathStr);

	ULARGE_INTEGER availableBytes;
	ULARGE_INTEGER totalBytes;
	ULARGE_INTEGER freeBytes;
	if (!GetDiskFreeSpaceExW(pathStr.c_str(),
			&availableBytes, &totalBytes, &freeBytes)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	SYSTEM_INFO systemInfo;
	GetSystemInfo(&systemInfo);

	const DWORD blockSize = systemInfo.dwPageSize;
	status->blockSize_ = blockSize;
	status->fragmentSize_ = blockSize;
	status->blockCount_ = totalBytes.QuadPart / blockSize;
	status->freeBlockCount_ = freeBytes.QuadPart / blockSize;
	status->availableBlockCount_ = availableBytes.QuadPart / blockSize;
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);

	struct statvfs stvBuf;
	if (statvfs(pathStr.c_str(), &stvBuf) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	status->blockSize_ = stvBuf.f_bsize;
	status->fragmentSize_ = stvBuf.f_frsize;
	status->blockCount_ = stvBuf.f_blocks;
	status->freeBlockCount_ = stvBuf.f_bfree;
	status->availableBlockCount_ = stvBuf.f_bavail;

	status->iNodeCount_ = stvBuf.f_files;
	status->freeINodeCount_ = stvBuf.f_ffree;
	status->availableINodeCount_ = stvBuf.f_favail;
	status->id_ = stvBuf.f_fsid;
	status->flags_ = stvBuf.f_flag;
	status->maxFileNameSize_ = stvBuf.f_namemax;
#endif
}

#ifndef _WIN32
bool FileLib::createSharedInstance(SharedInstanceInfo &out, size_t objsize) {
	if (objsize == 0) {
		return false;
	}

	int shmfd = -1;
	ssize_t res;
	char tmpbuf[1024];
	void* pmap = MAP_FAILED;

	do {
		memset(tmpbuf, 0x00, sizeof(tmpbuf));

		shmfd = createUnnamedSharedMemoryTemp();
		if (-1 == shmfd) {
			break;
		}

		size_t leftsize = objsize;
		size_t cpsize = 0;
		while (leftsize > 0) {
			cpsize = std::min(leftsize, sizeof(tmpbuf));
			res = write(shmfd, tmpbuf, cpsize);
			if (res == -1) {
				goto ERROR;
		}

			leftsize -= res;
		}

		pmap = mmap(NULL, objsize, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd,
				0);
		if (pmap == MAP_FAILED) {
			break;
		}

		out.p = pmap;
		out.size = objsize;
		out.fd = shmfd;

		return true;
	} while (false);

	ERROR:
	if (pmap != MAP_FAILED) {
		munmap(pmap, objsize);
		pmap = MAP_FAILED;
	}

	if (shmfd != -1) {
		close(shmfd);
		shmfd = -1;
	}

	return false;
}

void FileLib::destroySharedInstance(SharedInstanceInfo &in) {
	munmap(in.p, in.size);
	close(in.fd);

	in.p = NULL;
	in.size = 0;
	in.fd = -1;
}

int FileLib::createUnnamedSharedMemoryTemp(void) {
	int shmfd = -1;
	char tmpfn[1024];
	pid_t pid = getpid();

	do {
		snprintf(tmpfn, sizeof(tmpfn), "/.%d_tmp_%d", pid, rand());

		errno = 0;
		shmfd = shm_open(tmpfn, O_RDWR | O_CREAT | O_EXCL, 0400);
		if (-1 == shmfd) {
			if (errno != EEXIST) {
				break;
			}
		} else {
			shm_unlink(tmpfn);
			break;
		}
	} while (true);

	return shmfd;
}
#endif 

} 
