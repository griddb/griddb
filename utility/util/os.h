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
/*!
	@file
    @brief Definition of Utility of system functions
*/
#ifndef UTIL_OS_H_
#define UTIL_OS_H_

#include "util/type.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <deque>
#include <list>
#include <set>
#include <map>
#include <algorithm>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#ifdef UTIL_HAVE_STDINT_H
#	include <stdint.h>
#endif 
#ifdef UTIL_HAVE_INTTYPE_H
#	include <inttype.h>
#endif 
#ifdef UTIL_HAVE_UNISTD_H
#	include <unistd.h>
#endif 
#include <errno.h>
#include <sys/types.h>

#ifdef UTIL_HAVE_SYS_POLL_H
#	include <sys/poll.h>
#endif 
#ifdef UTIL_HAVE_POLL_H
#	include <poll.h>
#endif 
#ifdef UTIL_HAVE_FCNTL_H
#	include <fcntl.h>
#endif 
#ifdef UTIL_HAVE_IOCTL_H
#	include <ioctl.h>
#endif 
#ifdef UTIL_HAVE_SYS_IOCTL_H
#	include <sys/ioctl.h>
#endif 
#ifdef UTIL_HAVE_DLFCN_H
#	include <dlfcn.h>
#endif 
#ifdef _WIN32
#define NOMINMAX
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0501
#endif
#	include <winsock2.h>
#	include <ws2tcpip.h>
#	include <windows.h>
#	include <io.h>
#else
#	include <sys/socket.h>
#	include <netinet/in.h>
#	include <netinet/tcp.h>
#	include <arpa/inet.h>
#	include <netdb.h>
#endif 
#ifdef UTIL_HAVE_SYS_SELECT_H
#	include <sys/select.h>
#endif 
#ifdef UTIL_HAVE_SYS_UN_H
#	include <sys/un.h>
#endif 
#ifdef UTIL_HAVE_SYS_MMAN_H
#	include <sys/mman.h>
#endif 
#ifdef UTIL_HAVE_SYS_EPOLL_H
#	include <sys/epoll.h>
#endif 
#ifdef UTIL_HAVE_EPOLL_H
#	include <epoll.h>
#endif 
#ifdef UTIL_HAVE_SEMAPHORE_H
#	include <semaphore.h>
#endif 
#ifdef UTIL_HAVE_MQUEUE_H
#	include <mqueue.h>
#endif 
#ifdef UTIL_HAVE_SYS_STAT_H
#	include <sys/stat.h>
#endif 
#ifdef UTIL_HAVE_STAT_H
#	include <stat.h>
#endif 
#ifdef UTIL_HAVE_SYS_STATVFS_H
#	include <sys/statvfs.h>
#endif 
#ifdef UTIL_HAVE_STATVFS_H
#	include <statvfs.h>
#endif 
#include <signal.h>

#ifdef UTIL_HAVE_SYS_TIME_H
#	include <sys/time.h>
#endif 
#ifdef UTIL_HAVE_TIME_H
#	include <time.h>
#endif 
#ifndef UTIL_HAVE_SOCKLEN_T
typedef int socklen_t;
#endif 

#ifdef _WIN32
#	include <io.h>
#endif

namespace util {

class FileStatus;
class FileSystemStatus;

/*!
    @brief Information of shared memory.
*/
struct SharedInstanceInfo {
	void *p;
	int fd;
	size_t size;

	SharedInstanceInfo() :
			p(NULL), fd(-1), size(0) {
	}
};

namespace detail {
class OSLib {
public:
#ifdef _WIN32
	explicit OSLib(LPCWSTR libFileName);
#endif

	~OSLib();

#ifdef _WIN32
	void* findFunctionAddress(LPCSTR funcName);
#endif

private:
	OSLib(const OSLib&);
	OSLib& operator=(const OSLib&);

#ifdef _WIN32
	HMODULE handle_;
#endif
};

class OSFunction {
public:
#ifdef _WIN32
	OSFunction(LPCWSTR libFileName, LPCSTR funcName);
#endif

	~OSFunction();

	void* findAddress();

private:
	OSFunction(const OSFunction&);
	OSFunction& operator=(const OSFunction&);

	OSLib lib_;
	void* address_;
};

inline void* OSFunction::findAddress() {
	return address_;
}

} 

/*!
    @brief Common portable utility for platform-specific functions.
*/
struct FileLib {
public:
#ifdef _WIN32
	typedef void (*GetSystemTimePreciseAsFileTimeFunc)(LPFILETIME);
#endif

#ifdef _WIN32
	static GetSystemTimePreciseAsFileTimeFunc findPreciseSystemTimeFunc();
	static int64_t getUnixTime(const FILETIME &fileTime);
	static FILETIME getFileTime(
			SYSTEMTIME &systemTime, bool asLocalTimeZone, bool dstIgnored);
	static FILETIME getFileTime(int64_t unixTime);
	static SYSTEMTIME getSystemTime(
			const FILETIME &fileTime, bool asLocalTimeZone, bool dstIgnored);
	static TIME_ZONE_INFORMATION getTimeZoneInformation(bool dstIgnored);
#else
	static int64_t getUnixTime(
			tm &time, int32_t milliSecond, bool asLocalTimeZone);
	static int64_t getUnixTime(const timeval &time);
	static int64_t getUnixTime(const timespec &time);
	static int64_t getUnixTime(time_t time);
	static tm getTM(int64_t unixTime, bool asLocalTimeZone);
	static timeval getTimeval(int64_t unixTime);
	static timespec calculateTimeoutSpec(clockid_t clockId, uint32_t timeoutMillis);
#endif

#ifdef _WIN32
	static void getFileStatus(
			const BY_HANDLE_FILE_INFORMATION &info, FileStatus *status);
	static void getFileStatus(
			const WIN32_FILE_ATTRIBUTE_DATA &data, FileStatus *status);
	static void getFileStatus(
			const WIN32_FIND_DATAW &data, FileStatus *status);
#else
	static void getFileStatus(const struct stat &stBuf, FileStatus *status);
#endif

	static void getFileSystemStatus(
			const char8_t *path, FileSystemStatus *status);

#ifndef _WIN32
	static bool createSharedInstance(SharedInstanceInfo &out, size_t size);
	static void destroySharedInstance(SharedInstanceInfo &in);
	static int createUnnamedSharedMemoryTemp(void);
#endif

	template<typename T>
	inline static void setFlag(T &flagSet, T flag, bool value) {
		if (value) {
			flagSet |= flag;
		}
		else {
			flagSet &= ~flag;
		}
	}

	template<typename T>
	inline static bool getFlag(T flags1, T flags2) {
		return (flags1 & flags2) != 0;
	}

#ifdef UTIL_HAVE_FCNTL
	inline static void getFlags(bool &chk, int fd, int flags) {
		int result = fcntl(fd, F_GETFL);
		if (-1 == result) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		chk = (0 != (result & flags));
	}

	inline static void setFlags(bool set, int fd, int flags) {
		int result = fcntl(fd, F_GETFL);
		if (-1 == result) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		if (set) {
			result |= flags;
		} else {
			result &= ~flags;
		}

		if (-1 == fcntl(fd, F_SETFL, result)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}

	inline static void getFDFlags(bool &chk, int fd, int flags) {
		int result = fcntl(fd, F_GETFD);
		if (-1 == result) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		chk = (0 != (result & flags));
	}

	inline static void setFDFlags(bool set, int fd, int flags) {
		int result = fcntl(fd, F_GETFD);
		if (-1 == result) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		if (set) {
			result |= flags;
		} else {
			result &= ~flags;
		}

		if (-1 == fcntl(fd, F_SETFD, result)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
#endif 

private:
#ifdef _WIN32
	static detail::OSFunction preciseSystemTimeFunc_;
#endif
};

#ifdef _WIN32
inline FileLib::GetSystemTimePreciseAsFileTimeFunc
FileLib::findPreciseSystemTimeFunc() {
	return reinterpret_cast<GetSystemTimePreciseAsFileTimeFunc>(
			preciseSystemTimeFunc_.findAddress());
}
#endif

class Mutex;
namespace detail {
struct DirectMemoryUtils {
	static void* mallocDirect(size_t size) UTIL_NOEXCEPT;
	static void freeDirect(void *ptr) UTIL_NOEXCEPT;
};

class DirectMutex {
public:
	DirectMutex() throw();
	~DirectMutex();

	void lock() throw();
	void unlock() throw();

private:
	struct Binder;

	struct LocalData {
#ifdef UTIL_HAVE_POSIX_MUTEX
		pthread_mutex_t mutex_;
#else
		CRITICAL_SECTION cs_;
#endif
	};

	void* data() throw();

	LocalData data_;
};

class DirectMutex::Binder {
public:
	Binder(DirectMutex &src, Mutex &target) throw();
	~Binder();

private:
	Mutex &target_;
};
} 

} 

#endif
