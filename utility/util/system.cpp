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
#include "util/system.h"
#include "util/os.h"
#ifdef _WIN32
#include <psapi.h>
#endif

namespace util {

MemoryStatus MemoryStatus::getStatus() {
	return MemoryStatus();
}

size_t MemoryStatus::getPeakUsage() const {
	return peakUsage_;
}

size_t MemoryStatus::getLastUsage() const {
	return lastUsage_;
}

MemoryStatus::MemoryStatus() : peakUsage_(0), lastUsage_(0) {
#ifdef _WIN32
	HANDLE handle = GetCurrentProcess();
	PROCESS_MEMORY_COUNTERS memInfo;

	memInfo.PeakPagefileUsage = 0;
	memInfo.PagefileUsage = 0;
	BOOL result = GetProcessMemoryInfo(
		handle, &memInfo, sizeof(memInfo) );
	CloseHandle(handle);

	if (!result) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	peakUsage_ = memInfo.PeakWorkingSetSize;
	lastUsage_ = memInfo.WorkingSetSize;
#else
	char8_t path[1024];
	{
		const int ret = snprintf(path, sizeof(path), "/proc/%d/status", getpid());
		if (ret < 0 || static_cast<size_t>(ret) >= sizeof(path)) {
			UTIL_THROW_UTIL_ERROR(CODE_INVALID_STATUS,
					"Failed to format proc status path");
		}
	}

	FILE *const fp = fopen(path, "r");
	if (fp == NULL) {
		UTIL_THROW_PLATFORM_ERROR("Failed to open proc status");
	}

	try {
		const char8_t *const nameList[] = { "VmHWM:", "VmRSS:" };
		const size_t entryCount = sizeof(nameList) / sizeof(*nameList);

		uint64_t valueList[entryCount];
		std::fill(valueList, valueList + entryCount, 0);

		bool foundList[entryCount];
		bool *const foundListEnd = foundList + entryCount;
		std::fill(foundList, foundListEnd, false);

		char8_t lineBuf[1024];
		char8_t valueBuf[sizeof(lineBuf)];
		for (;;) {
			const char8_t *const line =
					fgets(lineBuf, static_cast<int>(sizeof(lineBuf)), fp);
			if (line == NULL) {
				break;
			}

			for (size_t i = 0; i < entryCount; i++) {
				if (strstr(line, nameList[i]) != line) {
					continue;
				}

				const char8_t *p = line + strlen(nameList[i]);
				while (*p == ' ' || *p == '\t') {
					p++;
				}

				const char8_t *const valueStart = p;
				while (*p != ' ' && *p != '\t' && *p != '\0') {
					p++;
				}

				const size_t valueStrLen = static_cast<size_t>(p - valueStart);
				memcpy(valueBuf, valueStart, valueStrLen + 1);
				valueBuf[valueStrLen] = '\0';

				errno = 0;
				valueList[i] = strtoul(valueBuf, NULL, 10);
				if (errno != 0) {
					continue;
				}
				foundList[i] = true;

				if (std::find(foundList, foundListEnd, false) == foundListEnd) {
					break;
				}

				break;
			}
		}

		if (std::find(foundList, foundListEnd, false) != foundListEnd) {
			UTIL_THROW_UTIL_ERROR(CODE_INVALID_STATUS,
					"Failed to extract status");
		}

		peakUsage_ = valueList[0] * 1024;
		lastUsage_ = valueList[1] * 1024;
	}
	catch (...) {
		fclose(fp);
		throw;
	}
	fclose(fp);
#endif
}

uint64_t ProcessUtils::getCurrentProcessId() {
#ifdef _WIN32
	return GetCurrentProcessId();
#else
	return static_cast<uint64_t>(getpid());
#endif
}

#if UTIL_MINOR_MODULE_ENABLED

SharedObject::SharedObject() {
}

SharedObject::~SharedObject() {
}

void SharedObject::open(const char8_t *path, int type) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	std::string pathStr;
	CodeConverter(Code::UTF8, Code::CHAR)(path, pathStr);
	void *handle = dlopen(pathStr.c_str(), type);
	if (!handle) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	data_ = handle;
#endif
}

void SharedObject::close(void) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (data_) {
		dlclose(data_);
		data_ = NULL;
	}
#endif
}

void* SharedObject::getSymbol(const char8_t *symbol) {
#ifdef _WIN32
	UTIL_THROW_PLATFORM_ERROR(NULL);
#else
	std::string symbolStr;
	CodeConverter(Code::UTF8, Code::CHAR)(symbol, symbolStr);
	return dlsym(data_, symbolStr.c_str());
#endif
}

char* System::getCurrentDirectory(char *buf, size_t blen) {
#ifndef _WIN32
	return getcwd(buf,blen);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

std::string& System::getCurrentDirectory(std::string &out) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	out.clear();

	char *buf = NULL;
	size_t blen = 1024;
	int eno = 0;

	do {
		if (NULL == (buf = (char*) malloc(blen))) {
			break;
		}

		errno = 0;
		if (NULL != getcwd(buf, blen)) {
			buf[blen - 1] = 0x00;
			out = buf;
			free(buf);
			break;
		}
		eno = errno;

		free(buf);

		if (eno != ERANGE) {
			break;
		}

		blen *= 2;

	} while (true);

	return out;
#endif
}

void System::getLibraryVersion(int *major, int *minor, int *patch,
		int *rel) {
	if (major) {
		*major = 0;
	}

	if (minor) {
		*minor = 0;
	}

	if (patch) {
		*patch = 0;
	}

	if (rel) {
		*rel = 0;
	}
}

size_t System::getCPUCount(void) {
#if defined(_SC_NPROCESSORS_CONF)
	const long result = sysconf(_SC_NPROCESSORS_CONF);
	if (result == -1) {
		UTIL_THROW_NOIMPL_UTIL();
	}
	return static_cast<size_t>(result);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

size_t System::getOnlineCPUCount(void) {
#if defined(_SC_NPROCESSORS_ONLN)
	const long result = sysconf(_SC_NPROCESSORS_ONLN);
	if (result == -1) {
		UTIL_THROW_NOIMPL_UTIL();
	}
	return static_cast<size_t>(result);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

enum {
	UTIL_USER_DEFAULT_BUFFER_SIZE = (1024 * 2),
	UTIL_GROUP_DEFAULT_BUFFER_SIZE = (1024 * 4),
};

User::User() :
		uid_(0), gid_(0) {
}

User::~User() {
}

void User::getUser(uid_t uid) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	struct passwd pwd;
	struct passwd *rpwd;
	char buf[UTIL_USER_DEFAULT_BUFFER_SIZE];
	if (0 != getpwuid_r(uid, &pwd, buf, sizeof(buf), &rpwd)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	id_ = pwd.pw_name;
	password_ = pwd.pw_passwd;
	uid_ = pwd.pw_uid;
	gid_ = pwd.pw_gid;
	name_ = pwd.pw_gecos;
	homedir_ = pwd.pw_dir;
	shell_ = pwd.pw_shell;
#endif
}

void User::getUser(const char *id) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	struct passwd pwd;
	struct passwd *rpwd;
	char buf[UTIL_USER_DEFAULT_BUFFER_SIZE];
	if (0 != getpwnam_r(id, &pwd, buf, sizeof(buf), &rpwd)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	id_ = pwd.pw_name;
	password_ = pwd.pw_passwd;
	uid_ = pwd.pw_uid;
	gid_ = pwd.pw_gid;
	name_ = pwd.pw_gecos;
	homedir_ = pwd.pw_dir;
	shell_ = pwd.pw_shell;
#endif
}

Group::Group() :
		gid_(0) {
}

Group::~Group() {
}

void Group::getGroup(gid_t gid) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	struct group grp;
	struct group *rgrp;
	char buf[UTIL_GROUP_DEFAULT_BUFFER_SIZE];
	if (0 != getgrgid_r(gid, &grp, buf, sizeof(buf), &rgrp)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	id_ = grp.gr_name;
	password_ = grp.gr_passwd;
	gid_ = grp.gr_gid;
	users_.clear();
	char **ptr = grp.gr_mem;
	if (NULL != ptr) {
		while (NULL != *ptr) {
			users_.push_back(*ptr);
			++ptr;
		}
	}
#endif
}

void Group::getGroup(const char *id) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	struct group grp;
	struct group *rgrp;
	char buf[UTIL_GROUP_DEFAULT_BUFFER_SIZE];
	if (0 != getgrnam_r(id, &grp, buf, sizeof(buf), &rgrp)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	id_ = grp.gr_name;
	password_ = grp.gr_passwd;
	gid_ = grp.gr_gid;
	users_.clear();
	char** ptr(grp.gr_mem);
	if (NULL != ptr) {
		while (NULL != *ptr) {
			users_.push_back(*ptr);
			++ptr;
		}
	}
#endif
}

#endif 

} 
