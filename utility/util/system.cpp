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


} 
