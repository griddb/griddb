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
	aint64_t with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*!
	@file
	@brief Implementation of V5Utility
*/
#include "utility_v5.h"
#include "util/file.h"

std::string UtilBytes::dump(const void* data, int32_t dataLength) {
	util::NormalOStringStream oss;
	oss << std::hex << std::setw(2) << std::setfill('0');
	const uint8_t* addr = static_cast<const uint8_t*>(data);
	for (int32_t pos = 0; pos < dataLength; ++pos, ++addr) {
		oss << (*addr);
	}
	return oss.str();
}

void UtilFile::getFiles(const char* path, const char* suffix, const char* exclude, std::vector<std::string>& list) {
	util::Directory dir(path);
	u8string realPath;
	const std::string suffixStr(suffix);
	std::string excludeStr;
	if (exclude) {
		excludeStr.assign(exclude);
	}
	for (u8string fileName; dir.nextEntry(fileName);) {
		if (fileName.rfind(suffixStr) != std::string::npos) {
			if (!exclude || (fileName.find(excludeStr) == std::string::npos)) {
				list.push_back(fileName);
			}
		}
	}
	std::sort(list.begin(), list.end(), std::greater<std::string>());
}

void UtilFile::getFileNumbers(
		const char* path, const char* suffix, const char* exclude,
		std::vector<int32_t>& list) {
	if (!util::FileSystem::isDirectory(path)) {
		util::FileSystem::createDirectoryTree(path);
		assert(util::FileSystem::isDirectory(path));
	}
	util::Directory dir(path);
	const std::string suffixStr(suffix);
	std::string excludeStr;
	if (exclude) {
		excludeStr.assign(exclude);
	}
	for (u8string fileName; dir.nextEntry(fileName);) {
		int64_t number = extractNumber(suffixStr, excludeStr, fileName);
		if (number >= 0) {
			list.push_back(static_cast<int32_t>(number));
		}
	}
	std::sort(list.begin(), list.end());
}

int64_t UtilFile::extractNumber(const std::string& suffix, const std::string& exclude, const std::string& str) {
	size_t pos2 = str.rfind(suffix);
	if (pos2 != std::string::npos && pos2 > 0) {
		if (exclude.size() == 0 || str.find(exclude) == std::string::npos) {
			size_t pos1 = str.rfind('_', pos2 - 1);
			if (pos1 != std::string::npos) {
				assert(pos1 < pos2);
				size_t len = pos2 - pos1 - 1;
				int64_t val = util::LexicalConverter<int64_t>()(str.substr(pos1 + 1, len));
				return val;
			}
		}
	}
	return LLONG_MIN;
}

void UtilFile::createDir(const char* path) {
	if (util::FileSystem::exists(path)) {
		if (!util::FileSystem::isDirectory(path)) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_IO_ERROR, "Create directory failed. path=\""
				<< path << "\" is not directory");
		}
		return;
	}
	else {
		try {
			util::FileSystem::createDirectoryTree(path);
		}
		catch (std::exception &e) {
			GS_RETHROW_SYSTEM_ERROR(
				e, "Create data directory failed. (path=\""
				<< path << "\""
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
}

void UtilFile::removeDir(const char* path) {
	if (!util::FileSystem::exists(path)) {
		return;
	}
	if (util::FileSystem::isDirectory(path)) {
		util::Directory dir(path);
		for (u8string fileName; dir.nextEntry(fileName);) {
			util::FileSystem::remove(fileName.c_str(), false);
		}
		util::FileSystem::remove(path, false);
	}
}

void UtilFile::createFile(const char* path) {
	if (util::FileSystem::exists(path)) {
		return;
	}
	std::unique_ptr<util::NamedFile> file(new util::NamedFile());
	assert(file);
	file->open(path, util::FileFlag::TYPE_READ_WRITE);
	if (!util::FileSystem::exists(path)) {
		GS_THROW_SYSTEM_ERROR(
			GS_ERROR_CM_IO_ERROR, "Create file failed. path=\""
			<< path << "\"");
	}
}

void UtilFile::removeFile(const char* path) {
	if (!util::FileSystem::exists(path)) {
		return;
	}
	util::FileSystem::remove(path, false);
}

void UtilFile::dump(const char* path) {
	util::Directory dir(path);
	u8string realPath;
	for (u8string fileName; dir.nextEntry(fileName);) {
		util::FileSystem::getRealPath(path, realPath);
		bool nodump = false;
		if (util::FileSystem::isDirectory(realPath.c_str())) {
			util::Directory dir2(realPath.c_str());
			std::vector<std::string> dirList;
			for (u8string fileName2; dir2.nextEntry(fileName2);) {
				dirList.push_back(fileName2);
			}
			std::sort(dirList.begin(), dirList.end());
			for(auto& subpath : dirList) {
				util::NormalOStringStream oss;
				oss << realPath.c_str() << "/" << subpath.c_str();
				dump(oss.str().c_str());
				nodump = true;
			}
		} else if (util::FileSystem::isRegularFile(realPath.c_str())) {
		}
		if (!nodump) {
			std::cout << path << std::endl;
		}
	}
}

#ifndef WIN32 
#include <iostream>
#include <execinfo.h>
#include <unistd.h>
#endif
void UtilDebug::dumpBackTrace() {
#ifndef WIN32
	int nptrs;
	const int FRAME_DEPTH = 50;
	void *buffer[FRAME_DEPTH];
	nptrs = backtrace(buffer, FRAME_DEPTH);
	backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO);
#endif
}

void UtilDebug::dumpMemory(const void* target, size_t dumpSize) {
	int64_t count = 0;

	const uint8_t* data = static_cast<const uint8_t*>(target);
	std::cerr << "addr=0x" << std::setw(8) << std::setfill('0') << std::hex
		  << std::nouppercase << (uintptr_t)data << std::endl;
	const uint8_t* startAddr = data;
	const uint8_t* addr = startAddr;
	for (; addr < startAddr + dumpSize; ++addr) {
		if (count % 16 == 0) {
			std::cerr << std::setw(8) << std::setfill('0') << std::hex
			  << std::nouppercase << (uintptr_t)(addr - startAddr) << " ";
		}
		std::cerr << std::setw(2) << std::setfill('0') << std::hex
		  << std::nouppercase << (uint32_t)(*addr) << " ";

		if (count % 16 == 15) {
			std::cerr << std::endl;
		}
		++count;
	}
	std::cerr << std::dec << std::endl;
}

