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
#include "util/code.h"
#include "util/os.h"
#ifdef _WIN32
#include <psapi.h>
#endif
#include <cassert>
#include <vector>

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


#if UTIL_DYNAMIC_LOAD_ENABLED
SharedObject::SharedObject() : data_(NULL) {
}

SharedObject::~SharedObject() {
	try {
		close();
	}
	catch (...) {
	}
}

void SharedObject::open(
		const char8_t *soPath, const char8_t *dllPath, int type) {
#ifdef _WIN32
	static_cast<void>(soPath);
	open(dllPath, type);
#else
	static_cast<void>(dllPath);
	open(soPath, type);
#endif
}

void SharedObject::open(const char8_t *path, int type) {
	close();

#ifdef _WIN32
	std::wstring encodedPath;
	CodeConverter(Code::UTF8, Code::WCHAR_T)(path, encodedPath);
	HANDLE handle = LoadLibraryW(encodedPath.c_str());
	if (handle == NULL) {
		UTIL_THROW_PLATFORM_ERROR(path);
	}
	data_ = handle;
#else
	if (type < 0) {
		open(path, RTLD_NOW | RTLD_LOCAL);
		return;
	}
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
	if (data_) {
		FreeLibrary(static_cast<HMODULE>(data_));
		data_ = NULL;
	}
#else
	if (data_) {
		dlclose(data_);
		data_ = NULL;
	}
#endif
}

void* SharedObject::getSymbol(const char8_t *symbol) {
#ifdef _WIN32
	return GetProcAddress(static_cast<HMODULE>(data_), symbol);
#else
	std::string symbolStr;
	CodeConverter(Code::UTF8, Code::CHAR)(symbol, symbolStr);
	return dlsym(data_, symbolStr.c_str());
#endif
}
#endif 



#if UTIL_DYNAMIC_LOAD_ENABLED
void LibraryFunctions::getEntryProviderFunctions(
		const char8_t *name, SharedObject &so,
		const char8_t *entryFuncName, int32_t reqVersion,
		const void *const *&funcList, size_t &funcCount) {
	funcList = NULL;
	funcCount = 0;

	void *entryFunc = so.getSymbol(entryFuncName);
	if (entryFunc == NULL) {
		errorEntryFunctionNotFound(name, entryFuncName);
		return;
	}

	const void *const *list;
	size_t count;
	int32_t libVersion;

	const int32_t code = reinterpret_cast<EntryProviderFunc>(
			entryFunc)(&list, &count, reqVersion, &libVersion);
	checkProviderResult(name, code, &reqVersion, &libVersion);

	funcList = list;
	funcCount = count;
}
#endif 

void LibraryFunctions::getProviderFunctions(
		const char8_t *name, ProviderFunc provider,
		const void *const *&funcList, size_t &funcCount) {
	if (provider == NULL) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "");
	}

	const void *const *list;
	size_t count;

	const int32_t code = provider(&list, &count);
	checkProviderResult(name, code, NULL, NULL);

	funcList = list;
	funcCount = count;
}

int32_t LibraryFunctions::checkVersion(
		const void *const **funcList, size_t *funcCount,
		int32_t reqVersion, int32_t *libVersionOut, int32_t libVersion,
		int32_t minVersion) throw() {
	trySet(libVersionOut, libVersion);

	if (minVersion >= 0 && reqVersion < minVersion) {
		trySetEmpty(funcList);
		trySetEmpty(funcCount);
		return UtilityException::CODE_LIBRARY_UNMATCH;
	}

	return succeed(NULL);
}

int32_t LibraryFunctions::succeed(UtilExceptionTag **ex) throw() {
	 trySetEmpty(ex);
	 return 0;
}

void LibraryFunctions::checkProviderResult(
		const char8_t *name, int32_t code, const int32_t *reqVersion,
		const int32_t *libVersion) {
	if (code == 0) {
		return;
	}

	if (code == UtilityException::CODE_LIBRARY_UNMATCH &&
			reqVersion != NULL && libVersion != NULL) {
		UTIL_THROW_UTIL_ERROR(
				CODE_LIBRARY_UNMATCH,
				"Library unmatched (name=" << filterName(name) <<
				", requestedVersion=" << *reqVersion <<
				", actualVersion=" << *libVersion << ")");
	}
	else {
		UTIL_THROW_UTIL_ERROR(
				CODE_INVALID_STATUS,
				"Unknown library error (name=" << filterName(name) <<
				", code=" << code << ")");
	}
}

void LibraryFunctions::errorNullArgument() {
	UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "Illegal null argument");
}

void LibraryFunctions::errorEntryFunctionNotFound(
		const char8_t *name, const char8_t *entryFuncName) {
	UTIL_THROW_UTIL_ERROR(
			CODE_LIBRARY_UNMATCH,
			"Library entry function not found ("
			"name=" << filterName(name) <<
			", entryFuncion=" << filterName(entryFuncName) << ")");
}

void LibraryFunctions::errorFunctionNotFound(
		const char8_t *name, size_t funcOrdinal) {
	UTIL_THROW_UTIL_ERROR(
			CODE_LIBRARY_UNMATCH,
			"Library function not found ("
			"name=" << filterName(name) <<
			", functionOrdinal=" << funcOrdinal << ")");
}

const char8_t* LibraryFunctions::filterName(const char8_t *name) throw() {
	if (name == NULL) {
		return "";
	}
	return name;
}


LibraryException::LibraryException() throw() :
		funcTable_("LibraryException"),
		obj_(NULL),
		errorCode_(0) {
}

LibraryException::~LibraryException() {
	clear();
}

void LibraryException::clear() throw() {
	if (obj_ != NULL) {
		try {
			funcTable_.resolve<Functions::FUNC_CLOSE>()(obj_);
		}
		catch (...) {
			assert(false);
		}
		obj_ = NULL;
		errorCode_ = 0;
	}
}

UtilExceptionTag* LibraryException::release() throw() {
	UtilExceptionTag *obj = obj_;
	obj_ = NULL;
	errorCode_ = 0;
	return obj;
}

void LibraryException::assign(
		LibraryFunctions::ProviderFunc provider,
		UtilExceptionTag *ex) throw() {
	clear();

	if (provider == NULL) {
		return;
	}

	try {
		funcTable_.assign(provider);
		obj_ = ex;
		errorCode_ = funcTable_.resolve<Functions::FUNC_GET_INTEGER_FIELD>()(
				obj_, Exception::FIELD_ERROR_CODE, 0);
	}
	catch (...) {
	}
}

void LibraryException::assign(const util::Exception &src) throw() {
	clear();

	try {
		funcTable_.assign(getDefaultProvider());
		DefaultException *dest = UTIL_NEW DefaultException();
		dest->base_ = src;
		obj_ = dest;
		errorCode_ = src.getErrorCode();
	}
	catch (...) {
	}
}

int32_t LibraryException::getCode() throw() {
	return errorCode_;
}

void LibraryException::get(Exception &dest) throw() {
	dest = Exception();
	if (obj_ == NULL) {
		return;
	}

	try {
		const size_t depth =
				funcTable_.resolve<Functions::FUNC_GET_DEPTH>()(obj_);
		for (size_t i = 0; i <= depth; i++) {
			const int32_t errorCode = (i == 0 ?
					errorCode_ :
					getIntegerField(Exception::FIELD_ERROR_CODE, i));

			dest.append(Exception(
					Exception::NamedErrorCode(
							errorCode, getStringField(
							Exception::FIELD_ERROR_CODE_NAME, i).c_str()),
					getStringField(Exception::FIELD_MESSAGE, i).c_str(),
					getStringField(Exception::FIELD_FILE_NAME, i).c_str(),
					getStringField(Exception::FIELD_FUNCTION_NAME, i).c_str(),
					getIntegerField(Exception::FIELD_LINE_NUMBER, i),
					NULL,
					getStringField(Exception::FIELD_TYPE_NAME, i).c_str(),
					Exception::STACK_TRACE_TOP,
					Exception::LITERAL_ALL_DUPLICATED));
		}
	}
	catch (...) {
		dest.append(Exception(
				Exception::NamedErrorCode(
						errorCode_)));
	}
}

LibraryFunctions::ProviderFunc LibraryException::getDefaultProvider() throw() {
	return DefaultProvider::provideFunctions;
}

int32_t LibraryException::getIntegerField(
		Exception::FieldType field, size_t depth) {
	return funcTable_.resolve<Functions::FUNC_GET_INTEGER_FIELD>()(
			obj_, field, depth);
}

std::string LibraryException::getStringField(
		Exception::FieldType field, size_t depth) {
	const size_t size = funcTable_.resolve<Functions::FUNC_GET_STRING_FIELD>()(
			obj_, field, depth, NULL, 0);
	std::vector<char8_t> buf(size + 1);

	if (!buf.empty()) {
		funcTable_.resolve<Functions::FUNC_GET_STRING_FIELD>()(
				obj_, field, depth, &buf[0], buf.size());
	}

	return std::string(&buf[0], size);
}


LibraryException::DefaultProvider::FuncTable
LibraryException::DefaultProvider::FUNC_TABLE;

LibraryException::DefaultProvider::Initializer
LibraryException::DefaultProvider::FUNC_TABLE_INITIALIZER(FUNC_TABLE);

int32_t LibraryException::DefaultProvider::provideFunctions(
		const void *const **funcList, size_t *funcCount) throw() {
	return FUNC_TABLE.getFunctionList(funcList, funcCount);
}

void LibraryException::DefaultProvider::close(UtilExceptionTag *ex) throw() {
	delete as(ex);
}

size_t LibraryException::DefaultProvider::getDepth(UtilExceptionTag *ex) throw() {
	if (ex == NULL) {
		return 0;
	}

	return as(ex)->base_.getMaxDepth();
}

int32_t LibraryException::DefaultProvider::getIntegerField(
		UtilExceptionTag *ex, int32_t fieldType, size_t depth) throw() {
	if (ex == NULL) {
		return 0;
	}

	switch (fieldType) {
	case Exception::FIELD_ERROR_CODE:
		return as(ex)->base_.getErrorCode(depth);
	case Exception::FIELD_LINE_NUMBER:
		return as(ex)->base_.getLineNumber(depth);
	default:
		return 0;
	}
}

size_t LibraryException::DefaultProvider::getStringField(
		UtilExceptionTag *ex, int32_t fieldType, size_t depth,
		char8_t *buf, size_t size) throw() {
	if (ex == NULL) {
		return 0;
	}

	util::NormalOStringStream oss;
	oss << as(ex)->base_.getField(
			static_cast<Exception::FieldType>(fieldType), depth);
	const std::string &str = oss.str();
	if (size > 0) {
		const size_t copySize =
				static_cast<size_t>(std::min<uint64_t>(size - 1, str.size()));
		memcpy(buf, str.c_str(), copySize);
		buf[copySize] = '\0';
	}

	return str.size();
}

LibraryException::DefaultException* LibraryException::DefaultProvider::as(
		UtilExceptionTag *ex) throw() {
	return static_cast<DefaultException*>(ex);
}


LibraryException::DefaultProvider::Initializer::Initializer(FuncTable &table) {
	table.set<Functions::FUNC_CLOSE>(&close);
	table.set<Functions::FUNC_GET_DEPTH>(&getDepth);
	table.set<Functions::FUNC_GET_INTEGER_FIELD>(&getIntegerField);
	table.set<Functions::FUNC_GET_STRING_FIELD>(&getStringField);
}


bool LibraryTool::findError(int32_t code, UtilExceptionTag *ex) throw() {
	return (code != 0 || ex != NULL);
}

void LibraryTool::fromLibraryException(
		int32_t code, ProviderFunc provider, UtilExceptionTag *&src,
		Exception &dest) throw() {
	if (src == NULL) {
		dest = UTIL_EXCEPTION_CREATE_DETAIL_TRACE(
				util::UtilityException,
				UTIL_EXCEPTION_UTIL_NAMED_CODE(CODE_INVALID_STATUS), NULL,
				"Library error occurred but detail lost (code=" <<code << ")");
	}
	else {
		LibraryException ex;
		ex.assign(provider, src);
		src = NULL;
		ex.get(dest);
	}
}

int32_t LibraryTool::toLibraryException(
		const Exception &src, UtilExceptionTag **dest) throw() {
	LibraryException ex;
	ex.assign(src);

	const int32_t code = ex.getCode();
	if (dest != NULL) {
		*dest = ex.release();
	}
	return code;
}

} 
