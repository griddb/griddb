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
#ifndef UTIL_SYSTEM_H_
#define UTIL_SYSTEM_H_

#include "util/type.h"
#include <string>
#include <list>

#ifndef UTIL_DYNAMIC_LOAD_ENABLED
#ifdef _WIN32
#define UTIL_DYNAMIC_LOAD_ENABLED 1
#else
#define UTIL_DYNAMIC_LOAD_ENABLED 0
#endif
#endif

#if defined(__GNUC__) && !defined(_WIN32)
#define UTIL_DLL_PUBLIC __attribute__ ((visibility("default")))
#else
#define UTIL_DLL_PUBLIC
#endif

#ifdef _WIN32
#define UTIL_DLL_CALL __stdcall
#else
#define UTIL_DLL_CALL
#endif

struct UtilExceptionTag {};

namespace util {

/*!
    @brief Status of using memory.
*/
class MemoryStatus {
public:

	static MemoryStatus getStatus();

	size_t getPeakUsage() const;
	size_t getLastUsage() const;

private:
	MemoryStatus();
	size_t peakUsage_;
	size_t lastUsage_;
};

class ProcessUtils {
public:
	static uint64_t getCurrentProcessId();

private:
	ProcessUtils();
};

class SharedObject {
#if UTIL_DYNAMIC_LOAD_ENABLED
public:
	void open(const char8_t *soPath, const char8_t *dllPath, int type = -1);

	void open(const char8_t *path, int type = -1);

	void close(void);

	void* getSymbol(const char8_t *symbol);

public:
	SharedObject();
	virtual ~SharedObject();

private:
	SharedObject(const SharedObject&);
	SharedObject& operator=(const SharedObject&);

private:
	void *data_;
#endif 
};


struct LibraryFunctions {
public:


	typedef int32_t (UTIL_DLL_CALL *EntryProviderFunc)(
			const void *const **funcList, size_t *funcCount,
			int32_t reqVersion, int32_t *libVersion);
	typedef LibraryTool::ProviderFunc ProviderFunc;


#if UTIL_DYNAMIC_LOAD_ENABLED
	static void getEntryProviderFunctions(
			const char8_t *name, SharedObject &so,
			const char8_t *entryFuncName, int32_t reqVersion,
			const void *const *&funcList, size_t &funcCount);
#endif 
	static void getProviderFunctions(
			const char8_t *name, ProviderFunc provider,
			const void *const *&funcList, size_t &funcCount);


	static int32_t checkVersion(
			const void *const **funcList, size_t *funcCount,
			int32_t reqVersion, int32_t *libVersionOut, int32_t libVersion,
			int32_t minVersion = -1) throw();

	template<typename T> static T& deref(T *arg);

	template<typename T> static void trySet(T *ptrArg, const T &value);
	template<typename T> static void trySetEmpty(T *ptrArg);

	static int32_t succeed(UtilExceptionTag **ex) throw();


	static void checkProviderResult(
			const char8_t *name, int32_t code, const int32_t *reqVersion,
			const int32_t *libVersion);
	static void errorNullArgument();
	static void errorEntryFunctionNotFound(
			const char8_t *name, const char8_t *entryFuncName);
	static void errorFunctionNotFound(
			const char8_t *name, size_t funcOrdinal);

private:
	static const char8_t* filterName(const char8_t *name) throw();
};

template<typename T, int32_t Ex = 0>
class LibraryFunctionTable {
public:
	template<size_t Size> class Builder;
	template<typename U, size_t N> struct FuncOf {
		typedef typename T::template Traits<N>::Func Type;
	};

	explicit LibraryFunctionTable(const char8_t *tableName = NULL) throw();

	bool isEmpty() const throw();

#if UTIL_DYNAMIC_LOAD_ENABLED
	void assign(
			SharedObject &so, const char8_t *entryFuncName,
			int32_t reqVersion);
#endif 

	void assign(LibraryFunctions::ProviderFunc provider);

	LibraryFunctions::ProviderFunc findExceptionProvider() const throw();

	template<size_t N> typename FuncOf<T, N>::Type resolve() const;
	template<size_t N> typename FuncOf<T, N>::Type find() const throw();

private:
	struct NoExceptionFunctions {
		enum {
			FUNC_EXCEPTION_PROVIDER = -1
		};
	};
	typedef typename Conditional<
			(Ex == 0), T, NoExceptionFunctions>::Type ExceptionProviderSource;

	const char8_t *tableName_;
	const void *const *funcList_;
	size_t funcCount_;
};

template<typename T, int32_t Ex>
template<size_t Size>
class LibraryFunctionTable<T, Ex>::Builder {
public:
	Builder();

	int32_t getFunctionList(
			const void *const **funcList, size_t *funcCount) const throw();

	template<size_t N> void set(typename FuncOf<T, N>::Type func);

private:
	const void *buildingFuncList_[Size];
};

class LibraryException {
public:
	struct Functions;

	LibraryException() throw();
	~LibraryException();

	void clear() throw();
	UtilExceptionTag* release() throw();

	void assign(
			LibraryFunctions::ProviderFunc provider,
			UtilExceptionTag *ex) throw();
	void assign(const util::Exception &src) throw();

	int32_t getCode() throw();
	void get(Exception &dest) throw();

	static LibraryFunctions::ProviderFunc getDefaultProvider() throw();

private:
	typedef LibraryFunctionTable<Functions, -1> FuncTableRef;
	struct DefaultProvider;
	struct DefaultException;

	LibraryException(const LibraryException&);
	LibraryException& operator=(const LibraryException&);

	int32_t getIntegerField(Exception::FieldType field, size_t depth);
	std::string getStringField(Exception::FieldType field, size_t depth);

	FuncTableRef funcTable_;
	UtilExceptionTag *obj_;
	int32_t errorCode_;
};

struct LibraryException::Functions {
	enum {
		FUNC_CLOSE,
		FUNC_GET_DEPTH,
		FUNC_GET_INTEGER_FIELD,
		FUNC_GET_STRING_FIELD,

		END_FUNC
	};

	template<size_t F, int = 0>
	struct Traits {
	};

	template<int C> struct Traits<FUNC_CLOSE, C> {
		typedef void (*Func)(UtilExceptionTag*);
	};
	template<int C> struct Traits<FUNC_GET_DEPTH, C> {
		typedef size_t (*Func)(UtilExceptionTag*);
	};
	template<int C> struct Traits<FUNC_GET_INTEGER_FIELD, C> {
		typedef int32_t (*Func)(UtilExceptionTag*, int32_t, size_t);
	};
	template<int C> struct Traits<FUNC_GET_STRING_FIELD, C> {
		typedef size_t (*Func)(
				UtilExceptionTag*, int32_t, size_t, char8_t*, size_t);
	};
};

struct LibraryException::DefaultProvider {
public:
	static int32_t provideFunctions(
			const void *const **funcList, size_t *funcCount) throw();

	static void close(UtilExceptionTag *ex) throw();
	static size_t getDepth(UtilExceptionTag *ex) throw();
	static int32_t getIntegerField(
			UtilExceptionTag *ex, int32_t fieldType, size_t depth) throw();
	static size_t getStringField(
			UtilExceptionTag *ex, int32_t fieldType, size_t depth,
			char8_t *buf, size_t size) throw();

private:
	typedef FuncTableRef::Builder<Functions::END_FUNC> FuncTable;

	struct Initializer {
		explicit Initializer(FuncTable &table);
	};

	static FuncTable FUNC_TABLE;
	static Initializer FUNC_TABLE_INITIALIZER;

	static DefaultException* as(UtilExceptionTag *ex) throw();
};

struct LibraryException::DefaultException : public UtilExceptionTag {
	util::Exception base_;
};

} 


namespace util {


template<typename T>
T& LibraryFunctions::deref(T *arg) {
	if (arg == NULL) {
		errorNullArgument();
	}
	return *arg;
}

template<typename T>
void LibraryFunctions::trySet(T *ptrArg, const T &value) {
	if (ptrArg == NULL) {
		return;
	}
	*ptrArg = value;
}

template<typename T>
void LibraryFunctions::trySetEmpty(T *ptrArg) {
	if (ptrArg == NULL) {
		return;
	}
	*ptrArg = T();
}


template<typename T, int32_t Ex>
LibraryFunctionTable<T, Ex>::LibraryFunctionTable(
		const char8_t *tableName) throw() :
		tableName_(tableName),
		funcList_(NULL),
		funcCount_(0) {
}

template<typename T, int32_t Ex>
bool LibraryFunctionTable<T, Ex>::isEmpty() const throw() {
	return (funcList_ == NULL);
}

#if UTIL_DYNAMIC_LOAD_ENABLED
template<typename T, int32_t Ex>
void LibraryFunctionTable<T, Ex>::assign(
		SharedObject &so, const char8_t *entryFuncName, int32_t reqVersion) {
	LibraryFunctions::getEntryProviderFunctions(
			tableName_, so, entryFuncName, reqVersion, funcList_, funcCount_);
}
#endif 

template<typename T, int32_t Ex>
void LibraryFunctionTable<T, Ex>::assign(LibraryFunctions::ProviderFunc provider) {
	LibraryFunctions::getProviderFunctions(
			tableName_, provider, funcList_, funcCount_);
}

template<typename T, int32_t Ex>
LibraryFunctions::ProviderFunc
LibraryFunctionTable<T, Ex>::findExceptionProvider() const throw() {
	if (ExceptionProviderSource::FUNC_EXCEPTION_PROVIDER < 0) {
		return NULL;
	}
	return find<ExceptionProviderSource::FUNC_EXCEPTION_PROVIDER>();
}

template<typename T, int32_t Ex>
template<size_t N>
typename LibraryFunctionTable<T, Ex>::template FuncOf<T, N>::Type
LibraryFunctionTable<T, Ex>::resolve() const {
	typename FuncOf<T, N>::Type func = find<N>();
	if (func == NULL) {
		LibraryFunctions::errorFunctionNotFound(tableName_, N);
	}
	return func;
}

template<typename T, int32_t Ex>
template<size_t N>
typename LibraryFunctionTable<T, Ex>::template FuncOf<T, N>::Type
LibraryFunctionTable<T, Ex>::find() const throw() {
	if (N >= funcCount_) {
		return NULL;
	}
	return reinterpret_cast<typename FuncOf<T, N>::Type>(funcList_[N]);
}


template<typename T, int32_t Ex>
template<size_t Size>
LibraryFunctionTable<T, Ex>::Builder<Size>::Builder() {
	std::fill(
			buildingFuncList_, buildingFuncList_ + Size,
			static_cast<void*>(NULL));
}

template<typename T, int32_t Ex>
template<size_t Size>
int32_t LibraryFunctionTable<T, Ex>::Builder<Size>::getFunctionList(
		const void *const **funcList, size_t *funcCount) const throw() {
	LibraryFunctions::trySet<const void *const*>(funcList, buildingFuncList_);
	LibraryFunctions::trySet(funcCount, Size);
	return 0;
}

template<typename T, int32_t Ex>
template<size_t Size>
template<size_t N>
void LibraryFunctionTable<T, Ex>::Builder<Size>::set(
		typename FuncOf<T, N>::Type func) {
	UTIL_STATIC_ASSERT(N < Size);
	buildingFuncList_[N] = reinterpret_cast<void*>(func);
}

} 

#endif
