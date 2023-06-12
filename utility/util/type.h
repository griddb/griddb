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
    @brief Definition of Utility of common configuration, data types, and exceptions
*/
#ifndef UTIL_TYPE_H_
#define UTIL_TYPE_H_


#ifndef UTIL_MINOR_MODULE_ENABLED
#define UTIL_MINOR_MODULE_ENABLED 0
#endif

#ifndef UTIL_FAILURE_SIMULATION_ENABLED
#define UTIL_FAILURE_SIMULATION_ENABLED 0
#endif

#ifndef UTIL_MEMORY_POOL_AGGRESSIVE
#define UTIL_MEMORY_POOL_AGGRESSIVE 0
#endif

#ifndef UTIL_MEMORY_POOL_PLACEMENT_NEW
#define UTIL_MEMORY_POOL_PLACEMENT_NEW 0
#endif

#ifndef UTIL_MEMORY_ALLOCATE_INITIALIZE
#define UTIL_MEMORY_ALLOCATE_INITIALIZE 0
#endif


#define UTIL_STACK_TRACE_ENABLED

#if UTIL_FAILURE_SIMULATION_ENABLED || \
	UTIL_MEMORY_POOL_PLACEMENT_NEW || \
	UTIL_MEMORY_ALLOCATE_INITIALIZE || \
	defined(UTIL_DUMP_OPERATOR_DELETE)
#define UTIL_PLACEMENT_NEW_ENABLED
#endif

#if (defined(_MSC_VER) && defined(_M_X64)) || \
	(defined(__GNUC__) && defined(__x86_64__))
#define UTIL_POINTER_SIZE 8
#else
#define UTIL_POINTER_SIZE 4
#endif

#if __cplusplus >= 201103L
#define UTIL_CXX11_SUPPORTED_EXACT 1
#else
#define UTIL_CXX11_SUPPORTED_EXACT 0
#endif

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || UTIL_CXX11_SUPPORTED_EXACT
#define UTIL_CXX11_SUPPORTED 1
#else
#define UTIL_CXX11_SUPPORTED 0
#endif

#if __cplusplus >= 201402L
#define UTIL_CXX14_SUPPORTED 1
#else
#define UTIL_CXX14_SUPPORTED 0
#endif

#if __cplusplus >= 201703L
#define UTIL_CXX17_SUPPORTED 1
#else
#define UTIL_CXX17_SUPPORTED 0
#endif

#if defined(_MSC_VER) && defined(_M_X64) && !defined(_WIN64)
#error 0
#endif

#if UTIL_CXX14_SUPPORTED && defined(__has_cpp_attribute)
#define UTIL_HAS_FEATURE_MACRO 1
#else
#define UTIL_HAS_FEATURE_MACRO 0
#endif

#if UTIL_CXX11_SUPPORTED_EXACT
#define UTIL_HAS_ATTRIBUTE_NORETURN 1
#elif defined(__GNUC__) && \
		(__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8))
#define UTIL_HAS_ATTRIBUTE_NORETURN 1
#else
#define UTIL_HAS_ATTRIBUTE_NORETURN 0
#endif

#if UTIL_HAS_FEATURE_MACRO
#if __has_cpp_attribute(maybe_unused) >= 201603L
#define UTIL_HAS_ATTRIBUTE_MAYBE_UNUSED 1
#else
#define UTIL_HAS_ATTRIBUTE_MAYBE_UNUSED 0
#endif
#else
#define UTIL_HAS_ATTRIBUTE_MAYBE_UNUSED 0
#endif

#if UTIL_HAS_FEATURE_MACRO
#if __has_cpp_attribute(nodiscard) >= 201603L
#define UTIL_HAS_ATTRIBUTE_NODISCARD 1
#else
#define UTIL_HAS_ATTRIBUTE_NODISCARD 0
#endif
#else
#define UTIL_HAS_ATTRIBUTE_NODISCARD 0
#endif

#ifndef UTIL_HAS_TEMPLATE_PLACEMENT_NEW
#define UTIL_HAS_TEMPLATE_PLACEMENT_NEW 0
#endif

#ifndef UTIL_CPU_BUILD_AVX2
#if defined(__GNUC__) && \
		(__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6))
#define UTIL_CPU_BUILD_AVX2 1
#else
#define UTIL_CPU_BUILD_AVX2 0
#endif
#endif

#if UTIL_CPU_BUILD_AVX2
#define UTIL_CPU_SUPPORTS_AVX2() (__builtin_cpu_supports("avx2") != 0)
#else
#define UTIL_CPU_SUPPORTS_AVX2() (false)
#endif

#ifndef UTIL_EXT_LIBRARY_ENABLED
#define UTIL_EXT_LIBRARY_ENABLED 0
#endif

#ifdef _WIN32
/* #undef UTIL_HAVE_BLKCNT_T */
/* #undef UTIL_HAVE_BLKSIZE_T */
/* #undef UTIL_HAVE_CLOCKID_T */
/* #undef UTIL_HAVE_DEV_T */
/* #undef UTIL_HAVE_DIRENT_H */
/* #undef UTIL_HAVE_DLFCN_H */
/* #undef UTIL_HAVE_EPOLL_CTL */
/* #undef UTIL_HAVE_FCNTL */
/* #undef UTIL_HAVE_FCNTL_H */
/* #undef UTIL_HAVE_FSBLKCNT_T */
/* #undef UTIL_HAVE_FSTAT */
/* #undef UTIL_HAVE_FSTATVFS */
#define UTIL_HAVE_GETSYSTEMTIMEASFILETIME 1
/* #undef UTIL_HAVE_GETTIMEOFDAY */
/* #undef UTIL_HAVE_GID_T */
/* #undef UTIL_HAVE_INO_T */
#define UTIL_HAVE_INTPTR_T	UTIL_POINTER_SIZE
/* #undef UTIL_HAVE_INTTYPE_H */
/* #undef UTIL_HAVE_IOCTL */
/* #undef UTIL_HAVE_IOCTLSOCKET */
/* #undef UTIL_HAVE_IOCTL_H */
/* #undef UTIL_HAVE_LONG_LONG_T */
/* #undef UTIL_HAVE_LSTAT */
/* #undef UTIL_HAVE_MODE_T */
/* #undef UTIL_HAVE_MQUEUE_H */
/* #undef UTIL_HAVE_NLINK_T */
/* #undef UTIL_HAVE_OFF_T */
/* #undef UTIL_HAVE_OPENDIR */
/* #undef UTIL_HAVE_POINTER_T */
/* #undef UTIL_HAVE_POLL */
/* #undef UTIL_HAVE_POLL_H */
/* #undef UTIL_HAVE_POSIX_BARRIER */
/* #undef UTIL_HAVE_POSIX_CONDITION */
/* #undef UTIL_HAVE_POSIX_MUTEX */
/* #undef UTIL_HAVE_POSIX_RWLOCK */
/* #undef UTIL_HAVE_POSIX_SPIN */
/* #undef UTIL_HAVE_POSIX_SPIN_TIMEDLOCK */
/* #undef UTIL_HAVE_PTHREAD_CONDATTR_GETCLOCK */
/* #undef UTIL_HAVE_PTHREAD_CONDATTR_SETCLOCK */
/* #undef UTIL_HAVE_RESTRICT */
/* #undef UTIL_HAVE_RESTRICT_CPP */
/* #undef UTIL_HAVE_SEMAPHORE_H */
/* #undef UTIL_HAVE_SIGINFO_T */
/* #undef UTIL_HAVE_SIGNAL_H */
/* #undef UTIL_HAVE_SIGSET_T */
#define UTIL_HAVE_SIZE_T	UTIL_POINTER_SIZE
/* #undef UTIL_HAVE_SOCKADDR_STORAGE_T */
/* #undef UTIL_HAVE_SOCKADDR_UN_T */
#define UTIL_HAVE_SOCKLEN_T	1
/* #undef UTIL_HAVE_SSIZE_T */
/* #undef UTIL_HAVE_STAT */
/* #undef UTIL_HAVE_STATVFS */
/* #undef UTIL_HAVE_STATVFS_H */
/* #undef UTIL_HAVE_STATVFS_T */
/* #undef UTIL_HAVE_STAT_H */
/* #undef UTIL_HAVE_STAT_T */
#define UTIL_HAVE_STDDEF_H	1
/* #undef UTIL_HAVE_STDINT_H */
/* #undef UTIL_HAVE_STRCASECMP */
#define UTIL_HAVE_STRICMP	1
/* #undef UTIL_HAVE_SYS_DIRENT_H */
/* #undef UTIL_HAVE_SYS_EPOLL_H */
/* #undef UTIL_HAVE_SYS_IOCTL_H */
/* #undef UTIL_HAVE_SYS_MMAN_H */
/* #undef UTIL_HAVE_SYS_POLL_H */
/* #undef UTIL_HAVE_SYS_RESOURCE_H */
/* #undef UTIL_HAVE_SYS_SELECT_H */
/* #undef UTIL_HAVE_SYS_SIGNAL_H */
/* #undef UTIL_HAVE_SYS_STATVFS_H */
/* #undef UTIL_HAVE_SYS_STAT_H */
/* #undef UTIL_HAVE_SYS_TIME_H */
/* #undef UTIL_HAVE_SYS_UN_H */
#define UTIL_HAVE_TIME_H		1
#define UTIL_HAVE_TIMEVAL_T	1
/* #undef UTIL_HAVE_UID_T */
/* #undef UTIL_HAVE_UINT8_T */
/* #undef UTIL_HAVE_UNISTD_H */
#define UTIL_HAVE_VSNPRINTF	1
/* #undef UTIL_HAVE___BLKSIZE_T */
/* #undef UTIL_HAVE___INT64_T */
/* #undef UTIL_HAVE_MEM_STAT_ST_BLKSIZE */
/* #undef UTIL_HAVE_MEM_STAT_ST_BLOCKS */
#else
#define UTIL_HAVE_BLKCNT_T	4
/* #undef UTIL_HAVE_BLKSIZE_T */
#define UTIL_HAVE_CLOCKID_T	4
#define UTIL_HAVE_DEV_T	8
#define UTIL_HAVE_DIRENT_H	1
#define UTIL_HAVE_DLFCN_H	1
#define UTIL_HAVE_EPOLL_CTL	1
#define UTIL_HAVE_FCNTL	1
#define UTIL_HAVE_FCNTL_H	1
#define UTIL_HAVE_FSBLKCNT_T 4
#define UTIL_HAVE_FSTAT	1
#define UTIL_HAVE_FSTATVFS	1
/* #undef UTIL_HAVE_GETSYSTEMTIMEASFILETIME */
#define UTIL_HAVE_GETTIMEOFDAY 1
#define UTIL_HAVE_GID_T	4
#define UTIL_HAVE_INO_T	4
#define UTIL_HAVE_INTPTR_T	4
/* #undef UTIL_HAVE_INTTYPE_H */
#define UTIL_HAVE_IOCTL	1
/* #undef UTIL_HAVE_IOCTLSOCKET */
/* #undef UTIL_HAVE_IOCTL_H */
#define UTIL_HAVE_LONG_LONG_T	8
#define UTIL_HAVE_LSTAT	1
#define UTIL_HAVE_MODE_T	4
#define UTIL_HAVE_MQUEUE_H	1
#define UTIL_HAVE_NLINK_T	4
#define UTIL_HAVE_OFF_T	4
#define UTIL_HAVE_OPENDIR	1
#define UTIL_HAVE_POINTER_T	UTIL_POINTER_SIZE
#define UTIL_HAVE_POLL	1
#define UTIL_HAVE_POLL_H	1
#define UTIL_HAVE_POSIX_BARRIER	1
#define UTIL_HAVE_POSIX_CONDITION	1
#define UTIL_HAVE_POSIX_MUTEX	1
#define UTIL_HAVE_POSIX_RWLOCK	1
#define UTIL_HAVE_POSIX_SPIN	1
/* #undef UTIL_HAVE_POSIX_SPIN_TIMEDLOCK */
#define UTIL_HAVE_PTHREAD_CONDATTR_GETCLOCK	1
#define UTIL_HAVE_PTHREAD_CONDATTR_SETCLOCK	1
/* #undef UTIL_HAVE_RESTRICT */
#define UTIL_HAVE_RESTRICT_CPP	4
#define UTIL_HAVE_SEMAPHORE_H	1
#define UTIL_HAVE_SIGINFO_T	128
#define UTIL_HAVE_SIGNAL_H	1
#define UTIL_HAVE_SIGSET_T	128
#define UTIL_HAVE_SIZE_T	UTIL_POINTER_SIZE
#define UTIL_HAVE_SOCKADDR_STORAGE_T	128
#define UTIL_HAVE_SOCKADDR_UN_T	110
#define UTIL_HAVE_SOCKLEN_T	4
#define UTIL_HAVE_SSIZE_T	UTIL_POINTER_SIZE
#define UTIL_HAVE_STAT	1
#define UTIL_HAVE_STATVFS	1
/* #undef UTIL_HAVE_STATVFS_H */
#define UTIL_HAVE_STATVFS_T	72
/* #undef UTIL_HAVE_STAT_H */
#define UTIL_HAVE_STAT_T	88
#define UTIL_HAVE_STDDEF_H	1
#define UTIL_HAVE_STDINT_H	1
#define UTIL_HAVE_STRCASECMP	1
/* #undef UTIL_HAVE_STRICMP */
/* #undef UTIL_HAVE_SYS_DIRENT_H */
#define UTIL_HAVE_SYS_EPOLL_H	1
#define UTIL_HAVE_SYS_IOCTL_H	1
#define UTIL_HAVE_SYS_MMAN_H	1
#define UTIL_HAVE_SYS_POLL_H	1
#define UTIL_HAVE_SYS_RESOURCE_H	1
#define UTIL_HAVE_SYS_SELECT_H	1
#define UTIL_HAVE_SYS_SIGNAL_H	1
#define UTIL_HAVE_SYS_STATVFS_H	1
#define UTIL_HAVE_SYS_STAT_H	1
#define UTIL_HAVE_SYS_TIME_H	1
#define UTIL_HAVE_SYS_UN_H	1
#define UTIL_HAVE_TIME_H		1
#define UTIL_HAVE_TIMEVAL_T	8
#define UTIL_HAVE_UID_T	4
#define UTIL_HAVE_UINT8_T	1
#define UTIL_HAVE_UNISTD_H	1
#define UTIL_HAVE_VSNPRINTF	1
#define UTIL_HAVE___BLKSIZE_T	4
/* #undef UTIL_HAVE___INT64_T */
#define UTIL_HAVE_MEM_STAT_ST_BLKSIZE	1
#define UTIL_HAVE_MEM_STAT_ST_BLOCKS	1
#endif 

#ifndef _WIN32
#ifdef __off_t_defined
#error "This header must be included before off_t declaration"
#endif
#define _FILE_OFFSET_BITS 64
#define LARGEFILE_SOURCE
#define LARGE_FILES
#endif

#define __STDC_CONSTANT_MACROS
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#if UTIL_CXX11_SUPPORTED
#include <cstdint>
#else
#ifdef _MSC_STDINT_H_
#error "Do not include stdint.h before this header is included"
#endif
#if defined(_MSC_VER) && (_MSC_VER <= 1500)
#include "util/stdint/stdint.h"
#else
#include <stdint.h>
#endif 
#endif 

#ifndef UINT64_MAX
#error "Integer limit macros are not defined"
#endif

#ifndef UINT64_C
#error "Integer constant macros are not defined"
#endif

#include <exception>
#include <sstream>

#ifndef _WIN32
#include <sys/types.h>
#endif

#ifdef UTIL_HAVE_STDDEF_H
#include <stddef.h>
#endif

#if UTIL_CXX11_SUPPORTED_EXACT
#include <type_traits>
#endif

#ifndef UTIL_HAVE_INTPTR_T
#	if ( UTIL_HAVE_POINTER_T == 4 )
typedef int32_t intptr_t;
#	elif ( UTIL_HAVE_POINTER_T == 8 )
typedef int64_t intptr_t;
#	else
#		error "Unknown pointer size"
#	endif
#endif

#ifndef UTIL_HAVE_SSIZE_T
#	if ( UTIL_HAVE_SIZE_T == 4 )
typedef int32_t ssize_t;
#	elif ( UTIL_HAVE_SIZE_T == 8 )
typedef int64_t ssize_t;
#	else
#		error "Unknown size_t size"
#	endif
#endif 


#ifndef UTIL_UTF8_CHAR_DEFINED
typedef char char8_t;
#define UTIL_UTF8_CHAR_DEFINED
#endif

#ifndef UTIL_UTF8_STRING_DEFINED
typedef std::string u8string;
#define UTIL_UTF8_STRING_DEFINED
#endif

#if defined(__GNUC__) && !UTIL_CXX11_SUPPORTED || \
	defined(_MSC_VER) && !( \
		defined(_HAS_CHAR16_T_LANGUAGE_SUPPORT) && \
		_HAS_CHAR16_T_LANGUAGE_SUPPORT ) && \
	!defined(_CHAR16T)
#ifdef __GNUC__
typedef uint16_t char16_t;
#else
#define _CHAR16T
typedef wchar_t char16_t;
#endif
typedef uint32_t char32_t;
#endif

namespace util {

int stricmp(const char *x, const char *y);

typedef std::basic_string< char8_t, std::char_traits<char8_t> > NormalString;

} 


#if UTIL_CXX11_SUPPORTED_EXACT
#define UTIL_NOEXCEPT noexcept
#else
#define UTIL_NOEXCEPT throw()
#endif

#if UTIL_CXX11_SUPPORTED_EXACT
#define UTIL_NULLPTR nullptr
#else
#define UTIL_NULLPTR NULL
#endif


#if UTIL_HAS_ATTRIBUTE_MAYBE_UNUSED
#define UTIL_UNUSED_VARIABLE(v) \
		do { using utilUnused_##v [[maybe_unused]] = decltype(v); } \
		while (false)
#else
#define UTIL_UNUSED_VARIABLE(v) static_cast<void>(v)
#endif

#if UTIL_HAS_ATTRIBUTE_MAYBE_UNUSED
#define UTIL_UNUSED_TYPE_ALIAS(type) \
	do { using utilUnused_##type [[maybe_unused]] = type; } while (false)
#else
#define UTIL_UNUSED_TYPE_ALIAS(type) \
	do { static_cast<void>(static_cast<type*>(NULL)); } while (false)
#endif

#ifdef _MSC_VER
#define UTIL_FORCEINLINE __forceinline
#else
#define UTIL_FORCEINLINE inline
#endif

#ifdef _MSC_VER
#define UTIL_NORETURN(func) __declspec(noreturn) func
#elif defined(__GNUC__)
#define UTIL_NORETURN(func) func __attribute__((noreturn))
#else
#error 0
#endif

namespace util {
typedef const int UTIL_FLAG_TYPE;

#if !defined(UTIL_HAVE_OFF_T)
typedef uint64_t off_t;
#endif 
#if !defined(UTIL_HAVE_DEV_T)
typedef size_t dev_t;
#endif 
#if !defined(UTIL_HAVE_INO_T)
typedef size_t ino_t;
#endif 
#if !defined(UTIL_HAVE_MODE_T)
typedef size_t mode_t;
#endif 
#if !defined(UTIL_HAVE_NLINK_T)
typedef size_t nlink_t;
#endif 
#if !defined(UTIL_HAVE_UID_T)
typedef size_t uid_t;
#endif 
#if !defined(UTIL_HAVE_GID_T)
typedef size_t gid_t;
#endif 
#if !defined(UTIL_HAVE_BLKSIZE_T)
#if defined (__USE_UNIX98) && !defined (__blksize_t_defined)
typedef __blksize_t blksize_t;
#elif defined(UTIL_HAVE___BLKSIZE_T)
typedef __blksize_t blksize_t;
#else
typedef size_t blksize_t;
#endif 
#endif 
#if !defined(UTIL_HAVE_BLKCNT_T)
typedef size_t blkcnt_t;
#endif 
#if !defined(UTIL_HAVE_FSBLKCNT_T)
typedef size_t fsblkcnt_t;
#endif 

} 

namespace util {
namespace detail {


class MemoryManagerInitializer {
public:
	MemoryManagerInitializer() throw();
	~MemoryManagerInitializer();
};

#if UTIL_MEMORY_POOL_AGGRESSIVE
static MemoryManagerInitializer g_memoryManagerInitializer;
#endif

} 
} 

const int32_t ERROR_UNDEF = 0;

namespace util {

#define UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL \
		const util::Exception::NamedErrorCode &namedErrorCode = \
				util::Exception::NamedErrorCode(), \
		const char8_t *message = NULL, \
		const util::Exception::SourceSymbolChar *fileNameLiteral = NULL, \
		const util::Exception::SourceSymbolChar *functionNameLiteral = NULL, \
		int32_t lineNumber = 0, \
		const std::exception *causeInHandling = NULL, \
		const char8_t *typeNameLiteral = NULL, \
		util::Exception::StackTraceMode stackTraceMode = \
				util::Exception::STACK_TRACE_NONE, \
		util::Exception::DuplicatedLiteralFlags literalFlags = \
				util::Exception::LITERAL_NORMAL

#define UTIL_EXCEPTION_CONSTRUCTOR_ARGS_LIST \
		const util::Exception::NamedErrorCode &namedErrorCode, \
		const char8_t *message, \
		const util::Exception::SourceSymbolChar *fileNameLiteral, \
		const util::Exception::SourceSymbolChar *functionNameLiteral, \
		int32_t lineNumber, \
		const std::exception *causeInHandling, \
		const char8_t *typeNameLiteral, \
		util::Exception::StackTraceMode stackTraceMode, \
		util::Exception::DuplicatedLiteralFlags literalFlags

#define UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET \
		namedErrorCode, \
		message, \
		fileNameLiteral, \
		functionNameLiteral, \
		lineNumber, \
		causeInHandling, \
		typeNameLiteral, \
		stackTraceMode, \
		literalFlags

/*!
    @brief Basic exception handling error messages and place caused the error.
*/
class Exception : public std::exception {
	friend class ExceptionInitializer;
public:
	typedef char8_t SourceSymbolChar;
	typedef uint8_t DuplicatedLiteralFlags;

	class Field;
	template<typename Alloc> struct NoThrowString;

	struct NamedErrorCode {
	public:
		NamedErrorCode(
				int32_t code = 0,
				const SourceSymbolChar *nameLiteral = NULL) throw();

		bool isEmpty() const throw() { return code_ == 0; }
		int32_t getCode() const throw() { return code_; }
		const SourceSymbolChar* getName() const throw() { return name_; }

	private:
		int32_t code_;
		const SourceSymbolChar *name_;
	};

	enum StackTraceMode {
		STACK_TRACE_NONE,
		STACK_TRACE_TOP
	};

	enum FieldType {
		FIELD_ERROR_CODE,
		FIELD_ERROR_CODE_NAME,
		FIELD_MESSAGE,
		FIELD_FILE_NAME,
		FIELD_FUNCTION_NAME,
		FIELD_LINE_NUMBER,
		FIELD_STACK_TRACE,
		FIELD_TYPE_NAME
	};

	static const DuplicatedLiteralFlags LITERAL_NORMAL;
	static const DuplicatedLiteralFlags LITERAL_ALL_DUPLICATED;


	explicit Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw();

	virtual ~Exception() throw();

	Exception(const Exception &another) throw();

	Exception& operator=(const Exception &another) throw();

	void assign(const Exception &another, size_t startDepth = 0) throw();

	void append(const Exception &another, size_t startDepth = 0) throw();

	virtual bool isEmpty() const throw();

	size_t getMaxDepth() const throw();

	virtual void format(std::ostream &stream) const;

	virtual void formatField(
			std::ostream &stream, FieldType fieldType, size_t depth = 0) const;

	Field getField(FieldType fieldType, size_t depth = 0) const throw();

	virtual int32_t getErrorCode(size_t depth = 0) const throw();

	virtual NamedErrorCode getNamedErrorCode(size_t depth = 0) const throw();

	virtual void formatEntry(
			std::ostream &stream, size_t depth = 0) const;

	virtual void formatErrorCodeName(
			std::ostream &stream, size_t depth = 0) const;

	virtual void formatMessage(
			std::ostream &stream, size_t depth = 0) const;

#ifdef UTIL_STACK_TRACE_ENABLED
	virtual void formatStackTrace(
			std::ostream &stream, size_t depth = 0) const;
#endif

	virtual void formatTypeName(
			std::ostream &stream, size_t depth = 0) const;

	virtual void formatFileName(
			std::ostream &stream, size_t depth = 0) const;

	virtual void formatFunctionName(
			std::ostream &stream, size_t depth = 0) const;

	virtual int32_t getLineNumber(size_t depth = 0) const throw();

	virtual bool hasErrorCode(size_t depth = 0) const throw();

	virtual bool hasErrorCodeName(size_t depth = 0) const throw();

	virtual bool hasMessage(size_t depth = 0) const throw();

#ifdef UTIL_STACK_TRACE_ENABLED
	virtual bool hasStackTrace(size_t depth = 0) const throw();
#endif

	virtual bool hasTypeName(size_t depth = 0) const throw();

	virtual bool hasFileName(size_t depth = 0) const throw();

	virtual bool hasFunctionName(size_t depth = 0) const throw();

	virtual bool hasLineNumber(size_t depth = 0) const throw();

	virtual DuplicatedLiteralFlags inheritLiteralFlags(
			DuplicatedLiteralFlags baseFlags = LITERAL_ALL_DUPLICATED,
			size_t depth = 0) const throw();

	virtual const char* what() const throw();

	static void enableWhat(bool enabled) throw();

	static NamedErrorCode makeNamedErrorCode(
			int32_t code,
			const SourceSymbolChar *nameLiteral,
			const SourceSymbolChar *nameLiteralPrefix) throw();

protected:
	void* allocate(size_t size) throw();
	void deallocate(void *ptr) throw();
	void deallocateLiteral(const char8_t *literalStr,
			FieldType fieldType, DuplicatedLiteralFlags literalFlags) throw();

	char8_t* tryCopyString(const char8_t *str) throw();
	const char8_t* tryCopyLiteral(const char8_t *literalStr,
			FieldType fieldType, DuplicatedLiteralFlags literalFlags) throw();
	NamedErrorCode tryCopyNamedErrorCode(const NamedErrorCode &src,
			DuplicatedLiteralFlags literalFlags) throw();

	virtual void clear() throw();

private:
	struct Entry {
		Entry() throw();
		bool isEmpty(bool typeNameIgnorable) const throw();

		NamedErrorCode namedErrorCode_;
		char8_t *message_;
#ifdef UTIL_STACK_TRACE_ENABLED
		char8_t *stackTrace_;
#endif
		const char8_t *typeName_;
		const SourceSymbolChar *fileName_;
		const SourceSymbolChar *functionName_;
		int32_t lineNumber_;
		DuplicatedLiteralFlags literalFlags_;
	};

	static const size_t BUFFER_SIZE = 512;

	const Entry* getEntryAt(size_t depth) const throw();

	static const char8_t* resolveException(
			Exception **fullEx, std::exception **stdEx) throw();

	static const char8_t* resolveTypeName(const char8_t *customName) throw();

	void fillWhat() throw();

	void setEntry(Entry &dest, const Entry &src) throw();

	void setEntry(Entry &entry,
			const NamedErrorCode &namedErrorCode,
			const char8_t *message,
			const char8_t *stackTrace,
			const char8_t *typeNameLiteral,
			const SourceSymbolChar *fileNameLiteral,
			const SourceSymbolChar *functionNameLiteral,
			int32_t lineNumber,
			DuplicatedLiteralFlags literalFlags) throw();

	void clearEntry(Entry &entry) throw();

	static bool whatEnabled_;

	uint8_t buffer_[BUFFER_SIZE];
	size_t bufferOffset_;

	Entry *subEntries_;
	size_t maxDepth_;
	Entry topEntry_;
	char8_t *what_;
};

/*!
    @brief String for exception without throwing.
*/
template<typename Alloc>
struct Exception::NoThrowString {
public:
	typedef std::basic_ostringstream<
			char8_t, std::char_traits<char8_t>, Alloc> Stream;
	typedef std::basic_string<
			char8_t, std::char_traits<char8_t>, Alloc> String;


	explicit NoThrowString(Stream &stream) throw();

	~NoThrowString() throw();

	const char8_t* get() const throw();

private:
	NoThrowString(const NoThrowString&);
	NoThrowString& operator=(NoThrowString&);

	String *str_;
	uint64_t storage_[
			(sizeof(String) + sizeof(uint64_t) - 1) / sizeof(uint64_t)];
};

class Exception::Field {
public:
	explicit Field(const Exception &exception, FieldType fieldType,
			size_t depth = 0) throw();

	void format(std::ostream &s) const;

private:
	friend class Exception;

	Field(const Field&);
	Field& operator=(const Field&);

	const Exception &exception_;
	FieldType fieldType_;
	size_t depth_;
};

std::ostream& operator<<(std::ostream &s, const Exception::Field &field);

/*!
    @brief Exception handling an error number of a system call.
*/
class PlatformException : public Exception {
public:
	explicit PlatformException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw();
	virtual ~PlatformException() throw();
};

class UtilityException : public Exception {
public:
	explicit UtilityException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw();
	virtual ~UtilityException() throw();

	static UtilityException inherit(
			UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw();

	enum Code {
		CODE_DEFAULT = 0,	
		CODE_ILLEGAL_OPERATION,	
		CODE_ILLEGAL_ARGUMENT,	
		CODE_INVALID_STATUS,	
		CODE_INVALID_PARAMETER,	
		CODE_NO_MEMORY,	
		CODE_MEMORY_LIMIT_EXCEEDED,	
		CODE_SIZE_LIMIT_EXCEEDED,	
		CODE_DECODE_FAILED,	
		CODE_VALUE_OVERFLOW, 
		CODE_LIBRARY_UNMATCH 
	};
};

class LocaleUtils {
public:
	inline static const std::locale& getCLocale() throw() {
		return *cLocale_;
	}

	class CLocaleScope {
	public:
		inline explicit CLocaleScope(std::ios_base &baseStream) :
				baseStream_(NULL),
				orgLocale_(NULL) {
			if (baseStream.getloc() != getCLocale()) {
				set(baseStream);
			}
		}

		inline ~CLocaleScope() {
			if (orgLocale_ != NULL) {
				unset();
			}
		}

	private:
		void set(std::ios_base &baseStream);
		void unset() throw();

		std::ios_base *baseStream_;
		std::locale *orgLocale_;
		uint8_t orgLocaleStorage_[sizeof(std::locale)];
	};

	class Initializer {
	public:
		Initializer();
		~Initializer();
	};

private:
	LocaleUtils();

	static const std::locale *cLocale_;
};

namespace detail {
static LocaleUtils::Initializer g_localeUtilsInitializer;
}

class NormalOStringStream : public std::ostringstream {
public:
	inline explicit NormalOStringStream(
			std::ios_base::openmode mode = ios_base::out) :
			std::ostringstream(mode) {
		if (getloc() != LocaleUtils::getCLocale()) {
			imbue(LocaleUtils::getCLocale());
		}
	}

	inline explicit NormalOStringStream(
			const std::string &str,
			std::ios_base::openmode mode = ios_base::out) :
			std::ostringstream(str, mode) {
	}

	virtual ~NormalOStringStream();
};

class NormalIStringStream : public std::istringstream {
public:
	inline explicit NormalIStringStream(
			std::ios_base::openmode mode = ios_base::in) :
			std::istringstream(mode) {
		if (getloc() != LocaleUtils::getCLocale()) {
			imbue(LocaleUtils::getCLocale());
		}
	}

	inline explicit NormalIStringStream(
			const std::string &str,
			std::ios_base::openmode mode = ios_base::in) :
			std::istringstream(str, mode) {
	}

	virtual ~NormalIStringStream();
};

} 

#define ostringstream ostringstream_is_disabled_because_of_locale_dependency
#define istringstream istringstream_is_disabled_because_of_locale_dependency
#define stringstream stringstream_is_disabled_because_of_locale_dependency

#ifdef __GNUC__
#define UTIL_EXCEPTION_THROWN_FUNCTION __PRETTY_FUNCTION__
#else
#define UTIL_EXCEPTION_THROWN_FUNCTION __FUNCTION__
#endif

#define UTIL_EXCEPTION_POSITION_ARGS \
	__FILE__, UTIL_EXCEPTION_THROWN_FUNCTION, __LINE__

#define UTIL_EXCEPTION_CREATE_MESSAGE_STREAM \
	util::NormalOStringStream().write("", 0)
#define UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(message) \
	util::Exception::NoThrowString<util::NormalOStringStream::allocator_type>( \
			static_cast<util::NormalOStringStream&>( \
					UTIL_EXCEPTION_CREATE_MESSAGE_STREAM << message)).get()

#define UTIL_EXCEPTION_CREATE_CUSTOM_DETAIL( \
		type, errorCode, cause, message, stackTraceMode) \
	type((errorCode), UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(message), \
			UTIL_EXCEPTION_POSITION_ARGS, (cause), #type, \
			stackTraceMode)
#define UTIL_EXCEPTION_CREATE_DETAIL(type, errorCode, cause, message) \
	UTIL_EXCEPTION_CREATE_CUSTOM_DETAIL( \
			type, errorCode, cause, message, util::Exception::STACK_TRACE_NONE)
#define UTIL_EXCEPTION_CREATE_DETAIL_TRACE(type, errorCode, cause, message) \
	UTIL_EXCEPTION_CREATE_CUSTOM_DETAIL( \
			type, errorCode, cause, message, util::Exception::STACK_TRACE_TOP)

#define UTIL_EXCEPTION_UTIL_NAMED_CODE(codeSymbol) \
	util::Exception::makeNamedErrorCode( \
			util::UtilityException::codeSymbol, #codeSymbol, "CODE_")
#define UTIL_THROW_UTIL_ERROR(codeSymbol, message) \
	throw UTIL_EXCEPTION_CREATE_DETAIL_TRACE(util::UtilityException, \
			UTIL_EXCEPTION_UTIL_NAMED_CODE(codeSymbol), NULL, message)
#define UTIL_THROW_UTIL_ERROR_CODED(codeSymbol) \
	UTIL_THROW_UTIL_ERROR(codeSymbol, "Utility error occurred (code=" << \
			UTIL_EXCEPTION_UTIL_NAMED_CODE(codeSymbol).getName() << ")")
#define UTIL_RETHROW_UTIL_ERROR(codeSymbol, cause, message) \
	throw util::UtilityException::inherit( \
			UTIL_EXCEPTION_UTIL_NAMED_CODE(codeSymbol), \
			UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(message), \
			UTIL_EXCEPTION_POSITION_ARGS, &(cause), \
			"util::UtilityException", util::Exception::STACK_TRACE_NONE)
#define UTIL_THROW_NOIMPL_UTIL() \
	UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,  "Not implemented")

#define UTIL_THROW_ERROR(errorCode, message) \
	throw UTIL_EXCEPTION_CREATE_DETAIL_TRACE( \
			util::Exception, errorCode, NULL, message)
#define UTIL_RETHROW(errorCode, cause, message) \
	throw UTIL_EXCEPTION_CREATE_DETAIL( \
			util::Exception, errorCode, &(cause), message)
#define UTIL_THROW_NOIMPL() UTIL_THROW_ERROR(0, "Not implemented")

namespace util {
class PlatformExceptionBuilder {
public:
	enum Type {
		TYPE_NORMAL,
		TYPE_ADDRINFO_LINUX
	};

	PlatformException operator()(
			const char8_t *message,
			const Exception::SourceSymbolChar *fileNameLiteral,
			const Exception::SourceSymbolChar *functionNameLiteral,
			int32_t lineNumber,
			Type type = TYPE_NORMAL,
			int32_t specialErrorCode = 0);
};
}

#define UTIL_THROW_PLATFORM_ERROR(message) \
	throw util::PlatformExceptionBuilder()(message, UTIL_EXCEPTION_POSITION_ARGS);

#define UTIL_THROW_PLATFORM_ERROR_WITH_CODE(type, errorCode, message) \
	throw util::PlatformExceptionBuilder()( \
			message, UTIL_EXCEPTION_POSITION_ARGS, \
			util::PlatformExceptionBuilder::type, errorCode);


struct UtilExceptionTag;

namespace util {
struct LibraryTool {
	typedef int32_t (*ProviderFunc)(
			const void *const **funcList, size_t *funcCount);

	static bool findError(int32_t code, UtilExceptionTag *ex) throw();

	template<typename E> static E fromLibraryException(
			int32_t code, ProviderFunc provider,
			UtilExceptionTag *&src) throw() {
		E dest;
		fromLibraryException(code, provider, src, dest);
		return dest;
	}

	static void fromLibraryException(
			int32_t code, ProviderFunc provider, UtilExceptionTag *&src,
			Exception &dest) throw();
	static int32_t toLibraryException(
			const Exception &src, UtilExceptionTag **dest) throw();
};
} 


namespace util {
	
/*!
    @brief Structure template for only assert macros during compile.
*/
template<bool>
struct StaticAssertChecker {
public:
	enum {
		ASSERTION_FAILED
	};
};

template<>
struct StaticAssertChecker<false> {
private:
	enum {
		ASSERTION_FAILED
	};
};
}

#define UTIL_STATIC_ASSERT(expression) \
	static_cast<void>( \
			util::StaticAssertChecker<(expression)>::ASSERTION_FAILED)


namespace util {
struct TrueType { enum Value { VALUE = 1 }; };
struct FalseType { enum Value { VALUE = 0 }; };
template<bool> struct BoolType;

template<> struct BoolType<false> { typedef FalseType Result; };
template<> struct BoolType<true> { typedef TrueType Result; };

template<typename Base, typename Sub>
class BaseOf {
private:
	typedef int T;
	struct F { T i, j; };

	static T check(const Base*);
	static F check(...);

public:
	typedef typename BoolType<
			sizeof(check(static_cast<Sub*>(NULL))) == sizeof(T)>::Result Result;
};

template<bool, typename T = void> struct EnableIf {
};

template<typename T> struct EnableIf<true, T> {
	typedef T Type;
};

template<bool Cond, typename T, typename F>
struct Conditional {
	typedef T Type;
};

template<typename T, typename F>
struct Conditional<false, T, F> {
	typedef F Type;
};

template<typename T, typename U> struct IsSame {
	typedef FalseType Type;
	enum Value { VALUE = Type::VALUE };
};

template<typename T> struct IsSame<T, T> {
	typedef TrueType Type;
	enum Value { VALUE = Type::VALUE };
};

template<typename>
struct IsPointer {
	typedef FalseType Type;
	enum Value { VALUE = Type::VALUE };
};

template<typename E>
struct IsPointer<E*> {
	typedef TrueType Type;
	enum Value { VALUE = Type::VALUE };
};

template<typename F>
struct BinaryFunctionResultOf {
#if UTIL_CXX17_SUPPORTED
	typedef typename std::invoke_result<F(int, int)>::type Type;
#elif UTIL_CXX11_SUPPORTED_EXACT
	typedef typename std::result_of<F(
			typename F::first_argument_type,
			typename F::second_argument_type)>::type Type;
#else
	typedef typename Conditional<
			(sizeof(typename F::first_argument_type) > 0 &&
			sizeof(typename F::second_argument_type) > 0),
			F, void>::Type::result_type Type;
#endif
};

}


namespace util {
namespace detail {
class RawNumberFormatter {
public:
	const char8_t* operator()(uint64_t value);
	const char8_t* operator()(int64_t value);

	const char8_t* operator()(uint32_t value);
	const char8_t* operator()(int32_t value);

private:
	char8_t result_[100];
};
}	
}	


namespace util {
namespace detail {
class LocalString {
public:
	LocalString(char8_t *localBuf, size_t capacity) throw();
	~LocalString();
	bool tryAppend(const char8_t *value) throw();
	bool tryAppend(const char8_t *begin, const char8_t *end) throw();
	const char8_t* tryGet(const char8_t *alternative = "") throw();

private:
	size_t size_;
	size_t capacity_;

	char8_t *localBuf_;
	char8_t *dynamicBuf_;
};
}	
}	


#ifdef UTIL_STACK_TRACE_ENABLED
namespace util {

class StackTraceHandler {
public:
	virtual void operator()(const char8_t *name, int32_t line) = 0;
};

class StackTraceUtils {
public:
	static void getStackTrace(StackTraceHandler &handler);
	static std::ostream& getStackTrace(std::ostream &stream);

	class Initializer {
	public:
		Initializer();
		~Initializer();

	private:
		static size_t counter_;
	};

private:
	struct Impl;

	StackTraceUtils();

	static Impl *impl_;
};

namespace detail {
static StackTraceUtils::Initializer g_stackTraceUtilsInitializer;

class StackTraceStringHandler : public StackTraceHandler {
public:
	explicit StackTraceStringHandler(
			detail::LocalString &str,
			size_t maxDepth = 0, bool ignoreLibs = false);

	virtual ~StackTraceStringHandler();

	virtual void operator()(const char8_t *name, int32_t line);

private:
	detail::LocalString &str_;
	size_t maxDepth_;
	bool ignoreLibs_;
	size_t lastDepth_;
};

}	
}	
#endif	


namespace util {
/*!
    @brief Utility for debugging.
*/
class DebugUtils {
public:

	static bool isDebuggerAttached();

	static void interrupt();

private:
	DebugUtils();
	DebugUtils(const DebugUtils&);
	DebugUtils& operator=(const DebugUtils&);
};
} 


namespace util {
namespace detail {
struct FreeLink {
	FreeLink *next_;
};
struct DirectAllocationUtils {
	enum {
		LARGE_ELEMENT_BITS = 20,
		ELEMENT_MARGIN_SIZE = sizeof(uint64_t) * 4
	};

	static void* allocate(size_t size, bool monitoring) UTIL_NOEXCEPT;
	static void deallocate(void *ptr, bool monitoring) UTIL_NOEXCEPT;

#if UTIL_MEMORY_POOL_AGGRESSIVE
	static FreeLink* allocateBulk(
			size_t size, bool monitoring, bool aligned,
			size_t &count) UTIL_NOEXCEPT;
	static void deallocateBulk(
			FreeLink *link, size_t size, bool monitoring,
			bool aligned) UTIL_NOEXCEPT;
#endif

	static void* allocateDirect(
			size_t size, bool monitoring, bool aligned) UTIL_NOEXCEPT;
	static void deallocateDirect(
			void *ptr, size_t size, bool monitoring,
			bool aligned) UTIL_NOEXCEPT;

	static size_t adjustAllocationSize(
			size_t size, bool withHead, size_t *index) UTIL_NOEXCEPT;

	static size_t getUnitCount() UTIL_NOEXCEPT;
	static bool getUnitProfile(
			size_t index, int64_t &totalSize, int64_t &cacheSize,
			int64_t &monitoringSize, int64_t &totalCount, int64_t &cacheCount,
			int64_t &deallocCount) UTIL_NOEXCEPT;

	static void dumpStats(std::ostream &os);
	static void dumpSizeHistogram(std::ostream &os);
};
} 
} 

#define UTIL_MALLOC(size) util::detail::DirectAllocationUtils::allocate(size, false)
#define UTIL_FREE(ptr) util::detail::DirectAllocationUtils::deallocate(ptr, false)

#define UTIL_MALLOC_MONITORING(size) \
	util::detail::DirectAllocationUtils::allocate(size, true)
#define UTIL_FREE_MONITORING(ptr) \
	util::detail::DirectAllocationUtils::deallocate(ptr, true)



#if UTIL_CXX11_SUPPORTED
#define UTIL_UNIQUE_PTR std::unique_ptr
#else
#define UTIL_UNIQUE_PTR std::auto_ptr
#endif



#if UTIL_FAILURE_SIMULATION_ENABLED
namespace util {
class AllocationFailureSimulator {
public:
	enum Target {
		TARGET_NEW,
		TARGET_STACK_ALLOCATION,
	};

	static void set(int32_t targetType, uint64_t startCount, uint64_t endCount);

	static void checkOperation(int32_t targetType, size_t size);
	static bool checkOperationDirect(int32_t targetType, size_t size) UTIL_NOEXCEPT;

	static uint64_t getLastOperationCount() { return lastOperationCount_; }

private:
	AllocationFailureSimulator();
	~AllocationFailureSimulator();

	static volatile bool enabled_;
	static volatile int32_t targetType_;
	static volatile uint64_t startCount_;
	static volatile uint64_t endCount_;
	static volatile uint64_t lastOperationCount_;
};
} 

void* operator new(size_t size);
void* operator new[](size_t size);
void operator delete(void *p);
void operator delete[](void *p);
#else

#ifdef UTIL_PLACEMENT_NEW_ENABLED
#if UTIL_CXX11_SUPPORTED
#define UTIL_PLACEMENT_NEW_SPECIFIER
#else
#define UTIL_PLACEMENT_NEW_SPECIFIER throw(std::bad_alloc)
#endif
void* operator new(size_t size) UTIL_PLACEMENT_NEW_SPECIFIER;
void* operator new[](size_t size) UTIL_PLACEMENT_NEW_SPECIFIER;
void operator delete(void *p) UTIL_NOEXCEPT;
void operator delete[](void *p) UTIL_NOEXCEPT;
#endif 

#endif	

#define UTIL_NEW new

#endif
