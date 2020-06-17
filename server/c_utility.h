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
/*!
	@file
	@brief C Utility
*/
#ifndef NEWSQL_C_UTILITY_H_
#define NEWSQL_C_UTILITY_H_

#include "gridstore.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __GNUC__
#define CUTIL_PRINTF_FORMAT_ATTRIBUTE(p1, p2) \
	__attribute__((__format__ (__printf__, p1, p2)))
#else
#define CUTIL_PRINTF_FORMAT_ATTRIBUTE(p1, p2)
#endif

#ifdef __GNUC__
#define CUTIL_EXCEPTION_SET_FUNCTION __PRETTY_FUNCTION__
#else
#define CUTIL_EXCEPTION_SET_FUNCTION __FUNCTION__
#endif

typedef struct CUtilExceptionTag CUtilException;
typedef struct CUtilFixedSizeAllocatorTag CUtilFixedSizeAllocator;
typedef struct CUtilVariableSizeAllocatorTag CUtilVariableSizeAllocator;
typedef struct CUtilStackAllocatorTag CUtilStackAllocator;
typedef struct CUtilStackAllocatorScopeTag CUtilStackAllocatorScope;

typedef int32_t CUtilAllocatorGroupId;
typedef struct CUtilAllocatorInfoTag {
	CUtilAllocatorGroupId groupId;
	const GSChar *name;
} CUtilAllocatorInfo;

#define CUTIL_ALLOCATOR_INFO_INITIALIZER { 0, NULL }

CUtilAllocatorInfo cutilMakeAllocatorInfo(
		CUtilAllocatorGroupId groupId, const GSChar *name);

typedef struct CUtilFixedSizeAllocatorConfigTag {
	size_t elementSize;
} CUtilFixedSizeAllocatorConfig;

#define CUTIL_FIXED_SIZE_ALLOCATOR_CONFIG_INITIALIZER \
		{ 0 }

typedef struct CUtilVariableSizeAllocatorConfigTag {
	size_t fixedAllocatorCount;
	const size_t *fixedSizeList;
} CUtilVariableSizeAllocatorConfig;

#define CUTIL_VARIABLE_SIZE_ALLOCATOR_CONFIG_INITIALIZER \
		{ 0, NULL }

enum CUtilExceptionTypeTag {
	CUTIL_EXCEPTION_USER,
	CUTIL_EXCEPTION_SYSTEM
};

typedef GSEnum CUtilExceptionType;

void cutilSetException(
		CUtilException *exception,
		CUtilExceptionType type, GSResult errorCode,
		const GSChar *file, int32_t line,
		const GSChar *function, const char *messageFormat, ...)
		CUTIL_PRINTF_FORMAT_ATTRIBUTE(7, 8);

#define CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, typeSuffix, errorCode, messageFormat) \
		exception, CUTIL_EXCEPTION_ ## typeSuffix, errorCode, \
		__FILE__, __LINE__, CUTIL_EXCEPTION_SET_FUNCTION, \
		messageFormat

#define CUTIL_SET_USER_EXCEPTION( \
		exception, errorCode, messageFormat) \
		cutilSetException(CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, USER, errorCode, messageFormat))

#define CUTIL_SET_USER_EXCEPTION_ARG1( \
		exception, errorCode, messageFormat, arg1) \
		cutilSetException(CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, USER, errorCode, messageFormat), arg1)

#define CUTIL_SET_USER_EXCEPTION_ARG2( \
		exception, errorCode, messageFormat, arg1, arg2) \
		cutilSetException(CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, USER, errorCode, messageFormat), arg1, arg2)

#define CUTIL_SET_SYSTEM_EXCEPTION( \
		exception, errorCode, messageFormat) \
		cutilSetException(CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, SYSTEM, errorCode, messageFormat))

#define CUTIL_SET_SYSTEM_EXCEPTION_ARG1( \
		exception, errorCode, messageFormat, arg1) \
		cutilSetException(CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, SYSTEM, errorCode, messageFormat), arg1)

#define CUTIL_SET_SYSTEM_EXCEPTION_ARG2( \
		exception, errorCode, messageFormat, arg1, arg2) \
		cutilSetException(CUTIL_DETAIL_EXCEPTION_MAIN_ARGS( \
		exception, SYSTEM, errorCode, messageFormat), arg1, arg2)

GSResult cutilCreateFixedSizeAllocator(
		CUtilException *exception, const CUtilFixedSizeAllocatorConfig *config,
		CUtilAllocatorInfo info, CUtilFixedSizeAllocator **allocator);
void cutilDestroyFixedSizeAllocator(CUtilFixedSizeAllocator *allocator);

GSResult cutilAllocateFixedSize(
		CUtilException *exception, CUtilFixedSizeAllocator *allocator,
		void **ptr);
void cutilDeallocateFixedSize(CUtilFixedSizeAllocator *allocator, void *ptr);

GSResult cutilCreateVariableSizeAllocator(
		CUtilException *exception, const CUtilVariableSizeAllocatorConfig *config,
		CUtilAllocatorInfo info, CUtilVariableSizeAllocator **allocator);
void cutilDestroyVariableSizeAllocator(CUtilVariableSizeAllocator *allocator);

GSResult cutilAllocateVariableSize(
		CUtilException *exception, CUtilVariableSizeAllocator *allocator,
		size_t size, void **ptr);
void cutilDeallocateVariableSize(
		CUtilVariableSizeAllocator *allocator, void *ptr);

size_t cutilGetVariableSizeElementCapacity(
		CUtilVariableSizeAllocator *allocator, void *ptr);

GSResult cutilAllocateStack(
		CUtilException *exception, CUtilStackAllocator *allocator,
		size_t size, void **ptr);

CUtilStackAllocatorScope* cutilEnterAllocatorScope(
		CUtilStackAllocator *allocator);
void cutilLeaveAllocatorScope(CUtilStackAllocatorScope *scope);


#ifdef __cplusplus
}	/* extern "C" { */
#endif

#endif /* NEWSQL_C_UTILITY_H_ */
