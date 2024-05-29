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
	@brief  C Utility
*/

#include "c_utility_cpp.h"
#include "gs_error.h"
#include <cstdarg>
#include <cstdio>


CUtilAllocatorInfo cutilMakeAllocatorInfo(
		CUtilAllocatorGroupId groupId, const GSChar *name) {
	CUtilAllocatorInfo info = CUTIL_ALLOCATOR_INFO_INITIALIZER;
	info.groupId = groupId;
	info.name = name;
	return info;
}

void cutilSetException(
		CUtilException *exception,
		CUtilExceptionType type, GSResult errorCode,
		const GSChar *file, int32_t line,
		const GSChar *function, const GSChar *messageFormat, ...) {
	if (exception == NULL) {
		return;
	}

	char8_t messageBuf[1024];

	va_list args;
	va_start(args, messageFormat);

#ifdef _MSC_VER
	_vsnprintf_s(
			messageBuf, sizeof(messageBuf), _TRUNCATE, messageFormat, args);
#else
	vsnprintf(messageBuf, sizeof(messageBuf), messageFormat, args);
#endif

	va_end(args);

	const char8_t *typeName = NULL;
	util::Exception::StackTraceMode traceMode =
			util::Exception::STACK_TRACE_TOP;
	switch (type) {
	case CUTIL_EXCEPTION_USER:
		typeName = "UserException";
		traceMode = util::Exception::STACK_TRACE_NONE;
		break;
	case CUTIL_EXCEPTION_SYSTEM:
		typeName = "SystemException";
		traceMode = util::Exception::STACK_TRACE_TOP;
		break;
	default:
		assert(false);
		break;
	}

	util::Exception &baseException =
			*CUtilExceptionWrapper::unwrap(exception);
	if (baseException.getMaxDepth() == 0 &&
			baseException.getLineNumber() == 0) {
		baseException = util::Exception(
				errorCode,
				messageBuf,
				file,
				function,
				line,
				NULL,
				typeName,
				traceMode);
	}
	else {
		try {
			throw baseException;
		}
		catch (util::Exception &e) {
			baseException = util::Exception(
					errorCode,
					messageBuf,
					file,
					function,
					line,
					&e,
					typeName,
					traceMode);
		}
		catch (...) {
			assert(false);
		}
	}
}

GSResult cutilCreateFixedSizeAllocator(
		CUtilException *exception, const CUtilFixedSizeAllocatorConfig *config,
		CUtilAllocatorInfo info, CUtilFixedSizeAllocator **allocator) {
	assert(config != NULL);
	assert(allocator != NULL);

	try {
		*allocator = CUtilFixedSizeAllocatorWrapper::wrap(
				UTIL_NEW util::FixedSizeAllocator<>(
						util::AllocatorInfo(info.groupId, info.name),
						config->elementSize));
	}
	catch (...) {
		*allocator = NULL;
		return CUtilTool::saveCurrentException(exception);
	}

	return GS_RESULT_OK;
}

void cutilDestroyFixedSizeAllocator(CUtilFixedSizeAllocator *allocator) {
	delete CUtilFixedSizeAllocatorWrapper::unwrap(allocator);
}

GSResult cutilAllocateFixedSize(
		CUtilException *exception, CUtilFixedSizeAllocator *allocator,
		void **ptr) {
	assert(allocator != NULL);
	assert(ptr != NULL);

	try {
		*ptr = CUtilFixedSizeAllocatorWrapper::unwrap(allocator)->allocate();
	}
	catch (...) {
		*ptr = NULL;
		return CUtilTool::saveCurrentException(exception);
	}

	return GS_RESULT_OK;
}

void cutilDeallocateFixedSize(CUtilFixedSizeAllocator *allocator, void *ptr) {
	assert(allocator != NULL);

	try {
		CUtilFixedSizeAllocatorWrapper::unwrap(allocator)->deallocate(ptr);
	}
	catch (...) {
		assert(false);
	}
}

GSResult cutilCreateVariableSizeAllocator(
		CUtilException *exception, const CUtilVariableSizeAllocatorConfig *config,
		CUtilAllocatorInfo info, CUtilVariableSizeAllocator **allocator) {
	assert(config != NULL);
	assert(allocator != NULL);
	UNUSED_VARIABLE(config);

	try {
		SQLVariableSizeGlobalAllocatorTraits traits;
		*allocator = CUtilVariableSizeAllocatorWrapper::wrap(
				UTIL_NEW CUtilRawVariableSizeAllocator(
						util::AllocatorInfo(info.groupId, info.name), traits));
	}
	catch (...) {
		*allocator = NULL;
		return CUtilTool::saveCurrentException(exception);
	}

	return GS_RESULT_OK;
}

void cutilDestroyVariableSizeAllocator(CUtilVariableSizeAllocator *allocator) {
	delete CUtilVariableSizeAllocatorWrapper::unwrap(allocator);
}

GSResult cutilAllocateVariableSize(
		CUtilException *exception, CUtilVariableSizeAllocator *allocator,
		size_t size, void **ptr) {
	assert(allocator != NULL);
	assert(ptr != NULL);

	try {
		*ptr = CUtilVariableSizeAllocatorWrapper::unwrap(
				allocator)->allocate(size);
	}
	catch (...) {
		*ptr = NULL;
		return CUtilTool::saveCurrentException(exception);
	}

	return GS_RESULT_OK;
}

void cutilDeallocateVariableSize(
		CUtilVariableSizeAllocator *allocator, void *ptr) {
	assert(allocator != NULL);

	try {
		CUtilVariableSizeAllocatorWrapper::unwrap(allocator)->deallocate(ptr);
	}
	catch (...) {
		assert(false);
	}
}

size_t cutilGetVariableSizeElementCapacity(
		CUtilVariableSizeAllocator *allocator, void *ptr) {
	assert(allocator != NULL);

	size_t capacity;
	try {
		capacity = CUtilVariableSizeAllocatorWrapper::unwrap(
				allocator)->getElementCapacity(ptr);
	}
	catch (...) {
		capacity = 0;
		assert(false);
	}

	return capacity;
}

GSResult cutilAllocateStack(
		CUtilException *exception, CUtilStackAllocator *allocator,
		size_t size, void **ptr) {
	assert(allocator != NULL);
	assert(ptr != NULL);

	try {
		*ptr = allocator->getBase().allocate(size);
	}
	catch (...) {
		*ptr = NULL;
		return CUtilTool::saveCurrentException(exception);
	}

	return GS_RESULT_OK;
}

CUtilStackAllocatorScope* cutilEnterAllocatorScope(
		CUtilStackAllocator *allocator) {
	assert (allocator != NULL);
	return allocator->enterScope();
}

void cutilLeaveAllocatorScope(CUtilStackAllocatorScope *scope) {
	if (scope == NULL) {
		assert(false);
		return;
	}

	scope->getAllocator().leaveScope(scope);
}

GSResult CUtilTool::saveCurrentException(CUtilException *exception) throw() {
	util::Exception *dest;
	if (exception != NULL) {
		dest = CUtilExceptionWrapper::unwrap(exception);
	}
	else if (LocalException::localRef_ != NULL) {
		dest = LocalException::localRef_;
	}
	else {
		return GS_ERROR_CM_FAILED;
	}

	try {
		throw;
	}
	catch (...) {
		std::exception e;
		*dest = GS_EXCEPTION_CONVERT(e, "");
		return dest->getErrorCode();
	}
}

DynamicVariableSizeAllocatorTraits::DynamicVariableSizeAllocatorTraits(
		size_t fixedAllocatorCount, const size_t *fixedSizeList) {
	if (fixedAllocatorCount != FIXED_ALLOCATOR_COUNT) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	memcpy(sizeList_, fixedSizeList,
			sizeof(*sizeList_) * FIXED_ALLOCATOR_COUNT);
}

CUtilStackAllocatorScopeTag::CUtilStackAllocatorScopeTag(
		CUtilStackAllocator &alloc) throw() :
		alloc_(alloc),
		base_(alloc.getBase()) {
}

CUtilStackAllocatorScopeTag::~CUtilStackAllocatorScopeTag() {
}

const size_t CUtilStackAllocatorTag::ALLOCATOR_MAX_SCOPE = 100;

CUtilStackAllocatorTag::CUtilStackAllocatorTag(util::StackAllocator &base) :
		base_(base),
		scopeList_(UTIL_NEW ScopeStorage[ALLOCATOR_MAX_SCOPE]),
		scopeIndex_(0) {
}

CUtilStackAllocatorTag::~CUtilStackAllocatorTag() {
	delete[] scopeList_;
}

CUtilStackAllocatorScope* CUtilStackAllocatorTag::enterScope() throw() {
	if (scopeIndex_ >= ALLOCATOR_MAX_SCOPE) {
		assert(false);
		return NULL;
	}

	ScopeStorage &scope = scopeList_[scopeIndex_];
	scopeIndex_++;
	return new(static_cast<void*>(&scope)) CUtilStackAllocatorScope(*this);
}

void CUtilStackAllocatorTag::leaveScope(
		CUtilStackAllocatorScope *scope) throw() {
	if (scope == NULL) {
		assert(false);
		return;
	}

	assert(&scope->getAllocator() == this);

	if (scopeIndex_ == 0) {
		assert(false);
		return;
	}

	if (static_cast<void*>(scope) != &scopeList_[scopeIndex_ - 1]) {
		assert(false);
		return;
	}

	scopeIndex_--;
	scope->~CUtilStackAllocatorScopeTag();
}

UTIL_THREAD_LOCAL util::Exception *LocalException::localRef_ = NULL;

LocalException::LocalException() throw() {
	assert(localRef_ == NULL);
	localRef_ = &storage_;
}

LocalException::~LocalException() throw() {
	assert(localRef_ != NULL);
	localRef_ = NULL;
}

util::Exception* LocalException::check() throw() {
	assert(localRef_ != NULL);
	if (storage_.getErrorCode() == 0) {
		return NULL;
	}
	return &storage_;
}

void LocalException::set(const util::Exception &exception) throw() {
	if (exception.getErrorCode() == 0 || localRef_ == NULL) {
		assert(false);
		return;
	}

	*localRef_ = exception;
}

const util::Exception *LocalException::get() throw() {
	if (localRef_ == NULL || localRef_->getErrorCode() == 0) {
		return NULL;
	}

	return localRef_;
}
