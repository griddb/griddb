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
	@brief Definitions of common errors
*/
#ifndef GS_ERROR_COMMON_H_
#define GS_ERROR_COMMON_H_

#ifndef GS_ERROR_COMMON_ONLY
#define GS_ERROR_COMMON_ONLY 0
#endif

#if GS_ERROR_COMMON_ONLY
#include "util/type.h"
#else
#include "gs_error.h"
#endif

enum GSCommonErrorCode {
	GS_ERROR_JSON_INVALID_SYNTAX = 121000,
	GS_ERROR_JSON_KEY_NOT_FOUND,
	GS_ERROR_JSON_VALUE_OUT_OF_RANGE,
	GS_ERROR_JSON_UNEXPECTED_TYPE,

	GS_ERROR_HTTP_INTERNAL_ILLEGAL_OPERATION = 122000,
	GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER,
	GS_ERROR_HTTP_UNEXPECTED_MESSAGE,
	GS_ERROR_HTTP_INVALID_MESSAGE,

	GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION = 123000,
	GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER,
	GS_ERROR_SA_INVALID_CONFIG,
	GS_ERROR_SA_ADDRESS_CONFLICTED,
	GS_ERROR_SA_ADDRESS_NOT_ASSIGNED,
	GS_ERROR_SA_INVALID_ADDRESS,

	GS_ERROR_AUTH_INTERNAL_ILLEGAL_OPERATION = 124000,
	GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE,
	GS_ERROR_AUTH_INVALID_CREDENTIALS
};

#if GS_ERROR_COMMON_ONLY
#define GS_COMMON_THROW_USER_ERROR(errorCode, message) \
	UTIL_THROW_ERROR(errorCode, message)
#define GS_COMMON_RETHROW_USER_ERROR(cause, message) \
	UTIL_RETHROW(0, cause, message)
#define GS_COMMON_EXCEPTION_MESSAGE(cause) ""
#else
#define GS_COMMON_THROW_USER_ERROR(errorCode, message) \
	GS_THROW_USER_ERROR(errorCode, message)
#define GS_COMMON_RETHROW_USER_ERROR(cause, message) \
	GS_RETHROW_USER_ERROR(cause, message)
#define GS_COMMON_EXCEPTION_MESSAGE(cause) GS_EXCEPTION_MESSAGE(cause)
#endif

#endif
