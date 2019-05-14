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
	@brief Definition of Numeric Utility function
*/
#ifndef UTIL_NUMERIC_H_
#define UTIL_NUMERIC_H_

#include "util/type.h"

#if UTIL_CXX11_SUPPORTED
#include <cmath>
#elif defined(_WIN32)
#include <cfloat>
#else
#include <math.h>
#endif

namespace util {

template<typename T> bool isInf(const T &value);
template<typename T> bool isFinite(const T &value);
template<typename T> bool isNaN(const T &value);
template<typename T> T copySign(const T &value1, const T &value2);

} // namespace util

namespace util {

template<typename T> inline bool isInf(const T &value) {
	UTIL_STATIC_ASSERT(!std::numeric_limits<T>::is_integer);

#if UTIL_CXX11_SUPPORTED
	return std::isinf(value);
#elif defined(_WIN32)
	return (!isFinite(value) && !isNaN(value));
#else
	return !!isinf(value);
#endif
}

template<typename T> inline bool isFinite(const T &value) {
	UTIL_STATIC_ASSERT(!std::numeric_limits<T>::is_integer);

#if UTIL_CXX11_SUPPORTED
	return std::isfinite(value);
#elif defined(_WIN32)
	return !!::_finite(value);
#else
	return !!isfinite(value);
#endif
}

template<typename T> inline bool isNaN(const T &value) {
	UTIL_STATIC_ASSERT(!std::numeric_limits<T>::is_integer);

#if UTIL_CXX11_SUPPORTED
	return std::isnan(value);
#elif defined(_WIN32)
	return !!::_isnan(value);
#else
	return !!isnan(value);
#endif
}

template<typename T> inline T copySign(const T &value1, const T &value2) {
	UTIL_STATIC_ASSERT(!std::numeric_limits<T>::is_integer);

#if UTIL_CXX11_SUPPORTED
	return std::copysign(value1, value2);
#elif defined(_WIN32)
	return _copysign(value1, value2);
#else
	return ::copysign(value1, value2);
#endif
}

} // namespace util

#endif
