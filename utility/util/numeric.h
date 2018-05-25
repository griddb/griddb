/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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

} 

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

} 

#endif
