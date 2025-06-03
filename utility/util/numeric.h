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
#include <limits>
#include <climits>

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

/**
 * 数値の算術演算ユーティリティ
 */
struct NumberArithmetic {
public:
	template<typename T, typename H> static T add(
			const T &value1, const T &value2, H errorHandler);
	template<typename T, typename H> static T subtract(
			const T &value1, const T &value2, H errorHandler);
	template<typename T, typename H> static T multiply(
			const T &value1, const T &value2, H errorHandler);
	template<typename T, typename H> static T divide(
			const T &value1, const T &value2, H errorHandler);
	template<typename T, typename H> static T remainder(
			const T &value1, const T &value2, H errorHandler);

private:
	struct Add;
	struct Subtract;
	struct Multiply;
	struct Divide;

	struct IntegralRemainder;
	struct FloatingRemainder;

	typedef int64_t LargeIntegralValue;
	typedef int32_t HalfIntegralValue;

	template<typename Op> struct LargeIntegral;
	template<typename Op> struct NormalIntegral;
	template<typename Op> struct Floating;
	template<typename Op> struct Direct;
	struct Unsupported;

	template<typename T> struct Category;
	template<typename Op, typename T> struct Specializer;

	template<typename T, typename H>
	static T errorZeroDivision(const T &ret, const H &errorHandler);
	template<typename T, typename H>
	static T errorValueOverflow(const T &ret, const H &errorHandler);
};

struct NumberArithmetic::Add {
	template<typename T> T operator()(const T &value1, const T &value2) const;
};

struct NumberArithmetic::Subtract {
	template<typename T> T operator()(const T &value1, const T &value2) const;
};

struct NumberArithmetic::Multiply {
	template<typename T> T operator()(const T &value1, const T &value2) const;
};

struct NumberArithmetic::Divide {
	template<typename T> T operator()(const T &value1, const T &value2) const;
};

struct NumberArithmetic::IntegralRemainder {
	template<typename T> T operator()(const T &value1, const T &value2) const;
};

struct NumberArithmetic::FloatingRemainder {
	template<typename T> T operator()(const T &value1, const T &value2) const;
};

template<typename Op> struct NumberArithmetic::LargeIntegral {
	typedef LargeIntegralValue Value;
	typedef HalfIntegralValue HalfValue;

	template<typename H> Value operator()(
			const Value &value1, const Value &value2,
			const H &errorHandler) const;
};

template<typename Op> struct NumberArithmetic::NormalIntegral {
	typedef LargeIntegralValue LargeValue;

	template<typename T, typename H> T operator()(
			const T &value1, const T &value2, const H &errorHandler) const;
};

template<typename Op> struct NumberArithmetic::Floating {
	template<typename T, typename H> T operator()(
			const T &value1, const T &value2, const H &errorHandler) const;

	template<typename T, typename H> static T checkOverflow(
			const T &value1, const T &value2, const T &ret,
			const H &errorHandler);
};

template<typename Op> struct NumberArithmetic::Direct {
	template<typename T, typename H> T operator()(
			const T &value1, const T &value2, const H &errorHandler) const;
};

template<typename T> struct NumberArithmetic::Category {
	enum {
		CATEGORY_BASE_SPECIALIZED = std::numeric_limits<T>::is_specialized,
		CATEGORY_BASE_INTEGER = std::numeric_limits<T>::is_integer,
		CATEGORY_BASE_SIGNED = std::numeric_limits<T>::is_signed,
		CATEGORY_BASE_HAS_INFINITY = std::numeric_limits<T>::has_infinity,

		CATEGORY_INTEGRAL =
				CATEGORY_BASE_SPECIALIZED && CATEGORY_BASE_INTEGER &&
				CATEGORY_BASE_SIGNED,
		CATEGORY_LARGE_INTEGRAL =
				CATEGORY_INTEGRAL && util::IsSame<T, LargeIntegralValue>::VALUE,
		CATEGORY_FLOATING =
				CATEGORY_BASE_SPECIALIZED && CATEGORY_BASE_HAS_INFINITY,
		CATEGORY_SUPPORTED = CATEGORY_INTEGRAL || CATEGORY_FLOATING
	};
};

template<typename Op, typename T> struct NumberArithmetic::Specializer {
	enum {
		CATEGORY_INTEGRAL = Category<T>::CATEGORY_INTEGRAL,
		CATEGORY_LARGE_INTEGRAL = Category<T>::CATEGORY_LARGE_INTEGRAL,
		CATEGORY_SUPPORTED = Category<T>::CATEGORY_SUPPORTED
	};

	typedef typename util::Conditional<CATEGORY_SUPPORTED,
			Direct<Op>, Unsupported>::Type DirectSpecific;

	typedef typename util::Conditional<CATEGORY_SUPPORTED,
			typename util::Conditional<CATEGORY_INTEGRAL,
					typename util::Conditional<CATEGORY_LARGE_INTEGRAL,
							LargeIntegral<Op>, NormalIntegral<Op> >::Type,
					Floating<Op> >::Type,
			Unsupported>::Type FullSpecific;
};

struct ArithmeticErrorHandlers {
	struct Strict;
	struct Checked;
};

struct ArithmeticErrorHandlers::Strict {
	void errorZeroDivision() const;
	void errorValueOverflow() const;
};

struct ArithmeticErrorHandlers::Checked {
public:
	explicit Checked(UtilityException::Code &errorCode);

	void errorZeroDivision() const;
	void errorValueOverflow() const;

	bool isError() const;
	UtilityException::Code getErrorCode() const;

private:
	UtilityException::Code &errorCode_;
};

/**
 * 固定長データ管理型の、double値(IEEE754の64ビット浮動小数点数)の合計値表現
 *
 * ほぼ任意回数(最大2^64回)のdouble値の加減算を繰り返しても、一切桁落ちしない。
 * 無限大、NaNの追加回数を記憶できる。その回数分除外操作をすると、それらの
 * 追加前のdouble値を参照できる。
 */
class FixedDoubleSum {
public:

	FixedDoubleSum();

	void add(double value);

	void remove(double value);

	double get() const;

private:
	enum MergeMode {
		MODE_ADD,
		MODE_SUBTRACT_NORMAL,
		MODE_SUBTRACT_REVERSED
	};

	enum {
		UNIT_BIT = (sizeof(uint64_t) * CHAR_BIT),

		MAX_EXPONENT = (std::numeric_limits<double>::max_exponent - 1),
		MIN_EXPONENT = (std::numeric_limits<double>::min_exponent - 1),
		EXPONENT_RANGE = (MAX_EXPONENT - MIN_EXPONENT + 1),

		MANTISSA_IMPLICIT_BIT = 1,
		MANTISSA_BIT = (
				std::numeric_limits<double>::digits -
				MANTISSA_IMPLICIT_BIT),
		EXPONENT_BIT = (sizeof(double) * CHAR_BIT - MANTISSA_BIT - 1),

		FINITE_VALUE_BIT = (
				UNIT_BIT +
				EXPONENT_RANGE +
				MANTISSA_BIT),

		FINITE_VALUE_SIZE = ((FINITE_VALUE_BIT + UNIT_BIT - 1) / UNIT_BIT)
	};

	typedef uint64_t FiniteValue[FINITE_VALUE_SIZE];
	typedef std::pair<uint64_t, uint64_t> FiniteValuePart;

	uint64_t& getSpecialValueCounter(double value);
	bool findSpecialValue(double &value) const;

	void addFiniteValue(double value);

	MergeMode getMergeMode(
			uint64_t mantissa, uint32_t exponent, bool sign,
			bool &nextSign) const;
	int32_t compareFiniteValue(uint64_t mantissa, uint32_t exponent) const;

	static uint32_t getAmountPosition(
			MergeMode mode, uint32_t exponent, bool lower);
	static uint64_t getAmountPart(
			uint64_t mantissa, uint32_t exponent, uint32_t unitPos);

	static FiniteValuePart getFiniteValuePart(
			const FiniteValue &finiteValue, uint32_t exponent);
	static uint64_t mergeFiniteValuePart(
			MergeMode mode, uint64_t target, uint64_t amount, bool lastCarry,
			bool &nextCarry);
	static uint64_t mergeFiniteValuePartWithLastCarry(
			MergeMode mode, uint64_t target, uint64_t amount, bool &nextCarry);
	static uint32_t resolveExponent(
			const FiniteValue &finiteValue, uint32_t curPos, uint32_t lastExponent);

	static void decomposeValue(
			double value, uint64_t &mantissa, uint32_t &exponent);
	static double makeFiniteValue(
			const FiniteValuePart &valuePart, uint32_t exponent, bool sign);

	static uint64_t valueToBits(double value);
	static double bitsToValue(uint64_t bits);

	static bool isSpecialValue(double value);
	static bool isNegativeZero(double value);

	static void initializeFiniteValue(FiniteValue &value);

	static void errorNegativeCount();

	FiniteValue finiteValue_;

	uint64_t negativeZeroCount_;
	uint64_t positiveInfCount_;
	uint64_t negativeInfCount_;
	uint64_t nanCount_;

	uint32_t exponent_;
	bool sign_;
};

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

#if !UTIL_CXX11_SUPPORTED && !defined(_WIN32)
template<> inline bool isInf(const long double &value) {
	return !!isinfl(value);
}
#endif

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

#if !UTIL_CXX11_SUPPORTED && !defined(_WIN32)
template<> inline bool isFinite(const long double &value) {
	return !!finitel(value);
}
#endif

template<typename T> inline bool isNaN(const T &value) {
	UTIL_STATIC_ASSERT(!std::numeric_limits<T>::is_integer);

#if UTIL_CXX11_SUPPORTED
	return std::isnan(value);
#elif defined(_WIN32)
	return !!::_isnan(value);
#else
	return !!isnanl(value);
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

#if !UTIL_CXX11_SUPPORTED && !defined(_WIN32)
template<> inline long double copySign(
		const long double &value1, const long double &value2) {
	return ::copysignl(value1, value2);
}
template<> inline float copySign(const float &value1, const float &value2) {
	return ::copysignf(value1, value2);
}
#elif defined(_WIN32)
template<> inline float copySign(const float &value1, const float &value2) {
	return static_cast<float>(_copysign(value1, value2));
}
#endif

template<typename T, typename H> inline T NumberArithmetic::add(
		const T &value1, const T &value2, H errorHandler) {
	typedef typename Specializer<Add, T>::FullSpecific Specific;
	return Specific()(value1, value2, errorHandler);
}

template<typename T, typename H> inline T NumberArithmetic::subtract(
		const T &value1, const T &value2, H errorHandler) {
	typedef typename Specializer<Subtract, T>::FullSpecific Specific;
	return Specific()(value1, value2, errorHandler);
}

template<typename T, typename H> inline T NumberArithmetic::multiply(
		const T &value1, const T &value2, H errorHandler) {
	typedef typename Specializer<Multiply, T>::FullSpecific Specific;
	return Specific()(value1, value2, errorHandler);
}

template<typename T, typename H> inline T NumberArithmetic::divide(
		const T &value1, const T &value2, H errorHandler) {
	if (value2 == 0) {
		return errorZeroDivision(T(), errorHandler);
	}

	typedef typename Specializer<Divide, T>::FullSpecific Specific;
	return Specific()(value1, value2, errorHandler);
}

template<typename T, typename H> inline T NumberArithmetic::remainder(
		const T &value1, const T &value2, H errorHandler) {
	if (value2 == 0) {
		return errorZeroDivision(T(), errorHandler);
	}

	typedef typename util::Conditional<Category<T>::CATEGORY_INTEGRAL,
			IntegralRemainder, FloatingRemainder>::Type Op;
	typedef typename Specializer<Op, T>::DirectSpecific Specific;
	return Specific()(value1, value2, errorHandler);
}

template<typename T, typename H>
T NumberArithmetic::errorZeroDivision(const T &ret, const H &errorHandler) {
	errorHandler.errorZeroDivision();
	return ret;
}

template<typename T, typename H>
T NumberArithmetic::errorValueOverflow(const T &ret, const H &errorHandler) {
	errorHandler.errorValueOverflow();
	return ret;
}

template<typename T>
inline T NumberArithmetic::Add::operator()(
		const T &value1, const T &value2) const {
	return value1 + value2;
}

template<typename T>
inline T NumberArithmetic::Subtract::operator()(
		const T &value1, const T &value2) const {
	return value1 - value2;
}

template<typename T>
inline T NumberArithmetic::Multiply::operator()(
		const T &value1, const T &value2) const {
	return value1 * value2;
}

template<typename T>
inline T NumberArithmetic::Divide::operator()(
		const T &value1, const T &value2) const {
	return value1 / value2;
}

template<typename T>
inline T NumberArithmetic::IntegralRemainder::operator()(
		const T &value1, const T &value2) const {
	if (value2 == -1) { 
		return value1 % 1;
	}
	return static_cast<T>(value1 % value2);
}

template<typename T>
inline T NumberArithmetic::FloatingRemainder::operator()(
		const T &value1, const T &value2) const {
#if UTIL_CXX11_SUPPORTED || defined(_WIN32)
	return std::fmod(value1, value2);
#else
	return static_cast<T>(fmod(value1, value2));
#endif
}

#if !UTIL_CXX11_SUPPORTED && !defined(_WIN32)
template<>
inline long double NumberArithmetic::FloatingRemainder::operator()(
		const long double &value1, const long double &value2) const {
	return fmodl(value1, value2);
}
template<>
inline float NumberArithmetic::FloatingRemainder::operator()(
		const float &value1, const float &value2) const {
	return fmodf(value1, value2);
}
#endif

template<typename Op>
template<typename H>
inline typename NumberArithmetic::LargeIntegral<Op>::Value
NumberArithmetic::LargeIntegral<Op>::operator()(
		const Value &value1, const Value &value2,
		const H &errorHandler) const {
	static_cast<void>(value1);
	static_cast<void>(value2);
	static_cast<void>(errorHandler);
	UTIL_STATIC_ASSERT(sizeof(Op) < 0);
	return Value();
}

template<>
template<typename H>
inline NumberArithmetic::LargeIntegral<void>::Value
NumberArithmetic::LargeIntegral<NumberArithmetic::Add>::operator()(
		const Value &value1, const Value &value2,
		const H &errorHandler) const {
	if ((value2 < 0 &&
			std::numeric_limits<Value>::min() - value2 > value1) ||
			(value2 >= 0 &&
			std::numeric_limits<Value>::max() - value2 < value1)) {
		return errorValueOverflow(Value(), errorHandler);
	}

	return value1 + value2;
}

template<>
template<typename H>
inline NumberArithmetic::LargeIntegral<void>::Value
NumberArithmetic::LargeIntegral<NumberArithmetic::Subtract>::operator()(
		const Value &value1, const Value &value2,
		const H &errorHandler) const {
	if ((value2 < 0 &&
			std::numeric_limits<int64_t>::max() + value2 < value1) ||
			(value2 >= 0 &&
			std::numeric_limits<int64_t>::min() + value2 > value1)) {
		return errorValueOverflow(Value(), errorHandler);
	}

	return value1 - value2;
}

template<>
template<typename H>
inline NumberArithmetic::LargeIntegral<void>::Value
NumberArithmetic::LargeIntegral<NumberArithmetic::Multiply>::operator()(
		const Value &value1, const Value &value2,
		const H &errorHandler) const {
	UTIL_STATIC_ASSERT(static_cast<Value>(-3) / 2 == -1);
	UTIL_STATIC_ASSERT(sizeof(Value) == sizeof(HalfValue) * 2);
	UTIL_STATIC_ASSERT(
			std::numeric_limits<Value>::digits + 1 ==
			(std::numeric_limits<HalfValue>::digits + 1) * 2);

	const Value half = static_cast<Value>(1) <<
			(std::numeric_limits<HalfValue>::digits + 1);
	const Value value1H = value1 / half;
	const Value value1L = value1 - value1H * half;
	const Value value2H = value2 / half;
	const Value value2L = value2 - value2H * half;
	do {
		if (value1H == 0) {
			if (value2H == 0) {
				return value1L * value2L;
			}
		}
		else if (value2H != 0) {
			break;
		}

		const Value middle = value1H * value2L + value1L * value2H;
		if (middle > std::numeric_limits<HalfValue>::max() ||
				middle < std::numeric_limits<HalfValue>::min()) {
			break;
		}

		return LargeIntegral<Add>()(
				middle * half, value1L * value2L, errorHandler);
	}
	while (false);

	return errorValueOverflow(Value(), errorHandler);
}

template<>
template<typename H>
inline NumberArithmetic::LargeIntegral<void>::Value
NumberArithmetic::LargeIntegral<NumberArithmetic::Divide>::operator()(
		const Value &value1, const Value &value2,
		const H &errorHandler) const {
	if (value1 == std::numeric_limits<Value>::min() && value2 == -1) {
		return errorValueOverflow(Value(), errorHandler);
	}

	return value1 / value2;
}

template<typename Op>
template<typename T, typename H>
inline T NumberArithmetic::NormalIntegral<Op>::operator()(
		const T &value1, const T &value2, const H &errorHandler) const {
	const LargeValue &largeValue1 = value1;
	const LargeValue &largeValue2 = value2;
	const LargeValue &ret = Op()(largeValue1, largeValue2);

	if (ret < std::numeric_limits<T>::min() ||
			ret > std::numeric_limits<T>::max()) {
		return errorValueOverflow(static_cast<T>(ret), errorHandler);
	}
	return static_cast<T>(ret);
}

template<typename Op>
template<typename T, typename H>
inline T NumberArithmetic::Floating<Op>::operator()(
		const T &value1, const T &value2, const H &errorHandler) const {
	const T &ret = Op()(value1, value2);

	if (ret == std::numeric_limits<T>::infinity() ||
			ret == -std::numeric_limits<T>::infinity()) {
		return Floating<void>::checkOverflow(
				value1, value2, ret, errorHandler);
	}
	return ret;
}

template<typename Op>
template<typename T, typename H>
T NumberArithmetic::Floating<Op>::checkOverflow(
		const T &value1, const T &value2, const T &ret,
		const H &errorHandler) {

	if (value1 != std::numeric_limits<T>::infinity() &&
			value1 != -std::numeric_limits<T>::infinity() &&
			value2 != std::numeric_limits<T>::infinity() &&
			value2 != -std::numeric_limits<T>::infinity()) {
		return errorValueOverflow(ret, errorHandler);
	}
	return ret;
}

template<typename Op>
template<typename T, typename H>
inline T NumberArithmetic::Direct<Op>::operator()(
		const T &value1, const T &value2, const H &errorHandler) const {
	static_cast<void>(errorHandler);
	return Op()(value1, value2);
}

inline void ArithmeticErrorHandlers::Strict::errorZeroDivision() const {
	UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "Divide by 0");
}

inline void ArithmeticErrorHandlers::Strict::errorValueOverflow() const {
	UTIL_THROW_UTIL_ERROR(
			CODE_VALUE_OVERFLOW, "Value overflow in arithmetic operation");
}

inline ArithmeticErrorHandlers::Checked::Checked(
		UtilityException::Code &errorCode) :
		errorCode_(errorCode) {
	errorCode_ = UtilityException::CODE_DEFAULT;
}

inline void ArithmeticErrorHandlers::Checked::errorZeroDivision() const {
	errorCode_ = UtilityException::CODE_ILLEGAL_ARGUMENT;
}

inline void ArithmeticErrorHandlers::Checked::errorValueOverflow() const {
	errorCode_ = UtilityException::CODE_VALUE_OVERFLOW;
}

inline bool ArithmeticErrorHandlers::Checked::isError() const {
	return (errorCode_ != UtilityException::CODE_DEFAULT);
}

inline UtilityException::Code
ArithmeticErrorHandlers::Checked::getErrorCode() const {
	return errorCode_;
}

} 

#endif
