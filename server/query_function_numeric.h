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
	@brief Definition of query functions for numeric values
*/

#ifndef QUERY_FUNCTION_NUMERIC_H_
#define QUERY_FUNCTION_NUMERIC_H_

#include "query_function.h"
#include <cmath>


struct NumericFunctions {
	struct Abs;
	struct HexToDec;
	struct Log;
	struct Round;
	struct Sqrt;
	struct Trunc;
};

struct NumericFunctions::Abs {
	template<typename C> int64_t operator()(C &cxt, int64_t value);
	template<typename C> double operator()(C &cxt, double value);
};

struct NumericFunctions::HexToDec {
	template<typename C, typename R>
	int64_t operator()(C &cxt, R &hex);

private:
	template<typename C, typename R>
	void skipHeadSpace(C &cxt, R &hex);

	template<typename C, typename R>
	void codePointListOfHexToDecArray(C &cxt, R &hex, int8_t (&decArray)[16], int8_t &digitNum);

	template<typename C, typename R, typename CP>
	bool isTailSpace(C &cxt, R &hex, CP &hexCodePoint);

	template<typename C, typename R, typename CP>
	int8_t codePointToInt(C &cxt, R &hex, CP &hexCodePoint, bool &isSpace);
};

struct NumericFunctions::Log {
	template<typename C>
	double operator()(C &cxt, double base, double antilogarithm);
};

struct NumericFunctions::Round {
public:
	template<typename C>
	int64_t operator()(C &cxt, int64_t value1, int64_t value2 = 0);

	template<typename C>
	double operator()(C &cxt, double value1, int64_t value2 = 0);

private:
	static bool checkBounds(double doubleValue, int64_t digit);
	static std::pair<double, double> digitToFinitePowers(int64_t digit);

	static int32_t digits10();
	static int32_t maxExponent10();
};

struct NumericFunctions::Sqrt {
	template<typename C>
	double operator()(C &cxt, double value);
};

struct NumericFunctions::Trunc {
	template<typename C>
	double operator()(C &cxt, double value);

	template<typename C>
	double operator()(C &cxt, double value, int64_t radix);

	template<typename C>
	int64_t operator()(C &cxt, int64_t value);

	template<typename C>
	int64_t operator()(C &cxt, int64_t value, int64_t radix);
};



template<typename C>
inline int64_t NumericFunctions::Abs::operator()(C &cxt, int64_t value) {
	static_cast<void>(cxt);

	if (value == std::numeric_limits<int64_t>::min()) {
		GS_THROW_USER_ERROR(GS_ERROR_QF_VALUE_OVERFLOW,
				"Integer overflow");
	}
	if (value < 0) {
		return -value;
	}
	else {
		return value;
	}
}

template<typename C>
inline double NumericFunctions::Abs::operator()(C &cxt, double value) {
	static_cast<void>(cxt);
	return std::abs(value);
}

template<typename C, typename R>
int64_t NumericFunctions::HexToDec::operator()(C &cxt, R &hex) {
	skipHeadSpace(cxt, hex);

	int8_t decArray[16]; 
	int8_t digitNum = 0; 
	codePointListOfHexToDecArray(cxt, hex, decArray, digitNum);

	if (digitNum == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_QF_INVALID_ARGUMENT,
			"The argument doesn't contain any valid hex value.");
	}

	if (digitNum == 16 && decArray[0] >= 8) {
		GS_THROW_USER_ERROR(GS_ERROR_QF_VALUE_OVERFLOW, "");
	}

	int64_t result = 0;
	for (int8_t i = 0; i < digitNum; ++i) {
		result = result * 16 + decArray[i];
	}

	return result;
}

template<typename C, typename R>
void NumericFunctions::HexToDec::skipHeadSpace(C &cxt, R &hex) {
	typedef typename R::CodeType HexCodePoint;
	HexCodePoint hexCodePoint;
	while (hex.nextCode(&hexCodePoint)) {
		if (cxt.getValueUtils().isSpaceChar(hexCodePoint)) {
			continue;
		}
		hex.prevCode();
		return;
	}
}

template<typename C, typename R>
void NumericFunctions::HexToDec::codePointListOfHexToDecArray(C &cxt, R &hex, int8_t(&decArray)[16], int8_t &digitNum) {
	typedef typename R::CodeType HexCodePoint;
	HexCodePoint hexCodePoint;
	bool spaceCodePointIsRead = false;

	while (hex.nextCode(&hexCodePoint)) {
		if (isTailSpace(cxt, hex, hexCodePoint)) {
			break;
		}
		int8_t hexVal = codePointToInt(cxt, hex, hexCodePoint, spaceCodePointIsRead);
		if (digitNum == 16) {
			GS_THROW_USER_ERROR(GS_ERROR_QF_VALUE_OVERFLOW, "");
		}
		decArray[digitNum] = hexVal;
		++digitNum;
	}
}

template<typename C, typename R, typename CP>
bool NumericFunctions::HexToDec::isTailSpace(C &cxt, R &hex, CP &hexCodePoint) {
	if (!cxt.getValueUtils().isSpaceChar(hexCodePoint)) {
		return false;
	}

	while (hex.nextCode(&hexCodePoint)) {
		if (!cxt.getValueUtils().isSpaceChar(hexCodePoint)) {
			GS_THROW_USER_ERROR(GS_ERROR_QF_INVALID_ARGUMENT, "");
		}
	}
	
	return true;
}

template<typename C, typename R, typename CP>
int8_t NumericFunctions::HexToDec::codePointToInt(C &cxt, R &hex, CP &hexCodePoint, bool &spaceCodePointIsRead) {
	static_cast<void>(cxt);
	static_cast<void>(hex);
	static_cast<void>(spaceCodePointIsRead);
	if (0x30 <= hexCodePoint && hexCodePoint <= 0x39) {
		return static_cast<int8_t>(hexCodePoint - 0x30);
	}

	if (0x41 <= hexCodePoint && hexCodePoint <= 0x46) {
		return static_cast<int8_t>(10 + (hexCodePoint - 0x41));
	}

	if (0x61 <= hexCodePoint && hexCodePoint <= 0x66) {
		return static_cast<int8_t>(10 + (hexCodePoint - 0x61));
	}

	GS_THROW_USER_ERROR(
		GS_ERROR_QF_INVALID_ARGUMENT,
		"The argument contains an invalid character. Code Point:" << hexCodePoint);
}

template<typename C>
inline double NumericFunctions::Log::operator()(
		C &cxt, double base, double antilogarithm) {
	static_cast<void>(cxt);

	if (base <= 0 || base == 1) {
		GS_THROW_USER_ERROR(
				GS_ERROR_QF_VALUE_OUT_OF_RANGE,
				"The base of the LOG function is out of range. "
				"base:" << base);
	}
	if (antilogarithm <= 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_QF_VALUE_OUT_OF_RANGE,
				"The antilogarithm of the LOG function is out of range. "
				"antilogarithm:" << antilogarithm);
	}

	return FunctionUtils::NumberArithmetic::divide(
			std::log(antilogarithm), std::log(base));
}


template<typename C>
inline int64_t NumericFunctions::Round::operator()(
		C &cxt, int64_t value1, int64_t value2) {
	static_cast<void>(cxt);
	static_cast<void>(value2);
	return value1;
}

template<typename C>
inline double NumericFunctions::Round::operator()(
		C &cxt, double value1, int64_t value2) {
	static_cast<void>(cxt);

	int64_t digit = value2;
	if (value2 < 0) {
		digit = 0;
	}

	const double doubleValue = value1;
	if (!checkBounds(doubleValue, digit)) {
		return doubleValue;
	}


	const std::pair<double, double> &base = digitToFinitePowers(digit);

	const double rounded = (doubleValue < 0 ?
			std::ceil(doubleValue * base.first * base.second - 0.5) :
			std::floor(doubleValue * base.first * base.second + 0.5)) /
			base.second / base.first;

	return rounded;
}

inline bool NumericFunctions::Round::checkBounds(
		double doubleValue, int64_t digit) {
	const int largeDigit = maxExponent10() + digits10() * 2;
	if (digit > largeDigit) {
		return false;
	}

	const double absValue =
			(doubleValue < 0 ? -doubleValue : doubleValue);

	const uint64_t largeValueBase =
			static_cast<uint64_t>(1) << (sizeof(uint64_t) * CHAR_BIT - 1);
	const double largeValue = 1.0 * largeValueBase * (1 << 10);
	if (absValue > largeValue) {
		return false;
	}

	const double middleValue = 1.0 / largeValue;
	if (absValue > middleValue) {
		const int middleDigit = largeDigit - digits10() * 4;
		if (digit > middleDigit) {
			return false;
		}
	}

	return true;
}

inline std::pair<double, double>
NumericFunctions::Round::digitToFinitePowers(int64_t digit) {
	assert(digit >= 0);
	assert(digit <= maxExponent10() + digits10() * 2);

	const int smallDigit = digits10() * 3;
	if (digit > smallDigit) {
		return std::make_pair(
				std::pow(10.0, static_cast<int>(digit - smallDigit)),
				std::pow(10.0, smallDigit));
	}
	else {
		return std::make_pair(std::pow(10.0, static_cast<int>(digit)), 1.0);
	}
}

inline int32_t NumericFunctions::Round::digits10() {
	return std::numeric_limits<double>::digits10;
}

inline int32_t NumericFunctions::Round::maxExponent10() {
	return std::numeric_limits<double>::max_exponent10;
}

template<typename C>
inline double NumericFunctions::Sqrt::operator()(C &cxt, double value) {
	static_cast<void>(cxt);

	if (value < 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_QF_VALUE_OUT_OF_RANGE,
			"The value of the SQRT function is out of range. "
			"value:" << value);
	}

	return std::sqrt(value);
}

template<typename C>
inline double NumericFunctions::Trunc::operator()(C &cxt, double value) {
	return (*this)(cxt, value, 0);
}

template<typename C>
inline double NumericFunctions::Trunc::operator()(
		C &cxt, double value, int64_t radix) {
	static_cast<void>(cxt);

	if (value == 0.0) {
		return 0.0;
	}

	if (radix > std::numeric_limits<double>::max_exponent10
			|| radix < std::numeric_limits<double>::min_exponent10) {
		GS_THROW_USER_ERROR(
				GS_ERROR_QF_VALUE_OUT_OF_RANGE,
				"The radix value is out of range. radix:" << radix);
	}

	double integerPart = 0.0;
	double floatingPart = std::modf(value, &integerPart);
	int32_t digit = 0;

	if (integerPart != 0.0) {
		digit = static_cast<int32_t>(std::log10(std::abs(integerPart)));
	}
	else {
		double tmp = floatingPart * std::pow(10.0, std::numeric_limits<double>::max_exponent10);
		digit = static_cast<int32_t>(
				std::log10(std::abs(tmp)) - 1 - std::numeric_limits<double>::max_exponent10);
	}

	if (digit + radix >= std::numeric_limits<double>::max_exponent10
			|| digit + radix < std::numeric_limits<double>::min_exponent10) {
		GS_THROW_USER_ERROR(
				GS_ERROR_QF_VALUE_OUT_OF_RANGE,
				"The combination of value and radix is out of range. "
				"value: " << value << " radix: " << radix);
	}

	double shiftedValue = FunctionUtils::NumberArithmetic::multiply(
			value, std::pow(10.0, static_cast<double>(radix)));

	double roundedValue = 0.0;
	if (value > 0) {
		roundedValue = std::floor(shiftedValue);
	}
	else {
		roundedValue = std::ceil(shiftedValue);
	}

	return FunctionUtils::NumberArithmetic::multiply(
			roundedValue, std::pow(10.0, static_cast<double>(-radix)));
}

template<typename C>
int64_t NumericFunctions::Trunc::operator()(C &cxt, int64_t value) {
	return (*this)(cxt, value, 0);
}

template<typename C>
int64_t NumericFunctions::Trunc::operator()(
		C &cxt, int64_t value, int64_t radix) {
	static_cast<void>(cxt);

	if (radix >= 0) {
		return value;
	}

	if ((-radix) > std::numeric_limits<int64_t>::digits10) {
		return 0;
	}

	int64_t powedValue = 1;
	for (int8_t i = 0; i < (-radix); ++i) {
		powedValue *= 10;
	}
	return value - (value % powedValue);
}

#endif
