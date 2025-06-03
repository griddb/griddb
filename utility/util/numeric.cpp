/*
    Copyright (c) 2025 TOSHIBA Digital Solutions Corporation
    
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
	@brief Implementation of Numeric Utility function
*/

#include "util/numeric.h"
#include "util/code.h"

namespace util {


FixedDoubleSum::FixedDoubleSum() :
		negativeZeroCount_(0),
		positiveInfCount_(0),
		negativeInfCount_(0),
		nanCount_(0),
		exponent_(0),
		sign_(false) {
	initializeFiniteValue(finiteValue_);
}

void FixedDoubleSum::add(double value) {
	if (isSpecialValue(value)) {
		uint64_t &count = getSpecialValueCounter(value);
		++count;
	}
	else {
		addFiniteValue(value);
	}
}

void FixedDoubleSum::remove(double value) {
	if (isSpecialValue(value)) {
		uint64_t &count = getSpecialValueCounter(value);
		if (count <= 0) {
			errorNegativeCount();
			return;
		}
		--count;
	}
	else {
		addFiniteValue(-value);
	}
}

double FixedDoubleSum::get() const {
	double specialValue;
	if (findSpecialValue(specialValue)) {
		return specialValue;
	}
	else {
		const FiniteValuePart &valuePart =
				getFiniteValuePart(finiteValue_, exponent_);
		return makeFiniteValue(valuePart, exponent_, sign_);
	}
}

uint64_t& FixedDoubleSum::getSpecialValueCounter(double value) {
	assert(isSpecialValue(value));

	if (value > 0) {
		return positiveInfCount_;
	}
	else if (value < 0) {
		return negativeInfCount_;
	}
	else if (value == 0) {
		return negativeZeroCount_;
	}
	else {
		return nanCount_;
	}
}

bool FixedDoubleSum::findSpecialValue(double &value) const {
	const bool withNan = (nanCount_ > 0);
	const bool withPositiveInf = (positiveInfCount_ > 0);
	const bool withNegativeInf = (negativeInfCount_ > 0);

	if (withNan || withPositiveInf || withNegativeInf) {
		if (withNan || (withPositiveInf && withNegativeInf)) {
			value = std::numeric_limits<double>::quiet_NaN();
		}
		else if (withPositiveInf) {
			value = std::numeric_limits<double>::infinity();
		}
		else {
			value = -std::numeric_limits<double>::infinity();
		}
		return true;
	}
	else if (exponent_ == 0 && negativeZeroCount_ > 0) {
		value = -0.0;
		return true;
	}
	else {
		value = 0;
		return false;
	}
}

void FixedDoubleSum::addFiniteValue(double value) {
	assert(isFinite(value));

	uint64_t mantissa;
	uint32_t exponent;
	decomposeValue(value, mantissa, exponent);
	if (mantissa == 0) {
		return;
	}

	const bool sign = (value < 0);
	bool nextSign;
	const MergeMode mode =
			getMergeMode(mantissa, exponent, sign, nextSign);

	uint32_t pos = getAmountPosition(mode, exponent, true);
	const uint32_t tailPos = getAmountPosition(mode, exponent, false);
	bool lastCarry = false;
	for (;; pos++) {
		const uint64_t amount = getAmountPart(mantissa, exponent, pos);
		uint64_t &dest = finiteValue_[pos];

		bool nextCarry;
		dest = mergeFiniteValuePart(mode, dest, amount, lastCarry, nextCarry);

		if (pos >= tailPos && !nextCarry) {
			break;
		}
		lastCarry = nextCarry;
	}

	exponent_ = resolveExponent(finiteValue_, pos, exponent_);
	sign_ = nextSign;
}

FixedDoubleSum::MergeMode FixedDoubleSum::getMergeMode(
		uint64_t mantissa, uint32_t exponent, bool sign,
		bool &nextSign) const {
	if (!sign_ == !sign) {
		nextSign = sign;
		return MODE_ADD;
	}
	else {
		const int32_t comp = compareFiniteValue(mantissa, exponent);
		const bool reversed = (comp < 0);
		nextSign = (sign ? (comp < 0) : (comp > 0));
		return (reversed ? MODE_SUBTRACT_REVERSED : MODE_SUBTRACT_NORMAL);
	}
}

int32_t FixedDoubleSum::compareFiniteValue(
		uint64_t mantissa, uint32_t exponent) const {
	if (exponent_ != exponent) {
		return (exponent_ < exponent ? -1 : 1);
	}

	const MergeMode mode = MODE_ADD;
	uint32_t pos = getAmountPosition(mode, exponent, false);
	const uint32_t tailPos = 0;
	for (;; pos--) {
		const uint64_t amount = getAmountPart(mantissa, exponent, pos);
		const uint64_t target = finiteValue_[pos];
		if (target != amount) {
			return (target < amount ? -1 : 1);
		}
		if (pos <= tailPos) {
			break;
		}
	}

	return 0;
}

uint32_t FixedDoubleSum::getAmountPosition(
		MergeMode mode, uint32_t exponent, bool lower) {
	if (mode == MODE_SUBTRACT_REVERSED && lower) {
		return 0;
	}

	const uint32_t basePos = exponent / UNIT_BIT;
	if (exponent >= UNIT_BIT &&
			exponent % UNIT_BIT <= MANTISSA_BIT && lower) {
		return basePos - 1;
	}
	else {
		return basePos;
	}
}

uint64_t FixedDoubleSum::getAmountPart(
		uint64_t mantissa, uint32_t exponent, uint32_t unitPos) {
	const uint32_t mod = (exponent % UNIT_BIT);
	const uint32_t size = (MANTISSA_BIT + 1);
	const MergeMode mode = MODE_ADD;
	if (unitPos == getAmountPosition(mode, exponent, true)) {
		if (exponent < size) {
			return mantissa;
		}
		else if (mod < size) {
			return mantissa << (mod + (UNIT_BIT - size));
		}
		else {
			return mantissa << (mod - size);
		}
	}
	else if (unitPos == getAmountPosition(mode, exponent, false)) {
		return mantissa >> (size - mod);
	}
	else {
		return 0;
	}
}

FixedDoubleSum::FiniteValuePart FixedDoubleSum::getFiniteValuePart(
		const FiniteValue &finiteValue, uint32_t exponent) {
	const uint32_t mod = (exponent % UNIT_BIT);
	const uint32_t size = (MANTISSA_BIT + 1);
	const MergeMode mode = MODE_ADD;

	const uint32_t lowPos = getAmountPosition(mode, exponent, true);
	const uint32_t highPos = getAmountPosition(mode, exponent, false);

	uint64_t low;
	{
		const uint64_t value = finiteValue[lowPos];
		if (exponent < size) {
			low = value;
		}
		else if (mod < size) {
			low = value >> (mod + (UNIT_BIT - size));
		}
		else {
			low = value >> (mod - size);
		}
	}

	uint64_t high;
	if (highPos > lowPos) {
		const uint64_t value = finiteValue[highPos];
		high = value << (size - mod);
	}
	else {
		high = 0;
	}

	return std::make_pair(high, low);
}

uint64_t FixedDoubleSum::mergeFiniteValuePart(
		MergeMode mode, uint64_t target, uint64_t amount, bool lastCarry,
		bool &nextCarry) {
	if (lastCarry) {
		return mergeFiniteValuePartWithLastCarry(
				mode, target, amount, nextCarry);
	}

	const uint64_t max = std::numeric_limits<uint64_t>::max();
	if (mode == MODE_ADD) {
		if (target <= max - amount) {
			nextCarry = false;
			return target + amount;
		}
		else {
			nextCarry = true;
			return max - (max - target + 1) - (max - amount + 1) + 1;
		}
	}
	else {
		uint64_t value1 = target;
		uint64_t value2 = amount;
		if (mode == MODE_SUBTRACT_REVERSED) {
			std::swap(value1, value2);
		}

		if (value1 < value2) {
			nextCarry = true;
			return max - value2 + value1 + 1;
		}
		else {
			nextCarry = false;
			return value1 - value2;
		}
	}
}

uint64_t FixedDoubleSum::mergeFiniteValuePartWithLastCarry(
		MergeMode mode, uint64_t target, uint64_t amount, bool &nextCarry) {
	uint64_t modTarget = target;
	uint64_t modAmount = amount;
	if (mode == MODE_SUBTRACT_REVERSED) {
		const uint64_t max = std::numeric_limits<uint64_t>::max();
		if (target < max) {
			modTarget++;
		}
		else if (amount > 0) {
			modAmount--;
		}
		else {
			nextCarry = true;
			return 0;
		}
	}
	else {
		modAmount++;
	}

	const bool lastCarry = false;
	return mergeFiniteValuePart(
			mode, modTarget, modAmount, lastCarry, nextCarry);
}

uint32_t FixedDoubleSum::resolveExponent(
		const FiniteValue &finiteValue, uint32_t curPos, uint32_t lastExponent) {
	const uint32_t lastPos = (lastExponent + UNIT_BIT - 1) / UNIT_BIT;
	uint32_t pos = std::max(curPos, lastPos);
	for (; pos > 0; pos--) {
		if (finiteValue[pos] != 0) {
			break;
		}
	}

	return (UNIT_BIT * pos) + (UNIT_BIT - nlz(finiteValue[pos]));
}

void FixedDoubleSum::decomposeValue(
		double value, uint64_t &mantissa, uint32_t &exponent) {
	const uint64_t bits = valueToBits(value);
	mantissa = bits & ((UINT64_C(1) << MANTISSA_BIT) - 1);
	exponent = (bits >> MANTISSA_BIT) & ((UINT64_C(1) << EXPONENT_BIT) - 1);
	if (exponent == 0) {
		exponent = UNIT_BIT - nlz(mantissa);
	}
	else {
		mantissa |= (UINT64_C(1) << MANTISSA_BIT);
		exponent += (MANTISSA_BIT + 1);
	}
}

double FixedDoubleSum::makeFiniteValue(
		const FiniteValuePart &valuePart, uint32_t exponent, bool sign) {
	const uint64_t high = valuePart.first;
	const uint64_t low = valuePart.second;

	if (exponent == 0) {
		return 0;
	}

	const uint64_t rawExponent = (exponent <= MANTISSA_BIT ?
			UINT64_C(0) : exponent - (MANTISSA_BIT + 1));
	if (rawExponent > EXPONENT_RANGE) {
		if (sign) {
			return -std::numeric_limits<double>::infinity();
		}
		else {
			return std::numeric_limits<double>::infinity();
		}
	}

	const uint64_t mantissa =
			(high | low) & ((UINT64_C(1) << MANTISSA_BIT) - 1);
	const uint64_t exponentBits = rawExponent << MANTISSA_BIT;
	const uint64_t signBits = (sign ? (UINT64_C(1) << (UNIT_BIT - 1)) : 0);

	const uint64_t bits = (signBits | exponentBits | mantissa);
	return bitsToValue(bits);
}

uint64_t FixedDoubleSum::valueToBits(double value) {
	uint64_t bits;
	UTIL_STATIC_ASSERT(sizeof(bits) == sizeof(value));
	memcpy(&bits, &value, sizeof(bits));
	return bits;
}

double FixedDoubleSum::bitsToValue(uint64_t bits) {
	double value;
	UTIL_STATIC_ASSERT(sizeof(bits) == sizeof(value));
	memcpy(&value, &bits, sizeof(bits));
	return value;
}

bool FixedDoubleSum::isSpecialValue(double value) {
	return !isFinite(value) || isNegativeZero(value);
}

bool FixedDoubleSum::isNegativeZero(double value) {
	return value == 0 && (std::numeric_limits<double>::infinity() / value < 0);
}

void FixedDoubleSum::initializeFiniteValue(FiniteValue &value) {
	for (size_t i = 0; i < FINITE_VALUE_SIZE; i++) {
		value[i] = 0;
	}
}

void FixedDoubleSum::errorNegativeCount() {
	UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "Unexpected special value");
}

} 
