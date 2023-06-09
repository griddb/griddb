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
	@brief Implementation of query functions
*/

#include "query_function.h"
#include "query_function_time.h"


void FunctionUtils::checkTimeField(
		int64_t fieldType, bool precise, bool days) {
	switch (fieldType) {
	case util::DateTime::FIELD_YEAR:
	case util::DateTime::FIELD_MONTH:
	case util::DateTime::FIELD_DAY_OF_MONTH:
	case util::DateTime::FIELD_HOUR:
	case util::DateTime::FIELD_MINUTE:
	case util::DateTime::FIELD_SECOND:
	case util::DateTime::FIELD_MILLISECOND:
	case util::DateTime::FIELD_MICROSECOND:
	case util::DateTime::FIELD_NANOSECOND:
	case util::DateTime::FIELD_DAY_OF_WEEK:
	case util::DateTime::FIELD_DAY_OF_YEAR:
		break;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_QF_INVALID_ARGUMENT,"Unknown timestamp field");
		break;
	}

	if (!precise) {
		switch (fieldType) {
		case util::DateTime::FIELD_MICROSECOND:
		case util::DateTime::FIELD_NANOSECOND:
			GS_THROW_USER_ERROR(
					GS_ERROR_QF_INVALID_ARGUMENT,
					"Specified function cannot accept timestamp field of "
					"milli or nano second");
		default:
			break;
		}
	}

	if (!days) {
		switch (fieldType) {
		case util::DateTime::FIELD_DAY_OF_WEEK:
		case util::DateTime::FIELD_DAY_OF_YEAR:
			GS_THROW_USER_ERROR(
					GS_ERROR_QF_INVALID_ARGUMENT,
					"Specified function cannot accept timestamp field of "
					"day of week or year");
		default:
			break;
		}
	}
}


bool FunctionUtils::BasicValueUtils::isSpaceChar(util::CodePoint c) const {
	return c == 0x20 || (0x09 <= c && c <= 0x0d);
}


void FunctionUtils::NumberArithmetic::ErrorHandler::errorZeroDivision() const {
	try {
		BaseErrorHandler().errorZeroDivision();
		assert(false);
	}
	catch (util::UtilityException &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_QF_DIVIDE_BY_ZERO, e,
				e.getField(util::Exception::FIELD_MESSAGE));
	}
	catch (...) {
		std::exception e;
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void FunctionUtils::NumberArithmetic::ErrorHandler::errorValueOverflow() const {
	try {
		BaseErrorHandler().errorValueOverflow();
		assert(false);
	}
	catch (util::UtilityException &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_QF_VALUE_OVERFLOW, e,
				e.getField(util::Exception::FIELD_MESSAGE));
	}
	catch (...) {
		std::exception e;
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


void FunctionUtils::TimeErrorHandler::errorTimeParse(std::exception &e) const {
	GS_RETHROW_USER_ERROR_CODED(
			GS_ERROR_QF_VALUE_OUT_OF_RANGE, e,
			GS_EXCEPTION_MERGE_MESSAGE(
					e, "Unacceptable time string specified"));
}

void FunctionUtils::TimeErrorHandler::errorTimeFormat(std::exception &e) const {
	GS_RETHROW_USER_ERROR_CODED(
			GS_ERROR_QF_VALUE_OUT_OF_RANGE, e,
			GS_EXCEPTION_MERGE_MESSAGE(
					e, "Unacceptable time value specified"));
}


bool FunctionUtils::NumberArithmetic::middleOfOrderedValues(
		int64_t value1, int64_t value2, int64_t &result) {
	if (value1 <= 0 && value2 >= 0) {
		result = ((value1 + value2) / 2);
		return true;
	}
	const int64_t delta = value2 - value1;
	if (!(delta < 0)) {
		int64_t halfDelta = delta / 2;
		if (value1 < 0 && delta % 2 != 0) {
			halfDelta++;
		}
		result = (value1 + halfDelta);
		return true;
	}
	return false;
}

bool FunctionUtils::NumberArithmetic::middleOfOrderedValues(
		double value1, double value2, double &result) {
	if (value1 <= 0 && value2 >= 0) {
		result = ((value1 + value2) / 2);
		return true;
	}
	const double delta = value2 - value1;
	if (!(delta < 0)) {
		result = (value1 + delta / 2);
		return true;
	}
	return false;
}

int64_t FunctionUtils::NumberArithmetic::interpolateLinear(
		int64_t x1, int64_t y1, int64_t x2, int64_t y2, int64_t x) {
	const int64_t dx = x2 - x1;
	const int64_t dy = y2 - y1;

	const int64_t w = x - x1;
	const int64_t h = (dx != 0 ? (w * dy / dx) : 0);

	return y1 + h;
}

int64_t FunctionUtils::NumberArithmetic::interpolateLinear(
		double x1, int64_t y1, double x2, int64_t y2, double x) {
	const double dx = x2 - x1;
	const double dy = static_cast<double>(y2 - y1);

	const double w = x - x1;
	const int64_t h = static_cast<int64_t>(w * dy / dx);

	return y1 + h;
}

double FunctionUtils::NumberArithmetic::interpolateLinear(
		double x1, double y1, double x2, double y2, double x) {
	const double dx = x2 - x1;
	const double dy = y2 - y1;

	const double w = x - x1;
	const double h = w * dy / dx;

	return y1 + h;
}

int32_t FunctionUtils::NumberArithmetic::compareDoubleLongPrecise(
		double value1, int64_t value2) {
	if (value1 < static_cast<double>(value2)) {
		return -1;
	}
	else if (value1 > static_cast<double>(value2) || util::isNaN(value1)) {
		return 1;
	}
	else if (static_cast<int64_t>(value1) < value2) {
		return -1;
	}
	else if (static_cast<int64_t>(value1) > value2) {
		return 1;
	}
	return 0;
}


const TimeFunctions::Strftime::Formatter *const
TimeFunctions::Strftime::Formatter::FORMATTER_LIST = createFormatterList();
