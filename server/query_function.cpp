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


const TimeFunctions::Strftime::Formatter *const
TimeFunctions::Strftime::Formatter::FORMATTER_LIST = createFormatterList();
