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
/*
    Copyright (c) 2008, Yubin Lim(purewell@gmail.com).
    All rights reserved.

    Redistribution and use in source and binary forms, with or without 
    modification, are permitted provided that the following conditions 
    are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the 
      documentation and/or other materials provided with the distribution.
    * Neither the name of the Purewell nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "util/time.h"
#include "util/code.h"
#include "util/os.h"
#include <iomanip>
#include <limits>
#include <cassert>

namespace util {

#if UTIL_FAILURE_SIMULATION_ENABLED
#define UTIL_FAILURE_SIMULATION_TIME_FILTER(time) \
		DateTimeFailureSimulator::filterTime(time)
#define UTIL_FAILURE_SIMULATION_CLOCK_FILTER(clock) \
		DateTimeFailureSimulator::filterClock(clock)
#define UTIL_FAILURE_SIMULATION_TRACE_COVERAGE(posName) \
		DateTimeFailureSimulator::traceCoverage( \
				DateTimeFailureSimulator::posName)
#else
#define UTIL_FAILURE_SIMULATION_TIME_FILTER(time) (time)
#define UTIL_FAILURE_SIMULATION_CLOCK_FILTER(clock) (clock)
#define UTIL_FAILURE_SIMULATION_TRACE_COVERAGE(posName)
#endif

#ifndef UTIL_DATE_TIME_DIFF_WITH_SUB_FIELDS
#define UTIL_DATE_TIME_DIFF_WITH_SUB_FIELDS 1
#endif



struct CharBufferUtils {
	static bool write(char8_t *&it, char8_t *end, const char8_t *str);
};

bool CharBufferUtils::write(char8_t *&it, char8_t *end, const char8_t *str) {
	for (const char8_t *strIt = str; *strIt != '\0'; ++strIt) {
		if (it == end) {
			return false;
		}
		*it = *strIt;
		++it;
	}
	return true;
}


TimeZone::TimeZone() :
		offsetMillis_(Constants::emptyOffsetMillis()) {
}

TimeZone TimeZone::getLocalTimeZone() {
	return TimeZone::ofOffsetMillis(getLocalOffsetMillis());
}

TimeZone TimeZone::getUTCTimeZone() {
	return TimeZone::ofOffsetMillis(0);
}

TimeZone TimeZone::ofOffsetMillis(Offset millis) {
	TimeZone zone;
	zone.setOffsetMillis(millis);
	return zone;
}

bool TimeZone::isEmpty() const {
	return (offsetMillis_ == Constants::emptyOffsetMillis());
}

bool TimeZone::checkRange(bool throwOnError) const {
	const Offset range = Constants::offsetMillisRange();
	if (offsetMillis_ < -range || range < offsetMillis_) {
		if (throwOnError) {
			UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
					"Time zone offset is out of range or not specified");
		}
		return false;
	}

	return true;
}

TimeZone::Offset TimeZone::getOffsetMillis() const {
	return offsetMillis_;
}

void TimeZone::setOffsetMillis(Offset millis) {
	offsetMillis_ = millis;
}

void TimeZone::format(std::ostream &s) const {
	try {
		char8_t buf[MAX_FORMAT_SIZE];
		const size_t size = format(buf, sizeof(buf));
		s.write(buf, static_cast<std::streamsize>(size));
	}
	catch (...) {
		s.setstate(std::ios::failbit);
		if (s.exceptions() & std::ios::failbit) {
			throw;
		}
		return;
	}
}

size_t TimeZone::format(char8_t *buf, size_t size) const {
	char8_t *it = buf;
	char8_t *end = it + size;

	if (isEmpty()) {
		CharBufferUtils::write(it, end, "(empty)");
	}
	else if (!checkRange(false)) {
		CharBufferUtils::write(it, end, "(error)");
	}
	else if (offsetMillis_== 0) {
		CharBufferUtils::write(it, end, "Z");
	}
	else {
		if (offsetMillis_ >= 0) {
			CharBufferUtils::write(it, end, "+");
		}
		else {
			CharBufferUtils::write(it, end, "-");
		}

		const Offset absOffset =
				offsetMillis_ >= 0 ? offsetMillis_ : -offsetMillis_;

		const Offset hour = (absOffset / (60 * 60 * 1000)) % 24;
		const Offset minute = (absOffset / (60 * 1000)) % 60;

		TinyLexicalIntConverter converter;
		converter.minWidth_ = 2;
		converter.maxWidth_ = 2;

		converter.format(it, end, static_cast<uint32_t>(hour));
		CharBufferUtils::write(it, end, ":");
		converter.format(it, end, static_cast<uint32_t>(minute));
	}

	return static_cast<size_t>(it - buf);
}

bool TimeZone::parse(const char8_t *buf, size_t size, bool throwOnError) {
	const char8_t *it = buf;
	const char8_t *end = it + size;

	Offset millis = 0;
	do {
		if (it == end) {
			break;
		}

		if (*it == '+' || *it == '-') {
			const int32_t sign = (*it == '+' ? 1 : -1);
			++it;

			TinyLexicalIntConverter converter;
			converter.minWidth_ = 2;
			converter.maxWidth_ = 2;

			uint32_t hour;
			if (!converter.parse(it, end, hour) || hour > 23) {
				break;
			}

			if (it != end && *it == ':') {
				++it;
			}

			if (it == end) {
				break;
			}

			uint32_t minute;
			if (!converter.parse(it, end, minute) || minute > 59) {
				break;
			}

			millis = sign *
					static_cast<Offset>((hour * 60 + minute) * 60 * 1000);
		}
		else if (*it == 'Z') {
			++it;
		}
		else {
			break;
		}

		if (it != end) {
			break;
		}

		offsetMillis_ = millis;
		return true;
	}
	while (false);

	if (throwOnError) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER, "Failed to parse");
	}
	return false;
}

TimeZone::Offset TimeZone::getLocalOffsetMillis() {
	static uint32_t absOffset = 0;
	const int32_t diff = 24 * 60 * 60 * 1000;

	if (absOffset == 0) {
		const Offset base = detectLocalOffsetMillis();
		assert(-diff < base && base < diff);
		absOffset = static_cast<uint32_t>(base + diff);
	}

	return static_cast<Offset>(absOffset) - diff;
}

TimeZone::Offset TimeZone::detectLocalOffsetMillis() {
	const int64_t localTimeMillis = 24 * 60 * 60 * 1000; 
	const bool asLocalTimeZone = true;
#ifdef _WIN32
	const bool dstIgnored = true;
	SYSTEMTIME time = FileLib::getSystemTime(
			FileLib::getFileTime(localTimeMillis), false, dstIgnored);
	const int64_t timeMillis = FileLib::getUnixTime(
			FileLib::getFileTime(time, asLocalTimeZone, dstIgnored));
#else
	tm time = FileLib::getTM(localTimeMillis, false);
	assert(time.tm_isdst == 0);
	time.tm_yday = 0;
	time.tm_wday = 0;
	time.tm_isdst = 0;
	const int32_t milliSecond = 0;
	const int64_t timeMillis =
			FileLib::getUnixTime(time, milliSecond, asLocalTimeZone);
#endif

	const Offset offset = localTimeMillis - timeMillis;
	const Offset range = Constants::offsetMillisRange();
	if (offset <= -range || range <= offset) {
		UTIL_THROW_UTIL_ERROR(
				CODE_INVALID_STATUS, "Unexpected time zone offset");
	}

	return offset;
}


inline TimeZone::Offset TimeZone::Constants::emptyOffsetMillis() {
	return std::numeric_limits<Offset>::max();
}

inline TimeZone::Offset TimeZone::Constants::offsetMillisRange() {
	return 24 * 60 * 60 * 1000;
}


const int64_t DateTime::INITIAL_UNIX_TIME = static_cast<int64_t>(0);

const int32_t DateTime::EPOCH_DAY_OF_WEEK = 5; 

DateTime::DateTime(const char8_t *str, bool fractionTrimming) :
		unixTimeMillis_(INITIAL_UNIX_TIME) {
	if (!parse(str, *this, fractionTrimming)) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Parse failed (" << str << ")");
	}
}

DateTime::DateTime(
		int32_t year,
		int32_t month,
		int32_t monthDay,
		int32_t hour,
		int32_t minute,
		int32_t second,
		int32_t milliSecond,
		bool asLocalTimeZone) {
	setFields(year, month, monthDay, hour, minute, second, milliSecond,
			asLocalTimeZone);
}

void DateTime::setUnixTime(int64_t unixTimeMillis, const Option &option) {
	checkUnixTimeBounds(unixTimeMillis, option);
	unixTimeMillis_ = unixTimeMillis;
}

void DateTime::getFields(
		FieldData &fieldData, const ZonedOption &option) const {
	checkUnixTimeBounds(unixTimeMillis_, option.baseOption_);

	const bool asLocalTimeZone = false;

	int64_t offsetMillis = 0;
	if (!option.zone_.isEmpty()) {
		option.zone_.checkRange(true);
		offsetMillis = option.zone_.getOffsetMillis();
	}
	else if (option.asLocalTimeZone_) {
		offsetMillis = getLocalOffsetMillis();
	}

	bool biased = false;
	int64_t modTimeMillis = unixTimeMillis_ + offsetMillis;
	if (modTimeMillis < 0) {
		const int64_t bias = 24 * 60 * 60 * 1000;
		if (modTimeMillis > -bias) {
			modTimeMillis += bias;
			biased = true;
		}
	}

#ifdef _WIN32
	const bool dstIgnored = true;
	SYSTEMTIME time = FileLib::getSystemTime(
			FileLib::getFileTime(modTimeMillis), asLocalTimeZone, dstIgnored);
	fieldData.year_ = static_cast<int32_t>(time.wYear);
	fieldData.month_ = static_cast<int32_t>(time.wMonth);
	fieldData.monthDay_ = static_cast<int32_t>(time.wDay);
	fieldData.hour_ = static_cast<int32_t>(time.wHour);
	fieldData.minute_ = static_cast<int32_t>(time.wMinute);
	fieldData.second_ = static_cast<int32_t>(time.wSecond);
	fieldData.milliSecond_ = static_cast<int32_t>(time.wMilliseconds);
#else
	tm time = FileLib::getTM(modTimeMillis, asLocalTimeZone);
	fieldData.year_ = time.tm_year + 1900;
	fieldData.month_ = time.tm_mon + 1;
	fieldData.monthDay_ = time.tm_mday;
	fieldData.hour_ = time.tm_hour;
	fieldData.minute_ = time.tm_min;
	fieldData.second_ = time.tm_sec;
	fieldData.milliSecond_ = static_cast<int32_t>(modTimeMillis % 1000);
#endif

	if (biased) {
		fieldData.year_ = 1969;
		fieldData.month_ = 12;
		fieldData.monthDay_ = 31;
	}

	if (option.baseOption_.isFractionTrimming()) {
		fieldData.milliSecond_ = 0;
	}
}

void DateTime::getFields(
		int32_t &year,
		int32_t &month,
		int32_t &dayOfMonth,
		int32_t &hour,
		int32_t &minute,
		int32_t &second,
		int32_t &milliSecond,
		bool asLocalTimeZone) const {
	ZonedOption option;
	option.asLocalTimeZone_ = asLocalTimeZone;

	FieldData fieldData;
	getFields(fieldData, option);

	year = fieldData.year_;
	month = fieldData.month_;
	dayOfMonth = fieldData.monthDay_;
	hour = fieldData.year_;
	minute = fieldData.year_;
	second = fieldData.second_;
	milliSecond = fieldData.milliSecond_;
}

int64_t DateTime::getField(
		FieldType type, const ZonedOption &option) const {
	FieldData fieldData;
	getFields(fieldData, option);

	switch (type) {
	case FIELD_YEAR:
		return fieldData.year_;
	case FIELD_MONTH:
		return fieldData.month_;
	case FIELD_DAY_OF_MONTH:
		return fieldData.monthDay_;
	case FIELD_HOUR:
		return fieldData.hour_;
	case FIELD_MINUTE:
		return fieldData.minute_;
	case FIELD_SECOND:
		return fieldData.second_;
	case FIELD_MILLISECOND:
		return fieldData.milliSecond_;
	default:
		break;
	}

	if (type == FIELD_DAY_OF_WEEK) {
		const int64_t timeDays =
				getUnixTimeDays(fieldData, FIELD_YEAR, option);
		return unixTimeDaysToWeek(timeDays);
	}
	else if (type == FIELD_DAY_OF_YEAR) {
		const int64_t timeDays =
				getUnixTimeDays(fieldData, FIELD_MONTH, option);
		return timeDays + 1;
	}
	else if (type == FIELD_WEEK_OF_YEAR_SUNDAY ||
			type == FIELD_WEEK_OF_YEAR_MONDAY) {
		const int64_t timeDays =
				getUnixTimeDays(fieldData, FIELD_MONTH, option);
		const int64_t yearDow = unixTimeDaysToWeek(
				getUnixTimeDays(fieldData, FIELD_YEAR, option) - timeDays);

		int64_t offset = yearDow;
		if (type == FIELD_WEEK_OF_YEAR_SUNDAY) {
			if (offset == 0) {
				offset = 7;
			}
		}
		else {
			offset = (offset + (7 - 1));

			if (offset > 7) {
				offset %= 7;
			}
		}

		const int64_t modDays = timeDays + offset;
		return modDays / 7;
	}
	else {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
				"Unknown DateTime field type (type=" <<
				static_cast<int32_t>(type) << ")");
	}
}

void DateTime::setFields(
		const FieldData &fieldData, const ZonedOption &option, bool strict) {
	checkFieldBounds(fieldData, option);

	int32_t year = fieldData.year_;
	int32_t month = fieldData.month_;
	int32_t monthDay = fieldData.monthDay_;

	int64_t offsetMillis = 0;
	if (year < 1970) {
		if (year == 1969 && month == 12 && monthDay == 31) {
			year = 1970;
			month = 1;
			monthDay = 1;
			offsetMillis = -(24 * 60 * 60 * 1000);
		}
		else {
			UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
					"Illegal unix time");
		}
	}

	const bool asLocalTimeZone = false;
	const int32_t modMilliSecond = (option.baseOption_.isFractionTrimming() ?
			0 : fieldData.milliSecond_);

#ifdef _WIN32
	const bool dstIgnored = true;
	SYSTEMTIME time;
	time.wYear = static_cast<WORD>(year);
	time.wMonth = static_cast<WORD>(month);
	time.wDay = static_cast<WORD>(monthDay);
	time.wDayOfWeek = 0;
	time.wHour = static_cast<WORD>(fieldData.hour_);
	time.wMinute = static_cast<WORD>(fieldData.minute_);
	time.wSecond = static_cast<WORD>(fieldData.second_);
	time.wMilliseconds = static_cast<WORD>(modMilliSecond);
	const int64_t modTimeMillis = FileLib::getUnixTime(
			FileLib::getFileTime(time, asLocalTimeZone, dstIgnored));
#else
	tm time;
	time.tm_year = year - 1900;
	time.tm_mon = month - 1;
	time.tm_yday = 0; 
	time.tm_wday = 0; 
	time.tm_mday = monthDay;
	time.tm_hour = fieldData.hour_;
	time.tm_min = fieldData.minute_;
	time.tm_sec = fieldData.second_;
	time.tm_isdst = 0;
	const int64_t modTimeMillis =
			FileLib::getUnixTime(time, modMilliSecond, asLocalTimeZone);
#endif

	if (!option.zone_.isEmpty()) {
		option.zone_.checkRange(true);
		offsetMillis -= option.zone_.getOffsetMillis();
	}
	else if (option.asLocalTimeZone_) {
		offsetMillis -= getLocalOffsetMillis();
	}

	const int64_t retMillis = modTimeMillis + offsetMillis;

	if (strict) {
		FieldData fieldData2;
		util::DateTime(retMillis).getFields(fieldData2, option);

		if (fieldData.year_ != fieldData2.year_ ||
				fieldData.month_ != fieldData2.month_ ||
				fieldData.monthDay_ != fieldData2.monthDay_ ||
				fieldData.hour_ != fieldData2.hour_ ||
				fieldData.minute_ != fieldData2.minute_ ||
				fieldData.second_ != fieldData2.second_) {
			UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
					"Date time field out of range");
		}
	}
	else {
		checkUnixTimeBounds(retMillis, option.baseOption_);
	}
	unixTimeMillis_ = retMillis;
}

void DateTime::setFields(
		int32_t year,
		int32_t month,
		int32_t dayOfMonth,
		int32_t hour,
		int32_t minute,
		int32_t second,
		int32_t milliSecond,
		bool asLocalTimeZone) {
	FieldData fieldData;
	fieldData.year_ = year;
	fieldData.month_ = month;
	fieldData.monthDay_ = dayOfMonth;
	fieldData.hour_ = hour;
	fieldData.minute_ = minute;
	fieldData.second_ = second;
	fieldData.milliSecond_ = milliSecond;

	ZonedOption option;
	option.asLocalTimeZone_ = asLocalTimeZone;

	setFields(fieldData, option);
}

void DateTime::addField(
		int64_t amount, FieldType fieldType, const ZonedOption &option) {
	checkUnixTimeBounds(unixTimeMillis_, option.baseOption_);


	int64_t unit = 0;
	switch (fieldType) {
	case FIELD_DAY_OF_MONTH:
		unit = 24 * 60 * 60 * 1000;
		break;
	case FIELD_HOUR:
		unit = 60 * 60 * 1000;
		break;
	case FIELD_MINUTE:
		unit = 60 * 1000;
		break;
	case FIELD_SECOND:
		unit = 1000;
		break;
	case FIELD_MILLISECOND:
		unit = 1;
		break;
	case FIELD_MICROSECOND:
		addField((amount / 1000), FIELD_MILLISECOND, option);
		return;
	case FIELD_NANOSECOND:
		addField((amount / 1000 / 1000), FIELD_MILLISECOND, option);
		return;
	case FIELD_DAY_OF_WEEK:
		addField(amount, FIELD_DAY_OF_MONTH, option);
		return;
	case FIELD_DAY_OF_YEAR:
		addField(amount, FIELD_DAY_OF_MONTH, option);
		return;
	default:
		break;
	}

	if (unit > 0) {
		const int64_t maxTime = resolveMaxUnixTime(option.baseOption_);
		if ((amount >= 0 && amount > (maxTime / unit)) ||
				(amount < 0 && amount < -(maxTime / unit))) {
			UTIL_THROW_UTIL_ERROR(
					CODE_VALUE_OVERFLOW, "Adding time value overflow");
		}

		const int64_t amountMillis = amount * unit;
		if ((amount >= 0 && unixTimeMillis_ > maxTime - amountMillis) ||
				(amount < 0 && unixTimeMillis_ + amountMillis < 0)) {
			UTIL_THROW_UTIL_ERROR(
					CODE_VALUE_OVERFLOW, "Adding time value overflow");
		}

		unixTimeMillis_ += amountMillis;
		return;
	}


	FieldData fieldData;
	getFields(fieldData, option);

	int64_t baseYear = fieldData.year_;
	int64_t baseMonth = fieldData.month_;

	const int64_t baseMax = std::numeric_limits<int32_t>::max();
	int64_t *baseField;
	switch (fieldType) {
	case FIELD_YEAR:
		baseField = &baseYear;
		break;

	case FIELD_MONTH:
		baseField = &baseMonth;
		break;

	default:
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
				"Unknown DateTime field type (type=" <<
				static_cast<int32_t>(fieldType) << ")");
	}

	if ((amount >= 0 && *baseField > baseMax - amount) ||
			(amount < 0 && *baseField + amount < -baseMax)) {
		UTIL_THROW_UTIL_ERROR(
				CODE_VALUE_OVERFLOW, "Adding time value overflow");
	}
	*baseField += amount;

	switch (fieldType) {
	case FIELD_MONTH:
		if (baseMonth > 12) {
			baseYear += (baseMonth - 1) / 12;
			baseMonth = (baseMonth - 1) % 12 + 1;
		}
		else if (baseMonth <= 0) {
			const int64_t yearAmount = -baseMonth / 12 + 1;
			baseYear -= yearAmount;
			baseMonth = 12 - (-baseMonth - 12 * (yearAmount - 1));
		}
		break;
	default:
		break;
	}

	if (baseYear < -baseMax || baseYear > baseMax ||
			baseMonth < -baseMax || baseMonth > baseMax) {
		UTIL_THROW_UTIL_ERROR(
				CODE_VALUE_OVERFLOW, "Adding time value overflow");
	}
	fieldData.year_ = static_cast<int32_t>(baseYear);
	fieldData.month_ = static_cast<int32_t>(baseMonth);

	const int32_t &monthDay = fieldData.monthDay_;
	if (monthDay >= 29) {
		DateTime straightTime;
		FieldData fieldDataS = fieldData;

		fieldDataS.monthDay_ = 1;
		straightTime.setFields(fieldDataS, option);

		straightTime.setUnixTime(
				straightTime.getUnixTime() +
				static_cast<int64_t>(monthDay - 1) * 24 * 60 * 60 * 1000);

		straightTime.getFields(fieldDataS, option);
		const int32_t yearS = fieldDataS.year_;
		const int32_t monthS = fieldDataS.month_;

		int32_t &year = fieldData.year_;
		int32_t &month = fieldData.month_;

		if (monthS != month || yearS != year) {
			month++;
			if (month >= 13) {
				month = 1;
				year++;
			}
			fieldData.monthDay_ = 1;

			setFields(fieldData, option);
			unixTimeMillis_ -= 24 * 60 * 60 * 1000;
			return;
		}
	}

	setFields(fieldData, option);
}

void DateTime::addField(int64_t amount, FieldType fieldType) {
	ZonedOption option;
	addField(amount, fieldType, option);
}

int64_t DateTime::getDifference(
		const DateTime &base, FieldType fieldType,
		const ZonedOption &option) const {
	const int64_t diffMillis = unixTimeMillis_ - base.unixTimeMillis_;

	switch (fieldType) {
	case FIELD_DAY_OF_MONTH:
		return diffMillis / (24 * 60 * 60 * 1000);
	case FIELD_HOUR:
		return diffMillis / (60 * 60 * 1000);
	case FIELD_MINUTE:
		return diffMillis / (60 * 1000);
	case FIELD_SECOND:
		return diffMillis / 1000;
	case FIELD_MILLISECOND:
		if (option.baseOption_.isFractionTrimming()) {
			return 0;
		}
		else {
			return diffMillis;
		}
	case FIELD_MICROSECOND:
		return PreciseDateTime::makeDifference(diffMillis, 0, 1000);
	case FIELD_NANOSECOND:
		return PreciseDateTime::makeDifference(diffMillis, 0, 1000 * 1000);
	case FIELD_DAY_OF_WEEK:
		return getDifference(base, FIELD_DAY_OF_MONTH, option);
	case FIELD_DAY_OF_YEAR:
		return getDifference(base, FIELD_DAY_OF_MONTH, option);
	default:
		break;
	}

	if (fieldType == FIELD_YEAR || fieldType == FIELD_MONTH) {
		FieldData thisFields;
		this->getFields(thisFields, option);

		FieldData baseFields;
		base.getFields(baseFields, option);

		const int32_t &thisYear = thisFields.year_;
		const int32_t &thisMonth = thisFields.month_;

		const int32_t &baseYear = baseFields.year_;
		const int32_t &baseMonth = baseFields.month_;

		const int64_t totalMonth =
				(static_cast<int64_t>(thisYear) * 12 + thisMonth) -
				(static_cast<int64_t>(baseYear) * 12 + baseMonth);
		int64_t ret;
		if (fieldType == FIELD_YEAR) {
			ret = totalMonth / 12;
		}
		else {
			ret = totalMonth;
		}

#if UTIL_DATE_TIME_DIFF_WITH_SUB_FIELDS
		if (fieldType == FIELD_YEAR) {
			ret = thisYear - baseYear;
		}

		int32_t subDiff = 0;
		{
			const FieldType typeList[] = {
				FIELD_MONTH,
				FIELD_DAY_OF_MONTH,
				FIELD_HOUR,
				FIELD_MINUTE,
				FIELD_SECOND,
				FIELD_MILLISECOND
			};
			const size_t typeCount = sizeof(typeList) / sizeof(*typeList);

			const FieldType *const typeEnd = typeList + typeCount;
			const FieldType *typeIt = typeList;
			if (fieldType == FIELD_MONTH) {
				++typeIt;
			}
			for (; typeIt != typeEnd; ++typeIt) {
				const int32_t fieldDiff =
						thisFields.getValue(*typeIt) - baseFields.getValue(*typeIt);
				if (fieldDiff != 0) {
					subDiff = fieldDiff;
					break;
				}
			}
		}

		if (ret > 0 && subDiff < 0) {
			ret--;
		}
		else if (ret < 0 && subDiff > 0) {
			ret++;
		}
#endif

		return ret;
	}
	else {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
				"Unknown DateTime field type (type=" <<
				static_cast<int32_t>(fieldType) << ")");
	}
}

int64_t DateTime::getDifference(
		const DateTime &base, FieldType fieldType) const {
	ZonedOption option;
	return getDifference(base, fieldType, option);
}

void DateTime::format(std::ostream &s, const ZonedOption &option) const {
	getFormatter(option)(s);
}

size_t DateTime::format(
		char8_t *buf, size_t size, const ZonedOption &option) const {
	return getFormatter(option)(buf, size);
}

void DateTime::format(
		std::ostream &s, bool fractionTrimming, bool asLocalTimeZone) const {
	ZonedOption option;
	option.baseOption_ = Option::create(fractionTrimming);
	option.asLocalTimeZone_ = asLocalTimeZone;

	getFormatter(option)(s);
}

bool DateTime::parse(
		const char8_t *buf, size_t size, bool throwOnError,
		const ZonedOption &option) {
	return getParser(option)(buf, size, throwOnError);
}

bool DateTime::parse(
		const char8_t *str, DateTime &dateTime, bool fractionTrimming) {
	ZonedOption option;
	option.baseOption_ = Option::create(fractionTrimming);

	const bool throwOnError = false;
	return dateTime.getParser(option)(str, strlen(str), throwOnError);
}

DateTime DateTime::now(const Option &option) {
#ifdef _WIN32
	FILETIME time;
	FileLib::GetSystemTimePreciseAsFileTimeFunc preciseFunc =
			FileLib::findPreciseSystemTimeFunc();
	if (preciseFunc == NULL) {
		GetSystemTimeAsFileTime(&time);
	}
	else {
		preciseFunc(&time);
	}
	int64_t unixTime = FileLib::getUnixTime(time);
#else
	timespec time;
	if (clock_gettime(CLOCK_REALTIME, &time) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	int64_t unixTime = FileLib::getUnixTime(time);
#endif
	if (option.isFractionTrimming()) {
		unixTime = unixTime / 1000 * 1000;
	}
	return DateTime(UTIL_FAILURE_SIMULATION_TIME_FILTER(unixTime));
}

DateTime DateTime::now(bool fractionTrimming) {
	const Option &option = Option::create(fractionTrimming);
	return now(option);
}

DateTime DateTime::max(const Option &option) {
	if (option.isFractionTrimming()) {
		static const DateTime maxTime(getMaxUnixTime(true));
		return maxTime;
	}
	else {
		static const DateTime maxTime(getMaxUnixTime(false));
		return maxTime;
	}
}

DateTime DateTime::max(bool fractionTrimming) {
	const Option &option = Option::create(fractionTrimming);
	return max(option);
}

TimeZone::Offset DateTime::getLocalOffsetMillis() {
	return TimeZone::getLocalTimeZone().getOffsetMillis();
}

int64_t DateTime::getUnixTimeDays(
		const FieldData &fieldData, FieldType trimingFieldType,
		const ZonedOption &option) const {
	const int64_t dayToMillis = 24 * 60 * 60 * 1000;
	const int32_t epochYear = 1970;

	FieldData modFieldData = fieldData;

	do {
		modFieldData.monthDay_ = 1;

		modFieldData.month_ = 1;
		if (trimingFieldType == FIELD_MONTH) {
			break;
		}

		modFieldData.year_ = epochYear;
		assert(trimingFieldType == FIELD_YEAR);
	}
	while (false);

	int64_t offsetMillis = 0;
	if (modFieldData.year_ == epochYear) {
		modFieldData.monthDay_++;
		offsetMillis = dayToMillis;
	}

	DateTime modTime;
	modTime.setFields(modFieldData, option);
	const int64_t modMillis = modTime.unixTimeMillis_ - offsetMillis;

	return (unixTimeMillis_ - modMillis) / dayToMillis;
}

int64_t DateTime::unixTimeDaysToWeek(int64_t timeDays) {
	return (timeDays + EPOCH_DAY_OF_WEEK - 1) % 7;
}

void DateTime::checkFieldBounds(
		const FieldData &fields, const ZonedOption &option) {
	if (fields.month_ <= 0 || fields.monthDay_ <= 0 ||
			fields.hour_ < 0 || fields.minute_ < 0 ||
			fields.second_ < 0 || fields.milliSecond_ < 0) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Time fields out of range");
	}

	if (fields.month_ > 12 || fields.monthDay_ > 31 ||
			fields.hour_ > 24 || fields.minute_ > 60 ||
			fields.second_ > 60 || fields.milliSecond_ > 1000) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Time fields out of range");
	}

	if (fields.year_ < 1970 &&
			(fields.month_ != 12 || fields.monthDay_ != 31)) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Time fields out of range");
	}

	FieldData localFields;
	const FieldData *maxFields = resolveMaxFields(option, localFields);
	if (fields.year_ >= maxFields->year_) {
		const FieldType typeList[] = {
			FIELD_YEAR,
			FIELD_MONTH,
			FIELD_DAY_OF_MONTH,
			FIELD_HOUR,
			FIELD_MINUTE,
			FIELD_SECOND,
			FIELD_MILLISECOND
		};
		const size_t typeCount = sizeof(typeList) / sizeof(*typeList);

		const FieldType *const typeEnd = typeList + typeCount;
		for (const FieldType *typeIt = typeList; typeIt != typeEnd; ++typeIt) {
			const int32_t diff =
					fields.getValue(*typeIt) - maxFields->getValue(*typeIt);
			if (diff <= 0) {
				if (diff < 0) {
					UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
							"Time fields out of range");
				}
				break;
			}
		}
	}
}

const DateTime::FieldData* DateTime::resolveMaxFields(
		const ZonedOption &option, FieldData &localFields) {
	if (option.maxFields_ == NULL) {
		const DateTime &maxTime = DateTime::max(option.baseOption_);
		ZonedOption modOption;
		modOption.baseOption_.maxTimeMillis_ = maxTime.getUnixTime();
		maxTime.getFields(localFields, modOption);
		return &localFields;
	}
	return option.maxFields_;
}

void DateTime::checkUnixTimeBounds(
		int64_t unixTimeMillis, const Option &option) {
	if (unixTimeMillis < 0 || unixTimeMillis > resolveMaxUnixTime(option)) {
		UTIL_THROW_UTIL_ERROR(
				CODE_INVALID_PARAMETER,
				"Time out of range (unixTimeMillis=" << unixTimeMillis << ")");
	}
}

int64_t DateTime::resolveMaxUnixTime(const Option &option) {
	if (option.maxTimeMillis_ <= 0) {
		return getMaxUnixTime(option.isFractionTrimming());
	}
	return option.maxTimeMillis_;
}

int64_t DateTime::getMaxUnixTime(bool fractionTrimming) {
	int64_t maxUnixTime;
#if defined(_WIN32)
	try {
		FILETIME time;
		time.dwHighDateTime = std::numeric_limits<DWORD>::max() >> 1;
		time.dwLowDateTime = std::numeric_limits<DWORD>::max();
		maxUnixTime = FileLib::getUnixTime(time);
	}
	catch (...) {
		assert(false);
		maxUnixTime =
				static_cast<int64_t>(std::numeric_limits<int32_t>::max()) * 1000;
	}
#elif defined(__x86_64__)
	maxUnixTime = std::numeric_limits<int64_t>::max();
#else
	maxUnixTime =
				static_cast<int64_t>(std::numeric_limits<int32_t>::max()) * 1000;
#endif
	assert(maxUnixTime / 1000 >= std::numeric_limits<int32_t>::max());

	return (fractionTrimming ? maxUnixTime / 1000 * 1000 : maxUnixTime);
}


void DateTime::FieldData::initialize() {
	setValue<FIELD_YEAR>(0);
	setValue<FIELD_MONTH>(0);
	setValue<FIELD_DAY_OF_MONTH>(0);
	setValue<FIELD_HOUR>(0);
	setValue<FIELD_MINUTE>(0);
	setValue<FIELD_SECOND>(0);
	setValue<FIELD_MILLISECOND>(0);
}

int32_t DateTime::FieldData::getValue(FieldType type) const {
	switch (type) {
	case FIELD_YEAR:
		return getValue<FIELD_YEAR>();
	case FIELD_MONTH:
		return getValue<FIELD_MONTH>();
	case FIELD_DAY_OF_MONTH:
		return getValue<FIELD_DAY_OF_MONTH>();
	case FIELD_HOUR:
		return getValue<FIELD_HOUR>();
	case FIELD_MINUTE:
		return getValue<FIELD_MINUTE>();
	case FIELD_SECOND:
		return getValue<FIELD_SECOND>();
	case FIELD_MILLISECOND:
		return getValue<FIELD_MILLISECOND>();
	default:
		assert(false);
		return int32_t();
	}
}

void DateTime::FieldData::setValue(FieldType type, int32_t value) {
	switch (type) {
	case FIELD_YEAR:
		setValue<FIELD_YEAR>(value);
		break;
	case FIELD_MONTH:
		setValue<FIELD_MONTH>(value);
		break;
	case FIELD_DAY_OF_MONTH:
		setValue<FIELD_DAY_OF_MONTH>(value);
		break;
	case FIELD_HOUR:
		setValue<FIELD_HOUR>(value);
		break;
	case FIELD_MINUTE:
		setValue<FIELD_MINUTE>(value);
		break;
	case FIELD_SECOND:
		setValue<FIELD_SECOND>(value);
		break;
	case FIELD_MILLISECOND:
		setValue<FIELD_MILLISECOND>(value);
		break;
	default:
		assert(false);
		break;
	}
}


DateTime::Option::Option() :
		precision_(PRECISION_NONE),
		maxTimeMillis_(0) {
}

DateTime::Option DateTime::Option::withDefaultPrecision(
		FieldType defaultType) const {
	Option dest = *this;
	dest.precision_ = resolvePrecision(precision_, defaultType);
	return dest;
}

DateTime::Option DateTime::Option::create(bool fractionTrimming) {
	Option option;
	option.precision_ = makePrecision(fractionTrimming);
	return option;
}

DateTime::FieldType DateTime::Option::makePrecision(bool fractionTrimming) {
	if (fractionTrimming) {
		return DateTime::FIELD_SECOND;
	}
	else {
		return PRECISION_NONE;
	}
}

DateTime::FieldType DateTime::Option::resolvePrecision(
		FieldType base, FieldType defaultType) {
	if (base == PRECISION_NONE) {
		return defaultType;
	}
	return base;
}

uint32_t DateTime::Option::getFractionalDigits() const {
	return getFractionalDigits(
			resolvePrecision(precision_, PRECISION_DEFAULT));
}

uint32_t DateTime::Option::getFractionalDigits(FieldType precision) {
	switch (precision) {
	case FIELD_MILLISECOND:
		return 3;
	case FIELD_MICROSECOND:
		return 6;
	case FIELD_NANOSECOND:
		return 9;
	default:
		assert(precision == FIELD_SECOND);
		return 0;
	}
}

bool DateTime::Option::isFractionTrimming() const {
	return (precision_ == FIELD_SECOND);
}


DateTime::ZonedOption::ZonedOption() :
		asLocalTimeZone_(false),
		maxFields_(NULL) {
}

DateTime::ZonedOption DateTime::ZonedOption::create(
		bool fractionTrimming, const TimeZone &zone) {
	ZonedOption option;
	option.baseOption_ = Option::create(fractionTrimming);
	option.zone_ = zone;
	return option;
}


void DateTime::Formatter::operator()(std::ostream &s) const {
	try {
		char8_t buf[MAX_FORMAT_SIZE];
		const size_t size = format(buf, sizeof(buf));
		s.write(buf, static_cast<std::streamsize>(size));
	}
	catch (...) {
		s.setstate(std::ios::failbit);
		if (s.exceptions() & std::ios::failbit) {
			throw;
		}
	}
}

size_t DateTime::Formatter::format(char8_t *buf, size_t size) const {
	TimeZone zone = option_.zone_;
	if (zone.isEmpty()) {
		if (option_.asLocalTimeZone_) {
			zone = TimeZone::getLocalTimeZone();
		}
		else {
			zone = TimeZone::getUTCTimeZone();
		}
	}

	char8_t *it = buf;
	char8_t *end = it + size;

	FieldData fieldData;
	dateTime_.getFields(fieldData, option_);

	formatMain(it, end, fieldData);
	formatFraction(
			it, end, fieldData, nanoSeconds_,
			option_.baseOption_.withDefaultPrecision(defaultPrecision_));

	const size_t zoneSize = zone.format(it, static_cast<size_t>(end - it));
	it += zoneSize;

	return static_cast<size_t>(it - buf);
}

void DateTime::Formatter::formatMain(
		char8_t *&it, char8_t *end, const FieldData &fieldData) {
	TinyLexicalIntConverter converter;

	converter.minWidth_ = 4;
	converter.format(it, end, static_cast<uint32_t>(fieldData.year_));
	CharBufferUtils::write(it, end, "-");

	converter.minWidth_ = 2;
	converter.format(it, end, static_cast<uint32_t>(fieldData.month_));
	CharBufferUtils::write(it, end, "-");

	converter.format(it, end, static_cast<uint32_t>(fieldData.monthDay_));
	CharBufferUtils::write(it, end, "T");

	converter.format(it, end, static_cast<uint32_t>(fieldData.hour_));
	CharBufferUtils::write(it, end, ":");

	converter.format(it, end, static_cast<uint32_t>(fieldData.minute_));
	CharBufferUtils::write(it, end, ":");

	converter.format(it, end, static_cast<uint32_t>(fieldData.second_));
}

void DateTime::Formatter::formatFraction(
		char8_t *&it, char8_t *end, const FieldData &fieldData,
		uint32_t nanoSeconds, const Option &option) {
	if (option.isFractionTrimming()) {
		return;
	}
	TinyLexicalIntConverter converter;

	CharBufferUtils::write(it, end, ".");
	converter.minWidth_ = 3;

	converter.format(it, end, static_cast<uint32_t>(fieldData.milliSecond_));

	const uint32_t digits = option.getFractionalDigits();
	if (digits < Option::getFractionalDigits(FIELD_MICROSECOND)) {
		return;
	}
	converter.format(it, end, (nanoSeconds / 1000) % 1000);

	if (digits < Option::getFractionalDigits(FIELD_NANOSECOND)) {
		return;
	}
	converter.format(it, end, nanoSeconds % 1000);
}


bool DateTime::Parser::parse(
		const char8_t *buf, size_t size, bool throwOnError) const {
	const char8_t *it = buf;
	const char8_t *end = it + size;

	if (it == end) {
		return errorParse(throwOnError);
	}

	FieldData fieldData;
	if (!parseMain(it, end, fieldData)) {
		return errorParse(throwOnError);
	}

	uint32_t nanoSeconds;
	if (!parseFraction(
			it, end, fieldData, nanoSeconds,
			option_.baseOption_.withDefaultPrecision(defaultPrecision_))) {
		return errorParse(throwOnError);
	}

	ZonedOption modOption = option_;
	modOption.asLocalTimeZone_ = false;

	if (!modOption.zone_.parse(
			it, static_cast<size_t>(end - it), throwOnError)) {
		return errorParse(throwOnError);
	}

	try {
		dateTime_.setFields(fieldData, modOption);
	}
	catch (...) {
		if (!throwOnError) {
			return errorParse(throwOnError);
		}
		throw;
	}

	if (nanoSecondsRef_ != NULL) {
		*nanoSecondsRef_ = nanoSeconds;
	}

	return true;
}

bool DateTime::Parser::parseMain(
		const char8_t *&it, const char8_t *end, FieldData &fieldData) {
	uint32_t value;
	TinyLexicalIntConverter converter;
	converter.minWidth_ = 4;

	if (!converter.parse(it, end, value) ||
			value > static_cast<uint32_t>(
					std::numeric_limits<int32_t>::max()) ||
			it == end || *it != '-') {
		return false;
	}
	++it;
	fieldData.year_ = static_cast<int32_t>(value);

	converter.minWidth_ = 2;
	converter.maxWidth_ = 2;

	if (!converter.parse(it, end, value) || it == end || *it != '-') {
		return false;
	}
	++it;
	fieldData.month_ = static_cast<int32_t>(value);

	if (!converter.parse(it, end, value) || it == end || *it != 'T') {
		return false;
	}
	++it;
	fieldData.monthDay_ = static_cast<int32_t>(value);

	if (!converter.parse(it, end, value) || it == end || *it != ':') {
		return false;
	}
	++it;
	fieldData.hour_ = static_cast<int32_t>(value);

	if (!converter.parse(it, end, value) || it == end || *it != ':') {
		return false;
	}
	++it;
	fieldData.minute_ = static_cast<int32_t>(value);

	if (!converter.parse(it, end, value)) {
		return false;
	}
	fieldData.second_ = static_cast<int32_t>(value);

	return true;
}

bool DateTime::Parser::parseFraction(
		const char8_t *&it, const char8_t *end, FieldData &fieldData,
		uint32_t &nanoSeconds, const Option &option) {
	fieldData.milliSecond_ = 0;
	nanoSeconds = 0;

	if (option.isFractionTrimming() || it == end || *it != '.') {
		return true;
	}
	++it;

	uint32_t value;
	TinyLexicalIntConverter converter;
	converter.minWidth_ = 3;
	converter.maxWidth_ = 3;

	if (!converter.parse(it, end, value)) {
		return true;
	}
	fieldData.milliSecond_ = static_cast<int32_t>(value);

	const uint32_t digits = option.getFractionalDigits();
	if (digits < Option::getFractionalDigits(FIELD_MICROSECOND)) {
		return true;
	}
	if (!converter.parse(it, end, value)) {
		return true;
	}
	nanoSeconds = static_cast<uint32_t>(value) * 1000;

	if (digits < Option::getFractionalDigits(FIELD_NANOSECOND)) {
		return true;
	}
	if (converter.parse(it, end, value)) {
		nanoSeconds += static_cast<uint32_t>(value);
	}
	return true;
}

bool DateTime::Parser::errorParse(bool throwOnError) {
	if (throwOnError) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER, "Failed to parse");
	}
	return false;
}


void PreciseDateTime::getFields(
		FieldData &fieldData, const ZonedOption &option) const {
	base_.getFields(fieldData.baseFields_, option);
	fieldData.nanoSecond_ = nanoSeconds_;
}

int64_t PreciseDateTime::getField(
		FieldType type, const ZonedOption &option) const {
	switch (type) {
	case DateTime::FIELD_MICROSECOND:
	case DateTime::FIELD_NANOSECOND:
		break;
	default:
		return base_.getField(type, option);
	}

	FieldData fieldData;
	getFields(fieldData, option);
	return fieldData.getValue(type);
}

void PreciseDateTime::setFields(
		const FieldData &fieldData, const ZonedOption &option, bool strict) {
	base_.setFields(fieldData.baseFields_, option, strict);
	nanoSeconds_ = fieldData.nanoSecond_;
}

void PreciseDateTime::addField(
		int64_t amount, FieldType fieldType, const ZonedOption &option) {
	switch (fieldType) {
	case DateTime::FIELD_MICROSECOND:
		addPreciseField(amount, 1000, option);
		break;
	case DateTime::FIELD_NANOSECOND:
		addPreciseField(amount, 1000 * 1000, option);
		break;
	default:
		base_.addField(amount, fieldType, option);
		break;
	}
}

int64_t PreciseDateTime::getDifference(
		const PreciseDateTime &base, FieldType fieldType,
		const ZonedOption &option) const {
	FieldType upperType;
	uint32_t preciseUnit;
	switch (fieldType) {
	case DateTime::FIELD_MICROSECOND:
		upperType = DateTime::FIELD_MILLISECOND;
		preciseUnit = 1000;
		break;
	case DateTime::FIELD_NANOSECOND:
		upperType = DateTime::FIELD_MILLISECOND;
		preciseUnit = 1000 * 1000;
		break;
	default:
		upperType = fieldType;
		preciseUnit = 0;
		break;
	}

	int64_t carry = 0;
	if (preciseUnit == 0 &&
			fieldType != DateTime::FIELD_YEAR &&
			fieldType != DateTime::FIELD_MONTH) {
		const int32_t baseComp = compareBase(*this, base);
		if (baseComp != 0) {
			const int32_t nanosComp = compareNanos(*this, base);
			if (baseComp > 0) {
				carry = (nanosComp < 0 ? -1 : 0);
			}
			else {
				carry = (nanosComp > 0 ? 1 : 0);
			}
		}
	}

	const DateTime adjustedTime(base_.getUnixTime() + carry);
	const int64_t upperDiff =
			adjustedTime.getDifference(base.base_, upperType, option);

	if (preciseUnit > 0) {
		return getPreciseDifference(
				upperDiff, nanoSeconds_, base.nanoSeconds_, preciseUnit);
	}
	return upperDiff;
}

int64_t PreciseDateTime::makeDifference(
		int64_t millisDiff, int64_t nanosDiff, int32_t unit) {
	const int32_t nanosPerMilli = 1000 * 1000;
	assert(1000 <= unit && unit <= nanosPerMilli);

	int64_t millisOffset = 0;
	int64_t nanosOffset = 0;
	if (millisDiff > 0 && nanosDiff < 0) {
		millisOffset = -1;
		nanosOffset = nanosPerMilli;
	}
	else if (millisDiff < 0 && nanosDiff > 0) {
		millisOffset = 1;
		nanosOffset = -nanosPerMilli;
	}

	const int64_t upper = millisDiff + millisOffset;
	const int64_t lower = (nanosDiff + nanosOffset) / (nanosPerMilli / unit);

	const int64_t min =
			std::numeric_limits<int64_t>::min() - lower * (lower < 0 ? 1 : 0);
	const int64_t max =
			std::numeric_limits<int64_t>::max() - lower * (lower > 0 ? 1 : 0);

	if (upper < min / unit || max / unit < upper) {
		UTIL_THROW_UTIL_ERROR(
				CODE_VALUE_OVERFLOW, "Time difference value overflow");
	}

	return upper * unit + lower;
}

void PreciseDateTime::addPreciseField(
		int64_t amount, int32_t unit, const ZonedOption &option) {
	const int32_t nanosPerMilli = 1000 * 1000;
	assert(1000 <= unit && unit <= nanosPerMilli);

	const int64_t upper = amount / unit;
	const int64_t lower = (amount - upper * unit) * (nanosPerMilli / unit);

	const int64_t rawNanos = static_cast<int64_t>(nanoSeconds_) + lower;
	int64_t carry;
	if (rawNanos < 0) {
		carry = -1;
	}
	else if (rawNanos >= nanosPerMilli) {
		carry = 1;
	}
	else {
		carry = 0;
	}

	base_.addField(upper + carry, DateTime::FIELD_MILLISECOND, option);
	nanoSeconds_ = static_cast<uint32_t>(rawNanos - nanosPerMilli * carry);
}

int64_t PreciseDateTime::getPreciseDifference(
		int64_t millisDiff, uint32_t nanos1, uint32_t nanos2, uint32_t unit) {
	const int64_t nanosDiff =
			static_cast<int64_t>(nanos1) - static_cast<int64_t>(nanos2);

	return makeDifference(millisDiff, nanosDiff, unit);
}

int32_t PreciseDateTime::compareBase(
		const PreciseDateTime &t1, const PreciseDateTime &t2) {
	const int64_t v1 = t1.base_.getUnixTime();
	const int64_t v2 = t2.base_.getUnixTime();
	return (v1 == v2 ? 0 : (v1 < v2 ? -1 : 1));
}

int32_t PreciseDateTime::compareNanos(
		const PreciseDateTime &t1, const PreciseDateTime &t2) {
	const uint32_t v1 = t1.getNanoSeconds();
	const uint32_t v2 = t2.getNanoSeconds();
	return (v1 == v2 ? 0 : (v1 < v2 ? -1 : 1));
}


void PreciseDateTime::FieldData::initialize() {
	baseFields_.initialize();
	setValue<DateTime::FIELD_MILLISECOND>(0);
}

int32_t PreciseDateTime::FieldData::getValue(FieldType type) const {
	switch (type) {
	case DateTime::FIELD_MILLISECOND:
		return getValue<DateTime::FIELD_MILLISECOND>();
	case DateTime::FIELD_MICROSECOND:
		return getValue<DateTime::FIELD_MICROSECOND>();
	case DateTime::FIELD_NANOSECOND:
		return getValue<DateTime::FIELD_NANOSECOND>();
	default:
		return baseFields_.getValue(type);
	}
}

void PreciseDateTime::FieldData::setValue(FieldType type, int32_t value) {
	switch (type) {
	case DateTime::FIELD_MILLISECOND:
		setValue<DateTime::FIELD_MILLISECOND>(value);
		break;
	case DateTime::FIELD_MICROSECOND:
		setValue<DateTime::FIELD_MICROSECOND>(value);
		break;
	case DateTime::FIELD_NANOSECOND:
		setValue<DateTime::FIELD_NANOSECOND>(value);
		break;
	default:
		baseFields_.setValue(type, value);
		break;
	}
}


Stopwatch::Stopwatch(Status initialStatus) :
		status_(STATUS_STOPPED),
		startClock_(0),
		elapsedClock_(0) {
	switch (initialStatus) {
	case STATUS_STOPPED:
		break;
	case STATUS_STARTED:
		start();
		break;
	default:
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

void Stopwatch::reset() {
	status_ = STATUS_STOPPED;
	elapsedClock_ = 0;
}

void Stopwatch::start() {
	if (status_ == STATUS_STARTED) {
		return;
	}

	startClock_ = currentClock();
	status_ = STATUS_STARTED;
}

uint32_t Stopwatch::stop() {
	const uint64_t lastDuration = currentClock() - startClock_;
	if (status_ != STATUS_STARTED) {
		return 0;
	}

	elapsedClock_ += lastDuration;
	status_ = STATUS_STOPPED;

	return clockToMillis(lastDuration);
}

uint32_t Stopwatch::elapsedMillis() {
	return clockToMillis(elapsedClock());
}

uint64_t Stopwatch::elapsedNanos() {
	const uint64_t clocks = elapsedClock();
	const uint64_t nanosPerSec = 1000 * 1000 * 1000;
	if (clocks >= std::numeric_limits<uint64_t>::max() / nanosPerSec) {

		if (clocks / clocksPerSec() >=
				std::numeric_limits<uint64_t>::max() / nanosPerSec) {

			UTIL_FAILURE_SIMULATION_TRACE_COVERAGE(
					COVERAGE_STOPWATCH_ELAPSED_NANOS1);

			return std::numeric_limits<uint64_t>::max();
		}

		UTIL_FAILURE_SIMULATION_TRACE_COVERAGE(
				COVERAGE_STOPWATCH_ELAPSED_NANOS2);

		return clocks / clocksPerSec() * nanosPerSec;
	}
	else {
		UTIL_FAILURE_SIMULATION_TRACE_COVERAGE(
				COVERAGE_STOPWATCH_ELAPSED_NANOS3);

		return clocks * nanosPerSec / clocksPerSec();
	}
}

uint64_t Stopwatch::elapsedClock() {
	if (status_ == STATUS_STARTED) {
		return (currentClock() - startClock_ + elapsedClock_);
	}
	else {
		return elapsedClock_;
	}
}

uint32_t Stopwatch::clockToMillis(uint64_t clockCount) {
	return static_cast<uint32_t>(clockCount * 1000 / clocksPerSec());
}

uint64_t Stopwatch::currentClock() {
#ifdef _WIN32
	LARGE_INTEGER current;
	if (!QueryPerformanceCounter(&current)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return UTIL_FAILURE_SIMULATION_CLOCK_FILTER(
			static_cast<uint64_t>(current.QuadPart));
#else
	timespec current;
	if (clock_gettime(CLOCK_MONOTONIC, &current) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return UTIL_FAILURE_SIMULATION_CLOCK_FILTER(
			static_cast<uint64_t>(current.tv_sec) * 1000 * 1000 +
			static_cast<uint64_t>(current.tv_nsec) / 1000);
#endif	
}

uint64_t Stopwatch::clocksPerSec() {
#ifdef _WIN32
	LARGE_INTEGER frequency;
	QueryPerformanceFrequency(&frequency);
	const uint64_t result = static_cast<uint64_t>(frequency.QuadPart);
	if (result == 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return result;
#else
	return 1000 * 1000;
#endif
}

#if UTIL_FAILURE_SIMULATION_ENABLED


volatile int32_t DateTimeFailureSimulator::mode_ = 0;
volatile uint64_t DateTimeFailureSimulator::startCount_ = 0;
volatile uint64_t DateTimeFailureSimulator::endCount_ = 0;

volatile int32_t DateTimeFailureSimulator::speedRate_ = 0;
volatile int64_t DateTimeFailureSimulator::startOffset_ = 0;
volatile int64_t DateTimeFailureSimulator::endDuration_ = 0;

volatile int64_t DateTimeFailureSimulator::startTime_ = 0;
volatile uint64_t DateTimeFailureSimulator::lastOperationCount_ = 0;

volatile uint64_t DateTimeFailureSimulator::coverage_[] = { 0 };

void DateTimeFailureSimulator::set(
		int32_t mode, uint64_t startCount, uint64_t endCount,
		int32_t speedRate, int64_t startOffset, int64_t endDuration) {
	if (mode != 0 && mode != 1) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
				"Illegal mode");
	}

	mode_ = 0;
	{
		startCount_ = startCount;
		endCount_ = endCount;
		speedRate_ = speedRate;
		startOffset_ = startOffset;
		endDuration_ = endDuration;
		startTime_ = 0;
		lastOperationCount_ = 0;
	}
	mode_ = mode;
}

int64_t DateTimeFailureSimulator::filterTime(int64_t time) {
	if (mode_ == 1) {
		int64_t filteredTime = time;

		if (startCount_ <= lastOperationCount_ &&
				lastOperationCount_ < endCount_ &&
				(startTime_ == 0 || endDuration_ <= 0 ||
				time - startTime_ < endDuration_)) {

			const int64_t base = (startTime_ == 0 ? time : startTime_);
			const int64_t diff = (startTime_ == 0 ? 0 : time - startTime_);
			filteredTime = base + startOffset_ + diff * speedRate_ / 100;
			if (filteredTime < 0) {
				lastOperationCount_++;
				UTIL_THROW_PLATFORM_ERROR(NULL);
			}
		}
		if (startTime_ == 0) {
			startTime_ = time;
		}
		lastOperationCount_++;

		return filteredTime;
	}
	else {
		return time;
	}
}

uint64_t DateTimeFailureSimulator::filterClock(uint64_t clock) {
	if (mode_ == 1) {
#ifdef _WIN32
		FILETIME time;
		GetSystemTimeAsFileTime(&time);
		const int64_t unixTime = FileLib::getUnixTime(time);
#else
		timespec time;
		if (clock_gettime(CLOCK_REALTIME, &time) != 0) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		const int64_t unixTime = FileLib::getUnixTime(time);
#endif

		const int64_t filteredTime = filterTime(unixTime);
		if (filteredTime == unixTime) {
			return clock;
		}

		const int64_t int64Min = std::numeric_limits<int64_t>::min();
		const int64_t int64Max = std::numeric_limits<int64_t>::max();

		const int64_t clocksPerSec = Stopwatch::clocksPerSec();
		const int64_t diffTime = (filteredTime - unixTime);

		int64_t diffClock;
		if (diffTime <= int64Min / clocksPerSec ||
				diffTime >= int64Max / clocksPerSec) {
			if (diffTime / 1000 <= int64Min / clocksPerSec) {
				diffClock = int64Min;
			}
			else if (diffTime / 1000 >= int64Max / clocksPerSec) {
				diffClock = int64Max;
			}
			else {
				diffClock = diffTime / 1000 * clocksPerSec;
			}
		}
		else {
			diffClock = diffTime * clocksPerSec / 1000;
		}

		if (diffClock > 0) {
			const uint64_t filteredClock =
					clock + static_cast<uint64_t>(diffClock);
			if (filteredClock < clock ||
					filteredClock < static_cast<uint64_t>(diffClock)) {
				return std::numeric_limits<uint64_t>::max();
			}
			return filteredClock;
		}
		else {
			return clock - static_cast<uint64_t>(-diffClock);
		}
	}
	else {
		return clock;
	}
}

#endif	

} 
