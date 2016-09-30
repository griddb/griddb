/*
    Copyright (c) 2012 TOSHIBA CORPORATION.
    
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
#include "util/os.h"
#include <iomanip>
#include <limits>
#include <assert.h>

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


const int64_t DateTime::INITIAL_UNIX_TIME = static_cast<int64_t>(0);

DateTime::DateTime(const char8_t *str, bool trimMilliseconds) :
		unixTimeMillis_(INITIAL_UNIX_TIME) {
	if (!parse(str, *this, trimMilliseconds)) {
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

void DateTime::getFields(
		int32_t &year,
		int32_t &month,
		int32_t &dayOfMonth,
		int32_t &hour,
		int32_t &minute,
		int32_t &second,
		int32_t &milliSecond,
		bool asLocalTimeZone) const {
#ifdef _WIN32
	SYSTEMTIME time = FileLib::getSystemTime(
			FileLib::getFileTime(unixTimeMillis_), asLocalTimeZone);
	year = static_cast<int32_t>(time.wYear);
	month = static_cast<int32_t>(time.wMonth);
	dayOfMonth = static_cast<int32_t>(time.wDay);
	hour = static_cast<int32_t>(time.wHour);
	minute = static_cast<int32_t>(time.wMinute);
	second = static_cast<int32_t>(time.wSecond);
	milliSecond = static_cast<int32_t>(time.wMilliseconds);
#else
	tm time = FileLib::getTM(unixTimeMillis_, asLocalTimeZone);
	year = time.tm_year + 1900;
	month = time.tm_mon + 1;
	dayOfMonth = time.tm_mday;
	hour = time.tm_hour;
	minute = time.tm_min;
	second = time.tm_sec;
	milliSecond = static_cast<int32_t>(unixTimeMillis_ % 1000);
#endif
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
	if (year < 1970) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_PARAMETER,
				"Illegal unix time");
	}
#ifdef _WIN32
	SYSTEMTIME time;
	time.wYear = static_cast<WORD>(year);
	time.wMonth = static_cast<WORD>(month);
	time.wDay = static_cast<WORD>(dayOfMonth);
	time.wDayOfWeek = 0;
	time.wHour = static_cast<WORD>(hour);
	time.wMinute = static_cast<WORD>(minute);
	time.wSecond = static_cast<WORD>(second);
	time.wMilliseconds = static_cast<WORD>(milliSecond);
	unixTimeMillis_ =
			FileLib::getUnixTime(FileLib::getFileTime(time, asLocalTimeZone));
#else
	tm time;
	time.tm_year = year - 1900;
	time.tm_mon = month - 1;
	time.tm_yday = 0; 
	time.tm_wday = 0; 
	time.tm_mday = dayOfMonth;
	time.tm_hour = hour;
	time.tm_min = minute;
	time.tm_sec = second;
	time.tm_isdst = 0;

	unixTimeMillis_ =
			FileLib::getUnixTime(time, milliSecond, asLocalTimeZone);
#endif
}

void DateTime::addField(int64_t amount, FieldType fieldType) {


	switch (fieldType) {
	case FIELD_DAY_OF_MONTH:
		unixTimeMillis_ += amount * 24 * 60 * 60 * 1000;
		return;
	case FIELD_HOUR:
		unixTimeMillis_ += amount * 60 * 60 * 1000;
		return;
	case FIELD_MINUTE:
		unixTimeMillis_ += amount * 60 * 1000;
		return;
	case FIELD_SECOND:
		unixTimeMillis_ += amount * 1000;
		return;
	case FIELD_MILLISECOND:
		unixTimeMillis_ += amount;
		return;
	default:
		break;
	}


	int32_t year, month, monthDay, hour, minute, second, milliSecond;

	getFields(year, month, monthDay, hour, minute, second, milliSecond, false);

	switch (fieldType) {
	case FIELD_YEAR:
		year += static_cast<int32_t>(amount);
		break;

	case FIELD_MONTH:
		month += static_cast<int32_t>(amount);
		if (month > 12) {
			year += (month - 1) / 12;
			month = (month - 1) % 12 + 1;
		}
		else if (month <= 0) {
			const int32_t yearAmount = -month / 12 + 1;
			year -= yearAmount;
			month = 12 - (-month - 12 * (yearAmount - 1));
		}
		break;

	default:
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
				"Unknown field type");
	}

	if (monthDay >= 29) {
		const DateTime straightTime(
				DateTime(year, month,
						1, hour, minute, second, milliSecond, false).getUnixTime() +
				static_cast<int64_t>(monthDay - 1) * 24 * 60 * 60 * 1000);

		int32_t yearS, monthS;
		straightTime.getFields(
				yearS, monthS, monthDay, hour, minute, second, milliSecond, false);
		if (monthS != month || yearS != year) {
			month++;
			if (month >= 13) {
				month = 1;
				year++;
			}
			setFields(year, month, 1, hour, minute, second, milliSecond, false);
			unixTimeMillis_ -= 24 * 60 * 60 * 1000;
			return;
		}
	}

	setFields(year, month, monthDay, hour, minute, second, milliSecond, false);
}

int64_t DateTime::getDifference(
		const DateTime &base, FieldType fieldType) const {
	const int64_t diffMillis = unixTimeMillis_ - base.unixTimeMillis_;

	switch (fieldType) {
	case FIELD_YEAR:	/* FALLTHROUGH */
	case FIELD_MONTH:
		{
			int32_t monthDay, hour, minute, second, milliSecond;

			int32_t thisYear, thisMonth;
			getFields(thisYear, thisMonth,
					monthDay, hour, minute, second, milliSecond, false);

			int32_t baseYear, baseMonth;
			base.getFields(baseYear, baseMonth,
					monthDay, hour, minute, second, milliSecond, false);

			const int64_t totalMonth =
					(static_cast<int64_t>(thisYear) * 12 + thisMonth) -
					(static_cast<int64_t>(baseYear) * 12 + baseMonth);
			if (fieldType == FIELD_YEAR) {
				return totalMonth / 12;
			}
			else {
				return totalMonth;
			}
		}
	case FIELD_DAY_OF_MONTH:
		return diffMillis / (24 * 60 * 60 * 1000);
	case FIELD_HOUR:
		return diffMillis / (60 * 60 * 1000);
	case FIELD_MINUTE:
		return diffMillis / (60 * 1000);
	case FIELD_SECOND:
		return diffMillis / 1000;
	case FIELD_MILLISECOND:
		return diffMillis;
	default:
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
				"Unsupported field type");
	}
}

void DateTime::format(
		std::ostream &s, bool trimMilliseconds, bool asLocalTimeZone) const {
	LocaleUtils::CLocaleScope localeScope(s);

	int32_t year, month, monthDay, hour, minute, second, milliSecond;

	try {
		getFields(year, month, monthDay, hour, minute, second, milliSecond,
				asLocalTimeZone);

		int32_t bias;
		if (asLocalTimeZone) {
			bias = static_cast<int32_t>(DateTime(
					year, month, monthDay, hour, minute, second, milliSecond,
					false).getUnixTime() - unixTimeMillis_) / 1000 / 60;
		}
		else {
			bias = 0;
		}

		const char orgFill = s.fill('0');
		try {
			s << std::setw(4) << year << "-" <<
					std::setw(2) << month << "-" <<
					std::setw(2) << monthDay << "T" <<
					std::setw(2) << hour << ":" <<
					std::setw(2) << minute << ":" <<
					std::setw(2) << second;
			if (!trimMilliseconds) {
				s << "." << std::setw(3) << milliSecond;
			}

			if (bias == 0) {
				s << "Z";
			}
			else if (bias > 0) {
				s << "+" <<
					std::setw(2) << (bias / 60) <<
					std::setw(2) << (bias % 60);
			}
			else {
				s << "-" <<
					std::setw(2) << (-bias / 60) <<
					std::setw(2) << (-bias % 60);
			}
		}
		catch (...) {
			s.fill(orgFill);
			throw;
		}
		s.fill(orgFill);
	}
	catch (...) {
		s.setstate(std::ios::failbit);
		if (s.exceptions() & std::ios::failbit) {
			throw;
		}
		return;
	}
}

bool DateTime::parse(
		const char8_t *str, DateTime &dateTime, bool trimMilliseconds) {
	try {
		util::NormalIStringStream iss(str);

		iss.unsetf(std::ios::skipws);

		char ch = '\0';
		int32_t year, month, monthDay, hour, minute, second, milliSecond;
		bool millisesondUsed;

		iss >> year;
		iss.get(ch);
		if (ch != '-') {
			return false;
		}

		iss >> std::setw(2) >> month;
		iss.get(ch);
		if (ch != '-') {
			return false;
		}

		iss >> std::setw(2) >> monthDay;
		iss.get(ch);
		if (ch != 'T') {
			return false;
		}

		iss >> std::setw(2) >> hour;
		iss.get(ch);
		if (ch != ':') {
			return false;
		}

		iss >> std::setw(2) >> minute;
		iss.get(ch);
		if (ch != ':') {
			return false;
		}

		iss >> std::setw(2) >> second;
		iss.get(ch);
		if (!iss.good()) {
			return false;
		}
		else if (ch == 'Z') {
			iss.get(ch);
			if (!iss.eof()) {
				return false;
			}

			milliSecond = 0;
			millisesondUsed = false;
		}
		else if (ch != '.' || trimMilliseconds) {
			return false;
		}
		else {
			iss >> std::setw(3) >> milliSecond;
			iss.get(ch);
			if (!iss.good() || ch != 'Z') {
				return false;
			}

			iss.get(ch);
			if (!iss.eof()) {
				return false;
			}
			millisesondUsed = true;
		}

		DateTime result(
				year, month, monthDay, hour, minute, second, milliSecond, false);

		NormalOStringStream oss;
		result.format(oss, !millisesondUsed, false);
		if (oss.str() != iss.str()) {
			return false;
		}

		dateTime = result;

		return true;
	}
	catch (...) {
		return false;
	}
}

DateTime DateTime::now(bool trimMilliseconds) {
#ifdef _WIN32
	FILETIME time;
	GetSystemTimeAsFileTime(&time);
	int64_t unixTime = FileLib::getUnixTime(time);
#else
	timespec time;
	if (clock_gettime(CLOCK_REALTIME, &time) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	int64_t unixTime = FileLib::getUnixTime(time);
#endif
	if (trimMilliseconds) {
		unixTime = unixTime / 1000 * 1000;
	}
	return DateTime(UTIL_FAILURE_SIMULATION_TIME_FILTER(unixTime));
}

DateTime DateTime::max(bool trimMilliseconds) {
	if (trimMilliseconds) {
		static const DateTime maxTime(getMaxUnixTime(true));
		return maxTime;
	}
	else {
		static const DateTime maxTime(getMaxUnixTime(false));
		return maxTime;
	}
}

int64_t DateTime::getMaxUnixTime(bool trimMilliseconds) {
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

	return (trimMilliseconds ? maxUnixTime / 1000 * 1000 : maxUnixTime);
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
