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
    @brief Definition of Utility of date and time
*/
#ifndef UTIL_TIME_H_
#define UTIL_TIME_H_

#include "util/type.h"

namespace util {

/*!
	@brief Manages date and time
*/
class DateTime {
public:

	enum FieldType {
		FIELD_YEAR,
		FIELD_MONTH,
		FIELD_DAY_OF_MONTH,
		FIELD_HOUR,
		FIELD_MINUTE,
		FIELD_SECOND,
		FIELD_MILLISECOND
	};

	static const int64_t INITIAL_UNIX_TIME;

	DateTime();

	DateTime(const DateTime &another);

	DateTime(const char8_t *str, bool trimMilliseconds);

	DateTime(int64_t unixTimeMillis);

	DateTime(
			int32_t year,
			int32_t month,
			int32_t dayOfMonth,
			int32_t hour,
			int32_t minute,
			int32_t second,
			int32_t milliSecond,
			bool asLocalTimeZone);

	DateTime& operator=(const DateTime &another);

	int64_t getUnixTime() const;

	void setUnixTime(int64_t unixTimeMillis);

	void getFields(
			int32_t &year,
			int32_t &month,
			int32_t &dayOfMonth,
			int32_t &hour,
			int32_t &minute,
			int32_t &second,
			int32_t &milliSecond,
			bool asLocalTimeZone) const;

	void setFields(
			int32_t year,
			int32_t month,
			int32_t monthDay,
			int32_t hour,
			int32_t minute,
			int32_t second,
			int32_t milliSecond,
			bool asLocalTimeZone);

	void addField(int64_t amount, FieldType fieldType);

	int64_t getDifference(const DateTime &base, FieldType fieldType) const;

	void format(std::ostream &s,
		bool trimMilliseconds, bool asLocalTimeZone = true) const;

	static bool parse(
		const char8_t *str, DateTime &dateTime, bool trimMilliseconds);

	static DateTime now(bool trimMilliseconds);

	static DateTime max(bool trimMilliseconds);

	bool operator==(const DateTime &another) const;
	bool operator!=(const DateTime &another) const;
	bool operator>(const DateTime &another) const;
	bool operator>=(const DateTime &another) const;
	bool operator<(const DateTime &another) const;
	bool operator<=(const DateTime &another) const;

private:
	static int64_t getMaxUnixTime(bool trimMilliseconds);

	int64_t unixTimeMillis_;
};

/*!
	@brief Measures time in high precision
*/
class Stopwatch {
public:
	enum Status {
		STATUS_STARTED,
		STATUS_STOPPED
	};

	Stopwatch(Status initialStatus = STATUS_STOPPED);

	void start();

	void reset();

	uint32_t stop();

	uint32_t elapsedMillis();

	uint64_t elapsedNanos();

	uint64_t elapsedClock();

	static uint32_t clockToMillis(uint64_t clockCount);

	static uint64_t currentClock();

	static uint64_t clocksPerSec();

private:
	Stopwatch(const Stopwatch&);
	Stopwatch& operator=(const Stopwatch&);

	Status status_;

	uint64_t startClock_;
	uint64_t elapsedClock_;
};


inline DateTime::DateTime() : unixTimeMillis_(INITIAL_UNIX_TIME) {
}

inline DateTime::DateTime(const DateTime &another) :
		unixTimeMillis_(another.unixTimeMillis_) {
}

inline DateTime::DateTime(int64_t unixTimeMillis) :
		unixTimeMillis_(unixTimeMillis) {
}

inline DateTime& DateTime::operator=(const DateTime &another) {
	unixTimeMillis_ = another.unixTimeMillis_;
	return *this;
}

inline int64_t DateTime::getUnixTime() const {
	return unixTimeMillis_;
}

inline void DateTime::setUnixTime(int64_t unixTimeMillis) {
	unixTimeMillis_ = unixTimeMillis;
}

inline bool DateTime::operator==(const DateTime &another) const {
	return (unixTimeMillis_ == another.unixTimeMillis_);
}

inline bool DateTime::operator!=(const DateTime &another) const {
	return (unixTimeMillis_ != another.unixTimeMillis_);
}

inline bool DateTime::operator>(const DateTime &another) const {
	return (unixTimeMillis_ > another.unixTimeMillis_);
}

inline bool DateTime::operator>=(const DateTime &another) const {
	return (unixTimeMillis_ >= another.unixTimeMillis_);
}

inline bool DateTime::operator<(const DateTime &another) const {
	return (unixTimeMillis_ < another.unixTimeMillis_);
}

inline bool DateTime::operator<=(const DateTime &another) const {
	return (unixTimeMillis_ <= another.unixTimeMillis_);
}

inline std::ostream& operator<<(
		std::ostream &s, const util::DateTime &dateTime) {
	dateTime.format(s, false);
	return s;
}

#if UTIL_FAILURE_SIMULATION_ENABLED
class DateTimeFailureSimulator {
public:

	static void set(int32_t mode, uint64_t startCount, uint64_t endCount,
			int32_t speedRate, int64_t startOffset, int64_t endDuration);

	static int64_t filterTime(int64_t time);

	static uint64_t filterClock(uint64_t clock);

	static uint64_t getLastOperationCount() { return lastOperationCount_; }

	enum CoveragePosition {
		COVERAGE_STOPWATCH_ELAPSED_NANOS1,
		COVERAGE_STOPWATCH_ELAPSED_NANOS2,
		COVERAGE_STOPWATCH_ELAPSED_NANOS3,
		COVERAGE_MAX
	};

	static void resetCoverage() {
		std::fill(coverage_, coverage_ + COVERAGE_MAX, 0);
	}

	static void traceCoverage(CoveragePosition pos) {
		coverage_[pos]++;
	}

	static uint64_t getCoverage(CoveragePosition pos) {
		return coverage_[pos];
	}

private:
	DateTimeFailureSimulator();
	~DateTimeFailureSimulator();

	static volatile int32_t mode_;
	static volatile uint64_t startCount_;
	static volatile uint64_t endCount_;

	static volatile int32_t speedRate_;
	static volatile int64_t startOffset_;
	static volatile int64_t endDuration_;

	static volatile int64_t startTime_;
	static volatile uint64_t lastOperationCount_;

	static volatile uint64_t coverage_[COVERAGE_MAX];
};
#endif	

} 

#endif
