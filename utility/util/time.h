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

class TimeZone {
public:
	typedef int64_t Offset;

	static const size_t MAX_FORMAT_SIZE = 7;

	TimeZone();

	static TimeZone getLocalTimeZone(int64_t unixTimeMillis);
	static TimeZone getUTCTimeZone();

	bool isEmpty() const;
	bool checkRange(bool throwOnError) const;

	Offset getOffsetMillis() const;
	void setOffsetMillis(Offset millis);

	void format(std::ostream &s) const;
	size_t format(char8_t *buf, size_t size) const;

	bool parse(const char8_t *buf, size_t size, bool throwOnError);

	template<typename W>
	void writeTo(W &writer, bool resolving = true) const;

	template<typename R, typename H>
	static TimeZone readFrom(R &reader, const H &errorHandler);

private:

	struct Constants {
		static Offset emptyOffsetMillis();
		static Offset offsetMillisRange();
	};

	static Offset getLocalOffsetMillis();
	static Offset detectLocalOffsetMillis();

	Offset offsetMillis_;
};

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
		FIELD_MILLISECOND,

		END_PRIMITIVE_FIELD,

		FIELD_DAY_OF_WEEK,
		FIELD_DAY_OF_YEAR,
		FIELD_WEEK_OF_YEAR_SUNDAY, 
		FIELD_WEEK_OF_YEAR_MONDAY, 
		FIELD_WEEK_OF_YEAR_ISO, 
		FIELD_YEAR_ISO 
	};

	struct FieldData;
	struct Option;
	struct ZonedOption;

	static const size_t MAX_FORMAT_SIZE = 32;

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
	void setUnixTime(int64_t unixTimeMillis, const Option &option);

	void getFields(FieldData &fieldData, const ZonedOption &option) const;
	void getFields(
			int32_t &year,
			int32_t &month,
			int32_t &dayOfMonth,
			int32_t &hour,
			int32_t &minute,
			int32_t &second,
			int32_t &milliSecond,
			bool asLocalTimeZone) const;

	int64_t getField(FieldType type, const ZonedOption &option) const;

	void setFields(
			const FieldData &fieldData, const ZonedOption &option,
			bool strict = true);
	void setFields(
			int32_t year,
			int32_t month,
			int32_t monthDay,
			int32_t hour,
			int32_t minute,
			int32_t second,
			int32_t milliSecond,
			bool asLocalTimeZone);

	void addField(
			int64_t amount, FieldType fieldType, const ZonedOption &option);
	void addField(int64_t amount, FieldType fieldType);

	int64_t getDifference(
			const DateTime &base, FieldType fieldType,
			const ZonedOption &option) const;
	int64_t getDifference(const DateTime &base, FieldType fieldType) const;

	void format(std::ostream &s, const ZonedOption &option) const;
	void format(
			std::ostream &s, bool trimMilliseconds,
			bool asLocalTimeZone = true) const;

	size_t format(char8_t *buf, size_t size, const ZonedOption &option) const;

	bool parse(
			const char8_t *buf, size_t size, bool throwOnError,
			const ZonedOption &option);
	static bool parse(
			const char8_t *str, DateTime &dateTime, bool trimMilliseconds);

	template<typename W, typename H>
	void writeTo(
			W &writer, const ZonedOption &option, const H& errorHandler) const;

	static DateTime now(const Option &option);
	static DateTime now(bool trimMilliseconds);

	static DateTime max(const Option &option);
	static DateTime max(bool trimMilliseconds);

	bool operator==(const DateTime &another) const;
	bool operator!=(const DateTime &another) const;
	bool operator>(const DateTime &another) const;
	bool operator>=(const DateTime &another) const;
	bool operator<(const DateTime &another) const;
	bool operator<=(const DateTime &another) const;

private:
	static const int32_t EPOCH_DAY_OF_WEEK;

	static TimeZone::Offset getLocalOffsetMillis();

	int64_t getUnixTimeDays(
			const FieldData &fieldData, FieldType trimingFieldType,
			const ZonedOption &option) const;
	static int64_t unixTimeDaysToWeek(int64_t timeDays);

	static void checkFieldBounds(
			const FieldData &fields, const ZonedOption &option);
	static const FieldData* resolveMaxFields(
			const ZonedOption &option, FieldData &localFields);

	static void checkUnixTimeBounds(
			int64_t unixTimeMillis, const Option &option);
	static int64_t resolveMaxUnixTime(const Option &option);

	static int64_t getMaxUnixTime(bool trimMilliseconds);

	int64_t unixTimeMillis_;
};

struct DateTime::FieldData {
	void initialize();

	template<FieldType T> int32_t getValue() const;
	template<FieldType T> void setValue(int32_t value);

	int32_t getValue(FieldType type) const;
	void setValue(FieldType type, int32_t value);

	int32_t year_;
	int32_t month_;
	int32_t monthDay_;
	int32_t hour_;
	int32_t minute_;
	int32_t second_;
	int32_t milliSecond_;
};

struct DateTime::Option {
	Option();

	static Option create(bool trimMilliseconds);

	bool trimMilliseconds_;
	int64_t maxTimeMillis_;
};

struct DateTime::ZonedOption {
	ZonedOption();

	static ZonedOption create(bool trimMilliseconds, const TimeZone &zone);

	Option baseOption_;

	bool asLocalTimeZone_;
	TimeZone zone_;
	const FieldData *maxFields_;
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



template<typename W>
void TimeZone::writeTo(W &writer, bool resolving) const {
	TimeZone resolvedZone = *this;
	if (resolvedZone.isEmpty() && resolving) {
		resolvedZone = getUTCTimeZone();
	}

	char8_t buf[MAX_FORMAT_SIZE];
	const size_t size = resolvedZone.format(buf, sizeof(buf));
	writer.append(buf, size);
}

template<typename R, typename H>
TimeZone TimeZone::readFrom(R &reader, const H &errorHandler) {
	const size_t size = reader.getNext();
	const char8_t *buf = (size > 0 ? &reader.get() : NULL);

	TimeZone zone;
	try {
		const bool throwOnError = true;
		zone.parse(buf, size, throwOnError);
	}
	catch (UtilityException &e) {
		if (e.getErrorCode() != UtilityException::CODE_INVALID_PARAMETER) {
			throw;
		}
		errorHandler.errorTimeParse(e);
	}
	return zone;
}

inline std::ostream& operator<<(std::ostream &s, const TimeZone &zone) {
	zone.format(s);
	return s;
}


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

template<typename W, typename H>
void DateTime::writeTo(
		W &writer, const ZonedOption &option, const H& errorHandler) const {
	char8_t buf[MAX_FORMAT_SIZE];
	size_t size;
	try {
		size = format(buf, sizeof(buf), option);
	}
	catch (std::exception &e) {
		errorHandler.errorTimeFormat(e);
		return;
	}
	writer.append(buf, size);
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

inline std::ostream& operator<<(std::ostream &s, const DateTime &dateTime) {
	dateTime.format(s, false);
	return s;
}


template<DateTime::FieldType T> int32_t DateTime::FieldData::getValue() const {
	UTIL_STATIC_ASSERT(sizeof(T) < 0);
	return int32_t();
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_YEAR>() const {
	return year_;
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_MONTH>() const {
	return month_;
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_DAY_OF_MONTH>() const {
	return monthDay_;
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_HOUR>() const {
	return hour_;
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_MINUTE>() const {
	return minute_;
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_SECOND>() const {
	return second_;
}

template<> inline
int32_t DateTime::FieldData::getValue<DateTime::FIELD_MILLISECOND>() const {
	return milliSecond_;
}

template<DateTime::FieldType T>
void DateTime::FieldData::setValue(int32_t value) {
	UTIL_STATIC_ASSERT(sizeof(T) < 0);
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_YEAR>(
		int32_t value) {
	year_ = value;
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_MONTH>(
		int32_t value) {
	month_ = value;
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_DAY_OF_MONTH>(
		int32_t value) {
	monthDay_ = value;
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_HOUR>(
		int32_t value) {
	hour_ = value;
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_MINUTE>(
		int32_t value) {
	minute_ = value;
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_SECOND>(
		int32_t value) {
	second_ = value;
}

template<>
inline void DateTime::FieldData::setValue<DateTime::FIELD_MILLISECOND>(
		int32_t value) {
	milliSecond_ = value;
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
