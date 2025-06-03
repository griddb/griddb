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

	static TimeZone getLocalTimeZone();
	static TimeZone getUTCTimeZone();

	static TimeZone ofOffsetMillis(Offset millis);

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
private:
	enum {
		DIGITS_YEAR_MIN = 4,
		DIGITS_YEAR_MAX = 5,
		DIGITS_FRACTION_UNIT = 3,
		DIGITS_OTHER = 2,
		DIGITS_SEPARATORS = 6,
		DIGITS_ZONE = (TimeZone::MAX_FORMAT_SIZE - 1),

		DIGITS_MAX = (
				DIGITS_YEAR_MAX +
				DIGITS_OTHER * 5 +
				DIGITS_FRACTION_UNIT * 3 +
				DIGITS_SEPARATORS +
				DIGITS_ZONE)
	};

public:

	enum FieldType {
		FIELD_YEAR,
		FIELD_MONTH,
		FIELD_DAY_OF_MONTH,
		FIELD_HOUR,
		FIELD_MINUTE,
		FIELD_SECOND,
		FIELD_MILLISECOND,
		FIELD_MICROSECOND,
		FIELD_NANOSECOND,

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

	class Formatter;
	class Parser;

	static const size_t MAX_FORMAT_SIZE = (DIGITS_MAX + 1);

	static const int64_t INITIAL_UNIX_TIME;

	DateTime();

	DateTime(const DateTime &another);

	DateTime(const char8_t *str, bool fractionTrimming);

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
			std::ostream &s, bool fractionTrimming,
			bool asLocalTimeZone = true) const;

	size_t format(char8_t *buf, size_t size, const ZonedOption &option) const;

	Formatter getFormatter(const ZonedOption &option) const;

	bool parse(
			const char8_t *buf, size_t size, bool throwOnError,
			const ZonedOption &option);
	static bool parse(
			const char8_t *str, DateTime &dateTime, bool fractionTrimming);

	Parser getParser(const ZonedOption &option);

	template<typename W, typename H>
	void writeTo(
			W &writer, const ZonedOption &option, const H& errorHandler) const;

	static DateTime now(const Option &option);
	static DateTime now(bool fractionTrimming);

	static DateTime max(const Option &option);
	static DateTime max(bool fractionTrimming);

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

	static int64_t getMaxUnixTime(bool fractionTrimming);

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
	static const FieldType PRECISION_NONE = END_PRIMITIVE_FIELD;
	static const FieldType PRECISION_DEFAULT = FIELD_MILLISECOND;

	Option();

	static Option create(bool fractionTrimming);

	Option withDefaultPrecision(FieldType defaultType) const;
	static FieldType makePrecision(bool fractionTrimming);
	static FieldType resolvePrecision(FieldType base, FieldType defaultType);

	uint32_t getFractionalDigits() const;
	static uint32_t getFractionalDigits(FieldType precision);

	bool isFractionTrimming() const;

	FieldType precision_;
	int64_t maxTimeMillis_;
};

struct DateTime::ZonedOption {
	ZonedOption();

	static ZonedOption create(bool fractionTrimming, const TimeZone &zone);

	Option baseOption_;

	bool asLocalTimeZone_;
	TimeZone zone_;
	const FieldData *maxFields_;
};

class DateTime::Formatter {
public:
	Formatter(const DateTime &dateTime, const ZonedOption &option);

	void operator()(std::ostream &s) const;
	size_t operator()(char8_t *buf, size_t size) const;

	template<typename W, typename H>
	void writeTo(W &writer, const H &errorHandler) const;

	Formatter withDefaultPrecision(FieldType defaultPrecision) const;

	void setDefaultPrecision(FieldType defaultPrecision);
	void setNanoSeconds(uint32_t nanoSeconds);

private:
	size_t format(char8_t *buf, size_t size) const;

	static void formatMain(
			char8_t *&it, char8_t *end, const FieldData &fieldData);
	static void formatFraction(
			char8_t *&it, char8_t *end, const FieldData &fieldData,
			uint32_t nanoSeconds, const Option &option);

	DateTime dateTime_;
	ZonedOption option_;
	FieldType defaultPrecision_;
	uint32_t nanoSeconds_;
};

class DateTime::Parser {
public:
	Parser(DateTime &dateTime, const ZonedOption &option);

	bool operator()(const char8_t *buf, size_t size, bool throwOnError) const;

	void setDefaultPrecision(FieldType defaultPrecision);
	void setNanoSecondsRef(uint32_t *nanoSecondsRef);

private:
	bool parse(const char8_t *buf, size_t size, bool throwOnError) const;

	static bool parseMain(
			const char8_t *&it, const char8_t *end, FieldData &fieldData);
	static bool parseFraction(
			const char8_t *&it, const char8_t *end, FieldData &fieldData,
			uint32_t &nanoSeconds, const Option &option);

	static bool errorParse(bool throwOnError);

	DateTime &dateTime_;
	ZonedOption option_;
	FieldType defaultPrecision_;
	uint32_t *nanoSecondsRef_;
};

class PreciseDateTime {
public:
	struct FieldData;

	typedef DateTime::FieldType FieldType;
	typedef DateTime::Option Option;
	typedef DateTime::ZonedOption ZonedOption;
	typedef DateTime::Formatter Formatter;
	typedef DateTime::Parser Parser;

	PreciseDateTime();

	static PreciseDateTime ofNanoSeconds(
			const DateTime &base, uint32_t nanoSeconds);

	static PreciseDateTime of(const DateTime &src);
	static PreciseDateTime of(const PreciseDateTime &src);

	const DateTime& getBase() const;
	DateTime& getBase();

	uint32_t getNanoSeconds() const;

	void getFields(FieldData &fieldData, const ZonedOption &option) const;
	int64_t getField(FieldType type, const ZonedOption &option) const;

	void setFields(
			const FieldData &fieldData, const ZonedOption &option,
			bool strict = true);

	void addField(
			int64_t amount, FieldType fieldType, const ZonedOption &option);

	int64_t getDifference(
			const PreciseDateTime &base, FieldType fieldType,
			const ZonedOption &option) const;
	static int64_t makeDifference(
			int64_t millisDiff, int64_t nanosDiff, int32_t unit);

	Formatter getFormatter(const ZonedOption &option) const;
	Parser getParser(const ZonedOption &option);

	static PreciseDateTime max(const Option &option);

private:
	void addPreciseField(
			int64_t amount, int32_t unit, const ZonedOption &option);
	static int64_t getPreciseDifference(
			int64_t millisDiff, uint32_t nanos1, uint32_t nanos2,
			uint32_t unit);

	static int32_t compareBase(
			const PreciseDateTime &t1, const PreciseDateTime &t2);
	static int32_t compareNanos(
			const PreciseDateTime &t1, const PreciseDateTime &t2);

	DateTime base_;
	uint32_t nanoSeconds_;
};

struct PreciseDateTime::FieldData {
	void initialize();

	template<FieldType T> int32_t getValue() const;
	template<FieldType T> void setValue(int32_t value);

	int32_t getValue(FieldType type) const;
	void setValue(FieldType type, int32_t value);

	DateTime::FieldData baseFields_;
	uint32_t nanoSecond_;
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

inline DateTime::Formatter DateTime::getFormatter(
		const ZonedOption &option) const {
	return Formatter(*this, option);
}

inline DateTime::Parser DateTime::getParser(const ZonedOption &option) {
	return Parser(*this, option);
}

template<typename W, typename H>
void DateTime::writeTo(
		W &writer, const ZonedOption &option, const H& errorHandler) const {
	getFormatter(option).writeTo(writer, errorHandler);
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


inline DateTime::Formatter::Formatter(
		const DateTime &dateTime, const ZonedOption &option) :
		dateTime_(dateTime),
		option_(option),
		defaultPrecision_(Option::PRECISION_NONE),
		nanoSeconds_(0) {
}

inline size_t DateTime::Formatter::operator()(
		char8_t *buf, size_t size) const {
	return format(buf, size);
}

template<typename W, typename H>
void DateTime::Formatter::writeTo(W &writer, const H &errorHandler) const {
	char8_t buf[MAX_FORMAT_SIZE];
	size_t size;
	try {
		size = format(buf, sizeof(buf));
	}
	catch (std::exception &e) {
		errorHandler.errorTimeFormat(e);
		return;
	}
	writer.append(buf, size);
}

inline DateTime::Formatter DateTime::Formatter::withDefaultPrecision(
		FieldType defaultPrecision) const {
	Formatter formatter = *this;
	formatter.setDefaultPrecision(defaultPrecision);
	return formatter;
}

inline void DateTime::Formatter::setDefaultPrecision(
		FieldType defaultPrecision) {
	defaultPrecision_ = defaultPrecision;
}

inline void DateTime::Formatter::setNanoSeconds(uint32_t nanoSeconds) {
	nanoSeconds_ = nanoSeconds;
}

inline std::ostream& operator<<(
		std::ostream &s, const DateTime::Formatter &formatter) {
	formatter(s);
	return s;
}


inline DateTime::Parser::Parser(
		DateTime &dateTime, const ZonedOption &option) :
		dateTime_(dateTime),
		option_(option),
		defaultPrecision_(Option::PRECISION_NONE),
		nanoSecondsRef_(NULL) {
}

inline bool DateTime::Parser::operator()(
		const char8_t *buf, size_t size, bool throwOnError) const {
	return parse(buf, size, throwOnError);
}

inline void DateTime::Parser::setDefaultPrecision(
		FieldType defaultPrecision) {
	defaultPrecision_ = defaultPrecision;
}

inline void DateTime::Parser::setNanoSecondsRef(uint32_t *nanoSecondsRef) {
	nanoSecondsRef_ = nanoSecondsRef;
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


inline PreciseDateTime::PreciseDateTime() :
		nanoSeconds_(0) {
}

inline PreciseDateTime PreciseDateTime::ofNanoSeconds(
		const DateTime &base, uint32_t nanoSeconds) {
	PreciseDateTime dateTime;
	dateTime.base_ = base;
	dateTime.nanoSeconds_ = nanoSeconds;
	return dateTime;
}

inline PreciseDateTime PreciseDateTime::of(const DateTime &src) {
	PreciseDateTime dateTime;
	dateTime.base_ = src;
	return dateTime;
}

inline PreciseDateTime PreciseDateTime::of(const PreciseDateTime &src) {
	return src;
}

inline const DateTime& PreciseDateTime::getBase() const {
	return base_;
}

inline DateTime& PreciseDateTime::getBase() {
	return base_;
}

inline uint32_t PreciseDateTime::getNanoSeconds() const {
	return nanoSeconds_;
}

inline DateTime::Formatter PreciseDateTime::getFormatter(
		const ZonedOption &option) const {
	Formatter formatter(base_, option);
	formatter.setDefaultPrecision(DateTime::FIELD_NANOSECOND);
	formatter.setNanoSeconds(nanoSeconds_);
	return formatter;
}

inline DateTime::Parser PreciseDateTime::getParser(const ZonedOption &option) {
	Parser parser(base_, option);
	parser.setDefaultPrecision(DateTime::FIELD_NANOSECOND);
	parser.setNanoSecondsRef(&nanoSeconds_);
	return parser;
}

inline PreciseDateTime PreciseDateTime::max(const Option &option) {
	const uint32_t maxNanoSeconds =
			(option.isFractionTrimming() ? 0 : 1000 * 1000 -1);
	return ofNanoSeconds(DateTime::max(option), maxNanoSeconds);
}

inline std::ostream& operator<<(
		std::ostream &s, const PreciseDateTime &dateTime) {
	dateTime.getFormatter(DateTime::ZonedOption())(s);
	return s;
}


template<DateTime::FieldType T>
int32_t PreciseDateTime::FieldData::getValue() const {
	UTIL_STATIC_ASSERT(
			T != DateTime::FIELD_MILLISECOND &&
			T != DateTime::FIELD_MICROSECOND &&
			T != DateTime::FIELD_NANOSECOND);
	return baseFields_.getValue<T>();
}

template<>
inline int32_t PreciseDateTime::FieldData::getValue<
		DateTime::FIELD_MILLISECOND>() const {
	return baseFields_.getValue<DateTime::FIELD_MILLISECOND>();
}

template<>
inline int32_t PreciseDateTime::FieldData::getValue<
		DateTime::FIELD_MICROSECOND>() const {
	return (baseFields_.milliSecond_ * 1000) +
			static_cast<int32_t>(nanoSecond_ / 1000);
}

template<>
inline int32_t PreciseDateTime::FieldData::getValue<
		DateTime::FIELD_NANOSECOND>() const {
	return (baseFields_.milliSecond_ * (1000 * 1000)) +
			static_cast<int32_t>(nanoSecond_);
}

template<DateTime::FieldType T>
void PreciseDateTime::FieldData::setValue(int32_t value) {
	UTIL_STATIC_ASSERT(
			T != DateTime::FIELD_MILLISECOND &&
			T != DateTime::FIELD_MICROSECOND &&
			T != DateTime::FIELD_NANOSECOND);
	baseFields_.setValue<T>(value);
}

template<>
inline void PreciseDateTime::FieldData::setValue<DateTime::FIELD_MILLISECOND>(
		int32_t value) {
	baseFields_.setValue<DateTime::FIELD_MILLISECOND>(value);
	nanoSecond_ = 0;
}

template<>
inline void PreciseDateTime::FieldData::setValue<DateTime::FIELD_MICROSECOND>(
		int32_t value) {
	baseFields_.milliSecond_ = value / 1000;
	nanoSecond_ = (static_cast<uint32_t>(value) % 1000) * 1000;
}

template<>
inline void PreciseDateTime::FieldData::setValue<DateTime::FIELD_NANOSECOND>(
		int32_t value) {
	baseFields_.milliSecond_ = value / (1000 * 1000);
	nanoSecond_ = static_cast<uint32_t>(value) % (1000 * 1000);
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
