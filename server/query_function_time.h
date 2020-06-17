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
	@brief Definition of query functions for time values
*/

#ifndef QUERY_FUNCTION_TIME_H_
#define QUERY_FUNCTION_TIME_H_

#include "query_function.h"

struct TimeFunctions {

	struct Strftime {
	public:
		template<typename C, typename R>
		typename C::WriterType& operator()(
				C &cxt, R &format, int64_t tsValue, R &zone);

		template<typename C, typename R>
		typename C::WriterType& operator()(
				C &cxt, R &format, int64_t tsValue,
				const util::TimeZone &zone = util::TimeZone());

	private:
		struct FormatContext;
		struct Formatter;
	};

};


struct TimeFunctions::Strftime::FormatContext {
	template<typename C>
	FormatContext(
			C &funcCxt, const util::DateTime &time,
			const util::TimeZone &zone);

	int64_t getFieldValue(util::DateTime::FieldType fieldType);

	util::DateTime time_;
	util::DateTime::ZonedOption option_;

	int32_t lastWidth_;
	util::DateTime::FieldData fieldData_;
	bool fieldResolved_;
};

struct TimeFunctions::Strftime::Formatter {
public:
	static const Formatter& getInstance(util::CodePoint key);

	void acceptWidth(const FormatContext &formatCxt) const;

	template<typename W>
	void format(FormatContext &formatCxt, W &writer) const;

private:
	enum Type {
		TYPE_DATE_TIME_FIELD,
		TYPE_TIME_VALUE,
		TYPE_TIME_ZONE,
		TYPE_KEY
	};

	static const Formatter *const FORMATTER_LIST;

	Formatter(char8_t key, Type type);

	static Formatter create(char8_t key, Type type);
	static Formatter create(
			char8_t key, util::DateTime::FieldType fieldType, int32_t width);

	Formatter& setWidthRange(int32_t min, int32_t max);
	Formatter& setFraction(const Formatter *fraction);

	static const Formatter* createFormatterList();

	char8_t key_;
	Type type_;
	util::DateTime::FieldType fieldType_;
	int32_t width_;
	int32_t minWidth_;
	int32_t maxWidth_;
	const Formatter *fraction_;
};


template<typename C, typename R>
inline typename C::WriterType& TimeFunctions::Strftime::operator()(
		C &cxt, R &format, int64_t tsValue, R &zone) {
	return (*this)(
			cxt, format, tsValue, FunctionUtils::resolveTimeZone(zone));
}

template<typename C, typename R>
inline typename C::WriterType& TimeFunctions::Strftime::operator()(
		C &cxt, R &format, int64_t tsValue,
		const util::TimeZone &zone) {
	typename C::WriterType &writer = cxt.getResultWriter();
	FormatContext formatCxt(cxt, util::DateTime(tsValue), zone);

	bool escaping = false;
	for (util::CodePoint c; format.nextCode(&c);) {
		if (!escaping) {
			if (c == '%') {
				escaping = true;
			}
			else {
				writer.appendCode(c);
			}
			continue;
		}
		else if (formatCxt.lastWidth_ < 0 && '0' <= c && c <= '9') {
			formatCxt.lastWidth_ = static_cast<int32_t>(c - '0');
			continue;
		}
		escaping = false;

		Formatter::getInstance(c).format(formatCxt, writer);
		formatCxt.lastWidth_ = -1;
	}

	return writer;
}


template<typename C>
TimeFunctions::Strftime::FormatContext::FormatContext(
		C &funcCxt, const util::DateTime &time, const util::TimeZone &zone) :
		time_(time),
		lastWidth_(-1),
		fieldResolved_(false) {
	funcCxt.applyDateTimeOption(option_);
	option_.zone_ = FunctionUtils::resolveTimeZone(funcCxt, zone);
}

inline int64_t TimeFunctions::Strftime::FormatContext::getFieldValue(
		util::DateTime::FieldType fieldType) {
	if (fieldType < static_cast<int32_t>(
			util::DateTime::END_PRIMITIVE_FIELD)) {
		if (!fieldResolved_) {
			try {
				time_.getFields(fieldData_, option_);
			}
			catch (std::exception &e) {
				FunctionUtils::TimeErrorHandler().errorTimeFormat(e);
				return 0;
			}
			fieldResolved_ = true;
		}
		return fieldData_.getValue(fieldType);
	}
	else {
		return time_.getField(fieldType, option_);
	}
}


inline const TimeFunctions::Strftime::Formatter&
TimeFunctions::Strftime::Formatter::getInstance(util::CodePoint key) {
	for (const Formatter *it = FORMATTER_LIST; it->key_ != char8_t(); ++it) {
		if (static_cast<util::CodePoint>(it->key_) == key) {
			return *it;
		}
	}
	GS_THROW_USER_ERROR(GS_ERROR_QF_VALUE_OUT_OF_RANGE,
			"Illegal timestamp format");
}

inline void TimeFunctions::Strftime::Formatter::acceptWidth(
		const FormatContext &formatCxt) const {
	const int32_t width = formatCxt.lastWidth_;
	if (width < 0 && maxWidth_ < 0) {
		return;
	}

	if (width < minWidth_ || maxWidth_ < width) {
		GS_THROW_USER_ERROR(GS_ERROR_QF_VALUE_OUT_OF_RANGE,
				"Illegal timestamp format because of unacceptable or "
				"empty width option");
	}
}

template<typename W>
inline void TimeFunctions::Strftime::Formatter::format(
		FormatContext &formatCxt, W &writer) const {
	acceptWidth(formatCxt);
	switch (type_) {
	case TYPE_DATE_TIME_FIELD:
		{
			const int64_t fieldValue = formatCxt.getFieldValue(fieldType_);
			util::SequenceUtils::appendValue(
					writer, static_cast<uint32_t>(fieldValue), width_);
		}
		if (fraction_ != NULL) {
			const util::CodePoint dot = '.';
			writer.appendCode(dot);
			fraction_->format(formatCxt, writer);
		}
		break;
	case TYPE_TIME_VALUE:
		formatCxt.time_.writeTo(
				writer, formatCxt.option_, FunctionUtils::TimeErrorHandler());
		break;
	case TYPE_TIME_ZONE:
		{
			const bool resolving = true;
			formatCxt.option_.zone_.writeTo(writer, resolving);
		}
		break;
	case TYPE_KEY:
		{
			const util::CodePoint code = key_;
			writer.appendCode(code);
		}
		break;
	default:
		assert(false);
		break;
	}
}

inline TimeFunctions::Strftime::Formatter::Formatter(char8_t key, Type type) :
		key_(key),
		type_(type),
		fieldType_(util::DateTime::FieldType()),
		width_(0),
		minWidth_(-1),
		maxWidth_(-1),
		fraction_(NULL) {
}

inline TimeFunctions::Strftime::Formatter
TimeFunctions::Strftime::Formatter::create(char8_t key, Type type) {
	return Formatter(key, type);
}

inline TimeFunctions::Strftime::Formatter
TimeFunctions::Strftime::Formatter::create(
		char8_t key, util::DateTime::FieldType fieldType, int32_t width) {
	Formatter formatter(key, TYPE_DATE_TIME_FIELD);
	formatter.fieldType_ = fieldType;
	formatter.width_ = width;
	return formatter;
}

inline TimeFunctions::Strftime::Formatter&
TimeFunctions::Strftime::Formatter::setWidthRange(int32_t min, int32_t max) {
	minWidth_ = min;
	maxWidth_ = max;
	return *this;
}

inline TimeFunctions::Strftime::Formatter&
TimeFunctions::Strftime::Formatter::setFraction(const Formatter *fraction) {
	fraction_ = fraction;
	return *this;
}

inline const TimeFunctions::Strftime::Formatter*
TimeFunctions::Strftime::Formatter::createFormatterList() {

	const Formatter fractionalSecond =
			create('f', util::DateTime::FIELD_MILLISECOND, 3)
			.setWidthRange(3, 3);

	const Formatter sentinel = create(char8_t(), TYPE_KEY);

	static const Formatter list[] = {
		create('Y', util::DateTime::FIELD_YEAR, 4),
		create('m', util::DateTime::FIELD_MONTH, 2),
		create('d', util::DateTime::FIELD_DAY_OF_MONTH, 2),
		create('H', util::DateTime::FIELD_HOUR, 2),
		create('M', util::DateTime::FIELD_MINUTE, 2),
		create('S', util::DateTime::FIELD_SECOND, 2),
		fractionalSecond,
		create('w', util::DateTime::FIELD_DAY_OF_WEEK, 1),
		create('W', util::DateTime::FIELD_WEEK_OF_YEAR_MONDAY, 2),
		create('j', util::DateTime::FIELD_DAY_OF_YEAR, 3),
		create('c', TYPE_TIME_VALUE),
		create('z', TYPE_TIME_ZONE),
		create('%', TYPE_KEY),
		sentinel
	};

	return list;
}

#endif
