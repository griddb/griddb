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

#include "sql_expression_time.h"
#include "query_function_time.h"


const SQLExprs::ExprRegistrar
SQLTimeExprs::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLTimeExprs::Registrar::operator()() const {
	add<SQLType::FUNC_EXTRACT, Functions::Extract>();
	add<SQLType::FUNC_MAKE_TIMESTAMP, Functions::MakeTimestamp>();
	add<SQLType::FUNC_MAKE_TIMESTAMP_BY_DATE, Functions::MakeTimestamp>();
	add<SQLType::FUNC_NOW, Functions::Now>();
	add<SQLType::FUNC_STRFTIME, TimeFunctions::Strftime>();
	add<SQLType::FUNC_TO_EPOCH_MS, Functions::ToEpochMs>();
	add<SQLType::FUNC_TO_TIMESTAMP_MS, Functions::ToTimestampMs>();
	add<SQLType::FUNC_TIMESTAMP, Functions::TimestampFunc>();
	add<SQLType::FUNC_TIMESTAMP_ADD, Functions::TimestampAdd>();
	add<SQLType::FUNC_TIMESTAMP_DIFF, Functions::TimestampDiff>();
	add<SQLType::FUNC_TIMESTAMP_TRUNC, Functions::TimestampTrunc>();
	add<SQLType::FUNC_TIMESTAMPADD, Functions::TimestampAdd>();
	add<SQLType::FUNC_TIMESTAMPDIFF, Functions::TimestampDiff>();
}


template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::Extract::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue, R &zone) {
	return (*this)(
			cxt, fieldTypeValue, tsValue,
			FunctionUtils::resolveTimeZone(zone));
}

template<typename C>
inline int64_t SQLTimeExprs::Functions::Extract::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue,
		const util::TimeZone &zone) {
	util::DateTime::ZonedOption option;
	cxt.applyDateTimeOption(option);
	option.zone_ = FunctionUtils::resolveTimeZone(cxt, zone);

	const util::DateTime::FieldType field =
			SQLValues::ValueUtils::toTimestampField(fieldTypeValue);

	int64_t fieldValue;
	try {
		fieldValue = util::DateTime(tsValue).getField(field, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Unacceptable timestamp specified"));
	}

	return fieldValue;
}


template<typename C, typename Sec, typename R>
inline int64_t SQLTimeExprs::Functions::MakeTimestamp::operator()(
		C &cxt, int64_t year, int64_t month, int64_t day,
		int64_t hour, int64_t min, Sec second, R &zone) {
	return (*this)(
			cxt, year, month, day, hour, min, second,
			FunctionUtils::resolveTimeZone(zone));
}

template<typename C, typename Sec>
inline int64_t SQLTimeExprs::Functions::MakeTimestamp::operator()(
		C &cxt, int64_t year, int64_t month, int64_t day,
		int64_t hour, int64_t min, Sec second,
		const util::TimeZone &zone) {
	util::DateTime::FieldData fieldData;
	fieldData.initialize();

	setFieldValue<util::DateTime::FIELD_YEAR>(fieldData, year);
	setFieldValue<util::DateTime::FIELD_MONTH>(fieldData, month);
	setFieldValue<util::DateTime::FIELD_DAY_OF_MONTH>(fieldData, day);
	setFieldValue<util::DateTime::FIELD_HOUR>(fieldData, hour);
	setFieldValue<util::DateTime::FIELD_MINUTE>(fieldData, min);

	const std::pair<int64_t, int64_t> secondsParts = decomposeSeconds(second);
	setFieldValue<util::DateTime::FIELD_SECOND>(fieldData, secondsParts.first);
	setFieldValue<util::DateTime::FIELD_MILLISECOND>(
			fieldData, secondsParts.second);

	util::DateTime::ZonedOption option;
	cxt.applyDateTimeOption(option);
	option.zone_ = FunctionUtils::resolveTimeZone(cxt, zone);

	util::DateTime ret;
	try {
		ret.setFields(fieldData, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_INVALID_EXPRESSION_INPUT, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Invalid timestamp fields"));
	}

	return ret.getUnixTime();
}

template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::MakeTimestamp::operator()(
		C &cxt, int64_t year, int64_t month, int64_t day,
		R &zone) {
	int64_t second = 0;
	return (*this)(cxt, year, month, day, 0, 0, second, zone);
}

template<typename C>
inline int64_t SQLTimeExprs::Functions::MakeTimestamp::operator()(
		C &cxt, int64_t year, int64_t month, int64_t day,
		const util::TimeZone &zone) {
	int64_t second = 0;
	return (*this)(cxt, year, month, day, 0, 0, second, zone);
}

template<util::DateTime::FieldType T>
inline void SQLTimeExprs::Functions::MakeTimestamp::setFieldValue(
		util::DateTime::FieldData &fieldData, int64_t value) {
	const int32_t intValue =
			SQLValues::ValueUtils::toIntegral<int32_t>(TupleValue(value));
	fieldData.setValue<T>(intValue);
}

inline std::pair<int64_t, int64_t>
SQLTimeExprs::Functions::MakeTimestamp::decomposeSeconds(int64_t value) {
	return std::pair<int64_t, int64_t>(value, 0);
}

inline std::pair<int64_t, int64_t>
SQLTimeExprs::Functions::MakeTimestamp::decomposeSeconds(double value) {
	if (!(0 <= value && value < 60)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INVALID_EXPRESSION_INPUT,
				"Seconds value out of range");
	}

	typedef FunctionUtils::NumberArithmetic NumberArithmetic;

	const int64_t integralPart = static_cast<int64_t>(std::floor(value));
	const int64_t floatingPart = std::min<int64_t>(static_cast<int64_t>(
			NumberArithmetic::remainder(value, 1.0) * 1000 + 0.5), 999);

	return std::make_pair(integralPart, floatingPart);
}


template<typename C>
inline int64_t SQLTimeExprs::Functions::Now::operator()(C &cxt) {
	return cxt.getCurrentTimeMillis();
}


template<typename C>
inline int64_t SQLTimeExprs::Functions::ToEpochMs::operator()(
		C &cxt, int64_t tsValue) {
	static_cast<void>(cxt);
	return tsValue;
}


template<typename C>
inline int64_t SQLTimeExprs::Functions::ToTimestampMs::operator()(
		C &cxt, int64_t millis) {
	static_cast<void>(cxt);
	return SQLValues::ValueUtils::checkTimestamp(millis);
}


template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::TimestampFunc::operator()(
		C &cxt, R &value, R &zone) {
	return (*this)(cxt, value, FunctionUtils::resolveTimeZone(zone));
}

template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::TimestampFunc::operator()(
		C &cxt, R &value, const util::TimeZone &zone) {
	return SQLValues::ValueUtils::parseTimestamp(
			SQLValues::ValueUtils::partToStringBuffer(value),
			FunctionUtils::resolveTimeZone(cxt, zone), !zone.isEmpty(),
			cxt.getAllocator());
}


template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::TimestampTrunc::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue, R &zone) {
	return (*this)(
			cxt, fieldTypeValue, tsValue,
			FunctionUtils::resolveTimeZone(zone));
}

template<typename C>
inline int64_t SQLTimeExprs::Functions::TimestampTrunc::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue,
		const util::TimeZone &zone) {
	const util::DateTime::FieldType field =
			SQLValues::ValueUtils::toTimestampField(fieldTypeValue);

	util::DateTime::ZonedOption option;
	cxt.applyDateTimeOption(option);
	option.zone_ = FunctionUtils::resolveTimeZone(cxt, zone);

	util::DateTime ts(tsValue);
	return TimeFunctionUtils::trunc(ts, field, option).getUnixTime();
}


template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::TimestampAdd::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue, int64_t amount,
		R &zone) {
	return (*this)(
			cxt, fieldTypeValue, tsValue, amount,
			FunctionUtils::resolveTimeZone(zone));
}

template<typename C>
inline int64_t SQLTimeExprs::Functions::TimestampAdd::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue, int64_t amount,
		const util::TimeZone &zone) {
	const util::DateTime::FieldType field =
			SQLValues::ValueUtils::toTimestampField(fieldTypeValue);

	util::DateTime::ZonedOption option;
	cxt.applyDateTimeOption(option);
	option.zone_ = FunctionUtils::resolveTimeZone(cxt, zone);

	try {
		util::DateTime ts(tsValue);
		ts.addField(amount, field, option);
		return ts.getUnixTime();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Timestamp range overflow"));
	}
}


template<typename C, typename R>
inline int64_t SQLTimeExprs::Functions::TimestampDiff::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue1, int64_t tsValue2,
		R &zone) {
	return (*this)(
			cxt, fieldTypeValue, tsValue1, tsValue2,
			FunctionUtils::resolveTimeZone(zone));
}

template<typename C>
inline int64_t SQLTimeExprs::Functions::TimestampDiff::operator()(
		C &cxt, int64_t fieldTypeValue, int64_t tsValue1, int64_t tsValue2,
		const util::TimeZone &zone) {
	const util::DateTime::FieldType field =
			SQLValues::ValueUtils::toTimestampField(fieldTypeValue);

	util::DateTime::ZonedOption option;
	cxt.applyDateTimeOption(option);
	option.zone_ = FunctionUtils::resolveTimeZone(cxt, zone);

	int64_t result;
	try {
		util::DateTime ts1(tsValue1);
		util::DateTime ts2(tsValue2);
		result = ts1.getDifference(ts2, field, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Timestamp range overflow"));
	}

	return result;
}

util::DateTime SQLTimeExprs::TimeFunctionUtils::trunc(
		const util::DateTime &value, util::DateTime::FieldType field,
		const util::DateTime::ZonedOption &option) {

	util::DateTime::FieldData fieldData;
	try {
		value.getFields(fieldData, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	do {
		if (field == util::DateTime::FIELD_MILLISECOND) {
			break;
		}
		fieldData.milliSecond_ = 0;

		if (field == util::DateTime::FIELD_SECOND) {
			break;
		}
		fieldData.second_ = 0;

		if (field == util::DateTime::FIELD_MINUTE) {
			break;
		}
		fieldData.minute_ = 0;

		if (field == util::DateTime::FIELD_HOUR) {
			break;
		}
		fieldData.hour_ = 0;

		if (field == util::DateTime::FIELD_DAY_OF_MONTH ||
				field == util::DateTime::FIELD_DAY_OF_WEEK ||
				field == util::DateTime::FIELD_DAY_OF_YEAR) {
			break;
		}
		fieldData.monthDay_ = 1;

		if (field == util::DateTime::FIELD_MONTH) {
			break;
		}
		fieldData.month_ = 1;
	}
	while (false);

	util::DateTime ret;
	try {
		ret.setFields(fieldData, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return ret;
}
