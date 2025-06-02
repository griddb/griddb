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
	@brief Definition of Time functions for TQL
*/
#ifndef FUNCTIONS_TIMESTAMP_H_
#define FUNCTIONS_TIMESTAMP_H_

#include "expression.h"

#include <algorithm>
#include <string>

#include "util/time.h"
#include "qp_def.h"

/*!
 * @brief NOW()
 *
 * @return create current timestamp
 */
class FunctorNow : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (!args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Argument count is invalid");
		}
		return Expr::newTimestampValue(txn.getStatementStartTime(), txn);
	}
	virtual ~FunctorNow() {}
};

/*!
 * @brief TIMESTAMP(str)
 *
 * @return create timestamp from string
 */
template<util::DateTime::FieldType Precision, bool Strict>
class FunctorTimestamp : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string value");
		}

		const util::DateTime::FieldType precision = Precision;
		const util::DateTime::FieldType *precisionForOption =
				(Strict ? NULL : &precision);
		const util::DateTime::ZonedOption &option =
				Expr::newZonedTimeOption(precisionForOption, util::TimeZone());

		util::PreciseDateTime d;
		util::DateTime::Parser parser = d.getParser(option);
		parser.setDefaultPrecision(Precision);

		const char8_t *str = args[0]->getValueAsString(txn);
		const bool throwOnError = false;
		if (!d.getParser(option)(str, strlen(str), throwOnError)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Cannot parse argument as Timestamp (value=" << str << ")");
		}

		const ColumnType retType = (
				Precision == util::DateTime::FIELD_MILLISECOND ?
						COLUMN_TYPE_TIMESTAMP :
				Precision == util::DateTime::FIELD_MICROSECOND ?
						COLUMN_TYPE_MICRO_TIMESTAMP :
				Precision == util::DateTime::FIELD_NANOSECOND ?
						COLUMN_TYPE_NANO_TIMESTAMP :
						COLUMN_TYPE_NULL);
		UTIL_STATIC_ASSERT(retType != COLUMN_TYPE_NULL);

		return Expr::newTimestampValue(d, txn, retType);
	}
	virtual ~FunctorTimestamp() {}
};

/*!
 * @brief TO_TIMESTAMP(double)
 *
 * @return create timestamp from double
 */
class FunctorToTimestamp : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isNumeric()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a numeric value");
		}
		Timestamp m = util::DateTime::max(false).getUnixTime();
		Timestamp n =
			static_cast<Timestamp>(args[0]->getValueAsDouble() * 1000.0);
		if (n < 0 || n > m) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"Argument 1 is out of timestamp range");
		}
		return Expr::newTimestampValue(n, txn);
	}
	virtual ~FunctorToTimestamp() {}
};

/*!
 * @brief FROM_TIMESTAMP(timestamp)
 *
 * @return create string from timestamp
 */
class Functor_from_timestamp : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[0]->isTimestamp()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a timestamp value");
		}
		const char *s = args[0]->getValueAsString(txn);
		return Expr::newStringValue(s, txn);
	}
	virtual ~Functor_from_timestamp() {}
};

/*!
 * @brief TO_TIMESTAMP_MS(long)
 *
 * @return create timestamp from long
 */
class FunctorToTimestampMS : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isNumericInteger()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not an integer value");
		}
		int64_t m = util::DateTime::max(false).getUnixTime();
		int64_t n = args[0]->getValueAsInt64();
		if (n < 0 || n > m) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"Argument 1 is out of timestamp range");
		}
		return Expr::newTimestampValue(n, txn);
	}
	virtual ~FunctorToTimestampMS() {}
};

/*!
 * @brief TO_EPOCH(timestamp)
 *
 * @return create epoch long value from timestamp
 */
class FunctorToEpoch : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isTimestamp()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a timestamp value");
		}
		return Expr::newNumericValue(args[0]->getValueAsDouble() / 1000.0, txn);
	}

	virtual ~FunctorToEpoch() {}
};

/*!
 * @brief TO_EPOCH_MS(timestamp)
 *
 * @return create epoch long value from timestamp
 */
class FunctorToEpochMS : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isTimestampFamily()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a timestamp value");
		}
		int64_t value = static_cast<int64_t>(args[0]->getTimeStamp());
		return Expr::newNumericValue(value, txn);
	}

	virtual ~FunctorToEpochMS() {}
};

/*!
 * @brief TIMESTAMPADD(DAY|HOUR|MINUTE|SECOND|MILLISECOND, timestamp, duration)
 *
 * @return new timestamp
 */
class FunctorTimestampadd : public TqlFunc {
public:
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.size() != 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not DURATION(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|MILLISECOND)");
		}
		else if (args[1]->isNullValue() || args[2]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[1]->isTimestampFamily()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 is not a timestamp value");
		}
		else if (!args[2]->isNumericInteger()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 3 is not a integer value");
		}
		else {
			/* DO NOTHING */
		}

		util::PreciseDateTime t1 =
				ValueProcessor::toDateTime(args[1]->getNanoTimestamp());
		const int64_t t2 = args[2]->getValueAsInt64();
		util::String s(args[0]->getValueAsString(txn),
			QP_ALLOCATOR);  
		std::transform(
			s.begin(), s.end(), s.begin(), (int (*)(int))std::toupper);

		util::DateTime::FieldType fieldType;
		if (s.compare("YEAR") == 0) {
			fieldType = util::DateTime::FIELD_YEAR;
		}
		else if (s.compare("MONTH") == 0) {
			fieldType = util::DateTime::FIELD_MONTH;
		}
		else if (s.compare("DAY") == 0) {
			fieldType = util::DateTime::FIELD_DAY_OF_MONTH;
		}
		else if (s.compare("HOUR") == 0) {
			fieldType = util::DateTime::FIELD_HOUR;
		}
		else if (s.compare("MINUTE") == 0) {
			fieldType = util::DateTime::FIELD_MINUTE;
		}
		else if (s.compare("SECOND") == 0) {
			fieldType = util::DateTime::FIELD_SECOND;
		}
		else if (s.compare("MILLISECOND") == 0) {
			fieldType = util::DateTime::FIELD_MILLISECOND;
		}
		else if (s.compare("MICROSECOND") == 0) {
			fieldType = util::DateTime::FIELD_MICROSECOND;
		}
		else if (s.compare("NANOSECOND") == 0) {
			fieldType = util::DateTime::FIELD_NANOSECOND;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"This time field is invalid or not supported.");
		}

		const util::DateTime::ZonedOption zonedOption =
				Expr::newZonedTimeOption(&fieldType, txn.getTimeZone());

		try {
			t1.addField(t2, fieldType, zonedOption);
		}
		catch (util::UtilityException &e) {
			GS_RETHROW_USER_ERROR_CODED(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE, e, "");
		}

		const ColumnType valueType = args[1]->resolveValueType();
		return Expr::newTimestampValue(t1, txn, valueType);
	}

	Expr *operator()(ExprList &args, ContainerRowWrapper *column_values,
		FunctionMap *function_map, EvalMode mode, TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ExprList &argsAfterEval) {
		if (args[0]->isColumn()) {
			argsAfterEval.insert(argsAfterEval.end(),
				Expr::newStringValue(args[0]->getValueAsString(txn), txn));
			for (size_t i = 1; i < args.size(); i++) {
				argsAfterEval.insert(argsAfterEval.end(),
					args[i]->eval(
						txn, objectManager, strategy, column_values, function_map, mode));
			}
		}
		else {
			for (size_t i = 0; i < args.size(); i++) {
				argsAfterEval.insert(argsAfterEval.end(),
					args[i]->eval(
						txn, objectManager, strategy, column_values, function_map, mode));
			}
		}

		Expr *ret = (*this)(argsAfterEval, txn, objectManager, strategy);

		for (ExprList::const_iterator it = argsAfterEval.begin();
			 it != argsAfterEval.end(); it++) {
			QP_DELETE(*it);
		}

		return ret;
	}

	virtual ~FunctorTimestampadd() {}
};

/*!
 * @brief TIMESTAMPDIFF(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|MILLISECOND,
 * timestamp1, timestamp2)
 *
 * @return integer
 */
class FunctorTimestampdiff : public TqlFunc {
public:
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &strategy) {
		UNUSED_VARIABLE(strategy);

		if (args.size() != 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string value");
		}
		else if (args[0]->isNullValue() || args[1]->isNullValue() || args[2]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[1]->isTimestampFamily()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 is not a timestamp value");
		}
		else if (!args[2]->isTimestampFamily()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 3 is not a timestamp value");
		}
		else {
			/* DO NOTHING */
		}

		util::PreciseDateTime dt1 =
				ValueProcessor::toDateTime(args[1]->getNanoTimestamp());
		util::PreciseDateTime dt2 =
				ValueProcessor::toDateTime(args[2]->getNanoTimestamp());
		util::String s(args[0]->getValueAsString(txn), QP_ALLOCATOR);
		std::transform(
			s.begin(), s.end(), s.begin(), (int (*)(int))std::toupper);

		util::DateTime::FieldType fieldType;
		if (s.compare("YEAR") == 0) {
			fieldType = util::DateTime::FIELD_YEAR;
		}
		else if (s.compare("MONTH") == 0) {
			fieldType = util::DateTime::FIELD_MONTH;
		}
		else if (s.compare("DAY") == 0) {
			fieldType = util::DateTime::FIELD_DAY_OF_MONTH;
		}
		else if (s.compare("HOUR") == 0) {
			fieldType = util::DateTime::FIELD_HOUR;
		}
		else if (s.compare("MINUTE") == 0) {
			fieldType = util::DateTime::FIELD_MINUTE;
		}
		else if (s.compare("SECOND") == 0) {
			fieldType = util::DateTime::FIELD_SECOND;
		}
		else if (s.compare("MILLISECOND") == 0) {
			fieldType = util::DateTime::FIELD_MILLISECOND;
		}
		else if (s.compare("MICROSECOND") == 0) {
			fieldType = util::DateTime::FIELD_MICROSECOND;
		}
		else if (s.compare("NANOSECOND") == 0) {
			fieldType = util::DateTime::FIELD_NANOSECOND;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"This time field is invalid or not supported.");
		}

		const util::DateTime::ZonedOption &zonedOption =
				Expr::newZonedTimeOption(&fieldType, txn.getTimeZone());

		int64_t result;
		try {
			result = dt1.getDifference(dt2, fieldType, zonedOption);
		}
		catch (util::UtilityException &e) {
			GS_RETHROW_USER_ERROR_CODED(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE, e, "");
		}

		return Expr::newNumericValue(result, txn);
	}

	Expr *operator()(ExprList &args, ContainerRowWrapper *column_values,
		FunctionMap *function_map, EvalMode mode, TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ExprList &argsAfterEval) {
		if (args[0]->isColumn()) {
			argsAfterEval.insert(argsAfterEval.end(),
				Expr::newStringValue(args[0]->getValueAsString(txn), txn));
			for (size_t i = 1; i < args.size(); i++) {
				argsAfterEval.insert(argsAfterEval.end(),
					args[i]->eval(
						txn, objectManager, strategy, column_values, function_map, mode));
			}
		}
		else {
			for (size_t i = 0; i < args.size(); i++) {
				argsAfterEval.insert(argsAfterEval.end(),
					args[i]->eval(
						txn, objectManager, strategy, column_values, function_map, mode));
			}
		}

		Expr *ret = (*this)(argsAfterEval, txn, objectManager, strategy);

		for (ExprList::const_iterator it = argsAfterEval.begin();
			 it != argsAfterEval.end(); it++) {
			QP_DELETE(*it);
		}

		return ret;
	}

	virtual ~FunctorTimestampdiff() {}
};

#endif /*FUNCTIONS_TIMESTAMP_H_ */
