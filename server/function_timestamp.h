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
		util::DateTime d;
		if (util::DateTime::parse(args[0]->getValueAsString(txn), d,
				TRIM_MILLISECONDS) == false) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot parse argument as Timestamp,"
					<< args[0]->getValueAsString(txn));
		}
		return Expr::newTimestampValue(d, txn);
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
		if (!args[0]->isTimestamp()) {
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
		else if (!args[1]->isTimestamp()) {
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

		util::DateTime t1 =
			static_cast<util::DateTime>(args[1]->getTimeStamp());
		time_t t2 = args[2]->getValueAsInt();
		util::String s(args[0]->getValueAsString(txn),
			QP_ALLOCATOR);  
		std::transform(
			s.begin(), s.end(), s.begin(), (int (*)(int))std::toupper);

		util::DateTime::ZonedOption zonedOption = util::DateTime::ZonedOption::create(false, txn.getTimeZone());

		if (s.compare("YEAR") == 0) {
			t1.addField(static_cast<int32_t>(t2), util::DateTime::FIELD_YEAR, zonedOption);
		}
		else if (s.compare("MONTH") == 0) {
			t1.addField(static_cast<int32_t>(t2), util::DateTime::FIELD_MONTH, zonedOption);
		}
		else if (s.compare("DAY") == 0) {
			t1.addField(
				static_cast<int32_t>(t2), util::DateTime::FIELD_DAY_OF_MONTH, zonedOption);
		}
		else if (s.compare("HOUR") == 0) {
			t1.addField(static_cast<int32_t>(t2), util::DateTime::FIELD_HOUR, zonedOption);
		}
		else if (s.compare("MINUTE") == 0) {
			t1.addField(static_cast<int32_t>(t2), util::DateTime::FIELD_MINUTE, zonedOption);
		}
		else if (s.compare("SECOND") == 0) {
			t1.addField(static_cast<int32_t>(t2), util::DateTime::FIELD_SECOND, zonedOption);
		}
		else if (s.compare("MILLISECOND") == 0) {
			t1.addField(
				static_cast<int32_t>(t2), util::DateTime::FIELD_MILLISECOND, zonedOption);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"This time kind is invalid or not supported.");
		}

		return Expr::newTimestampValue(t1, txn);
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
		else if (!args[1]->isTimestamp()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 is not a timestamp value");
		}
		else if (!args[2]->isTimestamp()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 3 is not a timestamp value");
		}
		else {
			/* DO NOTHING */
		}

		Timestamp t1 = args[1]->getTimeStamp();
		Timestamp t2 = args[2]->getTimeStamp();
		util::String s(args[0]->getValueAsString(txn), QP_ALLOCATOR);
		std::transform(
			s.begin(), s.end(), s.begin(), (int (*)(int))std::toupper);
		int64_t result;
		util::DateTime dt1(t1), dt2(t2);
		util::DateTime::FieldType field_type;

		if (s.compare("YEAR") == 0) {
			field_type = util::DateTime::FIELD_YEAR;
		}
		else if (s.compare("MONTH") == 0) {
			field_type = util::DateTime::FIELD_MONTH;
		}
		else if (s.compare("DAY") == 0) {
			field_type = util::DateTime::FIELD_DAY_OF_MONTH;
		}
		else if (s.compare("HOUR") == 0) {
			field_type = util::DateTime::FIELD_HOUR;
		}
		else if (s.compare("MINUTE") == 0) {
			field_type = util::DateTime::FIELD_MINUTE;
		}
		else if (s.compare("SECOND") == 0) {
			field_type = util::DateTime::FIELD_SECOND;
		}
		else if (s.compare("MILLISECOND") == 0) {
			field_type = util::DateTime::FIELD_MILLISECOND;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"This time kind is invalid or not supported.");
		}
		util::DateTime::ZonedOption zonedOption = util::DateTime::ZonedOption::create(false, txn.getTimeZone());
		result = dt1.getDifference(dt2, field_type, zonedOption);

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
