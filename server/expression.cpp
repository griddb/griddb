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
	@brief Implementation of Expr
*/
#include "util/type.h"
#include "util/allocator.h"
#include "util/container.h"
#include "util/time.h"
#include <algorithm>
#include <cctype>
#include <iostream>
#include <map>
#include <sstream>
#include <stack>
#include <stdexcept>
#include <string>

#include "expression.h"
#include "qp_def.h"
#include "gis_generator.h"

#include "function_map.h"
#include "lexer.h"
#include "query.h"

#include "meta_store.h"

/*!
 * @brief Return the type of the expression
 * @return The expression is a simple value
 */
bool Expr::isNumeric() const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	return value_->isNumerical();
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a simple value of the given type
 */
bool Expr::isNumericOfType(ColumnType t) const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	return value_->getType() == t;
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a simple value of the given type
 */
bool Expr::isNumericInteger() const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	return value_->isInteger();
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a simple value of the given type
 */
bool Expr::isNumericFloat() const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	return value_->isFloat();
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a simple value
 */
bool Expr::isBoolean() const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	ColumnType t = value_->getType();
	return (t == COLUMN_TYPE_BOOL);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a string value
 */
bool Expr::isString() const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}

	ColumnType t = value_->getType();
	return (t == COLUMN_TYPE_STRING);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a geometry value
 */
bool Expr::isGeometry() const {
	if (type_ != VALUE) return false;
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	ColumnType t = value_->getType();
	return (t == COLUMN_TYPE_GEOMETRY);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is an array
 */
bool Expr::isArrayValue() const {
	return (type_ == VALUE && value_ != NULL && value_->isArray());
}

/*!
 * @brief Return the type of the expression
 * @return The expression is an array
 */
bool Expr::isExprArray() const {
	return (type_ == EXPRARRAY);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a value
 */
bool Expr::isValue() const {
	return (type_ == VALUE);
}


/*!
 * @brief Return the type of the expression
 * @return The expression is null
 */
bool Expr::isNullValue() const {
	return (type_ == NULLVALUE);
}

/*!
 * @brief Return the type of the expression
 * @return The expression can have null
 */
bool Expr::isNullable() const {
	if (isColumn() && columnInfo_->isNotNull()) {
		return false;
	} else {
		return true;
	}
}

/*!
 * @brief Return the type of the expression
 * @return The expression is an expr label
 */
bool Expr::isExprLabel() const {
	return (type_ == EXPRLABEL);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is an aggregation function
 */
bool Expr::isAggregation() const {
	return (type_ == AGGREGATION);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a selection function
 */
bool Expr::isSelection() const {
	return (type_ == SELECTION);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a column id
 */
bool Expr::isColumn() const {
	return (type_ == COLUMN);
}

/*!
 * @brief Return the type of the expression
 * @return The expression is a timestamp
 */
bool Expr::isTimestamp() const {
	if (type_ != VALUE) return false;
	return (resolveValueType() == COLUMN_TYPE_TIMESTAMP);
}

bool Expr::isMicroTimestamp() const {
	if (type_ != VALUE) return false;
	return (resolveValueType() == COLUMN_TYPE_MICRO_TIMESTAMP);
}

bool Expr::isNanoTimestamp() const {
	if (type_ != VALUE) return false;
	return (resolveValueType() == COLUMN_TYPE_NANO_TIMESTAMP);
}

bool Expr::isTimestampFamily() const {
	if (type_ != VALUE) return false;
	return ValueProcessor::isTimestampFamily(resolveValueType());
}

/*!
 * @brief Cast an numeric value to another type.
 * @return casted value.
 */
template <typename T>
T Expr::castNumericValue() {
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	if (type_ != VALUE) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: expression is not a value");
	}
	switch (value_->getType()) {
	case COLUMN_TYPE_BYTE:
		return static_cast<T>(
			*reinterpret_cast<const int8_t *>(value_->data()));
	case COLUMN_TYPE_SHORT:
		return static_cast<T>(
			*reinterpret_cast<const int16_t *>(value_->data()));
	case COLUMN_TYPE_INT:
		return static_cast<T>(
			*reinterpret_cast<const int32_t *>(value_->data()));
	case COLUMN_TYPE_LONG:
		return static_cast<T>(
			*reinterpret_cast<const int64_t *>(value_->data()));
	case COLUMN_TYPE_FLOAT:
		return static_cast<T>(*reinterpret_cast<const float *>(value_->data()));
	case COLUMN_TYPE_DOUBLE:
		return static_cast<T>(
			*reinterpret_cast<const double *>(value_->data()));
	default:
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_CONSTRAINT_CANNOT_CAST, "Cannot cast value to numeric");
	}
}

/*!
 * @brief castNumericValue's specialized version for boolean
 * @return Value in expression that casted to bool
 */
bool Expr::castNumericValue() {
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	if (isNullValue()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: expression is null");
	}
	if (type_ != VALUE) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: expression is not a value");
	}

	switch (value_->getType()) {
	case COLUMN_TYPE_BOOL:
		return *reinterpret_cast<const bool *>(value_->data());
	default:
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_CONSTRAINT_CANNOT_CAST, "Cannot cast value to boolean");
	}
}

/*!
 * @brief Return the value of the expression
 * @return The expression as Integer
 */
int Expr::getValueAsInt() {
	return castNumericValue<int>();
}

/*!
 * @brief Return the value of the expression
 * @return The expression as Integer
 */
int64_t Expr::getValueAsInt64() {
	return castNumericValue<int64_t>();
}

/*!
 * @brief Return a expression type
 * @return The type of expression
 */
Expr::Type Expr::getType() {
	return type_;
}

ColumnType Expr::resolveValueType() const {
	assert(type_ == VALUE);

	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}

	return value_->getType();
}

/*!
 * @brief Return the value of the expression
 * @return The expression value as a Double
 */
double Expr::getValueAsDouble() {
	return castNumericValue<double>();
}

/*!
 * @brief Return the value of the expression
 * @return The expression value as a Boolean
 */
bool Expr::getValueAsBool() {
	return castNumericValue();
}

/*!
 * @brief Stringify numeric value
 * @return Stringified value
 */
template <typename T, typename R>
const char *Expr::stringifyValue(TransactionContext &txn) {
	util::NormalOStringStream os;
	os << R(*reinterpret_cast<const T *>(value_->data()));
	return os2char(txn, os);
}

#ifdef _WIN32
/*!
 * @brief Stringify numeric value
 * @return Stringified value
 */
template <>
const char *Expr::stringifyValue<double, double>(TransactionContext &txn) {
	double x = double(*reinterpret_cast<const double *>(value_->data()));
	util::NormalOStringStream os;
	if (_isnan(x)) {
		os << "nan";
	}
	else if (x == std::numeric_limits<double>::infinity()) {
		os << "inf";
	}
	else if (x == -std::numeric_limits<double>::infinity()) {
		os << "-inf";
	}
	else {
		os << x;
	}
	return os2char(txn, os);
}
#endif

template<typename T>
const char8_t* Expr::stringifyTimestamp(TransactionContext &txn) {
	util::DateTime::ZonedOption option = util::DateTime::ZonedOption::create(
			TRIM_MILLISECONDS, txn.getTimeZone());
	option.asLocalTimeZone_ = USE_LOCAL_TIMEZONE;

	const size_t maxSize = util::DateTime::MAX_FORMAT_SIZE;
	char8_t *str =static_cast<char8_t*>(
			txn.getDefaultAllocator().allocate(maxSize + 1));

	const util::DateTime::Formatter formatter =
			ValueProcessor::getTimestampFormatter(
					*reinterpret_cast<const T*>(value_->data()), option);
	const size_t size = formatter(str, maxSize);
	str[size] = '\0';

	return str;
}

/*!
 * @brief Return the value of the expression
 * @return The expression value as a string
 */
const char *Expr::getValueAsString(TransactionContext &txn) {
	if (isNullValue()) {
		return "NULL";
	} else 
	if (type_ == VALUE) {
		if (value_ == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: value == NULL");
		}

		ColumnType t = value_->getType();

		switch (t) {
		case COLUMN_TYPE_STRING: {
			const char *str = reinterpret_cast<const char *>(value_->data());
			size_t size = value_->size();
			if (str == NULL || size == 0) return "";
			char *allocatedStr = reinterpret_cast<char *>(
				txn.getDefaultAllocator().allocate(size + 1));
			memcpy(allocatedStr, str, size);
			allocatedStr[size] = '\0';
			return allocatedStr;
		}
		case COLUMN_TYPE_BOOL:
			if (*reinterpret_cast<const bool *>(value_->data())) {
				return "TRUE";
			}
			else {
				return "FALSE";
			}
		case COLUMN_TYPE_BYTE:
			return stringifyValue<int8_t, int16_t>(txn);
		case COLUMN_TYPE_SHORT:
			return stringifyValue<int16_t, int16_t>(txn);
		case COLUMN_TYPE_INT:
			return stringifyValue<int32_t, int32_t>(txn);
		case COLUMN_TYPE_LONG:
			return stringifyValue<int64_t, int64_t>(txn);
		case COLUMN_TYPE_FLOAT:
			return stringifyValue<float, float>(txn);
		case COLUMN_TYPE_DOUBLE:
			return stringifyValue<double, double>(txn);
		case COLUMN_TYPE_TIMESTAMP:
			return stringifyTimestamp<Timestamp>(txn);
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			return stringifyTimestamp<MicroTimestamp>(txn);
		case COLUMN_TYPE_NANO_TIMESTAMP:
			return stringifyTimestamp<NanoTimestamp>(txn);
		case COLUMN_TYPE_GEOMETRY: {
			Geometry *geom = geomCache_;
			return geom->getString(txn);
		}
		default:
			break;  
		}
	}
	else if (type_ == EXPRARRAY) {
		util::NormalOStringStream os;
		ExprList::const_iterator it;
		if (arglist_ == NULL || arglist_->empty()) {
			os << "EXPRARRAY(EMPTY)";
			return os2char(txn, os);
		}
		it = arglist_->begin();
		os << "EXPRARRAY(";
		do {
			if ((*it)->isString()) {
				os << '\'' << (*it)->getValueAsString(txn) << '\'';
			}
			else {
				os << (*it)->getValueAsString(txn);
			}
			++it;
			if (it != arglist_->end()) {
				os << ", ";
			}
			else {
				break;
			}
		} while (true);
		os << ')';
		return os2char(txn, os);
	}
	else {
		if (label_ != NULL) {
			return label_;
		}
		else {
			return "NULL";
		}
	}
	GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
		"Internal logic error: cannot create string");
}

/*!
 * @brief  Return the value of the expression
 * @return The expression as a Geometry
 */
Geometry *Expr::getGeometry() {
	if (geomCache_ != NULL) {
		return geomCache_;
	}
	else if (isNullValue()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: expression is null");
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: getGeometry() is called in invalid "
			"context.");
	}
}

/*!
 * @brief Return the value of the expression
 * @return The expression as Expression list
 */
const ExprList &Expr::getArgList() {
	if (type_ == EXPR && arglist_ != NULL) {
		return *arglist_;
	}
	GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
		"Internal logic error: getArgList() is called in invalid context.");
}

/*!
 * @brief Return the expression of an array element
 * @param txn The transaction context
 * @param txn Object manager
 * @param idx element index
 * @return The expression of the element
 */
Expr *Expr::getArrayElement(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, size_t idx) {
	if (type_ == VALUE) {
		assert(value_ != NULL);
		if (idx >= value_->getArrayLength(txn, objectManager, strategy)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_ARRAY_OUT_OF_RANGE,
				"Specified index is out of range.");
		}

		const uint8_t *data;
		uint32_t size;
		value_->getArrayElement(
			txn, objectManager, strategy, static_cast<uint32_t>(idx), data, size);
		switch (value_->getType()) {
		case COLUMN_TYPE_STRING_ARRAY:
			return Expr::newStringValue(
				reinterpret_cast<const char *>(data), size, txn);
		case COLUMN_TYPE_BOOL_ARRAY:
			return Expr::newBooleanValue(
				*reinterpret_cast<const bool *>(data), txn);
		case COLUMN_TYPE_BYTE_ARRAY:
			return Expr::newNumericValue(
				*reinterpret_cast<const int8_t *>(data), txn);
		case COLUMN_TYPE_SHORT_ARRAY:
			return Expr::newNumericValue(
				*reinterpret_cast<const int16_t *>(data), txn);
		case COLUMN_TYPE_INT_ARRAY:
			return Expr::newNumericValue(
				*reinterpret_cast<const int32_t *>(data), txn);
		case COLUMN_TYPE_LONG_ARRAY:
			return Expr::newNumericValue(
				*reinterpret_cast<const int64_t *>(data), txn);
		case COLUMN_TYPE_FLOAT_ARRAY:
			return Expr::newNumericValue(
				*reinterpret_cast<const float *>(data), txn);
		case COLUMN_TYPE_DOUBLE_ARRAY:
			return Expr::newNumericValue(
				*reinterpret_cast<const double *>(data), txn);
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			return Expr::newTimestampValue(
				*reinterpret_cast<const Timestamp*>(data),txn);
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: getArrayElement() is called with "
				"invalid array type.");
			break;
		}
	}

	GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
		"Internal logic error: getArrayElement() is called in invalid "
		"context.");
}

/*!
 * @brief Return the length of array
 * @param txn The transaction context
 * @param txn Object manager
 * @return The length of array
 */
size_t Expr::getArrayLength(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) const {
	if (type_ == VALUE) {
		assert(value_ != NULL);
		return value_->getArrayLength(txn, objectManager, strategy);
	}
	else if (type_ == EXPRARRAY && arglist_ != NULL) {
		return arglist_->size();
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: getArrayLength() is called in invalid "
			"context.");
	}
}

/*!
 * @brief Return the timestamp in a expression structure
 * @return The timestamp value
 */
Timestamp Expr::getTimeStamp() {
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	if (isNullValue()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: expression is null");
	}

	try {
		return ValueProcessor::getTimestamp(value_->getType(), value_->data());
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE, e,
				"Invalid datatypes: argument is not a timestamp value");
	}
}

NanoTimestamp Expr::getNanoTimestamp() {
	if (value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: value == NULL");
	}
	if (isNullValue()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: expression is null");
	}

	try {
		return ValueProcessor::getNanoTimestamp(
				value_->getType(), value_->data());
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE, e,
				"Invalid datatypes: argument is not a timestamp value");
	}
}

/*!
 * @brief Make a deep copy of an expression
 * @param txn The transaction context
 * @param txn Object manager
 * @return Duplicated expression
 */
Expr *Expr::dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	Expr *e = NULL;
	if (type_ == NULLVALUE) {
		e = Expr::newNullValue(txn);
	} else
	if (type_ == VALUE) {
		assert(value_ != NULL);
		if (!value_->isArray()) {
			switch (value_->getType()) {
			case COLUMN_TYPE_STRING:
				e = Expr::newStringValue(
					reinterpret_cast<const char *>(value_->data()),
					value_->size(), txn);
				break;
			case COLUMN_TYPE_BOOL:
				e = Expr::newBooleanValue(
					*reinterpret_cast<const bool *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_BYTE:
				e = Expr::newNumericValue(
					*reinterpret_cast<const int8_t *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_SHORT:
				e = Expr::newNumericValue(
					*reinterpret_cast<const int16_t *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_INT:
				e = Expr::newNumericValue(
					*reinterpret_cast<const int32_t *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_LONG:
				e = Expr::newNumericValue(
					*reinterpret_cast<const int64_t *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_FLOAT:
				e = Expr::newNumericValue(
					*reinterpret_cast<const float *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_DOUBLE:
				e = Expr::newNumericValue(
					*reinterpret_cast<const double *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_TIMESTAMP:
				e = Expr::newTimestampValue(
					*reinterpret_cast<const Timestamp *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_MICRO_TIMESTAMP:
				e = Expr::newTimestampValue(
					*reinterpret_cast<const MicroTimestamp *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_NANO_TIMESTAMP:
				e = Expr::newTimestampValue(
					*reinterpret_cast<const NanoTimestamp *>(value_->data()), txn);
				break;
			case COLUMN_TYPE_GEOMETRY: {
				Geometry *geom = getGeometry();
				e = Expr::newGeometryValue(geom, txn);
			} break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: cannot determine column type.");
			}
		}
		else {  
			Value *av;
			av = QP_NEW Value();
			av->copy(txn, objectManager, strategy, *value_);

			e = Expr::newArrayValue(av, txn);
		}
	}
	else {
		e = QP_NEW Expr(txn);
		e->type_ = type_;
		e->op_ = op_;
		if (label_) {
			size_t len = strlen(label_) + 1;
			e->label_ =
				static_cast<char *>(txn.getDefaultAllocator().allocate(len));
			memcpy(e->label_, label_, len);
		}
		assert(buf_ == NULL);
		assert(value_ == NULL);
		e->buf_ = NULL;
		e->value_ = NULL;
		e->columnInfo_ = columnInfo_;
		e->columnId_ = columnId_;
		e->columnType_ = columnType_;
		e->functor_ = functor_;
		e->geomCache_ = geomCache_;
		if (arglist_ != NULL) {
			e->arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
			for (ExprList::const_iterator it = arglist_->begin();
				 it != arglist_->end(); it++) {
				e->arglist_->insert(
					e->arglist_->end(), (*it)->dup(txn, objectManager, strategy));
			}
		}
	}
	return e;
}

/*!
 * @brief Common constructor
 */
void Expr::Init() {
	type_ = VALUE;
	label_ = NULL;
	arglist_ = NULL;
	value_ = NULL;
	buf_ = NULL;
	columnInfo_ = NULL;
	columnId_ = 0;
	functor_ = NULL;
	columnType_ = COLUMN_TYPE_WITH_BEGIN;
	geomCache_ = NULL;
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 */
Expr::Expr(bool v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 */
Expr::Expr(int8_t v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 */
Expr::Expr(int16_t v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 */
Expr::Expr(int32_t v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 * @param isTimestamp The int64 value is a timestamp
 */
Expr::Expr(int64_t v, TransactionContext &txn, bool isTimestamp) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
	if (isTimestamp) {
		value_->setTimestamp(v);
	}
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 */
Expr::Expr(float v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

/*!
 * @brief ctor
 *
 * @param v Value
 * @param txn The transaction context
 */
Expr::Expr(double v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

Expr::Expr(const MicroTimestamp &v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

Expr::Expr(const NanoTimestamp &v, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	value_ = QP_NEW Value(v);
}

/*!
 * @brief ctor
 *
 * @param s String value
 * @param txn The transaction context
 * @param isLabel The string is a expression label
 * @param needUnescape add escape character
*/
Expr::Expr(
	const char *s, TransactionContext &txn, bool isLabel, bool needUnescape) {
	Init();
	if (isLabel) {
		size_t len = strlen(s) + 1;
		type_ = EXPRLABEL;
		label_ = static_cast<char *>(txn.getDefaultAllocator().allocate(len));
		memcpy(label_, s, len);
	}
	else {
		util::String str(s, QP_ALLOCATOR);  
		if (needUnescape) {
			unescape(str, '\'');
		}
		type_ = VALUE;
		buf_ = QP_NEW util::XArray<uint8_t>(txn.getDefaultAllocator());
		if (str.empty()) {
			value_ =
				QP_NEW Value(txn.getDefaultAllocator(), const_cast<char *>(""));
		}
		else {
			value_ = QP_NEW Value(
				txn.getDefaultAllocator(), const_cast<char *>(str.c_str()));
		}
	}
}

/*!
 * @brief ctor
 *
 * @param s String value
 * @param len String length
 * @param txn The transaction context
 * @param isLabel The string is a expression label
 * @param needUnescape add escape character
 */
Expr::Expr(const char *s, size_t len, TransactionContext &txn, bool isLabel,
	bool needUnescape) {
	Init();
	if (isLabel) {
		type_ = EXPRLABEL;
		label_ =
			static_cast<char *>(txn.getDefaultAllocator().allocate(len + 1));
		memcpy(label_, s, len);
		label_[len] = '\0';
	}
	else {
		util::String str(s, len, QP_ALLOCATOR);  
		if (needUnescape) {
			unescape(str, '\'');
		}
		type_ = VALUE;
		buf_ = QP_NEW util::XArray<uint8_t>(txn.getDefaultAllocator());
		if (str.empty()) {
			value_ =
				QP_NEW Value(txn.getDefaultAllocator(), const_cast<char *>(""));
		}
		else {
			value_ = QP_NEW Value(
				txn.getDefaultAllocator(), const_cast<char *>(str.c_str()));
		}
	}
}

/*!
 * @brief ctor of a column expression
 *
 * @param name Column name
 * @param txn The transaction context
 * @param columnId Column ID
 * @param cInfo Column info
 */
Expr::Expr(const char *name, TransactionContext &txn, uint32_t columnId,
	ColumnInfo *cInfo) {
	Init();
	type_ = COLUMN;
	size_t len = strlen(name) + 1;
	label_ = static_cast<char *>(txn.getDefaultAllocator().allocate(len));
	memcpy(label_, name, len);
	columnInfo_ = cInfo;
	columnId_ = columnId;
	if (cInfo != NULL) {
		columnType_ = cInfo->getColumnType();
	}
}

/*!
 * @brief Ctor of binary operation
 *
 * @param op Operation
 * @param arg1 argument1
 * @param arg2 argument2
 * @param txn The transaction context
 */
Expr::Expr(Operation op, Expr *arg1, Expr *arg2, TransactionContext &txn) {
	Init();
	type_ = EXPR;
	op_ = op;
	arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
	arglist_->push_back(arg1);
	if (arg2 != NULL) {
		arglist_->push_back(arg2);
	}
}

/*!
 * @brief Ctor of operation
 *
 * @param op Operation
 * @param arg Argument list
 * @param txn The transaction context
 */
Expr::Expr(Operation op, ExprList &arg, TransactionContext &txn) {
	Init();
	type_ = EXPR;
	op_ = op;
	arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
	arglist_->assign(arg.begin(), arg.end());
}

/*!
 * @brief Ctor of function operation
 *
 * @param name Function name (for debug purpose)
 * @param func Function pointer
 * @param arg Function argument
 * @param txn The transaction context
 */
Expr::Expr(
	const char *name, TqlFunc *func, ExprList *arg, TransactionContext &txn) {
	Init();
	type_ = FUNCTION;
	size_t len = strlen(name) + 1;
	label_ = (char *)txn.getDefaultAllocator().allocate(len);
	memcpy(label_, name, len);
	arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
	if (arg != NULL) {
		arglist_->assign(arg->begin(), arg->end());
	}
	functor_ = reinterpret_cast<void *>(func);
}

/*!
 * @brief Ctor of Aggregation function
 *
 * @param name Function name (for debug purpose)
 * @param func Function pointer
 * @param arg Function argument
 * @param txn The transaction context
 */
Expr::Expr(const char *name, TqlAggregation *func, ExprList *arg,
	TransactionContext &txn) {
	Init();
	type_ = AGGREGATION;
	size_t len = strlen(name) + 1;
	label_ = static_cast<char *>(txn.getDefaultAllocator().allocate(len));
	memcpy(label_, name, len);
	arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
	if (arg != NULL) {
		arglist_->assign(arg->begin(), arg->end());
	}
	functor_ = reinterpret_cast<void *>(func);
}

/*!
 * @brief Ctor of Selection function
 *
 * @param name Function name (for debug purpose)
 * @param func Function pointer
 * @param arg Function argument
 * @param txn The transaction context
 */
Expr::Expr(const char *name, TqlSelection *func, ExprList *arg,
	TransactionContext &txn) {
	Init();
	type_ = SELECTION;
	size_t len = strlen(name) + 1;
	label_ = static_cast<char *>(txn.getDefaultAllocator().allocate(len));
	memcpy(label_, name, len);
	arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
	if (arg != NULL) {
		arglist_->assign(arg->begin(), arg->end());
	}
	functor_ = reinterpret_cast<void *>(func);
}

/*!
 * @brief Ctor of array value
 * @param array array value
 */
Expr::Expr(Value *array, TransactionContext &) {
	Init();
	type_ = VALUE;
	value_ = array;
}

/*!
 * @brief Ctor of geometry
 * @param geom Geometry object (deserialized)
 * @param txn The transaction context
 */
Expr::Expr(Geometry *geom, TransactionContext &txn) {
	Init();
	type_ = VALUE;
	buf_ = QP_NEW util::XArray<uint8_t>(txn.getDefaultAllocator());
	geomCache_ = geom;
	value_ = QP_NEW Value(geom);
}

/*!
 * @brief Ctor of ExprList
 * @param args Expr array
 * @param txn The transaction context
 * @param txn Object manager
 */
Expr::Expr(
	ExprList *args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	Init();
	type_ = EXPRARRAY;
	arglist_ = QP_NEW ExprList(txn.getDefaultAllocator());
	if (args == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
			"Internal logic error: Array cannot be constructed with NULL "
			"expression list");
	}
	if (!args->empty()) {
		ExprList::const_iterator it = args->begin();
		Expr::Type tx = (*it)->getType();
		ColumnType ct = COLUMN_TYPE_WITH_BEGIN;
		if (tx == VALUE && (*it)->value_ != NULL) {
			ct = (*it)->value_->getType();
		}
		while (it != args->end()) {
			if ((*it)->getType() != tx ||
				(tx == VALUE && (*it)->value_->getType() != ct)) {
				it = arglist_->begin();
				while (it != arglist_->end()) {
					QP_DELETE(*it);
					it++;
				}
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Array cannot be constructed with multiple types");
			}
			arglist_->push_back((*it)->dup(txn, objectManager, strategy));
			it++;
		}
	}
}

Expr::Expr(TrivalentLogicType logicType, TransactionContext &txn) {
	Init();
	if (logicType == TRI_NULL) {
		type_ = NULLVALUE;
		value_ = QP_NEW Value();
		value_->setNull();
	} else {
		type_ = VALUE;
		value_ = QP_NEW Value(logicType == TRI_TRUE);
	}
}

/*!
 * @brief Dtor
 */
Expr::~Expr() {
	if (arglist_ != NULL) {
		for (QP_XArray<Expr *>::iterator it = arglist_->begin();
			 it != arglist_->end(); it++) {
			Expr *p = *it;
			QP_SAFE_DELETE(p);
		}
		QP_SAFE_DELETE(arglist_);
	}
	if (label_) {
		label_ = NULL;
	}
	QP_SAFE_DELETE(buf_);
	QP_SAFE_DELETE(value_);
	switch (type_) {
	case FUNCTION:
		reinterpret_cast<TqlFunc *>(functor_)->destruct();
		break;
	case AGGREGATION:
		reinterpret_cast<TqlAggregation *>(functor_)->destruct();
		break;
	case SELECTION:
		reinterpret_cast<TqlSelection *>(functor_)->destruct();
		break;
	case COLUMN:
		if (functor_) {
			reinterpret_cast<TqlSpecialId *>(functor_)->destruct();
			break;
		}
	default:
		break;  
	}
}

/*!
 * @brief Create new expression to evaluated expression.
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param column_values Values to assign into columns
 * @param function_map Available functions
 * @param mode Evaluation mode (evaluation, contraction, print-only)
 *
 * @return Evaluation result as expression.
 */
Expr *Expr::eval(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	ContainerRowWrapper *column_values, FunctionMap *function_map,
	EvalMode mode) {
	switch (type_) {
	case VALUE:
		if (value_->getType() == COLUMN_TYPE_GEOMETRY) {
			Geometry *g = getGeometry();
			Expr *p =
				Expr::newGeometryValue(g->assign(txn, objectManager, strategy,
										   column_values, function_map, mode),
					txn);
			return p;
		}
		return this->dup(txn, objectManager, strategy);
	case NULLVALUE:
		return Expr::newNullValue(txn);
	case EXPR:
		{
			switch (this->op_) {
			case ADD:
				return evalSubBinOp(txn, objectManager, strategy, CalculatorTable::addTable_, column_values,
					function_map, "+", mode);
			case SUB:
				return evalSubBinOp(txn, objectManager, strategy, CalculatorTable::subTable_, column_values,
					function_map, "-", mode);
			case MUL:
				return evalSubBinOp(txn, objectManager, strategy, CalculatorTable::mulTable_, column_values,
					function_map, "*", mode);
			case DIV:
				return evalSubBinOp(txn, objectManager, strategy, CalculatorTable::divTable_, column_values,
					function_map, "/", mode);
			case REM:
				return evalSubBinOp(txn, objectManager, strategy, CalculatorTable::modTable_, column_values,
					function_map, "%", mode);
			case NE:
				return evalSubBinOp(txn, objectManager, strategy, ComparatorTable::neTable_, column_values,
					function_map, "<>", mode);
			case EQ:
				return evalSubBinOp(txn, objectManager, strategy, ComparatorTable::eqTable_, column_values,
					function_map, "=", mode);
			case LT:
				return evalSubBinOp(txn, objectManager, strategy, ComparatorTable::ltTable_, column_values,
					function_map, "<", mode);
			case LE:
				return evalSubBinOp(txn, objectManager, strategy, ComparatorTable::leTable_, column_values,
					function_map, "<=", mode);
			case GT:
				return evalSubBinOp(txn, objectManager, strategy, ComparatorTable::gtTable_, column_values,
					function_map, ">", mode);
			case GE:
				return evalSubBinOp(txn, objectManager, strategy, ComparatorTable::geTable_, column_values,
					function_map, ">=", mode);

			case PLUS:
				return evalSubUnaryOpBaseZero(txn, objectManager, strategy, CalculatorTable::addTable_,
					column_values, function_map, "+", mode);
			case MINUS:
			case BITMINUS:
				return evalSubUnaryOpBaseZero(txn, objectManager, strategy, CalculatorTable::subTable_,
					column_values, function_map, "-", mode);
			case IS:
				return evalSubBinOp(txn, objectManager, strategy, this->op_, column_values,
					function_map, "IS", mode);

			case ISNOT:
				return evalSubBinOp(txn, objectManager, strategy, this->op_, column_values,
					function_map, "ISNOT", mode);

			default:
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: not implemented operation.");
			}
		}
	case FUNCTION:  
	{
		if (mode == EVAL_MODE_PRINT) {
			util::NormalOStringStream os;

			os << label_ << '(';
			if (arglist_ != NULL) {
				for (ExprList::const_iterator it2 = arglist_->begin();
					 it2 != arglist_->end(); it2++) {
					Expr *e = (*it2)->eval(
						txn, objectManager, strategy, column_values, function_map, mode);
					if (e->isString()) {
						os << '\'' << e->getValueAsString(txn) << "',";
					}
					else {
						os << e->getValueAsString(txn) << ',';
					}
					QP_DELETE(e);
				}
			}

			std::string str = os.str();
			if (arglist_ != NULL && arglist_->size() > 0) {
				str = str.substr(0, str.size() - 1);
			}
			str += ')';
			return newExprLabel(str.c_str(), txn);
		}
		else {
			TqlFunc *func = reinterpret_cast<TqlFunc *>(functor_);
			Expr *ret = NULL;
			ExprList argsAfterEval(txn.getDefaultAllocator());
			try {
				ret = (*func)(*arglist_, column_values, function_map, mode, txn,
					objectManager, strategy, argsAfterEval);
			}
			catch (util::Exception &) {
				if (mode == EVAL_MODE_CONTRACT) {
					Expr *e = this->dup(txn, objectManager, strategy);
					if (e->arglist_ != NULL) {
						for (size_t i = 0; i < argsAfterEval.size(); i++) {
							QP_DELETE((*e->arglist_)[i]);
							(*e->arglist_)[i] = argsAfterEval[i];
						}
					}
					return e;
				}
				else {
					throw;
				}
			}
			return ret;
		}
	}
	case COLUMN:  
	{
		if (column_values == NULL ||
			(label_ != NULL && strcmp(label_, "*") == 0)) {
			if (columnInfo_ == NULL && strcmp(label_, "*") != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_NOT_FOUND,
					(util::String("No such column ", QP_ALLOCATOR) +
						getValueAsString(txn))
						.c_str());
			}
			switch (mode) {
			case EVAL_MODE_PRINT:
				return Expr::newExprLabel(getValueAsString(txn), txn);
			case EVAL_MODE_CONTRACT:
				return this->dup(txn, objectManager, strategy);
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					(util::String(
						 "Invalid argument type : cannot bind ", QP_ALLOCATOR) +
						getValueAsString(txn))
						.c_str());
			}
		}
		const Value *v;
		if (columnId_ != UNDEF_COLUMNID) {
			v = (*column_values)[columnId_];
		}
		else {
			switch (mode) {
			case EVAL_MODE_PRINT:
				return Expr::newExprLabel(getValueAsString(txn), txn);
			case EVAL_MODE_CONTRACT:
				return this->dup(txn, objectManager, strategy);
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TQ_INVALID_NAME,
					(util::String(
						 "Internal logic error: cannot bind ", QP_ALLOCATOR) +
						getValueAsString(txn))
						.c_str());
			}
		}
		ColumnType t = v->getType();
		if (v->isNullValue()) {
			return Expr::newNullValue(txn);
		} else 
		if (!v->isArray()) {
			switch (t) {
			case COLUMN_TYPE_STRING:
				return Expr::newStringValue(
					reinterpret_cast<const char *>(v->data()), v->size(), txn);
			case COLUMN_TYPE_GEOMETRY:
				{
					Geometry *geom =
						Geometry::deserialize(txn, v->data(), v->size());
					return Expr::newGeometryValue(geom, txn);
				}
			case COLUMN_TYPE_BOOL:
				return Expr::newBooleanValue(
					*reinterpret_cast<const bool *>(v->data()), txn);
			case COLUMN_TYPE_BYTE:
				return Expr::newNumericValue(
					*reinterpret_cast<const int8_t *>(v->data()), txn);
			case COLUMN_TYPE_SHORT:
				return Expr::newNumericValue(
					*reinterpret_cast<const int16_t *>(v->data()), txn);
			case COLUMN_TYPE_INT:
				return Expr::newNumericValue(
					*reinterpret_cast<const int32_t *>(v->data()), txn);
			case COLUMN_TYPE_LONG:
				return Expr::newNumericValue(
					*reinterpret_cast<const int64_t *>(v->data()), txn);
			case COLUMN_TYPE_FLOAT:
				return Expr::newNumericValue(
					*reinterpret_cast<const float *>(v->data()), txn);
			case COLUMN_TYPE_DOUBLE:
				return Expr::newNumericValue(
					*reinterpret_cast<const double *>(v->data()), txn);
			case COLUMN_TYPE_TIMESTAMP:
				return Expr::newTimestampValue(
					*reinterpret_cast<const Timestamp *>(v->data()), txn);
			case COLUMN_TYPE_MICRO_TIMESTAMP:
				return Expr::newTimestampValue(
					*reinterpret_cast<const MicroTimestamp *>(v->data()), txn);
			case COLUMN_TYPE_NANO_TIMESTAMP:
				return Expr::newTimestampValue(
					*reinterpret_cast<const NanoTimestamp *>(v->data()), txn);
			default:
				break;  
			}
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Invalid argument type: Unknown data type_ or "
				"cannot evaluate in this context.");
		}
		else {
			Value *av;
			switch (v->getType()) {
			case COLUMN_TYPE_STRING_ARRAY:
			case COLUMN_TYPE_BOOL_ARRAY:
			case COLUMN_TYPE_BYTE_ARRAY:
			case COLUMN_TYPE_SHORT_ARRAY:
			case COLUMN_TYPE_INT_ARRAY:
			case COLUMN_TYPE_LONG_ARRAY:
			case COLUMN_TYPE_FLOAT_ARRAY:
			case COLUMN_TYPE_DOUBLE_ARRAY:
			case COLUMN_TYPE_TIMESTAMP_ARRAY:
				av = QP_NEW Value();
				av->set(v->data(), v->size(), v->getType());
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: Internal logic error: cannot "
					"determine column type.");
			}
			return Expr::newArrayValue(av, txn);
		}
	}

	case EXPRARRAY: {
		ExprList args(txn.getDefaultAllocator());
		if (arglist_ != NULL) {
			for (ExprList::const_iterator it2 = arglist_->begin();
				 it2 != arglist_->end(); it2++) {
				args.insert(args.end(), (*it2)->eval(txn, objectManager, strategy,
											column_values, function_map, mode));
			}
		}

		Expr *ret = Expr::newExprArray(args, txn, objectManager, strategy);

		for (ExprList::const_iterator it2 = args.begin(); it2 != args.end();
			 it2++) {
			QP_DELETE(*it2);
		}

		return ret;
	}
	case BOOL_EXPR:
	{
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: can not eval BOOL_EXPR.");
	}
	case AGGREGATION:
	case SELECTION:
		if (mode == EVAL_MODE_PRINT) {
			assert(label_ != NULL);
			util::NormalOStringStream os;
			std::string str;
			os << label_ << '(';
			if (arglist_ != NULL) {
				for (ExprList::const_iterator it2 = arglist_->begin();
					 it2 != arglist_->end(); it2++) {
					Expr *e = (*it2)->eval(
						txn, objectManager, strategy, column_values, function_map, mode);
					if (e->isString()) {
						os << '\'' << e->getValueAsString(txn) << "',";
					}
					else {
						os << e->getValueAsString(txn) << ',';
					}
					QP_DELETE(e);
				}

				str = os.str();  
				if (arglist_->size() > 0) {
					str = str.substr(0, str.size() - 1);
				}
			}
			str += ')';
			return newExprLabel(str.c_str(), txn);
		}
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid argument type: Unknown data type_ or "
			"cannot evaluate in this context.");
	}
}

/*!
 * @brief Boolean two-operand operation
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param opTable Operator functions
 * @param column_values Bind environment
 * @param function_map Function environment
 * @param mark Symbol of a operation (for printing purpose)
 * @param mode Evaluation mode (evaluation, contraction, print-only)
 *
 * @return Evaluated expression
 */
Expr *Expr::evalSubBinOp(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, const ValueOperatorTable &opTable,
		ContainerRowWrapper *column_values, FunctionMap *function_map,
		const char *mark, EvalMode mode) {
	Expr *x1, *x2;

	assert(arglist_ && (*arglist_)[0] && (*arglist_)[1]);

	x1 = (*arglist_)[0]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);
	x2 = (*arglist_)[1]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);

	if (x1->isValueOrColumn() && x2->isValueOrColumn() &&
			(ValueProcessor::isArray(x1->getColumnType()) ||
			ValueProcessor::isArray(x2->getColumnType()) ||
			ComparatorTable::findOperator(
					opTable,
					x1->getColumnType(), x2->getColumnType()) == NULL)) {
		ColumnType colType1 = x1->getColumnType();
		ColumnType colType2 = x2->getColumnType();

		QP_SAFE_DELETE(x1);
		QP_SAFE_DELETE(x2);
		GS_THROW_USER_ERROR(GS_ERROR_TQ_OPERATION_NOT_DEFINED,
			"Binary operation is not defined for the types "
				<< ValueProcessor::getTypeName(colType1).c_str() << " and "
				<< ValueProcessor::getTypeName(colType2).c_str() << "");
	}
	if (x1->isNullValue() || x2->isNullValue()) {
		QP_DELETE(x1);
		QP_DELETE(x2);
		return Expr::newNullValue(txn);
	}
	if (!x1->isValue() || !x2->isValue() ||
			ValueProcessor::isArray(x1->value_->getType()) ||
			ValueProcessor::isArray(x2->value_->getType()) ||
			ComparatorTable::findOperator(
					opTable,
					x1->value_->getType(), x2->value_->getType()) == NULL) {
		switch (mode) {
		case EVAL_MODE_PRINT: {
			util::String x("(", QP_ALLOCATOR);  
			x += x1->getValueAsString(txn);
			x += mark;
			x += x2->getValueAsString(txn);
			x += ")";
			Expr *ret = Expr::newExprLabel(x.c_str(), txn);
			QP_SAFE_DELETE(x1);
			QP_SAFE_DELETE(x2);
			return ret;
		}
		case EVAL_MODE_CONTRACT: {
			Expr *ret = this->dup(txn, objectManager, strategy);
			QP_DELETE((*ret->arglist_)[0]);
			QP_DELETE((*ret->arglist_)[1]);
			(*ret->arglist_)[0] = x1;
			(*ret->arglist_)[1] = x2;
			return ret;
		}
		default:
			QP_SAFE_DELETE(x1);
			QP_SAFE_DELETE(x2);
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_CRITICAL_LOGIC_ERROR, "Operation not defined");
		}
	}

	bool d = ComparatorTable::getOperator(
			opTable, x1->value_->getType(), x2->value_->getType())(
			txn,
			x1->value_->data(), x1->value_->size(),
			x2->value_->data(), x2->value_->size());
	QP_DELETE(x1);
	QP_DELETE(x2);

	return Expr::newBooleanValue(d, txn);
}

/*!
 * @brief Generic two-operand operation
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param opTable Operator functions
 * @param column_values Bind environment
 * @param function_map Function environment
 * @param mark Symbol of a operation (for printing purpose)
 * @param mode Evaluation mode (evaluation, contraction, print-only)
 *
 * @return Evaluated expression
 */
Expr *Expr::evalSubBinOp(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, const ValueCalculatorTable &opTable,
		ContainerRowWrapper *column_values, FunctionMap *function_map,
		const char *mark, EvalMode mode) {
	Expr *x1, *x2;

	assert(arglist_ && (*arglist_)[0] && (*arglist_)[1]);
	x1 = (*arglist_)[0]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);
	x2 = (*arglist_)[1]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);

	if (x1->isValueOrColumn() && x2->isValueOrColumn() &&
			(ValueProcessor::isArray(x1->getColumnType()) ||
			ValueProcessor::isArray(x2->getColumnType()) ||
			CalculatorTable::findCalculator(
					opTable,
					x1->getColumnType(), x2->getColumnType()) == NULL)) {
		ColumnType colType1 = x1->getColumnType();
		ColumnType colType2 = x2->getColumnType();

		QP_SAFE_DELETE(x1);
		QP_SAFE_DELETE(x2);
		GS_THROW_USER_ERROR(GS_ERROR_TQ_OPERATION_NOT_DEFINED,
			"Binary operation is not defined for the types "
				<< ValueProcessor::getTypeName(colType1).c_str() << " and "
				<< ValueProcessor::getTypeName(colType2).c_str() << "");
	}

	if (x1->isNullValue() || x2->isNullValue()) {
		QP_DELETE(x1);
		QP_DELETE(x2);
		return Expr::newNullValue(txn);
	} else 
	if (!x1->isValue() || !x2->isValue()) {
		switch (mode) {
		case EVAL_MODE_PRINT: {
			util::String x("(", QP_ALLOCATOR);  
			x += x1->getValueAsString(txn);
			x += mark;
			x += x2->getValueAsString(txn);
			x += ")";
			Expr *ret = Expr::newExprLabel(x.c_str(), txn);
			QP_SAFE_DELETE(x1);
			QP_SAFE_DELETE(x2);
			return ret;
		}
		case EVAL_MODE_CONTRACT: {
			Expr *ret = this->dup(txn, objectManager, strategy);
			QP_DELETE((*ret->arglist_)[0]);
			QP_DELETE((*ret->arglist_)[1]);
			(*ret->arglist_)[0] = x1;
			(*ret->arglist_)[1] = x2;
			return ret;
		}
		default:
			QP_SAFE_DELETE(x1);
			QP_SAFE_DELETE(x2);
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: cannot evaluate expression.");
		}
	}

	Expr *ret = NULL;
	ret = Expr::newNumericValue(0, txn);

	CalculatorTable::getCalculator(
			opTable, x1->value_->getType(), x2->value_->getType())(
			txn,
			x1->value_->data(), x1->value_->size(),
			x2->value_->data(), x2->value_->size(),
			*ret->value_);

	QP_DELETE(x1);
	QP_DELETE(x2);

	return ret;
}

/*!
 * @brief Boolean two-operand operation
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param opTable Operator functions
 * @param column_values Bind environment
 * @param function_map Function environment
 * @param mark Symbol of a operation (for printing purpose)
 * @param mode Evaluation mode (evaluation, contraction, print-only)
 *
 * @return Evaluated expression
 */
Expr *Expr::evalSubBinOp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	const Operation opType, ContainerRowWrapper *column_values,
	FunctionMap *function_map, const char *mark, EvalMode mode) {
	Expr *x1, *x2;

	assert(arglist_ && (*arglist_)[0] && (*arglist_)[1]);

	x1 = (*arglist_)[0]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);
	x2 = (*arglist_)[1]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);

	if (!x2->isNullValue()) {
		QP_SAFE_DELETE(x1);
		QP_SAFE_DELETE(x2);
		GS_THROW_USER_ERROR(GS_ERROR_TQ_OPERATION_NOT_DEFINED,
			"Right operand of \"IS\" or \"ISNOT\" must be null");
	}
	if (opType != IS && opType != ISNOT) {
		QP_SAFE_DELETE(x1);
		QP_SAFE_DELETE(x2);
		GS_THROW_USER_ERROR(
				GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: Operation not defined, operation type = " <<
				static_cast<int32_t>(opType));
	}

	bool result = false;
	if (x1->isColumn() && !x1->isNullable() && opType == IS) {
	} else {
		if (!x1->isValue() && !x1->isNullValue()) {
			switch (mode) {
			case EVAL_MODE_PRINT: {
				util::String x("(", QP_ALLOCATOR);  
				x += x1->getValueAsString(txn);
				x += mark;
				x += x2->getValueAsString(txn);
				x += ")";
				Expr *ret = Expr::newExprLabel(x.c_str(), txn);
				QP_SAFE_DELETE(x1);
				QP_SAFE_DELETE(x2);
				return ret;
			}
			case EVAL_MODE_CONTRACT: {
				Expr *ret = this->dup(txn, objectManager, strategy);
				QP_DELETE((*ret->arglist_)[0]);
				QP_DELETE((*ret->arglist_)[1]);
				(*ret->arglist_)[0] = x1;
				(*ret->arglist_)[1] = x2;
				return ret;
			}
			default:
				QP_SAFE_DELETE(x1);
				QP_SAFE_DELETE(x2);
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CRITICAL_LOGIC_ERROR, "Internal logic error: cannot evaluate expression.");
			}
		}
		if ((x1->isNullValue() && opType == IS) ||
			(!x1->isNullValue() && opType == ISNOT)) {
			result = true;
		}
	}
	QP_DELETE(x1);
	QP_DELETE(x2);
	return Expr::newBooleanValue(result, txn);
}


/*!
 * @brief Force run two operand function with 0 and arg
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param opTable Operator function
 * @param column_values Bind environment
 * @param function_map Function environment
 * @param mark Symbol of a operation (for printing purpose)
 * @param mode Evaluation mode (evaluation, contraction, print-only)
 *
 * @return Evaluated expression
 */
Expr *Expr::evalSubUnaryOpBaseZero(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, const ValueCalculatorTable &opTable,
		ContainerRowWrapper *column_values, FunctionMap *function_map,
		const char *mark, EvalMode mode) {
	Expr *x;
	assert(arglist_ && (*arglist_)[0]);
	x = (*arglist_)[0]->eval(
		txn, objectManager, strategy, column_values, function_map, mode);
	Expr *ret = Expr::newNumericValue(0, txn);

	if (x->isNullValue()) {
		QP_DELETE(x);
		return Expr::newNullValue(txn);
	} else 
	if (x->isNumericInteger() && x->getValueAsInt64() == INT64_MIN) {
		Expr *ret2 = Expr::newNumericValue(INT64_MIN, txn);
		QP_SAFE_DELETE(x);
		QP_SAFE_DELETE(ret);
		switch (mode) {
		case EVAL_MODE_PRINT: {
			std::string tmp = "(";  
			tmp += mark;
			tmp += ret2->getValueAsString(txn);
			tmp += ")";
			ret = Expr::newExprLabel(tmp.c_str(), txn);
			QP_SAFE_DELETE(ret2);
			return ret;
		}
		case EVAL_MODE_CONTRACT: {
			return ret2;
		}
		default:
			QP_SAFE_DELETE(ret2);
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: cannot evaluate expression.");
		}
		return ret2;
	}
	else {
		if (!x->isNumeric() || CalculatorTable::findCalculator(
				opTable,
				ret->value_->getType(), x->value_->getType()) == NULL) {
			QP_SAFE_DELETE(ret);
			switch (mode) {
			case EVAL_MODE_PRINT: {
				std::string tmp = "(";
				tmp += mark;
				tmp += x->getValueAsString(txn);
				tmp += ")";
				ret = Expr::newExprLabel(tmp.c_str(), txn);
				QP_SAFE_DELETE(x);
				return ret;
			}
			case EVAL_MODE_CONTRACT: {
				ret = this->dup(txn, objectManager, strategy);
				QP_DELETE((*ret->arglist_)[0]);
				(*ret->arglist_)[0] = x;
				return ret;
			}
			default:
				QP_SAFE_DELETE(x);
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: cannot evaluate expression.");
			}
		}
	}

	CalculatorTable::getCalculator(
			opTable, ret->value_->getType(), x->value_->getType())(
			txn, ret->value_->data(), 0, x->value_->data(),
			0, *ret->value_);
	QP_DELETE(x);

	return ret;
}

/*!
 * @brief Get column name for test purpose
 *
 * @param txn The transaction context
 * @param[in,out] x result string
 */
void Expr::getMappingColumnInfo(
	TransactionContext &txn, std::vector<util::String> &x) {
	if (type_ == COLUMN) {
		x.push_back(util::String(getValueAsString(txn), QP_ALLOCATOR));
		return;
	}
	if (arglist_ != NULL) {
		for (size_t i = 0; i < arglist_->size(); i++) {
			(*arglist_)[i]->getMappingColumnInfo(txn, x);
		}
	}
}

/*!
 * @brief Obtain mapping variable info for test purpose
 *
 * @param txn The transaction context
 * @param x result string
 */
void Expr::getMappingVariableInfo(
	TransactionContext &txn, std::vector<util::String> &x) {
	if (type_ == VARIABLE) {
		util::NormalOStringStream os;
		os << getValueAsString(txn) << x.size();
		x.push_back(util::String(os.str().c_str(), QP_ALLOCATOR));
		return;
	}
	if (arglist_ != NULL) {
		for (size_t i = 0; i < arglist_->size(); i++) {
			(*arglist_)[i]->getMappingVariableInfo(txn, x);
		}
	}
}

/*!
 * @brief Equality comparison
 *
 * @param e Expression to compare
 *
 * @return Comparison result
 */
bool Expr::operator==(const Expr &e) const {
	if (e.type_ != type_) {
		return false;
	}
	switch (type_) {
	case VALUE: {
		assert(value_ != NULL);
		ColumnType vt = value_->getType();
		if (vt != e.value_->getType()) {
			return false;
		}
		const Operator op = ComparatorTable::findOperator(
				ComparatorTable::eqTable_, vt, vt);
		if (op) {
			int a;
			return op((TransactionContext &)a, value_->data(),
				value_->size(), e.value_->data(), value_->size());
		}
		else {
			return value_->data() == e.value_->data();
		}
	}
	case FUNCTION:
	case SELECTION:
	case AGGREGATION:
		if (strcmp(label_, e.label_) != 0) {
			return false;
		}
	case EXPRARRAY:
	case EXPR: {
		if (e.arglist_ == NULL && arglist_ == NULL) {
			return true;
		}
		else if (e.arglist_ == NULL || arglist_ == NULL) {
			return false;
		}
		else if (e.arglist_->size() != arglist_->size()) {
			return false;
		}
		for (size_t i = 0; i < arglist_->size(); i++) {
			if (*(*e.arglist_)[i] != *(*arglist_)[i]) {
				return false;
			}
		}
		return true;
	}
	case BOOL_EXPR:
	{
		return (
				static_cast<const BoolExpr&>(*this) ==
				static_cast<const BoolExpr&>(e));
	}
	case COLUMN:
	case EXPRLABEL:
		return strcmp(label_, e.label_) == 0;
	case VARIABLE:
		return true;  
	case NULLVALUE:
		if (e.isNullValue()) { 
			return true;
		} else {
			return false;
		}
	default:
		break;
	}
	return false;
}

/*!
 * @brief Check for that this expression is included by.
 *
 * @param outer Expr to test that it includes this expr.
 *
 * @return Test result
 */
bool Expr::isIncludedBy(Expr *outer) {
	if (this->type_ != EXPR || outer->type_ != EXPR) {
		return false;
	}

	switch (this->op_) {
	case NE:
	case EQ:
		if (*outer == *this) {
			return true;
		}
		return false;

	case LT:
	case LE:
	case GT:
	case GE:
	case BETWEEN:
	case NOTBETWEEN:

	default:
		return false;
	}
}

/*!
 * @brief Check for that this expression is antinomy of the passed expression
 *
 * @param testExpr Expr to test
 *
 * @return Test result
 */
bool Expr::isAntinomy(Expr *testExpr) {
	if (this->type_ != EXPR || testExpr->type_ != EXPR) {
		return false;
	}

	switch (this->op_) {
	case NE:
		if (testExpr->op_ == NE) {
			return *((*arglist_)[1]) == *(*testExpr->arglist_)[1];
		}
		return false;
	case EQ:
		if (testExpr->op_ == EQ) {
			return *((*arglist_)[1]) != *(*testExpr->arglist_)[1];
		}
		return false;
	case LT:

	case LE:
	case GT:
	case GE:
	case BETWEEN:
	case NOTBETWEEN:

	default:
		return false;
	}
}

/*!
 * @brief Transpose a compare-expression
 *        ex) A<1 -> A-1<0
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @return a contracted expression
 */
Expr *Expr::transposeExpr(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	if (type_ != EXPR) {
		return dup(txn, objectManager, strategy);  
	}
	switch (op_) {
	case NE:
	case EQ:
	case LT:
	case LE:
	case GT:
	case GE: {
	}
	default:
		return dup(txn, objectManager, strategy);  
	}
}

/*!
 * @param txn Transaction Context
 * @brief Get column name from expression.
 *
 * @param[out] outName
 */
void Expr::getColumnName(TransactionContext &txn, util::String &outName) {
	if (type_ == COLUMN) {
		assert(label_ != NULL);
		outName = util::String(label_, QP_ALLOCATOR);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_NOT_FOUND, "No column name.");
	}
}

/*!
 * @brief Get column id from expression.
 *
 * @return columnId
 */
uint32_t Expr::getColumnId() {
	if (type_ == COLUMN) {
		return columnId_;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_NOT_FOUND, "No column id.");
	}
}

/*!
 * @brief Get column info from expression.
 *
 * @return ColumnInfo
 */
const ColumnInfo *Expr::getColumnInfo() {
	if (type_ == COLUMN) {
		return columnInfo_;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_NOT_FOUND, "No column id.");
	}
}

/*!
 * @brief transform expression into TermCondition
 *
 * @return list of TermCondition
 */
TermCondition *Expr::toCondition(
		TransactionContext &txn, MapType mapType, Query &queryObj,
		bool notFlag, bool &semiFiltering) {
	UNUSED_VARIABLE(queryObj);

	semiFiltering = false;

	TermCondition c;
	Operation op = op_;  
	bool isValueCastableToKey =
		false;  
	uint8_t *indexValue = NULL;
	uint32_t indexValueSize = 0;
	Expr *indexExpr;

	if (type_ != EXPR && type_ != COLUMN && type_ != FUNCTION) {
		return NULL;
	}
	else if (type_ == EXPR) {
		ExprList &argList = *arglist_;
		if (arglist_ == NULL || argList.size() != 2) {
			return NULL;  
		}
		else if (!((argList[0]->type_ == COLUMN &&
					   argList[1]->type_ == VALUE) ||
					 (argList[1]->type_ == COLUMN &&
						 argList[0]->type_ == VALUE)
						 ||
					 (argList[0]->type_ == COLUMN &&
						 argList[1]->type_ == NULLVALUE) ||
					 (argList[1]->type_ == COLUMN &&
						 argList[0]->type_ == NULLVALUE)
						 )) {
			return NULL;
		}
		else {
		}

		ColumnType columnType;  

		if (argList[0]->type_ == COLUMN) {
			columnType = argList[0]->columnType_;
			c.valueType_ = argList[1]->value_->getType();
			c.columnId_ = argList[0]->columnId_;
			c.value_ = argList[1]->value_->data();
			c.valueSize_ = argList[1]->value_->size();
			indexExpr = argList[1];
		}
		else {
			columnType = argList[1]->columnType_;
			c.valueType_ = argList[0]->value_->getType();
			c.value_ = argList[0]->value_->data();
			c.valueSize_ = argList[0]->value_->size();
			c.columnId_ = argList[1]->columnId_;
			indexExpr = argList[0];
			switch (op) {
			case LT:
				op = GT;
				break;
			case LE:
				op = GE;
				break;
			case GT:
				op = LT;
				break;
			case GE:
				op = LE;
				break;
			default:
				break;
			}
		}

		if (notFlag) {
			switch (op) {
			case EQ:
				op = NE;
				break;
			case NE:
				op = EQ;
				break;
			case LT:
				op = GE;
				break;
			case LE:
				op = GT;
				break;
			case GT:
				op = LE;
				break;
			case GE:
				op = LT;
				break;
			case IS:
				op = ISNOT;
				break;
			case ISNOT:
				op = IS;
				break;
			default:
				break;
			}
		}
		switch (columnType) {
		case COLUMN_TYPE_STRING:
			isValueCastableToKey = (c.valueType_ == COLUMN_TYPE_STRING &&
									mapType != MAP_TYPE_SPATIAL);
			break;
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG: {
			int64_t i64;
			double d;
			isValueCastableToKey = (
				mapType != MAP_TYPE_SPATIAL &&
				ValueProcessor::isInteger(c.valueType_));
			if (isValueCastableToKey && columnType < c.valueType_) {
				isValueCastableToKey =
					checkValueRange(columnType, c.valueType_, static_cast<const uint8_t *>(c.value_), i64, d);
			}
			break;
		}
		case COLUMN_TYPE_FLOAT:
		case COLUMN_TYPE_DOUBLE: {
			isValueCastableToKey = (
				mapType != MAP_TYPE_SPATIAL &&
				ValueProcessor::isNumerical(c.valueType_));
			if (isValueCastableToKey && (ValueProcessor::isInteger(c.valueType_) ||
					c.valueType_ == COLUMN_TYPE_DOUBLE)) {
				double d;
				int64_t i64;
				if (checkValueRange(columnType, c.valueType_, c.value_, i64, d)) {
					if (columnType == COLUMN_TYPE_FLOAT) {
						float *f = QP_NEW float;
						if (util::isNaN(d)) {
							*f = std::numeric_limits<float>::quiet_NaN();
						}
						else if (util::isInf(d)) {
							*f = (d > 0)
									 ? std::numeric_limits<float>::infinity()
									 : -std::numeric_limits<float>::infinity();
						}
						else {
							*f = static_cast<float>(d);
						}
						indexValue = reinterpret_cast<uint8_t *>(f);
						indexValueSize = sizeof(float);
					}
					else {
						double *dp = QP_NEW double(d);
						indexValue = reinterpret_cast<uint8_t *>(dp);
						indexValueSize = sizeof(double);
					}
				}
				else {
					isValueCastableToKey = false;
				}
			}
			break;
		}
		case COLUMN_TYPE_TIMESTAMP:
			isValueCastableToKey = (
					mapType != MAP_TYPE_SPATIAL &&
					ValueProcessor::isTimestampFamily(c.valueType_));
			if (isValueCastableToKey && c.valueType_ != columnType) {
				Value condValue;
				condValue.set(c.value_, c.valueType_);

				const Timestamp ts = condValue.getTimestamp();
				indexValue = reinterpret_cast<uint8_t*>(QP_NEW Timestamp(ts));
				indexValueSize = sizeof(ts);

				const NanoTimestamp orgTs = condValue.getNanoTimestamp();
				semiFiltering = (ValueProcessor::compareTimestamp(
						orgTs, ValueProcessor::getNanoTimestamp(ts)) != 0);
			}
			break;
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			isValueCastableToKey = (
					mapType != MAP_TYPE_SPATIAL &&
					ValueProcessor::isTimestampFamily(c.valueType_));
			if (isValueCastableToKey && c.valueType_ != columnType) {
				Value condValue;
				condValue.set(c.value_, c.valueType_);

				const MicroTimestamp ts = condValue.getMicroTimestamp();
				indexValue = reinterpret_cast<uint8_t*>(
						QP_NEW MicroTimestamp(ts));
				indexValueSize = sizeof(ts);

				const NanoTimestamp orgTs = condValue.getNanoTimestamp();
				semiFiltering = (ValueProcessor::compareTimestamp(
						orgTs, ValueProcessor::getNanoTimestamp(ts)) != 0);
			}
			break;
		case COLUMN_TYPE_NANO_TIMESTAMP:
			isValueCastableToKey = (
					mapType != MAP_TYPE_SPATIAL &&
					ValueProcessor::isTimestampFamily(c.valueType_));
			if (isValueCastableToKey && c.valueType_ != columnType) {
				Value condValue;
				condValue.set(c.value_, c.valueType_);

				NanoTimestamp ts = condValue.getNanoTimestamp();
				indexValue = reinterpret_cast<uint8_t*>(
						QP_NEW NanoTimestamp(ts));
				indexValueSize = sizeof(ts);
			}
			break;
		default:
			isValueCastableToKey = false;
			break;
		}

		UNUSED_VARIABLE(indexExpr);
		if (isValueCastableToKey) {
			if (indexValue != NULL) {
				c.value_ = indexValue;
				c.valueSize_ = indexValueSize;
				c.valueType_ = columnType;
			}
		}
		ColumnType valueType = c.valueType_;
		c.opType_ = static_cast<DSExpression::Operation>(op);

		Operator condOp;
		switch (op) {
		case IS:
			condOp = ComparatorTable::isNull_;
			break;
		case ISNOT:
			condOp = ComparatorTable::isNotNull_;
			break;
		default:
			condOp = ComparatorTable::findOperator(
					c.opType_, columnType, valueType, false);
			break;
		}
		if (condOp == NULL) {
			return NULL; 
		}
		c.operator_ = condOp;
	}
	else if (type_ == COLUMN) {
		if (columnType_ != COLUMN_TYPE_BOOL) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_IS_NOT_BOOLEAN,
				"Cannot use non-boolean column as search condition");
		}

		c = TermCondition(COLUMN_TYPE_BOOL, COLUMN_TYPE_BOOL, 
			DSExpression::EQ, columnId_, (QP_NEW bool((notFlag) ? false : true)),
			sizeof(bool));
	}
	else if (type_ == FUNCTION) {
		if (notFlag) {
			return NULL;
		}
		if (arglist_ == NULL) {
			return NULL;
		}
		ExprList &argList = *arglist_;
		switch (mapType) {
		case MAP_TYPE_SPATIAL:
			{
				TqlGisFunc *f = reinterpret_cast<TqlGisFunc *>(functor_);
				const void *r1, *r2;
				GeometryOperator op_g;
				ColumnId indexColumnId;
				f->getIndexParam(txn, argList, op_g, r1, r2, indexColumnId);
				if (r1 == NULL) {
					return NULL;
				} else {
					RtreeMap::SearchContext::GeometryCondition *geomCond = 
						QP_NEW RtreeMap::SearchContext::GeometryCondition();
					geomCond->relation_ = static_cast<uint8_t>(op_g);
					if (geomCond->relation_ == GEOMETRY_QSF_INTERSECT) {
						geomCond->pkey_ = *reinterpret_cast<const TrPv3Key *>(r1);
					}
					else {
						geomCond->rect_[0] = *reinterpret_cast<const TrRectTag *>(r1);
						if (r2) {
							geomCond->rect_[1] = *reinterpret_cast<const TrRectTag *>(r2);
						}
					}
					geomCond->valid_ = true;

					c = TermCondition(COLUMN_TYPE_BOOL, COLUMN_TYPE_BOOL, 
						DSExpression::GEOM_OP, indexColumnId, geomCond,
						sizeof(RtreeMap::SearchContext::GeometryCondition));
				}
			}
			break;
		default:
			return NULL;
		}
	}
	else {
	}
	return (c.operator_ == NULL) ? NULL : QP_NEW TermCondition(c);
}

/*!
 * @brief get usable indices
 */
void Expr::getIndexBitmapAndInfo(
		TransactionContext &txn, BaseContainer &container, Query &queryObj,
		uint32_t &mapBitmap, ColumnInfo *&columnInfo, Operation &detectedOp,
		bool notFlag, bool withPartialMatch) {
#define TOBITMAP(x) (1 << (x))

	ObjectManagerV4 &objectManager = *(container.getObjectManager());
	AllocateStrategy &strategy = container.getRowAllocateStrategy();
	if (type_ == EXPR) {
		assert(arglist_ != NULL);
		for (uint32_t i = 0; i < arglist_->size(); i++) {
			(*arglist_)[i]->getIndexBitmapAndInfo(
					txn, container, queryObj, mapBitmap, columnInfo,
					detectedOp, notFlag, withPartialMatch);
			if (mapBitmap != 0) {
				break;
			}
		}

		Operation op = op_;
		if (notFlag) {
			switch (op) {
			case EQ:
				op = NE;
				break;
			case NE:
				op = EQ;
				break;
			case LT:
				op = GE;
				break;
			case LE:
				op = GT;
				break;
			case GT:
				op = LE;
				break;
			case GE:
				op = LT;
				break;
			case IS:
				op = ISNOT;
				break;
			case ISNOT:
				op = IS;
				break;
			default:
				break;
			}
		}
		switch (op) {
		case EQ:
			mapBitmap &= TOBITMAP(MAP_TYPE_BTREE);
			if (mapBitmap != 0) {
				detectedOp = EQ;
				if (queryObj.doExplain()) {
					Expr *e = this->eval(
						txn, objectManager, strategy, NULL, NULL, EVAL_MODE_PRINT);
					queryObj.addExplain(1, "INDEX_ENABLE", "INDEX_TYPE",
						"BTREE", e->getValueAsString(txn));
					QP_SAFE_DELETE(e);
				}
			}
			break;
		case GE:
		case GT:
		case LE:
		case LT:
			mapBitmap &= TOBITMAP(MAP_TYPE_BTREE);
			if (mapBitmap != 0) {
				detectedOp = op;
				if (queryObj.doExplain()) {
					Expr *e =
						eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_PRINT);
					queryObj.addExplain(1, "INDEX_ENABLE", "INDEX_TYPE",
						"BTREE", e->getValueAsString(txn));
					QP_SAFE_DELETE(e);
				}
			}
			break;
		case IS:
		case ISNOT:
			if ((*arglist_)[0]->isColumn()) {
				mapBitmap &= (TOBITMAP(MAP_TYPE_BTREE) | TOBITMAP(MAP_TYPE_SPATIAL));
				if (mapBitmap != 0) {
					detectedOp = op;
					if (queryObj.doExplain()) {
						Expr *e =
							eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_PRINT);
						queryObj.addExplain(1, "INDEX_ENABLE", "INDEX_TYPE",
							"BTREE", e->getValueAsString(txn));
						QP_SAFE_DELETE(e);
					}
				}
			} else {
				mapBitmap = 0;
			}
			break;
		case NE:
		default:
			mapBitmap = 0;
			break;
		}

		if (mapBitmap != 0) {
			if (arglist_->size() != 2) {
				mapBitmap = 0;
			}
			if ((*arglist_)[0]->isColumn() && (*arglist_)[1]->isColumn()) {
				mapBitmap = 0;
			}
		}
	}
	else if (type_ == FUNCTION) {

		uint8_t usableIndex =
			reinterpret_cast<TqlFunc *>(functor_)->checkUsableIndex();
		if (usableIndex != 0 && notFlag == false && arglist_ != NULL) {
			for (uint32_t i = 0; i < arglist_->size(); i++) {
				(*arglist_)[i]->getIndexBitmapAndInfo(
						txn, container, queryObj, mapBitmap, columnInfo,
						detectedOp, notFlag, withPartialMatch);
				if (mapBitmap != 0) break;
			}
			mapBitmap &= TOBITMAP(usableIndex);
		}
		else {
			if (queryObj.doExplain()) {
				queryObj.addExplain(1, "NOT_INDEX_USABLE", "FUNCTION", label_,
					"BECAUSE_OF_NOT");
			}
		}

		if (mapBitmap != 0) {
			if (mapBitmap & TOBITMAP(MAP_TYPE_SPATIAL)) {
				if (queryObj.doExplain()) {
					queryObj.addExplain(
						1, "INDEX_ENABLE", "INDEX_TYPE", "SPATIAL", label_);
				}
			}
		}
	}
	else if (type_ == COLUMN) {
		columnInfo = this->columnInfo_;
		assert(columnInfo != NULL);
		util::Vector<ColumnId> columnIds(txn.getDefaultAllocator());
		columnIds.push_back(columnInfo->getColumnId());
		const IndexTypes indexBit =
				container.getIndexTypes(txn, columnIds, withPartialMatch);

		if (container.hasIndex(indexBit, MAP_TYPE_BTREE)) {
			mapBitmap |= TOBITMAP(MAP_TYPE_BTREE);
			if (queryObj.doExplain()) {
				queryObj.addExplain(
					1, "INDEX_FOUND", "INDEX_TYPE", "BTREE", label_);
			}
		}
		if (container.hasIndex(indexBit, MAP_TYPE_SPATIAL)) {
			mapBitmap |= TOBITMAP(MAP_TYPE_SPATIAL);
			if (queryObj.doExplain()) {
				queryObj.addExplain(
					1, "INDEX_FOUND", "INDEX_TYPE", "SPATIAL", label_);
			}
		}
	}
	else {
	}

	if (columnInfo != NULL && mapBitmap == 0) {
		if (queryObj.doExplain()) {
			const char *columnName =
				columnInfo->getColumnName(txn, objectManager, container.getMetaAllocateStrategy());
			if (columnInfo->isKey()) {
				queryObj.addExplain(
					1, "INDEX_FOUND", "INDEX_TYPE", "ROWKEY", columnName);
			}
			else {
				queryObj.addExplain(
					1, "NOT_INDEX_USABLE", "COLUMN", columnName, "");
			}
		}
		columnInfo = NULL;
	}
}

/*!
 * @brief Create ID or Column Node
 *
 * @param[in] idName Token of column name
 * @param[in] txn TransactionContext
 * @param[in] parseState Parser state
 * @param[in] collection (can be null)
 * @param[in] timeSeries (can be null)
 * @param[in] map pointer of function
 * @note assert(collection != NULL || timeSeries != NULL)
 *
 * @return leaf as expression
 */
Expr *Expr::newColumnOrIdNode(Token &idName, TransactionContext &txn,
	unsigned int parseState, Collection *collection, TimeSeries *timeSeries,
	SpecialIdMap *map) {
	char buf[MAX_COLUMN_NAME_LEN];
	char *cName = buf;
	int i = 0;
	Expr *ret;

	if (idName.n > MAX_COLUMN_NAME_LEN) {
		cName = static_cast<char *>(
			txn.getDefaultAllocator().allocate(idName.n + 1));
	}

	for (i = 0; i < idName.n; i++) {
		cName[i] = Lexer::ToUpper(idName.z[i]);  
	}
	cName[i] = '\0';

	TqlSpecialId *f = static_cast<TqlSpecialId *>(
		map->findFunction(cName, txn.getDefaultAllocator()));

	memcpy(cName, idName.z, idName.n);
	cName[idName.n] = '\0';

	if (f != NULL) {
		try {
			ret = newColumnNode(cName, txn, parseState, collection, timeSeries);
			ret->functor_ = f;
		}
		catch (util::Exception &) {
			ret = newColumnNode(cName, txn, parseState, NULL, NULL);
		}
	}
	else {
		ret = newColumnNode(cName, txn, parseState, collection, timeSeries);
	}

	return ret;
}

/*!
 * Create column leaf of syntax tree.
 *
 * @param[in] colName Token of column name
 * @param[in] txn TransactionContext
 * @param[in] parseState Parser state
 * @param[in] collection (can be null)
 * @param[in] timeSeries (can be null)
 * @note assert(collection != NULL || timeSeries != NULL)
 *
 * @return leaf as expression
 */
Expr *Expr::newColumnNode(Token &colName, TransactionContext &txn,
	unsigned int parseState, Collection *collection, TimeSeries *timeSeries) {
	char buf[MAX_COLUMN_NAME_LEN];
	char *cName = buf;
	Expr *ret;

	if (colName.z[0] == '*') {
		if (colName.z[1] == '\0') {
			return newColumnNode("*", txn, parseState, collection, timeSeries);
		}
	}

	if (colName.n > MAX_COLUMN_NAME_LEN) {
		cName = static_cast<char *>(
			txn.getDefaultAllocator().allocate(colName.n + 1));
	}

	memcpy(cName, colName.z, colName.n);
	cName[colName.n] = '\0';

	ret = newColumnNode(cName, txn, parseState, collection, timeSeries);

	return ret;
}

/*!
 * Create column leaf of syntax tree.
 *
 * @param[in] upperName Token of column name
 * @param[in] txn TransactionContext
 * @param[in] parseState Parser state
 * @param[in] collection (can be null)
 * @param[in] timeSeries (can be null)
 * @note assert(collection != NULL || timeSeries != NULL)
 *
 * @return leaf as expression
 */
Expr *Expr::newColumnNode(const char *upperName, TransactionContext &txn,
	unsigned int parseState, Collection *collection, TimeSeries *timeSeries) {
	ColumnInfo *cInfo = NULL;
	uint32_t columnId = UNDEF_COLUMNID;
	Query::ParseState state = Query::ParseState(parseState);
	char *tmpCName;
	Expr *ret;

	bool isQuoted;
	tmpCName = dequote(QP_ALLOCATOR, upperName, isQuoted);

	if (tmpCName[0] == '*' && tmpCName[1] == '\0') {
		if (state == Query::PARSESTATE_CONDITION) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_COLUMN_ABUSE, "Cannot take * as search condition");
		}
	}
	else {
		MetaContainer *metaContainer =
				Query::getMetaContainer(collection, timeSeries);
		if (metaContainer != NULL) {
			ObjectManagerV4 &objectManager = *(metaContainer->getObjectManager());
			metaContainer->getColumnInfo(
				txn, objectManager, tmpCName, columnId, cInfo, isQuoted);
		}
		else if (collection != NULL) {
			ObjectManagerV4 &objectManager = *(collection->getObjectManager());
			collection->getColumnInfo(
				txn, objectManager, collection->getMetaAllocateStrategy(), tmpCName, columnId, cInfo, isQuoted);
		}
		else if (timeSeries != NULL) {
			ObjectManagerV4 &objectManager = *(timeSeries->getObjectManager());
			timeSeries->getColumnInfo(
				txn, objectManager, timeSeries->getMetaAllocateStrategy(), tmpCName, columnId, cInfo, isQuoted);
		}
		else {
			/* DO NOTHING */
		}

		if ((cInfo == NULL || columnId == UNDEF_COLUMNID) &&
			(collection != NULL || timeSeries != NULL)) {
			util::String str("No such column ", QP_ALLOCATOR);
			str += tmpCName;
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_NOT_FOUND, str.c_str());
		}
	}

	ret = QP_NEW Expr(tmpCName, txn, columnId, cInfo);

	return ret;
}

/*!
 * @brief Create function node of syntax tree.
 *
 * @param[in] fnName Token of function name
 * @param[in] args Arguments
 * @param[in] txn TransactionContext
 * @param[in] fmap function map.
 *
 * @return selection as expression
 */
Expr *Expr::newFunctionNode(
	Token &fnName, ExprList *args, TransactionContext &txn, FunctionMap &fmap) {
	char buf[MAX_COLUMN_NAME_LEN];
	int i = 0;
	char *tmpCName;

	tmpCName = const_cast<char *>(fnName.z);

	if (fnName.n >= MAX_COLUMN_NAME_LEN) {
		util::String fName =
			util::String(fnName.z, fnName.n, QP_ALLOCATOR);  
		GS_THROW_USER_ERROR(GS_ERROR_TQ_FUNCTION_NOT_FOUND,
			util::String("No such function : ", QP_ALLOCATOR) + fName);
	}

	for (i = 0; i < fnName.n; i++) {
		buf[i] = Lexer::ToUpper(tmpCName[i]);
	}
	buf[i] = '\0';

	TqlFunc *f = fmap.findFunction(buf, txn.getDefaultAllocator());
	if (f != NULL) {
		return QP_NEW Expr(buf, f, args, txn);
	}

	GS_THROW_USER_ERROR(GS_ERROR_TQ_FUNCTION_NOT_FOUND,
		util::String("No such function : ", QP_ALLOCATOR) + buf);
}

/*!
 * @brief Create selection node of syntax tree.
 *
 * @param[in] fnName Token of function name
 * @param[in] args Arguments
 * @param[in] txn TransactionContext
 * @param[in] fmap FunctionMap for calculation function
 * @param[in] amap FunctionMap for aggregation function
 * @param[in] smap FunctionMap for selection function
 *
 * @return selection as expression
 */
Expr *Expr::newSelectionNode(Token &fnName, ExprList *args,
	TransactionContext &txn, FunctionMap *fmap, AggregationMap *amap,
	SelectionMap *smap) {
	char buf[MAX_COLUMN_NAME_LEN];
	int i = 0;
	char *tmpCName;

	tmpCName = const_cast<char *>(fnName.z);

	if (fnName.n >= MAX_COLUMN_NAME_LEN) {
		util::String fName =
			util::String(fnName.z, fnName.n, QP_ALLOCATOR);  
		GS_THROW_USER_ERROR(GS_ERROR_TQ_FUNCTION_NOT_FOUND,
			util::String("No such function : ", QP_ALLOCATOR) + fName);
	}

	for (i = 0; i < fnName.n; i++) {
		buf[i] = Lexer::ToUpper(tmpCName[i]);
	}
	buf[i] = '\0';

	TqlFunc *f = fmap->findFunction(buf, txn.getDefaultAllocator());
	if (f != NULL) {
		return QP_NEW Expr(buf, f, args, txn);
	}

	TqlAggregation *agg = amap->findFunction(buf, txn.getDefaultAllocator());
	if (agg != NULL) {
		return QP_NEW Expr(buf, agg, args, txn);
	}

	TqlSelection *sel = smap->findFunction(buf, txn.getDefaultAllocator());
	if (sel != NULL) {
		return QP_NEW Expr(buf, sel, args, txn);
	}
	GS_THROW_USER_ERROR(GS_ERROR_TQ_FUNCTION_NOT_FOUND,
		util::String("No such function : ", QP_ALLOCATOR) + buf);
}

char *Expr::dequote(util::StackAllocator &alloc, const char *str, bool &isQuoted) {
	if (str[0] == '"') {
		isQuoted = true;
		size_t len = strlen(str);
		if (str[len - 1] != '"') {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_SYNTAX_ERROR_CANNOT_DEQUOTE, "Cannot dequote");
		}

		size_t newlen = len - 2;
		char *dequotedStr =
			reinterpret_cast<char *>(alloc.allocate(newlen + 1));
		memcpy(dequotedStr, str + 1, newlen);
		dequotedStr[newlen] = '\0';

		return dequotedStr;
	}
	else {
		isQuoted = false;
		return const_cast<char *>(str);
	}
}

util::DateTime::ZonedOption Expr::newZonedTimeOption(
		const util::DateTime::FieldType *precision,
		const util::TimeZone &zone) {
	UTIL_STATIC_ASSERT(!TRIM_MILLISECONDS);

	util::DateTime::ZonedOption option =
			util::DateTime::ZonedOption::create(TRIM_MILLISECONDS, zone);

	if (precision == NULL || isTimeRangeValidationStrict(*precision)) {
		option.baseOption_.maxTimeMillis_ =
				ValueProcessor::makeMaxTimestamp(TRIM_MILLISECONDS);
	}

	return option;
}

bool Expr::isTimeRangeValidationStrict(util::DateTime::FieldType precision) {
	switch (precision) {
	case util::DateTime::FIELD_MICROSECOND:
		return true;
	case util::DateTime::FIELD_NANOSECOND:
		return true;
	default:
		return false;
	}
}

bool Expr::aggregate(TransactionContext &txn, Collection &collection,
	util::XArray<OId> &resultRowIdList, Value &result) {
	assert(this->type_ == AGGREGATION);
	if (arglist_ == NULL || arglist_->size() != 1) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for aggregation");
	}
	else if (!(*arglist_)[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid argument type for aggregation");
	}

	uint32_t aggColumnId = (*arglist_)[0]->columnId_;
	ColumnInfo *aggColumnInfo = (*arglist_)[0]->columnInfo_;
	ColumnType aggColumnType = COLUMN_TYPE_WITH_BEGIN;

	TqlAggregation *agg = reinterpret_cast<TqlAggregation *>(functor_);
	if (aggColumnId != UNDEF_COLUMNID) {
		aggColumnType = (*arglist_)[0]->columnInfo_->getColumnType();
		if (!agg->isAcceptable(aggColumnType)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
				"Invalid column for aggregation " << (int) aggColumnType);
		}
	}
	else {
		if (label_ == NULL || strcmp(label_, "COUNT") != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
				"Invalid column for aggregation");
		}
	}

	if (aggColumnId == UNDEF_COLUMNID) {
		result.set((int64_t)resultRowIdList.size());
		return true;
	}
	else {
		ObjectManagerV4 &objectManager = *(collection.getObjectManager());
		agg->reset(aggColumnType);

		BaseContainer::RowArray rowArray(txn, &collection);  

		for (uint32_t i = 0; i < resultRowIdList.size(); i++) {
			rowArray.load(txn, resultRowIdList[i], &collection,
				OBJECT_READ_ONLY);  
			BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
			ContainerValue v(objectManager, collection.getRowAllocateStrategy());
			row.getField(txn, *aggColumnInfo, v);
			if (!v.getValue().isNullValue()) {
				agg->putValue(txn, v.getValue());
			}
		}

		return agg->getResult(txn, result);
	}
}

bool Expr::aggregate(TransactionContext &txn, TimeSeries &timeSeries,
	util::XArray<PointRowId> &resultRowIdList, Value &result) {

	assert(this->type_ == AGGREGATION);
	if (arglist_ == NULL || arglist_->size() != 1) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for aggregation");
	}
	else if (!(*arglist_)[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid argument type for aggregation");
	}

	uint32_t aggColumnId = (*arglist_)[0]->columnId_;
	ColumnType aggColumnType = COLUMN_TYPE_WITH_BEGIN;
	TqlAggregation *agg = reinterpret_cast<TqlAggregation *>(functor_);

	if (aggColumnId != UNDEF_COLUMNID) {
		aggColumnType = (*arglist_)[0]->columnInfo_->getColumnType();
		if (!agg->isAcceptable(aggColumnType)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
				"Invalid column for aggregation");
		}
	}

	agg->reset(aggColumnType);

	if (aggColumnId == UNDEF_COLUMNID) {
		if (label_ == NULL || strcmp(label_, "COUNT") != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
				"Invalid column for aggregation");
		}
		result.set((int64_t)resultRowIdList.size());
		return true;
	}
	else {
		ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());

		BaseContainer::RowArray rowArray(txn, &timeSeries);		  
		ColumnInfo &keyColumnInfo = timeSeries.getColumnInfo(0);  
		ColumnInfo &aggColumnInfo = timeSeries.getColumnInfo(aggColumnId);  
		for (uint32_t i = 0; i < resultRowIdList.size(); i++) {
			ContainerValue k(objectManager, timeSeries.getRowAllocateStrategy());
			ContainerValue v(objectManager, timeSeries.getRowAllocateStrategy());
			rowArray.load(txn, resultRowIdList[i], &timeSeries,
				OBJECT_READ_ONLY);  
			BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
			row.getField(txn, keyColumnInfo, k);
			row.getField(txn, aggColumnInfo, v);
			if (!v.getValue().isNullValue()) {
				agg->putValue(txn, k.getValue(), v.getValue());
			}
		}

		return agg->getResult(txn, result);
	}
}

void Expr::aggregate(TransactionContext &txn, TimeSeries &timeSeries,
	BtreeMap::SearchContext &sc, ResultSize &resultNum, Value &value) {
	assert(this->type_ == AGGREGATION);
	if (arglist_ == NULL || arglist_->size() != 1) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for aggregation");
	}
	else if (!(*arglist_)[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid argument type for aggregation");
	}

	uint32_t aggColumnId = (*arglist_)[0]->columnId_;
	ColumnType aggColumnType = COLUMN_TYPE_WITH_BEGIN;
	AggregationType aggType;

	TqlAggregation *agg = reinterpret_cast<TqlAggregation *>(functor_);
	if (aggColumnId != UNDEF_COLUMNID) {
		aggColumnType = (*arglist_)[0]->columnInfo_->getColumnType();
		if (!agg->isAcceptable(aggColumnType)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
				"Invalid column for aggregation");
		}
	}
	else {
		if (label_ == NULL || strcmp(label_, "COUNT") != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
				"Invalid column for aggregation");
		}
	}
	aggType = agg->apiAggregationType();

	timeSeries.aggregate(
		txn, sc, aggColumnId, aggType, resultNum, value);
}

void Expr::select(TransactionContext &txn, Collection &collection,
	util::XArray<PointRowId> &resultRowIdList, bool isSorted,
	OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
	uint64_t offset, FunctionMap &function_map, uint64_t &resultNum,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	assert(this->type_ == SELECTION);
	TqlSelection *sel = reinterpret_cast<TqlSelection *>(functor_);
	resultNum = (*sel)(txn, collection, resultRowIdList, isSorted,
		apiOutputOrder, orderByExpr, limit, offset, function_map, *(arglist_),
		messageRowStore, resultType);
}

void Expr::select(TransactionContext &txn, TimeSeries &timeSeries,
	util::XArray<PointRowId> &resultRowIdList, bool isSorted,
	OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
	uint64_t offset, FunctionMap &function_map, uint64_t &resultNum,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	assert(this->type_ == SELECTION);
	TqlSelection *sel = reinterpret_cast<TqlSelection *>(functor_);
	resultNum = (*sel)(txn, timeSeries, resultRowIdList, isSorted,
		apiOutputOrder, orderByExpr, limit, offset, function_map, *(arglist_),
		messageRowStore, resultType);
}

void Expr::select(TransactionContext &txn, TimeSeries &timeSeries,
	BtreeMap::SearchContext &sc, OutputOrder apiOutputOrder, uint64_t limit,
	uint64_t offset, uint64_t &resultNum,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	assert(this->type_ == SELECTION);
	TqlSelection *sel = reinterpret_cast<TqlSelection *>(functor_);
	assert(sel->getPassMode() != QP_PASSMODE_NO_PASS);
	resultNum = sel->apiPassThrough(txn, timeSeries, sc, apiOutputOrder, limit,
		offset, (*arglist_), messageRowStore, resultType);
}

/*!
 * @brief Check the selection expr requires special operation
 *
 * @return
 */
QpPassMode Expr::getPassMode(TransactionContext &txn) {
	if (this->type_ != SELECTION && this->type_ != AGGREGATION &&
		this->type_ != EXPR) {
		return QP_PASSMODE_NO_PASS;
	}
	if (this->type_ == SELECTION) {
		if (strcmp("*", getValueAsString(txn)) == 0) {
			return QP_PASSMODE_NO_PASS;
		}
		TqlSelection *sel = reinterpret_cast<TqlSelection *>(functor_);
		return sel->getPassMode();
	}
	else if (this->type_ == AGGREGATION) {
		TqlAggregation *agg = reinterpret_cast<TqlAggregation *>(functor_);
		return agg->getPassMode();
	}
	else {
		if ((*arglist_)[0]->type_ == SELECTION ||
			(*arglist_)[0]->type_ == AGGREGATION) {
			return (*arglist_)[0]->getPassMode(txn);
		}
		else
			return QP_PASSMODE_NO_PASS;
	}
}

void Expr::unescape(util::String &str, char escape) {
	std::string::size_type pos = 0;
	char buf[3], buf2[2];
	buf[0] = buf[1] = escape;
	buf[2] = '\0';
	buf2[0] = escape;
	buf2[1] = '\0';
	while (pos = str.find(buf, pos), pos != std::string::npos) {
		str.replace(pos, 2, buf2);
		pos++;
	}
}

SortExpr *SortExpr::dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	SortExpr *dest = QP_NEW SortExpr;
	dest->order = order;
	dest->nullsLast = nullsLast;
	dest->expr = expr->dup(txn, objectManager, strategy);
	return dest;
}
