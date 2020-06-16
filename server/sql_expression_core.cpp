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

#include "sql_expression_core.h"
#include "query_function.h"


const SQLExprs::ExprRegistrar
SQLCoreExprs::CustomRegistrar::REGISTRAR_INSTANCE((CustomRegistrar()));

void SQLCoreExprs::CustomRegistrar::operator()() const {
	addNonEvaluable<SQLType::EXPR_SELECTION>();
	addNonEvaluable<SQLType::EXPR_PRODUCTION>();
	addNonEvaluable<SQLType::EXPR_PROJECTION>();
	add<SQLType::EXPR_ID, IdExpression>();
	add<SQLType::EXPR_CONSTANT, ConstantExpression>();
	add<SQLType::EXPR_COLUMN, TupleColumnExpression>();
	add<SQLType::EXPR_TYPE, SQLExprs::PlanningExpression>();
	addNoArgs<SQLType::EXPR_AND, AndExpression>();
	addNoArgs<SQLType::EXPR_OR, OrExpression>();
	addNoArgs<SQLType::EXPR_CASE, CaseExpression>();
	add<SQLType::OP_CAST, CastExpression>();
	addNonEvaluable<SQLType::EXPR_PLACEHOLDER>();
	addNonEvaluable<SQLType::EXPR_LIST>();
	addNonEvaluable<SQLType::EXPR_AGG_FOLLOWING>();
	addNoArgs<SQLType::EXPR_HINT_NO_INDEX, NoopExpression>();
}


const SQLExprs::ExprRegistrar
SQLCoreExprs::BasicRegistrar::REGISTRAR_INSTANCE((BasicRegistrar()));

void SQLCoreExprs::BasicRegistrar::operator()() const {
	add<SQLType::FUNC_COALESCE, Functions::Coalesce>();
	add<SQLType::FUNC_IFNULL, Functions::Coalesce>();
	add<SQLType::FUNC_MAX, Functions::Max>();
	add<SQLType::FUNC_MIN, Functions::Min>();
	add<SQLType::FUNC_NULLIF, Functions::NullIf>();
	add<SQLType::FUNC_TYPEOF, Functions::TypeOf>();

	add<SQLType::OP_CONCAT, Functions::Concat>();
	add<SQLType::OP_MULTIPLY, Functions::Multiply>();
	add<SQLType::OP_DIVIDE, Functions::Divide>();
	add<SQLType::OP_REMAINDER, Functions::Remainder>();
	add<SQLType::OP_ADD, Functions::Add>();
	add<SQLType::OP_SUBTRACT, Functions::Subtract>();
	add<SQLType::OP_SHIFT_LEFT, Functions::ShiftLeft>();
	add<SQLType::OP_SHIFT_RIGHT, Functions::ShiftRight>();
	add<SQLType::OP_BIT_AND, Functions::BitAnd>();
	add<SQLType::OP_BIT_OR, Functions::BitOr>();
	add<SQLType::OP_LT, Functions::Lt>();
	add<SQLType::OP_LE, Functions::Le>();
	add<SQLType::OP_GT, Functions::Gt>();
	add<SQLType::OP_GE, Functions::Ge>();
	add<SQLType::OP_EQ, Functions::Eq>();
	add<SQLType::OP_NE, Functions::Ne>();
	add<SQLType::OP_IS, Functions::Is>();
	add<SQLType::OP_IS_NOT, Functions::IsNot>();
	add<SQLType::OP_IS_NULL, Functions::IsNull>();
	add<SQLType::OP_IS_NOT_NULL, Functions::IsNotNull>();
	add<SQLType::OP_BIT_NOT, Functions::BitNot>();
	add<SQLType::OP_NOT, Functions::Not>();
}


SQLCoreExprs::IdExpression::IdExpression(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(cxt);
	static_cast<void>(code);
}

TupleValue SQLCoreExprs::IdExpression::eval(ExprContext &cxt) const {
	return TupleValue(cxt.updateTupleId(getCode().getInput()));
}


SQLCoreExprs::ConstantExpression::ConstantExpression(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(cxt);
	static_cast<void>(code);
}

TupleValue SQLCoreExprs::ConstantExpression::eval(ExprContext &cxt) const {
	static_cast<void>(cxt);
	return getCode().getValue();
}


SQLCoreExprs::TupleColumnExpression::TupleColumnExpression(
		ExprFactoryContext &cxt, const ExprCode &code) :
		unified_((code.getAttributes() & ExprCode::ATTR_COLUMN_UNIFIED) != 0) {
	static_cast<void>(cxt);
}

TupleValue SQLCoreExprs::TupleColumnExpression::eval(ExprContext &cxt) const {
	const ExprCode &code = getCode();

	SQLValues::ArrayTuple *tuple = cxt.getArrayTuple(0);
	if (tuple != NULL) {
		return asColumnValue(SQLValues::ValueUtils::duplicateValue(
				cxt, tuple->get(code.getColumnPos())));
	}

	uint32_t input;
	if (unified_) {
		input = cxt.getActiveInput();
	}
	else {
		input = code.getInput();
	}

	return asColumnValue(cxt.getReadableTuple(input).get(
			cxt.getReaderColumn(input, code.getColumnPos())));
}


TupleValue SQLCoreExprs::AndExpression::eval(ExprContext &cxt) const {
	const Expression &expr = child();

	const TupleValue &value1 = expr.eval(cxt);
	if (SQLValues::ValueUtils::isFalse(value1)) {
		return SQLValues::ValueUtils::toAnyByBool(false);
	}

	const TupleValue &value2 = expr.next().eval(cxt);
	if (SQLValues::ValueUtils::isFalse(value2)) {
		return SQLValues::ValueUtils::toAnyByBool(false);
	}

	if (SQLValues::ValueUtils::isNull(value1) ||
			SQLValues::ValueUtils::isNull(value2)) {
		return TupleValue();
	}

	return SQLValues::ValueUtils::toAnyByBool(true);
}


TupleValue SQLCoreExprs::OrExpression::eval(ExprContext &cxt) const {
	const Expression &expr = child();

	const TupleValue &value1 = expr.eval(cxt);
	if (SQLValues::ValueUtils::isTrue(value1)) {
		return SQLValues::ValueUtils::toAnyByBool(true);
	}

	const TupleValue &value2 = expr.next().eval(cxt);
	if (SQLValues::ValueUtils::isTrue(value2)) {
		return SQLValues::ValueUtils::toAnyByBool(true);
	}

	if (SQLValues::ValueUtils::isNull(value1) ||
			SQLValues::ValueUtils::isNull(value2)) {
		return TupleValue();
	}

	return SQLValues::ValueUtils::toAnyByBool(false);
}


TupleValue SQLCoreExprs::CaseExpression::eval(ExprContext &cxt) const {
	const Expression *expr = &child();
	do {
		const Expression *thenExpr = expr->findNext();
		if (thenExpr == NULL) {
			return asColumnValue(expr->eval(cxt));
		}

		const TupleValue &condValue = expr->eval(cxt);
		if (!SQLValues::ValueUtils::isNull(condValue) &&
				SQLValues::ValueUtils::toBool(condValue)) {
			return asColumnValue(thenExpr->eval(cxt));
		}

		expr = thenExpr->findNext();
	}
	while (expr != NULL);

	return TupleValue();
}


SQLCoreExprs::CastExpression::CastExpression(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(cxt);
	static_cast<void>(code);
}

TupleValue SQLCoreExprs::CastExpression::eval(ExprContext &cxt) const {
	return SQLValues::ValueConverter(getCode().getColumnType())(
			cxt, child().eval(cxt));
}


TupleValue SQLCoreExprs::NoopExpression::eval(ExprContext &cxt) const {
	return child().eval(cxt);
}


template<typename C>
inline TupleValue SQLCoreExprs::Functions::Coalesce::operator()(C &cxt) {
	static_cast<void>(cxt);
	return TupleValue();
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Coalesce::operator()(
		C &cxt, const TupleValue &value) {
	if (SQLValues::ValueUtils::isNull(value)) {
		return TupleValue();
	}

	cxt.finishFunction();
	return value;
}


template<typename C>
inline TupleValue SQLCoreExprs::Functions::Max::operator()(C &cxt) {
	static_cast<void>(cxt);
	return result_;
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Max::operator()(
		C &cxt, const TupleValue &value) {
	if (SQLValues::ValueUtils::isNull(value)) {
		result_ = TupleValue();
		cxt.finishFunction();
	}
	else if (SQLValues::ValueGreater(
			SQLValues::ValueUtils::toCompType(value, result_))(
			value, result_)) {
		result_ = value;
	}
	return TupleValue();
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Max::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	result_ = value1;
	return (*this)(cxt, value2);
}


template<typename C>
inline TupleValue SQLCoreExprs::Functions::Min::operator()(C &cxt) {
	static_cast<void>(cxt);
	return result_;
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Min::operator()(
		C &cxt, const TupleValue &value) {
	if (SQLValues::ValueUtils::isNull(value)) {
		result_ = TupleValue();
		cxt.finishFunction();
	}
	else if (SQLValues::ValueLess(
			SQLValues::ValueUtils::toCompType(value, result_))(
			value, result_)) {
		result_ = value;
	}
	return TupleValue();
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Min::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	result_ = value1;
	return (*this)(cxt, value2);
}


template<typename C>
inline TupleValue SQLCoreExprs::Functions::NullIf::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	if (SQLValues::ValueEq(
			SQLValues::ValueUtils::toCompType(value1, value2))(
			value1, value2)) {
		return TupleValue();
	}
	else {
		return value1;
	}
}


template<typename C>
inline typename C::WriterType& SQLCoreExprs::Functions::TypeOf::operator()(
		C &cxt, const TupleValue &value) {
	typename C::WriterType &writer = cxt.getResultWriter();

	const bool nullOnUnknown = true;
	const char8_t *typeStr =
			SQLValues::TypeUtils::toString(value.getType(), nullOnUnknown);

	if (typeStr == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	writer.append(typeStr, strlen(typeStr));
	return writer;
}


template<typename C>
typename C::WriterType& SQLCoreExprs::Functions::Concat::operator()(C &cxt) {
	return cxt.getResultWriter();
}

template<typename C>
typename C::WriterType& SQLCoreExprs::Functions::Concat::operator()(
		C &cxt, const TupleValue &value) {
	typename C::WriterType &writer = cxt.getResultWriter();
	SQLValues::ValueUtils::formatValue(cxt.getBase(), writer, value);
	return writer;
}

template<typename C>
typename C::WriterType& SQLCoreExprs::Functions::Concat::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	typename C::WriterType &writer = cxt.getResultWriter();
	SQLValues::ValueUtils::formatValue(cxt.getBase(), writer, value1);
	SQLValues::ValueUtils::formatValue(cxt.getBase(), writer, value2);
	return writer;
}


template<typename C, typename T>
T SQLCoreExprs::Functions::Multiply::operator()(
		C &cxt, const T &value1, const T &value2) {
	static_cast<void>(cxt);
	return FunctionUtils::NumberArithmetic::multiply(value1, value2);
}


template<typename C, typename T>
T SQLCoreExprs::Functions::Divide::operator()(
		C &cxt, const T &value1, const T &value2) {
	static_cast<void>(cxt);
	return FunctionUtils::NumberArithmetic::divide(value1, value2);
}


template<typename C, typename T>
T SQLCoreExprs::Functions::Remainder::operator()(
		C &cxt, const T &value1, const T &value2) {
	static_cast<void>(cxt);
	return FunctionUtils::NumberArithmetic::remainder(value1, value2);
}


template<typename C, typename T>
T SQLCoreExprs::Functions::Add::operator()(
		C &cxt, const T &value1, const T &value2) {
	static_cast<void>(cxt);
	return FunctionUtils::NumberArithmetic::add(value1, value2);
}


template<typename C, typename T>
T SQLCoreExprs::Functions::Subtract::operator()(
		C &cxt, const T &value1, const T &value2) {
	static_cast<void>(cxt);
	return FunctionUtils::NumberArithmetic::subtract(value1, value2);
}


template<typename Left>
template<typename C>
int64_t SQLCoreExprs::Functions::Shift<Left>::operator()(
		C &cxt, int64_t value1, int64_t value2) {
	static_cast<void>(cxt);

	bool left;
	uint64_t shift;

	if (value2 == 0) {
		return value1;
	}
	else if (value2 < 0) {
		left = (!Left::VALUE);
		shift = static_cast<uint64_t>(-std::max<int64_t>(-64, value2));
	}
	else {
		left = (Left::VALUE);
		shift = static_cast<uint64_t>(std::min<int64_t>(64, value2));
	}

	if (shift >= 64) {
		if (left || value1 >= 0) {
			return 0;
		}
		else {
			return -1;
		}
	}

	union {
		int64_t asSigned_;
		int64_t asUnigned_;
	} value, bits;
	value.asSigned_ = value1;

	if (left) {
		bits.asUnigned_ = value.asUnigned_ << shift;
	}
	else {
		bits.asUnigned_ = value.asUnigned_ >> shift;
		if (value1 < 0) {
			bits.asUnigned_ |= ~static_cast<uint64_t>(0) << (64 - shift);
		}
	}

	return bits.asSigned_;
}


template<typename C>
int64_t SQLCoreExprs::Functions::BitAnd::operator()(
		C &cxt, int64_t value1, int64_t value2) {
	static_cast<void>(cxt);
	return value1 & value2;
}


template<typename C>
int64_t SQLCoreExprs::Functions::BitOr::operator()(
		C &cxt, int64_t value1, int64_t value2) {
	static_cast<void>(cxt);
	return value1 | value2;
}


template<typename C>
bool SQLCoreExprs::Functions::Lt::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	const bool sensitive = false;
	return SQLValues::ValueComparator(
			SQLValues::ValueUtils::toCompType(value1, value2), sensitive)(
			value1, value2) < 0;
}


template<typename C>
bool SQLCoreExprs::Functions::Le::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	const bool sensitive = false;
	return SQLValues::ValueComparator(
			SQLValues::ValueUtils::toCompType(value1, value2), sensitive)(
			value1, value2) <= 0;
}


template<typename C>
bool SQLCoreExprs::Functions::Gt::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	const bool sensitive = false;
	return SQLValues::ValueComparator(
			SQLValues::ValueUtils::toCompType(value1, value2), sensitive)(
			value1, value2) > 0;
}


template<typename C>
bool SQLCoreExprs::Functions::Ge::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	const bool sensitive = false;
	return SQLValues::ValueComparator(
			SQLValues::ValueUtils::toCompType(value1, value2), sensitive)(
			value1, value2) >= 0;
}


template<typename C>
bool SQLCoreExprs::Functions::Eq::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	const bool sensitive = false;
	return SQLValues::ValueComparator(
			SQLValues::ValueUtils::toCompType(value1, value2), sensitive)(
			value1, value2) == 0;
}


template<typename C>
bool SQLCoreExprs::Functions::Ne::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	static_cast<void>(cxt);
	const bool sensitive = false;
	return SQLValues::ValueComparator(
			SQLValues::ValueUtils::toCompType(value1, value2), sensitive)(
			value1, value2) != 0;
}


template<typename C>
bool SQLCoreExprs::Functions::Is::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	return Eq()(cxt, value1, value2);
}


template<typename C>
bool SQLCoreExprs::Functions::IsNot::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	return Ne()(cxt, value1, value2);
}


template<typename C>
bool SQLCoreExprs::Functions::IsNull::operator()(
		C &cxt, const TupleValue &value) {
	static_cast<void>(cxt);
	return SQLValues::ValueUtils::isNull(value);
}


template<typename C>
bool SQLCoreExprs::Functions::IsNotNull::operator()(
		C &cxt, const TupleValue &value) {
	static_cast<void>(cxt);
	return !SQLValues::ValueUtils::isNull(value);
}


template<typename C>
int64_t SQLCoreExprs::Functions::BitNot::operator()(C &cxt, int64_t value) {
	static_cast<void>(cxt);
	return ~value;
}


template<typename C>
bool SQLCoreExprs::Functions::Not::operator()(C &cxt, bool value) {
	static_cast<void>(cxt);
	return !value;
}
