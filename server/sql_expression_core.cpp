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
	addVariants<SQLType::EXPR_ID, IdExpression>();
	add<SQLType::EXPR_CONSTANT, ConstantExpression>();
	addVariants<SQLType::EXPR_COLUMN, ColumnExpression>();
	addVariants< SQLType::OP_LT, ComparisonExpression<Functions::Lt> >();
	addVariants< SQLType::OP_LE, ComparisonExpression<Functions::Le> >();
	addVariants< SQLType::OP_GT, ComparisonExpression<Functions::Gt> >();
	addVariants< SQLType::OP_GE, ComparisonExpression<Functions::Ge> >();
	addVariants< SQLType::OP_EQ, ComparisonExpression<Functions::Eq> >();
	addVariants< SQLType::OP_NE, ComparisonExpression<Functions::Ne> >();
	add<SQLType::EXPR_TYPE, SQLExprs::PlanningExpression>();
	addNoArgs<SQLType::EXPR_AND, AndExpression>();
	addNoArgs<SQLType::EXPR_OR, OrExpression>();
	addNoArgs<SQLType::EXPR_CASE, CaseExpression>();
	addVariants<SQLType::EXPR_IN, InExpression>();
	add<SQLType::OP_CAST, CastExpression>();
	addNonEvaluable<SQLType::EXPR_PLACEHOLDER>();
	add<SQLType::EXPR_LIST, SQLExprs::PlanningExpression>();
	addNonEvaluable<SQLType::EXPR_AGG_FOLLOWING>();
	addNoArgs<SQLType::EXPR_HINT_NO_INDEX, NoopExpression>();

	add<SQLType::EXPR_WINDOW_OPTION, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_GROUP, SQLExprs::PlanningExpression>();
	addNoArgs<SQLType::EXPR_RANGE_GROUP_ID, RangeGroupIdExpression>();
	add<SQLType::EXPR_RANGE_KEY, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_KEY_CURRENT, RangeKeyExpression>();
	add<SQLType::EXPR_RANGE_AGG, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_FILL, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_FILL_NONE, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_FILL_NULL, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_FILL_PREV, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_RANGE_FILL_LINEAR, SQLExprs::PlanningExpression>();
	add<SQLType::EXPR_LINEAR, LinearExpression>();
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
	add<SQLType::OP_IS, Functions::Is>();
	add<SQLType::OP_IS_NOT, Functions::IsNot>();
	add<SQLType::OP_IS_NULL, Functions::IsNull>();
	add<SQLType::OP_IS_NOT_NULL, Functions::IsNotNull>();
	add<SQLType::OP_BIT_NOT, Functions::BitNot>();
	add<SQLType::OP_NOT, Functions::Not>();
}


size_t SQLCoreExprs::VariantUtils::toCompTypeNumber(TupleColumnType src) {
	typedef SQLValues::TypeUtils TypeUtils;
	const TupleColumnType nonNullType = TypeUtils::toNonNullable(src);
	if (TypeUtils::isIntegral(nonNullType) ||
			nonNullType == TupleTypes::TYPE_TIMESTAMP ||
			nonNullType == TupleTypes::TYPE_BOOL) {
		return 0;
	}
	else if (TypeUtils::isFloating(nonNullType)) {
		return 1;
	}

	switch (TypeUtils::toNonNullable(src)) {
	case TupleTypes::TYPE_STRING:
		return 2;
	case TupleTypes::TYPE_BLOB:
		return 3;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

size_t SQLCoreExprs::VariantUtils::toBasicTypeNumber(TupleColumnType src) {
	typedef SQLValues::TypeUtils TypeUtils;
	switch (TypeUtils::toNonNullable(src)) {
	case TupleTypes::TYPE_BYTE:
		return 0;
	case TupleTypes::TYPE_SHORT:
		return 1;
	case TupleTypes::TYPE_INTEGER:
		return 2;
	case TupleTypes::TYPE_LONG:
		return 3;
	case TupleTypes::TYPE_FLOAT:
		return 4;
	case TupleTypes::TYPE_DOUBLE:
		return 5;

	case TupleTypes::TYPE_TIMESTAMP:
		return toBasicTypeNumber(TupleTypes::TYPE_LONG);

	case TupleTypes::TYPE_BOOL:
		return 6;
	case TupleTypes::TYPE_STRING:
		return 7;
	case TupleTypes::TYPE_BLOB:
		return 8;
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return 9;
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		return 10;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

size_t SQLCoreExprs::VariantUtils::toFullTypeNumber(TupleColumnType src) {
	typedef SQLValues::TypeUtils TypeUtils;

	const size_t offset = VariantTypes::COUNT_BASIC_TYPE;
	switch (TypeUtils::toNonNullable(src)) {
	case TupleTypes::TYPE_TIMESTAMP:
		return offset + 0;
	default:
		return toBasicTypeNumber(src);
	}
}

size_t SQLCoreExprs::VariantUtils::toBasicAndPromoTypeNumber(
		TupleColumnType src, TupleColumnType promo) {
	typedef SQLValues::TypeUtils TypeUtils;

	if (TypeUtils::toComparisonType(TypeUtils::toNonNullable(src)) !=
			TypeUtils::toComparisonType(TypeUtils::toNonNullable(promo))) {
		if (TypeUtils::isAny(src)) {
			return toBasicTypeNumber(promo);
		}
		else if (TypeUtils::isTimestampFamily(src)) {
			assert(src != TupleTypes::TYPE_NANO_TIMESTAMP);
			assert(TypeUtils::isTimestampFamily(promo));
			return (VariantTypes::COUNT_BASIC_TYPE +
					VariantTypes::COUNT_NUMERIC_PROMO) +
					toTimestampPromoSubIndex(src);
		}
		assert(TypeUtils::isIntegral(src) && TypeUtils::isFloating(promo));
		return VariantTypes::COUNT_BASIC_TYPE + toBasicTypeNumber(src);
	}

	return toBasicTypeNumber(src);
}

bool SQLCoreExprs::VariantUtils::findBasicCombiTypeNumber(
		TupleColumnType type1, TupleColumnType type2, size_t &num) {
	typedef SQLValues::TypeUtils TypeUtils;
	num = std::numeric_limits<size_t>::max();

	const size_t sub1 = toBasicTypeNumber(type1);
	const size_t sub2 = toBasicTypeNumber(type2);

	if (TypeUtils::isTimestampFamily(type1)) {
		if (!TypeUtils::isTimestampFamily(type2)) {
			return false;
		}

		const size_t tsSub1 = toTimestampCombiSubIndex(type1);
		const size_t tsSub2 = toTimestampCombiSubIndex(type2);
		size_t baseNum;
		if (!findCombiNumber(tsSub1, tsSub2, baseNum)) {
			return false;
		}

		if (baseNum < VariantTypes::COUNT_NO_BASIC_TIMESTAMP_COMBI) {
			assert(tsSub1 == 0 && tsSub2 == 0 && baseNum == 0);
			return findCombiNumber(sub1, sub2, num);
		}

		num = VariantTypes::COUNT_NUMERIC_COMBI + baseNum -
				VariantTypes::COUNT_NO_BASIC_TIMESTAMP_COMBI;
		return true;
	}

	if (TypeUtils::isNumerical(type1)) {
		if (!TypeUtils::isNumerical(type2)) {
			return false;
		}
		return findCombiNumber(sub1, sub2, num);
	}

	if (TypeUtils::toNonNullable(type1) != TypeUtils::toNonNullable(type2)) {
		return false;
	}

	assert(sub1 >= VariantTypes::INDEX_OTHER_BEGIN);
	num = VariantTypes::COUNT_SOME_BASIC_COMBI +
			(sub1 - VariantTypes::INDEX_OTHER_BEGIN);
	return true;
}

size_t SQLCoreExprs::VariantUtils::toTimestampPromoSubIndex(
		TupleColumnType src) {
	typedef SQLValues::TypeUtils TypeUtils;
	UTIL_UNUSED_TYPE_ALIAS(TypeUtils);

	assert(TypeUtils::toNonNullable(src) != TupleTypes::TYPE_NANO_TIMESTAMP);
	return toTimestampCombiSubIndex(src);
}

size_t SQLCoreExprs::VariantUtils::toTimestampCombiSubIndex(
		TupleColumnType src) {
	typedef SQLValues::TypeUtils TypeUtils;

	assert(TypeUtils::isTimestampFamily(src));

	if (TypeUtils::toNonNullable(src) == TupleTypes::TYPE_TIMESTAMP) {
		return 0;
	}

	const size_t index = toBasicTypeNumber(src) -
			(VariantTypes::COUNT_NUMERIC + VariantTypes::COUNT_OTHER_TYPE) + 1;
	assert(index < VariantTypes::COUNT_TIMESTAMP_FULL);
	return index;
}

bool SQLCoreExprs::VariantUtils::findCombiNumber(
		size_t n1, size_t n2, size_t &num) {
	num = std::numeric_limits<size_t>::max();

	if (n1 < n2) {
		return false;
	}

	num = getCombiCount(n1) + n2;
	return true;
}

size_t SQLCoreExprs::VariantUtils::getCombiCount(size_t n) {
	return (n + 1) * n / 2;
}

const SQLExprs::Expression& SQLCoreExprs::VariantUtils::getBaseExpression(
		ExprFactoryContext &cxt, const size_t *argIndex) {
	const Expression *top = cxt.getBaseExpression();

	const Expression *base = NULL;
	if (argIndex == NULL) {
		base = top;
	}
	else if (*argIndex == 0) {
		base = &top->child();
	}
	else if (*argIndex == 1) {
		base = &top->child().next();
	}

	if (base == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *base;
}


SQLCoreExprs::ArgExprCode::ArgExprCode() :
		expr_(NULL) {
}

void SQLCoreExprs::ArgExprCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(argIndex);

	expr_ = expr;
}


SQLCoreExprs::GeneralColumnCode::GeneralColumnCode() :
		unified_(false) {
}

void SQLCoreExprs::GeneralColumnCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	const ExprCode &base =
			VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	base_ = base;
	unified_ = ((base.getAttributes() & ExprCode::ATTR_COLUMN_UNIFIED) != 0);
}


SQLCoreExprs::ReaderColumnCode::ReaderColumnCode() :
		reader_(NULL),
		activeReaderRef_(NULL) {
}

void SQLCoreExprs::ReaderColumnCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	const ExprCode &base =
			VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	cxt.addReaderRef(base.getInput(), &reader_);
	activeReaderRef_ = cxt.getActiveReaderRef();
	column_ = cxt.getInputColumn(base.getInput(), base.getColumnPos());
}


SQLCoreExprs::ReadableTupleColumnCode::ReadableTupleColumnCode() :
		tuple_(NULL) {
}

void SQLCoreExprs::ReadableTupleColumnCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	const ExprCode &base =
			VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	tuple_ = cxt.getReadableTupleRef(base.getInput());
	column_ = cxt.getInputColumn(base.getInput(), base.getColumnPos());
}


SQLCoreExprs::SummaryColumnCode::SummaryColumnCode() :
		tuple_(NULL),
		column_(NULL) {
}

void SQLCoreExprs::SummaryColumnCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	const ExprCode &base =
			VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	tuple_ = cxt.getSummaryTupleRef(base.getInput());
	column_ = &(*cxt.getSummaryColumnListRef(
			base.getInput()))[base.getColumnPos()];
}


SQLCoreExprs::ConstCode::ConstCode() {
}

void SQLCoreExprs::ConstCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	const ExprCode &base =
			VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	value_ = base.getValue();
}


SQLCoreExprs::GeneralColumnEvaluator::ResultType
SQLCoreExprs::GeneralColumnEvaluator::eval(ExprContext &cxt) const {
	const ExprCode &code = code_.base_;

	const uint32_t input = code.getInput();
	const uint32_t pos = code.getColumnPos();

	switch (cxt.getInputSourceType(input)) {
	case SQLExprs::ExprCode::INPUT_ARRAY_TUPLE:
		return SQLValues::ValueUtils::duplicateValue(
				cxt, cxt.getArrayTuple(0)->get(pos));
	case SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE:
		return cxt.getSummaryTupleRef(input)->getValue(
				(*cxt.getSummaryColumnListRef(input))[pos]);
	default:
		return cxt.getReadableTuple(input).get(
				cxt.getReaderColumn(input, pos));
	}
}


size_t SQLCoreExprs::IdExpression::VariantTraits::resolveVariant(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(cxt);

	const bool grouping =
			((code.getAttributes() & ExprCode::ATTR_GROUPING) != 0);
	const ExprCode::InputSourceType sourceType =
			cxt.getInputSourceType(code.getInput());
	if (!grouping &&
			ExprCode::INPUT_CONTAINER < sourceType &&
			sourceType - ExprCode::INPUT_CONTAINER <=
					COUNT_CONTAINER_SOURCE) {
		const size_t sourceNum =
				(sourceType - ExprCode::INPUT_CONTAINER - 1);
		assert(sourceNum < COUNT_CONTAINER_SOURCE);
		return OFFSET_CONTAINER + sourceNum;
	}
	else {
		return OFFSET_NON_CONTAINER + (grouping ? 1 : 0);
	}
}

void SQLCoreExprs::IdExpression::VariantBase::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(cxt);
	input_ = code.getInput();
}

SQLCoreExprs::IdExpression::VariantBase::VariantBase() :
		input_(ExprCode().getInput()) {
}

SQLCoreExprs::IdExpression::VariantBase::~VariantBase() {
}

template<size_t V>
TupleValue SQLCoreExprs::IdExpression::VariantAt<V>::eval(ExprContext &cxt) const {
	if (Grouping::VALUE) {
		return TupleValue(cxt.getLastTupleId(input_));
	}
	else {
		return TupleValue(cxt.updateTupleId(input_));
	}
}


SQLCoreExprs::ConstantExpression::ConstantExpression(
		ExprFactoryContext &cxt, const ExprCode &code) :
		value_(code.getValue()) {
	static_cast<void>(cxt);
}

TupleValue SQLCoreExprs::ConstantExpression::eval(ExprContext &cxt) const {
	static_cast<void>(cxt);
	return value_;
}


size_t SQLCoreExprs::ColumnExpression::VariantTraits::resolveVariant(
		ExprFactoryContext &cxt, const ExprCode &code) {
	typedef SQLValues::TypeUtils TypeUtils;

	const ExprCode::InputSourceType sourceType =
			cxt.getInputSourceType(code.getInput());

	const TupleColumnType inColumnType =
			cxt.getInputType(code.getInput(), code.getColumnPos());

	const bool nullable = TypeUtils::isNullable(inColumnType);
	const bool any = TypeUtils::isAny(inColumnType);

	const size_t baseNum = COUNT_FULL_TYPE * (nullable ? 1 : 0);
	const size_t fullNum =
			(any ? 0 : VariantUtils::toFullTypeNumber(inColumnType));

	if ((code.getAttributes() &
			SQLExprs::ExprCode::ATTR_COLUMN_UNIFIED) != 0) {
		return OFFSET_NON_CONTAINER;
	}
	else if (ExprCode::INPUT_CONTAINER < sourceType &&
			sourceType - ExprCode::INPUT_CONTAINER <=
			COUNT_CONTAINER_SOURCE) {
		const size_t offset = OFFSET_CONTAINER;
		if (any) {
			return offset;
		}
		const size_t containerBaseNum =
				COUNT_BASE * (sourceType - ExprCode::INPUT_CONTAINER - 1);
		return (offset + 1) + containerBaseNum + baseNum + fullNum;
	}
	else {
		const size_t offset = OFFSET_NON_CONTAINER;

		if (any) {
			return offset;
		}

		const TupleColumnType outColumnType = code.getColumnType();
		if (SQLValues::TypeUtils::toNonNullable(inColumnType) !=
				SQLValues::TypeUtils::toNonNullable(outColumnType)) {
			return offset;
		}

		size_t srcNum;
		if (sourceType == ExprCode::INPUT_READER) {
			srcNum = 0;
		}
		else if (sourceType == ExprCode::INPUT_READER_MULTI) {
			srcNum = 1;
		}
		else if (sourceType == ExprCode::INPUT_SUMMARY_TUPLE) {
			srcNum = 2;
		}
		else {
			return offset;
		}

		return (offset + 1) + COUNT_BASE * srcNum + baseNum + fullNum;
	}
}


size_t SQLCoreExprs::ComparisonExpressionBase::VariantTraits::resolveVariant(
		ExprFactoryContext &cxt, const ExprCode &code) {
	typedef SQLValues::TypeUtils TypeUtils;
	typedef SQLExprs::Expression Expression;

	const size_t fullGeneral = OFFSET_EXPR;

	const Expression *baseExpr = cxt.getBaseExpression();
	assert(baseExpr != NULL);

	const Expression &arg1 = baseExpr->child();
	const Expression &arg2 = arg1.next();

	const ExprCode &code1 = arg1.getCode();
	const ExprCode &code2 = arg2.getCode();

	const TupleColumnType columnType1 = code1.getColumnType();
	const TupleColumnType columnType2 = code2.getColumnType();

	const bool any1 = TypeUtils::isAny(columnType1);
	const bool any2 = TypeUtils::isAny(columnType2);
	if (any1 && any2) {
		return fullGeneral;
	}
	const bool someAny = (any1 || any2);

	const bool anyAsNull = true;
	const TupleColumnType promoType =
			TypeUtils::findPromotionType(columnType1, columnType2, anyAsNull);
	if (TypeUtils::isNull(promoType)) {
		return fullGeneral;
	}

	const bool nullable1 = TypeUtils::isNullable(columnType1);
	const bool nullable2 = TypeUtils::isNullable(columnType2);
	const bool someNullable = (nullable1 || nullable2);

	const TupleColumnType outType = code.getColumnType();
	if (someNullable && !TypeUtils::isNullable(outType)) {
		return fullGeneral;
	}

	const TupleColumnType compType = TypeUtils::toNonNullable(promoType);

	const size_t basicPromoNum = (any1 ? 0 :
			VariantUtils::toBasicAndPromoTypeNumber(columnType1, compType));

	const bool promoted1 = !any1;
	const bool promoted2 =
			(TypeUtils::toNonNullable(columnType2) ==
			TypeUtils::toNonNullable(compType));

	const bool column1 = (code1.getType() == SQLType::EXPR_COLUMN);
	const bool column2 = (code2.getType() == SQLType::EXPR_COLUMN);

	const ExprCode::InputSourceType sourceType1 = (column1 ?
			cxt.getInputSourceType(code1.getInput()) : ExprCode::END_INPUT);
	const ExprCode::InputSourceType sourceType2 = (column2 ?
			cxt.getInputSourceType(code2.getInput()) : ExprCode::END_INPUT);

	const bool readerColumn1 = (sourceType1 == ExprCode::INPUT_READER);
	const bool readerColumn2 = (sourceType2 == ExprCode::INPUT_READER);

	const bool containerColumn1 = (ExprCode::INPUT_CONTAINER < sourceType1 &&
			sourceType1 - ExprCode::INPUT_CONTAINER <=
			COUNT_CONTAINER_SOURCE);

	const bool const2 = (code2.getType() == SQLType::EXPR_CONSTANT);

	SQLExprs::ExprProfile *profile = cxt.getProfile();
	SQLValues::ProfileElement emptyProfile;
	SQLValues::ProfileElement &constProfile =
			(profile == NULL ? emptyProfile : profile->compConst_);
	SQLValues::ProfileElement &columnsProfile =
			(profile == NULL ? emptyProfile : profile->compColumns_);

	if (!someAny && (const2 && promoted1 && promoted2 && !nullable2)) {
		constProfile.candidate_++;
		if (readerColumn1) {
			constProfile.target_++;
			const size_t offset = OFFSET_CONST +
					COUNT_BASIC_AND_PROMO_TYPE * (nullable1 ? 1 : 0);
			return offset + basicPromoNum;
		}
		else if (containerColumn1) {
			constProfile.target_++;
			const size_t offset = OFFSET_CONTAINER +
					COUNT_CONST *
							(sourceType1 - ExprCode::INPUT_CONTAINER - 1) +
					COUNT_BASIC_AND_PROMO_TYPE * (nullable1 ? 1 : 0);
			return offset + basicPromoNum;
		}
	}

	columnsProfile.candidate_++;
	if (!someAny) {
		const size_t baseOffset = (readerColumn1 && readerColumn2 ?
				OFFSET_COLUMN : (OFFSET_EXPR + 1));
		const size_t offset =
				baseOffset + COUNT_BASIC_COMBI * (someNullable ? 1 : 0);

		size_t num;
		if (VariantUtils::findBasicCombiTypeNumber(
				columnType1, columnType2, num)) {
			columnsProfile.target_++;
			return offset + num;
		}
	}

	return fullGeneral;
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
			return asColumnValue(&cxt.getValueContext(), expr->eval(cxt));
		}

		const TupleValue &condValue = expr->eval(cxt);
		if (!SQLValues::ValueUtils::isNull(condValue) &&
				SQLValues::ValueUtils::toBool(condValue)) {
			return asColumnValue(&cxt.getValueContext(), thenExpr->eval(cxt));
		}

		expr = thenExpr->findNext();
	}
	while (expr != NULL);

	return TupleValue();
}


size_t SQLCoreExprs::InExpression::VariantTraits::resolveVariant(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(code);

	const Expression *base = cxt.getBaseExpression();
	assert(base != NULL);

	const Expression &keyExpr = base->child();
	const Expression &condList = keyExpr.next();
	assert(condList.findChild() != NULL);

	const TupleColumnType keyType = keyExpr.getCode().getColumnType();
	const TupleColumnType condType = condList.getCode().getColumnType();

	const TupleColumnType compType = SQLValues::TypeUtils::toNonNullable(
			SQLValues::TypeUtils::findComparisonType(keyType, condType, true));
	assert(!SQLValues::TypeUtils::isNull(compType));

	if (SQLValues::TypeUtils::isAny(compType)) {
		return 0;
	}

	const size_t offset = 1;
	const bool nullableNum = (
			SQLValues::TypeUtils::isNullable(keyType) ||
			SQLValues::TypeUtils::isAny(keyType) ? 1 : 0);
	const size_t typeNum =
			VariantUtils::toBasicAndPromoTypeNumber(keyType, compType);

	return offset + COUNT_BASIC_AND_PROMO_TYPE * nullableNum + typeNum;
}

SQLCoreExprs::InExpression::VariantBase::~VariantBase() {
}

void SQLCoreExprs::InExpression::VariantBase::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(code);

	const Expression *base = cxt.getBaseExpression();
	assert(base != NULL);

	const Expression &keyExpr = base->child();
	const Expression &condList = keyExpr.next();
	assert(condList.findChild() != NULL);

	const TupleColumnType keyType = keyExpr.getCode().getColumnType();
	const TupleColumnType condType = condList.getCode().getColumnType();

	const TupleColumnType compType = SQLValues::TypeUtils::toNonNullable(
			SQLValues::TypeUtils::findComparisonType(keyType, condType, true));
	assert(!SQLValues::TypeUtils::isNull(compType));

	accessor_ = SQLValues::ValueAccessor(compType);
	digester_ = SQLValues::ValueDigester(
			accessor_, NULL, false, false,
			static_cast<int64_t>(SQLValues::ValueUtils::fnv1aHashInit()));

	typedef SQLValues::ValueDigester::Switcher<
			SQLValues::ValueAccessor::ByValue, 0, false> DigesterSwitcher;
	DigesterSwitcher::DefaultFuncType digesterFunc =
			DigesterSwitcher(digester_, false, false).getWith<
			const SQLValues::ValueDigester,
			DigesterSwitcher::DefaultTraitsType>();

	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(cxt.getAllocator()));
	bool nullFound = false;
	map_ = UTIL_MAKE_LOCAL_UNIQUE(
			map_, Map, 0, MapHasher(), EqPred(), cxt.getAllocator());
	for (Expression::Iterator condIt(condList);
			condIt.exists(); condIt.next()) {
		const TupleValue &baseValue = condIt.get().getCode().getValue();
		if (SQLValues::ValueUtils::isNull(baseValue)) {
			nullFound = true;
			continue;
		}

		const TupleValue &value =
				SQLValues::ValuePromoter(compType)(&valueCxt, baseValue);
		const int64_t digest = digesterFunc(digester_, value);

		const std::pair<MapIterator, MapIterator> &range =
				map_->equal_range(digest);
		for (MapIterator it = range.first;; ++it) {
			if (it == range.second) {
				map_->insert(std::make_pair(digest, value));
				break;
			}
			else if (SQLValues::ValueEq()(it->second, value)) {
				break;
			}
		}
	}

	if (nullFound) {
		unmatchResult_ = TupleValue();
	}
	else {
		unmatchResult_ = SQLValues::ValueUtils::toAnyByBool(false);
	}
}

SQLCoreExprs::InExpression::VariantBase::VariantBase() :
		accessor_(TupleTypes::TYPE_NULL),
		digester_(accessor_, NULL, false, false, 0) {
}

template<size_t V>
TupleValue SQLCoreExprs::InExpression::VariantAt<V>::eval(
		ExprContext &cxt) const {
	const TupleValue &uncheckedKey = child().eval(cxt);
	if (SUB_NULLABLE && SQLValues::ValueUtils::isNull(uncheckedKey)) {
		return TupleValue();
	}

	const LocalCompType &key = SQLValues::ValueUtils::getPromoted<
			CompTypeTag, SrcTypeTag>(
					SQLValues::ValueUtils::getValue<SrcType>(uncheckedKey));
	const int64_t digest = Digester(digester_)(key);

	const std::pair<MapIterator, MapIterator> &range =
			map_->equal_range(digest);
	for (MapIterator it = range.first; it != range.second; ++it) {
		const LocalCompType &cond =
				SQLValues::ValueUtils::getValue<CompType>(it->second);

		if (Comparator(COMP_SENSITIVE)(key, cond, EqPred())) {
			return SQLValues::ValueUtils::toAnyByBool(true);
		}
	}

	return unmatchResult_;
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


SQLCoreExprs::RangeKeyExpression::RangeKeyExpression(
		ExprFactoryContext &cxt, const ExprCode &code) :
		forPrev_(code.getColumnPos() == 1),
		forNext_(code.getColumnPos() == 2) {
	static_cast<void>(cxt);
}

TupleValue SQLCoreExprs::RangeKeyExpression::eval(ExprContext &cxt) const {
	const SQLExprs::WindowState *state = cxt.getWindowState();
	const SQLValues::LongInt &key =
			(forPrev_ ? state->rangePrevKey_ :
			(forNext_ ? state->rangeNextKey_ : state->rangeKey_));
	return SQLValues::ValueConverter(getCode().getColumnType())(
			cxt, SQLValues::ValueUtils::toAnyByLongInt(
					cxt.getValueContext(), key));
}


SQLCoreExprs::LinearExpression::LinearExpression(
		ExprFactoryContext &cxt, const ExprCode &code) :
		interpolationFunc_(resolveInterpolationFunction(code.getColumnType())) {
	static_cast<void>(cxt);
}

TupleValue SQLCoreExprs::LinearExpression::eval(ExprContext &cxt) const {
	const size_t argCount = 5;
	TupleValue argList[argCount];
	TupleValue *argIt = argList;
	TupleValue *const argEnd = argIt + argCount;
	for (Iterator it(*this);; it.next()) {
		assert(it.exists());
		*argIt = it.get().eval(cxt);
		if (SQLValues::ValueUtils::isNull(*argIt)) {
			return asColumnValue(&cxt.getValueContext(), TupleValue());
		}
		if (++argIt == argEnd) {
			break;
		}
	}

	return (this->*interpolationFunc_)(
			cxt, argList[0], argList[1], argList[2], argList[3], argList[4]);
}

SQLCoreExprs::LinearExpression::InterpolationFunc
SQLCoreExprs::LinearExpression::resolveInterpolationFunction(
		TupleColumnType type) {
	const TupleColumnType baseType = SQLValues::TypeUtils::toNonNullable(type);
	const SQLValues::TypeUtils::TypeCategory baseCategory =
			(SQLValues::TypeUtils::isAny(baseType) ?
					SQLValues::TypeUtils::TYPE_CATEGORY_NULL :
					SQLValues::TypeUtils::getStaticTypeCategory(baseType));

	switch (baseCategory) {
	case SQLValues::TypeUtils::TYPE_CATEGORY_BOOL:
		return &LinearExpression::interpolateLong;
	case SQLValues::TypeUtils::TYPE_CATEGORY_LONG:
		return &LinearExpression::interpolateLong;
	case SQLValues::TypeUtils::TYPE_CATEGORY_DOUBLE:
		return &LinearExpression::interpolateDouble;
	case SQLValues::TypeUtils::TYPE_CATEGORY_TIMESTAMP:
		switch (baseType) {
		case TupleTypes::TYPE_MICRO_TIMESTAMP:
			return &LinearExpression::interpolateMicroTimestamp;
		case TupleTypes::TYPE_NANO_TIMESTAMP:
			return &LinearExpression::interpolateNanoTimestamp;
		default:
			assert(baseType == TupleTypes::TYPE_TIMESTAMP);
			return &LinearExpression::interpolateLong;
		}
	default:
		assert(!(SQLValues::TypeUtils::isNumerical(baseType) ||
				SQLValues::TypeUtils::isSomeFixed(baseType)) ||
				SQLValues::TypeUtils::isTimestampFamily(baseType));
		return &LinearExpression::interpolateNonNumeric;
	}
}

SQLValues::DateTimeElements SQLCoreExprs::LinearExpression::interpolateDateTime(
		const TupleValue &x1, const TupleValue &y1, const TupleValue &x2,
		const TupleValue &y2, const TupleValue &x) const {
	const SQLValues::LongInt c = SQLValues::ValueUtils::toLongInt(y1);

	const SQLValues::LongInt dx =
			SQLValues::ValueUtils::toLongInt(x2).subtract(
					SQLValues::ValueUtils::toLongInt(x1));
	const SQLValues::LongInt dy =
			SQLValues::ValueUtils::toLongInt(y2).subtract(c);

	const SQLValues::LongInt w =
			SQLValues::ValueUtils::toLongInt(x).subtract(
					SQLValues::ValueUtils::toLongInt(x1));
	const SQLValues::LongInt h = (dx.isZero() ? dx : w.multiply(dy).divide(dx));

	const SQLValues::LongInt y = c.add(h);
	return SQLValues::DateTimeElements::ofLongInt(y);
}

TupleValue SQLCoreExprs::LinearExpression::interpolateLong(
		ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
		const TupleValue &x2, const TupleValue &y2, const TupleValue &x) const {
	const int64_t c = SQLValues::ValueUtils::toLong(y1);

	const SQLValues::LongInt dx =
			SQLValues::ValueUtils::toLongInt(x2).subtract(
					SQLValues::ValueUtils::toLongInt(x1));
	const SQLValues::LongInt dy =
			SQLValues::ValueUtils::toLongInt(y2).subtract(
					SQLValues::ValueUtils::toLongInt(TupleValue(c)));

	const SQLValues::LongInt w =
			SQLValues::ValueUtils::toLongInt(x).subtract(
					SQLValues::ValueUtils::toLongInt(x1));
	const int64_t h = (dx.isZero() ? 0 : w.multiply(dy).divide(dx).toLong());

	const int64_t y = c + h;
	return convertToResultValue(cxt, TupleValue(y));
}

TupleValue SQLCoreExprs::LinearExpression::interpolateDouble(
		ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
		const TupleValue &x2, const TupleValue &y2, const TupleValue &x) const {
	const double c = SQLValues::ValueUtils::toDouble(y1);

	const double dx =
			SQLValues::ValueUtils::toLongInt(x2).subtract(
					SQLValues::ValueUtils::toLongInt(x1)).toDouble();
	const double dy = SQLValues::ValueUtils::toDouble(y2) - c;

	const double w =
			SQLValues::ValueUtils::toLongInt(x).subtract(
					SQLValues::ValueUtils::toLongInt(x1)).toDouble();
	const double h = (w * dy) / dx;

	const double y = c + h;
	return convertToResultValue(cxt, TupleValue(y));
}

TupleValue SQLCoreExprs::LinearExpression::interpolateMicroTimestamp(
		ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
		const TupleValue &x2, const TupleValue &y2, const TupleValue &x) const {
	const SQLValues::DateTimeElements &dt =
			interpolateDateTime(x1, y1, x2, y2, x);
	return SQLValues::ValueUtils::toAnyByWritable<
			TupleTypes::TYPE_MICRO_TIMESTAMP>(
			cxt, dt.toTimestamp(SQLValues::Types::MicroTimestampTag()));
}

TupleValue SQLCoreExprs::LinearExpression::interpolateNanoTimestamp(
		ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
		const TupleValue &x2, const TupleValue &y2, const TupleValue &x) const {
	const SQLValues::DateTimeElements &dt =
			interpolateDateTime(x1, y1, x2, y2, x);
	return SQLValues::ValueUtils::toAnyByWritable<
			TupleTypes::TYPE_NANO_TIMESTAMP>(
			cxt, dt.toTimestamp(SQLValues::Types::NanoTimestampTag()));
}

TupleValue SQLCoreExprs::LinearExpression::interpolateNonNumeric(
		ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
		const TupleValue &x2, const TupleValue &y2, const TupleValue &x) const {
	static_cast<void>(x1);
	static_cast<void>(x2);
	static_cast<void>(y2);
	static_cast<void>(x);
	return asColumnValue(&cxt.getValueContext(), y1);
}

TupleValue SQLCoreExprs::LinearExpression::convertToResultValue(
		ExprContext &cxt, const TupleValue &src) const {
	return SQLValues::ValueConverter(getCode().getColumnType())(cxt, src);
}


inline TupleValue SQLCoreExprs::RangeGroupIdExpression::eval(
		ExprContext &cxt) const {
	Iterator it(*this);

	assert(it.exists());
	const TupleValue &value = it.get().eval(cxt);

	it.next();
	assert(it.exists());
	const SQLValues::LongInt &interval =
			SQLValues::ValueUtils::toLongInt(it.get().eval(cxt));

	it.next();
	assert(it.exists());
	const SQLValues::LongInt &offset =
			SQLValues::ValueUtils::toLongInt(it.get().eval(cxt));

	SQLExprs::RangeKey id = SQLExprs::RangeKey::invalid();
	if (!SQLExprs::FunctionValueUtils().getRangeGroupId(
			value, interval, offset, id)) {
		return TupleValue();
	}

	const NanoTimestamp &nanoTs =
			SQLValues::DateTimeElements::ofLongInt(id).toTimestamp(
					SQLValues::Types::NanoTimestampTag());
	const TupleValue &ret = asColumnValue(
			&cxt.getValueContext(), TupleNanoTimestamp(&nanoTs));

	return SQLValues::ValueUtils::duplicateValue(cxt, ret);
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
	static_cast<void>(cxt);
	if (SQLValues::ValueGreater()(value, result_)) {
		result_ = value;
	}
	return TupleValue();
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Max::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	if (cxt.getComparator()(value1, value2) >= 0) {
		result_ = value1;
	}
	else {
		result_ = value2;
	}
	return TupleValue();
}


template<typename C>
inline TupleValue SQLCoreExprs::Functions::Min::operator()(C &cxt) {
	static_cast<void>(cxt);
	return result_;
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Min::operator()(
		C &cxt, const TupleValue &value) {
	static_cast<void>(cxt);
	if (SQLValues::ValueLess()(value, result_)) {
		result_ = value;
	}
	return TupleValue();
}

template<typename C>
inline TupleValue SQLCoreExprs::Functions::Min::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	if (cxt.getComparator()(value1, value2) <= 0) {
		result_ = value1;
	}
	else {
		result_ = value2;
	}
	return TupleValue();
}


template<typename C>
inline TupleValue SQLCoreExprs::Functions::NullIf::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	if (cxt.getComparator()(value1, value2) == 0) {
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
	const bool forDisplay = true;
	const char8_t *typeStr =
			SQLValues::TypeUtils::toString(
					value.getType(), nullOnUnknown, forDisplay);

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
		int64_t asUnsigned_;
	} value, bits;
	value.asSigned_ = value1;

	if (left) {
		bits.asUnsigned_ = value.asUnsigned_ << shift;
	}
	else {
		bits.asUnsigned_ = value.asUnsigned_ >> shift;
		if (value1 < 0) {
			bits.asUnsigned_ |= ~static_cast<uint64_t>(0) << (64 - shift);
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
bool SQLCoreExprs::Functions::Is::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	return cxt.getComparator()(value1, value2) == 0;
}


template<typename C>
bool SQLCoreExprs::Functions::IsNot::operator()(
		C &cxt, const TupleValue &value1, const TupleValue &value2) {
	return cxt.getComparator()(value1, value2) != 0;
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
