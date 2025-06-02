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

#include "sql_value.h"
#include "sql_utils_vdbe.h"
#include "util/numeric.h"


bool SQLValues::TypeUtils::isAny(TupleColumnType type) {
	return BaseUtils::isAny(type);
}

bool SQLValues::TypeUtils::isNull(TupleColumnType type) {
	return BaseUtils::isNull(type);
}

bool SQLValues::TypeUtils::isSomeFixed(TupleColumnType type) {
	return BaseUtils::isSomeFixed(type);
}

bool SQLValues::TypeUtils::isNormalFixed(TupleColumnType type) {
	return BaseUtils::isNormalFixed(type);
}

bool SQLValues::TypeUtils::isLargeFixed(TupleColumnType type) {
	return BaseUtils::isLargeFixed(type);
}

bool SQLValues::TypeUtils::isSingleVar(TupleColumnType type) {
	return BaseUtils::isSingleVar(toNonNullable(type));
}

bool SQLValues::TypeUtils::isSingleVarOrLarge(TupleColumnType type) {
	return BaseUtils::isSingleVarOrLarge(toNonNullable(type));
}

bool SQLValues::TypeUtils::isArray(TupleColumnType type) {
	return BaseUtils::isArray(type);
}

bool SQLValues::TypeUtils::isLob(TupleColumnType type) {
	return BaseUtils::isLob(toNonNullable(type));
}

bool SQLValues::TypeUtils::isIntegral(TupleColumnType type) {
	return BaseUtils::isIntegral(type);
}

bool SQLValues::TypeUtils::isFloating(TupleColumnType type) {
	return BaseUtils::isFloating(type);
}

bool SQLValues::TypeUtils::isTimestampFamily(TupleColumnType type) {
	return BaseUtils::isTimestampFamily(type);
}

bool SQLValues::TypeUtils::isDeclarable(TupleColumnType type) {
	return BaseUtils::isDeclarable(type);
}

bool SQLValues::TypeUtils::isSupported(TupleColumnType type) {
	return BaseUtils::isSupported(type);
}

bool SQLValues::TypeUtils::isNullable(TupleColumnType type) {
	return BaseUtils::isNullable(type);
}

TupleColumnType SQLValues::TypeUtils::setNullable(
		TupleColumnType type, bool nullable) {
	const TupleColumnType baseType = toNonNullable(type);
	if (!nullable || isAny(baseType) || isNull(baseType)) {
		return baseType;
	}

	return static_cast<TupleColumnType>(
			baseType | TupleTypes::TYPE_MASK_NULLABLE);
}

TupleColumnType SQLValues::TypeUtils::toNonNullable(TupleColumnType type) {
	return static_cast<TupleColumnType>(
			type & ~TupleTypes::TYPE_MASK_NULLABLE);
}

TupleColumnType SQLValues::TypeUtils::toNullable(TupleColumnType type) {
	return setNullable(type, true);
}

size_t SQLValues::TypeUtils::getFixedSize(TupleColumnType type) {
	return BaseUtils::getFixedSize(type);
}

TupleColumnType SQLValues::TypeUtils::toComparisonType(TupleColumnType type) {
	if (isAny(type)) {
		return TupleTypes::TYPE_ANY;
	}

	TupleColumnType baseType;
	switch (getStaticTypeCategory(toNonNullable(type))) {
	case TYPE_CATEGORY_LONG:
		baseType = TupleTypes::TYPE_LONG;
		break;
	case TYPE_CATEGORY_DOUBLE:
		baseType = TupleTypes::TYPE_DOUBLE;
		break;
	default:
		baseType = type;
		break;
	}
	return setNullable(baseType, isNullable(type));
}

TupleColumnType SQLValues::TypeUtils::findComparisonType(
		TupleColumnType type1, TupleColumnType type2, bool anyAsNull) {
	const TupleColumnType promoType = findPromotionType(type1, type2, anyAsNull);
	if (isNull(promoType)) {
		return TupleTypes::TYPE_NULL;
	}

	return toComparisonType(promoType);
}

TupleColumnType SQLValues::TypeUtils::findPromotionType(
		TupleColumnType type1, TupleColumnType type2, bool anyAsNull) {
	const TupleColumnType base1 = toNonNullable(type1);
	const TupleColumnType base2 = toNonNullable(type2);
	const bool eitherNullable = (isNullable(type1) || isNullable(type2));

	if (base1 == base2) {
		return setNullable(base1, eitherNullable);
	}

	if (anyAsNull) {
		if (isAny(base1)) {
			return setNullable(base2, true);
		}
		if (isAny(base2)) {
			return setNullable(base1, true);
		}
	}

	const TypeCategory category1 = getStaticTypeCategory(base1);
	const TypeCategory category2 = getStaticTypeCategory(base2);

	if (category1 == category2 &&
			category1 == TYPE_CATEGORY_TIMESTAMP) {
		if (getTimePrecisionLevel(base1) >=
				getTimePrecisionLevel(base2)) {
			return setNullable(base1, eitherNullable);
		}
		else {
			return setNullable(base2, eitherNullable);
		}
	}

	const int32_t maskedCategory1 =
			category1 & ~TYPE_CATEGORY_NUMERIC_MASK;
	const int32_t maskedCategory2 =
			category2 & ~TYPE_CATEGORY_NUMERIC_MASK;
	if (maskedCategory1 == maskedCategory2) {
		const int32_t bothCategory = (category1 | category2);

		if (bothCategory == TYPE_CATEGORY_LONG) {
			return setNullable(TupleTypes::TYPE_LONG, eitherNullable);
		}
		else {
			assert((category1 & TYPE_CATEGORY_NUMERIC_FLAG) != 0);
			assert((category2 & TYPE_CATEGORY_NUMERIC_FLAG) != 0);
			return setNullable(TupleTypes::TYPE_DOUBLE, eitherNullable);
		}
	}

	return TupleTypes::TYPE_NULL;
}

TupleColumnType SQLValues::TypeUtils::resolveConversionType(
		TupleColumnType src, TupleColumnType desired, bool implicit,
		bool anyAsNull) {
	const TupleColumnType convType =
			findConversionType(src, desired, implicit, anyAsNull);
	if (isNull(convType)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION,
				(implicit ? "Unacceptable implicit" : "Unsupported") <<
				" type conversion (from=" << toString(src) <<
				", to=" <<toString(desired) << ")");
	}

	return convType;
}

TupleColumnType SQLValues::TypeUtils::findEvaluatingConversionType(
		TupleColumnType src, TupleColumnType desired, bool implicit,
		bool anyAsNull, bool evaluating) {
	TupleColumnType result =
			findConversionType(src, desired, implicit, anyAsNull);

	if (isNull(result) && !evaluating) {
		result = findConversionType(
				toNonNullable(src), desired, implicit, anyAsNull);

		if (isNull(result) && anyAsNull && isAny(src) && !isAny(desired)) {
			result = toNonNullable(desired);
		}
	}

	return result;
}

TupleColumnType SQLValues::TypeUtils::findConversionType(
		TupleColumnType src, TupleColumnType desired, bool implicit,
		bool anyAsNull) {
	const TupleColumnType srcBase = toNonNullable(src);
	const TupleColumnType desiredBase = toNonNullable(desired);
	const bool srcNullable = isNullable(src);
	const bool desiredNullable = isNullable(desired);

	if (srcBase == desiredBase) {
		if (srcNullable && !desiredNullable) {
			return TupleTypes::TYPE_NULL;
		}
		if (desiredBase == TupleTypes::TYPE_NUMERIC) {
			return TupleTypes::TYPE_ANY;
		}
		return setNullable(desiredBase, srcNullable);
	}

	if (desired == TupleTypes::TYPE_ANY) {
		return src;
	}

	if (anyAsNull && isAny(src)) {
		if (!desiredNullable) {
			return TupleTypes::TYPE_NULL;
		}
		if (desiredBase == TupleTypes::TYPE_NUMERIC) {
			return TupleTypes::TYPE_ANY;
		}
		return setNullable(desiredBase, true);
	}

	const TypeCategory srcCategory = getStaticTypeCategory(srcBase);
	const TypeCategory desiredCategory = getStaticTypeCategory(desiredBase);

	if (!implicit) {
		if (srcCategory == TYPE_CATEGORY_STRING ||
				desiredCategory == TYPE_CATEGORY_STRING ||
				(srcCategory == TYPE_CATEGORY_BOOL &&
						desiredCategory == TYPE_CATEGORY_LONG) ||
				(srcCategory == TYPE_CATEGORY_LONG &&
						desiredCategory == TYPE_CATEGORY_BOOL) ||
				(srcCategory == TYPE_CATEGORY_TIMESTAMP &&
						desiredCategory == TYPE_CATEGORY_TIMESTAMP)) {
			return setNullable(desiredBase, srcNullable);
		}
	}

	if (srcCategory == TYPE_CATEGORY_TIMESTAMP &&
			desiredCategory == TYPE_CATEGORY_TIMESTAMP) {
		return setNullable(desiredBase, srcNullable);
	}
	else if ((srcCategory & TYPE_CATEGORY_NUMERIC_FLAG) != 0 &&
			(desiredCategory & TYPE_CATEGORY_NUMERIC_FLAG) != 0) {
		if (desiredCategory == TYPE_CATEGORY_NUMERIC_FLAG) {
			if (srcCategory == TYPE_CATEGORY_LONG) {
				return setNullable(TupleTypes::TYPE_LONG, srcNullable);
			}
			else if (srcCategory == TYPE_CATEGORY_DOUBLE) {
				return setNullable(TupleTypes::TYPE_DOUBLE, srcNullable);
			}
			else {
				assert(false);
				return TupleTypes::TYPE_NULL;
			}
		}
		return setNullable(desiredBase, srcNullable);
	}

	return TupleTypes::TYPE_NULL;
}

TupleColumnType SQLValues::TypeUtils::filterColumnType(TupleColumnType type) {
	if (!isSupported(type) || isArray(type)) {
		return TupleTypes::TYPE_ANY;
	}

	return type;
}

SQLValues::TypeUtils::TypeCategory
SQLValues::TypeUtils::getStaticTypeCategory(TupleColumnType type) {
	if (type == TupleTypes::TYPE_NULL) {
		return TYPE_CATEGORY_NULL;
	}
	else if (isIntegral(type)) {
		return TYPE_CATEGORY_LONG;
	}
	else if (isFloating(type)) {
		return TYPE_CATEGORY_DOUBLE;
	}
	else if (type == TupleTypes::TYPE_STRING) {
		return TYPE_CATEGORY_STRING;
	}
	else if (type == TupleTypes::TYPE_BLOB) {
		return TYPE_CATEGORY_BLOB;
	}
	else if (type == TupleTypes::TYPE_BOOL) {
		return TYPE_CATEGORY_BOOL;
	}
	else if (TypeUtils::isTimestampFamily(type)) {
		return TYPE_CATEGORY_TIMESTAMP;
	}
	else if (type == TupleTypes::TYPE_NUMERIC) {
		return TYPE_CATEGORY_NUMERIC_FLAG;
	}

	assert(false);
	return TYPE_CATEGORY_NULL;
}

bool SQLValues::TypeUtils::isNumerical(TupleColumnType type) {
	return isIntegral(type) || isFloating(type);
}

util::DateTime::FieldType SQLValues::TypeUtils::getTimePrecision(
		TupleColumnType type) {
	switch (type) {
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return util::DateTime::FIELD_MICROSECOND;
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		return util::DateTime::FIELD_NANOSECOND;
	default:
		assert(type == TupleTypes::TYPE_TIMESTAMP);
		return util::DateTime::FIELD_MILLISECOND;
	}
}

int32_t SQLValues::TypeUtils::getTimePrecisionLevel(TupleColumnType type) {
	switch (type) {
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return 6;
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		return 9;
	default:
		assert(type == TupleTypes::TYPE_TIMESTAMP);
		return 3;
	}
}

void SQLValues::TypeUtils::setUpTupleInfo(
		TupleList::Info &info, const TupleColumnType *list, size_t count) {
	info.columnTypeList_ = list;
	info.columnCount_ = count;
}

const char8_t* SQLValues::TypeUtils::toString(
		TupleColumnType type, bool nullOnUnknown, bool internal) {
	if (!internal) {
		switch (type) {
		case TupleTypes::TYPE_MICRO_TIMESTAMP:
			return "TIMESTAMP(6)";
		case TupleTypes::TYPE_NANO_TIMESTAMP:
			return "TIMESTAMP(9)";
		default:
			break;
		}
	}

	const char8_t *str = TupleList::TupleColumnTypeCoder()(type);
	if (str == NULL && !nullOnUnknown) {
		return "";
	}
	return str;
}


SQLValues::TypeSwitcher::TypeSwitcher(
		TupleColumnType type1, TupleColumnType type2, bool nullableAlways) :
		type1_(type1),
		type2_(type2),
		nullableAlways_(nullableAlways),
		profile_(NULL) {
}

SQLValues::TypeSwitcher SQLValues::TypeSwitcher::withProfile(
		ValueProfile *profile) const {
	TypeSwitcher dest = *this;
	dest.profile_ = profile;
	return dest;
}

SQLValues::TypeSwitcher SQLValues::TypeSwitcher::toNonNullable() const {
	return TypeSwitcher(
			TypeUtils::toNonNullable(type1_),
			TypeUtils::toNonNullable(type2_),
			false);
}

SQLValues::TypeSwitcher SQLValues::TypeSwitcher::toNullableAlways() const {
	return TypeSwitcher(
			TypeUtils::toNullable(type1_),
			TypeUtils::toNullable(type2_),
			true);
}

bool SQLValues::TypeSwitcher::isAny() const {
	return TypeUtils::isAny(getMaskedType1());
}

TupleColumnType SQLValues::TypeSwitcher::getMaskedType1() const {
	TupleColumnType type;
	if (TypeUtils::isAny(type1_) && !TypeUtils::isNull(type2_)) {
		type = type2_;
	}
	else {
		type = type1_;
	}
	return TypeUtils::toNonNullable(type);
}

TupleColumnType SQLValues::TypeSwitcher::getMaskedType2() const {
	TupleColumnType type;
	if (TypeUtils::isAny(type2_)) {
		type = type1_;
	}
	else {
		type = type2_;
	}
	return TypeUtils::toNonNullable(type);
}

bool SQLValues::TypeSwitcher::isNullable() const {
	if (isNullableAlways()) {
		return true;
	}

	bool anyFound = false;
	bool nonAnyFound = false;
	for (size_t i = 0; i < 2; i++) {
		const TupleColumnType type = (i <= 0 ? type1_ : type2_);
		if (TypeUtils::isNull(type)) {
			break;
		}
		else if (TypeUtils::isNullable(type)) {
			return true;
		}
		else if (TypeUtils::isAny(type)) {
			anyFound = true;
		}
		else {
			nonAnyFound = true;
		}
	}
	return (anyFound && nonAnyFound);
}

bool SQLValues::TypeSwitcher::isNullableAlways() const {
	return nullableAlways_;
}

bool SQLValues::TypeSwitcher::checkNullable(bool nullableSwitching) const {
	const bool nullable = isNullable();

	if (profile_ != NULL && nullableSwitching) {
		ProfileElement &elem = profile_->noNull_;
		elem.candidate_++;
		if (!nullable) {
			elem.target_++;
		}
	}

	return nullable;
}


const size_t SQLValues::ValueContext::Constants::DEFAULT_MAX_STRING_LENGTH =
		128 * 1024;

SQLValues::ValueContext::ValueContext(const Source &source) :
		alloc_(source.alloc_),
		varCxt_(source.varCxt_),
		extCxt_(source.extCxt_),
		maxStringLength_(0),
		timeZoneAssigned_(false) {
	if (varCxt_ == NULL) {
		localVarCxt_ = UTIL_MAKE_LOCAL_UNIQUE(localVarCxt_, VarContext);
		varCxt_ = localVarCxt_.get();
		varCxt_->setStackAllocator(alloc_);
	}
	if (alloc_ == NULL) {
		alloc_ = varCxt_->getStackAllocator();
	}
}

SQLValues::ValueContext::Source SQLValues::ValueContext::ofAllocator(
		util::StackAllocator &alloc) {
	return Source(&alloc, NULL, NULL);
}

SQLValues::ValueContext::Source SQLValues::ValueContext::ofVarContext(
		VarContext &varCxt, util::StackAllocator *alloc,
		bool noVarAllocAllowed) {
	if (varCxt.getVarAllocator() == NULL) {
		if (noVarAllocAllowed) {
			util::StackAllocator *foundAlloc = findAllocator(varCxt, alloc);
			if (foundAlloc != NULL) {
				return ofAllocator(*foundAlloc);
			}
		}
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return Source(alloc, &varCxt, NULL);
}

util::StackAllocator& SQLValues::ValueContext::getAllocator() {
	if (alloc_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *alloc_;
}

SQLValues::VarAllocator& SQLValues::ValueContext::getVarAllocator() {
	VarAllocator *varAlloc = varCxt_->getVarAllocator();
	if (varAlloc == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *varAlloc;
}

util::StdAllocator<void, void> SQLValues::ValueContext::getStdAllocator() {
	if (varCxt_->getVarAllocator() != NULL) {
		return util::StdAllocator<void, void>(getVarAllocator());
	}
	return util::StdAllocator<void, void>(getAllocator());
}

size_t SQLValues::ValueContext::getVarContextDepth() {
	VarContext &varCxt = getVarContext();
	size_t depth = 0;
	{
		TupleValueVarUtils::VarData *data= varCxt.getHead();
		while ((data = data->getRelated()) != NULL) {
			depth++;
		}
	}
	return depth;
}

int64_t SQLValues::ValueContext::getCurrentTimeMillis() {
	if (extCxt_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int64_t time = extCxt_->getCurrentTimeMillis();
	try {
		return SQLValues::ValueUtils::checkTimestamp(
				DateTimeElements(time)).toTimestamp(Types::TimestampTag());
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, e, "");
	}
}

util::TimeZone SQLValues::ValueContext::getTimeZone() {
	if (timeZoneAssigned_) {
		return timeZone_;
	}

	if (extCxt_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return extCxt_->getTimeZone();
}

void SQLValues::ValueContext::setTimeZone(const util::TimeZone *zone) {
	if (zone == NULL) {
		timeZone_ = util::TimeZone();
		timeZoneAssigned_ = false;
	}
	else {
		timeZone_ = *zone;
		timeZoneAssigned_ = true;
	}
}

util::StackAllocator* SQLValues::ValueContext::findAllocator(
		VarContext &varCxt, util::StackAllocator *alloc) {
	if (alloc != NULL) {
		return alloc;
	}
	return varCxt.getStackAllocator();
}

size_t SQLValues::ValueContext::resolveMaxStringLength() {
	size_t len;
	do {
		if (extCxt_ != NULL) {
			len = extCxt_->findMaxStringLength();
			if (len != 0) {
				break;
			}
		}

		len = Constants::DEFAULT_MAX_STRING_LENGTH;
	}
	while (false);

	maxStringLength_ = len;
	return len;
}

SQLValues::ValueContext::Source::Source(
		util::StackAllocator *alloc, VarContext *varCxt,
		ExtValueContext *extCxt) :
		alloc_(alloc),
		varCxt_(varCxt),
		extCxt_(extCxt) {
}


SQLValues::ProfileElement::ProfileElement() :
		candidate_(0),
		target_(0) {
}

void SQLValues::ProfileElement::merge(const ProfileElement &src) {
	candidate_ += src.candidate_;
	target_ += src.target_;
}


SQLValues::ValueHolder::ValueHolder(SQLValues::ValueContext &valueCxt) :
		varCxt_(valueCxt.getVarContext()) {
}

SQLValues::ValueHolder::~ValueHolder() {
	reset();
}

void SQLValues::ValueHolder::assign(TupleValue &value) {
	reset();
	value_ = value;
	value = TupleValue();
}

TupleValue SQLValues::ValueHolder::release() {
	TupleValue ret = value_;
	value_ = TupleValue();
	return ret;
}

void SQLValues::ValueHolder::reset() {
	if (isDeallocatableContext(varCxt_)) {
		ValueContext cxt(ValueContext::ofVarContext(varCxt_));
		ValueUtils::destroyValue(cxt, value_);
	}
	value_ = TupleValue();
}

bool SQLValues::ValueHolder::isDeallocatableContext(VarContext &varCxt) {
	return (varCxt.getVarAllocator() != NULL);
}

bool SQLValues::ValueHolder::isDeallocatableValueType(TupleColumnType type) {
	assert(!TypeUtils::isAny(type) && !TypeUtils::isNullable(type));
	return !TypeUtils::isNull(type) && !TypeUtils::isNormalFixed(type);
}


SQLValues::ValueSetHolder::ValueSetHolder(SQLValues::ValueContext &valueCxt) :
		varCxt_(valueCxt.getVarContext()),
		values_(valueCxt.getStdAllocator()) {
}

SQLValues::ValueSetHolder::~ValueSetHolder() {
	reset();
}

void SQLValues::ValueSetHolder::add(TupleValue &value) {
	if (!ValueHolder::isDeallocatableContext(varCxt_) ||
			!ValueHolder::isDeallocatableValueType(value.getType())) {
		return;
	}
	ValueContext cxt(ValueContext::ofVarContext(varCxt_));
	ValueHolder holder(cxt);
	values_.push_back(TupleValue());
	values_.back() = holder.release();
}

void SQLValues::ValueSetHolder::reset() {
	if (!ValueHolder::isDeallocatableContext(varCxt_)) {
		return;
	}
	ValueContext cxt(ValueContext::ofVarContext(varCxt_));
	ValueHolder holder(cxt);
	while (!values_.empty()) {
		holder.assign(values_.back());
		values_.pop_back();
	}
}

SQLValues::VarContext& SQLValues::ValueSetHolder::getVarContext() {
	return varCxt_;
}


bool SQLValues::Decremental::Utils::isSupported(TupleColumnType type) {
	switch (TypeUtils::toNonNullable(type)) {
	case TupleTypes::TYPE_LONG:
		return true;
	case TupleTypes::TYPE_DOUBLE:
		return true;
	default:
		return false;
	}
}

uint32_t SQLValues::Decremental::Utils::getFixedSize(TupleColumnType type) {
	switch (TypeUtils::toNonNullable(type)) {
	case TupleTypes::TYPE_LONG:
		return sizeof(AsLong);
	case TupleTypes::TYPE_DOUBLE:
		return sizeof(AsDouble);
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLValues::Decremental::Utils::assignInitialValue(
		TupleColumnType type, void *addr) {
	switch (TypeUtils::toNonNullable(type)) {
	case TupleTypes::TYPE_LONG:
		*static_cast<AsLong*>(addr) = AsLong();
		break;
	case TupleTypes::TYPE_DOUBLE:
		*static_cast<AsDouble*>(addr) = AsDouble();
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}


SQLValues::SummaryColumn::SummaryColumn(const Source &src) :
		tupleColumn_(src.tupleColumn_),
		offset_(src.offset_),
		nullsOffset_(src.nullsOffset_),
		nullsMask_(src.nullsMask_),
		pos_(src.pos_),
		ordering_(src.ordering_) {
}


SQLValues::SummaryColumn::Source::Source() :
		type_(TupleTypes::TYPE_NULL),
		offset_(-1),
		nullsOffset_(-1),
		nullsMask_(0),
		pos_(-1),
		ordering_(-1),
		tupleColumnPos_(-1) {
}


SQLValues::CompColumn::CompColumn() :
		type_(TupleTypes::TYPE_NULL),
		columnPos1_(std::numeric_limits<uint32_t>::max()),
		columnPos2_(std::numeric_limits<uint32_t>::max()),
		ascending_(true),
		eitherUnbounded_(false),
		lowerUnbounded_(false),
		eitherExclusive_(false),
		lowerExclusive_(false),
		ordering_(true) {
}

void SQLValues::CompColumn::setSummaryColumn(
		const SummaryColumn &column, bool first) {
	(first ? summaryColumn1_ : summaryColumn2_) = column;
}

bool SQLValues::CompColumn::isColumnPosAssigned(bool first) const {
	return ((first ? columnPos1_ : columnPos2_) !=
			std::numeric_limits<uint32_t>::max());
}

uint32_t SQLValues::CompColumn::getColumnPos(bool first) const {
	assert(isColumnPosAssigned(first));
	return (first ? columnPos1_ : columnPos2_);
}

void SQLValues::CompColumn::setColumnPos(uint32_t columnPos, bool first) {
	(first ? columnPos1_ : columnPos2_) = columnPos;
}

TupleColumnType SQLValues::CompColumn::getType() const {
	return type_;
}

void SQLValues::CompColumn::setType(TupleColumnType type) {
	type_ = type;
}

void SQLValues::CompColumn::setAscending(bool ascending) {
	ascending_ = ascending;
}

void SQLValues::CompColumn::setEitherUnbounded(bool unbounded) {
	eitherUnbounded_ = unbounded;
}

void SQLValues::CompColumn::setLowerUnbounded(bool unbounded) {
	lowerUnbounded_ = unbounded;
}

void SQLValues::CompColumn::setEitherExclusive(bool exclusive) {
	eitherExclusive_ = exclusive;
}

void SQLValues::CompColumn::setLowerExclusive(bool exclusive) {
	lowerExclusive_ = exclusive;
}

bool SQLValues::CompColumn::isOrdering() const {
	return ordering_;
}

void SQLValues::CompColumn::setOrdering(bool ordering) {
	ordering_ = ordering;
}

SQLValues::CompColumn SQLValues::CompColumn::toSingleSide(bool first) const {
	CompColumn dest;

	dest.summaryColumn1_ = (first ? summaryColumn1_ : summaryColumn2_);
	dest.summaryColumn2_ = dest.summaryColumn1_;

	dest.columnPos1_ = (first ? columnPos1_ : columnPos2_);
	dest.columnPos2_ = dest.columnPos1_;

	dest.type_ = dest.getTupleColumn1().getType();
	dest.ordering_ = ordering_;

	return dest;
}

SQLValues::CompColumn SQLValues::CompColumn::toReversedSide() const {
	CompColumn dest = *this;

	dest.setSummaryColumn(getSummaryColumn(util::TrueType()), false);
	dest.setSummaryColumn(getSummaryColumn(util::FalseType()), true);
	dest.setColumnPos(getColumnPos(true), false);
	dest.setColumnPos(getColumnPos(false), true);

	if (isEitherUnbounded()) {
		dest.setLowerUnbounded(!isLowerUnbounded());
	}
	if (isEitherExclusive()) {
		dest.setLowerExclusive(!isLowerExclusive());
	}

	return dest;
}


SQLValues::ArrayTuple::ArrayTuple(util::StackAllocator &alloc) :
		valueList_(alloc) {
}

SQLValues::ArrayTuple::~ArrayTuple() {
	try {
		clear();
	}
	catch (...) {
	}
}

void SQLValues::ArrayTuple::clear() {
	resize(0);
}

void SQLValues::ArrayTuple::assign(ValueContext &cxt, const ArrayTuple &tuple) {
	const size_t size = tuple.valueList_.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		set(i, cxt, tuple.get(i));
	}
}

void SQLValues::ArrayTuple::assign(
		ValueContext &cxt, const ColumnTypeList &typeList) {
	const size_t size = typeList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		set(i, cxt, ValueUtils::createEmptyValue(cxt, typeList[i]));
	}
}

void SQLValues::ArrayTuple::assign(
		ValueContext &cxt, const ArrayTuple &tuple,
		const CompColumnList &columnList) {
	const size_t size = columnList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		set(i, cxt, tuple.get(columnList[i].getColumnPos(true)));
	}
}

void SQLValues::ArrayTuple::assign(
		ValueContext &cxt, const ReadableTuple &tuple,
		const CompColumnList &columnList) {
	const size_t size = columnList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		set(i, cxt, tuple.get(columnList[i].getTupleColumn1()));
	}
}

void SQLValues::ArrayTuple::assign(
		ValueContext &cxt, const ReadableTuple &tuple,
		const CompColumnList &columnList,
		const TupleColumnList &inColumnList) {
	const size_t size = columnList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		const CompColumn &compColumn = columnList[i];
		const TupleColumnType compType =
				TypeUtils::toNonNullable(compColumn.getType());

		TupleValue value = tuple.get(inColumnList[i]);
		if (value.getType() != compType &&
				!TypeUtils::isNull(value.getType())) {
			value = ValuePromoter(compType)(&cxt, value);
		}
		set(i, cxt, value);
	}
}

TupleValue SQLValues::ArrayTuple::get(size_t index) const {
	const TypeSwitcher switcher(toColumnType(
			static_cast<TupleColumnType>(getEntry(index).type_)));
	EntryGetter op(*this);
	return switcher(op, index);
}

void SQLValues::ArrayTuple::set(
		size_t index, ValueContext &cxt, const TupleValue &value) {
	const TypeSwitcher switcher(toColumnType(value.getType()));
	EntrySetter op(*this);
	switcher(op, index, cxt, value);
}

void SQLValues::ArrayTuple::resize(size_t size) {
	const size_t lastSize = valueList_.size();
	if (lastSize <= size) {
		if (lastSize < size) {
			valueList_.resize(size);
		}
		return;
	}

	while (valueList_.size() > size) {
		clear(valueList_.back());
		valueList_.pop_back();
	}
}

void SQLValues::ArrayTuple::clear(ValueEntry &entry) {
	if (entry.data_.ptr_ != NULL) {
		const TupleColumnType type = static_cast<TupleColumnType>(entry.type_);
		if (TypeUtils::isLob(type)) {
			ValueContext cxt(ValueContext::ofVarContext(*entry.data_.varCxt_));
			TupleValue dest(entry.value_.var_, type);
			ValueUtils::destroyValue(cxt, dest);

			entry.data_.ptr_ = NULL;
		}
	}
	entry.type_ = TupleTypes::TYPE_NULL;
}

uint8_t* SQLValues::ArrayTuple::prepareVarData(
		ValueEntry &entry, uint32_t size) {
	assert(size > 0);
	assert(entry.type_ == TupleTypes::TYPE_NULL ||
			TypeUtils::isSingleVarOrLarge(
					static_cast<TupleColumnType>(entry.type_)));

	VarData *&varData = entry.data_.var_;
	if (varData == NULL) {
		util::StackAllocator &alloc = *valueList_.get_allocator().base();
		varData = ALLOC_NEW(alloc) VarData(alloc);
	}

	if (size > varData->size()) {
		varData->resize(size);
	}

	return &(*varData)[0];
}

TupleColumnType SQLValues::ArrayTuple::toColumnType(TupleColumnType type) {
	if (TypeUtils::isNull(type)) {
		return TupleTypes::TYPE_ANY;
	}
	return type;
}


SQLValues::ArrayTuple::Assigner::Assigner(
		util::StackAllocator &alloc, const CompColumnList &columnList,
		const TupleColumnList *inColumnList) :
		columnList_(columnList),
		inColumnList_(inColumnList) {
	static_cast<void>(alloc);
}


SQLValues::ArrayTuple::EmptyAssigner::EmptyAssigner(
		ValueContext &cxt, const ColumnTypeList &typeList) :
		typeList_(typeList) {
	for (ColumnTypeList::const_iterator it = typeList.begin();
			it != typeList.end(); ++it) {
		if (!TypeUtils::isNullable(*it) && !TypeUtils::isAny(*it) &&
				!TypeUtils::isNormalFixed(*it)) {
			return;
		}
	}

	emptyTuple_ = UTIL_MAKE_LOCAL_UNIQUE(
			emptyTuple_, ArrayTuple, cxt.getAllocator());
	emptyTuple_->assign(cxt, typeList_);
}


TupleValue SQLValues::SummaryTuple::getValue(
		const SummaryColumn &column) const {
	TypeSwitcher typeSwitcher(TypeUtils::setNullable(
			column.getTupleColumn().getType(), (column.getNullsOffset() >= 0)));
	return typeSwitcher.get<const FieldGetter>()(FieldGetter(*this, column));
}

void SQLValues::SummaryTuple::initializeBodyDetail(
		SummaryTupleSet &tupleSet, TupleListReader *reader) {
	if (reader != NULL && tupleSet.isReadableTupleAvailable()) {
		const int32_t offset = SummaryTupleSet::getDefaultReadableTupleOffset();
		if (offset >= 0) {
			*static_cast<ReadableTuple*>(getField(offset)) = reader->get();
		}
	}
}

void SQLValues::SummaryTuple::swapValue(
		SummaryTupleSet &tupleSet, SummaryTuple &tuple1, uint32_t pos1,
		SummaryTuple &tuple2, uint32_t pos2, uint32_t count) {
	const ColumnList &totalColumnList = tupleSet.getTotalColumnList();

	assert(pos1 + count <= totalColumnList.size());
	assert(pos2 + count <= totalColumnList.size());

	uint64_t localData[2];
	void *localAddr = &localData;
	for (uint32_t i = 0; i < count; i++) {
		const SummaryColumn &column1 = totalColumnList[pos1 + i];
		const SummaryColumn &column2 = totalColumnList[pos2 + i];

		if (TypeUtils::isAny(column1.getTupleColumn().getType())) {
			continue;
		}

		void *field1 = tuple1.getField(column1.getOffset());
		void *field2 = tuple2.getField(column2.getOffset());

		const size_t size1 = tupleSet.getFixedFieldSize(pos1 + i);
		const size_t size2 = tupleSet.getFixedFieldSize(pos2 + i);
		if (size1 != size2 || size1 > sizeof(localData) ||
				column1.getTupleColumn().getType() !=
				column2.getTupleColumn().getType()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		const bool null1 = tuple1.isNull(column1);
		const bool null2 = tuple2.isNull(column2);

		if (null1) {
			if (null2) {
				continue;
			}
			tuple1.setNullDetail(column1, false);
			tuple2.setNullDetail(column2, true);
			memcpy(field1, field2, size1);
		}
		else if (null2) {
			tuple1.setNullDetail(column1, true);
			tuple2.setNullDetail(column2, false);
			memcpy(field2, field1, size1);
		}
		else {
			memcpy(localAddr, field1, size1);
			memcpy(field1, field2, size1);
			memcpy(field2, localAddr, size1);
		}
	}
}


SQLValues::SummaryTupleSet::SummaryTupleSet(
		ValueContext &cxt, SummaryTupleSet *parent) :
		alloc_(cxt.getAllocator()),
		varCxt_(cxt.getVarContext()),
		poolAlloc_(varCxt_.getVarAllocator()),
		parent_(parent),
		columnSourceList_(alloc_),
		keySourceList_(alloc_),
		subInputInfoList_(alloc_),
		totalColumnList_(alloc_),
		readerColumnList_(alloc_),
		modifiableColumnList_(alloc_),
		fieldReaderList_(alloc_),
		subFieldReaderList_(alloc_),
		getterFuncList_(alloc_),
		setterFuncList_(alloc_),
		promotedSetterFuncList_(alloc_),
		readableTupleRequired_(false),
		keyNullIgnorable_(false),
		orderedDigestRestricted_(false),
		readerColumnsDeep_(false),
		headNullAccessible_(false),
		valueDecremental_(false),
		columnsCompleted_(false),
		bodySize_(0),
		initialBodyImage_(NULL),
		initialBodyOffset_(0),
		restBodyData_(NULL),
		restBodySize_(0),
		lobLink_(NULL),
		varDataLimit_(0) {
	initializePool(cxt);
}

SQLValues::SummaryTupleSet::~SummaryTupleSet() {
	deallocateAll();
}

void SQLValues::SummaryTupleSet::addColumn(
		TupleColumnType type, const uint32_t *readerColumnPos) {
	assert(!columnsCompleted_);
	SummaryColumn::Source src;

	src.type_ = type;
	if (readerColumnPos != NULL) {
		src.tupleColumnPos_ = static_cast<int32_t>(*readerColumnPos);
	}

	columnSourceList_.push_back(src);
}

void SQLValues::SummaryTupleSet::addKey(
		uint32_t readerColumnPos, bool ordering, bool rotating) {
	assert(!columnsCompleted_);
	SummaryColumn::Source src;

	src.tupleColumnPos_ = static_cast<int32_t>(readerColumnPos);
	if (ordering) {
		if (rotating && keySourceList_.empty()) {
			src.ordering_ = 1;
		}
		else {
			src.ordering_ = 0;
		}
	}
	else {
		src.ordering_ = -1;
	}

	keySourceList_.push_back(src);
}

void SQLValues::SummaryTupleSet::setNullKeyIgnorable(bool ignorable) {
	assert(!columnsCompleted_);
	keyNullIgnorable_ = ignorable;
}

void SQLValues::SummaryTupleSet::setOrderedDigestRestricted(bool restricted) {
	assert(!columnsCompleted_);
	orderedDigestRestricted_ = restricted;
}

void SQLValues::SummaryTupleSet::setReaderColumnsDeep(bool deep) {
	assert(!columnsCompleted_);
	readerColumnsDeep_ = deep;
}

void SQLValues::SummaryTupleSet::setHeadNullAccessible(bool accessible) {
	assert(!columnsCompleted_);
	headNullAccessible_ = accessible;
}

void SQLValues::SummaryTupleSet::setValueDecremental(bool decremental) {
	assert(!columnsCompleted_);
	valueDecremental_ = decremental;
}

bool SQLValues::SummaryTupleSet::isDeepReaderColumnSupported(
		TupleColumnType type) {
	return (TypeUtils::isAny(type) || TypeUtils::isSomeFixed(type) ||
			TypeUtils::isSingleVar(type));
}

void SQLValues::SummaryTupleSet::addReaderColumnList(
		const ColumnTypeList &typeList) {
	uint32_t pos = 0;
	for (ColumnTypeList::const_iterator it = typeList.begin();
			it != typeList.end(); ++it, ++pos) {
		addColumn(*it, &pos);
	}
}

void SQLValues::SummaryTupleSet::addReaderColumnList(	
		const TupleColumnList &columnList) {
	uint32_t pos = 0;
	for (TupleColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it, ++pos) {
		addColumn(it->getType(), &pos);
	}
}

void SQLValues::SummaryTupleSet::addColumnList(const SummaryTupleSet &src) {
	assert(!columnsCompleted_);

	columnSourceList_.insert(
			columnSourceList_.end(),
			src.columnSourceList_.begin(), src.columnSourceList_.end());

	keySourceList_.insert(
			keySourceList_.end(),
			src.keySourceList_.begin(), src.keySourceList_.end());
}

void SQLValues::SummaryTupleSet::addKeyList(
		const CompColumnList &keyList, bool first, bool rotating) {
	for (CompColumnList::const_iterator it = keyList.begin();
			it != keyList.end(); ++it) {
		const SQLValues::CompColumn &column = *it;
		assert(column.isColumnPosAssigned(true));
		const bool second = column.isColumnPosAssigned(false);

		const uint32_t keyPos = column.getColumnPos(first || !second);
		addKey(keyPos, (rotating || column.isOrdering()), rotating);
	}
}

void SQLValues::SummaryTupleSet::addSubReaderColumnList(
		uint32_t index, const TupleColumnList &columnList) {
	while (index >= subInputInfoList_.size()) {
		subInputInfoList_.push_back(ColumnTypeList(alloc_));
	}

	ColumnTypeList &typeList = subInputInfoList_[index];
	for (TupleColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		typeList.push_back(it->getType());
	}
}

void SQLValues::SummaryTupleSet::completeColumns() {
	assert(!columnsCompleted_);

	const uint32_t columnCount =
			static_cast<uint32_t>(columnSourceList_.size());

	uint32_t fixedBodySize = 0;
	util::Vector<uint32_t> bodySizeList(columnCount, 0, alloc_);
	bool readableTupleRequired = false;

	util::Vector<bool> nullableList(columnCount, false, alloc_);
	util::Vector<int32_t> posListByReader(alloc_);
	ColumnTypeList readerColumnTypeList(alloc_);

	int32_t pos = 0;
	int32_t firstModPos = -1;
	for (ColumnSourceList::iterator it = columnSourceList_.begin();
			it != columnSourceList_.end(); ++it, ++pos) {
		it->pos_ = pos;
		const int32_t readerPos = it->tupleColumnPos_;

		const TupleColumnType type = it->type_;
		uint32_t fieldSize = 0;
		if (valueDecremental_ && readerPos < 0 &&
				Decremental::Utils::isSupported(type)) {
			fieldSize = Decremental::Utils::getFixedSize(type);
		}
		else if (TypeUtils::isSomeFixed(type)) {
			fieldSize = static_cast<uint32_t>(TypeUtils::getFixedSize(type));
		}
		else if (!TypeUtils::isAny(type)) {
			if (TypeUtils::isLob(type) && readerPos >= 0) {
				readableTupleRequired = true;
			}
			else {
				fieldSize = sizeof(void*);
			}
		}

		fixedBodySize += fieldSize;
		bodySizeList[pos] = fieldSize;
		nullableList[pos] = TypeUtils::isNullable(type);

		if (readerPos >= 0) {
			const size_t posIndex = static_cast<size_t>(readerPos);
			if (posIndex >= posListByReader.size()) {
				const size_t readerColumnCount = posIndex + 1;
				posListByReader.resize(readerColumnCount, -1);
				readerColumnTypeList.resize(
						readerColumnCount,
						static_cast<TupleColumnType>(TupleTypes::TYPE_NULL));
			}
			posListByReader[posIndex] = static_cast<int32_t>(pos);
			readerColumnTypeList[posIndex] = type;
		}
		else if (firstModPos < 0) {
			firstModPos = pos;
		}
	}

	assert(
			std::find(posListByReader.begin(), posListByReader.end(), -1) ==
			posListByReader.end());

	TupleColumnList tupleColumnList(alloc_);
	if (!posListByReader.empty()) {
		TupleList::Info tupleInfo;
		SQLValues::TypeUtils::setUpTupleInfo(
				tupleInfo, &readerColumnTypeList[0], readerColumnTypeList.size());

		tupleColumnList.resize(readerColumnTypeList.size());
		tupleInfo.getColumns(&tupleColumnList[0], tupleColumnList.size());
	}

	typedef util::Vector<TupleColumnList> AllTupleColumnList;
	AllTupleColumnList subColumnList(alloc_);
	for (TupleInfoList::const_iterator it = subInputInfoList_.begin();
			it != subInputInfoList_.end(); ++it) {
		const ColumnTypeList &typeList = *it;
		TupleColumnList columnList(alloc_);
		if (!typeList.empty()) {
			assert(typeList.size() == readerColumnTypeList.size());
			TupleList::Info tupleInfo;
			SQLValues::TypeUtils::setUpTupleInfo(
					tupleInfo, &typeList[0], typeList.size());

			columnList.resize(typeList.size());
			tupleInfo.getColumns(&columnList[0], columnList.size());
		}
		subColumnList.push_back(columnList);
	}
	subFieldReaderList_.assign(subInputInfoList_.size(), FieldReaderList(alloc_));

	int32_t headKeyPos = -1;
	for (ColumnSourceList::iterator it = keySourceList_.begin();
			it != keySourceList_.end(); ++it) {
		const int32_t readerPos = it->tupleColumnPos_;
		if (readerPos < 0 ||
				static_cast<uint32_t>(readerPos) >= posListByReader.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		const int32_t pos = posListByReader[readerPos];
		const bool onHead = (it == keySourceList_.begin());

		const int32_t ordering = it->ordering_;
		columnSourceList_[pos].ordering_ = ordering;

		const TupleColumnType type = readerColumnTypeList[pos];
		if (onHead && (ordering >= 0 || (keySourceList_.size() <= 1 &&
				(TypeUtils::isNormalFixed(type) || TypeUtils::isAny(type)))) &&
				!orderedDigestRestricted_) {
			headKeyPos = pos;

			if (!TypeUtils::isLargeFixed(type)) {
				fixedBodySize -= bodySizeList[pos];
				bodySizeList[pos] = 0;
			}
		}

		if (keyNullIgnorable_ && (onHead || ordering < 0)) {
			nullableList[pos] = false;
		}
	}

	uint32_t fixedBodyOffset = 0;
	if (readableTupleRequired) {
		fixedBodyOffset += static_cast<uint32_t>(sizeof(ReadableTuple));
	}

	const bool nullsRequired =
			(std::find(nullableList.begin(), nullableList.end(), true) !=
			nullableList.end());
	const uint32_t nullsUnitBits =
			static_cast<uint32_t>(sizeof(NullsUnitType) * CHAR_BIT);

	int32_t nullsStartOffset = -1;
	uint32_t nullsBytesSize = 0;
	if (nullsRequired) {
		nullsStartOffset = static_cast<int32_t>(fixedBodyOffset + fixedBodySize);
		nullsBytesSize = static_cast<uint32_t>(sizeof(NullsUnitType) *
				((columnCount + nullsUnitBits - 1) / nullsUnitBits));
	}

	const uint32_t bodySize = fixedBodyOffset + fixedBodySize + nullsBytesSize;

	void *initialBodyImage = NULL;
	uint32_t initialBodyOffset = bodySize;
	if (nullsRequired || firstModPos >= 0) {
		initialBodyOffset = fixedBodyOffset;
		for (ColumnSourceList::iterator it = columnSourceList_.begin();
				it != columnSourceList_.end(); ++it) {
			if (firstModPos >= it->pos_) {
				break;
			}
			initialBodyOffset += bodySizeList[it->pos_];
		}
		initialBodyImage = alloc_.allocate(bodySize - initialBodyOffset);

		const uint32_t nullsOffset = fixedBodyOffset + fixedBodySize;

		assert(nullsOffset >= initialBodyOffset);
		const uint32_t modSize = nullsOffset - initialBodyOffset;

		memset(initialBodyImage, 0x0, modSize);
		memset(
				static_cast<uint8_t*>(initialBodyImage) + modSize,
				0xff, nullsBytesSize);

		uint32_t offset = 0;
		for (ColumnSourceList::iterator it = columnSourceList_.begin();
				it != columnSourceList_.end(); ++it) {
			if (firstModPos >= it->pos_) {
				const TupleColumnType type = it->type_;
				if (valueDecremental_ && it->tupleColumnPos_ < 0 &&
						Decremental::Utils::isSupported(type)) {
					Decremental::Utils::assignInitialValue(
							type,
							static_cast<uint8_t*>(initialBodyImage) + offset);
				}
				else if (!TypeUtils::isSomeFixed(type) &&
						!TypeUtils::isAny(type) &&
						bodySizeList[it->pos_] > 0) {
					const void *data = NULL;
					if (!nullableList[it->pos_]) {
						ValueContext cxt(ValueContext::ofAllocator(alloc_));
						TupleValue value =
								ValueUtils::createEmptyValue(cxt, type);
						if (TypeUtils::isLob(type)) {
							LobLink *link = static_cast<LobLink*>(
									alloc_.allocate(sizeof(LobLink)));
							link->next_ = NULL;
							link->prev_ = NULL;
							link->lob_ = value.getRawData();
							link->type_ = TypeUtils::toNonNullable(type);
							data = link;
						}
						else {
							data = value.getRawData();
						}
					}
					assert(sizeof(data) == bodySizeList[it->pos_]);
					memcpy(
							static_cast<uint8_t*>(initialBodyImage) + offset,
							&data, sizeof(data));
				}
			}

			offset += bodySizeList[it->pos_];
		}
	}

	int32_t nextOffset = static_cast<int32_t>(fixedBodyOffset);
	for (ColumnSourceList::iterator it = columnSourceList_.begin();
			it != columnSourceList_.end(); ++it) {
		const uint32_t pos = it->pos_;
		it->offset_ = (bodySizeList[pos] > 0 ? nextOffset : -1);

		const bool nullable = nullableList[pos];
		it->nullsOffset_ = (nullable ?
				static_cast<int32_t>(
						nullsStartOffset +
						sizeof(NullsUnitType) * (pos / nullsUnitBits)) :
				-1);
		it->nullsMask_ = (nullable ? (1U << (pos % nullsUnitBits)) : 0);

		if (it->tupleColumnPos_ >= 0) {
			it->tupleColumn_ = tupleColumnList[pos];
		}
		else {
			it->tupleColumn_.offset_ = std::numeric_limits<uint32_t>::max();
			it->tupleColumn_.type_ = it->type_;
			it->tupleColumn_.pos_ = std::numeric_limits<uint16_t>::max();
		}

		SummaryColumn column(*it);

		const TupleColumnType resolvedType =
				TypeUtils::setNullable(it->type_, nullable);
		TypeSwitcher typeSwitcher(resolvedType);

		FieldGetterFunc getter =
				typeSwitcher.get<const SummaryTuple::FieldGetter>();
		FieldSetterFunc setter = NULL;
		FieldPromotedSetterFunc promotedSetter = NULL;
		if (it->tupleColumnPos_ >= 0) {
			readerColumnList_.push_back(column);
			const bool onBody = (it->offset_ >= 0);
			const bool forLob = TypeUtils::isLob(it->type_);
			const bool headNullable = (headNullAccessible_ &&
					!onBody && !forLob && it->nullsOffset_ >= 0);
			if (onBody || forLob || headNullable) {
				FieldReaderFunc func = resolveFieldReaderFunc(
						resolvedType, resolvedType, !readerColumnsDeep_,
						headNullable);
				fieldReaderList_.push_back(FieldReaderEntry(column, func));

				for (AllTupleColumnList::const_iterator subIt = subColumnList.begin();
						subIt != subColumnList.end(); ++subIt) {
					if (pos >= subIt->size()) {
						continue;
					}

					SummaryColumn::Source subSource = *it;
					subSource.tupleColumn_ = (*subIt)[pos];
					SummaryColumn subColumn(subSource);
					const TupleColumnType subType =
							subColumn.getTupleColumn().getType();

					if (TypeUtils::isAny(subType)) {
						continue;
					}

					FieldReaderFunc subFunc = resolveFieldReaderFunc(
							resolvedType, subType, !readerColumnsDeep_,
							headNullable);
					subFieldReaderList_[subIt - subColumnList.begin()].push_back(
							FieldReaderEntry(subColumn, subFunc));
				}
			}
		}
		else {
			modifiableColumnList_.push_back(column);
			setter = typeSwitcher.get< const SummaryTuple::FieldSetter<false> >();
			promotedSetter =
					typeSwitcher.get< const SummaryTuple::FieldSetter<true> >();
		}
		totalColumnList_.push_back(column);

		getterFuncList_.push_back(getter);
		setterFuncList_.push_back(setter);
		promotedSetterFuncList_.push_back(promotedSetter);

		if (static_cast<int32_t>(pos) == headKeyPos) {
			readerHeadColumn_ = column;
		}

		nextOffset += bodySizeList[pos];
	}

	readableTupleRequired_ = readableTupleRequired;
	bodySize_ = bodySize;
	initialBodyImage_ = initialBodyImage;
	initialBodyOffset_ = initialBodyOffset;
	columnsCompleted_ = true;
}

bool SQLValues::SummaryTupleSet::isColumnsCompleted() {
	return columnsCompleted_;
}

void SQLValues::SummaryTupleSet::applyKeyList(
		CompColumnList &keyList, const bool *first) const {
	assert(columnsCompleted_);
	for (CompColumnList::iterator it = keyList.begin(); it != keyList.end(); ++it) {
		for (size_t i = 0; i < 2; i++) {
			const bool curFirst = (i == 0);
			if (first != NULL && !!(*first) != curFirst) {
				continue;
			}
			assert(it->isColumnPosAssigned(curFirst));
			it->setSummaryColumn(
					readerColumnList_[it->getColumnPos(curFirst)], curFirst);
		}
	}
}

const SQLValues::SummaryTupleSet::ColumnList&
SQLValues::SummaryTupleSet::getTotalColumnList() const {
	assert(columnsCompleted_);
	return totalColumnList_;
}

const SQLValues::SummaryTupleSet::ColumnList&
SQLValues::SummaryTupleSet::getReaderColumnList() const {
	assert(columnsCompleted_);
	return readerColumnList_;
}

const SQLValues::SummaryTupleSet::ColumnList&
SQLValues::SummaryTupleSet::getModifiableColumnList() const {
	assert(columnsCompleted_);
	return modifiableColumnList_;
}

void SQLValues::SummaryTupleSet::applyColumnList(
		const ColumnList &src, ColumnList &dest) {
	assert(dest.empty() || dest.size() == src.size());
	dest.assign(src.begin(), src.end());
}

size_t SQLValues::SummaryTupleSet::estimateTupleSize() const {
	assert(columnsCompleted_);
	const size_t headSize = static_cast<size_t>(
			std::max<uint64_t>(sizeof(int64_t) * 2, sizeof(SummaryTuple)));
	return headSize + bodySize_;
}

bool SQLValues::SummaryTupleSet::isReadableTupleAvailable() const {
	assert(columnsCompleted_);
	return readableTupleRequired_;
}

int32_t SQLValues::SummaryTupleSet::getDefaultReadableTupleOffset() {
	return 0;
}

size_t SQLValues::SummaryTupleSet::getFixedFieldSize(uint32_t index) {
	assert(columnsCompleted_);
	assert(index < totalColumnList_.size());

	const SummaryColumn &column = totalColumnList_[index];
	assert(column.getOffset() >= 0);

	const TupleColumnType type = column.getTupleColumn().getType();
	if (TypeUtils::isSomeFixed(type)) {
		return TypeUtils::getFixedSize(type);
	}
	else if (!TypeUtils::isAny(type)) {
		return sizeof(void*);
	}

	return 0;
}

void* SQLValues::SummaryTupleSet::duplicateVarData(const void *data) {
	assert(data != NULL);

	const uint32_t bodySize = TupleValueUtils::decodeVarSize(data);
	const uint32_t size =
			TupleValueUtils::getEncodedVarSize(bodySize) + bodySize;

	void *dest = allocateDetail(size, NULL);
	memcpy(dest, data, size);

	return dest;
}

void SQLValues::SummaryTupleSet::deallocateVarData(void *data) {
	if (data == NULL) {
		return;
	}

	const uint32_t bodySize = TupleValueUtils::decodeVarSize(data);
	const uint32_t size =
			TupleValueUtils::getEncodedVarSize(bodySize) + bodySize;

	deallocateDetail(data, size);
}

void SQLValues::SummaryTupleSet::appendVarData(
		void *&data, StringReader &reader) {
	assert(data != NULL);

	for (;;) {
		const uint32_t size = static_cast<uint32_t>(std::min<uint64_t>(
				reader.getNext(), std::numeric_limits<uint32_t>::max()));
		if (size <= 0) {
			break;
		}

		const bool limited = true;
		data = appendVarDataDetail(&reader.get(), size, data, limited);
		reader.step(size);
	}
}

void* SQLValues::SummaryTupleSet::duplicateLob(const TupleValue &value) {
	LobLink *link = static_cast<LobLink*>(allocateDetail(sizeof(LobLink), NULL));

	link->next_ = lobLink_;
	link->prev_ = &lobLink_;
	link->lob_ = NULL;
	link->tupleSet_ = this;

	link->type_ = TupleTypes::TYPE_NULL;
	link->small_ = false;

	if (link->next_ != NULL) {
		link->next_->prev_ = &link->next_;
	}
	(*link->prev_) = link;

	const TupleColumnType type = value.getType();
	if (checkLobSmall(value)) {
		LocalLobBuilder builder(*this, type);

		LobReader reader(value);
		builder.append(reader);

		link->lob_ = builder.release(link->small_);
	}
	else {
		link->lob_ = duplicateNormalLob(varCxt_, value);
	}
	assert(link->lob_ != NULL);
	link->type_ = type;

	return link;
}

void SQLValues::SummaryTupleSet::deallocateLob(void *data) {
	if (data == NULL) {
		return;
	}

	LobLink *link = static_cast<LobLink*>(data);
	assert(link->tupleSet_ == this);
	if (link->prev_ == NULL) {
		return;
	}

	void *const lob = link->lob_;

	const TupleColumnType type = link->type_;
	const bool small = link->small_;

	link->lob_ = NULL;
	link->type_ = TupleTypes::TYPE_NULL;
	if (link->next_ != NULL) {
		link->next_->prev_ = link->prev_;
	}
	(*link->prev_) = link->next_;
	link->tupleSet_ = NULL;
	deallocateDetail(link, sizeof(LobLink));

	if (lob == NULL) {
		return;
	}

	if (small) {
		deallocateSmallLob(lob);
	}
	else {
		deallocateNormalLob(varCxt_, lob, type);
	}
}

void SQLValues::SummaryTupleSet::appendLob(void *data, LobReader &reader) {
	if (data == NULL) {
		assert(false);
		return;
	}

	LobLink *link = static_cast<LobLink*>(data);
	assert(link->tupleSet_ == this);

	LocalLobBuilder builder(*this, link->type_);
	if (link->small_) {
		builder.assignSmall(link->lob_);
		link->small_ = false;
	}
	else {
		LobReader lastReader(toValueByNormalLob(link->lob_, link->type_));
		builder.append(lastReader);
		deallocateNormalLob(varCxt_, link->lob_, link->type_);
	}
	link->lob_ = NULL;

	builder.append(reader);
	link->lob_ = builder.release(link->small_);
	assert(link->lob_ != NULL);
}

TupleValue SQLValues::SummaryTupleSet::getLobValue(void *data) {
	LobLink *link = static_cast<LobLink*>(data);
	assert(link->lob_ != NULL);
	assert(link->tupleSet_ != NULL);

	if (link->small_) {
		return toValueBySmallLob(
				link->tupleSet_->varCxt_, link->lob_, link->type_);
	}

	return toValueByNormalLob(link->lob_, link->type_);
}

size_t SQLValues::SummaryTupleSet::getPoolCapacity() {
	if (poolAlloc_ == NULL) {
		return 0;
	}

	size_t capacity = 0;

	if (initialDataPool_.first != NULL) {
		capacity += (1U << INITIAL_POOL_DATA_BITS);
	}

	if (!fixedDataPool_->empty()) {
		capacity += fixedDataPool_->size() * (1U << getFixedSizeBits());
	}

	for (Pool::const_iterator it = largeDataPool_->begin();
			it != largeDataPool_->end(); ++it) {
		capacity += poolAlloc_->getElementCapacity(*it);
	}

	return capacity;
}

void SQLValues::SummaryTupleSet::clearAll() {
	if (poolAlloc_ == NULL) {
		return;
	}

	restBodyData_ = NULL;
	restBodySize_ = 0;

	while (lobLink_ != NULL) {
		deallocateLob(lobLink_);
	}

	const size_t fixedBits = getFixedSizeBits();
	for (size_t i = 0; i <= fixedBits; i++) {
		if (i >= varDataPool_->size()) {
			break;
		}
		(*varDataPool_)[i] = NULL;
	}

	initialDataPool_.second = false;

	if (!fixedDataPool_->empty()) {
		void *&dataHead = (*varDataPool_)[fixedBits];
		assert(dataHead == NULL);

		for (Pool::const_iterator it = fixedDataPool_->begin();
				it != fixedDataPool_->end(); ++it) {
			void *data = *it;
			if (data == NULL) {
				continue;
			}
			static_cast<PoolData*>(data)->next_ = dataHead;
			dataHead = data;
		}
	}
}

SQLValues::SummaryTupleSet::FieldReaderFunc
SQLValues::SummaryTupleSet::resolveFieldReaderFunc(
		TupleColumnType destType, TupleColumnType srcType, bool shallow,
		bool headNullable) {
	assert(shallow || isDeepReaderColumnSupported(destType));

	if (headNullable) {
		return resolveFieldReaderFuncBy<true, true>(destType, srcType);
	}
	else if (shallow || TypeUtils::isAny(destType) ||
			TypeUtils::isSomeFixed(destType)) {
		return resolveFieldReaderFuncBy<true, false>(destType, srcType);
	}
	else if (TypeUtils::isSingleVar(destType)) {
		return resolveFieldReaderFuncBy<false, false>(destType, srcType);
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

template<bool Shallow, bool HeadNullable>
SQLValues::SummaryTupleSet::FieldReaderFunc
SQLValues::SummaryTupleSet::resolveFieldReaderFuncBy(
		TupleColumnType destType, TupleColumnType srcType) {
	typedef typename util::BoolType<Shallow>::Result ShallowType;
	typedef typename util::BoolType<HeadNullable>::Result HeadNullableType;
	typedef typename SummaryTuple::FieldReaderVariantTraits<
			ShallowType, HeadNullableType> VariantTraitsType;

	const bool typeSpecific = !HeadNullableType::VALUE;
	const TypeSwitcher::TypeTarget typeTarget = (ShallowType::VALUE ?
			TypeSwitcher::TYPE_TARGET_ALL :
			TypeSwitcher::TYPE_TARGET_SINGLE_VAR);

	typedef TypeSwitcher::OpTraitsOptions<
			VariantTraitsType, 2, false, true, 1,
			typeSpecific, typeTarget> OptionsType;
	typedef TypeSwitcher::OpTraits<void, OptionsType> TraitsType;

	return TypeSwitcher(destType, srcType).getWith<
			const SummaryTuple::FieldReader, TraitsType>();
}

void SQLValues::SummaryTupleSet::initializePool(ValueContext &cxt) {
	if (poolAlloc_ == NULL) {
		return;
	}

	varDataPool_ = UTIL_MAKE_LOCAL_UNIQUE(varDataPool_, Pool, *poolAlloc_);
	fixedDataPool_ = UTIL_MAKE_LOCAL_UNIQUE(fixedDataPool_, Pool, *poolAlloc_);
	largeDataPool_ = UTIL_MAKE_LOCAL_UNIQUE(largeDataPool_, Pool, *poolAlloc_);

	varDataLimit_ = cxt.getMaxStringLength();
}

void SQLValues::SummaryTupleSet::deallocateAll() {
	clearAll();

	if (poolAlloc_ == NULL) {
		return;
	}

	while (!fixedDataPool_->empty()) {
		void *data = fixedDataPool_->back();
		if (data != NULL) {
			alloc_.base().deallocate(data);
		}
		fixedDataPool_->pop_back();
	}

	while (!largeDataPool_->empty()) {
		void *data = largeDataPool_->back();
		if (data != NULL) {
			poolAlloc_->deallocate(data);
		}
		largeDataPool_->pop_back();
	}

	{
		void *&data = initialDataPool_.first;
		if (data != NULL) {
			poolAlloc_->deallocate(data);
			data = NULL;
		}
	}
}

void* SQLValues::SummaryTupleSet::appendVarDataDetail(
		const void *src, uint32_t size, void *dest, bool limited) {
	const uint32_t destBodySize =
			(dest == NULL ? 0 : TupleValueUtils::decodeVarSize(dest));
	const uint32_t destHeadSize =
			TupleValueUtils::getEncodedVarSize(destBodySize);
	const uint32_t destSize = destHeadSize + destBodySize;

	const uint32_t nextBodysize = destBodySize + size;
	const uint32_t nextHeadSize =
			TupleValueUtils::getEncodedVarSize(nextBodysize);
	const uint64_t nextSize =
			static_cast<uint64_t>(nextHeadSize) + nextBodysize;

	if (limited) {
		StringBuilder::checkLimit(nextSize, varDataLimit_);
	}

	const uint32_t destCapacity = 1U << getSizeBits(destSize, true);
	void *next;
	if (dest == NULL || nextSize > destCapacity) {
		next = allocateDetail(static_cast<size_t>(nextSize), NULL);
		memcpy(
				static_cast<uint8_t*>(next) + nextHeadSize,
				static_cast<uint8_t*>(dest) + destHeadSize, destBodySize);
	}
	else {
		next = dest;
		if (nextHeadSize != destHeadSize) {
			memmove(
					static_cast<uint8_t*>(next) + nextHeadSize,
					static_cast<uint8_t*>(dest) + destHeadSize, destBodySize);
		}
	}
	memcpy(
			static_cast<uint8_t*>(next) + nextHeadSize + destBodySize,
			src, size);
	TupleValueUtils::encodeInt32(nextBodysize, static_cast<uint8_t*>(next));

	return next;
}

void SQLValues::SummaryTupleSet::reserveBody() {
	assert(columnsCompleted_);
	if (restBodySize_ >= bodySize_) {
		return;
	}

	const size_t allocSize = (restBodyData_ == NULL ?
			bodySize_ : static_cast<size_t>(
					std::max<uint64_t>(bodySize_, 1U << MAX_POOL_DATA_BITS)));
	restBodyData_ = allocateDetail(allocSize, &restBodySize_);

	assert(restBodySize_ >= bodySize_);
}

bool SQLValues::SummaryTupleSet::checkLobSmall(const TupleValue &value) {
	const uint32_t maxSize = getMaxSmallLobBodySize();

	LobReader baseReader(value);
	LimitedReader<LobReader, uint8_t> reader(baseReader, maxSize + 1);

	const uint64_t minBodySize =
			static_cast<uint64_t>(ValueUtils::partLength(baseReader));

	return (minBodySize <= maxSize);
}

TupleValue SQLValues::SummaryTupleSet::toValueBySmallLob(
		VarContext &varCxt, const void *lob, TupleColumnType type) {
	LobBuilder builder(varCxt, type, 0);

	const uint32_t bodySize = TupleValueUtils::decodeVarSize(lob);
	const uint32_t headSize =
			TupleValueUtils::getEncodedVarSize(bodySize);

	builder.append(static_cast<const uint8_t*>(lob) + headSize, bodySize);
	return builder.build();
}

void* SQLValues::SummaryTupleSet::duplicateNormalLob(
		VarContext &varCxt, const TupleValue &value) {
	ValueContext cxt(ValueContext::ofVarContext(varCxt));
	TupleValue dest = ValueUtils::duplicateValue(cxt, value);
	ValueUtils::moveValueToRoot(cxt, dest, cxt.getVarContextDepth(), 0);

	return dest.getRawData();
}

void SQLValues::SummaryTupleSet::deallocateNormalLob(
		VarContext &varCxt, void *lob, TupleColumnType type) {
	TupleValue value(lob, type);
	ValueContext cxt(ValueContext::ofVarContext(varCxt));
	ValueUtils::destroyValue(cxt, value);
}

TupleValue SQLValues::SummaryTupleSet::toValueByNormalLob(
		void *lob, TupleColumnType type) {
	assert(lob != NULL);
	return TupleValue(lob, type);
}

void* SQLValues::SummaryTupleSet::toNormalLobByBuilder(
		VarContext &varCxt, LobBuilder &builder) {
	ValueContext cxt(ValueContext::ofVarContext(varCxt));
	TupleValue dest = builder.build();
	ValueUtils::moveValueToRoot(cxt, dest, cxt.getVarContextDepth(), 0);

	return dest.getRawData();
}

void SQLValues::SummaryTupleSet::deallocateSmallLob(void *lob) {
	deallocateVarData(lob);
}

uint32_t SQLValues::SummaryTupleSet::getMaxSmallLobBodySize() {
	const uint32_t totalSize = 1U << SMALL_LOB_DATA_BITS;

	const uint32_t headSize = sizeof(uint32_t);
	return static_cast<uint32_t>(
			std::max<uint32_t>(totalSize, headSize)) - headSize;
}

void* SQLValues::SummaryTupleSet::allocateDetail(
		size_t size, size_t *availableSize) {
	if (parent_ != NULL) {
		return parent_->allocateDetail(size, availableSize);
	}

	if (poolAlloc_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const size_t fixedBits = getFixedSizeBits();
	const size_t initialBits = INITIAL_POOL_DATA_BITS;
	if (!initialDataPool_.second && initialBits != fixedBits) {
		if (initialBits >= varDataPool_->size()) {
			varDataPool_->resize(initialBits + 1);
		}
		if (initialDataPool_.first == NULL) {
			initialDataPool_.first = poolAlloc_->allocate(1U << initialBits);
		}
		static_cast<PoolData*>(initialDataPool_.first)->next_ = NULL;
		(*varDataPool_)[initialBits] = initialDataPool_.first;
		initialDataPool_.second = true;
	}

	const size_t minBits = getSizeBits(size, true);

	void *data = NULL;
	size_t bits = minBits;
	do {
		const size_t maxBits =
				static_cast<size_t>(std::max<uint64_t>(bits, fixedBits));
		if (maxBits >= varDataPool_->size()) {
			varDataPool_->resize(maxBits + 1);
		}

		if (bits <= maxBits) {
			for (;; bits++) {
				data = (*varDataPool_)[bits];
				if (data != NULL || bits >= maxBits) {
					break;
				}
			}
		}

		if (data == NULL) {
			const bool fixed = (bits == fixedBits);
			Pool &pool = *(fixed ? fixedDataPool_ : largeDataPool_);

			if (pool.empty() || pool.back() != NULL) {
				pool.push_back(NULL);
			}
			data = (fixed ?
					alloc_.base().allocate() : poolAlloc_->allocate(1U << bits));

			pool.back() = data;
		}
		else {
			(*varDataPool_)[bits] = static_cast<PoolData*>(data)->next_;
		}

		if (availableSize != NULL) {
			break;
		}

		while (bits > minBits) {
			void *restData = static_cast<uint8_t*>(data) + (1U << (--bits));
			static_cast<PoolData*>(restData)->next_ = NULL;
			(*varDataPool_)[bits] = restData;
		}
	}
	while (false);

	if (availableSize != NULL) {
		*availableSize = 1U << bits;
	}

	return data;
}

void SQLValues::SummaryTupleSet::deallocateDetail(void *data, size_t size) {
	if (parent_ != NULL) {
		parent_->deallocateDetail(data, size);
		return;
	}

	const size_t sizeBits = getSizeBits(size, true);
	if (sizeBits >= varDataPool_->size()) {
		assert(false);
		return;
	}

	void *&ref = (*varDataPool_)[sizeBits];
	static_cast<PoolData*>(data)->next_ = ref;
	ref = data;
}

size_t SQLValues::SummaryTupleSet::getFixedSizeBits() const {
	return getSizeBits(alloc_.base().getElementSize(), false);
}

size_t SQLValues::SummaryTupleSet::getSizeBits(size_t size, bool ceil) {
	uint32_t bits = static_cast<uint32_t>(std::max<uint32_t>(
		MIN_POOL_DATA_BITS,
		static_cast<uint32_t>(sizeof(uint32_t) * CHAR_BIT) -
				util::nlz(static_cast<uint32_t>(size - 1))));
	if (!ceil && ((1U << bits) > size)) {
		--bits;
	}
	return std::max<uint32_t>(bits, MIN_POOL_DATA_BITS);
}


SQLValues::SummaryTupleSet::LocalLobBuilder::LocalLobBuilder(
		SummaryTupleSet &tupleSet, TupleColumnType type) :
		tupleSet_(tupleSet),
		type_(type),
		smallLob_(NULL) {
}

SQLValues::SummaryTupleSet::LocalLobBuilder::~LocalLobBuilder() {
	clear();
}

void SQLValues::SummaryTupleSet::LocalLobBuilder::assignSmall(void *lob) {
	clear();
	smallLob_ = lob;
}

void* SQLValues::SummaryTupleSet::LocalLobBuilder::release(bool &small) {
	small = false;

	void *ret = NULL;
	if (base_.get() != NULL) {
		ret = tupleSet_.toNormalLobByBuilder(tupleSet_.varCxt_, *base_);
		base_.reset();
	}
	else if (smallLob_ != NULL) {
		small = true;

		ret = smallLob_;
		smallLob_ = NULL;
	}

	return ret;
}

void SQLValues::SummaryTupleSet::LocalLobBuilder::append(LobReader &src) {
	bool found = false;
	for (;;) {
		const uint32_t size = static_cast<uint32_t>(std::min<uint64_t>(
				src.getNext(), std::numeric_limits<uint32_t>::max()));
		if (size <= 0) {
			break;
		}

		const void *data = &src.get();
		found = true;

		prepare(size, false);
		if (base_.get() != NULL) {
			base_->append(data, size);
		}
		else {
			smallLob_ =
					tupleSet_.appendVarDataDetail(data, size, smallLob_, false);
		}

		src.step(size);
	}

	if (!found) {
		prepare(0, true);
	}
}

void SQLValues::SummaryTupleSet::LocalLobBuilder::appendSmall(void *lob) {
	const uint32_t bodySize = TupleValueUtils::decodeVarSize(lob);
	const uint32_t headSize = TupleValueUtils::getEncodedVarSize(bodySize);
	const void *body = static_cast<const uint8_t*>(lob) + headSize;

	prepare(bodySize, false);
	if (base_.get() != NULL) {
		base_->append(body, bodySize);
	}
	else {
		smallLob_ =
				tupleSet_.appendVarDataDetail(body, bodySize, smallLob_, false);
	}
}

void SQLValues::SummaryTupleSet::LocalLobBuilder::clear() {
	base_.reset();

	if (smallLob_ != NULL) {
		tupleSet_.deallocateSmallLob(smallLob_);
		smallLob_ = NULL;
	}
}

void SQLValues::SummaryTupleSet::LocalLobBuilder::prepare(
		size_t size, bool force) {
	if (base_.get() != NULL) {
		return;
	}

	const uint32_t lastBodySize = (smallLob_ == NULL ?
			0 : TupleValueUtils::decodeVarSize(smallLob_));
	const uint64_t nextBodySize = static_cast<uint64_t>(lastBodySize) + size;

	if (nextBodySize > getMaxSmallLobBodySize()) {
		base_ = UTIL_MAKE_LOCAL_UNIQUE(
				base_, LobBuilder, tupleSet_.varCxt_, type_, 0);

		if (smallLob_ != NULL) {
			const uint32_t headSize =
					TupleValueUtils::getEncodedVarSize(lastBodySize);
			const void *body = static_cast<uint8_t*>(smallLob_) + headSize;
			base_->append(body, lastBodySize);
		}
	}
	else if (smallLob_ == NULL && force) {
		smallLob_ = tupleSet_.appendVarDataDetail(NULL, 0, NULL, false);
	}
}


SQLValues::DigestArrayTuple::DigestArrayTuple(util::StackAllocator &alloc) :
		digest_(0),
		tuple_(alloc) {
}


TupleColumnType SQLValues::ValueAccessor::getColumnType(
		bool promotable, bool first) const {
	if (columnList_ == NULL) {
		assert(promotable || first);
		assert(!TypeUtils::isNull(type_));
		return type_;
	}
	else {
		if (promotable) {
			if (first) {
				return columnIt_->getTupleColumn1().getType();
			}
			else {
				return columnIt_->getTupleColumn2().getType();
			}
		}
		else {
			assert(first);
			return columnIt_->getType();
		}
	}
}

SQLValues::ValueAccessor SQLValues::ValueAccessor::ofValue(
		const TupleValue &value) {
	return ValueAccessor(ValueUtils::toColumnType(value));
}

bool SQLValues::ValueAccessor::isSameVariant(
		const ValueAccessor &another, bool nullIgnorable,
		bool anotherNullIgnorable) const {
	if ((columnList_ == NULL) != (another.columnList_ == NULL)) {
		return false;
	}

	const bool promotable = (columnList_ != NULL);
	for (size_t i = 0; i < 2; i++) {
		const bool first = (i == 0);

		TupleColumnType typeList[2];
		for (size_t j = 0; j < 2; j++) {
			const ValueAccessor &src = (j == 0 ? *this : another);
			TupleColumnType &type = typeList[j];

			type = src.getColumnType(promotable, first);

			if ((j == 0 ? nullIgnorable : anotherNullIgnorable)) {
				type = TypeUtils::toNonNullable(type);
			}
		}

		if (typeList[0] != typeList[1]) {
			return false;
		}

		if (!promotable) {
			break;
		}
	}

	return true;
}


SQLValues::ValueComparator SQLValues::ValueComparator::ofValues(
		const TupleValue &v1, const TupleValue &v2, bool sensitive,
		bool ascending) {
	return ValueComparator(
			ValueAccessor::ofValue(v1), ValueAccessor::ofValue(v2),
			NULL, sensitive, ascending);
}

SQLValues::ValueComparator::DefaultResultType
SQLValues::ValueComparator::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	typedef DefaultSwitcherByValues SwitcherType;
	const bool nullIgnorable = false;
	return DefaultSwitcherByValues(*this, nullIgnorable).getWith<
			const ValueComparator, SwitcherType::DefaultTraitsType>()(*this, v1, v2);
}

bool SQLValues::ValueComparator::isEmpty() const {
	return getColumnTypeSwitcher(true, false, NULL).isAny();
}

bool SQLValues::ValueComparator::isSameVariant(
		const ValueComparator &another, bool nullIgnorable,
		bool anotherNullIgnorable, bool orderArranged) const {
	return (!sensitive_ == !another.sensitive_ &&
			(orderArranged ?
					ascending_ : (!ascending_ == !another.ascending_)) &&
			accessor1_.isSameVariant(
					another.accessor1_, nullIgnorable, anotherNullIgnorable) &&
			accessor2_.isSameVariant(
					another.accessor2_, nullIgnorable, anotherNullIgnorable));
}

SQLValues::TypeSwitcher
SQLValues::ValueComparator::getColumnTypeSwitcher(
		bool promotable, bool nullIgnorable, ValueProfile *profile) const {
	TypeSwitcher switcher(
			accessor1_.getColumnType(promotable, true),
			accessor2_.getColumnType(promotable, (!promotable || false)));
	if (nullIgnorable) {
		switcher = switcher.toNonNullable();
	}
	if (profile != NULL) {
		switcher = switcher.withProfile(profile);
	}
	return switcher;
}


SQLValues::ValueComparator::Arranged::Arranged(const ValueComparator &base) :
		base_(base),
		func_(SwitcherType(base_, false).getWith<
				const ValueComparator, SwitcherType::DefaultTraitsType>()) {
}


bool SQLValues::ValueEq::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool sensitive = false;
	return ValueComparator::compareValues(v1, v2, sensitive, true) == 0;
}


bool SQLValues::ValueLess::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool sensitive = true;
	return ValueComparator::compareValues(v1, v2, sensitive, true) < 0;
}


bool SQLValues::ValueGreater::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool sensitive = true;
	return ValueComparator::compareValues(v1, v2, sensitive, true) > 0;
}


SQLValues::ValueFnv1aHasher SQLValues::ValueFnv1aHasher::ofValue(
		const TupleValue &value) {
	return ValueFnv1aHasher(ValueAccessor::ofValue(value), NULL);
}

SQLValues::ValueFnv1aHasher SQLValues::ValueFnv1aHasher::ofValue(
		const TupleValue &value, int64_t seed) {
	return ValueFnv1aHasher(ValueAccessor::ofValue(value), NULL, seed);
}

uint32_t SQLValues::ValueFnv1aHasher::operator()(
		const TupleValue &value) const {
	typedef TypeSwitcher::OpTraitsOptions<ValueDigester::VariantTraits<
			false, false, ValueAccessor::ByValue> > Options;
	typedef TypeSwitcher::OpTraits<int64_t, Options, const TupleValue> Traits;

	const TypeSwitcher switcher(accessor_.getColumnType(false, true));
	const int64_t ret =
			switcher.getWith<const ValueFnv1aHasher, Traits>()(*this, value);
	return static_cast<uint32_t>(ret < 0 ? unmaskNull(ret) : ret);
}


bool SQLValues::ValueDigester::isSameVariant(
		const ValueDigester &another, bool nullIgnorable,
		bool anotherNullIgnorable) const {
	return (!ordering_ == !another.ordering_ &&
			!rotating_ == !another.rotating_ &&
			accessor_.isSameVariant(
					another.accessor_, nullIgnorable, anotherNullIgnorable));
}

bool SQLValues::ValueDigester::isEmpty() const {
	return getColumnTypeSwitcher(false, false, NULL).isAny();
}

SQLValues::TypeSwitcher
SQLValues::ValueDigester::getColumnTypeSwitcher(
		bool nullIgnorable, bool nullableAlways, ValueProfile *profile) const {
	TypeSwitcher switcher(accessor_.getColumnType(false, true));
	if (nullIgnorable) {
		switcher = switcher.toNonNullable();
	}
	if (nullableAlways) {
		switcher = switcher.toNullableAlways();
	}
	if (profile != NULL) {
		switcher = switcher.withProfile(profile);
	}
	return switcher;
}


SQLValues::ValueConverter::ValueConverter(TupleColumnType type) :
		type_(type) {
}

TupleValue SQLValues::ValueConverter::operator()(
		ValueContext &cxt, const TupleValue &src) const {
	if (ValueUtils::isNull(src)) {
		if (!TypeUtils::isNullable(type_) && !TypeUtils::isAny(type_)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION,
					"NULL value is not allowed (expected=" <<
					TypeUtils::toString(type_) << ")");
		}
		return TupleValue();
	}

	const TupleColumnType nonNullableType = TypeUtils::toNonNullable(type_);
	if (src.getType() == nonNullableType) {
		return src;
	}

	switch (nonNullableType) {
	case TupleTypes::TYPE_BYTE:
		return ValueUtils::toAnyByNumeric(ValueUtils::toIntegral<int8_t>(src));
	case TupleTypes::TYPE_SHORT:
		return ValueUtils::toAnyByNumeric(ValueUtils::toIntegral<int16_t>(src));
	case TupleTypes::TYPE_INTEGER:
		return ValueUtils::toAnyByNumeric(ValueUtils::toIntegral<int32_t>(src));
	case TupleTypes::TYPE_LONG:
		return ValueUtils::toAnyByNumeric(ValueUtils::toIntegral<int64_t>(src));
	case TupleTypes::TYPE_FLOAT:
		return ValueUtils::toAnyByNumeric(ValueUtils::toFloating<float>(src));
	case TupleTypes::TYPE_DOUBLE:
		return ValueUtils::toAnyByNumeric(ValueUtils::toFloating<double>(src));
	case TupleTypes::TYPE_TIMESTAMP:
		return ValueUtils::toAnyByValue<TupleTypes::TYPE_TIMESTAMP>(
				ValueUtils::toTimestamp(
						cxt, src, util::DateTime::FIELD_MILLISECOND).toTimestamp(
								Types::TimestampTag()));
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return ValueUtils::toAnyByValue<TupleTypes::TYPE_MICRO_TIMESTAMP>(
				ValueUtils::toTimestamp(
						cxt, src, util::DateTime::FIELD_MICROSECOND).toTimestamp(
								Types::MicroTimestampTag()));
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		return ValueUtils::toAnyByWritable<TupleTypes::TYPE_NANO_TIMESTAMP>(
				cxt, ValueUtils::toTimestamp(
						cxt, src, util::DateTime::FIELD_NANOSECOND).toTimestamp(
								Types::NanoTimestampTag()));
	case TupleTypes::TYPE_BOOL:
		return ValueUtils::toAnyByBool(ValueUtils::toBool(src));
	case TupleTypes::TYPE_STRING:
		return ValueUtils::toString(cxt, src);
	case TupleTypes::TYPE_BLOB:
		return ValueUtils::toBlob(cxt, src);
	default:
		assert(false);
		return TupleValue();
	}
}


SQLValues::ValuePromoter::ValuePromoter(TupleColumnType type) :
		type_(type) {
}

TupleValue SQLValues::ValuePromoter::operator()(
		ValueContext *cxt, const TupleValue &src) const {
	if (src.getType() == type_) {
		return src;
	}

	if (ValueUtils::isNull(src)) {
		return TupleValue();
	}

	switch (type_) {
	case TupleTypes::TYPE_LONG:
		return ValueUtils::toAnyByNumeric(ValueUtils::toIntegral<int64_t>(src));
	case TupleTypes::TYPE_DOUBLE:
		return ValueUtils::toAnyByNumeric(ValueUtils::toFloating<double>(src));
	case TupleTypes::TYPE_TIMESTAMP:
		return ValueUtils::toAnyByValue<TupleTypes::TYPE_TIMESTAMP>(
				ValueUtils::promoteValue<TupleTypes::TYPE_TIMESTAMP>(src));
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return ValueUtils::toAnyByValue<TupleTypes::TYPE_MICRO_TIMESTAMP>(
				ValueUtils::promoteValue<
						TupleTypes::TYPE_MICRO_TIMESTAMP>(src));
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		if (cxt == NULL) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		return ValueUtils::toAnyByWritable<TupleTypes::TYPE_NANO_TIMESTAMP>(
				*cxt, ValueUtils::promoteValue<
						TupleTypes::TYPE_NANO_TIMESTAMP>(src));
	default:
		assert(false);
		return TupleValue();
	}
}


SQLValues::TupleNullChecker::TupleNullChecker(
		const CompColumnList &columnList) :
		columnList_(columnList) {
}

bool SQLValues::TupleNullChecker::isEmpty() const {
	for (CompColumnList::const_iterator it = columnList_.begin();
			it != columnList_.end(); ++it) {
		if (TypeUtils::isNullable(it->getType())) {
			return false;
		}
	}
	return true;
}


SQLValues::TupleComparator::TupleComparator(
		const CompColumnList &columnList, VarContext *varCxt, bool sensitive,
		bool withDigest, bool nullIgnorable, bool orderedDigestRestricted) :
		columnList_(columnList),
		varCxt_(varCxt),
		sensitive_(sensitive),
		withDigest_(withDigest),
		nullIgnorable_(nullIgnorable),
		orderedDigestRestricted_(orderedDigestRestricted) {
}

SQLValues::ValueComparator SQLValues::TupleComparator::initialComparatorAt(
		CompColumnList::const_iterator it, bool ordering) const {
	bool ascending = it->isAscending();
	if (ordering && !columnList_.front().isAscending() &&
			it != columnList_.begin()) {
		ascending = !ascending;
	}
	return ValueComparator(
			ValueAccessor(it, columnList_), ValueAccessor(it, columnList_),
			NULL, sensitive_, ascending);
}

bool SQLValues::TupleComparator::isEmpty(size_t startIndex) const {
	if (startIndex >= columnList_.size()) {
		return true;
	}

	for (CompColumnList::const_iterator it = columnList_.begin() + startIndex;
			it != columnList_.end(); ++it) {
		if (!initialComparatorAt(it, false).isEmpty()) {
			return false;
		}
	}

	return true;
}

bool SQLValues::TupleComparator::isEitherSideEmpty(size_t startIndex) const {
	if (startIndex >= columnList_.size()) {
		return true;
	}

	for (CompColumnList::const_iterator it = columnList_.begin() + startIndex;
			it != columnList_.end(); ++it) {
		const TupleColumnType type1 = it->getTupleColumn1().getType();
		const TupleColumnType type2 = it->getTupleColumn2().getType();
		assert(!TypeUtils::isNull(type1) && !TypeUtils::isNull(type2));

		if (TypeUtils::isAny(type1) || TypeUtils::isAny(type2)) {
			return true;
		}
	}

	return false;
}

bool SQLValues::TupleComparator::isDigestOnly(bool promotable) const {
	if (withDigest_ && columnList_.size() == 1) {
		const CompColumn &column = columnList_.front();

		const TupleColumnType type = column.getType();
		assert(!TypeUtils::isNull(type));

		if ((TypeUtils::isNormalFixed(type) &&
				(nullIgnorable_ || !TypeUtils::isNullable(type))) ||
				TypeUtils::isAny(type)) {

			if (promotable && TypeUtils::isFloating(type)) {
				const TupleColumnType type1 =
						column.getTupleColumn1().getType();
				const TupleColumnType type2 =
						column.getTupleColumn2().getType();
				if (TypeUtils::isIntegral(type1) ||
						TypeUtils::isIntegral(type2)) {
					return false;
				}
			}
			return true;
		}
	}

	return false;
}

bool SQLValues::TupleComparator::isDigestOrdered() const {
	if (orderedDigestRestricted_) {
		return false;
	}

	if (columnList_.size() >= 1) {
		const CompColumn &column = columnList_.front();

		if (column.isOrdering()) {
			return true;
		}

		const TupleColumnType type = column.getType();
		assert(!TypeUtils::isNull(type));

		if ((TypeUtils::isNormalFixed(type) || TypeUtils::isAny(type)) &&
				columnList_.size() <= 1) {
			return true;
		}
	}

	return false;
}

bool SQLValues::TupleComparator::isSingleVarHeadOnly(bool ordering) const {
	if (columnList_.size() == 1) {
		const CompColumn &column = columnList_.front();

		if (!ordering != !column.isOrdering()) {
			return false;
		}

		const TupleColumnType type = column.getType();
		return TypeUtils::isSingleVar(type);
	}

	return false;
}


SQLValues::TupleRangeComparator::TupleRangeComparator(
		const CompColumnList &columnList, VarContext *varCxt,
		bool nullIgnorable) :
		base_(columnList, varCxt, false, false, nullIgnorable),
		columnList_(columnList),
		varCxt_(varCxt),
		nullIgnorable_(nullIgnorable) {
}


SQLValues::TupleDigester::TupleDigester(
		const CompColumnList &columnList, VarContext *varCxt, 
		const bool *ordering, bool rotationAllowed, bool nullIgnorable,
		bool nullableAlways) :
		columnList_(columnList),
		varCxt_(varCxt),
		ordering_((ordering == NULL ?
				isOrderingAvailable(columnList, false) : *ordering)),
		rotating_(isRotationAvailable(columnList, ordering_, rotationAllowed)),
		nullIgnorable_(nullIgnorable),
		nullableAlways_(nullableAlways) {
}

bool SQLValues::TupleDigester::isOrdering() const {
	return ordering_;
}

bool SQLValues::TupleDigester::isRotating() const {
	return rotating_;
}

bool SQLValues::TupleDigester::isOrderingAvailable(
		const CompColumnList &columnList, bool promotable) {
	if (columnList.empty() || columnList.front().isOrdering()) {
		return true;
	}
	return TupleComparator(
			columnList, NULL, false, true, true).isDigestOnly(promotable);
}

bool SQLValues::TupleDigester::isRotationAvailable(
		const CompColumnList &columnList, bool ordering,
		bool rotationAllowed) {
	if (!ordering || !rotationAllowed) {
		return false;
	}
	return (!columnList.empty() && !columnList.front().isOrdering());
}

SQLValues::ValueDigester SQLValues::TupleDigester::initialDigesterAt(
		CompColumnList::const_iterator it) const {
	return ValueDigester(
			ValueAccessor(it, columnList_), NULL, ordering_, rotating_, 0);
}


SQLValues::LongInt SQLValues::LongInt::multiply(const LongInt &base) const {
	const bool negative = isNegative();
	const bool baseNegative = base.isNegative();
	if (negative || baseNegative) {
		const bool retNegative = !(negative && baseNegative);
		return negate(negative).multiplyUnsigned(
				base.negate(baseNegative)).negate(retNegative);
	}

	return multiplyUnsigned(base);
}

SQLValues::LongInt SQLValues::LongInt::divide(const LongInt &base) const {
	const bool negative = isNegative();
	const bool baseNegative = base.isNegative();
	if (negative || baseNegative) {
		const bool retNegative = !(negative && baseNegative);
		return negate(negative).divideUnsigned(
				base.negate(baseNegative)).negate(retNegative);
	}

	return divideUnsigned(base);
}

SQLValues::LongInt SQLValues::LongInt::multiplyUnsigned(
		const LongInt &base) const {
	assert(isUnsignedWith(base));
	const High highUnit = static_cast<High>(unit_);

	const High v11 = static_cast<High>(low_) * static_cast<High>(base.low_);
	const High v12 = static_cast<High>(low_) * base.high_;
	const High v21 = high_ * static_cast<High>(base.low_);
	const High v22 = high_ * base.high_;

	const High high = (v22 * highUnit) + (v12 + v21) + (v11 / highUnit);
	const Low low = static_cast<Low>(v11 % highUnit);

	return LongInt(high, low, unit_);
}

SQLValues::LongInt SQLValues::LongInt::divideUnsigned(
		const LongInt &base) const {
	assert(isUnsignedWith(base));

	if (base.isZero()) {
		return errorZeroDivision();
	}

	const uint64_t u = static_cast<uint64_t>(unit_);

	const uint64_t t1 = static_cast<uint64_t>(high_);
	const uint64_t t2 = static_cast<uint64_t>(low_);

	const uint64_t b1 = static_cast<uint64_t>(base.high_);
	const uint64_t b2 = static_cast<uint64_t>(base.low_);


	uint64_t r1;
	uint64_t r2;
	if (b1 > 0) {
		const uint64_t m = std::numeric_limits<uint64_t>::max();
		const uint64_t ct = std::max<uint64_t>(t1 / (m / u), 1);
		const uint64_t cb = std::max<uint64_t>(b1 / (m / u), 1);
		const uint64_t c = std::max(ct, cb);
		const uint64_t db = b1 / c * u + b2 / c;
		if (db > 0) {
			const uint64_t dt = t1 / c * u + t2 / c;
			const uint64_t r = dt / db;
			r1 = r / u;
			r2 = r % u;
		}
		else {
			const uint64_t sb = b1 / cb * u + b2 / cb;
			const uint64_t s1 = t1 / sb * cb;
			const uint64_t s2 = t2 / sb * cb;
			r1 = (s2 / u) + s1;
			r2 = (s2 % u);
		}
	}
	else {
		const uint64_t s2 = t1 % b2 * u + t2;
		r1 = t1 / b2;
		r2 = s2 / b2;
	}

	return LongInt(static_cast<High>(r1), static_cast<Low>(r2), unit_);
}

SQLValues::LongInt SQLValues::LongInt::errorZeroDivision() {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_DIVIDE_BY_ZERO, "Divide by 0");
}


void SQLValues::StringBuilder::expandCapacity(size_t actualSize) {
	reserve(capacity() * 2, actualSize);

	if (data_ != localBuffer_) {
		dynamicBuffer_.resize(actualSize);
	}
}

void SQLValues::StringBuilder::initializeDynamicBuffer(
		size_t newCapacity, size_t actualSize) {
	assert(data_ == localBuffer_);

	dynamicBuffer_.resize(newCapacity);
	data_ = dynamicBuffer_.data();
	memcpy(data_, localBuffer_, actualSize);
}

void SQLValues::StringBuilder::errorLimit(uint64_t size, size_t limit) {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
			"Too large string (limit=" << limit <<
			", requested=" << size << ")");
}

void SQLValues::StringBuilder::errorMaxSize(size_t current, size_t appending) {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
			"Too large string in appending (currentSize=" << current <<
			", appendingSize=" << appending << ")");
}


SQLValues::LobReader::LobReader(const TupleValue &value) :
		it_(NULL),
		begin_(NULL),
		end_(NULL),
		index_(0),
		base_(value) {
	updateIterator();
}

void SQLValues::LobReader::next() {
	assert(hasNext());
	if (++it_ == end_) {
		updateIterator();
		if (hasNext()) {
			++index_;
		}
	}
}

void SQLValues::LobReader::prev() {
	assert(hasPrev());
	if (it_ == begin_) {
		const size_t prevIndex = index_ - 1;
		toBegin();
		assert(hasNext());

		for (;; ++index_) {
			it_ = end_;
			if (prevIndex <= index_) {
				break;
			}

			updateIterator();
			if (!hasNext()) {
				break;
			}
		}
	}
	--it_;
}

void SQLValues::LobReader::step(size_t size) {
	assert(size <= getNext());
	if ((it_ += size) >= end_ && it_ != begin_) {
		it_ = end_ - 1;
		next();
	}
}

void SQLValues::LobReader::back(size_t size) {
	assert(size <= getPrev());
	const ptrdiff_t diff = it_ - begin_;
	if (diff <= 0 && size > 0) {
		assert(size <= 1);
		prev();
	}
	else {
		it_-= size;
	}
}

void SQLValues::LobReader::toBegin() {
	it_ = NULL;
	begin_ = NULL;
	end_ = NULL;
	index_ = 0;
	base_.reset();
	updateIterator();
}

void SQLValues::LobReader::toEnd() {
	for (;; ++index_) {
		it_ = end_;
		updateIterator();
		if (!hasNext()) {
			break;
		}
	}
}

bool SQLValues::LobReader::atBegin() const {
	return (it_ == begin_ && index_ == 0);
}

bool SQLValues::LobReader::atEnd() const {
	return it_ == end_;
}

const uint8_t& SQLValues::LobReader::get() const {
	assert(it_ != end_);
	return *it_;
}

bool SQLValues::LobReader::hasNext() const {
	return !atEnd();
}

bool SQLValues::LobReader::hasPrev() const {
	return !atBegin();
}

size_t SQLValues::LobReader::getNext() const {
	return end_ - it_;
}

size_t SQLValues::LobReader::getPrev() const {
	const ptrdiff_t diff = it_ - begin_;
	if (diff <= 0) {
		if (index_ == 0) {
			return 0;
		}
		return 1;
	}
	return diff;
}

void SQLValues::LobReader::updateIterator() {
	const void *data;
	size_t size;
	while (base_.next(data, size)) {
		if (size > 0) {
			it_ = static_cast<Iterator>(data);
			begin_ = it_;
			end_ = it_ + size;
			break;
		}
	}
}


SQLValues::LobReaderRef::LobReaderRef(const TupleValue &value) :
		ptrRef_(ptr_),
		value_(value) {
}

SQLValues::LobReaderRef::LobReaderRef(const LobReaderRef &another) :
		ptrRef_(ptr_),
		value_(another.value_) {
}

SQLValues::LobReaderRef&
SQLValues::LobReaderRef::operator=(const LobReaderRef &another) {
	ptrRef_.reset();
	value_ = another.value_;
	return *this;
}

SQLValues::LobReader& SQLValues::LobReaderRef::operator*() const {
	if (ptrRef_.get() == NULL) {
		ptrRef_ = UTIL_MAKE_LOCAL_UNIQUE(ptrRef_, LobReader, value_);
	}
	return *ptrRef_;
}


int32_t SQLValues::ValueUtils::compareFloating(
		double v1, double v2, bool sensitive) {
	if (v1 < v2) {
		return -1;
	}
	else if (v1 > v2) {
		return 1;
	}

	if (v1 == 0 && v2 == 0) {
		if (!sensitive) {
			return 0;
		}
		return compareRawValue(
				util::copySign(1.0, v1) > 0 ? 1 : 0,
				util::copySign(1.0, v2) > 0 ? 1 : 0);
	}

	return compareRawValue(
			util::isNaN(v1) ? 1 : 0, util::isNaN(v2) ? 1 : 0);
}

int32_t SQLValues::ValueUtils::compareString(
		const TupleString &v1, const TupleString &v2) {
	typedef ValueComparator::ThreeWay PredType;

	const TupleString::BufferInfo &buf1 = v1.getBuffer();
	const TupleString::BufferInfo &buf2 = v2.getBuffer();

	const ptrdiff_t sizeDiff =
			static_cast<ptrdiff_t>(buf1.second) -
			static_cast<ptrdiff_t>(buf2.second);

	const ptrdiff_t rest =
			static_cast<ptrdiff_t>(sizeDiff <= 0 ? buf1.second : buf2.second);

	return compareBuffer<PredType, int32_t>(
			reinterpret_cast<const uint8_t*>(buf1.first),
			reinterpret_cast<const uint8_t*>(buf2.first),
			rest, sizeDiff, PredType());
}

int32_t SQLValues::ValueUtils::getTypeBoundaryDirectionAsLong(
		const TupleValue &value, TupleColumnType boundaryType, bool lower) {
	const TupleColumnType boundaryCategory =
			getBoundaryCategoryAsLong(boundaryType);
	if (boundaryCategory != TupleTypes::TYPE_LONG &&
			boundaryCategory != TupleTypes::TYPE_NANO_TIMESTAMP) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	int64_t boundary;
	switch (boundaryType) {
	case TupleTypes::TYPE_BYTE:
		boundary = (lower ?
				std::numeric_limits<int8_t>::min() :
				std::numeric_limits<int8_t>::max());
		break;
	case TupleTypes::TYPE_SHORT:
		boundary = (lower ?
				std::numeric_limits<int16_t>::min() :
				std::numeric_limits<int16_t>::max());
		break;
	case TupleTypes::TYPE_INTEGER:
		boundary = (lower ?
				std::numeric_limits<int32_t>::min() :
				std::numeric_limits<int32_t>::max());
		break;
	default:
		boundary = (lower ?
				std::numeric_limits<int64_t>::min() :
				std::numeric_limits<int64_t>::max());
		break;
	}

	const int64_t baseValue = toLong(value);
	const int32_t comp = compareIntegral(baseValue, boundary);

	if (comp == 0) {
		const int32_t dir = getBoundaryDirectionAsLong(value, 1);
		if (dir == 0) {
			return 0;
		}
		return 1;
	}
	else {
		return (comp < 0 ? -1 : 1);
	}
}

bool SQLValues::ValueUtils::isUpperBoundaryAsLong(
		const TupleValue &value, int64_t unit) {
	const int32_t direction = getBoundaryDirectionAsLong(value, unit);
	if (direction == 0) {
		return isSmallestBoundaryUnitAsLong(value, unit);
	}
	return (direction < 0);
}

bool SQLValues::ValueUtils::isLowerBoundaryAsLong(
		const TupleValue &value, int64_t unit) {
	const int32_t direction = getBoundaryDirectionAsLong(value, unit);
	return (direction == 0);
}

TupleColumnType SQLValues::ValueUtils::getBoundaryCategoryAsLong(
		TupleColumnType type) {
	const TypeUtils::TypeCategory typeCategory =
			TypeUtils::getStaticTypeCategory(type);

	if (type == TupleTypes::TYPE_TIMESTAMP ||
			typeCategory == TypeUtils::TYPE_CATEGORY_LONG) {
		return TupleTypes::TYPE_LONG;
	}
	else if (typeCategory == TypeUtils::TYPE_CATEGORY_TIMESTAMP) {
		return TupleTypes::TYPE_NANO_TIMESTAMP;
	}
	else {
		return TupleTypes::TYPE_NULL;
	}
}

bool SQLValues::ValueUtils::isSmallestBoundaryUnitAsLong(
		const TupleValue &value, int64_t unit) {
	if (unit > 1) {
		return false;
	}

	return (getBoundaryCategoryAsLong(value.getType()) ==
			TupleTypes::TYPE_LONG);
}

int32_t SQLValues::ValueUtils::getBoundaryDirectionAsLong(
		const TupleValue &value, int64_t unit) {
	if (unit <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const TupleColumnType type = value.getType();
	const TupleColumnType boundaryCategory =
			getBoundaryCategoryAsLong(type);

	if (boundaryCategory == TupleTypes::TYPE_LONG) {
		const int64_t baseValue = toLong(value);
		if (baseValue >= 0) {
			const int64_t mod = baseValue % unit;
			if (mod == 0) {
				return 0;
			}
			else if (mod == unit - 1) {
				return -1;
			}
			return 1;
		}
		else {
			const int64_t absValue =
					(baseValue == std::numeric_limits<int64_t>::min() ?
							-(baseValue + unit) :
							-baseValue);
			const int64_t mod = absValue % unit;
			if (mod == 0) {
				return 0;
			}
			else if (mod == 1) {
				return -1;
			}
			return 1;
		}
	}
	else if (boundaryCategory == TupleTypes::TYPE_NANO_TIMESTAMP) {
		const int32_t base = getBoundaryDirectionAsLong(
				TupleValue(toLong(value)), unit);
		if (base > 0) {
			return base;
		}

		uint32_t subUnit;
		uint32_t subValue;
		switch (type) {
		case TupleTypes::TYPE_MICRO_TIMESTAMP:
			subUnit = 1000;
			subValue = DateTimeElements(
					value.get<MicroTimestamp>()).getNanoSeconds() / 1000;
			break;
		case TupleTypes::TYPE_NANO_TIMESTAMP:
			subUnit = 1000 * 1000;
			subValue = DateTimeElements(
					value.get<TupleNanoTimestamp>()).getNanoSeconds();
			break;
		default:
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		if (base < 0 || (unit == 1 && subValue != 0)) {
			return (subValue == subUnit - 1 ? -1 : 1);
		}
		else {
			return (subValue == 0 ? 0 : 1);
		}
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}


uint32_t SQLValues::ValueUtils::fnv1aHashInit() {
	const uint32_t fnvOffsetBasis =
			ValueFnv1aHasher::Constants::FNV_OFFSET_BASIS;
	return fnvOffsetBasis;
}

uint32_t SQLValues::ValueUtils::fnv1aHashIntegral(
		uint32_t base, int64_t value) {
	return fnv1aHashFloating(base, static_cast<double>(value));
}

uint32_t SQLValues::ValueUtils::fnv1aHashFloating(
		uint32_t base, double value) {
	if (util::isNaN(value)) {
		return base;
	}

	if (value == 0) {
		const double zero = 0;
		return fnv1aHashBytes(base, &zero, sizeof(zero));
	}

	return fnv1aHashBytes(base, &value, sizeof(value));
}

uint32_t SQLValues::ValueUtils::fnv1aHashPreciseTimestamp(
		uint32_t base, const DateTimeElements &value) {
	uint32_t hash = fnv1aHashIntegral(base, value.getUnixTimeMillis());

	const uint32_t nanos = value.getNanoSeconds();
	if (nanos != 0) {
		hash = fnv1aHashBytes(hash, &nanos, sizeof(nanos));
	}

	return hash;
}

uint32_t SQLValues::ValueUtils::fnv1aHashBytes(
		uint32_t base, const void *value, size_t size) {
	const uint32_t fnvPrime = ValueFnv1aHasher::Constants::FNV_PRIME;

	uint32_t hash = base;
	for (size_t i = 0; i < size; i++) {
		hash = (hash ^ static_cast<const uint8_t*>(value)[i]) * fnvPrime;
	}
	return hash;
}


int64_t SQLValues::ValueUtils::toLong(const TupleValue &src) {
	if (src.getType() != TupleTypes::TYPE_LONG) {
		int64_t dest;
		const bool strict = true;
		toLongDetail(src, dest, strict);
		return dest;
	}
	return src.get<int64_t>();
}

double SQLValues::ValueUtils::toDouble(const TupleValue &src) {
	if (src.getType() != TupleTypes::TYPE_DOUBLE) {
		double dest;
		const bool strict = true;
		toDoubleDetail(src, dest, strict);
		return dest;
	}
	return src.get<double>();
}

SQLValues::LongInt SQLValues::ValueUtils::toLongInt(const TupleValue &src) {
	const TupleColumnType type = src.getType();

	if (TypeUtils::isNull(type)) {
		return LongInt::invalid();
	}

	return (TypeUtils::isTimestampFamily(type) ?
			DateTimeElements(promoteValue<TupleTypes::TYPE_NANO_TIMESTAMP>(
					src)).toLongInt() :
			LongInt::ofElements(toLong(src), 0, LongInt::mega()));
}

SQLValues::DateTimeElements SQLValues::ValueUtils::toTimestamp(
		ValueContext &cxt, const TupleValue &src,
		util::DateTime::FieldType precision) {
	const TupleColumnType type = src.getType();

	if (type == TupleTypes::TYPE_STRING) {
		util::StackAllocator &alloc = cxt.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		const TupleString::BufferInfo &strBuf = TupleString(src).getBuffer();
		const bool zoneSpecified = false;
		return parseTimestamp(
				strBuf, cxt.getTimeZone(), zoneSpecified, precision, alloc);
	}

	return checkTimestamp(TypeUtils::isTimestampFamily(type) ?
			DateTimeElements(
					promoteValue<TupleTypes::TYPE_NANO_TIMESTAMP>(src)) :
			DateTimeElements(toLong(src)));
}

bool SQLValues::ValueUtils::toBool(const TupleValue &src) {
	if (src.getType() == TupleTypes::TYPE_STRING) {
		bool dest;
		const bool strict = true;
		parseBool(TupleString(src).getBuffer(), dest, strict);
		return dest;
	}

	return (toLong(src) != 0);
}

TupleValue SQLValues::ValueUtils::toString(
		ValueContext &cxt, const TupleValue &src) {
	if (src.getType() != TupleTypes::TYPE_STRING) {
		StringBuilder builder(cxt);
		formatValue(cxt, builder, src);
		return builder.build(cxt);
	}

	return src;
}

TupleValue SQLValues::ValueUtils::toBlob(
		ValueContext &cxt, const TupleValue &src) {
	if (src.getType() != TupleTypes::TYPE_BLOB) {
		const TupleString::BufferInfo &strBuffer =
				TupleString(toString(cxt, src)).getBuffer();

		LobBuilder builder(
				cxt.getVarContext(), TupleTypes::TYPE_BLOB,
				strBuffer.second * 2);
		parseBlob(strBuffer, builder);
		return builder.build();
	}

	return src;
}

bool SQLValues::ValueUtils::toLongDetail(
		const TupleValue &src, int64_t &dest, bool strict) {

	if (!findLong(src, dest)) {
		double doubleValue;
		if (findDouble(src, doubleValue)) {
			bool overflow = false;
			if (doubleValue <= Constants::INT64_MIN_AS_DOUBLE) {
				overflow = (doubleValue < Constants::INT64_MIN_AS_DOUBLE);
				dest = std::numeric_limits<int64_t>::min();
			}
			else if (doubleValue >= Constants::INT64_MAX_AS_DOUBLE) {
				overflow = (doubleValue > Constants::INT64_MAX_AS_DOUBLE);
				dest = std::numeric_limits<int64_t>::max();
			}
			else {
				overflow = util::isNaN(doubleValue);
				dest = (overflow ? 0 : static_cast<int64_t>(doubleValue));
			}
			if (strict && overflow) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_PROC_VALUE_OVERFLOW,
						"Value overflow as integral value");
			}
			return true;
		}
		else if (src.getType() == TupleTypes::TYPE_STRING) {
			return parseLong(TupleString(src).getBuffer(), dest, strict);
		}
		else if (src.getType() == TupleTypes::TYPE_BLOB) {
			char8_t buffer[SQLVdbeUtils::VdbeUtils::NUMERIC_BUFFER_SIZE];
			const TupleString::BufferInfo &str =
					toNumericStringByBlob(src, buffer, sizeof(buffer));
			return (str.first != NULL) && parseLong(str, dest, strict);
		}
		else {
			if (strict) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION,
						"Unsupported type conversion to integral value (from=" <<
						TypeUtils::toString(src.getType()) << ")");
			}
			dest = int64_t();
			return false;
		}
	}
	else {
		return true;
	}
}

bool SQLValues::ValueUtils::toDoubleDetail(
		const TupleValue &src, double &dest, bool strict) {

	if (!findDouble(src, dest)) {
		int64_t longValue;
		if (findLong(src, longValue)) {
			dest = static_cast<double>(longValue);
			return true;
		}
		else if (src.getType() == TupleTypes::TYPE_STRING) {
			return parseDouble(TupleString(src).getBuffer(), dest, strict);
		}
		else if (src.getType() == TupleTypes::TYPE_BLOB) {
			char8_t buffer[SQLVdbeUtils::VdbeUtils::NUMERIC_BUFFER_SIZE];
			const TupleString::BufferInfo &str =
					toNumericStringByBlob(src, buffer, sizeof(buffer));
			return (str.first != NULL) && parseDouble(str, dest, strict);
		}
		else {
			if (strict) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION,
						"Unsupported type conversion to floating value (from=" <<
						TypeUtils::toString(src.getType()) << ")");
			}
			dest = double();
			return false;
		}
	}
	else {
		return true;
	}
}

TupleString::BufferInfo SQLValues::ValueUtils::toNumericStringByBlob(
		const TupleValue &src, char8_t *buf, size_t bufSize) {
	LobReader baseReader(src);
	LimitedReader<LobReader, uint8_t> reader(baseReader, bufSize / 2);

	BufferBuilder<char8_t> builder(buf, buf + bufSize);
	partToHexString(builder, reader);

	if (builder.isTruncated()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
				"Unacceptable hex numeric format");
	}

	return TupleString::BufferInfo(buf, builder.getSize());
}

bool SQLValues::ValueUtils::findLong(const TupleValue &src, int64_t &dest) {
	const TupleColumnType type = src.getType();

	if ((type & TupleTypes::TYPE_MASK_ARRAY_VAR)) {
		dest = int64_t();
		return false;
	}

	switch (type & TupleTypes::TYPE_MASK_SUB) {
	case TupleTypes::TYPE_LONG & TupleTypes::TYPE_MASK_SUB:
		break;
	case TupleTypes::TYPE_BOOL & TupleTypes::TYPE_MASK_SUB:
		break;
	default:
		if (TypeUtils::isTimestampFamily(type)) {
			dest = promoteValue<TupleTypes::TYPE_TIMESTAMP>(src);
			return true;
		}
		dest = int64_t();
		return false;
	}

	switch (type & TupleTypes::TYPE_MASK_FIXED_SIZE) {
	case TupleTypes::TYPE_BYTE & TupleTypes::TYPE_MASK_FIXED_SIZE:
		dest = getValue<int8_t>(src);
		break;
	case TupleTypes::TYPE_SHORT & TupleTypes::TYPE_MASK_FIXED_SIZE:
		dest = getValue<int16_t>(src);
		break;
	case TupleTypes::TYPE_INTEGER & TupleTypes::TYPE_MASK_FIXED_SIZE:
		dest = getValue<int32_t>(src);
		break;
	case TupleTypes::TYPE_LONG & TupleTypes::TYPE_MASK_FIXED_SIZE:
		dest = getValue<int64_t>(src);
		break;
	default:
		return false;
	}

	return true;
}

bool SQLValues::ValueUtils::findDouble(const TupleValue &src, double &dest) {
	switch (src.getType()) {
	case TupleTypes::TYPE_FLOAT:
		dest = getValue<float>(src);
		break;
	case TupleTypes::TYPE_DOUBLE:
		dest = getValue<double>(src);
		break;
	default:
		return false;
	}

	return true;
}


void SQLValues::ValueUtils::formatValue(
		ValueContext &cxt, StringBuilder &builder, const TupleValue &value) {
	const TupleColumnType type = value.getType();
	if (TypeUtils::isNull(type)) {
		return;
	}
	else if (type == TupleTypes::TYPE_STRING) {
		builder.appendAll(*toStringReader(value));
		return;
	}
	else if (TypeUtils::isTimestampFamily(type)) {
		formatTimestamp(
				builder, DateTimeElements(
						promoteValue<TupleTypes::TYPE_NANO_TIMESTAMP>(value)),
						cxt.getTimeZone(),
				TypeUtils::getTimePrecision(type));
		return;
	}
	else if (type == TupleTypes::TYPE_BOOL) {
		formatBool(builder, getValue<bool>(value));
		return;
	}
	else if (type == TupleTypes::TYPE_BLOB) {
		formatBlob(builder, value);
		return;
	}

	{
		int64_t longValue;
		if (findLong(value, longValue)) {
			formatLong(builder, longValue);
			return;
		}
	}

	{
		double doubleValue;
		if (findDouble(value, doubleValue)) {
			formatDouble(builder, doubleValue);
			return;
		}
	}

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

void SQLValues::ValueUtils::formatLong(StringBuilder &builder, int64_t value) {
	SQLVdbeUtils::VdbeUtils::numericToString(builder, TupleValue(value));
}

void SQLValues::ValueUtils::formatDouble(
		StringBuilder &builder, double value) {
	SQLVdbeUtils::VdbeUtils::numericToString(builder, TupleValue(value));
}

void SQLValues::ValueUtils::formatTimestamp(
		StringBuilder &builder, const DateTimeElements &value,
		const util::TimeZone &zone, util::DateTime::FieldType precision) {
	const util::PreciseDateTime &tsValue =
			value.toDateTime(Types::NanoTimestampTag());

	util::DateTime::ZonedOption option;
	applyDateTimeOption(option);
	option.zone_ = zone;

	util::DateTime::Formatter formatter = tsValue.getFormatter(option);
	formatter.setDefaultPrecision(precision);

	formatter.writeTo(builder, TimeErrorHandler());
}

void SQLValues::ValueUtils::formatBool(StringBuilder &builder, bool value) {
	if (value) {
		builder.appendAll(Constants::CONSTANT_TRUE_STR);
	}
	else {
		builder.appendAll(Constants::CONSTANT_FALSE_STR);
	}
}

void SQLValues::ValueUtils::formatBlob(
		StringBuilder &builder, const TupleValue &value) {
	LobReader reader(value);
	partToHexString(builder, reader);
}


bool SQLValues::ValueUtils::parseLong(
		const TupleString::BufferInfo &src, int64_t &dest, bool strict) {
	if (!SQLVdbeUtils::VdbeUtils::toLong(src.first, src.second, dest) && strict) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
				"Unacceptable format for integral value");
		return false;
	}
	return true;
}

bool SQLValues::ValueUtils::parseDouble(
		const TupleString::BufferInfo &src, double &dest, bool strict) {
	if (!SQLVdbeUtils::VdbeUtils::toDouble(src.first, src.second, dest) && strict) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
				"Unacceptable format for floating value");
		return false;
	}
	return true;
}

SQLValues::DateTimeElements SQLValues::ValueUtils::parseTimestamp(
		const TupleString::BufferInfo &src, const util::TimeZone &zone,
		bool zoneSpecified, util::DateTime::FieldType precision,
		util::StdAllocator<void, void> alloc) {
	typedef util::BasicString<
			char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, void> > String;

	String str(src.first, src.second, alloc);

	bool zoned = true;
	if (str.find('T') == util::String::npos) {
		if (str.find(':') == util::String::npos) {
			str.append("T00:00:00");
			zoned = false;
		}
		else {
			str.insert(0, "1970-01-01T");
			zoned = false;
		}
	}

	size_t zoneStrLen = 0;
	if (zoned) {
		if (!str.empty() && !zone.isEmpty() && zoneSpecified) {
			if (*(str.end() - 1) == 'Z') {
				zoneStrLen = 1;
			}
			else {
				size_t zonePos = str.find_last_of('+');
				if (zonePos == util::String::npos) {
					zonePos = str.find_last_of('-');
				}
				if (zonePos != util::String::npos) {
					zoneStrLen = str.size() - zonePos;
				}
			}
		}
	}
	else {
		util::TimeZone resolvedZone = zone;
		if (resolvedZone.isEmpty()) {
			resolvedZone = util::TimeZone::getUTCTimeZone();
		}

		char8_t buf[util::TimeZone::MAX_FORMAT_SIZE];
		const size_t zoneSize = resolvedZone.format(buf, sizeof(buf));

		str.append(buf, zoneSize);
	}

	util::DateTime::ZonedOption option;
	applyDateTimeOption(option);

	util::PreciseDateTime dateTime;
	util::TimeZone zoneByStr;
	try {
		const bool throwOnError = true;

		{
			util::DateTime::Parser parser = dateTime.getParser(option);
			parser.setDefaultPrecision(precision);
			parser(str.c_str(), str.size(), throwOnError);
		}

		if (zoneStrLen > 0) {
			zoneByStr.parse(
					str.c_str() + str.size() - zoneStrLen, zoneStrLen,
					throwOnError);
		}
	}
	catch (std::exception &e) {
		const String orgStr(src.first, src.second, alloc);
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Unacceptable timestamp string specified (value=" <<
						orgStr << ")"));
	}

	if (!zoneByStr.isEmpty() &&
			zoneByStr.getOffsetMillis() != zone.getOffsetMillis()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INVALID_EXPRESSION_INPUT,
				"Inconsistent time zone specified (zoneInTimestampString=" <<
				str << ", specifiedZone=" << zone << ")");
	}

	return DateTimeElements(dateTime);
}

bool SQLValues::ValueUtils::parseBool(
		const TupleString::BufferInfo &src, bool &dest, bool strict) {
	if (orderPartNoCase(
			src, toStringBuffer(Constants::CONSTANT_TRUE_STR)) == 0) {
		dest = true;
		return true;
	}
	else if (orderPartNoCase(
			src, toStringBuffer(Constants::CONSTANT_FALSE_STR)) == 0) {
		dest = false;
		return true;
	}
	else {
		dest = bool();
		if (strict) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
					"Invalid BOOL format");
		}
		return false;
	}
}

void SQLValues::ValueUtils::parseBlob(
		const TupleString::BufferInfo &src, LobBuilder &dest) {
	StringReader reader(src);
	hexStringToPart(dest, reader);
}


TupleValue SQLValues::ValueUtils::createEmptyValue(
		ValueContext &cxt, TupleColumnType type) {
	if (TypeUtils::isAny(type) || TypeUtils::isNullable(type)) {
		return TupleValue();
	}

	const TupleColumnType baseType = TypeUtils::toNonNullable(type);
	if (TypeUtils::isSomeFixed(baseType)) {
		const int64_t emptyLong = 0;
		return ValueConverter(baseType)(cxt, TupleValue(emptyLong));
	}

	if (baseType == TupleTypes::TYPE_STRING) {
		StringBuilder builder(cxt);
		return builder.build(cxt);
	}
	else if (baseType == TupleTypes::TYPE_BLOB) {
		VarContext &varCxt = cxt.getVarContext();
		if (varCxt.getVarAllocator() == NULL) {
			TupleValue::StackAllocLobBuilder builder(
					varCxt, TupleTypes::TYPE_BLOB, 0);
			return builder.build();
		}
		else {
			LobBuilder builder(varCxt, TupleTypes::TYPE_BLOB, 0);
			return builder.build();
		}
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}


TupleString::BufferInfo SQLValues::ValueUtils::toStringBuffer(
		const char8_t *src) {
	return TupleString::BufferInfo(src, strlen(src));
}

TupleString::BufferInfo SQLValues::ValueUtils::toStringBuffer(
		const TupleString &src) {
	return src.getBuffer();
}

SQLValues::LobReaderRef SQLValues::ValueUtils::toLobReader(
		const TupleValue &src) {
	return LobReaderRef(src);
}


TupleColumnType SQLValues::ValueUtils::toColumnType(const TupleValue &value) {
	const TupleColumnType type = value.getType();
	if (TypeUtils::isNull(type)) {
		return TupleTypes::TYPE_ANY;
	}
	return type;
}


SQLValues::DateTimeElements SQLValues::ValueUtils::checkTimestamp(
		const DateTimeElements &tsValue) {
	const util::DateTime::Option &option = getDefaultDateTimeOption();

	util::DateTime dateTime;
	try {
		dateTime.setUnixTime(tsValue.getUnixTimeMillis(), option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Unacceptable timestamp value specified"));
	}

	const uint32_t nanos = tsValue.getNanoSeconds();
	if (nanos >= 1000 * 1000) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_VALUE_OVERFLOW,
				"Unacceptable nano seconds of timestamp specified");
	}

	return DateTimeElements(
			util::PreciseDateTime::ofNanoSeconds(dateTime, nanos));
}

SQLValues::DateTimeElements SQLValues::ValueUtils::getMinTimestamp() {
	const int64_t epochMillis = 0;
	return DateTimeElements(epochMillis);
}

SQLValues::DateTimeElements SQLValues::ValueUtils::getMaxTimestamp() {
	const util::DateTime::Option &option = getDefaultDateTimeOption();
	return DateTimeElements(util::PreciseDateTime::max(option));
}

util::DateTime::Option SQLValues::ValueUtils::getDefaultDateTimeOption() {
	util::DateTime::Option option;
	applyDateTimeOption(option);
	return option;
}

void SQLValues::ValueUtils::applyDateTimeOption(
		util::DateTime::ZonedOption &option) {
	applyDateTimeOption(option.baseOption_);
	option.maxFields_ = &Constants::TIMESTAMP_MAX_FIELDS;
}

void SQLValues::ValueUtils::applyDateTimeOption(
		util::DateTime::Option &option) {
	option.maxTimeMillis_ = Constants::TIMESTAMP_MAX_UNIX_TIME;
}

util::DateTime::FieldType SQLValues::ValueUtils::toTimestampField(
		int64_t value) {
	switch (value) {
	case util::DateTime::FIELD_YEAR:
	case util::DateTime::FIELD_MONTH:
	case util::DateTime::FIELD_DAY_OF_MONTH:
	case util::DateTime::FIELD_HOUR:
	case util::DateTime::FIELD_MINUTE:
	case util::DateTime::FIELD_SECOND:
	case util::DateTime::FIELD_MILLISECOND:
	case util::DateTime::FIELD_MICROSECOND:
	case util::DateTime::FIELD_NANOSECOND:
	case util::DateTime::FIELD_DAY_OF_WEEK:
	case util::DateTime::FIELD_DAY_OF_YEAR:
		break;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	return static_cast<util::DateTime::FieldType>(value);
}


bool SQLValues::ValueUtils::isTrue(const TupleValue &value) {
	return !isNull(value) && toBool(value);
}

bool SQLValues::ValueUtils::isFalse(const TupleValue &value) {
	return !isNull(value) && !toBool(value);
}


size_t SQLValues::ValueUtils::toLobCapacity(uint64_t size) {
	return static_cast<size_t>(std::min<uint64_t>(
			size, std::numeric_limits<uint32_t>::max() / 2));
}


TupleValue SQLValues::ValueUtils::duplicateValue(
		ValueContext &cxt, const TupleValue &src) {
	const TupleColumnType type = src.getType();
	if (TypeUtils::isNormalFixed(type)) {
		return src;
	}
	else if (type == TupleTypes::TYPE_NANO_TIMESTAMP) {
		return createNanoTimestampValue(
				cxt, getValue<TupleNanoTimestamp>(src));
	}
	else if (type == TupleTypes::TYPE_STRING) {
		const TupleString::BufferInfo &info = TupleString(src).getBuffer();

		TupleValue::SingleVarBuilder builder(
				cxt.getVarContext(), type, info.second);
		builder.append(info.first, info.second);

		return builder.build();
	}
	else if (type == TupleTypes::TYPE_BLOB) {
		if (cxt.getVarContext().getVarAllocator() == NULL) {
			TupleValue::StackAllocLobBuilder lobBuilder(
					cxt.getVarContext(), TupleList::TYPE_BLOB, 0);

			LobReader reader(src);
			for (;;) {
				const size_t size = reader.getNext();
				if (size == 0) {
					break;
				}
				lobBuilder.append(&reader.get(), size);
				reader.step(size);
			}

			return lobBuilder.build();
		}
		else {
			LobBuilder builder(cxt.getVarContext(), type, 0);

			LobReader reader(src);
			for (;;) {
				const size_t size = reader.getNext();
				if (size == 0) {
					break;
				}
				builder.append(&reader.get(), size);
				reader.step(size);
			}

			return builder.build();
		}
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLValues::ValueUtils::destroyValue(
		ValueContext &cxt, TupleValue &value) {
	cxt.getVarContext().destroyValue(value);
	value = TupleValue();
}

TupleValue SQLValues::ValueUtils::duplicateAllocValue(
		const util::StdAllocator<void, void> &alloc, const TupleValue &src) {
	const TupleColumnType type = src.getType();
	if (TypeUtils::isNormalFixed(type)) {
		return src;
	}
	else if (type == TupleTypes::TYPE_NANO_TIMESTAMP) {
		const TupleNanoTimestamp srcTs(src);
		const size_t size = sizeof(NanoTimestamp);

		util::StdAllocator<uint8_t, void> subAlloc = alloc;
		void *addr = subAlloc.allocate(size);

		NanoTimestamp *dest = static_cast<NanoTimestamp*>(addr);
		*dest = srcTs;

		return TupleValue(TupleNanoTimestamp(dest));
	}
	else if (type == TupleTypes::TYPE_STRING) {
		const uint8_t *head = static_cast<const uint8_t*>(src.getRawData());
		const uint8_t *body = static_cast<const uint8_t*>(src.varData());

		const size_t headSize = static_cast<size_t>(body - head);
		const size_t totalSize = headSize + src.varSize();

		util::StdAllocator<uint8_t, void> subAlloc = alloc;
		void *addr = subAlloc.allocate(totalSize);
		memcpy(addr, head, totalSize);

		return TupleValue(TupleString(addr));
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLValues::ValueUtils::destroyAllocValue(
		const util::StdAllocator<void, void> &alloc, TupleValue &src) {
	const TupleColumnType type = src.getType();
	if (type == TupleTypes::TYPE_NANO_TIMESTAMP) {
		util::StdAllocator<uint8_t, void> subAlloc = alloc;
		subAlloc.deallocate(static_cast<uint8_t*>(src.getRawData()), 0);
	}
	else if (type == TupleTypes::TYPE_STRING) {
		util::StdAllocator<uint8_t, void> subAlloc = alloc;
		subAlloc.deallocate(static_cast<uint8_t*>(src.getRawData()), 0);
	}
	else if (!TypeUtils::isNormalFixed(type)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	src = TupleValue();
}

const void* SQLValues::ValueUtils::getValueBody(
		const TupleValue &value, size_t &size) {
	const TupleList::TupleColumnType type = value.getType();
	if (TupleColumnTypeUtils::isNormalFixed(type)) {
		size = TupleColumnTypeUtils::getFixedSize(type);
		return value.fixedData();
	}
	else if (TupleColumnTypeUtils::isLargeFixed(type)) {
		size = TupleColumnTypeUtils::getFixedSize(type);
		return value.getRawData();
	}
	else if (TupleColumnTypeUtils::isSingleVar(type)) {
		size = value.varSize();
		return value.varData();
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLValues::ValueUtils::moveValueToRoot(
		ValueContext &cxt, TupleValue &value,
		size_t currentDepth, size_t targetDepth) {
	assert(currentDepth == cxt.getVarContextDepth());
	assert(currentDepth > targetDepth);

	if (!TypeUtils::isNormalFixed(value.getType())) {
		if (currentDepth <= 0) {
			errorVarContextDepth();
		}
		VarContext &varCxt = cxt.getVarContext();
		varCxt.moveValueToParent(currentDepth - targetDepth, value);
	}
}

size_t SQLValues::ValueUtils::estimateTupleSize(
		const TupleColumnList &list) {
	size_t size = 0;
	bool nullable = false;

	for (TupleColumnList::const_iterator it = list.begin();
			it != list.end(); ++it) {
		TupleColumnType type = it->getType();
		assert(!TypeUtils::isNull(type));

		size += TypeUtils::getFixedSize(type);
		nullable |= TypeUtils::isNullable(type);

		if (!TypeUtils::isAny(type) && !TypeUtils::isSomeFixed(type)) {
			size += 1;
		}
	}

	if (nullable) {
		size += TupleValueUtils::calcNullsByteSize(
				static_cast<uint32_t>(list.size()));
	}

	return size;
}

uint64_t SQLValues::ValueUtils::getApproxPrimeNumber(uint64_t base) {
	uint64_t num = base;
	for (;; num++) {
		if (num % 2 != 0 && num % 3 != 0 && num % 5 != 0) {
			break;
		}
		assert(num < std::numeric_limits<uint64_t>::max());
	}
	return num;
}

uint64_t SQLValues::ValueUtils::findLargestPrimeNumber(uint64_t base) {
	const uint32_t *list = Constants::LARGEST_PRIME_OF_BITS;
	const uint32_t *listEnd = list + Constants::LARGEST_PRIME_OF_BITS_COUNT;

	const uint32_t *it = (base <= std::numeric_limits<uint32_t>::max() ?
			std::lower_bound(list, listEnd, static_cast<uint32_t>(base)) :
			listEnd);
	if (it == list || it == listEnd) {
		return base;
	}
	else if (*it > base) {
		return *(it - 1);
	}

	return *it;
}


TupleValue SQLValues::ValueUtils::readCurrentAny(
		TupleListReader &reader, const TupleColumn &column) {
	return reader.get().get(column);
}

TupleValue SQLValues::ValueUtils::readAny(
		const ReadableTuple &tuple, const TupleColumn &column) {
	return tuple.get(column);
}

void SQLValues::ValueUtils::writeAny(
		WritableTuple &tuple, const TupleColumn &column,
		const TupleValue &value) {
	tuple.set(column, value);
}

void SQLValues::ValueUtils::writeNull(
		WritableTuple &tuple, const TupleColumn &column) {
	tuple.set(column, TupleValue());
}

void SQLValues::ValueUtils::updateKeepInfo(TupleListReader &reader) {
	assert(reader.exists());
	static_cast<void>(TupleList::ReadableTuple(reader));
}


util::DateTime::FieldData
SQLValues::ValueUtils::resolveTimestampMaxFields() throw() {
	util::DateTime::FieldData fields;
	fields.year_ = 10000;
	fields.month_ = 1;
	fields.monthDay_ = 1;
	fields.hour_ = 23;
	fields.minute_ = 59;
	fields.second_ = 59;
	fields.milliSecond_ = 999;

	try {
		util::DateTime time;
		util::DateTime::ZonedOption option;
		try {
			time.setFields(fields, option);
		}
		catch (util::UtilityException&) {
			assert(sizeof(void*) <= 4);
			util::DateTime::max(option.baseOption_).getFields(fields, option);
		}
	}
	catch (...) {
		assert(false);
	}

	return fields;
}

int64_t SQLValues::ValueUtils::resolveTimestampMaxUnixTime() throw() {
	try {
		util::DateTime::FieldData fields = resolveTimestampMaxFields();
		if (fields.year_ >= 10000) {
			fields.year_ = 9999;
			fields.month_ = 12;
			fields.monthDay_ = 31;
		}
		util::DateTime time;
		util::DateTime::ZonedOption option;
		time.setFields(fields, option);
		return time.getUnixTime();
	}
	catch (...) {
		assert(false);
		return 0;
	}
}

template<typename T, typename U>
U SQLValues::ValueUtils::errorNumericOverflow(T value, U limit, bool strict) {
	if (!strict) {
		return limit;
	}

	const TupleColumnType type =
			TypeUtils::template ValueTraits<U>::COLUMN_TYPE_AS_NUMERIC;

	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_OVERFLOW,
			"Value overflow in type conversion (fromValue=" << value <<
			", toType=" << TypeUtils::toString(type) << ")");
}

void SQLValues::ValueUtils::errorVarContextDepth() {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}


const char8_t SQLValues::ValueUtils::Constants::CONSTANT_TRUE_STR[] = "true";
const char8_t SQLValues::ValueUtils::Constants::CONSTANT_FALSE_STR[] = "false";

const double SQLValues::ValueUtils::Constants::INT64_MIN_AS_DOUBLE =
		static_cast<double>(std::numeric_limits<int64_t>::min());
const double SQLValues::ValueUtils::Constants::INT64_MAX_AS_DOUBLE =
		static_cast<double>(std::numeric_limits<int64_t>::max());

const util::DateTime::FieldData
SQLValues::ValueUtils::Constants::TIMESTAMP_MAX_FIELDS =
		resolveTimestampMaxFields();

const int64_t SQLValues::ValueUtils::Constants::TIMESTAMP_MAX_UNIX_TIME =
		resolveTimestampMaxUnixTime();

const uint32_t SQLValues::ValueUtils::Constants::LARGEST_PRIME_OF_BITS[] = {
	2, 3, 7, 13,
	31, 61, 127, 251,
	509, 1021, 2039, 4093,
	8191, 16381, 32749, 65521,
	131071, 262139, 524287, 1048573,
	2097143, 4194301, 8388593, 16777213,
	33554393, 67108859, 134217689, 268435399,
	536870909, 1073741789, 2147483647, 4294967291
};

const size_t SQLValues::ValueUtils::Constants::LARGEST_PRIME_OF_BITS_COUNT =
		sizeof(LARGEST_PRIME_OF_BITS) / sizeof(*LARGEST_PRIME_OF_BITS);


void SQLValues::ValueUtils::TimeErrorHandler::errorTimeFormat(
		std::exception &e) const {
	GS_RETHROW_USER_ERROR_CODED(
			GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
			GS_EXCEPTION_MERGE_MESSAGE(
					e, "Unacceptable timestamp value specified"));
}


void SQLValues::SharedId::assign(SharedIdManager &manager) {
	assign(&manager, 0, 0);
}

size_t SQLValues::SharedId::errorIndex() {
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}


SQLValues::SharedIdManager::SharedIdManager(
		const util::StdAllocator<void, void> &alloc) :
		entryList_(alloc),
		freeList_(alloc),
		lastId_(0) {
}

SQLValues::SharedIdManager::~SharedIdManager() {
	assert(freeList_.size() == entryList_.size());

	for (EntryList::const_iterator it = entryList_.begin();
			it != entryList_.end(); ++it) {
		assert(it->refCount_ == 0);
	}
}


SQLValues::SharedIdManager::Entry& SQLValues::SharedIdManager::createEntry(
		size_t *nextIndex) {
	const uint64_t orgId = 0;
	size_t index;
	Entry *entry;
	if (freeList_.empty()) {
		index = entryList_.size();
		freeList_.reserve(entryList_.capacity());
		entryList_.push_back(Entry());
		entry = &getEntry(index, orgId);
	}
	else {
		index = static_cast<size_t>(freeList_.back());
		entry = &getEntry(index, orgId);
		if (entry->id_ != 0 || entry->refCount_ != 0) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		freeList_.pop_back();
	}
	do {
		entry->id_ = ++lastId_;
	}
	while (entry->id_ == 0);

	*nextIndex = index;
	return *entry;
}

void SQLValues::SharedIdManager::removeEntry(Entry &entry, size_t index) {
	assert(entry.refCount_ <= 0);
	assert(&entry == &entryList_[index]);

	entry.id_ = 0;
	freeList_.push_back(index);
}

void SQLValues::SharedIdManager::errorRefCount() {
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

SQLValues::SharedIdManager::Entry& SQLValues::SharedIdManager::errorEntry() {
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}


SQLValues::LatchHolder::LatchHolder() :
		target_(NULL) {
}

SQLValues::LatchHolder::~LatchHolder() {
	reset(NULL);
}

void SQLValues::LatchHolder::reset(BaseLatchTarget *target) throw() {
	if (target_ != NULL) {
		target_->unlatch();
	}
	target_ = target;
}

void SQLValues::LatchHolder::close() throw() {
	BaseLatchTarget *const target = target_;
	target_ = NULL;

	if (target != NULL) {
		target->close();
	}
}


SQLValues::BaseLatchTarget::~BaseLatchTarget() {
}

SQLValues::BaseLatchTarget::BaseLatchTarget() {
}
