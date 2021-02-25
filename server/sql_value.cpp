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

bool SQLValues::TypeUtils::isFixed(TupleColumnType type) {
	return BaseUtils::isFixed(type);
}

bool SQLValues::TypeUtils::isSingleVar(TupleColumnType type) {
	return BaseUtils::isSingleVar(type);
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
						desiredCategory == TYPE_CATEGORY_BOOL)) {
			return setNullable(desiredBase, srcNullable);
		}
	}

	if ((srcCategory & TYPE_CATEGORY_NUMERIC_FLAG) != 0 &&
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
	else if (type == TupleTypes::TYPE_TIMESTAMP) {
		return TYPE_CATEGORY_TIMESTAMP;
	}
	else if (type == TupleTypes::TYPE_NUMERIC) {
		return TYPE_CATEGORY_NUMERIC_FLAG;
	}

	assert(false);
	return TYPE_CATEGORY_NULL;
}

void SQLValues::TypeUtils::setUpTupleInfo(
		TupleList::Info &info, const TupleColumnType *list, size_t count) {
	info.columnTypeList_ = list;
	info.columnCount_ = count;
}

const char8_t* SQLValues::TypeUtils::toString(
		TupleColumnType type, bool nullOnUnknown) {
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
		nullableAlways_(nullableAlways) {
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


const size_t SQLValues::ValueContext::Constants::DEFAULT_MAX_STRING_LENGTH =
		128 * 1024;

SQLValues::ValueContext::ValueContext(const Source &source) :
		alloc_(source.alloc_),
		varCxt_(source.varCxt_),
		extCxt_(source.extCxt_),
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
		VarContext &varCxt, util::StackAllocator *alloc) {
	if (varCxt.getVarAllocator() == NULL) {
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

size_t SQLValues::ValueContext::getMaxStringLength() {
	do {
		if (extCxt_ == NULL) {
			break;
		}

		const size_t len = extCxt_->findMaxStringLength();
		if (len == 0) {
			break;
		}

		return len;
	}
	while (false);

	return Constants::DEFAULT_MAX_STRING_LENGTH;
}

int64_t SQLValues::ValueContext::getCurrentTimeMillis() {
	if (extCxt_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int64_t time = extCxt_->getCurrentTimeMillis();
	try {
		return SQLValues::ValueUtils::checkTimestamp(time);
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

SQLValues::ValueContext::Source::Source(
		util::StackAllocator *alloc, VarContext *varCxt,
		ExtValueContext *extCxt) :
		alloc_(alloc),
		varCxt_(varCxt),
		extCxt_(extCxt) {
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
		ValueContext &cxt, const util::Vector<TupleColumnType> &typeList) {
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
		const util::Vector<TupleColumn> &inColumnList) {
	const size_t size = columnList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		const CompColumn &compColumn = columnList[i];
		const TupleColumnType compType =
				TypeUtils::toNonNullable(compColumn.getType());

		TupleValue value = tuple.get(inColumnList[i]);
		if (value.getType() != compType &&
				!TypeUtils::isNull(value.getType())) {
			value = ValuePromoter(compType)(value);
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
			TypeUtils::isSingleVar(static_cast<TupleColumnType>(entry.type_)));

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
		const util::Vector<TupleColumn> *inColumnList) :
		columnList_(columnList),
		inColumnList_(inColumnList) {
	static_cast<void>(alloc);
}


SQLValues::ArrayTuple::EmptyAssigner::EmptyAssigner(
		ValueContext &cxt, const util::Vector<TupleColumnType> &typeList) :
		typeList_(typeList) {
	for (util::Vector<TupleColumnType>::const_iterator it = typeList.begin();
			it != typeList.end(); ++it) {
		if (!TypeUtils::isNullable(*it) && !TypeUtils::isAny(*it) &&
				!TypeUtils::isFixed(*it)) {
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


SQLValues::SummaryTupleSet::SummaryTupleSet(
		ValueContext &cxt, SummaryTupleSet *parent) :
		alloc_(cxt.getAllocator()),
		varCxt_(cxt.getVarContext()),
		parent_(parent),
		columnSourceList_(alloc_),
		keySourceList_(alloc_),
		totalColumnList_(alloc_),
		readerColumnList_(alloc_),
		modifiableColumnList_(alloc_),
		fieldReaderList_(alloc_),
		getterFuncList_(alloc_),
		setterFuncList_(alloc_),
		readableTupleRequired_(false),
		keyNullIgnorable_(false),
		columnsCompleted_(false),
		bodySize_(0),
		initialBodyImage_(NULL),
		initialBodyOffset_(0),
		restBodyData_(NULL),
		restBodySize_(0),
		lobLink_(NULL),
		varDataPool_(alloc_),
		fixedDataPool_(alloc_) {
}

SQLValues::SummaryTupleSet::~SummaryTupleSet() {
	clearAll();

	while (!fixedDataPool_.empty()) {
		void *data = fixedDataPool_.back();
		if (data != NULL) {
			alloc_.base().deallocate(data);
		}
		fixedDataPool_.pop_back();
	}
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
		uint32_t readerColumnPos, bool ordering, bool ascending) {
	assert(!columnsCompleted_);
	SummaryColumn::Source src;

	src.tupleColumnPos_ = static_cast<int32_t>(readerColumnPos);
	if (ordering) {
		src.ordering_ = (ascending ? 0 : 1);
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

void SQLValues::SummaryTupleSet::addReaderColumnList(	
		const util::Vector<TupleColumnType> &typeList) {
	uint32_t pos = 0;
	for (util::Vector<TupleColumnType>::const_iterator it = typeList.begin();
			it != typeList.end(); ++it, ++pos) {
		addColumn(*it, &pos);
	}
}

void SQLValues::SummaryTupleSet::addReaderColumnList(	
		const util::Vector<TupleColumn> &columnList) {
	uint32_t pos = 0;
	for (util::Vector<TupleColumn>::const_iterator it = columnList.begin();
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
		const CompColumnList &keyList, bool first) {
	for (CompColumnList::const_iterator it = keyList.begin();
			it != keyList.end(); ++it) {
		const SQLValues::CompColumn &column = *it;
		assert(column.isColumnPosAssigned(true));
		const bool second = column.isColumnPosAssigned(false);

		const uint32_t keyPos = column.getColumnPos(first || !second);
		addKey(keyPos, column.isOrdering(), column.isAscending());
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
	util::Vector<TupleColumnType> readerColumnTypeList(alloc_);

	int32_t pos = 0;
	int32_t firstModPos = -1;
	for (ColumnSourceList::iterator it = columnSourceList_.begin();
			it != columnSourceList_.end(); ++it, ++pos) {
		it->pos_ = pos;
		const int32_t readerPos = it->tupleColumnPos_;

		const TupleColumnType type = it->type_;
		uint32_t fieldSize = 0;
		if (TypeUtils::isFixed(type)) {
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

	TupleList::Info tupleInfo;
	util::Vector<TupleColumn> tupleColumnList(alloc_);
	if (!posListByReader.empty()) {
		SQLValues::TypeUtils::setUpTupleInfo(
				tupleInfo, &readerColumnTypeList[0], readerColumnTypeList.size());

		tupleColumnList.resize(readerColumnTypeList.size());
		tupleInfo.getColumns(&tupleColumnList[0], tupleColumnList.size());
	}

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
				(TypeUtils::isFixed(type) || TypeUtils::isAny(type))))) {
			headKeyPos = pos;

			fixedBodySize -= bodySizeList[pos];
			bodySizeList[pos] = 0;
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
				if (!TypeUtils::isFixed(type) && !TypeUtils::isAny(type) &&
						bodySizeList[it->pos_] > 0) {
					const void *data = NULL;
					if (!nullableList[it->pos_]) {
						ValueContext cxt(ValueContext::ofAllocator(alloc_));
						const TupleValue &value =
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

		TypeSwitcher typeSwitcher(TypeUtils::setNullable(it->type_, nullable));

		FieldGetterFunc getter =
				typeSwitcher.get<const SummaryTuple::FieldGetter>();
		FieldSetterFunc setter = NULL;
		if (it->tupleColumnPos_ >= 0) {
			readerColumnList_.push_back(column);
			if (it->offset_ >= 0 || TypeUtils::isLob(it->type_)) {
				typedef TypeSwitcher::OpTraitsOptions<> OptionsType;
				typedef TypeSwitcher::OpTraits<void, OptionsType> TraitsType;
				FieldReaderEntry entry(
						column, typeSwitcher.getWith<
								const SummaryTuple::FieldReader, TraitsType>());
				fieldReaderList_.push_back(entry);
			}
		}
		else {
			modifiableColumnList_.push_back(column);
			setter = typeSwitcher.get<const SummaryTuple::FieldSetter>();
		}
		totalColumnList_.push_back(column);

		getterFuncList_.push_back(getter);
		setterFuncList_.push_back(setter);

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

void* SQLValues::SummaryTupleSet::duplicateLob(const TupleValue &value) {
	LobLink *link = static_cast<LobLink*>(allocateDetail(sizeof(LobLink), NULL));
	link->next_ = lobLink_;
	link->prev_ = &lobLink_;
	link->lob_ = NULL;
	link->type_ = TupleTypes::TYPE_NULL;

	if (link->next_ != NULL) {
		link->next_->prev_ = &link->next_;
	}
	(*link->prev_) = link;

	ValueContext cxt(ValueContext::ofVarContext(varCxt_));
	TupleValue dest = ValueUtils::duplicateValue(cxt, value);
	ValueUtils::moveValueToRoot(cxt, dest, cxt.getVarContextDepth(), 0);

	link->lob_ = dest.getRawData();
	link->type_ = dest.getType();

	return link;
}

void SQLValues::SummaryTupleSet::deallocateLob(void *data) {
	if (data == NULL) {
		return;
	}

	LobLink *link = static_cast<LobLink*>(data);
	if (link->prev_ == NULL) {
		return;
	}

	assert(link->lob_ != NULL);
	TupleValue value(link->lob_, link->type_);

	link->lob_ = NULL;
	link->type_ = TupleTypes::TYPE_NULL;
	if (link->next_ != NULL) {
		link->next_->prev_ = link->prev_;
	}
	(*link->prev_) = link->next_;
	deallocateDetail(link, sizeof(LobLink));

	ValueContext cxt(ValueContext::ofVarContext(varCxt_));
	ValueUtils::destroyValue(cxt, value);
}

TupleValue SQLValues::SummaryTupleSet::getLobValue(const void *data) {
	const LobLink *link = static_cast<const LobLink*>(data);

	assert(link->lob_ != NULL);
	return TupleValue(link->lob_, link->type_);
}

void SQLValues::SummaryTupleSet::clearAll() {
	restBodyData_ = NULL;
	restBodySize_ = 0;

	while (lobLink_ != NULL) {
		deallocateLob(lobLink_);
	}

	const size_t fixedBits = getFixedSizeBits();
	for (size_t i = 0; i <= fixedBits; i++) {
		if (i >= varDataPool_.size()) {
			break;
		}
		varDataPool_[i] = NULL;
	}

	initialDataPool_.second = false;

	if (!fixedDataPool_.empty()) {
		void *&dataHead = fixedDataPool_[fixedBits];
		assert(dataHead == NULL);

		for (Pool::const_iterator it = fixedDataPool_.begin();
				it != fixedDataPool_.end(); ++it) {
			void *data = *it;
			if (data == NULL) {
				continue;
			}
			static_cast<PoolData*>(data)->next_ = dataHead;
			dataHead = data;
		}
	}
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

void* SQLValues::SummaryTupleSet::allocateDetail(
		size_t size, size_t *availableSize) {
	if (parent_ != NULL) {
		return parent_->allocateDetail(size, availableSize);
	}

	const size_t fixedBits = getFixedSizeBits();
	const size_t initialBits = INITIAL_POOL_DATA_BITS;
	if (!initialDataPool_.second && initialBits != fixedBits) {
		if (initialBits >= varDataPool_.size()) {
			varDataPool_.resize(initialBits + 1);
		}
		if (initialDataPool_.first == NULL) {
			initialDataPool_.first = alloc_.allocate(1U << initialBits);
		}
		static_cast<PoolData*>(initialDataPool_.first)->next_ = NULL;
		varDataPool_[initialBits] = initialDataPool_.first;
		initialDataPool_.second = true;
	}

	const size_t minBits = getSizeBits(size, true);

	void *data = NULL;
	size_t bits = minBits;
	do {
		const size_t maxBits =
				static_cast<size_t>(std::max<uint64_t>(bits, fixedBits));
		if (maxBits >= varDataPool_.size()) {
			varDataPool_.resize(maxBits + 1);
		}

		for (; bits <= maxBits; bits++) {
			data = varDataPool_[bits];
			if (data != NULL) {
				break;
			}
		}

		if (data == NULL) {
			if (bits == fixedBits) {
				if (fixedDataPool_.empty() || fixedDataPool_.back() != NULL) {
					fixedDataPool_.push_back(NULL);
				}
				data = alloc_.base().allocate();
				fixedDataPool_.back() = data;
			}
			else {
				data = alloc_.allocate(1U << bits);
			}
		}
		else {
			varDataPool_[bits] = static_cast<PoolData*>(data)->next_;
		}

		if (availableSize != NULL) {
			break;
		}

		while (bits > minBits) {
			void *restData = static_cast<uint8_t*>(data) + (1U << (--bits));
			static_cast<PoolData*>(restData)->next_ = NULL;
			varDataPool_[bits] = restData;
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
	if (sizeBits >= varDataPool_.size()) {
		assert(false);
		return;
	}

	void *&ref = varDataPool_[sizeBits];
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
		const TupleValue &v1, const TupleValue &v2,
		bool sensitive, bool ascending) {
	return ValueComparator(
			ValueAccessor::ofValue(v1), ValueAccessor::ofValue(v2),
			sensitive, ascending);
}

SQLValues::ValueComparator::DefaultResultType
SQLValues::ValueComparator::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	typedef SwitcherOf<
			void, false, true, true, ValueAccessor::ByValue>::Type SwitcherType;
	const bool nullIgnorable = false;
	return SwitcherType(*this, nullIgnorable).getWith<
			const ValueComparator, SwitcherType::DefaultTraitsType>()(*this, v1, v2);
}

bool SQLValues::ValueComparator::isEmpty() const {
	return getColumnTypeSwitcher(true, false).isAny();
}

bool SQLValues::ValueComparator::isSameVariant(
		const ValueComparator &another, bool nullIgnorable,
		bool anotherNullIgnorable) const {
	return (!sensitive_ == !another.sensitive_ &&
			!ascending_ == !another.ascending_ &&
			accessor1_.isSameVariant(
					another.accessor1_, nullIgnorable, anotherNullIgnorable) &&
			accessor2_.isSameVariant(
					another.accessor2_, nullIgnorable, anotherNullIgnorable));
}

SQLValues::TypeSwitcher
SQLValues::ValueComparator::getColumnTypeSwitcher(
		bool promotable, bool nullIgnorable) const {
	TypeSwitcher switcher(
			accessor1_.getColumnType(promotable, true),
			accessor2_.getColumnType(promotable, (!promotable || false)));
	if (nullIgnorable) {
		switcher = switcher.toNonNullable();
	}
	return switcher;
}


bool SQLValues::ValueEq::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool sensitive = false;
	return ValueComparator::ofValues(v1, v2, sensitive, true)(v1, v2) == 0;
}


bool SQLValues::ValueLess::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool sensitive = true;
	return ValueComparator::ofValues(v1, v2, sensitive, true)(v1, v2) < 0;
}


bool SQLValues::ValueGreater::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool sensitive = true;
	return ValueComparator::ofValues(v1, v2, sensitive, true)(v1, v2) > 0;
}


SQLValues::ValueFnv1aHasher SQLValues::ValueFnv1aHasher::ofValue(
		const TupleValue &value, int64_t seed) {
	return ValueFnv1aHasher(ValueAccessor::ofValue(value), seed);
}

uint32_t SQLValues::ValueFnv1aHasher::operator()(
		const TupleValue &value) const {
	typedef TypeSwitcher::OpTraitsOptions<
			ValueDigester::VariantTraits<void, ValueAccessor::ByValue> > Options;
	typedef TypeSwitcher::OpTraits<int64_t, Options, const TupleValue> Traits;

	const TypeSwitcher switcher(accessor_.getColumnType(false, true));
	const int64_t ret =
			switcher.getWith<const ValueFnv1aHasher, Traits>()(*this, value);
	return static_cast<uint32_t>(ret < 0 ? -ret : ret);
}


bool SQLValues::ValueDigester::isSameVariant(
		const ValueDigester &another, bool nullIgnorable,
		bool anotherNullIgnorable) const {
	return (!ordering_ == !another.ordering_ &&
			!ascending_ == !another.ascending_ &&
			accessor_.isSameVariant(
					another.accessor_, nullIgnorable, anotherNullIgnorable));
}

bool SQLValues::ValueDigester::isEmpty() const {
	return getColumnTypeSwitcher(false, false).isAny();
}

SQLValues::TypeSwitcher
SQLValues::ValueDigester::getColumnTypeSwitcher(
		bool nullIgnorable, bool nullableAlways) const {
	TypeSwitcher switcher(accessor_.getColumnType(false, true));
	if (nullIgnorable) {
		switcher = switcher.toNonNullable();
	}
	if (nullableAlways) {
		switcher = switcher.toNullableAlways();
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
		return ValueUtils::toAnyByTimestamp(ValueUtils::toTimestamp(cxt, src));
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

TupleValue SQLValues::ValuePromoter::operator()(const TupleValue &src) const {
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
		const CompColumnList &columnList, bool sensitive, bool withDigest,
		bool nullIgnorable) :
		columnList_(columnList),
		sensitive_(sensitive),
		withDigest_(withDigest),
		nullIgnorable_(nullIgnorable) {
}

SQLValues::ValueComparator SQLValues::TupleComparator::initialComparatorAt(
		CompColumnList::const_iterator it, bool reversed) const {
	bool ascending = it->isAscending();
	if (it->isOrdering() && reversed) {
		ascending = !ascending;
	}
	return ValueComparator(
			ValueAccessor(it, columnList_), ValueAccessor(it, columnList_),
			sensitive_, ascending);
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

		if ((TypeUtils::isFixed(type) &&
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
	if (columnList_.size() >= 1) {
		const CompColumn &column = columnList_.front();

		if (column.isOrdering()) {
			return true;
		}

		const TupleColumnType type = column.getType();
		assert(!TypeUtils::isNull(type));

		if ((TypeUtils::isFixed(type) || TypeUtils::isAny(type)) &&
				columnList_.size() <= 1) {
			return true;
		}
	}

	return false;
}


SQLValues::TupleRangeComparator::TupleRangeComparator(
		const CompColumnList &columnList, bool nullIgnorable) :
		base_(columnList, false, false, nullIgnorable),
		columnList_(columnList),
		nullIgnorable_(nullIgnorable) {
}


SQLValues::TupleDigester::TupleDigester(
		const CompColumnList &columnList, const bool *ordering,
		bool nullIgnorable, bool nullableAlways) :
		columnList_(columnList),
		ordering_((ordering == NULL ?
				isOrderingAvailable(columnList, false) : *ordering)),
		nullIgnorable_(nullIgnorable),
		nullableAlways_(nullableAlways) {
}

bool SQLValues::TupleDigester::isOrdering() const {
	return ordering_;
}

bool SQLValues::TupleDigester::isOrderingAvailable(
		const CompColumnList &columnList, bool promotable) {
	if (columnList.empty() || columnList.front().isOrdering()) {
		return true;
	}
	return TupleComparator(
			columnList, false, true, true).isDigestOnly(promotable);
}

SQLValues::ValueDigester SQLValues::TupleDigester::initialDigesterAt(
		CompColumnList::const_iterator it) const {
	return ValueDigester(
			ValueAccessor(it, columnList_), ordering_, true, 0);
}


SQLValues::TupleConverter::TupleConverter(
		const CompColumnList &srcColumnList,
		const CompColumnList &destColumnList) :
		srcColumnList_(srcColumnList),
		destColumnList_(destColumnList) {
}

void SQLValues::TupleConverter::operator()(
		ValueContext &cxt,
		const ReadableTuple &src, WritableTuple &dest) const {
	CompColumnList::const_iterator srcIt = srcColumnList_.begin();
	CompColumnList::const_iterator destIt = destColumnList_.begin();
	for (; srcIt != srcColumnList_.end(); ++srcIt, ++destIt) {
		const TupleColumn &srcColumn = srcIt->getTupleColumn1();
		const TupleColumn &destColumn = destIt->getTupleColumn1();
		const ValueConverter conv(srcIt->getType());
		dest.set(destColumn, conv(cxt, src.get(srcColumn)));
	}
}


SQLValues::TuplePromoter::TuplePromoter(
		const CompColumnList &srcColumnList,
		const CompColumnList &destColumnList) :
		srcColumnList_(srcColumnList),
		destColumnList_(destColumnList) {
}

void SQLValues::TuplePromoter::operator()(
		const ReadableTuple &src, WritableTuple &dest) const {
	CompColumnList::const_iterator srcIt = srcColumnList_.begin();
	CompColumnList::const_iterator destIt = destColumnList_.begin();
	for (; srcIt != srcColumnList_.end(); ++srcIt, ++destIt) {
		const TupleColumn &srcColumn = srcIt->getTupleColumn1();
		const TupleColumn &destColumn = destIt->getTupleColumn1();
		const ValuePromoter promo(srcIt->getType());
		dest.set(destColumn, promo(src.get(srcColumn)));
	}
}


SQLValues::StringBuilder::StringBuilder(ValueContext &cxt, size_t capacity) :
		data_(NULL),
		dynamicBuffer_(resolveAllocator(cxt)),
		size_(0),
		limit_(cxt.getMaxStringLength()) {
	data_ = localBuffer_;

	if (capacity > 0) {
		resize(std::min(capacity, limit_));
		resize(0);
	}
}

SQLValues::StringBuilder::StringBuilder(
		const util::StdAllocator<void, void> &alloc) :
		data_(NULL),
		dynamicBuffer_(alloc),
		size_(0),
		limit_(std::numeric_limits<size_t>::max()) {
	data_ = localBuffer_;
}

void SQLValues::StringBuilder::setLimit(size_t limit) {
	limit_ = limit;
	resize(size_);
}

void SQLValues::StringBuilder::appendCode(util::CodePoint c) {
	char8_t *it = data_ + size_;
	while (!util::UTF8Utils::encodeRaw(&it, data_ + capacity(), c)) {
		const size_t orgSize = size_;
		const size_t orgLimit = limit_;

		limit_ = std::numeric_limits<size_t>::max();
		resize(capacity() * 2);
		size_ = orgSize;
		limit_ = orgLimit;

		it = data_ + size_;
	}

	resize(it - data_);
}

void SQLValues::StringBuilder::append(const char8_t *data, size_t size) {
	if (std::numeric_limits<size_t>::max() - size < size_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
				"Too large string in appending (currentSize=" << size_ <<
				", appendingSize=" << size << ")");
	}

	resize(size_ + size);
	memcpy(data_ + size_ - size, data, size);
}

void SQLValues::StringBuilder::appendAll(const char8_t *str) {
	appendAll(*util::SequenceUtils::toReader(str));
}

template<typename R>
void SQLValues::StringBuilder::appendAll(R &reader) {
	for (size_t next; (next = reader.getNext()) > 0; reader.step(next)) {
		append(&reader.get(), next);
	}
}

TupleValue SQLValues::StringBuilder::build(ValueContext &cxt) {
	TupleValue::SingleVarBuilder builder(
			cxt.getVarContext(), TupleTypes::TYPE_STRING, size_);
	builder.append(data_, size_);
	return builder.build();
}

size_t SQLValues::StringBuilder::size() const {
	return size_;
}

size_t SQLValues::StringBuilder::capacity() const {
	if (data_ == localBuffer_) {
		return sizeof(localBuffer_);
	}
	else {
		return dynamicBuffer_.capacity();
	}
}

void SQLValues::StringBuilder::resize(size_t size) {
	if (size > limit_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
				"Too large string (limit=" << limit_ <<
				", requested=" << size << ")");
	}

	if (data_ == localBuffer_) {
		if (size > sizeof(localBuffer_)) {
			dynamicBuffer_.resize(size);
			data_ = dynamicBuffer_.data();
			memcpy(data_, localBuffer_, size_);
		}
	}
	else {
		dynamicBuffer_.resize(size);
		data_ = dynamicBuffer_.data();
	}
	size_ = size;
}

char8_t* SQLValues::StringBuilder::data() {
	return data_;
}

util::StdAllocator<void, void> SQLValues::StringBuilder::resolveAllocator(
		ValueContext &cxt) {
	SQLVarSizeAllocator *varAlloc = cxt.getVarContext().getVarAllocator();
	if (varAlloc != NULL) {
		return *varAlloc;
	}
	else {
		return cxt.getAllocator();
	}
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
	return compareSequence(*toStringReader(v1), *toStringReader(v2));
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

int64_t SQLValues::ValueUtils::toTimestamp(
		ValueContext &cxt, const TupleValue &src) {
	if (src.getType() == TupleTypes::TYPE_STRING) {
		util::StackAllocator &alloc = cxt.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		const TupleString::BufferInfo &strBuf = TupleString(src).getBuffer();
		const bool zoneSpecified = false;
		return parseTimestamp(strBuf, cxt.getTimeZone(), zoneSpecified, alloc);
	}

	return checkTimestamp(toLong(src));
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
	const int32_t type = src.getType();

	if ((type & TupleTypes::TYPE_MASK_ARRAY_VAR) ||
			(type & TupleTypes::TYPE_MASK_SUB) ==
					(TupleTypes::TYPE_DOUBLE & TupleTypes::TYPE_MASK_SUB)) {
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
	else if (type == TupleTypes::TYPE_TIMESTAMP) {
		formatTimestamp(builder, value.get<int64_t>(), cxt.getTimeZone());
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
		StringBuilder &builder, const util::DateTime &value,
		const util::TimeZone &zone) {
	util::DateTime::ZonedOption option;
	applyDateTimeOption(option);
	option.zone_ = zone;

	value.writeTo(builder, option, TimeErrorHandler());
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

int64_t SQLValues::ValueUtils::parseTimestamp(
		const TupleString::BufferInfo &src, const util::TimeZone &zone,
		bool zoneSpecified, util::StdAllocator<void, void> alloc) {
	typedef util::BasicString<
			char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, void> > String;

	String str(src.first, src.second, alloc);

	bool zoned = true;
	const size_t size = str.size();
	if (size <= 11 && str.find('-') != util::String::npos) {
		str.append("T00:00:00");
		zoned = false;
	}
	else if ((size == 12 || size == 8) && str.find(':') != util::String::npos) {
		str.insert(0, "1970-01-01T");
		zoned = false;
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

	int64_t time;
	util::TimeZone zoneByStr;
	try {
		const bool throwOnError = true;

		{
			util::DateTime dateTime;
			dateTime.parse(str.c_str(), str.size(), throwOnError, option);
			time = dateTime.getUnixTime();
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

	return time;
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


TupleValue SQLValues::ValueUtils::toAnyByTimestamp(int64_t src) {
	return TupleValue(&src, TupleTypes::TYPE_TIMESTAMP);
}

TupleValue SQLValues::ValueUtils::createEmptyValue(
		ValueContext &cxt, TupleColumnType type) {
	if (TypeUtils::isAny(type) || TypeUtils::isNullable(type)) {
		return TupleValue();
	}

	const TupleColumnType baseType = TypeUtils::toNonNullable(type);
	if (TypeUtils::isFixed(baseType)) {
		const int64_t emptyLong = 0;
		return ValueConverter(baseType)(cxt, TupleValue(emptyLong));
	}

	if (baseType == TupleTypes::TYPE_STRING) {
		StringBuilder builder(cxt);
		return builder.build(cxt);
	}
	else if (baseType == TupleTypes::TYPE_BLOB) {
		LobBuilder builder(cxt.getVarContext(), TupleTypes::TYPE_BLOB, 0);
		return builder.build();
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

SQLValues::StringBuilder& SQLValues::ValueUtils::initializeWriter(
		ValueContext &cxt, util::LocalUniquePtr<StringBuilder> &writerPtr,
		uint64_t initialCapacity) {
	const size_t strCapacity = static_cast<size_t>(std::min<uint64_t>(
			std::numeric_limits<size_t>::max(), initialCapacity));
	writerPtr = UTIL_MAKE_LOCAL_UNIQUE(
			writerPtr, StringBuilder, cxt, strCapacity);
	return *writerPtr;
}

SQLValues::LobBuilder& SQLValues::ValueUtils::initializeWriter(
		ValueContext &cxt, util::LocalUniquePtr<LobBuilder> &writerPtr,
		uint64_t initialCapacity) {
	writerPtr = UTIL_MAKE_LOCAL_UNIQUE(
			writerPtr, LobBuilder, cxt.getVarContext(),
			TupleTypes::TYPE_BLOB, toLobCapacity(initialCapacity));
	return *writerPtr;
}


TupleColumnType SQLValues::ValueUtils::toColumnType(const TupleValue &value) {
	const TupleColumnType type = value.getType();
	if (TypeUtils::isNull(type)) {
		return TupleTypes::TYPE_ANY;
	}
	return type;
}


int64_t SQLValues::ValueUtils::checkTimestamp(int64_t tsValue) {
	util::DateTime::Option option;
	applyDateTimeOption(option);

	util::DateTime dateTime;
	try {
		dateTime.setUnixTime(tsValue, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
				GS_EXCEPTION_MERGE_MESSAGE(
						e, "Unacceptable timestamp value specified"));
	}

	return dateTime.getUnixTime();
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
	if (TypeUtils::isFixed(type)) {
		return src;
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

void SQLValues::ValueUtils::moveValueToRoot(
		ValueContext &cxt, TupleValue &value,
		size_t currentDepth, size_t targetDepth) {
	assert(currentDepth == cxt.getVarContextDepth());
	assert(currentDepth > targetDepth);

	if (!TypeUtils::isFixed(value.getType())) {
		if (currentDepth <= 0) {
			errorVarContextDepth();
		}
		VarContext &varCxt = cxt.getVarContext();
		varCxt.moveValueToParent(currentDepth - targetDepth, value);
	}
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


void SQLValues::ValueUtils::TimeErrorHandler::errorTimeFormat(
		std::exception &e) const {
	GS_RETHROW_USER_ERROR_CODED(
			GS_ERROR_SQL_PROC_VALUE_OVERFLOW, e,
			GS_EXCEPTION_MERGE_MESSAGE(
					e, "Unacceptable timestamp value specified"));
}


SQLValues::SharedId::SharedId() :
		index_(0),
		id_(0),
		manager_(NULL) {
}

SQLValues::SharedId::SharedId(const SharedId &another) :
		index_(0),
		id_(0),
		manager_(NULL) {
	*this = another;
}

SQLValues::SharedId& SQLValues::SharedId::operator=(const SharedId &another) {
	assign(another.manager_, another.index_, another.id_);
	return *this;
}

SQLValues::SharedId::~SharedId() {
	try {
		clear();
	}
	catch (...) {
		assert(false);
	}
}

void SQLValues::SharedId::assign(SharedIdManager &manager) {
	assign(&manager, 0, 0);
}

void SQLValues::SharedId::clear() {
	SharedIdManager *orgManager = manager_;
	if (orgManager == NULL) {
		return;
	}

	const size_t orgIndex = index_;
	const uint64_t orgId = id_;

	index_ = 0;
	id_ = 0;
	manager_ = NULL;

	orgManager->release(orgIndex, orgId, this);
}

bool SQLValues::SharedId::isEmpty() const {
	return (manager_ == NULL);
}

bool SQLValues::SharedId::operator==(const SharedId &another) const {
	if (manager_ != another.manager_) {
		assert(false);
		return false;
	}
	return (index_ == another.index_);
}

bool SQLValues::SharedId::operator<(const SharedId &another) const {
	if (manager_ != another.manager_) {
		assert(false);
		return false;
	}
	return (index_ < another.index_);
}

size_t SQLValues::SharedId::getIndex(const SharedIdManager &manager) const {
	if (manager_ != &manager) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return index_;
}

void SQLValues::SharedId::assign(
		SharedIdManager *manager, size_t orgIndex, uint64_t orgId) {
	clear();

	if (manager == NULL) {
		return;
	}

	const size_t nextIndex = manager->allocate(orgIndex, orgId, this);
	const uint64_t nextId = manager->getId(nextIndex);

	index_ = nextIndex;
	id_ = nextId;
	manager_ = manager;
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

size_t SQLValues::SharedIdManager::allocate(
		size_t orgIndex, uint64_t orgId, void *key) {
	size_t index;
	Entry *entry;
	if (orgId == 0) {
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
	}
	else {
		index = orgIndex;
		entry = &getEntry(index, orgId);
	}

	checkKey(true, entry->id_, key);
	++entry->refCount_;
	return index;
}

void SQLValues::SharedIdManager::release(size_t index, uint64_t id, void *key) {
	Entry &entry = getEntry(index, id);
	if (entry.refCount_ <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	--entry.refCount_;
	checkKey(false, id, key);

	if (entry.refCount_ <= 0) {
		entry.id_ = 0;
		freeList_.push_back(index);
	}
}

void SQLValues::SharedIdManager::checkKey(
		bool allocating, uint64_t id, void *key) {
	static_cast<void>(allocating);
	static_cast<void>(id);
	static_cast<void>(key);
}

uint64_t SQLValues::SharedIdManager::getId(size_t index) {
	return getEntry(index, 0).id_;
}

SQLValues::SharedIdManager::Entry& SQLValues::SharedIdManager::getEntry(
		size_t index, uint64_t id) {
	if (index >= entryList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	Entry &entry = entryList_[index];

	if (id != 0 && entry.id_ != id) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return entry;
}

SQLValues::SharedIdManager::Entry::Entry() :
		id_(0),
		refCount_(0) {
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
