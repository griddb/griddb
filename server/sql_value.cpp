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

SQLValues::VarContext& SQLValues::ValueContext::getVarContext() {
	assert(varCxt_ != NULL);
	return *varCxt_;
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


SQLValues::CompColumn::CompColumn() :
		type_(TupleTypes::TYPE_NULL),
		columnPos1_(std::numeric_limits<uint32_t>::max()),
		columnPos2_(std::numeric_limits<uint32_t>::max()),
		ascending_(true),
		eitherUnbounded_(false),
		lowerUnbounded_(false),
		eitherExclusive_(false),
		lowerExclusive_(false) {
}

const SQLValues::TupleColumn& SQLValues::CompColumn::getTupleColumn1() const {
	return tupleColumn1_;
}

const SQLValues::TupleColumn& SQLValues::CompColumn::getTupleColumn2() const {
	return tupleColumn2_;
}

void SQLValues::CompColumn::setTupleColumn1(const TupleColumn &column) {
	tupleColumn1_ = column;
}

void SQLValues::CompColumn::setTupleColumn2(const TupleColumn &column) {
	tupleColumn2_ = column;
}

void SQLValues::CompColumn::setTupleColumn(
		const TupleColumn &column, bool first) {
	(first ? tupleColumn1_ : tupleColumn2_) = column;
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

bool SQLValues::CompColumn::isAscending() const {
	return ascending_;
}

void SQLValues::CompColumn::setAscending(bool ascending) {
	ascending_ = ascending;
}

bool SQLValues::CompColumn::isEitherUnbounded() const {
	return eitherUnbounded_;
}

bool SQLValues::CompColumn::isLowerUnbounded() const {
	return lowerUnbounded_;
}

void SQLValues::CompColumn::setEitherUnbounded(bool unbounded) {
	eitherUnbounded_ = unbounded;
}

void SQLValues::CompColumn::setLowerUnbounded(bool unbounded) {
	lowerUnbounded_ = unbounded;
}

bool SQLValues::CompColumn::isEitherExclusive() const {
	return eitherExclusive_;
}

bool SQLValues::CompColumn::isLowerExclusive() const {
	return lowerExclusive_;
}

void SQLValues::CompColumn::setEitherExclusive(bool exclusive) {
	eitherExclusive_ = exclusive;
}

void SQLValues::CompColumn::setLowerExclusive(bool exclusive) {
	lowerExclusive_ = exclusive;
}


SQLValues::ArrayTuple::ArrayTuple(util::StackAllocator &alloc) :
		valueList_(alloc),
		varCxt_(NULL) {
}

SQLValues::ArrayTuple::~ArrayTuple() {
	try {
		clear();
	}
	catch (...) {
	}
}

bool SQLValues::ArrayTuple::isEmpty() {
	return valueList_.empty();
}

void SQLValues::ArrayTuple::clear() {
	resize(0);
	varCxt_ = NULL;
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
		ValueContext &cxt, ReadableTuple &tuple,
		const CompColumnList &columnList) {
	const size_t size = columnList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		set(i, cxt, tuple.get(columnList[i].getTupleColumn1()));
	}
}

void SQLValues::ArrayTuple::assign(
		ValueContext &cxt, ReadableTuple &tuple,
		const util::Vector<TupleColumn> &columnList) {
	const size_t size = columnList.size();
	resize(size);

	for (size_t i = 0; i < size; i++) {
		set(i, cxt, tuple.get(columnList[i]));
	}
}

void SQLValues::ArrayTuple::swap(ArrayTuple &another) {
	valueList_.swap(another.valueList_);
	std::swap(varCxt_, another.varCxt_);
}

const TupleValue& SQLValues::ArrayTuple::get(size_t index) const {
	assert(index < valueList_.size());
	return valueList_[index];
}

void SQLValues::ArrayTuple::set(
		size_t index, ValueContext &cxt, const TupleValue &value) {
	assert(index < valueList_.size());
	TupleValue &dest = valueList_[index];

	assert(&cxt.getVarContext() == varCxt_ || varCxt_ == NULL);
	varCxt_ = &cxt.getVarContext();
	ValueUtils::destroyValue(cxt, dest);

	dest = ValueUtils::duplicateValue(cxt, value);

	if (!ValueUtils::isNull(dest)) {
		ValueUtils::moveValueToRoot(cxt, dest, cxt.getVarContextDepth(), 0);
	}
}

void SQLValues::ArrayTuple::resize(size_t size) {
	const size_t lastSize = valueList_.size();
	if (lastSize <= size) {
		if (lastSize < size) {
			valueList_.resize(size);
		}
		return;
	}

	assert(varCxt_ != NULL);
	ValueContext cxt(ValueContext::ofVarContext(*varCxt_));

	while (valueList_.size() > size) {
		set(valueList_.size() - 1, cxt, TupleValue());
		valueList_.pop_back();
	}
}


SQLValues::ValueComparator::ValueComparator(
		TupleColumnType type, bool sensitive) :
		type_(type),
		base_(sensitive) {
}

int32_t SQLValues::ValueComparator::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	return TypeSwitcher(type_)(base_, v1, v2);
}


SQLValues::ValueEq::ValueEq(TupleColumnType type) :
		comp_(type, false) {
}

bool SQLValues::ValueEq::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	return comp_(v1, v2) == 0;
}


SQLValues::ValueLess::ValueLess(TupleColumnType type) :
		comp_(type, true) {
}

bool SQLValues::ValueLess::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	return comp_(v1, v2) < 0;
}


SQLValues::ValueGreater::ValueGreater(TupleColumnType type) :
		comp_(type, true) {
}

bool SQLValues::ValueGreater::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	return comp_(v1, v2) > 0;
}


SQLValues::ValueFnv1aHasher::ValueFnv1aHasher(TupleColumnType type) :
		type_(type),
		seed_(ValueUtils::fnv1aHashInit()) {
}

SQLValues::ValueFnv1aHasher::ValueFnv1aHasher(
		TupleColumnType type, uint32_t seed) :
		type_(type),
		seed_(seed) {
}

uint32_t SQLValues::ValueFnv1aHasher::operator()(
		const TupleValue &value) const {
	if (ValueUtils::isNull(value)) {
		return seed_;
	}

	switch (type_) {
	case TupleTypes::TYPE_BYTE:
		return ValueUtils::fnv1aHashIntegral(
				seed_, ValueUtils::getValue<int8_t>(value));
	case TupleTypes::TYPE_SHORT:
		return ValueUtils::fnv1aHashIntegral(
				seed_, ValueUtils::getValue<int16_t>(value));
	case TupleTypes::TYPE_INTEGER:
		return ValueUtils::fnv1aHashIntegral(
				seed_, ValueUtils::getValue<int32_t>(value));
	case TupleTypes::TYPE_LONG:
		return ValueUtils::fnv1aHashIntegral(seed_, value.get<int64_t>());
	case TupleTypes::TYPE_FLOAT:
		return ValueUtils::fnv1aHashFloating(seed_, value.get<float>());
	case TupleTypes::TYPE_DOUBLE:
		return ValueUtils::fnv1aHashFloating(seed_, value.get<double>());
	case TupleTypes::TYPE_TIMESTAMP:
		return ValueUtils::fnv1aHashIntegral(seed_, value.get<int64_t>());
	case TupleTypes::TYPE_BOOL:
		return ValueUtils::fnv1aHashIntegral(
				seed_, ValueUtils::getValue<int8_t>(value));
	case TupleTypes::TYPE_STRING:
		return ValueUtils::fnv1aHashSequence(
				seed_, *ValueUtils::toStringReader(value));
	case TupleTypes::TYPE_BLOB:
		return ValueUtils::fnv1aHashSequence(
				seed_, *ValueUtils::toLobReader(value));
	default:
		assert(false);
		return 0;
	}
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


SQLValues::ValueWriter::ValueWriter(
		const TupleColumn &srcColumn, const TupleColumn &destColumn) :
		type_(TupleTypes::TYPE_NULL),
		srcColumn_(srcColumn),
		destColumn_(destColumn) {
	const TupleColumnType srcType = srcColumn.getType();
	const TupleColumnType destType = destColumn.getType();

	if (TypeUtils::toNonNullable(srcType) ==
			TypeUtils::toNonNullable(destType) &&
			!(TypeUtils::isNullable(srcType) &&
			!TypeUtils::isNullable(destType))) {
		type_ = TypeUtils::toNonNullable(destType);
	}
}


SQLValues::TupleComparator::TupleComparator(
		const CompColumnList &columnList, bool sensitive) :
		columnList_(columnList),
		promotable_(isPromotable(columnList)),
		sensitive_(sensitive),
		forKeyOnlyArray_(true) {
}

void SQLValues::TupleComparator::setForKeyOnlyArray(bool forKeyOnlyArray) {
	forKeyOnlyArray_ = forKeyOnlyArray;
}

SQLValues::TypeSwitcher SQLValues::TupleComparator::getTypeSwitcher() const {
	assert(!columnList_.empty());
	const TupleColumnType type = (promotable_ ?
			static_cast<TupleColumnType>(TupleTypes::TYPE_ANY) :
			TypeUtils::toNonNullable(columnList_.front().getType()));
	return TypeSwitcher(type);
}

bool SQLValues::TupleComparator::isPromotable(
		const CompColumnList &columnList) {
	assert(!columnList.empty());
	const TupleColumnType frontType =
			TypeUtils::toNonNullable(columnList.front().getType());

	for (CompColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		const TupleColumnType type = TypeUtils::toNonNullable(it->getType());
		if (type != frontType) {
			continue;
		}
		if (type != TypeUtils::toNonNullable(
				it->getTupleColumn1().getType())) {
			return true;
		}
		if (type != TypeUtils::toNonNullable(
				it->getTupleColumn2().getType())) {
			return true;
		}
	}

	return false;
}


SQLValues::TupleRangeComparator::TupleRangeComparator(
		const CompColumnList &columnList) :
		base_(columnList, false),
		columnList_(columnList) {
}

int32_t SQLValues::TupleRangeComparator::operator()(
		const ReadableTuple &t1, const ReadableTuple &t2) const {
	return base_.getTypeSwitcher()(*this, t1, t2);
}

template<typename T>
int32_t SQLValues::TupleRangeComparator::TypeAt<T>::operator()(
		const ReadableTuple &t1, const ReadableTuple &t2) const {
	const bool sensitive = false;
	const bool forKeyOnlyArray = false;
	for (CompColumnList::const_iterator it = base_.columnList_.begin();
			it != base_.columnList_.end(); ++it) {
		const int32_t ret = TupleComparator::compareElement<
				T, ReadableTuple, ReadableTuple>(
				*it, static_cast<uint32_t>(it - base_.columnList_.begin()),
				sensitive, forKeyOnlyArray, t1, t2);

		if (ret != 0) {
			if (it->isEitherUnbounded()) {
				if (it->isLowerUnbounded()) {
					if (ret < 0) {
						return 0;
					}
				}
				else {
					if (ret > 0) {
						return 0;
					}
				}
			}
			return ret;
		}

		if (it->isEitherExclusive()) {
			if (it->isLowerExclusive()) {
				return -1;
			}
			else {
				return 1;
			}
		}
	}

	return 0;
}


SQLValues::TupleEq::TupleEq(const CompColumnList &columnList) :
		comp_(columnList, false) {
}


SQLValues::TupleLess::TupleLess(const CompColumnList &columnList) :
		comp_(columnList, true) {
}


SQLValues::TupleGreater::TupleGreater(const CompColumnList &columnList) :
		comp_(columnList, true) {
}


SQLValues::TupleFnv1aHasher::TupleFnv1aHasher(
		const CompColumnList &columnList) :
		columnList_(columnList),
		seed_(ValueUtils::fnv1aHashInit()) {
}

SQLValues::TupleFnv1aHasher::TupleFnv1aHasher(
		const CompColumnList &columnList, uint32_t seed) :
		columnList_(columnList),
		seed_(seed) {
}

uint32_t SQLValues::TupleFnv1aHasher::operator()(
		const ReadableTuple &tuple) const {
	uint32_t hash = seed_;
	for (CompColumnList::const_iterator it = columnList_.begin();
			it != columnList_.end(); ++it) {
		const TupleColumn &column = it->getTupleColumn1();
		hash = ValueFnv1aHasher(it->getType(), hash)(tuple.get(column));
	}
	return hash;
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


bool SQLValues::TypeUtils::isAny(TupleColumnType type) {
	return BaseUtils::isAny(type);
}

bool SQLValues::TypeUtils::isNull(TupleColumnType type) {
	return BaseUtils::isNull(type);
}

bool SQLValues::TypeUtils::isFixed(TupleColumnType type) {
	return BaseUtils::isFixed(type);
}

bool SQLValues::TypeUtils::isArray(TupleColumnType type) {
	return BaseUtils::isArray(type);
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


uint32_t SQLValues::ValueUtils::fnv1aHashInit() {
	const uint32_t fnvOffsetBasis = 2166136261U;
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
	const uint32_t fnvPrime = 16777619U;

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

TupleValue SQLValues::ValueUtils::toAnyByBool(bool src) {
	return TupleValue(src);
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

SQLValues::StringReaderRef SQLValues::ValueUtils::toStringReader(
		const char8_t *src) {
	return util::SequenceUtils::toReader(src);
}

SQLValues::StringReaderRef SQLValues::ValueUtils::toStringReader(
		const TupleString &src) {
	const TupleString::BufferInfo &buf = src.getBuffer();
	return util::SequenceUtils::toReader(buf.first, buf.first + buf.second);
}

SQLValues::StringReaderRef SQLValues::ValueUtils::toStringReader(
		const TupleString::BufferInfo &src) {
	return util::SequenceUtils::toReader(src.first, src.first + src.second);
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


bool SQLValues::ValueUtils::isNull(const TupleValue &value) {
	return TypeUtils::isNull(value.getType());
}

TupleColumnType SQLValues::ValueUtils::toColumnType(const TupleValue &value) {
	const TupleColumnType type = value.getType();
	if (TypeUtils::isNull(type)) {
		return TupleTypes::TYPE_ANY;
	}
	return type;
}

TupleColumnType SQLValues::ValueUtils::toCompType(
		const TupleValue &v1, const TupleValue &v2) {
	const bool anyAsNull = true;
	const TupleColumnType anyType = TupleTypes::TYPE_ANY;
	const TupleColumnType type = TypeUtils::findPromotionType(
			(isNull(v1) ? anyType : v1.getType()),
			(isNull(v2) ? anyType : v2.getType()),
			anyAsNull);
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


SQLValues::ReadableTupleRef::ReadableTupleRef(TupleListReader &reader) :
		reader_(&reader) {
}


SQLValues::TupleListReader& SQLValues::ReadableTupleRef::getReader() const {
	return *reader_;
}


SQLValues::ReadableTupleRef::operator SQLValues::ReadableTuple() const {
	return reader_->get();
}


SQLValues::ArrayTupleRef::ArrayTupleRef(ArrayTuple &tuple) :
		tuple_(&tuple) {
}

SQLValues::ArrayTuple& SQLValues::ArrayTupleRef::getTuple() const {
	return *tuple_;
}


SQLValues::ArrayTupleRef::operator SQLValues::ArrayTuple&() const {
	return *tuple_;
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
