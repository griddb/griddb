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

#include "sql_expression_aggr.h"


const SQLExprs::ExprRegistrar
SQLAggrExprs::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLAggrExprs::Registrar::operator()() const {
	add<SQLType::AGG_AVG, Functions::Avg>();
	add<SQLType::AGG_COUNT_ALL, Functions::CountAll>();
	add<SQLType::AGG_COUNT_COLUMN, Functions::CountColumn>();
	add<SQLType::AGG_GROUP_CONCAT, Functions::GroupConcat>();
	add<SQLType::AGG_LAG, Functions::LagLead>();
	add<SQLType::AGG_LEAD, Functions::LagLead>();
	add<SQLType::AGG_MAX, Functions::Max>();
	add<SQLType::AGG_MEDIAN, Functions::Median>();
	add<SQLType::AGG_MIN, Functions::Min>();
	add<SQLType::AGG_ROW_NUMBER, Functions::RowNumber>();
	add<SQLType::AGG_STDDEV0, Functions::Stddev0>();
	add<SQLType::AGG_STDDEV_POP, Functions::StddevPop>();
	add<SQLType::AGG_STDDEV_SAMP, Functions::StddevSamp>();
	add<SQLType::AGG_SUM, Functions::Sum>();
	add<SQLType::AGG_TOTAL, Functions::Total>();
	add<SQLType::AGG_VAR_POP, Functions::VarPop>();
	add<SQLType::AGG_VAR_SAMP, Functions::VarSamp>();
	add<SQLType::AGG_VARIANCE0, Functions::Variance0>();

	add<SQLType::AGG_FIRST, Functions::First>();
	add<SQLType::AGG_LAST, Functions::Last>();
	add<SQLType::AGG_FOLD_EXISTS, Functions::FoldExists>();
	add<SQLType::AGG_FOLD_NOT_EXISTS, Functions::FoldNotExists>();
	add<SQLType::AGG_FOLD_IN, Functions::FoldIn>();
	add<SQLType::AGG_FOLD_NOT_IN, Functions::FoldNotIn>();
	add<SQLType::AGG_FOLD_UPTO_ONE, Functions::FoldUptoOne>();
}


template<typename C>
inline void SQLAggrExprs::Functions::Avg::Advance::operator()(
		C &cxt, const Aggr &aggr, double v) {
	aggr.addUnchecked<0>()(cxt, v);
	aggr.increment<1>()(cxt);
}

template<typename C>
inline void SQLAggrExprs::Functions::Avg::Merge::operator()(
		C &cxt, const Aggr &aggr, double v1, int64_t v2) {
	aggr.addUnchecked<0>()(cxt, v1);
	aggr.add<1>()(cxt, v2);
}

template<typename C>
inline std::pair<double, bool> SQLAggrExprs::Functions::Avg::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	const int64_t count = aggr.get<1>()(cxt);
	if (count == 0) {
		return std::pair<double, bool>();
	}

	return std::make_pair(
			aggr.get<0>()(cxt) / static_cast<double>(aggr.get<1>()(cxt)), true);
}


template<typename C>
inline void SQLAggrExprs::Functions::CountBase::Merge::operator()(
		C &cxt, const Aggr &aggr, int64_t v) {
	aggr.add<0>()(cxt, v);
}


template<typename C>
inline void SQLAggrExprs::Functions::CountAll::Advance::operator()(
		C &cxt, const Aggr &aggr) {
	aggr.increment<0>()(cxt);
}


template<typename C>
inline void SQLAggrExprs::Functions::CountColumn::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	static_cast<void>(v);
	aggr.increment<0>()(cxt);
}


SQLAggrExprs::Functions::GroupConcat::Advance::Advance() :
		forSeparatror_(false) {
}

template<typename C>
void SQLAggrExprs::Functions::GroupConcat::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	if (forSeparatror_) {
		if (!SQLValues::ValueUtils::isNull(v)) {
			aggr.set<2>()(
					cxt, SQLValues::ValueUtils::promoteValue<
							TupleTypes::TYPE_STRING>(v));
		}
		return;
	}

	const TupleString &strValue = SQLValues::ValueUtils::toString(cxt.getBase(), v);

	TupleValue sizeValue;
	{
		const uint32_t size = static_cast<uint32_t>(strValue.getBuffer().second);
		SQLValues::LobBuilder builder(
				cxt.getBase().getValueContext().getVarContext(),
				TupleTypes::TYPE_BLOB, 0);
		builder.append(&size, sizeof(size));
		sizeValue = builder.build();
	}

	SQLValues::StringReader strReader(strValue.getBuffer());
	SQLValues::LobReader lobReader(sizeValue);

	if (aggr.isNull<0>()(cxt) || aggr.isNull<1>()(cxt)) {
		aggr.set<0>()(cxt, strReader);
		aggr.set<1>()(cxt, lobReader);
	}
	else {
		aggr.add<0>()(cxt, strReader);
		aggr.add<1>()(cxt, lobReader);
	}

	forSeparatror_ = true;
	if (!aggr.isNull<2>()(cxt)) {
		cxt.finishFunction();
	}
}

template<typename C, typename R1, typename R2>
void SQLAggrExprs::Functions::GroupConcat::Merge::operator()(
		C &cxt, const Aggr &aggr, R1 &v1, R2 *v2) {
	do {
		if (v2 == NULL) {
			break;
		}

		if (aggr.isNull<0>()(cxt) || aggr.isNull<1>()(cxt)) {
			aggr.set<0>()(cxt, v1);
			aggr.set<1>()(cxt, *v2);
		}
		else {
			aggr.add<0>()(cxt, v1);
			aggr.add<1>()(cxt, *v2);
		}

		if (!aggr.isNull<2>()(cxt)) {
			break;
		}
		return;
	}
	while (false);
	cxt.finishFunction();
}

template<typename C>
void SQLAggrExprs::Functions::GroupConcat::Merge::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	if (!SQLValues::ValueUtils::isNull(v)) {
		aggr.set<2>()(cxt, v);
	}
}

template<typename C>
typename C::WriterType*
SQLAggrExprs::Functions::GroupConcat::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	if (aggr.isNull<0>()(cxt) || aggr.isNull<1>()(cxt)) {
		return NULL;
	}

	typename C::WriterType &writer = cxt.getResultWriter();

	SQLValues::StringReader strReader(aggr.get<0>()(cxt).getBuffer());
	SQLValues::LobReader sizeReader(aggr.get<1>()(cxt));
	bool first = true;
	for (;;) {
		uint32_t size;
		uint8_t sizeData[sizeof(size)];
		SQLValues::BufferBuilder<uint8_t> sizeWriter(
				sizeData, sizeData + sizeof(size));
		SQLValues::ValueUtils::subForwardPart(
				sizeWriter, sizeReader, sizeof(size));
		if (sizeWriter.getSize() != sizeof(size)) {
			break;
		}

		if (first) {
			first = false;
		}
		else if (!aggr.isNull<2>()(cxt)) {
			const TupleString::BufferInfo &sep = aggr.get<2>()(cxt).getBuffer();
			writer.append(sep.first, sep.second);
		}

		memcpy(&size, sizeData, sizeof(size));
		SQLValues::ValueUtils::subForwardPart(
				writer, strReader, size);
	}

	return &writer;
}


template<typename C>
inline void SQLAggrExprs::Functions::LagLead::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	aggr.set<0>()(cxt, v);
}

template<typename C>
inline void SQLAggrExprs::Functions::LagLead::Advance::operator()(
		C &cxt, const Aggr &aggr, const int64_t &v) {
	if (v >= 0) {
		cxt.finishFunction();
	}
}


template<typename C>
inline void SQLAggrExprs::Functions::Max::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	if (aggr.isNull<0>()(cxt) ||
			SQLValues::ValueGreater()(v, aggr.get<0>()(cxt))) {
		aggr.set<0>()(cxt, v);
	}
}


template<typename C, typename Aggr, typename V>
inline void SQLAggrExprs::Functions::Median::Advance::operator()(
		C &cxt, const Aggr &aggr, const V &v) {
	switch (nextAction(cxt, aggr)) {
	case ACTION_SET:
		aggr.template set<0>()(cxt, v);
		break;
	case ACTION_MERGE:
		{
			V merged;
			if (!FunctionUtils::NumberArithmetic::middleOfOrderedValues(
					aggr.template get<0>()(cxt), v, merged)) {
				errorUnordered(cxt);
			}
			aggr.template set<0>()(cxt, merged);
		}
		break;
	default:
		break;
	}
}

template<typename C, typename Aggr>
inline SQLAggrExprs::Functions::Median::MedianAction
SQLAggrExprs::Functions::Median::Advance::nextAction(
		C &cxt, const Aggr &aggr) {

	int64_t count = aggr.template get<1>()(cxt);
	MedianAction action = ACTION_NONE;
	do {
		if (count <= 0) {
			if (count < 0) {
				if (count < -1) {
					action = ACTION_MERGE;
					count = -1;
				}
				break;
			}
			count = cxt.getWindowValueCount();
		}

		count -= 2;
		if (count <= 0) {
			action = ACTION_SET;
			if (count == 0) {
				count -= 2;
			}
			break;
		}
	}
	while (false);

	aggr.template set<1>()(cxt, count);
	return action;
}

template<typename C>
void SQLAggrExprs::Functions::Median::Advance::errorUnordered(C &cxt) {
	static_cast<void>(cxt);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
			"Unordered MEDIAN values");
}


template<typename C>
inline void SQLAggrExprs::Functions::Min::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	if (aggr.isNull<0>()(cxt) ||
			SQLValues::ValueLess()(v, aggr.get<0>()(cxt))) {
		aggr.set<0>()(cxt, v);
	}
}


template<typename C>
inline void SQLAggrExprs::Functions::StddevBase::Advance::operator()(
		C &cxt, const Aggr &aggr, double v) {
	aggr.addUnchecked<0>()(cxt, v);
	aggr.addUnchecked<1>()(cxt, v * v);
	aggr.increment<2>()(cxt);
}

template<typename C>
inline void SQLAggrExprs::Functions::StddevBase::Merge::operator()(
		C &cxt, const Aggr &aggr, double v1, double v2, int64_t v3) {
	aggr.addUnchecked<0>()(cxt, v1);
	aggr.addUnchecked<1>()(cxt, v2);
	aggr.addUnchecked<2>()(cxt, v3);
}


template<typename C>
inline std::pair<double, bool>
SQLAggrExprs::Functions::Stddev0::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	Variance0::Finish varFunc;
	const std::pair<double, bool> &var = varFunc(cxt, aggr);
	return std::make_pair(std::sqrt(var.first), var.second);
}


template<typename C>
inline std::pair<double, bool>
SQLAggrExprs::Functions::StddevPop::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	VarPop::Finish varFunc;
	const std::pair<double, bool> &var = varFunc(cxt, aggr);
	return std::make_pair(std::sqrt(var.first), var.second);
}


template<typename C>
inline std::pair<double, bool>
SQLAggrExprs::Functions::StddevSamp::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	VarSamp::Finish varFunc;
	const std::pair<double, bool> &var = varFunc(cxt, aggr);
	return std::make_pair(std::sqrt(var.first), var.second);
}


template<typename C>
inline void SQLAggrExprs::Functions::Sum::Advance::operator()(
		C &cxt, const LongAggr &aggr, int64_t v) {
	if (aggr.isNull<0>()(cxt)) {
		aggr.set<0>()(cxt, v);
	}
	else {
		aggr.add<0>()(cxt, v);
	}
}

template<typename C>
inline void SQLAggrExprs::Functions::Sum::Advance::operator()(
		C &cxt, const DoubleAggr &aggr, double v) {
	if (aggr.isNull<0>()(cxt)) {
		aggr.set<0>()(cxt, v);
	}
	else {
		aggr.add<0>()(cxt, v);
	}
}


template<typename C>
inline void SQLAggrExprs::Functions::Total::Advance::operator()(
		C &cxt, const Aggr &aggr, double v) {
	aggr.addUnchecked<0>()(cxt, v);
}


template<typename C>
inline std::pair<double, bool>
SQLAggrExprs::Functions::VarPop::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	const int64_t count = aggr.get<2>()(cxt);
	if (count <= 0) {
		return std::pair<double, bool>();
	}

	const double n = static_cast<double>(count);
	const double avg = aggr.get<0>()(cxt) / n;
	const double squareAvg = aggr.get<1>()(cxt) / n;

	return std::make_pair((squareAvg - avg * avg), true);
}


template<typename C>
inline std::pair<double, bool>
SQLAggrExprs::Functions::VarSamp::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	const int64_t count = aggr.get<2>()(cxt);
	if (count <= 1) {
		return std::pair<double, bool>();
	}

	const double n = static_cast<double>(count);
	const double m = static_cast<double>(count - 1);
	const double avg = aggr.get<0>()(cxt) / n;
	const double squareAvg = aggr.get<1>()(cxt) / n;

	return std::make_pair((squareAvg - avg * avg) * (n / m), true);
}


template<typename C>
inline std::pair<double, bool>
SQLAggrExprs::Functions::Variance0::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	const int64_t count = aggr.get<2>()(cxt);
	if (count <= 1) {
		if (count <= 0) {
			return std::pair<double, bool>();
		}
		return std::make_pair(0.0, true);
	}

	const double n = static_cast<double>(count);
	const double m = static_cast<double>(count - 1);
	const double avg = aggr.get<0>()(cxt) / n;
	const double squareAvg = aggr.get<1>()(cxt) / n;

	return std::make_pair((squareAvg - avg * avg) * (n / m), true);
}


template<typename C>
inline void SQLAggrExprs::Functions::First::Advance::operator()(
		C &cxt, const Aggr &aggr) {
	if (!aggr.isNull<0>()(cxt)) {
		cxt.finishFunction();
	}
}

template<typename C>
inline void SQLAggrExprs::Functions::First::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	aggr.set<0>()(cxt, v);
}

template<typename C>
inline TupleValue SQLAggrExprs::Functions::First::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	return aggr.get<0>()(cxt);
}

template<typename C>
inline void SQLAggrExprs::Functions::First::Finish::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	if (!aggr.isNull<0>()(cxt)) {
		cxt.finishFunction();
		return;
	}
	aggr.set<0>()(cxt, v);
}


template<typename C>
inline void SQLAggrExprs::Functions::Last::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	aggr.set<0>()(cxt, v);
}


template<typename C>
inline void SQLAggrExprs::Functions::FoldExists::Advance::operator()(
		C &cxt, const Aggr &aggr, int64_t v) {
	static_cast<void>(v);
	aggr.set<0>()(cxt, 1);
}

template<typename C>
inline void SQLAggrExprs::Functions::FoldExists::Merge::operator()(
		C &cxt, const Aggr &aggr, int64_t v) {
	if (v != 0) {
		aggr.set<0>()(cxt, 1);
	}
}

template<typename C>
inline bool SQLAggrExprs::Functions::FoldExists::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	return (aggr.get<0>()(cxt) != 0);
}


template<typename C>
bool SQLAggrExprs::Functions::FoldNotExists::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	FoldExists::Finish baseFunc;
	return !baseFunc(cxt, aggr);
}


template<typename C>
inline void SQLAggrExprs::Functions::FoldIn::Advance::operator()(
		C &cxt, const Aggr &aggr, int64_t v) {
	static_cast<void>(v);
	if ((aggr.get<0>()(cxt) & FOLD_IN_MATCHED) != 0) {
		cxt.finishFunction();
	}
}

template<typename C>
inline void SQLAggrExprs::Functions::FoldIn::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	int64_t flags = aggr.get<0>()(cxt);
	do {
		if (!leftValue_.second) {
			if (SQLValues::ValueUtils::isNull(v)) {
				flags |= FOLD_IN_NULL_FOUND;
				cxt.finishFunction();
				break;
			}
			else {
				leftValue_ = std::make_pair(v, true);
				return;
			}
		}
		else if (SQLValues::ValueUtils::isNull(v)) {
			flags |= FOLD_IN_NULL_FOUND;
			break;
		}

		if (SQLValues::ValueEq()(leftValue_.first, v)) {
			flags |= FOLD_IN_MATCHED;
		}
	}
	while (false);

	flags |= FOLD_IN_FOUND;
	aggr.set<0>()(cxt, flags);
}

template<typename C>
inline void SQLAggrExprs::Functions::FoldIn::Merge::operator()(
		C &cxt, const Aggr &aggr, int64_t v) {
	aggr.set<0>()(cxt, aggr.get<0>()(cxt) | v);
}

template<typename C>
inline std::pair<bool, bool>
SQLAggrExprs::Functions::FoldIn::Finish::operator()(C &cxt, const Aggr &aggr) {
	const int64_t flags = aggr.get<0>()(cxt);
	if ((flags & FOLD_IN_MATCHED) != 0) {
		return std::make_pair(true, true);
	}
	else if ((flags & FOLD_IN_NULL_FOUND) == 0 ||
			(flags & FOLD_IN_FOUND) == 0) {
		return std::make_pair(false, true);
	}
	else {
		return std::make_pair(false, false);
	}
}


template<typename C>
inline std::pair<bool, bool>
SQLAggrExprs::Functions::FoldNotIn::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	FoldIn::Finish baseFunc;
	const std::pair<bool, bool> &base = baseFunc(cxt, aggr);
	return std::make_pair(!base.first, base.second);
}


template<typename C>
inline void SQLAggrExprs::Functions::FoldUptoOne::Advance::operator()(
		C &cxt, const Aggr &aggr, int64_t v) {
	static_cast<void>(v);
	if (aggr.get<0>()(cxt) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_MULTIPLE_TUPLE,
				"More than one tuples selected in a subquery");
	}
	aggr.set<0>()(cxt, 1);
}

template<typename C>
inline void SQLAggrExprs::Functions::FoldUptoOne::Advance::operator()(
		C &cxt, const Aggr &aggr, const TupleValue &v) {
	aggr.set<1>()(cxt, v);
}

template<typename C>
inline void SQLAggrExprs::Functions::FoldUptoOne::Merge::operator()(
		C &cxt, const Aggr &aggr, int64_t v1, const TupleValue &v2) {
	if (v1 == 0) {
		return;
	}
	Advance baseFunc;
	baseFunc(cxt, aggr, v1);
	baseFunc(cxt, aggr, v2);
}

template<typename C>
inline TupleValue SQLAggrExprs::Functions::FoldUptoOne::Finish::operator()(
		C &cxt, const Aggr &aggr) {
	return aggr.get<1>()(cxt);
}
