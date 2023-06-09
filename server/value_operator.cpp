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
	@brief Implementation of ValueOperator
*/
#include "value_operator.h"

struct ComparatorTable::Timestamps {
	template<typename Op, typename T1, typename T2>
	static bool op(
			TransactionContext &txn, const uint8_t *p,
			uint32_t size1, const uint8_t *q, uint32_t size2) {
		return Op()(comp<T1, T2>(txn, p, size1, q, size2));
	}

	template<typename T1, typename T2>
	static int32_t comp(
			TransactionContext &txn, const uint8_t *p,
			uint32_t size1, const uint8_t *q, uint32_t size2) {
		typedef typename T1::template With<T2>::Type CompType;
		static_cast<void>(txn);
		static_cast<void>(size1);
		static_cast<void>(size2);
		return ValueProcessor::compareTimestamp(
				T1()(p, CompType()), T2()(q, CompType()));
	}
};

struct ComparatorTable::Milli {
	template<typename T> struct With {
		typedef
				typename util::Conditional<util::IsSame<Nano, T>::VALUE, Nano,
				typename util::Conditional<util::IsSame<Micro, T>::VALUE, Micro,
						Milli>::Type>::Type Type;
	};
	Timestamp operator()(const void *p, const Milli&) const {
		return *static_cast<const Timestamp*>(p);
	}
	MicroTimestamp operator()(const void *p, const Micro&) const {
		return ValueProcessor::getMicroTimestamp((*this)(p, Milli()));
	}
	NanoTimestamp operator()(const void *p, const Nano&) const {
		return ValueProcessor::getNanoTimestamp((*this)(p, Milli()));
	}
};

struct ComparatorTable::Micro {
	template<typename T> struct With {
		typedef typename util::Conditional<
				util::IsSame<Nano, T>::VALUE, Nano, Micro>::Type Type;
	};
	MicroTimestamp operator()(const void *p, const Micro&) const {
		return *static_cast<const MicroTimestamp*>(p);
	}
	NanoTimestamp operator()(const void *p, const Nano&) const {
		return ValueProcessor::getNanoTimestamp((*this)(p, Micro()));
	}
};

struct ComparatorTable::Nano {
	template<typename> struct With {
		typedef Nano Type;
	};
	NanoTimestamp operator()(const void *p, const Nano&) const {
		return *static_cast<const NanoTimestamp*>(p);
	}
};

struct ComparatorTable::NeOp {
	bool operator()(int32_t comp) const {
		return comp != 0;
	}
};

struct ComparatorTable::EqOp {
	bool operator()(int32_t comp) const {
		return comp == 0;
	}
};

struct ComparatorTable::LtOp {
	bool operator()(int32_t comp) const {
		return comp < 0;
	}
};

struct ComparatorTable::LeOp {
	bool operator()(int32_t comp) const {
		return comp <= 0;
	}
};

struct ComparatorTable::GtOp {
	bool operator()(int32_t comp) const {
		return comp > 0;
	}
};

struct ComparatorTable::GeOp {
	bool operator()(int32_t comp) const {
		return comp >= 0;
	}
};

const ValueComparatorTable ComparatorTable::comparatorTable_ = {
	{&compareStringString, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		NULL}
	,
	{NULL, &compareBoolBool, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		NULL}
	,
	{NULL, NULL, &compareByteByte, &compareByteShort, &compareByteInt,
		&compareByteLong, &compareByteFloat, &compareByteDouble,
		NULL, NULL, NULL}
	,
	{NULL, NULL, &compareShortByte, &compareShortShort, &compareShortInt,
		&compareShortLong, &compareShortFloat, &compareShortDouble,
		NULL, NULL, NULL}
	,
	{NULL, NULL, &compareIntByte, &compareIntShort, &compareIntInt,
		&compareIntLong, &compareIntFloat, &compareIntDouble,
		NULL, NULL, NULL}
	,
	{NULL, NULL, &compareLongByte, &compareLongShort, &compareLongInt,
		&compareLongLong, &compareLongFloat, &compareLongDouble,
		NULL, NULL, NULL}
	,
	{NULL, NULL, &compareFloatByte, &compareFloatShort, &compareFloatInt,
		&compareFloatLong, &compareFloatFloat, &compareFloatDouble,
		NULL, NULL, NULL}
	,
	{NULL, NULL, &compareDoubleByte, &compareDoubleShort, &compareDoubleInt,
		&compareDoubleLong, &compareDoubleFloat, &compareDoubleDouble, NULL,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&compareTimestampTimestamp, NULL, NULL,
		&Timestamps::comp<Milli, Micro>,
		&Timestamps::comp<Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::comp<Micro, Milli>, NULL, NULL,
		&Timestamps::comp<Micro, Micro>,
		&Timestamps::comp<Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::comp<Nano, Milli>, NULL, NULL,
		&Timestamps::comp<Nano, Micro>,
		&Timestamps::comp<Nano, Nano>}};

const ValueOperatorTable ComparatorTable::eqTable_ = {
	{&eqStringString, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		NULL}
	,
	{NULL, &eqBoolBool, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, &eqByteByte, &eqByteShort, &eqByteInt, &eqByteLong,
		&eqByteFloat, &eqByteDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &eqShortByte, &eqShortShort, &eqShortInt, &eqShortLong,
		&eqShortFloat, &eqShortDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &eqIntByte, &eqIntShort, &eqIntInt, &eqIntLong, &eqIntFloat,
		&eqIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &eqLongByte, &eqLongShort, &eqLongInt, &eqLongLong,
		&eqLongFloat, &eqLongDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &eqFloatByte, &eqFloatShort, &eqFloatInt, &eqFloatLong,
		&eqFloatFloat, &eqFloatDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &eqDoubleByte, &eqDoubleShort, &eqDoubleInt, &eqDoubleLong,
		&eqDoubleFloat, &eqDoubleDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&eqTimestampTimestamp, NULL, NULL,
		&Timestamps::op<EqOp, Milli, Micro>,
		&Timestamps::op<EqOp, Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<EqOp, Micro, Milli>, NULL, NULL,
		&Timestamps::op<EqOp, Micro, Micro>,
		&Timestamps::op<EqOp, Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<EqOp, Nano, Milli>, NULL, NULL,
		&Timestamps::op<EqOp, Nano, Micro>,
		&Timestamps::op<EqOp, Nano, Nano>}};
const ValueOperatorTable ComparatorTable::neTable_ = {
	{&neStringString, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		NULL}
	,
	{NULL, &neBoolBool, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, &neByteByte, &neByteShort, &neByteInt, &neByteLong,
		&neByteFloat, &neByteDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &neShortByte, &neShortShort, &neShortInt, &neShortLong,
		&neShortFloat, &neShortDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &neIntByte, &neIntShort, &neIntInt, &neIntLong, &neIntFloat,
		&neIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &neLongByte, &neLongShort, &neLongInt, &neLongLong,
		&neLongFloat, &neLongDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &neFloatByte, &neFloatShort, &neFloatInt, &neFloatLong,
		&neFloatFloat, &neFloatDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &neDoubleByte, &neDoubleShort, &neDoubleInt, &neDoubleLong,
		&neDoubleFloat, &neDoubleDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&neTimestampTimestamp, NULL, NULL,
		&Timestamps::op<NeOp, Milli, Micro>,
		&Timestamps::op<NeOp, Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<NeOp, Micro, Milli>, NULL, NULL,
		&Timestamps::op<NeOp, Micro, Micro>,
		&Timestamps::op<NeOp, Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<NeOp, Nano, Milli>, NULL, NULL,
		&Timestamps::op<NeOp, Nano, Micro>,
		&Timestamps::op<NeOp, Nano, Nano>}};
const ValueOperatorTable ComparatorTable::ltTable_ = {
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, &ltByteByte, &ltByteShort, &ltByteInt, &ltByteLong,
		&ltByteFloat, &ltByteDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &ltShortByte, &ltShortShort, &ltShortInt, &ltShortLong,
		&ltShortFloat, &ltShortDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &ltIntByte, &ltIntShort, &ltIntInt, &ltIntLong, &ltIntFloat,
		&ltIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &ltLongByte, &ltLongShort, &ltLongInt, &ltLongLong,
		&ltLongFloat, &ltLongDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &ltFloatByte, &ltFloatShort, &ltFloatInt, &ltFloatLong,
		&ltFloatFloat, &ltFloatDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &ltDoubleByte, &ltDoubleShort, &ltDoubleInt, &ltDoubleLong,
		&ltDoubleFloat, &ltDoubleDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&ltTimestampTimestamp, NULL, NULL,
		&Timestamps::op<LtOp, Milli, Micro>,
		&Timestamps::op<LtOp, Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<LtOp, Micro, Milli>, NULL, NULL,
		&Timestamps::op<LtOp, Micro, Micro>,
		&Timestamps::op<LtOp, Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<LtOp, Nano, Milli>, NULL, NULL,
		&Timestamps::op<LtOp, Nano, Micro>,
		&Timestamps::op<LtOp, Nano, Nano>}};
const ValueOperatorTable ComparatorTable::gtTable_ = {
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, &gtByteByte, &gtByteShort, &gtByteInt, &gtByteLong,
		&gtByteFloat, &gtByteDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &gtShortByte, &gtShortShort, &gtShortInt, &gtShortLong,
		&gtShortFloat, &gtShortDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &gtIntByte, &gtIntShort, &gtIntInt, &gtIntLong, &gtIntFloat,
		&gtIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &gtLongByte, &gtLongShort, &gtLongInt, &gtLongLong,
		&gtLongFloat, &gtLongDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &gtFloatByte, &gtFloatShort, &gtFloatInt, &gtFloatLong,
		&gtFloatFloat, &gtFloatDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &gtDoubleByte, &gtDoubleShort, &gtDoubleInt, &gtDoubleLong,
		&gtDoubleFloat, &gtDoubleDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&gtTimestampTimestamp, NULL, NULL,
		&Timestamps::op<GtOp, Milli, Micro>,
		&Timestamps::op<GtOp, Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<GtOp, Micro, Milli>, NULL, NULL,
		&Timestamps::op<GtOp, Micro, Micro>,
		&Timestamps::op<GtOp, Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<GtOp, Nano, Milli>, NULL, NULL,
		&Timestamps::op<GtOp, Nano, Micro>,
		&Timestamps::op<GtOp, Nano, Nano>}};
const ValueOperatorTable ComparatorTable::leTable_ = {
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, &leByteByte, &leByteShort, &leByteInt, &leByteLong,
		&leByteFloat, &leByteDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &leShortByte, &leShortShort, &leShortInt, &leShortLong,
		&leShortFloat, &leShortDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &leIntByte, &leIntShort, &leIntInt, &leIntLong, &leIntFloat,
		&leIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &leLongByte, &leLongShort, &leLongInt, &leLongLong,
		&leLongFloat, &leLongDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &leFloatByte, &leFloatShort, &leFloatInt, &leFloatLong,
		&leFloatFloat, &leFloatDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &leDoubleByte, &leDoubleShort, &leDoubleInt, &leDoubleLong,
		&leDoubleFloat, &leDoubleDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&leTimestampTimestamp, NULL, NULL,
		&Timestamps::op<LeOp, Milli, Micro>,
		&Timestamps::op<LeOp, Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<LeOp, Micro, Milli>, NULL, NULL,
		&Timestamps::op<LeOp, Micro, Micro>,
		&Timestamps::op<LeOp, Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<LeOp, Nano, Milli>, NULL, NULL,
		&Timestamps::op<LeOp, Nano, Micro>,
		&Timestamps::op<LeOp, Nano, Nano>}};
const ValueOperatorTable ComparatorTable::geTable_ = {
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, &geByteByte, &geByteShort, &geByteInt, &geByteLong,
		&geByteFloat, &geByteDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &geShortByte, &geShortShort, &geShortInt, &geShortLong,
		&geShortFloat, &geShortDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &geIntByte, &geIntShort, &geIntInt, &geIntLong, &geIntFloat,
		&geIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &geLongByte, &geLongShort, &geLongInt, &geLongLong,
		&geLongFloat, &geLongDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &geFloatByte, &geFloatShort, &geFloatInt, &geFloatLong,
		&geFloatFloat, &geFloatDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &geDoubleByte, &geDoubleShort, &geDoubleInt, &geDoubleLong,
		&geDoubleFloat, &geDoubleDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&geTimestampTimestamp, NULL, NULL,
		&Timestamps::op<GeOp, Milli, Micro>,
		&Timestamps::op<GeOp, Milli, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<GeOp, Micro, Milli>, NULL, NULL,
		&Timestamps::op<GeOp, Micro, Micro>,
		&Timestamps::op<GeOp, Micro, Nano>}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		&Timestamps::op<GeOp, Nano, Milli>, NULL, NULL,
		&Timestamps::op<GeOp, Nano, Micro>,
		&Timestamps::op<GeOp, Nano, Nano>}};

const Operator ComparatorTable::isNull_ = &isNullAnyType;
const Operator ComparatorTable::isNotNull_ = &isNotNullAnyType;

bool geomOperation(TransactionContext& txn, uint8_t const* p,
				   uint32_t size1, uint8_t const* q, uint32_t size2);

const Operator ComparatorTable::geomOp_ = &geomOperation;

Operator ComparatorTable::getOperator(
		DSExpression::Operation opType, ColumnType type1, ColumnType type2) {
	Operator op = findOperator(opType, type1, type2);
	assert(op != NULL);
	return op;
}

Operator ComparatorTable::findOperator(
		DSExpression::Operation opType, ColumnType type1, ColumnType type2) {
	const ValueOperatorTable *table;
	switch (opType) {
	case DSExpression::NE:
		table = &neTable_;
		break;
	case DSExpression::EQ:
		table = &eqTable_;
		break;
	case DSExpression::LT:
		table = &ltTable_;
		break;
	case DSExpression::LE:
		table = &leTable_;
		break;
	case DSExpression::GT:
		table = &gtTable_;
		break;
	case DSExpression::GE:
		table = &geTable_;
		break;
	case DSExpression::IS:
		return isNull_;
	case DSExpression::ISNOT:
		return isNotNull_;
	case DSExpression::GEOM_OP:
		return geomOp_;
	default:
		return NULL;
	}
	return findOperator(*table, type1, type2);
}

const ValueCalculatorTable CalculatorTable::addTable_ = {
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &addByteByte, &addByteShort, &addByteInt, &addByteLong,
		&addByteFloat, &addByteDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &addShortByte, &addShortShort, &addShortInt, &addShortLong,
		&addShortFloat, &addShortDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &addIntByte, &addIntShort, &addIntInt, &addIntLong,
		&addIntFloat, &addIntDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &addLongByte, &addLongShort, &addLongInt, &addLongLong,
		&addLongFloat, &addLongDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &addFloatByte, &addFloatShort, &addFloatInt, &addFloatLong,
		&addFloatFloat, &addFloatDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &addDoubleByte, &addDoubleShort, &addDoubleInt,
		&addDoubleLong, &addDoubleFloat, &addDoubleDouble, NULL,
		NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{ NULL },
	{ NULL }
};

const ValueCalculatorTable CalculatorTable::subTable_ = {
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &subByteByte, &subByteShort, &subByteInt, &subByteLong,
		&subByteFloat, &subByteDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &subShortByte, &subShortShort, &subShortInt, &subShortLong,
		&subShortFloat, &subShortDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &subIntByte, &subIntShort, &subIntInt, &subIntLong,
		&subIntFloat, &subIntDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &subLongByte, &subLongShort, &subLongInt, &subLongLong,
		&subLongFloat, &subLongDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &subFloatByte, &subFloatShort, &subFloatInt, &subFloatLong,
		&subFloatFloat, &subFloatDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &subDoubleByte, &subDoubleShort, &subDoubleInt,
		&subDoubleLong, &subDoubleFloat, &subDoubleDouble, NULL,
		NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{ NULL },
	{ NULL }
};

const ValueCalculatorTable CalculatorTable::mulTable_ = {
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &mulByteByte, &mulByteShort, &mulByteInt, &mulByteLong,
		&mulByteFloat, &mulByteDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &mulShortByte, &mulShortShort, &mulShortInt, &mulShortLong,
		&mulShortFloat, &mulShortDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &mulIntByte, &mulIntShort, &mulIntInt, &mulIntLong,
		&mulIntFloat, &mulIntDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &mulLongByte, &mulLongShort, &mulLongInt, &mulLongLong,
		&mulLongFloat, &mulLongDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &mulFloatByte, &mulFloatShort, &mulFloatInt, &mulFloatLong,
		&mulFloatFloat, &mulFloatDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &mulDoubleByte, &mulDoubleShort, &mulDoubleInt,
		&mulDoubleLong, &mulDoubleFloat, &mulDoubleDouble, NULL,
		NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{ NULL },
	{ NULL }
};

const ValueCalculatorTable CalculatorTable::divTable_ = {
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &divByteByte, &divByteShort, &divByteInt, &divByteLong,
		&divByteFloat, &divByteDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &divShortByte, &divShortShort, &divShortInt, &divShortLong,
		&divShortFloat, &divShortDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &divIntByte, &divIntShort, &divIntInt, &divIntLong,
		&divIntFloat, &divIntDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &divLongByte, &divLongShort, &divLongInt, &divLongLong,
		&divLongFloat, &divLongDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &divFloatByte, &divFloatShort, &divFloatInt, &divFloatLong,
		&divFloatFloat, &divFloatDouble, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &divDoubleByte, &divDoubleShort, &divDoubleInt,
		&divDoubleLong, &divDoubleFloat, &divDoubleDouble, NULL,
		NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{ NULL },
	{ NULL }
};

const ValueCalculatorTable CalculatorTable::modTable_ = {
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &modByteByte, &modByteShort, &modByteInt, &modByteLong,
		NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &modShortByte, &modShortShort, &modShortInt, &modShortLong,
		NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &modIntByte, &modIntShort, &modIntInt, &modIntLong, NULL,
		NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, &modLongByte, &modLongShort, &modLongInt, &modLongLong,
		NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	},
	{ NULL },
	{ NULL }
};
