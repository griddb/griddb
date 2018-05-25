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
	@brief Definition and Implementation of compare and arithmetic functions
*/
#ifndef QP_OPERATOR_H_
#define QP_OPERATOR_H_

#include "value.h"
#include "util/numeric.h"


static inline int32_t compareByteByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int8_t*>(p))) <
				   (*(reinterpret_cast<const int8_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int8_t*>(p))) ==
						 (*(reinterpret_cast<const int8_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareByteShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int8_t*>(p))) <
				   (*(reinterpret_cast<const int16_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int8_t*>(p))) ==
						 (*(reinterpret_cast<const int16_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareByteInt(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int8_t*>(p))) <
				   (*(reinterpret_cast<const int32_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int8_t*>(p))) ==
						 (*(reinterpret_cast<const int32_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareByteLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int8_t*>(p))) <
				   (*(reinterpret_cast<const int64_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int8_t*>(p))) ==
						 (*(reinterpret_cast<const int64_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareByteFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(q)))) return -1;
	return (*(reinterpret_cast<const int8_t*>(p))) <
				   (*(reinterpret_cast<const float*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int8_t*>(p))) ==
						 (*(reinterpret_cast<const float*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareByteDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(q)))) return -1;
	return (*(reinterpret_cast<const int8_t*>(p))) <
				   (*(reinterpret_cast<const double*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int8_t*>(p))) ==
						 (*(reinterpret_cast<const double*>(q)))
					 ? 0
					 : 1;
}

static inline int32_t compareShortByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int16_t*>(p))) <
				   (*(reinterpret_cast<const int8_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int16_t*>(p))) ==
						 (*(reinterpret_cast<const int8_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareShortShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int16_t*>(p))) <
				   (*(reinterpret_cast<const int16_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int16_t*>(p))) ==
						 (*(reinterpret_cast<const int16_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareShortInt(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int16_t*>(p))) <
				   (*(reinterpret_cast<const int32_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int16_t*>(p))) ==
						 (*(reinterpret_cast<const int32_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareShortLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int16_t*>(p))) <
				   (*(reinterpret_cast<const int64_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int16_t*>(p))) ==
						 (*(reinterpret_cast<const int64_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareShortFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(q)))) return -1;
	return (*(reinterpret_cast<const int16_t*>(p))) <
				   (*(reinterpret_cast<const float*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int16_t*>(p))) ==
						 (*(reinterpret_cast<const float*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareShortDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(q)))) return -1;
	return (*(reinterpret_cast<const int16_t*>(p))) <
				   (*(reinterpret_cast<const double*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int16_t*>(p))) ==
						 (*(reinterpret_cast<const double*>(q)))
					 ? 0
					 : 1;
}

static inline int32_t compareIntByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int32_t*>(p))) <
				   (*(reinterpret_cast<const int8_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int32_t*>(p))) ==
						 (*(reinterpret_cast<const int8_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareIntShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int32_t*>(p))) <
				   (*(reinterpret_cast<const int16_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int32_t*>(p))) ==
						 (*(reinterpret_cast<const int16_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareIntInt(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int32_t*>(p))) <
				   (*(reinterpret_cast<const int32_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int32_t*>(p))) ==
						 (*(reinterpret_cast<const int32_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareIntLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int32_t*>(p))) <
				   (*(reinterpret_cast<const int64_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int32_t*>(p))) ==
						 (*(reinterpret_cast<const int64_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareIntFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(q)))) return -1;
	return (*(reinterpret_cast<const int32_t*>(p))) <
				   (*(reinterpret_cast<const float*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int32_t*>(p))) ==
						 (*(reinterpret_cast<const float*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareIntDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(q)))) return -1;
	return (*(reinterpret_cast<const int32_t*>(p))) <
				   (*(reinterpret_cast<const double*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int32_t*>(p))) ==
						 (*(reinterpret_cast<const double*>(q)))
					 ? 0
					 : 1;
}

static inline int32_t compareLongByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int64_t*>(p))) <
				   (*(reinterpret_cast<const int8_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int64_t*>(p))) ==
						 (*(reinterpret_cast<const int8_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareLongShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int64_t*>(p))) <
				   (*(reinterpret_cast<const int16_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int64_t*>(p))) ==
						 (*(reinterpret_cast<const int16_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareLongInt(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int64_t*>(p))) <
				   (*(reinterpret_cast<const int32_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int64_t*>(p))) ==
						 (*(reinterpret_cast<const int32_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareLongLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const int64_t*>(p))) <
				   (*(reinterpret_cast<const int64_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int64_t*>(p))) ==
						 (*(reinterpret_cast<const int64_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareLongFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(q)))) return -1;
	return (*(reinterpret_cast<const int64_t*>(p))) <
				   (*(reinterpret_cast<const float*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int64_t*>(p))) ==
						 (*(reinterpret_cast<const float*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareLongDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(q)))) return -1;
	return (*(reinterpret_cast<const int64_t*>(p))) <
				   (*(reinterpret_cast<const double*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const int64_t*>(p))) ==
						 (*(reinterpret_cast<const double*>(q)))
					 ? 0
					 : 1;
}

static inline int32_t compareFloatByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(p)))) return 1;
	return (*(reinterpret_cast<const float*>(p))) <
				   (*(reinterpret_cast<const int8_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const float*>(p))) ==
						 (*(reinterpret_cast<const int8_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareFloatShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(p)))) return 1;
	return (*(reinterpret_cast<const float*>(p))) <
				   (*(reinterpret_cast<const int16_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const float*>(p))) ==
						 (*(reinterpret_cast<const int16_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareFloatInt(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(p)))) return 1;
	return (*(reinterpret_cast<const float*>(p))) <
				   (*(reinterpret_cast<const int32_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const float*>(p))) ==
						 (*(reinterpret_cast<const int32_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareFloatLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const float*>(p)))) return 1;
	return (*(reinterpret_cast<const float*>(p))) <
				   (*(reinterpret_cast<const int64_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const float*>(p))) ==
						 (*(reinterpret_cast<const int64_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareFloatFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	float x = *(reinterpret_cast<const float*>(p));
	float y = *(reinterpret_cast<const float*>(q));
	if (util::isNaN(x)) {
		if (util::isNaN(y))
			return 0;
		else
			return 1;
	}
	else if (util::isNaN(y)) {
		return -1;
	}
	return ((x < y) ? -1 : ((x == y) ? 0 : 1));
}
static inline int32_t compareFloatDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	float x = *(reinterpret_cast<const float*>(p));
	double y = *(reinterpret_cast<const double*>(q));
	if (util::isNaN(x)) {
		if (util::isNaN(y))
			return 0;
		else
			return 1;
	}
	else if (util::isNaN(y)) {
		return -1;
	}
	return ((x < y) ? -1 : ((x == y) ? 0 : 1));
}

static inline int32_t compareDoubleByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(p)))) return 1;
	return (*(reinterpret_cast<const double*>(p))) <
				   (*(reinterpret_cast<const int8_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const double*>(p))) ==
						 (*(reinterpret_cast<const int8_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareDoubleShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(p)))) return 1;
	return (*(reinterpret_cast<const double*>(p))) <
				   (*(reinterpret_cast<const int16_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const double*>(p))) ==
						 (*(reinterpret_cast<const int16_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareDoubleInt(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(p)))) return 1;
	return (*(reinterpret_cast<const double*>(p))) <
				   (*(reinterpret_cast<const int32_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const double*>(p))) ==
						 (*(reinterpret_cast<const int32_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareDoubleLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	if (util::isNaN(*(reinterpret_cast<const double*>(p)))) return 1;
	return (*(reinterpret_cast<const double*>(p))) <
				   (*(reinterpret_cast<const int64_t*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const double*>(p))) ==
						 (*(reinterpret_cast<const int64_t*>(q)))
					 ? 0
					 : 1;
}
static inline int32_t compareDoubleFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	double x = *(reinterpret_cast<const double*>(p));
	float y = *(reinterpret_cast<const float*>(q));
	if (util::isNaN(x)) {
		if (util::isNaN(y))
			return 0;
		else
			return 1;
	}
	else if (util::isNaN(y)) {
		return -1;
	}
	return ((x < y) ? -1 : ((x == y) ? 0 : 1));
}
static inline int32_t compareDoubleDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	double x = *(reinterpret_cast<const double*>(p));
	double y = *(reinterpret_cast<const double*>(q));
	if (util::isNaN(x)) {
		if (util::isNaN(y))
			return 0;
		else
			return 1;
	}
	else if (util::isNaN(y)) {
		return -1;
	}
	return ((x < y) ? -1 : ((x == y) ? 0 : 1));
}

static inline int32_t compareTimestampTimestamp(TransactionContext&,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const Timestamp*>(p))) <
				   (*(reinterpret_cast<const Timestamp*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const Timestamp*>(p))) ==
						 (*(reinterpret_cast<const Timestamp*>(q)))
					 ? 0
					 : 1;
}

static inline bool eqByteByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteByte(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqByteShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteShort(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqByteInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteInt(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqByteLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteLong(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqByteFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteFloat(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqByteDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteDouble(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neByteByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteByte(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neByteShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteShort(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neByteInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteInt(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neByteLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteLong(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neByteFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteFloat(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neByteDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteDouble(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltByteByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteByte(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltByteShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteShort(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltByteInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteInt(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltByteLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteLong(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltByteFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteFloat(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltByteDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteDouble(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtByteByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteByte(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtByteShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteShort(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtByteInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteInt(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtByteLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteLong(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtByteFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteFloat(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtByteDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteDouble(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leByteByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteByte(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leByteShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteShort(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leByteInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteInt(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leByteLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteLong(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leByteFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteFloat(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leByteDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteDouble(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geByteByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteByte(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geByteShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteShort(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geByteInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteInt(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geByteLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteLong(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geByteFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteFloat(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geByteDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareByteDouble(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline bool eqShortByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortByte(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqShortShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortShort(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqShortInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortInt(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqShortLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortLong(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqShortFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortFloat(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqShortDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortDouble(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neShortByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortByte(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neShortShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortShort(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neShortInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortInt(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neShortLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortLong(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neShortFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortFloat(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neShortDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortDouble(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltShortByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortByte(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltShortShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortShort(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltShortInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortInt(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltShortLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortLong(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltShortFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortFloat(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltShortDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortDouble(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtShortByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortByte(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtShortShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortShort(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtShortInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortInt(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtShortLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortLong(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtShortFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortFloat(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtShortDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortDouble(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leShortByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortByte(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leShortShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortShort(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leShortInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortInt(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leShortLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortLong(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leShortFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortFloat(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leShortDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortDouble(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geShortByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortByte(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geShortShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortShort(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geShortInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortInt(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geShortLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortLong(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geShortFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortFloat(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geShortDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareShortDouble(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline bool eqIntByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntByte(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqIntShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntShort(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqIntInt(TransactionContext& txn, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (compareIntInt(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqIntLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntLong(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqIntFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntFloat(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqIntDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntDouble(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neIntByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntByte(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neIntShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntShort(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neIntInt(TransactionContext& txn, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (compareIntInt(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neIntLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntLong(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neIntFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntFloat(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neIntDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntDouble(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltIntByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntByte(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltIntShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntShort(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltIntInt(TransactionContext& txn, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (compareIntInt(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltIntLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntLong(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltIntFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntFloat(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltIntDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntDouble(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtIntByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntByte(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtIntShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntShort(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtIntInt(TransactionContext& txn, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (compareIntInt(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtIntLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntLong(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtIntFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntFloat(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtIntDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntDouble(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leIntByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntByte(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leIntShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntShort(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leIntInt(TransactionContext& txn, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (compareIntInt(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leIntLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntLong(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leIntFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntFloat(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leIntDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntDouble(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geIntByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntByte(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geIntShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntShort(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geIntInt(TransactionContext& txn, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (compareIntInt(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geIntLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntLong(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geIntFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntFloat(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geIntDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareIntDouble(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline bool eqLongByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongByte(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqLongShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongShort(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqLongInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongInt(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqLongLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongLong(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqLongFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongFloat(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqLongDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongDouble(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neLongByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongByte(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neLongShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongShort(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neLongInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongInt(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neLongLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongLong(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neLongFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongFloat(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neLongDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongDouble(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltLongByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongByte(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltLongShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongShort(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltLongInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongInt(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltLongLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongLong(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltLongFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongFloat(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltLongDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongDouble(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtLongByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongByte(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtLongShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongShort(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtLongInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongInt(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtLongLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongLong(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtLongFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongFloat(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtLongDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongDouble(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leLongByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongByte(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leLongShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongShort(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leLongInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongInt(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leLongLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongLong(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leLongFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongFloat(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leLongDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongDouble(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geLongByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongByte(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geLongShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongShort(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geLongInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongInt(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geLongLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongLong(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geLongFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongFloat(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geLongDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareLongDouble(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline bool eqFloatByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatByte(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqFloatShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatShort(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqFloatInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatInt(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqFloatLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatLong(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqFloatFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatFloat(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqFloatDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatDouble(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neFloatByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatByte(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neFloatShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatShort(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neFloatInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatInt(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neFloatLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatLong(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neFloatFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatFloat(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neFloatDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatDouble(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltFloatByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatByte(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltFloatShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatShort(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltFloatInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatInt(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltFloatLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatLong(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltFloatFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatFloat(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltFloatDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatDouble(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtFloatByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatByte(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtFloatShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatShort(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtFloatInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatInt(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtFloatLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatLong(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtFloatFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatFloat(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtFloatDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatDouble(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leFloatByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatByte(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leFloatShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatShort(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leFloatInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatInt(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leFloatLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatLong(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leFloatFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatFloat(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leFloatDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatDouble(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geFloatByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatByte(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geFloatShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatShort(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geFloatInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatInt(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geFloatLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatLong(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geFloatFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatFloat(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geFloatDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareFloatDouble(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline bool eqDoubleByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleByte(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqDoubleShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleShort(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqDoubleInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleInt(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqDoubleLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleLong(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqDoubleFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleFloat(txn, p, 0, q, 0) == 0) ? true : false;
}
static inline bool eqDoubleDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleDouble(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neDoubleByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleByte(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neDoubleShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleShort(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neDoubleInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleInt(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neDoubleLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleLong(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neDoubleFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleFloat(txn, p, 0, q, 0) != 0) ? true : false;
}
static inline bool neDoubleDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleDouble(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltDoubleByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleByte(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltDoubleShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleShort(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltDoubleInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleInt(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltDoubleLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleLong(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltDoubleFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleFloat(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool ltDoubleDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleDouble(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtDoubleByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleByte(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtDoubleShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleShort(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtDoubleInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleInt(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtDoubleLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleLong(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtDoubleFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleFloat(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool gtDoubleDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleDouble(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leDoubleByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleByte(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leDoubleShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleShort(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leDoubleInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleInt(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leDoubleLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleLong(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leDoubleFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleFloat(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool leDoubleDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleDouble(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geDoubleByte(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleByte(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geDoubleShort(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleShort(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geDoubleInt(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleInt(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geDoubleLong(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleLong(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geDoubleFloat(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleFloat(txn, p, 0, q, 0) >= 0) ? true : false;
}
static inline bool geDoubleDouble(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareDoubleDouble(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline bool eqTimestampTimestamp(TransactionContext& txn,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (compareTimestampTimestamp(txn, p, 0, q, 0) == 0) ? true : false;
}

static inline bool neTimestampTimestamp(TransactionContext& txn,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (compareTimestampTimestamp(txn, p, 0, q, 0) != 0) ? true : false;
}

static inline bool ltTimestampTimestamp(TransactionContext& txn,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (compareTimestampTimestamp(txn, p, 0, q, 0) < 0) ? true : false;
}

static inline bool gtTimestampTimestamp(TransactionContext& txn,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (compareTimestampTimestamp(txn, p, 0, q, 0) > 0) ? true : false;
}

static inline bool leTimestampTimestamp(TransactionContext& txn,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (compareTimestampTimestamp(txn, p, 0, q, 0) <= 0) ? true : false;
}

static inline bool geTimestampTimestamp(TransactionContext& txn,
	uint8_t const* p, uint32_t, uint8_t const* q, uint32_t) {
	return (compareTimestampTimestamp(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline int32_t compareBoolBool(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const bool*>(p))) <
				   (*(reinterpret_cast<const bool*>(q)))
			   ? -1
			   : (*(reinterpret_cast<const bool*>(p))) ==
						 (*(reinterpret_cast<const bool*>(q)))
					 ? 0
					 : 1;
}

static inline bool eqBoolBool(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const bool*>(p))) ==
		   (*(reinterpret_cast<const bool*>(q)));
}
static inline bool neBoolBool(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t) {
	return (*(reinterpret_cast<const bool*>(p))) !=
		   (*(reinterpret_cast<const bool*>(q)));
}
static inline bool ltBoolBool(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareBoolBool(txn, p, 0, q, 0) < 0) ? true : false;
}
static inline bool gtBoolBool(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareBoolBool(txn, p, 0, q, 0) > 0) ? true : false;
}
static inline bool leBoolBool(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareBoolBool(txn, p, 0, q, 0) <= 0) ? true : false;
}
static inline bool geBoolBool(TransactionContext& txn, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t) {
	return (compareBoolBool(txn, p, 0, q, 0) >= 0) ? true : false;
}

static inline int32_t compareStringString(TransactionContext&, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	if (size1 == size2) {
		return strncmp(reinterpret_cast<const char*>(p),
			reinterpret_cast<const char*>(q), size2);
	}
	else if (size1 < size2) {
		int32_t ret = strncmp(reinterpret_cast<const char*>(p),
			reinterpret_cast<const char*>(q), size1);
		return ret != 0 ? ret : -1;
	}
	else {
		int32_t ret = strncmp(reinterpret_cast<const char*>(p),
			reinterpret_cast<const char*>(q), size2);
		return ret != 0 ? ret : 1;
	}
}
static inline bool eqStringString(TransactionContext& txn, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringString(txn, p, size1, q, size2) == 0) ? true : false;
}
static inline bool neStringString(TransactionContext& txn, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringString(txn, p, size1, q, size2) != 0) ? true : false;
}
static inline bool ltStringString(TransactionContext& txn, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringString(txn, p, size1, q, size2) < 0) ? true : false;
}
static inline bool gtStringString(TransactionContext& txn, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringString(txn, p, size1, q, size2) > 0) ? true : false;
}
static inline bool leStringString(TransactionContext& txn, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringString(txn, p, size1, q, size2) <= 0) ? true : false;
}
static inline bool geStringString(TransactionContext& txn, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringString(txn, p, size1, q, size2) >= 0) ? true : false;
}

static inline int32_t compareStringStringI(
	uint8_t const* p, uint32_t size1, uint8_t const* q, uint32_t size2) {
	uint32_t cmpLen;
	int32_t sameValueResult;
	if (size1 == size2) {
		cmpLen = size1;
		sameValueResult = 0;
	}
	else if (size1 < size2) {
		cmpLen = size1;
		sameValueResult = -1;
	}
	else {
		cmpLen = size2;
		sameValueResult = 1;
	}
	char c1, c2;
	for (uint32_t i = 0; i < cmpLen; i++) {
		c1 = *(p + i);
		if ((c1 >= 'a') && (c1 <= 'z')) {
			c1 -= 32;
		}
		c2 = *(q + i);
		if ((c2 >= 'a') && (c2 <= 'z')) {
			c2 -= 32;
		}
		if (c1 < c2) {
			return -1;
		}
		else if (c1 > c2) {
			return 1;
		}
	}
	return sameValueResult;
}

static inline int32_t compareStringStringI(TransactionContext&,
	uint8_t const* p, uint32_t size1, uint8_t const* q, uint32_t size2) {
	return compareStringStringI(p, size1, q, size2);
}
static inline bool eqCaseStringStringI(TransactionContext& txn,
	uint8_t const* p, uint32_t size1, uint8_t const* q, uint32_t size2) {
	return (compareStringStringI(txn, p, size1, q, size2) == 0) ? true : false;
}

static inline int32_t compareBinaryBinary(TransactionContext&, uint8_t const* p,
	uint32_t size1, uint8_t const* q, uint32_t size2) {
	int32_t ret = size1 < size2 ? -1 : size1 == size2 ? 0 : 1;
	return ret != 0 ? ret : memcmp(p, q, size1);
}

static const Comparator comparatorTable[][11] = {
	{&compareStringString, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		NULL}
	,
	{NULL, &compareBoolBool, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		NULL}
	,
	{NULL, NULL, &compareByteByte, &compareByteShort, &compareByteInt,
		&compareByteLong, &compareByteFloat, &compareByteDouble, NULL, NULL,
		NULL}
	,
	{NULL, NULL, &compareShortByte, &compareShortShort, &compareShortInt,
		&compareShortLong, &compareShortFloat, &compareShortDouble, NULL, NULL,
		NULL}
	,
	{NULL, NULL, &compareIntByte, &compareIntShort, &compareIntInt,
		&compareIntLong, &compareIntFloat, &compareIntDouble, NULL, NULL, NULL}
	,
	{NULL, NULL, &compareLongByte, &compareLongShort, &compareLongInt,
		&compareLongLong, &compareLongFloat, &compareLongDouble, NULL, NULL,
		NULL}
	,
	{NULL, NULL, &compareFloatByte, &compareFloatShort, &compareFloatInt,
		&compareFloatLong, &compareFloatFloat, &compareFloatDouble, NULL, NULL,
		NULL}
	,
	{NULL, NULL, &compareDoubleByte, &compareDoubleShort, &compareDoubleInt,
		&compareDoubleLong, &compareDoubleFloat, &compareDoubleDouble, NULL,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &compareTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};

static const Operator eqTable[][11] = {
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
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &eqTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};
static const Operator neTable[][11] = {
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
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &neTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};
static const Operator ltTable[][11] = {
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
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &ltTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};
static const Operator gtTable[][11] = {
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
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &gtTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};
static const Operator leTable[][11] = {
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
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &leTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};
static const Operator geTable[][11] = {
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
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &geTimestampTimestamp,
		NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
	,
	{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}};

static inline void addByteByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) +
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void addByteShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) +
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void addByteInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) +
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void addByteLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) +
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void addByteFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) +
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void addByteDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) +
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void addShortByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) +
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void addShortShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) +
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void addShortInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) +
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void addShortLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) +
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void addShortFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) +
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void addShortDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) +
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void addIntByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) +
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void addIntShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) +
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void addIntInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) +
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void addIntLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) +
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void addIntFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) +
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void addIntDouble(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) +
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void addLongByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) +
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void addLongShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) +
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void addLongInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) +
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void addLongLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) +
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void addLongFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) +
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void addLongDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) +
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void addFloatByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) +
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void addFloatShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) +
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void addFloatInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) +
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void addFloatLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) +
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void addFloatFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) +
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void addFloatDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) +
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void addDoubleByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) +
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void addDoubleShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) +
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void addDoubleInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) +
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void addDoubleLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) +
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void addDoubleFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) +
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void addDoubleDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) +
			  (*(reinterpret_cast<const double*>(q))));
}
const Calculator2 addTable[][11] = {
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
		&addDoubleLong, &addDoubleFloat, &addDoubleDouble, NULL, NULL, NULL,
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
};
static inline void subByteByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) -
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void subByteShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) -
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void subByteInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) -
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void subByteLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) -
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void subByteFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) -
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void subByteDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) -
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void subShortByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) -
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void subShortShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) -
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void subShortInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) -
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void subShortLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) -
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void subShortFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) -
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void subShortDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) -
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void subIntByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) -
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void subIntShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) -
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void subIntInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) -
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void subIntLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) -
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void subIntFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) -
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void subIntDouble(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) -
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void subLongByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) -
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void subLongShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) -
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void subLongInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) -
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void subLongLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) -
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void subLongFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) -
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void subLongDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) -
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void subFloatByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) -
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void subFloatShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) -
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void subFloatInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) -
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void subFloatLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) -
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void subFloatFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) -
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void subFloatDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) -
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void subDoubleByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) -
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void subDoubleShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) -
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void subDoubleInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) -
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void subDoubleLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) -
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void subDoubleFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) -
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void subDoubleDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) -
			  (*(reinterpret_cast<const double*>(q))));
}

const Calculator2 subTable[][11] = {
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
		&subDoubleLong, &subDoubleFloat, &subDoubleDouble, NULL, NULL, NULL,
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
};

static inline void mulByteByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) *
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void mulByteShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) *
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void mulByteInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) *
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void mulByteLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) *
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void mulByteFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) *
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void mulByteDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) *
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void mulShortByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) *
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void mulShortShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) *
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void mulShortInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) *
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void mulShortLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) *
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void mulShortFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) *
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void mulShortDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) *
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void mulIntByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) *
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void mulIntShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) *
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void mulIntInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) *
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void mulIntLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) *
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void mulIntFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) *
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void mulIntDouble(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) *
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void mulLongByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) *
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void mulLongShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) *
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void mulLongInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) *
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void mulLongLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) *
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void mulLongFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) *
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void mulLongDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) *
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void mulFloatByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) *
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void mulFloatShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) *
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void mulFloatInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) *
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void mulFloatLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) *
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void mulFloatFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) *
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void mulFloatDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) *
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void mulDoubleByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) *
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void mulDoubleShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) *
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void mulDoubleInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) *
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void mulDoubleLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) *
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void mulDoubleFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) *
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void mulDoubleDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) *
			  (*(reinterpret_cast<const double*>(q))));
}
const Calculator2 mulTable[][11] = {
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
		&mulDoubleLong, &mulDoubleFloat, &mulDoubleDouble, NULL, NULL, NULL,
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
};
static inline void divByteByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) /
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void divByteShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) /
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void divByteInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) /
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void divByteLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) /
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void divByteFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) /
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void divByteDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int8_t*>(p))) /
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void divShortByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) /
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void divShortShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) /
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void divShortInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) /
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void divShortLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) /
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void divShortFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) /
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void divShortDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int16_t*>(p))) /
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void divIntByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) /
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void divIntShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) /
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void divIntInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) /
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void divIntLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) /
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void divIntFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) /
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void divIntDouble(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int32_t*>(p))) /
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void divLongByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) /
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void divLongShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) /
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void divLongInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) /
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void divLongLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) /
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void divLongFloat(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) /
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void divLongDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const int64_t*>(p))) /
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void divFloatByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) /
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void divFloatShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) /
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void divFloatInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) /
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void divFloatLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) /
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void divFloatFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) /
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void divFloatDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const float*>(p))) /
			  (*(reinterpret_cast<const double*>(q))));
}
static inline void divDoubleByte(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) /
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void divDoubleShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) /
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void divDoubleInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) /
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void divDoubleLong(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) /
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void divDoubleFloat(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) /
			  (*(reinterpret_cast<const float*>(q))));
}
static inline void divDoubleDouble(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	value.set((*(reinterpret_cast<const double*>(p))) /
			  (*(reinterpret_cast<const double*>(q))));
}
const Calculator2 divTable[][11] = {
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
		&divDoubleLong, &divDoubleFloat, &divDoubleDouble, NULL, NULL, NULL,
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
};
static inline void modByteByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) %
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void modByteShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) %
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void modByteInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) %
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void modByteLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int8_t*>(p))) %
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void modShortByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) %
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void modShortShort(TransactionContext&, uint8_t const* p,
	uint32_t, uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) %
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void modShortInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) %
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void modShortLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int16_t*>(p))) %
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void modIntByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) %
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void modIntShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) %
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void modIntInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) %
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void modIntLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int32_t*>(p))) %
			  (*(reinterpret_cast<const int64_t*>(q))));
}
static inline void modLongByte(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int8_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) %
			  (*(reinterpret_cast<const int8_t*>(q))));
}
static inline void modLongShort(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int16_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) %
			  (*(reinterpret_cast<const int16_t*>(q))));
}
static inline void modLongInt(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int32_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) %
			  (*(reinterpret_cast<const int32_t*>(q))));
}
static inline void modLongLong(TransactionContext&, uint8_t const* p, uint32_t,
	uint8_t const* q, uint32_t, Value& value) {
	if (*(reinterpret_cast<const int64_t*>(q)) == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_DIVIDE_BY_ZERO, "Divide by 0 detected.");
	}
	value.set((*(reinterpret_cast<const int64_t*>(p))) %
			  (*(reinterpret_cast<const int64_t*>(q))));
}
const Calculator2 modTable[][11] = {
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
};
#endif
