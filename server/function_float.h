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
	@brief Definition of Floating-point functions for TQL
*/

#ifndef FUNCTIONS_FLOAT_H_
#define FUNCTIONS_FLOAT_H_
#include <algorithm>
#include <cfloat>
#include <string>

#include "expression.h"
#include "qp_def.h"

#ifdef _WIN32
#define round(x) (((x) < 0) ? ceil((x)-0.5) : floor((x) + 0.5))
#endif
/*!
 * @brief ROUND()
 *
 * @return rounded value
 */
class FunctorRound : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
				"Invalid argument count");
		}
		if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isNumeric()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
				"Argument 1 is invalid");
		}
		return Expr::newNumericValue(round(args[0]->getValueAsDouble()), txn);
	}

	virtual ~FunctorRound() {}
};

/*!
 * @brief CEIL()
 *
 * @return ceiling value
 */
class FunctorCeil : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
				"Invalid argument count");
		}
		if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isNumeric()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
				"Argument 1 is invalid");
		}
		return Expr::newNumericValue(ceil(args[0]->getValueAsDouble()), txn);
	}

	virtual ~FunctorCeil() {}
};

/*!
 * @brief FLOOR()
 *
 * @return rounded value
 */
class FunctorFloor : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &) {
		if (args.empty() || args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
				"Invalid argument count");
		}
		if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isNumeric()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
				"Argument 1 is invalid");
		}
		return Expr::newNumericValue(floor(args[0]->getValueAsDouble()), txn);
	}
	virtual ~FunctorFloor() {}
};

#endif /*FUNCTIONS_FLOAT_HPP*/
