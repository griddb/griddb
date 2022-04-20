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
	@brief Definition of Array functions for TQL
*/

#ifndef FUNCTIONS_ARRAY_H_
#define FUNCTIONS_ARRAY_H_
#include "expression.h"
#include "qp_def.h"

#include <algorithm>
#include <string>

/*!
 * @brief ARRAY_LENGTH()
 *
 * @return array_length
 */
class FunctorArrayLength : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty() || args.size() > 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[0]->isArrayValue()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not an array");
		}
		else {
			size_t x = args[0]->getArrayLength(txn, objectManager, strategy);
			return Expr::newNumericValue(int32_t(x), txn);
		}
	}
	virtual ~FunctorArrayLength() {}
};

/*!
 * @brief ELEMENT(n, array)
 *
 * @return Element of an array
 */
class FunctorElement : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty() || args.size() != 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue() || args[1]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[0]->isNumericInteger()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Invalid datatypes: argument 1 is not an integer");
		}
		else if (!args[1]->isArrayValue()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Invalid datatypes: argument 2 is not an array");
		}
		else {
			int idx = args[0]->getValueAsInt();
			Expr *ar = args[1]->getArrayElement(txn, objectManager, strategy, idx);
			return ar;
		}
	}
	virtual ~FunctorElement() {}
};

#endif /*FUNCTIONS_ARRAY_HPP*/
