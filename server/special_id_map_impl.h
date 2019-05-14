﻿/*
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
	@brief Implementation of TqlSpecialId functions for TQL
*/

#ifndef SPECIAL_ID_IMPL_H_
#define SPECIAL_ID_IMPL_H_


/*!
	@brief Return ID name
*/
Expr *TqlSpecialId::operator()(
	ExprList &, TransactionContext &txn, ObjectManager &) {
	return Expr::newStringValue(name_, txn);
}

#endif
