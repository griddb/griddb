/*
	Copyright (c) 2023 TOSHIBA Digital Solutions Corporation
	
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


#ifndef SQL_COMPILER_INTERNAL_H_
#define SQL_COMPILER_INTERNAL_H_

#include "sql_compiler.h"


UTIL_TRACER_DECLARE(SQL_HINT);

#define SQL_COMPILER_THROW_ERROR(errorCode, exprPtr, message) \
	do { \
		/* TODO */ \
		static_cast<void>(static_cast<const SyntaxTree::Expr*>(exprPtr)); \
		GS_THROW_USER_ERROR(errorCode, message); \
	} \
	while (false)

#define SQL_COMPILER_RETHROW_ERROR(cause, exprPtr, message) \
	do { \
		/* TODO */ \
		static_cast<void>(static_cast<const SyntaxTree::Expr*>(exprPtr)); \
		GS_RETHROW_USER_ERROR(cause, message); \
	} \
	while (false)



#endif 
