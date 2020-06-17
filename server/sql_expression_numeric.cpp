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

#include "sql_expression_numeric.h"
#include "sql_utils_vdbe.h"
#include "query_function_numeric.h"


const SQLExprs::ExprRegistrar
SQLNumericExprs::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLNumericExprs::Registrar::operator()() const {
	add<SQLType::FUNC_ABS, NumericFunctions::Abs>();
	add<SQLType::FUNC_HEX_TO_DEC, NumericFunctions::HexToDec>();
	add<SQLType::FUNC_LOG, NumericFunctions::Log>();
	add<SQLType::FUNC_RANDOM, Functions::Random>();
	add<SQLType::FUNC_ROUND, NumericFunctions::Round>();
	add<SQLType::FUNC_SQRT, NumericFunctions::Sqrt>();
	add<SQLType::FUNC_TRUNC, NumericFunctions::Trunc>();
}


template<typename C>
inline int64_t SQLNumericExprs::Functions::Random::operator()(C &cxt) {
	static_cast<void>(cxt);

	int64_t value;
	SQLVdbeUtils::VdbeUtils::generateRandom(&value, sizeof(value));

	return value;
}
