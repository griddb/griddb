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

#ifndef SQL_EXPRESSION_NUMERIC_H_
#define SQL_EXPRESSION_NUMERIC_H_

#include "sql_expression_base.h"

struct SQLNumericExprs {
	typedef SQLExprs::ExprType ExprType;

	class Registrar;
	struct Specs;
	struct Functions;
};

class SQLNumericExprs::Registrar :
		public SQLExprs::BasicExprRegistrar<SQLNumericExprs::Specs> {
public:
	virtual void operator()() const;

private:
	static const SQLExprs::ExprRegistrar REGISTRAR_INSTANCE;
};

struct SQLNumericExprs::Specs {
	typedef SQLExprs::ExprSpec ExprSpec;
	typedef SQLExprs::ExprSpecBase Base;
	template<ExprType, int = 0> struct Spec;

	template<int C> struct Spec<SQLType::FUNC_ABS, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC> >,
				ExprSpec::FLAG_INHERIT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_HEX_TO_DEC, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_STRING> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_LOG, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE>,
				Base::In<TupleTypes::TYPE_DOUBLE> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_RANDOM, C> {
		typedef Base::Type<
				TupleTypes::TYPE_LONG, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_ROUND, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC>,
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_SQRT, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TRUNC, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC>,
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT1> Type;
	};
};

struct SQLNumericExprs::Functions {
	struct Random {
		template<typename C>
		int64_t operator()(C &cxt);
	};
};

#endif
