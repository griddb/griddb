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

#ifndef SQL_EXPRESSION_CORE_H_
#define SQL_EXPRESSION_CORE_H_

#include "sql_expression_base.h"

struct SQLCoreExprs {
	typedef SQLExprs::ExprType ExprType;
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	class CustomRegistrar;
	class BasicRegistrar;
	struct Specs;

	class IdExpression;
	class ConstantExpression;
	class TupleColumnExpression;

	class AndExpression;
	class OrExpression;
	class CaseExpression;
	class CastExpression;
	class NoopExpression;

	struct Functions;
};

class SQLCoreExprs::CustomRegistrar :
		public SQLExprs::CustomExprRegistrar<SQLCoreExprs::Specs> {
public:
	virtual void operator()() const;

private:
	static const SQLExprs::ExprRegistrar REGISTRAR_INSTANCE;
};

class SQLCoreExprs::BasicRegistrar :
		public SQLExprs::BasicExprRegistrar<SQLCoreExprs::Specs> {
public:
	virtual void operator()() const;

private:
	static const SQLExprs::ExprRegistrar REGISTRAR_INSTANCE;
};

struct SQLCoreExprs::Specs {
	typedef SQLExprs::ExprSpec ExprSpec;
	typedef SQLExprs::ExprSpecBase Base;
	template<ExprType, int = 0> struct Spec;

	typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
			Base::PromotableNumIn,
			Base::PromotableNumIn>,
			ExprSpec::FLAG_INHERIT1> BinaryArithBaseSpec;

	typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
			Base::In<TupleTypes::TYPE_LONG>,
			Base::In<TupleTypes::TYPE_LONG> > > BinaryBitsBaseSpec;

	typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
			Base::NullCheckedPromotableAnyIn,
			Base::NullCheckedPromotableAnyIn> > BinaryCompBaseSpec;


	template<int C> struct Spec<SQLType::EXPR_ID, C> {
		typedef Base::Type<
				TupleTypes::TYPE_LONG, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_COLUMN, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<> > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_CONSTANT, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<> > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_TYPE, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<> > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_SELECTION, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_PRODUCTION, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_PROJECTION, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_AND, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_BOOL>,
				Base::In<TupleTypes::TYPE_BOOL> > > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_OR, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_BOOL>,
				Base::In<TupleTypes::TYPE_BOOL> > > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_CASE, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_BOOL>,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_INHERIT2 | ExprSpec::FLAG_REPEAT2 |
				ExprSpec::FLAG_EVEN_ARGS_NULLABLE |
				ExprSpec::FLAG_ODD_TAIL |
				ExprSpec::FLAG_ARGS_LISTING> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_PLACEHOLDER, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_LIST, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_AGG_FOLLOWING, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_HINT_NO_INDEX, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::OP_CAST, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_ANY> > > Type;
	};


	template<int C> struct Spec<SQLType::FUNC_COALESCE, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_REPEAT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_IFNULL, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MAX, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_REPEAT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MIN, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_REPEAT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_NULLIF, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_NULLABLE | ExprSpec::FLAG_INHERIT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TYPEOF, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::OP_CONCAT, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				typename Base::NullCheckedAnyIn,
				typename Base::NullCheckedAnyIn>,
				ExprSpec::FLAG_REPEAT1> Type;
	};

	template<int C> struct Spec<SQLType::OP_MULTIPLY, C> {
		typedef BinaryArithBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_DIVIDE, C> {
		typedef BinaryArithBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_REMAINDER, C> {
		typedef BinaryArithBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_ADD, C> {
		typedef BinaryArithBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_SUBTRACT, C> {
		typedef BinaryArithBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_SHIFT_LEFT, C> {
		typedef BinaryBitsBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_SHIFT_RIGHT, C> {
		typedef BinaryBitsBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_BIT_AND, C> {
		typedef BinaryBitsBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_BIT_OR, C> {
		typedef BinaryBitsBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_LT, C> {
		typedef BinaryCompBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_LE, C> {
		typedef BinaryCompBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_GT, C> {
		typedef BinaryCompBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_GE, C> {
		typedef BinaryCompBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_EQ, C> {
		typedef BinaryCompBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_NE, C> {
		typedef BinaryCompBaseSpec Type;
	};

	template<int C> struct Spec<SQLType::OP_IS, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::OP_IS_NOT, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::OP_IS_NULL, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::OP_IS_NOT_NULL, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::OP_BIT_NOT, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::OP_NOT, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_BOOL> > > Type;
	};
};

class SQLCoreExprs::IdExpression : public SQLExprs::Expression {
public:
	IdExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;
};

class SQLCoreExprs::ConstantExpression : public SQLExprs::Expression {
public:
	ConstantExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;
};

class SQLCoreExprs::TupleColumnExpression : public SQLExprs::Expression {
public:
	TupleColumnExpression(
			ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;

private:
	bool unified_;
};

class SQLCoreExprs::AndExpression : public SQLExprs::Expression {
public:
	virtual TupleValue eval(ExprContext &cxt) const;
};

class SQLCoreExprs::OrExpression : public SQLExprs::Expression {
public:
	virtual TupleValue eval(ExprContext &cxt) const;
};

class SQLCoreExprs::CaseExpression : public SQLExprs::Expression {
public:
	virtual TupleValue eval(ExprContext &cxt) const;
};

class SQLCoreExprs::CastExpression : public SQLExprs::Expression {
public:
	CastExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;
};

class SQLCoreExprs::NoopExpression : public SQLExprs::Expression {
public:
	virtual TupleValue eval(ExprContext &cxt) const;
};

struct SQLCoreExprs::Functions {
	typedef SQLExprs::ExprUtils::FunctorPolicy::DefaultPolicy DefaultPolicy;

	struct Coalesce {
		typedef DefaultPolicy::AsPartiallyFinishable::AsResultPromotable::
				AsArgsDelayedEvaluable Policy;

		template<typename C>
		TupleValue operator()(C &cxt);

		template<typename C>
		TupleValue operator()(C &cxt, const TupleValue &value);
	};

	struct Max {
	public:
		typedef DefaultPolicy::AsPartiallyFinishable::AsResultPromotable Policy;

		template<typename C>
		TupleValue operator()(C &cxt);

		template<typename C>
		TupleValue operator()(C &cxt, const TupleValue &value);

		template<typename C>
		TupleValue operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);

	private:
		TupleValue result_;
	};

	struct Min {
	public:
		typedef DefaultPolicy::AsPartiallyFinishable::AsResultPromotable Policy;

		template<typename C>
		TupleValue operator()(C &cxt);

		template<typename C>
		TupleValue operator()(C &cxt, const TupleValue &value);

		template<typename C>
		TupleValue operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);

	private:
		TupleValue result_;
	};

	struct NullIf {
		typedef DefaultPolicy::AsResultPromotable Policy;

		template<typename C>
		TupleValue operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct TypeOf {
		template<typename C>
		typename C::WriterType& operator()(C &cxt, const TupleValue &value);
	};

	struct Concat {
		typedef DefaultPolicy::AsPartiallyFinishable Policy;

		template<typename C>
		typename C::WriterType& operator()(C &cxt);

		template<typename C>
		typename C::WriterType& operator()(C &cxt, const TupleValue &value);

		template<typename C>
		typename C::WriterType& operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Multiply {
		template<typename C, typename T>
		T operator()(C &cxt, const T &value1, const T &value2);
	};

	struct Divide {
		template<typename C, typename T>
		T operator()(C &cxt, const T &value1, const T &value2);
	};

	struct Remainder {
		template<typename C, typename T>
		T operator()(C &cxt, const T &value1, const T &value2);
	};

	struct Add {
		template<typename C, typename T>
		T operator()(C &cxt, const T &value1, const T &value2);
	};

	struct Subtract {
		template<typename C, typename T>
		T operator()(C &cxt, const T &value1, const T &value2);
	};

	template<typename Left>
	struct Shift {
		template<typename C>
		int64_t operator()(C &cxt, int64_t value1, int64_t value2);
	};
	typedef Shift<util::TrueType> ShiftLeft;
	typedef Shift<util::FalseType> ShiftRight;

	struct BitAnd {
		template<typename C>
		int64_t operator()(C &cxt, int64_t value1, int64_t value2);
	};

	struct BitOr {
		template<typename C>
		int64_t operator()(C &cxt, int64_t value1, int64_t value2);
	};

	struct Lt {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Le {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Gt {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Ge {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Eq {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Ne {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct Is {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct IsNot {
		template<typename C>
		bool operator()(
				C &cxt, const TupleValue &value1, const TupleValue &value2);
	};

	struct IsNull {
		template<typename C>
		bool operator()(C &cxt, const TupleValue &value);
	};

	struct IsNotNull {
		template<typename C>
		bool operator()(C &cxt, const TupleValue &value);
	};

	struct BitNot {
		template<typename C>
		int64_t operator()(C &cxt, int64_t value);
	};

	struct Not {
		template<typename C>
		bool operator()(C &cxt, bool value);
	};
};

#endif
