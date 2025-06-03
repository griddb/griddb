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
#include "sql_utils_algorithm.h"

struct SQLCoreExprs {
	typedef SQLExprs::ExprType ExprType;
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	class CustomRegistrar;
	class BasicRegistrar;
	struct Specs;

	struct VariantTypes;
	struct VariantUtils;

	struct ArgExprCode;
	struct GeneralColumnCode;
	struct ReaderColumnCode;
	struct ReadableTupleColumnCode;
	struct SummaryColumnCode;
	struct ConstCode;

	template<typename T, size_t I, bool Nullable>
	class ArgExprEvaluator;
	class GeneralColumnEvaluator;
	template<typename T, bool Nullable, bool Multi>
	class ReaderColumnEvaluator;
	template<typename T, bool Nullable>
	class ReadableTupleColumnEvaluator;
	template<typename T, bool Nullable>
	class SummaryColumnEvaluator;
	template<typename T>
	class ConstEvaluator;

	class IdExpression;
	class ConstantExpression;
	class ColumnExpression;

	class ComparisonExpressionBase;
	template<typename Func> class ComparisonExpression;

	class AndExpression;
	class OrExpression;
	class CaseExpression;
	class InExpression;
	class CastExpression;
	class NoopExpression;
	class RangeKeyExpression;
	class LinearExpression;
	class RangeGroupIdExpression;

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
		typedef Base::Type<
				TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_CONSTANT, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<> > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_TYPE, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<> > Type;
	};

	template<int C> struct Spec<SQLType::EXPR_SELECTION, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING |
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_PRODUCTION, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING |
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_PROJECTION, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_ARGS_LISTING |
				ExprSpec::FLAG_DYNAMIC> Type;
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

	template<int C> struct Spec<SQLType::EXPR_IN, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::NullCheckedPromotableAnyIn,
				Base::NullCheckedPromotableAnyIn>,
				ExprSpec::FLAG_INTERNAL> Type;
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

	template<int C> struct Spec<SQLType::EXPR_WINDOW_OPTION, C> {
		typedef Base::Type< TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_GROUP, C> {
		typedef Base::Type< TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_LONG> >,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_GROUP_ID, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_KEY, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_NON_NULLABLE |
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_KEY_CURRENT, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_AGG, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::PromotableAnyIn,
				Base::PromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_FILL, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_FILL_NONE, C> {
		typedef Base::Type< TupleTypes::TYPE_LONG, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_FILL_NULL, C> {
		typedef typename Spec<SQLType::EXPR_RANGE_FILL_NONE, C>::Type Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_FILL_PREV, C> {
		typedef typename Spec<SQLType::EXPR_RANGE_FILL_NONE, C>::Type Type;
	};

	template<int C> struct Spec<SQLType::EXPR_RANGE_FILL_LINEAR, C> {
		typedef typename Spec<SQLType::EXPR_RANGE_FILL_NONE, C>::Type Type;
	};

	template<int C> struct Spec<SQLType::EXPR_LINEAR, C> {
		typedef Base::Type< TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY>,
				Base::PromotableAnyIn,
				Base::In<TupleTypes::TYPE_ANY>,
				Base::PromotableAnyIn,
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT2> Type;
	};


	template<int C> struct Spec<SQLType::FUNC_COALESCE, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
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
				typename Base::NullCheckedPromotableAnyIn,
				typename Base::NullCheckedPromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_REPEAT1 |
				ExprSpec::FLAG_COMP_SENSITIVE> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MIN, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::NullCheckedPromotableAnyIn,
				typename Base::NullCheckedPromotableAnyIn>,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_REPEAT1 |
				ExprSpec::FLAG_COMP_SENSITIVE> Type;
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

struct SQLCoreExprs::VariantTypes {
	template<size_t N>
	struct CombiOf {
		enum {
			VALUE = (N + 1) * N / 2
		};
	};

	template<size_t N, size_t I>
	struct CombiIndexOf {
		enum {
			COMBI_COUNT = CombiOf<N>::VALUE,
			LOWER_COMBI_COUNT = (N <= 1 ? 0 : CombiOf<N - 1>::VALUE),
			UNDER_LOWER = (I < LOWER_COMBI_COUNT),
			VALUE1 = (UNDER_LOWER ?
					static_cast<size_t>(CombiIndexOf<N - 1, I>::VALUE1) :
					N - 1),
			VALUE2 = (UNDER_LOWER ?
					static_cast<size_t>(CombiIndexOf<N - 1, I>::VALUE2) :
					I - LOWER_COMBI_COUNT)
		};
	};

	template<size_t I>
	struct CombiIndexOf<0, I> {
		enum {
			VALUE1 = 0,
			VALUE2 = 0
		};
	};

	enum {
		COUNT_COMP_TYPE = 6, 
		COUNT_NULLABLE = 2, 
		COUNT_INTEGRAL = 4,
		COUNT_FLOATING = 2,
		COUNT_TIMESTAMP_FULL = 3, 
		COUNT_TIMESTAMP_BASIC = COUNT_TIMESTAMP_FULL - 1, 
		COUNT_OTHER_TYPE = 3, 

		COUNT_NUMERIC = COUNT_INTEGRAL + COUNT_FLOATING,
		COUNT_FULL_NO_NUMERIC =
				(COUNT_OTHER_TYPE + COUNT_TIMESTAMP_FULL),
		COUNT_BASIC_NO_NUMERIC =
				(COUNT_OTHER_TYPE + COUNT_TIMESTAMP_BASIC),
		COUNT_CONTAINER_SOURCE = 2, 

		COUNT_NUMERIC_PROMO = COUNT_INTEGRAL,
		COUNT_TIMESTAMP_PROMO = COUNT_TIMESTAMP_FULL - 1, 
		COUNT_PROMO = COUNT_NUMERIC_PROMO + COUNT_TIMESTAMP_PROMO,

		COUNT_BASIC_TYPE = COUNT_NUMERIC + COUNT_BASIC_NO_NUMERIC,
		COUNT_FULL_TYPE = COUNT_NUMERIC + COUNT_FULL_NO_NUMERIC,
		COUNT_BASIC_AND_PROMO_TYPE = COUNT_BASIC_TYPE + COUNT_PROMO,

		COUNT_NUMERIC_COMBI = CombiOf<COUNT_NUMERIC>::VALUE,
		COUNT_NO_BASIC_TIMESTAMP_COMBI = 1, 
		COUNT_BASIC_TIMESTAMP_COMBI =
				CombiOf<COUNT_TIMESTAMP_FULL>::VALUE -
				COUNT_NO_BASIC_TIMESTAMP_COMBI,
		COUNT_SOME_BASIC_COMBI =
				(COUNT_NUMERIC_COMBI + COUNT_BASIC_TIMESTAMP_COMBI),
		COUNT_NO_COMBI = COUNT_OTHER_TYPE,
		COUNT_BASIC_COMBI = (COUNT_SOME_BASIC_COMBI + COUNT_NO_COMBI),

		INDEX_NUMERIC_BEGIN = 0,
		INDEX_OTHER_BEGIN = COUNT_NUMERIC,
		INDEX_TIMESTAMP_BEGIN = INDEX_OTHER_BEGIN + COUNT_OTHER_TYPE,

		INDEX_NUMERIC_PROMO = COUNT_NUMERIC - 1,
		INDEX_BASIC_TIMESTAMP_PROMO =
				INDEX_TIMESTAMP_BEGIN + COUNT_TIMESTAMP_BASIC - 1
	};

	template<size_t I>
	struct ColumnTypeByComp {
		typedef SQLValues::Types Types;
		typedef
				typename util::Conditional<I == 0, Types::Long,
				typename util::Conditional<I == 1, Types::Double,
				typename util::Conditional<I == 2, Types::String,
				typename util::Conditional<I == 3, Types::Blob,
				typename util::Conditional<I == 4, Types::MicroTimestampTag,
				typename util::Conditional<I == 5, Types::NanoTimestampTag,
						void
				>::Type>::Type>::Type>::Type>::Type>::Type ValueTypeTag;
		typedef ValueTypeTag CompTypeTag;
	};

	template<size_t I>
	struct ColumnTypeByBasic {
		typedef SQLValues::Types Types;
		typedef
				typename util::Conditional<I == 0, Types::Byte,
				typename util::Conditional<I == 1, Types::Short,
				typename util::Conditional<I == 2, Types::Integer,
				typename util::Conditional<I == 3, Types::Long,
				typename util::Conditional<I == 4, Types::Float,
				typename util::Conditional<I == 5, Types::Double,
				typename util::Conditional<I == 6, Types::Bool,
				typename util::Conditional<I == 7, Types::String,
				typename util::Conditional<I == 8, Types::Blob,
				typename util::Conditional<I == 9, Types::MicroTimestampTag,
				typename util::Conditional<I == 10, Types::NanoTimestampTag,
						void
				>::Type>::Type>::Type>::Type>::Type
				>::Type>::Type>::Type>::Type>::Type
				>::Type ValueTypeTag;
		typedef typename util::Conditional<I == 6,
				Types::Bool,
				typename ColumnTypeByComp<(
						I < 4 ? 0 :
						I < 6 ? 1 : (I - 5))>::CompTypeTag>::Type CompTypeTag;
	};

	template<size_t I>
	struct ColumnTypeByFull {
		typedef SQLValues::Types Types;
		typedef typename util::Conditional<(I == 11),
				Types::TimestampTag,
				typename ColumnTypeByBasic<I>::ValueTypeTag
				>::Type ValueTypeTag;

		typedef typename util::Conditional<(I == 11),
				Types::TimestampTag,
				typename ColumnTypeByBasic<I>::CompTypeTag
				>::Type CompTypeTag;
	};

	template<size_t I>
	struct TimestampFullIndexOf {
		enum {
			VALUE = (INDEX_TIMESTAMP_BEGIN +
					(I > 0 ? I - 1 : COUNT_TIMESTAMP_BASIC + 0))
		};
	};

	template<size_t I>
	struct ColumnTypeByBasicAndPromo {
	private:
		enum {
			BASIC_PROMO = (I >= COUNT_BASIC_TYPE),
			PROMO_OFFSET = (I - (BASIC_PROMO ? COUNT_BASIC_TYPE : 0)),

			TIMESTAMP_PROMO = (PROMO_OFFSET >= COUNT_NUMERIC_PROMO),
			TIMESTAMP_PROMO_INDEX = (TimestampFullIndexOf<
					PROMO_OFFSET - (TIMESTAMP_PROMO ?
							COUNT_NUMERIC_PROMO : 0)>::VALUE),

			NUMERIC_PROMO = (BASIC_PROMO && !TIMESTAMP_PROMO),
			NUMERIC_PROMO_INDEX = INDEX_NUMERIC_BEGIN +
					(NUMERIC_PROMO ? PROMO_OFFSET : 0),
			NUMERIC_NON_PROMO = (!BASIC_PROMO && I < COUNT_NUMERIC),

			SRC_BASIC_NUM = (BASIC_PROMO ? (TIMESTAMP_PROMO ?
					static_cast<size_t>(TIMESTAMP_PROMO_INDEX) :
					static_cast<size_t>(NUMERIC_PROMO_INDEX)) : I),
			DEST_BASIC_NUM = (BASIC_PROMO ? (TIMESTAMP_PROMO ?
					INDEX_BASIC_TIMESTAMP_PROMO : INDEX_NUMERIC_PROMO) + 0 :
					SRC_BASIC_NUM),

			COMP_TYPE_ADJUSTING = (
					SRC_BASIC_NUM != DEST_BASIC_NUM ||
					NUMERIC_NON_PROMO)
		};

		typedef ColumnTypeByFull<SRC_BASIC_NUM> SrcBaseType;
		typedef ColumnTypeByFull<DEST_BASIC_NUM> DestBaseType;

	public:
		typedef typename SrcBaseType::ValueTypeTag ValueTypeTag;
		typedef typename util::Conditional<
				COMP_TYPE_ADJUSTING,
				typename DestBaseType::CompTypeTag,
				ValueTypeTag>::Type CompTypeTag;
	};

	template<size_t I, size_t S>
	struct ColumnTypeByBasicCombi {
	private:
		enum {
			NUMERIC_COMBI = (I < COUNT_NUMERIC_COMBI),
			NUMERIC_COMBI_INDEX = (NUMERIC_COMBI ? I : 0),

			TIMESTAMP_COMBI = (!NUMERIC_COMBI &&
					(I < COUNT_NUMERIC_COMBI + COUNT_BASIC_TIMESTAMP_COMBI)),
			TIMESTAMP_COMBI_INDEX = COUNT_NO_BASIC_TIMESTAMP_COMBI +
					(I - (TIMESTAMP_COMBI ? COUNT_NUMERIC_COMBI : 0)),

			NO_COMBI = (!NUMERIC_COMBI && !TIMESTAMP_COMBI),
			TYPE_INDEX_NO_COMBI = INDEX_OTHER_BEGIN +
					(I - (NO_COMBI ? COUNT_SOME_BASIC_COMBI : 0)),

			COMBI_COUNT = (NO_COMBI ? 1 : (TIMESTAMP_COMBI ?
					COUNT_TIMESTAMP_FULL : COUNT_NUMERIC)),
			COMBI_INDEX = (NO_COMBI ? 0 : (TIMESTAMP_COMBI ?
					TIMESTAMP_COMBI_INDEX : NUMERIC_COMBI_INDEX))
		};

		typedef CombiIndexOf<COMBI_COUNT, COMBI_INDEX> BaseType;

		enum {
			BASE_OFFSET = (S == 0 ? BaseType::VALUE1 : BaseType::VALUE2),
			TYPE_INDEX = (NO_COMBI ? TYPE_INDEX_NO_COMBI :
					(TIMESTAMP_COMBI ?
							TimestampFullIndexOf<BASE_OFFSET>::VALUE :
							(INDEX_NUMERIC_BEGIN + BASE_OFFSET)))
		};

	public:
		typedef typename ColumnTypeByFull<TYPE_INDEX>::ValueTypeTag TypeTag;
	};
};

struct SQLCoreExprs::VariantUtils {
	typedef SQLExprs::Expression Expression;

	static size_t toCompTypeNumber(TupleColumnType src);
	static size_t toBasicTypeNumber(TupleColumnType src);
	static size_t toFullTypeNumber(TupleColumnType src);
	static size_t toBasicAndPromoTypeNumber(
			TupleColumnType src, TupleColumnType promo);
	static bool findBasicCombiTypeNumber(
			TupleColumnType type1, TupleColumnType type2, size_t &num);

	static size_t toTimestampPromoSubIndex(TupleColumnType src);
	static size_t toTimestampCombiSubIndex(TupleColumnType src);

	static bool findCombiNumber(size_t n1, size_t n2, size_t &num);
	static size_t getCombiCount(size_t n);

	static const Expression& getBaseExpression(
			ExprFactoryContext &cxt, const size_t *argIndex);
};

struct SQLCoreExprs::ArgExprCode {
	typedef SQLExprs::Expression Expression;

	ArgExprCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

	const Expression *expr_;
};

struct SQLCoreExprs::GeneralColumnCode {
	typedef SQLExprs::Expression Expression;

	GeneralColumnCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

	ExprCode base_;
	bool unified_;
};

struct SQLCoreExprs::ReaderColumnCode {
public:
	typedef SQLExprs::Expression Expression;

	ReaderColumnCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	ReaderColumnCode(const ReaderColumnCode&);
	ReaderColumnCode& operator=(const ReaderColumnCode&);

	typedef SQLExprs::TupleColumn TupleColumn;
	typedef SQLExprs::TupleListReader TupleListReader;

public:
	TupleListReader *reader_;
	TupleListReader **activeReaderRef_;
	TupleColumn column_;
};

struct SQLCoreExprs::ReadableTupleColumnCode {
public:
	typedef SQLExprs::Expression Expression;

	ReadableTupleColumnCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	ReadableTupleColumnCode(const ReadableTupleColumnCode&);
	ReadableTupleColumnCode& operator=(const ReadableTupleColumnCode&);

	typedef SQLExprs::TupleColumn TupleColumn;
	typedef SQLExprs::ReadableTuple ReadableTuple;

public:
	const ReadableTuple *tuple_;
	TupleColumn column_;
};

struct SQLCoreExprs::SummaryColumnCode {
public:
	typedef SQLExprs::Expression Expression;

	SummaryColumnCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	SummaryColumnCode(const SummaryColumnCode&);
	SummaryColumnCode& operator=(const SummaryColumnCode&);

	typedef SQLExprs::SummaryTuple SummaryTuple;
	typedef SQLExprs::SummaryColumn SummaryColumn;

public:
	const SummaryTuple *tuple_;
	const SummaryColumn *column_;
};

struct SQLCoreExprs::ConstCode {
	typedef SQLExprs::Expression Expression;

	ConstCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

	TupleValue value_;
};

template<typename T, size_t I, bool Nullable>
class SQLCoreExprs::ArgExprEvaluator {
public:
	typedef T ValueTypeTag;

private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef SQLExprs::Expression Expression;
	typedef typename ValueTypeTag::ValueType ValueType;

public:
	typedef const Expression &EvaluatorType;
	typedef ArgExprCode CodeType;
	typedef TupleValue ResultType;

	static const Expression& evaluator(const CodeType &code) {
		assert(code.expr_ != NULL);
		const Expression &arg1 = code.expr_->child();
		return (I == 0 ? arg1 : arg1.next());
	}

	static bool check(const ResultType &value) {
		return !Nullable || !ValueUtils::isNull(value);
	}

	static ValueType get(const ResultType &value) {
		return ValueUtils::getValue<ValueType>(value);
	}
};

class SQLCoreExprs::GeneralColumnEvaluator {
private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef TupleValue ValueType;

public:
	typedef SQLValues::Types::Any ValueTypeTag;
	typedef GeneralColumnEvaluator EvaluatorType;
	typedef GeneralColumnCode CodeType;
	typedef TupleValue ResultType;

	static GeneralColumnEvaluator evaluator(const CodeType &code) {
		return GeneralColumnEvaluator(code);
	}

	static bool check(const ResultType &value) {
		return !ValueUtils::isNull(value);
	}

	static const ValueType& get(const ResultType &value) {
		return value;
	}

	ResultType eval(ExprContext &cxt) const;

private:
	explicit GeneralColumnEvaluator(const CodeType &code) : code_(code) {
	}

	const CodeType &code_;
};

template<typename T, bool Nullable, bool Multi>
class SQLCoreExprs::ReaderColumnEvaluator {
public:
	typedef T ValueTypeTag;

private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename ValueTypeTag::ValueType ValueType;

public:
	typedef ReaderColumnEvaluator EvaluatorType;
	typedef ReaderColumnCode CodeType;
	typedef std::pair<ValueType, bool> ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return EvaluatorType(code);
	}

	static bool check(const ResultType &value) {
		return !Nullable || !value.second;
	}

	static const ValueType& get(const ResultType &value) {
		return value.first;
	}

	ResultType eval(ExprContext &cxt) const {
		static_cast<void>(cxt);
		SQLExprs::TupleListReader *reader =
				(Multi ? *code_.activeReaderRef_ : code_.reader_);

		if (Nullable && ValueUtils::readCurrentNull(*reader, code_.column_)) {
			return ResultType(ValueType(), true);
		}

		return ResultType(
				ValueUtils::readCurrentValue<T>(*reader, code_.column_),
				false);
	}

private:
	explicit ReaderColumnEvaluator(const CodeType &code) : code_(code) {
	}

	const CodeType &code_;
};

template<typename T, bool Nullable>
class SQLCoreExprs::ReadableTupleColumnEvaluator {
public:
	typedef T ValueTypeTag;

private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename ValueTypeTag::ValueType ValueType;
	typedef SQLExprs::ReadableTuple ReadableTuple;

public:
	typedef ReadableTupleColumnEvaluator EvaluatorType;
	typedef ReadableTupleColumnCode CodeType;
	typedef std::pair<ValueType, bool> ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return EvaluatorType(code);
	}

	static bool check(const ResultType &value) {
		return !Nullable || !value.second;
	}

	static const ValueType& get(const ResultType &value) {
		return value.first;
	}

	ResultType eval(ExprContext &cxt) const {
		const ReadableTuple *tuple = code_.tuple_;
		if (Nullable && ValueUtils::readNull(*tuple, code_.column_)) {
			return ResultType(ValueType(), true);
		}
		return ResultType(
				ValueUtils::readValue<T>(*tuple, code_.column_),
				false);
	}

private:
	explicit ReadableTupleColumnEvaluator(const CodeType &code) : code_(code) {
	}

	const CodeType &code_;
};

template<typename T, bool Nullable>
class SQLCoreExprs::SummaryColumnEvaluator {
public:
	typedef T ValueTypeTag;

private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename ValueTypeTag::ValueType ValueType;

public:
	typedef SummaryColumnEvaluator EvaluatorType;
	typedef SummaryColumnCode CodeType;
	typedef std::pair<ValueType, bool> ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return EvaluatorType(code);
	}

	static bool check(const ResultType &value) {
		return !Nullable || !value.second;
	}

	static const ValueType& get(const ResultType &value) {
		return value.first;
	}

	ResultType eval(ExprContext &cxt) const {
		static_cast<void>(cxt);
		const SQLExprs::SummaryTuple *tuple = code_.tuple_;
		if (Nullable && tuple->isNull(*code_.column_)) {
			return ResultType(ValueType(), true);
		}
		return ResultType(tuple->getValueAs<T>(*code_.column_), false);
	}

private:
	explicit SummaryColumnEvaluator(const CodeType &code) : code_(code) {
	}

	const CodeType &code_;
};

template<typename T>
class SQLCoreExprs::ConstEvaluator {
public:
	typedef T ValueTypeTag;

private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::ValueType ValueType;

public:
	typedef ConstEvaluator EvaluatorType;
	typedef ConstCode CodeType;
	typedef TupleValue ResultType;

	static ConstEvaluator evaluator(const CodeType &code) {
		return ConstEvaluator(code);
	}

	static bool check(const ResultType&) {
		return true;
	}

	static ValueType get(const ResultType &value) {
		return ValueUtils::getValue<ValueType>(value);
	}

	const ResultType& eval(ExprContext&) const {
		return code_.value_;
	}

private:
	explicit ConstEvaluator(const CodeType &code) : code_(code) {
	}

	const CodeType &code_;
};

class SQLCoreExprs::IdExpression {
public:
	enum {
		COUNT_CONTAINER_SOURCE = VariantTypes::COUNT_CONTAINER_SOURCE,

		COUNT_NON_CONTAINER = 2,
		COUNT_CONTAINER = COUNT_CONTAINER_SOURCE,

		OFFSET_NON_CONTAINER = 0,
		OFFSET_CONTAINER = COUNT_NON_CONTAINER,
		OFFSET_END = OFFSET_CONTAINER + COUNT_CONTAINER
	};

	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = OFFSET_END,

			VARIANT_BEGIN = 0,
			VARIANT_END = OFFSET_CONTAINER
		};

		static size_t resolveVariant(ExprFactoryContext &cxt, const ExprCode &code);
	};

	class VariantBase : public SQLExprs::Expression {
	public:
		virtual ~VariantBase() = 0;
		void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	protected:
		VariantBase();

		uint32_t input_;
	};

	template<size_t V>
	class VariantAt : public VariantBase {
	public:
		typedef VariantAt VariantType;
		virtual TupleValue eval(ExprContext &cxt) const;

	private:
		typedef typename util::BoolType<
				(V - OFFSET_NON_CONTAINER != 0)>::Result Grouping;
	};
};

class SQLCoreExprs::ConstantExpression : public SQLExprs::Expression {
public:
	ConstantExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;

private:
	TupleValue value_;
};

class SQLCoreExprs::ColumnExpression {
public:
	enum {
		COUNT_FULL_TYPE = VariantTypes::COUNT_FULL_TYPE,
		COUNT_NULLABLE = VariantTypes::COUNT_NULLABLE,
		COUNT_NON_CONTAINER_SOURCE = 3,
		COUNT_CONTAINER_SOURCE = VariantTypes::COUNT_CONTAINER_SOURCE,

		COUNT_BASE = COUNT_FULL_TYPE * COUNT_NULLABLE,
		COUNT_NON_CONTAINER = 1 + COUNT_BASE * COUNT_NON_CONTAINER_SOURCE,
		COUNT_CONTAINER = 1 + COUNT_BASE * COUNT_CONTAINER_SOURCE,

		OFFSET_NON_CONTAINER = 0,
		OFFSET_CONTAINER = OFFSET_NON_CONTAINER + COUNT_NON_CONTAINER,
		OFFSET_END = OFFSET_CONTAINER + COUNT_CONTAINER
	};

	template<size_t V, bool = (V <= 0)>
	struct VariantTraitsBase {
		typedef SQLValues::Types::Any ValueTypeTag;

		typedef GeneralColumnEvaluator Evaluator;
		typedef typename Evaluator::CodeType Code;
		typedef util::TrueType Checking;
	};

	template<size_t V>
	struct VariantTraitsBase<V, false> {
		enum {
			SUB_OFFSET = V - 1,
			SUB_SOURCE = (SUB_OFFSET / COUNT_BASE),
			SUB_NULLABLE = ((SUB_OFFSET / COUNT_FULL_TYPE) % COUNT_NULLABLE),
			SUB_TYPE = (SUB_OFFSET % COUNT_FULL_TYPE)
		};

		typedef typename VariantTypes::ColumnTypeByFull<
				SUB_TYPE>::ValueTypeTag ValueTypeTag;

		typedef
				typename util::Conditional<
						(SUB_SOURCE == 0),
						ReaderColumnEvaluator<
								ValueTypeTag, SUB_NULLABLE, false>,
				typename util::Conditional<
						(SUB_SOURCE == 1),
						ReaderColumnEvaluator<
								ValueTypeTag, SUB_NULLABLE, true>,
				typename util::Conditional<
						(SUB_SOURCE == 2),
						SummaryColumnEvaluator<
								ValueTypeTag, SUB_NULLABLE>,
				void>::Type>::Type>::Type Evaluator;

		typedef typename Evaluator::CodeType Code;
		typedef util::FalseType Checking;
	};

	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = OFFSET_END,

			VARIANT_BEGIN = 0,
			VARIANT_END = OFFSET_CONTAINER
		};

		static size_t resolveVariant(ExprFactoryContext &cxt, const ExprCode &code);
	};

	struct ColumnValueChecker {
		TupleValue operator()(
				const SQLExprs::Expression &expr, ExprContext &cxt,
				const TupleValue &src, const util::TrueType&) const {
			return expr.asColumnValue(&cxt.getValueContext(), src);
		}

		TupleValue operator()(
				const SQLExprs::Expression&, ExprContext&,
				const TupleValue&, const util::FalseType&) const {
			assert(false);
			return TupleValue();
		}
	};

	template<typename Code, typename Checking>
	class VariantBase : public SQLExprs::Expression {
	public:
		virtual ~VariantBase() = 0;
		void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	protected:
		TupleValue toResult(
				ExprContext &cxt, const TupleValue &value = TupleValue(),
				const SQLValues::Types::Any& = SQLValues::Types::Any()) const {
			if (Checking::VALUE) {
				return ColumnValueChecker()(*this, cxt, value, Checking());
			}
			return value;
		}

		template<typename T>
		TupleValue toResult(
				ExprContext &cxt,
				const std::pair<typename T::ValueType, bool> &value,
				const T&) const {
			const TupleValue &anyValue =
					SQLValues::ValueUtils::toAnyByValue<
							T::COLUMN_TYPE>(value.first);
			if (Checking::VALUE) {
				return ColumnValueChecker()(*this, cxt, anyValue, Checking());
			}
			return anyValue;
		}

		const Code& getVariantCode() const { return code_; }

	private:
		Code code_;
	};

	template<size_t V, typename Base>
	class VariantBaseAt : public VariantBase<
			typename Base::template VariantTraitsBase<V>::Code,
			typename Base::template VariantTraitsBase<V>::Checking> {
	public:
		virtual TupleValue eval(ExprContext &cxt) const;

	private:
		typedef typename Base::template VariantTraitsBase<V> BaseType;

		typedef typename BaseType::ValueTypeTag ValueTypeTag;
		typedef typename BaseType::Evaluator Evaluator;
		typedef typename Evaluator::ResultType ResultType;
	};

	template<size_t V>
	struct VariantAt {
		typedef VariantBaseAt<V, ColumnExpression> VariantType;
	};
};

class SQLCoreExprs::ComparisonExpressionBase {
public:
	enum {
		COUNT_COMP_TYPE = VariantTypes::COUNT_COMP_TYPE,
		COUNT_NULLABLE = VariantTypes::COUNT_NULLABLE,
		COUNT_NUMERIC = VariantTypes::COUNT_NUMERIC,
		COUNT_CONTAINER_SOURCE = VariantTypes::COUNT_CONTAINER_SOURCE,

		COUNT_BASIC_AND_PROMO_TYPE = VariantTypes::COUNT_BASIC_AND_PROMO_TYPE,

		COUNT_NUMERIC_COMBI = VariantTypes::COUNT_NUMERIC_COMBI,
		COUNT_BASIC_COMBI = VariantTypes::COUNT_BASIC_COMBI,

		COUNT_EXPR = 1 + COUNT_NULLABLE * COUNT_BASIC_COMBI,
		COUNT_CONST = COUNT_NULLABLE * COUNT_BASIC_AND_PROMO_TYPE,
		COUNT_COLUMN = COUNT_NULLABLE * COUNT_BASIC_COMBI,
		COUNT_CONTAINER = COUNT_CONST * COUNT_CONTAINER_SOURCE,

		OFFSET_EXPR = 0,
		OFFSET_CONST = OFFSET_EXPR + COUNT_EXPR,
		OFFSET_COLUMN = OFFSET_CONST + COUNT_CONST,
		OFFSET_CONTAINER = OFFSET_COLUMN + COUNT_COLUMN,
		OFFSET_END = OFFSET_CONTAINER + COUNT_CONTAINER
	};

	template<size_t V>
	struct CategoryOf {
		enum {
			CATEGORY_INDEX = (
					V < OFFSET_CONST ? 0 :
					V < OFFSET_COLUMN ? 1 :
					V < OFFSET_CONTAINER ? 2 : 3)
		};
	};

	template<size_t V, size_t C = CategoryOf<V>::CATEGORY_INDEX>
	struct ArgTypeOf {
	};

	template<size_t V>
	struct ArgTypeOf<V, 0> {
	private:
		enum {
			SUB_OFFSET = V - OFFSET_EXPR,
			SUB_ANY_TYPE = (SUB_OFFSET < 1),

			SUB_TYPE_OFFSET = (SUB_ANY_TYPE ? 0 : SUB_OFFSET - 1),
			SUB_NULLABLE = (SUB_TYPE_OFFSET >= COUNT_BASIC_COMBI ||
					SUB_ANY_TYPE),
			SUB_COMBI = (SUB_TYPE_OFFSET % COUNT_BASIC_COMBI)
		};

		typedef typename util::Conditional<
				SUB_ANY_TYPE,
				SQLValues::Types::Any,
				typename VariantTypes::ColumnTypeByBasicCombi<
						SUB_COMBI, 0>::TypeTag>::Type SourceType1;
		typedef typename util::Conditional<
				SUB_ANY_TYPE,
				SQLValues::Types::Any,
				typename VariantTypes::ColumnTypeByBasicCombi<
						SUB_COMBI, 1>::TypeTag>::Type SourceType2;

	public:
		typedef ArgExprEvaluator<SourceType1, 0, SUB_NULLABLE> Arg1;
		typedef ArgExprEvaluator<SourceType2, 1, SUB_NULLABLE> Arg2;
	};

	template<size_t V>
	struct ArgTypeOf<V, 1> {
	private:
		enum {
			SUB_OFFSET = V - OFFSET_CONST,
			SUB_NULLABLE = (SUB_OFFSET >= COUNT_BASIC_AND_PROMO_TYPE),
			SUB_TYPE = (SUB_OFFSET % COUNT_BASIC_AND_PROMO_TYPE)
		};

		typedef typename VariantTypes::ColumnTypeByBasicAndPromo<
				SUB_TYPE>::ValueTypeTag SourceType;
		typedef typename VariantTypes::ColumnTypeByBasicAndPromo<
				SUB_TYPE>::CompTypeTag CompTypeTag;

	public:
		typedef ReaderColumnEvaluator<SourceType, SUB_NULLABLE, false> Arg1;
		typedef ConstEvaluator<CompTypeTag> Arg2;
	};

	template<size_t V>
	struct ArgTypeOf<V, 2> {
	private:
		enum {
			SUB_OFFSET = V - OFFSET_COLUMN,
			SUB_NULLABLE = (SUB_OFFSET >= COUNT_BASIC_COMBI),
			SUB_COMBI = (SUB_OFFSET % COUNT_BASIC_COMBI)
		};

		typedef typename VariantTypes::ColumnTypeByBasicCombi<
				SUB_COMBI, 0>::TypeTag SourceType1;
		typedef typename VariantTypes::ColumnTypeByBasicCombi<
				SUB_COMBI, 1>::TypeTag SourceType2;

	public:
		typedef ReaderColumnEvaluator<SourceType1, SUB_NULLABLE, false> Arg1;
		typedef ReaderColumnEvaluator<SourceType2, SUB_NULLABLE, false> Arg2;
	};

public:
	template<size_t V>
	struct VariantTraitsBase {
		typedef ArgTypeOf<V> ArgTypes;

		typedef typename ArgTypes::Arg1 Arg1;
		typedef typename ArgTypes::Arg2 Arg2;

		typedef typename Arg1::ValueTypeTag ValueTypeTag1;
		typedef typename Arg2::ValueTypeTag ValueTypeTag2;

		typedef typename Arg1::CodeType Code1;
		typedef typename Arg2::CodeType Code2;

		typedef typename util::BoolType<(V == 0)>::Result Checking;

		typedef typename util::Conditional<
				Checking::VALUE,
				SQLValues::ValueComparator,
				SQLValues::ValueComparator::BasicTypeAt<
						ValueTypeTag1, ValueTypeTag2> >::Type TypedComparator;
	};

	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = OFFSET_END,

			VARIANT_BEGIN = 0,
			VARIANT_END = OFFSET_CONTAINER
		};

		static size_t resolveVariant(ExprFactoryContext &cxt, const ExprCode &code);
	};

	template<typename Code1, typename Code2, typename Checking>
	class VariantBase : public SQLExprs::Expression {
	public:
		virtual ~VariantBase() = 0;
		void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	protected:
		TupleValue toResult() const {
			if (Checking::VALUE) {
				return asColumnValueSpecific(TupleValue());
			}
			return TupleValue();
		}

		TupleValue toResult(bool value) const {
			return SQLValues::ValueUtils::toAnyByBool(value);
		}

		const Code1& getArg1() const { return code1_; }
		const Code2& getArg2() const { return code2_; }

	private:
		Code1 code1_;
		Code2 code2_;
	};

	struct ComparatorGenerator {
		static const bool COMP_SENSITIVE = false;

		template<typename T>
		T generate(
				const SQLExprs::Expression &expr, const util::TrueType&) const {
			const SQLExprs::Expression &arg1 = expr.child();
			const SQLExprs::Expression &arg2 = arg1.next();
			return T(
					SQLValues::ValueAccessor(arg1.getCode().getColumnType()),
					SQLValues::ValueAccessor(arg2.getCode().getColumnType()),
					NULL, COMP_SENSITIVE, true);
		}

		template<typename T>
		T generate(const SQLExprs::Expression&, const util::FalseType&) const {
			return T(COMP_SENSITIVE);
		}
	};

	template<typename Func>
	struct ComparatorPredicate {
		typedef bool result_type;
		typedef int32_t first_argument_type;
		typedef int32_t second_argument_type;
		template<typename T> bool operator()(const T &v1, const T &v2) const {
			return Func()(v1, v2);
		}
	};

	template<typename Func, size_t V, typename Base>
	class VariantBaseAt : public VariantBase<
			typename Base::template VariantTraitsBase<V>::Code1,
			typename Base::template VariantTraitsBase<V>::Code2,
			typename Base::template VariantTraitsBase<V>::Checking> {
	public:
		virtual TupleValue eval(ExprContext &cxt) const;
		bool evalAsBool(ExprContext &cxt) const;

	private:
		typedef VariantBase<
				typename Base::template VariantTraitsBase<V>::Code1,
				typename Base::template VariantTraitsBase<V>::Code2,
				typename Base::template VariantTraitsBase<V>::Checking> BaseExpr;

		typedef typename Base::template VariantTraitsBase<V> TraitsBase;

		typedef typename TraitsBase::Code1 Code1;
		typedef typename TraitsBase::Code2 Code2;

		typedef typename TraitsBase::Arg1 Arg1;
		typedef typename TraitsBase::Arg2 Arg2;

		typedef typename Arg1::ResultType ResultType1;
		typedef typename Arg2::ResultType ResultType2;

		typedef typename TraitsBase::TypedComparator TypedComparator;
		typedef ComparatorPredicate<Func> PredType;

		typename Arg1::EvaluatorType getArg1() const {
			return Arg1::evaluator(BaseExpr::getArg1());
		}
		typename Arg2::EvaluatorType getArg2() const {
			return Arg2::evaluator(BaseExpr::getArg2());
		}

		TypedComparator getComparator() const {
			return ComparatorGenerator().generate<TypedComparator>(
					*this, typename TraitsBase::Checking());
		}

		TupleValue toResult() const { return BaseExpr::toResult(); }

		TupleValue toResult(bool value) const {
			return BaseExpr::toResult(value);
		}
	};
};

template<typename Func>
class SQLCoreExprs::ComparisonExpression {
	typedef ComparisonExpressionBase Base;
public:
	typedef Base::VariantTraits VariantTraits;

	template<size_t V>
	struct VariantAt {
	public:
		typedef typename Base::template VariantBaseAt<Func, V, Base> VariantType;
	};
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

class SQLCoreExprs::InExpression : public SQLExprs::Expression {
private:
	enum {
		COUNT_BASIC_TYPE = VariantTypes::COUNT_BASIC_TYPE,
		COUNT_BASIC_AND_PROMO_TYPE =
				VariantTypes::COUNT_BASIC_AND_PROMO_TYPE
	};
	typedef std::equal_to<int64_t> EqPred;

public:
	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = 1 + COUNT_BASIC_AND_PROMO_TYPE * 2,

			VARIANT_BEGIN = 0,
			VARIANT_END = TOTAL_VARIANT_COUNT
		};

		static size_t resolveVariant(ExprFactoryContext &cxt, const ExprCode &code);
	};

	class VariantBase : public SQLExprs::Expression {
	public:
		virtual ~VariantBase() = 0;
		void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	protected:
		VariantBase();

		typedef SQLValues::DigestHasher MapHasher;
		typedef SQLAlgorithmUtils::HashMultiMap<
				int64_t, TupleValue, MapHasher, EqPred> Map;
		typedef Map::iterator MapIterator;

		static const bool COMP_SENSITIVE =
				ComparisonExpressionBase::ComparatorGenerator::COMP_SENSITIVE;

		util::LocalUniquePtr<Map> map_;

		SQLValues::ValueAccessor accessor_;
		SQLValues::ValueDigester digester_;

		TupleValue unmatchResult_;
	};

	template<size_t V>
	class VariantAt : public VariantBase {
	public:
		typedef VariantAt VariantType;
		virtual TupleValue eval(ExprContext &cxt) const;

	private:
		enum {
			SUB_ANY = (V == 0),
			SUB_TYPED_OFFSET = (SUB_ANY ? 1 : V) - 1,
			SUB_TYPE = SUB_TYPED_OFFSET % COUNT_BASIC_AND_PROMO_TYPE,
			SUB_NULLABLE = (SUB_TYPED_OFFSET >= COUNT_BASIC_AND_PROMO_TYPE)
		};

		typedef typename VariantTypes::ColumnTypeByBasicAndPromo<
				SUB_TYPE> BasicTraits;

		typedef typename util::Conditional<
				SUB_ANY, SQLValues::Types::Any,
				typename BasicTraits::ValueTypeTag>::Type SrcTypeTag;
		typedef typename util::Conditional<
				SUB_ANY, SQLValues::Types::Any,
				typename BasicTraits::CompTypeTag>::Type CompTypeTag;

		typedef typename SrcTypeTag::ValueType SrcType;
		typedef typename CompTypeTag::ValueType CompType;
		typedef typename CompTypeTag::LocalValueType LocalCompType;

		typedef typename SQLValues::ValueComparator::template BasicTypeAt<
				CompTypeTag, CompTypeTag> Comparator;

		typedef typename SQLValues::ValueAccessor::ByLocalValue<
				CompTypeTag> AccessorType;
		typedef typename SQLValues::ValueDigester::VariantTraits<
				false, false, AccessorType> DigesterVariantTraits;
		typedef typename SQLValues::TypeSwitcher::template TypeTraits<
				CompTypeTag, void, util::FalseType,
				DigesterVariantTraits> DigesterTraits;
		typedef typename SQLValues::ValueDigester::template TypeAt<
				DigesterTraits>::TypedOp Digester;
	};
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

class SQLCoreExprs::RangeKeyExpression : public SQLExprs::Expression {
public:
	RangeKeyExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;

private:
	bool forPrev_;
	bool forNext_;
};

class SQLCoreExprs::LinearExpression : public SQLExprs::Expression {
public:
	LinearExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;

private:
	typedef TupleValue (LinearExpression::*InterpolationFunc)(
			ExprContext&, const TupleValue&, const TupleValue&,
			const TupleValue&, const TupleValue&, const TupleValue&) const;

	static InterpolationFunc resolveInterpolationFunction(
			TupleColumnType type);

	SQLValues::DateTimeElements interpolateDateTime(
			const TupleValue &x1, const TupleValue &y1, const TupleValue &x2,
			const TupleValue &y2, const TupleValue &x) const;

	TupleValue interpolateLong(
			ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
			const TupleValue &x2, const TupleValue &y2,
			const TupleValue &x) const;
	TupleValue interpolateDouble(
			ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
			const TupleValue &x2, const TupleValue &y2,
			const TupleValue &x) const;
	TupleValue interpolateMicroTimestamp(
			ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
			const TupleValue &x2, const TupleValue &y2,
			const TupleValue &x) const;
	TupleValue interpolateNanoTimestamp(
			ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
			const TupleValue &x2, const TupleValue &y2,
			const TupleValue &x) const;
	TupleValue interpolateNonNumeric(
			ExprContext &cxt, const TupleValue &x1, const TupleValue &y1,
			const TupleValue &x2, const TupleValue &y2,
			const TupleValue &x) const;

	TupleValue convertToResultValue(
			ExprContext &cxt, const TupleValue &src) const;

	InterpolationFunc interpolationFunc_;
};

class SQLCoreExprs::RangeGroupIdExpression : public SQLExprs::Expression {
public:
	virtual TupleValue eval(ExprContext &cxt) const;
};

struct SQLCoreExprs::Functions {
	typedef SQLExprs::ExprUtils::FunctorPolicy::DefaultPolicy DefaultPolicy;

	struct RangeGroupId {
		template<typename C>
		TupleValue operator()(
				C &cxt, const TupleValue &value, const TupleValue &interval,
				const TupleValue &offset);
	};

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
		bool operator()(int32_t comp);
		template<typename T> bool operator()(const T &v1, const T &v2) const { return v1 < v2; }
	};

	struct Le {
		bool operator()(int32_t comp);
		template<typename T> bool operator()(const T &v1, const T &v2) const { return v1 <= v2; }
	};

	struct Gt {
		bool operator()(int32_t comp);
		template<typename T> bool operator()(const T &v1, const T &v2) const { return v1 > v2; }
	};

	struct Ge {
		bool operator()(int32_t comp);
		template<typename T> bool operator()(const T &v1, const T &v2) const { return v1 >= v2; }
	};

	struct Eq {
		bool operator()(int32_t comp);
		template<typename T> bool operator()(const T &v1, const T &v2) const { return v1 == v2; }
	};

	struct Ne {
		bool operator()(int32_t comp);
		template<typename T> bool operator()(const T &v1, const T &v2) const { return v1 != v2; }
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



template<typename Code, typename Checking>
SQLCoreExprs::ColumnExpression::VariantBase<Code, Checking>::~VariantBase() {
}

template<typename Code, typename Checking>
void SQLCoreExprs::ColumnExpression::VariantBase<
		Code, Checking>::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(code);
	code_.initialize(cxt, this, NULL);
}

template<size_t V, typename Base>
TupleValue SQLCoreExprs::ColumnExpression::VariantBaseAt<V, Base>::eval(
		ExprContext &cxt) const {
	const ResultType &value =
			Evaluator::evaluator(this->getVariantCode()).eval(cxt);

	if (!Evaluator::check(value)) {
		return this->toResult(cxt);
	}

	return this->toResult(cxt, value, ValueTypeTag());
}


template<typename Code1, typename Code2, typename Checking>
SQLCoreExprs::ComparisonExpressionBase::VariantBase<
		Code1, Code2, Checking>::~VariantBase() {
}

template<typename Code1, typename Code2, typename Checking>
void SQLCoreExprs::ComparisonExpressionBase::VariantBase<
		Code1, Code2, Checking>::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(code);
	{
		const size_t argIndex = 0;
		code1_.initialize(cxt, this, &argIndex);
	}
	{
		const size_t argIndex = 1;
		code2_.initialize(cxt, this, &argIndex);
	}
}

template<typename Func, size_t V, typename Base>
TupleValue SQLCoreExprs::ComparisonExpressionBase::VariantBaseAt<
		Func, V, Base>::eval(ExprContext &cxt) const {
	const ResultType1 &v1 = getArg1().eval(cxt);
	if (!Arg1::check(v1)) {
		return toResult();
	}

	const ResultType2 &v2 = getArg2().eval(cxt);
	if (!Arg2::check(v2)) {
		return toResult();
	}

	return toResult(getComparator()(Arg1::get(v1), Arg2::get(v2), PredType()));
}

template<typename Func, size_t V, typename Base>
inline bool SQLCoreExprs::ComparisonExpressionBase::VariantBaseAt<
		Func, V, Base>::evalAsBool(ExprContext &cxt) const {
	const ResultType1 &v1 = getArg1().eval(cxt);
	if (!Arg1::check(v1)) {
		return false;
	}

	const ResultType2 &v2 = getArg2().eval(cxt);
	if (!Arg2::check(v2)) {
		return false;
	}

	return getComparator()(Arg1::get(v1), Arg2::get(v2), PredType());
}


inline bool SQLCoreExprs::Functions::Lt::operator()(int32_t comp) {
	return comp < 0;
}


inline bool SQLCoreExprs::Functions::Le::operator()(int32_t comp) {
	return comp <= 0;
}


inline bool SQLCoreExprs::Functions::Gt::operator()(int32_t comp) {
	return comp > 0;
}


inline bool SQLCoreExprs::Functions::Ge::operator()(int32_t comp) {
	return comp >= 0;
}


inline bool SQLCoreExprs::Functions::Eq::operator()(int32_t comp) {
	return comp == 0;
}


inline bool SQLCoreExprs::Functions::Ne::operator()(int32_t comp) {
	return comp != 0;
}

#endif
