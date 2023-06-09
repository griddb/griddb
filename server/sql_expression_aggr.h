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

#ifndef SQL_EXPRESSION_AGGR_H_
#define SQL_EXPRESSION_AGGR_H_

#include "query_function.h"
#include "sql_expression_base.h"

struct SQLAggrExprs {
	typedef SQLExprs::ExprType ExprType;

	class Registrar;
	class UnsupportedRegistrar;
	struct Specs;
	struct Functions;
};

class SQLAggrExprs::Registrar :
		public SQLExprs::AggregationExprRegistrar<SQLAggrExprs::Specs> {
public:
	virtual void operator()() const;

private:
	static const SQLExprs::ExprRegistrar REGISTRAR_INSTANCE;
};

struct SQLAggrExprs::Specs {
	typedef SQLExprs::ExprSpec ExprSpec;
	typedef SQLExprs::ExprSpecBase Base;
	template<ExprType, int = 0> struct Spec;

	typedef FunctionUtils AggregationValuesBase;

	typedef Base::InList<
			Base::In<TupleTypes::TYPE_DOUBLE>,
			Base::In<TupleTypes::TYPE_DOUBLE>,
			Base::In<TupleTypes::TYPE_LONG> > StddevBaseAggrList;

	template<int C> struct Spec<SQLType::AGG_AVG, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_WINDOW, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE>,
				Base::In<TupleTypes::TYPE_LONG> >,
				SQLType::AGG_DISTINCT_AVG> Type;
	};

	template<int C> struct Spec<SQLType::AGG_COUNT_ALL, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<>,
				ExprSpec::FLAG_NON_NULLABLE |
				ExprSpec::FLAG_WINDOW, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_COUNT_COLUMN, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_NON_NULLABLE |
				ExprSpec::FLAG_WINDOW, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> >,
				SQLType::AGG_DISTINCT_COUNT_COLUMN> Type;
	};

	template<int C> struct Spec<SQLType::AGG_GROUP_CONCAT, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_ANY>,
				Base::In<TupleTypes::TYPE_STRING> >,
				0, Base::InList<
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_NULLABLE>,
				Base::In<TupleTypes::TYPE_BLOB, ExprSpec::FLAG_NULLABLE>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_NULLABLE> >,
				SQLType::AGG_DISTINCT_GROUP_CONCAT> Type;
	};

	template<int C> struct Spec<SQLType::AGG_LAG, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				Base::In<TupleTypes::TYPE_LONG,
						ExprSpec::FLAG_WINDOW_POS_BEFORE |
						ExprSpec::FLAG_OPTIONAL>,
				Base::In<TupleTypes::TYPE_ANY,
						ExprSpec::FLAG_PROMOTABLE | ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_WINDOW_ONLY |
				ExprSpec::FLAG_WINDOW_COLUMN, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_LEAD, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableAnyIn,
				Base::In<TupleTypes::TYPE_LONG,
						ExprSpec::FLAG_WINDOW_POS_AFTER |
						ExprSpec::FLAG_OPTIONAL>,
				Base::In<TupleTypes::TYPE_ANY,
						ExprSpec::FLAG_PROMOTABLE | ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_WINDOW_ONLY |
				ExprSpec::FLAG_WINDOW_COLUMN, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_MAX, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_WINDOW |
				ExprSpec::FLAG_COMP_SENSITIVE, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				SQLType::AGG_MAX> Type;
	};

	template<int C> struct Spec<SQLType::AGG_MEDIAN, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_PSEUDO_WINDOW |
				ExprSpec::FLAG_WINDOW_COLUMN |
				ExprSpec::FLAG_WINDOW_VALUE_COUNTING, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC>,
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_MIN, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_WINDOW |
				ExprSpec::FLAG_COMP_SENSITIVE, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				SQLType::AGG_MIN> Type;
	};

	template<int C> struct Spec<SQLType::AGG_PERCENTILE_CONT, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC>,
				Base::In<TupleTypes::TYPE_DOUBLE, ExprSpec::FLAG_EXACT> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_PSEUDO_WINDOW |
				ExprSpec::FLAG_WINDOW_COLUMN |
				ExprSpec::FLAG_AGGR_ORDERING |
				ExprSpec::FLAG_WINDOW_VALUE_COUNTING, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC>,
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_ROW_NUMBER, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<>,
				ExprSpec::FLAG_NON_NULLABLE |
				ExprSpec::FLAG_WINDOW_ONLY, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_STDDEV0, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_WINDOW, StddevBaseAggrList,
				SQLType::AGG_DISTINCT_STDDEV0> Type;
	};

	template<int C> struct Spec<SQLType::AGG_STDDEV_POP, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_WINDOW, StddevBaseAggrList,
				SQLType::AGG_DISTINCT_STDDEV_POP> Type;
	};

	template<int C> struct Spec<SQLType::AGG_STDDEV_SAMP, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_WINDOW, StddevBaseAggrList,
				SQLType::AGG_DISTINCT_STDDEV_SAMP> Type;
	};

	template<int C> struct Spec<SQLType::AGG_SUM, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableNumIn>,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_WINDOW, Base::InList<
				Base::In<TupleTypes::TYPE_NUMERIC, ExprSpec::FLAG_NULLABLE> >,
				SQLType::AGG_DISTINCT_SUM> Type;
	};

	template<int C> struct Spec<SQLType::AGG_TOTAL, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_NON_NULLABLE |
				ExprSpec::FLAG_WINDOW, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				SQLType::AGG_DISTINCT_TOTAL> Type;
	};

	template<int C> struct Spec<SQLType::AGG_VAR_POP, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_WINDOW, StddevBaseAggrList,
				SQLType::AGG_DISTINCT_VAR_POP> Type;
	};

	template<int C> struct Spec<SQLType::AGG_VAR_SAMP, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_WINDOW, StddevBaseAggrList,
				SQLType::AGG_DISTINCT_VAR_SAMP> Type;
	};

	template<int C> struct Spec<SQLType::AGG_VARIANCE0, C> {
		typedef Base::Type<TupleTypes::TYPE_DOUBLE, Base::InList<
				Base::In<TupleTypes::TYPE_DOUBLE> >,
				ExprSpec::FLAG_WINDOW, StddevBaseAggrList,
				SQLType::AGG_DISTINCT_VARIANCE0> Type;
	};

	template<int C> struct Spec<SQLType::AGG_FIRST, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_INTERNAL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_LAST, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT1 |
				ExprSpec::FLAG_INTERNAL, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_FOLD_EXISTS, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> >,
				ExprSpec::FLAG_NON_NULLABLE |
				ExprSpec::FLAG_INTERNAL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_FOLD_NOT_EXISTS, C> {
		typedef typename Spec<SQLType::AGG_FOLD_EXISTS, C>::Type Type;
	};

	template<int C> struct Spec<SQLType::AGG_FOLD_IN, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				typename Base::PromotableAnyIn,
				typename Base::PromotableAnyIn>,
				ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_INTERNAL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::AGG_FOLD_NOT_IN, C> {
		typedef typename Spec<SQLType::AGG_FOLD_IN, C>::Type Type;
	};

	template<int C> struct Spec<SQLType::AGG_FOLD_UPTO_ONE, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_INHERIT2 |
				ExprSpec::FLAG_NULLABLE |
				ExprSpec::FLAG_INTERNAL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_ANY> > > Type;
	};
};

struct SQLAggrExprs::Functions {
	typedef SQLExprs::ExprUtils::FunctorPolicy::DefaultPolicy DefaultPolicy;

	struct Avg;
	struct CountBase;
	struct CountAll;
	struct CountColumn;
	struct GroupConcat;
	struct LagLead;
	struct Max;
	struct Median;
	struct Min;
	struct PercentileCont;
	struct RowNumber;
	struct StddevBase;
	struct Stddev0;
	struct StddevPop;
	struct StddevSamp;
	struct Sum;
	struct Total;
	struct VarPop;
	struct VarSamp;
	struct Variance0;

	struct First;
	struct Last;
	struct FoldExists;
	struct FoldNotExists;
	struct FoldIn;
	struct FoldNotIn;
	struct FoldUptoOne;
};

struct SQLAggrExprs::Functions::Avg {
	typedef FunctionUtils::AggregationValues<double, int64_t> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, double v);
	};
	struct Merge {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, double v1, int64_t v2);
	};
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::CountBase {
	typedef FunctionUtils::AggregationValues<int64_t> Aggr;
	struct Merge {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, int64_t v);
	};
};

struct SQLAggrExprs::Functions::CountAll {
	typedef CountBase::Aggr Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr);
	};
	typedef CountBase::Merge Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::CountColumn {
	typedef CountBase::Aggr Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	typedef CountBase::Merge Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::GroupConcat {
	typedef FunctionUtils::AggregationValues<
			std::pair<TupleString, bool>, 
			std::pair<TupleValue, bool>,
			std::pair<TupleString, bool> > Aggr;

	struct Advance {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		Advance();

		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);

		bool forSeparator_;
	};
	struct Merge {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		template<typename C, typename R1, typename R2>
		void operator()(C &cxt, const Aggr &aggr, R1 &v1, R2 *v2);
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	struct Finish {
		template<typename C>
		typename C::WriterType* operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::LagLead {
	typedef FunctionUtils::AggregationValues<TupleValue> Aggr;

	struct Advance {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		Advance();

		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr);

		bool positioning_;
	};

	typedef void Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::Max {
	typedef FunctionUtils::AggregationValues<TupleValue> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	typedef Advance Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::Median {
	typedef FunctionUtils::AggregationValues<
			std::pair<int64_t, bool>, int64_t> LongAggr;
	typedef FunctionUtils::AggregationValues<
			std::pair<double, bool>, int64_t> DoubleAggr;

	enum MedianAction {
		ACTION_NONE,
		ACTION_SET,
		ACTION_MERGE
	};

	struct Advance {
		template<typename C, typename Aggr, typename V>
		void operator()(C &cxt, const Aggr &aggr, const V &v);

		template<typename C, typename Aggr>
		static MedianAction nextAction(C &cxt, const Aggr &aggr);
		template<typename C>
		static void errorUnordered(C &cxt);
	};

	typedef void Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::Min {
	typedef FunctionUtils::AggregationValues<TupleValue> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	typedef Advance Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::PercentileCont {
	typedef FunctionUtils::AggregationValues<
			std::pair<int64_t, bool>, int64_t> LongAggr;
	typedef FunctionUtils::AggregationValues<
			std::pair<double, bool>, int64_t> DoubleAggr;

	enum PercentileAction {
		ACTION_NONE,
		ACTION_SET,
		ACTION_MERGE
	};

	struct Advance {
		template<typename C, typename Aggr, typename V>
		void operator()(C &cxt, const Aggr &aggr, const V &v, double rate);

		template<typename C, typename Aggr>
		static PercentileAction nextAction(
				C &cxt, const Aggr &aggr, double rate, int64_t &mergePos);
	};

	typedef void Merge;
	typedef void Finish;

	struct Checker {
		template<typename C, typename Aggr, typename V>
		void operator()(C &cxt, const Aggr&, const V&, double rate);
	};
};

struct SQLAggrExprs::Functions::RowNumber {
	typedef CountAll::Advance Advance;
	typedef void Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::StddevBase {
	typedef FunctionUtils::AggregationValues<double, double, int64_t> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, double v);
	};
	struct Merge {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, double v1, double v2, int64_t v3);
	};
};

struct SQLAggrExprs::Functions::Stddev0 {
	typedef StddevBase::Aggr Aggr;
	typedef StddevBase::Advance Advance;
	typedef StddevBase::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::StddevPop {
	typedef StddevBase::Aggr Aggr;
	typedef StddevBase::Advance Advance;
	typedef StddevBase::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::StddevSamp {
	typedef StddevBase::Aggr Aggr;
	typedef StddevBase::Advance Advance;
	typedef StddevBase::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::Sum {
	typedef FunctionUtils::AggregationValues<
			std::pair<int64_t, bool> > LongAggr;
	typedef FunctionUtils::AggregationValues<
			std::pair<double, bool> > DoubleAggr;

	struct Advance {
		template<typename C>
		void operator()(C &cxt, const LongAggr &aggr, int64_t v);
		template<typename C>
		void operator()(C &cxt, const DoubleAggr &aggr, double v);
	};
	typedef Advance Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::Total {
	typedef FunctionUtils::AggregationValues<double> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, double v);
	};
	typedef Advance Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::VarPop {
	typedef StddevBase::Aggr Aggr;
	typedef StddevBase::Advance Advance;
	typedef StddevBase::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::VarSamp {
	typedef StddevBase::Aggr Aggr;
	typedef StddevBase::Advance Advance;
	typedef StddevBase::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::Variance0 {
	typedef StddevBase::Aggr Aggr;
	typedef StddevBase::Advance Advance;
	typedef StddevBase::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<double, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::First {
	typedef FunctionUtils::AggregationValues<TupleValue> Aggr;
	struct Advance {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		template<typename C>
		void operator()(C &cxt, const Aggr &aggr);
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	typedef Advance Merge;
	struct Finish {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		template<typename C>
		TupleValue operator()(C &cxt, const Aggr &aggr);
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
};

struct SQLAggrExprs::Functions::Last {
	typedef FunctionUtils::AggregationValues<TupleValue> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	typedef Advance Merge;
	typedef void Finish;
};

struct SQLAggrExprs::Functions::FoldExists {
	typedef FunctionUtils::AggregationValues<int64_t> Aggr;
	struct Advance {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, int64_t v);
	};
	struct Merge {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, int64_t v);
	};
	struct Finish {
		template<typename C>
		bool operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::FoldNotExists {
	typedef FoldExists::Aggr Aggr;
	typedef FoldExists::Advance Advance;
	typedef FoldExists::Merge Merge;
	struct Finish {
		template<typename C>
		bool operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::FoldIn {
	typedef FunctionUtils::AggregationValues<int64_t> Aggr;
	struct Advance {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, int64_t v);
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);

		std::pair<TupleValue, bool> leftValue_;
	};
	struct Merge {
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, int64_t v);
	};
	struct Finish {
		template<typename C>
		std::pair<bool, bool> operator()(C &cxt, const Aggr &aggr);
	};
	enum FoldInFlags {
		FOLD_IN_MATCHED = 1 << 0,
		FOLD_IN_NULL_FOUND = 1 << 1,
		FOLD_IN_FOUND = 1 << 2
	};
};

struct SQLAggrExprs::Functions::FoldNotIn {
	typedef FoldIn::Aggr Aggr;
	typedef FoldIn::Advance Advance;
	typedef FoldIn::Merge Merge;
	struct Finish {
		template<typename C>
		std::pair<bool, bool> operator()(C &cxt, const Aggr &aggr);
	};
};

struct SQLAggrExprs::Functions::FoldUptoOne {
	typedef FunctionUtils::AggregationValues<int64_t, TupleValue> Aggr;
	struct Advance {
		typedef DefaultPolicy::AsPartiallyFinishable::
				AsArgsDelayedEvaluable Policy;

		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, int64_t v);
		template<typename C>
		void operator()(C &cxt, const Aggr &aggr, const TupleValue &v);
	};
	struct Merge {
		template<typename C>
		void operator()(
				C &cxt, const Aggr &aggr, int64_t v1, const TupleValue &v2);
	};
	struct Finish {
		template<typename C>
		TupleValue operator()(C &cxt, const Aggr &aggr);
	};
};

#endif
