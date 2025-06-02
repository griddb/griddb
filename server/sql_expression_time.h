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

#ifndef SQL_EXPRESSION_TIME_H_
#define SQL_EXPRESSION_TIME_H_

#include "sql_expression_base.h"

struct SQLTimeExprs {
	typedef SQLExprs::ExprType ExprType;

	class Registrar;
	struct Specs;
	struct Functions;

	struct TimeFunctionUtils;
};

class SQLTimeExprs::Registrar :
		public SQLExprs::BasicExprRegistrar<SQLTimeExprs::Specs> {
public:
	virtual void operator()() const;

private:
	static const SQLExprs::ExprRegistrar REGISTRAR_INSTANCE;
};

struct SQLTimeExprs::Specs {
	typedef SQLExprs::ExprSpec ExprSpec;
	typedef SQLExprs::ExprSpecBase Base;
	template<ExprType, int = 0> struct Spec;

	template<int C> struct Spec<SQLType::FUNC_EXTRACT, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::PromotableTimestampIn,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MAKE_TIMESTAMP, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_NUMERIC>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MAKE_TIMESTAMP_BY_DATE, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INTERNAL> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MAKE_TIMESTAMP_MS, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MAKE_TIMESTAMP_US, C> {
		typedef Base::Type<TupleTypes::TYPE_MICRO_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_MAKE_TIMESTAMP_NS, C> {
		typedef Base::Type<TupleTypes::TYPE_NANO_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_NOW, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_STRFTIME, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::PromotableTimestampIn,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TO_EPOCH_MS, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_TIMESTAMP> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TO_TIMESTAMP_MS, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_MS, C> {
		typedef typename Spec<SQLType::FUNC_TIMESTAMP, C>::Type Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_US, C> {
		typedef Base::Type<TupleTypes::TYPE_MICRO_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_NS, C> {
		typedef Base::Type<TupleTypes::TYPE_NANO_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_TRUNC, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::PromotableTimestampIn,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT2> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_ADD, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::PromotableTimestampIn,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT2> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_DIFF, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::PromotableTimestampIn,
				Base::PromotableTimestampIn,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMPADD, C> {
		typedef typename Spec<SQLType::FUNC_TIMESTAMP_ADD>::Type Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMPDIFF, C> {
		typedef typename Spec<SQLType::FUNC_TIMESTAMP_DIFF>::Type Type;
	};
};

struct SQLTimeExprs::Functions {
	typedef SQLExprs::ExprUtils::FunctorPolicy::DefaultPolicy DefaultPolicy;

	struct Extract {
		struct Checker {
			template<typename C, typename T>
			int64_t operator()(C&, int64_t fieldTypeValue, const T&);
		};

		template<typename C, typename T, typename R>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue, R &zone);

		template<typename C, typename T>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue,
				const util::TimeZone &zone = util::TimeZone());
	};

	struct MakeTimestamp {
	private:
		template<typename T>
		struct FractionalSeconds {
			FractionalSeconds(int64_t second, int64_t fration);

			int64_t second_;
			int64_t fration_;
		};

		template<typename T, int = 0>
		struct SecondsOf {
			enum {
				VALUE_SINGLE = (
						util::IsSame<T, int64_t>::VALUE ||
						util::IsSame<T, double>::VALUE)
			};
			typedef typename util::Conditional<
					VALUE_SINGLE,
							SQLValues::Types::TimestampTag,
							util::FalseType>::Type TagType;
			typedef typename TagType::LocalValueType RetType;
		};

		template<typename U, int C>
		struct SecondsOf<FractionalSeconds<U>, C> {
			typedef U TagType;
			typedef typename TagType::LocalValueType RetType;
		};

	public:
		template<typename T>
		struct Fractional {
			template<typename C, typename R>
			typename T::LocalValueType operator()(
					C &cxt, int64_t year, int64_t month, int64_t day,
					int64_t hour, int64_t min, int64_t second, int64_t fration,
					R &zone);

			template<typename C>
			typename T::LocalValueType operator()(
					C &cxt, int64_t year, int64_t month, int64_t day,
					int64_t hour, int64_t min, int64_t second, int64_t fration,
					const util::TimeZone &zone = util::TimeZone());
		};

		template<typename C, typename Sec, typename R>
		typename SecondsOf<Sec>::RetType operator()(
				C &cxt, int64_t year, int64_t month, int64_t day,
				int64_t hour, int64_t min, const Sec &second, R &zone);

		template<typename C, typename Sec>
		typename SecondsOf<Sec>::RetType operator()(
				C &cxt, int64_t year, int64_t month, int64_t day,
				int64_t hour, int64_t min, const Sec &second,
				const util::TimeZone &zone = util::TimeZone());

		template<typename C, typename R>
		int64_t operator()(
				C &cxt, int64_t year, int64_t month, int64_t day,
				R &zone);

		template<typename C>
		int64_t operator()(
				C &cxt, int64_t year, int64_t month, int64_t day,
				const util::TimeZone &zone = util::TimeZone());

	private:
		template<util::DateTime::FieldType T>
		static void setFieldValue(
				util::DateTime::FieldData &fieldData, int64_t value);

		static std::pair<int64_t, int64_t> decomposeSeconds(
				int64_t value, uint32_t &nanos);
		static std::pair<int64_t, int64_t> decomposeSeconds(
				double value, uint32_t &nanos);

		template<typename T>
		static std::pair<int64_t, int64_t> decomposeSeconds(
				const FractionalSeconds<T> &value, uint32_t &nanos);
	};

	typedef MakeTimestamp::Fractional<
			SQLValues::Types::TimestampTag> MakeTimestampMs;
	typedef MakeTimestamp::Fractional<
			SQLValues::Types::MicroTimestampTag> MakeTimestampUs;
	typedef MakeTimestamp::Fractional<
			SQLValues::Types::NanoTimestampTag> MakeTimestampNs;

	struct Now {
	public:
		template<typename C>
		int64_t operator()(C &cxt);
	};

	struct ToEpochMs {
	public:
		template<typename C>
		int64_t operator()(C &cxt, int64_t tsValue);
	};

	struct ToTimestampMs {
	public:
		template<typename C>
		int64_t operator()(C &cxt, int64_t millis);
	};

	template<typename T>
	struct TimestampFunc {
		typedef DefaultPolicy::AsAllocatable Policy;

		template<typename C, typename R>
		typename T::LocalValueType operator()(C &cxt, R &value, R &zone);

		template<typename C, typename R>
		typename T::LocalValueType operator()(
				C &cxt, R &value,
				const util::TimeZone &zone = util::TimeZone());
	};

	typedef TimestampFunc<SQLValues::Types::TimestampTag> TimestampMsFunc;
	typedef TimestampFunc<SQLValues::Types::MicroTimestampTag> TimestampUsFunc;
	typedef TimestampFunc<SQLValues::Types::NanoTimestampTag> TimestampNsFunc;

	struct TimestampTrunc {
		struct Checker {
			template<typename C, typename T>
			T operator()(C&, int64_t fieldTypeValue, const T &tsValue);
		};

		template<typename C, typename T, typename R>
		T operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue, R &zone);

		template<typename C, typename T>
		T operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue,
				const util::TimeZone &zone = util::TimeZone());
	};

	struct TimestampAdd {
		struct Checker {
			template<typename C, typename T>
			T operator()(C&, int64_t fieldTypeValue, const T &tsValue, int64_t);
		};

		template<typename C, typename T, typename R>
		T operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue,
				int64_t amount, R &zone);

		template<typename C, typename T>
		T operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue,
				int64_t amount, const util::TimeZone &zone = util::TimeZone());
	};

	struct TimestampDiff {
		struct Checker {
			template<typename C, typename T>
			int64_t operator()(C&, int64_t fieldTypeValue, const T&, const T&);
		};

		template<typename C, typename T, typename R>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue1,
				const T &tsValue2, R &zone);

		template<typename C, typename T>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, const T &tsValue1,
				const T &tsValue2, const util::TimeZone &zone = util::TimeZone());
	};
};

struct SQLTimeExprs::TimeFunctionUtils {
	static util::DateTime trunc(
			const util::DateTime &value, util::DateTime::FieldType field,
			const util::DateTime::ZonedOption &option);
	static util::PreciseDateTime trunc(
			const util::PreciseDateTime &value, util::DateTime::FieldType field,
			const util::DateTime::ZonedOption &option);
};

#endif
