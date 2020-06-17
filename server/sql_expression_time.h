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
				Base::In<TupleTypes::TYPE_TIMESTAMP>,
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

	template<int C> struct Spec<SQLType::FUNC_NOW, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<>,
				ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_STRFTIME, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_TIMESTAMP>,
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

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_TRUNC, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::In<TupleTypes::TYPE_TIMESTAMP>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_ADD, C> {
		typedef Base::Type<TupleTypes::TYPE_TIMESTAMP, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::In<TupleTypes::TYPE_TIMESTAMP>,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TIMESTAMP_DIFF, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_EXACT>,
				Base::In<TupleTypes::TYPE_TIMESTAMP>,
				Base::In<TupleTypes::TYPE_TIMESTAMP>,
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
		template<typename C, typename R>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue, R &zone);

		template<typename C>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue,
				const util::TimeZone &zone = util::TimeZone());
	};

	struct MakeTimestamp {
	public:
		template<typename C, typename Sec, typename R>
		int64_t operator()(
				C &cxt, int64_t year, int64_t month, int64_t day,
				int64_t hour, int64_t min, Sec second, R &zone);

		template<typename C, typename Sec>
		int64_t operator()(
				C &cxt, int64_t year, int64_t month, int64_t day,
				int64_t hour, int64_t min, Sec second,
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

		static std::pair<int64_t, int64_t> decomposeSeconds(int64_t value);
		static std::pair<int64_t, int64_t> decomposeSeconds(double value);
	};

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

	struct TimestampFunc {
		typedef DefaultPolicy::AsAllocatable Policy;

		template<typename C, typename R>
		int64_t operator()(C &cxt, R &value, R &zone);

		template<typename C, typename R>
		int64_t operator()(
				C &cxt, R &value,
				const util::TimeZone &zone = util::TimeZone());
	};

	struct TimestampTrunc {
		template<typename C, typename R>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue, R &zone);

		template<typename C>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue,
				const util::TimeZone &zone = util::TimeZone());
	};

	struct TimestampAdd {
		template<typename C, typename R>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue, int64_t amount,
				R &zone);

		template<typename C>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue, int64_t amount,
				const util::TimeZone &zone = util::TimeZone());
	};

	struct TimestampDiff {
		template<typename C, typename R>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue1, int64_t tsValue2,
				R &zone);

		template<typename C>
		int64_t operator()(
				C &cxt, int64_t fieldTypeValue, int64_t tsValue1, int64_t tsValue2,
				const util::TimeZone &zone = util::TimeZone());
	};
};

struct SQLTimeExprs::TimeFunctionUtils {
	static util::DateTime trunc(
			const util::DateTime &value, util::DateTime::FieldType field,
			const util::DateTime::ZonedOption &option);
};

#endif
