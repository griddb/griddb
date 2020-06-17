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

#ifndef SQL_EXPRESSION_STRING_H_
#define SQL_EXPRESSION_STRING_H_

#include "sql_expression_base.h"

struct SQLStringExprs {
	typedef SQLExprs::ExprType ExprType;

	class Registrar;
	struct Specs;
	struct Functions;
};

class SQLStringExprs::Registrar :
		public SQLExprs::BasicExprRegistrar<SQLStringExprs::Specs> {
public:
	virtual void operator()() const;

private:
	static const SQLExprs::ExprRegistrar REGISTRAR_INSTANCE;
};

struct SQLStringExprs::Specs {
	typedef SQLExprs::ExprSpec ExprSpec;
	typedef SQLExprs::ExprSpecBase Base;
	template<ExprType, int = 0> struct Spec;

	template<int C> struct Spec<SQLType::FUNC_CHAR, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_REPEAT1 | ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_GLOB, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_HEX, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				typename Base::PromotableStringOrBlobIn>,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_INSTR, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				typename Base::PromotableStringOrBlobIn,
				typename Base::PromotableStringOrBlobIn,
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL>,
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_LENGTH, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				typename Base::PromotableStringOrBlobIn> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_LIKE, C> {
		typedef Base::Type<TupleTypes::TYPE_BOOL, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_LOWER, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_LTRIM, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_PRINTF, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_ANY, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT_NULLABLE1 |
				ExprSpec::FLAG_REPEAT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_QUOTE, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_ANY> >,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_RANDOMBLOB, C> {
		typedef Base::Type<TupleTypes::TYPE_BLOB, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> >,
				ExprSpec::FLAG_NON_NULLABLE | ExprSpec::FLAG_DYNAMIC> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_REPLACE, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_RTRIM, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING,
				ExprSpec::FLAG_OPTIONAL> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_SUBSTR, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableStringOrBlobIn,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT1> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_SUBSTR_WITH_BOUNDS, C> {
		typedef Base::Type<TupleTypes::TYPE_NULL, Base::InList<
				typename Base::PromotableStringOrBlobIn,
				Base::In<TupleTypes::TYPE_LONG>,
				Base::In<TupleTypes::TYPE_LONG, ExprSpec::FLAG_OPTIONAL> >,
				ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_INTERNAL> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TRANSLATE, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_TRIM, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING>,
				Base::In<TupleTypes::TYPE_STRING, ExprSpec::FLAG_OPTIONAL>
				> > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_UNICODE, C> {
		typedef Base::Type<TupleTypes::TYPE_LONG, Base::InList<
				Base::In<TupleTypes::TYPE_STRING> >,
				ExprSpec::FLAG_NULLABLE> Type;
	};

	template<int C> struct Spec<SQLType::FUNC_UPPER, C> {
		typedef Base::Type<TupleTypes::TYPE_STRING, Base::InList<
				Base::In<TupleTypes::TYPE_STRING> > > Type;
	};

	template<int C> struct Spec<SQLType::FUNC_ZEROBLOB, C> {
		typedef Base::Type<TupleTypes::TYPE_BLOB, Base::InList<
				Base::In<TupleTypes::TYPE_LONG> >,
				ExprSpec::FLAG_NON_NULLABLE> Type;
	};
};

struct SQLStringExprs::Functions {
	typedef SQLExprs::ExprUtils::FunctorPolicy::DefaultPolicy DefaultPolicy;

	struct Char {
		typedef DefaultPolicy::AsPartiallyFinishable Policy;

		template<typename C>
		typename C::WriterType& operator()(C &cxt);

		template<typename C>
		typename C::WriterType& operator()(C &cxt, const int64_t *value);
	};

	struct Glob {
	public:
		typedef DefaultPolicy::AsAllocatable Policy;

		template<typename C, typename R>
		bool operator()(C &cxt, R &value1, R &value2);

	private:
		typedef util::BasicString<
				char8_t, std::char_traits<char8_t>,
				util::StdAllocator<char8_t, void> > String;
	};

	struct Hex {
		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R *value);
	};

	struct Instr {
		template<typename C, typename R>
		int64_t operator()(
				C &cxt, R &target, R &key,
				int64_t offset = 1, int64_t repeat = 1);
	};

	struct Length {
		template<typename C, typename R>
		int64_t operator()(C &cxt, R &value);
	};

	struct Like {
		template<typename C, typename R>
		bool operator()(C &cxt, R &value1, R &value2);

		template<typename C, typename R>
		bool operator()(C &cxt, R &value1, R &value2, R &reader3);
	};

	struct Lower {
		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader);
	};

	struct Ltrim {
		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader1);

		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader1, R &reader2);
	};

	struct Printf {
	public:
		typedef DefaultPolicy::AsAllocatable::AsPartiallyFinishable Policy;

		Printf();
		~Printf();

		template<typename C>
		typename C::WriterType* operator()(C &cxt);

		template<typename C, typename R>
		typename C::WriterType* operator()(C &cxt, R *format);

		template<typename C>
		typename C::WriterType* operator()(C &cxt, const TupleValue &argValue);

	private:
		typedef util::Vector<
				TupleValue, util::StdAllocator<TupleValue, void> > ArgList;
		typedef util::BasicString<
				char8_t, std::char_traits<char8_t>,
				util::StdAllocator<char8_t, void> > String;

		util::StdAllocator<void, void> alloc_;
		String *format_;
		ArgList *argList_;
	};

	struct Quote {
		template<typename C>
		typename C::WriterType& operator()(C &cxt, const TupleValue &value);
	};

	struct RandomBlob {
		typedef DefaultPolicy::AsWriterInitializable Policy;

		template<typename C>
		typename C::WriterType& operator()(C &cxt, const int64_t *value);
	};

	struct Replace {
		template<typename C, typename R>
		typename C::WriterType& operator()(
				C &cxt, R &reader1, R &reader2, R &reader3);
	};

	struct Rtrim {
		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader1);

		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader1, R &reader2);
	};

	template<typename Bounded>
	struct Substr {
		template<typename C, typename R>
		typename C::WriterType& operator()(
				C &cxt, R &value1, int64_t value2, int64_t value3);

		template<typename C, typename R>
		typename C::WriterType& operator()(
				C &cxt, R &value1, int64_t value2,
				const int64_t *value3 = NULL);
	};
	typedef Substr<util::TrueType> SubstrWithBounds;
	typedef Substr<util::FalseType> SubstrWithoutBounds;

	struct Trim {
		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader1);

		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader1, R &reader2);
	};

	struct Unicode {
		template<typename C, typename R>
		std::pair<int64_t, bool> operator()(C &cxt, R &value);
	};

	struct Upper {
		template<typename C, typename R>
		typename C::WriterType& operator()(C &cxt, R &reader);
	};

	struct ZeroBlob {
		template<typename C>
		typename C::WriterType& operator()(C &cxt, const int64_t *value);
	};

};

#endif
