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
#ifndef SQL_EXPRESSION_BASE_H_
#define SQL_EXPRESSION_BASE_H_

#include "sql_expression.h"

class SQLExprs::DefaultExprFactory : public SQLExprs::ExprFactory {
public:
	static const ExprFactory& getFactory();
	static DefaultExprFactory& getFactoryForRegistrar();

	virtual Expression& create(
			ExprFactoryContext &cxt, const ExprCode &code) const;
	virtual const ExprSpec& getSpec(ExprType type) const;

	void add(ExprType type, const ExprSpec &spec, FactoryFunc func);

private:
	struct Entry {
		Entry();

		ExprType type_;
		ExprSpec spec_;
		FactoryFunc func_;
	};

	DefaultExprFactory(Entry *entryList, size_t entryCount);

	size_t getEntryIndex(ExprType type, bool overwriting) const;

	static const ExprFactory &factory_;

	Entry *entryList_;
	size_t entryCount_;
};

class SQLExprs::ExprRegistrar {
public:
	struct VariantTraits;

	ExprRegistrar() throw();

	explicit ExprRegistrar(const ExprRegistrar &subRegistrar) throw();

	virtual void operator()() const;

protected:
	typedef ExprFactory::FactoryFunc FactoryFunc;

	template<typename S, ExprType T>
	struct SpecOf {
		typedef typename S::template Spec<T>::Type SpecType;
		ExprSpec operator()() const {
			return SpecType::create();
		}
	};

	struct FuncTableByTypes {
		enum {
			LIST_SIZE = ExprSpec::IN_EXPANDED_TYPE_COUNT
		};
		FuncTableByTypes();
		explicit FuncTableByTypes(const FactoryFunc (&list)[LIST_SIZE]);
		FactoryFunc list_[LIST_SIZE];
	};

	struct FuncTableByCounts {
		enum {
			LIST_SIZE = ExprSpec::IN_MAX_OPTS_COUNT
		};
		explicit FuncTableByCounts(const FuncTableByTypes (&list)[LIST_SIZE]);
		FuncTableByTypes list_[LIST_SIZE];
	};

	struct FuncTableByAggr {
		FuncTableByAggr(
				const FuncTableByTypes &advance,
				const FuncTableByTypes &merge,
				const FuncTableByTypes &finish,
				const FuncTableByTypes &decrForward,
				const FuncTableByTypes &decrBackward);
		FuncTableByTypes advance_;
		FuncTableByTypes merge_;
		FuncTableByTypes finish_;
		FuncTableByTypes decrForward_;
		FuncTableByTypes decrBackward_;
	};

	struct BasicFuncTable {
		BasicFuncTable(const FuncTableByCounts base, FactoryFunc checker);
		const FuncTableByCounts base_;
		FactoryFunc checker_;
	};

	struct AggrFuncTable {
		AggrFuncTable(const FuncTableByAggr base, FactoryFunc checker);
		const FuncTableByAggr base_;
		FactoryFunc checker_;
	};

	void addDirect(
			ExprType type, const ExprSpec &spec, FactoryFunc func) const;

	template<ExprType T, typename E>
	void addDirectVariants(const ExprSpec *spec) const;

	static FactoryFunc resolveFactoryFunc(
			ExprFactoryContext &cxt, const BasicFuncTable &table,
			const ExprSpec &spec);
	static FactoryFunc resolveFactoryFunc(
			ExprFactoryContext &cxt, const AggrFuncTable &table,
			const ExprSpec &spec);

	static const FuncTableByTypes& resolveAggregationSubTable(
			ExprFactoryContext &cxt, const FuncTableByAggr &baseTable);
	static const FuncTableByTypes& resolveDecrementalSubTable(
			ExprFactoryContext &cxt, const FuncTableByAggr &baseTable);

	static size_t getArgCountVariant(
			ExprFactoryContext &cxt, const ExprSpec &spec);
	static size_t getColumnTypeVariant(
			ExprFactoryContext &cxt, const ExprSpec &spec);
	static const Expression& resolveBaseExpression(ExprFactoryContext &cxt);

	template<typename E>
	static Expression& createDirect(
			ExprFactoryContext &cxt, const ExprCode &code) {
		return *(ALLOC_NEW(cxt.getAllocator()) E(cxt, code));
	}

	template<typename E>
	static Expression& createDirectNoArgs(
			ExprFactoryContext &cxt, const ExprCode &code) {
		static_cast<void>(cxt);
		static_cast<void>(code);
		return *(ALLOC_NEW(cxt.getAllocator()) E());
	}

	template<typename E>
	static Expression& createDirectCustom(
			ExprFactoryContext &cxt, const ExprCode &code) {
		E &expr = *(ALLOC_NEW(cxt.getAllocator()) E());
		expr.initializeCustom(cxt, code);
		return expr;
	}

private:
	DefaultExprFactory *factory_;
};

struct SQLExprs::ExprRegistrar::VariantTraits {
	typedef ExprFactoryContext ContextType;
	typedef ExprCode CodeType;
	typedef Expression ObjectType;

	template<typename V>
	static Expression& create(ExprFactoryContext &cxt, const ExprCode &code) {
		V &expr = *(ALLOC_NEW(cxt.getAllocator()) V());
		expr.initializeCustom(cxt, code);
		return expr;
	}
};

template<typename R>
class SQLExprs::VariantsRegistrarBase {
private:
	typedef R RegistrarTraitsType;

	typedef typename RegistrarTraitsType::ContextType ContextType;
	typedef typename RegistrarTraitsType::CodeType CodeType;
	typedef typename RegistrarTraitsType::ObjectType ObjectType;

	typedef ObjectType& (*FactoryFunc)(ContextType&, const CodeType&);
	typedef size_t (*VariantResolverFunc)(ContextType&, const CodeType&);

public:
	template<size_t T, typename C>
	static FactoryFunc add();
	template<size_t T, typename C, size_t Begin, size_t End>
	static FactoryFunc add();

private:
	struct FactoryFuncEntry {
		FactoryFuncEntry() : func_(NULL) {}
		FactoryFunc func_;
	};

	typedef std::pair<FactoryFuncEntry*, size_t> VariantEntryList;
	typedef std::pair<VariantEntryList, VariantResolverFunc> VariantTable;

	template<size_t T>
	static VariantTable& getVariantTableRef();

	template<size_t T, size_t N>
	static VariantTable& resolveVariantTable();

	template<typename C, size_t Begin, size_t End>
	static void addRange(VariantEntryList &entryList, const util::FalseType&);

	template<typename C, size_t Begin, size_t End>
	static void addRange(VariantEntryList &entryList, const util::TrueType&);

	static void addAt(
			VariantEntryList &entryList, size_t index, FactoryFunc func);

	template<size_t T>
	static ObjectType& create(ContextType &cxt, const CodeType &code);

	static ObjectType& createAt(
			const VariantTable &table, ContextType &cxt, const CodeType &code);

	static size_t checkTableEntry(
			const VariantEntryList &entryList, size_t index, bool forRead);
};

struct SQLExprs::ExprSpecBase {
	template<size_t N>
	struct NumericPromotion {
		static const TupleColumnType COLUMN_TYPE =
				(N == 0 ? TupleTypes::TYPE_LONG : TupleTypes::TYPE_DOUBLE);
	};

	template<size_t N>
	struct TimestampPromotion {
		static const TupleColumnType COLUMN_TYPE = (
				N == 0 ? TupleTypes::TYPE_TIMESTAMP :
				N == 1 ? TupleTypes::TYPE_MICRO_TIMESTAMP :
						TupleTypes::TYPE_NANO_TIMESTAMP);
	};

	template<TupleColumnType T, uint32_t Flags, size_t N>
	struct InPromotion {
		enum {
			NUMERIC_PROMO = (T == TupleTypes::TYPE_NUMERIC),
			TIMESTAMP_PROMO = (T == TupleTypes::TYPE_TIMESTAMP &&
					(Flags & ExprSpec::FLAG_PROMOTABLE) != 0),
			PROMO_ENABLED = (NUMERIC_PROMO || TIMESTAMP_PROMO)
		};
		static const TupleColumnType COLUMN_TYPE = (
				NUMERIC_PROMO ? NumericPromotion<N>::COLUMN_TYPE :
				TIMESTAMP_PROMO ? TimestampPromotion<N>::COLUMN_TYPE :
						static_cast<TupleColumnType>(TupleTypes::TYPE_NULL));
	};

	struct InPromotionVariants {
		static bool matchVariantIndex(
				const ExprSpec::In &in, TupleColumnType type,
				int32_t &lastIndex, bool &definitive);

		static bool isNumericPromotion(const ExprSpec::In &in);
		static bool isTimestampPromotion(const ExprSpec::In &in);
	};

	template<
			TupleColumnType T1 = TupleTypes::TYPE_NULL,
			uint32_t Flags = 0,
			TupleColumnType T2 = TupleTypes::TYPE_NULL>
	struct In {
		template<size_t N> 
		struct TypeAt {
		private:
			typedef InPromotion<T1, Flags, N> PromotionType;
			enum {
				PROMO_ENABLED = PromotionType::PROMO_ENABLED
			};
			static const TupleColumnType PROMO_COLUMN_TYPE =
					PromotionType::COLUMN_TYPE;

		public:
			static const TupleColumnType COLUMN_TYPE =
					(N == 0 ? T1 :
					(N == 1 ? T2 :
					static_cast<TupleColumnType>(TupleTypes::TYPE_NULL)));
			typedef SQLValues::TypeUtils::Traits<COLUMN_TYPE> TraitsType;

			enum {
				TYPE_EMPTY = (COLUMN_TYPE == TupleTypes::TYPE_NULL),
				NON_NUMERIC = (!TYPE_EMPTY &&
						COLUMN_TYPE != TupleTypes::TYPE_NUMERIC &&
						!TraitsType::FOR_NUMERIC_EXPLICIT)
			};

			static const TupleColumnType EXPANDED_COLUMN_TYPE =
					(PROMO_ENABLED ?
							PROMO_COLUMN_TYPE :
							((N != 0 && TYPE_EMPTY) ? T1 : COLUMN_TYPE));
			enum {
				EXPANDED_TYPE_EMPTY =
						(EXPANDED_COLUMN_TYPE == TupleTypes::TYPE_NULL)
			};
		};
		typedef In<T1,
				((Flags & ~ExprSpec::FLAG_NULLABLE) |
				((Flags & ExprSpec::FLAG_NULLABLE) == 0 ?
						ExprSpec::FLAG_EXACT : 0)),
				T2> NullableInv;
		static const uint32_t IN_FLAGS = Flags;
		enum {
			VALUE_EMPTY = (TypeAt<0>::TYPE_EMPTY),
			MULTI_COLUMN_TYPE1 = (!TypeAt<1>::EXPANDED_TYPE_EMPTY),
			MULTI_COLUMN_TYPE2 = (!TypeAt<2>::EXPANDED_TYPE_EMPTY),
			TYPE_ANY = (TypeAt<0>::COLUMN_TYPE == TupleTypes::TYPE_ANY),
			TYPE_NULLABLE_ANY =
					(TYPE_ANY &&
					(Flags & ExprSpec::FLAG_NON_NULLABLE) == 0),
			TYPE_NULLABLE =
					(!TYPE_NULLABLE_ANY && (Flags & ExprSpec::FLAG_EXACT) == 0),
			TYPE_NON_NUMERIC =
					(TypeAt<0>::NON_NUMERIC || TypeAt<1>::NON_NUMERIC),
			ANY_PROMOTABLE =
					(TYPE_ANY && (Flags & ExprSpec::FLAG_PROMOTABLE) != 0),
			ARG_CHECK = ((Flags & ExprSpec::FLAG_EXACT) != 0)
		};
		static ExprSpec::In create() {
			ExprSpec::In in;
			UTIL_STATIC_ASSERT(ExprSpec::IN_TYPE_COUNT == 2);
			in.typeList_[0] = TypeAt<0>::COLUMN_TYPE;
			in.typeList_[1] = TypeAt<1>::COLUMN_TYPE;
			in.flags_ = IN_FLAGS;
			return in;
		}
	};

	template<
			typename In1 = In<>, typename In2 = In<>, typename In3 = In<>,
			typename In4 = In<>, typename In5 = In<>, typename In6 = In<>,
			typename In7 = In<>, typename In8 = In<> >
	struct InList {
		template<size_t N>
		struct At : public util::Conditional<
				N == 0, In1, typename util::Conditional<
				N == 1, In2, typename util::Conditional<
				N == 2, In3, typename util::Conditional<
				N == 3, In4, typename util::Conditional<
				N == 4, In5, typename util::Conditional<
				N == 5, In6, typename util::Conditional<
				N == 6, In7, typename util::Conditional<
				N == 7, In8,
				void>::Type>::Type>::Type>::Type>::Type>::Type>::Type>::Type {
		};
		template<size_t N> static void getAll(ExprSpec::In (&dest)[N]) {
			const ExprSpec::In src[ExprSpec::IN_LIST_SIZE] = {
				At<0>::create(),
				At<1>::create(),
				At<2>::create(),
				At<3>::create(),
				At<4>::create(),
				At<5>::create(),
				At<6>::create(),
				At<7>::create()
			};
			UTIL_STATIC_ASSERT((N <= sizeof(src) / sizeof(*src)));
			for (size_t i = 0; i < N; i++) {
				dest[i] = src[i];
			}
		}
		template<typename M>
		struct ModOf {
			typedef InList<
					typename M::template BaseType< At<0> >::Type,
					typename M::template BaseType< At<1> >::Type,
					typename M::template BaseType< At<2> >::Type,
					typename M::template BaseType< At<3> >::Type,
					typename M::template BaseType< At<4> >::Type,
					typename M::template BaseType< At<5> >::Type,
					typename M::template BaseType< At<6> >::Type,
					typename M::template BaseType< At<7> >::Type> Type;
		};
		template<typename M>
		struct IndexOf {
			enum {
				VALUE =
						(M::template Type< At<0> >::VALUE) ? 0 :
						(M::template Type< At<1> >::VALUE) ? 1 :
						(M::template Type< At<2> >::VALUE) ? 2 :
						(M::template Type< At<3> >::VALUE) ? 3 :
						(M::template Type< At<4> >::VALUE) ? 4 :
						(M::template Type< At<5> >::VALUE) ? 5 :
						(M::template Type< At<6> >::VALUE) ? 6 :
						(M::template Type< At<7> >::VALUE) ? 7 : 8
			};
		};
		template<typename M>
		struct MatchWith {
			enum {
				VALUE =
						(IndexOf<M>::VALUE <
						static_cast<size_t>(ExprSpec::IN_TYPE_COUNT))
			};
		};
		struct NullableInvMod {
			template<typename I> struct BaseType {
				typedef typename I::NullableInv Type;
			};
		};
		struct EmptyMatch {
			template<typename I> struct Type {
				enum {
					VALUE = I::VALUE_EMPTY
				};
			};
		};
		struct EmptyOrOptionalMatch {
			template<typename I> struct Type {
				enum {
					VALUE = I::VALUE_EMPTY ||
							((I::IN_FLAGS & ExprSpec::FLAG_OPTIONAL) != 0)
				};
			};
		};
		struct MultiMatch1 {
			template<typename I> struct Type {
				enum {
					VALUE = I::MULTI_COLUMN_TYPE1
				};
			};
		};
		struct MultiMatch2 {
			template<typename I> struct Type {
				enum {
					VALUE = I::MULTI_COLUMN_TYPE2
				};
			};
		};
		struct NullableAnyTypeMatch {
			template<typename I> struct Type {
				enum {
					VALUE = I::TYPE_NULLABLE_ANY
				};
			};
		};
		struct AnyPromotableMatch {
			template<typename I> struct Type {
				enum {
					VALUE = I::ANY_PROMOTABLE
				};
			};
		};
		struct ArgCheckMatch {
			template<typename I> struct Type {
				enum {
					VALUE = I::ARG_CHECK
				};
			};
		};
		struct NonNumericMatch {
			template<typename I> struct Type {
				enum {
					VALUE = I::TYPE_NON_NUMERIC
				};
			};
		};
		typedef typename ModOf<NullableInvMod>::Type NullableInv;
		enum {
			MIN_COUNT = IndexOf<EmptyOrOptionalMatch>::VALUE,
			MAX_COUNT = IndexOf<EmptyMatch>::VALUE,
			MULTI_COLUMN_TYPE1 = MatchWith<MultiMatch1>::VALUE,
			MULTI_COLUMN_TYPE2 = MatchWith<MultiMatch2>::VALUE,
			IN_NULLABLE_ANY = MatchWith<NullableAnyTypeMatch>::VALUE,
			IN_ANY_PROMOTABLE = MatchWith<AnyPromotableMatch>::VALUE,
			IN_ARG_CHECKING = MatchWith<ArgCheckMatch>::VALUE,
			IN_NON_NUMERIC = MatchWith<NonNumericMatch>::VALUE
		};
	};

	template<
			TupleColumnType T, typename L, uint32_t Flags = 0,
			typename Aggr = InList<>, ExprType Distinct = SQLType::START_EXPR>
	struct Type {
		static const TupleColumnType RESULT_TYPE = T;

		typedef L InListType;
		typedef typename Aggr::NullableInv AggrListType;

		static const uint32_t TYPE_FLAGS = Flags;
		static const ExprType DISTINCT_TYPE = Distinct;

		enum {
			MULTI_COLUMN_TYPE1 = InListType::MULTI_COLUMN_TYPE1,
			MULTI_COLUMN_TYPE2 = InListType::MULTI_COLUMN_TYPE2,
			IN_REPEAT_UNIT = (
					((TYPE_FLAGS & ExprSpec::FLAG_REPEAT1) != 0) ? 1 :
					((TYPE_FLAGS & ExprSpec::FLAG_REPEAT2) != 0) ? 2 : 0),
			IN_REPEATABLE = (IN_REPEAT_UNIT > 0),
			IN_MIN_COUNT = InListType::MIN_COUNT,
			IN_BASE_MAX_COUNT = InListType::MAX_COUNT,
			IN_MAX_COUNT = (IN_REPEATABLE ? -1 : IN_BASE_MAX_COUNT),
			IN_OPT_COUNT = (IN_REPEATABLE ?
					0 : IN_BASE_MAX_COUNT - IN_MIN_COUNT),
			IN_LAST_POS = (IN_BASE_MAX_COUNT > 0 ? IN_BASE_MAX_COUNT - 1 : 0),
			IN_NULLABLE = ((TYPE_FLAGS & (
					ExprSpec::FLAG_NON_NULLABLE |
					ExprSpec::FLAG_INHERIT_NULLABLE1)) != 0),
			IN_NULLABLE_ANY = InListType::IN_NULLABLE_ANY,
			IN_ANY_PROMOTABLE = InListType::IN_ANY_PROMOTABLE,
			IN_ARG_CHECKING = InListType::IN_ARG_CHECKING,
			RESULT_NON_NULLABLE = ((TYPE_FLAGS &
					ExprSpec::FLAG_NON_NULLABLE) != 0),
			RESULT_NULLABLE =
					!RESULT_NON_NULLABLE && (IN_NULLABLE_ANY || ((TYPE_FLAGS & (
							ExprSpec::FLAG_NULLABLE |
							ExprSpec::FLAG_INHERIT_NULLABLE1)) != 0)),
			AGGR_IN_NON_NUMERIC = AggrListType::IN_NON_NUMERIC,
			AGGR_DECREMENTAL = (
					(TYPE_FLAGS & ExprSpec::FLAG_WINDOW) != 0 &&
					(TYPE_FLAGS & ExprSpec::FLAG_COMP_SENSITIVE) == 0 &&
					!AGGR_IN_NON_NUMERIC),
			TYPE_AUTO_FLAGS =
					(AGGR_DECREMENTAL ? ExprSpec::FLAG_AGGR_DECREMENTAL : 0),
			TYPE_FULL_FLAGS = (TYPE_FLAGS | TYPE_AUTO_FLAGS),
		};

		static ExprSpec create() {
			ExprSpec spec;
			InListType::getAll(spec.inList_);
			AggrListType::getAll(spec.aggrList_);
			spec.outType_ = RESULT_TYPE;
			spec.flags_ = TYPE_FULL_FLAGS;
			spec.argCounts_ =
					std::pair<int32_t, int32_t>(IN_MIN_COUNT, IN_MAX_COUNT);
			spec.distinctExprType_ = DISTINCT_TYPE;
			return spec;
		}
	};

	typedef In<
			TupleTypes::TYPE_NUMERIC,
			ExprSpec::FLAG_PROMOTABLE> PromotableNumIn;
	typedef In<
			TupleTypes::TYPE_TIMESTAMP,
			ExprSpec::FLAG_PROMOTABLE> PromotableTimestampIn;
	typedef In<
			TupleTypes::TYPE_STRING, ExprSpec::FLAG_PROMOTABLE,
			TupleTypes::TYPE_BLOB> PromotableStringOrBlobIn;
	typedef In<
			TupleTypes::TYPE_ANY, ExprSpec::FLAG_PROMOTABLE> PromotableAnyIn;

	typedef In<
			TupleTypes::TYPE_ANY,
			ExprSpec::FLAG_NON_NULLABLE> NullCheckedAnyIn;
	typedef In<
			TupleTypes::TYPE_ANY, ExprSpec::FLAG_PROMOTABLE |
			ExprSpec::FLAG_NON_NULLABLE> NullCheckedPromotableAnyIn;
};

struct SQLExprs::ExprUtils {
	typedef SQLValues::ValueContext ValueContext;
	typedef ExprSpec::DecrementalType DecrementalType;

	template<typename T, bool Initializable> struct ResultWriter;
	template<typename T> struct AggregationManipulator;

	class BasicComparatorRef;
	class EmptyComparatorRef;

	template<typename Alloc, typename W, typename C>
	class CustomFunctionContext;
	template<typename Alloc, typename W, typename C>
	class AggregationFunctionContext;
	template<typename Alloc, typename W, typename C, typename F>
	class DecrementalFunctionContext;

	template<typename Enabled> class BaseAllocatorScope;
	template<TupleColumnType T, typename Nullable> struct ArgHolder;
	template<typename Nullable> struct BaseArgChecker;
	template<TupleColumnType T> struct ResultValueGenerator;
	template<TupleColumnType T, typename Promotable> struct BaseResultValue;
	template<typename Enabled> struct BaseFinishChecker;

	struct FunctorPolicy;

	template<size_t M = 0> struct VariantTraits;
	template<size_t M, AggregationPhase Phase, DecrementalType D>
	struct AggregationVariantTraits;

	template<ExprType T, typename S, typename V> struct FunctorTraits;

	template<typename F, typename Traits> class BasicEvaluator;
};

template<typename T, bool Initializable>
struct SQLExprs::ExprUtils::ResultWriter {
public:
	typedef typename SQLValues::TypeUtils::template Traits<
			T::COLUMN_TYPE>::WriterType WriterType;
	typedef WriterType& WriterRef;

	ResultWriter(ExprContext&, uint64_t) {}

	WriterRef initialize(ExprContext &cxt, uint64_t initialCapacity) {
		writer_ = UTIL_MAKE_LOCAL_UNIQUE(
				writer_, WriterType,
				ValueUtils::toWriterContext<T>(cxt.getValueContext()),
				T::COLUMN_TYPE,
				ValueUtils::toWriterCapacity<T>(initialCapacity));
		return *writer_;
	}

	WriterRef resolve() {
		return *writer_;
	}

private:
	typedef SQLValues::ValueUtils ValueUtils;
	util::LocalUniquePtr<WriterType> writer_;
};

template<typename T>
struct SQLExprs::ExprUtils::ResultWriter<T, false> {
public:
	typedef typename SQLValues::TypeUtils::template Traits<
			T::COLUMN_TYPE>::WriterType WriterType;
	typedef WriterType& WriterRef;

	inline ResultWriter(ExprContext &cxt, uint64_t initialCapacity) :
			writer_(
					ValueUtils::toWriterContext<T>(cxt.getValueContext()),
					T::COLUMN_TYPE,
					ValueUtils::toWriterCapacity<T>(initialCapacity)) {
	}
	inline ~ResultWriter() {}

	inline WriterRef initialize(ExprContext&, uint64_t) {
		assert(false);
		return writer_;
	}

	inline WriterRef resolve() {
		return writer_;
	}

private:
	typedef SQLValues::ValueUtils ValueUtils;
	WriterType writer_;
};

template<>
struct SQLExprs::ExprUtils::ResultWriter<void, false> {
public:
	typedef util::FalseType WriterType;
	typedef WriterType WriterRef;

	ResultWriter(ExprContext&, uint64_t) {
	}

	WriterRef initialize(ExprContext&, uint64_t) {
		return resolve();
	}

	WriterRef resolve() {
		assert(false);
		return WriterType();
	}
};

template<>
struct SQLExprs::ExprUtils::AggregationManipulator<int64_t> {
	typedef int64_t RetType;
	static int64_t add(void*, int64_t v1, int64_t v2, const util::TrueType&);
	static int64_t add(void*, int64_t v1, int64_t v2, const util::FalseType&) {
		return v1 + v2;
	}
	static int64_t increment(int64_t v) { return ++v; }
};

template<>
struct SQLExprs::ExprUtils::AggregationManipulator<double> {
	typedef double RetType;
	static double add(void*, double v1, double v2, const util::TrueType&);
	static double add(void*, double v1, double v2, const util::FalseType&) {
		return v1 + v2;
	}
};

template<>
struct SQLExprs::ExprUtils::AggregationManipulator<SQLValues::StringReader> {
	typedef TupleString RetType;

	template<typename C> static TupleString build(
			C *cxt, SQLValues::StringReader &src) {
		assert(cxt != NULL);
		SQLValues::StringBuilder builder(cxt->getBase().getValueContext());

		SQLValues::ValueUtils::subForwardPart(
				builder, src, std::numeric_limits<uint64_t>::max());

		return builder.build(cxt->getBase());
	}

	template<typename C> static TupleString add(
			C *cxt, const TupleString &v1, SQLValues::StringReader &v2,
			const util::TrueType&) {
		assert(cxt != NULL);
		SQLValues::StringBuilder builder(cxt->getBase().getValueContext());

		SQLValues::ValueUtils::subForwardPart(
				builder, *SQLValues::ValueUtils::toStringReader(v1),
				std::numeric_limits<uint64_t>::max());
		SQLValues::ValueUtils::subForwardPart(
				builder, v2, std::numeric_limits<uint64_t>::max());

		return builder.build(cxt->getBase());
	}
};

template<>
struct SQLExprs::ExprUtils::AggregationManipulator<SQLValues::LobReader> {
	typedef TupleValue RetType;

	template<typename C> static TupleValue build(
			C *cxt, SQLValues::LobReader &src) {
		assert(cxt != NULL);
		SQLValues::LobBuilder builder(
				cxt->getBase().getValueContext().getVarContext(),
				TupleTypes::TYPE_BLOB, 0);

		SQLValues::ValueUtils::subForwardPart(
				builder, src, std::numeric_limits<uint64_t>::max());

		return builder.build();
	}

	template<typename C> static TupleValue add(
			C *cxt, const TupleValue &v1, SQLValues::LobReader &v2,
			const util::TrueType&) {
		assert(cxt != NULL);
		SQLValues::LobBuilder builder(
				cxt->getBase().getValueContext().getVarContext(),
				TupleTypes::TYPE_BLOB, 0);

		SQLValues::ValueUtils::subForwardPart(
				builder, *SQLValues::ValueUtils::toLobReader(v1),
				std::numeric_limits<uint64_t>::max());
		SQLValues::ValueUtils::subForwardPart(
				builder, v2, std::numeric_limits<uint64_t>::max());

		return builder.build();
	}
};

class SQLExprs::ExprUtils::BasicComparatorRef {
public:
	typedef SQLValues::ValueComparator::Arranged BaseType;

	template<typename Expr>
	explicit BasicComparatorRef(const Expr *expr) :
			base_(expr->getComparator()) {
	}

	const BaseType& get() const {
		assert(base_ != NULL);
		return *base_;
	}

private:
	const BaseType *base_;
};

class SQLExprs::ExprUtils::EmptyComparatorRef {
public:
	typedef util::FalseType BaseType;

	explicit EmptyComparatorRef(const Expression*) {}

	const BaseType& get() const;
};

template<typename Alloc, typename W, typename C>
class SQLExprs::ExprUtils::CustomFunctionContext :
		public SQLExprs::NormalFunctionContext {
private:
	typedef Alloc BaseAllocatorType;
	typedef W BaseWriterType;
	typedef typename util::BoolType<
			(!util::IsSame<BaseAllocatorType, util::FalseType>::VALUE)>::Result
			AllocatorEnabled;
	typedef typename BaseWriterType::WriterRef WriterRef;

public:
	typedef typename util::Conditional<
			AllocatorEnabled::VALUE,
			util::StdAllocator<void, void>, util::FalseType>::Type AllocatorType;
	typedef typename BaseWriterType::WriterType WriterType;

	template<typename Expr>
	explicit CustomFunctionContext(
			const std::pair<ExprContext*, const Expr*> &src);

	CustomFunctionContext& getFunctionContext() {
		return *this;
	}

	AllocatorType getAllocator();

	WriterRef initializeResultWriter(uint64_t initialCapacity);
	WriterRef getResultWriter();

	const typename C::BaseType& getComparator();

	void finishFunction();
	bool isFunctionFinished();

private:
	typedef typename util::Conditional<
			AllocatorEnabled::VALUE,
			BaseAllocatorType&, util::FalseType>::Type BaseAllocatorRef;

	static util::StackAllocator& resolveAllocator(
			ExprContext &baseCxt, const util::TrueType&);
	static util::FalseType resolveAllocator(
			ExprContext &baseCxt, const util::FalseType&);

	BaseAllocatorRef alloc_;
	BaseWriterType resultWriter_;
	C comparator_;
	bool functionFinished_;
};

template<typename Alloc, typename W, typename C>
class SQLExprs::ExprUtils::AggregationFunctionContext :
		public SQLExprs::ExprUtils::CustomFunctionContext<Alloc, W, C> {
protected:
	template<typename T>
	struct TypeTagOf {
		static const TupleColumnType COLUMN_TYPE =
				SQLValues::TypeUtils::template ValueTraits<
						T>::COLUMN_TYPE_AS_VALUE;
		static const TupleColumnType READER_COLUMN_TYPE =
				SQLValues::TypeUtils::template ValueTraits<
						T>::COLUMN_TYPE_AS_READER;

		typedef typename util::IsSame<T, TupleValue>::Type ForAny;
		typedef typename SQLValues::Types::template Of<COLUMN_TYPE>::Type Type;

		typedef typename SQLValues::Types::template Of<
				READER_COLUMN_TYPE>::Type TypeByReader;
	};

public:
	explicit AggregationFunctionContext(
			const std::pair<
					ExprContext*, const AggregationExpressionBase*> &src);

	AggregationFunctionContext& getFunctionContext() {
		return *this;
	}

	template<size_t N, typename T> T getAggrValue() {
		typedef typename TypeTagOf<T>::ForAny ForAny;
		return getAggregationValue<T>(aggrColumns_[N], ForAny());
	}

	template<size_t N, typename T, typename Promo>
	void setAggrValue(const T &value, const Promo&) {
		typedef typename TypeTagOf<T>::ForAny ForAny;
		setAggregationValue(aggrColumns_[N], value, ForAny(), Promo());
	}

	template<size_t N, typename T, typename Promo>
	void setAggrValue(T &value, const Promo&) {
		typedef typename TypeTagOf<T>::ForAny ForAny;
		typedef typename util::BoolType<util::IsSame<
				T, typename AggregationManipulator<T>::RetType>::VALUE
				>::Result SameRetType;
		setAggregationValue(
				aggrColumns_[N], build(value, SameRetType()), ForAny(),
				Promo());
	}

	template<size_t N, typename T, typename Checked> void addAggrValue(
			const T &value) {
		typedef typename AggregationManipulator<T>::RetType RetType;
		setAggrValue<N, RetType>(AggregationManipulator<T>::add(
				this, getAggrValue<N, RetType>(), value, Checked()),
				util::FalseType());
	}

	template<size_t N, typename T, typename Checked> void addAggrValue(
			T &value,
			typename TypeTagOf<T>::TypeByReader (*)[1] = NULL) {
		NormalFunctionContext::getBase().template appendAggregationValueBy<
				typename TypeTagOf<T>::TypeByReader>(aggrColumns_[N], value);
	}

	template<size_t N, typename T> void incrementAggrValue() {
		NormalFunctionContext::getBase().template incrementAggregationValueBy<
				typename TypeTagOf<T>::Type>(aggrColumns_[N]);
	}

	template<size_t N, typename T> bool checkNullAggrValue() {
		return isAggregationNull(aggrColumns_[N]);
	}

	int64_t getWindowValueCount() {
		const WindowState *state =
				NormalFunctionContext::getBase().getWindowState();
		assert(state != NULL);
		return state->partitionValueCount_;
	}

protected:
	const SummaryColumn* getAggrColumns() {
		return aggrColumns_;
	}

private:
	template<typename T>
	T build(const T &value, const util::TrueType&) {
		return value;
	}

	template<typename T>
	typename AggregationManipulator<T>::RetType build(
			T &value, const util::FalseType&) {
		return AggregationManipulator<T>::build(this, value);
	}

	template<typename T>
	TupleValue getAggregationValue(
			const SummaryColumn &column, const util::TrueType&) {
		return NormalFunctionContext::getBase().getAggregationValue(column);
	}

	template<typename T>
	T getAggregationValue(
			const SummaryColumn &column, const util::FalseType&) {
		return NormalFunctionContext::getBase().template getAggregationValueAs<
				typename TypeTagOf<T>::Type>(column);
	}

	void setAggregationValue(
			const SummaryColumn &column, const TupleValue &value,
			const util::TrueType&, const util::FalseType&) {
		NormalFunctionContext::getBase().setAggregationValue(
				column, value);
	}

	void setAggregationValue(
			const SummaryColumn &column, const TupleValue &value,
			const util::TrueType&, const util::TrueType&) {
		NormalFunctionContext::getBase().setAggregationValuePromoted(
				column, value);
	}

	template<typename T>
	void setAggregationValue(
			const SummaryColumn &column, const T &value,
			const util::FalseType&, const util::FalseType&) {
		NormalFunctionContext::getBase().template setAggregationValueBy<
				typename TypeTagOf<T>::Type>(column, value);
	}

	bool isAggregationNull(const SummaryColumn &column) {
		return NormalFunctionContext::getBase().isAggregationNull(column);
	}

	const SummaryColumn *aggrColumns_;
};

template<typename Alloc, typename W, typename C, typename F>
class SQLExprs::ExprUtils::DecrementalFunctionContext :
		public SQLExprs::ExprUtils::AggregationFunctionContext<Alloc, W, C> {
private:
	typedef AggregationFunctionContext<Alloc, W, C> AggrContext;

	template<typename T>
	struct AggrTagOf {
		typedef typename AggrContext::template TypeTagOf<T>::Type Type;
	};

public:
	explicit DecrementalFunctionContext(
			const std::pair<
					ExprContext*, const AggregationExpressionBase*> &src);

	DecrementalFunctionContext& getFunctionContext() {
		return *this;
	}

	template<size_t N, typename T, typename Promo>
	void setAggrValue(const T &value, const Promo&) {
		UTIL_STATIC_ASSERT(!Promo::VALUE);
		NormalFunctionContext::getBase().template setDecrementalValueBy<
				typename AggrTagOf<T>::Type, F>(getAggrColumns()[N], value);
	}

	template<size_t N, typename T, typename Checked> void addAggrValue(
			const T &value) {
		if (Checked::VALUE && F::VALUE) {
			typedef typename AggregationManipulator<T>::RetType RetType;
			AggregationManipulator<T>::add(
					this, AggrContext::template getAggrValue<N, RetType>(),
					value, Checked());
		}
		NormalFunctionContext::getBase().template addDecrementalValueBy<
				typename AggrTagOf<T>::Type, F>(getAggrColumns()[N], value);
	}

	template<size_t N, typename T> void incrementAggrValue() {
		NormalFunctionContext::getBase().template incrementDecrementalValueBy<
				typename AggrTagOf<T>::Type, F>(getAggrColumns()[N]);
	}

	template<size_t N, typename T> bool checkNullAggrValue() {
		ExprContext &base = NormalFunctionContext::getBase();
		return base.template checkDecrementalCounterAs<
				typename AggrTagOf<T>::Type, F>(getAggrColumns()[N]);
	}

private:
	const SummaryColumn* getAggrColumns() {
		return AggrContext::getAggrColumns();
	}
};

template<typename Enabled>
class SQLExprs::ExprUtils::BaseAllocatorScope {
public:
	explicit BaseAllocatorScope(ExprContext&) {
		UTIL_STATIC_ASSERT(!Enabled::VALUE);
	}
};

template<>
class SQLExprs::ExprUtils::BaseAllocatorScope<util::TrueType> {
public:
	explicit BaseAllocatorScope(ExprContext &cxt) :
			scope_(cxt.getAllocator()) {
	}

private:
	util::StackAllocator::Scope scope_;
};

template<TupleColumnType T, typename Nullable>
struct SQLExprs::ExprUtils::ArgHolder {
private:
	typedef SQLValues::TypeUtils::Traits<T> Traits;

	static const bool TYPE_INVALID = (T == TupleTypes::TYPE_NULL);
	static const bool READER_AVAILABLE = Traits::READER_AVAILABLE;
	static const TupleColumnType FILTERED_TYPE = TYPE_INVALID ?
			static_cast<TupleColumnType>(TupleTypes::TYPE_ANY) : T;

	typedef typename Traits::LocalValueType ValueType;
	typedef typename Traits::ReadableType ReadableType;
	typedef typename Traits::ReadableRefType ReadableRefType;
	typedef typename Traits::ReadablePtrType ReadablePtrType;

	typedef typename util::BoolType<!TYPE_INVALID>::Result ValidType;

	template<typename R>
	struct ReaderHolder {
		template<typename S> explicit ReaderHolder(const S &src) :
				reader_(src),
				readerRef_(reader_) {
		}
		operator R&() const {
			return readerRef_;
		};
		R reader_;
		R &readerRef_;
	};

	typedef typename util::Conditional<
			READER_AVAILABLE,
			ReaderHolder<ReadableType>, ValueType>::Type BaseHoldingType;
	typedef typename util::Conditional<
			TYPE_INVALID,
			util::FalseType, BaseHoldingType>::Type HoldingType;

public:
	typedef typename util::Conditional<
			TYPE_INVALID, util::FalseType, ReadableRefType>::Type RefType;
	typedef typename util::Conditional<
			TYPE_INVALID, util::FalseType, ReadablePtrType>::Type PtrType;

	explicit ArgHolder(const TupleValue &value) :
			holdingValue_(filterReadableSource(
					SQLValues::ValueUtils::toReadableSource<FILTERED_TYPE>(
							value), ValidType())) {
	}

	RefType operator()() const {
		return holdingValue_;
	}

private:
	ArgHolder(const ArgHolder&);
	ArgHolder& operator=(const ArgHolder&);

	template<typename U>
	static const U& filterReadableSource(const U &src, const util::TrueType&) {
		return src;
	}

	template<typename U>
	static util::FalseType filterReadableSource(
			const U&, const util::FalseType&) {
		assert(false);
		return util::FalseType();
	}

	HoldingType holdingValue_;
};

template<TupleColumnType T>
struct SQLExprs::ExprUtils::ArgHolder<T, util::TrueType> {
private:
	static const bool TYPE_INVALID = (T == TupleTypes::TYPE_NULL);

	typedef ArgHolder<T, util::FalseType> BaseType;
	typedef typename BaseType::PtrType BasePtrType;

	typedef util::LocalUniquePtr<BaseType> BaseHoldingType;
	typedef typename util::Conditional<
			TYPE_INVALID, util::FalseType, BaseHoldingType>::Type HoldingType;

public:
	typedef typename util::Conditional<
			TYPE_INVALID, util::FalseType, BasePtrType>::Type RefType;

	explicit ArgHolder(const TupleValue &value) {
		initialize(holdingValue_, value);
	}

	RefType operator()() const {
		return getRef(holdingValue_);
	}

private:
	template<typename U>
	static void initialize(
			util::LocalUniquePtr<U> &holdingValue, const TupleValue &value) {
		if (!SQLValues::ValueUtils::isNull(value)) {
			holdingValue = UTIL_MAKE_LOCAL_UNIQUE(holdingValue, U, value);
		}
	}

	static void initialize(util::FalseType&, const TupleValue&) {
		assert(false);
	}

	template<typename U>
	static BasePtrType getRef(const util::LocalUniquePtr<U> &holdingValue) {
		BaseType *base = holdingValue.get();
		if (base == NULL) {
			return NULL;
		}
		return &(*base)();
	}

	static util::FalseType getRef(const util::FalseType&) {
		return util::FalseType();
	}

	HoldingType holdingValue_;
};

template<>
struct SQLExprs::ExprUtils::ArgHolder<TupleTypes::TYPE_ANY, util::TrueType> {
private:
	typedef ArgHolder<TupleTypes::TYPE_ANY, util::FalseType> BaseType;

public:
	typedef BaseType::RefType RefType;

	explicit ArgHolder(const TupleValue &value) : base_(value) {
	}

	RefType operator()() const {
		return base_();
	}

private:
	BaseType base_;
};

template<typename Nullable>
struct SQLExprs::ExprUtils::BaseArgChecker {
public:
	BaseArgChecker() : acceptable_(true) {
	}

	bool operator()(const TupleValue &value) {
		if (SQLValues::ValueUtils::isNull(value)) {
			acceptable_ = false;
			return false;
		}
		return true;
	}

	bool operator()() const {
		return true;
	}

private:
	bool acceptable_;
};

template<>
struct SQLExprs::ExprUtils::BaseArgChecker<util::TrueType> {
	bool operator()(const TupleValue&) {
		return true;
	}

	bool operator()() const {
		return true;
	}
};

template<TupleColumnType T>
struct SQLExprs::ExprUtils::ResultValueGenerator {
	TupleValue operator()(
			SQLValues::ValueContext &cxt,
			typename SQLValues::TypeUtils::Traits<
					T>::WritableRefType src) const {
		return SQLValues::ValueUtils::toAnyByWritable<T>(cxt, src);
	}
};

template<TupleColumnType T, typename Promotable>
struct SQLExprs::ExprUtils::BaseResultValue {
	typedef Expression Expr;

	template<typename U>
	static TupleValue of(
			ValueContext &cxt, const Expr*, const std::pair<U, bool> &src) {
		if (!src.second) {
			return TupleValue();
		}
		return ResultValueGenerator<T>()(cxt, src.first);
	}

	template<typename U>
	static TupleValue of(
			ValueContext &cxt, const Expr*, std::pair<U, bool> &src) {
		if (!src.second) {
			return TupleValue();
		}
		return ResultValueGenerator<T>()(cxt, src.first);
	}

	template<typename U>
	static TupleValue of(ValueContext &cxt, const Expr*, U *src) {
		if (src == NULL) {
			return TupleValue();
		}
		return ResultValueGenerator<T>()(cxt, *src);
	}

	template<typename U>
	static TupleValue of(ValueContext &cxt, const Expr*, const U &src) {
		return ResultValueGenerator<T>()(cxt, src);
	}

	template<typename U>
	static TupleValue of(ValueContext &cxt, const Expr*, U &src) {
		return ResultValueGenerator<T>()(cxt, src);
	}
};

template<TupleColumnType T>
struct SQLExprs::ExprUtils::BaseResultValue<T, util::TrueType> {
	typedef Expression Expr;

	template<typename U>
	static TupleValue of(ValueContext &cxt, const Expr *expr, U &src) {
		return expr->asColumnValue(
				&cxt, BaseResultValue<T, util::FalseType>::of(cxt, expr, src));
	}
};

template<typename Enabled>
struct SQLExprs::ExprUtils::BaseFinishChecker {
	template<typename C> static bool check(C &funcCxt) {
		static_cast<void>(funcCxt);
		return true;
	}
};

template<>
struct SQLExprs::ExprUtils::BaseFinishChecker<util::TrueType> {
	template<typename C> static bool check(C &funcCxt) {
		return funcCxt.isFunctionFinished();
	}
};

struct SQLExprs::ExprUtils::FunctorPolicy {
	enum Flag {
		FLAG_ALLOCATABLE = 1 << 0,
		FLAG_PARTIALLY_FINISHABLE = 1 << 1,
		FLAG_WRITER_INITIALIZABLE = 1 << 2,
		FLAG_RESULT_PROMOTABLE = 1 << 3,
		FLAG_ARGS_DELAYED_EVALUABLE = 1 << 4
	};

	template<uint32_t Flags = 0> struct BasePolicy;
	typedef BasePolicy<> DefaultPolicy;

	template<typename T> struct PolicyDetector;
};

template<uint32_t Flags>
struct SQLExprs::ExprUtils::FunctorPolicy::BasePolicy {
	typedef typename util::BoolType<(Flags &
			FLAG_ALLOCATABLE) != 0>::Result Allocatable;
	typedef typename util::BoolType<(Flags &
			FLAG_PARTIALLY_FINISHABLE) != 0>::Result PartiallyFinishable;
	typedef typename util::BoolType<(Flags &
			FLAG_WRITER_INITIALIZABLE) != 0>::Result WriterInitializable;
	typedef typename util::BoolType<(Flags &
			FLAG_RESULT_PROMOTABLE) != 0>::Result ResultPromotable;
	typedef typename util::BoolType<(Flags &
			FLAG_ARGS_DELAYED_EVALUABLE) != 0>::Result ArgsDelayedEvaluable;

	typedef BasePolicy<Flags |
			FLAG_ALLOCATABLE> AsAllocatable;
	typedef BasePolicy<Flags |
			FLAG_PARTIALLY_FINISHABLE> AsPartiallyFinishable;
	typedef BasePolicy<Flags |
			FLAG_WRITER_INITIALIZABLE> AsWriterInitializable;
	typedef BasePolicy<Flags |
			FLAG_RESULT_PROMOTABLE> AsResultPromotable;
	typedef BasePolicy<Flags |
			FLAG_ARGS_DELAYED_EVALUABLE> AsArgsDelayedEvaluable;
};

template<typename T>
struct SQLExprs::ExprUtils::FunctorPolicy::PolicyDetector {
	template<Flag P> struct FlagTag {};

	typedef int FalseSize;
	typedef std::pair<FalseSize, FalseSize> TrueSize;

	template<typename>
	static FalseSize detect(void*) {
		return FalseSize();
	}

	template<typename U>
	static TrueSize detect(
			typename util::EnableIf<util::IsSame<
					typename U::Policy::Allocatable, util::TrueType>::VALUE,
					FlagTag<FLAG_ALLOCATABLE> >::Type*) {
		return TrueSize();
	}

	template<typename U>
	static TrueSize detect(
			typename util::EnableIf<util::IsSame<
					typename U::Policy::PartiallyFinishable, util::TrueType>::VALUE,
					FlagTag<FLAG_PARTIALLY_FINISHABLE> >::Type*) {
		return TrueSize();
	}

	template<typename U>
	static TrueSize detect(
			typename util::EnableIf<util::IsSame<
					typename U::Policy::WriterInitializable, util::TrueType>::VALUE,
					FlagTag<FLAG_WRITER_INITIALIZABLE> >::Type*) {
		return TrueSize();
	}

	template<typename U>
	static TrueSize detect(
			typename util::EnableIf<util::IsSame<
					typename U::Policy::ResultPromotable, util::TrueType>::VALUE,
					FlagTag<FLAG_RESULT_PROMOTABLE> >::Type*) {
		return TrueSize();
	}

	template<typename U>
	static TrueSize detect(
			typename util::EnableIf<util::IsSame<
					typename U::Policy::ArgsDelayedEvaluable, util::TrueType>::VALUE,
					FlagTag<FLAG_ARGS_DELAYED_EVALUABLE> >::Type*) {
		return TrueSize();
	}

	template<Flag P>
	struct Sub {
		typedef typename util::BoolType<
				sizeof(detect<T>(static_cast<FlagTag<P>*>(NULL))) ==
				sizeof(TrueSize)>::Result Result;
		static const int32_t RESULT_FLAG = (Result::VALUE ? P : 0);
	};

	typedef BasePolicy<
			Sub<FLAG_ALLOCATABLE>::RESULT_FLAG |
			Sub<FLAG_PARTIALLY_FINISHABLE>::RESULT_FLAG |
			Sub<FLAG_WRITER_INITIALIZABLE>::RESULT_FLAG |
			Sub<FLAG_RESULT_PROMOTABLE>::RESULT_FLAG |
			Sub<FLAG_ARGS_DELAYED_EVALUABLE>::RESULT_FLAG> Result;
};

template<size_t M>
struct SQLExprs::ExprUtils::VariantTraits {
	static const size_t MULTI_COLUMN_TYPE_ORDINAL = M;
	static const AggregationPhase AGGR_PHASE = SQLType::AGG_PHASE_ADVANCE_PIPE;
	static const DecrementalType DECREMENTAL_TYPE = ExprSpec::DECREMENTAL_NONE;

	template<typename S, typename A0, typename A1, typename A2>
	struct AggregationValuesResolver {
		typedef util::FalseType Type;
	};
};

template<
		size_t M,
		SQLType::AggregationPhase Phase,
		SQLExprs::ExprSpec::DecrementalType D>
struct SQLExprs::ExprUtils::AggregationVariantTraits {
	static const size_t MULTI_COLUMN_TYPE_ORDINAL = M;
	static const AggregationPhase AGGR_PHASE = Phase;
	static const DecrementalType DECREMENTAL_TYPE = D;

	template<typename S, typename A0, typename A1, typename A2>
	struct AggregationValuesResolver {
		typedef typename S::AggregationValuesBase::
				template AggregationValues<A0, A1, A2> Type;
	};
};

template<SQLExprs::ExprType T, typename S, typename V>
struct SQLExprs::ExprUtils::FunctorTraits {
	static const ExprType EXPR_TYPE = T;
	typedef typename S::template Spec<T>::Type SpecType;
	typedef typename util::Conditional<
			util::IsSame<V, void>::VALUE, VariantTraits<>, V>::Type VariantType;
	static const size_t MULTI_COLUMN_TYPE_ORDINAL =
			VariantType::MULTI_COLUMN_TYPE_ORDINAL;
	static const DecrementalType DECREMENTAL_TYPE =
			VariantType::DECREMENTAL_TYPE;

	enum {
		FOR_AGGR = (SpecType::AggrListType::MIN_COUNT > 0),
		FOR_DECREMENTAL = (DECREMENTAL_TYPE != ExprSpec::DECREMENTAL_NONE),
		FOR_DECREMENTAL_FORWARD =
				(DECREMENTAL_TYPE == ExprSpec::DECREMENTAL_FORWARD),
		FOR_COMP = (FOR_AGGR ?
				!!SpecType::IN_NULLABLE_ANY :
				(SpecType::IN_ANY_PROMOTABLE && SpecType::IN_MIN_COUNT > 1)),
		FOR_MERGE =
				(VariantType::AGGR_PHASE == SQLType::AGG_PHASE_MERGE_PIPE),
		FOR_FINISH =
				(VariantType::AGGR_PHASE == SQLType::AGG_PHASE_MERGE_FINISH),
		RESULT_EMPTY = (FOR_AGGR && !FOR_FINISH),
		IN_NULLABLE = (SpecType::IN_NULLABLE && !FOR_AGGR),
		IN_NULL_CHECKABLE =
				((SpecType::IN_NULLABLE || SpecType::IN_NULLABLE_ANY) &&
				!FOR_AGGR)
	};

	typedef
			typename util::Conditional<
					FOR_MERGE, typename SpecType::AggrListType,
			typename util::Conditional<
					FOR_FINISH, ExprSpecBase::InList<>,
			typename SpecType::InListType>::Type>::Type InListType;

	typedef typename util::BoolType<IN_NULLABLE>::Result InNullableType;
	typedef typename util::BoolType<
			IN_NULL_CHECKABLE>::Result InNullCheckableType;

	template<size_t N>
	struct ArgTraits {
		typedef typename InListType::template At<N> ArgInType;

		enum {
			BASE_ARG_NULLABLE = ArgInType::TYPE_NULLABLE,
			AGGR_ARG_NULLABLE =
					SpecType::AggrListType::template At<N>::TYPE_NULLABLE,
			ARG_NULLABLE =
					IN_NULLABLE || (FOR_AGGR && AGGR_ARG_NULLABLE && N > 0)
		};

		typedef typename ArgInType::template TypeAt<
				MULTI_COLUMN_TYPE_ORDINAL> BaseType;
		static const TupleColumnType IN_TYPE =
				BaseType::EXPANDED_COLUMN_TYPE;

		typedef typename util::BoolType<ARG_NULLABLE>::Result ArgNullableType;
		typedef ArgHolder<IN_TYPE, ArgNullableType> Type;

		typedef typename SpecType::InListType::template At<
				N>::template TypeAt<MULTI_COLUMN_TYPE_ORDINAL> OrgBaseType;
		static const TupleColumnType ORG_IN_TYPE =
				OrgBaseType::EXPANDED_COLUMN_TYPE;

		typedef typename SpecType::AggrListType::template At<
				N>::template TypeAt<MULTI_COLUMN_TYPE_ORDINAL> AggrBaseType;
		static const TupleColumnType AGGR_IN_TYPE =
				AggrBaseType::EXPANDED_COLUMN_TYPE;

		typedef typename SQLValues::TypeUtils::Traits<
				AGGR_IN_TYPE>::ValueType AggrBaseValueType;
		typedef typename util::Conditional<
				AGGR_IN_TYPE == TupleTypes::TYPE_NULL, util::FalseType,
				typename util::Conditional<
						AGGR_ARG_NULLABLE,
						std::pair<AggrBaseValueType, bool>, AggrBaseValueType
						>::Type>::Type AggrValueType;
	};

	typedef typename VariantType::template AggregationValuesResolver<
			S,
			typename ArgTraits<0>::AggrValueType,
			typename ArgTraits<1>::AggrValueType,
			typename ArgTraits<2>::AggrValueType>::Type AggrValuesType;

	static const TupleColumnType IN_LAST_TYPE =
			ArgTraits<SpecType::IN_LAST_POS>::IN_TYPE;

	static const TupleColumnType BASE_RESULT_TYPE =
			(SpecType::TYPE_FLAGS & ExprSpec::FLAG_INHERIT1) != 0 ?
					(ArgTraits<0>::ORG_IN_TYPE) :
			(SpecType::TYPE_FLAGS & ExprSpec::FLAG_INHERIT2) != 0 ?
					(ArgTraits<1>::ORG_IN_TYPE) :
					SpecType::RESULT_TYPE;
	static const TupleColumnType RESULT_TYPE = RESULT_EMPTY ?
			static_cast<TupleColumnType>(TupleTypes::TYPE_NULL) :
			BASE_RESULT_TYPE;

	enum {
		RESULT_WRITER_AVAILABLE =
				SQLValues::TypeUtils::Traits<RESULT_TYPE>::WRITER_AVAILABLE,
		RESULT_NO_ANY = !RESULT_EMPTY &&
				(RESULT_TYPE != static_cast<TupleColumnType>(TupleTypes::TYPE_ANY)),
		RESULT_NULLABLE = !RESULT_EMPTY && (FOR_AGGR ?
				!SpecType::RESULT_NON_NULLABLE : !!SpecType::RESULT_NULLABLE),
		IN_REPEATABLE = !RESULT_EMPTY && SpecType::IN_REPEATABLE
	};

	typedef typename SQLValues::TypeUtils::Traits<
			RESULT_TYPE>::LocalValueType NormalRetType;
	typedef typename SQLValues::TypeUtils::Traits<
			RESULT_TYPE>::WritableType WritableRetType;

	typedef typename util::Conditional<
			RESULT_WRITER_AVAILABLE,
			WritableRetType&, NormalRetType>::Type BaseRetType;
	typedef typename util::Conditional<
			RESULT_NO_ANY, std::pair<NormalRetType, bool>,
			NormalRetType>::Type BaseNullableNormalRetType;
	typedef typename util::Conditional<
			RESULT_WRITER_AVAILABLE, WritableRetType*,
			BaseNullableNormalRetType>::Type BaseNullableRetType;

	typedef typename util::Conditional<
			RESULT_NULLABLE, BaseNullableRetType, BaseRetType>::Type RetType;

	typedef typename util::Conditional<
			FOR_COMP,
			ComparableExpressionBase,
			Expression>::Type NonAggrSourceExpressionType;
	typedef typename util::Conditional<
			FOR_AGGR,
			AggregationExpressionBase,
			NonAggrSourceExpressionType>::Type SourceExpressionType;

	typedef typename util::BoolType<
			FOR_DECREMENTAL_FORWARD>::Result ForwardDecrementalType;
};

template<typename F, typename Traits>
class SQLExprs::ExprUtils::BasicEvaluator {
private:
	typedef typename FunctorPolicy::PolicyDetector<F>::Result PolicyType;

	enum {
		WRITER_AVAILABLE = SQLValues::TypeUtils::Traits<
				Traits::RESULT_TYPE>::WRITER_AVAILABLE,
		WRITER_INITIALIZABLE =
				(WRITER_AVAILABLE && PolicyType::WriterInitializable::VALUE),
		CONTEXT_CUSTOMIZED =
				PolicyType::Allocatable::VALUE ||
				PolicyType::PartiallyFinishable::VALUE ||
				Traits::RESULT_WRITER_AVAILABLE ||
				Traits::FOR_AGGR ||
				Traits::FOR_COMP,
		ARGS_DELAYED = PolicyType::ArgsDelayedEvaluable::VALUE,
		VAR_ARGS_ACCEPTABLE = ARGS_DELAYED || Traits::IN_REPEATABLE
	};

	typedef typename Traits::SourceExpressionType SourceExpressionType;

	typedef typename util::BoolType<
			!Traits::FOR_AGGR && VAR_ARGS_ACCEPTABLE
			>::Result VarArgsAcceptable;
	typedef typename util::BoolType<
			Traits::FOR_AGGR && VAR_ARGS_ACCEPTABLE
			>::Result AggrVarArgsAcceptable;

	typedef typename util::Conditional<
			PolicyType::Allocatable::VALUE,
			util::StackAllocator, util::FalseType>::Type AllocatorType;
	typedef ResultWriter<
			typename util::Conditional<
					WRITER_AVAILABLE,
					typename SQLValues::TypeUtils::template Traits<
							Traits::RESULT_TYPE>::TypeTag, void>::Type,
			WRITER_INITIALIZABLE> RetWriterType;

	typedef typename util::Conditional<
			Traits::FOR_COMP,
			BasicComparatorRef, EmptyComparatorRef>::Type ComparatorType;

	template<bool D, typename G = typename Traits::ForwardDecrementalType>
	struct AggregationFunctionContextOf {
		typedef DecrementalFunctionContext<
				AllocatorType, RetWriterType, ComparatorType, G> Type;
	};

	template<typename G>
	struct AggregationFunctionContextOf<false, G> {
		typedef AggregationFunctionContext<
				AllocatorType, RetWriterType, ComparatorType> Type;
	};

	typedef typename AggregationFunctionContextOf<
			Traits::FOR_DECREMENTAL>::Type BaseAggregationFunctionContext;
	typedef typename util::Conditional<
			Traits::FOR_AGGR,
			BaseAggregationFunctionContext,
			CustomFunctionContext<AllocatorType, RetWriterType, ComparatorType>
			>::Type BaseFunctionContext;

	typedef typename util::Conditional<
			CONTEXT_CUSTOMIZED,
			BaseFunctionContext, NormalFunctionContext>::Type FunctionContext;
	typedef typename util::Conditional<
			CONTEXT_CUSTOMIZED,
			FunctionContext, ExprContext&>::Type LocalFunctionContext;
	typedef typename util::BoolType<
			CONTEXT_CUSTOMIZED>::Result ContextCustomized;

	typedef typename Traits::AggrValuesType AggrValuesType;
	typedef typename Traits::RetType RetType;

	typedef ArgHolder<
			(ARGS_DELAYED ?
					static_cast<TupleColumnType>(TupleTypes::TYPE_ANY) :
			VAR_ARGS_ACCEPTABLE ? Traits::IN_LAST_TYPE :
					static_cast<TupleColumnType>(TupleTypes::TYPE_NULL)),
			typename Traits::InNullableType> VarArg;

	typedef F FunctorType;

public:
	BasicEvaluator(ExprContext &baseCxt, const SourceExpressionType &expr);

	const Expression& top();
	const Expression& next();

	FunctorType& func() { return func_; }
	FunctionContext& context() { return localFuncCxt_.getFunctionContext(); }
	AggrValuesType aggr() { return AggrValuesType(); }

	bool check(const TupleValue &value);

	TupleValue operator()(RetType ret);
	void operator()();

private:
	typedef BaseAllocatorScope<
			typename PolicyType::Allocatable> AllocatorScope;

	typedef BaseArgChecker<
			typename Traits::InNullCheckableType> ArgChecker;

	typedef BaseFinishChecker<
			typename PolicyType::PartiallyFinishable> FinishChecker;

	typedef BaseResultValue<
			Traits::RESULT_TYPE,
			typename PolicyType::ResultPromotable> ResultValue;


	template<typename Expr>
	static std::pair<ExprContext*, const Expr*> toContextSource(
			ExprContext &baseCxt, const Expr &expr, const util::TrueType&) {
		return std::make_pair(&baseCxt, &expr);
	}

	static ExprContext& toContextSource(
			ExprContext &baseCxt, const Expression&, const util::FalseType&) {
		return baseCxt;
	}

	template<typename G>
	TupleValue evalVarArgs(G &func, const util::TrueType&);

	template<typename G>
	TupleValue evalVarArgs(G&, const util::FalseType&) {
		assert(false);
		return TupleValue();
	}

	template<typename G>
	void evalDelayedAggrArgs(G &func, const util::TrueType&);

	template<typename G>
	void evalDelayedAggrArgs(G&, const util::FalseType&) {
		assert(false);
	}

	FunctorType func_;
	const Expression &expr_;
	const Expression *argExpr_;

	LocalFunctionContext localFuncCxt_;
	ExprContext &baseCxt_;

	AllocatorScope allocScope_;
	ArgChecker argChecker_;
};

class SQLExprs::PlanningExpression : public SQLExprs::Expression {
public:
	PlanningExpression(ExprFactoryContext &cxt, const ExprCode &code);
	virtual TupleValue eval(ExprContext &cxt) const;

private:
	ExprCode code_;
};

class SQLExprs::ComparableExpressionBase : public SQLExprs::Expression {
public:
	ComparableExpressionBase();

	void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	const SQLValues::ValueComparator::Arranged* getComparator() const {
		return comparator_;
	}

	static const SQLValues::ValueComparator::Arranged* arrangeComparator(
			ExprFactoryContext &cxt, const ExprSpec &spec);

private:
	const SQLValues::ValueComparator::Arranged *comparator_;
};

class SQLExprs::AggregationExpressionBase : public SQLExprs::Expression {
public:
	AggregationExpressionBase();

	void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	const SQLValues::ValueComparator::Arranged* getComparator() const {
		return comparator_;
	}

	const SummaryColumn* getAggregationColumns() const;

private:
	SummaryColumn aggrColumns_[ExprSpec::AGGR_LIST_SIZE];
	const SQLValues::ValueComparator::Arranged *comparator_;
};

template<typename F, typename Traits>
class SQLExprs::BasicExpressionBase :
		public util::Conditional<
						Traits::FOR_AGGR, SQLExprs::AggregationExpressionBase,
				typename util::Conditional<
						Traits::FOR_COMP, SQLExprs::ComparableExpressionBase,
				SQLExprs::Expression>::Type>::Type {
protected:
	typedef ExprUtils::BasicEvaluator<F, Traits> Evaluator;
	typedef typename Traits::template ArgTraits<0>::Type A0;
	typedef typename Traits::template ArgTraits<1>::Type A1;
	typedef typename Traits::template ArgTraits<2>::Type A2;
	typedef typename Traits::template ArgTraits<3>::Type A3;
	typedef typename Traits::template ArgTraits<4>::Type A4;
	typedef typename Traits::template ArgTraits<5>::Type A5;
	typedef typename Traits::template ArgTraits<6>::Type A6;
	typedef typename Traits::template ArgTraits<7>::Type A7;
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 0, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		typename Base::Evaluator e(cxt, *this);
		return e(e.func()(e.context()));
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 1, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			return e(e.func()(e.context(), typename Base::A0(v0)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 2, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			return e(e.func()(
					e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 3, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			const TupleValue &v2 = e.next().eval(cxt); if (!e.check(v2)) break;
			return e(e.func()(
					e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 4, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			const TupleValue &v2 = e.next().eval(cxt); if (!e.check(v2)) break;
			const TupleValue &v3 = e.next().eval(cxt); if (!e.check(v3)) break;
			return e(e.func()(
					e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)(), typename Base::A3(v3)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 5, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			const TupleValue &v2 = e.next().eval(cxt); if (!e.check(v2)) break;
			const TupleValue &v3 = e.next().eval(cxt); if (!e.check(v3)) break;
			const TupleValue &v4 = e.next().eval(cxt); if (!e.check(v4)) break;
			return e(e.func()(e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)(), typename Base::A3(v3)(),
					typename Base::A4(v4)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 6, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			const TupleValue &v2 = e.next().eval(cxt); if (!e.check(v2)) break;
			const TupleValue &v3 = e.next().eval(cxt); if (!e.check(v3)) break;
			const TupleValue &v4 = e.next().eval(cxt); if (!e.check(v4)) break;
			const TupleValue &v5 = e.next().eval(cxt); if (!e.check(v5)) break;
			return e(e.func()(
					e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)(), typename Base::A3(v3)(),
					typename Base::A4(v4)(), typename Base::A5(v5)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 7, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			const TupleValue &v2 = e.next().eval(cxt); if (!e.check(v2)) break;
			const TupleValue &v3 = e.next().eval(cxt); if (!e.check(v3)) break;
			const TupleValue &v4 = e.next().eval(cxt); if (!e.check(v4)) break;
			const TupleValue &v5 = e.next().eval(cxt); if (!e.check(v5)) break;
			const TupleValue &v6 = e.next().eval(cxt); if (!e.check(v6)) break;
			return e(e.func()(e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)(), typename Base::A3(v3)(),
					typename Base::A4(v4)(), typename Base::A5(v5)(),
					typename Base::A6(v6)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::BasicExpression<F, 8, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const {
		for (typename Base::Evaluator e(cxt, *this);;) {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt); if (!e.check(v1)) break;
			const TupleValue &v2 = e.next().eval(cxt); if (!e.check(v2)) break;
			const TupleValue &v3 = e.next().eval(cxt); if (!e.check(v3)) break;
			const TupleValue &v4 = e.next().eval(cxt); if (!e.check(v4)) break;
			const TupleValue &v5 = e.next().eval(cxt); if (!e.check(v5)) break;
			const TupleValue &v6 = e.next().eval(cxt); if (!e.check(v6)) break;
			const TupleValue &v7 = e.next().eval(cxt); if (!e.check(v7)) break;
			return e(e.func()(e.context(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)(), typename Base::A3(v3)(),
					typename Base::A4(v4)(), typename Base::A5(v5)(),
					typename Base::A6(v6)(), typename Base::A7(v7)()));
		}
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::AggregationExpression<F, -1, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const{
		typename Base::Evaluator e(cxt, *this);
		e();
		return e(e.func()(e.context(), e.aggr()));
	}
};

template<typename F, typename Traits>
class SQLExprs::AggregationExpression<F, 0, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const{
		typename Base::Evaluator e(cxt, *this);
		e.func()(e.context(), e.aggr());
		e();
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::AggregationExpression<F, 1, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const{
		typename Base::Evaluator e(cxt, *this);
		do {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			e.func()(e.context(), e.aggr(), typename Base::A0(v0)());
			e();
		}
		while (false);
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::AggregationExpression<F, 2, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const{
		typename Base::Evaluator e(cxt, *this);
		do {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt);
			e.func()(
					e.context(), e.aggr(),
					typename Base::A0(v0)(), typename Base::A1(v1)());
			e();
		}
		while (false);
		return TupleValue();
	}
};

template<typename F, typename Traits>
class SQLExprs::AggregationExpression<F, 3, Traits> :
		public SQLExprs::BasicExpressionBase<F, Traits> {
	typedef BasicExpressionBase<F, Traits> Base;
	virtual TupleValue eval(ExprContext &cxt) const{
		typename Base::Evaluator e(cxt, *this);
		do {
			const TupleValue &v0 = e.top().eval(cxt); if (!e.check(v0)) break;
			const TupleValue &v1 = e.next().eval(cxt);
			const TupleValue &v2 = e.next().eval(cxt);
			e.func()(
					e.context(), e.aggr(),
					typename Base::A0(v0)(), typename Base::A1(v1)(),
					typename Base::A2(v2)());
			e();
		}
		while (false);
		return TupleValue();
	}
};

template<typename S>
class SQLExprs::CustomExprRegistrar : public SQLExprs::ExprRegistrar {
public:
	virtual void operator()() const = 0;

protected:
	template<ExprType T, typename E> void add() const {
		addDirect(T, SpecOf<S, T>()(), &createDirect<E>);
	}

	template<ExprType T, typename E> void addNoArgs() const {
		addDirect(T, SpecOf<S, T>()(), &createDirectNoArgs<E>);
	}

	template<ExprType T> void addNonEvaluable() const {
		addDirect(T, SpecOf<S, T>()(), NULL);
	}

	template<ExprType T, typename E> void addVariants() const {
		const ExprSpec &spec = SpecOf<S, T>()();
		addDirectVariants<T, E>(&spec);
	}
};

template<typename S>
class SQLExprs::BasicExprRegistrar : public SQLExprs::ExprRegistrar {
public:
	virtual void operator()() const = 0;

protected:
	template<ExprType T, typename F> void add() const {
		resolveTable<T, F>();
		addDirect(T, SpecOf<S, T>()(), &create<T, F>);
	}

private:
	template<ExprType T, typename F>
	static Expression& create(
			ExprFactoryContext &cxt, const ExprCode &code) {
		return resolveFactoryFunc(
				cxt, resolveTable<T, F>(), SpecOf<S, T>()())(cxt, code);
	}

	template<ExprType T, typename F>
	static const BasicFuncTable& resolveTable() {
		static const BasicFuncTable table(createTable<T, F>());
		return table;
	};

	template<ExprType T, typename F>
	static BasicFuncTable createTable() {
		const FuncTableByTypes list[] = {
			createSubTable<T, F, 0>(),
			createSubTable<T, F, 1>(),
			createSubTable<T, F, 2>()
		};
		const FactoryFunc checker = createCheckerFunc<T, F>();
		return BasicFuncTable(FuncTableByCounts(list), checker);
	};

	template<ExprType T, typename F, size_t Opt>
	static FuncTableByTypes createSubTable() {
		const FactoryFunc list[] = {
			createFactoryFunc<T, F, Opt, 0>(),
			createFactoryFunc<T, F, Opt, 1>(),
			createFactoryFunc<T, F, Opt, 2>()
		};
		return FuncTableByTypes(list);
	}

	template<ExprType T, typename F>
	static FactoryFunc createCheckerFunc() {
		typedef typename S::template Spec<T>::Type SpecType;
		typedef typename util::BoolType<
				SpecType::IN_ARG_CHECKING>::Result ArgChecking;
		return createCheckerFuncDetail<T, F>(ArgChecking());
	}

	template<ExprType T, typename F>
	static FactoryFunc createCheckerFuncDetail(const util::TrueType&) {
		return createFactoryFunc<T, typename F::Checker, 0, 0>();
	}

	template<ExprType, typename>
	static FactoryFunc createCheckerFuncDetail(const util::FalseType&) {
		return FactoryFunc();
	}

	template<ExprType T, typename F, size_t Opt, size_t M>
	static FactoryFunc createFactoryFunc() {
		typedef typename S::template Spec<T>::Type SpecType;

		const size_t multiColumnTypeOrdinal = ((
				(M == 2 && SpecType::MULTI_COLUMN_TYPE2) ||
				(M == 1 && SpecType::MULTI_COLUMN_TYPE1)) ? M : 0);
		typedef typename util::Conditional<
				(multiColumnTypeOrdinal > 0),
				ExprUtils::VariantTraits<multiColumnTypeOrdinal>,
				void>::Type VariantTraitsType;

		const bool delayed = ExprUtils::FunctorPolicy::PolicyDetector<
				F>::Result::ArgsDelayedEvaluable::VALUE;
		const int32_t argCount = static_cast<int32_t>(delayed ? 0 :
				SpecType::IN_MIN_COUNT +
						(Opt <= SpecType::IN_OPT_COUNT ? Opt : 0));

		typedef typename ExprUtils::FunctorTraits<
				T, S, VariantTraitsType> Traits;
		typedef BasicExpression<F, argCount, Traits> Expr;
		typedef typename util::BoolType<Traits::FOR_COMP>::Result ForComp;
		return createFactoryFuncDirect<Expr>(ForComp());
	}

	template<typename Expr>
	static FactoryFunc createFactoryFuncDirect(const util::TrueType&) {
		return &createDirectCustom<Expr>;
	}

	template<typename Expr>
	static FactoryFunc createFactoryFuncDirect(const util::FalseType&) {
		return &createDirectNoArgs<Expr>;
	}
};

template<typename S>
class SQLExprs::AggregationExprRegistrar : public SQLExprs::ExprRegistrar {
public:
	virtual void operator()() const = 0;

protected:
	template<ExprType T, typename F> void add() const {
		resolveTable<T, F>();
		ExprSpec spec = SpecOf<S, T>()();
		if (util::IsSame<typename F::Finish, void>::VALUE) {
			spec.flags_ |= ExprSpec::FLAG_AGGR_FINISH_DEFAULT;
		}
		addDirect(T, spec, &create<T, F>);
	}

private:
	typedef ExprSpec::DecrementalType DecrementalType;

	template<ExprType T, typename F>
	static Expression& create(
			ExprFactoryContext &cxt, const ExprCode &code) {
		return resolveFactoryFunc(
				cxt, resolveTable<T, F>(), SpecOf<S, T>()())(cxt, code);
	}

	template<ExprType T, typename F>
	static const AggrFuncTable& resolveTable() {
		static const AggrFuncTable table(createTable<T, F>());
		return table;
	}

	template<ExprType T, typename F>
	static AggrFuncTable createTable() {
		const FactoryFunc checker = createCheckerFunc<T, F>();
		return AggrFuncTable(FuncTableByAggr(
				createSubTable<T, typename F::Advance, 0>(),
				createSubTable<T, typename F::Merge, 1>(),
				createSubTable<T, typename F::Finish, 2>(),
				createDecrementalSubTable<T, F, true>(),
				createDecrementalSubTable<T, F, false>()), checker);
	}

	template<ExprType T, typename F, bool Forward>
	static FuncTableByTypes createDecrementalSubTable() {
		typedef typename SpecOf<S, T>::SpecType SpecType;

		const size_t phase = 0;
		const DecrementalType decrBase = (Forward ?
				ExprSpec::DECREMENTAL_FORWARD :
				ExprSpec::DECREMENTAL_BACKWARD);
		const DecrementalType decr = (SpecType::AGGR_DECREMENTAL ?
				decrBase : ExprSpec::DECREMENTAL_NONE);
		return createSubTableDetail<T, typename F::Advance, phase, decr>();
	}

	template<ExprType T, typename G, size_t Phase>
	static FuncTableByTypes createSubTable() {
		const DecrementalType decr = ExprSpec::DECREMENTAL_NONE;
		return createSubTableDetail<T, G, Phase, decr>();
	}

	template<ExprType T, typename G, size_t Phase, DecrementalType D>
	static FuncTableByTypes createSubTableDetail() {
		typedef typename SpecOf<S, T>::SpecType SpecType;

		const bool available = (!util::IsSame<G, void>::VALUE);
		typedef typename util::BoolType<available>::Result Available;

		UTIL_STATIC_ASSERT(Available::VALUE || Phase == 2 ||
				(Phase == 1 && (SpecType::TYPE_FLAGS & (
						SQLExprs::ExprSpec::FLAG_WINDOW_ONLY |
						SQLExprs::ExprSpec::FLAG_PSEUDO_WINDOW)) != 0));

		const FactoryFunc list[] = {
			createFactoryFunc<T, G, Phase, 0, D>(Available()),
			createFactoryFunc<T, G, Phase, 1, D>(Available()),
			createFactoryFunc<T, G, Phase, 2, D>(Available())
		};
		return FuncTableByTypes(list);
	}

	template<ExprType T, typename F>
	static FactoryFunc createCheckerFunc() {
		typedef typename S::template Spec<T>::Type SpecType;
		typedef typename util::BoolType<
				(!util::IsSame<F, void>::VALUE) &&
				SpecType::IN_ARG_CHECKING>::Result ArgChecking;
		return createCheckerFuncDetail<T, F>(ArgChecking());
	}

	template<ExprType T, typename F>
	static FactoryFunc createCheckerFuncDetail(const util::TrueType&) {
		typedef typename F::Checker CheckerType;
		const DecrementalType decr = ExprSpec::DECREMENTAL_NONE;
		return createFactoryFunc<T, CheckerType, 0, 0, decr>(util::TrueType());
	}

	template<ExprType, typename>
	static FactoryFunc createCheckerFuncDetail(const util::FalseType&) {
		return FactoryFunc();
	}

	template<ExprType T, typename F, size_t Phase, size_t M, DecrementalType D>
	static FactoryFunc createFactoryFunc(const util::TrueType&) {
		typedef typename S::template Spec<T>::Type SpecType;

		const size_t multiColumnTypeOrdinal = ((
				(M == 2 && SpecType::MULTI_COLUMN_TYPE2) ||
				(M == 1 && SpecType::MULTI_COLUMN_TYPE1)) ? M : 0);
		const AggregationPhase aggrPhase =
				Phase == 1 ? SQLType::AGG_PHASE_MERGE_PIPE :
				Phase == 2 ? SQLType::AGG_PHASE_MERGE_FINISH :
				SQLType::AGG_PHASE_ADVANCE_PIPE;
		typedef ExprUtils::AggregationVariantTraits<
				multiColumnTypeOrdinal, aggrPhase, D> VariantTraitsType;
		typedef typename ExprUtils::FunctorTraits<
				T, S, VariantTraitsType> Traits;

		const bool delayed = ExprUtils::FunctorPolicy::PolicyDetector<
				F>::Result::ArgsDelayedEvaluable::VALUE;
		const size_t minArgCount = Traits::InListType::MIN_COUNT;
		const size_t baseArgCount = (delayed ?
				(minArgCount > 1 ?
						(Traits::FOR_MERGE ? minArgCount - 1 : 1) : 0) :
				minArgCount);
		const int32_t argCount = (Traits::FOR_FINISH ?
				-1 : static_cast<int32_t>(baseArgCount));

		typedef AggregationExpression<F, argCount, Traits> Expr;
		return &createDirectCustom<Expr>;
	}

	template<ExprType, typename, size_t, size_t, DecrementalType>
	static FactoryFunc createFactoryFunc(const util::FalseType&) {
		return FactoryFunc();
	}
};



template<SQLExprs::ExprType T, typename E>
void SQLExprs::ExprRegistrar::addDirectVariants(const ExprSpec *spec) const {
	typedef ExprRegistrar::VariantTraits TraitsType;
	FactoryFunc func = VariantsRegistrarBase<TraitsType>::add<T, E>();
	if (spec != NULL) {
		addDirect(T, *spec, func);
	}
}


template<typename R>
template<size_t T, typename C>
typename SQLExprs::VariantsRegistrarBase<R>::FactoryFunc
SQLExprs::VariantsRegistrarBase<R>::add() {
	typedef typename C::VariantTraits Traits;

	const size_t count = Traits::TOTAL_VARIANT_COUNT;
	VariantTable &table = resolveVariantTable<T, count>();

	const size_t begin = Traits::VARIANT_BEGIN;
	const size_t end = Traits::VARIANT_END;
	UTIL_STATIC_ASSERT(begin < end);
	UTIL_STATIC_ASSERT(end <= count);

	addRange<C, begin, end>(table.first, util::FalseType());

	VariantResolverFunc &resolverRef = table.second;
	if (resolverRef == NULL) {
		resolverRef = &Traits::resolveVariant;
	}
	else {
	}

	return &create<T>;
}

template<typename R>
template<size_t T, typename C, size_t Begin, size_t End>
typename SQLExprs::VariantsRegistrarBase<R>::FactoryFunc
SQLExprs::VariantsRegistrarBase<R>::add() {
	typedef typename C::VariantTraits Traits;

	const size_t count = Traits::TOTAL_VARIANT_COUNT;
	VariantTable &table = resolveVariantTable<T, count>();

	UTIL_STATIC_ASSERT(Begin < End);
	UTIL_STATIC_ASSERT(End <= count);

	addRange<C, Begin, End>(table.first, util::FalseType());

	VariantResolverFunc &resolverRef = table.second;
	if (resolverRef == NULL) {
		resolverRef = &Traits::resolveVariant;
	}
	else {
	}

	return &create<T>;
}

template<typename R>
template<size_t T>
typename SQLExprs::VariantsRegistrarBase<R>::VariantTable&
SQLExprs::VariantsRegistrarBase<R>::getVariantTableRef() {
	static VariantTable table;
	return table;
}

template<typename R>
template<size_t T, size_t N>
typename SQLExprs::VariantsRegistrarBase<R>::VariantTable&
SQLExprs::VariantsRegistrarBase<R>::resolveVariantTable() {
	static FactoryFuncEntry entryListBase[N];

	VariantTable &table = getVariantTableRef<T>();

	VariantEntryList &entryList = table.first;
	if (entryList.first == NULL) {
		entryList = VariantEntryList(entryListBase, N);
	}
	else {
		assert(entryList == VariantEntryList(entryListBase, N));
	}

	return table;
}

template<typename R>
template<typename C, size_t Begin, size_t End>
void SQLExprs::VariantsRegistrarBase<R>::addRange(
		VariantEntryList &entryList, const util::FalseType&) {
	UTIL_STATIC_ASSERT(Begin <= End);
	if (Begin >= End) {
		return;
	}

	const size_t mid = Begin + (End - Begin) / 2;

	const size_t width1 = mid - Begin;
	const size_t width2 = End - mid;

	const size_t begin1 = (width1 > 0 ? Begin : 0);
	const size_t end1 = (width1 > 0 ? mid : 0);

	const size_t begin2 = (width2 > 0 ? mid : 0);
	const size_t end2 = (width2 > 0 ? End : 0);

	typedef typename util::BoolType<(width1 == 1)>::Result Single1;
	typedef typename util::BoolType<(width2 == 1)>::Result Single2;

	addRange<C, begin1, end1>(entryList, Single1());
	addRange<C, begin2, end2>(entryList, Single2());
}

template<typename R>
template<typename C, size_t Begin, size_t End>
void SQLExprs::VariantsRegistrarBase<R>::addRange(
		VariantEntryList &entryList, const util::TrueType&) {
	const size_t index = Begin;
	UTIL_STATIC_ASSERT(index + 1 == End);
	typedef typename C::template VariantAt<index>::VariantType V;
	addAt(entryList, index, &RegistrarTraitsType::template create<V>);
}

template<typename R>
void SQLExprs::VariantsRegistrarBase<R>::addAt(
		VariantEntryList &entryList, size_t index, FactoryFunc func) {
	const bool forRead = false;
	entryList.first[checkTableEntry(entryList, index, forRead)].func_ = func;
}

template<typename R>
template<size_t T>
typename SQLExprs::VariantsRegistrarBase<R>::ObjectType&
SQLExprs::VariantsRegistrarBase<R>::create(
		ContextType &cxt, const CodeType &code) {
	const VariantTable &table = getVariantTableRef<T>();
	return createAt(table, cxt, code);
}

template<typename R>
typename SQLExprs::VariantsRegistrarBase<R>::ObjectType&
SQLExprs::VariantsRegistrarBase<R>::createAt(
		const VariantTable &table, ContextType &cxt, const CodeType &code) {
	const VariantResolverFunc resolver = table.second;
	if (resolver == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const VariantEntryList &entryList = table.first;
	const size_t index = resolver(cxt, code);

	const bool forRead = true;
	FactoryFunc func =
			entryList.first[checkTableEntry(entryList, index, forRead)].func_;
	return func(cxt, code);
}

template<typename R>
size_t SQLExprs::VariantsRegistrarBase<R>::checkTableEntry(
		const VariantEntryList &entryList, size_t index, bool forRead) {
	if (index >= entryList.second) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const FactoryFunc func = entryList.first[index].func_;
	if ((forRead && func == NULL) || (!forRead && func != NULL)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return index;
}


template<typename Alloc, typename W, typename C>
template<typename Expr>
inline SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::CustomFunctionContext(
		const std::pair<ExprContext*, const Expr*> &src) :
		NormalFunctionContext(*src.first),
		alloc_(resolveAllocator(*src.first, AllocatorEnabled())),
		resultWriter_(*src.first, 0),
		comparator_(src.second),
		functionFinished_(false) {
	setBase(src.first);
}

template<typename Alloc, typename W, typename C>
inline typename SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::AllocatorType
SQLExprs::ExprUtils::CustomFunctionContext<Alloc, W, C>::getAllocator() {
	return alloc_;
}

template<typename Alloc, typename W, typename C>
inline typename SQLExprs::ExprUtils::CustomFunctionContext<Alloc, W, C>::WriterRef
SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::initializeResultWriter(uint64_t initialCapacity) {
	return resultWriter_.initialize(getBase(), initialCapacity);
}

template<typename Alloc, typename W, typename C>
inline typename SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::WriterRef
SQLExprs::ExprUtils::CustomFunctionContext<Alloc, W, C>::getResultWriter() {
	return resultWriter_.resolve();
}

template<typename Alloc, typename W, typename C>
inline const typename C::BaseType&
SQLExprs::ExprUtils::CustomFunctionContext<Alloc, W, C>::getComparator() {
	return comparator_.get();
}

template<typename Alloc, typename W, typename C>
inline void SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::finishFunction() {
	functionFinished_ = true;
}

template<typename Alloc, typename W, typename C>
inline bool SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::isFunctionFinished() {
	return functionFinished_;
}

template<typename Alloc, typename W, typename C>
inline util::StackAllocator& SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::resolveAllocator(
		ExprContext &baseCxt, const util::TrueType&) {
	return baseCxt.getAllocator();
}

template<typename Alloc, typename W, typename C>
inline util::FalseType SQLExprs::ExprUtils::CustomFunctionContext<
		Alloc, W, C>::resolveAllocator(ExprContext&, const util::FalseType&) {
	return util::FalseType();
}


template<typename Alloc, typename W, typename C>
inline SQLExprs::ExprUtils::AggregationFunctionContext<
		Alloc, W, C>::AggregationFunctionContext(
		const std::pair<ExprContext*, const AggregationExpressionBase*> &src) :
		CustomFunctionContext<Alloc, W, C>(src),
		aggrColumns_(src.second->getAggregationColumns()) {
}


template<typename Alloc, typename W, typename C, typename F>
inline SQLExprs::ExprUtils::DecrementalFunctionContext<
		Alloc, W, C, F>::DecrementalFunctionContext(
		const std::pair<ExprContext*, const AggregationExpressionBase*> &src) :
		AggregationFunctionContext<Alloc, W, C>(src) {
}


template<typename F, typename Traits>
inline SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::BasicEvaluator(
		ExprContext &baseCxt, const SourceExpressionType &expr) :
		expr_(expr),
		argExpr_(NULL),
		localFuncCxt_(toContextSource(baseCxt, expr, ContextCustomized())),
		baseCxt_(baseCxt),
		allocScope_(baseCxt) {
}

template<typename F, typename Traits>
inline const SQLExprs::Expression&
SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::top() {
	assert(argExpr_ == NULL);
	argExpr_ = &expr_.child();
	return *argExpr_;
}

template<typename F, typename Traits>
inline const SQLExprs::Expression&
SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::next() {
	assert(argExpr_ != NULL);
	argExpr_ = &argExpr_->next();
	return *argExpr_;
}

template<typename F, typename Traits>
inline bool SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::check(
		const TupleValue &value) {
	return argChecker_(value);
}

template<typename F, typename Traits>
TupleValue SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::operator()(
		RetType ret) {
	if (!FinishChecker::check(context()) && VarArgsAcceptable::VALUE) {
		return evalVarArgs(func_, VarArgsAcceptable());
	}

	return ResultValue::of(baseCxt_, &expr_, ret);
}

template<typename F, typename Traits>
void SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::operator()() {
	if (!FinishChecker::check(context())) {
		evalDelayedAggrArgs(func_, AggrVarArgsAcceptable());
	}
}

template<typename F, typename Traits>
template<typename G>
TupleValue SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::evalVarArgs(
		G &func, const util::TrueType&) {
	if (argChecker_()) {
		if (argExpr_ == NULL) {
			argExpr_ = expr_.findChild();
		}
		else {
			argExpr_ = argExpr_->findNext();
		}

		FunctionContext &funcCxt = context();
		for (;; argExpr_ = argExpr_->findNext()) {
			if (argExpr_ == NULL) {
				RetType ret = func(funcCxt);
				return ResultValue::of(baseCxt_, &expr_, ret);
			}

			const TupleValue &value = argExpr_->eval(funcCxt.getBase());
			if (!argChecker_(value)) {
				break;
			}

			RetType ret = func(funcCxt, VarArg(value)());
			if (FinishChecker::check(funcCxt)) {
				return ResultValue::of(baseCxt_, &expr_, ret);
			}
		}
	}

	return TupleValue();
}

template<typename F, typename Traits>
template<typename G>
void SQLExprs::ExprUtils::BasicEvaluator<F, Traits>::evalDelayedAggrArgs(
		G &func, const util::TrueType&) {
	if (argChecker_()) {
		FunctionContext &funcCxt = context();

		if (argExpr_ == NULL) {
			if (Traits::FOR_FINISH) {
				func(funcCxt, aggr(), VarArg(TupleValue())());
				if (FinishChecker::check(funcCxt)) {
					return;
				}
			}

			argExpr_ = expr_.findChild();
		}
		else {
			argExpr_ = argExpr_->findNext();
		}

		for (;; argExpr_ = argExpr_->findNext()) {
			if (argExpr_ == NULL) {
				return;
			}

			const TupleValue &value = argExpr_->eval(funcCxt.getBase());

			func(funcCxt, aggr(), VarArg(value)());
			if (FinishChecker::check(funcCxt)) {
				return;
			}
		}
	}
}


inline const SQLExprs::SummaryColumn*
SQLExprs::AggregationExpressionBase::getAggregationColumns() const {
	return aggrColumns_;
}

#endif
