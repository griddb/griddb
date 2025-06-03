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

#ifndef SQL_VALUE_H_
#define SQL_VALUE_H_

#include "sql_tuple.h"
#include "util/container.h"
#include "util/numeric.h"


typedef TupleList::TupleColumnType TupleColumnType;

struct TupleTypes {
	enum {
		TYPE_BITS_NORMAL_SIZE = TupleList::TYPE_BITS_NORMAL_SIZE,
		TYPE_BITS_SUB_TYPE = TupleList::TYPE_BITS_SUB_TYPE,
		TYPE_BITS_VAR = TupleList::TYPE_BITS_VAR,
		TYPE_BITS_ARRAY = TupleList::TYPE_BITS_ARRAY,
		TYPE_BITS_NULLABLE = TupleList::TYPE_BITS_NULLABLE,
		TYPE_BITS_RESERVED = TupleList::TYPE_BITS_RESERVED,

		TYPE_MASK_FIXED_SIZE = TupleList::TYPE_MASK_FIXED_SIZE,
		TYPE_MASK_SUB = TupleList::TYPE_MASK_SUB,
		TYPE_MASK_ARRAY = TupleList::TYPE_MASK_ARRAY,
		TYPE_MASK_VAR = TupleList::TYPE_MASK_VAR,
		TYPE_MASK_ARRAY_VAR = TupleList::TYPE_MASK_ARRAY_VAR,
		TYPE_MASK_NULLABLE = TupleList::TYPE_MASK_NULLABLE,

		TYPE_BYTE = TupleList::TYPE_BYTE,
		TYPE_SHORT = TupleList::TYPE_SHORT,
		TYPE_INTEGER = TupleList::TYPE_INTEGER,
		TYPE_LONG = TupleList::TYPE_LONG,
		TYPE_FLOAT = TupleList::TYPE_FLOAT,
		TYPE_DOUBLE = TupleList::TYPE_DOUBLE,
		TYPE_TIMESTAMP = TupleList::TYPE_TIMESTAMP,
		TYPE_MICRO_TIMESTAMP = TupleList::TYPE_MICRO_TIMESTAMP,
		TYPE_NANO_TIMESTAMP = TupleList::TYPE_NANO_TIMESTAMP,
		TYPE_BOOL = TupleList::TYPE_BOOL,
		TYPE_STRING = TupleList::TYPE_STRING,
		TYPE_GEOMETRY = TupleList::TYPE_GEOMETRY,
		TYPE_BLOB = TupleList::TYPE_BLOB,

		TYPE_NULL = TupleList::TYPE_NULL,
		TYPE_ANY = TupleList::TYPE_ANY,
		TYPE_NUMERIC = TupleList::TYPE_NUMERIC,

		TYPE_BIT_SIZE_SUB_TYPE = (TYPE_BITS_VAR - TYPE_BITS_SUB_TYPE)
	};
};

struct SQLValues {
	typedef util::VariableSizeAllocator<> VarAllocator;
	typedef TupleValue::VarContext VarContext;

	typedef TupleList::Reader TupleListReader;

	typedef TupleList::ReadableTuple ReadableTuple;
	typedef TupleList::WritableTuple WritableTuple;

	typedef TupleList::Column TupleColumn;
	typedef util::Vector<TupleColumn> TupleColumnList;
	typedef util::Vector<TupleColumnType> ColumnTypeList;

	struct Types;

	struct TypeUtils;
	class TypeSwitcher;

	class ValueContext;
	class ExtValueContext;

	class RefVarContextScope;
	template<bool Checked> class EmptyVarContextScope;

	struct ProfileElement;
	struct ValueProfile;

	class ValueHolder;
	class ValueSetHolder;

	struct Decremental;

	class SummaryColumn;
	class CompColumn;
	typedef util::Vector<CompColumn> CompColumnList;

	class ArrayTuple;
	class SummaryTuple;
	class SummaryTupleSet;

	struct TupleListReaderSource;

	class DigestTupleListReader;
	class DigestReadableTuple;
	class DigestArrayTuple;
	class DigestHasher;

	class ValueAccessor;
	class ValueBasicComparator;
	class ValueComparator;
	class ValueEq;
	class ValueLess;
	class ValueGreater;
	class ValueFnv1aHasher;
	class ValueOrderedDigester;
	class ValueDigester;
	class ValueConverter;
	class ValuePromoter;

	class TupleNullChecker;
	class TupleComparator;
	class TupleRangeComparator;
	class TupleDigester;

	class LongInt;
	class DateTimeElements;

	typedef util::SequenceReader<
			char8_t, const char8_t*, util::UTF8Utils> StringReader;
	typedef util::SequenceUtils::Ref<StringReader> StringReaderRef;
	class StringBuilder;
	class LimitedStringBuilder;

	class LobReader;
	class LobReaderRef;
	typedef TupleValue::LobBuilder LobBuilder;

	template<typename Base, typename T> class LimitedReader;
	template<typename T> class BufferBuilder;

	struct ValueUtils;

	class ReadableTupleRef;
	class ArrayTupleRef;

	class SharedId;
	class SharedIdManager;

	class LatchHolder;
	class BaseLatchTarget;
};

struct SQLValues::Types {
private:
	template<TupleColumnType T, typename V, typename L = V>
	struct Base {
		enum { COLUMN_TYPE = T };
		typedef V ValueType;
		typedef L LocalValueType;
	};

public:
	struct Byte : public Base<TupleTypes::TYPE_BYTE, int8_t> {};
	struct Short : public Base<TupleTypes::TYPE_SHORT, int16_t> {};
	struct Integer : public Base<TupleTypes::TYPE_INTEGER, int32_t> {};
	struct Long : public Base<TupleTypes::TYPE_LONG, int64_t> {};
	struct Float : public Base<TupleTypes::TYPE_FLOAT, float> {};
	struct Double : public Base<TupleTypes::TYPE_DOUBLE, double> {};
	struct TimestampTag : public Base<TupleTypes::TYPE_TIMESTAMP, int64_t> {};
	struct MicroTimestampTag : public Base<
			TupleTypes::TYPE_MICRO_TIMESTAMP, MicroTimestamp> {};
	struct NanoTimestampTag : public Base<
			TupleTypes::TYPE_NANO_TIMESTAMP, TupleNanoTimestamp,
			NanoTimestamp> {};
	struct Bool : public Base<TupleTypes::TYPE_BOOL, bool> {};
	struct String : public Base<TupleTypes::TYPE_STRING, TupleString> {};
	struct Blob : public Base<TupleTypes::TYPE_BLOB, TupleValue> {};
	struct Any : public Base<TupleTypes::TYPE_ANY, TupleValue> {};

	struct Integral {};
	struct Floating {};
	struct Numeric {};
	struct PreciseTimestamp {};

	struct NormalFixed {};
	struct SingleVar {};
	struct MultiVar {};
	struct Sequential {};
	struct VarOrLarge {};

	template<TupleColumnType T>
	struct Of {
		typedef
				typename util::Conditional<T == Byte::COLUMN_TYPE, Byte,
				typename util::Conditional<T == Short::COLUMN_TYPE, Short,
				typename util::Conditional<T == Integer::COLUMN_TYPE, Integer,
				typename util::Conditional<T == Long::COLUMN_TYPE, Long,
				typename util::Conditional<T == Float::COLUMN_TYPE, Float,
				typename util::Conditional<T == Double::COLUMN_TYPE, Double,
				typename util::Conditional<T == TimestampTag::COLUMN_TYPE,
						TimestampTag,
				typename util::Conditional<T == MicroTimestampTag::COLUMN_TYPE,
						MicroTimestampTag,
				typename util::Conditional<T == NanoTimestampTag::COLUMN_TYPE,
						NanoTimestampTag,
				typename util::Conditional<T == Bool::COLUMN_TYPE, Bool,
				typename util::Conditional<T == String::COLUMN_TYPE, String,
				typename util::Conditional<T == Blob::COLUMN_TYPE, Blob,
				typename util::Conditional<T == Any::COLUMN_TYPE, Any,
						void
				>::Type>::Type>::Type>::Type>::Type
				>::Type>::Type>::Type>::Type>::Type
				>::Type>::Type>::Type Type;
	};
};

struct SQLValues::TypeUtils {
public:
	enum TypeCategory {
		TYPE_CATEGORY_BOOL = 1 << 0,
		TYPE_CATEGORY_NUMERIC_FLAG = 1 << 1,
		TYPE_CATEGORY_LONG = (1 << 2) | TYPE_CATEGORY_NUMERIC_FLAG,
		TYPE_CATEGORY_DOUBLE = (1 << 3) | TYPE_CATEGORY_NUMERIC_FLAG,
		TYPE_CATEGORY_TIMESTAMP = 1 << 4,
		TYPE_CATEGORY_STRING = 1 << 5,
		TYPE_CATEGORY_BLOB = 1 << 6,
		TYPE_CATEGORY_NULL = 1 << 7,
		TYPE_CATEGORY_NUMERIC_MASK =
				(TYPE_CATEGORY_LONG | TYPE_CATEGORY_DOUBLE) &
				~TYPE_CATEGORY_NUMERIC_FLAG
	};

	template<TupleColumnType T> struct Traits;
	template<TupleColumnType T1, TupleColumnType T2> struct PairTraits;

	template<typename T> struct ValueTraits;


	static bool isAny(TupleColumnType type);
	static bool isNull(TupleColumnType type);

	static bool isSomeFixed(TupleColumnType type);
	static bool isNormalFixed(TupleColumnType type);
	static bool isLargeFixed(TupleColumnType type);
	static bool isSingleVar(TupleColumnType type);
	static bool isSingleVarOrLarge(TupleColumnType type);
	static bool isArray(TupleColumnType type);
	static bool isLob(TupleColumnType type);

	static bool isIntegral(TupleColumnType type);
	static bool isFloating(TupleColumnType type);
	static bool isTimestampFamily(TupleColumnType type);

	static bool isDeclarable(TupleColumnType type);
	static bool isSupported(TupleColumnType type);

	static bool isNullable(TupleColumnType type);
	static TupleColumnType setNullable(TupleColumnType type, bool nullable);

	static TupleColumnType toNonNullable(TupleColumnType type);
	static TupleColumnType toNullable(TupleColumnType type);

	static size_t getFixedSize(TupleColumnType type);

	static TupleColumnType toComparisonType(TupleColumnType type);
	static TupleColumnType findComparisonType(
			TupleColumnType type1, TupleColumnType type2, bool anyAsNull);
	static TupleColumnType findPromotionType(
			TupleColumnType type1, TupleColumnType type2, bool anyAsNull);

	static TupleColumnType resolveConversionType(
			TupleColumnType src, TupleColumnType desired, bool implicit,
			bool anyAsNull);
	static TupleColumnType findEvaluatingConversionType(
			TupleColumnType src, TupleColumnType desired, bool implicit,
			bool anyAsNull, bool evaluating);
	static TupleColumnType findConversionType(
			TupleColumnType src, TupleColumnType desired, bool implicit,
			bool anyAsNull);

	static TupleColumnType filterColumnType(TupleColumnType type);

	static TypeCategory getStaticTypeCategory(TupleColumnType type);
	static bool isNumerical(TupleColumnType type);
	static util::DateTime::FieldType getTimePrecision(TupleColumnType type);
	static int32_t getTimePrecisionLevel(TupleColumnType type);

	static void setUpTupleInfo(
			TupleList::Info &info, const TupleColumnType *list, size_t count);

	static const char8_t* toString(
			TupleColumnType type, bool nullOnUnknown = false,
			bool internal = false);

private:
	typedef TupleColumnTypeUtils BaseUtils;
};

template<TupleColumnType T>
struct SQLValues::TypeUtils::Traits {
private:
	struct EmptyTag {
		typedef util::FalseType ValueType;
		typedef util::FalseType LocalValueType;
	};

	template<typename Tag>
	struct ValueTypeOf {
		typedef typename util::Conditional<
				util::IsSame<Tag, void>::VALUE,
				EmptyTag, Tag>::Type::ValueType Type;
	};

	template<typename Tag>
	struct LocalValueTypeOf {
		typedef typename util::Conditional<
				util::IsSame<Tag, void>::VALUE,
				EmptyTag, Tag>::Type::LocalValueType Type;
	};

public:
	typedef typename Types::template Of<T>::Type TypeTag;
	typedef typename ValueTypeOf<TypeTag>::Type ValueType;
	typedef typename LocalValueTypeOf<TypeTag>::Type LocalValueType;

	static const bool FOR_ANY = (T == TupleTypes::TYPE_ANY);

	static const bool SIZE_VAR = ((T & TupleTypes::TYPE_MASK_VAR) != 0);
	static const bool SIZE_VAR_EXPLICIT = (SIZE_VAR && !FOR_ANY);

	static const uint32_t FIXED_SIZE = (T & TupleTypes::TYPE_MASK_FIXED_SIZE);
	static const uint32_t SUB_TYPE =
			((T & TupleTypes::TYPE_MASK_SUB) >> TupleTypes::TYPE_BITS_SUB_TYPE);

	static const bool FOR_LARGE_FIXED = (
			!FOR_ANY && !SIZE_VAR &&
			FIXED_SIZE > (1 << TupleTypes::TYPE_BITS_NORMAL_SIZE));
	static const bool FOR_NORMAL_FIXED =
			(!FOR_ANY && !SIZE_VAR && !FOR_LARGE_FIXED);

	static const bool SIZE_VAR_OR_LARGE_EXPLICIT =
			(SIZE_VAR_EXPLICIT || FOR_LARGE_FIXED);


	typedef
			typename util::Conditional<
					T == TupleTypes::TYPE_STRING, StringReader,
			typename util::Conditional<
					T == TupleTypes::TYPE_BLOB, LobReader,
					void>::Type>::Type ReaderType;

	typedef
			typename util::Conditional<
					T == TupleTypes::TYPE_STRING, StringBuilder,
			typename util::Conditional<
					T == TupleTypes::TYPE_BLOB, LobBuilder,
					void>::Type>::Type WriterType;

	typedef typename util::Conditional<
			T == TupleTypes::TYPE_STRING, TupleString::BufferInfo,
			LocalValueType>::Type ReadableSourceType;

	typedef
			typename util::Conditional<
					T == TupleTypes::TYPE_STRING, StringReaderRef,
			typename util::Conditional<
					T == TupleTypes::TYPE_BLOB, LobReaderRef,
					void>::Type>::Type LocalReaderRefType;

	typedef
			typename util::Conditional<
					T == TupleTypes::TYPE_STRING, ValueContext,
			typename util::Conditional<
					T == TupleTypes::TYPE_BLOB, VarContext,
					void>::Type>::Type WriterContextType;

	static const bool FOR_INTEGRAL_EXPLICIT = (
			T == TupleTypes::TYPE_BYTE ||
			T == TupleTypes::TYPE_SHORT ||
			T == TupleTypes::TYPE_INTEGER ||
			T == TupleTypes::TYPE_LONG);

	static const bool FOR_INTEGRAL = (
			FOR_INTEGRAL_EXPLICIT ||
			T == TupleTypes::TYPE_BOOL ||
			T == TupleTypes::TYPE_TIMESTAMP);

	static const bool FOR_FLOATING = (
			T == TupleTypes::TYPE_FLOAT ||
			T == TupleTypes::TYPE_DOUBLE);

	static const bool FOR_NUMERIC_EXPLICIT =
			(FOR_INTEGRAL_EXPLICIT || FOR_FLOATING);
	static const bool FOR_NUMERIC = (FOR_INTEGRAL || FOR_FLOATING);

	static const bool FOR_PRECISE_TIMESTAMP = (
			T == TupleTypes::TYPE_MICRO_TIMESTAMP ||
			T == TupleTypes::TYPE_NANO_TIMESTAMP);
	static const bool FOR_TIMESTAMP_FAMILY = (
			T == TupleTypes::TYPE_TIMESTAMP || FOR_PRECISE_TIMESTAMP);

	static const bool FOR_SINGLE_VAR = (T == TupleTypes::TYPE_STRING);
	static const bool FOR_MULTI_VAR = (T == TupleTypes::TYPE_BLOB);
	static const bool FOR_SEQUENTIAL = (FOR_SINGLE_VAR || FOR_MULTI_VAR);

	static const bool TUPLE_SPECIALIZED = (
			T == TupleTypes::TYPE_INTEGER ||
			T == TupleTypes::TYPE_LONG ||
			T == TupleTypes::TYPE_STRING ||
			FOR_FLOATING ||
			FOR_TIMESTAMP_FAMILY);

	static const bool READABLE_TUPLE_SPECIALIZED = TUPLE_SPECIALIZED;

	static const bool WRITABLE_TUPLE_SPECIALIZED = TUPLE_SPECIALIZED;

	static const uint32_t TIMESTAMP_PRECISION_SCORE =
			(FOR_TIMESTAMP_FAMILY ?
					((FIXED_SIZE << TupleTypes::TYPE_BIT_SIZE_SUB_TYPE) |
							SUB_TYPE) : 0);


	static const bool READER_AVAILABLE =
			!util::IsSame<ReaderType, void>::VALUE;
	static const bool WRITER_AVAILABLE =
			!util::IsSame<WriterType, void>::VALUE;

	typedef typename util::Conditional<
			READER_AVAILABLE, ReaderType, LocalValueType>::Type ReadableType;
	typedef typename util::Conditional<
			WRITER_AVAILABLE, WriterType, LocalValueType>::Type WritableType;

	typedef typename util::Conditional<
			READER_AVAILABLE,
			ReadableType&, const LocalValueType&>::Type ReadableRefType;
	typedef typename util::Conditional<
			WRITER_AVAILABLE,
			WritableType&, const LocalValueType&>::Type WritableRefType;

	typedef typename util::Conditional<
			READER_AVAILABLE,
			ReadableType*, const LocalValueType*>::Type ReadablePtrType;

	typedef
			typename util::Conditional<FOR_NORMAL_FIXED, Types::NormalFixed,
			typename util::Conditional<SIZE_VAR_OR_LARGE_EXPLICIT,
					Types::VarOrLarge,
			typename util::Conditional<FOR_ANY, TypeTag,
					void>::Type>::Type>::Type NormalStorageTag;

	typedef
			typename util::Conditional<(FOR_SINGLE_VAR || FOR_LARGE_FIXED),
					TypeTag,
			typename util::Conditional<FOR_SEQUENTIAL, Types::Sequential,
					NormalStorageTag>::Type>::Type FineNormalStorageTag;

	typedef
			typename util::Conditional<FOR_INTEGRAL, Types::Integral,
			typename util::Conditional<FOR_FLOATING, Types::Floating,
			typename util::Conditional<FOR_PRECISE_TIMESTAMP,
					Types::PreciseTimestamp,
			typename util::Conditional<FOR_SEQUENTIAL, Types::Sequential,
			typename util::Conditional<FOR_ANY, TypeTag,
			void>::Type>::Type>::Type>::Type>::Type BasicComparableTag;

	typedef typename util::Conditional<FOR_SINGLE_VAR,
			TypeTag, BasicComparableTag>::Type FineComparableTag;
	typedef typename util::Conditional<FOR_PRECISE_TIMESTAMP,
			TypeTag, FineComparableTag>::Type ResultFineComparableTag;

	typedef
			typename util::Conditional<
					(FOR_TIMESTAMP_FAMILY || FOR_SEQUENTIAL || FOR_ANY), TypeTag,
			typename util::Conditional<FOR_INTEGRAL, Types::Long,
			typename util::Conditional<FOR_FLOATING, Types::Double,
			void>::Type>::Type>::Type ComparableValueTag;
	typedef typename ValueTypeOf<ComparableValueTag>::Type ComparableValueType;

	typedef
			typename util::Conditional<
					(T == TupleTypes::TYPE_BOOL), Types::Byte::ValueType,
			typename util::Conditional<
					(FOR_NORMAL_FIXED || FOR_LARGE_FIXED), LocalValueType,
			typename util::Conditional<FOR_SEQUENTIAL, void*,
			void>::Type>::Type>::Type StorableValueType;
};

template<TupleColumnType T1, TupleColumnType T2>
struct SQLValues::TypeUtils::PairTraits {
	typedef Traits<T1> Traits1;
	typedef Traits<T2> Traits2;

	static const bool FOR_SAME = (T1 == T2);
	static const bool SOME_ANY = (Traits1::FOR_ANY || Traits2::FOR_ANY);
	static const bool BOTH_ANY = (Traits1::FOR_ANY && Traits2::FOR_ANY);

	static const bool BOTH_INTEGRAL_EXPLICIT =
			(Traits1::FOR_INTEGRAL_EXPLICIT && Traits2::FOR_INTEGRAL_EXPLICIT);
	static const bool BOTH_NUMERIC_EXPLICIT =
			(Traits1::FOR_NUMERIC_EXPLICIT && Traits2::FOR_NUMERIC_EXPLICIT);
	static const bool BOTH_TIMESTAMP_FAMILY =
			(Traits1::FOR_TIMESTAMP_FAMILY && Traits2::FOR_TIMESTAMP_FAMILY);

	static const bool FOR_PROMOTION =
			(FOR_SAME ||
			SOME_ANY ||
			BOTH_NUMERIC_EXPLICIT ||
			BOTH_TIMESTAMP_FAMILY);

	static const bool IMPLICIT_INTEGRAL_CONVERTABLE =
			(!FOR_SAME && Traits1::FOR_INTEGRAL_EXPLICIT &&
			T2 == TupleTypes::TYPE_LONG);
	static const bool IMPLICIT_FLOATING_CONVERTABLE =
			(!FOR_SAME && Traits1::FOR_FLOATING &&
			T2 == TupleTypes::TYPE_DOUBLE);
	static const bool IMPLICIT_TIMESTAMP_CONVERTABLE =
			(!FOR_SAME && BOTH_TIMESTAMP_FAMILY);
	static const bool IMPLICIT_ANY_CONVERTABLE =
			(Traits1::FOR_ANY || !Traits2::FOR_ANY);

	static const bool IMPLICIT_CONVERTABLE =
			(IMPLICIT_INTEGRAL_CONVERTABLE ||
			IMPLICIT_FLOATING_CONVERTABLE ||
			IMPLICIT_TIMESTAMP_CONVERTABLE ||
			IMPLICIT_ANY_CONVERTABLE);
	static const bool IMPLICIT_CONVERTABLE_OR_SAME =
			(IMPLICIT_CONVERTABLE || FOR_SAME);

	typedef typename Traits<
			(FOR_PROMOTION ? ((BOTH_TIMESTAMP_FAMILY ?
					(Traits1::TIMESTAMP_PRECISION_SCORE >=
					Traits2::TIMESTAMP_PRECISION_SCORE) :
					Traits1::FOR_FLOATING) ? T1 : T2) :
			static_cast<TupleColumnType>(TupleTypes::TYPE_NULL))
			>::ComparableValueTag ComparableValueTag;
};

template<typename T>
struct SQLValues::TypeUtils::ValueTraits {
private:
	enum {
		UNMATCH_COLUMN_TYPE = TupleTypes::TYPE_NULL
	};

	struct EmptyTypeMatcher {
		enum {
			TYPE_MATCHED = false,
			COLUMN_TYPE = UNMATCH_COLUMN_TYPE
		};
		typedef EmptyTypeMatcher Type;
	};

	template<typename U, typename Other, int32_t Mode>
	struct TypeMatcherBase {
	private:
		typedef typename util::Conditional<
				util::IsSame<Other, void>::VALUE,
				EmptyTypeMatcher, Other>::Type MaskedOther;

		typedef typename util::Conditional<
					(Mode == 1),
					typename Traits<U::COLUMN_TYPE>::ReadableType,
					typename U::ValueType>::Type MatchingType;

		enum {
			TYPE_MATCHED_SELF = util::IsSame<T, MatchingType>::VALUE
		};

	public:
		typedef typename util::Conditional<
				TYPE_MATCHED_SELF,
				U, typename MaskedOther::Type>::Type Type;

		enum {
			TYPE_MATCHED = (TYPE_MATCHED_SELF || MaskedOther::TYPE_MATCHED),
			COLUMN_TYPE = Type::COLUMN_TYPE
		};
	};

	template<typename U, typename Other>
	struct TypeMatcher {
		typedef TypeMatcherBase<U, Other, 0> BaseType;
		typedef typename BaseType::Type Type;
		enum {
			TYPE_MATCHED = BaseType::TYPE_MATCHED,
			COLUMN_TYPE = BaseType::COLUMN_TYPE
		};
	};

	template<typename U, typename Other>
	struct ReaderMatcher {
		typedef TypeMatcherBase<U, Other, 1> BaseType;
		typedef typename BaseType::Type Type;
		enum {
			TYPE_MATCHED = BaseType::TYPE_MATCHED,
			COLUMN_TYPE = BaseType::COLUMN_TYPE
		};
	};

	static const TupleColumnType COLUMN_TYPE_AS_OTHER =
			TypeMatcher<Types::Bool, void>::COLUMN_TYPE;

public:
	static const TupleColumnType COLUMN_TYPE_AS_NUMERIC =
			TypeMatcher<Types::Byte,
			TypeMatcher<Types::Short,
			TypeMatcher<Types::Integer,
			TypeMatcher<Types::Long,
			TypeMatcher<Types::Float,
			TypeMatcher<Types::Double, void> > > > > >::COLUMN_TYPE;

	static const TupleColumnType COLUMN_TYPE_AS_SEQUENCE =
			TypeMatcher<Types::String,
			TypeMatcher<Types::Blob, void> >::COLUMN_TYPE;

	static const TupleColumnType COLUMN_TYPE_AS_READER =
			ReaderMatcher<Types::String,
			ReaderMatcher<Types::Blob, void> >::COLUMN_TYPE;

	static const TupleColumnType COLUMN_TYPE_AS_VALUE =
			COLUMN_TYPE_AS_NUMERIC != UNMATCH_COLUMN_TYPE ?
					COLUMN_TYPE_AS_NUMERIC :
			COLUMN_TYPE_AS_SEQUENCE != UNMATCH_COLUMN_TYPE ?
					COLUMN_TYPE_AS_SEQUENCE :
			COLUMN_TYPE_AS_OTHER != UNMATCH_COLUMN_TYPE ?
					COLUMN_TYPE_AS_OTHER :
			static_cast<TupleColumnType>(UNMATCH_COLUMN_TYPE);
};

class SQLValues::TypeSwitcher {
public:
	template<
			typename T1, typename T2, typename Nullable, typename V>
	struct TypeTraits {
	private:
		typedef T1 BaseType1;
		typedef typename util::Conditional<
				(util::IsSame<T2, void>::VALUE), T1, T2>::Type BaseType2;

		typedef typename TypeUtils::PairTraits<
				BaseType1::COLUMN_TYPE, BaseType2::COLUMN_TYPE> PairTraits;

		enum {
			FOR_NULLABLE = (util::Conditional<
					(util::IsSame<Nullable, void>::VALUE),
					util::TrueType, Nullable>::Type::VALUE),
			FOR_ANY = TypeUtils::Traits<BaseType1::COLUMN_TYPE>::FOR_ANY,
			FOR_NULLABLE_OR_ANY = (FOR_NULLABLE || FOR_ANY)
		};

	public:
		template<typename U>
		struct VariantRebind {
			typedef TypeTraits<T1, T2, Nullable, U> Type;
		};

		typedef TypeTraits<T1, T2, util::FalseType, V> NonNullableTraitsType;

		typedef BaseType1 Type1;
		typedef BaseType2 Type2;

		typedef typename util::BoolType<FOR_NULLABLE>::Result NullableType;
		typedef typename util::BoolType<FOR_ANY>::Result AnyType;
		typedef typename util::BoolType<
				FOR_NULLABLE_OR_ANY>::Result NullableOrAnyType;

		typedef V VariantTraitsType;

		typedef BaseType1 Type;
		typedef typename PairTraits::ComparableValueTag ComparableValueTag;

		typedef typename Type::ValueType ValueType;
		static const TupleColumnType COLUMN_TYPE = Type::COLUMN_TYPE;
	};

	enum TypeTarget {
		TYPE_TARGET_ALL,
		TYPE_TARGET_FIXED,
		TYPE_TARGET_SINGLE_VAR
	};

	template<
			typename V = void, size_t TypeCount = 1, bool OpDelegated = false,
			bool Promotable = false, int32_t NullableSpecific = 1,
			bool Specific = true, TypeTarget Target = TYPE_TARGET_ALL>
	struct OpTraitsOptions {
		enum {
			TYPE_COUNT = TypeCount,
			OP_DELEGATED = OpDelegated,
			TYPE_PROMOTABLE = Promotable,
			NULLABLE_SPECIFIC = NullableSpecific,
			TYPE_SPECIFIC = Specific
		};
		static const TypeTarget TYPE_TARGET = Target;

		typedef V VariantTraitsType;

		template<typename U>
		struct VariantRebind {
			typedef OpTraitsOptions<
					U, TypeCount, OpDelegated,
					Promotable, NullableSpecific, Specific, Target> Type;
		};

		template<bool D = false>
		struct DelegationRebind {
			typedef OpTraitsOptions<
					V, TypeCount, D,
					Promotable, NullableSpecific, Specific, Target> Type;
		};

		template<
				bool PromotableSub = false, int32_t NullableSpecificSub = 1,
				bool SpecificSub = true, TypeTarget TargetSub = TYPE_TARGET_ALL>
		struct TypesRebind {
			typedef OpTraitsOptions<
					V, TypeCount, OpDelegated,
					PromotableSub, NullableSpecificSub, SpecificSub,
					TargetSub> Type;
		};
	};

	template<
			typename Ret, typename Options = void,
			typename A1 = void, typename A2 = void, typename A3 = void>
	struct OpTraits {
		enum {
			ARG_COUNT = (
					util::IsSame<A1, void>::VALUE ? 0 :
					util::IsSame<A2, void>::VALUE ? 1 :
					util::IsSame<A3, void>::VALUE ? 2 : 3),
			OPTIONS_SPECIFIED = (!util::IsSame<Options, void>::VALUE)
		};

		typedef typename util::Conditional<
				OPTIONS_SPECIFIED,
				Options, OpTraitsOptions<> >::Type OptionsType;
		typedef typename OptionsType::VariantTraitsType VariantTraitsType;
		enum {
			VARIANTS_SPECIFIED = (!util::IsSame<VariantTraitsType, void>::VALUE)
		};

		template<size_t N>
		struct Arg {
			typedef
					typename util::Conditional<(N == 0), A1,
					typename util::Conditional<(N == 1), A2,
					typename util::Conditional<(N == 2), A3,
							void>::Type>::Type>::Type Type;
			typedef typename util::Conditional<
					(N < ARG_COUNT), Type, util::FalseType>::Type MaskedType;
		};

		typedef Ret RetType;

		template<typename Op, bool RetSpecified = false>
		struct Func {
			typedef typename util::Conditional<
					RetSpecified, Op, OpTraits>::Type::RetType SubRetType;

			typedef typename Arg<0>::MaskedType Arg1;
			typedef typename Arg<1>::MaskedType Arg2;
			typedef typename Arg<2>::MaskedType Arg3;

			typedef SubRetType (*Type0)(Op&);
			typedef SubRetType (*Type1)(Op&, Arg1&);
			typedef SubRetType (*Type2)(Op&, Arg1&, Arg2&);
			typedef SubRetType (*Type3)(Op&, Arg1&, Arg2&, Arg3&);

			typedef
					typename util::Conditional<(ARG_COUNT == 0), Type0,
					typename util::Conditional<(ARG_COUNT == 1), Type1,
					typename util::Conditional<(ARG_COUNT == 2), Type2,
					typename util::Conditional<(ARG_COUNT == 3), Type3,
							void>::Type>::Type>::Type>::Type Type;
		};

		template<typename V>
		struct VariantRebind {
			typedef OpTraits<
					Ret, typename OptionsType::template VariantRebind<V>::Type,
					A1, A2, A3> Type;
		};

		template<typename Op, typename R = typename Op::RetType>
		struct ResultRebind {
			typedef OpTraits<R, Options, A1, A2, A3> Type;
		};

		template<typename OptionsSub>
		struct OptionsRebind {
			typedef OpTraits<Ret, OptionsSub, A1, A2, A3> Type;
		};
	};

private:
	template<
			size_t TypeCount, bool Promotable, int32_t NullableSpecific,
			bool Specific, TypeTarget Target>
	struct TypeFilter {
		template<typename T1>
		struct First {
			typedef typename TypeUtils::template Traits<
					T1::COLUMN_TYPE> FirstTraitsType;
			enum {
				FOR_ANY = FirstTraitsType::FOR_ANY,
				SIZE_VAR_OR_LARGE =
						(FOR_ANY || FirstTraitsType::SIZE_VAR_OR_LARGE_EXPLICIT),
				NO_SINGLE_VAR = (FOR_ANY || !FirstTraitsType::FOR_SINGLE_VAR),
				FIRST_ALLOWED = (Specific &&
						!(Target == TYPE_TARGET_FIXED && SIZE_VAR_OR_LARGE) &&
						!(Target == TYPE_TARGET_SINGLE_VAR && NO_SINGLE_VAR))
			};
			typedef typename util::Conditional<
					FIRST_ALLOWED, T1, Types::Any>::Type Type;
		};

		template<typename T1, typename T2>
		struct Second {
		private:
			typedef typename First<T1>::Type Type1;
			typedef typename TypeUtils::template PairTraits<
					Type1::COLUMN_TYPE, T2::COLUMN_TYPE> PairTraits;
			enum {
				FOR_PROMOTION =
						(Promotable && PairTraits::FOR_PROMOTION &&
						!PairTraits::SOME_ANY && !PairTraits::FOR_SAME),
				SECOND_ALLOWED = (TypeCount > 1)
			};
			typedef typename util::Conditional<
					FOR_PROMOTION, T2, Type1>::Type BaseType;

		public:
			typedef typename util::Conditional<
					SECOND_ALLOWED, BaseType, void>::Type Type;
		};

		template<typename T1, typename T2, bool NullableBase>
		struct Nullable {
		private:
			typedef typename First<T1>::Type Type1;
			typedef typename util::Conditional<
					util::IsSame<T2, void>::VALUE, Type1, T2>::Type MaskedType2;
			typedef typename TypeUtils::template PairTraits<
					Type1::COLUMN_TYPE, MaskedType2::COLUMN_TYPE> PairTraits;
			enum {
				NULLABLE_SPECIFIC = (NullableSpecific != 0),
				NULLABLE_ON_ANY = (NullableSpecific == 2),

				NO_ANY = (!PairTraits::SOME_ANY),
				BOTH_ANY = (PairTraits::BOTH_ANY),

				FOR_NON_NULLABLE = (NULLABLE_ON_ANY ? !NullableBase :
						((NULLABLE_SPECIFIC && !NullableBase && NO_ANY) ||
						BOTH_ANY))
			};

		public:
			typedef typename util::Conditional<
					FOR_NON_NULLABLE, util::FalseType, void>::Type Type;
		};
	};

	template<typename Op, typename Traits>
	struct TraitsResolver {
		typedef Traits Type;
	};

	template<typename Op>
	struct TraitsResolver<Op, util::TrueType> {
		typedef typename Op::TraitsType Type;
	};

	template<typename Op>
	struct TraitsResolver<Op, util::FalseType> {
		typedef OpTraitsOptions<void, 1, false, false, 0> Options;
		typedef OpTraits<typename Op::RetType, Options> Type;
	};

	template<typename Op, typename Traits = util::FalseType>
	struct OpTraitsBase {
		typedef typename TraitsResolver<Op, Traits>::Type TraitsType;
		typedef typename TraitsType::OptionsType OptionsType;

		enum {
			ARG_COUNT = TraitsType::ARG_COUNT,
			TYPE_COUNT = OptionsType::TYPE_COUNT,
			OP_DELEGATED = OptionsType::OP_DELEGATED,
			TYPE_PROMOTABLE = OptionsType::TYPE_PROMOTABLE,
			NULLABLE_SPECIFIC = OptionsType::NULLABLE_SPECIFIC,
			TYPE_SPECIFIC = OptionsType::TYPE_SPECIFIC,
			VARIANTS_SPECIFIED = TraitsType::VARIANTS_SPECIFIED,
			FIRST_TYPE_ONLY = (
					(NULLABLE_SPECIFIC == 0) &&
					(VARIANTS_SPECIFIED == 0))
		};
		static const TypeTarget TYPE_TARGET = OptionsType::TYPE_TARGET;

		typedef TypeFilter<
				TYPE_COUNT, TYPE_PROMOTABLE, NULLABLE_SPECIFIC,
				TYPE_SPECIFIC, TYPE_TARGET> FilterType;

		template<size_t N>
		struct Arg {
			typedef typename TraitsType::template Arg<N>::Type Type;
		};

		typedef typename TraitsType::RetType RetType;
		typedef typename TraitsType::template Func<Op>::Type FuncType;
		typedef typename OptionsType::VariantTraitsType VariantTraitsType;

		template<typename Base>
		struct WrappedAt {
			typedef Base TypedOp;
		};

		template<typename T>
		struct TypedOf {
			typedef typename Op::template TypeAt<T> Base;
			typedef typename util::Conditional<
					OP_DELEGATED,
					Base, WrappedAt<Base> >::Type::TypedOp TypedOp;
		};

		template<typename T>
		struct ComposedTypeOf {
			typedef typename TypedOf<T>::TypedOp TypedOp;
		};

		template<typename T1, typename T2, typename Nullable>
		struct TypeAt {
			typedef TypedOf<
					TypeTraits<T1, T2, Nullable, VariantTraitsType> > Base;
			typedef ComposedTypeOf<T1> ComposedBase;

			typedef typename util::Conditional<
					FIRST_TYPE_ONLY,
					ComposedBase, Base>::Type::TypedOp TypedOp;
		};
	};

public:
	explicit TypeSwitcher(
			TupleColumnType type1,
			TupleColumnType type2 = TupleTypes::TYPE_NULL,
			bool nullableAlways = false);

	TypeSwitcher withProfile(ValueProfile *profile) const;

	TypeSwitcher toNonNullable() const;
	TypeSwitcher toNullableAlways() const;

	bool isAny() const;

	template<typename Op>
	typename OpTraitsBase<Op>::FuncType get() const;

	template<typename Op, typename Traits>
	typename OpTraitsBase<Op, Traits>::FuncType getWith() const;

	template<typename Op>
	typename Op::RetType operator()(Op &op) const;

	template<typename Op, typename A1>
	typename Op::RetType operator()(Op &op, A1 &arg1) const {
		OpWithArgs<Op, A1> opWithArgs(op, &arg1);
		return (*this)(opWithArgs);
	}

	template<typename Op, typename A1, typename A2>
	typename Op::RetType operator()(
			Op &op, A1 &arg1, A2 &arg2) const {
		OpWithArgs<Op, A1, A2> opWithArgs(op, &arg1, &arg2);
		return (*this)(opWithArgs);
	}

	template<typename Op, typename A1, typename A2, typename A3>
	typename Op::RetType operator()(
			Op &op, A1 &arg1, A2 &arg2, A3 &arg3) const {
		OpWithArgs<Op, A1, A2, A3> opWithArgs(op, &arg1, &arg2, &arg3);
		return (*this)(opWithArgs);
	}

private:
	template<typename Op, typename A1, typename A2 = void, typename A3 = void>
	struct OpWithArgs {
		typedef OpTraitsBase<
				Op, OpTraits<typename Op::RetType, void, A1, A2, A3> > TraitsType;
		typedef typename TraitsType::RetType RetType;
		template<typename T, size_t = TraitsType::ARG_COUNT> struct TypeAt {
		};
		template<typename T> struct TypeAt<T, 1> {
			explicit TypeAt(const OpWithArgs &base) : base_(base) {}
			RetType operator()() const {
				return typename TraitsType::template ComposedTypeOf<T>::TypedOp(
						base_.op_)(*base_.arg1_);
			}
			const OpWithArgs &base_;
		};
		template<typename T> struct TypeAt<T, 2> {
			explicit TypeAt(const OpWithArgs &base) : base_(base) {}
			RetType operator()() const {
				return typename TraitsType::template ComposedTypeOf<T>::TypedOp(
						base_.op_)(*base_.arg1_, *base_.arg2_);
			}
			const OpWithArgs &base_;
		};
		template<typename T> struct TypeAt<T, 3> {
			explicit TypeAt(const OpWithArgs &base) : base_(base) {}
			RetType operator()() const {
				return typename TraitsType::template ComposedTypeOf<T>::TypedOp(
						base_.op_)(*base_.arg1_, *base_.arg2_, *base_.arg3_);
			}
			const OpWithArgs &base_;
		};
		OpWithArgs(
				Op &op, A1 *arg1, A2 *arg2 = NULL, A3 *arg3 = NULL) :
				op_(op),
				arg1_(arg1),
				arg2_(arg2),
				arg3_(arg3) {
		}
		Op &op_;
		A1 *arg1_;
		A2 *arg2_;
		A3 *arg3_;
	};

	template<
			typename TypedOp, typename Op, typename RetType,
			typename A1, typename A2, typename A3>
	struct Executor {
		static RetType execute(Op &op, A1 &arg1, A2 &arg2, A3 &arg3) {
			return TypedOp(op)(arg1, arg2, arg3);
		}
	};

	template<
			typename TypedOp, typename Op, typename RetType,
			typename A1, typename A2>
	struct Executor<TypedOp, Op, RetType, A1, A2, void> {
		static RetType execute(Op &op, A1 &arg1, A2 &arg2) {
			return TypedOp(op)(arg1, arg2);
		}
	};

	template<typename TypedOp, typename Op, typename RetType, typename A1>
	struct Executor<TypedOp, Op, RetType, A1, void, void> {
		static RetType execute(Op &op, A1 &arg1) {
			return TypedOp(op)(arg1);
		}
	};

	template<typename TypedOp, typename Op, typename RetType>
	struct Executor<TypedOp, Op, RetType, void, void, void> {
		static RetType execute(Op &op) {
			return TypedOp(op)();
		}
	};

	template<typename Op, typename Traits>
	class ResolvingSubOp {
	public:
		typedef OpTraitsBase<Op, Traits> TraitsType;

	private:
		typedef typename TraitsType::FuncType FuncType;
		typedef typename TraitsType::RetType ExecRetType;

		template<typename T1, typename T2, typename Nullable>
		class Resolver {
		private:
			typedef typename TraitsType::template Arg<0>::Type A1;
			typedef typename TraitsType::template Arg<1>::Type A2;
			typedef typename TraitsType::template Arg<2>::Type A3;
			typedef typename TraitsType::template TypeAt<
					T1, T2, Nullable>::TypedOp TypedOp;

			typedef Executor<TypedOp, Op, ExecRetType, A1, A2, A3> ExecType;

		public:
			explicit Resolver(const ResolvingSubOp&) {}
			FuncType operator()() const {
				return &ExecType::execute;
			}
		};

	public:
		typedef FuncType RetType;
		typedef typename TraitsType::FilterType FilterType;

		template<typename T1, typename T2, typename Nullable>
		struct TypeAt {
			typedef Resolver<T1, T2, Nullable> TypedOp;
		};
	};

	template<typename Op, typename Traits>
	class ExecutingSubOp {
	public:
		typedef OpTraitsBase<Op, Traits> TraitsType;

		typedef typename TraitsType::RetType RetType;
		typedef typename TraitsType::FilterType FilterType;
		template<typename T1, typename T2, typename Nullable>
		struct TypeAt {
			struct TypedOp {
				explicit TypedOp(const ExecutingSubOp &op) : baseOp_(op.op_) {}
				RetType operator()() const {
					return typename TraitsType::template TypeAt<
							T1, T2, Nullable>::TypedOp(baseOp_)();
				}
				Op &baseOp_;
			};
		};

		explicit ExecutingSubOp(Op &op) : op_(op) {}

	private:
		Op &op_;
	};

	template<typename SubOp>
	typename SubOp::RetType operate(const SubOp &op) const;

	template<typename SubOp, typename T1>
	typename SubOp::RetType operate(const SubOp &op) const;

	template<typename SubOp, typename T1, typename T2>
	typename SubOp::RetType operate(const SubOp &op) const;

	template<typename SubOp, typename T1, typename T2, typename Nullable>
	typename SubOp::RetType operate(const SubOp &op) const;

	TupleColumnType getMaskedType1() const;
	TupleColumnType getMaskedType2() const;
	bool isNullable() const;
	bool isNullableAlways() const;

	bool checkNullable(bool nullableSwitching) const;

	TupleColumnType type1_;
	TupleColumnType type2_;
	bool nullableAlways_;
	ValueProfile *profile_;
};

class SQLValues::ValueContext {
public:
	class Source;

	explicit ValueContext(const Source &source);

	static Source ofAllocator(util::StackAllocator &alloc);
	static Source ofVarContext(
			VarContext &varCxt, util::StackAllocator *alloc = NULL,
			bool noVarAllocAllowed = false);

	util::StackAllocator& getAllocator();
	VarAllocator& getVarAllocator();
	util::StdAllocator<void, void> getStdAllocator();

	VarContext& getVarContext();
	size_t getVarContextDepth();

	size_t getMaxStringLength();

	int64_t getCurrentTimeMillis();

	util::TimeZone getTimeZone();
	void setTimeZone(const util::TimeZone *zone);

private:
	struct Constants {
		static const size_t DEFAULT_MAX_STRING_LENGTH;
	};

	static util::StackAllocator* findAllocator(
			VarContext &varCxt, util::StackAllocator *alloc);
	size_t resolveMaxStringLength();

	util::StackAllocator *alloc_;
	VarContext *varCxt_;
	ExtValueContext *extCxt_;

	size_t maxStringLength_;

	util::TimeZone timeZone_;
	bool timeZoneAssigned_;

	util::LocalUniquePtr<VarContext> localVarCxt_;
};

class SQLValues::ValueContext::Source {
public:
	Source(
			util::StackAllocator *alloc, VarContext *varCxt,
			ExtValueContext *extCxt);

private:
	friend class ValueContext;

	util::StackAllocator *alloc_;
	VarContext *varCxt_;
	ExtValueContext *extCxt_;
};

class SQLValues::ExtValueContext {
public:
	virtual size_t findMaxStringLength() = 0;
	virtual int64_t getCurrentTimeMillis() = 0;
	virtual util::TimeZone getTimeZone() = 0;
};

class SQLValues::RefVarContextScope {
public:
	explicit RefVarContextScope(VarContext *varCxt) : base_(resolve(varCxt)) {}

private:
	static VarContext& resolve(VarContext *varCxt) {
		assert(varCxt != NULL);
		return *varCxt;
	}

	VarContext::Scope base_;
};

template<bool Checked>
class SQLValues::EmptyVarContextScope {
public:
	explicit EmptyVarContextScope(VarContext *varCxt) {
		assert(!Checked || varCxt != NULL);
		static_cast<void>(varCxt);
	}
};

struct SQLValues::ProfileElement {
	ProfileElement();

	void merge(const ProfileElement &src);

	int64_t candidate_;
	int64_t target_;
};

struct SQLValues::ValueProfile {
	ProfileElement noNull_;
	ProfileElement noSwitch_;
};

class SQLValues::ValueHolder {
public:
	explicit ValueHolder(SQLValues::ValueContext &valueCxt);
	~ValueHolder();

	void assign(TupleValue &value);
	TupleValue release();
	void reset();

	static bool isDeallocatableContext(VarContext &varCxt);
	static bool isDeallocatableValueType(TupleColumnType type);

private:
	ValueHolder(const ValueHolder&);
	ValueHolder& operator=(const ValueHolder&);

	VarContext &varCxt_;
	TupleValue value_;
};

class SQLValues::ValueSetHolder {
public:
	explicit ValueSetHolder(SQLValues::ValueContext &valueCxt);
	~ValueSetHolder();

	void add(TupleValue &value);
	void reset();

	VarContext& getVarContext();

private:
	ValueSetHolder(const ValueSetHolder&);
	ValueSetHolder& operator=(const ValueSetHolder&);

	VarContext &varCxt_;
	util::AllocVector<TupleValue> values_;
};

struct SQLValues::Decremental {
	struct ValueCounter;

	struct AsLong;
	struct AsDouble;

	struct Utils;

	template<typename T> struct As;
};

struct SQLValues::Decremental::ValueCounter {
public:
	ValueCounter();

	uint64_t get() const;

	void increment();
	void decrement();

private:
	uint64_t count_;
};

struct SQLValues::Decremental::AsLong {
public:
	AsLong();

	int64_t get() const;

	void add(int64_t value, const util::TrueType&);
	void add(int64_t value, const util::FalseType&);

	void increment(const util::TrueType&);
	void increment(const util::FalseType&);

	const ValueCounter& getCounter() const;

private:
	int64_t base_;
	ValueCounter counter_;
};

struct SQLValues::Decremental::AsDouble {
public:
	AsDouble();

	double get() const;

	void add(double value, const util::TrueType&);
	void add(double value, const util::FalseType&);

	const ValueCounter& getCounter() const;

private:
	double base_;
	ValueCounter counter_;
	util::FixedDoubleSum sum_;
};

struct SQLValues::Decremental::Utils {
	static bool isSupported(TupleColumnType type);
	static uint32_t getFixedSize(TupleColumnType type);
	static void assignInitialValue(TupleColumnType type, void *addr);
};

template<typename T>
struct SQLValues::Decremental::As {
	typedef
			typename util::Conditional<
					T::COLUMN_TYPE == static_cast<TupleColumnType>(
							TupleTypes::TYPE_LONG), AsLong,
			typename util::Conditional<
					T::COLUMN_TYPE == static_cast<TupleColumnType>(
							TupleTypes::TYPE_DOUBLE), AsDouble,
			void>::Type>::Type Type;

	enum {
		TYPE_SUPPORTED = (!util::IsSame<Type, void>::VALUE),
		TYPE_INCREMENTAL = (util::IsSame<Type, AsLong>::VALUE)
	};
};

class SQLValues::SummaryColumn {
public:
	typedef uint32_t NullsUnitType;

	struct Source {
		Source();

		TupleColumnType type_;

		TupleColumn tupleColumn_;

		int32_t offset_;
		int32_t nullsOffset_;
		NullsUnitType nullsMask_;

		int32_t pos_;
		int32_t ordering_;

		int32_t tupleColumnPos_;
	};

	explicit SummaryColumn(const Source &src = Source());

	const TupleColumn& getTupleColumn() const;

	bool isEmpty() const;

	int32_t getOffset() const;
	int32_t getNullsOffset() const;
	NullsUnitType getNullsMask() const;

	uint32_t getSummaryPosition() const;
	bool isOnBody() const;
	bool isOrdering() const;
	bool isRotating() const;

private:
	TupleColumn tupleColumn_;

	int32_t offset_;
	int32_t nullsOffset_;
	NullsUnitType nullsMask_;

	int32_t pos_;
	int32_t ordering_;
};

class SQLValues::CompColumn {
public:
	CompColumn();

	const TupleColumn& getTupleColumn1() const;
	const TupleColumn& getTupleColumn2() const;

	const TupleColumn& getTupleColumn(const util::TrueType&) const;
	const TupleColumn& getTupleColumn(const util::FalseType&) const;

	const SummaryColumn& getSummaryColumn(const util::TrueType&) const;
	const SummaryColumn& getSummaryColumn(const util::FalseType&) const;

	void setSummaryColumn(const SummaryColumn &column, bool first);

	bool isColumnPosAssigned(bool first) const;
	uint32_t getColumnPos(bool first) const;
	void setColumnPos(uint32_t columnPos, bool first);

	uint32_t getColumnPos(const util::TrueType&) const;
	uint32_t getColumnPos(const util::FalseType&) const;

	TupleColumnType getType() const;
	void setType(TupleColumnType type);

	bool isAscending() const;
	void setAscending(bool ascending);

	bool isEitherUnbounded() const;
	bool isLowerUnbounded() const;

	void setEitherUnbounded(bool unbounded);
	void setLowerUnbounded(bool unbounded);

	bool isEitherExclusive() const;
	bool isLowerExclusive() const;

	void setEitherExclusive(bool exclusive);
	void setLowerExclusive(bool exclusive);

	bool isOrdering() const;
	void setOrdering(bool ordering);

	CompColumn toSingleSide(bool first) const;
	CompColumn toReversedSide() const;

private:
	SummaryColumn summaryColumn1_;
	SummaryColumn summaryColumn2_;

	TupleColumnType type_;

	uint32_t columnPos1_;
	uint32_t columnPos2_;

	bool ascending_;

	bool eitherUnbounded_;
	bool lowerUnbounded_;

	bool eitherExclusive_;
	bool lowerExclusive_;

	bool ordering_;
};

class SQLValues::ArrayTuple {
public:
	struct EntryGetter;
	struct EntrySetter;

	class Assigner;
	class EmptyAssigner;

	explicit ArrayTuple(util::StackAllocator &alloc);
	~ArrayTuple();

	bool isEmpty() const;
	void clear();

	void assign(ValueContext &cxt, const ColumnTypeList &typeList);

	void assign(ValueContext &cxt, const ArrayTuple &tuple);
	void assign(
			ValueContext &cxt, const ArrayTuple &tuple,
			const CompColumnList &columnList);
	void assign(
			ValueContext &cxt, const ReadableTuple &tuple,
			const CompColumnList &columnList);
	void assign(
			ValueContext &cxt, const ReadableTuple &tuple,
			const CompColumnList &columnList,
			const TupleColumnList &inColumnList);

	void swap(ArrayTuple &another);

	TupleValue get(size_t index) const;
	void set(size_t index, ValueContext &cxt, const TupleValue &value);

	bool isNull(size_t index) const;

	template<typename T>
	typename T::ValueType getAs(size_t index) const;

	template<typename T>
	void setBy(
			size_t index, ValueContext &cxt,
			const typename T::LocalValueType &value);

private:
	typedef util::Vector<uint8_t> VarData;

	union ValueBody {
		int64_t integral_;
		double floating_;
		const void *var_;
	};

	struct ValueEntry {
		ValueBody value_;
		int32_t type_;
		union {
			void *ptr_;
			VarData *var_;
			VarContext *varCxt_;
		} data_;
	};

	typedef util::Vector<ValueEntry> ValueList;

	ArrayTuple(const ArrayTuple&);
	ArrayTuple& operator=(const ArrayTuple&);

	void resize(size_t size);

	template<typename T>
	typename T::ValueType getAs(
			const ValueEntry &entry, const Types::NormalFixed&) const;
	template<typename T>
	typename T::ValueType getAs(
			const ValueEntry &entry, const Types::VarOrLarge&) const;
	template<typename>
	TupleValue getAs(const ValueEntry&, const Types::Any&) const;

	template<typename C>
	void setBy(
			ValueEntry &entry, ValueContext&,
			const typename C::LocalValueType &value, const C&,
			const Types::NormalFixed&);
	void setBy(
			ValueEntry &entry, ValueContext&, const NanoTimestamp &value,
			const Types::NanoTimestampTag&, const Types::NanoTimestampTag&);
	void setBy(
			ValueEntry &entry, ValueContext&, const TupleString &value,
			const Types::String&, const Types::String&);
	template<typename C>
	void setBy(
			ValueEntry &entry, ValueContext &cxt, const TupleValue &value,
			const C&, const Types::Sequential&);
	void setBy(
			ValueEntry &entry, ValueContext&, const TupleValue&,
			const Types::Any&, const Types::Any&);

	void clear(ValueEntry &entry);

	uint8_t* prepareVarData(ValueEntry &entry, uint32_t size);

	static TupleColumnType toColumnType(TupleColumnType type);

	ValueEntry& getEntry(size_t index);
	const ValueEntry& getEntry(size_t index) const;

	ValueList valueList_;
};

struct SQLValues::ArrayTuple::EntryGetter {
	typedef TupleValue RetType;

	template<typename T>
	struct TypeAt {
		explicit TypeAt(const EntryGetter &base) : base_(base.base_) {}

		RetType operator()(size_t index) const;

		const ArrayTuple &base_;
	};

	explicit EntryGetter(const ArrayTuple &base) : base_(base) {}
	const ArrayTuple &base_;
};

struct SQLValues::ArrayTuple::EntrySetter {
	typedef void RetType;

	template<typename T>
	struct TypeAt {
		explicit TypeAt(EntrySetter &base) : base_(base.base_) {}

		RetType operator()(
				size_t index, ValueContext &cxt, const TupleValue &value) const;

		ArrayTuple &base_;
	};

	explicit EntrySetter(ArrayTuple &base) : base_(base) {}
	ArrayTuple &base_;
};

class SQLValues::ArrayTuple::Assigner {
public:
	Assigner(
			util::StackAllocator &alloc, const CompColumnList &columnList,
			const TupleColumnList *inColumnList);

	void operator()(
			ValueContext &cxt, const ReadableTuple &src,
			ArrayTuple &dest) const;

	template<typename DigestOnly>
	void operator()(
			ValueContext &cxt, const DigestTupleListReader &src,
			DigestArrayTuple &dest, const DigestOnly&) const;

private:
	const CompColumnList &columnList_;
	const TupleColumnList *inColumnList_;
};

class SQLValues::ArrayTuple::EmptyAssigner {
public:
	EmptyAssigner(ValueContext &cxt, const ColumnTypeList &typeList);

	void operator()(ValueContext &cxt, ArrayTuple &dest) const;

private:
	const ColumnTypeList &typeList_;
	util::LocalUniquePtr<ArrayTuple> emptyTuple_;
};

class SQLValues::SummaryTuple {
private:
	template<typename T>
	struct TypeTraits {
		enum {
			COLUMN_TYPE = T::COLUMN_TYPE
		};
		typedef typename SQLValues::TypeUtils::template Traits<
				COLUMN_TYPE>::ReadableRefType ReadableRefType;
	};

public:
	typedef util::Vector<SummaryColumn> ColumnList;

	template<typename Shallow, typename HeadNullable>
	struct FieldReaderVariantTraits {
		typedef Shallow ShallowType;
		typedef HeadNullable HeadNullableType;
	};

	class FieldReader;
	class FieldGetter;
	template<bool Promo> class FieldSetter;

	class Wrapper;

	SummaryTuple();

	static SummaryTuple create(SummaryTupleSet &tupleSet);

	static SummaryTuple create(
			SummaryTupleSet &tupleSet, TupleListReader &reader,
			int64_t digest);
	static SummaryTuple create(
			SummaryTupleSet &tupleSet, TupleListReader &reader,
			int64_t digest, uint32_t subIndex);

	template<typename T, typename D>
	static SummaryTuple createBy(
			SummaryTupleSet &tupleSet, TupleListReader &reader,
			const D &digester);

	void initializeValues(SummaryTupleSet &tupleSet);

	void release(SummaryTupleSet &tupleSet);

	static void fillEmpty(SummaryTuple *tupleArrayElem);
	static bool checkEmpty(const SummaryTuple &tuple);

	int64_t getDigest() const;
	Wrapper getTuple() const;

	bool isNull(const SummaryColumn &column) const;
	void setNull(const SummaryColumn &column);

	TupleValue getValue(const SummaryColumn &column) const;
	TupleValue getValue(
			SummaryTupleSet &tupleSet, const SummaryColumn &column) const;

	template<typename T>
	typename T::ValueType getValueAs(const SummaryColumn &column) const;

	template<typename T, typename Rot>
	typename T::ValueType getHeadValueAs(const SummaryColumn &column) const;

	template<typename T>
	typename T::ValueType getBodyValueAs(const SummaryColumn &column) const;

	void setValue(
			SummaryTupleSet &tupleSet,
			const SummaryColumn &column, const TupleValue &value);
	void setValuePromoted(
			SummaryTupleSet &tupleSet,
			const SummaryColumn &column, const TupleValue &value);

	template<typename T>
	void setValueBy(
			SummaryTupleSet &tupleSet,
			const SummaryColumn &column,
			const typename T::LocalValueType &value);

	template<typename T>
	void incrementValueBy(const SummaryColumn &column);

	template<typename T>
	void appendValueBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			typename TypeTraits<T>::ReadableRefType value);

	template<typename T, typename F>
	bool checkDecrementalCounterAs(const SummaryColumn &column);

	template<typename T, typename F>
	void setDecrementalValueBy(
			const SummaryColumn &column, const typename T::ValueType &value);

	template<typename T, typename F>
	void incrementDecrementalValueBy(const SummaryColumn &column);

	template<typename T, typename F>
	void addDecrementalValueBy(
			const SummaryColumn &column, const typename T::ValueType &value);

	static void swapValue(
			SummaryTupleSet &tupleSet, SummaryTuple &tuple1, uint32_t pos1,
			SummaryTuple &tuple2, uint32_t pos2, uint32_t count);

private:
	typedef SummaryColumn::NullsUnitType NullsUnitType;

	union Head {
		int64_t digest_;
		const void *varData_;
	};

	SummaryTuple(const Head &head, void *body);

	void initializeBodyDetail(
			SummaryTupleSet &tupleSet, TupleListReader *reader);

	template<typename T>
	void readValue(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			TupleListReader &reader);

	template<typename T, typename Rot>
	typename T::ValueType getHeadValueDetailAs(
			const SummaryColumn &column, const T&, const Rot&) const;
	template<typename Rot>
	TupleNanoTimestamp getHeadValueDetailAs(
			const SummaryColumn &column, const Types::NanoTimestampTag&,
			const Rot&) const;
	template<typename Rot>
	TupleString getHeadValueDetailAs(
			const SummaryColumn &column, const Types::String&,
			const Rot&) const;

	template<typename T>
	typename T::ValueType getBodyValueDetailAs(
			const SummaryColumn &column, const T&) const;
	TupleNanoTimestamp getBodyValueDetailAs(
			const SummaryColumn &column, const Types::NanoTimestampTag&) const;
	TupleString getBodyValueDetailAs(
			const SummaryColumn &column, const Types::String&) const;
	TupleValue getBodyValueDetailAs(
			const SummaryColumn &column, const Types::Blob&) const;
	TupleValue getBodyValueDetailAs(
			const SummaryColumn &column, const Types::Any&) const;

	template<typename T, typename Shallow, typename Initializing>
	void setValueDetailBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			const typename T::LocalValueType &value, const T&, const Shallow&,
			const Initializing&);
	template<typename Shallow, typename Initializing>
	void setValueDetailBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			const TupleString &value, const Types::String&, const Shallow&,
			const Initializing&);
	template<typename Shallow, typename Initializing>
	void setValueDetailBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			const TupleValue &value, const Types::Blob&, const Shallow&,
			const Initializing&);
	template<typename Shallow, typename Initializing>
	void setValueDetailBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			const TupleValue &value, const Types::Any&, const Shallow&,
			const Initializing&);

	void appendValueDetailBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			StringReader &value, const Types::String&);
	void appendValueDetailBy(
			SummaryTupleSet &tupleSet, const SummaryColumn &column,
			LobReader &value, const Types::Blob&);

	void setNullDetail(const SummaryColumn &column, bool nullValue);

	template<typename T, typename Nullable, typename D>
	static Head readHead(
			SummaryTupleSet &tupleSet, TupleListReader &reader,
			const D &digester, const T&, const Nullable&);
	template<typename Nullable, typename D>
	static Head readHead(
			SummaryTupleSet &tupleSet, TupleListReader &reader,
			const D &digester, const Types::String&, const Nullable&);

	static Head digestToHead(int64_t digest);

	const void* getField(int32_t offset) const;
	void* getField(int32_t offset);

	void* getRawField(int32_t offset) const;

	Head head_;
	void *body_;
};

class SQLValues::SummaryTuple::FieldReader {
public:
	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const FieldReader &base) : base_(base) {}

		void operator()() const {
			base_.tuple_.template readValue<T>(
					base_.tupleSet_, base_.column_, base_.reader_);
		}

	private:
		const FieldReader &base_;
	};

	FieldReader(
			SummaryTuple &tuple, SummaryTupleSet &tupleSet,
			const SummaryColumn &column, TupleListReader &reader) :
			tuple_(tuple),
			tupleSet_(tupleSet),
			column_(column),
			reader_(reader) {
	}

private:
	SummaryTuple &tuple_;
	SummaryTupleSet &tupleSet_;
	const SummaryColumn &column_;
	TupleListReader &reader_;
};

class SQLValues::SummaryTuple::FieldGetter {
public:
	typedef TupleValue RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const FieldGetter &base) : base_(base) {}

		TupleValue operator()() const;

	private:
		const FieldGetter &base_;
	};

	FieldGetter(const SummaryTuple &tuple, const SummaryColumn &column) :
			tuple_(tuple),
			column_(column) {
	}

private:
	const SummaryTuple &tuple_;
	const SummaryColumn &column_;
};

template<bool Promo>
class SQLValues::SummaryTuple::FieldSetter {
public:
	typedef void RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const FieldSetter &base) : base_(base) {}

		void operator()() const;

	private:
		const FieldSetter &base_;
	};

	FieldSetter(
			SummaryTuple &tuple, SummaryTupleSet &tupleSet,
			const SummaryColumn &column, const TupleValue &value) :
			tuple_(tuple),
			tupleSet_(tupleSet),
			column_(column),
			value_(value) {
	}

private:
	template<typename T> void set(const util::TrueType&) const;
	template<typename T> void set(const util::FalseType&) const;

	SummaryTuple &tuple_;
	SummaryTupleSet &tupleSet_;
	const SummaryColumn &column_;
	const TupleValue &value_;
};

class SQLValues::SummaryTuple::Wrapper {
public:
	explicit Wrapper(const SummaryTuple &base) : base_(base) {}

	operator const SummaryTuple&() const {
		return base_;
	}

private:
	const SummaryTuple &base_;
};

class SQLValues::SummaryTupleSet {
public:
	typedef util::Vector<SummaryColumn> ColumnList;

	typedef void (*FieldReaderFunc)(const SummaryTuple::FieldReader&);
	typedef TupleValue (*FieldGetterFunc)(const SummaryTuple::FieldGetter&);
	typedef void (*FieldSetterFunc)(const SummaryTuple::FieldSetter<false>&);
	typedef void (*FieldPromotedSetterFunc)(
			const SummaryTuple::FieldSetter<true>&);

	typedef std::pair<SummaryColumn, FieldReaderFunc> FieldReaderEntry;
	typedef util::Vector<FieldReaderEntry> FieldReaderList;

	SummaryTupleSet(ValueContext &cxt, SummaryTupleSet *parent);
	~SummaryTupleSet();

	void addColumn(TupleColumnType type, const uint32_t *readerColumnPos);
	void addKey(uint32_t readerColumnPos, bool ordering, bool rotating);

	void setNullKeyIgnorable(bool ignorable);
	void setOrderedDigestRestricted(bool restricted);
	void setReaderColumnsDeep(bool deep);
	void setHeadNullAccessible(bool accessible);
	void setValueDecremental(bool decremental);
	static bool isDeepReaderColumnSupported(TupleColumnType type);

	void addReaderColumnList(const ColumnTypeList &typeList);
	void addReaderColumnList(const TupleColumnList &columnList);
	void addColumnList(const SummaryTupleSet &src);
	void addKeyList(const CompColumnList &keyList, bool first, bool rotating);
	void addSubReaderColumnList(
			uint32_t index, const TupleColumnList &columnList);

	void completeColumns();
	bool isColumnsCompleted();

	void applyKeyList(CompColumnList &keyList, const bool *first) const;
	const ColumnList& getTotalColumnList() const;
	const ColumnList& getReaderColumnList() const;
	const ColumnList& getModifiableColumnList() const;
	static void applyColumnList(const ColumnList &src, ColumnList &dest);

	size_t estimateTupleSize() const;
	bool isReadableTupleAvailable() const;
	static int32_t getDefaultReadableTupleOffset();
	size_t getFixedFieldSize(uint32_t index);

	const FieldReaderList& getFieldReaderList() const;
	const FieldReaderList& getSubFieldReaderList(uint32_t index) const;
	FieldGetterFunc getFieldGetterFunc(uint32_t pos) const;
	FieldSetterFunc getFieldSetterFunc(uint32_t pos) const;
	FieldPromotedSetterFunc getFieldPromotedSetterFunc(uint32_t pos) const;

	const SummaryColumn& getReaderHeadColumn() const;

	bool isDetailInitializationRequired() const;

	void* createBody();
	void initializeBody(void *body);
	void releaseBody(void *body);

	void* duplicateVarData(const void *data);
	void deallocateVarData(void *data);
	void appendVarData(void *&data, StringReader &reader);

	void* duplicateLob(const TupleValue &value);
	void deallocateLob(void *data);
	void appendLob(void *data, LobReader &reader);

	static TupleValue getLobValue(void *data);

	size_t getPoolCapacity();
	void clearAll();

private:
	typedef SummaryColumn::NullsUnitType NullsUnitType;

	typedef util::Vector<ColumnTypeList> TupleInfoList;

	typedef util::Vector<SummaryColumn::Source> ColumnSourceList;

	typedef util::Vector<FieldReaderList> AllFieldReaderList;
	typedef util::Vector<FieldGetterFunc> FieldGetterFuncList;
	typedef util::Vector<FieldSetterFunc> FieldSetterFuncList;
	typedef util::Vector<FieldPromotedSetterFunc> FieldPromotedSetterFuncList;

	typedef util::AllocVector<void*> Pool;

	struct PoolData {
		void *next_;
	};

	struct LobLink {
		LobLink *next_;
		LobLink **prev_;
		void *lob_;
		SummaryTupleSet *tupleSet_;

		TupleColumnType type_;
		bool small_;
	};

	class LocalLobBuilder {
	public:
		LocalLobBuilder(SummaryTupleSet &tupleSet, TupleColumnType type);
		~LocalLobBuilder();

		void assignSmall(void *lob);

		void* release(bool &small);

		void append(LobReader &src);
		void appendSmall(void *lob);

	private:
		void clear();
		void prepare(size_t size, bool force);

		SummaryTupleSet &tupleSet_;
		TupleColumnType type_;
		util::LocalUniquePtr<LobBuilder> base_;
		void *smallLob_;
	};

	enum {
		MIN_POOL_DATA_BITS = 3,
		MAX_POOL_DATA_BITS = 19,
		INITIAL_POOL_DATA_BITS = 10,
		SMALL_LOB_DATA_BITS = 16
	};

	SummaryTupleSet(const SummaryTupleSet&);
	SummaryTupleSet& operator=(const SummaryTupleSet&);

	static FieldReaderFunc resolveFieldReaderFunc(
			TupleColumnType destType, TupleColumnType srcType,
			bool shallow, bool headNullable);
	template<bool Shallow, bool HeadNullable>
	static FieldReaderFunc resolveFieldReaderFuncBy(
			TupleColumnType destType, TupleColumnType srcType);

	void initializePool(ValueContext &cxt);
	void deallocateAll();

	void reserveBody();

	void* appendVarDataDetail(
			const void *src, uint32_t size, void *dest, bool limited);

	static bool checkLobSmall(const TupleValue &value);
	static TupleValue toValueBySmallLob(
			VarContext &varCxt, const void *lob, TupleColumnType type);

	static void* duplicateNormalLob(
			VarContext &varCxt, const TupleValue &value);
	static void deallocateNormalLob(
			VarContext &varCxt, void *lob, TupleColumnType type);
	static TupleValue toValueByNormalLob(void *lob, TupleColumnType type);
	static void* toNormalLobByBuilder(VarContext &varCxt, LobBuilder &builder);

	void deallocateSmallLob(void *lob);
	static uint32_t getMaxSmallLobBodySize();

	void* allocateDetail(size_t size, size_t *availableSize);
	void deallocateDetail(void *data, size_t size);

	size_t getFixedSizeBits() const;
	static size_t getSizeBits(size_t size, bool ceil);

	util::StackAllocator &alloc_;
	VarContext &varCxt_;
	VarAllocator *poolAlloc_;
	SummaryTupleSet *parent_;

	ColumnSourceList columnSourceList_;
	ColumnSourceList keySourceList_;
	TupleInfoList subInputInfoList_;

	ColumnList totalColumnList_;
	ColumnList readerColumnList_;
	ColumnList modifiableColumnList_;

	FieldReaderList fieldReaderList_;
	AllFieldReaderList subFieldReaderList_;
	FieldGetterFuncList getterFuncList_;
	FieldSetterFuncList setterFuncList_;
	FieldPromotedSetterFuncList promotedSetterFuncList_;

	SummaryColumn readerHeadColumn_;

	bool readableTupleRequired_;
	bool keyNullIgnorable_;
	bool orderedDigestRestricted_;
	bool readerColumnsDeep_;
	bool headNullAccessible_;
	bool valueDecremental_;

	bool columnsCompleted_;

	size_t bodySize_;

	const void *initialBodyImage_;
	size_t initialBodyOffset_;

	void *restBodyData_;
	size_t restBodySize_;

	LobLink *lobLink_;
	util::LocalUniquePtr<Pool> varDataPool_;
	size_t varDataLimit_;

	std::pair<void*, bool> initialDataPool_;
	util::LocalUniquePtr<Pool> fixedDataPool_;
	util::LocalUniquePtr<Pool> largeDataPool_;
};

struct SQLValues::TupleListReaderSource {
	explicit TupleListReaderSource(TupleListReader &reader) : reader_(&reader) {}
	TupleListReader *reader_;
};

class SQLValues::DigestTupleListReader {
public:
	template<typename D>
	DigestTupleListReader(TupleListReader &reader, const D &digester);

	DigestTupleListReader(int64_t digest, TupleListReader &reader);

	const int64_t& getDigest() const { return digest_; }
	const TupleListReaderSource& getTuple() const { return readerSource_; }
	TupleListReader* getReader() const { return readerSource_.reader_; }

private:
	int64_t digest_;
	TupleListReaderSource readerSource_;
};

class SQLValues::DigestReadableTuple {
public:
	template<typename D>
	DigestReadableTuple(TupleListReader &reader, const D &digester);

	DigestReadableTuple(int64_t digest, const ReadableTuple &tuple);

	const int64_t& getDigest() const { return digest_; }
	const ReadableTuple& getTuple() const { return tuple_; }

private:
	int64_t digest_;
	ReadableTuple tuple_;
};

class SQLValues::DigestArrayTuple {
public:
	explicit DigestArrayTuple(util::StackAllocator &alloc);

	bool isEmpty() const { return tuple_.isEmpty(); }

	int64_t& getDigest() { return digest_; }
	const int64_t& getDigest() const { return digest_; }

	ArrayTuple& getTuple() { return tuple_; }
	const ArrayTuple& getTuple() const { return tuple_; }

	void moveTo(DigestArrayTuple &dest);
	void swap(DigestArrayTuple &another);

private:
	int64_t digest_;
	ArrayTuple tuple_;
};

class SQLValues::DigestHasher {
public:
	uint64_t operator()(int64_t digest) const {
		return static_cast<uint64_t>(digest);
	}
};


class SQLValues::ValueAccessor {
public:
	template<
			typename Base, bool VarGenerative = false,
			typename HeadOrd = Base, typename HeadRot = Base,
			typename Body = Base, bool FullDigest = true>
	struct TraitsBase;

	template<typename T, typename A1, typename A2>
	struct VarScopeTraits;

	class ByValue;
	template<typename V> class ByLocalValue;
	class ByReader;
	class ByReadableTuple;
	class ByArrayTuple;
	class ByKeyArrayTuple;
	class BySummaryTuple;
	template<typename Rot> class BySummaryTupleHead;
	class BySummaryTupleBody;

	explicit ValueAccessor(TupleColumnType type);

	ValueAccessor(
			CompColumnList::const_iterator columnIt,
			const CompColumnList &columnList);

	static ValueAccessor ofValue(const TupleValue &value);

	TupleColumnType getColumnType(bool promotable, bool first) const;

	bool isSameVariant(
			const ValueAccessor &another, bool nullIgnorable,
			bool anotherNullIgnorable) const;

private:
	class AccessorRef {
	public:
		explicit AccessorRef(const ValueAccessor &base) : base_(base) {}
		const ValueAccessor& get() const { return base_; }

	private:
		const ValueAccessor &base_;
	};

	class NoopNullChecker;

	TupleColumnType type_;
	CompColumnList::const_iterator columnIt_;
	const CompColumnList *columnList_;
};

template<
		typename Base, bool VarGenerative, typename HeadOrd, typename HeadRot,
		typename Body, bool FullDigest>
struct SQLValues::ValueAccessor::TraitsBase {
	typedef Base BaseType;

	template<typename Rot> struct HeadOf {
		typedef typename util::Conditional<
				Rot::VALUE, HeadRot, HeadOrd>::Type Type;
	};

	typedef Body BodyType;

	enum {
		FULL_DIGEST = FullDigest,
		HEAD_SEPARATED =
				(!util::IsSame<Base, HeadOrd>::VALUE ||
				!util::IsSame<Base, HeadRot>::VALUE ||
				!util::IsSame<Base, Body>::VALUE),
		VAR_GENERATIVE = VarGenerative
	};

	template<typename T>
	struct DigestAccessible {
		typedef typename util::BoolType<
				(FULL_DIGEST || !TypeUtils::Traits<
						T::COLUMN_TYPE>::FOR_SINGLE_VAR)>::Result Type;
	};

	template<typename T>
	struct BodyOnly {
		enum {
			FOR_MULTI_VAR_OR_LARGE_FIXED =
					(TypeUtils::Traits<T::COLUMN_TYPE>::FOR_MULTI_VAR ||
					TypeUtils::Traits<T::COLUMN_TYPE>::FOR_LARGE_FIXED)
		};
		typedef typename util::BoolType<
				(HEAD_SEPARATED && FOR_MULTI_VAR_OR_LARGE_FIXED)>::Result Type;
	};

	template<typename T>
	struct VarGenerativeBy {
		typedef typename util::BoolType<
				(VAR_GENERATIVE && TypeUtils::Traits<
						T::COLUMN_TYPE>::FOR_MULTI_VAR)>::Result Type;
	};
};

template<typename T, typename A1, typename A2>
struct SQLValues::ValueAccessor::VarScopeTraits {
private:
	typedef A1 AccessorType1;
	typedef typename util::Conditional<
			util::IsSame<A2, void>::VALUE, A1, A2>::Type AccessorType2;

	enum {
		POTENTIALLY_VAR_GENERATIVE =
				(AccessorType1::TraitsType::VAR_GENERATIVE ||
				AccessorType2::TraitsType::VAR_GENERATIVE),
		VAR_GENERATIVE =
				(AccessorType1::TraitsType::template VarGenerativeBy<
						T>::Type::VALUE ||
				AccessorType2::TraitsType::template VarGenerativeBy<
						T>::Type::VALUE)
	};

public:
	typedef typename util::Conditional<
			VAR_GENERATIVE,
			RefVarContextScope,
			EmptyVarContextScope<POTENTIALLY_VAR_GENERATIVE> >::Type Type;
};

class SQLValues::ValueAccessor::ByValue {
public:
	typedef TraitsBase<ByValue> TraitsType;
	typedef TupleValue SourceType;
	typedef SourceType SubSourceType;
	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&);
	};
};

template<typename V>
class SQLValues::ValueAccessor::ByLocalValue {
public:
	typedef TraitsBase<ByLocalValue> TraitsType;
	typedef typename V::LocalValueType SourceType;
	typedef SourceType SubSourceType;

	template<typename> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		const SourceType& operator()(const SourceType &src, const P&) {
			return src;
		}
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&) {
			typedef typename util::Conditional<
					TypeUtils::Traits<V::COLUMN_TYPE>::FOR_ANY,
					ByValue::NullChecker, NoopNullChecker>::Type BaseType;
			BaseType base((get()));
			return base(src, P());
		}
	};
};

class SQLValues::ValueAccessor::ByReader {
public:
	typedef TraitsBase<ByReader, true> TraitsType;
	typedef TupleListReaderSource SourceType;
	typedef DigestTupleListReader DigestSourceType;
	typedef SourceType SubSourceType;
	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&);
	};
};

class SQLValues::ValueAccessor::ByReadableTuple {
public:
	typedef TraitsBase<ByReadableTuple, true> TraitsType;
	typedef ReadableTuple SourceType;
	typedef DigestReadableTuple DigestSourceType;
	typedef SourceType SubSourceType;
	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&);
	};
};

class SQLValues::ValueAccessor::ByArrayTuple {
public:
	typedef TraitsBase<ByArrayTuple> TraitsType;
	typedef ArrayTuple SourceType;
	typedef DigestArrayTuple DigestSourceType;
	typedef SourceType SubSourceType;
	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&);
	};
};

class SQLValues::ValueAccessor::ByKeyArrayTuple {
public:
	typedef TraitsBase<ByKeyArrayTuple> TraitsType;
	typedef ArrayTuple SourceType;
	typedef DigestArrayTuple DigestSourceType;
	typedef SourceType SubSourceType;
	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&);
	};
};

class SQLValues::ValueAccessor::BySummaryTuple {
public:
	typedef TraitsBase<
			BySummaryTuple, true,
			BySummaryTupleHead<util::FalseType>,
			BySummaryTupleHead<util::TrueType>,
			BySummaryTupleBody, false> TraitsType;

	typedef SummaryTuple SourceType;
	typedef SummaryTuple DigestSourceType;
	typedef SummaryTuple::Wrapper SubSourceType;
	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};

	class NullChecker : private AccessorRef {
	public:
		explicit NullChecker(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		bool operator()(const SourceType &src, const P&);
	};
};

template<typename Rot>
class SQLValues::ValueAccessor::BySummaryTupleHead {
public:
	typedef BySummaryTuple::TraitsType TraitsType;
	typedef BySummaryTuple::SourceType SourceType;
	typedef BySummaryTuple::DigestSourceType DigestSourceType;
	typedef BySummaryTuple::SubSourceType SubSourceType;
	typedef BySummaryTuple::NullChecker NullChecker;

	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};
};

class SQLValues::ValueAccessor::BySummaryTupleBody {
public:
	typedef BySummaryTuple::TraitsType TraitsType;
	typedef BySummaryTuple::SourceType SourceType;
	typedef BySummaryTuple::DigestSourceType DigestSourceType;
	typedef BySummaryTuple::SubSourceType SubSourceType;
	typedef BySummaryTuple::NullChecker NullChecker;

	template<typename T> class TypeAt : private AccessorRef {
	public:
		explicit TypeAt(const ValueAccessor &base) : AccessorRef(base) {}
		template<typename P>
		typename T::ValueType operator()(const SourceType &src, const P&);
	};
};

class SQLValues::ValueAccessor::NoopNullChecker : private AccessorRef {
public:
	explicit NoopNullChecker(const ValueAccessor &base) :
			AccessorRef(base) {
	}

	template<typename S, typename P>
	bool operator()(const S&, const P&);

	template<typename P>
	bool operator()(const TupleValue &src, const P&);
};

class SQLValues::ValueBasicComparator {
public:
	typedef int32_t RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const ValueBasicComparator &base) : base_(base) {}

		template<typename Pred>
		bool operator()(
				const typename T::LocalValueType &v1,
				const typename T::LocalValueType &v2,
				const Pred &pred) const;

	private:
		const ValueBasicComparator &base_;
	};

	explicit ValueBasicComparator(bool sensitive) : sensitive_(sensitive) {}

private:
	bool sensitive_;
};

template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Byte>::operator()(
		const int8_t &v1, const int8_t &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Short>::operator()(
		const int16_t &v1, const int16_t &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Integer>::operator()(
		const int32_t &v1, const int32_t &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Long>::operator()(
		const int64_t &v1, const int64_t &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Float>::operator()(
		const float &v1, const float &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Double>::operator()(
		const double &v1, const double &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::TimestampTag>::operator()(
		const int64_t &v1, const int64_t &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::MicroTimestampTag>::operator()(
		const MicroTimestamp &v1, const MicroTimestamp &v2,
		const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::NanoTimestampTag>::operator()(
		const NanoTimestamp &v1, const NanoTimestamp &v2,
		const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Bool>::operator()(
		const bool &v1, const bool &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::String>::operator()(
		const TupleString &v1, const TupleString &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Blob>::operator()(
		const TupleValue &v1, const TupleValue &v2, const Pred &pred) const;
template<>
template<typename Pred>
bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Any>::operator()(
		const TupleValue &v1, const TupleValue &v2, const Pred &pred) const;

class SQLValues::ValueComparator {
public:
	typedef int64_t PredArgType;
	typedef int32_t DefaultResultType;

	template<typename Pred>
	struct ResultTypeOf;

	template<typename Pred>
	struct ReversedPred;

	template<typename Pred, bool Rev>
	struct PredTypeOf;

	template<typename Pred, bool Rev, typename A1, typename A2>
	struct VariantTraits;

	template<
			typename Pred, bool Rev, bool Promo, bool Ordering,
			typename A1, typename A2>
	class Switcher;

	template<
			typename Pred, bool Rev, bool Promo, bool Ordering,
			typename A1, typename A2 = A1>
	struct SwitcherOf {
		typedef Switcher<Pred, Rev, Promo, Ordering, A1, A2> Type;
	};

	template<typename T>
	class TypeAt;

	template<typename T1, typename T2>
	class BasicTypeAt;

	struct ThreeWay {
		typedef DefaultResultType result_type;
		typedef PredArgType first_argument_type;
		typedef PredArgType second_argument_type;

		result_type operator()(
				first_argument_type v1, second_argument_type v2) const;
	};

	class Arranged;

	ValueComparator(
			const ValueAccessor &accessor1, const ValueAccessor &accessor2,
			VarContext *varCxt, bool sensitive, bool ascending);

	static ValueComparator ofValues(
			const TupleValue &v1, const TupleValue &v2, bool sensitive,
			bool ascending);
	static DefaultResultType compareValues(
			const TupleValue &v1, const TupleValue &v2, bool sensitive,
			bool ascending);

	DefaultResultType operator()(
			const TupleValue &v1, const TupleValue &v2) const;

	template<typename Pred>
	bool operator()(
			const TupleValue &v1, const TupleValue &v2, const Pred &pred) const {
		return pred((*this)(v1, v2), 0);
	}

	bool isEmpty() const;

	bool isSameVariant(
			const ValueComparator &another, bool nullIgnorable,
			bool anotherNullIgnorable, bool orderArranged) const;

private:
	typedef ThreeWay DefaultPredType;

	typedef SwitcherOf<
			void, false, true, false,
			ValueAccessor::ByValue>::Type DefaultSwitcherByValues;

	template<typename A, typename T, typename Comp>
	struct PromotedOp;

	template<typename A, typename T, typename Comp>
	struct OpTypeOf;

	TypeSwitcher getColumnTypeSwitcher(
			bool promotable, bool nullIgnorable, ValueProfile *profile) const;

	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compare(
			int64_t v1, int64_t v2, bool, const Pred &pred, const Types::Integral&);

	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compare(
			double v1, double v2, bool sensitive,
			const Pred &pred, const Types::Floating&);

	template<typename Pred, typename T>
	static typename util::BinaryFunctionResultOf<Pred>::Type compare(
			const T &v1, const T &v2, bool, const Pred &pred,
			const Types::PreciseTimestamp&);

	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compare(
			const TupleString &v1, const TupleString &v2, bool,
			const Pred &pred, const Types::String&);

	template<typename Pred, typename T>
	static typename util::BinaryFunctionResultOf<Pred>::Type compare(
			const T &v1, const T &v2, bool, const Pred &pred,
			const Types::Sequential&);

	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compare(
			const TupleValue&, const TupleValue&, bool,
			const Pred &pred, const Types::Any&);

	ValueAccessor accessor1_;
	ValueAccessor accessor2_;
	VarContext *varCxt_;
	bool sensitive_;
	bool ascending_;
};

template<typename Pred>
struct SQLValues::ValueComparator::ResultTypeOf {
	typedef typename util::Conditional<
			util::IsSame<Pred, void>::VALUE,
			DefaultPredType, Pred>::Type PredType;

	typedef typename util::BinaryFunctionResultOf<PredType>::Type Type;
};

template<typename Pred>
struct SQLValues::ValueComparator::ReversedPred {
	typedef typename util::BinaryFunctionResultOf<Pred>::Type result_type;
	typedef PredArgType first_argument_type;
	typedef PredArgType second_argument_type;

	result_type operator()(
			first_argument_type v1, second_argument_type v2) const {
		return Pred()(v2, v1);
	}
};

template<typename Pred, bool Rev>
struct SQLValues::ValueComparator::PredTypeOf {
	typedef typename ResultTypeOf<Pred>::PredType BasePredType;
	typedef ReversedPred<BasePredType> RevPredType;

	typedef typename util::Conditional<
			Rev, RevPredType, BasePredType>::Type Type;
};

template<typename Pred, bool Rev, typename A1, typename A2>
struct SQLValues::ValueComparator::VariantTraits {
	typedef VariantTraits OfValueComparator;

	typedef Pred OrgPredType;
	typedef A2 OrgAccessorType2;

	static const bool ORDERING_REVERSED = Rev;

	typedef typename PredTypeOf<Pred, Rev>::BasePredType BasePredType;
	typedef typename PredTypeOf<Pred, Rev>::Type PredType;

	typedef typename ResultTypeOf<PredType>::Type RetType;

	typedef A1 AccessorType1;
	typedef typename util::Conditional<
			util::IsSame<A2, void>::VALUE,
			AccessorType1, A2>::Type AccessorType2;

	template<typename PredSub, bool RevSub>
	struct PredRebind {
		typedef VariantTraits<PredSub, RevSub, A1, A2> Type;
	};
};

template<
		typename Pred, bool Rev, bool Promo, bool Ordering,
		typename A1, typename A2>
class SQLValues::ValueComparator::Switcher {
public:
	typedef typename ResultTypeOf<Pred>::PredType BasePredType;
	typedef typename ResultTypeOf<BasePredType>::Type RetType;

	typedef typename A1::SubSourceType SourceType1;
	typedef typename A2::SubSourceType SourceType2;

	typedef typename TypeSwitcher::template OpTraitsOptions<
			Switcher, 2, false, Promo> PlainOptionsType;
	typedef typename PlainOptionsType::template DelegationRebind<
			true>::Type DefaultOptionsType;

	typedef typename TypeSwitcher::template OpTraits<
			RetType, PlainOptionsType> PlainTraitsType;
	typedef typename TypeSwitcher::template OpTraits<
			RetType, DefaultOptionsType,
			const SourceType1, const SourceType2> DefaultTraitsType;

	typedef typename DefaultTraitsType::template Func<
			const ValueComparator>::Type DefaultFuncType;

	template<typename Op>
	struct PlainOpBase {
		typedef typename PlainTraitsType::template Func<Op, true>::Type FuncType;
		typedef typename PlainTraitsType::template ResultRebind<Op>::Type TraitsType;
	};

	Switcher(const ValueComparator &base, bool nullIgnorable) :
			base_(base),
			nullIgnorable_(nullIgnorable),
			profile_(NULL) {
	}

	Switcher withProfile(ValueProfile *profile) const {
		Switcher dest = *this;
		dest.profile_ = profile;
		return dest;
	}

	template<typename Op>
	typename PlainOpBase<Op>::FuncType get() const {
		return getWith<Op, typename PlainOpBase<Op>::TraitsType>();
	}

	template<typename Op, typename Traits>
	typename Traits::template Func<Op>::Type getWith() const;

private:
	template<typename V>
	struct ValueComparatorRebind {
		typedef V Type;
	};

	template<typename Base, bool Ascending>
	struct OpTraitsAt {
		enum {
			ORG_REVERSED = !Ascending,
			SUB_REVERSED = (Rev ? !ORG_REVERSED : ORG_REVERSED)
		};

		typedef typename util::Conditional<
				util::IsSame<BasePredType, DefaultPredType>::VALUE,
				void, BasePredType>::Type PredType;

		typedef A1 AccessorType1;
		typedef typename util::Conditional<
				util::IsSame<A1, A2>::VALUE, void, A2>::Type AccessorType2;

		typedef VariantTraits<
				PredType, SUB_REVERSED,
				AccessorType1, AccessorType2> SubTraitsType;
		typedef typename Base::VariantTraitsType::
				template ValueComparatorRebind<
						SubTraitsType>::Type VariantTraitsType;
		typedef typename Base::template VariantRebind<
				VariantTraitsType>::Type Type;
	};

	template<typename Op, typename Traits, bool Ascending>
	typename Traits::template Func<Op>::Type getSubWith() const;

	ValueComparator base_;
	bool nullIgnorable_;
	ValueProfile *profile_;
};

template<typename T>
class SQLValues::ValueComparator::TypeAt {
public:
	typedef TypeAt TypedOp;

	typedef typename T::VariantTraitsType::OfValueComparator VariantTraitsType;
	typedef typename VariantTraitsType::RetType RetType;
	typedef typename VariantTraitsType::AccessorType1 AccessorType1;
	typedef typename VariantTraitsType::AccessorType2 AccessorType2;
	typedef typename ValueAccessor::VarScopeTraits<
			typename T::Type, AccessorType1, AccessorType2>::Type VarScopeType;

	explicit TypeAt(const ValueComparator &base) : base_(base) {}

	RetType operator()(
			const typename AccessorType1::SourceType &src1,
			const typename AccessorType2::SourceType &src2) const;

private:
	const ValueComparator &base_;
};

template<typename T1, typename T2>
class SQLValues::ValueComparator::BasicTypeAt {
public:
	explicit BasicTypeAt(bool sensitive) : sensitive_(sensitive) {
	}

	template<typename Pred>
	typename util::BinaryFunctionResultOf<Pred>::Type operator()(
			const typename T1::LocalValueType &v1,
			const typename T2::LocalValueType &v2,
			const Pred&) const;

private:
	typedef typename TypeUtils::PairTraits<
			T1::COLUMN_TYPE, T2::COLUMN_TYPE> PairTraits;
	typedef typename PairTraits::ComparableValueTag CompTypeTag;
	typedef typename TypeUtils::template Traits<
			CompTypeTag::COLUMN_TYPE> TypeTraits;

	bool sensitive_;
};

class SQLValues::ValueComparator::Arranged {
public:
	explicit Arranged(const ValueComparator &base);

	DefaultResultType operator()(
			const TupleValue &v1, const TupleValue &v2) const {
		return func_(base_, v1, v2);
	}

private:
	typedef DefaultSwitcherByValues SwitcherType;
	ValueComparator base_;
	SwitcherType::DefaultFuncType func_;
};

template<typename A, typename T, typename Comp>
struct SQLValues::ValueComparator::PromotedOp {
public:
	explicit PromotedOp(const ValueAccessor &accessor);

	template<typename Source, typename Pos>
	typename Comp::LocalValueType operator()(
			const Source &src, const Pos&) const;

private:
	typedef typename A::template TypeAt<T> BaseOpType;

	const ValueAccessor &accessor_;
};

template<typename A, typename T, typename Comp>
struct SQLValues::ValueComparator::OpTypeOf {
private:
	typedef PromotedOp<A, T, Comp> PromotedOpType;
	typedef typename A::template TypeAt<T> BaseOpType;
	typedef typename TypeUtils::Traits<T::COLUMN_TYPE> TraitsType;

	enum {
		PROMOTION_REQUIRED =
				(!util::IsSame<T, Comp>::VALUE &&
				TraitsType::FOR_TIMESTAMP_FAMILY)
	};

public:
	typedef typename util::Conditional<
			PROMOTION_REQUIRED, PromotedOpType, BaseOpType>::Type Type;
};

class SQLValues::ValueEq {
public:
	bool operator()(const TupleValue &v1, const TupleValue &v2) const;
};

class SQLValues::ValueLess {
public:
	bool operator()(const TupleValue &v1, const TupleValue &v2) const;
};

class SQLValues::ValueGreater {
public:
	bool operator()(const TupleValue &v1, const TupleValue &v2) const;
};

class SQLValues::ValueFnv1aHasher {
public:
	struct Constants;

	template<typename T> class TypeAt {
	public:
		typedef typename T::VariantTraitsType::OfValueDigester VariantTraitsType;
		typedef typename VariantTraitsType::AccessorType AccessorType;
		typedef typename ValueAccessor::VarScopeTraits<
				typename T::Type, AccessorType, void>::Type VarScopeType;

		explicit TypeAt(const ValueFnv1aHasher &base) : base_(base) {}
		int64_t operator()(const typename AccessorType::SourceType &src) const;

	private:
		const ValueFnv1aHasher &base_;
	};

	ValueFnv1aHasher(const ValueAccessor &accessor, VarContext *varCxt);
	ValueFnv1aHasher(
			const ValueAccessor &accessor, VarContext *varCxt, int64_t seed);

	static ValueFnv1aHasher ofValue(const TupleValue &value);
	static ValueFnv1aHasher ofValue(const TupleValue &value, int64_t seed);

	uint32_t operator()(const TupleValue &value) const;

	static int64_t maskNull(int64_t src);
	static int64_t unmaskNull(int64_t src);

private:
	static const int64_t NULL_MASK_VALUE = -static_cast<int64_t>(UINT32_MAX);

	static int64_t digest(
			uint32_t seed, const int64_t &value, const Types::Integral&);
	static int64_t digest(
			uint32_t seed, const double &value, const Types::Floating&);
	template<typename T>
	static int64_t digest(
			uint32_t seed, const T &value, const Types::PreciseTimestamp&);
	template<typename T>
	static int64_t digest(
			uint32_t seed, const T &value, const Types::Sequential&);
	static int64_t digest(
			uint32_t seed, const TupleValue &value, const Types::Any&);

	ValueAccessor accessor_;
	VarContext *varCxt_;
	uint32_t seed_;
};

struct SQLValues::ValueFnv1aHasher::Constants {
	static const uint32_t FNV_OFFSET_BASIS = 2166136261U;
	static const uint32_t FNV_PRIME = 16777619U;

	static const uint32_t INITIAL_SEED = FNV_OFFSET_BASIS;
};

class SQLValues::ValueOrderedDigester {
public:
	struct Constants;

	template<typename T> class TypeAt {
	public:
		typedef typename T::VariantTraitsType::OfValueDigester VariantTraitsType;
		typedef typename VariantTraitsType::AccessorType AccessorType;
		typedef typename ValueAccessor::VarScopeTraits<
				typename T::Type, AccessorType, void>::Type VarScopeType;

		explicit TypeAt(const ValueOrderedDigester &base) : base_(base) {}
		int64_t operator()(const typename AccessorType::SourceType &src) const;

	private:
		const ValueOrderedDigester &base_;
	};

	ValueOrderedDigester(
			const ValueAccessor &accessor, VarContext *varCxt, bool rotating);

private:
	typedef util::TrueType Ascending;

	static int64_t digest(const int64_t &value, const Types::Integral&);
	static int64_t digest(const double &value, const Types::Floating&);
	template<typename T>
	static int64_t digest(const T &value, const Types::PreciseTimestamp&);
	template<typename T>
	static int64_t digest(const T &value, const Types::Sequential&);
	static int64_t digest(const TupleValue &value, const Types::Any&);

	ValueAccessor accessor_;
	VarContext *varCxt_;
	bool rotating_;
};

struct SQLValues::ValueOrderedDigester::Constants {
private:
	static const uint64_t ROT_MAX = ~static_cast<uint64_t>(0);

public:
	static const uint64_t ROT_PRIME = 1048573U;
	static const uint64_t ROT_BASE = ROT_MAX / ROT_PRIME;
	static const uint64_t ROT_THRESHOLD = ROT_PRIME * ROT_BASE;
};

class SQLValues::ValueDigester {
private:
	typedef ValueFnv1aHasher UnorderedType;
	typedef ValueOrderedDigester OrderedType;

public:
	template<bool Ordering, bool Rotating, typename Accessor>
	struct VariantTraits;

	template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
	class Switcher;

	template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
	struct SwitcherOf {
		typedef Switcher<Accessor, NullableSpecific, RotationSpecific> Type;
	};

	template<typename T>
	class TypeAt;

	ValueDigester(
			const ValueAccessor &accessor, VarContext *varCxt, bool ordering,
			bool rotating, int64_t seed);

	operator UnorderedType() const {
		return UnorderedType(accessor_, varCxt_, seed_);
	}

	operator OrderedType() const {
		return OrderedType(accessor_, varCxt_, rotating_);
	}

	bool isEmpty() const;

	bool isSameVariant(
			const ValueDigester &another, bool nullIgnorable,
			bool anotherNullIgnorable) const;

private:
	TypeSwitcher getColumnTypeSwitcher(
			bool nullIgnorable, bool nullableAlways,
			ValueProfile *profile) const;

	ValueAccessor accessor_;
	VarContext *varCxt_;
	bool ordering_;
	bool rotating_;
	int64_t seed_;
};

template<bool Ordering, bool Rotating, typename Accessor>
struct SQLValues::ValueDigester::VariantTraits {
public:
	enum {
		VARIANT_ORDERED = Ordering
	};

	typedef VariantTraits OfValueDigester;

	typedef typename util::Conditional<
			VARIANT_ORDERED, OrderedType, UnorderedType>::Type VariantBase;

	typedef typename util::BoolType<Rotating>::Result RotatingType;
	typedef Accessor AccessorType;
};

template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
class SQLValues::ValueDigester::Switcher {
public:
	typedef typename Accessor::SourceType SourceType;

	typedef typename TypeSwitcher::template OpTraitsOptions<
			Switcher, 1, false, false, NullableSpecific> PlainOptionsType;
	typedef typename PlainOptionsType::template DelegationRebind<
			true>::Type DefaultOptionsType;

	typedef typename TypeSwitcher::template OpTraits<
			int64_t, PlainOptionsType> PlainTraitsType;
	typedef typename TypeSwitcher::template OpTraits<
			int64_t, DefaultOptionsType, const SourceType> DefaultTraitsType;

	typedef typename DefaultTraitsType::template Func<
			const ValueDigester>::Type DefaultFuncType;

	template<typename Op>
	struct PlainOpBase {
		typedef typename PlainTraitsType::template Func<Op, true>::Type FuncType;
		typedef typename PlainTraitsType::template ResultRebind<Op>::Type TraitsType;
	};

	Switcher(
			const ValueDigester &base, bool nullIgnorable,
			bool nullableAlways) :
			base_(base),
			nullIgnorable_(nullIgnorable),
			nullableAlways_(nullableAlways),
			profile_(NULL) {
	}

	Switcher withProfile(ValueProfile *profile) const {
		Switcher dest = *this;
		dest.profile_ = profile;
		return dest;
	}

	template<typename Op>
	typename PlainOpBase<Op>::FuncType get() const {
		return getWith<Op, typename PlainOpBase<Op>::TraitsType>();
	}

	template<typename Op, typename Traits>
	typename Traits::template Func<Op>::Type getWith() const;

private:
	template<typename V>
	struct ValueDigesterRebind {
		typedef V Type;
	};

	template<typename Base, bool Ordering, bool Rotating>
	struct OpTraitsAt {
		typedef VariantTraits<Ordering, Rotating, Accessor> SubTraitsType;
		typedef typename Base::VariantTraitsType::template ValueDigesterRebind<
				SubTraitsType>::Type VariantTraitsType;
		typedef typename Base::template VariantRebind<
				VariantTraitsType>::Type Type;
	};

	template<typename Op, typename Traits, bool Ordering, bool Rotating>
	typename Traits::template Func<Op>::Type getSubWith() const;

	ValueDigester base_;
	bool nullIgnorable_;
	bool nullableAlways_;
	ValueProfile *profile_;
};

template<typename T>
class SQLValues::ValueDigester::TypeAt {
public:
	typedef typename T::VariantTraitsType::OfValueDigester VariantTraitsType;
	typedef typename VariantTraitsType::VariantBase::template TypeAt<T> TypedOp;
};

class SQLValues::ValueConverter {
public:
	explicit ValueConverter(TupleColumnType type);
	TupleValue operator()(ValueContext &cxt, const TupleValue &src) const;

private:
	TupleColumnType type_;
};

class SQLValues::ValuePromoter {
public:
	explicit ValuePromoter(TupleColumnType type);
	TupleValue operator()(ValueContext *cxt, const TupleValue &src) const;

private:
	TupleColumnType type_;
};


class SQLValues::TupleNullChecker {
public:
	template<typename Accessor>
	class WithAccessor;

	explicit TupleNullChecker(const CompColumnList &columnList);

	bool isEmpty() const;

private:
	const CompColumnList &columnList_;
};

template<typename Accessor>
class SQLValues::TupleNullChecker::WithAccessor {
public:
	WithAccessor(util::StackAllocator &alloc, const TupleNullChecker &base);

	bool operator()(const typename Accessor::SourceType &src) const;

	const TupleNullChecker& getBase() const {
		return base_;
	}

private:
	typedef util::Vector<ValueAccessor> AccessorList;

	AccessorList accessorList_;
	TupleNullChecker base_;
};

class SQLValues::TupleComparator {
public:
	template<typename T>
	class BaseTypeAt;

	template<typename Ret, typename A1, typename A2>
	struct BaseWithAccessor;

	template<
			int32_t DigestOnly, typename Pred, bool Rev,
			typename A1, typename A2>
	struct VariantTraits;

	template<
			typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
			typename A1, typename A2>
	class Switcher;

	template<
			typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
			typename A1, typename A2 = A1>
	struct SwitcherOf {
		typedef Switcher<Pred, Rev, Promo, Typed, Ordering, A1, A2> Type;
	};

	template<
			typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
			typename A1, typename A2 = A1>
	class WithAccessor;

	template<typename T> class BaseTypeAt;

	TupleComparator(
			const CompColumnList &columnList, VarContext *varCxt,
			bool sensitive, bool withDigest = false, bool nullIgnorable = false,
			bool orderedDigestRestricted = false);

	ValueComparator initialComparatorAt(
			CompColumnList::const_iterator it, bool ordering) const;

	ValueComparator comparatorAt(CompColumnList::const_iterator it) const;

	bool isEmpty(size_t startIndex) const;
	bool isEitherSideEmpty(size_t startIndex) const;

	bool isDigestOnly(bool promotable) const;
	bool isDigestOrdered() const;
	bool isSingleVarHeadOnly(bool ordering) const;

	template<typename L>
	static void updateProfile(const L &subFuncList, ValueProfile *profile);

private:
	template<bool DigestOnly, bool DigestAccessible, bool HeadOnly>
	struct TuplePred;

	const CompColumnList &columnList_;
	VarContext *varCxt_;
	bool sensitive_;
	bool withDigest_;
	bool nullIgnorable_;
	bool orderedDigestRestricted_;
};

template<typename Ret, typename A1, typename A2>
struct SQLValues::TupleComparator::BaseWithAccessor {
	template<typename T>
	struct TypeAt {
		typedef BaseTypeAt<T> TypedOp;
	};

	typedef Ret RetType;

	typedef typename A1::SubSourceType SourceType1;
	typedef typename A2::SubSourceType SourceType2;

	typedef ValueComparator::DefaultResultType (*SubFuncType)(
			const ValueComparator&,
			const SourceType1&, const SourceType2&);

	typedef RetType (*FuncType)(
			const BaseWithAccessor&,
			const SourceType1&, const SourceType2&);

	typedef util::Vector<SubFuncType> SubFuncList;

	BaseWithAccessor(
			util::StackAllocator &alloc, const TupleComparator &base,
			ValueProfile *profile);

	TupleComparator base_;
	SubFuncList subFuncList_;
	FuncType func_;
	ValueProfile *profile_;
};

template<
		int32_t DigestOnly, typename Pred, bool Rev, typename A1, typename A2>
struct SQLValues::TupleComparator::VariantTraits {
	typedef ValueComparator::VariantTraits<
			Pred, Rev, A1, A2> OfValueComparator;
	typedef VariantTraits OfTupleComparator;

	template<typename V>
	struct ValueComparatorRebind {
		typedef typename V::OfValueComparator SubTraits;
		typedef VariantTraits<
				DigestOnly,
				typename SubTraits::OrgPredType,
				SubTraits::ORDERING_REVERSED,
				typename SubTraits::AccessorType1,
				typename SubTraits::OrgAccessorType2> Type;
	};

	typedef VariantTraits<
			DigestOnly, void, Rev, A1, A2> ThreeWayTraitsType;

	enum {
		HEAD_ONLY = (DigestOnly == 2 || DigestOnly == -2),
		DIGEST_ONLY = (DigestOnly == 1),
		DIGEST_UNORDERED = (DigestOnly < 0)
	};
};

template<
		typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
		typename A1, typename A2>
class SQLValues::TupleComparator::Switcher {
private:
	typedef typename ValueComparator::SwitcherOf<
			Pred, false, Promo, Ordering,
			typename A1::TraitsType::BaseType,
			typename A2::TraitsType::BaseType>::Type SubSwitcherType;
	typedef typename ValueComparator::SwitcherOf<
			Pred, Rev, Promo, Ordering,
			typename A1::TraitsType::BaseType,
			typename A2::TraitsType::BaseType>::Type MainSwitcherType;

	template<typename Base>
	struct TraitsTypeOf {
	typedef typename Base::template VariantRebind<Switcher>::Type Type;
	};

	template<typename Op>
	struct PlainOpBase {
		typedef typename SubSwitcherType::template PlainOpBase<
				Op>::FuncType FuncType;
		typedef typename TraitsTypeOf<typename SubSwitcherType::
				template PlainOpBase<Op>::TraitsType>::Type TraitsType;
	};

	typedef Switcher<Pred, Rev, Promo, Typed, false, A1, A2> UnorderedType;

public:
	typedef typename TraitsTypeOf<
			typename SubSwitcherType::DefaultTraitsType>::Type DefaultTraitsType;

	explicit Switcher(const TupleComparator &base) :
			base_(base),
			profile_(NULL) {
	}

	Switcher withProfile(ValueProfile *profile) const {
		Switcher dest = *this;
		dest.profile_ = profile;
		return dest;
	}

	template<typename Op>
	typename PlainOpBase<Op>::FuncType get() const {
		return getWith<Op, typename PlainOpBase<Op>::TraitsType>();
	}

	template<typename Op, typename Traits>
	typename Traits::template Func<Op>::Type getWith() const {
		if (base_.isSingleVarHeadOnly(true)) {
			return getSubWith<Op, Traits, 2, false>();
		}
		else if (base_.isSingleVarHeadOnly(false)) {
			return getSubWith<Op, Traits, -2, false>();
		}
		else if (base_.isDigestOnly(false)) {
			return getSubWith<Op, Traits, 1, false>();
		}
		else if (base_.isDigestOrdered()) {
			return getSubWith<Op, Traits, 0, false>();
		}
		else {
			return getSubWith<Op, Traits, -1, false>();
		}
	}

	template<typename Op>
	typename PlainOpBase<Op>::FuncType getWithOptions() const {
		typedef typename PlainOpBase<Op>::TraitsType TraitsType;
		typedef typename Op::OptionsType OptionsType;
		const bool fixedDigest = OptionsType::FixedDigest::VALUE;
		if (base_.isSingleVarHeadOnly(true)) {
			return getSubWith<Op, TraitsType, 2, false>();
		}
		else if (base_.isSingleVarHeadOnly(false)) {
			return getSubWith<Op, TraitsType, -2, false>();
		}
		else if (base_.isDigestOnly(false)) {
			return getSubWith<Op, TraitsType, 1, fixedDigest>();
		}
		else if (base_.isDigestOrdered()) {
			return getSubWith<Op, TraitsType, 0, fixedDigest>();
		}
		else {
			return getSubWith<Op, TraitsType, -1, fixedDigest>();
		}
	}

	UnorderedType toUnordered() const {
		return UnorderedType(base_);
	}

private:
	template<typename V>
	struct TupleComparatorRebind {
		typedef V Type;
	};

	template<typename Base, int32_t DigestOnly, bool FixedDigest>
	struct OpTraitsAt {
		enum {
			HEAD_SEPARATED =
					(A1::TraitsType::HEAD_SEPARATED ||
					A2::TraitsType::HEAD_SEPARATED),
			HEAD_ONLY = (DigestOnly == 2 || DigestOnly == -2),
			DIGEST_ONLY = (DigestOnly == 1),
			DIGEST_ONLY_DETAIL = ((HEAD_SEPARATED || HEAD_ONLY) ?
				DigestOnly : (DIGEST_ONLY ? 1 : 0)),

			NULLABLE_SPECIFIC_ON_SINGLE = (HEAD_ONLY ? 1 : 0),
			TYPE_SPECIFIC_ON_SINGLE = (HEAD_ONLY || FixedDigest),
			FOR_SINGLE =
					(HEAD_ONLY || (DIGEST_ONLY && (!Typed || FixedDigest)))
		};
		static const TypeSwitcher::TypeTarget TYPE_TARGET =
					(FixedDigest ? TypeSwitcher::TYPE_TARGET_FIXED :
					(HEAD_ONLY ? TypeSwitcher::TYPE_TARGET_SINGLE_VAR :
								TypeSwitcher::TYPE_TARGET_ALL));

		typedef VariantTraits<
				DIGEST_ONLY_DETAIL, void, Rev, void, void> SubTraitsType;
		typedef typename Base::VariantTraitsType::
				template TupleComparatorRebind<
						SubTraitsType>::Type VariantTraitsType;
		typedef typename Base::template VariantRebind<
				VariantTraitsType>::Type BaseType;

		typedef typename BaseType::OptionsType::template TypesRebind<
				false, NULLABLE_SPECIFIC_ON_SINGLE, TYPE_SPECIFIC_ON_SINGLE,
				TYPE_TARGET>::Type SingleOptions;
		typedef typename BaseType::template OptionsRebind<
				SingleOptions>::Type SingleType;

		typedef typename util::Conditional<
				FOR_SINGLE, SingleType, BaseType>::Type Type;
	};

	template<typename Op, typename Traits, int32_t DigestOnly, bool FixedDigest>
	typename Traits::template Func<Op>::Type getSubWith() const {
		const MainSwitcherType switcher = MainSwitcherType(
				base_.initialComparatorAt(base_.columnList_.begin(), Ordering),
				base_.nullIgnorable_).withProfile(profile_);
		return switcher.template getWith<
				Op, typename OpTraitsAt<Traits, DigestOnly, FixedDigest>::Type>();
	}

	TupleComparator base_;
	ValueProfile *profile_;
};

template<
		typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
		typename A1, typename A2>
class SQLValues::TupleComparator::WithAccessor {
public:
	template<typename T>
	struct TypeAt {
		typedef BaseTypeAt<T> TypedOp;
	};

	typedef typename SwitcherOf<
			Pred, Rev, Promo, Typed, Ordering, A1, A2>::Type SwitcherType;

	typedef typename ValueComparator::template Switcher<
			void, false, Promo, Ordering, A1, A2> SubSwitcherType;

	typedef typename ValueComparator::VariantTraits<
			Pred, Rev, A1, A2>::RetType RetType;

	typedef BaseWithAccessor<RetType, A1, A2> BaseType;

	typedef typename ValueComparator::template PredTypeOf<
			Pred, Rev>::Type PredType;

	WithAccessor(
			util::StackAllocator &alloc, const TupleComparator &base,
			ValueProfile *profile = NULL);

	operator const BaseType&() const {
		return base_;
	}

	const TupleComparator& getBase() const {
		return base_.base_;
	}

	SwitcherType getTypeSwitcher() const {
		return SwitcherType(base_.base_).withProfile(base_.profile_);
	}

	RetType operator()(
			const typename A1::SubSourceType &src1,
			const typename A2::SubSourceType &src2) const {
		return base_.func_(*this, src1, src2);
	}

	RetType operator()(
			const typename A1::DigestSourceType &src1,
			const typename A2::DigestSourceType &src2) const;

	template<typename DigestOnly>
	RetType operator()(
			const typename A1::DigestSourceType &src1,
			const typename A2::DigestSourceType &src2,
			const DigestOnly&) const {
		return TuplePred<DigestOnly::VALUE, true, false>()(*this, src1, src2);
	}

private:
	BaseType base_;
};

template<typename T>
class SQLValues::TupleComparator::BaseTypeAt {
public:
	typedef typename T::VariantTraitsType::OfValueComparator VariantTraitsType;

	typedef typename T::VariantTraitsType::OfTupleComparator
			TupleVariantTraitsType;

	typedef typename VariantTraitsType::OrgPredType OrgPredType;
	typedef typename VariantTraitsType::RetType RetType;
	typedef typename VariantTraitsType::PredType PredType;

	typedef typename VariantTraitsType::AccessorType1 AccessorType1;
	typedef typename VariantTraitsType::AccessorType2 AccessorType2;

	typedef BaseWithAccessor<
			RetType,
			typename AccessorType1::TraitsType::BaseType,
			typename AccessorType2::TraitsType::BaseType> BaseType;

	typedef BaseTypeAt<typename T::template VariantRebind<
			typename TupleVariantTraitsType::ThreeWayTraitsType>::Type>
			ThreeWayType;

	typedef BaseWithAccessor<
			ValueComparator::DefaultResultType,
			typename AccessorType1::TraitsType::BaseType,
			typename AccessorType2::TraitsType::BaseType> ThreeWayBaseType;

	explicit BaseTypeAt(const BaseType &base) : base_(base) {}

	RetType operator()(
			const typename AccessorType1::SubSourceType &src1,
			const typename AccessorType2::SubSourceType &src2) const;

	RetType operator()(
			const typename AccessorType1::SubSourceType &src1,
			const typename AccessorType2::SubSourceType &src2,
			const util::FalseType&) const;

	RetType operator()(
			const typename AccessorType1::DigestSourceType &src1,
			const typename AccessorType2::DigestSourceType &src2) const;

	ThreeWayType toThreeWay() const {
		return ThreeWayType(reinterpret_cast<const ThreeWayBaseType&>(base_));
	}

private:
	enum {
		HEAD_ONLY = TupleVariantTraitsType::HEAD_ONLY,
		DIGEST_ONLY = TupleVariantTraitsType::DIGEST_ONLY,
		DIGEST_UNORDERED = TupleVariantTraitsType::DIGEST_UNORDERED,
		DIGEST_ACCESSIBLE = (DIGEST_UNORDERED || (
				AccessorType1::TraitsType::template DigestAccessible<
						typename T::Type>::Type::VALUE &&
				AccessorType2::TraitsType::template DigestAccessible<
						typename T::Type>::Type::VALUE)),
		BODY_ONLY = (
				AccessorType1::TraitsType::template BodyOnly<
						typename T::Type>::Type::VALUE &&
				AccessorType2::TraitsType::template BodyOnly<
						typename T::Type>::Type::VALUE),
		ORDERING_REVERSED = VariantTraitsType::ORDERING_REVERSED,
	};

	typedef typename util::BoolType<!ORDERING_REVERSED>::Result SubAscending;

	typedef typename ValueComparator::template VariantTraits<
			void, false,
			typename AccessorType1::TraitsType::template HeadOf<
					util::FalseType>::Type,
			typename AccessorType2::TraitsType::template HeadOf<
					util::FalseType>::Type> HeadSubVariantTraitsType;
	typedef typename ValueComparator::template VariantTraits<
			void, false,
			typename AccessorType1::TraitsType::BodyType,
			typename AccessorType2::TraitsType::BodyType
	> BodySubVariantTraitsType;

	typedef typename ValueComparator::template TypeAt<
			typename T::template VariantRebind<
					HeadSubVariantTraitsType>::Type>::TypedOp BaseHeadSubType;
	typedef typename ValueComparator::template TypeAt<
			typename T::template VariantRebind<
					BodySubVariantTraitsType>::Type>::TypedOp BodySubType;
	typedef typename util::Conditional<
			(DIGEST_UNORDERED || BODY_ONLY),
			BodySubType, BaseHeadSubType>::Type HeadSubType;

	typedef typename HeadSubVariantTraitsType::template PredRebind<
			OrgPredType, ORDERING_REVERSED>::Type HeadOnlyVariantTraitsType;
	typedef typename BodySubVariantTraitsType::template PredRebind<
			OrgPredType, ORDERING_REVERSED>::Type BodyOnlyVariantTraitsType;

	typedef typename ValueComparator::template TypeAt<
			typename T::template VariantRebind<
					HeadOnlyVariantTraitsType>::Type>::TypedOp BaseHeadOnlyType;
	typedef typename ValueComparator::template TypeAt<
			typename T::template VariantRebind<
					BodyOnlyVariantTraitsType>::Type>::TypedOp BaseBodyOnlyType;
	typedef typename util::Conditional<
			DIGEST_UNORDERED,
			BaseBodyOnlyType, BaseHeadOnlyType>::Type HeadOnlyType;

	const BaseType &base_;
};

template<bool DigestOnly, bool DigestAccessible, bool HeadOnly>
struct SQLValues::TupleComparator::TuplePred {
	template<typename Comp, typename S1, typename S2>
	typename Comp::RetType operator()(
			const Comp&, const S1 &src1, const S2 &src2) const {
		return typename Comp::PredType()(src1.getDigest(), src2.getDigest());
	}
};

template<bool DigestAccessible>
struct SQLValues::TupleComparator::TuplePred<false, DigestAccessible, false> {
	template<typename Comp, typename S1, typename S2>
	typename Comp::RetType operator()(
			const Comp &comp, const S1 &src1, const S2 &src2) const {
		if (src1.getDigest() == src2.getDigest()) {
			return comp(src1.getTuple(), src2.getTuple());
		}
		return typename Comp::PredType()(src1.getDigest(), src2.getDigest());
	}
};

template<>
struct SQLValues::TupleComparator::TuplePred<false, true, true> {
	template<typename Comp, typename S1, typename S2>
	typename Comp::RetType operator()(
			const Comp &comp, const S1 &src1, const S2 &src2) const {
		if (src1.getDigest() == src2.getDigest()) {
			return comp(src1.getTuple(), src2.getTuple(), util::FalseType());
		}
		return typename Comp::PredType()(src1.getDigest(), src2.getDigest());
	}
};

template<>
struct SQLValues::TupleComparator::TuplePred<false, false, true> {
	template<typename Comp, typename S1, typename S2>
	typename Comp::RetType operator()(
			const Comp &comp, const S1 &src1, const S2 &src2) const {
		return comp(src1.getTuple(), src2.getTuple(), util::FalseType());
	}
};

template<>
struct SQLValues::TupleComparator::TuplePred<false, false, false> {
	template<typename Comp, typename S1, typename S2>
	typename Comp::RetType operator()(
			const Comp &comp, const S1 &src1, const S2 &src2) const {
		return comp(src1.getTuple(), src2.getTuple());
	}
};

class SQLValues::TupleRangeComparator {
public:
	template<typename T>
	class BaseTypeAt;

	template<typename A1, typename A2>
	struct BaseWithAccessor {
		template<typename T>
		struct TypeAt {
			typedef BaseTypeAt<T> TypedOp;
		};

		typedef typename ValueComparator::VariantTraits<
				void, false, A1, A2>::RetType RetType;

		typedef typename A1::SourceType SourceType1;
		typedef typename A2::SourceType SourceType2;

		typedef ValueComparator::DefaultResultType (*SubFuncType)(
				const ValueComparator&,
				const SourceType1&, const SourceType2&);

		typedef RetType (*FuncType)(
				const BaseWithAccessor&,
				const SourceType1&, const SourceType2&);

		typedef util::Vector<SubFuncType> SubFuncList;

		BaseWithAccessor(
				util::StackAllocator &alloc, const TupleComparator &base,
				const CompColumnList &columnList, ValueProfile *profile) :
				base_(base),
				columnList_(columnList),
				subFuncList_(alloc),
				func_(NULL),
				profile_(profile) {
		}

		TupleComparator base_;
		const CompColumnList &columnList_;
		SubFuncList subFuncList_;
		FuncType func_;
		ValueProfile *profile_;
	};

	template<bool Promo, typename A1, typename A2 = A1>
	struct SwitcherOf {
		typedef ValueComparator::Switcher<
				void, false, Promo, false, A1, A2> Type;
	};

	template<bool Promo, typename A1, typename A2 = A1>
	class WithAccessor {
	public:
		template<typename T>
		struct TypeAt {
			typedef BaseTypeAt<T> TypedOp;
		};

		typedef typename SwitcherOf<Promo, A1, A2>::Type SwitcherType;

		typedef typename ValueComparator::template Switcher<
				void, false, Promo, false, A1, A2> SubSwitcherType;

		typedef typename ValueComparator::VariantTraits<
				void, false, A1, A2>::RetType RetType;

		typedef BaseWithAccessor<A1, A2> BaseType;

		WithAccessor(
				util::StackAllocator &alloc, const TupleRangeComparator &base,
				ValueProfile *profile = NULL);

		operator const BaseType&() const {
			return base_;
		}

		SwitcherType getTypeSwitcher() const {
			return SwitcherType(
					base_.base_.initialComparatorAt(
							base_.base_.columnList_.begin(), false),
					base_.base_.nullIgnorable_).withProfile(base_.profile_);
		}

		RetType operator()(
				const typename A1::SourceType &src1,
				const typename A2::SourceType &src2) const {
			return base_.func_(*this, src1, src2);
		}

	private:
		BaseType base_;
	};

	template<typename T> class BaseTypeAt {
	public:
		typedef typename T::VariantTraitsType::OfValueComparator VariantTraitsType;

		typedef typename VariantTraitsType::RetType RetType;
		typedef typename VariantTraitsType::PredType PredType;

		typedef typename VariantTraitsType::AccessorType1 AccessorType1;
		typedef typename VariantTraitsType::AccessorType2 AccessorType2;

		typedef BaseWithAccessor<AccessorType1, AccessorType2> BaseType;

		explicit BaseTypeAt(const BaseType &base) : base_(base) {}

		RetType operator()(
				const typename AccessorType1::SourceType &src1,
				const typename AccessorType2::SourceType &src2) const;

	private:
		typedef typename ValueComparator::template TypeAt<T>::TypedOp SubType;

		const BaseType &base_;
	};

	TupleRangeComparator(
			const CompColumnList &columnList, VarContext *varCxt,
			bool nullIgnorable = false);

private:
	static int32_t toResultWithBounds(const CompColumn &column, int32_t ret);
	static int32_t toResultForExclusive(const CompColumn &column);

	const TupleComparator base_;
	const CompColumnList &columnList_;
	VarContext *varCxt_;
	bool nullIgnorable_;
};

class SQLValues::TupleDigester {
public:
	template<typename T> class BaseTypeAt;

	template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
	struct SwitcherOf {
		typedef ValueDigester::Switcher<
				Accessor, NullableSpecific, RotationSpecific> Type;
	};

	template<typename Accessor> class BaseWithAccessor;
	template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
	class WithAccessor;

	template<typename T> class BaseTypeAt;

	TupleDigester(
			const CompColumnList &columnList, VarContext *varCxt,
			const bool *ordering, bool rotationAllowed, bool nullIgnorable,
			bool nullableAlways);

	bool isOrdering() const;
	bool isRotating() const;

	static bool isOrderingAvailable(
			const CompColumnList &columnList, bool promotable);

	const CompColumnList& getColumnList() const {
		return columnList_;
	}

private:
	static bool isRotationAvailable(
			const CompColumnList &columnList, bool ordering,
			bool rotationAllowed);

	ValueDigester digesterAt(
			CompColumnList::const_iterator it, int64_t seed) const;

	ValueDigester initialDigesterAt(CompColumnList::const_iterator it) const;

	const CompColumnList &columnList_;
	VarContext *varCxt_;

	bool ordering_;
	bool rotating_;

	bool nullIgnorable_;
	bool nullableAlways_;
};

template<typename Accessor>
class SQLValues::TupleDigester::BaseWithAccessor {
public:
	template<typename T> struct TypeAt {
		typedef BaseTypeAt<T> TypedOp;
	};

	typedef typename Accessor::SourceType SourceType;
	typedef int64_t (*FuncType)(const BaseWithAccessor&, const SourceType&);

	typedef typename ValueDigester::template Switcher<
			Accessor, 0, false>::DefaultFuncType SubFuncType;
	typedef util::Vector<SubFuncType> SubFuncList;

	BaseWithAccessor(
			util::StackAllocator &alloc, const TupleDigester &base,
			ValueProfile *profile);

	const TupleDigester& getBase() const {
		return base_;
	}

	FuncType getFunc() const {
		return func_;
	}

	void setFunc(FuncType func) {
		func_ = func;
	}

	const SubFuncList& getSubFuncList() const {
		return subFuncList_;
	}

	SubFuncList& getSubFuncList() {
		return subFuncList_;
	}

	ValueProfile* getProfile() const {
		return profile_;
	}

private:
	TupleDigester base_;
	SubFuncList subFuncList_;
	FuncType func_;
	ValueProfile *profile_;
};

template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
class SQLValues::TupleDigester::WithAccessor {
public:
	typedef BaseWithAccessor<Accessor> AccessorBase;
	typedef typename AccessorBase::SourceType SourceType;

	template<typename T> struct TypeAt {
		typedef typename AccessorBase::template TypeAt<T>::TypedOp TypedOp;
	};

	typedef typename SwitcherOf<
			Accessor, NullableSpecific, RotationSpecific>::Type SwitcherType;

	WithAccessor(
			util::StackAllocator &alloc, const TupleDigester &base,
			ValueProfile *profile = NULL);

	SwitcherType getTypeSwitcher() const {
		return SwitcherType(
				base_.getBase().initialDigesterAt(
						base_.getBase().columnList_.begin()),
				base_.getBase().nullIgnorable_,
				base_.getBase().nullableAlways_).withProfile(base_.getProfile());
	}

	const TupleDigester& getBase() const {
		return base_.getBase();
	}

	int64_t operator()(const SourceType &src) const {
		return base_.getFunc()(*this, src);
	}

	operator const AccessorBase&() const {
		return base_;
	}

private:
	typedef SwitcherType SubSwitcherType;

	AccessorBase base_;
};

template<typename T>
class SQLValues::TupleDigester::BaseTypeAt {
public:
	typedef typename T::VariantTraitsType::OfValueDigester VariantTraitsType;
	typedef typename VariantTraitsType::AccessorType AccessorType;
	typedef BaseWithAccessor<AccessorType> BaseType;

	explicit BaseTypeAt(const BaseType &base) : base_(base) {}

	int64_t operator()(const typename AccessorType::SourceType &src) const;

private:
	enum {
		VARIANT_ORDERED = VariantTraitsType::VARIANT_ORDERED
	};

	typedef ValueDigester::VariantTraits<
			false, false, void>::VariantBase UnorderedDigester;

	static const int64_t INITIAL_SEED = static_cast<int64_t>(VARIANT_ORDERED ?
			0 : UnorderedDigester::Constants::INITIAL_SEED);

	typedef typename ValueDigester::template TypeAt<T>::TypedOp SubType;

	const BaseType &base_;
};

class SQLValues::LongInt {
public:
	typedef int64_t High;
	typedef uint32_t Low;
	typedef uint32_t UnitBase;

	class Unit;

	static LongInt ofElements(
			const High &high, const Low &low, const Unit &unit);

	static LongInt zero(const Unit &unit);
	static LongInt min(const Unit &unit);
	static LongInt max(const Unit &unit);

	static LongInt invalid();

	High getHigh() const;
	Low getLow() const;
	Unit getUnit() const;

	int64_t toLong() const;
	double toDouble() const;

	bool isValid() const;
	bool isZero() const;
	bool isNegative() const;

	bool isLessThan(const LongInt &another) const;
	bool isLessThanEq(const LongInt &another) const;

	LongInt add(const LongInt &base) const;
	LongInt subtract(const LongInt &base) const;
	LongInt multiply(const LongInt &base) const;
	LongInt divide(const LongInt &base) const;

	LongInt remainder(const LongInt &base) const;
	LongInt truncate(const LongInt &base) const;
	LongInt negate(bool negative) const;

	const LongInt& getMax(const LongInt &base) const;
	const LongInt& getMin(const LongInt &base) const;

	bool checkAdditionOverflow(const LongInt &base, bool &upper) const;

	static Unit mega();

private:
	typedef std::pair<High, Low> Pair;

	LongInt(const High &high, const Low &low, const UnitBase &unit);

	LongInt multiplyUnsigned(const LongInt &base) const;
	LongInt divideUnsigned(const LongInt &base) const;

	Pair toPair() const;

	bool isValidWith(const LongInt &another) const;
	bool isUnsignedWith(const LongInt &another) const;

	static LongInt errorZeroDivision();

	High high_;
	Low low_;
	UnitBase unit_;
};

class SQLValues::LongInt::Unit {
public:
	explicit Unit(UnitBase base);

	UnitBase get() const;

private:
	UnitBase base_;
};

class SQLValues::DateTimeElements {
public:
	explicit DateTimeElements(const int64_t &value);
	explicit DateTimeElements(const MicroTimestamp &value);
	explicit DateTimeElements(const NanoTimestamp &value);

	explicit DateTimeElements(const util::DateTime &value);
	explicit DateTimeElements(const util::PreciseDateTime &value);

	static DateTimeElements ofLongInt(const LongInt &src);

	int64_t toTimestamp(const Types::TimestampTag&) const;
	MicroTimestamp toTimestamp(const Types::MicroTimestampTag&) const;
	NanoTimestamp toTimestamp(const Types::NanoTimestampTag&) const;

	util::DateTime toDateTime(const Types::TimestampTag&) const;
	util::PreciseDateTime toDateTime(const Types::MicroTimestampTag&) const;
	util::PreciseDateTime toDateTime(const Types::NanoTimestampTag&) const;

	LongInt toLongInt() const;
	int64_t getUnixTimeMillis() const;
	uint32_t getNanoSeconds() const;

private:
	struct Constants {
		static const int64_t HIGH_MICRO_UNIT = NanoTimestamp::HIGH_MICRO_UNIT;
		static const int64_t HIGH_MICRO_REV_UNIT = 1000 / HIGH_MICRO_UNIT;
	};
	int64_t unixTimeMillis_;
	uint32_t nanoSeconds_;
};

class SQLValues::StringBuilder {
public:
	explicit StringBuilder(ValueContext &cxt, size_t capacity = 0);
	explicit StringBuilder(
			ValueContext &cxt, TupleColumnType, size_t capacity);
	explicit StringBuilder(const util::StdAllocator<void, void> &alloc);

	void setLimit(size_t limit);
	static void checkLimit(uint64_t size, size_t limit);

	void appendCode(util::CodePoint c);
	void append(const char8_t *data, size_t size);

	void appendAll(const char8_t *str);
	template<typename R> void appendAll(R &reader);

	TupleValue build(ValueContext &cxt);

	size_t size() const;
	size_t capacity() const;
	void resize(size_t size);
	char8_t* data();

private:
	static util::StdAllocator<void, void> resolveAllocator(ValueContext &cxt);

	void reserve(size_t newCapacity, size_t actualSize);

	void expandCapacity(size_t actualSize);
	void initializeDynamicBuffer(size_t newCapacity, size_t actualSize);

	static void errorLimit(uint64_t size, size_t limit);
	static void errorMaxSize(size_t current, size_t appending);

	char8_t *data_;
	util::XArray< char8_t, util::StdAllocator<char8_t, void> > dynamicBuffer_;
	size_t size_;
	size_t limit_;

	char8_t localBuffer_[128];
};

class SQLValues::LobReader {
public:
	typedef uint8_t CodeType;

	explicit LobReader(const TupleValue &value);

	void next();
	void prev();

	void step(size_t size);
	void back(size_t size);

	void toBegin();
	void toEnd();

	bool atBegin() const;
	bool atEnd() const;

	const uint8_t& get() const;

	bool hasNext() const;
	bool hasPrev() const;

	size_t getNext() const;
	size_t getPrev() const;

private:
	void updateIterator();

	typedef const uint8_t *Iterator;

	Iterator it_;
	Iterator begin_;
	Iterator end_;
	size_t index_;

	TupleValue::LobReader base_;
};

class SQLValues::LobReaderRef {
public:
	explicit LobReaderRef(const TupleValue &value);

	LobReaderRef(const LobReaderRef &another);
	LobReaderRef& operator=(const LobReaderRef &another);

	LobReader& operator*() const;

private:
	util::LocalUniquePtr<LobReader> ptr_;
	util::LocalUniquePtr<LobReader> &ptrRef_;
	TupleValue value_;
};

template<typename Base, typename T>
class SQLValues::LimitedReader {
public:
	LimitedReader(Base &base, size_t limit);

	void next();
	bool hasNext() const;

	const T& get() const;

private:
	Base &base_;
	size_t rest_;
};

template<typename T>
class SQLValues::BufferBuilder {
public:
	BufferBuilder(T *begin, T *end);

	bool isTruncated() const;
	size_t getSize() const;

	void append(const T *data, size_t size);

private:
	typedef T *Iterator;

	Iterator it_;
	Iterator begin_;
	Iterator end_;

	bool truncated_;
};

struct SQLValues::ValueUtils {
	class BytesHasher;

	enum {
		CONFIG_NAN_AS_NULL = false,
		CONFIG_STRICT_CONVERSION = true
	};


	static int32_t compareIntegral(int64_t v1, int64_t v2);
	static int32_t compareFloating(double v1, double v2, bool sensitive);
	static int32_t comparePreciseTimestamp(
			const MicroTimestamp &v1, const MicroTimestamp &v2);
	static int32_t comparePreciseTimestamp(
			const NanoTimestamp &v1, const NanoTimestamp &v2);
	static int32_t compareString(const TupleString &v1, const TupleString &v2);
	template<typename R> static int32_t compareSequence(R &v1, R &v2);

	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compareIntegral(
			int64_t v1, int64_t v2, const Pred &pred);
	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compareFloating(
			double v1, double v2, bool sensitive, const Pred &pred);
	template<typename Pred>
	static typename util::BinaryFunctionResultOf<
			Pred>::Type comparePreciseTimestamp(
			const MicroTimestamp &v1, const MicroTimestamp &v2,
			const Pred &pred);
	template<typename Pred>
	static typename util::BinaryFunctionResultOf<
			Pred>::Type comparePreciseTimestamp(
			const NanoTimestamp &v1, const NanoTimestamp &v2,
			const Pred &pred);
	template<typename Pred>
	static typename util::BinaryFunctionResultOf<Pred>::Type compareString(
			const TupleString &v1, const TupleString &v2, const Pred &pred);
	template<typename Pred, typename Ret>
	static Ret compareString(
			const TupleString &v1, const TupleString &v2, const Pred &pred);
	template<typename Pred, typename R>
	static typename util::BinaryFunctionResultOf<Pred>::Type compareSequence(
			R &v1, R &v2, const Pred &pred);

	template<typename Pred, typename Ret>
	static Ret compareBuffer(
			const uint8_t *it1, const uint8_t *it2, ptrdiff_t rest, ptrdiff_t sizeDiff,
			const Pred &pred);

	template<typename T>
	static int32_t compareRawValue(const T &v1, const T &v2);

	static int32_t compareNull(bool nullValue1, bool nullValue2);


	static int32_t getTypeBoundaryDirectionAsLong(
			const TupleValue &value, TupleColumnType boundaryType, bool lower);

	static bool isUpperBoundaryAsLong(const TupleValue &value, int64_t unit);
	static bool isLowerBoundaryAsLong(const TupleValue &value, int64_t unit);

	static TupleColumnType getBoundaryCategoryAsLong(TupleColumnType type);

	static bool isSmallestBoundaryUnitAsLong(
			const TupleValue &value, int64_t unit);
	static int32_t getBoundaryDirectionAsLong(
			const TupleValue &value, int64_t unit);


	static uint32_t fnv1aHashInit();
	static uint32_t fnv1aHashIntegral(uint32_t base, int64_t value);
	static uint32_t fnv1aHashFloating(uint32_t base, double value);
	static uint32_t fnv1aHashPreciseTimestamp(
			uint32_t base, const MicroTimestamp &value);
	static uint32_t fnv1aHashPreciseTimestamp(
			uint32_t base, const NanoTimestamp &value);
	static uint32_t fnv1aHashPreciseTimestamp(
			uint32_t base, const DateTimeElements &value);
	template<typename R> static uint32_t fnv1aHashSequence(
			uint32_t base, R &value);
	static uint32_t fnv1aHashBytes(
			uint32_t base, const void *value, size_t size);


	template<typename Asc> static int64_t digestOrderedNull(const Asc&);
	static int64_t digestOrderedIntegral(int64_t value, const util::TrueType&);
	static int64_t digestOrderedIntegral(int64_t value, const util::FalseType&);
	template<typename Asc> static int64_t digestOrderedFloating(
			double value, const Asc&);
	template<typename Asc> static int64_t digestOrderedPreciseTimestamp(
			const MicroTimestamp &value, const Asc&);
	template<typename Asc> static int64_t digestOrderedPreciseTimestamp(
			const NanoTimestamp &value, const Asc&);
	template<typename Asc, typename R> static int64_t digestOrderedSequence(
			R &value, const Asc&);

	template<typename T, typename Asc>
	static typename T::ValueType toValueByOrderdDigest(int64_t digest);
	template<typename Asc> static TupleValue toValueByOrderdDigest(
			int64_t digest, const Asc&, const Types::Any&);
	template<typename Asc> static int64_t toValueByOrderdDigest(
			int64_t digest, const Asc&, const Types::Integral&);
	template<typename Asc> static double toValueByOrderdDigest(
			int64_t digest, const Asc&, const Types::Floating&);
	template<typename Asc> static MicroTimestamp toValueByOrderdDigest(
			int64_t digest, const Asc&, const Types::MicroTimestampTag&);

	static int64_t rotateDigest(int64_t digest, const util::TrueType&);
	static int64_t rotateDigest(int64_t digest, const util::FalseType&);

	static int64_t digestByRotation(int64_t rotated, const util::TrueType&);
	static int64_t digestByRotation(int64_t rotated, const util::FalseType&);


	template<TupleColumnType T>
	static typename TypeUtils::Traits<T>::LocalValueType promoteValue(
			const TupleValue &src);

	static int64_t toLong(const TupleValue &src);
	static double toDouble(const TupleValue &src);
	template<typename T> static T toIntegral(const TupleValue &src);
	template<typename T> static T toFloating(const TupleValue &src);

	static LongInt toLongInt(const TupleValue &src);
	static DateTimeElements toTimestamp(
			ValueContext &cxt, const TupleValue &src,
			util::DateTime::FieldType precision);

	static bool toBool(const TupleValue &src);
	static TupleValue toString(ValueContext &cxt, const TupleValue &src);
	static TupleValue toBlob(ValueContext &cxt, const TupleValue &src);

	static bool toLongDetail(
			const TupleValue &src, int64_t &dest, bool strict);
	static bool toDoubleDetail(
			const TupleValue &src, double &dest, bool strict);
	template<typename T> static bool toIntegralDetail(
			const TupleValue &src, T &dest, bool strict);
	template<typename T> static bool toFloatingDetail(
			const TupleValue &src, T &dest, bool strict);

	static TupleString::BufferInfo toNumericStringByBlob(
			const TupleValue &src, char8_t *buf, size_t bufSize);

	static bool findLong(const TupleValue &src, int64_t &dest);
	static bool findDouble(const TupleValue &src, double &dest);


	static void formatValue(
			ValueContext &cxt, StringBuilder &builder,
			const TupleValue &value);

	static void formatLong(StringBuilder &builder, int64_t value);
	static void formatDouble(StringBuilder &builder, double value);
	static void formatTimestamp(
			StringBuilder &builder, const DateTimeElements &value,
			const util::TimeZone &zone, util::DateTime::FieldType precision);
	static void formatBool(StringBuilder &builder, bool value);
	static void formatBlob(StringBuilder &builder, const TupleValue &value);


	static bool parseLong(
			const TupleString::BufferInfo &src, int64_t &dest, bool strict);
	static bool parseDouble(
			const TupleString::BufferInfo &src, double &dest, bool strict);
	static DateTimeElements parseTimestamp(
			const TupleString::BufferInfo &src, const util::TimeZone &zone,
			bool zoneSpecified, util::DateTime::FieldType precision,
			util::StdAllocator<void, void> alloc);
	static bool parseBool(
			const TupleString::BufferInfo &src, bool &dest, bool strict);
	static void parseBlob(
			const TupleString::BufferInfo &src, LobBuilder &dest);


	template<TupleColumnType T> static TupleValue toAnyByValue(
			const typename TypeUtils::Traits<T>::ValueType &src);

	template<TupleColumnType T> static TupleValue toAnyByWritable(
			ValueContext &cxt,
			typename TypeUtils::Traits<T>::WritableRefType src);

	template<typename T> static TupleValue toAnyByNumeric(T src);
	static TupleValue toAnyByLongInt(ValueContext &cxt, const LongInt &src);
	static TupleValue toAnyByTimestamp(int64_t src);
	static TupleValue toAnyByBool(bool src);

	static TupleValue createNanoTimestampValue(
			util::StackAllocator &alloc, const NanoTimestamp &ts);
	static TupleValue createNanoTimestampValue(
			ValueContext &cxt, const NanoTimestamp &ts);

	static TupleValue createEmptyValue(
			ValueContext &cxt, TupleColumnType type);


	template<typename T> static T getValue(const TupleValue &src);

	template<typename T>
	static typename T::ValueType getValueByComparable(
			const typename TypeUtils::Traits<
					T::COLUMN_TYPE>::ComparableValueType &src);

	template<typename T>
	static typename T::ValueType getValueByAddress(const void *src);
	template<typename T>
	static const void* getAddressByValue(const typename T::ValueType &src);

	template<typename T, typename S>
	static typename T::LocalValueType getPromoted(
			const typename S::LocalValueType &src);

	template<TupleColumnType T>
	static typename TypeUtils::Traits<T>::ReadableSourceType toReadableSource(
			const TupleValue &src);

	static TupleString::BufferInfo toStringBuffer(const char8_t *src);
	static TupleString::BufferInfo toStringBuffer(const TupleString &src);

	template<TupleColumnType T>
	static typename TypeUtils::Traits<T>::LocalReaderRefType toReader(
			const typename TypeUtils::Traits<T>::ValueType &src);

	static StringReaderRef toStringReader(const char8_t *src);
	static StringReaderRef toStringReader(const TupleString &src);
	static StringReaderRef toStringReader(const TupleString::BufferInfo &src);

	static LobReaderRef toLobReader(const TupleValue &src);

	static StringBuilder& initializeWriter(
			ValueContext &cxt, util::LocalUniquePtr<StringBuilder> &writerPtr,
			uint64_t initialCapacity);
	static LobBuilder& initializeWriter(
			ValueContext &cxt, util::LocalUniquePtr<LobBuilder> &writerPtr,
			uint64_t initialCapacity);

	template<typename T>
	static typename TypeUtils::template Traits<
			T::COLUMN_TYPE>::WriterContextType& toWriterContext(
			ValueContext &cxt);

	template<typename T>
	static size_t toWriterCapacity(uint64_t base);

	static const void* getData(const TupleValue &value);
	static size_t getDataSize(const TupleValue &value);


	static bool isNull(const TupleValue &value);

	static TupleColumnType toColumnType(const TupleValue &value);


	static DateTimeElements checkTimestamp(
			const DateTimeElements& tsValue);

	static DateTimeElements getMinTimestamp();
	static DateTimeElements getMaxTimestamp();

	static util::DateTime::Option getDefaultDateTimeOption();

	static void applyDateTimeOption(util::DateTime::ZonedOption &option);
	static void applyDateTimeOption(util::DateTime::Option &option);

	static util::DateTime::FieldType toTimestampField(int64_t value);


	static bool isTrue(const TupleValue &value);
	static bool isFalse(const TupleValue &value);


	template<typename R>
	static int64_t codeLength(R &reader);
	template<typename R>
	static int64_t codeLength(R &reader, const util::TrueType&);
	template<typename R>
	static int64_t codeLength(R &reader, const util::FalseType&);

	template<typename R>
	static int64_t codeLengthLimited(R &reader, int64_t limitSize);
	template<typename R>
	static int64_t codeLengthLimited(
			R &reader, int64_t limitSize, const util::TrueType&);
	template<typename R>
	static int64_t codeLengthLimited(
			R &reader, int64_t limitSize, const util::FalseType&);

	template<typename R>
	static int64_t partLength(R &reader);

	template<typename R>
	static int64_t stepAllPart(R &reader, int64_t size);
	template<typename R>
	static int64_t backAllPart(R &reader, int64_t size);

	template<typename R>
	static int64_t findPart(R &reader1, R &reader2, int64_t offset1);
	template<typename R>
	static int64_t findPartBack(R &reader1, R &reader2, int64_t offset1);

	template<typename R>
	static int64_t findLtrimPart(R &reader1, R &reader2);
	template<typename R>
	static int64_t findRtrimPart(R &reader1, R &reader2);

	template<typename R>
	static int64_t findPartRepeatable(
			R &reader1, R &reader2, int64_t startPos1, int64_t repeat);

	template<typename R, bool Escape>
	static bool matchPartLike(R &reader1, R &reader2, R *reader3);

	template<typename W, typename R>
	static bool unescapeExactPartLikePattern(
			W &writer, R &patternReader, R *escapeReader);

	static bool unescapeExactLikePattern(
			ValueContext &cxt, const TupleValue &pattern, const TupleValue *escape,
			TupleValue &unescaped);

	template<typename R>
	static util::CodePoint getPartLikeEscape(R &escapeReader);

	static int32_t orderPartNoCase(
			const TupleString::BufferInfo &buf1,
			const TupleString::BufferInfo &buf2);

	template<typename R1, typename R2>
	static int32_t orderPart(R1 &reader1, R2 &reader2);
	template<typename R1, typename R2>
	static int32_t orderPartNoCase(R1 &reader1, R2 &reader2);

	template<typename W, typename R, typename Bounded>
	static void subCodedPart(
			W &writer, R &reader, int64_t start, int64_t limit,
			bool limited, const Bounded&);
	template<typename W, typename R, typename Bounded>
	static void subCodedPart(
			W &writer, R &reader, int64_t start, int64_t limit,
			bool limited, const Bounded&, const util::TrueType&);
	template<typename W, typename R, typename Bounded>
	static void subCodedPart(
			W &writer, R &reader, int64_t start, int64_t limit,
			bool limited, const Bounded&, const util::FalseType&);

	template<typename R, typename Bounded>
	static std::pair<int64_t, int64_t> findSubPart(
			R &reader, int64_t start, int64_t limit, bool limited,
			const Bounded&);
	template<typename R>
	static std::pair<int64_t, int64_t> findSubPart(R &reader, int64_t limit);

	template<typename W, typename R, typename Bounded>
	static void subPart(
			W &writer, R &reader, int64_t start, int64_t limit, bool limited,
			const Bounded&);
	template<typename W, typename R>
	static void subPart(W &writer, R &reader, int64_t limit);
	template<typename R>
	static void subPart(R &reader, int64_t limit);

	template<typename W, typename R>
	static void subForwardPart(W &writer, R &reader, uint64_t limit);

	template<typename W, typename R>
	static void partToHexString(W &writer, R &reader);
	template<typename W, typename R>
	static void hexStringToPart(W &writer, R &reader);

	template<typename W, typename T>
	static void writePart(W &writer, const T *data, size_t size);
	template<typename T>
	static void writePart(util::FalseType&, const T*, size_t);

	template<typename S, typename R>
	static void partToString(S &str, R &reader);
	template<typename R>
	static TupleString::BufferInfo partToStringBuffer(R &reader);

	static util::CodePoint toLower(util::CodePoint c);
	static util::CodePoint toUpper(util::CodePoint c);

	static util::String valueToString(ValueContext &cxt, const TupleValue &value);


	static size_t toLobCapacity(uint64_t size);


	static TupleValue duplicateValue(ValueContext &cxt, const TupleValue &src);
	static void destroyValue(ValueContext &cxt, TupleValue &value);

	static TupleValue duplicateAllocValue(
			const util::StdAllocator<void, void> &alloc, const TupleValue &src);
	static void destroyAllocValue(
			const util::StdAllocator<void, void> &alloc, TupleValue &value);

	static const void* getValueBody(const TupleValue &value, size_t &size);

	static void moveValueToRoot(
			ValueContext &cxt, TupleValue &value,
			size_t currentDepth, size_t targetDepth);

	static size_t estimateTupleSize(const TupleColumnList &list);

	static uint64_t getApproxPrimeNumber(uint64_t base);
	static uint64_t findLargestPrimeNumber(uint64_t base);


	template<typename T> static typename T::ValueType readCurrentValue(
			TupleListReader &reader, const TupleColumn &column);
	static TupleValue readCurrentAny(
			TupleListReader &reader, const TupleColumn &column);
	static bool readCurrentNull(
			TupleListReader &reader, const TupleColumn &column);

	template<typename T> static typename T::ValueType readValue(
			const ReadableTuple &tuple, const TupleColumn &column);
	static TupleValue readAny(
			const ReadableTuple &tuple, const TupleColumn &column);
	static bool readNull(
			const ReadableTuple &tuple, const TupleColumn &column);

	template<typename T> static void writeValue(
			WritableTuple &tuple, const TupleColumn &column,
			const typename T::LocalValueType &value);
	static void writeAny(
			WritableTuple &tuple, const TupleColumn &column,
			const TupleValue &value);
	static void writeNull(WritableTuple &tuple, const TupleColumn &column);

	static void updateKeepInfo(TupleListReader &reader);

private:
	struct Constants;
	struct TimeErrorHandler;
	struct ReaderWrapper;
	struct TupleWrapper;

	static util::DateTime::FieldData resolveTimestampMaxFields() throw();
	static int64_t resolveTimestampMaxUnixTime() throw();

	template<typename T, typename U>
	static U errorNumericOverflow(T value, U limit, bool strict);

	static void errorVarContextDepth();
};

template<>
int64_t SQLValues::ValueUtils::promoteValue<TupleTypes::TYPE_LONG>(
		const TupleValue &src);
template<>
double SQLValues::ValueUtils::promoteValue<TupleTypes::TYPE_DOUBLE>(
		const TupleValue &src);
template<>
int64_t SQLValues::ValueUtils::promoteValue<TupleTypes::TYPE_TIMESTAMP>(
		const TupleValue &src);
template<>
MicroTimestamp SQLValues::ValueUtils::promoteValue<
		TupleTypes::TYPE_MICRO_TIMESTAMP>(const TupleValue &src);
template<>
NanoTimestamp SQLValues::ValueUtils::promoteValue<
		TupleTypes::TYPE_NANO_TIMESTAMP>(const TupleValue &src);

template<> TupleValue SQLValues::ValueUtils::toAnyByValue<
		TupleTypes::TYPE_TIMESTAMP>(const int64_t &src);

template<> TupleValue SQLValues::ValueUtils::toAnyByWritable<
		TupleTypes::TYPE_NANO_TIMESTAMP>(
		ValueContext &cxt, const NanoTimestamp &src);
template<> TupleValue SQLValues::ValueUtils::toAnyByWritable<
		TupleTypes::TYPE_STRING>(ValueContext &cxt, StringBuilder &src);
template<> TupleValue SQLValues::ValueUtils::toAnyByWritable<
		TupleTypes::TYPE_BLOB>(ValueContext &cxt, LobBuilder &src);

template<>
TupleValue SQLValues::ValueUtils::getValue(const TupleValue &src);
template<>
int8_t SQLValues::ValueUtils::getValue(const TupleValue &src);
template<>
int16_t SQLValues::ValueUtils::getValue(const TupleValue &src);
template<>
int32_t SQLValues::ValueUtils::getValue(const TupleValue &src);
template<>
float SQLValues::ValueUtils::getValue(const TupleValue &src);
template<>
bool SQLValues::ValueUtils::getValue(const TupleValue &src);
template<>
TupleString SQLValues::ValueUtils::getValue(const TupleValue &src);

template<>
int64_t SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::TimestampTag, SQLValues::Types::MicroTimestampTag>(
		const MicroTimestamp &src);
template<>
int64_t SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::TimestampTag, SQLValues::Types::NanoTimestampTag>(
		const NanoTimestamp &src);
template<>
MicroTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::MicroTimestampTag, SQLValues::Types::TimestampTag>(
		const int64_t &src);
template<>
MicroTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::MicroTimestampTag, SQLValues::Types::NanoTimestampTag>(
		const NanoTimestamp &src);
template<>
NanoTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::NanoTimestampTag, SQLValues::Types::TimestampTag>(
		const int64_t &src);
template<>
NanoTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::NanoTimestampTag, SQLValues::Types::MicroTimestampTag>(
		const MicroTimestamp &src);

template<>
bool SQLValues::ValueUtils::getValueByComparable<
		SQLValues::Types::Bool>(
		const TypeUtils::Traits<
				Types::Bool::COLUMN_TYPE>::ComparableValueType &src);

template<>
TupleNanoTimestamp SQLValues::ValueUtils::getValueByAddress<
		SQLValues::Types::NanoTimestampTag>(const void *src);
template<>
TupleString SQLValues::ValueUtils::getValueByAddress<
		SQLValues::Types::String>(const void *src);

template<>
TupleString::BufferInfo SQLValues::ValueUtils::toReadableSource<
		TupleTypes::TYPE_STRING>(const TupleValue &src);

template<>
TupleValue SQLValues::ValueUtils::readCurrentValue<SQLValues::Types::Any>(
		TupleListReader &reader, const TupleColumn &column);
template<>
TupleValue SQLValues::ValueUtils::readValue<SQLValues::Types::Any>(
		const ReadableTuple &tuple, const TupleColumn &column);

struct SQLValues::ValueUtils::Constants {
	static const char8_t CONSTANT_TRUE_STR[];
	static const char8_t CONSTANT_FALSE_STR[];

	static const double INT64_MIN_AS_DOUBLE;
	static const double INT64_MAX_AS_DOUBLE;

	static const util::DateTime::FieldData TIMESTAMP_MAX_FIELDS;
	static const int64_t TIMESTAMP_MAX_UNIX_TIME;

	static const uint32_t LARGEST_PRIME_OF_BITS[];
	static const size_t LARGEST_PRIME_OF_BITS_COUNT;
};

struct SQLValues::ValueUtils::TimeErrorHandler {
	void errorTimeFormat(std::exception &e) const;
};

struct SQLValues::ValueUtils::ReaderWrapper {
	struct Readable {
		explicit Readable(TupleListReader &reader) : reader_(reader) {
		}

		template<typename T> T getAs(const TupleColumn &column) const {
			return getValue<T>(readCurrentAny(reader_, column));
		}

		TupleListReader &reader_;
	};

	template<typename T>
	struct WrapperOf {
	private:
		typedef TupleListReader SourceType;
		typedef Readable WrapperType;

		typedef typename TypeUtils::template Traits<T::COLUMN_TYPE> TypeTraits;
		typedef typename util::BoolType<
				!TypeTraits::READABLE_TUPLE_SPECIALIZED>::Result Wrapping;

		typedef typename util::Conditional<
				Wrapping::VALUE, WrapperType, SourceType&>::Type RetType;


		WrapperType operator()(SourceType &src, const util::TrueType&) const {
			return WrapperType(src);
		}

		SourceType& operator()(SourceType &src, const util::FalseType&) const {
			return src;
		}

	public:
		RetType operator()(SourceType &src) const {
			return (*this)(src, Wrapping());
		}
	};
};

struct SQLValues::ValueUtils::TupleWrapper {
	struct Readable {
		explicit Readable(const ReadableTuple &tuple) : tuple_(tuple) {
		}

		template<typename T> T getAs(const TupleColumn &column) const {
			return getValue<T>(readAny(tuple_, column));
		}

		const ReadableTuple &tuple_;
	};

	template<typename T>
	struct Writable {
		explicit Writable(WritableTuple &tuple) : tuple_(tuple) {
		}

		void setBy(
				const TupleColumn &column,
				const typename T::LocalValueType &value) const {
			writeAny(tuple_, column, toAnyByValue<T::COLUMN_TYPE>(value));
		}

		WritableTuple &tuple_;
	};

	template<typename T, bool ForRead>
	struct WrapperOf {
	private:
		typedef typename util::Conditional<
				ForRead, const ReadableTuple, WritableTuple>::Type SourceType;
		typedef typename util::Conditional<
				ForRead, Readable, Writable<T> >::Type WrapperType;

		typedef typename TypeUtils::template Traits<T::COLUMN_TYPE> TypeTraits;
		typedef typename util::BoolType<!(ForRead ?
				TypeTraits::READABLE_TUPLE_SPECIALIZED :
				TypeTraits::WRITABLE_TUPLE_SPECIALIZED)>::Result Wrapping;

		typedef typename util::Conditional<
				Wrapping::VALUE, WrapperType, SourceType&>::Type RetType;


		WrapperType operator()(SourceType &src, const util::TrueType&) const {
			return WrapperType(src);
		}

		SourceType& operator()(SourceType &src, const util::FalseType&) const {
			return src;
		}

	public:
		RetType operator()(SourceType &src) const {
			return (*this)(src, Wrapping());
		}
	};
};

class SQLValues::ReadableTupleRef {
private:
	enum {
		DIGESTER_ORDERING = true,
		DIGESTER_ROTATING = true,
		DIGESTER_NON_ROTATING = false,
	};

public:
	typedef DigestTupleListReader DigestTuple;

	template<typename D>
	class WithDigester {
	public:
		WithDigester(
				util::StackAllocator &alloc, TupleListReader &reader,
				const D &digester);
		explicit WithDigester(const util::FalseType&);

		bool isEmpty() const {
			return (base_ == NULL);
		}

		operator const DigestTuple&() const {
			return base_->digestTuple_;
		}

		TupleListReader& getReader() const {
			return *base_->digestTuple_.getReader();
		}

		bool next() const;

		template<typename T>
		bool nextAt() const;

	private:
		struct RefBase {
			RefBase(TupleListReader &reader, const D &digester);

			DigestTuple digestTuple_;
			D digester_;
			bool digesterRotating_;
		};

		template<typename T>
		struct DigesterOf {

			typedef typename T::VariantTraitsType::OfValueComparator
					ComparatorVariantTraits;
			typedef typename ComparatorVariantTraits::AccessorType1
					AccessorType;

			typedef ValueDigester::VariantTraits<
					DIGESTER_ORDERING, true, AccessorType> DigesterVariantTraits;
			typedef ValueDigester::VariantTraits<
					DIGESTER_ORDERING, false, AccessorType> RotDigesterVariantTraits;

			typedef TypeSwitcher::TypeTraits<
					typename T::Type, void, typename T::NullableType,
					DigesterVariantTraits> TraitsType;
			typedef typename TraitsType::template VariantRebind<
					RotDigesterVariantTraits>::Type RotTraitsType;

			typedef typename D::template TypeAt<TraitsType>::TypedOp Type;
			typedef typename D::template TypeAt<RotTraitsType>::TypedOp RotType;
		};

		RefBase *base_;
	};
};

class SQLValues::ArrayTupleRef {
private:
	template<typename D>
	struct RefBase;

public:
	typedef DigestArrayTuple DigestTuple;

	template<typename D>
	class WithDigester {
	public:
		explicit WithDigester(RefBase<D> &base);

		operator const DigestTuple&() const {
			return base_->digestTuple_;
		}

		DigestTuple& getTuple() const {
			return base_->digestTuple_;
		}

		TupleListReader& getReader() const {
			return base_->reader_;
		}

		bool next() const;

		template<typename>
		bool nextAt() const {
			return next();
		}

	private:
		RefBase<D> *base_;
	};

	template<typename D>
	class RefList {
	public:
		typedef WithDigester<D> RefType;

		explicit RefList(util::StackAllocator &alloc);
		~RefList();

		RefType add(
				ValueContext &valueCxt, TupleListReader &reader,
				const D &digester, const ArrayTuple::Assigner &assigner);

	private:
		typedef RefBase<D> Base;

		RefList(const RefList&);
		RefList& operator=(const RefList&);

		util::Vector<Base*> baseList_;
	};

private:

	template<typename D>
	struct RefBase {
		RefBase(
				ValueContext &valueCxt, TupleListReader &reader,
				const D &digester, const ArrayTuple::Assigner &assigner);
		void update(const ReadableTuple &tuple);

		DigestTuple digestTuple_;
		ValueContext &valueCxt_;
		TupleListReader &reader_;
		D digester_;
		ArrayTuple::Assigner assigner_;
	};

	ArrayTuple *tuple_;
};

class SQLValues::SharedId {
public:
	explicit SharedId();

	SharedId(const SharedId &another);
	SharedId& operator=(const SharedId &another);

	~SharedId();

	void assign(SharedIdManager &manager);
	void clear();

	bool isEmpty() const;
	bool operator==(const SharedId &another) const;
	bool operator<(const SharedId &another) const;

	size_t getIndex(const SharedIdManager &manager) const;

private:
	void assign(SharedIdManager *manager, size_t orgIndex, uint64_t orgId);

	static size_t errorIndex();

	size_t index_;
	uint64_t id_;
	SharedIdManager *manager_;
};

class SQLValues::SharedIdManager {
public:
	explicit SharedIdManager(const util::StdAllocator<void, void> &alloc);
	~SharedIdManager();

private:
	friend class SharedId;

	struct Entry {
		Entry();

		uint64_t id_;
		uint64_t refCount_;
	};

	typedef util::Vector< Entry, util::StdAllocator<Entry, void> > EntryList;
	typedef util::Vector< uint64_t, util::StdAllocator<uint64_t, void> > IndexList;

	SharedIdManager(const SharedIdManager&);
	SharedIdManager& operator=(const SharedIdManager&);

	size_t allocate(
			size_t orgIndex, uint64_t orgId, void *key, uint64_t *nextId);
	void release(size_t index, uint64_t id, void *key);

	void checkKey(bool allocating, uint64_t id, void *key);


	Entry& getEntry(size_t index, uint64_t id);

	Entry& createEntry(size_t *nextIndex);
	void removeEntry(Entry &entry, size_t index);

	static void errorRefCount();
	static Entry& errorEntry();

	EntryList entryList_;
	IndexList freeList_;
	uint64_t lastId_;

};

class SQLValues::LatchHolder {
public:
	LatchHolder();
	~LatchHolder();

	void reset(BaseLatchTarget *target = NULL) throw();
	void close() throw();

private:
	BaseLatchTarget *target_;
};

class SQLValues::BaseLatchTarget {
public:
	virtual ~BaseLatchTarget();

	virtual void unlatch() throw() = 0;
	virtual void close() throw() = 0;

protected:
	BaseLatchTarget();

private:
	BaseLatchTarget(const BaseLatchTarget&);
	BaseLatchTarget& operator=(const BaseLatchTarget&);
};



template<typename Op>
typename SQLValues::TypeSwitcher::OpTraitsBase<Op>::FuncType
SQLValues::TypeSwitcher::get() const {
	typedef ResolvingSubOp<Op, util::FalseType> SubOp;
	return operate<SubOp>(SubOp());
}

template<typename Op, typename Traits>
typename SQLValues::TypeSwitcher::OpTraitsBase<Op, Traits>::FuncType
SQLValues::TypeSwitcher::getWith() const {
	typedef ResolvingSubOp<Op, Traits> SubOp;
	return operate<SubOp>(SubOp());
}

template<typename Op>
inline typename Op::RetType
SQLValues::TypeSwitcher::operator()(Op &op) const {
	typedef ExecutingSubOp<Op, util::FalseType> SubOp;
	return operate<SubOp>(SubOp(op));
}

template<typename SubOp>
typename SubOp::RetType SQLValues::TypeSwitcher::operate(
		const SubOp &op) const {
	typedef typename SubOp::FilterType Filter;
	switch (getMaskedType1()) {
	case TupleTypes::TYPE_BYTE:
		return operate<SubOp, typename Filter::template First<
				Types::Byte>::Type>(op);
	case TupleTypes::TYPE_SHORT:
		return operate<SubOp, typename Filter::template First<
				Types::Short>::Type>(op);
	case TupleTypes::TYPE_INTEGER:
		return operate<SubOp, typename Filter::template First<
				Types::Integer>::Type>(op);
	case TupleTypes::TYPE_LONG:
		return operate<SubOp, typename Filter::template First<
				Types::Long>::Type>(op);
	case TupleTypes::TYPE_FLOAT:
		return operate<SubOp, typename Filter::template First<
				Types::Float>::Type>(op);
	case TupleTypes::TYPE_DOUBLE:
		return operate<SubOp, typename Filter::template First<
				Types::Double>::Type>(op);
	case TupleTypes::TYPE_TIMESTAMP:
		return operate<SubOp, typename Filter::template First<
				Types::TimestampTag>::Type>(op);
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return operate<SubOp, typename Filter::template First<
				Types::MicroTimestampTag>::Type>(op);
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		return operate<SubOp, typename Filter::template First<
				Types::NanoTimestampTag>::Type>(op);
	case TupleTypes::TYPE_BOOL:
		return operate<SubOp, typename Filter::template First<
				Types::Bool>::Type>(op);
	case TupleTypes::TYPE_STRING:
		return operate<SubOp, typename Filter::template First<
				Types::String>::Type>(op);
	case TupleTypes::TYPE_BLOB:
		return operate<SubOp, typename Filter::template First<
				Types::Blob>::Type>(op);
	default:
		assert(TypeUtils::isAny(getMaskedType1()));
		return operate<SubOp, typename Filter::template First<
				Types::Any>::Type>(op);
	}
}

template<typename SubOp, typename T1>
typename SubOp::RetType SQLValues::TypeSwitcher::operate(
		const SubOp &op) const {
	typedef typename SubOp::FilterType Filter;
	switch (getMaskedType2()) {
	case TupleTypes::TYPE_BYTE:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::Byte>::Type>(op);
	case TupleTypes::TYPE_SHORT:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::Short>::Type>(op);
	case TupleTypes::TYPE_INTEGER:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::Integer>::Type>(op);
	case TupleTypes::TYPE_LONG:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::Long>::Type>(op);
	case TupleTypes::TYPE_FLOAT:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::Float>::Type>(op);
	case TupleTypes::TYPE_DOUBLE:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::Double>::Type>(op);
	case TupleTypes::TYPE_TIMESTAMP:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::TimestampTag>::Type>(op);
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::MicroTimestampTag>::Type>(op);
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		return operate<SubOp, T1, typename Filter::template Second<
				T1, Types::NanoTimestampTag>::Type>(op);
	default:
		assert(getMaskedType1() == getMaskedType2() ||
				(SubOp::TraitsType::TYPE_COUNT ==1 &&
				TypeUtils::isNull(getMaskedType2())));
		return operate<SubOp, T1, typename Filter::template Second<
				T1, T1>::Type>(op);
	}
}

template<typename SubOp, typename T1, typename T2>
typename SubOp::RetType SQLValues::TypeSwitcher::operate(
		const SubOp &op) const {
	assert(!isNullableAlways() || SubOp::TraitsType::NULLABLE_SPECIFIC == 2);
	typedef typename SubOp::FilterType Filter;

	typedef typename Filter::template Nullable<T1, T2, true>::Type Nullable;
	typedef typename Filter::template Nullable<T1, T2, false>::Type NonNullable;

	if (checkNullable(!util::IsSame<NonNullable, void>::VALUE)) {
		return operate<SubOp, T1, T2, Nullable>(op);
	}
	else {
		return operate<SubOp, T1, T2, NonNullable>(op);
	}
}

template<typename SubOp, typename T1, typename T2, typename Nullable>
typename SubOp::RetType SQLValues::TypeSwitcher::operate(
		const SubOp &op) const {
	typedef typename SubOp::template TypeAt<T1, T2, Nullable>::TypedOp TypedOp;
	return TypedOp(op)();
}


inline SQLValues::VarContext& SQLValues::ValueContext::getVarContext() {
	assert(varCxt_ != NULL);
	return *varCxt_;
}

inline size_t SQLValues::ValueContext::getMaxStringLength() {
	const size_t len = maxStringLength_;
	if (len == 0) {
		return resolveMaxStringLength();
	}
	return len;
}


inline SQLValues::Decremental::ValueCounter::ValueCounter() :
		count_(0) {
}

inline uint64_t SQLValues::Decremental::ValueCounter::get() const {
	return count_;
}

inline void SQLValues::Decremental::ValueCounter::increment() {
	count_++;
}

inline void SQLValues::Decremental::ValueCounter::decrement() {
	assert(count_ > 0);
	count_--;
}


inline SQLValues::Decremental::AsLong::AsLong() :
		base_(0) {
}

inline int64_t SQLValues::Decremental::AsLong::get() const {
	return base_;
}

inline void SQLValues::Decremental::AsLong::add(
		int64_t value, const util::TrueType&) {
	base_ += value;
	counter_.increment();
}

inline void SQLValues::Decremental::AsLong::add(
		int64_t value, const util::FalseType&) {
	base_ -= value;
	counter_.decrement();
}

inline void SQLValues::Decremental::AsLong::increment(const util::TrueType&) {
	base_++;
}

inline void SQLValues::Decremental::AsLong::increment(const util::FalseType&) {
	base_--;
}

inline const SQLValues::Decremental::ValueCounter&
SQLValues::Decremental::AsLong::getCounter() const {
	return counter_;
}


inline SQLValues::Decremental::AsDouble::AsDouble() :
		base_(0) {
}

inline double SQLValues::Decremental::AsDouble::get() const {
	return base_;
}

inline void SQLValues::Decremental::AsDouble::add(
		double value, const util::TrueType&) {
	sum_.add(value);
	base_ = sum_.get();
	counter_.increment();
}

inline void SQLValues::Decremental::AsDouble::add(
		double value, const util::FalseType&) {
	sum_.remove(value);
	base_ = sum_.get();
	counter_.decrement();
}

inline const SQLValues::Decremental::ValueCounter&
SQLValues::Decremental::AsDouble::getCounter() const {
	return counter_;
}


inline const SQLValues::TupleColumn&
SQLValues::SummaryColumn::getTupleColumn() const {
	return tupleColumn_;
}

inline bool SQLValues::SummaryColumn::isEmpty() const {
	return (pos_ < 0);
}

inline int32_t SQLValues::SummaryColumn::getOffset() const {
	return offset_;
}

inline int32_t SQLValues::SummaryColumn::getNullsOffset() const {
	return nullsOffset_;
}

inline SQLValues::SummaryColumn::NullsUnitType
SQLValues::SummaryColumn::getNullsMask() const {
	return nullsMask_;
}

inline uint32_t SQLValues::SummaryColumn::getSummaryPosition() const {
	assert(pos_ >= 0);
	return static_cast<uint32_t>(pos_);
}

inline bool SQLValues::SummaryColumn::isOnBody() const {
	return (offset_ >= 0);
}

inline bool SQLValues::SummaryColumn::isOrdering() const {
	return (ordering_ >= 0);
}

inline bool SQLValues::SummaryColumn::isRotating() const {
	return (ordering_ > 0);
}


inline const SQLValues::TupleColumn&
SQLValues::CompColumn::getTupleColumn1() const {
	return summaryColumn1_.getTupleColumn();
}

inline const SQLValues::TupleColumn&
SQLValues::CompColumn::getTupleColumn2() const {
	return summaryColumn2_.getTupleColumn();
}

inline const SQLValues::TupleColumn&
SQLValues::CompColumn::getTupleColumn(const util::TrueType&) const {
	return getTupleColumn1();
}

inline const SQLValues::TupleColumn&
SQLValues::CompColumn::getTupleColumn(const util::FalseType&) const {
	return getTupleColumn2();
}

inline const SQLValues::SummaryColumn&
SQLValues::CompColumn::getSummaryColumn(const util::TrueType&) const {
	return summaryColumn1_;
}

inline const SQLValues::SummaryColumn&
SQLValues::CompColumn::getSummaryColumn(const util::FalseType&) const {
	return summaryColumn2_;
}

inline uint32_t SQLValues::CompColumn::getColumnPos(const util::TrueType&) const {
	return columnPos1_;
}

inline uint32_t SQLValues::CompColumn::getColumnPos(const util::FalseType&) const {
	return columnPos2_;
}

inline bool SQLValues::CompColumn::isAscending() const {
	return ascending_;
}

inline bool SQLValues::CompColumn::isEitherUnbounded() const {
	return eitherUnbounded_;
}

inline bool SQLValues::CompColumn::isLowerUnbounded() const {
	return lowerUnbounded_;
}

inline bool SQLValues::CompColumn::isEitherExclusive() const {
	return eitherExclusive_;
}

inline bool SQLValues::CompColumn::isLowerExclusive() const {
	return lowerExclusive_;
}


inline bool SQLValues::ArrayTuple::isEmpty() const {
	return valueList_.empty();
}

inline void SQLValues::ArrayTuple::swap(ArrayTuple &another) {
	valueList_.swap(another.valueList_);
}

inline bool SQLValues::ArrayTuple::isNull(size_t index) const {
	return (getEntry(index).type_ == TupleTypes::TYPE_NULL);
}

template<typename T>
inline typename T::ValueType SQLValues::ArrayTuple::getAs(size_t index) const {
	const ValueEntry &entry = getEntry(index);
	assert(entry.type_ == T::COLUMN_TYPE ||
			(util::IsSame<T, typename Types::Any>::VALUE &&
			entry.type_ == TupleTypes::TYPE_NULL));

	typedef typename TypeUtils::template Traits<T::COLUMN_TYPE> TraitsType;
	typedef typename TraitsType::NormalStorageTag TypeTag;

	return getAs<T>(entry, TypeTag());
}

template<typename T>
inline void SQLValues::ArrayTuple::setBy(
		size_t index, ValueContext &cxt, const typename T::LocalValueType &value) {
	ValueEntry &entry = getEntry(index);
	const TupleColumnType type = (util::IsSame<T, typename Types::Any>::VALUE ?
			static_cast<TupleColumnType>(TupleTypes::TYPE_NULL) :
			static_cast<TupleColumnType>(T::COLUMN_TYPE));

	assert(entry.type_ == TupleTypes::TYPE_NULL ||
			type == TupleTypes::TYPE_NULL || entry.type_ == type);

	typedef typename TypeUtils::template Traits<T::COLUMN_TYPE> TraitsType;
	typedef typename TraitsType::ComparableValueTag ComparableTag;
	typedef typename TraitsType::FineNormalStorageTag TypeTag;

	setBy(entry, cxt, value, ComparableTag(), TypeTag());
	entry.type_ = type;
}

template<typename T>
inline typename T::ValueType SQLValues::ArrayTuple::getAs(
		const ValueEntry &entry, const Types::NormalFixed&) const {
	const ValueBody &valueBody = entry.value_;

	UTIL_STATIC_ASSERT((
			sizeof(typename T::LocalValueType) <= sizeof(valueBody)));
	const void *addr = &valueBody;

	typedef typename TypeUtils::template Traits<
			T::COLUMN_TYPE>::ComparableValueType Comparable;
	return ValueUtils::getValueByComparable<T>(
			*static_cast<const Comparable*>(addr));
}

template<typename T>
inline typename T::ValueType SQLValues::ArrayTuple::getAs(
		const ValueEntry &entry, const Types::VarOrLarge&) const {
	return ValueUtils::getValueByAddress<T>(entry.value_.var_);
}

template<typename>
inline TupleValue SQLValues::ArrayTuple::getAs(
		const ValueEntry&, const Types::Any&) const {
	return TupleValue();
}

template<typename C>
inline void SQLValues::ArrayTuple::setBy(
		ValueEntry &entry, ValueContext&,
		const typename C::LocalValueType &value, const C&,
		const Types::NormalFixed&) {
	typedef typename C::LocalValueType ValueType;

	ValueBody &valueBody = entry.value_;

	UTIL_STATIC_ASSERT((sizeof(ValueType) <= sizeof(valueBody)));
	void *addr = &valueBody;

	*static_cast<ValueType*>(addr) = static_cast<ValueType>(value);
}

inline void SQLValues::ArrayTuple::setBy(
		ValueEntry &entry, ValueContext&, const NanoTimestamp &value,
		const Types::NanoTimestampTag&, const Types::NanoTimestampTag&) {
	const uint32_t size = sizeof(value);

	uint8_t *dest;
	if (entry.data_.var_ == NULL || size > entry.data_.var_->size()) {
		dest = prepareVarData(entry, size);
	}
	else {
		dest = &((*entry.data_.var_)[0]);
	}

	memcpy(dest, &value, size);
	entry.value_.var_ = dest;
}

inline void SQLValues::ArrayTuple::setBy(
		ValueEntry &entry, ValueContext&, const TupleString &value,
		const Types::String&, const Types::String&) {
	const void *src = value.data();

	const uint32_t bodySize = TupleValueUtils::decodeVarSize(src);
	const uint32_t size =
			TupleValueUtils::getEncodedVarSize(bodySize) + bodySize;

	uint8_t *dest;
	if (entry.data_.var_ == NULL || size > entry.data_.var_->size()) {
		dest = prepareVarData(entry, size);
	}
	else {
		dest = &((*entry.data_.var_)[0]);
	}

	memcpy(dest, src, size);
	entry.value_.var_ = dest;
}

template<typename C>
inline void SQLValues::ArrayTuple::setBy(
		ValueEntry &entry, ValueContext &cxt, const TupleValue &value,
		const C&, const Types::Sequential&) {
	clear(entry);

	TupleValue dest = ValueUtils::duplicateValue(cxt, value);
	ValueUtils::moveValueToRoot(cxt, dest, cxt.getVarContextDepth(), 0);

	entry.value_.var_ = dest.getRawData();
	entry.data_.varCxt_ = &cxt.getVarContext();
}

inline void SQLValues::ArrayTuple::setBy(
		ValueEntry &entry, ValueContext&, const TupleValue&,
		const Types::Any&, const Types::Any&) {
	clear(entry);
}

inline SQLValues::ArrayTuple::ValueEntry& SQLValues::ArrayTuple::getEntry(
		size_t index) {
	assert(index < valueList_.size());
	return valueList_[index];
}

inline const SQLValues::ArrayTuple::ValueEntry& SQLValues::ArrayTuple::getEntry(
		size_t index) const {
	assert(index < valueList_.size());
	return valueList_[index];
}

template<typename T>
inline TupleValue SQLValues::ArrayTuple::EntryGetter::TypeAt<T>::operator()(
		size_t index) const {
	return ValueUtils::toAnyByValue<T::COLUMN_TYPE>(
			base_.getAs<T>(index));
}

template<typename T>
inline void SQLValues::ArrayTuple::EntrySetter::TypeAt<T>::operator()(
		size_t index, ValueContext &cxt, const TupleValue &value) const {
	base_.setBy<T>(
			index, cxt, ValueUtils::getValue<typename T::ValueType>(value));
}


inline void SQLValues::ArrayTuple::Assigner::operator()(
		ValueContext &cxt, const ReadableTuple &src,
		ArrayTuple &dest) const {
	if (inColumnList_ == NULL) {
		dest.assign(cxt, src, columnList_);
	}
	else {
		dest.assign(cxt, src, columnList_, *inColumnList_);
	}
}

template<typename DigestOnly>
inline void SQLValues::ArrayTuple::Assigner::operator()(
		ValueContext &cxt, const DigestTupleListReader &src,
		DigestArrayTuple &dest, const DigestOnly&) const {
	dest.getDigest() = src.getDigest();

	if (!DigestOnly::VALUE) {
		(*this)(cxt, src.getReader()->get(), dest.getTuple());
	}
}


inline void SQLValues::ArrayTuple::EmptyAssigner::operator()(
		ValueContext &cxt, ArrayTuple &dest) const {
	if (emptyTuple_.get() == NULL) {
		dest.assign(cxt, typeList_);
	}
	else {
		dest.valueList_ = emptyTuple_->valueList_;
	}
}


inline SQLValues::SummaryTuple::SummaryTuple() :
		head_(digestToHead(0)),
		body_(NULL) {
}

inline SQLValues::SummaryTuple SQLValues::SummaryTuple::create(
		SummaryTupleSet &tupleSet) {
	SummaryTuple tuple(digestToHead(0), tupleSet.createBody());

	if (tupleSet.isDetailInitializationRequired()) {
		tuple.initializeBodyDetail(tupleSet, NULL);
	}

	return tuple;
}

inline SQLValues::SummaryTuple SQLValues::SummaryTuple::create(
		SummaryTupleSet &tupleSet, TupleListReader &reader,
		int64_t digest) {
	SummaryTuple tuple(digestToHead(digest), tupleSet.createBody());

	if (tuple.body_ != NULL) {
		if (tupleSet.isDetailInitializationRequired()) {
			tuple.initializeBodyDetail(tupleSet, &reader);
		}

		typedef SummaryTupleSet::FieldReaderList ReaderList;
		const ReaderList &readerList = tupleSet.getFieldReaderList();
		for (ReaderList::const_iterator it = readerList.begin();
				it != readerList.end(); ++it) {
			it->second(FieldReader(tuple, tupleSet, it->first, reader));
		}
	}

	return tuple;
}

inline SQLValues::SummaryTuple SQLValues::SummaryTuple::create(
		SummaryTupleSet &tupleSet, TupleListReader &reader,
		int64_t digest, uint32_t subIndex) {
	SummaryTuple tuple(digestToHead(digest), tupleSet.createBody());

	if (tuple.body_ != NULL) {
		if (tupleSet.isDetailInitializationRequired()) {
			tuple.initializeBodyDetail(tupleSet, &reader);
		}

		typedef SummaryTupleSet::FieldReaderList ReaderList;
		const ReaderList &readerList = tupleSet.getSubFieldReaderList(subIndex);
		for (ReaderList::const_iterator it = readerList.begin();
				it != readerList.end(); ++it) {
			it->second(FieldReader(tuple, tupleSet, it->first, reader));
		}
	}

	return tuple;
}

template<typename T, typename D>
inline SQLValues::SummaryTuple SQLValues::SummaryTuple::createBy(
		SummaryTupleSet &tupleSet, TupleListReader &reader,
		const D &digester) {
	SummaryTuple tuple(
			readHead(
					tupleSet, reader, digester,
					typename T::Type(), typename T::NullableType()),
			tupleSet.createBody());

	if (tuple.body_ != NULL) {
		if (tupleSet.isDetailInitializationRequired()) {
			tuple.initializeBodyDetail(tupleSet, &reader);
		}

		typedef SummaryTupleSet::FieldReaderList ReaderList;
		const ReaderList &readerList = tupleSet.getFieldReaderList();
		for (ReaderList::const_iterator it = readerList.begin();
				it != readerList.end(); ++it) {
			it->second(FieldReader(tuple, tupleSet, it->first, reader));
		}
	}

	return tuple;
}

inline void SQLValues::SummaryTuple::initializeValues(SummaryTupleSet &tupleSet) {
	tupleSet.initializeBody(body_);
}

inline void SQLValues::SummaryTuple::release(SummaryTupleSet &tupleSet) {
	tupleSet.releaseBody(body_);
}

inline void SQLValues::SummaryTuple::fillEmpty(SummaryTuple *tupleArrayElem) {
	UTIL_STATIC_ASSERT(sizeof(SummaryTuple[1]) == sizeof(uint64_t) * 2);
	void *addr = tupleArrayElem;
	static_cast<uint64_t*>(addr)[0] = 0;
	static_cast<uint64_t*>(addr)[1] = 0;
}

inline bool SQLValues::SummaryTuple::checkEmpty(const SummaryTuple &tuple) {
	return (tuple.body_ == NULL);
}

inline int64_t SQLValues::SummaryTuple::getDigest() const {
	return head_.digest_;
}

inline SQLValues::SummaryTuple::Wrapper
SQLValues::SummaryTuple::getTuple() const {
	return Wrapper(*this);
}

inline bool SQLValues::SummaryTuple::isNull(
		const SummaryColumn &column) const {
	const int32_t offset = column.getNullsOffset();
	return (offset >= 0 &&
			((*static_cast<const NullsUnitType*>(getField(offset)) &
			column.getNullsMask()) != 0));
}

inline void SQLValues::SummaryTuple::setNull(const SummaryColumn &column) {
	setNullDetail(column, true);
}

inline TupleValue SQLValues::SummaryTuple::getValue(
		SummaryTupleSet &tupleSet, const SummaryColumn &column) const {
	return tupleSet.getFieldGetterFunc(column.getSummaryPosition())(
			FieldGetter(*this, column));
}

template<typename T>
inline typename T::ValueType SQLValues::SummaryTuple::getValueAs(
		const SummaryColumn &column) const {
	const bool onHead =
			(static_cast<TupleColumnType>(T::COLUMN_TYPE) !=
					TupleTypes::TYPE_BLOB &&
			static_cast<TupleColumnType>(T::COLUMN_TYPE) !=
					TupleTypes::TYPE_NANO_TIMESTAMP &&
			!column.isOnBody());
	return (onHead ?
			(column.isRotating() ?
					getHeadValueAs<T, util::TrueType>(column) :
					getHeadValueAs<T, util::FalseType>(column)) :
			getBodyValueAs<T>(column));
}

template<typename T, typename Rot>
inline typename T::ValueType SQLValues::SummaryTuple::getHeadValueAs(
		const SummaryColumn &column) const {
	assert(!isNull(column));
	assert(!column.isOnBody());
	assert(TypeUtils::toNonNullable(column.getTupleColumn().getType()) ==
			T::COLUMN_TYPE ||
			(column.isEmpty() && (static_cast<TupleColumnType>(T::COLUMN_TYPE) ==
					TupleTypes::TYPE_LONG)));

	return getHeadValueDetailAs(column, T(), Rot());
}

template<typename T>
inline typename T::ValueType SQLValues::SummaryTuple::getBodyValueAs(
		const SummaryColumn &column) const {
	assert(!isNull(column));
	assert(TypeUtils::toNonNullable(column.getTupleColumn().getType()) ==
			T::COLUMN_TYPE);

	return getBodyValueDetailAs(column, T());
}

inline void SQLValues::SummaryTuple::setValue(
		SummaryTupleSet &tupleSet,
		const SummaryColumn &column, const TupleValue &value) {
	tupleSet.getFieldSetterFunc(column.getSummaryPosition())(
			FieldSetter<false>(*this, tupleSet, column, value));
}

inline void SQLValues::SummaryTuple::setValuePromoted(
		SummaryTupleSet &tupleSet,
		const SummaryColumn &column, const TupleValue &value) {
	tupleSet.getFieldPromotedSetterFunc(column.getSummaryPosition())(
			FieldSetter<true>(*this, tupleSet, column, value));
}

template<typename T>
inline void SQLValues::SummaryTuple::setValueBy(
		SummaryTupleSet &tupleSet,
		const SummaryColumn &column, const typename T::LocalValueType &value) {
	assert(tupleSet.getFieldSetterFunc(column.getSummaryPosition()) != NULL);

	setValueDetailBy(
			tupleSet, column, value, T(), util::FalseType(), util::FalseType());
	setNullDetail(column, false);
}

template<typename T>
inline void SQLValues::SummaryTuple::incrementValueBy(
		const SummaryColumn &column) {
	assert(!isNull(column));
	UTIL_STATIC_ASSERT(
			TypeUtils::Traits<T::COLUMN_TYPE>::FOR_INTEGRAL_EXPLICIT);

	typedef typename TypeUtils::Traits<T::COLUMN_TYPE>::StorableValueType Storable;
	++(*static_cast<Storable*>(getField(column.getOffset())));
}

template<typename T>
inline void SQLValues::SummaryTuple::appendValueBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		typename TypeTraits<T>::ReadableRefType value) {
	assert(tupleSet.getFieldSetterFunc(column.getSummaryPosition()) != NULL);
	appendValueDetailBy(tupleSet, column, value, T());
}

template<typename T, typename F>
inline bool SQLValues::SummaryTuple::checkDecrementalCounterAs(
		const SummaryColumn &column) {
	UTIL_STATIC_ASSERT(Decremental::As<T>::TYPE_SUPPORTED);
	typedef typename Decremental::As<T>::Type Storable;
	Storable &field = *static_cast<Storable*>(getField(column.getOffset()));

	const uint64_t count = field.getCounter().get();
	const uint64_t threshold = (F::VALUE ? 0 : 1);
	return (count <= threshold);
}

template<typename T, typename F>
inline void SQLValues::SummaryTuple::setDecrementalValueBy(
		const SummaryColumn &column, const typename T::ValueType &value) {
	UTIL_STATIC_ASSERT(Decremental::As<T>::TYPE_SUPPORTED);
	typedef typename Decremental::As<T>::Type Storable;
	Storable &field = *static_cast<Storable*>(getField(column.getOffset()));

	const uint64_t count = field.getCounter().get();
	const uint64_t threshold = 1;
	const bool byNull = (!(F::VALUE) && count <= threshold);

	field = Storable();
	if (!byNull) {
		field.add(value, util::TrueType());
	}
	setNullDetail(column, byNull);
}

template<typename T, typename F>
inline void SQLValues::SummaryTuple::incrementDecrementalValueBy(
		const SummaryColumn &column) {
	UTIL_STATIC_ASSERT(Decremental::As<T>::TYPE_INCREMENTAL);
	assert(!isNull(column));
	typedef typename Decremental::As<T>::Type Storable;
	static_cast<Storable*>(getField(column.getOffset()))->increment(F());
}

template<typename T, typename F>
inline void SQLValues::SummaryTuple::addDecrementalValueBy(
		const SummaryColumn &column, const typename T::ValueType &value) {
	UTIL_STATIC_ASSERT(Decremental::As<T>::TYPE_SUPPORTED);
	assert(!isNull(column));
	typedef typename Decremental::As<T>::Type Storable;
	static_cast<Storable*>(getField(column.getOffset()))->add(value, F());
}

inline SQLValues::SummaryTuple::SummaryTuple(const Head &head, void *body) :
		head_(head),
		body_(body) {
}

template<typename T>
inline void SQLValues::SummaryTuple::readValue(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		TupleListReader &reader) {
	typedef typename T::VariantTraitsType::HeadNullableType HeadNullableType;
	if (T::NullableType::VALUE || HeadNullableType::VALUE) {
		if (ValueUtils::readCurrentNull(reader, column.getTupleColumn())) {
			assert(isNull(column));
			return;
		}
		setNullDetail(column, false);
		if (HeadNullableType::VALUE) {
			return;
		}
	}

	if (TypeUtils::template Traits<T::COLUMN_TYPE>::FOR_MULTI_VAR) {
		if (!T::NullableType::VALUE) {
			setNullDetail(column, false);
		}
		return;
	}

	typedef typename T::Type1 DestTypeTag;
	typedef typename T::Type2 SrcTypeTag;
	typedef typename T::VariantTraitsType::ShallowType ShallowType;
	setValueDetailBy(
			tupleSet, column,
			ValueUtils::getPromoted<DestTypeTag, SrcTypeTag>(
					ValueUtils::readCurrentValue<SrcTypeTag>(
							reader, column.getTupleColumn())),
			DestTypeTag(), ShallowType(), util::TrueType());
}

template<typename T, typename Rot>
inline typename T::ValueType SQLValues::SummaryTuple::getHeadValueDetailAs(
		const SummaryColumn &column, const T&, const Rot&) const {
	static_cast<void>(column);
	const bool available =
			!TypeUtils::Traits<T::COLUMN_TYPE>::SIZE_VAR_OR_LARGE_EXPLICIT;
	typedef typename util::Conditional<
			available, T, SQLValues::Types::Any>::Type FixedTypeTag;
	typedef util::TrueType Ascending;
	assert(available);
	return ValueUtils::toValueByOrderdDigest<FixedTypeTag, Ascending>(
			ValueUtils::digestByRotation(head_.digest_, Rot()));
}

template<typename Rot>
inline TupleNanoTimestamp SQLValues::SummaryTuple::getHeadValueDetailAs(
		const SummaryColumn &column, const Types::NanoTimestampTag&,
		const Rot&) const {
	assert(false);
	return getBodyValueDetailAs(column, Types::NanoTimestampTag());
}

template<typename Rot>
inline TupleString SQLValues::SummaryTuple::getHeadValueDetailAs(
		const SummaryColumn &column, const Types::String&, const Rot&) const {
	static_cast<void>(column);
	assert(head_.varData_ != NULL);
	return TupleString(head_.varData_);
}

template<typename T>
inline typename T::ValueType SQLValues::SummaryTuple::getBodyValueDetailAs(
		const SummaryColumn &column, const T&) const {
	typedef typename TypeUtils::Traits<T::COLUMN_TYPE>::StorableValueType Storable;
	return ValueUtils::getValueByComparable<T>(
			*static_cast<const Storable*>(getField(column.getOffset())));
}

inline TupleNanoTimestamp SQLValues::SummaryTuple::getBodyValueDetailAs(
		const SummaryColumn &column, const Types::NanoTimestampTag&) const {
	return ValueUtils::getValueByAddress<Types::NanoTimestampTag>(
			getField(column.getOffset()));
}

inline TupleString SQLValues::SummaryTuple::getBodyValueDetailAs(
		const SummaryColumn &column, const Types::String&) const {
	return TupleString(
			*static_cast<void *const*>(getField(column.getOffset())));
}

inline TupleValue SQLValues::SummaryTuple::getBodyValueDetailAs(
		const SummaryColumn &column, const Types::Blob&) const {
	const int32_t offset = column.getOffset();
	if (offset < 0) {
		return ValueUtils::readValue<Types::Blob>(
				*static_cast<const ReadableTuple*>(getField(
						SummaryTupleSet::getDefaultReadableTupleOffset())),
				column.getTupleColumn());
	}
	else {
		const TupleValue &value = SummaryTupleSet::getLobValue(
				*static_cast<void**>(getRawField(column.getOffset())));
		assert(value.getType() == Types::Blob::COLUMN_TYPE);
		return value;
	}
}

inline TupleValue SQLValues::SummaryTuple::getBodyValueDetailAs(
		const SummaryColumn &column, const Types::Any&) const {
	static_cast<void>(column);
	return TupleValue();
}

template<typename T, typename Shallow, typename Initializing>
inline void SQLValues::SummaryTuple::setValueDetailBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		const typename T::LocalValueType &value, const T&, const Shallow&,
		const Initializing&) {
	static_cast<void>(tupleSet);
	typedef typename TypeUtils::Traits<T::COLUMN_TYPE>::StorableValueType Storable;
	*static_cast<Storable*>(getField(column.getOffset())) =
			static_cast<Storable>(value);
}

template<typename Shallow, typename Initializing>
inline void SQLValues::SummaryTuple::setValueDetailBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		const TupleString &value, const Types::String&, const Shallow&,
		const Initializing&) {
	const void *src = value.data();
	void *field = getField(column.getOffset());

	if (Shallow::VALUE) {
		*static_cast<const void**>(field) = src;
	}
	else {
		void *dup = tupleSet.duplicateVarData(src);
		void *&dest = *static_cast<void**>(field);

		if (!Initializing::VALUE) {
			tupleSet.deallocateVarData(dest);
		}
		dest = dup;
	}
}

template<typename Shallow, typename Initializing>
inline void SQLValues::SummaryTuple::setValueDetailBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		const TupleValue &value, const Types::Blob&, const Shallow&,
		const Initializing&) {
	assert(!Shallow::VALUE);
	UTIL_STATIC_ASSERT((Shallow::VALUE || !Initializing::VALUE));

	void *src = tupleSet.duplicateLob(value);
	void *&dest = *static_cast<void**>(getField(column.getOffset()));

	tupleSet.deallocateLob(dest);
	dest = src;
}

template<typename Shallow, typename Initializing>
inline void SQLValues::SummaryTuple::setValueDetailBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		const TupleValue &value, const Types::Any&, const Shallow&,
		const Initializing&) {
	static_cast<void>(tupleSet);
	static_cast<void>(column);
	static_cast<void>(value);
}

inline void SQLValues::SummaryTuple::setNullDetail(
		const SummaryColumn &column, bool nullValue) {
	const int32_t offset = column.getNullsOffset();
	if (offset >= 0) {
		NullsUnitType *unit = static_cast<NullsUnitType*>(getField(offset));
		const NullsUnitType mask = column.getNullsMask();
		if (nullValue) {
			*unit |= mask;
		}
		else {
			*unit &= ~mask;
		}
	}
}

inline void SQLValues::SummaryTuple::appendValueDetailBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		StringReader &value, const Types::String&) {
	void *&field = *static_cast<void**>(getField(column.getOffset()));
	tupleSet.appendVarData(field, value);
}

inline void SQLValues::SummaryTuple::appendValueDetailBy(
		SummaryTupleSet &tupleSet, const SummaryColumn &column,
		LobReader &value, const Types::Blob&) {
	void *field = *static_cast<void**>(getField(column.getOffset()));
	tupleSet.appendLob(field, value);
}

template<typename T, typename Nullable, typename D>
inline SQLValues::SummaryTuple::Head SQLValues::SummaryTuple::readHead(
		SummaryTupleSet &tupleSet, TupleListReader &reader, const D &digester,
		const T&, const Nullable&) {
	UTIL_STATIC_ASSERT(
			static_cast<TupleColumnType>(T::COLUMN_TYPE) !=
			TupleTypes::TYPE_STRING);
	static_cast<void>(tupleSet);
	return digestToHead(digester(SQLValues::TupleListReaderSource(reader)));
}

template<typename Nullable, typename D>
inline SQLValues::SummaryTuple::Head SQLValues::SummaryTuple::readHead(
		SummaryTupleSet &tupleSet, TupleListReader &reader, const D &digester,
		const Types::String&, const Nullable&) {
	const SummaryColumn &column = tupleSet.getReaderHeadColumn();
	if (column.isEmpty()) {
		return digestToHead(digester(SQLValues::TupleListReaderSource(reader)));
	}

	Head head;
	if (Nullable::VALUE && ValueUtils::readCurrentNull(
			reader, column.getTupleColumn())) {
		head.varData_ = NULL;
	}
	else {
		head.varData_ = ValueUtils::readCurrentValue<Types::String>(
				reader, column.getTupleColumn()).data();
	}
	return head;
}


inline SQLValues::SummaryTuple::Head SQLValues::SummaryTuple::digestToHead(
		int64_t digest) {
	Head head;
	head.digest_ = digest;
	return head;
}

inline const void* SQLValues::SummaryTuple::getField(int32_t offset) const {
	return getRawField(offset);
}

inline void* SQLValues::SummaryTuple::getField(int32_t offset) {
	return getRawField(offset);
}

inline void* SQLValues::SummaryTuple::getRawField(int32_t offset) const {
	assert(offset >= 0 && body_ != NULL);
	return static_cast<uint8_t*>(body_) + offset;
}


template<typename T>
inline TupleValue
SQLValues::SummaryTuple::FieldGetter::TypeAt<T>::operator()() const {
	if (base_.tuple_.isNull(base_.column_)) {
		return TupleValue();
	}
	return ValueUtils::toAnyByValue<T::COLUMN_TYPE>(
			base_.tuple_.template getValueAs<T>(base_.column_));
}


template<bool Promo>
template<typename T>
inline void SQLValues::SummaryTuple::FieldSetter<Promo>::set(
		const util::TrueType&) const {
	if (ValueUtils::isNull(value_)) {
		tuple_.setNull(column_);
		return;
	}
	tuple_.template setValueBy<T>(
			tupleSet_, column_,
			ValueUtils::promoteValue<T::COLUMN_TYPE>(value_));
}

template<bool Promo>
template<typename T>
inline void SQLValues::SummaryTuple::FieldSetter<Promo>::set(
		const util::FalseType&) const {
	if (ValueUtils::isNull(value_)) {
		tuple_.setNull(column_);
		return;
	}
	tuple_.template setValueBy<T>(
			tupleSet_, column_,
			ValueUtils::getValue<typename T::ValueType>(value_));
}

template<bool Promo>
template<typename T>
inline void SQLValues::SummaryTuple::FieldSetter<
		Promo>::TypeAt<T>::operator()() const {
	typedef typename util::BoolType<Promo>::Result PromoType;
	base_.set<T>(PromoType());
}


inline const SQLValues::SummaryTupleSet::FieldReaderList&
SQLValues::SummaryTupleSet::getFieldReaderList() const {
	return fieldReaderList_;
}

inline const SQLValues::SummaryTupleSet::FieldReaderList&
SQLValues::SummaryTupleSet::getSubFieldReaderList(uint32_t index) const {
	return subFieldReaderList_[index];
}

inline SQLValues::SummaryTupleSet::FieldGetterFunc
SQLValues::SummaryTupleSet::getFieldGetterFunc(uint32_t pos) const {
	return getterFuncList_[pos];
}

inline SQLValues::SummaryTupleSet::FieldSetterFunc
SQLValues::SummaryTupleSet::getFieldSetterFunc(uint32_t pos) const {
	return setterFuncList_[pos];
}

inline SQLValues::SummaryTupleSet::FieldPromotedSetterFunc
SQLValues::SummaryTupleSet::getFieldPromotedSetterFunc(uint32_t pos) const {
	return promotedSetterFuncList_[pos];
}

inline const SQLValues::SummaryColumn&
SQLValues::SummaryTupleSet::getReaderHeadColumn() const {
	return readerHeadColumn_;
}

inline bool SQLValues::SummaryTupleSet::isDetailInitializationRequired() const {
	return readableTupleRequired_;
}

inline void* SQLValues::SummaryTupleSet::createBody() {
	if (bodySize_ == 0) {
		return reinterpret_cast<void*>(static_cast<uintptr_t>(-1));
	}
	if (restBodySize_ < bodySize_) {
		reserveBody();
	}

	void *body = restBodyData_;
	initializeBody(body);

	restBodyData_ = static_cast<uint8_t*>(restBodyData_) + bodySize_;
	restBodySize_ -= bodySize_;

	return body;
}

inline void SQLValues::SummaryTupleSet::initializeBody(void *body) {
	if (initialBodyImage_ != NULL) {
		memcpy(
				static_cast<uint8_t*>(body) + initialBodyOffset_,
				initialBodyImage_, bodySize_ - initialBodyOffset_);
	}
}

inline void SQLValues::SummaryTupleSet::releaseBody(void *body) {
	static_cast<void>(body);
}


template<typename D>
inline SQLValues::DigestTupleListReader::DigestTupleListReader(
		TupleListReader &reader, const D &digester) :
		digest_(digester(TupleListReaderSource(reader))),
		readerSource_(TupleListReaderSource(reader)) {
}

inline SQLValues::DigestTupleListReader::DigestTupleListReader(
		int64_t digest, TupleListReader &reader) :
		digest_(digest),
		readerSource_(TupleListReaderSource(reader)) {
}


template<typename D>
inline SQLValues::DigestReadableTuple::DigestReadableTuple(
		TupleListReader &reader, const D &digester) :
		digest_(digester(TupleListReaderSource(reader))),
		tuple_(reader.get()) {
}

inline SQLValues::DigestReadableTuple::DigestReadableTuple(
		int64_t digest, const ReadableTuple &tuple) :
		digest_(digest),
		tuple_(tuple) {
}


inline void SQLValues::DigestArrayTuple::moveTo(DigestArrayTuple &dest) {
	dest.digest_ = digest_;
	dest.tuple_.swap(tuple_);
}

inline void SQLValues::DigestArrayTuple::swap(DigestArrayTuple &another) {
	std::swap(digest_, another.digest_);
	another.tuple_.swap(tuple_);
}


inline SQLValues::ValueAccessor::ValueAccessor(TupleColumnType type) :
		type_(type),
		columnList_(NULL) {
}

inline SQLValues::ValueAccessor::ValueAccessor(
		CompColumnList::const_iterator columnIt,
		const CompColumnList &columnList) :
		type_(TupleTypes::TYPE_NULL),
		columnIt_(columnIt),
		columnList_(&columnList) {
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::ByValue::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return ValueUtils::getValue<typename T::ValueType>(src);
}

template<typename P>
inline bool SQLValues::ValueAccessor::ByValue::NullChecker::operator()(
		const SourceType &src, const P&) {
	return ValueUtils::isNull(src);
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::ByReader::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return ValueUtils::readCurrentValue<T>(
			*src.reader_, get().columnIt_->getTupleColumn(P()));
}

template<typename P>
inline bool SQLValues::ValueAccessor::ByReader::NullChecker::operator()(
		const SourceType &src, const P&) {
	return ValueUtils::readCurrentNull(
			*src.reader_, get().columnIt_->getTupleColumn(P()));
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::ByReadableTuple::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return ValueUtils::readValue<T>(
			src, get().columnIt_->getTupleColumn(P()));
}

template<typename P>
inline bool SQLValues::ValueAccessor::ByReadableTuple::NullChecker::operator()(
		const SourceType &src, const P&) {
	return ValueUtils::readNull(src, get().columnIt_->getTupleColumn(P()));
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::ByArrayTuple::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return src.getAs<T>(get().columnIt_->getColumnPos(P()));
}

template<typename P>
inline bool SQLValues::ValueAccessor::ByArrayTuple::NullChecker::operator()(
		const SourceType &src, const P&) {
	return src.isNull(get().columnIt_->getColumnPos(P()));
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::ByKeyArrayTuple::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return src.getAs<T>(get().columnIt_ - get().columnList_->begin());
}

template<typename P>
inline bool SQLValues::ValueAccessor::ByKeyArrayTuple::NullChecker::operator()(
		const SourceType &src, const P&) {
	return src.isNull(get().columnIt_ - get().columnList_->begin());
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::BySummaryTuple::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return src.getValueAs<T>(get().columnIt_->getSummaryColumn(P()));
}

template<typename P>
inline bool SQLValues::ValueAccessor::BySummaryTuple::NullChecker::operator()(
		const SourceType &src, const P&) {
	return src.isNull(get().columnIt_->getSummaryColumn(P()));
}


template<typename Rot>
template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::BySummaryTupleHead<Rot>::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return src.getHeadValueAs<T, Rot>(get().columnIt_->getSummaryColumn(P()));
}


template<typename T>
template<typename P>
inline typename T::ValueType
SQLValues::ValueAccessor::BySummaryTupleBody::TypeAt<T>::operator()(
		const SourceType &src, const P&) {
	return src.getBodyValueAs<T>(get().columnIt_->getSummaryColumn(P()));
}


template<typename S, typename P>
inline bool SQLValues::ValueAccessor::NoopNullChecker::operator()(
		const S&, const P&) {
	UTIL_STATIC_ASSERT((!util::IsSame<S, TupleValue>::VALUE));
	return false;
}

template<typename P>
inline bool SQLValues::ValueAccessor::NoopNullChecker::operator()(
		const TupleValue &src, const P&) {
	assert(!ValueUtils::isNull(src));
	static_cast<void>(src);
	return false;
}


template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Byte>::operator()(
		const int8_t &v1, const int8_t &v2, const Pred &pred) const {
	return pred(v1, v2);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Short>::operator()(
		const int16_t &v1, const int16_t &v2, const Pred &pred) const {
	return pred(v1, v2);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Integer>::operator()(
		const int32_t &v1, const int32_t &v2, const Pred &pred) const {
	return pred(v1, v2);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Long>::operator()(
		const int64_t &v1, const int64_t &v2, const Pred &pred) const {
	return pred(v1, v2);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Float>::operator()(
		const float &v1, const float &v2, const Pred &pred) const {
	return pred(ValueUtils::compareFloating(v1, v2, base_.sensitive_), 0);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Double>::operator()(
		const double &v1, const double &v2, const Pred &pred) const {
	return pred(ValueUtils::compareFloating(v1, v2, base_.sensitive_), 0);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::TimestampTag>::operator()(
		const int64_t &v1, const int64_t &v2, const Pred &pred) const {
	return pred(v1, v2);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::MicroTimestampTag>::operator()(
		const MicroTimestamp &v1, const MicroTimestamp &v2,
		const Pred &pred) const {
	return pred(v1.value_, v2.value_);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::NanoTimestampTag>::operator()(
		const NanoTimestamp &v1, const NanoTimestamp &v2,
		const Pred &pred) const {
	return pred(ValueUtils::comparePreciseTimestamp(v1, v2), 0);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Bool>::operator()(
		const bool &v1, const bool &v2, const Pred &pred) const {
	return pred(v1, v2);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::String>::operator()(
		const TupleString &v1, const TupleString &v2, const Pred &pred) const {
	return ValueUtils::compareString<Pred, bool>(v1, v2, pred);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Blob>::operator()(
		const TupleValue &v1, const TupleValue &v2, const Pred &pred) const {
	return pred(ValueUtils::compareSequence(
			*ValueUtils::toLobReader(v1), *ValueUtils::toLobReader(v2)), 0);
}

template<>
template<typename Pred>
inline bool SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Any>::operator()(
		const TupleValue &v1, const TupleValue &v2, const Pred &pred) const {
	assert(ValueUtils::isNull(v1));
	assert(ValueUtils::isNull(v2));
	static_cast<void>(v1);
	static_cast<void>(v2);
	return pred(0, 0);
}


inline SQLValues::ValueComparator::ValueComparator(
		const ValueAccessor &accessor1, const ValueAccessor &accessor2,
		VarContext *varCxt, bool sensitive, bool ascending) :
		accessor1_(accessor1),
		accessor2_(accessor2),
		varCxt_(varCxt),
		sensitive_(sensitive),
		ascending_(ascending) {
}

inline SQLValues::ValueComparator::DefaultResultType
SQLValues::ValueComparator::compareValues(
		const TupleValue &v1, const TupleValue &v2, bool sensitive,
		bool ascending) {
	return ofValues(v1, v2, sensitive, ascending)(v1, v2);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::compare(
		int64_t v1, int64_t v2, bool,
		const Pred &pred, const Types::Integral&) {
	return ValueUtils::compareIntegral(v1, v2, pred);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::compare(
		double v1, double v2, bool sensitive,
		const Pred &pred, const Types::Floating&) {
	return ValueUtils::compareFloating(v1, v2, sensitive, pred);
}

template<typename Pred, typename T>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::compare(
		const T &v1, const T &v2, bool, const Pred &pred,
		const Types::PreciseTimestamp&) {
	return ValueUtils::comparePreciseTimestamp(v1, v2, pred);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::compare(
		const TupleString &v1, const TupleString &v2, bool,
		const Pred &pred, const Types::String&) {
	return ValueUtils::compareString(v1, v2, pred);
}

template<typename Pred, typename T>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::compare(
		const T &v1, const T &v2, bool, const Pred &pred,
		const Types::Sequential&) {
	typedef typename TypeUtils::template ValueTraits<T> ValueTraits;
	const TupleColumnType type = ValueTraits::COLUMN_TYPE_AS_SEQUENCE;

	return ValueUtils::compareSequence(
			*ValueUtils::toReader<type>(v1),
			*ValueUtils::toReader<type>(v2), pred);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::compare(
		const TupleValue&, const TupleValue&, bool,
		const Pred &pred, const Types::Any&) {
	return ValueUtils::compareIntegral(0, 0, pred);
}


template<typename T>
inline typename SQLValues::ValueComparator::TypeAt<T>::RetType
SQLValues::ValueComparator::TypeAt<T>::operator()(
		const typename AccessorType1::SourceType &src1,
		const typename AccessorType2::SourceType &src2) const {
	typedef typename VariantTraitsType::PredType PredType;

	typedef typename util::TrueType Pos1;
	typedef typename util::FalseType Pos2;

	if (T::NullableType::VALUE) {
		typedef typename AccessorType1::NullChecker Checker1;
		typedef typename AccessorType2::NullChecker Checker2;

		const bool nullValue1 = Checker1(base_.accessor1_)(src1, Pos1());
		const bool nullValue2 = Checker2(base_.accessor2_)(src2, Pos2());

		if (nullValue1 || nullValue2) {
			return PredType()(
					ValueUtils::compareNull(nullValue1, nullValue2), 0);
		}
	}

	typedef typename T::ComparableValueTag CompTypeTag;

	typedef typename TypeUtils::template Traits<
			CompTypeTag::COLUMN_TYPE> TypeTraits;
	typedef typename CompTypeTag::LocalValueType ValueType;

	typedef typename OpTypeOf<
			AccessorType1, typename T::Type1, CompTypeTag>::Type Op1;
	typedef typename OpTypeOf<
			AccessorType2, typename T::Type2, CompTypeTag>::Type Op2;

	VarScopeType varScope(base_.varCxt_);
	return compare(
			static_cast<ValueType>(Op1(base_.accessor1_)(src1, Pos1())),
			static_cast<ValueType>(Op2(base_.accessor2_)(src2, Pos2())),
			base_.sensitive_, PredType(),
			typename TypeTraits::FineComparableTag());
}


template<typename T1, typename T2>
template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueComparator::BasicTypeAt<T1, T2>::operator()(
		const typename T1::LocalValueType &v1,
		const typename T2::LocalValueType &v2,
		const Pred&) const {
	return compare(
			ValueUtils::getPromoted<CompTypeTag, T1>(v1),
			ValueUtils::getPromoted<CompTypeTag, T2>(v2),
			sensitive_, Pred(),
			typename TypeTraits::FineComparableTag());
}


inline int32_t SQLValues::ValueComparator::ThreeWay::operator()(
		first_argument_type v1, second_argument_type v2) const {
	return SQLValues::ValueUtils::compareRawValue(v1, v2);
}


template<
		typename Pred, bool Rev, bool Promo, bool Ordering,
		typename A1, typename A2>
template<typename Op, typename Traits>
typename Traits::template Func<Op>::Type
SQLValues::ValueComparator::Switcher<
		Pred, Rev, Promo, Ordering, A1, A2>::getWith() const {
	if (base_.ascending_) {
		return getSubWith<Op, Traits, true>();
	}
	else {
		return getSubWith<Op, Traits, !Ordering>();
	}
}

template<
		typename Pred, bool Rev, bool Promo, bool Ordering,
		typename A1, typename A2>
template<typename Op, typename Traits, bool Ascending>
typename Traits::template Func<Op>::Type
SQLValues::ValueComparator::Switcher<
		Pred, Rev, Promo, Ordering, A1, A2>::getSubWith() const {
	return base_.getColumnTypeSwitcher(
			Promo, nullIgnorable_, profile_).getWith<
					Op, typename OpTraitsAt<Traits, Ascending>::Type>();
}


template<typename A, typename T, typename Comp>
SQLValues::ValueComparator::PromotedOp<A, T, Comp>::PromotedOp(
		const ValueAccessor &accessor) :
		accessor_(accessor) {
}

template<typename A, typename T, typename Comp>
template<typename Source, typename Pos>
typename Comp::LocalValueType
SQLValues::ValueComparator::PromotedOp<A, T, Comp>::operator()(
		const Source &src, const Pos&) const {
	return ValueUtils::getPromoted<Comp, T>(BaseOpType(accessor_)(src, Pos()));
}


inline SQLValues::ValueFnv1aHasher::ValueFnv1aHasher(
		const ValueAccessor &accessor, VarContext *varCxt) :
		accessor_(accessor),
		varCxt_(varCxt),
		seed_(ValueUtils::fnv1aHashInit()) {
}

inline SQLValues::ValueFnv1aHasher::ValueFnv1aHasher(
		const ValueAccessor &accessor, VarContext *varCxt, int64_t seed) :
		accessor_(accessor),
		varCxt_(varCxt),
		seed_(static_cast<uint32_t>(seed)) {
	assert(seed >= 0);
}

inline int64_t SQLValues::ValueFnv1aHasher::maskNull(int64_t src) {
	assert(src >= 0);
	int64_t dest = -src;
	if (dest >= 0) {
		dest = NULL_MASK_VALUE;
	}
	return dest;
}

inline int64_t SQLValues::ValueFnv1aHasher::unmaskNull(int64_t src) {
	assert(src < 0);
	int64_t dest = src;
	if (dest == NULL_MASK_VALUE) {
		dest = 0;
	}
	return -dest;
}

inline int64_t SQLValues::ValueFnv1aHasher::digest(
		uint32_t seed, const int64_t &value, const Types::Integral&) {
	return ValueUtils::fnv1aHashIntegral(seed, value);
}

inline int64_t SQLValues::ValueFnv1aHasher::digest(
		uint32_t seed, const double &value, const Types::Floating&) {
	return ValueUtils::fnv1aHashFloating(seed, value);
}

template<typename T>
inline int64_t SQLValues::ValueFnv1aHasher::digest(
		uint32_t seed, const T &value, const Types::PreciseTimestamp&) {
	return ValueUtils::fnv1aHashPreciseTimestamp(seed, value);
}

template<typename T>
inline int64_t SQLValues::ValueFnv1aHasher::digest(
		uint32_t seed, const T &value, const Types::Sequential&) {
	typedef typename TypeUtils::template ValueTraits<T> ValueTraits;
	const TupleColumnType type = ValueTraits::COLUMN_TYPE_AS_SEQUENCE;
	return ValueUtils::fnv1aHashSequence(
			seed, *ValueUtils::toReader<type>(value));
}

inline int64_t SQLValues::ValueFnv1aHasher::digest(
		uint32_t seed, const TupleValue &value, const Types::Any&) {
	static_cast<void>(value);
	return -static_cast<int64_t>(seed);
}

template<typename T>
inline int64_t SQLValues::ValueFnv1aHasher::TypeAt<T>::operator()(
		const typename AccessorType::SourceType &src) const {
	typedef typename TypeUtils::template Traits<T::COLUMN_TYPE> TypeTraits;
	typedef typename util::TrueType Pos;
	typedef typename AccessorType::NullChecker NullChecker;

	if (T::NullableType::VALUE && NullChecker(base_.accessor_)(src, Pos())) {
		return maskNull(base_.seed_);
	}

	VarScopeType varScope(base_.varCxt_);
	typedef typename AccessorType::template TypeAt<
			typename T::Type> TypedAccessor;
	return digest(
			base_.seed_,
			TypedAccessor(base_.accessor_)(src, Pos()),
			typename TypeTraits::BasicComparableTag());
}


inline SQLValues::ValueOrderedDigester::ValueOrderedDigester(
		const ValueAccessor &accessor, VarContext *varCxt, bool rotating) :
		accessor_(accessor),
		varCxt_(varCxt),
		rotating_(rotating) {
}

inline int64_t SQLValues::ValueOrderedDigester::digest(
		const int64_t &value, const Types::Integral&) {
	return ValueUtils::digestOrderedIntegral(value, Ascending());
}

inline int64_t SQLValues::ValueOrderedDigester::digest(
		const double &value, const Types::Floating&) {
	return ValueUtils::digestOrderedFloating(value, Ascending());
}

template<typename T>
inline int64_t SQLValues::ValueOrderedDigester::digest(
		const T &value, const Types::PreciseTimestamp&) {
	return ValueUtils::digestOrderedPreciseTimestamp(value, Ascending());
}

template<typename T>
inline int64_t SQLValues::ValueOrderedDigester::digest(
		const T &value, const Types::Sequential&) {
	typedef typename TypeUtils::template ValueTraits<T> ValueTraits;
	const TupleColumnType type = ValueTraits::COLUMN_TYPE_AS_SEQUENCE;
	return ValueUtils::digestOrderedSequence(
			*ValueUtils::toReader<type>(value), Ascending());
}

inline int64_t SQLValues::ValueOrderedDigester::digest(
		const TupleValue &value, const Types::Any&) {
	static_cast<void>(value);
	return ValueUtils::digestOrderedNull(Ascending());
}

template<typename T>
inline int64_t SQLValues::ValueOrderedDigester::TypeAt<T>::operator()(
		const typename AccessorType::SourceType &src) const {
	typedef typename TypeUtils::template Traits<T::COLUMN_TYPE> TypeTraits;
	typedef typename VariantTraitsType::RotatingType RotatingType;
	typedef typename util::TrueType Pos;
	typedef typename AccessorType::NullChecker NullChecker;

	int64_t base;
	if (T::NullableType::VALUE && NullChecker(base_.accessor_)(src, Pos())) {
		base = digest(TupleValue(), Types::Any());
	}
	else {
		VarScopeType varScope(base_.varCxt_);
		typedef typename AccessorType::template TypeAt<
				typename T::Type> TypedAccessor;
		base = digest(
				TypedAccessor(base_.accessor_)(src, Pos()),
				typename TypeTraits::BasicComparableTag());
	}
	return ValueUtils::rotateDigest(base, RotatingType());
}


inline SQLValues::ValueDigester::ValueDigester(
		const ValueAccessor &accessor, VarContext *varCxt, bool ordering,
		bool rotating, int64_t seed) :
		accessor_(accessor),
		varCxt_(varCxt),
		ordering_(ordering),
		rotating_(rotating),
		seed_(seed) {
}

template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
template<typename Op, typename Traits>
typename Traits::template Func<Op>::Type
SQLValues::ValueDigester::Switcher<
		Accessor, NullableSpecific, RotationSpecific>::getWith() const {
	if (base_.ordering_) {
		const bool orderingSub = true;
		if (base_.rotating_) {
			const bool rotatingSub =
					util::BoolType<RotationSpecific>::Result::VALUE;
			assert(rotatingSub);
			return getSubWith<Op, Traits, orderingSub, rotatingSub>();
		}
		else {
			const bool rotatingSub = false;
			return getSubWith<Op, Traits, orderingSub, rotatingSub>();
		}
	}
	else {
		const bool orderingSub = false;
		const bool rotatingSub = false;
		return getSubWith<Op, Traits, orderingSub, rotatingSub>();
	}
}

template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
template<typename Op, typename Traits, bool Ordering, bool Rotating>
typename Traits::template Func<Op>::Type
SQLValues::ValueDigester::Switcher<
		Accessor, NullableSpecific, RotationSpecific>::getSubWith() const {
	assert(!nullableAlways_ || NullableSpecific == 2);
	return base_.getColumnTypeSwitcher(
			nullIgnorable_, nullableAlways_, profile_).getWith<
					Op, typename OpTraitsAt<Traits, Ordering, Rotating>::Type>();
}


template<typename Accessor>
SQLValues::TupleNullChecker::WithAccessor<Accessor>::WithAccessor(
		util::StackAllocator &alloc, const TupleNullChecker &base) :
		accessorList_(alloc),
		base_(base) {
	const CompColumnList &columnList = base_.columnList_;
	for (CompColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		if (!TypeUtils::isNullable(it->getType())) {
			continue;
		}
		accessorList_.push_back(ValueAccessor(it, columnList));
	}
}

template<typename Accessor>
inline bool SQLValues::TupleNullChecker::WithAccessor<Accessor>::operator()(
		const typename Accessor::SourceType &src) const {
	typedef typename Accessor::NullChecker Checker;
	typedef util::TrueType Pos;

	for (AccessorList::const_iterator it = accessorList_.begin();
			it != accessorList_.end(); ++it) {
		if ((Checker(*it))(src, Pos())) {
			return true;
		}
	}

	return false;
}


inline SQLValues::ValueComparator
SQLValues::TupleComparator::comparatorAt(
		CompColumnList::const_iterator it) const {
	return ValueComparator(
			ValueAccessor(it, columnList_), ValueAccessor(it, columnList_),
			varCxt_, sensitive_, it->isAscending());
}

template<typename L>
void SQLValues::TupleComparator::updateProfile(
		const L &subFuncList, ValueProfile *profile) {
	if (profile == NULL) {
		return;
	}

	ProfileElement &elem = profile->noSwitch_;
	for (typename L::const_iterator it = subFuncList.begin();
			it != subFuncList.end(); ++it) {
		if (it == subFuncList.begin()) {
			continue;
		}

		elem.candidate_++;
		if (*it == NULL) {
			elem.target_++;
		}
	}
}


template<typename Ret, typename A1, typename A2>
SQLValues::TupleComparator::BaseWithAccessor<Ret, A1, A2>::BaseWithAccessor(
		util::StackAllocator &alloc, const TupleComparator &base,
		ValueProfile *profile) :
		base_(base),
		subFuncList_(alloc),
		func_(NULL),
		profile_(profile) {
}


template<
		typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
		typename A1, typename A2>
SQLValues::TupleComparator::WithAccessor<
		Pred, Rev, Promo, Typed, Ordering, A1, A2>::WithAccessor(
		util::StackAllocator &alloc, const TupleComparator &base,
		ValueProfile *profile) :
		base_(alloc, base, profile) {
	const CompColumnList &columnList = base.columnList_;
	const bool nullIgnorable = base.nullIgnorable_;

	typename CompColumnList::const_iterator it = columnList.begin();
	const ValueComparator front = base.initialComparatorAt(it, Ordering);
	do {
		typename BaseType::SubFuncType subFunc = NULL;
		const bool subNullIgnorable = (nullIgnorable && !it->isOrdering());
		if (it != columnList.begin()) {
			const ValueComparator sub = base.initialComparatorAt(it, Ordering);
			const bool orderArranged = true;
			if (!sub.isSameVariant(
					front, subNullIgnorable, nullIgnorable, orderArranged)) {
				subFunc = SubSwitcherType(sub, subNullIgnorable).withProfile(
						profile).template getWith<
								const ValueComparator,
								typename SubSwitcherType::DefaultTraitsType>();
			}
		}
		base_.subFuncList_.push_back(subFunc);
	}
	while (++it != columnList.end());

	const TupleComparator noDigestBase(
			columnList, NULL, false, false, nullIgnorable,
			base.orderedDigestRestricted_);
	base_.func_ = SwitcherType(noDigestBase).withProfile(
			profile).template getWith<
					const BaseType, typename SwitcherType::DefaultTraitsType>();
	updateProfile(base_.subFuncList_, profile);
}

template<
		typename Pred, bool Rev, bool Promo, bool Typed, bool Ordering,
		typename A1, typename A2>
inline typename SQLValues::TupleComparator::WithAccessor<
		Pred, Rev, Promo, Typed, Ordering, A1, A2>::RetType
SQLValues::TupleComparator::WithAccessor<
		Pred, Rev, Promo, Typed, Ordering, A1, A2>::operator()(
		const typename A1::DigestSourceType &src1,
		const typename A2::DigestSourceType &src2) const {
	if (src1.getDigest() == src2.getDigest()) {
		return (*this)(src1.getTuple(), src2.getTuple());
	}
	return PredType()(src1.getDigest(), src2.getDigest());
}


template<typename T>
inline typename SQLValues::TupleComparator::BaseTypeAt<T>::RetType
SQLValues::TupleComparator::BaseTypeAt<T>::operator()(
		const typename AccessorType1::SubSourceType &src1,
		const typename AccessorType2::SubSourceType &src2) const {
	const TupleComparator &base = base_.base_;
	const CompColumnList &columnList = base.columnList_;

	assert(!(DIGEST_ONLY && base.withDigest_));

	CompColumnList::const_iterator it = columnList.begin();
	int32_t ret = HeadSubType(base.comparatorAt(it))(src1, src2);
	if (ret == 0 && ++it != columnList.end()) {
		typename BaseType::SubFuncList::const_iterator funcIt =
				base_.subFuncList_.begin();
		do {
			if (*(++funcIt) != NULL) {
				ret = (*funcIt)(base.comparatorAt(it), src1, src2);
			}
			else {
				ret = BodySubType(base.comparatorAt(it))(src1, src2);
			}
			if (ret != 0) {
				break;
			}
		}
		while (++it != columnList.end());
	}

	return PredType()(ret, 0);
}

template<typename T>
inline typename SQLValues::TupleComparator::BaseTypeAt<T>::RetType
SQLValues::TupleComparator::BaseTypeAt<T>::operator()(
		const typename AccessorType1::SubSourceType &src1,
		const typename AccessorType2::SubSourceType &src2,
		const util::FalseType&) const {
	const TupleComparator &base = base_.base_;
	const CompColumnList &columnList = base.columnList_;
	return HeadOnlyType(base.comparatorAt(columnList.begin()))(src1, src2);
}

template<typename T>
inline typename SQLValues::TupleComparator::BaseTypeAt<T>::RetType
SQLValues::TupleComparator::BaseTypeAt<T>::operator()(
		const typename AccessorType1::DigestSourceType &src1,
		const typename AccessorType2::DigestSourceType &src2) const {
	return TuplePred<
			DIGEST_ONLY, DIGEST_ACCESSIBLE, HEAD_ONLY>()(*this, src1, src2);
}


inline int32_t SQLValues::TupleRangeComparator::toResultWithBounds(
		const CompColumn &column, int32_t ret) {
	assert(ret != 0);

	if (column.isEitherUnbounded()) {
		if (column.isLowerUnbounded()) {
			if (ret < 0) {
				return 0;
			}
		}
		else {
			if (ret > 0) {
				return 0;
			}
		}
	}
	return ret;
}

inline int32_t SQLValues::TupleRangeComparator::toResultForExclusive(
		const CompColumn &column) {
	assert(column.isEitherExclusive());

	if (column.isLowerExclusive()) {
		return -1;
	}
	else {
		return 1;
	}
}


template<bool Promo, typename A1, typename A2>
SQLValues::TupleRangeComparator::WithAccessor<Promo, A1, A2>::WithAccessor(
		util::StackAllocator &alloc, const TupleRangeComparator &base,
		ValueProfile *profile) :
		base_(alloc, base.base_, base.columnList_, profile) {
	const TupleComparator &compBase = base.base_;
	const CompColumnList &columnList = base.columnList_;

	const bool nullIgnorable = base.nullIgnorable_;

	typename CompColumnList::const_iterator it = columnList.begin();
	const ValueComparator front = compBase.initialComparatorAt(it, false);
	do {
		typename BaseType::SubFuncType subFunc = NULL;
		const bool subNullIgnorable = (nullIgnorable && !it->isOrdering());
		if (it != columnList.begin()) {
			const ValueComparator sub =
					compBase.initialComparatorAt(it, false);
			const bool orderArranged = true;
			if (!sub.isSameVariant(
					front, subNullIgnorable, nullIgnorable, orderArranged)) {
				subFunc = SubSwitcherType(sub, subNullIgnorable).withProfile(
						profile).template getWith<
								const ValueComparator,
								typename SubSwitcherType::DefaultTraitsType>();
			}
		}
		base_.subFuncList_.push_back(subFunc);
	}
	while (++it != columnList.end());

	base_.func_ = SwitcherType(front, nullIgnorable).withProfile(
			profile).template getWith<
					const BaseType, typename SwitcherType::DefaultTraitsType>();
	TupleComparator::updateProfile(base_.subFuncList_, profile);
}


template<typename T>
inline typename SQLValues::TupleRangeComparator::BaseTypeAt<T>::RetType
SQLValues::TupleRangeComparator::BaseTypeAt<T>::operator()(
		const typename AccessorType1::SourceType &src1,
		const typename AccessorType2::SourceType &src2) const {
	const TupleComparator &base = base_.base_;
	const CompColumnList &columnList = base_.columnList_;

	typename CompColumnList::const_iterator it = columnList.begin();
	int32_t ret = SubType(base.comparatorAt(it))(src1, src2);
	if (ret != 0) {
		return toResultWithBounds(*it, ret);
	}
	else if (it->isEitherExclusive()) {
		return toResultForExclusive(*it);
	}
	else if (++it != columnList.end()) {
		typename BaseType::SubFuncList::const_iterator funcIt =
				base_.subFuncList_.begin();
		do {
			if (*(++funcIt) != NULL) {
				ret = (*funcIt)(base.comparatorAt(it), src1, src2);
			}
			else {
				ret = SubType(base.comparatorAt(it))(src1, src2);
			}
			if (ret != 0) {
				return toResultWithBounds(*it, ret);
			}
			else if (it->isEitherExclusive()) {
				return toResultForExclusive(*it);
			}
		}
		while (++it != columnList.end());
	}

	return 0;
}


inline SQLValues::ValueDigester SQLValues::TupleDigester::digesterAt(
		CompColumnList::const_iterator it, int64_t seed) const {
	return ValueDigester(
			ValueAccessor(it, columnList_), varCxt_, false, false, seed);
}


template<typename Accessor>
SQLValues::TupleDigester::BaseWithAccessor<Accessor>::BaseWithAccessor(
		util::StackAllocator &alloc, const TupleDigester &base,
		ValueProfile *profile) :
		base_(base),
		subFuncList_(alloc),
		func_(NULL),
		profile_(profile) {
}


template<typename Accessor, int32_t NullableSpecific, bool RotationSpecific>
SQLValues::TupleDigester::WithAccessor<
		Accessor, NullableSpecific, RotationSpecific>::WithAccessor(
		util::StackAllocator &alloc, const TupleDigester &base,
		ValueProfile *profile) :
		base_(alloc, base, profile) {
	assert(RotationSpecific || !base.rotating_);
	const CompColumnList &columnList = base.columnList_;

	const bool nullIgnorable = base.nullIgnorable_;

	typename CompColumnList::const_iterator it = columnList.begin();
	const ValueDigester &front = base.initialDigesterAt(it);

	do {
		if (base.ordering_) {
			break;
		}
		typename AccessorBase::SubFuncType subFunc = NULL;
		const bool subNullIgnorable = nullIgnorable;
		if (it != columnList.begin()) {
			const ValueDigester &sub = base.initialDigesterAt(it);
			if (!sub.isSameVariant(front, subNullIgnorable, nullIgnorable)) {
				subFunc = SwitcherType(sub, subNullIgnorable, false).withProfile(
						profile).template getWith<
								const ValueDigester,
								typename SubSwitcherType::DefaultTraitsType>();
			}
		}
		base_.getSubFuncList().push_back(subFunc);
	}
	while (++it != columnList.end());

	base_.setFunc(SwitcherType(front, nullIgnorable, false).withProfile(
			profile).template getWith<
					const AccessorBase,
					typename SubSwitcherType::DefaultTraitsType>());
	TupleComparator::updateProfile(base_.getSubFuncList(), profile);
}


template<typename T>
inline int64_t SQLValues::TupleDigester::BaseTypeAt<T>::operator()(
		const typename AccessorType::SourceType &src) const {
	const TupleDigester &base = base_.getBase();
	const CompColumnList &columnList = base.columnList_;
	typename CompColumnList::const_iterator it = columnList.begin();

	int64_t digest = SubType(base.digesterAt(it, INITIAL_SEED))(src);

	if (!VARIANT_ORDERED && ++it != columnList.end()) {
		typename BaseType::SubFuncList::const_iterator funcIt =
				base_.getSubFuncList().begin();
		bool nullFound = false;
		do {
			if (digest < 0) {
				digest = ValueFnv1aHasher::unmaskNull(digest);
				nullFound = true;
			}
			if (*(++funcIt) != NULL) {
				digest = (*funcIt)(base.digesterAt(it, digest), src);
			}
			else {
				digest = SubType(base.digesterAt(it, digest))(src);
			}
		}
		while (++it != columnList.end());
		if (nullFound && digest >= 0) {
			digest = ValueFnv1aHasher::maskNull(digest);
		}
	}

	return digest;
}


inline SQLValues::LongInt SQLValues::LongInt::ofElements(
		const High &high, const Low &low, const Unit &unit) {
	return LongInt(high, low, unit.get());
}

inline SQLValues::LongInt SQLValues::LongInt::zero(const Unit &unit) {
	return ofElements(0, 0, unit);
}

inline SQLValues::LongInt SQLValues::LongInt::min(const Unit &unit) {
	const Low low = 0;
	return ofElements(std::numeric_limits<High>::min(), low, unit);
}

inline SQLValues::LongInt SQLValues::LongInt::max(const Unit &unit) {
	const Low low = unit.get() - 1;
	return ofElements(std::numeric_limits<High>::max(), low, unit);
}

inline SQLValues::LongInt SQLValues::LongInt::invalid() {
	const Unit invalidUnit(0);
	return ofElements(0, 0, invalidUnit);
}

inline SQLValues::LongInt::High SQLValues::LongInt::getHigh() const {
	return high_;
}

inline SQLValues::LongInt::Low SQLValues::LongInt::getLow() const {
	return low_;
}

inline SQLValues::LongInt::Unit SQLValues::LongInt::getUnit() const {
	return Unit(unit_);
}

inline int64_t SQLValues::LongInt::toLong() const {
	assert(isValid());
	return high_;
}

inline double SQLValues::LongInt::toDouble() const {
	assert(isValid());
	return (
			static_cast<double>(high_) +
			static_cast<double>(low_) / static_cast<double>(unit_));
}

inline bool SQLValues::LongInt::isValid() const {
	return (unit_ > 0);
}

inline bool SQLValues::LongInt::isZero() const {
	assert(isValid());
	return (high_ == 0 && low_ == 0);
}

inline bool SQLValues::LongInt::isNegative() const {
	assert(isValid());
	return (high_ < 0);
}

inline bool SQLValues::LongInt::isLessThan(const LongInt &another) const {
	assert(isValidWith(another));
	return (toPair() < another.toPair());
}

inline bool SQLValues::LongInt::isLessThanEq(const LongInt &another) const {
	assert(isValidWith(another));
	return (toPair() <= another.toPair());
}

inline SQLValues::LongInt SQLValues::LongInt::add(const LongInt &base) const {
	assert(isValidWith(base));
	const Low plainLow = low_ + base.low_;

	const High high = high_ + base.high_ + static_cast<High>(plainLow / unit_);
	const Low low = plainLow % unit_;

	return LongInt(high, low, unit_);
}

inline SQLValues::LongInt SQLValues::LongInt::subtract(
		const LongInt &base) const {
	assert(isValidWith(base));
	const bool lowLess = (low_ < base.low_);

	const High high = high_ - base.high_ - (lowLess ? 1 : 0);
	const Low low = ((lowLess ? unit_ : 0) + low_) - base.low_;

	return LongInt(high, low, unit_);
}

inline SQLValues::LongInt SQLValues::LongInt::remainder(
		const LongInt &base) const {
	return subtract(truncate(base));
}

inline SQLValues::LongInt SQLValues::LongInt::truncate(
		const LongInt &base) const {
	return divide(base).multiply(base);
}

inline SQLValues::LongInt SQLValues::LongInt::negate(bool negative) const {
	assert(isValid());
	if (!negative) {
		return *this;
	}

	const High high = -high_ + (low_ == 0 ? 0 : -1);
	const Low low = (low_ == 0 ? 0 : unit_ - low_);

	return LongInt(high, low, unit_);
}

inline const SQLValues::LongInt& SQLValues::LongInt::getMax(
		const LongInt &base) const {
	return (base.isLessThan(*this) ? *this : base);
}

inline const SQLValues::LongInt& SQLValues::LongInt::getMin(
		const LongInt &base) const {
	return (base.isLessThan(*this) ? base : *this);
}

inline bool SQLValues::LongInt::checkAdditionOverflow(
		const LongInt &base, bool &upper) const {
	upper = false;

	const bool negative = isNegative();
	const bool baseNegative = base.isNegative();
	if (!negative != !baseNegative) {
		return false;
	}
	else if (negative) {
		return isLessThan(min(getUnit()).subtract(base));
	}
	else {
		upper = true;
		return max(getUnit()).subtract(base).isLessThan(*this);
	}
}

inline SQLValues::LongInt::Unit SQLValues::LongInt::mega() {
	return Unit(1000 * 1000);
}

inline SQLValues::LongInt::LongInt(
		const High &high, const Low &low, const UnitBase &unit) :
		high_(high),
		low_(low),
		unit_(unit) {
}

inline SQLValues::LongInt::Pair SQLValues::LongInt::toPair() const {
	return Pair(high_, low_);
}

inline bool SQLValues::LongInt::isValidWith(const LongInt &another) const {
	return (isValid() && another.isValid() && unit_ == another.unit_);
}

inline bool SQLValues::LongInt::isUnsignedWith(const LongInt &another) const {
	return (isValidWith(another) && !isNegative() && !another.isNegative());
}


inline SQLValues::LongInt::Unit::Unit(UnitBase base) :
		base_(base) {
}

inline SQLValues::LongInt::UnitBase SQLValues::LongInt::Unit::get() const {
	return base_;
}


inline SQLValues::DateTimeElements::DateTimeElements(const int64_t &value) :
		unixTimeMillis_(value),
		nanoSeconds_(0) {
}

inline SQLValues::DateTimeElements::DateTimeElements(
		const MicroTimestamp &value) :
		unixTimeMillis_(value.value_ / 1000),
		nanoSeconds_(static_cast<uint32_t>(value.value_ % 1000) * 1000) {
}

inline SQLValues::DateTimeElements::DateTimeElements(
		const NanoTimestamp &value) :
		unixTimeMillis_(
				value.getHigh() / (1000 * Constants::HIGH_MICRO_UNIT)),
		nanoSeconds_(
				static_cast<uint32_t>(
						value.getHigh() % (1000 * Constants::HIGH_MICRO_UNIT) *
						Constants::HIGH_MICRO_REV_UNIT) +
						value.getLow()) {
}

inline SQLValues::DateTimeElements::DateTimeElements(
		const util::DateTime &value) :
		unixTimeMillis_(value.getUnixTime()),
		nanoSeconds_(0) {
}

inline SQLValues::DateTimeElements::DateTimeElements(
		const util::PreciseDateTime &value) :
		unixTimeMillis_(value.getBase().getUnixTime()),
		nanoSeconds_(value.getNanoSeconds()) {
}

inline SQLValues::DateTimeElements SQLValues::DateTimeElements::ofLongInt(
		const LongInt &src) {
	const int64_t high = src.getHigh();
	const int64_t low = src.getLow();
	assert(0 <= low && low < 1000 * 1000);
	return DateTimeElements(util::PreciseDateTime::ofNanoSeconds(
			util::DateTime(high), static_cast<uint32_t>(low)));
}

inline int64_t SQLValues::DateTimeElements::toTimestamp(
		const Types::TimestampTag&) const {
	return unixTimeMillis_;
}

inline MicroTimestamp SQLValues::DateTimeElements::toTimestamp(
		const Types::MicroTimestampTag&) const {
	MicroTimestamp value;
	value.value_ = unixTimeMillis_ * 1000 + nanoSeconds_ / 1000;
	return value;
}

inline NanoTimestamp SQLValues::DateTimeElements::toTimestamp(
		const Types::NanoTimestampTag&) const {
	const int64_t unit = Constants::HIGH_MICRO_UNIT;
	const int64_t revUnit = Constants::HIGH_MICRO_REV_UNIT;
	const int64_t base = unixTimeMillis_;
	const int64_t nanos = nanoSeconds_;
	NanoTimestamp value;
	value.assign(
			base * (1000 * unit) + nanos / revUnit,
			static_cast<uint8_t>(nanos % revUnit));
	return value;
}

inline util::DateTime SQLValues::DateTimeElements::toDateTime(
		const Types::TimestampTag&) const {
	return util::DateTime(unixTimeMillis_);
}

inline util::PreciseDateTime SQLValues::DateTimeElements::toDateTime(
		const Types::MicroTimestampTag&) const {
	return util::PreciseDateTime::ofNanoSeconds(
			util::DateTime(unixTimeMillis_),
			nanoSeconds_ / 1000 * 1000);
}

inline util::PreciseDateTime SQLValues::DateTimeElements::toDateTime(
		const Types::NanoTimestampTag&) const {
	return util::PreciseDateTime::ofNanoSeconds(
			util::DateTime(unixTimeMillis_), nanoSeconds_);
}

inline SQLValues::LongInt SQLValues::DateTimeElements::toLongInt() const {
	return LongInt::ofElements(
			getUnixTimeMillis(), getNanoSeconds(), LongInt::mega());
}

inline int64_t SQLValues::DateTimeElements::getUnixTimeMillis() const {
	return unixTimeMillis_;
}

inline uint32_t SQLValues::DateTimeElements::getNanoSeconds() const {
	return nanoSeconds_;
}


inline SQLValues::StringBuilder::StringBuilder(ValueContext &cxt, size_t capacity) :
		data_(NULL),
		dynamicBuffer_(resolveAllocator(cxt)),
		size_(0),
		limit_(cxt.getMaxStringLength()) {
	data_ = localBuffer_;

	if (capacity > 0) {
		reserve(std::min(capacity, limit_), 0);
	}
}

inline SQLValues::StringBuilder::StringBuilder(
		ValueContext &cxt, TupleColumnType, size_t capacity) :
		data_(NULL),
		dynamicBuffer_(resolveAllocator(cxt)),
		size_(0),
		limit_(cxt.getMaxStringLength()) {
	data_ = localBuffer_;

	if (capacity > 0) {
		reserve(std::min(capacity, limit_), 0);
	}
}

inline SQLValues::StringBuilder::StringBuilder(
		const util::StdAllocator<void, void> &alloc) :
		data_(NULL),
		dynamicBuffer_(alloc),
		size_(0),
		limit_(std::numeric_limits<size_t>::max()) {
	data_ = localBuffer_;
}

inline void SQLValues::StringBuilder::setLimit(size_t limit) {
	checkLimit(size_, limit);
	limit_ = limit;
}

inline void SQLValues::StringBuilder::checkLimit(uint64_t size, size_t limit) {
	if (size > limit) {
		errorLimit(size, limit);
	}
}

inline void SQLValues::StringBuilder::appendCode(util::CodePoint c) {
	char8_t *it = data_ + size_;

	while (!util::UTF8Utils::encodeRaw(&it, data_ + capacity(), c)) {
		const size_t pos = static_cast<size_t>(it - data_);
		expandCapacity(pos);
		it = data_ + pos;
	}

	resize(it - data_);
}

inline void SQLValues::StringBuilder::append(const char8_t *data, size_t size) {
	if (std::numeric_limits<size_t>::max() - size < size_) {
		errorMaxSize(size_, size);
	}

	resize(size_ + size);
	memcpy(data_ + size_ - size, data, size);
}

inline void SQLValues::StringBuilder::appendAll(const char8_t *str) {
	appendAll(*util::SequenceUtils::toReader(str));
}

template<typename R>
inline void SQLValues::StringBuilder::appendAll(R &reader) {
	for (size_t next; (next = reader.getNext()) > 0; reader.step(next)) {
		append(&reader.get(), next);
	}
}

inline TupleValue SQLValues::StringBuilder::build(ValueContext &cxt) {
	TupleValue::SingleVarBuilder builder(
			cxt.getVarContext(), TupleTypes::TYPE_STRING, size_);
	builder.append(data_, size_);
	return builder.build();
}

inline size_t SQLValues::StringBuilder::size() const {
	return size_;
}

inline size_t SQLValues::StringBuilder::capacity() const {
	if (data_ == localBuffer_) {
		return sizeof(localBuffer_);
	}
	else {
		return dynamicBuffer_.capacity();
	}
}

inline void SQLValues::StringBuilder::resize(size_t size) {
	checkLimit(size, limit_);

	reserve(size, size_);
	size_ = size;
}

inline char8_t* SQLValues::StringBuilder::data() {
	return data_;
}

inline util::StdAllocator<void, void> SQLValues::StringBuilder::resolveAllocator(
		ValueContext &cxt) {
	SQLVarSizeAllocator *varAlloc = cxt.getVarContext().getVarAllocator();
	if (varAlloc != NULL) {
		return *varAlloc;
	}
	else {
		return cxt.getAllocator();
	}
}

inline void SQLValues::StringBuilder::reserve(
		size_t newCapacity, size_t actualSize) {

	if (data_ == localBuffer_) {
		if (newCapacity > sizeof(localBuffer_)) {
			initializeDynamicBuffer(newCapacity, actualSize);
		}
	}
	else {
		dynamicBuffer_.resize(newCapacity);
		data_ = dynamicBuffer_.data();
	}
}


template<typename Base, typename T>
SQLValues::LimitedReader<Base, T>::LimitedReader(
		Base &base, size_t limit) :
		base_(base), rest_(limit) {
}

template<typename Base, typename T>
inline void SQLValues::LimitedReader<Base, T>::next() {
	assert(hasNext());

	if (rest_ > 0) {
		base_.next();
		--rest_;
	}
}

template<typename Base, typename T>
inline bool SQLValues::LimitedReader<Base, T>::hasNext() const {
	return (rest_ > 0 && base_.hasNext());
}

template<typename Base, typename T>
inline const T& SQLValues::LimitedReader<Base, T>::get() const {
	return base_.get();
}


template<typename T>
SQLValues::BufferBuilder<T>::BufferBuilder(T *begin, T *end) :
		it_(begin),
		begin_(begin),
		end_(end),
		truncated_(false) {
}

template<typename T>
inline bool SQLValues::BufferBuilder<T>::isTruncated() const {
	return truncated_;
}

template<typename T>
inline size_t SQLValues::BufferBuilder<T>::getSize() const {
	return static_cast<size_t>(it_ - begin_);
}

template<typename T>
inline void SQLValues::BufferBuilder<T>::append(
		const T *data, size_t size) {

	const size_t partSize = std::min(size, static_cast<size_t>(end_ - it_));
	truncated_ |= (partSize != size);

	memcpy(it_, data, partSize);
	it_ += partSize;
}


inline int32_t SQLValues::ValueUtils::compareIntegral(int64_t v1, int64_t v2) {
	return compareRawValue(v1, v2);
}

inline int32_t SQLValues::ValueUtils::comparePreciseTimestamp(
		const MicroTimestamp &v1, const MicroTimestamp &v2) {
	return compareRawValue(v1.value_, v2.value_);
}

inline int32_t SQLValues::ValueUtils::comparePreciseTimestamp(
		const NanoTimestamp &v1, const NanoTimestamp &v2) {
	const int32_t comp = compareRawValue(v1.getHigh(), v2.getHigh());
	if (comp != 0) {
		return comp;
	}

	return compareRawValue(v1.getLow(), v2.getLow());
}

template<typename R>
int32_t SQLValues::ValueUtils::compareSequence(R &v1, R &v2) {
	for (;;) {
		const size_t next1 = v1.getNext();
		const size_t next2 = v2.getNext();

		const size_t size =
				static_cast<size_t>(std::min<uint64_t>(next1, next2));
		if (size == 0) {
			return compareRawValue(next1, next2);
		}

		const int32_t comp = memcmp(&v1.get(), &v2.get(), size);
		if (comp != 0) {
			return comp;
		}

		v1.step(size);
		v2.step(size);
	}
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueUtils::compareIntegral(
		int64_t v1, int64_t v2, const Pred &pred) {
	return pred(v1, v2);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueUtils::compareFloating(
		double v1, double v2, bool sensitive, const Pred &pred) {
	return pred(compareFloating(v1, v2, sensitive), 0);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueUtils::comparePreciseTimestamp(
		const MicroTimestamp &v1, const MicroTimestamp &v2,
		const Pred &pred) {
	return pred(v1.value_, v2.value_);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueUtils::comparePreciseTimestamp(
		const NanoTimestamp &v1, const NanoTimestamp &v2,
		const Pred &pred) {
	return pred(comparePreciseTimestamp(v1, v2), 0);
}

template<typename Pred>
inline typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueUtils::compareString(
		const TupleString &v1, const TupleString &v2, const Pred &pred) {
	typedef typename util::BinaryFunctionResultOf<Pred>::Type RetType;
	return compareString<Pred, RetType>(v1, v2, pred);
}

template<typename Pred, typename Ret>
inline Ret SQLValues::ValueUtils::compareString(
		const TupleString &v1, const TupleString &v2, const Pred &pred) {
	const uint8_t *it1 = static_cast<const uint8_t*>(v1.data());
	const uint8_t *it2 = static_cast<const uint8_t*>(v2.data());

	int32_t sizeDiff =
			static_cast<int32_t>(*it1) - static_cast<int32_t>(*it2);

	if (sizeDiff != 0) {
		const bool commutative = (pred(1, 0) == pred(0, 1));
		if (commutative) {
			const Ret negative = pred(1, 0);
			return negative;
		}
	}

	if ((*it1 & 0x01) == 0 || (*it2 & 0x01) == 0) {
		return pred(ValueUtils::compareString(v1, v2), 0);
	}

	const int32_t rest =
			static_cast<int32_t>((sizeDiff <= 0 ? *it1 : *it2) >> 1);
	return compareBuffer<Pred, Ret>(++it1, ++it2, rest, sizeDiff, pred);
}

template<typename Pred, typename R>
typename util::BinaryFunctionResultOf<Pred>::Type
SQLValues::ValueUtils::compareSequence(R &v1, R &v2, const Pred &pred) {
	return pred(compareSequence(v1, v2), 0);
}

template<typename Pred, typename Ret>
inline Ret SQLValues::ValueUtils::compareBuffer(
		const uint8_t *it1, const uint8_t *it2, ptrdiff_t rest, ptrdiff_t sizeDiff,
		const Pred &pred) {

	if ((rest -= 8) > 0) {
		for (;;) {
			if (*((const int64_t*) it1) != *((const int64_t*) it2)) {
				break;
			}
			it1 += 8;
			it2 += 8;
			if ((rest -= 8) < 0) {
				break;
			}
		}
	}

	if (rest >= -4 && *((const int32_t*) it1) == *((const int32_t*) it2)) {
		it1 += 4;
		it2 += 4;
		rest -= 4;
	}

	if (rest >= -6 && *((const int16_t*) it1) == *((const int16_t*) it2)) {
		it1 += 2;
		it2 += 2;
		rest -= 2;
	}

	if (rest > -8 && *it1 == *it2) {
		++it1;
		++it2;
		--rest;
	}

	if (rest > -8 && *it1 != *it2) {
		return pred(*it1, *it2);
	}

	return pred(sizeDiff, static_cast<ptrdiff_t>(0));
}

template<typename T>
inline int32_t SQLValues::ValueUtils::compareRawValue(
		const T &v1, const T &v2) {
	if (v1 != v2) {
		if (v1 < v2) {
			return -1;
		}
		else {
			return 1;
		}
	}
	return 0;
}

inline int32_t SQLValues::ValueUtils::compareNull(
		bool nullValue1, bool nullValue2) {
	return (nullValue1 ? 1 : 0) - (nullValue2 ? 1 : 0);
}


inline uint32_t SQLValues::ValueUtils::fnv1aHashPreciseTimestamp(
		uint32_t base, const MicroTimestamp &value) {
	return fnv1aHashPreciseTimestamp(base, DateTimeElements(value));
}

inline uint32_t SQLValues::ValueUtils::fnv1aHashPreciseTimestamp(
		uint32_t base, const NanoTimestamp &value) {
	return fnv1aHashPreciseTimestamp(base, DateTimeElements(value));
}

template<typename R>
uint32_t SQLValues::ValueUtils::fnv1aHashSequence(uint32_t base, R &value) {
	uint32_t hash = base;

	for (size_t size; (size = value.getNext()) > 0;) {
		hash = fnv1aHashBytes(hash, &value.get(), size);
		value.step(size);
	}

	return hash;
}


template<typename Asc>
inline int64_t SQLValues::ValueUtils::digestOrderedNull(const Asc&) {
	return (Asc::VALUE ?
			std::numeric_limits<int64_t>::max() :
			std::numeric_limits<int64_t>::min());
}

inline int64_t SQLValues::ValueUtils::digestOrderedIntegral(
		int64_t value, const util::TrueType&) {
	return value;
}

inline int64_t SQLValues::ValueUtils::digestOrderedIntegral(
		int64_t value, const util::FalseType&) {
	union {
		uint64_t asUnsigned_;
		int64_t asSigned_;
	} data;
	data.asSigned_ = value;
	data.asUnsigned_ = ~data.asUnsigned_;
	return data.asSigned_;
}

template<typename Asc>
inline int64_t SQLValues::ValueUtils::digestOrderedFloating(
		double value, const Asc&) {
	union {
		double asDouble_;
		uint64_t asUnsigned_;
		int64_t asSigned_;
	} data;
	data.asDouble_ = value;

	if (data.asSigned_ < 0) {
		const uint64_t flag = (static_cast<uint64_t>(1) <<
				std::numeric_limits<int64_t>::digits);
		data.asUnsigned_ = ((~data.asUnsigned_) | flag);
	}

	return digestOrderedIntegral(data.asSigned_, Asc());
}

template<typename Asc>
inline int64_t SQLValues::ValueUtils::digestOrderedPreciseTimestamp(
		const MicroTimestamp &value, const Asc&) {
	return digestOrderedIntegral(value.value_, Asc());
}

template<typename Asc>
inline int64_t SQLValues::ValueUtils::digestOrderedPreciseTimestamp(
		const NanoTimestamp &value, const Asc&) {
	return digestOrderedIntegral(
			DateTimeElements(value).getUnixTimeMillis(), Asc());
}

template<typename Asc, typename R>
inline int64_t SQLValues::ValueUtils::digestOrderedSequence(
		R &value, const Asc&) {
	uint8_t buf[sizeof(int64_t)] = { 0 };

	if (value.hasNext()) {
		uint8_t *const begin = buf;
		uint8_t *it = begin + sizeof(buf);
		{
			const uint8_t top = static_cast<uint8_t>(value.get());
			*(--it) = static_cast<uint8_t>((0x80 & (~top)) | (0x7f & top));
		}
		while ((value.next(), value.hasNext())) {
			*(--it) = static_cast<uint8_t>(value.get());
			if (it == begin) {
				break;
			}
		}
	}

	union {
		uint64_t in_;
		int64_t out_;
	} data;

	const uint64_t shift =
			std::numeric_limits<uint64_t>::digits / sizeof(uint64_t);
	data.in_ =
			(static_cast<uint64_t>(buf[7]) << (shift * 7)) |
			(static_cast<uint64_t>(buf[6]) << (shift * 6)) |
			(static_cast<uint64_t>(buf[5]) << (shift * 5)) |
			(static_cast<uint64_t>(buf[4]) << (shift * 4)) |
			(static_cast<uint64_t>(buf[3]) << (shift * 3)) |
			(static_cast<uint64_t>(buf[2]) << (shift * 2)) |
			(static_cast<uint64_t>(buf[1]) << (shift * 1)) |
			(static_cast<uint64_t>(buf[0]) << (shift * 0));

	return digestOrderedIntegral(data.out_, Asc());
}

template<typename T, typename Asc>
typename T::ValueType SQLValues::ValueUtils::toValueByOrderdDigest(
		int64_t digest) {
	typedef typename TypeUtils::template Traits<
			T::COLUMN_TYPE>::ResultFineComparableTag ComparableTag;
	return getValueByComparable<T>(
			toValueByOrderdDigest(digest, Asc(), ComparableTag()));
}

template<typename Asc>
inline TupleValue SQLValues::ValueUtils::toValueByOrderdDigest(
		int64_t digest, const Asc&, const Types::Any&) {
	static_cast<void>(digest);
	return TupleValue();
}

template<typename Asc>
inline int64_t SQLValues::ValueUtils::toValueByOrderdDigest(
		int64_t digest, const Asc&, const Types::Integral&) {
	return digestOrderedIntegral(digest, Asc());
}

template<typename Asc>
inline double SQLValues::ValueUtils::toValueByOrderdDigest(
		int64_t digest, const Asc&, const Types::Floating&) {
	union {
		double asDouble_;
		uint64_t asUnsigned_;
		int64_t asSigned_;
	} data;
	data.asSigned_ = digestOrderedIntegral(digest, Asc());

	if (data.asSigned_ < 0) {
		const uint64_t flag = (static_cast<uint64_t>(1) <<
				std::numeric_limits<int64_t>::digits);
		data.asUnsigned_ = ((~data.asUnsigned_) | flag);
	}

	return data.asDouble_;
}

template<typename Asc>
inline MicroTimestamp SQLValues::ValueUtils::toValueByOrderdDigest(
		int64_t digest, const Asc&, const Types::MicroTimestampTag&) {
	MicroTimestamp ts;
	ts.value_ = digestOrderedIntegral(digest, Asc());
	return ts;
}

inline int64_t SQLValues::ValueUtils::rotateDigest(
		int64_t digest, const util::TrueType&) {
	uint64_t src;
	if (digest < 0) {
		src = ~(static_cast<uint64_t>(digest) << 1U);
	}
	else {
		src = static_cast<uint64_t>(digest) << 1U;
	}
	if (static_cast<int64_t>(src) < 0 &&
			src >= ValueOrderedDigester::Constants::ROT_THRESHOLD) {
		return src;
	}
	const uint64_t prime = ValueOrderedDigester::Constants::ROT_PRIME;
	const uint64_t base = ValueOrderedDigester::Constants::ROT_BASE;
	return static_cast<int64_t>((src % prime) * base + src / prime);
}

inline int64_t SQLValues::ValueUtils::rotateDigest(
		int64_t digest, const util::FalseType&) {
	return digest;
}

inline int64_t SQLValues::ValueUtils::digestByRotation(
		int64_t rotated, const util::TrueType&) {
	const uint64_t src = static_cast<uint64_t>(rotated);
	uint64_t masked;
	if (rotated < 0 &&
			src >= ValueOrderedDigester::Constants::ROT_THRESHOLD) {
		masked = src;
	}
	else {
		const uint64_t prime = ValueOrderedDigester::Constants::ROT_PRIME;
		const uint64_t base = ValueOrderedDigester::Constants::ROT_BASE;
		masked = (src % base) * prime + src / base;
	}
	if ((masked & 0x1) != 0) {
		return static_cast<int64_t>(~(masked >> 1U));
	}
	else {
		return static_cast<int64_t>(masked >> 1U);
	}
}

inline int64_t SQLValues::ValueUtils::digestByRotation(
		int64_t rotated, const util::FalseType&) {
	return rotated;
}


template<TupleColumnType T>
inline typename SQLValues::TypeUtils::Traits<T>::LocalValueType
SQLValues::ValueUtils::promoteValue(const TupleValue &src) {
	typedef typename TypeUtils::Traits<T>::ValueType Type;
	return getValue<Type>(src);
}

template<>
inline int64_t SQLValues::ValueUtils::promoteValue<TupleTypes::TYPE_LONG>(
		const TupleValue &src) {
	return toLong(src);
}

template<>
inline double SQLValues::ValueUtils::promoteValue<TupleTypes::TYPE_DOUBLE>(
		const TupleValue &src) {
	return toDouble(src);
}

template<>
inline int64_t SQLValues::ValueUtils::promoteValue<TupleTypes::TYPE_TIMESTAMP>(
		const TupleValue &src) {
	typedef Types::TimestampTag DestTypeTag;
	const TupleColumnType type = src.getType();
	if (type != TupleTypes::TYPE_TIMESTAMP) {
		if (type == TupleTypes::TYPE_MICRO_TIMESTAMP) {
			return getPromoted<DestTypeTag, Types::MicroTimestampTag>(
					getValue<MicroTimestamp>(src));
		}
		assert(type == TupleTypes::TYPE_NANO_TIMESTAMP);
		return getPromoted<DestTypeTag, Types::NanoTimestampTag>(
				getValue<TupleNanoTimestamp>(src));
	}
	return getValue<int64_t>(src);
}

template<>
inline MicroTimestamp SQLValues::ValueUtils::promoteValue<
		TupleTypes::TYPE_MICRO_TIMESTAMP>(const TupleValue &src) {
	typedef Types::MicroTimestampTag DestTypeTag;
	const TupleColumnType type = src.getType();
	if (type != TupleTypes::TYPE_MICRO_TIMESTAMP) {
		if (type == TupleTypes::TYPE_TIMESTAMP) {
			return getPromoted<DestTypeTag, Types::TimestampTag>(
					getValue<int64_t>(src));
		}
		assert(type == TupleTypes::TYPE_NANO_TIMESTAMP);
		return getPromoted<DestTypeTag, Types::NanoTimestampTag>(
				getValue<TupleNanoTimestamp>(src));
	}
	return getValue<MicroTimestamp>(src);
}

template<>
inline NanoTimestamp SQLValues::ValueUtils::promoteValue<
		TupleTypes::TYPE_NANO_TIMESTAMP>(const TupleValue &src) {
	typedef Types::NanoTimestampTag DestTypeTag;
	const TupleColumnType type = src.getType();
	if (type != TupleTypes::TYPE_NANO_TIMESTAMP) {
		if (type == TupleTypes::TYPE_MICRO_TIMESTAMP) {
			return getPromoted<DestTypeTag, Types::MicroTimestampTag>(
					getValue<MicroTimestamp>(src));
		}
		assert(type == TupleTypes::TYPE_TIMESTAMP);
		return getPromoted<DestTypeTag, Types::TimestampTag>(
				getValue<int64_t>(src));
	}
	return getValue<TupleNanoTimestamp>(src);
}

template<typename T>
T SQLValues::ValueUtils::toIntegral(const TupleValue &src) {
	const TupleColumnType type =
			TypeUtils::template ValueTraits<T>::COLUMN_TYPE_AS_NUMERIC;
	if (src.getType() != type) {
		T dest;
		const bool strict = true;
		toIntegralDetail(src, dest, strict);
		return dest;
	}

	return getValue<T>(src);
}

template<typename T>
T SQLValues::ValueUtils::toFloating(const TupleValue &src) {
	const TupleColumnType type =
			TypeUtils::template ValueTraits<T>::COLUMN_TYPE_AS_NUMERIC;
	if (src.getType() != type) {
		T dest;
		const bool strict = true;
		toFloatingDetail(src, dest, strict);
		return dest;
	}

	return getValue<T>(src);
}

template<typename T>
bool SQLValues::ValueUtils::toIntegralDetail(
		const TupleValue &src, T &dest, bool strict) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::is_integer);
	do {
		int64_t longValue;
		if (!toLongDetail(src, longValue, strict)) {
			dest = T();
			break;
		}

		if (longValue < std::numeric_limits<T>::min()) {
			dest = errorNumericOverflow(
					longValue, std::numeric_limits<T>::min(), strict);
			break;
		}

		if (longValue > std::numeric_limits<T>::max()) {
			dest = errorNumericOverflow(
					longValue, std::numeric_limits<T>::max(), strict);
			break;
		}

		dest = static_cast<T>(longValue);
		return true;
	}
	while (false);

	return false;
}

template<typename T>
bool SQLValues::ValueUtils::toFloatingDetail(
		const TupleValue &src, T &dest, bool strict) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::has_infinity);
	do {
		double doubleValue;
		if (!toDoubleDetail(src, doubleValue, strict)) {
			dest = T();
			break;
		}

		if (doubleValue < -std::numeric_limits<T>::max() &&
				doubleValue != -std::numeric_limits<double>::infinity()) {
			dest = errorNumericOverflow(
					doubleValue, std::numeric_limits<T>::min(), strict);
			break;
		}

		if (doubleValue > std::numeric_limits<T>::max() &&
				doubleValue != std::numeric_limits<double>::infinity()) {
			dest = errorNumericOverflow(
					doubleValue, std::numeric_limits<T>::max(), strict);
			break;
		}

		dest = static_cast<T>(doubleValue);
		return true;
	}
	while (false);

	return false;
}


template<TupleColumnType T>
inline TupleValue SQLValues::ValueUtils::toAnyByValue(
		const typename TypeUtils::Traits<T>::ValueType &src) {
	return TupleValue(src);
}

template<> inline TupleValue SQLValues::ValueUtils::toAnyByValue<
		TupleTypes::TYPE_TIMESTAMP>(const int64_t &src) {
	return toAnyByTimestamp(src);
}

template<TupleColumnType T>
inline TupleValue SQLValues::ValueUtils::toAnyByWritable(
		ValueContext &cxt,
		typename TypeUtils::Traits<T>::WritableRefType src) {
	static_cast<void>(cxt);
	return toAnyByValue<T>(src);
}

template<> inline TupleValue SQLValues::ValueUtils::toAnyByWritable<
		TupleTypes::TYPE_NANO_TIMESTAMP>(
		ValueContext &cxt, const NanoTimestamp &src) {
	return createNanoTimestampValue(cxt, src);
}

template<> inline TupleValue SQLValues::ValueUtils::toAnyByWritable<
		TupleTypes::TYPE_STRING>(ValueContext &cxt, StringBuilder &src) {
	return src.build(cxt);
}

template<> inline TupleValue SQLValues::ValueUtils::toAnyByWritable<
		TupleTypes::TYPE_BLOB>(ValueContext &cxt, LobBuilder &src) {
	static_cast<void>(cxt);
	return src.build();
}

template<typename T>
TupleValue SQLValues::ValueUtils::toAnyByNumeric(T src) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::is_specialized);
	return TupleValue(src);
}

inline TupleValue SQLValues::ValueUtils::toAnyByLongInt(
		ValueContext &cxt, const LongInt &src) {
	if (!src.isValid()) {
		return TupleValue();
	}
	else if (src.getLow() != 0) {
		return createNanoTimestampValue(
				cxt, DateTimeElements::ofLongInt(src).toTimestamp(
						Types::NanoTimestampTag()));
	}
	return toAnyByTimestamp(src.getHigh());
}

inline TupleValue SQLValues::ValueUtils::toAnyByTimestamp(int64_t src) {
	return TupleValue(src, TupleValue::TimestampTag());
}

inline TupleValue SQLValues::ValueUtils::toAnyByBool(bool src) {
	return TupleValue(src);
}

inline TupleValue SQLValues::ValueUtils::createNanoTimestampValue(
		util::StackAllocator &alloc, const NanoTimestamp &ts) {
	return TupleValue(ALLOC_NEW(alloc) NanoTimestamp(ts));
}

inline TupleValue SQLValues::ValueUtils::createNanoTimestampValue(
		ValueContext &cxt, const NanoTimestamp &ts) {
	TupleValue::NanoTimestampBuilder builder(cxt.getVarContext());
	builder.setValue(ts);
	return TupleNanoTimestamp(builder.build());
}


template<typename T>
T SQLValues::ValueUtils::getValue(const TupleValue &src) {
	return src.get<T>();
}

template<>
inline TupleValue SQLValues::ValueUtils::getValue(const TupleValue &src) {
	return src;
}

template<>
inline int8_t SQLValues::ValueUtils::getValue(const TupleValue &src) {
	int8_t result;
	memcpy(&result, src.fixedData(), sizeof(result));
	return result;
}

template<>
inline int16_t SQLValues::ValueUtils::getValue(const TupleValue &src) {
	int16_t result;
	memcpy(&result, src.fixedData(), sizeof(result));
	return result;
}

template<>
inline int32_t SQLValues::ValueUtils::getValue(const TupleValue &src) {
	int32_t result;
	memcpy(&result, src.fixedData(), sizeof(result));
	return result;
}

template<>
inline float SQLValues::ValueUtils::getValue(const TupleValue &src) {
	float result;
	memcpy(&result, src.fixedData(), sizeof(result));
	return result;
}

template<>
inline bool SQLValues::ValueUtils::getValue(const TupleValue &src) {
	return !!getValue<int64_t>(src);
}

template<>
inline TupleString SQLValues::ValueUtils::getValue(const TupleValue &src) {
	return src;
}

template<typename T>
inline typename T::ValueType SQLValues::ValueUtils::getValueByComparable(
		const typename TypeUtils::Traits<
				T::COLUMN_TYPE>::ComparableValueType &src) {
	UTIL_STATIC_ASSERT((!util::IsSame<typename T::ValueType, bool>::VALUE));
	return static_cast<typename T::ValueType>(src);
}

template<>
inline bool SQLValues::ValueUtils::getValueByComparable<
		SQLValues::Types::Bool>(
		const TypeUtils::Traits<
				Types::Bool::COLUMN_TYPE>::ComparableValueType &src) {
	return !!src;
}

template<typename T>
inline typename T::ValueType SQLValues::ValueUtils::getValueByAddress(
		const void *src) {
	UTIL_STATIC_ASSERT((TypeUtils::Traits<T::COLUMN_TYPE>::FOR_MULTI_VAR));
	return getValue<typename T::ValueType>(TupleValue(src, T::COLUMN_TYPE));
}

template<>
inline TupleNanoTimestamp SQLValues::ValueUtils::getValueByAddress<
		SQLValues::Types::NanoTimestampTag>(const void *src) {
	return TupleNanoTimestamp(static_cast<const NanoTimestamp*>(src));
}

template<>
inline TupleString SQLValues::ValueUtils::getValueByAddress<
		SQLValues::Types::String>(const void *src) {
	return TupleString(src);
}

template<typename T, typename S>
inline typename T::LocalValueType SQLValues::ValueUtils::getPromoted(
		const typename S::LocalValueType &src) {
	UTIL_STATIC_ASSERT(
			(TypeUtils::PairTraits<
					S::COLUMN_TYPE, T::COLUMN_TYPE
					>::IMPLICIT_CONVERTABLE_OR_SAME));
	return static_cast<typename T::LocalValueType>(src);
}

template<>
inline int64_t SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::TimestampTag, SQLValues::Types::MicroTimestampTag>(
		const MicroTimestamp &src) {
	return DateTimeElements(src).toTimestamp(Types::TimestampTag());
}

template<>
inline int64_t SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::TimestampTag, SQLValues::Types::NanoTimestampTag>(
		const NanoTimestamp &src) {
	return DateTimeElements(src).toTimestamp(Types::TimestampTag());
}

template<>
inline MicroTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::MicroTimestampTag, SQLValues::Types::TimestampTag>(
		const int64_t &src) {
	return DateTimeElements(src).toTimestamp(Types::MicroTimestampTag());
}

template<>
inline MicroTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::MicroTimestampTag, SQLValues::Types::NanoTimestampTag>(
		const NanoTimestamp &src) {
	return DateTimeElements(src).toTimestamp(Types::MicroTimestampTag());
}

template<>
inline NanoTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::NanoTimestampTag, SQLValues::Types::TimestampTag>(
		const int64_t &src) {
	return DateTimeElements(src).toTimestamp(Types::NanoTimestampTag());
}

template<>
inline NanoTimestamp SQLValues::ValueUtils::getPromoted<
		SQLValues::Types::NanoTimestampTag, SQLValues::Types::MicroTimestampTag>(
		const MicroTimestamp &src) {
	return DateTimeElements(src).toTimestamp(Types::NanoTimestampTag());
}

template<TupleColumnType T>
inline typename SQLValues::TypeUtils::Traits<T>::ReadableSourceType
SQLValues::ValueUtils::toReadableSource(const TupleValue &src) {
	return promoteValue<T>(src);
}

template<>
inline TupleString::BufferInfo SQLValues::ValueUtils::toReadableSource<
		TupleTypes::TYPE_STRING>(const TupleValue &src) {
	return getValue<TupleString>(src).getBuffer();
}

template<>
inline SQLValues::StringReaderRef SQLValues::ValueUtils::toReader<
		TupleTypes::TYPE_STRING>(const TupleString &src) {
	return toStringReader(src);
}

template<>
inline SQLValues::LobReaderRef SQLValues::ValueUtils::toReader<
		TupleTypes::TYPE_BLOB>(const TupleValue &src) {
	return toLobReader(src);
}

inline SQLValues::StringReaderRef SQLValues::ValueUtils::toStringReader(
		const char8_t *src) {
	return util::SequenceUtils::toReader(src);
}

inline SQLValues::StringReaderRef SQLValues::ValueUtils::toStringReader(
		const TupleString &src) {
	const TupleString::BufferInfo &buf = src.getBuffer();
	return util::SequenceUtils::toReader(buf.first, buf.first + buf.second);
}

inline SQLValues::StringReaderRef SQLValues::ValueUtils::toStringReader(
		const TupleString::BufferInfo &src) {
	return util::SequenceUtils::toReader(src.first, src.first + src.second);
}

template<>
inline SQLValues::ValueContext& SQLValues::ValueUtils::toWriterContext<
		SQLValues::Types::String>(ValueContext &cxt) {
	return cxt;
}

template<>
inline SQLValues::VarContext& SQLValues::ValueUtils::toWriterContext<
		SQLValues::Types::Blob>(ValueContext &cxt) {
	return cxt.getVarContext();
}

template<>
inline size_t SQLValues::ValueUtils::toWriterCapacity<
		SQLValues::Types::String>(uint64_t base) {
	return static_cast<size_t>(
			std::min<uint64_t>(std::numeric_limits<size_t>::max(), base));
}

template<>
inline size_t SQLValues::ValueUtils::toWriterCapacity<
		SQLValues::Types::Blob>(uint64_t base) {
	return toLobCapacity(base);
}

inline const void* SQLValues::ValueUtils::getData(const TupleValue &value) {
	const TupleColumnType type = value.getType();
	if (TypeUtils::isSomeFixed(type)) {
		if (TypeUtils::isLargeFixed(type)) {
			return value.getRawData();
		}
		return value.fixedData();
	}
	else {
		assert(TypeUtils::isSingleVar(type));
		return value.varData();
	}
}

inline size_t SQLValues::ValueUtils::getDataSize(const TupleValue &value) {
	const TupleColumnType type = value.getType();
	if (TypeUtils::isSomeFixed(type)) {
		return TypeUtils::getFixedSize(type);
	}
	else {
		assert(TypeUtils::isSingleVar(type));
		return value.varSize();
	}
}


inline bool SQLValues::ValueUtils::isNull(const TupleValue &value) {
	return value.getType() == TupleTypes::TYPE_NULL;
}


template<typename R>
inline int64_t SQLValues::ValueUtils::codeLength(R &reader) {
	typedef typename R::CodeType CodeType;

	const bool forCodePoint = util::IsSame<CodeType, util::CodePoint>::VALUE;
	typedef typename util::BoolType<forCodePoint>::Result ForCodePoint;

	return codeLength(reader, ForCodePoint());
}

template<typename R>
inline int64_t SQLValues::ValueUtils::codeLength(
		R &reader, const util::TrueType&) {
	int64_t total = 0;
	while (reader.nextCode()) {
		total++;
	}
	return total;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::codeLength(
		R &reader, const util::FalseType&) {
	return partLength(reader);
}

template<typename R>
inline int64_t SQLValues::ValueUtils::codeLengthLimited(
		R &reader, int64_t limitSize) {
	typedef typename R::CodeType CodeType;

	const bool forCodePoint = util::IsSame<CodeType, util::CodePoint>::VALUE;
	typedef typename util::BoolType<forCodePoint>::Result ForCodePoint;

	return codeLengthLimited(reader, limitSize, ForCodePoint());
}

template<typename R>
inline int64_t SQLValues::ValueUtils::codeLengthLimited(
		R &reader, int64_t limitSize, const util::TrueType&) {
	int64_t total = 0;
	int64_t rest = limitSize;
	for (util::CodePoint c; rest > 0 && reader.nextCode(&c);) {
		char8_t buf[util::UTF8Utils::MAX_ENCODED_LENGTH];

		char8_t *bufIt = buf;
		util::UTF8Utils::encode(&bufIt, buf + sizeof(buf), c);
		const ptrdiff_t codeSize = bufIt - buf;

		rest -= codeSize;
		if (rest < 0) {
			break;
		}

		total++;
	}
	return total;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::codeLengthLimited(
		R &reader, int64_t limitSize, const util::FalseType&) {
	static_cast<void>(reader);
	return limitSize;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::partLength(R &reader) {
	int64_t total = 0;
	while (reader.hasNext()) {
		const size_t available = reader.getNext();
		total += static_cast<int64_t>(available);
		reader.step(available);
	}
	return total;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::stepAllPart(
		R &reader, int64_t size) {
	assert(size >= 0);
	for (int64_t rest = size; rest > 0;) {
		const size_t available = reader.getNext();
		if (available <= 0) {
			return rest;
		}
		const size_t curSize = static_cast<size_t>(std::min(
				static_cast<uint64_t>(available),
				static_cast<uint64_t>(rest)));
		reader.step(curSize);
		rest -= static_cast<int64_t>(curSize);
	}
	return 0;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::backAllPart(
		R &reader, int64_t size) {
	assert(size >= 0);
	for (int64_t rest = size; rest > 0;) {
		const size_t available = reader.getPrev();
		if (available <= 0) {
			return rest;
		}
		const size_t curSize = static_cast<size_t>(std::min(
				static_cast<uint64_t>(available),
				static_cast<uint64_t>(rest)));
		reader.back(curSize);
		rest -= static_cast<int64_t>(curSize);
	}
	return 0;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::findPart(
		R &reader1, R &reader2, int64_t offset1) {
	int64_t pos = 1;

	if (!reader2.hasNext()) {
		while (reader1.hasNext()) {
			reader1.step(reader1.getNext());
		}
		return pos;
	}

	int64_t foundCount = 0;
	for (; reader1.hasNext(); reader1.next(), pos++) {
		if (reader1.get() != reader2.get()) {
			if (foundCount == 0) {
				continue;
			}

			reader1.toBegin();
			pos -= foundCount;
			if (stepAllPart(reader1, pos + offset1) > 0) {
				assert(false);
			}
			pos++;
			assert(reader1.hasNext());

			reader2.toBegin();
			foundCount = 0;

			if (reader1.get() != reader2.get()) {
				continue;
			}
		}

		reader2.next();

		if (!reader2.hasNext()) {
			reader1.next();
			return pos - foundCount;
		}

		foundCount++;
	}

	return 0;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::findPartBack(
		R &reader1, R &reader2, int64_t offset1) {
	int64_t pos = 1;

	if (reader2.getPrev() <= 0) {
		while (reader1.getPrev() > 0) {
			reader1.back(reader1.getPrev());
		}
		return pos;
	}

	int64_t foundCount = 0;
	for (; reader1.getPrev() > 0; pos++) {
		reader1.prev();
		reader2.prev();
		if (reader1.get() != reader2.get()) {
			reader2.next();

			if (foundCount == 0) {
				continue;
			}

			reader1.toEnd();
			pos -= foundCount;
			if (backAllPart(reader1, pos + offset1) > 0) {
				assert(false);
			}
			pos++;
			assert(reader1.getPrev() > 0);

			reader2.toEnd();
			foundCount = 0;

			reader1.prev();
			reader2.prev();
			if (reader1.get() != reader2.get()) {
				reader2.next();
				continue;
			}
		}

		if (reader2.getPrev() <= 0) {
			return pos - foundCount;
		}

		foundCount++;
	}

	return 0;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::findLtrimPart(
		R &reader1, R &reader2) {
	const size_t size = reader1.getNext();
	for (util::CodePoint c1; reader1.nextCode(&c1);) {
		bool found = false;
		for (util::CodePoint c2; reader2.nextCode(&c2);) {
			if (c1 == c2) {
				found = true;
				break;
			}
			found = false;
		}
		if (!found) {
			reader1.prevCode(&c1);
			break;
		}
		reader2.toBegin();
	}
	return static_cast<int64_t>(size - reader1.getNext()) + 1;
}

template<typename R>
inline int64_t SQLValues::ValueUtils::findRtrimPart(
		R &reader1, R &reader2) {
	const size_t size = reader1.getNext();
	reader1.toEnd();
	for (util::CodePoint c1; reader1.prevCode(&c1);) {
		bool found = false;
		for (util::CodePoint c2; reader2.nextCode(&c2);) {
			if (c1 == c2) {
				found = true;
				break;
			}
			found = false;
		}
		if (!found) {
			reader1.nextCode(&c1);
			break;
		}
		reader2.toBegin();
	}
	return static_cast<int64_t>(size - reader1.getNext()) + 1;
}

template<typename R>
int64_t SQLValues::ValueUtils::findPartRepeatable(
		R &reader1, R &reader2, int64_t startPos1, int64_t repeat) {
	if (startPos1 == 0 || repeat <= 0) {
		return 0;
	}

	if (startPos1 < 0) {
		if (startPos1 <= std::numeric_limits<int64_t>::min()) {
			return 0;
		}

		reader1.toBegin();
		const int64_t len = partLength(reader1);
		int64_t keyLen = 0;

		int64_t foundPos = -startPos1;
		for (;;) {
			const int64_t lastOffset = foundPos - 1;

			reader1.toEnd();
			if (backAllPart(reader1, lastOffset) > 0) {
				return 0;
			}

			reader2.toEnd();
			const int64_t subPos = findPartBack(reader1, reader2, lastOffset);
			if (subPos <= 0) {
				return 0;
			}

			foundPos += subPos - 1;

			if (keyLen <= 0) {
				reader2.toBegin();
				keyLen = partLength(reader2);
			}
			foundPos += keyLen;

			if (--repeat <= 0 || keyLen <= 0) {
				return len - (foundPos - 1) + 1;
			}
		}
	}
	else {
		int64_t keyLen = 0;
		int64_t foundPos = startPos1;
		for (;;) {
			const int64_t lastOffset = foundPos - 1;

			reader1.toBegin();
			if (stepAllPart(reader1, lastOffset) > 0) {
				return 0;
			}

			const int64_t subPos = findPart(reader1, reader2, lastOffset);
			if (subPos <= 0) {
				return 0;
			}

			foundPos += subPos - 1;
			if (--repeat <= 0) {
				return foundPos;
			}

			if (keyLen <= 0) {
				reader2.toBegin();
				keyLen = partLength(reader2);
				if (keyLen <= 0) {
					return foundPos;
				}
			}
			foundPos += keyLen;
			reader2.toBegin();
		}
	}
}

template<typename R, bool Escape>
bool SQLValues::ValueUtils::matchPartLike(
		R &reader1, R &reader2, R *reader3) {
	assert(!Escape || reader3 != NULL);

	const util::CodePoint c3 = (Escape ? getPartLikeEscape(*reader3) : 0);

	size_t rest1 = reader1.getNext();
	size_t rest2 = reader2.getNext();
	bool allFound = false;
	bool escaping = false;
	for (;;) {
		for (;;) {
			util::CodePoint c2;
			if (!reader2.nextCode(&c2)) {
				if (!reader1.hasNext()) {
					return true;
				}
				break;
			}
			else if (!(Escape && escaping) && c2 == '%') {
				if (!reader2.hasNext()) {
					return true;
				}
				allFound = true;
				escaping = false;
				rest1 = reader1.getNext();
				rest2 = reader2.getNext();
			}
			else if (!(Escape && escaping) && c2 == c3) {
				if (!reader2.hasNext()) {
					return false;
				}
				escaping = true;
			}
			else {
				if (!(Escape && escaping) && c2 == '_') {
					if (!reader1.nextCode()) {
						break;
					}
				}
				else {
					util::CodePoint c1;
					if (!reader1.nextCode(&c1) ||
							toLower(c1) != toLower(c2)) {
						break;
					}
				}
				if (!allFound) {
					rest1 = reader1.getNext();
					rest2 = reader2.getNext();
				}
				escaping = false;
			}
		}

		if (!allFound || rest1 == 0) {
			return false;
		}

		reader1.back(rest1 - reader1.getNext());
		reader2.back(rest2 - reader2.getNext());

		reader1.nextCode();
		rest1 = reader1.getNext();
		escaping = false;
	}
}

template<typename W, typename R>
bool SQLValues::ValueUtils::unescapeExactPartLikePattern(
		W &writer, R &patternReader, R *escapeReader) {

	const util::CodePoint escape = (escapeReader == NULL ?
			std::numeric_limits<util::CodePoint>::max() :
			getPartLikeEscape(*escapeReader));

	bool escaping = false;
	for (;;) {
		util::CodePoint c;
		if (!patternReader.nextCode(&c)) {
			return !escaping;
		}
		else if (!escaping && (c == '%' || c == '_')) {
			return false;
		}
		else if (!escaping && c == escape) {
			escaping = true;
		}
		else {
			writer.appendCode(c);
			escaping = false;
		}
	}
}

inline bool SQLValues::ValueUtils::unescapeExactLikePattern(
		ValueContext &cxt, const TupleValue &pattern, const TupleValue *escape,
		TupleValue &unescaped) {
	StringBuilder builder(cxt);
	StringReader patternReader(getValue<TupleString>(pattern).getBuffer());

	bool done;
	if (escape == NULL) {
		StringReader *escapeReader = NULL;
		done = unescapeExactPartLikePattern(
				builder, patternReader, escapeReader);
	}
	else {
		StringReader escapeReader(getValue<TupleString>(*escape).getBuffer());
		done = unescapeExactPartLikePattern(
				builder, patternReader, &escapeReader);
	}

	unescaped = (done ? builder.build(cxt) : TupleValue());

	return done;
}

template<typename R>
util::CodePoint SQLValues::ValueUtils::getPartLikeEscape(
		R &escapeReader) {
	util::CodePoint escape;

	if (!escapeReader.nextCode(&escape) || escapeReader.hasNext()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INVALID_EXPRESSION_INPUT,
				"The number of escape characters must be one");
	}

	return escape;
}

template<typename W, typename R, typename Bounded>
inline void SQLValues::ValueUtils::subCodedPart(
		W &writer, R &reader, int64_t start, int64_t limit,
		bool limited, const Bounded&) {
	typedef typename R::CodeType CodeType;

	const bool forCodePoint = util::IsSame<CodeType, util::CodePoint>::VALUE;
	typedef typename util::BoolType<forCodePoint>::Result ForCodePoint;

	subCodedPart(
			writer, reader, start, limit, limited, Bounded(), ForCodePoint());
}

template<typename W, typename R, typename Bounded>
inline void SQLValues::ValueUtils::subCodedPart(
		W &writer, R &reader, int64_t start, int64_t limit,
		bool limited, const Bounded&, const util::TrueType&) {
	const std::pair<int64_t, int64_t> &bytes =
			ValueUtils::findSubPart(reader, start, limit, limited, Bounded());

	reader.toBegin();
	subPart(
			writer, reader, bytes.first, bytes.second, limited,
			Bounded());
}

template<typename W, typename R, typename Bounded>
inline void SQLValues::ValueUtils::subCodedPart(
		W &writer, R &reader, int64_t start, int64_t limit,
		bool limited, const Bounded&, const util::FalseType&) {
	subPart(writer, reader, start, limit, limited, Bounded());
}

template<typename R, typename Bounded>
inline std::pair<int64_t, int64_t> SQLValues::ValueUtils::findSubPart(
		R &reader, int64_t start, int64_t limit, bool limited,
		const Bounded&) {
	int64_t restLimit = limit;
	int64_t startBytes;
	if (start > 0) {
		const std::pair<int64_t, int64_t> &ret = findSubPart(reader, start - 1);
		startBytes = ret.first + 1;

		if (!Bounded::VALUE && restLimit < 0 && ret.second > 0) {
			restLimit += ret.second;
			restLimit = std::min<int64_t>(restLimit, 0);
		}
	}
	else if (start < 0) {
		reader.toEnd();
		const std::pair<int64_t, int64_t> &ret = findSubPart(reader, start);
		startBytes = ret.first;

		if (!Bounded::VALUE && restLimit > 0 && ret.second < 0 && limited) {
			restLimit += ret.second;
			restLimit = std::max<int64_t>(restLimit, 0);
		}
	}
	else {
		startBytes = 1;

		if (!Bounded::VALUE && restLimit > 0) {
			restLimit--;
		}
	}

	return std::make_pair(startBytes, findSubPart(reader, restLimit).first);
}

template<typename R>
inline std::pair<int64_t, int64_t> SQLValues::ValueUtils::findSubPart(
		R &reader, int64_t limit) {
	const size_t last = reader.getNext();
	int64_t rest = limit;
	if (rest >= 0) {
		for (; rest > 0; rest--) {
			if (!reader.nextCode()) {
				break;
			}
		}
	}
	else {
		for (; rest < 0; rest++) {
			util::CodePoint c;
			if (!reader.prevCode(&c)) {
				break;
			}
		}
	}

	const int64_t limitBytes = static_cast<int64_t>(last) - reader.getNext();
	return std::make_pair(limitBytes, rest);
}

template<typename W, typename R, typename Bounded>
inline void SQLValues::ValueUtils::subPart(
		W &writer, R &reader, int64_t start, int64_t limit, bool limited,
		const Bounded&) {
	uint64_t startSize = static_cast<uint64_t>(start);
	uint64_t limitSize = static_cast<uint64_t>(limit);

	if (!Bounded::VALUE && start == 0 && limit > 0) {
		limitSize--;
	}
	else if (start < 0 || limit < 0) {
		assert(reader.atBegin());
		const int64_t total = partLength(reader);
		reader.toBegin();

		if (start < 0) {
			const int64_t modStart = total + start;
			if (modStart > 0) {
				startSize = static_cast<uint64_t>(modStart) + 1;
			}
			else {
				if (!Bounded::VALUE && limit > 0 && limited) {
					const int64_t modLimit = modStart + limit;
					limitSize = static_cast<uint64_t>(
							std::max<int64_t>(modLimit, 0));
				}
				startSize = 1;
			}
		}

		if (limit < 0) {
			startSize = std::max<uint64_t>(startSize, 1) - 1;

			int64_t modLimit = limit;
			const int64_t modStart = static_cast<int64_t>(startSize) - total;
			if (modStart > 0) {
				if (!Bounded::VALUE) {
					modLimit = std::min<int64_t>(modLimit + modStart, 0);
				}
				startSize = static_cast<uint64_t>(total);
			}

			if (static_cast<int64_t>(startSize) + modLimit > 0) {
				limitSize = static_cast<uint64_t>(-modLimit);
				startSize = (startSize - limitSize);
			}
			else {
				limitSize = startSize;
				startSize = 0;
			}

			startSize++;
		}
	}

	if (startSize > 0) {
		util::FalseType falseWriter;
		subForwardPart(falseWriter, reader, startSize - 1);
	}

	subForwardPart(writer, reader, limitSize);
}

inline int32_t SQLValues::ValueUtils::orderPartNoCase(
		const TupleString::BufferInfo &buf1,
		const TupleString::BufferInfo &buf2) {
	StringReader reader1(buf1);
	StringReader reader2(buf2);
	return orderPartNoCase(reader1, reader2);
}

template<typename R1, typename R2>
inline int32_t SQLValues::ValueUtils::orderPart(
		R1 &reader1, R2 &reader2) {
	for (;;) {
		const uint64_t available1 = reader1.getNext();
		const uint64_t available2 = reader2.getNext();

		const size_t size = static_cast<size_t>(
				std::min<uint64_t>(available1, available2));
		if (size == 0) {
			return compareRawValue(available1, available2);
		}

		const int32_t order = memcmp(&reader1.get(), &reader2.get(), size);
		if (order != 0) {
			return order;
		}

		reader1.step(size);
		reader2.step(size);
	}
}

template<typename R1, typename R2>
inline int32_t SQLValues::ValueUtils::orderPartNoCase(
		R1 &reader1, R2 &reader2) {
	for (;;) {
		const uint64_t available1 = reader1.getNext();
		const uint64_t available2 = reader2.getNext();

		const size_t size = static_cast<size_t>(
				std::min<uint64_t>(available1, available2));
		if (size == 0) {
			return compareRawValue(available1, available2);
		}

		for (size_t i = 0;;) {
			const int32_t order =
					toLower((&reader1.get())[i]) - toLower((&reader2.get())[i]);
			if (order != 0) {
				return order;
			}
			if (++i >= size) {
				break;
			}
		}

		reader1.step(size);
		reader2.step(size);
	}
}

template<typename W, typename R>
inline void SQLValues::ValueUtils::subPart(
		W &writer, R &reader, int64_t limit) {
	uint64_t limitSize;
	if (limit < 0) {
		int64_t rest = limit;
		while (rest < 0 && !reader.atBegin()) {
			const size_t size = static_cast<size_t>(-std::max(
					-static_cast<int64_t>(reader.getPrev()), rest));

			reader.back(size);

			rest += size;
		}

		if (util::IsSame<W, util::FalseType>::VALUE) {
			return;
		}

		limitSize = static_cast<uint64_t>(rest - limit);
	}
	else {
		limitSize = static_cast<uint64_t>(limit);
	}

	subForwardPart(writer, reader, limitSize);
}

template<typename R>
inline void SQLValues::ValueUtils::subPart(R &reader, int64_t limit) {
	util::FalseType writer;
	subPart(writer, reader, limit);
}

template<typename W, typename R>
inline void SQLValues::ValueUtils::subForwardPart(
		W &writer, R &reader, uint64_t limit) {
	uint64_t rest = limit;
	while (rest > 0 && reader.hasNext()) {
		const size_t size = static_cast<size_t>(std::min<uint64_t>(
				reader.getNext(), rest));

		writePart(writer, &reader.get(), size);
		reader.step(size);

		rest -= size;
	}
}

template<typename W, typename R>
void SQLValues::ValueUtils::partToHexString(W &writer, R &reader) {
	for (; reader.hasNext(); reader.next()) {
		const uint32_t value = reader.get() & 0xff;
		char8_t result[2];
		{
			const uint32_t sub = value >> (CHAR_BIT / 2);
			result[0] = static_cast<char8_t>(
					sub < 0xa ? '0' + sub : 'A' + (sub - 0xa));
		}
		{
			const uint32_t sub = value & 0xf;
			result[1] = static_cast<char8_t>(
					sub < 0xa ? '0' + sub : 'A' + (sub - 0xa));
		}
		writePart(writer, result, sizeof(result));
	}
}

template<typename W, typename R>
void SQLValues::ValueUtils::hexStringToPart(W &writer, R &reader) {
	for (; reader.hasNext(); reader.next()) {
		const util::CodePoint highCh = toUpper(reader.get() & 0xff);

		reader.next();
		if (!reader.hasNext()) {
			if (CONFIG_STRICT_CONVERSION) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
						"Invalid BLOB format");
			}
			break;
		}

		const util::CodePoint lowCh = toUpper(reader.get() & 0xff);

		if ((!('0' <= highCh && highCh <= '9') &&
				!('A' <= highCh && highCh <= 'F')) ||
				(!('0' <= lowCh && lowCh <= '9') &&
				!('A' <= lowCh && lowCh <= 'F'))) {
			if (CONFIG_STRICT_CONVERSION) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
						"Invalid BLOB format");
			}
			continue;
		}

		const uint32_t high = highCh - (highCh <= '9' ? '0' : ('A' - 10));
		const uint32_t low = lowCh - (lowCh <= '9' ? '0' : ('A' - 10));

		const uint8_t result =
				static_cast<uint8_t>((high << (CHAR_BIT / 2)) | low);
		writePart(writer, &result, sizeof(result));
	}
}

template<typename W, typename T>
inline void SQLValues::ValueUtils::writePart(
		W &writer, const T *data, size_t size) {
	UTIL_STATIC_ASSERT((!util::IsSame<W, util::FalseType>::VALUE));
	writer.append(data, size);
}

template<typename T>
inline void SQLValues::ValueUtils::writePart(
		util::FalseType&, const T*, size_t) {
}

template<typename S, typename R>
inline void SQLValues::ValueUtils::partToString(S &str, R &reader) {
	while (reader.hasNext()) {
		const size_t available = reader.getNext();
		str.append(&reader.get(), available);
		reader.step(available);
	}
}

template<typename R>
inline TupleString::BufferInfo
SQLValues::ValueUtils::partToStringBuffer(R &reader) {
	const char8_t *strBuf = NULL;
	size_t size = 0;
	if (reader.hasNext()) {
		strBuf = &reader.get();
		size = reader.getNext();
	}
	return TupleString::BufferInfo(strBuf, size);
}

inline util::CodePoint SQLValues::ValueUtils::toLower(
		util::CodePoint c) {
	return 'A' <= c && c <= 'Z' ? c - 'A' + 'a' : c;
}

inline util::CodePoint SQLValues::ValueUtils::toUpper(
		util::CodePoint c) {
	return 'a' <= c && c <= 'z' ? c - 'a' + 'A' : c;
}

inline util::String SQLValues::ValueUtils::valueToString(
		ValueContext &cxt, const TupleValue &value) {
	assert(value.getType() == TupleTypes::TYPE_STRING);
	return util::String(
			static_cast<const char8_t*>(value.varData()),
			value.varSize(), cxt.getAllocator());
}


template<typename T>
inline typename T::ValueType SQLValues::ValueUtils::readCurrentValue(
		TupleListReader &reader, const TupleColumn &column) {
	typedef ReaderWrapper::WrapperOf<T> Wrapper;
	return Wrapper()(reader).template getAs<typename T::ValueType>(column);
}

template<>
inline TupleValue SQLValues::ValueUtils::readCurrentValue<SQLValues::Types::Any>(
		TupleListReader &reader, const TupleColumn &column) {
	static_cast<void>(reader);
	static_cast<void>(column);
	assert(column.getType() == TupleTypes::TYPE_ANY);
	return TupleValue();
}

inline bool SQLValues::ValueUtils::readCurrentNull(
		TupleListReader &reader, const TupleColumn &column) {
	return reader.checkIsNull(static_cast<uint16_t>(column.getPosition()));
}

template<typename T>
inline typename T::ValueType SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column) {
	typedef TupleWrapper::WrapperOf<T, true> Wrapper;
	return Wrapper()(tuple).template getAs<typename T::ValueType>(column);
}

template<>
inline TupleValue SQLValues::ValueUtils::readValue<SQLValues::Types::Any>(
		const ReadableTuple &tuple, const TupleColumn &column) {
	static_cast<void>(tuple);
	static_cast<void>(column);
	assert(column.getType() == TupleTypes::TYPE_ANY);
	return TupleValue();
}

inline bool SQLValues::ValueUtils::readNull(
		const ReadableTuple &tuple, const TupleColumn &column) {
	return tuple.checkIsNull(static_cast<uint16_t>(column.getPosition()));
}

template<typename T>
inline void SQLValues::ValueUtils::writeValue(
		WritableTuple &tuple, const TupleColumn &column,
		const typename T::LocalValueType &value) {
	typedef TupleWrapper::WrapperOf<T, false> Wrapper;
	return Wrapper()(tuple).setBy(column, value);
}


template<typename D>
SQLValues::ReadableTupleRef::WithDigester<D>::WithDigester(
		util::StackAllocator &alloc, TupleListReader &reader,
		const D &digester) :
		base_(ALLOC_NEW(alloc) RefBase(reader, digester)) {
}

template<typename D>
SQLValues::ReadableTupleRef::WithDigester<D>::WithDigester(
		const util::FalseType&) :
		base_(NULL) {
}

template<typename D>
inline bool SQLValues::ReadableTupleRef::WithDigester<D>::next() const {
	TupleListReader &reader = getReader();

	reader.next();
	if (!reader.exists()) {
		return false;
	}

	base_->digestTuple_ = DigestTuple(reader, base_->digester_);
	return true;
}

template<typename D>
template<typename T>
inline bool SQLValues::ReadableTupleRef::WithDigester<D>::nextAt() const {
	TupleListReader &reader = getReader();

	reader.next();
	if (!reader.exists()) {
		return false;
	}

	assert(base_->digester_.isOrdering());
	if (base_->digesterRotating_) {
		base_->digestTuple_ = DigestTuple(
				reader, typename DigesterOf<T>::RotType(base_->digester_));
	}
	else {
		base_->digestTuple_ = DigestTuple(
				reader, typename DigesterOf<T>::Type(base_->digester_));
	}

	return true;
}

template<typename D>
SQLValues::ReadableTupleRef::WithDigester<D>::RefBase::RefBase(
		TupleListReader &reader, const D &digester) :
		digestTuple_(reader, digester),
		digester_(digester),
		digesterRotating_(digester.getBase().isRotating()) {
}


template<typename D>
SQLValues::ArrayTupleRef::WithDigester<D>::WithDigester(RefBase<D> &base) :
		base_(&base) {
}

template<typename D>
inline bool SQLValues::ArrayTupleRef::WithDigester<D>::next() const {
	base_->reader_.next();
	if (!base_->reader_.exists()) {
		return false;
	}

	base_->update(base_->reader_.get());
	return true;
}

template<typename D>
SQLValues::ArrayTupleRef::RefBase<D>::RefBase(
		ValueContext &valueCxt, TupleListReader &reader,
		const D &digester, const ArrayTuple::Assigner &assigner) :
		digestTuple_(valueCxt.getAllocator()),
		valueCxt_(valueCxt),
		reader_(reader),
		digester_(digester),
		assigner_(assigner) {
	update(reader_.get());
}

template<typename D>
inline void SQLValues::ArrayTupleRef::RefBase<D>::update(
		const ReadableTuple &tuple) {
	assert(reader_.exists());

	ArrayTuple &destTuple = digestTuple_.getTuple();
	assigner_(valueCxt_, tuple, destTuple);
	digestTuple_.getDigest() = digester_(destTuple);
}

template<typename D>
SQLValues::ArrayTupleRef::RefList<D>::RefList(util::StackAllocator &alloc) :
		baseList_(alloc) {
}

template<typename D>
SQLValues::ArrayTupleRef::RefList<D>::~RefList() {
	util::StackAllocator *alloc = baseList_.get_allocator().base();
	while (!baseList_.empty()) {
		util::AllocUniquePtr<Base> base(baseList_.back(), *alloc);
		baseList_.pop_back();
	}
}

template<typename D>
typename SQLValues::ArrayTupleRef::RefList<D>::RefType
SQLValues::ArrayTupleRef::RefList<D>::add(
		ValueContext &valueCxt, TupleListReader &reader,
		const D &digester, const ArrayTuple::Assigner &assigner) {
	util::StackAllocator *alloc = baseList_.get_allocator().base();

	util::AllocUniquePtr<Base> base(ALLOC_UNIQUE(
			*alloc, Base, valueCxt, reader, digester, assigner));
	baseList_.push_back(base.get());

	return RefType(*base.release());
}


inline SQLValues::SharedId::SharedId() :
		index_(0),
		id_(0),
		manager_(NULL) {
}

inline SQLValues::SharedId::SharedId(const SharedId &another) :
		index_(0),
		id_(0),
		manager_(NULL) {
	*this = another;
}

inline SQLValues::SharedId& SQLValues::SharedId::operator=(
		const SharedId &another) {
	assign(another.manager_, another.index_, another.id_);
	return *this;
}

inline SQLValues::SharedId::~SharedId() {
	try {
		clear();
	}
	catch (...) {
		assert(false);
	}
}

inline void SQLValues::SharedId::clear() {
	SharedIdManager *orgManager = manager_;
	if (orgManager == NULL) {
		return;
	}

	const size_t orgIndex = index_;
	const uint64_t orgId = id_;

	index_ = 0;
	id_ = 0;
	manager_ = NULL;

	orgManager->release(orgIndex, orgId, this);
}

inline bool SQLValues::SharedId::isEmpty() const {
	return (manager_ == NULL);
}

inline bool SQLValues::SharedId::operator==(const SharedId &another) const {
	if (manager_ != another.manager_) {
		assert(false);
		return false;
	}
	return (index_ == another.index_);
}

inline bool SQLValues::SharedId::operator<(const SharedId &another) const {
	if (manager_ != another.manager_) {
		assert(false);
		return false;
	}
	return (index_ < another.index_);
}

inline size_t SQLValues::SharedId::getIndex(
		const SharedIdManager &manager) const {
	if (manager_ != &manager) {
		return errorIndex();
	}
	return index_;
}

inline void SQLValues::SharedId::assign(
		SharedIdManager *manager, size_t orgIndex, uint64_t orgId) {
	clear();

	if (manager == NULL) {
		return;
	}

	uint64_t nextId;
	const size_t nextIndex = manager->allocate(orgIndex, orgId, this, &nextId);

	index_ = nextIndex;
	id_ = nextId;
	manager_ = manager;
}


inline size_t SQLValues::SharedIdManager::allocate(
		size_t orgIndex, uint64_t orgId, void *key, uint64_t *nextId) {
	size_t index;
	Entry *entry;
	if (orgId == 0) {
		entry = &createEntry(&index);
	}
	else {
		index = orgIndex;
		entry = &getEntry(index, orgId);
	}

	checkKey(true, entry->id_, key);
	++entry->refCount_;
	*nextId = entry->id_;
	return index;
}

inline void SQLValues::SharedIdManager::release(
		size_t index, uint64_t id, void *key) {
	do {
		Entry &entry = getEntry(index, id);
		if (entry.refCount_ <= 0) {
			break;
		}
		--entry.refCount_;
		checkKey(false, id, key);

		if (entry.refCount_ <= 0) {
			removeEntry(entry, index);
		}
		return;
	}
	while (false);
	errorRefCount();
}

inline void SQLValues::SharedIdManager::checkKey(
		bool allocating, uint64_t id, void *key) {
	static_cast<void>(allocating);
	static_cast<void>(id);
	static_cast<void>(key);
}

inline SQLValues::SharedIdManager::Entry& SQLValues::SharedIdManager::getEntry(
		size_t index, uint64_t id) {
	do {
		if (index >= entryList_.size()) {
			break;
		}
		Entry &entry = entryList_[index];

		if (id != 0 && entry.id_ != id) {
			break;
		}

		return entry;
	}
	while (false);
	return errorEntry();
}

inline SQLValues::SharedIdManager::Entry::Entry() :
		id_(0),
		refCount_(0) {
}

#endif
