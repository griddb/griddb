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

typedef TupleList::TupleColumnType TupleColumnType;

struct TupleTypes {
	enum {
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
		TYPE_BOOL = TupleList::TYPE_BOOL,
		TYPE_STRING = TupleList::TYPE_STRING,
		TYPE_GEOMETRY = TupleList::TYPE_GEOMETRY,
		TYPE_BLOB = TupleList::TYPE_BLOB,

		TYPE_NULL = TupleList::TYPE_NULL,
		TYPE_ANY = TupleList::TYPE_ANY,
		TYPE_NUMERIC = TupleList::TYPE_NUMERIC
	};
};

struct SQLValues {
	typedef util::VariableSizeAllocator<> VarAllocator;
	typedef TupleValue::VarContext VarContext;

	typedef TupleList::Reader TupleListReader;

	typedef TupleList::ReadableTuple ReadableTuple;
	typedef TupleList::WritableTuple WritableTuple;

	typedef TupleList::Column TupleColumn;

	struct Types;
	class TypeSwitcher;

	class ValueContext;
	class ExtValueContext;
	class CompColumn;
	typedef util::Vector<CompColumn> CompColumnList;

	class ArrayTuple;

	class ValueBasicComparator;
	class ValuePromotableComparator;
	class ValueComparator;
	class ValueEq;
	class ValueLess;
	class ValueGreater;
	class ValueFnv1aHasher;
	class ValueConverter;
	class ValuePromoter;
	class ValueWriter;

	class TupleComparator;
	class TupleRangeComparator;
	class TupleEq;
	class TupleLess;
	class TupleGreater;
	class TupleFnv1aHasher;
	class TupleConverter;
	class TuplePromoter;

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

	struct TypeUtils;
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
	template<TupleColumnType T, typename V>
	struct Base {
		enum { COLUMN_TYPE = T };
		typedef V ValueType;
	};

public:
	struct Byte : public Base<TupleTypes::TYPE_BYTE, int8_t> {};
	struct Short : public Base<TupleTypes::TYPE_SHORT, int16_t> {};
	struct Integer : public Base<TupleTypes::TYPE_INTEGER, int32_t> {};
	struct Long : public Base<TupleTypes::TYPE_LONG, int64_t> {};
	struct Float : public Base<TupleTypes::TYPE_FLOAT, float> {};
	struct Double : public Base<TupleTypes::TYPE_DOUBLE, double> {};
	struct Timestamp : public Base<TupleTypes::TYPE_TIMESTAMP, int64_t> {};
	struct Bool : public Base<TupleTypes::TYPE_BOOL, bool> {};
	struct String : public Base<TupleTypes::TYPE_STRING, TupleString> {};
	struct Blob : public Base<TupleTypes::TYPE_BLOB, TupleValue> {};
	struct Any : public Base<TupleTypes::TYPE_ANY, TupleValue> {};
};

class SQLValues::TypeSwitcher {
public:
	explicit TypeSwitcher(TupleColumnType type) : type_(type) {}

	template<typename Op>
	typename Op::RetType operator()(Op &op) const;

	template<typename Op, typename A1>
	typename Op::RetType operator()(Op &op, A1 &arg1) const {
		OpWithArgs<Op, A1> opWithArgs(op, arg1);
		return (*this)(opWithArgs);
	}

	template<typename Op, typename A1, typename A2>
	typename Op::RetType operator()(
			Op &op, A1 &arg1, A2 &arg2) const {
		OpWithArgs<Op, A1, A2> opWithArgs(op, arg1, arg2);
		return (*this)(opWithArgs);
	}

	template<typename Op, typename A1, typename A2, typename A3>
	typename Op::RetType operator()(
			Op &op, A1 &arg1, A2 &arg2, A3 &arg3) const {
		OpWithArgs<Op, A1, A2, A3> opWithArgs(op, arg1, arg2, arg3);
		return (*this)(opWithArgs);
	}

private:
	template<typename Op, typename A1, typename A2 = void, typename A3 = void>
	struct OpWithArgs {
		typedef typename Op::RetType RetType;
		template<typename T> struct TypeAt {
			explicit TypeAt(const OpWithArgs &base) : base_(base) {}
			RetType operator()() const {
				typedef typename Op::template TypeAt<T> BaseType;
				return BaseType(base_.op_)(base_.arg1_, base_.arg2_, base_.arg3_);
			}
			const OpWithArgs &base_;
		};
		OpWithArgs(
				Op &op, A1 &arg1, A2 &arg2, A3 &arg3) :
				op_(op),
				arg1_(arg1),
				arg2_(arg2),
				arg3_(arg3) {
		}
		Op &op_;
		A1 &arg1_;
		A2 &arg2_;
		A3 &arg3_;
	};

	template<typename Op, typename A1, typename A2>
	struct OpWithArgs<Op, A1, A2, void> {
		typedef typename Op::RetType RetType;
		template<typename T> struct TypeAt {
			explicit TypeAt(const OpWithArgs &base) : base_(base) {}
			RetType operator()() const {
				typedef typename Op::template TypeAt<T> BaseType;
				return BaseType(base_.op_)(base_.arg1_, base_.arg2_);
			}
			const OpWithArgs &base_;
		};
		OpWithArgs(Op &op, A1 &arg1, A2 &arg2) :
				op_(op),
				arg1_(arg1),
				arg2_(arg2) {
		}
		Op &op_;
		A1 &arg1_;
		A2 &arg2_;
	};

	template<typename Op, typename A1>
	struct OpWithArgs<Op, A1, void, void> {
		typedef typename Op::RetType RetType;
		template<typename T> struct TypeAt {
			explicit TypeAt(const OpWithArgs &base) : base_(base) {}
			RetType operator()() const {
				typedef typename Op::template TypeAt<T> BaseType;
				return BaseType(base_.op_)(base_.arg1_);
			}
			const OpWithArgs &base_;
		};
		OpWithArgs(Op &op, A1 &arg1) :
				op_(op),
				arg1_(arg1) {
		}
		Op &op_;
		A1 &arg1_;
	};

	TupleColumnType type_;
};

class SQLValues::ValueContext {
public:
	class Source;

	explicit ValueContext(const Source &source);

	static Source ofAllocator(util::StackAllocator &alloc);
	static Source ofVarContext(
			VarContext &varCxt, util::StackAllocator *alloc = NULL);

	util::StackAllocator& getAllocator();
	VarAllocator& getVarAllocator();

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

	util::StackAllocator *alloc_;
	VarContext *varCxt_;
	ExtValueContext *extCxt_;

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

class SQLValues::CompColumn {
public:
	CompColumn();

	const TupleColumn& getTupleColumn1() const;
	const TupleColumn& getTupleColumn2() const;

	void setTupleColumn1(const TupleColumn &column);
	void setTupleColumn2(const TupleColumn &column);

	void setTupleColumn(const TupleColumn &column, bool first);

	bool isColumnPosAssigned(bool first) const;
	uint32_t getColumnPos(bool first) const;
	void setColumnPos(uint32_t columnPos, bool first);

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

private:
	TupleColumn tupleColumn1_;
	TupleColumn tupleColumn2_;

	TupleColumnType type_;

	uint32_t columnPos1_;
	uint32_t columnPos2_;

	bool ascending_;

	bool eitherUnbounded_;
	bool lowerUnbounded_;

	bool eitherExclusive_;
	bool lowerExclusive_;
};

class SQLValues::ArrayTuple {
public:
	explicit ArrayTuple(util::StackAllocator &alloc);
	~ArrayTuple();

	bool isEmpty();
	void clear();

	void assign(
			ValueContext &cxt, const util::Vector<TupleColumnType> &typeList);

	void assign(ValueContext &cxt, const ArrayTuple &tuple);
	void assign(
			ValueContext &cxt, const ArrayTuple &tuple,
			const CompColumnList &columnList);
	void assign(
			ValueContext &cxt, ReadableTuple &tuple,
			const CompColumnList &columnList);
	void assign(
			ValueContext &cxt, ReadableTuple &tuple,
			const util::Vector<TupleColumn> &columnList);

	void swap(ArrayTuple &another);

	const TupleValue& get(size_t index) const;
	void set(size_t index, ValueContext &cxt, const TupleValue &value);

private:
	ArrayTuple(const ArrayTuple&);
	ArrayTuple& operator=(const ArrayTuple&);

	void resize(size_t size);

	util::Vector<TupleValue> valueList_;
	VarContext *varCxt_;
};


class SQLValues::ValueBasicComparator {
public:
	typedef int32_t RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const ValueBasicComparator &base) : base_(base) {}

		RetType operator()(
				const typename T::ValueType &v1,
				const typename T::ValueType &v2) const;

	private:
		const ValueBasicComparator &base_;
	};

	explicit ValueBasicComparator(bool sensitive) : sensitive_(sensitive) {}

private:
	bool sensitive_;
};

template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Byte>::operator()(
		const int8_t &v1, const int8_t &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Short>::operator()(
		const int16_t &v1, const int16_t &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Integer>::operator()(
		const int32_t &v1, const int32_t &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Long>::operator()(
		const int64_t &v1, const int64_t &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Float>::operator()(
		const float &v1, const float &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Double>::operator()(
		const double &v1, const double &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Timestamp>::operator()(
		const int64_t &v1, const int64_t &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Bool>::operator()(
		const bool &v1, const bool &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::String>::operator()(
		const TupleString &v1, const TupleString &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Blob>::operator()(
		const TupleValue &v1, const TupleValue &v2) const;
template<>
int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Any>::operator()(
		const TupleValue &v1, const TupleValue &v2) const;

class SQLValues::ValuePromotableComparator {
public:
	typedef int32_t RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const ValuePromotableComparator &base) : base_(base) {}
		RetType operator()(const TupleValue &v1, const TupleValue &v2) const;

	private:
		const ValuePromotableComparator &base_;
	};

	explicit ValuePromotableComparator(bool sensitive) : base_(sensitive) {}

private:
	ValueBasicComparator base_;
};

class SQLValues::ValueComparator {
public:
	ValueComparator(TupleColumnType type, bool sensitive);
	int32_t operator()(const TupleValue &v1, const TupleValue &v2) const;

private:
	TupleColumnType type_;
	ValuePromotableComparator base_;
};

class SQLValues::ValueEq {
public:
	explicit ValueEq(TupleColumnType type);
	bool operator()(const TupleValue &v1, const TupleValue &v2) const;

private:
	ValueComparator comp_;
};

class SQLValues::ValueLess {
public:
	explicit ValueLess(TupleColumnType type);
	bool operator()(const TupleValue &v1, const TupleValue &v2) const;

private:
	ValueComparator comp_;
};

class SQLValues::ValueGreater {
public:
	explicit ValueGreater(TupleColumnType type);
	bool operator()(const TupleValue &v1, const TupleValue &v2) const;

private:
	ValueComparator comp_;
};

class SQLValues::ValueFnv1aHasher {
public:
	explicit ValueFnv1aHasher(TupleColumnType type);
	ValueFnv1aHasher(TupleColumnType type, uint32_t seed);

	uint32_t operator()(const TupleValue &value) const;

private:
	TupleColumnType type_;
	uint32_t seed_;
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
	TupleValue operator()(const TupleValue &src) const;

private:
	TupleColumnType type_;
};

class SQLValues::ValueWriter {
public:
	typedef void RetType;

	template<typename T>
	class TypeAt {
	public:
		explicit TypeAt(const ValueWriter &base) : base_(base) {}

		void operator()(const ReadableTuple &src, WritableTuple &dest) const;

	private:
		const ValueWriter &base_;
	};

	ValueWriter(const TupleColumn &srcColumn, const TupleColumn &destColumn);

	void operator()(const ReadableTuple &src, WritableTuple &dest) const;

private:
	TupleColumnType type_;
	TupleColumn srcColumn_;
	TupleColumn destColumn_;
};


class SQLValues::TupleComparator {
public:
	typedef int32_t RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const TupleComparator &base) : base_(base) {}

		template<typename T1, typename T2>
		int32_t operator()(const T1 &t1, const T2 &t2) const;

	private:
		const TupleComparator &base_;
	};

	TupleComparator(const CompColumnList &columnList, bool sensitive);

	template<typename T1, typename T2>
	int32_t operator()(const T1 &t1, const T2 &t2) const;

	template<typename T, typename T1, typename T2>
	static int32_t compareElement(
			const CompColumn &column, uint32_t ordinal, bool sensitive,
			bool forKeyOnlyArray, const T1 &t1, const T2 &t2);

	void setForKeyOnlyArray(bool forKeyOnlyArray);

	TypeSwitcher getTypeSwitcher() const;

private:
	template<typename T>
	static typename T::ValueType getElement(
			const CompColumn &column, uint32_t, const ReadableTuple &t,
			util::TrueType*, bool, bool &nullValue);
	template<typename T>
	static typename T::ValueType getElement(
			const CompColumn &column, uint32_t, const ReadableTuple &t,
			util::FalseType*, bool, bool &nullValue);

	template<typename T>
	static typename T::ValueType getElement(
			const CompColumn&, uint32_t ordinal, const ArrayTuple &t,
			util::TrueType*, bool forKeyOnlyArray, bool &nullValue);
	template<typename T>
	static typename T::ValueType getElement(
			const CompColumn&, uint32_t ordinal, const ArrayTuple &t,
			util::FalseType*, bool forKeyOnlyArray, bool &nullValue);

	static bool isPromotable(const CompColumnList &columnList);

	const CompColumnList &columnList_;
	bool promotable_;
	bool sensitive_;
	bool forKeyOnlyArray_;
};

class SQLValues::TupleRangeComparator {
public:
	typedef int32_t RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const TupleRangeComparator &base) : base_(base) {}

		int32_t operator()(
				const ReadableTuple &t1, const ReadableTuple &t2) const;

	private:
		const TupleRangeComparator &base_;
	};

	explicit TupleRangeComparator(const CompColumnList &columnList);
	int32_t operator()(const ReadableTuple &t1, const ReadableTuple &t2) const;

private:
	const TupleComparator base_;
	const CompColumnList &columnList_;
};

class SQLValues::TupleEq {
public:
	explicit TupleEq(const CompColumnList &columnList);

	template<typename T1, typename T2>
	bool operator()(const T1 &t1, const T2 &t2) const {
		return comp_(t1, t2) == 0;
	}

	TupleComparator& getComparator() { return comp_; }

private:
	TupleComparator comp_;
};

class SQLValues::TupleLess {
public:
	typedef bool RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const TupleLess &base) : base_(base) {}

		template<typename T1, typename T2>
		bool operator()(const T1 &t1, const T2 &t2) const {
			return TupleComparator::TypeAt<T>(base_.comp_)(t1, t2) < 0;
		}

	private:
		const TupleLess &base_;
	};

	explicit TupleLess(const CompColumnList &columnList);

	template<typename T1, typename T2>
	bool operator()(const T1 &t1, const T2 &t2) const {
		return getTypeSwitcher()(*this, t1, t2);
	}

	TupleComparator& getComparator() { return comp_; }

	TypeSwitcher getTypeSwitcher() const { return comp_.getTypeSwitcher(); }

private:
	TupleComparator comp_;
};

class SQLValues::TupleGreater {
public:
	typedef bool RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(const TupleGreater &base) : base_(base) {}

		template<typename T1, typename T2>
		bool operator()(const T1 &t1, const T2 &t2) const {
			return TupleComparator::TypeAt<T>(base_.comp_)(t1, t2) > 0;
		}

	private:
		const TupleGreater &base_;
	};

	explicit TupleGreater(const CompColumnList &columnList);

	template<typename T1, typename T2>
	bool operator()(const T1 &t1, const T2 &t2) const {
		return getTypeSwitcher()(*this, t1, t2);
	}

	TupleComparator& getComparator() { return comp_; }

	TypeSwitcher getTypeSwitcher() const { return comp_.getTypeSwitcher(); }

private:
	TupleComparator comp_;
};

class SQLValues::TupleFnv1aHasher {
public:
	TupleFnv1aHasher(const CompColumnList &columnList);
	TupleFnv1aHasher(const CompColumnList &columnList, uint32_t seed);
	uint32_t operator()(const ReadableTuple &tuple) const;

private:
	const CompColumnList &columnList_;
	uint32_t seed_;
};

class SQLValues::TupleConverter {
public:
	TupleConverter(
			const CompColumnList &srcColumnList,
			const CompColumnList &destColumnList);
	void operator()(
			ValueContext &cxt, const ReadableTuple &src,
			WritableTuple &dest) const;

private:
	const CompColumnList &srcColumnList_;
	const CompColumnList &destColumnList_;
};

class SQLValues::TuplePromoter {
public:
	TuplePromoter(
			const CompColumnList &srcColumnList,
			const CompColumnList &destColumnList);
	void operator()(
			const ReadableTuple &src, WritableTuple &dest) const;

private:
	const CompColumnList &srcColumnList_;
	const CompColumnList &destColumnList_;
};

class SQLValues::StringBuilder {
public:
	explicit StringBuilder(ValueContext &cxt, size_t capacity = 0);
	explicit StringBuilder(const util::StdAllocator<void, void> &alloc);

	void setLimit(size_t limit);

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
	template<typename T> struct ValueTraits;


	static bool isAny(TupleColumnType type);
	static bool isNull(TupleColumnType type);

	static bool isFixed(TupleColumnType type);
	static bool isArray(TupleColumnType type);

	static bool isIntegral(TupleColumnType type);
	static bool isFloating(TupleColumnType type);

	static bool isDeclarable(TupleColumnType type);
	static bool isSupported(TupleColumnType type);

	static bool isNullable(TupleColumnType type);
	static TupleColumnType setNullable(TupleColumnType type, bool nullable);

	static TupleColumnType toNonNullable(TupleColumnType type);
	static TupleColumnType toNullable(TupleColumnType type);

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

	static void setUpTupleInfo(
			TupleList::Info &info, const TupleColumnType *list, size_t count);

	static const char8_t* toString(
			TupleColumnType type, bool nullOnUnknown = false);

private:
	typedef TupleColumnTypeUtils BaseUtils;
};

template<TupleColumnType T>
struct SQLValues::TypeUtils::Traits {


	typedef
			typename util::Conditional<T == TupleTypes::TYPE_BOOL, bool,
			typename util::Conditional<T == TupleTypes::TYPE_BYTE, int8_t,
			typename util::Conditional<T == TupleTypes::TYPE_SHORT, int16_t,
			typename util::Conditional<T == TupleTypes::TYPE_INTEGER, int32_t,
			typename util::Conditional<T == TupleTypes::TYPE_LONG, int64_t,
			typename util::Conditional<T == TupleTypes::TYPE_FLOAT, float,
			typename util::Conditional<T == TupleTypes::TYPE_DOUBLE, double,
			typename util::Conditional<T == TupleTypes::TYPE_TIMESTAMP, int64_t,
			typename util::Conditional<T == TupleTypes::TYPE_STRING, TupleString,
					TupleValue
			>::Type>::Type>::Type>::Type>::Type
			>::Type>::Type>::Type>::Type
					ValueType;

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
			ValueType>::Type ReadableSourceType;


	static const bool SIZE_FIXED =
			((T & TupleTypes::TYPE_MASK_FIXED_SIZE) != 0);

	static const bool READER_AVAILABLE =
			!util::IsSame<ReaderType, void>::VALUE;
	static const bool WRITER_AVAILABLE =
			!util::IsSame<WriterType, void>::VALUE;

	typedef typename util::Conditional<
			READER_AVAILABLE, ReaderType, ValueType>::Type ReadableType;
	typedef typename util::Conditional<
			WRITER_AVAILABLE, WriterType, ValueType>::Type WritableType;

	typedef typename util::Conditional<
			READER_AVAILABLE,
			ReadableType&, const ValueType&>::Type ReadableRefType;
	typedef typename util::Conditional<
			WRITER_AVAILABLE,
			WritableType&, const ValueType&>::Type WritableRefType;

	typedef typename util::Conditional<
			READER_AVAILABLE,
			ReadableType*, const ValueType*>::Type ReadablePtrType;
};

template<typename T>
struct SQLValues::TypeUtils::ValueTraits {
	static const TupleColumnType COLUMN_TYPE_AS_NUMERIC =
			util::IsSame<T, typename Traits<
					TupleTypes::TYPE_BYTE>::ValueType>::VALUE ?
					TupleTypes::TYPE_BYTE :
			util::IsSame<T, typename Traits<
					TupleTypes::TYPE_SHORT>::ValueType>::VALUE ?
					TupleTypes::TYPE_SHORT :
			util::IsSame<T, typename Traits<
					TupleTypes::TYPE_INTEGER>::ValueType>::VALUE ?
					TupleTypes::TYPE_INTEGER :
			util::IsSame<T, typename Traits<
					TupleTypes::TYPE_LONG>::ValueType>::VALUE ?
					TupleTypes::TYPE_LONG :
			util::IsSame<T, typename Traits<
					TupleTypes::TYPE_FLOAT>::ValueType>::VALUE ?
					TupleTypes::TYPE_FLOAT :
			util::IsSame<T, typename Traits<
					TupleTypes::TYPE_DOUBLE>::ValueType>::VALUE ?
					TupleTypes::TYPE_DOUBLE :
			TupleTypes::TYPE_NULL;
};

struct SQLValues::ValueUtils {
	class BytesHasher;

	enum {
		CONFIG_NAN_AS_NULL = false,
		CONFIG_STRICT_CONVERSION = true
	};


	static int32_t compareIntegral(int64_t v1, int64_t v2);
	static int32_t compareFloating(double v1, double v2, bool sensitive);
	template<typename R> static int32_t compareSequence(R &v1, R &v2);

	template<typename T>
	static int32_t compareRawValue(const T &v1, const T &v2);

	static int32_t compareNull(bool nullValue1, bool nullValue2);


	static uint32_t fnv1aHashInit();
	static uint32_t fnv1aHashIntegral(uint32_t base, int64_t value);
	static uint32_t fnv1aHashFloating(uint32_t base, double value);
	template<typename R> static uint32_t fnv1aHashSequence(
			uint32_t base, R &value);
	static uint32_t fnv1aHashBytes(
			uint32_t base, const void *value, size_t size);


	template<TupleColumnType T>
	static typename TypeUtils::Traits<T>::ValueType promoteValue(
			const TupleValue &src);

	static int64_t toLong(const TupleValue &src);
	static double toDouble(const TupleValue &src);
	template<typename T> static T toIntegral(const TupleValue &src);
	template<typename T> static T toFloating(const TupleValue &src);
	static int64_t toTimestamp(ValueContext &cxt, const TupleValue &src);
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
			StringBuilder &builder, const util::DateTime &value,
			const util::TimeZone &zone);
	static void formatBool(StringBuilder &builder, bool value);
	static void formatBlob(StringBuilder &builder, const TupleValue &value);


	static bool parseLong(
			const TupleString::BufferInfo &src, int64_t &dest, bool strict);
	static bool parseDouble(
			const TupleString::BufferInfo &src, double &dest, bool strict);
	static int64_t parseTimestamp(
			const TupleString::BufferInfo &src, const util::TimeZone &zone,
			bool zoneSpecified, util::StdAllocator<void, void> alloc);
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
	static TupleValue toAnyByTimestamp(int64_t src);
	static TupleValue toAnyByBool(bool src);

	static TupleValue createEmptyValue(
			ValueContext &cxt, TupleColumnType type);


	template<typename T> static T getValue(const TupleValue &src);

	template<TupleColumnType T>
	static typename TypeUtils::Traits<T>::ReadableSourceType toReadableSource(
			const TupleValue &src);

	static TupleString::BufferInfo toStringBuffer(const char8_t *src);
	static TupleString::BufferInfo toStringBuffer(const TupleString &src);

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


	static bool isNull(const TupleValue &value);

	static TupleColumnType toColumnType(const TupleValue &value);

	static TupleColumnType toCompType(
			const TupleValue &v1, const TupleValue &v2);


	static int64_t checkTimestamp(int64_t tsValue);

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

	static void moveValueToRoot(
			ValueContext &cxt, TupleValue &value,
			size_t currentDepth, size_t targetDepth);


	template<typename T> static T readValue(
			const ReadableTuple &tuple, const TupleColumn &column);
	static bool readNull(
			const ReadableTuple &tuple, const TupleColumn &column);

	template<TupleColumnType T> static void writeValue(
			WritableTuple &tuple, const TupleColumn &column,
			const typename TypeUtils::Traits<T>::ValueType &value);

private:
	struct Constants;
	struct TimeErrorHandler;

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

template<> TupleValue SQLValues::ValueUtils::toAnyByValue<
		TupleTypes::TYPE_TIMESTAMP>(const int64_t &src);

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
TupleString::BufferInfo SQLValues::ValueUtils::toReadableSource<
		TupleTypes::TYPE_STRING>(const TupleValue &src);

#if GD_ENABLE_SQL_READABLE_TUPLE_TYPE_SPECIFIED_GET
template<>
int32_t SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column);
template<>
int64_t SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column);
template<>
TupleString SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column);
#endif 

#if GD_ENABLE_SQL_WRITABLE_TUPLE_TYPE_SPECIFIED_SET
template<>
void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_INTEGER>(
		WritableTuple &tuple, const TupleColumn &column,
		const int32_t &value);
template<>
void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_LONG>(
		WritableTuple &tuple, const TupleColumn &column,
		const int64_t &value);
template<>
void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_TIMESTAMP>(
		WritableTuple &tuple, const TupleColumn &column,
		const int64_t &value);
template<>
void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_STRING>(
		WritableTuple &tuple, const TupleColumn &column,
		const TupleString &value);
#endif 

struct SQLValues::ValueUtils::Constants {
	static const char8_t CONSTANT_TRUE_STR[];
	static const char8_t CONSTANT_FALSE_STR[];

	static const double INT64_MIN_AS_DOUBLE;
	static const double INT64_MAX_AS_DOUBLE;

	static const util::DateTime::FieldData TIMESTAMP_MAX_FIELDS;
	static const int64_t TIMESTAMP_MAX_UNIX_TIME;
};

struct SQLValues::ValueUtils::TimeErrorHandler {
	void errorTimeFormat(std::exception &e) const;
};


class SQLValues::ReadableTupleRef {
public:
	explicit ReadableTupleRef(TupleListReader &reader);
	TupleListReader& getReader() const;
	operator ReadableTuple() const;

private:
	TupleListReader *reader_;
};

class SQLValues::ArrayTupleRef {
public:
	explicit ArrayTupleRef(ArrayTuple &tuple);

	ArrayTuple& getTuple() const;
	operator ArrayTuple&() const;

private:
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

	size_t allocate(size_t orgIndex, uint64_t orgId, void *key);
	void release(size_t index, uint64_t id, void *key);

	void checkKey(bool allocating, uint64_t id, void *key);

	uint64_t getId(size_t index);
	Entry& getEntry(size_t index, uint64_t id);

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
	virtual void unlatch() throw() = 0;
	virtual void close() throw() = 0;

private:
};



template<typename Op>
inline typename Op::RetType
SQLValues::TypeSwitcher::operator()(Op &op) const {
	switch (TypeUtils::toNonNullable(type_)) {
	case TupleTypes::TYPE_BYTE:
		return typename Op::template TypeAt<Types::Byte>(op)();
	case TupleTypes::TYPE_SHORT:
		return typename Op::template TypeAt<Types::Short>(op)();
	case TupleTypes::TYPE_INTEGER:
		return typename Op::template TypeAt<Types::Integer>(op)();
	case TupleTypes::TYPE_LONG:
		return typename Op::template TypeAt<Types::Long>(op)();
	case TupleTypes::TYPE_FLOAT:
		return typename Op::template TypeAt<Types::Float>(op)();
	case TupleTypes::TYPE_DOUBLE:
		return typename Op::template TypeAt<Types::Double>(op)();
	case TupleTypes::TYPE_TIMESTAMP:
		return typename Op::template TypeAt<Types::Timestamp>(op)();
	case TupleTypes::TYPE_BOOL:
		return typename Op::template TypeAt<Types::Bool>(op)();
	case TupleTypes::TYPE_STRING:
		return typename Op::template TypeAt<Types::String>(op)();
	case TupleTypes::TYPE_BLOB:
		return typename Op::template TypeAt<Types::Blob>(op)();
	default:
		assert(type_ == TupleTypes::TYPE_ANY);
		return typename Op::template TypeAt<Types::Any>(op)();
	}
}


template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Byte>::operator()(
		const int8_t &v1, const int8_t &v2) const {
	return ValueUtils::compareIntegral(v1, v2);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Short>::operator()(
		const int16_t &v1, const int16_t &v2) const {
	return ValueUtils::compareIntegral(v1, v2);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Integer>::operator()(
		const int32_t &v1, const int32_t &v2) const {
	return ValueUtils::compareIntegral(v1, v2);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Long>::operator()(
		const int64_t &v1, const int64_t &v2) const {
	return ValueUtils::compareIntegral(v1, v2);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Float>::operator()(
		const float &v1, const float &v2) const {
	return ValueUtils::compareFloating(v1, v2, base_.sensitive_);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Double>::operator()(
		const double &v1, const double &v2) const {
	return ValueUtils::compareFloating(v1, v2, base_.sensitive_);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Timestamp>::operator()(
		const int64_t &v1, const int64_t &v2) const {
	return ValueUtils::compareIntegral(v1, v2);
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Bool>::operator()(
		const bool &v1, const bool &v2) const {
	return ValueUtils::compareIntegral((v1 ? 1 : 0), (v2 ? 1 : 0));
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::String>::operator()(
		const TupleString &v1, const TupleString &v2) const {
	return ValueUtils::compareSequence(
			*ValueUtils::toStringReader(v1), *ValueUtils::toStringReader(v2));
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Blob>::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	return ValueUtils::compareSequence(
			*ValueUtils::toLobReader(v1), *ValueUtils::toLobReader(v2));
}

template<>
inline int32_t SQLValues::ValueBasicComparator::TypeAt<
		SQLValues::Types::Any>::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	assert(ValueUtils::isNull(v1));
	assert(ValueUtils::isNull(v2));
	static_cast<void>(v1);
	static_cast<void>(v2);
	return 0;
}


template<typename T>
inline int32_t SQLValues::ValuePromotableComparator::TypeAt<T>::operator()(
		const TupleValue &v1, const TupleValue &v2) const {
	const bool null1 = ValueUtils::isNull(v1);
	const bool null2 = ValueUtils::isNull(v2);
	if (null1 || null2) {
		return ValueUtils::compareNull(null1, null2);
	}

	return ValueBasicComparator::TypeAt<T>(base_.base_)(
			ValueUtils::promoteValue<T::COLUMN_TYPE>(v1),
			ValueUtils::promoteValue<T::COLUMN_TYPE>(v2));
}


inline void SQLValues::ValueWriter::operator()(
		const ReadableTuple &src, WritableTuple &dest) const {
	if (TypeUtils::isNull(type_)) {
		return ValueUtils::writeValue<TupleTypes::TYPE_ANY>(
				dest, destColumn_,
				ValueUtils::readValue<TupleValue>(src, srcColumn_));
		return;
	}

	TypeSwitcher switcher(type_);
	switcher(*this, src, dest);
}

template<typename T>
void SQLValues::ValueWriter::TypeAt<T>::operator()(
		const ReadableTuple &src, WritableTuple &dest) const {
			if (ValueUtils::readNull(src, base_.srcColumn_)) {
		return ValueUtils::writeValue<TupleTypes::TYPE_ANY>(
				dest, base_.destColumn_, TupleValue());
	}

	typedef typename T::ValueType ValueType;
	return ValueUtils::writeValue<T::COLUMN_TYPE>(
			dest, base_.destColumn_,
			ValueUtils::readValue<ValueType>(src, base_.srcColumn_));
}


template<typename T1, typename T2>
int32_t SQLValues::TupleComparator::operator()(
		const T1 &t1, const T2 &t2) const {
	return getTypeSwitcher()(*this, t1, t2);
}

template<typename T, typename T1, typename T2>
int32_t SQLValues::TupleComparator::compareElement(
		const CompColumn &column, uint32_t ordinal, bool sensitive,
		bool forKeyOnlyArray, const T1 &t1, const T2 &t2) {

	const TupleColumnType type = TypeUtils::toNonNullable(column.getType());
	if (type != T::COLUMN_TYPE || util::IsSame<T, Types::Any>::VALUE) {
		bool null1;
		bool null2;

		const TupleValue &v1 = getElement<Types::Any>(
				column, ordinal, t1, static_cast<util::TrueType*>(NULL),
				forKeyOnlyArray, null1);
		const TupleValue &v2 = getElement<Types::Any>(
				column, ordinal, t2, static_cast<util::FalseType*>(NULL),
				forKeyOnlyArray, null2);

		const ValueComparator comp(column.getType(), sensitive);
		return comp(v1, v2);
	}

	bool null1;
	bool null2;

	typedef typename T::ValueType ValueType;
	const ValueType &v1 = getElement<T>(
			column, ordinal, t1, static_cast<util::TrueType*>(NULL),
			forKeyOnlyArray, null1);
	const ValueType &v2 = getElement<T>(
			column, ordinal, t2, static_cast<util::FalseType*>(NULL),
			forKeyOnlyArray, null2);

	if (null1 || null2) {
		return ValueUtils::compareNull(null1, null2);
	}

	ValueBasicComparator comp(sensitive);
	return ValueBasicComparator::TypeAt<T>(comp)(v1, v2);
}

template<typename T>
inline typename T::ValueType SQLValues::TupleComparator::getElement(
		const CompColumn &column, uint32_t, const ReadableTuple &t,
		util::TrueType*, bool, bool &nullValue) {
	typedef typename T::ValueType ValueType;
	const TupleColumn &tupleColumn = column.getTupleColumn1();
	if (ValueUtils::readNull(t, tupleColumn)) {
		nullValue = true;
		return ValueType();
	}
	nullValue = false;
	return ValueUtils::readValue<ValueType>(t, tupleColumn);
}

template<typename T>
inline typename T::ValueType SQLValues::TupleComparator::getElement(
		const CompColumn &column, uint32_t, const ReadableTuple &t,
		util::FalseType*, bool, bool &nullValue) {
	typedef typename T::ValueType ValueType;
	const TupleColumn &tupleColumn = column.getTupleColumn2();
	if (ValueUtils::readNull(t, tupleColumn)) {
		nullValue = true;
		return ValueType();
	}
	nullValue = false;
	return ValueUtils::readValue<ValueType>(t, tupleColumn);
}

template<typename T>
inline typename T::ValueType SQLValues::TupleComparator::getElement(
		const CompColumn &column, uint32_t ordinal, const ArrayTuple &t,
		util::TrueType*, bool forKeyOnlyArray, bool &nullValue) {
	typedef typename T::ValueType ValueType;
	const TupleValue &src =
			t.get(forKeyOnlyArray ? ordinal : column.getColumnPos(true));
	if (ValueUtils::isNull(src)) {
		nullValue = true;
		return ValueType();
	}
	nullValue = false;
	return ValueUtils::promoteValue<T::COLUMN_TYPE>(src);
}

template<typename T>
inline typename T::ValueType SQLValues::TupleComparator::getElement(
		const CompColumn &column, uint32_t ordinal, const ArrayTuple &t,
		util::FalseType*, bool forKeyOnlyArray, bool &nullValue) {
	typedef typename T::ValueType ValueType;
	const TupleValue &src =
			t.get(forKeyOnlyArray ? ordinal : column.getColumnPos(false));
	if (ValueUtils::isNull(src)) {
		nullValue = true;
		return ValueType();
	}
	nullValue = false;
	return ValueUtils::promoteValue<T::COLUMN_TYPE>(src);
}

template<typename T>
template<typename T1, typename T2>
int32_t SQLValues::TupleComparator::TypeAt<T>::operator()(
		const T1 &t1, const T2 &t2) const {
	for (CompColumnList::const_iterator it = base_.columnList_.begin();
			it != base_.columnList_.end(); ++it) {
		const int32_t ret = compareElement<T, T1, T2>(
				*it, static_cast<uint32_t>(it - base_.columnList_.begin()),
				base_.sensitive_, base_.forKeyOnlyArray_, t1, t2);
		if (ret != 0) {
			return ret;
		}
	}
	return 0;
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


template<typename R>
uint32_t SQLValues::ValueUtils::fnv1aHashSequence(uint32_t base, R &value) {
	uint32_t hash = base;

	for (size_t size; (size = value.getNext()) > 0;) {
		hash = fnv1aHashBytes(hash, &value.get(), size);
		value.step(size);
	}

	return hash;
}


template<TupleColumnType T>
inline typename SQLValues::TypeUtils::Traits<T>::ValueType
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
	return !!getValue<int8_t>(src);
}

template<>
inline TupleString SQLValues::ValueUtils::getValue(const TupleValue &src) {
	return src;
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
inline T SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column) {
	return ValueUtils::getValue<T>(tuple.get(column));
}

#if GD_ENABLE_SQL_READABLE_TUPLE_TYPE_SPECIFIED_GET
template<>
inline int32_t SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column) {
	return tuple.getAs<int32_t>(column);
}

template<>
inline int64_t SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column) {
	return tuple.getAs<int64_t>(column);
}

template<>
inline TupleString SQLValues::ValueUtils::readValue(
		const ReadableTuple &tuple, const TupleColumn &column) {
	return tuple.getAs<TupleString>(column);
}
#endif 

inline bool SQLValues::ValueUtils::readNull(
		const ReadableTuple &tuple, const TupleColumn &column) {
#if GD_ENABLE_SQL_READABLE_TUPLE_TYPE_SPECIFIED_GET
	return tuple.checkIsNull(static_cast<uint16_t>(column.getPosition()));
#else
	return isNull(readValue<TupleValue>(tuple, column));
#endif
}

template<TupleColumnType T>
inline void SQLValues::ValueUtils::writeValue(
		WritableTuple &tuple, const TupleColumn &column,
		const typename TypeUtils::Traits<T>::ValueType &value) {
	tuple.set(column, toAnyByValue<T>(value));
}

#if GD_ENABLE_SQL_WRITABLE_TUPLE_TYPE_SPECIFIED_SET
template<>
inline void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_INTEGER>(
		WritableTuple &tuple, const TupleColumn &column,
		const int32_t &value) {
	tuple.setBy(column, value);
}

template<>
inline void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_LONG>(
		WritableTuple &tuple, const TupleColumn &column,
		const int64_t &value) {
	tuple.setBy(column, value);
}

template<>
inline void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_TIMESTAMP>(
		WritableTuple &tuple, const TupleColumn &column,
		const int64_t &value) {
	tuple.setBy(column, value);
}

template<>
inline void SQLValues::ValueUtils::writeValue<TupleTypes::TYPE_STRING>(
		WritableTuple &tuple, const TupleColumn &column,
		const TupleString &value) {
	tuple.setBy(column, value);
}
#endif 

#endif
