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

#ifndef SQL_EXPRESSION_H_
#define SQL_EXPRESSION_H_

#include "sql_value.h"
#include "sql_parser.h"

struct SQLExprs {
	typedef TupleValue::VarContext VarContext;
	typedef TupleList::Reader TupleListReader;
	typedef TupleList::Writer TupleListWriter;
	typedef TupleList::ReadableTuple ReadableTuple;
	typedef TupleList::WritableTuple WritableTuple;

	typedef TupleList::Column TupleColumn;
	typedef util::Vector<TupleColumn> TupleColumnList;

	typedef SyntaxTree::Expr SyntaxExpr;
	typedef SyntaxTree::ExprList SyntaxExprRefList;
	typedef util::Vector<SyntaxExpr> SyntaxExprList;

	typedef SQLType::Id ExprType;
	typedef SQLType::AggregationPhase AggregationPhase;


	class ExprCode;

	class Expression;

	struct FunctionValueUtils;
	class NormalFunctionContext;
	class ExprContext;

	struct ExprSpec;

	class ExprFactory;
	class ExprFactoryContext;


	class ExprRewriter;
	class SyntaxExprRewriter;
	class TypeResolver;

	struct IndexSpec;
	struct IndexCondition;
	typedef util::Vector<IndexCondition> IndexConditionList;

	class IndexSelector;

	struct ExprTypeUtils;
	struct PlanPartitioningInfo;
	struct PlanNarrowingKey;
	struct DataPartitionUtils;


	class DefaultExprFactory;
	class ExprRegistrar;

	struct ExprSpecBase;
	struct ExprUtils;

	class PlanningExpression;
	class AggregationExpressionBase;
	template<typename F, typename T> class BasicExpressionBase;
	template<typename F, int32_t C, typename T> class BasicExpression;
	template<typename F, int32_t C, typename T> class AggregationExpression;

	template<typename S> class CustomExprRegistrar;
	template<typename S> class BasicExprRegistrar;
	template<typename S> class AggregationExprRegistrar;
};

class SQLExprs::ExprCode {
public:
	enum Attribute {
		ATTR_ASSIGNED = 1 << 0,
		ATTR_AGGR_ARRANGED = 1 << 1,
		ATTR_AGGREGATED = 1 << 2,
		ATTR_GROUPING = 1 << 3,
		ATTR_COLUMN_UNIFIED = 1 << 4
	};

	struct TableEntry;
	typedef util::AllocVector<TableEntry> TableList;

	ExprCode();

	ExprType getType() const;
	void setType(ExprType type);

	TupleColumnType getColumnType() const;
	void setColumnType(TupleColumnType type);

	uint32_t getInput() const;
	void setInput(uint32_t input);

	uint32_t getColumnPos() const;
	void setColumnPos(uint32_t columnPos);

	const TupleValue& getValue() const;
	void setValue(SQLValues::ValueContext &cxt, const TupleValue &value);

	void importFromSyntax(SQLValues::ValueContext &cxt, const SyntaxExpr &src);
	void exportToSyntax(SyntaxExpr &dest) const;

	uint32_t getAggregationIndex() const;
	void setAggregationIndex(uint32_t index);

	uint32_t getAttributes() const;
	void setAttributes(uint32_t attributes);

	UTIL_OBJECT_CODER_PARTIAL_OBJECT;
	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_ENUM(type_, SQLType::Coder()),
			UTIL_OBJECT_CODER_ENUM(
					columnType_,
					static_cast<TupleColumnType>(TupleTypes::TYPE_NULL),
					TupleList::TupleColumnTypeCoder()),
			UTIL_OBJECT_CODER_OPTIONAL(
					input_, std::numeric_limits<uint32_t>::max()),
			UTIL_OBJECT_CODER_OPTIONAL(
					column_, std::numeric_limits<uint32_t>::max()),
			value_,
			tableList_,
			UTIL_OBJECT_CODER_OPTIONAL(
					output_, std::numeric_limits<uint32_t>::max()));

private:
	struct Constants {
		static const TupleValue NULL_VALUE;
	};


	ExprType type_;
	TupleColumnType columnType_;
	uint32_t input_;
	uint32_t column_;
	TupleValue *value_;
	TableList *tableList_;
	uint32_t output_;


	uint32_t aggrIndex_;
	uint32_t attributes_;
};

struct SQLExprs::ExprCode::TableEntry {
	TableEntry();

	uint32_t input_;

	UTIL_OBJECT_CODER_MEMBERS(input_);
};

class SQLExprs::Expression {
public:
	struct InOption;
	struct OutOption;
	template<typename Base> class Coder;

	class Iterator;
	class ModIterator;

	Expression();
	virtual ~Expression();

	void initialize(ExprFactoryContext &cxt, const ExprCode &code);

	virtual TupleValue eval(ExprContext &cxt) const = 0;

	const Expression& child() const;
	const Expression& next() const;

	const Expression* findChild() const;
	const Expression* findNext() const;

	size_t getChildCount() const;

	const ExprCode& getCode() const;

	TupleValue asColumnValue(const TupleValue &value) const;


	bool isPlannable() const;
	ExprCode& getPlanningCode();

	void addChild(Expression *expr);


	static Expression& importFrom(
			ExprFactoryContext &cxt, const InOption &option);
	void exportTo(const OutOption &option) const;

	template<typename Base>
	Coder<Base> makeCoder(
			const Base &base, ExprFactoryContext *factoryCxt,
			const ExprFactory *factory);

protected:
	void setPlanningCode(ExprCode *code);

private:
	friend class ExprFactory;

	struct Constants {
		static const util::ObjectCoder::Attribute STREAM_ATTR_OP_LIST;
	};

	Expression(const Expression&);
	Expression& operator=(const Expression&);

	static Expression& importFromSyntax(
			ExprFactoryContext &cxt, const SyntaxExpr &src,
			const InOption &option);
	SyntaxExpr exportToSyntax(const OutOption &option) const;

	template<typename S> static Expression& importFromStream(
			ExprFactoryContext &cxt, S &stream, const InOption &option);
	template<typename S> void exportToStream(
			S &stream, const OutOption &option) const;

	static void resolvePlaceholder(
			SQLValues::ValueContext &cxt, ExprCode &code,
			const InOption &option);

	template<typename E> static bool isListingSyntaxExpr(
			ExprType type, const ExprFactory &factory, const E &expr);

	static size_t getChildCount(const SyntaxExpr &expr);
	static size_t getChildCount(const Expression &expr);

	const ExprCode *code_;
	ExprCode *planningCode_;
	Expression *child_;
	Expression *next_;
};

struct SQLExprs::Expression::InOption {
	typedef util::ObjectInStream<util::ArrayByteInStream> ObjectByteInStream;

	explicit InOption(SQLValues::ValueContext &valueCxt);

	VarContext& getVarContext() const;

	void setStream(ObjectByteInStream &stream);
	void setStream(util::AbstractObjectInStream &stream);

	SQLValues::ValueContext &valueCxt_;

	const SyntaxExpr *syntaxExpr_;
	ObjectByteInStream *byteStream_;
	util::AbstractObjectInStream *stream_;

	const util::Vector<TupleValue> *parameterList_;
};

struct SQLExprs::Expression::OutOption {
	typedef util::XArrayOutStream< util::StdAllocator<
			uint8_t, util::VariableSizeAllocator<> > > OutStream;
	typedef util::ObjectOutStream<
			util::ByteStream<OutStream> > ObjectByteOutStream;

	OutOption(util::StackAllocator *alloc, const ExprFactory &factory);

	util::StackAllocator& getAllocator() const;
	const ExprFactory& getFactory() const;

	void setStream(ObjectByteOutStream &stream);
	void setStream(util::AbstractObjectOutStream &stream);

	util::StackAllocator *alloc_;
	const ExprFactory *factory_;

	SyntaxExpr **syntaxExprRef_;
	ObjectByteOutStream *byteStream_;
	util::AbstractObjectOutStream *stream_;
};

template<typename Base>
class SQLExprs::Expression::Coder : public util::ObjectCoderBase<
		SQLExprs::Expression::Coder<Base>, typename Base::Allocator, Base> {
public:
	typedef Base BaseCoder;
	typedef typename Base::Allocator Allocator;
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoderBase<
			Expression::Coder<Base>, typename Base::Allocator, Base> BaseType;

	Coder(
			const BaseCoder &base, ExprFactoryContext *factoryCxt,
			const ExprFactory *factory, SQLValues::ValueContext *valueCxt);

	template<typename Org, typename S, typename T, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::encodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename T, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::decodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const Expression &value,
			const Attribute &attr, const Traits&) const;

	template<typename Org, typename S, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, Expression *&value,
			const Attribute &attr, const Traits&) const;

private:
	ExprFactoryContext *factoryCxt_;
	const ExprFactory *factory_;
	SQLValues::ValueContext *valueCxt_;
};

class SQLExprs::Expression::Iterator {
public:
	explicit Iterator(const Expression &expr);

	const Expression& get() const;

	bool exists() const;
	void next();

private:
	const Expression *cur_;
};

class SQLExprs::Expression::ModIterator {
public:
	explicit ModIterator(Expression &expr);

	Expression& get() const;

	bool exists() const;
	void next();

	void insert(Expression &expr);
	void append(Expression &expr);
	void remove();

private:
	Expression *parent_;
	Expression *prev_;
	Expression *cur_;
};

struct SQLExprs::FunctionValueUtils {
	bool isSpaceChar(util::CodePoint c) const;
};

class SQLExprs::NormalFunctionContext {
public:
	typedef util::FalseType AllocatorType;
	typedef util::FalseType WriterType;
	typedef FunctionValueUtils ValueUtilsType;

	explicit NormalFunctionContext(SQLValues::ValueContext &valueCxt);

	ExprContext& getBase();
	void setBase(ExprContext *base);

	util::FalseType getAllocator();

	util::FalseType initializeResultWriter(uint64_t initialCapacity);
	util::FalseType getResultWriter();
	const ValueUtilsType& getValueUtils();

	int64_t getCurrentTimeMillis();

	util::TimeZone getTimeZone();
	void applyDateTimeOption(util::DateTime::ZonedOption &option) const;
	void applyDateTimeOption(util::DateTime::Option &option) const;

	void finishFunction();
	util::FalseType isFunctionFinished();

private:
	static void handleBaseNotFound();

	ExprContext *base_;
	SQLValues::ValueContext &valueCxt_;
	ValueUtilsType valueUtils_;
};

class SQLExprs::ExprContext {
public:
	explicit ExprContext(const SQLValues::ValueContext::Source &source);


	operator SQLValues::ValueContext&();
	SQLValues::ValueContext& getValueContext();

	NormalFunctionContext& getFunctionContext();

	util::StackAllocator& getAllocator();


	const TupleColumn& getReaderColumn(uint32_t index, uint32_t pos);
	TupleListReader& getReader(uint32_t index);
	ReadableTuple getReadableTuple(uint32_t index);

	void setReader(
			uint32_t index, TupleListReader &reader,
			const TupleColumnList *columnList);
	void setReadableTuple(uint32_t index, const ReadableTuple &tuple);

	SQLValues::ArrayTuple* getArrayTuple(uint32_t index);
	void setArrayTuple(uint32_t index, SQLValues::ArrayTuple *tuple);

	uint32_t getActiveInput();
	void setActiveInput(uint32_t index);

	int64_t updateTupleId(uint32_t index);
	int64_t getLastTupleId(uint32_t index);
	void setLastTupleId(uint32_t index, int64_t id);


	void setAggregationTuple(SQLValues::ArrayTuple &aggrTuple);

	TupleValue getAggregationValue(uint32_t index);
	void setAggregationValue(uint32_t index, const TupleValue &value);

private:
	struct Entry {
		Entry();

		TupleListReader *reader_;
		const TupleColumnList *columnList_;

		ReadableTuple *readableTuple_;
		ReadableTuple *readableTupleStorage_;

		SQLValues::ArrayTuple *arrayTuple_;
		int64_t lastTupleId_;
	};

	typedef util::Vector<Entry> EntryList;

	Entry& prepareEntry(uint32_t index);

	SQLValues::ValueContext valueCxt_;
	NormalFunctionContext funcCxt_;

	EntryList entryList_;

	uint32_t activeInput_;
	SQLValues::ArrayTuple *aggrTuple_;
};

struct SQLExprs::ExprSpec {
	enum Constants {
		IN_TYPE_COUNT = 2,
		IN_LIST_SIZE = 7,
		AGGR_LIST_SIZE = 3,
		IN_MAX_OPTS_COUNT = 3
	};

	struct In {
		In();

		TupleColumnType typeList_[IN_TYPE_COUNT];
		uint32_t flags_;
	};

	enum Flag {
		FLAG_OPTIONAL = 1 << 0,
		FLAG_PROMOTABLE = 1 << 1,
		FLAG_EXACT = 1 << 2,

		FLAG_NON_NULLABLE = 1 << 3,
		FLAG_NULLABLE = 1 << 4,
		FLAG_EVEN_ARGS_NULLABLE = 1 << 5,
		FLAG_INHERIT1 = 1 << 6,
		FLAG_INHERIT2 = 1 << 7,
		FLAG_INHERIT_NULLABLE1 = 1 << 8,
		FLAG_REPEAT1 = 1 << 9,
		FLAG_REPEAT2 = 1 << 10,
		FLAG_ODD_TAIL = 1 << 11,

		FLAG_DYNAMIC = 1 << 12,
		FLAG_INTERNAL = 1 << 13,
		FLAG_EXPERIMENTAL = 1 << 14,

		FLAG_WINDOW = 1 << 15,
		FLAG_WINDOW_ONLY = 1 << 16,
		FLAG_WINDOW_COLUMN = 1 << 17,
		FLAG_PSEUDO_WINDOW = 1 << 18,
		FLAG_DISTINCT = 1 << 19,

		FLAG_ARGS_LISTING = 1 << 20
	};

	ExprSpec();

	bool isAggregation() const;

	In inList_[IN_LIST_SIZE];
	In aggrList_[AGGR_LIST_SIZE];
	TupleColumnType outType_;
	uint32_t flags_;
	std::pair<int32_t, int32_t> argCounts_;
	ExprType distinctExprType_;
};

class SQLExprs::ExprFactory {
public:
	typedef Expression& (*FactoryFunc)(
			ExprFactoryContext &cxt, const ExprCode &code);

	ExprFactory();

	static const ExprFactory& getDefaultFactory();

	Expression& create(ExprFactoryContext &cxt, ExprType type) const;

	virtual Expression& create(
			ExprFactoryContext &cxt, const ExprCode &code) const = 0;
	virtual const ExprSpec& getSpec(ExprType type) const = 0;

private:
	ExprFactory(const ExprFactory&);
	ExprFactory& operator=(const ExprFactory&);
};

class SQLExprs::ExprFactoryContext {
public:
	class Scope;

	explicit ExprFactoryContext(util::StackAllocator &alloc);

	util::StackAllocator& getAllocator();

	const ExprFactory& getFactory();
	void setFactory(const ExprFactory *factory);

	void clearInputTypes();
	TupleColumnType getInputType(uint32_t index, uint32_t pos);
	void setInputType(uint32_t index, uint32_t pos, TupleColumnType type);

	TupleColumnType getUnifiedInputType(uint32_t pos);

	TupleColumn getInputColumn(uint32_t index, uint32_t pos);

	uint32_t getInputCount();
	uint32_t getInputColumnCount(uint32_t index);

	bool isInputNullable(uint32_t index);
	void setInputNullable(uint32_t index, bool nullable);

	uint32_t getAggregationColumnCount();
	TupleColumnType getAggregationColumnType(uint32_t index);
	uint32_t addAggregationColumn(TupleColumnType type);
	void clearAggregationColumns();


	bool isPlanning();
	void setPlanning(bool planning);

	AggregationPhase getAggregationPhase(bool forSrc);
	void setAggregationPhase(bool forSrc, AggregationPhase aggrPhase);

	void setAggregationTypeListRef(
			const util::Vector<TupleColumnType> *typeListRef);

	const Expression* getBaseExpression();
	void setBaseExpression(const Expression *expr);

private:
	typedef util::Vector<TupleColumnType> TypeList;
	typedef util::Vector<TypeList> InputList;

	struct ScopedEntry {
		ScopedEntry();

		bool planning_;
		AggregationPhase srcAggrPhase_;
		AggregationPhase destAggrPhase_;
		const TypeList *aggrTypeListRef_;
		const Expression *baseExpr_;
	};

	ExprFactoryContext(const ExprFactoryContext&);
	ExprFactoryContext& operator=(const ExprFactoryContext&);

	util::StackAllocator &alloc_;
	const ExprFactory *factory_;

	ScopedEntry topScopedEntry_;
	ScopedEntry *scopedEntry_;

	InputList inputList_;
	util::Vector<TupleColumnList> inputColumnList_;
	util::Vector<bool> nullableList_;
	TypeList aggrTypeList_;
	uint32_t nextAggrIndex_;
};

class SQLExprs::ExprFactoryContext::Scope {
public:
	explicit Scope(ExprFactoryContext &cxt);
	~Scope();

private:
	ExprFactoryContext &cxt_;
	ScopedEntry curScopedEntry_;
	ScopedEntry *prevScopedEntry_;
};



template<typename Base>
SQLExprs::Expression::Coder<Base> SQLExprs::Expression::makeCoder(
		const Base &base, ExprFactoryContext *factoryCxt,
		const ExprFactory *factory) {
	return Coder<Base>(base, factoryCxt, factory);
}


template<typename Base>
SQLExprs::Expression::Coder<Base>::Coder(
		const BaseCoder &base, ExprFactoryContext *factoryCxt,
		const ExprFactory *factory, SQLValues::ValueContext *valueCxt) :
		BaseType(base.getAllocator(), base),
		factoryCxt_(factoryCxt),
		factory_(factory),
		valueCxt_(valueCxt) {
}

template<typename Base>
template<typename Org, typename S, typename Traits>
void SQLExprs::Expression::Coder<Base>::encodeBy(
		Org &orgCoder, S &stream, const Expression &value,
		const Attribute &attr, const Traits&) const {
	static_cast<void>(orgCoder);
	static_cast<void>(attr);

	assert(factory_ != NULL);
	OutOption option(NULL, *factory_);
	option.setStream(stream);
	value.exportTo(option);
}

template<typename Base>
template<typename Org, typename S, typename Traits>
void SQLExprs::Expression::Coder<Base>::decodeBy(
		Org &orgCoder, S &stream, Expression *&value,
		const Attribute &attr, const Traits&) const {
	static_cast<void>(orgCoder);
	static_cast<void>(attr);

	util::ObjectCoder::Type valueType;
	typename S::ValueScope valueScope(stream, valueType, attr);

	if (valueType == util::ObjectCoder::TYPE_NULL) {
		value = NULL;
	}
	else {
		assert(valueCxt_ != NULL);
		InOption option(*valueCxt_);
		option.setStream(valueScope.stream());
		value = &importFrom(*factoryCxt_, option);
	}
}

#endif
