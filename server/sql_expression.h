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
	typedef util::Vector<TupleListReader**> ReaderRefList;

	typedef SyntaxTree::Expr SyntaxExpr;
	typedef SyntaxTree::ExprList SyntaxExprRefList;
	typedef util::Vector<SyntaxExpr> SyntaxExprList;

	typedef SQLType::Id ExprType;
	typedef SQLType::AggregationPhase AggregationPhase;

	typedef SQLValues::SummaryColumn SummaryColumn;
	typedef SQLValues::SummaryTuple SummaryTuple;
	typedef SQLValues::SummaryTupleSet SummaryTupleSet;
	typedef util::Vector<SummaryColumn> SummaryColumnList;

	typedef SQLValues::LongInt RangeKey;


	class ExprCode;

	class Expression;

	struct FunctionValueUtils;
	struct WindowState;
	class NormalFunctionContext;
	class ExprContext;

	struct ExprSpec;

	class ExprFactory;
	class ExprFactoryContext;
	struct ExprProfile;


	class ExprRewriter;
	class SyntaxExprRewriter;
	class TypeResolver;
	struct TypeResolverResult;

	struct IndexSpec;
	struct IndexCondition;
	typedef util::Vector<IndexCondition> IndexConditionList;

	class IndexSelector;

	struct ExprTypeUtils;
	struct PlanPartitioningInfo;
	struct PlanNarrowingKey;
	struct DataPartitionUtils;
	struct RangeGroupUtils;


	class DefaultExprFactory;
	class ExprRegistrar;
	template<typename R> class VariantsRegistrarBase;

	struct ExprSpecBase;
	struct ExprUtils;

	class PlanningExpression;
	class ComparableExpressionBase;
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
		ATTR_WINDOWING = 1 << 4,
		ATTR_COLUMN_UNIFIED = 1 << 5,
		ATTR_AGGR_DECREMENTAL = 1 << 6,
		ATTR_DECREMENTAL_FORWARD = 1 << 7,
		ATTR_DECREMENTAL_BACKWARD = 1 << 8
	};

	enum InputSourceType {
		INPUT_READER,
		INPUT_READER_MULTI,
		INPUT_READER_TUPLE,
		INPUT_ARRAY_TUPLE,
		INPUT_SUMMARY_TUPLE,
		INPUT_SUMMARY_TUPLE_BODY,
		INPUT_MULTI_STAGE,

		INPUT_CONTAINER,
		INPUT_CONTAINER_GENERAL,
		INPUT_CONTAINER_PLAIN,

		END_INPUT
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

	uint32_t getOutput() const;
	void setOutput(uint32_t output);

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

	TupleValue asColumnValueSpecific(const TupleValue &value) const;
	TupleValue asColumnValue(
			SQLValues::ValueContext *cxt, const TupleValue &value) const;


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

	bool getRangeGroupId(
			const TupleValue &key, const RangeKey &interval,
			const RangeKey &offset, RangeKey &id) const;
};

struct SQLExprs::WindowState {
	WindowState();

	int64_t partitionTupleCount_;
	int64_t partitionValueCount_;

	RangeKey rangeKey_;
	RangeKey rangePrevKey_;
	RangeKey rangeNextKey_;
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

	int64_t toTimestamp(const util::DateTime &dateTime, const int64_t&) const;
	MicroTimestamp toTimestamp(
			const util::PreciseDateTime &dateTime, const MicroTimestamp&) const;
	NanoTimestamp toTimestamp(
			const util::PreciseDateTime &dateTime, const NanoTimestamp&) const;

	util::DateTime toDateTime(const int64_t &tsValue) const;
	util::PreciseDateTime toDateTime(const MicroTimestamp &tsValue) const;
	util::PreciseDateTime toDateTime(const NanoTimestamp &tsValue) const;

	util::DateTime::FieldType getTimePrecision(const int64_t&) const;
	util::DateTime::FieldType getTimePrecision(const MicroTimestamp&) const;
	util::DateTime::FieldType getTimePrecision(const NanoTimestamp&) const;

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
	typedef ExprCode::InputSourceType InputSourceType;

	explicit ExprContext(const SQLValues::ValueContext::Source &source);


	operator SQLValues::ValueContext&();
	SQLValues::ValueContext& getValueContext();

	NormalFunctionContext& getFunctionContext();

	util::StackAllocator& getAllocator();


	const TupleColumn& getReaderColumn(uint32_t index, uint32_t pos);

	TupleListReader& getReader(uint32_t index);
	ReadableTuple getReadableTuple(uint32_t index);

	void setReader(
			uint32_t index, TupleListReader *reader,
			const TupleColumnList *columnList);
	void setReadableTuple(uint32_t index, const ReadableTuple &tuple);
	void setSummaryTuple(uint32_t index, const SummaryTuple &tuple);

	SQLValues::ArrayTuple* getArrayTuple(uint32_t index);
	void setArrayTuple(uint32_t index, SQLValues::ArrayTuple *tuple);

	uint32_t getActiveInput();
	void setActiveInput(uint32_t index);

	TupleListReader** getActiveReaderRef();
	void setActiveReaderRef(TupleListReader **readerRef);

	int64_t updateTupleId(uint32_t index);
	int64_t getLastTupleId(uint32_t index);
	void setLastTupleId(uint32_t index, int64_t id);

	ReadableTuple* getReadableTupleRef(uint32_t index);
	void setReadableTupleRef(uint32_t index, ReadableTuple *tupleRef);

	SummaryTuple* getSummaryTupleRef(uint32_t index);
	void setSummaryTupleRef(uint32_t index, SummaryTuple *tupleRef);

	const SummaryColumnList* getSummaryColumnListRef(uint32_t index);
	void setSummaryColumnListRef(
			uint32_t index, const SummaryColumnList *listRef);

	InputSourceType getInputSourceType(uint32_t index);
	void setInputSourceType(uint32_t index, InputSourceType type);


	void setAggregationTupleRef(
			SummaryTupleSet *aggrTupleSet, SummaryTuple *aggrTuple);

	bool isAggregationNull(const SummaryColumn &column);

	TupleValue getAggregationValue(const SummaryColumn &column);
	void setAggregationValue(
			const SummaryColumn &column, const TupleValue &value);
	void setAggregationValuePromoted(
			const SummaryColumn &column, const TupleValue &value);

	template<typename T>
	typename T::ValueType getAggregationValueAs(
			const SummaryColumn &column);
	template<typename T>
	void setAggregationValueBy(
			const SummaryColumn &column, const typename T::ValueType &value);

	template<typename T>
	void incrementAggregationValueBy(const SummaryColumn &column);

	template<typename T>
	void appendAggregationValueBy(
			const SummaryColumn &column,
			typename SQLValues::TypeUtils::template Traits<
					T::COLUMN_TYPE>::ReadableRefType value);

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

	void initializeAggregationValues();

	const WindowState* getWindowState();
	void setWindowState(const WindowState *state);

private:
	struct Entry {
		Entry();

		TupleListReader *reader_;
		const TupleColumnList *columnList_;

		ReadableTuple *readableTuple_;
		SummaryTuple *summaryTuple_;
		const SummaryColumnList *summaryColumnList_;

		SQLValues::ArrayTuple *arrayTuple_;
		int64_t lastTupleId_;

		InputSourceType inputSourceType_;
	};

	typedef util::AllocVector<Entry> EntryList;

	Entry& prepareEntry(uint32_t index);

	SQLValues::ValueContext valueCxt_;
	NormalFunctionContext funcCxt_;

	EntryList entryList_;

	uint32_t activeInput_;
	SummaryTupleSet *aggrTupleSet_;
	SummaryTuple *aggrTuple_;
	TupleListReader **activeReaderRef_;
	const WindowState *windowState_;
};

struct SQLExprs::ExprSpec {
	enum Constants {
		IN_TYPE_COUNT = 2,
		IN_EXPANDED_TYPE_COUNT = 3,
		IN_LIST_SIZE = 8,
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

		FLAG_ARGS_LISTING = 1 << 20,
		FLAG_AGGR_FINISH_DEFAULT = 1 << 21,
		FLAG_AGGR_ORDERING = 1 << 22,
		FLAG_AGGR_DECREMENTAL = 1 << 23,

		FLAG_WINDOW_POS_BEFORE = 1 << 24,
		FLAG_WINDOW_POS_AFTER = 1 << 25,
		FLAG_WINDOW_VALUE_COUNTING = 1 << 26,

		FLAG_COMP_SENSITIVE = 1 << 27
	};

	enum DecrementalType {
		DECREMENTAL_NONE,
		DECREMENTAL_FORWARD,
		DECREMENTAL_BACKWARD,
		DECREMENTAL_SOME
	};

	ExprSpec();

	bool isAggregation() const;

	ExprSpec toDistinct(ExprType srcType) const;

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
	typedef ExprCode::InputSourceType InputSourceType;
	typedef ExprSpec::DecrementalType DecrementalType;

	class Scope;

	explicit ExprFactoryContext(util::StackAllocator &alloc);
	~ExprFactoryContext();

	util::StackAllocator& getAllocator();

	const ExprFactory& getFactory();
	void setFactory(const ExprFactory *factory);

	void clearInputTypes();
	TupleColumnType getInputType(uint32_t index, uint32_t pos);
	void setInputType(uint32_t index, uint32_t pos, TupleColumnType type);

	TupleColumnType getUnifiedInputType(uint32_t pos);

	TupleColumn getInputColumn(uint32_t index, uint32_t pos);

	InputSourceType getInputSourceType(uint32_t index);
	void setInputSourceType(uint32_t index, InputSourceType type);

	uint32_t getInputCount();
	uint32_t getInputColumnCount(uint32_t index);

	bool isInputNullable(uint32_t index);
	void setInputNullable(uint32_t index, bool nullable);

	uint32_t getAggregationColumnCount();
	TupleColumnType getAggregationColumnType(uint32_t index);
	uint32_t addAggregationColumn(TupleColumnType type);
	void clearAggregationColumns();

	void initializeReaderRefList(uint32_t index, ReaderRefList *list);
	void addReaderRef(uint32_t index, TupleListReader **readerRef);
	util::AllocUniquePtr<void>& getInputSourceRef(uint32_t index);

	TupleListReader** getActiveReaderRef();
	void setActiveReaderRef(TupleListReader **readerRef);

	ReadableTuple* getReadableTupleRef(uint32_t index);
	void setReadableTupleRef(uint32_t index, ReadableTuple *tupleRef);

	SummaryTuple* getSummaryTupleRef(uint32_t index);
	void setSummaryTupleRef(uint32_t index, SummaryTuple *tupleRef);

	SummaryColumnList* getSummaryColumnListRef(uint32_t index);
	void setSummaryColumnListRef(
			uint32_t index, SummaryColumnList *listRef);

	const SummaryColumn& getArrangedAggregationColumn(uint32_t index);


	bool isPlanning();
	void setPlanning(bool planning);

	bool isArgChecking();
	void setArgChecking(bool checking);

	AggregationPhase getAggregationPhase(bool forSrc);
	void setAggregationPhase(bool forSrc, AggregationPhase aggrPhase);

	DecrementalType getDecrementalType();
	void setDecrementalType(DecrementalType type);

	void setAggregationTypeListRef(
			const util::Vector<TupleColumnType> *typeListRef);

	SummaryTuple* getAggregationTupleRef();
	void setAggregationTupleRef(SummaryTuple *tupleRef);

	SummaryTupleSet* getAggregationTupleSet();
	void setAggregationTupleSet(SummaryTupleSet *tupleSet);

	const SQLValues::CompColumnList* getArrangedKeyList(
			bool &orderingRestricted);
	void setArrangedKeyList(
			const SQLValues::CompColumnList *keyList, bool orderingRestricted);

	bool isSummaryColumnsArranging();
	void setSummaryColumnsArranging(bool arranging);

	const Expression* getBaseExpression();
	void setBaseExpression(const Expression *expr);

	void setProfile(ExprProfile *profile);
	ExprProfile* getProfile();


	static TupleColumnType unifyInputType(
			TupleColumnType lastUnified, TupleColumnType elemType, bool first);

private:
	typedef util::Vector<TupleColumnType> TypeList;
	typedef util::Vector<TypeList> InputList;
	typedef util::Vector<ReaderRefList*> AllReaderRefList;
	typedef util::Vector<util::AllocUniquePtr<void>*> InputSourceRefList;

	struct ScopedEntry {
		ScopedEntry();

		bool planning_;
		bool argChecking_;
		bool summaryColumnsArranging_;

		AggregationPhase srcAggrPhase_;
		AggregationPhase destAggrPhase_;
		DecrementalType decrementalType_;

		const TypeList *aggrTypeListRef_;
		SummaryTuple *aggrTupleRef_;
		SummaryTupleSet *aggrTupleSet_;
		const SQLValues::CompColumnList *arrangedKeyList_;
		bool arrangedKeyOrderingRestricted_;

		const Expression *baseExpr_;

		ExprProfile *profile_;
	};

	ExprFactoryContext(const ExprFactoryContext&);
	ExprFactoryContext& operator=(const ExprFactoryContext&);

	util::StackAllocator &alloc_;
	const ExprFactory *factory_;

	ScopedEntry topScopedEntry_;
	ScopedEntry *scopedEntry_;

	InputList inputList_;
	util::Vector<TupleColumnList> inputColumnList_;
	util::Vector<InputSourceType> inputSourceTypeList_;
	util::Vector<bool> nullableList_;

	TypeList aggrTypeList_;
	uint32_t nextAggrIndex_;

	AllReaderRefList allReaderRefList_;
	InputSourceRefList inputSourceRefList_;

	TupleListReader **activeReaderRef_;
	util::Vector<ReadableTuple*> readableTupleRefList_;
	util::Vector<SummaryTuple*> summaryTupleRefList_;
	util::Vector<SummaryColumnList*> summaryColumnsRefList_;
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

struct SQLExprs::ExprProfile {
	static SQLValues::ValueProfile* getValueProfile(ExprProfile *profile);

	SQLValues::ValueProfile valueProfile_;
	SQLValues::ProfileElement compConst_;
	SQLValues::ProfileElement compColumns_;
	SQLValues::ProfileElement condInList_;
	SQLValues::ProfileElement outNoNull_;
};



inline const SQLExprs::Expression& SQLExprs::Expression::child() const {
	assert(child_ != NULL);
	return *child_;
}

inline const SQLExprs::Expression& SQLExprs::Expression::next() const {
	assert(next_ != NULL);
	return *next_;
}

inline const SQLExprs::Expression* SQLExprs::Expression::findChild() const {
	return child_;
}

inline const SQLExprs::Expression* SQLExprs::Expression::findNext() const {
	return next_;
}

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


inline SQLExprs::Expression::Iterator::Iterator(const Expression &expr) :
		cur_(expr.child_) {
}

inline const SQLExprs::Expression& SQLExprs::Expression::Iterator::get() const {
	assert(exists());
	return *cur_;
}

inline bool SQLExprs::Expression::Iterator::exists() const {
	return (cur_ != NULL);
}

inline void SQLExprs::Expression::Iterator::next() {
	cur_ = cur_->next_;
}


inline SQLExprs::ExprContext::operator SQLValues::ValueContext&() {
	return valueCxt_;
}

inline SQLValues::ValueContext& SQLExprs::ExprContext::getValueContext() {
	return valueCxt_;
}

inline SQLExprs::NormalFunctionContext&
SQLExprs::ExprContext::getFunctionContext() {
	return funcCxt_;
}

inline SQLExprs::TupleListReader**
SQLExprs::ExprContext::getActiveReaderRef() {
	return activeReaderRef_;
}

inline void SQLExprs::ExprContext::setReadableTuple(
		uint32_t index, const ReadableTuple &tuple) {
	Entry &entry = entryList_[index];
	assert(entry.readableTuple_ != NULL);

	*entry.readableTuple_ = tuple;
}

inline void SQLExprs::ExprContext::setSummaryTuple(
		uint32_t index, const SummaryTuple &tuple) {
	Entry &entry = entryList_[index];
	assert(entry.readableTuple_ != NULL);

	*entry.summaryTuple_ = tuple;
}

inline bool SQLExprs::ExprContext::isAggregationNull(
		const SummaryColumn &column) {
	assert(aggrTuple_ != NULL);
	return aggrTuple_->isNull(column);
}

inline TupleValue SQLExprs::ExprContext::getAggregationValue(
		const SummaryColumn &column) {
	assert(aggrTuple_ != NULL);
	return aggrTuple_->getValue(*aggrTupleSet_, column);
}

inline void SQLExprs::ExprContext::setAggregationValue(
		const SummaryColumn &column, const TupleValue &value) {
	assert(aggrTuple_ != NULL);
	return aggrTuple_->setValue(*aggrTupleSet_, column, value);
}

inline void SQLExprs::ExprContext::setAggregationValuePromoted(
		const SummaryColumn &column, const TupleValue &value) {
	assert(aggrTuple_ != NULL);
	return aggrTuple_->setValuePromoted(*aggrTupleSet_, column, value);
}

template<typename T>
inline typename T::ValueType SQLExprs::ExprContext::getAggregationValueAs(
		const SummaryColumn &column) {
	assert(aggrTuple_ != NULL);
	return aggrTuple_->getValueAs<T>(column);
}

template<typename T>
inline void SQLExprs::ExprContext::setAggregationValueBy(
		const SummaryColumn &column, const typename T::ValueType &value) {
	assert(aggrTuple_ != NULL);
	aggrTuple_->setValueBy<T>(*aggrTupleSet_, column, value);
}

template<typename T>
inline void SQLExprs::ExprContext::incrementAggregationValueBy(
		const SummaryColumn &column) {
	assert(aggrTuple_ != NULL);
	aggrTuple_->incrementValueBy<T>(column);
}

template<typename T>
inline void SQLExprs::ExprContext::appendAggregationValueBy(
		const SummaryColumn &column,
		typename SQLValues::TypeUtils::template Traits<
				T::COLUMN_TYPE>::ReadableRefType value) {
	assert(aggrTuple_ != NULL);
	aggrTuple_->template appendValueBy<T>(*aggrTupleSet_, column, value);
}

template<typename T, typename F>
inline bool SQLExprs::ExprContext::checkDecrementalCounterAs(
		const SummaryColumn &column) {
	assert(aggrTuple_ != NULL);
	return aggrTuple_->template checkDecrementalCounterAs<T, F>(column);
}

template<typename T, typename F>
inline void SQLExprs::ExprContext::setDecrementalValueBy(
		const SummaryColumn &column, const typename T::ValueType &value) {
	assert(aggrTuple_ != NULL);
	aggrTuple_->template setDecrementalValueBy<T, F>(column, value);
}

template<typename T, typename F>
inline void SQLExprs::ExprContext::incrementDecrementalValueBy(
		const SummaryColumn &column) {
	assert(aggrTuple_ != NULL);
	aggrTuple_->template incrementDecrementalValueBy<T, F>(column);
}

template<typename T, typename F>
inline void SQLExprs::ExprContext::addDecrementalValueBy(
		const SummaryColumn &column, const typename T::ValueType &value) {
	assert(aggrTuple_ != NULL);
	aggrTuple_->template addDecrementalValueBy<T, F>(column, value);
}

inline void SQLExprs::ExprContext::initializeAggregationValues() {
	assert(aggrTuple_ != NULL);
	aggrTuple_->initializeValues(*aggrTupleSet_);
}

inline const SQLExprs::WindowState* SQLExprs::ExprContext::getWindowState() {
	return windowState_;
}

inline void SQLExprs::ExprContext::setWindowState(const WindowState *state) {
	windowState_ = state;
}


inline SQLExprs::NormalFunctionContext::NormalFunctionContext(
		SQLValues::ValueContext &valueCxt) :
		base_(NULL),
		valueCxt_(valueCxt) {
}

inline SQLExprs::ExprContext& SQLExprs::NormalFunctionContext::getBase() {
#ifndef NDEBUG
	if (base_ == NULL) {
		handleBaseNotFound();
	}
#endif
	assert(base_ != NULL);
	return *base_;
}

inline void SQLExprs::NormalFunctionContext::setBase(ExprContext *base) {
	base_ = base;
}

inline int64_t SQLExprs::NormalFunctionContext::toTimestamp(
		const util::DateTime &dateTime, const int64_t&) const {
	return SQLValues::DateTimeElements(dateTime).toTimestamp(
			SQLValues::Types::TimestampTag());
}

inline MicroTimestamp SQLExprs::NormalFunctionContext::toTimestamp(
		const util::PreciseDateTime &dateTime, const MicroTimestamp&) const {
	return SQLValues::DateTimeElements(dateTime).toTimestamp(
			SQLValues::Types::MicroTimestampTag());
}

inline NanoTimestamp SQLExprs::NormalFunctionContext::toTimestamp(
		const util::PreciseDateTime &dateTime, const NanoTimestamp&) const {
	return SQLValues::DateTimeElements(dateTime).toTimestamp(
			SQLValues::Types::NanoTimestampTag());
}

inline util::DateTime SQLExprs::NormalFunctionContext::toDateTime(
		const int64_t &tsValue) const {
	return SQLValues::DateTimeElements(tsValue).toDateTime(
			SQLValues::Types::TimestampTag());
}

inline util::PreciseDateTime SQLExprs::NormalFunctionContext::toDateTime(
		const MicroTimestamp &tsValue) const {
	return SQLValues::DateTimeElements(tsValue).toDateTime(
			SQLValues::Types::MicroTimestampTag());
}

inline util::PreciseDateTime SQLExprs::NormalFunctionContext::toDateTime(
		const NanoTimestamp &tsValue) const {
	return SQLValues::DateTimeElements(tsValue).toDateTime(
			SQLValues::Types::NanoTimestampTag());
}

inline util::DateTime::FieldType
SQLExprs::NormalFunctionContext::getTimePrecision(
		const int64_t&) const {
	return util::DateTime::FIELD_MILLISECOND;
}

inline util::DateTime::FieldType
SQLExprs::NormalFunctionContext::getTimePrecision(
		const MicroTimestamp&) const {
	return util::DateTime::FIELD_MICROSECOND;
}

inline util::DateTime::FieldType
SQLExprs::NormalFunctionContext::getTimePrecision(
		const NanoTimestamp&) const {
	return util::DateTime::FIELD_NANOSECOND;
}

#endif
