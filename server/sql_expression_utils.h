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

#ifndef SQL_EXPRESSION_UTILS_H_
#define SQL_EXPRESSION_UTILS_H_

#include "sql_expression.h"

class SQLExprs::ExprRewriter {
public:
	typedef SQLValues::SummaryColumn SummaryColumn;

	class Scope;

	explicit ExprRewriter(util::StackAllocator &alloc);

	void activateColumnMapping(ExprFactoryContext &cxt);

	void clearInputUsage();
	void addInputUsage(uint32_t input);
	void setInputUsage(uint32_t input, bool enabled);

	void clearColumnUsage();
	void addColumnUsage(const Expression *expr, bool withId);
	void addColumnUsage(const Expression &expr, bool withId);
	void addColumnUsage(uint32_t input, uint32_t column);
	void addKeyColumnUsage(
			uint32_t input, const SQLValues::CompColumnList &keyList, bool front);
	void addInputColumnUsage(uint32_t input);
	void setInputColumnUsage(uint32_t input, bool enabled);

	void setIdOfInput(uint32_t input, bool enabled);
	void setInputNull(uint32_t input, bool enabled);
	void setMappedInput(uint32_t src, uint32_t dest);

	void setInputProjected(bool enabled);
	void setInputNullProjected(bool enabled);
	void setIdProjected(bool enabled);

	uint32_t getMappedInput(uint32_t input, uint32_t column) const;
	uint32_t getMappedColumn(uint32_t input, uint32_t column) const;

	uint32_t getMappedIdInput(uint32_t input) const;
	uint32_t getMappedIdColumn(uint32_t input) const;

	util::Vector<TupleColumnType> createColumnTypeList(
			ExprFactoryContext &cxt, uint32_t input, bool unified) const;
	util::Vector<TupleColumn> createColumnList(
			ExprFactoryContext &cxt, uint32_t input, bool unified) const;
	util::Vector<SummaryColumn> createSummaryColumnList(
			ExprFactoryContext &cxt, uint32_t input, bool unified, bool first,
			const SQLValues::CompColumnList *compColumnList,
			bool orderingRestricted) const;

	static void applySummaryTupleOptions(
			ExprFactoryContext &cxt, SQLValues::SummaryTupleSet &tupleSet);
	static void applyDecrementalType(
			ExprFactoryContext &cxt, const SQLExprs::ExprCode &code);

	void setCodeSetUpAlways(bool enabled);

	void setMultiStageGrouping(bool enabled);
	void setInputMiddle(bool enabled);
	void setOutputMiddle(bool enabled);

	Expression& rewrite(
			ExprFactoryContext &cxt, const Expression &src,
			Expression *destTop) const;
	Expression& rewritePredicate(
			ExprFactoryContext &cxt, const Expression *src) const;
	SQLValues::CompColumnList& rewriteCompColumnList(
			ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
			bool unified, const uint32_t *inputRef = NULL,
			bool orderingRestricted = false) const;

	void popLastDistinctAggregationOutput(
			ExprFactoryContext &cxt, Expression &destTop) const;

	void remapColumn(ExprFactoryContext &cxt, Expression &expr) const;

	static void normalizeCompColumnList(
			SQLValues::CompColumnList &list, util::Set<uint32_t> &posSet);
	SQLValues::CompColumnList& remapCompColumnList(
			ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
			uint32_t input, bool front, bool keyOnly,
			const util::Set<uint32_t> *keyPosSet,
			bool inputMapping = false) const;

	static void createIdenticalProjection(
			ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
			Expression &dest);
	static void createEmptyRefProjection(
			ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
			uint32_t startColumn, const util::Set<uint32_t> *keySet,
			Expression &dest);
	void createProjectionByUsage(
			ExprFactoryContext &cxt, bool inputUnified,
			Expression &dest) const;

	static Expression& createConstExpr(
			ExprFactoryContext &cxt, const TupleValue &value,
			TupleColumnType type = TupleTypes::TYPE_NULL);
	static Expression& createCastExpr(
			ExprFactoryContext &cxt, Expression &baseExpr,
			TupleColumnType type);
	static Expression& createColumnExpr(
			ExprFactoryContext &cxt, uint32_t input, uint32_t column);

	static Expression& createTypedColumnExpr(
			ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
			uint32_t column);
	static Expression& createTypedColumnExprBy(
			ExprFactoryContext &cxt, uint32_t input, uint32_t column,
			TupleColumnType type);
	static Expression& createTypedIdExpr(
			ExprFactoryContext &cxt, uint32_t input);

	static Expression& createEmptyRefConstExpr(
			ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
			uint32_t column);
	static Expression& createEmptyConstExpr(
			ExprFactoryContext &cxt, TupleColumnType type);
	static Expression& createIdRefColumnExpr(
			ExprFactoryContext &cxt, uint32_t input, uint32_t column);

	static TupleColumnType getRefColumnType(
			ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
			uint32_t column);
	static TupleColumnType getIdColumnType(const ExprFactory &factory);

	static const Expression* findPredicateBySelection(
			const Expression &selectExpr);
	static const Expression& getProjectionBySelection(
			const Expression &selectExpr);

	static Expression* compColumnListToPredicate(
			ExprFactoryContext &cxt,
			const SQLValues::CompColumnList &src);

	Expression* compColumnListToKeyFilterPredicate(
			ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
			bool first, const Expression *otherPred);
	static Expression* compColumnListToKeyFilterPredicate(
			ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
			bool first);

	static Expression& replaceColumnToConstExpr(
			ExprFactoryContext &cxt, Expression &expr);

	static Expression& retainSingleInputPredicate(
			ExprFactoryContext &cxt, Expression &expr, uint32_t input,
			bool negative);
	static bool checkSingleInput(const Expression &expr, uint32_t input);

	static Expression* createExprTree(
			ExprFactoryContext &cxt, ExprType type,
			util::Vector<Expression*> &list);

	static Expression& mergePredicate(
			ExprFactoryContext &cxt,
			Expression *leftExpr, Expression *rightExpr);
	static Expression& createTruePredicate(ExprFactoryContext &cxt);
	static bool isTruePredicate(const Expression &expr);

	static bool isConstEvaluable(
			const ExprFactory &factory, ExprType type);
	static std::pair<bool, bool> isConstWithPlaceholder(
			const ExprFactory &factory, const Expression &expr);

	static TupleValue evalConstExpr(
			ExprContext &cxt, const Expression &expr);

	static bool checkArgCount(
			const ExprFactory &factory, ExprType exprType, size_t argCount,
			AggregationPhase phase, bool throwOnError);
	static bool checkArgCount(
			const ExprSpec &spec, ExprType exprType, size_t argCount,
			AggregationPhase phase, bool throwOnError);

	static uint32_t getResultCount(
			const ExprFactory &factory, ExprType exprType,
			AggregationPhase phase);
	static uint32_t getResultCount(
			const ExprSpec &spec, AggregationPhase phase);

	static void removeUnificationAttributes(Expression &expr);

	static bool findDistinctAggregation(
			const ExprFactory &factory, const Expression &expr);
	static bool isDistinctAggregation(
			const ExprFactory &factory, const ExprType type);
	static bool isDistinctAggregation(
			const ExprSpec &spec, const ExprType type);
	static bool isWindowExpr(const ExprSpec &spec, bool withSpecialArgs);
	static bool findWindowPosArgIndex(const ExprSpec &spec, uint32_t *index);

	static void getDistinctExprList(
			const ExprFactory &factory, const Expression &expr,
			util::Vector<const Expression*> &exprList);
	static Expression& toNonDistinctExpr(
			ExprFactoryContext &cxt, const Expression &src, uint32_t input,
			uint32_t *refColumnPos);
	static void addDistinctRefColumnExprs(
			ExprFactoryContext &cxt, const Expression &src, bool forAdvance,
			bool refEmpty, uint32_t input, uint32_t *refColumnPos,
			Expression::ModIterator &destIt);
	static void replaceDistinctExprsToRef(
			ExprFactoryContext &cxt, Expression &expr, bool forAdvance,
			int32_t emptySide, const util::Set<uint32_t> *keySet,
			uint32_t *restDistinctCount, uint32_t *refColumnPos);

	static bool findVarGenerativeExpr(const Expression &expr);

	static SQLValues::CompColumnList& reduceDuplicateCompColumns(
			util::StackAllocator &alloc,
			const SQLValues::CompColumnList &srcList);
	static util::Set<uint32_t>& compColumnListToSet(
			util::StackAllocator &alloc,
			const SQLValues::CompColumnList &srcList);

	static bool predicateToExtContainerName(
			SQLValues::ValueContext &cxt, const ExprFactory &factory,
			const Expression *pred, uint32_t dbNameColumn,
			uint32_t containerNameColumn, uint32_t partitionNameColumn,
			TupleValue *dbName, TupleValue &containerName,
			std::pair<TupleValue, TupleValue> *containerNameElems,
			bool &placeholderAffected);
	static bool predicateToContainerId(
			SQLValues::ValueContext &cxt, const ExprFactory &factory,
			const Expression *pred, uint32_t partitionIdColumn,
			uint32_t containerIdColumn, PartitionId &partitionId,
			ContainerId &containerId, bool &placeholderAffected);

private:
	typedef util::Vector<bool> Usage;
	typedef util::Vector<Usage> UsageList;
	typedef std::pair<int32_t, int32_t> ColumnMapElem;
	typedef util::Vector<ColumnMapElem> ColumnMapEntry;
	typedef util::Vector<ColumnMapEntry> ColumnMap;
	typedef util::Vector<int32_t> InputMap;
	typedef util::Vector<int32_t> KeyMap;
	typedef util::Vector<Expression*> ExprRefList;

	struct ScopedEntry {
		explicit ScopedEntry(util::StackAllocator &alloc);

		bool columnMapping_;
		bool inputProjected_;
		bool inputNullProjected_;
		bool idProjected_;

		bool columnMapUpdated_;
		ColumnMap columnMap_;
		ColumnMap idColumnMap_;

		InputMap inputMap_;

		Usage inputUsage_;
		UsageList columnUsage_;
		Usage idUsage_;
		Usage inputNullList_;

		bool codeSetUpAlways_;

		bool multiStageGrouping_;
		bool inputMiddle_;
		bool outputMiddle_;
		bool multiStageKeyWithDigest_;
		KeyMap multiStageKeyMap_;
		uint32_t multiStageKeyCount_;

		ExprRefList distinctAggrMainList_;
		ExprRefList distinctAggrSubList_;
		Expression *distinctAggrsExpr_;
		uint32_t distinctAggrColumnCount_;
		bool distinctAggrFound_;
	};

	static Expression& duplicate(
			ExprFactoryContext &cxt, const Expression &src,
			Expression *destTop);

	bool isColumnMappingRequired() const;

	void remapColumnSub(ExprFactoryContext &cxt, Expression &expr) const;
	bool assignRemappedCode(
			ExprFactoryContext &cxt, uint32_t input, uint32_t column,
			bool forId, bool inputUnified, ExprCode &code,
			bool throwOnError) const;

	const ColumnMapElem& resolveColumnMapElement(
			uint32_t input, uint32_t column, bool forId) const;
	const ColumnMapElem* findColumnMapElement(
			uint32_t input, uint32_t column, bool forId) const;

	const ColumnMap& getColumnMap(bool forId) const;
	uint32_t resolveMappedInput(uint32_t src) const;

	bool isIdUsed(uint32_t input) const;

	ScopedEntry& getColumnMappingEntry();
	const ScopedEntry& getColumnMappingEntry() const;

	bool isCodeSetUpRequired(const Expression &src) const;

	uint32_t prepareTopAttributes(
			const Expression &expr, bool forProj, bool top) const;

	void setUpCodes(
			ExprFactoryContext &cxt, Expression &expr, const size_t *projDepth,
			uint32_t topAttributes, bool insideAggrFunc) const;
	void setUpCodesAt(
			ExprFactoryContext &cxt, Expression &expr, const size_t *projDepth,
			uint32_t topAttributes, bool insideAggrFunc) const;

	static uint32_t resolveAttributesAt(
			const Expression &expr, const size_t *projDepth,
			uint32_t topAttributes, bool insideAggrFunc);
	TupleColumnType resolveColumnTypeAt(
			ExprFactoryContext &cxt, const Expression &expr,
			const size_t *projDepth, uint32_t topAttributes,
			bool insideAggrFunc) const;

	static TypeResolverResult resolveBasicExprColumnTypes(
			const Expression &expr, const ExprSpec &spec,
			AggregationPhase aggrPhase, bool grouping, bool checking);
	static TypeResolverResult resolveNonDistinctExprColumnTypes(
			const ExprFactory &factory, const Expression &src,
			ExprType *destExprType);

	static void optimizeExpr(ExprFactoryContext &cxt, Expression &expr);
	static void optimizeCompExpr(ExprFactoryContext &cxt, Expression &expr);
	static void optimizeOrExpr(ExprFactoryContext &cxt, Expression &expr);

	static void swapExprArgs(Expression &expr);
	static bool isSameExpr(
			const ExprFactory &factory, const Expression &expr1,
			const Expression &expr2, bool excludesDynamic);
	static bool isSameExprCode(
			const ExprCode &code1, const ExprCode &code2);
	template<typename T> static bool checkNumericBounds(
			const TupleValue &value);

	bool isAggregationSetUpRequired(
			ExprFactoryContext &cxt, const Expression &src,
			bool forProjection) const;
	void setUpAggregation(ExprFactoryContext &cxt, Expression &expr) const;
	static bool isFinishAggregationArranging(ExprFactoryContext &cxt);

	void setUpAdvanceAggregation(
			ExprFactoryContext &cxt, Expression &expr,
			const ExprCode &topCode) const;
	void setUpMergeAggregation(
			ExprFactoryContext &cxt, Expression &expr,
			const ExprCode &topCode) const;
	void setUpAggregationColumnsAt(
			ExprFactoryContext &cxt, Expression &expr,
			const ExprCode &topCode) const;

	void setUpPipeAggregation(
			ExprFactoryContext &cxt, Expression &expr,
			Expression::ModIterator *destIt) const;
	void setUpFinishAggregation(
			ExprFactoryContext &cxt, Expression &expr,
			const ExprCode &topCode) const;

	void setUpDistinctPipeAggregation(
			ExprFactoryContext &cxt, const Expression &expr) const;
	void setUpDistinctFinishAggregation(
			ExprFactoryContext &cxt, Expression &expr) const;
	static Expression& createDistinctAggregationIdExpr(
			ExprFactoryContext &cxt);

	void prepareMultiStageGrouping(ExprFactoryContext &cxt) const;
	void setUpMiddleFinishAggregation(
			ExprFactoryContext &cxt, Expression &expr) const;
	void remapMultiStageGroupColumns(
			ExprFactoryContext &cxt, Expression &expr, bool forFinish) const;
	bool matchMultiStageGroupKey(
			const Expression &expr, bool *acceptable) const;

	void prepareDistinctAggregation() const;

	void replaceChildToAggrFirst(
			ExprFactoryContext &cxt, Expression::ModIterator &it) const;
	void replaceChildToAdvanceOutput(
			ExprFactoryContext &cxt, Expression::ModIterator &it) const;
	void replaceChildToDistinctFinishOutput(
			ExprFactoryContext &cxt, Expression::ModIterator &it) const;
	void replaceChildToNull(
			ExprFactoryContext &cxt, Expression::ModIterator &it) const;

	Expression& replaceParentToPlanning(
			ExprFactoryContext &cxt, Expression &parentExpr) const;
	static void setAggregationArranged(Expression &expr);

	util::StackAllocator &alloc_;
	ScopedEntry localEntry_;
	ScopedEntry *entry_;
};

class SQLExprs::ExprRewriter::Scope {
public:
	explicit Scope(ExprRewriter &rewriter);
	~Scope();

private:
	ExprRewriter &rewriter_;
	ScopedEntry *prevEntry_;
	ScopedEntry localEntry_;
};

class SQLExprs::SyntaxExprRewriter {
public:
	SyntaxExprRewriter(
			SQLValues::ValueContext &valueCxt,
			const util::Vector<TupleValue> *parameterList);

	Expression* toExpr(const SyntaxExpr *src) const;
	Expression& toColumnList(const SyntaxExpr *src, bool columnSingle) const;
	Expression& toColumnList(
			const SyntaxExprList &src, bool columnSingle) const;
	Expression& toProjection(const SyntaxExprList &src) const;
	Expression& toSelection(
			const SyntaxExprList &srcProj, const SyntaxExpr *srcPred) const;

	SyntaxExpr* toSyntaxExpr(const Expression &src) const;
	SyntaxExprList toSyntaxExprList(const Expression &src) const;

	Expression& makeExpr(ExprType type, const SyntaxExprList *argList) const;

	bool findAdjustedRangeValues(
			const SyntaxExpr &expr,
			const util::Vector<TupleColumnType> &columnTypeList,
			const util::Vector<TupleValue> *parameterList,
			SQLExprs::IndexCondition &cond) const;

	static TupleValue evalConstExpr(
			ExprContext &cxt, const ExprFactory &factory,
			const SyntaxExpr &expr);
	static void checkExprArgs(
			ExprContext &cxt, const ExprFactory &factory,
			const SyntaxExpr &expr);

private:
	static Expression* toColumnListElement(Expression *src, bool columnSingle);

	SQLValues::ValueContext &valueCxt_;
	const util::Vector<TupleValue> *parameterList_;
};

class SQLExprs::TypeResolver {
public:
	enum {
		RET_TYPE_LIST_SIZE = ExprSpec::AGGR_LIST_SIZE
	};

	typedef TypeResolverResult ResultInfo;

	TypeResolver(
			const ExprSpec &spec, AggregationPhase aggrPhase, bool grouping);

	void next(TupleColumnType type);
	ResultInfo complete(bool checking);

	int32_t getMinArgCount() const;
	int32_t getMaxArgCount() const;
	uint32_t getResultCount() const;

private:
	typedef SQLValues::TypeUtils TypeUtils;

	enum {
		INHERITANCE_LIST_SIZE = 2
	};

	bool isError() const;
	bool convertPromotedType();
	void acceptLast(bool tail);

	const ExprSpec::In* findIn(size_t index, bool tail) const;
	const ExprSpec::In* findLast() const;
	bool hasInheritableResult() const;

	const ExprSpec::In* getInList(size_t &size) const;
	TupleColumnType findResult(size_t index) const;
	bool isResultNonNullable(size_t index) const;

	uint32_t getInFlags(const ExprSpec::In &in) const;
	static uint32_t resolveSpecFlags(
			const ExprSpec &spec, AggregationPhase aggrPhase);

	bool isAdvanceAggregation() const;
	bool isMergeAggregation() const;

	bool isDistinctAggregation() const;

	const ExprSpec &spec_;
	const AggregationPhase aggrPhase_;
	const bool grouping_;
	const uint32_t flags_;

	size_t nextIndex_;
	TupleColumnType lastType_;
	TupleColumnType promotedType_;
	TupleColumnType inheritanceList_[INHERITANCE_LIST_SIZE];
	bool inNullable_;
	int32_t conversionError_;
	int32_t promotionError_;
};

struct SQLExprs::TypeResolverResult {
	TypeResolverResult();
	TupleColumnType typeList_[TypeResolver::RET_TYPE_LIST_SIZE];
};

struct SQLExprs::IndexSpec {
	typedef const uint32_t *ColumnsIterator;

	IndexSpec();

	bool operator==(const IndexSpec &another) const;
	bool operator<(const IndexSpec &another) const;

	int32_t compareTo(const IndexSpec &another) const;

	size_t getColumnCount() const;
	uint32_t getColumn(size_t index) const;
	ColumnsIterator columnsBegin() const;
	ColumnsIterator columnsEnd() const;

	const uint32_t *columnList_;
	size_t columnCount_;
	uint32_t indexFlags_;
	bool available_;
};

struct SQLExprs::IndexCondition {
	typedef uint64_t IndexSpecId;

	IndexCondition();

	static IndexCondition createNull();
	static IndexCondition createBool(bool value);

	bool isEmpty() const;
	bool isNull() const;
	bool equals(bool value) const;
	bool isStatic() const;

	bool isAndTop() const;
	bool isBulkTop() const;

	bool isBinded() const;

	static const uint32_t EMPTY_IN_COLUMN;

	uint32_t column_;
	SQLExprs::ExprType opType_;
	std::pair<TupleValue, TupleValue> valuePair_;
	std::pair<bool, bool> inclusivePair_;
	std::pair<uint32_t, uint32_t> inColumnPair_;

	size_t andCount_;
	size_t andOrdinal_;
	size_t compositeAndCount_;
	size_t compositeAndOrdinal_;
	size_t bulkOrCount_;
	size_t bulkOrOrdinal_;

	IndexSpecId specId_;
	IndexSpec spec_;

	bool firstHitOnly_;
	bool descending_;
};

class SQLExprs::IndexSelector {
public:
	typedef IndexCondition Condition;
	typedef IndexConditionList ConditionList;
	typedef uint32_t IndexFlags;
	typedef Condition::IndexSpecId IndexSpecId;

	IndexSelector(
			SQLValues::ValueContext &valueCxt, ExprType columnExprType,
			const TupleList::Info &info);
	~IndexSelector();

	void clearIndex();
	void addIndex(uint32_t column, IndexFlags flags);
	void addIndex(
			const util::Vector<uint32_t> &columnList,
			const util::Vector<IndexFlags> &flagsList);
	void completeIndex();

	void setMultiAndConditionEnabled(bool enabled);
	void setBulkGrouping(bool enabled);

	bool matchIndexList(const int32_t *indexList, size_t size) const;

	bool updateIndex(size_t condIndex);
	bool findIndex(size_t condIndex) const;
	SQLType::IndexType getIndexType(const Condition &cond) const;

	IndexSpecId getMaxSpecId() const;
	size_t getIndexColumnList(
			IndexSpecId specId, const uint32_t *&list) const;

	void select(const Expression &expr);
	void selectDetail(const Expression *proj, const Expression *pred);

	bool isSelected() const;
	bool isComplex() const;

	const ConditionList& getConditionList() const;
	ConditionList& getConditionList();

	void getIndexColumnList(
			const Condition &cond, util::Vector<uint32_t> &columnList) const;

	bool bindCondition(
			Condition &cond,
			const TupleValue &value1, const TupleValue &value2,
			SQLValues::ValueSetHolder &valuesHolder) const;

	bool isPlaceholderAffected() const;

	static size_t getOrConditionCount(
			ConditionList::const_iterator beginIt,
			ConditionList::const_iterator endIt);
	static size_t nextOrConditionDistance(
			ConditionList::const_iterator it,
			ConditionList::const_iterator endIt);
	static size_t nextCompositeConditionDistance(
			ConditionList::const_iterator beginIt,
			ConditionList::const_iterator endIt);
	template<typename It, typename Accessor>
	static size_t nextCompositeConditionDistance(
			It beginIt, It endIt, const Accessor &accessor);
	static size_t nextBulkConditionDistance(
			ConditionList::const_iterator beginIt,
			ConditionList::const_iterator endIt);

private:
	struct IndexFlagsEntry;
	struct IndexMatch;

	struct BulkPosition;
	struct BulkRewriter;
	struct BulkConditionLess;

	struct AggregationMatcher;

	typedef util::AllocVector<IndexSpec> SpecList;

	typedef std::pair<uint32_t, IndexSpecId> ColumnSpecId;
	typedef util::AllocVector<ColumnSpecId> ColumnSpecIdList;

	typedef util::Vector<IndexMatch> IndexMatchList;
	typedef util::Vector<uint64_t> ConditionOrdinalList;

	template<typename T>
	struct PairFirst { typedef typename T::first_type Type; };
	template<typename T>
	struct PairFirst<const T> { typedef const typename T::first_type Type; };

	struct Range {
	public:
		size_t first() const { return static_cast<size_t>(base_.first); }
		size_t second() const { return static_cast<size_t>(base_.second); }

		void first(size_t value) { base_.first = value; }
		void second(size_t value) { base_.second = value; }

	private:
		std::pair<uint64_t, uint64_t> base_;
	};

	struct ConditionAccessor {
		const Condition& operator()(ConditionList::const_iterator it) const {
			return *it;
		}
	};

	IndexSelector(const IndexSelector&);
	IndexSelector& operator=(const IndexSelector&);

	void selectSub(const Expression &expr, bool negative, bool inAndCond);
	void selectSubAnd(
			const Expression &expr, Range *simpleRangeRef,
			bool &complexLeading, bool negative);

	void narrowSimpleCondition(
			Range &simpleRange, size_t offset, bool atAndTop);
	void narrowComplexCondition(
			size_t firstOffset, size_t secondOffset, bool firstLeading);

	void assignAndOrdinals(Range &totalRange, bool atAndTop);

	size_t matchAndConditions(
			Range &totalRange, size_t acceptedCondCount);
	size_t arrangeMatchedAndConditions(
			Range &totalRange, size_t acceptedCondCount,
			ConditionOrdinalList &ordinalList, IndexSpecId specId);

	static void moveTailConditionToFront(
			ConditionList::iterator beginIt, ConditionList::iterator tailIt);

	static Condition makeNarrower(
			const Condition &cond1, const Condition &cond2);
	static bool tryMakeNarrower(
			const Condition &cond1, const Condition &cond2,
			Condition &retCond);

	static Condition makeRangeNarrower(
			const Condition &cond1, const Condition &cond2);
	static Condition toRangeCondition(const Condition &cond);

	size_t moveTailConditionList(size_t destOffset, size_t srcOffset);
	size_t eraseEmptyConditionList(size_t beginOffset, size_t endOffset);

	void reduceConditionList(size_t offset);

	size_t getOrConditionCount(size_t beginOffset, size_t endOffset) const;

	Condition makeValueCondition(
			const Expression &expr, bool negative, bool &placeholderAffected,
			SQLValues::ValueSetHolder &valuesHolder,
			bool topColumnOnly = false, bool treeIndexExtended = false) const;
	bool applyValue(
			Condition &cond, const TupleValue *value, uint32_t inColumn,
			SQLValues::ValueSetHolder &valuesHolder, bool rangePreferred) const;

	static bool isRangeConditionType(ExprType type);

	static void applyBoolBounds(Condition &cond, bool rangePreferred);

	template<TupleColumnType T>
	static void applyIntegralBounds(Condition &cond, bool rangePreferred);
	static bool applyIntegralBounds(
			Condition &cond, int64_t min, int64_t max, bool rangePreferred,
			int64_t &value);

	template<TupleColumnType T>
	static void applyFloatingBounds(Condition &cond);

	static void applyTimestampBounds(
			Condition &cond, TupleColumnType columnType,
			SQLValues::ValueSetHolder &valuesHolder);

	bool selectClosestIndex(
			ConditionList::const_iterator condBegin,
			ConditionList::const_iterator condEnd,
			size_t acceptedCondCount, size_t &condOrdinal,
			IndexSpecId &specId);
	bool reduceIndexMatch(
			ConditionList::const_iterator condBegin,
			ConditionList::const_iterator condEnd,
			size_t acceptedCondCount, IndexMatchList &matchList,
			size_t columnOrdinal) const;

	bool selectClosestIndexColumn(
			ConditionList::const_iterator condBegin,
			ConditionList::const_iterator condEnd,
			size_t acceptedCondCount, size_t condOrdinal, uint32_t column,
			IndexSpecId specId, size_t &nextCondOrdinal) const;
	bool matchIndexColumn(
			const Condition &cond, uint32_t column, IndexSpecId specId) const;

	bool getAvailableIndex(
			const Condition &cond, bool withSpec, SQLType::IndexType *indexType,
			IndexSpecId *specId, IndexMatchList *matchList,
			bool topColumnOnly = false, bool treeIndexExtended = false) const;

	bool findColumn(
			const Expression &expr, uint32_t &column, bool forScan) const;
	static const TupleValue* findConst(const Expression &expr);

	static bool isTrueValue(
			const TupleValue &value, bool negative, bool &nullFound);

	IndexSpec createIndexSpec(const util::Vector<uint32_t> &columnList);
	void clearIndexColumns();

	template<typename Pair>
	static typename PairFirst<Pair>::Type& getPairElement(
			size_t ordinal, Pair &src);

	static int32_t compareValue(
			const TupleValue &value1, const TupleValue &value2);

	util::StdAllocator<void, void> stdAlloc_;
	VarContext &varCxt_;
	SQLValues::ValueSetHolder localValuesHolder_;

	ExprType columnExprType_;
	util::Vector<IndexFlagsEntry> indexList_;
	util::AllocVector<util::AllocVector<uint32_t>*> allColumnsList_;
	SQLValues::ColumnTypeList tupleInfo_;
	ConditionList condList_;

	IndexMatchList indexMatchList_;
	ConditionOrdinalList condOrdinalList_;

	SpecList specList_;
	ColumnSpecIdList columnSpecIdList_;

	bool indexAvailable_;
	bool placeholderAffected_;
	bool multiAndConditionEnabled_;
	bool bulkGrouping_;
	bool complexReordering_;

	bool completed_;
	bool cleared_;

	const ExprFactory *exprFactory_;
};

struct SQLExprs::IndexSelector::IndexFlagsEntry {
public:
	enum IndexFlagsType {
		FLAGS_TYPE_SINGLE,
		FLAGS_TYPE_COMPOSITE_ALL,
		FLAGS_TYPE_COMPOSITE_TOP,
		END_FLAGS_TYPE
	};

	IndexFlagsEntry();

	IndexFlags get(IndexFlagsType type) const;
	void add(IndexFlagsType type, IndexFlags flags);

private:
	typedef uint16_t FlagsElement;
	FlagsElement elems_[END_FLAGS_TYPE];
};

struct SQLExprs::IndexSelector::IndexMatch {
	IndexMatch();
	bool operator<(const IndexMatch &another) const;

	IndexSpecId specId_;
	const IndexSpec *spec_;
	size_t condOrdinal_;

	uint64_t penalty_;
	bool completed_;
};

struct SQLExprs::IndexSelector::BulkPosition {
public:
	explicit BulkPosition(size_t pos);

	size_t get() const;

	bool operator<(const BulkPosition &another) const;

private:
	size_t pos_;
};

struct SQLExprs::IndexSelector::BulkRewriter {
public:
	BulkRewriter();

	void rewrite(ConditionList &condList) const;

	static int32_t comparePosition(
			const BulkPosition &pos1, const BulkPosition &pos2);
	static int32_t compareBulkCondition(
			const ConditionList &condList, const BulkPosition &pos1,
			const BulkPosition &pos2);

private:
	typedef util::Vector<BulkPosition> PositionList;

	typedef std::pair<BulkPosition, BulkPosition> PositionPair;
	typedef util::Vector<PositionPair> PositionPairList;

	static void generateInitialPositions(
			const ConditionList &condList, PositionList &posList);
	static void arrangeBulkPositions(
			const ConditionList &condList, PositionList &posList);
	static void applyBulkPositions(
			const PositionList &posList, ConditionList &condList);

	static int32_t compareBulkTarget(
			const ConditionList &condList, const BulkPosition &pos1,
			const BulkPosition &pos2);
	static int32_t compareBulkSpec(
			const ConditionList &condList, const BulkPosition &pos1,
			const BulkPosition &pos2);
	static int32_t compareBulkValue(
			const ConditionList &condList, const BulkPosition &pos1,
			const BulkPosition &pos2);
	static int32_t compareUIntValue(uint64_t value1, uint64_t value2);

	static bool isBulkTarget(
			const ConditionList &condList, const BulkPosition &pos);
	static size_t getBulkUnitSize(
			const ConditionList &condList, const BulkPosition &pos);

	static void fillGroupOrdinals(
			ConditionList &condList, size_t offset);
	static size_t getGroupPositionCount(
			const ConditionList &condList, const PositionList &posList,
			size_t offset);
	static bool isSameGroup(
			const ConditionList &condList, const BulkPosition &pos1,
			const BulkPosition &pos2);

	static const Condition& getCondition(
			const ConditionList &condList, const BulkPosition &pos);
	static const Condition& getCondition(
			const ConditionList &condList, size_t pos);

	template<typename T>
	static util::StackAllocator& getAllocator(util::Vector<T> &src);
};

struct SQLExprs::IndexSelector::BulkConditionLess {
public:
	explicit BulkConditionLess(const ConditionList &condList);
	bool operator()(const BulkPosition &pos1, const BulkPosition &pos2) const;

private:
	const ConditionList &condList_;
};

struct SQLExprs::IndexSelector::AggregationMatcher {
public:
	AggregationMatcher(
			const util::StdAllocator<void, void> &alloc,
			const IndexSelector &base,
			SQLValues::ValueSetHolder &valuesHolder);

	bool match(
			const Expression &proj, const Expression *pred,
			IndexConditionList &condList);

private:
	typedef util::AllocVector<IndexCondition> AllocIndexConditionList;

	typedef std::pair<uint32_t, bool> AggregationKey;
	typedef util::AllocSet<AggregationKey> AggregationKeySet;

	bool matchProjection(
			const Expression &expr, AllocIndexConditionList &condList,
			AggregationKeySet &keySet, bool top);
	bool matchPredicate(
			const Expression &expr, AllocIndexConditionList &condList,
			bool negative);

	bool matchAggregationExpr(
			const Expression &expr, AllocIndexConditionList &condList,
			AggregationKeySet &keySet);
	bool matchAggregationType(
			const Expression &expr, bool &descending);

	bool matchValueCondition(
			const Expression &expr, AllocIndexConditionList &condList,
			bool negative);

	util::StdAllocator<void, void> matcherAlloc_;
	const IndexSelector &base_;
	SQLValues::ValueSetHolder &valuesHolder_;
};

struct SQLExprs::ExprTypeUtils {
	static bool isAggregation(ExprType type);
	static bool isFunction(ExprType type);

	static bool isNoColumnTyped(ExprType type);
	static bool isCompOp(ExprType type, bool withNe);

	static bool isNormalWindow(ExprType type);
	static bool isDecremental(ExprType type);

	static ExprType swapCompOp(ExprType type);
	static ExprType negateCompOp(ExprType type);
	static ExprType getLogicalOp(ExprType type, bool negative);

	static SQLValues::CompColumn toCompColumn(
			ExprType type, uint32_t pos1, uint32_t pos2, bool last);
	static ExprType getCompColumnOp(const SQLValues::CompColumn &column);

	static bool isVarNonGenerative(
			ExprType type, bool forLob, bool forLargeFixed);
};

struct SQLExprs::PlanPartitioningInfo {
	typedef util::Vector<int64_t> CondensedPartitionIdList;
	typedef std::pair<int64_t, int64_t> IntervalEntry;
	typedef util::Vector<IntervalEntry> IntervalList;

	PlanPartitioningInfo();

	SyntaxTree::TablePartitionType partitioningType_;
	int32_t partitioningColumnId_;
	int32_t subPartitioningColumnId_;

	uint32_t partitioningCount_;
	uint32_t clusterPartitionCount_;
	int64_t intervalValue_;
	const CondensedPartitionIdList *nodeAffinityList_;
	const IntervalList *availableList_;
};

struct SQLExprs::PlanNarrowingKey {
	typedef std::pair<int64_t, int64_t> LongRange;

	PlanNarrowingKey();

	void setNone();
	void setRange(const LongRange &longRange);
	void setHash(uint32_t hashIndex, uint32_t hashCount);

	static LongRange getRangeFull();
	static LongRange getRangeNone();

	LongRange longRange_;
	uint32_t hashIndex_;
	uint32_t hashCount_;
};

struct SQLExprs::DataPartitionUtils {
	static int64_t intervalToAffinity(
			const PlanPartitioningInfo &partitioning, int64_t interval,
			uint32_t hash);
	static std::pair<int64_t, uint32_t> intervalHashFromAffinity(
			const PlanPartitioningInfo &partitioning,
			const util::Vector<uint32_t> &affinityRevList, int64_t affinity);
	static bool isFixedIntervalPartitionAffinity(
			const PlanPartitioningInfo &partitioning, int64_t affinity);
	static void makeAffinityRevList(
			const PlanPartitioningInfo &partitioning,
			util::Vector<uint32_t> &affinityRevList);

	static bool reducePartitionedTarget(
			SQLValues::ValueContext &cxt, bool forContainer,
			const PlanPartitioningInfo *partitioning,
			const util::Vector<TupleColumnType> &columnTypeList,
			const SyntaxExpr &expr, bool &uncovered,
			util::Vector<int64_t> *affinityList, util::Set<int64_t> &affinitySet,
			const util::Vector<TupleValue> *parameterList,
			bool &placeholderAffected, util::Set<int64_t> &unmatchAffinitySet);

	static bool isReducibleTablePartitionCondition(
			const PlanPartitioningInfo &partitioning, TupleColumnType columnType,
			const util::Vector<uint32_t> &affinityRevList, int64_t nodeAffinity,
			ExprType condType, uint32_t condColumn, const TupleValue &condValue);

	static void getTablePartitionKey(
			const PlanPartitioningInfo &partitioning,
			const util::Vector<uint32_t> &affinityRevList, int64_t nodeAffinity,
			PlanNarrowingKey &key, PlanNarrowingKey &subKey, bool &subFound);

	static int64_t intervalToRaw(int64_t interval);
	static int64_t intervalFromRaw(int64_t rawInterval);

	static int64_t rawIntervalFromValue(const TupleValue &value, int64_t unit);
	static int64_t rawIntervalFromValue(int64_t longValue, int64_t unit);

	static std::pair<int64_t, int64_t> rawIntervalToLong(
			int64_t rawInterval, int64_t unit);
	static int64_t intervalValueToLong(const TupleValue &value);

	static int64_t intervalToAffinity(
			SyntaxTree::TablePartitionType partitioningType,
			uint32_t partitioningCount, uint32_t clusterPartitionCount,
			const util::Vector<int64_t> &affinityList,
			int64_t interval, uint32_t hash);

	static std::pair<int64_t, uint32_t> intervalHashFromAffinity(
			SyntaxTree::TablePartitionType partitioningType,
			uint32_t partitioningCount, uint32_t clusterPartitionCount,
			const util::Vector<uint32_t> &affinityRevList, int64_t affinity);

	static bool isFixedIntervalPartitionAffinity(
			SyntaxTree::TablePartitionType partitioningType,
			uint32_t clusterPartitionCount, int64_t affinity);

	static void makeAffinityRevList(
			SyntaxTree::TablePartitionType partitioningType,
			uint32_t partitioningCount, uint32_t clusterPartitionCount,
			const util::Vector<int64_t> &affinityList,
			util::Vector<uint32_t> &affinityRevList);
};

struct SQLExprs::RangeGroupUtils {
	static bool isRangeGroupSupportedType(
			TupleColumnType type);

	static void resolveRangeGroupInterval(
			TupleColumnType type, int64_t baseInterval, int64_t baseOffset,
			util::DateTime::FieldType unit, const util::TimeZone &timeZone,
			RangeKey &interval, RangeKey &offset);
	static RangeKey resolveRangeGroupIntervalValue(
			int64_t baseInterval, util::DateTime::FieldType unit, bool forInterval);
	static void adjustRangeGroupInterval(
			RangeKey &interval, RangeKey &offset, const util::TimeZone &timeZone);

	static void checkRangeGroupInterval(
			TupleColumnType type, const RangeKey &interval);
	static RangeKey getRangeGroupMinInterval(TupleColumnType type);

	static bool findRangeGroupBoundary(
			util::StackAllocator &alloc, TupleColumnType keyType,
			const TupleValue &base, bool forLower, bool inclusive,
			RangeKey &boundary);
	static bool adjustRangeGroupBoundary(
			RangeKey &lower, RangeKey &upper, const RangeKey &interval,
			const RangeKey &offset);
	static TupleValue mergeRangeGroupBoundary(
			const TupleValue &base1, const TupleValue &base2, bool forLower);

	static bool getRangeGroupId(
			const TupleValue &key, const RangeKey &interval,
			const RangeKey &offset, RangeKey &id);
};

#endif
