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

#ifndef SQL_OPERATOR_UTILS_H_
#define SQL_OPERATOR_UTILS_H_

#include "sql_operator.h"
#include "sql_expression_utils.h"
#include "sql_expression_base.h"

class SQLExecutionManager;

class ResourceSet;

struct SQLOpUtils {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::ReadableTuple ReadableTuple;
	typedef SQLOps::WritableTuple WritableTuple;

	typedef SQLOps::TupleColumn TupleColumn;

	typedef SQLOps::SummaryColumn SummaryColumn;
	typedef SQLOps::SummaryTuple SummaryTuple;
	typedef SQLOps::SummaryTupleSet SummaryTupleSet;

	typedef SQLOps::DigestTupleListReader DigestTupleListReader;
	typedef SQLOps::DigestReadableTuple DigestReadableTuple;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::ProjectionCode ProjectionCode;

	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;
	typedef SQLOps::Projection Projection;

	typedef std::pair<Projection*, Projection*> ProjectionPair;
	typedef std::pair<const Projection*, const Projection*> ProjectionRefPair;

	typedef util::Vector<const SQLExprs::Expression*> ExprRefList;

	struct AnalysisInfo;
	struct AnalysisRootInfo;
	struct AnalysisIndexCondition;
	struct AnalysisIndexInfo;

	class CustomOpProjectionRegistrar;
	class ExpressionWriter;
	class ExpressionListWriter;

	class NonExecutableProjection;
};

class SQLOps::ExtOpContext : public SQLValues::ExtValueContext {
public:
	virtual void checkCancelRequest() = 0;
	virtual void transfer(TupleList::Block &block, uint32_t id) = 0;
	virtual void finishRootOp() = 0;

	virtual const Event* getEvent() = 0;
	virtual EventContext* getEventContext() = 0;
	virtual SQLExecutionManager* getExecutionManager() = 0;

	virtual const ResourceSet* getResourceSet() = 0;

	virtual bool isOnTransactionService() = 0;
	virtual double getStoreMemoryAgingSwapRate() = 0;
	virtual void getConfig(OpConfig &config) = 0;

	virtual uint32_t getTotalWorkerId() = 0;
};

class SQLOps::OpCodeBuilder {
public:
	typedef SQLOpUtils::ExprRefList ExprRefList;

	struct Source;

	explicit OpCodeBuilder(const Source &source);

	static Source ofAllocator(util::StackAllocator &alloc);
	static Source ofContext(OpContext &cxt);

	ProjectionFactoryContext& getProjectionFactoryContext();
	SQLExprs::ExprFactoryContext& getExprFactoryContext();
	SQLExprs::ExprRewriter& getExprRewriter();

	void applyJoinType(SQLType::JoinType type);
	static void applyJoinTypeTo(
			SQLType::JoinType type, SQLExprs::ExprFactoryContext &cxt);

	OpConfig& createConfig();

	ContainerLocation& createContainerLocation();
	SQLExprs::IndexConditionList& createIndexConditionList();

	SQLValues::CompColumnList& createCompColumnList();

	Projection& createProjection(
			SQLOpTypes::ProjectionType projType,
			const ProjectionCode &code = ProjectionCode());
	Expression& createExpression(
			SQLExprs::ExprType exprType,
			const SQLExprs::ExprCode &code = SQLExprs::ExprCode());

	Expression& createConstExpr(const TupleValue &value);
	Expression& createColumnExpr(uint32_t input, uint32_t column);

	Projection& toNormalProjectionByExpr(
			const Expression *src, SQLType::AggregationPhase aggrPhase);
	Projection& toGroupingProjectionByExpr(
			const Expression *src, SQLType::AggregationPhase aggrPhase,
			bool forWindow);

	Projection& createIdenticalProjection(bool unified, uint32_t index);
	Projection& createEmptyRefProjection(
			bool unified, uint32_t index, uint32_t startColumn);
	Projection& createProjectionByUsage(bool unified);

	Projection& rewriteProjection(
			const Projection &src, Projection **distinctProj = NULL);

	Projection& createFilteredProjection(Projection &src, OpCode &code);
	Projection& createFilteredProjection(
			Projection &src, const Expression *filterPred);
	Projection& createKeyFilteredProjection(Projection &src);

	Projection& createLimitedProjection(Projection &src, OpCode &code);

	Projection& createMultiOutputProjection(
			Projection &proj1, Projection &proj2);
	Projection& createPipeFinishProjection(
			Projection &pipe, Projection &finish);
	Projection& createMultiStageProjection(Projection &pipe, Projection &mid);

	Projection& createMultiStageAggregation(
			const Projection &src, int32_t stage, bool forFinish,
			const SQLValues::CompColumnList *keyList);

	std::pair<Projection*, Projection*> arrangeProjections(
			OpCode &code, bool withFilter, bool withLimit,
			Projection **distinctProj);
	SQLExprs::Expression& arrangePredicate(const SQLExprs::Expression &src);

	static void setUpOutputNodes(
			OpPlan &plan, OpNode *outNode, OpNode &inNode,
			const Projection *distinctProj,
			SQLType::AggregationPhase aggrPhase);

	OpCode toExecutable(const OpCode &src);
	OpCode rewriteCode(const OpCode &src, bool projectionArranging);

	void applySummaryAggregationColumns(const Projection &proj);
	static void applyAggregationColumns(
			const Projection &aggrProj, SummaryTupleSet &tupleSet);

	void addColumnUsage(const OpCode &code, bool withId);

	void assignInputTypesByProjection(
			const Projection &proj, const uint32_t *outIndex);

	static bool isInputUnified(const OpCode &code);

	static const Expression* findFilterPredicate(const Projection &src);

	static const Projection* findOutputProjection(
			const OpCode &code, const uint32_t *outIndex);
	static const Projection* findOutputProjection(
			const Projection &src, const uint32_t *outIndex);
	static Projection* findOutputProjection(
			Projection &src, const uint32_t *outIndex);

	static bool matchOutputProjectionAt(
			const Projection &proj, const uint32_t *outIndex);

	static uint32_t resolveOutputIndex(const Projection &proj);
	static uint32_t resolveOutputIndex(const SQLExprs::ExprCode &code);

	static const Projection* findAggregationProjection(const Projection &src);

	bool findDistinctAggregation(const Projection &src);
	static bool findDistinctAggregation(
			const SQLExprs::ExprFactory &factory, const Projection &src);

	static bool isAggregationArrangementRequired(
			const Projection *pipeProj, const Projection *finishProj);

	static void resolveColumnTypeList(
			const OpCode &code, ColumnTypeList &typeList,
			const uint32_t *outIndex);
	static void resolveColumnTypeList(
			const Projection &proj, ColumnTypeList &typeList,
			const uint32_t *outIndex);

	static uint32_t resolveOutputCount(const OpCode &code);
	static uint32_t resolveOutputCount(const Projection &proj);

	static void removeUnificationAttributes(Projection &proj);

	ExprRefList getDistinctExprList(const Projection &proj);

	SQLValues::CompColumnList& createDistinctGroupKeyList(
			const ExprRefList &distinctList, size_t index, bool idGrouping);
	Projection& createDistinctGroupProjection(
			const ExprRefList &distinctList, size_t index, bool idGrouping,
			SQLType::AggregationPhase aggrPhase);

	SQLValues::CompColumnList& createDistinctMergeKeyList();
	Projection& createDistinctMergeProjection(
			const ExprRefList &distinctList, size_t index,
			const Projection &proj, SQLType::AggregationPhase aggrPhase);
	Projection& createDistinctMergeProjectionAt(
			const ExprRefList &distinctList, size_t index,
			const Projection &proj, SQLType::AggregationPhase aggrPhase,
			int32_t emptySide);

private:
	typedef SQLOpUtils::ProjectionPair ProjectionPair;
	typedef SQLOpUtils::ProjectionRefPair ProjectionRefPair;

	void setUpExprFactoryContext(OpContext *cxt);

	util::StackAllocator& getAllocator();

	const ProjectionFactory& getProjectionFactory();
	const SQLExprs::ExprFactory& getExprFactory();

	ProjectionFactoryContext factoryCxt_;
	SQLExprs::ExprRewriter rewriter_;
};

struct SQLOps::OpCodeBuilder::Source {
	explicit Source(util::StackAllocator &alloc);

	util::StackAllocator &alloc_;
	OpContext *cxt_;
};

class SQLOps::OpLatchKeeperManager {
public:
	class KeeperId {
	private:
		friend class OpLatchKeeperManager;
		SQLValues::SharedId id_;
	};

	OpLatchKeeperManager(
			const util::StdAllocator<void, void> &alloc, bool composite);
	~OpLatchKeeperManager();

	OpLatchKeeperManager& getSubManager(OpContext &cxt);

	KeeperId create(uint64_t maxSize, bool hot);
	void release(const KeeperId &id);

	bool tryAcquire(const KeeperId &id);
	void adjust(const KeeperId &id, uint64_t size);

private:
	struct Entry {
		Entry();

		KeeperId id_;
		bool hot_;
		bool acquired_;
		uint64_t size_;
	};

	struct Usage {
		Usage();

		uint64_t used_;
		uint64_t limit_;
	};

	typedef util::AllocVector<OpLatchKeeperManager*> ManagerList;
	typedef util::AllocVector<Entry> EntryList;
	typedef util::AllocDeque<SQLValues::SharedId> IdQueue;

	util::StdAllocator<void, void> alloc_;
	bool composite_;
	ManagerList subList_;

	SQLValues::SharedIdManager idManager_;
	EntryList entryList_;
	IdQueue waiterQueue_;

	Usage usage_;
	Usage coldUsage_;
	uint64_t hotCount_;

	util::LocalUniquePtr<util::Mutex> mutex_;
};

class SQLOps::OpLatchKeeper {
public:
	OpLatchKeeper(OpLatchKeeperManager &manager, uint64_t maxSize, bool hot);
	~OpLatchKeeper();

	bool tryAcquire();
	void adjust(uint64_t size);

private:
	typedef OpLatchKeeperManager::KeeperId KeeperId;

	OpLatchKeeper(const OpLatchKeeper&);
	OpLatchKeeper& operator=(const OpLatchKeeper&);

	KeeperId id_;
	OpLatchKeeperManager &manager_;
};

class SQLOps::OpProfilerId {
private:
	friend class OpProfiler;
	SQLValues::SharedId id_;
};

class SQLOps::OpProfiler {
	typedef SQLOpUtils::AnalysisInfo AnalysisInfo;
	typedef SQLOpUtils::AnalysisIndexCondition AnalysisIndexCondition;
	typedef SQLOpUtils::AnalysisIndexInfo AnalysisIndexInfo;
public:
	struct LocalInfo;
	struct LocalIndexInfo;

	typedef util::AllocMap<SQLOpTypes::Type, OpProfilerId> SubMap;

	typedef util::AllocVector<LocalIndexInfo> LocalIndexInfoList;
	typedef util::AllocVector<LocalInfo> LocalInfoList;

	typedef util::AllocVector<util::AllocString> NameList;
	typedef util::AllocVector<util::AllocString*> NameRefList;
	typedef util::AllocVector<AnalysisIndexInfo> IndexList;
	typedef util::AllocVector<AnalysisIndexCondition> IndexCondList;

	explicit OpProfiler(const util::StdAllocator<void, void> &alloc);

	OpProfilerId getSubId(const OpProfilerId *id, SQLOpTypes::Type type);

	void addOperatorProfile(const OpProfilerId &id, const AnalysisInfo &info);
	void addIndexProfile(
			const OpProfilerId &id, const AnalysisIndexInfo &info);

	AnalysisInfo getAnalysisInfo(const OpProfilerId *id);

	static AnalysisInfo getAnalysisInfo(OpProfiler *profiler, LocalInfo &src);
	static AnalysisIndexInfo getAnalysisIndexInfo(LocalIndexInfo &src);

	static uint64_t nanoTimeToMillis(uint64_t nanoTime);

private:
	LocalInfo& createInfo(SQLOpTypes::Type type);

	util::StdAllocator<void, void> alloc_;
	SQLValues::SharedIdManager idManager_;
	LocalInfoList infoList_;
};

struct SQLOps::OpProfiler::LocalInfo {
	explicit LocalInfo(const util::StdAllocator<void, void> &alloc);

	OpProfilerId id_;

	LocalIndexInfoList index_;
	SubMap sub_;

	IndexList indexRef_;
	util::AllocVector<AnalysisInfo> subRef_;
	util::AllocVector<AnalysisInfo> ref_;
};

struct SQLOps::OpProfiler::LocalIndexInfo {
	explicit LocalIndexInfo(const util::StdAllocator<void, void> &alloc);

	util::AllocString name_;
	NameList columnNameList_;
	NameList conditionColumnList_;

	NameRefList columnNameListRef_;
	IndexCondList conditionListRef_;
	util::AllocVector<AnalysisIndexInfo> ref_;
};

class SQLOps::OpProfilerEntry {
	typedef OpProfiler::LocalInfo LocalInfo;
	typedef OpProfiler::LocalIndexInfo LocalIndexInfo;
	typedef SQLOpUtils::AnalysisInfo AnalysisInfo;
public:
	OpProfilerEntry(
			const util::StdAllocator<void, void> &alloc, const OpProfilerId &id);

	void startExec();
	void finishExec();

	const OpProfilerId& getId();
	AnalysisInfo get();

	OpProfilerIndexEntry& getIndexEntry();

private:
	util::Stopwatch executionWatch_;
	int64_t executionCount_;

	util::StdAllocator<void, void> alloc_;
	OpProfiler::LocalInfo localInfo_;

	util::AllocUniquePtr<OpProfilerIndexEntry> index_;
};

class SQLOps::OpProfilerIndexEntry {
	typedef OpProfiler::LocalIndexInfo LocalIndexInfo;
	typedef SQLOpUtils::AnalysisIndexCondition AnalysisIndexCondition;
	typedef SQLOpUtils::AnalysisIndexInfo AnalysisIndexInfo;
public:
	OpProfilerIndexEntry(
			const util::StdAllocator<void, void> &alloc,
			LocalIndexInfo *localInfo);

	bool isEmpty();

	const char8_t* findIndexName();
	void setIndexName(const char8_t *name);

	const char8_t* findColumnName(uint32_t column);
	void setColumnName(uint32_t column, const char8_t *name);

	void addIndexColumn(uint32_t column);
	void addCondition(const SQLExprs::IndexCondition &cond);

	void startSearch();
	void finishSearch();

	void update();

private:
	typedef util::AllocMap<uint32_t, util::AllocString> ColumnNameMap;

	OpProfilerIndexEntry(const OpProfilerIndexEntry&);
	OpProfilerIndexEntry& operator=(const OpProfilerIndexEntry&);

	util::Stopwatch searchWatch_;
	int64_t executionCount_;

	LocalIndexInfo *localInfo_;

	util::StdAllocator<void, void> alloc_;
	ColumnNameMap nameMap_;
};

struct SQLOpUtils::AnalysisInfo {
	AnalysisInfo();

	AnalysisRootInfo toRoot() const;

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_ENUM(type_, OP_TYPE_CODER),
			UTIL_OBJECT_CODER_OPTIONAL(opCount_, -1),
			executionCount_,
			actualTime_,
			index_,
			sub_);

	static const util::NameCoderEntry<SQLOpTypes::Type> OP_TYPE_LIST[];
	static const util::NameCoder<
			SQLOpTypes::Type, SQLOpTypes::END_OP> OP_TYPE_CODER;

	SQLOpTypes::Type type_;
	int64_t opCount_;
	int64_t executionCount_;
	int64_t actualTime_;

	util::AllocVector<AnalysisIndexInfo> *index_;
	util::AllocVector<AnalysisInfo> *sub_;

	uint64_t actualNanoTime_;
};

struct SQLOpUtils::AnalysisRootInfo {
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	AnalysisRootInfo();

	UTIL_OBJECT_CODER_MEMBERS(sub_);

	util::AllocVector<AnalysisInfo> *sub_;
};

struct SQLOpUtils::AnalysisIndexCondition {
	enum ConditionType {
		CONDITION_RANGE,
		CONDITION_EQ,

		END_CONDITION
	};

	AnalysisIndexCondition();

	UTIL_OBJECT_CODER_MEMBERS(
			column_,
			UTIL_OBJECT_CODER_ENUM(condition_, CONDITION_CODER));

	static const util::NameCoderEntry<ConditionType> CONDITION_LIST[];
	static const util::NameCoder<ConditionType, END_CONDITION> CONDITION_CODER;

	util::AllocString *column_;
	ConditionType condition_;
};

struct SQLOpUtils::AnalysisIndexInfo {
	AnalysisIndexInfo();

	UTIL_OBJECT_CODER_MEMBERS(
			name_,
			columnList_,
			conditionList_,
			executionCount_,
			actualTime_);

	util::AllocString *name_;
	util::AllocVector<util::AllocString*> *columnList_;
	util::AllocVector<AnalysisIndexCondition> *conditionList_;
	int64_t executionCount_;
	int64_t actualTime_;
};

class SQLOpUtils::CustomOpProjectionRegistrar :
		public SQLOps::OpProjectionRegistrar {
public:
	struct VariantTraits;

	CustomOpProjectionRegistrar() throw();

	explicit CustomOpProjectionRegistrar(
			const CustomOpProjectionRegistrar &sub) throw();

protected:
	template<SQLOpTypes::ProjectionType T, typename P>
	void addProjectionVariants() const;
};

struct SQLOpUtils::CustomOpProjectionRegistrar::VariantTraits  {
	typedef ProjectionFactoryContext ContextType;
	typedef ProjectionCode CodeType;
	typedef Projection ObjectType;

	template<typename V>
	static Projection& create(
			ProjectionFactoryContext &cxt, const ProjectionCode &code) {
		return *(ALLOC_NEW(cxt.getAllocator()) V(cxt, code));
	}
};

class SQLOpUtils::ExpressionWriter {
private:
	typedef SQLValues::TypeSwitcher TypeSwitcher;

	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprContext ExprContext;

	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	template<typename T>
	class ByReader {
	public:
		explicit ByReader(const ExpressionWriter &base) : base_(base) {
		}
		void operator()() const;

	private:
		const ExpressionWriter &base_;
	};

	template<typename T>
	class ByReaderMulti {
	public:
		explicit ByReaderMulti(const ExpressionWriter &base) : base_(base) {
		}
		void operator()() const;

	private:
		const ExpressionWriter &base_;
	};

	template<typename T>
	class ByReadableTuple {
	public:
		explicit ByReadableTuple(const ExpressionWriter &base) : base_(base) {
		}
		void operator()() const;

	private:
		const ExpressionWriter &base_;
	};

	template<typename T>
	class BySummaryTuple {
	public:
		explicit BySummaryTuple(const ExpressionWriter &base) : base_(base) {
		}
		void operator()() const;

	private:
		const ExpressionWriter &base_;
	};

	template<typename T>
	class BySummaryTupleBody {
	public:
		explicit BySummaryTupleBody(const ExpressionWriter &base) : base_(base) {
		}
		void operator()() const;

	private:
		const ExpressionWriter &base_;
	};

	template<typename T>
	class ByGeneral {
	public:
		explicit ByGeneral(const ExpressionWriter &base) : base_(base) {
		}
		void operator()() const;

	private:
		const ExpressionWriter &base_;
	};

public:
	struct ContextRef {
		ContextRef();

		OpContext *cxt_;
	};

	template<typename T>
	struct TypeAt {
		typedef typename T::VariantTraitsType::template SubTypeOf<T>::Type TypedOp;
	};

	ExpressionWriter();

	void initialize(
			ExprFactoryContext &cxt, const Expression &expr,
			const TupleColumn &outColumn, ContextRef *cxtRef,
			TupleListReader **readerRef, WritableTuple *outTuple);

	void setExpression(const Expression &expr);

	void write() const;

	bool isForColumn() const;
	bool isSummaryColumn() const;

	bool matchIdColumn() const;
	bool matchCompColumn(
			const SQLValues::CompColumn &compColumn) const;

	TupleColumn getOutColumn() const;
	const SummaryColumn* getInSummaryColumn() const;

private:
	template<SQLExprs::ExprCode::InputSourceType S>
	struct VariantTraits {
		typedef VariantTraits OfExpressionWriter;

		static const SQLExprs::ExprCode::InputSourceType SOURCE_TYPE = S;

		enum {
			FOR_COLUMN = (SOURCE_TYPE != SQLExprs::ExprCode::END_INPUT)
		};

		template<typename T>
		struct SubTypeOf {
			typedef
					typename util::Conditional<
							SOURCE_TYPE == SQLExprs::ExprCode::INPUT_READER,
							ByReader<T>,
					typename util::Conditional<
							SOURCE_TYPE == SQLExprs::ExprCode::INPUT_READER_MULTI,
							ByReaderMulti<T>,
					typename util::Conditional<
							SOURCE_TYPE == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE,
							BySummaryTuple<T>,
					typename util::Conditional<
							SOURCE_TYPE == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY,
							BySummaryTupleBody<T>,
					typename util::Conditional<
							!FOR_COLUMN,
							ByGeneral<T>, void>::Type>::Type>::Type>::Type>::Type Type;
		};
	};

	struct Switcher {
		typedef TypeSwitcher::OpTraitsOptions<
				Switcher> PlainOptionsType;
		typedef PlainOptionsType::DelegationRebind<
				true>::Type DefaultOptionsType;

		typedef TypeSwitcher::OpTraits<
				void, DefaultOptionsType> DefaultTraitsType;
		typedef DefaultTraitsType::Func<
				const ExpressionWriter>::Type DefaultFuncType;

		template<typename V>
		struct ExpressionWriterRebind {
			typedef V Type;
		};

		template<typename Base, SQLExprs::ExprCode::InputSourceType S>
		struct OpTraitsAt {
			typedef VariantTraits<S> SubTraitsType;
			typedef typename Base::VariantTraitsType::
					template ExpressionWriterRebind<
							SubTraitsType>::Type VariantTraitsType;
			typedef typename Base::template VariantRebind<
					VariantTraitsType>::Type Type;
		};

		explicit Switcher(const ExpressionWriter &base) : base_(base) {}

		template<typename Op, typename Traits>
		typename Traits::template Func<Op>::Type getWith() const;

		template<typename Op, typename Traits, SQLExprs::ExprCode::InputSourceType S>
		typename Traits::template Func<Op>::Type getSubWith() const;

		const ExpressionWriter &base_;
	};

	template<typename T>
	void writeAt() const;

	template<typename Op>
	Switcher::DefaultFuncType resolveFunc() const;

	Switcher::DefaultFuncType func_;

	TupleListReader *reader_;
	TupleListReader **readerRef_;
	ReadableTuple *inTuple_;
	SummaryTuple *inSummaryTuple_;

	TupleColumn inColumn_;
	const SummaryColumn *inSummaryColumn_;

	WritableTuple *outTuple_;
	TupleColumn outColumn_;

	const Expression *expr_;

	ContextRef *cxtRef_;
	SQLExprs::ExprCode::InputSourceType srcType_;
};

class SQLOpUtils::ExpressionListWriter {
private:
	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

public:
	class Source;

	class ByProjection;
	class ByGeneral;
	class ByDigestTuple;

	explicit ExpressionListWriter(const Source &source);

	void setKeyColumnList(
			const SQLValues::CompColumnList &keyColumnList,
			bool forMiddle, bool longKeyOnly, bool nullIgnorable);
	void setTupleWriter(TupleListWriter *writer);

	void applyProjection(const Projection &proj) const;

	void applyContext(OpContext &cxt) const;
	void applyReader(TupleListReader *reader) const;
	void applyTuple(const ReadableTuple &tuple) const;
	void applyTuple(const SummaryTuple &tuple) const;

	void write() const;
	void writeBy(const DigestTupleListReader &reader) const;
	void writeBy(const SummaryTuple &tuple) const;

	bool isAvailable() const;
	bool isColumnExprFound() const;
	bool isDigestColumnAssigned() const;

	bool isDigestColumnAscending() const;
	TupleColumnType getDigestColumnType() const;

private:
	ExpressionListWriter(const ExpressionListWriter&);
	ExpressionListWriter& operator=(const ExpressionListWriter&);

	typedef SQLOpUtils::ExpressionWriter ExpressionWriter;
	typedef util::Vector<SQLOpUtils::ExpressionWriter> WriterList;

	WriterList writerListBase_;
	WriterList &writerList_;
	bool columExprFound_;
	bool digestColumnAssigned_;

	TupleListReader **activeReaderRef_;
	TupleListWriter *writer_;
	ExpressionWriter::ContextRef cxtRefBase_;
	ExpressionWriter::ContextRef &cxtRef_;

	ReadableTuple *inTupleRef_;
	SummaryTuple *inSummaryTupleRef_;

	WritableTuple outTupleBase_;
	WritableTuple &outTuple_;

	TupleColumn digestColumn_;
	SummaryColumn headSummaryColumn_;
	bool digestColumnAscending_;
};

class SQLOpUtils::ExpressionListWriter::Source {
public:
	Source(
			ProjectionFactoryContext &factoryCxt,
			const ProjectionCode &code);

	Source(
			OpContext &cxt, const Projection &proj, bool forMiddle,
			const SQLValues::CompColumnList *keyColumnList,
			bool nullIgnorable, const Projection *inMiddleProj,
			SQLExprs::ExprCode::InputSourceType srcType);

	void setOutput(uint32_t index);

	ProjectionFactoryContext& getFactoryContext() const;
	const ProjectionCode& getCode() const;

	void setUp(ExpressionListWriter &writer) const;

private:
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	struct Entry {
		Entry();

		ProjectionFactoryContext *factoryCxt_;
		const ProjectionCode *code_;

		OpContext *cxt_;
		const Projection *proj_;
		bool forMiddle_;
		bool longKeyOnly_;
		bool nullIgnorable_;
		const SQLValues::CompColumnList *keyColumnList_;
		uint32_t outIndex_;
	};

	Entry entry_;
	util::LocalUniquePtr<OpCodeBuilder> builder_;
};

class SQLOpUtils::ExpressionListWriter::ByProjection {
public:
	typedef Projection BaseType;

	explicit ByProjection(const BaseType &base) : base_(base) {
	}

	template<typename>
	const Projection& projectorAt() const {
		return base_;
	}

private:
	const BaseType &base_;
};

class SQLOpUtils::ExpressionListWriter::ByGeneral {
public:
	typedef ExpressionListWriter BaseType;

	explicit ByGeneral(const BaseType &base) : base_(base) {
	}

	void project(OpContext &cxt) const;
	void projectBy(OpContext &cxt, const DigestTupleListReader &reader) const;
	void projectBy(OpContext &cxt, const SummaryTuple &tuple) const;

	template<typename>
	const ByGeneral& projectorAt() const {
		return *this;
	}

private:
	const BaseType &base_;
};

class SQLOpUtils::ExpressionListWriter::ByDigestTuple {
public:
	typedef ExpressionListWriter BaseType;

	template<typename T>
	class TypeAt {
	public:
		explicit TypeAt(const BaseType &base) : base_(base) {
		}
		void projectBy(OpContext &cxt, const DigestTupleListReader &reader) const;
		void projectBy(OpContext &cxt, const SummaryTuple &tuple) const;

	private:
		const BaseType &base_;
	};

	explicit ByDigestTuple(const ExpressionListWriter &base) : base_(base) {
	}

	template<typename T>
	TypeAt<T> projectorAt() const {
		return TypeAt<T>(base_);
	}

private:
	const BaseType &base_;
};

class SQLOpUtils::NonExecutableProjection : public SQLOps::Projection {
public:
	virtual void project(OpContext &cxt) const;
};



template<SQLOpTypes::ProjectionType T, typename P>
void SQLOpUtils::CustomOpProjectionRegistrar::addProjectionVariants() const {
	typedef typename SQLExprs::template VariantsRegistrarBase<
			VariantTraits> Base;
	typedef typename SQLOps::ProjectionFactory::FactoryFunc FactoryFunc;
	FactoryFunc func = Base::template add<T, P>();
	addProjectionDirect(T, func);
}


inline void SQLOpUtils::ExpressionWriter::write() const {
	func_(*this);
}


inline void SQLOpUtils::ExpressionListWriter::applyContext(
		OpContext &cxt) const {
	cxtRef_.cxt_ = &cxt;
}

inline void SQLOpUtils::ExpressionListWriter::applyReader(
		TupleListReader *reader) const {
	*activeReaderRef_ = reader;
}

inline void SQLOpUtils::ExpressionListWriter::applyTuple(
		const ReadableTuple &tuple) const {
	*inTupleRef_ = tuple;
}

inline void SQLOpUtils::ExpressionListWriter::applyTuple(
		const SummaryTuple &tuple) const {
	*inSummaryTupleRef_ = tuple;
}

inline void SQLOpUtils::ExpressionListWriter::write() const {
	assert(writer_ != NULL);
	writer_->next();
	outTuple_ = writer_->get();

	for (WriterList::const_iterator it = writerList_.begin();
			it != writerList_.end(); ++it) {
		it->write();
	}
}

inline void SQLOpUtils::ExpressionListWriter::writeBy(
		const DigestTupleListReader &reader) const {
	applyReader(reader.getReader());
	write();

	if (digestColumnAssigned_) {
		assert(digestColumn_.getType() == TupleTypes::TYPE_LONG);
		SQLValues::ValueUtils::writeValue<SQLValues::Types::Long>(
				outTuple_, digestColumn_, reader.getDigest());
	}
}

inline void SQLOpUtils::ExpressionListWriter::writeBy(
		const SummaryTuple &tuple) const {
	applyTuple(tuple.getTuple());
	write();

	if (digestColumnAssigned_) {
		assert(digestColumn_.getType() == TupleTypes::TYPE_LONG);
		SQLValues::ValueUtils::writeValue<SQLValues::Types::Long>(
				outTuple_, digestColumn_, tuple.getDigest());
	}
}

inline bool SQLOpUtils::ExpressionListWriter::isColumnExprFound() const {
	return columExprFound_;
}


inline void SQLOpUtils::ExpressionListWriter::ByGeneral::project(
		OpContext &cxt) const {
	base_.applyContext(cxt);
	base_.write();
	assert(!base_.digestColumnAssigned_);
}

inline void SQLOpUtils::ExpressionListWriter::ByGeneral::projectBy(
		OpContext &cxt, const DigestTupleListReader &reader) const {
	base_.applyContext(cxt);
	base_.applyReader(reader.getReader());
	base_.write();
	assert(!base_.digestColumnAssigned_);
}

inline void SQLOpUtils::ExpressionListWriter::ByGeneral::projectBy(
		OpContext &cxt, const SummaryTuple &tuple) const {
	base_.applyContext(cxt);
	base_.applyTuple(tuple.getTuple());
	base_.write();
	assert(!base_.digestColumnAssigned_);
}


template<typename T> inline
void SQLOpUtils::ExpressionListWriter::ByDigestTuple::TypeAt<T>::projectBy(
		OpContext &cxt, const DigestTupleListReader &reader) const {
	static_cast<void>(cxt);

	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	const bool reversed =
			T::VariantTraitsType::OfValueComparator::ORDERING_REVERSED;
	typedef typename util::BoolType<!reversed>::Result Ascending;

	assert(base_.writer_ != NULL);
	base_.writer_->next();
	WritableTuple outTuple = base_.writer_->get();

	const bool digestWritable =
			!SQLValues::TypeUtils::Traits<TypeTag::COLUMN_TYPE>::FOR_SEQUENTIAL;
	if (digestWritable) {
		assert(base_.isDigestColumnAssigned());
		assert(T::COLUMN_TYPE ==
				SQLValues::TypeUtils::toNonNullable(base_.getDigestColumnType()));

		typedef typename util::Conditional<
				digestWritable, TypeTag, SQLValues::Types::Any>::Type FixedTypeTag;
		ValueUtils::writeValue<FixedTypeTag>(
				outTuple, base_.digestColumn_,
				ValueUtils::toValueByOrdredDigest<FixedTypeTag, Ascending>(
						reader.getDigest()));
	}
	else {
		assert(!base_.isDigestColumnAssigned());
	}

	if (!base_.writerList_.empty()) {
		base_.applyReader(reader.getReader());
		base_.outTuple_ = outTuple;
		typename WriterList::const_iterator it = base_.writerList_.begin();
		do {
			it->write();
		}
		while (++it != base_.writerList_.end());
	}
}

template<typename T> inline
void SQLOpUtils::ExpressionListWriter::ByDigestTuple::TypeAt<T>::projectBy(
		OpContext &cxt, const SummaryTuple &tuple) const {
	static_cast<void>(cxt);

	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	const bool reversed =
			T::VariantTraitsType::OfValueComparator::ORDERING_REVERSED;
	typedef typename util::BoolType<!reversed>::Result Ascending;

	assert(base_.writer_ != NULL);
	base_.writer_->next();
	WritableTuple outTuple = base_.writer_->get();

	const bool digestWritable =
			!SQLValues::TypeUtils::Traits<TypeTag::COLUMN_TYPE>::FOR_MULTI_VAR;
	if (digestWritable) {
		assert(base_.isDigestColumnAssigned());
		assert(T::COLUMN_TYPE ==
				SQLValues::TypeUtils::toNonNullable(base_.getDigestColumnType()));

		if (T::NullableType::VALUE &&
				tuple.isNull(base_.headSummaryColumn_)) {
			ValueUtils::writeNull(outTuple, base_.digestColumn_);
		}
		else {
			ValueUtils::writeValue<TypeTag>(
					outTuple, base_.digestColumn_,
					tuple.getHeadValueAs<TypeTag, Ascending>(
							base_.headSummaryColumn_));
		}
	}
	else {
		assert(!base_.isDigestColumnAssigned());
	}

	if (!base_.writerList_.empty()) {
		base_.applyTuple(tuple.getTuple());
		base_.outTuple_ = outTuple;
		typename WriterList::const_iterator it = base_.writerList_.begin();
		do {
			it->write();
		}
		while (++it != base_.writerList_.end());
	}
}

#endif
