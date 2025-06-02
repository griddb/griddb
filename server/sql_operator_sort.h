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

#ifndef SQL_OPERATOR_SORT_H_
#define SQL_OPERATOR_SORT_H_

#include "sql_operator.h"

#include "sql_operator_utils.h"
#include "sql_utils_algorithm.h"

struct SQLSortOps {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::ReadableTuple ReadableTuple;
	typedef SQLOps::WritableTuple WritableTuple;

	typedef SQLOps::DigestTupleListReader DigestTupleListReader;
	typedef SQLOps::DigestReadableTuple DigestReadableTuple;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::OpConfig OpConfig;

	typedef SQLValues::TupleNullChecker::WithAccessor<
			SQLValues::ValueAccessor::ByReader> TupleNullChecker;

	typedef SQLValues::TupleDigester::WithAccessor<
			SQLValues::ValueAccessor::ByReader, 2, true> TupleDigester;

	typedef SQLValues::SummaryTuple SummaryTuple;
	typedef SQLValues::SummaryTupleSet SummaryTupleSet;

	typedef SQLValues::TupleComparator::WithAccessor<
			std::less<SQLValues::ValueComparator::PredArgType>,
			false, false, false, true,
			SQLValues::ValueAccessor::BySummaryTuple> TupleLess;
	typedef SQLAlgorithmUtils::Sorter<SummaryTuple, TupleLess> TupleSorter;

	typedef SQLValues::ReadableTupleRef::WithDigester<
			TupleDigester> ReadableTupleRef;
	typedef SQLValues::TupleComparator::WithAccessor<
			std::less<SQLValues::ValueComparator::PredArgType>, true, false,
			false,
			true, SQLValues::ValueAccessor::ByReader> TupleGreater;
	typedef SQLAlgorithmUtils::HeapQueue<
			ReadableTupleRef, TupleGreater> TupleHeapQueue;
	typedef util::Vector<TupleHeapQueue::Element> TupleHeapRefList;

	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpNode OpNode;
	typedef SQLOps::OpPlan OpPlan;

	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef SQLOps::Projection Projection;

	typedef SQLOpUtils::NonExecutableProjection NonExecutableProjection;
	typedef SQLOpUtils::ProjectionRefPair ProjectionRefPair;

	class Registrar;

	class Sort;
	class SortNway;

	struct SortStageElement;
	typedef util::Vector<SortStageElement> SortStage;
	typedef util::Vector<SortStage> SortStageList;

	struct SorterEntry;
	struct SortContext;

	class Window;
	class WindowPartition;
	class WindowRankPartition;
	class WindowFramePartition;
	class WindowMerge;
};

class SQLSortOps::Registrar : public SQLOps::OpProjectionRegistrar {
public:
	virtual void operator()() const;

private:
	static const SQLOps::OpRegistrar REGISTRAR_INSTANCE;
};

class SQLSortOps::Sort : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	static Projection& createSubLimitedProjection(
			SQLOps::OpCodeBuilder &builder, Projection &src, OpCode &code);
	static const SQLValues::CompColumnList& toSubLimitedKeyList(
			SQLOps::OpCodeBuilder &builder,
			const SQLValues::CompColumnList &src);
};

class SQLSortOps::SortNway : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

private:
	class SorterBuilder;
	class SorterWriter;
	template<bool Fixed, bool Limited, bool Rotating> struct MergeAction;

	struct GroupSortAction;
	template<typename InputUnique> struct GroupMergeAction;

	bool partialSort(SortContext &cxt) const;
	bool sortMerge(SortContext &cxt) const;
	bool nextStage(SortContext &cxt) const;

	TupleSorter& prepareSorter(
			SortContext &cxt, const SQLValues::CompColumnList &keyColumnList,
			TupleListReader &reader) const;
	TupleHeapQueue createHeapQueue(
			SortContext &cxt, bool primary,
			const SQLValues::CompColumnList &keyColumnList,
			TupleHeapRefList &refList) const;

	static TupleNullChecker createSorterNullChecker(
			util::StackAllocator &alloc,
			const SQLValues::CompColumnList &totalKeys,
			SQLValues::CompColumnList &nullCheckerKeys);
	static TupleDigester createSorterDigester(
			util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
			const SQLValues::CompColumnList &keyColumnList,
			const TupleNullChecker &nullChecker, bool unique, bool primary,
			SQLValues::ValueProfile *profile);
	static bool isHashAvailable(
			const SQLValues::CompColumnList &keyColumnList, bool unique);
	static bool isPredicateEmpty(
			SortContext &cxt, const SQLValues::TupleComparator &basePred,
			const SQLValues::CompColumnList &keyColumnList, bool primary);

	bool isMergeCompleted(SortContext &cxt) const;

	ProjectionRefPair prepareSortProjection(
			SortContext &cxt, bool primary) const;
	ProjectionRefPair prepareMergeProjection(
			SortContext &cxt, bool primary) const;

	ProjectionRefPair findProjection(bool merging, bool forFinal) const;

	bool isGrouping(bool merging, bool forFinal) const;
	bool isDuplicateGroupMerging() const;
	bool isUnique() const;
	bool isOrderedUnique() const;
	bool isKeyFiltering() const;

	bool isKeyEmpty(TupleSorter &sorter) const;
	static bool isKeyEmpty(TupleSorter &sorter, bool keyFiltering);

	void updateProjections(
			SortContext &cxt, const ProjectionRefPair &projectionPair) const;
	void prepareProjectionOutput(SortContext &cxt, bool primary) const;

	bool isSorting(SortContext &cxt) const;
	bool isMerging(SortContext &cxt) const;
	bool isFinalStage(SortContext &cxt) const;

	bool isPrimaryWorking(
			size_t ordinal,
			const SQLValues::CompColumnList &keyColumnList) const;

	void finishMerging(SortContext &cxt, bool primary) const;

	uint32_t prepareStageElement(
			SortContext &cxt, SortStageElement &elem, bool primary) const;
	void finishStageElement(SortContext &cxt, SortStageElement &elem) const;
	void releaseStageElement(
			SortContext &cxt, SortStageElement &elem,
			const bool *primaryRef) const;

	SortContext& getSortContext(OpContext &cxt) const;

	bool checkSorterBuilding(SortContext &cxt, uint32_t inIndex) const;
	bool checkMemoryLimitReached(
			SortContext &cxt, TupleListReader &reader) const;

	size_t getSorterCapacity(util::StackAllocator &alloc) const;
	size_t getMergeWidth() const;

	static const Projection& resolveOutputProjection(const Projection &src);

	bool sortMergeDetail(
			SortContext &cxt, TupleHeapQueue &queue,
			const ProjectionRefPair &projectionPair,
			TupleHeapRefList &refList) const;
	bool sortMergeDetail(
			SortContext &cxt, TupleHeapQueue &queue,
			SQLOpUtils::ExpressionListWriter &writer) const;

	bool sortOptional(SortContext &cxt, TupleSorter &sorter) const;
};

class SQLSortOps::SortNway::SorterBuilder {
public:
	class Normal {
	public:
		typedef void RetType;

		template<typename T> class TypeAt {
		public:
			explicit TypeAt(const Normal &base) : base_(base.base_) {}

			void operator()() {
				base_.buildAt<T>();
			}

		private:
			SorterBuilder &base_;
		};

		explicit Normal(SorterBuilder &base) : base_(base) {}

	private:
		SorterBuilder &base_;
	};

	class Unique {
	public:
		typedef void RetType;

		template<typename T> class TypeAt {
		public:
			explicit TypeAt(const Unique &base) : base_(base.base_) {}

			void operator()() {
				base_.uniqueBuildAt<T>();
			}

		private:
			SorterBuilder &base_;
		};

		explicit Unique(SorterBuilder &base) : base_(base) {}

	private:
		SorterBuilder &base_;
	};

	class Grouping {
	public:
		typedef void RetType;

		template<typename T> class TypeAt {
		public:
			explicit TypeAt(const Grouping &base) : base_(base.base_) {}

			void operator()() {
				base_.groupBuildAt<T>();
			}

		private:
			SorterBuilder &base_;
		};

		explicit Grouping(SorterBuilder &base) : base_(base) {}

	private:
		SorterBuilder &base_;
	};

	SorterBuilder(
			SortContext &cxt, TupleSorter &sorter, TupleListReader &reader,
			const SQLValues::CompColumnList &keyColumnList, bool keyFiltering,
			const Projection *pipeProj, const Projection *mergeProj);
	~SorterBuilder();

	bool build(SortContext &cxt);

private:
	typedef SQLValues::TupleComparator::WithAccessor<
			std::equal_to<SQLValues::ValueComparator::PredArgType>,
			false, false, false, false,
			SQLValues::ValueAccessor::BySummaryTuple> TupleEq;

	template<typename T> void buildAt();
	template<typename T> void uniqueBuildAt();
	template<typename T> void groupBuildAt();

	bool checkEmpty();

	OpContext &cxt_;

	TupleSorter &sorter_;
	TupleListReader &reader_;
	TupleNullChecker &nullChecker_;
	const TupleDigester &digester1_;
	const TupleDigester &digester2_;
	bool keyFiltering_;
	bool withAny_;
	SummaryTupleSet &tupleSet1_;
	SummaryTupleSet &tupleSet2_;
	uint64_t nextInterruption_;

	TupleEq tupleEq_;
	size_t hashSize_;
	uint64_t hashUnit_;
	SummaryTuple *hashTable_;
	size_t hashConflictionLimit_;

	SummaryTuple *inTupleRef_;
	SummaryTuple *outTupleRef_;
	const Projection *pipeProj_;
	const Projection *mergeProj_;

	TupleDigester::SwitcherType::PlainOpBase<
			const Normal>::FuncType normalFunc_;
	TupleDigester::SwitcherType::PlainOpBase<
			const Unique>::FuncType uniqueFunc_;
	TupleDigester::SwitcherType::PlainOpBase<
			const Grouping>::FuncType groupFunc_;
};

class SQLSortOps::SortNway::SorterWriter {
public:
	class Normal {
	public:
		template<typename T>
		class TypeAt {
		public:
			explicit TypeAt(const Normal &base) : base_(base.base_) {}

			void operator()() const {
				typedef typename SQLOpUtils::ExpressionListWriter::ByDigestTuple<
						false>::template TypeAt<T> Projector;
				base_.writeBy(Projector(base_.writer_));
			}

		private:
			SorterWriter &base_;
		};

		explicit Normal(SorterWriter &base) : base_(base) {}

	private:
		SorterWriter &base_;
	};

	class Unique {
	public:
		typedef void RetType;

		template<typename T>
		class TypeAt {
		public:
			explicit TypeAt(const Unique &base) : base_(base.base_) {}

			void operator()() const {
				base_.writeUnique<T>();
			}

		private:
			SorterWriter &base_;
		};

		explicit Unique(SorterWriter &base) : base_(base) {}

	private:
		SorterWriter &base_;
	};

	SorterWriter(
			SortContext &cxt, TupleSorter &sorter, const Projection &proj,
			const SQLValues::CompColumnList &keyColumnList, bool primary,
			bool unique);

	void write();

private:
	typedef SQLValues::TupleComparator::WithAccessor<
			std::less<SQLValues::ValueComparator::PredArgType>,
			false, false, false, false,
			SQLValues::ValueAccessor::BySummaryTuple> WriterTupleLess;

	typedef void (*TypedFunc)(const Normal&);

	template<typename T> void writeUnique();
	template<typename T> void writeUniqueByDigestTuple();

	template<typename P> void writeBy(const P &projector);
	template<typename P, typename T> void writeUniqueBy(const P &projector);

	template<bool Rev>
	static TypedFunc getTypedFunction(TupleColumnType type);

	SummaryTuple* getHashMiddle(bool ordering) const;

	OpContext &cxt_;
	TupleSorter &sorter_;
	const Projection &proj_;
	SQLOpUtils::ExpressionListWriter writer_;
	bool primary_;
	bool unique_;

	const TupleDigester &digester_;
	WriterTupleLess pred_;
	SummaryTuple *hashTable_;
	SQLOps::OpAllocatorManager::BufferRef *hashBufferRef_;
	size_t hashSize_;
};

template<bool Fixed, bool Limited, bool Rotating>
struct SQLSortOps::SortNway::MergeAction {
	struct Options {
		typedef typename util::BoolType<Fixed>::Result FixedDigest;
	};
	typedef Options OptionsType;

	typedef typename util::Conditional<
			Fixed,
			SQLOpUtils::ExpressionListWriter::ByDigestTuple<Rotating>,
			SQLOpUtils::ExpressionListWriter::ByProjection>::Type ProjectorType;
	typedef typename ProjectorType::BaseType BaseType;

	template<typename T>
	struct TypeAt {
		typedef TypeAt TypedOp;

		explicit TypeAt(const MergeAction &base) : base_(base) {}

		bool operator()(const TupleHeapQueue::Element &elem) const {
			return (
					base_.template projectBy<T>(elem) &&
					base_.checkContinuable());
		}

		bool operator()(
				const TupleHeapQueue::Element&, const util::FalseType&) const {
			return base_.checkContinuable();
		}

		void operator()(
				const TupleHeapQueue::Element &elem, const util::TrueType&) const {
			base_.template projectBy<T>(elem);
		}

		bool operator()(
				const TupleHeapQueue::Element&, const util::TrueType&,
				const util::TrueType&) const {
			return true;
		}

		template<typename Pred>
		bool operator()(
				const TupleHeapQueue::Element&, const util::TrueType&,
				const Pred&) const {
			return true;
		}

		const MergeAction &base_;
	};

	MergeAction(SortContext &cxt, const BaseType &projectorBase);

	bool operator()(const TupleHeapQueue::Element &elem) const {
		return (project(elem) && checkContinuable());
	}

	bool operator()(const TupleHeapQueue::Element&, const util::FalseType&) {
		return checkContinuable();
	}

	void operator()(
			const TupleHeapQueue::Element &elem, const util::TrueType&) {
		project(elem);
	}

	bool project(const TupleHeapQueue::Element &elem) const;

	template<typename T>
	bool projectBy(const TupleHeapQueue::Element &elem) const;

	bool checkContinuable() const;

	OpContext &cxt_;
	ProjectorType projector_;
	int64_t &workingRestLimit_;
};

struct SQLSortOps::SortNway::GroupSortAction {
	typedef std::pair<
			const SQLOps::SummaryColumnList*,
			const SQLOps::SummaryColumnList*> ColumListPair;
	typedef std::pair<SummaryTupleSet*, SummaryTupleSet*> TupleSetPair;

	GroupSortAction(
			OpContext &cxt,
			const Projection *pipeProj, const Projection *mergeProj,
			SummaryTuple *outTupleRef, const ColumListPair &columnListPair,
			const TupleSetPair &tupleSetPair);
	~GroupSortAction();

	void operator()(bool primary) const;

	void operator()(SummaryTuple &tuple) const {
		*inTupleRef_ = tuple;
		*outTupleRef_ = tuple;
		pipeProj_->project(cxt_);
	}

	void operator()(SummaryTuple &outTuple, SummaryTuple &inTuple) const {
		*inTupleRef_ = inTuple;
		*outTupleRef_ = outTuple;
		pipeProj_->project(cxt_);
	}

	void operator()(
			SummaryTuple &outTuple, SummaryTuple &inTuple,
			const util::FalseType&) const {
		*inTupleRef_ = inTuple;
		*outTupleRef_ = outTuple;
		mergeProj_->project(cxt_);
	}

	OpContext &cxt_;
	const Projection *pipeProj_;
	const Projection *mergeProj_;

	SummaryTuple *inTupleRef_;
	SummaryTuple *outTupleRef_;
	ColumListPair columListPair_;
	TupleSetPair tupleSetPair_;
};

template<typename InputUnique>
struct SQLSortOps::SortNway::GroupMergeAction {
	struct Options {
		typedef util::FalseType FixedDigest;
	};
	typedef Options OptionsType;

	struct SubOp {
		explicit SubOp(const GroupMergeAction &base) : base_(base) {}

		bool operator()(
				const TupleHeapQueue::Element &elem, const util::FalseType&) const {
			*base_.activeReaderRef_ = &elem.getValue().getReader();
			base_.pipeProj_->project(base_.cxt_);
			return true;
		}

		void operator()(
				const TupleHeapQueue::Element &elem, const util::TrueType&) const {
			base_.finishProj_->projectBy(base_.cxt_, elem.getValue());
			base_.exprCxt_.initializeAggregationValues();
			if (!InputUnique::VALUE) {
				base_.exprCxt_.updateTupleId(0);
			}
		}

		bool operator()(
				const TupleHeapQueue::Element&, const util::TrueType&,
				const util::TrueType&) const {
			return true;
		}

		template<typename Pred>
		bool operator()(
				const TupleHeapQueue::Element &elem, const util::TrueType&,
				const Pred &pred) const {
			if (!InputUnique::VALUE) {
				TupleHeapQueue::Element &ref =
						base_.refList_[elem.getOrdinal()];
				if (!ref.getValue().isEmpty()) {
					const bool subFinishable = pred(ref, elem);
					if (!ref.next()) {
						ref = TupleHeapQueue::Element(
								ReadableTupleRef(util::FalseType()), 0);
					}
					return subFinishable;
				}
			}
			return true;
		}

		const GroupMergeAction &base_;
	};

	template<typename T>
	struct TypeAt {
		typedef SubOp TypedOp;
	};

	GroupMergeAction(
			OpContext &cxt,
			const Projection *pipeProj, const Projection *finishProj,
			TupleHeapRefList &refList);

	bool operator()(
			const TupleHeapQueue::Element &elem, const util::FalseType&) const {
		return SubOp(*this)(elem, util::FalseType());
	}

	void operator()(
			const TupleHeapQueue::Element &elem, const util::TrueType&) const {
		SubOp(*this)(elem, util::TrueType());
	}

	OpContext &cxt_;
	SQLExprs::ExprContext &exprCxt_;
	TupleListReader **activeReaderRef_;
	const Projection *pipeProj_;
	const Projection *finishProj_;
	TupleHeapRefList &refList_;
};

struct SQLSortOps::SortStageElement {
public:
	SortStageElement();

	void clear(bool primary);

	bool isAssigned(bool primary) const;
	void assign(bool primary, uint32_t storeIndex);

	uint32_t resolve(bool primary) const;

private:
	void set(bool primary, uint32_t storeIndex);
	uint32_t get(bool primary) const;

	uint32_t storeIndex1_;
	uint32_t storeIndex2_;
};

struct SQLSortOps::SorterEntry {
	typedef SummaryTuple SorterElementType;
	typedef SQLOps::OpAllocatorManager::BufferRef BufferRef;

	SorterEntry(
			OpContext &cxt, size_t capacity, bool hashAvailable,
			int64_t hashSizeBase, TupleListReader &reader);

	SummaryTupleSet& getSummaryTupleSet(bool primary);
	TupleSorter& resolveSorter(
			const std::pair<TupleLess, TupleLess> &predPair);

	size_t getAllocatedSize();
	size_t getHashConflictionLimit() const;

	void initializeHashTable();
	SorterElementType* getBuffer(bool forHash, bool primary);
	BufferRef& getBufferRef(bool forHash, bool primary);

	static size_t toBufferBytes(size_t capacity);
	static size_t toHashSize(
			size_t capacity, bool hashAvailable, int64_t hashSizeBase);
	static uint64_t toHashUnit(size_t hashSize, const TupleDigester &digester);
	static size_t toHashConflictionLimit(size_t capacity, size_t hashSize);

	SQLOps::OpAllocatorManager &allocManager_;
	SQLOps::OpStore::AllocatorRef allocRef_;

	SQLValues::ValueContext valueCxt_;

	SummaryTupleSet totalTupleSet_;
	SummaryTupleSet tupleSet1_;
	SummaryTupleSet tupleSet2_;

	size_t capacity_;
	size_t hashSize_;

	BufferRef buffer1_;
	BufferRef buffer2_;
	util::LocalUniquePtr<BufferRef> hashBuffer_;

	util::LocalUniquePtr<TupleSorter> sorter_;
	uint64_t initialReferencingBlockCount_;
	bool filled_;
};

struct SQLSortOps::SortContext {
	explicit SortContext(SQLValues::ValueContext &cxt);

	OpContext& getBase();
	util::StackAllocator& getAllocator();

	SQLOps::SummaryColumnList& getSummaryColumnList(bool primary);
	SQLValues::CompColumnList& getKeyColumnList(bool primary);
	bool& getFinalStageStarted(bool primary);
	util::LocalUniquePtr<TupleDigester>& getDigester(bool primary);

	OpContext *baseCxt_;

	uint32_t workingDepth_;
	util::LocalUniquePtr<SorterEntry> sorterEntry_;
	std::pair<bool, bool> finalStageStarted_;
	bool readerAccessible_;
	int64_t initialTupleCout_;
	SortStageList stageList_;

	util::Vector<uint32_t> freeStoreList_;

	int64_t workingRestLimit_;

	SQLOps::TupleColumnList middleColumnList_;
	SQLOps::SummaryColumnList summaryColumnList1_;
	SQLOps::SummaryColumnList summaryColumnList2_;

	SQLValues::CompColumnList keyColumnList1_;
	SQLValues::CompColumnList keyColumnList2_;

	SummaryTuple groupTuple_;

	SQLValues::CompColumnList nullCheckerKeys_;
	util::LocalUniquePtr<TupleNullChecker> nullChecker_;

	util::LocalUniquePtr<TupleDigester> digester1_;
	util::LocalUniquePtr<TupleDigester> digester2_;
};

class SQLSortOps::Window : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	typedef util::Vector<SQLExprs::Expression*> ExprRefList;

	static OpCode createPartitionCode(
			OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
			SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
			const Projection &pipeProj, const ExprRefList &windowExprList);

	static OpCode createPositioningJoinCode(
			OpCodeBuilder &builder, TupleColumnType valueColumnType);
	static OpCode createPositioningSortCode(
			OpCodeBuilder &builder, TupleColumnType valueColumnType);

	static OpCode createMergeCode(
			OpCodeBuilder &builder, SQLOpUtils::ProjectionPair &projections,
			ExprRefList &windowExprList, bool positioning);

	static OpCode createSingleCode(
			OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
			SQLOpUtils::ProjectionPair &projections,
			ExprRefList &windowExprList,
			const SQLExprs::Expression *windowOption);

	static void splitWindowExpr(
			OpCodeBuilder &builder, Projection &src,
			SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
			bool &valueCounting, ExprRefList &windowExprList);
	static SQLExprs::Expression& createPositioningValueExpr(
			OpCodeBuilder &builder, const SQLExprs::Expression &src,
			const SQLExprs::Expression &posExpr);

	static Projection& createProjectionWithWindow(
			OpCodeBuilder &builder, const Projection &pipeProj,
			Projection &src, ExprRefList &windowExprList, bool withFrame);
	static Projection& createProjectionWithWindowSub(
			OpCodeBuilder &builder, const Projection &pipeProj,
			ExprRefList *windowExprList);
	static Projection& createUnmatchWindowPipeProjection(
			OpCodeBuilder &builder, const Projection &src);

	static void applyFrameAttributes(
			Projection &proj, bool forWindowPipe, bool forward);

	static void setUpUnmatchWindowExpr(
			SQLExprs::ExprFactoryContext &factoryCxt,
			SQLExprs::Expression &expr);
	static SQLExprs::Expression& createUnmatchPositioningExpr(
			SQLExprs::ExprFactoryContext &factoryCxt,
			const SQLExprs::Expression *baseExpr);

	static void duplicateExprList(
			OpCodeBuilder &builder, const ExprRefList &src, ExprRefList &dest);

	static SQLOpTypes::Type getSingleNodeType(
			OpCodeBuilder &builder, const Projection &pipeProj,
			const SQLExprs::Expression *windowOption);
	static bool isRanking(
			OpCodeBuilder &builder, const Projection &pipeProj);
};

class SQLSortOps::WindowPartition : public SQLOps::Operator {
public:
	typedef SQLValues::TupleComparator::WithAccessor<
			std::equal_to<SQLValues::ValueComparator::PredArgType>,
			false, false, false, false,
			SQLValues::ValueAccessor::ByReader> TupleEq;

	virtual void execute(OpContext &cxt) const;

	static bool toAbsolutePosition(
			int64_t curPos, int64_t relativePos, bool following, int64_t &pos);

	static TupleEq* createKeyComparator(
			OpContext &cxt, const SQLValues::CompColumnList *keyList,
			util::LocalUniquePtr<TupleEq> &ptr);

private:
	typedef SQLOps::TupleColumn TupleColumn;

	struct PartitionContext {
		PartitionContext();

		util::LocalUniquePtr<TupleEq> keyEqBase_;
		TupleEq *keyEq_;

		const SQLExprs::Expression *posExpr_;
		const SQLExprs::Expression *valueExpr_;

		bool keyReaderStarted_;

		int64_t partitionOffset_;
		int64_t tupleOffset_;
		int64_t lastPartitionValueCount_;
		int32_t relativePositionDirection_;

		TupleColumn tupleCountColumn_;
		TupleColumn valueCountColumn_;

		TupleColumn orgPositionColumn_;
		TupleColumn refPositionColumn_;
		TupleColumn windowValueColumn_;
	};

	typedef SQLValues::Types::Long CountTypeTag;

	bool getProjections(
			const Projection *&pipeProj, const Projection *&subFinishProj) const;

	void findWindowExpr(
			OpContext &cxt, const SQLExprs::Expression *&valueExpr,
			const SQLExprs::Expression *&posExpr, bool &following) const;

	PartitionContext& preparePartitionContext(
			OpContext &cxt, bool projecting) const;

	TupleListReader* prepareKeyReader(
			OpContext &cxt, PartitionContext &partCxt) const;
	TupleListWriter* prepareCountingWriter(
			OpContext &cxt, bool projecting) const;
	TupleListWriter* preparePositioningWriter(
			OpContext &cxt, bool projecting) const;

	void writeCountingTuple(
			PartitionContext &partCxt, TupleListWriter &writer) const;
	void writePositioningTuple(
			PartitionContext &partCxt, TupleListWriter &writer,
			const TupleValue &posValue, const TupleValue &windowValue) const;

	void countUpValueColumn(
			PartitionContext &partCxt, const TupleValue &value) const;

	static uint32_t getCountingOutput(bool projecting);
	static uint32_t getPositioningOutput(bool projecting);
};

class SQLSortOps::WindowRankPartition : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

	static void resolveKeyListAt(
			const OpCode &code, SQLValues::CompColumnList &partKeyList,
			SQLValues::CompColumnList &rankKeyList);

private:
	typedef WindowPartition::TupleEq TupleEq;

	struct PartitionContext {
		explicit PartitionContext(util::StackAllocator &alloc);

		TupleEq *partEq_;
		TupleEq *rankEq_;
		util::LocalUniquePtr<TupleEq> partEqBase_;
		util::LocalUniquePtr<TupleEq> rankEqBase_;
		SQLValues::CompColumnList partKeyList_;
		SQLValues::CompColumnList rankKeyList_;

		bool keyReaderStarted_;
		bool pipeDone_;
		int64_t rankGap_;
	};

	void getProjections(
			const Projection *&pipeProj, const Projection *&subFinishProj) const;
	PartitionContext& preparePartitionContext(OpContext &cxt) const;

	void resolveKeyList(
			SQLValues::CompColumnList &partKeyList,
			SQLValues::CompColumnList &rankKeyList) const;

	static TupleListReader* prepareKeyReader(
			OpContext &cxt, PartitionContext &partCxt);
	static TupleListReader& preparePipeReader(OpContext &cxt);
	static TupleListReader& prepareSubFinishReader(OpContext &cxt);
};

class SQLSortOps::WindowFramePartition : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

	static bool isFrameEnabled(const SQLExprs::Expression *windowOption);

private:
	typedef SQLOps::TupleColumn TupleColumn;
	typedef WindowPartition::TupleEq TupleEq;

	typedef SQLValues::TupleComparator::WithAccessor<
			std::less<SQLValues::ValueComparator::PredArgType>,
			false, false, false, true,
			SQLValues::ValueAccessor::ByReader> ReaderTupleLess;
	typedef SQLValues::TupleComparator::WithAccessor<
			SQLValues::ValueComparator::ThreeWay,
			false, false, false, true,
			SQLValues::ValueAccessor::ByReader> ReaderTupleComp;

	enum {
		IN_KEY = 0,
		IN_PIPE = 1,
		IN_LOWER = 2,
		IN_SUB_FINISH = 3,
		IN_AGGR = 4
	};

	struct OrderedAggregatorKey;
	struct OrderedAggregatorKeyLess;
	struct OrderedAggregatorPosition;
	struct OrderedAggregatorInfo;

	class OrderedAggregator;

	struct Boundary;
	struct PartitionContext;

	static bool isPartTail(
			PartitionContext &partCxt, bool totalTail,
			TupleListReader &tailReader, TupleListReader &keyReader);

	static bool isLowerInside(
			PartitionContext &partCxt, TupleListReader &subFinishReader,
			TupleListReader &lowerReader);
	static bool isUpperInside(
			PartitionContext &partCxt, TupleListReader &subFinishReader,
			TupleListReader &pipeReader);

	static bool isInsideByCurrent(
			PartitionContext &partCxt, TupleListReader &lastReader,
			TupleListReader &nextReader);
	static bool isInsideByRange(
			PartitionContext &partCxt, bool upper, TupleListReader &lastReader,
			TupleListReader &nextReader);

	static bool resoveActivePipeReader(
			OpContext &cxt, OrderedAggregator *aggregator, bool upper,
			TupleListReader &base, TupleListReader *&activeReaderRef);
	static void clearAggregation(
			OpContext &cxt, OrderedAggregator *aggregator,
			TupleListReader &tailReader);

	void getProjections(
			const Projection *&pipeProj, const Projection *&lowerProj,
			const Projection *&subFinishProj) const;
	PartitionContext& preparePartitionContext(
			OpContext &cxt, const Projection &pipeProj) const;

	void tryPrepareAggregatorInfo(
			OpContext &cxt, PartitionContext &partCxt,
			const Projection &pipeProj) const;

	static void applyWindowOption(
			PartitionContext &partCxt,
			const SQLExprs::Expression *windowOption);

	static void applyBoundaryOption(
			PartitionContext &partCxt, int64_t flags, const TupleValue &value,
			bool start);

	static bool prepareAggregator(
			OpContext &cxt, PartitionContext &partCxt,
			TupleListReader &pipeReader,
			util::LocalUniquePtr<OrderedAggregator> &aggregator);

	static TupleListReader* prepareKeyReader(
			OpContext &cxt, PartitionContext &partCxt);
	static TupleListReader& preparePipeReader(OpContext &cxt);
	static TupleListReader& prepareLowerReader(OpContext &cxt);
	static TupleListReader& prepareSubFinishReader(OpContext &cxt);
};

struct SQLSortOps::WindowFramePartition::OrderedAggregatorKey {
	OrderedAggregatorKey(
			TupleListReader &reader, uint32_t level, bool upper);

	TupleListReader &reader_;
	uint32_t level_;
	bool upper_;
};

struct SQLSortOps::WindowFramePartition::OrderedAggregatorKeyLess {
	typedef OrderedAggregatorKey Key;

	explicit OrderedAggregatorKeyLess(const ReaderTupleComp &aggrComp);

	bool operator()(const Key &key1, const Key &key2) const;

	ReaderTupleComp aggrComp_;
};

struct SQLSortOps::WindowFramePartition::OrderedAggregatorPosition {
	OrderedAggregatorPosition(uint32_t keyReaderId, uint32_t tailReaderId);

	uint32_t keyReaderId_;

	uint32_t tailReaderId_;
	uint64_t tailPos_;

	bool active_;
};

struct SQLSortOps::WindowFramePartition::OrderedAggregatorInfo {
	typedef OrderedAggregatorPosition Position;
	typedef util::AllocVector<Position> PositionList;

	OrderedAggregatorInfo(
			SQLValues::ValueContext &valueCxt,
			const SQLValues::CompColumnList &aggrKeyList);

	uint64_t lowerTargetPos_;
	uint64_t upperTargetPos_;

	PositionList lowerPosList_;
	PositionList upperPosList_;

	SQLValues::CompColumnList aggrKeyList_;
	SQLValues::TupleComparator baseAggrComp_;
	ReaderTupleComp aggrComp_;
	ReaderTupleComp aggrLess_;
	OrderedAggregatorKeyLess keyLess_;

	bool updating_;
	bool updatingReady_;
	bool upperUpdating_;
};

class SQLSortOps::WindowFramePartition::OrderedAggregator {
public:
	typedef OrderedAggregatorInfo Info;

	OrderedAggregator(
			OpContext &cxt, Info &info, TupleListReader &curReader);

	bool update(OpContext &cxt, bool upper, TupleListReader &baseReader);
	bool resumeUpdates(OpContext &cxt);

	void reset(OpContext &cxt, TupleListReader &tailReader);

	TupleListReader* getTop();

private:
	typedef OrderedAggregatorKey Key;
	typedef OrderedAggregatorKeyLess KeyLess;
	typedef OrderedAggregatorPosition Position;
	typedef Info::PositionList PositionList;

	typedef util::AllocSet<Key, KeyLess> KeySet;
	typedef KeySet::iterator KeyIterator;

	typedef std::pair<KeyIterator, KeyIterator> KeyIteratorPair;
	typedef util::AllocVector<KeyIteratorPair> KeyIteratorList;

	void initializeKeys(OpContext &cxt);
	void initializeKeysAt(OpContext &cxt, bool upper);

	void resetReaders(OpContext &cxt, TupleListReader &reader, bool resumeOnly);
	void resetReadersAt(
			OpContext &cxt, TupleListReader &reader, bool resumeOnly,
			bool upper);

	bool updateKeysAt(OpContext &cxt, bool upper);
	bool updateKey(OpContext &cxt, bool upper, uint32_t level);
	bool updateKeyDetail(
			OpContext &cxt, bool upper, uint32_t level, Position &pos);

	void clearAllKeys();
	void eraseKey(bool upper, uint32_t level);
	void insertKey(OpContext &cxt, bool upper, uint32_t level);

	bool isKeyUpdatable(uint64_t lastPos, bool upper, uint32_t level);

	uint32_t resolveEffectiveEndLevel(bool upper, bool last);
	static uint32_t resolveEffectiveEndLevelAt(
			uint64_t lowerPos, uint64_t upperPos, bool upper);

	uint64_t getTargetPosition(bool upper, bool last);
	uint64_t& getTargetPositionRef(bool upper);

	void inheritActivePosition(OpContext &cxt, bool upper, uint32_t level);
	Position& resolvePosition(bool upper, uint32_t level);

	static uint32_t toReaderId(bool upper, bool forTail, uint32_t level);
	static TupleListReader& getReader(OpContext &cxt, uint32_t id);

	static void assignInitialReader(
			OpContext &cxt, Position &pos, TupleListReader &reader);
	static void assignReader(
			OpContext &cxt, Position &pos, TupleListReader &keyReader,
			TupleListReader &tailReader);

	static bool isPositionActive(const Position &pos);
	static void activatePosition(Position &pos, uint64_t tailPos);
	static void deactivatePosition(Position &pos);

	static std::pair<uint64_t, uint64_t> getEffectivePositionRange(
			uint64_t pos, bool upper, uint32_t level);

	static uint64_t roundPosition(uint64_t pos, bool upper, uint32_t level);
	static uint64_t getPositionRangeSize(uint32_t level);

	Info &info_;
	KeySet keySet_;
	KeyIteratorList keyItList_;
};

struct SQLSortOps::WindowFramePartition::Boundary {
	Boundary();

	SQLValues::LongInt distance_;
	bool unbounded_;
	bool rows_;
	bool current_;
};

struct SQLSortOps::WindowFramePartition::PartitionContext {
	explicit PartitionContext(util::StackAllocator &alloc);

	TupleEq *partEq_;
	TupleEq *rankEq_;
	util::LocalUniquePtr<TupleEq> partEqBase_;
	util::LocalUniquePtr<TupleEq> rankEqBase_;
	SQLValues::CompColumnList partKeyList_;
	SQLValues::CompColumnList rankKeyList_;

	std::pair<Boundary, Boundary> boundary_;

	bool keyReaderStarted_;
	bool rankAscending_;

	int64_t lowerPos_;
	int64_t upperPos_;

	int64_t currentPos_;
	int64_t initialPipePos_;

	util::LocalUniquePtr<OrderedAggregatorInfo> aggrInfo_;
};

class SQLSortOps::WindowMerge : public SQLOps::Operator {
public:
	enum {
		IN_MAIN = 0,
		IN_COUNTING = 1,
		IN_POSITIONING = 2,
	};

	enum {
		COLUMN_POSITIONING_ORG = 0,
		COLUMN_POSITIONING_VALUE = 1,
		COLUMN_POSITIONING_REF = 2
	};

	virtual void execute(OpContext &cxt) const;

private:
	typedef SQLOps::TupleColumn TupleColumn;

	struct MergeContext {
		MergeContext();

		SQLExprs::WindowState windowState_;
		int64_t partitionRemaining_;
		int64_t partitionOffset_;

		int32_t relativePositionDirection_;
		int64_t relativePositionAmount_;

		int64_t positioningReaderProgress_;
		TupleColumn positionColumn_;

		TupleColumn tupleCountColumn_;
		TupleColumn valueCountColumn_;
	};

	typedef SQLValues::Types::Long CountTypeTag;

	void getProjections(
			const Projection *&matchedPipeProj,
			const Projection *&unmatchedPipeProj,
			const Projection *&subFinishProj) const;
	const SQLExprs::Expression* findPositioningExpr(
			OpContext &cxt, bool &following, bool &withDirection,
			int64_t &amount) const;

	MergeContext& prepareMergeContext(OpContext &cxt) const;
	TupleListReader* preparePositioningReader(
			OpContext &cxt, const MergeContext &mergeCxt) const;

	void readNextPartitionCounts(
			MergeContext &mergeCxt, TupleListReader &reader) const;
	bool locatePosition(MergeContext &mergeCxt, TupleListReader &reader) const;

	static bool getDesiredPosition(const MergeContext &mergeCxt, int64_t &pos);
	static bool isPositionInRange(const MergeContext &mergeCxt, int64_t pos);
	static int64_t getCurrentPosition(const MergeContext &mergeCxt);
};



template<bool Fixed, bool Limited, bool Rotating>
SQLSortOps::SortNway::MergeAction<Fixed, Limited, Rotating>::MergeAction(
		SortContext &cxt, const BaseType &projectorBase) :
		cxt_(cxt.getBase()),
		projector_(projectorBase),
		workingRestLimit_(cxt.workingRestLimit_) {
}

template<bool Fixed, bool Limited, bool Rotating>
inline bool SQLSortOps::SortNway::MergeAction<
		Fixed, Limited, Rotating>::project(
		const TupleHeapQueue::Element &elem) const {
	projector_.template projectorAt<void>().projectBy(cxt_, elem.getValue());

	if (Limited) {
		assert(workingRestLimit_ > 0);
		if (--workingRestLimit_ <= 0) {
			return false;
		}
	}

	return true;
}

template<bool Fixed, bool Limited, bool Rotating>
template<typename T>
inline bool SQLSortOps::SortNway::MergeAction<
		Fixed, Limited, Rotating>::projectBy(
		const TupleHeapQueue::Element &elem) const {
	const bool reversed =
			T::VariantTraitsType::OfValueComparator::ORDERING_REVERSED;
	typedef SQLValues::ValueComparator::VariantTraits<
			void, !reversed, void, void> VariantType;
	typedef typename T::template VariantRebind<VariantType>::Type TraitsType;

	projector_.template projectorAt<TraitsType>().projectBy(
			cxt_, elem.getValue());

	if (Limited) {
		assert(workingRestLimit_ > 0);
		if (--workingRestLimit_ <= 0) {
			return false;
		}
	}

	return true;
}

template<bool Fixed, bool Limited, bool Rotating>
inline bool SQLSortOps::SortNway::MergeAction<
		Fixed, Limited, Rotating>::checkContinuable() const {

	return true;
}


template<typename InputUnique>
SQLSortOps::SortNway::GroupMergeAction<InputUnique>::GroupMergeAction(
		OpContext &cxt,
		const Projection *pipeProj, const Projection *finishProj,
		TupleHeapRefList &refList) :
		cxt_(cxt),
		exprCxt_(cxt.getExprContext()),
		activeReaderRef_(cxt.getExprContext().getActiveReaderRef()),
		pipeProj_(pipeProj),
		finishProj_(&resolveOutputProjection(*finishProj)),
		refList_(refList) {
}

#endif
