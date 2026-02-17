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

#include "sql_operator_sort.h"

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
	class WindowMatch;
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

class SQLSortOps::WindowMatch : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

	static bool tryMakeWindowSubPlan(OpContext &cxt, const OpCode &code);

private:
	enum {
		IN_KEY = 0,
		IN_PREDICATE = 1,
		IN_NAVIGATION_START = 2
	};

	enum NavigationType {
		NAV_NONE,
		NAV_MATCHING,
		NAV_PROJECTION_PIPE_ONCE,
		NAV_PROJECTION_PIPE_EACH,
		NAV_PROJECTION_FINISH,
		NAV_CLASSIFIER
	};

	class MicroCompiler;

	struct VariablePredicateInfo;
	struct NavigationInfo;

	struct VariableGroupEntry;
	struct MatchEntry;
	struct MatchStackElement;
	struct VariableHistoryElement;
	struct MatchState;
	struct ProjectionState;

	class PatternGraph;
	struct PatternCode;
	struct PatternNode;

	class VariableTable;

	class NavigationReaderEntry;
	class NavigationReaderSet;
	class ColumnNavigator;

	struct MatchContext;

	typedef util::Vector<int64_t> VariableIdList;
	typedef util::AllocVector<int64_t> AllocVariableIdList;

	typedef util::Map<int64_t, VariablePredicateInfo> VariablePredicateMap;
	typedef util::Set<int64_t> VariableIdSet;
	typedef util::Vector<VariableIdSet> VariableIdsList;

	typedef std::pair<NavigationType, int64_t> NavigationKey;

	typedef std::pair<int64_t, uint32_t> PlanColumnKey;
	typedef std::pair<PlanColumnKey, SQLType::Id> PlanNavigationColumnKey;
	typedef std::pair<NavigationKey, PlanNavigationColumnKey> PlanNavigationKey;

	typedef std::pair<const SQLExprs::Expression*, uint32_t> PlanNavigationValue;
	typedef std::pair<Projection*, SQLOpUtils::ProjectionPair> NavigationProjection;
	typedef std::pair<const Projection*, SQLOpUtils::ProjectionRefPair> NavigationProjectionRef;

	typedef util::Map<TupleColumnType, uint32_t> PlanTypedCountMap;
	typedef util::Map<PlanNavigationKey, const SQLExprs::Expression*> PlanNavigationExprMap;
	typedef util::Map<PlanNavigationKey, PlanNavigationValue> PlanNavigationMap;
	typedef util::Map<uint32_t, const SQLExprs::Expression*> BaseExprMap;

	typedef util::Map<int64_t, const SQLExprs::Expression*> VariableExprMap;
	typedef util::Vector<VariableIdList> TotalVariableIdList;

	typedef util::MultiMap<NavigationKey, NavigationInfo> NavigationMap;
	typedef std::pair<uint64_t, bool> NavigationPosition;
	typedef std::pair<NavigationPosition, const NavigationInfo*> LocatedNavigationInfo;
	typedef util::AllocVector<LocatedNavigationInfo> LocatedNavigationList;

	typedef util::AllocVector<MatchStackElement> MatchStack;
	typedef util::AllocVector<VariableHistoryElement> VariableHistory;

	typedef util::AllocVector<VariableGroupEntry> VariableGroupList;
	typedef util::AllocMap<int64_t, VariableGroupEntry> VariableGroupMap;

	typedef std::pair<uint32_t, VariableGroupList> LocatedVariableGroupList;
	typedef util::AllocMap<int64_t, LocatedVariableGroupList> LocatedVariableGroupMap;

	typedef util::AllocMap<uint64_t, MatchEntry> MatchEntryMap;
	typedef MatchEntryMap::iterator MatchEntryIterator;

	typedef std::pair<uint32_t, uint64_t> MatchHashKey;

	typedef util::AllocMap<uint64_t, MatchEntryIterator> OrderedMatchKeyMap;
	typedef util::AllocMap<MatchHashKey, MatchEntryIterator> UnorderedMatchKeyMap;

	typedef util::AllocVector<uint32_t> PatternStackPath;
	typedef util::AllocMap<
			PatternStackPath, MatchEntryIterator> PatternStackPathMap;

	typedef util::Vector<NavigationReaderEntry> NavigationReaderList;

	typedef NavigationReaderList::iterator NavigationReaderIterator;

	typedef std::pair<uint64_t, NavigationReaderIterator> LocatedNavigationReader;
	typedef util::AllocSet<LocatedNavigationReader> LocatedNavigationReaderSet;
	typedef util::AllocSet<NavigationReaderIterator> NavigationReaderRefSet;

	typedef util::Vector<const SQLExprs::Expression*> NavigationExprList;
	typedef util::AllocMap<NavigationKey, NavigationExprList> NavigationExprMap;

	typedef WindowPartition::TupleEq TupleEq;

	static bool matchRow(
			OpContext &cxt, MatchContext &matchCxt, bool partTail);
	static bool projectMatchedRows(
			OpContext &cxt, MatchContext &matchCxt);

	static bool preparePredicateReaders(
			OpContext &cxt, MatchContext &matchCxt, TupleListReader *&predReader,
			TupleListReader *&keyReader);
	static bool nextPrediateRow(
			OpContext &cxt, MatchContext &matchCxt, TupleListReader &predReader,
			TupleListReader &keyReader, bool partTail);

	static bool isPartitionTail(
			MatchContext &matchCxt, TupleListReader &predReader,
			TupleListReader &keyReader);

	static void checkMemoryLimit(OpContext &cxt, MatchContext &matchCxt);

	MatchContext& prepareMatchContext(OpContext &cxt) const;
};

class SQLSortOps::WindowMatch::MicroCompiler {
public:
	explicit MicroCompiler(util::StackAllocator &alloc);

	static bool isAcceptableWindowPlan(const OpCode &code);
	void makeWindowSubPlan(OpContext &cxt, const OpCode &code);

	static const SQLExprs::Expression& predicateToPatternExpr(
			const SQLExprs::Expression *pred);

	VariablePredicateMap makeVariablePredicateMap(
			const SQLExprs::Expression *pred, const Projection *proj);
	NavigationMap makeNavigationMap(
			const SQLExprs::Expression *pred, const Projection *proj);

	static NavigationProjectionRef getNavigableProjections(const Projection *proj);
	static bool isAllRowsPerMatch(const SQLExprs::Expression *pred);

	static uint32_t getNavigationReaderCount(
			OpContext &cxt, const OpCode &code);
	static uint64_t getPatternMatchMemoryLimit(const OpCode &code);

	static const Projection* getProjectionElement(
			const NavigationProjectionRef &base, size_t ordinal);
	static const Projection*& getProjectionElement(
			NavigationProjectionRef &base, size_t ordinal);
	static Projection*& getProjectionElement(
			NavigationProjection &base, size_t ordinal);

private:
	NavigationProjectionRef makeBaseProjection(
			OpCodeBuilder &builder, const OpCode &code);

	void setUpBaseProjectionExtra(
			OpCodeBuilder &builder, Projection &proj, bool pre, bool forPipe);
	Projection& toGroupedBaseProjection(
			OpCodeBuilder &builder, const Projection &src);
	SQLOpUtils::ProjectionPair pullUpBaseProjections(
			OpCodeBuilder &builder, const SQLOpUtils::ProjectionPair &src);
	Projection& splitBasePipeProjection(
			OpCodeBuilder &builder, const Projection &src, bool once);

	static void arrangeForGroupedBaseExpr(
			SQLExprs::ExprFactoryContext &factoryCxt,
			SQLExprs::Expression &expr);
	static void makePullUpTargetMap(
			const SQLExprs::Expression &expr, BaseExprMap &map);
	static void arrangeForPulledUpBaseExpr(
			OpCodeBuilder &builder, SQLExprs::Expression &expr, bool forPipe,
			const BaseExprMap &map);
	static void arrangeForSplittedBaseExpr(
			SQLExprs::Expression &expr, bool once);

	static bool isEachNavigationExpr(const SQLExprs::Expression *expr);
	static bool isPipeOnceBaseExpr(const SQLExprs::Expression &expr);

	Projection& makeSubPlanProjection(
			OpCodeBuilder &builder, const NavigationProjectionRef &baseProj,
			const SQLExprs::Expression &basePred,
			const SQLValues::TupleColumnList &inColumnList);

	Projection& makeTotalPipeProjection(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const NavigationProjectionRef &baseProj,
			const SQLExprs::Expression &basePred);
	Projection& makeRefProjection(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const NavigationProjectionRef &baseProj, bool forPipe, bool once);
	Projection& makeMatchingProjection(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const NavigationProjectionRef &baseProj,
			const VariableExprMap &varMap);
	Projection& makeNavigationProjection(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const NavigationProjectionRef &baseProj,
			const VariableExprMap &varMap);
	Projection& makeNavigationProjectionSub(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const SQLOps::ProjectionCode &baseProjCode,
			const VariableExprMap &varMap, bool original, bool empty);
	Projection& makeNavigationProjectionSubByKey(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const SQLOps::ProjectionCode &baseProjCode, bool original,
			bool empty, const NavigationKey &navKey);

	void setUpArrangedNavigationExpr(
			OpCodeBuilder &builder, const PlanNavigationMap &navMap,
			const NavigationKey &navKey, SQLExprs::Expression &expr);
	VariableExprMap makeVariableExprMap(
			OpCodeBuilder &builder, const SQLExprs::Expression &basePred);

	PlanNavigationMap makePlanNavigationMap(
			const NavigationProjectionRef &baseProj,
			const SQLExprs::Expression &basePred,
			const SQLValues::TupleColumnList &inColumnList,
			const util::Vector<TupleColumnType> *&aggrTypeList);
	PlanNavigationExprMap makePlanNavigationExprMap(
			const NavigationProjectionRef &baseProj,
			const SQLExprs::Expression &basePred);
	void setUpPlanNavigationExprMap(
			const NavigationKey &navKey, const SQLExprs::Expression &expr,
			PlanNavigationExprMap &navExprMap);

	PlanTypedCountMap makePlanNavigationTypeCounts(
			const PlanNavigationExprMap &navExprMap,
			const SQLValues::TupleColumnList &inColumnList);
	PlanNavigationMap makePlanNavigationMapByCounts(
			const PlanNavigationExprMap &navExprMap,
			const PlanTypedCountMap &typeCounts,
			const util::Vector<TupleColumnType> &baseAggrTypeList,
			const SQLValues::TupleColumnList &inColumnList);
	static TupleColumnType getNavigationBaseColumnType(
			const SQLValues::TupleColumnList &inColumnList,
			const PlanNavigationKey &key);

	util::Vector<TupleColumnType> makeAggregationTypeList(
			const util::Vector<TupleColumnType> &baseAggrTypeList,
			const PlanTypedCountMap &typeCounts);
	NavigationProjectionRef makeNavigableBaseProjection(
			OpCodeBuilder &builder, const NavigationProjectionRef &src,
			const util::Vector<TupleColumnType> *aggrTypeList);

	static PlanNavigationColumnKey navigationColumnTopKey();

	static void setUpNavigationMapForClassifier(
			const SQLExprs::Expression &basePred,
			const NavigationProjectionRef &navProjs, NavigationMap &navMap);
	static bool findClassifier(const NavigationProjectionRef &navProjs);
	static bool findClassifierSub(const SQLExprs::Expression *expr);

	static NavigationInfo resolveNavigationInfo(
			const SQLExprs::Expression &descExpr,
			const SQLExprs::Expression &assignExpr,
			const SQLExprs::Expression &emptyExpr, uint32_t navIndex,
			NavigationType type);

	static NavigationProjectionRef firstNavigationProjections(
			const Projection *proj);
	static bool nextNavigationProjections(
			uint32_t ordinal, NavigationKey &key, NavigationProjectionRef &cur,
			NavigationProjectionRef &next);
	static const Projection* nextMatchingProjection(
			const Projection *&next, bool top);
	static void checkNavigationProjection(const Projection *proj);

	static const Projection& resolveMultiStageElement(
			const Projection *proj, size_t ordinal);
	static const Projection& resolveMultiStageSubElement(
			const Projection *proj, size_t ordinal1, size_t ordinal2);
	static const Projection& resolveMultiStageSubSubElement(
			const Projection *proj, size_t ordinal1, size_t ordinal2,
			size_t ordinal3);

	static const Projection* filterEmptyProjection(const Projection *proj);
	static void arrangeForEmptyProjection(
			SQLExprs::ExprFactoryContext &factoryCxt, Projection &proj);

	static NavigationProjectionRef makeProjectionRefElements(
			const Projection *proj1, const Projection *proj2,
			const Projection *proj3);

	static bool isMultiStageProjection(const Projection *proj);
	static bool findNagvigableColumnKey(
			const SQLExprs::Expression &expr,
			PlanNavigationColumnKey &columnKey);
	static const SQLExprs::Expression& predicateToVariableListExpr(
			const SQLExprs::Expression *base);
	static const SQLExprs::Expression& variableToElementExprs(
			const SQLExprs::Expression &base, const TupleValue **name);

	static void optionToElementExprs(
			const SQLExprs::Expression &base,
			const SQLExprs::Expression *&patternExpr,
			const SQLExprs::Expression *&varListExpr,
			const SQLExprs::Expression *&modeExpr);

	static const SQLExprs::Expression& resolvePatternOptionExpr(
			const SQLExprs::Expression *base);
	static const SQLExprs::Expression* findPatternOptionExpr(
			const SQLExprs::Expression *base);

	static int32_t getNavigationDirection(SQLType::Id exprType);
	static bool isNavigationByMatch(SQLType::Id exprType);
	static bool isNavigationColumn(SQLType::Id exprType);

	util::StackAllocator &alloc_;
};

struct SQLSortOps::WindowMatch::VariablePredicateInfo {
	VariablePredicateInfo();

	uint32_t predicateIndex_;
	const TupleValue *name_;
	const SQLExprs::Expression *expr_;
};

struct SQLSortOps::WindowMatch::NavigationInfo {
	NavigationInfo();

	uint32_t navigationIndex_;
	int64_t variableId_;
	int32_t direction_;
	bool byMatch_;

	const SQLExprs::Expression *assignmentExpr_;
	const SQLExprs::Expression *emptyExpr_;
};

struct SQLSortOps::WindowMatch::VariableGroupEntry {
	VariableGroupEntry();

	uint64_t beginPos_;
	uint64_t endPos_;
};

struct SQLSortOps::WindowMatch::MatchEntry {
	explicit MatchEntry(SQLValues::VarAllocator &varAlloc);

	MatchStack stack_;
	VariableHistory history_;
	VariableGroupMap groupMap_;
	VariableGroupEntry searchRange_;
	uint64_t firstPos_;
	int64_t matchingVariableId_;
};

struct SQLSortOps::WindowMatch::MatchStackElement {
	MatchStackElement(uint32_t patternId, bool withTailAnchor);

	uint32_t hash_;
	uint32_t patternId_;
	uint64_t repeat_;
	bool withTailAnchor_;
};

struct SQLSortOps::WindowMatch::VariableHistoryElement {
	VariableHistoryElement();

	int64_t variableId_;
	uint64_t beginPos_;
	uint64_t endPos_;
};

struct SQLSortOps::WindowMatch::MatchState {
public:
	explicit MatchState(SQLValues::VarAllocator &varAlloc);

	MatchEntryMap map_;
	UnorderedMatchKeyMap keyMap_;
	OrderedMatchKeyMap workingKeys_;

	MatchEntryIterator prevMatchIt_;
	MatchEntryIterator curMatchIt_;

	bool curStarted_;
	bool curFinished_;

	bool varMatching_;

	bool prevMatched_;
	bool curMatched_;

	uint64_t nextKey_;

	VariableGroupEntry searchRange_;
	bool partitionTail_;
	uint64_t matchCount_;

private:
	MatchState(const MatchState&);
	MatchState& operator=(const MatchState&);
};

struct SQLSortOps::WindowMatch::ProjectionState {
	explicit ProjectionState(SQLValues::VarAllocator &varAlloc);

	uint32_t matchOrdinal_;
	NavigationType type_;

	size_t historyIndex_;
	uint64_t nextPos_;

	VariableHistory history_;
	LocatedVariableGroupMap groupMap_;
	VariableGroupEntry searchRange_;
	uint64_t firstPos_;
};

class SQLSortOps::WindowMatch::PatternGraph {
public:
	PatternGraph(OpContext &cxt, const SQLExprs::Expression &expr);

	bool findSub(
			uint32_t id, bool forChild, uint32_t ordinal,
			uint32_t &subId) const;
	uint32_t getDepth(uint32_t id) const;

	bool findVariable(uint32_t id, int64_t &varId) const;

	uint64_t getMinRepeat(uint32_t id) const;
	bool findMaxRepeat(uint32_t id, uint64_t &repeat) const;

	bool getTopAnchor(uint32_t id) const;
	bool getTailAnchor(uint32_t id) const;

private:
	typedef util::Vector<PatternNode> NodeList;

	const PatternCode& getCode(uint32_t id) const;
	const PatternNode& getNode(uint32_t id) const;

	static void build(const SQLExprs::Expression &expr, NodeList &nodeList);

	static void buildSub(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList);
	static void buildVariable(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList);
	static void buildRepeat(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList);
	static void buildAnchor(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList);
	static void buildConcat(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList);
	static void buildOr(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList);

	static uint32_t prepareStack(
			const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList,
			bool onlyForMultiElements);
	static bool isNewStackRequired(
			const SQLExprs::Expression &expr,
			const PatternNode &node, bool onlyForMultiElements);
	static bool isQuantitySpecified(const PatternCode &code);

	static uint32_t newStack(NodeList &nodeList, uint32_t baseId);
	static uint32_t newNode(NodeList &nodeList, const uint32_t *baseId);

	util::Vector<PatternNode> nodeList_;
};

struct SQLSortOps::WindowMatch::PatternCode {
	PatternCode();

	uint32_t depth_;
	int64_t variableId_;
	int64_t minRepeat_;
	int64_t maxRepeat_;
	bool top_;
	bool tail_;
};

struct SQLSortOps::WindowMatch::PatternNode {
	explicit PatternNode(util::StackAllocator &alloc);

	util::Vector<uint32_t> following_;
	util::Vector<uint32_t> child_;

	PatternCode code_;
};

class SQLSortOps::WindowMatch::VariableTable {
public:
	VariableTable(
			OpContext &cxt, const PatternGraph *patternGraph,
			const VariablePredicateMap *predMap, const NavigationMap *navMap);

	void nextMatchingPosition(uint64_t pos);
	bool prepareProjection(
			bool forAllRows, NavigationType &type, bool &matchTail);
	void setPartitionTail();

	bool findMatchingVariable(const VariablePredicateInfo *&predInfo);
	void updateMatchingVariable(bool matched);

	bool findNavigationColumn(
			uint32_t ordinal, NavigationKey &key, uint32_t &navIndex,
			NavigationPosition &pos);

	SQLExprs::WindowState& getWindowState();
	bool resolveMatch(uint64_t &effectiveStartPos);

	void getStats(uint64_t &searchingRowCount, uint64_t &candidateCount);

private:
	void clearAll();
	void prepareNextMatchingPosition();

	static void setUpNextWorkingKeys(
			SQLValues::VarAllocator &varAlloc, MatchEntryIterator prevMatchIt,
			const PatternGraph *patternGraph, bool inheritable,
			UnorderedMatchKeyMap &keyMap, OrderedMatchKeyMap &workingKeys);
	static void setUpNextMatchEntries(
			MatchEntryIterator prevMatchIt, bool partitionTail,
			const OrderedMatchKeyMap &workingKeys, MatchEntryMap &map);

	void addInitialMatchEntry();

	void setUpProjectionVariableGroups(
			const VariableHistory &history, LocatedVariableGroupMap &groupMap);

	void applyWindowState();
	uint64_t resolveMatchCount(uint64_t startPos);
	const TupleValue* resolveVariableName(uint64_t pos);

	const LocatedNavigationList& prepareNavigationList(NavigationKey &key);

	NavigationKey resolveNavigationKey(
			const MatchEntry *&matchEntry, uint64_t &curPos);
	VariableGroupEntry resolveNavigationGroup(
			const MatchEntry *matchEntry, const NavigationInfo &info,
			int64_t refVarId, uint64_t curPos);
	NavigationPosition resolveNavigationPosition(
			const NavigationInfo &info, const VariableGroupEntry &group,
			uint64_t curPos);

	bool updateMatchingState();
	void assignMatchResult();
	bool isMatched();

	bool checkNextPattern(
			MatchEntryIterator entryIt, bool &currentAllowed,
			bool &followingAllowed);
	bool applyVariablePattern(
			MatchEntryIterator entryIt, bool currentAllowed, bool followingAllowed,
			bool &working);
	void applyNextPattern(
			MatchEntryIterator entryIt, bool currentAllowed, bool followingAllowed);

	void completeMatchEntryWorking(MatchEntryIterator entryIt);

	void repeatMatchStack(MatchEntryIterator entryIt);
	void forwardMatchStack(
			MatchEntryIterator entryIt, uint32_t patternId,
			util::LocalUniquePtr<MatchEntry> &prevEntry);
	void pushMatchStack(
			MatchEntryIterator entryIt, uint32_t patternId,
			util::LocalUniquePtr<MatchEntry> &prevEntry);
	MatchEntryIterator popMatchStack(
			MatchEntryIterator entryIt,
			util::LocalUniquePtr<MatchEntry> &prevEntry);

	MatchStackElement newMatchStackElement(uint32_t patternId);
	MatchEntryIterator prepareMatchEntry(
			MatchEntryIterator entryIt,
			util::LocalUniquePtr<MatchEntry> *prevEntry);
	MatchEntryIterator addMatchEntry(const MatchEntry &src);

	MatchEntryIterator reduceMatchEntry(
			MatchEntryIterator entryIt, bool &matchUpdatable);
	void eraseMatchWorkingKey(MatchEntryIterator entryIt);

	bool findSameMatchEntry(
			uint32_t hash, const MatchEntry &entry, MatchEntryIterator &foundIt);
	bool isSameStack(const MatchStack &stack1, const MatchStack &stack2);
	bool isSameStackElement(
			const MatchStackElement &elem1, const MatchStackElement &elem2);
	uint32_t updateMatchStackHash(MatchStack &stack);

	static PatternStackPath getStackPath(
			SQLValues::VarAllocator &varAlloc, const MatchStack &stack);

	static bool isPriorMatchEntry(
			const MatchEntryIterator &it1, const MatchEntryIterator &it2);
	static bool isReachedMatchEntry(
			const PatternGraph *patternGraph, MatchEntryIterator entryIt);

	LocatedVariableGroupList& resolveLocatedVariableGroupList(int64_t refVarId);
	const VariablePredicateInfo& resolveVariablePredicateInfo(int64_t refVarId);

	static VariableIdsList makeVariableIdsList(
			util::StackAllocator &alloc, const NavigationMap &navMap);
	static void makeVariableIdSet(
			const NavigationMap &navMap, NavigationType type,
			VariableIdSet &idSet);
	static void makeVariableIdSetSub(
			const NavigationMap &navMap, NavigationType baseType,
			VariableIdSet &idSet);

	SQLValues::VarAllocator &varAlloc_;

	const PatternGraph *patternGraph_;
	const VariablePredicateMap *predMap_;
	const NavigationMap *navMap_;
	VariableIdsList varIdsList_;

	MatchState match_;
	ProjectionState proj_;
	LocatedNavigationList navList_;
	SQLExprs::WindowState windowState_;
};

class SQLSortOps::WindowMatch::NavigationReaderEntry {
public:
	NavigationReaderEntry();

	uint64_t position_;
	uint32_t subIndex_;
	bool navigated_;
	bool preserved_;
};

class SQLSortOps::WindowMatch::NavigationReaderSet {
public:
	NavigationReaderSet(OpContext &cxt, uint32_t maxReaderCount);

	void setBaseReader(OpContext &cxt, TupleListReader &baseReader);

	bool navigateReader(OpContext &cxt, uint64_t pos, TupleListReader *&reader);
	void prepareNextPositions();

	void invalidatePrevPositionRange(OpContext &cxt, uint64_t nextPos);
	void clearPositions(OpContext &cxt, TupleListReader &baseReader);

private:
	bool navigateFirstPosition(OpContext &cxt);
	void clearPositionsDetail(
			OpContext &cxt, uint64_t firstPos, TupleListReader *baseReader,
			NavigationReaderIterator baseIt);

	bool navigateReaderDetail(
			OpContext &cxt, uint64_t pos, TupleListReader *&reader,
			NavigationReaderIterator &it);

	NavigationReaderIterator prepareNavigableEntry(
			OpContext &cxt, uint64_t pos);
	void applyNavigatedEntry(NavigationReaderIterator it, bool done);

	NavigationReaderIterator popLeastUseEntry(
			OpContext &cxt, NavigationReaderIterator baseIt);

	void addDistanceEntry(NavigationReaderIterator it);
	void removeDistanceEntry(NavigationReaderIterator it);

	std::pair<bool, bool> findNearEntry(
			NavigationReaderIterator baseIt, NavigationReaderIterator &preceding,
			NavigationReaderIterator &following);

	void inheritLocatedReader(
			OpContext &cxt, NavigationReaderIterator src,
			NavigationReaderIterator dest);
	void applyLocatedReader(
			OpContext &cxt, NavigationReaderIterator it, uint64_t pos,
			TupleListReader &baseReader);

	uint64_t distanceof(
			NavigationReaderIterator preceding,
			NavigationReaderIterator following);
	void checkReaderIterator(NavigationReaderIterator it);

	static NavigationReaderList makeReaderList(
			util::StackAllocator &alloc, uint32_t count);

	NavigationReaderList readerList_;
	uint64_t firstNavigablePosition_;
	bool firstPositionNavigated_;
	uint32_t activeReaderCount_;

	LocatedNavigationReaderSet posSet_;
	LocatedNavigationReaderSet distanceSet_;
	NavigationReaderRefSet prevOnlySet_;
	NavigationReaderRefSet unusedSet_;
};

class SQLSortOps::WindowMatch::ColumnNavigator {
public:
	ColumnNavigator(
			OpContext &cxt, const NavigationMap &navMap,
			NavigationReaderSet &readerSet);

	bool navigate(OpContext &cxt, VariableTable &variableTable);
	void clear();

private:
	const SQLExprs::Expression& resolveExpression(
			const NavigationKey &key, bool forAssignment, uint32_t navIndex);

	static NavigationExprMap makeExpressionMap(
			util::StackAllocator &alloc, const NavigationMap &navMap,
			bool forAssignment);

	NavigationReaderSet &readerSet_;

	NavigationExprMap assignmentExprMap_;
	NavigationExprMap emptyExprMap_;

	NavigationType type_;
	uint32_t ordinal_;
};

struct SQLSortOps::WindowMatch::MatchContext {
	MatchContext(OpContext &cxt, const OpCode &code);

	const Projection* findProjectionAt(NavigationType type);

	MicroCompiler compiler_;
	VariablePredicateMap predMap_;
	NavigationMap navMap_;

	PatternGraph patternGraph_;
	VariableTable variableTable_;

	NavigationReaderSet navReaderSet_;
	ColumnNavigator navigator_;

	NavigationProjectionRef proj_;
	util::LocalUniquePtr<TupleEq> partEq_;

	uint64_t memoryLimit_;
	bool allRowsPerMatch_;

	bool keyReaderStarted_;
	uint64_t predPos_;
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
