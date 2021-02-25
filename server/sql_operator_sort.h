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

	typedef SQLValues::TupleDigester::WithAccessor<
			SQLValues::ValueAccessor::ByReader, 2> TupleDigester;

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

	struct SortContext;

	class Window;
	class WindowPartition;
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
};

class SQLSortOps::SortNway : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

private:
	class SorterBuilder;
	class SorterWriter;
	template<bool Fixed, bool Limited> struct MergeAction;

	struct GroupSortAction;
	template<typename InputUnique> struct GroupMergeAction;

	bool partialSort(SortContext &cxt) const;
	bool sortMerge(SortContext &cxt) const;
	bool nextStage(SortContext &cxt) const;

	TupleSorter& prepareSorter(
			SortContext &cxt,
			const SQLValues::CompColumnList &keyColumnList) const;
	TupleHeapQueue createHeapQueue(
			SortContext &cxt, bool primary,
			const SQLValues::CompColumnList &keyColumnList,
			TupleHeapRefList &refList) const;

	bool isMergeCompleted(SortContext &cxt) const;

	ProjectionRefPair prepareSortProjection(
			SortContext &cxt, bool primary) const;
	ProjectionRefPair prepareMergeProjection(
			SortContext &cxt, bool primary) const;

	ProjectionRefPair findProjection(bool merging, bool forFinal) const;

	bool isGrouping(bool merging, bool forFinal) const;
	bool isDuplicateGroupMerging() const;
	bool isUnique() const;
	bool isKeyFiltering() const;
	void updateProjections(
			SortContext &cxt, const ProjectionRefPair &projectionPair) const;
	void prepareProjectionOutput(SortContext &cxt, bool primary) const;

	bool isSorting(SortContext &cxt) const;
	bool isMerging(SortContext &cxt) const;
	bool isFinalStage(SortContext &cxt) const;

	bool isPrimaryWorking(
			size_t ordinal,
			const SQLValues::CompColumnList &keyColumnList) const;

	void finshMerging(SortContext &cxt, bool primary) const;

	uint32_t prepareStageElement(
			SortContext &cxt, SortStageElement &elem, bool primary) const;
	void finishStageElement(SortContext &cxt, SortStageElement &elem) const;
	void releaseStageElement(
			SortContext &cxt, SortStageElement &elem,
			const bool *primaryRef) const;

	SortContext& getSortContext(OpContext &cxt) const;

	bool checkSorterBuilding(
			SortContext &cxt, TupleSorter &sorter,
			TupleListReader &reader) const;
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
	typedef bool RetType;

	template<typename T> class TypeAt {
	public:
		explicit TypeAt(SorterBuilder &base) : base_(base) {}

		bool operator()() {
			return base_.buildAt<T>();
		}

	private:
		SorterBuilder &base_;
	};

	SorterBuilder(
			SortContext &cxt, TupleSorter &sorter, TupleListReader &reader,
			const SQLValues::CompColumnList &keyColumnList, bool keyFiltering);

	bool isInterruptible();

	bool build();

private:
	template<typename T> bool buildAt();

	bool checkEmpty();

	TupleSorter &sorter_;
	TupleListReader &reader_;
	SQLValues::TupleNullChecker::WithAccessor<
			SQLValues::ValueAccessor::ByReader> nullChecker_;
	TupleDigester digester_;
	TupleDigester digester2_;
	bool keyFiltering_;
	SummaryTupleSet &tupleSet1_;
	SummaryTupleSet &tupleSet2_;
	uint64_t nextInterruption_;
};

class SQLSortOps::SortNway::SorterWriter {
public:
	template<typename T>
	class TypeAt {
	public:
		explicit TypeAt(SorterWriter &base) : base_(base) {}

		void operator()() const {
			typedef typename SQLOpUtils::ExpressionListWriter::ByDigestTuple::
					template TypeAt<T> Projector;
			base_.writeBy(Projector(base_.writer_));
		}

	private:
		SorterWriter &base_;
	};

	SorterWriter(
			OpContext &cxt, TupleSorter &sorter, const Projection &proj,
			const SQLValues::CompColumnList &keyColumnList, bool primary);

	void write();

private:
	typedef void (*TypedFunc)(SorterWriter&);

	template<typename P> void writeBy(const P &projector);

	template<bool Rev>
	static TypedFunc getTypedFunction(TupleColumnType type);

	OpContext &cxt_;
	TupleSorter &sorter_;
	const Projection &proj_;
	SQLOpUtils::ExpressionListWriter writer_;
	bool primary_;
};

template<bool Fixed, bool Limited>
struct SQLSortOps::SortNway::MergeAction {
	struct Options {
		typedef typename util::BoolType<Fixed>::Result FixedDigest;
	};
	typedef Options OptionsType;

	typedef typename util::Conditional<
			Fixed,
			SQLOpUtils::ExpressionListWriter::ByDigestTuple,
			SQLOpUtils::ExpressionListWriter::ByProjection>::Type ProjectorType;
	typedef typename ProjectorType::BaseType BaseType;

	template<typename T>
	struct TypeAt {
		typedef TypeAt TypedOp;

		explicit TypeAt(const MergeAction &base) : base_(base) {}
		bool operator()(const TupleHeapQueue::Element &elem) const;

		bool operator()(
				const TupleHeapQueue::Element&, const util::FalseType&) const {
			return true;
		}

		void operator()(
				const TupleHeapQueue::Element &elem, const util::TrueType&) const {
			(*this)(elem);
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

	bool operator()(const TupleHeapQueue::Element &elem) const;

	bool operator()(const TupleHeapQueue::Element&, const util::FalseType&) {
		return true;
	}

	void operator()(
			const TupleHeapQueue::Element &elem, const util::TrueType&) {
		(*this)(elem);
	}

	OpContext &cxt_;
	ProjectorType projector_;
	int64_t &workingRestLimit_;
};

struct SQLSortOps::SortNway::GroupSortAction {
	typedef std::pair<SummaryTupleSet*, SummaryTupleSet*> TupleSetPair;

	GroupSortAction(
			OpContext &cxt,
			const Projection *pipeProj, const Projection *mergeProj,
			SummaryTuple *outTupleRef, const TupleSetPair &tupleSetPair);

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

struct SQLSortOps::SortContext {
	SortContext(SQLValues::ValueContext &cxt);

	OpContext& getBase();
	util::StackAllocator& getAllocator();

	SQLOps::SummaryColumnList& getSummaryColumnList(bool pimary);
	SummaryTupleSet& getSummaryTupleSet(bool pimary);
	SQLValues::CompColumnList& getKeyColumnList(bool pimary);
	bool& getFinalStageStarted(bool pimary);

	OpContext *baseCxt_;

	uint32_t workingDepth_;
	TupleSorter *sorter_;
	bool sorterFilled_;
	std::pair<bool, bool> finalStageStarted_;
	SortStageList stageList_;

	util::Vector<uint32_t> freeStoreList_;

	int64_t workingRestLimit_;

	SQLOps::TupleColumnList middleColumnList_;
	SQLOps::SummaryColumnList summaryColumnList1_;
	SQLOps::SummaryColumnList summaryColumnList2_;

	SummaryTupleSet totalTupleSet_;
	SummaryTupleSet tupleSet1_;
	SummaryTupleSet tupleSet2_;

	SQLValues::CompColumnList keyColumnList1_;
	SQLValues::CompColumnList keyColumnList2_;

	SummaryTuple groupTuple_;
};

class SQLSortOps::Window : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	static OpCode createPartitionCode(
			OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
			SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
			const Projection &pipeProj, const SQLExprs::Expression *windowExpr);

	static OpCode createPositioningJoinCode(OpCodeBuilder &builder);
	static OpCode createPositioningSortCode(OpCodeBuilder &builder);

	static OpCode createMergeCode(
			OpCodeBuilder &builder, SQLOpUtils::ProjectionPair &projections,
			SQLExprs::Expression *&windowExpr, bool positioning);

	static OpCode createSingleCode(
			OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
			SQLOpUtils::ProjectionPair &projections,
			SQLExprs::Expression *&windowExpr);

	static SQLExprs::Expression* splitWindowExpr(
			OpCodeBuilder &builder, Projection &src,
			SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
			bool &valueCounting);

	static Projection& createProjectionWithWindow(
			OpCodeBuilder &builder, const Projection &pipeProj,
			Projection &src, SQLExprs::Expression *&windowExpr);
	static Projection& createUnmatchWindowPipeProjection(
			OpCodeBuilder &builder, const Projection &src);
};

class SQLSortOps::WindowPartition : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;

	static bool toAbsolutePosition(
			int64_t curPos, int64_t relativePos, bool following, int64_t &pos);

private:
	typedef SQLValues::TupleComparator::WithAccessor<
			std::equal_to<SQLValues::ValueComparator::PredArgType>,
			false, false, false, false,
			SQLValues::ValueAccessor::ByReader> TupleEq;
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

class SQLSortOps::WindowMerge : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;

private:
	enum {
		IN_MAIN = 0,
		IN_COUNTING = 1,
		IN_POSITIONING = 2
	};

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
			OpContext &cxt, bool &following) const;

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



template<bool Fixed, bool Limited>
SQLSortOps::SortNway::MergeAction<Fixed, Limited>::MergeAction(
		SortContext &cxt, const BaseType &projectorBase) :
		cxt_(cxt.getBase()),
		projector_(projectorBase),
		workingRestLimit_(cxt.workingRestLimit_) {
}

template<bool Fixed, bool Limited>
inline bool SQLSortOps::SortNway::MergeAction<Fixed, Limited>::operator()(
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

template<bool Fixed, bool Limited>
template<typename T>
inline bool SQLSortOps::SortNway::MergeAction<Fixed, Limited>::TypeAt<T>::operator()(
		const TupleHeapQueue::Element &elem) const {
	const bool reversed =
			T::VariantTraitsType::OfValueComparator::ORDERING_REVERSED;
	typedef SQLValues::ValueComparator::VariantTraits<
			void, !reversed, void, void> VariantType;
	typedef typename T::template VariantRebind<VariantType>::Type TraitsType;

	base_.projector_.template projectorAt<TraitsType>().projectBy(
			base_.cxt_, elem.getValue());

	if (Limited) {
		assert(base_.workingRestLimit_ > 0);
		if (--base_.workingRestLimit_ <= 0) {
			return false;
		}
	}


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
