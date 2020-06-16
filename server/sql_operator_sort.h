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

#include "sql_operator.h"

#include "sql_utils_algorithm.h"

struct SQLSortOps {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::ReadableTuple ReadableTuple;
	typedef SQLOps::WritableTuple WritableTuple;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::OpConfig OpConfig;

	typedef SQLAlgorithmUtils::Sorter<
			ReadableTuple, SQLValues::TupleLess> TupleSorter;
	typedef SQLAlgorithmUtils::HeapQueue<
			SQLValues::ReadableTupleRef,
			SQLValues::TupleGreater> TupleHeapQueue;

	typedef util::Vector<uint32_t> SortStage;
	typedef util::Vector<SortStage> SortStageList;

	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpNode OpNode;
	typedef SQLOps::OpPlan OpPlan;

	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef SQLOps::Projection Projection;

	class Registrar;

	class Sort;
	class SortNway;

	class Window;
	class WindowPartition;
	class WindowExtract;
	class WindowSelect;

	struct SortContext;
};

class SQLSortOps::Registrar : public SQLOps::OpRegistrar {
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
	virtual void execute(OpContext &cxt) const;

private:
	bool partialSort(SortContext &cxt) const;
	bool sortMerge(SortContext &cxt) const;
	bool nextStage(SortContext &cxt) const;

	TupleSorter& preapareSorter(SortContext &cxt) const;
	TupleHeapQueue createHeapQueue(SortContext &cxt) const;

	const SQLOps::Projection* prepareSortProjection(SortContext &cxt) const;
	const SQLOps::Projection* prepareMergeProjection(SortContext &cxt) const;
	void prepareProjectionOutput(SortContext &cxt) const;

	bool isSorting(SortContext &cxt) const;
	bool isMerging(SortContext &cxt) const;
	bool isFinalStage(SortContext &cxt) const;

	uint32_t allocateStageElement(SortContext &cxt) const;
	void releaseStageElement(SortContext &cxt, uint32_t elem) const;

	SortContext& getSortContext(OpContext &cxt) const;

	size_t getSorterCapacity(util::StackAllocator &alloc) const;
	size_t getMergeWidth() const;
};

struct SQLSortOps::SortContext {
	SortContext(util::StackAllocator &alloc);

	OpContext& getBase();
	util::StackAllocator& getAllocator();

	OpContext *baseCxt_;

	uint32_t workingDepth_;
	TupleSorter *sorter_;
	SortStageList stageList_;

	SortStage freeElemList_;
};
