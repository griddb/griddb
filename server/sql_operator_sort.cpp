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

#include "sql_operator_sort.h"
#include "sql_operator_utils.h"


const SQLOps::OpRegistrar
SQLSortOps::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLSortOps::Registrar::operator()() const {
	add<SQLOpTypes::OP_SORT, Sort>();
	add<SQLOpTypes::OP_SORT_NWAY, SortNway>();
}


void SQLSortOps::Sort::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SORT_NWAY);

	node.addInput(plan.getParentInput(0));
	plan.getParentOutput(0).addInput(node);

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	if (code.getMiddleProjection() == NULL) {
		code.setMiddleProjection(&builder.createIdenticalProjection(false, 0));
	}

	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, false, true);

	projections.first =
			&createSubLimitedProjection(builder, *projections.first, code);

	code.setPipeProjection(projections.first);
	code.setFinishProjection(projections.second);

	node.setCode(code);
}

SQLOps::Projection& SQLSortOps::Sort::createSubLimitedProjection(
		SQLOps::OpCodeBuilder &builder, Projection &src, OpCode &code) {
	const int64_t limit = code.getSubLimit();
	const int64_t offset = code.getSubOffset();
	if (limit < 0 && offset <= 0) {
		return src;
	}

	code.setSubLimit(-1);
	code.setSubOffset(-1);

	SQLOps::Projection &dest =
			builder.createProjection(SQLOpTypes::PROJ_SUB_LIMIT);
	dest.getProjectionCode().setKeyList(&code.getKeyColumnList());

	dest.addChild(&builder.createConstExpr(
			SQLValues::ValueUtils::toAnyByNumeric(limit)));
	dest.addChild(&builder.createConstExpr(
			SQLValues::ValueUtils::toAnyByNumeric(offset)));

	dest.addChain(src);

	return dest;
}


void SQLSortOps::SortNway::execute(OpContext &cxt) const {
	for (SortContext &sortCxt = getSortContext(cxt); nextStage(sortCxt);) {
		if (isMerging(sortCxt)) { 
			if (!sortMerge(sortCxt)) {
				break; 
			}
		}
		else {
			if (!partialSort(sortCxt)) {
				break; 
			}
		}
	}
}

bool SQLSortOps::SortNway::partialSort(SortContext &cxt) const {
	TupleSorter &sorter = preapareSorter(cxt);
	TupleListReader &reader = cxt.getBase().getReader(0);
	for (;; reader.next()) {
		if (sorter.isFilled()) {
			break;
		}
		else if (!reader.exists()) {
			if (!cxt.getBase().isAllInputCompleted()) {
				return false;
			}
			break;
		}
		sorter.add(reader.get());
	}

	while (!sorter.isSorted()) {
		if (cxt.getBase().checkSuspended()) { 
			return false;
		}
		sorter.sort();
	}

	const SQLOps::Projection *projection = prepareSortProjection(cxt);
	for (TupleSorter::Iterator it = sorter.begin(); it != sorter.end(); ++it) {
		projection->projectBy(cxt.getBase(), *it);
	}
	sorter.clear();

	return true;
}

bool SQLSortOps::SortNway::sortMerge(SortContext &cxt) const {
	TupleHeapQueue queue = createHeapQueue(cxt);

	const SQLOps::Projection *projection = prepareMergeProjection(cxt);
	while (!queue.isEmpty()) {
		const TupleHeapQueue::Element &elem = queue.pop();
		TupleListReader &reader = elem.getValue().getReader();

		projection->projectBy(cxt.getBase(), reader, elem.getOrdinal());

		reader.next();
		if (cxt.getBase().checkSuspended()) {
			return false;
		}
		if (!reader.exists()) {
			continue;
		}
		queue.push(elem);
	}

	return true;
}

bool SQLSortOps::SortNway::nextStage(SortContext &cxt) const {
	cxt.getBase().setReaderRandomAccess(0);

	const bool topCompleted =
			(cxt.getBase().isAllInputCompleted() &&
			!cxt.getBase().getReader(0).exists() &&
			cxt.sorter_ != NULL &&
			(cxt.sorter_->isSorted() || cxt.sorter_->isEmpty()));

	if (topCompleted) {
		cxt.getBase().releaseReaderLatch(0);
	}

	if (isMerging(cxt) && !createHeapQueue(cxt).isEmpty()) {
		return true;
	}

	if (isFinalStage(cxt)) {
		return false;
	}

	const size_t mergeWidth = getMergeWidth();
	while (cxt.workingDepth_ < cxt.stageList_.size()) {
		const uint32_t lastDepth = cxt.workingDepth_;
		const uint32_t nextDepth = ++cxt.workingDepth_;

		assert(!cxt.stageList_[lastDepth].empty());
		cxt.getBase().closeLocalWriter(cxt.stageList_[lastDepth].back());

		if (lastDepth > 0) {
			SortStage &stage = cxt.stageList_[lastDepth - 1];
			for (SortStage::iterator it = stage.begin();
					it != stage.end(); ++it) {
				releaseStageElement(cxt, *it);
			}
			stage.clear();
		}

		if (!topCompleted && cxt.stageList_[lastDepth].size() < mergeWidth) {
			break;
		}

		if (nextDepth >= cxt.stageList_.size()) {
			cxt.stageList_.push_back(SortStage(cxt.baseCxt_->getAllocator()));
		}
		if (!topCompleted || !isFinalStage(cxt)) {
			cxt.stageList_[nextDepth].push_back(allocateStageElement(cxt));
		}
		return true;
	}

	if (cxt.stageList_.empty()) {
		cxt.stageList_.push_back(SortStage(cxt.baseCxt_->getAllocator()));
	}
	cxt.stageList_[0].push_back(allocateStageElement(cxt));
	cxt.workingDepth_ = std::numeric_limits<uint32_t>::max();

	cxt.getBase().keepReaderLatch(0);
	return true;
}

SQLSortOps::TupleSorter& SQLSortOps::SortNway::preapareSorter(
		SortContext &cxt) const {
	if (!isSorting(cxt)) {
		if (cxt.sorter_ == NULL) {
			util::StackAllocator &alloc = cxt.getAllocator();
			cxt.sorter_ = ALLOC_NEW(alloc) TupleSorter(
					getSorterCapacity(alloc),
					getCode().getLessPredicate(), alloc);
		}

		cxt.workingDepth_ = 0;
		assert(isSorting(cxt));
	}

	assert(cxt.sorter_ != NULL);
	return *cxt.sorter_;
}

SQLSortOps::TupleHeapQueue SQLSortOps::SortNway::createHeapQueue(
		SortContext &cxt) const {
	assert(isMerging(cxt));
	SortStage &stage = cxt.stageList_[cxt.workingDepth_ - 1];

	TupleHeapQueue queue(getCode().getGreaterPredicate(), cxt.getAllocator());

	for (SortStage::iterator it = stage.begin(); it != stage.end(); ++it) {
		TupleListReader &reader = cxt.getBase().getLocalReader(*it);
		if (!reader.exists()) {
			continue;
		}
		queue.push(queue.newElement(SQLValues::ReadableTupleRef(reader)));
	}

	return queue;
}

const SQLOps::Projection* SQLSortOps::SortNway::prepareSortProjection(
		SortContext &cxt) const {
	prepareProjectionOutput(cxt);
	return getCode().getMiddleProjection();
}

const SQLOps::Projection* SQLSortOps::SortNway::prepareMergeProjection(
		SortContext &cxt) const {
	prepareProjectionOutput(cxt);
	return isFinalStage(cxt) ?
			getCode().getPipeProjection() :
			getCode().getMiddleProjection();
}

void SQLSortOps::SortNway::prepareProjectionOutput(SortContext &cxt) const {
	uint32_t outElem;
	if (isFinalStage(cxt)) {
		outElem = 0;
	}
	else {
		outElem = cxt.stageList_[cxt.workingDepth_].back();
	}
	cxt.getBase().setLocalLink(0, outElem);
}

bool SQLSortOps::SortNway::isSorting(SortContext &cxt) const {
	return (cxt.workingDepth_ == 0);
}

bool SQLSortOps::SortNway::isMerging(SortContext &cxt) const {
	return (0 < cxt.workingDepth_ &&
			cxt.workingDepth_ < cxt.stageList_.size());
}

bool SQLSortOps::SortNway::isFinalStage(SortContext &cxt) const {
	return (!cxt.stageList_.empty() &&
			cxt.workingDepth_ > 0 &&
			cxt.workingDepth_ == cxt.stageList_.size() - 1 &&
			cxt.stageList_.back().empty());
}

uint32_t SQLSortOps::SortNway::allocateStageElement(SortContext &cxt) const {
	uint32_t elem;
	if (cxt.freeElemList_.empty()) {
		elem = cxt.getBase().createLocal();
	}
	else {
		elem = cxt.freeElemList_.back();
		cxt.freeElemList_.pop_back();
	}

	cxt.getBase().createLocalTupleList(elem);
	return elem;
}

void SQLSortOps::SortNway::releaseStageElement(
		SortContext &cxt, uint32_t elem) const {
	cxt.getBase().closeLocalTupleList(elem);
	cxt.freeElemList_.push_back(elem);
}

SQLSortOps::SortContext& SQLSortOps::SortNway::getSortContext(
		OpContext &cxt) const {
	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(
				cxt.getAllocator(), SortContext, cxt.getAllocator());
	}

	SortContext &sortCxt = cxt.getResource(0).resolveAs<SortContext>();
	sortCxt.baseCxt_ = &cxt;

	return sortCxt;
}

size_t SQLSortOps::SortNway::getSorterCapacity(
		util::StackAllocator &alloc) const {
	const int64_t confValue = OpConfig::resolve(
			SQLOpTypes::CONF_SORT_PARTIAL_UNIT, getCode().getConfig());

	if (confValue >= 0) {
		const uint64_t maxValue =
				static_cast<uint64_t>(std::numeric_limits<size_t>::max());
		return static_cast<size_t>(std::max<uint64_t>(std::min(
				static_cast<uint64_t>(confValue), maxValue), 1));
	}

	const size_t blockSize = alloc.base().getElementSize();
	const size_t headSize = 32; 
	assert(blockSize > headSize);

	const size_t capacity = (blockSize - headSize) / sizeof(ReadableTuple);
	assert(capacity > 0);

	return capacity;
}

size_t SQLSortOps::SortNway::getMergeWidth() const {
	const int64_t defaultValue = 32;
	const int64_t confValue = OpConfig::resolve(
			SQLOpTypes::CONF_SORT_MERGE_UNIT, getCode().getConfig());

	if (confValue < 0) {
		return defaultValue;
	}

	const int64_t maxValue =
			static_cast<int64_t>(std::numeric_limits<uint32_t>::max());
	return static_cast<size_t>(std::max<uint64_t>(
			std::min(confValue, maxValue), 2));
}

SQLSortOps::SortContext::SortContext(util::StackAllocator &alloc) :
		baseCxt_(NULL),
		workingDepth_(std::numeric_limits<uint32_t>::max()),
		sorter_(NULL),
		stageList_(alloc),
		freeElemList_(alloc) {
}

SQLOps::OpContext& SQLSortOps::SortContext::getBase() {
	assert(baseCxt_ != NULL);
	return *baseCxt_;
}

util::StackAllocator& SQLSortOps::SortContext::getAllocator() {
	return baseCxt_->getAllocator();
}
