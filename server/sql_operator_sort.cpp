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

	add<SQLOpTypes::OP_WINDOW, Window>();
	add<SQLOpTypes::OP_WINDOW_PARTITION, WindowPartition>();
	add<SQLOpTypes::OP_WINDOW_MERGE, WindowMerge>();

	addProjection<SQLOpTypes::PROJ_PIPE_FINISH, NonExecutableProjection>();
	addProjection<SQLOpTypes::PROJ_MULTI_STAGE, NonExecutableProjection>();
	addProjection<SQLOpTypes::PROJ_KEY_FILTER, NonExecutableProjection>();
}


void SQLSortOps::Sort::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SORT_NWAY);

	node.addInput(plan.getParentInput(0));

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	if (code.findMiddleKeyColumnList() == NULL) {
		code.setMiddleKeyColumnList(&code.getKeyColumnList());
	}

	int64_t localLimit = -1;
	do {
		const int64_t limit = code.getLimit();
		if (limit < 0) {
			break;
		}
		const int64_t offset = code.getOffset();
		if (offset > 0 && limit > std::numeric_limits<int64_t>::max() - offset) {
			break;
		}
		localLimit = limit + (offset > 0 ? offset : 0);
	}
	while (false);

	code.setLimit(localLimit);

	const Projection *midProj = code.getMiddleProjection();
	if (midProj != NULL) {
		builder.assignInputTypesByProjection(*midProj, NULL);
	}

	Projection *distinctProj;
	const bool withLimit = (localLimit < 0 || code.getOffset() > 0);
	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, false, withLimit, &distinctProj);

	projections.first =
			&createSubLimitedProjection(builder, *projections.first, code);

	if (midProj == NULL ||
			projections.first->getProjectionCode().getType() !=
			SQLOpTypes::PROJ_MULTI_STAGE) {
		const size_t projCount = 2;
		Projection *projList[projCount] = { NULL };
		for (size_t i = 0; i < projCount; i++) {
			Projection &proj = builder.createIdenticalProjection(false, 0);
			proj.getProjectionCode().setKeyList(&code.getKeyColumnList());
			projList[i] = &proj;
		}
		if (code.getMiddleProjection() == NULL) {
			code.setMiddleProjection(projList[0]);
		}
		if (projections.first->getProjectionCode().getType() !=
				SQLOpTypes::PROJ_MULTI_STAGE) {
			projections.first = &builder.createMultiStageProjection(
					builder.rewriteProjection(*projections.first),
					*projList[1]);
		}
	}

	code.setPipeProjection(projections.first);
	code.setFinishProjection(projections.second);

	node.setCode(code);

	OpCodeBuilder::setUpOutputNodes(
			plan, NULL, node, distinctProj, getCode().getAggregationPhase());
}

SQLOps::Projection& SQLSortOps::Sort::createSubLimitedProjection(
		SQLOps::OpCodeBuilder &builder, Projection &src, OpCode &code) {
	const int64_t limit = code.getSubLimit();
	const int64_t offset = code.getSubOffset();
	if (limit < 0 && offset <= 0) {
		return src;
	}

	const SQLValues::CompColumnList &keyList = code.getKeyColumnList();
	if (limit == 1) {
		const bool unique = (offset < 0);
		const bool singleKeyUnique = (offset <= 0 && keyList.size() <= 1);
		if (unique || singleKeyUnique) {
			return src;
		}
	}

	SQLValues::CompColumnList &subLimitKeyList =
			builder.createCompColumnList();
	subLimitKeyList.push_back(keyList.front());

	code.setSubLimit(-1);
	code.setSubOffset(-1);

	SQLOps::Projection &dest =
			builder.createProjection(SQLOpTypes::PROJ_SUB_LIMIT);
	dest.getProjectionCode().setKeyList(&subLimitKeyList);

	dest.addChild(&builder.createConstExpr(
			SQLValues::ValueUtils::toAnyByNumeric(limit)));
	dest.addChild(&builder.createConstExpr(
			SQLValues::ValueUtils::toAnyByNumeric(offset)));

	dest.addChain(src);

	return dest;
}


void SQLSortOps::SortNway::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_MULTI_STAGE);

	Operator::compile(cxt);
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
	const SQLValues::CompColumnList &keyColumnList =
			getCode().getMiddleKeyColumnList();

	TupleSorter &sorter = prepareSorter(cxt, keyColumnList);
	if (!cxt.sorterFilled_) {
		TupleListReader &reader = cxt.getBase().getReader(0);
		if (!checkSorterBuilding(cxt, sorter, reader)) {
			return false;
		}

		SorterBuilder builder(
				cxt, sorter, reader, keyColumnList, isKeyFiltering());
		while (!builder.build()) {
			if (!builder.isInterruptible()) {
				if (!cxt.getBase().isAllInputCompleted()) {
					return false;
				}
				break;
			}
			cxt.getBase().checkSuspendedAlways();
			if (!checkMemoryLimitReached(cxt, reader)) {
				break;
			}
		}
		cxt.sorterFilled_ = true;
	}

	while (!sorter.isSorted()) {
		if (cxt.getBase().checkSuspended()) { 
			return false;
		}
		if (sorter.isOptional()) {
			sortOptional(cxt, sorter);
		}
		else {
			sorter.sort();
		}
	}

	for (size_t i = 0; i < 2; i++) {
		const bool primary = isPrimaryWorking(i, keyColumnList);
		if (sorter.isEmptyAt(primary)) {
			continue;
		}

		const ProjectionRefPair &projectionPair =
				prepareSortProjection(cxt, primary);

		SorterWriter writer(
				cxt.getBase(), sorter, *projectionPair.first, keyColumnList,
				primary);
		writer.write();
	}

	sorter.clear();

	cxt.tupleSet1_.clearAll();
	cxt.tupleSet2_.clearAll();
	cxt.totalTupleSet_.clearAll();
	cxt.sorterFilled_ = false;
	return true;
}

bool SQLSortOps::SortNway::sortMerge(SortContext &cxt) const {
	const SQLValues::CompColumnList &keyColumnList =
			getCode().getKeyColumnList();

	bool continuable = true;
	for (size_t i = 0; i < 2 && continuable; i++) {
		const bool primary = isPrimaryWorking(i, keyColumnList);

		const ProjectionRefPair &projectionPair =
				prepareMergeProjection(cxt, primary);

		TupleHeapRefList refList(cxt.getBase().getAllocator());
		TupleHeapQueue queue =
				createHeapQueue(cxt, primary, keyColumnList, refList);
		if (queue.isEmpty()) {
			finshMerging(cxt, primary);
			continue;
		}
		assert(cxt.workingRestLimit_ != 0);

		SQLOpUtils::ExpressionListWriter writer(
				SQLOpUtils::ExpressionListWriter::Source(
						cxt.getBase(), *projectionPair.first, !isFinalStage(cxt),
						&keyColumnList, primary,
						getCode().getMiddleProjection(),
						SQLExprs::ExprCode::INPUT_MULTI_STAGE));

		if (writer.isDigestColumnAssigned() && !queue.isPredicateEmpty()) {
			if (isUnique() || cxt.workingRestLimit_ >= 0) {
				continuable = sortMergeDetail(cxt, queue, writer);
			}
			else {
				MergeAction<true, false> action(cxt, writer);
				continuable = queue.merge(action);
			}
		}
		else {
			continuable = sortMergeDetail(cxt, queue, projectionPair, refList);
		}

		if (!continuable) {
			if (cxt.workingRestLimit_ != 0) {
				break;
			}
			continuable = true;
		}

		finshMerging(cxt, primary);
	}

	assert(!continuable == !isMergeCompleted(cxt));
	return continuable;
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

	if (isSorting(cxt) && !cxt.sorter_->isEmpty()) {
		return true;
	}

	if (isMerging(cxt) && !isMergeCompleted(cxt)) {
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
		finishStageElement(cxt, cxt.stageList_[lastDepth].back());

		if (lastDepth > 0) {
			SortStage &stage = cxt.stageList_[lastDepth - 1];
			for (SortStage::iterator it = stage.begin();
					it != stage.end(); ++it) {
				releaseStageElement(cxt, *it, NULL);
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
			cxt.stageList_[nextDepth].push_back(SortStageElement());
		}
		cxt.workingRestLimit_ = getCode().getLimit();
		return true;
	}

	if (cxt.stageList_.empty()) {
		cxt.stageList_.push_back(SortStage(cxt.baseCxt_->getAllocator()));
	}
	cxt.stageList_[0].push_back(SortStageElement());
	cxt.workingDepth_ = std::numeric_limits<uint32_t>::max();

	cxt.getBase().keepReaderLatch(0);
	cxt.workingRestLimit_ = -1;
	return true;
}

SQLSortOps::TupleSorter& SQLSortOps::SortNway::prepareSorter(
		SortContext &cxt,
		const SQLValues::CompColumnList &keyColumnList) const {
	do {
		if (isSorting(cxt)) {
			break;
		}
		cxt.workingDepth_ = 0;

		if (cxt.sorter_ != NULL) {
			break;
		}

		util::StackAllocator &alloc = cxt.getAllocator();

		const Projection *aggrProj = OpCodeBuilder::findAggregationProjection(
				*getCode().getMiddleProjection());

		for (size_t i = 0; i < 2; i++) {
			const bool primary = (i == 0);
			SummaryTupleSet &tupleSet = cxt.getSummaryTupleSet(primary);

			tupleSet.addReaderColumnList(cxt.getBase().getInputColumnList(0));
			tupleSet.addKeyList(keyColumnList, true);
			if (aggrProj != NULL) {
				OpCodeBuilder::applyAggregationColumns(*aggrProj, tupleSet);
			}
			if (primary) {
				tupleSet.setNullKeyIgnorable(true);
			}
			tupleSet.completeColumns();

			cxt.getSummaryColumnList(primary) = tupleSet.getTotalColumnList();

			SQLValues::CompColumnList &subKeyColumnList =
					cxt.getKeyColumnList(primary);
			subKeyColumnList = keyColumnList;
			tupleSet.applyKeyList(subKeyColumnList, NULL);
		}

		std::pair<TupleLess, TupleLess> predPair(
				TupleLess(alloc, SQLValues::TupleComparator(
						cxt.keyColumnList1_, true, true, true)),
				TupleLess(alloc, SQLValues::TupleComparator(
						cxt.keyColumnList2_, true, true, false)));

		TupleSorter *sorter = ALLOC_NEW(alloc) TupleSorter(
				getSorterCapacity(alloc),
				predPair.first, predPair.second, alloc);
		sorter->setPredicateEmpty(true, predPair.first.getBase().isEmpty(0));
		sorter->setPredicateEmpty(false, predPair.second.getBase().isEmpty(1));

		SQLAlgorithmUtils::SortConfig config;

		assert(getCode().getOffset() <= 0);
		config.limit_ = getCode().getLimit();

		if (isUnique() || isGrouping(false, false)) {
			config.unique_ = true;
		}

		sorter->setConfig(config);

		cxt.sorter_ = sorter;
	}
	while (false);

	assert(isSorting(cxt));
	assert(cxt.sorter_ != NULL);
	return *cxt.sorter_;
}

SQLSortOps::TupleHeapQueue SQLSortOps::SortNway::createHeapQueue(
		SortContext &cxt, bool primary,
		const SQLValues::CompColumnList &keyColumnList,
		TupleHeapRefList &refList) const {
	assert(isMerging(cxt));
	SortStage &stage = cxt.stageList_[cxt.workingDepth_ - 1];

	util::StackAllocator &alloc = cxt.getAllocator();

	const bool nullIgnorable = primary;
	TupleGreater pred(
			alloc,
			SQLValues::TupleComparator(keyColumnList, true, true, nullIgnorable));
	TupleHeapQueue queue(pred, alloc);
	queue.setPredicateEmpty(pred.getBase().isEmpty((primary ? 0 : 1)));

	TupleDigester digester(
			alloc, SQLValues::TupleDigester(keyColumnList, NULL, nullIgnorable));

	const bool finalStage = isFinalStage(cxt);
	const bool duplicateGroupMerging =
			(finalStage && isDuplicateGroupMerging());

	for (SortStage::iterator it = stage.begin(); it != stage.end(); ++it) {
		if (!it->isAssigned(primary) || cxt.workingRestLimit_ == 0) {
			continue;
		}

		TupleListReader &reader =
				cxt.getBase().getLocalReader(it->resolve(primary));
		if (!reader.exists()) {
			continue;
		}

		queue.push(
				queue.newElement(ReadableTupleRef(alloc, reader, digester)));

		if (duplicateGroupMerging) {
			TupleListReader &subReader =
					cxt.getBase().getLocalReader(it->resolve(primary), 1);
			if (!cxt.getFinalStageStarted(primary) && subReader.exists()) {
				subReader.next();
			}

			if (subReader.exists()) {
				refList.push_back(TupleHeapQueue::Element(
						ReadableTupleRef(alloc, subReader, digester),
						refList.size()));
			}
			else {
				refList.push_back(TupleHeapQueue::Element(
						ReadableTupleRef(util::FalseType()), refList.size()));
			}
		}
	}

	if (finalStage) {
		cxt.getFinalStageStarted(primary) = true;
	}

	return queue;
}

bool SQLSortOps::SortNway::isMergeCompleted(SortContext &cxt) const {
	assert(isMerging(cxt));
	SortStage &stage = cxt.stageList_[cxt.workingDepth_ - 1];

	for (SortStage::iterator it = stage.begin(); it != stage.end(); ++it) {
		for (size_t i = 0; i < 2; i++) {
			const bool primary = (i == 0);

			if (!it->isAssigned(primary)) {
				continue;
			}

			TupleListReader &reader =
					cxt.getBase().getLocalReader(it->resolve(primary));
			if (reader.exists()) {
				return false;
			}
		}
	}

	return true;
}

SQLOpUtils::ProjectionRefPair SQLSortOps::SortNway::prepareSortProjection(
		SortContext &cxt, bool primary) const {
	prepareProjectionOutput(cxt, primary);

	*cxt.getBase().getSummaryColumnListRef(0) =
			cxt.getSummaryColumnList(primary);

	const ProjectionRefPair &projectionPair = findProjection(false, true);
	updateProjections(cxt, projectionPair);

	cxt.getBase().setUpExprContext();

	SQLExprs::ExprContext &exprCxt = cxt.getBase().getExprContext();

	exprCxt.setReader(0, NULL, &cxt.getBase().getInputColumnList(0));
	exprCxt.setInputSourceType(0, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE);

	exprCxt.setAggregationTupleRef(
			&cxt.getSummaryTupleSet(primary), exprCxt.getSummaryTupleRef(0));

	return projectionPair;
}

SQLOpUtils::ProjectionRefPair SQLSortOps::SortNway::prepareMergeProjection(
		SortContext &cxt, bool primary) const {
	prepareProjectionOutput(cxt, primary);

	if (cxt.middleColumnList_.empty()) {
		SQLOps::TupleColumnList &columnList = cxt.middleColumnList_;

		SQLOps::ColumnTypeList typeList(cxt.getAllocator());
		OpCodeBuilder::resolveColumnTypeList(
				*getCode().getMiddleProjection(), typeList, NULL);
		assert(!typeList.empty());

		TupleList::Info info;
		SQLValues::TypeUtils::setUpTupleInfo(info, &typeList[0], typeList.size());

		columnList.resize(typeList.size());
		info.getColumns(&columnList[0], columnList.size());
	}

	const ProjectionRefPair projectionPair = findProjection(true, isFinalStage(cxt));
	updateProjections(cxt, projectionPair);

	cxt.getBase().setUpExprContext();

	SQLExprs::ExprContext &exprCxt = cxt.getBase().getExprContext();

	exprCxt.setReader(0, NULL, &cxt.middleColumnList_);
	exprCxt.setInputSourceType(0, SQLExprs::ExprCode::INPUT_READER_MULTI);

	return projectionPair;
}

SQLOpUtils::ProjectionRefPair SQLSortOps::SortNway::findProjection(
		bool merging, bool forFinal) const {
	const SQLOps::Projection *proj = (merging ?
			getCode().getPipeProjection() : getCode().getMiddleProjection());
	const SQLOps::Projection *finishProj = NULL;

	if (merging) {
		if (proj->getProjectionCode().getType() == SQLOpTypes::PROJ_MULTI_STAGE) {
			if (forFinal) {
				proj = &proj->chainAt(0);
			}
			else {
				proj = &proj->chainAt(1);
			}
		}
		else if (!forFinal) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
	}
	else {
		if (proj->getProjectionCode().getType() == SQLOpTypes::PROJ_KEY_FILTER) {
			proj = &proj->chainAt(0);
		}
	}

	if (proj->getProjectionCode().getType() == SQLOpTypes::PROJ_PIPE_FINISH) {
		finishProj = &proj->chainAt(1);
		proj = &proj->chainAt(0);
	}

	if (!merging) {
		if (forFinal) {
			if (finishProj != NULL) {
				proj = finishProj;
				finishProj = NULL;
			}
		}
		else {
			if (proj->getProjectionCode().getType() ==
					SQLOpTypes::PROJ_MULTI_STAGE) {
				finishProj = &proj->chainAt(0);
				proj = &proj->chainAt(1);
			}
		}
	}

	return ProjectionRefPair(proj, finishProj);
}

bool SQLSortOps::SortNway::isGrouping(bool merging, bool forFinal) const {
	return (findProjection(merging, forFinal).second != NULL);
}

bool SQLSortOps::SortNway::isDuplicateGroupMerging() const {
	return isGrouping(true, true) && !isGrouping(true, false);
}

bool SQLSortOps::SortNway::isUnique() const {
	return (getCode().getSubLimit() == 1 && getCode().getSubOffset() <= 0);
}

bool SQLSortOps::SortNway::isKeyFiltering() const {
	const SQLOps::Projection *proj = getCode().getMiddleProjection();
	return (proj->getProjectionCode().getType() == SQLOpTypes::PROJ_KEY_FILTER);
}

void SQLSortOps::SortNway::updateProjections(
		SortContext &cxt, const ProjectionRefPair &projectionPair) const {
	{
		const SQLOps::Projection *proj = projectionPair.first;
		assert(proj != NULL);
		proj->updateProjectionContext(cxt.getBase());
	}

	{
		const SQLOps::Projection *proj = projectionPair.second;
		if (proj != NULL) {
			proj->updateProjectionContext(cxt.getBase());
		}
	}
}

void SQLSortOps::SortNway::prepareProjectionOutput(
		SortContext &cxt, bool primary) const {
	uint32_t outIndex;
	if (isFinalStage(cxt)) {
		outIndex = 0;
	}
	else {
		outIndex = prepareStageElement(
				cxt, cxt.stageList_[cxt.workingDepth_].back(), primary);
	}
	cxt.getBase().setLocalLink(0, outIndex);
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

bool SQLSortOps::SortNway::isPrimaryWorking(
		size_t ordinal,
		const SQLValues::CompColumnList &keyColumnList) const {
	const SQLValues::CompColumn &column = keyColumnList.front();
	const bool ascending = (column.isAscending() || !column.isOrdering());
	if (ascending && SQLValues::TupleDigester::isOrderingAvailable(
			getCode().getMiddleKeyColumnList(), false)) {
		return (ordinal == 0);
	}
	else {
		return (ordinal != 0);
	}
}

void SQLSortOps::SortNway::finshMerging(SortContext &cxt, bool primary) const {
	assert(cxt.workingDepth_ > 0);

	SortStage &stage = cxt.stageList_[cxt.workingDepth_ - 1];
	for (SortStage::iterator it = stage.begin();
			it != stage.end(); ++it) {
		releaseStageElement(cxt, *it, &primary);
	}
}

uint32_t SQLSortOps::SortNway::prepareStageElement(
		SortContext &cxt, SortStageElement &elem, bool primary) const {
	if (!elem.isAssigned(primary)) {
		uint32_t storeIndex;
		if (cxt.freeStoreList_.empty()) {
			SQLOps::ColumnTypeList typeList(cxt.getAllocator());
			OpCodeBuilder::resolveColumnTypeList(
					*getCode().getMiddleProjection(), typeList, NULL);

			storeIndex = cxt.getBase().createLocal(&typeList);
		}
		else {
			storeIndex = cxt.freeStoreList_.back();
			cxt.freeStoreList_.pop_back();
		}
		cxt.getBase().createLocalTupleList(storeIndex);
		elem.assign(primary, storeIndex);
	}
	return elem.resolve(primary);
}

void SQLSortOps::SortNway::finishStageElement(
		SortContext &cxt, SortStageElement &elem) const {
	for (size_t i = 0; i < 2; i++) {
		const bool primary = (i == 0);
		if (!elem.isAssigned(primary)) {
			continue;
		}

		const uint32_t storeIndex = elem.resolve(primary);
		cxt.getBase().closeLocalWriter(storeIndex);
	}
}

void SQLSortOps::SortNway::releaseStageElement(
		SortContext &cxt, SortStageElement &elem,
		const bool *primaryRef) const {
	for (size_t i = 0; i < 2; i++) {
		const bool primary = (i == 0);
		if (primaryRef != NULL && !primary != !*primaryRef) {
			continue;
		}

		if (!elem.isAssigned(primary)) {
			continue;
		}

		const uint32_t storeIndex = elem.resolve(primary);
		elem.clear(primary);

		cxt.getBase().closeLocalTupleList(storeIndex);
		cxt.freeStoreList_.push_back(storeIndex);
	}
}

SQLSortOps::SortContext& SQLSortOps::SortNway::getSortContext(
		OpContext &cxt) const {
	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(
				cxt.getAllocator(), SortContext, cxt.getValueContext());
	}

	SortContext &sortCxt = cxt.getResource(0).resolveAs<SortContext>();
	sortCxt.baseCxt_ = &cxt;

	return sortCxt;
}

bool SQLSortOps::SortNway::checkSorterBuilding(
		SortContext &cxt, TupleSorter &sorter,
		TupleListReader &reader) const {
	do {
		if (!sorter.isEmpty() || cxt.getBase().isAllInputCompleted()) {
			break;
		}

		const int64_t memLimit = OpConfig::resolve(
				SQLOpTypes::CONF_WORK_MEMORY_LIMIT, getCode().getConfig());

		const uint64_t readableSize =
				reader.getTupleList().getInfo().blockSize_ *
				reader.getFollowingBlockCount();

		if (memLimit < 0 || readableSize >= static_cast<uint64_t>(memLimit)) {
			break;
		}

		cxt.getBase().releaseReaderLatch(0);
		return false;
	}
	while (false);


	cxt.getBase().keepReaderLatch(0);
	return true;
}

bool SQLSortOps::SortNway::checkMemoryLimitReached(
		SortContext &cxt, TupleListReader &reader) const {
	const int64_t memLimit = OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, getCode().getConfig());

	const uint64_t latchSize =
			reader.getTupleList().getInfo().blockSize_ *
			reader.getReferencingBlockCount();

	static_cast<void>(cxt);
	const uint64_t workUsage = 0;

	if (memLimit >= 0 &&
			latchSize + workUsage > static_cast<uint64_t>(memLimit)) {
		return false;
	}

	return true;
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

	const int64_t memLimit = 32 * 1024 * 1024 / 2;

	size_t blockSize = alloc.base().getElementSize();
	if (memLimit >= 0) {
		blockSize = static_cast<size_t>(memLimit);
	}

	const size_t headSize = 32; 
	assert(blockSize > headSize);

	const size_t capacity = (blockSize - headSize) / sizeof(SummaryTuple);
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

const SQLOps::Projection& SQLSortOps::SortNway::resolveOutputProjection(
		const Projection &src) {
	if (src.getProjectionCode().getType() == SQLOpTypes::PROJ_AGGR_OUTPUT) {
		return src.chainAt(0);
	}
	return src;
}

bool SQLSortOps::SortNway::sortMergeDetail(
		SortContext &cxt, TupleHeapQueue &queue,
		const ProjectionRefPair &projectionPair,
		TupleHeapRefList &refList) const {
	if (cxt.workingRestLimit_ >= 0) {
		MergeAction<false, true> action(cxt, *projectionPair.first);
		if (queue.isPredicateEmpty()) {
			return queue.mergeUnchecked(action);
		}
		else {
			return queue.mergeLimited(action);
		}
	}
	else if (isGrouping(true, isFinalStage(cxt))) {
		const bool empty = queue.isPredicateEmpty();
		const bool inputUnique = refList.empty();
		if (empty || inputUnique) {
			GroupMergeAction<util::TrueType> action(
					cxt.getBase(), projectionPair.first, projectionPair.second,
					refList);
			if (empty) {
				return queue.mergeUncheckedUnique(action);
			}
			else {
				return queue.mergeUnique(action);
			}
		}
		else {
			GroupMergeAction<util::FalseType> action(
					cxt.getBase(), projectionPair.first, projectionPair.second,
					refList);
			return queue.mergeUnique(action);
		}
	}
	else {
		MergeAction<false, false> action(cxt, *projectionPair.first);
		const bool unique = isUnique();
		if (queue.isPredicateEmpty()) {
			if (unique) {
				return queue.mergeUncheckedUnique(action);
			}
			else {
				return queue.mergeUnchecked(action);
			}
		}
		else {
			if (unique) {
				return queue.mergeUnique(action);
			}
			else {
				return queue.merge(action);
			}
		}
	}
}

bool SQLSortOps::SortNway::sortMergeDetail(
		SortContext &cxt, TupleHeapQueue &queue,
		SQLOpUtils::ExpressionListWriter &writer) const {
	assert(writer.isDigestColumnAssigned() && !queue.isPredicateEmpty());
	assert(isUnique() || cxt.workingRestLimit_ >= 0);

	if (isUnique()) {
		MergeAction<true, false> action(cxt, writer);
		return queue.mergeUnique(action);
	}
	else {
		MergeAction<true, true> action(cxt, writer);
		return queue.mergeLimited(action);
	}
}

bool SQLSortOps::SortNway::sortOptional(
		SortContext &cxt, TupleSorter &sorter) const {
	if (isGrouping(false, false)) {
		prepareSortProjection(cxt, true);
		const ProjectionRefPair &projectionPair = findProjection(false, false);
		const GroupSortAction::TupleSetPair tupleSetPair(
				&cxt.getSummaryTupleSet(true),
				&cxt.getSummaryTupleSet(false));
		const GroupSortAction action(
				cxt.getBase(), projectionPair.first, projectionPair.second,
				&cxt.groupTuple_, tupleSetPair);
		return sorter.sortGroup(action);
	}
	else {
		return sorter.sortOptional(util::FalseType());
	}
}


SQLSortOps::SortNway::SorterBuilder::SorterBuilder(
		SortContext &cxt, TupleSorter &sorter, TupleListReader &reader,
		const SQLValues::CompColumnList &keyColumnList, bool keyFiltering) :
		sorter_(sorter),
		reader_(reader),
		nullChecker_(cxt.getAllocator(), SQLValues::TupleNullChecker(keyColumnList)),
		digester_(cxt.getAllocator(), SQLValues::TupleDigester(
				keyColumnList, NULL, true, !nullChecker_.getBase().isEmpty())),
		digester2_(cxt.getAllocator(), SQLValues::TupleDigester(
				keyColumnList, NULL, false)),
		keyFiltering_(keyFiltering),
		tupleSet1_(cxt.tupleSet1_),
		tupleSet2_(cxt.tupleSet2_),
		nextInterruption_(cxt.getBase().getNextCountForSuspend()) {
}

bool SQLSortOps::SortNway::SorterBuilder::isInterruptible() {
	return (nextInterruption_ <= 0);
}

bool SQLSortOps::SortNway::SorterBuilder::build() {
	if (checkEmpty()) {
		return false;
	}
	if (nextInterruption_ <= 1) {
		nextInterruption_ = 2;
	}
	return digester_.getTypeSwitcher().get<SorterBuilder>()(*this);
}

template<typename T>
bool SQLSortOps::SortNway::SorterBuilder::buildAt() {
	assert(nextInterruption_ > 0);

	typedef typename TupleDigester::template TypeAt<
			typename T::NonNullableTraitsType>::TypedOp TypedDigester;

	for (;; reader_.next()) {
		if (sorter_.isFilled()) {
			return true;
		}
		else if (!reader_.exists()) {
			return false;
		}

		assert(!!T::NullableType::VALUE == !nullChecker_.getBase().isEmpty());
		if (T::NullableType::VALUE && nullChecker_(
				SQLValues::TupleListReaderSource(reader_))) {
			if (keyFiltering_) {
				continue;
			}
			sorter_.addSecondary(SummaryTuple::createBy<T, TupleDigester>(
					tupleSet2_, reader_, digester2_));
			continue;
		}

		sorter_.add(SummaryTuple::createBy<T, TypedDigester>(
				tupleSet1_, reader_, TypedDigester(digester_)));
	}
}

bool SQLSortOps::SortNway::SorterBuilder::checkEmpty() {
	if (keyFiltering_ && sorter_.isEmptyPredicateAt(true)) {
		for (; reader_.exists(); reader_.next()) {
		}
		return true;
	}

	return false;
}


SQLSortOps::SortNway::SorterWriter::SorterWriter(
		OpContext &cxt, TupleSorter &sorter, const Projection &proj,
		const SQLValues::CompColumnList &keyColumnList, bool primary) :
		cxt_(cxt),
		sorter_(sorter),
		proj_(resolveOutputProjection(proj)),
		writer_(SQLOpUtils::ExpressionListWriter::Source(
				cxt, proj_, true, &keyColumnList, primary, NULL,
				SQLExprs::ExprCode::INPUT_MULTI_STAGE)),
		primary_(primary) {
}

void SQLSortOps::SortNway::SorterWriter::write() {
	if (writer_.isDigestColumnAssigned()) {
		TupleColumnType type = writer_.getDigestColumnType();
		if (primary_) {
			type = SQLValues::TypeUtils::toNonNullable(type);
		}
		if (writer_.isDigestColumnAscending()) {
			getTypedFunction<false>(type)(*this);
		}
		else {
			getTypedFunction<true>(type)(*this);
		}
	}
	else if (writer_.isAvailable()) {
		writeBy(SQLOpUtils::ExpressionListWriter::ByGeneral(writer_));
	}
	else {
		writeBy(proj_);
	}
}

template<typename P>
void SQLSortOps::SortNway::SorterWriter::writeBy(const P &projector) {
	typedef typename TupleSorter::Iterator Iterator;
	const Iterator begin = sorter_.beginAt(primary_);
	const Iterator end = sorter_.endAt(primary_);
	for (Iterator it = begin; it != end; ++it) {
		projector.projectBy(cxt_, *it);
	}
}

template<bool Rev>
SQLSortOps::SortNway::SorterWriter::TypedFunc
SQLSortOps::SortNway::SorterWriter::getTypedFunction(TupleColumnType type) {
	typedef SQLValues::ValueComparator::VariantTraits<
			void, Rev, void, void> VariantType;
	typedef SQLValues::TypeSwitcher::OpTraitsOptions<
			VariantType, 1, false, false, 1, true, false> OptionsType;
	typedef SQLValues::TypeSwitcher::OpTraits<
			void, OptionsType> TraitsType;

	return SQLValues::TypeSwitcher(type).getWith<SorterWriter, TraitsType>();
}


SQLSortOps::SortNway::GroupSortAction::GroupSortAction(
		OpContext &cxt,
		const Projection *pipeProj, const Projection *mergeProj,
		SummaryTuple *outTupleRef, const TupleSetPair &tupleSetPair) :
		cxt_(cxt),
		pipeProj_(pipeProj),
		mergeProj_(mergeProj),
		inTupleRef_(cxt.getExprContext().getSummaryTupleRef(0)),
		outTupleRef_(outTupleRef),
		tupleSetPair_(tupleSetPair) {
}

void SQLSortOps::SortNway::GroupSortAction::operator()(bool primary) const {
	SummaryTupleSet *tupleSet =
			(primary ? tupleSetPair_.first : tupleSetPair_.second);
	cxt_.getExprContext().setAggregationTupleRef(tupleSet, outTupleRef_);
}


SQLSortOps::SortStageElement::SortStageElement() :
		storeIndex1_(std::numeric_limits<uint32_t>::max()),
		storeIndex2_(std::numeric_limits<uint32_t>::max()) {
}

void SQLSortOps::SortStageElement::clear(bool primary) {
	set(primary, std::numeric_limits<uint32_t>::max());
}

bool SQLSortOps::SortStageElement::isAssigned(bool primary) const {
	return (get(primary) != std::numeric_limits<uint32_t>::max());
}

void SQLSortOps::SortStageElement::assign(bool primary, uint32_t storeIndex) {
	assert(!isAssigned(primary));
	set(primary, storeIndex);
	assert(isAssigned(primary));
}

uint32_t SQLSortOps::SortStageElement::resolve(bool primary) const {
	assert(isAssigned(primary));
	return get(primary);
}

void SQLSortOps::SortStageElement::set(bool primary, uint32_t storeIndex) {
	(primary ? storeIndex1_ : storeIndex2_) = storeIndex;
}

uint32_t SQLSortOps::SortStageElement::get(bool primary) const {
	return (primary ? storeIndex1_ : storeIndex2_);
}


SQLSortOps::SortContext::SortContext(SQLValues::ValueContext &cxt) :
		baseCxt_(NULL),
		workingDepth_(std::numeric_limits<uint32_t>::max()),
		sorter_(NULL),
		sorterFilled_(false),
		stageList_(cxt.getAllocator()),
		freeStoreList_(cxt.getAllocator()),
		workingRestLimit_(-1),
		middleColumnList_(cxt.getAllocator()),
		summaryColumnList1_(cxt.getAllocator()),
		summaryColumnList2_(cxt.getAllocator()),
		totalTupleSet_(cxt, NULL),
		tupleSet1_(cxt, &totalTupleSet_),
		tupleSet2_(cxt, &totalTupleSet_),
		keyColumnList1_(cxt.getAllocator()),
		keyColumnList2_(cxt.getAllocator()) {
}

SQLOps::OpContext& SQLSortOps::SortContext::getBase() {
	assert(baseCxt_ != NULL);
	return *baseCxt_;
}

util::StackAllocator& SQLSortOps::SortContext::getAllocator() {
	return baseCxt_->getAllocator();
}

SQLOps::SummaryColumnList& SQLSortOps::SortContext::getSummaryColumnList(
		bool pimary) {
	return (pimary ? summaryColumnList1_ : summaryColumnList2_);
}

SQLSortOps::SummaryTupleSet& SQLSortOps::SortContext::getSummaryTupleSet(
		bool pimary) {
	return (pimary ? tupleSet1_ : tupleSet2_);
}

SQLValues::CompColumnList& SQLSortOps::SortContext::getKeyColumnList(
		bool pimary) {
	return (pimary ? keyColumnList1_ : keyColumnList2_);
}

bool& SQLSortOps::SortContext::getFinalStageStarted(bool pimary) {
	return (pimary ? finalStageStarted_.first : finalStageStarted_.second);
}


void SQLSortOps::Window::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	OpCode srcCode = getCode();

	const SQLValues::CompColumnList *keyList = srcCode.findKeyColumnList();
	SQLOpUtils::ProjectionPair projections =
			builder.arrangeProjections(srcCode, false, true, NULL);

	SQLExprs::Expression *valueExpr;
	SQLExprs::Expression *posExpr;
	bool valueCounting;
	SQLExprs::Expression *windowExpr = splitWindowExpr(
			builder, *projections.first, valueExpr, posExpr, valueCounting);

	const bool positioning = (posExpr != NULL);

	if (valueCounting || positioning) {
		const OpNode &inNode = plan.getParentInput(0);

		OpNode &partNode = plan.createNode(SQLOpTypes::OP_WINDOW_PARTITION);
		{
			OpNode &node = partNode;
			node.setCode(createPartitionCode(
					builder, keyList, valueExpr, posExpr, *projections.first,
					windowExpr));
			node.addInput(inNode);
		}

		OpNode *posNode = NULL;
		if (posExpr != NULL &&
				posExpr->getCode().getType() != SQLType::EXPR_CONSTANT) {
			OpNode &posJoinNode = plan.createNode(SQLOpTypes::OP_JOIN);
			{
				OpNode &node = posJoinNode;
				node.setCode(createPositioningJoinCode(builder));
				node.addInput(inNode);
				node.addInput(inNode);
			}

			posNode = &plan.createNode(SQLOpTypes::OP_SORT);
			{
				OpNode &node = *posNode;
				node.setCode(createPositioningSortCode(builder));
				node.addInput(posJoinNode);
			}
		}

		OpNode &mergeNode = plan.createNode(SQLOpTypes::OP_WINDOW_MERGE);
		{
			OpNode &node = mergeNode;
			node.setCode(createMergeCode(
					builder, projections, windowExpr, positioning));
			node.addInput(inNode);
			node.addInput(partNode);
			if (posNode != NULL) {
				node.addInput(*posNode);
			}
		}

		plan.linkToAllParentOutput(mergeNode);
	}
	else {
		OpNode &node = plan.createNode(SQLOpTypes::OP_WINDOW_PARTITION);
		{
			node.setCode(createSingleCode(
					builder, keyList, projections, windowExpr));
			node.addInput(plan.getParentInput(0));
		}

		plan.linkToAllParentOutput(node);
	}
}

SQLOps::OpCode SQLSortOps::Window::createPartitionCode(
		OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
		SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
		const Projection &pipeProj, const SQLExprs::Expression *windowExpr) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	const uint32_t input = 0;

	OpCode code;
	code.setKeyColumnList(keyList);

	SQLExprs::Expression *dupValueExpr = NULL;
	if (posExpr != NULL) {
		assert(valueExpr != NULL);
		dupValueExpr = &builder.getExprRewriter().rewrite(
				factoryCxt, *valueExpr, NULL);
	}

	Projection *countProj;
	{
		Projection &baseProj =
				builder.createProjection(SQLOpTypes::PROJ_OUTPUT);
		baseProj.getPlanningCode().setAttributes(
				SQLExprs::ExprCode::ATTR_ASSIGNED);

		SQLExprs::Expression::ModIterator it(baseProj);
		it.append(
				SQLExprs::ExprRewriter::createTypedIdExpr(factoryCxt, input));
		if (valueExpr != NULL) {
			it.append(
					SQLExprs::ExprRewriter::createTypedIdExpr(factoryCxt, input));
			valueExpr = NULL;
		}

		SQLExprs::Expression *dupWindowExpr =
				&builder.getExprRewriter().rewrite(
						factoryCxt, *windowExpr, NULL);
		countProj = &createProjectionWithWindow(
				builder, pipeProj, baseProj, dupWindowExpr);
	}

	Projection *posProj = NULL;
	if (posExpr != NULL) {
		posProj = &builder.createProjection(SQLOpTypes::PROJ_OUTPUT);
		posProj->getPlanningCode().setAttributes(
				SQLExprs::ExprCode::ATTR_ASSIGNED);
		posProj->getPlanningCode().setOutput(1);

		SQLExprs::Expression::ModIterator it(*posProj);

		it.append(
				SQLExprs::ExprRewriter::createTypedIdExpr(factoryCxt, input));

		it.append(*posExpr);
		posExpr = NULL;

		assert(dupValueExpr != NULL);
		it.append(*dupValueExpr);
	}

	const Projection &totalProj = (posProj == NULL ?
			*countProj :
			builder.createMultiOutputProjection(*countProj, *posProj));
	code.setPipeProjection(&totalProj);

	return code;
}

SQLOps::OpCode SQLSortOps::Window::createPositioningJoinCode(
		OpCodeBuilder &builder) {
	OpCode code;

	code.setJoinType(SQLType::JOIN_LEFT_OUTER);

	{
		SQLValues::CompColumnList &keyList = builder.createCompColumnList();

		SQLValues::CompColumn key;
		key.setColumnPos(1, true);
		key.setColumnPos(0, false);
		keyList.push_back(key);

		code.setKeyColumnList(&keyList);
	}

	{
		Projection &proj = builder.createProjection(SQLOpTypes::PROJ_OUTPUT);
		SQLExprs::ExprFactoryContext &factoryCxt =
				builder.getExprFactoryContext();

		SQLExprs::Expression::ModIterator it(proj);
		it.append(SQLExprs::ExprRewriter::createColumnExpr(factoryCxt, 0, 0));
		it.append(SQLExprs::ExprRewriter::createColumnExpr(factoryCxt, 1, 1));
		it.append(SQLExprs::ExprRewriter::createColumnExpr(factoryCxt, 1, 2));

		code.setPipeProjection(&proj);
	}

	return code;
}

SQLOps::OpCode SQLSortOps::Window::createPositioningSortCode(
		OpCodeBuilder &builder) {
	OpCode code;

	{
		SQLValues::CompColumnList &keyList = builder.createCompColumnList();

		SQLValues::CompColumn key;
		key.setColumnPos(0, true);
		keyList.push_back(key);

		code.setKeyColumnList(&keyList);
	}

	{
		Projection &proj = builder.createProjection(SQLOpTypes::PROJ_OUTPUT);
		SQLExprs::ExprFactoryContext &factoryCxt =
				builder.getExprFactoryContext();

		SQLExprs::Expression::ModIterator it(proj);
		it.append(SQLExprs::ExprRewriter::createColumnExpr(factoryCxt, 0, 1));
		it.append(SQLExprs::ExprRewriter::createColumnExpr(factoryCxt, 0, 2));

		code.setPipeProjection(&proj);
	}

	return code;
}

SQLOps::OpCode SQLSortOps::Window::createMergeCode(
		OpCodeBuilder &builder, SQLOpUtils::ProjectionPair &projections,
		SQLExprs::Expression *&windowExpr, bool positioning) {
	Projection *pipeProj;
	if (positioning) {
		pipeProj = &builder.createMultiStageProjection(
				*projections.first,
				createUnmatchWindowPipeProjection(builder, *projections.first));
	}
	else {
		pipeProj = projections.first;
	}

	assert(projections.second != NULL);
	Projection &finishProj = createProjectionWithWindow(
			builder, *projections.first, *projections.second, windowExpr);

	OpCode code;
	code.setPipeProjection(
			&builder.createPipeFinishProjection(*pipeProj, finishProj));
	return code;
}

SQLOps::OpCode SQLSortOps::Window::createSingleCode(
		OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
		SQLOpUtils::ProjectionPair &projections,
		SQLExprs::Expression *&windowExpr) {
	OpCode code;
	code.setKeyColumnList(keyList);

	const Projection &pipeProj = *projections.first;

	Projection &pipeFinProj = builder.createPipeFinishProjection(
			*projections.first, *projections.second);
	projections = SQLOpUtils::ProjectionPair();

	code.setPipeProjection(&createProjectionWithWindow(
			builder, pipeProj, pipeFinProj, windowExpr));

	return code;
}

SQLExprs::Expression* SQLSortOps::Window::splitWindowExpr(
		OpCodeBuilder &builder, Projection &src,
		SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
		bool &valueCounting) {
	valueExpr = NULL;
	posExpr = NULL;
	valueCounting = false;

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	const SQLExprs::ExprSpec *spec = NULL;
	SQLExprs::Expression *windowExprRef = NULL;

	for (SQLExprs::Expression::ModIterator it(src); it.exists(); it.next()) {
		SQLExprs::Expression &subExpr = it.get();
		const SQLExprs::ExprSpec &subSpec =
				factoryCxt.getFactory().getSpec(subExpr.getCode().getType());
		if (SQLExprs::ExprRewriter::isWindowExpr(subSpec, true)) {
			if (windowExprRef != NULL) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}
			windowExprRef = &subExpr;
			spec = &subSpec;
		}
	}

	SQLExprs::Expression *windowExpr = NULL;
	if (windowExprRef != NULL) {
		windowExpr = &builder.getExprRewriter().rewrite(
				factoryCxt, *windowExprRef, NULL);

		size_t index = 0;
		for (SQLExprs::Expression::ModIterator it(*windowExprRef);
				it.exists(); it.next()) {
			const uint32_t &flags = spec->inList_[index].flags_;
			if ((flags & (
					SQLExprs::ExprSpec::FLAG_WINDOW_POS_BEFORE |
					SQLExprs::ExprSpec::FLAG_WINDOW_POS_AFTER)) != 0) {
				posExpr = &builder.getExprRewriter().rewrite(
						factoryCxt, it.get(), NULL);
				if (posExpr->getCode().getType() != SQLType::EXPR_CONSTANT) {
					it.remove();
					it.insert(SQLExprs::ExprRewriter::createColumnExpr(
							factoryCxt, 1, 0));
				}
				break;
			}
		}

		valueCounting = ((spec->flags_ &
				SQLExprs::ExprSpec::FLAG_WINDOW_VALUE_COUNTING) != 0);

		if (posExpr != NULL || valueCounting) {
			for (SQLExprs::Expression::ModIterator it(*windowExprRef);
					it.exists();) {
				valueExpr = &builder.getExprRewriter().rewrite(
						factoryCxt, it.get(), NULL);
				if (posExpr != NULL &&
						posExpr->getCode().getType() != SQLType::EXPR_CONSTANT) {
					it.remove();
					it.insert(SQLExprs::ExprRewriter::createColumnExpr(
							factoryCxt, 1, 1));
				}
				break;
			}
		}

		if (posExpr == NULL && valueExpr == NULL) {
			windowExpr = NULL;
		}
	}

	return windowExpr;
}

SQLOps::Projection& SQLSortOps::Window::createProjectionWithWindow(
		OpCodeBuilder &builder, const Projection &pipeProj, Projection &src,
		SQLExprs::Expression *&windowExpr) {
	Projection &windowProj = builder.createProjection(
			SQLOpTypes::PROJ_AGGR_PIPE, pipeProj.getProjectionCode());

	windowProj.getPlanningCode().setAttributes(
			SQLExprs::ExprCode::ATTR_ASSIGNED |
			SQLExprs::ExprCode::ATTR_AGGR_ARRANGED);

	if (windowExpr != NULL) {
		SQLExprs::Expression::ModIterator it(windowProj);
		it.append(*windowExpr);
		windowExpr = NULL;
	}

	SQLOps::Projection &dest =
			builder.createMultiStageProjection(windowProj, src);

	return dest;
}

SQLOps::Projection& SQLSortOps::Window::createUnmatchWindowPipeProjection(
		OpCodeBuilder &builder, const Projection &src) {
	SQLOps::Projection &dest = builder.rewriteProjection(src);
	assert(dest.getProjectionCode().getType() == SQLOpTypes::PROJ_AGGR_PIPE);

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	const SQLExprs::ExprSpec *spec = NULL;
	SQLExprs::Expression *windowExpr = NULL;

	for (SQLExprs::Expression::ModIterator it(dest); it.exists(); it.next()) {
		SQLExprs::Expression &subExpr = it.get();
		const SQLExprs::ExprSpec &subSpec =
				factoryCxt.getFactory().getSpec(subExpr.getCode().getType());
		if (SQLExprs::ExprRewriter::isWindowExpr(subSpec, true)) {
			if (windowExpr != NULL) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}
			windowExpr = &subExpr;
			spec = &subSpec;
		}
	}

	if (windowExpr != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	size_t index = 0;
	for (SQLExprs::Expression::ModIterator it(*windowExpr);
			it.exists(); it.next(), index++) {
		const uint32_t &flags = spec->inList_[index].flags_;
		if ((flags & (
				SQLExprs::ExprSpec::FLAG_WINDOW_POS_BEFORE |
				SQLExprs::ExprSpec::FLAG_WINDOW_POS_AFTER)) != 0) {
			const int64_t unmatchValue = -1;
			it.remove();
			it.insert(SQLExprs::ExprRewriter::createConstExpr(
					factoryCxt, TupleValue(unmatchValue),
					TupleTypes::TYPE_LONG));
			break;
		}
	}

	return dest;
}


void SQLSortOps::WindowPartition::execute(OpContext &cxt) const {
	const Projection *pipeProj;
	const Projection *subFinishProj;
	const bool projecting = getProjections(pipeProj, subFinishProj);

	PartitionContext &partCxt = preparePartitionContext(cxt, projecting);

	TupleListReader &reader = cxt.getReader(0);
	TupleListReader *keyReader = prepareKeyReader(cxt, partCxt);

	TupleListWriter *countWriter = prepareCountingWriter(cxt, projecting);
	TupleListWriter *posWriter = preparePositioningWriter(cxt, projecting);

	SQLExprs::ExprContext &exprCxt = cxt.getExprContext();
	for (; reader.exists(); reader.next()) {
		if (cxt.checkSuspended()) {
			return;
		}

		if (keyReader != NULL && !keyReader->exists() &&
				!cxt.isAllInputCompleted()) {
			return;
		}

		if (projecting) {
			pipeProj->project(cxt);
			subFinishProj->project(cxt);
		}

		if (partCxt.posExpr_ != NULL) {
			const TupleValue &posValue = partCxt.posExpr_->eval(exprCxt);
			const TupleValue &windowValue = partCxt.valueExpr_->eval(exprCxt);
			assert(posWriter != NULL);
			writePositioningTuple(partCxt, *posWriter, posValue, windowValue);
		}

		if (partCxt.valueExpr_ != NULL) {
			const TupleValue &value = partCxt.valueExpr_->eval(exprCxt);
			countUpValueColumn(partCxt, value);
		}

		partCxt.tupleOffset_++;

		if (keyReader != NULL && keyReader->exists()) {
			assert(partCxt.keyReaderStarted_);

			if (!(*partCxt.keyEq_)(
					SQLValues::TupleListReaderSource(reader),
					SQLValues::TupleListReaderSource(*keyReader))) {
				if (countWriter != NULL) {
					writeCountingTuple(partCxt, *countWriter);
				}
				if (projecting) {
					cxt.getExprContext().initializeAggregationValues();
				}
			}
			keyReader->next();
		}
	}

	if (countWriter != NULL && cxt.isAllInputCompleted()) {
		writeCountingTuple(partCxt, *countWriter);
	}
}

bool SQLSortOps::WindowPartition::toAbsolutePosition(
		int64_t curPos, int64_t amount, bool following, int64_t &pos) {
	pos = -1;
	if (following) {
		if (amount > 0 &&
				curPos > std::numeric_limits<int64_t>::max() - amount) {
			return false;
		}
		pos = curPos + amount;
	}
	else {
		if (amount < 0 &&
				curPos > std::numeric_limits<int64_t>::max() + amount) {
			return false;
		}
		pos = curPos - amount;
	}
	return true;
}

bool SQLSortOps::WindowPartition::getProjections(
		const Projection *&pipeProj, const Projection *&subFinishProj) const {
	const Projection *totalProj = getCode().getPipeProjection();
	if (totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_OUTPUT) {
		totalProj = &totalProj->chainAt(0);
	}

	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
	const Projection *pipeFinishProj = &totalProj->chainAt(1);

	if (pipeFinishProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_PIPE_FINISH) {
		pipeProj = &pipeFinishProj->chainAt(0);
		subFinishProj = &pipeFinishProj->chainAt(1);

		assert(SQLOps::OpCodeBuilder::findOutputProjection(
				*subFinishProj, NULL) != NULL);
		return true;
	}
	else {
		pipeProj = NULL;
		subFinishProj = NULL;
		return false;
	}
}

void SQLSortOps::WindowPartition::findWindowExpr(
		OpContext &cxt, const SQLExprs::Expression *&valueExpr,
		const SQLExprs::Expression *&posExpr, bool &following) const {
	valueExpr = NULL;
	posExpr = NULL;
	following = false;

	const Projection *totalProj = getCode().getPipeProjection();
	if (totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_OUTPUT) {
		totalProj = &totalProj->chainAt(0);
	}

	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
	const Projection *windowProj = &totalProj->chainAt(0);
	assert(windowProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_AGGR_PIPE);

	const SQLExprs::Expression *expr = windowProj->findChild();
	if (expr == NULL) {
		return;
	}
	const SQLExprs::ExprType type = expr->getCode().getType();

	SQLOps::OpCodeBuilder builder(SQLOps::OpCodeBuilder::ofContext(cxt));
	const SQLExprs::ExprSpec &spec =
			builder.getExprFactoryContext().getFactory().getSpec(type);
	assert(SQLExprs::ExprRewriter::isWindowExpr(spec, true));

	if ((spec.flags_ & SQLExprs::ExprSpec::FLAG_WINDOW_VALUE_COUNTING) != 0) {
		valueExpr = &expr->child();
	}

	size_t index = 0;
	for (SQLExprs::Expression::Iterator it(*expr); it.exists(); it.next(), index++) {
		const SQLExprs::Expression *posExprBase = &it.get();
		if (posExprBase->getCode().getType() == SQLType::EXPR_CONSTANT) {
			continue;
		}

		const uint32_t &flags = spec.inList_[index].flags_;
		if ((flags & SQLExprs::ExprSpec::FLAG_WINDOW_POS_BEFORE) != 0) {
			posExpr = posExprBase;
			break;
		}
		else if ((flags & SQLExprs::ExprSpec::FLAG_WINDOW_POS_AFTER) != 0) {
			following = true;
			posExpr = posExprBase;
			break;
		}
	}
}

SQLSortOps::WindowPartition::PartitionContext&
SQLSortOps::WindowPartition::preparePartitionContext(
		OpContext &cxt, bool projecting) const {
	if (!cxt.getResource(0).isEmpty()) {
		return cxt.getResource(0).resolveAs<PartitionContext>();
	}

	cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), PartitionContext);
	PartitionContext &partCxt = cxt.getResource(0).resolveAs<PartitionContext>();

	const SQLValues::CompColumnList *keyList = getCode().findKeyColumnList();
	if (keyList != NULL && !keyList->empty()) {
		partCxt.keyEqBase_ = UTIL_MAKE_LOCAL_UNIQUE(
				partCxt.keyEqBase_, TupleEq, cxt.getAllocator(),
				SQLValues::TupleComparator(*keyList, false));
		partCxt.keyEq_ = partCxt.keyEqBase_.get();
	}

	bool following;
	findWindowExpr(cxt, partCxt.valueExpr_, partCxt.posExpr_, following);

	do {
		const uint32_t index = getCountingOutput(projecting);
		if (index >= cxt.getOutputCount()) {
			break;
		}

		partCxt.tupleCountColumn_ = cxt.getWriterColumn(index, 0);
		if (partCxt.valueExpr_ != NULL) {
			partCxt.valueCountColumn_ = cxt.getWriterColumn(index, 1);
		}
	}
	while (false);

	do {
		const uint32_t index = getPositioningOutput(projecting);
		if (index >= cxt.getOutputCount()) {
			break;
		}

		assert(partCxt.posExpr_ != NULL);
		partCxt.relativePositionDirection_ = (following ? 1 : 0);

		partCxt.orgPositionColumn_ = cxt.getWriterColumn(index, 0);
		partCxt.refPositionColumn_ = cxt.getWriterColumn(index, 1);
		partCxt.windowValueColumn_ = cxt.getWriterColumn(index, 2);
	}
	while (false);

	return partCxt;
}

SQLOps::TupleListReader* SQLSortOps::WindowPartition::prepareKeyReader(
		OpContext &cxt, PartitionContext &partCxt) const {
	if (partCxt.keyEq_ == NULL) {
		return NULL;
	}

	SQLOps::TupleListReader &reader = cxt.getReader(0, 1);
	if (!partCxt.keyReaderStarted_ && reader.exists()) {
		reader.next();
		partCxt.keyReaderStarted_ = true;
	}

	return &reader;
}

SQLOps::TupleListWriter* SQLSortOps::WindowPartition::prepareCountingWriter(
		OpContext &cxt, bool projecting) const {
	const uint32_t index = getCountingOutput(projecting);
	if (index >= cxt.getOutputCount()) {
		return NULL;
	}
	return &cxt.getWriter(index);
}

SQLOps::TupleListWriter* SQLSortOps::WindowPartition::preparePositioningWriter(
		OpContext &cxt, bool projecting) const {
	const uint32_t index = getPositioningOutput(projecting);
	if (index >= cxt.getOutputCount()) {
		return NULL;
	}
	return &cxt.getWriter(index);
}

void SQLSortOps::WindowPartition::writeCountingTuple(
		PartitionContext &partCxt, TupleListWriter &writer) const {
	writer.next();
	WritableTuple tuple = writer.get();

	const int64_t tupleCount = partCxt.tupleOffset_ - partCxt.partitionOffset_;
	partCxt.partitionOffset_ = partCxt.tupleOffset_;

	SQLValues::ValueUtils::writeValue<CountTypeTag>(
			tuple, partCxt.tupleCountColumn_, tupleCount);

	if (partCxt.valueExpr_ != NULL) {
		const int64_t valueCount = partCxt.lastPartitionValueCount_;
		partCxt.lastPartitionValueCount_ = 0;

		SQLValues::ValueUtils::writeValue<CountTypeTag>(
				tuple, partCxt.valueCountColumn_, valueCount);
	}
}

void SQLSortOps::WindowPartition::writePositioningTuple(
		PartitionContext &partCxt, TupleListWriter &writer,
		const TupleValue &posValue, const TupleValue &windowValue) const {
	writer.next();
	WritableTuple tuple = writer.get();

	const int64_t curPos = partCxt.tupleOffset_;

	int64_t arrangedPos = -1;
	if (!SQLValues::ValueUtils::isNull(posValue)) {
		const bool following = (partCxt.relativePositionDirection_ > 0);
		int64_t pos;
		if (toAbsolutePosition(
				curPos, SQLValues::ValueUtils::getValue<int64_t>(posValue),
				following, pos)) {
			arrangedPos = pos;
		}
	}

	SQLValues::ValueUtils::writeValue<CountTypeTag>(
			tuple, partCxt.orgPositionColumn_, curPos);
	SQLValues::ValueUtils::writeValue<CountTypeTag>(
			tuple, partCxt.refPositionColumn_, arrangedPos);
	SQLValues::ValueUtils::writeAny(
			tuple, partCxt.windowValueColumn_, windowValue);
}

void SQLSortOps::WindowPartition::countUpValueColumn(
		PartitionContext &partCxt, const TupleValue &value) const {
	if (!SQLValues::ValueUtils::isNull(value)) {
		partCxt.lastPartitionValueCount_++;
	}
}

uint32_t SQLSortOps::WindowPartition::getCountingOutput(bool projecting) {
	return (projecting ? 1 : 0);
}

uint32_t SQLSortOps::WindowPartition::getPositioningOutput(bool projecting) {
	return (projecting ? 2 : 1);
}


SQLSortOps::WindowPartition::PartitionContext::PartitionContext() :
		keyEq_(NULL),
		posExpr_(NULL),
		valueExpr_(NULL),
		keyReaderStarted_(false),
		partitionOffset_(0),
		tupleOffset_(0),
		lastPartitionValueCount_(0),
		relativePositionDirection_(-1) {
}


void SQLSortOps::WindowMerge::execute(OpContext &cxt) const {
	const Projection *matchedPipeProj;
	const Projection *unmatchedPipeProj;
	const Projection *subFinishProj;
	getProjections(matchedPipeProj, unmatchedPipeProj, subFinishProj);

	MergeContext &mergeCxt = prepareMergeContext(cxt);
	cxt.getExprContext().setWindowState(&mergeCxt.windowState_);

	TupleListReader &reader = cxt.getReader(IN_MAIN);
	TupleListReader &countReader = cxt.getReader(IN_COUNTING);
	TupleListReader *posReader = preparePositioningReader(cxt, mergeCxt);

	for (; reader.exists(); reader.next()) {
		if (cxt.checkSuspended()) {
			return;
		}

		if (--mergeCxt.partitionRemaining_ <= 0) {
			cxt.getExprContext().initializeAggregationValues();
			readNextPartitionCounts(mergeCxt, countReader);
		}

		const Projection *pipeProj = matchedPipeProj;
		if (unmatchedPipeProj != NULL &&
				!locatePosition(mergeCxt, *posReader)) {
			pipeProj = unmatchedPipeProj;
		}

		pipeProj->project(cxt);
		subFinishProj->project(cxt);
	}
}

void SQLSortOps::WindowMerge::getProjections(
		const Projection *&matchedPipeProj,
		const Projection *&unmatchedPipeProj,
		const Projection *&subFinishProj) const {
	const Projection *totalProj = getCode().getPipeProjection();
	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_PIPE_FINISH);

	const Projection *pipeProj = &totalProj->chainAt(0);
	const Projection *finishProj = &totalProj->chainAt(1);

	if (pipeProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE) {
		matchedPipeProj = &pipeProj->chainAt(0);
		unmatchedPipeProj = &pipeProj->chainAt(1);
	}
	else {
		matchedPipeProj = pipeProj;
		unmatchedPipeProj = NULL;
	}

	assert(finishProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
	subFinishProj = &finishProj->chainAt(1);

	assert(SQLOps::OpCodeBuilder::findOutputProjection(
			*subFinishProj, NULL) != NULL);
}

const SQLExprs::Expression*
SQLSortOps::WindowMerge::findPositioningExpr(
		OpContext &cxt, bool &following) const {
	following = false;

	const Projection *totalProj = getCode().getPipeProjection();
	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_PIPE_FINISH);

	const Projection *finishProj = &totalProj->chainAt(1);
	assert(finishProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);

	const Projection *refProj = &finishProj->chainAt(0);
	assert(refProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_AGGR_PIPE);

	const SQLExprs::Expression &expr = refProj->child();
	const SQLExprs::ExprType type = expr.getCode().getType();

	SQLOps::OpCodeBuilder builder(SQLOps::OpCodeBuilder::ofContext(cxt));
	const SQLExprs::ExprSpec &spec =
			builder.getExprFactoryContext().getFactory().getSpec(type);
	assert(SQLExprs::ExprRewriter::isWindowExpr(spec, true));

	size_t index = 0;
	for (SQLExprs::Expression::Iterator it(expr); it.exists(); it.next(), index++) {
		const SQLExprs::Expression &posExpr = it.get();
		const uint32_t &flags = spec.inList_[index].flags_;
		if ((flags & SQLExprs::ExprSpec::FLAG_WINDOW_POS_BEFORE) != 0) {
			following = false;
			return &posExpr;
		}
		else if ((flags & SQLExprs::ExprSpec::FLAG_WINDOW_POS_AFTER) != 0) {
			following = true;
			return &posExpr;
		}
	}

	return NULL;
}

SQLSortOps::WindowMerge::MergeContext&
SQLSortOps::WindowMerge::prepareMergeContext(OpContext &cxt) const {
	if (!cxt.getResource(0).isEmpty()) {
		return cxt.getResource(0).resolveAs<MergeContext>();
	}

	cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), MergeContext);
	MergeContext &mergeCxt = cxt.getResource(0).resolveAs<MergeContext>();

	bool following;
	const SQLExprs::Expression *expr = findPositioningExpr(cxt, following);
	if (expr == NULL) {
		mergeCxt.positioningReaderProgress_ = -1;
		mergeCxt.relativePositionDirection_ = -1;
	}
	else {
		mergeCxt.positioningReaderProgress_ = 0;

		if (expr->getCode().getType() == SQLType::EXPR_CONSTANT) {
			mergeCxt.relativePositionDirection_ = (following ? 1 : 0);
			mergeCxt.relativePositionAmount_ =
					SQLValues::ValueUtils::getValue<int64_t>(
							expr->getCode().getValue());
		}
		else {
			mergeCxt.relativePositionDirection_ = -1;
		}

		const SQLExprs::ExprCode &code = expr->getCode();
		assert(code.getInput() == IN_MAIN);

		mergeCxt.positionColumn_ =
				cxt.getReaderColumn(code.getInput(), code.getColumnPos());
	}

	SQLExprs::WindowState &windowState = mergeCxt.windowState_;
	windowState.partitionTupleCount_ = 0;
	mergeCxt.tupleCountColumn_ = cxt.getReaderColumn(IN_COUNTING, 0);

	if (cxt.getColumnCount(IN_COUNTING) > 1) {
		windowState.partitionValueCount_ = 0;
		mergeCxt.valueCountColumn_ = cxt.getReaderColumn(IN_COUNTING, 1);
	}
	else {
		windowState.partitionValueCount_ = -1;
	}

	return mergeCxt;
}

SQLOps::TupleListReader* SQLSortOps::WindowMerge::preparePositioningReader(
		OpContext &cxt, const MergeContext &mergeCxt) const {
	if (mergeCxt.positioningReaderProgress_ < 0) {
		return NULL;
	}

	if (mergeCxt.relativePositionDirection_ < 0) {
		return &cxt.getReader(IN_POSITIONING);
	}
	else {
		return &cxt.getReader(IN_MAIN, 1);
	}
}

void SQLSortOps::WindowMerge::readNextPartitionCounts(
		MergeContext &mergeCxt, TupleListReader &reader) const {
	if (!reader.exists()) {
		assert(false);
		return;
	}

	SQLExprs::WindowState &windowState = mergeCxt.windowState_;
	int64_t &tupleCount = windowState.partitionTupleCount_;
	int64_t &valueCount = windowState.partitionValueCount_;

	mergeCxt.partitionOffset_ += tupleCount;

	tupleCount = SQLValues::ValueUtils::readCurrentValue<CountTypeTag>(
			reader, mergeCxt.tupleCountColumn_);

	if (valueCount >= 0) {
		valueCount = SQLValues::ValueUtils::readCurrentValue<CountTypeTag>(
				reader, mergeCxt.valueCountColumn_);
		assert(valueCount >= 0);
	}

	assert(tupleCount > 0);
	mergeCxt.partitionRemaining_ = tupleCount;

	reader.next();
}

bool SQLSortOps::WindowMerge::locatePosition(
		MergeContext &mergeCxt, TupleListReader &reader) const {
	int64_t &progress = mergeCxt.positioningReaderProgress_;
	assert(progress >= 0);

	const int32_t direction = mergeCxt.relativePositionDirection_;
	if (direction < 0) {
		if (getCurrentPosition(mergeCxt) > 0) {
			if (reader.exists()) {
				reader.next();
			}
		}

		if (!reader.exists()) {
			assert(false);
			return false;
		}

		if (SQLValues::ValueUtils::readCurrentNull(
				reader, mergeCxt.positionColumn_)) {
			return false;
		}

		const int64_t pos =
				SQLValues::ValueUtils::readCurrentValue<CountTypeTag>(
						reader, mergeCxt.positionColumn_);
		return isPositionInRange(mergeCxt, pos);
	}
	else {
		int64_t pos;
		if (!getDesiredPosition(mergeCxt, pos)) {
			return false;
		}

		for (int64_t i = (pos - progress); i > 0; i--) {
			if (!reader.exists()) {
				assert(false);
				return false;
			}
			reader.next();
			progress++;
		}

		assert(pos == progress);
		assert(reader.exists());
		return true;
	}
}

bool SQLSortOps::WindowMerge::getDesiredPosition(
		const MergeContext &mergeCxt, int64_t &pos) {
	pos = -1;

	const int64_t &amount = mergeCxt.relativePositionAmount_;
	const int64_t curPos = getCurrentPosition(mergeCxt);

	const bool following = (mergeCxt.relativePositionDirection_ > 0);
	int64_t desiredPos;
	if (!WindowPartition::toAbsolutePosition(
			curPos, amount, following, desiredPos)) {
		return false;
	}

	if (!isPositionInRange(mergeCxt, desiredPos)) {
		return false;
	}

	pos = desiredPos;
	return true;
}

bool SQLSortOps::WindowMerge::isPositionInRange(
		const MergeContext &mergeCxt, int64_t pos) {

	const int64_t offset = mergeCxt.partitionOffset_;
	const int64_t count = mergeCxt.windowState_.partitionTupleCount_;
	return (pos >= offset && pos - offset < count);
}

int64_t SQLSortOps::WindowMerge::getCurrentPosition(
		const MergeContext &mergeCxt) {
	const int64_t curPos = mergeCxt.partitionOffset_ +
			mergeCxt.windowState_.partitionTupleCount_ -
			mergeCxt.partitionRemaining_;
	assert(curPos >= 0);
	return curPos;
}


SQLSortOps::WindowMerge::MergeContext::MergeContext() :
		partitionRemaining_(0),
		partitionOffset_(0),
		relativePositionDirection_(-1),
		relativePositionAmount_(0),
		positioningReaderProgress_(-1) {
}
