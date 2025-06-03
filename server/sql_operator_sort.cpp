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
	add<SQLOpTypes::OP_WINDOW_RANK_PARTITION, WindowRankPartition>();
	add<SQLOpTypes::OP_WINDOW_FRAME_PARTITION, WindowFramePartition>();
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

	code.setKeyColumnList(&SQLExprs::ExprRewriter::reduceDuplicateCompColumns(
			cxt.getAllocator(), code.getKeyColumnList()));
	code.setMiddleKeyColumnList(
			&SQLExprs::ExprRewriter::reduceDuplicateCompColumns(
					cxt.getAllocator(), code.getMiddleKeyColumnList()));

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
		if (singleKeyUnique) {
			code.setMiddleKeyColumnList(&toSubLimitedKeyList(
					builder, code.getMiddleKeyColumnList()));
			code.setKeyColumnList(&toSubLimitedKeyList(
					builder, code.getKeyColumnList()));
		}
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

const SQLValues::CompColumnList& SQLSortOps::Sort::toSubLimitedKeyList(
		SQLOps::OpCodeBuilder &builder, const SQLValues::CompColumnList &src) {
	if (src.empty() || !src.front().isOrdering()) {
		return src;
	}

	SQLValues::CompColumnList &dest = builder.createCompColumnList();
	for (SQLValues::CompColumnList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		SQLValues::CompColumn column = *it;
		column.setOrdering(false);
		dest.push_back(column);
	}

	return dest;
}


void SQLSortOps::SortNway::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_MULTI_STAGE);
	cxt.setReaderRandomAccess(0);

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
	const uint32_t inIndex = 0;
	if (!checkSorterBuilding(cxt, inIndex)) {
		return false;
	}

	const SQLValues::CompColumnList &keyColumnList =
			getCode().getMiddleKeyColumnList();

	TupleListReader &reader = cxt.getBase().getReader(inIndex);
	TupleSorter &sorter = prepareSorter(cxt, keyColumnList, reader);
	if (!cxt.sorterEntry_->filled_) {
		if (!isKeyEmpty(sorter)) {
			cxt.getBase().keepReaderLatch(0);
			if (reader.exists()) {
				SQLValues::ValueUtils::updateKeepInfo(reader);
			}
		}

		ProjectionRefPair projectionPair;
		if (isGrouping(false, false)) {
			prepareSortProjection(cxt, true);
			projectionPair = findProjection(false, false);
		}

		SorterBuilder builder(
				cxt, sorter, reader, keyColumnList, isKeyFiltering(),
				projectionPair.first, projectionPair.second);
		while (!builder.build(cxt)) {
			cxt.getBase().checkSuspendedAlways(false);
			cxt.getBase().resetNextCountForSuspend();
			if (checkMemoryLimitReached(cxt, reader)) {
				break;
			}
		}
		cxt.sorterEntry_->filled_ = true;
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

		const ProjectionRefPair &projectionPair =
				prepareSortProjection(cxt, primary);

		const bool unique = (isUnique() || isGrouping(false, false));
		SorterWriter writer(
				cxt, sorter, *projectionPair.first, keyColumnList,
				primary, unique);
		writer.write();
	}

	cxt.getBase().releaseReaderLatch(0);
	cxt.sorterEntry_.reset();
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
			finishMerging(cxt, primary);
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
			if (isUnique() || isOrderedUnique() || cxt.workingRestLimit_ >= 0) {
				continuable = sortMergeDetail(cxt, queue, writer);
			}
			else {
				MergeAction<true, false, false> action(cxt, writer);
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

		finishMerging(cxt, primary);
	}

	assert(!continuable == !isMergeCompleted(cxt));
	return continuable;
}

bool SQLSortOps::SortNway::nextStage(SortContext &cxt) const {
	const bool topCompleted =
			(cxt.readerAccessible_ &&
			cxt.getBase().isAllInputCompleted() &&
			!cxt.getBase().getReader(0).exists() &&
			cxt.sorterEntry_.get() == NULL);

	if (isSorting(cxt) && cxt.sorterEntry_.get() != NULL &&
			!cxt.sorterEntry_->sorter_->isEmpty()) {
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
	cxt.workingDepth_ = std::numeric_limits<uint32_t>::max();

	cxt.workingRestLimit_ = -1;
	return true;
}

SQLSortOps::TupleSorter& SQLSortOps::SortNway::prepareSorter(
		SortContext &cxt, const SQLValues::CompColumnList &keyColumnList,
		TupleListReader &reader) const {
	do {
		if (isSorting(cxt)) {
			break;
		}
		cxt.workingDepth_ = 0;
		cxt.stageList_[0].push_back(SortStageElement());

		if (cxt.sorterEntry_.get() != NULL) {
			break;
		}

		util::StackAllocator &alloc = cxt.getAllocator();
		SQLValues::VarContext &varCxt = cxt.getBase().getVarContext();
		SQLValues::ValueProfile *profile = cxt.getBase().getValueProfile();

		const bool unique = (isUnique() || isGrouping(false, false));
		const int64_t hashSizeBase = cxt.initialTupleCout_;
		cxt.sorterEntry_ = UTIL_MAKE_LOCAL_UNIQUE(
				cxt.sorterEntry_, SorterEntry,
				cxt.getBase(), getSorterCapacity(alloc),
				isHashAvailable(keyColumnList, unique),
				hashSizeBase, reader);

		const Projection *aggrProj = OpCodeBuilder::findAggregationProjection(
				*getCode().getMiddleProjection());

		cxt.nullChecker_ = UTIL_MAKE_LOCAL_UNIQUE(
				cxt.nullChecker_, TupleNullChecker,
				createSorterNullChecker(
						alloc, keyColumnList, cxt.nullCheckerKeys_));

		for (size_t i = 0; i < 2; i++) {
			const bool primary = (i == 0);

			util::LocalUniquePtr<TupleDigester> &digester =
					cxt.getDigester(primary);
			digester = UTIL_MAKE_LOCAL_UNIQUE(
					digester, TupleDigester,
					createSorterDigester(
							alloc, varCxt, keyColumnList, *cxt.nullChecker_,
							unique, primary, profile));

			SummaryTupleSet &tupleSet =
					cxt.sorterEntry_->getSummaryTupleSet(primary);

			tupleSet.addReaderColumnList(cxt.getBase().getInputColumnList(0));
			tupleSet.addKeyList(
					keyColumnList, true, digester->getBase().isRotating());
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

		const std::pair<TupleLess, TupleLess> predPair(
				TupleLess(alloc, SQLValues::TupleComparator(
						cxt.keyColumnList1_, &varCxt, true, true, true), profile),
				TupleLess(alloc, SQLValues::TupleComparator(
						cxt.keyColumnList2_, &varCxt, true, true, false), profile));

		TupleSorter &sorter = cxt.sorterEntry_->resolveSorter(predPair);
		sorter.setPredicateEmpty(true, isPredicateEmpty(
				cxt, predPair.first.getBase(), keyColumnList, true));
		sorter.setPredicateEmpty(false, isPredicateEmpty(
				cxt, predPair.first.getBase(), keyColumnList, false));

		SQLAlgorithmUtils::SortConfig config;

		assert(getCode().getOffset() <= 0);
		config.limit_ = getCode().getLimit();
		config.orderedUnique_ = isOrderedUnique();
		config.unique_ = (unique || config.orderedUnique_);

		sorter.setConfig(config);
	}
	while (false);

	assert(isSorting(cxt));
	assert(cxt.sorterEntry_.get() != NULL);
	return *cxt.sorterEntry_->sorter_;
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
			SQLValues::TupleComparator(
					keyColumnList, &cxt.getBase().getVarContext(),
					true, true, nullIgnorable),
			cxt.getBase().getValueProfile());

	TupleHeapQueue queue(pred, alloc);
	queue.setPredicateEmpty(
			isPredicateEmpty(cxt, pred.getBase(), keyColumnList, primary));

	const bool rotating = (isUnique() || isGrouping(false, false));
	TupleDigester digester(
			alloc,
			SQLValues::TupleDigester(
					keyColumnList, &cxt.getBase().getVarContext(),
					NULL, rotating, nullIgnorable, false),
			cxt.getBase().getValueProfile());

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

SQLSortOps::TupleNullChecker
SQLSortOps::SortNway::createSorterNullChecker(
		util::StackAllocator &alloc,
		const SQLValues::CompColumnList &totalKeys,
		SQLValues::CompColumnList &nullCheckerKeys) {
	nullCheckerKeys = totalKeys;

	if (SQLValues::TupleDigester::isOrderingAvailable(
			nullCheckerKeys, false) && nullCheckerKeys.size() > 1) {
		nullCheckerKeys.erase(
				nullCheckerKeys.begin() + 1, nullCheckerKeys.end());
	}

	return TupleNullChecker(
			alloc, SQLValues::TupleNullChecker(nullCheckerKeys));
}

SQLSortOps::TupleDigester SQLSortOps::SortNway::createSorterDigester(
		util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
		const SQLValues::CompColumnList &keyColumnList,
		const TupleNullChecker &nullChecker, bool unique, bool primary,
		SQLValues::ValueProfile *profile) {
	const bool rotationAllowed = unique;
	const bool nullableAlways = (primary && !nullChecker.getBase().isEmpty());
	TupleDigester digester(
			alloc,
			SQLValues::TupleDigester(
					keyColumnList, &varCxt,
					NULL, rotationAllowed, primary, nullableAlways),
			profile);
	return digester;
}

bool SQLSortOps::SortNway::isHashAvailable(
		const SQLValues::CompColumnList &keyColumnList, bool unique) {
	if (!unique) {
		return false;
	}

	const bool rotationAllowed = unique;
	const SQLValues::TupleDigester digester(
			keyColumnList, NULL, NULL, rotationAllowed, false, false);
	return (!digester.isOrdering() || digester.isRotating());
}

bool SQLSortOps::SortNway::isPredicateEmpty(
		SortContext &cxt, const SQLValues::TupleComparator &basePred,
		const SQLValues::CompColumnList &keyColumnList, bool primary) {
	size_t startIndex = 0;
	if (!primary && (cxt.getDigester(primary)->getBase().isOrdering() ||
			keyColumnList.size() <= 1)) {
		startIndex = 1;
	}
	return basePred.isEmpty(startIndex);
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

	SummaryTupleSet::applyColumnList(
			cxt.getSummaryColumnList(primary),
			*cxt.getBase().getSummaryColumnListRef(0));

	const ProjectionRefPair &projectionPair = findProjection(false, true);
	updateProjections(cxt, projectionPair);

	cxt.getBase().setUpExprContext();

	SQLExprs::ExprContext &exprCxt = cxt.getBase().getExprContext();

	exprCxt.setReader(0, NULL, &cxt.getBase().getInputColumnList(0));
	exprCxt.setInputSourceType(0, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE);

	exprCxt.setAggregationTupleRef(
			&cxt.sorterEntry_->getSummaryTupleSet(primary),
			exprCxt.getSummaryTupleRef(0));

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

bool SQLSortOps::SortNway::isOrderedUnique() const {
	const SQLOps::OpConfig *config = getCode().getConfig();
	return (config != NULL &&
			config->get(SQLOpTypes::CONF_SORT_ORDERED_UNIQUE) == 1);
}

bool SQLSortOps::SortNway::isKeyFiltering() const {
	const SQLOps::Projection *proj = getCode().getMiddleProjection();
	return (proj->getProjectionCode().getType() == SQLOpTypes::PROJ_KEY_FILTER);
}

bool SQLSortOps::SortNway::isKeyEmpty(TupleSorter &sorter) const {
	return isKeyEmpty(sorter, isKeyFiltering());
}

bool SQLSortOps::SortNway::isKeyEmpty(
		TupleSorter &sorter, bool keyFiltering) {
	return (keyFiltering && sorter.isEmptyPredicateAt(true));
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

void SQLSortOps::SortNway::finishMerging(SortContext &cxt, bool primary) const {
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
		SortContext &cxt, uint32_t inIndex) const {
	do {
		if (cxt.sorterEntry_.get() != NULL || cxt.getBase().isAllInputCompleted()) {
			break;
		}

		TupleListReader *reader = NULL;
		if (cxt.readerAccessible_) {
			reader = &cxt.getBase().getReader(inIndex);
			if (!reader->exists() && !cxt.getBase().isAllInputCompleted()) {
				return false;
			}
		}

		const int64_t memLimit = OpConfig::resolve(
				SQLOpTypes::CONF_WORK_MEMORY_LIMIT, getCode().getConfig());

		const uint64_t readableBlockCount = (reader == NULL ?
				cxt.getBase().getInputBlockCount(inIndex) :
				reader->getFollowingBlockCount());
		const uint64_t readableSize =
				cxt.getBase().getInputBlockSize(inIndex) * readableBlockCount;

		if (memLimit < 0 || readableSize >= static_cast<uint64_t>(memLimit)) {
			break;
		}

		return false;
	}
	while (false);

	if (!cxt.readerAccessible_) {
		const bool unique = (isUnique() || isGrouping(false, false));
		if (unique && cxt.getBase().isAllInputCompleted()) {
			cxt.initialTupleCout_ = static_cast<int64_t>(
					cxt.getBase().getInputTupleCount(inIndex));
		}
		cxt.readerAccessible_ = true;
	}

	return true;
}

bool SQLSortOps::SortNway::checkMemoryLimitReached(
		SortContext &cxt, TupleListReader &reader) const {
	const int64_t memLimit = OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, getCode().getConfig());

	const uint64_t prevRefBlocks =
			cxt.sorterEntry_->initialReferencingBlockCount_;
	const uint64_t curRefBlocks = reader.getReferencingBlockCount();
	assert(curRefBlocks >= prevRefBlocks);

	const uint64_t latchSize =
			reader.getTupleList().getInfo().blockSize_ *
			(curRefBlocks - prevRefBlocks) * 2;

	const uint64_t workUsage = cxt.sorterEntry_->getAllocatedSize();

	if (memLimit >= 0 &&
			(latchSize > static_cast<uint64_t>(memLimit) ||
			workUsage > static_cast<uint64_t>(memLimit))) {
		return true;
	}

	return false;
}

size_t SQLSortOps::SortNway::getSorterCapacity(
		util::StackAllocator &alloc) const {
	static_cast<void>(alloc);

	const OpConfig *config = getCode().getConfig();

	do {
		const int64_t confValue = OpConfig::resolve(
				SQLOpTypes::CONF_SORT_PARTIAL_UNIT, config);
		if (confValue < 0) {
			break;
		}

		const uint64_t maxValue =
				static_cast<uint64_t>(std::numeric_limits<size_t>::max());
		return static_cast<size_t>(std::max<uint64_t>(std::min(
				static_cast<uint64_t>(confValue), maxValue), 1));
	}
	while (false);

	const int64_t memLimit = OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, config);

	const size_t capacity = static_cast<size_t>(
			std::min(
					static_cast<uint64_t>(memLimit) - std::min<uint64_t>(
							static_cast<uint64_t>(memLimit),
							sizeof(SummaryTuple) * 3),
					static_cast<uint64_t>(
							std::numeric_limits<uint32_t>::max())) /
			2 / sizeof(SummaryTuple) + 1);

	return capacity;
}

size_t SQLSortOps::SortNway::getMergeWidth() const {
	const OpConfig *config = getCode().getConfig();

	const int64_t defaultValue = 32;
	const int64_t confValue = OpConfig::resolve(
			SQLOpTypes::CONF_SORT_MERGE_UNIT, config);

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
		MergeAction<false, true, false> action(cxt, *projectionPair.first);
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
		MergeAction<false, false, false> action(cxt, *projectionPair.first);
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
			if (isOrderedUnique()) {
				return queue.mergeOrderedUnique(action);
			}
			else if (unique) {
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
	assert(isUnique() || isOrderedUnique() || cxt.workingRestLimit_ >= 0);

	if (isOrderedUnique()) {
		MergeAction<true, false, false> action(cxt, writer);
		return queue.mergeOrderedUnique(action);
	}
	else if (isUnique()) {
		MergeAction<true, false, true> action(cxt, writer);
		return queue.mergeUnique(action);
	}
	else {
		MergeAction<true, true, false> action(cxt, writer);
		return queue.mergeLimited(action);
	}
}

bool SQLSortOps::SortNway::sortOptional(
		SortContext &cxt, TupleSorter &sorter) const {
	if (isGrouping(false, false)) {
		prepareSortProjection(cxt, true);

		const ProjectionRefPair &projectionPair = findProjection(false, false);
		const GroupSortAction::ColumListPair columnListPair(
				&cxt.getSummaryColumnList(true),
				&cxt.getSummaryColumnList(false));
		const GroupSortAction::TupleSetPair tupleSetPair(
				&cxt.sorterEntry_->getSummaryTupleSet(true),
				&cxt.sorterEntry_->getSummaryTupleSet(false));

		const GroupSortAction action(
				cxt.getBase(), projectionPair.first, projectionPair.second,
				&cxt.groupTuple_, columnListPair, tupleSetPair);
		return sorter.sortGroup(action);
	}
	else {
		return sorter.sortOptional(util::FalseType());
	}
}


SQLSortOps::SortNway::SorterBuilder::SorterBuilder(
		SortContext &cxt, TupleSorter &sorter, TupleListReader &reader,
		const SQLValues::CompColumnList &keyColumnList, bool keyFiltering,
		const Projection *pipeProj, const Projection *mergeProj) :
		cxt_(cxt.getBase()),
		sorter_(sorter),
		reader_(reader),
		nullChecker_(*cxt.nullChecker_),
		digester1_(*cxt.getDigester(true)),
		digester2_(*cxt.getDigester(false)),
		keyFiltering_(keyFiltering),
		withAny_(false),
		tupleSet1_(cxt.sorterEntry_->getSummaryTupleSet(true)),
		tupleSet2_(cxt.sorterEntry_->getSummaryTupleSet(false)),
		nextInterruption_(0),
		tupleEq_(cxt.getAllocator(), SQLValues::TupleComparator(
				cxt.keyColumnList1_, &cxt.getBase().getVarContext(),
				false, true, true)),
		hashSize_(cxt.sorterEntry_->hashSize_),
		hashUnit_(SorterEntry::toHashUnit(hashSize_, digester1_)),
		hashTable_(((hashSize_ <= 0) ?
				NULL : cxt.sorterEntry_->getBuffer(true, true))),
		hashConflictionLimit_(cxt.sorterEntry_->getHashConflictionLimit()),
		inTupleRef_((hashTable_ == NULL || pipeProj == NULL ?
				NULL : cxt_.getExprContext().getSummaryTupleRef(0))),
		outTupleRef_(&cxt.groupTuple_),
		pipeProj_(pipeProj),
		mergeProj_(mergeProj),
		normalFunc_((hashTable_ == NULL ?
				digester1_.getTypeSwitcher().get<const Normal>() : NULL)),
		uniqueFunc_((hashTable_ != NULL && pipeProj == NULL ?
				digester1_.getTypeSwitcher().get<const Unique>() : NULL)),
		groupFunc_((hashTable_ != NULL && pipeProj != NULL ?
				digester1_.getTypeSwitcher().get<const Grouping>() : NULL)) {
	if (hashTable_ != NULL && pipeProj != NULL) {
		SummaryTupleSet::applyColumnList(
				cxt.getSummaryColumnList(true),
				*cxt_.getSummaryColumnListRef(0));
		cxt_.getExprContext().setAggregationTupleRef(&tupleSet1_, outTupleRef_);
	}
	for (SQLValues::CompColumnList::const_iterator it = keyColumnList.begin();
			it != keyColumnList.end(); ++it) {
		if (SQLValues::TypeUtils::isAny(it->getType())) {
			withAny_ = true;
			break;
		}
	}
}

SQLSortOps::SortNway::SorterBuilder::~SorterBuilder() {
	if (pipeProj_ != NULL) {
		cxt_.getExprContext().setAggregationTupleRef(NULL, NULL);
	}
}

bool SQLSortOps::SortNway::SorterBuilder::build(SortContext &cxt) {
	if (checkEmpty()) {
		return true;
	}

	const uint64_t baseNextInterruption =
			std::max<uint64_t>(cxt.getBase().getNextCountForSuspend(), 2);
	if (normalFunc_ != NULL) {
		nextInterruption_ = std::min<uint64_t>(
				sorter_.getRemaining() + 1, baseNextInterruption);
		normalFunc_(Normal(*this));
	}
	else {
		nextInterruption_ = baseNextInterruption;
		if (uniqueFunc_ != NULL) {
			uniqueFunc_(Unique(*this));
		}
		else {
			assert(groupFunc_ != NULL);
			groupFunc_(Grouping(*this));
		}

		if (nextInterruption_ > 0 &&
				sorter_.beginAt(true) + hashConflictionLimit_ >=
				sorter_.endAt(true)) {
			return true;
		}
	}

	if (sorter_.isFilled() || !reader_.exists()) {
		return true;
	}

	return false;
}

template<typename T>
void SQLSortOps::SortNway::SorterBuilder::buildAt() {
	assert(nextInterruption_ > 0);

	typedef typename TupleDigester::template TypeAt<
			typename T::NonNullableTraitsType>::TypedOp TypedDigester;

	for (;; reader_.next()) {
		if (--nextInterruption_ <= 0) {
			break;
		}
		else if (!reader_.exists()) {
			break;
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
				tupleSet1_, reader_, TypedDigester(digester1_)));
	}
}

template<typename T>
void SQLSortOps::SortNway::SorterBuilder::uniqueBuildAt() {
	assert(nextInterruption_ > 0);
	assert(!keyFiltering_);

	typedef typename TupleDigester::template TypeAt<
			typename T::NonNullableTraitsType>::TypedOp TypedDigester;

	typedef typename T::VariantTraitsType DigesterVariantTraitsType;
	const bool digestOrdering = DigesterVariantTraitsType::VARIANT_ORDERED;

	typedef typename SQLValues::TupleComparator::template VariantTraits<
			(digestOrdering ? 0 : -1),
			std::equal_to<SQLValues::ValueComparator::PredArgType>, false,
			SQLValues::ValueAccessor::BySummaryTuple,
			SQLValues::ValueAccessor::BySummaryTuple> EqVariantTraitsType;
	typedef typename T::NonNullableTraitsType::template VariantRebind<
			EqVariantTraitsType>::Type EqTraitsType;
	typedef typename SQLValues::TupleComparator::template BaseTypeAt<
			EqTraitsType> TypedEq;

	TupleSorter::Iterator primaryTail = std::min(
			sorter_.beginAt(true) + hashConflictionLimit_,
			sorter_.beginAt(false));

	for (;; reader_.next()) {
		if (--nextInterruption_ <= 0) {
			break;
		}
		else if (!reader_.exists()) {
			break;
		}

		assert(!!T::NullableType::VALUE == !nullChecker_.getBase().isEmpty());
		if ((T::NullableType::VALUE && nullChecker_(
				SQLValues::TupleListReaderSource(reader_))) ||
				(!digestOrdering && withAny_)) {
			if (sorter_.beginAt(false) <= primaryTail) {
				if (sorter_.baseEndAt(true) >= primaryTail) {
					break;
				}
				--primaryTail;
			}
			sorter_.addSecondary(SummaryTuple::createBy<T, TupleDigester>(
					tupleSet2_, reader_, digester2_));
			continue;
		}

		SummaryTuple inTuple = SummaryTuple::createBy<T, TypedDigester>(
				tupleSet1_, reader_, TypedDigester(digester1_));
		const size_t hashIndex = static_cast<size_t>(
				static_cast<uint64_t>(inTuple.getDigest()) / hashUnit_);
		assert(hashIndex < hashSize_);
		SummaryTuple &outTuple = hashTable_[hashIndex];

		if (!SummaryTuple::checkEmpty(outTuple)) {
			if (!TypedEq(tupleEq_)(inTuple, outTuple)) {
				if (sorter_.baseEndAt(true) >= primaryTail) {
					break;
				}
				sorter_.add(inTuple);
			}
			continue;
		}

		outTuple = inTuple;
	}
}

template<typename T>
void SQLSortOps::SortNway::SorterBuilder::groupBuildAt() {
	assert(nextInterruption_ > 0);
	assert(!keyFiltering_);

	typedef typename TupleDigester::template TypeAt<
			typename T::NonNullableTraitsType>::TypedOp TypedDigester;

	typedef typename T::VariantTraitsType DigesterVariantTraitsType;
	const bool digestOrdering = DigesterVariantTraitsType::VARIANT_ORDERED;

	typedef typename SQLValues::TupleComparator::template VariantTraits<
			(digestOrdering ? 0 : -1),
			std::equal_to<SQLValues::ValueComparator::PredArgType>, false,
			SQLValues::ValueAccessor::BySummaryTuple,
			SQLValues::ValueAccessor::BySummaryTuple> EqVariantTraitsType;
	typedef typename T::NonNullableTraitsType::template VariantRebind<
			EqVariantTraitsType>::Type EqTraitsType;
	typedef typename SQLValues::TupleComparator::template BaseTypeAt<
			EqTraitsType> TypedEq;

	TupleSorter::Iterator primaryTail = std::min(
			sorter_.beginAt(true) + hashConflictionLimit_,
			sorter_.beginAt(false));

	for (;; reader_.next()) {
		if (--nextInterruption_ <= 0) {
			break;
		}
		else if (!reader_.exists()) {
			break;
		}

		assert(!!T::NullableType::VALUE == !nullChecker_.getBase().isEmpty());
		if ((T::NullableType::VALUE && nullChecker_(
				SQLValues::TupleListReaderSource(reader_))) ||
				(!digestOrdering && withAny_)) {
			if (sorter_.beginAt(false) <= primaryTail) {
				if (sorter_.baseEndAt(true) >= primaryTail) {
					break;
				}
				--primaryTail;
			}
			sorter_.addSecondary(SummaryTuple::createBy<T, TupleDigester>(
					tupleSet2_, reader_, digester2_));
			continue;
		}

		SummaryTuple inTuple = SummaryTuple::createBy<T, TypedDigester>(
				tupleSet1_, reader_, TypedDigester(digester1_));
		const size_t hashIndex = static_cast<size_t>(
				static_cast<uint64_t>(inTuple.getDigest()) / hashUnit_);
		assert(hashIndex < hashSize_);
		SummaryTuple &outTuple = hashTable_[hashIndex];

		if (!SummaryTuple::checkEmpty(outTuple)) {
			if (!TypedEq(tupleEq_)(inTuple, outTuple)) {
				if (sorter_.baseEndAt(true) >= primaryTail) {
					break;
				}
				sorter_.add(inTuple);
				continue;
			}

			*inTupleRef_ = inTuple;
			*outTupleRef_ = outTuple;
			pipeProj_->project(cxt_);
			continue;
		}

		outTuple = inTuple;
		*inTupleRef_ = inTuple;
		*outTupleRef_ = inTuple;
		pipeProj_->project(cxt_);
	}
}

bool SQLSortOps::SortNway::SorterBuilder::checkEmpty() {
	if (isKeyEmpty(sorter_, keyFiltering_)) {
		for (; reader_.exists(); reader_.next()) {
		}
		return true;
	}

	return false;
}


SQLSortOps::SortNway::SorterWriter::SorterWriter(
		SortContext &cxt, TupleSorter &sorter, const Projection &proj,
		const SQLValues::CompColumnList &keyColumnList, bool primary,
		bool unique) :
		cxt_(cxt.getBase()),
		sorter_(sorter),
		proj_(resolveOutputProjection(proj)),
		writer_(SQLOpUtils::ExpressionListWriter::Source(
				cxt.getBase(), proj_, true, &keyColumnList, primary, NULL,
				SQLExprs::ExprCode::INPUT_MULTI_STAGE)),
		primary_(primary),
		unique_(unique),
		digester_(*cxt.getDigester(true)),
		pred_(cxt.getAllocator(), SQLValues::TupleComparator(
				cxt.keyColumnList1_, &cxt.getBase().getVarContext(),
				false, false, true)),
		hashTable_((primary && cxt.sorterEntry_->hashSize_ > 0) ?
				cxt.sorterEntry_->getBuffer(true, true) : NULL),
		hashBufferRef_((hashTable_ == NULL ?
				NULL : &cxt.sorterEntry_->getBufferRef(true, true))),
		hashSize_((hashTable_ == NULL ? 0 : cxt.sorterEntry_->hashSize_)) {
}

void SQLSortOps::SortNway::SorterWriter::write() {
	if (unique_) {
		pred_.getTypeSwitcher().get<const Unique>()(Unique(*this));
		if (hashBufferRef_ != NULL) {
			hashBufferRef_->setFilled();
		}
	}
	else if (writer_.isDigestColumnAssigned()) {
		TupleColumnType type = writer_.getDigestColumnType();
		if (primary_) {
			type = SQLValues::TypeUtils::toNonNullable(type);
		}
		if (writer_.isDigestColumnAscending()) {
			getTypedFunction<false>(type)(Normal(*this));
		}
		else {
			getTypedFunction<true>(type)(Normal(*this));
		}
	}
	else if (writer_.isAvailable()) {
		writeBy(SQLOpUtils::ExpressionListWriter::ByGeneral(writer_));
	}
	else {
		writeBy(proj_);
	}
}

template<typename T>
void SQLSortOps::SortNway::SorterWriter::writeUnique() {
	if (writer_.isDigestColumnAssigned()) {
		if (SQLValues::TypeUtils::toNonNullable(
				writer_.getDigestColumnType()) == TupleTypes::TYPE_LONG) {
			typedef SQLValues::TypeSwitcher::TypeTraits<
					SQLValues::Types::Long, void, void,
					typename T::VariantTraitsType> TraitsType;
			writeUniqueByDigestTuple<TraitsType>();
		}
		else {
			writeUniqueByDigestTuple<T>();
		}
	}
	else if (writer_.isAvailable()) {
		typedef SQLOpUtils::ExpressionListWriter::ByGeneral Projector;
		writeUniqueBy<Projector, T>(Projector(writer_));
	}
	else {
		writeUniqueBy<Projection, T>(proj_);
	}
}

template<typename T>
void SQLSortOps::SortNway::SorterWriter::writeUniqueByDigestTuple() {
	if (digester_.getBase().isRotating()) {
		typedef typename SQLOpUtils::ExpressionListWriter::ByDigestTuple<
				true>::template TypeAt<T> Projector;
		writeUniqueBy<Projector, T>(Projector(writer_));
	}
	else {
		typedef typename SQLOpUtils::ExpressionListWriter::ByDigestTuple<
				false>::template TypeAt<T> Projector;
		writeUniqueBy<Projector, T>(Projector(writer_));
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

template<typename P, typename T>
void SQLSortOps::SortNway::SorterWriter::writeUniqueBy(const P &projector) {
	typedef typename util::BoolType<
			T::VariantTraitsType::DIGEST_ONLY>::Result DigestOnly;
	typedef typename TupleSorter::Iterator Iterator;

	const Iterator sorterBegin = sorter_.beginAt(primary_);
	const Iterator sorterEnd = sorter_.endAt(primary_);
	SummaryTuple *const hashBegin = hashTable_;
	SummaryTuple *const hashEnd = hashBegin + hashSize_;

	const bool ordering = digester_.getBase().isOrdering();
	SummaryTuple *hashMid = getHashMiddle(ordering);

	Iterator sorterIt = sorterBegin;
	for (size_t i = 0; i < 2; i++) {
		SummaryTuple *hashSubBegin = (i == 0 ? hashMid : hashBegin);
		SummaryTuple *hashSubEnd = (i == 0 ? hashEnd : hashMid);

		for (SummaryTuple *it = hashSubBegin; it != hashSubEnd; ++it) {
			if (SummaryTuple::checkEmpty(*it)) {
				continue;
			}

			assert((i == 0) == (it->getDigest() < 0));

			while (sorterIt != sorterEnd && pred_(*sorterIt, *it, DigestOnly())) {
				projector.projectBy(cxt_, *sorterIt);
				++sorterIt;
			}

			projector.projectBy(cxt_, *it);
			SummaryTuple::fillEmpty(it);
		}
	}

	for (; sorterIt != sorterEnd; ++sorterIt) {
		projector.projectBy(cxt_, *sorterIt);
	}
}

template<bool Rev>
SQLSortOps::SortNway::SorterWriter::TypedFunc
SQLSortOps::SortNway::SorterWriter::getTypedFunction(TupleColumnType type) {
	typedef SQLValues::ValueComparator::VariantTraits<
			void, Rev, void, void> VariantType;
	typedef SQLValues::TypeSwitcher::OpTraitsOptions<
			VariantType, 1, false, false, 1, true> OptionsType;
	typedef SQLValues::TypeSwitcher::OpTraits<
			void, OptionsType> TraitsType;

	return SQLValues::TypeSwitcher(type).getWith<const Normal, TraitsType>();
}

SQLValues::SummaryTuple*
SQLSortOps::SortNway::SorterWriter::getHashMiddle(bool ordering) const {
	SummaryTuple *const hashBegin = hashTable_;
	SummaryTuple *const hashEnd = hashBegin + hashSize_;

	if (!ordering) {
		return hashEnd;
	}

	SummaryTuple *midBase = hashBegin + hashSize_ / 2;
	std::pair<SummaryTuple*, SummaryTuple*> midRange(midBase, midBase);
	if (midRange.first != hashBegin) {
		--midRange.first;
	}
	if (midRange.second != hashEnd) {
		++midRange.second;
	}

	{
		SummaryTuple *it = midRange.first;
		for (; it != midRange.second; ++it) {
			if (!SummaryTuple::checkEmpty(*it) && it->getDigest() < 0) {
				break;
			}
		}
		return it;
	}
}


SQLSortOps::SortNway::GroupSortAction::GroupSortAction(
		OpContext &cxt,
		const Projection *pipeProj, const Projection *mergeProj,
		SummaryTuple *outTupleRef, const ColumListPair &columnListPair,
		const TupleSetPair &tupleSetPair) :
		cxt_(cxt),
		pipeProj_(pipeProj),
		mergeProj_(mergeProj),
		inTupleRef_(cxt.getExprContext().getSummaryTupleRef(0)),
		outTupleRef_(outTupleRef),
		columListPair_(columnListPair),
		tupleSetPair_(tupleSetPair) {
}

SQLSortOps::SortNway::GroupSortAction::~GroupSortAction() {
	cxt_.getExprContext().setAggregationTupleRef(NULL, NULL);
}

void SQLSortOps::SortNway::GroupSortAction::operator()(bool primary) const {
	const SQLOps::SummaryColumnList *columnList =
			(primary ? columListPair_.first : columListPair_.second);
	SummaryTupleSet *tupleSet =
			(primary ? tupleSetPair_.first : tupleSetPair_.second);

	SummaryTupleSet::applyColumnList(
			*columnList, *cxt_.getSummaryColumnListRef(0));
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


SQLSortOps::SorterEntry::SorterEntry(
		OpContext &cxt, size_t capacity, bool hashAvailable, int64_t hashSizeBase,
		TupleListReader &reader) :
		allocManager_(cxt.getAllocatorManager()),
		allocRef_(allocManager_, cxt.getAllocator().base()),
		valueCxt_(SQLValues::ValueContext::ofVarContext(
				cxt.getVarContext(), &allocRef_.get())),
		totalTupleSet_(valueCxt_, NULL),
		tupleSet1_(valueCxt_, &totalTupleSet_),
		tupleSet2_(valueCxt_, &totalTupleSet_),
		capacity_(capacity),
		hashSize_(toHashSize(capacity, hashAvailable, hashSizeBase)),
		buffer1_(allocManager_, toBufferBytes(capacity), false),
		buffer2_(allocManager_, toBufferBytes(capacity), false),
		initialReferencingBlockCount_(reader.getReferencingBlockCount()),
		filled_(false) {
	if (hashSize_ > 0) {
		initializeHashTable();
	}
}

SQLSortOps::SummaryTupleSet& SQLSortOps::SorterEntry::getSummaryTupleSet(
		bool primary) {
	return (primary ? tupleSet1_ : tupleSet2_);
}

SQLSortOps::TupleSorter& SQLSortOps::SorterEntry::resolveSorter(
		const std::pair<TupleLess, TupleLess> &predPair) {
	if (sorter_.get() == NULL) {
		const bool forHash = false;
		sorter_ = UTIL_MAKE_LOCAL_UNIQUE(
				sorter_, TupleSorter,
				capacity_, predPair.first, predPair.second,
				std::make_pair(
						getBuffer(forHash, true), getBuffer(forHash, false)));
	}
	return *sorter_;
}

void SQLSortOps::SorterEntry::initializeHashTable() {
	hashBuffer_ = UTIL_MAKE_LOCAL_UNIQUE(
			hashBuffer_, BufferRef,
			allocManager_, toBufferBytes(capacity_), toBufferBytes(hashSize_));
}

SQLSortOps::SorterEntry::SorterElementType* SQLSortOps::SorterEntry::getBuffer(
		bool forHash, bool primary) {
	return static_cast<SorterElementType*>(
			getBufferRef(forHash, primary).get().addr_);
}

SQLOps::OpAllocatorManager::BufferRef& SQLSortOps::SorterEntry::getBufferRef(
		bool forHash, bool primary) {
	assert(!forHash || primary);
	return (forHash ? *hashBuffer_ : (primary ? buffer1_ : buffer2_));
}

size_t SQLSortOps::SorterEntry::getAllocatedSize() {
	return allocRef_.get().getTotalSize() +
			totalTupleSet_.getPoolCapacity();
}

size_t SQLSortOps::SorterEntry::getHashConflictionLimit() const {
	return toHashConflictionLimit(capacity_, hashSize_);
}

size_t SQLSortOps::SorterEntry::toBufferBytes(size_t capacity) {
	return sizeof(SorterElementType) * capacity;
}

size_t SQLSortOps::SorterEntry::toHashSize(
		size_t capacity, bool hashAvailable, int64_t hashSizeBase) {
	if (!hashAvailable || capacity <= 2) {
		return 0;
	}

	uint64_t baseCapacity = capacity - 2;
	if (hashSizeBase >= 0 && static_cast<uint64_t>(hashSizeBase) <
			std::min<uint64_t>(
					baseCapacity, std::numeric_limits<uint32_t>::max()) / 2) {
		const uint32_t smallCapacity = std::max(
				1U << ((sizeof(uint32_t) * CHAR_BIT) -
						util::nlz(static_cast<uint32_t>(hashSizeBase - 1))),
				32U) * 2;
		if (smallCapacity < baseCapacity) {
			baseCapacity = smallCapacity;
		}
	}
	const uint64_t size =
			SQLValues::ValueUtils::findLargestPrimeNumber(baseCapacity);
	return static_cast<size_t>(size) + 1;
}

uint64_t SQLSortOps::SorterEntry::toHashUnit(
		size_t hashSize, const TupleDigester &digester) {
	if (hashSize < 2) {
		assert(hashSize == 0);
		return 0;
	}

	if (digester.getBase().isOrdering()) {
		const uint64_t maxDigest = std::numeric_limits<uint64_t>::max();
		return maxDigest / (hashSize - 1);
	}
	else {
		const uint64_t maxDigest = std::numeric_limits<uint32_t>::max();
		return maxDigest / (hashSize - 1) + 1;
	}
}

size_t SQLSortOps::SorterEntry::toHashConflictionLimit(
		size_t capacity, size_t hashSize) {
	if (hashSize <= 0) {
		return 0;
	}

	const size_t conflictionRate = 10;
	const size_t minCapacity = 1024;

	const uint64_t base =
			std::max<uint64_t>(hashSize * conflictionRate / 100, minCapacity);
	return static_cast<size_t>(std::min<uint64_t>(base, capacity));
}


SQLSortOps::SortContext::SortContext(SQLValues::ValueContext &cxt) :
		baseCxt_(NULL),
		workingDepth_(std::numeric_limits<uint32_t>::max()),
		readerAccessible_(false),
		initialTupleCout_(-1),
		stageList_(cxt.getAllocator()),
		freeStoreList_(cxt.getAllocator()),
		workingRestLimit_(-1),
		middleColumnList_(cxt.getAllocator()),
		summaryColumnList1_(cxt.getAllocator()),
		summaryColumnList2_(cxt.getAllocator()),
		keyColumnList1_(cxt.getAllocator()),
		keyColumnList2_(cxt.getAllocator()),
		nullCheckerKeys_(cxt.getAllocator()) {
}

SQLOps::OpContext& SQLSortOps::SortContext::getBase() {
	assert(baseCxt_ != NULL);
	return *baseCxt_;
}

util::StackAllocator& SQLSortOps::SortContext::getAllocator() {
	return baseCxt_->getAllocator();
}

SQLOps::SummaryColumnList& SQLSortOps::SortContext::getSummaryColumnList(
		bool primary) {
	return (primary ? summaryColumnList1_ : summaryColumnList2_);
}

SQLValues::CompColumnList& SQLSortOps::SortContext::getKeyColumnList(
		bool primary) {
	return (primary ? keyColumnList1_ : keyColumnList2_);
}

bool& SQLSortOps::SortContext::getFinalStageStarted(bool primary) {
	return (primary ? finalStageStarted_.first : finalStageStarted_.second);
}

util::LocalUniquePtr<SQLSortOps::TupleDigester>&
SQLSortOps::SortContext::getDigester(bool primary) {
	return (primary ? digester1_ : digester2_);
}


void SQLSortOps::Window::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	util::StackAllocator &alloc = cxt.getAllocator();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	OpCode srcCode = getCode();

	const SQLValues::CompColumnList *keyList = srcCode.findKeyColumnList();
	SQLOpUtils::ProjectionPair projections =
			builder.arrangeProjections(srcCode, false, true, NULL);

	const SQLExprs::Expression *windowOption = srcCode.getFilterPredicate();

	SQLExprs::Expression *valueExpr;
	SQLExprs::Expression *posExpr;
	bool valueCounting;
	ExprRefList windowExprList(alloc);
	splitWindowExpr(
			builder, *projections.first, valueExpr, posExpr, valueCounting,
			windowExprList);

	const bool positioning = (posExpr != NULL);
	const bool nonConstPositioning = (positioning &&
			posExpr->getCode().getType() != SQLType::EXPR_CONSTANT);

	if (valueCounting || positioning) {
		const OpNode &inNode = plan.getParentInput(0);
		TupleColumnType valueColumnType = TupleTypes::TYPE_NULL;
		if (valueExpr != NULL) {
			valueColumnType = valueExpr->getCode().getColumnType();
		}

		OpNode &partNode = plan.createNode(SQLOpTypes::OP_WINDOW_PARTITION);
		{
			OpNode &node = partNode;
			node.setCode(createPartitionCode(
					builder, keyList, valueExpr, posExpr, *projections.first,
					windowExprList));
			node.addInput(inNode);
		}

		OpNode *posNode = NULL;
		if (nonConstPositioning) {
			OpNode &posJoinNode = plan.createNode(SQLOpTypes::OP_JOIN);
			{
				OpNode &node = posJoinNode;
				node.setCode(
						createPositioningJoinCode(builder, valueColumnType));
				node.addInput(partNode, 1);
				node.addInput(partNode, 1);
			}

			posNode = &plan.createNode(SQLOpTypes::OP_SORT);
			{
				OpNode &node = *posNode;
				node.setCode(
						createPositioningSortCode(builder, valueColumnType));
				node.addInput(posJoinNode);
			}
		}

		OpNode &mergeNode = plan.createNode(SQLOpTypes::OP_WINDOW_MERGE);
		{
			OpNode &node = mergeNode;
			node.setCode(createMergeCode(
					builder, projections, windowExprList, positioning));
			node.addInput(inNode);
			node.addInput(partNode);
			if (nonConstPositioning) {
				node.addInput(*posNode);
			}
			else {
				node.addInput(inNode);
			}
		}

		plan.linkToAllParentOutput(mergeNode);
	}
	else {
		const SQLOpTypes::Type type = getSingleNodeType(
				builder, *projections.first, windowOption);

		OpNode &node = plan.createNode(type);
		{
			node.setCode(createSingleCode(
					builder, keyList, projections, windowExprList,
					windowOption));
			node.addInput(plan.getParentInput(0));
		}

		plan.linkToAllParentOutput(node);
	}
}

SQLOps::OpCode SQLSortOps::Window::createPartitionCode(
		OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
		SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
		const Projection &pipeProj, const ExprRefList &windowExprList) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	util::StackAllocator &alloc = factoryCxt.getAllocator();
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

		const bool withFrame = false;
		ExprRefList dupWindowExprList(alloc);
		duplicateExprList(builder, windowExprList, dupWindowExprList);
		countProj = &createProjectionWithWindow(
				builder, pipeProj, baseProj, dupWindowExprList, withFrame);
	}

	Projection *posProj = NULL;
	if (posExpr != NULL &&
			posExpr->getCode().getType() != SQLType::EXPR_CONSTANT) {
		posProj = &builder.createProjection(SQLOpTypes::PROJ_OUTPUT);
		posProj->getPlanningCode().setAttributes(
				SQLExprs::ExprCode::ATTR_ASSIGNED);
		posProj->getPlanningCode().setOutput(1);

		SQLExprs::Expression::ModIterator it(*posProj);

		SQLExprs::Expression &idExpr =
				SQLExprs::ExprRewriter::createTypedIdExpr(factoryCxt, input);
		{
			SQLExprs::ExprCode &code = idExpr.getPlanningCode();
			code.setColumnType(
					SQLValues::TypeUtils::toNullable(code.getColumnType()));
		}
		it.append(idExpr);

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
		OpCodeBuilder &builder, TupleColumnType valueColumnType) {
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
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 0, 0, TupleTypes::TYPE_LONG));
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 1, 0,
				SQLValues::TypeUtils::toNullable(TupleTypes::TYPE_LONG)));
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 1, 2,
				SQLValues::TypeUtils::toNullable(valueColumnType)));
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 0, 1,
				SQLValues::TypeUtils::toNullable(TupleTypes::TYPE_LONG)));

		code.setPipeProjection(&proj);
	}

	return code;
}

SQLOps::OpCode SQLSortOps::Window::createPositioningSortCode(
		OpCodeBuilder &builder, TupleColumnType valueColumnType) {
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
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 0, 1,
				SQLValues::TypeUtils::toNullable(TupleTypes::TYPE_LONG)));
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 0, 2,
				SQLValues::TypeUtils::toNullable(valueColumnType)));
		it.append(SQLExprs::ExprRewriter::createTypedColumnExprBy(
				factoryCxt, 0, 3,
				SQLValues::TypeUtils::toNullable(TupleTypes::TYPE_LONG)));

		code.setPipeProjection(&proj);
	}

	return code;
}

SQLOps::OpCode SQLSortOps::Window::createMergeCode(
		OpCodeBuilder &builder, SQLOpUtils::ProjectionPair &projections,
		ExprRefList &windowExprList, bool positioning) {
	Projection *pipeProj;
	if (positioning) {
		pipeProj = &builder.createMultiStageProjection(
				*projections.first,
				createUnmatchWindowPipeProjection(builder, *projections.first));
	}
	else {
		pipeProj = projections.first;
	}

	const bool withFrame = false;
	assert(projections.second != NULL);
	Projection &finishProj = createProjectionWithWindow(
			builder, *projections.first, *projections.second, windowExprList,
			withFrame);

	OpCode code;
	code.setPipeProjection(
			&builder.createPipeFinishProjection(*pipeProj, finishProj));
	return code;
}

SQLOps::OpCode SQLSortOps::Window::createSingleCode(
		OpCodeBuilder &builder, const SQLValues::CompColumnList *keyList,
		SQLOpUtils::ProjectionPair &projections,
		ExprRefList &windowExprList, const SQLExprs::Expression *windowOption) {
	OpCode code;
	code.setKeyColumnList(keyList);

	const Projection &pipeProj = *projections.first;

	Projection &pipeFinProj = builder.createPipeFinishProjection(
			*projections.first, *projections.second);
	projections = SQLOpUtils::ProjectionPair();

	const bool withFrame = WindowFramePartition::isFrameEnabled(windowOption);

	code.setPipeProjection(&createProjectionWithWindow(
			builder, pipeProj, pipeFinProj, windowExprList, withFrame));
	code.setFilterPredicate(windowOption);

	return code;
}

void SQLSortOps::Window::splitWindowExpr(
		OpCodeBuilder &builder, Projection &src,
		SQLExprs::Expression *&valueExpr, SQLExprs::Expression *&posExpr,
		bool &valueCounting, ExprRefList &windowExprList) {
	valueExpr = NULL;
	posExpr = NULL;
	valueCounting = false;

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();
	util::StackAllocator &alloc = factoryCxt.getAllocator();

	const SQLExprs::ExprSpec *spec = NULL;
	ExprRefList windowExprRefList(alloc);

	for (SQLExprs::Expression::ModIterator it(src); it.exists(); it.next()) {
		SQLExprs::Expression &subExpr = it.get();
		const SQLExprs::ExprSpec &subSpec =
				factoryCxt.getFactory().getSpec(subExpr.getCode().getType());
		if (SQLExprs::ExprRewriter::isWindowExpr(subSpec, true)) {
			if (spec != NULL && &subSpec != spec) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}
			windowExprRefList.push_back(&subExpr);
			spec = &subSpec;
		}
	}

	SQLExprs::Expression *windowExpr = NULL;
	for (ExprRefList::iterator exprIt = windowExprRefList.begin();
			exprIt != windowExprRefList.end(); ++exprIt) {
		SQLExprs::Expression *windowExprRef = *exprIt;

		windowExpr = &builder.getExprRewriter().rewrite(
				factoryCxt, *windowExprRef, NULL);

		uint32_t index;
		if (SQLExprs::ExprRewriter::findWindowPosArgIndex(*spec, &index)) {
			SQLExprs::Expression::ModIterator baseIt(*windowExpr);
			SQLExprs::Expression::ModIterator it(*windowExprRef);
			const SQLExprs::Expression *subPosExpr = NULL;
			for (uint32_t i = index; it.exists(); it.next(), i--) {
				if (i <= 0) {
					subPosExpr = &it.get();
					break;
				}
				if (baseIt.exists()) {
					baseIt.next();
				}
			}

			const bool forConstant = (subPosExpr == NULL ||
					subPosExpr->getCode().getType() == SQLType::EXPR_CONSTANT);
			if (posExpr == NULL) {
				if (forConstant) {
					TupleValue posValue;
					bool posValueNullable = true;
					if (subPosExpr == NULL) {
						const int64_t basePosValue = 1;
						posValue = TupleValue(basePosValue);
						posValueNullable = false;
					}
					else {
						const TupleValue basePosValue =
								subPosExpr->getCode().getValue();
						if (!SQLValues::ValueUtils::isNull(basePosValue)) {
							posValue = TupleValue(SQLValues::ValueUtils::promoteValue<
									TupleTypes::TYPE_LONG>(basePosValue));
							posValueNullable = false;
						}
					}
					posExpr = &builder.getExprRewriter().createConstExpr(
							factoryCxt, posValue,
							SQLValues::TypeUtils::setNullable(
									TupleTypes::TYPE_LONG, posValueNullable));
				}
				else if (SQLValues::TypeUtils::toNonNullable(
						subPosExpr->getCode().getColumnType()) ==
						TupleTypes::TYPE_LONG) {
					posExpr = &builder.getExprRewriter().rewrite(
							factoryCxt, *subPosExpr, NULL);
				}
				else {
					posExpr = &SQLExprs::ExprRewriter::createCastExpr(
							factoryCxt,
							builder.getExprRewriter().rewrite(
									factoryCxt, *subPosExpr, NULL),
							SQLValues::TypeUtils::toNullable(
									TupleTypes::TYPE_LONG));
				}
				posExpr->getPlanningCode().setAttributes(
						posExpr->getCode().getAttributes() |
						SQLExprs::ExprCode::ATTR_ASSIGNED);
			}
			else if ((posExpr->getCode().getType() ==
					SQLType::EXPR_CONSTANT) != forConstant) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}

			if (forConstant) {
				if (it.exists()) {
					it.remove();
					it.insert(builder.getExprRewriter().rewrite(
							factoryCxt, *posExpr, NULL));
				}
			}
			else {
				it.remove();
				it.insert(SQLExprs::ExprRewriter::createTypedColumnExprBy(
						factoryCxt, WindowMerge::IN_POSITIONING,
						WindowMerge::COLUMN_POSITIONING_ORG,
						TupleTypes::TYPE_LONG));
			}

			if (baseIt.exists()) {
				baseIt.remove();
				baseIt.insert(builder.getExprRewriter().rewrite(
						factoryCxt, *posExpr, NULL));
			}
		}

		valueCounting = ((spec->flags_ &
				SQLExprs::ExprSpec::FLAG_WINDOW_VALUE_COUNTING) != 0);

		do {
			if (!valueCounting && posExpr == NULL) {
				break;
			}

			SQLExprs::Expression::ModIterator it(*windowExprRef);
			if (!it.exists()) {
				break;
			}

			if (valueExpr == NULL) {
				valueExpr = &builder.getExprRewriter().rewrite(
						factoryCxt, it.get(), NULL);
			}

			if (posExpr != NULL) {
				SQLExprs::Expression &posValueExpr =
						createPositioningValueExpr(builder, it.get(), *posExpr);

				it.remove();
				it.insert(posValueExpr);
			}
		}
		while (false);

		if (posExpr != NULL || valueExpr != NULL) {
			windowExprList.push_back(windowExpr);
		}
	}
}

SQLExprs::Expression& SQLSortOps::Window::createPositioningValueExpr(
		OpCodeBuilder &builder, const SQLExprs::Expression &src,
		const SQLExprs::Expression &posExpr) {

	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	const uint32_t destInput = WindowMerge::IN_POSITIONING;

	if (posExpr.getCode().getType() == SQLType::EXPR_CONSTANT) {
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
		SQLExprs::ExprRewriter::Scope rewriterScope(rewriter);
		rewriter.activateColumnMapping(factoryCxt);
		rewriter.setMappedInput(0, destInput);
		return rewriter.rewrite(factoryCxt, src, NULL);
	}
	else {
		SQLExprs::Expression &dest = SQLExprs::ExprRewriter::createColumnExpr(
				factoryCxt, destInput, WindowMerge::COLUMN_POSITIONING_VALUE);
		dest.getPlanningCode().setColumnType(src.getCode().getColumnType());
		return dest;
	}
}

SQLOps::Projection& SQLSortOps::Window::createProjectionWithWindow(
		OpCodeBuilder &builder, const Projection &pipeProj, Projection &src,
		ExprRefList &windowExprList, bool withFrame) {
	SQLOps::Projection *windowProj;
	if (withFrame) {
		SQLOps::Projection &lowerProj =
				createProjectionWithWindowSub(builder, pipeProj, NULL);
		SQLOps::Projection &upperProj =
				createProjectionWithWindowSub(builder, pipeProj, NULL);

		applyFrameAttributes(lowerProj, true, false);
		applyFrameAttributes(upperProj, true, true);

		applyFrameAttributes(src.chainAt(0), false, false);
		applyFrameAttributes(src.chainAt(1), false, false);

		windowProj = &builder.createMultiStageProjection(lowerProj, upperProj);
	}
	else {
		windowProj = &createProjectionWithWindowSub(
				builder, pipeProj, &windowExprList);
	}

	windowExprList.clear();

	SQLOps::Projection &dest =
			builder.createMultiStageProjection(*windowProj, src);

	return dest;
}

SQLOps::Projection& SQLSortOps::Window::createProjectionWithWindowSub(
		OpCodeBuilder &builder, const Projection &pipeProj,
		ExprRefList *windowExprList) {
	Projection &windowProj = builder.createProjection(
			SQLOpTypes::PROJ_AGGR_PIPE, pipeProj.getProjectionCode());

	windowProj.getPlanningCode().setAttributes(
			SQLExprs::ExprCode::ATTR_ASSIGNED |
			SQLExprs::ExprCode::ATTR_AGGR_ARRANGED);

	SQLExprs::Expression::ModIterator destIt(windowProj);
	if (windowExprList == NULL) {
		SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();

		SQLExprs::Expression::Iterator it(pipeProj);
		for (; it.exists(); it.next()) {
			destIt.append(rewriter.rewrite(factoryCxt, it.get(), NULL));
		}
	}
	else {
		for (ExprRefList::iterator exprIt = windowExprList->begin();
				exprIt != windowExprList->end(); ++exprIt) {
			destIt.append(**exprIt);
		}
	}

	return windowProj;
}

SQLOps::Projection& SQLSortOps::Window::createUnmatchWindowPipeProjection(
		OpCodeBuilder &builder, const Projection &src) {
	SQLOps::Projection &dest = builder.rewriteProjection(src);
	assert(dest.getProjectionCode().getType() == SQLOpTypes::PROJ_AGGR_PIPE);

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	for (SQLExprs::Expression::ModIterator projIt(dest);
			projIt.exists(); projIt.next()) {
		setUpUnmatchWindowExpr(factoryCxt, projIt.get());
	}

	return dest;
}

void SQLSortOps::Window::applyFrameAttributes(
		Projection &proj, bool forWindowPipe, bool forward) {
	SQLExprs::ExprCode &code = proj.getProjectionCode().getExprCode();

	uint32_t attributes = code.getAttributes();
	attributes |= SQLExprs::ExprCode::ATTR_AGGR_DECREMENTAL;

	if (forWindowPipe) {
		attributes |= (forward ?
				SQLExprs::ExprCode::ATTR_DECREMENTAL_FORWARD :
				SQLExprs::ExprCode::ATTR_DECREMENTAL_BACKWARD);
	}

	code.setAttributes(attributes);
}

void SQLSortOps::Window::setUpUnmatchWindowExpr(
		SQLExprs::ExprFactoryContext &factoryCxt,
		SQLExprs::Expression &expr) {
	const SQLExprs::ExprSpec &spec =
			factoryCxt.getFactory().getSpec(expr.getCode().getType());
	uint32_t index = 0;
	if (!SQLExprs::ExprRewriter::findWindowPosArgIndex(spec, &index)) {
		return;
	}

	SQLExprs::Expression::ModIterator it(expr);

	if (it.exists()) {
		it.remove();
	}
	it.insert(SQLExprs::ExprRewriter::createEmptyConstExpr(
			factoryCxt, TupleTypes::TYPE_ANY));

	for (uint32_t i = index; it.exists() && i > 0; it.next(), i--) {
	}

	const SQLExprs::Expression *basePosExpr = NULL;
	if (it.exists()) {
		basePosExpr = &it.get();
		it.remove();
	}

	it.insert(createUnmatchPositioningExpr(factoryCxt, basePosExpr));
}

SQLExprs::Expression& SQLSortOps::Window::createUnmatchPositioningExpr(
		SQLExprs::ExprFactoryContext &factoryCxt,
		const SQLExprs::Expression *baseExpr) {

	bool forConstant = true;
	TupleValue posValue;
	if (baseExpr != NULL) {
		const SQLExprs::ExprCode &code = baseExpr->getCode();
		forConstant = (code.getType() == SQLType::EXPR_CONSTANT);
		if (!forConstant || SQLValues::ValueUtils::isNull(code.getValue())) {
			const int64_t posValueNum = 0;
			posValue = TupleValue(posValueNum);
		}
	}

	const TupleColumnType posExprType =
			SQLValues::TypeUtils::toNullable(TupleTypes::TYPE_LONG);
	SQLExprs::Expression &basePosExpr =
			SQLExprs::ExprRewriter::createConstExpr(
					factoryCxt, posValue, posExprType);

	if (forConstant) {
		return basePosExpr;
	}


	const SQLExprs::ExprFactory &factory = factoryCxt.getFactory();
	SQLExprs::Expression &posColumnExpr =
			SQLExprs::ExprRewriter::createTypedColumnExprBy(
					factoryCxt, WindowMerge::IN_POSITIONING,
					WindowMerge::COLUMN_POSITIONING_REF,
					posExprType);

	SQLExprs::Expression *checkingExpr;
	{
		SQLExprs::ExprCode code;
		code.setType(SQLType::OP_IS_NULL);
		code.setColumnType(TupleTypes::TYPE_BOOL);
		checkingExpr = &factory.create(factoryCxt, code);

		SQLExprs::Expression::ModIterator exprIt(*checkingExpr);
		exprIt.append(posColumnExpr);
	}

	SQLExprs::Expression *posExpr;
	{
		SQLExprs::ExprCode code;
		code.setType(SQLType::EXPR_CASE);
		code.setColumnType(posExprType);
		posExpr = &factory.create(factoryCxt, code);

		SQLExprs::Expression::ModIterator exprIt(*posExpr);
		exprIt.append(*checkingExpr);
		exprIt.append(basePosExpr);
	}

	return *posExpr;
}

void SQLSortOps::Window::duplicateExprList(
		OpCodeBuilder &builder, const ExprRefList &src, ExprRefList &dest) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	for (ExprRefList::const_iterator it = src.begin(); it != src.end(); ++it) {
		dest.push_back(
				&builder.getExprRewriter().rewrite(factoryCxt, **it, NULL));
	}
}

SQLOpTypes::Type SQLSortOps::Window::getSingleNodeType(
		OpCodeBuilder &builder, const Projection &pipeProj,
		const SQLExprs::Expression *windowOption) {
	if (WindowFramePartition::isFrameEnabled(windowOption)) {
		return SQLOpTypes::OP_WINDOW_FRAME_PARTITION;
	}

	return (isRanking(builder, pipeProj) ?
			SQLOpTypes::OP_WINDOW_RANK_PARTITION :
			SQLOpTypes::OP_WINDOW_PARTITION);
}

bool SQLSortOps::Window::isRanking(
		OpCodeBuilder &builder, const Projection &pipeProj) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	for (SQLExprs::Expression::Iterator it(pipeProj); it.exists(); it.next()) {
		const SQLExprs::ExprSpec &spec = factoryCxt.getFactory().getSpec(
				it.get().getCode().getType());
		if ((spec.flags_ & SQLExprs::ExprSpec::FLAG_WINDOW) != 0) {
			return true;
		}
	}

	return false;
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

		SQLValues::VarContext::Scope varScope(cxt.getVarContext());

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

SQLSortOps::WindowPartition::TupleEq*
SQLSortOps::WindowPartition::createKeyComparator(
		OpContext &cxt, const SQLValues::CompColumnList *keyList,
		util::LocalUniquePtr<TupleEq> &ptr) {
	if (keyList != NULL && !keyList->empty()) {
		ptr = UTIL_MAKE_LOCAL_UNIQUE(
				ptr, TupleEq, cxt.getAllocator(),
				SQLValues::TupleComparator(
						*keyList, &cxt.getVarContext(), false));
	}
	return ptr.get();
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

	bool withPos = false;
	do {
		uint32_t index;
		if (!SQLExprs::ExprRewriter::findWindowPosArgIndex(spec, &index)) {
			break;
		}

		withPos = true;
		following = ((spec.inList_[index].flags_ &
				SQLExprs::ExprSpec::FLAG_WINDOW_POS_AFTER) != 0);

		SQLExprs::Expression::Iterator it(*expr);
		for (uint32_t i = index; it.exists() && i > 0; it.next(), i--) {
		}

		if (!it.exists()) {
			break;
		}

		const SQLExprs::Expression &posExprBase = it.get();
		if (posExprBase.getCode().getType() == SQLType::EXPR_CONSTANT) {
			break;
		}

		posExpr = &posExprBase;
	}
	while (false);

	if ((spec.flags_ & SQLExprs::ExprSpec::FLAG_WINDOW_VALUE_COUNTING) != 0 ||
			withPos) {
		valueExpr = &expr->child();
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
	partCxt.keyEq_ = createKeyComparator(cxt, keyList, partCxt.keyEqBase_);

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
	SQLValues::ValueUtils::writeValue<CountTypeTag>(
			tuple, partCxt.orgPositionColumn_, curPos);

	if (SQLValues::ValueUtils::isNull(posValue)) {
		SQLValues::ValueUtils::writeNull(tuple, partCxt.refPositionColumn_);
	}
	else {
		const bool following = (partCxt.relativePositionDirection_ > 0);
		int64_t pos;
		toAbsolutePosition(
				curPos, SQLValues::ValueUtils::getValue<int64_t>(posValue),
				following, pos);
		SQLValues::ValueUtils::writeValue<CountTypeTag>(
				tuple, partCxt.refPositionColumn_, pos);
	}

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


void SQLSortOps::WindowRankPartition::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_READER_MULTI);

	Operator::compile(cxt);
}

void SQLSortOps::WindowRankPartition::execute(OpContext &cxt) const {
	const Projection *pipeProj;
	const Projection *subFinishProj;
	getProjections(pipeProj, subFinishProj);

	PartitionContext &partCxt = preparePartitionContext(cxt);
	bool &pipeDone = partCxt.pipeDone_;
	int64_t &rankGap = partCxt.rankGap_;

	TupleListReader *keyReader = prepareKeyReader(cxt, partCxt);
	if (keyReader == NULL) {
		return;
	}

	TupleListReader *&activeReaderRef = *cxt.getActiveReaderRef();
	TupleListReader &pipeReader = preparePipeReader(cxt);
	TupleListReader &subFinishReader = prepareSubFinishReader(cxt);

	for (;;) {
		const bool totalTail = !keyReader->exists();
		if (totalTail && !cxt.isAllInputCompleted()) {
			return;
		}

		if (!pipeDone) {
			if (cxt.checkSuspended()) {
				return;
			}
			activeReaderRef = &pipeReader;
			pipeProj->project(cxt);
			pipeDone = true;
			rankGap++;
		}

		const bool partTail = totalTail || (partCxt.partEq_ != NULL &&
				!(*partCxt.partEq_)(
						SQLValues::TupleListReaderSource(pipeReader),
						SQLValues::TupleListReaderSource(*keyReader)));
		const bool rankTail = partTail || (partCxt.rankEq_ != NULL &&
				!(*partCxt.rankEq_)(
						SQLValues::TupleListReaderSource(pipeReader),
						SQLValues::TupleListReaderSource(*keyReader)));
		if (rankTail) {
			activeReaderRef = &subFinishReader;
			for (; rankGap > 0; rankGap--) {
				if (cxt.checkSuspended()) {
					return;
				}
				subFinishProj->project(cxt);
				subFinishReader.next();
			}

			if (partTail) {
				if (totalTail) {
					break;
				}
				cxt.getExprContext().initializeAggregationValues();
			}
		}

		pipeDone = false;
		keyReader->next();
		pipeReader.next();
	}
}

void SQLSortOps::WindowRankPartition::resolveKeyListAt(
		const OpCode &code, SQLValues::CompColumnList &partKeyList,
		SQLValues::CompColumnList &rankKeyList) {
	const SQLValues::CompColumnList *baseKeyList = code.findKeyColumnList();
	if (baseKeyList == NULL) {
		return;
	}

	SQLValues::CompColumnList::const_iterator midIt = baseKeyList->begin();
	for (; midIt != baseKeyList->end(); ++midIt) {
		if (!midIt->isAscending()) {
			break;
		}
	}

	partKeyList.assign(baseKeyList->begin(), midIt);
	rankKeyList.assign(midIt, baseKeyList->end());
}

void SQLSortOps::WindowRankPartition::getProjections(
		const Projection *&pipeProj, const Projection *&subFinishProj) const {
	const Projection *totalProj = getCode().getPipeProjection();

	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
	const Projection *pipeFinishProj = &totalProj->chainAt(1);

	assert(pipeFinishProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_PIPE_FINISH);
	pipeProj = &pipeFinishProj->chainAt(0);
	subFinishProj = &pipeFinishProj->chainAt(1);

	assert(SQLOps::OpCodeBuilder::findOutputProjection(
			*subFinishProj, NULL) != NULL);
}

SQLSortOps::WindowRankPartition::PartitionContext&
SQLSortOps::WindowRankPartition::preparePartitionContext(
		OpContext &cxt) const {
	if (!cxt.getResource(0).isEmpty()) {
		return cxt.getResource(0).resolveAs<PartitionContext>();
	}

	cxt.getResource(0) = ALLOC_UNIQUE(
			cxt.getAllocator(), PartitionContext, cxt.getAllocator());
	PartitionContext &partCxt = cxt.getResource(0).resolveAs<PartitionContext>();
	resolveKeyList(partCxt.partKeyList_, partCxt.rankKeyList_);

	partCxt.partEq_ = WindowPartition::createKeyComparator(
			cxt, &partCxt.partKeyList_, partCxt.partEqBase_);
	partCxt.rankEq_ = WindowPartition::createKeyComparator(
			cxt, &partCxt.rankKeyList_, partCxt.rankEqBase_);

	return partCxt;
}

void SQLSortOps::WindowRankPartition::resolveKeyList(
		SQLValues::CompColumnList &partKeyList,
		SQLValues::CompColumnList &rankKeyList) const {
	resolveKeyListAt(getCode(), partKeyList, rankKeyList);
}

SQLOps::TupleListReader* SQLSortOps::WindowRankPartition::prepareKeyReader(
		OpContext &cxt, PartitionContext &partCxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, 0);
	if (!partCxt.keyReaderStarted_) {
		if (!reader.exists()) {
			return NULL;
		}
		reader.next();
		partCxt.keyReaderStarted_ = true;
	}

	return &reader;
}

SQLOps::TupleListReader& SQLSortOps::WindowRankPartition::preparePipeReader(
		OpContext &cxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, 1);
	reader.exists();
	return reader;
}

SQLOps::TupleListReader&
SQLSortOps::WindowRankPartition::prepareSubFinishReader(OpContext &cxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, 2);
	reader.exists();
	return reader;
}


SQLSortOps::WindowRankPartition::PartitionContext::PartitionContext(
		util::StackAllocator &alloc) :
		partEq_(NULL),
		rankEq_(NULL),
		partKeyList_(alloc),
		rankKeyList_(alloc),
		keyReaderStarted_(false),
		pipeDone_(false),
		rankGap_(0) {
}


void SQLSortOps::WindowFramePartition::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_READER_MULTI);
	cxt.setReaderRandomAccess(0);

	Operator::compile(cxt);
}

void SQLSortOps::WindowFramePartition::execute(OpContext &cxt) const {
	const Projection *pipeProj;
	const Projection *lowerProj;
	const Projection *subFinishProj;
	getProjections(pipeProj, lowerProj, subFinishProj);

	PartitionContext &partCxt = preparePartitionContext(cxt, *pipeProj);

	int64_t &lowerPos = partCxt.lowerPos_;
	int64_t &upperPos = partCxt.upperPos_;

	int64_t &currentPos = partCxt.currentPos_;
	int64_t &tailPos = partCxt.initialPipePos_;

	TupleListReader *keyReader = prepareKeyReader(cxt, partCxt);
	if (keyReader == NULL) {
		return;
	}

	TupleListReader *&activeReaderRef = *cxt.getActiveReaderRef();
	TupleListReader &pipeReader = preparePipeReader(cxt);
	TupleListReader &lowerReader = prepareLowerReader(cxt);
	TupleListReader &subFinishReader = prepareSubFinishReader(cxt);

	util::LocalUniquePtr<OrderedAggregator> aggregator;
	if (!prepareAggregator(cxt, partCxt, pipeReader, aggregator)) {
		return;
	}

	for (;;) {
		const bool totalTail = !keyReader->exists();
		if (totalTail && !cxt.isAllInputCompleted()) {
			return;
		}

		int64_t maxPos = -1;
		for (;;) {
			const int64_t subMaxPos = std::max(currentPos, upperPos);
			if (tailPos < 0 && maxPos >= 0 && subMaxPos != maxPos) {
				break;
			}
			maxPos = subMaxPos;

			if (tailPos < 0) {
				TupleListReader &tailReader = (upperPos < currentPos ?
						subFinishReader : pipeReader);
				if (isPartTail(partCxt, totalTail, tailReader, *keyReader)) {
					tailPos = maxPos;
				}
			}

			if (tailPos >= 0 && currentPos > tailPos) {
				if (totalTail) {
					return;
				}

				lowerReader.assign(*keyReader);
				pipeReader.assign(*keyReader);
				subFinishReader.assign(*keyReader);

				clearAggregation(cxt, aggregator.get(), pipeReader);

				lowerPos = 0;
				upperPos = 0;
				currentPos = 0;
				tailPos = -1;
				break;
			}

			if (cxt.checkSuspended()) {
				return;
			}

			const bool upperInside = (
					(tailPos < 0 || upperPos <= tailPos) &&
					isUpperInside(partCxt, subFinishReader, pipeReader));

			if (upperInside) {
				if (!resoveActivePipeReader(
						cxt, aggregator.get(), true, pipeReader,
						activeReaderRef)) {
					return;
				}
				if (activeReaderRef != NULL) {
					pipeProj->project(cxt);
				}
				pipeReader.next();
				upperPos++;
				continue;
			}

			const bool lowerOutside = (
					lowerPos < upperPos &&
					!isLowerInside(partCxt, subFinishReader, lowerReader));

			if (lowerOutside) {
				if (!resoveActivePipeReader(
						cxt, aggregator.get(), false, lowerReader,
						activeReaderRef)) {
					return;
				}
				if (activeReaderRef != NULL) {
					lowerProj->project(cxt);
				}
				lowerReader.next();
				lowerPos++;
				continue;
			}

			activeReaderRef = &subFinishReader;
			subFinishProj->project(cxt);
			subFinishReader.next();
			currentPos++;
		}

		keyReader->next();
	}
}

bool SQLSortOps::WindowFramePartition::isFrameEnabled(
		const SQLExprs::Expression *windowOption) {
	return (windowOption != NULL && windowOption->getChildCount() >= 3);
}

bool SQLSortOps::WindowFramePartition::isPartTail(
		PartitionContext &partCxt, bool totalTail,
		TupleListReader &tailReader, TupleListReader &keyReader) {
	return totalTail || (partCxt.partEq_ != NULL &&
			!(*partCxt.partEq_)(
					SQLValues::TupleListReaderSource(tailReader),
					SQLValues::TupleListReaderSource(keyReader)));
}

bool SQLSortOps::WindowFramePartition::isLowerInside(
		PartitionContext &partCxt, TupleListReader &subFinishReader,
		TupleListReader &lowerReader) {
	const Boundary &boundary = partCxt.boundary_.first;
	if (boundary.unbounded_) {
		return true;
	}
	else if (boundary.rows_) {
		const int64_t &lowerPos = partCxt.lowerPos_;
		const int64_t &currentPos = partCxt.currentPos_;
		return (lowerPos - currentPos) >= boundary.distance_.toLong();
	}
	else if (boundary.current_) {
		return isInsideByCurrent(partCxt, subFinishReader, lowerReader);
	}
	else {
		return isInsideByRange(partCxt, false, subFinishReader, lowerReader);
	}
}

bool SQLSortOps::WindowFramePartition::isUpperInside(
		PartitionContext &partCxt, TupleListReader &subFinishReader,
		TupleListReader &pipeReader) {
	const Boundary &boundary = partCxt.boundary_.second;
	if (boundary.unbounded_) {
		return true;
	}
	else if (boundary.rows_) {
		const int64_t &upperPos = partCxt.upperPos_;
		const int64_t &currentPos = partCxt.currentPos_;
		return (upperPos - currentPos) <= boundary.distance_.toLong();
	}
	else if (boundary.current_) {
		return isInsideByCurrent(partCxt, subFinishReader, pipeReader);
	}
	else {
		return isInsideByRange(partCxt, true, subFinishReader, pipeReader);
	}
}

bool SQLSortOps::WindowFramePartition::isInsideByCurrent(
		PartitionContext &partCxt, TupleListReader &lastReader,
		TupleListReader &nextReader) {
	return (partCxt.rankEq_ == NULL ||
			(*partCxt.rankEq_)(
					SQLValues::TupleListReaderSource(lastReader),
					SQLValues::TupleListReaderSource(nextReader)));
}

bool SQLSortOps::WindowFramePartition::isInsideByRange(
		PartitionContext &partCxt, bool upper, TupleListReader &lastReader,
		TupleListReader &nextReader) {
	const Boundary &boundary =
			(upper ? partCxt.boundary_.second : partCxt.boundary_.first);

	const SQLValues::CompColumnList &rankKeyList = partCxt.rankKeyList_;
	assert(!rankKeyList.empty());

	const TupleColumn &column = rankKeyList.front().getTupleColumn1();

	const SQLValues::LongInt &last = SQLValues::ValueUtils::toLongInt(
			SQLValues::ValueUtils::readCurrentAny(lastReader, column));
	const SQLValues::LongInt &next = SQLValues::ValueUtils::toLongInt(
			SQLValues::ValueUtils::readCurrentAny(nextReader, column));
	if (!last.isValid() || !next.isValid()) {
		return (!last.isValid() && !next.isValid());
	}

	const SQLValues::LongInt &delta =
			boundary.distance_.negate(!partCxt.rankAscending_);

	bool overflowUpper;
	if (last.checkAdditionOverflow(delta, overflowUpper)) {
		return overflowUpper;
	}
	else {
		const SQLValues::LongInt &lastWithDelta = last.add(delta);
		if (upper) {
			return next.isLessThanEq(lastWithDelta);
		}
		else {
			return lastWithDelta.isLessThanEq(next);
		}
	}
}

bool SQLSortOps::WindowFramePartition::resoveActivePipeReader(
		OpContext &cxt, OrderedAggregator *aggregator, bool upper,
		TupleListReader &base, TupleListReader *&activeReaderRef) {
	bool done;
	TupleListReader *ref;

	do {
		if (aggregator == NULL) {
			ref = &base;
			done = true;
			break;
		}

		if (!aggregator->update(cxt, upper, base)) {
			ref = NULL;
			done = false;
			break;
		}

		cxt.getExprContext().initializeAggregationValues();
		ref = aggregator->getTop();
		done = true;
	}
	while (false);

	activeReaderRef = ref;
	return done;
}

void SQLSortOps::WindowFramePartition::clearAggregation(
		OpContext &cxt, OrderedAggregator *aggregator,
		TupleListReader &tailReader) {
	cxt.getExprContext().initializeAggregationValues();

	if (aggregator != NULL) {
		aggregator->reset(cxt, tailReader);
	}
}

void SQLSortOps::WindowFramePartition::getProjections(
		const Projection *&pipeProj, const Projection *&lowerProj,
		const Projection *&subFinishProj) const {
	const Projection *totalProj = getCode().getPipeProjection();

	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
	const Projection &basePipeProj = totalProj->chainAt(0);
	const Projection &pipeFinishProj = totalProj->chainAt(1);

	assert(pipeFinishProj.getProjectionCode().getType() ==
			SQLOpTypes::PROJ_PIPE_FINISH);
	subFinishProj = &pipeFinishProj.chainAt(1);

	assert(SQLOps::OpCodeBuilder::findOutputProjection(
			*subFinishProj, NULL) != NULL);

	assert(basePipeProj.getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
	lowerProj = &basePipeProj.chainAt(0);
	pipeProj = &basePipeProj.chainAt(1);
}

SQLSortOps::WindowFramePartition::PartitionContext&
SQLSortOps::WindowFramePartition::preparePartitionContext(
		OpContext &cxt, const Projection &pipeProj) const {
	if (!cxt.getResource(0).isEmpty()) {
		return cxt.getResource(0).resolveAs<PartitionContext>();
	}

	cxt.getResource(0) = ALLOC_UNIQUE(
			cxt.getAllocator(), PartitionContext, cxt.getAllocator());
	PartitionContext &partCxt = cxt.getResource(0).resolveAs<PartitionContext>();

	WindowRankPartition::resolveKeyListAt(
			getCode(), partCxt.partKeyList_, partCxt.rankKeyList_);

	partCxt.partEq_ = WindowPartition::createKeyComparator(
			cxt, &partCxt.partKeyList_, partCxt.partEqBase_);
	partCxt.rankEq_ = WindowPartition::createKeyComparator(
			cxt, &partCxt.rankKeyList_, partCxt.rankEqBase_);

	const SQLExprs::Expression *windowOption = getCode().getFilterPredicate();
	applyWindowOption(partCxt, windowOption);

	tryPrepareAggregatorInfo(cxt, partCxt, pipeProj);

	return partCxt;
}

void SQLSortOps::WindowFramePartition::tryPrepareAggregatorInfo(
		OpContext &cxt, PartitionContext &partCxt,
		const Projection &pipeProj) const {
	SQLValues::CompColumnList aggrKeyList(cxt.getAllocator());

	SQLExprs::Expression::Iterator exprIt(pipeProj);
	for (; exprIt.exists(); exprIt.next()) {
		const SQLExprs::ExprType type = exprIt.get().getCode().getType();
		if (SQLExprs::ExprTypeUtils::isNormalWindow(type) &&
				!SQLExprs::ExprTypeUtils::isDecremental(type)) {
			SQLExprs::Expression::Iterator argIt(exprIt.get());
			const SQLExprs::ExprCode *code =
					(argIt.exists() ? &argIt.get().getCode() : NULL);

			if (code == NULL || code->getType() != SQLType::EXPR_COLUMN) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}

			SQLValues::SummaryColumn::Source src;
			src.tupleColumn_ = cxt.getReaderColumn(0, code->getColumnPos());
			const SQLValues::SummaryColumn summaryColumn(src);

			const bool ascending = (type == SQLType::AGG_MIN);
			SQLValues::CompColumn column;
			column.setSummaryColumn(summaryColumn, true);
			column.setSummaryColumn(summaryColumn, false);

			column.setType(code->getColumnType());
			column.setColumnPos(code->getColumnPos(), true);
			column.setAscending(ascending);
			aggrKeyList.push_back(column);
			break;
		}
	}

	if (!aggrKeyList.empty()) {
		partCxt.aggrInfo_ = UTIL_MAKE_LOCAL_UNIQUE(
				partCxt.aggrInfo_, OrderedAggregatorInfo,
				cxt.getValueContext(), aggrKeyList);
	}
}

void SQLSortOps::WindowFramePartition::applyWindowOption(
		PartitionContext &partCxt, const SQLExprs::Expression *windowOption) {
	assert(isFrameEnabled(windowOption));

	SQLExprs::Expression::Iterator it(*windowOption);
	const int64_t flags = it.get().getCode().getValue().get<int64_t>();
	it.next();
	const TupleValue &startValue = it.get().getCode().getValue();
	it.next();
	const TupleValue &finishValue = it.get().getCode().getValue();

	partCxt.rankAscending_ = ((flags & (1 << SQLType::FRAME_ASCENDING)) != 0);

	applyBoundaryOption(partCxt, flags, startValue, true);
	applyBoundaryOption(partCxt, flags, finishValue, false);
}

void SQLSortOps::WindowFramePartition::applyBoundaryOption(
		PartitionContext &partCxt, int64_t flags, const TupleValue &value,
		bool start) {
	const bool preceding = ((flags & (1 << (start ?
			SQLType::FRAME_START_PRECEDING :
			SQLType::FRAME_FINISH_PRECEDING))) != 0);
	const bool following = ((flags & (1 << (start ?
			SQLType::FRAME_START_FOLLOWING :
			SQLType::FRAME_FINISH_FOLLOWING))) != 0);

	const bool withDirection = (preceding || following);
	const bool withValue = !SQLValues::ValueUtils::isNull(value);

	const SQLValues::LongInt &distance = (withValue ?
			SQLValues::ValueUtils::toLongInt(value).negate(preceding) :
			SQLValues::ValueUtils::toLongInt(
					TupleValue(static_cast<int64_t>(0))));

	Boundary &boundary =
			(start ? partCxt.boundary_.first : partCxt.boundary_.second);

	boundary.distance_ = distance;
	boundary.unbounded_ = (withDirection && !withValue);
	boundary.rows_ = ((flags & (1 << SQLType::FRAME_ROWS)) != 0);
	boundary.current_ = !withDirection;
}

bool SQLSortOps::WindowFramePartition::prepareAggregator(
		OpContext &cxt, PartitionContext &partCxt,
		TupleListReader &pipeReader,
		util::LocalUniquePtr<OrderedAggregator> &aggregator) {
	OrderedAggregatorInfo *info = partCxt.aggrInfo_.get();
	if (info != NULL) {
		aggregator = UTIL_MAKE_LOCAL_UNIQUE(
				aggregator, OrderedAggregator, cxt, *info, pipeReader);
		if (!aggregator->resumeUpdates(cxt)) {
			return false;
		}
	}
	return true;
}

SQLOps::TupleListReader* SQLSortOps::WindowFramePartition::prepareKeyReader(
		OpContext &cxt, PartitionContext &partCxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, IN_KEY);
	if (!partCxt.keyReaderStarted_) {
		if (!reader.exists()) {
			return NULL;
		}
		reader.next();
		partCxt.keyReaderStarted_ = true;
	}

	return &reader;
}

SQLOps::TupleListReader& SQLSortOps::WindowFramePartition::preparePipeReader(
		OpContext &cxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, IN_PIPE);
	reader.exists();
	return reader;
}

SQLOps::TupleListReader& SQLSortOps::WindowFramePartition::prepareLowerReader(
		OpContext &cxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, IN_LOWER);
	reader.exists();
	return reader;
}

SQLOps::TupleListReader&
SQLSortOps::WindowFramePartition::prepareSubFinishReader(
		OpContext &cxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, IN_SUB_FINISH);
	reader.exists();
	return reader;
}


SQLSortOps::WindowFramePartition::OrderedAggregatorKey::OrderedAggregatorKey(
		TupleListReader &reader, uint32_t level, bool upper) :
		reader_(reader),
		level_(level),
		upper_(upper) {
}


SQLSortOps::WindowFramePartition::
OrderedAggregatorKeyLess::OrderedAggregatorKeyLess(
		const ReaderTupleComp &aggrComp) :
		aggrComp_(aggrComp) {
}

bool SQLSortOps::WindowFramePartition::OrderedAggregatorKeyLess::operator()(
		const Key &key1, const Key &key2) const {
	int32_t ret;

	if ((ret = aggrComp_(
			SQLValues::TupleListReaderSource(key1.reader_),
			SQLValues::TupleListReaderSource(key2.reader_))) != 0) {
		return (ret < 0);
	}

	if ((ret = SQLValues::ValueUtils::compareRawValue(
			key1.level_, key2.level_)) != 0) {
		return (ret < 0);
	}

	return (SQLValues::ValueUtils::compareIntegral(
			(key1.upper_ ? 1 : 0),
			(key2.upper_ ? 1 : 0)) < 0);
}


SQLSortOps::WindowFramePartition::
OrderedAggregatorPosition::OrderedAggregatorPosition(
		uint32_t keyReaderId, uint32_t tailReaderId) :
		keyReaderId_(keyReaderId),
		tailReaderId_(tailReaderId),
		tailPos_(0),
		active_(false) {
}


SQLSortOps::WindowFramePartition::OrderedAggregatorInfo::OrderedAggregatorInfo(
		SQLValues::ValueContext &valueCxt,
		const SQLValues::CompColumnList &aggrKeyList) :
		lowerTargetPos_(0),
		upperTargetPos_(0),
		lowerPosList_(valueCxt.getVarAllocator()),
		upperPosList_(valueCxt.getVarAllocator()),
		aggrKeyList_(
				aggrKeyList.begin(), aggrKeyList.end(),
				valueCxt.getAllocator()),
		baseAggrComp_(aggrKeyList_, &valueCxt.getVarContext(), true),
		aggrComp_(valueCxt.getAllocator(), baseAggrComp_),
		aggrLess_(valueCxt.getAllocator(), baseAggrComp_),
		keyLess_(aggrComp_),
		updating_(false),
		updatingReady_(false),
		upperUpdating_(false) {
}


SQLSortOps::WindowFramePartition::OrderedAggregator::OrderedAggregator(
		OpContext &cxt, Info &info, TupleListReader &curReader) :
		info_(info),
		keySet_(info_.keyLess_, cxt.getValueContext().getVarAllocator()),
		keyItList_(cxt.getValueContext().getVarAllocator()) {
	initializeKeys(cxt);

	const bool resumeOnly =
			(info_.updating_ || info_.lowerTargetPos_ > 0 || info_.upperTargetPos_ > 0);
	resetReaders(cxt, curReader, resumeOnly);
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::update(
		OpContext &cxt, bool upper, TupleListReader &baseReader) {
	do {
		if (info_.updating_) {
			if (!info_.updatingReady_) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}
			break;
		}

		if (upper && info_.lowerTargetPos_ >= info_.upperTargetPos_) {
			reset(cxt, baseReader);
		}

		info_.updating_ = true;
		info_.updatingReady_ = false;
		info_.upperUpdating_ = upper;

		if (!resumeUpdates(cxt)) {
			return false;
		}
	}
	while (false);

	uint64_t &targetPos = getTargetPositionRef(info_.upperUpdating_);
	targetPos++;

	info_.updating_ = false;
	info_.updatingReady_ = false;
	info_.upperUpdating_ = false;
	return true;
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::resumeUpdates(
		OpContext &cxt) {
	if (!info_.updating_) {
		return true;
	}

	if (!updateKeysAt(cxt, false)) {
		return false;
	}

	if (!updateKeysAt(cxt, true)) {
		return false;
	}

	info_.updatingReady_ = true;
	return true;
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::reset(
		OpContext &cxt, TupleListReader &tailReader) {
	clearAllKeys();

	info_.lowerTargetPos_ = 0;
	info_.upperTargetPos_ = 0;

	const bool resumeOnly = false;
	resetReaders(cxt, tailReader, resumeOnly);
}

SQLOps::TupleListReader*
SQLSortOps::WindowFramePartition::OrderedAggregator::getTop() {
	if (keySet_.empty()) {
		return NULL;
	}

	return &keySet_.begin()->reader_;
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::initializeKeys(
		OpContext &cxt) {
	initializeKeysAt(cxt, true);
	initializeKeysAt(cxt, false);
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::initializeKeysAt(
		OpContext &cxt, bool upper) {
	if (info_.updating_) {
		return;
	}

	const uint32_t endLevel = resolveEffectiveEndLevel(upper, true);
	for (uint32_t i = 0; i < endLevel; i++) {
		insertKey(cxt, upper, i);
	}
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::resetReaders(
		OpContext &cxt, TupleListReader &reader, bool resumeOnly) {
	resetReadersAt(cxt, reader, resumeOnly, false);
	resetReadersAt(cxt, reader, resumeOnly, true);
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::resetReadersAt(
		OpContext &cxt, TupleListReader &reader, bool resumeOnly, bool upper) {
	PositionList &posList = (upper ? info_.upperPosList_ : info_.lowerPosList_);

	for (PositionList::iterator it = posList.begin(); it != posList.end(); ++it) {
		if (resumeOnly) {
			if (!isPositionActive(*it)) {
				assignInitialReader(cxt, *it, reader);
			}
		}
		else {
			if (!isPositionActive(*it)) {
				break;
			}
			if (it != posList.begin()) {
				deactivatePosition(*it);
			}
		}
	}

	if (!resumeOnly) {
		Position &pos = resolvePosition(upper, 0);
		assignInitialReader(cxt, pos, reader);
		activatePosition(pos, 0);
	}
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::updateKeysAt(
		OpContext &cxt, bool upper) {
	PositionList &posList = (upper ? info_.upperPosList_ : info_.lowerPosList_);

	const size_t posCount = posList.size();
	const uint32_t nextLevel = resolveEffectiveEndLevel(upper, false);

	for (uint32_t i = nextLevel; i < posCount; i++) {
		eraseKey(upper, i);
		if (i > 0) {
			deactivatePosition(posList[i]);
		}
	}

	for (uint32_t i = 0; i < nextLevel; i++) {
		if (!updateKey(cxt, upper, i)) {
			return false;
		}
	}

	return true;
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::updateKey(
		OpContext &cxt, bool upper, uint32_t level) {
	const uint64_t lastPos = getTargetPosition(upper, true);

	Position &pos = resolvePosition(upper, level);

	if (!isPositionActive(pos)) {
		inheritActivePosition(cxt, upper, level);
	}

	if (isKeyUpdatable(lastPos, upper, level) &&
			!updateKeyDetail(cxt, upper, level, pos)) {
		return false;
	}

	return true;
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::updateKeyDetail(
		OpContext &cxt, bool upper, uint32_t level, Position &pos) {
	assert(isPositionActive(pos));

	eraseKey(upper, level);

	TupleListReader &keyReader = getReader(cxt, pos.keyReaderId_);
	TupleListReader &tailReader = getReader(cxt, pos.tailReaderId_);

	const uint64_t nextPos = getTargetPosition(upper, false);
	const std::pair<uint64_t, uint64_t> range =
			getEffectivePositionRange(nextPos, upper, level);

	const uint64_t endPos = getTargetPosition(true, false);

	while (pos.tailPos_ < range.second && pos.tailPos_ < endPos) {
		if (!tailReader.exists()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		if (cxt.checkSuspended()) {
			return false;
		}

		if (pos.tailPos_ == range.first || info_.aggrLess_(
				SQLValues::TupleListReaderSource(tailReader),
				SQLValues::TupleListReaderSource(keyReader))) {
			keyReader.assign(tailReader);
		}

		pos.tailPos_++;
		tailReader.next();
	}

	insertKey(cxt, upper, level);
	return true;
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::clearAllKeys() {
	keyItList_.clear();
	keySet_.clear();
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::eraseKey(
		bool upper, uint32_t level) {
	if (level >= keyItList_.size()) {
		return;
	}

	KeyIteratorPair &pair = keyItList_[level];
	KeyIterator &it = (upper ? pair.second : pair.first);

	KeyIterator endIt = keySet_.end();
	if (it != endIt) {
		keySet_.erase(it);
		it = endIt;
	}
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::insertKey(
		OpContext &cxt, bool upper, uint32_t level) {
	while (level >= keyItList_.size()) {
		KeyIterator endIt = keySet_.end();
		keyItList_.push_back(KeyIteratorPair(endIt, endIt));
	}

	KeyIteratorPair &pair = keyItList_[level];
	KeyIterator &it = (upper ? pair.second : pair.first);

	PositionList &posList = (upper ? info_.upperPosList_ : info_.lowerPosList_);
	assert(level < posList.size());

	Position &pos = posList[level];

	OrderedAggregatorKey key(
			getReader(cxt, pos.keyReaderId_), level, upper);
	it = keySet_.insert(key).first;
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::isKeyUpdatable(
		uint64_t lastPos, bool upper, uint32_t level) {
	const uint64_t nextPos = getTargetPosition(upper, level);
	const std::pair<uint64_t, uint64_t> range =
			getEffectivePositionRange(nextPos, upper, level);
	return lastPos < range.second;
}

uint32_t
SQLSortOps::WindowFramePartition::OrderedAggregator::resolveEffectiveEndLevel(
		bool upper, bool last) {
	const uint64_t lowerPos = getTargetPosition(false, last);
	const uint64_t upperPos = getTargetPosition(true, last);
	return resolveEffectiveEndLevelAt(lowerPos, upperPos, upper);
}

uint32_t
SQLSortOps::WindowFramePartition::OrderedAggregator::resolveEffectiveEndLevelAt(
		uint64_t lowerPos, uint64_t upperPos, bool upper) {
	for (uint32_t i = 0;; i++) {
		const std::pair<uint64_t, uint64_t> lowerRange =
				getEffectivePositionRange(lowerPos, false, i);
		const std::pair<uint64_t, uint64_t> upperRange =
				getEffectivePositionRange(upperPos, true, i);

		if (lowerRange.second < upperRange.first) {
			continue;
		}
		if (!upper && lowerRange.second <= upperRange.first) {
			continue;
		}
		return i;
	}
}

uint64_t SQLSortOps::WindowFramePartition::OrderedAggregator::getTargetPosition(
		bool upper, bool last) {
	const uint64_t targetPos = getTargetPositionRef(upper);
	const uint64_t delta = (!last && (!upper == !info_.upperUpdating_) ? 1 : 0);
	return targetPos + delta;
}

uint64_t&
SQLSortOps::WindowFramePartition::OrderedAggregator::getTargetPositionRef(
		bool upper) {
	return (upper ? info_.upperTargetPos_ : info_.lowerTargetPos_);
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::inheritActivePosition(
		OpContext &cxt, bool upper, uint32_t level) {
	if (level <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const Position &srcPos = (upper ?
			resolvePosition(false, level) :
			resolvePosition(false, level - 1));

	if (!isPositionActive(srcPos)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	Position &pos = resolvePosition(upper, level);
	assignReader(
			cxt, pos,
			getReader(cxt, srcPos.keyReaderId_),
			getReader(cxt, srcPos.tailReaderId_));

	activatePosition(pos, srcPos.tailPos_);
}

SQLSortOps::WindowFramePartition::OrderedAggregatorPosition&
SQLSortOps::WindowFramePartition::OrderedAggregator::resolvePosition(
		bool upper, uint32_t level) {
	PositionList &posList = (upper ? info_.upperPosList_ : info_.lowerPosList_);

	while (level >= posList.size()) {
		const uint32_t curLevel = static_cast<uint32_t>(posList.size());
		posList.push_back(Position(
				toReaderId(upper, false, curLevel),
				toReaderId(upper, true, curLevel)));
	}

	return posList[level];
}

uint32_t SQLSortOps::WindowFramePartition::OrderedAggregator::toReaderId(
		bool upper, bool forTail, uint32_t level) {
	return (
			IN_AGGR +
			4 * level +
			2 * (upper ? 1 : 0) +
			1 * (forTail ? 1 : 0));
}

SQLOps::TupleListReader&
SQLSortOps::WindowFramePartition::OrderedAggregator::getReader(
		OpContext &cxt, uint32_t id) {
	const uint32_t index = 0;
	const uint32_t sub = id;
	return cxt.getReader(index, sub);
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::assignInitialReader(
		OpContext &cxt, Position &pos, TupleListReader &reader) {
	assignReader(cxt, pos, reader, reader);
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::assignReader(
		OpContext &cxt, Position &pos, TupleListReader &keyReader,
		TupleListReader &tailReader) {
	getReader(cxt, pos.keyReaderId_).assign(keyReader);
	getReader(cxt, pos.tailReaderId_).assign(tailReader);
}

bool SQLSortOps::WindowFramePartition::OrderedAggregator::isPositionActive(
		const Position &pos) {
	return pos.active_;
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::activatePosition(
		Position &pos, uint64_t tailPos) {
	pos.tailPos_ = tailPos;
	pos.active_ = true;
}

void SQLSortOps::WindowFramePartition::OrderedAggregator::deactivatePosition(
		Position &pos) {
	pos.tailPos_ = 0;
	pos.active_ = false;
}

std::pair<uint64_t, uint64_t>
SQLSortOps::WindowFramePartition::OrderedAggregator::getEffectivePositionRange(
		uint64_t pos, bool upper, uint32_t level) {
	const uint64_t beginPos = roundPosition(pos, upper, level);
	const uint64_t endPos = beginPos + getPositionRangeSize(level);
	return std::make_pair(beginPos, endPos);
}

uint64_t SQLSortOps::WindowFramePartition::OrderedAggregator::roundPosition(
		uint64_t pos, bool upper, uint32_t level) {
	const uint64_t size = getPositionRangeSize(level);
	const uint64_t offset = (upper ? 0 : (size - 1));
	return (pos + offset) / size * size;
}

uint64_t
SQLSortOps::WindowFramePartition::OrderedAggregator::getPositionRangeSize(
		uint32_t level) {
	assert(level < sizeof(uint64_t) * CHAR_BIT);
	return (UINT64_C(1) << level);
}


SQLSortOps::WindowFramePartition::Boundary::Boundary() :
		distance_(SQLValues::LongInt::invalid()),
		unbounded_(false),
		rows_(false),
		current_(false) {
}


SQLSortOps::WindowFramePartition::PartitionContext::PartitionContext(
		util::StackAllocator &alloc) :
		partEq_(NULL),
		rankEq_(NULL),
		partKeyList_(alloc),
		rankKeyList_(alloc),
		keyReaderStarted_(false),
		rankAscending_(false),
		lowerPos_(0),
		upperPos_(0),
		currentPos_(0),
		initialPipePos_(-1) {
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
		OpContext &cxt, bool &following, bool &withDirection,
		int64_t &amount) const {
	following = false;
	withDirection = false;
	amount = 0;

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

	const SQLExprs::Expression *posExpr = NULL;
	uint32_t index;
	if (SQLExprs::ExprRewriter::findWindowPosArgIndex(spec, &index)) {
		following = ((spec.inList_[index].flags_ &
				SQLExprs::ExprSpec::FLAG_WINDOW_POS_AFTER) != 0);

		SQLExprs::Expression::Iterator it(expr);
		for (uint32_t i = index; it.exists() && i > 0; it.next(), i--) {
		}

		if (it.exists()) {
			posExpr = &it.get();
			if (posExpr->getCode().getType() == SQLType::EXPR_CONSTANT) {
				withDirection = true;
				const TupleValue posValue = posExpr->getCode().getValue();
				if (SQLValues::ValueUtils::isNull(posValue)) {
					amount = std::numeric_limits<int64_t>::max();
				}
				else {
					amount = SQLValues::ValueUtils::getValue<int64_t>(posValue);
				}
			}
		}
		else {
			withDirection = true;
			amount = 1;
		}
	}

	return posExpr;
}

SQLSortOps::WindowMerge::MergeContext&
SQLSortOps::WindowMerge::prepareMergeContext(OpContext &cxt) const {
	if (!cxt.getResource(0).isEmpty()) {
		return cxt.getResource(0).resolveAs<MergeContext>();
	}

	cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), MergeContext);
	MergeContext &mergeCxt = cxt.getResource(0).resolveAs<MergeContext>();

	bool following;
	bool withDirection;
	int64_t amount;
	const SQLExprs::Expression *expr =
			findPositioningExpr(cxt, following, withDirection, amount);
	if (expr == NULL && !withDirection) {
		mergeCxt.positioningReaderProgress_ = -1;
		mergeCxt.relativePositionDirection_ = -1;
	}
	else {
		mergeCxt.positioningReaderProgress_ = 0;

		if (withDirection) {
			mergeCxt.relativePositionDirection_ = (following ? 1 : 0);
			mergeCxt.relativePositionAmount_ = amount;
		}
		else {
			mergeCxt.relativePositionDirection_ = -1;
			mergeCxt.positionColumn_ =
					cxt.getReaderColumn(IN_POSITIONING, 0);
		}
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

	return &cxt.getReader(IN_POSITIONING);
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
