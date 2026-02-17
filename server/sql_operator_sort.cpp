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
	add<SQLOpTypes::OP_WINDOW_MATCH, WindowMatch>();

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
	OpCode srcCode = getCode();

	if (WindowMatch::tryMakeWindowSubPlan(cxt, srcCode)) {
		return;
	}

	OpPlan &plan = cxt.getPlan();

	util::StackAllocator &alloc = cxt.getAllocator();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));


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


void SQLSortOps::WindowMatch::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_READER_MULTI);
	cxt.setReaderRandomAccess(0);
	Operator::compile(cxt);
}

void SQLSortOps::WindowMatch::execute(OpContext &cxt) const {
	MatchContext &matchCxt = prepareMatchContext(cxt);

	TupleListReader *predReader;
	TupleListReader *keyReader;
	if (!preparePredicateReaders(cxt, matchCxt, predReader, keyReader)) {
		return;
	}

	for (;;) {
		const bool partTail =
				isPartitionTail(matchCxt, *predReader, *keyReader);

		if (!matchRow(cxt, matchCxt, partTail)) {
			return;
		}

		if (!projectMatchedRows(cxt, matchCxt)) {
			return;
		}

		if (!nextPrediateRow(
				cxt, matchCxt, *predReader, *keyReader, partTail)) {
			break;
		}
	}
}

bool SQLSortOps::WindowMatch::tryMakeWindowSubPlan(
		OpContext &cxt, const OpCode &code) {
	if (!MicroCompiler::isAcceptableWindowPlan(code)) {
		return false;
	}

	MicroCompiler compiler(cxt.getAllocator());
	compiler.makeWindowSubPlan(cxt, code);
	return true;
}

bool SQLSortOps::WindowMatch::matchRow(
		OpContext &cxt, MatchContext &matchCxt, bool partTail) {
	VariableTable &table = matchCxt.variableTable_;
	ColumnNavigator &navigator = matchCxt.navigator_;

	if (partTail) {
		table.setPartitionTail();
	}

	const VariablePredicateInfo *predInfo;
	while (table.findMatchingVariable(predInfo)) {
		if (!navigator.navigate(cxt, table)) {
			return false;
		}

		const TupleValue &value = predInfo->expr_->eval(cxt.getExprContext());
		navigator.clear();

		const bool matched = (
				!SQLValues::ValueUtils::isNull(value) &&
				SQLValues::ValueUtils::toBool(value));
		table.updateMatchingVariable(matched);

		checkMemoryLimit(cxt, matchCxt);
	}

	checkMemoryLimit(cxt, matchCxt);
	return true;
}

bool SQLSortOps::WindowMatch::projectMatchedRows(
		OpContext &cxt, MatchContext &matchCxt) {
	VariableTable &table = matchCxt.variableTable_;
	ColumnNavigator &navigator = matchCxt.navigator_;
	const bool forAllRows = matchCxt.allRowsPerMatch_;

	NavigationType type;
	bool matchTail;
	while (table.prepareProjection(forAllRows, type, matchTail)) {
		const Projection *proj = matchCxt.findProjectionAt(type);
		if (proj != NULL) {
			if (!navigator.navigate(cxt, table)) {
				return false;
			}

			proj->project(cxt);
			navigator.clear();
		}

		if (matchTail) {
			cxt.getExprContext().initializeAggregationValues();
		}
	}

	return true;
}

bool SQLSortOps::WindowMatch::preparePredicateReaders(
		OpContext &cxt, MatchContext &matchCxt, TupleListReader *&predReader,
		TupleListReader *&keyReader) {
	predReader = &cxt.getReader(0, IN_PREDICATE);
	keyReader = &cxt.getReader(0, IN_KEY);

	if (!predReader->exists()) {
		return false;
	}

	if (!matchCxt.keyReaderStarted_) {
		matchCxt.navReaderSet_.setBaseReader(cxt, *predReader);
		keyReader->next();
		matchCxt.keyReaderStarted_ = true;
	}

	if (!keyReader->exists() && !cxt.isAllInputCompleted()) {
		return false;
	}

	*cxt.getActiveReaderRef() = predReader;
	return true;
}

bool SQLSortOps::WindowMatch::nextPrediateRow(
		OpContext &cxt, MatchContext &matchCxt, TupleListReader &predReader,
		TupleListReader &keyReader, bool partTail) {
	predReader.next();

	if (!predReader.exists()) {
		return false;
	}

	keyReader.next();

	if (partTail) {
		matchCxt.navReaderSet_.clearPositions(cxt, predReader);
		matchCxt.predPos_ = 0;
	}
	else {
		uint64_t effectiveStartPos;
		if (matchCxt.variableTable_.resolveMatch(effectiveStartPos)) {
			matchCxt.navReaderSet_.invalidatePrevPositionRange(
					cxt, effectiveStartPos);
		}
		else {
			matchCxt.navReaderSet_.prepareNextPositions();
		}
		matchCxt.predPos_++;
	}

	matchCxt.variableTable_.nextMatchingPosition(matchCxt.predPos_);

	if (!keyReader.exists() && !cxt.isAllInputCompleted()) {
		return false;
	}

	return true;
}

bool SQLSortOps::WindowMatch::isPartitionTail(
		MatchContext &matchCxt, TupleListReader &predReader,
		TupleListReader &keyReader) {
	if (!keyReader.exists()) {
		return true;
	}

	return (matchCxt.partEq_.get() != NULL &&
			!(*matchCxt.partEq_)(
					SQLValues::TupleListReaderSource(predReader),
					SQLValues::TupleListReaderSource(keyReader)));
}

void SQLSortOps::WindowMatch::checkMemoryLimit(
		OpContext &cxt, MatchContext &matchCxt) {
	SQLValues::VarAllocator &varAlloc = cxt.getValueContext().getVarAllocator();
	const size_t usage =
			(varAlloc.getTotalElementSize() - varAlloc.getFreeElementSize());
	const size_t limit = matchCxt.memoryLimit_;
	if (usage > limit) {
		uint64_t searchingRowCount;
		uint64_t candidateCount;
		matchCxt.variableTable_.getStats(searchingRowCount, candidateCount);

		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_PATTERN_MATCH_LIMIT_EXCEEDED,
				"Pattern match memory limit exceeded ("
				"usageBytes=" << usage << ", "
				"limitBytes=" << limit << ", "
				"searchingRowCount=" << searchingRowCount << ", "
				"candidateCount=" << candidateCount << ")");
	}
}

SQLSortOps::WindowMatch::MatchContext&
SQLSortOps::WindowMatch::prepareMatchContext(OpContext &cxt) const {
	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(
				cxt.getAllocator(), MatchContext, cxt, getCode());
	}

	MatchContext &matchCxt = cxt.getResource(0).resolveAs<MatchContext>();

	cxt.getExprContext().setWindowState(
			&matchCxt.variableTable_.getWindowState());

	return matchCxt;
}


SQLSortOps::WindowMatch::MicroCompiler::MicroCompiler(
		util::StackAllocator &alloc) :
		alloc_(alloc) {
}

bool SQLSortOps::WindowMatch::MicroCompiler::isAcceptableWindowPlan(
		const OpCode &code) {
	const SQLExprs::Expression *pred = code.getFilterPredicate();
	return (findPatternOptionExpr(pred) != NULL);
}

void SQLSortOps::WindowMatch::MicroCompiler::makeWindowSubPlan(
		OpContext &cxt, const OpCode &code) {

	OpPlan &plan = cxt.getPlan();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	assert(isAcceptableWindowPlan(code));

	const SQLExprs::Expression &basePred =
			resolvePatternOptionExpr(code.getFilterPredicate());
	const NavigationProjectionRef &baseProj = makeBaseProjection(builder, code);

	OpCode subCode;
	subCode.setKeyColumnList(code.findKeyColumnList());
	subCode.setFilterPredicate(&basePred);
	subCode.setPipeProjection(&makeSubPlanProjection(
			builder, baseProj, basePred, cxt.getInputColumnList(0)));

	OpNode &node = plan.createNode(SQLOpTypes::OP_WINDOW_MATCH);
	node.setCode(subCode);
	node.addInput(plan.getParentInput(0));
	plan.linkToAllParentOutput(node);
}

const SQLExprs::Expression&
SQLSortOps::WindowMatch::MicroCompiler::predicateToPatternExpr(
		const SQLExprs::Expression *pred) {

	const SQLExprs::Expression *patternExpr;
	const SQLExprs::Expression *varListExpr;
	const SQLExprs::Expression *modeExpr;
	optionToElementExprs(
			resolvePatternOptionExpr(pred), patternExpr, varListExpr, modeExpr);

	return *patternExpr;
}

SQLSortOps::WindowMatch::VariablePredicateMap
SQLSortOps::WindowMatch::MicroCompiler::makeVariablePredicateMap(
		const SQLExprs::Expression *pred, const Projection *proj) {

	VariablePredicateMap map(alloc_);

	const SQLExprs::Expression &listExpr = predicateToVariableListExpr(pred);

	{
		int64_t varId = 0;
		for (SQLExprs::Expression::Iterator it(listExpr); it.exists(); it.next()) {
			VariablePredicateInfo &info = map.insert(std::make_pair(
					varId, VariablePredicateInfo())).first->second;
			variableToElementExprs(it.get(), &info.name_);

			varId++;
		}
	}

	const Projection &refPred = resolveMultiStageSubSubElement(proj, 0, 1, 1);
	{
		int64_t varId = -1;
		for (SQLExprs::Expression::Iterator it(refPred); it.exists(); it.next()) {
			VariablePredicateInfo &info = map.insert(std::make_pair(
					varId, VariablePredicateInfo())).first->second;
			info.expr_ = &it.get();
			varId++;
		}
	}

	return map;
}

SQLSortOps::WindowMatch::NavigationMap
SQLSortOps::WindowMatch::MicroCompiler::makeNavigationMap(
		const SQLExprs::Expression *pred, const Projection *proj) {
	NavigationMap map(alloc_);

	uint32_t ordinal = 0;
	NavigationKey key;
	NavigationProjectionRef cur;
	NavigationProjectionRef next = firstNavigationProjections(proj);

	while (nextNavigationProjections(ordinal, key, cur, next)) {
		SQLExprs::Expression::Iterator descIt(*cur.first);
		SQLExprs::Expression::Iterator assignIt(*cur.second.first);
		SQLExprs::Expression::Iterator emptyIt(*cur.second.second);

		uint32_t navIndex = 0;
		while (descIt.exists() && assignIt.exists() && emptyIt.exists()) {
			if (descIt.get().getCode().getType() != SQLType::EXPR_CONSTANT) {
				const NavigationInfo &info = resolveNavigationInfo(
						descIt.get(), assignIt.get(), emptyIt.get(), navIndex,
						key.first);
				map.insert(std::make_pair(key, info));
			}

			descIt.next();
			assignIt.next();
			emptyIt.next();
			navIndex++;
		}
		ordinal++;
	}

	setUpNavigationMapForClassifier(
			resolvePatternOptionExpr(pred), getNavigableProjections(proj), map);
	return map;
}

SQLSortOps::WindowMatch::NavigationProjectionRef
SQLSortOps::WindowMatch::MicroCompiler::getNavigableProjections(
		const Projection *proj) {

	const Projection &pipeProj = resolveMultiStageSubSubElement(proj, 0, 1, 0);

	const Projection *pipeOnceProj = &resolveMultiStageElement(&pipeProj, 0);
	const Projection *pipeEachProj = &resolveMultiStageElement(&pipeProj, 1);
	const Projection &finishProj = resolveMultiStageElement(proj, 1);

	pipeOnceProj = filterEmptyProjection(pipeOnceProj);
	pipeEachProj = filterEmptyProjection(pipeEachProj);

	return makeProjectionRefElements(pipeOnceProj, pipeEachProj, &finishProj);
}

bool SQLSortOps::WindowMatch::MicroCompiler::isAllRowsPerMatch(
		const SQLExprs::Expression *pred) {

	const SQLExprs::Expression *patternExpr;
	const SQLExprs::Expression *varListExpr;
	const SQLExprs::Expression *modeExpr;
	optionToElementExprs(
			resolvePatternOptionExpr(pred), patternExpr, varListExpr, modeExpr);

	const int64_t mode = modeExpr->getCode().getValue().get<int64_t>();
	return (mode & static_cast<int64_t>(
			SyntaxTree::PATERN_MATCH_ONE_ROW_PER_MATCH)) == 0;
}

uint32_t SQLSortOps::WindowMatch::MicroCompiler::getNavigationReaderCount(
		OpContext &cxt, const OpCode &code) {
	const uint64_t memLimit = getPatternMatchMemoryLimit(code);
	const uint32_t blockSize =
			cxt.getReader(0).getTupleList().getInfo().blockSize_;

	const uint64_t countLimit = 1024;
	return static_cast<uint32_t>(std::min<uint64_t>(
			std::max<uint64_t>(memLimit, 0) / (blockSize * 2), countLimit));
}

uint64_t SQLSortOps::WindowMatch::MicroCompiler::getPatternMatchMemoryLimit(
		const OpCode &code) {
	const OpConfig *config = code.getConfig();

	const int64_t memLimit = OpConfig::resolve(
			SQLOpTypes::CONF_PATTERN_MATCH_MEMORY_LIMIT, config);
	return static_cast<uint64_t>(std::max<int64_t>(memLimit, 0));
}

const SQLOps::Projection*
SQLSortOps::WindowMatch::MicroCompiler::getProjectionElement(
		const NavigationProjectionRef &base, size_t ordinal) {
	assert(ordinal < 3);
	return (ordinal == 0 ? base.first :
			(ordinal == 1 ? base.second.first : base.second.second));
}

const SQLOps::Projection*&
SQLSortOps::WindowMatch::MicroCompiler::getProjectionElement(
		NavigationProjectionRef &base, size_t ordinal) {
	assert(ordinal < 3);
	return (ordinal == 0 ? base.first :
			(ordinal == 1 ? base.second.first : base.second.second));
}

SQLOps::Projection*&
SQLSortOps::WindowMatch::MicroCompiler::getProjectionElement(
		NavigationProjection &base, size_t ordinal) {
	assert(ordinal < 3);
	return (ordinal == 0 ? base.first :
			(ordinal == 1 ? base.second.first : base.second.second));
}

SQLSortOps::WindowMatch::NavigationProjectionRef
SQLSortOps::WindowMatch::MicroCompiler::makeBaseProjection(
		OpCodeBuilder &builder, const OpCode &code) {
	OpCode groupedCode = code;
	groupedCode.setPipeProjection(
			&toGroupedBaseProjection(builder, *code.getPipeProjection()));

	for (bool extra = false;; extra = true) {
		OpCode subCode = groupedCode;

		if (extra) {
			Projection &proj =
					builder.rewriteProjection(*subCode.getPipeProjection());
			setUpBaseProjectionExtra(builder, proj, true, true);
			subCode.setPipeProjection(&proj);
		}

		const SQLOpUtils::ProjectionPair &projPair =
				builder.arrangeProjections(subCode, false, true, NULL);

		if (projPair.second == NULL) {
			if (extra) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}
			continue;
		}

		if (extra) {
			setUpBaseProjectionExtra(builder, *projPair.first, false, true);
			setUpBaseProjectionExtra(builder, *projPair.second, false, false);
		}

		const SQLOpUtils::ProjectionPair &pulledUp =
				pullUpBaseProjections(builder, projPair);

		const Projection &splittedOnce =
				splitBasePipeProjection(builder, *pulledUp.first, true);
		const Projection &splittedEach =
				splitBasePipeProjection(builder, *pulledUp.first, false);

		return makeProjectionRefElements(
				&splittedOnce, &splittedEach, pulledUp.second);
	}
}

void SQLSortOps::WindowMatch::MicroCompiler::setUpBaseProjectionExtra(
		OpCodeBuilder &builder, Projection &proj, bool pre, bool forPipe) {

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	if (pre) {
		SQLExprs::Expression::ModIterator it(proj);

		SQLExprs::ExprCode code;
		code.setType(SQLType::AGG_COUNT_ALL);
		code.setColumnType(TupleTypes::TYPE_LONG);

		for (; it.exists(); it.next()) {
		}
		it.append(factoryCxt.getFactory().create(factoryCxt, code));
	}
	else if (forPipe) {
		assert(proj.getChildCount() == 1);
		{
			SQLExprs::Expression::ModIterator it(proj);
			while (it.exists()) {
				it.remove();
			}
		}
		arrangeForEmptyProjection(factoryCxt, proj);
	}
	else {
		const size_t count = proj.getChildCount();

		SQLExprs::Expression::ModIterator it(proj);
		for (size_t i = 1;; i++) {
			if (i >= count) {
				it.remove();
				break;
			}
			it.next();
		}
		assert(proj.getChildCount() == count - 1);
	}
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::toGroupedBaseProjection(
		OpCodeBuilder &builder, const Projection &src) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	Projection &dest = builder.rewriteProjection(src);
	arrangeForGroupedBaseExpr(factoryCxt, dest);
	return dest;
}

SQLOpUtils::ProjectionPair
SQLSortOps::WindowMatch::MicroCompiler::pullUpBaseProjections(
		OpCodeBuilder &builder, const SQLOpUtils::ProjectionPair &src) {
	BaseExprMap map(alloc_);
	makePullUpTargetMap(*src.first, map);

	const SQLOpUtils::ProjectionPair dest(
			&builder.rewriteProjection(*src.first),
			&builder.rewriteProjection(*src.second));
	arrangeForPulledUpBaseExpr(builder, *dest.first, true, map);
	arrangeForPulledUpBaseExpr(builder, *dest.second, false, map);
	return dest;
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::splitBasePipeProjection(
		OpCodeBuilder &builder, const Projection &src, bool once) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	Projection &dest = builder.rewriteProjection(src);
	arrangeForSplittedBaseExpr(dest, once);
	arrangeForEmptyProjection(factoryCxt, dest);
	return dest;
}

void SQLSortOps::WindowMatch::MicroCompiler::arrangeForGroupedBaseExpr(
		SQLExprs::ExprFactoryContext &factoryCxt, SQLExprs::Expression &expr) {
	for (SQLExprs::Expression::ModIterator it(expr); it.exists(); it.next()) {
		const SQLExprs::ExprType type = it.get().getCode().getType();
		if (SQLExprs::ExprTypeUtils::isAggregation(type)) {
			continue;
		}

		if (isNavigationColumn(type) || type == SQLType::EXPR_ID) {
			SQLExprs::Expression &navExpr = it.get();
			it.remove();

			SQLExprs::ExprCode code;
			code.setType(SQLType::AGG_LAST);
			code.setColumnType(navExpr.getCode().getColumnType());
			SQLExprs::Expression &sub =
					factoryCxt.getFactory().create(factoryCxt, code);

			SQLExprs::Expression::ModIterator subIt(sub);
			subIt.append(navExpr);

			it.insert(sub);
		}
		else {
			arrangeForGroupedBaseExpr(factoryCxt, it.get());
		}
	}
}

void SQLSortOps::WindowMatch::MicroCompiler::makePullUpTargetMap(
		const SQLExprs::Expression &expr, BaseExprMap &map) {
	for (SQLExprs::Expression::Iterator it(expr); it.exists(); it.next()) {
		const SQLExprs::ExprType type = it.get().getCode().getType();

		if (type != SQLType::AGG_LAST) {
			makePullUpTargetMap(it.get(), map);
			continue;
		}

		const SQLExprs::Expression *sub = it.get().findChild();
		if (isEachNavigationExpr(sub) || type == SQLType::EXPR_ID) {
			map.insert(std::make_pair(
					it.get().getCode().getAggregationIndex(), sub));
		}
	}
}

void SQLSortOps::WindowMatch::MicroCompiler::arrangeForPulledUpBaseExpr(
		OpCodeBuilder &builder, SQLExprs::Expression &expr, bool forPipe,
		const BaseExprMap &map) {
	for (SQLExprs::Expression::ModIterator it(expr); it.exists();) {
		const SQLExprs::ExprType type = it.get().getCode().getType();

		if (type != (forPipe ? SQLType::AGG_LAST : SQLType::AGG_FIRST)) {
			if (!forPipe) {
				arrangeForPulledUpBaseExpr(builder, it.get(), forPipe, map);
			}
			it.next();
			continue;
		}

		BaseExprMap::const_iterator mapIt =
				map.find(it.get().getCode().getAggregationIndex());
		if (mapIt != map.end()) {
			it.remove();
			if (forPipe) {
				if (!it.exists()) {
					break;
				}
			}
			else {
				SQLExprs::ExprFactoryContext &factoryCxt =
						builder.getExprFactoryContext();
				SQLExprs::Expression &pulledUp =
						builder.getExprRewriter().rewrite(
								factoryCxt, *mapIt->second, NULL);
				it.insert(pulledUp);
				it.next();
			}
		}
		else {
			it.next();
		}
	}
}

void SQLSortOps::WindowMatch::MicroCompiler::arrangeForSplittedBaseExpr(
		SQLExprs::Expression &expr, bool once) {
	for (SQLExprs::Expression::ModIterator it(expr); it.exists();) {
		const bool forOnceExpr = isPipeOnceBaseExpr(it.get());
		if (!once != !forOnceExpr) {
			it.remove();
			if (!it.exists()) {
				break;
			}
		}
		else {
			it.next();
		}
	}
}

bool SQLSortOps::WindowMatch::MicroCompiler::isEachNavigationExpr(
		const SQLExprs::Expression *expr) {
	if (expr == NULL) {
		return false;
	}

	const SQLExprs::ExprType type = expr->getCode().getType();
	return (isNavigationColumn(type) && !isNavigationByMatch(type));
}

bool SQLSortOps::WindowMatch::MicroCompiler::isPipeOnceBaseExpr(
		const SQLExprs::Expression &expr) {
	const SQLExprs::ExprType type = expr.getCode().getType();
	if (isNavigationByMatch(type)) {
		return true;
	}

	for (SQLExprs::Expression::Iterator it(expr); it.exists(); it.next()) {
		if (isPipeOnceBaseExpr(it.get())) {
			return true;
		}
	}

	return false;
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::makeSubPlanProjection(
		OpCodeBuilder &builder, const NavigationProjectionRef &baseProj,
		const SQLExprs::Expression &basePred,
		const SQLValues::TupleColumnList &inColumnList) {

	const bool forPipe = false;
	const util::Vector<TupleColumnType> *aggrTypeList;

	const PlanNavigationMap &navMap = makePlanNavigationMap(
			baseProj, basePred, inColumnList, aggrTypeList);
	const NavigationProjectionRef &navBaseProj =
			makeNavigableBaseProjection(builder, baseProj, aggrTypeList);

	Projection &totalPipeProj = makeTotalPipeProjection(
			builder, navMap, navBaseProj, basePred);
	Projection &finishPefProj = makeRefProjection(
			builder, navMap, navBaseProj, forPipe, false);

	return builder.createMultiStageProjection(totalPipeProj, finishPefProj);
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::makeTotalPipeProjection(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const NavigationProjectionRef &baseProj,
		const SQLExprs::Expression &basePred) {

	const bool forPipe = true;
	const VariableExprMap &varMap = makeVariableExprMap(builder, basePred);

	Projection &pipeOnceRefProj = makeRefProjection(
			builder, navMap, baseProj, forPipe, true);
	Projection &pipeEachRefProj = makeRefProjection(
			builder, navMap, baseProj, forPipe, false);

	Projection &pipeRefProj = builder.createMultiStageProjection(
			pipeOnceRefProj, pipeEachRefProj);
	Projection &matchingRefProj = makeMatchingProjection(
			builder, navMap, baseProj, varMap);

	Projection &navProj = makeNavigationProjection(
			builder, navMap, baseProj, varMap);
	Projection &refProj =
			builder.createMultiStageProjection(pipeRefProj, matchingRefProj);

	return builder.createMultiStageProjection(navProj, refProj);
}

SQLOps::Projection& SQLSortOps::WindowMatch::MicroCompiler::makeRefProjection(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const NavigationProjectionRef &baseProj, bool forPipe, bool once) {

	Projection &proj = builder.rewriteProjection(*getProjectionElement(
			baseProj, (forPipe ? (once ? 0 : 1) : 2)));
	const NavigationKey navKey(
			(forPipe ? (once ?
					NAV_PROJECTION_PIPE_ONCE : NAV_PROJECTION_PIPE_EACH) :
					NAV_PROJECTION_FINISH), -1);
	setUpArrangedNavigationExpr(builder, navMap, navKey, proj);

	return proj;
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::makeMatchingProjection(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const NavigationProjectionRef &baseProj,
		const VariableExprMap &varMap) {

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	const SQLOps::ProjectionCode &aggrFinishCode =
			getProjectionElement(baseProj, 2)->getProjectionCode();
	Projection &proj = builder.createProjection(
			SQLOpTypes::PROJ_AGGR_PIPE, aggrFinishCode);

	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
	factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);

	SQLExprs::Expression::ModIterator projIt(proj);
	for (VariableExprMap::const_iterator it = varMap.begin();
			it != varMap.end(); ++it) {
		const SQLExprs::Expression *base = it->second;

		SQLExprs::Expression &arranged =
				builder.getExprRewriter().rewrite(factoryCxt, *base, NULL);

		const NavigationKey navKey(NAV_MATCHING, it->first);
		setUpArrangedNavigationExpr(builder, navMap, navKey, arranged);
		projIt.append(arranged);
	}

	return proj;
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::makeNavigationProjection(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const NavigationProjectionRef &baseProj,
		const VariableExprMap &varMap) {

	const SQLOps::ProjectionCode baseProjCode =
			getProjectionElement(baseProj, 1)->getProjectionCode();

	Projection &originalProj = makeNavigationProjectionSub(
			builder, navMap, baseProjCode, varMap, true, false);
	Projection &assignProj = makeNavigationProjectionSub(
			builder, navMap, baseProjCode, varMap, false, false);
	Projection &emptyProj = makeNavigationProjectionSub(
			builder, navMap, baseProjCode, varMap, false, true);

	Projection &navigableProj =
			builder.createMultiStageProjection(assignProj, emptyProj);
	return builder.createMultiStageProjection(originalProj, navigableProj);
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::makeNavigationProjectionSub(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const SQLOps::ProjectionCode &baseProjCode,
		const VariableExprMap &varMap, bool original, bool empty) {

	Projection &pipeOnceProj = makeNavigationProjectionSubByKey(
			builder, navMap, baseProjCode, original, empty,
			NavigationKey(NAV_PROJECTION_PIPE_ONCE, -1));
	Projection &pipeEachProj = makeNavigationProjectionSubByKey(
			builder, navMap, baseProjCode, original, empty,
			NavigationKey(NAV_PROJECTION_PIPE_EACH, -1));
	Projection &finishProj = makeNavigationProjectionSubByKey(
			builder, navMap, baseProjCode, original, empty,
			NavigationKey(NAV_PROJECTION_FINISH, -1));

	Projection &pipeProj =
			builder.createMultiStageProjection(pipeOnceProj, pipeEachProj);
	Projection &refProj =
			builder.createMultiStageProjection(pipeProj, finishProj);

	Projection *matchingProj = NULL;
	for (VariableExprMap::const_iterator it = varMap.end();
			it != varMap.begin();) {
		const NavigationKey key(NAV_MATCHING, (--it)->first);
		Projection *proj = &makeNavigationProjectionSubByKey(
				builder, navMap, baseProjCode, original, empty, key);
		if (matchingProj != NULL) {
			proj = &builder.createMultiStageProjection(*proj, *matchingProj);
		}
		matchingProj = proj;
	}

	assert(matchingProj != NULL);
	return builder.createMultiStageProjection(refProj, *matchingProj);
}

SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::makeNavigationProjectionSubByKey(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const SQLOps::ProjectionCode &baseProjCode, bool original,
		bool empty, const NavigationKey &navKey) {

	Projection &proj =
			builder.createProjection(baseProjCode.getType(), baseProjCode);
	SQLExprs::Expression::ModIterator projIt(proj);

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
	factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);

	const PlanNavigationKey planKey(navKey, navigationColumnTopKey());
	for (PlanNavigationMap::const_iterator it = navMap.lower_bound(planKey);
			it != navMap.end() && it->first.first == navKey; ++it) {
		const SQLExprs::Expression *baseExpr = it->second.first;
		SQLExprs::Expression *expr;
		if (original) {
			expr = &builder.getExprRewriter().rewrite(
					factoryCxt, *baseExpr, NULL);
		}
		else {
			{
				SQLExprs::ExprCode code;
				code.setType(SQLType::AGG_LAST);
				code.setColumnType(baseExpr->getCode().getColumnType());
				code.setAttributes(
						SQLExprs::ExprCode::ATTR_ASSIGNED |
						SQLExprs::ExprCode::ATTR_AGGR_ARRANGED);
				code.setAggregationIndex(it->second.second);
				expr = &factoryCxt.getFactory().create(factoryCxt, code);
			}

			SQLExprs::Expression *subExpr;
			if (empty) {
				subExpr = &SQLExprs::ExprRewriter::createEmptyConstExpr(
						factoryCxt, baseExpr->getCode().getColumnType());
			}
			else {
				const SQLExprs::Expression &subBaseExpr =
						baseExpr->child().next();
				if (subBaseExpr.getCode().getType() != SQLType::EXPR_COLUMN) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
				}

				subExpr = &builder.getExprRewriter().rewrite(
						factoryCxt, subBaseExpr, NULL);
			}

			SQLExprs::Expression::ModIterator exprIt(*expr);
			exprIt.append(*subExpr);
		}
		projIt.append(*expr);
	}

	arrangeForEmptyProjection(factoryCxt, proj);
	return proj;
}

void SQLSortOps::WindowMatch::MicroCompiler::setUpArrangedNavigationExpr(
		OpCodeBuilder &builder, const PlanNavigationMap &navMap,
		const NavigationKey &navKey, SQLExprs::Expression &expr) {

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	for (SQLExprs::Expression::ModIterator it(expr); it.exists(); it.next()) {
		PlanNavigationColumnKey columnKey;
		if (!findNagvigableColumnKey(it.get(), columnKey)) {
			setUpArrangedNavigationExpr(builder, navMap, navKey, it.get());
			continue;
		}

		PlanNavigationMap::const_iterator navIt =
				navMap.find(PlanNavigationKey(navKey, columnKey));
		if (navIt == navMap.end()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		uint32_t attributes = (
				SQLExprs::ExprCode::ATTR_ASSIGNED |
				SQLExprs::ExprCode::ATTR_AGGR_ARRANGED);
		if (navKey.first != NAV_PROJECTION_FINISH) {
			attributes |= SQLExprs::ExprCode::ATTR_AGGR_FINISH_ALWAYS;
		}

		SQLExprs::ExprCode code;
		code.setType(SQLType::AGG_FIRST);
		code.setColumnType(it.get().getCode().getColumnType());
		code.setAttributes(attributes);
		code.setAggregationIndex(navIt->second.second);

		SQLExprs::Expression &subExpr =
				factoryCxt.getFactory().create(factoryCxt, code);

		it.remove();
		it.insert(subExpr);
	}
}

SQLSortOps::WindowMatch::VariableExprMap
SQLSortOps::WindowMatch::MicroCompiler::makeVariableExprMap(
		OpCodeBuilder &builder, const SQLExprs::Expression &basePred) {

	SQLExprs::ExprFactoryContext &factoryCxt =
			builder.getExprFactoryContext();

	int64_t varId = -1;
	VariableExprMap map(alloc_);
	{
		const SQLExprs::Expression &expr =
				SQLExprs::ExprRewriter::createEmptyConstExpr(
						factoryCxt, TupleTypes::TYPE_ANY);
		map.insert(std::make_pair(varId, &expr));
	}

	const SQLExprs::Expression &varList =
			predicateToVariableListExpr(&basePred);
	for (SQLExprs::Expression::Iterator it(varList); it.exists(); it.next()) {
		map.insert(std::make_pair(
				++varId, &variableToElementExprs(it.get(), NULL)));
	}

	return map;
}

SQLSortOps::WindowMatch::PlanNavigationMap
SQLSortOps::WindowMatch::MicroCompiler::makePlanNavigationMap(
		const NavigationProjectionRef &baseProj,
		const SQLExprs::Expression &basePred,
		const SQLValues::TupleColumnList &inColumnList,
		const util::Vector<TupleColumnType> *&aggrTypeList) {

	const PlanNavigationExprMap navExprMap =
			makePlanNavigationExprMap(baseProj, basePred);
	const PlanTypedCountMap typeCounts =
			makePlanNavigationTypeCounts(navExprMap, inColumnList);

	const util::Vector<TupleColumnType> *baseAggrTypeList =
			baseProj.first->getProjectionCode().getColumnTypeList();

	aggrTypeList = ALLOC_NEW(alloc_) util::Vector<TupleColumnType>(
			makeAggregationTypeList(*baseAggrTypeList, typeCounts));
	return makePlanNavigationMapByCounts(
			navExprMap, typeCounts, *baseAggrTypeList, inColumnList);
}

SQLSortOps::WindowMatch::PlanNavigationExprMap
SQLSortOps::WindowMatch::MicroCompiler::makePlanNavigationExprMap(
		const NavigationProjectionRef &baseProj,
		const SQLExprs::Expression &basePred) {

	PlanNavigationExprMap navExprMap(alloc_);
	{
		int64_t varId = 0;
		const SQLExprs::Expression &varList =
				predicateToVariableListExpr(&basePred);
		for (SQLExprs::Expression::Iterator it(varList); it.exists(); it.next()) {
			const SQLExprs::Expression &varPred =
					variableToElementExprs(it.get(), NULL);
			setUpPlanNavigationExprMap(
					NavigationKey(NAV_MATCHING, varId), varPred, navExprMap);
			varId++;
		}
	}

	setUpPlanNavigationExprMap(
			NavigationKey(NAV_PROJECTION_PIPE_ONCE, -1),
			*getProjectionElement(baseProj, 0), navExprMap);
	setUpPlanNavigationExprMap(
			NavigationKey(NAV_PROJECTION_PIPE_EACH, -1),
			*getProjectionElement(baseProj, 1), navExprMap);
	setUpPlanNavigationExprMap(
			NavigationKey(NAV_PROJECTION_FINISH, -1),
			*getProjectionElement(baseProj, 2), navExprMap);

	return navExprMap;
}

void SQLSortOps::WindowMatch::MicroCompiler::setUpPlanNavigationExprMap(
		const NavigationKey &navKey, const SQLExprs::Expression &expr,
		PlanNavigationExprMap &navExprMap) {

	PlanNavigationColumnKey columnKey;
	if (findNagvigableColumnKey(expr, columnKey)) {
		navExprMap.insert(
				std::make_pair(PlanNavigationKey(navKey, columnKey), &expr));
		return;
	}

	for (SQLExprs::Expression::Iterator it(expr); it.exists(); it.next()) {
		setUpPlanNavigationExprMap(navKey, it.get(), navExprMap);
	}
}

SQLSortOps::WindowMatch::PlanTypedCountMap
SQLSortOps::WindowMatch::MicroCompiler::makePlanNavigationTypeCounts(
		const PlanNavigationExprMap &navExprMap,
		const SQLValues::TupleColumnList &inColumnList) {

	PlanTypedCountMap totalMap(alloc_);
	PlanTypedCountMap subMap(alloc_);

	PlanNavigationExprMap::const_iterator prevIt = navExprMap.end();
	for (PlanNavigationExprMap::const_iterator it = navExprMap.begin();; ++it) {
		if (it == navExprMap.end() || (prevIt != navExprMap.end() &&
				it->first.first != prevIt->first.first)) {
			for (PlanTypedCountMap::iterator subIt = subMap.begin();
					subIt != subMap.end(); ++subIt) {
				uint32_t &total = totalMap[subIt->first];
				uint32_t &sub = subIt->second;

				total = std::max<uint32_t>(total, sub);
				sub = 0;
			}
			if (it == navExprMap.end()) {
				break;
			}
		}

		const TupleColumnType type =
				getNavigationBaseColumnType(inColumnList, it->first);
		subMap[type]++;
		prevIt = it;
	}

	return totalMap;
}

SQLSortOps::WindowMatch::PlanNavigationMap
SQLSortOps::WindowMatch::MicroCompiler::makePlanNavigationMapByCounts(
		const PlanNavigationExprMap &navExprMap,
		const PlanTypedCountMap &typeCounts,
		const util::Vector<TupleColumnType> &baseAggrTypeList,
		const SQLValues::TupleColumnList &inColumnList) {

	PlanTypedCountMap offsetMap(alloc_);
	uint32_t nextOffset = static_cast<uint32_t>(baseAggrTypeList.size());
	for (PlanTypedCountMap::const_iterator it = typeCounts.begin();
			it != typeCounts.end(); ++it) {
		offsetMap.insert(std::make_pair(it->first, nextOffset));
		nextOffset += it->second;
	}

	PlanNavigationMap navMap(alloc_);
	PlanTypedCountMap indexMap(alloc_);

	PlanNavigationExprMap::const_iterator prevIt = navExprMap.end();
	for (PlanNavigationExprMap::const_iterator it = navExprMap.begin();
			it != navExprMap.end(); ++it) {
		if (prevIt == navExprMap.end() ||
				it->first.first != prevIt->first.first) {
			indexMap = offsetMap;
		}

		const TupleColumnType type =
				getNavigationBaseColumnType(inColumnList, it->first);
		uint32_t &navIndex = indexMap[type];

		const PlanNavigationValue value(it->second, navIndex);
		navMap.insert(std::make_pair(it->first, value));
		navIndex++;
		prevIt = it;
	}

	return navMap;
}

TupleColumnType
SQLSortOps::WindowMatch::MicroCompiler::getNavigationBaseColumnType(
		const SQLValues::TupleColumnList &inColumnList,
		const PlanNavigationKey &key) {
	const PlanNavigationColumnKey &columnKey = key.second;
	const uint32_t column = columnKey.first.second;
	if (column >= inColumnList.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return SQLValues::TypeUtils::toNonNullable(inColumnList[column].type_);
}

util::Vector<TupleColumnType>
SQLSortOps::WindowMatch::MicroCompiler::makeAggregationTypeList(
		const util::Vector<TupleColumnType> &baseAggrTypeList,
		const PlanTypedCountMap &typeCounts) {

	util::Vector<TupleColumnType> dest(alloc_);
	dest = baseAggrTypeList;

	for (PlanTypedCountMap::const_iterator it = typeCounts.begin();
			it != typeCounts.end(); ++it) {
		const TupleColumnType type =
				SQLValues::TypeUtils::toNullable(it->first);
		const size_t count = it->second;
		dest.insert(dest.end(), count, type);
	}

	return dest;
}

SQLSortOps::WindowMatch::NavigationProjectionRef
SQLSortOps::WindowMatch::MicroCompiler::makeNavigableBaseProjection(
		OpCodeBuilder &builder, const NavigationProjectionRef &src,
		const util::Vector<TupleColumnType> *aggrTypeList) {

	NavigationProjection dest;
	for (size_t i = 0; i < 3; i++) {
		const Projection *srcProj = getProjectionElement(src, i);
		Projection *&destProj = getProjectionElement(dest, i);

		destProj = &builder.rewriteProjection(*srcProj);
		destProj->getProjectionCode().setColumnTypeList(aggrTypeList);
	}

	return makeProjectionRefElements(
			getProjectionElement(dest, 0),
			getProjectionElement(dest, 1),
			getProjectionElement(dest, 2));
}

SQLSortOps::WindowMatch::PlanNavigationColumnKey
SQLSortOps::WindowMatch::MicroCompiler::navigationColumnTopKey() {
	return PlanNavigationColumnKey(PlanColumnKey(-1, 0), SQLType::START_EXPR);
}

void SQLSortOps::WindowMatch::MicroCompiler::setUpNavigationMapForClassifier(
		const SQLExprs::Expression &basePred,
		const NavigationProjectionRef &navProjs, NavigationMap &navMap) {
	if (!findClassifier(navProjs)) {
		return;
	}

	const NavigationKey navKey(NAV_CLASSIFIER, -1);
	const int64_t varCount = static_cast<int64_t>(
			predicateToVariableListExpr(&basePred).getChildCount());
	for (int64_t i = 0; i < varCount; i++) {
		NavigationInfo info;
		info.variableId_ = i;
		navMap.insert(std::make_pair(navKey, info));
	}
}

bool SQLSortOps::WindowMatch::MicroCompiler::findClassifier(
		const NavigationProjectionRef &navProjs) {
	return (
			findClassifierSub(getProjectionElement(navProjs, 0)) ||
			findClassifierSub(getProjectionElement(navProjs, 1)) ||
			findClassifierSub(getProjectionElement(navProjs, 2)));
}

bool SQLSortOps::WindowMatch::MicroCompiler::findClassifierSub(
		const SQLExprs::Expression *expr) {
	if (expr == NULL) {
		return false;
	}

	if (expr->getCode().getType() == SQLType::FUNC_CLASSIFIER) {
		return true;
	}

	for (SQLExprs::Expression::Iterator it(*expr); it.exists(); it.next()) {
		if (findClassifierSub(&it.get())) {
			return true;
		}
	}

	return false;
}

SQLSortOps::WindowMatch::NavigationInfo
SQLSortOps::WindowMatch::MicroCompiler::resolveNavigationInfo(
		const SQLExprs::Expression &descExpr,
		const SQLExprs::Expression &assignExpr,
		const SQLExprs::Expression &emptyExpr, uint32_t navIndex,
		NavigationType type) {

	PlanNavigationColumnKey columnKey;
	if (!findNagvigableColumnKey(descExpr, columnKey)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	NavigationInfo info;
	info.navigationIndex_ = navIndex;
	info.variableId_ = columnKey.first.first;
	info.direction_ = getNavigationDirection(columnKey.second);
	info.byMatch_ = isNavigationByMatch(columnKey.second);
	info.assignmentExpr_ = &assignExpr;
	info.emptyExpr_ = &emptyExpr;

	if (type != NAV_PROJECTION_PIPE_EACH &&
			info.variableId_ >= 0 && info.direction_ == 0 && !info.byMatch_) {
		info.direction_ = 1;
		info.byMatch_ = true;
	}

	return info;
}

SQLSortOps::WindowMatch::NavigationProjectionRef
SQLSortOps::WindowMatch::MicroCompiler::firstNavigationProjections(
		const Projection *proj) {

	const Projection &base = resolveMultiStageSubElement(proj, 0, 0);

	const Projection &desc = resolveMultiStageElement(&base, 0);
	const Projection &nav = resolveMultiStageElement(&base, 1);

	const Projection &assign = resolveMultiStageElement(&nav, 0);
	const Projection &empty = resolveMultiStageElement(&nav, 1);

	return NavigationProjectionRef(
			&desc, SQLOpUtils::ProjectionRefPair(&assign, &empty));
}

bool SQLSortOps::WindowMatch::MicroCompiler::nextNavigationProjections(
		uint32_t ordinal, NavigationKey &key, NavigationProjectionRef &cur,
		NavigationProjectionRef &next) {
	const size_t navElemCount = 3;
	const uint32_t matchingTopOrdinal = 3;

	if (ordinal < matchingTopOrdinal) {
		const NavigationType type = (ordinal <= 1 ?
				(ordinal == 0 ?
						NAV_PROJECTION_PIPE_ONCE : NAV_PROJECTION_PIPE_EACH) :
				NAV_PROJECTION_FINISH);
		key = NavigationKey(type, -1);

		for (size_t i = 0; i < navElemCount; i++) {
			const Projection *src = getProjectionElement(next, i);
			const Projection *&dest = getProjectionElement(cur, i);

			dest = (ordinal <= 1 ?
					&resolveMultiStageSubSubElement(src, 0, 0, ordinal) :
					&resolveMultiStageSubElement(src, 0, 1));
			checkNavigationProjection(dest);
		}
	}
	else {
		const bool top = (ordinal <= matchingTopOrdinal);
		const int64_t varId =
				static_cast<int64_t>(ordinal - matchingTopOrdinal) - 1;
		key = NavigationKey(NAV_MATCHING, varId);

		cur = makeProjectionRefElements(
				nextMatchingProjection(getProjectionElement(next, 0), top),
				nextMatchingProjection(getProjectionElement(next, 1), top),
				nextMatchingProjection(getProjectionElement(next, 2), top));

		if (getProjectionElement(cur, 0) == NULL ||
				getProjectionElement(cur, 1) == NULL ||
				getProjectionElement(cur, 2) == NULL) {
			cur = NavigationProjectionRef();
			return false;
		}
	}
	return true;
}

const SQLOps::Projection*
SQLSortOps::WindowMatch::MicroCompiler::nextMatchingProjection(
		const Projection *&next, bool top) {
	const Projection *base = (top ? &resolveMultiStageElement(next, 1) : next);

	const Projection *cur;
	if (isMultiStageProjection(base)) {
		cur = &resolveMultiStageElement(base, 0);
		next = &resolveMultiStageElement(base, 1);
		checkNavigationProjection(cur);
	}
	else {
		cur = base;
		next = NULL;
	}
	return cur;
}

void SQLSortOps::WindowMatch::MicroCompiler::checkNavigationProjection(
		const Projection *proj) {
	if (proj == NULL || proj->getProjectionCode().getType() !=
			SQLOpTypes::PROJ_AGGR_PIPE) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

const SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::resolveMultiStageElement(
		const Projection *proj, size_t ordinal) {

	if (!isMultiStageProjection(proj) || ordinal >= proj->getChainCount()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return proj->chainAt(ordinal);
}

const SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::resolveMultiStageSubElement(
		const Projection *proj, size_t ordinal1, size_t ordinal2) {
	return resolveMultiStageElement(
			&resolveMultiStageElement(proj, ordinal1), ordinal2);
}

const SQLOps::Projection&
SQLSortOps::WindowMatch::MicroCompiler::resolveMultiStageSubSubElement(
		const Projection *proj, size_t ordinal1, size_t ordinal2,
		size_t ordinal3) {
	return resolveMultiStageSubElement(
			&resolveMultiStageElement(proj, ordinal1), ordinal2, ordinal3);
}

bool SQLSortOps::WindowMatch::MicroCompiler::isMultiStageProjection(
		const Projection *proj) {
	return (proj != NULL && proj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);
}

const SQLOps::Projection*
SQLSortOps::WindowMatch::MicroCompiler::filterEmptyProjection(
		const Projection *proj) {
	if (proj == NULL || (
			proj->getChildCount() <= 1 &&
			proj->findChild() != NULL &&
			proj->child().getCode().getType() == SQLType::EXPR_CONSTANT)) {
		return NULL;
	}

	return proj;
}

void SQLSortOps::WindowMatch::MicroCompiler::arrangeForEmptyProjection(
		SQLExprs::ExprFactoryContext &factoryCxt, Projection &proj) {
	if (proj.findChild() == NULL) {
		SQLExprs::Expression::ModIterator projIt(proj);
		projIt.append(SQLExprs::ExprRewriter::createEmptyConstExpr(
				factoryCxt, TupleTypes::TYPE_ANY));
	}
}

SQLSortOps::WindowMatch::NavigationProjectionRef
SQLSortOps::WindowMatch::MicroCompiler::makeProjectionRefElements(
		const Projection *proj1, const Projection *proj2,
		const Projection *proj3) {
	return NavigationProjectionRef(
			proj1, SQLOpUtils::ProjectionRefPair(proj2, proj3));
}

bool SQLSortOps::WindowMatch::MicroCompiler::findNagvigableColumnKey(
		const SQLExprs::Expression &expr,
		PlanNavigationColumnKey &columnKey) {

	columnKey = navigationColumnTopKey();

	const SQLType::Id type = expr.getCode().getType();
	if (!isNavigationColumn(type)) {
		return false;
	}

	const SQLExprs::Expression &varIdExpr = expr.child();
	const SQLExprs::Expression &columnExpr = varIdExpr.next();
	if (columnExpr.getCode().getType() != SQLType::EXPR_COLUMN) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int64_t refVarId = varIdExpr.getCode().getValue().get<int64_t>();
	if (refVarId < -1) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const uint32_t column = columnExpr.getCode().getColumnPos();
	columnKey = PlanNavigationColumnKey(PlanColumnKey(refVarId, column), type);

	return true;
}

const SQLExprs::Expression&
SQLSortOps::WindowMatch::MicroCompiler::predicateToVariableListExpr(
		const SQLExprs::Expression *base) {

	const SQLExprs::Expression *patternExpr;
	const SQLExprs::Expression *varListExpr;
	const SQLExprs::Expression *modeExpr;
	optionToElementExprs(
			resolvePatternOptionExpr(base),
			patternExpr, varListExpr, modeExpr);

	return *varListExpr;
}

const SQLExprs::Expression&
SQLSortOps::WindowMatch::MicroCompiler::variableToElementExprs(
		const SQLExprs::Expression &base, const TupleValue **name) {
	if (base.getCode().getType() != SQLType::EXPR_PATTERN_VARIABLE_DEF) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	const SQLExprs::Expression &nameExpr = base.child();
	const SQLExprs::Expression &predExpr = nameExpr.next();

	if (name != NULL) {
		*name = &nameExpr.getCode().getValue();
	}
	return predExpr;
}

void SQLSortOps::WindowMatch::MicroCompiler::optionToElementExprs(
		const SQLExprs::Expression &base,
		const SQLExprs::Expression *&patternExpr,
		const SQLExprs::Expression *&varListExpr,
		const SQLExprs::Expression *&modeExpr) {
	if (base.getCode().getType() != SQLType::EXPR_PATTERN_OPTION) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	patternExpr = &base.child();
	varListExpr = &patternExpr->next();
	modeExpr = &varListExpr->next();
}

const SQLExprs::Expression&
SQLSortOps::WindowMatch::MicroCompiler::resolvePatternOptionExpr(
		const SQLExprs::Expression *base) {
	const SQLExprs::Expression *optionExpr = findPatternOptionExpr(base);

	if (optionExpr == NULL) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	return *optionExpr;
}

const SQLExprs::Expression*
SQLSortOps::WindowMatch::MicroCompiler::findPatternOptionExpr(
		const SQLExprs::Expression *base) {
	if (base == NULL ||
			base->getCode().getType() != SQLType::EXPR_PATTERN_OPTION) {
		return NULL;
	}

	return base;
}

int32_t SQLSortOps::WindowMatch::MicroCompiler::getNavigationDirection(
		SQLType::Id exprType) {
	switch (exprType) {
	case SQLType::EXPR_PATTERN_PREV:
		return -1;
	case SQLType::EXPR_PATTERN_NEXT:
		return 1;
	case SQLType::EXPR_PATTERN_FIRST:
		return -1;
	case SQLType::EXPR_PATTERN_LAST:
		return 1;
	default:
		assert(exprType == SQLType::EXPR_PATTERN_CURRENT);
		return 0;
	}
}

bool SQLSortOps::WindowMatch::MicroCompiler::isNavigationByMatch(
		SQLType::Id exprType) {
	switch (exprType) {
	case SQLType::EXPR_PATTERN_FIRST:
	case SQLType::EXPR_PATTERN_LAST:
		return true;
	default:
		return false;
	}
}

bool SQLSortOps::WindowMatch::MicroCompiler::isNavigationColumn(
		SQLType::Id exprType) {
	switch (exprType) {
	case SQLType::EXPR_PATTERN_CURRENT:
	case SQLType::EXPR_PATTERN_PREV:
	case SQLType::EXPR_PATTERN_NEXT:
	case SQLType::EXPR_PATTERN_FIRST:
	case SQLType::EXPR_PATTERN_LAST:
		return true;
	default:
		return false;
	}
}


SQLSortOps::WindowMatch::VariablePredicateInfo::VariablePredicateInfo() :
		predicateIndex_(0),
		name_(NULL),
		expr_(NULL) {
}


SQLSortOps::WindowMatch::NavigationInfo::NavigationInfo() :
		navigationIndex_(0),
		variableId_(0),
		direction_(0),
		byMatch_(false),
		assignmentExpr_(NULL),
		emptyExpr_(NULL) {
}


SQLSortOps::WindowMatch::VariableGroupEntry::VariableGroupEntry() :
		beginPos_(0),
		endPos_(0) {
}


SQLSortOps::WindowMatch::MatchEntry::MatchEntry(
		SQLValues::VarAllocator &varAlloc) :
		stack_(varAlloc),
		history_(varAlloc),
		groupMap_(varAlloc),
		searchRange_(VariableGroupEntry()),
		firstPos_(0),
		matchingVariableId_(-1) {
}


SQLSortOps::WindowMatch::MatchStackElement::MatchStackElement(
		uint32_t patternId, bool withTailAnchor) :
		hash_(0),
		patternId_(patternId),
		repeat_(0),
		withTailAnchor_(withTailAnchor) {
}


SQLSortOps::WindowMatch::VariableHistoryElement::VariableHistoryElement() :
		variableId_(-1),
		beginPos_(0),
		endPos_(0) {
}


SQLSortOps::WindowMatch::MatchState::MatchState(
		SQLValues::VarAllocator &varAlloc) :
		map_(varAlloc),
		keyMap_(varAlloc),
		workingKeys_(varAlloc),
		prevMatchIt_(map_.end()),
		curMatchIt_(map_.end()),
		curStarted_(false),
		curFinished_(false),
		varMatching_(false),
		prevMatched_(false),
		curMatched_(false),
		nextKey_(0),
		searchRange_(VariableGroupEntry()),
		partitionTail_(false),
		matchCount_(0) {
}


SQLSortOps::WindowMatch::ProjectionState::ProjectionState(
		SQLValues::VarAllocator &varAlloc) :
		matchOrdinal_(0),
		type_(NAV_NONE),
		historyIndex_(0),
		nextPos_(0),
		history_(varAlloc),
		groupMap_(varAlloc),
		searchRange_(VariableGroupEntry()),
		firstPos_(0) {
}


SQLSortOps::WindowMatch::PatternGraph::PatternGraph(
		OpContext &cxt, const SQLExprs::Expression &expr) :
		nodeList_(cxt.getAllocator()) {
	build(expr, nodeList_);
}

bool SQLSortOps::WindowMatch::PatternGraph::findSub(
		uint32_t id, bool forChild, uint32_t ordinal,
		uint32_t &subId) const {
	subId = std::numeric_limits<uint32_t>::max();
	const PatternNode &node = getNode(id);
	const util::Vector<uint32_t> &list = (forChild ? node.child_ : node.following_);

	if (ordinal >= list.size()) {
		return false;
	}

	subId = list[ordinal];
	return true;
}

uint32_t SQLSortOps::WindowMatch::PatternGraph::getDepth(
		uint32_t id) const {
	const PatternCode &code = getCode(id);
	return code.depth_;
}

bool SQLSortOps::WindowMatch::PatternGraph::findVariable(
		uint32_t id, int64_t &varId) const {
	varId = -1;
	const PatternCode &code = getCode(id);

	if (code.variableId_ < 0) {
		return false;
	}

	varId = code.variableId_;
	return true;
}

uint64_t SQLSortOps::WindowMatch::PatternGraph::getMinRepeat(
		uint32_t id) const {
	const PatternCode &code = getCode(id);
	return static_cast<uint64_t>(code.minRepeat_);
}

bool SQLSortOps::WindowMatch::PatternGraph::findMaxRepeat(
		uint32_t id, uint64_t &repeat) const {
	repeat = std::numeric_limits<uint64_t>::max();
	const PatternCode &code = getCode(id);

	if (code.maxRepeat_ < 0) {
		return false;
	}

	repeat = static_cast<uint64_t>(code.maxRepeat_);
	return true;
}

bool SQLSortOps::WindowMatch::PatternGraph::getTopAnchor(
		uint32_t id) const {
	const PatternCode &code = getCode(id);
	return code.top_;
}

bool SQLSortOps::WindowMatch::PatternGraph::getTailAnchor(
		uint32_t id) const {
	const PatternCode &code = getCode(id);
	return code.tail_;
}

const SQLSortOps::WindowMatch::PatternCode&
SQLSortOps::WindowMatch::PatternGraph::getCode(uint32_t id) const {
	const PatternNode &node = getNode(id);
	return node.code_;
}

const SQLSortOps::WindowMatch::PatternNode&
SQLSortOps::WindowMatch::PatternGraph::getNode(uint32_t id) const {
	if (id >= nodeList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return nodeList_[id];
}

void SQLSortOps::WindowMatch::PatternGraph::build(
		const SQLExprs::Expression &expr, NodeList &nodeList) {
	const uint32_t id = newNode(nodeList, NULL);
	buildSub(expr, id, nodeList);
}

void SQLSortOps::WindowMatch::PatternGraph::buildSub(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList) {
	switch (expr.getCode().getType()) {
	case SQLType::EXPR_PATTERN_VARIABLE:
		buildVariable(expr, id, nodeList);
		break;
	case SQLType::EXPR_PATTERN_REPEAT:
		buildRepeat(expr, id, nodeList);
		break;
	case SQLType::EXPR_PATTERN_ANCHOR:
		buildAnchor(expr, id, nodeList);
		break;
	case SQLType::EXPR_PATTERN_CONCAT:
		buildConcat(expr, id, nodeList);
		break;
	case SQLType::EXPR_PATTERN_OR:
		buildOr(expr, id, nodeList);
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLSortOps::WindowMatch::PatternGraph::buildVariable(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList) {
	PatternCode &code = nodeList[id].code_;
	code.variableId_ = expr.child().getCode().getValue().get<int64_t>();
}

void SQLSortOps::WindowMatch::PatternGraph::buildRepeat(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList) {
	const uint32_t lastId = prepareStack(expr, id, nodeList, false);

	SQLExprs::Expression::Iterator it(expr);

	const SQLExprs::Expression &subExpr = it.get();
	it.next();
	const int64_t min = it.get().getCode().getValue().get<int64_t>();
	it.next();
	const int64_t max = it.get().getCode().getValue().get<int64_t>();

	if (min < 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	{
		PatternCode &code = nodeList[lastId].code_;
		code.minRepeat_ = min;
		code.maxRepeat_ = max;
	}

	buildSub(subExpr, lastId, nodeList);
}

void SQLSortOps::WindowMatch::PatternGraph::buildAnchor(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList) {
	const uint32_t lastId = prepareStack(expr, id, nodeList, false);

	SQLExprs::Expression::Iterator it(expr);

	const SQLExprs::Expression &subExpr = it.get();
	it.next();
	const bool top = SQLValues::ValueUtils::getValue<bool>(
			it.get().getCode().getValue());
	it.next();
	const bool tail = SQLValues::ValueUtils::getValue<bool>(
			it.get().getCode().getValue());

	{
		PatternCode &code = nodeList[lastId].code_;
		code.top_ = top;
		code.tail_ = tail;
	}

	buildSub(subExpr, lastId, nodeList);
}

void SQLSortOps::WindowMatch::PatternGraph::buildConcat(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList) {
	const uint32_t lastId = prepareStack(expr, id, nodeList, true);

	bool first = true;
	uint32_t prevId = lastId;
	for (SQLExprs::Expression::Iterator it(expr); it.exists(); it.next()) {
		uint32_t curId = prevId;
		if (!first) {
			curId = newNode(nodeList, &lastId);

			PatternNode &node = nodeList[curId];
			PatternNode &prevNode = nodeList[prevId];

			std::swap(node.following_, prevNode.following_);
			prevNode.following_.push_back(curId);
		}
		buildSub(it.get(), curId, nodeList);
		prevId = curId;
		first = false;
	}
}

void SQLSortOps::WindowMatch::PatternGraph::buildOr(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList) {
	for (SQLExprs::Expression::Iterator it(expr); it.exists(); it.next()) {
		const uint32_t curId = newStack(nodeList, id);
		buildSub(it.get(), curId, nodeList);
	}
}

uint32_t SQLSortOps::WindowMatch::PatternGraph::prepareStack(
		const SQLExprs::Expression &expr, uint32_t id, NodeList &nodeList,
		bool onlyForMultiElements) {
	if (!isNewStackRequired(expr, nodeList[id], onlyForMultiElements)) {
		return id;
	}

	return newStack(nodeList, id);
}

bool SQLSortOps::WindowMatch::PatternGraph::isNewStackRequired(
		const SQLExprs::Expression &expr,
		const PatternNode &node, bool onlyForMultiElements) {
	if (!isQuantitySpecified(node.code_)) {
		return false;
	}

	if (onlyForMultiElements) {
		SQLExprs::Expression::Iterator it(expr);
		if (!it.exists()) {
			return false;
		}
		it.next();
		if (!it.exists()) {
			return false;
		}
	}

	return true;
}

bool SQLSortOps::WindowMatch::PatternGraph::isQuantitySpecified(
		const PatternCode &code) {
	return (
			code.minRepeat_ != 1 ||
			code.maxRepeat_ != 1 ||
			code.top_ ||
			code.tail_);
}

uint32_t SQLSortOps::WindowMatch::PatternGraph::newStack(
		NodeList &nodeList, uint32_t baseId) {
	const uint32_t newId = newNode(nodeList, &baseId);
	nodeList[newId].code_.depth_++;
	nodeList[baseId].child_.push_back(newId);
	return newId;
}

uint32_t SQLSortOps::WindowMatch::PatternGraph::newNode(
		NodeList &nodeList, const uint32_t *baseId) {
	const uint32_t newId = static_cast<uint32_t>(nodeList.size());

	util::StackAllocator *alloc = nodeList.get_allocator().base();
	nodeList.push_back(PatternNode(*alloc));

	if (baseId != NULL) {
		const uint32_t depth = nodeList[*baseId].code_.depth_;
		nodeList[newId].code_.depth_ = depth;
	}

	return newId;
}


SQLSortOps::WindowMatch::PatternCode::PatternCode() :
		depth_(0),
		variableId_(-1),
		minRepeat_(1),
		maxRepeat_(1),
		top_(false),
		tail_(false) {
}


SQLSortOps::WindowMatch::PatternNode::PatternNode(util::StackAllocator &alloc) :
		following_(alloc),
		child_(alloc) {
}


SQLSortOps::WindowMatch::VariableTable::VariableTable(
		OpContext &cxt, const PatternGraph *patternGraph,
		const VariablePredicateMap *predMap, const NavigationMap *navMap) :
		varAlloc_(cxt.getValueContext().getVarAllocator()),
		patternGraph_(patternGraph),
		predMap_(predMap),
		navMap_(navMap),
		varIdsList_(makeVariableIdsList(cxt.getAllocator(), *navMap)),
		match_(varAlloc_),
		proj_(varAlloc_),
		navList_(varAlloc_) {
}

void SQLSortOps::WindowMatch::VariableTable::nextMatchingPosition(
		uint64_t pos) {
	{
		const uint64_t expectedPos = (
				match_.partitionTail_ ? 0 : match_.searchRange_.endPos_ + 1);
		if (expectedPos != pos) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
	}

	if (match_.partitionTail_) {
		clearAll();
	}
	else {
		const uint64_t nextPos = match_.searchRange_.endPos_ + 1;
		if (match_.prevMatched_) {
			const uint64_t matchCount = resolveMatchCount(nextPos);
			prepareNextMatchingPosition();
			match_.searchRange_.beginPos_ = nextPos;
			match_.matchCount_ = matchCount;
		}
		else {
			prepareNextMatchingPosition();
		}
		match_.searchRange_.endPos_ = nextPos;
	}
	navList_.clear();
}

bool SQLSortOps::WindowMatch::VariableTable::prepareProjection(
		bool forAllRows, NavigationType &type, bool &matchTail) {
	bool posTail = true;
	for (;;) {
		if (proj_.nextPos_ < proj_.searchRange_.endPos_) {
			proj_.nextPos_++;
			posTail = (proj_.nextPos_ >= proj_.searchRange_.endPos_);
			break;
		}

		switch (proj_.type_) {
		case NAV_PROJECTION_PIPE_ONCE:
			proj_.type_ = NAV_PROJECTION_PIPE_EACH;
			break;
		case NAV_PROJECTION_PIPE_EACH:
			proj_.type_ = NAV_PROJECTION_FINISH;
			break;
		default:
			proj_.type_ = NAV_NONE;
			break;
		}

		if (proj_.type_ == NAV_NONE) {
			MatchEntryIterator entryIt = match_.map_.end();

			const uint32_t limit = (isMatched() ? 2 : 0);
			while (proj_.matchOrdinal_ < limit) {
				const uint32_t ordinal = ++proj_.matchOrdinal_;

				if (ordinal == 1 && match_.prevMatched_) {
					entryIt = match_.prevMatchIt_;
				}
				else if (ordinal == 2 && match_.curMatched_) {
					entryIt = match_.curMatchIt_;
				}

				if (entryIt != match_.map_.end()) {
					break;
				}
			}

			if (entryIt == match_.map_.end()) {
				break;
			}

			proj_.type_ = NAV_PROJECTION_PIPE_ONCE;
			proj_.history_ = entryIt->second.history_;
			setUpProjectionVariableGroups(proj_.history_, proj_.groupMap_);

			proj_.searchRange_ = entryIt->second.searchRange_;
			proj_.firstPos_ = entryIt->second.firstPos_;
		}

		const bool lastOnly =
				(proj_.type_ == NAV_PROJECTION_PIPE_ONCE ||
				(proj_.type_ == NAV_PROJECTION_FINISH && !forAllRows));
		if (lastOnly) {
			proj_.historyIndex_ =
					proj_.history_.size() - (proj_.history_.empty() ? 0 : 1);
			proj_.nextPos_ = proj_.searchRange_.endPos_ -
					(proj_.firstPos_ >= proj_.searchRange_.endPos_ ? 0 : 1);
		}
		else {
			proj_.historyIndex_ = 0;
			proj_.nextPos_ = proj_.firstPos_;
		}
	}

	navList_.clear();
	type = proj_.type_;
	matchTail = (proj_.type_ == NAV_PROJECTION_FINISH && posTail);

	if (proj_.type_ == NAV_NONE) {
		return false;
	}

	applyWindowState();
	return true;
}

void SQLSortOps::WindowMatch::VariableTable::setPartitionTail() {
	if (match_.curStarted_ || isMatched()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	match_.partitionTail_ = true;
}

bool SQLSortOps::WindowMatch::VariableTable::findMatchingVariable(
		const VariablePredicateInfo *&predInfo) {
	predInfo = NULL;

	if (updateMatchingState()) {
		return false;
	}

	if (!match_.varMatching_ || match_.workingKeys_.empty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	MatchEntryIterator entryIt = match_.workingKeys_.begin()->second;
	const int64_t varId = entryIt->second.matchingVariableId_;
	if (varId < 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	predInfo = &resolveVariablePredicateInfo(varId);
	applyWindowState();
	return true;
}

void SQLSortOps::WindowMatch::VariableTable::updateMatchingVariable(
		bool matched) {
	if (!match_.varMatching_ || match_.workingKeys_.empty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	match_.varMatching_ = false;

	MatchEntryIterator entryIt = match_.workingKeys_.begin()->second;
	if (!matched) {
		eraseMatchWorkingKey(entryIt);
		return;
	}

	MatchEntry &entry = entryIt->second;

	uint64_t &entryEndPos = entry.searchRange_.endPos_;
	int64_t &varId = entry.matchingVariableId_;

	if (varId < 0 || entryEndPos != match_.searchRange_.endPos_ ||
			entry.stack_.empty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const VariableIdSet &idSet = varIdsList_[NAV_NONE];
	if (idSet.find(varId) != idSet.end()) {
		VariableHistory &history = entry.history_;
		if (!history.empty() &&
				history.back().variableId_ == varId &&
				history.back().endPos_ == entryEndPos) {
			history.back().endPos_++;
		}
		else {
			VariableHistoryElement elem;
			elem.variableId_ = varId;
			elem.beginPos_ = entryEndPos;
			elem.endPos_ = entryEndPos + 1;
			history.push_back(elem);
		}
	}

	entryEndPos++;
	varId = -1;
	repeatMatchStack(entryIt);
}

bool SQLSortOps::WindowMatch::VariableTable::findNavigationColumn(
		uint32_t ordinal, NavigationKey &key, uint32_t &navIndex,
		NavigationPosition &pos) {
	navIndex = std::numeric_limits<uint32_t>::max();
	pos = NavigationPosition();

	const LocatedNavigationList &navList = prepareNavigationList(key);

	if (ordinal >= navList.size()) {
		return false;
	}

	const LocatedNavigationInfo &info = navList[ordinal];
	navIndex = info.second->navigationIndex_;
	pos = info.first;
	return true;
}

SQLExprs::WindowState&
SQLSortOps::WindowMatch::VariableTable::getWindowState() {
	return windowState_;
}

bool SQLSortOps::WindowMatch::VariableTable::resolveMatch(
		uint64_t &effectiveStartPos) {
	effectiveStartPos = match_.searchRange_.beginPos_;

	if (!updateMatchingState()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (!isMatched()) {
		return false;
	}

	if (match_.curMatchIt_ == match_.map_.end()) {
		effectiveStartPos = match_.searchRange_.endPos_ + 1;
	}
	else {
		effectiveStartPos =
				match_.curMatchIt_->second.searchRange_.beginPos_;
	}
	return true;
}

void SQLSortOps::WindowMatch::VariableTable::getStats(
		uint64_t &searchingRowCount, uint64_t &candidateCount) {
	searchingRowCount =
			match_.searchRange_.endPos_ - match_.searchRange_.beginPos_ + 1;
	candidateCount = match_.map_.size();
}

void SQLSortOps::WindowMatch::VariableTable::clearAll() {
	prepareNextMatchingPosition();

	match_.searchRange_ = VariableGroupEntry();
	match_.partitionTail_ = false;
	match_.matchCount_ = 0;
	match_.nextKey_ = 0;
}

void SQLSortOps::WindowMatch::VariableTable::prepareNextMatchingPosition() {
	const bool inheritable = (!match_.partitionTail_ && !match_.prevMatched_);

	match_.prevMatchIt_ =
			(match_.partitionTail_ ? match_.map_.end() : match_.curMatchIt_);
	match_.curMatchIt_ = match_.map_.end();

	match_.workingKeys_.clear();

	setUpNextWorkingKeys(
			varAlloc_, match_.prevMatchIt_, patternGraph_, inheritable,
			match_.keyMap_, match_.workingKeys_);
	setUpNextMatchEntries(
			match_.prevMatchIt_, match_.partitionTail_, match_.workingKeys_,
			match_.map_);

	match_.curStarted_ = false;
	match_.curFinished_ = false;
	match_.varMatching_ = false;

	match_.prevMatched_ = false;
	match_.curMatched_ = false;

	proj_ = ProjectionState(varAlloc_);
}

void SQLSortOps::WindowMatch::VariableTable::setUpNextWorkingKeys(
		SQLValues::VarAllocator &varAlloc, MatchEntryIterator prevMatchIt,
		const PatternGraph *patternGraph, bool inheritable,
		UnorderedMatchKeyMap &keyMap, OrderedMatchKeyMap &workingKeys) {
	if (inheritable) {
		PatternStackPathMap pathMap(varAlloc);

		for (UnorderedMatchKeyMap::iterator it = keyMap.begin();
				it != keyMap.end(); ++it) {
			const uint64_t key = it->first.second;
			MatchEntryIterator entryIt = it->second;

			if (entryIt == prevMatchIt || entryIt->second.stack_.empty()) {
				continue;
			}

			bool acceptable = true;

			if (isReachedMatchEntry(patternGraph, entryIt)) {
				const PatternStackPath &path =
						getStackPath(varAlloc, entryIt->second.stack_);
				PatternStackPathMap::iterator pathIt = pathMap.find(path);

				if (pathIt == pathMap.end()) {
					pathMap.insert(std::make_pair(path, entryIt));
				}
				else if (isPriorMatchEntry(entryIt, pathIt->second)) {
					workingKeys.erase(pathIt->second->first);
					pathIt->second = entryIt;
				}
				else {
					acceptable = false;
				}
			}

			if (acceptable) {
				workingKeys.insert(std::make_pair(key, entryIt));
			}
		}
	}

	keyMap.clear();
}

void SQLSortOps::WindowMatch::VariableTable::setUpNextMatchEntries(
		MatchEntryIterator prevMatchIt, bool partitionTail,
		const OrderedMatchKeyMap &workingKeys, MatchEntryMap &map) {
	if (partitionTail) {
		map.clear();
		return;
	}

	for (MatchEntryIterator it = map.begin(); it != map.end();) {
		MatchEntryIterator next = it;
		++next;
		if (it != prevMatchIt &&
				workingKeys.find(it->first) == workingKeys.end()) {
			map.erase(it);
		}
		it = next;
	}
}

void SQLSortOps::WindowMatch::VariableTable::addInitialMatchEntry() {
	const uint32_t patternId = 0;

	MatchEntry entry(varAlloc_);
	entry.stack_.push_back(newMatchStackElement(patternId));
	entry.searchRange_ = match_.searchRange_;
	entry.firstPos_ = match_.searchRange_.endPos_;

	if (match_.prevMatchIt_ != match_.map_.end()) {
		entry.searchRange_.beginPos_ = entry.searchRange_.endPos_;
	}

	addMatchEntry(entry);
}

void SQLSortOps::WindowMatch::VariableTable::setUpProjectionVariableGroups(
		const VariableHistory &history, LocatedVariableGroupMap &groupMap) {
	groupMap.clear();
	const LocatedVariableGroupList initialList(0, VariableGroupList(varAlloc_));

	for (VariableHistory::const_iterator it = history.begin();
			it != history.end(); ++it) {
		const VariableHistoryElement &elem = *it;
		LocatedVariableGroupList &locatedList = groupMap.insert(std::make_pair(
				elem.variableId_, initialList)).first->second;
		VariableGroupList &list = locatedList.second;
		if (!list.empty() && list.back().endPos_ == elem.beginPos_) {
			list.back().endPos_ = elem.endPos_;
		}
		else {
			VariableGroupEntry entry;
			entry.beginPos_ = elem.beginPos_;
			entry.endPos_ = elem.endPos_;
			list.push_back(entry);
		}
	}
}

void SQLSortOps::WindowMatch::VariableTable::applyWindowState() {
	assert(!isMatched() || proj_.nextPos_ > 0);
	const uint64_t lastPos = (isMatched() ?
			proj_.nextPos_ - 1 : match_.searchRange_.beginPos_);

	windowState_.partitionValueCount_ =
			static_cast<int64_t>(resolveMatchCount(lastPos));
	windowState_.variableName_ = resolveVariableName(lastPos);
}

uint64_t SQLSortOps::WindowMatch::VariableTable::resolveMatchCount(
		uint64_t startPos) {
	uint64_t projectingMatchCount = 0;

	if (isMatched()) {
		if (match_.prevMatched_) {
			projectingMatchCount++;
		}

		if (match_.curMatched_ &&
				match_.curMatchIt_ != match_.map_.end() &&
				match_.curMatchIt_->second.firstPos_ <= startPos) {
			projectingMatchCount++;
		}
	}
	else {
		OrderedMatchKeyMap::iterator it = match_.workingKeys_.begin();
		if (it != match_.workingKeys_.end() &&
				it->second->second.firstPos_ <= startPos) {
			projectingMatchCount++;
		}
	}

	return match_.matchCount_ + projectingMatchCount;
}

const TupleValue* SQLSortOps::WindowMatch::VariableTable::resolveVariableName(
		uint64_t pos) {
	if (!isMatched() || varIdsList_[NAV_CLASSIFIER].empty()) {
		return NULL;
	}

	const VariableHistory &history = proj_.history_;
	size_t &i = proj_.historyIndex_;

	while (i > 0 && i < history.size() && pos < history[i].beginPos_) {
		i--;
	}
	while (i + 1 < history.size() && pos >= history[i].endPos_) {
		i++;
	}

	if (i >= history.size()) {
		return NULL;
	}

	const VariableHistoryElement &elem = history[i];
	if (!(elem.beginPos_ <= pos && pos < elem.endPos_)) {
		return NULL;
	}

	return resolveVariablePredicateInfo(elem.variableId_).name_;
}

const SQLSortOps::WindowMatch::LocatedNavigationList&
SQLSortOps::WindowMatch::VariableTable::prepareNavigationList(
		NavigationKey &key) {
	const MatchEntry *matchEntry;
	uint64_t curPos;
	key = resolveNavigationKey(matchEntry, curPos);

	if (!navList_.empty()) {
		return navList_;
	}

	for (NavigationMap::const_iterator it = navMap_->lower_bound(key);
			it != navMap_->end() && it->first == key; ++it) {
		const NavigationInfo *info = &it->second;
		const int64_t refVarId = info->variableId_;

		const VariableGroupEntry &group =
				resolveNavigationGroup(matchEntry, *info, refVarId, curPos);
		const NavigationPosition &navPos =
				resolveNavigationPosition(*info, group, curPos);

		navList_.push_back(std::make_pair(navPos, info));
	}
	std::sort(navList_.begin(), navList_.end());

	return navList_;
}

SQLSortOps::WindowMatch::NavigationKey
SQLSortOps::WindowMatch::VariableTable::resolveNavigationKey(
		const MatchEntry *&matchEntry, uint64_t &curPos) {
	matchEntry = NULL;

	if (proj_.matchOrdinal_ == 0) {
		if (isMatched() || !match_.varMatching_) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		MatchEntryIterator entryIt = match_.workingKeys_.begin()->second;
		matchEntry = &entryIt->second;
		if (matchEntry->matchingVariableId_ < 0) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		curPos = match_.searchRange_.endPos_;
		return NavigationKey(NAV_MATCHING, matchEntry->matchingVariableId_);
	}
	else {
		if (!isMatched() || proj_.matchOrdinal_ > 2) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		curPos = proj_.nextPos_ - 1;
		return NavigationKey(proj_.type_, -1);
	}
}

SQLSortOps::WindowMatch::VariableGroupEntry
SQLSortOps::WindowMatch::VariableTable::resolveNavigationGroup(
		const MatchEntry *matchEntry, const NavigationInfo &info,
		int64_t refVarId, uint64_t curPos) {
	VariableGroupEntry searchRange =
			(matchEntry == NULL ? proj_.searchRange_ : match_.searchRange_);

	const VariableGroupEntry *baseGroup;
	if (matchEntry == NULL) {
		if (info.byMatch_) {
			if (refVarId < 0) {
				baseGroup = NULL;
			}
			else {
				const LocatedVariableGroupList &locatedList =
						resolveLocatedVariableGroupList(refVarId);
				const VariableGroupList &list = locatedList.second;
				if (info.direction_ < 0) {
					baseGroup = &list.front();
				}
				else {
					baseGroup = &list.back();
				}
			}
		}
		else {
			if (refVarId < 0 || info.direction_ != 0) {
				baseGroup = &searchRange;
			}
			else {
				LocatedVariableGroupList &locatedList =
						resolveLocatedVariableGroupList(refVarId);
				uint32_t &loc = locatedList.first;
				const VariableGroupList &list = locatedList.second;

				while (loc > 0 && loc < list.size() &&
						curPos < list[loc].beginPos_) {
					loc--;
				}
				while (loc + 1 < list.size() && curPos >= list[loc].endPos_) {
					loc++;
				}

				if (loc >= list.size()) {
					baseGroup = NULL;
				}
				else {
					baseGroup = &list[loc];
				}
			}
		}
	}
	else {
		if (info.byMatch_ || info.direction_ > 0) {
			baseGroup = NULL;
		}
		else if (refVarId >= 0) {
			VariableGroupMap::const_iterator it =
					matchEntry->groupMap_.find(refVarId);
			if (it == matchEntry->groupMap_.end()) {
				baseGroup = NULL;
			}
			else {
				baseGroup = &it->second;
			}
		}
		else {
			searchRange.endPos_++;
			baseGroup = &searchRange;
		}
	}

	if (baseGroup == NULL) {
		return VariableGroupEntry();
	}

	VariableGroupEntry group = *baseGroup;
	if (!info.byMatch_ && info.direction_ < 0 &&
			group.beginPos_ <= searchRange.endPos_) {
		group.beginPos_ = std::min(group.beginPos_, searchRange.beginPos_);
	}
	return group;
}

SQLSortOps::WindowMatch::NavigationPosition
SQLSortOps::WindowMatch::VariableTable::resolveNavigationPosition(
		const NavigationInfo &info, const VariableGroupEntry &group,
		uint64_t curPos) {
	uint64_t pos = 0;
	bool empty = true;
	if (group.beginPos_ < group.endPos_) {
		if (info.byMatch_) {
			pos = (info.direction_ < 0 ? group.beginPos_ : group.endPos_ - 1);
			empty = false;
		}
		else if (curPos > 0 || info.direction_ >= 0) {
			const uint64_t navPos = curPos +
					(info.direction_ == 0 ? 0 : (info.direction_ < 0 ? -1 : 1));
			if (group.beginPos_ <= navPos && navPos < group.endPos_) {
				pos = navPos;
				empty = false;
			}
		}
	}

	return NavigationPosition(pos, empty);
}

bool SQLSortOps::WindowMatch::VariableTable::updateMatchingState() {
	if (match_.curFinished_) {
		return true;
	}

	if (!match_.curStarted_) {
		addInitialMatchEntry();
		match_.curStarted_ = true;
	}
	navList_.clear();

	while (!match_.workingKeys_.empty()) {
		MatchEntryIterator entryIt = match_.workingKeys_.begin()->second;

		bool currentAllowed;
		bool followingAllowed;
		if (!checkNextPattern(entryIt, currentAllowed, followingAllowed)) {
			continue;
		}

		bool working;
		if (!applyVariablePattern(
				entryIt, currentAllowed, followingAllowed, working)) {
			if (working) {
				match_.varMatching_ = true;
				return false;
			}
			continue;
		}

		applyNextPattern(entryIt, currentAllowed, followingAllowed);
	}

	assignMatchResult();
	return true;
}

void SQLSortOps::WindowMatch::VariableTable::assignMatchResult() {
	match_.prevMatched_ =
			(match_.prevMatchIt_ != match_.map_.end() &&
			(match_.curMatchIt_ == match_.map_.end() ||
					match_.prevMatchIt_->second.firstPos_ !=
					match_.curMatchIt_->second.firstPos_));
	match_.curMatched_ =
			(match_.curMatchIt_ != match_.map_.end() && match_.partitionTail_);

	match_.curFinished_ = true;
	navList_.clear();
}

bool SQLSortOps::WindowMatch::VariableTable::isMatched() {
	return (match_.prevMatched_ || match_.curMatched_);
}

bool SQLSortOps::WindowMatch::VariableTable::checkNextPattern(
		MatchEntryIterator entryIt, bool &currentAllowed,
		bool &followingAllowed) {
	currentAllowed = true;
	followingAllowed = true;

	const MatchEntry &entry = entryIt->second;
	const MatchStackElement &stackElem = entry.stack_.back();

	const uint32_t patternId = stackElem.patternId_;
	const uint64_t repeat = stackElem.repeat_;

	if (patternGraph_->getTopAnchor(patternId)) {
		if (entry.firstPos_ != 0) {
			eraseMatchWorkingKey(entryIt);
			return false;
		}
	}

	const uint64_t min = patternGraph_->getMinRepeat(patternId);
	if (repeat < min) {
		followingAllowed = false;
	}

	uint64_t max;
	if (patternGraph_->findMaxRepeat(patternId, max)) {
		if (repeat > max) {
			currentAllowed = false;
			followingAllowed = false;
		}
		else if (repeat >= max) {
			currentAllowed = false;
		}
	}
	return true;
}

bool SQLSortOps::WindowMatch::VariableTable::applyVariablePattern(
		MatchEntryIterator entryIt, bool currentAllowed, bool followingAllowed,
		bool &working) {
	working = true;

	if (!currentAllowed) {
		return true;
	}

	MatchEntry &entry = entryIt->second;
	const uint32_t patternId = entry.stack_.back().patternId_;

	int64_t varId;
	if (!patternGraph_->findVariable(patternId, varId)) {
		return true;
	}

	if (entry.searchRange_.endPos_ > match_.searchRange_.endPos_) {
		if (followingAllowed) {
			MatchEntryIterator it = addMatchEntry(entryIt->second);
			completeMatchEntryWorking(it);
			return true;
		}
		else {
			completeMatchEntryWorking(entryIt);
			working = false;
			return false;
		}
	}
	else if (entry.searchRange_.endPos_ == match_.searchRange_.endPos_) {
		entry.matchingVariableId_ = varId;
		return false;
	}
	else {
		return true;
	}
}

void SQLSortOps::WindowMatch::VariableTable::applyNextPattern(
		MatchEntryIterator entryIt, bool currentAllowed, bool followingAllowed) {
	MatchEntry &entry = entryIt->second;
	const uint32_t patternId = entry.stack_.back().patternId_;
	const bool withTailAnchor = entry.stack_.back().withTailAnchor_;
	const size_t depth = entry.stack_.size();

	util::LocalUniquePtr<MatchEntry> prevEntry;
	bool updated = false;

	for (uint32_t ordinal = 0, subId; patternGraph_->findSub(
			patternId, true, ordinal, subId); ordinal++) {
		if (currentAllowed) {
			pushMatchStack(entryIt, subId, prevEntry);
			updated = true;
		}
	}

	bool followingFound = false;
	for (uint32_t ordinal = 0, subId; patternGraph_->findSub(
			patternId, false, ordinal, subId); ordinal++) {
		if (followingAllowed) {
			forwardMatchStack(entryIt, subId, prevEntry);
			updated = true;
		}
		followingFound = true;
	}

	if (followingAllowed && !followingFound) {
		if (depth > 1) {
			popMatchStack(entryIt, prevEntry);
			updated = true;
		}
		else if (!withTailAnchor || (match_.partitionTail_ &&
				entry.searchRange_.endPos_ == match_.searchRange_.endPos_ + 1)) {
			MatchEntryIterator it = popMatchStack(entryIt, prevEntry);
			completeMatchEntryWorking(it);
			updated = true;
		}
	}

	if (!updated) {
		eraseMatchWorkingKey(entryIt);
	}
}

void SQLSortOps::WindowMatch::VariableTable::completeMatchEntryWorking(
		MatchEntryIterator entryIt) {
	bool matchUpdatable;
	MatchEntryIterator it = reduceMatchEntry(entryIt, matchUpdatable);

	eraseMatchWorkingKey(it);

	if (matchUpdatable) {
		match_.curMatchIt_ = it;
	}
}

void SQLSortOps::WindowMatch::VariableTable::repeatMatchStack(
		MatchEntryIterator entryIt) {
	MatchEntry &entry = entryIt->second;
	entry.stack_.back().repeat_++;
}

void SQLSortOps::WindowMatch::VariableTable::forwardMatchStack(
		MatchEntryIterator entryIt, uint32_t patternId,
		util::LocalUniquePtr<MatchEntry> &prevEntry) {
	MatchEntryIterator it = prepareMatchEntry(entryIt, &prevEntry);
	MatchEntry &entry = it->second;
	entry.stack_.back() = newMatchStackElement(patternId);
}

void SQLSortOps::WindowMatch::VariableTable::pushMatchStack(
		MatchEntryIterator entryIt, uint32_t patternId,
		util::LocalUniquePtr<MatchEntry> &prevEntry) {
	MatchEntryIterator it = prepareMatchEntry(entryIt, &prevEntry);
	MatchEntry &entry = it->second;
	updateMatchStackHash(entry.stack_);
	entry.stack_.push_back(newMatchStackElement(patternId));
}

SQLSortOps::WindowMatch::MatchEntryIterator
SQLSortOps::WindowMatch::VariableTable::popMatchStack(
		MatchEntryIterator entryIt,
		util::LocalUniquePtr<MatchEntry> &prevEntry) {
	MatchEntryIterator it = prepareMatchEntry(entryIt, &prevEntry);
	MatchStack &stack = it->second.stack_;

	const bool withTailAnchor = stack.back().withTailAnchor_;
	stack.pop_back();

	if (!stack.empty()) {
		stack.back().withTailAnchor_ = withTailAnchor;
		repeatMatchStack(it);
	}

	return it;
}

SQLSortOps::WindowMatch::MatchStackElement
SQLSortOps::WindowMatch::VariableTable::newMatchStackElement(
		uint32_t patternId) {
	const bool withTailAnchor = patternGraph_->getTailAnchor(patternId);
	return MatchStackElement(patternId, withTailAnchor);
}

SQLSortOps::WindowMatch::MatchEntryIterator
SQLSortOps::WindowMatch::VariableTable::prepareMatchEntry(
		MatchEntryIterator entryIt,
		util::LocalUniquePtr<MatchEntry> *prevEntry) {
	if (prevEntry == NULL) {
		return entryIt;
	}

	if (prevEntry->get() == NULL) {
		*prevEntry =
				UTIL_MAKE_LOCAL_UNIQUE(*prevEntry, MatchEntry, entryIt->second);
		return entryIt;
	}
	else {
		return addMatchEntry(MatchEntry(**prevEntry));
	}
}

SQLSortOps::WindowMatch::MatchEntryIterator
SQLSortOps::WindowMatch::VariableTable::addMatchEntry(
		const MatchEntry &src) {
	MatchEntryIterator it =
			match_.map_.insert(std::make_pair(match_.nextKey_, src)).first;
	match_.nextKey_++;
	match_.workingKeys_.insert(std::make_pair(it->first, it));
	return it;
}

SQLSortOps::WindowMatch::MatchEntryIterator
SQLSortOps::WindowMatch::VariableTable::reduceMatchEntry(
		MatchEntryIterator entryIt, bool &matchUpdatable) {
	MatchEntry &entry = entryIt->second;
	matchUpdatable = entry.stack_.empty();

	const uint32_t hash = updateMatchStackHash(entry.stack_);
	MatchEntryIterator anotherIt;
	if (findSameMatchEntry(hash, entry, anotherIt)) {
		MatchEntryIterator priorIt;
		MatchEntryIterator reducingIt;

		if (isPriorMatchEntry(anotherIt, entryIt)) {
			if (match_.curMatchIt_ == entryIt) {
				match_.curMatchIt_ = anotherIt;
			}
			priorIt = anotherIt;
			reducingIt = entryIt;
		}
		else {
			priorIt = entryIt;
			reducingIt = anotherIt;
		}

		match_.keyMap_.erase(MatchHashKey(hash, reducingIt->first));
		eraseMatchWorkingKey(reducingIt);
		match_.map_.erase(reducingIt);
		return priorIt;
	}

	if (entry.stack_.empty() &&
			match_.curMatchIt_ != match_.map_.end() &&
			isPriorMatchEntry(match_.curMatchIt_, entryIt)) {
		matchUpdatable = false;
		return entryIt;
	}

	match_.keyMap_.insert(std::make_pair(
			MatchHashKey(hash, entryIt->first), entryIt));
	return entryIt;
}

void SQLSortOps::WindowMatch::VariableTable::eraseMatchWorkingKey(
		MatchEntryIterator entryIt) {
	match_.workingKeys_.erase(entryIt->first);
}

bool SQLSortOps::WindowMatch::VariableTable::findSameMatchEntry(
		uint32_t hash, const MatchEntry &entry, MatchEntryIterator &foundIt) {
	foundIt = match_.map_.end();

	MatchHashKey hashKey(hash, 0);
	UnorderedMatchKeyMap::const_iterator it =
			match_.keyMap_.lower_bound(hashKey);
	for (; it != match_.keyMap_.end() &&
			it->first.first == hashKey.first; ++it) {
		if (isSameStack(it->second->second.stack_, entry.stack_)) {
			foundIt = it->second;
			return true;
		}
	}
	return false;
}

bool SQLSortOps::WindowMatch::VariableTable::isSameStack(
		const MatchStack &stack1, const MatchStack &stack2) {
	if (stack1.size() != stack2.size()) {
		return false;
	}

	MatchStack::const_iterator it1 = stack1.end();
	MatchStack::const_iterator it2 = stack2.end();

	while (it1 != stack1.begin() && it2 != stack2.begin()) {
		if (isSameStackElement(*(--it1), *(--it2))) {
			return true;
		}
	}

	return false;
}

bool SQLSortOps::WindowMatch::VariableTable::isSameStackElement(
		const MatchStackElement &elem1, const MatchStackElement &elem2) {
	return (
			elem1.hash_ == elem2.hash_ &&
			elem1.patternId_ == elem2.patternId_ &&
			elem1.repeat_ == elem2.repeat_ &&
			!elem1.withTailAnchor_ == !elem2.withTailAnchor_);
}

uint32_t SQLSortOps::WindowMatch::VariableTable::updateMatchStackHash(
		MatchStack &stack) {
	if (stack.empty()) {
		return 0;
	}

	const MatchStackElement &elem = stack.back();

	uint32_t hash;
	hash = (stack.size() < 2 ?
			SQLValues::ValueUtils::fnv1aHashInit() :
			stack[stack.size() - 2].hash_);
	hash = SQLValues::ValueUtils::fnv1aHashIntegral(
			hash, static_cast<int64_t>(elem.patternId_));
	hash = SQLValues::ValueUtils::fnv1aHashIntegral(
			hash, static_cast<int64_t>(elem.repeat_));
	hash = SQLValues::ValueUtils::fnv1aHashIntegral(
			hash, (elem.withTailAnchor_ ? 1 : 0));

	stack.back().hash_ = hash;
	return hash;
}

SQLSortOps::WindowMatch::PatternStackPath
SQLSortOps::WindowMatch::VariableTable::getStackPath(
		SQLValues::VarAllocator &varAlloc, const MatchStack &stack) {
	PatternStackPath path(varAlloc);
	for (MatchStack::const_iterator it = stack.begin(); it != stack.end(); ++it) {
		path.push_back(it->patternId_);
	}
	return path;
}

bool SQLSortOps::WindowMatch::VariableTable::isPriorMatchEntry(
		const MatchEntryIterator &it1, const MatchEntryIterator &it2) {
	if (it1->second.firstPos_ != it2->second.firstPos_) {
		return (it1->second.firstPos_ < it2->second.firstPos_);
	}

	if (it1->second.searchRange_.endPos_ != it2->second.searchRange_.endPos_) {
		return (it1->second.searchRange_.endPos_ >
				it2->second.searchRange_.endPos_);
	}

	return (it1->first < it2->first);
}

bool SQLSortOps::WindowMatch::VariableTable::isReachedMatchEntry(
		const PatternGraph *patternGraph, MatchEntryIterator entryIt) {
	const MatchStack &stack = entryIt->second.stack_;
	if (stack.empty()) {
		return false;
	}

	bool multiRepeatFound = false;
	for (MatchStack::const_iterator it = stack.begin(); it != stack.end(); ++it) {
		const bool forChild = false;
		const uint32_t ordinal = 0;
		uint32_t subId;
		if (patternGraph->findSub(it->patternId_, forChild, ordinal, subId)) {
			return false;
		}
		else if (it->repeat_ < patternGraph->getMinRepeat(it->patternId_)) {
			return false;
		}

		uint64_t repeat;
		if (patternGraph->findMaxRepeat(it->patternId_, repeat) &&
				repeat > 1) {
			if (multiRepeatFound) {
				return false;
			}
			multiRepeatFound = true;
		}
	}

	return true;
}

SQLSortOps::WindowMatch::LocatedVariableGroupList&
SQLSortOps::WindowMatch::VariableTable::resolveLocatedVariableGroupList(
		int64_t refVarId) {
	LocatedVariableGroupMap::iterator it = proj_.groupMap_.find(refVarId);

	if (it == proj_.groupMap_.end()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return it->second;
}

const SQLSortOps::WindowMatch::VariablePredicateInfo&
SQLSortOps::WindowMatch::VariableTable::resolveVariablePredicateInfo(
		int64_t refVarId) {
	VariablePredicateMap::const_iterator it = predMap_->find(refVarId);

	if (it == predMap_->end()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return it->second;
}

SQLSortOps::WindowMatch::VariableIdsList
SQLSortOps::WindowMatch::VariableTable::makeVariableIdsList(
		util::StackAllocator &alloc, const NavigationMap &navMap) {
	const NavigationType typeList[] = {
			NAV_NONE,
			NAV_MATCHING,
			NAV_PROJECTION_PIPE_ONCE,
			NAV_PROJECTION_PIPE_EACH,
			NAV_PROJECTION_FINISH,
			NAV_CLASSIFIER
	};
	const size_t typeCount = sizeof(typeList) / sizeof(*typeList);

	VariableIdsList list(typeCount, VariableIdSet(alloc), alloc);
	for (size_t i = 0 ; i < typeCount; i++) {
		const NavigationType type = typeList[i];
		const size_t index = static_cast<size_t>(type);
		assert(index < list.size());
		makeVariableIdSet(navMap, type, list[index]);
	}

	return list;
}

void SQLSortOps::WindowMatch::VariableTable::makeVariableIdSet(
		const NavigationMap &navMap, NavigationType type,
		VariableIdSet &idSet) {
	if (type == NAV_NONE) {
		makeVariableIdSetSub(navMap, NAV_PROJECTION_PIPE_ONCE, idSet);
		makeVariableIdSetSub(navMap, NAV_PROJECTION_PIPE_EACH, idSet);
		makeVariableIdSetSub(navMap, NAV_PROJECTION_FINISH, idSet);
		makeVariableIdSetSub(navMap, NAV_CLASSIFIER, idSet);
	}
	else {
		makeVariableIdSetSub(navMap, type, idSet);
	}
}

void SQLSortOps::WindowMatch::VariableTable::makeVariableIdSetSub(
		const NavigationMap &navMap, NavigationType baseType,
		VariableIdSet &idSet) {
	NavigationMap::const_iterator it =
			navMap.lower_bound(NavigationKey(baseType, -1));
	for (; it != navMap.end() && it->first.first == baseType; ++it) {
		const int64_t varId = it->second.variableId_;
		if (varId >= 0) {
			idSet.insert(varId);
		}
	}
}


SQLSortOps::WindowMatch::NavigationReaderEntry::NavigationReaderEntry() :
		position_(0),
		subIndex_(0),
		navigated_(false),
		preserved_(false) {
}


SQLSortOps::WindowMatch::NavigationReaderSet::NavigationReaderSet(
		OpContext &cxt, uint32_t maxReaderCount) :
		readerList_(makeReaderList(cxt.getAllocator(), maxReaderCount)),
		firstNavigablePosition_(0),
		firstPositionNavigated_(false),
		activeReaderCount_(0),
		posSet_(cxt.getValueContext().getVarAllocator()),
		distanceSet_(cxt.getValueContext().getVarAllocator()),
		prevOnlySet_(cxt.getValueContext().getVarAllocator()),
		unusedSet_(cxt.getValueContext().getVarAllocator()) {
}

void SQLSortOps::WindowMatch::NavigationReaderSet::setBaseReader(
		OpContext &cxt, TupleListReader &baseReader) {
	if (readerList_.empty() || firstNavigablePosition_ != 0 ||
			activeReaderCount_ != 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	activeReaderCount_ = static_cast<uint32_t>(readerList_.size());

	const uint64_t firstPos = 0;
	clearPositionsDetail(cxt, firstPos, &baseReader, readerList_.end());
}

bool SQLSortOps::WindowMatch::NavigationReaderSet::navigateReader(
		OpContext &cxt, uint64_t pos, TupleListReader *&reader) {

	if (!navigateFirstPosition(cxt)) {
		return false;
	}

	NavigationReaderIterator it;
	return navigateReaderDetail(cxt, pos, reader, it);
}

void SQLSortOps::WindowMatch::NavigationReaderSet::prepareNextPositions() {
	NavigationReaderIterator it = readerList_.begin();
	NavigationReaderIterator end = it + activeReaderCount_;
	for (; it != end; ++it) {
		if (it->preserved_) {
			continue;
		}

		NavigationReaderRefSet::iterator setIt = prevOnlySet_.find(it);
		if (setIt == prevOnlySet_.end()) {
			if (it->navigated_) {
				prevOnlySet_.insert(it);
			}
		}
		else {
			prevOnlySet_.erase(setIt);
			unusedSet_.insert(it);
		}
		it->navigated_ = false;
	}
}

void SQLSortOps::WindowMatch::NavigationReaderSet::invalidatePrevPositionRange(
		OpContext &cxt, uint64_t nextPos) {
	LocatedNavigationReaderSet::iterator setIt =
			posSet_.lower_bound(std::make_pair(nextPos, readerList_.begin()));

	if (setIt == posSet_.end() || setIt->first > nextPos) {
		if (setIt == posSet_.begin()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		--setIt;
	}

	NavigationReaderIterator baseIt = setIt->second;
	if (nextPos < baseIt->position_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	clearPositionsDetail(cxt, nextPos, NULL, baseIt);
}

void SQLSortOps::WindowMatch::NavigationReaderSet::clearPositions(
		OpContext &cxt, TupleListReader &baseReader) {
	const uint64_t firstPos = 0;
	clearPositionsDetail(cxt, firstPos, &baseReader, readerList_.end());
}

bool SQLSortOps::WindowMatch::NavigationReaderSet::navigateFirstPosition(
		OpContext &cxt) {
	if (firstPositionNavigated_) {
		return true;
	}

	TupleListReader *reader;
	NavigationReaderIterator it;
	if (!navigateReaderDetail(cxt, firstNavigablePosition_, reader, it)) {
		return false;
	}

	firstPositionNavigated_ = true;
	it->preserved_ = true;
	return true;
}

void SQLSortOps::WindowMatch::NavigationReaderSet::clearPositionsDetail(
		OpContext &cxt, uint64_t firstPos, TupleListReader *baseReader,
		NavigationReaderIterator baseIt) {
	firstNavigablePosition_ = firstPos;
	firstPositionNavigated_ = false;
	posSet_.clear();
	distanceSet_.clear();
	prevOnlySet_.clear();
	unusedSet_.clear();

	NavigationReaderIterator it = readerList_.begin();
	NavigationReaderIterator end = it + activeReaderCount_;
	for (; it != end; ++it) {
		if (baseReader == NULL) {
			inheritLocatedReader(cxt, baseIt, it);
		}
		else {
			applyLocatedReader(cxt, it, firstPos, *baseReader);
		}

		it->navigated_ = false;
		it->preserved_ = false;

		unusedSet_.insert(it);
		addDistanceEntry(it);
		posSet_.insert(std::make_pair(it->position_, it));
	}
}

bool SQLSortOps::WindowMatch::NavigationReaderSet::navigateReaderDetail(
		OpContext &cxt, uint64_t pos, TupleListReader *&reader,
		NavigationReaderIterator &it) {
	reader = NULL;
	it = prepareNavigableEntry(cxt, pos);

	TupleListReader &curReader = cxt.getReader(0, it->subIndex_);
	if (it->position_ > pos) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	bool done = false;
	for (;;) {
		if (cxt.checkSuspended()) {
			break;
		}
		else if (it->position_ >= pos) {
			done = true;
			break;
		}

		if (!curReader.exists()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		curReader.next();
		it->position_++;
	}

	applyNavigatedEntry(it, done);
	if (!curReader.exists() || (done && it->position_ != pos)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	reader = &curReader;
	return done;
}

SQLSortOps::WindowMatch::NavigationReaderIterator
SQLSortOps::WindowMatch::NavigationReaderSet::prepareNavigableEntry(
		OpContext &cxt, uint64_t pos) {
	LocatedNavigationReaderSet::iterator it =
			posSet_.lower_bound(std::make_pair(pos, readerList_.begin()));

	if (it == posSet_.end() || it->first > pos) {
		if (it == posSet_.begin()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		--it;
	}

	NavigationReaderIterator entryIt = it->second;
	if (it->first != pos || entryIt->preserved_) {
		return popLeastUseEntry(cxt, entryIt);
	}

	removeDistanceEntry(entryIt);
	posSet_.erase(it);

	return entryIt;
}

void SQLSortOps::WindowMatch::NavigationReaderSet::applyNavigatedEntry(
		NavigationReaderIterator it, bool done) {
	if (done && !it->navigated_) {
		prevOnlySet_.erase(it);
		unusedSet_.erase(it);
		it->navigated_ = true;
	}

	addDistanceEntry(it);
	posSet_.insert(std::make_pair(it->position_, it));
}

SQLSortOps::WindowMatch::NavigationReaderIterator
SQLSortOps::WindowMatch::NavigationReaderSet::popLeastUseEntry(
		OpContext &cxt, NavigationReaderIterator baseIt) {
	NavigationReaderIterator end = readerList_.end();
	NavigationReaderIterator it = end;

	bool reuse = true;
	do {
		if (!unusedSet_.empty()) {
			it = *unusedSet_.begin();
			break;
		}

		if (activeReaderCount_ < readerList_.size()) {
			it = readerList_.begin() + activeReaderCount_;
			unusedSet_.insert(it);
			activeReaderCount_++;
			reuse = false;
			break;
		}

		LocatedNavigationReaderSet::iterator setIt = distanceSet_.begin();
		if (setIt != distanceSet_.end() && setIt->second->preserved_) {
			++setIt;
		}

		if (setIt == distanceSet_.end()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		it = setIt->second;
	}
	while (false);

	if (reuse) {
		removeDistanceEntry(it);
		posSet_.erase(std::make_pair(it->position_, it));
	}

	inheritLocatedReader(cxt, baseIt, it);
	return it;
}

void SQLSortOps::WindowMatch::NavigationReaderSet::addDistanceEntry(
		NavigationReaderIterator it) {
	NavigationReaderIterator preceding;
	NavigationReaderIterator following;
	const std::pair<bool, bool> found = findNearEntry(it, preceding, following);
	if (found.second) {
		if (found.first) {
			distanceSet_.erase(
					std::make_pair(distanceof(preceding, following), following));
		}
		distanceSet_.insert(
				std::make_pair(distanceof(it, following), following));
	}


	if (found.first) {
		distanceSet_.insert(std::make_pair(distanceof(preceding, it), it));
	}
}

void SQLSortOps::WindowMatch::NavigationReaderSet::removeDistanceEntry(
		NavigationReaderIterator it) {
	NavigationReaderIterator preceding;
	NavigationReaderIterator following;
	const std::pair<bool, bool> found = findNearEntry(it, preceding, following);
	if (found.first) {
		distanceSet_.erase(std::make_pair(distanceof(preceding, it), it));
	}

	if (found.second) {
		distanceSet_.erase(
				std::make_pair(distanceof(it, following), following));
		if (found.first) {
			distanceSet_.insert(
					std::make_pair(distanceof(preceding, following), following));
		}
	}
}

std::pair<bool, bool>
SQLSortOps::WindowMatch::NavigationReaderSet::findNearEntry(
		NavigationReaderIterator baseIt, NavigationReaderIterator &preceding,
		NavigationReaderIterator &following) {
	const NavigationReaderIterator end = readerList_.end();

	preceding = end;
	following = end;

	LocatedNavigationReaderSet::iterator it = posSet_.lower_bound(
			std::make_pair(baseIt->position_, readerList_.begin()));
	if (it != posSet_.begin()) {
		LocatedNavigationReaderSet::iterator sub = it;
		--sub;
		preceding = sub->second;
	}

	if (it != posSet_.end()) {
		LocatedNavigationReaderSet::iterator sub = it;
		if (sub->second == baseIt) {
			++sub;
		}
		if (sub != posSet_.end()) {
			following = sub->second;
		}
	}

	return std::make_pair((preceding != end), (following != end));
}

void SQLSortOps::WindowMatch::NavigationReaderSet::inheritLocatedReader(
		OpContext &cxt, NavigationReaderIterator src,
		NavigationReaderIterator dest) {
	checkReaderIterator(src);

	if (src == dest) {
		return;
	}

	const uint64_t pos = src->position_;
	TupleListReader &baseRadaer = cxt.getReader(0, src->subIndex_);

	applyLocatedReader(cxt, dest, pos, baseRadaer);
}

void SQLSortOps::WindowMatch::NavigationReaderSet::applyLocatedReader(
		OpContext &cxt, NavigationReaderIterator it, uint64_t pos,
		TupleListReader &baseReader) {
	checkReaderIterator(it);

	if (!baseReader.exists()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	TupleListReader &reader = cxt.getReader(0, it->subIndex_);
	reader.assign(baseReader);
	it->position_ = pos;
}

uint64_t SQLSortOps::WindowMatch::NavigationReaderSet::distanceof(
		NavigationReaderIterator preceding, NavigationReaderIterator following) {
	checkReaderIterator(preceding);
	checkReaderIterator(following);

	if (following->position_ < preceding->position_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return (following->position_ - preceding->position_);
}
void SQLSortOps::WindowMatch::NavigationReaderSet::checkReaderIterator(
		NavigationReaderIterator it) {
	if (it < readerList_.begin() || it >= readerList_.end()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

SQLSortOps::WindowMatch::NavigationReaderList
SQLSortOps::WindowMatch::NavigationReaderSet::makeReaderList(
		util::StackAllocator &alloc, uint32_t count) {
	const uint32_t resolvedCount = std::max<uint32_t>(count, 2);

	NavigationReaderList list(alloc);
	while (list.size() < resolvedCount) {
		NavigationReaderEntry entry;
		entry.subIndex_ =
				IN_NAVIGATION_START + static_cast<uint32_t>(list.size());
		list.push_back(entry);
	}

	return list;
}


SQLSortOps::WindowMatch::ColumnNavigator::ColumnNavigator(
		OpContext &cxt, const NavigationMap &navMap,
		NavigationReaderSet &readerSet) :
		readerSet_(readerSet),
		assignmentExprMap_(makeExpressionMap(cxt.getAllocator(), navMap, true)),
		emptyExprMap_(makeExpressionMap(cxt.getAllocator(), navMap, false)),
		ordinal_ (0) {
}

bool SQLSortOps::WindowMatch::ColumnNavigator::navigate(
		OpContext &cxt, VariableTable &variableTable) {
	NavigationKey key;
	uint32_t navIndex;
	NavigationPosition pos;
	for (; variableTable.findNavigationColumn(
			ordinal_, key, navIndex, pos); ordinal_++) {
		const bool forAssignment = !pos.second;
		if (forAssignment) {
			TupleListReader *&reader = *cxt.getActiveReaderRef();
			if (!readerSet_.navigateReader(cxt, pos.first, reader)) {
				return false;
			}
		}

		const SQLExprs::Expression &expr =
				resolveExpression(key, forAssignment, navIndex);
		if (forAssignment) {
			expr.eval(cxt.getExprContext());
		}
		else {
			cxt.getDefaultAggregationTupleRef()->setNull(
					cxt.getDefaultAggregationTupleSet()->getTotalColumnList()[
							expr.getCode().getAggregationIndex()]);
		}
	}
	return true;
}

void SQLSortOps::WindowMatch::ColumnNavigator::clear() {
	ordinal_ = 0;
}

const SQLExprs::Expression&
SQLSortOps::WindowMatch::ColumnNavigator::resolveExpression(
		const NavigationKey &key, bool forAssignment, uint32_t navIndex) {
	const NavigationExprMap &map =
			(forAssignment ? assignmentExprMap_: emptyExprMap_);
	NavigationExprMap::const_iterator it = map.find(key);

	do {
		if (it == map.end()) {
			break;
		}

		const NavigationExprList &list = it->second;
		if (navIndex >= list.size()) {
			break;
		}

		const SQLExprs::Expression *expr = list[navIndex];
		if (expr == NULL) {
			break;
		}

		return *expr;
	}
	while (false);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

SQLSortOps::WindowMatch::NavigationExprMap
SQLSortOps::WindowMatch::ColumnNavigator::makeExpressionMap(
		util::StackAllocator &alloc, const NavigationMap &navMap,
		bool forAssignment) {
	NavigationExprMap exprMap(alloc);

	for (NavigationMap::const_iterator it = navMap.begin();
			it != navMap.end(); ++it) {
		const NavigationKey &key = it->first;

		NavigationExprList &list = exprMap.insert(
				std::make_pair(key, NavigationExprList(alloc))).first->second;

		const NavigationInfo &info = it->second;
		const size_t navIndex = info.navigationIndex_;
		list.resize(std::max(list.size(), navIndex + 1), NULL);
		list[navIndex] =
				(forAssignment ? info.assignmentExpr_ : info.emptyExpr_);
	}

	return exprMap;
}


SQLSortOps::WindowMatch::MatchContext::MatchContext(
		OpContext &cxt, const OpCode &code) :
		compiler_(cxt.getAllocator()),
		predMap_(compiler_.makeVariablePredicateMap(
				code.getFilterPredicate(), code.getPipeProjection())),
		navMap_(compiler_.makeNavigationMap(
				code.getFilterPredicate(), code.getPipeProjection())),
		patternGraph_(cxt, MicroCompiler::predicateToPatternExpr(
				code.getFilterPredicate())),
		variableTable_(cxt, &patternGraph_, &predMap_, &navMap_),
		navReaderSet_(cxt, MicroCompiler::getNavigationReaderCount(cxt, code)),
		navigator_(cxt, navMap_, navReaderSet_),
		proj_(MicroCompiler::getNavigableProjections(code.getPipeProjection())),
		memoryLimit_(MicroCompiler::getPatternMatchMemoryLimit(code)),
		allRowsPerMatch_(
				MicroCompiler::isAllRowsPerMatch(code.getFilterPredicate())),
		keyReaderStarted_(false),
		predPos_(0) {
	const SQLValues::CompColumnList *keyColumnList = code.findKeyColumnList();
	if (keyColumnList != NULL && !keyColumnList->empty()) {
		partEq_ = UTIL_MAKE_LOCAL_UNIQUE(
				partEq_, TupleEq, cxt.getAllocator(),
				SQLValues::TupleComparator(
						*keyColumnList, &cxt.getVarContext(), false));
	}
}

const SQLOps::Projection*
SQLSortOps::WindowMatch::MatchContext::findProjectionAt(NavigationType type) {
	switch (type) {
	case NAV_PROJECTION_PIPE_ONCE:
		return MicroCompiler::getProjectionElement(proj_, 0);
	case NAV_PROJECTION_PIPE_EACH:
		return MicroCompiler::getProjectionElement(proj_, 1);
	default:
		assert(type == NAV_PROJECTION_FINISH);
		return MicroCompiler::getProjectionElement(proj_, 2);
	}
}
