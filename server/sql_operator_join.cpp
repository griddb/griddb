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

#include "sql_operator_join.h"
#include "sql_operator_utils.h"

const SQLOps::OpRegistrar
SQLJoinOps::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLJoinOps::Registrar::operator()() const {
	add<SQLOpTypes::OP_JOIN, Join>();
	add<SQLOpTypes::OP_JOIN_OUTER, JoinOuter>();
	add<SQLOpTypes::OP_JOIN_GRACE_HASH, JoinGraceHash>();
	add<SQLOpTypes::OP_JOIN_SORTED, JoinSorted>();
	add<SQLOpTypes::OP_JOIN_NESTED, JoinNested>();
	add<SQLOpTypes::OP_JOIN_COMP_MERGE, JoinCompMerge>();
	add<SQLOpTypes::OP_JOIN_HASH, JoinInnerHash>();
	add<SQLOpTypes::OP_JOIN_OUTER_NESTED, JoinOuterNested>();
	add<SQLOpTypes::OP_JOIN_OUTER_COMP_MERGE, JoinOuterCompMerge>();
	add<SQLOpTypes::OP_JOIN_OUTER_HASH, JoinOuterHash>();
}


void SQLJoinOps::Join::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	const OpCode &code = getCode();

	const uint32_t inCount = code.getInputCount();

	const SQLType::JoinType joinType = code.getJoinType();

	const bool outer = (joinType != SQLType::JOIN_INNER);
	if (outer && joinType != SQLType::JOIN_LEFT_OUTER) {
		OpNode &node = plan.createNode(SQLOpTypes::OP_JOIN_OUTER);

		for (uint32_t i = 0; i < inCount; i++) {
			node.addInput(plan.getParentInput(i));
		}
		plan.linkToAllParentOutput(node);

		node.setCode(code);
		return;
	}

	const int32_t outerDrivingInput =
			(joinType == SQLType::JOIN_LEFT_OUTER ? 0 : -1);

	const SQLValues::CompColumnList *keyColumnList = code.findKeyColumnList();

	bool withKey = (keyColumnList != NULL);
	const bool eqOnly = isOnlyEqKeyFound(keyColumnList);

	const bool outEmpty = (code.getLimit() == 0);

	bool empty;
	bool small;
	int32_t baseDrivingInput;
	if (outEmpty) {
		empty = true;
		small = true;
		baseDrivingInput = 0;
	}
	else {
		uint32_t priorInput;
		baseDrivingInput =
				detectDrivingInput(cxt, code, &small, &empty, priorInput);
		cxt.getExtContext().setInputPriority(priorInput);
	}

	if (baseDrivingInput < 0) {
		cxt.setPlanPending();
		return;
	}


	const int64_t graceHashLimit = SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_GRACE_HASH_LIMIT, getCode().getConfig());

	const bool graceHashAlways = false;

	if (graceHashAlways) {
		empty = false;
		small = false;
	}

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	if (withKey && SQLValues::TupleComparator(
			builder.getExprRewriter().rewriteCompColumnList(
					builder.getExprFactoryContext(), *keyColumnList, false),
			NULL, false).isEitherSideEmpty(0) &&
			!graceHashAlways) {
		withKey = false;
		empty = true;
	}

	bool sorting = withKey;
	if (empty) {
		withKey = false;
		sorting = false;
	}
	else if (eqOnly && (small || (!small && graceHashLimit <= 0))) {
		sorting = false;
	}

	int32_t drivingInput = -1;
	if (!outer || (withKey && !sorting) ||
			baseDrivingInput == outerDrivingInput) {
		drivingInput = baseDrivingInput;
	}
	else {
		drivingInput = outerDrivingInput;
	}

	OpNode &joinNode = plan.createNode(
			sorting ? SQLOpTypes::OP_JOIN_SORTED :
			withKey ? (small ?
					(outer ?
							SQLOpTypes::OP_JOIN_OUTER_HASH :
							SQLOpTypes::OP_JOIN_HASH) :
					SQLOpTypes::OP_JOIN_GRACE_HASH) :
			(outer ?
					SQLOpTypes::OP_JOIN_OUTER_NESTED :
					SQLOpTypes::OP_JOIN_NESTED));

	for (uint32_t i = 0; i < inCount; i++) {
		const SQLOps::OpNode *inNode = &plan.getParentInput(i);
		if (empty &&
				(outEmpty || static_cast<int32_t>(i) != outerDrivingInput)) {
			OpCode emptyCode;
			emptyCode.setPipeProjection(
					&builder.createEmptyRefProjection(false, i, 0, NULL));
			emptyCode.setLimit(0);

			SQLOps::OpNode &emptyNode = plan.createNode(SQLOpTypes::OP_SELECT);
			emptyNode.setCode(emptyCode);

			emptyNode.addInput(*inNode);
			inNode = &emptyNode;
		}

		joinNode.addInput(*inNode);
	}

	Projection *distinctProj = NULL;
	{
		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
		SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

		OpCode joinCode = code;

		if (withKey) {
			assert(keyColumnList != NULL);
			SQLValues::CompColumnList &subKeyColumnList =
					builder.createCompColumnList();
			subKeyColumnList.insert(
					subKeyColumnList.end(),
					keyColumnList->begin(), keyColumnList->end());

			if (!sorting) {
				SQLOpUtils::CompPosSet posSet(factoryCxt.getAllocator());

				joinCode.setJoinPredicate(builder.extractJoinKeyColumns(
						joinCode.getJoinPredicate(), NULL,
						subKeyColumnList, posSet, NULL));

				joinCode.setFilterPredicate(builder.extractJoinKeyColumns(
						joinCode.getFilterPredicate(), &joinType,
						subKeyColumnList, posSet, NULL));
			}

			joinCode.setKeyColumnList(&subKeyColumnList);
			joinCode.setSideKeyColumnList(
					toSideKeyList(cxt.getAllocator(), subKeyColumnList));
		}
		else {
			joinCode.setKeyColumnList(NULL);
		}

		std::pair<Projection*, Projection*> projections;
		if (sorting || !withKey || small) {
			joinCode.setDrivingInput(drivingInput);
			projections = builder.arrangeProjections(
					joinCode, true, true, &distinctProj);
		}

		if (outer && sorting) {
			const bool first = (drivingInput == 0);
			joinCode.setJoinPredicate(rewriter.compColumnListToKeyFilterPredicate(
					factoryCxt, *keyColumnList, first, joinCode.getJoinPredicate()));
		}

		const Projection *pipeProj;
		if (outer && !sorting && projections.first != NULL) {
			SQLExprs::ExprRewriter::Scope scope(rewriter);
			rewriter.activateColumnMapping(factoryCxt);

			Projection &plainProj = builder.rewriteProjection(*projections.first);
			builder.applyJoinType(joinType);
			rewriter.setInputNull(1, true);
			rewriter.setCodeSetUpAlways(true);
			Projection &nullProj = builder.rewriteProjection(*projections.first);
			pipeProj = &builder.createMultiStageProjection(plainProj, nullProj);
		}
		else {
			pipeProj = projections.first;
		}

		if (pipeProj != NULL) {
			joinCode.setPipeProjection(pipeProj);
			joinCode.setFinishProjection(projections.second);
		}

		joinNode.setCode(joinCode);
	}

	OpCodeBuilder::setUpOutputNodes(
			plan, NULL, joinNode, distinctProj,
			getCode().getAggregationPhase());
}

bool SQLJoinOps::Join::isOnlyEqKeyFound(
		const SQLValues::CompColumnList *keyList) {
	if (keyList == NULL || keyList->empty()) {
		return false;
	}

	for (SQLValues::CompColumnList::const_iterator it = keyList->begin();
			it != keyList->end(); ++it) {
		if (it->isEitherUnbounded()) {
			return false;
		}
	}

	return true;
}

int32_t SQLJoinOps::Join::detectDrivingInput(
		OpContext &cxt, const OpCode &code, bool *small, bool *empty,
		uint32_t &priorInput) {
	const int32_t baseDrivingInput = detectDrivingInputDetail(
			cxt, code, small, empty, false, priorInput);

	if (baseDrivingInput >= 0 && *small && !(*empty)) {
		return detectDrivingInputDetail(
				cxt, code, small, empty, true, priorInput);
	}

	return baseDrivingInput;
}

int32_t SQLJoinOps::Join::detectDrivingInputDetail(
		OpContext &cxt, const OpCode &code, bool *small, bool *empty,
		bool exact, uint32_t &priorInput) {
	priorInput = 0;

	const SQLValues::CompColumnList *keyList = code.findKeyColumnList();

	const int64_t memLimit = SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, code.getConfig());
	const uint64_t baseThreshold = 32 * 1024 * 1024;
	const uint64_t rawThreshold =
			(memLimit < 0 ? baseThreshold : static_cast<uint64_t>(memLimit));
	const uint64_t threshold =
			std::max(rawThreshold, std::min(rawThreshold * 2, baseThreshold));

	bool eitherEmpty = false;
	bool eitherSmall = false;
	bool bothLarge = true;

	int32_t smallSide = -1;
	uint64_t sizeList[2];
	for (uint32_t i = 2; i > 0;) {
		uint64_t *size = &sizeList[--i];
		*size = cxt.getInputSize(i);

		if (*size <= threshold && (!exact ||
				estimateHashMapSize(cxt, keyList, i) <= threshold)) {
			if (!eitherSmall && cxt.isInputCompleted(i)) {
				if (*size <= 0) {
					eitherEmpty = true;
				}
				eitherSmall = true;
				smallSide = static_cast<int32_t>(i);
			}
			bothLarge = false;
		}
	}

	*empty = eitherEmpty;
	*small = eitherSmall;

	if (eitherEmpty) {
		assert(smallSide >= 0);
		return smallSide;
	}
	else if (cxt.isAllInputCompleted()) {
		return (sizeList[0] >= sizeList[1] ? 0 : 1);
	}
	else if (eitherSmall) {
		return (smallSide != 0 ? 0 : 1);
	}
	else if (bothLarge) {
		return 0;
	}
	else {
		priorInput = (sizeList[0] > sizeList[1] ? 1 : 0);
		return -1;
	}
}

uint64_t SQLJoinOps::Join::estimateHashMapSize(
		OpContext &cxt, const SQLValues::CompColumnList *keyList,
		uint32_t input) {
	assert(input < 2);
	const SQLOps::TupleColumnList &columnList = cxt.getInputColumnList(input);
	const uint64_t tupleCount = cxt.getInputTupleCount(input);

	util::StackAllocator &alloc = cxt.getAllocator();
	SQLValues::ValueContext valueCxt(SQLValues::ValueContext::ofAllocator(alloc));

	SummaryTupleSet tupleSet(valueCxt, NULL);
	tupleSet.addReaderColumnList(columnList);
	if (keyList != NULL) {
		tupleSet.addKeyList(*keyList, (input == 0), false);
	}
	tupleSet.setNullKeyIgnorable(true);
	tupleSet.completeColumns();

	const size_t hashHeadSize = sizeof(uint64_t) * 2;
	const size_t hashValueSize = tupleSet.estimateTupleSize();

	return tupleCount * (hashHeadSize + hashValueSize);
}

SQLJoinOps::JoinInputOrdinals SQLJoinOps::Join::getOrdinals(
		const OpCode &code) {
	const int32_t drivingInput = code.getDrivingInput();

	const bool reversed = (drivingInput == 1);
	assert(reversed || drivingInput <= 0);

	return JoinInputOrdinals(reversed);
}

bool SQLJoinOps::Join::checkJoinReady(
		OpContext &cxt, const JoinInputOrdinals &ordinals) {
	return cxt.isInputCompleted(ordinals.inner());
}

const SQLOps::Projection* SQLJoinOps::Join::expandProjection(
		const SQLOps::Projection *&proj, bool outer) {
	assert(proj != NULL);
	assert(!!outer == (proj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE));

	if (!outer) {
		return NULL;
	}

	const SQLOps::Projection *plainProj = &proj->chainAt(0);
	const SQLOps::Projection *nullProj = &proj->chainAt(1);

	proj = plainProj;
	return nullProj;
}

inline bool SQLJoinOps::Join::matchCondition(
		OpContext &cxt, const SQLExprs::Expression *condExpr) {
	if (condExpr != NULL) {
		SQLValues::VarContext::Scope varScope(cxt.getVarContext());
		return SQLValues::ValueUtils::isTrue(
				condExpr->eval(cxt.getExprContext()));
	}

	return true;
}

SQLValues::CompColumnList SQLJoinOps::Join::resolveKeyList(
		util::StackAllocator &alloc, const OpCode &code) {
	const SQLValues::CompColumnList &src = code.getKeyColumnList();

	SQLValues::CompColumnList dest(alloc);
	if (getOrdinals(code).driving() == 0) {
		dest = src;
	}
	else {
		for (SQLValues::CompColumnList::const_iterator it = src.begin();
				it != src.end(); ++it) {
			dest.push_back(it->toReversedSide());
		}
	}

	return dest;
}

SQLOps::CompColumnListPair SQLJoinOps::Join::resolveSideKeyList(
		const OpCode &code) {
	const SQLOps::CompColumnListPair *src = code.findSideKeyColumnList();
	assert(src != NULL);

	if (getOrdinals(code).driving() == 0) {
		return SQLOps::CompColumnListPair(*src);
	}
	else {
		return SQLOps::CompColumnListPair(src->second, src->first);
	}
}

SQLOps::CompColumnListPair* SQLJoinOps::Join::toSideKeyList(
		util::StackAllocator &alloc, const SQLValues::CompColumnList &src) {
	return ALLOC_NEW(alloc) SQLOps::CompColumnListPair(
			ALLOC_NEW(alloc) SQLValues::CompColumnList(
					toSingleSideKeyList(alloc, src, true)),
			ALLOC_NEW(alloc) SQLValues::CompColumnList(
					toSingleSideKeyList(alloc, src, false)));
}

SQLValues::CompColumnList SQLJoinOps::Join::toSingleSideKeyList(
		util::StackAllocator &alloc, const SQLValues::CompColumnList &src,
		bool first) {
	SQLValues::CompColumnList dest(alloc);

	for (SQLValues::CompColumnList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		dest.push_back(it->toSingleSide(first));
	}

	return dest;
}


void SQLJoinOps::JoinOuter::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	rewriter.activateColumnMapping(factoryCxt);

	const uint32_t inCount = 2;

	const SQLType::JoinType joinType = getCode().getJoinType();
	const bool outerList[inCount] = {
			(joinType == SQLType::JOIN_LEFT_OUTER ||
			joinType == SQLType::JOIN_FULL_OUTER),
			(joinType == SQLType::JOIN_RIGHT_OUTER ||
			joinType == SQLType::JOIN_FULL_OUTER)
	};
	for (uint32_t i = 0; i < inCount; i++) {
		if (outerList[i]) {
			rewriter.setIdOfInput(i, true);
		}
	}

	const OpNode *inNodeList[inCount];
	for (uint32_t i = 0; i < inCount; i++) {
		const OpNode *&inNode = inNodeList[i];

		inNode = &plan.getParentInput(i);

		SQLExprs::ExprRewriter::Scope scope(rewriter);
		rewriter.setMappedInput(i, 0);
		rewriter.setInputNull((inCount - i - 1), true);

		OpNode &node = plan.createNode(SQLOpTypes::OP_SELECT);
		node.addInput(*inNode);

		OpCode code;
		code.setPipeProjection(&builder.createProjectionByUsage(false));
		node.setCode(code);

		inNode = &node;
	}

	rewriter.setInputNullProjected(true);
	rewriter.setIdProjected(true);

	OpNode &joinNode = plan.createNode(SQLOpTypes::OP_JOIN);
	for (uint32_t i = 0; i < inCount; i++) {
		joinNode.addInput(*inNodeList[i]);
	}
	{
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
		factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);

		OpCode code;
		code.setJoinType(SQLType::JOIN_INNER);
		if (getCode().findKeyColumnList() != NULL) {
			code.setKeyColumnList(&rewriter.rewriteCompColumnList(
					factoryCxt, getCode().getKeyColumnList(), false));
		}
		if (getCode().getJoinPredicate() != NULL) {
			code.setFilterPredicate(&rewriter.rewritePredicate(
					factoryCxt, getCode().getJoinPredicate()));
		}
		code.setPipeProjection(&builder.createProjectionByUsage(false));
		joinNode.setCode(code);
	}

	rewriter.setInputProjected(true);
	builder.applyJoinType(joinType);

	OpNode *compensatingNode = &joinNode;
	for (uint32_t i = 0; i < inCount; i++) {
		if (!outerList[i]) {
			continue;
		}

		OpNode &node = plan.createNode(SQLOpTypes::OP_UNION);
		node.addInput(*compensatingNode);
		node.addInput(*inNodeList[i]);

		SQLValues::CompColumnList &keyColumnList =
				builder.createCompColumnList();
		{
			SQLValues::CompColumn keyColumn;
			keyColumn.setColumnPos(rewriter.getMappedIdColumn(i), true);
			keyColumnList.push_back(keyColumn);
		}

		OpCode code;
		code.setUnionType(SQLType::END_UNION);
		code.setKeyColumnList(&keyColumnList);
		code.setPipeProjection(&builder.createProjectionByUsage(true));
		node.setCode(code);

		compensatingNode = &node;
	}

	{
		OpNode &node = plan.createNode(SQLOpTypes::OP_SELECT);
		node.addInput(*compensatingNode);

		OpCode code = getCode();

		code.setJoinType(SQLType::END_JOIN);
		code.setJoinPredicate(NULL);
		code.setKeyColumnList(NULL);

		Projection *distinctProj;
		std::pair<Projection*, Projection*> projections =
				builder.arrangeProjections(code, true, true, &distinctProj);
		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);
		node.setCode(code);

		OpCodeBuilder::setUpOutputNodes(
				plan, NULL, node, distinctProj,
				getCode().getAggregationPhase());
	}
}


void SQLJoinOps::JoinGraceHash::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	const SQLOps::CompColumnListPair *sideKeyList =
			getCode().findSideKeyColumnList();

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	const bool orderingAvailable = JoinHash::isOrderingAvailable(
			builder.getExprRewriter().rewriteCompColumnList(
					builder.getExprFactoryContext(),
					getCode().getKeyColumnList(), false));

	SQLOps::OpConfig &config = builder.createConfig();
	const SQLOps::OpConfig *srcConfig = getCode().getConfig();
	if (srcConfig != NULL) {
		config = *srcConfig;
	}
	config.set(SQLOpTypes::CONF_GRACE_HASH_LIMIT, 1);

	if (getCode().getLimit() >= 0) {
		config.set(SQLOpTypes::CONF_LIMIT_INHERITANCE, 1);
	}

	const uint32_t bucketCount = getBucketCount(cxt);
	const uint32_t inCount = 2;

	OpNode *bucketNodeList[inCount] = { NULL };
	for (uint32_t i = 0; i < inCount; i++) {
		const bool first = (i == 0);
		OpNode &node = plan.createNode(SQLOpTypes::OP_GROUP_BUCKET_HASH);

		SQLValues::CompColumnList &bucketKeyList =
				builder.createCompColumnList();
		bucketKeyList = *(first ? sideKeyList->first : sideKeyList->second);
		if (!bucketKeyList.empty()) {
			bucketKeyList.front().setOrdering(orderingAvailable);
		}

		OpCode code;
		code.setKeyColumnList(&bucketKeyList);
		code.setPipeProjection(&createBucketProjection(builder, first));
		node.setCode(code);

		node.addInput(plan.getParentInput(i));
		bucketNodeList[i] = &node;
	}

	const ProjectionRefPair srcProjections(
			getCode().getPipeProjection(), getCode().getFinishProjection());

	SQLType::AggregationPhase mergePhase;
	const int32_t aggrMode = setUpAggregationBuilding(
			builder, srcProjections, getCode().getAggregationPhase(),
			mergePhase);

	const ProjectionRefPair &joinProjections =
			createJoinProjections(builder, srcProjections, aggrMode);

	OpNode *unionNode = NULL;
	if (aggrMode != 0) {
		unionNode = &plan.createNode(SQLOpTypes::OP_UNION);
	}

	for (uint32_t i = 0; i < bucketCount; i++) {
		OpNode &node = plan.createNode(SQLOpTypes::OP_JOIN);
		{
			OpCode code = getCode();
			code.setConfig(&config);
			code.setPipeProjection(joinProjections.first);
			code.setFinishProjection(joinProjections.second);
			node.setCode(code);
		}

		for (size_t j = 0; j < inCount; j++) {
			node.addInput(*bucketNodeList[j], i);
		}

		if (unionNode != NULL) {
			unionNode->addInput(node);
		}
		else {
			plan.getParentOutput(0).addInput(node);
		}
	}

	if (unionNode == NULL) {
		return;
	}

	{
		OpCode code;
		code.setPipeProjection(&createUnionProjection(cxt, joinProjections));
		code.setUnionType(SQLType::UNION_ALL);
		unionNode->setCode(code);
	}

	OpNode *resultNode;
	if (aggrMode == 0) {
		resultNode = unionNode;
	}
	else {
		const ProjectionRefPair &mergeProjections =
				createMergeProjections(builder, srcProjections, aggrMode);

		OpNode &selectNode = plan.createNode(SQLOpTypes::OP_SELECT);
		{
			OpCode code;
			code.setAggregationPhase(mergePhase);
			code.setPipeProjection(mergeProjections.first);
			code.setFinishProjection(mergeProjections.second);
			selectNode.setCode(code);
		}
		selectNode.addInput(*unionNode);
		resultNode = &selectNode;
	}

	{
		OpCode code = resultNode->getCode();
		code.setLimit(getCode().getLimit());
		resultNode->setCode(code);
		plan.linkToAllParentOutput(*resultNode);
	}
}

uint32_t SQLJoinOps::JoinGraceHash::getBucketCount(OpContext &cxt) const {
	static_cast<void>(cxt);

	const int64_t memLimit = SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, getCode().getConfig());
	const uint64_t resolvedLimit =
			static_cast<uint64_t>(memLimit < 0 ? 32 * 1024 * 1024 : memLimit);

	const uint64_t count = SQLValues::ValueUtils::getApproxPrimeNumber(
			std::max<uint64_t>(resolvedLimit / cxt.getInputBlockSize(0) / 4, 1U));
	return static_cast<uint32_t>(count);
}

SQLOps::Projection& SQLJoinOps::JoinGraceHash::createBucketProjection(
		OpCodeBuilder &builder, bool first) {
	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprRewriter::Scope scope(rewriter);

	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	rewriter.activateColumnMapping(factoryCxt);

	const uint32_t input = (first ? 0 : 1);
	rewriter.clearInputUsage();
	rewriter.addInputUsage(input);
	rewriter.addInputColumnUsage(input);
	rewriter.setMappedInput(input, 0);
	return builder.createProjectionByUsage(false);
}

SQLOps::Projection& SQLJoinOps::JoinGraceHash::createUnionProjection(
		OpContext &cxt, const ProjectionRefPair &joinProjections) {
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	builder.assignInputTypesByProjection(*(joinProjections.second == NULL ?
			joinProjections.first : joinProjections.second), NULL);
	return builder.createIdenticalProjection(true, 0);
}

SQLOpUtils::ProjectionRefPair SQLJoinOps::JoinGraceHash::createJoinProjections(
		OpCodeBuilder &builder, const ProjectionRefPair &src, int32_t aggrMode) {
	ProjectionRefPair dest;
	if (aggrMode == 0) {
		assert(src.second == NULL);
		dest.first = src.first;
	}
	else if (aggrMode == 1) {
		const Projection *proj = src.first;
		dest = ProjectionRefPair(
				&builder.createMultiStageAggregation(*proj, 0, false, NULL),
				&builder.createMultiStageAggregation(*proj, 0, true, NULL));
	}
	else {
		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
		SQLExprs::ExprRewriter::Scope scope(rewriter);

		SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
		rewriter.activateColumnMapping(factoryCxt);

		dest.first = &builder.createProjectionByUsage(false);
	}
	return dest;
}

SQLOpUtils::ProjectionRefPair SQLJoinOps::JoinGraceHash::createMergeProjections(
		OpCodeBuilder &builder, const ProjectionRefPair &src, int32_t aggrMode) {
	 if (aggrMode == 0) {
		return ProjectionRefPair();
	}
	else if (aggrMode == 1) {
		const Projection *proj = src.first;
		return ProjectionRefPair(
				&builder.createMultiStageAggregation(*proj, 2, false, NULL),
				&builder.createMultiStageAggregation(*proj, 2, true, NULL));
	}
	else {
		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
		SQLExprs::ExprRewriter::Scope scope(rewriter);

		SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
		rewriter.activateColumnMapping(factoryCxt);
		rewriter.setInputProjected(true);

		ProjectionRefPair dest;
		dest.first = &builder.rewriteProjection(*src.first);
		if (src.second != NULL) {
			dest.second = &builder.rewriteProjection(*src.second);
		}
		return dest;
	}
}

int32_t SQLJoinOps::JoinGraceHash::setUpAggregationBuilding(
		OpCodeBuilder &builder, const ProjectionRefPair &src,
		SQLType::AggregationPhase srcPhase,
		SQLType::AggregationPhase &mergePhase) {
	mergePhase = SQLType::END_AGG_PHASE;

	const Projection *srcProj = src.first;
	if (src.second != NULL || builder.findDistinctAggregation(*srcProj)) {
		mergePhase = srcPhase;
		return 2;
	}
	else if (OpCodeBuilder::isAggregationArrangementRequired(
			src.first, src.second)) {
		builder.getExprFactoryContext().setAggregationPhase(true, srcPhase);
		return 1;
	}
	else {
		return 0;
	}
}


void SQLJoinOps::JoinSorted::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	const OpCode &code = getCode();

	const bool outer = (code.getJoinType() != SQLType::JOIN_INNER);

	OpNode &joinNode = plan.createNode(outer ?
			SQLOpTypes::OP_JOIN_OUTER_COMP_MERGE :
			SQLOpTypes::OP_JOIN_COMP_MERGE);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	rewriter.activateColumnMapping(factoryCxt);

	const SQLValues::CompColumnList &srcList = code.getKeyColumnList();
	const SQLValues::CompColumnList &assignedSrcList =
			rewriter.rewriteCompColumnList(factoryCxt, srcList, false);
	const bool withDigest = !SQLValues::TupleDigester::isOrderingAvailable(
			assignedSrcList, false);

	const uint32_t inCount = getCode().getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		OpNode &subNode = plan.createNode(SQLOpTypes::OP_SORT);
		OpCode subCode;

		SQLExprs::ExprRewriter::Scope scope(rewriter);
		rewriter.clearInputUsage();
		rewriter.addInputUsage(i);
		rewriter.addInputColumnUsage(i);
		if (withDigest) {
			rewriter.setIdOfInput(i, true);
		}
		rewriter.setMappedInput(i, 0);

		Projection *midProj = &builder.createProjectionByUsage(false);
		if (!outer || i != 0) {
			midProj = &builder.createKeyFilteredProjection(*midProj);
		}
		subCode.setMiddleProjection(midProj);

		SQLValues::CompColumnList &midList = builder.createCompColumnList();
		for (SQLValues::CompColumnList::const_iterator it = srcList.begin();
				it != srcList.end(); ++it) {
			SQLValues::CompColumn key;
			key.setColumnPos(it->getColumnPos((i == 0)), true);
			key.setOrdering(it->isOrdering());
			midList.push_back(key);
		}
		subCode.setMiddleKeyColumnList(&midList);

		if (withDigest) {
			rewriter.setIdProjected(true);
		}
		rewriter.setInputProjected(true);

		SQLValues::CompColumnList &outList = rewriter.remapCompColumnList(
				factoryCxt, midList, i, true, false, NULL);
		subCode.setKeyColumnList(&outList);

		Projection *pipeProj = &builder.createMultiStageProjection(
				builder.createProjectionByUsage(false),
				builder.createProjectionByUsage(false));
		subCode.setPipeProjection(pipeProj);

		subNode.setCode(subCode);
		subNode.addInput(plan.getParentInput(i));

		joinNode.addInput(subNode);
	}

	if (withDigest || outer) {
		if (withDigest) {
			for (uint32_t i = 0; i < inCount; i++) {
				rewriter.setIdOfInput(i, true);
			}
			rewriter.setIdProjected(true);
		}

		SQLValues::CompColumnList &destList = builder.createCompColumnList();
		if (withDigest) {
			SQLValues::CompColumn key;
			for (uint32_t i = 0; i < inCount; i++) {
				key.setColumnPos(rewriter.getMappedIdColumn(i), (i == 0));
			}
			destList.push_back(key);
		}
		for (SQLValues::CompColumnList::const_iterator it = srcList.begin();
				it != srcList.end(); ++it) {
			SQLValues::CompColumn key = *it;
			for (uint32_t i = 0; i < inCount; i++) {
				const bool first = (i == 0);
				key.setColumnPos(
						rewriter.getMappedColumn(i, it->getColumnPos(first)), first);
			}
			destList.push_back(key);
		}

		OpCode subCode = code;
		subCode.setKeyColumnList(NULL);

		const Projection *pipeProj = NULL;
		if (outer) {
			const Projection *srcProj = subCode.getPipeProjection();
			subCode.setPipeProjection(NULL);

			SQLExprs::ExprRewriter::Scope scope(rewriter);

			Projection &plainPipe = builder.rewriteProjection(*srcProj);
			builder.applyJoinType(code.getJoinType());
			rewriter.setInputNull(1, true);
			rewriter.setCodeSetUpAlways(true);
			Projection &nullPipe = builder.rewriteProjection(*srcProj);
			pipeProj = &builder.createMultiStageProjection(plainPipe, nullPipe);
		}

		subCode = builder.rewriteCode(subCode, false);
		if (outer) {
			subCode.setPipeProjection(pipeProj);
		}
		subCode.setKeyColumnList(&destList);

		joinNode.setCode(subCode);
	}
	else {
		joinNode.setCode(code);
	}

	plan.linkToAllParentOutput(joinNode);
}


void SQLJoinOps::JoinNested::compile(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	cxt.setReaderRandomAccess(ordinals.inner());

	Operator::compile(cxt);
}

void SQLJoinOps::JoinNested::execute(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	TupleListReader &reader1 = cxt.getReader(ordinals.driving());
	TupleListReader &reader2 = cxt.getReader(ordinals.inner());
	TupleListReader &reader2Top = cxt.getReader(ordinals.inner(), 1);

	if (!Join::checkJoinReady(cxt, ordinals) || !reader2.exists()) {
		return;
	}
	reader2Top.exists(); 

	const SQLOps::Projection *proj = getCode().getPipeProjection();
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}
		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}
			proj->project(cxt);
		}

		reader2.assign(reader2Top);
	}
}


void SQLJoinOps::JoinCompMerge::compile(OpContext &cxt) const {
	const JoinInputOrdinals ordinals(false); 
	cxt.setReaderRandomAccess(ordinals.inner());

	Operator::compile(cxt);
}

void SQLJoinOps::JoinCompMerge::execute(OpContext &cxt) const {
	const JoinInputOrdinals ordinals(false); 
	TupleListReader &reader1 = cxt.getReader(ordinals.driving());
	TupleListReader &reader2 = cxt.getReader(ordinals.inner());
	TupleListReader &reader2Lower = cxt.getReader(ordinals.inner(), 1); 

	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), JoinMergeContext);
	}
	JoinMergeContext &mergeCxt = cxt.getResource(0).resolveAs<JoinMergeContext>();

	const Comparator comp(
			cxt.getAllocator(),
			TupleRangeComparator(
					getCode().getKeyColumnList(), &cxt.getVarContext()),
			cxt.getValueProfile());

	if (!reader2.exists()) {
		return;
	}

	const SQLOps::Projection *proj = getCode().getPipeProjection();
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}

		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}

			const int32_t ret =
					comp(ReaderSourceType(reader1), ReaderSourceType(reader2));

			if (!mergeCxt.lowerReached_) {
				if (ret > 0) {
					continue; 
				}
				reader2Lower.assign(reader2);
				mergeCxt.lowerReached_ = true;
			}

			if (ret < 0) {
				break; 
			}

			proj->project(cxt);
		}

		if (mergeCxt.lowerReached_) {
			reader2.assign(reader2Lower);
			mergeCxt.lowerReached_ = false;
		}
	}
}


void SQLJoinOps::JoinOuterNested::compile(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	cxt.setReaderRandomAccess(ordinals.inner());

	Operator::compile(cxt);
}

void SQLJoinOps::JoinOuterNested::execute(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	TupleListReader &reader1 = cxt.getReader(ordinals.driving());
	TupleListReader &reader2 = cxt.getReader(ordinals.inner());
	TupleListReader &reader2Top = cxt.getReader(ordinals.inner(), 1);

	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), JoinOuterContext);
	}
	JoinOuterContext &outerCxt = cxt.getResource(0).resolveAs<JoinOuterContext>();

	if (!Join::checkJoinReady(cxt, ordinals)) {
		return;
	}
	const bool reader2Exists = reader2Top.exists();

	const SQLOps::Projection *proj = getCode().getPipeProjection();
	const SQLOps::Projection *nullProj = Join::expandProjection(proj, true);
	const SQLExprs::Expression *joinPred = getCode().getJoinPredicate();
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}
		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}

			if (!Join::matchCondition(cxt, joinPred)) {
				continue;
			}

			proj->project(cxt);
			outerCxt.matched_ = true;
		}

		if (outerCxt.matched_) {
			outerCxt.matched_ = false;
		}
		else {
			nullProj->project(cxt);
		}

		if (reader2Exists) {
			reader2.assign(reader2Top);
		}
	}
}


void SQLJoinOps::JoinOuterCompMerge::compile(OpContext &cxt) const {
	const JoinInputOrdinals ordinals(false); 
	cxt.setReaderRandomAccess(ordinals.inner());

	Operator::compile(cxt);
}

void SQLJoinOps::JoinOuterCompMerge::execute(OpContext &cxt) const {
	const JoinInputOrdinals ordinals(false); 
	TupleListReader &reader1 = cxt.getReader(ordinals.driving());
	TupleListReader &reader2 = cxt.getReader(ordinals.inner());
	TupleListReader &reader2Lower = cxt.getReader(ordinals.inner(), 1); 

	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), JoinMergeContext);
	}
	JoinMergeContext &mergeCxt = cxt.getResource(0).resolveAs<JoinMergeContext>();

	const Comparator comp(
			cxt.getAllocator(),
			TupleRangeComparator(
					getCode().getKeyColumnList(), &cxt.getVarContext()),
			cxt.getValueProfile());

	const SQLOps::Projection *proj = getCode().getPipeProjection();
	const SQLOps::Projection *nullProj = Join::expandProjection(proj, true);
	const SQLExprs::Expression *joinPred = getCode().getJoinPredicate();
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}

		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}

			const int32_t ret =
					comp(ReaderSourceType(reader1), ReaderSourceType(reader2));

			if (!mergeCxt.lowerReached_) {
				if (ret > 0) {
					continue; 
				}
				reader2Lower.assign(reader2);
				mergeCxt.lowerReached_ = true;
			}

			if (ret < 0) {
				break; 
			}

			if (!Join::matchCondition(cxt, joinPred)) {
				continue;
			}

			proj->project(cxt);
			mergeCxt.matched_ = true;
		}

		if (mergeCxt.matched_) {
			mergeCxt.matched_ = false;
		}
		else {
			nullProj->project(cxt);
		}

		if (mergeCxt.lowerReached_) {
			reader2.assign(reader2Lower);
			mergeCxt.lowerReached_ = false;
		}
	}
}


void SQLJoinOps::JoinHash::compile(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	cxt.setReaderRandomAccess(ordinals.inner());
	cxt.setInputSourceType(
			ordinals.inner(), SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE);

	cxt.setReaderLatchDelayed(ordinals.inner());
	cxt.keepReaderLatch(ordinals.inner());

	Operator::compile(cxt);
}

template<bool Outer, bool InnerMatching>
void SQLJoinOps::JoinHash::executeAt(OpContext &cxt) const {
	typedef JoinHashContext<InnerMatching> HashContext;
	typedef TupleChain<InnerMatching> Chain;

	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	TupleListReader &reader1 = cxt.getReader(ordinals.driving());
	cxt.keepReaderLatch(ordinals.inner());

	HashContext &hashCxt = prepareHashContext<InnerMatching>(
			cxt, Join::resolveKeyList(cxt.getAllocator(), getCode()),
			Join::resolveSideKeyList(getCode()),
			cxt.getInputColumnList(ordinals.inner()), ordinals);
	if (!Join::checkJoinReady(cxt, ordinals)) {
		return;
	}

	cxt.setUpExprInputContext(ordinals.inner());
	*cxt.getSummaryColumnListRef(ordinals.inner()) =
			hashCxt.tupleSet_->getReaderColumnList();

	const SQLOps::Projection *proj = getCode().getPipeProjection();
	const SQLOps::Projection *nullProj = Join::expandProjection(proj, Outer);
	const SQLExprs::Expression *joinPred = getCode().getJoinPredicate();

	Chain *chain = hashCxt.nextChain_;
	hashCxt.nextChain_ = NULL;

	SummaryTuple *tupleRef =
			cxt.getExprContext().getSummaryTupleRef(ordinals.inner());

	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}

		do {
			if (chain == NULL) {
				chain = find(hashCxt, reader1);
				if (chain == NULL) {
					break;
				}
			}

			do {
				if (cxt.checkSuspended()) { 
					hashCxt.nextChain_ = chain;
					return;
				}

				*tupleRef = chain->tuple_;
				if (!Outer || Join::matchCondition(cxt, joinPred)) {
					proj->project(cxt);

					if (Outer) {
						if (InnerMatching) {
							chain->match_.set();
						}
						else {
							hashCxt.matched_ = true;
						}
					}
				}
			}
			while ((chain = chain->next_) != NULL);
		}
		while (false);

		if (Outer && !InnerMatching) {
			if (hashCxt.matched_) {
				hashCxt.matched_ = false;
			}
			else {
				nullProj->project(cxt);
			}
		}
	}

	if (Outer && InnerMatching && cxt.isAllInputCompleted()) {
		typedef typename MapOf<InnerMatching>::Type Map;
		Map &map = hashCxt.map_;
		bool nullChainChecked = false;
		for (typename Map::iterator it = map.begin();;) {
			if (it == map.end()) {
				chain = hashCxt.nullChain_;
				if (nullChainChecked || chain == NULL) {
					break;
				}
				nullChainChecked = true;
				*cxt.getSummaryColumnListRef(ordinals.inner()) =
						hashCxt.nullTupleSet_->getReaderColumnList();
			}
			else {
				chain = &it->second;
				++it;
			}
			do {
				if (!chain->match_.get()) {
					*tupleRef = chain->tuple_;
					nullProj->project(cxt);
				}
			}
			while ((chain = chain->next_) != NULL);
		}
	}
}

SQLJoinOps::JoinHash::TupleDigester SQLJoinOps::JoinHash::createDigester(
		util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
		const SQLValues::CompColumnList *bothKeyList,
		const SQLValues::CompColumnList *sideKeyList,
		bool nullIgnorable, SQLValues::ValueProfile *valueProfile) {
	const bool ordering = isOrderingAvailable(*bothKeyList);
	return TupleDigester(
			alloc,
			SQLValues::TupleDigester(
					*sideKeyList, &varCxt, &ordering, false, nullIgnorable, false),
			valueProfile);
}

bool SQLJoinOps::JoinHash::isOrderingAvailable(
		const SQLValues::CompColumnList &bothKeyList) {
	return SQLValues::TupleDigester::isOrderingAvailable(bothKeyList, true);
}

template<bool Matching>
SQLJoinOps::JoinHashContext<Matching>&
SQLJoinOps::JoinHash::prepareHashContext(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		const SQLOps::CompColumnListPair &sideKeyList,
		const util::Vector<TupleColumn> &innerColumnList,
		const JoinInputOrdinals &ordinals) {
	typedef JoinHashContext<Matching> HashContext;
	typedef TupleChain<Matching> Chain;

	typedef typename MapOf<Matching>::Type Map;
	typedef typename Map::iterator MapIterator;

	util::StackAllocator &alloc = cxt.getAllocator();
	const bool drivingSideFirst = true;
	const bool orderingAvailable = isOrderingAvailable(keyList);

	if (cxt.getResource(0).isEmpty()) {
		const bool innerSideFirst = !drivingSideFirst;

		util::AllocUniquePtr<SummaryTupleSet> nullTupleSet;
		if (Matching) {
			nullTupleSet = ALLOC_UNIQUE(
					alloc, SummaryTupleSet, cxt.getValueContext(), NULL);
			nullTupleSet->addReaderColumnList(innerColumnList);
			nullTupleSet->addKeyList(keyList, innerSideFirst, false);
			nullTupleSet->setOrderedDigestRestricted(!orderingAvailable);
			nullTupleSet->completeColumns();
		}

		util::AllocUniquePtr<SummaryTupleSet> tupleSet(ALLOC_UNIQUE(
				alloc, SummaryTupleSet, cxt.getValueContext(), NULL));
		tupleSet->addReaderColumnList(innerColumnList);
		tupleSet->addKeyList(keyList, innerSideFirst, false);
		tupleSet->setNullKeyIgnorable(true);
		tupleSet->setOrderedDigestRestricted(!orderingAvailable);
		tupleSet->completeColumns();

		SQLValues::CompColumnList bothKeyList(cxt.getAllocator());
		bothKeyList = keyList;
		tupleSet->applyKeyList(bothKeyList, &innerSideFirst);

		SummaryTupleSet::applyColumnList(
				tupleSet->getTotalColumnList(),
				*cxt.getSummaryColumnListRef(ordinals.inner()));

		const size_t capacity = resolveHashCapacity(cxt, ordinals);
		cxt.getResource(0) = ALLOC_UNIQUE(
				alloc, HashContext, alloc, cxt.getVarContext(),
				bothKeyList, *sideKeyList.first, tupleSet, nullTupleSet,
				capacity, cxt.getValueProfile());
	}
	HashContext &hashCxt = cxt.getResource(0).resolveAs<HashContext>();

	TupleListReader &reader = cxt.getReader(ordinals.inner());
	if (!reader.exists()) {
		return hashCxt;
	}

	SQLValues::CompColumnList nullInnerKeyList(alloc);
	util::LocalUniquePtr<TupleDigester> nullDigester;
	if (Matching) {
		nullInnerKeyList = *sideKeyList.second;
		hashCxt.nullTupleSet_->applyKeyList(nullInnerKeyList, NULL);
		nullDigester = UTIL_MAKE_LOCAL_UNIQUE(
				nullDigester, TupleDigester,
				createDigester(
						alloc, cxt.getVarContext(), &keyList, &nullInnerKeyList,
						false, cxt.getValueProfile()));
	}

	SQLValues::CompColumnList innerKeyList(alloc);
	innerKeyList = *sideKeyList.second;
	hashCxt.tupleSet_->applyKeyList(innerKeyList, NULL);

	SelfTupleEq tupleEq(
			alloc,
			SQLValues::TupleComparator(
					innerKeyList, &cxt.getVarContext(), false, true, true,
					!orderingAvailable),
			cxt.getValueProfile());
	if (tupleEq.getBase().isEmpty(0)) {
		return hashCxt;
	}

	NullChecker nullChecker(alloc, SQLValues::TupleNullChecker(innerKeyList));
	TupleDigester digester(createDigester(
			alloc, cxt.getVarContext(), &keyList, &innerKeyList,
			true, cxt.getValueProfile()));

	Map &map = hashCxt.map_;

	if (reader.exists()) {
		SQLValues::ValueUtils::updateKeepInfo(reader);
	}

	for (; reader.exists(); reader.next()) {
		if (nullChecker(ReaderSourceType(reader))) {
			if (Matching) {
				const int64_t digest = (*nullDigester)(ReaderSourceType(reader));
				const SummaryTuple tuple =
						SummaryTuple::create(*hashCxt.nullTupleSet_, reader, digest);
				hashCxt.nullChain_ =
						ALLOC_NEW(alloc) Chain(tuple, hashCxt.nullChain_);
			}
			continue;
		}

		const int64_t digest = digester(ReaderSourceType(reader));
		const SummaryTuple tuple =
				SummaryTuple::create(*hashCxt.tupleSet_, reader, digest);
		const std::pair<MapIterator, MapIterator> &range =
				map.equal_range(digest);
		for (MapIterator it = range.first;; ++it) {
			if (it == range.second) {
				map.insert(
						range.second,
						std::make_pair(digest, Chain(tuple, NULL)));
				break;
			}

			if (tupleEq(
					ReaderSourceType(reader),
					SummaryTuple::Wrapper(it->second.tuple_))) {
				it->second = Chain(tuple, ALLOC_NEW(alloc) Chain(it->second));
				break;
			}
		}
	}

	return hashCxt;
}

template<bool Matching>
inline SQLJoinOps::JoinHash::TupleChain<Matching>* SQLJoinOps::JoinHash::find(
		JoinHashContext<Matching> &hashCxt, TupleListReader &reader) {
	typedef typename MapOf<Matching>::Type Map;
	typedef typename Map::iterator MapIterator;

	if (hashCxt.nullChecker_(ReaderSourceType(reader))) {
		return NULL;
	}

	const std::pair<MapIterator, MapIterator> &range =
			hashCxt.map_.equal_range(hashCxt.digester_(ReaderSourceType(reader)));
	for (MapIterator it = range.first; it != range.second; ++it) {
		if (hashCxt.tupleEq_(
				ReaderSourceType(reader),
				SummaryTuple::Wrapper(it->second.tuple_))) {
			return &it->second;
		}
	}

	return NULL;
}

size_t SQLJoinOps::JoinHash::resolveHashCapacity(
		OpContext &cxt, const JoinInputOrdinals &ordinals) {
	const uint64_t tupleCount = cxt.getInputTupleCount(ordinals.inner());
	const uint64_t capacity = SQLValues::ValueUtils::findLargestPrimeNumber(
			tupleCount * Constants::HASH_MAP_CAPACITY_PCT / 100);
	return capacity;
}

template<bool Matching>
SQLJoinOps::JoinHash::TupleChain<Matching>::TupleChain(
		const SummaryTuple &tuple, TupleChain *next) :
		tuple_(tuple),
		next_(next) {
}

const uint64_t SQLJoinOps::JoinHash::Constants::HASH_MAP_CAPACITY_PCT = 250;


void SQLJoinOps::JoinInnerHash::execute(OpContext &cxt) const {
	const bool outer = false;
	const bool innerMatching = false;
	executeAt<outer, innerMatching>(cxt);
}


void SQLJoinOps::JoinOuterHash::execute(OpContext &cxt) const {
	const bool outer = true;

	if (Join::getOrdinals(getCode()).driving() == 0) {
		const bool innerMatching = false;
		executeAt<outer, innerMatching>(cxt);
	}
	else {
		const bool innerMatching = true;
		executeAt<outer, innerMatching>(cxt);
	}
}


SQLJoinOps::JoinOuterContext::JoinOuterContext() :
		matched_(false) {
}


SQLJoinOps::JoinMergeContext::JoinMergeContext() :
		matched_(false),
		lowerReached_(false) {
}


template<bool Matching>
SQLJoinOps::JoinHashContext<Matching>::JoinHashContext(
		util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
		const SQLValues::CompColumnList &bothKeyList,
		const SQLValues::CompColumnList &drivingKeyList,
		util::AllocUniquePtr<SummaryTupleSet> &tupleSet,
		util::AllocUniquePtr<SummaryTupleSet> &nullTupleSet,
		size_t capacity, SQLValues::ValueProfile *valueProfile) :
		bothKeyList_(bothKeyList.begin(), bothKeyList.end(), alloc),
		drivingKeyList_(drivingKeyList.begin(), drivingKeyList.end(), alloc),
		map_(capacity, JoinHash::MapHasher(), JoinHash::MapPred(), alloc),
		nextChain_(NULL),
		nullChain_(NULL),
		nullChecker_(alloc, SQLValues::TupleNullChecker(drivingKeyList_)),
		digester_(JoinHash::createDigester(
				alloc, varCxt, &bothKeyList_, &drivingKeyList_, true,
				valueProfile)),
		tupleEq_(
				alloc,
				SQLValues::TupleComparator(
						bothKeyList_, &varCxt, false, true, true,
						!JoinHash::isOrderingAvailable(bothKeyList)),
				valueProfile),
		matched_(false) {
	tupleSet_.swap(tupleSet);
	nullTupleSet_.swap(nullTupleSet);
}
