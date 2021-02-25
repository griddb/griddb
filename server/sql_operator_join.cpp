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
	add<SQLOpTypes::OP_JOIN_OUTER_NESTED, JoinOuterNested>();
	add<SQLOpTypes::OP_JOIN_OUTER_COMP_MERGE, JoinOuterCompMerge>();
	add<SQLOpTypes::OP_JOIN_HASH, JoinHash>();
}


void SQLJoinOps::Join::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	const OpCode &code = getCode();

	const uint32_t inCount = getCode().getInputCount();

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

	bool empty;
	bool small;
	const int32_t baseDrivingInput =
			detectDrivingInput(cxt, getCode(), &small, &empty);
	if (baseDrivingInput < 0) {
		cxt.setPlanPending();
		return;
	}

	const int64_t graceHashLimit = 1;

	const bool graceHashAlways = false;

	if (graceHashAlways) {
		empty = false;
		small = false;
	}

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	if (withKey && SQLValues::TupleComparator(
			builder.getExprRewriter().rewriteCompColumnList(
					builder.getExprFactoryContext(),
					*keyColumnList, false), false).isEitherSideEmpty(0) &&
			!graceHashAlways) {
		withKey = false;
		empty = true;
	}

	bool sorting = withKey;
	if (empty) {
		withKey = false;
		sorting = false;
	}
	else if (eqOnly && (
			(small && !outer) ||
			(!small && graceHashLimit <= 0))) {
		sorting = false;
	}

	int32_t drivingInput = -1;
	if (!outer || baseDrivingInput == outerDrivingInput) {
		drivingInput = baseDrivingInput;
	}
	else {
		drivingInput = outerDrivingInput;
	}

	OpNode &joinNode = plan.createNode(
			sorting ? SQLOpTypes::OP_JOIN_SORTED :
			withKey ? (small ?
					SQLOpTypes::OP_JOIN_HASH :
					SQLOpTypes::OP_JOIN_GRACE_HASH) :
			(outer ?
					SQLOpTypes::OP_JOIN_OUTER_NESTED :
					SQLOpTypes::OP_JOIN_NESTED));

	for (uint32_t i = 0; i < inCount; i++) {
		const SQLOps::OpNode *inNode = &plan.getParentInput(i);
		if (empty && static_cast<int32_t>(i) != outerDrivingInput) {
			OpCode emptyCode;
			emptyCode.setPipeProjection(
					&builder.createEmptyRefProjection(false, i, 0));
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
		OpCode joinCode = code;

		if (withKey) {
			assert(keyColumnList != NULL);
			SQLValues::CompColumnList &subKeyColumnList =
					builder.createCompColumnList();
			subKeyColumnList.insert(
					subKeyColumnList.end(),
					keyColumnList->begin(), keyColumnList->end());

			exrtactJoinKeyColumns(code.getJoinPredicate(), subKeyColumnList);
			exrtactJoinKeyColumns(code.getFilterPredicate(), subKeyColumnList);

			joinCode.setKeyColumnList(&subKeyColumnList);
			joinCode.setSideKeyColumnList(
					toSideKeyList(cxt.getAllocator(), subKeyColumnList));
		}
		else {
			joinCode.setKeyColumnList(NULL);
		}

		std::pair<Projection*, Projection*> projections;
		if (!(!sorting && withKey && !small)) {
			joinCode.setDrivingInput(drivingInput);
			projections = builder.arrangeProjections(
					joinCode, true, true, &distinctProj);
		}


		const Projection *pipeProj;
		if (!withKey && outer) {
			SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
			SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

			SQLExprs::ExprRewriter::Scope scope(rewriter);
			rewriter.activateColumnMapping(factoryCxt);

			Projection &plainPipe = builder.rewriteProjection(*projections.first);
			builder.applyJoinType(joinType);
			rewriter.setInputNull(1, true);
			rewriter.setCodeSetUpAlways(true);
			Projection &nullPipe = builder.rewriteProjection(*projections.first);
			pipeProj = &builder.createMultiStageProjection(plainPipe, nullPipe);
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
		OpContext &cxt, const OpCode &code, bool *small, bool *empty) {
	const int64_t memLimit = SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, code.getConfig());
	const uint64_t threshold =
			static_cast<uint64_t>(memLimit < 0 ? 16 * 1024 * 1024 : memLimit / 2);

	bool eitherEmpty = false;
	bool eitherSmall = false;
	bool eitherLarge = false;

	int32_t smallSide = -1;
	uint64_t sizeList[2];
	for (uint32_t i = 0; i < 2; i++) {
		sizeList[i] = cxt.getInputSize(i);

		if (sizeList[i] <= threshold) {
			if (!eitherSmall && cxt.isInputCompleted(i)) {
				if (sizeList[i] <= 0) {
					eitherEmpty = true;
				}
				eitherSmall = true;
				smallSide = static_cast<int32_t>(i);
			}
		}
		else {
			eitherLarge = true;
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
	else if (eitherSmall && eitherLarge) {
		return (smallSide != 0 ? 0 : 1);
	}
	else {
		return -1;
	}
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

void SQLJoinOps::Join::exrtactJoinKeyColumns(
		const SQLExprs::Expression *expr, SQLValues::CompColumnList &dest) {
	if (expr == NULL) {
		return;
	}

	const SQLExprs::ExprType type = expr->getCode().getType();
	if (type == SQLType::EXPR_AND) {
		const SQLExprs::Expression &left = expr->child();
		const SQLExprs::Expression &right = left.next();
		exrtactJoinKeyColumns(&left, dest);
		exrtactJoinKeyColumns(&right, dest);
	}
	else if (type == SQLType::OP_EQ) {
		const SQLExprs::Expression *left = &expr->child();
		const SQLExprs::Expression *right = &left->next();
		if (left->getCode().getType() != SQLType::EXPR_COLUMN ||
				right->getCode().getType() != SQLType::EXPR_COLUMN) {
			return;
		}
		if (left->getCode().getInput() != 0) {
			std::swap(left, right);
		}
		if (left->getCode().getInput() != 0 ||
				right->getCode().getInput() != 1) {
			return;
		}
		dest.push_back(SQLExprs::ExprTypeUtils::toCompColumn(
				type,
				left->getCode().getColumnPos(),
				right->getCode().getColumnPos(),
				false));
	}
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

	SQLOps::OpConfig &config = builder.createConfig();
	const SQLOps::OpConfig *srcConfig = getCode().getConfig();
	if (srcConfig != NULL) {
		config = *srcConfig;
	}
	config.set(SQLOpTypes::CONF_GRACE_HASH_LIMIT, 1);

	const uint32_t bucketCount = getBucketCount(cxt);
	const uint32_t inCount = 2;

	OpNode *bucketNodeList[inCount] = { NULL };
	for (uint32_t i = 0; i < inCount; i++) {
		const bool first = (i == 0);
		OpNode &node = plan.createNode(SQLOpTypes::OP_GROUP_BUCKET_HASH);

		SQLValues::CompColumnList &bucketKeyList =
				builder.createCompColumnList();
		bucketKeyList = *(first ? sideKeyList->first : sideKeyList->second);

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

	OpNode &unionNode = plan.createNode(SQLOpTypes::OP_UNION);

	for (uint32_t i = 0; i < bucketCount; i++) {
		OpNode &node = plan.createNode(SQLOpTypes::OP_JOIN);
		{
			OpCode code = getCode();
			code.setConfig(&config);
			code.setPipeProjection(joinProjections.first);
			code.setFinishProjection(joinProjections.second);
			code.setLimit(-1);
			node.setCode(code);
		}

		for (size_t j = 0; j < inCount; j++) {
			node.addInput(*bucketNodeList[j], i);
		}

		unionNode.addInput(node);
	}

	{
		OpCode code;
		code.setPipeProjection(&createUnionProjection(cxt, joinProjections));
		code.setUnionType(SQLType::UNION_ALL);
		unionNode.setCode(code);
	}

	OpNode *resultNode;
	if (aggrMode == 0) {
		resultNode = &unionNode;
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
		selectNode.addInput(unionNode);
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
	return static_cast<uint32_t>(
			std::max<uint64_t>(resolvedLimit / (1024 * 512) / 4, 1U));
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

		SQLValues::CompColumnList &outList =
				rewriter.remapCompColumnList(factoryCxt, midList, i, true);
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

	const Comparator comp(
			cxt.getAllocator(),
			TupleRangeComparator(getCode().getKeyColumnList()));

	if (!reader2.exists()) {
		return;
	}
	reader2Lower.exists(); 

	const SQLOps::Projection *proj = getCode().getPipeProjection();
	bool lowerAssignable = false;
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}

		bool lowerReached = false;
		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}

			const int32_t ret =
					comp(ReaderSourceType(reader1), ReaderSourceType(reader2));

			if (!lowerReached) {
				if (ret > 0) {
					continue; 
				}
				if (lowerAssignable) {
					reader2Lower.assign(reader2);
				}
				lowerReached = true;
			}

			if (ret < 0) {
				break; 
			}

			proj->project(cxt);
		}

		reader2.assign(reader2Lower);
		lowerAssignable = true;
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

	const SQLOps::Projection &proj = getCode().getPipeProjection()->chainAt(0);
	const SQLOps::Projection &nullProj = getCode().getPipeProjection()->chainAt(1);
	const SQLExprs::Expression *joinPred = getCode().getJoinPredicate();
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}
		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}

			if (joinPred != NULL && !SQLValues::ValueUtils::isTrue(
					joinPred->eval(cxt.getExprContext()))) {
				continue;
			}

			proj.project(cxt);
			outerCxt.matched_ = true;
		}

		if (outerCxt.matched_) {
			outerCxt.matched_ = false;
		}
		else {
			nullProj.project(cxt);
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
		cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), JoinOuterContext);
	}
	JoinOuterContext &outerCxt = cxt.getResource(0).resolveAs<JoinOuterContext>();

	const Comparator comp(
			cxt.getAllocator(),
			TupleRangeComparator(getCode().getKeyColumnList()));

	const SQLOps::Projection &proj = getCode().getPipeProjection()->chainAt(0);
	const SQLOps::Projection &nullProj = getCode().getPipeProjection()->chainAt(1);
	const SQLExprs::Expression *joinPred = getCode().getJoinPredicate();
	bool lowerAssignable = false;
	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}

		bool lowerReached = false;
		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}

			const int32_t ret =
					comp(ReaderSourceType(reader1), ReaderSourceType(reader2));

			if (!lowerReached) {
				if (ret > 0) {
					continue; 
				}
				if (lowerAssignable) {
					reader2Lower.assign(reader2);
				}
				lowerReached = true;
			}

			if (ret < 0) {
				break; 
			}

			if (joinPred != NULL && !SQLValues::ValueUtils::isTrue(
					joinPred->eval(cxt.getExprContext()))) {
				continue;
			}

			proj.project(cxt);
			outerCxt.matched_ = true;
		}

		if (outerCxt.matched_) {
			outerCxt.matched_ = false;
		}
		else {
			nullProj.project(cxt);
		}

		if (reader2Lower.exists()) { 
			reader2.assign(reader2Lower);
			lowerAssignable = true;
		}
	}
}


void SQLJoinOps::JoinHash::compile(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	cxt.setReaderRandomAccess(ordinals.inner());
	cxt.setInputSourceType(
			ordinals.inner(), SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE);

	Operator::compile(cxt);
}

void SQLJoinOps::JoinHash::execute(OpContext &cxt) const {
	const JoinInputOrdinals &ordinals = Join::getOrdinals(getCode());
	TupleListReader &reader1 = cxt.getReader(ordinals.driving());
	TupleListReader &reader2 = cxt.getReader(ordinals.inner());
	cxt.keepReaderLatch(ordinals.inner());

	JoinHashContext &hashCxt = prepareHashContext(
			cxt, Join::resolveKeyList(cxt.getAllocator(), getCode()),
			Join::resolveSideKeyList(getCode()), reader2,
			cxt.getInputColumnList(ordinals.inner()));
	if (!Join::checkJoinReady(cxt, ordinals)) {
		return;
	}

	*cxt.getSummaryColumnListRef(ordinals.inner()) =
			hashCxt.tupleSet_->getReaderColumnList();

	const SQLOps::Projection *proj = getCode().getPipeProjection();

	TupleChain *chain = hashCxt.nextChain_;
	hashCxt.nextChain_ = NULL;

	SummaryTuple *tupleRef =
			cxt.getExprContext().getSummaryTupleRef(ordinals.inner());

	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}

		if (chain == NULL) {
			chain = find(hashCxt, reader1);
			if (chain == NULL) {
				continue;
			}
		}

		do {
			if (cxt.checkSuspended()) { 
				hashCxt.nextChain_ = chain;
				return;
			}

			*tupleRef = chain->tuple_;
			proj->project(cxt);
		}
		while ((chain = chain->next_) != NULL);
	}
}

SQLJoinOps::JoinHash::TupleDigester SQLJoinOps::JoinHash::createDigester(
		util::StackAllocator &alloc,
		const SQLValues::CompColumnList *bothKeyList,
		const SQLValues::CompColumnList *sideKeyList) {
	const bool ordering = SQLValues::TupleDigester::isOrderingAvailable(
			*bothKeyList, true);
	return TupleDigester(
			alloc, SQLValues::TupleDigester(*sideKeyList, &ordering, true));
}

SQLJoinOps::JoinHashContext& SQLJoinOps::JoinHash::prepareHashContext(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		const SQLOps::CompColumnListPair &sideKeyList,
		TupleListReader &reader,
		const util::Vector<TupleColumn> &innerColumnList) {
	util::StackAllocator &alloc = cxt.getAllocator();
	const bool drivingSideFirst = true;

	if (cxt.getResource(0).isEmpty()) {
		const bool innerSideFirst = !drivingSideFirst;

		util::AllocUniquePtr<SummaryTupleSet> tupleSet(ALLOC_UNIQUE(
				alloc, SummaryTupleSet, cxt.getValueContext(), NULL));
		tupleSet->addReaderColumnList(innerColumnList);
		tupleSet->addKeyList(keyList, innerSideFirst);
		tupleSet->setNullKeyIgnorable(true);
		tupleSet->completeColumns();

		SQLValues::CompColumnList bothKeyList(cxt.getAllocator());
		bothKeyList = keyList;
		tupleSet->applyKeyList(bothKeyList, &innerSideFirst);

		cxt.getResource(0) = ALLOC_UNIQUE(
				alloc, JoinHashContext, alloc, bothKeyList, *sideKeyList.first,
				tupleSet);
	}
	JoinHashContext &hashCxt = cxt.getResource(0).resolveAs<JoinHashContext>();

	if (!reader.exists()) {
		return hashCxt;
	}

	SQLValues::CompColumnList innerKeyList(alloc);
	innerKeyList = *sideKeyList.second;
	hashCxt.tupleSet_->applyKeyList(innerKeyList, NULL);

	SelfTupleEq tupleEq(
			alloc, SQLValues::TupleComparator(innerKeyList, false, true, true));
	if (tupleEq.getBase().isEmpty(0)) {
		return hashCxt;
	}

	NullChecker nullChecker(alloc, SQLValues::TupleNullChecker(innerKeyList));
	TupleDigester digester(createDigester(alloc, &keyList, &innerKeyList));

	JoinHash::Map &map = hashCxt.map_;

	for (; reader.exists(); reader.next()) {
		if (nullChecker(ReaderSourceType(reader))) {
			continue;
		}

		const int64_t digest = digester(ReaderSourceType(reader));
		const SummaryTuple tuple =
				SummaryTuple::create(*hashCxt.tupleSet_, reader, digest);
		const std::pair<Map::iterator, Map::iterator> &range =
				map.equal_range(digest);
		for (Map::iterator it = range.first;; ++it) {
			if (it == range.second) {
				map.insert(
						range.second,
						std::make_pair(digest, TupleChain(tuple, NULL)));
				break;
			}

			if (tupleEq(
					ReaderSourceType(reader),
					SummaryTuple::Wrapper(it->second.tuple_))) {
				it->second = TupleChain(tuple, ALLOC_NEW(alloc) TupleChain(it->second));
				break;
			}
		}
	}

	return hashCxt;
}

inline SQLJoinOps::JoinHash::TupleChain* SQLJoinOps::JoinHash::find(
		JoinHashContext &hashCxt, TupleListReader &reader) {
	if (hashCxt.nullChecker_(ReaderSourceType(reader))) {
		return NULL;
	}

	const std::pair<Map::iterator, Map::iterator> &range =
			hashCxt.map_.equal_range(hashCxt.digester_(ReaderSourceType(reader)));
	for (Map::iterator it = range.first; it != range.second; ++it) {
		if (hashCxt.tupleEq_(
				ReaderSourceType(reader),
				SummaryTuple::Wrapper(it->second.tuple_))) {
			return &it->second;
		}
	}

	return NULL;
}

SQLJoinOps::JoinHash::TupleChain::TupleChain(
		const SummaryTuple &tuple, TupleChain *next) :
		tuple_(tuple),
		next_(next) {
}


SQLJoinOps::JoinHashContext::JoinHashContext(
		util::StackAllocator &alloc,
		const SQLValues::CompColumnList &bothKeyList,
		const SQLValues::CompColumnList &drivingKeyList,
		util::AllocUniquePtr<SummaryTupleSet> &tupleSet) :
		bothKeyList_(bothKeyList.begin(), bothKeyList.end(), alloc),
		drivingKeyList_(drivingKeyList.begin(), drivingKeyList.end(), alloc),
		map_(0, JoinHash::MapHasher(), JoinHash::MapPred(), alloc),
		nextChain_(NULL),
		nullChecker_(alloc, SQLValues::TupleNullChecker(drivingKeyList_)),
		digester_(
				JoinHash::createDigester(alloc, &bothKeyList_, &drivingKeyList_)),
		tupleEq_(alloc, SQLValues::TupleComparator(
				bothKeyList_, false, true, true)) {
	tupleSet_.swap(tupleSet);
}
