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

#include "sql_operator_utils.h"

SQLOps::OpCodeBuilder::OpCodeBuilder(const Source &source) :
		factoryCxt_(source.alloc_),
		rewriter_(source.alloc_) {
	setUpExprFactoryContext(source.cxt_);
}

SQLOps::OpCodeBuilder::Source SQLOps::OpCodeBuilder::ofAllocator(
		util::StackAllocator &alloc) {
	return Source(alloc);
}

SQLOps::OpCodeBuilder::Source SQLOps::OpCodeBuilder::ofContext(
		OpContext &cxt) {
	Source source(cxt.getAllocator());
	source.cxt_ = &cxt;
	return source;
}

SQLOps::ProjectionFactoryContext&
SQLOps::OpCodeBuilder::getProjectionFactoryContext() {
	return factoryCxt_;
}

SQLExprs::ExprFactoryContext& SQLOps::OpCodeBuilder::getExprFactoryContext() {
	return getProjectionFactoryContext().getExprFactoryContext();
}

SQLExprs::ExprRewriter& SQLOps::OpCodeBuilder::getExprRewriter() {
	return rewriter_;
}

void SQLOps::OpCodeBuilder::applyJoinType(SQLType::JoinType type) {
	applyJoinTypeTo(type, getExprFactoryContext());
}

void SQLOps::OpCodeBuilder::applyJoinTypeTo(
		SQLType::JoinType type, SQLExprs::ExprFactoryContext &cxt) {
	switch (type) {
	case SQLType::JOIN_INNER:
		break;
	case SQLType::JOIN_LEFT_OUTER:
		cxt.setInputNullable(1, true);
		break;
	case SQLType::JOIN_RIGHT_OUTER:
		cxt.setInputNullable(0, true);
		break;
	case SQLType::JOIN_FULL_OUTER:
		cxt.setInputNullable(0, true);
		cxt.setInputNullable(1, true);
		break;
	case SQLType::END_JOIN:
		break;
	default:
		assert(false);
		break;
	}
}

SQLOps::OpConfig& SQLOps::OpCodeBuilder::createConfig() {
	return *(ALLOC_NEW(getAllocator()) OpConfig());
}

SQLOps::ContainerLocation& SQLOps::OpCodeBuilder::createContainerLocation() {
	return *(ALLOC_NEW(getAllocator()) ContainerLocation());
}

SQLExprs::IndexConditionList& SQLOps::OpCodeBuilder::createIndexConditionList() {
	return *(ALLOC_NEW(getAllocator()) SQLExprs::IndexConditionList(
			getAllocator()));
}

SQLValues::CompColumnList& SQLOps::OpCodeBuilder::createCompColumnList() {
	return *(ALLOC_NEW(getAllocator()) SQLValues::CompColumnList(
			getAllocator()));
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createProjection(
		SQLOpTypes::ProjectionType projType, const ProjectionCode &code) {
	if (projType != SQLOpTypes::END_PROJ) {
		ProjectionCode destCode = code;
		destCode.setType(projType);
		return createProjection(SQLOpTypes::END_PROJ, destCode);
	}

	return getProjectionFactory().create(getProjectionFactoryContext(), code);
}

SQLOps::Expression& SQLOps::OpCodeBuilder::createExpression(
		SQLExprs::ExprType exprType, const SQLExprs::ExprCode &code) {
	if (exprType != SQLType::END_EXPR) {
		SQLExprs::ExprCode destCode = code;
		destCode.setType(exprType);
		return createExpression(SQLType::END_EXPR, destCode);
	}

	return getExprFactory().create(getExprFactoryContext(), code);
}

SQLExprs::Expression& SQLOps::OpCodeBuilder::createConstExpr(
		const TupleValue &value) {
	return SQLExprs::ExprRewriter::createConstExpr(
			getExprFactoryContext(), value);
}

SQLExprs::Expression& SQLOps::OpCodeBuilder::createColumnExpr(
		uint32_t input, uint32_t column) {
	return SQLExprs::ExprRewriter::createColumnExpr(
			getExprFactoryContext(), input, column);
}

SQLOps::Projection& SQLOps::OpCodeBuilder::toNormalProjectionByExpr(
		const Expression *src, SQLType::AggregationPhase aggrPhase) {
	if (src == NULL) {
		return createIdenticalProjection(false, 0);
	}

	SQLExprs::ExprFactoryContext &cxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(cxt);
	cxt.setAggregationPhase(true, aggrPhase);

	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT);
	rewriter_.rewrite(cxt, *src, &proj);

	return proj;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::toGroupingProjectionByExpr(
		const Expression *src, SQLType::AggregationPhase aggrPhase,
		bool forWindow) {
	if (src == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	SQLExprs::ExprFactoryContext &cxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(cxt);
	cxt.setAggregationPhase(true, aggrPhase);

	ProjectionCode code;

	uint32_t attributes = SQLExprs::ExprCode::ATTR_GROUPING;
	if (forWindow) {
		attributes |= SQLExprs::ExprCode::ATTR_WINDOWING;
	}
	code.getExprCode().setAttributes(attributes);

	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT, code);
	rewriter_.rewrite(getExprFactoryContext(), *src, &proj);

	return proj;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createIdenticalProjection(
		bool unified, uint32_t index) {
	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT);

	SQLExprs::ExprRewriter::createIdenticalProjection(
			getExprFactoryContext(), unified, index, proj);
	rewriter_.remapColumn(getExprFactoryContext(), proj);

	return proj;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createEmptyRefProjection(
		bool unified, uint32_t index, uint32_t startColumn) {
	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT);

	SQLExprs::ExprRewriter::createEmptyRefProjection(
			getExprFactoryContext(), unified, index, startColumn, proj);
	rewriter_.remapColumn(getExprFactoryContext(), proj);

	return proj;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createProjectionByUsage(
		bool unified) {
	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT);

	rewriter_.createProjectionByUsage(getExprFactoryContext(), unified, proj);

	SQLExprs::ExprCode &exprCode = proj.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	return proj;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::rewriteProjection(
		const Projection &src, Projection **distinctProj) {
	if (distinctProj != NULL) {
		*distinctProj = NULL;
	}

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

	const SQLOps::Projection *checked;
	if (((src.getProjectionCode().getExprCode().getAttributes() &
			SQLExprs::ExprCode::ATTR_ASSIGNED) == 0) &&
			!factoryCxt.isPlanning()) {
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
		factoryCxt.setPlanning(true);
		checked = &rewriteProjection(src);
	}
	else {
		checked = &src;
	}

	applySummaryAggregationColumns(src);

	SQLOps::Projection *dest;
	{
		ProjectionCode srcCode = src.getProjectionCode();

		const SQLValues::CompColumnList *keyList = srcCode.getKeyList();
		if (keyList != NULL) {
			const bool unified = ((srcCode.getExprCode().getAttributes() &
					SQLExprs::ExprCode::ATTR_COLUMN_UNIFIED) != 0);
			srcCode.setKeyList(&rewriter_.rewriteCompColumnList(
					getExprFactoryContext(), *keyList, unified));
		}

		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
		factoryCxt.setBaseExpression(checked);
		dest = &createProjection(SQLOpTypes::END_PROJ, srcCode);
	}

	SQLOps::Projection *distinctDest = NULL;
	{
		const bool aggrArranging = (factoryCxt.getAggregationPhase(false) !=
				SQLType::END_AGG_PHASE);

		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

		const ProjectionCode &srcCode = src.getProjectionCode();

		const SQLType::AggregationPhase srcAggrPhase =
				srcCode.getAggregationPhase();
		if (srcAggrPhase != SQLType::END_AGG_PHASE) {
			factoryCxt.setAggregationPhase(false, srcAggrPhase);
		}

		if (srcCode.getColumnTypeList() != NULL) {
			factoryCxt.setAggregationTypeListRef(srcCode.getColumnTypeList());
		}

		factoryCxt.setBaseExpression(&src);
		rewriter_.rewrite(factoryCxt, src, dest);

		const bool distinct = findDistinctAggregation(src);
		if (distinct && aggrArranging) {
			distinctDest = &createProjection(SQLOpTypes::PROJ_OUTPUT);
			rewriter_.popLastDistinctAggregationOutput(
					factoryCxt, *distinctDest);
		}
	}

	ProjectionCode *code = &dest->getProjectionCode();

	const SQLType::AggregationPhase aggrPhase =
			factoryCxt.getAggregationPhase(false);
	if (aggrPhase != SQLType::END_AGG_PHASE &&
			code->getType() == SQLOpTypes::PROJ_OUTPUT) {
		assert(factoryCxt.isPlanning());

		const bool forFinish = (
				aggrPhase == SQLType::AGG_PHASE_ADVANCE_FINISH ||
				aggrPhase == SQLType::AGG_PHASE_MERGE_FINISH);

		code->setAggregationPhase(aggrPhase);

		if (forFinish) {
			if ((dest->getCode().getAttributes() &
					SQLExprs::ExprCode::ATTR_WINDOWING) == 0) {
				SQLOps::Projection &outProj = *dest;

				dest = &createProjection(SQLOpTypes::PROJ_AGGR_OUTPUT);
				dest->addChain(outProj);
			}
		}
		else {
			code->setType(SQLOpTypes::PROJ_AGGR_PIPE);
		}

		ColumnTypeList *typeList =
				ALLOC_NEW(getAllocator()) ColumnTypeList(getAllocator());
		const uint32_t count = factoryCxt.getAggregationColumnCount();
		for (uint32_t i = 0; i < count; i++) {
			typeList->push_back(factoryCxt.getAggregationColumnType(i));
		}
		code->setColumnTypeList(typeList);

		ProjectionCode *destCode = &dest->getProjectionCode();
		if (code != destCode) {
			destCode->setColumnTypeList(typeList);
		}

		if (distinctDest != NULL) {
			if (forFinish) {
				if (distinctProj == NULL) {
					assert(false);
					GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
				}
				*distinctProj = distinctDest;
			}
			else {
				distinctDest->getPlanningCode().setOutput(1);
				dest = &createMultiOutputProjection(*dest, *distinctDest);
			}
		}
	}
	else {
		const bool arranging = factoryCxt.isSummaryColumnsArranging();
		factoryCxt.setSummaryColumnsArranging(false);

		for (Projection::ChainIterator it(src); it.exists(); it.next()) {
			dest->addChain(rewriteProjection(it.get()));
		}

		factoryCxt.setSummaryColumnsArranging(arranging);
	}

	return *dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createFilteredProjection(
		Projection &src, OpCode &code) {
	const Expression *filterPred = code.getFilterPredicate();
	if (filterPred == NULL) {
		return src;
	}

	code.setFilterPredicate(NULL);
	return createFilteredProjection(src, filterPred);
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createFilteredProjection(
		Projection &src, const Expression *filterPred) {
	if (filterPred == NULL) {
		return src;
	}

	SQLOps::Projection &dest = getProjectionFactory().create(
			getProjectionFactoryContext(), SQLOpTypes::PROJ_FILTER);

	dest.addChild(&arrangePredicate(*filterPred));
	dest.addChain(src);

	SQLExprs::ExprCode &exprCode = dest.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	return dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createKeyFilteredProjection(
		Projection &src) {
	SQLOps::Projection &dest = getProjectionFactory().create(
			getProjectionFactoryContext(), SQLOpTypes::PROJ_KEY_FILTER);
	dest.addChain(src);

	SQLExprs::ExprCode &exprCode = dest.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	return dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createLimitedProjection(
		Projection &src, OpCode &code) {
	const int64_t limit = code.getLimit();
	const int64_t offset = code.getOffset();
	if (limit < 0 && offset <= 0) {
		return src;
	}

	code.setLimit(-1);
	code.setOffset(-1);

	SQLOps::Projection &dest = createProjection(SQLOpTypes::PROJ_LIMIT);

	dest.addChild(&createConstExpr(
			SQLValues::ValueUtils::toAnyByNumeric(limit)));
	dest.addChild(&createConstExpr(
			SQLValues::ValueUtils::toAnyByNumeric(offset)));

	dest.addChain(src);

	return dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createMultiOutputProjection(
		Projection &proj1, Projection &proj2) {
	SQLOps::Projection &dest = getProjectionFactory().create(
			getProjectionFactoryContext(), SQLOpTypes::PROJ_MULTI_OUTPUT);
	dest.addChain(proj1);
	dest.addChain(proj2);

	SQLExprs::ExprCode &exprCode = dest.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	return dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createPipeFinishProjection(
		Projection &pipe, Projection &finish) {
	SQLOps::Projection &dest = getProjectionFactory().create(
			getProjectionFactoryContext(), SQLOpTypes::PROJ_PIPE_FINISH);
	dest.addChain(pipe);
	dest.addChain(finish);

	SQLExprs::ExprCode &exprCode = dest.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	return dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createMultiStageProjection(
		Projection &pipe, Projection &mid) {
	SQLOps::Projection &dest = getProjectionFactory().create(
			getProjectionFactoryContext(), SQLOpTypes::PROJ_MULTI_STAGE);
	dest.addChain(pipe);
	dest.addChain(mid);

	SQLExprs::ExprCode &exprCode = dest.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	return dest;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createMultiStageAggregation(
		const Projection &src, int32_t stage, bool forFinish,
		const SQLValues::CompColumnList *keyList) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

	SQLExprs::ExprRewriter &rewriter = getExprRewriter();
	SQLExprs::ExprRewriter::Scope rewriterScope(rewriter);

	rewriter.setMultiStageGrouping(true);
	rewriter.setInputMiddle((stage >= 1));
	rewriter.setOutputMiddle((stage <= 1 && stage >= 0));

	const SQLType::AggregationPhase srcPhase =
			factoryCxt.getAggregationPhase(true);

	SQLType::AggregationPhase destPhase;
	switch (srcPhase) {
	case SQLType::AGG_PHASE_ALL_PIPE:
		destPhase = (forFinish ?
				SQLType::AGG_PHASE_MERGE_FINISH :
				SQLType::AGG_PHASE_ADVANCE_PIPE);
		break;
	case SQLType::AGG_PHASE_ADVANCE_PIPE:
		destPhase = (forFinish ?
				SQLType::AGG_PHASE_ADVANCE_FINISH :
				SQLType::AGG_PHASE_ADVANCE_PIPE);
		break;
	case SQLType::AGG_PHASE_MERGE_PIPE:
		destPhase = (forFinish ?
				SQLType::AGG_PHASE_MERGE_FINISH :
				SQLType::AGG_PHASE_MERGE_PIPE);
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (!forFinish && (stage >= 1 || stage < 0)) {
		destPhase = SQLType::AGG_PHASE_MERGE_PIPE;
	}
	else if (forFinish && stage <= 1) {
		destPhase = SQLType::AGG_PHASE_ADVANCE_FINISH;
	}

	factoryCxt.setAggregationPhase(false, destPhase);
	SQLOps::Projection *proj = &rewriteProjection(src);

	if (forFinish) {
		assert(proj->getProjectionCode().getType() ==
				SQLOpTypes::PROJ_AGGR_OUTPUT);
		SQLOps::Projection &outProj = proj->chainAt(0);

		outProj.getProjectionCode().setKeyList(keyList);
	}
	assert(forFinish || keyList == NULL);

	return *proj;
}

std::pair<SQLOps::Projection*, SQLOps::Projection*>
SQLOps::OpCodeBuilder::arrangeProjections(
		OpCode &code, bool withFilter, bool withLimit,
		Projection **distinctProj) {
	if (distinctProj != NULL) {
		*distinctProj = NULL;
	}

	const Projection *srcPipe = code.getPipeProjection();
	const Projection *srcFinish = code.getFinishProjection();
	assert(srcPipe != NULL);

	code.setPipeProjection(NULL);
	code.setFinishProjection(NULL);

	Projection *destPipe = NULL;
	Projection *destFinish = NULL;
	if (isAggregationArrangementRequired(srcPipe, srcFinish)) {
		SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

		SQLType::AggregationPhase pipePhase;
		SQLType::AggregationPhase finishPhase;
		const SQLType::AggregationPhase orgPhase = code.getAggregationPhase();
		switch (orgPhase) {
		case SQLType::AGG_PHASE_ALL_PIPE:
			pipePhase = SQLType::AGG_PHASE_ADVANCE_PIPE;
			finishPhase = SQLType::AGG_PHASE_MERGE_FINISH;
			break;
		case SQLType::AGG_PHASE_ADVANCE_PIPE:
			pipePhase = SQLType::AGG_PHASE_ADVANCE_PIPE;
			finishPhase = SQLType::AGG_PHASE_ADVANCE_FINISH;
			break;
		case SQLType::AGG_PHASE_MERGE_PIPE:
			pipePhase = SQLType::AGG_PHASE_MERGE_PIPE;
			finishPhase = SQLType::AGG_PHASE_MERGE_FINISH;
			break;
		default:
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		{
			SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
			factoryCxt.setAggregationPhase(true, orgPhase);
			factoryCxt.setAggregationPhase(false, pipePhase);
			destPipe = &rewriteProjection(*srcPipe);
		}
		{
			SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
			factoryCxt.setAggregationPhase(true, orgPhase);
			factoryCxt.setAggregationPhase(false, finishPhase);
			destFinish = &rewriteProjection(*srcPipe, distinctProj);
		}
	}
	else {
		destPipe = &rewriteProjection(*srcPipe);
		if (srcFinish != NULL) {
			destFinish = &rewriteProjection(*srcFinish);
		}
	}

	if (withLimit) {
		Projection *&target = (distinctProj != NULL && *distinctProj != NULL ?
				*distinctProj : (destFinish == NULL ? destPipe : destFinish));
		target = &createLimitedProjection(*target, code);
	}

	if (withFilter) {
		destPipe = &createFilteredProjection(*destPipe, code);
	}

	return std::make_pair(destPipe, destFinish);
}

SQLExprs::Expression& SQLOps::OpCodeBuilder::arrangePredicate(
		const SQLExprs::Expression &src) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

	if (factoryCxt.getAggregationPhase(true) == SQLType::END_AGG_PHASE) {
		factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ADVANCE_PIPE);
	}

	return rewriter_.rewrite(factoryCxt, src, NULL);
}

void SQLOps::OpCodeBuilder::setUpOutputNodes(
		OpPlan &plan, OpNode *outNode, OpNode &inNode,
		const Projection *distinctProj, SQLType::AggregationPhase aggrPhase) {
	if (distinctProj == NULL) {
		if (outNode == NULL) {
			plan.linkToAllParentOutput(inNode);
		}
		else {
			outNode->addInput(inNode);
		}
		return;
	}

	if (outNode == NULL) {
		assert(plan.getParentOutputCount() == 1);
		setUpOutputNodes(
				plan, &plan.getParentOutput(0), inNode, distinctProj,
				aggrPhase);
		return;
	}

	OpNode &distinctNode = plan.createNode(SQLOpTypes::OP_GROUP_DISTINCT);
	distinctNode.addInput(inNode, 0);
	distinctNode.addInput(inNode, 1);

	OpCode code;
	code.setPipeProjection(distinctProj);
	code.setAggregationPhase(aggrPhase);
	distinctNode.setCode(code);

	outNode->addInput(distinctNode);
}

SQLOps::OpCode SQLOps::OpCodeBuilder::toExecutable(const OpCode &src) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
	factoryCxt.setPlanning(false);

	return rewriteCode(src, true);
}

SQLOps::OpCode SQLOps::OpCodeBuilder::rewriteCode(
		const OpCode &src, bool projectionArranging) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
	SQLOps::OpCode dest = src;

	const SQLExprs::ExprCode::InputSourceType orgInType =
			factoryCxt.getInputSourceType(0);

	{
		const SQLValues::CompColumnList *list = src.findMiddleKeyColumnList();
		if (list != NULL) {
			dest.setMiddleKeyColumnList(&rewriter_.rewriteCompColumnList(
					factoryCxt, *list, isInputUnified(dest)));
		}
	}

	{
		const Projection *srcProj = src.getMiddleProjection();
		if (srcProj != NULL) {
			if (orgInType == SQLExprs::ExprCode::INPUT_MULTI_STAGE) {
				factoryCxt.setInputSourceType(
						0, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE);
			}

			SQLValues::ValueContext valueCxt(
					SQLValues::ValueContext::ofAllocator(getAllocator()));
			SummaryTupleSet midAggrTupleSet(valueCxt, NULL);

			const Projection *destProj;
			{
				SQLExprs::ExprFactoryContext::Scope midScope(factoryCxt);

				factoryCxt.setArrangedKeyList(dest.findMiddleKeyColumnList());
				factoryCxt.setAggregationTupleSet(&midAggrTupleSet);

				destProj = &rewriteProjection(*srcProj);
			}
			dest.setMiddleProjection(destProj);
			assignInputTypesByProjection(*destProj, NULL);
		}
	}

	if (orgInType == SQLExprs::ExprCode::INPUT_MULTI_STAGE) {
		factoryCxt.setInputSourceType(
				0, SQLExprs::ExprCode::INPUT_READER_MULTI);
	}

	{
		const SQLValues::CompColumnList *list = src.findKeyColumnList();
		if (list != NULL) {
			dest.setKeyColumnList(&rewriter_.rewriteCompColumnList(
					factoryCxt, *list, isInputUnified(dest)));
		}
	}
	factoryCxt.setArrangedKeyList(dest.findKeyColumnList());

	{
		const SQLOps::CompColumnListPair *list = src.findSideKeyColumnList();
		if (list != NULL) {
			const uint32_t firstInput = 0;
			const uint32_t secondInput = 1;
			const SQLOps::CompColumnListPair destList(
					&rewriter_.rewriteCompColumnList(
							factoryCxt, *list->first, isInputUnified(dest),
							&firstInput),
					&rewriter_.rewriteCompColumnList(
							factoryCxt, *list->second, isInputUnified(dest),
							&secondInput));
			dest.setSideKeyColumnList(
					ALLOC_NEW(getAllocator()) SQLOps::CompColumnListPair(
							destList));
		}
	}

	{
		ProjectionPair projs;
		if (projectionArranging) {
			projs = arrangeProjections(dest, false, false, NULL);
		}
		else {
			ProjectionRefPair projRefs(
					dest.getPipeProjection(), dest.getFinishProjection());
			for (size_t i = 0; i < 2; i++) {
				const Projection *srcProj = (i == 0 ? projRefs.first : projRefs.second);
				Projection *&destProj = (i == 0 ? projs.first : projs.second);
				if (srcProj != NULL) {
					destProj = &rewriteProjection(*srcProj);
				}
			}
		}
		dest.setPipeProjection(projs.first);
		dest.setFinishProjection(projs.second);
	}

	{
		const Expression *expr = src.getFilterPredicate();
		if (expr != NULL) {
			dest.setFilterPredicate(&arrangePredicate(*expr));
		}
	}
	{
		const Expression *expr = src.getJoinPredicate();
		if (expr != NULL) {
			dest.setJoinPredicate(&arrangePredicate(*expr));
		}
	}

	if (orgInType == SQLExprs::ExprCode::INPUT_MULTI_STAGE) {
		factoryCxt.setInputSourceType(0, orgInType);
	}

	return dest;
}

void SQLOps::OpCodeBuilder::applySummaryAggregationColumns(
		const Projection &proj) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

	if (factoryCxt.isPlanning() || !factoryCxt.isSummaryColumnsArranging()) {
		return;
	}

	SummaryTupleSet *aggrTupleSet = factoryCxt.getAggregationTupleSet();
	assert(aggrTupleSet != NULL);

	const Projection *aggrProj = findAggregationProjection(proj);
	const SQLValues::CompColumnList *keyList = factoryCxt.getArrangedKeyList();

	if (aggrProj != NULL && !aggrTupleSet->isColumnsCompleted()) {
		const uint32_t input = 0;
		if (keyList != NULL &&
				factoryCxt.getInputSourceType(input) ==
						SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE &&
				factoryCxt.getInputCount() == 1) {
			const uint32_t count = factoryCxt.getInputColumnCount(input);
			for (uint32_t i = 0; i < count; i++) {
				aggrTupleSet->addColumn(
						factoryCxt.getInputType(input, i), &i);
			}
			aggrTupleSet->addKeyList(*keyList, true);
		}

		applyAggregationColumns(*aggrProj, *aggrTupleSet);
		aggrTupleSet->completeColumns();
	}
	while (false);

	const uint32_t inCount = factoryCxt.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		if (factoryCxt.getInputSourceType(i) !=
				SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE) {
			continue;
		}

		SummaryColumnList *summaryColumns =
				factoryCxt.getSummaryColumnListRef(i);
		const size_t lastCount = summaryColumns->size();

		if (aggrProj != NULL && factoryCxt.getInputCount() == 1) {
			assert(aggrTupleSet->isColumnsCompleted());
			*summaryColumns = aggrTupleSet->getTotalColumnList();
		}
		else {
			*summaryColumns = rewriter_.createSummaryColumnList(
					factoryCxt, i, false, (i == 0), keyList);
		}
		assert(lastCount == 0 || summaryColumns->size() == lastCount);
		static_cast<void>(lastCount);
	}
}

void SQLOps::OpCodeBuilder::applyAggregationColumns(
		const Projection &aggrProj, SummaryTupleSet &tupleSet) {
	assert(findAggregationProjection(aggrProj) == &aggrProj);

	const ColumnTypeList *aggrTypeList =
			aggrProj.getProjectionCode().getColumnTypeList();
	assert(aggrTypeList != NULL);

	for (ColumnTypeList::const_iterator it = aggrTypeList->begin();
			it != aggrTypeList->end(); ++it) {
		tupleSet.addColumn(*it, NULL);
	}
}

void SQLOps::OpCodeBuilder::addColumnUsage(const OpCode &code, bool withId) {
	assert(code.findKeyColumnList() == NULL);
	assert(code.getJoinPredicate() == NULL);
	assert(code.getMiddleProjection() == NULL);
	assert(code.getFinishProjection() == NULL);

	rewriter_.addColumnUsage(code.getPipeProjection(), withId);
	rewriter_.addColumnUsage(code.getFilterPredicate(), withId);
}

void SQLOps::OpCodeBuilder::assignInputTypesByProjection(
		const Projection &proj, const uint32_t *outIndex) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

	ColumnTypeList typeList(getAllocator());
	resolveColumnTypeList(proj, typeList, outIndex);

	factoryCxt.clearInputTypes();

	const uint32_t index = 0;
	const size_t columnCount = typeList.size();
	for (uint32_t pos = 0; pos < columnCount; pos++) {
		factoryCxt.setInputType(index, pos, typeList[pos]);
		factoryCxt.setInputNullable(index, false);
	}
}

bool SQLOps::OpCodeBuilder::isInputUnified(const OpCode &code) {
	return (code.getUnionType() != SQLType::END_UNION);
}

const SQLExprs::Expression* SQLOps::OpCodeBuilder::findFilterPredicate(
		const Projection &src) {
	if (src.getProjectionCode().getType() == SQLOpTypes::PROJ_FILTER) {
		return src.findChild();
	}

	for (Projection::ChainIterator it(src); it.exists(); it.next()) {
		const Expression *pred = findFilterPredicate(it.get());
		if (pred != NULL) {
			return pred;
		}
	}

	return NULL;
}

const SQLOps::Projection* SQLOps::OpCodeBuilder::findOutputProjection(
		const OpCode &code, const uint32_t *outIndex) {
	const Projection *proj = code.getFinishProjection();

	if (proj != NULL) {
		proj = findOutputProjection(*proj, outIndex);
	}

	if (proj == NULL) {
		proj = code.getPipeProjection();
		if (proj != NULL) {
			proj = findOutputProjection(*proj, outIndex);
		}
	}

	return proj;
}

const SQLOps::Projection* SQLOps::OpCodeBuilder::findOutputProjection(
		const Projection &src, const uint32_t *outIndex) {
	if (matchOutputProjectionAt(src, outIndex)) {
		return &src;
	}

	for (Projection::ChainIterator it(src); it.exists(); it.next()) {
		const SQLOps::Projection *foundProj =
				findOutputProjection(it.get(), outIndex);
		if (foundProj != NULL) {
			return foundProj;
		}
	}

	return NULL;
}

SQLOps::Projection* SQLOps::OpCodeBuilder::findOutputProjection(
		Projection &src, const uint32_t *outIndex) {
	if (matchOutputProjectionAt(src, outIndex)) {
		return &src;
	}

	for (Projection::ChainModIterator it(src); it.exists(); it.next()) {
		SQLOps::Projection *foundProj =
				findOutputProjection(it.get(), outIndex);
		if (foundProj != NULL) {
			return foundProj;
		}
	}

	return NULL;
}

bool SQLOps::OpCodeBuilder::matchOutputProjectionAt(
		const Projection &proj, const uint32_t *outIndex) {
	if (proj.getProjectionCode().getType() != SQLOpTypes::PROJ_OUTPUT) {
		return false;
	}

	if (outIndex == NULL) {
		return (resolveOutputIndex(proj) == 0);
	}

	return (*outIndex == resolveOutputIndex(proj));
}

uint32_t SQLOps::OpCodeBuilder::resolveOutputIndex(const Projection &proj) {
	assert(proj.getProjectionCode().getType() == SQLOpTypes::PROJ_OUTPUT);
	return resolveOutputIndex(proj.getCode());
}

uint32_t SQLOps::OpCodeBuilder::resolveOutputIndex(
		const SQLExprs::ExprCode &code) {
	const uint32_t outIndex = code.getOutput();

	if (outIndex == std::numeric_limits<uint32_t>::max()) {
		return 0;
	}
	return outIndex;
}

const SQLOps::Projection* SQLOps::OpCodeBuilder::findAggregationProjection(
		const Projection &src) {
	const SQLOpTypes::ProjectionType type = src.getProjectionCode().getType();
	if (type == SQLOpTypes::PROJ_AGGR_PIPE ||
			type == SQLOpTypes::PROJ_AGGR_OUTPUT) {
		return &src;
	}

	for (Projection::ChainIterator it(src); it.exists(); it.next()) {
		const SQLOps::Projection *foundProj = findAggregationProjection(it.get());
		if (foundProj != NULL) {
			return foundProj;
		}
	}

	return NULL;
}

bool SQLOps::OpCodeBuilder::findDistinctAggregation(const Projection &src) {
	return findDistinctAggregation(getExprFactoryContext().getFactory(), src);
}

bool SQLOps::OpCodeBuilder::findDistinctAggregation(
		const SQLExprs::ExprFactory &factory, const Projection &src) {
	if (SQLExprs::ExprRewriter::findDistinctAggregation(factory, src)) {
		return true;
	}

	for (Projection::ChainIterator it(src); it.exists(); it.next()) {
		if (findDistinctAggregation(factory, it.get())) {
			return true;
		}
	}

	return false;
}

bool SQLOps::OpCodeBuilder::isAggregationArrangementRequired(
		const Projection *pipeProj, const Projection *finishProj) {
	return (finishProj == NULL &&
			pipeProj->getProjectionCode().getType() ==
					SQLOpTypes::PROJ_OUTPUT &&
			(pipeProj->getCode().getAttributes() &
					SQLExprs::ExprCode::ATTR_AGGR_ARRANGED) == 0 &&
			(pipeProj->getCode().getAttributes() &
					(SQLExprs::ExprCode::ATTR_AGGREGATED |
					SQLExprs::ExprCode::ATTR_GROUPING)) != 0);
}

void SQLOps::OpCodeBuilder::resolveColumnTypeList(
		const OpCode &code, ColumnTypeList &typeList,
		const uint32_t *outIndex) {
	const Projection *proj = findOutputProjection(code, outIndex);

	if (proj == NULL && outIndex != NULL && *outIndex > 0 &&
			resolveOutputCount(code) == 1) {
		const uint32_t topOutIndex = 0;
		resolveColumnTypeList(code, typeList, &topOutIndex);
		return;
	}

	if (proj == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
	resolveColumnTypeList(*proj, typeList, outIndex);
}

void SQLOps::OpCodeBuilder::resolveColumnTypeList(
		const Projection &proj, ColumnTypeList &typeList,
		const uint32_t *outIndex) {
	assert(typeList.empty());
	const Projection *outProj = findOutputProjection(proj, outIndex);
	if (outProj == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
	for (SQLExprs::Expression::Iterator it(*outProj); it.exists(); it.next()) {
		const TupleColumnType type = it.get().getCode().getColumnType();
		typeList.push_back(type);
		if (SQLValues::TypeUtils::isNull(type)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
		}
	}
	if (typeList.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
}

uint32_t SQLOps::OpCodeBuilder::resolveOutputCount(const OpCode &code) {
	uint32_t maxCount = 0;
	for (size_t i = 0; i < 2; i++) {
		const Projection *proj =
				(i == 0 ? code.getPipeProjection() : code.getFinishProjection());
		if (proj == NULL) {
			continue;
		}

		const uint32_t subCount = resolveOutputCount(*proj);
		maxCount = static_cast<uint32_t>(std::max(maxCount, subCount));
	}
	return maxCount;
}

uint32_t SQLOps::OpCodeBuilder::resolveOutputCount(const Projection &proj) {
	if (proj.getProjectionCode().getType() == SQLOpTypes::PROJ_OUTPUT) {
		return resolveOutputIndex(proj) + 1;
	}

	uint32_t maxCount = 0;
	for (Projection::ChainIterator it(proj); it.exists(); it.next()) {
		const uint32_t subCount = resolveOutputCount(it.get());
		maxCount = static_cast<uint32_t>(std::max(maxCount, subCount));
	}

	return maxCount;
}

void SQLOps::OpCodeBuilder::removeUnificationAttributes(Projection &proj) {
	SQLExprs::ExprRewriter::removeUnificationAttributes(proj);

	const size_t count = proj.getChainCount();
	for (size_t i = 0; i < count; i++) {
		removeUnificationAttributes(proj.chainAt(i));
	}
}

SQLOpUtils::ExprRefList SQLOps::OpCodeBuilder::getDistinctExprList(
		const Projection &proj) {
	ExprRefList exprList(getAllocator());

	const Projection *outProj = findOutputProjection(proj, NULL);
	if (outProj != NULL) {
		SQLExprs::ExprRewriter::getDistinctExprList(
				getExprFactoryContext().getFactory(), *outProj, exprList);
	}

	return exprList;
}

SQLValues::CompColumnList& SQLOps::OpCodeBuilder::createDistinctGroupKeyList(
		const ExprRefList &distinctList, size_t index, bool idGrouping) {
	SQLValues::CompColumnList &keyList = createCompColumnList();

	{
		SQLValues::CompColumn key;
		key.setColumnPos(0, true);
		keyList.push_back(key);
	}

	if (!idGrouping) {
		SQLValues::CompColumn key;
		const SQLExprs::Expression *expr = distinctList[index];
		key.setColumnPos(expr->child().getCode().getColumnPos(), true);
		keyList.push_back(key);
	}

	return keyList;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createDistinctGroupProjection(
		const ExprRefList &distinctList, size_t index, bool idGrouping,
		SQLType::AggregationPhase aggrPhase) {
	static_cast<void>(aggrPhase);

	ProjectionCode code;
	code.getExprCode().setAttributes(SQLExprs::ExprCode::ATTR_GROUPING);

	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT, code);

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::Expression::ModIterator projIt(proj);

	const uint32_t input = 0;
	projIt.append(
			SQLExprs::ExprRewriter::createIdRefColumnExpr(factoryCxt, input, 0));

	const SQLExprs::Expression *expr = distinctList[index];
	if (idGrouping) {
		assert(aggrPhase != SQLType::AGG_PHASE_ADVANCE_PIPE);
		uint32_t refCoulmnPos = 1;
		SQLExprs::Expression &nonDistinctExpr =
				SQLExprs::ExprRewriter::toNonDistinctExpr(
						factoryCxt, *expr, input, &refCoulmnPos);
		projIt.append(nonDistinctExpr);
	}
	else {
		SQLExprs::ExprRewriter::addDistinctRefColumnExprs(
				factoryCxt, *expr, true, false, input, NULL, projIt);
	}

	return proj;
}

SQLValues::CompColumnList&
SQLOps::OpCodeBuilder::createDistinctMergeKeyList() {
	SQLValues::CompColumnList &keyList = createCompColumnList();

	SQLValues::CompColumn key;
	key.setColumnPos(0, true);
	key.setColumnPos(0, false);
	keyList.push_back(key);

	return keyList;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createDistinctMergeProjection(
		const ExprRefList &distinctList, size_t index, const Projection &proj,
		SQLType::AggregationPhase aggrPhase) {
	SQLOps::Projection &mainProj = createDistinctMergeProjectionAt(
			distinctList, index, proj, aggrPhase, -1);
	SQLOps::Projection &emptyProj = createMultiStageProjection(
			createDistinctMergeProjectionAt(
					distinctList, index, proj, aggrPhase, 1),
			createDistinctMergeProjectionAt(
					distinctList, index, proj, aggrPhase, 0));
	return createMultiStageProjection(mainProj, emptyProj);
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createDistinctMergeProjectionAt(
		const ExprRefList &distinctList, size_t index, const Projection &proj,
		SQLType::AggregationPhase aggrPhase, int32_t emptySide) {
	const bool forAdvance = (aggrPhase == SQLType::AGG_PHASE_ADVANCE_PIPE);

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	if (index + 1 < distinctList.size()) {
		Projection &destProj = (emptySide == 0 ?
				createEmptyRefProjection(false, 0, 1) :
				createIdenticalProjection(false, 0));

		SQLExprs::Expression::ModIterator projIt(destProj);
		while (projIt.exists()) {
			projIt.next();
		}

		uint32_t lastCount = factoryCxt.getInputColumnCount(0);
		for (size_t i = 0; i <= index; i++) {
			const bool merged = (i < index);
			const bool emptyRef = (emptySide >= 0 ?
					(emptySide == 0 ? merged : !merged) : false);
			const Expression *expr = distinctList[index];

			const uint32_t input = (merged ? 0 : 1);
			const uint32_t refPosBase = (merged ? lastCount : 1);

			uint32_t refPos = refPosBase;
			SQLExprs::ExprRewriter::addDistinctRefColumnExprs(
					factoryCxt, *expr, forAdvance, emptyRef, input, &refPos,
					projIt);

			lastCount += (refPos - refPosBase);
		}
		return destProj;
	}
	else {
		SQLOps::Projection &destProj = rewriteProjection(proj);
		SQLOps::Projection *outProj = findOutputProjection(destProj, NULL);
		if (outProj != NULL) {
			SQLExprs::ExprRewriter::replaceDistinctExprsToRef(
					factoryCxt, *outProj, forAdvance, emptySide,
					NULL, NULL);
		}
		return destProj;
	}
}

void SQLOps::OpCodeBuilder::setUpExprFactoryContext(OpContext *cxt) {
	if (cxt == NULL) {
		return;
	}

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	const uint32_t inCount = cxt->getInputCount();
	for (uint32_t index = 0; index < inCount; index++) {
		const uint32_t columnCount = cxt->getColumnCount(index);
		for (uint32_t pos = 0; pos < columnCount; pos++) {
			factoryCxt.setInputType(
					index, pos, cxt->getReaderColumn(index, pos).getType());
			factoryCxt.setInputNullable(index, false);
		}
		factoryCxt.setReadableTupleRef(index, cxt->getReadableTupleRef(index));
		factoryCxt.setSummaryTupleRef(index, cxt->getSummaryTupleRef(index));
		factoryCxt.setSummaryColumnListRef(
				index, cxt->getSummaryColumnListRef(index));
	}

	factoryCxt.setActiveReaderRef(cxt->getActiveReaderRef());
	factoryCxt.setAggregationTupleRef(
			cxt->getDefaultAggregationTupleRef());
	factoryCxt.setAggregationTupleSet(
			cxt->getDefaultAggregationTupleSet());
}

util::StackAllocator& SQLOps::OpCodeBuilder::getAllocator() {
	return getExprFactoryContext().getAllocator();
}

const SQLOps::ProjectionFactory&
SQLOps::OpCodeBuilder::getProjectionFactory() {
	return getProjectionFactoryContext().getFactory();
}

const SQLExprs::ExprFactory& SQLOps::OpCodeBuilder::getExprFactory() {
	return getExprFactoryContext().getFactory();
}

SQLOps::OpCodeBuilder::Source::Source(util::StackAllocator &alloc) :
		alloc_(alloc),
		cxt_(NULL) {
}


SQLOps::OpLatchKeeperManager::OpLatchKeeperManager(
		const util::StdAllocator<void, void> &alloc, bool composite) :
		alloc_(alloc),
		composite_(composite),
		subList_(alloc),
		idManager_(alloc),
		entryList_(alloc),
		waiterQueue_(alloc),
		hotCount_(0) {
	if (composite) {
		mutex_ = UTIL_MAKE_LOCAL_UNIQUE(mutex_, util::Mutex);
	}
}

SQLOps::OpLatchKeeperManager::~OpLatchKeeperManager() {
	while (!subList_.empty()) {
		ALLOC_DELETE(alloc_, subList_.back());
		subList_.pop_back();
	}
	waiterQueue_.clear();
	entryList_.clear();
}

SQLOps::OpLatchKeeperManager&
SQLOps::OpLatchKeeperManager::getSubManager(OpContext &cxt) {
	assert(composite_);

	const uint32_t workerId = cxt.getExtContext().getTotalWorkerId();

	util::LockGuard<util::Mutex> guard(*mutex_);
	if (workerId >= subList_.size()) {
		subList_.resize(workerId + 1);
	}

	OpLatchKeeperManager *&sub = subList_[workerId];
	if (sub == NULL) {
		util::StdAllocator<void, void> subAlloc =
				cxt.getValueContext().getVarAllocator();
		sub = ALLOC_NEW(alloc_) OpLatchKeeperManager(subAlloc, false);
	}

	return *sub;
}

SQLOps::OpLatchKeeperManager::KeeperId
SQLOps::OpLatchKeeperManager::create(uint64_t maxSize, bool hot) {
	assert(!composite_);

	KeeperId id;
	id.id_.assign(idManager_);

	const size_t idIndex = id.id_.getIndex(idManager_);
	if (idIndex >= entryList_.size()) {
		entryList_.resize(idIndex + 1);
	}

	Entry &entry = entryList_[id.id_.getIndex(idManager_)];
	entry.id_ = id;
	entry.size_ = maxSize;
	entry.hot_ = hot;

	waiterQueue_.push_back(id.id_);
	return id;
}

void SQLOps::OpLatchKeeperManager::release(const KeeperId &id) {
	assert(!composite_);

	Entry &entry = entryList_[id.id_.getIndex(idManager_)];

	if (!(id.id_ == entry.id_.id_)) {
		assert(false);
		return;
	}

	{
		IdQueue::iterator it =
				std::find(waiterQueue_.begin(), waiterQueue_.end(), id.id_);
		if (it != waiterQueue_.end()) {
			waiterQueue_.erase(it);
		}
	}

	if (entry.acquired_) {
		usage_.used_ -= entry.size_;
		if (entry.hot_) {
			hotCount_--;
		}
		else {
			coldUsage_.used_ -= entry.size_;
		}
	}

	entry = Entry();
}

bool SQLOps::OpLatchKeeperManager::tryAcquire(const KeeperId &id) {
	assert(!composite_);

	Entry &entry = entryList_[id.id_.getIndex(idManager_)];
	if (entry.acquired_) {
		return true;
	}

	IdQueue::iterator it = waiterQueue_.begin();
	for (;;) {
		if (it == waiterQueue_.end()) {
			assert(false);
			return false;
		}

		Entry &subEntry = entryList_[it->getIndex(idManager_)];
		if (id.id_ == subEntry.id_.id_) {
			break;
		}
		else if (!entry.hot_ || subEntry.hot_) {
			return false;
		}
	}

	if (entry.hot_) {
		if (hotCount_ > 0 &&
				usage_.used_ + entry.size_ > usage_.limit_) {
			return false;
		}
		hotCount_++;
	}
	else {
		if (coldUsage_.used_ + entry.size_ > coldUsage_.limit_) {
			return false;
		}
		coldUsage_.used_ += entry.size_;
	}

	waiterQueue_.erase(it);
	usage_.used_ += entry.size_;
	entry.acquired_ = true;
	return true;
}

void SQLOps::OpLatchKeeperManager::adjust(const KeeperId &id, uint64_t size) {
	assert(!composite_);

	Entry &entry = entryList_[id.id_.getIndex(idManager_)];
	if (size < entry.size_) {
		entry.size_ = size;
	}
}


SQLOps::OpLatchKeeperManager::Entry::Entry() :
		hot_(false),
		acquired_(false),
		size_(0) {
}


SQLOps::OpLatchKeeperManager::Usage::Usage() :
		used_(0),
		limit_(0) {
}


SQLOps::OpLatchKeeper::OpLatchKeeper(
		OpLatchKeeperManager &manager, uint64_t maxSize, bool hot) :
		id_(manager.create(maxSize, hot)),
		manager_(manager) {
}

SQLOps::OpLatchKeeper::~OpLatchKeeper() {
	manager_.release(id_);
}

bool SQLOps::OpLatchKeeper::tryAcquire() {
	return manager_.tryAcquire(id_);
}

void SQLOps::OpLatchKeeper::adjust(uint64_t size) {
	manager_.adjust(id_, size);
}


SQLOps::OpProfiler::OpProfiler(const util::StdAllocator<void, void> &alloc) :
		alloc_(alloc),
		idManager_(alloc),
		infoList_(alloc) {
}

SQLOps::OpProfilerId SQLOps::OpProfiler::getSubId(
		const OpProfilerId *id, SQLOpTypes::Type type) {
	OpProfilerId baseId;
	if (id == NULL) {
		if (infoList_.empty()) {
			createInfo(SQLOpTypes::END_OP);
		}
		baseId = infoList_.front().id_;
	}
	else {
		baseId = *id;
	}

	const size_t baseIndex = baseId.id_.getIndex(idManager_);
	assert((id == NULL) == (baseIndex == 0));

	SubMap *subMap = &infoList_[baseIndex].sub_;
	SubMap::iterator subIt = subMap->find(type);
	if (subIt == subMap->end()) {
		LocalInfo &info = createInfo(type);
		subMap = &infoList_[baseIndex].sub_;
		subMap->insert(std::make_pair(type, info.id_));
		return info.id_;
	}
	else {
		return subIt->second;
	}
}

void SQLOps::OpProfiler::addOperatorProfile(
		const OpProfilerId &id, const AnalysisInfo &info) {
	LocalInfo &dest = infoList_[id.id_.getIndex(idManager_)];

	const bool first = dest.ref_.empty();
	if (first) {
		dest.ref_.push_back(AnalysisInfo());
	}

	AnalysisInfo &ref = dest.ref_.front();
	ref.opCount_ += (first ? 0 : (ref.opCount_ < 0 ? 2 : 1));
	ref.executionCount_ += info.executionCount_;
	ref.actualNanoTime_ += info.actualNanoTime_;

	if (info.index_ != NULL) {
		for (util::AllocVector<AnalysisIndexInfo>::iterator it =
				info.index_->begin(); it != info.index_->end(); ++it) {
			addIndexProfile(id, *it);
		}
	}
}

void SQLOps::OpProfiler::addIndexProfile(
		const OpProfilerId &id, const AnalysisIndexInfo &info) {
	LocalInfo &opDest = infoList_[id.id_.getIndex(idManager_)];

	opDest.index_.push_back(LocalIndexInfo(alloc_));
	LocalIndexInfo &dest = opDest.index_.back();

	if (info.name_ != NULL) {
		dest.name_ = *info.name_;
	}

	if (info.columnList_ != NULL) {
		for (NameRefList::iterator it = info.columnList_->begin();
				it != info.columnList_->end(); ++it) {
			dest.columnNameList_.push_back(
					util::AllocString((*it)->c_str(), alloc_));
		}
	}

	if (info.conditionList_ != NULL) {
		for (IndexCondList::iterator it = info.conditionList_->begin();
				it != info.conditionList_->end(); ++it) {
			dest.conditionColumnList_.push_back(
					util::AllocString(it->column_->c_str(), alloc_));

			dest.conditionListRef_.push_back(AnalysisIndexCondition());
			dest.conditionListRef_.back().condition_ = it->condition_;
		}
	}

	dest.ref_.push_back(AnalysisIndexInfo());
	AnalysisIndexInfo &ref = dest.ref_.back();

	ref.executionCount_ = info.executionCount_;
	ref.actualTime_ = info.actualTime_;
}

SQLOpUtils::AnalysisInfo SQLOps::OpProfiler::getAnalysisInfo(
		const OpProfilerId *id) {
	if (id == NULL) {
		if (infoList_.empty()) {
			return AnalysisInfo();
		}
		return getAnalysisInfo(&infoList_.front().id_);
	}

	LocalInfo &info = infoList_[id->id_.getIndex(idManager_)];
	if (info.ref_.empty()) {
		return AnalysisInfo();
	}

	return getAnalysisInfo(this, info);
}

SQLOpUtils::AnalysisInfo SQLOps::OpProfiler::getAnalysisInfo(
		OpProfiler *profiler, LocalInfo &src) {
	AnalysisInfo &ref = src.ref_.front();

	ref.index_ = NULL;
	LocalIndexInfoList &indexList = src.index_;
	if (!indexList.empty()) {
		src.indexRef_.clear();
		for (LocalIndexInfoList::iterator it = indexList.begin();
				it != indexList.end(); ++it) {
			src.indexRef_.push_back(getAnalysisIndexInfo(*it));
		}
		ref.index_ = &src.indexRef_;
	}

	ref.sub_ = NULL;
	SubMap &subMap = src.sub_;
	if (!subMap.empty()) {
		assert(profiler != NULL);
		src.subRef_.clear();
		for (SubMap::iterator it = subMap.begin(); it != subMap.end(); ++it) {
			src.subRef_.push_back(profiler->getAnalysisInfo(&it->second));
		}
		ref.sub_ = &src.subRef_;
	}

	ref.actualTime_ =
			static_cast<int64_t>(nanoTimeToMillis(ref.actualNanoTime_));

	return ref;
}

SQLOpUtils::AnalysisIndexInfo SQLOps::OpProfiler::getAnalysisIndexInfo(
		LocalIndexInfo &src) {
	if (src.ref_.empty()) {
		return AnalysisIndexInfo();
	}

	AnalysisIndexInfo dest = src.ref_.front();

	if (!src.name_.empty()) {
		dest.name_ = &src.name_;
	}

	dest.columnList_ = NULL;
	NameList &columnList = src.columnNameList_;
	if (!columnList.empty()) {
		src.columnNameListRef_.clear();
		for (NameList::iterator it = columnList.begin();
				it != columnList.end(); ++it) {
			src.columnNameListRef_.push_back(&(*it));
		}
		dest.columnList_ = &src.columnNameListRef_;
	}

	dest.conditionList_ = NULL;
	IndexCondList &condList = src.conditionListRef_;
	if (!condList.empty()) {
		for (IndexCondList::iterator it = condList.begin();
				it != condList.end(); ++it) {
			it->column_ = &src.columnNameList_[it - condList.begin()];
		}
		dest.conditionList_ = &src.conditionListRef_;
	}

	return dest;
}

uint64_t SQLOps::OpProfiler::nanoTimeToMillis(uint64_t nanoTime) {
	return nanoTime / 1000 / 1000;
}

SQLOps::OpProfiler::LocalInfo& SQLOps::OpProfiler::createInfo(
		SQLOpTypes::Type type) {
	OpProfilerId nextId;
	nextId.id_.assign(idManager_);

	const size_t idIndex = nextId.id_.getIndex(idManager_);
	if (idIndex >= infoList_.size()) {
		infoList_.resize(idIndex + 1, LocalInfo(alloc_));
	}

	LocalInfo &info = infoList_[idIndex];

	info.id_ = nextId;
	info.ref_.push_back(AnalysisInfo());
	info.ref_.front().type_ = type;

	return info;
}


SQLOps::OpProfiler::LocalInfo::LocalInfo(
		const util::StdAllocator<void, void> &alloc) :
		index_(alloc),
		sub_(alloc),
		indexRef_(alloc),
		subRef_(alloc),
		ref_(alloc) {
}


SQLOps::OpProfiler::LocalIndexInfo::LocalIndexInfo(
		const util::StdAllocator<void, void> &alloc) :
		name_(alloc),
		columnNameList_(alloc),
		conditionColumnList_(alloc),
		columnNameListRef_(alloc),
		conditionListRef_(alloc),
		ref_(alloc) {
}


SQLOps::OpProfilerEntry::OpProfilerEntry(
		const util::StdAllocator<void, void> &alloc, const OpProfilerId &id) :
		executionCount_(0),
		alloc_(alloc),
		localInfo_(alloc) {
	localInfo_.id_ = id;
	localInfo_.ref_.push_back(AnalysisInfo());
}

void SQLOps::OpProfilerEntry::startExec() {
	executionWatch_.start();
}

void SQLOps::OpProfilerEntry::finishExec() {
	executionWatch_.stop();
	++executionCount_;
}

const SQLOps::OpProfilerId& SQLOps::OpProfilerEntry::getId() {
	return localInfo_.id_;
}

SQLOpUtils::AnalysisInfo SQLOps::OpProfilerEntry::get() {
	localInfo_.ref_.front().executionCount_ = executionCount_;
	localInfo_.ref_.front().actualNanoTime_ = executionWatch_.elapsedNanos();

	if (index_.get() != NULL) {
		index_->update();
	}

	return OpProfiler::getAnalysisInfo(NULL, localInfo_);
}

SQLOps::OpProfilerIndexEntry& SQLOps::OpProfilerEntry::getIndexEntry() {
	if (index_.get() == NULL) {
		if (localInfo_.index_.empty()) {
			localInfo_.index_.push_back(LocalIndexInfo(alloc_));
		}
		LocalIndexInfo *localIndexInfo = &localInfo_.index_[0];
		index_ = ALLOC_UNIQUE(
				alloc_, OpProfilerIndexEntry, alloc_, localIndexInfo);
	}
	return *index_;
}


SQLOps::OpProfilerIndexEntry::OpProfilerIndexEntry(
		const util::StdAllocator<void, void> &alloc, LocalIndexInfo *localInfo) :
		executionCount_(0),
		localInfo_(localInfo),
		alloc_(alloc),
		nameMap_(alloc) {
}

bool SQLOps::OpProfilerIndexEntry::isEmpty() {
	return nameMap_.empty();
}

const char8_t* SQLOps::OpProfilerIndexEntry::findIndexName() {
	const util::AllocString &name = localInfo_->name_;
	if (name.empty()) {
		return NULL;
	}
	return name.c_str();
}

void SQLOps::OpProfilerIndexEntry::setIndexName(const char8_t *name) {
	localInfo_->name_ = name;
}

const char8_t* SQLOps::OpProfilerIndexEntry::findColumnName(
		uint32_t column) {
	ColumnNameMap::const_iterator it = nameMap_.find(column);
	if (it == nameMap_.end() || it->second.empty()) {
		return NULL;
	}
	return it->second.c_str();
}

void SQLOps::OpProfilerIndexEntry::setColumnName(
		uint32_t column, const char8_t *name) {
	nameMap_.insert(std::make_pair(column, util::AllocString(name, alloc_)));
}

void SQLOps::OpProfilerIndexEntry::addIndexColumn(uint32_t column) {
	OpProfiler::NameList &nameList = localInfo_->columnNameList_;

	nameList.push_back(util::AllocString(alloc_));

	ColumnNameMap::const_iterator nameIt = nameMap_.find(column);
	if (nameIt != nameMap_.end()) {
		nameList.back() = nameIt->second.c_str();
	}
}

void SQLOps::OpProfilerIndexEntry::addCondition(
		const SQLExprs::IndexCondition &cond) {
	AnalysisIndexCondition destCond;
	switch (cond.opType_) {
	case SQLType::EXPR_BETWEEN:
		destCond.condition_ = AnalysisIndexCondition::CONDITION_RANGE;
		break;
	case SQLType::OP_EQ:
		destCond.condition_ = AnalysisIndexCondition::CONDITION_EQ;
		break;
	default:
		return;
	}

	OpProfiler::IndexCondList &condList = localInfo_->conditionListRef_;
	const size_t condIndex = condList.size();

	condList.push_back(destCond);

	OpProfiler::NameList &nameList = localInfo_->conditionColumnList_;
	if (condIndex >= nameList.size()) {
		nameList.resize(condIndex + 1, util::AllocString(alloc_));
	}

	ColumnNameMap::const_iterator nameIt = nameMap_.find(cond.column_);
	if (nameIt != nameMap_.end()) {
		nameList[condIndex] = nameIt->second.c_str();
	}
}

void SQLOps::OpProfilerIndexEntry::startSearch() {
	searchWatch_.start();
}

void SQLOps::OpProfilerIndexEntry::finishSearch() {
	searchWatch_.stop();
	++executionCount_;
}

void SQLOps::OpProfilerIndexEntry::update() {
	if (localInfo_->ref_.empty()) {
		localInfo_->ref_.push_back(AnalysisIndexInfo());
	}

	AnalysisIndexInfo &ref = localInfo_->ref_.front();
	ref.executionCount_ = executionCount_;
	ref.actualTime_ = static_cast<int64_t>(
			OpProfiler::nanoTimeToMillis(searchWatch_.elapsedNanos()));
}


const util::NameCoderEntry<SQLOpTypes::Type>
		SQLOpUtils::AnalysisInfo::OP_TYPE_LIST[] = {
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_FULL),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_INDEX),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_UPDATES),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_META),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SELECT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SELECT_PIPE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SELECT_FINISH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_LIMIT),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SORT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SORT_NWAY),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW_PARTITION),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW_MERGE),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_GRACE_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_SORTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_NESTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_COMP_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER_NESTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER_COMP_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_HASH),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_DISTINCT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_DISTINCT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_BUCKET_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_ALL),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_SORTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_DISTINCT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_INTERSECT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_EXCEPT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_COMPENSATE_MERGE),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_PARENT_INPUT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_PARENT_OUTPUT)
};

const util::NameCoder<SQLOpTypes::Type, SQLOpTypes::END_OP>
		SQLOpUtils::AnalysisInfo::OP_TYPE_CODER(OP_TYPE_LIST, 1);

SQLOpUtils::AnalysisInfo::AnalysisInfo() :
		type_(),
		opCount_(-1),
		executionCount_(0),
		actualTime_(0),
		index_(NULL),
		sub_(NULL),
		actualNanoTime_(0) {
}

SQLOpUtils::AnalysisRootInfo SQLOpUtils::AnalysisInfo::toRoot() const {
	AnalysisRootInfo dest;
	dest.sub_ = sub_;
	return dest;
}


SQLOpUtils::AnalysisRootInfo::AnalysisRootInfo() :
		sub_(NULL) {
}


const util::NameCoderEntry<SQLOpUtils::AnalysisIndexCondition::ConditionType>
		SQLOpUtils::AnalysisIndexCondition::CONDITION_LIST[] = {
	UTIL_NAME_CODER_ENTRY(CONDITION_RANGE),
	UTIL_NAME_CODER_ENTRY(CONDITION_EQ)
};

const util::NameCoder<
		SQLOpUtils::AnalysisIndexCondition::ConditionType,
		SQLOpUtils::AnalysisIndexCondition::END_CONDITION>
		SQLOpUtils::AnalysisIndexCondition::CONDITION_CODER(	CONDITION_LIST, 1);

SQLOpUtils::AnalysisIndexCondition::AnalysisIndexCondition() :
		column_(NULL),
		condition_(END_CONDITION) {
}


SQLOpUtils::AnalysisIndexInfo::AnalysisIndexInfo() :
		name_(NULL),
		columnList_(NULL),
		conditionList_(NULL),
		executionCount_(0),
		actualTime_(0) {
}


SQLOpUtils::CustomOpProjectionRegistrar::CustomOpProjectionRegistrar() throw() {
}

SQLOpUtils::CustomOpProjectionRegistrar::CustomOpProjectionRegistrar(
		const CustomOpProjectionRegistrar &sub) throw() :
		SQLOps::OpProjectionRegistrar(sub) {
}


SQLOpUtils::ExpressionWriter::ExpressionWriter() :
		func_(NULL),
		reader_(NULL),
		readerRef_(NULL),
		inTuple_(NULL),
		inSummaryTuple_(NULL),
		inSummaryColumn_(NULL),
		outTuple_(NULL),
		expr_(NULL),
		cxtRef_(NULL),
		srcType_(SQLExprs::ExprCode::END_INPUT) {
}

void SQLOpUtils::ExpressionWriter::initialize(
		ExprFactoryContext &cxt, const Expression &expr,
		const TupleColumn &outColumn, ContextRef *cxtRef,
		TupleListReader **readerRef, WritableTuple *outTuple) {
	typedef SQLExprs::ExprCode ExprCode;

	readerRef_ = readerRef;
	inTuple_ = NULL;
	inSummaryTuple_ = NULL;
	outTuple_ = outTuple;
	outColumn_ = outColumn;
	expr_ = &expr;
	cxtRef_ = cxtRef;

	const ExprCode &code = expr.getCode();

	const uint32_t input =
			(code.getType() == SQLType::AGG_FIRST ? 0 : code.getInput());
	const uint32_t column = code.getColumnPos();

	const SQLExprs::ExprCode::InputSourceType srcType =
			cxt.getInputSourceType(input);
	const bool forColumn = (
			code.getType() == SQLType::EXPR_COLUMN &&
			(code.getAttributes() &
					SQLExprs::ExprCode::ATTR_COLUMN_UNIFIED) == 0 &&
			(srcType == SQLExprs::ExprCode::INPUT_READER ||
					srcType == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE ||
					srcType == SQLExprs::ExprCode::INPUT_READER_MULTI) &&
			SQLValues::TypeUtils::toNonNullable(outColumn.getType()) ==
					SQLValues::TypeUtils::toNonNullable(
							cxt.getInputType(input, column)));
	const bool forSimpleAggr = (
			!forColumn &&
			code.getType() == SQLType::AGG_FIRST &&
			expr.findChild() == NULL &&
			(srcType == SQLExprs::ExprCode::INPUT_READER ||
					srcType == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE ||
					srcType == SQLExprs::ExprCode::INPUT_READER_MULTI));

	if (forColumn) {
		inColumn_ = cxt.getInputColumn(input, column);
		srcType_ = srcType;
	}
	else {
		inColumn_ = TupleColumn();
		srcType_ = SQLExprs::ExprCode::END_INPUT;
	}

	if (forColumn) {
		if (srcType == SQLExprs::ExprCode::INPUT_READER) {
			cxt.addReaderRef(input, &reader_);
		}
		else if (srcType == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE) {
			inSummaryTuple_ = cxt.getSummaryTupleRef(input);
			assert(inSummaryTuple_ != NULL);

			inSummaryColumn_ = &(*cxt.getSummaryColumnListRef(input))[column];
			if (inSummaryColumn_->isOnBody()) {
				srcType_ = SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY;
			}
		}
	}
	else if (forSimpleAggr) {
		const uint32_t aggrIndex = code.getAggregationIndex();
		assert(aggrIndex != std::numeric_limits<uint32_t>::max());

		if (srcType == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE &&
				cxt.getInputCount() == 1) {
			inSummaryTuple_ = cxt.getSummaryTupleRef(input);
			assert(inSummaryTuple_ != NULL);

			const uint32_t aggrColumn = cxt.getInputColumnCount(input) + aggrIndex;
			inSummaryColumn_ =
					&(*cxt.getSummaryColumnListRef(input))[aggrColumn];
		}
		else {
			inSummaryTuple_ = cxt.getAggregationTupleRef();
			assert(inSummaryTuple_ != NULL);

			SummaryTupleSet *tupleSet = cxt.getAggregationTupleSet();
			assert(tupleSet != NULL);

			inSummaryColumn_ =
					&tupleSet->getModifiableColumnList()[aggrIndex];
		}

		if (inSummaryColumn_->isOnBody()) {
			srcType_ = SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY;
		}
		else {
			srcType_ = SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE;
		}
	}

	func_ = resolveFunc<const ExpressionWriter>();
}

void SQLOpUtils::ExpressionWriter::setExpression(const Expression &expr) {
	expr_ = &expr;
}

bool SQLOpUtils::ExpressionWriter::isForColumn() const {
	return (srcType_ != SQLExprs::ExprCode::END_INPUT);
}

bool SQLOpUtils::ExpressionWriter::isSummaryColumn() const {
	return (srcType_ == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE ||
			srcType_ == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY);
}

bool SQLOpUtils::ExpressionWriter::matchIdColumn() const {
	return (expr_ != NULL && expr_->getCode().getType() == SQLType::EXPR_ID);
}

bool SQLOpUtils::ExpressionWriter::matchCompColumn(
		const SQLValues::CompColumn &compColumn) const {
	if (expr_ == NULL || !isForColumn()) {
		return false;
	}

	if (expr_->getCode().getColumnPos() != compColumn.getColumnPos(true)) {
		return false;
	}

	return true;
}

TupleList::Column SQLOpUtils::ExpressionWriter::getOutColumn() const {
	return outColumn_;
}

const SQLOpUtils::SummaryColumn*
SQLOpUtils::ExpressionWriter::getInSummaryColumn() const {
	return inSummaryColumn_;
}

template<typename Op>
SQLOpUtils::ExpressionWriter::Switcher::DefaultFuncType
SQLOpUtils::ExpressionWriter::resolveFunc() const {
	return Switcher(*this).getWith<Op, Switcher::DefaultTraitsType>();
}


template<typename T>
inline void SQLOpUtils::ExpressionWriter::ByReader<T>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	if (T::NullableType::VALUE &&
			ValueUtils::readCurrentNull(*base_.reader_, base_.inColumn_)) {
		ValueUtils::writeNull(*base_.outTuple_, base_.outColumn_);
		return;
	}

	ValueUtils::writeValue<TypeTag>(
			*base_.outTuple_, base_.outColumn_,
			ValueUtils::readCurrentValue<TypeTag>(
					*base_.reader_, base_.inColumn_));
}


template<typename T>
inline void SQLOpUtils::ExpressionWriter::ByReaderMulti<T>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	if (T::NullableType::VALUE &&
			ValueUtils::readCurrentNull(**base_.readerRef_, base_.inColumn_)) {
		ValueUtils::writeNull(*base_.outTuple_, base_.outColumn_);
		return;
	}

	ValueUtils::writeValue<TypeTag>(
			*base_.outTuple_, base_.outColumn_,
			ValueUtils::readCurrentValue<TypeTag>(
					**base_.readerRef_, base_.inColumn_));
}


template<typename T>
inline void SQLOpUtils::ExpressionWriter::ByReadableTuple<T>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	if (T::NullableType::VALUE &&
			ValueUtils::readNull(*base_.inTuple_, base_.inColumn_)) {
		ValueUtils::writeNull(*base_.outTuple_, base_.outColumn_);
		return;
	}

	ValueUtils::writeValue<TypeTag>(
			*base_.outTuple_, base_.outColumn_,
			ValueUtils::readValue<TypeTag>(*base_.inTuple_, base_.inColumn_));
}


template<typename T>
inline void SQLOpUtils::ExpressionWriter::BySummaryTuple<T>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	if (T::NullableType::VALUE &&
			base_.inSummaryTuple_->isNull(*base_.inSummaryColumn_)) {
		ValueUtils::writeNull(*base_.outTuple_, base_.outColumn_);
		return;
	}

	ValueUtils::writeValue<TypeTag>(
			*base_.outTuple_, base_.outColumn_,
			base_.inSummaryTuple_->getValueAs<TypeTag>(
					*base_.inSummaryColumn_));
}


template<typename T>
inline void SQLOpUtils::ExpressionWriter::BySummaryTupleBody<T>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;

	if (T::NullableType::VALUE &&
			base_.inSummaryTuple_->isNull(*base_.inSummaryColumn_)) {
		ValueUtils::writeNull(*base_.outTuple_, base_.outColumn_);
		return;
	}

	ValueUtils::writeValue<TypeTag>(
			*base_.outTuple_, base_.outColumn_,
			base_.inSummaryTuple_->getBodyValueAs<TypeTag>(
					*base_.inSummaryColumn_));
}


template<typename T>
inline void SQLOpUtils::ExpressionWriter::ByGeneral<T>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;
	typedef typename T::ValueType ValueType;

	const TupleValue &value =
			base_.expr_->eval(base_.cxtRef_->cxt_->getExprContext());

	if (T::NullableType::VALUE && ValueUtils::isNull(value)) {
		ValueUtils::writeNull(*base_.outTuple_, base_.outColumn_);
		return;
	}

	ValueUtils::writeValue<TypeTag>(
			*base_.outTuple_, base_.outColumn_,
			ValueUtils::getValue<ValueType>(value));
}


SQLOpUtils::ExpressionWriter::ContextRef::ContextRef() :
		cxt_(NULL) {
}


template<typename Op, typename Traits>
typename Traits::template Func<Op>::Type
SQLOpUtils::ExpressionWriter::Switcher::getWith() const {
	switch (base_.srcType_) {
	case SQLExprs::ExprCode::INPUT_READER:
		return getSubWith<Op, Traits, SQLExprs::ExprCode::INPUT_READER>();
	case SQLExprs::ExprCode::INPUT_READER_MULTI:
		return getSubWith<Op, Traits, SQLExprs::ExprCode::INPUT_READER_MULTI>();
	case SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE:
		return getSubWith<Op, Traits, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE>();
	case SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY:
		return getSubWith<Op, Traits, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY>();
	default:
		assert(!base_.isForColumn());
		return getSubWith<Op, Traits, SQLExprs::ExprCode::END_INPUT>();
	}
}

template<typename Op, typename Traits, SQLExprs::ExprCode::InputSourceType S>
typename Traits::template Func<Op>::Type
SQLOpUtils::ExpressionWriter::Switcher::getSubWith() const {
	TupleColumnType type = base_.expr_->getCode().getColumnType();
	if (base_.inSummaryColumn_ != NULL &&
			base_.inSummaryColumn_->getNullsOffset() < 0) {
		type = SQLValues::TypeUtils::toNonNullable(type);
	}
	return TypeSwitcher(type).getWith<
			Op, typename OpTraitsAt<Traits, S>::Type>();
}


SQLOpUtils::ExpressionListWriter::ExpressionListWriter(const Source &source) :
		writerListBase_(source.getFactoryContext().getAllocator()),
		writerList_(writerListBase_),
		columExprFound_(false),
		digestColumnAssigned_(false),
		activeReaderRef_(NULL),
		writer_(NULL),
		cxtRef_(cxtRefBase_),
		inTupleRef_(NULL),
		inSummaryTupleRef_(NULL),
		outTupleBase_(util::FalseType()),
		outTuple_(outTupleBase_),
		digestColumnAscending_(false) {
	ProjectionFactoryContext &cxt = source.getFactoryContext();

	SQLExprs::ExprFactoryContext &exprCxt = cxt.getExprFactoryContext();
	const SQLExprs::Expression *base = exprCxt.getBaseExpression();

	if (base == NULL || exprCxt.isPlanning() ||
			source.getCode().getType() != SQLOpTypes::PROJ_OUTPUT) {
		return;
	}

	util::StackAllocator &alloc = cxt.getAllocator();
	SQLOps::ColumnTypeList typeList(alloc);
	for (Expression::Iterator it(*base); it.exists(); it.next()) {
		const SQLExprs::ExprCode &subCode = it.get().getCode();
		typeList.push_back(subCode.getColumnType());
	}

	TupleList::Info info;
	info.columnTypeList_ = &typeList[0];
	info.columnCount_ = typeList.size();

	SQLOps::TupleColumnList destColumnList(alloc);
	destColumnList.resize(info.columnCount_);
	writerList_.resize(info.columnCount_);
	info.getColumns(&destColumnList[0], destColumnList.size());

	activeReaderRef_ = exprCxt.getActiveReaderRef();

	SQLOps::TupleColumnList::const_iterator colIt = destColumnList.begin();
	for (SQLExprs::Expression::Iterator it(*base); it.exists(); it.next()) {
		writerList_[colIt - destColumnList.begin()].initialize(
				exprCxt, it.get(), *colIt,
				&cxtRef_, activeReaderRef_, &outTuple_);
		columExprFound_ |= writerList_.back().isForColumn();
		++colIt;
	}

	if (exprCxt.getInputCount() > 0) {
		inTupleRef_ = exprCxt.getReadableTupleRef(0);
		inSummaryTupleRef_ = exprCxt.getSummaryTupleRef(0);
	}

	source.setUp(*this);
}

void SQLOpUtils::ExpressionListWriter::setKeyColumnList(
		const SQLValues::CompColumnList &keyColumnList, bool forMiddle,
		bool longKeyOnly, bool nullIgnorable) {
	if (digestColumnAssigned_) {
		return;
	}

	WriterList::iterator it = writerList_.end();
	bool ascending = true;
	bool forSummary = false;
	if (forMiddle) {
		for (it = writerList_.begin(); it != writerList_.end(); ++it) {
			forSummary = it->isSummaryColumn();
			if (it->matchIdColumn()) {
				break;
			}
		}
	}

	do {
		if (it != writerList_.end() || keyColumnList.empty()) {
			break;
		}

		const SQLValues::CompColumn &keyColumn = keyColumnList.front();
		const TupleColumnType type = keyColumn.getType();
		if (type != TupleTypes::TYPE_LONG && longKeyOnly) {
			break;
		}

		const SQLValues::TupleComparator comp(
				keyColumnList, false, true, nullIgnorable);
		if (!comp.isDigestOnly(false)) {
			if (forSummary) {
				if (SQLValues::TypeUtils::isLob(type) ||
						!keyColumn.isOrdering()) {
					break;
				}
			}
			else {
				if ((SQLValues::TypeUtils::isNullable(type) && !nullIgnorable) ||
						!SQLValues::TypeUtils::isFixed(type) ||
						!keyColumn.isOrdering()) {
					break;
				}
			}
		}

		for (it = writerList_.begin(); it != writerList_.end(); ++it) {
			if (it->matchCompColumn(keyColumn)) {
				ascending = keyColumn.isAscending();
				break;
			}
		}
	}
	while(false);

	if (it == writerList_.end()) {
		return;
	}

	digestColumn_ = it->getOutColumn();
	digestColumnAssigned_ = true;
	digestColumnAscending_ = ascending;
	if (it->getInSummaryColumn() != NULL) {
		headSummaryColumn_ = *it->getInSummaryColumn();
	}
	writerList_.erase(it);
}

void SQLOpUtils::ExpressionListWriter::setTupleWriter(
		TupleListWriter *writer) {
	writer_ = writer;
}

void SQLOpUtils::ExpressionListWriter::applyProjection(
		const Projection &proj) const {
	uint32_t pos = 0;
	WriterList::iterator writerIt = writerList_.begin();
	for (SQLExprs::Expression::Iterator it(proj); it.exists(); it.next()) {
		if (!digestColumnAssigned_ || pos != digestColumn_.getPosition()) {
			writerIt->setExpression(it.get());
			++writerIt;
		}
		++pos;
	}
}

bool SQLOpUtils::ExpressionListWriter::isAvailable() const {
	return (!writerList_.empty() || digestColumnAssigned_);
}

bool SQLOpUtils::ExpressionListWriter::isDigestColumnAssigned() const {
	return digestColumnAssigned_;
}

bool SQLOpUtils::ExpressionListWriter::isDigestColumnAscending() const {
	assert(isDigestColumnAssigned());
	return digestColumnAscending_;
}

TupleColumnType SQLOpUtils::ExpressionListWriter::getDigestColumnType() const {
	const TupleColumnType type = digestColumn_.getType();
	assert(isDigestColumnAssigned() && !SQLValues::TypeUtils::isNull(type));
	return type;
}


SQLOpUtils::ExpressionListWriter::Source::Source(
		ProjectionFactoryContext &factoryCxt,
		const ProjectionCode &code) {
	entry_.factoryCxt_ = &factoryCxt;
	entry_.code_ = &code;
	entry_.forMiddle_ = (code.getKeyList() != NULL);
	entry_.longKeyOnly_ = true;
	entry_.keyColumnList_ = code.getKeyList();
}

SQLOpUtils::ExpressionListWriter::Source::Source(
		OpContext &cxt, const Projection &proj, bool forMiddle,
		const SQLValues::CompColumnList *keyColumnList,
		bool nullIgnorable, const Projection *inMiddleProj,
		SQLExprs::ExprCode::InputSourceType srcType) {
	entry_.code_ = &proj.getProjectionCode();

	entry_.cxt_ = &cxt;
	entry_.proj_ = &proj;
	entry_.forMiddle_ = forMiddle;
	entry_.nullIgnorable_ = nullIgnorable;
	entry_.keyColumnList_ = keyColumnList;

	builder_ = UTIL_MAKE_LOCAL_UNIQUE(
			builder_, OpCodeBuilder, OpCodeBuilder::ofContext(cxt));
	entry_.factoryCxt_ = &builder_->getProjectionFactoryContext();

	SQLExprs::ExprCode::InputSourceType resolvedSrcType = srcType;
	if (srcType == SQLExprs::ExprCode::INPUT_MULTI_STAGE) {
		assert(forMiddle || (inMiddleProj != NULL));
		if (inMiddleProj != NULL) {
			builder_->assignInputTypesByProjection(*inMiddleProj, NULL);
		}
		resolvedSrcType = (inMiddleProj == NULL ?
				SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE :
				SQLExprs::ExprCode::INPUT_READER_MULTI);
	}
	else {
		cxt.setUpProjectionFactoryContext(*entry_.factoryCxt_);
	}

	SQLExprs::ExprFactoryContext &exprCxt =
			builder_->getExprFactoryContext();
	exprCxt.setPlanning(false);
	exprCxt.setBaseExpression(&proj);
	exprCxt.setInputSourceType(0, resolvedSrcType);
}

void SQLOpUtils::ExpressionListWriter::Source::setOutput(uint32_t index) {
	entry_.outIndex_ = index;
}

SQLOps::ProjectionFactoryContext&
SQLOpUtils::ExpressionListWriter::Source::getFactoryContext() const {
	assert(entry_.factoryCxt_ != NULL);
	return *entry_.factoryCxt_;
}

const SQLOps::ProjectionCode&
SQLOpUtils::ExpressionListWriter::Source::getCode() const {
	assert(entry_.code_ != NULL);
	return *entry_.code_;
}

void SQLOpUtils::ExpressionListWriter::Source::setUp(
		ExpressionListWriter &writer) const {
	if (entry_.keyColumnList_ != NULL) {
		writer.setKeyColumnList(
				*entry_.keyColumnList_, entry_.forMiddle_,
				entry_.longKeyOnly_, entry_.nullIgnorable_);
	}
	if (entry_.proj_ != NULL) {
		writer.applyProjection(*entry_.proj_);
	}

	{
		ProjectionFactoryContext &cxt = getFactoryContext();
		SQLExprs::ExprFactoryContext &exprCxt = cxt.getExprFactoryContext();
		const SQLExprs::Expression *base = exprCxt.getBaseExpression();
		assert(base != NULL);

		uint32_t index = entry_.outIndex_;
		if (index == std::numeric_limits<uint32_t>::max()) {
			index = SQLOps::OpCodeBuilder::resolveOutputIndex(base->getCode());
		}

		if (entry_.cxt_ == NULL) {
			getFactoryContext().addWriterRef(index, &writer.writer_);
		}
		else {
			writer.setTupleWriter(&entry_.cxt_->getWriter(index));
			writer.applyContext(*entry_.cxt_);
		}
	}
}


SQLOpUtils::ExpressionListWriter::Source::Entry::Entry():
		factoryCxt_(NULL),
		code_(NULL),
		cxt_(NULL),
		proj_(NULL),
		forMiddle_(false),
		longKeyOnly_(false),
		nullIgnorable_(false),
		keyColumnList_(NULL),
		outIndex_(std::numeric_limits<uint32_t>::max()) {
}


void SQLOpUtils::NonExecutableProjection::project(OpContext &cxt) const {
	static_cast<void>(cxt);
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}
