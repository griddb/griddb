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
	setUpExprFactoryContext(source);
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

SQLOps::OpConfig& SQLOps::OpCodeBuilder::createConfig(const OpConfig *src) {
	OpConfig *dest = ALLOC_NEW(getAllocator()) OpConfig();
	if (src != NULL) {
		*dest = *src;
	}
	return *dest;
}

SQLOps::ContainerLocation& SQLOps::OpCodeBuilder::createContainerLocation(
		const ContainerLocation &src) {
	return *(ALLOC_NEW(getAllocator()) ContainerLocation(src));
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
	applyDecrementalType(cxt, proj);
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
	applyDecrementalType(cxt, proj);
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
		bool unified, uint32_t index, uint32_t startColumn,
		const util::Set<uint32_t> *keySet) {
	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT);

	SQLExprs::ExprRewriter::createEmptyRefProjection(
			getExprFactoryContext(), unified, index, startColumn, keySet,
			proj);
	rewriter_.remapColumn(getExprFactoryContext(), proj);

	return proj;
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createEmptyConstProjection(
		const ColumnTypeList &typeList, bool withEmptyLimit) {
	Projection &proj = createProjection(SQLOpTypes::PROJ_OUTPUT);

	SQLExprs::Expression::ModIterator projIt(proj);
	for (ColumnTypeList::const_iterator it = typeList.begin();
			it != typeList.end(); ++it) {
		projIt.append(SQLExprs::ExprRewriter::createEmptyConstExpr(
				getExprFactoryContext(), *it));
	}

	SQLExprs::ExprCode &exprCode = proj.getProjectionCode().getExprCode();
	exprCode.setAttributes(
			exprCode.getAttributes() | SQLExprs::ExprCode::ATTR_ASSIGNED);

	if (withEmptyLimit) {
		OpCode code;
		code.setLimit(0);
		return createLimitedProjection(proj, code);
	}

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
		const Projection &src, Projection **distinctProj,
		bool chainOmitted) {
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
		applyDecrementalType(factoryCxt, src);

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

				assert(!chainOmitted);
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

		if (!chainOmitted) {
			for (Projection::ChainIterator it(src); it.exists(); it.next()) {
				dest->addChain(rewriteProjection(it.get()));
			}
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
	applyDecrementalType(factoryCxt, src);
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

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

	const Projection *srcPipe = code.getPipeProjection();
	const Projection *srcFinish = code.getFinishProjection();
	assert(srcPipe != NULL);

	code.setPipeProjection(NULL);
	code.setFinishProjection(NULL);

	Projection *destPipe = NULL;
	Projection *destFinish = NULL;
	if (isAggregationArrangementRequired(srcPipe, srcFinish)) {
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
			applyDecrementalType(factoryCxt, *srcPipe);
			destPipe = &rewriteProjection(*srcPipe);
		}
		{
			SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
			factoryCxt.setAggregationPhase(true, orgPhase);
			factoryCxt.setAggregationPhase(false, finishPhase);
			applyDecrementalType(factoryCxt, *srcPipe);
			destFinish = &rewriteProjection(*srcPipe, distinctProj);
		}
	}
	else {
		{
			SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
			applyDecrementalType(factoryCxt, *srcPipe);
			destPipe = &rewriteProjection(*srcPipe);
		}

		if (srcFinish != NULL) {
			SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
			applyDecrementalType(factoryCxt, *srcFinish);
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

	SQLExprs::ExprRewriter::applyDecrementalType(factoryCxt, src.getCode());
	return rewriter_.rewrite(factoryCxt, src, NULL);
}

void SQLOps::OpCodeBuilder::replaceColumnToConstExpr(Projection &proj) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::Expression &dest =
			SQLExprs::ExprRewriter::replaceColumnToConstExpr(factoryCxt, proj);
	assert(&dest == &proj);
	static_cast<void>(dest);

	const size_t count = proj.getChainCount();
	for (size_t i = 0; i < count; i++) {
		replaceColumnToConstExpr(proj.chainAt(i));
	}
}

const SQLExprs::Expression* SQLOps::OpCodeBuilder::extractJoinKeyColumns(
		const SQLExprs::Expression *srcExpr,
		const SQLType::JoinType *joinTypeOfFilter,
		SQLValues::CompColumnList &destKeyList, CompPosSet &posSet,
		SQLExprs::Expression **destExpr) {
	if (srcExpr == NULL) {
		return NULL;
	}

	if (destExpr == NULL) {
		if (!destKeyList.empty() && posSet.empty()) {
			for (SQLValues::CompColumnList::const_iterator it =
					destKeyList.begin(); it != destKeyList.end(); ++it) {
				assert(!it->isEitherUnbounded());
				posSet.insert(CompPos(
						it->getColumnPos(true),
						it->getColumnPos(false)));
			}
		}

		SQLExprs::Expression *destExprBase = NULL;
		const size_t orgCount = destKeyList.size();
		extractJoinKeyColumns(
				srcExpr, joinTypeOfFilter, destKeyList, posSet,
				&destExprBase);

		if (destKeyList.size() == orgCount ||
				(joinTypeOfFilter != NULL &&
				*joinTypeOfFilter != SQLType::JOIN_INNER)) {
			return srcExpr;
		}

		{
			SQLExprs::ExprRewriter &rewriter = getExprRewriter();
			SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

			SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
			factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);
			SQLExprs::ExprRewriter::applyDecrementalType(
					factoryCxt, srcExpr->getCode());

			destExprBase = &rewriter.rewrite(factoryCxt, *srcExpr, NULL);
		}

		return extractJoinKeyColumns(
				destExprBase, joinTypeOfFilter, destKeyList, posSet,
				&destExprBase);
	}

	assert(*destExpr == NULL || srcExpr == *destExpr);

	const SQLExprs::ExprType type = srcExpr->getCode().getType();
	if (type == SQLType::EXPR_AND) {
		if (*destExpr == NULL) {
			for (SQLExprs::Expression::Iterator it(*srcExpr); it.exists(); it.next()) {
				extractJoinKeyColumns(
						&it.get(), joinTypeOfFilter, destKeyList, posSet, destExpr);
			}
			return srcExpr;
		}

		SQLExprs::Expression *destSubLast = NULL;
		bool eitherEmpty = false;
		for (SQLExprs::Expression::ModIterator it(**destExpr);
				it.exists(); it.next()) {
			SQLExprs::Expression *destSub = &it.get();
			extractJoinKeyColumns(
					&it.get(), joinTypeOfFilter, destKeyList, posSet, &destSub);
			if (destSub == NULL) {
				eitherEmpty = true;
			}
			else {
				if (&it.get() != destSub) {
					it.remove();
					it.insert(*destSub);
				}
				destSubLast = &it.get();
			}
		}
		if (eitherEmpty) {
			*destExpr = destSubLast;
		}
		return *destExpr;
	}

	do {
		if (type != SQLType::OP_EQ) {
			break;
		}

		const SQLExprs::Expression *left = &srcExpr->child();
		const SQLExprs::Expression *right = &left->next();
		if (left->getCode().getType() != SQLType::EXPR_COLUMN ||
				right->getCode().getType() != SQLType::EXPR_COLUMN) {
			break;
		}
		if (left->getCode().getInput() != 0) {
			std::swap(left, right);
		}
		if (left->getCode().getInput() != 0 ||
				right->getCode().getInput() != 1) {
			break;
		}

		const CompPos pos(
				left->getCode().getColumnPos(),
				right->getCode().getColumnPos());
		if (!posSet.insert(pos).second) {
			break;
		}

		destKeyList.push_back(SQLExprs::ExprTypeUtils::toCompColumn(
				type, pos.first, pos.second, false));

		*destExpr = NULL;
		return *destExpr;
	}
	while (false);

	return srcExpr;
}

void SQLOps::OpCodeBuilder::setUpOutputNodes(
		OpPlan &plan, OpNode *outNode, OpNode &inNode,
		const Projection *distinctProj, SQLType::AggregationPhase aggrPhase,
		const SQLValues::CompColumnList *distinctKeyList) {
	setUpOutputNodesDetail(
			plan, outNode, false, &inNode, SQLOpTypes::END_OP, distinctProj,
			aggrPhase, distinctKeyList);
}

SQLOps::OpNode& SQLOps::OpCodeBuilder::setUpOutputNodesDetail(
		OpPlan &plan, OpNode *outNode, bool outNodePending,
		OpNode *inNode, SQLOpTypes::Type inNodeType,
		const Projection *distinctProj, SQLType::AggregationPhase aggrPhase,
		const SQLValues::CompColumnList *distinctKeyList) {
	OpNode *resolvedInNode = inNode;

	if (distinctProj == NULL) {
		if (resolvedInNode == NULL) {
			assert(inNodeType != SQLOpTypes::END_OP);
			resolvedInNode = &plan.createNode(inNodeType);
		}

		if (!outNodePending) {
			if (outNode == NULL) {
				plan.linkToAllParentOutput(*resolvedInNode);
			}
			else {
				outNode->addInput(*resolvedInNode);
			}
		}

		return *resolvedInNode;
	}

	OpNode &distinctNode = plan.createNode(SQLOpTypes::OP_GROUP_DISTINCT);

	if (resolvedInNode == NULL) {
		assert(inNodeType != SQLOpTypes::END_OP);
		resolvedInNode = &plan.createNode(inNodeType);
	}
	distinctNode.addInput(*resolvedInNode, 0);
	distinctNode.addInput(*resolvedInNode, 1);

	OpCode code;
	code.setPipeProjection(distinctProj);
	code.setAggregationPhase(aggrPhase);
	code.setKeyColumnList(distinctKeyList);
	distinctNode.setCode(code);

	if (!outNodePending) {
		OpNode &resolvedOutNode =
				(outNode == NULL ? plan.getParentOutput(0) : *outNode);
		resolvedOutNode.addInput(distinctNode);
	}

	return *resolvedInNode;
}

void SQLOps::OpCodeBuilder::setUpNoopNodes(
		OpPlan &plan, const ColumnTypeList &outTypeList) {
	const OpCode &code = createOutputEmptyCode(outTypeList);
	OpNode &node = plan.createNode(code.getType());
	node.setCode(code);
	{
		const uint32_t count = plan.getParentInputCount();
		for (uint32_t i = 0; i < count; i++) {
			node.addInput(plan.getParentInput(i));
		}
	}
	{
		const uint32_t count = plan.getParentOutputCount();
		for (uint32_t i = 0; i < count; i++) {
			plan.getParentOutput(i).addInput(node);
		}
	}
}

SQLOps::OpCode SQLOps::OpCodeBuilder::createOutputEmptyCode(
		const ColumnTypeList &outTypeList) {
	OpCode code;
	code.setType(SQLOpTypes::OP_SELECT_NONE);
	code.setPipeProjection(&createEmptyConstProjection(outTypeList, true));
	return code;
}

bool SQLOps::OpCodeBuilder::isNoopAlways(const OpCode &code) {
	return (code.getLimit() == 0 &&
			code.findContainerLocation() == NULL &&
			!(code.getType() == SQLOpTypes::OP_SELECT_PIPE ||
			code.getType() == SQLOpTypes::OP_SELECT_NONE));
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

				factoryCxt.setArrangedKeyList(dest.findMiddleKeyColumnList(), false);
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
	factoryCxt.setArrangedKeyList(dest.findKeyColumnList(), false);

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
	bool orderingRestricted;
	const SQLValues::CompColumnList *keyList =
			factoryCxt.getArrangedKeyList(orderingRestricted);

	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

	const Projection *aggrOrBaseProj = (aggrProj == NULL ? &proj : aggrProj);
	if (((proj.getProjectionCode().getExprCode().getAttributes() |
			aggrOrBaseProj->getProjectionCode().getExprCode().getAttributes()) &
			SQLExprs::ExprCode::ATTR_AGGR_DECREMENTAL) != 0) {
		factoryCxt.setDecrementalType(SQLExprs::ExprSpec::DECREMENTAL_SOME);
	}

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
			aggrTupleSet->addKeyList(*keyList, true, false);
		}

		applyAggregationColumns(*aggrProj, *aggrTupleSet);
		SQLExprs::ExprRewriter::applySummaryTupleOptions(
				factoryCxt, *aggrTupleSet);

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
			SummaryTupleSet::applyColumnList(
					aggrTupleSet->getTotalColumnList(), *summaryColumns);
		}
		else {
			SummaryTupleSet::applyColumnList(
					rewriter_.createSummaryColumnList(
							factoryCxt, i, false, (i == 0), keyList,
							orderingRestricted),
					*summaryColumns);
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

void SQLOps::OpCodeBuilder::applyDecrementalType(
		SQLExprs::ExprFactoryContext &cxt, const Projection &proj) {
	SQLExprs::ExprRewriter::applyDecrementalType(
			cxt, proj.getProjectionCode().getExprCode());
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

const SQLOps::Projection& SQLOps::OpCodeBuilder::resolveOutputProjection(
		const OpCode &code, const uint32_t *outIndex) {
	const Projection *proj = findOutputProjection(code, outIndex);
	if (proj == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
	return *proj;
}

const SQLOps::Projection& SQLOps::OpCodeBuilder::resolveOutputProjection(
		const Projection &src, const uint32_t *outIndex) {
	const Projection *proj = findOutputProjection(src, outIndex);
	if (proj == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
	return *proj;
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
		const Projection &src, const bool *forPipe) {
	const SQLOpTypes::ProjectionType type = src.getProjectionCode().getType();
	if (((forPipe == NULL || *forPipe) &&
			type == SQLOpTypes::PROJ_AGGR_PIPE) ||
			((forPipe == NULL || !*forPipe) &&
					type == SQLOpTypes::PROJ_AGGR_OUTPUT)) {
		return &src;
	}

	for (Projection::ChainIterator it(src); it.exists(); it.next()) {
		const SQLOps::Projection *foundProj =
				findAggregationProjection(it.get(), forPipe);
		if (foundProj != NULL) {
			return foundProj;
		}
	}

	return NULL;
}

SQLOps::Projection* SQLOps::OpCodeBuilder::findAggregationModifiableProjection(
		Projection &src, const bool *forPipe) {
	if (&src == findAggregationProjection(src, forPipe)) {
		return &src;
	}

	for (Projection::ChainModIterator it(src); it.exists(); it.next()) {
		SQLOps::Projection *foundProj =
				findAggregationModifiableProjection(it.get(), forPipe);
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
	const Projection &outProj = resolveOutputProjection(proj, outIndex);
	for (SQLExprs::Expression::Iterator it(outProj); it.exists(); it.next()) {
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

void SQLOps::OpCodeBuilder::getColumnTypeListByColumns(
		const TupleColumnList &columnList, ColumnTypeList &typeList) {
	for (TupleColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		typeList.push_back(it->getType());
	}
}

TupleColumnType SQLOps::OpCodeBuilder::resolveUnifiedInputType(
		OpContext &cxt, uint32_t pos) {
	const uint32_t count = cxt.getInputCount();
	TupleColumnType type = TupleTypes::TYPE_NULL;

	bool first = true;
	for (uint32_t i = 0; i < count; i++) {
		const TupleColumnType elemType = cxt.getReaderColumn(i, pos).getType();
		type = SQLExprs::ExprFactoryContext::unifyInputType(
				type, elemType, first);
		first = false;
	}

	assert(!SQLValues::TypeUtils::isNull(type));
	return type;
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
		const ExprRefList &distinctList, size_t index, bool idGrouping,
		bool idSingle) {
	SQLValues::CompColumnList &keyList = createCompColumnList();

	if (idGrouping || !idSingle) {
		SQLValues::CompColumn key;
		key.setColumnPos(0, true);
		if (idSingle) {
			key.setOrdering(false);
		}
		keyList.push_back(key);
	}

	if (!idGrouping) {
		SQLValues::CompColumn key;
		const SQLExprs::Expression *expr = distinctList[index];
		key.setColumnPos(expr->child().getCode().getColumnPos(), true);
		if (idSingle) {
			key.setOrdering(false);
		}
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
		uint32_t refColumnPos = 1;
		SQLExprs::Expression &nonDistinctExpr =
				SQLExprs::ExprRewriter::toNonDistinctExpr(
						factoryCxt, *expr, input, &refColumnPos);
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
		SQLType::AggregationPhase aggrPhase,
		const util::Set<uint32_t> *keySet) {
	SQLOps::Projection &mainProj = createDistinctMergeProjectionAt(
			distinctList, index, proj, aggrPhase, -1, keySet);
	SQLOps::Projection &emptyProj = createMultiStageProjection(
			createDistinctMergeProjectionAt(
					distinctList, index, proj, aggrPhase, 1, keySet),
			createDistinctMergeProjectionAt(
					distinctList, index, proj, aggrPhase, 0, keySet));
	return createMultiStageProjection(mainProj, emptyProj);
}

SQLOps::Projection& SQLOps::OpCodeBuilder::createDistinctMergeProjectionAt(
		const ExprRefList &distinctList, size_t index, const Projection &proj,
		SQLType::AggregationPhase aggrPhase, int32_t emptySide,
		const util::Set<uint32_t> *keySet) {
	const bool forAdvance = (aggrPhase == SQLType::AGG_PHASE_ADVANCE_PIPE);

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	if (index + 1 < distinctList.size()) {
		Projection &destProj = (emptySide == 0 ?
				createEmptyRefProjection(false, 0, 1, keySet) :
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
			const Expression *expr = distinctList[i];

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
					keySet, NULL, NULL);
		}
		return destProj;
	}
}

void SQLOps::OpCodeBuilder::setUpExprFactoryContext(const Source &source) {
	OpContext *cxt = source.cxt_;
	if (cxt == NULL) {
		return;
	}

	const bool inputMapping =
			(source.mappedInput_ != std::numeric_limits<uint32_t>::max());

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	const uint32_t inCount = (inputMapping ? 1 : cxt->getInputCount());
	for (uint32_t index = 0; index < inCount; index++) {
		const uint32_t srcIndex = (inputMapping ? source.mappedInput_ : index);
		const uint32_t columnCount = cxt->getColumnCount(srcIndex);
		for (uint32_t pos = 0; pos < columnCount; pos++) {
			TupleColumnType inType;
			if (source.inputUnified_) {
				inType = resolveUnifiedInputType(*cxt, pos);
			}
			else {
				inType = cxt->getReaderColumn(srcIndex, pos).getType();
			}
			factoryCxt.setInputType(index, pos, inType);
		}
		factoryCxt.setInputNullable(index, false);
		factoryCxt.setReadableTupleRef(
				index, cxt->getReadableTupleRef(srcIndex));
		factoryCxt.setSummaryTupleRef(
				index, cxt->getSummaryTupleRef(srcIndex));
		factoryCxt.setSummaryColumnListRef(
				index, cxt->getSummaryColumnListRef(srcIndex));
	}

	factoryCxt.setActiveReaderRef(cxt->getActiveReaderRef());
	factoryCxt.setAggregationTupleRef(
			cxt->getDefaultAggregationTupleRef());
	factoryCxt.setAggregationTupleSet(
			cxt->getDefaultAggregationTupleSet());

	getProjectionFactoryContext().setProfiler(cxt->getOptimizationProfiler());
	factoryCxt.setProfile(cxt->getExprProfile());
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
		cxt_(NULL),
		mappedInput_(std::numeric_limits<uint32_t>::max()),
		inputUnified_(false) {
}


SQLOps::OpAllocatorManager*
SQLOps::OpAllocatorManager::DEFAULT_INSTANCE = initializeDefault();

SQLOps::OpAllocatorManager::OpAllocatorManager(
		const util::StdAllocator<void, void> &alloc, bool composite,
		OpAllocatorManager *sharedManager,
		SQLValues::VarAllocator *localVarAllocRef,
		util::AllocatorLimitter *limitter) :
		alloc_(alloc),
		composite_(composite),
		subList_(alloc),
		sharedManager_(sharedManager),
		localVarAllocRef_(localVarAllocRef),
		cachedBuffer_(NULL),
		baseAlloc_(NULL),
		limitter_(limitter) {
	assert(!composite || localVarAllocRef == NULL);

	if (sharedManager == NULL) {
		assert(localVarAllocRef == NULL);
		mutex_ = UTIL_MAKE_LOCAL_UNIQUE(mutex_, util::Mutex);
	}

	if (!composite) {
		prepareAllocators();
	}
}

SQLOps::OpAllocatorManager::~OpAllocatorManager() {
	while (!subList_.empty()) {
		ALLOC_DELETE(alloc_, subList_.back());
		subList_.pop_back();
	}

	cleanUpAllocators();
}

SQLOps::OpAllocatorManager& SQLOps::OpAllocatorManager::getDefault() {
	if (DEFAULT_INSTANCE == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *DEFAULT_INSTANCE;
}

SQLOps::OpAllocatorManager& SQLOps::OpAllocatorManager::getSubManager(
		ExtOpContext &cxt) {
	assert(composite_);

	const uint32_t workerId = cxt.getTotalWorkerId();

	util::LockGuard<util::Mutex> guard(*mutex_);
	if (workerId >= subList_.size()) {
		subList_.resize(workerId + 1);
	}

	OpAllocatorManager *&sub = subList_[workerId];
	if (sub == NULL) {
		sub = ALLOC_NEW(alloc_) OpAllocatorManager(
				alloc_, false, NULL, NULL, cxt.getAllocatorLimitter());
	}
	else {
		sub->prepareAllocators();
	}

	return *sub;
}

util::StackAllocator& SQLOps::OpAllocatorManager::create(
		util::StackAllocator::BaseAllocator &base) {
	assert(sharedManager_ != NULL && localVarAllocRef_ != NULL);
	checkAllocators();

	if (baseAlloc_ == NULL) {
		assert(cachedAllocList_->empty());
		cleanUpHandler_ = UTIL_MAKE_LOCAL_UNIQUE(
				cleanUpHandler_, CleanUpHandler, this);
		base.addCleanUpHandler(*cleanUpHandler_);
		baseAlloc_ = &base;
	}
	else if (baseAlloc_ != &base) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (cachedAllocList_->empty()) {
		const size_t headSize = SQLValues::VarAllocator::getElementHeadSize();
		const size_t blockSizeBase =
				SQLValues::VarAllocator::TraitsType::getFixedSize(1);
		assert(headSize < blockSizeBase);
		
		util::StackAllocator::Option option;
		option.smallAlloc_ = localAlloc_.get();
		option.smallBlockSize_ = blockSizeBase - headSize;
		option.smallBlockLimit_ =
				SQLValues::VarAllocator::TraitsType::getFixedSize(2) /
				blockSizeBase;

		util::AllocUniquePtr<util::StackAllocator> ptr(ALLOC_UNIQUE(
				*localAlloc_, util::StackAllocator,
				util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, "sqlOp"),
				&base, &option));
		ptr->setLimit(
				util::AllocatorStats::STAT_GROUP_TOTAL_LIMIT, limitter_);
		return *ptr.release();
	}
	else {
		util::StackAllocator *alloc = cachedAllocList_->back();
		assert(alloc != NULL);

		alloc->setLimit(
				util::AllocatorStats::STAT_GROUP_TOTAL_LIMIT, limitter_);
		cachedAllocList_->pop_back();
		return *alloc;
	}
}

void SQLOps::OpAllocatorManager::release(util::StackAllocator *alloc) {
	assert(sharedManager_ != NULL && localVarAllocRef_ != NULL);
	checkAllocators();

	if (alloc == NULL) {
		return;
	}

	util::StackAllocator::Tool::forceReset(*alloc);
	alloc->setFreeSizeLimit(0);
	alloc->trim();

	alloc->setLimit(
			util::AllocatorStats::STAT_GROUP_TOTAL_LIMIT,
			static_cast<util::AllocatorLimitter*>(NULL));

	util::AllocUniquePtr<util::StackAllocator> ptr(alloc, *localAlloc_);
	cachedAllocList_->push_back(ptr.get());
	ptr.release();
}

SQLOps::OpAllocatorManager::Buffer SQLOps::OpAllocatorManager::createBuffer(
		size_t size, size_t filledRange) {
	if (sharedManager_ != NULL) {
		return sharedManager_->createBuffer(size, filledRange);
	}

	assert(size >= filledRange);

	Buffer buf;
	{
		util::LockGuard<util::Mutex> guard(*mutex_);
		checkAllocators();

		Buffer **target = NULL;
		for (bool filtering = true;; filtering = false) {
			for (target = &cachedBuffer_; *target != NULL;) {
				Buffer *cur = (*target);

				Buffer **nextRef = reinterpret_cast<Buffer**>(&cur->addr_);
				Buffer *next = *nextRef;

				if (size > cur->size_) {
					getLocalVarAllocatorDirect().deallocate(cur);
					*target = next;
				}
				else if ((filledRange > 0) != (cur->filledRange_ > 0)) {
					target = nextRef;
				}
				else {
					break;
				}
			}
			if (*target != NULL || cachedBuffer_ == NULL || !filtering) {
				break;
			}
		}

		if (*target != NULL) {
			Buffer &cur = (**target);
			Buffer *next = static_cast<Buffer*>(cur.addr_);

			*target = next;
			buf = Buffer(&cur, cur.size_, cur.filledRange_);
		}
		else {
			const size_t allocSize =
					static_cast<size_t>(std::max<uint64_t>(size, sizeof(Buffer)));
			buf = Buffer(
					getLocalVarAllocatorDirect().allocate(allocSize), allocSize, 0);
		}
	}

	if (filledRange > 0) {
		const size_t range =
				(filledRange <= buf.filledRange_ ? sizeof(Buffer) : filledRange);

		Buffer::fill(buf.addr_, range);
		assert(Buffer::checkFilled(buf.addr_, filledRange));
	}
	buf.filledRange_ = filledRange;

	return buf;
}

void SQLOps::OpAllocatorManager::releaseBuffer(Buffer &buf) {
	if (sharedManager_ != NULL) {
		sharedManager_->releaseBuffer(buf);
		return;
	}

	void *cur = buf.addr_;
	if (cur == NULL) {
		return;
	}

	const size_t size = buf.size_;
	const size_t filledRange = buf.filledRange_;
	assert(size >= sizeof(Buffer));
	assert(Buffer::checkFilled(cur, filledRange));

	util::LockGuard<util::Mutex> guard(*mutex_);
	checkAllocators();

	void *next = cachedBuffer_;
	cachedBuffer_ = static_cast<Buffer*>(cur);

	cachedBuffer_->addr_ = next;
	cachedBuffer_->size_ = size;
	cachedBuffer_->filledRange_ = filledRange;
}

SQLValues::VarAllocator& SQLOps::OpAllocatorManager::getLocalVarAllocator() {
	assert(sharedManager_ != NULL && localVarAllocRef_ != NULL);
	checkAllocators();
	return getLocalVarAllocatorDirect();
}

util::StdAllocator<void, void>&
SQLOps::OpAllocatorManager::getLocalAllocator() {
	assert(sharedManager_ != NULL && localVarAllocRef_ != NULL);
	checkAllocators();
	return *localAlloc_;
}

SQLOps::OpAllocatorManager*
SQLOps::OpAllocatorManager::initializeDefault() throw() {
	static std::allocator<uint8_t> alloc;
	static util::LocalUniquePtr<OpAllocatorManager> instance;
	assert(instance.get() == NULL);
	try {
		instance = UTIL_MAKE_LOCAL_UNIQUE(
				instance, OpAllocatorManager, alloc, true, NULL, NULL, NULL);
	}
	catch (...) {
	}
	return instance.get();
}

void SQLOps::OpAllocatorManager::checkAllocators() {
	if (composite_ || cachedAllocList_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLOps::OpAllocatorManager::prepareAllocators() {
	assert(!composite_);

	if (cachedAllocList_.get() != NULL) {
		return;
	}

	typedef util::StdAllocator<void, void> Alloc;

	if (localVarAllocRef_ == NULL) {
		localVarAlloc_ = UTIL_MAKE_LOCAL_UNIQUE(
				localVarAlloc_, SQLValues::VarAllocator,
				util::AllocatorInfo(
						ALLOCATOR_GROUP_SQL_WORK, "sqlOpLocalVar"));
	}

	localAlloc_ = UTIL_MAKE_LOCAL_UNIQUE(
			localAlloc_, Alloc, getLocalVarAllocatorDirect());
	cachedAllocList_ = UTIL_MAKE_LOCAL_UNIQUE(
			cachedAllocList_, StackAllocatorList, *localAlloc_);
}

void SQLOps::OpAllocatorManager::cleanUpAllocators() {
	if (cachedAllocList_.get() != NULL) {
		while (!cachedAllocList_->empty()) {
			ALLOC_DELETE(*localAlloc_, cachedAllocList_->back());
			cachedAllocList_->pop_back();
		}
	}
	baseAlloc_ = NULL;

	while (cachedBuffer_ != NULL) {
		void *cur = cachedBuffer_;
		void *next = cachedBuffer_->addr_;

		cachedBuffer_ = static_cast<Buffer*>(next);
		getLocalVarAllocatorDirect().deallocate(cur);
	}

	cachedAllocList_.reset();
	localAlloc_.reset();
	localVarAlloc_.reset();
}

SQLValues::VarAllocator&
SQLOps::OpAllocatorManager::getLocalVarAllocatorDirect() {
	if (localVarAllocRef_ != NULL) {
		return *localVarAllocRef_;
	}
	assert(localVarAlloc_.get() != NULL);
	return *localVarAlloc_;
}


SQLOps::OpAllocatorManager::Buffer::Buffer() :
		addr_(NULL),
		size_(0),
		filledRange_(0) {
}

SQLOps::OpAllocatorManager::Buffer::Buffer(
		void *addr, size_t size, size_t filledRange) :
		addr_(addr),
		size_(size),
		filledRange_(filledRange) {
}

void SQLOps::OpAllocatorManager::Buffer::fill(void *addr, size_t size) {
	memset(addr, 0, size);
}

bool SQLOps::OpAllocatorManager::Buffer::checkFilled(
		const void *addr, size_t size) {
	const uint8_t *it = static_cast<const uint8_t*>(addr);
	return (static_cast<size_t>(std::count(it, it + size, 0)) == size);
}


SQLOps::OpAllocatorManager::BufferRef::BufferRef(
		OpAllocatorManager &manager, size_t size, size_t filledRange) :
		buffer_(manager.createBuffer(size, filledRange)),
		manager_(manager),
		filled_(true) {
}

SQLOps::OpAllocatorManager::BufferRef::~BufferRef() {
	if (!filled_) {
		buffer_.filledRange_ = 0;
	}
	manager_.releaseBuffer(buffer_);
}

const SQLOps::OpAllocatorManager::Buffer&
SQLOps::OpAllocatorManager::BufferRef::get() {
	filled_ = false;
	return buffer_;
}

void SQLOps::OpAllocatorManager::BufferRef::setFilled() {
	assert(Buffer::checkFilled(buffer_.addr_, buffer_.filledRange_));
	filled_ = true;
}


SQLOps::OpAllocatorManager::CleanUpHandler::CleanUpHandler(
		OpAllocatorManager *manager) :
		manager_(manager) {
}

void SQLOps::OpAllocatorManager::CleanUpHandler::operator()() throw() {
	if (manager_ != NULL) {
		manager_->cleanUpAllocators();
		manager_ = NULL;
	}
}


SQLOps::OpLatchKeeperManager*
SQLOps::OpLatchKeeperManager::DEFAULT_INSTANCE = initializeDefault();

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

SQLOps::OpLatchKeeperManager& SQLOps::OpLatchKeeperManager::getDefault() {
	if (DEFAULT_INSTANCE == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *DEFAULT_INSTANCE;
}

SQLOps::OpLatchKeeperManager&
SQLOps::OpLatchKeeperManager::getSubManager(
		ExtOpContext &cxt, util::StdAllocator<void, void> &subAlloc) {
	assert(composite_);

	const uint32_t workerId = cxt.getTotalWorkerId();

	util::LockGuard<util::Mutex> guard(*mutex_);
	if (workerId >= subList_.size()) {
		subList_.resize(workerId + 1);
	}

	OpLatchKeeperManager *&sub = subList_[workerId];
	if (sub == NULL) {
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

SQLOps::OpLatchKeeperManager*
SQLOps::OpLatchKeeperManager::initializeDefault() throw() {
	static std::allocator<uint8_t> alloc;
	static util::LocalUniquePtr<OpLatchKeeperManager> instance;
	assert(instance.get() == NULL);
	try {
		instance = UTIL_MAKE_LOCAL_UNIQUE(
				instance, OpLatchKeeperManager, alloc, true);
	}
	catch (...) {
	}
	return instance.get();
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


const SQLOps::OpSimulator::PointEntry SQLOps::OpSimulator::POINT_LIST[] = {
	UTIL_NAME_CODER_ENTRY(PONIT_INIT),
	UTIL_NAME_CODER_ENTRY(POINT_SCAN_MAIN),
	UTIL_NAME_CODER_ENTRY(POINT_SCAN_OID)
};
const SQLOps::OpSimulator::PointCoder SQLOps::OpSimulator::POINT_CODER(
		POINT_LIST, 1);

const SQLOps::OpSimulator::ActionEntry SQLOps::OpSimulator::ACTION_LIST[] = {
	UTIL_NAME_CODER_ENTRY(ACTION_INTERRUPT),
	UTIL_NAME_CODER_ENTRY(ACTION_INDEX_LOST),
	UTIL_NAME_CODER_ENTRY(ACTION_UPDATE_ROW),
	UTIL_NAME_CODER_ENTRY(ACTION_UPDATE_LIST_INVALID)
};
const SQLOps::OpSimulator::ActionCoder SQLOps::OpSimulator::ACTION_CODER(
		ACTION_LIST, 1);

SQLOps::OpSimulator::OpSimulator(SQLValues::VarAllocator &varAlloc) :
		inactiveList_(varAlloc),
		activeList_(varAlloc),
		lastTupleId_(std::numeric_limits<int64_t>::max()) {
}

void SQLOps::OpSimulator::addEntry(const Entry &entry) {
	if (entry.point_ == PONIT_INIT) {
		activeList_.push_back(entry);
	}
	else {
		inactiveList_.push_back(entry);
	}
}

void SQLOps::OpSimulator::handlePoint(
		OpSimulator *simulator, PointType point) {
	if (simulator != NULL) {
		simulator->handlePoint(point);
	}
}

void SQLOps::OpSimulator::handlePoint(PointType point) {
	for (EntryList::iterator it = inactiveList_.begin();
			it != inactiveList_.end();) {
		if (it->point_ == point) {
			activeList_.push_back(*it);
			it = inactiveList_.erase(it);
		}
		else {
			++it;
		}
	}
}

bool SQLOps::OpSimulator::findAction(ActionType action) const {
	for (size_t i = 0; i < 2; i++) {
		const bool forActive = (i == 0);
		const EntryList &list = (forActive ? activeList_ : inactiveList_);
		for (EntryList::const_iterator it = list.begin(); it != list.end(); ++it) {
			if (it->action_ == action) {
				return true;
			}
		}
	}
	return false;
}

bool SQLOps::OpSimulator::nextAction(
		ActionType action, int64_t *param, bool once) {
	for (EntryList::iterator it = activeList_.begin();
			it != activeList_.end(); ++it) {
		if (it->action_ == action) {
			if (param != NULL) {
				*param = it->param_;
			}
			if (once) {
				activeList_.erase(it);
			}
			return true;
		}
	}
	return false;
}

bool SQLOps::OpSimulator::nextInterruptionAction(int64_t &interval) {
	return nextAction(ACTION_INTERRUPT, &interval, true);
}

void SQLOps::OpSimulator::acceptTupleId(int64_t tupleId) {
	lastTupleId_ = tupleId;
}

bool SQLOps::OpSimulator::getLastTupleId(int64_t &tupleId) {
	tupleId = lastTupleId_;
	return (tupleId != std::numeric_limits<int64_t>::max());
}


SQLOps::OpSimulator::Entry::Entry() :
		point_(END_POINT),
		action_(END_ACTION),
		param_(0) {
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

	addOptimizationProfile(info.optimization_, dest.optimization_);

	if (info.index_ != NULL) {
		for (util::AllocVector<AnalysisIndexInfo>::iterator it =
				info.index_->begin(); it != info.index_->end(); ++it) {
			addIndexProfile(id, *it);
		}
	}
}

void SQLOps::OpProfiler::addOptimizationProfile(
		const OptimizationList *src, LocalOptimizationInfoList &dest) {
	if (src == NULL || src->empty()) {
		return;
	}

	if (dest.empty()) {
		dest.push_back(LocalOptimizationInfo());
	}
	LocalOptimizationInfo &destInfo = dest.front();

	for (OptimizationList::const_iterator it = src->begin();	
			it != src->end(); ++it) {
		SQLValues::ProfileElement &destElem = destInfo.get(it->type_);

		SQLValues::ProfileElement srcElem;
		srcElem.candidate_ = it->candidate_;
		srcElem.target_ = it->target_;

		destElem.merge(srcElem);
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

	ref.modification_ = info.modification_;
	ref.bulk_ = info.bulk_;
	ref.hit_ = info.hit_;
	ref.mvccHit_ = info.mvccHit_;
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

	ref.optimization_ = getOptimizations(src);

	ref.index_ = NULL;
	LocalIndexInfoList &indexList = src.index_;
	if (!indexList.empty()) {
		src.indexRef_.clear();
		for (LocalIndexInfoList::iterator it = indexList.begin();
				it != indexList.end(); ++it) {
			AnalysisIndexInfo indexInfo;
			if (!findAnalysisIndexInfo(*it, indexInfo)) {
				continue;
			}
			src.indexRef_.push_back(indexInfo);
		}
		ref.index_ = &src.indexRef_;
	}

	ref.op_ = NULL;
	SubMap &subMap = src.sub_;
	if (!subMap.empty()) {
		assert(profiler != NULL);
		src.subRef_.clear();
		for (SubMap::iterator it = subMap.begin(); it != subMap.end(); ++it) {
			src.subRef_.push_back(profiler->getAnalysisInfo(&it->second));
		}
		reduceNoopInfo(src.subRef_);
		ref.op_ = &src.subRef_;
	}

	ref.actualTime_ =
			static_cast<int64_t>(nanoTimeToMillis(ref.actualNanoTime_));

	return ref;
}

SQLOps::OpProfiler::OptimizationList* SQLOps::OpProfiler::getOptimizations(
		LocalInfo &src) {
	OptimizationList &destList = src.optRef_;
	destList.clear();

	if (src.optimization_.empty()) {
		return NULL;
	}
	const LocalOptimizationInfo &srcInfo = src.optimization_.front();

	for (size_t i = 0; i < SQLOpTypes::END_OPT; i++) {
		const SQLOpTypes::Optimization type =
				static_cast<SQLOpTypes::Optimization>(i);

		const SQLValues::ProfileElement &elem = srcInfo.get(type);
		if (elem.candidate_ <= 0) {
			continue;
		}

		AnalysisOptimizationInfo destInfo;
		destInfo.type_ = type;
		destInfo.candidate_ = elem.candidate_;
		destInfo.target_ = elem.target_;
		destList.push_back(destInfo);
	}

	if (destList.empty()) {
		return NULL;
	}

	return &destList;
}

bool SQLOps::OpProfiler::findAnalysisIndexInfo(
		LocalIndexInfo &src, AnalysisIndexInfo &dest) {
	if (src.ref_.empty()) {
		return false;
	}

	dest = src.ref_.front();

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

	return true;
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

void SQLOps::OpProfiler::reduceNoopInfo(
		util::AllocVector<AnalysisInfo> &subInfoList) {
	size_t noopCount = 0;
	for (util::AllocVector<AnalysisInfo>::iterator it = subInfoList.begin();
			it != subInfoList.end(); ++it) {
		if (isNoopInfo(*it)) {
			noopCount++;
		}
	}

	if (noopCount == 0 || noopCount == subInfoList.size()) {
		return;
	}

	for (util::AllocVector<AnalysisInfo>::iterator it = subInfoList.begin();
			it != subInfoList.end();) {
		if (isNoopInfo(*it)) {
			it = subInfoList.erase(it);
		}
		else {
			++it;
		}
	}
}

bool SQLOps::OpProfiler::isNoopInfo(const AnalysisInfo &info) {
	if (info.op_ != NULL) {
		bool noopOnly = false;
		for (util::AllocVector<AnalysisInfo>::iterator it = info.op_->begin();
				it != info.op_->end(); ++it) {
			if (!isNoopInfo(*it)) {
				return false;
			}
			noopOnly = true;
		}
		if (noopOnly) {
			return true;
		}
	}

	return (info.type_ == SQLOpTypes::OP_SELECT_NONE);
}


SQLOps::OpProfiler::LocalInfo::LocalInfo(
		const util::StdAllocator<void, void> &alloc) :
		optimization_(alloc),
		index_(alloc),
		sub_(alloc),
		optRef_(alloc),
		indexRef_(alloc),
		subRef_(alloc),
		ref_(alloc) {
}


SQLValues::ProfileElement& SQLOps::OpProfiler::LocalOptimizationInfo::get(
		SQLOpTypes::Optimization type) {
	void *addr = this;
	void *elemAddr = static_cast<uint8_t*>(addr) + getOffset(type);
	return *static_cast<SQLValues::ProfileElement*>(elemAddr);
}

const SQLValues::ProfileElement& SQLOps::OpProfiler::LocalOptimizationInfo::get(
		SQLOpTypes::Optimization type) const {
	const void *addr = this;
	const void *elemAddr = static_cast<const uint8_t*>(addr) + getOffset(type);
	return *static_cast<const SQLValues::ProfileElement*>(elemAddr);
}

size_t SQLOps::OpProfiler::LocalOptimizationInfo::getOffset(
		SQLOpTypes::Optimization type) {
	typedef SQLExprs::ExprProfile ExprProfile;
	typedef SQLValues::ValueProfile ValueProfile;
	switch (type) {
	case SQLOpTypes::OPT_VALUE_COMP_NO_NULL:
		return getValueOffset() + offsetof(ValueProfile, noNull_);
	case SQLOpTypes::OPT_VALUE_COMP_NO_SWITCH:
		return getValueOffset() + offsetof(ValueProfile, noSwitch_);
	case SQLOpTypes::OPT_EXPR_COMP_CONST:
		return getExprOffset() + offsetof(ExprProfile, compConst_);
	case SQLOpTypes::OPT_EXPR_COMP_COLUMNS:
		return getExprOffset() + offsetof(ExprProfile, compColumns_);
	case SQLOpTypes::OPT_EXPR_COND_IN_LIST:
		return getExprOffset() + offsetof(ExprProfile, condInList_);
	case SQLOpTypes::OPT_EXPR_OUT_NO_NULL:
		return getExprOffset() + offsetof(ExprProfile, outNoNull_);
	case SQLOpTypes::OPT_OP_COMP_CONST:
		return offsetof(LocalOptimizationInfo, compConst_);
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

size_t SQLOps::OpProfiler::LocalOptimizationInfo::getExprOffset() {
	return offsetof(LocalOptimizationInfo, exprProfile_);
}

size_t SQLOps::OpProfiler::LocalOptimizationInfo::getValueOffset() {
	typedef SQLExprs::ExprProfile ExprProfile;
	return getExprOffset() + offsetof(ExprProfile, valueProfile_);
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

SQLOps::OpProfilerOptimizationEntry&
SQLOps::OpProfilerEntry::getOptimizationEntry() {
	if (optimization_.get() == NULL) {
		if (localInfo_.optimization_.empty()) {
			localInfo_.optimization_.push_back(LocalOptimizationInfo());
		}
		optimization_ = ALLOC_UNIQUE(
				alloc_, OpProfilerOptimizationEntry,
				localInfo_.optimization_.front());
	}
	return *optimization_;
}

SQLOps::OpProfilerIndexEntry& SQLOps::OpProfilerEntry::getIndexEntry(
		uint32_t ordinal) {
	while (ordinal >= localInfo_.index_.size()) {
		localInfo_.index_.push_back(LocalIndexInfo(alloc_));
	}
	if (index_.get() == NULL) {
		index_ = ALLOC_UNIQUE(
				alloc_, OpProfilerIndexEntry, alloc_, &localInfo_.index_);
	}
	index_->updateOrdinal(ordinal);
	return *index_;
}



const util::NameCoderEntry<SQLOps::OpProfilerIndexEntry::ModificationType>
		SQLOps::OpProfilerIndexEntry::Coders::MOD_LIST[] = {
	UTIL_NAME_CODER_ENTRY(MOD_UPDATE_ROW),
	UTIL_NAME_CODER_ENTRY(MOD_REMOVE_ROW),
	UTIL_NAME_CODER_ENTRY(MOD_REMOVE_ROW_ARRAY),
	UTIL_NAME_CODER_ENTRY(MOD_REMOVE_CHUNK)
};

const util::NameCoder<
		SQLOps::OpProfilerIndexEntry::ModificationType,
		SQLOps::OpProfilerIndexEntry::END_MOD>
		SQLOps::OpProfilerIndexEntry::Coders::MOD_CODER(MOD_LIST, 1);


SQLOps::OpProfilerOptimizationEntry::OpProfilerOptimizationEntry(
		LocalOptimizationInfo &localInfo) :
		localInfo_(localInfo) {
}

SQLExprs::ExprProfile* SQLOps::OpProfilerOptimizationEntry::getExprProfile(
		OpProfilerOptimizationEntry *entry) {
	if (entry == NULL) {
		return NULL;
	}
	return &entry->localInfo_.exprProfile_;
}

SQLValues::ProfileElement& SQLOps::OpProfilerOptimizationEntry::get(
		SQLOpTypes::Optimization type) {
	return localInfo_.get(type);
}

const SQLValues::ProfileElement& SQLOps::OpProfilerOptimizationEntry::get(
		SQLOpTypes::Optimization type) const {
	return localInfo_.get(type);
}


SQLOps::OpProfilerIndexEntry::OpProfilerIndexEntry(
		const util::StdAllocator<void, void> &alloc, LocalIndexInfoList *localInfoList) :
		executionCount_(0),
		localInfoList_(localInfoList),
		localInfoOrdinal_(std::numeric_limits<uint32_t>::max()),
		alloc_(alloc),
		nameMap_(alloc),
		modification_(END_MOD),
		bulk_(0),
		hit_(0),
		mvccHit_(0) {
}

bool SQLOps::OpProfilerIndexEntry::isEmpty() {
	return (localInfoOrdinal_ == std::numeric_limits<uint32_t>::max() ||
			getLocalInfo().columnNameList_.empty());
}

void SQLOps::OpProfilerIndexEntry::setModificationType(
		ModificationType modification) {
	modification_ = modification;
}

const char8_t* SQLOps::OpProfilerIndexEntry::findIndexName() {
	const util::AllocString &name = getLocalInfo().name_;
	if (name.empty()) {
		return NULL;
	}
	return name.c_str();
}

void SQLOps::OpProfilerIndexEntry::setIndexName(const char8_t *name) {
	getLocalInfo().name_ = name;
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
	OpProfiler::NameList &nameList = getLocalInfo().columnNameList_;

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
		if (cond.firstHitOnly_) {
			destCond.condition_ = (cond.descending_ ?
					AnalysisIndexCondition::CONDITION_RANGE_LAST :
					AnalysisIndexCondition::CONDITION_RANGE_FIRST);
		}
		else {
			destCond.condition_ = AnalysisIndexCondition::CONDITION_RANGE;
		}
		break;
	case SQLType::OP_EQ:
		destCond.condition_ = AnalysisIndexCondition::CONDITION_EQ;
		break;
	default:
		return;
	}

	OpProfiler::IndexCondList &condList = getLocalInfo().conditionListRef_;
	const size_t condIndex = condList.size();

	if (condList.empty() && cond.bulkOrCount_ > 1) {
		bulk_ = static_cast<int64_t>(cond.bulkOrCount_);
	}

	condList.push_back(destCond);

	OpProfiler::NameList &nameList = getLocalInfo().conditionColumnList_;
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

void SQLOps::OpProfilerIndexEntry::addHitCount(int64_t count, bool onMvcc) {
	(onMvcc ? mvccHit_ : hit_) += count;
}

void SQLOps::OpProfilerIndexEntry::updateOrdinal(uint32_t ordinal) {
	if (localInfoOrdinal_ == ordinal) {
		return;
	}
	update();
	localInfoOrdinal_ = ordinal;
}

void SQLOps::OpProfilerIndexEntry::update() {
	if (localInfoOrdinal_ == std::numeric_limits<uint32_t>::max()) {
		return;
	}

	LocalIndexInfo &localInfo = getLocalInfo();
	if (localInfo.ref_.empty()) {
		localInfo.ref_.push_back(AnalysisIndexInfo());
	}

	AnalysisIndexInfo &ref = localInfo.ref_.front();
	ref.executionCount_ = executionCount_;
	ref.actualTime_ = static_cast<int64_t>(
			OpProfiler::nanoTimeToMillis(searchWatch_.elapsedNanos()));

	ref.modification_ = modification_;
	ref.bulk_ = bulk_;
	ref.hit_ = hit_;
	ref.mvccHit_ = mvccHit_;
}

SQLOps::OpProfiler::LocalIndexInfo&
SQLOps::OpProfilerIndexEntry::getLocalInfo() {
	assert(localInfoList_ != NULL);
	assert(localInfoOrdinal_ < localInfoList_->size());

	return (*localInfoList_)[localInfoOrdinal_];
}


const util::NameCoderEntry<SQLOpTypes::Type>
		SQLOpUtils::AnalysisInfo::OP_TYPE_LIST[] = {
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_SEMI_FILTERING),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_UNIQUE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_NO_INDEXED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_FULL),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_RANGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_INDEX),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_META),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SCAN_CONTAINER_VISITED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SELECT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SELECT_NONE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SELECT_PIPE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_LIMIT),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SORT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_SORT_NWAY),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW_PARTITION),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW_RANK_PARTITION),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW_FRAME_PARTITION),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_WINDOW_MERGE),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_GRACE_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_SORTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_NESTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_COMP_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER_NESTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER_COMP_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_JOIN_OUTER_HASH),

	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_DISTINCT),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_DISTINCT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_BUCKET_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_RANGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_GROUP_RANGE_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_ALL),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_SORTED),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_DISTINCT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_DISTINCT_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_INTERSECT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_INTERSECT_HASH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_EXCEPT_MERGE),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OP_UNION_EXCEPT_HASH),
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
		optimization_(NULL),
		index_(NULL),
		op_(NULL),
		partial_(NULL),
		actualNanoTime_(0) {
}

SQLOpUtils::AnalysisRootInfo SQLOpUtils::AnalysisInfo::toRoot() const {
	AnalysisRootInfo dest;
	dest.op_ = op_;
	return dest;
}


SQLOpUtils::AnalysisRootInfo::AnalysisRootInfo() :
		op_(NULL) {
}


const util::NameCoderEntry<SQLOpTypes::Optimization>
		SQLOpUtils::AnalysisOptimizationInfo::OPT_TYPE_LIST[] = {
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_VALUE_COMP_NO_NULL),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_VALUE_COMP_NO_SWITCH),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_EXPR_COMP_CONST),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_EXPR_COMP_COLUMNS),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_EXPR_COND_IN_LIST),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_EXPR_OUT_NO_NULL),
	UTIL_NAME_CODER_ENTRY(SQLOpTypes::OPT_OP_COMP_CONST)
};

const util::NameCoder<
		SQLOpTypes::Optimization, SQLOpTypes::END_OPT>
		SQLOpUtils::AnalysisOptimizationInfo::OPT_TYPE_CODER(OPT_TYPE_LIST, 1);

SQLOpUtils::AnalysisOptimizationInfo::AnalysisOptimizationInfo() :
		type_(SQLOpTypes::END_OPT),
		candidate_(0),
		target_(0) {
}


const util::NameCoderEntry<SQLOpUtils::AnalysisIndexCondition::ConditionType>
		SQLOpUtils::AnalysisIndexCondition::CONDITION_LIST[] = {
	UTIL_NAME_CODER_ENTRY(CONDITION_RANGE),
	UTIL_NAME_CODER_ENTRY(CONDITION_RANGE_FIRST),
	UTIL_NAME_CODER_ENTRY(CONDITION_RANGE_LAST),
	UTIL_NAME_CODER_ENTRY(CONDITION_EQ)
};

const util::NameCoder<
		SQLOpUtils::AnalysisIndexCondition::ConditionType,
		SQLOpUtils::AnalysisIndexCondition::END_CONDITION>
		SQLOpUtils::AnalysisIndexCondition::CONDITION_CODER(CONDITION_LIST, 1);

SQLOpUtils::AnalysisIndexCondition::AnalysisIndexCondition() :
		column_(NULL),
		condition_(END_CONDITION) {
}


SQLOpUtils::AnalysisIndexInfo::AnalysisIndexInfo() :
		modification_(IndexProfiler::END_MOD),
		name_(NULL),
		columnList_(NULL),
		conditionList_(NULL),
		bulk_(0),
		executionCount_(0),
		actualTime_(0),
		hit_(0),
		mvccHit_(0) {
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
		TupleListReader **readerRef, WritableTuple *outTuple,
		bool inputMapping) {
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
			(inputMapping || code.getType() == SQLType::AGG_FIRST ?
					0 : code.getInput());
	const uint32_t column = code.getColumnPos();

	const TupleColumnType baseInColumnType = (
			code.getType() == SQLType::EXPR_COLUMN ?
					cxt.getInputType(input, column) :
					code.getColumnType());

	const bool inColumnUnified = ((code.getAttributes() &
					SQLExprs::ExprCode::ATTR_COLUMN_UNIFIED) != 0);
	const bool inColumnPromoting = inColumnUnified || (
			SQLValues::TypeUtils::toNonNullable(outColumn.getType()) !=
			SQLValues::TypeUtils::toNonNullable(baseInColumnType));

	const SQLExprs::ExprCode::InputSourceType srcType =
			cxt.getInputSourceType(input);
	const bool sourceSpecific =
			(srcType == SQLExprs::ExprCode::INPUT_READER ||
			srcType == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE ||
			srcType == SQLExprs::ExprCode::INPUT_READER_MULTI);

	const bool varGenerative =
			SQLExprs::ExprRewriter::findVarGenerativeExpr(expr) ||
			(srcType == SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE &&
					SQLValues::TypeUtils::isLob(baseInColumnType));
	const bool forColumn = (
			!varGenerative &&
			code.getType() == SQLType::EXPR_COLUMN &&
			sourceSpecific && !inColumnPromoting);
	const bool forSimpleAggr = (
			!varGenerative && !forColumn &&
			code.getType() == SQLType::AGG_FIRST &&
			expr.findChild() == NULL &&
			sourceSpecific);

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

	TupleColumnType inColumnType = outColumn.getType();
	if ((inSummaryColumn_ != NULL &&
			inSummaryColumn_->getNullsOffset() < 0) ||
			(code.getType() == SQLType::EXPR_COLUMN &&
					inSummaryColumn_ == NULL && !inColumnPromoting &&
					!SQLValues::TypeUtils::isNullable(baseInColumnType))) {
		inColumnType = SQLValues::TypeUtils::toNonNullable(inColumnType);
	}

	if (inSummaryColumn_ != NULL &&
			SQLValues::TypeUtils::isNullable(
					inSummaryColumn_->getTupleColumn().getType()) &&
			!SQLValues::TypeUtils::isNullable(inColumnType)) {
		inColumnType = SQLValues::TypeUtils::toNullable(inColumnType);
	}

	func_ = resolveFunc<const ExpressionWriter>(
			cxt, inColumnType, varGenerative);
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
SQLOpUtils::ExpressionWriter::resolveFunc(
		ExprFactoryContext &cxt, TupleColumnType inColumnType,
		bool varGenerative) const {
	SQLValues::ValueProfile valueProfile;

	Switcher switcher(*this, inColumnType, varGenerative);
	switcher.profile_ = &valueProfile;
	const Switcher::DefaultFuncType func = switcher.getWith<
			Op, Switcher::DefaultTraitsType>();

	SQLExprs::ExprProfile *profile = cxt.getProfile();
	if (profile != NULL) {
		profile->outNoNull_.merge(valueProfile.noNull_);
	}

	return func;
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


template<typename T, bool G>
inline void SQLOpUtils::ExpressionWriter::ByGeneral<T, G>::operator()() const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef typename T::Type TypeTag;
	typedef typename T::ValueType ValueType;

	assert(base_.cxtRef_->cxt_ != NULL);
	ConditionalVarContextScope<G> varScope(*base_.cxtRef_->cxt_);

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


SQLOpUtils::ExpressionWriter::Switcher::Switcher(
		const ExpressionWriter &base, TupleColumnType inColumnType,
		bool varGenerative) :
		base_(base),
		inColumnType_(inColumnType),
		varGenerative_(varGenerative),
		profile_(NULL) {
}

template<typename Op, typename Traits>
typename Traits::template Func<Op>::Type
SQLOpUtils::ExpressionWriter::Switcher::getWith() const {
	switch (base_.srcType_) {
	case SQLExprs::ExprCode::INPUT_READER:
		return getSubWith<Op, Traits, SQLExprs::ExprCode::INPUT_READER, false>();
	case SQLExprs::ExprCode::INPUT_READER_MULTI:
		return getSubWith<
				Op, Traits, SQLExprs::ExprCode::INPUT_READER_MULTI, false>();
	case SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE:
		return getSubWith<
				Op, Traits, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE, false>();
	case SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY:
		return getSubWith<
				Op, Traits, SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE_BODY, false>();
	default:
		assert(!base_.isForColumn());
		if (varGenerative_) {
			return getSubWith<Op, Traits, SQLExprs::ExprCode::END_INPUT, true>();
		}
		else {
			return getSubWith<Op, Traits, SQLExprs::ExprCode::END_INPUT, false>();
		}
	}
}

template<
		typename Op, typename Traits, SQLExprs::ExprCode::InputSourceType S,
		bool G>
typename Traits::template Func<Op>::Type
SQLOpUtils::ExpressionWriter::Switcher::getSubWith() const {
	return TypeSwitcher(inColumnType_).withProfile(profile_).getWith<
			Op, typename OpTraitsAt<Traits, S, G>::Type>();
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
				&cxtRef_, activeReaderRef_, &outTuple_,
				source.isInputMapping());
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

		if (!keyColumn.isOrdering() && keyColumnList.size() == 1 && longKeyOnly) {
			break;
		}

		const SQLValues::TupleComparator comp(
				keyColumnList, NULL, false, true, nullIgnorable);
		if (!comp.isDigestOnly(false)) {
			if (forSummary) {
				if (SQLValues::TypeUtils::isLob(type) ||
						SQLValues::TypeUtils::isLargeFixed(type) ||
						!keyColumn.isOrdering()) {
					break;
				}
			}
			else {
				if ((SQLValues::TypeUtils::isNullable(type) && !nullIgnorable) ||
						!SQLValues::TypeUtils::isNormalFixed(type) ||
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
		SQLExprs::ExprCode::InputSourceType srcType,
		const uint32_t *mappedInput) {
	entry_.code_ = &proj.getProjectionCode();

	entry_.cxt_ = &cxt;
	entry_.proj_ = &proj;
	entry_.forMiddle_ = forMiddle;
	entry_.nullIgnorable_ = nullIgnorable;
	entry_.keyColumnList_ = keyColumnList;

	OpCodeBuilder::Source builderSrc = OpCodeBuilder::ofContext(cxt);
	if (mappedInput != NULL) {
		assert(*mappedInput != std::numeric_limits<uint32_t>::max());
		builderSrc.mappedInput_ = *mappedInput;
		entry_.inputMapping_ = true;
	}

	builder_ = UTIL_MAKE_LOCAL_UNIQUE(builder_, OpCodeBuilder, builderSrc);
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
		cxt.setUpProjectionFactoryContext(*entry_.factoryCxt_, mappedInput);
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

bool SQLOpUtils::ExpressionListWriter::Source::isInputMapping() const {
	return entry_.inputMapping_;
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
		inputMapping_(false),
		keyColumnList_(NULL),
		outIndex_(std::numeric_limits<uint32_t>::max()) {
}


void SQLOpUtils::NonExecutableProjection::project(OpContext &cxt) const {
	static_cast<void>(cxt);
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}
