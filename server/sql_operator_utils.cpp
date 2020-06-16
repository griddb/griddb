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
#include "sql_common.h"


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
		const Expression *src, SQLType::AggregationPhase aggrPhase) {
	if (src == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	SQLExprs::ExprFactoryContext &cxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(cxt);
	cxt.setAggregationPhase(true, aggrPhase);

	ProjectionCode code;
	code.getExprCode().setAttributes(SQLExprs::ExprCode::ATTR_GROUPING);

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
		const Projection &src) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();

	SQLOps::Projection *dest;
	{
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
		factoryCxt.setBaseExpression(&src);
		dest = &createProjection(
				SQLOpTypes::END_PROJ, src.getProjectionCode());
	}

	{
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

		const ProjectionCode &srcCode = src.getProjectionCode();

		const SQLType::AggregationPhase aggrPhase =
				srcCode.getAggregationPhase();
		if (aggrPhase != SQLType::END_AGG_PHASE) {
			factoryCxt.setAggregationPhase(false, aggrPhase);
		}

		if (srcCode.getColumnTypeList() != NULL) {
			factoryCxt.setAggregationTypeListRef(srcCode.getColumnTypeList());
		}

		factoryCxt.setBaseExpression(&src);
		rewriter_.rewrite(factoryCxt, src, dest);
	}

	ProjectionCode *code = &dest->getProjectionCode();

	const SQLValues::CompColumnList *keyList = code->getKeyList();
	if (keyList != NULL) {
		const bool unified = ((code->getExprCode().getAttributes() &
				SQLExprs::ExprCode::ATTR_COLUMN_UNIFIED) != 0);
		code->setKeyList(&rewriter_.rewriteCompColumnList(
				getExprFactoryContext(), *keyList, unified));
	}

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
			SQLOps::Projection &outProj = *dest;

			dest = &createProjection(SQLOpTypes::PROJ_AGGR_OUTPUT);
			dest->addChain(outProj);
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
	}
	else {
		for (Projection::ChainIterator it(src); it.exists(); it.next()) {
			dest->addChain(rewriteProjection(it.get()));
		}
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

	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

	if (factoryCxt.getAggregationPhase(true) == SQLType::END_AGG_PHASE) {
		factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ADVANCE_PIPE);
	}

	dest.addChild(&rewriter_.rewrite(factoryCxt, *filterPred, NULL));
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

std::pair<SQLOps::Projection*, SQLOps::Projection*>
SQLOps::OpCodeBuilder::arrangeProjections(
		OpCode &code, bool withFilter, bool withLimit) {
	const Projection *srcPipe = code.getPipeProjection();
	const Projection *srcFinish = code.getFinishProjection();
	assert(srcPipe != NULL);

	code.setPipeProjection(NULL);
	code.setFinishProjection(NULL);

	Projection *destPipe = NULL;
	Projection *destFinish = NULL;
	if (srcFinish == NULL &&
			srcPipe->getProjectionCode().getType() ==
					SQLOpTypes::PROJ_OUTPUT &&
			(srcPipe->getCode().getAttributes() &
					SQLExprs::ExprCode::ATTR_AGGR_ARRANGED) == 0 &&
			(srcPipe->getCode().getAttributes() &
					(SQLExprs::ExprCode::ATTR_AGGREGATED |
					SQLExprs::ExprCode::ATTR_GROUPING)) != 0) {
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
			destFinish = &rewriteProjection(*srcPipe);
		}
	}
	else {
		destPipe = &rewriteProjection(*srcPipe);
		if (srcFinish != NULL) {
			destFinish = &rewriteProjection(*srcFinish);
		}
	}

	if (withLimit) {
		Projection *&target = (destFinish == NULL ? destPipe : destFinish);
		target = &createLimitedProjection(*target, code);
	}

	if (withFilter) {
		destPipe = &createFilteredProjection(*destPipe, code);
	}

	return std::make_pair(destPipe, destFinish);
}

SQLOps::OpCode SQLOps::OpCodeBuilder::toExecutable(const OpCode &src) {
	SQLExprs::ExprFactoryContext &factoryCxt = getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
	factoryCxt.setPlanning(false);

	SQLOps::OpCode dest = src;

	{
		const Projection *proj = src.getMiddleProjection();
		if (proj != NULL) {
			dest.setMiddleProjection(&rewriteProjection(*proj));

			ColumnTypeList typeList(getAllocator());
			resolveColumnTypeList(*proj, typeList);

			factoryCxt.clearInputTypes();

			const uint32_t index = 0;
			const size_t columnCount = typeList.size();
			for (uint32_t pos = 0; pos < columnCount; pos++) {
				factoryCxt.setInputType(index, pos, typeList[pos]);
				factoryCxt.setInputNullable(index, false);
			}
		}
	}

	std::pair<Projection*, Projection*> projs =
			arrangeProjections(dest, false, false);

	dest.setPipeProjection(projs.first);
	dest.setFinishProjection(projs.second);

	{
		const Expression *expr = src.getFilterPredicate();
		if (expr != NULL) {
			dest.setFilterPredicate(&rewriter_.rewrite(factoryCxt, *expr, NULL));
		}
	}
	{
		const Expression *expr = src.getJoinPredicate();
		if (expr != NULL) {
			dest.setJoinPredicate(&rewriter_.rewrite(factoryCxt, *expr, NULL));
		}
	}
	{
		const SQLValues::CompColumnList *list = src.findKeyColumnList();
		if (list != NULL) {
			dest.setKeyColumnList(&rewriter_.rewriteCompColumnList(
					factoryCxt, *list, isInputUnified(dest)));
		}
	}

	return dest;
}

void SQLOps::OpCodeBuilder::addColumnUsage(const OpCode &code, bool withId) {
	assert(code.findKeyColumnList() == NULL);
	assert(code.getJoinPredicate() == NULL);
	assert(code.getMiddleProjection() == NULL);
	assert(code.getFinishProjection() == NULL);

	rewriter_.addColumnUsage(code.getPipeProjection(), withId);
	rewriter_.addColumnUsage(code.getFilterPredicate(), withId);
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
		const OpCode &code) {
	const Projection *proj = code.getFinishProjection();
	if (proj == NULL) {
		proj = code.getPipeProjection();
	}
	return proj;
}

const SQLOps::Projection* SQLOps::OpCodeBuilder::findOutputProjection(
		const Projection &src) {
	if (src.getProjectionCode().getType() == SQLOpTypes::PROJ_OUTPUT) {
		return &src;
	}

	for (Projection::ChainIterator it(src); it.exists(); it.next()) {
		const SQLOps::Projection *foundProj = findOutputProjection(it.get());
		if (foundProj != NULL) {
			return foundProj;
		}
	}

	return NULL;
}

void SQLOps::OpCodeBuilder::resolveColumnTypeList(
		const OpCode &code, ColumnTypeList &typeList) {
	const Projection *proj = findOutputProjection(code);
	if (proj == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
	resolveColumnTypeList(*proj, typeList);
}

void SQLOps::OpCodeBuilder::resolveColumnTypeList(
		const Projection &proj, ColumnTypeList &typeList) {
	for (SQLExprs::Expression::Iterator it(proj); it.exists(); it.next()) {
		const TupleColumnType type = it.get().getCode().getColumnType();
		typeList.push_back(type);
		if (SQLValues::TypeUtils::isNull(type)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
		}
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
	}
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
