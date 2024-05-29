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

#include "sql_operator_scan.h"
#include "sql_utils_container.h"


const SQLOps::OpProjectionRegistrar
SQLScanOps::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLScanOps::Registrar::operator()() const {
	add<SQLOpTypes::OP_SCAN, Scan>();
	add<SQLOpTypes::OP_SCAN_SEMI_FILTERING, ScanSemiFiltering>();
	add<SQLOpTypes::OP_SCAN_UNIQUE, ScanUnique>();
	add<SQLOpTypes::OP_SCAN_NO_INDEXED, ScanNoIndex>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_FULL, ScanContainerFull>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_RANGE, ScanContainerRange>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_INDEX, ScanContainerIndex>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_META, ScanContainerMeta>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_VISITED, ScanContainerVisited>();
	add<SQLOpTypes::OP_SELECT, Select>();
	add<SQLOpTypes::OP_SELECT_NONE, SelectPipe>();
	add<SQLOpTypes::OP_SELECT_PIPE, SelectPipe>();
	add<SQLOpTypes::OP_LIMIT, Limit>();

	addProjectionCustom<SQLOpTypes::PROJ_OUTPUT, OutputProjection>();
	addProjection<SQLOpTypes::PROJ_AGGR_PIPE, AggrPipeProjection>();
	addProjection<SQLOpTypes::PROJ_AGGR_OUTPUT, AggrOutputProjection>();
	addProjection<SQLOpTypes::PROJ_MULTI_OUTPUT, MultiOutputProjection>();
	addProjection<SQLOpTypes::PROJ_FILTER, FilterProjection>();
	addProjection<SQLOpTypes::PROJ_LIMIT, LimitProjection>();
	addProjection<SQLOpTypes::PROJ_SUB_LIMIT, SubLimitProjection>();
}


SQLScanOps::ScanCursorAccessor&
SQLScanOps::BaseScanContainerOperator::getScanCursorAccessor(
		OpContext &cxt) const {
	const uint32_t inIndex = 0;
	const bool forLatch = true;

	if (cxt.getResourceRef(inIndex, forLatch).get() == NULL) {
		SQLValues::VarAllocator &varAlloc =
				cxt.getValueContext().getVarAllocator();
		ScanCursorAccessor::Source source(
				varAlloc, getCode().getContainerLocation(), NULL,
				cxt.getSource());

		const SQLOps::OpConfig *config = getCode().getConfig();

		source.indexLimit_ = SQLOps::OpConfig::resolve(
				SQLOpTypes::CONF_INDEX_LIMIT, config);
		source.memLimit_ = SQLOps::OpConfig::resolve(
				SQLOpTypes::CONF_WORK_MEMORY_LIMIT, config);

		const SQLOps::OpConfig::Value scanCount = SQLOps::OpConfig::resolve(
				SQLOpTypes::CONF_INTERRUPTION_SCAN_COUNT, config);
		source.partialExecSizeRange_.first =
				static_cast<uint64_t>(std::max<int64_t>(scanCount, 0));
		source.partialExecSizeRange_.second =
				static_cast<uint64_t>(std::max<int64_t>(
						cxt.getNextCountForSuspend(), 0));

		const SQLOps::OpConfig::Value countBased = SQLOps::OpConfig::resolve(
				SQLOpTypes::CONF_SCAN_COUNT_BASED, config);
		source.partialExecCountBased_ = (countBased > 0);

		util::AllocUniquePtr<ScanCursorAccessor> accessor(
				ScanCursorAccessor::create(source));
		cxt.setLatchResource(inIndex, accessor);
	}

	return cxt.getResourceRef(
			inIndex, forLatch).resolve().resolveAs<ScanCursorAccessor>();
}

util::AllocUniquePtr<SQLScanOps::ScanCursor>::ReturnType
SQLScanOps::BaseScanContainerOperator::getScanCursor(OpContext &cxt) const {
	const uint32_t inIndex = 0;
	const bool forLatch = true;
	const SQLOps::OpStore::ResourceRef &accessorRef =
			cxt.getResourceRef(inIndex, forLatch);

	const uint32_t cursorIndex = 0;
	util::AllocUniquePtr<void> &resource = cxt.getResource(cursorIndex);
	if (resource.isEmpty()) {
		ScanCursorAccessor &accessor =
				accessorRef.resolve().resolveAs<ScanCursorAccessor>();
		resource = accessor.createCursor(accessorRef);
	}

	util::AllocUniquePtr<ScanCursor> cursor(
			resource.resolveAs<ScanCursor::Holder>().attach());
	{
		const Projection *midProj = getCode().getMiddleProjection();
		if (midProj != NULL) {
			SQLOps::ColumnTypeList typeList(cxt.getAllocator());
			OpCodeBuilder::resolveColumnTypeList(*midProj, typeList, NULL);

			if (cursor->getOutputIndex() ==
					std::numeric_limits<uint32_t>::max()) {
				const uint32_t outIndex = cxt.createLocal(&typeList);
				cursor->setOutputIndex(outIndex);
			}

			SQLExprs::TupleColumnList *columnList =
					ALLOC_NEW(cxt.getAllocator()) SQLExprs::TupleColumnList(
							cxt.getAllocator());
			columnList->resize(typeList.size());
			TupleList::Info info;
			info.columnTypeList_ = &typeList[0];
			info.columnCount_ = typeList.size();
			info.getColumns(&(*columnList)[0], columnList->size());

			cxt.getExprContext().setReader(
					inIndex, &cxt.getExprContext().getReader(inIndex), columnList);
		}
	}

	return cursor;
}

void SQLScanOps::BaseScanContainerOperator::unbindScanCursor(
		OpContext *cxt, SQLScanOps::ScanCursor &cursor) {
	if (cxt == NULL || cxt->isAllInputCompleted()) {
		cursor.setOutputIndex(std::numeric_limits<uint32_t>::max());
	}
}


void SQLScanOps::Scan::compile(OpContext &cxt) const {
	const OpCode &baseCode = getCode();

	if (isDrivingInputFound(baseCode)) {
		generateIndexJoinPlan(cxt, baseCode);
	}
	else {
		ScanCursorAccessor &accessor = getScanCursorAccessor(cxt);
		if (!generateMainScanPlan(cxt, accessor, baseCode)) {
			cxt.setPlanPending();
		}
	}
}

bool SQLScanOps::Scan::generateMainScanPlan(
		OpContext &cxt, ScanCursorAccessor &accessor, const OpCode &baseCode) {
	if (generateCheckedScanPlan(cxt, accessor, baseCode)) {
		return true;
	}

	OpPlan &plan = cxt.getPlan();

	bool indexPlanned;
	const SQLExprs::IndexSelector *selector =
			selectIndex(cxt, accessor, baseCode, plan, indexPlanned);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	ProjectionSet projSet;
	setUpMainScanPlanBuilder(
			builder, selector, indexPlanned, baseCode, projSet);

	if (selector != NULL) {
		return generateIndexScanNodes(
				builder, accessor, baseCode, *selector, projSet, plan);
	}

	generateNoIndexedScanNodes(indexPlanned, baseCode, projSet, plan);
	return true;
}

void SQLScanOps::Scan::generateEmptyPlan(OpContext &cxt) {
	OpPlan &plan = cxt.getPlan();

	ColumnTypeList outTypeList(cxt.getAllocator());
	OpCodeBuilder::getColumnTypeListByColumns(
			cxt.getOutputColumnList(0), outTypeList);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	builder.setUpNoopNodes(plan, outTypeList);
}

void SQLScanOps::Scan::generateNoIndexedScanPlan(
		OpContext &cxt, const OpCode &baseCode) {
	OpPlan &plan = cxt.getPlan();

	const SQLExprs::IndexSelector *selector = NULL;
	const bool indexPlanned = false;

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	ProjectionSet projSet;
	setUpMainScanPlanBuilder(
			builder, selector, indexPlanned, baseCode, projSet);

	generateNoIndexedScanNodes(indexPlanned, baseCode, projSet, plan);
}

bool SQLScanOps::Scan::generateCheckedScanPlan(
		OpContext &cxt, ScanCursorAccessor &accessor, const OpCode &baseCode) {
	if (!(accessor.isRowDuplicatable() || accessor.isIndexLost())) {
		return false;
	}

	OpPlan &plan = cxt.getPlan();
	OpNode &node = plan.createNode((accessor.isRowDuplicatable() ?
			SQLOpTypes::OP_SCAN_UNIQUE :
			SQLOpTypes::OP_SCAN_NO_INDEXED));

	OpCode code = baseCode;
	code.setJoinPredicate(NULL);
	node.setCode(code);

	node.addInput(plan.getParentInput(0));
	plan.linkToAllParentOutput(node);
	return true;
}

void SQLScanOps::Scan::generateIndexJoinPlan(
		OpContext &cxt, const OpCode &baseCode) {
	OpPlan &plan = cxt.getPlan();

	assert(baseCode.getInputCount() >= 2);

	const uint32_t scanIn = INPUT_JOIN_SCAN;
	const uint32_t drivingIn = INPUT_JOIN_DRIVING;

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	const SQLOps::OpConfig *scanConfig = baseCode.getConfig();

	const SQLExprs::Expression *baseSemiPred = baseCode.getFilterPredicate();

	SQLValues::CompColumnList *keyColumnList =
			&builder.createCompColumnList();
	{
		SQLOpUtils::CompPosSet posSet(cxt.getAllocator());
		builder.extractJoinKeyColumns(
				baseSemiPred, NULL, *keyColumnList, posSet, NULL);
		if (keyColumnList->empty()) {
			keyColumnList = NULL;
		}
	}

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	const SQLExprs::Expression *scanFilterPred = NULL;
	if (baseSemiPred != NULL) {
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
		factoryCxt.setAggregationPhase(true, baseCode.getAggregationPhase());

		SQLExprs::Expression &srcExpr =
				rewriter.rewrite(factoryCxt, *baseSemiPred, NULL);
		scanFilterPred = &SQLExprs::ExprRewriter::retainSingleInputPredicate(
				factoryCxt, srcExpr, scanIn, false);
	}

	SQLExprs::Expression *semiPred;
	const OpNode *arrangedDrivingNode;
	generateIndexJoinDrivingNode(
			builder, baseCode, keyColumnList, baseSemiPred, semiPred,
			arrangedDrivingNode, plan);

	rewriter.activateColumnMapping(factoryCxt);

	{
		rewriter.clearColumnUsage();

		const bool withId = true;
		builder.addColumnUsage(baseCode, withId);
		rewriter.addInputColumnUsage(drivingIn);

		rewriter.setIdOfInput(drivingIn, false);
	}

	OpNode &scanNode = plan.createNode(SQLOpTypes::OP_SCAN_SEMI_FILTERING);
	{
		OpCode code;
		code.setConfig(scanConfig);

		code.setJoinPredicate(semiPred);
		code.setFilterPredicate(scanFilterPred);
		code.setContainerLocation(&baseCode.getContainerLocation());

		{
			SQLExprs::ExprRewriter::Scope rewriterScope(rewriter);
			rewriter.setInputUsage(drivingIn, false);
			code.setPipeProjection(&builder.createProjectionByUsage(false));
		}

		OpNode &node = scanNode;
		node.addInput(plan.getParentInput(scanIn));
		node.addInput(*arrangedDrivingNode);
		node.setCode(code);
	}

	rewriter.setIdProjected(true);

	OpNode &joinNode = plan.createNode(SQLOpTypes::OP_JOIN);
	{
		OpNode &node = joinNode;

		OpCode code = baseCode;
		code.setKeyColumnList(keyColumnList);
		code.setContainerLocation(NULL);
		code.setJoinType(SQLType::JOIN_INNER);

		node.addInput(scanNode);
		node.addInput(plan.getParentInput(drivingIn));
		node.setCode(builder.rewriteCode(code, false));
	}

	plan.linkToAllParentOutput(joinNode);
}

void SQLScanOps::Scan::generateIndexJoinDrivingNode(
		OpCodeBuilder &builder, const OpCode &baseCode,
		const SQLValues::CompColumnList *keyColumnList,
		const SQLExprs::Expression *baseSemiPred,
		SQLExprs::Expression *&semiPred, const OpNode *&drivingNode,
		OpPlan &plan) {
	const uint32_t scanIn = INPUT_JOIN_SCAN;
	const uint32_t drivingIn = INPUT_JOIN_DRIVING;
	const bool keyFront = (drivingIn == 0);

	semiPred = NULL;
	drivingNode = &plan.getParentInput(drivingIn);

	if (keyColumnList == NULL) {
		return;
	}

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	SQLExprs::ExprRewriter::Scope rewriterScope(rewriter);
	rewriter.activateColumnMapping(factoryCxt);

	rewriter.clearColumnUsage();
	rewriter.addKeyColumnUsage(drivingIn, *keyColumnList, keyFront);
	rewriter.addColumnUsage(baseSemiPred, false);

	SQLValues::CompColumnList *mappedInKeys;
	SQLValues::CompColumnList *mappedOutKeys;
	Projection *midInProj;
	Projection *pipeProj;
	{
		SQLExprs::ExprRewriter::Scope rewriterSubScope1(rewriter);

		rewriter.setInputUsage(scanIn, false);
		rewriter.setMappedInput(drivingIn, 0);

		const bool keyInputMapping = true;
		{
			SQLExprs::ExprRewriter::Scope rewriterSubScope2(rewriter);
			rewriter.setInputColumnUsage(drivingIn, true);
			mappedInKeys = &rewriter.remapCompColumnList(
					factoryCxt, *keyColumnList, drivingIn, keyFront, false,
					NULL, keyInputMapping);
		}

		midInProj = &builder.createKeyFilteredProjection(
				builder.createProjectionByUsage(false));

		rewriter.setInputProjected(true);

		mappedOutKeys = &rewriter.remapCompColumnList(
				factoryCxt, *keyColumnList, drivingIn, keyFront, false, NULL,
				keyInputMapping);
		Projection &midOutProj = builder.createProjectionByUsage(false);
		midOutProj.getProjectionCode().setKeyList(mappedOutKeys);

		Projection &outProj = builder.createProjectionByUsage(false);
		pipeProj = &builder.createMultiStageProjection(outProj, midOutProj);
	}

	{
		SQLOps::OpConfig &config = builder.createConfig();
		config.set(SQLOpTypes::CONF_SORT_ORDERED_UNIQUE, 1);

		OpCode code;
		code.setConfig(&config);
		code.setMiddleKeyColumnList(mappedInKeys);
		code.setKeyColumnList(mappedOutKeys);
		code.setMiddleProjection(midInProj);
		code.setPipeProjection(pipeProj);

		OpNode &node = plan.createNode(SQLOpTypes::OP_SORT);
		node.setCode(code);
		node.addInput(plan.getParentInput(drivingIn));
		drivingNode = &node;
	}

	if (baseSemiPred != NULL) {
		factoryCxt.setAggregationPhase(true, baseCode.getAggregationPhase());

		rewriter.setInputColumnUsage(scanIn, true);
		rewriter.setIdProjected(true);
		semiPred = &rewriter.rewritePredicate(factoryCxt, baseSemiPred);
	}
}

bool SQLScanOps::Scan::generateIndexScanNodes(
		OpCodeBuilder &builder, ScanCursorAccessor &accessor,
		const OpCode &baseCode, const SQLExprs::IndexSelector &selector,
		const ProjectionSet &projSet, OpPlan &plan) {
	const OpCode &arrangedCode = projSet.arrangedCode_;
	const SQLOpUtils::ProjectionPair &projections = projSet.projections_;

	const bool parentPending = (projections.second != NULL);
	accessor.setIndexSelection(selector);

	OpNode &rangeNode = OpCodeBuilder::setUpOutputNodesDetail(
			plan, NULL, parentPending, NULL,
			SQLOpTypes::OP_SCAN_CONTAINER_RANGE,
			projSet.distinctProj_, arrangedCode.getAggregationPhase(), NULL);
	{
		OpNode &node = rangeNode;
		OpCode code;
		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);
		node.setCode(code);

		node.addInput(plan.getParentInput(0));
	}

	{
		OpNode &node = plan.createNode(
				SQLOpTypes::OP_SCAN_CONTAINER_INDEX);

		OpCode code;
		code.setIndexConditionList(&selector.getConditionList());
		code.setPipeProjection(projSet.midInProj_);
		node.setCode(code);

		node.addInput(plan.getParentInput(0));
		if (arrangedCode.getInputCount() > 1) {
			node.addInput(plan.getParentInput(1));
		}

		rangeNode.addInput(node);
	}

	if (parentPending) {
		return false;
	}

	accessor.setRowIdFiltering();

	SQLOps::OpConfig &config = builder.createConfig(baseCode.getConfig());
	if (baseCode.getLimit() >= 0) {
		config.set(SQLOpTypes::CONF_LIMIT_INHERITANCE, 1);
	}

	for (size_t i = 0; i < 2; i++) {
		OpCode code = baseCode;
		code.setJoinPredicate(NULL);

		OpNode &node = plan.createNode((i == 0 ?
				SQLOpTypes::OP_SCAN_UNIQUE :
				SQLOpTypes::OP_SCAN_NO_INDEXED));
		node.setCode(code);
		code.setConfig(&config);

		node.addInput(plan.getParentInput(0));
		plan.linkToAllParentOutput(node);
	}
	return true;
}

void SQLScanOps::Scan::generateNoIndexedScanNodes(
		bool indexPlanned, const OpCode &baseCode,
		const ProjectionSet &projSet, OpPlan &plan) {
	const OpCode &arrangedCode = projSet.arrangedCode_;
	const SQLOpUtils::ProjectionPair &projections = projSet.projections_;

	const SQLOps::ContainerLocation &location =
			baseCode.getContainerLocation();
	const bool forMeta = isForMetaScan(location);

	const OpNode *inNode;
	SQLOpTypes::Type opType;
	if (indexPlanned) {
		inNode = plan.findNextNode(0);
		assert(inNode != NULL);
		opType = SQLOpTypes::OP_SELECT;
	}
	else if (forMeta) {
		inNode = &plan.getParentInput(0);
		opType = SQLOpTypes::OP_SCAN_CONTAINER_META;
	}
	else {
		inNode = &plan.getParentInput(0);
		opType = SQLOpTypes::OP_SCAN_CONTAINER_FULL;
	}

	{
		OpCode code = arrangedCode;

		if (opType != SQLOpTypes::OP_SELECT) {
			code.setContainerLocation(&location);
		}

		if (forMeta) {
			code.setMiddleProjection(projSet.midInProj_);
		}

		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);

		OpNode &node = plan.createNode(opType);
		node.setCode(code);
		node.addInput(*inNode);
		if (baseCode.getInputCount() > 1) {
			node.addInput(plan.getParentInput(1));
		}

		OpCodeBuilder::setUpOutputNodes(
				plan, NULL, node, projSet.distinctProj_,
				baseCode.getAggregationPhase());
	}
}

void SQLScanOps::Scan::setUpMainScanPlanBuilder(
		OpCodeBuilder &builder, const SQLExprs::IndexSelector *selector,
		bool indexPlanned, const OpCode &baseCode, ProjectionSet &projSet) {
	OpCode &arrangedCode = projSet.arrangedCode_;
	SQLOpUtils::ProjectionPair &projections = projSet.projections_;
	Projection *distinctProj = NULL;

	const bool indexScanCompleted = (indexPlanned && selector == NULL);

	const SQLOps::ContainerLocation &location =
			baseCode.getContainerLocation();
	const bool forMeta = isForMetaScan(location);

	arrangedCode = baseCode;
	if (indexScanCompleted) {
		builder.assignInputTypesByProjection(
				OpCodeBuilder::resolveOutputProjection(baseCode, NULL), NULL);
		projections.first = &builder.createIdenticalProjection(false, 0);
		arrangedCode = OpCode();
	}
	else if (!forMeta) {
		projections = builder.arrangeProjections(
				arrangedCode, true, true, &distinctProj);
		assert(projections.first != NULL);
	}

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	rewriter.activateColumnMapping(factoryCxt);

	if (!forMeta) {
		rewriter.clearColumnUsage();

		const bool withId = true;

		if (indexPlanned || selector != NULL) {
			rewriter.setIdOfInput(0, true);
		}
		else {
			OpCode code = baseCode;
			code.setJoinPredicate(NULL);

			builder.addColumnUsage(code, withId);
		}
	}

	projSet.midInProj_ = &builder.createProjectionByUsage(false);

	rewriter.setIdProjected(true);
	rewriter.setInputProjected(true);

	if (projections.first == NULL) {
		projections = builder.arrangeProjections(
				arrangedCode, true, true, &distinctProj);
	}

	projSet.distinctProj_ = distinctProj;
}

const SQLExprs::IndexSelector* SQLScanOps::Scan::selectIndex(
		OpContext &cxt, ScanCursorAccessor &accessor, const OpCode &baseCode,
		const OpPlan &plan, bool &indexPlanned) {
	indexPlanned = !plan.isEmpty();
	SQLExprs::IndexSelector *selector = NULL;
	do {
		const SQLOps::ContainerLocation &location =
				baseCode.getContainerLocation();
		const bool forMeta = isForMetaScan(location);

		const SQLExprs::Expression *expr = baseCode.getJoinPredicate();
		if (expr == NULL) {
			expr = baseCode.getFilterPredicate();
		}
		if (forMeta || !location.indexActivated_) {
			break;
		}

		if (indexPlanned || baseCode.getLimit() == 0 || accessor.isIndexLost()) {
			if (accessor.isIndexLost()) {
				indexPlanned = false;
			}
			break;
		}

		const Projection *pipeProj = baseCode.getPipeProjection();

		util::StackAllocator &alloc = cxt.getAllocator();

		ColumnTypeList inputTypeList(alloc);
		TupleList::Info inputInfo = getInputInfo(cxt, inputTypeList);

		SQLValues::ValueContext valueCxt(
				SQLValues::ValueContext::ofAllocator(alloc));
		selector = ALLOC_NEW(alloc) SQLExprs::IndexSelector(
				valueCxt, SQLType::EXPR_COLUMN, inputInfo);
		if (location.multiIndexActivated_) {
			selector->setMultiAndConditionEnabled(true);
		}

		accessor.getIndexSpec(
				cxt.getExtContext(), cxt.getInputColumnList(0), *selector);
		selector->setBulkGrouping(true);
		selector->selectDetail(pipeProj, expr);
		if (selector->isSelected()) {
			break;
		}

		const SQLExprs::IndexConditionList &condList =
				selector->getConditionList();
		if (condList.size() != 1) {
			break;
		}

		const SQLExprs::IndexCondition &cond = condList.front();
		if (cond.isNull() || cond.equals(false)) {
			break;
		}
		selector = NULL;
	}
	while (false);
	return selector;
}

bool SQLScanOps::Scan::isForMetaScan(
		const SQLOps::ContainerLocation &location) {
	return (location.type_ == SQLType::TABLE_META);
}

bool SQLScanOps::Scan::isDrivingInputFound(const OpCode &baseCode) {
	return (baseCode.getInputCount() >= 2);
}

TupleList::Info SQLScanOps::Scan::getInputInfo(
		OpContext &cxt, SQLOps::ColumnTypeList &typeList) {
	const SQLOps::TupleColumnList &columnList = cxt.getInputColumnList(0);
	for (SQLOps::TupleColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		typeList.push_back(it->getType());
	}
	assert(!typeList.empty());

	TupleList::Info info;
	info.columnTypeList_ = &typeList[0];
	info.columnCount_ = typeList.size();
	return info;
}


SQLScanOps::Scan::ProjectionSet::ProjectionSet() :
		distinctProj_(NULL),
		midInProj_(NULL) {
}


void SQLScanOps::ScanSemiFiltering::compile(OpContext &cxt) const {
	const OpCode &baseCode = getCode();
	ScanCursorAccessor &accessor = getScanCursorAccessor(cxt);

	if (!Scan::generateMainScanPlan(cxt, accessor, baseCode)) {
		cxt.setPlanPending();
	}
}


void SQLScanOps::ScanUnique::compile(OpContext &cxt) const {
	ScanCursorAccessor &accessor = getScanCursorAccessor(cxt);

	bool duplicatable;
	if (!detectRowDuplicatable(accessor, duplicatable)) {
		cxt.setPlanPending();
		return;
	}

	if (!accessor.isRowIdFiltering() && accessor.isIndexLost()) {
		OpPlan &plan = cxt.getPlan();
		OpNode &node = plan.createNode(SQLOpTypes::OP_SCAN_NO_INDEXED);
		node.setCode(getCode());
		node.addInput(plan.getParentInput(0));
		plan.linkToAllParentOutput(node);
		return;
	}

	if (duplicatable && !accessor.isIndexLost()) {
		const OpCode &baseCode = getCode();
		if (!generateUniqueScanPlan(cxt, accessor, baseCode)) {
			cxt.setPlanPending();
		}
	}
	else {
		assert(accessor.isRowIdFiltering());
		Scan::generateEmptyPlan(cxt);
	}
}

bool SQLScanOps::ScanUnique::generateUniqueScanPlan(
		OpContext &cxt, ScanCursorAccessor &accessor,
		const OpCode &baseCode) {
	OpPlan &plan = cxt.getPlan();

	assert(accessor.isRowDuplicatable());
	assert(baseCode.getInputCount() == 1);

	const bool indexLost = accessor.isIndexLost();
	const bool indexPlanned = (!plan.isEmpty() && !indexLost);
	const SQLOps::ContainerLocation &location =
			baseCode.getContainerLocation();

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	rewriter.activateColumnMapping(factoryCxt);
	rewriter.clearColumnUsage();

	const bool withId = true;
	builder.addColumnUsage(baseCode, withId);
	rewriter.setIdOfInput(0, true);

	Projection &midInProj = builder.createFilteredProjection(
			builder.createProjectionByUsage(false),
			baseCode.getFilterPredicate());

	Projection &visitedProj = builder.createProjectionByUsage(false);
	builder.replaceColumnToConstExpr(visitedProj);

	rewriter.setIdProjected(true);
	rewriter.setInputProjected(true);

	Projection &midOutProj = builder.createProjectionByUsage(false);

	OpCode tailBaseCode = baseCode;
	tailBaseCode.setFilterPredicate(NULL);

	Projection *distinctProj;
	SQLOpUtils::ProjectionPair projections = builder.arrangeProjections(
			tailBaseCode, true, true, &distinctProj);

	const bool aggregated = (projections.second != NULL);
	const bool parentPending = (aggregated && !indexLost && !indexPlanned);

	const OpNode *inNode;
	if (indexPlanned) {
		inNode = plan.findNextNode(0);
	}
	else {
		OpCode code;
		code.setContainerLocation(&location);
		code.setPipeProjection(&midInProj);

		OpNode &node = plan.createNode((indexLost ?
				SQLOpTypes::OP_SCAN_CONTAINER_FULL :
				SQLOpTypes::OP_SCAN_CONTAINER_RANGE));
		node.setCode(code);
		node.addInput(plan.getParentInput(0));

		inNode = &node;
	}

	if (parentPending) {
		return false;
	}

	const bool withVisited = !(indexLost && aggregated);

	OpNode *visitedScanNode = NULL;
	if (withVisited) {
		OpCode code;
		code.setContainerLocation(&location);
		code.setPipeProjection(&visitedProj);

		OpNode &node = plan.createNode(SQLOpTypes::OP_SCAN_CONTAINER_VISITED);
		node.setCode(code);
		node.addInput(plan.getParentInput(0));
		visitedScanNode = &node;
	}

	OpNode *groupNode = NULL;
	if (withVisited) {
		SQLValues::CompColumnList &keyColumnList =
				builder.createCompColumnList();
		{
			SQLValues::CompColumn keyColumn;
			keyColumn.setColumnPos(rewriter.getMappedIdColumn(0), true);
			keyColumn.setOrdering(false);
			keyColumnList.push_back(keyColumn);
		}

		OpCode code;
		if (aggregated) {
			code.setPipeProjection(&midOutProj);
		}
		else {
			code = tailBaseCode;
			code.setPipeProjection(projections.first);
			code.setContainerLocation(NULL);
		}
		code.setKeyColumnList(&keyColumnList);
		code.setUnionType(SQLType::UNION_EXCEPT);

		OpNode &node = plan.createNode(SQLOpTypes::OP_UNION);
		node.setCode(code);
		node.addInput(*inNode);
		node.addInput(*visitedScanNode);
		groupNode = &node;
	}

	const OpNode *tailNode = (groupNode == NULL ? inNode : groupNode);
	if (aggregated) {
		OpNode &node = plan.createNode(SQLOpTypes::OP_SELECT);

		Projection &mappedProj =
				builder.rewriteProjection(*baseCode.getPipeProjection());

		OpCode code = baseCode;
		code.setContainerLocation(NULL);
		code.setFilterPredicate(NULL);
		code.setPipeProjection(&mappedProj);

		node.setCode(code);
		node.addInput(*tailNode);

		tailNode = &node;
	}

	plan.linkToAllParentOutput(*tailNode);
	return true;
}

bool SQLScanOps::ScanUnique::detectRowDuplicatable(
		ScanCursorAccessor &accessor, bool &duplicatable) {
	duplicatable = accessor.isRowDuplicatable();
	bool pending = false;
	do {
		if (duplicatable) {
			break;
		}

		assert(accessor.isRowIdFiltering());

		if (accessor.isRangeScanFinished() || accessor.isIndexLost()) {
			break;
		}

		pending = true;
	}
	while (false);
	return !pending;
}


void SQLScanOps::ScanNoIndex::compile(OpContext &cxt) const {
	ScanCursorAccessor &accessor = getScanCursorAccessor(cxt);

	bool indexLost;
	if (!detectIndexLoss(accessor, indexLost)) {
		cxt.setPlanPending();
		return;
	}

	if (indexLost) {
		const OpCode &baseCode = getCode();
		if (accessor.isRowDuplicatable()) {
			ScanUnique::generateUniqueScanPlan(cxt, accessor, baseCode);
			return;
		}
		Scan::generateNoIndexedScanPlan(cxt, baseCode);
	}
	else {
		assert(accessor.isRowIdFiltering());
		Scan::generateEmptyPlan(cxt);
	}
}

bool SQLScanOps::ScanNoIndex::detectIndexLoss(
		ScanCursorAccessor &accessor, bool &indexLost) {
	indexLost = accessor.isIndexLost();
	bool pending = false;
	do {
		if (indexLost) {
			break;
		}

		assert(accessor.isRowIdFiltering());

		if (accessor.isRangeScanFinished()) {
			break;
		}

		pending = true;
	}
	while (false);
	return !pending;
}


void SQLScanOps::ScanContainerFull::execute(OpContext &cxt) const {
	util::AllocUniquePtr<ScanCursor> cursor(getScanCursor(cxt));

	for (;;) {
		const bool done =
				cursor->scanFull(cxt, *getCode().getPipeProjection());

		if (done || cxt.checkSuspended()) {
			break;
		}
	}
}


void SQLScanOps::ScanContainerRange::execute(OpContext &cxt) const {
	util::AllocUniquePtr<ScanCursor> cursor(getScanCursor(cxt));

	for (;;) {
		const bool done = cursor->scanRange(
				cxt, *getCode().getPipeProjection());

		if (done) {
			break;
		}
		else if (cxt.checkSuspended()) {
			return;
		}
	}
	unbindScanCursor(&cxt, *cursor);
}


void SQLScanOps::ScanContainerIndex::execute(OpContext &cxt) const {
	if (!cxt.isAllInputCompleted()) {
		return;
	}

	util::LocalUniquePtr<uint32_t> indexPtr;
	if (getCode().getInputCount() > 1) {
		const uint32_t index = 1;
		indexPtr = UTIL_MAKE_LOCAL_UNIQUE(indexPtr, uint32_t, index);
	}
	const IndexScanInfo info(
			getCode().getIndexConditionList(), indexPtr.get());

	util::AllocUniquePtr<ScanCursor> cursor(getScanCursor(cxt));
	for (;;) {
		const bool done = cursor->scanIndex(cxt, info);

		if (done) {
			break;
		}
		else if (cxt.checkSuspended()) {
			return;
		}
	}
	unbindScanCursor(&cxt, *cursor);
}


void SQLScanOps::ScanContainerMeta::compile(OpContext &cxt) const {
	cxt.setInputSourceType(0, SQLExprs::ExprCode::INPUT_READER_MULTI);
	Operator::compile(cxt);
}

void SQLScanOps::ScanContainerMeta::execute(OpContext &cxt) const {
	util::AllocUniquePtr<ScanCursor> cursor(getScanCursor(cxt));

	for (;;) {
		const uint32_t outIndex = cursor->getOutputIndex();
		const bool done = cursor->scanMeta(
				cxt, *getCode().getMiddleProjection(),
				OpCodeBuilder::findFilterPredicate(
						*getCode().getPipeProjection()));

		TupleListReader &outReader = cxt.getLocalReader(outIndex);
		*cxt.getExprContext().getActiveReaderRef() = &outReader;
		for (; outReader.exists(); outReader.next()) {
			getCode().getPipeProjection()->project(cxt);
		}

		if (done || cxt.checkSuspendedAlways()) {
			break;
		}
	}
}


void SQLScanOps::ScanContainerVisited::execute(OpContext &cxt) const {
	util::AllocUniquePtr<ScanCursor> cursor(getScanCursor(cxt));

	for (;;) {
		const bool done = cursor->scanVisited(
				cxt, *getCode().getPipeProjection());

		if (done) {
			break;
		}
		else if (cxt.checkSuspended()) {
			return;
		}
	}
}


void SQLScanOps::Select::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SELECT_PIPE);

	if (getCode().getInputCount() > 0) {
		node.addInput(plan.getParentInput(0));
	}

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	Projection *distinctProj;
	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, true, true, &distinctProj);
	code.setPipeProjection(projections.first);
	code.setFinishProjection(projections.second);

	node.setCode(code);

	OpCodeBuilder::setUpOutputNodes(
			plan, NULL, node, distinctProj, getCode().getAggregationPhase());
}


void SQLScanOps::SelectPipe::execute(OpContext &cxt) const {
	TupleListReader *reader = NULL;
	if (getCode().getInputCount() > 0) {
		reader = &cxt.getReader(0);
	}

	for (;;) {
		if (cxt.checkSuspended()) {
			break;
		}

		if (reader != NULL && !reader->exists()) {
			break;
		}

		getCode().getPipeProjection()->project(cxt);

		if (reader == NULL) {
			break;
		}
		reader->next();
	}
}


void SQLScanOps::Limit::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SELECT);

	node.addInput(plan.getParentInput(0));
	plan.linkToAllParentOutput(node);

	node.setCode(getCode());
}


SQLScanOps::OutputProjection::OutputProjection(
		ProjectionFactoryContext &cxt, const ProjectionCode &code) :
		writer_(SQLOpUtils::ExpressionListWriter::Source(cxt, code)) {
}

void SQLScanOps::OutputProjection::initializeProjectionAt(
		OpContext &cxt) const {
	static_cast<void>(cxt);
	writer_.applyProjection(*this);
}

void SQLScanOps::OutputProjection::updateProjectionContextAt(
		OpContext &cxt) const {
	static_cast<void>(cxt);
	writer_.applyContext(cxt);
}

void SQLScanOps::OutputProjection::project(OpContext &cxt) const {
	static_cast<void>(cxt);
	writer_.write();
	assert(!writer_.isDigestColumnAssigned());
}

void SQLScanOps::OutputProjection::projectBy(
		OpContext &cxt, const ReadableTuple &tuple) const {
	static_cast<void>(cxt);
	writer_.applyTuple(tuple);
	writer_.write();
}

void SQLScanOps::OutputProjection::projectBy(
		OpContext &cxt, const DigestTupleListReader &reader) const {
	static_cast<void>(cxt);
	writer_.writeBy(reader);
}

void SQLScanOps::OutputProjection::projectBy(
		OpContext &cxt, const SummaryTuple &tuple) const {
	static_cast<void>(cxt);
	writer_.writeBy(tuple);
}

void SQLScanOps::OutputProjection::projectBy(
		OpContext &cxt, TupleListReader &reader, size_t index) const {
	cxt.getExprContext().setActiveInput(static_cast<uint32_t>(index));
	cxt.getExprContext().setReader(0, &reader, NULL);

	writer_.applyTuple(reader.get());
	writer_.write();
}


void SQLScanOps::AggrPipeProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	for (Iterator it(*this); it.exists(); it.next()) {
		it.get().eval(cxt.getExprContext());
	}
}


void SQLScanOps::AggrOutputProjection::project(OpContext &cxt) const {
	chainAt(0).project(cxt);
	cxt.getExprContext().initializeAggregationValues();
}


void SQLScanOps::MultiOutputProjection::project(OpContext &cxt) const {
	assert(getChainCount() == 2);
	chainAt(0).project(cxt);
	chainAt(1).project(cxt);
}


void SQLScanOps::FilterProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	const TupleValue &value = child().eval(cxt.getExprContext());

	if (SQLValues::ValueUtils::isNull(value) ||
			!SQLValues::ValueUtils::toBool(value)) {
		return;
	}

	chainAt(0).project(cxt);
}


void SQLScanOps::LimitProjection::initializeProjectionAt(OpContext &cxt) const {
	const std::pair<int64_t, int64_t> &limits = getLimits(*this);

	util::StackAllocator &alloc = cxt.getAllocator();
	cxt.getResource(0, SQLOpTypes::PROJ_LIMIT) =
			ALLOC_UNIQUE(alloc, LimitContext, limits.first, limits.second);
}

void SQLScanOps::LimitProjection::updateProjectionContextAt(
		OpContext &cxt) const {
	LimitContext &limitCxt = cxt.getResource(
			0, SQLOpTypes::PROJ_LIMIT).resolveAs<LimitContext>();

	const int64_t count = limitCxt.update();
	if (count > 0) {
		cxt.addOutputTupleCount(0, count);
	}

	if (!limitCxt.isAcceptable()) {
		cxt.setCompleted();
	}
}

void SQLScanOps::LimitProjection::project(OpContext &cxt) const {
	LimitContext &limitCxt = cxt.getResource(
			0, SQLOpTypes::PROJ_LIMIT).resolveAs<LimitContext>();

	const LimitContext::Acceptance acceptance = limitCxt.accept();
	if (acceptance != LimitContext::ACCEPTABLE_CURRENT_AND_AFTER) {
		if (acceptance == LimitContext::ACCEPTABLE_CURRENT_ONLY) {
			cxt.setNextCountForSuspend(0);
		}
		else {
			if (acceptance == LimitContext::ACCEPTABLE_NONE) {
				cxt.setCompleted();
			}
			return;
		}
	}

	chainAt(0).project(cxt);
}

std::pair<int64_t, int64_t> SQLScanOps::LimitProjection::getLimits(
		const SQLOps::Projection &proj) {
	Iterator it(proj);

	const int64_t limit = it.get().getCode().getValue().get<int64_t>();
	it.next();
	const int64_t offset = it.get().getCode().getValue().get<int64_t>();

	return std::make_pair(limit, offset);
}


SQLScanOps::LimitContext::LimitContext(int64_t limit, int64_t offset) :
		restLimit_(limit),
		restOffset_(offset),
		prevRest_(restLimit_) {
}

bool SQLScanOps::LimitContext::isAcceptable() const {
	return (restLimit_ != 0);
}

SQLScanOps::LimitContext::Acceptance SQLScanOps::LimitContext::accept() {
	if (restLimit_ <= 0) {
		if (restLimit_ < 0) {
			return ACCEPTABLE_CURRENT_AND_AFTER;
		}
		return ACCEPTABLE_NONE;
	}

	if (restOffset_ > 0) {
		--restOffset_;
		return ACCEPTABLE_AFTER_ONLY;
	}

	if (--restLimit_ <= 0) {
		return ACCEPTABLE_CURRENT_ONLY;
	}
	return ACCEPTABLE_CURRENT_AND_AFTER;
}

int64_t SQLScanOps::LimitContext::update() {
	const int64_t count = (prevRest_ - restLimit_);
	assert(count >= 0);

	prevRest_ = restLimit_;
	return count;
}


void SQLScanOps::SubLimitProjection::initializeProjectionAt(
		OpContext &cxt) const {
	const std::pair<int64_t, int64_t> &limits = LimitProjection::getLimits(*this);

	util::StackAllocator &alloc = cxt.getAllocator();
	SubLimitContext::TupleEq pred(alloc, SQLValues::TupleComparator(
			*getProjectionCode().getKeyList(), &cxt.getVarContext(),
			false, false));
	cxt.getResource(0, SQLOpTypes::PROJ_SUB_LIMIT) = ALLOC_UNIQUE(
			alloc, SubLimitContext, alloc, limits.first, limits.second, pred);
}

void SQLScanOps::SubLimitProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SubLimitContext &limitCxt = cxt.getResource(
			0, SQLOpTypes::PROJ_SUB_LIMIT).resolveAs<SubLimitContext>();

	ReadableTuple curTuple = cxt.getExprContext().getReadableTuple(0);
	SQLValues::ArrayTuple &key = limitCxt.getKey();
	if (key.isEmpty() || !limitCxt.getPredicate()(curTuple, key)) {
		const SQLValues::CompColumnList *keyColumnList =
				getProjectionCode().getKeyList();
		assert(keyColumnList != NULL);

		key.assign(cxt.getValueContext(), curTuple, *keyColumnList);
		limitCxt.clear();
	}

	if (!limitCxt.accept()) {
		return;
	}

	chainAt(0).project(cxt);
}


SQLScanOps::SubLimitContext::SubLimitContext(
		util::StackAllocator &alloc, int64_t limit, int64_t offset,
		const TupleEq &pred) :
		initial_(limit, offset),
		cur_(initial_),
		key_(alloc),
		pred_(pred) {
}

bool SQLScanOps::SubLimitContext::accept() {
	const LimitContext::Acceptance acceptance = cur_.accept();
	return (acceptance == LimitContext::ACCEPTABLE_CURRENT_AND_AFTER ||
			acceptance == LimitContext::ACCEPTABLE_CURRENT_ONLY);
}

void SQLScanOps::SubLimitContext::clear() {
	cur_ = initial_;
}

SQLValues::ArrayTuple& SQLScanOps::SubLimitContext::getKey() {
	return key_;
}

const SQLScanOps::SubLimitContext::TupleEq&
SQLScanOps::SubLimitContext::getPredicate() const {
	return pred_;
}
