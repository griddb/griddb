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
	add<SQLOpTypes::OP_SCAN_CONTAINER_FULL, ScanContainerFull>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_RANGE, ScanContainerRange>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_INDEX, ScanContainerIndex>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_META, ScanContainerMeta>();
	add<SQLOpTypes::OP_SELECT, Select>();
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


SQLScanOps::ScanCursor&
SQLScanOps::BaseScanContainerOperator::getScanCursor(
		OpContext &cxt, bool forCompiling) const {
	const uint32_t inIndex = 0;

	if (cxt.getInputResource(inIndex).isEmpty()) {
		ScanCursor::Source source(
				cxt.getAllocator(), cxt.getLatchHolder(),
				getCode().getContainerLocation(), NULL, cxt.getSource());

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

		util::StdAllocator<void, void> cursorAlloc = cxt.getValueContext().getVarAllocator();
		cxt.getInputResource(inIndex) =
				SQLContainerUtils::ScanCursor::create(cursorAlloc, source);
	}

	ScanCursor &cursor = cxt.getInputResource(inIndex).resolveAs<ScanCursor>();

	if (!forCompiling) {
		if (getCode().getMiddleProjection() != NULL) {
			SQLOps::ColumnTypeList typeList(cxt.getAllocator());
			OpCodeBuilder::resolveColumnTypeList(
					*getCode().getMiddleProjection(), typeList, NULL);

			if (cursor.getOutputIndex() == std::numeric_limits<uint32_t>::max()) {
				const uint32_t outIndex = cxt.createLocal(&typeList);
				cursor.setOutputIndex(outIndex);
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

	return cxt.getInputResource(inIndex).resolveAs<ScanCursor>();
}

void SQLScanOps::BaseScanContainerOperator::unbindScanCursor(
		OpContext *cxt, SQLScanOps::ScanCursor &cursor) {
	if (cxt == NULL || cxt->isAllInputCompleted()) {
		cursor.setOutputIndex(std::numeric_limits<uint32_t>::max());
	}
}

void SQLScanOps::Scan::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	if (tryCompileIndexJoin(cxt, plan)) {
		return;
	}
	else if (tryCompileEmpty(cxt, plan)) {
		return;
	}


	util::StackAllocator &alloc = cxt.getAllocator();

	const SQLOps::ContainerLocation &location =
			getCode().getContainerLocation();
	const bool forMeta = (location.type_ == SQLType::TABLE_META);

	bool indexPlanned = !plan.isEmpty();
	SQLExprs::IndexSelector *selector = NULL;
	do {
		const SQLExprs::Expression *expr = getCode().getJoinPredicate();
		if (expr == NULL) {
			expr = getCode().getFilterPredicate();
		}
		if (forMeta || expr == NULL || !location.indexActivated_) {
			break;
		}

		ScanCursor &cursor = getScanCursor(cxt, true);
		if (indexPlanned || getCode().getLimit() == 0 || cursor.isIndexLost()) {
			if (cursor.isIndexLost()) {
				unbindScanCursor(NULL, cursor);
				indexPlanned = false;
			}
			break;
		}

		ColumnTypeList inputTypeList(alloc);
		TupleList::Info inputInfo = getInputInfo(cxt, inputTypeList);

		selector = ALLOC_NEW(alloc) SQLExprs::IndexSelector(
				alloc, alloc, SQLType::EXPR_COLUMN, inputInfo);
		if (location.multiIndexActivated_) {
			selector->setMultiAndConditionEnabled(true);
		}

		cursor.getIndexSpec(cxt, *selector);
		selector->select(*expr);
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

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	OpCode baseCode = getCode();
	Projection *distinctProj = NULL;
	std::pair<Projection*, Projection*> projections;
	if (indexPlanned && selector == NULL) {
		builder.assignInputTypesByProjection(
				OpCodeBuilder::resolveOutputProjection(baseCode, NULL), NULL);
		projections.first = &builder.createIdenticalProjection(false, 0);
		baseCode = OpCode();
	}
	else if (!forMeta) {
		projections = builder.arrangeProjections(
				baseCode, true, true, &distinctProj);
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
			builder.addColumnUsage(getCode(), withId);
		}
	}

	Projection *midInProj = &builder.createProjectionByUsage(false);

	rewriter.setIdProjected(true);
	rewriter.setInputProjected(true);

	if (selector != NULL) {
		const bool parentPending = (projections.second != NULL);

		const SQLExprs::IndexConditionList &condList =
				selector->getConditionList();

		SQLValues::CompColumnList &keyColumnList =
				builder.createCompColumnList();
		{
			SQLValues::CompColumn keyColumn;
			keyColumn.setColumnPos(rewriter.getMappedIdColumn(0), true);
			keyColumnList.push_back(keyColumn);
		}

		Projection *midUnionProj = &builder.createProjectionByUsage(true);

		OpNode &rangeNode = OpCodeBuilder::setUpOutputNodesDetail(
				plan, NULL, parentPending, NULL,
				SQLOpTypes::OP_SCAN_CONTAINER_RANGE,
				distinctProj, baseCode.getAggregationPhase(), NULL);
		{
			OpNode &node = rangeNode;
			OpCode code;
			code.setPipeProjection(projections.first);
			code.setFinishProjection(projections.second);
			node.setCode(code);

			node.addInput(plan.getParentInput(0));
		}

		OpNode *orNode = NULL;
		if (SQLExprs::IndexSelector::getOrConditionCount(
				condList.begin(), condList.end()) > 1) {
			OpNode &node = plan.createNode(SQLOpTypes::OP_UNION_DISTINCT_MERGE);
			OpCode code;
			code.setKeyColumnList(&keyColumnList);
			code.setPipeProjection(midUnionProj);
			node.setCode(code);

			orNode = &node;
			rangeNode.addInput(*orNode);
		}

		OpNode &topNode = (orNode == NULL ? rangeNode : *orNode);

		bool andNodeFound = false;
		OpNode *followingNode = NULL;
		for (SQLExprs::IndexConditionList::const_iterator it = condList.begin();
				it != condList.end();) {
			const size_t count =
					SQLExprs::IndexSelector::nextCompositeConditionDistance(
							it, condList.end());
			if (it->andOrdinal_ == 0 && count < it->andCount_) {
				OpNode &node =
						plan.createNode(SQLOpTypes::OP_UNION_INTERSECT_MERGE);
				OpCode code;
				code.setKeyColumnList(&keyColumnList);
				code.setPipeProjection(midUnionProj);
				node.setCode(code);

				topNode.addInput(node);
				followingNode = &node;
				andNodeFound = true;
			}
			else if (it->andOrdinal_ == 0) {
				followingNode = &topNode;
			}

			{
				OpNode &node = plan.createNode(
						SQLOpTypes::OP_SCAN_CONTAINER_INDEX);

				SQLExprs::IndexConditionList &condList =
						builder.createIndexConditionList();
				condList.assign(it, it + count);

				OpCode code;
				code.setIndexConditionList(&condList);
				code.setPipeProjection(midInProj);
				node.setCode(code);

				node.addInput(plan.getParentInput(0));
				if (baseCode.getInputCount() > 1) {
					node.addInput(plan.getParentInput(1));
				}

				assert(followingNode != NULL);
				followingNode->addInput(node);
			}
			it += count;
		}

		typedef SQLContainerUtils::ScanMergeMode MergeMode;
		const MergeMode::Type mergeMode =
				(andNodeFound && orNode != NULL ? MergeMode::MODE_ROW_ARRAY :
				!andNodeFound && orNode == NULL ? MergeMode::MODE_MIXED :
				MergeMode::MODE_MIXED_MERGING);

		ScanCursor &cursor = getScanCursor(cxt, true);
		cursor.setMergeMode(mergeMode);
		cursor.setIndexSelection(*selector);

		if (parentPending) {
			cxt.setPlanPending();
		}
		else {
			cursor.setRowIdFiltering();

			OpCode code = getCode();
			code.setJoinPredicate(NULL);

			SQLOps::OpConfig &config = builder.createConfig();
			const SQLOps::OpConfig *srcConfig = getCode().getConfig();
			if (srcConfig != NULL) {
				config = *srcConfig;
			}
			if (code.getLimit() >= 0) {
				config.set(SQLOpTypes::CONF_LIMIT_INHERITANCE, 1);
			}
			code.setConfig(&config);

			OpNode &node = plan.createNode(SQLOpTypes::OP_SCAN);
			node.setCode(code);

			node.addInput(plan.getParentInput(0));
			plan.linkToAllParentOutput(node);
		}
		return;
	}

	const OpNode *inNode;
	SQLOpTypes::Type opType;
	if (forMeta) {
		inNode = &plan.getParentInput(0);
		opType = SQLOpTypes::OP_SCAN_CONTAINER_META;
	}
	else if (!indexPlanned) {
		inNode = &plan.getParentInput(0);
		opType = SQLOpTypes::OP_SCAN_CONTAINER_FULL;
	}
	else {
		inNode = plan.findNextNode(0);
		assert(inNode != NULL);

		opType = SQLOpTypes::OP_SELECT;
	}

	if (projections.first == NULL) {
		projections = builder.arrangeProjections(
				baseCode, true, true, &distinctProj);
	}

	{
		OpCode code = baseCode;

		if (opType != SQLOpTypes::OP_SELECT) {
			code.setContainerLocation(&location);
		}

		if (forMeta) {
			code.setMiddleProjection(midInProj);
		}

		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);

		OpNode &node = plan.createNode(opType);
		node.setCode(code);
		node.addInput(*inNode);

		OpCodeBuilder::setUpOutputNodes(
				plan, NULL, node, distinctProj,
				baseCode.getAggregationPhase());
	}
}

bool SQLScanOps::Scan::tryCompileIndexJoin(
		OpContext &cxt, OpPlan &plan) const {
	if (!plan.isEmpty()) {
		return false;
	}

	const OpCode &baseCode = getCode();
	if (baseCode.getInputCount() <= 1) {
		return false;
	}

	{
		const SQLOps::OpConfig *config = baseCode.getConfig();
		const int64_t semi = SQLOps::OpConfig::resolve(
				SQLOpTypes::CONF_JOIN_SEMI, config);
		if (semi > 0) {
			return false;
		}
	}

	const uint32_t scanIn = 0;
	const uint32_t drivingIn = 1;

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	SQLOps::OpConfig &scanConfig = builder.createConfig();
	{
		SQLOps::OpConfig &config = scanConfig;
		const SQLOps::OpConfig *srcConfig = baseCode.getConfig();
		if (srcConfig != NULL) {
			config = *srcConfig;
		}
		config.set(SQLOpTypes::CONF_JOIN_SEMI, 1);
	}

	const SQLExprs::Expression *semiPred = baseCode.getFilterPredicate();

	SQLValues::CompColumnList *keyColumnList =
			&builder.createCompColumnList();
	{
		SQLOpUtils::CompPosSet posSet(cxt.getAllocator());
		builder.extractJoinKeyColumns(
				semiPred, NULL, *keyColumnList, posSet, NULL);
		if (keyColumnList->empty()) {
			keyColumnList = NULL;
		}
	}

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	const SQLExprs::Expression *scanFilterPred = NULL;
	if (semiPred != NULL) {
		SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);
		factoryCxt.setAggregationPhase(true, baseCode.getAggregationPhase());

		SQLExprs::Expression &srcExpr =
				rewriter.rewrite(factoryCxt, *semiPred, NULL);
		scanFilterPred = &SQLExprs::ExprRewriter::retainSingleInputPredicate(
				factoryCxt, srcExpr, scanIn, false);
	}

	rewriter.activateColumnMapping(factoryCxt);
	{
		rewriter.clearColumnUsage();

		const bool withId = true;
		builder.addColumnUsage(baseCode, withId);
		rewriter.addInputColumnUsage(drivingIn);

		rewriter.setIdOfInput(drivingIn, false);
	}

	OpNode &scanNode = plan.createNode(SQLOpTypes::OP_SCAN);
	{
		OpCode code;
		code.setConfig(&scanConfig);

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
		node.addInput(plan.getParentInput(drivingIn));
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
	return true;
}

bool SQLScanOps::Scan::tryCompileEmpty(OpContext &cxt, OpPlan &plan) const {
	ScanCursor &cursor = getScanCursor(cxt, true);
	if (!cursor.isRowIdFiltering()) {
		return false;
	}

	if (cursor.isIndexLost()) {
		return false;
	}

	ColumnTypeList outTypeList(cxt.getAllocator());
	OpCodeBuilder::getColumnTypeListByColumns(
			cxt.getOutputColumnList(0), outTypeList);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	builder.setUpNoopNodes(plan, outTypeList);

	return true;
}

TupleList::Info SQLScanOps::Scan::getInputInfo(
		OpContext &cxt, SQLOps::ColumnTypeList &typeList) const {
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


void SQLScanOps::ScanContainerFull::execute(OpContext &cxt) const {
	ScanCursor &cursor = getScanCursor(cxt);

	for (;;) {
		const bool done =
				cursor.scanFull(cxt, *getCode().getPipeProjection());

		if (done || cxt.checkSuspended()) {
			break;
		}
	}
}


void SQLScanOps::ScanContainerRange::execute(OpContext &cxt) const {
	ScanCursor &cursor = getScanCursor(cxt);

	for (;;) {
		const bool done = cursor.scanRange(
				cxt, *getCode().getPipeProjection());

		if (done) {
			break;
		}
		else if (cxt.checkSuspended()) {
			return;
		}
	}
	unbindScanCursor(&cxt, cursor);
}


void SQLScanOps::ScanContainerIndex::execute(OpContext &cxt) const {
	ScanCursor &cursor = getScanCursor(cxt);

	TupleListReader *inReader = NULL;
	const TupleColumnList *inColumnList = NULL;

	if (getCode().getInputCount() > 1) {
		const uint32_t index = 1;
		inReader = &cxt.getReader(index);
		inColumnList = &cxt.getInputColumnList(index);
	}

	SQLExprs::IndexConditionList condList(cxt.getAllocator());

	for (;;) {
		condList.assign(
				getCode().getIndexConditionList().begin(),
				getCode().getIndexConditionList().end());
		if (inReader != NULL) {
			if (!inReader->exists()) {
				break;
			}
			bindIndexCondition(
					*inReader, *inColumnList, cursor.getIndexSelection(),
					condList);
		}

		for (;;) {
			const bool done = cursor.scanIndex(
					cxt, *getCode().getPipeProjection(), condList);

			if (done) {
				break;
			}
			else if (cxt.checkSuspended()) {
				return;
			}
		}

		if (inReader == NULL) {
			break;
		}
		inReader->next();
	}
	if (cxt.isAllInputCompleted()) {
		cursor.finishIndexScan(cxt);
	}
	unbindScanCursor(&cxt, cursor);
}

void SQLScanOps::ScanContainerIndex::bindIndexCondition(
		TupleListReader &reader, const TupleColumnList &columnList,
		const SQLExprs::IndexSelector &selector,
		SQLExprs::IndexConditionList &condList) {
	const uint32_t emptyColumn = SQLExprs::IndexCondition::EMPTY_IN_COLUMN;

	for (SQLExprs::IndexConditionList::iterator it = condList.begin();
			it != condList.end(); ++it) {
		const std::pair<uint32_t, uint32_t> &pair = it->inColumnPair_;

		TupleValue value1;
		if (pair.first != emptyColumn) {
			value1 = SQLValues::ValueUtils::readCurrentAny(
					reader, columnList[pair.first]);
		}

		TupleValue value2;
		if (pair.second != emptyColumn) {
			value2 = SQLValues::ValueUtils::readCurrentAny(
					reader, columnList[pair.second]);
		}

		selector.bindCondition(*it, value1, value2);
	}
}


void SQLScanOps::ScanContainerMeta::compile(OpContext &cxt) const {
	cxt.setInputSourceType(0, SQLExprs::ExprCode::INPUT_READER_MULTI);
	Operator::compile(cxt);
}

void SQLScanOps::ScanContainerMeta::execute(OpContext &cxt) const {
	ScanCursor &cursor = getScanCursor(cxt);

	for (;;) {
		const uint32_t outIndex = cursor.getOutputIndex();
		const bool done = cursor.scanMeta(
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
