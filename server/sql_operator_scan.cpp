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
#include "sql_operator_utils.h"
#include "sql_utils_container.h"


const SQLOps::OpProjectionRegistrar
SQLScanOps::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLScanOps::Registrar::operator()() const {
	add<SQLOpTypes::OP_SCAN, Scan>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_FULL, ScanContainerFull>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_INDEX, ScanContainerIndex>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_UPDATES, ScanContainerUpdates>();
	add<SQLOpTypes::OP_SCAN_CONTAINER_META, ScanContainerMeta>();
	add<SQLOpTypes::OP_SELECT, Select>();
	add<SQLOpTypes::OP_SELECT_PIPE, SelectPipe>();
	add<SQLOpTypes::OP_LIMIT, Limit>();

	addProjectionCustom<SQLOpTypes::PROJ_OUTPUT, OutputProjection>();
	addProjection<SQLOpTypes::PROJ_AGGR_PIPE, AggrPipeProjection>();
	addProjection<SQLOpTypes::PROJ_AGGR_OUTPUT, AggrOutputProjection>();
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
				cxt.getAllocator(), getCode().getContainerLocation(), NULL);

		util::StdAllocator<void, void> cursorAlloc = cxt.getAllocator(); 
		cxt.getInputResource(inIndex) =
				SQLContainerUtils::ScanCursor::create(cursorAlloc, source);
	}

	ScanCursor &cursor = cxt.getInputResource(inIndex).resolveAs<ScanCursor>();

	if (!forCompiling) {
		SQLOps::ColumnTypeList typeList(cxt.getAllocator());
		OpCodeBuilder::resolveColumnTypeList(
				*getCode().getMiddleProjection(), typeList);

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
				inIndex, cxt.getExprContext().getReader(inIndex), columnList);

		SQLOps::OpConfig config;
		cxt.getExtContext().getConfig(config);
		const SQLOps::OpConfig::Value count =
				config.get(SQLOpTypes::CONF_INTERRUPTION_SCAN_COUNT);
		if (count >= 0) {
			cxt.setNextCountForSuspend(static_cast<uint64_t>(count));
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


	const SQLOps::ContainerLocation &location =
			getCode().getContainerLocation();
	const bool forMeta = (location.type_ == SQLType::TABLE_META);

	bool indexPlanned = !plan.isEmpty();
	SQLExprs::IndexSelector *selector = NULL;
	do {
		const SQLExprs::Expression *expr = getCode().getFilterPredicate();
		if (forMeta || expr == NULL || !location.indexActivated_) {
			break;
		}

		ScanCursor &cursor = getScanCursor(cxt, true);
		if (indexPlanned) {
			if (cursor.isIndexLost()) {
				unbindScanCursor(NULL, cursor);
				indexPlanned = false;
			}
			break;
		}

		util::StackAllocator &alloc = cxt.getAllocator();
		SQLOps::ColumnTypeList typeList(alloc);

		selector = ALLOC_NEW(alloc) SQLExprs::IndexSelector(
				alloc, alloc, SQLType::EXPR_COLUMN,
				getInputInfo(cxt, typeList));
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

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	rewriter.activateColumnMapping(factoryCxt);

	if (!forMeta) {
		rewriter.clearColumnUsage();

		const bool withId = true;
		builder.addColumnUsage(getCode(), withId);

		if (indexPlanned || selector != NULL) {
			rewriter.setIdOfInput(0, true);
		}
	}

	Projection *midInProj = &builder.createProjectionByUsage(false);

	rewriter.setIdProjected(true);
	rewriter.setInputProjected(true);

	if (selector != NULL) {
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
		Projection *midRefProj = &builder.createProjectionByUsage(false);

		OpNode &topNode = plan.createNode(SQLOpTypes::OP_UNION);
		{
			OpCode code;
			code.setUnionType(SQLType::UNION_DISTINCT);
			code.setKeyColumnList(&keyColumnList);
			code.setPipeProjection(midUnionProj);
			topNode.setCode(code);
		}

		OpNode *followingNode = NULL;
		for (SQLExprs::IndexConditionList::const_iterator it = condList.begin();
				it != condList.end();) {
			const size_t count =
					SQLExprs::IndexSelector::nextCompositeConditionDistance(
							it, condList.end());
			if (it->andOrdinal_ == 0 && count < it->andCount_) {
				OpNode &node = plan.createNode(SQLOpTypes::OP_UNION);
				OpCode code;
				code.setUnionType(SQLType::UNION_INTERSECT);
				code.setKeyColumnList(&keyColumnList);
				code.setPipeProjection(midUnionProj);
				node.setCode(code);

				topNode.addInput(node);
				followingNode = &node;
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
				code.setMiddleProjection(midInProj);
				code.setPipeProjection(midRefProj);
				node.setCode(code);

				node.addInput(plan.getParentInput(0));

				assert(followingNode != NULL);
				followingNode->addInput(node);
			}
			it += count;
		}

		{
			OpNode &node =
					plan.createNode(SQLOpTypes::OP_SCAN_CONTAINER_UPDATES);
			OpCode code;
			code.setMiddleProjection(midInProj);
			code.setPipeProjection(midRefProj);
			node.setCode(code);

			node.addInput(plan.getParentInput(0));
			topNode.addInput(node);
		}

		cxt.setPlanPending();
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

	{
		OpCode baseCode = getCode();
		OpCode code;

		if (opType != SQLOpTypes::OP_SELECT) {
			code.setContainerLocation(&location);
			code.setMiddleProjection(midInProj);
		}

		std::pair<Projection*, Projection*> projections =
				builder.arrangeProjections(baseCode, true, true);
		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);

		OpNode &node = plan.createNode(opType);
		node.setCode(code);
		node.addInput(*inNode);

		plan.getParentOutput(0).addInput(node);
	}
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
		const uint32_t outIndex = cursor.getOutputIndex();
		const bool done =
				cursor.scanFull(cxt, *getCode().getMiddleProjection());

		TupleListReader &outReader = cxt.getLocalReader(outIndex);
		for (; outReader.exists(); outReader.next()) {
			getCode().getPipeProjection()->projectBy(cxt, outReader.get());
		}

		if (done || cxt.checkSuspendedAlways()) {
			break;
		}
	}
}


void SQLScanOps::ScanContainerIndex::execute(OpContext &cxt) const {
	ScanCursor &cursor = getScanCursor(cxt);

	TupleListReader *inReader = NULL;
	if (getCode().getInputCount() > 1) {
		inReader = &cxt.getReader(0);
	}

	SQLExprs::IndexConditionList condList(cxt.getAllocator());
	condList.assign(
			getCode().getIndexConditionList().begin(),
			getCode().getIndexConditionList().end());

	for (;;) {
		if (inReader != NULL && !inReader->exists()) {
			break;
		}


		for (;;) {
			const uint32_t outIndex = cursor.getOutputIndex();
			const bool done = cursor.scanIndex(
					cxt, *getCode().getMiddleProjection(), condList);

			TupleListReader &outReader = cxt.getLocalReader(outIndex);
			for (; outReader.exists(); outReader.next()) {
				getCode().getPipeProjection()->projectBy(cxt, outReader.get());
			}

			if (done) {
				break;
			}
			else if (cxt.checkSuspendedAlways()) {
				return;
			}
		}

		if (inReader == NULL) {
			break;
		}
		inReader->next();
	}
	unbindScanCursor(&cxt, cursor);
}


void SQLScanOps::ScanContainerUpdates::execute(OpContext &cxt) const {
	ScanCursor &cursor = getScanCursor(cxt);

	for (;;) {
		const uint32_t outIndex = cursor.getOutputIndex();
		const bool done = cursor.scanUpdates(
				cxt, *getCode().getMiddleProjection());

		TupleListReader &outReader = cxt.getLocalReader(outIndex);
		for (; outReader.exists(); outReader.next()) {
			getCode().getPipeProjection()->projectBy(cxt, outReader.get());
		}

		if (done) {
			break;
		}
		else if (cxt.checkSuspendedAlways()) {
			return;
		}
	}
	unbindScanCursor(&cxt, cursor);
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
		for (; outReader.exists(); outReader.next()) {
			getCode().getPipeProjection()->projectBy(cxt, outReader.get());
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
	plan.getParentOutput(0).addInput(node);

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, true, true);
	code.setPipeProjection(projections.first);
	code.setFinishProjection(projections.second);

	node.setCode(code);
}


void SQLScanOps::SelectPipe::execute(OpContext &cxt) const {
	TupleListReader *reader = NULL;
	if (getCode().getInputCount() > 0) {
		reader = &cxt.getReader(0);
	}

	for (;;) {
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
	plan.getParentOutput(0).addInput(node);

	node.setCode(getCode());
}


SQLScanOps::OutputProjection::OutputProjection(
		ProjectionFactoryContext &cxt, const ProjectionCode &code) :
		writerList_(cxt.getAllocator()),
		columExprOnly_(false) {
	static_cast<void>(code);

	SQLExprs::ExprFactoryContext &exprCxt = cxt.getExprFactoryContext();
	const SQLExprs::Expression *base = exprCxt.getBaseExpression();

	if (base == NULL || cxt.getExprFactoryContext().isPlanning()) {
		return;
	}

	util::StackAllocator &alloc = cxt.getAllocator();

	SQLOps::ColumnTypeList typeList(alloc);
	util::Vector<const SQLExprs::ExprCode*> codeList(alloc);

	for (SQLExprs::Expression::Iterator it(*base); it.exists(); it.next()) {
		const SQLExprs::ExprCode &subCode = it.get().getCode();
		typeList.push_back(subCode.getColumnType());

		if (SQLValues::TypeUtils::isNull(subCode.getColumnType())) {
			return;
		}

		if (subCode.getType() == SQLType::EXPR_COLUMN &&
				subCode.getInput() == 0 &&
				(subCode.getAttributes() &
						SQLExprs::ExprCode::ATTR_COLUMN_UNIFIED) == 0) {
			codeList.push_back(&subCode);
		}
		else {
			codeList.push_back(NULL);
		}
	}

	columExprOnly_ = true;

	TupleList::Info info;
	info.columnTypeList_ = &typeList[0];
	info.columnCount_ = typeList.size();

	SQLOps::TupleColumnList destColumnList(alloc);
	destColumnList.resize(info.columnCount_);
	info.getColumns(&destColumnList[0], destColumnList.size());

	for (SQLOps::TupleColumnList::iterator it = destColumnList.begin();
			it != destColumnList.end(); ++it) {
		const SQLExprs::ExprCode *subCode =
				codeList[it - destColumnList.begin()];
		if (subCode == NULL) {
			columExprOnly_ = false;
			writerList_.push_back(NULL);
		}
		else {
			writerList_.push_back(ALLOC_NEW(alloc) SQLValues::ValueWriter(
					exprCxt.getInputColumn(
							subCode->getInput(),
							subCode->getColumnPos()), *it));
		}
	}
}

void SQLScanOps::OutputProjection::project(OpContext &cxt) const {
	if (!columExprOnly_) {
		projectInternal(cxt, NULL);
		return;
	}

	const ReadableTuple &srcTuple = cxt.getExprContext().getReadableTuple(0);
	projectInternal(cxt, &srcTuple);
}

void SQLScanOps::OutputProjection::projectBy(
		OpContext &cxt, const ReadableTuple &tuple) const {
	if (!columExprOnly_) {
		cxt.getExprContext().setReadableTuple(0, tuple);
		projectInternal(cxt, NULL);
		return;
	}

	projectInternal(cxt, &tuple);
}

void SQLScanOps::OutputProjection::projectBy(
		OpContext &cxt, TupleListReader &reader, size_t index) const {
	if (!columExprOnly_) {
		cxt.getExprContext().setActiveInput(static_cast<uint32_t>(index));
		cxt.getExprContext().setReader(0, reader, NULL);
		projectInternal(cxt, NULL);
		return;
	}

	ReadableTuple srcTuple = reader.get();
	projectInternal(cxt, &srcTuple);
}

inline void SQLScanOps::OutputProjection::projectInternal(
		OpContext &cxt, const ReadableTuple *srcTuple) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	const uint32_t index = 0;
	TupleListWriter &writer = cxt.getWriter(index);

	writer.next();
	WritableTuple tuple = writer.get();

	if (!columExprOnly_) {
		uint32_t columnPos = 0;
		for (Iterator it(*this); it.exists(); it.next()) {
			const TupleValue &value = it.get().eval(cxt.getExprContext());
			tuple.set(cxt.getWriterColumn(index, columnPos), value);
			++columnPos;
		}
		return;
	}

	assert(srcTuple != NULL);
	for (util::Vector<SQLValues::ValueWriter*>::const_iterator it =
			writerList_.begin(); it != writerList_.end(); ++it) {
		(**it)(*srcTuple, tuple);
	}
}


SQLValues::ArrayTuple& SQLScanOps::AggrPipeProjection::getTuple(
		OpContext &cxt) {
	return cxt.getResource(
			0, SQLOpTypes::PROJ_AGGR_PIPE).resolveAs<SQLValues::ArrayTuple>();
}

void SQLScanOps::AggrPipeProjection::initializeProjectionAt(
		OpContext &cxt) const {
	util::StackAllocator &alloc = cxt.getAllocator();
	cxt.getResource(0, SQLOpTypes::PROJ_AGGR_PIPE) =
			ALLOC_UNIQUE(alloc, SQLValues::ArrayTuple, alloc);
	clearProjection(cxt);
}

void SQLScanOps::AggrPipeProjection::clearProjection(OpContext &cxt) const {
	const util::Vector<TupleColumnType> *typeList =
			getProjectionCode().getColumnTypeList();
	assert(typeList != NULL);

	SQLValues::VarContext::Scope varScope(cxt.getVarContext());
	getTuple(cxt).assign(cxt.getValueContext(), *typeList);
}

void SQLScanOps::AggrPipeProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	cxt.getExprContext().setAggregationTuple(getTuple(cxt));

	for (Iterator it(*this); it.exists(); it.next()) {
		it.get().eval(cxt.getExprContext());
	}
}


void SQLScanOps::AggrOutputProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	cxt.getExprContext().setAggregationTuple(
			AggrPipeProjection::getTuple(cxt));

	chainAt(0).project(cxt);
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

void SQLScanOps::LimitProjection::project(OpContext &cxt) const {
	LimitContext &limitCxt = cxt.getResource(
			0, SQLOpTypes::PROJ_LIMIT).resolveAs<LimitContext>();

	bool limited;
	if (!limitCxt.accept(limited)) {
		if (limited) {
			cxt.setCompleted();
		}
		return;
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
		restOffset_(offset) {
}

bool SQLScanOps::LimitContext::accept(bool &limited) {
	limited = false;

	if (restLimit_ <= 0) {
		if (restLimit_ < 0) {
			return true;
		}
		limited = true;
		return false;
	}

	if (restOffset_ > 0) {
		--restOffset_;
		return false;
	}

	--restLimit_;
	return true;
}


void SQLScanOps::SubLimitProjection::initializeProjectionAt(
		OpContext &cxt) const {
	const std::pair<int64_t, int64_t> &limits = LimitProjection::getLimits(*this);

	util::StackAllocator &alloc = cxt.getAllocator();
	cxt.getResource(0, SQLOpTypes::PROJ_SUB_LIMIT) = ALLOC_UNIQUE(
			alloc, SubLimitContext, alloc, limits.first, limits.second);
}

void SQLScanOps::SubLimitProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SubLimitContext &limitCxt = cxt.getResource(
			0, SQLOpTypes::PROJ_SUB_LIMIT).resolveAs<SubLimitContext>();

	const SQLValues::CompColumnList *keyColumnList =
			getProjectionCode().getKeyList();
	assert(keyColumnList != NULL);

	ReadableTuple curTuple = cxt.getExprContext().getReadableTuple(0);
	SQLValues::ArrayTuple &key = limitCxt.getKey();
	if (key.isEmpty() || !SQLValues::TupleEq(*keyColumnList)(curTuple, key)) {
		key.assign(cxt.getValueContext(), curTuple, *keyColumnList);
		limitCxt.clear();
	}

	if (!limitCxt.accept()) {
		return;
	}

	chainAt(0).project(cxt);
}


SQLScanOps::SubLimitContext::SubLimitContext(
		util::StackAllocator &alloc, int64_t limit, int64_t offset) :
		initial_(limit, offset),
		cur_(initial_),
		key_(alloc) {
}

bool SQLScanOps::SubLimitContext::accept() {
	bool limited;
	return cur_.accept(limited);
}

void SQLScanOps::SubLimitContext::clear() {
	cur_ = initial_;
}

SQLValues::ArrayTuple& SQLScanOps::SubLimitContext::getKey() {
	return key_;
}
