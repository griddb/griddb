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

#include "sql_operator_group.h"


const SQLOps::OpProjectionRegistrar
SQLGroupOps::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLGroupOps::Registrar::operator()() const {
	add<SQLOpTypes::OP_GROUP, Group>();
	add<SQLOpTypes::OP_GROUP_DISTINCT, GroupDistinct>();
	add<SQLOpTypes::OP_GROUP_DISTINCT_MERGE, GroupDistinctMerge>();
	add<SQLOpTypes::OP_GROUP_BUCKET_HASH, GroupBucketHash>();

	add<SQLOpTypes::OP_GROUP_RANGE, GroupRange>();
	add<SQLOpTypes::OP_GROUP_RANGE_MERGE, GroupRangeMerge>();

	add<SQLOpTypes::OP_UNION, Union>();
	add<SQLOpTypes::OP_UNION_ALL, UnionAll>();

	add<SQLOpTypes::OP_UNION_DISTINCT_MERGE, UnionDistinct>();
	add<SQLOpTypes::OP_UNION_INTERSECT_MERGE, UnionIntersect>();
	add<SQLOpTypes::OP_UNION_EXCEPT_MERGE, UnionExcept>();
	add<SQLOpTypes::OP_UNION_COMPENSATE_MERGE, UnionCompensate>();

	add<SQLOpTypes::OP_UNION_DISTINCT_HASH, UnionDistinctHash>();
	add<SQLOpTypes::OP_UNION_INTERSECT_HASH, UnionIntersectHash>();
	add<SQLOpTypes::OP_UNION_EXCEPT_HASH, UnionExceptHash>();
}


void SQLGroupOps::Group::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SORT);

	node.addInput(plan.getParentInput(0));

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	const Projection *srcProj = code.getPipeProjection();
	const bool distinct = builder.findDistinctAggregation(*srcProj);

	{
		const SQLValues::CompColumnList &keyList = code.getKeyColumnList();
		setUpGroupKeys(builder, code, keyList, distinct);
	}

	Projection *distinctProj = NULL;
	{
		assert(srcProj != NULL);
		assert(code.getFinishProjection() == NULL);
		if (distinct) {
			setUpDistinctGroupProjections(
					builder, code, code.getMiddleKeyColumnList(),
					&distinctProj);
		}
		else {
			setUpGroupProjections(
					builder, code, *srcProj,
					code.getKeyColumnList(), code.getMiddleKeyColumnList());
		}
	}
	node.setCode(code);

	const SQLType::AggregationPhase aggrPhase =
			getCode().getAggregationPhase();
	SQLValues::CompColumnList *distinctKeyList = createDistinctGroupKeys(
			cxt, distinct, aggrPhase,
			code.getPipeProjection(), code.getKeyColumnList());

	OpCodeBuilder::setUpOutputNodes(
			plan, NULL, node, distinctProj, aggrPhase, distinctKeyList);
}

void SQLGroupOps::Group::setUpGroupKeys(
		OpCodeBuilder &builder, OpCode &code,
		const SQLValues::CompColumnList &keyList, bool distinct) {

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	util::Set<uint32_t> keyPosSet(factoryCxt.getAllocator());

	SQLValues::CompColumnList &midKeyList = builder.createCompColumnList();
	midKeyList = rewriter.rewriteCompColumnList(factoryCxt, keyList, false);
	SQLExprs::ExprRewriter::normalizeCompColumnList(midKeyList, keyPosSet);

	const bool withDigest =
			!SQLValues::TupleDigester::isOrderingAvailable(midKeyList, false);

	code.setMiddleKeyColumnList(&midKeyList);

	SQLExprs::ExprRewriter::Scope scope(rewriter);
	rewriter.activateColumnMapping(factoryCxt);

	const uint32_t input = 0;

	if (!distinct) {
		rewriter.clearColumnUsage();
		rewriter.addKeyColumnUsage(input, midKeyList, true);
	}

	if (withDigest) {
		rewriter.setIdOfInput(input, true);
		rewriter.setIdProjected(true);
	}

	rewriter.setInputProjected(true);

	const bool keyOnly = !distinct;
	code.setKeyColumnList(&rewriter.remapCompColumnList(
			factoryCxt, midKeyList, input, true, keyOnly, &keyPosSet));
}

SQLValues::CompColumnList* SQLGroupOps::Group::createDistinctGroupKeys(
		OpContext &cxt, bool distinct, SQLType::AggregationPhase aggrPhase,
		const Projection *srcProj, const SQLValues::CompColumnList &srcList) {
	if (!distinct || aggrPhase != SQLType::AGG_PHASE_ADVANCE_PIPE) {
		return NULL;
	}

	const bool forPipe = true;
	const Projection *pipeProj = SQLOps::OpCodeBuilder::findAggregationProjection(
			*srcProj, &forPipe);

	assert(srcProj != NULL);
	const uint32_t outIndex = 0;
	const Projection *outProj =
			SQLOps::OpCodeBuilder::findOutputProjection(*srcProj, &outIndex);

	if (pipeProj == NULL || outProj == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	util::StackAllocator &alloc = cxt.getAllocator();

	util::Set<uint32_t> aggrSet(alloc);
	{
		const util::Set<uint32_t> &keySet =
				SQLExprs::ExprRewriter::compColumnListToSet(alloc, srcList);
		for (SQLExprs::Expression::Iterator it(*pipeProj);
				it.exists(); it.next()) {
			const SQLExprs::Expression &expr = it.get();
			if (expr.getCode().getType() != SQLType::AGG_FIRST) {
				continue;
			}

			const SQLExprs::Expression *subExpr = expr.findChild();
			if (subExpr == NULL ||
					subExpr->getCode().getType() != SQLType::EXPR_COLUMN ||
					keySet.find(subExpr->getCode().getColumnPos()) ==
							keySet.end()) {
				continue;
			}

			aggrSet.insert(expr.getCode().getAggregationIndex());
		}
	}

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	SQLValues::CompColumnList &destList = builder.createCompColumnList();

	uint32_t destPos = 0;
	for (SQLExprs::Expression::Iterator it(*outProj); it.exists(); it.next()) {
		const SQLExprs::Expression &expr = it.get();

		if (expr.getCode().getType() == SQLType::AGG_FIRST &&
				aggrSet.find(expr.getCode().getAggregationIndex()) !=
						aggrSet.end()) {
			SQLValues::CompColumn column;
			column.setColumnPos(destPos, true);
			column.setOrdering(false);
			destList.push_back(column);
		}

		destPos++;
	}

	return &destList;
}

void SQLGroupOps::Group::setUpGroupProjections(
		OpCodeBuilder &builder, OpCode &code, const Projection &src,
		const SQLValues::CompColumnList &keyList,
		const SQLValues::CompColumnList &midKeyList) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

	const SQLType::AggregationPhase srcPhase = code.getAggregationPhase();
	factoryCxt.setAggregationPhase(true, srcPhase);

	factoryCxt.setArrangedKeyList(&midKeyList, false);

	code.setMiddleProjection(&builder.createPipeFinishProjection(
			builder.createMultiStageProjection(
					builder.createMultiStageAggregation(src, -1, false, NULL),
					builder.createMultiStageAggregation(src, 0, false, NULL)),
			builder.createMultiStageAggregation(src, 0, true, &midKeyList)));

	Projection &mid = builder.createPipeFinishProjection(
			builder.createMultiStageAggregation(src, 1, false, NULL),
			builder.createMultiStageAggregation(src, 1, true, &keyList));

	Projection &pipe = builder.createPipeFinishProjection(
			builder.createMultiStageAggregation(src, 2, false, NULL),
			builder.createLimitedProjection(
					builder.createMultiStageAggregation(src, 2, true, NULL), code));

	code.setPipeProjection(&builder.createMultiStageProjection(pipe, mid));
}

void SQLGroupOps::Group::setUpDistinctGroupProjections(
		OpCodeBuilder &builder, OpCode &code,
		const SQLValues::CompColumnList &midKeyList,
		Projection **distinctProj) {

	const bool withDigest =
			!SQLValues::TupleDigester::isOrderingAvailable(midKeyList, false);

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	SQLExprs::ExprRewriter::Scope scope(rewriter);
	rewriter.activateColumnMapping(factoryCxt);

	if (withDigest) {
		const uint32_t input = 0;
		rewriter.setIdOfInput(input, true);
	}

	{
		Projection &proj = builder.createProjectionByUsage(false);
		assert(proj.getProjectionCode().getType() == SQLOpTypes::PROJ_OUTPUT);

		proj.getProjectionCode().setKeyList(&midKeyList);
		code.setMiddleProjection(&proj);
	}

	if (withDigest) {
		rewriter.setIdProjected(true);
	}
	rewriter.setInputProjected(true);

	Projection &mid = builder.createProjectionByUsage(false);

	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, false, true, distinctProj);
	assert(projections.second != NULL && *distinctProj != NULL);

	Projection &pipe = builder.createPipeFinishProjection(
			*projections.first, *projections.second);

	code.setPipeProjection(&builder.createMultiStageProjection(pipe, mid));
	code.setFinishProjection(NULL);
}


void SQLGroupOps::GroupDistinct::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	const Projection *proj = getCode().getPipeProjection();

	SQLOpUtils::ExprRefList distinctList = builder.getDistinctExprList(*proj);
	const SQLType::AggregationPhase aggrPhase = getCode().getAggregationPhase();

	const SQLValues::CompColumnList *keyList = getCode().findKeyColumnList();
	const util::Set<uint32_t> *keySet = NULL;
	if (keyList != NULL) {
		keySet = &SQLExprs::ExprRewriter::compColumnListToSet(
			cxt.getAllocator(), *keyList);
	}

	const uint32_t outAttributes = OpCodeBuilder::resolveOutputProjection(
			*proj, NULL).getCode().getAttributes();
	const bool idSingle =
			((outAttributes & SQLExprs::ExprCode::ATTR_GROUPING) == 0);

	const OpNode *baseNode = &plan.getParentInput(0);
	for (SQLOpUtils::ExprRefList::const_iterator it = distinctList.begin();
			it != distinctList.end(); ++it) {
		const size_t index = it - distinctList.begin();

		OpNode &uniqNode = plan.createNode(SQLOpTypes::OP_GROUP);
		{
			OpCode code = getCode();
			code.setKeyColumnList(&builder.createDistinctGroupKeyList(
					distinctList, index, false, idSingle));
			code.setPipeProjection(&builder.createDistinctGroupProjection(
					distinctList, index, false, aggrPhase));
			uniqNode.setCode(code);
		}
		uniqNode.addInput(plan.getParentInput(1));

		OpNode *groupNode;
		if (aggrPhase == SQLType::AGG_PHASE_ADVANCE_PIPE) {
			groupNode = &uniqNode;
		}
		else {
			groupNode = &plan.createNode(SQLOpTypes::OP_GROUP);
			{
				OpCode code = getCode();
				code.setKeyColumnList(&builder.createDistinctGroupKeyList(
						distinctList, index, true, idSingle));
				code.setPipeProjection(&builder.createDistinctGroupProjection(
						distinctList, index, true, aggrPhase));
				code.setAggregationPhase(SQLType::AGG_PHASE_ALL_PIPE);
				groupNode->setCode(code);
			}
			groupNode->addInput(uniqNode);
		}

		OpNode &mergeNode =
				plan.createNode(SQLOpTypes::OP_GROUP_DISTINCT_MERGE);
		{
			OpCode code = getCode();
			code.setKeyColumnList(&builder.createDistinctMergeKeyList());
			code.setPipeProjection(&builder.createDistinctMergeProjection(
					distinctList, index, *proj, aggrPhase, keySet));
			mergeNode.setCode(code);
		}
		mergeNode.addInput(*baseNode);
		mergeNode.addInput(*groupNode);

		baseNode = &mergeNode;
	}
	plan.linkToAllParentOutput(*baseNode);
}


void SQLGroupOps::GroupDistinctMerge::execute(OpContext &cxt) const {
	typedef SQLValues::ValueUtils ValueUtils;
	typedef SQLValues::Types::Long IdType;

	TupleListReader &reader1 = cxt.getReader(0);
	TupleListReader &reader2 = cxt.getReader(1);
	TupleListReader &nextIdReader = cxt.getReader(0, 1);

	const SQLValues::CompColumn &idKeyColumn = getIdKeyColumn();
	const TupleColumn &column1 = idKeyColumn.getTupleColumn1();
	const TupleColumn &column2 = idKeyColumn.getTupleColumn2();

	SQLOpUtils::ProjectionRefPair emptyProjs;
	const SQLOps::Projection &proj = getProjections(emptyProjs);

	if (!reader1.exists()) {
		return;
	}

	MergeContext &mergeCxt = prepareMergeContext(cxt);

	int64_t id1 = ValueUtils::readCurrentValue<IdType>(reader1, column1);

	int64_t id2 = -1;
	if (reader2.exists()) {
		id2 = ValueUtils::readCurrentValue<IdType>(reader2, column2);
	}

	int64_t nextId = -1;
	do {
		if (!nextIdReader.exists()) {
			break;
		}
		else if (!mergeCxt.nextReaderStarted_) {
			nextIdReader.next();
			mergeCxt.nextReaderStarted_ = true;
			if (!nextIdReader.exists()) {
				break;
			}
		}
		nextId = ValueUtils::readCurrentValue<IdType>(nextIdReader, column1);
	}
	while (false);

	for (;;) {
		if (cxt.checkSuspended()) {
			return;
		}

		if (id2 < 0) {
			emptyProjs.first->project(cxt);
		}
		else {
			assert(id1 == id2);
			if (mergeCxt.merged_) {
				emptyProjs.second->project(cxt);
			}
			else {
				proj.project(cxt);
				mergeCxt.merged_ = true;
			}
		}

		bool stepping;
		{
			if (id2 >= 0) {
				reader2.next();
			}

			int64_t id;
			if (reader2.exists()) {
				id = ValueUtils::readCurrentValue<IdType>(reader2, column2);
				assert(id >= 0);
				stepping = (id != id2);
			}
			else {
				id = -1;
				stepping = true;
			}
			id2 = id;
		}

		if (!stepping && id1 != nextId) {
			continue;
		}

		reader1.next();
		if (!reader1.exists()) {
			assert(nextId < 0);
			break;
		}
		mergeCxt.merged_ = false;

		if (nextIdReader.exists()) {
			assert(nextId >= 0);
			nextIdReader.next();
		}
		else {
			assert(false);
		}

		id1 = nextId;
		if (!nextIdReader.exists()) {
			nextId = -1;
			continue;
		}

		nextId = ValueUtils::readCurrentValue<IdType>(nextIdReader, column1);
		assert(nextId >= 0);
	}
}

const SQLValues::CompColumn&
SQLGroupOps::GroupDistinctMerge::getIdKeyColumn() const {
	return getCode().getKeyColumnList().front();
}

const SQLOps::Projection& SQLGroupOps::GroupDistinctMerge::getProjections(
		SQLOpUtils::ProjectionRefPair &emptyProjs) const {
	const Projection *totalProj = getCode().getPipeProjection();
	assert(totalProj->getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);

	const Projection &mainProj = totalProj->chainAt(0);
	const Projection &totalEmptyProj = totalProj->chainAt(1);
	assert(totalEmptyProj.getProjectionCode().getType() ==
			SQLOpTypes::PROJ_MULTI_STAGE);

	emptyProjs.first = &totalEmptyProj.chainAt(0);
	emptyProjs.second = &totalEmptyProj.chainAt(1);

	return mainProj;
}

SQLGroupOps::GroupDistinctMerge::MergeContext&
SQLGroupOps::GroupDistinctMerge::prepareMergeContext(OpContext &cxt) const {
	if (cxt.getResource(0).isEmpty()) {
		cxt.getResource(0) = ALLOC_UNIQUE(cxt.getAllocator(), MergeContext);
	}
	return cxt.getResource(0).resolveAs<MergeContext>();
}


SQLGroupOps::GroupDistinctMerge::MergeContext::MergeContext() :
		merged_(false),
		nextReaderStarted_(false) {
}


void SQLGroupOps::GroupBucketHash::execute(OpContext &cxt) const {
	BucketContext &bucketCxt = prepareContext(cxt);
	cxt.setUpExprContext();

	const uint32_t outCount = cxt.getOutputCount();
	TupleListReader &reader = cxt.getReader(0);
	SQLValues::TupleListReaderSource readerSrc(reader);

	for (; reader.exists(); reader.next()) {
		if (cxt.checkSuspended()) {
			return;
		}

		const int64_t digest = bucketCxt.digester_(readerSrc);
		const uint64_t outIndex = static_cast<uint64_t>(digest) % outCount;

		bucketCxt.getOutput(outIndex).project(cxt);
	}
}

SQLGroupOps::GroupBucketHash::BucketContext&
SQLGroupOps::GroupBucketHash::prepareContext(OpContext &cxt) const {
	util::AllocUniquePtr<void> &resource = cxt.getResource(0);
	if (!resource.isEmpty()) {
		return resource.resolveAs<BucketContext>();
	}

	util::StackAllocator &alloc = cxt.getAllocator();
	resource = ALLOC_UNIQUE(
			alloc, BucketContext,
			alloc, cxt.getVarContext(), getCode().getKeyColumnList());

	BucketContext &bucketCxt = resource.resolveAs<BucketContext>();

	SQLOpUtils::ExpressionListWriter::Source writerSrc(
			cxt, *getCode().getPipeProjection(), false, NULL, false, NULL,
			SQLExprs::ExprCode::INPUT_READER);

	const uint32_t outCount = cxt.getOutputCount();
	for (uint32_t i = 0; i < outCount; i++) {
		writerSrc.setOutput(i);
		bucketCxt.writerList_.push_back(
				ALLOC_NEW(alloc) SQLOpUtils::ExpressionListWriter(writerSrc));
	}

	return bucketCxt;
}


SQLGroupOps::GroupBucketHash::BucketContext::BucketContext(
		util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
		const SQLValues::CompColumnList &keyList) :
		writerList_(alloc),
		digester_(alloc, createBaseDigester(varCxt, keyList)) {
}

inline SQLOpUtils::ExpressionListWriter::ByGeneral
SQLGroupOps::GroupBucketHash::BucketContext::getOutput(uint64_t index) {
	return SQLOpUtils::ExpressionListWriter::ByGeneral(
			*writerList_[static_cast<size_t>(index)]);
}

SQLValues::TupleDigester
SQLGroupOps::GroupBucketHash::BucketContext::createBaseDigester(
		SQLValues::VarContext &varCxt,
		const SQLValues::CompColumnList &keyList) {
	const bool orderingAvailable =
			(keyList.empty() ? false : keyList.front().isOrdering());
	return SQLValues::TupleDigester(
			keyList, &varCxt, &orderingAvailable, false, false, false);
}


void SQLGroupOps::GroupRange::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_GROUP_RANGE_MERGE);

	OpCode srcCode = getCode();
	OpCode code;

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	code.setPipeProjection(&createTotalProjection(builder, srcCode));
	code.setKeyColumnList(&srcCode.getKeyColumnList());
	code.setFilterPredicate(srcCode.getFilterPredicate());

	node.addInput(plan.getParentInput(0));
	node.setCode(code);
	plan.linkToAllParentOutput(node);
}

const SQLExprs::Expression&
SQLGroupOps::GroupRange::resolveRangeOptionPredicate(const OpCode &code) {
	const SQLExprs::Expression *pred = code.getFilterPredicate();
	if (pred == NULL ||
			pred->getCode().getType() != SQLType::EXPR_RANGE_GROUP) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *pred;
}

bool SQLGroupOps::GroupRange::isFillingWithPrevious(SQLExprs::ExprType fillType) {
	return (
			fillType != SQLType::EXPR_RANGE_FILL_NONE &&
			fillType != SQLType::EXPR_RANGE_FILL_NULL);
}

SQLExprs::ExprType SQLGroupOps::GroupRange::getFillType(
		const SQLExprs::Expression &pred) {
	SQLExprs::Expression::Iterator it(pred);
	it.next();
	return it.get().getCode().getType();
}

SQLOps::Projection& SQLGroupOps::GroupRange::createTotalProjection(
		OpCodeBuilder &builder, OpCode &srcCode) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	const SQLExprs::Expression &pred = resolveRangeOptionPredicate(srcCode);
	const SQLExprs::ExprType fillType = getFillType(pred);

	const Projection *baseProj = srcCode.getPipeProjection();
	if (baseProj == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	Projection &proj = builder.rewriteProjection(*baseProj);
	arrangeBaseProjection(factoryCxt, proj, fillType);
	srcCode.setPipeProjection(&proj);

	uint32_t baseAggrColumnCount;
	const SQLOpUtils::ProjectionPair projections =
			createAggregatableProjections(
					builder, srcCode, baseAggrColumnCount, fillType);
	Projection &baseFinishProj =
			createBaseFinishProjection(builder, *projections.second);

	if (fillType == SQLType::EXPR_RANGE_FILL_NONE) {
		Projection &finishSubProj = baseFinishProj;
		Projection &finishTopProj = builder.createMultiStageProjection(
				*projections.second, finishSubProj);
		return builder.createPipeFinishProjection(
				*projections.first, finishTopProj);
	}

	Projection &pipeTopProj =
			createGroupProjection(builder, *projections.first);
	Projection &finishSubProj = builder.createMultiStageProjection(
			createGroupProjection(builder, baseFinishProj),
			createFillProjection(
					builder, baseFinishProj, pred, baseAggrColumnCount));
	Projection &finishTopProj = builder.createMultiStageProjection(
			*projections.second, finishSubProj);
	return builder.createPipeFinishProjection(pipeTopProj, finishTopProj);
}

SQLOpUtils::ProjectionPair
SQLGroupOps::GroupRange::createAggregatableProjections(
		OpCodeBuilder &builder, OpCode &srcCode, uint32_t &baseAggrColumnCount,
		SQLExprs::ExprType fillType) {
	baseAggrColumnCount = 0;
	const SQLOpUtils::ProjectionPair baseProjections =
			builder.arrangeProjections(srcCode, false, true, NULL);
	if (baseProjections.first == NULL || baseProjections.second == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	SQLOpUtils::ProjectionPair dest = baseProjections;

	const SQLValues::ColumnTypeList *baseTypeList = NULL;
	Projection *aggrProjList[2];
	for (size_t i = 0; i < 2; i++) {
		Projection *baseProj = (i == 0 ? dest.first : dest.second);
		Projection *aggrProj =
				SQLOps::OpCodeBuilder::findAggregationModifiableProjection(
						*baseProj);
		if (aggrProj == NULL) {
			aggrProj = SQLOps::OpCodeBuilder::findOutputProjection(
					*baseProj, NULL);
		}
		aggrProjList[i] = aggrProj;

		const SQLValues::ColumnTypeList *typeList = (aggrProj == NULL ?
				NULL : aggrProj->getProjectionCode().getColumnTypeList());
		if (typeList == NULL ||
				(baseTypeList != NULL && *typeList != *baseTypeList)) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		baseTypeList = typeList;
	}

	if (!isFillingWithPrevious(fillType)) {
		return dest;
	}

	baseAggrColumnCount = static_cast<uint32_t>(baseTypeList->size());

	util::StackAllocator &alloc =
			builder.getExprFactoryContext().getAllocator();
	SQLValues::ColumnTypeList *typeList =
			ALLOC_NEW(alloc) SQLValues::ColumnTypeList(alloc);

	typeList->insert(typeList->end(), baseTypeList->begin(), baseTypeList->end());
	typeList->insert(typeList->end(), baseTypeList->begin(), baseTypeList->end());

	for (size_t i = 0; i < 2; i++) {
		aggrProjList[i]->getProjectionCode().setColumnTypeList(typeList);
	}

	return dest;
}

SQLOps::Projection& SQLGroupOps::GroupRange::createBaseFinishProjection(
		OpCodeBuilder &builder, const Projection &srcProj) {
	const SQLOps::ProjectionCode &srcCode = srcProj.getProjectionCode();

	if (srcCode.getType() == SQLOpTypes::PROJ_AGGR_OUTPUT) {
		assert(srcProj.getChainCount() > 0);
		Projection &dest = createBaseFinishProjection(builder, srcProj.chainAt(0));
		dest.getProjectionCode().setColumnTypeList(srcCode.getColumnTypeList());
		return dest;
	}

	SQLOps::Projection &dest = builder.rewriteProjection(srcProj, NULL, true);

	const size_t count = srcProj.getChainCount();
	for (size_t i = 0; i < count; i++) {
		dest.addChain(createBaseFinishProjection(builder, srcProj.chainAt(i)));
	}

	return dest;
}

SQLOps::Projection& SQLGroupOps::GroupRange::createFillProjection(
		OpCodeBuilder &builder, const Projection &srcProj,
		const SQLExprs::Expression &pred, uint32_t baseAggrColumnCount) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	Projection &emptyFillProj = builder.rewriteProjection(srcProj);
	Projection &nextFillProj = builder.rewriteProjection(srcProj);
	Projection &prevFillProj = builder.rewriteProjection(srcProj);
	Projection &bothFillProj = builder.rewriteProjection(srcProj);

	arrangeFillProjection(
			factoryCxt, emptyFillProj, pred, baseAggrColumnCount, true, true);
	arrangeFillProjection(
			factoryCxt, nextFillProj, pred, baseAggrColumnCount, true, false);
	arrangeFillProjection(
			factoryCxt, prevFillProj, pred, baseAggrColumnCount, false, true);
	arrangeFillProjection(
			factoryCxt, bothFillProj, pred, baseAggrColumnCount, false, false);

	return builder.createMultiStageProjection(
			builder.createMultiStageProjection(emptyFillProj, nextFillProj),
			builder.createMultiStageProjection(prevFillProj, bothFillProj));
}

SQLOps::Projection& SQLGroupOps::GroupRange::createGroupProjection(
		OpCodeBuilder &builder, const Projection &srcProj) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	Projection &destProj = builder.rewriteProjection(srcProj);
	arrangeGroupProjection(factoryCxt, destProj);
	return destProj;
}

void SQLGroupOps::GroupRange::arrangeBaseProjection(
		SQLExprs::ExprFactoryContext &cxt, Projection &proj,
		SQLExprs::ExprType fillType) {
	arrangeBaseExpression(cxt, proj, fillType);

	const size_t count = proj.getChainCount();
	for (size_t i = 0; i < count; i++) {
		arrangeBaseProjection(cxt, proj.chainAt(i), fillType);
	}
}

void SQLGroupOps::GroupRange::arrangeFillProjection(
		SQLExprs::ExprFactoryContext &cxt, Projection &proj,
		const SQLExprs::Expression &pred, uint32_t baseAggrColumnCount,
		bool noPrev, bool noNext) {
	arrangeFillExpression(cxt, proj, pred, baseAggrColumnCount, noPrev, noNext);

	const size_t count = proj.getChainCount();
	for (size_t i = 0; i < count; i++) {
		arrangeFillProjection(
				cxt, proj.chainAt(i), pred, baseAggrColumnCount, noPrev, noNext);
	}
}

void SQLGroupOps::GroupRange::arrangeGroupProjection(
		SQLExprs::ExprFactoryContext &cxt, Projection &proj) {
	arrangeGroupExpression(cxt, proj);

	const size_t count = proj.getChainCount();
	for (size_t i = 0; i < count; i++) {
		arrangeGroupProjection(cxt, proj.chainAt(i));
	}
}

void SQLGroupOps::GroupRange::arrangeBaseExpression(
		SQLExprs::ExprFactoryContext &cxt, SQLExprs::Expression &expr,
		SQLExprs::ExprType fillType) {
	SQLExprs::Expression::ModIterator it(expr);
	for (; it.exists(); it.next()) {
		const SQLExprs::ExprType subType = it.get().getCode().getType();

		if (subType == SQLType::EXPR_RANGE_KEY) {
			SQLExprs::Expression &keyExpr =
					createRangeKeyExpression(cxt, it.get(), false, false);

			it.remove();
			it.insert(keyExpr);
		}
		else if ((subType == SQLType::EXPR_RANGE_FILL ||
				subType == SQLType::EXPR_RANGE_AGG) &&
				fillType == SQLType::EXPR_RANGE_FILL_NONE) {
			SQLExprs::Expression::ModIterator subIt(it.get());
			SQLExprs::Expression &fillTarget = subIt.get();

			it.remove();
			it.insert(fillTarget);
		}
		else {
			arrangeBaseExpression(cxt, it.get(), fillType);
		}
	}
}

void SQLGroupOps::GroupRange::arrangeFillExpression(
		SQLExprs::ExprFactoryContext &cxt, SQLExprs::Expression &expr,
		const SQLExprs::Expression &pred, uint32_t baseAggrColumnCount,
		bool noPrev, bool noNext) {
	SQLExprs::Expression::ModIterator it(expr);
	for (; it.exists(); it.next()) {
		const SQLExprs::ExprType subType = it.get().getCode().getType();

		if (subType == SQLType::EXPR_RANGE_FILL) {
			SQLExprs::Expression &fillExpr = createFillExpression(
					cxt, it.get(), pred, baseAggrColumnCount, noPrev, noNext);
			it.remove();
			it.insert(fillExpr);
		}
		else if (subType == SQLType::EXPR_COLUMN) {
			SQLExprs::Expression &nullExpr =
					createFillNullExpression(cxt, it.get());
			it.remove();
			it.insert(nullExpr);
		}
		else if (subType == SQLType::EXPR_RANGE_AGG) {
			SQLExprs::Expression::ModIterator subIt(it.get());
			it.remove();

			assert(subIt.exists());
			subIt.next();

			assert(subIt.exists());
			it.insert(subIt.get());

			arrangeFillExpression(
					cxt, it.get(), pred, baseAggrColumnCount, noPrev, noNext);
		}
		else {
			arrangeFillExpression(
					cxt, it.get(), pred, baseAggrColumnCount, noPrev, noNext);
		}
	}
}

void SQLGroupOps::GroupRange::arrangePrevFillTarget(
		SQLExprs::ExprFactoryContext &cxt, SQLExprs::Expression &expr,
		uint32_t baseAggrColumnCount) {
	SQLExprs::ExprCode &code = expr.getPlanningCode();

	if (SQLExprs::ExprTypeUtils::isAggregation(code.getType())) {
		const uint32_t baseAggrIndex = code.getAggregationIndex();

		if (baseAggrIndex >= baseAggrColumnCount) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		code.setAggregationIndex(baseAggrColumnCount + baseAggrIndex);
	}

	SQLExprs::Expression::ModIterator it(expr);
	for (; it.exists(); it.next()) {
		arrangePrevFillTarget(cxt, it.get(), baseAggrColumnCount);
	}
}

void SQLGroupOps::GroupRange::arrangeGroupExpression(
		SQLExprs::ExprFactoryContext &cxt, SQLExprs::Expression &expr) {
	SQLExprs::Expression::ModIterator it(expr);
	for (; it.exists(); it.next()) {
		const SQLExprs::ExprType subType = it.get().getCode().getType();
		if (subType == SQLType::EXPR_RANGE_AGG ||
				subType == SQLType::EXPR_RANGE_FILL) {
			SQLExprs::Expression::ModIterator subIt(it.get());
			it.remove();

			assert(subIt.exists());
			it.insert(subIt.get());

			arrangeGroupExpression(cxt, it.get());
		}
		else {
			arrangeGroupExpression(cxt, it.get());
		}
	}
}

SQLExprs::Expression& SQLGroupOps::GroupRange::createFillExpression(
		SQLExprs::ExprFactoryContext &cxt,
		const SQLExprs::Expression &srcExpr,
		const SQLExprs::Expression &pred, uint32_t baseAggrColumnCount,
		bool noPrev, bool noNext) {
	const SQLExprs::ExprCode &srcCode = srcExpr.getCode();
	assert(srcCode.getType() == SQLType::EXPR_RANGE_FILL);

	const SQLExprs::Expression &targetExpr = srcExpr.child();
	const SQLExprs::ExprType fillType = getFillType(pred);
	if (fillType == SQLType::EXPR_RANGE_FILL_NULL) {
		return createFillNullExpression(cxt, srcExpr);
	}
	else if (fillType == SQLType::EXPR_RANGE_FILL_PREV) {
		if (noPrev) {
			return createFillNullExpression(cxt, srcExpr);
		}
		return createFillTargetExpression(
				cxt, targetExpr, baseAggrColumnCount, true);
	}
	else if (fillType == SQLType::EXPR_RANGE_FILL_LINEAR) {
		if (noPrev || noNext) {
			return createFillNullExpression(cxt, srcExpr);
		}

		const SQLExprs::Expression &keyExpr = getBaseKeyExpression(pred);

		SQLExprs::ExprFactoryContext::Scope scope(cxt);
		cxt.setPlanning(true);

		SQLExprs::ExprCode code;
		code.setType(SQLType::EXPR_LINEAR);
		code.setColumnType(srcCode.getColumnType());
		code.setAttributes(srcCode.getAttributes());

		SQLExprs::Expression &linearExpr = cxt.getFactory().create(cxt, code);

		SQLExprs::Expression::ModIterator it(linearExpr);
		it.append(createRangeKeyExpression(cxt, keyExpr, true, false));
		it.append(createFillTargetExpression(
				cxt, targetExpr, baseAggrColumnCount, true));
		it.append(createRangeKeyExpression(cxt, keyExpr, false, true));
		it.append(createFillTargetExpression(
				cxt, targetExpr, baseAggrColumnCount, false));
		it.append(createRangeKeyExpression(cxt, keyExpr, false, false));
		return linearExpr;
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

SQLExprs::Expression& SQLGroupOps::GroupRange::createFillTargetExpression(
		SQLExprs::ExprFactoryContext &cxt,
		const SQLExprs::Expression &srcExpr, uint32_t baseAggrColumnCount,
		bool forPrev) {
	SQLExprs::ExprRewriter rewriter(cxt.getAllocator());

	SQLExprs::Expression &expr = rewriter.rewrite(cxt, srcExpr, NULL);
	if (forPrev) {
		arrangePrevFillTarget(cxt, expr, baseAggrColumnCount);
	}

	return expr;
}

SQLExprs::Expression& SQLGroupOps::GroupRange::createFillNullExpression(
		SQLExprs::ExprFactoryContext &cxt,
		const SQLExprs::Expression &srcExpr) {
	const TupleValue nullValue;
	const SQLExprs::ExprCode &srcCode = srcExpr.getCode();

	if (SQLValues::TypeUtils::isNull(
			SQLValues::TypeUtils::findConversionType(
					SQLValues::ValueUtils::toColumnType(nullValue),
					srcCode.getColumnType(), false, true))) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	SQLExprs::Expression &expr = SQLExprs::ExprRewriter::createConstExpr(
			cxt, nullValue, srcCode.getColumnType());

	SQLExprs::ExprCode &code = expr.getPlanningCode();
	code.setAttributes(srcCode.getAttributes());

	return expr;
}

SQLExprs::Expression& SQLGroupOps::GroupRange::createRangeKeyExpression(
		SQLExprs::ExprFactoryContext &cxt,
		const SQLExprs::Expression &srcExpr, bool forPrev, bool forNext) {
	const SQLExprs::ExprCode &srcCode = srcExpr.getCode();
	const uint32_t columnPos = (forPrev ? 1 : (forNext ? 2 : 0));

	SQLExprs::Expression &expr =
			cxt.getFactory().create(cxt, SQLType::EXPR_RANGE_KEY_CURRENT);

	SQLExprs::ExprCode &code = expr.getPlanningCode();
	code.setColumnPos(columnPos);
	code.setColumnType(srcCode.getColumnType());
	code.setAttributes(srcCode.getAttributes());

	return expr;
}

const SQLExprs::Expression& SQLGroupOps::GroupRange::getBaseKeyExpression(
		const SQLExprs::Expression &pred) {
	SQLExprs::Expression::Iterator it(pred);
	return it.get();
}


void SQLGroupOps::GroupRangeMerge::execute(OpContext &cxt) const {
	MergeContext &mergeCxt = prepareMergeContext(cxt);
	cxt.getExprContext().setWindowState(&mergeCxt.windowState_);

	TupleListReader &pipeReader = preparePipeReader(cxt);
	TupleListReader &keyReader = prepareKeyReader(cxt, mergeCxt);
	const Projection &aggrPipeProj = *mergeCxt.aggrPipeProj_;

	for (;;) { 
		Directions directions;
		for (;;) {
			if (cxt.checkSuspended()) { 
				return;
			}

			if (!checkGroupRow(
					cxt, mergeCxt, pipeReader, keyReader, directions)) {
				break;
			}

			if (directions.isOnNormalGroup()) {
				aggrPipeProj.project(cxt);
			}

			if (!directions.isBeforeGroupTailRow()) {
				break;
			}

			assert(keyReader.exists());
			pipeReader.next();
			keyReader.next();
		}

		if (!checkGroup(cxt, mergeCxt, directions)) {
			break;
		}

		if (mergeCxt.isForNormalGroup()) {
			if (mergeCxt.isFilling()) {
				if (!finishPendingGroups(cxt, mergeCxt, directions)) {
					return;
				}
				if (!directions.isOnNormalGroup()) {
					if (!setGroupPending(mergeCxt)) {
						const bool noNext = true;
						mergeCxt.windowState_.rangeNextKey_ =
								mergeCxt.windowState_.rangeKey_;
						mergeCxt.windowState_.rangePrevKey_ =
								mergeCxt.windowState_.rangeKey_;
						getFillFinishProjection(mergeCxt, noNext).project(cxt);
						mergeCxt.windowState_.rangeNextKey_ =
								RangeKey::invalid();
						mergeCxt.windowState_.rangePrevKey_ =
								RangeKey::invalid();
					}
				}
				else {
					getFinishProjection(mergeCxt).project(cxt);
				}
			}
			else {
				getFinishProjection(mergeCxt).project(cxt);
			}
		}

		if (!nextGroup(mergeCxt, directions)) {
			break;
		}
		clearAggregationValues(cxt, mergeCxt, directions);

		if (directions.isNextRowForNextGroup()) {
			assert(pipeReader.exists());
			pipeReader.next();
			if (keyReader.exists()) {
				keyReader.next();
			}
		}
	}
}

void SQLGroupOps::GroupRangeMerge::clearAggregationValues(
		OpContext &cxt, MergeContext &mergeCxt,
		const Directions &directions) {
	if (mergeCxt.fillingWithPrev_) {
		if (directions.isOnNormalGroup()) {
			swapAggregationValues(cxt, mergeCxt, true);
			mergeCxt.prevValuesPreserved_ = true;
		}
		else {
			swapAggregationValues(cxt, mergeCxt, false);
		}
	}

	cxt.getExprContext().initializeAggregationValues();

	if (mergeCxt.fillingWithPrev_) {
		swapAggregationValues(cxt, mergeCxt, false);
	}
}

bool SQLGroupOps::GroupRangeMerge::finishPendingGroups(
		OpContext &cxt, MergeContext &mergeCxt, const Directions &directions) {
	if (!mergeCxt.isFillPending()) {
		return true;
	}

	if (!directions.isOnNormalGroup() &&
			mergeCxt.windowState_.rangeKey_.isLessThan(mergeCxt.partEnd_)) {
		return true;
	}

	const bool noNext = !directions.isOnNormalGroup();
	bool projected = false;
	do {
		if (projected && cxt.checkSuspended()) { 
			return false;
		}

		beginPendingGroup(mergeCxt);
		getFillFinishProjection(mergeCxt, noNext).project(cxt);
		endPendingGroup(mergeCxt);

		projected = true;
	}
	while (mergeCxt.isFillPending());

	mergeCxt.prevValuesPreserved_ = false;
	return true;
}

#include <iostream>
bool SQLGroupOps::GroupRangeMerge::checkGroupRow(
		OpContext &cxt, MergeContext &mergeCxt, TupleListReader &pipeReader,
		TupleListReader &keyReader, Directions &directions) {

	const RangeKey &groupEnd = mergeCxt.getGroupEnd();
	if (!mergeCxt.isFirstRowReady()) {
		const RangeKey &nextRangeKey = groupEnd;
		directions = Directions::of(nextRangeKey, 1, -1, false, false);
		return false;
	}

	int32_t partitionDirection;
	RangeKey nextKey = RangeKey::invalid();
	if (keyReader.exists()) {
		if (mergeCxt.partEq_ == NULL || (*mergeCxt.partEq_)(
				SQLValues::TupleListReaderSource(pipeReader),
				SQLValues::TupleListReaderSource(keyReader))) {
			partitionDirection = -1;
		}
		else {
			partitionDirection = 0;
		}
		nextKey = getRangeKey(mergeCxt, keyReader);
	}
	else {
		partitionDirection = 1;
		nextKey = RangeKey::invalid();
	}

	const RangeKey &curKey = (pipeReader.exists() ?
			getRangeKey(mergeCxt, pipeReader) : groupEnd);
	const bool afterNormal = !mergeCxt.isForNormalGroup();

	int32_t groupDirection;
	bool rowAcceptable;
	bool onNormalGroup;
	if (curKey.isLessThan(groupEnd) || afterNormal) {
		groupDirection = (partitionDirection < 0 &&
				(nextKey.isLessThan(groupEnd) || afterNormal) ? 0 : 1);
		if (!curKey.isLessThan(mergeCxt.getGroupBegin())) {
			rowAcceptable = true;
			onNormalGroup = !afterNormal;
		}
		else {
			rowAcceptable = (groupDirection == 0);
			onNormalGroup = false;
		}
	}
	else {
		groupDirection = -1;
		rowAcceptable = false;
		onNormalGroup = false;
	}

	RangeKey nextRangeKey = RangeKey::invalid();
	bool nextRowForNextGroup = false;
	if (!rowAcceptable || groupDirection > 0) {
		if (mergeCxt.isFilling()) {
			if (partitionDirection < 0) {
				nextRangeKey = groupEnd;
				nextRowForNextGroup = !nextKey.isLessThan(groupEnd);
			}
			else if (mergeCxt.isBeforePartitionTail()) {
				nextRangeKey = groupEnd;
				nextRowForNextGroup = false;
			}
			else {
				nextRangeKey = mergeCxt.partBegin_;
				nextRowForNextGroup = (partitionDirection <= 0);
			}
		}
		else {
			if (partitionDirection < 0) {
				nextRangeKey =
						getNextRangeKeyByValue(mergeCxt, groupEnd, nextKey);
			}
			else {
				nextRangeKey = mergeCxt.partBegin_;
			}
			nextRowForNextGroup = (partitionDirection <= 0);
		}
	}

	if (!onNormalGroup &&
			(partitionDirection > 0 || !curKey.isLessThan(groupEnd))) {
		nextRowForNextGroup = false;
	}

	directions = Directions::of(
			nextRangeKey, partitionDirection, groupDirection, onNormalGroup,
			nextRowForNextGroup);

	if (rowAcceptable && !checkGroup(cxt, mergeCxt, directions)) {
		return false;
	}
	return rowAcceptable;
}

bool SQLGroupOps::GroupRangeMerge::checkGroup(
		OpContext &cxt, MergeContext &mergeCxt,
		const Directions &directions) {
	if (!directions.isBeforeTotalTailRow() &&
			!(cxt.isAllInputCompleted() &&
			(directions.isOnNormalGroup() ||
					mergeCxt.isTotalEmptyPossible() || mergeCxt.isFilling()))) {
		return false;
	}
	return true;
}

bool SQLGroupOps::GroupRangeMerge::nextGroup(
		MergeContext &mergeCxt, const Directions &directions) {
	mergeCxt.groupFoundLast_ = (mergeCxt.isBeforePartitionTail() ?
			(directions.isOnNormalGroup() ? true : mergeCxt.groupFoundLast_) :
			false);

	if (!directions.isBeforeTotalTailRow() &&
			(!mergeCxt.isFilling() || !mergeCxt.isBeforePartitionTail())) {
		return false;
	}

	mergeCxt.windowState_.rangeKey_ = directions.getNextRangeKey();
	return true;
}

bool SQLGroupOps::GroupRangeMerge::setGroupPending(
		MergeContext &mergeCxt) {
	assert(!mergeCxt.fillProjecting_);

	if (!mergeCxt.fillingWithPrev_ || !mergeCxt.isBeforePartitionTail()) {
		return false;
	}

	if (!mergeCxt.fillPending_) {
		mergeCxt.fillPending_ = true;
		mergeCxt.pendingKey_ = mergeCxt.windowState_.rangeKey_;
		mergeCxt.windowState_.rangePrevKey_ =
				mergeCxt.pendingKey_.subtract(mergeCxt.interval_);
	}

	return true;
}

void SQLGroupOps::GroupRangeMerge::beginPendingGroup(
		MergeContext &mergeCxt) {
	assert(mergeCxt.fillingWithPrev_);
	assert(mergeCxt.fillPending_);

	assert(!mergeCxt.fillProjecting_);
	mergeCxt.fillProjecting_ = true;

	mergeCxt.windowState_.rangeNextKey_ = mergeCxt.windowState_.rangeKey_;
	std::swap(mergeCxt.pendingKey_, mergeCxt.windowState_.rangeKey_);
}

void SQLGroupOps::GroupRangeMerge::endPendingGroup(
		MergeContext &mergeCxt) {
	assert(mergeCxt.fillingWithPrev_);
	assert(mergeCxt.fillPending_);

	assert(mergeCxt.fillProjecting_);
	mergeCxt.fillProjecting_ = false;

	std::swap(mergeCxt.pendingKey_, mergeCxt.windowState_.rangeKey_);
	mergeCxt.windowState_.rangeNextKey_ = RangeKey::invalid();

	if (mergeCxt.windowState_.rangeKey_.subtract(
			mergeCxt.interval_).isLessThanEq(mergeCxt.pendingKey_)) {
		mergeCxt.windowState_.rangePrevKey_ = RangeKey::invalid();
		mergeCxt.pendingKey_ = RangeKey::invalid();
		mergeCxt.fillPending_ = false;
	}
	else {
		mergeCxt.pendingKey_ = mergeCxt.pendingKey_.add(mergeCxt.interval_);
	}
}

SQLGroupOps::GroupRangeMerge::MergeContext&
SQLGroupOps::GroupRangeMerge::prepareMergeContext(OpContext &cxt) const {
	if (!cxt.getResource(0).isEmpty()) {
		return cxt.getResource(0).resolveAs<MergeContext>();
	}

	cxt.getResource(0) = ALLOC_UNIQUE(
			cxt.getAllocator(), MergeContext,
			cxt.getAllocator(), cxt.getVarContext(), getCode());
	MergeContext &mergeCxt = cxt.getResource(0).resolveAs<MergeContext>();

	return mergeCxt;
}

SQLOps::TupleListReader& SQLGroupOps::GroupRangeMerge::preparePipeReader(
		OpContext &cxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0);
	reader.exists();
	return reader;
}

SQLOps::TupleListReader& SQLGroupOps::GroupRangeMerge::prepareKeyReader(
		OpContext &cxt, MergeContext &mergeCxt) {
	SQLOps::TupleListReader &reader = cxt.getReader(0, 1);

	if (!mergeCxt.firstRowReady_ && reader.exists()) {
		if (!mergeCxt.isFilling()) {
			const RangeKey &baseKey = getRangeKey(mergeCxt, reader);
			mergeCxt.windowState_.rangeKey_ = getNextRangeKeyByValue(
					mergeCxt, mergeCxt.partBegin_, baseKey);
		}
		mergeCxt.firstRowReady_ = true;
		reader.next();
	}

	return reader;
}

const SQLOps::Projection& SQLGroupOps::GroupRangeMerge::getFillFinishProjection(
		MergeContext &mergeCxt, bool noNext) {
	if (mergeCxt.restGenerationLimit_ <= 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
				"Too many groups filled (limit=" << mergeCxt.limit_ << ")");
	}
	mergeCxt.restGenerationLimit_--;

	const bool noPrev = !mergeCxt.groupFoundLast_;
	if (noPrev) {
		if (noNext) {
			return *mergeCxt.emptyFillProj_;
		}
		else {
			return *mergeCxt.nextFillProj_;
		}
	}
	else {
		if (noNext) {
			return *mergeCxt.prevFillProj_;
		}
		else {
			return *mergeCxt.bothFillProj_;
		}
	}
}

const SQLOps::Projection& SQLGroupOps::GroupRangeMerge::getFinishProjection(
		MergeContext &mergeCxt) {
	if (!mergeCxt.isFirstRowReady()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *mergeCxt.currentFinishProj_;
}

void SQLGroupOps::GroupRangeMerge::swapAggregationValues(
		OpContext &cxt, MergeContext &mergeCxt, bool withCurrent) {
	SQLValues::SummaryTupleSet *tupleSet;
	SummaryTuple *aggr;
	SummaryTuple &local =
			prepareLocalAggragationTuple(cxt, mergeCxt, tupleSet, aggr);

	const AggrColumnList &columnList = tupleSet->getModifiableColumnList();
	const uint32_t size = static_cast<uint32_t>(columnList.size()) / 2;
	if (columnList.empty()) {
		return;
	}

	const uint32_t basePos = columnList.front().getSummaryPosition();
	const uint32_t aggrPos = basePos + (withCurrent ? 0 : size);
	const uint32_t localPos = basePos + size;

	SummaryTuple::swapValue(*tupleSet, *aggr, aggrPos, local, localPos, size);
}

SQLValues::SummaryTuple&
SQLGroupOps::GroupRangeMerge::prepareLocalAggragationTuple(
		OpContext &cxt, MergeContext &mergeCxt,
		SQLValues::SummaryTupleSet *&tupleSet, SummaryTuple *&aggrTuple) {

	if (mergeCxt.aggrTupleSet_ == NULL) {
		tupleSet = cxt.getDefaultAggregationTupleSet();
		mergeCxt.aggrTupleSet_ = tupleSet;

		if (tupleSet == NULL) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		if (tupleSet->getModifiableColumnList().size() % 2 != 0) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		mergeCxt.localAggrTuple_ = SummaryTuple::create(*tupleSet);
		mergeCxt.localAggrTuple_.initializeValues(*tupleSet);

		mergeCxt.aggrTuple_ = cxt.getDefaultAggregationTupleRef();
		assert(mergeCxt.aggrTuple_ != NULL);
	}

	tupleSet = mergeCxt.aggrTupleSet_;
	aggrTuple = mergeCxt.aggrTuple_;
	return mergeCxt.localAggrTuple_;
}

SQLGroupOps::RangeKey
SQLGroupOps::GroupRangeMerge::getNextRangeKeyByValue(
		MergeContext &mergeCxt, const RangeKey &groupEnd,
		const RangeKey &value) {
	assert(groupEnd.isLessThanEq(mergeCxt.partEnd_));
	return mergeCxt.partEnd_.getMin(groupEnd.add(
			(value.getMax(groupEnd).subtract(groupEnd)).truncate(
					mergeCxt.interval_)));
}

SQLGroupOps::RangeKey SQLGroupOps::GroupRangeMerge::getRangeKey(
		MergeContext &mergeCxt, TupleListReader &reader) {
	if (SQLValues::ValueUtils::readCurrentNull(reader, mergeCxt.rangeColumn_)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	ValueReader valuerReader(reader, mergeCxt.rangeColumn_);
	return mergeCxt.valueReaderFunc_(valuerReader);
}

SQLGroupOps::RangeKey SQLGroupOps::GroupRangeMerge::getRangeKey(
		const TupleValue &value) {
	if (SQLValues::ValueUtils::isNull(value)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	return SQLValues::ValueUtils::toLongInt(value);
}


template<typename T>
SQLGroupOps::GroupRangeMerge::ValueReader::RetType
SQLGroupOps::GroupRangeMerge::ValueReader::TypeAt<T>::operator()() const {
	typedef typename SQLValues::Types::template Of<
			T::COLUMN_TYPE>::Type SrcType;
	typedef typename SQLValues::TypeUtils::Traits<
			T::COLUMN_TYPE>::BasicComparableTag BasicComparableTag;
	return base_.read(SrcType(), BasicComparableTag());
}

template<typename T>
SQLGroupOps::GroupRangeMerge::ValueReader::RetType
SQLGroupOps::GroupRangeMerge::ValueReader::read(
		const T&, const SQLValues::Types::Integral&) const {
	typedef SQLValues::Types::Long DestType;
	return RangeKey::ofElements(SQLValues::ValueUtils::getPromoted<DestType, T>(
			SQLValues::ValueUtils::readCurrentValue<T>(reader_, column_)), 0,
			RangeKey::mega());
}

template<typename T>
SQLGroupOps::GroupRangeMerge::ValueReader::RetType
SQLGroupOps::GroupRangeMerge::ValueReader::read(
		const T&, const SQLValues::Types::PreciseTimestamp&) const {
	typedef SQLValues::Types::NanoTimestampTag DestTypeTag;
	typedef DestTypeTag::LocalValueType DestType;

	const DestType &dest = SQLValues::ValueUtils::getPromoted<DestTypeTag, T>(
			SQLValues::ValueUtils::readCurrentValue<T>(reader_, column_));
	return SQLValues::DateTimeElements(dest).toLongInt();
}

template<typename T, typename C>
SQLGroupOps::GroupRangeMerge::ValueReader::RetType
SQLGroupOps::GroupRangeMerge::ValueReader::read(const T&, const C&) const {
	assert(false);
	return RangeKey::invalid();
}


SQLGroupOps::GroupRangeMerge::MergeContext::MergeContext(
		util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
		const OpCode &code) :
		partKeyList_(resolvePartitionKeyList(alloc, code)),
		partEq_(createKeyComparator(alloc, varCxt, partKeyList_, partEqBase_)),
		rangeColumn_(resoveRangeColumn(code)),
		aggrPipeProj_(findProjection(code, false, false, true)),
		currentFinishProj_(findProjection(code, false, false, false)),
		emptyFillProj_(findProjection(code, true, true, true)),
		nextFillProj_(findProjection(code, true, true, false)),
		prevFillProj_(findProjection(code, true, false, true)),
		bothFillProj_(findProjection(code, true, false, false)),
		partBegin_(RangeKey::invalid()),
		partEnd_(RangeKey::invalid()),
		interval_(RangeKey::invalid()),
		windowState_(resolveRangeOptions(
				code, partBegin_, partEnd_, interval_, limit_,
				fillingWithPrev_)),
		pendingKey_(RangeKey::invalid()),
		restGenerationLimit_(limit_),
		firstRowReady_(false),
		groupFoundLast_(false),
		fillPending_(false),
		fillProjecting_(false),
		prevValuesPreserved_(false),
		aggrTupleSet_(NULL),
		aggrTuple_(NULL),
		valueReaderFunc_(getValueReaderFunction(rangeColumn_)) {
}

inline bool
SQLGroupOps::GroupRangeMerge::MergeContext::isTotalEmptyPossible() const {
	return (isFilling() && !isFirstRowReady() && partEq_ != NULL);
}

inline bool SQLGroupOps::GroupRangeMerge::MergeContext::isFilling() const {
	return (emptyFillProj_ != NULL);
}

inline bool SQLGroupOps::GroupRangeMerge::MergeContext::isFillPending() const {
	return fillPending_;
}

inline bool
SQLGroupOps::GroupRangeMerge::MergeContext::isFirstRowReady() const {
	return firstRowReady_;
}

inline bool
SQLGroupOps::GroupRangeMerge::MergeContext::isForNormalGroup() const {
	return windowState_.rangeKey_.isLessThanEq(partEnd_);
}

inline bool
SQLGroupOps::GroupRangeMerge::MergeContext::isBeforePartitionTail() const {
	return windowState_.rangeKey_.isLessThan(getTailGroupKey());
}

inline SQLExprs::RangeKey
SQLGroupOps::GroupRangeMerge::MergeContext::getGroupBegin() const {
	return windowState_.rangeKey_;
}

inline SQLExprs::RangeKey
SQLGroupOps::GroupRangeMerge::MergeContext::getGroupEnd() const {
	const RangeKey &groupBegin = getGroupBegin();
	return groupBegin.add(interval_);
}

inline SQLExprs::RangeKey
SQLGroupOps::GroupRangeMerge::MergeContext::getTailGroupKey() const {
	return partEnd_;
}

SQLValues::CompColumnList
SQLGroupOps::GroupRangeMerge::MergeContext::resolvePartitionKeyList(
		util::StackAllocator &alloc, const OpCode &code) {
	SQLValues::CompColumnList destList(alloc);

	const SQLValues::CompColumnList &srcList = code.getKeyColumnList();
	if (srcList.size() > 1) {
		destList.assign(srcList.begin(), srcList.end() - 1);
	}

	return destList;
}

SQLGroupOps::GroupRangeMerge::TupleEq*
SQLGroupOps::GroupRangeMerge::MergeContext::createKeyComparator(
		util::StackAllocator &alloc, SQLValues::VarContext &varCxt,
		const SQLValues::CompColumnList &keyList,
		util::LocalUniquePtr<TupleEq> &ptr) {
	if (!keyList.empty()) {
		ptr = UTIL_MAKE_LOCAL_UNIQUE(
				ptr, TupleEq, alloc,
				SQLValues::TupleComparator(keyList, &varCxt, false));
	}
	return ptr.get();
}

SQLValues::TupleColumn
SQLGroupOps::GroupRangeMerge::MergeContext::resoveRangeColumn(
		const OpCode &code) {
	const SQLValues::CompColumnList &baseKeyList = code.getKeyColumnList();
	if (baseKeyList.empty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return baseKeyList.back().getTupleColumn1();
}

const SQLOps::Projection*
SQLGroupOps::GroupRangeMerge::MergeContext::findProjection(
		const OpCode &code, bool filling, bool noPrev, bool noNext) {
	const Projection *topProj = code.getPipeProjection();
	if (!filling && noNext) {
		assert(!noPrev);
		const Projection *pipeTopProj = getSubProjection(
				topProj, SQLOpTypes::PROJ_PIPE_FINISH, 0, false, true);
		return pipeTopProj;
	}

	const Projection *finishTopProj = getSubProjection(
			topProj, SQLOpTypes::PROJ_PIPE_FINISH, 1, false, true);
	const Projection *finishSubProj = getSubProjection(
			finishTopProj, SQLOpTypes::PROJ_MULTI_STAGE, 1, false, true);

	if (filling) {
		const Projection *fillTopProj = getSubProjection(
				finishSubProj, SQLOpTypes::PROJ_MULTI_STAGE, 1, true, false);
		if (fillTopProj == finishSubProj) {
			return NULL;
		}

		const Projection *subProj = getSubProjection(
				fillTopProj, SQLOpTypes::PROJ_MULTI_STAGE,
				(noPrev ? 0 : 1), false, true);
		return getSubProjection(
				subProj, SQLOpTypes::PROJ_MULTI_STAGE,
				(noNext ? 0 : 1), false, true);
	}
	else {
		assert(!noPrev);
		return getSubProjection(
				finishSubProj, SQLOpTypes::PROJ_MULTI_STAGE, 0, true, false);
	}
}

SQLExprs::WindowState
SQLGroupOps::GroupRangeMerge::MergeContext::resolveRangeOptions(
		const OpCode &code, RangeKey &partBegin, RangeKey &partEnd,
		RangeKey &interval, int64_t &limit, bool &fillingWithPrev) {
	const SQLExprs::Expression &pred =
			GroupRange::resolveRangeOptionPredicate(code);
	{
		SQLExprs::Expression::Iterator it(pred);
		it.next();

		it.next();
		interval = getRangeKey(it.get().getCode().getValue());

		it.next();
		partBegin = getRangeKey(it.get().getCode().getValue());

		it.next();
		partEnd = getRangeKey(it.get().getCode().getValue());

		it.next();
		limit = getRangeKey(it.get().getCode().getValue()).toLong();
		if (limit < 0) {
			limit = Constants::ROW_GENERATION_LIMIT;
		}
	}
	fillingWithPrev =
			GroupRange::isFillingWithPrevious(GroupRange::getFillType(pred));

	SQLExprs::WindowState windowState;
	windowState.rangeKey_ = partBegin;
	windowState.partitionValueCount_ = 1; 
	return windowState;
}

const SQLOps::Projection*
SQLGroupOps::GroupRangeMerge::MergeContext::getSubProjection(
		const Projection *base, SQLOpTypes::ProjectionType baseTypeFilter,
		size_t index, bool selfOnUnmatch, bool always) {
	do {
		if (base == NULL) {
			break;
		}

		if (base->getProjectionCode().getType() != baseTypeFilter) {
			if (selfOnUnmatch) {
				return base;
			}
			break;
		}

		if (index >= base->getChainCount()) {
			break;
		}

		return &base->chainAt(index);
	}
	while (false);

	if (always) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return NULL;
}

SQLGroupOps::GroupRangeMerge::ValueReaderFunc
SQLGroupOps::GroupRangeMerge::MergeContext::getValueReaderFunction(
		const TupleColumn &rangeColumn) {
	SQLValues::TypeSwitcher typeSwitcher(
			SQLValues::TypeUtils::toNonNullable(rangeColumn.getType()));
	return typeSwitcher.get<const ValueReader>();
}


inline SQLGroupOps::GroupRangeMerge::Directions::Directions() :
		nextRangeKey_(RangeKey::invalid()),
		partition_(0),
		group_(0),
		onNormalGroup_(false),
		nextRowForNextGroup_(false) {
}

inline SQLGroupOps::GroupRangeMerge::Directions
SQLGroupOps::GroupRangeMerge::Directions::of(
		const RangeKey &nextRangeKey, int32_t partition, int32_t group,
		bool onNormalGroup, bool nextRowForNextGroup) {
	Directions directions;
	directions.nextRangeKey_ = nextRangeKey;
	directions.partition_ = partition;
	directions.group_ = group;
	directions.onNormalGroup_ = onNormalGroup;
	directions.nextRowForNextGroup_ = nextRowForNextGroup;
	return directions;
}

inline SQLGroupOps::RangeKey
SQLGroupOps::GroupRangeMerge::Directions::getNextRangeKey() const {
	return nextRangeKey_;
}

inline bool
SQLGroupOps::GroupRangeMerge::Directions::isBeforeTotalTailRow() const {
	return partition_ <= 0;
}

inline bool
SQLGroupOps::GroupRangeMerge::Directions::isBeforePartitionTailRow() const {
	return partition_ < 0;
}

inline bool
SQLGroupOps::GroupRangeMerge::Directions::isBeforeGroupTailRow() const {
	return group_ <= 0;
}

inline bool
SQLGroupOps::GroupRangeMerge::Directions::isGroupPreceding() const {
	return group_ < 0;
}

inline bool SQLGroupOps::GroupRangeMerge::Directions::isOnNormalGroup() const {
	return onNormalGroup_;
}

inline bool
SQLGroupOps::GroupRangeMerge::Directions::isNextRowForNextGroup() const {
	return onNormalGroup_;
}


void SQLGroupOps::Union::compile(OpContext &cxt) const {
	if (tryCompileHashPlan(cxt)) {
		return;
	}
	else if (tryCompileEmptyPlan(cxt)) {
		return;
	}

	OpPlan &plan = cxt.getPlan();
	OpCode code = getCode();

	const uint32_t inCount = cxt.getInputCount();
	const SQLType::UnionType unionType = code.getUnionType();

	const bool sorted = (unionType != SQLType::UNION_ALL);
	const bool unique = (sorted && unionType != SQLType::END_UNION);
	const bool singleDistinct =
			(inCount == 1 && unionType == SQLType::UNION_DISTINCT);
	OpNode &node = (singleDistinct ?
			plan.getParentOutput(0) :
			plan.createNode(toOperatorType(unionType)));

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	if (sorted) {
		code.setKeyColumnList(&resolveKeyColumnList(
				cxt, builder, code.findKeyColumnList(), false));
	}

	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, false, true, NULL);
	if (sorted) {
		OpCodeBuilder::removeUnificationAttributes(*projections.first);
		if (projections.second != NULL) {
			OpCodeBuilder::removeUnificationAttributes(*projections.second);
		}
	}

	for (uint32_t i = 0; i < inCount; i++) {
		if (sorted) {
			OpNode &subNode = plan.createNode(SQLOpTypes::OP_SORT);
			OpCode subCode;

			SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
			SQLExprs::ExprRewriter::Scope scope(rewriter);
			rewriter.activateColumnMapping(builder.getExprFactoryContext());
			rewriter.setMappedInput(i, 0);

			subCode.setKeyColumnList(&code.getKeyColumnList());

			std::pair<Projection*, Projection*> subProjections;
			if (singleDistinct) {
				subProjections = projections;
			}
			else {
				Projection &proj = builder.createIdenticalProjection(true, i);
				OpCodeBuilder::removeUnificationAttributes(proj);
				subProjections.first = &proj;
			}
			subCode.setPipeProjection(subProjections.first);
			subCode.setFinishProjection(subProjections.second);

			if (unique) {
				subCode.setSubLimit(1);
			}

			subNode.setCode(subCode);
			subNode.addInput(plan.getParentInput(i));

			node.addInput(subNode);
		}
		else {
			node.addInput(plan.getParentInput(i));
		}
	}

	if (!singleDistinct) {
		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);

		node.setCode(code);
		plan.linkToAllParentOutput(node);
	}
}

bool SQLGroupOps::Union::tryCompileHashPlan(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	SQLOpTypes::Type hashOpType;
	const SQLValues::CompColumnList *keyList;
	if (!checkHashPlanAcceptable(
			cxt, builder, getCode(), &hashOpType, &keyList)) {
		return false;
	}

	OpNode &hashNode = plan.createNode(hashOpType);
	{
		OpCode code = getCode();
		OpNode &node = hashNode;

		code.setKeyColumnList(keyList);

		const uint32_t inCount = cxt.getInputCount();
		for (uint32_t i = 0; i < inCount; i++) {
			node.addInput(plan.getParentInput(i));
		}
		std::pair<Projection*, Projection*> projections =
				builder.arrangeProjections(code, false, true, NULL);
		assert(projections.second == NULL);

		util::Vector<Projection*> subProjList(cxt.getAllocator());
		for (uint32_t i = 0; i < inCount; i++) {
			Projection &subProj = builder.createIdenticalProjection(false, i);
			subProj.getPlanningCode().setOutput(i + 1);
			subProjList.push_back(&subProj);
		}

		SQLOps::Projection &proj = builder.createMultiOutputProjection(
				*projections.first, *subProjList.front());
		for (uint32_t i = 1; i < inCount; i++) {
			proj.addChain(*subProjList[i]);
		}
		code.setPipeProjection(&proj);

		node.setCode(code);

		plan.linkToAllParentOutput(node);
	}

	OpNode &nonHashNode = plan.createNode(SQLOpTypes::OP_UNION);
	{
		OpCode code = getCode();
		OpNode &node = nonHashNode;

		SQLOps::OpConfig &config = builder.createConfig();
		const SQLOps::OpConfig *srcConfig = getCode().getConfig();
		if (srcConfig != NULL) {
			config = *srcConfig;
		}
		config.set(SQLOpTypes::CONF_UNION_HASH_LIMIT, 1);
		if (code.getLimit() >= 0) {
			config.set(SQLOpTypes::CONF_LIMIT_INHERITANCE, 1);
		}
		code.setConfig(&config);

		const uint32_t inCount = cxt.getInputCount();
		for (uint32_t i = 0; i < inCount; i++) {
			node.addInput(hashNode, i + 1);
		}
		node.setCode(code);

		plan.linkToAllParentOutput(node);
	}

	return true;
}

bool SQLGroupOps::Union::tryCompileEmptyPlan(OpContext &cxt) const {
	const uint32_t inCount = cxt.getInputCount();
	if (inCount > 2 || !cxt.isAllInputCompleted()) {
		return false;
	}

	for (uint32_t i = 0; i < inCount; i++) {
		if (cxt.getInputBlockCount(i) > 0) {
			return false;
		}
	}

	OpPlan &plan = cxt.getPlan();

	ColumnTypeList outTypeList(cxt.getAllocator());
	OpCodeBuilder::getColumnTypeListByColumns(
			cxt.getOutputColumnList(0), outTypeList);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	builder.setUpNoopNodes(plan, outTypeList);

	return true;
}

bool SQLGroupOps::Union::checkHashPlanAcceptable(
		OpContext &cxt, OpCodeBuilder &builder, const OpCode &code,
		SQLOpTypes::Type *hashOpType,
		const SQLValues::CompColumnList **keyList) {
	*hashOpType = SQLOpTypes::END_OP;
	*keyList = NULL;

	if (code.getLimit() < 0) {
		return false;
	}

	if (SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_UNION_HASH_LIMIT, code.getConfig()) > 0) {
		return false;
	}

	const uint32_t inCount = cxt.getInputCount();
	if (inCount > 2) {
		return false;
	}

	const SQLOpTypes::Type foundOpType =
			findHashOperatorType(code.getUnionType(), inCount);
	if (foundOpType == SQLOpTypes::END_OP) {
		return false;
	}

	if (code.getOffset() > 0) {
		return false;
	}

	const SQLValues::CompColumnList &resolvedKeyList = resolveKeyColumnList(
			cxt, builder, code.findKeyColumnList(), true);
	if (!checkKeySimple(resolvedKeyList)) {
		return false;
	}

	for (uint32_t i = 0; i < inCount; i++) {
		if (!checkInputSimple(cxt.getInputColumnList(i), resolvedKeyList)) {
			return false;
		}
	}

	if (!checkOutputSimple(
			code.getPipeProjection(), code.getFinishProjection())) {
		return false;
	}

	const int64_t memLimit = SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, code.getConfig());
	const uint64_t threshold =
			static_cast<uint64_t>(memLimit < 0 ? 32 * 1024 * 1024 : memLimit) / 2;
	if (estimateHashMapSize(cxt, resolvedKeyList, code.getLimit()) > threshold) {
		return false;
	}

	*hashOpType = foundOpType;
	*keyList = &resolvedKeyList;
	return true;
}

SQLOpTypes::Type SQLGroupOps::Union::toOperatorType(
		SQLType::UnionType unionType) {
	switch (unionType) {
	case SQLType::UNION_ALL:
		return SQLOpTypes::OP_UNION_ALL;
	case SQLType::UNION_DISTINCT:
		return SQLOpTypes::OP_UNION_DISTINCT_MERGE;
	case SQLType::UNION_INTERSECT:
		return SQLOpTypes::OP_UNION_INTERSECT_MERGE;
	case SQLType::UNION_EXCEPT:
		return SQLOpTypes::OP_UNION_EXCEPT_MERGE;
	default:
		assert(unionType == SQLType::END_UNION);
		return SQLOpTypes::OP_UNION_COMPENSATE_MERGE;
	}
}

SQLOpTypes::Type SQLGroupOps::Union::findHashOperatorType(
		SQLType::UnionType unionType, uint32_t inCount) {
	switch (unionType) {
	case SQLType::UNION_ALL:
		break;
	case SQLType::UNION_DISTINCT:
		return SQLOpTypes::OP_UNION_DISTINCT_HASH;
	case SQLType::UNION_INTERSECT:
		if (inCount != 2) {
			break;
		}
		return SQLOpTypes::OP_UNION_INTERSECT_HASH;
	case SQLType::UNION_EXCEPT:
		return SQLOpTypes::OP_UNION_EXCEPT_HASH;
	default:
		assert(unionType == SQLType::END_UNION);
		break;
	}
	return SQLOpTypes::END_OP;
}

const SQLValues::CompColumnList& SQLGroupOps::Union::resolveKeyColumnList(
		OpContext &cxt, OpCodeBuilder &builder,
		const SQLValues::CompColumnList *src, bool withAttributes) {
	if (!withAttributes && src != NULL) {
		return *src;
	}

	SQLValues::CompColumnList &dest = builder.createCompColumnList();

	if (src == NULL) {
		const uint32_t input = 0;
		const uint32_t columnCount = cxt.getColumnCount(input);
		for (uint32_t i = 0; i < columnCount; i++) {
			SQLValues::CompColumn column;
			column.setColumnPos(i, true);
			column.setOrdering(false);
			dest.push_back(column);
		}
	}
	else {
		dest = *src;
	}

	if (!withAttributes) {
		return dest;
	}

	return builder.getExprRewriter().rewriteCompColumnList(
			builder.getExprFactoryContext(), dest, true, NULL);
}

bool SQLGroupOps::Union::checkKeySimple(
		const SQLValues::CompColumnList &keyList) {
	for (SQLValues::CompColumnList::const_iterator it = keyList.begin();
			it != keyList.end(); ++it) {
		if (!SQLValues::SummaryTupleSet::isDeepReaderColumnSupported(
				it->getType())) {
			return false;
		}
	}
	return true;
}

bool SQLGroupOps::Union::checkInputSimple(
		const TupleColumnList &columnList,
		const SQLValues::CompColumnList &keyList) {
	for (SQLValues::CompColumnList::const_iterator it = keyList.begin();
			it != keyList.end(); ++it) {
		const TupleColumnType inType =
				columnList[it->getColumnPos(util::TrueType())].getType();
		if (SQLValues::TypeUtils::isAny(inType) &&
				!SQLValues::TypeUtils::isAny(it->getType())) {
			return false;
		}
	}
	return true;
}

bool SQLGroupOps::Union::checkOutputSimple(
		const Projection *pipeProj, const Projection *finishProj) {
	if (finishProj != NULL) {
		return false;
	}

	if (OpCodeBuilder::isAggregationArrangementRequired(
			pipeProj, finishProj)) {
		return false;
	}

	if (pipeProj->getProjectionCode().getType() != SQLOpTypes::PROJ_OUTPUT) {
		return false;
	}

	return true;
}

uint64_t SQLGroupOps::Union::estimateHashMapSize(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		int64_t tupleCount) {
	if (tupleCount <= 0) {
		return 0;
	}

	const uint32_t input = 0;
	const SQLOps::TupleColumnList &columnList = cxt.getInputColumnList(input);

	util::StackAllocator &alloc = cxt.getAllocator();
	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(alloc));

	SQLValues::SummaryTupleSet tupleSet(valueCxt, NULL);
	tupleSet.addReaderColumnList(columnList);
	tupleSet.addKeyList(keyList, (input == 0), false);
	tupleSet.setReaderColumnsDeep(true);
	tupleSet.setHeadNullAccessible(true);
	tupleSet.completeColumns();

	const size_t hashHeadSize = sizeof(uint64_t) * 2;
	const size_t hashValueSize = tupleSet.estimateTupleSize();

	return static_cast<uint64_t>(tupleCount) * (hashHeadSize + hashValueSize);
}


void SQLGroupOps::UnionAll::compile(OpContext &cxt) const {
	const uint32_t count = cxt.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		cxt.setReaderLatchDelayed(i);
	}

	Operator::compile(cxt);
}

void SQLGroupOps::UnionAll::execute(OpContext &cxt) const {
	for (;;) {
		uint32_t index;
		if (!cxt.findRemainingInput(&index)) {
			break;
		}

		TupleListReader &reader = cxt.getReader(index);

		if (reader.exists()) {
			WriterEntry &writer = prepareWriter(cxt, index);
			*cxt.getActiveReaderRef() = &reader;
			if (writer.first != NULL) {
				writer.first->applyContext(cxt);
				do {
					writer.first->write();
					reader.next();

					if (cxt.checkSuspended()) {
						return;
					}
				}
				while (reader.exists());
			}
			else {
				writer.second->updateProjectionContext(cxt);
				do {
					writer.second->project(cxt);
					reader.next();

					if (cxt.checkSuspended()) {
						return;
					}
				}
				while (reader.exists());
			}
		}

		cxt.popRemainingInput();
		cxt.releaseReaderLatch(index);
	}
}

SQLGroupOps::UnionAll::WriterEntry&
SQLGroupOps::UnionAll::prepareWriter(OpContext &cxt, uint32_t index) const {
	util::StackAllocator &alloc = cxt.getAllocator();

	util::AllocUniquePtr<void> &resource = cxt.getResource(0);
	if (resource.isEmpty()) {
		resource = ALLOC_UNIQUE(
				alloc, UnionAllContext, alloc, cxt.getInputCount());
	}
	UnionAllContext &unionCxt = resource.resolveAs<UnionAllContext>();

	WriterEntry *&writer = unionCxt.writerList_[index];
	do {
		if (writer != NULL) {
			break;
		}

		std::pair<ColumnTypeList, WriterEntry*> mapEntry(
				ColumnTypeList(alloc), static_cast<WriterEntry*>(NULL));
		ColumnTypeList &typeList = mapEntry.first;
		getInputColumnTypeList(cxt, index, typeList);

		WriterMap::iterator mapIt = unionCxt.writerMap_.find(typeList);
		if (mapIt != unionCxt.writerMap_.end()) {
			writer = mapIt->second;
			break;
		}

		const SQLExprs::ExprCode::InputSourceType srcType =
				SQLExprs::ExprCode::INPUT_READER_MULTI;
		OpCodeBuilder::Source builderSrc = OpCodeBuilder::ofContext(cxt);
		builderSrc.mappedInput_ = index;

		OpCodeBuilder builder(builderSrc);
		cxt.setUpProjectionFactoryContext(
				builder.getProjectionFactoryContext(), &index);

		SQLExprs::ExprFactoryContext &exprCxt =
				builder.getExprFactoryContext();
		exprCxt.setInputSourceType(0, srcType);
		
		Projection *planningProj;
		{
			SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
			SQLExprs::ExprRewriter::Scope scope(rewriter);
			planningProj = &builder.rewriteProjection(
					*getCode().getPipeProjection(), NULL);
			OpCodeBuilder::removeUnificationAttributes(*planningProj);
		}

		exprCxt.setPlanning(false);
		Projection *proj = &builder.rewriteProjection(*planningProj, NULL);

		SQLOpUtils::ExpressionListWriter::Source writerSrc(
				cxt, *proj, false, NULL, false, NULL, srcType, &index);

		SQLOpUtils::ExpressionListWriter *writerBase =
				ALLOC_NEW(alloc) SQLOpUtils::ExpressionListWriter(writerSrc);
		if (!writerBase->isAvailable()) {
			writerBase = NULL;
			proj = &builder.rewriteProjection(*proj, NULL);
		}
		writer = ALLOC_NEW(alloc) WriterEntry(writerBase, proj);
		mapEntry.second = writer;
		unionCxt.writerMap_.insert(mapEntry);

		cxt.setUpExprContext();
	}
	while (false);

	cxt.getExprContext().setReader(
			0, &cxt.getReader(index), &cxt.getInputColumnList(index));

	return *writer;
}

void SQLGroupOps::UnionAll::getInputColumnTypeList(
		OpContext &cxt, uint32_t index, ColumnTypeList &typeList) {
	const TupleColumnList &columnList = cxt.getInputColumnList(index);
	for (TupleColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		typeList.push_back(it->getType());
	}
}

SQLGroupOps::UnionAll::UnionAllContext::UnionAllContext(
		util::StackAllocator &alloc, uint32_t inCount) :
		writerList_(inCount, NULL, alloc),
		writerMap_(alloc) {
}


SQLGroupOps::UnionMergeContext::UnionMergeContext(int64_t initialState) :
		state_(initialState),
		initialState_(initialState),
		topElemChecked_(false) {
}

SQLGroupOps::TupleHeapQueue
SQLGroupOps::UnionMergeContext::createHeapQueue(
		OpContext &cxt,
		const SQLValues::CompColumnList &keyColumnList,
		util::LocalUniquePtr<TupleHeapQueue::Element> *topElem) {
	util::StackAllocator &alloc = cxt.getAllocator();

	TupleGreater pred(
			alloc,
			SQLValues::TupleComparator(
					keyColumnList, &cxt.getVarContext(), false, true),
			cxt.getValueProfile());
	UniqTupleDigester digester(
			alloc,
			SQLValues::TupleDigester(
					keyColumnList, &cxt.getVarContext(),
					NULL, true, false, false),
			cxt.getValueProfile());

	TupleHeapQueue queue(pred, alloc);
	queue.setPredicateEmpty(pred.getBase().isEmpty(0));

	const uint32_t count = cxt.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		TupleListReader &reader = cxt.getReader(i);
		if (!reader.exists()) {
			continue;
		}

		queue.push(TupleHeapQueue::Element(
				ReadableTupleRef(alloc, reader, digester), i));
	}

	do {
		if (topElem == NULL) {
			break;
		}

		TupleListReader &reader = cxt.getReader(0, 1);
		if (!reader.exists()) {
			break;
		}

		if (!topElemChecked_) {
			reader.next();
			if (!reader.exists()) {
				break;
			}
			topElemChecked_ = true;
		}

		*topElem = UTIL_MAKE_LOCAL_UNIQUE(
				*topElem, TupleHeapQueue::Element,
				ReadableTupleRef(alloc, reader, digester), 0);
	}
	while (false);

	return queue;
}


template<typename Op>
void SQLGroupOps::UnionMergeBase<Op>::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_READER_MULTI);

	Operator::compile(cxt);
}

template<typename Op>
void SQLGroupOps::UnionMergeBase<Op>::execute(OpContext &cxt) const {
	const SQLValues::CompColumnList &keyColumnList =
			getCode().getKeyColumnList();

	util::AllocUniquePtr<void> &resource = cxt.getResource(0);
	if (resource.isEmpty()) {
		resource = ALLOC_UNIQUE(
				cxt.getAllocator(), UnionMergeContext, Op::toInitial(cxt, getCode()));
	}
	UnionMergeContext &unionCxt = resource.resolveAs<UnionMergeContext>();

	util::LocalUniquePtr<TupleHeapQueue::Element> topElem;
	util::LocalUniquePtr<TupleHeapQueue::Element> *topElemRef =
			(Op::InputUnique::VALUE ? NULL : &topElem);
	TupleHeapQueue queue =
			unionCxt.createHeapQueue(cxt, keyColumnList, topElemRef);

	const SQLOps::Projection *projection = getCode().getPipeProjection();
	TupleHeapQueue::Element *topElemPtr = topElem.get();
	MergeAction action(cxt, unionCxt, projection, &topElemPtr);

	queue.mergeUnique(action);
}

template<typename Op>
inline bool SQLGroupOps::UnionMergeBase<Op>::onTuple(
		int64_t &state, size_t ordinal) {
	static_cast<void>(state);
	static_cast<void>(ordinal);
	return false;
}

template<typename Op>
inline bool SQLGroupOps::UnionMergeBase<Op>::onFinish(
		int64_t &state, int64_t initialState) {
	static_cast<void>(state);
	static_cast<void>(initialState);
	return true;
}

template<typename Op>
inline bool SQLGroupOps::UnionMergeBase<Op>::onSingle(
		size_t ordinal, int64_t initialState) {
	static_cast<void>(ordinal);
	static_cast<void>(initialState);
	return true;
}

template<typename Op>
int64_t SQLGroupOps::UnionMergeBase<Op>::toInitial(
		OpContext &cxt, const OpCode &code) {
	static_cast<void>(cxt);
	static_cast<void>(code);
	return 0;
}


template<typename Op>
SQLGroupOps::UnionMergeBase<Op>::MergeAction::MergeAction(
		OpContext &cxt, UnionMergeContext &unionCxt,
		const SQLOps::Projection *projection,
		TupleHeapQueue::Element **topElemPtr) :
		cxt_(cxt),
		unionCxt_(unionCxt),
		projection_(projection),
		topElemPtr_(topElemPtr) {
}

template<typename Op>
inline bool SQLGroupOps::UnionMergeBase<Op>::MergeAction::operator()(
		const TupleHeapQueue::Element &elem, const util::FalseType&) {
	if (Op::onTuple(unionCxt_.state_, elem.getOrdinal())) {
		projection_->projectBy(cxt_, elem.getValue());
	}


	return true;
}

template<typename Op>
inline void SQLGroupOps::UnionMergeBase<Op>::MergeAction::operator()(
		const TupleHeapQueue::Element &elem, const util::TrueType&) {
	if (Op::onFinish(unionCxt_.state_, unionCxt_.initialState_)) {
		projection_->projectBy(cxt_, elem.getValue());
	}
}

template<typename Op>
inline bool SQLGroupOps::UnionMergeBase<Op>::MergeAction::operator()(
		const TupleHeapQueue::Element &elem, const util::TrueType&,
		const util::TrueType&) {
	return Op::onSingle(elem.getOrdinal(), unionCxt_.initialState_);
}

template<typename Op>
template<typename Pred>
inline bool SQLGroupOps::UnionMergeBase<Op>::MergeAction::operator()(
		const TupleHeapQueue::Element &elem, const util::TrueType&,
		const Pred &pred) {
	if (!Op::InputUnique::VALUE) {
		if (elem.getOrdinal() == 0 && *topElemPtr_ != NULL) {
			const bool subFinishable = pred(**topElemPtr_, elem);
			if (!(*topElemPtr_)->next()) {
				(*topElemPtr_) = NULL;
			}
			return subFinishable;
		}
	}

	return true;
}


inline bool SQLGroupOps::UnionIntersect::onTuple(int64_t &state, size_t ordinal) {
	static_cast<void>(ordinal);
	--state;
	return false;
}

inline bool SQLGroupOps::UnionIntersect::onFinish(
		int64_t &state, int64_t initialState) {
	const bool matched = (state == 0);
	state = initialState;
	return matched;
}

inline bool SQLGroupOps::UnionIntersect::onSingle(
		size_t ordinal, int64_t initialState) {
	static_cast<void>(ordinal);
	return (initialState <= 1);
}

inline int64_t SQLGroupOps::UnionIntersect::toInitial(
		OpContext &cxt, const OpCode &code) {
	static_cast<void>(code);
	return static_cast<int64_t>(cxt.getInputCount());
}


inline bool SQLGroupOps::UnionExcept::onTuple(int64_t &state, size_t ordinal) {
	if (ordinal != 0) {
		state = 1;
	}
	return false;
}

inline bool SQLGroupOps::UnionExcept::onFinish(
		int64_t &state, int64_t initialState) {
	static_cast<void>(initialState);

	const bool matched = (state == 0);
	state = 0;
	return matched;
}

inline bool SQLGroupOps::UnionExcept::onSingle(
		size_t ordinal, int64_t initialState) {
	static_cast<void>(initialState);
	return (ordinal == 0);
}


inline bool SQLGroupOps::UnionCompensate::onTuple(
		int64_t &state, size_t ordinal) {
	if (ordinal != 0) {
		return false;
	}
	state = 1;
	return true;
}

inline bool SQLGroupOps::UnionCompensate::onFinish(
		int64_t &state, int64_t initialState) {
	static_cast<void>(initialState);

	const bool matched = (state == 0);
	state = 0;
	return matched;
}


SQLGroupOps::UnionHashContext& SQLGroupOps::UnionHashContext::resolve(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		const Projection &baseProj, const SQLOps::OpConfig *config) {

	util::AllocUniquePtr<void> &resource = cxt.getResource(0);
	if (resource.isEmpty()) {
		resource = ALLOC_UNIQUE(
				cxt.getAllocator(), UnionHashContext,
				cxt, keyList, baseProj, config);
	}

	return resource.resolveAs<UnionHashContext>();
}

SQLGroupOps::UnionHashContext::UnionHashContext(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		const Projection &baseProj, const SQLOps::OpConfig *config) :
		alloc_(cxt.getAllocator()),
		memoryLimit_(resolveMemoryLimit(config)),
		memoryLimitReached_(false),
		topInputPending_(false),
		followingInputCompleted_(false),
		map_(0, MapHasher(), MapPred(), alloc_),
		inputEntryList_(alloc_),
		proj_(NULL),
		projTupleRef_(NULL) {
	const bool digestOrdering = checkDigestOrdering(cxt, keyList);

	initializeTupleSet(cxt, keyList, tupleSet_, digestOrdering);
	setUpInputEntryList(cxt, keyList, baseProj, inputEntryList_, digestOrdering);

	cxt.setUpExprInputContext(0);
	proj_ = &createProjection(cxt, baseProj, keyList, digestOrdering);
	projTupleRef_ = cxt.getExprContext().getSummaryTupleRef(0);
	assert(projTupleRef_ != NULL);
}

template<typename Op>
const SQLOps::Projection* SQLGroupOps::UnionHashContext::accept(
		TupleListReader &reader, const uint32_t index, InputEntry &entry) {
	const int64_t digest = entry.digester_(ReaderSourceType(reader));
	const std::pair<Map::iterator, Map::iterator> &range =
			map_.equal_range(digest);
	for (Map::iterator it = range.first;; ++it) {
		if (it == range.second) {
			if (memoryLimitReached_) {
				return &entry.subProj_;
			}
			MapEntry entry(
					SummaryTuple::create(*tupleSet_, reader, digest, index), 0);
			const bool acceptable = Op::onTuple(entry.second, index);
			*projTupleRef_ = map_.insert(
					range.second, std::make_pair(digest, entry))->second.first;
			if (!acceptable) {
				return NULL;
			}
			break;
		}

		if (entry.tupleEq_(
				ReaderSourceType(reader),
				SummaryTuple::Wrapper(it->second.first))) {
			if (!Op::onTuple(it->second.second, index)) {
				return NULL;
			}
			*projTupleRef_ = it->second.first;
			break;
		}
	}

	return proj_;
}

void SQLGroupOps::UnionHashContext::checkMemoryLimit() {
	if (memoryLimitReached_) {
		return;
	}
	memoryLimitReached_ = (alloc_.getTotalSize() >= memoryLimit_);
}

SQLGroupOps::UnionHashContext::InputEntry&
SQLGroupOps::UnionHashContext::prepareInput(
		OpContext &cxt, const uint32_t index) {
	InputEntry *entry = inputEntryList_[index];

	entry->subProj_.updateProjectionContext(cxt);
	proj_->updateProjectionContext(cxt);

	return *entry;
}

bool SQLGroupOps::UnionHashContext::isTopInputPending() {
	return topInputPending_;
}

void SQLGroupOps::UnionHashContext::setTopInputPending(bool pending) {
	topInputPending_ = pending;
}

bool SQLGroupOps::UnionHashContext::isFollowingInputCompleted(OpContext &cxt) {
	do {
		if (followingInputCompleted_) {
			break;
		}

		const uint32_t inCount = cxt.getInputCount();
		for (uint32_t i = 1; i < inCount; i++) {
			if (!cxt.isInputCompleted(i)) {
				return false;
			}
		}

		followingInputCompleted_ = true;
	}
	while (false);
	return followingInputCompleted_;
}

uint64_t SQLGroupOps::UnionHashContext::resolveMemoryLimit(
		const SQLOps::OpConfig *config) {
	const int64_t base = SQLOps::OpConfig::resolve(
			SQLOpTypes::CONF_WORK_MEMORY_LIMIT, config);
	return static_cast<uint64_t>(base < 0 ? 32 * 1024 * 1024 : base);
}

const SQLOps::Projection& SQLGroupOps::UnionHashContext::createProjection(
		OpContext &cxt, const Projection &baseProj,
		const SQLValues::CompColumnList &keyList, bool digestOrdering) {
	const uint32_t index = 0;

	const SQLExprs::ExprCode::InputSourceType srcType =
			SQLExprs::ExprCode::INPUT_SUMMARY_TUPLE;
	OpCodeBuilder::Source builderSrc = OpCodeBuilder::ofContext(cxt);
	builderSrc.mappedInput_ = index;
	builderSrc.inputUnified_ = true;

	OpCodeBuilder builder(builderSrc);
	cxt.setUpProjectionFactoryContext(
			builder.getProjectionFactoryContext(), &index);

	SQLExprs::ExprFactoryContext &exprCxt =
			builder.getExprFactoryContext();
	exprCxt.setInputSourceType(0, srcType);

	Projection *planningProj;
	{
		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
		SQLExprs::ExprRewriter::Scope scope(rewriter);
		planningProj = &builder.rewriteProjection(baseProj.chainAt(0), NULL);
		OpCodeBuilder::removeUnificationAttributes(*planningProj);
	}

	exprCxt.setPlanning(false);
	exprCxt.setArrangedKeyList(&keyList, !digestOrdering);
	Projection *proj = &builder.rewriteProjection(*planningProj, NULL);
	proj = &builder.rewriteProjection(*proj, NULL);

	cxt.setUpExprContext();
	return *proj;
}

void SQLGroupOps::UnionHashContext::initializeTupleSet(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		util::LocalUniquePtr<SummaryTupleSet> &tupleSet,
		bool digestOrdering) {
	const uint32_t input = 0;

	tupleSet = UTIL_MAKE_LOCAL_UNIQUE(
			tupleSet, SummaryTupleSet, cxt.getValueContext(), NULL);

	{
		ColumnTypeList typeList(cxt.getAllocator());
		{
			const uint32_t count = cxt.getColumnCount(input);
			for (uint32_t i = 0; i < count; i++) {
				typeList.push_back(OpCodeBuilder::resolveUnifiedInputType(cxt, i));
			}
		}
		tupleSet->addReaderColumnList(typeList);
		tupleSet->addKeyList(keyList, (input == 0), false);
	}

	{
		const uint32_t count = cxt.getInputCount();
		for (uint32_t i = 0; i < count; i++) {
			tupleSet->addSubReaderColumnList(i, cxt.getInputColumnList(i));
		}
	}

	tupleSet->setReaderColumnsDeep(true);
	tupleSet->setHeadNullAccessible(true);
	tupleSet->setOrderedDigestRestricted(!digestOrdering);

	tupleSet->completeColumns();

	SummaryTupleSet::applyColumnList(
			tupleSet->getReaderColumnList(),
			*cxt.getSummaryColumnListRef(input));
}

void SQLGroupOps::UnionHashContext::setUpInputEntryList(
		OpContext &cxt, const SQLValues::CompColumnList &keyList,
		const Projection &baseProj, InputEntryList &entryList,
		bool digestOrdering) {
	util::StackAllocator &alloc = cxt.getAllocator();

	SQLExprs::ExprFactoryContext factoryCxt(alloc);
	SQLExprs::ExprRewriter rewriter(alloc);

	const uint32_t digesterInput = 0;

	{
		const uint32_t count = cxt.getColumnCount(0);
		for (uint32_t i = 0; i < count; i++) {
			const TupleColumnType type =
					OpCodeBuilder::resolveUnifiedInputType(cxt, i);
			factoryCxt.setInputType(0, i, type);
			factoryCxt.setInputType(1, i, type);
		}
	}

	const uint32_t inCount = cxt.getInputCount();
	assert(inCount <= 2);
	for (uint32_t i = 0; i < inCount; i++) {
		{
			const uint32_t count = cxt.getColumnCount(0);
			for (uint32_t j = 0; j < count; j++) {
				factoryCxt.setInputType(
						0, j, cxt.getReaderColumn(i, j).getType());

				assert(SQLValues::TypeUtils::findPromotionType(
						factoryCxt.getInputType(0, j),
						factoryCxt.getInputType(1, j), true) ==
								factoryCxt.getInputType(1, j));
				assert(!SQLValues::TypeUtils::isAny(factoryCxt.getInputType(0, j)) ==
					!SQLValues::TypeUtils::isAny(factoryCxt.getInputType(1, j)));
			}
		}

		const SQLValues::CompColumnList &digesterKeyList =
				rewriter.rewriteCompColumnList(
						factoryCxt, keyList, false, &digesterInput, !digestOrdering);
		const SQLValues::CompColumnList &eqKeyList =
				rewriter.rewriteCompColumnList(
						factoryCxt, keyList, false, NULL, !digestOrdering);

		const Projection &subProj = baseProj.chainAt(i + 1);
		entryList.push_back(ALLOC_NEW(alloc) InputEntry(
				cxt, digesterKeyList, digestOrdering, eqKeyList, subProj));
	}
}

bool SQLGroupOps::UnionHashContext::checkDigestOrdering(
		OpContext &cxt, const SQLValues::CompColumnList &keyList) {
	if (!SQLValues::TupleDigester::isOrderingAvailable(keyList, true)) {
		return false;
	}

	const uint32_t inCount = cxt.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		for (SQLValues::CompColumnList::const_iterator it = keyList.begin();
				it != keyList.end(); ++it) {
			const TupleColumnType type = cxt.getReaderColumn(
					i, it->getColumnPos(util::TrueType())).getType();
			if (type != it->getType()) {
				return false;
			}
		}
	}

	return true;
}


SQLGroupOps::UnionHashContext::InputEntry::InputEntry(
		OpContext &cxt, const SQLValues::CompColumnList &digesterKeyList,
		bool digestOrdering, const SQLValues::CompColumnList &eqKeyList,
		const Projection &subProj) :
		digester_(
				cxt.getAllocator(),
				SQLValues::TupleDigester(
						digesterKeyList, &cxt.getVarContext(), &digestOrdering,
						false, false, false),
				cxt.getValueProfile()),
		tupleEq_(
				cxt.getAllocator(),
				SQLValues::TupleComparator(
						eqKeyList, &cxt.getVarContext(),
						false, true, false, !digestOrdering),
				cxt.getValueProfile()),
		subProj_(subProj) {
}


template<typename Op>
void SQLGroupOps::UnionHashBase<Op>::compile(OpContext &cxt) const {
	const uint32_t count = cxt.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		cxt.setReaderLatchDelayed(i);
	}

	Operator::compile(cxt);
}

template<typename Op>
void SQLGroupOps::UnionHashBase<Op>::execute(OpContext &cxt) const {
	UnionHashContext &hashCxt = UnionHashContext::resolve(
			cxt, getCode().getKeyColumnList(), *getCode().getPipeProjection(),
			getCode().getConfig());
	const bool topInputCascading = Op::TopInputCascading::VALUE;
	for (;;) {
		uint32_t index;
		bool onPending = false;
		if (!cxt.findRemainingInput(&index)) {
			if (!topInputCascading || !hashCxt.isTopInputPending()) {
				break;
			}
			index = 0;
			onPending = true;
		}

		bool pendingNext = false;
		do {
			if (topInputCascading && index == 0 && !(onPending &&
					hashCxt.isFollowingInputCompleted(cxt))) {
				pendingNext = true;
				break;
			}

			TupleListReader &reader = cxt.getReader(index);
			if (!reader.exists()) {
				break;
			}

			hashCxt.checkMemoryLimit();
			UnionHashContext::InputEntry &entry =
					hashCxt.prepareInput(cxt, index);
			do {
				const Projection *proj =
						hashCxt.accept<Op>(reader, index, entry);
				if (proj != NULL) {
					proj->project(cxt);
				}
				reader.next();

				if (cxt.checkSuspended()) {
					return;
				}
			}
			while (reader.exists());
		}
		while (false);

		cxt.popRemainingInput();
		cxt.releaseReaderLatch(index);

		if (topInputCascading && index == 0) {
			hashCxt.setTopInputPending(pendingNext);
		}

		if (onPending) {
			break;
		}
	}
}

template<typename Op>
bool SQLGroupOps::UnionHashBase<Op>::onTuple(int64_t &state, size_t ordinal) {
	static_cast<void>(state);
	static_cast<void>(ordinal);
	assert(false);
	return false;
}


inline bool SQLGroupOps::UnionDistinctHash::onTuple(
		int64_t &state, size_t ordinal) {
	static_cast<void>(ordinal);

	if (state != 0) {
		return false;
	}

	state = -1;
	return true;
}


inline bool SQLGroupOps::UnionIntersectHash::onTuple(
		int64_t &state, size_t ordinal) {

	if (state <= 0) {
		if (state == 0) {
			state = static_cast<int64_t>(ordinal) + 1;
		}
		return false;
	}
	else if (state == static_cast<int64_t>(ordinal) + 1) {
		return false;
	}

	state = -1;
	return true;
}


inline bool SQLGroupOps::UnionExceptHash::onTuple(
		int64_t &state, size_t ordinal) {

	if (ordinal > 0) {
		state = -1;
		return false;
	}
	else if (state != 0) {
		return false;
	}

	state = -1;
	return true;
}
