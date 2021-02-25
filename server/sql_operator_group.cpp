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

	add<SQLOpTypes::OP_UNION, Union>();
	add<SQLOpTypes::OP_UNION_ALL, UnionAll>();

	add<SQLOpTypes::OP_UNION_DISTINCT_MERGE, UnionDistinct>();
	add<SQLOpTypes::OP_UNION_INTERSECT_MERGE, UnionIntersect>();
	add<SQLOpTypes::OP_UNION_EXCEPT_MERGE, UnionExcept>();
	add<SQLOpTypes::OP_UNION_COMPENSATE_MERGE, UnionCompensate>();
}


void SQLGroupOps::Group::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SORT);

	node.addInput(plan.getParentInput(0));

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	const Projection *srcProj = code.getPipeProjection();
	const bool distinct = builder.findDistinctAggregation(*srcProj);

	{
		const SQLValues::CompColumnList &srcKeyList =
				rewriter.rewriteCompColumnList(
						factoryCxt, code.getKeyColumnList(), false);
		setUpGroupKeys(builder, code, srcKeyList, distinct);
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

	OpCodeBuilder::setUpOutputNodes(
			plan, NULL, node, distinctProj, getCode().getAggregationPhase());
}

void SQLGroupOps::Group::setUpGroupKeys(
		OpCodeBuilder &builder, OpCode &code,
		const SQLValues::CompColumnList &keyList, bool distinct) {

	const bool withDigest =
			!SQLValues::TupleDigester::isOrderingAvailable(keyList, false);

	code.setMiddleKeyColumnList(&keyList);

	SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();

	SQLExprs::ExprRewriter::Scope scope(rewriter);
	rewriter.activateColumnMapping(factoryCxt);

	const uint32_t input = 0;

	if (!distinct) {
		rewriter.clearColumnUsage();
		rewriter.addKeyColumnUsage(input, keyList, true);
	}

	if (withDigest) {
		rewriter.setIdOfInput(input, true);
		rewriter.setIdProjected(true);
	}

	rewriter.setInputProjected(true);

	code.setKeyColumnList(
			&rewriter.remapCompColumnList(factoryCxt, keyList, input, true));
}

void SQLGroupOps::Group::setUpGroupProjections(
		OpCodeBuilder &builder, OpCode &code, const Projection &src,
		const SQLValues::CompColumnList &keyList,
		const SQLValues::CompColumnList &midKeyList) {
	SQLExprs::ExprFactoryContext &factoryCxt = builder.getExprFactoryContext();
	SQLExprs::ExprFactoryContext::Scope scope(factoryCxt);

	const SQLType::AggregationPhase srcPhase = code.getAggregationPhase();
	factoryCxt.setAggregationPhase(true, srcPhase);

	factoryCxt.setArrangedKeyList(&midKeyList);

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

	const OpNode *baseNode = &plan.getParentInput(0);
	for (SQLOpUtils::ExprRefList::const_iterator it = distinctList.begin();
			it != distinctList.end(); ++it) {
		const size_t index = it - distinctList.begin();

		OpNode &uniqNode = plan.createNode(SQLOpTypes::OP_GROUP);
		{
			OpCode code = getCode();
			code.setKeyColumnList(&builder.createDistinctGroupKeyList(
					distinctList, index, false));
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
						distinctList, index, true));
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
					distinctList, index, *proj, aggrPhase));
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
			alloc, BucketContext, alloc, getCode().getKeyColumnList());

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
		util::StackAllocator &alloc, const SQLValues::CompColumnList &keyList) :
		writerList_(alloc),
		digester_(alloc, SQLValues::TupleDigester(keyList)) {
}

inline SQLOpUtils::ExpressionListWriter::ByGeneral
SQLGroupOps::GroupBucketHash::BucketContext::getOutput(uint64_t index) {
	return SQLOpUtils::ExpressionListWriter::ByGeneral(
			*writerList_[static_cast<size_t>(index)]);
}


void SQLGroupOps::Union::compile(OpContext &cxt) const {
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

	if (sorted && code.findKeyColumnList() == NULL) {
		SQLValues::CompColumnList &list = builder.createCompColumnList();

		const uint32_t columnCount = cxt.getColumnCount(0);
		for (uint32_t i = 0; i < columnCount; i++) {
			SQLValues::CompColumn column;
			column.setColumnPos(i, true);
			column.setOrdering(false);
			list.push_back(column);
		}
		code.setKeyColumnList(&list);
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


void SQLGroupOps::UnionAll::compile(OpContext &cxt) const {
	cxt.setAllInputSourceType(SQLExprs::ExprCode::INPUT_ARRAY_TUPLE);

	Operator::compile(cxt);
}

void SQLGroupOps::UnionAll::execute(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SQLValues::ArrayTuple tuple(cxt.getAllocator());
	cxt.getExprContext().setArrayTuple(0, &tuple);

	cxt.setUpExprContext();

	SQLValues::CompColumnList outColumnList(cxt.getAllocator());
	const TupleList::Info &info = cxt.getWriter(0).getTupleList().getInfo();
	for (uint32_t i = 0; i < info.columnCount_; i++) {
		SQLValues::CompColumn column;
		column.setType(info.columnTypeList_[i]);
		outColumnList.push_back(column);
	}

	const uint32_t count = cxt.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		for (TupleListReader &reader = cxt.getReader(i); reader.exists();) {
			ReadableTuple readerTuple = reader.get();
			tuple.assign(
					cxt.getValueContext(), readerTuple, outColumnList,
					cxt.getInputColumnList(i));
			getCode().getPipeProjection()->projectBy(cxt, reader, i);
			reader.next();

			if (cxt.checkSuspended()) {
				return;
			}
		}
	}
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
			SQLValues::TupleComparator(keyColumnList, false, true));
	TupleDigester digester(
			alloc, SQLValues::TupleDigester(keyColumnList, NULL));

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
