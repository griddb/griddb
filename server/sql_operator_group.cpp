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
#include "sql_operator_utils.h"


const SQLOps::OpProjectionRegistrar
SQLGroupOps::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLGroupOps::Registrar::operator()() const {
	add<SQLOpTypes::OP_GROUP, Group>();
	add<SQLOpTypes::OP_UNION, Union>();
	add<SQLOpTypes::OP_UNION_ALL, UnionAll>();
	add<SQLOpTypes::OP_UNION_SORTED, UnionSorted>();

	addProjection<SQLOpTypes::PROJ_GROUP, GroupProjection>();
	addProjection<SQLOpTypes::PROJ_DISTINCT, UnionDistinctProjection>();
	addProjection<SQLOpTypes::PROJ_INTERSECT, UnionIntersectProjection>();
	addProjection<SQLOpTypes::PROJ_EXCEPT, UnionExceptProjection>();
	addProjection<SQLOpTypes::PROJ_COMPENSATE, UnionCompensateProjection>();
}


void SQLGroupOps::Group::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();

	OpNode &node = plan.createNode(SQLOpTypes::OP_SORT);

	node.addInput(plan.getParentInput(0));
	plan.getParentOutput(0).addInput(node);

	OpCode code = getCode();
	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, false, true);
	setUpGroupProjections(builder, projections);
	code.setPipeProjection(projections.first);
	code.setFinishProjection(projections.second);

	node.setCode(code);
}

void SQLGroupOps::Group::setUpGroupProjections(
		OpCodeBuilder &builder,
		std::pair<Projection*, Projection*> &projections) const {
	assert(projections.second != NULL);

	Projection &finishProj = builder.createProjection(SQLOpTypes::PROJ_GROUP);
	{
		finishProj.addChain(builder.rewriteProjection(*projections.second));
	}

	Projection &pipeProj = builder.createProjection(SQLOpTypes::PROJ_GROUP);
	{
		pipeProj.getProjectionCode().setKeyList(
				&getCode().getKeyColumnList());
		pipeProj.addChain(*projections.second);
		pipeProj.addChain(*projections.first);
	}

	projections.first = &pipeProj;
	projections.second = &finishProj;
}



void SQLGroupOps::Union::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	OpCode code = getCode();

	const SQLType::UnionType unionType = code.getUnionType();
	const bool sorted = (unionType != SQLType::UNION_ALL);
	OpNode &node = plan.createNode(
			sorted ? SQLOpTypes::OP_UNION_SORTED : SQLOpTypes::OP_UNION_ALL);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	if (sorted && code.findKeyColumnList() == NULL) {
		SQLValues::CompColumnList &list = builder.createCompColumnList();

		const uint32_t columnCount = cxt.getColumnCount(0);
		for (uint32_t i = 0; i < columnCount; i++) {
			SQLValues::CompColumn column;
			column.setColumnPos(i, true);
			list.push_back(column);
		}
		code.setKeyColumnList(&list);
	}

	const uint32_t inCount = cxt.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		if (sorted) {
			OpNode &subNode = plan.createNode(SQLOpTypes::OP_SORT);
			OpCode subCode;

			SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
			SQLExprs::ExprRewriter::Scope scope(rewriter);
			rewriter.activateColumnMapping(builder.getExprFactoryContext());
			rewriter.setMappedInput(i, 0);

			subCode.setKeyColumnList(&code.getKeyColumnList());
			subCode.setPipeProjection(
					&builder.createIdenticalProjection(false, i));

			subNode.setCode(subCode);
			subNode.addInput(plan.getParentInput(i));

			node.addInput(subNode);
		}
		else {
			node.addInput(plan.getParentInput(i));
		}
	}

	std::pair<Projection*, Projection*> projections =
			builder.arrangeProjections(code, false, true);

	setUpUnionProjections(
			builder, unionType, code.findKeyColumnList(), projections);

	code.setPipeProjection(projections.first);
	code.setFinishProjection(projections.second);

	node.setCode(code);
	plan.getParentOutput(0).addInput(node);
}

void SQLGroupOps::Union::setUpUnionProjections(
		OpCodeBuilder &builder, SQLType::UnionType unionType,
		const SQLValues::CompColumnList *keyColumnList,
		std::pair<Projection*, Projection*> &projections) const {
	SQLOpTypes::ProjectionType projType;
	switch (unionType) {
	case SQLType::UNION_ALL:
		return;
	case SQLType::UNION_DISTINCT:
		projType = SQLOpTypes::PROJ_DISTINCT;
		break;
	case SQLType::UNION_INTERSECT:
		projType = SQLOpTypes::PROJ_INTERSECT;
		break;
	case SQLType::UNION_EXCEPT:
		projType = SQLOpTypes::PROJ_EXCEPT;
		break;
	default:
		projType = SQLOpTypes::PROJ_COMPENSATE;
		assert(unionType == SQLType::END_UNION);
		break;
	}

	assert(keyColumnList != NULL);

	Projection &pipeProj = builder.createProjection(projType);
	{
		pipeProj.getProjectionCode().setKeyList(keyColumnList);
		pipeProj.addChain(builder.rewriteProjection(*projections.first));
	}

	Projection &finishProj = builder.createProjection(projType);
	{
		finishProj.addChain(*projections.first);

		if (projections.second != NULL) {
			finishProj.addChain(*projections.second);
		}
	}

	projections.first = &pipeProj;
	projections.second = &finishProj;
}

void SQLGroupOps::UnionAll::execute(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SQLValues::ArrayTuple tuple(cxt.getAllocator());
	cxt.getExprContext().setArrayTuple(0, &tuple);

	const uint32_t count = cxt.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		for (TupleListReader &reader = cxt.getReader(i); reader.exists();) {
			ReadableTuple readerTuple = reader.get();
			tuple.assign(
					cxt.getValueContext(), readerTuple,
					cxt.getInputColumnList(i));
			getCode().getPipeProjection()->projectBy(cxt, reader, i);
			reader.next();

			if (cxt.checkSuspended()) {
				return;
			}
		}
	}
}

void SQLGroupOps::UnionSorted::execute(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	UnionHeapQueue baseQueue(
			cxt.getAllocator(), getCode().getGreaterPredicate());
	TupleHeapQueue &queue = baseQueue.build(cxt);

	while (!queue.isEmpty()) {
		const TupleHeapQueue::Element &elem = queue.pop();
		const uint32_t index = static_cast<uint32_t>(elem.getOrdinal());

		TupleListReader &reader = cxt.getReader(index);

		SQLValues::ArrayTuple &tuple = elem.getValue().getTuple();
		cxt.getExprContext().setArrayTuple(0, &tuple);

		getCode().getPipeProjection()->projectBy(cxt, reader, index);

		reader.next();
		if (cxt.checkSuspended()) {
			return;
		}
		if (!reader.exists()) {
			continue;
		}

		ReadableTuple readerTuple = reader.get();
		tuple.assign(
				cxt.getValueContext(), readerTuple,
				cxt.getInputColumnList(index));

		queue.push(elem);
	}
}


SQLGroupOps::UnionSorted::UnionHeapQueue::UnionHeapQueue(
		util::StackAllocator &alloc, const SQLValues::TupleGreater &pred) :
		alloc_(alloc),
		queue_(setUpPredicate(pred), alloc),
		list_(alloc) {
}

SQLGroupOps::UnionSorted::UnionHeapQueue::~UnionHeapQueue() {
	while (!list_.empty()) {
		util::AllocUniquePtr<SQLValues::ArrayTuple> tuple(list_.back(), alloc_);
		list_.pop_back();
	}
}

SQLGroupOps::TupleHeapQueue& SQLGroupOps::UnionSorted::UnionHeapQueue::build(
		OpContext &cxt) {
	const uint32_t count = cxt.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		TupleListReader &reader = cxt.getReader(i);
		if (!reader.exists()) {
			continue;
		}

		util::AllocUniquePtr<SQLValues::ArrayTuple> tuple(
				ALLOC_UNIQUE(alloc_, SQLValues::ArrayTuple, alloc_));
		list_.push_back(tuple.get());

		ReadableTuple readerTuple = reader.get();
		tuple->assign(
				cxt.getValueContext(), readerTuple,
				cxt.getInputColumnList(i));

		queue_.push(TupleHeapQueue::Element(
				SQLValues::ArrayTupleRef(*tuple.release()), i));
	}

	return queue_;
}

SQLValues::TupleGreater
SQLGroupOps::UnionSorted::UnionHeapQueue::setUpPredicate(
		const SQLValues::TupleGreater &pred) {
	SQLValues::TupleGreater destPred = pred;
	destPred.getComparator().setForKeyOnlyArray(false);
	return destPred;
}


void SQLGroupOps::GroupProjection::initializeProjectionAt(
		OpContext &cxt) const {
	if (isFinishOnly()) {
		return;
	}

	util::StackAllocator &alloc = cxt.getAllocator();
	cxt.getResource(0, SQLOpTypes::PROJ_GROUP) =
			ALLOC_UNIQUE(alloc, SQLValues::ArrayTuple, alloc);
}

void SQLGroupOps::GroupProjection::project(OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SQLValues::ArrayTuple &key = cxt.getResource(
			0, SQLOpTypes::PROJ_GROUP).resolveAs<SQLValues::ArrayTuple>();

	if (isFinishOnly()) {
		if (!key.isEmpty()) {
			chainAt(CHAIN_FINISH).project(cxt);
		}
		return;
	}

	const SQLValues::CompColumnList *keyColumnList =
			getProjectionCode().getKeyList();
	assert(keyColumnList != NULL);

	ReadableTuple curTuple = cxt.getExprContext().getReadableTuple(0);
	if (key.isEmpty() || !SQLValues::TupleEq(*keyColumnList)(curTuple, key)) {
		if (!key.isEmpty()) {
			chainAt(CHAIN_FINISH).project(cxt);
		}
		chainAt(CHAIN_PIPE).clearProjection(cxt);
		key.assign(cxt.getValueContext(), curTuple, *keyColumnList);
	}
	chainAt(CHAIN_PIPE).project(cxt);
}

bool SQLGroupOps::GroupProjection::isFinishOnly() const {
	return (getProjectionCode().getKeyList() == NULL);
}


bool SQLGroupOps::UnionActionBase::onDifferent(
		UnionFlags &flags) const {
	static_cast<void>(flags);
	return false;
}

bool SQLGroupOps::UnionActionBase::onFinish(
		UnionFlags &flags) const {
	static_cast<void>(flags);
	return false;
}

bool SQLGroupOps::UnionActionBase::onNonMatch(
		UnionFlags &flags, const UnionFlags &initialFlags) const {
	static_cast<void>(flags);
	static_cast<void>(initialFlags);
	return false;
}

bool SQLGroupOps::UnionActionBase::onTuple(
		UnionFlags &flags, uint32_t index) const {
	static_cast<void>(flags);
	static_cast<void>(index);
	return false;
}

SQLGroupOps::UnionFlags SQLGroupOps::UnionActionBase::getInitialFlags(
		uint32_t inputCount) const {
	static_cast<void>(inputCount);
	return UnionFlags();
}


bool SQLGroupOps::UnionDistinctAction::onNonMatch(
		UnionFlags &flags, const UnionFlags &initialFlags) const {
	static_cast<void>(flags);
	static_cast<void>(initialFlags);
	return true;
}


bool SQLGroupOps::UnionIntersectAction::onNonMatch(
		UnionFlags &flags, const UnionFlags &initialFlags) const {
	flags = initialFlags;
	return false;
}

bool SQLGroupOps::UnionIntersectAction::onTuple(
		UnionFlags &flags, uint32_t index) const {
	flags &= ~(static_cast<UnionFlags>(1) << index);

	if (flags == 0) {
		flags = ~static_cast<UnionFlags>(0);
		return true;
	}

	return false;
}

SQLGroupOps::UnionFlags SQLGroupOps::UnionIntersectAction::getInitialFlags(
		uint32_t inputCount) const {
	return (~static_cast<UnionFlags>(
			(~static_cast<UnionFlags>(0)) << inputCount));
}


bool SQLGroupOps::UnionExceptAction::onDifferent(
		UnionFlags &flags) const {
	return (flags == 0);
}

bool SQLGroupOps::UnionExceptAction::onFinish(
		UnionFlags &flags) const {
	return (flags == 0);
}

bool SQLGroupOps::UnionExceptAction::onNonMatch(
		UnionFlags &flags, const UnionFlags &initialFlags) const {
	static_cast<void>(initialFlags);
	flags = 0;
	return false;
}

bool SQLGroupOps::UnionExceptAction::onTuple(
		UnionFlags &flags, uint32_t index) const {
	if (index != 0) {
		flags = 1;
	}

	return false;
}


bool SQLGroupOps::UnionCompensateAction::onDifferent(
		UnionFlags &flags) const {
	return (flags == 0);
}

bool SQLGroupOps::UnionCompensateAction::onFinish(
		UnionFlags &flags) const {
	return (flags == 0);
}

bool SQLGroupOps::UnionCompensateAction::onNonMatch(
		UnionFlags &flags, const UnionFlags &initialFlags) const {
	static_cast<void>(initialFlags);
	flags = 0;
	return false;
}

bool SQLGroupOps::UnionCompensateAction::onTuple(
		UnionFlags &flags, uint32_t index) const {
	if (index == 0) {
		flags = 1;
		return true;
	}
	return false;
}


template<typename Action>
void SQLGroupOps::SortedUnionProjection<Action>::initializeProjectionAt(
		OpContext &cxt) const {
	if (isFinishOnly()) {
		return;
	}

	util::StackAllocator &alloc = cxt.getAllocator();
	cxt.getResource(0, SQLOpTypes::PROJ_DISTINCT) = ALLOC_UNIQUE(
			alloc, SortedUnionContext, alloc,
			action_.getInitialFlags(cxt.getInputCount()));
}

template<typename Action>
void SQLGroupOps::SortedUnionProjection<Action>::project(
		OpContext &cxt) const {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SortedUnionContext &unionCxt = cxt.getResource(
			0, SQLOpTypes::PROJ_DISTINCT).resolveAs<SortedUnionContext>();

	if (isFinishOnly()) {
		if (!unionCxt.getLastTuple().isEmpty() &&
				action_.onFinish(unionCxt.getFlags())) {
			cxt.getExprContext().setArrayTuple(0, &unionCxt.getLastTuple());
			for (ChainIterator it(*this); it.exists(); it.next()) {
				it.get().project(cxt);
			}
		}
		return;
	}

	const SQLValues::CompColumnList *keyColumnList =
			getProjectionCode().getKeyList();
	assert(keyColumnList != NULL);

	SQLValues::ArrayTuple *lastTuple = cxt.getExprContext().getArrayTuple(0);
	assert(lastTuple != NULL);

	SQLValues::ArrayTuple lastKey(cxt.getAllocator());
	lastKey.assign(cxt.getValueContext(), *lastTuple, *keyColumnList);

	SQLValues::ArrayTuple &key = unionCxt.getKey();
	if (key.isEmpty() || !SQLValues::TupleEq(*keyColumnList)(lastKey, key)) {
		if (!key.isEmpty() && action_.onDifferent(unionCxt.getFlags())) {
			cxt.getExprContext().setArrayTuple(0, &unionCxt.getLastTuple());
			chainAt(0).project(cxt);
		}

		key.swap(lastKey);
		if (action_.onNonMatch(unionCxt.getFlags(), unionCxt.getInitialFlags())) {
			cxt.getExprContext().setArrayTuple(0, lastTuple);
			chainAt(0).project(cxt);
		}
	}

	const uint32_t index = cxt.getExprContext().getActiveInput();
	if (action_.onTuple(unionCxt.getFlags(), index)) {
		cxt.getExprContext().setArrayTuple(0, lastTuple);
		chainAt(0).project(cxt);
	}

	unionCxt.getLastTuple().assign(cxt.getValueContext(), *lastTuple);
}

template<typename Action>
bool SQLGroupOps::SortedUnionProjection<Action>::isFinishOnly() const {
	return (getProjectionCode().getKeyList() == NULL);
}


SQLGroupOps::SortedUnionContext::SortedUnionContext(
		util::StackAllocator &alloc, UnionFlags initialFlags) :
		key_(alloc),
		lastTuple_(alloc),
		initialFlags_(initialFlags),
		flags_(initialFlags) {
}

SQLGroupOps::UnionFlags SQLGroupOps::SortedUnionContext::getInitialFlags() {
	return initialFlags_;
}

SQLGroupOps::UnionFlags& SQLGroupOps::SortedUnionContext::getFlags() {
	return flags_;
}

SQLValues::ArrayTuple& SQLGroupOps::SortedUnionContext::getKey() {
	return key_;
}

SQLValues::ArrayTuple& SQLGroupOps::SortedUnionContext::getLastTuple() {
	return lastTuple_;
}
