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
	add<SQLOpTypes::OP_JOIN_SORTED, JoinSorted>();
	add<SQLOpTypes::OP_JOIN_NESTED, JoinNested>();
	add<SQLOpTypes::OP_JOIN_COMP_MERGE, JoinCompMerge>();
}


void SQLJoinOps::Join::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	const OpCode &code = getCode();

	const uint32_t inCount = getCode().getInputCount();

	if (code.getJoinType() != SQLType::JOIN_INNER) {
		OpNode &node = plan.createNode(SQLOpTypes::OP_JOIN_OUTER);

		for (uint32_t i = 0; i < inCount; i++) {
			node.addInput(plan.getParentInput(i));
		}
		plan.getParentOutput(0).addInput(node);

		node.setCode(code);
		return;
	}

	const SQLValues::CompColumnList *keyColumnList = code.findKeyColumnList();
	const bool sorted = (keyColumnList != NULL);

	OpNode &joinNode = plan.createNode(
			sorted ? SQLOpTypes::OP_JOIN_SORTED : SQLOpTypes::OP_JOIN_NESTED);

	for (uint32_t i = 0; i < inCount; i++) {
		joinNode.addInput(plan.getParentInput(i));
	}

	{
		OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
		OpCode joinCode = code;

		std::pair<Projection*, Projection*> projections =
				builder.arrangeProjections(joinCode, true, true);

		if (sorted) {
			projections.first = &builder.createFilteredProjection(
					*projections.first,
					builder.getExprRewriter().compColumnListToPredicate(
							builder.getExprFactoryContext(),
							*keyColumnList));
		}

		joinCode.setPipeProjection(projections.first);
		joinCode.setFinishProjection(projections.second);

		joinNode.setCode(joinCode);
	}
	plan.getParentOutput(0).addInput(joinNode);
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

		std::pair<Projection*, Projection*> projections =
				builder.arrangeProjections(code, true, true);
		code.setPipeProjection(projections.first);
		code.setFinishProjection(projections.second);
		node.setCode(code);

		plan.getParentOutput(0).addInput(node);
	}
}


void SQLJoinOps::JoinSorted::compile(OpContext &cxt) const {
	OpPlan &plan = cxt.getPlan();
	const OpCode &code = getCode();

	OpNode &joinNode = plan.createNode(SQLOpTypes::OP_JOIN_COMP_MERGE);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	const uint32_t inCount = getCode().getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		OpNode &subNode = plan.createNode(SQLOpTypes::OP_SORT);
		OpCode subCode;

		SQLExprs::ExprRewriter &rewriter = builder.getExprRewriter();
		SQLExprs::ExprRewriter::Scope scope(rewriter);
		rewriter.activateColumnMapping(builder.getExprFactoryContext());
		rewriter.setMappedInput(i, 0);

		SQLValues::CompColumnList &list = builder.createCompColumnList();

		const SQLValues::CompColumnList srcList = code.getKeyColumnList();
		for (SQLValues::CompColumnList::const_iterator it = srcList.begin();
				it != srcList.end(); ++it) {
			SQLValues::CompColumn key;
			key.setColumnPos(it->getColumnPos((i == 0)), true);
			list.push_back(key);
		}

		subCode.setKeyColumnList(&list);
		subCode.setPipeProjection(
				&builder.createIdenticalProjection(false, i));

		subNode.setCode(subCode);
		subNode.addInput(plan.getParentInput(i));

		joinNode.addInput(subNode);
	}

	joinNode.setCode(code);
	plan.getParentOutput(0).addInput(joinNode);
}


void SQLJoinOps::JoinNested::execute(OpContext &cxt) const {
	uint32_t inList[] = { IN_LEFT, IN_RIGHT }; 
	cxt.setReaderRandomAccess(inList[1]);

	TupleListReader &reader1 = cxt.getReader(inList[0]); 
	TupleListReader &reader2 = cxt.getReader(inList[1]); 
	TupleListReader &reader2Top = cxt.getReader(inList[1], 1); 

	if (!cxt.isInputCompleted(inList[1]) || !reader2.exists()) {
		return;
	}
	reader2Top.exists(); 

	for (; reader1.exists(); reader1.next()) {
		if (cxt.checkSuspended()) { 
			return;
		}
		for (; reader2.exists(); reader2.next()) {
			if (cxt.checkSuspended()) { 
				return;
			}
			getCode().getPipeProjection()->project(cxt);
		}

		reader2.assign(reader2Top);
	}
}


void SQLJoinOps::JoinCompMerge::execute(OpContext &cxt) const {
	uint32_t inList[] = { IN_LEFT, IN_RIGHT }; 
	cxt.setReaderRandomAccess(inList[1]);

	TupleListReader &reader1 = cxt.getReader(inList[0]); 
	TupleListReader &reader2 = cxt.getReader(inList[1], IN_INSIDE_MAIN); 
	TupleListReader &reader2Lower = cxt.getReader(inList[1], IN_INSIDE_SUB); 
	const SQLValues::TupleRangeComparator &comp =
			getCode().getJoinRangePredicate();

	if (!reader2.exists()) {
		return;
	}
	reader2Lower.exists(); 

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

			const int32_t ret = comp(reader1.get(), reader2.get());

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

			getCode().getPipeProjection()->project(cxt);
		}

		reader2.assign(reader2Lower);
		lowerAssignable = true;
	}
}
