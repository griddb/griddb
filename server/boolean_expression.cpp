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
/*!
	@file
	@brief Implementation of BoolExpr
*/

#include "boolean_expression.h"
#include "util/type.h"
#include "util/allocator.h"
#include "util/container.h"
#include "qp_def.h"
#include "query.h"
#include "query_processor.h"
#include "value_processor.h"
#include <cassert>
#include <iostream>

#if !WIN32
const ColumnId ColumnInfo::ROW_KEY_COLUMN_ID;
#endif

/*!
* @brief Constructor. Boolean expressions passed by the arguments are copied as
* pointer
*
* @param opType Operation type
* @param e1 Boolean expression instance
* @param e2 Boolean expression instance
* @param txn The transaction context
*/
BoolExpr::BoolExpr(
	Operation opType, BoolExpr *e1, BoolExpr *e2, TransactionContext &txn)
	: Expr(txn), operands_(txn.getDefaultAllocator()) {
	assert(e1 != NULL);
	operands_.push_back(e1);
	if (e2 != NULL) {
		operands_.push_back(e2);
	}
	unary_ = NULL;
	this->opeType_ = opType;
	noEval_ = false;
	type_ = BOOL_EXPR;
}

/*!
* @brief Constructor
*
* @param opType Operation type
* @param terms Vector of boolean expressions
* @param txn The transaction context
*/
BoolExpr::BoolExpr(Operation opType, BoolTerms &terms, TransactionContext &txn)
	: Expr(txn), operands_(txn.getDefaultAllocator()) {
	operands_.assign(terms.begin(), terms.end());
	unary_ = NULL;
	this->opeType_ = opType;
	noEval_ = false;
	type_ = BOOL_EXPR;
}

/*!
* @brief Constructor
*
* @param opType Operation type
* @param txn The transaction context
* @param size Number of boolean expressions
*/
BoolExpr::BoolExpr(Operation opType, TransactionContext &txn, int size, ...)
	: Expr(txn), operands_(txn.getDefaultAllocator()) {
	unary_ = NULL;
	this->opeType_ = opType;

	va_list argptr;
	va_start(argptr, size);
	if (size < 0) {
		size = 1024;
	}
	while (size-- > 0) {
		BoolExpr *p = va_arg(argptr, BoolExpr *);
		if (p == NULL) break;
		operands_.push_back(p);
	}
	va_end(argptr);
	noEval_ = false;
	type_ = BOOL_EXPR;
}

/*!
* @brief Constructor as unary
*
* @param e An expression instance
* @param txn The transaction context
*/
BoolExpr::BoolExpr(Expr *e, TransactionContext &txn)
	: Expr(txn), operands_(txn.getDefaultAllocator()) {
	unary_ = e;
	assert(e != NULL);
	opeType_ = UNARY;
	noEval_ = false;
	type_ = BOOL_EXPR;
}

/*!
* @brief Constructor as unary
*
* @param b a boolean value
* @param txn The transaction context
*/
BoolExpr::BoolExpr(bool b, TransactionContext &txn)
	: Expr(txn),
	  operands_(txn.getDefaultAllocator()),
	  unary_(Expr::newBooleanValue(b, txn)),
	  opeType_(UNARY),
	  noEval_(false) {

	type_ = BOOL_EXPR;
}

/*!
* @brief Constructor as unary
*
* @param b a boolean value
* @param txn The transaction context
*/
BoolExpr::BoolExpr(TrivalentLogicType b, TransactionContext &txn)
	: Expr(txn),
	  operands_(txn.getDefaultAllocator()),
	  unary_(QP_NEW_BY_TXN(txn) Expr(b, txn)),
	  opeType_(UNARY),
	  noEval_(false) {

	type_ = BOOL_EXPR;
}

/*!
* @brief Generate full duplicate of the expression. (Deep copy)
*
* @param txn The transaction context
* @param txn Object manager
*/
BoolExpr *BoolExpr::dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	BoolExpr *b = NULL;

	if (opeType_ == UNARY) {
		b = QP_NEW BoolExpr(unary_->dup(txn, objectManager, strategy), txn);
		b->noEval_ = noEval_;
	}
	else {
		b = QP_NEW BoolExpr(txn);
		b->opeType_ = opeType_;
		b->noEval_ = noEval_;
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			b->operands_.push_back((*it)->dup(txn, objectManager, strategy));
		}
		b->unary_ = NULL;
	}
	return b;
}

/*!
* @brief Destructor: recursively deletes all connected expression.
*
*/
BoolExpr::~BoolExpr() {
	if (opeType_ == UNARY) {
		QP_SAFE_DELETE(unary_);
	}
	else {
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			QP_DELETE(*it);
		}
	}
}

/*!
* @brief Evaluate the expression as a boolean value
*
* @param txn The transaction context
* @param txn Object manager
* @param column_values Binding environment
* @param function_map Function list
* @param default_return return value if cannot evaluate
*
* @return Evaluation result
*/
TrivalentLogicType BoolExpr::eval(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	ContainerRowWrapper *column_values, FunctionMap *function_map,
	TrivalentLogicType default_return) {
	TrivalentLogicType x;
	TrivalentLogicType thrown = TRI_FALSE;
	util::Exception ex;
	if (this->noEval_) {
		return default_return;  
	}
	switch (opeType_) {
	case AND: {
		TrivalentLogicType ret = TRI_TRUE;
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			try {
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				x = (*it)->eval(txn, objectManager, strategy, column_values, function_map,
					TRI_TRUE);  
				if (x == TRI_FALSE) {
					return TRI_FALSE;
				} else if (x == TRI_NULL) {
					ret = TRI_NULL;
				}
			}
			catch (util::Exception &e) {
				ex = e;
				thrown = TRI_TRUE;  
			}
		}
		if (thrown != TRI_FALSE) {
			throw ex;
		}
		return ret;
	}
	case OR: {
		TrivalentLogicType ret = TRI_FALSE;
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			try {
				if ((*it)->noEval_ == false) {
					util::StackAllocator::Scope scope(
						txn.getDefaultAllocator());
					x = (*it)->eval(txn, objectManager, strategy, column_values,
						function_map, TRI_FALSE);  
					if (x == TRI_TRUE) {
						return TRI_TRUE;
					} else if (x == TRI_NULL) {
						ret = TRI_NULL;
					}
				}
			}
			catch (util::Exception &e) {
				ex = e;
				thrown = TRI_TRUE;  
			}
		}
		if (thrown != TRI_FALSE) {
			throw ex;
		}
		return ret;
	}
	case NOT: {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		x = operands_[0]->eval(
			txn, objectManager, strategy, column_values, function_map, default_return);
		return notTrivalentLogic(x);
	}
	case UNARY: {
		Expr *e;
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		e = unary_->eval(
			txn, objectManager, strategy, column_values, function_map, EVAL_MODE_NORMAL);
		x = e->castTrivalentLogicValue();
		QP_DELETE(e);
		return x;
	}
	}
	GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
		"Internal logic error: Cannot eval expression.");
}

/*!
* @brief Make a new expression which expresses the disjunctive normal form
* (DNF) of the argument
*
* @param txn The transaction context
* @param txn Object manager
*
* @return New expression that is generated, which expresses
* DNF of this expression.
*/
BoolExpr *BoolExpr::makeDNF(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	BoolTerms cExprs(txn.getDefaultAllocator());
	BoolTerms::const_iterator it1, it2, it3, it4;

	if (opeType_ == UNARY) {
		return this->dup(txn, objectManager, strategy);
	}
	else if (opeType_ == NOT) {
		if (operands_[0]->opeType_ == UNARY) {
			return this->dup(txn, objectManager, strategy);
		}
		else if (operands_[0]->opeType_ == NOT) {
			return operands_[0]->operands_[0]->makeDNF(txn, objectManager, strategy);
		}
		else {
		}
	}
	else {
	}

	for (it1 = operands_.begin(); it1 != operands_.end(); it1++) {
		BoolExpr *b = (*it1)->makeDNF(txn, objectManager, strategy);
		b->checkDNF();
		cExprs.push_back(b);
	}

	if (isAllHaveTheType(UNARY, cExprs)) {
		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->opeType_ = opeType_;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			b->addOperand(txn, objectManager, strategy, *it1);
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;  
	}

	if (opeType_ == AND && isNooneHaveTheType(OR, cExprs)) {
		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->opeType_ = AND;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			if ((*it1)->opeType_ == UNARY || (*it1)->opeType_ == NOT) {
				b->addOperand(txn, objectManager, strategy, *it1);
			}
			else {
				b->addOperandsDNF(txn, objectManager, strategy, (*it1)->operands_);
			}
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;
	}

	if (opeType_ == OR && isNooneHaveTheType(AND, cExprs)) {
		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->opeType_ = OR;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			if ((*it1)->opeType_ == UNARY || (*it1)->opeType_ == NOT) {
				b->addOperand(txn, objectManager, strategy, *it1);
			}
			else {
				b->addOperandsDNF(txn, objectManager, strategy, (*it1)->operands_);
			}
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;
	}

	if (opeType_ == NOT) {

		assert(cExprs[0]->opeType_ == AND || cExprs[0]->opeType_ == OR);
		BoolExpr *b = QP_NEW BoolExpr(txn);
		if (cExprs[0]->opeType_ == AND) {  
			b->opeType_ = OR;
			for (it1 = cExprs[0]->operands_.begin();
				 it1 != cExprs[0]->operands_.end(); it1++) {
				BoolExpr *x = QP_NEW BoolExpr(NOT, *it1, NULL, txn);
				b->addOperand(
					txn, objectManager, strategy, x->makeDNF(txn, objectManager, strategy));
				QP_DELETE(x);
			}

			for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
				QP_DELETE(*it1);
			}
			b->checkDNF();
			return b;
		}
		else {  
			assert(cExprs[0]->opeType_ == OR);
			b->opeType_ = AND;
			for (it1 = cExprs[0]->operands_.begin();
				 it1 != cExprs[0]->operands_.end(); it1++) {
				BoolExpr *x = QP_NEW BoolExpr(NOT, *it1, NULL, txn);
				b->addOperand(txn, objectManager, strategy, x);
				QP_DELETE(x);
			}
			BoolExpr *r = b->makeDNF(txn, objectManager, strategy);
			for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
				QP_DELETE(*it1);
			}
			QP_DELETE(b);
			r->checkDNF();
			return r;
		}
	}
	else if (opeType_ == AND) {

		BoolTerms tmpTerms1(txn.getDefaultAllocator()),
			tmpTerms2(txn.getDefaultAllocator()),
			tmpTerms3(txn.getDefaultAllocator());
		if (cExprs[0]->opeType_ == OR) {
			tmpTerms3.assign(
				cExprs[0]->operands_.begin(), cExprs[0]->operands_.end());
		}
		else {
			tmpTerms3.clear();
			tmpTerms3.push_back(cExprs[0]);
		}
		it2 = cExprs.begin();
		while (++it2 != cExprs.end()) {
			tmpTerms1.assign(tmpTerms3.begin(), tmpTerms3.end());
			if ((*it2)->opeType_ == OR) {
				tmpTerms2.assign(
					(*it2)->operands_.begin(), (*it2)->operands_.end());
			}
			else {
				tmpTerms2.clear();
				tmpTerms2.push_back(*it2);
			}
			tmpTerms3.clear();

			for (it3 = tmpTerms1.begin(); it3 != tmpTerms1.end(); it3++) {
				for (it4 = tmpTerms2.begin(); it4 != tmpTerms2.end(); it4++) {
					BoolExpr *e = (*it3)->makeDNF(txn, objectManager, strategy);
					BoolExpr *f = (*it4)->makeDNF(txn, objectManager, strategy);
					if (e->opeType_ == AND) {
						e->addOperand(txn, objectManager, strategy, f);
						tmpTerms3.push_back(e->makeDNF(txn, objectManager, strategy));
					}
					else {
						BoolExpr *b = QP_NEW BoolExpr(txn);
						b->opeType_ = AND;
						b->addOperand(txn, objectManager, strategy, e);
						b->addOperand(txn, objectManager, strategy, f);
						tmpTerms3.push_back(b->makeDNF(txn, objectManager, strategy));
						QP_DELETE(b);
					}
					QP_DELETE(e);
					QP_DELETE(f);
				}
			}
		}

		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			QP_DELETE(*it1);
		}
		BoolExpr *b = QP_NEW BoolExpr(OR, tmpTerms3, txn);
		b->checkDNF();
		return b;
	}
	else if (opeType_ == OR) {

		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->opeType_ = OR;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			switch ((*it1)->opeType_) {
			case UNARY:
			case NOT:
			case AND:
				b->addOperand(txn, objectManager, strategy, *it1);
				break;
			case OR:
				b->addOperandsDNF(txn, objectManager, strategy, (*it1)->operands_);
				break;
			}
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Cannot determine type of expression.");
	}
}

/*!
* @brief Dump expression tree to the output stream for debug
*
* @param txn The transaction context
* @param txn Object manager
* @param os Output stream
*/
void BoolExpr::dumpTree(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, std::ostream &os) {
	dumpTreeSub(txn, objectManager, strategy, os, 0, "", NULL, NULL);
}

/*!
* @brief Dump expression tree to the output stream for debug
*
* @param txn The transaction context
* @param txn Object manager
* @param os Output stream
* @param column_values Binding environment
* @param function_map Function list
*/
void BoolExpr::dumpTree(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	std::ostream &os, ContainerRowWrapper *column_values,
	FunctionMap *function_map) {
	dumpTreeSub(txn, objectManager, strategy, os, 0, "", column_values, function_map);
}

/*!
* @brief Subroutine for dumpTree to the output stream
*
* @param txn The transaction context
* @param txn Object manager
* @param os Output stream
* @param level Tree depth
* @param head Header to express a tree
* @param column_values Binding environment
* @param function_map Function list
*/
void BoolExpr::dumpTreeSub(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, std::ostream &os, int level, const char *head,
	ContainerRowWrapper *column_values, FunctionMap *function_map) {
	os << head << ':';
	switch (opeType_) {
	case AND:
		os << "AND" << std::endl;
		break;
	case OR:
		os << "OR" << std::endl;
		break;
	case NOT:
		os << "NOT" << std::endl;
		break;
	case UNARY: {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		Expr *e = unary_->eval(
			txn, objectManager, strategy, column_values, function_map, EVAL_MODE_PRINT);
		os << "UNARY: " << e->getValueAsString(txn) << std::endl;
		QP_DELETE(e);
		break;
	}
	}
	if (opeType_ != UNARY) {
		util::String x("", QP_ALLOCATOR);
		for (int i = 0; i < level; i++) {
			x += "  ";
		}
		for (BoolTerms::iterator it = operands_.begin(); it != operands_.end();
			 it++) {
			(*it)->dumpTreeSub(txn, objectManager, strategy, os, level + 1,
				(x + "--").c_str(), column_values, function_map);
		}
	}
}

/*!
* @brief Make an optimized expression.
* The optimization is done in two ways.
* 1. If cond A 'AND' cond B are evaluated and
*    A includes B, only B is returned.
* 2. If cond A 'OR' cond B are evaluated and
*    A includes B, only A is evaluated.
*
* @param txn The transaction context
* @param txn Object manager
* @param column_values Binding environment
* @param function_map Function list
*
* @return Optimized expression.
*/
BoolExpr *BoolExpr::makeOptimizedExpr(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ContainerRowWrapper *column_values,
	FunctionMap *function_map) {
	try {
		checkDNF();
	}
	catch (util::Exception &) {
		BoolExpr *e = makeDNF(txn, objectManager, strategy);
		BoolExpr *f = e->makeOptimizedExpr(
			txn, objectManager, strategy, column_values, function_map);

		QP_DELETE(e);
		return f;
	}

	BoolTerms::iterator it;


	if (opeType_ == AND) {
		BoolTerms newOps(txn.getDefaultAllocator());
		for (it = operands_.begin(); it != operands_.end(); it++) {
			BoolExpr *bExpr = (*it)->makeOptimizedExpr(
				txn, objectManager, strategy, column_values, function_map);
			newOps.push_back(bExpr);
		}
		return QP_NEW BoolExpr(AND, newOps, txn);

	}
	else if (opeType_ == OR) {
		BoolTerms newOps(txn.getDefaultAllocator());
		for (it = operands_.begin(); it != operands_.end(); it++) {
			BoolExpr *bExpr = (*it)->makeOptimizedExpr(
				txn, objectManager, strategy, column_values, function_map);
			newOps.push_back(bExpr);
		}
		return QP_NEW BoolExpr(OR, newOps, txn);

	}
	else if (opeType_ == NOT) {
		BoolExpr *bExpr = operands_[0]->makeOptimizedExpr(
			txn, objectManager, strategy, column_values, function_map);
		try {
			TrivalentLogicType b;
			{
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				b = bExpr->eval(
					txn, objectManager, strategy, column_values, function_map, TRI_TRUE);
				QP_DELETE(bExpr);
			}
			return QP_NEW BoolExpr(notTrivalentLogic(b), txn);
		}
		catch (util::Exception &) {
			BoolExpr *p = QP_NEW BoolExpr(NOT, bExpr, NULL, txn);
			BoolExpr *r = p->makeDNF(txn, objectManager, strategy);
			QP_DELETE(p);
			return r;
		}

	}
	else if (opeType_ == UNARY) {
		Expr *e = NULL;
		e = unary_->eval(txn, objectManager, strategy, column_values, function_map,
			EVAL_MODE_CONTRACT);
		return QP_NEW BoolExpr(e, txn);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Cannot optimize expression");
	}
}


/*!
 * @brief transform expression into list of conjunctive clauses
 *
 * @return list of conjunctive clauses
 */
void BoolExpr::toOrList(util::XArray<BoolExpr *> &orList) {
	assert(orList.empty());
	this->checkDNF();

	switch (opeType_) {
	case AND:
	case UNARY:
	case NOT:
		orList.push_back(
			this);  
		break;
	case OR:
		for (size_t i = 0; i < this->operands_.size(); i++) {
			orList.push_back(operands_[i]);  
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Cannot determine type of expression.");
	}
}

/*!
 * @brief transform expression into list of simple clause
 *
 * @return list of simple clause
 */
void BoolExpr::toAndList(util::XArray<BoolExpr *> &andList) {
	assert(andList.empty());
	this->checkDNF();

	switch (opeType_) {
	case UNARY:
	case NOT:  
		andList.push_back(this);  
		break;
	case AND:
		for (size_t i = 0; i < this->operands_.size(); i++) {
			andList.push_back(operands_[i]);  
		}
		break;
	case OR:  
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: AND list contains OR expression.");
	default:  
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Cannot determine type of expression.");
	}
}

/*!
 * @brief transform into TermCondition
 *
 * @return true : success, false : fail to get
 */
bool BoolExpr::getCondition(TransactionContext &txn, MapType type,
	Query &queryObj, TermCondition *&cond) {
	Expr *unary;
	if (this->opeType_ == UNARY) {
		unary = this->unary_;
		cond = unary->toCondition(txn, type, queryObj, false);
	}
	else if (this->opeType_ == NOT) {
		unary = this->operands_[0]->unary_;
		cond = unary->toCondition(txn, type, queryObj, true);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Invalid boolean expression type.");
	}

	return (cond != NULL);
}

/*!
 * @brief transform and expressions into BtreeMap's SearchContext
 *
 */
void BoolExpr::toSearchContext(TransactionContext &txn,
	util::XArray<BoolExpr *> &andList, IndexData *indexData,
	QueryForCollection &queryObj, BtreeMap::SearchContext *&sc,
	uint32_t &restEval, ResultSize limit) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	if (indexData != NULL) {
		sc = ALLOC_NEW(alloc) BtreeMap::SearchContext (alloc, *(indexData->columnIds_));
	} else {
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(queryObj.collection_->getRowIdColumnId());
		sc = ALLOC_NEW(alloc) BtreeMap::SearchContext (alloc, columnIds);
	}
	sc->reserveCondition(andList.size());
	restEval = 0;
	util::Vector<ColumnId>::const_iterator columnIdItr;
	const util::Vector<ColumnId> &columnIds = sc->getColumnIds();
	for (size_t i = 0; i < andList.size(); i++) {
		TermCondition *c = NULL;
		andList[i]->getCondition(txn, MAP_TYPE_BTREE, queryObj, c);
		if (c == NULL) {
			restEval++;
		} else {
			andList[i]->enableEvaluationFilter();
			columnIdItr = std::find(columnIds.begin(), 
				columnIds.end(), c->columnId_);
			bool isKey = false;
			if (columnIdItr != columnIds.end()) {
				if (sc->getKeyColumnNum() == 1) {
					if (sc->getNullCond() != BaseIndex::SearchContext::IS_NULL && c->isRangeCondition()) {
						isKey = true;
					} else if (c->opType_ == DSExpression::IS) {
						sc->setNullCond(BaseIndex::SearchContext::IS_NULL);
					}
				} else {
					isKey = true;
				}
			}
			sc->addCondition(txn, *c, isKey);
		}
	}
	sc->setLimit((0 == restEval) ? limit : MAX_RESULT_SIZE);
}

/*!
 * @brief transform and expressions into RtreeMap's SearchContext
 *
 */
void BoolExpr::toSearchContext(TransactionContext &txn,
	util::XArray<BoolExpr *> &andList, IndexData *indexData,
	QueryForCollection &queryObj, RtreeMap::SearchContext *&sc,
	uint32_t &restEval, ResultSize limit) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	if (indexData != NULL) {
		sc = ALLOC_NEW(alloc) RtreeMap::SearchContext (alloc, *(indexData->columnIds_));
	} else {
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(queryObj.collection_->getRowIdColumnId());
		sc = ALLOC_NEW(alloc) RtreeMap::SearchContext (alloc, columnIds);
	}
	sc->reserveCondition(andList.size());
	restEval = 0;

	int32_t conditionDealed;
	const util::Vector<ColumnId> &columnIds = sc->getColumnIds();
	for (size_t i = 0; i < andList.size(); i++) {
		TermCondition *c = NULL;
		conditionDealed = andList[i]->getCondition(txn, MAP_TYPE_SPATIAL,
			queryObj, c);
		if (conditionDealed) {
			andList[i]->enableEvaluationFilter();
			util::Vector<ColumnId>::const_iterator columnIdItr = 
				std::find(columnIds.begin(), 
				columnIds.end(), c->columnId_);
			bool isKey = false;
			if (columnIdItr != columnIds.end()) {
				if (sc->getNullCond() != BaseIndex::SearchContext::IS_NULL) {
					isKey = true;
				}
				if (c->opType_ == DSExpression::IS) {
					sc->setNullCond(BaseIndex::SearchContext::IS_NULL);
				}
			}
			sc->addCondition(txn, *c, isKey);
		}
		else {
			restEval++;
		}
	}
	sc->setLimit((0 == restEval) ? limit : MAX_RESULT_SIZE);
}

/*!
 * @brief transform and expressions into BtreeMap's SearchContext
 *
 */
void BoolExpr::toSearchContext(TransactionContext &txn,
	util::XArray<BoolExpr *> &andList,
	IndexData *indexData, QueryForTimeSeries &queryObj,
	BtreeMap::SearchContext *&sc, uint32_t &restEval, ResultSize limit) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	if (indexData != NULL) {
		sc = ALLOC_NEW(alloc) BtreeMap::SearchContext (alloc, *(indexData->columnIds_));
	} else {
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(queryObj.timeSeries_->getRowIdColumnId());
		sc = ALLOC_NEW(alloc) BtreeMap::SearchContext (alloc, columnIds);
	}
	sc->reserveCondition(andList.size() + 1);  
	restEval = 0;

	util::Vector<ColumnId>::const_iterator columnIdItr;
	const util::Vector<ColumnId> &columnIds = sc->getColumnIds();
	for (size_t i = 0; i < andList.size(); i++) {
		TermCondition *c = NULL;
		andList[i]->getCondition(txn, MAP_TYPE_BTREE, queryObj, c);
		if (c == NULL) {
			restEval++;
		} else {
			andList[i]->enableEvaluationFilter();
			columnIdItr = std::find(columnIds.begin(), 
				columnIds.end(), c->columnId_);
			bool isKey = false;
			if (columnIdItr != columnIds.end()) {
				if (sc->getKeyColumnNum() == 1) {
					if (sc->getNullCond() != BaseIndex::SearchContext::IS_NULL && c->isRangeCondition()) {
						isKey = true;
					} else if (c->opType_ == DSExpression::IS) {
						sc->setNullCond(BaseIndex::SearchContext::IS_NULL);
					}
				} else {
					isKey = true;
				}
			}
			sc->addCondition(txn, *c, isKey);
		}
	}

	sc->setLimit((0 == restEval) ? limit : MAX_RESULT_SIZE);
}

/*!
 * @brief get usable indices
 */
void BoolExpr::getIndexBitmapAndInfo(TransactionContext &txn,
	BaseContainer &baseContainer, Query &queryObj, uint32_t &mapBitmap,
	ColumnInfo *&indexColumnInfo) {
	Expr *unary;
	Expr::Operation op = Expr::NONE;
	if (this->opeType_ == UNARY) {
		unary = this->unary_;
		unary->getIndexBitmapAndInfo(txn, baseContainer, queryObj, mapBitmap,
			indexColumnInfo, op, false);
		if (unary->isColumn()) {
#define TOBITMAP(x) (1 << (x))
			mapBitmap &= TOBITMAP(MAP_TYPE_BTREE);
			if (queryObj.doExplain()) {
				util::String str("", QP_ALLOCATOR);
				str += "JUST COLUMN (";
				str += unary->getValueAsString(txn);
				str += ")";
				queryObj.addExplain(
					1, "INDEX_DISABLE", "INDEX_TYPE", "BTREE", str.c_str());
			}
		}
	}
	else if (this->opeType_ == NOT) {
		unary = this->operands_[0]->unary_;
		unary->getIndexBitmapAndInfo(
			txn, baseContainer, queryObj, mapBitmap, indexColumnInfo, op, true);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Invalid boolean expression type.");
	}
}

/*!
 * @brief Create new expression to optimized expression.
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param column_values Binding environment
 * @param function_map Function list
 * @param mode EvalMode
 *
 * @return Evaluation result as expression.
 */
Expr *BoolExpr::eval(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	ContainerRowWrapper *column_values, FunctionMap *function_map,
	EvalMode mode) {
	switch (mode) {
	case EVAL_MODE_NORMAL: {
		TrivalentLogicType b;
		{
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			b = this->eval(txn, objectManager, strategy, column_values, function_map, TRI_TRUE);
		}
		if (b == TRI_NULL) {
			return Expr::newNullValue(txn);
		} else {
			return Expr::newBooleanValue(b == TRI_TRUE, txn);
		}
	}

	case EVAL_MODE_PRINT: {
		util::NormalOStringStream os;
		dumpTreeSub(txn, objectManager, strategy, os, 0, "", column_values, function_map);
		return Expr::newStringValue(os.str().c_str(), txn);
	}
	case EVAL_MODE_CONTRACT:
		return makeOptimizedExpr(
			txn, objectManager, strategy, column_values, function_map);
	}

	GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
		"Internal logic error: Invalid boolean evaluation mode.");
}
