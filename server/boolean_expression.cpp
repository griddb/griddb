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
	this->type_ = opType;
	noEval_ = false;
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
	this->type_ = opType;
	noEval_ = false;
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
	this->type_ = opType;

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
	type_ = UNARY;
	noEval_ = false;
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
	  type_(UNARY),
	  noEval_(false) {}

/*!
* @brief Generate full duplicate of the expression. (Deep copy)
*
* @param txn The transaction context
* @param txn Object manager
*/
BoolExpr *BoolExpr::dup(TransactionContext &txn, ObjectManager &objectManager) {
	BoolExpr *b = NULL;

	if (type_ == UNARY) {
		b = QP_NEW BoolExpr(unary_->dup(txn, objectManager), txn);
		b->noEval_ = noEval_;
	}
	else {
		b = QP_NEW BoolExpr(txn);
		b->type_ = type_;
		b->noEval_ = noEval_;
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			b->operands_.push_back((*it)->dup(txn, objectManager));
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
	if (type_ == UNARY) {
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
bool BoolExpr::eval(TransactionContext &txn, ObjectManager &objectManager,
	ContainerRowWrapper *column_values, FunctionMap *function_map,
	bool default_return) {
	bool x;
	bool thrown = false;
	util::Exception ex;
	if (this->noEval_) {
		return default_return;  
	}
	switch (type_) {
	case AND: {
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			try {
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				x = (*it)->eval(txn, objectManager, column_values, function_map,
					true);  
				if (x == false) {
					return false;
				}
			}
			catch (util::Exception &e) {
				ex = e;
				thrown = true;  
			}
		}
		if (thrown) {
			throw ex;
		}
		return true;
	}
	case OR: {
		for (BoolTerms::const_iterator it = operands_.begin();
			 it != operands_.end(); it++) {
			try {
				if ((*it)->noEval_ == false) {
					util::StackAllocator::Scope scope(
						txn.getDefaultAllocator());
					x = (*it)->eval(txn, objectManager, column_values,
						function_map, false);  
					if (x == true) {
						return true;
					}
				}
			}
			catch (util::Exception &e) {
				ex = e;
				thrown = true;  
			}
		}
		if (thrown) {
			throw ex;
		}
		return false;
	}
	case NOT: {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		x = operands_[0]->eval(
			txn, objectManager, column_values, function_map, !default_return);
		return !x;
	}
	case UNARY: {
		Expr *e;
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		e = unary_->eval(
			txn, objectManager, column_values, function_map, EVAL_MODE_NORMAL);
		x = *e;
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
	TransactionContext &txn, ObjectManager &objectManager) {
	BoolTerms cExprs(txn.getDefaultAllocator());
	BoolTerms::const_iterator it1, it2, it3, it4;

	if (type_ == UNARY) {
		return this->dup(txn, objectManager);
	}
	else if (type_ == NOT) {
		if (operands_[0]->type_ == UNARY) {
			return this->dup(txn, objectManager);
		}
		else if (operands_[0]->type_ == NOT) {
			return operands_[0]->operands_[0]->makeDNF(txn, objectManager);
		}
		else {
		}
	}
	else {
	}

	for (it1 = operands_.begin(); it1 != operands_.end(); it1++) {
		BoolExpr *b = (*it1)->makeDNF(txn, objectManager);
		b->checkDNF();
		cExprs.push_back(b);
	}

	if (isAllHaveTheType(UNARY, cExprs)) {
		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->type_ = type_;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			b->addOperand(txn, objectManager, *it1);
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;  
	}

	if (type_ == AND && isNooneHaveTheType(OR, cExprs)) {
		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->type_ = AND;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			if ((*it1)->type_ == UNARY || (*it1)->type_ == NOT) {
				b->addOperand(txn, objectManager, *it1);
			}
			else {
				b->addOperandsDNF(txn, objectManager, (*it1)->operands_);
			}
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;
	}

	if (type_ == OR && isNooneHaveTheType(AND, cExprs)) {
		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->type_ = OR;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			if ((*it1)->type_ == UNARY || (*it1)->type_ == NOT) {
				b->addOperand(txn, objectManager, *it1);
			}
			else {
				b->addOperandsDNF(txn, objectManager, (*it1)->operands_);
			}
			QP_DELETE(*it1);
		}
		b->checkDNF();
		return b;
	}

	if (type_ == NOT) {

		assert(cExprs[0]->type_ == AND || cExprs[0]->type_ == OR);
		BoolExpr *b = QP_NEW BoolExpr(txn);
		if (cExprs[0]->type_ == AND) {  
			b->type_ = OR;
			for (it1 = cExprs[0]->operands_.begin();
				 it1 != cExprs[0]->operands_.end(); it1++) {
				BoolExpr *x = QP_NEW BoolExpr(NOT, *it1, NULL, txn);
				b->addOperand(
					txn, objectManager, x->makeDNF(txn, objectManager));
				QP_DELETE(x);
			}

			for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
				QP_DELETE(*it1);
			}
			b->checkDNF();
			return b;
		}
		else {  
			assert(cExprs[0]->type_ == OR);
			b->type_ = AND;
			for (it1 = cExprs[0]->operands_.begin();
				 it1 != cExprs[0]->operands_.end(); it1++) {
				BoolExpr *x = QP_NEW BoolExpr(NOT, *it1, NULL, txn);
				b->addOperand(txn, objectManager, x);
				QP_DELETE(x);
			}
			BoolExpr *r = b->makeDNF(txn, objectManager);
			for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
				QP_DELETE(*it1);
			}
			QP_DELETE(b);
			r->checkDNF();
			return r;
		}
	}
	else if (type_ == AND) {

		BoolTerms tmpTerms1(txn.getDefaultAllocator()),
			tmpTerms2(txn.getDefaultAllocator()),
			tmpTerms3(txn.getDefaultAllocator());
		if (cExprs[0]->type_ == OR) {
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
			if ((*it2)->type_ == OR) {
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
					BoolExpr *e = (*it3)->makeDNF(txn, objectManager);
					BoolExpr *f = (*it4)->makeDNF(txn, objectManager);
					if (e->type_ == AND) {
						e->addOperand(txn, objectManager, f);
						tmpTerms3.push_back(e->makeDNF(txn, objectManager));
					}
					else {
						BoolExpr *b = QP_NEW BoolExpr(txn);
						b->type_ = AND;
						b->addOperand(txn, objectManager, e);
						b->addOperand(txn, objectManager, f);
						tmpTerms3.push_back(b->makeDNF(txn, objectManager));
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
	else if (type_ == OR) {

		BoolExpr *b = QP_NEW BoolExpr(txn);
		b->type_ = OR;
		for (it1 = cExprs.begin(); it1 != cExprs.end(); it1++) {
			switch ((*it1)->type_) {
			case UNARY:
			case NOT:
			case AND:
				b->addOperand(txn, objectManager, *it1);
				break;
			case OR:
				b->addOperandsDNF(txn, objectManager, (*it1)->operands_);
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
	TransactionContext &txn, ObjectManager &objectManager, std::ostream &os) {
	dumpTreeSub(txn, objectManager, os, 0, "", NULL, NULL);
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
void BoolExpr::dumpTree(TransactionContext &txn, ObjectManager &objectManager,
	std::ostream &os, ContainerRowWrapper *column_values,
	FunctionMap *function_map) {
	dumpTreeSub(txn, objectManager, os, 0, "", column_values, function_map);
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
	ObjectManager &objectManager, std::ostream &os, int level, const char *head,
	ContainerRowWrapper *column_values, FunctionMap *function_map) {
	os << head << ':';
	switch (type_) {
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
			txn, objectManager, column_values, function_map, EVAL_MODE_PRINT);
		os << "UNARY: " << e->getValueAsString(txn) << std::endl;
		QP_DELETE(e);
		break;
	}
	}
	if (type_ != UNARY) {
		util::String x("", QP_ALLOCATOR);
		for (int i = 0; i < level; i++) {
			x += "  ";
		}
		for (BoolTerms::iterator it = operands_.begin(); it != operands_.end();
			 it++) {
			(*it)->dumpTreeSub(txn, objectManager, os, level + 1,
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
	ObjectManager &objectManager, ContainerRowWrapper *column_values,
	FunctionMap *function_map) {
	try {
		checkDNF();
	}
	catch (util::Exception &) {
		BoolExpr *e = makeDNF(txn, objectManager);
		BoolExpr *f = e->makeOptimizedExpr(
			txn, objectManager, column_values, function_map);

		QP_DELETE(e);
		return f;
	}

	BoolTerms::iterator it;


	if (type_ == AND) {
		BoolTerms newOps(txn.getDefaultAllocator());
		for (it = operands_.begin(); it != operands_.end(); it++) {
			BoolExpr *bExpr = (*it)->makeOptimizedExpr(
				txn, objectManager, column_values, function_map);
			newOps.push_back(bExpr);
		}
		return QP_NEW BoolExpr(AND, newOps, txn);

	}
	else if (type_ == OR) {
		BoolTerms newOps(txn.getDefaultAllocator());
		for (it = operands_.begin(); it != operands_.end(); it++) {
			BoolExpr *bExpr = (*it)->makeOptimizedExpr(
				txn, objectManager, column_values, function_map);
			newOps.push_back(bExpr);
		}
		return QP_NEW BoolExpr(OR, newOps, txn);

	}
	else if (type_ == NOT) {
		BoolExpr *bExpr = operands_[0]->makeOptimizedExpr(
			txn, objectManager, column_values, function_map);
		bool b;
		try {
			{
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				b = bExpr->eval(
					txn, objectManager, column_values, function_map, true);
				QP_DELETE(bExpr);
			}
			return QP_NEW BoolExpr(!b, txn);
		}
		catch (util::Exception &) {
			BoolExpr *p = QP_NEW BoolExpr(NOT, bExpr, NULL, txn);
			BoolExpr *r = p->makeDNF(txn, objectManager);
			QP_DELETE(p);
			return r;
		}
	}
	else if (type_ == UNARY) {
		Expr *e = NULL;
		e = unary_->eval(txn, objectManager, column_values, function_map,
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

	switch (type_) {
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

	switch (type_) {
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
	uint32_t indexColumnId, Query &queryObj, TermCondition *&cond,
	const void *&startKey, uint32_t &startKeySize, int32_t &isStartKeyIncluded,
	const void *&endKey, uint32_t &endKeySize, int32_t &isEndKeyIncluded) {
	Expr *unary;
	const void *startKey2 = startKey, *endKey2 = endKey;
	if (this->type_ == UNARY) {
		unary = this->unary_;
		cond = unary->toCondition(txn, type, indexColumnId, queryObj, startKey,
			startKeySize, isStartKeyIncluded, endKey, endKeySize,
			isEndKeyIncluded, false);
	}
	else if (this->type_ == NOT) {
		unary = this->operands_[0]->unary_;
		cond = unary->toCondition(txn, type, indexColumnId, queryObj, startKey,
			startKeySize, isStartKeyIncluded, endKey, endKeySize,
			isEndKeyIncluded, true);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Invalid boolean expression type.");
	}

	return (cond != NULL) || (startKey2 != startKey) || (endKey2 != endKey);
}

/*!
 * @brief transform and expressions into BtreeMap's SearchContext
 *
 */
void BoolExpr::toSearchContext(TransactionContext &txn,
	util::XArray<BoolExpr *> &andList, ColumnInfo *indexColumnInfo,
	QueryForCollection &queryObj, BtreeMap::SearchContext &sc,
	uint32_t &restEval, ResultSize limit) {
	sc = BtreeMap::SearchContext();
	sc.conditionList_ =
		reinterpret_cast<TermCondition *>(txn.getDefaultAllocator().allocate(
			sizeof(TermCondition) * andList.size()));
	sc.isStartKeyIncluded_ = true;
	sc.isEndKeyIncluded_ = true;
	restEval = 0;

	bool conditionDealed;
	uint32_t cid = (indexColumnInfo == NULL) ? UNDEF_COLUMNID
											 : indexColumnInfo->getColumnId();
	sc.columnId_ = cid;

	for (size_t i = 0; i < andList.size(); i++) {
		TermCondition *c = NULL;
		conditionDealed = andList[i]->getCondition(txn, MAP_TYPE_BTREE, cid,
			queryObj, c, sc.startKey_, sc.startKeySize_, sc.isStartKeyIncluded_,
			sc.endKey_, sc.endKeySize_, sc.isEndKeyIncluded_);
		if (c) {
			sc.conditionList_[sc.conditionNum_++] = *c;
		}
		if (conditionDealed) {
			andList[i]->enableEvaluationFilter();
		}
		else {
			restEval++;
		}
	}
	sc.limit_ = (0 == restEval) ? limit : MAX_RESULT_SIZE;
}

/*!
 * @brief transform and expressions into HashMap's SearchContext
 *
 */
void BoolExpr::toSearchContext(TransactionContext &txn,
	util::XArray<BoolExpr *> &andList, ColumnInfo *indexColumnInfo,
	QueryForCollection &queryObj, HashMap::SearchContext &sc,
	uint32_t &restEval, ResultSize limit) {
	uint32_t cid = (indexColumnInfo == NULL) ? UNDEF_COLUMNID
											 : indexColumnInfo->getColumnId();
	sc = HashMap::SearchContext();
	sc.conditionList_ =
		reinterpret_cast<TermCondition *>(txn.getDefaultAllocator().allocate(
			sizeof(TermCondition) * andList.size()));
	sc.columnId_ = cid;
	restEval = 0;

	const void *dummyKey = NULL;
	uint32_t dummySize;
	int32_t dummyBool;
	bool conditionDealed = false;

	for (size_t i = 0; i < andList.size(); i++) {
		TermCondition *c = NULL;
		conditionDealed = andList[i]->getCondition(txn, MAP_TYPE_HASH,
			sc.columnId_, queryObj, c, sc.key_, sc.keySize_, dummyBool,
			dummyKey, dummySize, dummyBool);
		if (c) {
			sc.conditionList_[sc.conditionNum_++] = *c;
		}
		if (conditionDealed) {
			andList[i]->enableEvaluationFilter();
		}
		else {
			restEval++;
		}
	}
	sc.limit_ = (0 == restEval) ? limit : MAX_RESULT_SIZE;
}


/*!
 * @brief transform and expressions into BtreeMap's SearchContext
 *
 */
void BoolExpr::toSearchContext(TransactionContext &txn,
	util::XArray<BoolExpr *> &andList, Timestamp &expireTs,
	ColumnInfo *indexColumnInfo, QueryForTimeSeries &queryObj,
	BtreeMap::SearchContext &sc, uint32_t &restEval, ResultSize limit) {
	uint32_t cid = (indexColumnInfo == NULL) ? UNDEF_COLUMNID
											 : indexColumnInfo->getColumnId();
	sc = BtreeMap::SearchContext();
	sc.conditionList_ =
		reinterpret_cast<TermCondition *>(txn.getDefaultAllocator().allocate(
			sizeof(TermCondition) *
			(andList.size() + 1)));  
	sc.columnId_ = cid;
	if (sc.columnId_ == UNDEF_COLUMNID && indexColumnInfo == NULL) {
		sc.columnId_ = 0;  
	}
	sc.isStartKeyIncluded_ = true;
	sc.isEndKeyIncluded_ = true;
	restEval = 0;

	bool conditionDealed = false;

	for (size_t i = 0; i < andList.size(); i++) {
		TermCondition *c = NULL;
		conditionDealed = andList[i]->getCondition(txn, MAP_TYPE_BTREE,
			sc.columnId_, queryObj, c, sc.startKey_, sc.startKeySize_,
			sc.isStartKeyIncluded_, sc.endKey_, sc.endKeySize_,
			sc.isEndKeyIncluded_);
		if (c) {
			sc.conditionList_[sc.conditionNum_++] = *c;
		}
		if (conditionDealed) {
			andList[i]->enableEvaluationFilter();
		}
		else {
			restEval++;
		}
	}
	if (sc.columnId_ != UNDEF_COLUMNID && indexColumnInfo != NULL &&
		indexColumnInfo->isKey() && sc.startKey_ != NULL) {
		if (compareTimestampTimestamp(txn,
				reinterpret_cast<uint8_t *>(&expireTs), 0,
				reinterpret_cast<const uint8_t *>(sc.startKey_), 0) > 0) {
			sc.startKey_ = &expireTs;
			sc.isStartKeyIncluded_ = true;  
		}
	}
	else {
		TermCondition c;

		if (sc.columnId_ == 0) {  
			if (sc.startKey_ != NULL && expireTs != MINIMUM_EXPIRED_TIMESTAMP) {
				Timestamp t1 =
					*reinterpret_cast<const Timestamp *>(sc.startKey_);
				if (t1 < expireTs) {
					sc.startKey_ = &expireTs;
				}
			}
			else if (sc.startKey_ == NULL) {
				if (expireTs == MINIMUM_EXPIRED_TIMESTAMP) {
					expireTs = 0;
					sc.startKey_ = &expireTs;
				}
				else {
					sc.startKey_ = &expireTs;
				}
			}
		}
		else {
			memset(&c, 0, sizeof(TermCondition));
			c.columnOffset_ = 0;
			c.columnId_ = 0;
			c.operator_ = gtTable[COLUMN_TYPE_TIMESTAMP][COLUMN_TYPE_TIMESTAMP];
			c.value_ = reinterpret_cast<uint8_t *>(&expireTs);
			sc.conditionList_[sc.conditionNum_++] = c;
		}
	}
	sc.limit_ = (0 == restEval) ? limit : MAX_RESULT_SIZE;
}

/*!
 * @brief get usable indices
 */
void BoolExpr::getIndexBitmapAndInfo(TransactionContext &txn,
	BaseContainer &baseContainer, Query &queryObj, uint32_t &mapBitmap,
	ColumnInfo *&indexColumnInfo) {
	Expr *unary;
	Expr::Operation op = Expr::NONE;
	if (this->type_ == UNARY) {
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
					1, "INDEX_DISABLE", "INDEX_TYPE", "HASH", str.c_str());
			}
		}
	}
	else if (this->type_ == NOT) {
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
 * @brief Create new expression to optimized expresion.
 *
 * @param txn The transaction context
 * @param txn Object manager
 * @param column_values Binding environment
 * @param function_map Function list
 * @param mode EvalMode
 *
 * @return Evaluation result as expression.
 */
Expr *BoolExpr::eval(TransactionContext &txn, ObjectManager &objectManager,
	ContainerRowWrapper *column_values, FunctionMap *function_map,
	EvalMode mode) {
	bool b;
	switch (mode) {
	case EVAL_MODE_NORMAL: {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		b = this->eval(txn, objectManager, column_values, function_map, true);
	}
		return Expr::newBooleanValue(b, txn);
	case EVAL_MODE_PRINT: {
		util::NormalOStringStream os;
		dumpTreeSub(txn, objectManager, os, 0, "", column_values, function_map);
		return Expr::newStringValue(os.str().c_str(), txn);
	}
	case EVAL_MODE_CONTRACT:
		return makeOptimizedExpr(
			txn, objectManager, column_values, function_map);
	}

	GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
		"Internal logic error: Invalid boolean evaluation mode.");
}
