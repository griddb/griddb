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
	@brief Definition of BoolExpr
*/

#ifndef BOOLEAN_EXPRESSION_H_
#define BOOLEAN_EXPRESSION_H_

#include "util/container.h"
#include "btree_map.h"
#include "expression.h"
#include "qp_def.h"
#include "rtree_map.h"

class Expr;
class QueryForCollection;
class QueryForTimeSeries;

/*!
 * @brief  Boolean expression (treated as special case.
 *         since we need to expand one expression into DNF
 *         (Disjunctive Normal Form.)
 */
class BoolExpr : public Expr {
public:
	typedef QP_XArray<BoolExpr *> BoolTerms;

	/*!
	 * @brief  Operation type
	 */
	enum Operation { AND, OR, NOT, UNARY };

	BoolExpr(Operation t, BoolExpr *e1, BoolExpr *e2, TransactionContext &txn);
	BoolExpr(Expr *e, TransactionContext &txn);
	BoolExpr(Operation t, BoolTerms &terms, TransactionContext &txn);
	BoolExpr(Operation t, TransactionContext &txn, int size, ...);
	BoolExpr(bool b, TransactionContext &txn);
	BoolExpr(TrivalentLogicType b, TransactionContext &txn);

	virtual ~BoolExpr();

	TrivalentLogicType eval(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		ContainerRowWrapper *column_values, FunctionMap *function_map,
		TrivalentLogicType default_return);

	/*!
	 * @brief Evaluate the expression as a boolean value
	 */
	operator bool() const {
		if (*this == true) {
			return true;
		}
		return false;
	}

	bool operator==(const BoolExpr &e) const {
		if (opeType_ != e.opeType_) return false;
		if (opeType_ == UNARY) {
			return *unary_ == *e.unary_;
		}
		if (e.operands_.size() != operands_.size()) {
			return false;
		}
		for (size_t i = 0; i < operands_.size(); i++) {
			if (*e.operands_[i] != *operands_[i]) {
				return false;
			}
		}
		return true;
	}

	bool operator!=(const BoolExpr &e) const {
		return !(*this == e);
	}

	Expr *eval(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		ContainerRowWrapper *column_values, FunctionMap *function_map,
		EvalMode mode);
	BoolExpr *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);

	BoolExpr *makeDNF(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);
	BoolExpr *makeOptimizedExpr(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ContainerRowWrapper *column_values,
		FunctionMap *function_map);

	void toOrList(util::XArray<BoolExpr *> &orList);
	void toAndList(util::XArray<BoolExpr *> &andList);

	static void toSearchContext(TransactionContext &txn,
		util::XArray<BoolExpr *> &andList, IndexData *indexData,
		QueryForCollection &queryObj, BtreeMap::SearchContext *&sc,
		uint32_t &restEval, ResultSize limit = MAX_RESULT_SIZE);
	static void toSearchContext(TransactionContext &txn,
		util::XArray<BoolExpr *> &andList, IndexData *indexData,
		QueryForCollection &queryObj, RtreeMap::SearchContext *&sc,
		uint32_t &restEval, ResultSize limit = MAX_RESULT_SIZE);

	static void toSearchContext(TransactionContext &txn,
		util::XArray<BoolExpr *> &andList,
		IndexData *indexData, QueryForTimeSeries &queryObj,
		BtreeMap::SearchContext *&sc, uint32_t &restEval,
		ResultSize limit = MAX_RESULT_SIZE);

	/*!
	 * @brief Set evaluation ignorance flag
	 */
	void enableEvaluationFilter() {
		noEval_ = true;
	}
	/*!
	 * @brief Clear evaluation ignorance flag
	 */
	void disableEvaluationFilter() {
		noEval_ = false;
	}

	bool getCondition(
			TransactionContext &txn, MapType type, Query &queryObj,
			TermCondition *&cond, bool &semiFiltering);

	void getIndexBitmapAndInfo(TransactionContext &txn,
		BaseContainer &baseContainer, Query &queryObj, uint32_t &mapBitmap,
		ColumnInfo *&indexColumnInfo);

	void dumpTree(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		std::ostream &os);
	void dumpTree(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		std::ostream &os, ContainerRowWrapper *column_values,
		FunctionMap *function_map);

	/*!
	 * @brief Checks operation type
	 * @param t operation type
	 * @return test result
	 */
	bool checkOperationType(Operation t) {
		return opeType_ == t;
	}
	/*!
	 * @brief Checks operand num
	 * @param x numbers of operands
	 * @return test result
	 */
	bool checkOperandCount(unsigned int x) {
		return operands_.size() == x;
	}
	void getMappingColumnInfo(
		TransactionContext &txn, std::vector<util::String> &x) {
		if (opeType_ == UNARY) {
			unary_->getMappingColumnInfo(txn, x);
		}
		else {
			BoolTerms::iterator it;
			for (it = operands_.begin(); it != operands_.end(); it++) {
				(*it)->getMappingColumnInfo(txn, x);
			}
		}
	}

	Expr *getUnary() {
		return unary_;
	}

protected:
	BoolExpr(TransactionContext &txn)
		: Expr(txn), operands_(txn.getDefaultAllocator()) {
		type_ = BOOL_EXPR;
	}

	/*!
	 * @brief Check for that all expression in the term list are unary or NOT
	 *
	 * @param v Test terms
	 *
	 * @return Test result
	 */
	bool isAllUnaryOrNotExpr(BoolTerms &v) {
		return !(isOneHasTheType(AND, v) | isOneHasTheType(OR, v));
	}

	/*!
	 * @brief Check for that all expression in the term list are the passed type
	 *
	 * @param t Test type
	 * @param v Test terms
	 *
	 * @return Test result
	 */
	bool isAllHaveTheType(Operation t, BoolTerms &v) {
		return !isOneNotHasTheType(t, v);
	}
	/*!
	 * @brief Check for that no expression in the term list is the passed type
	 *
	 * @param t Test type
	 * @param v Test terms
	 *
	 * @return Test result
	 */
	bool isNooneHaveTheType(Operation t, BoolTerms &v) {
		return !isOneHasTheType(t, v);
	}

	/*!
	 * @brief Check that at least one term in the vector have the same type
	 *
	 * @param t Target type
	 * @param v Vector to check
	 *
	 * @return Condition
	 */
	bool isOneHasTheType(Operation t, BoolTerms &v) {
		for (BoolTerms::const_iterator it = v.begin(); it != v.end(); it++) {
			if ((*it)->opeType_ == t) {
				return true;
			}
		}
		return false;
	}

	/*!
	 * @brief Check that at least one term in the vector do 'NOT' have the same
	 * type
	 *
	 * @param t Target type
	 * @param v Vector to check
	 *
	 * @return Condition
	 */
	bool isOneNotHasTheType(Operation t, BoolTerms &v) {
		for (BoolTerms::const_iterator it = v.begin(); it != v.end(); it++) {
			if ((*it)->opeType_ != t) {
				return true;
			}
		}
		return false;
	}

	void dumpTreeSub(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		std::ostream &os, int level, const char *head,
		ContainerRowWrapper *column_values, FunctionMap *function_map);

	/*!
	 * @brief Add terms to operands of this expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param t terms to add
	 */
	void addOperands(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, BoolTerms &t) {
		if (opeType_ == UNARY) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: cannot add operands in unary "
				"expression");
		}
		BoolTerms::iterator it;
		for (it = t.begin(); it != t.end(); it++) {
			operands_.push_back((*it)->dup(txn, objectManager, strategy));
		}
	}

	/*!
	 * @brief Add every DNF form of the terms to operands of this expression
	 *
	 * @param txn The transaction context
	 * @param t terms to add
	 */
	void addOperandsDNF(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, BoolTerms &t) {
		if (opeType_ == UNARY) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: cannot add operands in unary "
				"expression");
		}
		BoolTerms::iterator it;
		for (it = t.begin(); it != t.end(); it++) {
			operands_.push_back((*it)->makeDNF(txn, objectManager, strategy));
		}
	}

	/*!
	 * @brief Add expression to the operands of this expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param t expression to add
	 */
	void addOperand(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, BoolExpr *t) {
		operands_.push_back(t->dup(txn, objectManager, strategy));
	}

	/*!
	 * @brief Check the expression is a value or a column
	 * @return test result
	 */
	virtual bool isValueOrColumn() {
		return opeType_ == UNARY && (unary_->isColumn() || unary_->isValue());
	}
	/*!
	 * @brief get the column type
	 * @return column type
	 */
	virtual ColumnType getColumnType() {
		return COLUMN_TYPE_BOOL;
	}

	/*!
	 * @brief Check the expression is DNF form. (for internal use)
	 */
	inline void checkDNF() {
#ifndef NDEBUG
		BoolTerms::const_iterator it;
		switch (opeType_) {
		case UNARY:
			return;
		case AND:
			for (it = operands_.begin(); it != operands_.end(); it++) {
				if ((*it)->opeType_ != UNARY &&
					!((*it)->opeType_ == NOT &&
						(*it)->operands_[0]->opeType_ == UNARY)) {
					GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
						"Internal logic error: checkDNF failed");
				}
			}
			return;
		case OR:
			for (it = operands_.begin(); it != operands_.end(); it++) {
				switch ((*it)->opeType_) {
				case UNARY:
					break;
				case NOT:
				case AND:
					(*it)->checkDNF();
					break;
				case OR:
					GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
						"Internal logic error: checkDNF failed");
					break;
				}
			}
			break;
		case NOT:
			if (operands_[0]->opeType_ != UNARY) {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: checkDNF failed");
			}
			break;
		}
#endif
	}

private:
	bool operator==(const Expr &e) const {
		return Expr::operator==(e);
	}

	bool operator!=(const Expr &e) const {
		return Expr::operator!=(e);
	}

protected:
	BoolTerms operands_;
	Expr *unary_;
	Operation opeType_;
	bool noEval_;  
};

#endif
