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
	@brief Definition of Query
*/

#ifndef QUERY_H_
#define QUERY_H_

#include "util/type.h"
#include "util/allocator.h"
#include "util/container.h"
#include "boolean_expression.h"
#include "data_type.h"
#include "expression.h"
#include "gis_geometry.h"
#include "gis_linestring.h"
#include "gis_pointgeom.h"
#include "gis_polygon.h"
#include "gis_polyhedralsurface.h"
#include "gis_surface.h"
#include "schema.h"
#include <cassert>
#include <iostream>
#include <iterator>
#include <list>
#include <stdexcept>
#include <string.h>
#include <string>

#define INITIAL_EXPLAIN_DATA_NUM 0x100

class ResultSet;
class MetaContainer;

namespace lemon_tqlParser {
class tqlParser;
};

/*!
 * @brief Message buffer for Explain plan
 *
 */
struct ExplainData {
	uint32_t id;
	uint32_t depth;
	util::String *exp_type;
	util::String *value_type;
	util::String *value_string;
	util::String *statement;

	ExplainData *dup(TransactionContext &txn) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		ExplainData *dest = QP_ALLOC_NEW(alloc) ExplainData;
		dest->depth = depth;
		dest->id = id;
		dest->exp_type = NULL;
		dest->value_type = NULL;
		dest->value_string = NULL;
		dest->statement = NULL;

		if (exp_type != NULL) {
			dest->exp_type = QP_NEW util::String(alloc);
			dest->exp_type = exp_type;
		}
		if (value_type != NULL) {
			dest->value_type = QP_NEW util::String(alloc);
			dest->value_type = value_type;
		}
		if (value_string != NULL) {
			dest->value_string = QP_NEW util::String(alloc);
			dest->value_string = value_string;
		}
		if (statement != NULL) {
			dest->statement = QP_NEW util::String(alloc);
			dest->statement = statement;
		}
		return dest;
	}
};

/*!
 * @brief Interface for hooking query execution
 */
class QueryHookClass {
public:
	virtual void qpBuildBeginHook(Query &queryObj) = 0;
	virtual void qpBuildFinishHook(Query &queryObj) = 0;
	virtual void qpSearchBeginHook(Query &queryObj) = 0;
	virtual void qpSearchFinishHook(Query &queryObj) = 0;
	virtual void qpSelectFinishHook(Query &queryObj) = 0;
	virtual void qpExecuteFinishHook(Query &queryObj) = 0;

	virtual void qpBuildTmpHook(Query &queryObj, int i) = 0;
};

class BaseContainer;
/*!
 * @brief Query object
 *
 */
class Query {
	friend class lemon_tqlParser::tqlParser;
	friend class QueryStopwatchHook;
	friend class QueryProcessor;
	friend class Expr;
	friend class BoolExpr;

public:
	struct QueryAccessor;
	struct ExprAccessor;
	struct BoolExprAccessor;

	Query(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		const TQLInfo &tqlInfo, uint64_t limit = MAX_RESULT_SIZE,
		QueryHookClass *hook = NULL);
	virtual ~Query();

	void dumpSelectionExpr(TransactionContext &txn, std::ostream &os,
		ContainerRowWrapper *x = NULL);
	void dumpConditionExpr(
		TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x);
	void dumpConditionExprDNF(
		TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x);
	void dumpConditionExprOptimized(
		TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x);
	void dumpSelectOptions(TransactionContext &txn, std::ostream &os);

	/*!
	* @brief Represents the status of parse
	*/
	enum ParseState {
		PARSESTATE_SELECTION,
		PARSESTATE_CONDITION,
		PARSESTATE_END
	};

protected:
	virtual Collection *getCollection() {
		return NULL;
	}
	virtual TimeSeries *getTimeSeries() {
		return NULL;
	}

	MetaContainer* getMetaContainer() {
		return getMetaContainer(getCollection(), getTimeSeries());
	}

	static MetaContainer* getMetaContainer(
			Collection *collection, TimeSeries *timeSeries) {
		if (collection != NULL && timeSeries != NULL) {
			return getMetaContainer(*collection, *timeSeries);
		}
		return NULL;
	}

	static MetaContainer* getMetaContainer(
			Collection &collection, TimeSeries &timeSeries);

	virtual uint64_t getLimit() {
		return nActualLimit_;
	}

	void setError(const char *errMsg);
	void setSelectionExpr(ExprList *pExprList);
	void setConditionExpr(Expr *e, TransactionContext &txn);
	void setFromCollection(Token &name1, Token &name2, TransactionContext &txn);
	void setTraceMode(bool t);
	void setPragma(Token *pName1, Token *pName2, Token *pValue, int minusflag);
	void setFunctionMap(FunctionMap *fmap = NULL, AggregationMap *aggmap = NULL,
		SelectionMap *selmap = NULL, SpecialIdMap *idmap = NULL) {
		functionMap_ = fmap;
		aggregationMap_ = aggmap;
		selectionMap_ = selmap;
		specialIdMap_ = idmap;
	}
	void setDefaultFunctionMap();

	void evalConditionExpr(ContainerRowWrapper *x);

	TransactionContext &getTransactionContext() {
		return txn_;
	}

	Query(TransactionContext &txn, ObjectManagerV4&objectManager, AllocateStrategy &strategy)
		: tqlInfo_(TQLInfo()), txn_(txn), objectManager_(objectManager), strategy_(strategy) {
		isSetError_ = false;
		isExplainExecute_ = true;
		explainAllocNum_ = 0;
		explainNum_ = 0;
		setDefaultFunctionMap();
		parseState_ = PARSESTATE_SELECTION;
		explainData_ = NULL;
		hook_ = NULL;
		pSelectionExprList_ = NULL;
		pWhereExpr_ = NULL;
		pErrorMsg_ = NULL;
	}

	bool getIndexDataInAndList(TransactionContext &txn,
		BaseContainer &container, util::XArray<BoolExpr *> &andList,
		IndexData &indexData);
	BoolExpr *getConditionExpr() const;
	Expr *getSelectionExpr(size_t x) const;
	size_t getSelectionExprLength() const;
	void getConditionAsOrList(util::XArray<BoolExpr *> &orList);
	FunctionMap *getFunctionMap() {
		return functionMap_;
	}
	AggregationMap *getAggregationMap() {
		return aggregationMap_;
	}
	SelectionMap *getSelectionMap() {
		return selectionMap_;
	}
	SpecialIdMap *getSpecialIdMap() {
		return specialIdMap_;
	}
	void enableExplain(bool doExecute);
	bool doExecute() {
		return isExplainExecute_;
	}
	bool doExplain() {
		return (explainAllocNum_ > 0);
	}
	const char *getFromName() {
		if (pFromCollectionName_) {
			return pFromCollectionName_;
		}
		else {
			return NULL;
		}
	}
	virtual Query *dup(TransactionContext &, ObjectManagerV4 &, AllocateStrategy &) {
		return NULL;
	}
	virtual void finishQuery(TransactionContext &txn, ResultSet &resultSet,
		BaseContainer &container);

	void setQueryOption(TransactionContext &txn, ResultSet &resultSet);
	void doQueryPartial(
		TransactionContext &txn, BaseContainer &container, ResultSet &resultSet);
protected:
	void contractCondition();
	void addExplain(uint32_t depth, const char *exp_type,
		const char *value_type, const char *value_string,
		const char *statement);
	void serializeExplainData(TransactionContext &txn, uint64_t &resultNum,
		MessageRowStore *messageRowStore);

	/*!
	 * Called when DISTINCT keyword is used.
	 */
	void setDistinct() {
		isDistinct_ = true;
	}

	/*!
	 * Called when LIMIT keyword is used.
	 */
	virtual void setLimitExpr(Expr *e1 = NULL, Expr *e2 = NULL) {
		QP_SAFE_DELETE(pLimitExpr_);

		pLimitExpr_ = e1;

		uint64_t limit = MAX_RESULT_SIZE;
		if (pLimitExpr_ != NULL) {
			int64_t tmp = pLimitExpr_->eval(txn_, objectManager_, strategy_, NULL,
							functionMap_, EVAL_MODE_NORMAL)
							->getValueAsInt64();
			if (tmp < 0) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_SYNTAX_ERROR_EXECUTION, "limit(" << tmp << ") is negative");
			}
			limit = static_cast<uint64_t>(tmp);
		}
		
		if (limit < nLimit_) {
			nLimit_ = limit;
		}
		nActualLimit_ = nLimit_;

		if (nLimit_ != MAX_RESULT_SIZE &&
			(getSelectionExpr(0)->isSelection() ||
				getSelectionExpr(0)->isAggregation() ||
				pOrderByExpr_ != NULL)) {
			nActualLimit_ = nLimit_;
			nLimit_ = MAX_RESULT_SIZE;
		}
		nOffset_ = 0;
		if (e2 != NULL) {
			int64_t tmp = e2->eval(txn_, objectManager_, strategy_, NULL,
							functionMap_, EVAL_MODE_NORMAL)
							->getValueAsInt64();
			if (tmp < 0) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_SYNTAX_ERROR_EXECUTION, "offset(" << tmp << ") is negative");
			}
			nOffset_ = static_cast<uint64_t>(tmp);
			QP_SAFE_DELETE(e2);
		}
		if (nLimit_ != MAX_RESULT_SIZE) {
			nLimit_ += nOffset_;
		}
	}

	/*!
	 * Called when LIMIT keyword is used and sorted by value index.
	 */
	virtual void setLimitByIndexSort() {
		if (nLimit_ == MAX_RESULT_SIZE && nActualLimit_ != MAX_RESULT_SIZE &&
			(!getSelectionExpr(0)->isSelection() &&
				!getSelectionExpr(0)->isAggregation())) {
			nLimit_ = nActualLimit_ + nOffset_;
		}
	}

	/*!
	 * Called when GROUPBY keyword is used.
	 */
	void setGroupByExpr(ExprList *e) {
		QP_SAFE_DELETE(pGroupByExpr_);
		pGroupByExpr_ = e;
	}

	/*!
	 * Called when HAVING keyword is used.
	 */
	void setHavingExpr(Expr *e) {
		QP_SAFE_DELETE(pHavingExpr_);
		pHavingExpr_ = e;
	}

	/*!
	 * Called when ORDER BY (ASC, DESC) keyword is used.
	 */
	void setSortList(SortExprList *e) {
		pOrderByExpr_ = e;
		if (e != NULL) {
			for (size_t i = 0; i < e->size(); i++) {
				if (!(*e)[i].expr->isColumn()) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_SYNTAX_ERROR_INVALID_ORDERBY_EXPR,
						"Only column name is allowed in order-by clause");
				}
			}
		}
		if (e != NULL && hasAggregationClause()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_SYNTAX_ERROR_ORDERBY_WITH_AGGREGATION,
				"Cannot use order-by clause with aggregation");
		}
	}

	/*!
	 * @brief Return the type of the expression
	 * @return The expression is an aggregation function
	 */
	bool hasAggregationClause() {
		Expr *eSel = getSelectionExpr(0);
		if (eSel != NULL && eSel->isAggregation()) {
			return true;
		} else {
			return false;
		}
	}

	bool isSortBeforeSelectClause() {
		Expr *pSel = getSelectionExpr(0);
		if (pOrderByExpr_ && pSel && !pSel->isSelection()) {
			return true;
		} else {
			return false;
		}
	}

	bool isIndexSortAvailable(BaseContainer &container, size_t orListNum) {
		if (isSortBeforeSelectClause() && orListNum <= 1 && 
			pOrderByExpr_) {
			uint32_t orderColumnId = (*pOrderByExpr_)[0].expr->getColumnId();
			ColumnInfo columnInfo = container.getColumnInfo(orderColumnId);
			if (pOrderByExpr_->size() == 1 || columnInfo.isKey()) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
/*
	bool isIndexSortAvailable(size_t orListNum, ColumnId &indexColumnId, OutputOrder &order) {
		bool isAvailable = false;
		indexColumnId = UNDEF_COLUMNID;
		if (orListNum <= 1 && pOrderByExpr_ && pOrderByExpr_->size() == 1) {
			SortExpr &orderExpr = (*pOrderByExpr_)[0];
			uint32_t orderColumnId = (orderExpr.expr)
										 ? orderExpr.expr->getColumnId()
										 : UNDEF_COLUMNID;
			if ((orderColumnId == ColumnInfo::ROW_KEY_COLUMN_ID && 
				container.getContainerType() == TIME_SERIES_CONTAINER) ||
				(orderColumnId != UNDEF_COLUMNID && 
					container.hasIndex(txn, orderColumnId, MAP_TYPE_BTREE)) {
				outputOrder = (orderExpr.order == ASC)
								  ? ORDER_ASCENDING
								  : ORDER_DESCENDING;
				indexColumnId = orderColumnId;
				isAvailable = true;
			}
		}
		return isAvailable;
	}
	*/

	ColumnInfo *makeExplainColumnInfo(TransactionContext &txn) {
		ColumnInfo *explainColumnInfoList =
			reinterpret_cast<ColumnInfo *>(txn.getDefaultAllocator().allocate(
				sizeof(ColumnInfo) * EXPLAIN_COLUMN_NUM));
		uint16_t variableColumnIndex = 0;  
		uint32_t rowFixedSize = 0;
		ColumnType explainColumnTypeList[EXPLAIN_COLUMN_NUM] = {COLUMN_TYPE_INT,
			COLUMN_TYPE_INT, COLUMN_TYPE_STRING, COLUMN_TYPE_STRING,
			COLUMN_TYPE_STRING, COLUMN_TYPE_STRING};
		uint64_t nullsAndVarOffset =
			ValueProcessor::calcNullsByteSize(EXPLAIN_COLUMN_NUM) +
			sizeof(OId);  
		for (uint32_t i = 0; i < EXPLAIN_COLUMN_NUM; i++) {
			explainColumnInfoList[i].initialize();
			explainColumnInfoList[i].setType(explainColumnTypeList[i]);
			if (explainColumnInfoList[i].isVariable()) {  
				explainColumnInfoList[i].setOffset(variableColumnIndex);
				++variableColumnIndex;
			}
			else {
				explainColumnInfoList[i].setOffset(
					static_cast<uint16_t>(nullsAndVarOffset + rowFixedSize));
				rowFixedSize += explainColumnInfoList[i].getColumnSize();
			}
		}
		return explainColumnInfoList;
	}

	template <class T>
	void mergeRowIdList(TransactionContext &txn,
		util::XArray<T> &resultRowIdList, util::XArray<T> &andRowIdArray) {
		size_t currentResultSize = resultRowIdList.size();
		size_t newResultSize = andRowIdArray.size();
		const size_t sortThreshold = 100;

		if (currentResultSize + newResultSize > sortThreshold) {
			util::XArray<T> tmp(txn.getDefaultAllocator());

			switch (nResultSorted_) {
			case 0:
				resultRowIdList.swap(andRowIdArray);
				nResultSorted_++;
				break;
			case 1:
				std::sort(resultRowIdList.begin(),
					resultRowIdList.end());  
				std::sort(andRowIdArray.begin(),
					andRowIdArray.end());  
				std::set_union(resultRowIdList.begin(), resultRowIdList.end(),
					andRowIdArray.begin(), andRowIdArray.end(),
					std::back_inserter(tmp));
				resultRowIdList.clear();
				resultRowIdList.swap(tmp);
				nResultSorted_++;
				break;
			default:
				std::sort(andRowIdArray.begin(),
					andRowIdArray.end());  
				std::set_union(resultRowIdList.begin(), resultRowIdList.end(),
					andRowIdArray.begin(), andRowIdArray.end(),
					std::back_inserter(tmp));
				resultRowIdList.clear();
				resultRowIdList.swap(tmp);
				break;
			}
		}
		else {
			for (uint32_t i = 0; i < andRowIdArray.size(); i++) {
				bool includeFlag = false;  
				for (size_t k = 0; k < currentResultSize; k++) {
					if (memcmp(&andRowIdArray[i], &resultRowIdList[k],
							sizeof(T)) == 0) {
						includeFlag = true;
						break;
					}
				}
				if (!includeFlag) {  
					resultRowIdList.push_back(andRowIdArray[i]);
				}
				nResultSorted_ =
					1;  
			}
		}
	}

	const TQLInfo tqlInfo_;
	util::String
		*pErrorMsg_;  
	bool isSetError_;  
	ExprList *
		pSelectionExprList_;  
	BoolExpr
		*pWhereExpr_;  
	const char *
		pFromCollectionName_;  
	TransactionContext &txn_;  
	ObjectManagerV4
		&objectManager_;  
	AllocateStrategy &strategy_;
	ParseState parseState_;  
	static bool sIsTrace_;
	int nResultSorted_;  

	ExplainData *
		explainData_;  
	uint32_t explainNum_;  
	uint32_t explainAllocNum_;  
	bool
		isExplainExecute_;  
	static const uint32_t EXPLAIN_COLUMN_NUM = 6;
	QueryHookClass
		*hook_;  

	bool isDistinct_;  
	ResultSize nActualLimit_, nLimit_,
		nOffset_;  
	Expr *pLimitExpr_;  
	ExprList *pGroupByExpr_;  
	Expr *pHavingExpr_;		  
	SortExprList
		*pOrderByExpr_;  

	FunctionMap *functionMap_;  
	AggregationMap *aggregationMap_;  
	SelectionMap *selectionMap_;	  
	SpecialIdMap *specialIdMap_;	  
};

struct Query::QueryAccessor {
	static const BoolExpr* findWhereExpr(const Query &query);
};

struct Query::ExprAccessor : public Expr {
	static Type getType(const Expr &expr);
	static Operation getOp(const Expr &expr);
	static const ExprList* getArgList(const Expr &expr);
	static const Value* getValue(const Expr &expr);
	static uint32_t getColumnId(const Expr &expr);
	static ColumnType getExprColumnType(const Expr &expr);
};

struct Query::BoolExprAccessor : public BoolExpr {
	static const BoolExpr::BoolTerms& getOperands(const BoolExpr &expr);
	static const Expr* getUnary(const BoolExpr &expr);
	static Operation getOpType(const BoolExpr &expr);
};

/*!
 * @brief Callback member function by template.
 * Use like "cast_to_callback<Query, bool, &Query::ComparatorFunc>"
 *
 * @return
 */
template <typename T, typename RET_T, RET_T (T::*FUNC)()>
RET_T cast_to_callback(void *pThis) {
	T *obj = reinterpret_cast<T *>(pThis);
	return (obj->*FUNC)();
}

/*!
	@brief QueryHook for measure time
*/
class QueryStopwatchHook : public QueryHookClass {
public:
	void qpBuildBeginHook(Query &) {
		watch.reset();
		takeTimes = 0;
		memset(takeTime, 0, sizeof(double) * 100);
		watch.start();
	}
	void qpBuildFinishHook(Query &) {
		watch.stop();
		takeTime[0] = static_cast<double>(watch.elapsedNanos()) / 1000000.0;
		watch.reset();
		watch.start();
	}

	void qpBuildTmpHook(Query &, int i) {
		watch.stop();
		takeTime[i] = static_cast<double>(watch.elapsedNanos()) / 1000000.0;
		watch.start();
	}

	void qpSearchBeginHook(Query &) {}

	void qpSearchFinishHook(Query &) {
		watch.stop();
		takeTime[1] = static_cast<double>(watch.elapsedNanos()) / 1000000.0;
		watch.reset();
		watch.start();
	}

	void qpSelectFinishHook(Query &) {
		watch.stop();
		takeTime[2] = static_cast<double>(watch.elapsedNanos()) / 1000000.0;
	}

	void qpExecuteFinishHook(Query &queryObj) {
		util::NamedFile file;
		file.open("query_bench.dat", util::FileFlag::TYPE_APPEND);
		util::NormalOStringStream oss;

		for (int i = 0; i < 10; i++) {
			oss << std::fixed << std::setprecision(3) << takeTime[i] << ",";
		}
		if (strlen(queryObj.tqlInfo_.query_) < 200) {
			oss << "\"" << queryObj.tqlInfo_.query_ << "\"";
		}
		oss << std::endl;
		const std::string data = oss.str();
		file.write(data.c_str(), data.size());
		file.close();
	}

private:
	util::Stopwatch watch;
	double takeTime[100];
	int takeTimes;
};

#include "query_collection.h"
#include "query_timeseries.h"

#endif /* QUERY_H_ */
