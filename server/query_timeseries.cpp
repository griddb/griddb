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
	@brief Implementation of QueryForTimeSeries
*/

#include "query.h"
#include "collection.h"
#include "qp_def.h"
#include "time_series.h"
#include "transaction_context.h"

#include "lexer.h"
#include "schema.h"
#include "tql.h"
#include "value_processor.h"

#include "boolean_expression.h"
#include "result_set.h"

/*!
* @brief Query constructor
*
* @param txn Transaction context
* @param timeSeries timeSeries, to decide index usage
* @param statement TQL statement
* @param limit Limit of the size of result rowset
* @param hook hook for query
*
*/
QueryForTimeSeries::QueryForTimeSeries(TransactionContext &txn,
	TimeSeries &timeSeries, const TQLInfo &tqlInfo, uint64_t limit,
	QueryHookClass *hook)
	: Query(txn, *(timeSeries.getObjectManager()), tqlInfo, limit, hook),
	timeSeries_(&timeSeries), scPass_(txn.getDefaultAllocator(), ColumnInfo::ROW_KEY_COLUMN_ID) {
	if (hook_) {
		hook_->qpBuildBeginHook(*this);	
	}
	isExplainExecute_ = true;
	explainAllocNum_ = 0;
	explainNum_ = 0;
	isSetError_ = false;
	parseState_ = PARSESTATE_SELECTION;
	apiOutputOrder_ = ORDER_ASCENDING;
	setDefaultFunctionMap();

	if (hook_) {
		hook_->qpBuildTmpHook(*this, 3);	
	}

	lemon_tqlParser::tqlParser *x = QP_NEW lemon_tqlParser::tqlParser();
	if (x == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CM_NO_MEMORY, "Cannot allocate TQL parser");
	}
	Token t;
	int ret;
	const char *c_str = tqlInfo.query_;
#ifndef NDEBUG
	if (sIsTrace_) {
		x->tqlParserSetTrace(&std::cerr, "trace: ");
	}
	else {
		x->tqlParserSetTrace(NULL, "");
	}
#endif

	do {
		ret = Lexer::gsGetToken(c_str, t);
		if (ret <= 0) break;
		if (t.id == TK_ILLEGAL) {
			isSetError_ = true;
			break;
		}
		if (t.id != TK_SPACE) {
			x->Execute(t.id, t, this);
			if (isSetError_) {
				QP_SAFE_DELETE(x);
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_SYNTAX_ERROR_EXECUTION, pErrorMsg_->c_str());
			}
		}
		c_str += t.n;
	} while (1);

	if (!isSetError_) {
		x->Execute(0, t, this);
	}
	QP_SAFE_DELETE(x);

	if (isSetError_) {
		if (pSelectionExprList_) {
			for (ExprList::iterator it = pSelectionExprList_->begin();
				 it != pSelectionExprList_->end(); it++) {
				QP_SAFE_DELETE(*it);
			}
		}
		QP_SAFE_DELETE(pSelectionExprList_);
		QP_SAFE_DELETE(pWhereExpr_);

		if (t.id == TK_ILLEGAL) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_SYNTAX_ERROR_INVALID_TOKEN,
				(util::String("Invalid token \'", QP_ALLOCATOR) +
					util::String(t.z, t.n, QP_ALLOCATOR) + '\'')
					.c_str());
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_SYNTAX_ERROR_EXECUTION, pErrorMsg_->c_str());
		}
	}

	if (hook_) {
		hook_->qpBuildTmpHook(*this, 4);	
	}


	BoolExpr *tmp = pWhereExpr_;
	if (tmp != NULL) {
		pWhereExpr_ = tmp->makeDNF(txn, objectManager_);  
		QP_DELETE(tmp);  
	}

	if (hook_) {
		hook_->qpBuildTmpHook(*this, 5);	
	}
	contractCondition();

	expireTs_ = timeSeries.getCurrentExpiredTime(txn);

	if (hook_) {
		hook_->qpBuildFinishHook(*this);	
	}
}

/*!
* @brief Execute Query for TimeSeries
*
*/
void QueryForTimeSeries::doQuery(
	TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet) {
	if (hook_) {
		hook_->qpSearchBeginHook(*this);	
	}
	BoolExpr *conditionExpr = getConditionExpr();  
	Expr *selectionExpr = getSelectionExpr(0);


	if (selectionExpr != NULL) {
		nPassSelectRequested_ = selectionExpr->getPassMode(txn);
	}
	else {
		nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
	}

	if (resultSet.getQueryOption().isPartial()) {
		nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
		doQueryPartial(txn, timeSeries, resultSet);
	}
	else 
	if (conditionExpr == NULL) {
		doQueryWithoutCondition(txn, timeSeries, resultSet);
	}
	else {
		doQueryWithCondition(txn, timeSeries, resultSet);
	}

	if (doExplain() &&
		(nPassSelectRequested_ == QP_PASSMODE_NO_PASS ||
			(nPassSelectRequested_ == QP_PASSMODE_PASS_IF_NO_WHERE &&
				pWhereExpr_ != NULL))) {
		util::NormalOStringStream os;
		os.str("");
		os << resultSet.getOIdList()->size();
		addExplain(
			0, "QUERY_EXECUTE_RESULT_ROWS", "INTEGER", os.str().c_str(), "");
	}
	if (hook_) {
		hook_->qpSearchFinishHook(*this);	
	}
}

void QueryForTimeSeries::doQueryWithoutCondition(
	TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet) {
	util::XArray<OId> &resultOIdList = *resultSet.getOIdList();
	if (doExplain()) {
		addExplain(0, "SELECTION", "CONDITION", "NULL", "");
		addExplain(1, "INDEX", "BTREE", "SUMMARYROWMAP", "");
	}
	OutputOrder outputOrder = ORDER_UNDEFINED;  
	isRowKeySearched_ = true;

	Timestamp *pExpireTs = &expireTs_;
	util::DateTime dt;

	if (expireTs_ != MINIMUM_EXPIRED_TIMESTAMP) {
		dt.setUnixTime(expireTs_);
	}
	else {
		dt.setUnixTime(0);
	}
	if (doExplain()) {
		util::NormalOStringStream os;
		util::DateTime::ZonedOption zonedOption = util::DateTime::ZonedOption::create(true, txn.getTimeZone());
		zonedOption.asLocalTimeZone_ = USE_LOCAL_TIMEZONE;
		dt.format(os, zonedOption);
		addExplain(1, "TIMESERIES_EXPIRE", "TIMESTAMP", os.str().c_str(), "");
	}
	if (expireTs_ == MINIMUM_EXPIRED_TIMESTAMP) {  
		pExpireTs = NULL;
	}

	if (nPassSelectRequested_ != QP_PASSMODE_NO_PASS && pOrderByExpr_ == NULL) {
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), timeSeries_->getRowIdColumnId());
		if (pExpireTs != NULL) {
			TermCondition cond(timeSeries_->getRowIdColumnType(), timeSeries_->getRowIdColumnType(),
				DSExpression::GE, timeSeries_->getRowIdColumnId(), pExpireTs,
				sizeof(*pExpireTs));
			sc.addCondition(txn, cond, true);
		}
		sc.setLimit(nLimit_);
		if (doExplain()) {
			addExplain(1, "API_PASSTHROUGH", "BOOLEAN", "TRUE", "NO ORDERBY");
		}
		sc.copy(txn.getDefaultAllocator(), scPass_);
	}
	else {
		nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
		ColumnId searchColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		ColumnType searchType = COLUMN_TYPE_WITH_BEGIN;

		if (isIndexSortAvailable(timeSeries, 0)) {
			SortExpr &orderExpr = (*pOrderByExpr_)[0];
			uint32_t orderColumnId = (orderExpr.expr)
										 ? orderExpr.expr->getColumnId()
										 : UNDEF_COLUMNID;
			util::Vector<ColumnId> otherColumnIds(txn.getDefaultAllocator());
			otherColumnIds.push_back(orderColumnId);
			bool withPartialMatch = false;
			if (orderExpr.expr &&
				(orderColumnId == ColumnInfo::ROW_KEY_COLUMN_ID ||
				timeSeries.hasIndex(txn, otherColumnIds, MAP_TYPE_BTREE, withPartialMatch))) {
				if (doExplain()) {
					addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
					if (orderColumnId == ColumnInfo::ROW_KEY_COLUMN_ID) {
						addExplain(1, "QUERY_RESULT_SORT_API_ROWKEY_ORDER", "STRING",
							(orderExpr.order == ASC) ? "ASC" : "DESC", "");
					} else {
						addExplain(1, "QUERY_RESULT_SORT_API_INDEX_ORDER", "STRING",
							(orderExpr.order == ASC) ? "ASC" : "DESC", "");
					}
				}
				outputOrder = (orderExpr.order == ASC)
								  ? ORDER_ASCENDING
								  : ORDER_DESCENDING;  
				setLimitByIndexSort();
				searchColumnId = orderColumnId;
				searchType =
					timeSeries.getColumnInfo(orderColumnId).getColumnType();
			}
		}

		UNUSED_VARIABLE(searchType);
		BtreeMap::SearchContext sc (txn.getDefaultAllocator(), searchColumnId);
		sc.setLimit(nLimit_);
		if (searchColumnId != ColumnInfo::ROW_KEY_COLUMN_ID) {
			sc.setNullCond(BaseIndex::SearchContext::ALL);
		}
		if (pExpireTs != NULL) {
			TermCondition condition(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
				DSExpression::GT, ColumnInfo::ROW_KEY_COLUMN_ID, pExpireTs,  sizeof(*pExpireTs));
			bool isKey = searchColumnId == ColumnInfo::ROW_KEY_COLUMN_ID;
			sc.addCondition(txn, condition, isKey);
		}
		if (searchColumnId != ColumnInfo::ROW_KEY_COLUMN_ID || outputOrder == ORDER_DESCENDING) {
			isRowKeySearched_ = false;
		}
		if (doExecute()) {
			if (searchColumnId != ColumnInfo::ROW_KEY_COLUMN_ID) {
				timeSeries.searchColumnIdIndex(
					txn, sc, resultOIdList, outputOrder);  
			}
			else {
				timeSeries.searchRowIdIndex(
					txn, sc, resultOIdList, outputOrder);  
			}

			if (isSortBeforeSelectClause() && outputOrder == ORDER_UNDEFINED) {
				assert(pOrderByExpr_->size() > 0);
				SortExpr &firstExpr = (*pOrderByExpr_)[0];
				if (firstExpr.expr->isColumn() &&
					firstExpr.expr->getColumnId() == 0) {
					assert(false);
					GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
						"Internal logic error: rowkey is already sorted ");
				}
				else {
					assert(pOrderByExpr_->size() > 0);
					if (doExplain()) {
						util::NormalOStringStream os;
						os.str("");
						addExplain(
							0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
						for (size_t i = 0; i < pOrderByExpr_->size(); i++) {
							SortExpr &orderExpr = (*pOrderByExpr_)[i];
							addExplain(1, "QUERY_RESULT_SORT_EXPRESSION",
								"STRING", orderExpr.expr->getValueAsString(txn),
								(orderExpr.order == ASC) ? "ASC" : "DESC");
						}
					}
					TimeSeriesOrderByComparator comp(
						txn, timeSeries, *getFunctionMap(), *pOrderByExpr_);
					std::sort(resultOIdList.begin(), resultOIdList.end(), comp);
				}
			}
		}
	}
}

void QueryForTimeSeries::doQueryWithCondition(
	TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet) {
	util::XArray<OId> &resultOIdList = *resultSet.getOIdList();

	util::DateTime dt;
	if (expireTs_ != MINIMUM_EXPIRED_TIMESTAMP) {
		dt.setUnixTime(expireTs_);
	}
	else {
		dt.setUnixTime(0);
	}
	if (doExplain()) {
		util::NormalOStringStream os;
		util::DateTime::ZonedOption zonedOption = util::DateTime::ZonedOption::create(true, txn.getTimeZone());
		zonedOption.asLocalTimeZone_ = USE_LOCAL_TIMEZONE;
		dt.format(os, zonedOption);
		addExplain(1, "TIMESERIES_EXPIRE", "TIMESTAMP", os.str().c_str(), "");
	}
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::XArray<BoolExpr *> orList(alloc),		
		andList(alloc);							
	uint32_t restConditions = 0;				
	OutputOrder outputOrder = ORDER_UNDEFINED;  
	isRowKeySearched_ = true;

	util::XArray<PointRowId> andPointRowIdArray(
		txn.getDefaultAllocator());  
	util::XArray<OId> operatorOutPointRowIdArray(
		txn.getDefaultAllocator());  

	orList.clear();
	getConditionAsOrList(orList);
	assert(!orList.empty());  

	for (uint32_t p = 0; p < orList.size(); p++) {
		if (doExplain()) {
			util::NormalOStringStream os;
			os << p;
			addExplain(0, "QUERY_LOOP_NUMBER", "INTEGER", os.str().c_str(), "");
		}

		andList.clear();
		orList[p]->toAndList(andList);
		assert(!andList.empty());  

		IndexData indexData(txn.getDefaultAllocator());

		bool indexFound = getIndexDataInAndList(
			txn, timeSeries, andList, indexData);

		if (!indexFound) {
			BtreeMap::SearchContext *sc = NULL;
			BoolExpr::toSearchContext(txn, andList, expireTs_, NULL, *this, sc,
				restConditions, nLimit_);

			if (orList.size() == 1 &&
				(nPassSelectRequested_ == QP_PASSMODE_PASS ||
					(nPassSelectRequested_ == QP_PASSMODE_PASS_IF_NO_WHERE &&
						pWhereExpr_ == NULL)) &&
				restConditions == 0 && sc->getRestConditionNum() == 0 &&
				pOrderByExpr_ == NULL) {
				if (doExplain()) {
					addExplain(1, "API_PASSTHROUGH", "BOOLEAN", "TRUE",
						"TIME_CONDITIONS_ONLY");
				}
				sc->copy(txn.getDefaultAllocator(), scPass_);
			}
			else {
				nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
				if (isIndexSortAvailable(timeSeries, orList.size())) {
					SortExpr &orderExpr = (*pOrderByExpr_)[0];
					if (orderExpr.expr &&
						orderExpr.expr->getColumnId() ==
							ColumnInfo::ROW_KEY_COLUMN_ID) {
						if (doExplain()) {
							addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN",
								"TRUE", "");
							addExplain(1,
								"QUERY_RESULT_SORT_API_ROWKEY_ORDER",
								"STRING",
								(orderExpr.order == ASC) ? "ASC" : "DESC",
								"");
						}
						outputOrder = (orderExpr.order == ASC)
										  ? ORDER_ASCENDING
										  : ORDER_DESCENDING;  
						setLimitByIndexSort();
						BoolExpr::toSearchContext(txn, andList, expireTs_, NULL, *this, sc,
							restConditions, nLimit_);
					}
				}
				if (doExecute()) {
					addExplain(1, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", "");
					addExplain(
						2, "SEARCH_MAP", "STRING", "TIME_SERIES_ROW_MAP", "");
					operatorOutPointRowIdArray.clear();
					timeSeries.searchRowIdIndex(txn, *sc,
						operatorOutPointRowIdArray, outputOrder);  
				}
			}
		}
		else {
			assert(!indexData.columnIds_->empty());
			ColumnInfo *indexColumnInfo = &(timeSeries.getColumnInfo((*(indexData.columnIds_))[0]));
			switch (indexData.mapType_) {
			case MAP_TYPE_BTREE: {
				assert(indexColumnInfo != NULL);
				BtreeMap::SearchContext *sc = NULL;
				BoolExpr::toSearchContext(txn, andList, expireTs_,
					&indexData, *this, sc, restConditions, nLimit_);

				nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
				if (isIndexSortAvailable(timeSeries, orList.size())) {
					SortExpr &orderExpr = (*pOrderByExpr_)[0];
					if (orderExpr.expr &&
						orderExpr.expr->getColumnId() ==
							indexColumnInfo
								->getColumnId()) {  
						if (doExplain()) {
							addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN",
								"TRUE", "");
							addExplain(1,
								"QUERY_RESULT_SORT_API_INDEX_ORDER",
								"STRING",
								(orderExpr.order == ASC) ? "ASC" : "DESC",
								"");
						}
						outputOrder = (orderExpr.order == ASC)
										  ? ORDER_ASCENDING
										  : ORDER_DESCENDING;  
						setLimitByIndexSort();
						BoolExpr::toSearchContext(txn, andList, expireTs_,
							&indexData, *this, sc, restConditions, nLimit_);

					}
				}
				if (doExecute()) {
					util::NormalOStringStream os;
					{
						util::Vector<TermCondition> condList(alloc);
						sc->getConditionList(condList, BaseIndex::SearchContext::COND_KEY);
						util::Set<ColumnId> useColumnSet(alloc);
						util::Set<ColumnId>::iterator itr;
						bool isFirstColumn = true;
						for (size_t i = 0; i < condList.size(); i++) {
							itr = useColumnSet.find(condList[i].columnId_);
							if (itr == useColumnSet.end()) {
								useColumnSet.insert(condList[i].columnId_);
								if (!isFirstColumn) {
									os << " ";
								} else {
									isFirstColumn = false;
								}
								ColumnInfo &columnInfo = timeSeries.getColumnInfo(condList[i].columnId_);
								os << columnInfo.getColumnName(txn, objectManager_);
							}
						}
						addExplain(
							1, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", os.str().c_str());
					}
					{
						os.str("");
						for (size_t i = 0; i < indexData.columnIds_->size(); i++) {
							if (i != 0) {
								os << " ";
							}
							ColumnInfo &columnInfo = timeSeries.getColumnInfo((*(indexData.columnIds_))[i]);
							os << columnInfo.getColumnName(txn, objectManager_);
						}

						addExplain(2, "SEARCH_MAP", "STRING", os.str().c_str(), "");
					}
					operatorOutPointRowIdArray.clear();
					timeSeries.searchColumnIdIndex(txn, *sc,
						operatorOutPointRowIdArray, outputOrder);  
				}
			} break;
			default:  
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: Invalid map type specified.");
				break;
			}
			if (indexColumnInfo->getColumnId() != ColumnInfo::ROW_KEY_COLUMN_ID && outputOrder != ORDER_UNDEFINED) {
				isRowKeySearched_ = false;
			}
		}

		if (restConditions == 0) {  
			andPointRowIdArray.assign(operatorOutPointRowIdArray.begin(),
				operatorOutPointRowIdArray.end());
		}
		else {
			uint64_t *pBitmap = reinterpret_cast<uint64_t *>(alloc.allocate(
				sizeof(uint64_t) * ((timeSeries.getColumnNum() / 64) + 1)));
			TimeSeriesRowWrapper row(txn, timeSeries, pBitmap);

			for (uint32_t i = 0; i < operatorOutPointRowIdArray.size(); i++) {
				PointRowId pointRowId = operatorOutPointRowIdArray[i];
				bool conditionFlag = true;  
				{
					util::StackAllocator::Scope scope(alloc);
					row.load(pointRowId);
					for (uint32_t q = 0; q < andList.size(); q++) {
						TrivalentLogicType evalResult = andList[q]->eval(
							txn, objectManager_, &row, getFunctionMap(), TRI_TRUE);
						if (evalResult != TRI_TRUE) {
							conditionFlag = false;
							break;
						}
					}
				}
				if (conditionFlag) {
					andPointRowIdArray.push_back(pointRowId);
				}
			}
			alloc.deallocate(pBitmap);
		}  

		if (doExecute() && doExplain()) {
			util::NormalOStringStream os;
			os << andPointRowIdArray.size();
			addExplain(
				1, "SEARCH_RESULT_ROWS", "INTEGER", os.str().c_str(), "");
		}

		if (andPointRowIdArray.size() > 0) {
			util::XArray<PointRowId> mergeTemporaryList(alloc);
			mergeTemporaryList.clear();
			util::XArray<PointRowId>::iterator it = resultOIdList.begin();
			while (it != resultOIdList.end()) {
				mergeTemporaryList.push_back(*it);
				++it;
			}

			resultOIdList.clear();
			tsResultMerge(txn, timeSeries, mergeTemporaryList,
				andPointRowIdArray, resultOIdList);
		}
		andPointRowIdArray.clear();

		if (resultOIdList.size() >= nLimit_) {
			break;
		}
	}  

	if (resultOIdList.size() > nLimit_) {
		resultOIdList.erase(
			resultOIdList.begin() + static_cast<size_t>(nLimit_),
			resultOIdList.end());
	}

	if (isSortBeforeSelectClause() && outputOrder == ORDER_UNDEFINED) {
		assert(pOrderByExpr_->size() > 0);

		SortExpr &firstExpr = (*pOrderByExpr_)[0];
		if (firstExpr.expr->isColumn() && firstExpr.expr->getColumnId() == 0) {
			apiOutputOrder_ =
				(firstExpr.order == ASC) ? ORDER_ASCENDING : ORDER_DESCENDING;
			if (doExplain()) {
				addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "FALSE",
					"BECAUSE_OF_ROWKEY");
				addExplain(1, "QUERY_RESULT_SORT_API_ROWKEY_ORDER", "STRING",
					(firstExpr.order == ASC) ? "ASC" : "DESC", "");
			}
		}
		else {
			if (doExplain()) {
				addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
				for (size_t i = 0; i < pOrderByExpr_->size(); i++) {
					SortExpr &orderExpr = (*pOrderByExpr_)[i];
					addExplain(1, "QUERY_RESULT_SORT_EXPRESSION", "STRING",
						orderExpr.expr->getValueAsString(txn),
						(orderExpr.order == ASC) ? "ASC" : "DESC");
				}
			}

			TimeSeriesOrderByComparator comp(
				txn, timeSeries, *getFunctionMap(), *pOrderByExpr_);
			std::sort(resultOIdList.begin(), resultOIdList.end(), comp);
		}
	}
}

void QueryForTimeSeries::doSelectionByAPI(TransactionContext &txn,
	TimeSeries &timeSeries, ResultSet &resultSet) {
	Expr *selectionExpr = getSelectionExpr(0);

	ResultType type;
	ResultSize resultNum;
	util::XArray<uint8_t> &serializedRowList =
		*resultSet.getRowDataFixedPartBuffer();
	util::XArray<uint8_t> &serializedVarDataList =
		*resultSet.getRowDataVarPartBuffer();

	OutputMessageRowStore outputMessageRowStore(
		timeSeries.getDataStore()->getValueLimitConfig(),
		timeSeries.getColumnInfoList(), timeSeries.getColumnNum(),
		serializedRowList, serializedVarDataList, false);
	if (selectionExpr->isAggregation()) {
		Value value;
		if (nOffset_ < 1) {
			selectionExpr->aggregate(
				txn, timeSeries, scPass_, resultNum, value);
		} else {
			resultNum = 0;
		}
		if (resultNum > 0) {
			value.serialize(serializedRowList);
		}
		type = RESULT_AGGREGATE;
	}
	else {
		selectionExpr->select(txn, timeSeries, scPass_, apiOutputOrder_,
			nActualLimit_, nOffset_, resultNum, &outputMessageRowStore, type);
		type = RESULT_ROWSET;
	}
	resultSet.setResultType(type, resultNum);
}

void QueryForTimeSeries::doSelection(
	TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet) {
	ResultType type;
	ResultSize resultNum;
	util::XArray<uint8_t> &serializedRowList =
		*resultSet.getRowDataFixedPartBuffer();
	util::XArray<uint8_t> &serializedVarDataList =
		*resultSet.getRowDataVarPartBuffer();
	util::XArray<OId> &resultOIdList = *resultSet.getOIdList();
	if (nPassSelectRequested_) {
		doSelectionByAPI(txn, timeSeries, resultSet);
		resultNum = resultSet.getResultNum();
		type = resultSet.getResultType();
	}
	else {
		size_t numSelection = getSelectionExprLength();
		if (resultSet.getQueryOption().isPartial()) {
			resultNum = resultSet.getResultNum();
			type = RESULT_ROWSET;
		} else
		if (numSelection > 0) {
			Expr *selectionExpr = getSelectionExpr(0);
			if (selectionExpr->isAggregation()) {
				type = RESULT_AGGREGATE;
				Value result;
				bool resultOK = false;
				if (nOffset_ < 1) {
					resultOK = selectionExpr->aggregate(
						txn, timeSeries, resultOIdList, result);
				}
				if (resultOK) {
					result.serialize(serializedRowList);
					resultNum = 1;
				}
				else {
					resultNum = 0;
				}
			}
			else {
				if (strcmp(selectionExpr->getValueAsString(txn), "*") == 0) {
					if (nOffset_ < resultOIdList.size()) {
						type = RESULT_ROW_ID_SET;
						if (apiOutputOrder_ == ORDER_DESCENDING) {
							std::reverse(
								resultOIdList.begin(), resultOIdList.end());
						}
						resultOIdList.erase(resultOIdList.begin(),
							resultOIdList.begin() +
								static_cast<size_t>(nOffset_));

						if (nActualLimit_ < resultOIdList.size()) {
							resultOIdList.erase(
								resultOIdList.begin() +
									static_cast<size_t>(nActualLimit_),
								resultOIdList.end());
						}
						resultNum = resultOIdList.size();
					}
					else {
						type = RESULT_ROWSET;
						resultNum = 0;
					}
				}
				else if (selectionExpr->isSelection()) {
					OutputMessageRowStore outputMessageRowStore(
						timeSeries.getDataStore()->getValueLimitConfig(),
						timeSeries.getColumnInfoList(),
						timeSeries.getColumnNum(), serializedRowList,
						serializedVarDataList, false);
					type = RESULT_ROWSET;
					bool doSort = (pOrderByExpr_ != NULL);
					if (doSort) {
						SortExpr &firstExpr = (*pOrderByExpr_)[0];
						if (firstExpr.expr->isColumn() &&
							firstExpr.expr->getColumnId() == 0) {
							doSort = false;
							apiOutputOrder_ = (firstExpr.order == ASC)
												  ? ORDER_ASCENDING
												  : ORDER_DESCENDING;
							if (doExplain()) {
								addExplain(0, "QUERY_RESULT_SORT_SELECTION",
									"BOOLEAN", "FALSE", "BECAUSE_OF_ROWKEY");
								addExplain(1,
									"QUERY_RESULT_SORT_API_ROWKEY_ORDER",
									"STRING",
									(firstExpr.order == ASC) ? "ASC" : "DESC",
									"");
							}
						}

						if (doSort && doExplain()) {
							util::NormalOStringStream os;
							os.str("");
							addExplain(0, "QUERY_RESULT_SORT_SELECTION",
								"BOOLEAN", "TRUE", "");
							for (size_t i = 0; i < pOrderByExpr_->size(); i++) {
								SortExpr &orderExpr = (*pOrderByExpr_)[i];
								addExplain(1, "QUERY_RESULT_SORT_EXPRESSION",
									"STRING",
									orderExpr.expr->getValueAsString(txn),
									(orderExpr.order == ASC) ? "ASC" : "DESC");
							}
						}
					}

					selectionExpr->select(txn, timeSeries, resultOIdList,
						isRowKeySearched_, apiOutputOrder_, pOrderByExpr_,
						nActualLimit_, nOffset_, *getFunctionMap(), resultNum,
						&outputMessageRowStore, type);
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
						"Column selection and direct value selection are not "
						"implemented");
				}
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: no selection expression specified.");
		}
	}

	if (doExplain()) {
		util::NormalOStringStream os;
		switch (type) {
		case RESULT_ROWSET:
			addExplain(0, "QUERY_RESULT_TYPE", "STRING", "RESULT_ROWSET", "");
			break;
		case RESULT_AGGREGATE:
			addExplain(
				0, "QUERY_RESULT_TYPE", "STRING", "RESULT_AGGREGATE", "");

			break;
		case RESULT_ROW_ID_SET:
			addExplain(
				0, "QUERY_RESULT_TYPE", "STRING", "RESULT_ROW_ID_SET", "");
			break;
		default:
			addExplain(0, "QUERY_RESULT_TYPE", "STRING", "?", "");
			break;
		}
		os << resultNum;
		addExplain(0, "QUERY_RESULT_ROWS", "INTEGER", os.str().c_str(), "");
	}

	if (hook_) {
		hook_->qpSelectFinishHook(*this);	
	}
	resultSet.setResultType(type, resultNum);
}

void QueryForTimeSeries::tsResultMerge(TransactionContext &txn,
	TimeSeries &timeSeries, util::XArray<PointRowId> &list1,
	util::XArray<PointRowId> &list2, util::XArray<PointRowId> &listOut) {
	util::XArray<PointRowId>::iterator it1 = list1.begin();
	util::XArray<PointRowId>::iterator it2 = list2.begin();

	OId p;  
	Timestamp xt = UNDEF_TIMESTAMP, yt = UNDEF_TIMESTAMP;
	BaseContainer::RowArray rowArray(txn, &timeSeries);  

	assert(listOut.size() == 0);
	if (it1 != list1.end()) {
		rowArray.load(
			txn, *it1, &timeSeries, OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		xt = row.getRowId();											  
	}
	if (it2 != list2.end()) {
		rowArray.load(
			txn, *it2, &timeSeries, OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		yt = row.getRowId();											  
	}

	while (it1 != list1.end() && it2 != list2.end()) {

		if (xt == yt || (*it1 == *it2)) {  
			p = *it1;
			listOut.push_back(p);
			++it1;
			++it2;
			if (it1 != list1.end()) {
				rowArray.load(txn, *it1, &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				xt = row.getRowId();					
			}
			if (it2 != list2.end()) {
				rowArray.load(txn, *it2, &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				yt = row.getRowId();					
			}
		}
		else if (xt < yt) {  
			p = *it1;
			listOut.push_back(p);
			++it1;
			if (it1 != list1.end()) {
				rowArray.load(txn, *it1, &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				xt = row.getRowId();					
			}
		}
		else if (xt > yt) {
			p = *it2;
			listOut.push_back(p);
			++it2;
			if (it2 != list2.end()) {
				rowArray.load(txn, *it2, &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				yt = row.getRowId();					
			}
		}
		else {
			assert(0);
		}
	}
	assert((list1.end() == it1 && list2.end() != it2) ||
		   (list1.end() != it1 && list2.end() == it2) ||
		   (list1.end() == it1 && list2.end() == it2));
	if (xt == yt && xt != UNDEF_TIMESTAMP) {
		if (it1 != list1.end()) {
			++it1;
		}
		if (it2 != list2.end()) {
			++it2;
		}
	}

	while (list1.end() != it1) {
		listOut.push_back(*it1);
		++it1;
	}

	while (list2.end() != it2) {
		listOut.push_back(*it2);
		++it2;
	}
}

QueryForTimeSeries *QueryForTimeSeries::dup(
	TransactionContext &txn, ObjectManager &objectManager_) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	char *dbName = NULL;
	char *queryStr = NULL;
	FullContainerKey *containerKey = NULL;
	if (tqlInfo_.dbName_ != NULL) {
		size_t len = strlen(tqlInfo_.dbName_) + 1;
		char *dbName = QP_ALLOC_NEW(alloc) char[len];
		memcpy(dbName, tqlInfo_.dbName_, len);
	}
	if (tqlInfo_.query_ != NULL) {
		size_t len = strlen(tqlInfo_.query_) + 1;
		char *queryStr = QP_ALLOC_NEW(alloc) char[len];
		memcpy(queryStr, tqlInfo_.query_, len);
	}
	if (tqlInfo_.containerKey_ != NULL) {
		const void *body;
		size_t size;
		tqlInfo_.containerKey_->toBinary(body, size);
		containerKey = QP_ALLOC_NEW(alloc) FullContainerKey(alloc, KeyConstraint(), body, size);
	}

	TQLInfo tqlInfo(dbName, containerKey, queryStr);
	QueryForTimeSeries *query =
		QP_ALLOC_NEW(alloc) QueryForTimeSeries(txn_, *timeSeries_, tqlInfo);
	if (pErrorMsg_ != NULL) {
		query->pErrorMsg_ = QP_ALLOC_NEW(alloc) util::String(alloc);
		query->pErrorMsg_ = pErrorMsg_;
	}
	else {
		query->pErrorMsg_ = NULL;
	}

	query->isSetError_ = isSetError_;
	if (pSelectionExprList_ != NULL) {
		query->pSelectionExprList_ = QP_ALLOC_NEW(alloc) ExprList(alloc);
		for (ExprList::iterator itr = pSelectionExprList_->begin();
			 itr != pSelectionExprList_->end(); itr++) {
			query->pSelectionExprList_->push_back(
				(*itr)->dup(txn, objectManager_));
		}
	}
	else {
		query->pSelectionExprList_ = NULL;
	}
	if (pWhereExpr_ != NULL) {
		query->pWhereExpr_ = pWhereExpr_->dup(txn, objectManager_);
	}
	else {
		query->pWhereExpr_ = NULL;
	}
	if (pFromCollectionName_ != NULL) {
		size_t len = strlen(pFromCollectionName_) + 1;
		char *str = QP_ALLOC_NEW(alloc) char[len];
		memcpy(str, pFromCollectionName_, len);
		query->pFromCollectionName_ = str;
	}
	else {
		query->pFromCollectionName_ = NULL;
	}
	query->parseState_ = parseState_;
	query->nResultSorted_ = nResultSorted_;

	query->explainNum_ = explainNum_;
	query->explainAllocNum_ = explainAllocNum_;
	query->isExplainExecute_ = isExplainExecute_;
	if (explainData_ != NULL) {
		query->explainData_ = reinterpret_cast<ExplainData *>(
			alloc.allocate(sizeof(struct ExplainData) * explainAllocNum_));
		for (uint32_t i = 0; i < explainNum_; i++) {
			query->explainData_[i] = *explainData_[i].dup(txn);
		}
	}
	else {
		query->explainData_ = NULL;
	}
	query->hook_ = hook_;  

	query->isDistinct_ = isDistinct_;
	query->nActualLimit_ = nActualLimit_;
	query->nLimit_ = nLimit_;
	query->nOffset_ = nOffset_;
	if (pLimitExpr_ != NULL) {
		query->pLimitExpr_ = pLimitExpr_->dup(txn, objectManager_);
	}
	else {
		query->pLimitExpr_ = NULL;
	}
	if (pGroupByExpr_ != NULL) {
		query->pGroupByExpr_ = QP_ALLOC_NEW(alloc) ExprList(alloc);
		for (ExprList::iterator itr = pGroupByExpr_->begin();
			 itr != pGroupByExpr_->end(); itr++) {
			query->pGroupByExpr_->push_back((*itr)->dup(txn, objectManager_));
		}
	}
	else {
		query->pGroupByExpr_ = NULL;
	}
	if (pHavingExpr_ != NULL) {
		query->pHavingExpr_ = pHavingExpr_->dup(txn, objectManager_);
	}
	else {
		query->pHavingExpr_ = NULL;
	}
	if (pOrderByExpr_ != NULL) {
		query->pOrderByExpr_ = QP_ALLOC_NEW(alloc) SortExprList(alloc);
		for (SortExprList::iterator itr = pOrderByExpr_->begin();
			 itr != pOrderByExpr_->end(); itr++) {
			query->pOrderByExpr_->push_back(*((*itr).dup(txn, objectManager_)));
		}
	}
	else {
		query->pOrderByExpr_ = NULL;
	}

	query->functionMap_ = functionMap_;
	query->aggregationMap_ = aggregationMap_;
	query->selectionMap_ = selectionMap_;
	query->specialIdMap_ = specialIdMap_;

	query->expireTs_ = expireTs_;
	query->scPass_ =
		scPass_;  
	{

		scPass_.copy(alloc, query->scPass_);
	}
	query->nPassSelectRequested_ = nPassSelectRequested_;
	query->isRowKeySearched_ = isRowKeySearched_;
	query->apiOutputOrder_ = apiOutputOrder_;

	return query;
}
