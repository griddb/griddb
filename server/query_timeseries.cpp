/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	TimeSeries &timeSeries, const char *statement, uint64_t limit,
	QueryHookClass *hook)
	: Query(txn, *(timeSeries.getObjectManager()), "", limit, hook),
	  timeSeries_(&timeSeries) {
	if (hook_) {
		hook_->qpBuildBeginHook(*this);
	}
	this->str_ = statement;
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
	const char *c_str = statement;
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


#ifdef QP_ENABLE_SELECTION_PASSTHROUGH
	if (selectionExpr != NULL) {
		nPassSelectRequested_ = selectionExpr->getPassMode(txn);
	}
	else {
		nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
	}
#endif

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
		dt.format(os, true);
		addExplain(1, "TIMESERIES_EXPIRE", "TIMESTAMP", os.str().c_str(), "");
	}
	if (expireTs_ == MINIMUM_EXPIRED_TIMESTAMP) {  
		pExpireTs = NULL;
	}


	if (nPassSelectRequested_ != QP_PASSMODE_NO_PASS && pOrderByExpr_ == NULL) {
		BtreeMap::SearchContext sc(
			0, pExpireTs, 0, true, NULL, 0, true, 0, NULL, nLimit_);
		if (doExplain()) {
			addExplain(1, "API_PASSTHROUGH", "BOOLEAN", "TRUE", "NO ORDERBY");
		}
		memcpy(&scPass_, &sc, sizeof(sc));
	}
	else {
		nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
		ColumnId searchColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		ColumnType searchType = COLUMN_TYPE_WITH_BEGIN;

		if (pOrderByExpr_ && pOrderByExpr_->size() == 1) {
			SortExpr &orderExpr = (*pOrderByExpr_)[0];
			uint32_t orderColumnId = (orderExpr.expr)
										 ? orderExpr.expr->getColumnId()
										 : UNDEF_COLUMNID;
			if (orderExpr.expr &&
				timeSeries.hasIndex(
					txn, orderColumnId, MAP_TYPE_BTREE)) {  
				if (doExplain()) {
					addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
					addExplain(1, "QUERY_RESULT_SORT_API_INDEX_ORDER", "STRING",
						(orderExpr.order == ASC) ? "ASC" : "DESC", "");
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

		BtreeMap::SearchContext sc(searchColumnId, pExpireTs, 0, true, NULL, 0,
			true, 0, NULL, nLimit_);
		sc.keyType_ = searchType;
		if (pExpireTs != NULL &&
			searchColumnId != ColumnInfo::ROW_KEY_COLUMN_ID) {
			sc.startKey_ = NULL;
			TermCondition *condition =
				ALLOC_NEW(txn.getDefaultAllocator()) TermCondition;
			condition->columnId_ = ColumnInfo::ROW_KEY_COLUMN_ID;
			condition->operator_ = &geTimestampTimestamp;
			condition->value_ = reinterpret_cast<const uint8_t *>(pExpireTs);
			condition->valueSize_ = sizeof(Timestamp);
			sc.conditionList_ = condition;
			sc.conditionNum_ = 1;
		}
		if (doExecute()) {
			if (outputOrder != ORDER_UNDEFINED) {
				timeSeries.searchColumnIdIndex(
					txn, sc, resultOIdList, outputOrder);  
			}
			else {
				timeSeries.searchRowIdIndex(
					txn, sc, resultOIdList, ORDER_UNDEFINED);  
			}

			Expr *pSel = getSelectionExpr(0);
			if (pOrderByExpr_ && pSel && !pSel->isSelection() &&
				(outputOrder == ORDER_UNDEFINED)) {
				assert(
					!pSel->isAggregation());  
				assert(pOrderByExpr_->size() > 0);
				SortExpr &firstExpr = (*pOrderByExpr_)[0];
				if (firstExpr.expr->isColumn() &&
					firstExpr.expr->getColumnId() == 0) {
					apiOutputOrder_ = (firstExpr.order == ASC)
										  ? ORDER_ASCENDING
										  : ORDER_DESCENDING;
					if (doExplain()) {
						addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "FALSE",
							"BECAUSE_OF_ROWKEY");
						addExplain(1, "QUERY_RESULT_SORT_API_ROWKEY_ORDER",
							"STRING", (firstExpr.order == ASC) ? "ASC" : "DESC",
							"");
					}
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
		dt.format(os, true);
		addExplain(1, "TIMESERIES_EXPIRE", "TIMESTAMP", os.str().c_str(), "");
	}
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::XArray<BoolExpr *> orList(alloc),		
		andList(alloc);							
	uint32_t restConditions = 0;				
	OutputOrder outputOrder = ORDER_UNDEFINED;  

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

		MapType indexType;
		ColumnInfo *indexColumnInfo;

		getIndexInfoInAndList(
			txn, timeSeries, andList, indexType, indexColumnInfo);

		if (indexColumnInfo == NULL) {
			BtreeMap::SearchContext sc(
				0, pExpireTs, 0, true, NULL, 0, true, 0, NULL, nLimit_);
			BoolExpr::toSearchContext(txn, andList, expireTs_, NULL, *this, sc,
				restConditions, nLimit_);

			if (orList.size() == 1 &&
				(nPassSelectRequested_ == QP_PASSMODE_PASS ||
					(nPassSelectRequested_ == QP_PASSMODE_PASS_IF_NO_WHERE &&
						pWhereExpr_ == NULL)) &&
				restConditions == 0 && sc.conditionNum_ == 0 &&
				pOrderByExpr_ == NULL) {
				if (doExplain()) {
					addExplain(1, "API_PASSTHROUGH", "BOOLEAN", "TRUE",
						"TIME_CONDITIONS_ONLY");
				}
				memcpy(&scPass_, &sc, sizeof(sc));
			}
			else {
				nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
				isRowKeySearched_ = true;
				if (doExecute()) {
					addExplain(1, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", "");
					addExplain(
						2, "SEARCH_MAP", "STRING", "TIME_SERIES_ROW_MAP", "");
					operatorOutPointRowIdArray.clear();
					timeSeries.searchRowIdIndex(txn, sc,
						operatorOutPointRowIdArray, ORDER_UNDEFINED);  
				}
			}
		}
		else {
			isRowKeySearched_ = (indexColumnInfo->getColumnId() ==
								 ColumnInfo::ROW_KEY_COLUMN_ID);
			switch (indexType) {
			case MAP_TYPE_BTREE: {
				assert(indexColumnInfo != NULL);
				BtreeMap::SearchContext sc;
				BoolExpr::toSearchContext(txn, andList, expireTs_,
					indexColumnInfo, *this, sc, restConditions, nLimit_);

				if (orList.size() == 1 &&
					(nPassSelectRequested_ == QP_PASSMODE_PASS ||
						(nPassSelectRequested_ ==
								QP_PASSMODE_PASS_IF_NO_WHERE &&
							pWhereExpr_ == NULL)) &&
					restConditions == 0 && isRowKeySearched_ &&
					sc.conditionNum_ == 0) {
					if (doExplain()) {
						addExplain(1, "API_PASSTHROUGH", "BOOLEAN", "TRUE",
							"TIME_CONDITIONS_ONLY");
					}
				}
				else {
					nPassSelectRequested_ = QP_PASSMODE_NO_PASS;
					if (orList.size() == 1 && pOrderByExpr_ &&
						pOrderByExpr_->size() == 1) {
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
							pOrderByExpr_->clear();
							setLimitByIndexSort();
							BoolExpr::toSearchContext(txn, andList, expireTs_,
								indexColumnInfo, *this, sc, restConditions,
								nLimit_);
						}
					}
					if (doExecute()) {
						addExplain(
							1, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", "");
						const char *columnName =
							indexColumnInfo->getColumnName(txn, objectManager_);
						addExplain(2, "SEARCH_MAP", "STRING", columnName, "");
						operatorOutPointRowIdArray.clear();
						timeSeries.searchColumnIdIndex(txn, sc,
							operatorOutPointRowIdArray, outputOrder);  
					}
				}
			} break;
			default:  
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: Invalid map type specified.");
				break;
			}
		}

		if (restConditions == 0) {  
			andPointRowIdArray.assign(operatorOutPointRowIdArray.begin(),
				operatorOutPointRowIdArray.end());
		}
		else {
			uint64_t *pBitmap = reinterpret_cast<uint64_t *>(alloc.allocate(
				sizeof(uint64_t) * ((timeSeries.getColumnNum() / 64) + 1)));

			for (uint32_t i = 0; i < operatorOutPointRowIdArray.size(); i++) {
				PointRowId pointRowId = operatorOutPointRowIdArray[i];
				bool conditionFlag = true;  
				{
					util::StackAllocator::Scope scope(alloc);
					TimeSeriesRowWrapper row(
						txn, timeSeries, pointRowId, pBitmap);
					for (uint32_t q = 0; q < andList.size(); q++) {
						bool evalResult = andList[q]->eval(
							txn, objectManager_, &row, getFunctionMap(), true);
						if (!evalResult) {
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
		ResultSize eraseSize = resultOIdList.size() - nLimit_;
		resultOIdList.erase(
			resultOIdList.begin() + static_cast<size_t>(nLimit_),
			resultOIdList.end());
		if (resultSet.getRowIdList()->size() > 0) {
			ResultSize erasePos = resultSet.getRowIdList()->size() - eraseSize;
			resultSet.getRowIdList()->erase(resultSet.getRowIdList()->begin() +
												static_cast<size_t>(erasePos),
				resultSet.getRowIdList()->end());
		}
	}

	Expr *pSel = getSelectionExpr(0);
	if (pOrderByExpr_ && pSel && !pSel->isSelection() &&
		(outputOrder == ORDER_UNDEFINED)) {
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
	TimeSeries &timeSeries, ResultType &type, ResultSize &resultNum,
	util::XArray<uint8_t> &serializedRowList,
	util::XArray<uint8_t> &serializedVarDataList) {
	Expr *selectionExpr = getSelectionExpr(0);
	OutputMessageRowStore outputMessageRowStore(
		timeSeries.getDataStore()->getValueLimitConfig(),
		timeSeries.getColumnInfoList(), timeSeries.getColumnNum(),
		serializedRowList, serializedVarDataList, false);
	if (selectionExpr->isAggregation()) {
		selectionExpr->aggregate(
			txn, timeSeries, scPass_, resultNum, serializedRowList, type);
		type = RESULT_AGGREGATE;
	}
	else {
		selectionExpr->select(txn, timeSeries, scPass_, apiOutputOrder_,
			nActualLimit_, nOffset_, resultNum, &outputMessageRowStore, type);
		type = RESULT_ROWSET;
	}
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
		doSelectionByAPI(txn, timeSeries, type, resultNum, serializedRowList,
			serializedVarDataList);
	}
	else {
		size_t numSelection = getSelectionExprLength();
		if (numSelection > 0) {
			Expr *selectionExpr = getSelectionExpr(0);
			if (selectionExpr->isAggregation()) {
				type = RESULT_AGGREGATE;
				Value result;
				bool resultOK = selectionExpr->aggregate(
					txn, timeSeries, resultOIdList, result);
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
						if (resultSet.getRowIdList()->size() > 0) {
							resultSet.getRowIdList()->erase(
								resultSet.getRowIdList()->begin(),
								resultSet.getRowIdList()->begin() +
									static_cast<size_t>(nOffset_));
						}

						if (nActualLimit_ < resultOIdList.size()) {
							ResultSize eraseSize =
								resultOIdList.size() - nActualLimit_;
							resultOIdList.erase(
								resultOIdList.begin() +
									static_cast<size_t>(nActualLimit_),
								resultOIdList.end());
							if (resultSet.getRowIdList()->size() > 0) {
								ResultSize erasePos =
									resultSet.getRowIdList()->size() -
									eraseSize;
								resultSet.getRowIdList()->erase(
									resultSet.getRowIdList()->begin() +
										static_cast<size_t>(erasePos),
									resultSet.getRowIdList()->end());
							}
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
	TimeSeries::RowArray rowArray(txn, &timeSeries);  

	assert(listOut.size() == 0);
	if (it1 != list1.end()) {
		rowArray.load(
			txn, *it1, &timeSeries, OBJECT_READ_ONLY);  
		TimeSeries::RowArray::Row row(rowArray.getRow(), &rowArray);  
		xt = row.getTime();											  
	}
	if (it2 != list2.end()) {
		rowArray.load(
			txn, *it2, &timeSeries, OBJECT_READ_ONLY);  
		TimeSeries::RowArray::Row row(rowArray.getRow(), &rowArray);  
		yt = row.getTime();											  
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
				TimeSeries::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				xt = row.getTime();					
			}
			if (it2 != list2.end()) {
				rowArray.load(txn, *it2, &timeSeries,
					OBJECT_READ_ONLY);  
				TimeSeries::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				yt = row.getTime();					
			}
		}
		else if (xt < yt) {  
			p = *it1;
			listOut.push_back(p);
			++it1;
			if (it1 != list1.end()) {
				rowArray.load(txn, *it1, &timeSeries,
					OBJECT_READ_ONLY);  
				TimeSeries::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				xt = row.getTime();					
			}
		}
		else if (xt > yt) {
			p = *it2;
			listOut.push_back(p);
			++it2;
			if (it2 != list2.end()) {
				rowArray.load(txn, *it2, &timeSeries,
					OBJECT_READ_ONLY);  
				TimeSeries::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				yt = row.getTime();					
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
	QueryForTimeSeries *query =
		QP_ALLOC_NEW(alloc) QueryForTimeSeries(txn_, *timeSeries_);
	if (str_ != NULL) {
		size_t len = strlen(str_) + 1;
		char *str = QP_ALLOC_NEW(alloc) char[len];
		memcpy(str, str_, len);
		query->str_ = str;
	}
	else {
		query->str_ = NULL;
	}
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
	query->scPass_ = scPass_;  
	{
		if (scPass_.startKeySize_ > 0) {
			query->scPass_.startKey_ =
				QP_ALLOC_NEW(alloc) uint8_t[scPass_.startKeySize_];
			memcpy(const_cast<void *>(query->scPass_.startKey_),
				scPass_.startKey_, scPass_.startKeySize_);
		}
		if (scPass_.endKeySize_ > 0) {
			query->scPass_.endKey_ =
				QP_ALLOC_NEW(alloc) uint8_t[scPass_.endKeySize_];
			memcpy(const_cast<void *>(query->scPass_.endKey_), scPass_.endKey_,
				scPass_.endKeySize_);
		}
		if (scPass_.conditionNum_ > 0) {
			query->scPass_.conditionList_ =
				QP_ALLOC_NEW(alloc) TermCondition[scPass_.conditionNum_];
			memcpy(query->scPass_.conditionList_, scPass_.conditionList_,
				scPass_.conditionNum_);
		}
		for (uint32_t i = 0; i < scPass_.conditionNum_; i++) {
			if (scPass_.conditionList_[i].valueSize_ > 0) {
				query->scPass_.conditionList_[i].value_ = QP_ALLOC_NEW(alloc)
					uint8_t[scPass_.conditionList_[i].valueSize_];
				memcpy(const_cast<uint8_t *>(
						   query->scPass_.conditionList_[i].value_),
					scPass_.conditionList_[i].value_,
					scPass_.conditionList_[i].valueSize_);
			}
		}
	}
	query->nPassSelectRequested_ = nPassSelectRequested_;
	query->isRowKeySearched_ = isRowKeySearched_;
	query->apiOutputOrder_ = apiOutputOrder_;

	return query;
}
