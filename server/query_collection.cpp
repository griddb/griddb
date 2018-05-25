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
	@brief Implementation of QueryForCollection
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
* @param collection Collection, to decide index usage
* @param statement TQL statement
* @param limit Limit of the size of result rowset
* @param hook hook for query
*
*/
QueryForCollection::QueryForCollection(TransactionContext &txn,
	Collection &collection, const char *statement, uint64_t limit,
	QueryHookClass *hook)
	: Query(txn, *(collection.getObjectManager()), "", limit, hook),
	  collection_(&collection) {
	if (hook_) {
		hook_->qpBuildBeginHook(*this);
	}
	this->str_ = statement;
	isExplainExecute_ = true;
	explainAllocNum_ = 0;
	explainNum_ = 0;
	isSetError_ = false;
	parseState_ = PARSESTATE_SELECTION;
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

	contractCondition();

	if (hook_) {
		hook_->qpBuildFinishHook(*this);
	}
}

/*!
* @brief Execute Query for Collection
*
*/
void QueryForCollection::doQuery(
	TransactionContext &txn, Collection &collection, ResultSet &resultSet) {
	if (hook_) {
		hook_->qpSearchBeginHook(*this);
	}
	BoolExpr *conditionExpr = getConditionExpr();  
	if (conditionExpr == NULL) {
		doQueryWithoutCondition(txn, collection, resultSet);
	}
	else {
		doQueryWithCondition(txn, collection, resultSet);
	}
	if (doExplain()) {
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

void QueryForCollection::doQueryWithoutCondition(
	TransactionContext &txn, Collection &collection, ResultSet &resultSet) {
	util::XArray<OId> &resultOIdList = *resultSet.getOIdList();
	if (doExplain()) {
		addExplain(0, "SELECTION", "CONDITION", "NULL", "");
		addExplain(1, "INDEX", "BTREE", "ROWMAP", "");
	}
	OutputOrder outputOrder = ORDER_UNDEFINED;  

	BtreeMap::SearchContext sc;

	if (pOrderByExpr_ && pOrderByExpr_->size() == 1) {
		SortExpr &orderExpr = (*pOrderByExpr_)[0];
		uint32_t orderColumnId =
			(orderExpr.expr) ? orderExpr.expr->getColumnId() : UNDEF_COLUMNID;
		if (orderExpr.expr &&
			collection.hasIndex(txn, orderColumnId, MAP_TYPE_BTREE)) {  
			if (doExplain()) {
				addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
				addExplain(1, "QUERY_RESULT_SORT_API_INDEX_ORDER", "STRING",
					(orderExpr.order == ASC) ? "ASC" : "DESC", "");
			}
			outputOrder = (orderExpr.order == ASC) ? ORDER_ASCENDING
												   : ORDER_DESCENDING;  
			sc.columnId_ = orderColumnId;
			sc.keyType_ =
				collection.getColumnInfo(orderColumnId).getColumnType();
			setLimitByIndexSort();
			sc.limit_ = nLimit_;
		}
	}

	if (doExecute()) {
		if (outputOrder != ORDER_UNDEFINED) {
			collection.searchColumnIdIndex(txn, sc, resultOIdList, outputOrder);
		}
		else {
			collection.searchRowIdIndex(txn, sc, resultOIdList, outputOrder);
		}

		if (resultOIdList.size() > nLimit_) {
			ResultSize eraseSize = resultOIdList.size() - nLimit_;
			resultOIdList.erase(
				resultOIdList.begin() + static_cast<size_t>(nLimit_),
				resultOIdList.end());
			if (resultSet.getRowIdList()->size() > 0) {
				ResultSize erasePos =
					resultSet.getRowIdList()->size() - eraseSize;
				resultSet.getRowIdList()->erase(
					resultSet.getRowIdList()->begin() +
						static_cast<size_t>(erasePos),
					resultSet.getRowIdList()->end());
			}
		}
	}
	if (pOrderByExpr_ && (outputOrder == ORDER_UNDEFINED)) {  
		assert(pOrderByExpr_->size() > 0);
		if (doExplain()) {
			util::NormalOStringStream os;
			os.str("");
			addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
			for (size_t i = 0; i < pOrderByExpr_->size(); i++) {
				SortExpr &orderExpr = (*pOrderByExpr_)[i];
				addExplain(1, "QUERY_RESULT_SORT_EXPRESSION", "STRING",
					orderExpr.expr->getValueAsString(txn),
					(orderExpr.order == ASC) ? "ASC" : "DESC");
			}
		}

		CollectionOrderByComparator comp(
			txn, collection, *getFunctionMap(), *pOrderByExpr_);
		std::sort(resultOIdList.begin(), resultOIdList.end(), comp);
	}
}

void QueryForCollection::doQueryWithCondition(
	TransactionContext &txn, Collection &collection, ResultSet &resultSet) {
	util::XArray<OId> &resultOIdList = *resultSet.getOIdList();

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::XArray<BoolExpr *> orList(alloc),  
		andList(alloc);						 

	util::XArray<OId> andOIdArray(
		txn.getDefaultAllocator());  
	util::XArray<OId> operatorOutOIdArray(
		txn.getDefaultAllocator());  
	uint32_t restConditions = 0;	 
	OutputOrder outputOrder = ORDER_UNDEFINED;  

	if (hook_) {
		hook_->qpBuildTmpHook(*this, 5);
	}

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

		if (hook_) {
			hook_->qpBuildTmpHook(*this, 6);
		}
		getIndexInfoInAndList(
			txn, collection, andList, indexType, indexColumnInfo);

		if (hook_) {
			hook_->qpBuildTmpHook(*this, 7);
		}

		if (indexColumnInfo == NULL) {
			BtreeMap::SearchContext sc;
			BoolExpr::toSearchContext(
				txn, andList, NULL, *this, sc, restConditions, nLimit_);

			if (false) {
				SortExpr &orderExpr = (*pOrderByExpr_)[0];
				uint32_t orderColumnId = (orderExpr.expr)
											 ? orderExpr.expr->getColumnId()
											 : UNDEF_COLUMNID;
				if (orderExpr.expr &&
					collection.hasIndex(txn, orderColumnId, MAP_TYPE_BTREE)) {
					if (doExplain()) {
						addExplain(
							0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
						addExplain(1, "QUERY_RESULT_SORT_API_INDEX_ORDER",
							"STRING", (orderExpr.order == ASC) ? "ASC" : "DESC",
							"");
					}
					outputOrder = (orderExpr.order == ASC)
									  ? ORDER_ASCENDING
									  : ORDER_DESCENDING;  
					assert(sc.columnId_ == UNDEF_COLUMNID);
					sc.columnId_ = orderColumnId;
					sc.keyType_ =
						collection.getColumnInfo(orderColumnId).getColumnType();
					pOrderByExpr_->clear();
					setLimitByIndexSort();
				}
			}
			if (doExecute()) {
				if (doExplain()) {
					addExplain(0, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", "");
					addExplain(1, "SEARCH_MAP", "STRING", "ROWMAP", "");
				}
				operatorOutOIdArray.clear();
				if (outputOrder != ORDER_UNDEFINED) {
					collection.searchColumnIdIndex(
						txn, sc, operatorOutOIdArray, outputOrder);
				}
				else {
					collection.searchRowIdIndex(
						txn, sc, operatorOutOIdArray, outputOrder);
				}
			}
		}
		else {
			switch (indexType) {
			case MAP_TYPE_BTREE: {
				assert(indexColumnInfo != NULL);
				if (orList.size() == 1 && pOrderByExpr_ &&
					pOrderByExpr_->size() == 1) {
					SortExpr &orderExpr = (*pOrderByExpr_)[0];
					if (orderExpr.expr &&
						orderExpr.expr->getColumnId() ==
							indexColumnInfo
								->getColumnId()) {  
						if (doExplain()) {
							addExplain(
								0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
							addExplain(1, "QUERY_RESULT_SORT_API_INDEX_ORDER",
								"STRING",
								(orderExpr.order == ASC) ? "ASC" : "DESC", "");
						}
						outputOrder = (orderExpr.order == ASC)
										  ? ORDER_ASCENDING
										  : ORDER_DESCENDING;  
						pOrderByExpr_->clear();
						setLimitByIndexSort();
					}
				}
				BtreeMap::SearchContext sc;
				BoolExpr::toSearchContext(txn, andList, indexColumnInfo, *this,
					sc, restConditions, nLimit_);

				if (doExecute()) {
					if (doExplain()) {
						addExplain(
							1, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", "");
						const char *columnName =
							indexColumnInfo->getColumnName(txn, objectManager_);
						addExplain(2, "SEARCH_MAP", "STRING", columnName, "");
					}
					operatorOutOIdArray.clear();
					collection.searchColumnIdIndex(
						txn, sc, operatorOutOIdArray, outputOrder);
				}
			} break;
			case MAP_TYPE_HASH: {
				HashMap::SearchContext sc;
				BoolExpr::toSearchContext(txn, andList, indexColumnInfo, *this,
					sc, restConditions, nLimit_);
				if (doExecute()) {
					if (doExplain()) {
						addExplain(1, "SEARCH_EXECUTE", "MAP_TYPE", "HASH", "");
						const char *columnName =
							indexColumnInfo->getColumnName(txn, objectManager_);
						addExplain(2, "SEARCH_MAP", "STRING", columnName, "");
					}
					operatorOutOIdArray.clear();
					collection.searchColumnIdIndex(
						txn, sc, operatorOutOIdArray);
				}
			} break;
			default:  
				GS_THROW_SYSTEM_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: Invalid map type specified.");
				break;
			}
		}

		if (hook_) {
			hook_->qpBuildTmpHook(*this, 8);
		}

		if (restConditions == 0) {  
			andOIdArray.assign(
				operatorOutOIdArray.begin(), operatorOutOIdArray.end());
		}
		else {
			uint64_t *pBitmap = reinterpret_cast<uint64_t *>(alloc.allocate(
				sizeof(uint64_t) * ((collection.getColumnNum() / 64) + 1)));

			for (uint32_t i = 0; i < operatorOutOIdArray.size(); i++) {
				bool conditionFlag = true;  
				OId rowId = operatorOutOIdArray[i];
				{  
					util::StackAllocator::Scope scope(alloc);
					CollectionRowWrapper row(txn, collection, rowId, pBitmap);

					for (uint32_t q = 0; q < andList.size(); q++) {
						util::StackAllocator::Scope scope(alloc);
						bool evalResult = andList[q]->eval(
							txn, objectManager_, &row, getFunctionMap(), true);
						if (!evalResult) {
							conditionFlag = false;
							break;
						}
					}
				}
				if (conditionFlag) {
					andOIdArray.push_back(rowId);
				}
			}
			alloc.deallocate(pBitmap);
		}  

		if (doExecute() && doExplain()) {
			util::NormalOStringStream os;
			os << andOIdArray.size();
			addExplain(
				1, "SEARCH_RESULT_ROWS", "INTEGER", os.str().c_str(), "");
		}
		mergeRowIdList<OId>(txn, resultOIdList, andOIdArray);

		if (resultOIdList.size() >= nLimit_) {
			break;
		}
		andOIdArray.clear();
	}  

	if (resultOIdList.size() > nLimit_) {
		resultOIdList.erase(
			resultOIdList.begin() + static_cast<size_t>(nLimit_),
			resultOIdList.end());
	}

	if (pOrderByExpr_ && (outputOrder == ORDER_UNDEFINED)) {  
		assert(pOrderByExpr_->size() > 0);
		if (doExplain()) {
			util::NormalOStringStream os;
			os.str("");
			addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
			for (size_t i = 0; i < pOrderByExpr_->size(); i++) {
				SortExpr &orderExpr = (*pOrderByExpr_)[i];
				addExplain(1, "QUERY_RESULT_SORT_EXPRESSION", "STRING",
					orderExpr.expr->getValueAsString(txn),
					(orderExpr.order == ASC) ? "ASC" : "DESC");
			}
		}
		CollectionOrderByComparator comp(
			txn, collection, *getFunctionMap(), *pOrderByExpr_);
		std::sort(resultOIdList.begin(), resultOIdList.end(), comp);
	}
}

void QueryForCollection::doSelection(
	TransactionContext &txn, Collection &collection, ResultSet &resultSet) {
	ResultType type;
	ResultSize resultNum;
	util::XArray<uint8_t> &serializedRowList =
		*resultSet.getRowDataFixedPartBuffer();
	util::XArray<uint8_t> &serializedVarDataList =
		*resultSet.getRowDataVarPartBuffer();
	util::XArray<OId> &resultOIdList = *resultSet.getOIdList();

	size_t numSelection = getSelectionExprLength();

	if (hook_) {
		hook_->qpBuildTmpHook(*this, 9);
	}

	if (numSelection > 0) {
		Expr *selectionExpr = getSelectionExpr(0);
		if (selectionExpr->isAggregation()) {
			type = RESULT_AGGREGATE;
			Value result;
			bool resultOK = selectionExpr->aggregate(
				txn, collection, resultOIdList, result);
			if (resultOK) {
				result.serialize(serializedRowList);
				resultNum = 1;
			}
			else {
				resultNum = 0;
			}
		}
		else {

			if (strcmp("*", selectionExpr->getValueAsString(txn)) == 0) {
				if (nOffset_ < resultOIdList.size()) {
					type = RESULT_ROW_ID_SET;
					resultOIdList.erase(resultOIdList.begin(),
						resultOIdList.begin() + static_cast<size_t>(nOffset_));

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
								resultSet.getRowIdList()->size() - eraseSize;
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
					collection.getDataStore()->getValueLimitConfig(),
					collection.getColumnInfoList(), collection.getColumnNum(),
					serializedRowList, serializedVarDataList, true);
				type = RESULT_ROWSET;
				bool isDummySorted = false;
				OutputOrder dummyApiOutputOrder = ORDER_UNDEFINED;
				selectionExpr->select(txn, collection, resultOIdList,
					isDummySorted, dummyApiOutputOrder, pOrderByExpr_,
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

QueryForCollection *QueryForCollection::dup(
	TransactionContext &txn, ObjectManager &objectManager) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	QueryForCollection *query =
		QP_ALLOC_NEW(alloc) QueryForCollection(txn_, *collection_);
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
				(*itr)->dup(txn, objectManager));
		}
	}
	else {
		query->pSelectionExprList_ = NULL;
	}
	if (pWhereExpr_ != NULL) {
		query->pWhereExpr_ = pWhereExpr_->dup(txn, objectManager);
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
		query->pLimitExpr_ = pLimitExpr_->dup(txn, objectManager);
	}
	else {
		query->pLimitExpr_ = NULL;
	}
	if (pGroupByExpr_ != NULL) {
		query->pGroupByExpr_ = QP_ALLOC_NEW(alloc) ExprList(alloc);
		for (ExprList::iterator itr = pGroupByExpr_->begin();
			 itr != pGroupByExpr_->end(); itr++) {
			query->pGroupByExpr_->push_back((*itr)->dup(txn, objectManager));
		}
	}
	else {
		query->pGroupByExpr_ = NULL;
	}
	if (pHavingExpr_ != NULL) {
		query->pHavingExpr_ = pHavingExpr_->dup(txn, objectManager);
	}
	else {
		query->pHavingExpr_ = NULL;
	}
	if (pOrderByExpr_ != NULL) {
		query->pOrderByExpr_ = QP_ALLOC_NEW(alloc) SortExprList(alloc);
		for (SortExprList::iterator itr = pOrderByExpr_->begin();
			 itr != pOrderByExpr_->end(); itr++) {
			query->pOrderByExpr_->push_back(*((*itr).dup(txn, objectManager)));
		}
	}
	else {
		query->pOrderByExpr_ = NULL;
	}

	query->functionMap_ = functionMap_;
	query->aggregationMap_ = aggregationMap_;
	query->selectionMap_ = selectionMap_;
	query->specialIdMap_ = specialIdMap_;
	return query;
}
