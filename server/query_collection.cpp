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
#include "key_data_store.h"

#include "meta_store.h"

class QueryForMetaContainer::MessageRowHandler :
		public MetaProcessor::RowHandler {
public:
	MessageRowHandler(
			OutputMessageRowStore &out, BoolExpr *expr,
			ObjectManagerV4 &objectManager, AllocateStrategy &strategy, FunctionMap *functionMap);

	virtual void operator()(
			TransactionContext &txn,
			const MetaProcessor::ValueList &valueList);

private:
	OutputMessageRowStore &out_;
	BoolExpr *expr_;
	ObjectManagerV4 &objectManager_;
	AllocateStrategy &strategy_;
	FunctionMap *functionMap_;
};

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
	Collection &collection, const TQLInfo &tqlInfo, 
	uint64_t limit, QueryHookClass *hook)
	: Query(txn, *(collection.getObjectManager()), collection.getRowAllcateStrategy(), tqlInfo, limit, hook),
	  collection_(&collection) {
	if (hook_) {
		hook_->qpBuildBeginHook(*this);	
	}
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
		pWhereExpr_ = tmp->makeDNF(txn, objectManager_, strategy_);
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
	if (resultSet.getQueryOption().isPartial()) {
		doQueryPartial(txn, collection, resultSet);
	}
	else 
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

	ColumnId searchColumnId = UNDEF_COLUMNID;

	if (isIndexSortAvailable(collection, 0)) {
		SortExpr &orderExpr = (*pOrderByExpr_)[0];
		uint32_t orderColumnId =
			(orderExpr.expr) ? orderExpr.expr->getColumnId() : UNDEF_COLUMNID;
		util::Vector<ColumnId> otherColumnIds(txn.getDefaultAllocator());
		otherColumnIds.push_back(orderColumnId);
		bool withPartialMatch = false;
		if (orderExpr.expr &&
			collection.hasIndex(txn, otherColumnIds, MAP_TYPE_BTREE, withPartialMatch)) {  
			if (doExplain()) {
				addExplain(0, "QUERY_RESULT_SORT", "BOOLEAN", "TRUE", "");
				addExplain(1, "QUERY_RESULT_SORT_API_INDEX_ORDER", "STRING",
					(orderExpr.order == ASC) ? "ASC" : "DESC", "");
			}
			outputOrder = (orderExpr.order == ASC) ? ORDER_ASCENDING
												   : ORDER_DESCENDING;  
			searchColumnId = orderColumnId;
			setLimitByIndexSort();
		}
	}
	BtreeMap::SearchContext sc (txn.getDefaultAllocator(), searchColumnId);
	sc.setLimit(nLimit_);
	if (searchColumnId != UNDEF_COLUMNID) {
		sc.setNullCond(BaseIndex::SearchContext::ALL);
	}
	if (doExecute()) {
		if (outputOrder != ORDER_UNDEFINED) {
			collection.searchColumnIdIndex(txn, sc, resultOIdList, outputOrder);
		}
		else {
			collection.searchRowIdIndex(txn, sc, resultOIdList, outputOrder);
		}

		if (resultOIdList.size() > nLimit_) {
			resultOIdList.erase(
				resultOIdList.begin() + static_cast<size_t>(nLimit_),
				resultOIdList.end());
		}
	}
	if (isSortBeforeSelectClause() && outputOrder == ORDER_UNDEFINED) {
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

		IndexData indexData(txn.getDefaultAllocator());

		if (hook_) {
			hook_->qpBuildTmpHook(*this, 6);	
		}
		bool indexFound = getIndexDataInAndList(
			txn, collection, andList, indexData);

		if (hook_) {
			hook_->qpBuildTmpHook(*this, 7);	
		}

		if (!indexFound) {
			BtreeMap::SearchContext *sc = NULL;
			BoolExpr::toSearchContext(
				txn, andList, NULL, *this, sc, restConditions, nLimit_);

			if (doExecute()) {
				if (doExplain()) {
					addExplain(0, "SEARCH_EXECUTE", "MAP_TYPE", "BTREE", "");
					addExplain(1, "SEARCH_MAP", "STRING", "ROWMAP", "");
				}
				operatorOutOIdArray.clear();
				if (outputOrder != ORDER_UNDEFINED) {
					collection.searchColumnIdIndex(
						txn, *sc, operatorOutOIdArray, outputOrder);
				}
				else {
					collection.searchRowIdIndex(
						txn, *sc, operatorOutOIdArray, outputOrder);
				}
			}
		}
		else {
			assert(!indexData.columnIds_->empty());
			ColumnInfo *indexColumnInfo = &(collection.getColumnInfo((*(indexData.columnIds_))[0]));
			switch (indexData.mapType_) {
			case MAP_TYPE_BTREE: {
				assert(indexColumnInfo != NULL);
				if (isIndexSortAvailable(collection, orList.size())) {
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
						setLimitByIndexSort();
					}
				}
				BtreeMap::SearchContext *sc = NULL;
				BoolExpr::toSearchContext(txn, andList, &indexData, *this,
					sc, restConditions, nLimit_);

				if (doExecute()) {
					if (doExplain()) {
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
									ColumnInfo &columnInfo = collection.getColumnInfo(condList[i].columnId_);
									os << columnInfo.getColumnName(txn, objectManager_, collection.getMetaAllcateStrategy());
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
								ColumnInfo &columnInfo = collection.getColumnInfo((*(indexData.columnIds_))[0]);
								os << columnInfo.getColumnName(txn, objectManager_, collection.getMetaAllcateStrategy());
							}

							addExplain(2, "SEARCH_MAP", "STRING", os.str().c_str(), "");
						}
					}
					operatorOutOIdArray.clear();
					collection.searchColumnIdIndex(
						txn, *sc, operatorOutOIdArray, outputOrder);
				}
			} break;
			case MAP_TYPE_SPATIAL: {
				RtreeMap::SearchContext *sc = NULL;
				BoolExpr::toSearchContext(txn, andList, &indexData, *this,
					sc, restConditions, nLimit_);
				if (doExecute()) {
					if (doExplain()) {
						addExplain(
							1, "SEARCH_EXECUTE", "MAP_TYPE", "SPATIAL", "");
						const char *columnName =
							indexColumnInfo->getColumnName(txn, objectManager_, collection.getMetaAllcateStrategy());
						addExplain(2, "SEARCH_MAP", "STRING", columnName, "");
					}
					operatorOutOIdArray.clear();
					collection.searchColumnIdIndex(
						txn, *sc, operatorOutOIdArray);
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
			CollectionRowWrapper row(txn, collection, pBitmap);
			for (uint32_t i = 0; i < operatorOutOIdArray.size(); i++) {
				bool conditionFlag = true;  
				OId rowId = operatorOutOIdArray[i];
				{  
					util::StackAllocator::Scope scope(alloc);
					row.load(rowId);

					for (uint32_t q = 0; q < andList.size(); q++) {
						util::StackAllocator::Scope scope(alloc);
						TrivalentLogicType evalResult = andList[q]->eval(
							txn, objectManager_, strategy_, &row, getFunctionMap(), TRI_TRUE);
						if (evalResult != TRI_TRUE) {
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

	if (isSortBeforeSelectClause() && outputOrder == ORDER_UNDEFINED) {
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
					txn, collection, resultOIdList, result);
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

			if (strcmp("*", selectionExpr->getValueAsString(txn)) == 0) {
				if (nOffset_ < resultOIdList.size()) {
					type = RESULT_ROW_ID_SET;
					resultOIdList.erase(resultOIdList.begin(),
						resultOIdList.begin() + static_cast<size_t>(nOffset_));

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
					collection.getDataStore()->getConfig(),
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
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
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
	QueryForCollection *query =
		QP_ALLOC_NEW(alloc) QueryForCollection(txn_, *collection_, tqlInfo);
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
				(*itr)->dup(txn, objectManager, strategy));
		}
	}
	else {
		query->pSelectionExprList_ = NULL;
	}
	if (pWhereExpr_ != NULL) {
		query->pWhereExpr_ = pWhereExpr_->dup(txn, objectManager, strategy);
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
		query->pLimitExpr_ = pLimitExpr_->dup(txn, objectManager, strategy);
	}
	else {
		query->pLimitExpr_ = NULL;
	}
	if (pGroupByExpr_ != NULL) {
		query->pGroupByExpr_ = QP_ALLOC_NEW(alloc) ExprList(alloc);
		for (ExprList::iterator itr = pGroupByExpr_->begin();
			 itr != pGroupByExpr_->end(); itr++) {
			query->pGroupByExpr_->push_back((*itr)->dup(txn, objectManager, strategy));
		}
	}
	else {
		query->pGroupByExpr_ = NULL;
	}
	if (pHavingExpr_ != NULL) {
		query->pHavingExpr_ = pHavingExpr_->dup(txn, objectManager, strategy);
	}
	else {
		query->pHavingExpr_ = NULL;
	}
	if (pOrderByExpr_ != NULL) {
		query->pOrderByExpr_ = QP_ALLOC_NEW(alloc) SortExprList(alloc);
		for (SortExprList::iterator itr = pOrderByExpr_->begin();
			 itr != pOrderByExpr_->end(); itr++) {
			query->pOrderByExpr_->push_back(*((*itr).dup(txn, objectManager, strategy)));
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

QueryForMetaContainer::QueryForMetaContainer(
		TransactionContext &txn, MetaContainer &container,
		const TQLInfo &tqlInfo, uint64_t limit, QueryHookClass *hook) :
		Query(txn, *(container.getObjectManager()), container.getRowAllcateStrategy(), tqlInfo, limit, hook),
		container_(container) {

	if (hook_) {
		hook_->qpBuildBeginHook(*this);	
	}
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
		pWhereExpr_ = tmp->makeDNF(txn, objectManager_, strategy_);
		QP_DELETE(tmp);  
	}

	contractCondition();

	if (hook_) {
		hook_->qpBuildFinishHook(*this);	
	}
}

void QueryForMetaContainer::doQuery(
		TransactionContext &txn, MetaProcessorSource &processorSource,
		ResultSet &rs) {
	check(txn);

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	DataStoreV4 *dataStore = container_.getDataStore();

	util::XArray<ColumnInfo> columnInfoList(alloc);
	container_.getColumnInfoList(columnInfoList);

	const uint32_t columnCount = container_.getColumnNum();
	OutputMessageRowStore out(
			dataStore->getConfig(),
			columnInfoList.data(), columnCount,
			*rs.getRSRowDataFixedPartBuffer(),
			*rs.getRSRowDataVarPartBuffer(), true);

	const MetaContainerInfo &info = container_.getMetaContainerInfo();
	MessageRowHandler handler(
			out, pWhereExpr_, *container_.getObjectManager(), container_.getRowAllcateStrategy(),
			getFunctionMap());
	MetaProcessor proc(txn, info.id_, info.forCore_);

	const TransactionManager *txnMgr =
			processorSource.transactionManager_;
	const PartitionId partitionCount  =
			txnMgr->getPartitionGroupConfig().getPartitionCount();

	bool reduced;
	PartitionId reducedPartitionId;
	const FullContainerKey *containerKey = predicateToContainerKey(
			txn, *dataStore, processorSource.dbId_, pWhereExpr_, info,
			NULL, partitionCount, reduced, reducedPartitionId);
	proc.setContainerKey(containerKey);

	if (!reduced || containerKey != NULL) {
		proc.scan(txn, processorSource, handler);
	}

	rs.getRowDataFixedPartBuffer()->assign(
			rs.getRSRowDataFixedPartBuffer()->begin(),
			rs.getRSRowDataFixedPartBuffer()->end());
	rs.getRowDataVarPartBuffer()->assign(
			rs.getRSRowDataVarPartBuffer()->begin(),
			rs.getRSRowDataVarPartBuffer()->end());

	const ResultSize resultNum = out.getRowCount();

	rs.setDataPos(
			0, static_cast<uint32_t>(rs.getRowDataFixedPartBuffer()->size()),
			0, static_cast<uint32_t>(rs.getRowDataVarPartBuffer()->size()));

	rs.setResultType(RESULT_ROWSET, resultNum, true);
	rs.setFetchNum(resultNum);

	if (reduced) {
		const bool uncovered = false;
		rs.setDistributedTargetStatus(uncovered, reduced);

		util::Vector<int64_t> *distTarget = rs.getDistributedTarget();
		if (reducedPartitionId != UNDEF_PARTITIONID) {
			distTarget->push_back(reducedPartitionId);
		}
	}
}

Collection* QueryForMetaContainer::getCollection() {
	BaseContainer &base = container_;
	return static_cast<Collection*>(&base);
}

TimeSeries* QueryForMetaContainer::getTimeSeries() {
	BaseContainer &base = container_;
	return static_cast<TimeSeries*>(&base);
}

void QueryForMetaContainer::check(TransactionContext &txn) {
	if (nLimit_  < static_cast<ResultSize>(std::numeric_limits<int64_t>::max())) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
				"Fetch limit option is not implemented for meta container ("
				"limit=" << nLimit_ << ")");
	}

	if (pLimitExpr_) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
				"LIMIT is not implemented for meta container");
	}

	if (pOrderByExpr_) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
				"ORDER BY is not implemented for meta container");
	}

	if (doExplain()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
				"EXPLAIN is not implemented for meta container");
	}

	size_t numSelection = getSelectionExprLength();
	if (numSelection <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: no selection expression specified.");
	}

	Expr *selectionExpr = getSelectionExpr(0);
	if (selectionExpr->isAggregation()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
				"Aggregation is not implemented for meta container");
	}

	if (strcmp("*", selectionExpr->getValueAsString(txn)) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
				"Complex selection is not implemented for meta container");
	}
}

FullContainerKey* QueryForMetaContainer::predicateToContainerKey(
		TransactionContext &txn, DataStoreV4 &dataStore, DatabaseId dbId,
		const BoolExpr *expr, const MetaContainerInfo &metaInfo,
		const MetaContainerInfo *coreMetaInfo, PartitionId partitionCount,
		bool &reduced, PartitionId &reducedPartitionId) {
	reduced = false;
	reducedPartitionId = UNDEF_PARTITIONID;

	if (coreMetaInfo == NULL) {
		const MetaContainerInfo *resolvedInfo;
		if (metaInfo.forCore_) {
			resolvedInfo = &metaInfo;
		}
		else {
			const bool forCore = true;
			const MetaType::InfoTable &infoTable =
					MetaType::InfoTable::getInstance();
			resolvedInfo = &infoTable.resolveInfo(metaInfo.id_, forCore);
		}
		return predicateToContainerKey(
				txn, dataStore, dbId, expr, metaInfo, resolvedInfo,
				partitionCount, reduced, reducedPartitionId);
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	const MetaContainerInfo::CommonColumnInfo &commonInfo =
			coreMetaInfo->commonInfo_;

	FullContainerKey *containerKey = NULL;
	if (commonInfo.containerNameColumn_ != UNDEF_COLUMNID) {
		util::String containerName(alloc);
		if (!predicateToContainerName(
				expr, metaInfo, *coreMetaInfo, containerName)) {
			return NULL;
		}
		try {
			containerKey = ALLOC_NEW(alloc) FullContainerKey(
					alloc, KeyConstraint::getNoLimitKeyConstraint(), dbId,
					containerName.c_str(), containerName.size());
		}
		catch (...) {
		}
	}
	else if (commonInfo.containerIdColumn_ != UNDEF_COLUMNID) {
		int64_t partitionId = -1;
		int64_t containerId = -1;
		if (!predicateToContainerId(
				expr, metaInfo, *coreMetaInfo, partitionId, containerId)) {
			return NULL;
		}

		if (partitionId != static_cast<int64_t>(txn.getPartitionId())) {
			reduced = true;
			if (partitionId >= 0 &&
					partitionId < static_cast<int64_t>(partitionCount)) {
				reducedPartitionId = static_cast<PartitionId>(partitionId);
			}
			return NULL;
		}
		try {
			KeyDataStoreValue keyStoreValue = dataStore.getKeyDataStore()->get(alloc, static_cast<ContainerId>(containerId));
			ContainerAutoPtr containerAutoPtr(txn, &dataStore, keyStoreValue.oId_, ANY_CONTAINER, false);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			if (container == NULL) {
				return NULL;
			}
			containerKey = ALLOC_NEW(alloc) FullContainerKey(
					container->getContainerKey(txn));
		}
		catch (...) {
		}
	}

	reduced = (containerKey != NULL);
	if (reduced) {
		const ContainerHashMode hashMode = CONTAINER_HASH_MODE_CRC32;

		reducedPartitionId = KeyDataStore::resolvePartitionId(
				alloc, *containerKey, partitionCount, hashMode);
	}

	return containerKey;
}

bool QueryForMetaContainer::predicateToContainerName(
		const Expr *expr, const MetaContainerInfo &metaInfo,
		const MetaContainerInfo &coreMetaInfo, util::String &containerName) {
	if (expr == NULL) {
		return false;
	}

	const BoolExpr *boolExpr = findBoolExpr(*expr);
	if (boolExpr != NULL) {
		const BoolExpr::BoolTerms *andOperands = findAndOperands(*boolExpr);
		if (andOperands != NULL) {
			for (BoolExpr::BoolTerms::const_iterator it = andOperands->begin();
					it != andOperands->end(); ++it) {
				if (predicateToContainerName(
						*it, metaInfo, coreMetaInfo, containerName)) {
					return true;
				}
			}
			return false;
		}

		return predicateToContainerName(
				findUnary(*boolExpr), metaInfo, coreMetaInfo, containerName);
	}

	uint32_t baseColumnId;
	const Value *value;
	if (findEqCond(*expr, baseColumnId, value)) {
		const uint32_t columnId =
				getCoreColumnId(baseColumnId, metaInfo, coreMetaInfo);
		const MetaContainerInfo::CommonColumnInfo &commonInfo =
				coreMetaInfo.commonInfo_;

		if (columnId != commonInfo.containerNameColumn_) {
			return false;
		}

		if (value->getType() != COLUMN_TYPE_STRING) {
			return false;
		}

		containerName.assign(
				reinterpret_cast<const char8_t*>(value->data()), value->size());
		return true;
	}

	return false;
}

bool QueryForMetaContainer::predicateToContainerId(
		const Expr *expr, const MetaContainerInfo &metaInfo,
		const MetaContainerInfo &coreMetaInfo, int64_t &partitionId,
		int64_t &containerId) {
	if (expr == NULL) {
		return false;
	}

	const BoolExpr *boolExpr = findBoolExpr(*expr);
	if (boolExpr != NULL) {
		const BoolExpr::BoolTerms *andOperands = findAndOperands(*boolExpr);
		if (andOperands != NULL) {
			for (BoolExpr::BoolTerms::const_iterator it = andOperands->begin();
					it != andOperands->end(); ++it) {
				if (predicateToContainerId(
						*it, metaInfo, coreMetaInfo, partitionId,
						containerId)) {
					return true;
				}
			}
			return false;
		}

		return predicateToContainerId(
				findUnary(*boolExpr), metaInfo, coreMetaInfo, partitionId,
				containerId);
	}

	uint32_t baseColumnId;
	const Value *value;
	if (findEqCond(*expr, baseColumnId, value)) {
		const uint32_t columnId =
				getCoreColumnId(baseColumnId, metaInfo, coreMetaInfo);
		const MetaContainerInfo::CommonColumnInfo &commonInfo =
				coreMetaInfo.commonInfo_;

		int64_t *condValueRef;
		if (columnId == commonInfo.partitionIndexColumn_) {
			condValueRef = &partitionId;
		}
		else if (columnId == commonInfo.containerIdColumn_) {
			condValueRef = &containerId;
		}
		else {
			return false;
		}

		if (!value->isNumerical()) {
			return false;
		}

		*condValueRef = value->getLong();
		return (partitionId != -1 && containerId != -1);
	}

	return false;
}

uint32_t QueryForMetaContainer::getCoreColumnId(
		uint32_t columnId, const MetaContainerInfo &metaInfo,
		const MetaContainerInfo &coreMetaInfo) {
	if (columnId >= metaInfo.columnCount_) {
		assert(false);
		return UNDEF_COLUMNID;
	}

	const ColumnId refId = metaInfo.columnList_[columnId].refId_;
	if (refId == UNDEF_COLUMNID) {
		assert(metaInfo.forCore_);
		return columnId;
	}

	if (refId >= coreMetaInfo.columnCount_) {
		assert(false);
		return UNDEF_COLUMNID;
	}

	return refId;
}

const BoolExpr* QueryForMetaContainer::findBoolExpr(const Expr &expr) {
	if (ExprAccessor::getType(expr) != Expr::BOOL_EXPR) {
		return NULL;
	}

	return static_cast<const BoolExpr*>(&expr);
}

const BoolExpr::BoolTerms* QueryForMetaContainer::findAndOperands(
		const BoolExpr &expr) {
	if (BoolExprAccessor::getOpType(expr) != BoolExpr::AND) {
		return NULL;
	}

	return &BoolExprAccessor::getOperands(expr);
}

const Expr* QueryForMetaContainer::findUnary(const BoolExpr &expr) {
	if (BoolExprAccessor::getOpType(expr) != BoolExpr::UNARY) {
		return NULL;
	}

	return BoolExprAccessor::getUnary(expr);
}

bool QueryForMetaContainer::findEqCond(
		const Expr &expr, uint32_t &columnId, const Value *&value) {
	if (ExprAccessor::getType(expr) != Expr::EXPR) {
		return false;
	}

	if (ExprAccessor::getOp(expr) != Expr::EQ) {
		return false;
	}
	const ExprList *argList = ExprAccessor::getArgList(expr);
	if (argList == NULL || argList->size() != 2) {
		return false;
	}

	const Expr *columnExpr = (*argList)[0];
	const Expr *valueExpr = (*argList)[1];
	if (ExprAccessor::getType(*columnExpr) != Expr::COLUMN) {
		std::swap(columnExpr, valueExpr);
	}
	if (ExprAccessor::getType(*columnExpr) != Expr::COLUMN ||
			ExprAccessor::getType(*valueExpr) != Expr::VALUE) {
		return false;
	}

	value = ExprAccessor::getValue(*valueExpr);
	if (value == NULL) {
		return false;
	}

	columnId = ExprAccessor::getColumnId(*columnExpr);
	return true;
}

QueryForMetaContainer::MessageRowHandler::MessageRowHandler(
		OutputMessageRowStore &out, BoolExpr *expr,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, FunctionMap *functionMap) :
		out_(out),
		expr_(expr),
		objectManager_(objectManager),
		strategy_(strategy),
		functionMap_(functionMap) {
}

void QueryForMetaContainer::MessageRowHandler::operator()(
		TransactionContext &txn,
		const MetaProcessor::ValueList &valueList) {

	if (expr_ != NULL) {
		MetaContainerRowWrapper row(valueList);
		const TrivalentLogicType evalResult = expr_->eval(
				txn, objectManager_, strategy_, &row, functionMap_, TRI_TRUE);
		if (evalResult != TRI_TRUE) {
			return;
		}
	}

	const uint32_t count = out_.getColumnCount();
	assert(count == valueList.size());

	out_.beginRow();
	out_.setRowId(out_.getRowCount());
	for (uint32_t i = 0; i < count; i++) {
		const Value &value = valueList[i];
		assert(value.isNullValue() || value.getType() ==
				out_.getColumnInfoList()[i].getColumnType());

		if (value.isNullValue()) {
			out_.setNull(i);
		}
		else if (value.getType() == COLUMN_TYPE_STRING) {
			const uint32_t size = value.size();
			const void *addr =
					value.data() - ValueProcessor::getEncodedVarSize(size);
			out_.setField(i, addr, size);
		}
		else if (value.getType() == COLUMN_TYPE_BOOL) {
			out_.setField<COLUMN_TYPE_BOOL>(i, value.getBool());
		}
		else {
			out_.setField(i, value);
		}
	}
	out_.next();
}

MetaContainerRowWrapper::MetaContainerRowWrapper(
		const ValueList &valueList) :
		valueList_(valueList) {
}

const Value* MetaContainerRowWrapper::getColumn(uint32_t columnId) {
	return &valueList_[columnId];
}

void MetaContainerRowWrapper::load(OId oId) {
	static_cast<void>(oId);
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
}

RowId MetaContainerRowWrapper::getRowId() {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
}

void MetaContainerRowWrapper::getImage(
		TransactionContext &txn, MessageRowStore *messageRowStore,
		bool isWithRowId) {
	static_cast<void>(txn);
	static_cast<void>(messageRowStore);
	static_cast<void>(isWithRowId);
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
}
