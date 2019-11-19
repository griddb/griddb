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
	@brief Implementation of Query
*/

#include "transaction_context.h"
#include "gis_generator.h"
#include "gis_geomfromtext.h"
#include "collection.h"
#include "qp_def.h"
#include "query.h"
#include "time_series.h"

#include "lexer.h"
#include "schema.h"
#include "tql.h"
#include "value_processor.h"

#include "boolean_expression.h"
#include "result_set.h"

#include "meta_store.h"

bool Query::sIsTrace_ = false;

/*!
* @brief Query Constructor (for debug and internal use only)
*
* @param txn The transaction context
* @param txn Object manager
* @param str TQL string
* @param limit Limit of the size of result rowset
* @param hook hook for query
*
*/
Query::Query(TransactionContext &txn, ObjectManager &objectManager,
	const TQLInfo &tqlInfo, uint64_t limit, QueryHookClass *hook)
	: tqlInfo_(tqlInfo),
	  pErrorMsg_(NULL),
	  pSelectionExprList_(NULL),
	  pWhereExpr_(NULL),
	  pFromCollectionName_(NULL),
	  txn_(txn),
	  objectManager_(objectManager),
	  nResultSorted_(0),
	  hook_(hook),
	  isDistinct_(false),
	  nLimit_(limit),
	  pLimitExpr_(NULL),
	  pGroupByExpr_(NULL),
	  pHavingExpr_(NULL),
	  pOrderByExpr_(NULL) {
	if (util::stricmp(tqlInfo.query_, "") == 0) return;  

	if (hook_) {
		hook_->qpBuildBeginHook(*this);
	}

	isSetError_ = false;
	isExplainExecute_ = true;
	explainAllocNum_ = 0;
	explainNum_ = 0;
	setDefaultFunctionMap();
	parseState_ = PARSESTATE_SELECTION;
}

/*!
* @brief Query Destructor
*/
Query::~Query() {
	if (pSelectionExprList_) {
		for (ExprList::iterator it = pSelectionExprList_->begin();
			 it != pSelectionExprList_->end(); it++) {
			QP_SAFE_DELETE(*it);
		}
	}
	QP_SAFE_DELETE(pSelectionExprList_);
	QP_SAFE_DELETE(pWhereExpr_);
	QP_SAFE_DELETE(pErrorMsg_);
}

/*!
 * @brief Get condition expression (Where clause)
 * @return The root node of condition expression
 */
BoolExpr *Query::getConditionExpr() const {
	return pWhereExpr_;
}

/*!
 * @brief Get selection expression list's entry (not used)
 * @return expression
 */
Expr *Query::getSelectionExpr(size_t x) const {
	if (pSelectionExprList_ == NULL) {
		return NULL;
	}
	if ((*pSelectionExprList_).empty()) {
		return NULL;
	}
	if ((*pSelectionExprList_).size() <= x) {
		return NULL;
	}
	return (*pSelectionExprList_)[x];
}

/*!
 * @brief Get selection expression list's length (not used)
 * @return
 */
size_t Query::getSelectionExprLength() const {
	if (pSelectionExprList_ == NULL) {
		return 0;
	}
	if ((*pSelectionExprList_).empty()) {
		return 0;
	}
	return (*pSelectionExprList_).size();
}

/*!
 * @brief return list of conjunctive clauses
 */
void Query::getConditionAsOrList(util::XArray<BoolExpr *> &orList) {
	this->pWhereExpr_->toOrList(orList);
}

MetaContainer* Query::getMetaContainer(
		Collection &collection, TimeSeries &timeSeries) {
	BaseContainer *base1 = &collection;
	BaseContainer *base2 = &timeSeries;
	if (base1 != base2) {
		return NULL;
	}
	return static_cast<MetaContainer*>(base1);
}

/*!
* @brief Interface to report a parser error
*
* @param errMsg error message.
*/
void Query::setError(const char *errMsg) {
	QP_SAFE_DELETE(pWhereExpr_);
	pErrorMsg_ = QP_NEW_BY_TXN(txn_) util::String(errMsg, QP_ALLOCATOR_);
	isSetError_ = true;
}

/*!
* @brief Set the selection expression
*
* @param pExprList selection expression
*/
void Query::setSelectionExpr(ExprList *pExprList) {
	if (pSelectionExprList_) {
		for (ExprList::iterator it = pSelectionExprList_->begin();
			 it != pSelectionExprList_->end(); it++) {
			QP_DELETE(*it);
		}
		QP_SAFE_DELETE(pSelectionExprList_);
	}
	pSelectionExprList_ = pExprList;
	for (ExprList::iterator it = pSelectionExprList_->begin();
		 it != pSelectionExprList_->end(); it++) {
		QP_DELETE(*it);
	}
}

/*!
* @brief Set the search-condition expression
*
* @param e search condition
* @param txn The transaction context
*/
void Query::setConditionExpr(Expr *e, TransactionContext &txn) {
	QP_SAFE_DELETE(pWhereExpr_);
	pWhereExpr_ = dynamic_cast<BoolExpr *>(e);
	if (pWhereExpr_ == NULL) {
		pWhereExpr_ = QP_NEW BoolExpr(e, txn);
	}
}

/*!
* @brief Print selection expression
*
* @param txn The transaction context
* @param os stream to print
* @param x map to assign symbols in evaluation
*/
void Query::dumpSelectionExpr(
	TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x) {
	if (pSelectionExprList_) {
		for (size_t i = 0; i < pSelectionExprList_->size(); i++) {
			Expr *e = (*pSelectionExprList_)[i]->eval(
				txn_, objectManager_, x, NULL, EVAL_MODE_PRINT);
			os << "\tSELECTION_EXPR[" << i << "]=" << e->getValueAsString(txn)
			   << std::endl;
			QP_DELETE(e);
		}
	}
}

/*!
* @brief Evaluate condition expression
*
* @param x map to assign symbols
*/
void Query::evalConditionExpr(ContainerRowWrapper *x) {
	if (pWhereExpr_ != NULL) {
		std::cout << "EVAL_RESULT="
				  << pWhereExpr_->eval(
						 txn_, objectManager_, x, functionMap_, TRI_TRUE)
				  << std::endl;
	}
}

/*!
* @brief Print search-condition expression
*
* @param txn The transaction context
* @param os stream to print
* @param x map to assign symbols in evaluation
*/
void Query::dumpConditionExpr(
	TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x) {
	if (pWhereExpr_) {
		pWhereExpr_->dumpTree(txn, objectManager_, os, x, functionMap_);
		os << std::endl;
		try {
			os << "EVAL_RESULT="
			   << pWhereExpr_->eval(txn, objectManager_, x, functionMap_, TRI_TRUE)

			   << std::endl;
		}
		catch (util::Exception &e) {
			e.format(os);
			os << "EVAL_RESULT=UNKNOWN" << std::endl;
		}
	}
}

/*!
* @brief Print search-condition expression after
* disjunctive-normal form (DNF) transformation
*
* @param txn The transaction context
* @param os stream to print
* @param x map to assign symbols in evaluation
*/
void Query::dumpConditionExprDNF(
	TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x) {
	if (pWhereExpr_) {
		BoolExpr *e = pWhereExpr_->makeDNF(txn, objectManager_);
		e->dumpTree(txn, objectManager_, os, x, functionMap_);
		os << std::endl;
		try {
			os << "EVAL_RESULT="
			   << e->eval(txn, objectManager_, x, functionMap_, TRI_TRUE)
			   << std::endl;
		}
		catch (util::Exception &e) {
			e.format(os);
			os << "EVAL_RESULT=UNKNOWN" << std::endl;
		}
		QP_DELETE(e);
	}
}

/*!
* @brief Print search-condition expression after
* query-level optimization
*
* @param txn The transaction context
* @param os stream to print
* @param x map to assign symbols in evaluation
*/
void Query::dumpConditionExprOptimized(
	TransactionContext &txn, std::ostream &os, ContainerRowWrapper *x) {
	if (pWhereExpr_) {
		BoolExpr *e = pWhereExpr_->makeOptimizedExpr(
			txn, objectManager_, x, functionMap_);
		e->dumpTree(txn, objectManager_, os, x, functionMap_);
		os << std::endl;
		try {
			os << "EVAL_RESULT="
			   << e->eval(txn, objectManager_, x, functionMap_, TRI_TRUE)
			   << std::endl;
		}
		catch (util::Exception &e) {
			e.format(os);
			os << "EVAL_RESULT=UNKNOWN" << std::endl;
		}
		QP_DELETE(e);
	}
}

/*!
* @brief Print select options
*
* @param txn The transaction context
* @param os stream to print
*/
void Query::dumpSelectOptions(TransactionContext &txn, std::ostream &os) {
	if (isDistinct_) {
		os << "\tDISTINCT" << std::endl;
	}
	if (pGroupByExpr_ != NULL) {
		for (size_t i = 0; i < pGroupByExpr_->size(); i++) {
			Expr *e = (*pGroupByExpr_)[i]->eval(
				txn_, objectManager_, NULL, NULL, EVAL_MODE_PRINT);
			std::cout << "\tGROUP BY[" << i << "]: " << e->getValueAsString(txn)
					  << std::endl;
			QP_DELETE(e);
		}
	}
	if (pHavingExpr_ != NULL) {
		Expr *e = pHavingExpr_->eval(
			txn_, objectManager_, NULL, NULL, EVAL_MODE_PRINT);
		std::cout << "\tHAVING: " << e->getValueAsString(txn) << std::endl;
		QP_DELETE(e);
	}
	if (pOrderByExpr_ != NULL) {
		for (size_t i = 0; i < pOrderByExpr_->size(); i++) {
			Expr *e = (*pOrderByExpr_)[i].expr->eval(
				txn_, objectManager_, NULL, NULL, EVAL_MODE_PRINT);
			std::cout << "\tORDER BY[" << i
					  << "]: " << e->getValueAsString(txn);
			if ((*pOrderByExpr_)[i].order == DESC) {
				os << " DESC";
			}
			os << std::endl;
			QP_DELETE(e);
		}
	}
	if (nLimit_ != MAX_RESULT_SIZE) {
		os << "\tLIMIT:" << nLimit_ << ", OFFSET:" << nOffset_ << std::endl;
	}
}

/*!
 * @brief Set collections to search FROM
 *
 * @param name1 DB name
 * @param txn The transaction context
*/
void Query::setFromCollection(Token &name1, Token &name2, TransactionContext &txn) {
	const Token *dbNameToken = NULL;
	const Token *containerNameToken = &name1;
	if (name2.n != 0) {
		dbNameToken = &name1;
		containerNameToken = &name2;

		char *dbStr = static_cast<char *>(txn.getDefaultAllocator().allocate(dbNameToken->n + 1));
		memcpy(dbStr, dbNameToken->z, dbNameToken->n + 1);
		dbStr[dbNameToken->n] = '\0';

		bool isQuoted = false;
		const char *dbName = Expr::dequote(txn.getDefaultAllocator(), dbStr, isQuoted);

		bool isExist = 
			eqCaseStringString(txn_, dbName,
				static_cast<uint32_t>(strlen(dbName)),
				tqlInfo_.dbName_, static_cast<uint32_t>(strlen(tqlInfo_.dbName_)), isQuoted);
		if (!isExist) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INVALID_COLLECTION_NAME,
				"Database names given to API and specified in FROM clause "
				"are not equal.");
		}
	}
	const char *collectionName;
	const char *pCollectionName = NULL;
	uint32_t collectionNameLen = 0;
	char *str;
	util::String containerName(txn.getDefaultAllocator());
	if (tqlInfo_.containerKey_ != NULL) {
		tqlInfo_.containerKey_->toString(txn.getDefaultAllocator(), containerName);
		pCollectionName = containerName.c_str();
		collectionNameLen = static_cast<uint32_t>(strlen(pCollectionName));
	}
	else if (getMetaContainer() != NULL) {
		const FullContainerKey containerKey = getMetaContainer()->getContainerKey(txn);
		containerKey.toString(txn.getDefaultAllocator(), containerName);
		pCollectionName = containerName.c_str();
		collectionNameLen = static_cast<uint32_t>(strlen(pCollectionName));
	}
	else if (getCollection() != NULL) {
		const FullContainerKey containerKey = getCollection()->getContainerKey(txn);
		containerKey.toString(txn.getDefaultAllocator(), containerName);
		pCollectionName = containerName.c_str();
		collectionNameLen = static_cast<uint32_t>(strlen(pCollectionName));
	}
	else if (getTimeSeries() != NULL) {
		const FullContainerKey containerKey = getTimeSeries()->getContainerKey(txn);
		containerKey.toString(txn.getDefaultAllocator(), containerName);
		pCollectionName = containerName.c_str();
		collectionNameLen = static_cast<uint32_t>(strlen(pCollectionName));
	}
	else {
	}

	str = static_cast<char *>(txn.getDefaultAllocator().allocate(containerNameToken->n + 1));
	memcpy(str, containerNameToken->z, containerNameToken->n + 1);
	str[containerNameToken->n] = '\0';

	bool isQuoted = false;
	collectionName = Expr::dequote(txn.getDefaultAllocator(), str, isQuoted);
	if (pCollectionName) {
		bool isExist = 
			eqCaseStringString(txn_, collectionName,
				static_cast<uint32_t>(strlen(collectionName)),
				pCollectionName, collectionNameLen, isQuoted);
		if (!isExist) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INVALID_COLLECTION_NAME,
				"Collection names given to API and specified in FROM clause "
				"are not equal.");
		}
	}
	pFromCollectionName_ = str;

	if (doExplain()) {
		addExplain(0, "CONTAINER", "STRING", pFromCollectionName_, "NULL");
	}
}

/*!
* @brief Set flag to output trace string
*
* @param t flag
*/
void Query::setTraceMode(bool t) {
	sIsTrace_ = t;
}

/*!
* @brief Control pragma options
*
* @param pName1 pragma name
* @param pName2 pragma name 2
* @param pValue pragma value
*/
void Query::setPragma(Token *pName1, Token *pName2, Token *pValue, int) {
	util::String pragmaName1, pragmaName2, pragmaValue;
	if (pName1 != NULL) {
		pragmaName1 = util::String(pName1->z, pName1->n, QP_ALLOCATOR_);
	}
	if (pName2 != NULL) {
		pragmaName2 = util::String(pName2->z, pName2->n, QP_ALLOCATOR_);
	}
	if (pValue != NULL) {
		pragmaValue = util::String(pValue->z, pValue->n, QP_ALLOCATOR_);
	}

	if (util::stricmp(pragmaName1.c_str(), "trace") == 0) {
		sIsTrace_ = (util::stricmp(pragmaValue.c_str(), "on") == 0);
	}
}

/*!
* @brief Index planner
*
* @param[in] txn The transaction context
* @param[in] container Collection/TimeSeries
* @param[in] andList AND-connected Expressions
* @param[out] mapType returns mapType if possible.
* @param[out] indexColumnInfo returns ColumnInfo to use its index. returns NULL
* if we cannot use any index.
*/
bool Query::getIndexDataInAndList(TransactionContext &txn,
	BaseContainer &container, util::XArray<BoolExpr *> &andList,
	IndexData &indexData) {
	MapType mapType;
	ColumnInfo *indexColumnInfo;
	uint32_t mapBitmap = 0;
	indexColumnInfo = NULL;
	for (uint32_t i = 0; i < andList.size(); i++) {
		andList[i]->getIndexBitmapAndInfo(
			txn, container, *this, mapBitmap, indexColumnInfo);

		if (mapBitmap != 0) {
			for (int k = 3; k >= 0; k--) {
				if (((1 << k) & mapBitmap) != 0) {
					const char *str = "";
					switch (k) {
					case 2:
						mapType = MAP_TYPE_SPATIAL;
						str = "SPATIAL";
						break;
					case 1:
						mapType = MAP_TYPE_HASH;
						str = "HASH";
						break;
					case 0:
					default:
						mapType = MAP_TYPE_BTREE;
						str = "BTREE";
						break;
					}
					this->addExplain(1, "USE_INDEX", "STRING", str, "");
					{
						util::Vector<ColumnId> columnIds(txn.getDefaultAllocator());
						columnIds.push_back(indexColumnInfo->getColumnId());
						bool withPartialMatch = true;
						util::Vector<IndexData> indexDataList(txn.getDefaultAllocator());
						container.getIndexDataList(txn, mapType, columnIds,
							indexDataList, withPartialMatch);
						assert(!indexDataList.empty());
						indexData = indexDataList[0];
						if (indexDataList.size() > 1) {
							uint32_t maxConditionNum = 0;
							util::Vector<IndexData>::iterator dataItr;
							for (dataItr = indexDataList.begin(); dataItr != indexDataList.end(); dataItr++) {
								uint32_t restConditions = 0;
								BtreeMap::SearchContext *sc = NULL;
								if (container.getContainerType() == COLLECTION_CONTAINER) {
									BoolExpr::toSearchContext(txn, andList,
										&(*dataItr), *static_cast<QueryForCollection*>(this), 
										sc, restConditions,  MAX_RESULT_SIZE);
								} else {
									Timestamp dummy = 0;
									BoolExpr::toSearchContext(txn, andList, dummy,
										&(*dataItr), *static_cast<QueryForTimeSeries*>(this), 
										sc, restConditions,  MAX_RESULT_SIZE);
								}
								uint32_t indexConditionNum = sc->getRangeConditionNum();
								if (maxConditionNum < indexConditionNum) {
									indexData = *dataItr;
									maxConditionNum = indexConditionNum;
								} else if (maxConditionNum == indexConditionNum && 
									indexData.columnIds_->size() > dataItr->columnIds_->size()) {
									indexData = *dataItr;
									maxConditionNum = indexConditionNum;
								}
							}
						}
					}
					return true;
				}
			}
		}
	}
	return false;
}

#include "function_array.h"
#include "function_float.h"
#include "function_string.h"
#include "function_timestamp.h"
/*!
* @brief Set default function map
*
*/
void Query::setDefaultFunctionMap() {
	functionMap_ = FunctionMap::getInstance();
	aggregationMap_ = AggregationMap::getInstance();
	selectionMap_ = SelectionMap::getInstance();
	specialIdMap_ = SpecialIdMap::getInstance();
}

/*!
 * @brief Enable explain query plan
 *
 * @param doExecute true when EXPLAIN STAT is queried.
 *                    execute query and statictics is outputed.
 *                  false when EXPLAIN only.
 */
void Query::enableExplain(bool doExecute) {
	explainNum_ = 0;
	explainAllocNum_ = INITIAL_EXPLAIN_DATA_NUM;
	explainData_ =
		reinterpret_cast<ExplainData *>(txn_.getDefaultAllocator().allocate(
			sizeof(struct ExplainData) * explainAllocNum_));
	explainData_ =
		new (explainData_) ExplainData[explainAllocNum_];  
	isExplainExecute_ = doExecute;
}

/*!
 * @brief Add explain data
 *
 * @param depth If not 0, an explain entry above relates to this entry
 * @param exp_type explain Type
 * @param value_type Value type
 * @param value_string Explain value
 * @param statement The related statement
 */
void Query::addExplain(uint32_t depth, const char *exp_type,
	const char *value_type, const char *value_string, const char *statement) {
	if (explainAllocNum_ > 0) {
		TransactionContext &txn = txn_;
		ExplainData &e = explainData_[explainNum_];
		e.id = explainNum_++;
		e.depth = depth;
		e.exp_type = QP_NEW util::String(exp_type, txn_.getDefaultAllocator());
		e.value_type =
			QP_NEW util::String(value_type, txn_.getDefaultAllocator());
		e.value_string =
			QP_NEW util::String(value_string, txn_.getDefaultAllocator());
		e.statement =
			QP_NEW util::String(statement, txn_.getDefaultAllocator());
		if (explainAllocNum_ <= explainNum_) {
			ExplainData *next_ = reinterpret_cast<ExplainData *>(
				txn_.getDefaultAllocator().allocate(
					sizeof(struct ExplainData) * explainAllocNum_ * 2));
			memcpy(next_, explainData_,
				sizeof(struct ExplainData) * explainAllocNum_);
			explainData_ = next_;
			explainAllocNum_ *= 2;
		}
	}
}

void Query::serializeExplainData(TransactionContext &txn, uint64_t &resultNum,
	MessageRowStore *messageRowStore) {
	assert(explainNum_ > 0);
	for (unsigned int i = 0; i < explainNum_; i++) {
		messageRowStore->beginRow();
		Value v;
		util::XArray<uint8_t> buf(txn.getDefaultAllocator());
		ColumnId columnId = 0;
		v.set(static_cast<int32_t>(explainData_[i].id));
		v.get(txn, objectManager_, messageRowStore, columnId++);
		v.set(static_cast<int32_t>(explainData_[i].depth));
		v.get(txn, objectManager_, messageRowStore, columnId++);
		v.set(txn.getDefaultAllocator(),
			const_cast<char *>(explainData_[i].exp_type->c_str()));
		v.get(txn, objectManager_, messageRowStore, columnId++);
		v.set(txn.getDefaultAllocator(),
			const_cast<char *>(explainData_[i].value_type->c_str()));
		v.get(txn, objectManager_, messageRowStore, columnId++);
		v.set(txn.getDefaultAllocator(),
			const_cast<char *>(explainData_[i].value_string->c_str()));
		v.get(txn, objectManager_, messageRowStore, columnId++);
		v.set(txn.getDefaultAllocator(),
			const_cast<char *>(explainData_[i].statement->c_str()));
		v.get(txn, objectManager_, messageRowStore, columnId++);
		messageRowStore->next();

		callDestructor(explainData_[i].exp_type);
		callDestructor(explainData_[i].value_type);
		callDestructor(explainData_[i].value_string);
		callDestructor(explainData_[i].statement);
		explainData_[i].~ExplainData();  
	}
	resultNum = explainNum_;
	explainNum_ = 0;
	explainAllocNum_ = 0;
	txn.getDefaultAllocator().deallocate(explainData_);  
}

void Query::contractCondition() {
	if (pWhereExpr_) {
		BoolExpr *pWhereExprOld = pWhereExpr_;
		pWhereExpr_ = pWhereExpr_->makeOptimizedExpr(
			txn_, objectManager_, NULL, functionMap_);
		QP_DELETE(pWhereExprOld);
	}
}

void Query::finishQuery(
	TransactionContext &txn, ResultSet &resultSet, BaseContainer &container) {
	if (hook_) {
		hook_->qpExecuteFinishHook(*this);	

	}
	if (doExplain()) {
		util::XArray<uint8_t> &serializedRowList =
			*resultSet.getRowDataFixedPartBuffer();
		util::XArray<uint8_t> &serializedVarDataList =
			*resultSet.getRowDataVarPartBuffer();
		serializedRowList.clear();
		serializedVarDataList.clear();
		ColumnInfo *explainColumnInfoList = makeExplainColumnInfo(txn);
		OutputMessageRowStore outputMessageRowStore(
			container.getDataStore()->getValueLimitConfig(),
			explainColumnInfoList, EXPLAIN_COLUMN_NUM, serializedRowList,
			serializedVarDataList, false);
		ResultSize resultNum;
		serializeExplainData(txn, resultNum, &outputMessageRowStore);

		resultSet.setResultType(RESULT_EXPLAIN);
		resultSet.setResultNum(resultNum);
	}
}

void Query::setQueryOption(TransactionContext &txn, ResultSet &resultSet) {
	ResultSetOption &queryOption = resultSet.getQueryOption();

	if (queryOption.isPartial() || queryOption.isDistribute()) {
		size_t numSelection = getSelectionExprLength();
		if (pOrderByExpr_  != NULL || numSelection == 0 || 
			strcmp("*", getSelectionExpr(0)->getValueAsString(txn)) != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_SYNTAX_ERROR_EXECUTION, "Partial/Distribute TQL does not support "
				<< "order by and selection expression except for '*'");
		}

		queryOption.setDistLimit(nActualLimit_);
		queryOption.setDistOffset(nOffset_);

		{
			int64_t filteredNum = queryOption.getFilteredNum();
			if (filteredNum != 0) {
				if (nOffset_ <= static_cast<ResultSize>(filteredNum)) {
					nOffset_ = 0;
					filteredNum -= nOffset_;
				}
				if (static_cast<ResultSize>(filteredNum) > nLimit_) {
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_FETCH_PARAMETER_INVALID, "Invalid Parameter, filtered num exceeds limit");
				}
				if (filteredNum > 0) {
					nLimit_ -= filteredNum;
					nActualLimit_ -= filteredNum;
				}
			}
			if (queryOption.isDistribute()) {
				nActualLimit_ = nLimit_;
				nOffset_ = 0;
			}
		}
	}
}

void Query::doQueryPartial(
	TransactionContext &txn, BaseContainer &container, ResultSet &resultSet) {
	ResultSetOption &queryOption = resultSet.getQueryOption();
		
	if (doExplain()) {
		addExplain(0, "SELECTION", "CONDITION", "NULL", "");
		addExplain(1, "INDEX", "BTREE", "ROWMAP", "");
	}
	if (!doExecute()) {
		queryOption.setMinRowId(MAX_ROWID);
		queryOption.setMaxRowId(MAX_ROWID);
		return;
	}

	container.getObjectManager()->setSwapOutCounter(txn.getPartitionId(), queryOption.getSwapOutNum());

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<BoolExpr *> orList(alloc);  
	if (getConditionExpr() != NULL) {  
		getConditionAsOrList(orList);
		nLimit_ = MAX_RESULT_SIZE;
	}
	util::XArray< util::XArray<BoolExpr *> *> andLists(alloc);
	for (uint32_t p = 0; p < orList.size(); p++) {
		util::XArray<BoolExpr *> *andList= QP_NEW util::XArray<BoolExpr *>(alloc);  
		orList[p]->toAndList(*andList);
		andLists.push_back(andList);
	}

	uint64_t *pBitmap = reinterpret_cast<uint64_t *>(alloc.allocate(
		sizeof(uint64_t) * ((container.getColumnNum() / 64) + 1)));
	ContainerRowWrapper *row = NULL;
	bool isWithRowId = true;
	ColumnId rowColumnId = container.getRowIdColumnId();
	OutputOrder outputOrder = ORDER_ASCENDING;
	switch (container.getContainerType()) {
	case COLLECTION_CONTAINER:
		isWithRowId = true;
		row = ALLOC_NEW(alloc) CollectionRowWrapper(txn, *static_cast<Collection *>(&container), pBitmap);
		break;
	case TIME_SERIES_CONTAINER:
		isWithRowId = false;
		row = ALLOC_NEW(alloc) TimeSeriesRowWrapper(txn, *static_cast<TimeSeries *>(&container), pBitmap);
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		break;
	}
	StackAllocAutoPtr<ContainerRowWrapper> stackAutoPtr(alloc, row);

	OutputMessageRowStore outputMessageRowStore(
		container.getDataStore()->getValueLimitConfig(),
		container.getColumnInfoList(), container.getColumnNum(),
		*resultSet.getRowDataFixedPartBuffer(), 
		*resultSet.getRowDataVarPartBuffer(), isWithRowId);

	ResultSize scanLimit;
	size_t repeatNum = 10;
	scanLimit = ResultSetOption::PARTIAL_SCAN_LIMIT / repeatNum / container.getNormalRowArrayNum() + 1;

	ResultSize resultNum = 0;
	ResultSize currentOffset = nOffset_;
	RowId lastRowId = queryOption.getMinRowId();
	bool isFetchSizeLimit = false;
	bool isAllFinished = false;
	bool enablePartialSuspend = !doExplain();

	util::XArray<OId> scanOIdList(alloc);
	for (size_t nth = 0; ; nth++) {
		scanOIdList.clear();
		RowId minRowId = queryOption.getMinRowId();
		RowId maxRowId = queryOption.getMaxRowId();
		TermCondition startCond(container.getRowIdColumnType(), container.getRowIdColumnType(), 
			DSExpression::GE, rowColumnId, &minRowId, sizeof(minRowId));
		TermCondition endCond(container.getRowIdColumnType(), container.getRowIdColumnType(), 
			DSExpression::LE, rowColumnId, &maxRowId, sizeof(maxRowId));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), startCond, endCond, nLimit_);
		sc.setSuspendLimit(scanLimit);
		container.searchRowIdIndex(txn, sc, scanOIdList, outputOrder);

		uint32_t i = 0;
		for (; i < scanOIdList.size(); i++) {
			OId oId = scanOIdList[i];
			row->load(oId);
			lastRowId = row->getRowId();
			bool conditionFlag = true;
			for (uint32_t p = 0; p < orList.size(); p++) {
				util::StackAllocator::Scope scope(alloc);
				util::XArray<BoolExpr *> &andList = *(andLists[p]);
				assert(!andList.empty());  

				conditionFlag = true;
				for (uint32_t q = 0; q < andList.size(); q++) {
					util::StackAllocator::Scope scope(alloc);
					TrivalentLogicType evalResult = andList[q]->eval(
						txn, objectManager_, row, getFunctionMap(), TRI_TRUE);
					if (evalResult != TRI_TRUE) {
						conditionFlag = false;
						break;
					}
				}
				if (conditionFlag) {
					break;
				}
			}  
			if (conditionFlag) {
				if (currentOffset > 0) {
					currentOffset--;
					continue;
				} else {
					resultNum++;
					if (!doExplain()) {
						row->getImage(txn, &outputMessageRowStore, isWithRowId);
						outputMessageRowStore.next();

						if (queryOption.isPartial()) {
							const uint8_t *data1, *data2;
							uint32_t fixedSize, variableSize;
							outputMessageRowStore.getAllFixedPart(data1, fixedSize);
							outputMessageRowStore.getAllVariablePart(data2, variableSize);
							if (fixedSize + variableSize > static_cast<uint32_t>(queryOption.getFetchByteSize())) {
								isFetchSizeLimit = true;

							}
						}
					}
				}
			}

			if (resultNum >= nActualLimit_ || isFetchSizeLimit) {
				break;
			}
		}
		queryOption.setMinRowId(lastRowId + 1);

		if ((!sc.isSuspended() && !isFetchSizeLimit) || resultNum >= nActualLimit_) {
			isAllFinished = true;
		}

		if (isAllFinished || isFetchSizeLimit || 
			(queryOption.isPartial() && !doExplain() && 
			resultNum > 0 && nth >= repeatNum)) {
			break;
		}
	}

	if (isAllFinished) {
		queryOption.setMinRowId(MAX_ROWID);
		queryOption.setMaxRowId(MAX_ROWID);
	} else {
		if (queryOption.getMaxRowId() == MAX_ROWID) {
			RowId maxRowId = container.getMaxRowId(txn);
			queryOption.setMaxRowId(maxRowId);
		}
		queryOption.setFilteredNum(queryOption.getFilteredNum() + resultNum);
		queryOption.setSwapOutNum(container.getObjectManager()->getSwapOutCounter(txn.getPartitionId()));
	}
	resultSet.setResultNum(resultNum);
}

const BoolExpr* Query::QueryAccessor::findWhereExpr(const Query &query) {
	return query.pWhereExpr_;
}

Expr::Type Query::ExprAccessor::getType(const Expr &expr) {
	return static_cast<const ExprAccessor&>(expr).type_;
}

Expr::Operation Query::ExprAccessor::getOp(const Expr &expr) {
	return static_cast<const ExprAccessor&>(expr).op_;
}

const Value* Query::ExprAccessor::getValue(const Expr &expr) {
	return static_cast<const ExprAccessor&>(expr).value_;
}

const ExprList* Query::ExprAccessor::getArgList(const Expr &expr) {
	return static_cast<const ExprAccessor&>(expr).arglist_;
}

uint32_t Query::ExprAccessor::getColumnId(const Expr &expr) {
	return static_cast<const ExprAccessor&>(expr).columnId_;
}

ColumnType Query::ExprAccessor::getExprColumnType(const Expr &expr) {
	return static_cast<const ExprAccessor&>(expr).columnType_;
}

const BoolExpr::BoolTerms& Query::BoolExprAccessor::getOperands(
		const BoolExpr &expr) {
	return static_cast<const BoolExprAccessor&>(expr).operands_;
}

const Expr* Query::BoolExprAccessor::getUnary(const BoolExpr &expr) {
	return static_cast<const BoolExprAccessor&>(expr).unary_;
}

BoolExpr::Operation Query::BoolExprAccessor::getOpType(const BoolExpr &expr) {
	return static_cast<const BoolExprAccessor&>(expr).opeType_;
}
