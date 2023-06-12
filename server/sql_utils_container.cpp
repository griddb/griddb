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

#ifdef _WIN32
#define NOGDI
#endif

#include "sql_utils_container_impl.h"
#include "query.h"
#include "sql_execution.h"

typedef NoSQLCommonUtils NoSQLUtils;



SQLContainerUtils::ScanCursor::~ScanCursor() {
}

SQLContainerUtils::ScanCursor::ScanCursor() {
}


SQLContainerUtils::ScanCursor::Holder::~Holder() {
}

SQLContainerUtils::ScanCursor::Holder::Holder() {
}


SQLContainerUtils::ScanCursorAccessor::~ScanCursorAccessor() {
}

util::AllocUniquePtr<SQLContainerUtils::ScanCursorAccessor>::ReturnType
SQLContainerUtils::ScanCursorAccessor::create(const Source &source) {
	SQLValues::VarAllocator &varAlloc = source.varAlloc_;
	util::AllocUniquePtr<ScanCursorAccessor> ptr(
			ALLOC_NEW(varAlloc) SQLContainerImpl::CursorAccessorImpl(source),
			varAlloc);
	return ptr;
}

SQLContainerUtils::ScanCursorAccessor::ScanCursorAccessor() {
}


SQLContainerUtils::ScanCursorAccessor::Source::Source(
		SQLValues::VarAllocator &varAlloc,
		const SQLOps::ContainerLocation &location,
		const ColumnTypeList *columnTypeList,
		const OpContext::Source &cxtSrc) :
		varAlloc_(varAlloc),
		location_(location),
		columnTypeList_(columnTypeList),
		indexLimit_(-1),
		memLimit_(-1),
		cxtSrc_(cxtSrc) {
}



size_t SQLContainerUtils::ContainerUtils::findMaxStringLength(
		const ResourceSet *resourceSet) {
	do {
		if (resourceSet == NULL) {
			break;
		}

		const PartitionId pId = 0;
		DataStoreBase& dsBase =
			resourceSet->getPartitionList()->partition(pId).dataStore();
		DataStoreV4* dataStore = static_cast<DataStoreV4*>(&dsBase);

		if (dataStore == NULL) {
			break;
		}

		return dataStore->getConfig().getLimitSmallSize();
	}
	while (false);
	return 0;
}

bool SQLContainerUtils::ContainerUtils::toTupleColumnType(
		uint8_t src, bool nullable, TupleColumnType &dest,
		bool failOnUnknown) {
	typedef SQLContainerImpl::ContainerValueUtils Utils;
	dest = Utils::toTupleColumnType(src, nullable);
	const bool converted =
			!SQLValues::TypeUtils::isAny(dest) &&
			!SQLValues::TypeUtils::isNull(dest);
	if (!converted && failOnUnknown) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return converted;
}

int32_t SQLContainerUtils::ContainerUtils::toSQLColumnType(
		TupleColumnType type) {
	typedef SQLContainerImpl::ContainerValueUtils Utils;
	SQLContainerImpl::ContainerColumnType typeAsContainer;
	const bool failOnError = true;
	Utils::toContainerColumnType(type, typeAsContainer, failOnError);
	return MetaProcessor::SQLMetaUtils::toSQLColumnType(typeAsContainer);
}

bool SQLContainerUtils::ContainerUtils::predicateToMetaTarget(
		SQLValues::ValueContext &cxt, const SQLExprs::Expression *pred,
		uint32_t partitionIdColumn, uint32_t containerNameColumn,
		uint32_t containerIdColumn, PartitionId partitionCount,
		PartitionId &partitionId, bool &placeholderAffected) {

	partitionId = UNDEF_PARTITIONID;
	placeholderAffected = false;

	const SQLExprs::ExprFactory &factory =
			SQLExprs::ExprFactory::getDefaultFactory();

	if (containerNameColumn != UNDEF_COLUMNID) {
		TupleValue containerName;
		if (!SQLExprs::ExprRewriter::predicateToExtContainerName(
				cxt, factory, pred, UNDEF_COLUMNID, containerNameColumn,
				NULL, containerName, placeholderAffected)) {
			return false;
		}

		const TupleString::BufferInfo &nameBuf =
				TupleString(containerName).getBuffer();
		try {
			FullContainerKey containerKey(
					cxt.getAllocator(), KeyConstraint::getNoLimitKeyConstraint(),
					GS_PUBLIC_DB_ID, nameBuf.first, nameBuf.second);

			const ContainerHashMode hashMode =
					CONTAINER_HASH_MODE_CRC32;

			partitionId = KeyDataStore::resolvePartitionId(
					cxt.getAllocator(), containerKey, partitionCount,
					hashMode);
		}
		catch (UserException&) {
			return false;
		}

		return true;
	}
	else if (containerIdColumn != UNDEF_COLUMNID) {
		ContainerId containerId = UNDEF_CONTAINERID;
		return SQLExprs::ExprRewriter::predicateToContainerId(
				cxt, factory, pred, partitionIdColumn, containerIdColumn,
				partitionId, containerId, placeholderAffected);
	}
	else {
		return false;
	}
}

SQLExprs::SyntaxExpr SQLContainerUtils::ContainerUtils::tqlToPredExpr(
		util::StackAllocator &alloc, const Query &query) {
	return SQLContainerImpl::TQLTool::genPredExpr(alloc, query);
}


SQLContainerImpl::CursorImpl::CursorImpl(Accessor &accessor, Data &data) :
		accessor_(accessor),
		data_(data) {
}

SQLContainerImpl::CursorImpl::~CursorImpl() {
}

bool SQLContainerImpl::CursorImpl::scanFull(
		OpContext &cxt, const Projection &proj) {
	accessor_.startScope(cxt.getExtContext(), cxt.getInputColumnList(0));

	if (accessor_.getContainer() == NULL) {
		return true;
	}

	BaseContainer &container = accessor_.resolveContainer();
	TransactionContext &txn = accessor_.resolveTransactionContext();

	try {
	RowIdFilter *rowIdFilter = checkRowIdFilter(cxt, txn, container, true);

	ContainerRowScanner *scanner =
			tryCreateScanner(cxt, proj, container, rowIdFilter);

	BtreeSearchContext sc(
			txn.getDefaultAllocator(), container.getRowIdColumnId());
	preparePartialSearch(cxt, sc, NULL);
	const OutputOrder outputOrder = ORDER_UNDEFINED;

	switch (container.getContainerType()) {
	case COLLECTION_CONTAINER:
		static_cast<Collection&>(container).scanRowIdIndex(txn, sc, *scanner);
		break;
	case TIME_SERIES_CONTAINER:
		static_cast<TimeSeries&>(container).scanRowIdIndex(
				txn, sc, outputOrder, *scanner);
		break;
	default:
		assert(false);
		break;
	}

	const bool finished = finishPartialSearch(cxt, sc, false);
	if (!finished) {
		return finished;
	}

	updateScanner(cxt, finished);
	return finished;
	}
	catch (std::exception& e) {
		container.handleSearchError(txn, e, GS_ERROR_DS_CON_STATUS_INVALID);
		return false;
	}
}

bool SQLContainerImpl::CursorImpl::scanRange(
		OpContext &cxt, const Projection &proj) {
	accessor_.startScope(cxt.getExtContext(), cxt.getInputColumnList(0));

	if (accessor_.getContainer() == NULL || accessor_.isIndexLost()) {
		return true;
	}

	BaseContainer &container = accessor_.resolveContainer();
	TransactionContext &txn = accessor_.resolveTransactionContext();

	ResultSet &resultSet = accessor_.resolveResultSet();
	try {

	OIdTable *oIdTable = findOIdTable();
	RowIdFilter *rowIdFilter = checkRowIdFilter(cxt, txn, container, false);

	const uint32_t index = 1;
	TupleListReader &subReader = cxt.getReader(index, 1);
	if (checkUpdates(txn, container, resultSet) || oIdTable != NULL) {
		const bool found = (oIdTable != NULL);
		oIdTable = &resolveOIdTable(cxt, accessor_.getMergeMode());
		if (!found) {
			const bool forPipe = true;
			if (SQLOps::OpCodeBuilder::findAggregationProjection(
					proj, &forPipe) != NULL) {
				cxt.getExprContext().initializeAggregationValues();
			}
			OIdTable::Reader reader(subReader, cxt.getInputColumnList(index));
			oIdTable->load(reader);
		}
		rowIdFilter = &resolveRowIdFilter(
				cxt, accessor_.getMaxRowId(txn, container));
	}

	ContainerRowScanner *scanner =
			tryCreateScanner(cxt, proj, container, rowIdFilter);
	accessor_.clearResultSetPreserving(resultSet, false);

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	SearchResult result(alloc);

	uint64_t suspendLimit = cxt.getNextCountForSuspend();
	bool finished;
	if (oIdTable != NULL) {
		if (accessor_.prepareUpdateRowIdHandler(
				txn, container, resultSet)->apply(*oIdTable, suspendLimit)) {
			oIdTable->popAll(
					result, suspendLimit, container.getNormalRowArrayNum());
			finished = oIdTable->isEmpty();
		}
		else {
			finished = false;
		}
	}
	else {
		OIdTable::Reader reader(
				cxt.getReader(index), cxt.getInputColumnList(index));
		OIdTable::readAll(
				result, container.getNormalRowArrayNum(),
				OIdTable::Source::detectLarge(container.getDataStore()),
				accessor_.getMergeMode(),
				container.getNormalRowArrayNum(), reader);
		finished = !reader.exists();
	}

	acceptSearchResult(cxt, result, NULL, scanner, NULL);

	if (finished) {
		accessor_.finishRowIdFilter(rowIdFilter);
	}
	else {
		accessor_.activateResultSetPreserving(resultSet);
	}
	updateScanner(cxt, finished);

	return finished;
	}
	catch (std::exception& e) {
		container.handleSearchError(txn, e, GS_ERROR_DS_CON_STATUS_INVALID);
		return false;
	}
}

bool SQLContainerImpl::CursorImpl::scanIndex(
		OpContext &cxt, const SQLExprs::IndexConditionList &condList) {
	accessor_.startScope(cxt.getExtContext(), cxt.getInputColumnList(0));

	if (accessor_.getContainer() == NULL) {
		return true;
	}

	BaseContainer &container = accessor_.resolveContainer();
	TransactionContext &txn = accessor_.resolveTransactionContext();

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	try  {
	util::Vector<uint32_t> indexColumnList(alloc);
	const IndexType indexType =
			acceptIndexCond(cxt, condList, indexColumnList);
	SQLOps::OpProfilerIndexEntry *profilerEntry = cxt.getIndexProfiler();

	bool finished = true;
	SearchResult result(alloc);
	if (indexType == SQLType::INDEX_TREE_RANGE ||
			indexType == SQLType::INDEX_TREE_EQ) {
		BtreeSearchContext sc(alloc, indexColumnList);
		preparePartialSearch(cxt, sc, &condList);
		if (profilerEntry != NULL) {
			profilerEntry->startSearch();
		}

		if (container.getContainerType() == TIME_SERIES_CONTAINER) {
			TimeSeries &timeSeries = static_cast<TimeSeries&>(container);
			if (condList.front().column_ == ColumnInfo::ROW_KEY_COLUMN_ID) {
				const SearchResult::OIdListRef oIdListRef =
						result.getOIdListRef(true);
				timeSeries.searchRowIdIndexAsRowArray(
						txn, sc, *oIdListRef.first, *oIdListRef.second);
			}
			else {
				BtreeSearchContext orgSc(alloc, indexColumnList);
				setUpSearchContext(txn, condList, orgSc, container);
				const SearchResult::OIdListRef oIdListRef =
						result.getOIdListRef(false);
				timeSeries.searchColumnIdIndex(
						txn, sc, orgSc, *oIdListRef.first, *oIdListRef.second);
			}
		}
		else {
			const SearchResult::OIdListRef oIdListRef = result.getOIdListRef(false);
			container.searchColumnIdIndex(
					txn, sc, *oIdListRef.first, *oIdListRef.second);
		}

		if (profilerEntry != NULL) {
			profilerEntry->finishSearch();
		}
		finished = finishPartialSearch(cxt, sc, true);
	}

	OIdTable &oIdTable = resolveOIdTable(cxt, accessor_.getMergeMode());
	acceptSearchResult(cxt, result, &condList, NULL, &oIdTable);

	return finished;
	}
	catch (std::exception& e) {
		container.handleSearchError(txn, e, GS_ERROR_DS_CON_STATUS_INVALID);
		return false;
	}
}

bool SQLContainerImpl::CursorImpl::scanMeta(
		OpContext &cxt, const Projection &proj, const Expression *pred) {
	ExtOpContext &extCxt = cxt.getExtContext();
	accessor_.startScope(extCxt, cxt.getInputColumnList(0));

	cxt.closeLocalTupleList(data_.outIndex_);
	cxt.createLocalTupleList(data_.outIndex_);

	TransactionContext &txn = accessor_.resolveTransactionContext();
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	DataStoreV4 *dataStore = Accessor::getDataStore(extCxt);
	assert(dataStore != NULL);

	const ContainerLocation &location = accessor_.getLocation();
	util::String dbName(alloc);
	FullContainerKey *containerKey = predicateToContainerKey(
			cxt.getValueContext(), txn, *dataStore, pred, location, dbName);

	MetaProcessorSource source(location.dbVersionId_, dbName.c_str());
	source.dataStore_ = dataStore;
	source.eventContext_= Accessor::getEventContext(extCxt);
	source.transactionManager_ = Accessor::getTransactionManager(extCxt);
	source.transactionService_ = Accessor::getTransactionService(extCxt);
	source.partitionTable_ = Accessor::getPartitionTable(extCxt);
	source.sqlService_ = Accessor::getSQLService(extCxt);
	source.sqlExecutionManager_ = Accessor::getExecutionManager(extCxt);

	const bool forCore = true;
	MetaScanHandler handler(cxt, *this, proj);
	MetaProcessor proc(txn, location.id_, forCore);

	proc.setNextContainerId(getNextContainerId());
	proc.setContainerLimit(cxt.getNextCountForSuspend());
	proc.setContainerKey(containerKey);

	proc.scan(txn, source, handler);

	setNextContainerId(proc.getNextContainerId());

	cxt.closeLocalWriter(data_.outIndex_);
	return !proc.isSuspended();
}

void SQLContainerImpl::CursorImpl::finishIndexScan(OpContext &cxt) {
	OIdTable *oIdTable = findOIdTable();

	if (oIdTable != NULL) {
		const uint32_t index = 0;
		OIdTable::Writer writer(
				cxt.getWriter(index), cxt.getOutputColumnList(index));
		oIdTable->save(writer);
	}
}

uint32_t SQLContainerImpl::CursorImpl::getOutputIndex() {
	return data_.outIndex_;
}

void SQLContainerImpl::CursorImpl::setOutputIndex(uint32_t index) {
	data_.outIndex_ = index;
}

SQLContainerUtils::ScanCursorAccessor&
SQLContainerImpl::CursorImpl::getAccessor() {
	return accessor_;
}

void SQLContainerImpl::CursorImpl::addScannerUpdator(
		const UpdatorEntry &updator) {
	data_.updatorList_->push_back(updator);
}

void SQLContainerImpl::CursorImpl::addContextRef(OpContext **ref) {
	data_.cxtRefList_.push_back(ref);
}

void SQLContainerImpl::CursorImpl::addContextRef(SQLExprs::ExprContext **ref) {
	data_.exprCxtRefList_.push_back(ref);
}

ContainerId SQLContainerImpl::CursorImpl::getNextContainerId() const {
	if (data_.nextContainerId_ == UNDEF_CONTAINERID) {
		return 0;
	}

	return data_.nextContainerId_;
}

void SQLContainerImpl::CursorImpl::setNextContainerId(ContainerId containerId) {
	data_.nextContainerId_ = containerId;
}

ContainerRowScanner* SQLContainerImpl::CursorImpl::tryCreateScanner(
		OpContext &cxt, const Projection &proj,
		const BaseContainer &container, RowIdFilter *rowIdFilter) {

	ContainerRowScanner *&scanner = data_.scanner_;
	util::LocalUniquePtr<UpdatorList> &updatorList = data_.updatorList_;

	if (!accessor_.updateNullsStats(cxt.getAllocator(), container) ||
			!accessor_.resolveResultSet().isPartialExecuteSuspend()) {
		scanner = NULL;
		if (updatorList.get() != NULL) {
			updatorList->clear();
		}
	}

	if (scanner == NULL) {
		clearAllContextRef();
		if (updatorList.get() == NULL) {
			updatorList = UTIL_MAKE_LOCAL_UNIQUE(
					updatorList, UpdatorList,
					cxt.getValueContext().getVarAllocator());
		}
		scanner = &ScannerHandlerFactory::getInstance().create(
				cxt, *this, accessor_, proj, rowIdFilter);
		cxt.setUpExprContext();
	}
	updateAllContextRef(cxt);
	updateScanner(cxt, false);
	return scanner;
}

void SQLContainerImpl::CursorImpl::updateScanner(
		OpContext &cxt, bool finished) {
	UpdatorList &updatorList = *data_.updatorList_;

	for (UpdatorList::iterator it = updatorList.begin();
			it != updatorList.end(); ++it) {
		it->first(cxt, it->second);
	}

	if (finished) {
		data_.scanner_ = NULL;
		updatorList.clear();
	}
}

void SQLContainerImpl::CursorImpl::preparePartialSearch(
		OpContext &cxt, BtreeSearchContext &sc,
		const SQLExprs::IndexConditionList *targetCondList) {
	BaseContainer &container = accessor_.resolveContainer();
	ResultSet &resultSet = accessor_.resolveResultSet();
	TransactionContext &txn = accessor_.resolveTransactionContext();

	{
		const uint64_t restSize = cxt.getNextCountForSuspend();
		const uint64_t execSize =
				std::min<uint64_t>(restSize, data_.lastPartialExecSize_);

		cxt.setNextCountForSuspend(restSize - execSize);
		resultSet.setPartialExecuteSize(std::max<uint64_t>(execSize, 1));
	}

	const bool lastSuspended = resultSet.isPartialExecuteSuspend();
	const ResultSize suspendLimit = resultSet.getPartialExecuteSize();
	const OutputOrder outputOrder = ORDER_UNDEFINED;

	accessor_.clearResultSetPreserving(resultSet, false);

	if (targetCondList == NULL) {
		resultSet.setUpdateIdType(ResultSet::UPDATE_ID_NONE);
	}
	else {
		setUpSearchContext(txn, *targetCondList, sc, container);
		if (!lastSuspended) {
			resultSet.setPartialMode(true);
		}
		resultSet.setUpdateIdType(ResultSet::UPDATE_ID_OBJECT);
	}

	resultSet.rewritePartialContext(
			txn, sc, suspendLimit, outputOrder, 0, container,
			container.getNormalRowArrayNum());
}

bool SQLContainerImpl::CursorImpl::finishPartialSearch(
		OpContext &cxt, BtreeSearchContext &sc, bool forIndex) {
	BaseContainer &container = accessor_.resolveContainer();
	ResultSet &resultSet = accessor_.resolveResultSet();
	TransactionContext &txn = accessor_.resolveTransactionContext();

	const bool lastSuspended = resultSet.isPartialExecuteSuspend();
	ResultSize suspendLimit = resultSet.getPartialExecuteSize();

	resultSet.setPartialContext(sc, suspendLimit, 0, MAP_TYPE_BTREE);

	if (!lastSuspended) {
		if (!forIndex) {
			resultSet.setLastRowId(accessor_.getMaxRowId(txn, container));
		}
	}

	const bool finished = (!sc.isSuspended() || cxt.isCompleted());
	if (finished) {
		resultSet.setPartialExecStatus(ResultSet::PARTIAL_FINISH);
	}
	else {
		resultSet.setPartialExecStatus(ResultSet::PARTIAL_SUSPENDED);
		resultSet.incrementExecCount();
	}

	accessor_.prepareUpdateRowIdHandler(txn, container, resultSet);

	resultSet.incrementReturnCount();
	accessor_.activateResultSetPreserving(resultSet);

	{
		const std::pair<uint64_t, uint64_t> &range =
				accessor_.getPartialExecSizeRange();
		uint64_t &last = data_.lastPartialExecSize_;

		CursorScope &scope = accessor_.resolveScope();
		const uint64_t elapsed = scope.getElapsedNanos();
		const uint64_t threshold = 1000 * 1000;
		if (elapsed < threshold / 2) {
			last = std::min(last * 2, range.second);
		}
		else {
			if (elapsed > threshold &&
					resultSet.getPartialExecuteCount() <=
					scope.getStartPartialExecCount() + 1) {
				last = std::max(last / 2, range.first);
			}
			cxt.setNextCountForSuspend(0);
		}
	}

	return finished;
}

bool SQLContainerImpl::CursorImpl::checkUpdates(
		TransactionContext &txn, BaseContainer &container,
		ResultSet &resultSet) {
	TupleUpdateRowIdHandler *handler =
			accessor_.prepareUpdateRowIdHandler(txn, container, resultSet);

	return (handler != NULL && handler->isUpdated());
}

void SQLContainerImpl::CursorImpl::setUpSearchContext(
		TransactionContext &txn,
		const SQLExprs::IndexConditionList &targetCondList,
		BtreeSearchContext &sc, const BaseContainer &container) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	const bool forKey = true;
	for (SQLExprs::IndexConditionList::const_iterator it =
			targetCondList.begin(); it != targetCondList.end(); ++it) {
		const SQLExprs::IndexCondition &cond = *it;
		const ContainerColumnType columnType =
				container.getColumnInfo(cond.column_).getColumnType();

		if (cond.opType_ != SQLType::OP_EQ &&
				cond.opType_ != SQLType::EXPR_BETWEEN) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		if (!SQLValues::TypeUtils::isNull(cond.valuePair_.first.getType())) {
			Value *value = ALLOC_NEW(alloc) Value();
			ContainerValueUtils::toContainerValue(
					alloc, cond.valuePair_.first, *value);

			DSExpression::Operation opType;
			if (cond.opType_ == SQLType::OP_EQ) {
				opType = DSExpression::EQ;
			}
			else if (cond.inclusivePair_.first) {
				opType = DSExpression::GE;
			}
			else {
				opType = DSExpression::GT;
			}

			TermCondition c(
					columnType, columnType,
					opType, cond.column_, value->data(), value->size());
			sc.addCondition(txn, c, forKey);
		}

		if (!SQLValues::TypeUtils::isNull(cond.valuePair_.second.getType())) {
			Value *value = ALLOC_NEW(alloc) Value();
			ContainerValueUtils::toContainerValue(
					alloc, cond.valuePair_.second, *value);

			DSExpression::Operation opType;
			if (cond.inclusivePair_.second) {
				opType = DSExpression::LE;
			}
			else {
				opType = DSExpression::LT;
			}

			TermCondition c(
					columnType, columnType,
					opType, cond.column_, value->data(), value->size());
			sc.addCondition(txn, c, forKey);
		}
	}
}

SQLType::IndexType SQLContainerImpl::CursorImpl::acceptIndexCond(
		OpContext &cxt, const SQLExprs::IndexConditionList &targetCondList,
		util::Vector<uint32_t> &indexColumnList) {
	if (accessor_.isIndexLost()) {
		setIndexLost(cxt);
		return SQLType::END_INDEX;
	}

	if (targetCondList.size() <= 1) {
		assert(!targetCondList.empty());
		const SQLExprs::IndexCondition &cond = targetCondList.front();

		if (cond.isEmpty() || cond.equals(true)) {
			setIndexLost(cxt);
			return SQLType::END_INDEX;
		}
		else if (cond.isNull() || cond.equals(false)) {
			return SQLType::END_INDEX;
		}
	}

	const SQLExprs::IndexCondition &cond = targetCondList.front();
	if (cond.specId_ > 0) {
		const SQLExprs::IndexSpec &spec = cond.spec_;
		indexColumnList.assign(
				spec.columnList_, spec.columnList_ + spec.columnCount_);
	}
	else {
		indexColumnList.push_back(cond.column_);
	}

	TransactionContext &txn = accessor_.resolveTransactionContext();
	BaseContainer &container = accessor_.resolveContainer();

	typedef util::Vector<IndexInfo> IndexInfoList;
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	IndexInfoList indexInfoList(alloc);
	Accessor::getIndexInfoList(txn, container, indexInfoList);

	for (IndexInfoList::iterator it = indexInfoList.begin();; ++it) {
		if (it == indexInfoList.end()) {
			setIndexLost(cxt);
			return SQLType::END_INDEX;
		}
		else if (it->mapType == MAP_TYPE_BTREE &&
				it->columnIds_ == indexColumnList) {
			acceptIndexInfo(cxt, txn, container, targetCondList, *it);
			return SQLType::INDEX_TREE_RANGE;
		}
	}
}

void SQLContainerImpl::CursorImpl::acceptSearchResult(
		OpContext &cxt, const SearchResult &result,
		const SQLExprs::IndexConditionList *targetCondList,
		ContainerRowScanner *scanner, OIdTable *oIdTable) {
	static_cast<void>(targetCondList);

	if (result.isEmpty()) {
		return;
	}

	if (scanner == NULL) {
		ResultSet &resultSet = accessor_.resolveResultSet();

		const int64_t indexLimit = accessor_.getIndexLimit();
		if (indexLimit > 0) {
			data_.totalSearchCount_ += result.getSize();
			if (resultSet.isPartialExecuteSuspend() &&
					data_.totalSearchCount_ > indexLimit) {
				setIndexLost(cxt);
				return;
			}
		}

		assert(oIdTable != NULL);
		oIdTable->addAll(result);

		const int64_t memLimit = accessor_.getMemoryLimit();
		if (memLimit > 0 && resultSet.isPartialExecuteSuspend() &&
				oIdTable->getTotalAllocationSize() >
				static_cast<uint64_t>(memLimit)) {
			setIndexLost(cxt);
		}
		return;
	}

	TransactionContext &txn = accessor_.resolveTransactionContext();
	BaseContainer &container = accessor_.resolveContainer();

	const ContainerType containerType = container.getContainerType();
	if (containerType == COLLECTION_CONTAINER) {
		scanRow(txn, static_cast<Collection&>(container), result, *scanner);
	}
	else if (containerType == TIME_SERIES_CONTAINER) {
		scanRow(txn, static_cast<TimeSeries&>(container), result, *scanner);
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLContainerImpl::CursorImpl::acceptIndexInfo(
		OpContext &cxt, TransactionContext &txn, BaseContainer &container,
		const SQLExprs::IndexConditionList &targetCondList,
		const IndexInfo &info) {
	SQLOps::OpProfilerIndexEntry *profilerEntry = cxt.getIndexProfiler();
	if (profilerEntry == NULL || !profilerEntry->isEmpty()) {
		return;
	}

	ObjectManagerV4 *objectManager = container.getObjectManager();

	if (profilerEntry->findIndexName() == NULL) {
		profilerEntry->setIndexName(info.indexName_.c_str());
	}

	for (util::Vector<uint32_t>::const_iterator it = info.columnIds_.begin();
			it != info.columnIds_.end(); ++it) {
		if (profilerEntry->findColumnName(*it) == NULL) {
			ColumnInfo &columnInfo = container.getColumnInfo(*it);
			profilerEntry->setColumnName(
					*it, columnInfo.getColumnName(
							txn, *objectManager,
							container.getMetaAllocateStrategy()));
		}
		profilerEntry->addIndexColumn(*it);
	}

	for (SQLExprs::IndexConditionList::const_iterator it =
			targetCondList.begin(); it != targetCondList.end(); ++it) {
		profilerEntry->addCondition(*it);
	}
}

void SQLContainerImpl::CursorImpl::setIndexLost(OpContext &cxt) {
	data_.scanner_ = NULL;
	if (data_.updatorList_.get() != NULL) {
		data_.updatorList_->clear();
	}

	accessor_.clearResultSetPreserving(accessor_.resolveResultSet(), true);
	if (accessor_.isRowIdFiltering()) {
		cxt.setCompleted();
	}
	else {
		cxt.invalidate();
	}
	accessor_.finishScope();
	accessor_.acceptIndexLost();
}

template<typename Container>
void SQLContainerImpl::CursorImpl::scanRow(
		TransactionContext &txn, Container &container,
		const SearchResult &result, ContainerRowScanner &scanner) {
	for (size_t i = 0; i < 2; i++) {
		const bool onMvcc = (i != 0);
		const SearchResult::Entry &entry = result.getEntry(onMvcc);

		for (size_t j = 0; j < 2; j++) {
			const bool forRowArray = (j != 0);
			const SearchResult::OIdList &oIdList = entry.getOIdList(forRowArray);
			if (oIdList.empty()) {
				continue;
			}

			const OId *begin = &oIdList.front();
			const OId *end = &oIdList.back() + 1;
			if (forRowArray) {
				container.scanRowArray(txn, begin, end, onMvcc, scanner);
			}
			else {
				container.scanRow(txn, begin, end, onMvcc, scanner);
			}
		}
	}
}

FullContainerKey*
SQLContainerImpl::CursorImpl::predicateToContainerKey(
		SQLValues::ValueContext &cxt, TransactionContext &txn,
		DataStoreV4 &dataStore, const Expression *pred,
		const ContainerLocation &location, util::String &dbNameStr) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	const MetaContainerInfo &metaInfo = resolveMetaContainerInfo(location.id_);
	const SQLExprs::ExprFactory &factory =
			SQLExprs::ExprFactory::getDefaultFactory();

	FullContainerKey *containerKey = NULL;
	bool placeholderAffected;
	if (metaInfo.commonInfo_.dbNameColumn_ != UNDEF_COLUMNID) {
		TupleValue dbName;
		TupleValue containerName;

		const bool found = SQLExprs::ExprRewriter::predicateToExtContainerName(
				cxt, factory, pred, metaInfo.commonInfo_.dbNameColumn_,
				metaInfo.commonInfo_.containerNameColumn_,
				&dbName, containerName, placeholderAffected);
		if (dbName.getType() == TupleTypes::TYPE_STRING) {
			dbNameStr = ValueUtils::valueToString(cxt, dbName);
		}

		if (found) {
			const util::String containerNameStr =
					ValueUtils::valueToString(cxt, containerName);
			try {
				containerKey = ALLOC_NEW(alloc) FullContainerKey(
						alloc, KeyConstraint::getNoLimitKeyConstraint(),
						location.dbVersionId_, containerNameStr.c_str(),
						containerNameStr.size());
			}
			catch (...) {
			}
		}

		VarContext &varCxt = cxt.getVarContext();
		varCxt.destroyValue(dbName);
		varCxt.destroyValue(containerName);
	}

	do {
		if (containerKey != NULL) {
			break;
		}
		if (metaInfo.commonInfo_.containerIdColumn_ == UNDEF_COLUMNID) {
			break;
		}

		PartitionId partitionId = UNDEF_PARTITIONID;
		ContainerId containerId = UNDEF_CONTAINERID;
		if (!SQLExprs::ExprRewriter::predicateToContainerId(
				cxt, factory, pred, metaInfo.commonInfo_.partitionIndexColumn_,
				metaInfo.commonInfo_.containerIdColumn_,
				partitionId, containerId, placeholderAffected)) {
			break;
		}

		if (partitionId != txn.getPartitionId()) {
			break;
		}

		try {
			KeyDataStoreValue keyStoreValue =
					dataStore.getKeyDataStore()->get(alloc, containerId);
			DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
			StackAllocAutoPtr<DSContainerOutputMes> ret(
					alloc, static_cast<DSContainerOutputMes*>(
							dataStore.exec(&txn, &keyStoreValue, &input)));
			BaseContainer* container = ret.get()->container_;
			if (container == NULL) {
				break;
			}

			containerKey = ALLOC_NEW(alloc) FullContainerKey(
					container->getContainerKey(txn));
		}
		catch (...) {
		}
	}
	while (false);

	return containerKey;
}

const MetaContainerInfo&
SQLContainerImpl::CursorImpl::resolveMetaContainerInfo(
		ContainerId id) {
	const bool forCore = true;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	return infoTable.resolveInfo(id, forCore);
}

void SQLContainerImpl::CursorImpl::updateAllContextRef(OpContext &cxt) {
	{
		const ContextRefList &list = data_.cxtRefList_;
		for (ContextRefList::const_iterator it = list.begin();
				it != list.end(); ++it) {
			**it = &cxt;
		}
	}
	{
		const ExprContextRefList &list = data_.exprCxtRefList_;
		for (ExprContextRefList::const_iterator it = list.begin();
				it != list.end(); ++it) {
			**it = &cxt.getExprContext();
		}
	}
}

void SQLContainerImpl::CursorImpl::clearAllContextRef() {
	data_.cxtRefList_.clear();
	data_.exprCxtRefList_.clear();
}

SQLContainerImpl::RowIdFilter* SQLContainerImpl::CursorImpl::checkRowIdFilter(
		OpContext &cxt, TransactionContext& txn, BaseContainer &container,
		bool readOnly) {
	RowIdFilter *rowIdFilter = findRowIdFilter();
	if (rowIdFilter != NULL) {
		return rowIdFilter;
	}

	if (!accessor_.isRowIdFiltering()) {
		return NULL;
	}

	rowIdFilter = &resolveRowIdFilter(cxt, accessor_.getMaxRowId(txn, container));

	if (readOnly) {
		accessor_.restoreRowIdFilter(*rowIdFilter);
		rowIdFilter->setReadOnly();
	}

	return rowIdFilter;
}

SQLContainerImpl::OIdTable* SQLContainerImpl::CursorImpl::findOIdTable() {
	return data_.oIdTable_.get();
}

SQLContainerImpl::OIdTable& SQLContainerImpl::CursorImpl::resolveOIdTable(
		OpContext &cxt, MergeMode::Type mode) {
	if (data_.oIdTable_.get() == NULL) {
		data_.oIdTable_ = ALLOC_UNIQUE(
				cxt.getValueContext().getVarAllocator(), OIdTable,
				OIdTable::Source::ofContext(cxt, mode));
	}
	return *data_.oIdTable_;
}

SQLContainerImpl::RowIdFilter* SQLContainerImpl::CursorImpl::findRowIdFilter() {
	return data_.rowIdFilter_.get();
}

SQLContainerImpl::RowIdFilter&
SQLContainerImpl::CursorImpl::resolveRowIdFilter(
		OpContext &cxt, RowId maxRowId) {
	if (data_.rowIdFilter_.get() == NULL) {
		data_.rowIdFilter_ = ALLOC_UNIQUE(
				cxt.getValueContext().getVarAllocator(), RowIdFilter,
				cxt.getAllocatorManager(), cxt.getAllocator().base(), maxRowId);
	}
	return *data_.rowIdFilter_;
}


SQLContainerImpl::CursorImpl::Data::Data(
		SQLValues::VarAllocator &varAlloc, Accessor &accessor) :
		outIndex_(std::numeric_limits<uint32_t>::max()),
		nextContainerId_(UNDEF_CONTAINERID),
		totalSearchCount_(0),
		lastPartialExecSize_(accessor.getPartialExecSizeRange().first),
		scanner_(NULL),
		cxtRefList_(varAlloc),
		exprCxtRefList_(varAlloc) {
}


SQLContainerImpl::CursorImpl::HolderImpl::HolderImpl(
		SQLValues::VarAllocator &varAlloc, Accessor &accessor,
		const OpStore::ResourceRef &accessorRef) :
		varAlloc_(varAlloc),
		accessorRef_(accessorRef),
		accessorPtr_(&accessor),
		data_(varAlloc, accessor) {
	resolveAccessor();
}

SQLContainerImpl::CursorImpl::HolderImpl::~HolderImpl() {
}

util::AllocUniquePtr<SQLContainerUtils::ScanCursor>::ReturnType
SQLContainerImpl::CursorImpl::HolderImpl::attach() {
	Accessor &accessor = resolveAccessor();
	util::AllocUniquePtr<ScanCursor> cursor(
			ALLOC_NEW(varAlloc_) CursorImpl(accessor, data_), varAlloc_);
	return cursor;
}

SQLContainerImpl::CursorAccessorImpl&
SQLContainerImpl::CursorImpl::HolderImpl::resolveAccessor() {
	typedef CursorAccessorImpl::ScanCursorAccessor ScanCursorAccessor;
	ScanCursorAccessor &accessor =
			accessorRef_.resolve().resolveAs<ScanCursorAccessor>();
	if (&accessor != accessorPtr_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return static_cast<CursorAccessorImpl&>(accessor);
}


SQLContainerImpl::CursorAccessorImpl::CursorAccessorImpl(const Source &source) :
		varAlloc_(source.varAlloc_),
		location_(source.location_),
		cxtSrc_(source.cxtSrc_),
		mergeMode_(MergeMode::END_MODE),
		closed_(false),
		rowIdFiltering_(false),
		resultSetPreserving_(false),
		containerExpired_(false),
		indexLost_(false),
		resultSetLost_(false),
		nullsStatsChanged_(false),
		resultSetId_(UNDEF_RESULTSETID),
		lastPartitionId_(UNDEF_PARTITIONID),
		lastDataStore_(NULL),
		container_(NULL),
		containerType_(UNDEF_CONTAINER),
		schemaVersionId_(UNDEF_SCHEMAVERSIONID),
		maxRowId_(UNDEF_ROWID),
		generalRowArray_(NULL),
		plainRowArray_(NULL),
		row_(NULL),
		initialNullsStats_(varAlloc_),
		lastNullsStats_(varAlloc_),
		indexLimit_(source.indexLimit_),
		memLimit_(source.memLimit_),
		partialExecSizeRange_(source.partialExecSizeRange_),
		indexSelection_(NULL) {
}

util::AllocUniquePtr<SQLContainerUtils::ScanCursor::Holder>::ReturnType
SQLContainerImpl::CursorAccessorImpl::createCursor(
		const OpStore::ResourceRef &accessorRef) {
	util::AllocUniquePtr<CursorImpl::Holder> holder(
			ALLOC_NEW(varAlloc_) CursorImpl::HolderImpl(
					varAlloc_, *this, accessorRef), varAlloc_);
	return holder;
}

SQLContainerImpl::CursorAccessorImpl::~CursorAccessorImpl() {
	destroy(false);
}

void SQLContainerImpl::CursorAccessorImpl::unlatch() throw() {
	finishScope();
}

void SQLContainerImpl::CursorAccessorImpl::close() throw() {
	destroy(true);
}

void SQLContainerImpl::CursorAccessorImpl::destroy(bool withResultSet) throw() {
	unlatch();

	if (closed_) {
		return;
	}

	closed_ = true;

	destroyUpdateRowIdHandler();

	if (resultSetId_ == UNDEF_RESULTSETID) {
		return;
	}

	const ResultSetId rsId = resultSetId_;
	const PartitionId partitionId = lastPartitionId_;
	DataStoreV4 *dataStore = lastDataStore_;

	resultSetId_ = UNDEF_RESULTSETID;
	lastPartitionId_ = UNDEF_PARTITIONID;
	lastDataStore_ = NULL;

	if (withResultSet) {
		static_cast<void>(partitionId);
		dataStore->getResultSetManager()->close(rsId);
	}
}

void SQLContainerImpl::CursorAccessorImpl::getIndexSpec(
		ExtOpContext &cxt, const TupleColumnList &inColumnList,
		SQLExprs::IndexSelector &selector) {
	startScope(cxt, inColumnList);

	TransactionContext &txn = resolveTransactionContext();
	BaseContainer *container = getContainer();
	if (container == NULL) {
		selector.completeIndex();
		return;
	}

	assignIndexInfo(txn, *container, selector, inColumnList);
}

bool SQLContainerImpl::CursorAccessorImpl::isIndexLost() {
	return indexLost_;
}

void SQLContainerImpl::CursorAccessorImpl::setIndexSelection(
		const SQLExprs::IndexSelector &selector) {
	indexSelection_ = &selector;
}

const SQLExprs::IndexSelector&
SQLContainerImpl::CursorAccessorImpl::getIndexSelection() {
	if (indexSelection_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *indexSelection_;
}

void SQLContainerImpl::CursorAccessorImpl::setRowIdFiltering() {
	rowIdFiltering_ = true;
}

bool SQLContainerImpl::CursorAccessorImpl::isRowIdFiltering() {
	return rowIdFiltering_;
}

void SQLContainerImpl::CursorAccessorImpl::setMergeMode(MergeMode::Type mode) {
	assert(mergeMode_ == MergeMode::END_MODE);
	mergeMode_ = mode;
}

const SQLOps::ContainerLocation&
SQLContainerImpl::CursorAccessorImpl::getLocation() {
	return location_;
}

SQLContainerUtils::ScanMergeMode::Type
SQLContainerImpl::CursorAccessorImpl::getMergeMode() {
	return mergeMode_;
}

int64_t SQLContainerImpl::CursorAccessorImpl::getIndexLimit() {
	return indexLimit_;
}

int64_t SQLContainerImpl::CursorAccessorImpl::getMemoryLimit() {
	return memLimit_;
}

const std::pair<uint64_t, uint64_t>&
SQLContainerImpl::CursorAccessorImpl::getPartialExecSizeRange() {
	return partialExecSizeRange_;
}

void SQLContainerImpl::CursorAccessorImpl::startScope(
		ExtOpContext &cxt, const TupleColumnList &inColumnList) {
	if (scope_.get() != NULL) {
		return;
	}

	scope_ = ALLOC_UNIQUE(varAlloc_, CursorScope, cxt, inColumnList, *this);
}

void SQLContainerImpl::CursorAccessorImpl::finishScope() throw() {
	scope_.reset();
}

SQLContainerImpl::CursorScope&
SQLContainerImpl::CursorAccessorImpl::resolveScope() {
	if (scope_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *scope_;
}

BaseContainer& SQLContainerImpl::CursorAccessorImpl::resolveContainer() {
	if (container_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (container_->isInvalid()) {
		TransactionContext &txn = resolveTransactionContext();
		FullContainerKey key = container_->getContainerKey(txn);
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::String keyStr(alloc);
		key.toString(alloc, keyStr) ;
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
			"can not continue to scan, invalid container, name=" << keyStr);
	}
	return *container_;
}

BaseContainer* SQLContainerImpl::CursorAccessorImpl::getContainer() {
	assert(scope_.get() != NULL);
	return container_;
}

TransactionContext&
SQLContainerImpl::CursorAccessorImpl::resolveTransactionContext() {
	return resolveScope().getTransactionContext();
}

ResultSet& SQLContainerImpl::CursorAccessorImpl::resolveResultSet() {
	return resolveScope().getResultSet();
}

BaseContainer::RowArray* SQLContainerImpl::CursorAccessorImpl::getRowArray() {
	assert(rowArray_.get() != NULL);
	return rowArray_.get();
}

bool SQLContainerImpl::CursorAccessorImpl::isValueNull(
		const ContainerColumn &column) {
	RowArray::Row row(row_, getRowArray());
	return row.isNullValue(column.getColumnInfo());
}

template<RowArrayType RAType, typename Ret, TupleColumnType T>
Ret SQLContainerImpl::CursorAccessorImpl::getValueDirect(
		SQLValues::ValueContext &cxt, const ContainerColumn &column) {
	return DirectValueAccessor<T>().template access<RAType, Ret>(
			cxt, *this, column);
}

template<TupleColumnType T>
void SQLContainerImpl::CursorAccessorImpl::writeValueAs(
		TupleListWriter &writer, const TupleList::Column &column,
		const typename SQLValues::TypeUtils::Traits<T>::ValueType &value) {
	WritableTuple tuple = writer.get();
	tuple.set(column, ValueUtils::toAnyByValue<T>(value));
}

void SQLContainerImpl::CursorAccessorImpl::clearResultSetPreserving(
		ResultSet &resultSet, bool force) {
	if (resultSetPreserving_ || (force && !resultSet.isRelease())) {
		resultSet.setPartialExecStatus(ResultSet::NOT_PARTIAL);
		resultSet.setResultType(RESULT_NONE);
		resultSet.resetExecCount();
		assert(resultSet.isRelease());
		resultSetPreserving_ = false;
	}
}

void SQLContainerImpl::CursorAccessorImpl::activateResultSetPreserving(
		ResultSet &resultSet) {
	if (resultSetPreserving_ || resultSet.isPartialExecuteSuspend()) {
		return;
	}

	resultSet.setPartialExecStatus(ResultSet::PARTIAL_SUSPENDED);
	resultSet.incrementExecCount();
	resultSetPreserving_ = true;
}

void SQLContainerImpl::CursorAccessorImpl::checkInputInfo(
		const TupleColumnList &list, const BaseContainer &container) {
	const uint32_t count = container.getColumnNum();

	if (count != list.size()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
				"Inconsistent column count");
	}

	for (uint32_t i = 0; i < count; i++) {
		const TupleColumnType expectedType = list[i].getType();
		const TupleColumnType actualType = resolveColumnType(
				container.getColumnInfo(i), false, false);

		if (SQLValues::TypeUtils::toNonNullable(expectedType) !=
				SQLValues::TypeUtils::toNonNullable(actualType)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
					"Inconsistent column type");
		}
	}

	for (uint32_t i = 0; i < count; i++) {
		if (container.getColumnInfo(i).getColumnId() != i) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
					"Inconsistent column ID");
		}
	}
}

TupleColumnType SQLContainerImpl::CursorAccessorImpl::resolveColumnType(
		const ColumnInfo &columnInfo, bool used, bool withStats) {
	static_cast<void>(used);
	static_cast<void>(withStats);

	bool nullable;
	if (columnInfo.getColumnId() == Constants::SCHEMA_INVALID_COLUMN_ID) {
		nullable = false;
	}
	else {
		nullable = isColumnSchemaNullable(columnInfo);
	}

	return ContainerValueUtils::toTupleColumnType(
			columnInfo.getColumnType(), nullable);
}

SQLContainerImpl::ContainerColumn
SQLContainerImpl::CursorAccessorImpl::resolveColumn(
		BaseContainer &container, bool forId, uint32_t pos) {
	if (forId) {
		if (container.getContainerType() == TIME_SERIES_CONTAINER) {
			ContainerColumn column =
					RowArray::getRowIdColumn<TimeSeries>(container);

			ColumnInfo modInfo = column.getColumnInfo();
			modInfo.setType(COLUMN_TYPE_TIMESTAMP);
			column.setColumnInfo(modInfo);

			return column;
		}
		else {
			return RowArray::getRowIdColumn<Collection>(container);
		}
	}
	else {
		const ColumnInfo &info = container.getColumnInfo(pos);
		if (container.getContainerType() == TIME_SERIES_CONTAINER) {
			return RowArray::getColumn<TimeSeries>(info);
		}
		else {
			return RowArray::getColumn<Collection>(info);
		}
	}
}

bool SQLContainerImpl::CursorAccessorImpl::isColumnNonNullable(uint32_t pos) {
	if (nullsStatsChanged_) {
		return false;
	}

	return !getBit(lastNullsStats_, pos);
}

bool SQLContainerImpl::CursorAccessorImpl::updateNullsStats(
		util::StackAllocator &alloc, const BaseContainer &container) {
	if (nullsStatsChanged_) {
		return true;
	}

	getNullsStats(alloc, container, lastNullsStats_);

	if (initialNullsStats_.empty()) {
		initialNullsStats_.assign(
				lastNullsStats_.begin(), lastNullsStats_.end());
		return true;
	}

	const size_t size = initialNullsStats_.size();
	return (lastNullsStats_.size() == size && memcmp(
			initialNullsStats_.data(), lastNullsStats_.data(), size) == 0);
}

SQLContainerImpl::TupleUpdateRowIdHandler*
SQLContainerImpl::CursorAccessorImpl::prepareUpdateRowIdHandler(
		TransactionContext &txn, BaseContainer &container,
		ResultSet &resultSet) {
	do {
		if (resultSet.getUpdateIdType() == ResultSet::UPDATE_ID_NONE) {
			break;
		}

		if (updateRowIdHandler_.get() != NULL &&
				updateRowIdHandler_->isUpdated() &&
				resultSet.getUpdateRowIdHandler() != NULL) {
			break;
		}

		if (updateRowIdHandler_.get() == NULL) {
			updateRowIdHandler_ = ALLOC_UNIQUE(
					varAlloc_, TupleUpdateRowIdHandler,
					cxtSrc_, getMaxRowId(txn, container));
		}

		if (resultSet.getUpdateRowIdHandler() == NULL) {
			util::StackAllocator *rsAlloc = resultSet.getRSAllocator();
			const size_t updateListThreshold = 0;
			resultSet.setUpdateRowIdHandler(
					updateRowIdHandler_->share(*rsAlloc),
					updateListThreshold);
		}

		if (container.checkRunTime(txn)) {
			updateRowIdHandler_->setUpdated();
		}
	}
	while (false);
	return updateRowIdHandler_.get();
}

void SQLContainerImpl::CursorAccessorImpl::acceptIndexLost() {
	indexLost_ = true;
}

void SQLContainerImpl::CursorAccessorImpl::initializeScanRange(
		TransactionContext &txn, BaseContainer &container) {
	getMaxRowId(txn, container);
	assert(maxRowId_ != UNDEF_ROWID);
}

RowId SQLContainerImpl::CursorAccessorImpl::getMaxRowId(
		TransactionContext& txn, BaseContainer &container) {
	if (maxRowId_ == UNDEF_ROWID) {
		maxRowId_ = resolveMaxRowId(txn, container);
		if (maxRowId_ == UNDEF_ROWID) {
			maxRowId_ = 0;
		}
	}
	assert(maxRowId_ != UNDEF_ROWID);
	return maxRowId_;
}

void SQLContainerImpl::CursorAccessorImpl::assignIndexInfo(
		TransactionContext &txn, BaseContainer &container,
		IndexSelector &indexSelector, const TupleColumnList &columnList) {
	typedef util::Vector<IndexInfo> IndexInfoList;
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	IndexInfoList indexInfoList(alloc);
	getIndexInfoList(txn, container, indexInfoList);

	util::Vector<IndexSelector::IndexFlags> flagsList(alloc);
	for (IndexInfoList::iterator it = indexInfoList.begin();
			it != indexInfoList.end(); ++it) {
		const util::Vector<uint32_t> &columnIds = it->columnIds_;

		flagsList.clear();
		bool acceptable = true;
		for (util::Vector<uint32_t>::const_iterator colIt =
				columnIds.begin(); colIt != columnIds.end(); ++colIt) {
			const uint32_t columnId = *colIt;

			ContainerColumnType containerColumnType;
			if (!ContainerValueUtils::toContainerColumnType(
					SQLValues::TypeUtils::toNonNullable(
							columnList[columnId].getType()),
					containerColumnType, false)) {
				acceptable = false;
				break;
			}

			const int32_t flags = NoSQLUtils::mapTypeToSQLIndexFlags(
					it->mapType, containerColumnType);
			if (flags == 0) {
				acceptable = false;
				break;
			}

			flagsList.push_back(flags);
		}

		if (!acceptable) {
			continue;
		}

		indexSelector.addIndex(columnIds, flagsList);
	}
	indexSelector.completeIndex();
}

void SQLContainerImpl::CursorAccessorImpl::getIndexInfoList(
		TransactionContext &txn, BaseContainer &container,
		util::Vector<IndexInfo> &indexInfoList) {
	container.getIndexInfoList(txn, indexInfoList);

	if (container.getContainerType() == TIME_SERIES_CONTAINER) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();

		util::Vector<uint32_t> columnIds(alloc);
		ColumnId firstColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		columnIds.push_back(firstColumnId);
		IndexInfo indexInfo(alloc, columnIds, MAP_TYPE_BTREE);

		indexInfoList.push_back(indexInfo);
	}
}

void SQLContainerImpl::CursorAccessorImpl::restoreRowIdFilter(
		RowIdFilter &rowIdFilter) {
	assert(rowIdFiltering_);
	if (rowIdFilterId_.get() != NULL) {
		rowIdFilter.load(cxtSrc_, *rowIdFilterId_);
	}
}

void SQLContainerImpl::CursorAccessorImpl::finishRowIdFilter(
		RowIdFilter *rowIdFilter) {
	if (rowIdFilter == NULL || !rowIdFiltering_) {
		return;
	}

	assert(rowIdFilterId_.get() == NULL);

	rowIdFilterId_ = UTIL_MAKE_LOCAL_UNIQUE(
			rowIdFilterId_, uint32_t, rowIdFilter->save(cxtSrc_));
}

TransactionContext&
SQLContainerImpl::CursorAccessorImpl::prepareTransactionContext(
		ExtOpContext &cxt) {
	EventContext *ec = getEventContext(cxt);
	if (ec->isOnIOWorker()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
				"Internal error by invalid event context");
	}

	util::StackAllocator &alloc = ec->getAllocator();

	const PartitionId partitionId = cxt.getEvent()->getPartitionId();

	{
		GetContainerPropertiesHandler handler;
		handler.partitionTable_ = getPartitionTable(cxt);
		const StatementHandler::ClusterRole clusterRole =
				(StatementHandler::CROLE_MASTER |
				StatementHandler::CROLE_FOLLOWER);
		const StatementHandler::PartitionRoleType partitionRole =
				StatementHandler::PROLE_OWNER;
		const StatementHandler::PartitionStatus partitionStatus =
				(StatementHandler::PSTATE_ON |
				StatementHandler::PSTATE_SYNC);
		if (cxt.isOnTransactionService()) {
			handler.checkExecutable(
					partitionId, clusterRole, partitionRole, partitionStatus,
					handler.partitionTable_);
		}
		else {
			return getTransactionManager(cxt)->putAuto(alloc);
		}
	}

	{
		const StatementId stmtId = 0;
		const TransactionManager::ContextSource src(
				QUERY_TQL, stmtId, UNDEF_CONTAINERID,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO,
				TransactionManager::AUTO_COMMIT, false,
				cxt.getStoreMemoryAgingSwapRate(),
				cxt.getTimeZone());
		return getTransactionManager(cxt)->put(
				alloc, partitionId, TXN_EMPTY_CLIENTID, src,
				ec->getHandlerStartTime(),
				ec->getHandlerStartMonotonicTime());
	}
}

DataStoreV4* SQLContainerImpl::CursorAccessorImpl::getDataStore(
		ExtOpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	const PartitionId pId = cxt.getEvent()->getPartitionId();
	DataStoreBase& dsBase =
			resourceSet->getPartitionList()->partition(pId).dataStore();
	return static_cast<DataStoreV4*>(&dsBase);
}

EventContext* SQLContainerImpl::CursorAccessorImpl::getEventContext(
		ExtOpContext &cxt) {
	return cxt.getEventContext();
}

TransactionManager*
SQLContainerImpl::CursorAccessorImpl::getTransactionManager(
		ExtOpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getTransactionManager();
}

TransactionService*
SQLContainerImpl::CursorAccessorImpl::getTransactionService(
		ExtOpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getTransactionService();
}

PartitionTable* SQLContainerImpl::CursorAccessorImpl::getPartitionTable(
		ExtOpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getPartitionTable();
}

ClusterService* SQLContainerImpl::CursorAccessorImpl::getClusterService(
		ExtOpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getClusterService();
}

SQLExecutionManager* SQLContainerImpl::CursorAccessorImpl::getExecutionManager(
		ExtOpContext &cxt) {
	return cxt.getExecutionManager();
}

SQLService* SQLContainerImpl::CursorAccessorImpl::getSQLService(ExtOpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getSQLService();
}

const ResourceSet* SQLContainerImpl::CursorAccessorImpl::getResourceSet(
		ExtOpContext &cxt) {
	return cxt.getResourceSet();
}

template<RowArrayType RAType, typename T>
const T& SQLContainerImpl::CursorAccessorImpl::getFixedValue(
		const ContainerColumn &column) {
	return *static_cast<const T*>(getFixedValueAddr<RAType>(column));
}

template<RowArrayType RAType>
const void* SQLContainerImpl::CursorAccessorImpl::getFixedValueAddr(
		const ContainerColumn &column) {
	return getRowArrayImpl<RAType>(true)->getRowCursor().getFixedField(column);
}

template<RowArrayType RAType>
inline const void* SQLContainerImpl::CursorAccessorImpl::getVariableArray(
		const ContainerColumn &column) {
	return getRowArrayImpl<RAType>(true)->getRowCursor().getVariableField(
			column);
}

void SQLContainerImpl::CursorAccessorImpl::initializeRowArray(
		TransactionContext &txn, BaseContainer &container) {
	rowArray_ = UTIL_MAKE_LOCAL_UNIQUE(rowArray_, RowArray, txn, &container);

	generalRowArray_ = rowArray_->getImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>();
	plainRowArray_ = rowArray_->getImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_PLAIN>();
}

void SQLContainerImpl::CursorAccessorImpl::destroyRowArray() {
	generalRowArray_ = NULL;
	plainRowArray_ = NULL;
	rowArray_.reset();

	row_ = NULL;
}

template<>
BaseContainer::RowArrayImpl<
		BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>*
SQLContainerImpl::CursorAccessorImpl::getRowArrayImpl(bool loaded) {
	static_cast<void>(loaded);
	assert(generalRowArray_ != NULL);
	return generalRowArray_;
}

template<>
BaseContainer::RowArrayImpl<
		BaseContainer, BaseContainer::ROW_ARRAY_PLAIN>*
SQLContainerImpl::CursorAccessorImpl::getRowArrayImpl(bool loaded) {
	static_cast<void>(loaded);
	assert(plainRowArray_ != NULL);
	assert(!loaded || getRowArray()->getRowArrayType() ==
			BaseContainer::ROW_ARRAY_PLAIN);
	return plainRowArray_;
}

void SQLContainerImpl::CursorAccessorImpl::destroyUpdateRowIdHandler() throw() {
	updateRowIdHandler_.reset();
}

RowId SQLContainerImpl::CursorAccessorImpl::resolveMaxRowId(
		TransactionContext& txn, BaseContainer &container) {
	const ContainerType containerType = container.getContainerType();

	if (containerType == COLLECTION_CONTAINER) {
		return static_cast<Collection&>(container).getMaxRowId(txn);
	}
	else if (containerType == TIME_SERIES_CONTAINER) {
		return static_cast<TimeSeries&>(container).getMaxRowId(txn);
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

bool SQLContainerImpl::CursorAccessorImpl::isColumnSchemaNullable(
		const ColumnInfo &info) {
	return !info.isNotNull();
}

void SQLContainerImpl::CursorAccessorImpl::getNullsStats(
		util::StackAllocator &alloc, const BaseContainer &container,
		NullsStats &nullsStats) {
	const uint32_t columnCount = container.getColumnNum();
	initializeBits(nullsStats, columnCount);
	const size_t bitsSize =  nullsStats.size();

	{
		util::StackAllocator::Scope scope(alloc);
		util::XArray<uint8_t> localStats(alloc);
		container.getNullsStats(localStats);
		nullsStats.assign(localStats.begin(), localStats.end());
	}

	if (nullsStats.size() != bitsSize) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLContainerImpl::CursorAccessorImpl::initializeBits(
		NullsStats &bits, size_t bitCount) {
	const uint8_t unitBits = 0;
	bits.assign((bitCount + CHAR_BIT - 1) / CHAR_BIT, unitBits);
}

void SQLContainerImpl::CursorAccessorImpl::setBit(
		NullsStats &bits, size_t index, bool value) {
	uint8_t &unitBits = bits[index / CHAR_BIT];
	const uint8_t targetBit = static_cast<uint8_t>(1 << (index % CHAR_BIT));

	if (value) {
		unitBits |= targetBit;
	}
	else {
		unitBits &= targetBit;
	}
}

bool SQLContainerImpl::CursorAccessorImpl::getBit(
		const NullsStats &bits, size_t index) {
	const uint8_t &unitBits = bits[index / CHAR_BIT];
	const uint8_t targetBit = static_cast<uint8_t>(1 << (index % CHAR_BIT));

	return ((unitBits & targetBit) != 0);
}


template<TupleColumnType T, bool VarSize>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorAccessorImpl::DirectValueAccessor<
		T, VarSize>::access(
		SQLValues::ValueContext &cxt, CursorAccessorImpl &base,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	UTIL_STATIC_ASSERT(!VarSize);
	return base.getFixedValue<RAType, Ret>(column);
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorAccessorImpl::DirectValueAccessor<
		TupleTypes::TYPE_BOOL, false>::access(
		SQLValues::ValueContext &cxt, CursorAccessorImpl &base,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	return !!base.getFixedValue<RAType, int8_t>(column);
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorAccessorImpl::DirectValueAccessor<
		TupleTypes::TYPE_NANO_TIMESTAMP, false>::access(
		SQLValues::ValueContext &cxt, CursorAccessorImpl &base,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	return TupleNanoTimestamp(
			&base.getFixedValue<RAType, NanoTimestamp>(column));
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorAccessorImpl::DirectValueAccessor<
		TupleTypes::TYPE_STRING, true>::access(
		SQLValues::ValueContext &cxt, CursorAccessorImpl &base,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	return TupleString(base.getVariableArray<RAType>(column));
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorAccessorImpl::DirectValueAccessor<
		TupleTypes::TYPE_BLOB, true>::access(
		SQLValues::ValueContext &cxt, CursorAccessorImpl &base,
		const ContainerColumn &column) const {

	const void *data = base.getVariableArray<RAType>(column);
	const BaseObject &baseObject = *base.frontFieldCache_;

	return TupleValue::LobBuilder::fromDataStore(
			cxt.getVarContext(), baseObject, data);
}


const uint16_t SQLContainerImpl::Constants::SCHEMA_INVALID_COLUMN_ID =
		std::numeric_limits<uint16_t>::max();
const uint32_t SQLContainerImpl::Constants::ESTIMATED_ROW_ARRAY_ROW_COUNT =
		50;


SQLContainerImpl::ColumnCode::ColumnCode() :
		accessor_(NULL) {
}

void SQLContainerImpl::ColumnCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLCoreExprs::VariantUtils VariantUtils;

	CursorRef &ref = cxt.getInputSourceRef(0).resolveAs<CursorRef>();

	CursorAccessorImpl &accessor = ref.resolveAccessor();
	BaseContainer &container = accessor.resolveContainer();

	const ExprCode &code = VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	const bool forId = (code.getType() == SQLType::EXPR_ID);
	column_ = CursorAccessorImpl::resolveColumn(
			container, forId, code.getColumnPos());
	accessor_ = &accessor;
}


template<RowArrayType RAType, typename T, bool Nullable>
inline typename
SQLContainerImpl::ColumnEvaluator<RAType, T, Nullable>::ResultType
SQLContainerImpl::ColumnEvaluator<RAType, T, Nullable>::eval(
		ExprContext &cxt) const {
	if (Nullable && code_.accessor_->isValueNull(code_.column_)) {
		return ResultType(ValueType(), true);
	}
	return ResultType(
			code_.accessor_->getValueDirect<
					RAType, ValueType, T::COLUMN_TYPE>(
							cxt.getValueContext(), code_.column_),
			false);
}


SQLContainerImpl::IdExpression::VariantBase::~VariantBase() {
}

void SQLContainerImpl::IdExpression::VariantBase::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	static_cast<void>(code);

	columnCode_.initialize(cxt, this, NULL);
}

template<size_t V>
TupleValue SQLContainerImpl::IdExpression::VariantAt<V>::eval(
		ExprContext &cxt) const {
	const int64_t value =
			Evaluator::get(Evaluator::evaluator(columnCode_).eval(cxt));
	return TupleValue(value);
}


void SQLContainerImpl::GeneralCondEvaluator::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(argIndex);

	expr_ = expr;
}


void SQLContainerImpl::VarCondEvaluator::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(argIndex);

	expr_ = expr;
}


template<typename Func, size_t V>
void SQLContainerImpl::ComparisonEvaluator<Func, V>::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLCoreExprs::VariantUtils VariantUtils;

	const ExprCode &code = VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	expr_.initializeCustom(cxt, code);
}


void SQLContainerImpl::EmptyCondEvaluator::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(expr);
	static_cast<void>(argIndex);
}


template<typename Base, BaseContainer::RowArrayType RAType>
SQLContainerImpl::RowIdCondEvaluator<Base, RAType>::RowIdCondEvaluator() :
		rowIdFilter_(NULL),
		accessor_(NULL) {
}

template<typename Base, BaseContainer::RowArrayType RAType>
inline bool SQLContainerImpl::RowIdCondEvaluator<Base, RAType>::eval(
		ExprContext &cxt) const {
	const typename Base::ResultType &matched =
			Base::evaluator(baseCode_).eval(cxt);

	if (!Base::check(matched) || !Base::get(matched)) {
		return false;
	}

	const RowId rowId = accessor_->getValueDirect<
			RAType, RowId, TupleTypes::TYPE_LONG>(
					cxt.getValueContext(), column_);
	if (!rowIdFilter_->accept(rowId)) {
		return false;
	}

	return true;
}

template<typename Base, BaseContainer::RowArrayType RAType>
void SQLContainerImpl::RowIdCondEvaluator<Base, RAType>::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	baseCode_.initialize(cxt, expr, argIndex);

	CursorRef &ref = cxt.getInputSourceRef(0).resolveAs<CursorRef>();

	rowIdFilter_ = ref.findRowIdFilter();
	assert(rowIdFilter_ != NULL);

	CursorAccessorImpl &accessor = ref.resolveAccessor();
	BaseContainer &container = accessor.resolveContainer();

	const bool forId = true;
	column_ = CursorAccessorImpl::resolveColumn(container, forId, 0);
	accessor_ = &accessor;
}


void SQLContainerImpl::GeneralProjector::initialize(
		ProjectionFactoryContext &cxt, const Projection *proj) {
	static_cast<void>(cxt);

	proj_ = proj;
}

bool SQLContainerImpl::GeneralProjector::update(OpContext &cxt) {
	proj_->updateProjectionContext(cxt);
	return true;
}


bool SQLContainerImpl::CountProjector::update(OpContext &cxt) {
	SQLValues::VarContext::Scope varScope(cxt.getVarContext());

	SQLExprs::ExprContext &exprCxt = cxt.getExprContext();

	const int64_t totalCount =
			exprCxt.getAggregationValueAs<SQLValues::Types::Long>(column_) +
			counter_;

	exprCxt.setAggregationValueBy<SQLValues::Types::Long>(column_, totalCount);
	counter_ = 0;

	return true;
}

void SQLContainerImpl::CountProjector::initialize(
		ProjectionFactoryContext &cxt, const Projection *proj) {
	const SQLExprs::Expression &countExpr = proj->child();
	assert(countExpr.getCode().getType() == SQLType::AGG_COUNT_ALL);

	const uint32_t aggrIndex = countExpr.getCode().getAggregationIndex();
	assert(aggrIndex != std::numeric_limits<uint32_t>::max());

	SQLValues::SummaryTupleSet *tupleSet =
			cxt.getExprFactoryContext().getAggregationTupleSet();
	assert(tupleSet != NULL);

	column_ = tupleSet->getModifiableColumnList()[aggrIndex];
}


SQLContainerImpl::CursorRef::CursorRef() :
		cursor_(NULL),
		accessor_(NULL),
		rowIdFilter_(NULL) {
}

SQLContainerImpl::CursorImpl& SQLContainerImpl::CursorRef::resolveCursor() {
	if (cursor_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *cursor_;
}

SQLContainerImpl::CursorAccessorImpl&
SQLContainerImpl::CursorRef::resolveAccessor() {
	if (accessor_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *accessor_;
}

SQLContainerImpl::RowIdFilter* SQLContainerImpl::CursorRef::findRowIdFilter() {
	return rowIdFilter_;
}

void SQLContainerImpl::CursorRef::setCursor(
		CursorImpl *cursor, CursorAccessorImpl *accessor,
		RowIdFilter *rowIdFilter) {
	cursor_ = cursor;
	accessor_ = accessor;
	rowIdFilter_ = rowIdFilter;
}

void SQLContainerImpl::CursorRef::addContextRef(OpContext **ref) {
	resolveCursor().addContextRef(ref);
}

void SQLContainerImpl::CursorRef::addContextRef(ExprContext **ref) {
	resolveCursor().addContextRef(ref);
}


size_t
SQLContainerImpl::ScannerHandlerBase::HandlerVariantUtils::toInputSourceNumber(
		InputSourceType type) {
	switch (type) {
	case ExprCode::INPUT_CONTAINER_GENERAL:
		return 0;
	case ExprCode::INPUT_CONTAINER_PLAIN:
		return 1;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

size_t
SQLContainerImpl::ScannerHandlerBase::HandlerVariantUtils::toCompFuncNumber(
		ExprType type) {
	switch (type) {
	case SQLType::OP_LT:
		return 0;
	case SQLType::OP_GT:
		return 1;
	case SQLType::OP_LE:
		return 2;
	case SQLType::OP_GE:
		return 3;
	case SQLType::OP_EQ:
		return 4;
	case SQLType::OP_NE:
		return 5;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLContainerImpl::ScannerHandlerBase::HandlerVariantUtils::setUpContextRef(
		ProjectionFactoryContext &cxt, OpContext **ref, ExprContext **exprRef) {
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	ExprFactoryContext &exprCxt = cxt.getExprFactoryContext();
	CursorRef &cursorRef = exprCxt.getInputSourceRef(0).resolveAs<CursorRef>();

	cursorRef.addContextRef(ref);
	cursorRef.addContextRef(exprRef);
}

void SQLContainerImpl::ScannerHandlerBase::HandlerVariantUtils::addUpdator(
		ProjectionFactoryContext &cxt, const UpdatorEntry &updator) {
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	ExprFactoryContext &exprCxt = cxt.getExprFactoryContext();
	CursorRef &cursorRef = exprCxt.getInputSourceRef(0).resolveAs<CursorRef>();

	if (updator.first != NULL) {
		cursorRef.resolveCursor().addScannerUpdator(updator);
	}
}

std::pair<const SQLExprs::Expression*, const SQLOps::Projection*>
SQLContainerImpl::ScannerHandlerBase::HandlerVariantUtils::
getFilterProjectionElements(const Projection &src) {
	const SQLExprs::Expression *filterExpr = NULL;
	const SQLOps::Projection *chainProj = NULL;
	do {
		if (src.getProjectionCode().getType() != SQLOpTypes::PROJ_FILTER) {
			break;
		}

		const SQLExprs::Expression *filterExprBase = src.findChild();

		filterExpr = filterExprBase;
		chainProj = &src.chainAt(0);
	}
	while (false);

	if (chainProj == NULL) {
		filterExpr = NULL;
		chainProj = &src;
	}

	return std::make_pair(filterExpr, chainProj);
}


size_t SQLContainerImpl::ScannerHandlerBase::VariantTraits::resolveVariant(
		FactoryContext &cxt, const Projection &proj) {
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;
	typedef SQLExprs::ExprTypeUtils ExprTypeUtils;

	ExprFactoryContext &exprCxt = cxt.first->getExprFactoryContext();

	CursorRef &ref = exprCxt.getInputSourceRef(0).resolveAs<CursorRef>();
	const bool rowIdFiltering = (ref.findRowIdFilter() != NULL);
	size_t condNumBase = (rowIdFiltering ? COUNT_NO_COMP_FILTER_BASE : 0);

	const size_t sourceNum = HandlerVariantUtils::toInputSourceNumber(
			exprCxt.getInputSourceType(0));

	const std::pair<const Expression*, const Projection*> &elems =
			HandlerVariantUtils::getFilterProjectionElements(proj);
	const Expression *condExpr = elems.first;
	const Projection *chainProj = elems.second;

	size_t condNum;
	do {
		if (condExpr == NULL) {
			condNum = EMPTY_FILTER_NUM;
			break;
		}

		const TupleColumnType condColumnType =
				SQLValues::TypeUtils::findConversionType(
						condExpr->getCode().getColumnType(),
						SQLValues::TypeUtils::toNullable(TupleTypes::TYPE_BOOL),
						true, true);
		if (SQLValues::TypeUtils::isNull(condColumnType)) {
			condNum = EMPTY_FILTER_NUM;
			break;
		}

		const ExprCode &condCode = condExpr->getCode();
		if (condCode.getType() == SQLType::EXPR_CONSTANT &&
				SQLValues::ValueUtils::isTrue(condCode.getValue())) {
			condNum = EMPTY_FILTER_NUM;
			break;
		}

		if (SQLExprs::ExprRewriter::findVarGenerativeExpr(*condExpr)) {
			condNum = VAR_FILTER_NUM;
			break;
		}

		if (rowIdFiltering) {
			condNum = GENERAL_FILTER_NUM;
			break;
		}

		const bool withNe = true;
		if (!ExprTypeUtils::isCompOp(condCode.getType(), withNe)) {
			condNum = GENERAL_FILTER_NUM;
			break;
		}

		ExprFactoryContext::Scope scope(exprCxt);
		exprCxt.setBaseExpression(condExpr);

		SQLExprs::ExprProfile exprProfile;
		exprCxt.setProfile(&exprProfile);

		const size_t baseNum =
				CoreCompExpr::VariantTraits::resolveVariant(exprCxt, condCode);

		SQLOps::OpProfilerOptimizationEntry *profiler =
				cxt.first->getProfiler();
		if (profiler != NULL) {
			SQLValues::ProfileElement &elem =
					profiler->get(SQLOpTypes::OPT_OP_COMP_CONST);
			elem.merge(exprProfile.compConst_);
			elem.merge(exprProfile.compColumns_);
		}

		if (baseNum < OFFSET_COMP_CONTAINER) {
			condNum = GENERAL_FILTER_NUM;
			break;
		}

		const size_t compFuncNum =
				HandlerVariantUtils::toCompFuncNumber(condCode.getType());
		condNumBase = 0;
		condNum = COUNT_NO_COMP_FILTER +
				compFuncNum * COUNT_COMP_BASE +
				((baseNum - OFFSET_COMP_CONTAINER) % COUNT_COMP_BASE);
	}
	while (false);

	size_t projNum = 0;
	do {
		if (chainProj->getProjectionCode().getType() != SQLOpTypes::PROJ_AGGR_PIPE ||
				chainProj->getProjectionCode().getAggregationPhase() !=
						SQLType::AGG_PHASE_ADVANCE_PIPE ||
				chainProj->findChild() == NULL) {
			break;
		}

		const SQLExprs::Expression &child = chainProj->child();
		if (child.findNext() != NULL ||
				child.getCode().getType() != SQLType::AGG_COUNT_ALL) {
			break;
		}

		projNum = 1;
	}
	while (false);

	return
			sourceNum * (COUNT_PROJECTION * COUNT_FILTER) +
			projNum * COUNT_FILTER +
			condNumBase + condNum;
}


template<size_t V>
SQLContainerImpl::ScannerHandlerBase::VariantAt<V>::VariantAt(
		ProjectionFactoryContext &cxt, const Projection &proj) :
		cxt_(NULL),
		exprCxt_(NULL) {
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	HandlerVariantUtils::setUpContextRef(cxt, &cxt_, &exprCxt_);

	const std::pair<const Expression*, const Projection*> &elems =
			HandlerVariantUtils::getFilterProjectionElements(proj);

	ExprFactoryContext &exprCxt = cxt.getExprFactoryContext();
	exprCxt.setBaseExpression(elems.first);
	condCode_.initialize(exprCxt, elems.first, NULL);

	projector_.initialize(cxt, elems.second);

	HandlerVariantUtils::addUpdator(cxt, projector_.getUpdator());
}

template<size_t V>
template<BaseContainer::RowArrayType InRAType>
inline void SQLContainerImpl::ScannerHandlerBase::VariantAt<V>::operator()(
		TransactionContext&,
		typename BaseContainer::RowArrayImpl<
				BaseContainer, InRAType>&) {

	const ResultType &matched =
			Evaluator::evaluator(condCode_).eval(*exprCxt_);

	if (!Evaluator::check(matched) || !Evaluator::get(matched)) {
		return;
	}

	projector_.project(*cxt_);
}

template<size_t V>
inline bool SQLContainerImpl::ScannerHandlerBase::VariantAt<V>::update(
		TransactionContext&) {
	return true;
}


const SQLContainerImpl::ScannerHandlerFactory
&SQLContainerImpl::ScannerHandlerFactory::FACTORY_INSTANCE =
		getInstanceForRegistrar();

const SQLContainerImpl::ScannerHandlerFactory&
SQLContainerImpl::ScannerHandlerFactory::getInstance() {
	return FACTORY_INSTANCE;
}

SQLContainerImpl::ScannerHandlerFactory&
SQLContainerImpl::ScannerHandlerFactory::getInstanceForRegistrar() {
	static ScannerHandlerFactory instance;
	return instance;
}

ContainerRowScanner& SQLContainerImpl::ScannerHandlerFactory::create(
		OpContext &cxt, CursorImpl &cursor, CursorAccessorImpl &accessor,
		const Projection &proj, RowIdFilter *rowIdFilter) const {
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef ExprCode::InputSourceType InputSourceType;

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	ProjectionFactoryContext &projCxt = builder.getProjectionFactoryContext();
	cxt.setUpProjectionFactoryContext(projCxt, NULL);

	ContainerRowScanner::HandlerSet handlerSet;
	FactoryContext factoryCxt(&projCxt, &handlerSet);

	ExprFactoryContext &exprCxt = projCxt.getExprFactoryContext();
	exprCxt.setPlanning(false);
	exprCxt.getInputSourceRef(0) = ALLOC_UNIQUE(cxt.getAllocator(), CursorRef);
	exprCxt.getInputSourceRef(0).resolveAs<CursorRef>().setCursor(
			&cursor, &accessor, rowIdFilter);

	const uint32_t columnCount = exprCxt.getInputColumnCount(0);
	for (uint32_t i = 0; i < columnCount; i++) {
		if (accessor.isColumnNonNullable(i)) {
			exprCxt.setInputType(0, i, SQLValues::TypeUtils::toNonNullable(
					exprCxt.getInputType(0, i)));
		}
	}

	const InputSourceType typeList[] = {
		ExprCode::INPUT_CONTAINER_GENERAL,
		ExprCode::INPUT_CONTAINER_PLAIN,
		ExprCode::END_INPUT
	};

	for (const InputSourceType *it = typeList; *it != ExprCode::END_INPUT; ++it) {
		ExprFactoryContext::Scope scope(exprCxt);
		exprCxt.setInputSourceType(0, *it);
		builder.getExprRewriter().setCodeSetUpAlways(true);

		Projection &destProj = builder.rewriteProjection(proj);
		destProj.initializeProjection(cxt);
		create(factoryCxt, destProj);
	}

	RowArray *rowArray = accessor.getRowArray();
	return *(ALLOC_NEW(cxt.getAllocator()) ContainerRowScanner(
			handlerSet, *rowArray, NULL));
}

ContainerRowScanner::HandlerSet&
SQLContainerImpl::ScannerHandlerFactory::create(
			FactoryContext &cxt, const Projection &proj) const {
	if (factoryFunc_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return factoryFunc_(cxt, proj);
}

void SQLContainerImpl::ScannerHandlerFactory::setFactoryFunc(FactoryFunc func) {
	if (factoryFunc_ != NULL) {
	}
	factoryFunc_ = func;
}

SQLContainerImpl::ScannerHandlerFactory::ScannerHandlerFactory() :
		factoryFunc_(NULL) {
}


SQLContainerImpl::BaseRegistrar::BaseRegistrar(
		const BaseRegistrar &subRegistrar) throw() :
		factory_(NULL) {
	try {
		subRegistrar();
	}
	catch (...) {
		assert(false);
	}
}

void SQLContainerImpl::BaseRegistrar::operator()() const {
	assert(false);
}

SQLContainerImpl::BaseRegistrar::BaseRegistrar() throw() :
		factory_(&ScannerHandlerFactory::getInstanceForRegistrar()) {
}

template<SQLExprs::ExprType T, typename E>
void SQLContainerImpl::BaseRegistrar::addExprVariants() const {
	SQLExprs::VariantsRegistrarBase<ExprVariantTraits>::add<T, E>();
}

template<typename S>
void SQLContainerImpl::BaseRegistrar::addScannerVariants() const {
	ScannerHandlerFactory::getInstanceForRegistrar().setFactoryFunc(
			SQLExprs::VariantsRegistrarBase<ScannerVariantTraits>::add<0, S>());
}

template<typename S, size_t Begin, size_t End>
void SQLContainerImpl::BaseRegistrar::addScannerVariants() const {
	ScannerHandlerFactory::getInstanceForRegistrar().setFactoryFunc(
			SQLExprs::VariantsRegistrarBase<ScannerVariantTraits>::add<0, S, Begin, End>());
}


template<typename V>
SQLContainerImpl::BaseRegistrar::ScannerVariantTraits::ObjectType&
SQLContainerImpl::BaseRegistrar::ScannerVariantTraits::create(
		ContextType &cxt, const CodeType &code) {
	V *handler = ALLOC_NEW(cxt.first->getAllocator()) V(*cxt.first, code);
	cxt.second->getEntry<V::ROW_ARRAY_TYPE>() =
			ContainerRowScanner::createHandlerEntry(*handler);
	return *cxt.second;
}


const SQLContainerImpl::BaseRegistrar
SQLContainerImpl::DefaultRegistrar::REGISTRAR_INSTANCE((DefaultRegistrar()));

void SQLContainerImpl::DefaultRegistrar::operator()() const {
	typedef SQLCoreExprs::Functions Functions;

	addExprVariants<SQLType::EXPR_ID, IdExpression>();
	addExprVariants<SQLType::EXPR_COLUMN, ColumnExpression>();

	addExprVariants< SQLType::OP_LT, ComparisonExpression<Functions::Lt> >();
	addExprVariants< SQLType::OP_LE, ComparisonExpression<Functions::Le> >();
	addExprVariants< SQLType::OP_GT, ComparisonExpression<Functions::Gt> >();
	addExprVariants< SQLType::OP_GE, ComparisonExpression<Functions::Ge> >();
	addExprVariants< SQLType::OP_EQ, ComparisonExpression<Functions::Eq> >();
	addExprVariants< SQLType::OP_NE, ComparisonExpression<Functions::Ne> >();

	addScannerVariants<ScannerHandlerBase>();
}


SQLContainerImpl::SearchResult::SearchResult(util::StackAllocator &alloc) :
		normalEntry_(alloc),
		mvccEntry_(alloc) {
}

inline bool SQLContainerImpl::SearchResult::isEmpty() const{
	return (
			normalEntry_.isEntryEmpty() &&
			mvccEntry_.isEntryEmpty());
}

inline size_t SQLContainerImpl::SearchResult::getSize() const{
	return (
			normalEntry_.getEntrySize() +
			mvccEntry_.getEntrySize());
}

inline SQLContainerImpl::SearchResult::Entry&
SQLContainerImpl::SearchResult::getEntry(bool onMvcc){
	return (onMvcc ? mvccEntry_ : normalEntry_);
}

inline const SQLContainerImpl::SearchResult::Entry&
SQLContainerImpl::SearchResult::getEntry(bool onMvcc) const{
	return (onMvcc ? mvccEntry_ : normalEntry_);
}

inline SQLContainerImpl::SearchResult::OIdListRef
SQLContainerImpl::SearchResult::getOIdListRef(bool forRowArray) {
	return OIdListRef(
			&normalEntry_.getOIdList(forRowArray),
			&mvccEntry_.getOIdList(forRowArray));
}


SQLContainerImpl::SearchResult::Entry::Entry(util::StackAllocator &alloc) :
		rowOIdList_(alloc),
		rowArrayOIdList_(alloc) {
}

inline bool SQLContainerImpl::SearchResult::Entry::isEntryEmpty() const {
	return (rowOIdList_.empty() && rowArrayOIdList_.empty());
}

inline size_t SQLContainerImpl::SearchResult::Entry::getEntrySize() const {
	return (rowOIdList_.size() + rowArrayOIdList_.size());
}

inline SQLContainerImpl::SearchResult::OIdList&
SQLContainerImpl::SearchResult::Entry::getOIdList(bool forRowArray) {
	return (forRowArray ? rowArrayOIdList_ : rowOIdList_);
}

inline const SQLContainerImpl::SearchResult::OIdList&
SQLContainerImpl::SearchResult::Entry::getOIdList(bool forRowArray) const {
	return (forRowArray ? rowArrayOIdList_ : rowOIdList_);
}

inline void SQLContainerImpl::SearchResult::Entry::swap(Entry &another) {
	rowOIdList_.swap(another.rowOIdList_);
	rowArrayOIdList_.swap(another.rowArrayOIdList_);
}


SQLContainerImpl::CursorScope::CursorScope(
		ExtOpContext &cxt, const TupleColumnList &inColumnList,
		CursorAccessorImpl &accessor) :
		accessor_(accessor),
		txn_(CursorAccessorImpl::prepareTransactionContext(cxt)),
		txnSvc_(CursorAccessorImpl::getTransactionService(cxt)),
		partitionGuard_(getPartitionLock(txn_, cxt)),
		containerAutoPtr_(
				txn_.getDefaultAllocator(),
				getContainer(
						txn_, cxt, getStoreScope(txn_, cxt, scope_),
						accessor_.location_,
						accessor_.containerExpired_,
						accessor_.containerType_)),
		resultSet_(prepareResultSet(
				cxt, txn_,
				checkContainer(
						inColumnList, containerAutoPtr_.get(),
						accessor_.location_,
						accessor_.schemaVersionId_),
				rsGuard_, accessor_)),
		partialExecCount_((resultSet_ == NULL ?
				0 : resultSet_->getPartialExecuteCount())) {
	try {
		initialize(cxt);
	}
	catch (...) {
		clear(true);
		throw;
	}
}

SQLContainerImpl::CursorScope::~CursorScope() {
	try {
		clear(false);
	}
	catch (...) {
		assert(false);
	}
}

TransactionContext& SQLContainerImpl::CursorScope::getTransactionContext() {
	return txn_;
}

ResultSet& SQLContainerImpl::CursorScope::getResultSet() {
	assert(resultSet_ != NULL);
	return *resultSet_;
}

int64_t SQLContainerImpl::CursorScope::getStartPartialExecCount() {
	return partialExecCount_;
}

uint64_t SQLContainerImpl::CursorScope::getElapsedNanos() {
	return watch_.elapsedNanos();
}

void SQLContainerImpl::CursorScope::initialize(ExtOpContext &cxt) {
	static_cast<void>(cxt);

	watch_.start();

	BaseContainer *container = containerAutoPtr_.get();

	try {
	accessor_.container_ = container;
	accessor_.containerExpired_ = (container == NULL);
	accessor_.resultSetId_ = findResultSetId(resultSet_);

	accessor_.lastPartitionId_ = txn_.getPartitionId();
	accessor_.lastDataStore_ =
			(container == NULL ? NULL : container->getDataStore());

	if (container == NULL) {
		return;
	}
	if (container->isInvalid()) {
		FullContainerKey key = container->getContainerKey(txn_);
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		util::String keyStr(alloc);
		key.toString(alloc, keyStr) ;
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
			"can not continue to scan, invalid container, name=" << keyStr);
	}

	accessor_.containerType_ = container->getContainerType();
	accessor_.schemaVersionId_ = container->getVersionId();

	accessor_.initializeScanRange(txn_, *container);
	accessor_.initializeRowArray(txn_, *container);

	DataStoreV4 *dataStore = container->getDataStore();
	ObjectManagerV4 *objectManager = dataStore->getObjectManager();
	accessor_.frontFieldCache_ = UTIL_MAKE_LOCAL_UNIQUE(
			accessor_.frontFieldCache_, BaseObject,
			*objectManager, container->getRowAllocateStrategy());
	}
	catch (std::exception& e) {
		container->handleSearchError(txn_, e, GS_ERROR_DS_CON_STATUS_INVALID);
	}
}

void SQLContainerImpl::CursorScope::clear(bool force) {
	if (containerAutoPtr_.get() == NULL) {
		return;
	}
	assert(accessor_.container_ != NULL);

	accessor_.destroyRowArray();
	accessor_.frontFieldCache_.reset();
	accessor_.container_ = NULL;

	clearResultSet(force);
}

void SQLContainerImpl::CursorScope::clearResultSet(bool force) {
	if (resultSet_ == NULL) {
		return;
	}

	bool closeable = (force || resultSet_->isRelease());
	if (!closeable && accessor_.resultSetHolder_.isEmpty()) {
		assert(txnSvc_ != NULL);
		assert(accessor_.lastPartitionId_ != UNDEF_PARTITIONID);
		try {
			accessor_.resultSetHolder_.assign(
					txnSvc_->getResultSetHolderManager(),
					accessor_.lastPartitionId_,
					accessor_.resultSetId_);
		}
		catch (...) {
			accessor_.resultSetLost_ = true;
			closeable = true;
		}
	}

	if (closeable) {
		resultSet_->setResultType(RESULT_ROWSET);
		resultSet_->setPartialMode(false);

		accessor_.resultSetId_ = UNDEF_RESULTSETID;
		accessor_.resultSetPreserving_ = false;
		accessor_.resultSetHolder_.release();

		accessor_.lastPartitionId_ = UNDEF_PARTITIONID;
		accessor_.lastDataStore_ = NULL;
		accessor_.destroyUpdateRowIdHandler();
	}

	rsGuard_.reset();
	resultSet_ = NULL;
}

util::Mutex* SQLContainerImpl::CursorScope::getPartitionLock(
		TransactionContext &txn, ExtOpContext &cxt) {
	if (!cxt.isOnTransactionService()) {
		return NULL;
	}

	PartitionList *list =
			CursorAccessorImpl::getResourceSet(cxt)->getPartitionList();
	return &list->partition(txn.getPartitionId()).mutex();
}

const SQLContainerImpl::CursorScope::DataStoreScope*
SQLContainerImpl::CursorScope::getStoreScope(
		TransactionContext &txn, ExtOpContext &cxt,
		util::LocalUniquePtr<DataStoreScope> &scope) {
	if (!cxt.isOnTransactionService()) {
		return NULL;
	}

	scope = UTIL_MAKE_LOCAL_UNIQUE(
			scope, DataStoreScope,
			&txn,
			CursorAccessorImpl::getDataStore(cxt),
			CursorAccessorImpl::getClusterService(cxt));
	return scope.get();
}

BaseContainer* SQLContainerImpl::CursorScope::getContainer(
		TransactionContext &txn, ExtOpContext &cxt,
		const DataStoreScope *latch, const ContainerLocation &location,
		bool expired, ContainerType type) {
	if (latch == NULL || location.type_ != SQLType::TABLE_CONTAINER ||
			expired) {
		return NULL;
	}

	DataStoreV4 *dataStore = CursorAccessorImpl::getDataStore(cxt);
	const PartitionId partitionId = txn.getPartitionId();
	const ContainerType requiredType =
			(type == UNDEF_CONTAINER ? ANY_CONTAINER : type);

	try {
		static_cast<void>(partitionId);
		util::StackAllocator& alloc = txn.getDefaultAllocator();
		KeyDataStoreValue keyStoreValue =
				dataStore->getKeyDataStore()->get(alloc, location.id_);

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, requiredType);
		DSContainerOutputMes *ret = static_cast<DSContainerOutputMes*>(
				dataStore->exec(&txn, &keyStoreValue, &input));
		return ret->container_;
	}
	catch (UserException &e) {
		if ((e.getErrorCode() == GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED ||
				e.getErrorCode() == GS_ERROR_DS_DS_CONTAINER_EXPIRED) &&
				location.expirable_) {
			return NULL;
		}
		throw;
	}
}

BaseContainer* SQLContainerImpl::CursorScope::checkContainer(
		const TupleColumnList &inColumnList, BaseContainer *container,
		const ContainerLocation &location, SchemaVersionId schemaVersionId) {
	if (container == NULL && (location.type_ != SQLType::TABLE_CONTAINER ||
			location.expirable_)) {
		return NULL;
	}

	CursorAccessorImpl::checkInputInfo(inColumnList, *container);

	GetContainerPropertiesHandler handler;
	if (schemaVersionId == UNDEF_SCHEMAVERSIONID) {
		handler.checkContainerExistence(container);
	}
	else {
		handler.checkContainerSchemaVersion(container, schemaVersionId);
	}

	if (location.partitioningVersionId_ >= 0 &&
			container->getAttribute() == CONTAINER_ATTR_SUB &&
			location.partitioningVersionId_ <
					container->getTablePartitioningVersionId()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION,
				"Table schema cache mismatch partitioning version, actual=" <<
				container->getTablePartitioningVersionId() <<
				", cache=" << location.partitioningVersionId_);
	}

	return container;
}

ResultSet* SQLContainerImpl::CursorScope::prepareResultSet(
		ExtOpContext &cxt, TransactionContext &txn, BaseContainer *container,
		util::LocalUniquePtr<ResultSetGuard> &rsGuard,
		const CursorAccessorImpl &accessor) {
	if (container == NULL) {
		return NULL;
	}

	DataStoreV4 *dataStore = CursorAccessorImpl::getDataStore(cxt);
	assert(dataStore != NULL);

	TransactionService *txnSvc = CursorAccessorImpl::getTransactionService(cxt);
	assert(txnSvc != NULL);

	txnSvc->getResultSetHolderManager().closeAll(
			txn, *CursorAccessorImpl::getResourceSet(cxt)->getPartitionList());

	const int64_t emNow = CursorAccessorImpl::getEventContext(
			cxt)->getHandlerStartMonotonicTime();

	if (accessor.closed_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (accessor.resultSetLost_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	ResultSet *resultSet;
	if (accessor.resultSetId_ == UNDEF_RESULTSETID) {
		const bool noExpire = true;
		resultSet = dataStore->getResultSetManager()->create(
				txn, container->getContainerId(), container->getVersionId(),
				emNow, NULL, noExpire);
		resultSet->setUpdateIdType(ResultSet::UPDATE_ID_NONE);
	}
	else {
		resultSet = dataStore->getResultSetManager()->get(txn, accessor.resultSetId_);
	}

	if (resultSet == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_RESULT_ID_INVALID, "");
	}

	rsGuard = UTIL_MAKE_LOCAL_UNIQUE(
			rsGuard, ResultSetGuard, txn, *dataStore, *resultSet);

	return resultSet;
}

ResultSetId SQLContainerImpl::CursorScope::findResultSetId(
		ResultSet *resultSet) {
	if (resultSet == NULL) {
		return UNDEF_RESULTSETID;
	}
	return resultSet->getId();
}


SQLContainerImpl::MetaScanHandler::MetaScanHandler(
		OpContext &cxt, CursorImpl &cursor, const Projection &proj) :
		cxt_(cxt),
		cursor_(cursor),
		proj_(proj) {
}

void SQLContainerImpl::MetaScanHandler::operator()(
		TransactionContext &txn,
		const MetaProcessor::ValueList &valueList) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	VarContext &varCxt = cxt_.getVarContext();
	VarContext::Scope varScope(varCxt);

	const uint32_t outIndex = cursor_.getOutputIndex();
	TupleListWriter &writer= cxt_.getWriter(outIndex);
	writer.next();

	TupleValue value;
	uint32_t pos = 0;
	for (Expression::Iterator it(proj_); it.exists(); it.next()) {
		const Value &srcValue = valueList[it.get().getCode().getColumnPos()];
		ContainerValueUtils::toTupleValue(alloc, srcValue, value, true);
		CursorAccessorImpl::writeValueAs<TupleTypes::TYPE_ANY>(
				writer, cxt_.getWriterColumn(outIndex, pos), value);
		pos++;
	}
}


util::ObjectPool<
		SQLContainerImpl::TupleUpdateRowIdHandler::Shared, util::Mutex>
		SQLContainerImpl::TupleUpdateRowIdHandler::
		sharedPool_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_WORK, "updateRowIdHandlerPool"));

SQLContainerImpl::TupleUpdateRowIdHandler::TupleUpdateRowIdHandler(
		const OpContext::Source &cxtSrc, RowId maxRowId) :
		shared_(UTIL_OBJECT_POOL_NEW(sharedPool_) Shared(cxtSrc, maxRowId)) {
}

SQLContainerImpl::TupleUpdateRowIdHandler::~TupleUpdateRowIdHandler() {
	try {
		close();
	}
	catch (...) {
		assert(false);
	}
}

void SQLContainerImpl::TupleUpdateRowIdHandler::close() {
	if (shared_ == NULL) {
		return;
	}

	bool noRef;
	{
		util::LockGuard<util::Mutex> guard(shared_->mutex_);

		shared_->cxt_.reset();

		assert(shared_->refCount_ > 0);
		noRef = (--shared_->refCount_ == 0);
	}

	if (noRef) {
		UTIL_OBJECT_POOL_DELETE(sharedPool_, shared_);
	}

	shared_ = NULL;
}

bool SQLContainerImpl::TupleUpdateRowIdHandler::isUpdated() {
	if (shared_ == NULL || shared_->cxt_.get() == NULL) {
		return false;
	}
	return shared_->updated_;
}

void SQLContainerImpl::TupleUpdateRowIdHandler::setUpdated() {
	if (shared_ == NULL || shared_->cxt_.get() == NULL) {
		return;
	}
	shared_->updated_ = true;
}

void SQLContainerImpl::TupleUpdateRowIdHandler::operator()(
		RowId rowId, uint64_t id, UpdateOperation op) {
	if (shared_ == NULL) {
		return;
	}

	util::LockGuard<util::Mutex> guard(shared_->mutex_);
	if (shared_->cxt_.get() == NULL) {
		return;
	}

	shared_->updated_ = true;

	if (rowId > shared_->maxRowId_ &&
			(op == ResultSet::UPDATE_OP_UPDATE_NORMAL_ROW ||
			op == ResultSet::UPDATE_OP_UPDATE_MVCC_ROW)) {
		return;
	}

	OpContext &cxt = *shared_->cxt_;
	const IdStoreColumnList &columnList = shared_->columnList_;
	const uint32_t index = shared_->rowIdStoreIndex_;
	{
		TupleListWriter &writer = cxt.getWriter(index);
		writer.next();

		WritableTuple tuple = writer.get();
		SQLValues::ValueUtils::writeValue<IdValueTag>(
				tuple, columnList[ID_COLUMN_POS],
				static_cast<IdValueTag::ValueType>(id));
		SQLValues::ValueUtils::writeValue<OpValueTag>(
				tuple, columnList[OP_COLUMN_POS],
				static_cast<OpValueTag::ValueType>(op));
	}
	cxt.releaseWriterLatch(index);
}

bool SQLContainerImpl::TupleUpdateRowIdHandler::apply(
		OIdTable &oIdTable, uint64_t limit) {
	if (shared_ == NULL || shared_->cxt_.get() == NULL) {
		return true;
	}

	OpContext &cxt = *shared_->cxt_;
	const IdStoreColumnList &columnList = shared_->columnList_;
	const uint32_t index = shared_->rowIdStoreIndex_;
	const uint32_t extraIndex = shared_->extraStoreIndex_;

	cxt.closeLocalWriter(index);
	bool extra = false;
	{
		uint64_t rest = std::max<uint64_t>(limit, 1);
		TupleListReader &reader = cxt.getLocalReader(index);
		for (; reader.exists(); reader.next(), --rest) {
			if (rest <= 0) {
				extra = true;
				break;
			}
			const uint64_t id = static_cast<uint64_t>(
					SQLValues::ValueUtils::readCurrentValue<IdValueTag>(
							reader, columnList[ID_COLUMN_POS]));
			const UpdateOperation op = static_cast<UpdateOperation>(
					SQLValues::ValueUtils::readCurrentValue<OpValueTag>(
							reader, columnList[OP_COLUMN_POS]));
			oIdTable.update(op, id);
		}
		shared_->updated_ = false;
	}

	if (extra) {
		cxt.createLocalTupleList(extraIndex);
		copyRowIdStore(cxt, index, extraIndex, columnList);
		cxt.closeLocalWriter(extraIndex);
	}

	cxt.closeLocalTupleList(index);
	cxt.createLocalTupleList(index);

	if (extra) {
		copyRowIdStore(cxt, extraIndex, index, columnList);
		cxt.closeLocalTupleList(extraIndex);
	}

	cxt.releaseWriterLatch(index);

	const bool finished = !extra;
	return finished;
}

SQLContainerImpl::TupleUpdateRowIdHandler*
SQLContainerImpl::TupleUpdateRowIdHandler::share(util::StackAllocator &alloc) {
	assert(shared_ != NULL);
	return ALLOC_NEW(alloc) TupleUpdateRowIdHandler(*shared_);
}

SQLContainerImpl::TupleUpdateRowIdHandler::TupleUpdateRowIdHandler(
		Shared &shared) :
		shared_(&shared) {
	shared_->refCount_++;
}

uint32_t SQLContainerImpl::TupleUpdateRowIdHandler::createRowIdStore(
		util::LocalUniquePtr<OpContext> &cxt, const OpContext::Source &cxtSrc,
		IdStoreColumnList &columnList, uint32_t &extraStoreIndex) {
	cxt = UTIL_MAKE_LOCAL_UNIQUE(cxt, OpContext, cxtSrc);

	const size_t columnCount = sizeof(columnList) / sizeof(*columnList);
	const TupleColumnType baseTypeList[columnCount] = {
		IdValueTag::COLUMN_TYPE,
		OpValueTag::COLUMN_TYPE
	};

	util::Vector<TupleColumnType> typeList(
			baseTypeList, baseTypeList + columnCount, cxt->getAllocator());

	const uint32_t index = cxt->createLocal(&typeList);
	extraStoreIndex = cxt->createLocal(&typeList);

	const TupleList::Info &info = cxt->createLocalTupleList(index).getInfo();
	info.getColumns(columnList, columnCount);

	return index;
}

void SQLContainerImpl::TupleUpdateRowIdHandler::copyRowIdStore(
		OpContext &cxt, uint32_t srcIndex, uint32_t destIndex,
		const IdStoreColumnList &columnList) {
	const size_t columnCount = sizeof(columnList) / sizeof(*columnList);

	TupleListReader &reader = cxt.getLocalReader(srcIndex);
	TupleListWriter &writer = cxt.getWriter(destIndex);

	for (; reader.exists(); reader.next()) {
		writer.next();

		WritableTuple tuple = writer.get();
		for (size_t i = 0; i < columnCount; i++) {
			SQLValues::ValueUtils::writeAny(
					tuple, columnList[i],
					SQLValues::ValueUtils::readCurrentAny(
							reader, columnList[i]));
		}
	}
}

SQLContainerImpl::TupleUpdateRowIdHandler::Shared::Shared(
		const OpContext::Source &cxtSrc, RowId maxRowId) :
		refCount_(1),
		rowIdStoreIndex_(createRowIdStore(
				cxt_, cxtSrc, columnList_, extraStoreIndex_)),
		maxRowId_(maxRowId),
		updated_(false) {
}


SQLContainerImpl::OIdTable::OIdTable(const Source &source) :
		allocManager_(*source.allocManager_),
		allocRef_(allocManager_, *source.baseAlloc_),
		alloc_(allocRef_.get()),
		large_(source.large_),
		mode_(source.mode_),
		category_(-1) {
	assert(mode_ != MergeMode::END_MODE);

	for (size_t i = 0; i < SUB_TABLE_COUNT; i++) {
		const bool mapOrdering = (mode_ != MergeMode::MODE_MIXED);

		if (mapOrdering) {
			subTables_[i].treeMap_ = ALLOC_NEW(alloc_) TreeGroupMap(alloc_);
		}
		else {
			subTables_[i].hashMap_ = ALLOC_NEW(alloc_) HashGroupMap(
					0, GroupMapHasher(), GroupMapPred(), alloc_);
		}
	}
}

bool SQLContainerImpl::OIdTable::isEmpty() const {
	for (size_t i = 0; i < SUB_TABLE_COUNT; i++) {
		if (!subTables_[i].isSubTableEmpty()) {
			return false;
		}
	}
	return true;
}

void SQLContainerImpl::OIdTable::addAll(const SearchResult &result) {
	for (size_t i = 0; i < 2; i++) {
		const bool onMvcc = (i != 0);
		addAll(result.getEntry(onMvcc), onMvcc);
	}
}

void SQLContainerImpl::OIdTable::addAll(
		const SearchResultEntry &entry, bool onMvcc) {
	prepareToAdd(entry);

	const SwitcherOption option = toSwitcherOption(onMvcc);
	AddAllOp op(entry);
	(op.*Switcher(option).resolve<AddAllOp>())(option);
}

void SQLContainerImpl::OIdTable::popAll(
		SearchResult &result, uint64_t limit, uint32_t rowArraySize) {
	assert(result.isEmpty());

	SearchResultEntry &entry = result.getEntry(false);

	bool onMvcc;
	popAll(entry, onMvcc, limit, rowArraySize);

	if (onMvcc) {
		entry.swap(result.getEntry(true));
	}
}

void SQLContainerImpl::OIdTable::popAll(
		SearchResultEntry &entry, bool &onMvcc, uint64_t limit,
		uint32_t rowArraySize) {
	assert(entry.isEntryEmpty());
	onMvcc = false;
	for (;;) {
		const SwitcherOption option = toSwitcherOption(onMvcc);
		PopAllOp op(entry, limit, rowArraySize);
		(op.*Switcher(option).resolve<PopAllOp>())(option);

		if (!entry.isEntryEmpty() || onMvcc) {
			break;
		}
		onMvcc = !onMvcc;
	}
}

void SQLContainerImpl::OIdTable::update(
		ResultSet::UpdateOperation op, OId oId) {
	switch (op) {
	case ResultSet::UPDATE_OP_UPDATE_NORMAL_ROW:
		add(oId, false);
		break;
	case ResultSet::UPDATE_OP_UPDATE_MVCC_ROW:
		add(oId, true);
		break;
	case ResultSet::UPDATE_OP_REMOVE_ROW:
		remove(oId, true);
		remove(oId, false);
		break;
	case ResultSet::UPDATE_OP_REMOVE_ROW_ARRAY:
		removeRa(oId, true);
		removeRa(oId, false);
		break;
	case ResultSet::UPDATE_OP_REMOVE_CHUNK:
		removeChunk(oId, true);
		removeChunk(oId, false);
		break;
	}
}

void SQLContainerImpl::OIdTable::readAll(
		SearchResult &result, uint64_t limit, bool large, MergeMode::Type mode,
		uint32_t rowArraySize, Reader &reader) {
	assert(result.isEmpty());

	SwitcherOption option;
	option.onLarge_ = large;
	option.mode_ = mode;

	uint64_t rest = limit;
	for (size_t i = 0; i < 2; i++) {
		const bool onMvcc = (i == 0);
		option.onMvcc_ = onMvcc;
		SearchResultEntry &entry = result.getEntry(onMvcc);

		ReadAllOp op(entry, rest, rowArraySize, reader);
		(op.*Switcher(option).resolve<ReadAllOp>())(option);

		rest -= std::min<uint64_t>(getEntrySize(entry, rowArraySize), rest);
	}
}

void SQLContainerImpl::OIdTable::load(Reader &reader) {
	for (size_t i = 0; i < 2; i++) {
		const bool onMvcc = (i == 0);
		const SwitcherOption option = toSwitcherOption(onMvcc);
		LoadOp op(reader);
		(op.*Switcher(option).resolve<LoadOp>())(option);
	}
}

void SQLContainerImpl::OIdTable::save(Writer &writer) {
	for (size_t i = 0; i < 2; i++) {
		const bool onMvcc = (i == 0);
		const SwitcherOption option = toSwitcherOption(onMvcc);
		SaveOp op(writer);
		(op.*Switcher(option).resolve<SaveOp>())(option);
	}
}

size_t SQLContainerImpl::OIdTable::getTotalAllocationSize() {
	return alloc_.getTotalSize();
}

void SQLContainerImpl::OIdTable::add(OId oId, bool onMvcc) {
	prepareToAdd(oId);
	const bool forRa = isForRaWithMode(false, mode_);

	const SwitcherOption option = toSwitcherOption(onMvcc);
	AddOp op(oId, forRa);
	(op.*Switcher(option).resolve<AddOp>())(option);
}

void SQLContainerImpl::OIdTable::remove(OId oId, bool onMvcc, bool forRa) {
	const SwitcherOption option = toSwitcherOption(onMvcc);
	RemoveOp op(oId, forRa);
	(op.*Switcher(option).resolve<RemoveOp>())(option);
}

void SQLContainerImpl::OIdTable::removeRa(OId oId, bool onMvcc) {
	const bool forRa = true;
	remove(oId, onMvcc, forRa);
}

void SQLContainerImpl::OIdTable::removeChunk(OId oId, bool onMvcc) {
	const SwitcherOption option = toSwitcherOption(onMvcc);
	RemoveChunkOp op(oId);
	(op.*Switcher(option).resolve<RemoveChunkOp>())(option);
}

template<bool Mvcc, bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
void SQLContainerImpl::OIdTable::readAllDetail(
		SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize,
		Reader &reader) {
	uint64_t rest = limit;
	bool forRa;
	Code code;
	while (rest > 0 && readTopElement<Mvcc, Large, Mode>(reader, forRa, code)) {
		OIdList &oIdList = entry.getOIdList(isForRaWithMode<Mode>(forRa));
		const size_t prevSize = oIdList.size();

		const Code lastMasked = CodeUtils::toMaskedCode<Large>(code);
		do {
			oIdList.push_back(CodeUtils::codeToOId<Large>(code));
		}
		while (readNextElement<Large, Mode>(reader, forRa, lastMasked, code));

		rest -= std::min<uint64_t>(getEntrySize(
				forRa, (oIdList.size() - prevSize), rowArraySize), rest);
	}
}

template<bool Mvcc, bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
void SQLContainerImpl::OIdTable::loadDetail(
		typename MapByMode<Mode>::Type &map, Reader &reader) {
	bool forRa;
	Code code;
	while (readTopElement<Mvcc, Large, Mode>(reader, forRa, code)) {
		prepareToAdd(CodeUtils::codeToOId<Large>(code));

		ElementSet &elemSet =
				insertGroup(map, CodeUtils::toGroup<Large>(code), forRa);

		ElementList &list = elemSet.elems_;
		assert(list.empty());

		const Code lastMasked = CodeUtils::toMaskedCode<Large>(code);
		do {
			list.push_back(CodeUtils::toElement<Large>(code));
		}
		while (readNextElement<Large, Mode>(reader, forRa, lastMasked, code));
	}
}

template<bool Mvcc, bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
void SQLContainerImpl::OIdTable::saveDetail(
		typename MapByMode<Mode>::Type &map, Writer &writer) {
	typedef typename MapByMode<Mode>::Type Map;
	typedef typename Map::iterator MapIterator;

	if (map.empty()) {
		return;
	}
	const Element category = resolveCategory();
	for (MapIterator mapIt = map.begin(); mapIt != map.end(); ++mapIt) {
		writeGroupHead<Mvcc, Large, Mode>(
				writer, mapIt->second.isForRa(), mapIt->first);

		ElementList &list = mapIt->second.elems_;
		for (ElementList::iterator it = list.begin(); it != list.end(); ++it) {
			writer.writeCode(
					CodeUtils::toCode<Mvcc, Large>(mapIt->first, *it, category));
		}
	}
}

inline bool SQLContainerImpl::OIdTable::isForRaWithMode(
		bool forRa, MergeMode::Type mode) {
	return (mode == MergeMode::MODE_ROW_ARRAY || forRa);
}

template<SQLContainerUtils::ScanMergeMode::Type Mode>
inline bool SQLContainerImpl::OIdTable::isForRaWithMode(bool forRa) {
	return (Mode == MergeMode::MODE_ROW_ARRAY || forRa);
}

template<SQLContainerUtils::ScanMergeMode::Type Mode>
inline bool SQLContainerImpl::OIdTable::isRaMerging(bool forRa) {
	return (Mode == MergeMode::MODE_MIXED_MERGING && !forRa);
}

template<bool Mvcc, bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
inline bool SQLContainerImpl::OIdTable::readTopElement(
		Reader &reader, bool &forRa, Code &code) {
	if (!reader.exists()) {
		return false;
	}

	bool withHead = false;
	forRa = (Mode == MergeMode::MODE_ROW_ARRAY);

	code = reader.getCode();
	Code lastMasked = CodeUtils::toMaskedCode<Large>(code);
	for (;;) {
		if (Mvcc && !CodeUtils::isOnMvcc(code)) {
			return false;
		}
		assert(!Mvcc == !CodeUtils::isOnMvcc(code));

		if (Mode != MergeMode::MODE_ROW_ARRAY &&
				CodeUtils::isGroupHead<Large>(code)) {
			if (Mode == MergeMode::MODE_MIXED_MERGING) {
				forRa |= CodeUtils::isRaHead<Large>(code);
			}

			reader.next();
			if (!reader.exists()) {
				return false;
			}
			code = reader.getCode();

			const Code masked = CodeUtils::toMaskedCode<Large>(code);
			if (masked == lastMasked) {
				withHead = true;
			}
			else {
				if (Mode == MergeMode::MODE_MIXED_MERGING) {
					forRa = false;
				}
				lastMasked = masked;
				withHead = false;
			}
			continue;
		}
		assert(!CodeUtils::isGroupHead<Large>(code));

		if (Mode != MergeMode::MODE_ROW_ARRAY && !withHead) {
			forRa = true;
		}
		if (Mode == MergeMode::MODE_MIXED_MERGING && forRa) {
			code = CodeUtils::toRaCode(code);
		}
		return true;
	}
}

template<bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
inline bool SQLContainerImpl::OIdTable::readNextElement(
		Reader &reader, bool forRa, Code lastMasked, Code &code) {
	Code prevCode = code;
	for (;;) {
		reader.next();
		if (!reader.exists()) {
			return false;
		}

		code = reader.getCode();
		if (CodeUtils::toMaskedCode<Large>(code) != lastMasked) {
			return false;
		}
		assert(!CodeUtils::isGroupHead<Large>(code));

		if (Mode == MergeMode::MODE_MIXED_MERGING && forRa) {
			code = CodeUtils::toRaCode(code);

			if (code == prevCode) {
				prevCode = code;
				continue;
			}
		}

		return true;
	}
}

template<bool Mvcc, bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
inline void SQLContainerImpl::OIdTable::writeGroupHead(
		Writer &writer, bool forRa, Group group) {
	if (Mode == MergeMode::MODE_ROW_ARRAY ||
			(Mode != MergeMode::MODE_MIXED_MERGING && forRa)) {
		return;
	}

	Code code;
	if (Mode == MergeMode::MODE_MIXED_MERGING && forRa) {
		code = CodeUtils::toGroupHead<Mvcc, Large, true>(group);
	}
	else {
		assert(!forRa);
		code = CodeUtils::toGroupHead<Mvcc, Large, false>(group);
	}

	writer.writeCode(code);
}

template<bool Large, bool Ordering>
void SQLContainerImpl::OIdTable::addAllDetail(
		typename MapByOrdering<Ordering>::Type &map,
		const SearchResultEntry &entry) {
	{
		const bool forRa = false;
		const OIdList &list = entry.getOIdList(forRa);
		addAllDetail<Large, Ordering>(map, list, isForRaWithMode(forRa, mode_));
	}
	{
		const bool forRa = true;
		const OIdList &list = entry.getOIdList(forRa);
		addAllDetail<Large, Ordering>(map, list, forRa);
	}
}

template<bool Large, bool Ordering>
void SQLContainerImpl::OIdTable::addAllDetail(
		typename MapByOrdering<Ordering>::Type &map, const OIdList &list,
		bool forRa) {
	if (forRa) {
		for (OIdList::const_iterator it = list.begin(); it != list.end(); ++it) {
			addRaDetail<Large, Ordering>(map, *it);
		}
	}
	else {
		for (OIdList::const_iterator it = list.begin(); it != list.end(); ++it) {
			addDetail<Large, Ordering>(map, *it);
		}
	}
}

template<bool Large, bool Ordering>
inline void SQLContainerImpl::OIdTable::addDetail(
		typename MapByOrdering<Ordering>::Type &map, OId oId, bool forRa) {
	if (forRa) {
		addRaDetail<Large, Ordering>(map, oId);
	}
	else {
		addDetail<Large, Ordering>(map, oId);
	}
}

template<bool Large, bool Ordering>
inline void SQLContainerImpl::OIdTable::addDetail(
		typename MapByOrdering<Ordering>::Type &map, OId oId) {
	ElementSet &elemSet =
			insertGroup(map, CodeUtils::oIdToGroup<Large>(oId), false);

	if (elemSet.isForRa()) {
		insertElement(elemSet, CodeUtils::oIdToRaElement<Large>(oId));
	}
	else if (insertElement(elemSet, CodeUtils::oIdToElement<Large>(oId)) &&
			!elemSet.acceptRows()) {
		elementSetToRa(elemSet);
	}
}

template<bool Large, bool Ordering>
inline void SQLContainerImpl::OIdTable::addRaDetail(
		typename MapByOrdering<Ordering>::Type &map, OId oId) {
	ElementSet &elemSet =
			insertGroup(map, CodeUtils::oIdToGroup<Large>(oId), true);

	if (!elemSet.isForRa()) {
		elementSetToRa(elemSet);
	}

	insertElement(elemSet, CodeUtils::oIdToRaElement<Large>(oId));
}

template<bool Large, bool Ordering>
void SQLContainerImpl::OIdTable::popAllDetail(
		typename MapByOrdering<Ordering>::Type &map, SearchResultEntry &entry,
		uint64_t limit, uint32_t rowArraySize) {
	typedef typename MapByOrdering<Ordering>::Type Map;
	typedef typename Map::iterator MapIterator;

	if (map.empty()) {
		return;
	}
	const Element category = resolveCategory();
	uint64_t rest = limit;
	for (MapIterator mapIt = map.begin(); mapIt != map.end();) {
		if (rest <= 0) {
			break;
		}

		const bool forRa = mapIt->second.isForRa();
		OIdList &oIdList = entry.getOIdList(forRa);
		const size_t prevSize = oIdList.size();

		ElementList &list = mapIt->second.elems_;
		for (ElementList::iterator it = list.begin(); it != list.end(); ++it) {
			oIdList.push_back(
					CodeUtils::toOId<Large>(mapIt->first, *it, category));
		}

		MapIterator lastIt = mapIt;
		++mapIt;
		map.erase(lastIt);

		rest -= std::min<uint64_t>(getEntrySize(
				forRa, (oIdList.size() - prevSize), rowArraySize), rest);
	}
}

template<bool Large, bool Ordering>
void SQLContainerImpl::OIdTable::removeDetail(
		typename MapByOrdering<Ordering>::Type &map, OId oId, bool forRa) {
	typedef typename MapByOrdering<Ordering>::Type Map;
	typedef typename Map::iterator MapIterator;

	MapIterator mapIt = map.find(CodeUtils::oIdToGroup<Large>(oId));
	if (mapIt == map.end()) {
		return;
	}
	ElementSet &elemSet = mapIt->second;

	if (mapIt->second.isForRa()) {
		if (!forRa) {
			return;
		}
	}
	else {
		if (forRa) {
			elementSetToRa(elemSet);
		}
	}

	const Element elem = (mapIt->second.isForRa() ?
			CodeUtils::oIdToRaElement<Large>(oId) :
			CodeUtils::oIdToElement<Large>(oId));

	if (eraseElement(elemSet, elem) && elemSet.elems_.empty()) {
		map.erase(mapIt);
	}
}

template<bool Large, bool Ordering>
void SQLContainerImpl::OIdTable::removeChunkDetail(
		typename MapByOrdering<Ordering>::Type &map, OId oId) {
	typedef typename MapByOrdering<Ordering>::Type Map;
	typedef typename Map::iterator MapIterator;

	MapIterator mapIt = map.find(CodeUtils::oIdToGroup<Large>(oId));
	if (mapIt == map.end()) {
		return;
	}

	map.erase(mapIt);
}

void SQLContainerImpl::OIdTable::elementSetToRa(ElementSet &elemSet) {
	ElementList &list = elemSet.elems_;

	ElementList::iterator it = list.begin();
	if (it != list.end()) {
		*it = CodeUtils::toRaElement(*it);
		ElementList::iterator next = it;
		while ((++next) != list.end()) {
			const Element elem = CodeUtils::toRaElement(*next);
			if (elem != *it) {
				*(++it) = elem;
			}
		}
		list.erase((++it), list.end());
	}

	elemSet.setForRa();
}

template<typename Map>
inline SQLContainerImpl::OIdTable::ElementSet&
SQLContainerImpl::OIdTable::insertGroup(Map &map, Group group, bool forRa) {
	typename Map::iterator mapIt = map.find(group);
	if (mapIt == map.end()) {
		ElementSet &elemSet = map.insert(std::make_pair(
				group, ElementSet(alloc_, forRa))).first->second;
		elemSet.elems_.reserve(ELEMENT_SET_ROWS_CAPACITY);
		return elemSet;
	}
	return mapIt->second;
}

inline bool SQLContainerImpl::OIdTable::insertElement(
		ElementSet &elemSet, Element elem) {
	ElementList &list = elemSet.elems_;
	ElementList::iterator it = std::lower_bound(list.begin(), list.end(), elem);

	if (it == list.end() || *it != elem) {
		list.insert(it, elem);
		return true;
	}

	return false;
}

inline bool SQLContainerImpl::OIdTable::eraseElement(
		ElementSet &elemSet, Element elem) {
	ElementList &list = elemSet.elems_;
	ElementList::iterator it = std::lower_bound(list.begin(), list.end(), elem);

	if (it != list.end() && *it == elem) {
		list.erase(it);
		return true;
	}

	return false;
}

void SQLContainerImpl::OIdTable::prepareToAdd(const SearchResultEntry &entry) {
	if (category_ >= 0) {
		return;
	}

	for (size_t i = 0; i < 2; i++) {
		const bool forRa = (i != 0);
		const OIdList &oIdList = entry.getOIdList(forRa);
		if (!oIdList.empty()) {
			prepareToAdd(oIdList.front());
			break;
		}
	}
}

void SQLContainerImpl::OIdTable::prepareToAdd(OId oId) {
	if (category_ >= 0) {
		return;
	}
	category_ = static_cast<int64_t>(CodeUtils::oIdToCategory(oId));
}

SQLContainerImpl::OIdTable::TreeGroupMap&
SQLContainerImpl::OIdTable::getGroupMap(bool onMvcc, const util::TrueType&) {
	TreeGroupMap *map = subTables_[(onMvcc ? 1 : 0)].treeMap_;
	assert(map != NULL);
	return *map;
}

SQLContainerImpl::OIdTable::HashGroupMap&
SQLContainerImpl::OIdTable::getGroupMap(bool onMvcc, const util::FalseType&) {
	HashGroupMap *map = subTables_[(onMvcc ? 1 : 0)].hashMap_;
	assert(map != NULL);
	return *map;
}

SQLContainerImpl::OIdTable::Element SQLContainerImpl::OIdTable::resolveCategory() {
	if (category_ < 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return static_cast<Element>(category_);
}

SQLContainerImpl::OIdTable::SwitcherOption
SQLContainerImpl::OIdTable::toSwitcherOption(bool onMvcc) {
	SwitcherOption option;
	option.onMvcc_ = onMvcc;
	option.onLarge_ = large_;
	option.mode_ = mode_;
	option.table_ = this;
	return option;
}

uint64_t SQLContainerImpl::OIdTable::getEntrySize(
		const SearchResultEntry &entry, uint32_t rowArraySize) {
	uint64_t size = 0;
	for (size_t i = 0; i < 2; i++) {
		const bool forRa = (i != 0);
		size += getEntrySize(forRa, entry.getOIdList(forRa).size(), rowArraySize);
	}
	return size;
}

uint64_t SQLContainerImpl::OIdTable::getEntrySize(
		bool forRa, size_t elemCount, uint32_t rowArraySize) {
	return static_cast<uint64_t>(forRa ? rowArraySize : 1) * elemCount;
}


SQLContainerImpl::OIdTable::Source::Source() :
		allocManager_(NULL),
		baseAlloc_(NULL),
		large_(false),
		mode_(MergeMode::END_MODE) {
}

SQLContainerImpl::OIdTable::Source
SQLContainerImpl::OIdTable::Source::ofContext(
		OpContext &cxt, MergeMode::Type mode) {
	Source source;
	source.allocManager_ = &cxt.getAllocatorManager();
	source.baseAlloc_ = &cxt.getAllocator().base();
	source.large_ =
			detectLarge(cxt.getExtContext().getResourceSet()->getDataStore());
	source.mode_ = mode;
	return source;
}

bool SQLContainerImpl::OIdTable::Source::detectLarge(DataStoreV4 *dataStore) {
	if (dataStore == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	ObjectManagerV4 *objectManager = dataStore->getObjectManager();
	if (objectManager == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const uint32_t chunkSize = objectManager->getChunkSize();
	const uint32_t chunkExpSize = util::nextPowerBitsOf2(chunkSize);
	if (chunkSize != (static_cast<uint32_t>(1) << chunkExpSize)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return OIdBitUtils::isLargeChunkMode(chunkExpSize);
}


SQLContainerImpl::OIdTable::Reader::Reader(
		BaseReader &base, const TupleColumnList &columnList) :
		base_(base),
		column_(columnList.front()) {
}

inline bool SQLContainerImpl::OIdTable::Reader::exists() {
	return base_.exists();
}

inline void SQLContainerImpl::OIdTable::Reader::next() {
	base_.next();
}

inline SQLContainerImpl::OIdTable::Code
SQLContainerImpl::OIdTable::Reader::getCode() {
	typedef SQLValues::Types::Long TypeTag;
	return static_cast<Code>(
			SQLValues::ValueUtils::readCurrentValue<TypeTag>(base_, column_));
}


SQLContainerImpl::OIdTable::Writer::Writer(
		BaseWriter &base, const TupleColumnList &columnList) :
		base_(base),
		column_(columnList.front()) {
}

inline void SQLContainerImpl::OIdTable::Writer::writeCode(Code code) {
	typedef SQLValues::Types::Long TypeTag;

	base_.next();

	WritableTuple tuple = base_.get();
	SQLValues::ValueUtils::writeValue<TypeTag>(
			tuple, column_, static_cast<TypeTag::ValueType>(code));
}


SQLContainerImpl::OIdTable::SwitcherOption::SwitcherOption() :
		onMvcc_(false),
		onLarge_(false),
		mode_(MergeMode::END_MODE),
		table_(NULL) {
}

SQLContainerImpl::OIdTable&
SQLContainerImpl::OIdTable::SwitcherOption::getTable() const {
	assert(table_ != NULL);
	return *table_;
}


SQLContainerImpl::OIdTable::Switcher::Switcher(const SwitcherOption &option) :
		option_(option) {
}

template<typename Op>
typename SQLContainerImpl::OIdTable::Switcher::template OpTraits<Op>::FuncType
SQLContainerImpl::OIdTable::Switcher::resolve() const {
	typedef typename Op::SwitcherTraitsType::MvccFilter FilterType;
	if (option_.onMvcc_ && FilterType::OPTION_SPECIFIC) {
		return resolveDetail<Op, FilterType::template At<true>::VALUE>();
	}
	else {
		return resolveDetail<Op, FilterType::template At<false>::VALUE>();
	}
}

template<typename Op, bool Mvcc>
typename SQLContainerImpl::OIdTable::Switcher::template OpTraits<Op>::FuncType
SQLContainerImpl::OIdTable::Switcher::resolveDetail() const {
	if (option_.onLarge_) {
		return resolveDetail<Op, Mvcc, true>();
	}
	else {
		return resolveDetail<Op, Mvcc, false>();
	}
}

template<typename Op, bool Mvcc, bool Large>
typename SQLContainerImpl::OIdTable::Switcher::template OpTraits<Op>::FuncType
SQLContainerImpl::OIdTable::Switcher::resolveDetail() const {
	typedef typename Op::SwitcherTraitsType::ModeFilter FilterType;
	switch (option_.mode_) {
	case MergeMode::MODE_MIXED:
		return resolveDetail<Op, Mvcc, Large, FilterType::template At<
				MergeMode::MODE_MIXED>::VALUE>();
	case MergeMode::MODE_MIXED_MERGING:
		return resolveDetail<Op, Mvcc, Large, FilterType::template At<
				MergeMode::MODE_MIXED_MERGING>::VALUE>();
	default:
		assert(option_.mode_ == MergeMode::MODE_ROW_ARRAY);
		return resolveDetail<Op, Mvcc, Large, FilterType::template At<
				MergeMode::MODE_ROW_ARRAY>::VALUE>();
	}
}

template<
		typename Op, bool Mvcc, bool Large,
		SQLContainerUtils::ScanMergeMode::Type Mode>
typename SQLContainerImpl::OIdTable::Switcher::template OpTraits<Op>::FuncType
SQLContainerImpl::OIdTable::Switcher::resolveDetail() const {
	typedef OpFuncTraits<Mvcc, Large, Mode> FuncTraitsType;
	return &Op::template execute<FuncTraitsType>;
}


SQLContainerImpl::OIdTable::ReadAllOp::ReadAllOp(
		SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize,
		Reader &reader) :
		entry_(entry),
		limit_(limit),
		rowArraySize_(rowArraySize),
		reader_(reader) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::ReadAllOp::execute(
		const SwitcherOption &option) const {
	static_cast<void>(option);
	readAllDetail<Traits::ON_MVCC, Traits::ON_LARGE, Traits::MERGE_MODE>(
			entry_, limit_, rowArraySize_, reader_);
}


SQLContainerImpl::OIdTable::LoadOp::LoadOp(Reader &reader) :
		reader_(reader) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::LoadOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.loadDetail<
			Traits::ON_MVCC, Traits::ON_LARGE, Traits::MERGE_MODE>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), reader_);
}


SQLContainerImpl::OIdTable::SaveOp::SaveOp(Writer &writer) :
		writer_(writer) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::SaveOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.saveDetail<
			Traits::ON_MVCC, Traits::ON_LARGE, Traits::MERGE_MODE>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), writer_);
}


SQLContainerImpl::OIdTable::AddAllOp::AddAllOp(const SearchResultEntry &entry) :
		entry_(entry) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::AddAllOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.addAllDetail<Traits::ON_LARGE, Traits::MAP_ORDERING>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), entry_);
}


SQLContainerImpl::OIdTable::PopAllOp::PopAllOp(
		SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize) :
		entry_(entry) {
	static_cast<void>(limit);
	static_cast<void>(rowArraySize);
}

template<typename Traits>
void SQLContainerImpl::OIdTable::PopAllOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.popAllDetail<Traits::ON_LARGE, Traits::MAP_ORDERING>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), entry_,
			limit_, rowArraySize_);
}


SQLContainerImpl::OIdTable::AddOp::AddOp(OId oId, bool forRa) :
		oId_(oId),
		forRa_(forRa) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::AddOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.addDetail<Traits::ON_LARGE, Traits::MAP_ORDERING>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), oId_, forRa_);
}


SQLContainerImpl::OIdTable::RemoveOp::RemoveOp(OId oId, bool forRa) :
		oId_(oId),
		forRa_(forRa) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::RemoveOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.removeDetail<Traits::ON_LARGE, Traits::MAP_ORDERING>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), oId_, forRa_);
}


SQLContainerImpl::OIdTable::RemoveChunkOp::RemoveChunkOp(OId oId) :
		oId_(oId) {
}

template<typename Traits>
void SQLContainerImpl::OIdTable::RemoveChunkOp::execute(
		const SwitcherOption &option) const {
	typedef typename Traits::MapOrderingType MapOrderingType;
	OIdTable &table = option.getTable();
	table.removeChunkDetail<Traits::ON_LARGE, Traits::MAP_ORDERING>(
			table.getGroupMap(option.onMvcc_, MapOrderingType()), oId_);
}


template<bool Large>
inline OId SQLContainerImpl::OIdTable::CodeUtils::toOId(
		Group group, Element elem, Element category) {
	typedef OIdConstants<Large> SubConstants;
	return
			(static_cast<Code>(group) << SubConstants::GROUP_TO_OID_CHUNK_BIT) |
			(static_cast<Code>(elem &
					OIdConstantsBase::ELEMENT_NON_USER_MASK) <<
					OIdConstantsBase::ELEMENT_TO_OID_OFFSET_BIT) |
			OIdConstantsBase::MAGIC_NUMBER | category |
			(elem & OIdConstantsBase::USER_MASK);
}

template<bool Large>
inline OId SQLContainerImpl::OIdTable::CodeUtils::codeToOId(Code code) {
	typedef OIdConstants<Large> SubConstants;
	return
			((code & SubConstants::CODE_GROUP_MASK) << CODE_TO_OID_CHUNK_BIT) |
			((code &
					SubConstants::CODE_OFFSET_MASK) <<
					OIdConstantsBase::CODE_TO_OID_OFFSET_BIT) |
			OIdConstantsBase::MAGIC_NUMBER |
			(code & OIdConstantsBase::CATEGORY_USER_MASK);
}

template<bool Large>
inline SQLContainerImpl::OIdTable::Group
SQLContainerImpl::OIdTable::CodeUtils::oIdToGroup(OId oId) {
	typedef OIdConstants<Large> SubConstants;
	return static_cast<Group>(oId >> SubConstants::GROUP_TO_OID_CHUNK_BIT);
}

template<bool Large>
inline SQLContainerImpl::OIdTable::Element
SQLContainerImpl::OIdTable::CodeUtils::oIdToElement(OId oId) {
	typedef OIdConstants<Large> SubConstants;
	return static_cast<Element>(
			((oId &
					SubConstants::OID_OFFSET_MASK) >>
					OIdConstantsBase::ELEMENT_TO_OID_OFFSET_BIT) |
			(oId & OIdConstantsBase::USER_MASK));
}

template<bool Large>
inline SQLContainerImpl::OIdTable::Element
SQLContainerImpl::OIdTable::CodeUtils::oIdToRaElement(OId oId) {
	typedef OIdConstants<Large> SubConstants;
	return static_cast<Element>(
			((oId &
					SubConstants::OID_OFFSET_MASK) >>
					OIdConstantsBase::ELEMENT_TO_OID_OFFSET_BIT));
}

inline SQLContainerImpl::OIdTable::Element
SQLContainerImpl::OIdTable::CodeUtils::oIdToCategory(OId oId) {
	return static_cast<Element>(oId & OIdConstantsBase::CATEGORY_MASK);
}

template<bool Mvcc, bool Large>
inline SQLContainerImpl::OIdTable::Code
SQLContainerImpl::OIdTable::CodeUtils::toCode(
		Group group, Element elem, Element category) {
	typedef OIdConstants<Large> SubConstants;
	return 
			(Mvcc ? CODE_MVCC_FLAG : 0) |
			(static_cast<Code>(group) << SubConstants::GROUP_TO_CODE_BIT) |
			SubConstants::CODE_BODY_FLAG |
			(static_cast<Code>(elem &
					OIdConstantsBase::ELEMENT_NON_USER_MASK) <<
					OIdConstantsBase::ELEMENT_TO_CODE_OFFSET_BIT) |
			category |
			(elem & OIdConstantsBase::USER_MASK);
}

template<bool Large> 
inline SQLContainerImpl::OIdTable::Code
SQLContainerImpl::OIdTable::CodeUtils::toMaskedCode(Code code) {
	typedef OIdConstants<Large> SubConstants;
	return (code & SubConstants::CODE_MVCC_GROUP_MASK);
}

inline SQLContainerImpl::OIdTable::Code
SQLContainerImpl::OIdTable::CodeUtils::toRaCode(Code code) {
	return (code & OIdConstantsBase::CODE_NON_USER_MASK);
}

template<bool Mvcc, bool Large, bool ForRa>
inline SQLContainerImpl::OIdTable::Code
SQLContainerImpl::OIdTable::CodeUtils::toGroupHead(Group group) {
	typedef OIdConstants<Large> SubConstants;
	return
			(Mvcc ? CODE_MVCC_FLAG : 0) |
			(static_cast<Code>(group) << SubConstants::GROUP_TO_CODE_BIT) |
			(ForRa ? SubConstants::CODE_RA_FLAG : 0);
}

template<bool Large>
inline bool SQLContainerImpl::OIdTable::CodeUtils::isGroupHead(Code code) {
	typedef OIdConstants<Large> SubConstants;
	return ((code & SubConstants::CODE_BODY_FLAG) == 0);
}

template<bool Large>
inline bool SQLContainerImpl::OIdTable::CodeUtils::isRaHead(Code code) {
	typedef OIdConstants<Large> SubConstants;
	return ((code & SubConstants::CODE_RA_FLAG) != 0);
}

inline bool SQLContainerImpl::OIdTable::CodeUtils::isOnMvcc(Code code) {
	UTIL_STATIC_ASSERT(sizeof(Code) == sizeof(SignedCode));
	UTIL_STATIC_ASSERT(std::numeric_limits<SignedCode>::is_signed);
	return static_cast<SignedCode>(code) < 0;
}

template<bool Large>
inline SQLContainerImpl::OIdTable::Group
SQLContainerImpl::OIdTable::CodeUtils::toGroup(Code code) {
	typedef OIdConstants<Large> SubConstants;
	return static_cast<Group>(
			((code &
					SubConstants::CODE_GROUP_MASK) >>
					SubConstants::GROUP_TO_CODE_BIT));
}

template<bool Large>
inline SQLContainerImpl::OIdTable::Element
SQLContainerImpl::OIdTable::CodeUtils::toElement(Code code) {
	typedef OIdConstants<Large> SubConstants;
	return static_cast<Element>(
			((code &
					SubConstants::CODE_OFFSET_MASK) >>
					OIdConstantsBase::ELEMENT_TO_CODE_OFFSET_BIT) |
			(code & OIdConstantsBase::USER_MASK));
}

inline SQLContainerImpl::OIdTable::Element
SQLContainerImpl::OIdTable::CodeUtils::toRaElement(Element elem) {
	return (elem & OIdConstantsBase::ELEMENT_NON_USER_MASK);
}




inline SQLContainerImpl::OIdTable::ElementSet::ElementSet(
		util::StackAllocator &alloc, bool forRa) :
		rowsRest_((forRa ? 0 : ELEMENT_SET_ROWS_CAPACITY))
		,
		elems_(alloc)
{
}


inline bool SQLContainerImpl::OIdTable::ElementSet::isForRa() const {
	return (rowsRest_ <= 0);
}

inline void SQLContainerImpl::OIdTable::ElementSet::setForRa() {
	rowsRest_ = 0;
}

inline bool SQLContainerImpl::OIdTable::ElementSet::acceptRows() {
	assert(!isForRa());
	return (--rowsRest_ > 0);
}


SQLContainerImpl::OIdTable::SubTable::SubTable() :
		treeMap_(NULL),
		hashMap_(NULL) {
}

bool SQLContainerImpl::OIdTable::SubTable::isSubTableEmpty() const {
	if (treeMap_ != NULL && !treeMap_->empty()) {
		return false;
	}
	if (hashMap_ != NULL && !hashMap_->empty()) {
		return false;
	}
	return true;
}


SQLContainerImpl::RowIdFilter::RowIdFilter(
		SQLOps::OpAllocatorManager &allocManager,
		util::StackAllocator::BaseAllocator &baseAlloc, RowId maxRowId) :
		allocManager_(allocManager),
		allocRef_(allocManager_, baseAlloc),
		alloc_(allocRef_.get()),
		maxRowId_(maxRowId),
		rowIdBits_(alloc_),
		readOnly_(false) {
}

inline bool SQLContainerImpl::RowIdFilter::accept(RowId rowId) {
	if (rowId > maxRowId_) {
		return false;
	}

	if (readOnly_) {
		return (rowIdBits_.find(rowId) == rowIdBits_.end());
	}
	else {
		return rowIdBits_.insert(rowId).second;
	}
}

void SQLContainerImpl::RowIdFilter::load(
		OpContext::Source &cxtSrc, uint32_t input) {
	OpContext cxt(cxtSrc);
	load(cxt.getLocalReader(input));
}

uint32_t SQLContainerImpl::RowIdFilter::save(OpContext::Source &cxtSrc) {
	OpContext cxt(cxtSrc);

	const uint32_t columnCount = BITS_STORE_COLUMN_COUNT;
	const util::Vector<TupleColumnType> typeList(
			Constants::COLUMN_TYPES,
			Constants::COLUMN_TYPES + columnCount, cxt.getAllocator());

	const uint32_t output = cxt.createLocal(&typeList);
	cxt.createLocalTupleList(output);
	save(cxt.getWriter(output));
	cxt.closeLocalWriter(output);
	cxt.releaseWriterLatch(output);
	return output;
}

void SQLContainerImpl::RowIdFilter::load(TupleListReader &reader) {
	BitsStoreColumnList columnList;
	getBitsStoreColumnList(columnList);

	BitSet::OutputCursor cursor(rowIdBits_);
	for (; reader.exists(); reader.next()) {
		cursor.next(BitSet::CodedEntry(
				SQLValues::ValueUtils::readCurrentValue<BitsKeyTag>(
						reader, columnList[BITS_KEY_COLUMN_POS]),
				SQLValues::ValueUtils::readCurrentValue<BitsValueTag>(
						reader, columnList[BITS_VALUE_COLUMN_POS])));
	}
}

void SQLContainerImpl::RowIdFilter::save(TupleListWriter &writer) {
	BitsStoreColumnList columnList;
	getBitsStoreColumnList(columnList);

	BitSet::InputCursor cursor(rowIdBits_);
	for (BitSet::CodedEntry entry; cursor.next(entry);) {
		writer.next();

		WritableTuple tuple = writer.get();
		SQLValues::ValueUtils::writeValue<BitsKeyTag>(
				tuple, columnList[BITS_KEY_COLUMN_POS], entry.first);
		SQLValues::ValueUtils::writeValue<BitsValueTag>(
				tuple, columnList[BITS_VALUE_COLUMN_POS], entry.second);
	}
}

void SQLContainerImpl::RowIdFilter::setReadOnly() {
	readOnly_ = true;
}

void SQLContainerImpl::RowIdFilter::getBitsStoreColumnList(
		BitsStoreColumnList &columnList) {
	const uint32_t count = BITS_STORE_COLUMN_COUNT;
	UTIL_STATIC_ASSERT(sizeof(columnList) / sizeof(*columnList) == count);

	TupleList::Info info;
	info.columnCount_ = count;
	info.columnTypeList_ = Constants::COLUMN_TYPES;
	info.getColumns(columnList, info.columnCount_);
}

const TupleColumnType SQLContainerImpl::RowIdFilter::Constants::COLUMN_TYPES[
		BITS_STORE_COLUMN_COUNT] = {
	BitsKeyTag::COLUMN_TYPE,
	BitsValueTag::COLUMN_TYPE
};


void SQLContainerImpl::ContainerValueUtils::toContainerValue(
		util::StackAllocator &alloc, const TupleValue &src, Value &dest) {
	switch (src.getType()) {
	case TupleTypes::TYPE_BOOL:
		dest.set(!!ValueUtils::getValue<int8_t>(src));
		break;
	case TupleTypes::TYPE_BYTE:
		dest.set(ValueUtils::getValue<int8_t>(src));
		break;
	case TupleTypes::TYPE_SHORT:
		dest.set(ValueUtils::getValue<int16_t>(src));
		break;
	case TupleTypes::TYPE_INTEGER:
		dest.set(ValueUtils::getValue<int32_t>(src));
		break;
	case TupleTypes::TYPE_LONG:
		dest.set(ValueUtils::getValue<int64_t>(src));
		break;
	case TupleTypes::TYPE_FLOAT:
		dest.set(ValueUtils::getValue<float>(src));
		break;
	case TupleTypes::TYPE_DOUBLE:
		dest.set(ValueUtils::getValue<double>(src));
		break;
	case TupleTypes::TYPE_TIMESTAMP:
		dest.setTimestamp(ValueUtils::getValue<int64_t>(src));
		break;
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		dest.setMicroTimestamp(ValueUtils::getValue<MicroTimestamp>(src));
		break;
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		dest.setNanoTimestamp(
				ValueUtils::getValue<TupleNanoTimestamp>(src).get());
		break;
	case TupleTypes::TYPE_STRING:
		{
			const TupleString::BufferInfo &str = TupleString(src).getBuffer();
			char8_t *addr = const_cast<char8_t*>(str.first);
			const uint32_t size = static_cast<uint32_t>(str.second);
			dest.set(alloc, addr, size);
		}
		break;
	case TupleTypes::TYPE_BLOB:
		{
			util::XArray<char8_t> buf(alloc);
			{
				TupleValue::LobReader reader(src);
				const void *data;
				size_t size;
				while (reader.next(data, size)) {
					const size_t offset = buf.size();
					buf.resize(offset + size);
					memcpy(&buf[offset], data, size);
				}
			}
			{
				const uint32_t size = static_cast<uint32_t>(buf.size());
				dest.set(alloc, buf.data(), size);
			}
		}
		break;
	case TupleTypes::TYPE_NULL:
		dest.setNull();
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
				"Unexpected parameter value type");
	}
}

bool SQLContainerImpl::ContainerValueUtils::toTupleValue(
		util::StackAllocator &alloc, const Value &src, TupleValue &dest,
		bool failOnUnsupported) {
	switch (src.getType()) {
	case COLUMN_TYPE_STRING:
		dest = SyntaxTree::makeStringValue(
				alloc, reinterpret_cast<const char8_t*>(src.data()), src.size());
		break;
	case COLUMN_TYPE_BOOL:
		dest = TupleValue(src.getBool());
		break;
	case COLUMN_TYPE_BYTE:
		dest = TupleValue(src.getByte());
		break;
	case COLUMN_TYPE_SHORT:
		dest = TupleValue(src.getShort());
		break;
	case COLUMN_TYPE_INT:
		dest = TupleValue(src.getInt());
		break;
	case COLUMN_TYPE_LONG:
		dest = TupleValue(src.getLong());
		break;
	case COLUMN_TYPE_FLOAT:
		dest = TupleValue(src.getFloat());
		break;
	case COLUMN_TYPE_DOUBLE:
		dest = TupleValue(src.getDouble());
		break;
	case COLUMN_TYPE_TIMESTAMP:
		dest = ValueUtils::toAnyByTimestamp(src.getTimestamp());
		break;
	case COLUMN_TYPE_MICRO_TIMESTAMP:
		dest = TupleValue(src.getMicroTimestamp());
		break;
	case COLUMN_TYPE_NANO_TIMESTAMP:
		dest = SyntaxTree::makeNanoTimestampValue(alloc, src.getNanoTimestamp());
		break;
	case COLUMN_TYPE_BLOB: {
		TupleValue::VarContext varCxt;
		varCxt.setStackAllocator(&alloc);
		dest = SyntaxTree::makeBlobValue(varCxt, src.data(), src.size());
		break;
	}
	default:
		dest = TupleValue();
		if (COLUMN_TYPE_NULL){
			break;
		}
		if (!failOnUnsupported) {
			return false;
		}
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
				"Unsupported value type");
	}
	return true;
}

TupleColumnType SQLContainerImpl::ContainerValueUtils::toTupleColumnType(
		ContainerColumnType src, bool nullable) {
	return SQLValues::TypeUtils::setNullable(
			SQLValues::TypeUtils::filterColumnType(toRawTupleColumnType(src)),
			nullable);
}

TupleColumnType SQLContainerImpl::ContainerValueUtils::toRawTupleColumnType(
		ContainerColumnType src) {
	TupleColumnType dest;

	switch (ValueProcessor::getArrayElementType(src)) {
	case COLUMN_TYPE_STRING:
		dest = TupleTypes::TYPE_STRING;
		break;
	case COLUMN_TYPE_BOOL:
		dest = TupleTypes::TYPE_BOOL;
		break;
	case COLUMN_TYPE_BYTE:
		dest = TupleTypes::TYPE_BYTE;
		break;
	case COLUMN_TYPE_SHORT:
		dest = TupleTypes::TYPE_SHORT;
		break;
	case COLUMN_TYPE_INT:
		dest = TupleTypes::TYPE_INTEGER;
		break;
	case COLUMN_TYPE_LONG:
		dest = TupleTypes::TYPE_LONG;
		break;
	case COLUMN_TYPE_FLOAT:
		dest = TupleTypes::TYPE_FLOAT;
		break;
	case COLUMN_TYPE_DOUBLE:
		dest = TupleTypes::TYPE_DOUBLE;
		break;
	case COLUMN_TYPE_TIMESTAMP:
		dest = TupleTypes::TYPE_TIMESTAMP;
		break;
	case COLUMN_TYPE_MICRO_TIMESTAMP:
		dest = TupleTypes::TYPE_MICRO_TIMESTAMP;
		break;
	case COLUMN_TYPE_NANO_TIMESTAMP:
		dest = TupleTypes::TYPE_NANO_TIMESTAMP;
		break;
	case COLUMN_TYPE_GEOMETRY:
		dest = TupleTypes::TYPE_GEOMETRY;
		break;
	case COLUMN_TYPE_BLOB:
		dest = TupleTypes::TYPE_BLOB;
		break;
	case COLUMN_TYPE_ANY:
		dest = TupleTypes::TYPE_ANY;
		break;
	default:
		assert(false);
		return TupleColumnType();
	}

	if (ValueProcessor::isArray(src)) {
		dest = static_cast<TupleColumnType>(dest | TupleTypes::TYPE_MASK_ARRAY);
	}

	return dest;
}

bool SQLContainerImpl::ContainerValueUtils::toContainerColumnType(
		TupleColumnType src, ContainerColumnType &dest, bool failOnError) {

	switch (src) {
	case TupleTypes::TYPE_STRING:
		dest = COLUMN_TYPE_STRING;
		break;
	case TupleTypes::TYPE_BOOL:
		dest = COLUMN_TYPE_BOOL;
		break;
	case TupleTypes::TYPE_BYTE:
		dest = COLUMN_TYPE_BYTE;
		break;
	case TupleTypes::TYPE_SHORT:
		dest = COLUMN_TYPE_SHORT;
		break;
	case TupleTypes::TYPE_INTEGER:
		dest = COLUMN_TYPE_INT;
		break;
	case TupleTypes::TYPE_LONG:
		dest = COLUMN_TYPE_LONG;
		break;
	case TupleTypes::TYPE_FLOAT:
		dest = COLUMN_TYPE_FLOAT;
		break;
	case TupleTypes::TYPE_DOUBLE:
		dest = COLUMN_TYPE_DOUBLE;
		break;
	case TupleTypes::TYPE_TIMESTAMP:
		dest = COLUMN_TYPE_TIMESTAMP;
		break;
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		dest = COLUMN_TYPE_MICRO_TIMESTAMP;
		break;
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		dest = COLUMN_TYPE_NANO_TIMESTAMP;
		break;
	case TupleTypes::TYPE_GEOMETRY:
		dest = COLUMN_TYPE_GEOMETRY;
		break;
	case TupleTypes::TYPE_BLOB:
		dest = COLUMN_TYPE_BLOB;
		break;
	case TupleTypes::TYPE_ANY:
		dest = COLUMN_TYPE_ANY;
		break;
	default:
		dest = ContainerColumnType();
		if (failOnError) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		return false;
	}

	return true;
}


SQLContainerImpl::SyntaxExpr
SQLContainerImpl::TQLTool::genPredExpr(
		util::StackAllocator &alloc, const Query &query) {
	typedef Query::QueryAccessor QueryAccessor;
	return genPredExpr(alloc, QueryAccessor::findWhereExpr(query), NULL);
}

SQLContainerImpl::SyntaxExpr
SQLContainerImpl::TQLTool::genPredExpr(
		util::StackAllocator &alloc, const BoolExpr *src, bool *unresolved) {
	typedef Query::BoolExprAccessor BoolExprAccessor;
	static_cast<void>(unresolved);

	if (src == NULL) {
		return genConstExpr(alloc, ValueUtils::toAnyByBool(true));
	}

	const BoolExpr::BoolTerms &argList = BoolExprAccessor::getOperands(*src);
	const Expr *unaryExpr = BoolExprAccessor::getUnary(*src);
	BoolExpr::Operation opType = BoolExprAccessor::getOpType(*src);

	SQLType::Id type;
	switch (opType) {
	case BoolExpr::AND:
		type = SQLType::EXPR_AND;
		break;
	case BoolExpr::OR:
		type = SQLType::EXPR_OR;
		break;
	case BoolExpr::NOT: {
		SyntaxExprRefList *destArgList = genExprList(alloc, &argList, NULL, 1);
		SyntaxExpr expr(alloc);
		expr.op_ = SQLType::OP_NOT;
		expr.left_ = (*destArgList)[0];
		return expr;
	}
	case BoolExpr::UNARY: {
		util::XArray<const Expr*> srcExprList(alloc);
		srcExprList.push_back(unaryExpr);
		SyntaxExprRefList *destExprList = genExprList(alloc, &srcExprList, NULL, 1);
		return *(*destExprList)[0];
	}
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}

	{
		const size_t minArgCount = 2;
		const size_t maxArgCount = std::numeric_limits<size_t>::max();

		SyntaxExpr expr(alloc);
		expr.op_ = type;
		expr.next_ =
				genExprList(alloc, &argList, NULL, minArgCount, maxArgCount);
		return expr;
	}
}

SQLContainerImpl::SyntaxExpr
SQLContainerImpl::TQLTool::genPredExpr(
		util::StackAllocator &alloc, const Expr *src, bool *unresolved) {
	typedef Query::ExprAccessor ExprAccessor;
	assert(unresolved != NULL);

	if (src == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}

	switch (ExprAccessor::getType(*src)) {
	case Expr::VALUE: {
		TupleValue value;
		ContainerValueUtils::toTupleValue(
				alloc, *ExprAccessor::getValue(*src), value, false);
		return genConstExpr(alloc, value);
	}
	case Expr::EXPR: {
		SQLType::Id type = SQLType::START_EXPR;
		switch (ExprAccessor::getOp(*src)) {
		case Expr::NE:
			type = SQLType::OP_NE;
			break;
		case Expr::EQ:
			type = SQLType::OP_EQ;
			break;
		case Expr::LT:
			type = SQLType::OP_LT;
			break;
		case Expr::LE:
			type = SQLType::OP_LE;
			break;
		case Expr::GT:
			type = SQLType::OP_GT;
			break;
		case Expr::GE:
			type = SQLType::OP_GE;
			break;
		default:
			break;
		}

		if (type != SQLType::START_EXPR) {
			SyntaxExprRefList *argList = genExprList(
					alloc, ExprAccessor::getArgList(*src), unresolved, 2);
			SyntaxExpr expr(alloc);
			expr.op_ = type;
			expr.left_ = (*argList)[0];
			expr.right_ = (*argList)[1];
			return expr;
		}

		bool negative = false;
		switch (ExprAccessor::getOp(*src)) {
		case Expr::BETWEEN:
			type = SQLType::EXPR_BETWEEN;
			break;
		case Expr::NOTBETWEEN:
			type = SQLType::EXPR_BETWEEN;
			negative = true;
			break;
		default:
			break;
		}

		if (type != SQLType::START_EXPR) {
			SyntaxExpr expr(alloc);
			expr.op_ = type;
			expr.next_ = genExprList(
					alloc, ExprAccessor::getArgList(*src), unresolved, 3);

			if (negative) {
				SyntaxExpr *left = ALLOC_NEW(alloc) SyntaxExpr(expr);
				expr = SyntaxExpr(alloc);
				expr.op_ = SQLType::OP_NOT;
				expr.left_ = left;
			}
			return expr;
		}

		return genUnresolvedLeafExpr(alloc, unresolved);
	}
	case Expr::COLUMN: {
		SyntaxExpr expr(alloc);
		expr.op_ = SQLType::EXPR_COLUMN;
		expr.inputId_ = 0;
		expr.columnId_ = ExprAccessor::getColumnId(*src);
		expr.columnType_ = ContainerValueUtils::toTupleColumnType(
				ExprAccessor::getExprColumnType(*src), src->isNullable());
		expr.autoGenerated_ = true;
		return expr;
	}
	case Expr::NULLVALUE:
		return genConstExpr(alloc, TupleValue());
	case Expr::BOOL_EXPR:
		return genPredExpr(
				alloc, static_cast<const BoolExpr*>(src), unresolved);
	default:
		return genUnresolvedLeafExpr(alloc, unresolved);
	}
}

SQLContainerImpl::SyntaxExpr
SQLContainerImpl::TQLTool::genUnresolvedTopExpr(
		util::StackAllocator &alloc) {
	SyntaxExpr expr(alloc);
	expr.op_ = SQLType::OP_EQ;

	for (size_t i = 0; i < 2; i++) {
		SyntaxExpr *argExpr = ALLOC_NEW(alloc) SyntaxExpr(alloc);
		argExpr->op_ = SQLType::FUNC_RANDOM;
		(i == 0 ? expr.left_ : expr.right_) = argExpr;
	}

	return expr;
}

SQLContainerImpl::SyntaxExpr
SQLContainerImpl::TQLTool::genUnresolvedLeafExpr(
		util::StackAllocator &alloc, bool *unresolved) {
	if (unresolved != NULL) {
		*unresolved = true;
	}
	return genConstExpr(alloc, TupleValue());
}

SQLContainerImpl::SyntaxExpr
SQLContainerImpl::TQLTool::genConstExpr(
		util::StackAllocator &alloc, const TupleValue &value) {
	SyntaxExpr expr(alloc);
	expr.op_ = SQLType::EXPR_CONSTANT;
	expr.value_ = value;
	return expr;
}

template<typename T>
SQLContainerImpl::SyntaxExprRefList*
SQLContainerImpl::TQLTool::genExprList(
		util::StackAllocator &alloc, const util::XArray<T*> *src,
		bool *unresolved, size_t argCount) {
	return genExprList(alloc, src, unresolved, argCount, argCount);
}

template<typename T>
SQLContainerImpl::SyntaxExprRefList*
SQLContainerImpl::TQLTool::genExprList(
		util::StackAllocator &alloc, const util::XArray<T*> *src,
		bool *unresolved, size_t minArgCount, size_t maxArgCount) {
	if (src == NULL ||
			!(minArgCount <= src->size() && src->size() <= maxArgCount)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}

	SyntaxExprRefList *dest = ALLOC_NEW(alloc) SyntaxExprRefList(alloc);
	for (typename util::XArray<T*>::const_iterator it = src->begin();
			it != src->end(); ++it) {
		bool unresolvedLocal = false;
		SyntaxExpr expr = genPredExpr(alloc, *it, &unresolvedLocal);

		if (unresolved == NULL) {
			if (unresolvedLocal) {
				expr = genUnresolvedTopExpr(alloc);
			}
		}
		else {
			*unresolved |= unresolvedLocal;
		}

		dest->push_back(ALLOC_NEW(alloc) SyntaxExpr(expr));
	}

	return dest;
}

