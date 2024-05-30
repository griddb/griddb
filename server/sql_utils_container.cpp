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


//
// ScanCursor
//

SQLContainerUtils::ScanCursor::~ScanCursor() {
}

SQLContainerUtils::ScanCursor::ScanCursor() {
}

//
// ScanCursor::Holder
//

SQLContainerUtils::ScanCursor::Holder::~Holder() {
}

SQLContainerUtils::ScanCursor::Holder::Holder() {
}

//
// ScanCursorAccessor
//

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

//
// ScanCursorAccessor::Source
//

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
		partialExecCountBased_(false),
		cxtSrc_(cxtSrc) {
}

//
// IndexScanInfo
//

SQLContainerUtils::IndexScanInfo::IndexScanInfo(
		const SQLExprs::IndexConditionList &condList,
		const uint32_t *inputIndex) :
		condList_(&condList),
		inputIndex_(inputIndex) {
}

//
// ContainerUtils
//

size_t SQLContainerUtils::ContainerUtils::findMaxStringLength(
		const ResourceSet *resourceSet) {
	do {
		if (resourceSet == NULL) {
			break;
		}

		const DataStoreConfig *dsConfig = resourceSet->getDataStoreConfig();

		if (dsConfig == NULL) {
			break;
		}

		return dsConfig->getLimitSmallSize();
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

//
// CursorImpl
//

SQLContainerImpl::CursorImpl::CursorImpl(Accessor &accessor, Data &data) :
		accessor_(accessor),
		data_(data) {
}

SQLContainerImpl::CursorImpl::~CursorImpl() {
}

bool SQLContainerImpl::CursorImpl::scanFull(
		OpContext &cxt, const Projection &proj) {
	bool done = false;
	LocalCursorScope scope(cxt, accessor_);
	try {
		TransactionContext &txn = resolveTransactionContext();

		BtreeSearchContext *sc;
		ContainerRowScanner *scanner;
		switch (prepareFullScan(cxt, sc, proj, scanner)) {
		case TYPE_COLLECTION_ROWS:
			resolveCollection().scanRowIdIndex(txn, *sc, *scanner);
			break;
		case TYPE_TIME_SERIES_ROWS:
			resolveTimeSeries().scanRowIdIndex(txn, *sc, getOrder(), *scanner);
			break;
		default:
			break;
		}

		finishFullScan(cxt, sc, done);
		scope.close();
	}
	catch (...) {
		std::exception e;
		scope.closeWithError(e);
	}
	return done;
}

bool SQLContainerImpl::CursorImpl::scanRange(
		OpContext &cxt, const Projection &proj) {
	bool done = false;
	LocalCursorScope scope(cxt, accessor_);
	try {
		TransactionContext &txn = resolveTransactionContext();

		ContainerRowScanner *scanner;
		SearchResult *result;
		switch (prepareRangeScan(cxt, proj, scanner, result)) {
		case TYPE_COLLECTION_ROWS:
			scanRow(txn, resolveCollection(), *result, *scanner);
			break;
		case TYPE_TIME_SERIES_ROWS:
			scanRow(txn, resolveTimeSeries(), *result, *scanner);
			break;
		default:
			break;
		}

		finishRangeScan(cxt, done);
		scope.close();
	}
	catch (...) {
		std::exception e;
		scope.closeWithError(e);
	}
	return done;
}

bool SQLContainerImpl::CursorImpl::scanIndex(
		OpContext &cxt, const IndexScanInfo &info) {
	bool done = false;
	LocalCursorScope scope(cxt, accessor_);
	try {
		TransactionContext &txn = resolveTransactionContext();

		IndexSearchContext *sc;
		SearchResult *result;
		do {
			switch (prepareIndexSearch(cxt, sc, info, result)) {
			case TYPE_TIME_SERIES_KEY_TREE:
				resolveTimeSeries().searchRowIdIndexAsRowArray(
						txn, sc->tree(), result->normalRa(), result->mvccRa());
				break;
			case TYPE_TIME_SERIES_VALUE_TREE:
				resolveTimeSeries().searchColumnIdIndex(
						txn, sc->tree(), result->normal(), result->mvcc());
				break;
			case TYPE_DEFAULT_TREE:
				resolveContainer().searchColumnIdIndex(
						txn, sc->tree(), result->normal(), result->mvcc());
				break;
			default:
				break;
			}
		}
		while (finishIndexSearch(cxt, sc, done));
		scope.close();
	}
	catch (...) {
		std::exception e;
		scope.closeWithError(e);
	}
	return done;
}

bool SQLContainerImpl::CursorImpl::scanMeta(
		OpContext &cxt, const Projection &proj, const Expression *pred) {
	cxt.closeLocalTupleList(data_.outIndex_);
	cxt.createLocalTupleList(data_.outIndex_);

	bool done = false;
	LocalCursorScope scope(cxt, accessor_);
	try {
		TransactionContext &txn = resolveTransactionContext();

		MetaScanHandler handler(cxt, *this, proj);
		MetaProcessorSource *source;
		MetaProcessor *proc = prepareMetaProcessor(
				cxt, txn, getNextContainerId(), pred, source);

		proc->scan(txn, *source, handler);

		setNextContainerId(proc->getNextContainerId());
		done = !proc->isSuspended();

		scope.close();
	}
	catch (...) {
		std::exception e;
		scope.closeWithError(e);
	}
	cxt.closeLocalWriter(data_.outIndex_);
	return done;
}

bool SQLContainerImpl::CursorImpl::scanVisited(
		OpContext &cxt, const Projection &proj) {
	bool done = true;
	RowIdFilter *filter = accessor_.findRowIdFilter();
	if (filter != NULL) {
		uint64_t rest = std::max<uint64_t>(cxt.getNextCountForSuspend(), 1);
		SQLExprs::ExprContext &exprCxt = cxt.getExprContext();
		RowIdFilter::RowIdCursor cursor(*filter);
		for (; cursor.exists(); cursor.next()) {
			if (--rest <= 0) {
				done = false;
				break;
			}
			const int64_t lastId = static_cast<int64_t>(cursor.get()) - 1;
			exprCxt.setLastTupleId(0, lastId);
			proj.project(cxt);
		}
	}
	return done;
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

void SQLContainerImpl::CursorImpl::setUpConditionColumnTypeList(
		const BaseContainer &container,
		SQLExprs::IndexConditionList::const_iterator condBegin,
		SQLExprs::IndexConditionList::const_iterator condEnd,
		util::Vector<ContainerColumnType> &condTypeList) {
	condTypeList.clear();
	for (SQLExprs::IndexConditionList::const_iterator it = condBegin;
			it != condEnd; ++it) {
		const SQLExprs::IndexCondition &cond = *it;
		const ContainerColumnType columnType =
				container.getColumnInfo(cond.column_).getColumnType();
		condTypeList.push_back(columnType);
	}
}

void SQLContainerImpl::CursorImpl::setUpSearchContext(
		TransactionContext &txn,
		SQLExprs::IndexConditionList::const_iterator condBegin,
		SQLExprs::IndexConditionList::const_iterator condEnd,
		const util::Vector<ContainerColumnType> &condTypeList,
		BtreeSearchContext &sc) {
	SearchContextPool *scPool = sc.getPool();
	assert(scPool != NULL);

	Value::Pool &valuePool = scPool->condValuePool_;

	const bool forKey = true;
	for (SQLExprs::IndexConditionList::const_iterator it = condBegin;
			it != condEnd; ++it) {
		const SQLExprs::IndexCondition &cond = *it;

		assert(static_cast<size_t>(it - condBegin) < condTypeList.size());
		const ContainerColumnType columnType = condTypeList[it - condBegin];

		if (cond.opType_ != SQLType::OP_EQ &&
				cond.opType_ != SQLType::EXPR_BETWEEN) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		if (!SQLValues::TypeUtils::isNull(cond.valuePair_.first.getType())) {
			Value &value = ContainerValueUtils::toContainerValue(
					valuePool, cond.valuePair_.first);

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

			TermCondition c = makeTermCondition(
					columnType, columnType,
					opType, cond.column_, value.data(), value.size());
			sc.addCondition(txn, c, forKey);
		}

		if (!SQLValues::TypeUtils::isNull(cond.valuePair_.second.getType())) {
			Value &value = ContainerValueUtils::toContainerValue(
					valuePool, cond.valuePair_.second);

			DSExpression::Operation opType;
			if (cond.inclusivePair_.second) {
				opType = DSExpression::LE;
			}
			else {
				opType = DSExpression::LT;
			}

			TermCondition c = makeTermCondition(
					columnType, columnType,
					opType, cond.column_, value.data(), value.size());
			sc.addCondition(txn, c, forKey);
		}

		if (cond.firstHitOnly_) {
			sc.setLimit(1);
			sc.setOutputOrder(
					(cond.descending_ ? ORDER_DESCENDING : ORDER_ASCENDING));
		}
	}
}

TermCondition SQLContainerImpl::CursorImpl::makeTermCondition(
		ColumnType columnType, ColumnType valueType,
		DSExpression::Operation opType, uint32_t columnId,
		const void *value, uint32_t valueSize) {
	const bool opExtra = true;
	return TermCondition(
			columnType, valueType, opType, columnId, value, valueSize,
			opExtra);
}

SQLContainerImpl::CursorImpl::RowsTargetType
SQLContainerImpl::CursorImpl::prepareFullScan(
		OpContext &cxt, BtreeSearchContext *&sc, const Projection &proj,
		ContainerRowScanner *&scanner) {
	OpSimulator::handlePoint(cxt.getSimulator(), OpSimulator::POINT_SCAN_MAIN);

	sc = NULL;
	scanner = NULL;

	BaseContainer *container = accessor_.getContainer();
	if (container == NULL) {
		return TYPE_NO_ROWS;
	}

	TransactionContext &txn = resolveTransactionContext();
	IndexSearchContext &indexSc = prepareIndexSearchContext(cxt, NULL);
	sc = &indexSc.tree();

	RowIdFilter *rowIdFilter = checkRowIdFilter(txn, *container, true);
	scanner = tryCreateScanner(cxt, proj, *container, rowIdFilter);

	ColumnIdList &columnList = indexSc.columnList();
	columnList.clear();
	columnList.push_back(container->getRowIdColumnId());

	preparePartialSearch(cxt, *sc, NULL, columnList, NULL, NULL, 0);
	return getRowsTargetType(*container);
}

void SQLContainerImpl::CursorImpl::finishFullScan(
		OpContext &cxt, BtreeSearchContext *sc, bool &done) {
	done = true;

	if (sc == NULL) {
		return;
	}

	const bool partiallyFinished = finishPartialSearch(cxt, *sc, false);

	done = partiallyFinished;
	if (done) {
		updateScanner(cxt, done);
	}

	prepareIndexSearchContext(cxt, NULL).clear();
	updatePartialExecutionSize(cxt, done);
}

SQLContainerImpl::CursorImpl::RowsTargetType
SQLContainerImpl::CursorImpl::prepareRangeScan(
		OpContext &cxt, const Projection &proj,
		ContainerRowScanner *&scanner, SearchResult *&result) {
	OpSimulator::handlePoint(cxt.getSimulator(), OpSimulator::POINT_SCAN_OID);

	scanner = NULL;
	result = NULL;

	BaseContainer *container = accessor_.getContainer();
	TupleUpdateRowIdHandler *handler = accessor_.findUpdateRowIdHandler();
	OIdTable *oIdTable = accessor_.findOIdTable();

	if (detectRangeScanFinish(container, handler, oIdTable)) {
		return TYPE_NO_ROWS;
	}
	assert(container != NULL);

	TransactionContext &txn = resolveTransactionContext();

	RowIdFilter *rowIdFilter = checkRowIdFilter(txn, *container, false);
	if (checkUpdatesStart(*handler, *oIdTable)) {
		rowIdFilter = &restartRangeScanWithUpdates(
				cxt, txn, proj, *container, *oIdTable);
	}

	const uint64_t suspendLimit = cxt.getNextCountForSuspend();
	if (!handler->apply(*oIdTable, suspendLimit)) {
		return TYPE_NO_ROWS;
	}

	SearchResult &baseResult = prepareIndexSearchContext(cxt, NULL).result();
	assert(baseResult.isEmpty());

	const bool withUpdatesAlways =
			(accessor_.isRowDuplicatable() || accessor_.isRowIdFiltering());
	oIdTable->popAll(
			baseResult, suspendLimit, container->getNormalRowArrayNum(),
			withUpdatesAlways);

	if (baseResult.isEmpty()) {
		return TYPE_NO_ROWS;
	}

	scanner = tryCreateScanner(cxt, proj, *container, rowIdFilter);
	result = &baseResult;

	return getRowsTargetType(*container);
}

void SQLContainerImpl::CursorImpl::finishRangeScan(
		OpContext &cxt, bool &done) {
	done = false;

	BaseContainer *container = accessor_.getContainer();
	TupleUpdateRowIdHandler *handler = accessor_.findUpdateRowIdHandler();
	OIdTable *oIdTable = accessor_.findOIdTable();

	const bool totallyFinished =
			detectRangeScanFinish(container, handler, oIdTable);

	ResultSet &resultSet = accessor_.resolveResultSet();
	RowIdFilter *rowIdFilter = accessor_.findRowIdFilter();

	IndexSearchContext *sc =
			accessor_.resolveScope().findIndexSearchContext();
	if (sc != NULL) {
		sc->clear();
	}

	if (totallyFinished) {
		accessor_.clearResultSetPreserving(resultSet, false);
		accessor_.setRangeScanFinished();
	}
	else {
		accessor_.activateResultSetPreserving(resultSet);
	}

	bool rowDuplicatable = false;
	if (!totallyFinished && RowIdFilter::tryAdjust(rowIdFilter) &&
			!accessor_.isRowDuplicatable() &&
			oIdTable->isUpdatesAcceptable()) {
		setRowDuplicatable(cxt);
		rowDuplicatable = true;
	}

	if (totallyFinished || rowDuplicatable) {
		applyRangeScanResultProfile(cxt, oIdTable);
		done = true;
	}

	updateScanner(cxt, done);
	updatePartialExecutionSize(cxt, done);
}

SQLContainerImpl::CursorImpl::IndexTargetType
SQLContainerImpl::CursorImpl::prepareIndexSearch(
		OpContext &cxt, IndexSearchContext *&sc,
		const IndexScanInfo &info, SearchResult *&result) {
	OpSimulator::handlePoint(cxt.getSimulator(), OpSimulator::POINT_SCAN_MAIN);

	sc = &prepareIndexSearchContext(cxt, &info);
	result = NULL;

	IndexProfiler *&profiler = data_.lastIndexProfiler_;
	profiler = NULL;

	BaseContainer *container = accessor_.getContainer();
	const SQLExprs::IndexConditionList &condList = sc->conditionList();

	bool cursorInitialized;
	IndexConditionCursor &condCursor =
			prepareIndexConditionCursor(condList, cursorInitialized);

	if (cursorInitialized && container != NULL) {
		OIdTable &oIdTable = accessor_.prepareOIdTable(*container);
		setUpOIdTable(cxt, oIdTable, condList);
	}

	IndexInfoList &infoList = sc->indexInfoList();
	ColumnIdList &columnList = sc->columnList();

	size_t pos;
	IndexTargetType targetType;
	for (;; condCursor.next()) {
		if (condCursor.exists()) {
			pos = condCursor.getCurrentPosition();
			targetType = acceptIndexCondition(
					cxt, container, infoList, condList, pos, columnList);
			if (targetType == TYPE_NO_INDEX) {
				condCursor.setNextPosition(
						pos + SQLExprs::IndexSelector::nextCompositeConditionDistance(
								condList.begin() + pos, condList.end()));
				continue;
			}
			break;
		}
		condCursor.close();
		sc = NULL;
		return TYPE_NO_INDEX;
	}

	util::Vector<ContainerColumnType> &typeList = sc->columnTypeList();

	size_t nextPos;
	const size_t subPos = condCursor.getCurrentSubPosition();
	BtreeConditionUpdator *updator = prepareIndexConditionUpdator(
			cxt, *sc, *container, info, condList, pos, subPos, nextPos);
	condCursor.setNextPosition(nextPos);

	preparePartialSearch(
			cxt, sc->tree(), updator, columnList, &condList, &typeList, pos);

	result = &sc->result();
	assert(result->isEmpty());

	if (profiler != NULL) {
		profiler->startSearch();
	}
	return targetType;
}

bool SQLContainerImpl::CursorImpl::finishIndexSearch(
		OpContext &cxt, IndexSearchContext *sc, bool &done) {
	done = true;

	IndexProfiler *&profiler = data_.lastIndexProfiler_;
	if (profiler != NULL) {
		profiler->finishSearch();
		applyIndexResultProfile(sc, *profiler);
	}

	OIdTable *oIdTable = accessor_.findOIdTable();
	bool partiallyFinished;
	bool continuable;
	if (sc == NULL) {
		partiallyFinished = true;
		continuable = false;
	}
	else {
		ScanSimulatorUtils::tryAcceptUpdates(cxt, accessor_, sc->result());

		IndexConditionCursor &condCursor = *data_.condCursor_;
		assert(condCursor.exists());

		const size_t pos = condCursor.getCurrentPosition();
		oIdTable->addAll(getIndexGroupId(pos), sc->result());

		partiallyFinished = finishPartialSearch(cxt, sc->tree(), true);

		if (partiallyFinished) {
			condCursor.next();
			continuable = condCursor.exists();
		}
		else {
			condCursor.setCurrentSubPosition(
					sc->conditionUpdator().getSubPosition());
			continuable = false;
		}
		sc->clear();
	}

	if (partiallyFinished && !continuable) {
		if (oIdTable == NULL) {
			done = true;
		}
		else {
			done = oIdTable->finalizeAddition(cxt.getNextCountForSuspend());
		}
	}
	else {
		done = false;
	}

	updatePartialExecutionSize(cxt, done);
	return continuable;
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
		clearScanner();
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
	if (data_.updatorList_.get() == NULL) {
		return;
	}
	UpdatorList &updatorList = *data_.updatorList_;

	for (UpdatorList::iterator it = updatorList.begin();
			it != updatorList.end(); ++it) {
		it->first(cxt, it->second);
	}

	if (finished) {
		clearScanner();
	}
}

void SQLContainerImpl::CursorImpl::clearScanner() {
	ContainerRowScanner *&scanner = data_.scanner_;
	util::LocalUniquePtr<UpdatorList> &updatorList = data_.updatorList_;

	scanner = NULL;
	if (updatorList.get() != NULL) {
		updatorList->clear();
	}
}

void SQLContainerImpl::CursorImpl::preparePartialSearch(
		OpContext &cxt, BtreeSearchContext &sc, BtreeConditionUpdator *updator,
		const ColumnIdList &indexColumnList,
		const SQLExprs::IndexConditionList *targetCondList,
		util::Vector<ContainerColumnType> *condTypeList, size_t condPos) {
	TransactionContext &txn = resolveTransactionContext();
	BaseContainer &container = resolveContainer();
	ResultSet &resultSet = accessor_.resolveResultSet();

	{
		const uint64_t restSize = cxt.getNextCountForSuspend();
		const uint64_t execSize =
				std::min<uint64_t>(restSize, data_.lastPartialExecSize_);

		resultSet.setPartialExecuteSize(std::max<uint64_t>(execSize, 1));
	}

	const bool lastSuspended = resultSet.isPartialExecuteSuspend();
	const ResultSize suspendLimit = resultSet.getPartialExecuteSize();
	const OutputOrder outputOrder = ORDER_UNDEFINED;

	accessor_.clearResultSetPreserving(resultSet, false);

	if (targetCondList == NULL) {
		resultSet.setUpdateIdType(ResultSet::UPDATE_ID_NONE);
		assert(sc.getColumnIds().empty());
		sc.setColumnIds(indexColumnList);
	}
	else {
		assert(condTypeList != NULL);

		SQLExprs::IndexConditionList::const_iterator condBegin =
				targetCondList->begin() + condPos;

		const size_t condCount =
				SQLExprs::IndexSelector::nextCompositeConditionDistance(
						condBegin, targetCondList->end());
		SQLExprs::IndexConditionList::const_iterator condEnd =
				condBegin + condCount;

		setUpConditionColumnTypeList(
				container, condBegin, condEnd, *condTypeList);

		assert(sc.getColumnIds().empty());
		sc.setColumnIds(indexColumnList);
		if (updator == NULL) {
			setUpSearchContext(txn, condBegin, condEnd, *condTypeList, sc);
		}
		sc.setTermConditionUpdator(updator);

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
	TransactionContext &txn = resolveTransactionContext();
	BaseContainer &container = resolveContainer();
	ResultSet &resultSet = accessor_.resolveResultSet();

	const bool lastSuspended = resultSet.isPartialExecuteSuspend();
	ResultSize suspendLimit = resultSet.getPartialExecuteSize();

	resultSet.setPartialContext(sc, suspendLimit, 0, MAP_TYPE_BTREE);

	if (!lastSuspended) {
		if (!forIndex) {
			resultSet.setLastRowId(accessor_.getMaxRowId(txn, container));
		}
	}

	BtreeConditionUpdator *updator = sc.getTermConditionUpdator();
	const bool finished =
			(cxt.isCompleted() || (!sc.isSuspended() &&
					(updator == NULL || !updator->exists())));
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

	return finished;
}

void SQLContainerImpl::CursorImpl::updatePartialExecutionSize(
		OpContext &cxt, bool done) {
	if (done) {
		return;
	}

	if (accessor_.isPartialExecCountBased()) {
		cxt.setNextCountForSuspend(0);
		return;
	}

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
		ResultSet &resultSet = accessor_.resolveResultSet();
		if (elapsed > threshold &&
				resultSet.getPartialExecuteCount() <=
				scope.getStartPartialExecCount() + 1) {
			last = std::max(last / 2, range.first);
		}
		cxt.setNextCountForSuspend(0);
	}
}

bool SQLContainerImpl::CursorImpl::checkUpdatesStart(
		TupleUpdateRowIdHandler &handler, OIdTable &oIdTable) {
	if (oIdTable.isUpdatesAcceptable()) {
		return false;
	}

	return handler.isUpdated();
}

SQLContainerImpl::RowIdFilter&
SQLContainerImpl::CursorImpl::restartRangeScanWithUpdates(
		OpContext &cxt, TransactionContext &txn, const Projection &proj,
		BaseContainer &container, OIdTable &oIdTable) {
	assert(!oIdTable.isUpdatesAcceptable());

	if (oIdTable.isCursorStarted()) {
		const bool forPipe = true;
		if (SQLOps::OpCodeBuilder::findAggregationProjection(
				proj, &forPipe) != NULL) {
			cxt.getExprContext().initializeAggregationValues();
			oIdTable.restartCursor();
		}
	}

	clearScanner();

	oIdTable.acceptUpdates();
	return accessor_.prepareRowIdFilter(txn, container);
}

bool SQLContainerImpl::CursorImpl::detectRangeScanFinish(
		BaseContainer *container, TupleUpdateRowIdHandler *handler,
		OIdTable *oIdTable) {
	if (container == NULL || handler == NULL || oIdTable == NULL ||
			accessor_.isIndexLost()) {
		return true;
	}

	return oIdTable->isCursorFinished();
}

void SQLContainerImpl::CursorImpl::applyRangeScanResultProfile(
		OpContext &cxt, OIdTable *oIdTable) {
	if (oIdTable == NULL || !cxt.isProfiling()) {
		return;
	}

	uint32_t nextOrdinal = 0;
	for (size_t i = 0; i < IndexProfiler::END_MOD; i++) {
		const IndexProfiler::ModificationType modification =
				static_cast<IndexProfiler::ModificationType>(i);
		IndexProfiler *profiler = NULL;
		for (size_t j = 0; j < 2; j++) {
			const bool onMvcc = (j > 0);

			int64_t count;
			if (!oIdTable->getModificationProfile(
					modification, onMvcc, count)) {
				continue;
			}

			if (profiler == NULL) {
				profiler = cxt.getIndexProfiler(nextOrdinal);
				nextOrdinal++;
			}
			profiler->addHitCount(count, onMvcc);
		}
	}
}

SQLContainerImpl::CursorImpl::IndexTargetType
SQLContainerImpl::CursorImpl::acceptIndexCondition(
		OpContext &cxt, BaseContainer *container, IndexInfoList &infoList,
		const SQLExprs::IndexConditionList &targetCondList, size_t pos,
		ColumnIdList &indexColumnList) {
	if (container == NULL) {
		return TYPE_NO_INDEX;
	}

	if (accessor_.isIndexLost()) {
		setIndexLost(cxt);
		return TYPE_NO_INDEX;
	}

	const size_t condCount =
			SQLExprs::IndexSelector::nextCompositeConditionDistance(
					targetCondList.begin() + pos, targetCondList.end());
	{
		assert(condCount > 0);
		const SQLExprs::IndexCondition &cond = targetCondList[pos];

		if (condCount <= 1) {
			if (cond.isEmpty() || cond.equals(true)) {
				setIndexLost(cxt);
				return TYPE_NO_INDEX;
			}
			else if (cond.isNull() || cond.equals(false)) {
				return TYPE_NO_INDEX;
			}
		}

		if (cond.specId_ > 0) {
			const SQLExprs::IndexSpec &spec = cond.spec_;
			indexColumnList.assign(
					spec.columnList_, spec.columnList_ + spec.columnCount_);
		}
		else {
			indexColumnList.clear();
			indexColumnList.push_back(cond.column_);
		}
	}

	TransactionContext &txn = resolveTransactionContext();

	if (infoList.empty()) {
		Accessor::getIndexInfoList(txn, *container, infoList);
	}

	for (IndexInfoList::iterator it = infoList.begin();; ++it) {
		if (it == infoList.end()) {
			setIndexLost(cxt);
			return TYPE_NO_INDEX;
		}
		else if (it->mapType == MAP_TYPE_BTREE &&
				it->columnIds_ == indexColumnList) {
			acceptIndexInfo(
					cxt, txn, *container, targetCondList, *it, pos, condCount);
			return detectIndexTargetType(
					SQLType::INDEX_TREE_RANGE, *container, targetCondList,
					pos);
		}
	}
}

SQLContainerImpl::CursorImpl::IndexTargetType
SQLContainerImpl::CursorImpl::detectIndexTargetType(
		IndexType indexType, BaseContainer &container,
		const SQLExprs::IndexConditionList &condList, size_t pos) {
	assert(pos < condList.size());

	if (indexType == SQLType::INDEX_TREE_RANGE) {
		if (container.getContainerType() == TIME_SERIES_CONTAINER) {
			if (condList[pos].column_ == ColumnInfo::ROW_KEY_COLUMN_ID) {
				return TYPE_TIME_SERIES_KEY_TREE;
			}
			else {
				return TYPE_TIME_SERIES_VALUE_TREE;
			}
		}
		else {
			return TYPE_DEFAULT_TREE;
		}
	}
	else {
		assert(false);
		return TYPE_NO_INDEX;
	}
}

void SQLContainerImpl::CursorImpl::acceptIndexInfo(
		OpContext &cxt, TransactionContext &txn, BaseContainer &container,
		const SQLExprs::IndexConditionList &targetCondList,
		const IndexInfo &info, size_t pos, size_t condCount) {
	IndexProfiler *profiler = cxt.getIndexProfiler(getIndexOrdinal(pos));
	data_.lastIndexProfiler_ = profiler;

	if (profiler == NULL || !profiler->isEmpty()) {
		return;
	}

	ObjectManagerV4 &objectManager = Accessor::resolveObjectManager(container);

	if (profiler->findIndexName() == NULL) {
		profiler->setIndexName(info.indexName_.c_str());
	}

	for (ColumnIdList::const_iterator it = info.columnIds_.begin();
			it != info.columnIds_.end(); ++it) {
		if (profiler->findColumnName(*it) == NULL) {
			ColumnInfo &columnInfo = container.getColumnInfo(*it);
			profiler->setColumnName(
					*it, columnInfo.getColumnName(
							txn, objectManager,
							container.getMetaAllocateStrategy()));
		}
		profiler->addIndexColumn(*it);
	}

	for (size_t i = 0; i < condCount; i++) {
		profiler->addCondition(targetCondList[pos + i]);
	}
}

void SQLContainerImpl::CursorImpl::applyIndexResultProfile(
		IndexSearchContext *sc, IndexProfiler &profiler) {
	if (sc == NULL) {
		return;
	}

	const SearchResult &result = sc->result();
	for (size_t i = 0; i < 2; i++) {
		const bool onMvcc = (i > 0);
		const size_t count = result.getEntry(onMvcc).getEntrySize();
		profiler.addHitCount(static_cast<int64_t>(count), onMvcc);
	}
}

SQLContainerImpl::OIdTableGroupId
SQLContainerImpl::CursorImpl::getIndexGroupId(size_t pos) {
	do {
		const IndexGroupIdMap *map = data_.indexGroupIdMap_;
		if (map == NULL) {
			break;
		}

		IndexGroupIdMap::const_iterator it = map->find(pos);
		if (it == map->end()) {
			break;
		}

		return it->second;
	}
	while (false);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

uint32_t SQLContainerImpl::CursorImpl::getIndexOrdinal(size_t pos) {
	do {
		const IndexOrdinalMap *map = data_.indexOrdinalMap_;
		if (map == NULL) {
			break;
		}

		IndexOrdinalMap::const_iterator it = map->find(pos);
		if (it == map->end()) {
			break;
		}

		return it->second;
	}
	while (false);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

SQLContainerImpl::IndexSearchContext&
SQLContainerImpl::CursorImpl::prepareIndexSearchContext(
		OpContext &cxt, const IndexScanInfo *info) {
	const uint32_t partialExecMaxMillis = 10;
	const uint32_t *partialExecMaxMillisRef =
			(accessor_.isPartialExecCountBased() ?
					NULL : &partialExecMaxMillis);

	return accessor_.resolveScope().prepareIndexSearchContext(
			cxt.getVarContext(), info, cxt.getAllocatorManager(),
			cxt.getAllocator().base(), partialExecMaxMillisRef,
			accessor_.getPartialExecSizeRange());
}

SQLContainerImpl::CursorImpl::IndexConditionCursor&
SQLContainerImpl::CursorImpl::prepareIndexConditionCursor(
		const SQLExprs::IndexConditionList &condList, bool &initialized) {
	initialized = false;

	util::LocalUniquePtr<IndexConditionCursor> &condCursor = data_.condCursor_;
	if (condCursor.get() == NULL) {
		condCursor = UTIL_MAKE_LOCAL_UNIQUE(
				condCursor, IndexConditionCursor, condList);
		initialized = true;
	}
	return *condCursor;
}

SQLContainerImpl::BtreeConditionUpdator*
SQLContainerImpl::CursorImpl::prepareIndexConditionUpdator(
		OpContext &cxt, IndexSearchContext &sc, BaseContainer &container,
		const IndexScanInfo &info,
		const SQLExprs::IndexConditionList &condList, size_t pos,
		size_t subPos, size_t &nextPos) {
	TupleListReader *reader = NULL;
	const util::Vector<SQLValues::TupleColumn> *inColumnList = NULL;
	if (info.inputIndex_ != NULL) {
		const uint32_t inputIndex = *info.inputIndex_;
		if (!cxt.isInputCompleted(inputIndex)) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		reader = &cxt.getReader(inputIndex);
		inColumnList = &cxt.getInputColumnList(inputIndex);
	}

	const SQLExprs::IndexSelector *selector = &accessor_.getIndexSelection();

	SQLValues::ValueSetHolder &valuesHolder = prepareValueSetHolder(cxt);
	return sc.conditionUpdator().prepare(
			container, valuesHolder, selector, &condList, inColumnList, reader,
			pos, subPos, nextPos);
}

SQLValues::ValueSetHolder&
SQLContainerImpl::CursorImpl::prepareValueSetHolder(OpContext &cxt) {
	util::LocalUniquePtr<SQLValues::ValueSetHolder> &valuesHolder =
			data_.valuesHolder_;
	if (valuesHolder.get() == NULL) {
		util::LocalUniquePtr<SQLValues::ValueContext> &valuesHolderCxt =
				data_.valuesHolderCxt_;

		SQLValues::VarContext &varCxt =
				cxt.getValueContext().getVarContext();

		valuesHolderCxt = UTIL_MAKE_LOCAL_UNIQUE(
				valuesHolderCxt, SQLValues::ValueContext,
				SQLValues::ValueContext::ofVarContext(varCxt));
		valuesHolder = UTIL_MAKE_LOCAL_UNIQUE(
				valuesHolder, SQLValues::ValueSetHolder, *valuesHolderCxt);
	}
	return *valuesHolder;
}

void SQLContainerImpl::CursorImpl::setRowDuplicatable(OpContext &cxt) {
	if (!accessor_.isRowIdFiltering()) {
		OIdTable *oIdTable = accessor_.findOIdTable();
		if (oIdTable != NULL) {
			oIdTable->restartCursor();
		}
		accessor_.clearRowIdFilter();
	}

	setIndexAbortion(cxt, true);
}

void SQLContainerImpl::CursorImpl::setIndexLost(OpContext &cxt) {
	setIndexAbortion(cxt, false);
}

void SQLContainerImpl::CursorImpl::setIndexAbortion(
		OpContext &cxt, bool duplicatableOrLost) {
	data_.scanner_ = NULL;
	if (data_.updatorList_.get() != NULL) {
		data_.updatorList_->clear();
	}

	if (!duplicatableOrLost) {
		accessor_.clearResultSetPreserving(accessor_.resolveResultSet(), true);
	}

	if (accessor_.isRowIdFiltering()) {
		cxt.setCompleted();
	}
	else {
		cxt.invalidate();
	}
	accessor_.acceptIndexAbortion(duplicatableOrLost);
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

MetaProcessor* SQLContainerImpl::CursorImpl::prepareMetaProcessor(
		OpContext &cxt, TransactionContext &txn, ContainerId nextContainerId,
		const Expression *pred, MetaProcessorSource *&source) {
	const FullContainerKey *containerKey;
	source = prepareMetaProcessorSource(cxt, txn, pred, containerKey);

	const ContainerLocation &location = accessor_.getLocation();
	const bool forCore = true;

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	MetaProcessor *proc =
			ALLOC_NEW(alloc) MetaProcessor(txn, location.id_, forCore);
	proc->setNextContainerId(nextContainerId);
	proc->setContainerLimit(cxt.getNextCountForSuspend());
	proc->setContainerKey(containerKey);
	return proc;
}

MetaProcessorSource* SQLContainerImpl::CursorImpl::prepareMetaProcessorSource(
		OpContext &cxt, TransactionContext &txn, const Expression *pred,
		const FullContainerKey *&containerKey) {
	containerKey = NULL;

	ExtOpContext &extCxt = cxt.getExtContext();
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	DataStoreV4 *dataStore = Accessor::resolveDataStoreOnTransaction(extCxt);

	const ContainerLocation &location = accessor_.getLocation();
	util::String *dbName = ALLOC_NEW(alloc) util::String(alloc);
	if (dataStore != NULL) {
		containerKey = predicateToContainerKey(
				cxt.getValueContext(), txn, *dataStore, pred, location,
				*dbName);
	}

	MetaProcessorSource *source = ALLOC_NEW(alloc) MetaProcessorSource(
			location.dbVersionId_, dbName->c_str());
	source->dataStore_ = dataStore;
	source->eventContext_= Accessor::getEventContext(extCxt);
	source->transactionManager_ = Accessor::getTransactionManager(extCxt);
	source->transactionService_ = Accessor::getTransactionService(extCxt);
	source->partitionTable_ = Accessor::getPartitionTable(extCxt);
	source->sqlService_ = Accessor::getSQLService(extCxt);
	source->sqlExecutionManager_ = Accessor::getExecutionManager(extCxt);
	source->isAdministrator_ = extCxt.isAdministrator();
	return source;
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

OutputOrder SQLContainerImpl::CursorImpl::getOrder() {
	return ORDER_UNDEFINED;
}

SQLContainerImpl::CursorImpl::RowsTargetType
SQLContainerImpl::CursorImpl::getRowsTargetType(BaseContainer &container) {
	switch (container.getContainerType()) {
	case COLLECTION_CONTAINER:
		return TYPE_COLLECTION_ROWS;
	case TIME_SERIES_CONTAINER:
		return TYPE_TIME_SERIES_ROWS;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

Collection& SQLContainerImpl::CursorImpl::resolveCollection() {
	BaseContainer &container = resolveContainer();

	assert(container.getContainerType() == COLLECTION_CONTAINER);
	return static_cast<Collection&>(container);
}

TimeSeries& SQLContainerImpl::CursorImpl::resolveTimeSeries() {
	BaseContainer &container = resolveContainer();

	assert(container.getContainerType() == TIME_SERIES_CONTAINER);
	return static_cast<TimeSeries&>(container);
}

BaseContainer& SQLContainerImpl::CursorImpl::resolveContainer() {
	return accessor_.resolveContainer();
}

TransactionContext& SQLContainerImpl::CursorImpl::resolveTransactionContext() {
	return accessor_.resolveTransactionContext();
}

SQLContainerImpl::RowIdFilter* SQLContainerImpl::CursorImpl::checkRowIdFilter(
		TransactionContext& txn, BaseContainer &container, bool readOnly) {
	RowIdFilter *rowIdFilter = accessor_.findRowIdFilter();

	if (rowIdFilter == NULL) {
		if (!accessor_.isRowIdFiltering()) {
			return NULL;
		}

		rowIdFilter = &accessor_.prepareRowIdFilter(txn, container);
	}

	if (readOnly || accessor_.isRowDuplicatable()) {
		rowIdFilter->setReadOnly();
	}

	return rowIdFilter;
}

void SQLContainerImpl::CursorImpl::setUpOIdTable(
		OpContext &cxt, OIdTable &oIdTable,
		const SQLExprs::IndexConditionList &condList) {
	util::StackAllocator &alloc = cxt.getAllocator();

	IndexGroupIdMap *map = ALLOC_NEW(alloc) IndexGroupIdMap(alloc);
	IndexOrdinalMap *ordinalMap = ALLOC_NEW(alloc) IndexOrdinalMap(alloc);

	bool suspendable = true;
	const OIdTableGroupId rootId = -1;
	OIdTableGroupId nextId = 0;
	uint32_t nextOrdinal = 0;
	for (SQLExprs::IndexConditionList::const_iterator it = condList.begin();
			it != condList.end();) {
		const size_t count =
				SQLExprs::IndexSelector::nextCompositeConditionDistance(
						it, condList.end());
		OIdTableGroupId followingId;
		if (it->andOrdinal_ == 0 && count < it->andCount_) {
			oIdTable.createGroup(nextId, rootId, OIdTable::MODE_INTERSECT);
			followingId = nextId;
			nextId++;
		}
		else {
			followingId = rootId;
		}

		const uint64_t pos = static_cast<uint64_t>(it - condList.begin());
		map->insert(std::make_pair(pos, nextId));

		oIdTable.createGroup(nextId, followingId, OIdTable::MODE_UNION);
		nextId++;

		ordinalMap->insert(std::make_pair(pos, nextOrdinal));
		nextOrdinal++;

		suspendable &= !(it->firstHitOnly_);
		it += count;
	}
	oIdTable.setSuspendable(suspendable);

	assert(data_.indexGroupIdMap_ == NULL);
	assert(data_.indexOrdinalMap_ == NULL);

	data_.indexGroupIdMap_ = map;
	data_.indexOrdinalMap_ = ordinalMap;
}

//
// CursorImpl::IndexConditionCursor
//

SQLContainerImpl::CursorImpl::IndexConditionCursor::IndexConditionCursor(
		const SQLExprs::IndexConditionList &condList) :
		currentPos_(0),
		currentSubPos_(0),
		nextPos_(0),
		condCount_(condList.size()) {
}

bool SQLContainerImpl::CursorImpl::IndexConditionCursor::exists() {
	return (currentPos_ < condCount_);
}

void SQLContainerImpl::CursorImpl::IndexConditionCursor::next() {
	if (!exists() || nextPos_ <= currentPos_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	currentPos_ = nextPos_;
}

void SQLContainerImpl::CursorImpl::IndexConditionCursor::close() {
	nextPos_ = condCount_;
	currentPos_ = nextPos_;
}

void SQLContainerImpl::CursorImpl::IndexConditionCursor::setNextPosition(
		size_t pos) {
	assert(pos > currentPos_);
	assert(pos >= nextPos_);
	assert(pos <= condCount_);

	nextPos_ = pos;
}

size_t SQLContainerImpl::CursorImpl::IndexConditionCursor::getCurrentPosition() {
	assert(exists());
	return currentPos_;
}


void SQLContainerImpl::CursorImpl::IndexConditionCursor::setCurrentSubPosition(
		size_t subPos) {
	assert(exists());
	assert(currentPos_ <= subPos);
	assert(subPos <= nextPos_);

	currentSubPos_ = subPos;
}

size_t SQLContainerImpl::CursorImpl::IndexConditionCursor::getCurrentSubPosition() {
	assert(exists());
	return currentSubPos_;
}

//
// CursorImpl::Data
//

SQLContainerImpl::CursorImpl::Data::Data(
		SQLValues::VarAllocator &varAlloc, Accessor &accessor) :
		outIndex_(std::numeric_limits<uint32_t>::max()),
		nextContainerId_(UNDEF_CONTAINERID),
		totalSearchCount_(0),
		lastPartialExecSize_(accessor.getPartialExecSizeRange().first),
		scanner_(NULL),
		cxtRefList_(varAlloc),
		exprCxtRefList_(varAlloc),
		indexGroupIdMap_(NULL),
		indexOrdinalMap_(NULL),
		lastIndexProfiler_(NULL) {
}

//
// CursorImpl::HolderImpl
//

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
	typedef Accessor::ScanCursorAccessor ScanCursorAccessor;
	ScanCursorAccessor &accessor =
			accessorRef_.resolve().resolveAs<ScanCursorAccessor>();
	if (&accessor != accessorPtr_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return static_cast<CursorAccessorImpl&>(accessor);
}

//
// CursorAccessorImpl
//

SQLContainerImpl::CursorAccessorImpl::CursorAccessorImpl(const Source &source) :
		varAlloc_(source.varAlloc_),
		location_(source.location_),
		cxtSrc_(source.cxtSrc_),
		closed_(false),
		rowIdFiltering_(false),
		resultSetPreserving_(false),
		containerExpired_(false),
		indexLost_(false),
		resultSetLost_(false),
		nullsStatsChanged_(false),
		rowDuplicatable_(false),
		rangeScanFinished_(false),
		resultSetId_(UNDEF_RESULTSETID),
		lastPartitionId_(UNDEF_PARTITIONID),
		lastRSManager_(NULL),
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
		partialExecCountBased_(source.partialExecCountBased_),
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
	resultSetId_ = UNDEF_RESULTSETID;

	ResultSetManager *rsManager = lastRSManager_;
	lastRSManager_ = NULL;

	if (withResultSet) {
		assert(rsManager != NULL);
		rsManager->close(rsId);
	}
}

void SQLContainerImpl::CursorAccessorImpl::getIndexSpec(
		ExtOpContext &cxt, const TupleColumnList &inColumnList,
		SQLExprs::IndexSelector &selector) {
	LocalCursorScope scope(cxt, *this, inColumnList);
	try {
		TransactionContext &txn = resolveTransactionContext();
		BaseContainer *container = getContainer();

		if (container == NULL) {
			selector.completeIndex();
		}
		else {
			assignIndexInfo(txn, *container, selector, inColumnList);
		}

		scope.close();
	}
	catch (...) {
		std::exception e;
		scope.closeWithError(e);
	}
}

bool SQLContainerImpl::CursorAccessorImpl::isIndexLost() {
	return indexLost_;
}

bool SQLContainerImpl::CursorAccessorImpl::isRowDuplicatable() {
	return rowDuplicatable_;
}

bool SQLContainerImpl::CursorAccessorImpl::isRangeScanFinished() {
	return rangeScanFinished_;
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

const SQLOps::ContainerLocation&
SQLContainerImpl::CursorAccessorImpl::getLocation() {
	return location_;
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

bool SQLContainerImpl::CursorAccessorImpl::isPartialExecCountBased() {
	return partialExecCountBased_;
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

void SQLContainerImpl::CursorAccessorImpl::handleSearchError(
		std::exception &e) {
	assert(scope_.get() != NULL);

	TransactionContext *txn =
			(scope_.get() == NULL ? NULL : &scope_->getTransactionContext());

	handleSearchError(txn, container_, e);
}

void SQLContainerImpl::CursorAccessorImpl::handleSearchError(
		TransactionContext *txn, BaseContainer *container,
		std::exception &e) {
	if (txn != NULL && container != NULL) {
		container->handleSearchError(*txn, e, GS_ERROR_DS_CON_STATUS_INVALID);
	}
	throw;
}

BaseContainer& SQLContainerImpl::CursorAccessorImpl::resolveContainer() {
	if (container_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	assert(!container_->isInvalid());
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

void SQLContainerImpl::CursorAccessorImpl::checkContainer(
		TransactionContext &txn, BaseContainer *container,
		const ContainerLocation &location, SchemaVersionId schemaVersionId,
		const TupleColumnList &inColumnList) {
	if (container == NULL && (location.type_ != SQLType::TABLE_CONTAINER ||
			location.expirable_)) {
		return;
	}

	GetContainerPropertiesHandler handler;
	if (schemaVersionId == UNDEF_SCHEMAVERSIONID) {
		handler.checkContainerExistence(container);
	}
	else {
		handler.checkContainerSchemaVersion(container, schemaVersionId);
	}
	assert(container != NULL);

	checkContainerValidity(txn, *container, location);
	checkInputInfo(*container, inColumnList);
	checkPartitioningVersion(txn, *container, location);
	checkContainerSize(txn, *container, location);
}

void SQLContainerImpl::CursorAccessorImpl::checkContainerValidity(
		TransactionContext &txn, BaseContainer &container,
		const ContainerLocation &location) {
	if (container.isInvalid()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_DS_CON_STATUS_INVALID,
				"Invalid container ("
				"name=" << ContainerNameFormatter(
						txn, container, location) << ")");
	}
}

void SQLContainerImpl::CursorAccessorImpl::checkInputInfo(
		const BaseContainer &container, const TupleColumnList &list) {
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

void SQLContainerImpl::CursorAccessorImpl::checkPartitioningVersion(
		TransactionContext &txn, BaseContainer &container,
		const ContainerLocation &location) {
	if (location.partitioningVersionId_ >= 0 &&
			container.getAttribute() == CONTAINER_ATTR_SUB &&
			location.partitioningVersionId_ <
					container.getTablePartitioningVersionId()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION,
				"Partitioning version mismatch ("
				"container=" << ContainerNameFormatter(
						txn, container, location) <<
				", cached=" << location.partitioningVersionId_ <<
				", actual=" << container.getTablePartitioningVersionId() << ")");
	}
}

void SQLContainerImpl::CursorAccessorImpl::checkContainerSize(
		TransactionContext &txn, BaseContainer &container,
		const ContainerLocation &location) {
	if (location.approxSize_ < 0) {
		return;
	}

	const uint64_t lowerRate = 50;
	const uint64_t upperRate = 200;
	const uint64_t max = std::numeric_limits<uint64_t>::max();
	const uint64_t cached = static_cast<uint64_t>(location.approxSize_);
	const uint64_t actual = container.getRowNum();

	if (cached < max / upperRate ?
			(cached * lowerRate / 100 > actual ||
					cached * upperRate / 100 < actual) :
			(cached / 100 * lowerRate > actual ||
					cached / 100 * upperRate < actual)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DML_CONTAINER_STAT_GAP_TOO_LARGE,
				"Too large gap of container size between cache and actual ("
				"container=" << ContainerNameFormatter(
						txn, container, location) <<
				", cached=" << cached <<
				", actual=" << actual << ")");
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
SQLContainerImpl::CursorAccessorImpl::findUpdateRowIdHandler() {
	return updateRowIdHandler_.get();
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

void SQLContainerImpl::CursorAccessorImpl::acceptIndexAbortion(
		bool duplicatableOrLost) {
	bool &state = (duplicatableOrLost ? rowDuplicatable_ : indexLost_);
	state = true;
}

void SQLContainerImpl::CursorAccessorImpl::setRangeScanFinished() {
	if (isIndexLost()) {
		return;
	}
	rangeScanFinished_ = true;
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
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	IndexInfoList indexInfoList(alloc);
	getIndexInfoList(txn, container, indexInfoList);

	util::Vector<IndexSelector::IndexFlags> flagsList(alloc);
	for (IndexInfoList::iterator it = indexInfoList.begin();
			it != indexInfoList.end(); ++it) {
		const ColumnIdList &columnIds = it->columnIds_;

		flagsList.clear();
		bool acceptable = true;
		for (ColumnIdList::const_iterator colIt =
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
		IndexInfoList &indexInfoList) {
	container.getIndexInfoList(txn, indexInfoList);

	if (container.getContainerType() == TIME_SERIES_CONTAINER) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();

		ColumnIdList columnIds(alloc);
		ColumnId firstColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		columnIds.push_back(firstColumnId);
		IndexInfo indexInfo(alloc, columnIds, MAP_TYPE_BTREE);

		indexInfoList.push_back(indexInfo);
	}
}

SQLContainerImpl::OIdTable&
SQLContainerImpl::CursorAccessorImpl::resolveOIdTable() {
	if (findOIdTable() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *oIdTable_;
}

SQLContainerImpl::OIdTable&
SQLContainerImpl::CursorAccessorImpl::prepareOIdTable(
		BaseContainer &container) {
	if (findOIdTable() == NULL) {
		OpContext cxt(cxtSrc_);
		const OIdTable::Source &source = OIdTable::Source::ofContext(
				cxt, container, getMemoryLimit());
		oIdTable_ = ALLOC_UNIQUE(varAlloc_, OIdTable, source);
	}
	return *oIdTable_;
}

SQLContainerImpl::OIdTable*
SQLContainerImpl::CursorAccessorImpl::findOIdTable() {
	return oIdTable_.get();
}

SQLContainerImpl::RowIdFilter&
SQLContainerImpl::CursorAccessorImpl::prepareRowIdFilter(
		TransactionContext &txn, BaseContainer &container) {
	if (rowIdFilter_.get() == NULL) {
		OpContext cxt(cxtSrc_);
		const RowId maxRowId = getMaxRowId(txn, container);
		const int64_t memLimit = getMemoryLimit();

		rowIdFilter_ = ALLOC_UNIQUE(
				varAlloc_, RowIdFilter,
				cxtSrc_, cxt.getAllocatorManager(), cxt.getAllocator().base(),
				maxRowId, memLimit);
	}
	return *rowIdFilter_;
}

SQLContainerImpl::RowIdFilter*
SQLContainerImpl::CursorAccessorImpl::findRowIdFilter() {
	return rowIdFilter_.get();
}

void SQLContainerImpl::CursorAccessorImpl::clearRowIdFilter() {
	rowIdFilter_.reset();
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
	if (resourceSet == NULL || !cxt.isOnTransactionService()) {
		return NULL;
	}
	const PartitionId pId = cxt.getEvent()->getPartitionId();
	Partition &partition = resourceSet->getPartitionList()->partition(pId);
	if (!partition.isActive()) {
		return NULL;
	}
	DataStoreBase &dsBase = partition.dataStore();
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

DataStoreV4& SQLContainerImpl::CursorAccessorImpl::resolveDataStore(
		BaseContainer &container) {
	DataStoreV4 *dataStore = container.getDataStore();
	if (dataStore == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *dataStore;
}

DataStoreV4*
SQLContainerImpl::CursorAccessorImpl::resolveDataStoreOnTransaction(
		ExtOpContext &cxt) {
	DataStoreV4 *dataStore = getDataStore(cxt);

	if (dataStore == NULL && cxt.isOnTransactionService()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return dataStore;
}

ObjectManagerV4& SQLContainerImpl::CursorAccessorImpl::resolveObjectManager(
		BaseContainer &container) {
	ObjectManagerV4 *objectManager = container.getObjectManager();
	if (objectManager == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *objectManager;
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

//
// DirectValueAccessor
//

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

//
// Constants
//

const uint16_t SQLContainerImpl::Constants::SCHEMA_INVALID_COLUMN_ID =
		std::numeric_limits<uint16_t>::max();
const uint32_t SQLContainerImpl::Constants::ESTIMATED_ROW_ARRAY_ROW_COUNT =
		50;

//
// ColumnCode
//

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

//
// ColumnEvaluator
//

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

//
// IdExpression
//

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

//
// GeneralCondEvaluator
//

void SQLContainerImpl::GeneralCondEvaluator::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(argIndex);

	expr_ = expr;
}

//
// VarCondEvaluator
//

void SQLContainerImpl::VarCondEvaluator::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(argIndex);

	expr_ = expr;
}

//
// ComparisonEvaluator
//

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

//
// EmptyCondEvaluator
//

void SQLContainerImpl::EmptyCondEvaluator::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(cxt);
	static_cast<void>(expr);
	static_cast<void>(argIndex);
}

//
// RowIdCondEvaluator
//

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

//
// GeneralProjector
//

void SQLContainerImpl::GeneralProjector::initialize(
		ProjectionFactoryContext &cxt, const Projection *proj) {
	static_cast<void>(cxt);

	proj_ = proj;
}

bool SQLContainerImpl::GeneralProjector::update(OpContext &cxt) {
	proj_->updateProjectionContext(cxt);
	return true;
}

//
// CountProjector
//

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

//
// CursorRef
//

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

//
// ScannerHandlerBase::HandlerVariantUtils
//

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

//
// ScannerHandlerBase::VariantTraits
//

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

//
// ScannerHandlerBase::VariantAt
//

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

//
// ScannerHandlerFactory
//

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

//
// BaseRegistrar
//

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

//
// BaseRegistrar::ScannerVariantTraits
//

template<typename V>
SQLContainerImpl::BaseRegistrar::ScannerVariantTraits::ObjectType&
SQLContainerImpl::BaseRegistrar::ScannerVariantTraits::create(
		ContextType &cxt, const CodeType &code) {
	V *handler = ALLOC_NEW(cxt.first->getAllocator()) V(*cxt.first, code);
	cxt.second->getEntry<V::ROW_ARRAY_TYPE>() =
			ContainerRowScanner::createHandlerEntry(*handler);
	return *cxt.second;
}

//
// DefaultRegistrar
//

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

//
// SearchResult
//

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

inline void SQLContainerImpl::SearchResult::clear() {
	normalEntry_.clearEntry();
	mvccEntry_.clearEntry();
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

inline SQLContainerImpl::SearchResult::OIdList&
SQLContainerImpl::SearchResult::normal() {
	return normalEntry_.getOIdList(false);
}

inline SQLContainerImpl::SearchResult::OIdList&
SQLContainerImpl::SearchResult::normalRa() {
	return normalEntry_.getOIdList(true);
}

inline SQLContainerImpl::SearchResult::OIdList&
SQLContainerImpl::SearchResult::mvcc() {
	return mvccEntry_.getOIdList(false);
}

inline SQLContainerImpl::SearchResult::OIdList&
SQLContainerImpl::SearchResult::mvccRa() {
	return mvccEntry_.getOIdList(true);
}

//
// SearchResult::Entry
//

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

inline void SQLContainerImpl::SearchResult::Entry::clearEntry() {
	rowOIdList_.clear();
	rowArrayOIdList_.clear();
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

//
// ExtendedSuspendLimitter
//

SQLContainerImpl::ExtendedSuspendLimitter::ExtendedSuspendLimitter(
		util::Stopwatch &watch, uint32_t partialExecMaxMillis,
		const std::pair<uint64_t, uint64_t> &partialExecSizeRange) :
		watch_(watch),
		partialExecMaxMillis_(partialExecMaxMillis),
		partialExecSizeRange_(partialExecSizeRange) {
}

SQLContainerImpl::ExtendedSuspendLimitter::~ExtendedSuspendLimitter() {
}

bool SQLContainerImpl::ExtendedSuspendLimitter::isLimitReachable(
		ResultSize &nextLimit) {
	const uint64_t max = partialExecSizeRange_.second * 10;

	const uint64_t baseStep = std::min(std::min(
			partialExecSizeRange_.first, partialExecSizeRange_.second), max);
	const uint64_t step =
			std::min(std::max(nextLimit + baseStep, nextLimit), max) - nextLimit;
	if (step <= 0) {
		return true;
	}

	if (watch_.elapsedMillis() >= partialExecMaxMillis_) {
		return true;
	}

	nextLimit += step;
	return false;
}

//
// IndexConditionUpdator
//

SQLContainerImpl::IndexConditionUpdator::IndexConditionUpdator(
		TransactionContext &txn, SQLValues::VarContext &varCxt) :
		txn_(txn),
		alloc_(txn.getDefaultAllocator()),
		valueCxt_(SQLValues::ValueContext::ofVarContext(varCxt, &alloc_)),
		subPos_(0) {
}

SQLContainerImpl::BtreeConditionUpdator*
SQLContainerImpl::IndexConditionUpdator::prepare(
		BaseContainer &container, SQLValues::ValueSetHolder &valuesHolder,
		const SQLExprs::IndexSelector *selector,
		const SQLExprs::IndexConditionList *condList,
		const util::Vector<SQLValues::TupleColumn> *columnList,
		TupleListReader *reader, size_t pos, size_t subPos, size_t &nextPos) {
	assert(condList != NULL);
	subPos_ = pos;
	nextPos = pos + SQLExprs::IndexSelector::nextCompositeConditionDistance(
			condList->begin() + pos, condList->end());

	if (ByReader::isAcceptable(*condList, pos)) {
		if (columnList == NULL || reader == NULL) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		return &ByReader::create(
				txn_, container, valuesHolder, selector, *condList,
				*columnList, *reader, pos, nextPos, byReader_);
	}

	if (ByValue::isAcceptable(*condList, pos)) {
		if (byValue_.get() == NULL) {
			byValue_ = ALLOC_UNIQUE(alloc_, ByValue, txn_, subPos_);
		}
		byValue_->assign(container, condList, pos, subPos, nextPos);
		return byValue_.get();
	}

	return NULL;
}

size_t SQLContainerImpl::IndexConditionUpdator::getSubPosition() {
	return subPos_;
}

//
// IndexConditionUpdator::ByValue
//

SQLContainerImpl::IndexConditionUpdator::ByValue::ByValue(
		TransactionContext &txn, size_t &subPos) :
		txn_(txn),
		alloc_(txn.getDefaultAllocator()),
		condList_(NULL),
		columnTypeList_(alloc_),
		termCondList_(alloc_),
		valuePool_(alloc_),
		subPos_(subPos),
		endPos_(0){
}

SQLContainerImpl::IndexConditionUpdator::ByValue::~ByValue() {
}

bool SQLContainerImpl::IndexConditionUpdator::ByValue::isAcceptable(
		const SQLExprs::IndexConditionList &condList, size_t pos) {
	assert(pos < condList.size());

	const SQLExprs::IndexCondition &topCond = condList[pos];
	const bool bulkTop = topCond.isBulkTop();

	const size_t bulkDistance = (bulkTop ?
			SQLExprs::IndexSelector::nextBulkConditionDistance(
					condList.begin() + pos, condList.end()) :
			SQLExprs::IndexSelector::nextCompositeConditionDistance(
					condList.begin() + pos, condList.end()));

	for (size_t i = 0; i < bulkDistance; i++) {
		const SQLExprs::IndexCondition &cond = condList[pos + i];

		if (!cond.isBinded()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
	}

	return (bulkTop && topCond.bulkOrCount_ > 1);
}

void SQLContainerImpl::IndexConditionUpdator::ByValue::assign(
		BaseContainer &container, const SQLExprs::IndexConditionList *condList,
		size_t pos, size_t subPos, size_t &nextPos) {
	assert(condList != NULL);
	assert(isAcceptable(*condList, pos));

	condList_ = condList;
	endPos_ = pos + SQLExprs::IndexSelector::nextBulkConditionDistance(
			condList->begin() + pos, condList->end());

	const bool atBegin = (subPos <= pos);
	if (pos < subPos && subPos <= endPos_) {
		subPos_ = subPos;
	}
	else {
		subPos_ = pos;
	}

	const size_t singleEndPos =
			pos + SQLExprs::IndexSelector::nextCompositeConditionDistance(
					condList->begin() + pos, condList->end());
	CursorImpl::setUpConditionColumnTypeList(
			container,
			condList->begin() + pos,
			condList->begin() + singleEndPos,
			columnTypeList_);
	assert(columnTypeList_.size() == (singleEndPos - pos));

	if (atBegin) {
		reset();
	}

	if (subPos_ >= endPos_) {
		assert(subPos_ <= endPos_);
		close();
	}
	nextPos = endPos_;
}

void SQLContainerImpl::IndexConditionUpdator::ByValue::update(
		BtreeSearchContext &sc) {
	if (subPos_ >= endPos_) {
		return;
	}
	assert(subPos_ + columnTypeList_.size() <= endPos_);

	valuePool_.clear();
	termCondList_.clear();

	util::Vector<ContainerColumnType>::iterator typeIt = columnTypeList_.begin();

	SQLExprs::IndexConditionList::const_iterator it = condList_->begin() + subPos_;
	SQLExprs::IndexConditionList::const_iterator end = it + columnTypeList_.size();
	for (; it != end; ++it) {
		termCondList_.push_back(CursorImpl::makeTermCondition(
				*typeIt, *typeIt,
				DSExpression::EQ, it->column_, NULL, 0));
		++typeIt;
	}

	sc.setKeyConditions(&termCondList_[0], termCondList_.size());
	updateKeySet();
}

void SQLContainerImpl::IndexConditionUpdator::ByValue::next() {
	assert(subPos_ < endPos_);
	subPos_ += columnTypeList_.size();

	if (subPos_ >= endPos_) {
		assert(subPos_ <= endPos_);
		close();
		return;
	}

	updateKeySet();
}

inline void SQLContainerImpl::IndexConditionUpdator::ByValue::updateKeySet() {
	assert(subPos_ + columnTypeList_.size() <= endPos_);
	valuePool_.clear();

	BtreeSearchContext *sc = getActiveSearchContext();
	assert(sc != NULL);

	size_t keyPos = 0;
	SQLExprs::IndexConditionList::const_iterator it = condList_->begin() + subPos_;
	SQLExprs::IndexConditionList::const_iterator end = it + columnTypeList_.size();
	for (; it != end; ++it) {
		Value &value = ContainerValueUtils::toContainerValue(
				valuePool_, it->valuePair_.first);
		sc->setKeyConditionValue(keyPos, &value);
		++keyPos;
	}
}

//
// IndexConditionUpdator::ByReader
//

SQLContainerImpl::IndexConditionUpdator::ByReader::~ByReader() {
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::isAcceptable(
		const SQLExprs::IndexConditionList &condList, size_t pos) {
	assert(pos < condList.size());
	const size_t distance =
			SQLExprs::IndexSelector::nextCompositeConditionDistance(
					condList.begin() + pos, condList.end());

	bool drivingSome = false;
	for (size_t i = 0; i < distance; i++) {
		const SQLExprs::IndexCondition &cond = condList[pos + i];

		if (!cond.isBinded()) {
			drivingSome = true;
		}
	}
	return drivingSome;
}

SQLContainerImpl::BtreeConditionUpdator&
SQLContainerImpl::IndexConditionUpdator::ByReader::create(
		TransactionContext &txn, BaseContainer &container,
		SQLValues::ValueSetHolder &valuesHolder,
		const SQLExprs::IndexSelector *selector,
		const SQLExprs::IndexConditionList &condList,
		const util::Vector<SQLValues::TupleColumn> &columnList,
		TupleListReader &reader, size_t pos, size_t &nextPos,
		util::AllocUniquePtr<ByReader> &updator) {
	assert(isAcceptable(condList, pos));
	assert(selector != NULL);

	Data data(txn, valuesHolder, selector, reader);
	if (updator.get() != NULL) {
		swapElements(updator->data_, data);
		clearElements(data);
		updator.reset();
	}

	bool somePromotable;
	const size_t endPos = setUpEntries(
			container, condList, columnList, pos, data.columnTypeList_,
			data.entryList_, somePromotable);

	generateTypedUpdator(
			somePromotable, isComposite(data.entryList_),
			getKeyType(data.columnTypeList_.front()), data, updator);

	if (!updator->updateKeySet()) {
		updator->close();
	}
	updator->data_.bindable_ = true;

	nextPos = endPos;
	return *updator;
}

void SQLContainerImpl::IndexConditionUpdator::ByReader::update(
		BtreeSearchContext &sc) {
	const bool withValues = false;
	applyKeyConditions(sc, withValues, NULL);

	if (!updateKeySet()) {
		close();
	}
}

SQLContainerImpl::IndexConditionUpdator::ByReader::ByReader(Data &src) :
		data_(src.txn_, src.valuesHolder_, &src.selector_, src.reader_) {
	swapElements(src, data_);
}

template<typename SomePromo, typename Compo, typename T>
inline void SQLContainerImpl::IndexConditionUpdator::ByReader::nextAt() {
	TupleListReader &reader = data_.reader_;
	assert(reader.exists());

	reader.next();
	if (!updateKeySetAt<SomePromo, Compo, T>()) {
		close();
	}
}

void SQLContainerImpl::IndexConditionUpdator::ByReader::generateTypedUpdator(
		bool somePromotable, bool composite, TupleColumnType frontKeyType,
		Data &data, util::AllocUniquePtr<ByReader> &updator) {
	typedef util::TrueType True;
	typedef util::FalseType False;

	SQLValues::TypeSwitcher switcher(frontKeyType);
	if (somePromotable) {
		if (composite) {
			generateTypedUpdatorAt<True, True, void>(data, updator);
		}
		else {
			generateTypedUpdatorAt<True, False, void>(data, updator);
		}
	}
	else {
		if (composite) {
			Generator<True> generator;
			switcher(generator, data, updator);
		}
		else {
			Generator<False> generator;
			switcher(generator, data, updator);
		}
	}
}

template<typename SomePromo, typename Compo, typename T>
void SQLContainerImpl::IndexConditionUpdator::ByReader::generateTypedUpdatorAt(
		Data &data, util::AllocUniquePtr<ByReader> &updator) {
	typedef Typed<SomePromo, Compo, T> TypedUpdator;
	updator = util::AllocUniquePtr<ByReader>::of(
			ALLOC_NEW(data.alloc_) TypedUpdator(data), data.alloc_);
}

SQLContainerImpl::IndexConditionUpdator::ByReader::KeyUpdatorFunc
SQLContainerImpl::IndexConditionUpdator::ByReader::generateKeyUpdator(
		bool promotable, TupleColumnType keyType) {
	typedef util::TrueType True;
	typedef util::FalseType False;

	SQLValues::TypeSwitcher switcher(keyType);
	if (promotable) {
		SubGenerator<True> generator;
		return switcher(generator);
	}
	else {
		SubGenerator<False> generator;
		return switcher(generator);
	}
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::updateKeySet() {
	typedef util::TrueType SomePromo;
	if (isComposite(data_.entryList_)) {
		return updateKeySetAt<SomePromo, util::TrueType, void>();
	}
	else {
		return updateKeySetAt<SomePromo, util::FalseType, void>();
	}
}

template<typename SomePromo, typename Compo, typename T>
inline bool SQLContainerImpl::IndexConditionUpdator::ByReader::updateKeySetAt() {
	TupleListReader &reader = data_.reader_;

	for (;;) {
		if (!reader.exists()) {
			return false;
		}

		const bool updated = updateCurrentKeySet<SomePromo, Compo, T>();

		if (SomePromo::VALUE && !updated) {
			reader.next();
			continue;
		}

		return true;
	}
}

template<typename SomePromo, typename Compo, typename T>
inline
bool SQLContainerImpl::IndexConditionUpdator::ByReader::updateCurrentKeySet() {
	UTIL_STATIC_ASSERT((!SomePromo::VALUE == !util::IsSame<T, void>::VALUE));

	Entry &entry = data_.entryList_.front();

	if (SomePromo::VALUE) {
		data_.valuesHolder_.reset();
		if (!entry.keyUpdatorFunc_(*this, entry)) {
			return false;
		}
	}
	else {
		updateKey<util::FalseType, T>(entry, SomePromo());
	}

	if (Compo::VALUE) {
		updateFollowingKeySet<SomePromo>();
	}

	return true;
}

template<typename SomePromo>
void
SQLContainerImpl::IndexConditionUpdator::ByReader::updateFollowingKeySet() {
	const util::Vector<Entry>::iterator begin = data_.entryList_.begin();
	util::Vector<Entry>::iterator it = begin;

	++it;
	assert(it != data_.entryList_.end());
	do {
		const bool updated = it->keyUpdatorFunc_(*this, *it);
		if (SomePromo::VALUE && !updated) {
			break;
		}
	}
	while (++it != data_.entryList_.end());

	if (SomePromo::VALUE && data_.bindable_) {
		BtreeSearchContext *sc = getActiveSearchContext();
		assert(sc != NULL);

		const bool withValues = true;
		const size_t keyCount = static_cast<size_t>(it - begin);
		applyKeyConditions(*sc, withValues, &keyCount);
	}
}

template<typename Promo, typename T>
bool SQLContainerImpl::IndexConditionUpdator::ByReader::executeKeyUpdator(
		ByReader &updator, Entry &entry) {
	return updator.updateKey<Promo, T>(entry, util::FalseType());
}

template<typename, typename T>
bool SQLContainerImpl::IndexConditionUpdator::ByReader::updateKey(
		Entry&, const util::TrueType&) {
	UTIL_STATIC_ASSERT((util::IsSame<T, void>::VALUE));
	assert(false);
	return false;
}

template<typename Promo, typename T>
inline bool SQLContainerImpl::IndexConditionUpdator::ByReader::updateKey(
		Entry &entry, const util::FalseType&) {
	UTIL_STATIC_ASSERT((!util::IsSame<T, void>::VALUE));

	if (!readKey<T>(entry, Promo())) {
		return false;
	}

	if (!SQLValues::TypeUtils::Traits<T::COLUMN_TYPE>::FOR_NORMAL_FIXED &&
			data_.bindable_) {
		BtreeSearchContext *sc = getActiveSearchContext();
		assert(sc != NULL);

		TupleValue &key = getKeyRef(entry);
		sc->setKeyConditionValueData(
				entry.keyPos_,
				SQLValues::ValueUtils::getData(key),
				static_cast<uint32_t>(
						SQLValues::ValueUtils::getDataSize(key)));
	}

	return true;
}

template<typename>
bool SQLContainerImpl::IndexConditionUpdator::ByReader::readKey(
		Entry &entry, const util::TrueType&) {
	TupleListReader &reader = data_.reader_;
	assert(reader.exists());

	const SQLExprs::IndexCondition &subCond = entry.subCond_;
	if (!findInputColumnPos(subCond, NULL)) {
		return true;
	}

	SQLExprs::IndexCondition &localSubCond = entry.localSubCond_;

	const TupleValue &baseValue = SQLValues::ValueUtils::readCurrentAny(
			reader, entry.inColumn_);
	assert(!SQLValues::ValueUtils::isNull(baseValue));

	localSubCond = subCond;
	return data_.selector_.bindCondition(
			localSubCond, baseValue, TupleValue(), data_.valuesHolder_);
}

template<typename T>
inline bool SQLContainerImpl::IndexConditionUpdator::ByReader::readKey(
		Entry &entry, const util::FalseType&) {
	TupleListReader &reader = data_.reader_;
	assert(reader.exists());

	getKeyRef(entry) = SQLValues::ValueUtils::toAnyByValue<T::COLUMN_TYPE>(
			SQLValues::ValueUtils::readCurrentValue<T>(
					reader, entry.inColumn_));
	return true;
}

void SQLContainerImpl::IndexConditionUpdator::ByReader::applyKeyConditions(
		BtreeSearchContext &sc, bool withValues, const size_t *keyCount) {
	util::Vector<Entry> &entryList = data_.entryList_;
	const size_t targetKeyCount =
			(keyCount == NULL ? entryList.size() : *keyCount);
	assert(targetKeyCount <= entryList.size());

	const util::Vector<ContainerColumnType> &columnTypeList =
			data_.columnTypeList_;
	assert(entryList.size() <= columnTypeList.size());

	util::Vector<TermCondition> &termCondList = data_.termCondList_;
	termCondList.clear();

	util::Vector<ContainerColumnType>::const_iterator typeIt =
			columnTypeList.begin();

	util::Vector<Entry>::iterator it = entryList.begin();
	util::Vector<Entry>::iterator end = it + targetKeyCount;
	for (; it != end; ++it) {
		uint32_t keySize;
		const void *keyData = prepareKeyData(withValues, *it, keySize);

		termCondList.push_back(CursorImpl::makeTermCondition(
				*typeIt, *typeIt,
				DSExpression::EQ, it->subCond_.column_, keyData, keySize));
		++typeIt;
	}

	sc.setKeyConditions(&termCondList[0], targetKeyCount);
}

const void* SQLContainerImpl::IndexConditionUpdator::ByReader::prepareKeyData(
		bool withValues, Entry &entry, uint32_t &keySize) {
	keySize = 0;

	const TupleColumnType type =
			SQLValues::TypeUtils::toNonNullable(entry.keyType_);
	if (!withValues && !SQLValues::TypeUtils::isNormalFixed(type)) {
		return NULL;
	}

	TupleValue &key = getKeyRef(entry);

	if (!withValues) {
		SQLValues::ValueContext valueCxt(
				SQLValues::ValueContext::ofAllocator(data_.alloc_));
		key = SQLValues::ValueUtils::createEmptyValue(valueCxt, type);
	}

	keySize = static_cast<uint32_t>(SQLValues::ValueUtils::getDataSize(key));
	return SQLValues::ValueUtils::getData(key);
}

void SQLContainerImpl::IndexConditionUpdator::ByReader::swapElements(
		Data &src, Data &dest) {
	src.entryList_.swap(dest.entryList_);
	src.columnTypeList_.swap(dest.columnTypeList_);
	src.termCondList_.swap(dest.termCondList_);
}

void SQLContainerImpl::IndexConditionUpdator::ByReader::clearElements(
		Data &data) {
	data.entryList_.clear();
	data.columnTypeList_.clear();
	data.termCondList_.clear();
}

size_t SQLContainerImpl::IndexConditionUpdator::ByReader::setUpEntries(
		const BaseContainer &container,
		const SQLExprs::IndexConditionList &condList,
		const util::Vector<SQLValues::TupleColumn> &columnList,
		size_t pos, util::Vector<ContainerColumnType> &columnTypeList,
		util::Vector<Entry> &entryList, bool &somePromotable) {
	somePromotable = false;

	const size_t distance =
			SQLExprs::IndexSelector::nextCompositeConditionDistance(
					condList.begin() + pos, condList.end());
	const size_t endPos = pos + distance;

	CursorImpl::setUpConditionColumnTypeList(
			container,
			condList.begin() + pos,
			condList.begin() + endPos,
			columnTypeList);

	for (size_t i = 0; i < distance; i++) {
		bool promotable;
		if (!tryAppendEntry(
				condList[pos + i], columnList, columnTypeList, entryList,
				promotable)) {
			break;
		}
		somePromotable |= promotable;
	}
	assert(!entryList.empty());
	return endPos;
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::tryAppendEntry(
		const SQLExprs::IndexCondition &cond,
		const util::Vector<SQLValues::TupleColumn> &columnList,
		const util::Vector<ContainerColumnType> &columnTypeList,
		util::Vector<Entry> &entryList, bool &promotable) {
	promotable = false;

	const size_t keyPos = entryList.size();
	if (!isAssignableKey(keyPos, cond)) {
		return false;
	}

	assert(keyPos < columnTypeList.size());
	const TupleColumnType keyType = getKeyType(columnTypeList[keyPos]);
	const SQLValues::TupleColumn &inColumn = getInputColumn(cond, columnList);

	promotable = isPromotable(keyType, inColumn);
	KeyUpdatorFunc keyUpdatorFunc = generateKeyUpdator(promotable, keyType);

	Entry entry(keyPos, keyType, cond, inColumn, keyUpdatorFunc);
	entryList.push_back(entry);

	return true;
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::isAssignableKey(
		size_t keyPos, const SQLExprs::IndexCondition &cond) {
	if (cond.opType_ != SQLType::OP_EQ || (cond.isBinded() && keyPos == 0)) {
		if (keyPos == 0) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		return false;
	}
	return true;
}

TupleColumnType SQLContainerImpl::IndexConditionUpdator::ByReader::getKeyType(
		ContainerColumnType src) {
	return ContainerValueUtils::toTupleColumnType(src, false);
}

inline TupleValue&
SQLContainerImpl::IndexConditionUpdator::ByReader::getKeyRef(Entry &entry) {
	return entry.localSubCond_.valuePair_.first;
}

SQLValues::TupleColumn
SQLContainerImpl::IndexConditionUpdator::ByReader::getInputColumn(
		const SQLExprs::IndexCondition &cond,
		const util::Vector<SQLValues::TupleColumn> &columnList) {
	uint32_t columnPos;
	if (!findInputColumnPos(cond, &columnPos)) {
		return SQLValues::TupleColumn();
	}

	assert(columnPos < columnList.size());
	return columnList[columnPos];
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::findInputColumnPos(
		const SQLExprs::IndexCondition &cond, uint32_t *columnPos) {
	if (columnPos == NULL) {
		uint32_t localColumnPos;
		return findInputColumnPos(cond, &localColumnPos);
	}

	const uint32_t emptyColumn = SQLExprs::IndexCondition::EMPTY_IN_COLUMN;
	*columnPos = emptyColumn;

	const std::pair<uint32_t, uint32_t> &pair = cond.inColumnPair_;
	assert(pair.second == emptyColumn);

	if (pair.first == emptyColumn) {
		return false;
	}

	*columnPos = pair.first;
	return true;
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::isPromotable(
		TupleColumnType keyType, const SQLValues::TupleColumn &inColumn) {
	return (keyType !=
			SQLValues::TypeUtils::toNonNullable(inColumn.type_));
}

bool SQLContainerImpl::IndexConditionUpdator::ByReader::isComposite(
		const util::Vector<Entry> &entryList) {
	return (entryList.size() > 1);
}

//
// IndexConditionUpdator::ByReader::Entry
//

SQLContainerImpl::IndexConditionUpdator::ByReader::Entry::Entry(
		size_t keyPos, TupleColumnType keyType,
		const SQLExprs::IndexCondition &subCond,
		const SQLValues::TupleColumn &inColumn, KeyUpdatorFunc func) :
		keyPos_(keyPos),
		keyType_(keyType),
		subCond_(subCond),
		localSubCond_(subCond),
		inColumn_(inColumn),
		keyUpdatorFunc_(func) {
}

//
// IndexConditionUpdator::ByReader::Data
//

SQLContainerImpl::IndexConditionUpdator::ByReader::Data::Data(
		TransactionContext &txn, SQLValues::ValueSetHolder &valuesHolder,
		const SQLExprs::IndexSelector *selector, TupleListReader &reader) :
		txn_(txn),
		alloc_(txn.getDefaultAllocator()),
		valuesHolder_(valuesHolder),
		selector_(*selector),
		entryList_(alloc_),
		columnTypeList_(alloc_),
		termCondList_(alloc_),
		reader_(reader),
		bindable_(false) {
	assert(selector != NULL);
}

//
// IndexSearchContext
//

SQLContainerImpl::IndexSearchContext::IndexSearchContext(
		TransactionContext &txn, SQLValues::VarContext &varCxt,
		SQLOps::OpAllocatorManager &allocManager,
		util::StackAllocator::BaseAllocator &baseAlloc,
		util::Stopwatch &watch, const uint32_t *partialExecMaxMillis,
		const std::pair<uint64_t, uint64_t> &partialExecSizeRange) :
		alloc_(txn.getDefaultAllocator()),
		settingAllocRef_(allocManager, baseAlloc),
		condList_(NULL),
		columnList_(alloc_),
		columnTypeList_(alloc_),
		indexInfoList_(alloc_),
		scPool_(alloc_, settingAllocRef_.get()),
		tree_(alloc_, ColumnIdList(alloc_)),
		indexConditionUpdator_(txn, varCxt),
		result_(alloc_) {
	tree_.setPool(&scPool_);

	if (partialExecMaxMillis != NULL) {
		suspendLimitter_ = UTIL_MAKE_LOCAL_UNIQUE(
				suspendLimitter_, ExtendedSuspendLimitter,
				watch, *partialExecMaxMillis, partialExecSizeRange);
		tree_.getSuspendLimitter() =
				BaseIndex::SuspendLimitter(suspendLimitter_.get());
	}
}

void SQLContainerImpl::IndexSearchContext::clear() {
	columnList_.clear();
	columnTypeList_.clear();
	indexInfoList_.clear();
	tree_.clear();
	result_.clear();
}

void SQLContainerImpl::IndexSearchContext::setInfo(
		const IndexScanInfo *info) {
	condList_ = (info == NULL ? NULL : info->condList_);
}

const SQLExprs::IndexConditionList&
SQLContainerImpl::IndexSearchContext::conditionList() {
	assert(condList_ != NULL);
	return *condList_;
}

SQLContainerImpl::IndexConditionUpdator&
SQLContainerImpl::IndexSearchContext::conditionUpdator() {
	return indexConditionUpdator_;
}

SQLContainerImpl::ColumnIdList&
SQLContainerImpl::IndexSearchContext::columnList() {
	return columnList_;
}

util::Vector<SQLContainerImpl::ContainerColumnType>&
SQLContainerImpl::IndexSearchContext::columnTypeList() {
	return columnTypeList_;
}

SQLContainerImpl::IndexInfoList&
SQLContainerImpl::IndexSearchContext::indexInfoList() {
	return indexInfoList_;
}

SQLContainerImpl::BtreeSearchContext&
SQLContainerImpl::IndexSearchContext::tree() {
	return tree_;
}

SQLContainerImpl::SearchResult&
SQLContainerImpl::IndexSearchContext::result() {
	return result_;
}

//
// CursorScope
//

SQLContainerImpl::CursorScope::CursorScope(
		ExtOpContext &cxt, const TupleColumnList &inColumnList,
		CursorAccessorImpl &accessor) :
		accessor_(accessor),
		txn_(CursorAccessorImpl::prepareTransactionContext(cxt)),
		txnSvc_(CursorAccessorImpl::getTransactionService(cxt)),
		partitionGuard_(getPartitionLock(txn_, cxt)),
		dataStore_(CursorAccessorImpl::getDataStore(cxt)),
		containerAutoPtr_(
				txn_.getDefaultAllocator(),
				getContainer(
						txn_, dataStore_,
						getStoreScope(txn_, cxt, dataStore_, scope_),
						accessor_.location_,
						accessor_.containerExpired_,
						accessor_.containerType_)),
		resultSet_(NULL),
		partialExecCount_(0) {
	try {
		initialize(cxt, containerAutoPtr_.get(), inColumnList);
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

SQLContainerImpl::IndexSearchContext&
SQLContainerImpl::CursorScope::prepareIndexSearchContext(
		SQLValues::VarContext &varCxt, const IndexScanInfo *info,
		SQLOps::OpAllocatorManager &allocManager,
		util::StackAllocator::BaseAllocator &baseAlloc,
		const uint32_t *partialExecMaxMillis,
		const std::pair<uint64_t, uint64_t> &partialExecSizeRange) {
	if (findIndexSearchContext() == NULL) {
		indexSearchContext_ = UTIL_MAKE_LOCAL_UNIQUE(
				indexSearchContext_, IndexSearchContext,
				txn_, varCxt, allocManager, baseAlloc, watch_,
				partialExecMaxMillis, partialExecSizeRange);
	}
	indexSearchContext_->setInfo(info);
	return *indexSearchContext_;
}

SQLContainerImpl::IndexSearchContext*
SQLContainerImpl::CursorScope::findIndexSearchContext() {
	return indexSearchContext_.get();
}

int64_t SQLContainerImpl::CursorScope::getStartPartialExecCount() {
	return partialExecCount_;
}

uint64_t SQLContainerImpl::CursorScope::getElapsedNanos() {
	return watch_.elapsedNanos();
}

void SQLContainerImpl::CursorScope::initialize(
		ExtOpContext &cxt, BaseContainer *container,
		const TupleColumnList &inColumnList) {
	try {
		CursorAccessorImpl::checkContainer(
				txn_, container, accessor_.location_,
				accessor_.schemaVersionId_, inColumnList);

		resultSet_ =
				prepareResultSet(cxt, txn_, container, rsGuard_, accessor_);

		if (resultSet_ != NULL) {
			partialExecCount_ = resultSet_->getPartialExecuteCount();
		}

		setUpAccessor(container);
	}
	catch (...) {
		std::exception e;
		CursorAccessorImpl::handleSearchError(
				&txn_, containerAutoPtr_.get(), e);
	}
}

void SQLContainerImpl::CursorScope::setUpAccessor(BaseContainer *container) {
	watch_.start();

	accessor_.container_ = container;
	accessor_.containerExpired_ = (container == NULL);
	accessor_.resultSetId_ = findResultSetId(resultSet_);

	accessor_.lastPartitionId_ = txn_.getPartitionId();

	DataStoreV4 *dataStore =  (container == NULL ?
			NULL : &CursorAccessorImpl::resolveDataStore(*container));
	accessor_.lastRSManager_ =
			(dataStore == NULL ? NULL : dataStore->getResultSetManager());

	if (container == NULL) {
		return;
	}
	assert(!container->isInvalid());

	accessor_.containerType_ = container->getContainerType();
	accessor_.schemaVersionId_ = container->getVersionId();

	accessor_.initializeScanRange(txn_, *container);
	accessor_.initializeRowArray(txn_, *container);

	ObjectManagerV4 &objectManager =
			CursorAccessorImpl::resolveObjectManager(*container);
	accessor_.frontFieldCache_ = UTIL_MAKE_LOCAL_UNIQUE(
			accessor_.frontFieldCache_, BaseObject,
			objectManager, container->getRowAllocateStrategy());
}

void SQLContainerImpl::CursorScope::clear(bool force) {
	if (containerAutoPtr_.get() == NULL) {
		return;
	}

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
		accessor_.lastRSManager_ = NULL;
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
		TransactionContext &txn, ExtOpContext &cxt, DataStoreV4 *dataStore,
		util::LocalUniquePtr<DataStoreScope> &scope) {
	if (dataStore == NULL || !cxt.isOnTransactionService()) {
		return NULL;
	}

	ClusterService *clusterService = CursorAccessorImpl::getClusterService(cxt);

	scope = UTIL_MAKE_LOCAL_UNIQUE(
			scope, DataStoreScope,
			&txn, dataStore, clusterService);

	return scope.get();
}

BaseContainer* SQLContainerImpl::CursorScope::getContainer(
		TransactionContext &txn, DataStoreV4 *dataStore,
		const DataStoreScope *latch, const ContainerLocation &location,
		bool expired, ContainerType type) {
	if (dataStore == NULL || latch == NULL ||
			location.type_ != SQLType::TABLE_CONTAINER || expired) {
		return NULL;
	}

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

ResultSet* SQLContainerImpl::CursorScope::prepareResultSet(
		ExtOpContext &cxt, TransactionContext &txn, BaseContainer *container,
		util::LocalUniquePtr<ResultSetGuard> &rsGuard,
		const CursorAccessorImpl &accessor) {
	if (container == NULL) {
		return NULL;
	}

	DataStoreV4 &dataStore = CursorAccessorImpl::resolveDataStore(*container);

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
		resultSet = dataStore.getResultSetManager()->create(
				txn, container->getContainerId(), container->getVersionId(),
				emNow, NULL, noExpire);
		resultSet->setUpdateIdType(ResultSet::UPDATE_ID_NONE);
	}
	else {
		resultSet = dataStore.getResultSetManager()->get(txn, accessor.resultSetId_);
	}

	if (resultSet == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_RESULT_ID_INVALID, "");
	}

	rsGuard = UTIL_MAKE_LOCAL_UNIQUE(
			rsGuard, ResultSetGuard, txn, dataStore, *resultSet);

	return resultSet;
}

ResultSetId SQLContainerImpl::CursorScope::findResultSetId(
		ResultSet *resultSet) {
	if (resultSet == NULL) {
		return UNDEF_RESULTSETID;
	}
	return resultSet->getId();
}

//
// LocalCursorScope
//

SQLContainerImpl::LocalCursorScope::LocalCursorScope(
		OpContext &cxt, CursorAccessorImpl &accessor) :
		data_(accessor) {
	ExtOpContext &extCxt = cxt.getExtContext();
	data_.accessor_.startScope(extCxt, cxt.getInputColumnList(INPUT_INDEX));
	ScanSimulatorUtils::trySimulateUpdates(cxt, accessor);
}

SQLContainerImpl::LocalCursorScope::LocalCursorScope(
		ExtOpContext &extCxt, CursorAccessorImpl &accessor,
		const TupleColumnList &inColumnList) :
		data_(accessor) {
	data_.accessor_.startScope(extCxt, inColumnList);
}

SQLContainerImpl::LocalCursorScope::~LocalCursorScope() {
	assert(data_.closed_);
}

void SQLContainerImpl::LocalCursorScope::close() throw() {
	assert(!data_.closed_);
	data_.closed_ = true;
}

void SQLContainerImpl::LocalCursorScope::closeWithError(
		std::exception &e) {
	close();
	data_.accessor_.handleSearchError(e);
}

//
// LocalCursorScope::Data
//

SQLContainerImpl::LocalCursorScope::Data::Data(CursorAccessorImpl &accessor) :
		accessor_(accessor),
		closed_(false) {
}

//
// ContainerNameFormatter
//

SQLContainerImpl::ContainerNameFormatter::ContainerNameFormatter(
		TransactionContext &txn, BaseContainer &container,
		const ContainerLocation &location) :
		txn_(txn),
		container_(container),
		location_(location) {
}

std::ostream& SQLContainerImpl::ContainerNameFormatter::format(
		std::ostream &os) const throw() {
	try {
		FullContainerKey key = container_.getContainerKey(txn_);
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		util::String keyStr(alloc);
		key.toString(alloc, keyStr);
		os << keyStr;
	}
	catch (...) {
		try {
			os << "(Container name inaccessible (containerId=" << location_.id_ << "))";
		}
		catch (...) {
		}
	}
	return os;
}

std::ostream& operator<<(
		std::ostream &os,
		const SQLContainerImpl::ContainerNameFormatter &formatter) {
	return formatter.format(os);
}

//
// MetaScanHandler
//

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

//
// TupleUpdateRowIdHandler
//

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

	const uint64_t localLimit = std::max<uint64_t>(limit, 1);
	bool &initialUpdatesAccepted = shared_->initialUpdatesAccepted_;

	cxt.closeLocalWriter(index);
	bool extra = false;
	{
		uint64_t rest = localLimit;
		TupleListReader &reader = cxt.getLocalReader(index);
		for (; reader.exists(); reader.next()) {
			if (!initialUpdatesAccepted) {
				if (rest <= 0) {
					extra = true;
					break;
				}
				--rest;
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

	uint64_t extraCount = 0;
	if (extra) {
		cxt.createLocalTupleList(extraIndex);
		copyRowIdStore(cxt, index, extraIndex, columnList, &extraCount);
		cxt.closeLocalWriter(extraIndex);
	}

	cxt.closeLocalTupleList(index);
	cxt.createLocalTupleList(index);

	if (extra) {
		copyRowIdStore(cxt, extraIndex, index, columnList, NULL);
		cxt.closeLocalTupleList(extraIndex);
	}

	cxt.releaseWriterLatch(index);

	if (!initialUpdatesAccepted) {
		uint64_t &rest = shared_->restInitialUpdateCount_;
		if (extra) {
			if (rest > 0) {
				rest -= std::min(rest, localLimit);
			}
			else {
				rest = extraCount;
			}
		}
		else {
			rest = 0;
		}
		initialUpdatesAccepted = (rest <= 0);
	}

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

	SQLValues::ColumnTypeList typeList(
			baseTypeList, baseTypeList + columnCount, cxt->getAllocator());

	const uint32_t index = cxt->createLocal(&typeList);
	extraStoreIndex = cxt->createLocal(&typeList);

	const TupleList::Info &info = cxt->createLocalTupleList(index).getInfo();
	info.getColumns(columnList, columnCount);

	return index;
}

void SQLContainerImpl::TupleUpdateRowIdHandler::copyRowIdStore(
		OpContext &cxt, uint32_t srcIndex, uint32_t destIndex,
		const IdStoreColumnList &columnList, uint64_t *copiedCountRef) {
	const size_t columnCount = sizeof(columnList) / sizeof(*columnList);

	TupleListReader &reader = cxt.getLocalReader(srcIndex);
	TupleListWriter &writer = cxt.getWriter(destIndex);

	uint64_t copiedCount = 0;
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

	if (copiedCountRef != NULL) {
		*copiedCountRef = copiedCount;
	}
}

//
// TupleUpdateRowIdHandler::Shared
//

SQLContainerImpl::TupleUpdateRowIdHandler::Shared::Shared(
		const OpContext::Source &cxtSrc, RowId maxRowId) :
		refCount_(1),
		rowIdStoreIndex_(createRowIdStore(
				cxt_, cxtSrc, columnList_, extraStoreIndex_)),
		maxRowId_(maxRowId),
		restInitialUpdateCount_(0),
		updated_(false),
		initialUpdatesAccepted_(false) {
}

//
// BitUtils::DenseValues
//

template<typename V>
inline V SQLContainerImpl::BitUtils::DenseValues<V>::toHighPart(
		ValueType value) {
	return (value & ~ValueConstants::LOW_BIT_MASK);
}

template<typename V>
inline V SQLContainerImpl::BitUtils::DenseValues<V>::toLowBit(
		ValueType value) {
	const uint32_t bitSize = ValueConstants::FULL_BIT_COUNT;
	return (static_cast<ValueType>(1) << (bitSize - extraOfHighPart(value) - 1));
}

template<typename V>
inline V SQLContainerImpl::BitUtils::DenseValues<V>::extraOfHighPart(
		ValueType value) {
	return (value & ValueConstants::LOW_BIT_MASK);
}

template<typename V>
inline bool SQLContainerImpl::BitUtils::DenseValues<V>::nextValue(
		uint32_t &nextPos, ValueType highPart, ValueType lowBits,
		ValueType &value) {
	const uint32_t bitSize = ValueConstants::FULL_BIT_COUNT;
	assert(nextPos <= bitSize);
	if (nextPos >= bitSize) {
		return false;
	}

	const ValueType mask = (~static_cast<ValueType>(0) >> nextPos);
	const uint32_t basePos = util::nlz(lowBits & mask);
	if (basePos >= bitSize) {
		return false;
	}

	nextPos = basePos + 1;
	value = ((highPart & ~ValueConstants::LOW_BIT_MASK) | basePos);
	return true;
}

//
// BitUtils::TupleColumns
//

template<typename V>
inline typename SQLContainerImpl::BitUtils::template TupleColumns<V>::KeyType
SQLContainerImpl::BitUtils::TupleColumns<V>::readCurrentKey(
		TupleListReader &reader) {
	return SQLValues::ValueUtils::readCurrentValue<KeyTag>(
			reader, getKeyColumn());
}

template<typename V>
inline typename SQLContainerImpl::BitUtils::template TupleColumns<V>::ValueType
SQLContainerImpl::BitUtils::TupleColumns<V>::readCurrentValue(
		TupleListReader &reader) {
	return SQLValues::ValueUtils::readCurrentValue<ValueTag>(
			reader, getValueColumn());
}

template<typename V>
inline void SQLContainerImpl::BitUtils::TupleColumns<V>::writeKey(
		WritableTuple &tuple, const KeyType &key) {
	SQLValues::ValueUtils::writeValue<KeyTag>(
			tuple, getKeyColumn(), key);
}

template<typename V>
inline void SQLContainerImpl::BitUtils::TupleColumns<V>::writeValue(
		WritableTuple &tuple, const ValueType &value) {
	SQLValues::ValueUtils::writeValue<ValueTag>(
			tuple, getValueColumn(), value);
}

template<typename V>
inline TupleList::Column
SQLContainerImpl::BitUtils::TupleColumns<V>::getKeyColumn() {
	return Constants::COLUMN_KEY;
}

template<typename V>
inline TupleList::Column
SQLContainerImpl::BitUtils::TupleColumns<V>::getValueColumn() {
	return Constants::COLUMN_VALUE;
}

template<typename V>
inline void SQLContainerImpl::BitUtils::TupleColumns<V>::getColumnTypeList(
		SQLValues::ColumnTypeList &list) {
	list.assign(
			Constants::COLUMN_TYPES + 0,
			Constants::COLUMN_TYPES + BITS_COLUMN_COUNT);
}

template<typename V>
TupleList::Column SQLContainerImpl::BitUtils::TupleColumns<V>::resolveColumn(
		bool forKey) {
	TupleList::Info info;
	info.columnCount_ = BITS_COLUMN_COUNT;
	info.columnTypeList_ = Constants::COLUMN_TYPES;

	TupleList::Column columnList[BITS_COLUMN_COUNT];
	info.getColumns(columnList, info.columnCount_);

	return columnList[(forKey ? 0 : 1)];
}

//
// BitUtils::TupleColumns::Constants
//

template<typename V>
const TupleList::Column
SQLContainerImpl::BitUtils::TupleColumns<V>::Constants::COLUMN_KEY =
		resolveColumn(true);

template<typename V>
const TupleList::Column
SQLContainerImpl::BitUtils::TupleColumns<V>::Constants::COLUMN_VALUE =
		resolveColumn(false);

template<typename V>
const TupleColumnType
SQLContainerImpl::BitUtils::TupleColumns<V>::Constants::COLUMN_TYPES[
		SQLContainerImpl::BitUtils::TupleColumns<V>::BITS_COLUMN_COUNT] = {
	KeyTag::COLUMN_TYPE,
	ValueTag::COLUMN_TYPE
};

//
// OIdTable
//

SQLContainerImpl::OIdTable::OIdTable(const Source &source) :
		allocManager_(source.getAlloactorManager()),
		baseAlloc_(source.getBaseAllocator()),
		varAlloc_(source.getVarAllocator()),
		cxtSrc_(source.getContextSource()),
		stageSizeLimit_(source.isStageSizeLimit()),
		large_(source.isLarge()),
		suspendable_(true),
		groupMap_(varAlloc_),
		freeSlotList_(varAlloc_),
		updatesAcceptable_(false) {
}

SQLContainerImpl::OIdTable::~OIdTable() {
	clearGroups();
}

void SQLContainerImpl::OIdTable::createGroup(
		GroupId groupId, GroupId parentId, GroupMode mode) {
	if (findGroup(validateGroupId(groupId, false)) != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	Group *parent = findGroup(validateGroupId(parentId, true));
	if (parent == NULL) {
		if (parentId != GROUP_ID_ROOT) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		parent = &createGroupDirect(parentId, MODE_UNION);
	}

	Group &group = createGroupDirect(groupId, mode);
	group.parentId_ = parentId;
	parent->subIdList_.push_back(groupId);
}

void SQLContainerImpl::OIdTable::addAll(
		GroupId groupId, SearchResult &result) {
	checkAdditionWorking();

	OpContext cxt(cxtSrc_);
	Slot slot = resultToSlot(cxt, result);

	const bool withMerge = true;
	Group &group = getGroup(getAdditionGroupId(groupId));
	addSlot(cxt, group, slot, withMerge);
}

void SQLContainerImpl::OIdTable::popAll(
		SearchResult &result, uint64_t limit, uint32_t rowArraySize,
		bool withUpdatesAlways) {
	checkAdditionFinalized();

	Group &group = getGroup(GROUP_ID_ROOT);
	Slot *rootSlot = getSingleSlot(group);

	bool finished = false;
	if (rootSlot != NULL) {
		HotSlot *removalSlot = findHotSlot(GROUP_ID_DELTA_REMOVAL);

		OpContext cxt(cxtSrc_);
		finished = slotToResult(
				cxt, *rootSlot, result, limit, rowArraySize, removalSlot);
	}

	if (finished || withUpdatesAlways) {
		popUpdates(result);
	}

	Cursor &cursor = *cursor_;
	cursor.started_ = true;
	cursor.finished_ = finished;
}

void SQLContainerImpl::OIdTable::update(
		ResultSet::UpdateOperation op, OId oId) {
	checkUpdatesAcceptable();
	switch (op) {
	case ResultSet::UPDATE_OP_UPDATE_NORMAL_ROW:
		updateEntry(TYPE_ROW_NORMAL, oId);
		++modificationProfile_[IndexProfiler::MOD_UPDATE_ROW].first;
		break;
	case ResultSet::UPDATE_OP_UPDATE_MVCC_ROW:
		updateEntry(TYPE_ROW_MVCC, oId);
		++modificationProfile_[IndexProfiler::MOD_UPDATE_ROW].second;
		break;
	case ResultSet::UPDATE_OP_REMOVE_ROW:
		removeEntries(TYPE_ROW_NORMAL, oId);
		++modificationProfile_[IndexProfiler::MOD_REMOVE_ROW].first;
		break;
	case ResultSet::UPDATE_OP_REMOVE_ROW_ARRAY:
		removeEntries(TYPE_RA_NORMAL, oId);
		++modificationProfile_[IndexProfiler::MOD_REMOVE_ROW_ARRAY].first;
		break;
	case ResultSet::UPDATE_OP_REMOVE_CHUNK:
		removeEntries(TYPE_CHUNK, oId);
		++modificationProfile_[IndexProfiler::MOD_REMOVE_CHUNK].first;
		break;
	default:
		assert(false);
		break;
	}
}

bool SQLContainerImpl::OIdTable::finalizeAddition(uint64_t limit) {
	if (cursor_.get() == NULL) {
		OpContext cxt(cxtSrc_);
		uint64_t localLimit = limit;

		if (mergingGroupId_.get() == NULL) {
			checkAdditionWorking();
		}
		else {
			if (!mergeGroup(cxt, *mergingGroupId_, localLimit)) {
				return false;
			}
		}

		if (!mergeGroup(cxt, GROUP_ID_ROOT, localLimit)) {
			return false;
		}

		cursor_ = ALLOC_UNIQUE(varAlloc_, Cursor);
	}
	return true;
}

bool SQLContainerImpl::OIdTable::isUpdatesAcceptable() {
	return updatesAcceptable_;
}

void SQLContainerImpl::OIdTable::acceptUpdates() {
	checkAdditionFinalized();
	updatesAcceptable_ = true;
}

bool SQLContainerImpl::OIdTable::isCursorStarted() {
	return (cursor_.get() != NULL && cursor_->started_);
}

bool SQLContainerImpl::OIdTable::isCursorFinished() {
	return (cursor_.get() != NULL && cursor_->finished_);
}

void SQLContainerImpl::OIdTable::restartCursor() {
	checkAdditionFinalized();

	assert(cursor_.get() != NULL);
	*cursor_ = Cursor();

	Group &group = getGroup(GROUP_ID_ROOT);
	Slot *slot = getSingleSlot(group);
	if (slot != NULL) {
		resetSlotReader(*slot);
	}
}

void SQLContainerImpl::OIdTable::setSuspendable(bool suspendable) {
	suspendable_ = suspendable;
}

bool SQLContainerImpl::OIdTable::getModificationProfile(
		IndexProfiler::ModificationType modification, bool onMvcc,
		int64_t &count) const {
	const size_t ordinal = modification;
	assert(ordinal < MOD_TYPE_COUNT);

	const ModificationProfileEntry &entry = modificationProfile_[ordinal];
	count = (onMvcc ? entry.second : entry.first);
	return (count > 0);
}

//
// OIdTable - Status Operations
//

void SQLContainerImpl::OIdTable::checkAdditionWorking() {
	if (cursor_.get() != NULL || mergingGroupId_.get() != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLContainerImpl::OIdTable::checkAdditionFinalized() {
	if (cursor_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLContainerImpl::OIdTable::checkUpdatesAcceptable() {
	if (!updatesAcceptable_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

//
// OIdTable - Group Operations
//

SQLContainerImpl::OIdTable::Group* SQLContainerImpl::OIdTable::findGroup(
		GroupId groupId) {
	GroupMap::iterator it = groupMap_.find(groupId);
	if (it == groupMap_.end()) {
		return NULL;
	}
	return it->second;
}

SQLContainerImpl::OIdTable::Group& SQLContainerImpl::OIdTable::getGroup(
		GroupId groupId) {
	Group *group = findGroup(groupId);
	if (group == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *group;
}

SQLContainerImpl::OIdTable::Group&
SQLContainerImpl::OIdTable::createGroupDirect(GroupId groupId, GroupMode mode) {
	if (findGroup(groupId) != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	util::AllocUniquePtr<Group> group;
	group = ALLOC_UNIQUE(varAlloc_, Group, varAlloc_, mode);
	groupMap_.insert(std::make_pair(groupId, group.get()));
	return *group.release();
}

void SQLContainerImpl::OIdTable::clearGroups() {
	for (GroupMap::iterator it = groupMap_.begin();
			it != groupMap_.end(); ++it) {
		util::AllocUniquePtr<Group> group(it->second, varAlloc_);
	}
	groupMap_.clear();
}

SQLContainerImpl::OIdTable::GroupId
SQLContainerImpl::OIdTable::getAdditionGroupId(GroupId groupId) {
	if (groupId == GROUP_ID_ROOT) {
		return groupId;
	}

	Group &group = getGroup(groupId);
	Group &parentGroup = getGroup(group.parentId_);
	if (group.mode_ == parentGroup.mode_ ||
			parentGroup.subIdList_.size() == 1) {
		return getAdditionGroupId(group.parentId_);
	}
	return groupId;
}

SQLContainerImpl::OIdTable::GroupId
SQLContainerImpl::OIdTable::validateGroupId(GroupId groupId, bool withRoot) {
	do {
		if (groupId >= 0) {
			break;
		}
		if (withRoot && groupId == GROUP_ID_ROOT) {
			break;
		}

		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	while (false);
	return groupId;
}

//
// OIdTable - Read/Write Operations
//

SQLContainerImpl::OIdTable::Slot
SQLContainerImpl::OIdTable::resultToSlot(
		OpContext &cxt, SearchResult &result) {
	EntryType type;
	OIdList *oIdList = findSingleSearchResultOIdList(result, type);

	if (oIdList == NULL) {
		EntryList entryList(varAlloc_);
		resultToEntries(result, entryList);
		return entryListToSlot(cxt, entryList);
	}

	return oIdListToSlot(cxt, type, *oIdList);
}

SQLContainerImpl::OIdTable::Slot
SQLContainerImpl::OIdTable::oIdListToSlot(
		OpContext &cxt, EntryType type, OIdList &list) {
	Slot slot = allocateSlot(cxt);

	TupleListWriter &writer = slot.getWriter(cxt);
	if (!list.empty()) {
		std::sort(list.begin(), list.end());
		Entry entry = Coders::oIdToEntry(type, list.front());
		for (OIdList::iterator it = list.begin(); ++it != list.end();) {
			const Entry sub = Coders::oIdToEntry(type, *it);
			if (Coders::tryMergeEntry(sub, entry)) {
				continue;
			}

			writeEntry(writer, entry);
			entry = sub;
		}
		writeEntry(writer, entry);
	}
	slot.finalizeWriter(cxt);
	return slot;
}

SQLContainerImpl::OIdTable::Slot
SQLContainerImpl::OIdTable::entryListToSlot(
		OpContext &cxt, EntryList &list) {
	Slot slot = allocateSlot(cxt);

	TupleListWriter &writer = slot.getWriter(cxt);
	if (!list.empty()) {
		std::sort(list.begin(), list.end());
		Entry entry = list.front();
		for (EntryList::iterator it = list.begin(); ++it != list.end();) {
			if (Coders::tryMergeEntry(*it, entry)) {
				continue;
			}

			writeEntry(writer, entry);
			entry = *it;
		}
		writeEntry(writer, entry);
	}
	slot.finalizeWriter(cxt);
	return slot;
}

void SQLContainerImpl::OIdTable::resultToEntries(
		SearchResult &result, EntryList &dest) {
	for (size_t i = 0; i < END_TYPE; i++) {
		const EntryType type = static_cast<EntryType>(i);
		if (type == TYPE_CHUNK) {
			continue;
		}
		OIdList &src = getSearchResultOIdList(type, result);
		oIdListToEntries(type, src, dest);
	}
}

void SQLContainerImpl::OIdTable::oIdListToEntries(
		EntryType type, OIdList &src, EntryList &dest) {
	for (OIdList::iterator it = src.begin(); it != src.end(); ++it) {
		dest.push_back(Coders::oIdToEntry(type, *it));
	}
}

bool SQLContainerImpl::OIdTable::slotToResult(
		OpContext &cxt, Slot &slot, SearchResult &result, uint64_t limit,
		uint32_t rowArraySize, HotSlot *removalSlot) {
	uint64_t lastChunkBits = ~static_cast<uint64_t>(0);
	uint64_t lastRaBits = ~static_cast<uint64_t>(0);
	EntryType lastType = END_TYPE;

	TupleListReader &reader = slot.getReader(cxt);
	for (; reader.exists(); reader.next()) {
		Entry entry = readEntry(reader);
		const EntryType type = Coders::typeOfKey(entry.first);

		if (removalSlot != NULL && checkRemoved(type, entry, *removalSlot)) {
			continue;
		}

		const uint64_t chunkBits = Coders::chunkBitsOfKey(large_, entry.first);
		const uint64_t raBits = Coders::raBitsOfKey(entry.first);
		if (chunkBits != lastChunkBits) {
			if (isLimitReached(result, limit, rowArraySize)) {
				return false;
			}
			lastChunkBits = chunkBits;
		}
		else if (raBits == lastRaBits) {
			if (type != lastType) {
				continue;
			}
		}
		else {
			lastRaBits = raBits;
			lastType = type;
		}
		entryToResult(type, entry, result);
	}

	return true;
}

void SQLContainerImpl::OIdTable::resetSlotReader(Slot &slot) {
	OpContext cxt(cxtSrc_);
	slot.resetReaderPosition(cxt);
}

bool SQLContainerImpl::OIdTable::checkRemoved(
		EntryType type, Entry &entry, HotSlot &removalSlot) {
	if (checkRemovedSub(TYPE_CHUNK, entry, removalSlot)) {
		return true;
	}

	if (checkRemovedSub(TYPE_RA_NORMAL, entry, removalSlot)) {
		return true;
	}

	if (!isRowArrayEntryType(type) &&
			checkRemovedSub(TYPE_ROW_NORMAL, entry, removalSlot)) {
		return true;
	}

	return false;
}

bool SQLContainerImpl::OIdTable::checkRemovedSub(
		EntryType type, Entry &entry, HotSlot &removalSlot) {
	EntryMap *map = findEntryMap(type, removalSlot);
	if (map == NULL) {
		return false;
	}

	const uint64_t key = entry.first;
	if (type == TYPE_ROW_NORMAL) {
		EntryMap::iterator it = map->find(key);
		if (it == map->end()) {
			return false;
		}
		entry.second &= ~(it->second);
		return (entry.second == 0);
	}

	const uint64_t mask = Coders::chunkOrRaMaskOfKey(type, large_);
	const uint64_t maskedKey = (key & mask);

	EntryMap::iterator it = map->lower_bound(maskedKey);
	return (it != map->end() && (it->first & mask) == maskedKey);
}

bool SQLContainerImpl::OIdTable::isLimitReached(
		const SearchResult &result, uint64_t limit, uint32_t rowArraySize) {
	if (!suspendable_) {
		return false;
	}

	const uint64_t estimatedRowCount =
			(getSearchResultOIdList(TYPE_RA_NORMAL, result).size() +
			getSearchResultOIdList(TYPE_RA_MVCC, result).size()) *
					rowArraySize +
			(getSearchResultOIdList(TYPE_ROW_NORMAL, result).size() +
			getSearchResultOIdList(TYPE_ROW_MVCC, result).size());
	return (estimatedRowCount >= std::max<uint64_t>(limit, 1));
}

void SQLContainerImpl::OIdTable::entryToResult(
		EntryType type, const Entry &entry, SearchResult &result) {
	OIdList &oIdList = getSearchResultOIdList(type, result);
	OId oId;
	for (uint32_t pos = 0; Coders::nextOId(pos, entry, oId);) {
		oIdList.push_back(oId);
	}
}

inline SQLContainerImpl::OIdTable::Entry
SQLContainerImpl::OIdTable::readEntry(TupleListReader &reader) {
	return Entry(readKey(reader), readPositionBits(reader));
}

inline void SQLContainerImpl::OIdTable::writeEntry(
		TupleListWriter &writer, const Entry &entry) {
	writer.next();
	WritableTuple tuple = writer.get();

	BitTupleColumns::writeKey(
			tuple, static_cast<BitTupleColumns::KeyType>(entry.first));
	BitTupleColumns::writeValue(
			tuple, static_cast<BitTupleColumns::ValueType>(entry.second));
}

inline uint64_t SQLContainerImpl::OIdTable::readKey(TupleListReader &reader) {
	assert(reader.exists());
	return static_cast<uint64_t>(BitTupleColumns::readCurrentKey(reader));
}

inline uint64_t SQLContainerImpl::OIdTable::readPositionBits(
		TupleListReader &reader) {
	assert(reader.exists());
	return static_cast<uint64_t>(BitTupleColumns::readCurrentValue(reader));
}

//
// OIdTable - Merge Operations
//

bool SQLContainerImpl::OIdTable::mergeGroup(
		OpContext &cxt, GroupId groupId, uint64_t &limit) {
	mergingGroupId_ = UTIL_MAKE_LOCAL_UNIQUE(
			mergingGroupId_, GroupId, groupId);

	Group &group = getGroup(groupId);

	if (group.mergingSlot_.get() == NULL) {
		GroupIdList &idList = group.subIdList_;
		for (GroupIdList::iterator it = idList.begin();
				it != idList.end() ; ++it) {
			if (!mergeGroup(cxt, *it, limit)) {
				return false;
			}

			Slot *slot = getSingleSlot(getGroup(*it));
			if (slot != NULL) {
				const bool withMerge = false;
				addSlot(cxt, group, *slot, withMerge);
			}
		}
	}

	const bool full = true;
	if (!mergeSlot(cxt, group, full, &limit)) {
		return false;
	}
	mergingGroupId_.reset();
	return true;
}

bool SQLContainerImpl::OIdTable::mergeSlot(
		OpContext &cxt, Group &group, bool full, uint64_t *limit) {
	SlotStageList &stageList = group.stageList_;

	size_t depth = 0;
	for (;;) {
		Slot *dest = updateMergingStage(cxt, group, full, &depth);
		if (dest == NULL) {
			break;
		}

		if (!mergeStageSlot(
				cxt, group.mode_, stageList[depth], *dest, limit)) {
			return false;
		}

		while (stageList.size() <= depth + 1) {
			stageList.push_back(SlotList(varAlloc_));
		}
		stageList[depth + 1].push_back(*dest);

		updateMergingStage(cxt, group, full, NULL);
	}

	return true;
}

SQLContainerImpl::OIdTable::Slot*
SQLContainerImpl::OIdTable::updateMergingStage(
		OpContext &cxt, Group &group, bool full, size_t *depth) {
	util::LocalUniquePtr<uint64_t> &mergingDepth = group.mergingStageDepth_;
	util::AllocUniquePtr<Slot> &mergingSlot = group.mergingSlot_;

	if (depth == NULL) {
		mergingDepth.reset();
		mergingSlot.reset();
		return NULL;
	}

	if (group.mergingStageDepth_.get() != NULL) {
		Slot *slot = group.mergingSlot_.get();
		assert(slot != NULL);
		return slot;
	}

	SlotStageList &stageList = group.stageList_;
	const size_t repeat = (*depth > 0 ? 2 : 1);
	for (size_t i = 0; i < repeat; i++) {
		for (; *depth < stageList.size(); ++(*depth)) {
			SlotList &slotList = stageList[*depth];
			if (!full) {
				if (*depth > 0) {
					return NULL;
				}
				else if (slotList.size() < stageSizeLimit_) {
					return NULL;
				}
			}

			if (slotList.empty()) {
				continue;
			}
			else if (slotList.size() > 1) {
				mergingDepth = UTIL_MAKE_LOCAL_UNIQUE(
						mergingDepth, uint64_t, *depth);
				mergingSlot = ALLOC_UNIQUE(varAlloc_, Slot, allocateSlot(cxt));
				return mergingSlot.get();
			}

			if (full && *depth + 1 < stageList.size()) {
				stageList[*depth + 1].push_back(slotList.front());
				slotList.clear();
			}
		}
	}
	return NULL;
}

bool SQLContainerImpl::OIdTable::mergeStageSlot(
		OpContext &cxt, GroupMode mode, SlotList &slotList, Slot &dest,
		uint64_t *limit) {
	if (limit != NULL && !suspendable_) {
		return mergeStageSlot(cxt, mode, slotList, dest, NULL);
	}

	typedef util::AllocVector<MergeElement> MergeElementList;
	typedef util::StdAllocator<void, void> Alloc;
	typedef SQLAlgorithmUtils::HeapQueue<
			MergeElementRef, MergeElementGreater, Alloc> SlotHeapQueue;

	size_t targetCount = 0;
	size_t mergingCount = 0;
	MergeElementList elemList(varAlloc_);
	for (SlotList::iterator it = slotList.begin(); it != slotList.end(); ++it) {
		TupleListReader &reader = it->getReader(cxt);
		++targetCount;
		if (reader.exists()) {
			elemList.push_back(MergeElement(reader));
			if (++mergingCount >= stageSizeLimit_ &&
					mergingCount > 1) {
				break;
			}
		}
	}

	SlotHeapQueue queue(MergeElementGreater(), varAlloc_);
	for (MergeElementList::iterator it = elemList.begin();
			it != elemList.end(); ++it) {
		const size_t ordinal = static_cast<size_t>(it - elemList.begin());
		queue.push(SlotHeapQueue::Element(MergeElementRef(*it), ordinal));
	}

	MergeContext mergeCxt(dest.getWriter(cxt), mode, limit);
	bool finished;
	if (mode == MODE_UNION) {
		MergeAction<MODE_UNION> action(mergeCxt);
		finished = queue.mergeOrderedUnique(action);
	}
	else {
		assert(mode == MODE_INTERSECT);
		MergeAction<MODE_INTERSECT> action(mergeCxt);
		if (targetCount == mergingCount) {
			finished = queue.mergeOrderedUnique(action);
		}
		else {
			finished = true;
		}
	}

	if (finished) {
		dest.finalizeWriter(cxt);
		for (size_t i = 0; i < targetCount; i++) {
			releaseSlot(cxt, slotList[i]);
		}
		slotList.erase(slotList.begin(), slotList.begin() + targetCount);
	}

	if (limit != NULL) {
		*limit = mergeCxt.limit_;
	}
	return finished;
}

//
// OIdTable - Slot Operations
//

SQLContainerImpl::OIdTable::Slot*
SQLContainerImpl::OIdTable::getSingleSlot(Group &group) {
	SlotStageList &stageList = group.stageList_;
	if (stageList.empty()) {
		return NULL;
	}

	SlotList &slotList = stageList.back();
	if (slotList.empty()) {
		return NULL;
	}

	assert(slotList.size() == 1);
	return &slotList.front();
}

void SQLContainerImpl::OIdTable::addSlot(
		OpContext &cxt, Group &group, Slot &slot, bool withMerge) {
	SlotStageList &stageList = group.stageList_;
	if (stageList.empty()) {
		stageList.push_back(SlotList(varAlloc_));
	}

	SlotList &slotList = stageList.front();
	slotList.push_back(slot);

	if (withMerge) {
		const bool full = false;
		mergeSlot(cxt, group, full, NULL);
	}
}

SQLContainerImpl::OIdTable::Slot
SQLContainerImpl::OIdTable::allocateSlot(OpContext &cxt) {
	Slot slot = Slot::invalidSlot();

	if (freeSlotList_.empty()) {
		SQLValues::ColumnTypeList typeList(cxt.getAllocator());
		BitTupleColumns::getColumnTypeList(typeList);
		slot = Slot::createSlot(cxt, typeList);
	}
	else {
		slot = freeSlotList_.back();
		freeSlotList_.pop_back();
	}

	slot.createTupleList(cxt);
	return slot;
}

void SQLContainerImpl::OIdTable::releaseSlot(OpContext &cxt, Slot &slot) {
	slot.closeTupleList(cxt);
	freeSlotList_.push_back(slot);
	slot = Slot::invalidSlot();
}

//
// OIdTable - Hot slot Operations
//

SQLContainerImpl::OIdTable::HotSlot*
SQLContainerImpl::OIdTable::findHotSlot(GroupId groupId) {
	HotData *hotData = hotData_.get();
	if (hotData == NULL) {
		return NULL;
	}

	HotSlotMap &map = prepareHotData().hotSlots_;
	HotSlotMap::iterator it = map.find(groupId);
	if (it == map.end()) {
		return NULL;
	}

	return it->second;
}

SQLContainerImpl::OIdTable::HotSlot&
SQLContainerImpl::OIdTable::prepareHotSlot(GroupId groupId) {
	HotData &hotData = prepareHotData();
	HotSlotMap &map = hotData.hotSlots_;

	HotSlotMap::iterator it = map.find(groupId);
	if (it == map.end()) {
		util::StackAllocator &alloc = hotData.alloc_;
		HotSlot *slot = ALLOC_NEW(alloc) HotSlot(alloc);
		it = map.insert(std::make_pair(groupId, slot)).first;
	}

	return *it->second;
}

SQLContainerImpl::OIdTable::HotData&
SQLContainerImpl::OIdTable::prepareHotData() {
	if (hotData_.get() == NULL) {
		hotData_ = ALLOC_UNIQUE(varAlloc_, HotData, allocManager_, baseAlloc_);
	}
	return *hotData_;
}

//
// OIdTable - Map Operations
//

SQLContainerImpl::OIdTable::EntryMap* SQLContainerImpl::OIdTable::findEntryMap(
		EntryType type, HotSlot &slot) {
	return slot.mapAt(type).get();
}

SQLContainerImpl::OIdTable::EntryMap&
SQLContainerImpl::OIdTable::prepareEntryMap(EntryType type, HotSlot &slot) {
	EntryMapPtr &ptr = slot.mapAt(type);
	if (ptr.get() == NULL) {
		util::StackAllocator &alloc = slot.alloc_;
		ptr = ALLOC_UNIQUE(alloc, EntryMap, alloc);
	}
	return *ptr;
}

void SQLContainerImpl::OIdTable::updateEntry(EntryType type, OId oId) {
	addModificationEntry(GROUP_ID_DELTA_ADDITION, type, oId);
}

void SQLContainerImpl::OIdTable::removeEntries(EntryType type, OId oId) {
	addModificationEntry(GROUP_ID_DELTA_REMOVAL, type, oId);

	HotSlot *slot = findHotSlot(GROUP_ID_DELTA_ADDITION);
	if (slot == NULL) {
		return;
	}

	const bool forRow = (type != TYPE_CHUNK) && !isRowArrayEntryType(type);
	for (size_t i = 0; i < END_TYPE; i++) {
		const EntryType mapType = static_cast<EntryType>(i);
		if (mapType == TYPE_CHUNK) {
			continue;
		}
		if (forRow && isRowArrayEntryType(mapType)) {
			continue;
		}

		EntryMap *map = findEntryMap(mapType, *slot);
		if (map != NULL) {
			removeEntriesAt(type, oId, *map);
		}
	}
}

void SQLContainerImpl::OIdTable::removeEntriesAt(
		EntryType type, OId oId, EntryMap &map) {
	const uint64_t key = Coders::oIdToKey(type, oId);

	if (type == TYPE_ROW_NORMAL) {
		EntryMap::iterator it = map.find(key);
		if (it != map.end()) {
			it->second &= ~Coders::oIdToPositionBit(oId);
			if (it->second == 0) {
				map.erase(it);
			}
		}
		return;
	}

	const uint64_t mask = Coders::chunkOrRaMaskOfKey(type, large_);
	const uint64_t maskedKey = (key & mask);
	EntryMap::iterator it = map.lower_bound(maskedKey);
	while (it != map.end() && (it->first & mask) == maskedKey) {
		EntryMap::iterator next = it;
		++next;
		map.erase(it);
		it = next;
	}
}

void SQLContainerImpl::OIdTable::addModificationEntry(
		GroupId groupId, EntryType type, OId oId) {
	HotSlot &slot = prepareHotSlot(groupId);
	EntryMap &map = prepareEntryMap(type, slot);

	const uint64_t key = Coders::oIdToKey(type, oId);
	map.insert(Entry(key, 0)).first->second |= Coders::oIdToPositionBit(oId);
}

void SQLContainerImpl::OIdTable::popUpdates(SearchResult &result) {
	HotSlot *slot = findHotSlot(GROUP_ID_DELTA_ADDITION);
	if (slot == NULL) {
		return;
	}

	for (size_t i = 0; i < END_TYPE; i++) {
		const EntryType type = static_cast<EntryType>(i);

		if (type == TYPE_CHUNK) {
			continue;
		}

		EntryMap *map = findEntryMap(type, *slot);
		if (map == NULL) {
			continue;
		}

		OIdList &oIdList = getSearchResultOIdList(type, result);
		EntryIteratorPair itPair(map->begin(), map->end());
		while (itPair.first != itPair.second) {
			popEntries(type, itPair, 1, oIdList);
		}

		map->clear();
	}
}

uint64_t SQLContainerImpl::OIdTable::popEntries(
		EntryType type, EntryIteratorPair &src, uint32_t rowArraySize,
		OIdList &dest) {
	EntryIterator &it = src.first;
	EntryIterator end = src.second;
	assert(it != end);

	const uint64_t chunkBits = Coders::chunkBitsOfKey(large_, it->first);
	uint64_t count = 0;
	do {
		OId oId;
		for (uint32_t pos = 0; Coders::nextOId(pos, *it, oId);) {
			dest.push_back(oId);
			count++;
		}
	}
	while (++it != end &&
			Coders::chunkBitsOfKey(large_, it->first) == chunkBits);

	return count * (isRowArrayEntryType(type) ? rowArraySize : 1);
}

//
// OIdTable - Search result Operations
//

SQLContainerImpl::OIdTable::OIdList*
SQLContainerImpl::OIdTable::findSingleSearchResultOIdList(
		SearchResult &result, EntryType &type) {
	EntryType foundType = END_TYPE;

	OIdList *oIdList = NULL;
	for (size_t i = 0; i < END_TYPE; i++) {
		const EntryType subType = static_cast<EntryType>(i);
		if (subType == TYPE_CHUNK) {
			continue;
		}

		OIdList &sub = getSearchResultOIdList(subType, result);
		if (oIdList != NULL && !oIdList->empty() && !sub.empty()) {
			foundType = END_TYPE;
			oIdList = NULL;
			break;
		}

		if (oIdList == NULL || !sub.empty()) {
			oIdList = &sub;
			foundType = subType;
		}
	}

	type = foundType;
	return oIdList;
}

const SQLContainerImpl::OIdTable::OIdList&
SQLContainerImpl::OIdTable::getSearchResultOIdList(
		EntryType type, const SearchResult &result) {
	const SearchResult::Entry &entry = result.getEntry(isMvccEntryType(type));
	return entry.getOIdList(isRowArrayEntryType(type));
}

SQLContainerImpl::OIdTable::OIdList&
SQLContainerImpl::OIdTable::getSearchResultOIdList(
		EntryType type, SearchResult &result) {
	SearchResult::Entry &entry = result.getEntry(isMvccEntryType(type));
	return entry.getOIdList(isRowArrayEntryType(type));
}

bool SQLContainerImpl::OIdTable::isMvccEntryType(EntryType type) {
	assert(type != TYPE_CHUNK);
	return (type == TYPE_RA_MVCC || type == TYPE_ROW_MVCC);
}

bool SQLContainerImpl::OIdTable::isRowArrayEntryType(EntryType type) {
	assert(type != TYPE_CHUNK);
	return (type == TYPE_RA_NORMAL || type == TYPE_RA_MVCC);
}

//
// OIdTable::Source
//

SQLContainerImpl::OIdTable::Source::Source(const OpContext::Source &cxtSrc) :
		allocManager_(NULL),
		baseAlloc_(NULL),
		varAlloc_(NULL),
		cxtSrc_(cxtSrc),
		stageSizeLimit_(0),
		large_(false) {
}

SQLContainerImpl::OIdTable::Source
SQLContainerImpl::OIdTable::Source::ofContext(
		OpContext &cxt, BaseContainer &container, int64_t memoryLimit) {
	Source source(cxt.getSource());
	source.allocManager_ = &cxt.getAllocatorManager();
	source.baseAlloc_ = &cxt.getAllocator().base();
	source.varAlloc_ = &cxt.getValueContext().getVarAllocator();
	source.stageSizeLimit_ =
			resolveStageSizeLimit(cxt.getInputBlockSize(0), memoryLimit);
	source.large_ = detectLarge(container);
	return source;
}

bool SQLContainerImpl::OIdTable::Source::detectLarge(BaseContainer &container) {
	ObjectManagerV4 &objectManager =
			CursorAccessorImpl::resolveObjectManager(container);

	const uint32_t chunkSize = objectManager.getChunkSize();
	const uint32_t chunkExpSize = util::nextPowerBitsOf2(chunkSize);
	if (chunkSize != (static_cast<uint32_t>(1) << chunkExpSize)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return OIdBitUtils::isLargeChunkMode(chunkExpSize);
}

uint32_t SQLContainerImpl::OIdTable::Source::resolveStageSizeLimit(
		uint32_t blockSize, int64_t memoryLimit) {
	if (memoryLimit < 0) {
		return 0;
	}
	return static_cast<uint32_t>(std::min<uint64_t>(
			std::numeric_limits<uint32_t>::max(),
			static_cast<uint64_t>(memoryLimit) /
					std::max<uint32_t>(blockSize, 1)));
}

SQLOps::OpAllocatorManager&
SQLContainerImpl::OIdTable::Source::getAlloactorManager() const {
	if (allocManager_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *allocManager_;
}

util::StackAllocator::BaseAllocator&
SQLContainerImpl::OIdTable::Source::getBaseAllocator() const {
	if (baseAlloc_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *baseAlloc_;
}

SQLValues::VarAllocator&
SQLContainerImpl::OIdTable::Source::getVarAllocator() const {
	if (varAlloc_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *varAlloc_;
}

const SQLOps::OpContext::Source&
SQLContainerImpl::OIdTable::Source::getContextSource() const {
	return cxtSrc_;
}

uint32_t SQLContainerImpl::OIdTable::Source::isStageSizeLimit() const {
	return stageSizeLimit_;
}

bool SQLContainerImpl::OIdTable::Source::isLarge() const {
	return large_;
}

//
// OIdTable::MergeContext
//

SQLContainerImpl::OIdTable::MergeContext::MergeContext(
		TupleListWriter &writer, GroupMode mode, const uint64_t *limit) :
		positionBits_(Coders::initialPositionBits(mode)),
		writer_(writer),
		limited_((limit != NULL)),
		limit_(limited_ ?
				std::max<uint64_t>(*limit, 1) :
				std::numeric_limits<uint64_t>::max()) {
}

//
// OIdTable::MergeElement
//

SQLContainerImpl::OIdTable::MergeElement::MergeElement(
		TupleListReader &reader) :
		reader_(reader) {
	assert(reader_.exists());
}

//
// OIdTable::MergeElementRef
//
SQLContainerImpl::OIdTable::MergeElementRef::MergeElementRef(
		MergeElement &elem) :
		reader_(&elem.reader_) {
}

inline uint64_t SQLContainerImpl::OIdTable::MergeElementRef::key() const {
	return readKey(*reader_);
}

inline uint64_t SQLContainerImpl::OIdTable::MergeElementRef::positionBits() const {
	return readPositionBits(*reader_);
}

inline bool SQLContainerImpl::OIdTable::MergeElementRef::next() const {
	assert(reader_->exists());
	reader_->next();
	return reader_->exists();
}

//
// OIdTable::MergeElementGreater
//

inline bool SQLContainerImpl::OIdTable::MergeElementGreater::operator()(
		const Element &elem1, const Element &elem2) const {
	return (*this)(elem1.getValue(), elem2.getValue());
}

inline bool SQLContainerImpl::OIdTable::MergeElementGreater::operator()(
		const MergeElementRef &elem1, const MergeElementRef &elem2) const {
	return (elem1.key() > elem2.key());
}

inline const SQLContainerImpl::OIdTable::MergeElementGreater&
SQLContainerImpl::OIdTable::MergeElementGreater::getTypeSwitcher() const {
	return *this;
}

template<typename Op>
typename SQLContainerImpl::OIdTable::MergeElementGreater::template Func<
		Op>::Type
SQLContainerImpl::OIdTable::MergeElementGreater::get() const {
	return getWithOptions<Op>();
}

template<typename Op>
typename SQLContainerImpl::OIdTable::MergeElementGreater::template Func<
		Op>::Type
SQLContainerImpl::OIdTable::MergeElementGreater::getWithOptions() const {
	return &execute<Op>;
}

template<typename Op> inline
typename Op::RetType SQLContainerImpl::OIdTable::MergeElementGreater::execute(
		Op &op) {
	typedef typename Op::template TypeAt<void> TypedOp;
	return TypedOp(op)();
}

//
// OIdTable::MergeAction
//

template<SQLContainerImpl::OIdTable::GroupMode M>
SQLContainerImpl::OIdTable::MergeAction<M>::MergeAction(MergeContext &cxt) :
		cxt_(cxt) {
}

template<SQLContainerImpl::OIdTable::GroupMode M>
inline bool SQLContainerImpl::OIdTable::MergeAction<M>::operator()(
		const Element &elem, const util::FalseType&) {
	const MergeElementRef &ref = elem.getValue();

	assert(ref.positionBits() != 0);
	Coders::mergePositionBits<M>(ref.positionBits(), cxt_.positionBits_);

	return true;
}

template<SQLContainerImpl::OIdTable::GroupMode M>
inline void SQLContainerImpl::OIdTable::MergeAction<M>::operator()(
		const Element &elem, const util::TrueType&) {
	const MergeElementRef &ref = elem.getValue();

	assert(ref.positionBits() != 0);
	Coders::mergePositionBits<M>(ref.positionBits(), cxt_.positionBits_);

	if (M == MODE_UNION || cxt_.positionBits_ != 0) {
		writeEntry(cxt_.writer_, Entry(ref.key(), cxt_.positionBits_));
	}
	cxt_.positionBits_ = Coders::initialPositionBits<M>();
}

template<SQLContainerImpl::OIdTable::GroupMode M>
inline bool SQLContainerImpl::OIdTable::MergeAction<M>::operator()(
		const Element &elem, const util::TrueType&,
		const util::TrueType&) {
	static_cast<void>(elem);
	return true;
}

template<SQLContainerImpl::OIdTable::GroupMode M>
inline bool SQLContainerImpl::OIdTable::MergeAction<M>::operator()(
		const Element &elem, const util::TrueType&,
		const Predicate &pred) {
	static_cast<void>(elem);
	static_cast<void>(pred);
	return true;
}

//
// OIdTable::Group
//

SQLContainerImpl::OIdTable::Group::Group(
		SQLValues::VarAllocator &varAlloc, GroupMode mode) :
		stageList_(varAlloc),
		parentId_(GROUP_ID_ROOT),
		subIdList_(varAlloc),
		mode_(mode) {
}

//
// OIdTable::Slot
//

SQLContainerImpl::OIdTable::Slot SQLContainerImpl::OIdTable::Slot::createSlot(
		OpContext &cxt, const SQLValues::ColumnTypeList &typeList) {
	return Slot(cxt.createLocal(&typeList));
}

SQLContainerImpl::OIdTable::Slot SQLContainerImpl::OIdTable::Slot::invalidSlot() {
	return Slot(std::numeric_limits<uint32_t>::max());
}

void SQLContainerImpl::OIdTable::Slot::createTupleList(OpContext &cxt) {
	cxt.createLocalTupleList(tupleListIndex_);
}

void SQLContainerImpl::OIdTable::Slot::closeTupleList(OpContext &cxt) {
	cxt.closeLocalTupleList(tupleListIndex_);
}

SQLOps::TupleListReader& SQLContainerImpl::OIdTable::Slot::getReader(
		OpContext &cxt) {
	cxt.setReaderRandomAccess(tupleListIndex_);
	SQLOps::TupleListReader &reader =
			cxt.getLocalReader(tupleListIndex_, READER_SUB_INDEX_CURRENT);

	SQLOps::TupleListReader &initialReader =
			cxt.getLocalReader(tupleListIndex_, READER_SUB_INDEX_INITIAL);
	initialReader.exists();

	return reader;
}

void SQLContainerImpl::OIdTable::Slot::resetReaderPosition(OpContext &cxt) {
	cxt.setReaderRandomAccess(tupleListIndex_);
	SQLOps::TupleListReader &reader =
			cxt.getLocalReader(tupleListIndex_, READER_SUB_INDEX_CURRENT);

	SQLOps::TupleListReader &initialReader =
			cxt.getLocalReader(tupleListIndex_, READER_SUB_INDEX_INITIAL);
	reader.assign(initialReader);
}

SQLOps::TupleListWriter& SQLContainerImpl::OIdTable::Slot::getWriter(
		OpContext &cxt) {
	return cxt.getWriter(tupleListIndex_);
}

void SQLContainerImpl::OIdTable::Slot::finalizeWriter(OpContext &cxt) {
	cxt.closeLocalWriter(tupleListIndex_);
}

SQLContainerImpl::OIdTable::Slot::Slot(uint32_t tupleListIndex) :
		tupleListIndex_(tupleListIndex) {
}

//
// OIdTable::HotSlot
//

SQLContainerImpl::OIdTable::HotSlot::HotSlot(util::StackAllocator &alloc) :
		alloc_(alloc) {
}

SQLContainerImpl::OIdTable::EntryMapPtr&
SQLContainerImpl::OIdTable::HotSlot::mapAt(EntryType type) {
	const size_t pos = type;
	assert(pos < END_TYPE);
	return mapList_[pos];
}

//
// OIdTable::HotData
//

SQLContainerImpl::OIdTable::HotData::HotData(
		SQLOps::OpAllocatorManager &allocManager,
		util::StackAllocator::BaseAllocator &baseAlloc) :
		allocRef_(allocManager, baseAlloc),
		alloc_(allocRef_.get()),
		hotSlots_(alloc_) {
}

//
// OIdTable::Cursor
//

SQLContainerImpl::OIdTable::Cursor::Cursor() :
		started_(false),
		finished_(false) {
}

//
// OIdTable::Coders
//

inline SQLContainerImpl::OIdTable::Entry
SQLContainerImpl::OIdTable::Coders::oIdToEntry(EntryType type, OId oId) {
	return Entry(oIdToKey(type, oId), oIdToPositionBit(oId));
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::oIdToKey(
		EntryType type, OId oId) {
	const uint64_t typeBits = static_cast<uint64_t>(type);
	return (OIdValues::toHighPart(oId) | typeBits);
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::oIdToPositionBit(
		OId oId) {
	return OIdValues::toLowBit(oId);
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::chunkOrRaMaskOfKey(
		EntryType type, bool large) {
	if (type == TYPE_CHUNK) {
		return chunkMaskOfKey(large);
	}
	else {
		assert(type == TYPE_RA_NORMAL);
		return raMaskOfKey();
	}
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::chunkBitsOfKey(
		bool large, uint64_t key) {
	return key & chunkMaskOfKey(large);
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::chunkMaskOfKey(
		bool large) {
	return ((~static_cast<uint64_t>(0)) << (large ?
			CoderConstants::UNIT_LARGE_CHUNK_SHIFT_BIT :
			CoderConstants::UNIT_SMALL_CHUNK_SHIFT_BIT));
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::raBitsOfKey(uint64_t key) {
	return key & raMaskOfKey();
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::raMaskOfKey() {
	return (~CoderConstants::OID_USER_MASK);
}

inline SQLContainerImpl::OIdTable::EntryType
SQLContainerImpl::OIdTable::Coders::typeOfKey(uint64_t key) {
	const uint64_t typeBits = OIdValues::extraOfHighPart(key);
	assert(typeBits < END_TYPE);
	return static_cast<EntryType>(typeBits);
}

inline bool SQLContainerImpl::OIdTable::Coders::nextOId(
		uint32_t &nextPos, const Entry &entry, OId &oId) {
	return OIdValues::nextValue(nextPos, entry.first, entry.second, oId);
}

inline bool SQLContainerImpl::OIdTable::Coders::tryMergeEntry(
		const Entry &src, Entry &dest) {
	if (src.first == dest.first) {
		mergePositionBits<MODE_UNION>(src.second, dest.second);
		return true;
	}
	return false;
}

inline uint64_t SQLContainerImpl::OIdTable::Coders::initialPositionBits(
		GroupMode mode) {
	if (mode == MODE_UNION) {
		return initialPositionBits<MODE_UNION>();
	}
	else {
		assert(mode == MODE_INTERSECT);
		return initialPositionBits<MODE_INTERSECT>();
	}
}

template<SQLContainerImpl::OIdTable::GroupMode M>
inline uint64_t SQLContainerImpl::OIdTable::Coders::initialPositionBits() {
	UTIL_STATIC_ASSERT(M == MODE_UNION || M == MODE_INTERSECT);
	return (M == MODE_UNION ? 0 : ~static_cast<uint64_t>(0));
}

template<SQLContainerImpl::OIdTable::GroupMode M>
inline void SQLContainerImpl::OIdTable::Coders::mergePositionBits(
		const uint64_t &src, uint64_t &dest) {
	UTIL_STATIC_ASSERT(M == MODE_UNION || M == MODE_INTERSECT);
	dest = (M == MODE_UNION ? (dest | src) : (dest & src));
}

//
// RowIdFilter
//

SQLContainerImpl::RowIdFilter::RowIdFilter(
		const OpContext::Source &cxtSrc,
		SQLOps::OpAllocatorManager &allocManager,
		util::StackAllocator::BaseAllocator &baseAlloc, RowId maxRowId,
		int64_t memoryLimit) :
		cxtSrc_(cxtSrc),
		allocManager_(allocManager),
		allocRef_(allocManager_, baseAlloc),
		alloc_(allocRef_.get()),
		maxRowId_(maxRowId),
		rowIdBits_(NULL),
		readOnly_(false),
		duplicatable_(false),
		cursorStarted_(false),
		memLimit_(memoryLimit),
		currentBitsPos_(-1) {
	initializeBits();
}

inline bool SQLContainerImpl::RowIdFilter::accept(RowId rowId) {
	assert(rowIdBits_ != NULL);
	if (rowId > maxRowId_) {
		return false;
	}

	if (readOnly_) {
		return (rowIdBits_->find(rowId) == rowIdBits_->end());
	}
	else {
		return rowIdBits_->insert(rowId).second;
	}
}

bool SQLContainerImpl::RowIdFilter::tryAdjust(RowIdFilter *filter) {
	if (filter == NULL || !filter->isAllocationLimitReached()) {
		return false;
	}

	filter->adjust();
	return filter->isDuplicatable();
}

void SQLContainerImpl::RowIdFilter::adjust() {
	OpContext cxt(cxtSrc_);
	adjustDetail(cxt);
}

bool SQLContainerImpl::RowIdFilter::isDuplicatable() {
	return duplicatable_;
}

void SQLContainerImpl::RowIdFilter::setReadOnly() {
	readOnly_ = true;
}

bool SQLContainerImpl::RowIdFilter::isReadOnly() {
	return readOnly_;
}

SQLOps::TupleListReader& SQLContainerImpl::RowIdFilter::getBitsEntryReader(
		util::LocalUniquePtr<OpContext> &localCxt) {
	localCxt = UTIL_MAKE_LOCAL_UNIQUE(localCxt, OpContext, cxtSrc_);
	if (!cursorStarted_) {
		adjustDetail(*localCxt);
		localCxt->closeLocalWriter(*tupleListIndex_);
		localCxt->releaseWriterLatch(*tupleListIndex_);
		cursorStarted_ = true;
	}
	return localCxt->getLocalReader(*tupleListIndex_);
}

bool SQLContainerImpl::RowIdFilter::hasCurrentRowId() {
	assert(cursorStarted_);
	return (currentBitsPos_ >= 0);
}

RowId SQLContainerImpl::RowIdFilter::getCurrentRowId() {
	assert(hasCurrentRowId());
	return static_cast<RowId>(currentBitsEntry_.first) |
			static_cast<RowId>(currentBitsPos_);
}

void SQLContainerImpl::RowIdFilter::prepareCurrentRowId(
		TupleListReader &reader, bool forNext) {
	assert(cursorStarted_);

	if (forNext) {
		if (++currentBitsPos_ >= BITS_ENTRY_POS_COUNT) {
			currentBitsPos_ = -1;
		}
	}

	for (;;) {
		if (currentBitsPos_ < 0) {
			if (!reader.exists()) {
				return;
			}
			readBitsEntry(reader, currentBitsEntry_);
			currentBitsPos_ = 0;
		}

		do {
			if ((static_cast<RowId>(currentBitsEntry_.second) &
					(static_cast<RowId>(1) <<
							static_cast<RowId>(currentBitsPos_))) != 0) {
				return;
			}
		}
		while (++currentBitsPos_ < BITS_ENTRY_POS_COUNT);
		currentBitsPos_ = -1;
	}
}

void SQLContainerImpl::RowIdFilter::adjustDetail(OpContext &cxt) {
	if (tupleListIndex_.get() == NULL) {
		SQLValues::ColumnTypeList typeList(cxt.getAllocator());
		BitTupleColumns::getColumnTypeList(typeList);

		tupleListIndex_ = UTIL_MAKE_LOCAL_UNIQUE(
				tupleListIndex_, uint32_t, cxt.createLocal(&typeList));
		cxt.createLocalTupleList(*tupleListIndex_);
	}
	writeBits(cxt.getWriter(*tupleListIndex_));
	initializeBits();
	duplicatable_ = true;
}

void SQLContainerImpl::RowIdFilter::initializeBits() {
	rowIdBits_ = NULL;

	allocScope_ = UTIL_MAKE_LOCAL_UNIQUE(
			allocScope_, util::StackAllocator::Scope, alloc_);
	rowIdBits_ = ALLOC_NEW(alloc_) BitSet(alloc_);
}

void SQLContainerImpl::RowIdFilter::readBitsEntry(
		TupleListReader &reader, BitSet::CodedEntry &entry) {
	assert(reader.exists());
	entry = BitSet::CodedEntry(
			BitTupleColumns::readCurrentKey(reader),
			BitTupleColumns::readCurrentValue(reader));
	reader.next();
}

void SQLContainerImpl::RowIdFilter::writeBits(TupleListWriter &writer) {
	assert(rowIdBits_ != NULL);
	BitSet::InputCursor cursor(*rowIdBits_);
	for (BitSet::CodedEntry entry; cursor.next(entry);) {
		writer.next();

		WritableTuple tuple = writer.get();
		BitTupleColumns::writeKey(tuple, entry.first);
		BitTupleColumns::writeValue(tuple, entry.second);
	}
}

bool SQLContainerImpl::RowIdFilter::isAllocationLimitReached() {
	if (memLimit_ < 0 || getAllocationUsageSize() <=
			static_cast<uint64_t>(memLimit_)) {
		return false;
	}

	return true;
}

uint64_t SQLContainerImpl::RowIdFilter::getAllocationUsageSize() {
	return alloc_.getTotalSize() - alloc_.getFreeSize();
}

//
// RowIdFilter::RowIdCursor
//

SQLContainerImpl::RowIdFilter::RowIdCursor::RowIdCursor(RowIdFilter &filter) :
		filter_(filter),
		reader_(filter_.getBitsEntryReader(localCxt_)) {
	const bool forNext = false;
	filter_.prepareCurrentRowId(reader_, forNext);
}

inline bool SQLContainerImpl::RowIdFilter::RowIdCursor::RowIdCursor::exists() {
	return filter_.hasCurrentRowId();
}

inline RowId SQLContainerImpl::RowIdFilter::RowIdCursor::RowIdCursor::get() {
	return filter_.getCurrentRowId();
}

inline void SQLContainerImpl::RowIdFilter::RowIdCursor::RowIdCursor::next() {
	const bool forNext = true;
	filter_.prepareCurrentRowId(reader_, forNext);
}

//
// ScanSimulatorUtils
//

void SQLContainerImpl::ScanSimulatorUtils::trySimulateUpdates(
		OpContext &cxt, CursorAccessorImpl &accessor) {
	OpSimulator *simulator = cxt.getSimulator();
	if (simulator != NULL) {
		BaseContainer *container = accessor.getContainer();
		if (container != NULL) {
			simulateUpdates(
					accessor.resolveTransactionContext(), *simulator,
					*container, accessor.resolveResultSet());
		}
	}
}

void SQLContainerImpl::ScanSimulatorUtils::tryAcceptUpdates(
		OpContext &cxt, CursorAccessorImpl &accessor,
		const SearchResult &result) {
	OpSimulator *simulator = cxt.getSimulator();
	if (simulator != NULL) {
		acceptUpdates(
				accessor.resolveTransactionContext(), *simulator,
				accessor.resolveContainer(), result);
	}
}

void SQLContainerImpl::ScanSimulatorUtils::simulateUpdates(
		TransactionContext &txn, OpSimulator &simulator,
		BaseContainer &container, ResultSet &resultSet) {
	if (simulator.nextAction(
			OpSimulator::ACTION_UPDATE_LIST_INVALID, NULL, true)) {
		resultSet.invalidateUpdateRowIdList();
		return;
	}

	int64_t param;
	if (!simulator.nextAction(
			OpSimulator::ACTION_UPDATE_ROW, &param, false)) {
		return;
	}

	int64_t tupleId;
	if (!simulator.getLastTupleId(tupleId)) {
		return;
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<RowId> targetRowIdList(alloc);
	targetRowIdList.push_back(tupleId);

	util::XArray<OId> targetOIdList(alloc);
	uint64_t skipped;
	container.getOIdList(
			txn, 0, MAX_RESULT_SIZE, skipped,
			targetRowIdList, targetOIdList);
	if (targetOIdList.empty()) {
		return;
	}

	ResultSet::UpdateOperation updateOp;
	switch (param) {
	case ResultSet::UPDATE_OP_UPDATE_NORMAL_ROW:
	case ResultSet::UPDATE_OP_UPDATE_MVCC_ROW:
	case ResultSet::UPDATE_OP_REMOVE_ROW:
	case ResultSet::UPDATE_OP_REMOVE_ROW_ARRAY:
	case ResultSet::UPDATE_OP_REMOVE_CHUNK:
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}
	updateOp = static_cast<ResultSet::UpdateOperation>(param);

	resultSet.addUpdatedId(tupleId, targetOIdList.front(), updateOp);

	simulator.nextAction(OpSimulator::ACTION_UPDATE_ROW, NULL, true);
}

void SQLContainerImpl::ScanSimulatorUtils::acceptUpdates(
		TransactionContext &txn, OpSimulator &simulator,
		BaseContainer &container, const SearchResult &result) {
	if (!simulator.findAction(OpSimulator::ACTION_UPDATE_ROW)) {
		return;
	}

	const util::XArray<OId> &oIdList = result.getEntry(false).getOIdList(false);
	if (oIdList.empty()) {
		return;
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<OId> targetOIdList(alloc);
	targetOIdList.push_back(oIdList.front());

	util::XArray<RowId> targetRowIdList(alloc);
	container.getRowIdList(txn, targetOIdList, targetRowIdList);
	if (!targetRowIdList.empty()) {
		simulator.acceptTupleId(targetRowIdList.front());
	}
	else {
		assert(false);
	}
}

//
// ContainerValueUtils
//

Value& SQLContainerImpl::ContainerValueUtils::toContainerValue(
		Value::Pool &valuePool, const TupleValue &src) {
	Value::Buffer *valueBuf;
	Value &dest = valuePool.newValue(valueBuf);
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
			dest.setString(*valueBuf, addr, size);
		}
		break;
	case TupleTypes::TYPE_BLOB:
		{
			Value::Buffer *buf;
			valuePool.newValue(buf);
			{
				TupleValue::LobReader reader(src);
				const void *data;
				size_t size;
				while (reader.next(data, size)) {
					const size_t offset = buf->size();
					buf->resize(offset + size);
					memcpy(&(*buf)[offset], data, size);
				}
			}
			{
				const uint32_t size = static_cast<uint32_t>(buf->size());
				void *data = buf->data();
				dest.setString(*valueBuf, static_cast<char8_t*>(data), size);
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
	return dest;
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

//
// TQLTool
//

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
