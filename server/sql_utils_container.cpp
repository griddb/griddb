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
#include "sql_operator_utils.h"
#include "query.h"
#include "sql_execution.h"
#include "sql_execution_manager.h"
#include "nosql_utils.h"




util::AllocUniquePtr<SQLContainerUtils::ScanCursor>::ReturnType
SQLContainerUtils::ScanCursor::create(
		util::StdAllocator<void, void> &alloc, const Source &source) {
	util::AllocUniquePtr<ScanCursor> ptr(
			ALLOC_NEW(alloc) SQLContainerImpl::CursorImpl(source), alloc);
	return ptr;
}

SQLContainerUtils::ScanCursor::~ScanCursor() {
}


SQLContainerUtils::ScanCursor::Source::Source(
		util::StackAllocator &alloc,
		const SQLOps::ContainerLocation &location,
		const ColumnTypeList *columnTypeList) :
		alloc_(alloc),
		location_(location),
		columnTypeList_(columnTypeList),
		indexLimit_(-1) {
}


SQLContainerUtils::ScanCursor::LatchTarget::LatchTarget(ScanCursor *cursor) :
		cursor_(cursor) {
}

void SQLContainerUtils::ScanCursor::LatchTarget::unlatch() throw() {
	if (cursor_ != NULL) {
		cursor_->unlatch();
	}
}

void SQLContainerUtils::ScanCursor::LatchTarget::close() throw() {
	if (cursor_ != NULL) {
		cursor_->close();
	}
}



size_t SQLContainerUtils::ContainerUtils::findMaxStringLength(
		const ResourceSet *resourceSet) {
	do {
		if (resourceSet == NULL) {
			break;
		}

		DataStore *dataStore = resourceSet->getDataStore();
		if (dataStore == NULL) {
			break;
		}

		return dataStore->getValueLimitConfig().getLimitSmallSize();
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

			partitionId = DataStore::resolvePartitionId(
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


SQLContainerImpl::CursorImpl::CursorImpl(const Source &source) :
		location_(source.location_),
		outIndex_(std::numeric_limits<uint32_t>::max()),
		closed_(false),
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
		nextContainerId_(UNDEF_CONTAINERID),
		maxRowId_(UNDEF_ROWID),
		generalRowArray_(NULL),
		plainRowArray_(NULL),
		row_(NULL),
		scanner_(NULL),
		initialNullsStats_(source.alloc_),
		lastNullsStats_(source.alloc_),
		indexLimit_(source.indexLimit_),
		totalSearchCount_(0) {
}

SQLContainerImpl::CursorImpl::~CursorImpl() {
	destroy();
}

void SQLContainerImpl::CursorImpl::unlatch() throw() {
	finishScope();
}

void SQLContainerImpl::CursorImpl::close() throw() {
	unlatch();
}

void SQLContainerImpl::CursorImpl::destroy() throw() {
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
	DataStore *dataStore = lastDataStore_;

	resultSetId_ = UNDEF_RESULTSETID;
	lastPartitionId_ = UNDEF_PARTITIONID;
	lastDataStore_ = NULL;

	dataStore->closeResultSet(partitionId, rsId);
}

bool SQLContainerImpl::CursorImpl::scanFull(
		OpContext &cxt, const Projection &proj) {
	startScope(cxt);

	if (getContainer() == NULL) {
		return true;
	}

	BaseContainer &container = resolveContainer();
	ContainerRowScanner *scanner = tryCreateScanner(cxt, proj, container);
	TransactionContext &txn = resolveTransactionContext();

	for (;;) {
		BtreeSearchContext sc(cxt.getAllocator(), container.getRowIdColumnId());
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

		const bool finished = finishPartialSearch(cxt, sc);
		if (!finished) {
			return finished;
		}

		updateScanner(cxt, finished);
		return finished;
	}
	while (false);
}

bool SQLContainerImpl::CursorImpl::scanIndex(
		OpContext &cxt, const Projection &proj,
		const SQLExprs::IndexConditionList &condList) {
	startScope(cxt);

	if (getContainer() == NULL) {
		return true;
	}

	BaseContainer &container = resolveContainer();
	ContainerRowScanner *scanner = tryCreateScanner(cxt, proj, container);
	TransactionContext &txn = resolveTransactionContext();

	util::Vector<uint32_t> indexColumnList(cxt.getAllocator());
	const IndexType indexType =
			acceptIndexCond(cxt, condList, indexColumnList);
	SQLOps::OpProfilerIndexEntry *profilerEntry = cxt.getIndexProfiler();

	bool finished = true;
	SearchResult result(cxt.getAllocator());
	if (indexType == SQLType::INDEX_TREE_RANGE ||
			indexType == SQLType::INDEX_TREE_EQ) {
		BtreeSearchContext sc(txn.getDefaultAllocator(), indexColumnList);
		preparePartialSearch(cxt, sc, &condList);
		if (profilerEntry != NULL) {
			profilerEntry->startSearch();
		}

		if (container.getContainerType() == TIME_SERIES_CONTAINER) {
			TimeSeries &timeSeries = static_cast<TimeSeries&>(container);
			if (condList.front().column_ == ColumnInfo::ROW_KEY_COLUMN_ID) {
				timeSeries.searchRowIdIndexAsRowArray(
						txn, sc, result.oIdList_, result.mvccOIdList_);
				result.asRowArray_ = true;
			}
			else {
				BtreeSearchContext orgSc(
						txn.getDefaultAllocator(), indexColumnList);
				setUpSearchContext(cxt, txn, condList, orgSc, container);
				timeSeries.searchColumnIdIndex(
						txn, sc, orgSc, result.oIdList_, result.mvccOIdList_);
			}
		}
		else {
			container.searchColumnIdIndex(
					txn, sc, result.oIdList_, result.mvccOIdList_);
		}

		if (profilerEntry != NULL) {
			profilerEntry->finishSearch();
		}
		finished = finishPartialSearch(cxt, sc);
	}

	acceptSearchResult(cxt, result, &condList, *scanner);

	updateScanner(cxt, finished);
	return finished;
}

bool SQLContainerImpl::CursorImpl::scanUpdates(
		OpContext &cxt, const Projection &proj) {
	startScope(cxt);

	if (getContainer() == NULL || updateRowIdHandler_.get() == NULL) {
		return true;
	}

	BaseContainer &container = resolveContainer();
	ContainerRowScanner *scanner = tryCreateScanner(cxt, proj, container);
	TransactionContext &txn = resolveTransactionContext();

	ResultSet &resultSet = resolveResultSet();
	clearResultSetPreserving(resultSet);

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::StackAllocator::Scope scope(alloc);

	util::XArray<RowId> rowIdList(alloc);
	SearchResult result(alloc);
	const bool finished = updateRowIdHandler_->getRowIdList(
			rowIdList, cxt.getNextCountForSuspend());

	uint64_t skipped;
	container.getOIdList(
			txn, 0, std::numeric_limits<uint64_t>::max(),
			skipped, rowIdList, result.oIdList_);

	acceptSearchResult(cxt, result, NULL, *scanner);

	if (!finished) {
		activateResultSetPreserving(resultSet);
	}

	return finished;
}

bool SQLContainerImpl::CursorImpl::scanMeta(
		OpContext &cxt, const Projection &proj, const Expression *pred) {
	startScope(cxt);

	cxt.closeLocalTupleList(outIndex_);
	cxt.createLocalTupleList(outIndex_);

	util::StackAllocator &alloc = cxt.getAllocator();

	TransactionContext &txn = resolveTransactionContext();
	DataStore *dataStore = getDataStore(cxt);
	assert(dataStore != NULL);

	util::String dbName(alloc);
	FullContainerKey *containerKey = predicateToContainerKey(
			cxt.getValueContext(), txn, *dataStore, pred, location_, dbName);

	MetaProcessorSource source(location_.dbVersionId_, dbName.c_str());
	source.dataStore_ = dataStore;
	source.eventContext_= getEventContext(cxt);
	source.transactionManager_ = getTransactionManager(cxt);
	source.transactionService_ = getTransactionService(cxt);
	source.partitionTable_ = getPartitionTable(cxt);
	source.sqlService_ = getSQLService(cxt);
	source.sqlExecutionManager_ = getExecutionManager(cxt);
	source.resourceSet_ = source.sqlExecutionManager_->getResourceSet();

	const bool forCore = true;
	MetaScanHandler handler(cxt, *this, proj);
	MetaProcessor proc(txn, location_.id_, forCore);

	proc.setNextContainerId(getNextContainerId());
	proc.setContainerLimit(cxt.getNextCountForSuspend());
	proc.setContainerKey(containerKey);

	proc.scan(txn, source, handler);

	setNextContainerId(proc.getNextContainerId());

	cxt.closeLocalWriter(outIndex_);
	return !proc.isSuspended();
}

void SQLContainerImpl::CursorImpl::getIndexSpec(
		OpContext &cxt, SQLExprs::IndexSelector &selector) {
	startScope(cxt);

	TransactionContext &txn = resolveTransactionContext();
	BaseContainer &container = resolveContainer();

	const TupleColumnList &columnList = cxt.getInputColumnList(0);
	assignIndexInfo(txn, container, selector, columnList);
}

bool SQLContainerImpl::CursorImpl::isIndexLost() {
	return indexLost_;
}

uint32_t SQLContainerImpl::CursorImpl::getOutputIndex() {
	return outIndex_;
}

void SQLContainerImpl::CursorImpl::setOutputIndex(uint32_t index) {
	outIndex_ = index;
}

void SQLContainerImpl::CursorImpl::startScope(OpContext &cxt) {
	if (scope_.get() != NULL) {
		return;
	}

	scope_ = ALLOC_UNIQUE(cxt.getAllocator(), CursorScope, cxt, *this);
}

void SQLContainerImpl::CursorImpl::finishScope() throw() {
	scope_.reset();
}

SQLContainerImpl::CursorScope& SQLContainerImpl::CursorImpl::resolveScope() {
	if (scope_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *scope_;
}

ContainerId SQLContainerImpl::CursorImpl::getNextContainerId() const {
	if (nextContainerId_ == UNDEF_CONTAINERID) {
		return 0;
	}

	return nextContainerId_;
}

void SQLContainerImpl::CursorImpl::setNextContainerId(ContainerId containerId) {
	nextContainerId_ = containerId;
}

BaseContainer& SQLContainerImpl::CursorImpl::resolveContainer() {
	if (container_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *container_;
}

BaseContainer* SQLContainerImpl::CursorImpl::getContainer() {
	assert(scope_.get() != NULL);
	return container_;
}

TransactionContext& SQLContainerImpl::CursorImpl::resolveTransactionContext() {
	return resolveScope().getTransactionContext();
}

ResultSet& SQLContainerImpl::CursorImpl::resolveResultSet() {
	return resolveScope().getResultSet();
}

template<BaseContainer::RowArrayType RAType>
void SQLContainerImpl::CursorImpl::setRow(
		typename BaseContainer::RowArrayImpl<
				BaseContainer, RAType> &rowArrayImpl) {
	row_ = rowArrayImpl.getRow();
}

bool SQLContainerImpl::CursorImpl::isValueNull(const ContainerColumn &column) {
	RowArray::Row row(row_, getRowArray());
	return row.isNullValue(column.getColumnInfo());
}

template<RowArrayType RAType, typename Ret, TupleColumnType T>
Ret SQLContainerImpl::CursorImpl::getValueDirect(
		SQLValues::ValueContext &cxt, const ContainerColumn &column) {
	return DirectValueAccessor<T>().template access<RAType, Ret>(
			cxt, *this, column);
}

template<RowArrayType RAType>
void SQLContainerImpl::CursorImpl::writeValue(
		SQLValues::ValueContext &cxt, TupleList::Writer &writer,
		const TupleList::Column &destColumn,
		const ContainerColumn &srcColumn, bool forId) {
	if (!forId) {
		RowArray::Row row(row_, getRowArray());
		if (row.isNullValue(srcColumn.getColumnInfo())) {
			writeValueAs<TupleTypes::TYPE_ANY>(writer, destColumn, TupleValue());
			return;
		}
	}

	switch (srcColumn.getColumnInfo().getColumnType()) {
	case COLUMN_TYPE_STRING:
		writeValueAs<TupleTypes::TYPE_STRING>(writer, destColumn, getValueDirect<
				RAType, TupleString, TupleTypes::TYPE_STRING>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_BOOL:
		writeValueAs<TupleTypes::TYPE_BOOL>(writer, destColumn, getValueDirect<
				RAType, bool, TupleTypes::TYPE_BOOL>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_BYTE:
		writeValueAs<TupleTypes::TYPE_BYTE>(writer, destColumn, getValueDirect<
				RAType, int8_t, TupleTypes::TYPE_BYTE>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_SHORT:
		writeValueAs<TupleTypes::TYPE_SHORT>(writer, destColumn, getValueDirect<
				RAType, int16_t, TupleTypes::TYPE_SHORT>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_INT:
		writeValueAs<TupleTypes::TYPE_INTEGER>(writer, destColumn, getValueDirect<
				RAType, int32_t, TupleTypes::TYPE_INTEGER>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_LONG:
		writeValueAs<TupleTypes::TYPE_LONG>(writer, destColumn, getValueDirect<
				RAType, int64_t, TupleTypes::TYPE_LONG>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_FLOAT:
		writeValueAs<TupleTypes::TYPE_FLOAT>(writer, destColumn, getValueDirect<
				RAType, float, TupleTypes::TYPE_FLOAT>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_DOUBLE:
		writeValueAs<TupleTypes::TYPE_DOUBLE>(writer, destColumn, getValueDirect<
				RAType, double, TupleTypes::TYPE_DOUBLE>(
						cxt, srcColumn));
		break;
	case COLUMN_TYPE_TIMESTAMP:
		{
			const int64_t value = getValueDirect<
					RAType, int64_t, TupleTypes::TYPE_TIMESTAMP>(cxt, srcColumn);
			if (forId) {
				writeValueAs<TupleTypes::TYPE_LONG>(writer, destColumn, value);
			}
			else {
				writeValueAs<TupleTypes::TYPE_TIMESTAMP>(writer, destColumn, value);
			}
		}
		break;
	case COLUMN_TYPE_BLOB:
		{
			VarContext::Scope scope(cxt.getVarContext());
			writeValueAs<TupleTypes::TYPE_BLOB>(writer, destColumn, getValueDirect<
					RAType, TupleValue, TupleTypes::TYPE_BLOB>(
							cxt, srcColumn));
		}
		break;
	default:
		writeValueAs<TupleTypes::TYPE_ANY>(writer, destColumn, TupleValue());
		break;
	}
}

template<TupleColumnType T>
void SQLContainerImpl::CursorImpl::writeValueAs(
		TupleList::Writer &writer, const TupleList::Column &column,
		const typename SQLValues::TypeUtils::Traits<T>::ValueType &value) {
	TupleList::WritableTuple tuple = writer.get();
	tuple.set(column, ValueUtils::toAnyByValue<T>(value));
}

ContainerRowScanner* SQLContainerImpl::CursorImpl::tryCreateScanner(
		OpContext &cxt, const Projection &proj,
		const BaseContainer &container) {

	if (!updateNullsStats(container)) {
		scanner_ = NULL;
	}

	if (scanner_ == NULL) {
		if (updatorList_.get() == NULL) {
			updatorList_ =
					UTIL_MAKE_LOCAL_UNIQUE(updatorList_, UpdatorList, cxt.getAllocator());
		}
		scanner_ = &ScannerHandlerFactory::getInstance().create(
				cxt, *this, *getRowArray(), proj);
		cxt.setUpExprContext();
	}
	updateScanner(cxt, false);
	return scanner_;
}

void SQLContainerImpl::CursorImpl::updateScanner(
		OpContext &cxt, bool finished) {
	for (UpdatorList::iterator it = updatorList_->begin();
			it != updatorList_->end(); ++it) {
		it->first(cxt, it->second);
	}

	if (finished) {
		scanner_ = NULL;
	}
}

void SQLContainerImpl::CursorImpl::clearResultSetPreserving(
		ResultSet &resultSet) {
	if (resultSetPreserving_) {
		resultSet.setPartialExecStatus(ResultSet::NOT_PARTIAL);
		resultSet.resetExecCount();
		resultSetPreserving_ = false;
	}
}

void SQLContainerImpl::CursorImpl::activateResultSetPreserving(
		ResultSet &resultSet) {
	if (resultSetPreserving_ || resultSet.isPartialExecuteSuspend()) {
		return;
	}

	resultSet.setPartialExecStatus(ResultSet::PARTIAL_SUSPENDED);
	resultSet.incrementExecCount();
	resultSetPreserving_ = true;
}

void SQLContainerImpl::CursorImpl::checkInputInfo(
		OpContext &cxt, const BaseContainer &container) {
	const uint32_t count = container.getColumnNum();
	const SQLOps::TupleColumnList &list = cxt.getInputColumnList(0);

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

TupleColumnType SQLContainerImpl::CursorImpl::resolveColumnType(
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

bool SQLContainerImpl::CursorImpl::isColumnNonNullable(uint32_t pos) {
	if (nullsStatsChanged_) {
		return false;
	}

	return !getBit(lastNullsStats_, pos);
}

SQLContainerImpl::ContainerColumn
SQLContainerImpl::CursorImpl::resolveColumn(
		BaseContainer &container, bool forId, uint32_t pos) {
	if (forId) {
		if (container.getContainerType() == TIME_SERIES_CONTAINER) {
			ContainerColumn column =
					RowArray::getRowIdColumn<TimeSeries>(container);

			ColumnInfo modInfo = column.getColumnInfo();
			modInfo.setType(COLUMN_TYPE_TIMESTAMP, false);
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

void SQLContainerImpl::CursorImpl::preparePartialSearch(
		OpContext &cxt, BtreeSearchContext &sc,
		const SQLExprs::IndexConditionList *targetCondList) {
	BaseContainer &container = resolveContainer();
	ResultSet &resultSet = resolveResultSet();
	TransactionContext &txn = resolveTransactionContext();

	const uint32_t scale = Constants::ESTIMATED_ROW_ARRAY_ROW_COUNT;
	resultSet.setPartialExecuteSize(
			std::max<uint64_t>(cxt.getNextCountForSuspend() * scale, 1));

	const bool lastSuspended = resultSet.isPartialExecuteSuspend();
	const ResultSize suspendLimit = resultSet.getPartialExecuteSize();
	const OutputOrder outputOrder = ORDER_UNDEFINED;

	clearResultSetPreserving(resultSet);

	if (targetCondList == NULL) {
		resultSet.setUpdateIdType(ResultSet::UPDATE_ID_NONE);
	}
	else {
		setUpSearchContext(cxt, txn, *targetCondList, sc, container);
		if (!lastSuspended) {
			resultSet.setPartialMode(true);
		}
		resultSet.setUpdateIdType(ResultSet::UPDATE_ID_ROW);
	}

	resultSet.rewritePartialContext(
			txn, sc, suspendLimit, outputOrder, 0, container,
			container.getNormalRowArrayNum());
}

bool SQLContainerImpl::CursorImpl::finishPartialSearch(
		OpContext &cxt, BtreeSearchContext &sc) {
	ResultSet &resultSet = resolveResultSet();

	const bool lastSuspended = resultSet.isPartialExecuteSuspend();
	ResultSize suspendLimit = resultSet.getPartialExecuteSize();

	resultSet.setPartialContext(sc, suspendLimit, 0, MAP_TYPE_BTREE);

	if (!lastSuspended) {
		TransactionContext &txn = resolveTransactionContext();
		BaseContainer &container = resolveContainer();

		resultSet.setLastRowId(getMaxRowId(txn, container));
	}

	const bool finished = (!sc.isSuspended() || cxt.isCompleted());
	if (finished) {
		resultSet.setPartialExecStatus(ResultSet::PARTIAL_FINISH);
	}
	else {
		resultSet.setPartialExecStatus(ResultSet::PARTIAL_SUSPENDED);
		resultSet.incrementExecCount();

		if (resultSet.getUpdateIdType() != ResultSet::UPDATE_ID_NONE) {
			if (updateRowIdHandler_.get() == NULL) {
				updateRowIdHandler_ = ALLOC_UNIQUE(
						cxt.getValueContext().getVarAllocator(),
						TupleUpdateRowIdHandler,
						cxt, resultSet.getLastRowId());
			}
			if (resultSet.getUpdateRowIdHandler() == NULL) {
				util::StackAllocator *rsAlloc = resultSet.getRSAllocator();
				const size_t updateListThreshold = 0;
				resultSet.setUpdateRowIdHandler(
						updateRowIdHandler_->share(*rsAlloc),
						updateListThreshold);
			}
		}
	}

	resultSet.incrementReturnCount();
	activateResultSetPreserving(resultSet);

	return finished;
}

void SQLContainerImpl::CursorImpl::setUpSearchContext(
		OpContext &cxt, TransactionContext &txn,
		const SQLExprs::IndexConditionList &targetCondList,
		BtreeSearchContext &sc, const BaseContainer &container) {

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
			Value *value = ALLOC_NEW(cxt.getAllocator()) Value();
			ContainerValueUtils::toContainerValue(
					cxt.getAllocator(), cond.valuePair_.first, *value);

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
			Value *value = ALLOC_NEW(cxt.getAllocator()) Value();
			ContainerValueUtils::toContainerValue(
					cxt.getAllocator(), cond.valuePair_.second, *value);

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

	TransactionContext &txn = resolveTransactionContext();
	BaseContainer &container = resolveContainer();

	typedef util::Vector<IndexInfo> IndexInfoList;
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	IndexInfoList indexInfoList(alloc);
	getIndexInfoList(txn, container, indexInfoList);

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
		ContainerRowScanner &scanner) {
	static_cast<void>(cxt);
	static_cast<void>(targetCondList);

	if (result.oIdList_.empty() && result.mvccOIdList_.empty()) {
		return;
	}

	if (indexLimit_ > 0) {
		totalSearchCount_ += result.oIdList_.empty();
		totalSearchCount_ += result.mvccOIdList_.empty();
		if (resolveResultSet().isPartialExecuteSuspend() &&
				totalSearchCount_ > static_cast<uint64_t>(indexLimit_)) {
			setIndexLost(cxt);
			return;
		}
	}

	TransactionContext &txn = resolveTransactionContext();
	BaseContainer &container = resolveContainer();

	const ContainerType containerType = container.getContainerType();
	if (containerType == COLLECTION_CONTAINER) {
		scanRow(txn, static_cast<Collection&>(container), result, scanner);
	}
	else if (containerType == TIME_SERIES_CONTAINER) {
		scanRow(txn, static_cast<TimeSeries&>(container), result, scanner);
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

	ObjectManager *objectManager = container.getObjectManager();

	if (profilerEntry->findIndexName() == NULL) {
		profilerEntry->setIndexName(info.indexName_.c_str());
	}

	for (util::Vector<uint32_t>::const_iterator it = info.columnIds_.begin();
			it != info.columnIds_.end(); ++it) {
		if (profilerEntry->findColumnName(*it) == NULL) {
			ColumnInfo &columnInfo = container.getColumnInfo(*it);
			profilerEntry->setColumnName(
					*it, columnInfo.getColumnName(txn, *objectManager));
		}
		profilerEntry->addIndexColumn(*it);
	}

	for (SQLExprs::IndexConditionList::const_iterator it =
			targetCondList.begin(); it != targetCondList.end(); ++it) {
		profilerEntry->addCondition(*it);
	}
}

void SQLContainerImpl::CursorImpl::setIndexLost(OpContext &cxt) {
	clearResultSetPreserving(resolveResultSet());
	cxt.invalidate();
	indexLost_ = true;
}

template<typename Container>
void SQLContainerImpl::CursorImpl::scanRow(
		TransactionContext &txn, Container &container,
		const SearchResult &result, ContainerRowScanner &scanner) {
	if (!result.oIdList_.empty()) {
		const bool onMvcc = false;
		const OId *begin = &result.oIdList_.front();
		const OId *end = &result.oIdList_.back() + 1;
		if (result.asRowArray_) {
			container.scanRowArray(txn, begin, end, onMvcc, scanner);
		}
		else {
			container.scanRow(txn, begin, end, onMvcc, scanner);
		}
	}

	if (!result.mvccOIdList_.empty()) {
		const bool onMvcc = true;
		const OId *begin = &result.mvccOIdList_.front();
		const OId *end = &result.mvccOIdList_.back() + 1;
		if (result.asRowArray_) {
			container.scanRowArray(txn, begin, end, onMvcc, scanner);
		}
		else {
			container.scanRow(txn, begin, end, onMvcc, scanner);
		}
	}
}

void SQLContainerImpl::CursorImpl::initializeScanRange(
		TransactionContext &txn, BaseContainer &container) {
	const RowId maxRowId = getMaxRowId(txn, container);
	if (maxRowId != UNDEF_ROWID) {
		maxRowId_ = maxRowId;
	}
}

void SQLContainerImpl::CursorImpl::assignIndexInfo(
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

void SQLContainerImpl::CursorImpl::getIndexInfoList(
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

TransactionContext& SQLContainerImpl::CursorImpl::prepareTransactionContext(
		OpContext &cxt) {
	util::StackAllocator &alloc = cxt.getAllocator();

	const EventContext *ec = getEventContext(cxt);
	if (ec->isOnIOWorker()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
				"Internal error by invalid event context");
	}

	const PartitionId partitionId = cxt.getExtContext().getEvent()->getPartitionId();

	{
		GetContainerPropertiesHandler handler;
		handler.partitionTable_ = getPartitionTable(cxt);
		const StatementHandler::ClusterRole clusterRole =
				(StatementHandler::CROLE_MASTER |
				StatementHandler::CROLE_FOLLOWER);
		const StatementHandler::PartitionRoleType partitionRole =
				StatementHandler::PROLE_OWNER;
		const StatementHandler::PartitionStatus partitionStatus =
				StatementHandler::PSTATE_ON;
		if (cxt.getExtContext().isOnTransactionService()) {
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
				cxt.getExtContext().getStoreMemoryAgingSwapRate(),
				cxt.getExtContext().getTimeZone());
		return getTransactionManager(cxt)->put(
				alloc, partitionId, TXN_EMPTY_CLIENTID, src,
				ec->getHandlerStartTime(),
				ec->getHandlerStartMonotonicTime());
	}
}

FullContainerKey* SQLContainerImpl::CursorImpl::predicateToContainerKey(
		SQLValues::ValueContext &cxt, TransactionContext &txn,
		DataStore &dataStore, const Expression *pred,
		const ContainerLocation &location, util::String &dbNameStr) {
	util::StackAllocator &alloc = cxt.getAllocator();
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
			StackAllocAutoPtr<BaseContainer> containerAutoPtr(
					alloc, dataStore.getContainer(
							txn, txn.getPartitionId(), containerId,
							ANY_CONTAINER));
			BaseContainer *container = containerAutoPtr.get();
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

const MetaContainerInfo& SQLContainerImpl::CursorImpl::resolveMetaContainerInfo(
		ContainerId id) {
	const bool forCore = true;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	return infoTable.resolveInfo(id, forCore);
}

DataStore* SQLContainerImpl::CursorImpl::getDataStore(OpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getDataStore();
}

EventContext* SQLContainerImpl::CursorImpl::getEventContext(OpContext &cxt) {
	return cxt.getExtContext().getEventContext();
}

TransactionManager* SQLContainerImpl::CursorImpl::getTransactionManager(
		OpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getTransactionManager();
}

TransactionService* SQLContainerImpl::CursorImpl::getTransactionService(
		OpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getTransactionService();
}

PartitionTable* SQLContainerImpl::CursorImpl::getPartitionTable(
		OpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getPartitionTable();
}

ClusterService* SQLContainerImpl::CursorImpl::getClusterService(
		OpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getClusterService();
}

SQLExecutionManager* SQLContainerImpl::CursorImpl::getExecutionManager(
		OpContext &cxt) {
	return cxt.getExtContext().getExecutionManager();
}

SQLService* SQLContainerImpl::CursorImpl::getSQLService(OpContext &cxt) {
	const ResourceSet *resourceSet = getResourceSet(cxt);
	if (resourceSet == NULL) {
		return NULL;
	}
	return resourceSet->getSQLService();
}

const ResourceSet* SQLContainerImpl::CursorImpl::getResourceSet(
		OpContext &cxt) {
	return cxt.getExtContext().getResourceSet();
}

template<RowArrayType RAType, typename T>
const T& SQLContainerImpl::CursorImpl::getFixedValue(
		const ContainerColumn &column) {
	return *static_cast<const T*>(getFixedValueAddr<RAType>(column));
}

template<RowArrayType RAType>
const void* SQLContainerImpl::CursorImpl::getFixedValueAddr(
		const ContainerColumn &column) {
	return getRowArrayImpl<RAType>(true)->getRowCursor().getFixedField(column);
}

template<RowArrayType RAType>
inline const void* SQLContainerImpl::CursorImpl::getVariableArray(
		const ContainerColumn &column) {
	return getRowArrayImpl<RAType>(true)->getRowCursor().getVariableField(
			column);
}

void SQLContainerImpl::CursorImpl::initializeRowArray(
		TransactionContext &txn, BaseContainer &container) {
	rowArray_ = UTIL_MAKE_LOCAL_UNIQUE(rowArray_, RowArray, txn, &container);

	generalRowArray_ = rowArray_->getImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>();
	plainRowArray_ = rowArray_->getImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_PLAIN>();
}

void SQLContainerImpl::CursorImpl::destroyRowArray() {
	generalRowArray_ = NULL;
	plainRowArray_ = NULL;
	rowArray_.reset();

	row_ = NULL;
}

BaseContainer::RowArray* SQLContainerImpl::CursorImpl::getRowArray() {
	assert(rowArray_.get() != NULL);
	return rowArray_.get();
}

template<>
BaseContainer::RowArrayImpl<
		BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>*
SQLContainerImpl::CursorImpl::getRowArrayImpl(bool loaded) {
	static_cast<void>(loaded);
	assert(generalRowArray_ != NULL);
	return generalRowArray_;
}

template<>
BaseContainer::RowArrayImpl<
		BaseContainer, BaseContainer::ROW_ARRAY_PLAIN>*
SQLContainerImpl::CursorImpl::getRowArrayImpl(bool loaded) {
	static_cast<void>(loaded);
	assert(plainRowArray_ != NULL);
	assert(!loaded || getRowArray()->getRowArrayType() ==
			BaseContainer::ROW_ARRAY_PLAIN);
	return plainRowArray_;
}

template<RowArrayType RAType>
void SQLContainerImpl::CursorImpl::setUpScannerHandler(
		OpContext &cxt, const Projection &proj,
		ContainerRowScanner::HandlerSet &handlerSet) {

	typedef ScannerHandler<RAType> HandlerType;
	HandlerType *handler =
			ALLOC_NEW(cxt.getAllocator()) HandlerType(cxt, *this, proj);

	handlerSet.getEntry<RAType>() =
			ContainerRowScanner::createHandlerEntry(*handler);
}

void SQLContainerImpl::CursorImpl::destroyUpdateRowIdHandler() throw() {
	if (updateRowIdHandler_.get() != NULL) {
		updateRowIdHandler_->destroy();
		updateRowIdHandler_.reset();
	}
}

RowId SQLContainerImpl::CursorImpl::getMaxRowId(
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

bool SQLContainerImpl::CursorImpl::isColumnSchemaNullable(
		const ColumnInfo &info) {
	return !info.isNotNull();
}

bool SQLContainerImpl::CursorImpl::updateNullsStats(
		const BaseContainer &container) {
	if (nullsStatsChanged_) {
		return true;
	}

	getNullsStats(container, lastNullsStats_);

	if (initialNullsStats_.empty()) {
		initialNullsStats_.assign(
				lastNullsStats_.begin(), lastNullsStats_.end());
		return true;
	}

	const size_t size = initialNullsStats_.size();
	return (lastNullsStats_.size() == size && memcmp(
			initialNullsStats_.data(), lastNullsStats_.data(), size) == 0);
}

void SQLContainerImpl::CursorImpl::getNullsStats(
		const BaseContainer &container, util::XArray<uint8_t> &nullsStats) {
	const uint32_t columnCount = container.getColumnNum();
	initializeBits(nullsStats, columnCount);
	const size_t bitsSize =  nullsStats.size();

	nullsStats.clear();
	container.getNullsStats(nullsStats);

	if (nullsStats.size() != bitsSize) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLContainerImpl::CursorImpl::initializeBits(
		util::XArray<uint8_t> &bits, size_t bitCount) {
	const uint8_t unitBits = 0;
	bits.assign((bitCount + CHAR_BIT - 1) / CHAR_BIT, unitBits);
}

void SQLContainerImpl::CursorImpl::setBit(
		util::XArray<uint8_t> &bits, size_t index, bool value) {
	uint8_t &unitBits = bits[index / CHAR_BIT];
	const uint8_t targetBit = static_cast<uint8_t>(1 << (index % CHAR_BIT));

	if (value) {
		unitBits |= targetBit;
	}
	else {
		unitBits &= targetBit;
	}
}

bool SQLContainerImpl::CursorImpl::getBit(
		const util::XArray<uint8_t> &bits, size_t index) {
	const uint8_t &unitBits = bits[index / CHAR_BIT];
	const uint8_t targetBit = static_cast<uint8_t>(1 << (index % CHAR_BIT));

	return ((unitBits & targetBit) != 0);
}


template<TupleColumnType T, bool VarSize>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorImpl::DirectValueAccessor<
		T, VarSize>::access(
		SQLValues::ValueContext &cxt, CursorImpl &cursor,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	UTIL_STATIC_ASSERT(!VarSize);
	return cursor.getFixedValue<RAType, Ret>(column);
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorImpl::DirectValueAccessor<
		TupleTypes::TYPE_BOOL, false>::access(
		SQLValues::ValueContext &cxt, CursorImpl &cursor,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	return !!cursor.getFixedValue<RAType, int8_t>(column);
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorImpl::DirectValueAccessor<
		TupleTypes::TYPE_STRING, true>::access(
		SQLValues::ValueContext &cxt, CursorImpl &cursor,
		const ContainerColumn &column) const {
	static_cast<void>(cxt);
	return TupleString(cursor.getVariableArray<RAType>(column));
}

template<>
template<BaseContainer::RowArrayType RAType, typename Ret>
inline Ret SQLContainerImpl::CursorImpl::DirectValueAccessor<
		TupleTypes::TYPE_BLOB, true>::access(
		SQLValues::ValueContext &cxt, CursorImpl &cursor,
		const ContainerColumn &column) const {

	const void *data = cursor.getVariableArray<RAType>(column);
	const BaseObject &baseObject = *cursor.frontFieldCache_;

	return TupleValue::LobBuilder::fromDataStore(
			cxt.getVarContext(), baseObject, data);
}


const uint16_t SQLContainerImpl::Constants::SCHEMA_INVALID_COLUMN_ID =
		std::numeric_limits<uint16_t>::max();
const uint32_t SQLContainerImpl::Constants::ESTIMATED_ROW_ARRAY_ROW_COUNT =
		50;


SQLContainerImpl::ColumnCode::ColumnCode() :
		cursor_(NULL) {
}

void SQLContainerImpl::ColumnCode::initialize(
		ExprFactoryContext &cxt, const Expression *expr,
		const size_t *argIndex) {
	static_cast<void>(expr);

	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLCoreExprs::VariantUtils VariantUtils;

	CursorRef &ref = cxt.getInputSourceRef(0).resolveAs<CursorRef>();

	CursorImpl &cursor = ref.resolveCursor();
	BaseContainer &container = cursor.resolveContainer();

	const ExprCode &code = VariantUtils::getBaseExpression(cxt, argIndex).getCode();

	const bool forId = (code.getType() == SQLType::EXPR_ID);
	column_ = CursorImpl::resolveColumn(container, forId, code.getColumnPos());
	cursor_ = &cursor;
}


template<RowArrayType RAType, typename T, typename S, bool Nullable>
inline typename
SQLContainerImpl::ColumnEvaluator<RAType, T, S, Nullable>::ResultType
SQLContainerImpl::ColumnEvaluator<RAType, T, S, Nullable>::eval(
		ExprContext &cxt) const {
	if (Nullable && code_.cursor_->isValueNull(code_.column_)) {
		return ResultType(ValueType(), true);
	}
	return ResultType(
			static_cast<ValueType>(code_.cursor_->getValueDirect<
					RAType, SourceType, S::COLUMN_TYPE>(
							cxt.getValueContext(), code_.column_)),
			false);
}


SQLContainerImpl::IdExpression::VariantBase::~VariantBase() {
}

void SQLContainerImpl::IdExpression::VariantBase::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
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


SQLContainerImpl::CursorRef::CursorRef(util::StackAllocator &alloc) :
		cursor_(NULL),
		cxtRefList_(alloc),
		exprCxtRefList_(alloc) {
}

SQLContainerImpl::CursorImpl& SQLContainerImpl::CursorRef::resolveCursor() {
	if (cursor_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *cursor_;
}

void SQLContainerImpl::CursorRef::setCursor(CursorImpl *cursor) {
	cursor_ = cursor;
}

void SQLContainerImpl::CursorRef::addContextRef(OpContext **ref) {
	cxtRefList_.push_back(ref);
}

void SQLContainerImpl::CursorRef::addContextRef(ExprContext **ref) {
	exprCxtRefList_.push_back(ref);
}

void SQLContainerImpl::CursorRef::updateAll(OpContext &cxt) {
	for (ContextRefList::const_iterator it = cxtRefList_.begin();
			it != cxtRefList_.end(); ++it) {
		**it = &cxt;
	}
	for (ExprContextRefList::const_iterator it = exprCxtRefList_.begin();
			it != exprCxtRefList_.end(); ++it) {
		**it = &cxt.getExprContext();
	}
}


template<BaseContainer::RowArrayType RAType>
SQLContainerImpl::ScannerHandler<RAType>::ScannerHandler(
		OpContext &cxt, CursorImpl &cursor, const Projection &proj) :
		cxt_(cxt),
		cursor_(cursor),
		proj_(proj) {
}

template<BaseContainer::RowArrayType RAType>
template<BaseContainer::RowArrayType InRAType>
void SQLContainerImpl::ScannerHandler<RAType>::operator()(
		TransactionContext &txn,
		typename BaseContainer::RowArrayImpl<
				BaseContainer, InRAType> &rowArrayImpl) {
	static_cast<void>(txn);
	static_cast<void>(rowArrayImpl);

	const uint32_t outIndex = cursor_.getOutputIndex();
	TupleList::Writer &writer= cxt_.getWriter(outIndex);
	writer.next();

	BaseContainer &container = cursor_.resolveContainer();

	uint32_t pos = 0;
	for (Expression::Iterator it(proj_); it.exists(); it.next()) {
		const bool forId = (it.get().getCode().getType() ==SQLType::EXPR_ID);
		const ContainerColumn &srcColumn = CursorImpl::resolveColumn(
				container, forId, it.get().getCode().getColumnPos());

		cursor_.writeValue<RAType>(
				cxt_.getValueContext(), writer,
				cxt_.getWriterColumn(outIndex, pos), srcColumn, forId);
		pos++;
	}
}

template<BaseContainer::RowArrayType RAType>
bool SQLContainerImpl::ScannerHandler<RAType>::update(
		TransactionContext &txn) {
	static_cast<void>(txn);
	return true;
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
		cursorRef.resolveCursor().updatorList_->push_back(updator);
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

		filterExpr = src.findChild();
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

		const bool withNe = true;
		if (!ExprTypeUtils::isCompOp(condCode.getType(), withNe)) {
			condNum = 0;
			break;
		}
		ExprFactoryContext::Scope scope(exprCxt);
		exprCxt.setBaseExpression(condExpr);

		const size_t baseNum =
				CoreCompExpr::VariantTraits::resolveVariant(exprCxt, condCode);
		if (baseNum < OFFSET_COMP_CONTAINER) {
			condNum = 0;
			break;
		}

		const size_t compFuncNum =
				HandlerVariantUtils::toCompFuncNumber(condCode.getType());
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
			condNum;
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
		OpContext &cxt, CursorImpl &cursor, RowArray &rowArray,
		const Projection &proj) const {
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef ExprCode::InputSourceType InputSourceType;

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));

	ProjectionFactoryContext &projCxt = builder.getProjectionFactoryContext();
	cxt.setUpProjectionFactoryContext(projCxt);

	ContainerRowScanner::HandlerSet handlerSet;
	FactoryContext factoryCxt(&projCxt, &handlerSet);

	ExprFactoryContext &exprCxt = projCxt.getExprFactoryContext();
	exprCxt.setPlanning(false);
	exprCxt.getInputSourceRef(0) =
			ALLOC_UNIQUE(cxt.getAllocator(), CursorRef, cxt.getAllocator());
	exprCxt.getInputSourceRef(0).resolveAs<CursorRef>().setCursor(&cursor);

	const size_t columnCount = exprCxt.getInputColumnCount(0);
	for (size_t i = 0; i < columnCount; i++) {
		if (cursor.isColumnNonNullable(i)) {
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

	exprCxt.getInputSourceRef(0).resolveAs<CursorRef>().updateAll(cxt);

	return *(ALLOC_NEW(cxt.getAllocator()) ContainerRowScanner(
			handlerSet, rowArray, NULL));
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
		oIdList_(alloc),
		mvccOIdList_(alloc),
		asRowArray_(false) {
}


SQLContainerImpl::CursorScope::CursorScope(
		OpContext &cxt, CursorImpl &cursor) :
		cursor_(cursor),
		txn_(CursorImpl::prepareTransactionContext(cxt)),
		txnSvc_(CursorImpl::getTransactionService(cxt)),
		containerAutoPtr_(
				cxt.getAllocator(),
				getContainer(
						txn_, cxt, getStoreLatch(txn_, cxt, latch_),
						cursor_.location_,
						cursor_.containerExpired_,
						cursor_.containerType_)),
		resultSet_(prepareResultSet(
				cxt, txn_,
				checkContainer(
						containerAutoPtr_.get(),
						cursor_.location_,
						cursor_.schemaVersionId_),
				rsGuard_, cursor_)) {
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

void SQLContainerImpl::CursorScope::initialize(OpContext &cxt) {
	BaseContainer *container = containerAutoPtr_.get();

	cursor_.container_ = container;
	cursor_.containerExpired_ = (container == NULL);
	cursor_.resultSetId_ = findResultSetId(resultSet_);

	cursor_.lastPartitionId_ = txn_.getPartitionId();
	cursor_.lastDataStore_ =
			(container == NULL ? NULL : container->getDataStore());

	cursor_.latchTarget_ = LatchTarget(&cursor_);
	cxt.getLatchHolder().reset(&cursor_.latchTarget_);

	if (container == NULL) {
		return;
	}

	cursor_.containerType_ = container->getContainerType();
	cursor_.schemaVersionId_ = container->getVersionId();

	cursor_.initializeScanRange(txn_, *container);
	cursor_.initializeRowArray(txn_, *container);

	DataStore *dataStore = container->getDataStore();
	ObjectManager *objectManager = dataStore->getObjectManager();
	cursor_.frontFieldCache_ = UTIL_MAKE_LOCAL_UNIQUE(
			cursor_.frontFieldCache_, BaseObject, txn_.getPartitionId(),
			*objectManager);
}

void SQLContainerImpl::CursorScope::clear(bool force) {
	if (containerAutoPtr_.get() == NULL) {
		return;
	}
	assert(cursor_.container_ != NULL);

	cursor_.destroyRowArray();
	cursor_.frontFieldCache_.reset();
	cursor_.container_ = NULL;

	clearResultSet(force);
}

void SQLContainerImpl::CursorScope::clearResultSet(bool force) {
	if (resultSet_ == NULL) {
		return;
	}

	bool closeable = (force || resultSet_->isRelease());
	if (!closeable && cursor_.resultSetHolder_.isEmpty()) {
		assert(txnSvc_ != NULL);
		assert(cursor_.lastPartitionId_ != UNDEF_PARTITIONID);
		try {
			cursor_.resultSetHolder_.assign(
					txnSvc_->getResultSetHolderManager(),
					cursor_.lastPartitionId_,
					cursor_.resultSetId_);
		}
		catch (...) {
			cursor_.resultSetLost_ = true;
			closeable = true;
		}
	}

	if (closeable) {
		resultSet_->setResultType(RESULT_ROWSET);
		resultSet_->setPartialMode(false);

		cursor_.resultSetId_ = UNDEF_RESULTSETID;
		cursor_.resultSetPreserving_ = false;
		cursor_.resultSetHolder_.release();

		cursor_.lastPartitionId_ = UNDEF_PARTITIONID;
		cursor_.lastDataStore_ = NULL;
		cursor_.destroyUpdateRowIdHandler();
	}

	rsGuard_.reset();
	resultSet_ = NULL;
}

const DataStore::Latch* SQLContainerImpl::CursorScope::getStoreLatch(
		TransactionContext &txn, OpContext &cxt,
		util::LocalUniquePtr<DataStore::Latch> &latch) {
	if (!cxt.getExtContext().isOnTransactionService()) {
		return NULL;
	}

	latch = UTIL_MAKE_LOCAL_UNIQUE(
			latch, DataStore::Latch,
			txn, txn.getPartitionId(),
			CursorImpl::getDataStore(cxt),
			CursorImpl::getClusterService(cxt));
	return latch.get();
}

BaseContainer* SQLContainerImpl::CursorScope::getContainer(
		TransactionContext &txn, OpContext &cxt,
		const DataStore::Latch *latch, const ContainerLocation &location,
		bool expired, ContainerType type) {
	if (latch == NULL || location.type_ != SQLType::TABLE_CONTAINER ||
			expired) {
		return NULL;
	}

	DataStore *dataStore = CursorImpl::getDataStore(cxt);
	const PartitionId partitionId = txn.getPartitionId();
	const ContainerType requiredType =
			(type == UNDEF_CONTAINER ? ANY_CONTAINER : type);

	try {
		return dataStore->getContainer(
				txn, partitionId, location.id_, requiredType);
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
		BaseContainer *container, const ContainerLocation &location,
		SchemaVersionId schemaVersionId) {
	if (container == NULL && (location.type_ != SQLType::TABLE_CONTAINER ||
			location.expirable_)) {
		return NULL;
	}

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
		OpContext &cxt, TransactionContext &txn, BaseContainer *container,
		util::LocalUniquePtr<ResultSetGuard> &rsGuard,
		const CursorImpl &cursor) {
	if (container == NULL) {
		return NULL;
	}

	DataStore *dataStore = CursorImpl::getDataStore(cxt);
	assert(dataStore != NULL);

	TransactionService *txnSvc = CursorImpl::getTransactionService(cxt);
	assert(txnSvc != NULL);

	txnSvc->getResultSetHolderManager().closeAll(txn, *dataStore);

	const int64_t emNow =
			CursorImpl::getEventContext(cxt)->getHandlerStartMonotonicTime();

	if (cursor.closed_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (cursor.resultSetLost_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	ResultSet *resultSet;
	if (cursor.resultSetId_ == UNDEF_RESULTSETID) {
		const bool noExpire = true;
		resultSet = dataStore->createResultSet(
				txn, container->getContainerId(), container->getVersionId(),
				emNow, NULL, noExpire);
		resultSet->setUpdateIdType(ResultSet::UPDATE_ID_NONE);
	}
	else {
		resultSet = dataStore->getResultSet(txn, cursor.resultSetId_);
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
	TupleList::Writer &writer= cxt_.getWriter(outIndex);
	writer.next();

	TupleValue value;
	uint32_t pos = 0;
	for (Expression::Iterator it(proj_); it.exists(); it.next()) {
		const Value &srcValue = valueList[it.get().getCode().getColumnPos()];
		ContainerValueUtils::toTupleValue(alloc, srcValue, value, true);
		CursorImpl::writeValueAs<TupleTypes::TYPE_ANY>(
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
		OpContext &cxt, RowId maxRowId) :
		shared_(UTIL_OBJECT_POOL_NEW(sharedPool_) Shared(cxt, maxRowId)) {
}

SQLContainerImpl::TupleUpdateRowIdHandler::~TupleUpdateRowIdHandler() {
	close();
}

void SQLContainerImpl::TupleUpdateRowIdHandler::destroy() throw() {
	try {
		if (shared_ != NULL) {
			shared_->cxt_.reset();
		}
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

	shared_->cxt_.reset();

	assert(shared_->refCount_ > 0);
	if (--shared_->refCount_ == 0) {
		UTIL_OBJECT_POOL_DELETE(sharedPool_, shared_);
	}

	shared_ = NULL;
}

void SQLContainerImpl::TupleUpdateRowIdHandler::operator()(
		RowId rowId, uint64_t id, ResultSet::UpdateOperation op) {
	static_cast<void>(id);

	if (shared_ == NULL || shared_->cxt_.get() == NULL) {
		return;
	}

	if (rowId > shared_->maxRowId_) {
		return;
	}

	OpContext &cxt = *shared_->cxt_;
	const IdStoreColumnList &columnList = shared_->columnList_;
	const uint32_t index = shared_->rowIdStoreIndex_;
	{
		TupleList::Writer &writer = cxt.getWriter(index);
		writer.next();
		writer.get().set(
				columnList[0], SQLValues::ValueUtils::toAnyByNumeric(rowId));
	}
	cxt.releaseWriterLatch(index);
}

bool SQLContainerImpl::TupleUpdateRowIdHandler::getRowIdList(
		util::XArray<RowId> &list, size_t limit) {
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
		TupleList::Reader &reader = cxt.getLocalReader(index);
		for (; reader.exists(); reader.next(), --rest) {
			if (rest <= 0) {
				extra = true;
				break;
			}
			list.push_back(SQLValues::ValueUtils::getValue<RowId>(
					reader.get().get(columnList[0])));
		}
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
		TupleTypes::TYPE_LONG
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

	TupleList::Reader &reader = cxt.getLocalReader(srcIndex);
	TupleList::Writer &writer = cxt.getWriter(destIndex);

	for (; reader.exists(); reader.next()) {
		writer.next();
		for (size_t i = 0; i < columnCount; i++) {
			writer.get().set(columnList[i], reader.get().get(columnList[i]));
		}
	}
}

SQLContainerImpl::TupleUpdateRowIdHandler::Shared::Shared(
		OpContext &cxt, RowId maxRowId) :
		refCount_(1),
		rowIdStoreIndex_(createRowIdStore(
				cxt_, cxt.getSource(), columnList_, extraStoreIndex_)),
		maxRowId_(maxRowId) {
}


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

	switch (ValueProcessor::getSimpleColumnType(src)) {
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

