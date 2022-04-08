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
	@brief Implementation of ResultSet
*/
#include "result_set.h"
#include "base_container.h"

ResultSet::ResultSet()
	: rsAlloc_(NULL),
	  txnAlloc_(NULL),
	  oIdList_(NULL),
	  serializedRowList_(NULL),
	  serializedVarDataList_(NULL),
	  rowIdList_(NULL),
	  rsSerializedRowList_(NULL),
	  rsSerializedVarDataList_(NULL),
	  container_(NULL),
	  rsetId_(UNDEF_RESULTSETID),
	  txnId_(UNDEF_TXNID),
	  timeoutTimestamp_(MAX_TIMESTAMP),
	  startLsn_(UNDEF_LSN),
	  pId_(UNDEF_PARTITIONID),
	  containerId_(UNDEF_CONTAINERID),
	  schemaVersionId_(UNDEF_SCHEMAVERSIONID),
	  resultNum_(0),
	  fetchNum_(0),
	  memoryLimit_(0),
	  fixedStartPos_(0),
	  fixedOffsetSize_(0),
	  varStartPos_(0),
	  varOffsetSize_(0),
	  skipCount_(0),
	  rowScanner_(NULL),
	  resultType_(RESULT_NONE),
	  isId_(false),
	  isRowIdInclude_(false),
	  useRSRowImage_(false),
	  distributedTarget_(NULL),
	  distributedTargetUncovered_(false),
	  distributedTargetReduced_(false),
	  rsRowIdAlloc_(NULL),
	  rsSwapAlloc_(NULL),
	  partialExecState_(NOT_PARTIAL),
	  partialReturnCount_(0),
	  partialExecCount_(0),
	  queryObj_(NULL),
	  suspendKeyBuffer_(NULL),
	  suspendValueBuffer_(NULL),
	  updateRowIdList_(NULL),
	  swapRowIdList_(NULL),
	  updateObjectIdList_(NULL),
	  updateOperationList_(NULL),
	  updateRowIdHandler_(NULL),
	  updateRowIdListThreshold_(std::numeric_limits<size_t>::max()),
	  updateRowIdListInvalid_(false),
	  isRowIdSorted_(false),
	  updateIdType_(UPDATE_ID_ROW)
{
}

ResultSet::~ResultSet() {
	if (rsAlloc_) {
		ALLOC_DELETE((*rsAlloc_), rsSerializedRowList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedVarDataList_);
		ALLOC_DELETE((*rsAlloc_), distributedTarget_);
		ALLOC_DELETE((*rsAlloc_), suspendKeyBuffer_);
		ALLOC_DELETE((*rsAlloc_), suspendValueBuffer_);
		ALLOC_DELETE((*rsAlloc_), updateRowIdList_);
		ALLOC_DELETE((*rsAlloc_), updateObjectIdList_);
		ALLOC_DELETE((*rsAlloc_), updateOperationList_);
		ALLOC_DELETE((*rsAlloc_), partialExecInfo_.columnIds_);
	}
	if (rsRowIdAlloc_) {
		ALLOC_DELETE((*rsRowIdAlloc_), rowIdList_);
	}
	if (rsSwapAlloc_) {
		ALLOC_DELETE((*rsSwapAlloc_), swapRowIdList_);
	}
	setUpdateRowIdHandler(NULL, 0);
}

/*!
	@brief Clear temporary data for Message
*/
void ResultSet::resetMessageBuffer() {
	if (useRSRowImage_) {
		if (rsSerializedRowList_) {
			rsSerializedRowList_->clear();
			rsSerializedVarDataList_->clear();
		}
	}
	else {
		resetSerializedData();
	}
	fixedStartPos_ = 0;
	fixedOffsetSize_ = 0;
	varStartPos_ = 0;
	varOffsetSize_ = 0;
}

/*!
	@brief Reset transaction allocator memory
*/
void ResultSet::resetSerializedData() {
	assert(txnAlloc_);
	if (serializedRowList_) {
		serializedRowList_->clear();
	}
	else {
		serializedRowList_ =
			ALLOC_NEW(*txnAlloc_) util::XArray<uint8_t>(*txnAlloc_);
	}
	if (serializedVarDataList_) {
		serializedVarDataList_->clear();
	}
	else {
		serializedVarDataList_ =
			ALLOC_NEW(*txnAlloc_) util::XArray<uint8_t>(*txnAlloc_);
	}
	txnAlloc_->trim();
}

/*!
	@brief Clear all data
*/
void ResultSet::clear() {

	if (txnAlloc_) {
		oIdList_ = NULL;
		serializedRowList_ = NULL;
		serializedVarDataList_ = NULL;
		txnAlloc_ = NULL;
	}
	if (rsAlloc_) {
		ALLOC_DELETE((*rsAlloc_), rsSerializedRowList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedVarDataList_);
		ALLOC_DELETE((*rsAlloc_), distributedTarget_);
		ALLOC_DELETE((*rsAlloc_), suspendKeyBuffer_);
		ALLOC_DELETE((*rsAlloc_), suspendValueBuffer_);
		ALLOC_DELETE((*rsAlloc_), updateRowIdList_);
		ALLOC_DELETE((*rsAlloc_), updateObjectIdList_);
		ALLOC_DELETE((*rsAlloc_), updateOperationList_);
		ALLOC_DELETE((*rsAlloc_), partialExecInfo_.columnIds_);
	}
	if (rsRowIdAlloc_) {
		ALLOC_DELETE((*rsRowIdAlloc_), rowIdList_);
	}
	if (rsSwapAlloc_) {
		ALLOC_DELETE((*rsSwapAlloc_), swapRowIdList_);
	}
	rowIdList_ = NULL;
	rsSerializedRowList_ = NULL;
	rsSerializedVarDataList_ = NULL;
	distributedTarget_ = NULL;
	distributedTargetUncovered_ = false;
	distributedTargetReduced_ = false;
	suspendKeyBuffer_ = NULL;
	suspendValueBuffer_ = NULL;
	updateRowIdList_ = NULL;
	swapRowIdList_ = NULL;
	updateObjectIdList_ = NULL;
	updateOperationList_ = NULL;
	setUpdateRowIdHandler(NULL, 0);
	updateRowIdListInvalid_ = false;
	updateIdType_ = UPDATE_ID_ROW;
	isRowIdSorted_ = false;
	partialExecInfo_ = PartialExecInfo();

	rsetId_ = UNDEF_RESULTSETID;
	txnId_ = UNDEF_TXNID;
	timeoutTimestamp_ = MAX_TIMESTAMP;
	startLsn_ = UNDEF_LSN;

	pId_ = UNDEF_PARTITIONID;
	containerId_ = UNDEF_CONTAINERID;
	schemaVersionId_ = UNDEF_SCHEMAVERSIONID;
	resultNum_ = 0;
	fetchNum_ = 0;
	memoryLimit_ = 0;

	fixedStartPos_ = 0;
	fixedOffsetSize_ = 0;
	varStartPos_ = 0;
	varOffsetSize_ = 0;
	skipCount_ = 0;

	resultType_ = RESULT_NONE;
	isId_ = false;
	isRowIdInclude_ = false;
	useRSRowImage_ = false;

	rsAlloc_->setFreeSizeLimit(rsAlloc_->base().getElementSize());
	rsAlloc_->trim();
	rsRowIdAlloc_->setFreeSizeLimit(rsRowIdAlloc_->base().getElementSize());
	rsRowIdAlloc_->trim();
	rsSwapAlloc_->setFreeSizeLimit(rsSwapAlloc_->base().getElementSize());
	rsSwapAlloc_->trim();
	queryOption_ = ResultSetOption();
}

/*!
	@brief Release transaction allocator
*/
void ResultSet::releaseTxnAllocator() {
	if (txnAlloc_) {
		oIdList_ = NULL;
		serializedRowList_ = NULL;
		serializedVarDataList_ = NULL;
		txnAlloc_ = NULL;
	}
}

/*!
	@brief Set position and offset of data to send
*/
void ResultSet::setDataPos(uint64_t fixedStartPos, uint64_t fixedOffsetSize,
	uint64_t varStartPos, uint64_t varOffsetSize) {
	fixedStartPos_ = fixedStartPos;
	fixedOffsetSize_ = fixedOffsetSize;
	varStartPos_ = varStartPos;
	varOffsetSize_ = varOffsetSize;
}

const util::XArray<OId>* ResultSet::getOIdList() const {
	return oIdList_;
}

util::XArray<OId>* ResultSet::getOIdList() {
	assert(txnAlloc_);
	if (oIdList_ == NULL) {
		oIdList_ = ALLOC_NEW(*txnAlloc_) util::XArray<OId>(*txnAlloc_);
	}
	return oIdList_;
}

const util::XArray<uint8_t>* ResultSet::getRowDataFixedPartBuffer() const {
	return serializedRowList_;
}

util::XArray<uint8_t>* ResultSet::getRowDataFixedPartBuffer() {
	assert(txnAlloc_);
	if (serializedRowList_ == NULL) {
		serializedRowList_ =
			ALLOC_NEW(*txnAlloc_) util::XArray<uint8_t>(*txnAlloc_);
	}
	return serializedRowList_;
}

const util::XArray<uint8_t>* ResultSet::getRowDataVarPartBuffer() const {
	return serializedVarDataList_;
}

util::XArray<uint8_t>* ResultSet::getRowDataVarPartBuffer() {
	assert(txnAlloc_);
	if (serializedVarDataList_ == NULL) {
		serializedVarDataList_ =
			ALLOC_NEW(*txnAlloc_) util::XArray<uint8_t>(*txnAlloc_);
	}
	return serializedVarDataList_;
}

const util::XArray<RowId>* ResultSet::getRowIdList() const {
	return rowIdList_;
}

util::XArray<RowId>* ResultSet::getRowIdList() {
	assert(rsRowIdAlloc_);
	if (rowIdList_ == NULL) {
		rowIdList_ =
			ALLOC_NEW(*rsRowIdAlloc_) util::XArray<RowId>(*rsRowIdAlloc_);
	}
	return rowIdList_;
}

const util::XArray<uint8_t>* ResultSet::getRSRowDataFixedPartBuffer() const {
	return rsSerializedRowList_;
}

util::XArray<uint8_t>* ResultSet::getRSRowDataFixedPartBuffer() {
	assert(rsAlloc_);
	if (rsSerializedRowList_ == NULL) {
		rsSerializedRowList_ =
			ALLOC_NEW(*rsAlloc_) util::XArray<uint8_t>(*rsAlloc_);
	}
	return rsSerializedRowList_;
}

const util::XArray<uint8_t>* ResultSet::getRSRowDataVarPartBuffer() const {
	return rsSerializedVarDataList_;
}

util::XArray<uint8_t>* ResultSet::getRSRowDataVarPartBuffer() {
	assert(rsAlloc_);
	if (rsSerializedVarDataList_ == NULL) {
		rsSerializedVarDataList_ =
			ALLOC_NEW(*rsAlloc_) util::XArray<uint8_t>(*rsAlloc_);
	}
	return rsSerializedVarDataList_;
}

/*!
	@brief Check if data is releasable
*/
bool ResultSet::isRelease() const {
	if (isPartialExecuteMode()) {
		return !isPartialExecuteSuspend();
	}
	else {
		return (resultType_ != RESULT_ROW_ID_SET) &&
			   (resultType_ != PARTIAL_RESULT_ROWSET);
	}
}

const util::Vector<int64_t>* ResultSet::getDistributedTarget() const {
	return distributedTarget_;
}

util::Vector<int64_t>* ResultSet::getDistributedTarget() {
	assert(rsAlloc_);
	if (distributedTarget_ == NULL) {
		distributedTarget_ =
				ALLOC_NEW(*rsAlloc_) util::Vector<int64_t>(*rsAlloc_);
	}
	return distributedTarget_;
}

void ResultSet::getDistributedTargetStatus(
		bool &uncovered, bool &reduced) const {
	uncovered = distributedTargetUncovered_;
	reduced = distributedTargetReduced_;
}

void ResultSet::setDistributedTargetStatus(bool uncovered, bool reduced) {
	distributedTargetUncovered_ = uncovered;
	distributedTargetReduced_ = reduced;
}

const util::XArray<RowId>* ResultSet::getUpdateRowIdList() const {
	return updateRowIdList_;
}

util::XArray<RowId>* ResultSet::getUpdateRowIdList() {
	assert(rsAlloc_);
	if (updateRowIdList_ == NULL) {
		updateRowIdList_ = ALLOC_NEW(*rsAlloc_) util::XArray<RowId>(*rsAlloc_);
	}
	return updateRowIdList_;
}

const util::XArray<RowId>* ResultSet::getSwapRowIdList() const {
	return swapRowIdList_;
}

util::XArray<RowId>* ResultSet::getSwapRowIdList() {
	assert(rsSwapAlloc_);
	if (swapRowIdList_ != NULL) {
		rsSwapAlloc_->setFreeSizeLimit(rsSwapAlloc_->base().getElementSize());
		rsSwapAlloc_->trim();
	}
	swapRowIdList_ =
		ALLOC_NEW(*rsSwapAlloc_) util::XArray<RowId>(*rsSwapAlloc_);
	return swapRowIdList_;
}

const util::XArray<uint64_t>* ResultSet::getUpdateObjectIdList() const {
	return updateObjectIdList_;
}

util::XArray<uint64_t>* ResultSet::getUpdateObjectIdList() {
	assert(rsAlloc_);
	if (updateObjectIdList_ == NULL) {
		updateObjectIdList_ =
				ALLOC_NEW(*rsAlloc_) util::XArray<uint64_t>(*rsAlloc_);
	}
	return updateObjectIdList_;
}

const util::XArray<int8_t>* ResultSet::getUpdateOperationList() const {
	return updateOperationList_;
}

util::XArray<int8_t>* ResultSet::getUpdateOperationList() {
	assert(rsAlloc_);
	if (updateOperationList_ == NULL) {
		updateOperationList_ =
				ALLOC_NEW(*rsAlloc_) util::XArray<int8_t>(*rsAlloc_);
	}
	return updateOperationList_;
}

void ResultSet::setLargeInfo(const SQLTableInfo *largeInfo) {
	queryOption_.setLargeInfo(largeInfo);
}

const SQLTableInfo* ResultSet::getLargeInfo() const {
	return queryOption_.getLargeInfo();
}

void ResultSet::resetPartialContext() {
	partialExecInfo_.startOrPos_ = 0;
	if (partialExecInfo_.columnIds_ != NULL) {
		partialExecInfo_.columnIds_->clear();
	}
	partialExecInfo_.indexType_ = MAP_TYPE_BTREE;
	partialExecInfo_.suspendKey_ = NULL;
	partialExecInfo_.suspendKeySize_ = 0;
	partialExecInfo_.suspendValue_ = NULL;
	partialExecInfo_.suspendValueSize_ = 0;
	partialExecInfo_.suspendRowId_ = UNDEF_ROWID;
}

/*!
	@brief Set the information of suspending
*/
void ResultSet::setPartialContext(BtreeMap::SearchContext& sc,
	ResultSize& currentSuspendLimit, uint32_t orPos, MapType indexType) {
	resetPartialContext();
	if (sc.isSuspended()) {
		partialExecInfo_.startOrPos_ = orPos;

		const util::Vector<ColumnId> &columnIds = sc.getColumnIds();
		if (partialExecInfo_.columnIds_ == NULL) {
			partialExecInfo_.columnIds_ =
					ALLOC_NEW(*rsAlloc_) util::Vector<ColumnId>(*rsAlloc_);
		} else {
			partialExecInfo_.columnIds_->clear();
		}
		partialExecInfo_.columnIds_->assign(columnIds.begin(), columnIds.end());

		partialExecInfo_.indexType_ = indexType;
		partialExecInfo_.isNullSuspended_ = sc.isNullSuspended();

		if (suspendKeyBuffer_ == NULL) {
			suspendKeyBuffer_ =
					ALLOC_NEW(*rsAlloc_) util::XArray<uint8_t>(*rsAlloc_);
		}
		suspendKeyBuffer_->resize(sc.getSuspendKeySize());
		partialExecInfo_.suspendKey_ = suspendKeyBuffer_->data();
		memcpy(partialExecInfo_.suspendKey_, sc.getSuspendKey(),
			sc.getSuspendKeySize());
		partialExecInfo_.suspendKeySize_ = sc.getSuspendKeySize();

		if (suspendValueBuffer_ == NULL) {
			suspendValueBuffer_ =
					ALLOC_NEW(*rsAlloc_) util::XArray<uint8_t>(*rsAlloc_);
		}
		suspendValueBuffer_->resize(sc.getSuspendValueSize());
		partialExecInfo_.suspendValue_ = suspendValueBuffer_->data();
		memcpy(partialExecInfo_.suspendValue_, sc.getSuspendValue(),
			sc.getSuspendValueSize());
		partialExecInfo_.suspendValueSize_ = sc.getSuspendValueSize();

		partialExecInfo_.suspendRowId_ = sc.getSuspendRowId();
	}
	else if (isPartialExecuteMode()) {
		currentSuspendLimit = sc.getSuspendLimit();
	}
}
/*!
	@brief Set the information of resuming
*/
void ResultSet::rewritePartialContext(TransactionContext &txn,
	BtreeMap::SearchContext& sc,
	ResultSize currentSuspendLimit, OutputOrder outputOrder, uint32_t orPos,
	BaseContainer &container, uint16_t unitRowNum, bool isResumeRewrite) {
	ContainerType containerType = container.getContainerType();
	if (unitRowNum == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, 
			"Invalid unitRowNum of partial execution : " << unitRowNum);
	}

	bool isRowIdIndex = sc.getKeyColumnNum() == 1 && 
		sc.getScColumnId() == container.getRowIdColumnId();
	assert(!(containerType == TIME_SERIES_CONTAINER && 
		sc.getKeyColumnNum() == 1 && sc.getScColumnId() == UNDEF_COLUMNID));
	if (currentSuspendLimit >= unitRowNum * 2 && isRowIdIndex) {
		currentSuspendLimit = currentSuspendLimit / unitRowNum;
	}
	sc.setSuspendLimit(currentSuspendLimit);

	if (getPartialExecuteCount() != 0 &&
		orPos == partialExecInfo_.startOrPos_ && isResumeRewrite) {
		if (partialExecInfo_.isNullSuspended_) {
			sc.setResumeStatus(BaseIndex::SearchContext::NULL_RESUME);
		} else {
			sc.setResumeStatus(BaseIndex::SearchContext::NOT_NULL_RESUME);
		}

		sc.setSuspendKey(partialExecInfo_.suspendKey_);
		sc.setSuspendKeySize(partialExecInfo_.suspendKeySize_);
		sc.setSuspendValue(partialExecInfo_.suspendValue_);
		sc.setSuspendValueSize(partialExecInfo_.suspendValueSize_);

		if (isRowIdIndex) {
			DSExpression::Operation opType = DSExpression::GE;
			if (outputOrder == ORDER_DESCENDING) {
				opType = DSExpression::LE;
			}
			TermCondition newCondition(container.getRowIdColumnType(), container.getRowIdColumnType(),
				opType, container.getRowIdColumnId(),
				partialExecInfo_.suspendKey_, partialExecInfo_.suspendKeySize_);
			sc.updateCondition(txn, newCondition);
		}
	}
	if (containerType == TIME_SERIES_CONTAINER) {
		if (partialExecInfo_.suspendRowId_ != UNDEF_ROWID && !isRowIdIndex) {
			TermCondition newCondition(container.getRowIdColumnType(), container.getRowIdColumnType(),
				DSExpression::GE, container.getRowIdColumnId(),
				&(partialExecInfo_.suspendRowId_), sizeof(partialExecInfo_.suspendRowId_));
			sc.updateCondition(txn, newCondition);
			sc.setSuspendRowId(partialExecInfo_.suspendRowId_);
		}
		if (partialExecInfo_.lastRowId_ != UNDEF_ROWID) {
			TermCondition newCondition(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
				DSExpression::LE, ColumnInfo::ROW_KEY_COLUMN_ID,
				&(partialExecInfo_.lastRowId_), sizeof(partialExecInfo_.lastRowId_));
			sc.updateCondition(txn, newCondition);
		}
	}
	else {
		if (partialExecInfo_.lastRowId_ != UNDEF_ROWID) {
			TermCondition newCondition(container.getRowIdColumnType(), container.getRowIdColumnType(),
				DSExpression::LE, container.getRowIdColumnId(),
				&(partialExecInfo_.lastRowId_), sizeof(partialExecInfo_.lastRowId_));
			sc.updateCondition(txn, newCondition);
		}
	}
}

void ResultSet::setUpdateRowIdHandler(
		UpdateRowIdHandler *handler, size_t threshold) {
	if (handler == updateRowIdHandler_) {
		return;
	}

	if (updateRowIdHandler_ != NULL) {
		updateRowIdHandler_->close();
	}
	updateRowIdHandler_ = handler;

	updateRowIdListThreshold_ =
			(handler == NULL ? std::numeric_limits<size_t>::max() : threshold);
}

void ResultSet::handleUpdateRowIdError(std::exception &e) {
	invalidateUpdateRowIdList();
	UTIL_TRACE_EXCEPTION(DATA_STORE, e, "");
}



ResultSetManager::ResultSetManager(util::StackAllocator* stAlloc,
	util::FixedSizeAllocator<util::Mutex>* memoryPool,
	ObjectManagerV4* objectManager, uint32_t rsCacheSize) :
	objectManager_(objectManager),
	resultSetPool_(memoryPool),
	resultSetId_(1),
	resultSetAllocator_(NULL),
	resultSetRowIdAllocator_(NULL),
	resultSetSwapAllocator_(NULL),
	resultSetMapManager_(NULL),
	resultSetMap_(NULL)
{
	try {
		if (rsCacheSize > 0) {
			resultSetPool_->setLimit(
				util::AllocatorStats::STAT_STABLE_LIMIT,
				ConfigTable::megaBytesToBytes(rsCacheSize));
		}
		else {
			resultSetPool_->setFreeElementLimit(0);
		}

		resultSetAllocator_ = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSet"),
			resultSetPool_);
		resultSetRowIdAllocator_ = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(
				ALLOCATOR_GROUP_TXN_RESULT, "resultSetRowId"),
			resultSetPool_);
		resultSetSwapAllocator_ = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(
				ALLOCATOR_GROUP_TXN_RESULT, "resultSetSwap"),
			resultSetPool_);
		resultSetMapManager_ = UTIL_NEW util::ExpirableMap<ResultSetId,
			ResultSet, int64_t, ResultSetIdHash>::
			Manager(util::AllocatorInfo(
				ALLOCATOR_GROUP_TXN_RESULT, "resultSetMapManager"));
		resultSetMapManager_->setFreeElementLimit(
			RESULTSET_FREE_ELEMENT_LIMIT);
		resultSetMap_ =
			resultSetMapManager_->create(RESULTSET_MAP_HASH_SIZE,
				DS_MAX_RESULTSET_TIMEOUT_INTERVAL * 1000, 1000);
	}
	catch (std::exception& e) {
		resultSetMapManager_->remove(resultSetMap_);
		delete resultSetMapManager_;
		delete resultSetAllocator_;
		delete resultSetRowIdAllocator_;
		delete resultSetSwapAllocator_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

ResultSetManager::~ResultSetManager() {
	forceCloseAll();

	resultSetMapManager_->remove(resultSetMap_);
	delete resultSetMapManager_;
	delete resultSetAllocator_;
	delete resultSetRowIdAllocator_;
	delete resultSetSwapAllocator_;
}

/*!
	@brief Create ResultSet
*/

ResultSet* ResultSetManager::create(TransactionContext& txn,
	ContainerId containerId, SchemaVersionId schemaVersionId, int64_t emNow,
	ResultSetOption* queryOption, bool noExpire) {
	ResultSetId rsId = ++resultSetId_;
	int32_t rsTimeout = txn.getTransationTimeoutInterval();
	const int64_t timeout = emNow + static_cast<int64_t>(rsTimeout) * 1000;
	ResultSet& rs = (noExpire ?
		resultSetMap_->createNoExpire(rsId) :
		resultSetMap_->create(rsId, timeout));
	rs.setTxnAllocator(&txn.getDefaultAllocator());
	if (resultSetAllocator_) {
		rs.setRSAllocator(resultSetAllocator_);
		resultSetAllocator_ = NULL;
	}
	else {
		util::StackAllocator* allocator = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSet"),
			resultSetPool_);
		rs.setRSAllocator(allocator);
	}
	if (resultSetRowIdAllocator_) {
		rs.setRSRowIdAllocator(resultSetRowIdAllocator_);
		resultSetRowIdAllocator_ = NULL;
	}
	else {
		util::StackAllocator* allocatorRowId = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetRowId"),
			resultSetPool_);
		rs.setRSRowIdAllocator(allocatorRowId);
	}
	if (resultSetSwapAllocator_) {
		rs.setRSSwapAllocator(resultSetSwapAllocator_);
		resultSetSwapAllocator_ = NULL;
	}
	else {
		util::StackAllocator* allocatorSwap = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetSwap"),
			resultSetPool_);
		rs.setRSSwapAllocator(allocatorSwap);
	}
	rs.getRSAllocator()->setTotalSizeLimit(rs.getTxnAllocator()->getTotalSizeLimit());
	rs.setMemoryLimit(rs.getTxnAllocator()->getTotalSizeLimit());
	rs.setContainerId(containerId);
	rs.setSchemaVersionId(schemaVersionId);
	rs.setId(rsId);
	rs.setStartLsn(0);
	rs.setTimeoutTime(rsTimeout);
	rs.setPartitionId(txn.getPartitionId());
	rs.setQueryOption(queryOption);
	return &rs;
}

/*!
	@brief Get ResultSet
*/
ResultSet* ResultSetManager::get(TransactionContext& txn, ResultSetId rsId) {
	ResultSet* rs = resultSetMap_->get(rsId);
	if (rs) {
		rs->setTxnAllocator(&txn.getDefaultAllocator());
		ResultSetOption& queryOption = rs->getQueryOption();
		objectManager_->setSwapOutCounter(queryOption.getSwapOutNum());
	}
	return rs;
}

/*!
	@brief Close ResultSet
*/
void ResultSetManager::close(ResultSetId rsId) {
	if (rsId == UNDEF_RESULTSETID) {
		return;
	}
	ResultSet* rs = resultSetMap_->get(rsId);
	if (rs) {
		closeInternal(*rs);
	}
}

/*!
	@brief If ResultSet is no need, then clse , othewise clear temporary memory
*/
void ResultSetManager::closeOrClear(ResultSetId rsId) {
	if (rsId == UNDEF_RESULTSETID) {
		return;
	}
	ResultSet* rs = resultSetMap_->get(rsId);
	if (rs) {
		rs->releaseTxnAllocator();
		ResultSetOption& queryOption = rs->getQueryOption();
		queryOption.setSwapOutNum(objectManager_->getSwapOutCounter());
	}
	if (rs && (rs->isRelease())) {
		closeInternal(*rs);
	}
}

void ResultSetManager::closeInternal(ResultSet & rs) {
	ResultSetId removeRSId = rs.getId();
	util::StackAllocator* allocator = rs.getRSAllocator();
	util::StackAllocator* allocatorRowId = rs.getRSRowIdAllocator();
	util::StackAllocator* allocatorSwap = rs.getRSSwapAllocator();
	rs.clear();				  
	rs.setRSAllocator(NULL);  
	rs.setRSRowIdAllocator(NULL);  
	rs.setRSSwapAllocator(NULL);   

	util::StackAllocator::Tool::forceReset(*allocator);
	util::StackAllocator::Tool::forceReset(*allocatorRowId);
	util::StackAllocator::Tool::forceReset(*allocatorSwap);
	allocator->setFreeSizeLimit(allocator->base().getElementSize());
	allocatorRowId->setFreeSizeLimit(allocatorRowId->base().getElementSize());
	allocatorSwap->setFreeSizeLimit(allocatorSwap->base().getElementSize());
	allocator->trim();
	allocatorRowId->trim();
	allocatorSwap->trim();

	delete resultSetAllocator_;
	resultSetAllocator_ = allocator;
	delete resultSetRowIdAllocator_;
	resultSetRowIdAllocator_ = allocatorRowId;
	delete resultSetSwapAllocator_;
	resultSetSwapAllocator_ = allocatorSwap;

	resultSetMap_->remove(removeRSId);
}

/*!
	@brief Check if ResultSet is timeout
*/
void ResultSetManager::checkTimeout(
	int64_t checkTime) {
	ResultSetId rsId = UNDEF_RESULTSETID;
	ResultSetId* rsIdPtr = &rsId;
	ResultSet* rs = resultSetMap_->refresh(checkTime, rsIdPtr);
	while (rs) {
		closeInternal(*rs);
		rs = resultSetMap_->refresh(checkTime, rsIdPtr);
	}
}

void ResultSetManager::addUpdatedRow(ContainerId containerId, RowId rowId, OId oId, bool isMvccRow) {
	if (resultSetMap_->size() == 0) {
		return;
	}

	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_->getCursor();
	ResultSet* rs = rsCursor.next();
	while (rs) {
		if (rs->getContainerId() == containerId && rs->isPartialExecuteMode()) {
			if (isMvccRow) {
				rs->addUpdatedMvccRow(rowId, oId);
			}
			else {
				rs->addUpdatedRow(rowId, oId);
			}
		}
		rs = rsCursor.next();
	}
}
void ResultSetManager::addRemovedRow(ContainerId containerId, RowId rowId, OId oId) {
	if (resultSetMap_->size() == 0) {
		return;
	}

	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_->getCursor();
	ResultSet* rs = rsCursor.next();
	while (rs) {
		if (rs->getContainerId() == containerId && rs->isPartialExecuteMode()) {
			rs->addRemovedRow(rowId, oId);
		}
		rs = rsCursor.next();
	}
}
void ResultSetManager::addRemovedRowArray(ContainerId containerId, OId oId) {
	if (resultSetMap_->size() == 0) {
		return;
	}

	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_->getCursor();
	ResultSet* rs = rsCursor.next();
	while (rs) {
		if (rs->getContainerId() == containerId && rs->isPartialExecuteMode()) {
			rs->addRemovedRowArray(oId);
		}
		rs = rsCursor.next();
	}
}


void ResultSetManager::forceCloseAll() {
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_->getCursor();
	ResultSet* rs = rsCursor.next();
	while (rs) {
		closeInternal(*rs);
		rs = rsCursor.next();
	}
}


ResultSetHolderManager::ResultSetHolderManager() :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_TXN_RESULT, "resultSetHolder")),
		config_(NULL, alloc_),
		groupList_(alloc_) {
}

ResultSetHolderManager::~ResultSetHolderManager() try {
	clear();
}
catch (...) {
	assert(false);
}

void ResultSetHolderManager::initialize(const PartitionGroupConfig &config) {
	if (config_.get() != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	clear();

	const uint32_t groupCount = config.getPartitionGroupCount();
	groupList_.reserve(groupCount);

	for (uint32_t i = 0; i < groupCount; i++) {
		groupList_.push_back(ALLOC_VAR_SIZE_NEW(alloc_) Group(alloc_));
	}

	config_.reset(ALLOC_VAR_SIZE_NEW(alloc_) PartitionGroupConfig(config));
}

void ResultSetHolderManager::closeAll(
		TransactionContext &txn, PartitionList &partitionList) {
	closeAll(
			txn.getDefaultAllocator(),
			getPartitionGroupId(txn.getPartitionId()),
			partitionList);
}

void ResultSetHolderManager::closeAll(
		util::StackAllocator &alloc, PartitionGroupId pgId,
		PartitionList &partitionList) {
	util::Vector<EntryKey> rsIdList(alloc);
	pullCloseable(pgId, rsIdList);

	for (util::Vector<EntryKey>::iterator it = rsIdList.begin();
			it != rsIdList.end(); ++it) {
		Partition& partition = partitionList.partition(it->first);
		if (!partition.isActive()) {
			continue;
		}
		static_cast<DataStoreV4&>(partition.dataStore()).getResultSetManager()->close(it->second);
	}
}

void ResultSetHolderManager::add(PartitionId pId, ResultSetId rsId) {
	if (rsId == UNDEF_RESULTSETID) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	const EntryKey key(pId, rsId);
	Group &group = getGroup(getPartitionGroupId(pId));

	util::LockGuard<util::Mutex> guard(mutex_);

	util::AllocUniquePtr<Entry> entry(
			ALLOC_VAR_SIZE_NEW(alloc_) Entry(key), alloc_);

	if (!group.map_.insert(std::make_pair(key, entry.get())).second) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	entry.release();
}

void ResultSetHolderManager::setCloseable(PartitionId pId, ResultSetId rsId) {
	const EntryKey key(pId, rsId);
	Group &group = getGroup(getPartitionGroupId(pId));

	util::LockGuard<util::Mutex> guard(mutex_);

	Map::iterator it = group.map_.find(key);
	if (it == group.map_.end() || it->second->next_ != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	it->second->next_ = group.closeableEntry_;
	group.closeableEntry_ = it->second;
	group.map_.erase(it);
	group.version_++;
}

void ResultSetHolderManager::release(PartitionId pId, ResultSetId rsId) {
	const EntryKey key(pId, rsId);
	Group &group = getGroup(getPartitionGroupId(pId));

	util::LockGuard<util::Mutex> guard(mutex_);

	Map::iterator it = group.map_.find(key);
	if (it == group.map_.end() || it->second->next_ != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	ALLOC_VAR_SIZE_DELETE(alloc_, it->second);
	group.map_.erase(it);
}

void ResultSetHolderManager::clear() {
	config_.reset();

	while (!groupList_.empty()) {
		{
			Group *group = groupList_.back();
			clearEntries(*group, true);
			ALLOC_VAR_SIZE_DELETE(alloc_, group);
		}
		groupList_.pop_back();
	}
}

void ResultSetHolderManager::clearEntries(Group &group, bool withMap) {
	if (withMap) {
		for (Map::iterator it = group.map_.begin();
				it != group.map_.end(); ++it) {
			ALLOC_VAR_SIZE_DELETE(alloc_, it->second);
		}
		group.map_.clear();
	}

	for (Entry *entry = group.closeableEntry_; entry != NULL;) {
		Entry *next = entry->next_;
		ALLOC_VAR_SIZE_DELETE(alloc_, entry);
		entry = next;
	}

	group.closeableEntry_ = NULL;
}

void ResultSetHolderManager::pullCloseable(
		PartitionGroupId pgId, util::Vector<EntryKey> &keyList) {
	Group &group = getGroup(pgId);

	if (group.checkedVersion_ != group.version_) {
		util::LockGuard<util::Mutex> guard(mutex_);

		for (Entry *entry = group.closeableEntry_;
				entry != NULL; entry = entry->next_) {
			keyList.push_back(entry->key_);
		}

		clearEntries(group, false);

		group.checkedVersion_ = group.version_;
	}
}

ResultSetHolderManager::Group& ResultSetHolderManager::getGroup(
		PartitionGroupId pgId) {
	if (pgId >= groupList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	return *groupList_[pgId];
}

PartitionGroupId ResultSetHolderManager::getPartitionGroupId(
		PartitionId pId) const {
	if (config_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	return config_->getPartitionGroupId(pId);
}

ResultSetHolderManager::Entry::Entry(const EntryKey &key) :
		key_(key),
		next_(NULL) {
}

ResultSetHolderManager::Group::Group(Allocator &alloc) :
		map_(alloc),
		version_(0),
		checkedVersion_(0),
		closeableEntry_(NULL) {
}
