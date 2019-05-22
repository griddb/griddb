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
	  resultType_(RESULT_NONE),
	  isId_(false),
	  isRowIdInclude_(false),
	  useRSRowImage_(false),
	  distributedTarget_(NULL),
	  distributedTargetUncovered_(false),
	  distributedTargetReduced_(false)
{
}

ResultSet::~ResultSet() {
	if (rsAlloc_) {
		ALLOC_DELETE((*rsAlloc_), rowIdList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedRowList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedVarDataList_);
		ALLOC_DELETE((*rsAlloc_), distributedTarget_);
	}
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
		ALLOC_DELETE((*rsAlloc_), rowIdList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedRowList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedVarDataList_);
		ALLOC_DELETE((*rsAlloc_), distributedTarget_);
	}
	rowIdList_ = NULL;
	rsSerializedRowList_ = NULL;
	rsSerializedVarDataList_ = NULL;
	distributedTarget_ = NULL;
	distributedTargetUncovered_ = false;
	distributedTargetReduced_ = false;

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
	assert(rsAlloc_);
	if (rowIdList_ == NULL) {
		rowIdList_ = ALLOC_NEW(*rsAlloc_) util::XArray<RowId>(*rsAlloc_);
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
	return (resultType_ != RESULT_ROW_ID_SET) &&
		   (resultType_ != PARTIAL_RESULT_ROWSET);
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
		TransactionContext &txn, DataStore &dataStore) {
	closeAll(
			txn.getDefaultAllocator(),
			getPartitionGroupId(txn.getPartitionId()),
			dataStore);
}

void ResultSetHolderManager::closeAll(
		util::StackAllocator &alloc, PartitionGroupId pgId,
		DataStore &dataStore) {
	util::Vector<EntryKey> rsIdList(alloc);
	pullCloseable(pgId, rsIdList);

	for (util::Vector<EntryKey>::iterator it = rsIdList.begin();
			it != rsIdList.end(); ++it) {
		dataStore.closeResultSet(it->first, it->second);
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
