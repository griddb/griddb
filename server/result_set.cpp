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
	  useRSRowImage_(false)
{
}

ResultSet::~ResultSet() {
	if (rsAlloc_) {
		ALLOC_DELETE((*rsAlloc_), rowIdList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedRowList_);
		ALLOC_DELETE((*rsAlloc_), rsSerializedVarDataList_);
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
	}
	rowIdList_ = NULL;
	rsSerializedRowList_ = NULL;
	rsSerializedVarDataList_ = NULL;

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

