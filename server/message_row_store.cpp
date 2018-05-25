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
    @brief Implementation of MessageRowStore
*/

#include "message_row_store.h"
#include "data_store.h"
#include <algorithm>


MessageRowStore::MessageRowStore(const DataStoreValueLimitConfig &dsValueLimitConfig, const ColumnInfo *columnInfoList, uint32_t columnCount) :
		dsValueLimitConfig_(dsValueLimitConfig),
		columnInfoList_(columnInfoList),
		columnCount_(columnCount) {
	if (columnCount <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "Empty column info list");
	}
}

MessageRowStore::~MessageRowStore() {
}

/*!
	@brief Set Row
*/
void MessageRowStore::setRow(const MessageRowStore &source) {
	assert (getColumnInfoList() == source.getColumnInfoList());
	assert (getColumnCount() == source.getColumnCount());

	const void *data;
	uint32_t size;
	source.getRowFixedPart(data, size);
	setRowFixedPart(data, size);
	if (variableColumnNum_ > 0) {
		setVarSize(variableColumnNum_);
	}
	for (ColumnId id = 0; (id = findVariableColumn(id)) != UNDEF_COLUMNID; id++) {
		setField(source, id);
	}
}

/*!
	@brief Set field value
*/
void MessageRowStore::setField(const MessageRowStore &source, ColumnId columnId) {
	assert (getColumnInfoList() == source.getColumnInfoList());
	assert (getColumnCount() == source.getColumnCount());

	const void *data;
	uint32_t size;
	source.getField(columnId, data, size); 
	setField(columnId, data, size);
}

const ColumnInfo* MessageRowStore::getColumnInfoList() const {
	return columnInfoList_;
}

uint32_t MessageRowStore::getColumnCount() const {
	return columnCount_;
}

/*!
	@brief Get Variable ColumId
*/
ColumnId MessageRowStore::findVariableColumn(ColumnId startId) const {
	for (ColumnId id = startId; id < columnCount_; id++) {
		if (getColumnInfo(id).isVariable()) {
			return id;
		}
	}

	return UNDEF_COLUMNID;
}

/*!
	@brief Get pointer and size of fixed-type field values
*/
void MessageRowStore::getRowFixedPart(const void *&, uint32_t &) const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Get pointer and size of fixed-type field values
*/
void MessageRowStore::getRowFixedPart(const uint8_t *&data, uint32_t &size) const {
	getRowFixedPart(reinterpret_cast<const void*&>(data), size);
}

/*!
	@brief Get pointer and size of variable-type field values
*/
uint8_t* MessageRowStore::getRowVariablePart() const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set field value
*/
void MessageRowStore::getField(
		ColumnId, const void *&, uint32_t &) const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set field value
*/
void MessageRowStore::getField(
		ColumnId columnId, const uint8_t *&data, uint32_t &size) const {
	getField(columnId, reinterpret_cast<const void*&>(data), size);
}

/*!
	@brief Get array length
*/
uint32_t MessageRowStore::getArrayLength(ColumnId) const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Get array data size
*/
uint32_t MessageRowStore::getTotalArraySize(ColumnId) const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Get element of array
*/
void MessageRowStore::getArrayElement(ColumnId,
		uint32_t, const void *&, uint32_t &) const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Get element of array
*/
void MessageRowStore::getArrayElement(ColumnId columnId,
		uint32_t arrayIndex, const uint8_t *&data, uint32_t &size) const {
	getArrayElement(
			columnId, arrayIndex, reinterpret_cast<const void*&>(data), size);
}

/*!
	@brief Set RowId
*/
void MessageRowStore::setRowId(RowId) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set fixed-type field values
*/
void MessageRowStore::setRowFixedPart(const void *, uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Initialize Row
*/
void MessageRowStore::beginRow() {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set field value
*/
void MessageRowStore::setField(ColumnId, const void *, uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set fixed-type field values
*/
void MessageRowStore::setFieldForRawData(ColumnId, const void *, uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set field value
*/
void MessageRowStore::setField(ColumnId, const Value &) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set variable-type field value
*/
void MessageRowStore::addVariableFieldPart(const void *, uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set array-type field value
*/
void MessageRowStore::setArrayField(ColumnId) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set element of array-type field value
*/
void MessageRowStore::addArrayElement(const void *, uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set header of variable-type field value
*/
void MessageRowStore::setVarDataHeaderField(
		ColumnId, uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set variable size of variable-type field value
*/
void MessageRowStore::setVarSize(uint32_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Set uint64 data
*/
void MessageRowStore::setUInt64(uint64_t) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

/*!
	@brief Validate current Row
*/
void MessageRowStore::validate() const {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}


const ColumnInfo& MessageRowStore::getColumnInfo(ColumnId columnId) const {
	assert(columnCount_ > 0);
	return columnInfoList_[columnId];
}

bool MessageRowStore::isVariableColumn(ColumnId columnId) const {
	const ColumnInfo &info = getColumnInfo(columnId);
	return (info.getColumnType() == COLUMN_TYPE_STRING ||
			info.getColumnType() == COLUMN_TYPE_BLOB ||
			info.isArray());
}

bool MessageRowStore::isLinkedVariableColumn(ColumnId columnId) const {
	const ColumnInfo &info = getColumnInfo(columnId);
	return (info.getColumnType() == COLUMN_TYPE_BLOB
			|| info.getColumnType() == COLUMN_TYPE_STRING_ARRAY);
}

uint32_t MessageRowStore::getColumnElementFixedSize(const ColumnInfo &info) {
	return FixedSizeOfColumnType[info.getSimpleColumnType()];
}

template<typename S>
MessageRowStore::StreamResetter<S>::StreamResetter(S &stream) :
		stream_(stream),
		position_(stream.base().position()) {
}

template<typename S>
MessageRowStore::StreamResetter<S>::~StreamResetter() {
	try {
		stream_.base().position(position_);
	}
	catch (...) {
	}
}


InputMessageRowStore::InputMessageRowStore(
		const DataStoreValueLimitConfig &dsValueLimitConfig, 
		const ColumnInfo *columnInfoList, uint32_t columnCount,
		void *data, uint32_t size, uint64_t rowCount, uint32_t rowFixedDataSize,
		bool validateOnConstruct) :
		MessageRowStore(dsValueLimitConfig, columnInfoList, columnCount),
		rowCount_(rowCount), rowIdIncluded_(false),
		rowIdSize_(rowIdIncluded_ ? sizeof(RowId) : 0),
		varDataIn_(getVarDataInput(data, size, rowCount, (rowIdIncluded_ ? sizeof(RowId) : 0) + rowFixedDataSize)),
		fixedDataIn_(util::ByteStream<util::ArrayInStream>(util::ArrayInStream(
				data, size - varDataIn_.base().remaining()))),
		varData_(static_cast<uint8_t*>(data) + fixedDataIn_.base().remaining()),
		fixedData_(static_cast<uint8_t*>(data)),
		nextRowPosition_(0),
		lastVarColumnNth_(0), lastVarColumnOffset_(0) {
	rowFixedSize_ = 0;
	variableColumnNum_ = 0;
	nullsOffset_ = 0;
	for (uint32_t columnId = 0; columnId < columnCount; ++columnId) {
		const ColumnInfo &info = columnInfoList[columnId];
		if (info.isVariable()) {
			++variableColumnNum_;
			nullsOffset_ = sizeof(OId);
		} else {
			rowFixedSize_ += info.getColumnSize();
		}
	}
	nullsBytes_ = ValueProcessor::calcNullsByteSize(columnCount);
	rowImageSize_ = (rowIdIncluded_ ? sizeof(RowId) : 0) + nullsOffset_ + nullsBytes_ + rowFixedSize_;

	if (validateOnConstruct) {
		while (next()) {
		}
	}

	reset();
}

InputMessageRowStore::InputMessageRowStore(
		const DataStoreValueLimitConfig &dsValueLimitConfig, 
		const ColumnInfo *columnInfoList, uint32_t columnCount,
		void *fixedData, uint32_t fixedDataSize, void *varData, uint32_t varDataSize, uint64_t rowCount, bool rowIdIncluded,
		bool validateOnConstruct) :
		MessageRowStore(dsValueLimitConfig, columnInfoList, columnCount),
		rowCount_(rowCount), rowIdIncluded_(rowIdIncluded),
		rowIdSize_(rowIdIncluded_ ? sizeof(RowId) : 0),
		varDataIn_(util::ByteStream<util::ArrayInStream>(util::ArrayInStream(varData, varDataSize))),
		fixedDataIn_(util::ByteStream<util::ArrayInStream>(util::ArrayInStream(fixedData, fixedDataSize))),
		varData_(static_cast<uint8_t*>(varData)),
		fixedData_(static_cast<uint8_t*>(fixedData)),
		nextRowPosition_(0),
		lastVarColumnNth_(0), lastVarColumnOffset_(0) {
	rowFixedSize_ = 0;
	variableColumnNum_ = 0;
	nullsOffset_ = 0;
	for (uint32_t columnId = 0; columnId < columnCount; ++columnId) {
		const ColumnInfo &info = columnInfoList[columnId];
		if (info.isVariable()) {
			++variableColumnNum_;
			nullsOffset_ = sizeof(OId);
		} else {
			rowFixedSize_ += info.getColumnSize();
		}
	}
	nullsBytes_ = ValueProcessor::calcNullsByteSize(columnCount);
	rowImageSize_ = (rowIdIncluded_ ? sizeof(RowId) : 0) + nullsOffset_ + nullsBytes_ + rowFixedSize_;

	if (validateOnConstruct) {
		while (next()) {
		}
	}

	reset();
}


/*!
	@brief Move to next Row
*/
bool InputMessageRowStore::next() {
	if (nextRowPosition_ >= rowCount_) {
		return false;
	}

	if (nextRowPosition_ > 0) {
		fixedDataIn_.base().position(
				fixedDataIn_.base().position() + rowImageSize_);
		if (variableColumnNum_ > 0) {
			uint64_t pos = fixedDataIn_.base().position();
			if (rowIdIncluded_) {
				RowId rowId;
				fixedDataIn_ >> rowId;
			}
			uint64_t varOffset = 0;
			fixedDataIn_ >> varOffset;
			varDataIn_.base().position(varOffset);
			fixedDataIn_.base().position(pos);
		}
	}
	nextRowPosition_++;

	validate();

	return true;
}

/*!
	@brief Move to first Row
*/
void InputMessageRowStore::reset() {
	varDataIn_.base().position(0);
	fixedDataIn_.base().position(0);
	nextRowPosition_ = 0;
}

/*!
	@brief Get current Row position
*/
uint64_t InputMessageRowStore::position() {
	if (nextRowPosition_ == 0) {
		return std::numeric_limits<uint64_t>::max();
	}

	return nextRowPosition_ - 1;
}

/*!
	@brief Set current Row position
*/
void InputMessageRowStore::position(uint64_t p) {
	if (p >= rowCount_) {
		if (p != std::numeric_limits<uint64_t>::max()) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Illegal position");
		}
		reset();
	}
	else {
		fixedDataIn_.base().position(
			static_cast<size_t>(p) * rowImageSize_);
		if (variableColumnNum_ > 0) {
			uint64_t pos = fixedDataIn_.base().position();
			if (rowIdIncluded_) {
				RowId rowId;
				fixedDataIn_ >> rowId;
			}
			uint64_t varOffset = 0;
			fixedDataIn_ >> varOffset;
			varDataIn_.base().position(varOffset);
			fixedDataIn_.base().position(pos);
		}
		nextRowPosition_ = p + 1;
	}
}


uint64_t InputMessageRowStore::getRowCount() {
	return rowCount_;
}

/*!
	@brief Get pointer and size of fixed-type field values
*/
void InputMessageRowStore::getRowFixedPart(
		const void *&data, uint32_t &size) const {
	assert(hasActiveRow());
	data = fixedData_ + fixedDataIn_.base().position();
	size = rowImageSize_;
}

/*!
	@brief Get pointer and size of variable-type field values
*/
uint8_t* InputMessageRowStore::getRowVariablePart() const {
	assert(hasActiveRow());
	return varData_ + varDataIn_.base().position();
}

/*!
	@brief Get array length
*/
uint32_t InputMessageRowStore::getArrayLength(ColumnId columnId) const {
	assert(hasActiveRow());

	util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;
	varDataIn.base().position(getVarDataOffset(columnId));

	MessageRowStore::getVarSize(varDataIn); 
	uint32_t elementNum = MessageRowStore::getVarSize(varDataIn); 

	return elementNum;
}

/*!
	@brief Get array data size
*/
uint32_t InputMessageRowStore::getTotalArraySize(ColumnId columnId) const {
	assert(hasActiveRow());

	util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;
	varDataIn.base().position(getVarDataOffset(columnId));

	uint32_t length = MessageRowStore::getVarSize(varDataIn); 

	return length;
}

/*!
	@brief Get element of array
*/
void InputMessageRowStore::getArrayElement(ColumnId columnId,
		uint32_t arrayIndex, const void *&data, uint32_t &size) const {
	assert(hasActiveRow());

	const ColumnInfo &info = getColumnInfo(columnId);
	assert(info.isArray());

	util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;
	varDataIn.base().position(getVarDataOffset(columnId));

	MessageRowStore::getVarSize(varDataIn); 
	uint32_t elementNum = MessageRowStore::getVarSize(varDataIn); 
	UNUSED_VARIABLE(elementNum);

	assert(elementNum > arrayIndex);
	if (info.getColumnType() == COLUMN_TYPE_STRING_ARRAY) {
		for (uint32_t i = 0; i <= arrayIndex; i++) {
			size = MessageRowStore::getVarSize(varDataIn);
			data = varData_ + varDataIn.base().position();
			varDataIn.base().position(varDataIn.base().position() + size);
		}
	}
	else {
		assert(info.getColumnType() != COLUMN_TYPE_BLOB);

		size = getColumnElementFixedSize(info);
		assert(size > 0);
		varDataIn.base().position(varDataIn.base().position() + size * arrayIndex);

		data = varData_ + varDataIn.base().position();
		varDataIn.base().position(varDataIn.base().position() + size);
	}
}

/*!
	@brief Get offset of fixed-type values and variable-typed values
*/
bool InputMessageRowStore::getStartOffset(uint64_t startPos, uint64_t &fixedOffset, uint64_t &varOffset) {
	bool valid = true;
	if (startPos > rowCount_) {
		valid = false;
	} else if (startPos == rowCount_) {
		util::ByteStream<util::ArrayInStream> fixedDataIn = fixedDataIn_;
		fixedDataIn.base().position(0);
		fixedOffset = fixedDataIn.base().remaining();
		util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;
		varDataIn.base().position(0);
		varOffset = varDataIn.base().remaining();
	} else {
		uint64_t currentPos = position();
		position(startPos);
		fixedOffset = fixedDataIn_.base().position();;
		varOffset = varDataIn_.base().position();;
		position(currentPos);
	}
	return valid;
}


/*!
	@brief Get offset and size of fixed-type values and variable-typed values
*/
void InputMessageRowStore::getPartialRowSet(uint64_t startPos, uint64_t rowNum, uint64_t &fixedOffset,
											uint64_t &fixedSize, uint64_t &varOffset, uint64_t &varSize) {
	if (startPos >= rowCount_) {
		fixedOffset = 0;
		fixedSize = 0;
		varOffset = 0;
		varSize = 0;
		return;
	}

	uint64_t currentPos = position();
	position(startPos);
	fixedOffset = fixedDataIn_.base().position();
	varOffset = varDataIn_.base().position();

	if (startPos + rowNum >= rowCount_) {
		util::ByteStream<util::ArrayInStream> fixedDataIn = fixedDataIn_;
		fixedDataIn.base().position(0);
		fixedSize = fixedDataIn.base().remaining() - fixedOffset;
		util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;
		varDataIn.base().position(0);
		varSize = varDataIn.base().remaining() - varOffset;
	} else {
		position(startPos + rowNum);
		fixedSize = fixedDataIn_.base().position() - fixedOffset;
		varSize = varDataIn_.base().position() - varOffset;
	}
	position(currentPos);
}

/*!
	@brief Get current Row
*/
void InputMessageRowStore::getCurrentRowData(util::XArray<uint8_t> &rowData) {
	uint64_t fixedOffset, fixedSize;
	uint64_t varOffset, varSize;

	getPartialRowSet(position(), 1, fixedOffset, fixedSize, varOffset, varSize);

	util::XArrayOutStream<> arrayOut(rowData);
	util::ByteStream< util::XArrayOutStream<> > out(arrayOut);

	out << std::pair<const uint8_t*, size_t>(fixedData_ + fixedOffset, fixedSize);

	if (variableColumnNum_ > 0) {
		out << std::pair<const uint8_t*, size_t>(varData_ + varOffset, varSize);

		const size_t currentPos = out.base().position();
		out.base().position(0);
		out << static_cast<uint64_t>(0);
		out.base().position(currentPos);
	}
}

/*!
	@brief Set field value
*/
void InputMessageRowStore::setField(
		ColumnId columnId, const void *data, uint32_t size) {
	assert(hasActiveRow());

	if (isVariableColumn(columnId)) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
	}

	const ColumnInfo &info = getColumnInfo(columnId);
	assert(size == info.getColumnSize());

	memcpy(fixedData_ + rowIdSize_ + info.getColumnOffset(), data, size);
}

/*!
	@brief Set field value
*/
void InputMessageRowStore::setFieldForRawData(
		ColumnId columnId, const void *data, uint32_t size) {
	assert(hasActiveRow());

	setField(columnId, data, size);
}


/*!
	@brief Set field value
*/
void InputMessageRowStore::setField(ColumnId, const Value &) {
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unsupported operation");
}

util::ByteStream<util::ArrayInStream> InputMessageRowStore::getVarDataInput(
		void *data, uint32_t size, uint64_t rowCount, size_t fixedRowPartSize) {
	const uint64_t fixedDataSize = fixedRowPartSize * rowCount;
	assert(fixedDataSize <= size);
	if (fixedDataSize > size) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "Buffer overflow");
	}

	const void *const varData = static_cast<uint8_t*>(data) + fixedDataSize;
	const size_t varDataSize = static_cast<size_t>(size - fixedDataSize);

	return util::ByteStream<util::ArrayInStream>(
			util::ArrayInStream(varData, varDataSize));
}

bool InputMessageRowStore::hasActiveRow() const {
	return (fixedDataIn_.base().remaining() >= rowImageSize_);
}

/*!
	@brief Validate current Row
*/
void InputMessageRowStore::validate() const {
	for (ColumnId id = 0; id < getColumnCount(); id++) {

		const ColumnInfo &columnInfo =  getColumnInfo(id);
		const void *data; 
		uint32_t size;
		if (columnInfo.isArray()){
			uint32_t totalSize = getTotalArraySize(id);
			UNUSED_VARIABLE(totalSize);
			uint32_t elementNum = getArrayLength(id);
			uint32_t elementSize = FixedSizeOfColumnType[columnInfo.getSimpleColumnType()];
			if (elementSize != 0) {
				assert(elementNum == (totalSize - ValueProcessor::getEncodedVarSize(elementNum)) / elementSize);
			}
			if (elementNum > dsValueLimitConfig_.getLimitArrayNum()){
				GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID, "Array length of Column[" << id << "] exceeds maximum size : " << elementNum);
			}
			switch (columnInfo.getColumnType()) {
			case COLUMN_TYPE_STRING_ARRAY :
				{
					for (uint32_t nth = 0; nth < elementNum; nth++) {
						getArrayElement(id, nth , data, size);
						if (size > dsValueLimitConfig_.getLimitSmallSize()) {
							GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID, "Size of element[" << nth << "] of Column[" << id << "] exceeds maximum size");
						}
					}
				}
				break;
			case COLUMN_TYPE_TIMESTAMP_ARRAY :
				{
					for (uint32_t nth = 0; nth < elementNum; nth++) {
						getArrayElement(id, nth , data, size);
						Timestamp val = *reinterpret_cast<const Timestamp*>(data);
						if (TRIM_MILLISECONDS) {
							val = (val/1000)*1000;
						}
						if (!ValueProcessor::validateTimestamp(val)) {
							GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID, "Timestamp of Column[" << id << "] out of range (val=" << val << ")");
						}
					}
				}
				break;
			default:
				break;
			}
		} else {
			switch (columnInfo.getColumnType()) {
			case COLUMN_TYPE_STRING :
				{
					getField(id, data, size);
					if (size > dsValueLimitConfig_.getLimitSmallSize()) {
						GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID, "Size of Column[" << id << "] exceeds maximum size : " << size);
					}
				}
				break;
			case COLUMN_TYPE_TIMESTAMP :
				{
					getField(id, data, size);
					Timestamp val = *reinterpret_cast<const Timestamp*>(data);
					if (TRIM_MILLISECONDS) {
						val = (val/1000)*1000;
					}
					if (!ValueProcessor::validateTimestamp(val)) {
						GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID, "Timestamp of Column[" << id << "] out of range (val=" << val << ")");
					}
				}
				break;
			case COLUMN_TYPE_BLOB:
				{
					getField(id, data, size);
					if (size > dsValueLimitConfig_.getLimitBigSize()) {
						GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID, "Size of Column[" << id << "] exceeds maximum size : " << size);
					}
				}
				break;
			default:
				break;
			}
		}
	}
}



OutputMessageRowStore::OutputMessageRowStore(
		const DataStoreValueLimitConfig &dsValueLimitConfig, 
		const ColumnInfo *columnInfoList, uint32_t columnCount,
		util::XArray<uint8_t> &fixedData, util::XArray<uint8_t> &variableData,
		bool rowIdIncluded) :
		MessageRowStore(dsValueLimitConfig, columnInfoList, columnCount),
		varData_(variableData),
		fixedData_(fixedData),
		varDataOut_(util::ByteStream< util::XArrayOutStream<> >(
				util::XArrayOutStream<>(variableData))),
		fixedDataOut_(util::ByteStream< util::XArrayOutStream<> >(
				util::XArrayOutStream<>(fixedData))),
		fieldsInitialized_(fixedData.get_allocator()),
		rowCount_(0),
		lastColumnId_(UNDEF_COLUMNID),
		rowIdIncluded_(rowIdIncluded)
{
	assert((fieldsInitialized_.assign(
			columnCount + (rowIdIncluded_ ? 1 : 0), false), true));
	rowIdSize_ = rowIdIncluded ? sizeof(RowId) : 0;
	variableColumnNum_ = 0;
	nullsOffset_ = 0;
	rowFixedSize_ = 0;
	for (uint32_t columnId = 0; columnId < columnCount; ++columnId) {
		const ColumnInfo &info = columnInfoList[columnId];
		if (info.isVariable()) {
			++variableColumnNum_;
			nullsOffset_ = sizeof(OId);
		} else {
			rowFixedSize_ += info.getColumnSize();
		}
	}
	nullsBytes_ = ValueProcessor::calcNullsByteSize(columnCount);
	rowImageSize_ = (rowIdIncluded ? sizeof(RowId) : 0) + nullsOffset_ + nullsBytes_ + rowFixedSize_;
}

/*!
	@brief Move to next Row
*/
bool OutputMessageRowStore::next() {
	assert(std::find(fieldsInitialized_.begin(), fieldsInitialized_.end(), false) ==
			fieldsInitialized_.end());

	const size_t orgPosition = fixedDataOut_.base().position();
	if (orgPosition >= rowCount_ * getFullRowFixedPartSize()) {
		assert(orgPosition == rowCount_ * getFullRowFixedPartSize());
		rowCount_++;
	}
	fixedDataOut_.base().position(orgPosition + getFullRowFixedPartSize());

	lastColumnId_ = UNDEF_COLUMNID;
	assert((fieldsInitialized_.assign(
			getColumnCount() + (rowIdIncluded_ ? 1 : 0), false), true));

	return false;
}

/*!
	@brief Move to first Row
*/
void OutputMessageRowStore::reset() {
	fixedDataOut_.base().position(0);
	lastColumnId_ = UNDEF_COLUMNID;
	assert((fieldsInitialized_.assign(
			getColumnCount() + (rowIdIncluded_ ? 1 : 0), false), true));
}

/*!
	@brief Get current Row position
*/
uint64_t OutputMessageRowStore::position() {
	return fixedDataOut_.base().position() / getFullRowFixedPartSize();
}

/*!
	@brief Set current Row position
*/
void OutputMessageRowStore::position(uint64_t p) {
	assert(position() < rowCount_ ||
			std::find(fieldsInitialized_.begin(), fieldsInitialized_.end(), true) ==
			fieldsInitialized_.end());

	if (p > rowCount_) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_ENCODE_FAILED, "Illegal position");
	}

	fixedDataOut_.base().position(
			static_cast<size_t>(p) * getFullRowFixedPartSize());
	lastColumnId_ = UNDEF_COLUMNID;
	assert((fieldsInitialized_.assign(
			getColumnCount() + (rowIdIncluded_ ? 1 : 0), p < rowCount_), true));
}

uint64_t OutputMessageRowStore::getRowCount() {
	return rowCount_;
}

/*!
	@brief Set variable size of variable-type field value
*/
void OutputMessageRowStore::setVarSize(uint32_t varSize) {
	uint32_t encodedVarSize = ValueProcessor::encodeVarSize(varSize);
	if (varSize <= OBJECT_MAX_1BYTE_LENGTH_VALUE) {
		varDataOut_ << static_cast<uint8_t>(encodedVarSize);
	} else {
		varDataOut_ << encodedVarSize;
	}
}

/*!
	@brief Set uint64 data
*/
void OutputMessageRowStore::setUInt64(uint64_t data) {
	varDataOut_ << data;
}

void OutputMessageRowStore::setNextOId(OId oId) {
	varDataOut_ << ValueProcessor::encodeVarSizeOId(oId);
}

/*!
	@brief Set RowId
*/
void OutputMessageRowStore::setRowId(RowId rowId) {
	assert(rowIdIncluded_);
	StreamResetter< util::ByteStream< util::XArrayOutStream<> > > resetter(
			fixedDataOut_);

	fixedDataOut_ << rowId;
	assert((fieldsInitialized_[getColumnCount()] = true, true));
}

/*!
	@brief Set fixed-type field values
*/
void OutputMessageRowStore::setRowFixedPart(const void *data, uint32_t size) {
	assert(size == (rowImageSize_ - rowIdSize_));
	StreamResetter< util::ByteStream< util::XArrayOutStream<> > > resetter(
			fixedDataOut_);
	if (rowIdIncluded_) {
		fixedDataOut_.base().position(
				fixedDataOut_.base().position() + sizeof(RowId));
	}
	uint64_t varDataOffsetPos = fixedDataOut_.base().position();
	fixedDataOut_.writeAll(data, size);
	if (variableColumnNum_ > 0) {
		uint64_t varDataOffset = varDataOut_.base().position();
		fixedDataOut_.base().position(varDataOffsetPos);
		fixedDataOut_ << varDataOffset;
	}
	for (ColumnId id = 0; id < getColumnCount(); id++) {
		assert((fieldsInitialized_[id] |= !isVariableColumn(id), true));
	}
}

/*!
	@brief Initialize Row
*/
void OutputMessageRowStore::beginRow() {
	StreamResetter< util::ByteStream< util::XArrayOutStream<> > > resetter(
		fixedDataOut_);
	if (rowIdIncluded_) {
		fixedDataOut_.base().position(
			fixedDataOut_.base().position() + sizeof(RowId));
	}
	uint64_t varDataOffsetPos = fixedDataOut_.base().position();
	if (variableColumnNum_ > 0) {
		uint64_t varDataOffset = varDataOut_.base().position();
		fixedDataOut_.base().position(varDataOffsetPos);
		fixedDataOut_ << varDataOffset;
		setVarSize(variableColumnNum_);
	}
	uint32_t nullsBytes = getNullsBytes();
	uint8_t byte = 0;
	for (uint32_t i = 0; i < nullsBytes; ++i) {
		fixedDataOut_ << byte;
	}
}

/*!
	@brief Set field value
*/
void OutputMessageRowStore::setField(
		ColumnId columnId, const void *data, uint32_t size) {
	StreamResetter< util::ByteStream< util::XArrayOutStream<> > > resetter(
		fixedDataOut_);
	if (isVariableColumn(columnId)) {
		if (size > 0) {
			assert(data);
			assert(ValueProcessor::decodeVarSize(data) == size);
			varDataOut_.writeAll(data, size + ValueProcessor::getEncodedVarSize(size));
		} else {
			setVarSize(size);
		}
	}
	else {
		assert(getColumnInfo(columnId).getColumnSize() == size);

		fixedDataOut_.base().position(getFixedDataOffset(columnId));
		fixedDataOut_.writeAll(data, size);
	}
	assert((fieldsInitialized_[columnId] = true, true));
	lastColumnId_ = columnId;
}

/*!
	@brief Set field value
*/
void OutputMessageRowStore::setFieldForRawData(
		ColumnId columnId, const void *data, uint32_t size) {
	StreamResetter< util::ByteStream< util::XArrayOutStream<> > > resetter(
		fixedDataOut_);
	if (isVariableColumn(columnId)) {
		setVarSize(size);
		if (size > 0) {
			assert(data);
			varDataOut_.writeAll(data, size);
		}
	}
	else {
		assert(getColumnInfo(columnId).getColumnSize() == size);

		fixedDataOut_.base().position(getFixedDataOffset(columnId));
		fixedDataOut_.writeAll(data, size);
	}
	assert((fieldsInitialized_[columnId] = true, true));
	lastColumnId_ = columnId;
}

/*!
	@brief Set field value
*/
void OutputMessageRowStore::setField(ColumnId columnId, const Value &value) {
	const ColumnInfo &info = getColumnInfo(columnId);
	switch (info.getColumnType()) {
	case COLUMN_TYPE_BYTE:
		{
			int8_t val = static_cast<int8_t>(*reinterpret_cast<const int64_t*>(value.data()));
			setField<COLUMN_TYPE_BYTE>(columnId, val);
		}
		break;
	case COLUMN_TYPE_SHORT:
		{
			int16_t val = static_cast<int16_t>(*reinterpret_cast<const int64_t*>(value.data()));
			setField<COLUMN_TYPE_SHORT>(columnId, val);
		}
		break;
	case COLUMN_TYPE_INT:
		{
			int32_t val = static_cast<int32_t>(*reinterpret_cast<const int64_t*>(value.data()));
			setField<COLUMN_TYPE_INT>(columnId, val);
		}
		break;
	case COLUMN_TYPE_LONG:
		{
			int64_t val = static_cast<int64_t>(*reinterpret_cast<const int64_t*>(value.data()));
			setField<COLUMN_TYPE_LONG>(columnId, val);
		}
		break;
	case COLUMN_TYPE_FLOAT:
		{
			float val = static_cast<float>(*reinterpret_cast<const float*>(value.data()));
			setField<COLUMN_TYPE_FLOAT>(columnId, val);
		}
		break;
	case COLUMN_TYPE_DOUBLE:
		{
			double val = static_cast<double>(*reinterpret_cast<const double*>(value.data()));
			setField<COLUMN_TYPE_DOUBLE>(columnId, val);
		}
		break;
	case COLUMN_TYPE_TIMESTAMP:
		{
			Timestamp val = static_cast<Timestamp>(*reinterpret_cast<const int64_t*>(value.data()));
			setField<COLUMN_TYPE_TIMESTAMP>(columnId, val);
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
}

/*!
	@brief Set variable-type field value
*/
void OutputMessageRowStore::addVariableFieldPart(
		const void *data, uint32_t size) {
	assert(isVariableColumn(lastColumnId_));
	assert(fieldsInitialized_[lastColumnId_] == true);

	varDataOut_.writeAll(data, size);

	assert((fieldsInitialized_[lastColumnId_] = true, true));
}

/*!
	@brief Set array-type field value
*/
void OutputMessageRowStore::setArrayField(ColumnId columnId) {
	assert(getColumnInfo(columnId).isArray());

	assert((fieldsInitialized_[columnId] = true, true));
	lastColumnId_ = columnId;
}

/*!
	@brief Set element of array-type field value
*/
void OutputMessageRowStore::addArrayElement(const void *data, uint32_t size) {
	assert(getColumnInfo(lastColumnId_).isArray());
	assert(fieldsInitialized_[lastColumnId_] == true);

	if (getColumnInfo(lastColumnId_).getColumnType() != COLUMN_TYPE_STRING_ARRAY) {
		assert(size == getColumnElementFixedSize(getColumnInfo(lastColumnId_)));
	}
	varDataOut_.writeAll(data, size);
}

/*!
	@brief Set header of variable-type field value
*/
void OutputMessageRowStore::setVarDataHeaderField(
		ColumnId columnId, uint32_t size) {
	setVarSize(size);
	assert((fieldsInitialized_[columnId] = true, true));
	lastColumnId_ = columnId;
}

/*!
	@brief Get size of fixed data area
*/
uint32_t OutputMessageRowStore::getFullRowFixedPartSize() const {
	return rowImageSize_;
}

/*!
	@brief Get offset of fixed data area
*/
size_t OutputMessageRowStore::getFixedDataOffset(ColumnId columnId) const {
	return fixedDataOut_.base().position() +
			(rowIdIncluded_ ? sizeof(RowId) : 0) +
			getColumnInfo(columnId).getColumnOffset();
}

/*!
	@brief Get pointer and size of fixed data area
*/
void OutputMessageRowStore::getAllFixedPart(
		const uint8_t *&data, uint32_t &size) const {
	data = fixedData_.data();
	size = static_cast<uint32_t>(fixedData_.size());
}

/*!
	@brief Get pointer and size of variable data area
*/
void OutputMessageRowStore::getAllVariablePart(
		const uint8_t *&data, uint32_t &size) const {
	data = varData_.data();
	size = static_cast<uint32_t>(varData_.size());
}

MessageRowKeyCoder::MessageRowKeyCoder(ColumnType keyType) :
		keyType_(keyType) {
	switch (keyType) {
	case COLUMN_TYPE_STRING:
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_LONG:
	case COLUMN_TYPE_TIMESTAMP:
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
	}
}

/*!
	@brief Decode Row key
*/
void MessageRowKeyCoder::decode(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<uint8_t> &rowKey) const {
	rowKey.clear();

	uint32_t size;
	if (keyType_ == COLUMN_TYPE_STRING) {
		const size_t orgPos = in.base().position();

		size = MessageRowStore::getVarSize(in);
		size += (size <= OBJECT_MAX_1BYTE_LENGTH_VALUE) ? 1 : 4;

		in.base().position(orgPos);
	}
	else {
		size = FixedSizeOfColumnType[keyType_];
	}

	rowKey.resize(size);
	in.readAll(rowKey.data(), size);
}
