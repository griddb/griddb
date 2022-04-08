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
#include "sql_result_set.h"
#include "value_processor.h"
#include "transaction_service.h"

static const uint8_t NULLABLE_MASK = 0x80;

static ColumnType
convertTupleTypeToNoSQLTypeWithNullable(TupleList::TupleColumnType type) {
	ColumnType returnType;
	TupleList::TupleColumnType tmpType = static_cast<TupleList::TupleColumnType>((type & ~TupleList::TYPE_MASK_NULLABLE));
	switch (tmpType) {
	case TupleList::TYPE_BOOL: returnType = COLUMN_TYPE_BOOL;break;
	case TupleList::TYPE_BYTE: returnType = COLUMN_TYPE_BYTE;break;
	case TupleList::TYPE_SHORT: returnType = COLUMN_TYPE_SHORT;break;
	case TupleList::TYPE_INTEGER: returnType = COLUMN_TYPE_INT;break;
	case TupleList::TYPE_LONG: returnType = COLUMN_TYPE_LONG;break;
	case TupleList::TYPE_FLOAT: returnType = COLUMN_TYPE_FLOAT;break;
	case TupleList::TYPE_NUMERIC: returnType = COLUMN_TYPE_DOUBLE;break;
	case TupleList::TYPE_DOUBLE: returnType = COLUMN_TYPE_DOUBLE;break;
	case TupleList::TYPE_TIMESTAMP: returnType = COLUMN_TYPE_TIMESTAMP;break;
	case TupleList::TYPE_NULL: returnType = COLUMN_TYPE_NULL;break;
	case TupleList::TYPE_STRING: returnType = COLUMN_TYPE_STRING;break;
	case TupleList::TYPE_GEOMETRY: returnType = COLUMN_TYPE_GEOMETRY;break;
	case TupleList::TYPE_BLOB:returnType = COLUMN_TYPE_BLOB;break;
	case TupleList::TYPE_ANY: returnType = COLUMN_TYPE_ANY;break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
			"Unsupported type, type=" << static_cast<int32_t>(type));
	}
	if (TupleColumnTypeUtils::isNullable(type)) {
		returnType |= NULLABLE_MASK;
	}
	return returnType;
}

static bool isVariableTypeEx(ColumnType type) {
	if (type == COLUMN_TYPE_ANY) {
		return true;
	}
	ColumnType tmp = static_cast<ColumnType>(type & ~NULLABLE_MASK);
	switch (tmp) {
	case COLUMN_TYPE_STRING:
	case COLUMN_TYPE_GEOMETRY:
	case COLUMN_TYPE_BLOB:
		return true;
	default:
		return false;
	}
};

SQLResultSet::SQLResultSet(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
	globalVarAlloc_(globalVarAlloc),
	blockRowMax_(BLOCK_ROW_MAX),
	columnCount_(0),
	nullsBytes_(0),
	lastNullsPos_(0),
	realLastVarOffset_(0),
	rowCount_(0),
	rowKeyAssigned_(false),
	hasVariableType_(false),
	columnNameList_(globalVarAlloc_),
	columnTypeList_(globalVarAlloc_),
	blockList_(globalVarAlloc_),
	padding_(0), paddingBuffer_((const void*)&padding_) {
}

/*!
	@brief デストラクタ
*/
SQLResultSet::~SQLResultSet() {
	clear();
}

void SQLResultSet::setup() {
	try {
		clear();
		columnCount_ = static_cast<uint32_t>(columnNameList_.size());
		nullsBytes_ = ValueProcessor::calcNullsByteSize(columnCount_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLResultSet::setColumnTypeList(
	util::Vector<TupleList::TupleColumnType>& typeList) {
	hasVariableType_ = false;
	columnTypeList_.clear();
	for (size_t pos = 0; pos < typeList.size(); pos++) {
		ColumnType currentType
			= convertTupleTypeToNoSQLTypeWithNullable(typeList[pos]);
		if (isVariableTypeEx(currentType)) {
			hasVariableType_ = true;
		}
		columnTypeList_.push_back(currentType);
	}
	setup();
}

void SQLResultSet::exportSchema(util::StackAllocator& alloc,
	EventByteOutStream& out) {
	try {
		const size_t startPos = out.base().position();
		StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(out, 0);
		StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
			out, static_cast<uint32_t>(columnCount_));
		uint32_t keyColumnId;
		if (rowKeyAssigned_) {
			keyColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		}
		else {
			keyColumnId = -1;
		}
		uint32_t columnNo = 0;
		for (columnNo = 0; columnNo < columnCount_; columnNo++) {
			const util::String emptyColumnName("", alloc);
			StatementHandler::encodeStringData<EventByteOutStream>(out, emptyColumnName);
			uint8_t type = columnTypeList_[columnNo];
			const bool nullable = (type != COLUMN_TYPE_ANY &&
				((type & NULLABLE_MASK) != 0));
			if (nullable) {
				type &= ~NULLABLE_MASK;
			}
			out << type;
			const bool isArrayVal = false;
			uint8_t flags = 0;
			if (isArrayVal) {
				flags |= ColumnInfo::COLUMN_FLAG_KEY;
			}
			if (!nullable) {
				flags |= ColumnInfo::COLUMN_FLAG_NOT_NULL;
			}
			out << flags;
		}
		const int16_t keyCount = (keyColumnId >= 0 ? 0 : 1);
		StatementHandler::encodeIntData<EventByteOutStream>(out, keyCount);

		if (keyCount > 0) {
			StatementHandler::encodeIntData<EventByteOutStream>(
				out, static_cast<int16_t>(keyColumnId));
		}

		for (columnNo = 0; columnNo < columnCount_; columnNo++) {
			util::String str(columnNameList_[columnNo].c_str(), alloc);
			StatementHandler::encodeStringData<EventByteOutStream>(out, str);
		}

		const size_t endPos = out.base().position();
		out.base().position(startPos);
		StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
			out, static_cast<uint32_t>(endPos - startPos - sizeof(uint32_t)));
		out.base().position(endPos);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief データをエンコードする
	@param [in] alloc アロケータ
	@param [in, out] out 出力ストリーム
*/
void SQLResultSet::exportData(
	EventByteOutStream& out) {

	try {
		size_t pos;
		for (pos = 0; pos < blockList_.size(); pos++) {
			out.writeAll(static_cast<const uint8_t*>(blockList_[pos]->getFixedPart().data()),
				blockList_[pos]->getFixedPart().size());
		}
		for (pos = 0; pos < blockList_.size(); pos++) {
			out.writeAll(static_cast<const uint8_t*>(blockList_[pos]->getVarPart().data()),
				blockList_[pos]->getVarPart().size());
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLResultSet::next(util::StackAllocator& alloc) {

	SQLBlockInfo* blockInfo = NULL;
	try {
		if (blockRowMax_ >= cursor_.rowPos_ + 1) {
			cursor_.rowPos_++;
		}
		else {
			realLastVarOffset_ += getVarPart().size();
			blockInfo = ALLOC_NEW(alloc) SQLBlockInfo(alloc);
			blockList_.push_back(blockInfo);
			cursor_.next(blockInfo);
		}
		ResultSetStream& fixedPartOut = getFixedPartOut();
		ResultSetStream& varPartOut = getVarPartOut();

		if (hasVariableType_) {
			const uint64_t varPartOffset =
				realLastVarOffset_ + varPartOut.base().position();
			fixedPartOut << varPartOffset;
		}

		lastNullsPos_ = fixedPartOut.base().position();
		for (uint16_t pos = 0; pos < nullsBytes_; pos++) {
			fixedPartOut << static_cast<uint8_t>(0);
		}

		setVarSize(varPartOut, 0);
		cursor_.baseVarOffset_++;
		rowCount_++;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLResultSet::clear() {
	blockList_.clear();
	cursor_.init();
	realLastVarOffset_ = 0;
	rowCount_ = 0;
}

void SQLResultSet::setNull(size_t index, bool nullValue) {
	if (nullValue) {
		(*cursor_.fixedPart_)[
			lastNullsPos_ + index / CHAR_BIT] |= 1U << (index % CHAR_BIT);
	}
}

void SQLResultSet::init(util::StackAllocator& alloc) {
	assert(columnCount_ > 0);
	assert(columnNameList_.size() > 0);
	assert(columnTypeList_.size() > 0);
	SQLBlockInfo* blockInfo = ALLOC_NEW(alloc) SQLBlockInfo(alloc);
	blockList_.push_back(blockInfo);
	cursor_.next(blockInfo);
}

void SQLResultSet::setVarSize(util::ByteStream< util::XArrayOutStream<> >& varPartOut,
	size_t varSize) {
	if (varSize < VAR_SIZE_1BYTE_THRESHOLD) {
		varPartOut << ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(varSize));
	}
	else if (varSize < VAR_SIZE_4BYTE_THRESHOLD) {
		varPartOut << ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(varSize));
	}
	else {
		if (varSize > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
			UTIL_THROW_ERROR(GS_ERROR_DS_OUT_OF_RANGE, "");
		}
		varPartOut << ValueProcessor::encode8ByteVarSize(static_cast<uint64_t>(varSize));
	}
}