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
#include "nosql_utils.h"



SQLResultSet::SQLResultSet(
		SQLVariableSizeGlobalAllocator &globalVarAlloc) :
			globalVarAlloc_(globalVarAlloc),
			blockRowMax_(BLOCK_ROW_MAX),
			columnCount_(0),
			nullsBytes_(0),
			lastNullsPos_(0),
			realLastVarOffset_(0),
			rowCount_(0),
			rowKeyAssigned_(false),
			hasVariableType_(false),
			blockList_(globalVarAlloc_),
			columnNameList_(globalVarAlloc_),
			columnTypeList_(globalVarAlloc_),
			padding_(0),
			paddingBuffer_((const void*)&padding_){
}

SQLResultSet::~SQLResultSet() {
	clear();
}

void SQLResultSet::setup() {
	try {
		clear();
		columnCount_
				= static_cast<uint32_t>(columnNameList_.size());
		nullsBytes_
				= ValueProcessor::calcNullsByteSize(columnCount_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLResultSet::setColumnTypeList(
		util::Vector<TupleList::TupleColumnType> &typeList) {

	hasVariableType_ = false;
	columnTypeList_.clear();
	for (size_t pos = 0; pos < typeList.size(); pos++) {
		ColumnType currentType
			= NoSQLUtils::convertTupleTypeToNoSQLTypeWithNullable(
					typeList[pos]);

		if (NoSQLUtils::isVariableType(currentType)) {
			hasVariableType_ = true;
		}
		columnTypeList_.push_back(currentType);
	}
	setup();
}

void SQLResultSet::exportSchema(
		util::StackAllocator &alloc,
		EventByteOutStream &out) {

	try {
		
		const size_t startPos = out.base().position();
		StatementHandler::encodeIntData<uint32_t>(out, 0);
		StatementHandler::encodeIntData<uint32_t>(
				out, static_cast<uint32_t>(columnCount_));
		
		uint32_t keyColumnId;
		if (rowKeyAssigned_) {
			keyColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		}
		else {
			keyColumnId = UNDEF_COLUMNID;
		}

		uint32_t columnNo = 0;
		for (columnNo = 0; columnNo < columnCount_; columnNo++) {
		
			const util::String emptyColumnName("", alloc);
			StatementHandler::encodeStringData(out, emptyColumnName);
			uint8_t type = columnTypeList_[columnNo];
			const bool nullable = (type != COLUMN_TYPE_ANY &&
					((type & NoSQLUtils::NULLABLE_MASK) != 0));
			if (nullable) {
				type &= ~NoSQLUtils::NULLABLE_MASK;
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
		StatementHandler::encodeIntData(out, keyCount);

		if (keyCount > 0) {
			StatementHandler::encodeIntData(
					out, static_cast<int16_t>(keyColumnId));
		}

		for (columnNo = 0; columnNo < columnCount_; columnNo++) {
			util::String str(
					columnNameList_[columnNo].c_str(), alloc);
			StatementHandler::encodeStringData(out, str);
		}

		const size_t endPos = out.base().position();
		out.base().position(startPos);
		StatementHandler::encodeIntData<uint32_t>(
			out, static_cast<uint32_t>(
					endPos - startPos - sizeof(uint32_t)));
		out.base().position(endPos);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLResultSet::exportData(
		EventByteOutStream &out) {

	try {
		size_t pos;
		for (pos = 0; pos < blockList_.size(); pos++) {
			out.writeAll(
					static_cast<const uint8_t*>(
							blockList_[pos]->getFixedPart().data()),
				blockList_[pos]->getFixedPart().size());
		}

		for (pos = 0; pos < blockList_.size(); pos++) {
			out.writeAll(
					static_cast<const uint8_t*>(
							blockList_[pos]->getVarPart().data()),
				blockList_[pos]->getVarPart().size());
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLResultSet::next(util::StackAllocator &alloc) {

	SQLBlockInfo *blockInfo = NULL;
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
		ResultSetStream &fixedPartOut = getFixedPartOut();
		ResultSetStream &varPartOut = getVarPartOut();

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
	catch (std::exception &e) {
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
		(*cursor_.fixedPart_)[lastNullsPos_ + index / CHAR_BIT]
				|= 1U << (index % CHAR_BIT);
	}
}

void SQLResultSet::init(util::StackAllocator &alloc) {
	
	assert(columnCount_ > 0);
	assert(columnNameList_.size() > 0);
	assert(columnTypeList_.size() > 0);
	
	SQLBlockInfo *blockInfo = ALLOC_NEW(alloc) SQLBlockInfo(alloc);
	blockList_.push_back(blockInfo);
	cursor_.next(blockInfo);
}

void SQLResultSet::setVarSize(
		util::ByteStream< util::XArrayOutStream<> > &varPartOut,
		size_t varSize) {

	if (varSize < VAR_SIZE_1BYTE_THRESHOLD) {
		varPartOut << ValueProcessor::encode1ByteVarSize(
				static_cast<uint8_t>(varSize));
	} else if (varSize < VAR_SIZE_4BYTE_THRESHOLD) {
		varPartOut << ValueProcessor::encode4ByteVarSize(
				static_cast<uint32_t>(varSize));
	} else {
		if (varSize > static_cast<uint32_t>(
				std::numeric_limits<int32_t>::max())) {
			UTIL_THROW_ERROR(GS_ERROR_DS_OUT_OF_RANGE, "");
		}
		varPartOut << ValueProcessor::encode8ByteVarSize(
				static_cast<uint64_t>(varSize));
	}
}