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
	@brief Definition of sql resultset
*/

#ifndef SQL_RESULT_SET_H_
#define SQL_RESULT_SET_H_

#include "sql_common.h"
#include "data_store_common.h"
#include "event_engine.h"
#include "sql_tuple.h"

class SQLResultSet {

	static const int64_t BLOCK_ROW_MAX = 65535;
	typedef util::ByteStream< util::XArrayOutStream<> > ResultSetStream;
	struct SQLBlockInfo {
		SQLBlockInfo(util::StackAllocator& alloc) :
			alloc_(alloc), fixedPart_(alloc), varPart_(alloc),
			fixedPartOut_(util::ByteStream< util::XArrayOutStream<> >(
				util::XArrayOutStream<>(fixedPart_))),
			varPartOut_(util::ByteStream< util::XArrayOutStream<> >(
				util::XArrayOutStream<>(varPart_))) {
		}
		~SQLBlockInfo() {}
		util::XArray<uint8_t>& getFixedPart() {
			return fixedPart_;
		}

		util::XArray<uint8_t>& getVarPart() {
			return varPart_;
		}

		int64_t getSize() {
			return (fixedPart_.size() + varPart_.size());
		}

		ResultSetStream& getFixedPartOut() {
			return fixedPartOut_;
		}

		ResultSetStream& getVarPartOut() {
			return varPartOut_;
		}

		util::StackAllocator& alloc_;
		util::XArray<uint8_t> fixedPart_;
		util::XArray<uint8_t> varPart_;
		util::ByteStream< util::XArrayOutStream<> > fixedPartOut_;
		util::ByteStream< util::XArrayOutStream<> > varPartOut_;
	};

	struct SQLCursor {

		SQLCursor() {
			init();
		}

		void init() {
			blockNo_ = -1;
			block_ = NULL;
			fixedPart_ = NULL;
			varPart_ = NULL;
			fixedPartOut_ = NULL;
			varPartOut_ = NULL;
			rowPos_ = -1;
			baseVarOffset_ = 0;
		}

		void next(SQLBlockInfo* block) {
			blockNo_++;
			block_ = block;
			fixedPart_ = &block->getFixedPart();
			varPart_ = &block->getVarPart();
			fixedPartOut_ = &block->getFixedPartOut();
			varPartOut_ = &block->getVarPartOut();
			rowPos_ = 0;
			baseVarOffset_ = 0;
		}

		int64_t blockNo_;
		SQLBlockInfo* block_;
		util::XArray<uint8_t>* fixedPart_;
		util::XArray<uint8_t>* varPart_;
		util::ByteStream< util::XArrayOutStream<> >* fixedPartOut_;
		util::ByteStream< util::XArrayOutStream<> >* varPartOut_;
		int64_t rowPos_;
		int64_t baseVarOffset_;
	};

public:

	typedef std::vector<SQLString, util::StdAllocator<
		SQLString, SQLVariableSizeGlobalAllocator> > ResultSetColumnNameList;
	typedef std::vector<ColumnType, util::StdAllocator<
		ColumnType, SQLVariableSizeGlobalAllocator> > ResultSetColumnTypeList;
	typedef std::vector<SQLBlockInfo*, util::StdAllocator<
		SQLBlockInfo*, SQLVariableSizeGlobalAllocator> > ResultSetBlockList;

	SQLResultSet(SQLVariableSizeGlobalAllocator& globalVarAlloc);

	~SQLResultSet();

	void resetSchema() {
		columnCount_ = 0;
		columnNameList_.clear();
		columnTypeList_.clear();
		clear();
	}

	void init(util::StackAllocator& alloc);

	void setup();


	void exportSchema(util::StackAllocator& alloc, EventByteOutStream& out);

	void exportData(EventByteOutStream& out);

	void next(util::StackAllocator& alloc);
	void clear();

	int64_t getSize() {
		int64_t size = 0;
		for (size_t pos = 0; pos < blockList_.size(); pos++) {
			size += blockList_[pos]->getSize();
		}
		return size;
	}

	void setNull(size_t index, bool nullValue);

	void setFixedNull() {
		(*cursor_.fixedPartOut_) << COLUMN_TYPE_NULL;
		(*cursor_.fixedPartOut_) << static_cast<int64_t>(0);
	}

	template <typename T> void setFixedValueWithPadding(ColumnType type, const void* value) {
		(*cursor_.fixedPartOut_) << type;
		(*cursor_.fixedPartOut_).writeAll(value, sizeof(T));
		(*cursor_.fixedPartOut_).writeAll(paddingBuffer_, sizeof(int64_t) - sizeof(T));
	}

	template <typename T> void setFixedValue(ColumnType type, const void* value) {
		(*cursor_.fixedPartOut_) << type;
		(*cursor_.fixedPartOut_).writeAll(value, sizeof(T));
	}

	void setVariable(ColumnType type, const char* value, size_t size) {
		size_t startPos = cursor_.varPartOut_->base().position();
		setVarSize(*cursor_.varPartOut_, size);
		if (size > 0) {
			cursor_.varPartOut_->writeAll(value, size);
		}
		(*cursor_.fixedPartOut_) << type;
		(*cursor_.fixedPartOut_) << cursor_.baseVarOffset_;
		cursor_.baseVarOffset_ += (cursor_.varPartOut_->base().position() - startPos);
	}
	template <typename T> void setNull(ColumnType type) {
		(*cursor_.fixedPartOut_) << COLUMN_TYPE_NULL;
	}

	void setVarFieldValue(const void* value, size_t size) {
		size_t startPos = cursor_.varPartOut_->base().position();
		setVarSize(*cursor_.varPartOut_, size);
		if (size > 0) {
			cursor_.varPartOut_->writeAll(value, size);
		}
		cursor_.baseVarOffset_ += (cursor_.varPartOut_->base().position() - startPos);
	}

	void setFixedFieldValue(const void* value, size_t size) {
		(*cursor_.fixedPartOut_).writeAll(value, size);
	}
	int64_t getRowCount() {
		return rowCount_;
	}
	void setRowCount(int64_t rowCount) {
		rowCount_ = rowCount;
	}
	void setVarSize(util::ByteStream< util::XArrayOutStream<> >& varPartOut, size_t varSize);
	util::XArray<uint8_t>& getFixedPart() {
		return *cursor_.fixedPart_;
	}
	util::XArray<uint8_t>& getVarPart() {
		return *cursor_.varPart_;
	}
	ResultSetStream& getFixedPartOut() {
		return *cursor_.fixedPartOut_;
	}
	ResultSetStream& getVarPartOut() {
		return *cursor_.varPartOut_;
	}

	void getColumnNameList(util::StackAllocator& alloc, util::Vector<util::String>& columnNameList) {
		for (size_t pos = 0; pos < columnNameList_.size(); pos++) {
			columnNameList.push_back(util::String(columnNameList_[pos].c_str(), alloc));
		}
	}

	void setColumnNameList(util::Vector<util::String>& columnNameList) {
		for (size_t pos = 0; pos < columnNameList.size(); pos++) {
			SQLString key(globalVarAlloc_);
			key = columnNameList[pos].c_str();
			columnNameList_.push_back(key);
		}
	}

	void setColumnTypeList(util::Vector<TupleList::TupleColumnType>& typeList);

private:

	SQLVariableSizeGlobalAllocator& globalVarAlloc_;

	int64_t blockRowMax_;
	uint32_t columnCount_;
	uint32_t nullsBytes_;
	size_t lastNullsPos_;
	uint64_t realLastVarOffset_;
	int64_t rowCount_;
	bool rowKeyAssigned_;
	bool hasVariableType_;
	ResultSetBlockList blockList_;
	ResultSetColumnNameList columnNameList_;
	ResultSetColumnTypeList columnTypeList_;
	SQLCursor cursor_;
	const int64_t padding_;
	const void* paddingBuffer_;
};

#endif
