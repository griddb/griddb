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
	@brief Definition of sql processor dml
*/
#ifndef SQL_PROCESSOR_DML_H_
#define SQL_PROCESSOR_DML_H_

#include "sql_common.h"
#include "sql_processor.h"
#include "data_store_common.h"
#include "message_row_store.h"
#include "sql_compiler.h"
#include "container_key.h"

class NoSQLContainer;
struct TableContainerInfo;
class ColumnInfo;
struct TableSchemaInfo;
struct NoSQLStoreOption;
struct TargetContainerInfo;

/*!
	@brief DMLProcessor
*/
class DMLProcessor : public SQLProcessor {

	typedef SQLContext Context;
	typedef int32_t AccessControlType;
	
	typedef util::AllocVector<TupleList*> TupleListArray;
	typedef util::AllocVector<TupleList::Group*> GroupListArray;
	typedef util::AllocVector<TupleList::Reader*> TupleListReaderArray;

	typedef SQLPreparedPlan::Node::PartitioningInfo PartitioningInfo;
	typedef SQLTableInfo::IdInfo TableIdInfo;

	static const int32_t ROW_COLUMN_ID = 1;
	static const int32_t SUBCONTAINER_COLUMN_ID = 0;
	static const int32_t RESERVE_COLUMN_NUM = ROW_COLUMN_ID + 1;
	static const int32_t UNIT_PROCESS_NUM = 5000000;
	static const int32_t UNIT_LIMIT_TIME = 2*1000;
	static const int32_t UNIT_CHECK_COUNT = 127;

	static const uint64_t BULK_INSERT_THRESHOLD_COUNT = 4096;
	static const uint64_t BULK_UPDATE_THRESHOLD_COUNT = 4096;
	static const uint64_t BULK_DELETE_THRESHOLD_COUNT = 100000; 
	static const uint64_t BULK_INSERT_THRESHOLD_SIZE = 2 *1024*1024; 
	static const uint64_t BULK_UPDATE_THRESHOLD_SIZE = 2 *1024*1024; 

	friend class DDLProcessor;

public:

	typedef int32_t DmlType;
	static const int32_t DML_INSERT = 0;
	static const int32_t DML_UPDATE = 1;
	static const int32_t DML_DELETE = 2;
	static const int32_t DML_MAX = 3;
	static const int32_t DML_NONE = -1;

	DMLProcessor(Context &cxt, const TypeInfo &typeInfo);

	virtual ~DMLProcessor();

	virtual bool pipe(
			Context &cxt,
			InputId inputId,
			const Block &block);

	virtual bool finish(
			Context &cxt,
			InputId inputId);

	virtual bool applyInfo(
			Context &cxt,
			const Option &option,
			const TupleInfoList &inputInfo,
			TupleInfo &outputInfo);

	void exportTo(
			Context &cxt, const OutOption &option) const;

	static void setField(
			TupleList::TupleColumnType type,
			ColumnId columnId,
			const TupleValue &value,
			OutputMessageRowStore &rowStore) {

	switch (type) {

		case TupleList::TYPE_STRING: {
		
			if (value.varSize() > static_cast<size_t>(INT32_MAX)) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_PROC_LIMIT_EXCEEDED, 
						"String size limit exceeded");
			}

			const TupleString::BufferInfo &info
					= TupleString(value).getBuffer();

				rowStore.setFieldForRawData(
					columnId,
					info.first,
					static_cast<uint32_t>(info.second));
		}

		break;

		case TupleList::TYPE_BLOB: {
	
			const void *data;
			size_t size;
			size_t totalSize = 0;
			
			TupleValue::LobReader readerForSize(value);
			while (readerForSize.next(data, size)) {
				totalSize += size;
			}

			if (totalSize > static_cast<size_t>(INT32_MAX)) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_PROC_LIMIT_EXCEEDED, 
						"BLOB size limit exceeded");
			}
			
			rowStore.setVarDataHeaderField(
						columnId,
						static_cast<uint32_t>(totalSize));
			
			TupleValue::LobReader readerValue(value);
			while (readerValue.next(data, size)) {
				
				rowStore.addVariableFieldPart(
						data,
						static_cast<uint32_t>(size));
			}
		}
		break;

		case TupleList::TYPE_BOOL:
		case TupleList::TYPE_BYTE:
		case TupleList::TYPE_SHORT:
		case TupleList::TYPE_INTEGER:
		case TupleList::TYPE_LONG:
		case TupleList::TYPE_FLOAT:
		case TupleList::TYPE_NUMERIC:
		case TupleList::TYPE_DOUBLE:
		case TupleList::TYPE_TIMESTAMP:

			rowStore.setField(
					columnId,
					value.fixedData(),
					static_cast<uint32_t>(
							TupleColumnTypeUtils::getFixedSize(type)));

		break;

		case TupleList::TYPE_NULL:
		
			rowStore.setNull(columnId);
		
		break;
		
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DML_INTERNAL, "");
	}
}

private:

	class BulkManager;
	typedef Factory::Registrar<DMLProcessor> Registrar;

	bool processInternal(
			Context &cxt, bool isPipe);

	bool isImmediate() {
		return immediated_;
	}

	bool checkCompleted() {

		completedNum_++;
		
		if (completedNum_ >= inputTupleList_.size()) {
			return true;
		}
		else {
			return false;
		}
	}

	util::StackAllocator &jobStackAlloc_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	const ResourceSet *resourceSet_;

	BulkManager *bulkManager_;
	TupleListArray inputTupleList_;
	TupleListReaderArray inputTupleReaderList_;
	GroupListArray groupList_;
	size_t inputSize_;
	ColumnInfo *nosqlColumnList_;
	int32_t nosqlColumnCount_;
	TupleList::Column *inputTupleColumnList_;
	TupleList::TupleColumnType *columnTypeList_;
	TupleList::Info **tupleInfoList_;
	int32_t inputTupleColumnCount_;
	bool immediated_;
	DmlType dmlType_;
	uint32_t completedNum_;
	InputId currentPos_;
	ClientId clientId_;
	bool isTimeSeries_;
	PartitioningInfo *partitioningInfo_;
	static const Registrar deleteRegistrar_;
	static const Registrar updateRegistrar_;
	static const Registrar insertRegistrar_;
};

class DMLProcessor::BulkManager {
public:

	typedef SQLContext Context;
	static const int32_t UNDEF_INPUTID = -1;
	class BulkEntry;
	typedef util::Vector<BulkEntry*> BulkEntryArray;
	typedef util::Vector<int32_t> InsertColumnMap;
	typedef util::Vector<uint8_t> NoSQLColumnInfoList;

		BulkManager(
				util::StackAllocator &jobStackAlloc,
				SQLVariableSizeGlobalAllocator &globalVarAlloc,
				DMLProcessor::DmlType dmlType,
				SQLExecutionManager *executionManager) :
						jobStackAlloc_(jobStackAlloc),
						globalVarAlloc_(globalVarAlloc),
						type_(dmlType),
						partitioningColumnId_(UNDEF_COLUMNID),
						partitioningNum_(0),
						columnCount_(0),
						columnList_(NULL),
						nosqlColumnList_(NULL),
						nosqlColumnSize_(0),
						execCount_(0),
						bulkEntryList_(jobStackAlloc),
						bulkEntryListMap_(jobStackAlloc),
						partitioningType_(0),
						insertColumnMap_(NULL),
						targetColumnId_(0), 
						isInsertReplace_(false),
						currentPos_(0),
						prevPos_(-1),
						prevStore_(NULL),
						partitionNum_(0),
						executionManager_(executionManager) {}
		~BulkManager();

		void clear() {

			bulkEntryListMap_.clear();
			
			for (size_t pos = 0;
					pos < bulkEntryList_.size(); pos++) {

				ALLOC_DELETE(
						jobStackAlloc_, bulkEntryList_[pos]);
				
				if (partitioningType_ <= 1) {
					bulkEntryList_[pos] = NULL;
				}
			}

			if (partitioningType_ > 1) {
				bulkEntryList_.clear();
			}
		}

		void setup(
				PartitioningInfo *partitioningInfo,
				TupleList::Column *inputColumnList,
				int32_t inputColumnSize,
				ColumnInfo *columnList,
				int32_t columnSize,
				InsertColumnMap *insertColumnMap,
				bool isReplace);


		void import(
				Context &cxt,
				TupleList::Reader &reader,
				bool &needRefresh);

		int64_t getExecCount() {
			return execCount_;
		}

		BulkEntry *getBulkEntry(InputId target) {
			return bulkEntryList_[target];
		}

		BulkEntry *add(
				Context &cxt,
				NodeAffinityNumber pos,
				TableContainerInfo *containerInfo,
				TargetContainerInfo &targetInfo);
		
		BulkEntry *addPosition(
				Context &cxt,
				size_t pos,
				TargetContainerInfo &targetInfo);

		BulkEntry *addValue(
				Context &cxt,
				NodeAffinityNumber affinity,
				TargetContainerInfo &targetInfo);

		void begin() {

			currentPos_ = 0;
			prevPos_ = 0;
			prevStore_ = NULL;
		}

		BulkEntry *next();

	private:

		class Operation;
		class Insert;
		class Update;
		class Delete;
		class BulkStore;

		TupleValue *setTupleValue(
				TupleList::Reader &reader,
				ColumnId columnId,
				TupleValue &value);

		void appendValue(
				TupleList::Reader &reader,
				ColumnId inputColumnId,
				ColumnId inputNoSQLColumnId,
				ColumnId outputColumnId,
				OutputMessageRowStore &rowStore);


		Operation *getOperation(
				InputId targetId, DmlType type);

		util::StackAllocator &jobStackAlloc_;
		SQLVariableSizeGlobalAllocator &globalVarAlloc_;
		DmlType type_;
		int32_t partitioningColumnId_;
		int32_t partitioningNum_;
		int32_t columnCount_;
		TupleList::Column *columnList_;
		ColumnInfo *nosqlColumnList_;
		int32_t nosqlColumnSize_;
		int64_t execCount_;
		BulkEntryArray bulkEntryList_;
		util::Map<NodeAffinityNumber, BulkEntry*> bulkEntryListMap_;
		uint8_t partitioningType_;
		PartitioningInfo *partitioningInfo_;
		InsertColumnMap *insertColumnMap_;
		int32_t targetColumnId_;
		bool isInsertReplace_;
		size_t currentPos_;
		size_t prevPos_;

		BulkEntry *prevStore_;
		int32_t partitionNum_;
		SQLExecutionManager *executionManager_;
	};

class DMLProcessor::BulkManager::Operation {

public:

	typedef SQLContext Context;

	Operation(
			util::StackAllocator &alloc,
			const ColumnInfo *columnInfoList,
			uint32_t columnCount,
			NoSQLContainer *container,
			size_t pos);

	~Operation();

	virtual void execute(Context &cxt) = 0;

	virtual bool full() = 0;

	void commit(Context &cxt);

	void abort(Context &cxt);

	OutputMessageRowStore &getRowStore() {
		return *rowStore_;
	}

	util::XArray<RowId> &getRowIdList() {
		return *rowIdList_;
	}

	virtual void next() = 0;

	virtual void initialize() = 0;

	util::StackAllocator &getAllocatorInfo() {
		return alloc_;
	}

	void setPartitioning() {
		partitioning_ = true;
	}

	bool isPartitioning() {
		return partitioning_;
	}

	void setDataStore(DataStore *dataStore) {
		dataStore_ = dataStore;
	}

protected:

	util::StackAllocator &alloc_;

	const ColumnInfo *columnInfoList_;
	uint32_t columnCount_;
	NoSQLContainer *container_;
	size_t pos_;
	NoSQLStoreOption *option_;

	int64_t totalProcessedNum_;
	int64_t currentProcessedNum_;

	util::XArray<uint8_t> *fixedData_;
	util::XArray<uint8_t> *varData_;
	OutputMessageRowStore *rowStore_;
	util::XArray<RowId> *rowIdList_;
	int64_t threashold_;
	bool partitioning_;
	DataStore *dataStore_;
};

class DMLProcessor::BulkManager::Insert : public Operation {
public:
	Insert(
			util::StackAllocator &alloc,
			const ColumnInfo *columnInfoList,
			uint32_t columnCount,
			NoSQLContainer *container,
			size_t pos,
			bool isInsertReplace);

	void execute(Context &cxt);

	virtual void initialize();
	
	bool full() {

		return ((fixedData_->size() + varData_->size())
				>= BULK_INSERT_THRESHOLD_SIZE);
	}

	void next() {
		
		totalProcessedNum_++;
		currentProcessedNum_++;
		rowStore_->next();
	}
};

class DMLProcessor::BulkManager::Update : public Operation {

public:
	Update(
			util::StackAllocator &alloc,
			const ColumnInfo *columnInfoList,
			uint32_t columnCount,
			NoSQLContainer *container,
			size_t pos);

	void execute(Context &cxt);

	void execute(Context &cxt, RowId rowId);

	void append(Context &cxt, RowId rowId);

	virtual void initialize();

	bool full() {

		return ((fixedData_->size() + varData_->size())
				>= BULK_UPDATE_THRESHOLD_SIZE);
	}

	void next() {
		
		totalProcessedNum_++;
		currentProcessedNum_++;
		rowStore_->next();
	}
};

class DMLProcessor::BulkManager::Delete : public Operation {
public:
	Delete(util::StackAllocator &alloc,
			const ColumnInfo *columnInfoList,
			uint32_t columnCount,
			NoSQLContainer *container,
			size_t pos) :
					Operation(
							alloc,
							columnInfoList,
							columnCount,
							container,
							pos) {

		threashold_ = BULK_DELETE_THRESHOLD_COUNT;
		initialize();
	}

	void execute(Context &cxt);

	void execute(Context &cxt, RowId rowId);

	void append(Context &cxt, RowId rowId);

	bool full() {

		return (threashold_
				<= static_cast<int64_t>(rowIdList_->size()));
	}

	virtual void initialize();

	void next() {

		totalProcessedNum_++;
		currentProcessedNum_++;
	}
};

class DMLProcessor::BulkManager::BulkEntry {

	typedef util::XArray<Operation*> OperationArray;

public:

	BulkEntry(
			util::StackAllocator &alloc,
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			size_t pos,
			DmlType type) :
					alloc_(alloc),
					globalVarAlloc_(globalVarAlloc),
					entries_(alloc),
					container_(NULL),
					pos_(pos),
					type_(type) {}

	~BulkEntry();

	void setup(
			Context &cxt,
			DmlType type,
			ColumnInfo *columnInfoList,
			uint32_t columnCount,
			bool isReplace,
			bool isPartitioning,
			TargetContainerInfo &info);

	Operation *getOperation(DmlType type) {
		return entries_[type];
	}

	void execute(
			Context &cxt,
			DmlType type,
			bool isForce) {

		if (entries_[type] != NULL
				&& (isForce || entries_[type]->full())) {

			entries_[type]->execute(cxt);
		}
	}

	void commit(Context &cxt) {
		
		for (DmlType type = 0; type <
				static_cast<DmlType>(entries_.size()); type++) {

			if (entries_[type] == NULL) continue;
			
			entries_[type]->commit(cxt);
		}
	}

	void abort(Context &cxt) {

		for (DmlType type = 0; type <
				static_cast<DmlType>(entries_.size()); type++) {
		
			if (entries_[type] == NULL) continue;
			
			entries_[type]->abort(cxt);
		}
	}

private:

	util::StackAllocator &alloc_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	OperationArray entries_;
	NoSQLContainer *container_;
	size_t pos_;
	DmlType type_;
	int32_t partitionNum_;
};

#endif
