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
#include "sql_processor_dml.h"
#include "sql_execution.h"
#include "sql_execution_manager.h"
#include "sql_compiler.h"
#include "sql_service.h"
#include "nosql_db.h"
#include "nosql_container.h"
#include "nosql_utils.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

const DMLProcessor::Registrar DMLProcessor::deleteRegistrar_(
		SQLType::EXEC_DELETE);
const DMLProcessor::Registrar DMLProcessor::updateRegistrar_(
		SQLType::EXEC_UPDATE);
const DMLProcessor::Registrar DMLProcessor::insertRegistrar_(
		SQLType::EXEC_INSERT);

DMLProcessor::DMLProcessor(
		Context &cxt, const TypeInfo &typeInfo) :
				SQLProcessor(cxt, typeInfo),
				jobStackAlloc_(cxt.getAllocator()), 
				globalVarAlloc_(
						cxt.getExecutionManager()->getVarAllocator()),
				resourceSet_(cxt.getResourceSet()),
				bulkManager_(NULL),
				inputTupleList_(globalVarAlloc_),
				inputTupleReaderList_(globalVarAlloc_),
				groupList_(globalVarAlloc_),
				inputSize_(0),
				nosqlColumnList_(NULL),
				nosqlColumnCount_(0),
				inputTupleColumnList_(NULL),
				columnTypeList_(NULL),
				tupleInfoList_(NULL),
				inputTupleColumnCount_(0),
				immediated_(false),
				dmlType_(DML_NONE),
				completedNum_(0),
				currentPos_(0),
				clientId_(*cxt.getClientId()),
				isTimeSeries_(false),
				partitioningInfo_(NULL) {
}

DMLProcessor::~DMLProcessor() {
	
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, bulkManager_);


	for (TupleListReaderArray::iterator itTupleReader
			= inputTupleReaderList_.begin();
			itTupleReader != inputTupleReaderList_.end();
					itTupleReader++) {

		(*itTupleReader)->close();
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, (*itTupleReader));
	}

	for (TupleListArray::iterator
			itTuple = inputTupleList_.begin();
			itTuple != inputTupleList_.end();
			itTuple++) {

		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, (*itTuple));
	}

	for (GroupListArray::iterator
			itGroup = groupList_.begin();
			itGroup != groupList_.end(); itGroup++) {

		ALLOC_VAR_SIZE_DELETE
				(globalVarAlloc_, (*itGroup));
	}

	if (tupleInfoList_) {
	
		for (size_t pos = 0; pos < inputSize_; pos++) {
			ALLOC_VAR_SIZE_DELETE(
					globalVarAlloc_, tupleInfoList_[pos]);
		}

		if (inputTupleColumnList_)  {

			globalVarAlloc_.deallocate(
					columnTypeList_);
			globalVarAlloc_.deallocate(
					inputTupleColumnList_);
			
			columnTypeList_ = NULL;
			inputTupleColumnList_ = NULL;
		}
	}

	globalVarAlloc_.deallocate(tupleInfoList_);
	globalVarAlloc_.deallocate(nosqlColumnList_);
	
	ALLOC_VAR_SIZE_DELETE(
			globalVarAlloc_, partitioningInfo_);
}

bool DMLProcessor::applyInfo(
		Context &cxt,
		const Option &option,
		const TupleInfoList &inputInfo,
		TupleInfo &outputInfo) {

	try {
		util::StackAllocator &alloc
				= cxt.getEventContext()->getAllocator();
		inputSize_ = static_cast<int32_t>(inputInfo.size());

		if (inputSize_ != 1) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
					"Invalid input info, dml input size must be 0, "
					"but current = " << inputSize_);
		}

		inputTupleColumnCount_
				= static_cast<int32_t>(inputInfo[0].size());

		if (inputTupleColumnCount_ == 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DML_INTERNAL,
					"Invalid dml input info, column count = 0");
		}

		if (option.plan_ == NULL
				|| (option.plan_ != NULL &&
						option.plan_->nodeList_.size()
								<= option.planNodeId_)) {

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DML_INTERNAL,
					"Invalid dml input info");
		}

		util::XArray<ColumnType> nosqlColumnTypeList(alloc);
		const SQLPreparedPlan::Node &node
				= option.plan_->nodeList_[option.planNodeId_];
		
		partitioningInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				PartitioningInfo(jobStackAlloc_);
		partitioningInfo_->copy(
				*node.partitioningInfo_);

		switch (node.type_) {

		case SQLType::EXEC_INSERT: {
			
			dmlType_ = DML_INSERT;
			nosqlColumnCount_
					=  static_cast<int32_t>((*node.nosqlTypeList_).size());

			break;
		}

		case SQLType::EXEC_UPDATE: {
			
			dmlType_ = DML_UPDATE;
			nosqlColumnCount_ =  static_cast<int32_t>(
					(*node.nosqlTypeList_).size());

			break;
		}

		case SQLType::EXEC_DELETE: {

			dmlType_ = DML_DELETE;
			if (inputTupleColumnCount_ != RESERVE_COLUMN_NUM) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DML_INTERNAL,
						"Invalid column count = " << inputTupleColumnCount_
						<< ", must be " << RESERVE_COLUMN_NUM);
			}

			nosqlColumnCount_ = 0;
			break;
		}

		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DML_INTERNAL,
					"Undefined dml type");
		}

		ColumnType columnType;
		ColumnId colId;
		bool hasVarData = false;
		util::XArray<uint8_t> isVariableList(alloc);
		uint8_t variable = 0;
		
		for (colId = 0; colId < static_cast<ColumnId>(nosqlColumnCount_); colId++) {

			columnType = (*node.nosqlTypeList_)[colId];
			variable = NoSQLUtils::isVariableType(columnType);
			isVariableList.push_back(variable);

			if (NoSQLUtils::isVariableType(columnType)) {
				hasVarData = true;
			}
			nosqlColumnTypeList.push_back(columnType);
		}

		if (nosqlColumnCount_ > 0) {
			nosqlColumnList_ = static_cast<ColumnInfo*>(
					globalVarAlloc_.allocate(
							sizeof(ColumnInfo) * nosqlColumnCount_));
		}

		uint16_t fixDataOffset = 0;
		uint16_t varDataPos = 0;

		fixDataOffset = static_cast<uint16_t>(
				ValueProcessor::calcNullsByteSize(
						nosqlColumnCount_));

		if (hasVarData) {
			fixDataOffset += static_cast<uint16_t>(sizeof(OId));
		}

		for (colId = 0; colId < static_cast<ColumnId>(nosqlColumnCount_); colId++) {
		
			ColumnInfo &columnInfo = nosqlColumnList_[colId];
			columnInfo.initialize();
			columnInfo.setColumnId(static_cast<uint16_t>(colId));
			columnInfo.setType(nosqlColumnTypeList[colId], false);

			uint8_t option = (*node.nosqlColumnOptionList_)[colId];
			if (ColumnInfo::isNotNull(option)) {
				columnInfo.setNotNull();
			}

			if (NoSQLUtils::isVariableType(
					nosqlColumnTypeList[colId])) {

				columnInfo.setOffset(varDataPos);
				varDataPos++;
			}
			else {
				columnInfo.setOffset(fixDataOffset);
				fixDataOffset += static_cast<uint16_t>(
						NoSQLUtils::getFixedSize(
								nosqlColumnTypeList[colId]));
			}
		}

		tupleInfoList_ = static_cast<TupleList::Info**>(
				globalVarAlloc_.allocate(
						sizeof(TupleList::Info) * inputSize_));

		for (int32_t inputPos = 0;
				inputPos < static_cast<int32_t>(inputSize_);
						inputPos++) {
		
			tupleInfoList_[inputPos]
					= ALLOC_VAR_SIZE_NEW(globalVarAlloc_) TupleList::Info;
			
			tupleInfoList_[inputPos]->columnCount_
					= inputTupleColumnCount_;

			if (inputTupleColumnList_ == NULL) {
			
				columnTypeList_ = static_cast<TupleList::TupleColumnType*>(
						globalVarAlloc_.allocate(
								sizeof(TupleList::TupleColumnType)
										* tupleInfoList_[inputPos]->columnCount_));
				
				for (size_t colId = 0; colId
						< tupleInfoList_[inputPos]->columnCount_;
								colId++) {

						columnTypeList_[colId]
								= inputInfo[inputPos][colId];
				}
			}

			inputTupleColumnList_ = static_cast<TupleList::Column*>(
					globalVarAlloc_.allocate(
							static_cast<int32_t>(sizeof(TupleList::Column)
									* tupleInfoList_[inputPos]->columnCount_)));

			tupleInfoList_[inputPos]->columnTypeList_
					= columnTypeList_;
			tupleInfoList_[inputPos]->getColumns(
					inputTupleColumnList_,
					tupleInfoList_[inputPos]->columnCount_);
			
			groupList_.push_back(
					ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
							TupleList::Group(cxt.getStore()));

			inputTupleList_.push_back(
					ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
							TupleList(
									*groupList_.back(),
									*tupleInfoList_[inputPos]));

			TupleList::Reader *reader =
					ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
							TupleList::Reader(
									*inputTupleList_.back(),
									TupleList::Reader::ORDER_SEQUENTIAL);

			inputTupleReaderList_.push_back(reader);
		}

		bulkManager_= ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			BulkManager(jobStackAlloc_, globalVarAlloc_,
			dmlType_, cxt.getExecutionManager());

		bool isInsertReplace = false;

		if (node.cmdOptionFlag_ == static_cast<int32_t>(
				SyntaxTree::RESOLVETYPE_REPLACE)) {
			
					isInsertReplace = true;
		}
		bulkManager_->setup(
				partitioningInfo_,
				inputTupleColumnList_,
				inputTupleColumnCount_,
				nosqlColumnList_,
				nosqlColumnCount_,
				node.insertColumnMap_,
				isInsertReplace
		);

		outputInfo.push_back(TupleList::TYPE_LONG);
		
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool DMLProcessor::pipe(
		Context &cxt,
		InputId inputId,
		const TupleList::Block &block) {

	try {
		inputTupleList_[inputId]->append(block);
		if (isImmediate()) {
			processInternal(cxt, true);
		}
		else {
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

bool DMLProcessor::finish(
		Context &cxt,
		InputId inputId) {

	UNUSED_VARIABLE(inputId);

	try {
		if (!checkCompleted()) {
			return false;
		}
		if (processInternal(cxt, false)) {
			int64_t resultCount = bulkManager_->getExecCount();
			
			TupleList::Block block;
			getResultCountBlock(cxt, resultCount, block);
			cxt.transfer(block);
			
			cxt.finish();
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	return false;
}

bool DMLProcessor::processInternal(Context &cxt, bool isPipe) {

	NoSQLDB::TableLatch *tableLatch = NULL;
	util::StackAllocator &alloc = cxt.getEventAllocator();

	try {

		int64_t execCount = 0;
		bool finished = true;

		EventMonotonicTime startTime, currentTime;
		EventContext &ec = *cxt.getEventContext();
		
		startTime = cxt.getEventContext()
				->getHandlerStartMonotonicTime();
		ExecutionLatch latch(
				clientId_,
				cxt.getExecutionManager()->getResourceManager(),
				NULL);

		SQLExecution *execution = latch.get();
		if (execution == NULL) {
			return false;
		}

		const SQLExecution::SQLExecutionContext &sqlContext
				= execution->getContext();
		cxt.setExecution(execution);
		cxt.setSyncContext(
				&execution->getContext().getSyncContext());
		
		bulkManager_->clear();

		NoSQLDB *db = resourceSet_->getSQLExecutionManager()->getDB();
		
		const NameWithCaseSensitivity tableName(
				partitioningInfo_->tableName_.c_str(), 
				partitioningInfo_->isCaseSensitive_);
		
		tableLatch = ALLOC_NEW(alloc) NoSQLDB::TableLatch(
				ec, db, execution,
				sqlContext.getDBName(), tableName, true);

		TableSchemaInfo *tableSchema = tableLatch->get();
		if (tableSchema == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
				"Table name '" << tableName.name_ << "' not found");
		}

		if (tableSchema->containerAttr_ == CONTAINER_ATTR_VIEW) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}

		cxt.setTableSchema(tableSchema);
		
		if (partitioningInfo_->partitioningType_
				!= tableSchema->partitionInfo_.partitionType_) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
		}
		
		bulkManager_->begin();

		TupleValue::VarContext varCxt;
		varCxt.setStackAllocator(&alloc);
		varCxt.setVarAllocator(&cxt.getVarAllocator());
		
		LocalTempStore::Group group(cxt.getStore());
		varCxt.setGroup(&group);

		InputId start = currentPos_;
		InputId pos;
		
		bool needRefresh = false;
		bool remoteUpdateCache = false; 
		for (pos = start;
				pos < static_cast<InputId>(
						inputTupleList_.size()); pos++) {

			TupleList::Reader &reader = *inputTupleReaderList_[pos];
			reader.setVarContext(varCxt);
			while (reader.exists()) {
				bulkManager_->import(
						cxt, reader, needRefresh);

				if (needRefresh) {
					ALLOC_DELETE(alloc, tableLatch);
					
					tableLatch = ALLOC_NEW(alloc) 
							NoSQLDB::TableLatch(ec, db, execution,
									sqlContext.getDBName(), tableName, false);

					cxt.setTableSchema(tableLatch->get());
					
					remoteUpdateCache = true;
				}

				reader.next();
				execCount++;

				if ((execCount & UNIT_CHECK_COUNT) == 0) {
					
					cxt.checkCancelRequest();
					currentTime = cxt.getMonotonicTime();
					
					if (currentTime - startTime > UNIT_LIMIT_TIME) {
						finished = false;
						break;
					}
				}
			}
			DMLProcessor::BulkManager::BulkEntry *currentStore;
			
			while ((currentStore
					= bulkManager_->next()) != NULL) {
				currentStore->execute(cxt, dmlType_, true);
			}

			if (reader.exists()) {
				finished = false;
				break;
			}
			currentPos_++;
		}
		if (isPipe) {
			currentPos_ = 0;
		}
		else {
			if (finished) {
				DMLProcessor::BulkManager::BulkEntry *currentStore;
				while ((currentStore
						= bulkManager_->next()) != NULL) {
					currentStore->commit(cxt);
				}
			}
		}

		bulkManager_->clear();		
		
		if (remoteUpdateCache) {
			TableSchemaInfo *schemaInfo = tableLatch->get();
			if (schemaInfo) {
		
				SQLExecution::updateRemoteCache(
						*cxt.getEventContext(),
						resourceSet_->getSQLService()->getEE(),
						resourceSet_,
						execution->getContext().getDBId(),
						execution->getContext().getDBName(),
						tableName.name_,
						schemaInfo->partitionInfo_.partitioningVersionId_);
			}
		}

		ALLOC_DELETE(alloc, tableLatch);
		return finished;
	}
	catch (std::exception &e) {
		DMLProcessor::BulkManager::BulkEntry *currentStore;
		
		while ((currentStore
				= bulkManager_->next()) != NULL) {
			currentStore->abort(cxt);
		}

		bulkManager_->clear();		
		
		ALLOC_DELETE(alloc, tableLatch);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

DMLProcessor::BulkManager::~BulkManager() {
	
	bulkEntryList_.clear();
	if (insertColumnMap_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, insertColumnMap_);
	}
}

void DMLProcessor::BulkManager::setup(
		PartitioningInfo *partitioningInfo,
		TupleList::Column *inputColumnList,
		int32_t inputColumnSize,
		ColumnInfo *nosqlColumnInfoList,
		int32_t nosqlColumnSize,
		InsertColumnMap *insertColumnMap,
		bool isInsertReplace) {
	
	if (insertColumnMap
			&& insertColumnMap->size() > 0) {

		insertColumnMap_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				InsertColumnMap(jobStackAlloc_);

		insertColumnMap_->assign(
				insertColumnMap->begin(),
				insertColumnMap->end());
	}

	try {
		if (partitioningInfo != NULL) {

			switch (partitioningInfo->partitioningType_) {

				case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF: 
				case SyntaxTree::TABLE_PARTITION_TYPE_HASH: 

					partitioningColumnId_ = partitioningInfo->partitioningColumnId_;
					partitioningNum_
							= static_cast<int32_t>(
									partitioningInfo->subInfoList_.size());

					if (partitioningNum_ == 0) partitioningNum_ = 1;
					
					bulkEntryList_.assign(
							partitioningNum_,
							static_cast<BulkEntry *>(NULL));

					break;

				default:
					break;
			}
		}

		columnList_ = inputColumnList;
		columnCount_ = inputColumnSize;
		nosqlColumnList_ = nosqlColumnInfoList;
		nosqlColumnSize_ = nosqlColumnSize;
		partitioningInfo_ = partitioningInfo;
		isInsertReplace_ = isInsertReplace;
		partitioningType_ = partitioningInfo->partitioningType_;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

DMLProcessor::BulkManager::BulkEntry::~BulkEntry() {

	if (container_ != NULL) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, container_);
		container_ = NULL;
	}

	for (size_t pos = 0; pos < entries_.size(); pos++) {
	
		if (entries_[pos] != NULL) {
			ALLOC_DELETE(
					alloc_, entries_[pos]);
			entries_[pos] = NULL;
		}
	}
}

void DMLProcessor::BulkManager::BulkEntry::setup(
		Context &cxt,
		DmlType type,
		ColumnInfo *nosqlColumnInfoList,
		uint32_t nosqlColumnCount,
		bool isInsertReplace,
		bool isPartitioning,
		TargetContainerInfo &targetInfo) {
	
	Operation *entry = NULL;
	
	try {
	
		EventContext &ec = *cxt.getEventContext();
		container_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				NoSQLContainer(
						ec,
						targetInfo.containerId_,
						targetInfo.versionId_,
						targetInfo.pId_,
						*cxt.getSyncContext(),
						cxt.getExecution());

		if (cxt.isDMLTimeSeries()) {
			container_->setContainerType(
					TIME_SERIES_CONTAINER);
		}

		for (int32_t target = 0; target < DML_MAX; target++) {
	
			if (target == type) {

				switch (type) {
				case DML_INSERT:

					entry = ALLOC_NEW(alloc_)
							Insert(
									alloc_,
									nosqlColumnInfoList,
									nosqlColumnCount,
									container_,
									pos_,
									isInsertReplace);

					if (isPartitioning) {
						entry->setPartitioning();
					}
					break;

				case DML_UPDATE:
					
					entry = ALLOC_NEW(alloc_)
							Update(
									alloc_,
									nosqlColumnInfoList,
									nosqlColumnCount,
									container_,
									pos_);

					break;

				case DML_DELETE:
					
					entry = ALLOC_NEW(alloc_)
							Delete(
									alloc_,
									nosqlColumnInfoList,
									nosqlColumnCount,
									container_,
									pos_);

					break;
				}
			}
			else {
				entry = NULL;
			}

			if (entry) {

				entry->setDataStore(cxt.getDataStore());
			}

			entries_.push_back(entry);
			entry = NULL;
		}

		partitionNum_
				= cxt.getPartitionTable()->getPartitionNum();
	}
	catch (std::exception &e) {

		if (nosqlColumnInfoList == NULL) {
			GS_TRACE_ERROR(SQL_SERVICE, 0, "EXCEPTION");
		}

		if (entry != NULL) {
			ALLOC_DELETE(alloc_, entry);
		}

		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

TupleValue *DMLProcessor::BulkManager::setTupleValue(
			TupleList::Reader &reader,
			ColumnId columnId,
			TupleValue &value) {

	if (columnId != UNDEF_COLUMNID) {
		
		if (insertColumnMap_) {
		
			int32_t columnPos = (*insertColumnMap_)[columnId];
			if (columnPos != -1) {
				value = reader.get().get(columnList_[columnPos]);
			}
		}
		else {
				value = reader.get().get(columnList_[columnId]);
		}
		return &value;
	}
	return NULL;
}

void DMLProcessor::BulkManager::import(
		Context &cxt,
		TupleList::Reader &reader,
		bool &needRefresh) {

	needRefresh = false;
	EventContext &ec = *cxt.getEventContext();
	
	TableSchemaInfo *tableSchema = cxt.getTableSchema();	
	SQLExecution *execution = cxt.getExecution();
	
	switch (type_) {
	
	case DML_INSERT: {
	
		BulkManager::BulkEntry *store = NULL;
		TableContainerInfo *containerInfo = NULL;
		NameWithCaseSensitivity dbName(
				execution->getContext().getDBName(),
				false);

		NameWithCaseSensitivity tableName(
				partitioningInfo_->tableName_.c_str(), 
				partitioningInfo_->isCaseSensitive_);

		NoSQLDB *db = executionManager_->getDB();
		const TupleValue *value1 = NULL;
		const TupleValue *value2 = NULL;
		
		TupleValue tmpValue1(NULL, TupleList::TYPE_NULL);
		TupleValue tmpValue2(NULL, TupleList::TYPE_NULL);

		if (insertColumnMap_) {
			
			for (ColumnId columnId = 0;
					columnId < insertColumnMap_->size(); columnId++) {

				int32_t columnPos = (*insertColumnMap_)[columnId];
				if (columnPos == -1) {
					
					if (nosqlColumnList_[columnId].isNotNull()) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_DML_INSERT_INVALID_NULL_CONSTRAINT,
								"Target column (position="
								<< columnId << ") is not nullable");
					}
				}
			}
		}
		
		value1 = setTupleValue(
				reader,
				partitioningInfo_->partitioningColumnId_,
				tmpValue1);
		
		value2 = setTupleValue(
				reader,
				partitioningInfo_->subPartitioningColumnId_,
				tmpValue2);

		TargetContainerInfo targetInfo;
		
		NoSQLUtils::resolveTargetContainer(
				ec,
				value1,
				value2,
				db,
				tableSchema,
				execution,
				dbName,
				tableName,
				targetInfo,
				needRefresh);

		NodeAffinityNumber pos;

		if (partitioningInfo_->partitioningColumnId_
				!= UNDEF_COLUMNID) {

			if (partitioningInfo_->getPartitioningType()
					== SyntaxTree::TABLE_PARTITION_TYPE_HASH
						|| partitioningInfo_->getPartitioningType()
								== SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {

				if (targetInfo.pos_ == 0) {
					return;
				}
				store = addValue(
						cxt, targetInfo.pos_ - 1, targetInfo);
			}
			else {
				store = addValue(
						cxt, targetInfo.affinity_, targetInfo);
			}
		}
		else {

			pos = 0;
			containerInfo
					= &tableSchema->containerInfoList_[0];
			targetInfo.containerId_ = containerInfo->containerId_;
			targetInfo.pId_ = containerInfo->pId_;
			targetInfo.versionId_ = containerInfo->versionId_;
			store = addValue(cxt, 0, targetInfo);
		}

		OutputMessageRowStore &rowStore
				= store->getOperation(DML_INSERT)->getRowStore();
		
		rowStore.beginRow();
		
		if (insertColumnMap_) {
		
			for (uint32_t columnId = 0;
					columnId < static_cast<uint32_t>(
						insertColumnMap_->size()); columnId++) {

					appendValue(
							reader,
							(*insertColumnMap_)[columnId],
							columnId,
							columnId,
							rowStore);
			}
		}
		else {
			for (int32_t columnId = 0;
					columnId < columnCount_; columnId++) {

				appendValue(
						reader,
						columnId,
						columnId,
						columnId,
						rowStore);
			}
		}

		store->getOperation(DML_INSERT)->next();
		
		store->execute(cxt, DML_INSERT, false);

		execCount_++;
	}
	break;

	case DML_UPDATE: {

		RowId rowId = reader.get().get(
				columnList_[ROW_COLUMN_ID]).get<int64_t>();

		size_t subContainerId = static_cast<size_t>(
				reader.get().get(columnList_[SUBCONTAINER_COLUMN_ID]).
						get<int64_t>());

		BulkManager::BulkEntry *store = NULL;
		TargetContainerInfo targetInfo;
		
		if (subContainerId
						> partitioningInfo_->subInfoList_.size()) {

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
					"Target pos="
					<< subContainerId << " is out of range, max="
					<< partitioningInfo_->subInfoList_.size());
		}

		SQLTableInfo::SubInfo &currentSubInfo
				= partitioningInfo_->subInfoList_[subContainerId];

		targetInfo.containerId_
				= currentSubInfo.containerId_;

		targetInfo.pId_
				= currentSubInfo.partitionId_;

		targetInfo.versionId_
				= currentSubInfo.schemaVersionId_;

		store = addPosition(
			cxt, subContainerId, targetInfo);

		DMLProcessor::BulkManager::Update *updateOp
				= static_cast<DMLProcessor::BulkManager::Update *>(
						store->getOperation(DML_UPDATE));

		OutputMessageRowStore &rowStore = updateOp->getRowStore();
		
		rowStore.beginRow();
		int32_t outColumnId = 0;
		
		for (int32_t columnId = RESERVE_COLUMN_NUM;
				columnId < columnCount_;
				columnId++, outColumnId++) {

			appendValue(
					reader,
					columnId,
					outColumnId,
					outColumnId,
					rowStore);
		}

		updateOp->next();
		updateOp->append(cxt, rowId);
		
		if (updateOp->full()) {
			updateOp->execute(cxt);
		}
		
		execCount_++;
		
		return;
	}
	break;

	case DML_DELETE: {	

		RowId rowId = reader.get().get(
				columnList_[ROW_COLUMN_ID]).get<int64_t>();

		size_t subContainerId = static_cast<size_t>(
				reader.get().get(columnList_[SUBCONTAINER_COLUMN_ID]).
						get<int64_t>());

		BulkManager::BulkEntry *store = NULL;
		TargetContainerInfo targetInfo;

		if (subContainerId
					> partitioningInfo_->subInfoList_.size()) {

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
					"Target pos=" << subContainerId
					<< " is out of range, max="
					<< partitioningInfo_->subInfoList_.size());
		}

		SQLTableInfo::SubInfo &currentSubInfo
				= partitioningInfo_->subInfoList_[subContainerId];

		targetInfo.containerId_
				= currentSubInfo.containerId_;

		targetInfo.pId_
				= currentSubInfo.partitionId_;
		
		targetInfo.versionId_
				= currentSubInfo.schemaVersionId_;

		store = addPosition(cxt, subContainerId, targetInfo);
		
		DMLProcessor::BulkManager::Delete *deleteOp
				= static_cast<DMLProcessor::BulkManager::Delete *>(
						store->getOperation(DML_DELETE));

		deleteOp->append(cxt, rowId);

		if (deleteOp->full()) {
			deleteOp->execute(cxt);
		}
		execCount_++;
		return;
	}
	break;

	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DML_INTERNAL, "");
	}
}

void DMLProcessor::BulkManager::appendValue(
		TupleList::Reader &reader,
		ColumnId inputColumnId,
		ColumnId inputNoSQLColumnId,
		ColumnId outputColumnId,
		OutputMessageRowStore &rowStore) {

	if (inputColumnId == UNDEF_COLUMNID) {

		const TupleValue value(NULL, TupleList::TYPE_NULL);

		DMLProcessor::setField(
				value.getType(),
				outputColumnId,
				value,
				rowStore);
	}
	else {
		const TupleValue &value = reader.get().get(columnList_[inputColumnId]);

		DMLProcessor::setField(
				value.getType(),
				outputColumnId,
				value,
				rowStore);
	}
}

DMLProcessor::BulkManager::Operation::Operation(
		util::StackAllocator &alloc,
		const ColumnInfo *columnInfoList,
		uint32_t columnCount,
		NoSQLContainer *container,
		size_t pos) :
				alloc_(alloc),
				columnInfoList_(columnInfoList),
				columnCount_(columnCount),
				container_(container),
				pos_(pos),
				option_(NULL),
				totalProcessedNum_(0),
				currentProcessedNum_(0),
				fixedData_(NULL),
				varData_(NULL),
				rowStore_(NULL),
				rowIdList_(NULL),
				threashold_(0),
				partitioning_(false),
				dataStore_(NULL)  {

	option_ = ALLOC_NEW(alloc_) NoSQLStoreOption;
}

DMLProcessor::BulkManager::Operation::~Operation() {

	if (option_ != NULL) {
		ALLOC_DELETE(alloc_, option_);
	}
}


void DMLProcessor::BulkManager::Operation::
		commit(Context &cxt) {

	UNUSED_VARIABLE(cxt);
	container_->commit(*option_);
}

void DMLProcessor::BulkManager::Operation::
		abort(Context &cxt) {

	UNUSED_VARIABLE(cxt);
	container_->abort(*option_);
}

void DMLProcessor::BulkManager::Insert::
		execute(Context &cxt) {

	try {

		if (rowStore_->getRowCount() == 0) {
			return;
		}
		
		try {
		
			container_->setExecution(
					cxt.getExecution());
			container_->setEventContext(
					cxt.getEventContext());

			container_->putRowSet(
					*fixedData_,
					*varData_,
					rowStore_->getRowCount(),
					*option_);
		}
		catch (util::Exception &e) {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}

		initialize();
		currentProcessedNum_ = 0;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void DMLProcessor::BulkManager::Update::
		execute(Context &cxt) {

	UNUSED_VARIABLE(cxt);

	if (rowIdList_->size() == 0) return;
	
	container_->setExecution(
			cxt.getExecution());
	container_->setEventContext(
			cxt.getEventContext());

	if (container_->getContainerType()
			== TIME_SERIES_CONTAINER) {

		container_->putRowSet(
				*fixedData_,
				*varData_,
				static_cast<uint64_t>(
						rowIdList_->size()),
				*option_);
	}
	else {

		container_->updateRowSet(
				*fixedData_,
				*varData_,
				*rowIdList_,
				*option_);
	}

	initialize();
	currentProcessedNum_ = 0;
}

void DMLProcessor::BulkManager::Delete::
		execute(Context &cxt) {

	if (rowIdList_->size() == 0) {
		return;
	}	

	container_->setExecution(
			cxt.getExecution());
	container_->setEventContext(
			cxt.getEventContext());
	
	container_->deleteRowSet(
			*rowIdList_,
			*option_);

	initialize();
}

void DMLProcessor::BulkManager::Delete::
		append(Context &cxt, RowId rowId) {

	UNUSED_VARIABLE(cxt);

	try {
		rowIdList_->push_back(rowId);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void DMLProcessor::BulkManager::Update::
		append(Context &cxt, RowId rowId) {

	UNUSED_VARIABLE(cxt);

	try {
		rowIdList_->push_back(rowId);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

DMLProcessor::BulkManager::Operation
		*DMLProcessor::BulkManager::getOperation(
						InputId target, DmlType type) {

	return bulkEntryList_[target]->getOperation(type);
}

void SQLProcessor::getResultCountBlock(
		Context &cxt,
		int64_t resultCount,
		TupleList::Block &block) {

	try {

		TupleList::Info outTupleInfo;
		outTupleInfo.columnCount_ = 1;
		
		TupleList::TupleColumnType outColumnTypeList[1];
		outColumnTypeList[0] = TupleList::TYPE_LONG;
		outTupleInfo.columnTypeList_
				= outColumnTypeList;
		
		TupleList::Group outGroup(cxt.getStore());
		TupleList outputTuple(outGroup, outTupleInfo);
		
		TupleList::Writer writer(outputTuple);
		
		TupleList::Column column;
		outputTuple.getInfo().getColumns(&column, 1);

		writer.next();
		writer.get().set(column, TupleValue(resultCount));
		writer.close();
		
		TupleList::BlockReader blockReader(outputTuple);
		blockReader.next(block);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void DMLProcessor::BulkManager::Update::
		initialize() {
	
	if (fixedData_) {
		ALLOC_DELETE(alloc_, fixedData_);
	}
	if (varData_) {
		ALLOC_DELETE(alloc_, varData_);
	}
	if (rowStore_) {
		ALLOC_DELETE(alloc_, rowStore_);
	}
	if (rowIdList_) {
		rowIdList_->clear();
	}
	else {
		rowIdList_ = ALLOC_NEW(alloc_)
				util::XArray<RowId>(alloc_);
	}

	rowIdList_->clear();

	fixedData_ = ALLOC_NEW(alloc_) util::XArray<uint8_t>(alloc_);
	varData_ = ALLOC_NEW(alloc_) util::XArray<uint8_t>(alloc_);

	rowStore_ = ALLOC_NEW(alloc_)
			OutputMessageRowStore(
					dataStore_->getValueLimitConfig(),
					columnInfoList_,
					columnCount_,
					*fixedData_,
					*varData_,
					false);
}

DMLProcessor::BulkManager::Insert::Insert(
		util::StackAllocator &alloc,
		const ColumnInfo *columnInfoList,
		uint32_t columnCount,
		NoSQLContainer *container,
		size_t pos,
		bool isInsertReplace) :
				Operation(
						alloc,
						columnInfoList,
						columnCount,
						container,
						pos) {

	threashold_ = BULK_INSERT_THRESHOLD_COUNT;
	
	if (isInsertReplace) {
		option_->putRowOption_ = PUT_INSERT_OR_UPDATE;
	}
	else {
		option_->putRowOption_ = PUT_INSERT_ONLY;
	}
	initialize();
}

DMLProcessor::BulkManager::Update::Update(
		util::StackAllocator &alloc,
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

	threashold_ = BULK_UPDATE_THRESHOLD_COUNT;
	option_->putRowOption_ = PUT_UPDATE_ONLY;

	initialize();
}

void DMLProcessor::BulkManager::Insert::initialize() {

	if (fixedData_) {
		ALLOC_DELETE(alloc_, fixedData_);
	}
	if (varData_) {
		ALLOC_DELETE(alloc_, varData_);
	}
	if (rowStore_) {
		ALLOC_DELETE(alloc_, rowStore_);
	}

	fixedData_ = ALLOC_NEW(alloc_) util::XArray<uint8_t>(alloc_);
	
	varData_ = ALLOC_NEW(alloc_) util::XArray<uint8_t>(alloc_);

	rowStore_ = ALLOC_NEW(alloc_)
			OutputMessageRowStore(
					dataStore_->getValueLimitConfig(),
					columnInfoList_,
					columnCount_,
					*fixedData_,
					*varData_,
					false);
}

void DMLProcessor::BulkManager::Delete::initialize() {

	if (rowIdList_) {
		rowIdList_->clear();
	}
	else {
		rowIdList_ = ALLOC_NEW(alloc_) util::XArray<RowId>(alloc_);
	}
}

void DMLProcessor::exportTo(
		Context &cxt,
		const OutOption &option) const {

	UNUSED_VARIABLE(cxt);
	UNUSED_VARIABLE(option);
}

DMLProcessor::BulkManager::BulkEntry *DMLProcessor::BulkManager::
		addValue(
				Context &cxt,
				NodeAffinityNumber value,
				TargetContainerInfo &targetInfo) {

	bool isPartitioning = true;
	util::StackAllocator
			&alloc = cxt.getEventContext()->getAllocator();

	switch (partitioningType_) {
	
		case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF: 

			isPartitioning = false;

		case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
		
			size_t pos = static_cast<size_t>(value);

			if (bulkEntryList_.size() > 0) {
				if (pos < bulkEntryList_.size()
						&& bulkEntryList_[pos] != NULL) {

					return bulkEntryList_[pos];
				}
			}

			if (pos == static_cast<size_t>(-1)
					|| bulkEntryList_.size() <= pos) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH, "");
			}

			bulkEntryList_[pos] = ALLOC_NEW(jobStackAlloc_)
					BulkEntry(
							alloc,
							globalVarAlloc_,
							pos,
							type_);

			bulkEntryList_[pos]->setup(
					cxt,
					type_,
					nosqlColumnList_,
					nosqlColumnSize_,
					isInsertReplace_,
					isPartitioning,
					targetInfo);

			return 	bulkEntryList_[pos];
		}
		break;

		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE: 
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:  {
		
			util::Map<NodeAffinityNumber, BulkEntry*>::iterator
					it = bulkEntryListMap_.find(value);

			if (it == bulkEntryListMap_.end()) {

				BulkEntry *bulkEntry = ALLOC_NEW(jobStackAlloc_)
						BulkEntry(
								alloc,
								globalVarAlloc_,
								static_cast<size_t>(value),
								type_);

				bulkEntry->setup(
						cxt,
						type_,
						nosqlColumnList_,
						nosqlColumnSize_,
						isInsertReplace_,
						isPartitioning,
						targetInfo);

				bulkEntryList_.push_back(bulkEntry);
				bulkEntryListMap_.insert(
						std::make_pair(value, bulkEntry));
				
				return bulkEntry;
			}
			else {
				return (*it).second;
			}
		}
		break;
	}
	return NULL;
}

DMLProcessor::BulkManager::BulkEntry *DMLProcessor::BulkManager::
		add(
				Context &cxt,
				NodeAffinityNumber pos, 
				TableContainerInfo *containerInfo,
				TargetContainerInfo &targetInfo) {

	bool isPartitioning = true;
	util::StackAllocator &alloc
			= cxt.getEventContext()->getAllocator();

	switch (partitioningType_) {
	
		case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF: 
		
			isPartitioning = false;

		case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
		
			size_t hashPos = static_cast<size_t>(pos);
			if (bulkEntryList_.size() > 0) {

				if (pos < bulkEntryList_.size()
						&& bulkEntryList_[hashPos] != NULL) {
					return bulkEntryList_[hashPos];
				}
			}

			if (hashPos == static_cast<size_t>(-1)
					|| bulkEntryList_.size() <= hashPos) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH, "");
			}

			bulkEntryList_[hashPos] = ALLOC_NEW(jobStackAlloc_)
					BulkEntry(
							alloc,
							globalVarAlloc_,
							hashPos,
							type_);

			bulkEntryList_[hashPos]->setup(
					cxt,
					type_,
					nosqlColumnList_,
					nosqlColumnSize_,
					isInsertReplace_,
					isPartitioning,
					targetInfo);

			return 	bulkEntryList_[hashPos];
		}
		break;

		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE: 
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:  {

			util::Map<NodeAffinityNumber, BulkEntry*>::iterator
					it = bulkEntryListMap_.find(pos);

			if (it == bulkEntryListMap_.end()) {

				BulkEntry *bulkEntry = ALLOC_NEW(jobStackAlloc_)
						BulkEntry(
								alloc,
								globalVarAlloc_,
								static_cast<size_t>(pos),
								type_);

				bulkEntry->setup(
						cxt,
						type_,
						nosqlColumnList_,
						nosqlColumnSize_,
						isInsertReplace_,
						isPartitioning,
						targetInfo);

				bulkEntryList_.push_back(bulkEntry);
				bulkEntryListMap_.insert(
						std::make_pair(pos, bulkEntry));

				return bulkEntry;
			}
			else {
				return (*it).second;
			}
		}
		break;
	}
	return NULL;
}

DMLProcessor::BulkManager::BulkEntry*
		DMLProcessor::BulkManager::addPosition(
				Context &cxt,
				size_t pos,
				TargetContainerInfo &targetInfo) {

	util::StackAllocator &alloc = cxt.getEventContext()->getAllocator();

	if (pos < bulkEntryList_.size() 
			&& bulkEntryList_[pos] != NULL) {

		return bulkEntryList_[pos];
	}

	if (bulkEntryList_.size() == 0) {

		bulkEntryList_.assign(
				partitioningInfo_->subInfoList_.size(),
				static_cast<BulkEntry *>(NULL));
	}

	bulkEntryList_[pos] = ALLOC_NEW(jobStackAlloc_)
			BulkEntry(
					alloc,
					globalVarAlloc_,
					pos,
					type_);

	bulkEntryList_[pos]->setup(
			cxt,
			type_,
			nosqlColumnList_,
			nosqlColumnSize_,
			isInsertReplace_, 
			(partitioningType_ != 
					SyntaxTree::TABLE_PARTITION_TYPE_UNDEF),
			targetInfo);

	return 	bulkEntryList_[pos];
}

DMLProcessor::BulkManager::BulkEntry*DMLProcessor::BulkManager::
		next() {

	if (prevStore_) {

		ALLOC_DELETE(jobStackAlloc_, prevStore_);
		bulkEntryList_[prevPos_]
				= static_cast<BulkEntry*>(NULL);
	}

	while (currentPos_ < bulkEntryList_.size()) {

		if (bulkEntryList_[currentPos_] != NULL) {

			prevPos_ = currentPos_;
			prevStore_ = bulkEntryList_[currentPos_];
			return bulkEntryList_[currentPos_];
		}
		currentPos_++;
	}

	return NULL;
}