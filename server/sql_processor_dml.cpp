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
#include "sql_compiler.h"
#include "sql_command_manager.h"
#include "sql_service.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

const DMLProcessor::Registrar DMLProcessor::deleteRegistrar_(
	SQLType::EXEC_DELETE);
const DMLProcessor::Registrar DMLProcessor::updateRegistrar_(
	SQLType::EXEC_UPDATE);
const DMLProcessor::Registrar DMLProcessor::insertRegistrar_(
	SQLType::EXEC_INSERT);

/*!
	@brief コンストラクタ
	@note コンストラクタでは何もしない
	@note 以後、StackAllocatorは、Processorごとに割り当てたものをセットする
	@note タスクが消滅するまでイベントを跨って利用することが可能
*/
DMLProcessor::DMLProcessor(Context& cxt, const TypeInfo& typeInfo) :
	SQLProcessor(cxt, typeInfo),
	jobStackAlloc_(cxt.getAllocator()),
	globalVarAlloc_(cxt.getExecutionManager()->getVarAllocator()),
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
	partitioningInfo_(NULL),
	pt_(cxt.getExecutionManager()->getJobManager()->getPartitionTable()),
	executionManager_(cxt.getExecutionManager()) {
}

/*!
	@brief デストラクタ
*/
DMLProcessor::~DMLProcessor() {

	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, bulkManager_);
	for (TupleListReaderArray::iterator itTupleReader
		= inputTupleReaderList_.begin();
		itTupleReader != inputTupleReaderList_.end(); itTupleReader++) {
		(*itTupleReader)->close();
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, (*itTupleReader));
	}
	for (TupleListArray::iterator itTuple = inputTupleList_.begin();
		itTuple != inputTupleList_.end(); itTuple++) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, (*itTuple));
	}
	for (GroupListArray::iterator itGroup = groupList_.begin();
		itGroup != groupList_.end(); itGroup++) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, (*itGroup));
	}
	if (tupleInfoList_) {
		for (size_t pos = 0; pos < inputSize_; pos++) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, tupleInfoList_[pos]);
		}
		if (inputTupleColumnList_) {
			globalVarAlloc_.deallocate(columnTypeList_);
			globalVarAlloc_.deallocate(inputTupleColumnList_);
			columnTypeList_ = NULL;
			inputTupleColumnList_ = NULL;
		}
	}

	globalVarAlloc_.deallocate(tupleInfoList_);
	globalVarAlloc_.deallocate(nosqlColumnList_);
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, partitioningInfo_);
}

/*!
	@brief タスク情報のセット
*/
bool DMLProcessor::applyInfo(Context& cxt, const Option& option,
	const TupleInfoList& inputInfo, TupleInfo& outputInfo) {
	try {
		util::StackAllocator& eventStackAlloc = cxt.getEventContext()->getAllocator();
		inputSize_ = static_cast<int32_t>(inputInfo.size());
		if (inputSize_ != 1) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Invalid input info, dml input size must be 0, but current = " << inputSize_);
		}
		inputTupleColumnCount_ = static_cast<int32_t>(inputInfo[0].size());
		if (inputTupleColumnCount_ == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INTERNAL,
				"Invalid dml input info, column count = 0");
		}
		if (option.plan_ == NULL || (option.plan_ != NULL &&
			option.plan_->nodeList_.size() <= option.planNodeId_)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INTERNAL,
				"Invalid dml input info");
		}

		util::XArray<ColumnType> nosqlColumnTypeList(eventStackAlloc);
		const SQLPreparedPlan::Node& node
			= option.plan_->nodeList_[option.planNodeId_];
		partitioningInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			PartitioningInfo(jobStackAlloc_);
		partitioningInfo_->copy(*node.partitioningInfo_);

		const SQLTableInfo::IdInfo& idInfo = node.tableIdInfo_;

		switch (node.type_) {
		case SQLType::EXEC_INSERT: {
			dmlType_ = DML_INSERT;
			nosqlColumnCount_ = static_cast<int32_t>((*node.nosqlTypeList_).size());
			break;
		}
		case SQLType::EXEC_UPDATE: {
			dmlType_ = DML_UPDATE;
			nosqlColumnCount_ = static_cast<int32_t>((*node.nosqlTypeList_).size());
			break;
		}
		case SQLType::EXEC_DELETE: {
			dmlType_ = DML_DELETE;
			if (inputTupleColumnCount_ != RESERVE_COLUMN_NUM) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INTERNAL,
					"Invalid column count = " << inputTupleColumnCount_
					<< ", must be " << RESERVE_COLUMN_NUM);
			}
			nosqlColumnCount_ = 0;
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INTERNAL,
				"Undefined dml type");
		}

		ColumnType columnType;
		int32_t colId;
		uint16_t varTypeColumnCount = 0;
		bool hasVarData = false;
		util::XArray<uint8_t> isVariableList(eventStackAlloc);
		uint8_t variable = 0;
		for (colId = 0; colId < nosqlColumnCount_; colId++) {
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
				globalVarAlloc_.allocate(sizeof(ColumnInfo) * nosqlColumnCount_));
		}

		uint16_t fixDataOffset = 0;
		uint16_t varDataPos = 0;
		fixDataOffset = static_cast<uint16_t>(
			ValueProcessor::calcNullsByteSize(nosqlColumnCount_));
		if (hasVarData) {
			fixDataOffset += sizeof(OId);
		}

		for (colId = 0; colId < nosqlColumnCount_; colId++) {
			ColumnInfo& columnInfo = nosqlColumnList_[colId];
			columnInfo.initialize();
			columnInfo.setColumnId(colId);
			columnInfo.setType(nosqlColumnTypeList[colId], false);
			uint8_t option = (*node.nosqlColumnOptionList_)[colId];
			if (ColumnInfo::isNotNull(option)) {
				columnInfo.setNotNull();
			}
			if (NoSQLUtils::isVariableType(nosqlColumnTypeList[colId])) {
				columnInfo.setOffset(varDataPos);
				varDataPos++;
			}
			else {
				columnInfo.setOffset(fixDataOffset);
				fixDataOffset += NoSQLUtils::getFixedSize(nosqlColumnTypeList[colId]);
			}
		}

		tupleInfoList_ = static_cast<TupleList::Info**>(
			globalVarAlloc_.allocate(sizeof(TupleList::Info) * inputSize_));
		for (int32_t inputPos = 0;
			inputPos < static_cast<int32_t>(inputSize_); inputPos++) {
			tupleInfoList_[inputPos]
				= ALLOC_VAR_SIZE_NEW(globalVarAlloc_) TupleList::Info;
			tupleInfoList_[inputPos]->columnCount_ = inputTupleColumnCount_;
			if (inputTupleColumnList_ == NULL) {
				columnTypeList_ = static_cast<TupleList::TupleColumnType*>(
					globalVarAlloc_.allocate(sizeof(TupleList::TupleColumnType)
						* tupleInfoList_[inputPos]->columnCount_));
				for (size_t colId = 0; colId
					< tupleInfoList_[inputPos]->columnCount_; colId++) {
					columnTypeList_[colId] = inputInfo[inputPos][colId];
				}
			}

			inputTupleColumnList_ = static_cast<TupleList::Column*>(
				globalVarAlloc_.allocate(static_cast<int32_t>(sizeof(TupleList::Column)
					* tupleInfoList_[inputPos]->columnCount_)));
			tupleInfoList_[inputPos]->columnTypeList_ = columnTypeList_;
			tupleInfoList_[inputPos]->getColumns(
				inputTupleColumnList_, tupleInfoList_[inputPos]->columnCount_);
			groupList_.push_back(ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TupleList::Group(cxt.getStore()));
			inputTupleList_.push_back(ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TupleList(*groupList_.back(), *tupleInfoList_[inputPos]));
			TupleList::Reader* reader =
				ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TupleList::Reader(*inputTupleList_.back(),
					TupleList::Reader::ORDER_SEQUENTIAL);
			inputTupleReaderList_.push_back(reader);
		}

		bulkManager_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			BulkManager(this, jobStackAlloc_, globalVarAlloc_,
				dmlType_, cxt.getExecutionManager());

		bool isInsertReplace = false;
		if (node.cmdOptionFlag_ == static_cast<int32_t>(
			SyntaxTree::RESOLVETYPE_REPLACE)) {
			isInsertReplace = true;
		}
		bulkManager_->setup(cxt, idInfo, partitioningInfo_,
			inputTupleColumnList_, inputTupleColumnCount_,
			nosqlColumnList_, nosqlColumnCount_,
			node.insertColumnMap_, isInsertReplace
		);
		outputInfo.push_back(TupleList::TYPE_LONG);
		return true;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief pipe処理を実行する
*/
bool DMLProcessor::pipe(
	Context& cxt, InputId inputId, const TupleList::Block& block) {
	try {
		inputTupleList_[inputId]->append(block);
		if (isImmediate()) {
			processInternal(cxt, true);
		}
		else {
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

/*!
	@brief finish処理を実行する
*/
bool DMLProcessor::finish(Context& cxt, InputId inputId) {

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
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	return false;
}

/*!
	@brief バルク処理を実行する
	@note 入力タプルリストからカーソル読み出ししてバルク登録する
*/
bool DMLProcessor::processInternal(Context& cxt, bool isPipe) {

	TableLatch* tableLatch = NULL;
	util::StackAllocator& eventStackAlloc = cxt.getEventAllocator();
	cxt.setTableSchema(NULL, NULL);

	try {
		int64_t execCount = 0;
		bool finished = true;

		EventMonotonicTime startTime, currentTime;
		EventContext& ec = *cxt.getEventContext();
		startTime = cxt.getEventContext()->getHandlerStartMonotonicTime();
		SQLExecutionManager::Latch latch(clientId_, cxt.getExecutionManager());
		SQLExecution* execution = latch.get();
		if (execution == NULL) {
			return false;
		}

		const SQLExecution::SQLExecutionContext& sqlContext
			= execution->getContext();
		cxt.setExecution(execution);
		cxt.setSyncContext(&execution->getContext().getSyncContext());
		bulkManager_->clear();

		DBConnection* conn = executionManager_->getDBConnection();
		const NameWithCaseSensitivity tableName(
			partitioningInfo_->tableName_.c_str(),
			partitioningInfo_->isCaseSensitive_);

		tableLatch = ALLOC_NEW(eventStackAlloc) TableLatch(
			ec, conn, execution, sqlContext.getDBName(), tableName, true);

		TableSchemaInfo* tableSchema = tableLatch->get();
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
		cxt.setTableSchema(tableSchema, tableLatch);
		if (partitioningInfo_->partitioningType_
			!= tableSchema->partitionInfo_.partitionType_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
		}
		bulkManager_->begin();

		TupleValue::VarContext varCxt;
		varCxt.setStackAllocator(&eventStackAlloc);
		varCxt.setVarAllocator(&cxt.getVarAllocator());
		LocalTempStore::Group group(cxt.getStore());
		varCxt.setGroup(&group);

		InputId start = currentPos_;
		InputId pos;
		bool needRefresh = false;
		bool remoteUpdateCache = false;
		for (pos = start;
			pos < static_cast<InputId>(inputTupleList_.size()); pos++) {
			TupleList::Reader& reader = *inputTupleReaderList_[pos];
			reader.setVarContext(varCxt);
			while (reader.exists()) {
				bulkManager_->import(cxt, pos, reader, needRefresh);
				if (needRefresh) {
					ALLOC_DELETE(eventStackAlloc, tableLatch);
					cxt.setTableSchema(NULL, NULL);
					tableLatch = NULL;
					tableLatch = ALLOC_NEW(eventStackAlloc)
						TableLatch(ec, conn, execution,
							sqlContext.getDBName(), tableName, false, true);
					cxt.setTableSchema(tableLatch->get(), tableLatch);
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
			DMLProcessor::BulkManager::BulkEntry* currentStore;
			while ((currentStore = bulkManager_->next()) != NULL) {
				try {
					currentStore->execute(cxt, dmlType_, true);
				}
				catch (std::exception& e) {
					GS_RETHROW_USER_OR_SYSTEM(e, "");
				}
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
				DMLProcessor::BulkManager::BulkEntry* currentStore;
				while ((currentStore = bulkManager_->next()) != NULL) {
					currentStore->commit(cxt);
				}
			}
		}
		bulkManager_->clear();
		if (remoteUpdateCache) {
			TableSchemaInfo* schemaInfo = tableLatch->get();
			if (schemaInfo) {
				SQLExecution::updateRemoteCache(*cxt.getEventContext(),
					executionManager_->getJobManager()->getEE(SQL_SERVICE),
					pt_,
					execution->getContext().getDBId(),
					execution->getContext().getDBName(), tableName.name_,
					schemaInfo->partitionInfo_.partitioningVersionId_);
			}
		}
		ALLOC_DELETE(eventStackAlloc, cxt.getTableLatch());
		cxt.setTableSchema(NULL, NULL);
		return finished;
	}
	catch (std::exception& e) {
		DMLProcessor::BulkManager::BulkEntry* currentStore;
		while ((currentStore = bulkManager_->next()) != NULL) {
			currentStore->abort(cxt);
		}
		bulkManager_->clear();
		ALLOC_DELETE(eventStackAlloc, cxt.getTableLatch());
		cxt.setTableSchema(NULL, NULL);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief デストラクタ
*/
DMLProcessor::BulkManager::~BulkManager() {
	bulkEntryList_.clear();
	if (insertColumnMap_) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, insertColumnMap_);
	}
}

/*!
	@brief バルクマネージャ初期化
	@note マネージャ - エントリ(コンテナごと) - オペレーション(コマンドごと)の関係
*/
void DMLProcessor::BulkManager::setup(
	Context& cxt, const TableIdInfo& tableIdInfo,
	PartitioningInfo* partitioningInfo,
	TupleList::Column* inputColumnList, int32_t inputColumnSize,
	ColumnInfo* nosqlColumnInfoList, int32_t nosqlColumnSize,
	InsertColumnMap* insertColumnMap, bool isInsertReplace) {

	if (insertColumnMap && insertColumnMap->size() > 0) {
		insertColumnMap_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			InsertColumnMap(jobStackAlloc_);
		insertColumnMap_->assign(
			insertColumnMap->begin(), insertColumnMap->end());

	}

	BulkEntry* bulkEntry = NULL;
	try {
		if (partitioningInfo != NULL) {
			switch (partitioningInfo->partitioningType_) {
			case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
			case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
				partitioningColumnId_ = partitioningInfo->partitioningColumnId_;
				partitioningNum_
					= static_cast<int32_t>(partitioningInfo->subInfoList_.size());
				if (partitioningNum_ == 0) partitioningNum_ = 1;
				bulkEntryList_.assign(
					partitioningNum_, static_cast<BulkEntry*>(NULL));
				break;
			default:
				break;
			}
		}

		columnList_ = inputColumnList;
		columnCount_ = inputColumnSize;
		nosqlColumnList_ = nosqlColumnInfoList;
		nosqlColumnSize_ = nosqlColumnSize;
		checkInsertColumnMap();

		partitioningInfo_ = partitioningInfo;
		isInsertReplace_ = isInsertReplace;
		partitioningType_ = partitioningInfo->partitioningType_;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief デストラクタ
*/
DMLProcessor::BulkManager::BulkEntry::~BulkEntry() {
	if (container_ != NULL) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, container_);
		container_ = NULL;
	}
	for (size_t pos = 0; pos < entries_.size(); pos++) {
		if (entries_[pos] != NULL) {
			ALLOC_DELETE(eventStackAlloc_, entries_[pos]);
			entries_[pos] = NULL;
		}
	}
}

/*!
	@brief バルクエントリ初期化
*/
void DMLProcessor::BulkManager::BulkEntry::setup(
	Context& cxt, DmlType type,
	TupleList::Column* inputColumnList,
	int32_t inputColumnSize,
	ColumnInfo* nosqlColumnInfoList,
	uint32_t nosqlColumnCount,
	TableContainerInfo* containerInfo,
	bool isInsertReplace,
	bool isPartitioning,
	TargetContainerInfo& targetInfo) {
	Operation* entry = NULL;
	try {
		EventContext& ec = *cxt.getEventContext();
		container_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			NoSQLContainer(ec, targetInfo.containerId_,
				targetInfo.versionId_, targetInfo.pId_,
				*cxt.getSyncContext(), cxt.getExecution());
		if (cxt.isDMLTimeSeries()) {
			container_->setContainerType(TIME_SERIES_CONTAINER);
		}
		for (int32_t target = 0; target < DML_MAX; target++) {
			if (target == type) {
				switch (type) {
				case DML_INSERT:
					entry = ALLOC_NEW(eventStackAlloc_) Insert(
						bulkManager_,
						eventStackAlloc_,
						nosqlColumnInfoList, nosqlColumnCount,
						container_, pos_, isInsertReplace, targetInfo.affinity_);
					if (isPartitioning) {
						entry->setPartitioning();
					}
					break;
				case DML_UPDATE:
					entry = ALLOC_NEW(eventStackAlloc_) Update(
						bulkManager_, eventStackAlloc_,
						nosqlColumnInfoList, nosqlColumnCount, container_, pos_, targetInfo.affinity_);
					break;
				case DML_DELETE:
					entry = ALLOC_NEW(eventStackAlloc_) Delete(
						bulkManager_, eventStackAlloc_,
						nosqlColumnInfoList, nosqlColumnCount, container_, pos_, targetInfo.affinity_);
					break;
				}
			}
			else {
				entry = NULL;
			}
			if (entry) {
				int64_t sizeLimit = cxt.getExecutionManager()->getNoSQLSizeLimit();
				entry->setLimitSize(sizeLimit);

				const DataStoreConfig* dsConfig = cxt.getExecutionManager()->getManagerSet()->dsConfig_;
				entry->setDataStoreConfig(dsConfig);
			}
			entries_.push_back(entry);
			entry = NULL;
		}
		partitionNum_ = cxt.getPartitionTable()->getPartitionNum();
	}
	catch (std::exception& e) {
		if (nosqlColumnInfoList == NULL) {
			GS_TRACE_ERROR(SQL_SERVICE, 0, "EXCEPTION");
		}
		if (entry != NULL) {
			ALLOC_DELETE(eventStackAlloc_, entry);
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

TupleValue* DMLProcessor::BulkManager::setTupleValue(
	TupleList::Reader& reader, ColumnId columnId, TupleValue& value) {
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

/*!
	@brief カーソルからコマンド種別に応じてバルク管理に登録し、可能ならばコマンド実行する
*/
void DMLProcessor::BulkManager::import(Context& cxt, InputId inputId,
	TupleList::Reader& reader, bool& needRefresh) {
	needRefresh = false;
	EventContext& ec = *cxt.getEventContext();
	InputId targetId = UNDEF_INPUTID;
	TableSchemaInfo* tableSchema = cxt.getTableSchema();
	NodeAffinityNumber affinity = UNDEF_NODE_AFFINITY_NUMBER;
	SQLExecution* execution = cxt.getExecution();
	util::StackAllocator& alloc = cxt.getEventAllocator();
	switch (type_) {
	case DML_INSERT: {
		BulkManager::BulkEntry* store = NULL;
		TableContainerInfo* containerInfo = NULL;
		NameWithCaseSensitivity dbName(
			execution->getContext().getDBName(), false);
		NameWithCaseSensitivity tableName(
			partitioningInfo_->tableName_.c_str(),
			partitioningInfo_->isCaseSensitive_);

		DBConnection* conn = executionManager_->getDBConnection();
		const TupleValue* value1 = NULL;
		const TupleValue* value2 = NULL;
		TupleValue tmpValue1(NULL, TupleList::TYPE_NULL);
		TupleValue tmpValue2(NULL, TupleList::TYPE_NULL);

		value1 = setTupleValue(
			reader, partitioningInfo_->partitioningColumnId_, tmpValue1);

		value2 = setTupleValue(
			reader, partitioningInfo_->subPartitioningColumnId_, tmpValue2);

		TableSchemaInfo* afterSchema = NULL;
		TargetContainerInfo targetInfo;
		resolveTargetContainer(ec, value1, value2, conn, tableSchema,
			execution, dbName, tableName, targetInfo, needRefresh);
		NodeAffinityNumber pos;
		if (partitioningInfo_->partitioningColumnId_ != UNDEF_COLUMNID) {
			if (partitioningInfo_->partitioningType_ == SyntaxTree::TABLE_PARTITION_TYPE_HASH
				|| partitioningInfo_->partitioningType_ == SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
				if (targetInfo.pos_ == 0) {
					return;
				}
				store = addValue(cxt, targetInfo.pos_ - 1, targetInfo);
			}
			else {
				store = addValue(cxt, targetInfo.affinity_, targetInfo);
			}
		}
		else {
			pos = 0;
			containerInfo = &tableSchema->containerInfoList_[0];
			targetInfo.containerId_ = containerInfo->containerId_;
			targetInfo.pId_ = containerInfo->pId_;
			targetInfo.versionId_ = containerInfo->versionId_;
			store = addValue(cxt, 0, targetInfo);
		}

		OutputMessageRowStore& rowStore
			= store->getOperation(DML_INSERT)->getRowStore();
		rowStore.beginRow();
		if (insertColumnMap_) {
			for (uint32_t columnId = 0;
				columnId < static_cast<uint32_t>(insertColumnMap_->size()); columnId++) {
				appendValue(DML_INSERT, targetId,
					reader, inputId, (*insertColumnMap_)[columnId],
					columnId, columnId, rowStore);
			}
		}
		else {
			for (int32_t columnId = 0; columnId < columnCount_; columnId++) {
				appendValue(DML_INSERT, targetId,
					reader, inputId, columnId, columnId, columnId, rowStore);
			}
		}

		DMLProcessor::BulkManager::Insert* insertOp
			= static_cast<DMLProcessor::BulkManager::Insert*>(
				store->getOperation(DML_INSERT));
		insertOp->next();
		if (insertOp->full()) {
			try {
				insertOp->execute(cxt);
			}
			catch (std::exception& e) {
				resolveTargetContainer(ec, value1, value2, conn, tableSchema,
					execution, dbName, tableName, targetInfo, needRefresh);
				insertOp->execute(cxt);
			}
		}
	}
				   break;
	case DML_UPDATE: {
		RowId rowId = reader.get().getAs<int64_t>(columnList_[ROW_COLUMN_ID]);
		int32_t subContainerId = static_cast<int32_t>(
			reader.get().getAs<int64_t>(columnList_[SUBCONTAINER_COLUMN_ID]));
		BulkManager::BulkEntry* store = NULL;
		TargetContainerInfo targetInfo;
		if (subContainerId > partitioningInfo_->subInfoList_.size()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
				"Target pos=" << subContainerId << " is out of range, max="
				<< partitioningInfo_->subInfoList_.size());
		}
		targetInfo.containerId_
			= partitioningInfo_->subInfoList_[subContainerId].containerId_;
		targetInfo.pId_ = partitioningInfo_->subInfoList_[
			subContainerId].partitionId_;
		targetInfo.versionId_
			= partitioningInfo_->subInfoList_[subContainerId].schemaVersionId_;
		store = addPosition(cxt, subContainerId, targetInfo);
		DMLProcessor::BulkManager::Update* updateOp
			= static_cast<DMLProcessor::BulkManager::Update*>(
				store->getOperation(DML_UPDATE));

		OutputMessageRowStore& rowStore = updateOp->getRowStore();
		rowStore.beginRow();
		int32_t outColumnId = 0;
		for (int32_t columnId = RESERVE_COLUMN_NUM;
			columnId < columnCount_; columnId++, outColumnId++) {
			appendValue(DML_UPDATE, subContainerId,
				reader, subContainerId, columnId,
				outColumnId, outColumnId, rowStore);
		}
		updateOp->next();
		updateOp->append(cxt, rowId);
		if (updateOp->full()) {
			updateOp->execute(cxt);
		}

		return;
	}
				   break;
	case DML_DELETE: {
		RowId rowId = reader.get().getAs<int64_t>(columnList_[ROW_COLUMN_ID]);
		int32_t subContainerId = static_cast<int32_t>(
			reader.get().getAs<int64_t>(columnList_[SUBCONTAINER_COLUMN_ID]));
		BulkManager::BulkEntry* store = NULL;
		TargetContainerInfo targetInfo;

		if (subContainerId > partitioningInfo_->subInfoList_.size()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
				"Target pos=" << subContainerId << " is out of range, max="
				<< partitioningInfo_->subInfoList_.size());
		}
		targetInfo.containerId_
			= partitioningInfo_->subInfoList_[subContainerId].containerId_;
		targetInfo.pId_
			= partitioningInfo_->subInfoList_[subContainerId].partitionId_;
		targetInfo.versionId_
			= partitioningInfo_->subInfoList_[subContainerId].schemaVersionId_;
		store = addPosition(cxt, subContainerId, targetInfo);
		DMLProcessor::BulkManager::Delete* deleteOp
			= static_cast<DMLProcessor::BulkManager::Delete*>(
				store->getOperation(DML_DELETE));
		deleteOp->append(cxt, rowId);
		if (deleteOp->full()) {
			deleteOp->execute(cxt);
		}
		return;
	}
				   break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INTERNAL, "");
	}
}

bool DMLProcessor::recovery(Context& cxt,
	NodeAffinityNumber affinity, NoSQLContainer& container) {

	NoSQLContainer* newContainer = NULL;
	EventContext& ec = *cxt.getEventContext();
	util::StackAllocator& alloc = ec.getAllocator();
	TableSchemaInfo* tableSchema = cxt.getTableSchema();

	try {

		SQLExecution* execution = cxt.getExecution();

		if (execution == NULL) {
			return false;
		}

		DBConnection* conn = executionManager_->getDBConnection();
		const SQLExecution::SQLExecutionContext& sqlContext
			= execution->getContext();
		NoSQLStore* store = conn->getNoSQLStore(
			sqlContext.getDBId(), sqlContext.getDBName());

		const NameWithCaseSensitivity tableName(
			partitioningInfo_->tableName_.c_str(),
			partitioningInfo_->isCaseSensitive_);

		uint32_t partitionNum = conn->getPartitionTable()->getPartitionNum();

		NoSQLContainer* newContainer = recoverySubContainer(alloc, ec, store, execution,
			tableName, partitionNum, affinity);

		if (newContainer == NULL) {
			return false;
		}

		container.updateInfo(newContainer->getContainerId(),
			newContainer->getVersionId());

		ALLOC_DELETE(alloc, cxt.getTableLatch());
		cxt.setTableSchema(NULL, NULL);

		TableLatch* tableLatch = ALLOC_NEW(alloc)
			TableLatch(ec, conn, execution,
				sqlContext.getDBName(), tableName, false, true);
		cxt.setTableSchema(tableLatch->get(), tableLatch);

		ALLOC_DELETE(alloc, newContainer);
		return true;
	}
	catch (std::exception& e) {
		ALLOC_DELETE(alloc, newContainer);
		return false;
	}
}

void DMLProcessor::BulkManager::checkInsertColumnMap() {
	if (insertColumnMap_) {
		for (ColumnId columnId = 0; columnId < insertColumnMap_->size(); columnId++) {
			int32_t columnPos = (*insertColumnMap_)[columnId];
			if (columnPos == -1) {
				if (nosqlColumnList_ == NULL) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
				}
				if (columnId >= nosqlColumnSize_) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
				}
				if (nosqlColumnList_[columnId].isNotNull()) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DML_INSERT_INVALID_NULL_CONSTRAINT,
						"Target column (position=" << columnId << ") is not nullable");
				}
			}
		}
	}
}


/*!
	@brief 型に応じてバルク管理先を決定し、エントリする
	@note INSERTのパーティショニング有の場合だけ管理先を適宜決定する
	@note バルクにエントリするだけで、実行確認、実施等は上位で実施する
*/
void DMLProcessor::BulkManager::appendValue(
	int32_t dmlType, InputId& targetId, TupleList::Reader& reader,
	InputId input, ColumnId inputColumnId,
	ColumnId inputNoSQLColumnId,
	ColumnId outputColumnId, OutputMessageRowStore& rowStore) {
	if (inputColumnId == -1) {
		const TupleValue value(NULL, TupleList::TYPE_NULL);
		DMLProcessor::setField(value.getType(), outputColumnId, value, rowStore,
			nosqlColumnList_[inputNoSQLColumnId].getColumnType());
	}
	else {
		const TupleValue& value
			= reader.get().get(columnList_[inputColumnId]);
		DMLProcessor::setField(value.getType(), outputColumnId, value, rowStore,
			nosqlColumnList_[inputNoSQLColumnId].getColumnType());
	}
}

DMLProcessor::BulkManager::Operation::Operation(
	BulkManager* bulkManager,
	util::StackAllocator& eventStackAlloc,
	const ColumnInfo* columnInfoList,
	uint32_t columnCount,
	NoSQLContainer* container, int32_t pos, NodeAffinityNumber affinity) :
	eventStackAlloc_(eventStackAlloc),
	columnInfoList_(columnInfoList),
	columnCount_(columnCount),
	container_(container),
	pos_(pos),
	option_(NULL),
	currentProcessedNum_(0),
	totalProcessedNum_(0),
	fixedData_(NULL),
	varData_(NULL),
	rowStore_(NULL),
	rowIdList_(NULL),
	partitioning_(false),
	dsConfig_(NULL),
	bulkManager_(bulkManager),
	affinity_(affinity)
{
	option_ = ALLOC_NEW(eventStackAlloc_) NoSQLStoreOption;
}

DMLProcessor::BulkManager::Operation::~Operation() {
	if (option_ != NULL) {
		ALLOC_DELETE(eventStackAlloc_, option_);
	}
}

/*!
	@brief ロウIDを登録する
	@note カラム[0]がロウID, カラム[1]がサブコンテナID
	@note UPDATEとDELETEの場合のみ
*/
int32_t DMLProcessor::BulkManager::appendDbPosition(int32_t dmlType,
	InputId targetId, TupleList::Reader& reader,
	ColumnId rowColumn, ColumnId subContainerColumn) {

	RowId rowId = reader.get().getAs<int64_t>(columnList_[rowColumn]);
	int32_t subContainerId = static_cast<int32_t>(
		reader.get().getAs<int64_t>(columnList_[subContainerColumn]));
	getOperation(subContainerId, dmlType)->getRowIdList().push_back(rowId);
	return subContainerId;
}

void DMLProcessor::BulkManager::Operation::commit(Context& cxt) {
	container_->commit(*option_);
}

void DMLProcessor::BulkManager::Operation::abort(Context& cxt) {
	container_->abort(*option_);
}

/*!
	@brief 型に応じてバルク管理先を決定し、エントリする
	@note INSERTのパーティショニング有の場合だけ管理先を適宜決定する
	@note バルクにエントリするだけで、実行確認、実施等は上位で実施する
	@note 例外に対するアボート処理は上位で行う
*/
void DMLProcessor::BulkManager::Insert::execute(Context& cxt) {

	try {
		if (rowStore_->getRowCount() == 0) {
			return;
		}
		try {
			container_->setExecution(cxt.getExecution());
			container_->setEventContext(cxt.getEventContext());
			container_->putRowSet(*fixedData_, *varData_,
				rowStore_->getRowCount(), *option_);
		}
		catch (util::Exception& e) {
			checkException(e);
			if (bulkManager_->getProcessor()->recovery(cxt, affinity_, *container_)) {
				container_->putRowSet(*fixedData_, *varData_,
					rowStore_->getRowCount(), *option_);
			}
			else {
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}
		}
		bulkManager_->incCount(rowStore_->getRowCount());

		initialize();
		currentProcessedNum_ = 0;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void DMLProcessor::BulkManager::Update::execute(Context& cxt) {

	if (rowIdList_->size() == 0) return;

	container_->setExecution(cxt.getExecution());
	container_->setEventContext(cxt.getEventContext());

	try {
		if (container_->getContainerType() == TIME_SERIES_CONTAINER) {
			container_->putRowSet(*fixedData_, *varData_,
				static_cast<uint64_t>(rowIdList_->size()), *option_);
		}
		else {
			container_->updateRowSet(*fixedData_, *varData_, *rowIdList_, *option_);
		}
	}
	catch (std::exception& e) {
		checkException(e);
		if (bulkManager_->getProcessor()->recovery(cxt, affinity_, *container_)) {
			if (container_->getContainerType() == TIME_SERIES_CONTAINER) {
				container_->putRowSet(*fixedData_, *varData_,
					static_cast<uint64_t>(rowIdList_->size()), *option_);
			}
			else {
				container_->updateRowSet(*fixedData_, *varData_, *rowIdList_, *option_);
			}
		}
		else {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}
	}

	bulkManager_->incCount(rowIdList_->size());
	initialize();
	currentProcessedNum_ = 0;
}

void DMLProcessor::BulkManager::Delete::execute(Context& cxt) {
	if (rowIdList_->size() == 0) {
		return;
	}
	container_->setExecution(cxt.getExecution());
	container_->setEventContext(cxt.getEventContext());
	try {
		container_->deleteRowSet(*rowIdList_, *option_);
	}
	catch (std::exception& e) {
		checkException(e);
		if (bulkManager_->getProcessor()->recovery(cxt, affinity_, *container_)) {
			container_->deleteRowSet(*rowIdList_, *option_);
		}
		else {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}
	}
	bulkManager_->incCount(rowIdList_->size());
	initialize();
}

void DMLProcessor::BulkManager::Delete::append(Context& cxt, RowId rowId) {
	try {
		rowIdList_->push_back(rowId);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void DMLProcessor::BulkManager::Update::append(Context& cxt, RowId rowId) {
	try {
		rowIdList_->push_back(rowId);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

DMLProcessor::BulkManager::Operation
* DMLProcessor::BulkManager::getOperation(
	InputId target, DmlType type) {
	return bulkEntryList_[target]->getOperation(type);
}

void SQLProcessor::getResultCountBlock(Context& cxt,
	int64_t resultCount, TupleList::Block& block) {
	try {
		TupleList::Info outTupleInfo;
		outTupleInfo.columnCount_ = 1;
		TupleList::TupleColumnType outColumnTypeList[1];
		outColumnTypeList[0] = TupleList::TYPE_LONG;
		outTupleInfo.columnTypeList_ = outColumnTypeList;
		TupleList::Group outGroup(cxt.getStore());
		TupleList outputTuple(outGroup, outTupleInfo);
		TupleList::Writer writer(outputTuple);
		TupleList::Column column;
		outputTuple.getInfo().getColumns(&column, 1);
		writer.next();
		writer.get().setBy(column, resultCount);
		writer.close();
		TupleList::BlockReader blockReader(outputTuple);
		blockReader.next(block);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*
	@brief このスコープで実行するアロケータをセットする
	@note 実際のアロケータ情報。スコープの度に、指定されたアロケータを利用してエリアを確保する
*/
void DMLProcessor::BulkManager::Update::initialize() {
	if (fixedData_) {
		ALLOC_DELETE(eventStackAlloc_, fixedData_);
	}
	if (varData_) {
		ALLOC_DELETE(eventStackAlloc_, varData_);
	}
	if (rowStore_) {
		ALLOC_DELETE(eventStackAlloc_, rowStore_);
	}
	if (rowIdList_) {
		rowIdList_->clear();
	}
	else {
		rowIdList_ = ALLOC_NEW(eventStackAlloc_) util::XArray<RowId>(eventStackAlloc_);
	}

	rowIdList_->clear();
	fixedData_ = ALLOC_NEW(eventStackAlloc_) util::XArray<uint8_t>(eventStackAlloc_);
	varData_ = ALLOC_NEW(eventStackAlloc_) util::XArray<uint8_t>(eventStackAlloc_);
	rowStore_ = ALLOC_NEW(eventStackAlloc_) OutputMessageRowStore(
		*dsConfig_,
		columnInfoList_, columnCount_, *fixedData_, *varData_, false);
}

DMLProcessor::BulkManager::Insert::Insert(
	BulkManager* bulkManager,
	util::StackAllocator& alloc,
	const ColumnInfo* columnInfoList,
	uint32_t columnCount, NoSQLContainer* container,
	int32_t pos, bool isInsertReplace, NodeAffinityNumber affinity) :
	Operation(bulkManager, alloc, columnInfoList, columnCount, container, pos, affinity) {
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
	BulkManager* bulkManager,
	util::StackAllocator& alloc,
	const ColumnInfo* columnInfoList,
	uint32_t columnCount, NoSQLContainer* container, int32_t pos, NodeAffinityNumber affinity) :
	Operation(bulkManager, alloc, columnInfoList, columnCount, container, pos, affinity) {
	threashold_ = BULK_UPDATE_THRESHOLD_COUNT;
	option_->putRowOption_ = PUT_UPDATE_ONLY;
	initialize();
}

void DMLProcessor::BulkManager::Insert::initialize() {
	if (fixedData_) {
		ALLOC_DELETE(eventStackAlloc_, fixedData_);
	}
	if (varData_) {
		ALLOC_DELETE(eventStackAlloc_, varData_);
	}
	if (rowStore_) {
		ALLOC_DELETE(eventStackAlloc_, rowStore_);
	}
	fixedData_ = ALLOC_NEW(eventStackAlloc_) util::XArray<uint8_t>(eventStackAlloc_);
	varData_ = ALLOC_NEW(eventStackAlloc_) util::XArray<uint8_t>(eventStackAlloc_);
	rowStore_ = ALLOC_NEW(eventStackAlloc_) OutputMessageRowStore(
		*dsConfig_,
		columnInfoList_, columnCount_, *fixedData_, *varData_, false);
}

void DMLProcessor::BulkManager::Delete::initialize() {
	if (rowIdList_) {
		rowIdList_->clear();
	}
	else {
		rowIdList_ = ALLOC_NEW(eventStackAlloc_)
			util::XArray<RowId>(eventStackAlloc_);
	}
}

void DMLProcessor::exportTo(Context& cxt, const OutOption& option) const {
}

DMLProcessor::BulkManager::BulkEntry* DMLProcessor::BulkManager::addValue(
	Context& cxt, NodeAffinityNumber value, TargetContainerInfo& targetInfo) {
	bool isPartitioning = true;
	util::StackAllocator& eventStackAlloc = cxt.getEventContext()->getAllocator();
	switch (partitioningType_) {
	case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
		isPartitioning = false;
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
		size_t pos = static_cast<size_t>(value);
		if (bulkEntryList_.size() > 0) {
			if (pos < bulkEntryList_.size() && bulkEntryList_[pos] != NULL) {
				return bulkEntryList_[pos];
			}
		}
		if (pos == -1 || bulkEntryList_.size() <= pos) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH, "");
		}

		bulkEntryList_[pos] = ALLOC_NEW(jobStackAlloc_)
			BulkEntry(this, eventStackAlloc, globalVarAlloc_, pos, type_, targetInfo.affinity_);
		bulkEntryList_[pos]->setup(cxt, type_, columnList_, columnCount_,
			nosqlColumnList_, nosqlColumnSize_,
			NULL, isInsertReplace_, isPartitioning, targetInfo);
		return 	bulkEntryList_[pos];
	}
											  break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {
		util::Map<NodeAffinityNumber, BulkEntry*>::iterator
			it = bulkEntryListMap_.find(value);
		if (it == bulkEntryListMap_.end()) {
			BulkEntry* bulkEntry = ALLOC_NEW(jobStackAlloc_)
				BulkEntry(this, eventStackAlloc, globalVarAlloc_, value, type_, targetInfo.affinity_);
			bulkEntry->setup(cxt, type_, columnList_, columnCount_,
				nosqlColumnList_, nosqlColumnSize_,
				NULL, isInsertReplace_, isPartitioning, targetInfo);
			bulkEntryList_.push_back(bulkEntry);
			bulkEntryListMap_.insert(std::make_pair(value, bulkEntry));
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


DMLProcessor::BulkManager::BulkEntry* DMLProcessor::BulkManager::add(
	Context& cxt, NodeAffinityNumber pos,
	TableSchemaInfo* schemaInfo, TableContainerInfo* containerInfo,
	TargetContainerInfo& targetInfo) {
	bool isPartitioning = true;
	util::StackAllocator& eventStackAlloc = cxt.getEventContext()->getAllocator();
	switch (partitioningType_) {
	case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
		isPartitioning = false;
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
		if (bulkEntryList_.size() > 0) {
			if (pos < bulkEntryList_.size() && bulkEntryList_[pos] != NULL) {
				return bulkEntryList_[pos];
			}
		}
		if (pos == -1 || bulkEntryList_.size() <= pos) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH, "");
		}

		bulkEntryList_[pos] = ALLOC_NEW(jobStackAlloc_)
			BulkEntry(this, eventStackAlloc, globalVarAlloc_, pos, type_, targetInfo.affinity_);
		bulkEntryList_[pos]->setup(cxt, type_, columnList_, columnCount_,
			nosqlColumnList_, nosqlColumnSize_,
			containerInfo, isInsertReplace_, isPartitioning, targetInfo);
		return 	bulkEntryList_[pos];
	}
											  break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {
		util::Map<NodeAffinityNumber, BulkEntry*>::iterator
			it = bulkEntryListMap_.find(pos);
		if (it == bulkEntryListMap_.end()) {
			BulkEntry* bulkEntry = ALLOC_NEW(jobStackAlloc_)
				BulkEntry(this, eventStackAlloc, globalVarAlloc_, pos, type_, targetInfo.affinity_);
			bulkEntry->setup(cxt, type_, columnList_, columnCount_,
				nosqlColumnList_, nosqlColumnSize_,
				containerInfo, isInsertReplace_, isPartitioning, targetInfo);
			bulkEntryList_.push_back(bulkEntry);
			bulkEntryListMap_.insert(std::make_pair(pos, bulkEntry));
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
	Context& cxt, int32_t pos, TargetContainerInfo& targetInfo) {
	util::StackAllocator& eventStackAlloc = cxt.getEventContext()->getAllocator();
	if (pos < bulkEntryList_.size() && bulkEntryList_[pos] != NULL) {
		return bulkEntryList_[pos];
	}
	if (bulkEntryList_.size() == 0) {
		bulkEntryList_.assign(partitioningInfo_->subInfoList_.size(),
			static_cast<BulkEntry*>(NULL));
	}
	bulkEntryList_[pos] = ALLOC_NEW(jobStackAlloc_)
		BulkEntry(this, eventStackAlloc, globalVarAlloc_, pos, type_, targetInfo.affinity_);
	bulkEntryList_[pos]->setup(cxt, type_, columnList_, columnCount_,
		nosqlColumnList_, nosqlColumnSize_,
		NULL, isInsertReplace_,
		(partitioningType_ !=
			SyntaxTree::TABLE_PARTITION_TYPE_UNDEF), targetInfo);
	return 	bulkEntryList_[pos];
}

DMLProcessor::BulkManager::BulkEntry* DMLProcessor::BulkManager::next() {
	if (prevStore_) {
		ALLOC_DELETE(jobStackAlloc_, prevStore_);
		prevStore_ = NULL;
		bulkEntryList_[prevPos_] = static_cast<BulkEntry*>(NULL);
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
