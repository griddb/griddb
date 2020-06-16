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
#include "resource_set.h"
#include "sql_execution.h"
#include "sql_execution_manager.h"
#include "nosql_db.h"
#include "nosql_store.h"
#include "sql_processor_dml.h"
#include "sql_service.h"
#include "sql_profiler.h"
#include "nosql_store.h"
#include "nosql_container.h"
#include "nosql_utils.h"

struct BulkEntry {

	BulkEntry(
			util::StackAllocator &alloc,
			const DataStoreValueLimitConfig &dsValueLimitConfig,
			ColumnInfo *columnInfoList,
			uint32_t columnCount,
			TargetContainerInfo &targetInfo) :
					alloc_(alloc),
					fixedPart_(NULL),
					varPart_(NULL),
					columnInfoList_(NULL),
					columnCount_(columnCount),
					dsValueLimitConfig_(dsValueLimitConfig),
					rowStore_(NULL),
					containerId_(targetInfo.containerId_),
					pId_(targetInfo.pId_),
					versionId_(targetInfo.versionId_),
					affinity_(targetInfo.affinity_) {

			init(columnInfoList, columnCount);
		}

	void init(
			ColumnInfo *columnInfoList,
			uint32_t columnCount) {

		ALLOC_DELETE(alloc_, fixedPart_);
		ALLOC_DELETE(alloc_, varPart_);
		ALLOC_DELETE(alloc_, rowStore_);
		
		fixedPart_ = ALLOC_NEW(alloc_)
				util::XArray<uint8_t>(alloc_);
		
		varPart_ = ALLOC_NEW(alloc_)
				util::XArray<uint8_t>(alloc_);

		if (!columnInfoList_) {
			
			ALLOC_DELETE(alloc_, columnInfoList_);

			columnInfoList_ = ALLOC_NEW(alloc_)
					ColumnInfo [columnCount];
			memcpy(
					columnInfoList_,
					columnInfoList,
					sizeof(ColumnInfo) * columnCount);
		}

		rowStore_ = ALLOC_NEW(alloc_)
				OutputMessageRowStore(
						dsValueLimitConfig_,
						columnInfoList_,
						columnCount_,
						*fixedPart_,
						*varPart_,
						false);
	}


	int64_t getSize() {
		return (
				fixedPart_->size() + varPart_->size());
	}

	util::StackAllocator &alloc_;
	util::XArray<uint8_t> *fixedPart_;
	util::XArray<uint8_t> *varPart_;
	ColumnInfo *columnInfoList_;
	uint32_t columnCount_;
	const DataStoreValueLimitConfig &dsValueLimitConfig_;
	OutputMessageRowStore *rowStore_;
	ContainerId containerId_;
	PartitionId pId_;
	SchemaVersionId versionId_;
	NodeAffinityNumber affinity_;
};

class BulkEntryManager {
public:

	BulkEntryManager(
			util::StackAllocator &alloc,
			uint8_t partitioningType,
			int32_t partitioningNum,
			const DataStoreValueLimitConfig &config,
			ColumnInfo *columnInfoList,
			uint32_t columnCount) :
					alloc_(alloc),
					storeList_(alloc),
					storeMapList_(alloc),
					partitioningType_(partitioningType),
					partitioningNum_(partitioningNum),
					config_(config),
					columnInfoList_(columnInfoList),
					columnCount_(columnCount),
					currentPos_(0),
					prevPos_(0),
					prevStore_(NULL) {

		switch (partitioningType_) {
			case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
			case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
	
				storeList_.assign(
						partitioningNum,
						static_cast<BulkEntry *>(NULL));
			break;

			default:
			break;
		}
	}

	BulkEntry *add(
			NodeAffinityNumber affinity,
			TableSchemaInfo *schema,
			TargetContainerInfo &targetInfo) {

		switch (partitioningType_) {

			case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
			case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {

				size_t targetPos = static_cast<size_t>(affinity);
				if (targetPos == SIZE_MAX || storeList_.size() <= targetPos) {
					GS_THROW_USER_ERROR(
							GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH, "");
				}

				if (storeList_[targetPos] != NULL) {
					return storeList_[targetPos];
				}
				else {
					storeList_[targetPos] = ALLOC_NEW(alloc_) BulkEntry(
							alloc_,
							config_,
							schema->nosqlColumnInfoList_,
							static_cast<uint32_t>(
									schema->columnInfoList_.size()),
							targetInfo);

					return storeList_[targetPos];
				}
			}
			break;

			case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
			case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:  {
				util::Map<NodeAffinityNumber, BulkEntry*>::iterator
						it = storeMapList_.find(affinity);
			
				if (it == storeMapList_.end()) {
					BulkEntry *store = ALLOC_NEW(alloc_) BulkEntry(
							alloc_,
							config_,
							schema->nosqlColumnInfoList_,
							static_cast<uint32_t>(
									schema->columnInfoList_.size()),
							targetInfo);
					
					storeMapList_.insert(
							std::make_pair(affinity, store));
					storeList_.push_back(store);
					
					return store;
				}
				else {
					return (*it).second;
				}
			}
		}
		return NULL;
	}

	void begin() {

		currentPos_ = 0;
		prevPos_ = 0;
		prevStore_ = NULL;
	}

	BulkEntry *next() {

		switch (partitioningType_) {
		
			case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
			case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
		
				if (prevStore_) {
					ALLOC_DELETE(alloc_, prevStore_);
					storeList_[prevPos_] = NULL;
				}

				while (currentPos_ < storeList_.size()) {
					if (storeList_[currentPos_] != NULL) {
						prevPos_ = currentPos_;
						prevStore_ = storeList_[currentPos_];
						return storeList_[currentPos_];
					}
					currentPos_++;
				}
				return NULL;
			}
			break;

			case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
			case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {

				if (prevStore_) {
					ALLOC_DELETE(alloc_, storeList_[prevPos_]);
					storeList_[prevPos_] = NULL;
				}

				if (currentPos_ >= storeList_.size()) {
					return NULL;
				}
				prevPos_ = currentPos_;
				prevStore_ = storeList_[static_cast<size_t>(currentPos_)];
				return storeList_[currentPos_++];
			}
			break;
		}
		return NULL;
	}

	util::StackAllocator &alloc_;
	util::Vector<BulkEntry*> storeList_;
	util::Map<NodeAffinityNumber, BulkEntry*> storeMapList_;
	uint8_t partitioningType_;
	uint32_t partitioningNum_;
	const DataStoreValueLimitConfig &config_;
	ColumnInfo *columnInfoList_;
	uint32_t columnCount_;
	size_t currentPos_;
	size_t prevPos_;
	BulkEntry *prevStore_;
};

bool SQLExecution::executeFastInsert(
		EventContext &ec,
		util::Vector<BindParam*> &bindParamInfos,
		bool useCache) {

	NoSQLDB::TableLatch *latch = NULL;
	util::StackAllocator &alloc = ec.getAllocator();
	util::Stopwatch watch;

	watch.start();

	SQLParsedInfo &parsedInfo = getParsedInfo();
	DataStore *dataStore = resourceSet_->getDataStore();

	try {
		util::Vector<SyntaxTree::ExprList *>
				&mergeSelectList = getMergeSelectList();
		assert(mergeSelectList.size() > 0);

		SyntaxTree::ExprList *selectList = mergeSelectList[0];
		
		NameWithCaseSensitivity tableName(
				parsedInfo.tableList_[0]->qName_->table_->c_str(),
				parsedInfo.tableList_[0]->qName_->tableCaseSensitive_);

		const char *dbName = NULL;
		bool isAdjust = false;
		bool dbCaseSensitive = false;
		if (parsedInfo.tableList_[0]->qName_->db_ == NULL) {
			isAdjust = true;
		}
		else {
			dbName = parsedInfo.tableList_[0]
					->qName_->db_->c_str();
			dbCaseSensitive
					= parsedInfo.tableList_[0]->qName_->dbCaseSensitive_;
	
			NoSQLUtils::checkConnectedDbName(
					alloc,
					context_.getDBName(),
					dbName,
					dbCaseSensitive);
		}

		NameWithCaseSensitivity
				dbNameWithCaseSensitive(dbName,dbCaseSensitive);

		CreateTableOption option(alloc);
		NoSQLDB *db = executionManager_->getDB();
		TableSchemaInfo *tableSchema;
		
		const char8_t *currentDBName;
		if (isAdjust) {
			currentDBName = NULL;
		}
		else {
			currentDBName = dbName;
		}
		
		bool remoteUpdateCache = false;
		bool needSchemaRefresh = false;
		int64_t totalRowCount = 0;

		try {
			latch = ALLOC_NEW(alloc) NoSQLDB::TableLatch(
					ec, db, this, currentDBName, tableName, useCache);
			tableSchema = latch->get();
			
			if (tableSchema == NULL) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
						"Table name '" << tableName.name_ << "' not found");
			}
			
			if (!NoSQLUtils::isWritableContainer(
					tableSchema->containerAttr_)) {

				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DML_INVALID_CONTAINER_ATTRIBUTE,
						"Target table '" << tableName.name_
						<< "' is read-only table");
			}

			if (tableSchema->containerAttr_ == CONTAINER_ATTR_VIEW) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
						"Target table '" << tableName.name_ << "' is VIEW");
			}

			tableSchema->checkWritableContainer();
			if (tableSchema->partitionInfo_.partitionType_
					== SyntaxTree::TABLE_PARTITION_TYPE_HASH
				&& (tableSchema->containerInfoList_.size()  
						!= tableSchema->partitionInfo_.partitioningNum_ + 1)) {
				
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
						"Table name '" << tableName.name_ 
						<< "' is not found or already removed or under removing");
			}

			NoSQLStoreOption cmdOption(this);
			ColumnId keyColumnId
					= tableSchema->partitionInfo_.partitioningColumnId_;
			ColumnId subKeyColumnId
					=  tableSchema->partitionInfo_.subPartitioningColumnId_;
			int32_t partitioningCount
					= tableSchema->partitionInfo_.getCurrentPartitioningCount();
			int32_t startPos = 0;
			if (partitioningCount == 0) {
				partitioningCount = 1;
			}
			else {
				startPos = 1;
			}
			
			TupleValue::VarContext varCxt;
			varCxt.setStackAllocator(&alloc);
			varCxt.setVarAllocator(&localVarAlloc_);
			varCxt.setGroup(&group_);
			
			BulkEntryManager bulkManager(
					alloc,
					tableSchema->partitionInfo_.partitionType_,
					partitioningCount, dataStore->getValueLimitConfig(),
					tableSchema->nosqlColumnInfoList_,
					static_cast<uint32_t>(
							tableSchema->columnInfoList_.size()));

			int64_t currentSize = 0;
			int64_t sizeLimit = executionManager_->getNoSQLSizeLimit();
			BulkEntry *currentStore;
			if (parsedInfo.syntaxTreeList_[0]->cmdOptionValue_
					== SyntaxTree::RESOLVETYPE_REPLACE) {
					cmdOption.putRowOption_ = PUT_INSERT_OR_UPDATE;
			}

			for (size_t rowPos = 0;
					rowPos < mergeSelectList.size(); rowPos++) {

				if (tableSchema->columnInfoList_.size()
						!= mergeSelectList[rowPos]->size()) {

					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_MISMATCH_SCHEMA,
							"Specified column list count is unmatch with target table, expected="
							<< tableSchema->columnInfoList_.size()
							<< ", actual=" << mergeSelectList[rowPos]->size());
				}

				const TupleValue *value1 = NULL;
				const TupleValue *value2 = NULL;
				if (keyColumnId != UNDEF_COLUMNID) {
					value1 = ALLOC_NEW(alloc) TupleValue(
							getTupleValue(
									varCxt,
									(*mergeSelectList[rowPos])[keyColumnId],
									tableSchema->columnInfoList_
											[keyColumnId].tupleType_,
									bindParamInfos));
				}

				if (subKeyColumnId != UNDEF_COLUMNID) {
					value2 = ALLOC_NEW(alloc) TupleValue(
							getTupleValue(
									varCxt,
									(*mergeSelectList[rowPos])[subKeyColumnId],
									tableSchema->columnInfoList_
											[subKeyColumnId].tupleType_,
									bindParamInfos));
				}

				NameWithCaseSensitivity dbName(context_.getDBName(), false);
				TargetContainerInfo targetInfo;
				bool needRefresh = false;
				NoSQLUtils::resolveTargetContainer(
						ec,
						value1,
						value2,
						db,
						tableSchema,
						this,
						dbNameWithCaseSensitive,
						tableName,
						targetInfo,
						needRefresh);

				if (needRefresh) {

					ALLOC_DELETE(alloc, latch);
					latch = ALLOC_NEW(alloc) NoSQLDB::TableLatch(
							ec, db, this, currentDBName, tableName, false);
					
					tableSchema = latch->get();
					remoteUpdateCache = true;
					
					if (bulkManager.partitioningType_
							!= tableSchema->partitionInfo_.partitionType_) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
					}
				}

				NodeAffinityNumber pos;
				if (keyColumnId != -1) {
					if (SyntaxTree::isRangePartitioningType(
							tableSchema->partitionInfo_.partitionType_)) {
						pos = targetInfo.affinity_;
					}
					else {
						pos = targetInfo.pos_;
					}
				}
				else {
					
					TableContainerInfo *containerInfo;
					containerInfo = &tableSchema->containerInfoList_[0];
					targetInfo.affinity_ = 0;
					targetInfo.pId_ = containerInfo->pId_;
					targetInfo.containerId_ = containerInfo->containerId_;
					targetInfo.versionId_ = containerInfo->versionId_;
					targetInfo.pos_ = containerInfo->pos_;
					pos = 0;
				}
				
				BulkEntry *store = bulkManager.add(
						pos, tableSchema, targetInfo);
				int64_t beforeSize = store->getSize();
				store->rowStore_->beginRow();

				for (size_t columnId = 0;
						columnId < selectList->size(); columnId++) {
					const TupleValue &value = getTupleValue(
							varCxt,
							(*mergeSelectList[rowPos])[columnId],
							tableSchema->columnInfoList_
									[columnId].tupleType_,
							bindParamInfos);

					DMLProcessor::setField(
							value.getType(),
							static_cast<ColumnId>(columnId),
							value, *store->rowStore_);
				}
				store->rowStore_->next();
				currentSize += (store->getSize() - beforeSize);

				if (currentSize > sizeLimit) {
	
					BulkEntry *currentStore;
					bulkManager.begin();

					while ((currentStore
							= bulkManager.next()) != NULL) {

						NoSQLContainer container(
								ec,
								currentStore->containerId_,
								currentStore->versionId_,
								currentStore->pId_,
								*context_.syncContext_,
								this);

						container.putRowSet(
								*currentStore->fixedPart_,
								*currentStore->varPart_,
								currentStore->rowStore_->getRowCount(),
								cmdOption);

						totalRowCount += currentStore->rowStore_->getRowCount();
						
						currentStore->init(
								currentStore->columnInfoList_,
								currentStore->columnCount_);
					}
				}
			}

			bulkManager.begin();
			while ((currentStore
					= bulkManager.next()) != NULL) {

				NoSQLContainer container(
						ec,
						currentStore->containerId_,
						currentStore->versionId_,
						currentStore->pId_,
						*context_.syncContext_,
						this);

				container.putRowSet(
						*currentStore->fixedPart_,
						*currentStore->varPart_, 
						currentStore->rowStore_->getRowCount(),
						cmdOption);

				totalRowCount += currentStore->rowStore_->getRowCount();
			}

			response_.updateCount_ = totalRowCount;
			executionManager_->incOperation(
					false, ec.getWorkerId(), totalRowCount);
		}
		catch (std::exception &e) {
			
			needSchemaRefresh = true;
			const util::Exception checkException 
					= GS_EXCEPTION_CONVERT(e, "");
			int32_t errorCode = checkException.getErrorCode();
			
			bool clearCache = false;
			checkReplyType(errorCode, clearCache);

			if (clearCache) {
				updateRemoteCache(
						ec,
						resourceSet_->getSQLService()->getEE(),
						resourceSet_,
						context_.getDBId(),
						context_.getDBName(),
						tableName.name_,
						MAX_TABLE_PARTITIONING_VERSIONID);

				remoteUpdateCache = true;
			}
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}

		if (latch && (needSchemaRefresh || remoteUpdateCache)) {
			TableSchemaInfo *schemaInfo = latch->get();
			if (schemaInfo) {
				TablePartitioningVersionId currentVersionId
						= MAX_TABLE_PARTITIONING_VERSIONID;

				if (needSchemaRefresh) {
				}
				else {
					if (remoteUpdateCache) {
						currentVersionId
								= schemaInfo->partitionInfo_.partitioningVersionId_;
					}
				}
				updateRemoteCache(
						ec,
						resourceSet_->getSQLService()->getEE(), 
						resourceSet_,
						context_.getDBId(),
						context_.getDBName(),
						tableName.name_,
						currentVersionId);
			}
		}

		SQLReplyContext replyCxt;
		replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		replyClient(replyCxt);
		
		if (!context_.isPreparedStatement()) {
			executionManager_->remove(ec, clientId_);
		}
		if (latch) {
			ALLOC_DELETE(alloc, latch);
		}

		uint32_t lap = watch.elapsedMillis();
		if (lap >= static_cast<uint32_t>(
				executionManager_->getTraceLimitTime())) {

			SQLDetailProfs profs(globalVarAlloc_,
					executionManager_->getTraceLimitTime(),
					executionManager_->getTraceLimitQuerySize());
			
			profs.set(
					context_.getDBName(),
					context_.getApplicationName(),
					context_.getQuery()); 
			
			profs.complete(lap);
		}
		return true;
	}
	catch (std::exception &e) {
		ALLOC_DELETE(alloc, latch);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}