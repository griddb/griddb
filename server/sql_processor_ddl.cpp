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
#include "sql_processor_ddl.h"
#include "sql_execution.h"
#include "sql_execution_manager.h"
#include "sql_compiler.h"
#include "nosql_db.h"
#include "nosql_container.h"
#include "nosql_request.h"
#include "resource_set.h"
#include "partition_table.h"
#include "transaction_service.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

const DDLProcessor::Registrar DDLProcessor::registrar_(SQLType::EXEC_DDL);

DDLProcessor::DDLProcessor(
		Context &cxt, const TypeInfo &typeInfo) :
				SQLProcessor(cxt, typeInfo),
				resourceSet_(cxt.getResourceSet()),
				globalVarAlloc_(resourceSet_->getSQLExecutionManager()
						->getVarAllocator()),
				clientId_(*cxt.getClientId()),
				commandType_(SyntaxTree::CMD_NONE),
				createUserInfo_(NULL),
				dropUserInfo_(NULL),
				setPasswordInfo_(NULL),
				grantInfo_(NULL),
				revokeInfo_(NULL),
				createDatabaseInfo_(NULL),
				dropDatabaseInfo_(NULL),
				createTableInfo_(NULL),
				dropTableInfo_(NULL),
				createIndexInfo_(NULL),
				dropIndexInfo_(NULL),
				dropPartitionInfo_(NULL),
				addColumnInfo_(NULL),
				createViewInfo_(NULL),
				dropViewInfo_(NULL),
				targetAckCount_(1),
				currentAckCount_(0),
				phase_(0),
				ackList_(globalVarAlloc_),
				ackContainerInfoList_(globalVarAlloc_),
				isRetry_(false),
				execution_(NULL),
				targetCheckAckCount_(0),
				currentCheckAckCount_(0),
				baseInfo_(NULL),
				isCleanup_(false) {
}

DDLProcessor::~DDLProcessor() {

	cleanupNoSQL();
	
	if (createUserInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, createUserInfo_);
	}

	if (dropUserInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, dropUserInfo_);
	}

	if (setPasswordInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, setPasswordInfo_);
	}

	if (grantInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, grantInfo_);
	}

	if (revokeInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, revokeInfo_);
	}

	if (createDatabaseInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, createDatabaseInfo_);
	}

	if (dropDatabaseInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, dropDatabaseInfo_);
	}

	if (createTableInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, createTableInfo_);
	}

	if (dropTableInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, dropTableInfo_);
	}

	if (createIndexInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, createIndexInfo_);
	}

	if (dropIndexInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, dropIndexInfo_);
	}

	if (dropPartitionInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, dropPartitionInfo_);
	}

	if (addColumnInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, addColumnInfo_);
	}

	if (createViewInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, createViewInfo_);
	}

	if (dropViewInfo_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, dropViewInfo_);
	}
}

struct AllocatorScope {

	AllocatorScope(SQLExecutionManager *manager) :
			manager_(manager),
			alloc_(manager->getStackAllocator()) {}

	~AllocatorScope() {
		if (alloc_) {
			manager_->releaseStackAllocator(alloc_);
		}
	}

	SQLExecutionManager *manager_;
	util::StackAllocator *alloc_;
};

void DDLProcessor::cleanupNoSQL() {
	
	try {
		if (isCleanup_) {
			return;
		}
		isCleanup_ = true;
		if (baseInfo_ && baseInfo_->isSync()) {
			
			ExecutionLatch latch(
					clientId_,
					resourceSet_->getSQLExecutionManager()
							->getResourceManager(),
					NULL);

			SQLExecution *execution = latch.get();
			if (execution) {
			
				if (execution->getContext().getSyncContext().isRunning() 
						&& baseInfo_->isSync()
						&& baseInfo_->currentContainerName_ != NULL) {

					AllocatorScope allocatorScope(
							resourceSet_->getSQLExecutionManager());
					util::StackAllocator &alloc = *allocatorScope.alloc_;
					util::StackAllocator::Scope scope(alloc);
					
					EventEngine::Stats stats;
					EventEngine::VariableSizeAllocator varSizeAlloc(
							util::AllocatorInfo(
									ALLOCATOR_GROUP_STORE, "getContainer"));

					EventEngine::EventContext::Source eventSource(
							varSizeAlloc, alloc, stats);
					EventEngine::EventContext ec(eventSource);
					
					NoSQLContainer targetContainer(
							ec,
							baseInfo_->currentContainerName_,
							execution->getContext().getSyncContext(),
							execution);

					NoSQLStoreOption option;
					option.isSync_ = false;

					targetContainer.setNoSQLAbort(
							execution->getContext().getCurrentSessionId());
					
					targetContainer.abort(option);
				}
			}
		}

		if (targetAckCount_ != currentAckCount_) {

			ExecutionLatch latch(
					clientId_,
					resourceSet_->getSQLExecutionManager()
							->getResourceManager(),
					NULL);
			
			SQLExecution *execution = latch.get();
			
			if (execution) {
				if (targetAckCount_ != currentAckCount_) {
			
					AllocatorScope allocatorScope(
							resourceSet_->getSQLExecutionManager());
					util::StackAllocator &alloc = *allocatorScope.alloc_;
					util::StackAllocator::Scope scope(alloc);

					EventEngine::Stats stats;
					EventEngine::VariableSizeAllocator varSizeAlloc(
							util::AllocatorInfo(
									ALLOCATOR_GROUP_STORE, "getContainer"));

					EventEngine::EventContext::Source eventSource(
							varSizeAlloc, alloc, stats);
					EventEngine::EventContext ec(eventSource);


					for (size_t pos = 0; pos < ackList_.size(); pos++) {
						if (ackList_[pos] == ACK_STATUS_ON) {
							NoSQLContainer targetContainer(
									ec,
									ackContainerInfoList_[pos].containerId_,
									0,
									ackContainerInfoList_[pos].pId_,
									execution->getContext().getSyncContext(),
									execution);

							NoSQLStoreOption option;
							option.isSync_ = false;

							targetContainer.setNoSQLAbort(
									ackContainerInfoList_[pos].sessionId_);
							targetContainer.setNoSQLClientId(
									ackContainerInfoList_[pos].clientId_);

							targetContainer.abort(option);
						}
					}
				}
			}
		}
		isCleanup_ = true;
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(
				SQL_SERVICE, e, "Cleanup nosql operation failed");
	}
}

bool DDLProcessor::applyInfo(
		Context &cxt,
		const Option &option,
		const TupleInfoList &inputInfo,
		TupleInfo &outputInfo) {

	UNUSED_VARIABLE(inputInfo);

	try {
		if (option.plan_ != NULL) {

			if (option.planNodeId_ >= option.plan_->nodeList_.size()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
			}

			const SQLPreparedPlan::Node &node
					= option.plan_->nodeList_[option.planNodeId_];
			commandType_ = node.commandType_;
		
			switch (commandType_) {

			case SyntaxTree::CMD_CREATE_USER:

				createUserInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						CreateUserInfo(globalVarAlloc_);
				
				setOptionString(
						node,
						0,
						createUserInfo_->userName_,
						&createUserInfo_->userNameCaseSensitive_);
				
				if (node.cmdOptionList_->size() > 1) {
					setOptionString(
							node, 1, createUserInfo_->password_);
				}
				break;

			case SyntaxTree::CMD_DROP_USER:

				dropUserInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						DropUserInfo(globalVarAlloc_);

				setOptionString(
						node,
						0,
						dropUserInfo_->userName_,
						&dropUserInfo_->userNameCaseSensitive_);
				break;

			case SyntaxTree::CMD_SET_PASSWORD:
				
				setPasswordInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						SetPasswordInfo(globalVarAlloc_);
				
				setOptionString(
						node,
						0,
						setPasswordInfo_->userName_,
						&setPasswordInfo_->userNameCaseSensitive_);

				if (node.cmdOptionList_->size() > 1) {
					setOptionString(
							node,
							1,
							setPasswordInfo_->password_);
				}

				break;

			case SyntaxTree::CMD_GRANT:

				grantInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						GrantInfo(globalVarAlloc_);

				setOptionString(
						node,
						0,
						grantInfo_->dbName_,
						&grantInfo_->dbNameCaseSensitive_);

				if (node.cmdOptionList_->size() > 1) {

					setOptionString(
							node,
							1,
							grantInfo_->userName_,
							&grantInfo_->userNameCaseSensitive_);
				}

				if (node.cmdOptionList_->size() > 2) {

					setOptionValue(
							node,
							2,
							grantInfo_->controlType_);
				}
				break;

			case SyntaxTree::CMD_REVOKE:
				
				revokeInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						RevokeInfo(globalVarAlloc_);

				setOptionString(
						node,
						0,
						revokeInfo_->dbName_,
						&revokeInfo_->dbNameCaseSensitive_);

				if (node.cmdOptionList_->size() > 1) {
				
					setOptionString(
							node,
							1,
							revokeInfo_->userName_,
							&revokeInfo_->userNameCaseSensitive_);
				}

				if (node.cmdOptionList_->size() > 2) {
				
					setOptionValue(
							node,
							2,
							revokeInfo_->controlType_);
				}
				break;

			case SyntaxTree::CMD_CREATE_DATABASE:

				createDatabaseInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						CreateDatabaseInfo(globalVarAlloc_);
				
				setOptionString(
						node,
						0,
						createDatabaseInfo_->dbName_,
						&createDatabaseInfo_->dbNameCaseSensitive_);

				break;

			case SyntaxTree::CMD_DROP_DATABASE:
				
				dropDatabaseInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						DropDatabaseInfo(globalVarAlloc_);
	
				setOptionString(
						node,
						0,
						dropDatabaseInfo_->dbName_,
						&dropDatabaseInfo_->dbNameCaseSensitive_);

				break;

			case SyntaxTree::CMD_CREATE_TABLE:
				
				createTableInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						CreateTableInfo(globalVarAlloc_);

				if (node.qName_->db_) {
				
					createTableInfo_->dbName_
							= node.qName_->db_->c_str();
					createTableInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				baseInfo_ = static_cast<DDLBaseInfo*>(
						createTableInfo_);

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				createTableInfo_->tableName_
						= node.qName_->table_->c_str();
				createTableInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;

				if (node.createTableOpt_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				createTableInfo_->createTableOpt_
						= node.createTableOpt_;
				createTableInfo_->execId_ = cxt.getExecId();
				createTableInfo_->jobVersionId_ = cxt.getVersionId();
				createTableInfo_->processor_ = this;
				
				break;

			case SyntaxTree::CMD_DROP_TABLE:
				
				dropTableInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						DropTableInfo(globalVarAlloc_);
				baseInfo_ = static_cast<DDLBaseInfo*>(dropTableInfo_);
			
				if (node.qName_->db_) {
					dropTableInfo_->dbName_
							= node.qName_->db_->c_str();
					dropTableInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				dropTableInfo_->tableName_
						= node.qName_->table_->c_str();
				dropTableInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;

				if (node.cmdOptionFlag_ == 1) {
					dropTableInfo_->ifExists_ = true;
				}

				dropTableInfo_->execId_ = cxt.getExecId();
				dropTableInfo_->jobVersionId_ = cxt.getVersionId();
				dropTableInfo_->processor_ = this;
				
				break;

			case SyntaxTree::CMD_CREATE_INDEX: {
				
				createIndexInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						CreateIndexInfo(globalVarAlloc_);
				baseInfo_ = static_cast<DDLBaseInfo*>(createIndexInfo_);

				if (node.qName_->db_) {

					createIndexInfo_->dbName_
							= node.qName_->db_->c_str();
					createIndexInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				createIndexInfo_->tableName_
						= node.qName_->table_->c_str();
				createIndexInfo_->tableNameCaseSensitive_ 
						= node.qName_->tableCaseSensitive_;

				if (node.createIndexOpt_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				if (node.cmdOptionList_->size() > 0) {
					
					setOptionString(
							node,
							0,
							createIndexInfo_->indexName_,
							&createIndexInfo_->indexNameCaseSensitive_);
				}

				createIndexInfo_->createIndexOpt_
						= node.createIndexOpt_;
				createIndexInfo_->execId_ = cxt.getExecId();
				createIndexInfo_->jobVersionId_ = cxt.getVersionId();
				
				createIndexInfo_->processor_ = this;
			}
			break;

			case SyntaxTree::CMD_DROP_INDEX:
			
				dropIndexInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						DropIndexInfo(globalVarAlloc_);
				baseInfo_ = static_cast<DDLBaseInfo*>(dropIndexInfo_);
			
				if (node.qName_->db_) {

					dropIndexInfo_->dbName_
							= node.qName_->db_->c_str();
					dropIndexInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				if (node.qName_->name_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Index name is invalid");
				}

				dropIndexInfo_->tableName_
						= node.qName_->table_->c_str();
				dropIndexInfo_->indexName_
						= node.qName_->name_->c_str();
				dropIndexInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;
				dropIndexInfo_->indexNameCaseSensitive_
						= node.qName_->nameCaseSensitive_;
				
				if (node.cmdOptionFlag_ == 1) {
					dropIndexInfo_->ifExists_ = true;
				}

				dropIndexInfo_->execId_ = cxt.getExecId();
				dropIndexInfo_->jobVersionId_ = cxt.getVersionId();
				dropIndexInfo_->processor_ = this;
				break;

			case SyntaxTree::CMD_ALTER_TABLE_DROP_PARTITION:
				
				dropPartitionInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						DropTablePartitionInfo(globalVarAlloc_);
				baseInfo_ = static_cast<DDLBaseInfo*>(dropPartitionInfo_);
				
				if (node.qName_->db_) {

					dropPartitionInfo_->dbName_
							= node.qName_->db_->c_str();
					dropPartitionInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				dropPartitionInfo_->tableName_
						= node.qName_->table_->c_str();
				dropPartitionInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;

				if (node.cmdOptionFlag_ == 1) {
					dropPartitionInfo_->ifExists_ = true;
				}
				
				dropPartitionInfo_->cmdOptionList_ 
						= node.cmdOptionList_;
				dropPartitionInfo_->execId_ = cxt.getExecId();
				dropPartitionInfo_->jobVersionId_ = cxt.getVersionId();
				dropPartitionInfo_->processor_ = this;
				
				break;

			case SyntaxTree::CMD_ALTER_TABLE_ADD_COLUMN:
				addColumnInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						AddColumnInfo(globalVarAlloc_);
				baseInfo_ = static_cast<DDLBaseInfo*>(addColumnInfo_);
				
				if (node.qName_->db_) {
					
					addColumnInfo_->dbName_
							= node.qName_->db_->c_str();
					addColumnInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				addColumnInfo_->tableName_
						= node.qName_->table_->c_str();
				addColumnInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;

				if (node.createTableOpt_->columnInfoList_) {

					SyntaxTree::ColumnInfoList::iterator
							itr = node.createTableOpt_->columnInfoList_->begin();

					for (; itr != node.createTableOpt_->columnInfoList_->end();
							++itr) {

						if ((*itr)->isPrimaryKey()) {
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_DDL_INVALID_PARAMETER,
									"Column constraint PRIMARY KEY is not allowed here");
						}

						if ((*itr)->isVirtual()) {
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_DDL_INVALID_PARAMETER,
									"Column constraint VIRTUAL is not allowed here");
						}
					}

					addColumnInfo_->createTableOpt_
							= node.createTableOpt_;
					addColumnInfo_->execId_ = cxt.getExecId();
					addColumnInfo_->jobVersionId_ = cxt.getVersionId();

					addColumnInfo_->processor_ = this;
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"No column list");
				}
				break;

			case SyntaxTree::CMD_CREATE_VIEW:

				createViewInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						CreateViewInfo(globalVarAlloc_);

				if (node.qName_->db_) {

					createViewInfo_->dbName_
							= node.qName_->db_->c_str();
					createViewInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				baseInfo_ = static_cast<DDLBaseInfo*>(createViewInfo_);

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"View name is invalid");
				}

				createViewInfo_->tableName_
						= node.qName_->table_->c_str();
				createViewInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;

				if (node.createTableOpt_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"View parametor is invalid");
				}

				createViewInfo_->createTableOpt_
						= node.createTableOpt_;
				createViewInfo_->execId_ = cxt.getExecId();
				createViewInfo_->jobVersionId_ = cxt.getVersionId();
				createViewInfo_->processor_ = this;

				break;

			case SyntaxTree::CMD_DROP_VIEW:

				dropViewInfo_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						DropViewInfo(globalVarAlloc_);
				baseInfo_ = static_cast<DDLBaseInfo*>(dropViewInfo_);
				
				if (node.qName_->db_) {
					
					dropViewInfo_->dbName_
							= node.qName_->db_->c_str();
					dropViewInfo_->dbNameCaseSensitive_
							= node.qName_->dbCaseSensitive_;
				}

				if (node.qName_->table_ == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Table name is invalid");
				}

				dropViewInfo_->tableName_
						= node.qName_->table_->c_str();
				dropViewInfo_->tableNameCaseSensitive_
						= node.qName_->tableCaseSensitive_;

				if (node.cmdOptionFlag_ == 1) {
					dropViewInfo_->ifExists_ = true;
				}

				dropViewInfo_->execId_ = cxt.getExecId();
				dropViewInfo_->jobVersionId_ = cxt.getVersionId();
				dropViewInfo_->processor_ = this;

				break;

			default:
				break;
			}
		}
		else if (option.byteInStream_ != NULL) {
		}
		else if (option.inStream_ != NULL) {
		}
		else if (option.jsonValue_ != NULL) {
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
		}

		outputInfo.push_back(TupleList::TYPE_LONG);
		
		ExecutionLatch latch(
				clientId_,
				resourceSet_->getSQLExecutionManager()
						->getResourceManager(),
				NULL);
		
		execution_ = latch.get();
		
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool DDLProcessor::pipe(
		Context &cxt, InputId inputId,
		const Block &block) {

	UNUSED_VARIABLE(inputId);
	UNUSED_VARIABLE(block);

	try {

		EventContext &ec = *cxt.getEventContext();
		
		ExecutionLatch latch(
				clientId_,
				cxt.getExecutionManager()->getResourceManager(),
				NULL);

		SQLExecution *execution = latch.get();
		
		if (execution == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_CANCELLED,
					"Cancel SQL, clientId=" << clientId_ << ", location=ddl");
		}

		NoSQLDB *db = resourceSet_->getSQLExecutionManager()->getDB();
		bool isFinish = true;
		
		switch (commandType_) {
		
		case SyntaxTree::CMD_CREATE_USER:
				
			db->createUser(
					ec,
					execution,
					NameWithCaseSensitivity(
							createUserInfo_->userName_.c_str(),
							createUserInfo_->userNameCaseSensitive_),
					NameWithCaseSensitivity(
							createUserInfo_->password_.c_str()));
			
			break;

		case SyntaxTree::CMD_DROP_USER:
			
			db->dropUser(
					ec,
					execution,
					NameWithCaseSensitivity(
							dropUserInfo_->userName_.c_str(),
							dropUserInfo_->userNameCaseSensitive_));
			
			break;

		case SyntaxTree::CMD_SET_PASSWORD:

			db->setPassword(
					ec,
					execution,
					NameWithCaseSensitivity(
							setPasswordInfo_->userName_.c_str(),
							setPasswordInfo_->userNameCaseSensitive_),
					NameWithCaseSensitivity(
							setPasswordInfo_->password_.c_str()));
			
			break;

		case SyntaxTree::CMD_GRANT:
			
			db->grant(
					ec,
					execution,
					NameWithCaseSensitivity(
							grantInfo_->userName_.c_str(),
							grantInfo_->userNameCaseSensitive_),
				NameWithCaseSensitivity(
							grantInfo_->dbName_.c_str(),
							grantInfo_->dbNameCaseSensitive_),
				grantInfo_->controlType_);
			
			break;

		case SyntaxTree::CMD_REVOKE:
			
			db->revoke(
					ec,
					execution,
					NameWithCaseSensitivity(
							revokeInfo_->userName_.c_str(),
							revokeInfo_->userNameCaseSensitive_),
					NameWithCaseSensitivity(
							revokeInfo_->dbName_.c_str(),
							revokeInfo_->dbNameCaseSensitive_),
				revokeInfo_->controlType_);

			break;

		case SyntaxTree::CMD_CREATE_DATABASE:
			
			db->createDatabase(
					ec,
					execution,
					NameWithCaseSensitivity(
							createDatabaseInfo_->dbName_.c_str(),
							createDatabaseInfo_->dbNameCaseSensitive_));

			break;

		case SyntaxTree::CMD_DROP_DATABASE:

			db->dropDatabase(
					ec,
					execution,
					NameWithCaseSensitivity(
							dropDatabaseInfo_->dbName_.c_str(),
							dropDatabaseInfo_->dbNameCaseSensitive_));

			break;
		
		case SyntaxTree::CMD_CREATE_TABLE:
			
			db->createTable(
					ec,
					execution, 
					NameWithCaseSensitivity(
							createTableInfo_->dbName_.c_str(),
							createTableInfo_->dbNameCaseSensitive_),
					NameWithCaseSensitivity(
							createTableInfo_->tableName_.c_str(),
							createTableInfo_->tableNameCaseSensitive_),
					*createTableInfo_->createTableOpt_,
					createTableInfo_);

			break;

		case SyntaxTree::CMD_DROP_TABLE:

			if (phase_ == DDL_STATUS_INIT) {

				db->dropTable(
						ec,
						execution, 
						NameWithCaseSensitivity(
							dropTableInfo_->dbName_.c_str(),
							dropTableInfo_->dbNameCaseSensitive_),
						NameWithCaseSensitivity(
							dropTableInfo_->tableName_.c_str(),
							dropTableInfo_->tableNameCaseSensitive_),
						dropTableInfo_->ifExists_,
						dropTableInfo_);

				isFinish = dropTableInfo_->isFinished_;
				phase_ = DDL_STATUS_POST;
			}
			else {
				currentAckCount_++;
				if (currentAckCount_ != targetAckCount_) {
					isFinish = false;
				}
				phase_ = DDL_STATUS_END;
			}

			break;

		case SyntaxTree::CMD_CREATE_INDEX:
			
			if (phase_ == DDL_STATUS_INIT) {

				createIndexInfo_->ec_ = cxt.getEventContext();
				
				db->createIndex(
						ec,
						execution,
						NameWithCaseSensitivity(
								createIndexInfo_->dbName_.c_str(),
								createIndexInfo_->dbNameCaseSensitive_),
						NameWithCaseSensitivity(
								createIndexInfo_->indexName_.c_str(),
								createIndexInfo_->indexNameCaseSensitive_),
						NameWithCaseSensitivity(
								createIndexInfo_->tableName_.c_str(),
								createIndexInfo_->tableNameCaseSensitive_),
						*createIndexInfo_->createIndexOpt_,
						createIndexInfo_);

				isFinish = createIndexInfo_->isFinished_;
				phase_ = DDL_STATUS_POST;
			}
			else {
				currentAckCount_++;
				if (currentAckCount_ != targetAckCount_) {
					isFinish = false;
				}
				else {
				
					db->createIndexPost(
							ec,
							execution,
							NameWithCaseSensitivity(
									createIndexInfo_->dbName_.c_str(),
									createIndexInfo_->dbNameCaseSensitive_),
							NameWithCaseSensitivity(
									createIndexInfo_->indexName_.c_str(),
									createIndexInfo_->indexNameCaseSensitive_),
							NameWithCaseSensitivity(
									createIndexInfo_->tableName_.c_str(),
									createIndexInfo_->tableNameCaseSensitive_),
							createIndexInfo_);

					phase_ = DDL_STATUS_END;
				}
			}

			break;

		case SyntaxTree::CMD_DROP_INDEX:
			
			if (phase_ == DDL_STATUS_INIT) {
			
				dropIndexInfo_->ec_ = cxt.getEventContext();

				db->dropIndex(
						ec,
						execution,
						NameWithCaseSensitivity(
								dropIndexInfo_->dbName_.c_str(),
								dropIndexInfo_->dbNameCaseSensitive_),
						NameWithCaseSensitivity(
								dropIndexInfo_->indexName_.c_str(),
								dropIndexInfo_->indexNameCaseSensitive_),
						NameWithCaseSensitivity(
								dropIndexInfo_->tableName_.c_str(),
								dropIndexInfo_->tableNameCaseSensitive_),
						dropIndexInfo_->ifExists_,
						dropIndexInfo_);

				isFinish = dropIndexInfo_->isFinished_;
				phase_ = DDL_STATUS_POST;
			}
			else {
				currentAckCount_++;
				if (currentAckCount_ != targetAckCount_) {
					isFinish = false;
				}
				else {
					db->dropIndexPost(
							ec,
							execution,
							NameWithCaseSensitivity(
									dropIndexInfo_->dbName_.c_str(),
									dropIndexInfo_->dbNameCaseSensitive_),
							NameWithCaseSensitivity(
									dropIndexInfo_->indexName_.c_str(),
									dropIndexInfo_->indexNameCaseSensitive_),
							NameWithCaseSensitivity(
									dropIndexInfo_->tableName_.c_str(),
									dropIndexInfo_->tableNameCaseSensitive_),
							dropIndexInfo_);

					phase_ = DDL_STATUS_END;
				}
			}

			break;

		case SyntaxTree::CMD_ALTER_TABLE_DROP_PARTITION:
			
			if (phase_ == DDL_STATUS_INIT) {
			
				db->dropTablePartition(
						ec,
						execution, 
						NameWithCaseSensitivity(
								dropPartitionInfo_->dbName_.c_str(),
								dropPartitionInfo_->dbNameCaseSensitive_),
						NameWithCaseSensitivity(
								dropPartitionInfo_->tableName_.c_str(),
								dropPartitionInfo_->tableNameCaseSensitive_),
						dropPartitionInfo_);

				isFinish = dropPartitionInfo_->isFinished_;
					phase_ = DDL_STATUS_POST;
			}
			else {
				currentAckCount_++;
				if (currentAckCount_ != targetAckCount_) {
					isFinish = false;
				}
				else {
					db->dropTablePartitionPost(
							ec,
							execution, 
							NameWithCaseSensitivity(
									dropPartitionInfo_->dbName_.c_str(),
									dropPartitionInfo_->dbNameCaseSensitive_),
							NameWithCaseSensitivity(
									dropPartitionInfo_->tableName_.c_str(),
									dropPartitionInfo_->tableNameCaseSensitive_));
				}
				
				phase_ = DDL_STATUS_END;
			}

			break;

		case SyntaxTree::CMD_ALTER_TABLE_ADD_COLUMN:
			
			db->addColumn(
					ec, 
					execution, 
					NameWithCaseSensitivity(
							addColumnInfo_->dbName_.c_str(),
							addColumnInfo_->dbNameCaseSensitive_),
					NameWithCaseSensitivity(
							addColumnInfo_->tableName_.c_str(),
							addColumnInfo_->tableNameCaseSensitive_),
					*addColumnInfo_->createTableOpt_,
					addColumnInfo_);

			break;

		case SyntaxTree::CMD_CREATE_VIEW:
			
			db->createView(
					ec,
					execution, 
					NameWithCaseSensitivity(
							createViewInfo_->dbName_.c_str(),
							createViewInfo_->dbNameCaseSensitive_),
					NameWithCaseSensitivity(
							createViewInfo_->tableName_.c_str(),
							createViewInfo_->tableNameCaseSensitive_),
					*createViewInfo_->createTableOpt_,
					createViewInfo_);

			break;

		case SyntaxTree::CMD_DROP_VIEW:

			db->dropView(
					ec,
					execution, 
					NameWithCaseSensitivity(
							dropViewInfo_->dbName_.c_str(),
							dropViewInfo_->dbNameCaseSensitive_),
					NameWithCaseSensitivity(
						dropViewInfo_->tableName_.c_str(),
						dropViewInfo_->tableNameCaseSensitive_),
					dropViewInfo_->ifExists_);

			break;

		default:
			break;
		}

		if (isFinish) {
			int64_t resultCount = 0;
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

bool DDLProcessor::finish(
		Context &cxt, InputId inputId) {

	UNUSED_VARIABLE(cxt);
	UNUSED_VARIABLE(inputId);

	try {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INTERNAL, "");
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

void DDLProcessor::exportTo(
		Context &cxt, const OutOption &option) const {
	
	UNUSED_VARIABLE(cxt);
	UNUSED_VARIABLE(option);
}

void DDLProcessor::setOptionString(
		const SQLPreparedPlan::Node &node,
		int32_t pos, 
		SQLString &str,
		bool *isCaseSensitive) {

	if ((*node.cmdOptionList_)[pos] != NULL) {

		const SyntaxTree::Expr&
				expr = *(*node.cmdOptionList_)[pos];

		if (expr.op_ == SQLType::EXPR_CONSTANT) {

			if (expr.value_.getType() == TupleList::TYPE_STRING) {
					str.append(
							static_cast<const char*>(expr.value_.varData()),
							expr.value_.varSize());
			}
			else if (expr.value_.getType() == TupleList::TYPE_NULL) {
			}
			else {
				assert(0);
			}

			if (isCaseSensitive != NULL) {
				*isCaseSensitive = expr.qName_->nameCaseSensitive_;
			}
		}
		else if (expr.op_ == SQLType::EXPR_COLUMN ||
					expr.op_ == SQLType::EXPR_ID) {

			assert(expr.qName_ && expr.qName_->name_);
			
			SQLString tmpStr(globalVarAlloc_);
			tmpStr = expr.qName_->name_->c_str();
			str.append(tmpStr);
			
			if (isCaseSensitive != NULL) {
				*isCaseSensitive = expr.qName_->nameCaseSensitive_;
			}
		}
		else {
			assert(false);
		}
	}
}

template <typename T>
void DDLProcessor::setOptionValue(
		const SQLPreparedPlan::Node &node,
		int32_t pos, T &t) {

	int32_t value;
	memcpy(&value,
			(*node.cmdOptionList_)[pos]->value_.fixedData(),
			sizeof(int32_t));

	t = static_cast<T>(value);
}

void DDLProcessor::setAckStatus(
		NoSQLContainer *containerInfo,
		int32_t pos,
		uint8_t status) {

	if (pos < 0) {
		return;
	}
	
	if (pos < static_cast<int32_t>(ackList_.size())) {
	}
	else {
		return;
	}
	ackList_[pos] = status;
	
	if (containerInfo) {
	
		AckContainerInfo ackInfo;
		ackInfo.containerId_ = containerInfo->getContainerId();
		ackInfo.pId_ = containerInfo->getPartitionId();
		ackInfo.stmtId_ = containerInfo->getStatementId();
		ackInfo.clientId_ = containerInfo->getNoSQLClientId();

		if (execution_) {
			ackInfo.sessionId_
					= execution_->getContext().getCurrentSessionId();
		}

		PartitionTable *pt
				= execution_->getResourceSet()->getPartitionTable();

		if (ackInfo.pId_ != UNDEF_PARTITIONID) {
		
			ackInfo.ptRev_
					= pt->getNewSQLPartitionRevision(ackInfo.pId_);
			ackInfo.nodeId_
					= pt->getNewSQLOwner(ackInfo.pId_);
			ackInfo.masterNodeId_ = pt->getMaster();
			ackInfo.pos_ = pos;
		}

		ackContainerInfoList_.push_back(ackInfo);
	}
}

void DDLProcessor::checkPartitionStatus() {

	if (phase_ == DDL_STATUS_INIT) return;

	PartitionTable *pt = resourceSet_->getPartitionTable();
	
	assert(ackList_.size() == ackContainerInfoList_.size());

	for (size_t pos = 0;
			pos < ackContainerInfoList_.size(); pos++) {

		size_t realPos = ackContainerInfoList_[pos].pos_;
		
		if (realPos >= 0
				&& realPos < ackList_.size()
				&& ackList_[realPos] == ACK_STATUS_OFF) continue;

		PartitionId pId = ackContainerInfoList_[pos].pId_;
		if (pId == UNDEF_PARTITIONID) continue;
		
		if (ackContainerInfoList_[pos].ptRev_
				!= pt->getNewSQLPartitionRevision(pId)
				|| ackContainerInfoList_[pos].masterNodeId_
						!= pt->getMaster()) {

			GS_THROW_CUSTOM_ERROR(DenyException,
					GS_ERROR_TXN_PARTITION_ROLE_UNMATCH, 
							"Check partition status, unmatch partition revison "
							"(expected revision=" << ackContainerInfoList_[pos].ptRev_
							<< ", actual revision=" <<pt->getNewSQLPartitionRevision(pId)
							<< ", expected master=" << pt->dumpNodeAddress(
									ackContainerInfoList_[pos].masterNodeId_)
							<< ", actual master=" << pt->dumpNodeAddress(pt->getMaster()) << ")");
		}

		if (ackContainerInfoList_[pos].nodeId_
				!= pt->getNewSQLOwner(pId)) {
			
			GS_THROW_CUSTOM_ERROR(
					DenyException,
					GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
							"Check partition status, "
							"unmatch partition role (expected owner="
							<<  pt->dumpNodeAddress(
											ackContainerInfoList_[pos].nodeId_)
							<< ",  actual owner=" 
							<< pt->dumpNodeAddress(
										pt->getNewSQLOwner(pId)));
		}
	}
}
