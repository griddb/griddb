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
#include "sql_execution.h"
#include "resource_set.h"
#include "sql_execution_manager.h"
#include "sql_job_manager.h"
#include "sql_compiler.h"
#include "sql_processor.h"
#include "sql_service.h"
#include "sql_utils.h"
#include "sql_allocator_manager.h"
#include "nosql_db.h"
#include "nosql_store.h"
#include "nosql_container.h"
#include "nosql_request.h"
#include "nosql_utils.h"
#include "picojson.h"
#include "json.h"
#include "sql_job.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(SQL_DETAIL);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK_DETAIL);

typedef StatementHandler::ConnectionOption ConnectionOption;


#define TRACE_SQL_START(jobId, hostName, processorName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" SQL 0 START IN 0 " << \
			processorName \
			<< " PIPE");

#define TRACE_SQL_END(jobId, hostName, processorName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" SQL 0 END IN 0 " << \
			processorName \
			<< " PIPE");

SQLExecution::SQLExecution(
		SQLExecutionRequestInfo &executionRequest) try :
				clientId_(executionRequest.requestInfo_.clientId_),
				resourceSet_(&executionRequest.resourceSet_),
				jobManager_(resourceSet_->getJobManager()),
				executionManager_(
						resourceSet_->getSQLExecutionManager()),
				db_(executionManager_->getDB()),
				executionStackAlloc_(*wrapStackAllocator()),
				globalVarAlloc_(
						*resourceSet_->getGlobaVariableSizeAllocator()),
				localVarAlloc_(*resourceSet_->getSQLService()
						->getAllocatorManager()
						->getCommonVariableSizeLocalAllocator(
								SQL_SERVICE,
								executionRequest.requestInfo_.pgId_)),
				scope_(NULL),
				group_(*resourceSet_->getTempolaryStore()),
				resultSet_(NULL),
				isCancelled_(false),
				currentJobId_(JobId()),
				preparedInfo_(globalVarAlloc_),
				bindParamSet_(executionStackAlloc_),
				clientInfo_(
						executionStackAlloc_,
						globalVarAlloc_,
						*executionRequest.clientNd_,
						clientId_),
				analyedQueryInfo_(
						globalVarAlloc_, executionStackAlloc_),
				executionStatus_(executionRequest.requestInfo_),
				config_(executionRequest.requestInfo_),
				batchCount_(0),
				batchCurrentPos_(0),
				isBatch_(false),
				context_(
						resourceSet_,
						lock_,
						clientInfo_,
						analyedQueryInfo_,executionStatus_,
						response_,
						clientId_,
						currentJobId_) {

	executionStatus_.set(
			executionRequest.requestInfo_);
	scope_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			util::StackAllocator::Scope(executionStackAlloc_);

	setupSyncContext();
	resultSet_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			SQLResultSet(globalVarAlloc_);

	currentJobId_.set(clientId_, 0);
}
catch (...) {
	ALLOC_VAR_SIZE_DELETE(
			globalVarAlloc_, resultSet_);
	
	if (context_.syncContext_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, context_.syncContext_);
	}
	
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, scope_);
	ALLOC_VAR_SIZE_DELETE(
			globalVarAlloc_, &executionStackAlloc_);
	updateConnection();
}

SQLExecution::~SQLExecution()  try {

	ALLOC_VAR_SIZE_DELETE(
			globalVarAlloc_, resultSet_);

	try {
		if (context_.syncContext_) {
			context_.syncContext_->cancel();
		}
	}
	catch (std::exception&) {
	}
	
	if (context_.syncContext_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, context_.syncContext_);
	}
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, scope_);

	executionManager_->releaseStackAllocator(
			&executionStackAlloc_);
	updateConnection();
	cleanupSerializedData();

}
catch (...) {
	assert(false);
}

void SQLExecution::setRequest(RequestInfo &request) {

	executionStatus_.execId_ = request.stmtId_;
	executionStatus_.connectedPId_ = request.pId_;
	executionStatus_.connectedPgId_ = request.pgId_;
	executionStatus_.requestType_
			= request.requestType_;
	context_.setQuery(request.sqlString_);
	response_.set(request, resultSet_);
	executionStatus_.responsed_ = false;
}

SQLTableInfoList* SQLExecution::generateTableInfo(
		SQLExpandViewContext &cxt) {

	SQLParsedInfo &parsedInfo = *cxt.parsedInfo_;
	SQLConnectionEnvironment  *control = cxt.control_;
	SQLTableInfoList *tableInfoList = cxt.tableInfoList_;

	std::pair<util::Set<util::String>::iterator, bool> outResult;
	cxt.forceMetaDataQuery_ = false;

	std::string value;
	if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_EXECUTE_AS_META_DATA_QUERY,
					value)) {

		if (value == SQLPragma::VALUE_TRUE) {
			cxt.forceMetaDataQuery_ = true;
		}
	}

	cxt.isMetaDataQuery_ = (
			executionStatus_.requestType_ == REQUEST_TYPE_PRAGMA
					|| cxt.forceMetaDataQuery_);

	std::string pragmaValue;
	cxt.metaVisible_ = true;
	if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_META_TABLE_VISIBLE,
					pragmaValue)) {
		cxt.metaVisible_ = (pragmaValue == SQLPragma::VALUE_TRUE);
	}

	cxt.internalMode_ = false;
	if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_INTERNAL_META_TABLE_VISIBLE,
			pragmaValue)) {
		cxt.internalMode_ = (pragmaValue == SQLPragma::VALUE_TRUE);
	}

	cxt.forDriver_ = cxt.isMetaDataQuery_;
	if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_DRIVER_META_TABLE_VISIBLE,
			pragmaValue)) {
		cxt.forDriver_ |= (pragmaValue == SQLPragma::VALUE_TRUE);
	}

	if (parsedInfo.syntaxTreeList_.size() > 0) {
		if (!parsedInfo.createForceView_) {
			for (util::Vector<SyntaxTree::Expr*>::iterator
						it = parsedInfo.tableList_.begin();
						it != parsedInfo.tableList_.end(); it++) {
				SyntaxTree::Expr *expr = *it;
				getTableInfo(cxt, expr, 0);
			}
		}
	}
	return tableInfoList;
}

const SQLTableInfo* SQLExecution::getTableInfo(
		SQLExpandViewContext &cxt,
		SyntaxTree::Expr *expr,
		uint32_t viewDepth,
		bool viewOnly) {

	UNUSED_VARIABLE(viewDepth);

	util::StackAllocator &alloc = cxt.getAllocator();
	PartitionTable *pt = resourceSet_->getPartitionTable();
	const char8_t *defaultDbName = cxt.defaultDbName_;
	SQLConnectionEnvironment  *control = cxt.control_;
	EventContext &ec = *cxt.ec_;
	SQLTableInfoList *tableInfoList = cxt.tableInfoList_;

	bool useCache = cxt.useCache_;
	bool withVersion = cxt.withVersion_;
	util::Vector<NodeId> &liveNodeIdList
			= *cxt.liveNodeIdList_;
	std::pair<util::Set<util::String>::iterator, bool> outResult;

	TableSchemaInfo *tableSchema = NULL;
	const SyntaxTree::QualifiedName
			*srcName = expr->qName_;

	if (srcName == NULL) {
		return NULL;
	}

	const util::String *tableName = srcName->table_;
	if (tableName == NULL) {
		return NULL;
	}
	
	NameWithCaseSensitivity currentTableName(
			tableName->c_str(),
			srcName->tableCaseSensitive_);

	if (SQLCompiler::Meta::isSystemTable(*srcName)) {
		if (viewOnly) {
			return NULL;
		}
		SQLTableInfo tableInfo(alloc);
		tableInfo.tableName_ = *tableName;
		tableInfo.dbName_ = defaultDbName;
		tableInfo.idInfo_.dbId_ = context_.getDBId();
		tableInfo.hasRowKey_ = false;

		std::string pragmaValue;

		bool metaVisible = true;
		if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_META_TABLE_VISIBLE,
			pragmaValue)) {
			metaVisible = (pragmaValue == SQLPragma::VALUE_TRUE);
		}

		bool internalMode = false;
		if (control->getEnv(
				SQLPragma::PRAGMA_INTERNAL_COMPILER_INTERNAL_META_TABLE_VISIBLE,
			pragmaValue)) {
			internalMode = (pragmaValue == SQLPragma::VALUE_TRUE);
		}

		bool forDriver = cxt.isMetaDataQuery_;
		if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_DRIVER_META_TABLE_VISIBLE,
			pragmaValue)) {
			forDriver |= (pragmaValue == SQLPragma::VALUE_TRUE);
		}

		const SQLTableInfo::IdInfo &idInfo =
				SQLCompiler::Meta::setSystemTableColumnInfo(
						alloc,
						*srcName,
						tableInfo,
						metaVisible,
						internalMode,
						forDriver);

		bool isNodeExpansion
				= SQLCompiler::Meta::isNodeExpansion(idInfo.containerId_);

		const SQLTableInfo &sqlTableInfo
				= tableInfoList->add(tableInfo);
		uint32_t partitionCount = pt->getPartitionNum(); 
		
		if (isNodeExpansion) { 
			partitionCount = static_cast<uint32_t>(
					liveNodeIdList.size()); 
		} 

		tableInfoList->prepareMeta(
				*srcName,
				idInfo,
				context_.getDBId(),
				partitionCount);

		return &sqlTableInfo;
	}

	bool isAdjust = false;
	bool dbCaseSensitive = expr->qName_->dbCaseSensitive_;
	util::String currentDb(alloc);
	if (expr->qName_->db_) {

		NoSQLUtils::checkConnectedDbName(
				alloc,
				context_.getDBName(),
				expr->qName_->db_->c_str(),
				dbCaseSensitive);

		currentDb = *(expr->qName_->db_);
	}
	else {
		currentDb = util::String(defaultDbName, alloc);
		isAdjust = true;
	}

	NameWithCaseSensitivity currentDbName(
			currentDb.c_str(), dbCaseSensitive);

	if (!currentTableName.isCaseSensitive_) {
		const util::String &normalizeTableName
				= SQLUtils::normalizeName(
						alloc, tableName->c_str());
		outResult = cxt.tableSet_->insert(
				normalizeTableName);
	}
	else {
		util::String currentTableNameStr(
				currentTableName.name_, alloc);
		outResult = cxt.tableSet_->insert(
				currentTableNameStr);
	}

	if (!outResult.second) {
		const SQLTableInfo* sqlTableInfo = tableInfoList->find(
				currentDb.c_str(), 
				tableName->c_str(),
				dbCaseSensitive,
				currentTableName.isCaseSensitive_);
		return sqlTableInfo;
	}

	CreateTableOption option(alloc);
	NoSQLDB *db = executionManager_->getDB();

	try {
		SQLTableInfo tableInfo(alloc);
		tableInfo.dbName_ = currentDb;
		
		NoSQLDB::TableLatch latch(
				ec, db, this, currentDbName,
				currentTableName, useCache);
		
		bool withoutCache = !cxt.useCache_;
		tableSchema = latch.get();
		if (tableSchema != NULL) {
			if (tableSchema->containerInfoList_.size() == 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_INVALID_SCHEMA_INFO,
						"Invalid scheme info, container list is empty");
			}

			setTimeSeriesIncluded(
					tableSchema->isTimeSeriesContainer());
			EventMonotonicTime currentEmTime
					= ec.getEngine().getMonotonicTime();
			checkRefreshTable(
					ec,
					currentTableName,
					tableSchema,
					currentEmTime);
			
			tableSchema->checkSubContainer(0);
			checkWritableTable(
					*control,
					tableSchema,
					tableInfo);
			
			const uint32_t clusterPartitionCount =
					pt->getPartitionNum();

			tableSchema->setupSQLTableInfo(
					alloc,
					tableInfo, 
					currentTableName.name_,
					context_.getDBId(),
					withVersion,
					clusterPartitionCount,
					withoutCache,
					ec.getHandlerStartTime().getUnixTime(),
					tableSchema->sqlString_.c_str()
			);

			const SQLTableInfo &sqlTableInfo
					= tableInfoList->add(tableInfo);
			return &sqlTableInfo;
		}
		return NULL;
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecution::checkCancel() {
	
	if (isCancelled_) {
		isCancelled_ = false;
	}
}

void SQLExecution::execute(
		EventContext &ec,
		RequestInfo &request,
		bool prepareBinded,
		std::exception *e,
		uint8_t versionId,
		JobId *responseJobId) {

	util::StackAllocator &alloc = ec.getAllocator();
	PartitionTable *pt
			= resourceSet_->getPartitionTable();
	if (e == NULL && !isBatch_) {
		setRequest(request);
	}

	
	if (e == NULL) {
		GS_TRACE_INFO(
				DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
				"SQL=" << request.sqlString_.c_str()
				<< ", clientId=" << clientId_);
	
		GS_TRACE_DEBUG(SQL_DETAIL,
				GS_TRACE_SQL_EXECUTION_INFO,
				"sql=" << request.sqlString_.c_str());
	}

	if ((request.requestType_ == REQUEST_TYPE_PREPARE)
			|| (request.isBind() && request.retrying_
			&& request.sqlString_.size() > 0)) {
		setPrepared();
	}

	bool isException = (e != NULL);
	response_.prepareBinded_  = prepareBinded;
	SQLParsedInfo &parsedInfo = getParsedInfo();
	checkCancel();
	
	if (!isException) {
		cleanupPrevJob();
		if (!isBatch_) {
			cleanupSerializedData();
			copySerializedBindInfo(
					request.serializedBindInfo_);
		}
	}
		
	RequestInfo request1(alloc, false);
	restoreBindInfo(request1);

	if (!isBatch_ && request1.bindParamSet_.getRowCount() > 1) {
		isBatch_ = true;
		request1.bindParamSet_.reset();
		batchCount_ = request1.bindParamSet_.getRowCount();
	}
	else {
		if (!isBatch_) {
			isBatch_ = false;
		}
	}
	
	SQLConnectionEnvironment connectionControl(
			executionManager_, this);

	JobId currentJobId;
	int32_t currentVersionId = versionId;
	context_.getCurrentJobId(currentJobId);
	
	if (versionId == UNDEF_JOB_VERSIONID) {
		currentVersionId = currentJobId.versionId_;
	}

	int64_t delayTime = 0;
	bool isPreaparedRetry = false;
	
	if (context_.isRetryStatement()) {
		JobId jobId(clientId_, context_.getExecId());
		currentVersionId = 1;
		jobId.versionId_ = static_cast<uint8_t>(currentVersionId);
		context_.setCurrentJobId(jobId);
	}

	util::Vector<NodeId> liveNodeIdList(alloc);
	pt->getLiveNodeIdList(liveNodeIdList);
	int32_t maxNodeNum = pt->getNodeNum();

	for (;currentVersionId < MAX_JOB_VERSIONID + 1;
			currentVersionId++) {

		try {

			if (e != NULL) {
				GS_RETHROW_USER_OR_SYSTEM(*e, "");
			}

			bool useCache =
					(!context_.isRetryStatement()
							&& currentVersionId == 0);
			resultSet_->resetSchema();

			SQLPreparedPlan::ValueTypeList
					parameterTypeList(alloc);

			if (!request.isAutoCommit_) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED,
						"Self commit mode is not supported");
			}	

			if (request.transactionStarted_) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED,
						"Transaction is not supported");
			}

			if (isPreaparedRetry) {
				request.bindParamSet_.clear();
				restoreBindInfo(request);
			}

			SQLTableInfoList *tableInfoList = NULL;
			util::Set<util::String> tableSet(alloc);
			util::Set<util::String> viewSet(alloc);
			
			TupleValue::VarContext varCxt;
			varCxt.setStackAllocator(&alloc);
			varCxt.setVarAllocator(&localVarAlloc_);
			varCxt.setGroup(&group_);
			TupleValue::VarContext::Scope varScope(varCxt);

			SQLExpandViewContext cxt;
			setupViewContext(
					cxt,
					ec,
					connectionControl,
					tableSet,
					viewSet,
					liveNodeIdList,
					useCache,
					(currentVersionId == 0));

			if (request.requestType_ == REQUEST_TYPE_PREPARE
					|| prepareBinded) {

				TRACE_SQL_START(
						clientId_, jobManager_->getHostName(), "PARSE");
				
				tableInfoList
						= ALLOC_NEW(alloc) SQLTableInfoList(alloc);
				tableInfoList->setDefaultDBName(
						context_.getDBName());

				cxt.tableInfoList_ = tableInfoList;
				if (parsedInfo.getCommandNum() == 0
						&& !parse(cxt, request, 0, 0, 0)) {

					SQLReplyContext replyCxt;
					replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
					replyClient(replyCxt);
					
					if (analyedQueryInfo_.isPragmaStatement()) {
						executionManager_->remove(ec, clientId_);
					}
					return;
				}
				TRACE_SQL_END(
						clientId_, jobManager_->getHostName(), "PARSE");

				getContext().getConnectionOption<
						ConnectionOption>().checkSelect(parsedInfo);

				checkClientResponse();
				if (!isBatch() && fastInsert(
						ec, request1.bindParamSet_.getParamList(batchCurrentPos_), useCache)) {
					return;
				}
				tableInfoList = generateTableInfo(cxt);

				if (request.requestType_ == REQUEST_TYPE_PREPARE ) {

					TRACE_SQL_START(
							clientId_, jobManager_->getHostName(), "COMPILE");
					
					compile(
							alloc,
							connectionControl,
							parameterTypeList,
							tableInfoList,
							request1.bindParamSet_.getParamList(batchCurrentPos_),
							varCxt);

					TRACE_SQL_END(
							clientId_, jobManager_->getHostName(), "COMPILE");

					SQLReplyContext replyCxt;
					replyCxt.set(
							&ec, &parameterTypeList, UNDEF_JOB_VERSIONID, NULL);
					replyClient(replyCxt);

					return;
				}
			}

			checkClientResponse();

			if (!isBatch() && fastInsert(
				ec, request1.bindParamSet_.getParamList(batchCurrentPos_), useCache)) {
				return;
			}

			if (tableInfoList == NULL) {
				
				tableInfoList = ALLOC_NEW(alloc) SQLTableInfoList(alloc);
				tableInfoList->setDefaultDBName(context_.getDBName());
				cxt.tableInfoList_ = tableInfoList;
				
				tableInfoList = generateTableInfo(cxt);
			}
			parameterTypeList.clear();

			TRACE_SQL_START(
					clientId_, jobManager_->getHostName(), "COMPILE");

			compile(
					alloc,
					connectionControl,
					parameterTypeList,
					tableInfoList,
					request1.bindParamSet_.getParamList(batchCurrentPos_),
					varCxt);
			setPreparedOption(request);
			
			TRACE_SQL_END(clientId_, jobManager_->getHostName(), "COMPILE");

			if (analyedQueryInfo_.isExplain()) {
				SQLReplyContext replyCxt;
				replyCxt.setExplain(&ec);
			
				replyClient(replyCxt);
				
				return;
			}
			
			TRACE_SQL_START(
					clientId_, jobManager_->getHostName(), "ASSIGN");
			
			JobInfo jobInfo(alloc);
			generateJobInfo(
					alloc,
					tableInfoList,
					executionStatus_.preparedPlan_,
					&jobInfo,
					ec.getEngine().getMonotonicTime(),
					liveNodeIdList,
					maxNodeNum);

			TRACE_SQL_END(
					clientId_, jobManager_->getHostName(), "ASSIGN");

			JobId jobId;
			generateJobId(jobId, request.stmtId_);

			jobManager_->beginJob(
					ec, jobId, &jobInfo, delayTime);
			return;
		}
		catch (std::exception &e1) {

			if (context_.isPreparedStatement()) {
				isPreaparedRetry = true;
			}
			
			if (!checkJobVersionId(
					static_cast<uint8_t>(currentVersionId), false, responseJobId)) {
				return;
			}

			if (handleError(
					ec, e1, currentVersionId, delayTime)) {
				return;
			}
		}
		isException = false;
		e = NULL;
	}
	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_EXECUTION_RETRY_QUERY_LIMIT,
			"Sql stetement execution is reached retry max count="
			<< static_cast<int32_t>(MAX_JOB_VERSIONID));
}

void SQLExecution::generateJobInfo(
		util::StackAllocator &alloc,
		SQLTableInfoList *tableInfoList,
		SQLPreparedPlan *pPlan,
		JobInfo *jobInfo,
		EventMonotonicTime emNow,
		util::Vector<NodeId> &liveNodeIdList,
		int32_t maxNodeNum) {

	try {
		PartitionTable *pt
				= resourceSet_->getPartitionTable();

		jobInfo->syncContext_ = &context_.getSyncContext();
		jobInfo->option_.plan_ = executionStatus_.preparedPlan_;
		
		if (isTimeSeriesIncluded()) {
			jobInfo->containerType_ = TIME_SERIES_CONTAINER;
		}
		
		jobInfo->isSQL_ = !isNotSelect();
		jobInfo->isExplainAnalyze_ = analyedQueryInfo_.isExplainAnalyze();
		
		size_t pos = 0;
		util::Vector<NodeId> rootAssignTaskList(
				pPlan->nodeList_.size(), 0, alloc);

		for (pos = 0; pos < pPlan->nodeList_.size(); pos++) {
			jobInfo->createTaskInfo(pPlan, pos);
		}

		for (pos = 0; pos <  pPlan->nodeList_.size(); pos++) {
			TaskInfo *taskInfo = jobInfo->taskInfoList_[pos];

			for (TaskInfo::ResourceList::iterator
					it = taskInfo->inputList_.begin();
					it != taskInfo->inputList_.end(); ++it) {
		
				jobInfo->taskInfoList_[*it]->outputList_.push_back(
						static_cast<uint32_t>(pos));
			}

			if (taskInfo->isDml_) {
				
				SQLProcessor::planToInputInfoForDml(
						*pPlan,
						static_cast<uint32_t>(pos),
						*tableInfoList,
						taskInfo->inputTypeList_);
				rootAssignTaskList[pos] = 1;
			}
			else {
				
				if (taskInfo->sqlType_ == SQLType::EXEC_DDL) {
					rootAssignTaskList[pos] = 1;
				}

				SQLProcessor::planToInputInfo(
						*pPlan,
						static_cast<uint32_t>(pos),
						*tableInfoList,
						taskInfo->inputTypeList_);
			}

			SQLProcessor::planToOutputInfo(
					*pPlan,
					static_cast<uint32_t>(pos),
					taskInfo->outColumnTypeList_);

			if (taskInfo->sqlType_
					== SQLType::EXEC_RESULT) {

				jobInfo->resultTaskId_
						= static_cast<int32_t>(pos);

				jobInfo->pId_ = getConnectedPId();
				rootAssignTaskList[pos] = 1;

				util::Vector<util::String> columnNameList(alloc);
				resultSet_->getColumnNameList(
						alloc, columnNameList);
				
				if (columnNameList.size() == 0) {
					pPlan->nodeList_[pos].getColumnNameList(
							columnNameList);
					resultSet_->setColumnNameList(columnNameList);
				}

				resultSet_->setColumnTypeList(
						taskInfo->inputTypeList_[0]);
			}
		}

		
		int32_t nodeNum = static_cast<int32_t>(
				liveNodeIdList.size());

		if (nodeNum == 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
					"Invalid node assigned, active node is none");
		}

		util::XArray<NodeId> activeNodeListMap(alloc);
		activeNodeListMap.assign(
				static_cast<size_t>(maxNodeNum), -1);

		util::Vector<int32_t> assignNodes(
				nodeNum, 0, alloc);

		util::Vector<bool> checkNodes(
				nodeNum, false, alloc);

		if (nodeNum > 1) {
			for (size_t i = 0;i < liveNodeIdList.size(); i++) {
				activeNodeListMap[liveNodeIdList[i]]
						= static_cast<NodeId>(i);
			}

			TaskInfo *resultTask
					= jobInfo->taskInfoList_[jobInfo->resultTaskId_];
			assert(resultTask->inputList_.size() > 0);
			TaskInfo *targetTask
					= jobInfo->taskInfoList_[resultTask->inputList_[0]];

			if (targetTask->sqlType_ == SQLType::EXEC_SELECT) {
				
				rootAssignTaskList[targetTask->taskId_] = 1;
				if (targetTask->inputList_.size() > 0) {
					
					TaskInfo *unionTask
							= jobInfo->taskInfoList_[targetTask->inputList_[0]];
					
					if (unionTask->sqlType_ == SQLType::EXEC_UNION) {
						rootAssignTaskList[unionTask->taskId_] = 1;
					}
				}
			}
			else if (targetTask->sqlType_ == SQLType::EXEC_DDL
					|| targetTask->sqlType_ == SQLType::EXEC_UNION) {
				rootAssignTaskList[targetTask->taskId_] = 1;
			}

			else if (targetTask->isDml_) {
				assert(resultTask->inputList_.size() > 0);
				TaskInfo *currentTask
						= jobInfo->taskInfoList_[resultTask->inputList_[0]];
				
				if (currentTask->sqlType_ == SQLType::EXEC_SELECT) {
					rootAssignTaskList[currentTask->taskId_] = 1;
				
					if (currentTask->inputList_.size() > 0) {
						TaskInfo *unionTask
								= jobInfo->taskInfoList_[currentTask->inputList_[0]];

						if (unionTask->sqlType_ == SQLType::EXEC_UNION) {
							rootAssignTaskList[unionTask->taskId_] = 1;
						}
					}
				}
				else if (currentTask->sqlType_
						== SQLType::EXEC_UNION) {
					rootAssignTaskList[targetTask->taskId_] = 1;
				}
			}
		}
		if (nodeNum == 1) {
			
			bool forceAssign = false;
			if (pPlan->nodeList_.size() == 2) {
				forceAssign  = true;
			}

			for (size_t pos = 0;
					pos < pPlan->nodeList_.size(); pos++) {
				
				TaskInfo *taskInfo = jobInfo->taskInfoList_[pos];
				if (taskInfo->sqlType_ == SQLType::EXEC_RESULT 
						|| taskInfo->sqlType_ == SQLType::EXEC_DDL 
						|| taskInfo->isDml_) {
					
					taskInfo->loadBalance_ = getConnectedPId();
					jobInfo->pId_ = getConnectedPId();
					taskInfo->nodePos_ = 0;
				}
				else if (
						taskInfo->sqlType_ == SQLType::EXEC_SCAN) {
					
					taskInfo->type_
							= static_cast<uint8_t>(TRANSACTION_SERVICE);
					SQLPreparedPlan::Node &node = pPlan->nodeList_[pos];
					taskInfo->loadBalance_ = node.tableIdInfo_.partitionId_;
					
					if (node.tableIdInfo_.isNodeExpansion_) {
						taskInfo->type_ = static_cast<uint8_t>(SQL_SERVICE);
						taskInfo->nodePos_ = 0;
						continue;
					}

					taskInfo->nodePos_
							= pt->getNewSQLOwner(
									node.tableIdInfo_.partitionId_);
					
					if (taskInfo->nodePos_ != 0) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
								"Invalid node assign (pId="
								<< 	node.tableIdInfo_.partitionId_
								<< ", nodeId=" << taskInfo->nodePos_ << ")");
					}
				}
				else {
					taskInfo->nodePos_ = 0;
					if (forceAssign) {
						taskInfo->loadBalance_ = getConnectedPId();
					}
					else {
						taskInfo->loadBalance_ = -1;
					}
				}
			}
		}
		else {

			util::Vector<TaskInfo *> notAssignTaskList(alloc);

			for (size_t pos = 0;
					pos < pPlan->nodeList_.size(); pos++) {

				TaskInfo *taskInfo = jobInfo->taskInfoList_[pos];
				if (taskInfo->sqlType_ == SQLType::EXEC_RESULT
						|| taskInfo->isDml_
						|| taskInfo->sqlType_ == SQLType::EXEC_DDL) {

					taskInfo->loadBalance_ = getConnectedPId();
					taskInfo->nodePos_ = 0;
					jobInfo->pId_ = getConnectedPId();
				}
				else if (
						taskInfo->sqlType_ == SQLType::EXEC_SCAN) {

					taskInfo->type_
							= static_cast<uint8_t>(TRANSACTION_SERVICE);
					SQLPreparedPlan::Node &node = pPlan->nodeList_[pos];
					taskInfo->loadBalance_
							= node.tableIdInfo_.partitionId_;

					if (node.tableIdInfo_.isNodeExpansion_) {
						taskInfo->type_ = static_cast<uint8_t>(SQL_SERVICE);
						taskInfo->nodePos_
								= activeNodeListMap[liveNodeIdList[
										node.tableIdInfo_.subContainerId_]];
						continue;
					}

					NodeId ownerNodeId = UNDEF_NODEID;
					ownerNodeId = pt->getNewSQLOwner(
							node.tableIdInfo_.partitionId_);

					if (ownerNodeId == UNDEF_NODEID
							|| static_cast<int32_t>(
									activeNodeListMap.size()) <= ownerNodeId) {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
								"Invalid node assign (pId="
								<< node.tableIdInfo_.partitionId_
								<< ", nodeId=" << ownerNodeId
								<< ", nodeNum=" << nodeNum << ")");
					}

					taskInfo->nodePos_  = activeNodeListMap[ownerNodeId];
					if (taskInfo->nodePos_ == -1
							|| taskInfo->nodePos_ >= nodeNum) {
						
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
									"Invalid node assign (pId="
									<< node.tableIdInfo_.partitionId_
									<< ", nodeId=" << ownerNodeId
									<< ", nodePos=" << taskInfo->nodePos_
									<< ", nodeNum=" << nodeNum << ")");
					}
					checkNodes[taskInfo->nodePos_] = true;
				}
				else {
					if (rootAssignTaskList[pos] == 1) {
						taskInfo->nodePos_ = 0;
						taskInfo->loadBalance_ = -1;
					}
					else {
						notAssignTaskList.push_back(taskInfo);
					}
				}
			}
			for (size_t i = 0; i < notAssignTaskList.size(); i++) {

				TaskInfo *taskInfo = notAssignTaskList[i];
				if (taskInfo->inputList_.size() == 0) {
					int32_t min = INT32_MAX;
					size_t pos = 0;

					for (size_t j = 0; j < assignNodes.size(); j++) {
						if (min > assignNodes[j]) {
							min = assignNodes[j];
							pos = j;
						}
					}

					taskInfo->nodePos_ = static_cast<int32_t>(pos);

					if (taskInfo->nodePos_ == -1
							|| taskInfo->nodePos_ >= nodeNum) {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
								"Invalid node assign (nodePos="
								<< taskInfo->nodePos_ << ", nodeNum="
								<< nodeNum << ")");
					}

					assignNodes[taskInfo->nodePos_]++;
					checkNodes[taskInfo->nodePos_] = true;
				}
				else if (taskInfo->inputList_.size() == 1) {
					TaskInfo *taskInfo1
							= jobInfo->taskInfoList_[taskInfo->inputList_[0]];
					taskInfo->nodePos_ = taskInfo1->nodePos_;
					
					if (taskInfo->nodePos_ == -1 
							|| taskInfo->nodePos_ >= nodeNum) {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
								"Invalid node assign (nodePos="
								<< taskInfo->nodePos_ << ", nodeNum="
								<< nodeNum << ")");
					}

					assignNodes[taskInfo->nodePos_]++;
					checkNodes[taskInfo->nodePos_] = true;
				}
				else {
					int32_t nodePos
							= static_cast<int32_t>(
									executionManager_->incNodeCounter() % nodeNum);
					taskInfo->nodePos_ = nodePos;
					
					if (taskInfo->nodePos_ == -1
							|| taskInfo->nodePos_ >= nodeNum) {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
								"Invalid node assign (nodePos="
								<< taskInfo->nodePos_
								<< ", nodeNum=" << nodeNum << ")");
					}
					assignNodes[nodePos]++;
					checkNodes[nodePos] = true;
				}
				taskInfo->loadBalance_ = -1;
			}
		}

		for (size_t nodeId = 0;
				nodeId < liveNodeIdList.size(); nodeId++) {
			
			jobInfo->createAssignInfo(
					pt,
					liveNodeIdList[nodeId],
					(nodeNum == 1),
					jobManager_);
		}

		executionStatus_.setMaxRows(config_.maxRows_);
		
		jobInfo->storeMemoryAgingSwapRate_
				= context_.getStoreMemoryAgingSwapRate();
		jobInfo->timezone_ = context_.getTimezone();
		
		int64_t startTime = util::DateTime::now(false).getUnixTime();
		getContext().setStartTime(startTime);
		
		jobInfo->setup(
				config_.queryTimeout_,
				emNow,
				startTime, 
				isCurrentTimeRequired(),
				true);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Create job information failed, clientId=" << clientId_ 
			<< ", executionId=" << context_.getExecId()
			<< ", reason=" << GS_EXCEPTION_MESSAGE(e));
	}
}

void SQLExecution::fetch(
		EventContext &ec, RequestInfo &request) {

	try {
	
		setRequest(request);
		checkCancel();

		if (executionStatus_.execId_ != request.stmtId_) {
			
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_QUERY_EXECUTION_ID,
					"Invalid statement execution number, expected="
					<< request.stmtId_ 
					<< ", current=" << executionStatus_.execId_);
		}

		int32_t requestCommandNo = request.sqlCount_;
		if (requestCommandNo > 1) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_CLIENT_REQUST_PROTOCOL_ERROR,
					"Invalid statement command number, expected="
					<< requestCommandNo << ", current = 1");
		}

		JobId jobId;
		context_.getCurrentJobId(jobId);
		JobLatch latch(
				jobId, jobManager_->getResourceManager(), NULL);

		Job *job = latch.get();
		if (job) {
			job->fetch(ec, this);
		}
		else {
			try {
				GS_THROW_USER_ERROR(GS_ERROR_JOB_CANCELLED,
						"Cancel job, jobId=" << jobId << ", (fetch)");
			} 
			catch (std::exception &e) {
				replyError(
						ec, StatementHandler::TXN_STATEMENT_ERROR, e);

				if (context_.isPreparedStatement()) {
					executionManager_->remove(ec, clientId_);
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "Fetch failed, clientId= " << clientId_
				<< ", executionId=" << request.stmtId_ << ",reason="
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

bool SQLExecution::fetch(
		EventContext &ec,
		SQLFetchContext &cxt,
		JobExecutionId execId,
		uint8_t versionId) {

	try {
		checkCancel();
		checkJobVersionId(versionId, true, NULL);
		
		if (executionStatus_.responsed_) {
			return false;
		}
		
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		TupleValue::VarContext varCxt;
		varCxt.setStackAllocator(&alloc);
		varCxt.setVarAllocator(&localVarAlloc_);
		varCxt.setGroup(&group_);
		cxt.reader_->setVarContext(varCxt);
		
		if (isBatch()) {
			if (isBatchComplete()) {
				return fetchBatch(
							ec, &batchCountList_);
			}
			else {
				RequestInfo request(alloc, false);
				execute(ec, request, true, NULL, 0, NULL);
				return false;
			}
		}
		else 
		if (isNotSelect()) {
			return fetchNotSelect(
					ec, cxt.reader_, cxt.columnList_);
		}
		else {
			return fetchSelect(
					ec, cxt.reader_, cxt.columnList_,
							cxt.columnSize_, cxt.isCompleted_);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "Fetch failed, clientId=" << clientId_
				<< ", executionId=" << execId << ", reason="
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

void SQLExecution::setAnyTypeData(
		util::StackAllocator &alloc, const TupleValue &value) {

	switch (value.getType()) {

	case TupleList::TYPE_STRING: {
		resultSet_->setVariable(COLUMN_TYPE_STRING,
				reinterpret_cast<const char *>(
						value.varData()), value.varSize());
		break;
	}

	case TupleList::TYPE_BLOB: {
		util::XArray<uint8_t> blobs(alloc);
		generateBlobValue(value, blobs);
		resultSet_->setVariable(COLUMN_TYPE_BLOB,
				reinterpret_cast<const char *>(
						blobs.data()), blobs.size());
		break;
	}

	case TupleList::TYPE_BYTE: {
		resultSet_->setFixedValueWithPadding<int8_t>(
				COLUMN_TYPE_BYTE, value.fixedData());
		break;
	}

	case TupleList::TYPE_SHORT: {
		resultSet_->setFixedValueWithPadding<int16_t>(
				COLUMN_TYPE_SHORT, value.fixedData());
		break;
	}

	case TupleList::TYPE_INTEGER: {
		resultSet_->setFixedValueWithPadding<int32_t>(
				COLUMN_TYPE_INT, value.fixedData());
		break;
	}

	case TupleList::TYPE_LONG: {
		resultSet_->setFixedValue<int64_t>(
				COLUMN_TYPE_LONG, value.fixedData());
		break;
	}

	case TupleList::TYPE_FLOAT: {
		resultSet_->setFixedValueWithPadding<float>(
				COLUMN_TYPE_FLOAT, value.fixedData());
		break;
	}

	case TupleList::TYPE_NUMERIC:
	case TupleList::TYPE_DOUBLE: {
		resultSet_->setFixedValue<double>(
				COLUMN_TYPE_DOUBLE, value.fixedData());
		break;
	}

	case TupleList::TYPE_TIMESTAMP: {
		resultSet_->setFixedValue<int64_t>(
				COLUMN_TYPE_TIMESTAMP, value.fixedData());
		break;
	}

	case TupleList::TYPE_BOOL: {
		resultSet_->setFixedValueWithPadding<bool>(
				COLUMN_TYPE_BOOL, value.fixedData());
		break;
	}

	case TupleList::TYPE_NULL: {
		resultSet_->setFixedNull();
		break;
	}

	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_OPTYPE_UNSUPPORTED, "");
	}
}

void SQLExecution::cancel(JobExecutionId execId) {

	try {
		isCancelled_ = true;

		if (executionStatus_.execId_ > execId) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_QUERY_EXECUTION_ID,
					"Invalid statement execution number, expected="
					<< execId << ", min=" << executionStatus_.execId_);
		}
		else if (
				execId != MAX_STATEMENTID
				&& executionStatus_.execId_ < execId) {
			executionStatus_.pendingCancelId_ = execId;
		}
		else {
			executeCancel();
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, e, "");
	}
}

void SQLExecution::close(EventContext &ec, RequestInfo &request) {

	try {
		setRequest(request);
		executionStatus_.currentType_ = SQL_EXEC_CLOSE;
		SQLReplyContext replyCxt;
		replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		replyClient(replyCxt);
		executeCancel();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "Close SQL exectuion failed, reason = "
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

void SQLExecution::executeCancel() {

	GS_TRACE_INFO(
			DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
			"Execute cancel, clientId=" << clientId_);

	isCancelled_ = true;
	context_.syncContext_->cancel();
}

bool SQLExecution::replySuccessInternal(
		EventContext &ec,
		int32_t type,
		uint8_t versionId,
		ReplyOption &option,
		JobId *responseJobId) {

	util::StackAllocator &alloc = ec.getAllocator();

	bool responsed = executionStatus_.responsed_;
	if (executionStatus_.currentType_
			!= SQL_EXEC_CLOSE && responsed) {
		
		GS_TRACE_INFO(
				DISTRIBUTED_FRAMEWORK,
				GS_TRACE_SQL_INTERNAL_DEBUG,
				"Call reply success, but not executed because of already responded,"
				"clientId=" << clientId_);
		return false;
	}

	if (!checkJobVersionId(
			versionId, false, responseJobId)) {
		return false;
	}
	setResponse();

	Event replyEv(
			ec, SQL_EXECUTE_QUERY, getConnectedPId());
	replyEv.setPartitionIdSpecified(false);
	EventByteOutStream out = replyEv.getOutStream();

	switch (type) {
		
		case SQL_REPLY_SUCCESS: {

			ValueTypeList *typeList = option.typeList_;
			if (typeList && typeList->size() == 0) {
				response_.existsResult_ = false;
			}
			encodeSuccess(alloc, out, typeList);
		}
		break;
		
		case SQL_REPLY_SUCCESS_EXPLAIN: {

			encodeSuccessExplain(alloc, out);
		}
		break;
		
		case SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE: {
		
			Job *job = option.job_;
			encodeSuccessExplainAnalyze(alloc, out, job);

			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED, "");
	}
	sendClient(ec, replyEv);

	if (context_.isPreparedStatement()) {
		executionStatus_.reset();

		if (!response_.isContinue_) {
			response_.reset();
			resetJobVersion();
		}
	}
	return true;
}

bool SQLExecution::replySuccess(
		EventContext &ec,
		ValueTypeList *typeList,
		uint8_t versionId,
		JobId *responseJobId) {

	ReplyOption option;
	option.typeList_ = typeList;

	return replySuccessInternal(
			ec, 
			SQL_REPLY_SUCCESS,
			versionId,
			option,
			responseJobId);
}

bool SQLExecution::replySuccessExplain(
		EventContext &ec,
		uint8_t versionId, 
		JobId *responseJobId) {

	ReplyOption option;
	bool retVal = replySuccessInternal(
			ec,
			SQL_REPLY_SUCCESS_EXPLAIN,
			versionId,
			option,
			responseJobId);

	if (!context_.isPreparedStatement()) {
		executionManager_->remove(ec, clientId_);
	}

	return retVal;
}

void SQLExecution::encodeSuccess(
		util::StackAllocator &alloc, 
		EventByteOutStream &out,
		ValueTypeList *typeList) {

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;

	StatementHandler::encodeBooleanData(
			out, response_.transactionStarted_);

	StatementHandler::encodeBooleanData(
			out, response_.isAutoCommit_);
	
	StatementHandler::encodeIntData<int32_t>(
			out, static_cast<int32_t>(
					response_.resultCount_));

	if (response_.resultCount_ > 0) {

		StatementHandler::encodeIntData<int32_t>(
				out, static_cast<int32_t>(
						response_.updateCount_));

		StatementHandler::encodeIntData<int32_t>(
				out, response_.parameterCount_);

		StatementHandler::encodeBooleanData(
				out, response_.existsResult_);

		if (response_.existsResult_) {

			StatementHandler::encodeBooleanData(
					out, response_.isContinue_);
			out << static_cast<int32_t>(
					response_.resultSet_->getRowCount());
			if (!typeList) {
				response_.resultSet_->exportSchema(alloc, out);
			}
			else {
				size_t placeHolderCount = typeList->size();
				if (placeHolderCount > 0) {
					
					const size_t startPos = out.base().position();
					StatementHandler::encodeIntData<uint32_t>(out, 0);
					StatementHandler::encodeIntData<uint32_t>(
							out, static_cast<uint32_t>(placeHolderCount));

					uint32_t placeHolderPos;
					
					for (placeHolderPos = 0;
							placeHolderPos < placeHolderCount; placeHolderPos++) {

						const util::String emptyColumnName("", alloc);
						StatementHandler::encodeStringData(
								out, emptyColumnName);
						
						uint8_t type = SQLUtils::convertTupleTypeToNoSQLType(
								(*typeList)[placeHolderPos]);
						out << type;
						
						const bool isArrayVal = false;
						StatementHandler::encodeBooleanData(out, isArrayVal);
					}

					const int16_t keyCount = 0;
					StatementHandler::encodeIntData(out, keyCount);

					for (placeHolderPos = 0;
							placeHolderPos < placeHolderCount; placeHolderPos++) {
						
						const util::String emptyColumnName("", alloc);
						StatementHandler::encodeStringData(
								out, emptyColumnName);
					}

					const size_t endPos = out.base().position();
					out.base().position(startPos);
					
					StatementHandler::encodeIntData<uint32_t>(
						out, static_cast<uint32_t>(
								endPos - startPos - sizeof(uint32_t)));
					out.base().position(endPos);
				}
			}

			const size_t startPos = out.base().position();
			StatementHandler::encodeIntData<uint32_t>(out, 0);
			const uint64_t varDataBaseOffset = 0;
			out << varDataBaseOffset;
			
			if (response_.resultSet_->getRowCount() != 0) {
				response_.resultSet_->exportData(out);
			}
			
			const size_t endPos = out.base().position();
			out.base().position(startPos);

			StatementHandler::encodeIntData<uint32_t>(
					out, static_cast<uint32_t>(
							endPos - startPos - sizeof(uint32_t)));

			out.base().position(endPos);
		}
	}
	
	SQLRequestHandler::encodeEnvList(
			out,
			alloc,
			*executionManager_,
			context_.getClientNd());
}

void SQLExecution::encodeSuccessExplain(
		util::StackAllocator &alloc,
		EventByteOutStream &out) {

	SQLResultSet resultSet(globalVarAlloc_);
	util::Vector<TupleList::TupleColumnType> typeList(alloc);
	typeList.push_back(TupleList::TYPE_STRING);
	
	util::Vector<util::String> columnNameList(alloc);
	resultSet.getColumnNameList(alloc, columnNameList);
	columnNameList.push_back("");
	
	resultSet.setColumnNameList(columnNameList);
	resultSet.setColumnTypeList(typeList);
	resultSet.init(alloc);
	
	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);
	varCxt.setVarAllocator(&localVarAlloc_);
	varCxt.setGroup(&group_);
	
	SQLPreparedPlan *plan = executionStatus_.preparedPlan_;
	
	for (size_t planId = 0;
			planId < plan->nodeList_.size(); planId++) {

		resultSet.next(alloc);
		picojson::value jsonOutValue;
		JsonUtils::OutStream out(jsonOutValue);
		
		TupleValue::coder(
				util::ObjectCoder(), &varCxt).encode(
						out, (*plan).nodeList_[planId]);

		util::String tmpStr(alloc);
		tmpStr =  jsonOutValue.serialize().c_str();
		resultSet.setVarFieldValue(
				reinterpret_cast<const char *>(tmpStr.data()),
				static_cast<uint32_t>(tmpStr.size()));
	}

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;
	StatementHandler::encodeBooleanData(
			out, response_.transactionStarted_);
	StatementHandler::encodeBooleanData(
			out, response_.isAutoCommit_);
	
	out << static_cast<int32_t>(response_.resultCount_);
	out << static_cast<int32_t>(0);
	out << static_cast<int32_t>(0);
	
	StatementHandler::encodeBooleanData(out, true);
	StatementHandler::encodeBooleanData(out, false);
	out << static_cast<int32_t>(resultSet.getRowCount());
	resultSet.exportSchema(alloc, out);
	
	const size_t startPos = out.base().position();
	out << static_cast<uint32_t>(0);
	
	const uint64_t varDataBaseOffset = 0;
	out << varDataBaseOffset;
	resultSet.exportData(out);
	
	const size_t endPos = out.base().position();
	
	out.base().position(startPos);
	out << static_cast<uint32_t>(
			endPos - startPos - sizeof(uint32_t));
	out.base().position(endPos);
}

void SQLExecution::encodeSuccessExplainAnalyze(
		util::StackAllocator &alloc,
		EventByteOutStream &out,
		Job *job) {

	SQLResultSet resultSet(globalVarAlloc_);
	util::Vector<TupleList::TupleColumnType>
			typeList(alloc);
	typeList.push_back(TupleList::TYPE_STRING);
	
	util::Vector<util::String> columnNameList(alloc);
	resultSet.getColumnNameList(
			alloc, columnNameList);
	
	columnNameList.push_back("");
	resultSet.setColumnNameList(columnNameList);
	resultSet.setColumnTypeList(typeList);
	resultSet.init(alloc);

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);
	varCxt.setVarAllocator(&localVarAlloc_);
	varCxt.setGroup(&group_);

	SQLPreparedPlan *plan = NULL;
	restorePlanInfo(plan, alloc, varCxt);
	cleanupSerializedData();
	
	if (plan) {
		for (size_t planId = 0;
				planId < plan->nodeList_.size(); planId++) {
			
			resultSet.next(alloc);
			picojson::value jsonPlanOutValue;
			JsonUtils::OutStream planOut(jsonPlanOutValue);

			TaskProfiler profile;
			job->getProfiler(
					alloc, profile, planId);
			SQLPreparedPlan::Node &node
					= (*plan).nodeList_[planId];
			TupleValue::VarContext::Scope varScope(varCxt);

			try {
				TupleValue::coder(
						util::ObjectCoder(), &varCxt).encode(
								planOut, node);
			}
			catch (std::exception &) {
			}
			{
				util::String tmpStr(alloc);
				tmpStr =  jsonPlanOutValue.serialize().c_str();
				
				size_t currentSize = tmpStr.size();
				tmpStr.erase(currentSize - 1, 1);
				tmpStr += ",\"profile\":";
				
				picojson::value jsonProfileOutValue;
				JsonUtils::OutStream profileOut(jsonProfileOutValue);
				
				SQLProcessor::Profiler::makeProfilerCoder(
						alloc,
						util::ObjectCoder()).encode(
								profileOut, profile);
				
				tmpStr +=  jsonProfileOutValue.serialize().c_str();
				tmpStr += "}";

				resultSet.setVarFieldValue(
						reinterpret_cast<const char *>(tmpStr.data()),
						static_cast<uint32_t>(tmpStr.size()));
			}

			ALLOC_DELETE(alloc, profile.rows_);
			ALLOC_DELETE(alloc, profile.address_);
		}
	}
	executionStatus_.preparedPlan_= NULL;

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;
	
	StatementHandler::encodeBooleanData(
			out, response_.transactionStarted_);

	StatementHandler::encodeBooleanData(
			out, response_.isAutoCommit_);

	out << static_cast<int32_t>(
			response_.resultCount_);
	out << static_cast<int32_t>(0);
	out << static_cast<int32_t>(0);
	
	StatementHandler::encodeBooleanData(out, true);
	StatementHandler::encodeBooleanData(out, false);
	
	out << static_cast<int32_t>(
			resultSet.getRowCount());
	resultSet.exportSchema(alloc, out);
	
	const size_t startPos = out.base().position();
	out << static_cast<uint32_t>(0);
	
	const uint64_t varDataBaseOffset = 0;
	out << varDataBaseOffset;
	resultSet.exportData(out);
	
	const size_t endPos = out.base().position();
	
	out.base().position(startPos);
	out << static_cast<uint32_t>(
			endPos - startPos - sizeof(uint32_t));
	
	out.base().position(endPos);
}

bool SQLExecution::replySuccessExplainAnalyze(
		EventContext &ec,
		Job *job,
		uint8_t versionId,
		JobId *responseJobId) {

	ReplyOption option;
	option.job_ = job;
	
	bool retVal = replySuccessInternal(
			ec,
			SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE,
			versionId,
			option,
			responseJobId);

	if (!context_.isPreparedStatement()) {
		executionManager_->remove(ec, clientId_);
	}

	return retVal;
}


void SQLExecution::sendClient(
		EventContext &ec, Event &ev) {
	
	ec.getEngine().send(ev, context_.getClientNd());
	executionStatus_.responsed_ = true;
}

bool SQLExecution::replyError(
		EventContext &ec,
		StatementHandler::StatementExecStatus status,
		std::exception &e) {

	bool responsed = executionStatus_.responsed_;

	if (executionStatus_.currentType_
			!= SQL_EXEC_CLOSE && responsed) {

		GS_TRACE_INFO(
				DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
				"Call reply success, "
				"but not executed because of already responded,"
				"clientId=" << clientId_);
		
		return false;
	}

	UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	
	Event replyEv(
			ec, SQL_EXECUTE_QUERY, getConnectedPId());

	StatementHandler::setErrorReply(
			replyEv,
			response_.stmtId_,
			status,
			e,
			context_.getClientNd());

	sendClient(ec, replyEv);
	
	return true;
}

void SQLExecution::setResponse() {

	if (isNotSelect()) {
		response_.existsResult_ = false;
		return;
	}
	switch (executionStatus_.requestType_) {
	case REQUEST_TYPE_EXECUTE:
	case REQUEST_TYPE_QUERY:
	case REQUEST_TYPE_FETCH:
	case REQUEST_TYPE_PRAGMA:
		response_.existsResult_ = true;
		break;
	case REQUEST_TYPE_UPDATE:
		response_.existsResult_ = true;
		break;
	case REQUEST_TYPE_PREPARE :
		response_.existsResult_ = true;
		break;
	case REQUEST_TYPE_CLOSE:
		response_.existsResult_ = false;
	default:
		break;
	}
}

NoSQLStore *SQLExecution::getNoSQLStore() {
	
	return db_->getNoSQLStore(
			context_.getDBId(), context_.getDBName());
}

SQLExecution::ErrorFormatter::ErrorFormatter(
		SQLExecution &execution, const std::exception &cause) :
				execution_(execution),
				cause_(cause) {
}

void SQLExecution::ErrorFormatter::format(
		std::ostream &os) const {

	os << GS_EXCEPTION_MESSAGE(cause_);
	os << " on ";
	bool hasQuery = false;

	switch (
			execution_.executionStatus_.requestType_) {
	
	case REQUEST_TYPE_EXECUTE:
		os << "executing statement";
		hasQuery = true;
		break;
	
	case REQUEST_TYPE_PREPARE :
		os << "preparing statement";
		hasQuery = true;
		break;
	
	case REQUEST_TYPE_QUERY:
		os << "executing query";
		hasQuery = true;
		break;
	
	case REQUEST_TYPE_UPDATE:
		os << "updating";
		hasQuery = true;
		break;
	
	case REQUEST_TYPE_PRAGMA:
		os << "executing statement(metadata)";
		hasQuery = true;
		break;
	
	case REQUEST_TYPE_FETCH:
		os << "fetching";
		break;
	
	case REQUEST_TYPE_CLOSE:
		os << "closing";
		break;
	
	case REQUEST_TYPE_CANCEL:
		os << "cancelling";
		break;
	
	default:
		break;
	}
	if (execution_.getContext().isPreparedStatement()) {
		os << " (prepared) ";
	}
	if (hasQuery) {
		os << " (sql=\"" << execution_.getContext().getQuery() << "\")";
	}
	os << " (db=\'" << execution_.getContext().getDBName() << "\')";
	os << " (user=\'" << execution_.getContext().getUserName() << "\')";

	const char *appName = execution_.getContext().getApplicationName();
	if (appName && strlen(appName) > 0) {
		os << " (appName=\'" << appName << "\')";
	}
	os << " (clientId=\'" << execution_.getContext().getId()<< "\')";
	os << " (clientNd=\'" << execution_.getContext().getClientNd()<< "\')";
}

std::ostream& operator<<(
		std::ostream &os,
		const SQLExecution::ErrorFormatter &formatter) {

	formatter.format(os);
	return os;
}

void SQLExecution::checkClientResponse() {

	if (analyedQueryInfo_.isExplain()
			|| analyedQueryInfo_.isExplainAnalyze()) {
		return;
	}

	if (!isNotSelect()) {

		if (executionStatus_.requestType_
				== REQUEST_TYPE_UPDATE) {
		
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_STATEMENT_CATEGORY_UNMATCHED,
					"ExecuteUpdate() can not use for query "
					"with result set, use executeQuery() or execute()");
		}
	}
	else {
		if (executionStatus_.requestType_
				== REQUEST_TYPE_QUERY) {
					
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_STATEMENT_CATEGORY_UNMATCHED,
				"ExecuteQuery() can not use for query "
				"without result set, use executeUpdate() or execute()");
		}
	}
}

TupleValue SQLExecution::getTupleValue(
		TupleValue::VarContext &varCxt,
		SyntaxTree::Expr *targetExpr,
		TupleList::TupleColumnType type,
		util::Vector<BindParam*> &bindParamInfos) {

	const bool implicit = true;
	if (targetExpr->op_ == SQLType::EXPR_CONSTANT) {
		return SQLCompiler::ProcessorUtils::convertType(
				varCxt, targetExpr->value_, type, implicit);
	}
	else {
		return SQLCompiler::ProcessorUtils::convertType(
				varCxt, bindParamInfos[
						targetExpr->placeHolderPos_]->value_,
				type, implicit);
	}
}

void SQLExecution::cancel(const Event::Source &eventSource) {

	try {
		JobId jobId;
		isCancelled_ = true;
		context_.getCurrentJobId(jobId);
		CancelOption option;
		jobManager_->cancel(eventSource, jobId, option);
	}
	catch (std::exception &e) {

		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
		context_.syncContext_->cancel();
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	resetCurrentJobInfo();
	context_.syncContext_->cancel();
}

SQLTableInfo* SQLExecution::decodeTableInfo(
		util::StackAllocator &alloc,
		util::ArrayByteInStream &in) {

	SQLTableInfo *info = ALLOC_NEW(alloc) SQLTableInfo(alloc);
	SQLTableInfo::PartitioningInfo *partitioning =
			ALLOC_NEW(alloc) SQLTableInfo::PartitioningInfo(alloc);
	info->partitioning_ = partitioning;

	int32_t distributedPolicy;
	in >> distributedPolicy;

	if (distributedPolicy != 1
			&& distributedPolicy != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}

	int32_t partitioningType;
	in >> partitioningType;

	switch (partitioningType) {
	
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:

		partitioning->partitioningType_
				= static_cast<uint8_t>(partitioningType);
	break;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_DECODE_FAILED, "");
	}

	int32_t distributedMethod;
	in >> distributedMethod;

	if (SyntaxTree::isInlcludeHashPartitioningType(
			partitioningType)) {

		int32_t partitioningCount;
		in >> partitioningCount;
		if (partitioningCount <= 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}
		partitioning->partitioningCount_ =
				static_cast<uint32_t>(partitioningCount);
	}

	int8_t partitionColumnType;
	if (SyntaxTree::isRangePartitioningType(
			partitioningType)) {

		in >> partitionColumnType;
		int64_t intervalValue;
		
		switch (partitionColumnType) {
		
		case COLUMN_TYPE_BYTE: {
			int8_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		
		case COLUMN_TYPE_SHORT: {
			int16_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		
		case COLUMN_TYPE_INT: {
			int32_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		
		case COLUMN_TYPE_LONG: {
			int64_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		
		case COLUMN_TYPE_TIMESTAMP: {
			int64_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}

		if (intervalValue <= 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}

		partitioning->intervalValue_ = intervalValue;

		int8_t unitValue;
		in >> unitValue;
	}
	else {
		partitionColumnType = -1;
	}

	int64_t partitioningVersionId;
	in >> partitioningVersionId;

	int32_t count;
	in >> count;
	for (int32_t i = 0; i < count; i++) {
		int64_t nodeAffinity;
		in >> nodeAffinity;
		if (nodeAffinity < 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}
		partitioning->nodeAffinityList_.
				push_back(nodeAffinity);
	}

	for (size_t i = 0; i < 2; i++) {

		int32_t count;
		in >> count;
		if (count != 1) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}

		int32_t columnId;
		in >> columnId;
		if (columnId < 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}

		(i == 0 ?
				partitioning->partitioningColumnId_ :
						partitioning->subPartitioningColumnId_)
				= columnId;

		if (partitioningType
				!= SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
			break;
		}
	}

	for (size_t i = 0; i < 2; i++) {
		
		int32_t count;
		in >> count;
		
		if (count < 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}

		for (int32_t j = 0; j < count; j++) {
	
			int64_t interval;
			int64_t intervalCount;
			in >> interval;
			in >> intervalCount;

			if (interval < 0 || intervalCount <= 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_DECODE_FAILED, "");
			}

			if (i == 0) {
				partitioning->availableList_.push_back(
						std::make_pair(interval, intervalCount));
			}
		}
	}

	int8_t currentStatus;
	in >> currentStatus;

	if (currentStatus == PARTITION_STATUS_CREATE_START ||
			currentStatus == PARTITION_STATUS_DROP_START) {
	
		int64_t nodeAffinity;
		in >> nodeAffinity;
		
		if (nodeAffinity < 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}
	}
	else if (currentStatus
			== INDEX_STATUS_CREATE_START ||
			currentStatus == INDEX_STATUS_DROP_START) {

		IndexInfo indexInfo(alloc);
		StatementHandler::decodeIndexInfo(in, indexInfo);
	}
	else if (
			currentStatus != PARTITION_STATUS_NONE) {

		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_DECODE_FAILED, "");
	}

	if (distributedPolicy >= 2) {

		int64_t currentTime;
		int32_t expirationTime;
		int8_t expirationTimeUnit;

		in >> currentTime;
		in >> expirationTime;
		in >> expirationTimeUnit;

		if (currentTime < 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}

		if (expirationTime > 0) {
			if (partitionColumnType != COLUMN_TYPE_TIMESTAMP) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_DECODE_FAILED, "");
			}
		}
		else {
			if (expirationTime < -1) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_DECODE_FAILED, "");
			}
		}

		switch (expirationTimeUnit) {
		case TIME_UNIT_DAY:
		case TIME_UNIT_HOUR:
		case TIME_UNIT_MINUTE:
		case TIME_UNIT_SECOND:
		case TIME_UNIT_MILLISECOND:
			break;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DECODE_FAILED, "");
		}
	}

	return info;
}

void SQLExecution::applyClusterPartitionCount(
		SQLTableInfo &tableInfo,
		uint32_t clusterPartitionCount) {

	if (tableInfo.partitioning_ != NULL) {
		tableInfo.partitioning_->clusterPartitionCount_
				= clusterPartitionCount;
	}
}

bool SQLExecution::checkJobVersionId(
		uint8_t versionId,
		bool isFetch,
		JobId *jobId) {

	if (jobId) {
		JobId currentJobId;
		context_.getCurrentJobId(currentJobId);
		if (jobId->clientId_ != currentJobId.clientId_) {
			return false;
		}
	}

	uint8_t currentVersionId
			= context_.getCurrentJobVersionId();
	
	if (versionId != UNDEF_JOB_VERSIONID
			&& versionId != currentVersionId) {

			if (isFetch) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_EXECUTION_INVALID_FETCH_STATEMENT,
						"Current fetch statement is changed, clientId="
						<< clientId_
						<< ", expected versionId="
						<< static_cast<int32_t>(currentVersionId)
						<< " current versionId="
						<< static_cast<int32_t>(versionId));
			}
			else {
				GS_TRACE_INFO(
						DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
						"Already internal execution, clientId="
						<< clientId_ << ", expected versionId=" 
						<< static_cast<int32_t>(currentVersionId) 
						<< ", current versionId="
						<< static_cast<int32_t>(versionId));
				return false;
			}
		}
		return true;
}

void SQLExecution::compile(
		util::StackAllocator &alloc,
		SQLConnectionEnvironment  &connectionControl,
		ValueTypeList &parameterTypeList,
		SQLTableInfoList *tableInfoList, 
		util::Vector<BindParam*> &bindParamInfos,
		TupleValue::VarContext &varCxt) {

	SQLPreparedPlan *plan;
	SQLParsedInfo &parsedInfo = getParsedInfo();
	SQLCompiler::CompileOption option;
	option.setTimeZone(getContext().getTimezone());
	
	SQLCompiler compiler(varCxt, &option);
	compiler.setMetaDataQueryFlag(
			executionStatus_.requestType_ == REQUEST_TYPE_PRAGMA);
	compiler.setExplainType(parsedInfo.explainType_);
	compiler.setInputSql(parsedInfo.inputSql_);

	if (parsedInfo.placeHolderCount_ > 0) {
		compiler.setParameterTypeList(&parameterTypeList);
	}

	plan = ALLOC_NEW(alloc) SQLPreparedPlan(alloc);
	assert(parsedInfo.syntaxTreeList_.size() == 1);
	plan->setUpHintInfo(
			parsedInfo.syntaxTreeList_[0]->hintList_,
			*tableInfoList);
	compiler.setTableInfoList(tableInfoList);
	plan->parameterList_.clear();
	
	for (size_t bindPos = 0;
			bindPos < bindParamInfos.size(); bindPos++) {
		plan->parameterList_.push_back(
				bindParamInfos[bindPos]->value_);
	}

	compiler.compile(
			*parsedInfo.syntaxTreeList_[0],
			*plan,
			&connectionControl);

	executionStatus_.preparedPlan_ = plan;
	response_.parameterCount_
			= parsedInfo.placeHolderCount_;

	if (bindParamInfos.size() > 0) {
		compiler.bindParameterTypes(*plan);
	}
	
	if (analyedQueryInfo_.isExplainAnalyze()) {
		util::XArray<uint8_t> binary(alloc);
		util::XArrayOutStream<> arrayOut(binary);
		util::ByteStream< util::XArrayOutStream<> > out(arrayOut);
		TupleValue::coder(
				util::ObjectCoder(), NULL).encode(out, plan);
		
		copySerializedData(binary);
	}
}

void SQLExecution::cleanupPrevJob() {

	if (context_.isPreparedStatement()) {
		JobId currentJobId;
		context_.getCurrentJobId(currentJobId);
		jobManager_->remove(currentJobId);
	}
}

void SQLExecution::cancelPrevJob(EventContext &ec) {

	JobId currentJobId;
	context_.getCurrentJobId(currentJobId);
	jobManager_->cancel(ec, currentJobId, false);
}

bool SQLExecution::fastInsert(
		EventContext &ec,
		util::Vector<BindParam*> &bindParamInfos,
		bool useCache) {

	if (isAnalyzed() && !isFastInserted()) {
		return false;
	}

	if (executionStatus_.requestType_
			== REQUEST_TYPE_PREPARE ) {
		return false;
	}

	if (!isAnalyzed()) {
		if (!checkFastInsert(bindParamInfos)) {
			return false;
		}
	}

	return executeFastInsert(
			ec, bindParamInfos, useCache);
}

bool SQLExecution::parseViewSelect(
		SQLExpandViewContext &cxt,
		SQLParsedInfo &viewParsedInfo,
		uint32_t viewDepth,
		int64_t viewNsId,
		int64_t maxViewNsId,
		util::String &viewSelectString) {

	GenSyntaxTree genSyntaxTree(executionStackAlloc_);
	SQLParsedInfo *orgParsedInfo = cxt.parsedInfo_;
	cxt.parsedInfo_ = &viewParsedInfo;

	genSyntaxTree.parseAll(
			&cxt,
			this,
			viewSelectString,
			viewDepth,
			viewNsId,
			maxViewNsId,
			true);

	cxt.parsedInfo_ = orgParsedInfo;
	
	if (viewParsedInfo.getCommandNum() > 1) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"Multiple SQL statement is not supported");
	}

	if (viewParsedInfo.pragmaType_
			!= SQLPragma::PRAGMA_NONE) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"Pragma is not supported");
	}

	if (viewParsedInfo.syntaxTreeList_[0]->
			calcCommandType() != SyntaxTree::COMMAND_SELECT) {

		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"View definition must be select statement");
	}

	if (viewParsedInfo.syntaxTreeList_[0]->isDDL()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"DDL is not supported");
	}

	if (viewParsedInfo.placeHolderCount_ > 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"PlaceHolder is not supported");
	}

	if (viewParsedInfo.tableList_.size() > 0) {
		orgParsedInfo->tableList_.insert(
				orgParsedInfo->tableList_.end(),
				viewParsedInfo.tableList_.begin(),
				viewParsedInfo.tableList_.end());
	}

	return true;
}

bool SQLExecution::parse(
		SQLExpandViewContext &cxt,
		RequestInfo &request,
		uint32_t viewDepth,
		int64_t viewNsId,
		int64_t maxViewNsId) {

	GenSyntaxTree genSyntaxTree(executionStackAlloc_);
	SQLParsedInfo &parsedInfo = getParsedInfo();

	try {
		
		genSyntaxTree.parseAll(
				&cxt,
				this,
				request.sqlString_,
				viewDepth,
				viewNsId,
				maxViewNsId,
				false);

		return doAfterParse(request);
	} 
	catch (std::exception &e) {

		if (!analyedQueryInfo_.isPragmaStatement()
				&& parsedInfo.getCommandNum() == 0) {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}

		doAfterParse(request);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool SQLExecution::doAfterParse(RequestInfo &request) {

	SQLParsedInfo &parsedInfo = getParsedInfo();
	if (parsedInfo.getCommandNum() > 1) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"Multiple SQL statement is not supported");
	}

	if (analyedQueryInfo_.isPragmaStatement()) {
		if (request.requestType_ == REQUEST_TYPE_UPDATE) {
			setNotSelect(true);
		}
		return false;
	}

	if (parsedInfo.getCommandNum() == 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL,
				"Not supported query syntax");
	}

	executionStatus_.setFetchRemain(config_.maxRows_);
	if (parsedInfo.syntaxTreeList_[0]->calcCommandType()
			!= SyntaxTree::COMMAND_SELECT) {
		setNotSelect(true);
	}

	if (parsedInfo.syntaxTreeList_[0]->isDDL()) {
		setDDL();
	}

	if (context_.isPreparedStatement()
			&& request.requestType_ == REQUEST_TYPE_EXECUTE) {

		if (response_.parameterCount_
				!= static_cast<int32_t>(
						request.bindParamSet_.getParamList(batchCurrentPos_).size())) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_EXECUTION_BIND_FAILED,
					"Unmatch bind parameter count, expect="
					<< response_.parameterCount_
					<< ", actual="
					<< request.bindParamSet_.getParamList(batchCurrentPos_).size());
		}
	}

	if (!context_.isPreparedStatement()
			&& parsedInfo.placeHolderCount_ > 0) {

		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"PlaceHolder must be specified in prepared statement");
	}
	return true;
}

bool SQLExecution::checkFastInsert(
		util::Vector<BindParam*> &setInfo) {

	SQLParsedInfo &parsedInfo = getParsedInfo();
	if (!isFastInserted()) {
		
		if (parsedInfo.syntaxTreeList_.size() == 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_SYNTAX_TREE,
					"Syntax tree is not found");
		}

		if (parsedInfo.syntaxTreeList_[0]->cmdType_
				== SyntaxTree::CMD_INSERT
				&& parsedInfo.syntaxTreeList_[0]->insertSet_
				&& !parsedInfo.syntaxTreeList_[0]->insertList_) {
		}
		else {
			return false;
		}

		if (context_.isPreparedStatement()
				&& response_.parameterCount_
						!= static_cast<int32_t>(setInfo.size())) {
			
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_EXECUTION_BIND_FAILED,
					"Unmatch bind parameter count, expect="
					<< response_.parameterCount_
					<< ", actual=" << setInfo.size());
		}

		SyntaxTree::Set *targetSet
				= parsedInfo.syntaxTreeList_[0]->insertSet_;
		util::Vector<SyntaxTree::ExprList *>
				&mergeSelectList = getMergeSelectList();
		int32_t placeHolderCount = 0;
		
		if (targetSet->right_
				&& targetSet->right_->selectList_) {

			if (targetSet->right_->limitList_) {
				return false;
			}

			if (!checkSelectList(
					targetSet->right_->selectList_,
					mergeSelectList,
					placeHolderCount)) {
				
				return false;
			}
		}
		else {

			if (targetSet->unionAllList_
					&& targetSet->unionAllList_->size() > 0) {
				
				for (size_t pos = 0;
						pos < targetSet->unionAllList_->size(); pos++) {

					if ((*targetSet->unionAllList_)[pos]->limitList_) {
						return false;
					}

					if (!checkSelectList(
							(*targetSet->unionAllList_)[pos]->selectList_,
							mergeSelectList, placeHolderCount)) {
						return false;
					}
				}
			}
			else {
				return false;
			}
		}

		assert(mergeSelectList.size() > 0);
		if (parsedInfo.tableList_.size() == 1 &&
				parsedInfo.tableList_[0]->qName_ &&
				parsedInfo.tableList_[0]->qName_->table_) {
		}
		else {
			return false;
		}
	}

	if (parsedInfo.tableList_[0]->qName_ &&
			SQLCompiler::Meta::isSystemTable(
					*parsedInfo.tableList_[0]->qName_)) {

		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_METATABLE_NOT_UPDATABLE,
				"Meta table '"
				<< parsedInfo.tableList_[0]->qName_->table_->c_str() 
				<< "' is not updatable");
	}

	setFastInserted();
	return true;
}

bool SQLExecution::metaDataQueryCheck(
		SQLConnectionEnvironment  *control) {

	bool forceMetaDataQuery = false;
	std::string value;
	if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_EXECUTE_AS_META_DATA_QUERY, value)) {
		if (value == SQLPragma::VALUE_TRUE) {
			forceMetaDataQuery = true;
		}
	}

	return (executionStatus_.requestType_ == REQUEST_TYPE_PRAGMA
			|| forceMetaDataQuery);
}

bool SQLExecution::checkRefreshTable(
		EventContext &ec,
		NameWithCaseSensitivity &tableName,
		TableSchemaInfo *tableSchema,
		EventMonotonicTime currentEmTime) {

	if (tableSchema->partitionInfo_.partitionType_
			== SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
		if (tableSchema->containerAttr_ != CONTAINER_ATTR_VIEW) {
			return false;
		}
	}

	if ((currentEmTime - tableSchema->lastExecutedTime_)
			> CACHE_EXPIRED_MAX_TIME) {
		
		NoSQLStore *store = getNoSQLStore();
		NoSQLContainer targetContainer(
				ec,
				tableName,
				context_.getSyncContext(),
				this);

		NoSQLStoreOption option(this);
		if (tableName.isCaseSensitive_) {
			option.caseSensitivity_.
					setContainerNameCaseSensitive();
		}
		
		store->getContainer(targetContainer, false, option);
		
		if (targetContainer.isExists()
				&& (targetContainer.isLargeContainer()
				|| targetContainer.isViewContainer())) {
			
			tableSchema->lastExecutedTime_ = currentEmTime;
			tableSchema->disableCache_ = true;
		}
	}
	return false;
}

void SQLExecution::setPreparedOption(
		RequestInfo &request) {

	if (context_.isPreparedStatement()) {
		
		typedef StatementMessage::Options Options;
		if (config_.maxRows_ == INT64_MAX &&
				request.option_.get<
						Options::MAX_ROWS>() != INT64_MAX) {

			config_.maxRows_
					= request.option_.get<Options::MAX_ROWS>();
			executionStatus_.setFetchRemain(config_.maxRows_);
		}

		if (config_.queryTimeout_ == INT32_MAX &&
				request.option_.get<
						Options::STATEMENT_TIMEOUT_INTERVAL>() != INT32_MAX)  {
			config_.queryTimeout_ =
					request.option_.get<
							Options::STATEMENT_TIMEOUT_INTERVAL>();
		}
	}
}

void SQLExecution::checkExecutableJob(
		JobExecutionId targetStatementId) {

	if (executionStatus_.pendingCancelId_
			== targetStatementId) {
	
		JobId jobId(clientId_, targetStatementId);
		
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_CANCELLED,
				"Cancel job, jobId = " << jobId);
	}
}

bool SQLExecution::isCurrentTimeRequired() {

	return (executionStatus_.preparedPlan_
			->currentTimeRequired_);
}

void SQLExecution::cleanupTableCache() {

	NoSQLDB *db = executionManager_->getDB();
	if (db != NULL) {
	
		SQLParsedInfo &parsedInfo = getParsedInfo();
		std::pair<util::Set<util::String>::iterator, bool> outResult;
	
		for (util::Vector<SyntaxTree::Expr*>::iterator
				it = parsedInfo.tableList_.begin();
				it != parsedInfo.tableList_.end(); it++) {

			const SyntaxTree::QualifiedName
					*qName = (*it)->qName_;
			
			if (qName == NULL || qName->table_ == NULL) {
				continue;
			}

			if (SQLCompiler::Meta::isSystemTable(*qName)) {
				continue;
			}

			NoSQLStore *store = getNoSQLStore();
			store->removeCache(qName->table_->c_str());
		}
	}
}

void SQLExecution::checkWritableTable(
		SQLConnectionEnvironment  &control,
		TableSchemaInfo *tableSchema,
		SQLTableInfo &tableInfo) {

	if (!NoSQLUtils::isAccessibleContainer(
		tableSchema->containerAttr_, tableInfo.writable_)) {

		std::string pragmaValue;
		if (control.getEnv(
				SQLPragma::PRAGMA_EXPERIMENTAL_SHOW_SYSTEM, pragmaValue)) {
			
			if (!pragmaValue.empty()) {
				if (!SQLProcessor::ValueUtils::strICmp(
								pragmaValue.c_str(), "1")) {
					
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
							"Target table '" << tableSchema->tableName_
							<< "' is not accessible, attribute=" <<
							static_cast<int32_t>(
									tableSchema->containerAttr_));
				}
			}
		}
	}
}

void SQLExecution::PreparedInfo::cleanup() {

	if (serializedPlan_) {
		globalVarAlloc_.deallocate(serializedPlan_);
		serializedPlan_ = NULL;
		serializedPlanSize_ = 0;
	}

	if (serializedBindInfo_) {
		globalVarAlloc_.deallocate(serializedBindInfo_);
		serializedBindInfo_ = NULL;
		serializedBindInfoSize_ = 0;
	}
}

void SQLExecution::PreparedInfo::copySerialized(
		util::XArray<uint8_t> &binary) {

	if (binary.size() > 0) {

		serializedPlan_ = static_cast<char8_t *>(
				globalVarAlloc_.allocate(binary.size()));
		serializedPlanSize_ = binary.size();
		memcpy(serializedPlan_, binary.data(), binary.size());
	}
}

void SQLExecution::PreparedInfo::copyBind(
		util::XArray<uint8_t> &binary) {

	if (binary.size() > 0) {
		serializedBindInfo_ = static_cast<char8_t *>(
				globalVarAlloc_.allocate(binary.size()));
		serializedBindInfoSize_ = binary.size();
		memcpy(serializedBindInfo_, binary.data(), binary.size());
	}
}

void SQLExecution::PreparedInfo::restoreBind(
		RequestInfo &request) {

	if (serializedBindInfo_ && serializedBindInfoSize_ > 0) {
			util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(
						serializedBindInfo_,
						serializedBindInfoSize_));

		SQLRequestHandler::decodeBindInfo(in, request);
	}
}

void SQLExecution::PreparedInfo::restorePlan(
		SQLPreparedPlan *&plan,
		util::StackAllocator &alloc,
		TupleValue::VarContext &varCxt) {

	if (serializedPlan_ && serializedPlanSize_ > 0) {
	
		util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(
						serializedPlan_,
						serializedPlanSize_));

		TupleValue::coder(
				util::ObjectCoder::withAllocator(
						alloc), &varCxt).decode(in, plan);
	}
}

void SQLExecution::cleanupSerializedData() {

	preparedInfo_.cleanup();
}

void SQLExecution::copySerializedData(
		util::XArray<uint8_t> &binary) {

	preparedInfo_.copySerialized(binary);
}

void SQLExecution::copySerializedBindInfo(
		util::XArray<uint8_t> &binary) {

	preparedInfo_.copyBind(binary);
}

void SQLExecution::restoreBindInfo(
		RequestInfo &request) {
	
	preparedInfo_.restoreBind(request);
}

void SQLExecution::restorePlanInfo(
		SQLPreparedPlan *&plan,
		util::StackAllocator &alloc,
		TupleValue::VarContext &varCxt) {

	preparedInfo_.restorePlan(
			plan, alloc, varCxt);
}

bool SQLExecution::fetchBatch(EventContext &ec, 
		std::vector<int64_t> *countList) {

	bool responsed = executionStatus_.responsed_;
	if (executionStatus_.currentType_
			!= SQL_EXEC_CLOSE && responsed) {
		
		GS_TRACE_INFO(
				DISTRIBUTED_FRAMEWORK,
				GS_TRACE_SQL_INTERNAL_DEBUG,
				"Call reply success, but not executed because of already responded,"
				"clientId=" << clientId_);
		return false;
	}

	Event replyEv(
			ec, SQL_EXECUTE_QUERY, getConnectedPId());
	replyEv.setPartitionIdSpecified(false);
	EventByteOutStream out = replyEv.getOutStream();

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;
	StatementHandler::encodeBooleanData(
			out, response_.transactionStarted_);

	StatementHandler::encodeBooleanData(
			out, response_.isAutoCommit_);

	StatementHandler::encodeIntData<int32_t>(
			out, static_cast<int32_t>(
					batchCountList_.size()));

	for (int32_t i = 0; i < batchCountList_.size(); i++) {

		executionManager_->incOperation(
				false, ec.getWorkerId(),
				static_cast<int32_t>(batchCountList_[i]));

		StatementHandler::encodeIntData<int32_t>(
				out, static_cast<int32_t>(
						batchCountList_[i]));

		StatementHandler::encodeIntData<int32_t>(
				out, response_.parameterCount_);

		bool existsResult = false;
		StatementHandler::encodeBooleanData(
				out, existsResult);
	}

	SQLRequestHandler::encodeEnvList(
			out,
			ec.getAllocator(),
			*executionManager_,
			context_.getClientNd());

	sendClient(ec, replyEv);

	resultSet_->clear();
	executionStatus_.reset();
	response_.reset();
	resetJobVersion();

	isBatch_ = false;
	batchCountList_.clear();
	batchCount_ = 0;
	batchCurrentPos_ = 0;

	return true;
}

bool SQLExecution::fetchNotSelect(
		EventContext &ec,
		TupleList::Reader *reader,
		TupleList::Column *columnList) {

	if (reader->exists()) {

		TupleValue::VarContext::Scope
				varScope(reader->getVarContext());
		
		assert(columnList != NULL);
		TupleList::Column &column = columnList[0];
		TupleList::ReadableTuple rt = reader->get();
		TupleValue val = rt.get(column);
		response_.updateCount_ = val.get<int64_t>();
		
		if (isDDL()) {
			executionManager_->incOperation(
					false, ec.getWorkerId(), 1);
		}
		else {
			executionManager_->incOperation(
					false, ec.getWorkerId(),
					static_cast<int32_t>(response_.updateCount_));
		}

		SQLReplyContext replyCxt;
		replyCxt.set(
				&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		
		if (replyClient(replyCxt)) {
			resultSet_->clear();
			return true;
		}
		else {
			return false;
		}
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_EXECUTION_INVALID_STATUS,
				"Not select sql must be only one result");
	}
}

bool SQLExecution::fetchSelect(
		EventContext &ec,
		TupleList::Reader *reader,
		TupleList::Column *columnList,
		size_t columnSize,
		bool isCompleted) {

	util::StackAllocator &alloc = ec.getAllocator();
	bool retFlag = false;

	int64_t actualFetchCount = 0;
	int64_t currentFetchNum
			= executionStatus_.getFetchRemain();
	
	if (currentFetchNum > SQLExecution::FETCH_LIMIT_SIZE) {
		currentFetchNum = SQLExecution::FETCH_LIMIT_SIZE;
	}

	int64_t sizeLimit
			= executionManager_->getFetchSizeLimit();			
	resultSet_->clear();
	resultSet_->init(alloc);

	while (reader->exists()
			&& actualFetchCount < currentFetchNum
			&& resultSet_->getSize() < sizeLimit) {

		resultSet_->next(alloc);

		TupleValue::VarContext::Scope
				varScope(reader->getVarContext());

		TupleList::ReadableTuple rt = reader->get();
		for (int32_t columnPos = 0;
				columnPos < static_cast<int32_t>(columnSize);
				columnPos++) {
			
			TupleList::Column &column = columnList[columnPos];
			const TupleValue &value = rt.get(column);

			if (TupleColumnTypeUtils::isNull(value.getType())) {
				setNullValue(value, alloc, column);	
				continue;
			}
			
			switch (column.type_
					& ~TupleList::TYPE_MASK_NULLABLE) {

				case TupleList::TYPE_ANY: {
					setAnyTypeData(alloc, value);
				}
				break;

				case TupleList::TYPE_BYTE:
				case TupleList::TYPE_SHORT:
				case TupleList::TYPE_INTEGER:
				case TupleList::TYPE_LONG:
				case TupleList::TYPE_FLOAT:
				case TupleList::TYPE_NUMERIC:
				case TupleList::TYPE_DOUBLE:
				case TupleList::TYPE_TIMESTAMP:
				case TupleList::TYPE_BOOL: {
				
					resultSet_->setFixedFieldValue(value.fixedData(),
							TupleColumnTypeUtils::getFixedSize(value.getType()));
				}
				break;

				case TupleList::TYPE_STRING: {

					resultSet_->setVarFieldValue(
							value.varData(), value.varSize());
				}
				break;
				case TupleList::TYPE_BLOB: {

					util::XArray<uint8_t> blobs(alloc);
					generateBlobValue(value, blobs);

					resultSet_->setVarFieldValue(
							blobs.data(), blobs.size());
				}
				break;
				default:
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_OPTYPE_UNSUPPORTED, "");
			}
		}
		actualFetchCount++;
		reader->next();
	}

	bool isFetchSizeLimit
			= executionStatus_.declFetchRemain(
					actualFetchCount);

	if ((!reader->exists() && isCompleted)
			|| isFetchSizeLimit) {

		response_.isContinue_ = false;
		retFlag = true;
	}
	else {
		response_.isContinue_ = true;
	}

	if (actualFetchCount > 0
			|| !response_.isContinue_) {

		SQLReplyContext replyCxt;
		replyCxt.set(
				&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		
		if (replyClient(replyCxt)) {
			resultSet_->clear();
			
			if (isFetchSizeLimit) {
				executionStatus_.setMaxRows(config_.maxRows_);
			}
		}
	}
	return retFlag;
}

void SQLExecution::generateBlobValue(
		const TupleValue &value,
		util::XArray<uint8_t> &blobs) {

	TupleValue::LobReader lobReader(value);
	const void *data;
	size_t size;
	
	while (lobReader.next(data, size)) {
		blobs.push_back(
				static_cast<const uint8_t*>(data), size);
	}
}

void SQLExecution::setNullValue(
		const TupleValue &value,
		util::StackAllocator &alloc,
		TupleList::Column &column) {

	if (column.type_ == TupleList::TYPE_ANY) {
		setAnyTypeData(alloc, value);
	}
	else {
		if (TupleColumnTypeUtils::isFixed(column.type_)) {
		
			const uint64_t padding = 0;
			const size_t paddingSize =
					TupleColumnTypeUtils::getFixedSize(column.type_);

			resultSet_->setFixedFieldValue(
					&padding, paddingSize);
		}
		else {
			resultSet_->setVarFieldValue(NULL, 0);
		}
		resultSet_->setNull(column.pos_, true);
	}
}

void SQLExecution::checkConcurrency(
		EventContext &ec, std::exception *e) {

	if (ec.getWorkerId() != getConnectedPgId()) {
		if (e != NULL) {
			try {
				throw *e;
			}
			catch (std::exception &e1) {
				GS_RETHROW_USER_OR_SYSTEM(e1, "");
			}
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_EXECUTION_INTERNAL,
					"Thread concurrency error, current="
					<< ec.getWorkerId() << ", expected="
					<< getConnectedPgId());
		}
	}
}

bool SQLExecution::handleError(
		EventContext &ec,
		std::exception &e,
		int32_t currentVersionId,
		int64_t &delayTime) {

	const util::Exception checkException
			= GS_EXCEPTION_CONVERT(e, "");
	int32_t errorCode = checkException.getErrorCode();
	bool cacheClear = false;
	delayTime = 0;

	SQLReplyType replyType
			= checkReplyType(errorCode, cacheClear);

	if (checkRetryError(
			replyType, currentVersionId)) {
		replyType = SQL_REPLY_ERROR;
	}
	try {
		if (replyType == SQL_REPLY_INTERNAL
				&& executionStatus_.isFetched()) {
			replyType = SQL_REPLY_ERROR;
			
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_EXECUTION_ALREADY_SEND_RESULTSET, 
					"SQL retry operation is failed"
					", already send resultset, reason=("
					<< GS_EXCEPTION_MESSAGE(e) << ")");
		}
		else {
			GS_RETHROW_USER_OR_SYSTEM(
					e, ErrorFormatter(*this, e));
		}
	}
	catch (std::exception &e2) {
		
		bool removed = false;
		switch (replyType) {
		
			case SQL_REPLY_INTERNAL: {
				UTIL_TRACE_EXCEPTION_WARNING(
						SQL_SERVICE, e2, "");
				
				if (NoSQLUtils::isDenyException(errorCode)) {
					delayTime = DEFAULT_NOSQL_FAILOVER_WAIT_TIME;
					resourceSet_->getClusterService()
							->requestRefreshPartition(ec);
				}
				try {
					if (cacheClear) {
						cleanupTableCache();
					}

					resultSet_->resetSchema();
					cancelPrevJob(ec);
					
					updateJobVersion();
					return false;
				}
				catch (std::exception &e2) {
					replyError(
							ec, StatementHandler::TXN_STATEMENT_ERROR, e2);
					removed = true;
				}
			}
			break;

			case SQL_REPLY_DENY: {
				if (replyError(
						ec, StatementHandler::TXN_STATEMENT_DENY, e2)) {
					removed = true;
				}

				resourceSet_->getClusterService()
						->requestRefreshPartition(ec);
			}
			break;

			case SQL_REPLY_ERROR: {
				if (replyError(
						ec, StatementHandler::TXN_STATEMENT_ERROR, e2)) {
					removed = true;
				}	
			}
		}

		if (removed && !context_.isPreparedStatement()) {
			executionManager_->remove(ec, clientId_);
		}
	}
	return true;
}

void SQLExecution::setupViewContext(
		SQLExpandViewContext &cxt,
		EventContext &ec,
		SQLConnectionEnvironment &connectionControl,
		util::Set<util::String> &tableSet,
		util::Set<util::String> &viewSet,
		util::Vector<NodeId> &liveNodeIdList,
		bool useCache,
		bool withVersion) {

	cxt.alloc_ = &ec.getAllocator();
	cxt.control_ = &connectionControl;
	cxt.parsedInfo_ = &getParsedInfo();
	cxt.defaultDbName_ = context_.getDBName();
	cxt.ec_ = &ec;
	cxt.tableInfoList_ = NULL;
	cxt.tableSet_ = &tableSet;
	cxt.liveNodeIdList_ = &liveNodeIdList;
	cxt.viewSet_ = &viewSet;
	cxt.useCache_ = useCache;
	cxt.withVersion_ = withVersion;
}

void SQLExecution::generateJobId(
		JobId &jobId, JobExecutionId execId) {

	if (isBatch_) {
		context_.getCurrentJobId(jobId);
		jobId.clientId_ = clientId_;
		jobId.execId_ = INT64_MAX - batchCurrentPos_;
	}
	else {
		checkExecutableJob(execId);
		context_.getCurrentJobId(jobId);

		jobId.clientId_ = clientId_;
		jobId.execId_ = context_.getExecId();
	}
	context_.setCurrentJobId(jobId);
}

void SQLExecution::setupSyncContext() {

	StatementMessage::UserType userType
			= context_.getConnectionOption<
					ConnectionOption>().userType_;

	context_.syncContext_=
			ALLOC_VAR_SIZE_NEW(globalVarAlloc_) NoSQLSyncContext(
					resourceSet_,
					clientId_,
					jobManager_->getGlobalAllocator(),
					userType,
					context_.getDBName(),
					context_.getDBId(),
					config_.queryTimeout_,
					config_.txnTimeout_);
}


SessionId SQLExecution::SQLExecutionContext::
		incCurrentSessionId() {
	
	return getConnectionOption<
			ConnectionOption>().currentSessionId_++;
}

SessionId SQLExecution::SQLExecutionContext::
		getCurrentSessionId() {
	
	return getConnectionOption<
			ConnectionOption>().currentSessionId_;
}

void SQLExecution::updateJobVersion(JobId &jobId) {
	
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_.versionId_++;
	jobId = currentJobId_;
}

void SQLExecution::updateJobVersion() {
	
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_.versionId_++;
}

void SQLExecution::resetJobVersion() {
	
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_.versionId_ = 0;
}

void SQLExecution::updateConnection() {
	
	context_.getConnectionOption<
			ConnectionOption>().removeSessionId(clientId_);
}

bool SQLExecution::checkSelectList(
		SyntaxTree::ExprList *selectList,
		util::Vector<SyntaxTree::ExprList *> &mergeSelectList,
		int32_t &placeHolderCount) {

	SyntaxTree::ExprList ::iterator it;
	for (it = selectList->begin();
			it != selectList->end(); it++) {

		if ((*it)->op_ != SQLType::EXPR_CONSTANT) {
			
			if ((*it)->op_ != SQLType::EXPR_PLACEHOLDER) {
				return false;
			}
			else {
				if (!context_.isPreparedStatement()) {
					return false;
				}
				
				(*it)->placeHolderPos_ = placeHolderCount;
				placeHolderCount++;
			}
		}
	}

	mergeSelectList.push_back(selectList);
	return true;
};

void ResponseInfo::reset() {

	stmtId_ = UNDEF_STATEMENTID;
	transactionStarted_ = false;
	isAutoCommit_ = true;
	isRetrying_ = false;
	resultCount_ = 1;
	updateCount_ = 0;
	existsResult_ = false;
	isContinue_ = false;
}

void ResponseInfo::set(
		RequestInfo &request, SQLResultSet *resultSet) {

	resultSet_ = resultSet;
	reset();
	stmtId_ = request.stmtId_;
	
	transactionStarted_ = request.transactionStarted_;
	isAutoCommit_ = request.isAutoCommit_;
	isRetrying_ = request.retrying_;
}

void SQLExecution::updateRemoteCache(
		EventContext &ec,
		EventEngine *ee,
		const ResourceSet *resourceSet,
		DatabaseId dbId,
		const char *dbName,
		const char *tableName,
		TablePartitioningVersionId versionId) {

	PartitionTable *pt
			= resourceSet->getPartitionTable();

	if (pt->getNodeNum() == 1) return;
	
	Event request(
			ec, UPDATE_TABLE_CACHE, IMMEDIATE_PARTITION_ID);
	EventByteOutStream out = request.getOutStream();
	out << dbId;
	StatementHandler::encodeStringData(out, dbName);
	StatementHandler::encodeStringData(out, tableName);
	out << versionId;
	
	for (int32_t pos = 1;
			pos < pt->getNodeNum(); pos++) {
		
		const NodeDescriptor &nd = ee->getServerND(pos);
		try {
			ee->send(request, nd);
		}
		catch (std::exception &e) {
			UTIL_TRACE_EXCEPTION_WARNING(
					SQL_SERVICE, e, "");
		}
	}
}

SQLExecution::SQLReplyType SQLExecution::checkReplyType(
		int32_t errorCode, bool &clearCache) {
	
	switch (errorCode) {

		case GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH:
		case GS_ERROR_DS_COL_LOCK_CONFLICT:
		case GS_ERROR_DS_TIM_LOCK_CONFLICT:
		case GS_ERROR_JOB_INVALID_RECV_BLOCK_NO:
			clearCache =  true;
			return SQL_REPLY_DENY;
		
		case GS_ERROR_TXN_PARTITION_ROLE_UNMATCH:
		case GS_ERROR_TXN_PARTITION_STATE_UNMATCH:
		case GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN:
		case GS_ERROR_NOSQL_FAILOVER_TIMEOUT:
		case GS_ERROR_DS_DS_CONTAINER_ID_INVALID:
		case GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED:
		case GS_ERROR_DS_DS_CONTAINER_EXPIRED:
		case GS_ERROR_TXN_CONTAINER_NOT_FOUND:
		case GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH:
		case GS_ERROR_SQL_PROC_INVALID_CONSTRAINT_NULL:
		case GS_ERROR_SQL_PROC_INTERNAL_INDEX_UNMATCH:
		case GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION:
		case GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION:
		case GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR:
		case GS_ERROR_CM_INTERNAL_ERROR:
		case GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION:
		case GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH:
			clearCache =  true;
			return SQL_REPLY_INTERNAL;
		
		case GS_ERROR_SQL_COMPILE_COLUMN_NOT_FOUND:
		case GS_ERROR_SQL_COMPILE_COLUMN_NOT_RESOLVED:
		case GS_ERROR_SQL_COMPILE_COLUMN_LIST_UNMATCH:
		case GS_ERROR_SQL_COMPILE_MISMATCH_SCHEMA:
			clearCache =  true;
			return SQL_REPLY_INTERNAL;
		
		default:
			return SQL_REPLY_ERROR;
	}
}

const char *SQLExecution::SQLExecutionContext::
		getDBName() const {

	return clientInfo_.dbName_.c_str();
}

const char *SQLExecution::SQLExecutionContext::
		getApplicationName() const {

	return clientInfo_.applicationName_.c_str();
}
const char *SQLExecution::SQLExecutionContext::
		getQuery() const {

	return analyedQueryInfo_.query_.c_str();
}
void SQLExecution::SQLExecutionContext::
		setQuery(util::String &query) const {

	if (query.size() > 0) {
			analyedQueryInfo_.query_ = query.c_str();
	}
}

DatabaseId SQLExecution::SQLExecutionContext::
		getDBId() const {

	return clientInfo_.dbId_;
}

bool SQLExecution::SQLExecutionContext::
		isAdministrator() const {

	return clientInfo_.isAdministrator_;
}

const SQLString &SQLExecution::SQLExecutionContext::
		getUserName() const {

	return clientInfo_.userName_;
}

int64_t SQLExecution::SQLExecutionContext::
		getStartTime() const {

	return executionStatus_.startTime_;
}

void SQLExecution::SQLExecutionContext::
		setStartTime(int64_t startTime)  {

	executionStatus_.startTime_ = startTime;
}

double SQLExecution::SQLExecutionContext::

		getStoreMemoryAgingSwapRate() const {
	return clientInfo_.storeMemoryAgingSwapRate_;
}

const util::TimeZone &SQLExecution::SQLExecutionContext::
		getTimezone() const {

		return clientInfo_.timezone_;
}

ClientId &SQLExecution::SQLExecutionContext::
		getId() const {

	return clientId_;
}

const char *SQLExecution::SQLExecutionContext::
		getNormalizedDBName() {

	return clientInfo_.normalizedDbName_.c_str();
}

bool SQLExecution::SQLExecutionContext::
		isRetryStatement() const {

	return response_.isRetrying_;
}

bool SQLExecution::SQLExecutionContext::
		isPreparedStatement() const {

	return analyedQueryInfo_.prepared_;
}

const NodeDescriptor &SQLExecution::SQLExecutionContext::
		getClientNd() const {

	return clientInfo_.clientNd_;
}

JobExecutionId SQLExecution::SQLExecutionContext::
		getExecId() {

	return executionStatus_.execId_;
}

NoSQLSyncContext &SQLExecution::SQLExecutionContext::
		getSyncContext() {

	return *syncContext_;
}

template<typename T>
T &SQLExecution::SQLExecutionContext::getConnectionOption() {

	return clientInfo_.clientNd_.getUserData<T>();
}

template ConnectionOption &SQLExecution::SQLExecutionContext::
		getConnectionOption();

bool SQLExecution::replyClient(
		SQLExecution::SQLReplyContext &cxt) {

	switch (cxt.replyType_) {
		
			case SQL_REPLY_SUCCESS:
				return replySuccess(
						*cxt.ec_,
						cxt.typeList_,cxt.versionId_,
						cxt.responseJobId_);
			break;

		case SQL_REPLY_SUCCESS_EXPLAIN:
				return replySuccessExplain(
						*cxt.ec_,
						UNDEF_JOB_VERSIONID, NULL);
			break;

		case SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE:
			return replySuccessExplainAnalyze(
					*cxt.ec_,
					cxt.job_,
					cxt.versionId_,
					cxt.responseJobId_);
			break;

		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INTERNAL, "");
	}
}

void SQLExecution::SQLExecutionContext::
		getCurrentJobId(JobId &jobId, bool isLocal) {

	UNUSED_VARIABLE(isLocal);
	util::LockGuard<util::Mutex> guard(lock_);
	jobId = currentJobId_;
}

void SQLExecution::SQLExecutionContext::
		setCurrentJobId(JobId &jobId) {

	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_ = jobId;
}

uint8_t SQLExecution::SQLExecutionContext::
		getCurrentJobVersionId() {

	util::LockGuard<util::Mutex> guard(lock_);
	return currentJobId_.versionId_;
}

util::StackAllocator *SQLExecution::wrapStackAllocator() {
	
	return executionManager_->getStackAllocator();
}