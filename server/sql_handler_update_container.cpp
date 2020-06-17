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
#include "sql_service_handler.h"
#include "log_manager.h"
#include "result_set.h"
#include "query_processor.h"
#include "data_store.h"
#include "sql_service.h"
#include "sql_request_info.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "nosql_utils.h"
#include "message_schema.h"
#include "uuid_utils.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

template<typename T>
void decodeLargeRow(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		const char8_t *dbName,
		BaseContainer *container,
		T &record,
		const EventMonotonicTime emNow);

template<typename T>
void putLargeRows(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		LogManager *logManager,
		PartitionTable *partitionTable,
		TransactionManager *transactionManager,
		StatementHandler::Request &request,
		util::XArray<ColumnInfo> &columnInfoList,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList,
		BaseContainer *container, T &record) {

	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	DataStore::PutStatus putStatus;
	
	OutputMessageRowStore outputMrs(
			dataStore->getValueLimitConfig(),
			&columnInfoList[0],
			static_cast<uint32_t>(columnInfoList.size()),
			fixedPart,
			varPart,
			false);

	NoSQLUtils::makeLargeContainerRow(
			alloc, key, outputMrs, record);

	fixedPart.push_back(varPart.data(), varPart.size());
	
	uint64_t numRow = 1;
	util::XArray<RowId> rowIds(alloc);
	rowIds.assign(
			static_cast<size_t>(numRow), UNDEF_ROWID);
	
	container->putRow(
			txn,
			static_cast<uint32_t>(fixedPart.size()),
			fixedPart.data(),
			rowIds[0],
			putStatus,
			PUT_INSERT_OR_UPDATE);

	const bool executed
			= (putStatus != DataStore::NOT_EXECUTED);
	util::XArray<uint8_t> *log = ALLOC_NEW(alloc)
			util::XArray<uint8_t>(alloc);

	ClientId clientId;
	if (txn.getContainerId() == UNDEF_CONTAINERID) {
		clientId = TXN_EMPTY_CLIENTID;
	}
	else {
		clientId = txn.getClientId();
	}
	const LogSequentialNumber lsn = logManager->putPutRowLog(
			*log,
			txn.getPartitionId(),
			clientId,
			txn.getId(),
			container->getContainerId(),
			request.fixed_.cxtSrc_.stmtId_,
			(executed ? numRow : 0),
			rowIds,
			(executed ? numRow : 0),
			fixedPart,
			txn.getTransationTimeoutInterval(),
			request.fixed_.cxtSrc_.getMode_,
			false,
			true);

	partitionTable->setLSN(txn.getPartitionId(), lsn);
	logRecordList.push_back(log);
	
	transactionManager->update(
			txn, request.fixed_.cxtSrc_.stmtId_);
};

void putLargeBinaryRows(
		const char *key, util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		LogManager *logManager,
		PartitionTable *partitionTable,
		TransactionManager *transactionManager,
		StatementHandler::Request &request,
		util::XArray<ColumnInfo> &columnInfoList,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList,
		BaseContainer *container,
		util::XArray<uint8_t> &record) {

	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);
	DataStore::PutStatus putStatus;
	
	OutputMessageRowStore outputMrs(
			dataStore->getValueLimitConfig(),
			&columnInfoList[0],
			static_cast<uint32_t>(columnInfoList.size()),
			fixedPart,
			varPart,
			false);

	NoSQLUtils::makeLargeContainerRowBinary(
			key, outputMrs, record);
	fixedPart.push_back(varPart.data(), varPart.size());
	
	uint64_t numRow = 1;
	util::XArray<RowId> rowIds(alloc);
	rowIds.assign(static_cast<size_t>(numRow), UNDEF_ROWID);
	
	container->putRow(
			txn, 
			static_cast<uint32_t>(fixedPart.size()),
			fixedPart.data(),
			rowIds[0],
			putStatus,
			PUT_INSERT_OR_UPDATE);

	const bool executed
			= (putStatus != DataStore::NOT_EXECUTED);
	util::XArray<uint8_t> *log
			= ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
	
	const LogSequentialNumber lsn = logManager->putPutRowLog(
			*log,
			txn.getPartitionId(),
			txn.getClientId(), txn.getId(),
			txn.getContainerId(),
			request.fixed_.cxtSrc_.stmtId_,
			(executed ? numRow : 0),
			rowIds,
			(executed ? numRow : 0),
			fixedPart,
			txn.getTransationTimeoutInterval(),
			request.fixed_.cxtSrc_.getMode_,
			false,
			true);

	partitionTable->setLSN(txn.getPartitionId(), lsn);
	logRecordList.push_back(log);

	transactionManager->update(
			txn, request.fixed_.cxtSrc_.stmtId_);
};

template<typename T>
void decodeLargeRow(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		const char *dbName,
		BaseContainer *container,
		T &record,
		const EventMonotonicTime emNow) {
	
	UNUSED_VARIABLE(dbName);

	if (container->getAttribute() != CONTAINER_ATTR_LARGE
		&& container->getAttribute() != CONTAINER_ATTR_VIEW) {

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
			<< "' OR key='" << key << "'";
	
	util::String query(queryStr.str().c_str(), alloc);
	ResultSet *rs = dataStore->createResultSet(
		txn,
		container->getContainerId(),
		container->getVersionId(),
		emNow,
		NULL);

	const ResultSetGuard rsGuard(txn, *dataStore, *rs);
	
	QueryProcessor::executeTQL(
			txn,
			*container,
			MAX_RESULT_SIZE,
			TQLInfo(GS_SYSTEM, NULL, query.c_str()),
			*rs);

	rs->setResultType(RESULT_ROWSET);
	
	OutputMessageRowStore outputMessageRowStore(
			dataStore->getValueLimitConfig(),
			container->getColumnInfoList(),
			container->getColumnNum(),
			*(rs->getRowDataFixedPartBuffer()),
			*(rs->getRowDataVarPartBuffer()),
			false /*isRowIdIncluded*/);

	ResultSize resultNum;
	container->getRowList(
			txn,
			*(rs->getOIdList()),
			rs->getResultNum(),
			resultNum,
			&outputMessageRowStore,
			false /*isRowIdIncluded*/,
			0);

	const uint8_t *data1;
	uint32_t size1;
	outputMessageRowStore.getAllFixedPart(data1, size1);
	
	const uint8_t *data2;
	uint32_t size2;
	outputMessageRowStore.getAllVariablePart(data2, size2);

	InputMessageRowStore inputMessageRowStore(
			dataStore->getValueLimitConfig(),
			container->getColumnInfoList(),
			container->getColumnNum(),
			reinterpret_cast<void *>(const_cast<uint8_t *>(data1)),
			size1,
			reinterpret_cast<void *>(const_cast<uint8_t *>(data2)),
			size2,
			rs->getResultNum(),
			false,false);

		VersionInfo versionInfo;
		NoSQLUtils::decodeRow<T>(
				inputMessageRowStore, record, versionInfo, key);
};

template
void decodeLargeRow(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		const char *dbName,
		BaseContainer *container,
		TablePartitioningInfo<util::StackAllocator> &record,
		const EventMonotonicTime emNow);

template
void decodeLargeRow(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		const char *dbName,
		BaseContainer *container,
		TablePartitioningIndexInfo &record,
		const EventMonotonicTime emNow);

template
void decodeLargeRow(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		const char *dbName,
		BaseContainer *container,
		ViewInfo &record,
		const EventMonotonicTime emNow);

void decodeLargeBinaryRow(
		const char *key,
		util::StackAllocator &alloc,
		TransactionContext &txn,
		DataStore *dataStore,
		const char *dbName,
		BaseContainer *container,
		util::XArray<uint8_t> &record,
		const EventMonotonicTime emNow) {

	if (container->getAttribute() != CONTAINER_ATTR_LARGE) {
		
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
			<< "' OR key='" << key << "'";
	
	util::String query(queryStr.str().c_str(), alloc);
	ResultSet *rs = dataStore->createResultSet(
		txn,
		container->getContainerId(),
		container->getVersionId(),
		emNow,
		NULL);

	const ResultSetGuard rsGuard(txn, *dataStore, *rs);
	
	QueryProcessor::executeTQL(
			txn,
			*container,
			MAX_RESULT_SIZE,
			TQLInfo(dbName, NULL, query.c_str()),
			*rs);

	rs->setResultType(RESULT_ROWSET);
	OutputMessageRowStore outputMessageRowStore(
			dataStore->getValueLimitConfig(),
			container->getColumnInfoList(),
			container->getColumnNum(),
			*(rs->getRowDataFixedPartBuffer()),
			*(rs->getRowDataVarPartBuffer()),
			false /*isRowIdIncluded*/);

	ResultSize resultNum;
	container->getRowList(
			txn,
			*(rs->getOIdList()),
			rs->getResultNum(),
			resultNum,
			&outputMessageRowStore,
			false /*isRowIdIncluded*/,
			0);

	const uint8_t *data1;
	uint32_t size1;
	outputMessageRowStore.getAllFixedPart(data1, size1);
	
	const uint8_t *data2;
	uint32_t size2;
	outputMessageRowStore.getAllVariablePart(data2, size2);

	InputMessageRowStore inputMessageRowStore(
			dataStore->getValueLimitConfig(),
			container->getColumnInfoList(),
			container->getColumnNum(),
			reinterpret_cast<void *>(
					const_cast<uint8_t *>(data1)),
			size1,
			reinterpret_cast<void *>(
					const_cast<uint8_t *>(data2)),
			size2,
			rs->getResultNum(),
			false,
			false);

		VersionInfo versionInfo;
		NoSQLUtils::decodeBinaryRow<util::XArray<uint8_t> >(
				inputMessageRowStore,
				record,
				versionInfo,
				key);
};

void PutLargeContainerHandler::operator()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		util::XArray<uint8_t> containerNameBinary(alloc);
		util::String containerName(alloc);
		ContainerType containerType;
		bool modifiable;
		util::XArray<uint8_t> containerInfo(alloc);

		decodeVarSizeBinaryData(in, containerNameBinary);
		decodeEnumData<ContainerType>(in, containerType);
		decodeBooleanData(in, modifiable);
		decodeBinaryData(in, containerInfo, false);

		util::XArray<uint8_t> normalContainerInfo(alloc);
		decodeBinaryData(in, normalContainerInfo, false);

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		if (in.base().remaining() != 0) {

			util::XArray<uint8_t> binary(alloc);
			decodeBinaryData(in, binary, false);
			
			util::ArrayByteInStream inStream(
					util::ArrayInStream(
							binary.data(), binary.size()));

			util::ObjectCoder::withAllocator(alloc).
					decode(inStream, partitioningInfo);
		}

		setContainerAttributeForCreate(request.optional_);
		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_
					= ec.getHandlerStartMonotonicTime();
		}
		
		request.fixed_.clientId_
				= request.optional_.get<Options::CLIENT_ID>();

		const util::String extensionName(
				getExtensionName(request.optional_), alloc);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_,
				clusterRole,
				partitionRole,
				partitionStatus,
				partitionTable_);

		GS_TRACE_INFO(
				DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"[PutContainerHandler PartitionId = " << request.fixed_.pId_ <<
				", stmtId = " << request.fixed_.cxtSrc_.stmtId_);

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT; 
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;

		TransactionContext &txn = transactionManager_->putNoExpire(
				alloc,
				request.fixed_.pId_,
				request.fixed_.clientId_,
				request.fixed_.cxtSrc_,
				now,
				emNow);

		if (txn.getPartitionId() != request.fixed_.pId_) {

			GS_THROW_SYSTEM_ERROR(
					GS_ERROR_TM_TRANSACTION_MODE_INVALID,
					"Invalid context exist request pId=" << request.fixed_.pId_ <<
					", txn pId=" << txn.getPartitionId());
		}

		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);
	
		if (!checkPrivilege(
				ev.getType(),
				connOption.userType_,
				connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
				ANY_CONTAINER,
				static_cast<ContainerAttribute>(
						request.optional_.get<Options::CONTAINER_ATTRIBUTE>()))) {

			GS_THROW_USER_ERROR(
					GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container attribute : " <<
					static_cast<int32_t>(request.optional_.get<
							Options::CONTAINER_ATTRIBUTE>()));
		}

		const FullContainerKey containerKey(
				alloc,
				getKeyConstraint(request.optional_),
				containerNameBinary.data(), containerNameBinary.size());

		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
				connOption.dbId_,
				connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>(),
				isNewSql);

		if (containerName.compare(GS_USERS) == 0 ||
			containerName.compare(GS_DATABASES) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container name : " <<
					request.optional_.get<Options::DB_NAME>());
		}

		{
			bool isAllowExpiration = true;
			ContainerAutoPtr containerAutoPtr(
					txn,
					dataStore_,
					txn.getPartitionId(),
					containerKey,
					containerType,
					getCaseSensitivity(request).isContainerNameCaseSensitive(),
					isAllowExpiration);

			BaseContainer *container
					= containerAutoPtr.getBaseContainer();

			if (container != NULL &&
					container->hasUncommitedTransaction(txn)) {

				try {

					container->getContainerCursor(txn);
					GS_TRACE_INFO(
						DATASTORE_BACKGROUND,
						GS_TRACE_DS_DS_UPDATE_CONTAINER,
						"Continue to change shema");
				}
				catch (UserException &) {

					GS_TRACE_INFO(TRANSACTION_SERVICE,
							GS_TRACE_TXN_WAIT_FOR_TRANSACTION_END,
							"Container has uncommited transactions" <<
							" (pId=" << request.fixed_.pId_ <<
							", stmtId=" << request.fixed_.cxtSrc_.stmtId_ <<
							", eventType=" << request.fixed_.stmtType_ <<
							", clientId=" << request.fixed_.clientId_ <<
							", containerId=" << container->getContainerId() <<
							", pendingCount=" << ev.getQueueingCount() << ")");
					
					ec.getEngine().addPending(
							ev, EventEngine::RESUME_AT_PARTITION_CHANGE);

					return;
				}
			}
		}

		DataStore::PutStatus putStatus;

		ContainerAutoPtr containerAutoPtr(
				txn,
				dataStore_,
				txn.getPartitionId(),
				containerKey,
				containerType,
				static_cast<uint32_t>(containerInfo.size()),
				containerInfo.data(),
				modifiable,
				request.optional_.get<Options::FEATURE_VERSION>(),
				putStatus,
				getCaseSensitivity(request).isContainerNameCaseSensitive());

		BaseContainer *container
				= containerAutoPtr.getBaseContainer();

		request.fixed_.cxtSrc_.containerId_
				= container->getContainerId();
		txn.setContainerId(container->getContainerId());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);
		bool isImmediate = false;
		ContainerCursor containerCursor(isImmediate);
		
		if (putStatus != DataStore::UPDATE) {
		
			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			response.binaryData2_.assign(
				static_cast<const uint8_t*>(containerNameBinary.data()),
				static_cast<const uint8_t*>(
						containerNameBinary.data()) + containerNameBinary.size());

			const bool optionIncluded = true;
			bool internalOptionIncluded = true;
			container->getContainerInfo(
					txn,
					response.binaryData_,
					optionIncluded,
					internalOptionIncluded);

			if (putStatus == DataStore::CREATE
					|| putStatus == DataStore::CHANGE_PROPERY) {

				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutContainerLog(
						*log,
						txn.getPartitionId(),
						txn.getClientId(),
						txn.getId(),
						response.containerId_,
						request.fixed_.cxtSrc_.stmtId_,
						static_cast<uint32_t>(
								containerNameBinary.size()),
						static_cast<const uint8_t*>(
								containerNameBinary.data()),
						response.binaryData_,
						container->getContainerType(),
						static_cast<uint32_t>(extensionName.size()),
						extensionName.c_str(),
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_,
						true,
						false,
						MAX_ROWID,
						(container->getExpireType() == TABLE_EXPIRE));

				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}
		}
		else {
			GS_TRACE_INFO(
					DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
					"[PutContainerHandler PartitionId = " << txn.getPartitionId() <<
					", tId = " << txn.getId() <<
					", stmtId = " << txn.getLastStatementId() <<
					", orgStmtId = " << request.fixed_.cxtSrc_.stmtId_ <<
					", continueChangeSchema = ");

			containerCursor
					= container->getContainerCursor(txn);
			container->continueChangeSchema(txn, containerCursor);
			
			ContainerAutoPtr newContainerAutoPtr(
					txn,
					dataStore_,
					txn.getPartitionId(),
					containerCursor);

			BaseContainer *newContainer
					= newContainerAutoPtr.getBaseContainer();

			response.schemaVersionId_ = newContainer->getVersionId();
			response.containerId_ = newContainer->getContainerId();
			
			response.binaryData2_.assign(
					static_cast<const uint8_t*>(containerNameBinary.data()),
					static_cast<const uint8_t*>(
							containerNameBinary.data()) + containerNameBinary.size());

			const bool optionIncluded = true;
			bool internalOptionIncluded = true;
			newContainer->getContainerInfo(
					txn,
					response.binaryData_,
					optionIncluded,
					internalOptionIncluded);
			{
				util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				
				const LogSequentialNumber lsn = logManager_->putPutContainerLog(
						*log,
						txn.getPartitionId(),
						txn.getClientId(), txn.getId(),
						response.containerId_,
						request.fixed_.cxtSrc_.stmtId_,
						static_cast<uint32_t>(
								containerNameBinary.size()),
						static_cast<const uint8_t*>(
								containerNameBinary.data()),
						response.binaryData_,
						newContainer->getContainerType(),
						static_cast<uint32_t>(extensionName.size()),
						extensionName.c_str(),
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_,
						true,
						false,
						containerCursor.getRowId(),
						(newContainer->getExpireType() == TABLE_EXPIRE));

				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}
			{
				util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);

				const LogSequentialNumber lsn = logManager_->putContinueAlterContainerLog(
						*log,
						txn.getPartitionId(),
						txn.getClientId(),
						txn.getId(),
						txn.getContainerId(),
						request.fixed_.cxtSrc_.stmtId_,
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_,
						true,
						false,
						containerCursor.getRowId());

				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}
		}

		int32_t replicationMode
				= transactionManager_->getReplicationMode();
		ReplicationContext::TaskStatus taskStatus
				= ReplicationContext::TASK_FINISHED;
		
		if (putStatus == DataStore::CREATE
				|| putStatus == DataStore::CHANGE_PROPERY
				|| containerCursor.isFinished()) {

			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);

			const LogSequentialNumber lsn = logManager_->putCommitTransactionLog(
					*log,
					txn.getPartitionId(),
					txn.getClientId(),
					txn.getId(),
					txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_);

			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->commit(txn, *container);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);

			logRecordList.push_back(log);

			transactionManager_->remove(
					request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
					alloc,
					request.fixed_.pId_,
					request.fixed_.clientId_,
					request.fixed_.cxtSrc_,
					now,
					emNow);

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);

			VersionInfo versionInfo;
			putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_VERSION,
					alloc,
					txn,
					dataStore_,
					logManager_,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					container,
					versionInfo);

			partitioningInfo.largeContainerId_ = response.containerId_;
			putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					alloc,
					txn,
					dataStore_,
					logManager_,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					container,
					partitioningInfo);
			
			putLargeBinaryRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
					alloc,
					txn,
					dataStore_,
					logManager_,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					container,
					normalContainerInfo);

			util::ArrayByteInStream normalIn = util::ArrayByteInStream(
					util::ArrayInStream(
							normalContainerInfo.data(),
							normalContainerInfo.size()));

			MessageSchema messageSchema(
					alloc,
					dataStore_->getValueLimitConfig(),
					containerName.c_str(),
					normalIn,
					request.optional_.get<Options::FEATURE_VERSION>());

			util::Vector<ColumnId> rowKeys
					= messageSchema.getRowKeyColumnIdList();

			if (rowKeys.size() > 0
					&& partitioningInfo.containerType_ == COLLECTION_CONTAINER) {

				TablePartitioningIndexInfo
						tablePartitioningIndexInfo(alloc);

				const NameWithCaseSensitivity indexName("", false);
				util::Vector<util::String> columnNameList(alloc);
				util::Vector<ColumnType> columnTypeList(alloc);
				util::Vector<uint8_t> columnOptionList(alloc);
				
				NoSQLUtils::makeContainerColumns(
						alloc,
						normalContainerInfo,
						columnNameList,
						columnTypeList,
						columnOptionList);

				TablePartitioningIndexInfoEntry *entry;
				
				if (rowKeys.size() == 1) {

					ColumnType targetColumnType = columnTypeList[rowKeys[0]];
					
					MapType targetIndexType = NoSQLUtils::getAvailableIndex(
							dataStore_,
							indexName.name_,
							targetColumnType,
							partitioningInfo.containerType_,
							true);

					entry = tablePartitioningIndexInfo.add(
							indexName,
							rowKeys,
							targetIndexType);
				}
				else {
					entry = tablePartitioningIndexInfo.add(
							indexName,
							rowKeys,
							MAP_TYPE_BTREE);
				}

				entry->status_ = INDEX_STATUS_CREATE_END;
				putLargeRows(
						NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
						alloc,
						txn,
						dataStore_,
						logManager_,
						partitionTable_,
						transactionManager_,
						request,
						columnInfoList,
						logRecordList,
						container,
						tablePartitioningIndexInfo);
			}
		}
		else if (putStatus == DataStore::NOT_EXECUTED) {

			transactionManager_->commit(txn, *container);
			transactionManager_->remove(
					request.fixed_.pId_,
					request.fixed_.clientId_);

			closedResourceIds.push_back(
					request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_
					= TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_
					= TransactionManager::AUTO_COMMIT;
			
			txn = transactionManager_->put(
					alloc,
					request.fixed_.pId_,
					request.fixed_.clientId_,
					request.fixed_.cxtSrc_,
					now,
					emNow);
		}
		else {
			replicationMode = TransactionManager::REPLICATION_SEMISYNC;
			taskStatus = ReplicationContext::TASK_CONTINUE;

			transactionManager_->update(
					txn,
					request.fixed_.cxtSrc_.stmtId_);
		}

		const bool ackWait = executeReplication(
				request,
				ec,
				alloc,
				ev.getSenderND(),
				txn, ev.getType(),
				request.fixed_.cxtSrc_.stmtId_,
				replicationMode,
				taskStatus,
				closedResourceIds.data(),
				closedResourceIds.size(),
				logRecordList.data(),
				logRecordList.size(),
				request.fixed_.cxtSrc_.stmtId_,
				0,
				response);

		if (putStatus != DataStore::UPDATE
				|| containerCursor.isFinished()) {

			replySuccess(
					ec,
					alloc,
					ev.getSenderND(),
					ev.getType(),
					TXN_STATEMENT_SUCCESS,
					request,
					response,
					ackWait);
		}
		else {
			continueEvent(
					ec,
					alloc,
					ev.getSenderND(), ev.getType(),
					request.fixed_.cxtSrc_.stmtId_,
					request,
					response,
					ackWait);
		}
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void UpdateContainerStatusHandler::decode(
		EventByteInStream &in,
		Request &request, ConnectionOption &conn,
		util::XArray<uint8_t> &containerNameBinary,
		ContainerCategory &category,
		NodeAffinityNumber &affinity,
		TablePartitioningVersionId &versionId,
		ContainerId &largeContainerId,
		LargeContainerStatusType &status,
		IndexInfo &indexInfo) {	

	int32_t dataSize = 0;
	decodeRequestCommonPart(in, request, conn);
	decodeVarSizeBinaryData(in, containerNameBinary);
	
	in >> category;
	in >> dataSize;
	if (category == TABLE_PARTITIONING_CHECK_EXPIRED) {
		largeContainerId = UNDEF_CONTAINERID;
	} 
	else 
	if (category == TABLE_PARTITIONING_VERSION) {
		in >> versionId;
	}
	else {
		in >> largeContainerId;
		in >> affinity;
		if (category != TABLE_PARTITIONING_RECOVERY) {
			in >> status;
		}
	
		if (in.base().remaining() != 0
				&& category != TABLE_PARTITIONING_RECOVERY) {

			if (status != PARTITION_STATUS_NONE) {
				decodeIndexInfo(in, indexInfo);
			}
			else {
				if (isNewSQL(request)) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_TABLE_PARTITION_INVALID_MESSAGE,
								"Invalid message from sql server, affinity=" << affinity);
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_TABLE_PARTITION_INVALID_MESSAGE,
							"Invalid message from java client, affinity=" << affinity);
				}
			}
		}
	}
}

bool checkExpiredContainer(
		util::StackAllocator &alloc,
		EventContext &ec,
		const FullContainerKey &containerKey,
		TransactionContext &txn,
		BaseContainer *container,
		TablePartitioningInfo<util::StackAllocator> &partitioningInfo, 
		PartitionTable *pt,
		DataStore *dataStore) {

	int64_t currentTime = ec.getHandlerStartTime().getUnixTime();

	util::Vector<NodeAffinityNumber> affinityNumberList(alloc);
	util::Vector<size_t> affinityPosList(alloc);

	Timestamp currentErasableTimestamp;
	if (dataStore->getConfig().isAutoExpire()) {
		currentErasableTimestamp
				= txn.getStatementStartTime().getUnixTime();
	} else {
		currentErasableTimestamp
				= dataStore->getConfig().getErasableExpiredTime();
	}

  int64_t duration = MessageSchema::getTimestampDuration(
			partitioningInfo.timeSeriesProperty_.elapsedTime_,
			partitioningInfo.timeSeriesProperty_.timeUnit_);

	partitioningInfo.checkExpireableInterval(
			currentTime,
			currentErasableTimestamp,
			duration,
			affinityNumberList,
			affinityPosList);

	if (affinityNumberList.size() > 0) {
		
		FullContainerKeyComponents component
				= containerKey.getComponents(alloc, true);
		DatabaseId dbId = component.dbId_;
		
		util::String containerNameStr(alloc);
		containerKey.toString(alloc, containerNameStr);
		
		ContainerId largeContainerId = container->getContainerId();
		for (size_t pos = 0;
				pos < affinityNumberList.size(); pos++) {

			FullContainerKeyComponents subComponents;
			subComponents.dbId_ = dbId;
			subComponents.affinityNumber_ = affinityNumberList[pos];
			subComponents.baseName_ = containerNameStr.c_str();
			subComponents.baseNameSize_ = static_cast<int32_t>(
					strlen(containerNameStr.c_str()));
			subComponents.largeContainerId_ =  largeContainerId;
			
			KeyConstraint keyConstraint(
					KeyConstraint::getNoLimitKeyConstraint());
			FullContainerKey *subContainerKey
					= ALLOC_NEW(alloc) FullContainerKey(
							alloc, keyConstraint, subComponents);

			PartitionId targetPId = NoSQLUtils::resolvePartitionId(alloc,
					pt->getPartitionNum(), *subContainerKey);

			Event sendRequest(ec, DROP_CONTAINER, targetPId);
			EventEngine &ee = ec.getEngine();

			EventByteOutStream out = sendRequest.getOutStream();
			StatementMessage::FixedRequest::Source
					source(targetPId, DROP_CONTAINER);
			
			StatementMessage::FixedRequest request2(source);
			request2.cxtSrc_.stmtId_ = 0;
			
			StatementMessage::OptionSet option(alloc);
			option.set<StatementMessage::Options::REPLY_PID>(targetPId);
			
			bool systemMode = true;
			option.set<StatementMessage::Options::SYSTEM_MODE>(systemMode);
			
			bool forSync = true;
			option.set<StatementMessage::Options::FOR_SYNC>(forSync);
			
			ContainerId containerId = -1;
			option.set<StatementMessage::Options::SUB_CONTAINER_ID>(
					static_cast<int32_t>(containerId));

			request2.encode(out);
			ClientId clientId;
			UUIDUtils::generate(clientId.uuid_);

			option.set<StatementMessage::Options::CLIENT_ID>(clientId);

			option.set<StatementMessage::Options::CONTAINER_ATTRIBUTE>(
					CONTAINER_ATTR_SUB);

			option.set<StatementMessage::Options::FEATURE_VERSION>(
					MessageSchema::V4_1_VERSION);
			
			option.encode(out);
			
			StatementHandler::encodeContainerKey(out, *subContainerKey);
			out << partitioningInfo.containerType_;

			NodeId ownerId = pt->getOwner(targetPId);
			if (ownerId == UNDEF_NODEID) {
				continue;
			}
			
			const NodeDescriptor &nd = ee.getServerND(ownerId);
			if (ownerId == 0) {
				sendRequest.setSenderND(nd);
				ee.add(sendRequest);
			}
			else {
				ee.send(sendRequest, nd);
			}
			
			for (size_t pos = 0; pos < affinityPosList.size(); pos++) {
				
				if (partitioningInfo.assignStatusList_
						[affinityPosList[pos]] != PARTITION_STATUS_DROP_START) {
				
					partitioningInfo.assignStatusList_[affinityPosList[pos]]
							= PARTITION_STATUS_DROP_START;
					partitioningInfo.activeContainerCount_--;
				}
			}
		}
		return true;
	}
	else {
		return false;
	}
}

void UpdateContainerStatusHandler::operator()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		util::XArray<uint8_t> containerNameBinary(alloc);
		
		bool updatePartitioningInfo = false;
		bool updateIndexInfo = false;
		
		TablePartitioningVersionId targetVersionId;
		IndexInfo indexInfo(alloc);
		
		int64_t baseValue = 0;
		
		ContainerId largeContainerId = UNDEF_CONTAINERID;
		LargeContainerStatusType targetStatus = PARTITION_STATUS_NONE;
		NodeAffinityNumber affinityNumber = UNDEF_NODE_AFFINITY_NUMBER;
		ContainerCategory category = UNDEF_LARGE_CONTAINER_CATEGORY;

		decode(
				in,
				request,
				connOption,
				containerNameBinary,
				category,
				affinityNumber,
				targetVersionId,
				largeContainerId,
				targetStatus,
				indexInfo);

		const ClusterRole clusterRole
				= (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		
		if (category != TABLE_PARTITIONING_CHECK_EXPIRED) {
			checkAuthentication(ev.getSenderND(), emNow);  
			checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
			checkTransactionTimeout(
					emNow,
					ev.getQueuedMonotonicTime(),
					request.fixed_.cxtSrc_.txnTimeoutInterval_,
					ev.getQueueingCount());
		}

		checkExecutable(
				request.fixed_.pId_,
				clusterRole,
				partitionRole,
				partitionStatus,
				partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc,
				request.fixed_.pId_ /*pId*/,
				TXN_EMPTY_CLIENTID,
				request.fixed_.cxtSrc_,
				now,
				emNow);

		const DataStore::Latch latch(
				txn,
				txn.getPartitionId(),
				dataStore_,
				clusterService_);
		
		const FullContainerKey containerKey(
				alloc,
				getKeyConstraint(CONTAINER_ATTR_ANY, false),
				containerNameBinary.data(),
				containerNameBinary.size());

		if (category == TABLE_PARTITIONING_VERSION) {

			util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
			util::XArray<uint8_t> containerInfo(alloc);
			ContainerType containerType;

			bool ackWait = false;
			bool isAllowExpiration = true;
			
			ContainerAutoPtr containerAutoPtr(
					txn,
					dataStore_,
					txn.getPartitionId(),
					containerKey,
					ANY_CONTAINER,
					getCaseSensitivity(request).isContainerNameCaseSensitive(),
					isAllowExpiration);

			BaseContainer *beforeContainer
					= containerAutoPtr.getBaseContainer();

			if (beforeContainer
					&& beforeContainer->getTablePartitioningVersionId()
							< targetVersionId) {

				beforeContainer->setTablePartitioningVersionId(targetVersionId);
				containerType = beforeContainer->getContainerType();
				BaseContainer *container = beforeContainer;

				if (container) {
					request.fixed_.cxtSrc_.getMode_
							= TransactionManager::PUT; 
					request.fixed_.cxtSrc_.txnMode_
							= TransactionManager::NO_AUTO_COMMIT_BEGIN;
					
					util::XArray<const util::XArray<uint8_t> *>
							logRecordList(alloc);
					const util::String extensionName(
							getExtensionName(request.optional_), alloc);
					
					util::XArray<uint8_t> *log
							= ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					
					containerInfo.clear();

					const bool optionIncluded = true;
					bool internalOptionIncluded = true;
					container->getContainerInfo(
							txn,
							containerInfo,
							optionIncluded,
							internalOptionIncluded);
					
					const void *createdContainerNameBinary;
					size_t createdContainerNameBinarySize;
					
					containerKey.toBinary(
							createdContainerNameBinary,
							createdContainerNameBinarySize);

					response.binaryData2_.assign(
							static_cast<const uint8_t*>(
									createdContainerNameBinary),
							static_cast<const uint8_t*>(
									createdContainerNameBinary)
											+ createdContainerNameBinarySize);

					const LogSequentialNumber lsn = logManager_->putPutContainerLog(
							*log,
							txn.getPartitionId(),
							txn.getClientId(), UNDEF_TXNID,
							container->getContainerId(),
							request.fixed_.cxtSrc_.stmtId_,
							static_cast<uint32_t>(
									response.binaryData2_.size()),
							response.binaryData2_.data(),
							containerInfo,
							containerType,
							static_cast<uint32_t>(
									extensionName.size()),
							extensionName.c_str(),
							txn.getTransationTimeoutInterval(),
							request.fixed_.cxtSrc_.getMode_,
							false,
							true,
							MAX_ROWID,
							(container->getExpireType() == TABLE_EXPIRE));

					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					logRecordList.push_back(log);

					ackWait = executeReplication(
							request,
							ec,
							alloc,
							ev.getSenderND(),
							txn, ev.getType(),
							request.fixed_.cxtSrc_.stmtId_,
							transactionManager_->getReplicationMode(),
							NULL,
							0,
							logRecordList.data(),
							logRecordList.size(),
							response);
				}
			}

			replySuccess(
					ec,
					alloc,
					ev.getSenderND(),
					ev.getType(),
					TXN_STATEMENT_SUCCESS,
					request,
					response,
					ackWait);

			return;
		}

		ContainerAutoPtr containerAutoPtr(
				txn,
				dataStore_, 
				txn.getPartitionId(),
				containerKey,
				ANY_CONTAINER,
				getCaseSensitivity(request).isContainerNameCaseSensitive());

		BaseContainer *container
				= containerAutoPtr.getBaseContainer();

		if (container == NULL
				|| (container != NULL
						&& container->getContainerId() != largeContainerId
						&& largeContainerId != UNDEF_CONTAINERID)) {

			util::String containerNameStr(alloc);
			containerKey.toString(
					alloc, containerNameStr);
			
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
					"Target large container '"
					<< containerNameStr << "' is already removed");
		}

		TablePartitioningInfo<util::StackAllocator>
				partitioningInfo(alloc);

		decodeLargeRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				alloc,
				txn,
				dataStore_,
				request.optional_.get<Options::DB_NAME>(),
				container,
				partitioningInfo,
				emNow);

		partitioningInfo.init();
		
		if (category == TABLE_PARTITIONING_CHECK_EXPIRED) {
			updatePartitioningInfo = checkExpiredContainer(
					alloc,
					ec,
					containerKey,
					txn,
					container,
					partitioningInfo,
					partitionTable_,
					dataStore_);

			if (!updatePartitioningInfo) {
				return;
			}
		}
		if (category == TABLE_PARTITIONING_RECOVERY) {
			
			ClientId tmpClientId;
			UUIDUtils::generate(tmpClientId.uuid_);
			SQLExecutionManager *executionManager
					= resourceSet_->getSQLExecutionManager();
			util::Random random(
					util::DateTime::now(false).getUnixTime());

			PartitionId targetPId
					= random.nextInt32(DataStore::MAX_PARTITION_NUM);
			PartitionGroupId targetPgId
					= resourceSet_->getSQLService()->getPartitionGroupId(targetPId);

			RequestInfo tmpRequestInfo(alloc, targetPId, targetPgId);
			tmpRequestInfo.clientId_ = tmpClientId;

			SQLExecutionRequestInfo executionInfo(
					tmpRequestInfo, &ev.getSenderND(), *resourceSet_);

			ExecutionLatch tmpLatch(
					tmpClientId,
					executionManager->getResourceManager(),
					&executionInfo);

			Event tmpRequest(ec, SQL_REQUEST_NOSQL_CLIENT, targetPId);
			EventByteOutStream out = tmpRequest.getOutStream();
			StatementHandler::encodeUUID(
					out, tmpClientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);

			const FullContainerKey containerKey
					= container->getContainerKey(txn);
			util::String containerName(txn.getDefaultAllocator());
			containerKey.toString(
					txn.getDefaultAllocator(), containerName);
			StatementHandler::encodeStringData(out, containerName);
			
			out << container->getContainerId();
			out << container->getVersionId();
			out << affinityNumber;
			out << ev.getPartitionId();
			out << request.fixed_.cxtSrc_.stmtId_;
			EventEngine *ee = sqlService_->getEE();
			ee->add(tmpRequest);
			return;
		}

		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);

		bool isOptionSetValue = false;
		if (request.optional_.get<
				Options::INTERVAL_BASE_VALUE>().enabled_) {

			baseValue = request.optional_.get<
					Options::INTERVAL_BASE_VALUE>().baseValue_;
			isOptionSetValue = true;
		}

		int32_t partitioningNum = (partitioningInfo.partitionType_
				== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH
				? partitioningInfo.partitioningNum_ : 1);
		
		switch (targetStatus) {
			
			case PARTITION_STATUS_CREATE_START: {
			
				if (SyntaxTree::isRangePartitioningType(
						partitioningInfo.partitionType_)) {

					if (!isOptionSetValue) {
						baseValue = partitioningInfo.calcValueFromAffinity(
							partitionTable_->getPartitionNum(),
							affinityNumber);
					}
					size_t entryPos
							= partitioningInfo.findEntry(affinityNumber);

					if (entryPos == SIZE_MAX) {
						partitioningInfo.checkMaxAssigned(
								txn,
								container,
								partitioningNum);

						partitioningInfo.newEntry(
								affinityNumber,
								PARTITION_STATUS_CREATE_END,
								partitioningNum,
								baseValue);
						
						partitioningInfo.incrementTablePartitioningVersionId();
						partitioningInfo.activeContainerCount_ += partitioningNum;
						updatePartitioningInfo = true;
					}
				}
			}
			break;

			case PARTITION_STATUS_DROP_START: {
				
				if (SyntaxTree::isRangePartitioningType(
						partitioningInfo.partitionType_)) {

					if (!isOptionSetValue) {
						baseValue = partitioningInfo.calcValueFromAffinity(
							partitionTable_->getPartitionNum(),
							affinityNumber);
					}

					size_t entryPos = partitioningInfo.findEntry(
							affinityNumber);

					if (entryPos == SIZE_MAX) {

						partitioningInfo.newEntry(
								affinityNumber,
								PARTITION_STATUS_DROP_START,
								partitioningNum,
								baseValue);

						updatePartitioningInfo = true;
					}
					else {
						for (size_t currentPos = 0;
								currentPos < static_cast<size_t>(partitioningNum);
								currentPos++) {

							if (partitioningInfo.assignStatusList_
									[entryPos + currentPos] != PARTITION_STATUS_DROP_START) {

								partitioningInfo.assignStatusList_
										[entryPos + currentPos] = targetStatus;

								partitioningInfo.activeContainerCount_--;
								updatePartitioningInfo = true;
							}
						}
					}
				}
			}
			break;

			case INDEX_STATUS_DROP_START:
			case INDEX_STATUS_DROP_END: {

				decodeLargeRow(
						NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
						alloc,
						txn,
						dataStore_,
						request.optional_.get<Options::DB_NAME>(),
						container,
						tablePartitioningIndexInfo,
						emNow);

					TablePartitioningIndexInfoEntry *entry;
					const NameWithCaseSensitivity indexName(
							indexInfo.indexName_.c_str(),
							getCaseSensitivity(request).isIndexNameCaseSensitive());
				
					entry = tablePartitioningIndexInfo.find(indexName);

					if (targetStatus == INDEX_STATUS_DROP_START &&
							request.optional_.get<Options::CREATE_DROP_INDEX_MODE>() !=
							Message::INDEX_MODE_SQL_EXISTS) {
	
						if (entry == NULL) {
							const FullContainerKey containerKey
									= container->getContainerKey(txn);
							util::String *containerNameString =
									ALLOC_NEW(txn.getDefaultAllocator())
											util::String(txn.getDefaultAllocator());
							
							containerKey.toString(
									txn.getDefaultAllocator(), *containerNameString);

							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_DDL_INDEX_NOT_EXISTS,
									"Specified table '" << *containerNameString <<
									"', index '" << indexInfo.indexName_ << "' not exists");
						}
					}
					else {
						if (entry != NULL) {

							size_t removePos = entry->pos_;
							tablePartitioningIndexInfo.remove(removePos);

							partitioningInfo.incrementTablePartitioningVersionId();
							updatePartitioningInfo = true;
							updateIndexInfo = true;
						}
					}
			}
			break;
		}

		util::XArray<ColumnInfo> columnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(columnInfoList);

		if (updatePartitioningInfo || updateIndexInfo) {
			util::XArray<const util::XArray<uint8_t> *>
					logRecordList(alloc);

			if (updatePartitioningInfo) {

				putLargeRows(
						NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
						alloc,
						txn,
						dataStore_,
						logManager_,
						partitionTable_,
						transactionManager_,
						request,
						columnInfoList,
						logRecordList,
						container,
						partitioningInfo);
			}

			if (updateIndexInfo) {
				putLargeRows(
						NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
						alloc,
						txn,
						dataStore_,
						logManager_,
						partitionTable_,
						transactionManager_,
						request,
						columnInfoList,
						logRecordList,
						container,
						tablePartitioningIndexInfo);
			}

			const bool ackWait = executeReplication(
					request,
					ec,
					alloc,
					ev.getSenderND(),
					txn,
					ev.getType(),
					request.fixed_.cxtSrc_.stmtId_,
					transactionManager_->getReplicationMode(),
					NULL,
					0,
					logRecordList.data(),
					logRecordList.size(),
					response);

			replySuccess(
					ec,
					alloc,
					ev.getSenderND(),
					ev.getType(),
					TXN_STATEMENT_SUCCESS,
					request,
					response,
					ackWait);

			if (!isNewSQL(request)) {

				ConnectionOption &connOption
						= ev.getSenderND().getUserData<ConnectionOption>();
				
				Event request(
						ec, UPDATE_TABLE_CACHE, IMMEDIATE_PARTITION_ID);
				EventByteOutStream out = request.getOutStream();
				out << connOption.dbId_;
				StatementHandler::encodeStringData(
						out, connOption.dbName_.c_str());

				const FullContainerKey &containerKey
						= container->getContainerKey(txn);
				util::String containerName(txn.getDefaultAllocator());
				containerKey.toString(
						txn.getDefaultAllocator(), containerName);

				StatementHandler::encodeStringData(
						out, containerName.c_str());
				int64_t versionId = MAX_TABLE_PARTITIONING_VERSIONID;
				out << versionId;
				
				EventEngine *ee =  sqlService_->getEE();
				for (int32_t pos = 0;
						pos < partitionTable_->getNodeNum(); pos++) {
					
					const NodeDescriptor &nd = ee->getServerND(pos);
					try {
						ee->send(request, nd);
					}
					catch (std::exception &) {
					}
				}
			}
		}
		else {
			replySuccess(
					ec,
					alloc,
					ev.getSenderND(),
					ev.getType(),
					TXN_STATEMENT_SUCCESS,
					request,
					response,
					false);
		}
	}
	catch (std::exception &e) {
		try {
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void CreateLargeIndexHandler::operator()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow
			= ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		const uint64_t numRow = 1;
		RowData rowData(alloc);
		IndexInfo indexInfo(alloc);
		bool isComposite = false;

		decodeIndexInfo(in, indexInfo);
		if (ev.getType() == DELETE_INDEX) {
			decodeIntData<uint8_t>(in, indexInfo.anyNameMatches_);
			decodeIntData<uint8_t>(in, indexInfo.anyTypeMatches_);
		}
		const NameWithCaseSensitivity indexName(
				indexInfo.indexName_.c_str(), false);

		assignIndexExtension(indexInfo, request.optional_);

		util::Vector<util::String> indexColumnNameList(alloc);
		util::Vector<ColumnId> &indexColumnIdList
				= indexInfo.columnIds_;
		
		util::String indexColumnNameStr(alloc);
		decodeStringData<util::String>(in, indexColumnNameStr);
		indexColumnNameList.push_back(indexColumnNameStr);

		if (indexColumnIdList.size() > 1) {
			if (in.base().remaining() == 0) {

				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INTERNAL, "");
			}

			isComposite = true;
			
			for (size_t pos = 1;
					pos < indexColumnIdList.size(); pos++) {

				util::String indexColumnNameStr(alloc);
				decodeStringData<util::String>(in, indexColumnNameStr);
				
				indexColumnNameList.push_back(indexColumnNameStr);
			}
		}

		const ClusterRole clusterRole
				= (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		
		checkAuthentication(ev.getSenderND(), emNow);  
	
		checkConsistency(
				ev.getSenderND(), IMMEDIATE_CONSISTENCY);

		checkTransactionTimeout(
				emNow,
				ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_,
				ev.getQueueingCount());

		checkExecutable(
				request.fixed_.pId_,
				clusterRole, partitionRole,
				partitionStatus,
				partitionTable_);

		EmptyAllowedKey::validate(
				KeyConstraint::getNoLimitKeyConstraint(),
				indexInfo.indexName_.c_str(),
				static_cast<uint32_t>(indexInfo.indexName_.size()),
				"indexName");

		TransactionContext &txn = transactionManager_->put(
				alloc,
				request.fixed_.pId_,
				request.fixed_.clientId_,
				request.fixed_.cxtSrc_,
				now,
				emNow);

		const DataStore::Latch latch(
				txn,
				txn.getPartitionId(),
				dataStore_,
				clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn,
				dataStore_,
				txn.getPartitionId(),
				txn.getContainerId(),
				ANY_CONTAINER);

		BaseContainer *container
				= containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(
				container, request.fixed_.schemaVersionId_);

		VersionInfo versionInfo;
		TablePartitioningInfo<util::StackAllocator>
				partitioningInfo(alloc);

		decodeLargeRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				alloc,
				txn,
				dataStore_,
				request.optional_.get<Options::DB_NAME>(),
				container,
				partitioningInfo,
				emNow);

		util::XArray<uint8_t> containerSchema(alloc);
		decodeLargeBinaryRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
				alloc,
				txn,
				dataStore_,
				request.optional_.get<Options::DB_NAME>(),
				container,
				containerSchema,
				emNow);

		util::Vector<ColumnType> indexColumnTypeList(alloc);
		util::Vector<uint8_t> indexColumnOptionList(alloc);
		NoSQLUtils::makeContainerColumns(
				alloc,
				containerSchema,
				indexColumnNameList,
				indexColumnTypeList,
				indexColumnOptionList);

		const NameWithCaseSensitivity columnName(
				indexColumnNameStr.c_str(),
				getCaseSensitivity(request).isColumnNameCaseSensitive());

		TablePartitioningIndexInfoEntry *entry, *checkEntry;
		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
		bool clientSuccess = false;

		decodeLargeRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
				alloc,
				txn,
				dataStore_,
				request.optional_.get<Options::DB_NAME>(),
				container,
				tablePartitioningIndexInfo,
				emNow);

		const FullContainerKey containerKey
				= container->getContainerKey(txn);
		util::String *containerNameString =
				ALLOC_NEW(txn.getDefaultAllocator())
						util::String(txn.getDefaultAllocator());

		containerKey.toString(
				txn.getDefaultAllocator(), *containerNameString);
		
		MapType targetIndexType;
		if (isComposite) {
			targetIndexType = MAP_TYPE_BTREE;
		}
		else {

			targetIndexType = NoSQLUtils::getAvailableIndex(
					dataStore_,
					indexInfo.indexName_.c_str(),
					indexColumnTypeList[indexColumnIdList[0]],
					partitioningInfo.containerType_, false);
		}

		if (partitioningInfo.containerType_ == TIME_SERIES_CONTAINER) {

			for (size_t pos = 0;
						pos < indexColumnIdList.size(); pos++) {

				if (indexColumnIdList[pos] == 0) {
					GS_THROW_USER_ERROR(
							GS_ERROR_DS_TIM_CREATEINDEX_ON_ROWKEY,  "");
				}
			}
		}

		entry = tablePartitioningIndexInfo.find(indexName);
		const bool isExistsOrNotExistsFlag =
				(request.optional_.get<
						Options::CREATE_DROP_INDEX_MODE>() ==
								Message::INDEX_MODE_SQL_EXISTS);
		
		if (entry != NULL && !isExistsOrNotExistsFlag) {
			switch (entry->status_) {
		
				case INDEX_STATUS_CREATE_START: {
		
					entry = tablePartitioningIndexInfo.find(
							indexName,
							indexColumnIdList,
							targetIndexType);
					
					if (entry == NULL) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
								"Specified table '"
								<< containerNameString->c_str() << "', column '"
								<< columnName.name_
								<<"' already exists, and specifed column condition "
								"is not same to original");
					}
					else {
						response.existIndex_ = 1;
					}
				}
				break;
				case INDEX_STATUS_CREATE_END:
				case INDEX_STATUS_DROP_START:
				case INDEX_STATUS_DROP_END: {

					entry = NULL;
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
							"Specified table '" << containerNameString->c_str()
							<< "', indexName '" << indexName. name_ << "' already exists");
				}
				break;
			}
		}

		checkEntry = tablePartitioningIndexInfo.check(
			indexColumnIdList, targetIndexType);
		if (checkEntry) {
		
			if (checkEntry->status_ == INDEX_STATUS_CREATE_END) {

				if (!isExistsOrNotExistsFlag) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
							"Specified table '" << containerNameString->c_str() << "', column '"
							<< columnName.name_ <<"' already exists");
				}
				else {
					clientSuccess = true;
				}
			}
			else if (checkEntry->status_ == INDEX_STATUS_CREATE_START) {

				entry = tablePartitioningIndexInfo.find(
						indexName,
						indexColumnIdList,
						targetIndexType);

				if (!entry) {
				
					if (!isExistsOrNotExistsFlag) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
								"Specified table '" << containerNameString->c_str() << "', column '"
							<< columnName.name_ <<"' already exists");
					}
					else {
						clientSuccess = true;
						response.existIndex_ = 1;
					}
				}
			}
		}

		if (response.existIndex_ != 1) {
		
			entry = tablePartitioningIndexInfo.add(
					indexName,
					indexColumnIdList,
					targetIndexType);
	
			entry->status_ = INDEX_STATUS_CREATE_START;
			partitioningInfo.incrementTablePartitioningVersionId();
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		if (!clientSuccess) {

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);

			if (response.existIndex_ != 1) {

				putLargeRows(
						NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
						alloc,
						txn,
						dataStore_,
						logManager_, 
						partitionTable_,
						transactionManager_,
						request,
						columnInfoList,
						logRecordList,
						container,
						partitioningInfo);
			}

			putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					alloc,
					txn,
					dataStore_,
					logManager_,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					container,
					tablePartitioningIndexInfo);
		}

		const bool ackWait = executeReplication(
				request,
				ec,
				alloc,
				ev.getSenderND(),
				txn,
				ev.getType(),
				request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(),
				NULL,
				0,
				logRecordList.data(),
				logRecordList.size(),
				response);

		replySuccess(
				ec,
				alloc,
				ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS,
				request,
				response,
				ackWait);

		transactionService_->incrementWriteOperationCount(
				ev.getPartitionId());
		transactionService_->addRowWriteCount(
				ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		try {
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

bool CreateLargeIndexHandler::checkCreateIndex(
		IndexInfo &indexInfo,
		ColumnType targetColumnType,
		ContainerType targetContainerType) {

	EmptyAllowedKey::validate(
			KeyConstraint::getUserKeyConstraint(
					dataStore_->getValueLimitConfig().
							getLimitContainerNameSize()),
			indexInfo.indexName_.c_str(),
			static_cast<uint32_t>(indexInfo.indexName_.size()),
			"indexName");

	if (ValueProcessor::isArray(targetColumnType)) {
		return false;
	}
	
	MapType realMapType = defaultIndexType[targetColumnType];
	if (realMapType < 0 || realMapType >= MAP_TYPE_NUM) {
		return false;
	}
	
	bool isSuport;
	
	switch (targetContainerType) {
	
	case COLLECTION_CONTAINER:

		isSuport = Collection::indexMapTable
				[targetColumnType][realMapType];
		break;

	case TIME_SERIES_CONTAINER:

		isSuport = TimeSeries::indexMapTable
				[targetColumnType][realMapType];
		break;

	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}

	if (isSuport) {
		return true;
	}
	else {
		return false;
	}
}