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
#include "sql_service.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "nosql_db.h"
#include "nosql_container.h"
#include "nosql_utils.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

void SQLRequestNoSQLClientHandler::executeRepair(
		EventContext &ec, 
		SQLExecution *execution,
		util::String &tableNameStr,
		ContainerId largeContainerId,
		SchemaVersionId versionId,
		NodeAffinityNumber affinity) {

	util::StackAllocator &alloc = ec.getAllocator();
	NameWithCaseSensitivity tableName(
			tableNameStr.c_str(), false);

	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();
	PartitionTable *pt = resourceSet_->getPartitionTable();

	NoSQLDB *db = executionManager->getDB();
	NoSQLStore *store = db->getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());

	NoSQLContainer largeContainer(
			ec,
			tableName,
			execution->getContext().getSyncContext(),
			execution);

	NoSQLStoreOption option(execution);
	store->getContainer(largeContainer, false, option);
	
	if (!largeContainer.isExists()) {
	
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_
				<< "' is already dropped");
	}

	if (largeContainer.getContainerId() == largeContainerId
			&& largeContainer.getSchemaVersionId() == versionId) {
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_
				<< "' is already dropped");
	}

	TablePartitioningInfo<util::StackAllocator>
			partitioningInfo(alloc);
	util::XArray<ColumnInfo> largeColumnInfoList(alloc);
	
	NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
	
	store->getLargeRecord<
			TablePartitioningInfo<util::StackAllocator> >(
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					largeContainer,
					largeColumnInfoList,
					partitioningInfo,
					option);

	TablePartitioningIndexInfo
			tablePartitioningIndexInfo(alloc);

	store->getLargeRecord<TablePartitioningIndexInfo>(
			alloc,
			NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
			largeContainer,
			largeColumnInfoList,
			tablePartitioningIndexInfo,
			option);

	util::XArray<uint8_t> containerSchema(alloc);
	store->getLargeBinaryRecord(
			alloc,
			NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
			largeContainer,
			largeColumnInfoList,
			containerSchema,
			option);

	NoSQLContainer *subContainer
			= NoSQLUtils::createNoSQLContainer(
					ec,
					tableName,
					largeContainerId,
					affinity,
					execution);

	TableExpirationSchemaInfo info;
	partitioningInfo.checkTableExpirationSchema(
			info,
			affinity,
			pt->getPartitionNum());

	store->createSubContainer(
			info,
			alloc,
			largeContainer,
			subContainer,
			containerSchema,
			partitioningInfo, 
			tablePartitioningIndexInfo,
			tableName,
			pt->getPartitionNum(),
			affinity);
}

void SQLRequestNoSQLClientHandler::operator()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	
	PartitionId replyPId = UNDEF_PARTITIONID;
	ClientId clientId;
	StatementId stmtId = UNDEF_STATEMENTID;
	bool isSetted = false;
	StatementExecStatus status;
	
	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();
	TransactionService *txnSvc
			= resourceSet_->getTransactionService();

	try {

		EventByteInStream in(ev.getInStream());
		util::String tableName(alloc);
		ContainerId largeContainerId;
		SchemaVersionId versionId;
		NodeAffinityNumber affinityNumber;
		in.readAll(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		
		StatementHandler::decodeStringData<util::String>(in, tableName);
		
		in >> largeContainerId;
		in >> versionId;
		in >> affinityNumber;
		in >> replyPId;
		in >> stmtId;

		isSetted = true;
		ExecutionLatch latch(
				clientId,
				executionManager->getResourceManager(),
				NULL);

		SQLExecution *execution = latch.get();
		if (execution == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED, "");
		}

		executeRepair(
				ec,
				execution,
				tableName,
				largeContainerId,
				versionId,
				affinityNumber);

		Event response(ec, UPDATE_CONTAINER_STATUS, replyPId);
		EventByteOutStream out = encodeCommonPart(
				response, stmtId, TXN_STATEMENT_SUCCESS);

		out << static_cast<LargeContainerStatusType>(
				PARTITION_STATUS_NONE);
		out << static_cast<NodeAffinityNumber>(
				UNDEF_NODE_AFFINITY_NUMBER);
		
		EventEngine *ee = txnSvc->getEE();
		ee->send(response, execution->getContext().getClientNd());

		executionManager->remove(ec, clientId);

	}
	catch (std::exception &e) {

		try {
			if (isSetted) {
				status = StatementHandler::TXN_STATEMENT_ERROR;

				const util::Exception checkException
						= GS_EXCEPTION_CONVERT(e, "");		
				int32_t errorCode = checkException.getErrorCode();
				
				if (NoSQLUtils::isDenyException(errorCode)) {
					status = StatementHandler::TXN_STATEMENT_DENY;
				}

				Event response(ec, UPDATE_CONTAINER_STATUS, replyPId);
				EventByteOutStream out = encodeCommonPart(
						response, stmtId, TXN_STATEMENT_SUCCESS);
				
				EventEngine *ee = txnSvc->getEE();
				ExecutionLatch latch(
						clientId,
						executionManager->getResourceManager(),
						NULL);
				
				SQLExecution *execution = latch.get();
				
				if (execution) {

					ee->send(
							response,
							execution->getContext().getClientNd());
					
					executionManager->remove(ec, clientId);
				}
			}
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}