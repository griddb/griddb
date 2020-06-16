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
#include "message_schema.h"
#include "sql_utils.h"
#include "nosql_utils.h"
#include "sql_compiler.h"

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

void SQLGetContainerHandler::operator()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow
			= ec.getHandlerStartMonotonicTime();
	PartitionId pId = ev.getPartitionId();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	
	util::XArray<uint8_t> containerNameBinary(alloc);
	util::String containerName(alloc);
	
	EVENT_START(ec, ev, transactionManager_);

	bool isSettedCompositeIndex = false;

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		int32_t acceptableVersion
				= request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>();
		
		if (acceptableVersion > MessageSchema::V4_1_VERSION) {

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_MSG_VERSION_NOT_ACCEPTABLE,
					"SQL container access version unmatch, expected="
					<< MessageSchema::V4_1_VERSION
					<< ", actual=" << acceptableVersion);
		}

		TableSchemaInfo info(*sqlService_->getAllocator());
		response.schemaMessage_ = &info;
		CompositeIndexInfos compIndex(alloc);
		response.compositeIndexInfos_ = &compIndex;
		bool isContainerLock = false;

		decodeVarSizeBinaryData(in, containerNameBinary);

		decodeBooleanData(in, isContainerLock);

		ClientId clientId;
		decodeUUID(
				in, clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);

		SessionId queryId;
		in >> queryId;

		if (connOption.clientVersion_
				== TXN_V2_5_X_CLIENT_VERSION) {

			request.optional_.set<Options::DB_NAME>(GS_PUBLIC);
		}

		checkTransactionTimeout(
				emNow,
				ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_,
				ev.getQueueingCount());

		checkExecutable(
				pId,
				(CROLE_MASTER | CROLE_FOLLOWER),
				(PROLE_OWNER | PROLE_BACKUP),
				PSTATE_ON, 
				partitionTable_);

		TransactionContext &txn
				= transactionManager_->put(
						alloc,
						request.fixed_.pId_,
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

		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
				connOption.dbId_,
				connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>(),
				isNewSql);

		ContainerAutoPtr containerAutoPtr(
				txn,
				dataStore_,
				txn.getPartitionId(),
				containerKey,
				ANY_CONTAINER,
				getCaseSensitivity(request).isContainerNameCaseSensitive(),
				true);

		BaseContainer* container = containerAutoPtr.getBaseContainer();

		if (container) {
			
			const FullContainerKey
					currentContainerKey = containerAutoPtr.getBaseContainer()
							->getContainerKey(txn);

			util::String *containerNameString =
					ALLOC_NEW(txn.getDefaultAllocator())
					util::String(txn.getDefaultAllocator());

			currentContainerKey.toString(
					txn.getDefaultAllocator(), *containerNameString);

			if (getCaseSensitivity(request).
						isContainerNameCaseSensitive()) {

				util::String targetContainerName(
						txn.getDefaultAllocator());

				containerKey.toString(
						txn.getDefaultAllocator(),
						targetContainerName);

				if (containerNameString->compare(
						targetContainerName)) {

					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
							"Target table = " << targetContainerName
							<< " is not found");
				}
			}
		}

		if (container != NULL &&
				!checkPrivilege(
						ev.getType(),
						connOption.userType_,
						connOption.requestType_,
						request.optional_.get<Options::SYSTEM_MODE>(),
						request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
						container->getContainerType(),
						container->getAttribute())) {

			container = NULL;
		}

		info.founded_ = (container != NULL);
		
		if (info.founded_) {
		
			SQLVariableSizeGlobalAllocator &sqlAllocator
					= *sqlService_->getAllocator();

			containerKey.toString(alloc, containerName);
			TableContainerInfo containerInfo(sqlAllocator);

			OptionSet optionSet(alloc);
			containerInfo.containerName_
					= containerName.c_str();
			containerInfo.containerType_
					= container->getContainerType();
			containerInfo.containerAttribute_
					= container->getAttribute();
			containerInfo.versionId_
					= container->getVersionId();
			containerInfo.containerId_
					= container->getContainerId();

			info.containerInfoList_.push_back(containerInfo);

			uint32_t columnNum = container->getColumnNum();
			
			for (uint32_t i = 0; i < columnNum; i++) {
			
				TableColumnInfo columnInfo(sqlAllocator);

				info.columnInfoList_.push_back(columnInfo);
				
				TableColumnInfo &current
						= info.columnInfoList_.back();
				ColumnInfo &noSqlColumn = container->getColumnInfo(i);
				
				current.name_ = noSqlColumn.getColumnName(
						txn, *container->getObjectManager());
				current.type_ = noSqlColumn.getColumnType();
				
				TupleList::TupleColumnType currentType
						= NoSQLUtils::convertNoSQLTypeToTupleType(current.type_);
				
				bool notNull = noSqlColumn.isNotNull();
				current.tupleType_ = NoSQLUtils::setColumnTypeNullable(
						currentType, !notNull);
				
				if (notNull) {
					ColumnInfo::setNotNull(current.option_);
				}
			}

			if (container->getColumnInfo(
					ColumnInfo::ROW_KEY_COLUMN_ID).isKey()) {
				info.hasRowKey_ = true;
			}
			else {
				info.hasRowKey_ = false;
			}

			typedef util::Vector<IndexInfo> IndexInfoList;
			
			IndexInfoList indexInfoList(alloc);
			container->getIndexInfoList(txn, indexInfoList);
			
			if (indexInfoList.size() > 0)  {
				compIndex.setIndexInfos(indexInfoList);
			}

			if (compIndex.isExists()) {
				isSettedCompositeIndex = true;
			}
			else {
				isSettedCompositeIndex = false;
			}
			
			util::XArray<uint8_t> nullsList(alloc);
			container->getNullsStats(nullsList);
			std::copy(
					nullsList.begin(),
					nullsList.end(),
					std::back_inserter(
							info.containerInfoList_.back().nullStatisticsList_));

			for (IndexInfoList::iterator it = indexInfoList.begin();
					it != indexInfoList.end(); ++it) {

				TableColumnIndexInfoEntry entry(sqlAllocator);

				entry.indexName_ = it->indexName_.c_str();
				entry.indexType_ = it->mapType;
				info.columnInfoList_[it->columnIds_[0]].
						indexInfo_.push_back(entry);

				const int32_t flags
						= NoSQLUtils::mapTypeToSQLIndexFlags(
								it->mapType,
								info.columnInfoList_[it->columnIds_[0]].type_);

				if (flags != 0) {

					if (info.indexInfoList_.empty()) {
						info.indexInfoList_.resize(columnNum, 0);
					}

					info.indexInfoList_[it->columnIds_[0]] |= flags;
				}
			}

			if (container->getAttribute() == CONTAINER_ATTR_VIEW) {
			
				ViewInfo viewInfo(alloc);
				const int64_t emNow = ec.getHandlerStartMonotonicTime();

				try {

					decodeLargeRow(
							NoSQLUtils::LARGE_CONTAINER_KEY_VIEW_INFO,
							alloc,
							txn,
							container->getDataStore(),
							connOption.dbName_.c_str(),
							container,
							viewInfo,
							emNow);

					info.sqlString_ = viewInfo.sqlString_.c_str();

				} catch (std::exception &) {
				}
			}
		}

		if (!isSettedCompositeIndex) {
			response.compositeIndexInfos_ = NULL;
		}

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
	catch (std::exception &e) {
		try {
			response.compositeIndexInfos_ = NULL;
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}
