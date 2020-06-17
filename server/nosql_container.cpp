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
#include "nosql_container.h"
#include "sql_execution.h"
#include "sql_service.h"
#include "sql_utils.h"
#include "nosql_request.h"
#include "nosql_utils.h"
#include "message_schema.h"
#include "uuid_utils.h"

UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(SQL_SERVICE);

const int32_t NoSQLContainer::DEFAULT_DUMP_INTERVAL = 5;

NoSQLContainerBase::NoSQLContainerBase(
		EventContext &ec,
		NoSQLSyncContext &context,
		SQLExecution *execution) :
				ec_(&ec),
				alloc_(ec.getAllocator()),
				context_(&context),
				eventType_(UNDEF_EVENT_TYPE),
				txnMode_(TransactionManager::AUTO_COMMIT),
				getMode_(TransactionManager::AUTO),
				waitInterval_(0),
				isBegin_(false),
				attribute_(CONTAINER_ATTR_SINGLE),
				containerType_(COLLECTION_CONTAINER),
				hasRowKey_(false),
				founded_(false),
				setted_(false),
				sqlString_(alloc_),
				nodeId_(UNDEF_NODEID),
				containerId_(UNDEF_CONTAINERID),
				versionId_(UNDEF_SCHEMAVERSIONID),
				txnPId_(UNDEF_PARTITIONID),
				stmtId_(0),
				partitionRevNo_(0),
				forceAbort_(false),
				execution_(execution),
				currentSessionId_(UNDEF_SESSIONID),
				isCaseSensitive_(false),
				keyConstraint_(KeyConstraint::getNoLimitKeyConstraint()),
				startTime_(0),
				containerName_(alloc_),
				containerKey_(NULL),
				columnInfoList_(alloc_),
				indexInfoList_(alloc_),
				nullStatisticsList_(alloc_),
				indexEntryList_(alloc_),
				compositeIndexEntryList_(alloc_) {
}

NoSQLContainer::NoSQLContainer(
		EventContext &ec,
		const NameWithCaseSensitivity &containerName,
		NoSQLSyncContext &context,
		SQLExecution *execution) :
				NoSQLContainerBase(ec, context, execution),
				withAllocate_(false) {

	containerName_ = containerName.name_;
	isCaseSensitive_ = containerName.isCaseSensitive_;

	PartitionTable *pt
			= context_->getResourceSet()->getPartitionTable();
	
	containerKey_ = ALLOC_NEW(alloc_) FullContainerKey(
			alloc_,
			keyConstraint_,
			execution->getContext().getDBId(),
			containerName_.c_str(), 
			static_cast<uint32_t>(containerName_.size()));
	
	withAllocate_ =  true;

	txnPId_ = NoSQLUtils::resolvePartitionId(
			alloc_, 
			pt->getPartitionNum(),
			*containerKey_);

	nodeId_ = pt->getNewSQLOwner(txnPId_);
	partitionRevNo_ = pt->getNewSQLPartitionRevision(txnPId_);
}

NoSQLContainer::NoSQLContainer(
		EventContext &ec,
		const NameWithCaseSensitivity &containerName,
		FullContainerKey *containerKey,
		NoSQLSyncContext &context,
		SQLExecution *execution) :
				NoSQLContainerBase(ec, context, execution),
				withAllocate_(false) {

	containerName_ = containerName.name_;
	isCaseSensitive_ = containerName.isCaseSensitive_;

	PartitionTable *pt
			= context_->getResourceSet()->getPartitionTable();
	containerKey_ = containerKey;
	
	txnPId_ = NoSQLUtils::resolvePartitionId(
			alloc_,
			pt->getPartitionNum(),
			*containerKey_);
	
	partitionRevNo_
			= pt->getNewSQLPartitionRevision(txnPId_);
	nodeId_ = pt->getNewSQLOwner(txnPId_);
}

NoSQLContainer::NoSQLContainer(
		EventContext &ec,
		ContainerId containerId,
		SchemaVersionId versionId,
		PartitionId pId,
		NoSQLSyncContext &context,
		SQLExecution *execution) :
				NoSQLContainerBase(ec, context, execution),
				withAllocate_(false) {
				
	txnPId_ = pId;
	containerId_ = containerId;
	versionId_ = versionId;

	PartitionTable *pt
			= context_->getResourceSet()->getPartitionTable();
	partitionRevNo_
			= pt->getNewSQLPartitionRevision(txnPId_);
	nodeId_ = pt->getNewSQLOwner(txnPId_);
}

NoSQLContainer::NoSQLContainer(
		EventContext &ec,
		PartitionId pId,
		NoSQLSyncContext &context,
		SQLExecution *execution) :
				NoSQLContainerBase(ec, context, execution),
				withAllocate_(false) {

	txnPId_ = pId;

	PartitionTable *pt
			= context_->getResourceSet()->getPartitionTable();
	nodeId_ = pt->getNewSQLOwner(txnPId_);
}

NoSQLContainer::~NoSQLContainer() {

	if (withAllocate_) {
		ALLOC_DELETE(alloc_, containerKey_);
	}

	for (size_t pos = 0;
			pos < columnInfoList_.size(); pos++) {
		ALLOC_DELETE(alloc_, columnInfoList_[pos]);
	}
}

void NoSQLContainer::start() {

	try {
		PartitionTable *pt
				= context_->getResourceSet()->getPartitionTable();
		nodeId_ = pt->getNewSQLOwner(txnPId_);
		++stmtId_;
	}
	catch (std::exception &e) {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void NoSQLContainerBase::sendMessage(
		EventEngine &ee, Event &request) {

	if (nodeId_ == 0) {
		const NodeDescriptor &nd = ee.getSelfServerND();
		request.setSenderND(nd);
		if (waitInterval_ == 0) {
			ee.add(request);
		}
		else {
			ee.addTimer(request, waitInterval_);
		}
	}
	else {
		const NodeDescriptor nd = ee.getServerND(nodeId_);
		request.setSenderND(nd);
		if (waitInterval_ == 0) {
			ee.send(request, nd);
		}
		else {
			EventRequestOption sendOption;
			sendOption.timeoutMillis_ = waitInterval_;
			ee.send(request, nd, &sendOption);
		}
	}
}

bool NoSQLContainer::executeCommand(
		Event &request,
		bool isSync,
		util::XArray<uint8_t> &response) {

	TransactionService *txnSvc =
			context_->getResourceSet()->getTransactionService();
	
	SQLService *sqlSvc =
			context_->getResourceSet()->getSQLService();

	try {
		
		startTime_ = sqlSvc->getEE()->getMonotonicTime();
		EventMonotonicTime currentTime = startTime_;
		int32_t counter = 0;
		int32_t waitInterval = DEFAULT_WAIT_INTERVAL;
		int32_t timeoutInterval = DEFAULT_TIMEOUT_INTERVAL;

		if (timeoutInterval > context_->getTxnTimeoutInterval()) {
			timeoutInterval = context_->getTxnTimeoutInterval();
		}

		EventMonotonicTime limitTime
				= startTime_ + timeoutInterval;
		util::Exception checkException;
		bool isExceptionOccured;

		while (currentTime < limitTime) {
			isExceptionOccured = false;
			try {
				if (nodeId_ == UNDEF_NODEID) {
					refreshPartition();
				}
				else {
					if (isSync) {
						context_->get(request, alloc_,
							this, waitInterval, timeoutInterval, response);
					}
					else {
						sendMessage(*txnSvc->getEE(), request);
					}
					return true;
				}
			}
			catch (std::exception &e) {
				UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, e, "");

				isExceptionOccured = true;
				checkException = GS_EXCEPTION_CONVERT(e, "");
				int32_t errorCode = checkException.getErrorCode();

				if (NoSQLUtils::isDenyException(errorCode)
						&& isRetryEvent()) {
					refreshPartition();
				}
				else {
					GS_RETHROW_USER_OR_SYSTEM(e, "");
				}
			}

			sqlSvc->checkActiveStatus();
			updateCondition();
			currentTime = sqlSvc->getEE()->getMonotonicTime();

			if ((counter % DEFAULT_DUMP_INTERVAL) == 0) {
				GS_TRACE_WARNING(
						SQL_SERVICE, GS_TRACE_SQL_FAILOVER_WORKING,
						"Failover working, totalDuraton=" << (currentTime - startTime_)
						<< ", limitDuration=" << (limitTime - startTime_) 
						<< ", pId=" << txnPId_ << ", nodeId=" << nodeId_
						<< ", revision=" << partitionRevNo_);
			}
			counter++;

			if (currentTime >= limitTime) {
				if (isExceptionOccured) {
					try {
						throw checkException;
					}
					catch (std::exception &e) {
						GS_RETHROW_USER_ERROR_CODED(
								GS_ERROR_JOB_CANCELLED, e,
								GS_EXCEPTION_MERGE_MESSAGE(
										e, "Cancel SQL, clientId=" 
										<< context_->getClinetId() 
										<< ", location=executeCommand,"
										"reason=Failover timeout, totalDuraton=" 
										<< (currentTime - startTime_)
										<< ", limitDuration=" << (limitTime - startTime_)
										<< ", pId=" << txnPId_ << ", nodeId=" << nodeId_
										<< ", revision=" << partitionRevNo_ 
										<< ", event=" << getEventTypeName(eventType_) << ")"));
					}
				}
				else {

					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_PARTITION_NOT_AVAILABLE,
							"Cancel SQL, clientId=" << context_->getClinetId()  
							<< ", location=executeCommand,"
							" reason=Failover timeout, totalDuraton=" 
							<< (currentTime - startTime_)
							<< ", limitDuration=" << (limitTime - startTime_)
							<< ", pId=" << txnPId_ << ", nodeId=" << nodeId_
							<< ", revision=" << partitionRevNo_ 
							<< ", event=" << getEventTypeName(eventType_) << ")");
				}			
			}
		}
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void NoSQLContainer::getContainerInfo(NoSQLStoreOption &option) {

	try {
		eventType_ = SQL_GET_CONTAINER;
		start();
		
		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeContainerKey(out, *containerKey_);
		bool isContainerLock = false;
		StatementHandler::encodeBooleanData(out, isContainerLock);
		StatementHandler::encodeUUID(
				out, context_->getClinetId().uuid_,
				TXN_CLIENT_UUID_BYTE_SIZE);
		StatementHandler::encodeLongData<SessionId>(out,
				context_->getClinetId().sessionId_);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		SQLVariableSizeGlobalAllocator &globalVarAlloc
				= context_->getGlobalAllocator();
		TableSchemaInfo schemaInfo(globalVarAlloc);
		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}

		util::ArrayByteInStream in =
				util::ArrayByteInStream(
						util::ArrayInStream(
								syncBinaryData, syncBinarySize));
		util::XArray<uint8_t> schemaData(alloc_);

		int32_t schemaSize = static_cast<uint32_t>(
				in.base().remaining());
		schemaData.resize(schemaSize);
		in >> std::make_pair(schemaData.data(), schemaSize);

		try {
			util::ArrayByteInStream inStream(
					util::ArrayInStream(
							schemaData.data(), schemaData.size()));
			util::ObjectCoder::withAllocator(globalVarAlloc)
					.decode(inStream, schemaInfo);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
					e, "Failed to decode schema message"));
		}

		founded_ = schemaInfo.founded_;
		if (founded_) {
			if (!setted_) {

				TableContainerInfo &containerInfo
						= schemaInfo.containerInfoList_[0];
				attribute_ = static_cast<ContainerAttribute>(
						containerInfo.containerAttribute_);
				containerType_ = static_cast<ContainerType>(
						containerInfo.containerType_);

				versionId_ = containerInfo.versionId_;
				containerId_ = containerInfo.containerId_;
				hasRowKey_ = schemaInfo.hasRowKey_;
				
				std::copy(
						containerInfo.nullStatisticsList_.begin(),
						containerInfo.nullStatisticsList_.end(),
						std::back_inserter(nullStatisticsList_));
				
				for (size_t columnId = 0;
						columnId < schemaInfo.columnInfoList_.size(); columnId++) {
					NoSQLColumn *column = ALLOC_NEW (alloc_)
							NoSQLColumn(alloc_);

					TableColumnInfo &currentColumn
							= schemaInfo.columnInfoList_[columnId];

					column->name_ = currentColumn.name_.c_str();
					column->type_ = currentColumn.type_;
					column->option_ = currentColumn.option_;
					column->tupleType_ = currentColumn.tupleType_;
					columnInfoList_.push_back(column);

					for (size_t pos = 0;
							pos < currentColumn.indexInfo_.size(); pos++) {

						IndexInfo indexInfo(alloc_);
						indexInfo.columnIds_.push_back(static_cast<ColumnId>(columnId));
						indexInfo.mapType
								= currentColumn.indexInfo_[pos].indexType_;
						indexInfo.indexName_ 
								= currentColumn.indexInfo_[pos].indexName_.c_str();
						indexEntryList_.push_back(indexInfo);
					}
				}

				if (indexInfoList_.empty()) {
					indexInfoList_.resize(
							schemaInfo.columnInfoList_.size(), 0);
				}
				for (size_t pos = 0;
						pos < schemaInfo.indexInfoList_.size(); pos++) {
					indexInfoList_[pos] = schemaInfo.indexInfoList_[pos];
				}

				sqlString_ = schemaInfo.sqlString_.c_str();
				setted_ = true;
			}
		}
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::putLargeContainer(
		util::XArray<uint8_t> &binarySchemaInfo,
		bool modifiable,
		NoSQLStoreOption &option,
		util::XArray<uint8_t> *schema,
		util::XArray<uint8_t> *binary) {

	try {
		eventType_ = PUT_LARGE_CONTAINER;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeContainerKey(out, *containerKey_);
		containerType_  = option.containerType_;
		out << containerType_;

		modifiable = false;
		StatementHandler::encodeBooleanData(out, modifiable);
		uint32_t size;
		size = static_cast<uint32_t>(binarySchemaInfo.size());
		out << size;

		StatementHandler::encodeBinaryData(
				out, binarySchemaInfo.data(), binarySchemaInfo.size());

		if (schema) {
			size = static_cast<uint32_t>(schema->size());
			out << size;
			StatementHandler::encodeBinaryData(
					out, schema->data(), schema->size());
		}
		if (binary) {
			size = static_cast<uint32_t>(binary->size());
			out << size;
			StatementHandler::encodeBinaryData(
					out, binary->data(), binary->size());
		}

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();

		if (syncBinarySize == 0) {
			return;
		}

		int32_t columnNum = 0;
		int16_t keyColumnId = 0;
		uint8_t isArrayVal;
		
		if (syncBinarySize != 0) {
			util::ArrayByteInStream in =
					util::ArrayByteInStream(
							util::ArrayInStream(binarySchemaInfo.data(),
									binarySchemaInfo.size()));
			in >> columnNum;

			for (int32_t i = 0; i < columnNum; i++) {
				NoSQLColumn *column = ALLOC_NEW(alloc_) NoSQLColumn(alloc_);

				in >> column->name_;
				in >> column->type_;
				in >> isArrayVal;

				columnInfoList_.push_back(column);
			}

			int16_t rowKeyNum;
			in >> rowKeyNum;
			assert(rowKeyNum >= 0 && rowKeyNum <= 1);

			if (rowKeyNum > 0) {
				in >> keyColumnId;
				hasRowKey_ = true;
			}
			else {
				keyColumnId = -1;
				hasRowKey_= false;
			}
		}

		util::ArrayByteInStream remainIn =
				util::ArrayByteInStream(
						util::ArrayInStream(syncBinaryData, syncBinarySize));
		remainIn >> versionId_;
		remainIn >> containerId_;
		attribute_ = option.containerAttr_;

		if (indexInfoList_.empty()) {
			indexInfoList_.resize(getColumnSize(), 0);
		}

		NoSQLUtils::initializeBits(
				nullStatisticsList_, getColumnSize());
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::dropContainer(NoSQLStoreOption &option) {
	try {
		eventType_ = DROP_CONTAINER;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeContainerKey(out, *containerKey_);
		out << containerType_;

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, option.isSync_, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::putUser(
		const char *userName,
		const char *password,
		bool modifiable,
		NoSQLStoreOption &option) {

	try {
		eventType_ = PUT_USER; 
		if (userName == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_USER, "Target userName is NULL");
		}
		start();

		Event request(
				*ec_, eventType_, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeStringData(out, userName);
		int8_t prop = 0;
		out << prop;
		DbUserHandler::checkPasswordLength(password);
		bool hasDigest = (password != NULL);
		StatementHandler::encodeBooleanData(out, hasDigest);
		
		if (hasDigest) {
			char digest[SHA256_DIGEST_STRING_LENGTH + 1];
			SHA256_Data(reinterpret_cast<const uint8_t*>(password),
					strlen(password), digest);
			digest[SHA256_DIGEST_STRING_LENGTH] = '\0';
			StatementHandler::encodeStringData(out, digest);
		}

		StatementHandler::encodeBooleanData(out, modifiable);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::dropUser(const char *userName,
		NoSQLStoreOption &option) {

	try {
		eventType_ = DROP_USER;
		if (userName == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_USER, "Target userName is NULL");
		}
	
		start();
		Event request(
				*ec_, eventType_, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeStringData(out, userName);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::createDatabase(
		const char *dbName, NoSQLStoreOption &option) {
	eventType_ = PUT_DATABASE;
	execDatabaseProcInternal(dbName, option);
}

void NoSQLContainer::dropDatabase(
		const char *dbName, NoSQLStoreOption &option) {

	eventType_ = DROP_DATABASE;
	execDatabaseProcInternal(dbName, option);
}

void NoSQLContainer::execDatabaseProcInternal(
		const char *dbName,
		NoSQLStoreOption &option) {

	try {
		if (dbName == NULL
				|| (dbName != NULL && strlen(dbName) == 0)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_DATABASE, "");
		}
		start();

		Event request(
				*ec_, eventType_, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeStringData(out, dbName);
		int8_t prop = 0;
		out << prop;
		int32_t privilegeNum = 0;
		out << privilegeNum;

		bool modifiable = false;
		StatementHandler::encodeBooleanData(out, modifiable);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}
void NoSQLContainer::putContainer(
	TableExpirationSchemaInfo &info,
		util::XArray<uint8_t> &binarySchemaInfo,
		bool modifiable,
		NoSQLStoreOption &option) {

	try {
		eventType_ = PUT_CONTAINER;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeContainerKey(out, *containerKey_);
		containerType_  = option.containerType_;
		out << containerType_;
		StatementHandler::encodeBooleanData(out, modifiable);

		StatementHandler::encodeBinaryData(
				out, binarySchemaInfo.data(),
				binarySchemaInfo.size());

		info.encode(out);
		
		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}

		int32_t columnNum = 0;
		int16_t keyColumnId = 0;
		uint8_t isArrayVal;
		if (syncBinarySize != 0) {
			founded_ = true;
			util::ArrayByteInStream in =
					util::ArrayByteInStream(
							util::ArrayInStream(binarySchemaInfo.data(),
							binarySchemaInfo.size()));
			in >> columnNum;

			for (int32_t i = 0; i < columnNum; i++) {
				NoSQLColumn *column = ALLOC_NEW(alloc_)
						NoSQLColumn(alloc_);
				in >> column->name_;
				in >> column->type_;
				in >> isArrayVal;
				columnInfoList_.push_back(column);
			}

			int16_t rowKeyNum;
			in >> rowKeyNum;

			if (rowKeyNum > 0) {
				for (int32_t pos = 0; pos < rowKeyNum; pos++) {
					in >> keyColumnId;
				}
				hasRowKey_ = true;
			}
			else {
				keyColumnId = -1;
				hasRowKey_= false;
			}
		}

		util::ArrayByteInStream remainIn =
				util::ArrayByteInStream(
						util::ArrayInStream(syncBinaryData, syncBinarySize));
		remainIn >> versionId_;
		remainIn >> containerId_;
		attribute_ = option.containerAttr_;
		
		if (indexInfoList_.empty()) {
			indexInfoList_.resize(getColumnSize(), 0);
		}
		NoSQLUtils::initializeBits(
				nullStatisticsList_, getColumnSize());
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::grant(
		const char *userName,
		const char *dbName,
		int32_t accessType,
		NoSQLStoreOption &option) {

	execPrivilegeProcessInternal(
			PUT_PRIVILEGE, userName, dbName, accessType, option);
}

void NoSQLContainer::revoke(
		const char *userName,
		const char *dbName,
		int32_t accessType,
		NoSQLStoreOption &option) {

	execPrivilegeProcessInternal(
			DROP_PRIVILEGE, userName, dbName, accessType, option);
}

void NoSQLContainer::execPrivilegeProcessInternal(
		EventType eventType,
		const char *userName,
		const char *dbName,
		int32_t accessType,
		NoSQLStoreOption &option) {

	try {

		eventType_ = eventType;
		start();

		Event request(
				*ec_, eventType_, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);
		StatementHandler::encodeStringData(out, dbName);
		int8_t property = 0;
		out << property;
		int32_t privilegeNum = 1;
		out << privilegeNum;

		const char* privilege;
		if (accessType == 0){
			privilege = "ALL";
		} else {
			privilege = "READ";
		}

		for (int32_t i = 0; i < privilegeNum; ++i) {
			StatementHandler::encodeStringData(out, userName);
			StatementHandler::encodeStringData(out, privilege);
		}

		StatementMessage::OptionSet optionalRequest(alloc_);
		optionalRequest.set<
				StatementMessage::Options::FEATURE_VERSION>(
					StatementMessage::FEATURE_V4_3);
		optionalRequest.encode(out);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::executeSyncQuery(
		const char *query,
		NoSQLStoreOption &option,
		util::XArray<uint8_t> &fixedPart,
		util::XArray<uint8_t> &varPart,
		int64_t &rowCount,
		bool &hasRowKey) {

	try {
		eventType_ = QUERY_TQL;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		out << INT64_MAX;
		ResultSize partialSize_ = INT64_MAX;
		out << partialSize_;
		StatementHandler::encodeBooleanData(out, false);
		int32_t entryNum = 0;
		out << entryNum;
		StatementHandler::encodeStringData(out, query);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}
		util::ArrayByteInStream in =
			util::ArrayByteInStream(
					util::ArrayInStream(syncBinaryData, syncBinarySize));
		ResultType rsType;

		int8_t tmp;
		in >> tmp;
		rsType = static_cast<ResultType>(tmp);
		in >> rowCount;
		size_t size = static_cast<uint32_t>(in.base().remaining());
		uint64_t readSize;

		if (size > 0) {
			in >> readSize;
			fixedPart.resize(static_cast<size_t>(readSize));
			in >> std::make_pair(fixedPart.data(), readSize);
			in >> readSize;
			varPart.resize(static_cast<size_t>(readSize));
			in >> std::make_pair(varPart.data(), readSize);
		}
		hasRowKey = hasRowKey_;
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::putRow(
		util::XArray<uint8_t> &fixedPart,
		util::XArray<uint8_t> &varPart,
		RowId rowId,
		NoSQLStoreOption &option) {

	try {
		if (rowId == UNDEF_ROWID) {
			eventType_ = PUT_ROW;
		}
		else {
			eventType_ = UPDATE_ROW_BY_ID;
		}

		start();
		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		if (rowId != UNDEF_ROWID) {
			out << rowId;
		}

		out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(
						fixedPart.data()), fixedPart.size());
		out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(
						varPart.data()), varPart.size());
		
		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::updateRowSet(
		util::XArray<uint8_t> &fixedPart,
		util::XArray<uint8_t> &varPart,
		util::XArray<RowId> &rowIdList,
		NoSQLStoreOption &option) {

	try {
		eventType_ = UPDATE_MULTIPLE_ROWS_BY_ID_SET;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		uint64_t numRow = static_cast<uint64_t>(rowIdList.size());
		out << numRow;

		for (size_t pos = 0; pos < rowIdList.size();pos++) {
			out << rowIdList[pos];
		}

		out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(
						fixedPart.data()), fixedPart.size());
		out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(
						varPart.data()), varPart.size());

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::putRowSet(
		util::XArray<uint8_t> &fixedPart,
		util::XArray<uint8_t> &varPart,
		uint64_t numRow,
		NoSQLStoreOption &option) {

	try {
		eventType_ = PUT_MULTIPLE_ROWS;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		
		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		out << numRow;

		out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(
						fixedPart.data()), fixedPart.size());
		out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(
						varPart.data()), varPart.size());

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::updateRow(
		RowId rowId,
		util::XArray<uint8_t> &rowData,
		NoSQLStoreOption &option) {

	try {
		eventType_ = UPDATE_ROW_BY_ID;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		
		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		out << rowId;
		StatementHandler::encodeBinaryData(
				out, rowData.data(), rowData.size());

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::deleteRowSet(
		util::XArray<RowId> &rowIdList, NoSQLStoreOption &option) {
	try {
		eventType_ = REMOVE_MULTIPLE_ROWS_BY_ID_SET;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		uint64_t size =  rowIdList.size();
		out << size;
		for (size_t pos = 0; pos < rowIdList.size();pos++) {
			out << rowIdList[pos];
		}

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::createIndex(
		const char *indexName,
		MapType mapType,
		util::Vector<uint32_t> &columnIds,
		NoSQLStoreOption &option) {

	try {
		eventType_ = CREATE_INDEX;
		start();

		createNoSQLClientId();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		IndexInfo indexInfo(alloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.columnIds_.assign(
				columnIds.begin(), columnIds.end()); 
		indexInfo.mapType = mapType;
		StatementHandler::encodeIndexInfo(out, indexInfo);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, option.isSync_, response);

		if (indexInfoList_.empty()) {
			indexInfoList_.resize(getColumnSize(), 0);
		}
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

bool NoSQLContainer::createLargeIndex(
		const char *indexName,
		util::Vector<util::String> &columnNameList,
		MapType mapType,
		util::Vector<ColumnId> &columnIdList, 
		NoSQLStoreOption &option) {

	try {
		eventType_ = CREATE_LARGE_INDEX;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		IndexInfo indexInfo(alloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.columnIds_ = columnIdList;
		indexInfo.mapType = mapType;
	
		StatementHandler::encodeIndexInfo(out, indexInfo);
		StatementHandler::encodeStringData(out, columnNameList[0]);

		if (columnIdList.size() > 1) {
			for (size_t pos = 1; pos < columnIdList.size(); pos++) {
				StatementHandler::encodeStringData(
						out, columnNameList[pos].c_str());
			}
		}

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return false;
		}
		util::ArrayByteInStream in =
				util::ArrayByteInStream(
						util::ArrayInStream(syncBinaryData, syncBinarySize));
		uint8_t ret;
		in >> ret;
		if (indexInfoList_.empty()) {
			indexInfoList_.resize(getColumnSize(), 0);
		}
		if (ret == 1) {
			return true;
		}
		else {
			return false;
		}
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
		return false;
	}
}

bool NoSQLContainer::createLargeIndex(
		const char *indexName, 
		const char *columnName,
		MapType mapType,
		uint32_t columnId,
		NoSQLStoreOption &option) {

	try {
		eventType_ = CREATE_LARGE_INDEX;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		IndexInfo indexInfo(alloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.columnIds_.push_back(columnId); 
		indexInfo.mapType = mapType;
		StatementHandler::encodeIndexInfo(out, indexInfo);
		StatementHandler::encodeStringData(out, columnName);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return false;
		}
		util::ArrayByteInStream in =
				util::ArrayByteInStream(
						util::ArrayInStream(syncBinaryData, syncBinarySize));
		uint8_t ret;
		in >> ret;
		if (indexInfoList_.empty()) {
			indexInfoList_.resize(getColumnSize(), 0);
		}
		if (ret == 1) {
			return true;
		}
		else {
			return false;
		}
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
		return false;
	}
}

void NoSQLContainer::dropIndex(
		const char *indexName, NoSQLStoreOption &option) {
	try {
		eventType_ = DELETE_INDEX;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		IndexInfo indexInfo(alloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.mapType = 0;
		StatementHandler::encodeIndexInfo(out, indexInfo);
		uint8_t anyNameMatches = 0;
		uint8_t anyTypeMatches = 1;
		out << anyNameMatches;
		out << anyTypeMatches;

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, option.isSync_, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::commit(NoSQLStoreOption &option) {

	commitOrRollbackInternal(COMMIT_TRANSACTION, option);
}

void NoSQLContainer::abort(NoSQLStoreOption &option) {

	commitOrRollbackInternal(ABORT_TRANSACTION, option);
}

void NoSQLContainer::commitOrRollbackInternal(
		EventType eventType,
		NoSQLStoreOption &option) {

	try {
		eventType_ = eventType;

		if (enableCommitOrAbort()) {

			start();
			
			Event request(*ec_, eventType_, getPartitionId());
			EventByteOutStream out = request.getOutStream();

			encodeFixedPart(out, eventType_);
			encodeOptionPart(out, eventType_, option);

			util::XArray<uint8_t> response(alloc_);
			executeCommand(request, option.isSync_, response);

			if (!forceAbort_) {
				closeContainer(option);
			}
			isBegin_ = false;
		}
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::createTransactionContext(
		NoSQLStoreOption &option) {

	try {
		eventType_ = CREATE_TRANSACTION_CONTEXT;
		start();
	
		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		closeContainer(option);
		isBegin_ = true;
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::closeContainer(NoSQLStoreOption &option) {

	try {
		eventType_ = CLOSE_SESSION;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, option.isSync_, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::getContainerCount(
		uint64_t &containerCount,
		NoSQLStoreOption &option) {

	try {
		eventType_ = GET_PARTITION_CONTAINER_NAMES;
		containerCount = 0;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		int64_t start = 0, limit = 0;
		out << start;
		out << limit;

		ContainerAttribute targetAttrs[] = {
			CONTAINER_ATTR_SINGLE,
			CONTAINER_ATTR_LARGE,
			CONTAINER_ATTR_SUB,
			CONTAINER_ATTR_SINGLE_SYSTEM
		};
		int32_t count
				= sizeof(targetAttrs) / sizeof(ContainerAttribute);
		out << count;
		
		for (size_t pos = 0;
				pos < static_cast<size_t>(count); pos++) {
			out << static_cast<int32_t>(targetAttrs[pos]);
		}

		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}
		util::ArrayByteInStream in =
				util::ArrayByteInStream(
						util::ArrayInStream(syncBinaryData, syncBinarySize));
		in >> containerCount;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "NoSQL get container count failed");
	}
}

void NoSQLContainer::updateContainerProperty(
		TablePartitioningVersionId versionId,
		NoSQLStoreOption &option) {

	try {

		eventType_ = UPDATE_CONTAINER_STATUS;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();
	
		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);
		
		StatementHandler::encodeContainerKey(out, *containerKey_);

		out << TABLE_PARTITIONING_VERSION;
		const size_t startPos = out.base().position();
		int32_t bodySize = 0;
		out << bodySize;
		out << versionId;
		const size_t endPos = out.base().position();
		out.base().position(startPos);
		StatementHandler::encodeIntData<int32_t>(
				out, static_cast<int32_t>(
						endPos - startPos - sizeof(int32_t)));
		out.base().position(endPos);
		
		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::updateLargeContainerStatus(
		LargeContainerStatusType partitionStatus,
		NodeAffinityNumber affinity,
		NoSQLStoreOption &option,
		LargeExecStatus &execStatus,
		IndexInfo *indexInfo) {

	try {
		eventType_ = UPDATE_CONTAINER_STATUS;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		
		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);
		
		StatementHandler::encodeContainerKey(out, *containerKey_);

		out << LARGE_CONTAINER_STATUS;
		const size_t startPos = out.base().position();
		int32_t bodySize = 0;
		out << bodySize;
		out << containerId_;
		out << affinity;
		out << partitionStatus;
		if (indexInfo) {
			StatementHandler::encodeIndexInfo(out, *indexInfo);
		}
		const size_t endPos = out.base().position();
		out.base().position(startPos);
		StatementHandler::encodeIntData<int32_t>(
				out, static_cast<int32_t>(
						endPos - startPos - sizeof(int32_t)));
		out.base().position(endPos);
		util::XArray<uint8_t> response(alloc_);
		executeCommand(request, true, response);

		uint8_t *syncBinaryData = response.data();
		size_t syncBinarySize = response.size();

		if (syncBinarySize == 0) {
			return;
		}
		util::ArrayByteInStream in =
				util::ArrayByteInStream(
						util::ArrayInStream(
								syncBinaryData, syncBinarySize));
		execStatus.reset();

		in >> execStatus.currentStatus_;
		in >> execStatus.affinityNumber_;
		if (in.base().remaining()) {		
			StatementHandler::decodeIndexInfo(
					in, execStatus.indexInfo_);
		}
	}
	catch (std::exception &e) {
		checkException(eventType_, e);
	}
}

void NoSQLContainer::getContainerBinary(
		NoSQLStoreOption &option,
		util::XArray<uint8_t> &response) {

	try {
		eventType_ = GET_CONTAINER;
		start();

		Event request(*ec_, eventType_, getPartitionId());
		EventByteOutStream out = request.getOutStream();

		encodeFixedPart(out, eventType_);
		encodeOptionPart(out, eventType_, option);

		StatementHandler::encodeContainerKey(out, *containerKey_);
		out << containerType_;
		executeCommand(request, true, response);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "NoSQL get container failed");
	}
}

int32_t NoSQLContainerBase::getColumnId(
		util::StackAllocator &alloc,
		const NameWithCaseSensitivity &columnName) {

	int32_t indexColId = -1;
	int32_t realIndexColId = 0;

	bool isCaseSensitive = columnName.isCaseSensitive_;
	const util::String &normalizeSpecfiedColumnName =
			SQLUtils::normalizeName(alloc, columnName.name_, isCaseSensitive);
	
	for (int32_t col = 0; col < static_cast<int32_t>(
			columnInfoList_.size()); col++) {

		const util::String &normalizeColumnName =
			SQLUtils::normalizeName(alloc,
				columnInfoList_[col]->name_.c_str(), isCaseSensitive);
		if (!normalizeColumnName.compare(
				normalizeSpecfiedColumnName)) {
			indexColId = realIndexColId;
			break;
		}
		realIndexColId++;
	}
	return indexColId;
}

void NoSQLContainer::encodeFixedPart(
		EventByteOutStream &out, EventType type) {

	StatementMessage::FixedRequest::Source source(
			getPartitionId(), type);
	StatementMessage::FixedRequest request(source);

	try {
		switch (type) {
			case PUT_CONTAINER:
			case PUT_LARGE_CONTAINER:
			case DROP_CONTAINER:
			case SQL_GET_CONTAINER:
			case GET_CONTAINER:
			case GET_CONTAINER_PROPERTIES:
			case PUT_USER:
			case DROP_USER:
			case GET_USERS:
			case PUT_DATABASE:
			case DROP_DATABASE:
			case GET_DATABASES:
			case PUT_PRIVILEGE:
			case DROP_PRIVILEGE:
			case GET_PARTITION_CONTAINER_NAMES:
			case UPDATE_CONTAINER_STATUS:
				request.cxtSrc_.stmtId_ = stmtId_;
			
			break;
			
			case CREATE_INDEX:
			case DELETE_INDEX:
				request.cxtSrc_.stmtId_ = stmtId_;
				request.cxtSrc_.containerId_ = containerId_;
				request.schemaVersionId_ = versionId_;
			break;

			case CREATE_TRANSACTION_CONTEXT:
			case CLOSE_TRANSACTION_CONTEXT:
			case COMMIT_TRANSACTION:
			case ABORT_TRANSACTION:
				request.cxtSrc_.stmtId_ = stmtId_;
				request.cxtSrc_.containerId_ = containerId_;
				request.clientId_ = nosqlClientId_;
			break;

			case QUERY_TQL:
			case PUT_ROW:
			case CREATE_LARGE_INDEX:
			case DROP_LARGE_INDEX:
			case PUT_MULTIPLE_ROWS:
			case GET_ROW:
			case UPDATE_ROW_BY_ID:
			case REMOVE_ROW_BY_ID:
			case REMOVE_ROW:
			case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
			case UPDATE_MULTIPLE_ROWS_BY_ID_SET: {
				if (currentSessionId_ == UNDEF_SESSIONID) {
					currentSessionId_
							= execution_->getContext().incCurrentSessionId();
				}
				request.cxtSrc_.stmtId_ = stmtId_;
				request.cxtSrc_.containerId_ = containerId_;
				request.schemaVersionId_ = versionId_;
				request.cxtSrc_.getMode_ = getMode_;
				request.cxtSrc_.txnMode_ = txnMode_;
				memcpy(request.clientId_.uuid_, 
					context_->getClinetId().uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
				request.clientId_.sessionId_ = currentSessionId_;
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED,
					"Unsupported stmtType=" << static_cast<int32_t>(type));
		}
		request.encode(out);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void NoSQLContainer::encodeOptionPart(
		EventByteOutStream &out,
		EventType type,
		NoSQLStoreOption &option) {

	try {
		util::StackAllocator::Scope scope(alloc_);
		StatementMessage::OptionSet optionalRequest(alloc_);

		switch (type) {
			case ABORT_TRANSACTION:
			{
				if (forceAbort_) {
					optionalRequest.set<
							StatementMessage::Options::DDL_TRANSACTION_MODE>(true);
				}
			}
			break;
			case PUT_CONTAINER:
			case DROP_CONTAINER:
			case PUT_LARGE_CONTAINER:
			{
				ClientId clientId;
				UUIDUtils::generate(clientId.uuid_);
				optionalRequest.set<
					StatementMessage::Options::CLIENT_ID>(clientId);
				optionalRequest.set<
					StatementMessage::Options::CONTAINER_ATTRIBUTE>
					(option.containerAttr_);
				optionalRequest.set<
					StatementMessage::Options::FEATURE_VERSION>
					(MessageSchema::V4_1_VERSION);
			}

			case SQL_GET_CONTAINER:
				optionalRequest.set<
					StatementMessage::Options::FEATURE_VERSION>
					(MessageSchema::V4_1_VERSION);
			case GET_CONTAINER:
			case GET_CONTAINER_PROPERTIES:
				if (option.featureVersion_ != -1) {
					optionalRequest.set<
							StatementMessage::Options::FEATURE_VERSION>
							(option.featureVersion_);
					optionalRequest.set<
							StatementMessage::Options::ACCEPTABLE_FEATURE_VERSION>
							(option.acceptableFeatureVersion_);
				}
				optionalRequest.set<
						StatementMessage::Options::SYSTEM_MODE>(true);
				break;
			case UPDATE_CONTAINER_STATUS:
			case PUT_ROW:
			case PUT_MULTIPLE_ROWS:
				optionalRequest.set<
					StatementMessage::Options::PUT_ROW_OPTION>
					(option.putRowOption_);
			break;
			case CREATE_INDEX: {
				ClientId clientId;
				if (isSetClientId()) {
					memcpy(clientId.uuid_, nosqlClientId_.uuid_,
							TXN_CLIENT_UUID_BYTE_SIZE);
				}
				else {
					UUIDUtils::generate(clientId.uuid_);
				}
				clientId.sessionId_ = 1;
				optionalRequest.set<
						StatementMessage::Options::CLIENT_ID>(clientId);
			}
			break;
			case DELETE_INDEX: {
				ClientId clientId;
				UUIDUtils::generate(clientId.uuid_);
				optionalRequest.set<
						StatementMessage::Options::CLIENT_ID>(clientId);
			}
			break;
		}

		StatementMessage::UUIDObject uuid(
				context_->getClinetId().uuid_);
		
		optionalRequest.set<
				StatementMessage::Options::UUID>(uuid);
		optionalRequest.set<
				StatementMessage::Options::QUERY_ID>
				(context_->getClinetId().sessionId_);

		optionalRequest.set<
				StatementMessage::Options::USER_TYPE>
				(context_->getUserType());

		DatabaseId dbVersion;
		const char *dbName;
		if (option.isSystemDb_) {
			dbVersion = GS_SYSTEM_DB_ID;
			dbName = GS_SYSTEM;
		}
		else {
			dbVersion = context_->getDbId();
			dbName = context_->getDbName();
		}
		optionalRequest.set<
				StatementMessage::Options::DB_VERSION_ID>(dbVersion);
		optionalRequest.set<
				StatementMessage::Options::DB_NAME>(dbName);

		optionalRequest.set<
					StatementMessage::Options::STATEMENT_TIMEOUT_INTERVAL>
					(context_->getStatementTimeoutInterval());

		optionalRequest.set<
					StatementMessage::Options::TXN_TIMEOUT_INTERVAL>
					(context_->getTxnTimeoutInterval());

		optionalRequest.set<
					StatementMessage::Options::REPLY_PID>
					(context_->getReplyPId());

		optionalRequest.set<
					StatementMessage::Options::REPLY_EVENT_TYPE>
					(SQL_RECV_SYNC_REQUEST);

		optionalRequest.set<
					StatementMessage::Options::FOR_SYNC>(option.isSync_);

		if (!option.isSync_) {
			optionalRequest.set<
					StatementMessage::Options::SUB_CONTAINER_ID>
					(static_cast<int32_t>(option.subContainerId_));
			optionalRequest.set<
					StatementMessage::Options::JOB_EXEC_ID>
					(option.execId_);
			optionalRequest.set<
					StatementMessage::Options::JOB_VERSION>
					(option.jobVersionId_);
		}
		else {
			optionalRequest.set<
					StatementMessage::Options::NOSQL_SYNC_ID>
					(context_->getRequestId());
		}

		StatementHandler::CreateDropIndexMode indexMode;
		if(option.ifNotExistsOrIfExists_) {
			indexMode = StatementMessage::INDEX_MODE_SQL_EXISTS;
		}
		else {
			indexMode = StatementMessage::INDEX_MODE_SQL_DEFAULT;
		}
		optionalRequest.set<
				StatementMessage::Options::CREATE_DROP_INDEX_MODE>
				(indexMode);

		if (!option.caseSensitivity_.isAllNameCaseInsensitive()) {
			optionalRequest.set<
					StatementMessage::Options::SQL_CASE_SENSITIVITY>
					(option.caseSensitivity_);
		}

		if (option.sendBaseValue_) {
			StatementMessage::IntervalBaseValue baseValue;
			baseValue.baseValue_ = option.baseValue_;
			baseValue.enabled_ = true;
			optionalRequest.set<
					StatementMessage::Options::INTERVAL_BASE_VALUE>(baseValue);
		}

		if (option.applicationName_
				&& strlen(option.applicationName_) > 0) {
			optionalRequest.set<
					StatementMessage::Options::APPLICATION_NAME>
					(option.applicationName_);
		}

		if (option.storeMemoryAgingSwapRate_
				!= TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE) {
			optionalRequest.set<
					StatementMessage::Options::STORE_MEMORY_AGING_SWAP_RATE>
					(option.storeMemoryAgingSwapRate_);
		}

		if (!option.timezone_.isEmpty()) {
			optionalRequest.set<
					StatementMessage::Options::TIME_ZONE_OFFSET>(
					option.timezone_);
		}
		optionalRequest.encode(out);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void NoSQLContainer::OptionalRequest::putType(
		EventByteOutStream &reqOut,
		StatementHandler::OptionType type) {

	reqOut << static_cast<int16_t>(type);
}

void NoSQLContainer::OptionalRequest::clear() {
	optionSet_.clear();
}

void NoSQLContainer::OptionalRequest::set(
		StatementMessage::OptionType type) {
	optionSet_.insert(type);
}

void NoSQLContainerBase::checkCondition(
		uint32_t currentTimer,
		int32_t timeoutInterval) {

	PartitionTable *pt =
			context_->getResourceSet()->getPartitionTable();
	const StatementHandler::ClusterRole
			clusterRole = (StatementHandler::CROLE_MASTER
					| StatementHandler::CROLE_FOLLOWER);
	StatementHandler::checkExecutable(clusterRole, pt);

	NodeId currentOwnerId = pt->getNewSQLOwner(txnPId_);
	PartitionRevisionNo  currentRevisionNo
			= pt->getNewSQLPartitionRevision(txnPId_);

	if (nodeId_ == UNDEF_NODEID) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
				"Invalid partition role, current=UNDEF, expected=OWNER");
	}
	if (nodeId_ != currentOwnerId) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
				"Invalid partition role, current=NOT OWNER"
				<< ", expected=OWNER");
	}
	if (currentRevisionNo != partitionRevNo_) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
				"Invalid partition role revision, current="
				<< currentRevisionNo << ", expected=" << partitionRevNo_);
	}

	if (checkTimeoutEvent(currentTimer, timeoutInterval)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
				"NoSQL command execution timeout, event="
				<< getEventTypeName(eventType_));
	}
}

void NoSQLContainerBase::updateCondition() {
	PartitionTable *pt = 
			context_->getResourceSet()->getPartitionTable();
	nodeId_ = pt->getNewSQLOwner(txnPId_);
	partitionRevNo_ = pt->getNewSQLPartitionRevision(txnPId_);
}

void NoSQLContainerBase::createNoSQLClientId() {

	UUIDUtils::generate(nosqlClientId_.uuid_);
	nosqlClientId_.sessionId_ = 1;
}

void NoSQLContainer::encodeRequestId(
		util::StackAllocator &alloc,
		Event &request,
		NoSQLRequestId requestId) {

	util::StackAllocator::Scope scope(alloc);
	StatementHandler::updateRequestOption<
			StatementMessage::Options::NOSQL_SYNC_ID>(
			alloc, request, requestId);
}


void NoSQLContainerBase::setRequestOption(
		StatementMessage::Request &request) {

	StatementHandler::CompositeIndexInfos *compositeIndexInfos = 
			request.optional_.get<
					StatementMessage::Options::COMPOSITE_INDEX>();

	if (compositeIndexInfos) {
		util::Vector<IndexInfo> &indexInfoList
				= compositeIndexInfos->indexInfoList_;

		for (size_t pos = 0; pos < indexInfoList.size(); pos++) {
			IndexInfo info(alloc_);
			info = indexInfoList[pos];
			compositeIndexEntryList_.push_back(info);
		}
	}
}

util::Vector<IndexInfo> &NoSQLContainerBase::getCompositeIndex() {
	return compositeIndexEntryList_;
}

int32_t NoSQLContainerBase::getProtocolVersion() const {
	return StatementHandler::TXN_CLIENT_VERSION;
}

SQLVariableSizeGlobalAllocator &NoSQLContainerBase::getGlobalAllocator() {
	return context_->getGlobalAllocator();
}

void NoSQLContainerBase::checkException(
		EventType type, std::exception &e) {

	switch (type) { 
		case PUT_ROW:
		case PUT_MULTIPLE_ROWS:
		case UPDATE_ROW_BY_ID:
		case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
		case DROP_CONTAINER:
		case SQL_GET_CONTAINER:
		case GET_CONTAINER:
		case GET_CONTAINER_PROPERTIES:
		case CREATE_INDEX:
		case DELETE_INDEX: {
			const util::Exception check = GS_EXCEPTION_CONVERT(e, "");
			int32_t errorCode = check.getErrorCode();
			if (errorCode != GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}
		}
		break;
		default : {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}
	}
}
	
void NoSQLContainer::refreshPartition() {

	ClusterService *clsSvc =  
			context_->getResourceSet()->getClusterService();

	waitInterval_ = DEFAULT_NOSQL_FAILOVER_WAIT_TIME;
	clsSvc->requestRefreshPartition(*ec_);
	context_->wait(DEFAULT_NOSQL_FAILOVER_WAIT_TIME);
}

void NoSQLContainerBase::setNoSQLAbort(
		SessionId sessionId) {

	setCurrentSesionId(sessionId);

	setMode(
			TransactionManager::NO_AUTO_COMMIT_BEGIN,
			TransactionManager::GET);

	setBeginTransaction();
	setForceAbort();
}
