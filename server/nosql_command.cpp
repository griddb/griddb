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
	@brief nosql command
*/
#include "nosql_command.h"
#include "sql_execution.h"
#include "sql_compiler.h" 
#include "sql_processor.h"
#include "time_series.h"
#include "collection.h"
#include "schema.h"
#include "sql_command_manager.h"
#include "sql_utils.h"
#include "sql_service.h"
#include "message_schema.h"
#include "message_row_store.h"

UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(SQL_SERVICE);

const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_VERSION = "0";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO = "1";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_EXECUTING = "2";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_INDEX = "3";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_ASSIGN_INFO = "4";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA = "5";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_VIEW_INFO = "6";
const char8_t* const NoSQLUtils::LARGE_CONTAINER_KEY_TABLE_PROPERTY = "7";
const char8_t* const NoSQLUtils::DATA_AFFINITY_POLICY_ALL = "all";
const char8_t* const NoSQLUtils::DATA_AFFINITY_POLICY_DISTRIBUTED = "distributed";


/*!
@brief	コンストラクタ
*/
NoSQLContainer::NoSQLContainer(
	EventContext& ec,
	const NameWithCaseSensitivity& containerName,
	NoSQLSyncContext& context, SQLExecution* execution) :
	eventStackAlloc_(ec.getAllocator()),
	nodeId_(UNDEF_NODEID),
	containerId_(UNDEF_CONTAINERID),
	versionId_(UNDEF_SCHEMAVERSIONID),
	txnPId_(UNDEF_PARTITIONID),
	containerName_(containerName.name_, eventStackAlloc_),
	stmtId_(0),
	context_(&context),
	statementType_(UNDEF_EVENT_TYPE),
	txnMode_(TransactionManager::AUTO_COMMIT),
	getMode_(TransactionManager::AUTO),
	waitInterval_(0),
	isBegin_(false),
	attribute_(CONTAINER_ATTR_SINGLE),
	containerType_(COLLECTION_CONTAINER),
	hasRowKey_(false),
	founded_(false),
	setted_(false),
	columnInfoList_(eventStackAlloc_),
	indexInfoList_(eventStackAlloc_),
	nullStatisticsList_(eventStackAlloc_),
	indexEntryList_(eventStackAlloc_),
	compositeIndexEntryList_(eventStackAlloc_),
	forceAbort_(false),
	execution_(execution),
	currentSessionId_(UNDEF_SESSIONID),
	isCaseSensitive_(containerName.isCaseSensitive_),
	keyConstraint_(KeyConstraint::getNoLimitKeyConstraint()),
	containerKey_(NULL),
	startTime_(0),
	ec_(&ec),
	sqlString_(eventStackAlloc_),
	approxSize_(-1)
{
	containerKey_ = ALLOC_NEW(eventStackAlloc_) FullContainerKey(
		eventStackAlloc_,
		keyConstraint_, execution->getContext().getDBId(),
		containerName_.c_str(),
		static_cast<uint32_t>(containerName_.size()));
	txnPId_ = NoSQLUtils::resolvePartitionId(
		eventStackAlloc_,
		context_->pt_->getPartitionNum(),
		*containerKey_);
	nodeId_ = context_->pt_->getNewSQLOwner(txnPId_);
	partitionRevNo_ = context_->pt_->getNewSQLPartitionRevision(txnPId_);
}

NoSQLContainer::NoSQLContainer(
	EventContext& ec,
	const NameWithCaseSensitivity& containerName,
	FullContainerKey* containerKey,
	NoSQLSyncContext& context, SQLExecution* execution) :
	eventStackAlloc_(ec.getAllocator()),
	nodeId_(UNDEF_NODEID),
	containerId_(UNDEF_CONTAINERID),
	versionId_(UNDEF_SCHEMAVERSIONID),
	txnPId_(UNDEF_PARTITIONID),
	containerName_(containerName.name_, eventStackAlloc_),
	stmtId_(0),
	context_(&context),
	statementType_(UNDEF_EVENT_TYPE),
	txnMode_(TransactionManager::AUTO_COMMIT),
	getMode_(TransactionManager::AUTO),
	waitInterval_(0),
	isBegin_(false),
	attribute_(CONTAINER_ATTR_SINGLE),
	containerType_(COLLECTION_CONTAINER),
	hasRowKey_(false),
	founded_(false),
	setted_(false),
	columnInfoList_(eventStackAlloc_),
	indexInfoList_(eventStackAlloc_),
	nullStatisticsList_(eventStackAlloc_),
	indexEntryList_(eventStackAlloc_),
	compositeIndexEntryList_(eventStackAlloc_),
	forceAbort_(false),
	execution_(execution),
	currentSessionId_(UNDEF_SESSIONID),
	isCaseSensitive_(containerName.isCaseSensitive_),
	keyConstraint_(KeyConstraint::getNoLimitKeyConstraint()),
	containerKey_(NULL),
	startTime_(0),
	ec_(&ec),
	sqlString_(eventStackAlloc_),
	approxSize_(-1)
{
	containerKey_ = containerKey;
	txnPId_ = NoSQLUtils::resolvePartitionId(
		eventStackAlloc_, context_->pt_->getPartitionNum(), *containerKey_);
	partitionRevNo_ = context_->pt_->getNewSQLPartitionRevision(txnPId_);
	nodeId_ = context_->pt_->getNewSQLOwner(txnPId_);
}

NoSQLContainer::NoSQLContainer(
	EventContext& ec,
	ContainerId containerId, SchemaVersionId versionId,
	PartitionId pId, NoSQLSyncContext& context, SQLExecution* execution) :
	eventStackAlloc_(ec.getAllocator()),
	nodeId_(UNDEF_NODEID),
	containerId_(containerId),
	versionId_(versionId),
	txnPId_(pId),
	containerName_(eventStackAlloc_),
	stmtId_(0),
	context_(&context),
	statementType_(UNDEF_EVENT_TYPE),
	txnMode_(TransactionManager::AUTO_COMMIT),
	getMode_(TransactionManager::AUTO),
	waitInterval_(0),
	isBegin_(false),
	attribute_(CONTAINER_ATTR_SINGLE),
	containerType_(COLLECTION_CONTAINER),
	hasRowKey_(false),
	founded_(false),
	setted_(false),
	columnInfoList_(eventStackAlloc_),
	indexInfoList_(eventStackAlloc_),
	nullStatisticsList_(eventStackAlloc_),
	indexEntryList_(eventStackAlloc_),
	compositeIndexEntryList_(eventStackAlloc_),
	forceAbort_(false),
	execution_(execution),
	currentSessionId_(UNDEF_SESSIONID),
	isCaseSensitive_(false),
	keyConstraint_(KeyConstraint::getNoLimitKeyConstraint()),
	containerKey_(NULL),
	ec_(&ec),
	sqlString_(eventStackAlloc_),
	approxSize_(-1)
{
	partitionRevNo_ = context_->pt_->getNewSQLPartitionRevision(txnPId_);
	nodeId_ = context_->pt_->getNewSQLOwner(txnPId_);
}

NoSQLContainer::NoSQLContainer(
	EventContext& ec,
	PartitionId pId, NoSQLSyncContext& context, SQLExecution* execution) :
	eventStackAlloc_(ec.getAllocator()),
	nodeId_(UNDEF_NODEID),
	containerId_(UNDEF_CONTAINERID),
	versionId_(UNDEF_SCHEMAVERSIONID),
	txnPId_(pId),
	containerName_(eventStackAlloc_),
	stmtId_(0),
	context_(&context),
	statementType_(UNDEF_EVENT_TYPE),
	txnMode_(TransactionManager::AUTO_COMMIT),
	getMode_(TransactionManager::AUTO),
	waitInterval_(0),
	isBegin_(false),
	attribute_(CONTAINER_ATTR_SINGLE),
	containerType_(COLLECTION_CONTAINER),
	hasRowKey_(false),
	founded_(false),
	setted_(false),
	columnInfoList_(eventStackAlloc_),
	indexInfoList_(eventStackAlloc_),
	nullStatisticsList_(eventStackAlloc_),
	indexEntryList_(eventStackAlloc_),
	compositeIndexEntryList_(eventStackAlloc_),
	forceAbort_(false),
	execution_(execution),
	currentSessionId_(UNDEF_SESSIONID),
	isCaseSensitive_(false),
	keyConstraint_(KeyConstraint::getNoLimitKeyConstraint()),
	containerKey_(NULL),
	startTime_(0),
	ec_(&ec),
	sqlString_(eventStackAlloc_),
	approxSize_(-1)
{
	nodeId_ = context_->pt_->getNewSQLOwner(txnPId_);
}

/*!
	@brief	デストラクタ
*/
NoSQLContainer::~NoSQLContainer() {
	for (size_t pos = 0; pos < columnInfoList_.size(); pos++) {
		ALLOC_DELETE(eventStackAlloc_, columnInfoList_[pos]);
	}
}

/*!
	@brief	コンテナ処理の前準備
*/
void NoSQLContainer::start(ClientSession::Builder& sessionBuilder) {
	try {
		statementType_ = sessionBuilder.getStatementType();
		PartitionTable* pt = context_->pt_;
		nodeId_ = pt->getNewSQLOwner(txnPId_);
		clientSession_.setUpStatementId(sessionBuilder);
		++stmtId_;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void NoSQLContainer::sendMessage(EventEngine& ee, Event& request) {
	if (nodeId_ == 0) {
		const NodeDescriptor& nd = ee.getSelfServerND();
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

bool NoSQLContainer::executeCommand(Event& request,
	bool isSync, util::XArray<uint8_t>& response) {
	TransactionService* txnSvc = context_->txnSvc_;
	SQLService* sqlSvc = context_->sqlSvc_;
	try {
		startTime_ = sqlSvc->getEE()->getMonotonicTime();
		EventMonotonicTime currentTime = startTime_;
		int32_t counter = 0;
		int32_t waitInterval = 15 * 1000;
		int32_t timeoutInterval = sqlSvc->getNoSQLFailoverTimeout();
		if (timeoutInterval > context_->timeoutInterval_) {
			timeoutInterval = context_->timeoutInterval_;
		}
		EventMonotonicTime limitTime = startTime_ + timeoutInterval;
		util::Exception checkException;
		bool isExceptionOccured;
		while (currentTime < limitTime) {
			isExceptionOccured = false;
			try {
				if (nodeId_ == UNDEF_NODEID) {
					waitInterval_ = DEFAULT_NOSQL_FAILOVER_WAIT_TIME;
					ClusterService* clsSvc = context_->sqlSvc_->getClusterService();
					clsSvc->requestRefreshPartition(*ec_);
					context_->wait(DEFAULT_NOSQL_FAILOVER_WAIT_TIME);
				}
				else {
					if (isSync) {
						context_->get(request, eventStackAlloc_,
							this, waitInterval, timeoutInterval, response);
					}
					else {
						sendMessage(*txnSvc->getEE(), request);
					}
					if (execution_) {
						execution_->getProfilerInfo().incPendingTime(
							sqlSvc->getEE()->getMonotonicTime() - startTime_);
					}
					return true;
				}
			}
			catch (std::exception& e) {
				UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, e, "");
				isExceptionOccured = true;
				checkException = GS_EXCEPTION_CONVERT(e, "");
				int32_t errorCode = checkException.getErrorCode();
				if (isDenyException(errorCode) && isRetryEvent()) {
					waitInterval_ = DEFAULT_NOSQL_FAILOVER_WAIT_TIME;
					ClusterService* clsSvc = context_->sqlSvc_->getClusterService();
					clsSvc->requestRefreshPartition(*ec_);
					context_->wait(DEFAULT_NOSQL_FAILOVER_WAIT_TIME);
				}
				else {
					GS_RETHROW_USER_OR_SYSTEM(e, "");
				}
			}
			sqlSvc->checkActiveStatus();
			updateCondition();
			currentTime = sqlSvc->getEE()->getMonotonicTime();
			if (counter % 5 == 0) {
				GS_TRACE_WARNING(SQL_SERVICE, GS_TRACE_SQL_FAILOVER_WORKING,
					"Failover working, totalDuration=" << (currentTime - startTime_)
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
					catch (std::exception& e) {
						GS_RETHROW_USER_ERROR_CODED(GS_ERROR_JOB_CANCELLED, e,
							GS_EXCEPTION_MERGE_MESSAGE(e, "Cancel SQL, clientId="
								<< context_->clientId_ << ", location=executeCommand, reason=Failover timeout, totalDuration="
								<< (currentTime - startTime_)
								<< ", limitDuration=" << (limitTime - startTime_)
								<< ", pId=" << txnPId_ << ", nodeId=" << nodeId_
								<< ", revision=" << partitionRevNo_
								<< ", event=" << EventTypeUtility::getEventTypeName(statementType_) << ")"));
					}
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_PARTITION_NOT_AVAILABLE,
						"Cancel SQL, clientId=" << context_->clientId_
						<< ", location=executeCommand, reason=Failover timeout, totalDuration="
						<< (currentTime - startTime_)
						<< ", limitDuration=" << (limitTime - startTime_)
						<< ", pId=" << txnPId_ << ", nodeId=" << nodeId_
						<< ", revision=" << partitionRevNo_
						<< ", event=" << EventTypeUtility::getEventTypeName(statementType_) << ")");
				}
			}
		}

		if (execution_) {
			execution_->getProfilerInfo().incPendingTime(
				sqlSvc->getEE()->getMonotonicTime() - startTime_);
		}

		return true;
	}
	catch (std::exception& e) {
		if (execution_) {
			execution_->getProfilerInfo().incPendingTime(
				sqlSvc->getEE()->getMonotonicTime() - startTime_);
		}

		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief	コンテナを取得する
*/
void NoSQLContainer::getContainerInfo(NoSQLStoreOption& option) {
	try {
		EventType eventType = SQL_GET_CONTAINER;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		SQLGetContainerHandler::encodeSubCommand(
				out, SQLGetContainerHandler::SUB_CMD_GET_CONTAINER);
		StatementHandler::encodeContainerKey(out, *containerKey_);
		bool isContainerLock = false;
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, isContainerLock);
		StatementHandler::encodeUUID<EventByteOutStream>(out, context_->clientId_.uuid_,
			TXN_CLIENT_UUID_BYTE_SIZE);
		StatementHandler::encodeLongData<EventByteOutStream, SessionId>(out,
			context_->clientId_.sessionId_);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);

		SQLVariableSizeGlobalAllocator &globalVarAlloc =
				context_->getGlobalAllocator();
		const uint8_t *syncBinaryData = response.data();
		const size_t syncBinarySize = response.size();

		util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(syncBinaryData, syncBinarySize));

		util::AllocUniquePtr<TableSchemaInfo> schemaInfoPtr;
		SQLGetContainerHandler::EstimationList estimationList(eventStackAlloc_);

		SQLGetContainerHandler::decodeResult(
				in, eventStackAlloc_, globalVarAlloc, schemaInfoPtr,
				estimationList);

		founded_ = (schemaInfoPtr.get() != NULL && schemaInfoPtr->founded_);
		if (founded_) {
			if (!setted_) {
				TableSchemaInfo &schemaInfo = *schemaInfoPtr;
				TableContainerInfo& containerInfo = schemaInfo.containerInfoList_[0];
				attribute_
					= static_cast<ContainerAttribute>(schemaInfo.containerAttr_);
				containerType_
					= static_cast<ContainerType>(schemaInfo.containerType_);
				versionId_ = containerInfo.versionId_;
				containerId_ = containerInfo.containerId_;
				approxSize_ = containerInfo.approxSize_;
				hasRowKey_ = schemaInfo.hasRowKey_;
				for (size_t columnId = 0;
					columnId < schemaInfo.columnInfoList_.size(); columnId++) {
					NoSQLColumn* column = ALLOC_NEW(eventStackAlloc_)
						NoSQLColumn(eventStackAlloc_);
					column->name_
						= schemaInfo.columnInfoList_[columnId].name_.c_str();
					column->type_
						= schemaInfo.columnInfoList_[columnId].type_;
					column->option_
						= schemaInfo.columnInfoList_[columnId].option_;
					column->tupleType_
						= schemaInfo.columnInfoList_[columnId].tupleType_;

					columnInfoList_.push_back(column);
					for (size_t pos = 0;
						pos < schemaInfo.columnInfoList_[columnId].indexInfo_.size(); pos++) {
						IndexInfo indexInfo(eventStackAlloc_);
						indexInfo.columnIds_.push_back(
								static_cast<uint32_t>(columnId));
						indexInfo.mapType
							= schemaInfo.columnInfoList_[columnId].indexInfo_[pos].indexType_;
						indexInfo.indexName_
							= schemaInfo.columnInfoList_[columnId].indexInfo_[pos].indexName_.c_str();
						indexEntryList_.push_back(indexInfo);
					}
				}
				if (indexInfoList_.empty()) {
					indexInfoList_.resize(schemaInfo.columnInfoList_.size(), 0);
				}
				for (size_t pos = 0; pos < schemaInfo.indexInfoList_.size(); pos++) {
					indexInfoList_[pos] = schemaInfo.indexInfoList_[pos];
				}
				sqlString_ = schemaInfo.sqlString_.c_str();
				setted_ = true;
			}
		}
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

void NoSQLContainer::estimateIndexSearchSize(
		const SQLIndexStatsCache::KeyList &keyList, NoSQLStoreOption &option,
		util::Vector<int64_t> &estimationList) {
	try {
		EventType eventType = SQL_GET_CONTAINER;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		SQLGetContainerHandler::encodeSubCommand(
				out,
				SQLGetContainerHandler::SUB_CMD_ESTIMATE_INDEX_SEARCH_SIZE);
		encodeIndexEstimationKeyList(out, keyList);

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);

		SQLVariableSizeGlobalAllocator &globalVarAlloc =
				context_->getGlobalAllocator();
		const uint8_t *syncBinaryData = response.data();
		const size_t syncBinarySize = response.size();

		util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(syncBinaryData, syncBinarySize));

		util::AllocUniquePtr<TableSchemaInfo> schemaInfoPtr;

		SQLGetContainerHandler::decodeResult(
				in, eventStackAlloc_, globalVarAlloc, schemaInfoPtr,
				estimationList);

		if (keyList.size() != estimationList.size()) {
			estimationList.resize(keyList.size(), -1);
		}
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

void NoSQLUtils::getAffinitySubValue(
	util::StackAllocator& alloc, NodeAffinityNumber affinity,
	util::String& affinityValue) {

	int32_t affinityStrLen = AFFINITY_STRING_MAX_LENGTH;
	util::NormalOStringStream strstrm;
	strstrm << affinity;
	util::String digitStr(strstrm.str().c_str(),
		strlen(strstrm.str().c_str()), alloc);
	const uint32_t crcValue = util::CRC32::calculate(
		digitStr.c_str(), digitStr.length());
	uint32_t affinityNo = crcValue;
	char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
	memset(affinityStr, '\0', AFFINITY_STRING_MAX_LENGTH);
	for (int32_t i = AFFINITY_STRING_MAX_LENGTH - 1; i >= 0; i--) {
		affinityStr[i] = static_cast<char8_t>('0' + (affinityNo % 10));
		affinityNo /= 10;
	}
	affinityValue.assign(affinityStr, affinityStrLen);
}

bool TableExpirationSchemaInfo::updateSchema(util::StackAllocator& alloc,
	util::XArray<uint8_t>& binarySchemaInfo,
	util::XArray<uint8_t>& newBinarySchemaInfo) {
	if (subContainerAffinity_ == -1 || dataAffinityPos_ == 0) {
		return false;
	}
	else {
		if (dataAffinityPos_ == SIZE_MAX) {
			util::XArray<uint8_t> remaining(alloc);
			util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(binarySchemaInfo.data(), binarySchemaInfo.size()));
			util::XArrayOutStream<> arrayOut(newBinarySchemaInfo);
			util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
			int32_t columnNum;
			in >> columnNum;
			out << columnNum;
			for (int32_t i = 0; i < columnNum; i++) {
				int32_t columnNameLen;
				in >> columnNameLen;
				out << columnNameLen;
				util::XArray<uint8_t> columnName(alloc);
				columnName.resize(static_cast<size_t>(columnNameLen));
				in >> std::make_pair(columnName.data(), columnNameLen);
				out << std::make_pair(columnName.data(), columnNameLen);
				int8_t typeOrdinal;
				in >> typeOrdinal;
				out << typeOrdinal;
				uint8_t flags;
				in >> flags;
				out << flags;
			}
			int16_t rowKeyNum = 0;
			in >> rowKeyNum;
			out << rowKeyNum;
			int16_t keyColumnId = 0;
			for (int16_t pos = 0; pos < rowKeyNum; pos++) {
				in >> keyColumnId;
				out << keyColumnId;
			}
			int32_t affinityStrLen = 0;
			in >> affinityStrLen;
			if (affinityStrLen == 0) {
				return false;
			}
			util::XArray<uint8_t> affinityValue(alloc);
			affinityValue.resize(static_cast<size_t>(affinityStrLen));
			in >> std::make_pair(affinityValue.data(), affinityStrLen);
			StatementHandler::decodeBinaryData<util::ArrayByteInStream>(in, remaining, true);

			util::String  affinityValueStr(alloc);
			NoSQLUtils::getAffinitySubValue(alloc, subContainerAffinity_, affinityValueStr);

			affinityStrLen = static_cast<int32_t>(affinityValueStr.size());
			out << affinityStrLen;
			out << std::make_pair(affinityValueStr.c_str(), affinityStrLen);

			out << std::make_pair(remaining.data(), remaining.size());
			return true;
		}
	}
	return false;
}

/*!
	@brief	コンテナを作成する
*/
void NoSQLContainer::putContainer(TableExpirationSchemaInfo& info,

	RenameColumnSchemaInfo& renameColInfo, 
	util::XArray<uint8_t>& binarySchemaInfo, bool modifiable,
	NoSQLStoreOption& option) {
	try {
		EventType eventType = PUT_CONTAINER;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeContainerKey(out, *containerKey_);
		containerType_ = option.containerType_;
		out << containerType_;
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, modifiable);

		StatementHandler::encodeBinaryData<EventByteOutStream>(out, binarySchemaInfo.data(),
			binarySchemaInfo.size());

		info.encode(out);

		renameColInfo.encode(out);

		out << static_cast<int32_t>(MessageSchema::OPTION_END);

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}
		int32_t columnNum = 0;
		int16_t keyColumnId = 0;

		if (syncBinarySize != 0) {
			founded_ = true;
			util::ArrayByteInStream in =
				util::ArrayByteInStream(
					util::ArrayInStream(binarySchemaInfo.data(),
						binarySchemaInfo.size()));
			in >> columnNum;
			for (int32_t i = 0; i < columnNum; i++) {
				NoSQLColumn* column = ALLOC_NEW(eventStackAlloc_)
					NoSQLColumn(eventStackAlloc_);
				in >> column->name_;

				int8_t typeOrdinal;
				uint8_t flags;
				in >> typeOrdinal;
				in >> flags;
				const bool forArray = MessageSchema::getIsArrayByFlags(flags);
				if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
						typeOrdinal, forArray, false, column->type_)) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
				}

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
				hasRowKey_ = false;
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
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

void NoSQLContainer::putLargeContainer(
	util::XArray<uint8_t>& binarySchemaInfo, bool modifiable,
	NoSQLStoreOption& option, util::XArray<uint8_t>* schema,
	util::XArray<uint8_t>* binary) {
	try {
		EventType eventType = PUT_LARGE_CONTAINER;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeContainerKey(out, *containerKey_);
		containerType_ = option.containerType_;
		out << containerType_;
		modifiable = false;
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, modifiable);
		uint32_t size;
		size = static_cast<uint32_t>(binarySchemaInfo.size());
		out << size;
		StatementHandler::encodeBinaryData<EventByteOutStream>(
			out, binarySchemaInfo.data(), binarySchemaInfo.size());
		if (schema) {
			size = static_cast<uint32_t>(schema->size());
			out << size;
			StatementHandler::encodeBinaryData<EventByteOutStream>(
				out, schema->data(), schema->size());
		}
		if (binary) {
			size = static_cast<uint32_t>(binary->size());
			out << size;
			StatementHandler::encodeBinaryData<EventByteOutStream>(
				out, binary->data(), binary->size());
		}

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);

		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();

		if (syncBinarySize == 0) {
			return;
		}
		int32_t columnNum = 0;
		int16_t keyColumnId = 0;
		if (syncBinarySize != 0) {
			util::ArrayByteInStream in =
				util::ArrayByteInStream(
					util::ArrayInStream(binarySchemaInfo.data(),
						binarySchemaInfo.size()));
			in >> columnNum;
			for (int32_t i = 0; i < columnNum; i++) {
				NoSQLColumn* column = ALLOC_NEW(eventStackAlloc_)
					NoSQLColumn(eventStackAlloc_);
				in >> column->name_;

				int8_t typeOrdinal;
				uint8_t flags;
				in >> typeOrdinal;
				in >> flags;
				const bool forArray = MessageSchema::getIsArrayByFlags(flags);
				if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
						typeOrdinal, forArray, false, column->type_)) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
				}

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
				hasRowKey_ = false;
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
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}


/*!
	@brief	コンテナを削除する
*/
void NoSQLContainer::dropContainer(NoSQLStoreOption& option) {
	try {
		EventType eventType = DROP_CONTAINER;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeContainerKey(out, *containerKey_);
		out << containerType_;

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, option.isSync_, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	ユーザの追加、或いはパスワード変更を行う
	@note SYSTEM_TABLE_USERS_NAME
*/
void NoSQLContainer::putUser(const char* userName, const char* password,
	bool modifiable, NoSQLStoreOption& option, bool isRole) {
	try {
		EventType eventType = PUT_USER; 
		if (userName == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_USER, "Target userName is NULL");
		}
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeStringData<EventByteOutStream>(out, userName);
		int8_t prop = 0;
		out << prop;
		DbUserHandler::checkPasswordLength(password);
		bool hasDigest = (password != NULL);
		if (isRole) {
			hasDigest = false;
		}
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, hasDigest);
		if (hasDigest) {
			char digest[SHA256_DIGEST_STRING_LENGTH + 1];
			SHA256_Data(reinterpret_cast<const uint8_t*>(password),
				strlen(password), digest);
			digest[SHA256_DIGEST_STRING_LENGTH] = '\0';
			StatementHandler::encodeStringData<EventByteOutStream>(out, digest);
		}
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, modifiable);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	ユーザの追加、或いはパスワード変更を行う
	@note SYSTEM_TABLE_USERS_NAME
*/
void NoSQLContainer::dropUser(const char* userName,
	NoSQLStoreOption& option) {
	try {
		EventType eventType = DROP_USER;
		if (userName == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_USER, "Target userName is NULL");
		}
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeStringData<EventByteOutStream>(out, userName);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	データベース生成
*/
void NoSQLContainer::createDatabase(
	const char* dbName, NoSQLStoreOption& option) {
	execDatabaseProcInternal(PUT_DATABASE, dbName, option);
}

/*!
	@brief	データベース削除
*/
void NoSQLContainer::dropDatabase(
	const char* dbName, NoSQLStoreOption& option) {
	execDatabaseProcInternal(DROP_DATABASE, dbName, option);
}

/*!
	@brief	データベース生成/削除
*/
void NoSQLContainer::execDatabaseProcInternal(EventType eventType,
	const char* dbName, NoSQLStoreOption& option) {
	try {
		if (dbName == NULL || (dbName != NULL && strlen(dbName) == 0)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_DATABASE, "");
		}
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, SYSTEM_CONTAINER_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeStringData<EventByteOutStream>(out, dbName);
		int8_t prop = 0;
		out << prop;
		int32_t privilegeNum = 0;
		out << privilegeNum;
		bool modifiable = false;
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, modifiable);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	権限付与
*/
void NoSQLContainer::grant(const char* userName, const char* dbName,
	int32_t accessType, NoSQLStoreOption& option) {
	execPrivilegeProcessInternal(
		PUT_PRIVILEGE, userName, dbName, accessType, option);
}

/*!
	@brief	権限剥奪
*/
void NoSQLContainer::revoke(const char* userName, const char* dbName,
	int32_t accessType, NoSQLStoreOption& option) {
	execPrivilegeProcessInternal(
		DROP_PRIVILEGE, userName, dbName, accessType, option);
}

/*!
	@brief	権限付与/剥奪
*/
void NoSQLContainer::execPrivilegeProcessInternal(EventType eventType,
	const char* userName, const char* dbName,
	int32_t accessType, NoSQLStoreOption& option) {
	try {
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, SYSTEM_CONTAINER_PARTITION_ID); 
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeStringData<EventByteOutStream>(out, dbName);
		int8_t property = 0;
		out << property;
		int32_t privilegeNum = 1;
		out << privilegeNum;
		const char* privilege;
		if (accessType == 0) {
			privilege = "ALL";
		}
		else {
			privilege = "READ";
		}
		for (int32_t i = 0; i < privilegeNum; ++i) {
			StatementHandler::encodeStringData<EventByteOutStream>(out, userName);
			StatementHandler::encodeStringData<EventByteOutStream>(out, privilege);
		}
		StatementMessage::OptionSet optionalRequest(eventStackAlloc_);
		optionalRequest.set<StatementMessage::Options::FEATURE_VERSION>(StatementMessage::FEATURE_V4_3);
		optionalRequest.encode(out);

		accessType = 0;
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	TQLを実行する
	@note v1.5と異なり、fix/varを直接返す
*/
void NoSQLContainer::executeSyncQuery(
	const char* query, NoSQLStoreOption& option,
	util::XArray<uint8_t>& fixedPart,
	util::XArray<uint8_t>& varPart, int64_t& rowCount, bool& hasRowKey) {
	try {
		EventType eventType = QUERY_TQL;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		out << INT64_MAX;
		ResultSize partialSize_ = INT64_MAX;
		out << partialSize_;
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, false);
		int32_t entryNum = 0;
		out << entryNum;
		StatementHandler::encodeStringData<EventByteOutStream>(out, query);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}
		util::ArrayByteInStream in =
			util::ArrayByteInStream(util::ArrayInStream(syncBinaryData, syncBinarySize));

		int8_t resultType;
		in >> resultType;

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
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	ロウを追加、若しくは更新を行う
*/
void NoSQLContainer::putRow(util::XArray<uint8_t>& fixedPart,
	util::XArray<uint8_t>& varPart, RowId rowId, NoSQLStoreOption& option) {
	try {
		EventType eventType;
		if (rowId == UNDEF_ROWID) {
			eventType = PUT_ROW;
		}
		else {
			eventType = UPDATE_ROW_BY_ID;
		}
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		if (rowId != UNDEF_ROWID) {
			out << rowId;
		}
		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(fixedPart.data()), fixedPart.size());
		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(varPart.data()), varPart.size());
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

void NoSQLContainer::updateRowSet(
	util::XArray<uint8_t>& fixedPart, util::XArray<uint8_t>& varPart,
	util::XArray<RowId>& rowIdList, NoSQLStoreOption& option) {
	try {
		EventType eventType = UPDATE_MULTIPLE_ROWS_BY_ID_SET;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		uint64_t numRow = static_cast<uint64_t>(rowIdList.size());
		out << numRow;
		for (size_t pos = 0; pos < rowIdList.size();pos++) {
			out << rowIdList[pos];
		}

		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(fixedPart.data()), fixedPart.size());
		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(varPart.data()), varPart.size());
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	ロウ集合を追加する
*/
void NoSQLContainer::putRowSet(
	util::XArray<uint8_t>& fixedPart, util::XArray<uint8_t>& varPart,
	uint64_t numRow, NoSQLStoreOption& option) {
	try {
		EventType eventType = PUT_MULTIPLE_ROWS;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		out << numRow;
		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(fixedPart.data()), fixedPart.size());
		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(varPart.data()), varPart.size());
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	ロウIDをキーにロウを更新する
	@note 実際はputRowと同じなのだが、エンコードの都合で分けた
*/
void NoSQLContainer::updateRow(RowId rowId,
	util::XArray<uint8_t>& rowData, NoSQLStoreOption& option) {
	try {
		EventType eventType = UPDATE_ROW_BY_ID;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		out << rowId;
		StatementHandler::encodeBinaryData<EventByteOutStream>(out, rowData.data(), rowData.size());
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	ロウキーIDからロウを削除する
*/
void NoSQLContainer::deleteRowSet(
	util::XArray<RowId>& rowIdList, NoSQLStoreOption& option) {
	try {
		EventType eventType = REMOVE_MULTIPLE_ROWS_BY_ID_SET;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		uint64_t size = rowIdList.size();
		out << size;
		for (size_t pos = 0; pos < rowIdList.size();pos++) {
			out << rowIdList[pos];
		}
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	索引を生成する
*/
void NoSQLContainer::createIndex(const char* indexName, MapType mapType,
	util::Vector<uint32_t>& columnIds, NoSQLStoreOption& option) {
	try {
		EventType eventType = CREATE_INDEX;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		IndexInfo indexInfo(eventStackAlloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.columnIds_.assign(columnIds.begin(), columnIds.end());
		indexInfo.mapType = mapType;
		StatementHandler::encodeIndexInfo(out, indexInfo);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, option.isSync_, response);
		if (indexInfoList_.empty()) {
			indexInfoList_.resize(getColumnSize(), 0);
		}
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

bool NoSQLContainer::createLargeIndex(const char* indexName,
	util::Vector<util::String>& columnNameList,
	MapType mapType,
	util::Vector<ColumnId>& columnIdList,
	NoSQLStoreOption& option) {
	try {
		EventType eventType = CREATE_LARGE_INDEX;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		IndexInfo indexInfo(eventStackAlloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.columnIds_ = columnIdList;
		indexInfo.mapType = mapType;
		StatementHandler::encodeIndexInfo(out, indexInfo);
		StatementHandler::encodeStringData<EventByteOutStream>(out, columnNameList[0]);
		if (columnIdList.size() > 1) {
			for (size_t pos = 1; pos < columnIdList.size(); pos++) {
				StatementHandler::encodeStringData<EventByteOutStream>(out, columnNameList[pos].c_str());
			}
		}

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return false;
		}
		util::ArrayByteInStream in =
			util::ArrayByteInStream(util::ArrayInStream(syncBinaryData, syncBinarySize));
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
	catch (std::exception& e) {
		checkException(statementType_, e);
		return false;
	}
}

bool NoSQLContainer::createLargeIndex(const char* indexName,
	const char* columnName, MapType mapType, uint32_t columnId,
	NoSQLStoreOption& option) {
	try {
		EventType eventType = CREATE_LARGE_INDEX;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		IndexInfo indexInfo(eventStackAlloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.columnIds_.push_back(columnId);
		indexInfo.mapType = mapType;
		StatementHandler::encodeIndexInfo(out, indexInfo);
		StatementHandler::encodeStringData<EventByteOutStream>(out, columnName);

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return false;
		}
		util::ArrayByteInStream in =
			util::ArrayByteInStream(util::ArrayInStream(syncBinaryData, syncBinarySize));
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
	catch (std::exception& e) {
		checkException(statementType_, e);
		return false;
	}
}

/*!
	@brief	索引を削除する
*/
void NoSQLContainer::dropIndex(const char* indexName, NoSQLStoreOption& option) {
	try {
		EventType eventType = DELETE_INDEX;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		IndexInfo indexInfo(eventStackAlloc_);
		indexInfo.indexName_ = indexName;
		indexInfo.mapType = 0;
		StatementHandler::encodeIndexInfo(out, indexInfo);
		uint8_t anyNameMatches = 0;
		uint8_t anyTypeMatches = 1;
		out << anyNameMatches;
		out << anyTypeMatches;

		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, option.isSync_, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	コミットを実行する
*/
void NoSQLContainer::commit(NoSQLStoreOption& option) {
	commitOrRollbackInternal(COMMIT_TRANSACTION, option);
}

/*!
	@brief	アボートを実行する
*/
void NoSQLContainer::abort(NoSQLStoreOption& option) {
	commitOrRollbackInternal(ABORT_TRANSACTION, option);
}

/*!
	@brief	コミット若しくはアボートを実行する
*/
void NoSQLContainer::commitOrRollbackInternal(EventType eventType,
	NoSQLStoreOption& option) {
	try {
		if (enableCommitOrAbort()) {
			ClientSession::Builder sessionBuilder(this, eventType);
			start(sessionBuilder);
			Event request(*ec_, eventType, getPartitionId());
			EventByteOutStream out = request.getOutStream();
			encodeFixedPart(out, eventType);
			encodeOptionPart(out, eventType, option);
			util::XArray<uint8_t> response(eventStackAlloc_);
			executeCommand(request, option.isSync_, response);
			if (!forceAbort_) {
				closeContainer(option);
			}
			isBegin_ = false;
		}
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

void NoSQLContainer::createTransactionContext(NoSQLStoreOption& option) {
	EventType eventType = CREATE_TRANSACTION_CONTEXT;
	try {
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
		closeContainer(option);
		isBegin_ = true;
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief ステートメントをクローズする
	@return NoSQLStatementId 実行対象のステートメントID
*/
void NoSQLContainer::closeContainer(NoSQLStoreOption& option) {
	try {
		EventType eventType = CLOSE_SESSION;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, option.isSync_, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

/*!
	@brief	指定カラム名に対するカラムIDを取得する
*/
int32_t NoSQLContainer::getColumnId(util::StackAllocator& alloc,
	const NameWithCaseSensitivity& columnName) {
	int32_t indexColId = -1;
	int32_t realIndexColId = 0;

	if (columnName.isCaseSensitive_) {
		for (int32_t col = 0; col < static_cast<int32_t>(columnInfoList_.size()); col++) {
			if (!strcmp(columnInfoList_[col]->name_.c_str(), columnName.name_)) {
				indexColId = realIndexColId;
				break;
			}
			realIndexColId++;
		}
	}
	else {
		const util::String& normalizeSpecifiedColumnName =
			normalizeName(alloc, columnName.name_);
		for (int32_t col = 0; col < static_cast<int32_t>(columnInfoList_.size()); col++) {
			const util::String& normalizeColumnName =
				normalizeName(alloc, columnInfoList_[col]->name_.c_str());
			if (!normalizeColumnName.compare(normalizeSpecifiedColumnName)) {
				indexColId = realIndexColId;
				break;
			}
			realIndexColId++;
		}
	}
	return indexColId;
}

bool NoSQLContainer::getIndexEntry(
		util::StackAllocator& alloc,
		const NameWithCaseSensitivity& indexName, ColumnId columnId, MapType indexType,
		util::Vector<IndexInfoEntry>& entryList) {
	UNUSED_VARIABLE(indexType);

	bool existsSameColumn = false;

	if (indexName.isCaseSensitive_) {
		for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
			if (!strcmp(indexName.name_, indexEntryList_[pos].indexName_.c_str())) {
				IndexInfoEntry entry;
				entry.columnId_ = indexEntryList_[pos].columnIds_[0];
				entry.indexType_ = indexEntryList_[pos].mapType;
				entryList.push_back(entry);
				break;
			}
			if (indexEntryList_[pos].columnIds_[0] == columnId) {
				existsSameColumn = true;
			}
		}
	}
	else {
		const util::String& normalizeQueryName = normalizeName(alloc, indexName.name_);
		for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
			const util::String& normalizeIndexName =
				normalizeName(alloc, indexEntryList_[pos].indexName_.c_str());
			if (!normalizeIndexName.compare(normalizeQueryName)) {
				IndexInfoEntry entry;
				entry.columnId_ = indexEntryList_[pos].columnIds_[0];
				entry.indexType_ = indexEntryList_[pos].mapType;
				entryList.push_back(entry);
				break;
			}
			if (indexEntryList_[pos].columnIds_[0] == columnId) {
				existsSameColumn = true;
			}
		}
	}

	return existsSameColumn;
}

/*!
	@brief	固定長部分をエンコードする
*/
void NoSQLContainer::encodeFixedPart(EventByteOutStream& out, EventType type) {

	StatementMessage::FixedRequest::Source source(getPartitionId(), type);
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
				currentSessionId_ = execution_->getContext().incCurrentSessionId();
			}
			request.cxtSrc_.stmtId_ = stmtId_;
			request.cxtSrc_.containerId_ = containerId_;
			request.schemaVersionId_ = versionId_;
			request.cxtSrc_.getMode_ = getMode_;
			request.cxtSrc_.txnMode_ = txnMode_;
			memcpy(request.clientId_.uuid_,
				context_->clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
			request.clientId_.sessionId_ = currentSessionId_;
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED,
				"Unsupported stmtType=" << static_cast<int32_t>(type));
		}
		request.encode(out);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief	オプション部をエンコードする
*/
void NoSQLContainer::encodeOptionPart(EventByteOutStream& out, EventType type,
	NoSQLStoreOption& option) {
	try {
		util::StackAllocator::Scope scope(eventStackAlloc_);
		StatementMessage::OptionSet optionalRequest(eventStackAlloc_);

		switch (type) {
		case ABORT_TRANSACTION:
		{
			if (forceAbort_) {
				optionalRequest.set<StatementMessage::Options::DDL_TRANSACTION_MODE>(true);
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
				(SQLGetContainerHandler::LATEST_FEATURE_VERSION);
			optionalRequest.set<
				StatementMessage::Options::ACCEPTABLE_FEATURE_VERSION>
				(SQLGetContainerHandler::LATEST_FEATURE_VERSION);
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
		StatementMessage::UUIDObject uuid(context_->clientId_.uuid_);
		optionalRequest.set<
			StatementMessage::Options::UUID>(uuid);
		optionalRequest.set<
			StatementMessage::Options::QUERY_ID>
			(context_->clientId_.sessionId_);

		optionalRequest.set<
			StatementMessage::Options::USER_TYPE>
			(context_->userType_);

		DatabaseId dbVersion;
		const char* dbName;
		if (option.isSystemDb_) {
			dbVersion = GS_SYSTEM_DB_ID;
			dbName = GS_SYSTEM;
		}
		else {
			dbVersion = context_->dbId_;
			dbName = context_->dbName_;
		}
		optionalRequest.set<
			StatementMessage::Options::DB_VERSION_ID>(dbVersion);
		optionalRequest.set<
			StatementMessage::Options::DB_NAME>(dbName);

		optionalRequest.set<
			StatementMessage::Options::STATEMENT_TIMEOUT_INTERVAL>
			(context_->timeoutInterval_);

		optionalRequest.set<
			StatementMessage::Options::TXN_TIMEOUT_INTERVAL>
			(context_->txnTimeoutInterval_);

		optionalRequest.set<
			StatementMessage::Options::REPLY_PID>
			(context_->replyPId_);

		optionalRequest.set<
			StatementMessage::Options::REPLY_EVENT_TYPE>
			(SQL_RECV_SYNC_REQUEST);

		optionalRequest.set<
			StatementMessage::Options::FOR_SYNC>(option.isSync_);

		if (!option.isSync_) {
			optionalRequest.set<
				StatementMessage::Options::SUB_CONTAINER_ID>
				(option.subContainerId_);
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
		CreateDropIndexMode indexMode;
		if (option.ifNotExistsOrIfExists_) {
			indexMode = INDEX_MODE_SQL_EXISTS;
		}
		else {
			indexMode = INDEX_MODE_SQL_DEFAULT;
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

		if (option.applicationName_ && strlen(option.applicationName_) > 0) {
			optionalRequest.set<
				StatementMessage::Options::APPLICATION_NAME>
				(option.applicationName_);
		}
		if (option.storeMemoryAgingSwapRate_ != TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE) {
			optionalRequest.set<
				StatementMessage::Options::STORE_MEMORY_AGING_SWAP_RATE>
				(option.storeMemoryAgingSwapRate_);
		}
		if (!option.timezone_.isEmpty()) {
			optionalRequest.set<StatementMessage::Options::TIME_ZONE_OFFSET>(
				option.timezone_);
		}
		optionalRequest.encode(out);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief	オプション種別の追加
*/
void NoSQLContainer::OptionalRequest::putType(
	EventByteOutStream& reqOut, StatementHandler::OptionType type) {
	reqOut << static_cast<int16_t>(type);
}

/*!
	@brief	オプション領域をクリア
*/
void NoSQLContainer::OptionalRequest::clear() {
	optionSet_.clear();
}

/*!
	@brief	オプション領域をセット
*/
void NoSQLContainer::OptionalRequest::set(
	StatementHandler::OptionType type) {
	optionSet_.insert(type);
}

void TableSchemaInfo::setContainerInfo(
	int32_t pos, TableContainerInfo& containerInfo, NodeAffinityNumber affinity) {
	containerInfo.affinity_ = affinity;
	containerInfo.pos_ = pos;
	containerInfoList_.push_back(containerInfo);
}


/*!
	@brief	コンテナ情報をセットする
	@note 実データから
*/
TableContainerInfo* TableSchemaInfo::setContainerInfo(
	int32_t pos, NoSQLContainer& container, NodeAffinityNumber affinity) {
	ColumnIdList* columnIdList = NULL;
	try {
		TableContainerInfo containerInfo;
		containerInfo.versionId_ = container.getSchemaVersionId();
		containerInfo.containerId_ = container.getContainerId();
		containerInfo.pId_ = container.getPartitionId();
		containerInfo.affinity_ = affinity;
		containerInfo.pos_ = pos;
		containerInfo.approxSize_ = container.containerApproxSize();
		containerInfoList_.push_back(containerInfo);

		if (pos == 0) {
			setColumnInfo(container);

			containerType_ = container.getContainerType();
			if (container.getContainerAttribute() == CONTAINER_ATTR_VIEW) {
				sqlString_ = container.getSQLString();
			}

			if (containerType_ == TIME_SERIES_CONTAINER) {
				setPrimaryKeyIndex(ColumnInfo::ROW_KEY_COLUMN_ID);
			}
			util::Vector<IndexInfo>& indexInfoLists = container.getCompositeIndex();
			for (size_t pos = 0; pos < indexInfoLists.size(); pos++) {
				columnIdList = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) ColumnIdList(globalVarAlloc_);
				for (size_t columnPos = 0;
					columnPos < indexInfoLists[pos].columnIds_.size(); columnPos++) {
					columnIdList->push_back(indexInfoLists[pos].columnIds_[columnPos]);
				}
				compositeColumnIdList_.push_back(columnIdList);
			}
		}
		hasRowKey_ = container.hasRowKey();
		if (indexInfoList_.empty()) {
			indexInfoList_.resize(container.getColumnSize(), 0);
		}
		if (indexInfoList_.size() < container.getIndexInfoList().size()) {
			indexInfoList_.resize(container.getIndexInfoList().size(), 0);
		}
		if (pos == 0) {
			for (size_t idx = 0; idx < container.getIndexInfoList().size(); idx++) {
				indexInfoList_[idx] = container.getIndexInfoList()[idx];
			}
		}
		else {
			for (size_t idx = 0; idx < container.getIndexInfoList().size(); idx++) {
				indexInfoList_[idx] &= container.getIndexInfoList()[idx];
			}
		}
		return &containerInfoList_.back();
	}
	catch (std::exception& e) {
		if (columnIdList) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, columnIdList);
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief	カラム情報をセットする
	@note 実データから
*/
void TableSchemaInfo::setColumnInfo(NoSQLContainer& container) {
	size_t fixDataOffset = 0;
	uint16_t varDataPos = 0;
	if (nosqlColumnInfoList_ == NULL) {
		nosqlColumnInfoList_ = static_cast<ColumnInfo*>(
			globalVarAlloc_.allocate(
				sizeof(ColumnInfo) * container.getColumnSize()));
		memset(nosqlColumnInfoList_, 0,
			sizeof(ColumnInfo) * container.getColumnSize());
	}
	fixDataOffset = ValueProcessor::calcNullsByteSize(
		static_cast<uint32_t>(container.getColumnSize()));
	size_t pos = 0;
	for (pos = 0; pos < container.getColumnSize(); pos++) {
		if (NoSQLUtils::isVariableType(container.getColumnType(pos))) {
			fixDataOffset += sizeof(OId);
			break;
		}
	}
	for (pos = 0; pos < container.getColumnSize(); pos++) {
		TableColumnInfo info(globalVarAlloc_);
		info.name_ = container.getColumnName(pos);
		info.type_ = container.getColumnType(pos);
		info.option_ = container.getColumnOption(pos);
		info.tupleType_ = container.getTupleColumnType(pos);

		columnInfoList_.push_back(info);
		ColumnInfo& columnInfo = nosqlColumnInfoList_[pos];
		columnInfo.setColumnId(static_cast<uint16_t>(pos));
		columnInfo.setType(info.type_);
		if (NoSQLUtils::isVariableType(info.type_)) {
			columnInfo.setOffset(varDataPos);
			varDataPos++;
		}
		else {
			columnInfo.setOffset(static_cast<uint16_t>(fixDataOffset));
			fixDataOffset += NoSQLUtils::getFixedSize(info.type_);
		}
	}
	columnSize_ = static_cast<uint32_t>(container.getColumnSize());
}

void TableSchemaInfo::setOptionList(util::XArray<uint8_t>& optionList) {
	assert(columnInfoList_.size() == optionList.size());
	if (columnInfoList_.size() != optionList.size()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_COLUMN, "");
	}
	for (size_t pos = 0; pos < columnInfoList_.size(); pos++) {
		columnInfoList_[pos].option_ = optionList[pos];
		TupleList::TupleColumnType currentType
			= convertNoSQLTypeToTupleType(columnInfoList_[pos].type_);
		columnInfoList_[pos].tupleType_
			= setColumnTypeNullable(currentType, !ColumnInfo::isNotNull(optionList[pos]));
	}
}

TableSchemaInfo::~TableSchemaInfo() {
	if (nosqlColumnInfoList_) {
		globalVarAlloc_.deallocate(nosqlColumnInfoList_);
		nosqlColumnInfoList_ = NULL;
	}
	for (size_t pos = 0; pos < compositeColumnIdList_.size(); pos++) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, compositeColumnIdList_[pos]);
	}
}

void NoSQLSyncContext::put(
		EventType eventType, int64_t syncId, StatementExecStatus status,
		EventByteInStream& in, util::Exception& exception,
		StatementMessage::Request& request) {
	UNUSED_VARIABLE(eventType);

	nosqlRequest_.put(syncId, status, in, &exception, request);
}

void NoSQLSyncContext::cancel() {
	nosqlRequest_.cancel();
}

bool NoSQLSyncContext::isRunning() {
	return nosqlRequest_.isRunning();
}

void NoSQLSyncContext::get(Event& request, util::StackAllocator& alloc,
	NoSQLContainer* container, int32_t waitInterval,
	int32_t timeoutInterval, util::XArray<uint8_t>& response) {
	nosqlRequest_.get(request, alloc, container, waitInterval, timeoutInterval, response);
}
void NoSQLSyncContext::wait(int32_t waitInterval) {
	nosqlRequest_.wait(waitInterval);
}

void NoSQLContainer::checkCondition(uint32_t currentTimer, int32_t timeoutInterval) {
	PartitionTable* pt = context_->pt_;
	const StatementHandler::ClusterRole
		clusterRole = (StatementHandler::CROLE_MASTER | StatementHandler::CROLE_FOLLOWER);
	StatementHandler::checkExecutable(clusterRole, pt);

	NodeId currentOwnerId = pt->getNewSQLOwner(txnPId_);
	PartitionRevisionNo  currentRevisionNo = pt->getNewSQLPartitionRevision(txnPId_);
	if (nodeId_ == UNDEF_NODEID) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
			"Invalid partition role, current=UNDEF, expected=OWNER");
	}
	if (nodeId_ != currentOwnerId) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
			"Invalid partition role, current=NOT OWNER"
			<< ", expected=OWNER");
	}
	if (currentRevisionNo != partitionRevNo_) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
			"Invalid partition role revision, current="
			<< currentRevisionNo << ", expected=" << partitionRevNo_);
	}
	if (checkTimeoutEvent(currentTimer, timeoutInterval)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
			"NoSQL command execution timeout, event=" << EventTypeUtility::getEventTypeName(statementType_));
	}
}

void NoSQLContainer::updateCondition() {
	PartitionTable* pt = context_->pt_;
	nodeId_ = pt->getNewSQLOwner(txnPId_);
	partitionRevNo_ = pt->getNewSQLPartitionRevision(txnPId_);
}

template void NoSQLUtils::makeNormalContainerSchema(
	util::StackAllocator& alloc, const char* containerName,
	const CreateTableOption& createOption,
	util::XArray<uint8_t>& containerSchema,
	util::XArray<uint8_t>& optionList,
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo);

template void NoSQLUtils::makeNormalContainerSchema(
	util::StackAllocator& alloc, const char* containerName,
	const CreateTableOption& createOption,
	util::XArray<uint8_t>& containerSchema,
	util::XArray<uint8_t>& optionList,
	TablePartitioningInfo<SQLVariableSizeGlobalAllocator>& partitioningInfo);

void NoSQLUtils::getAffinityValue(
		util::StackAllocator& alloc,
		const CreateTableOption& createOption, util::String& affinityValue) {
	UNUSED_VARIABLE(alloc);

	if (createOption.propertyMap_) {
		SyntaxTree::DDLWithParameterMap& paramMap = *createOption.propertyMap_;
		SyntaxTree::DDLWithParameterMap::iterator it;
		it = paramMap.find(DDLWithParameter::DATA_AFFINITY);
		if (it != paramMap.end()) {
			const char* valueStr =
				static_cast<const char*>(it->second.varData());
			affinityValue.assign(valueStr, it->second.varSize());
		}
	}
}

template<typename Alloc>
void NoSQLUtils::makeNormalContainerSchema(
		util::StackAllocator& alloc, const char* containerName,
		const CreateTableOption& createOption,
		util::XArray<uint8_t>& containerSchema,
		util::XArray<uint8_t>& optionList,
		TablePartitioningInfo<Alloc>& partitioningInfo) {
	UNUSED_VARIABLE(containerName);

	try {
		int32_t columnNum = createOption.columnNum_;
		util::Vector<ColumnId> primaryKeyList(alloc);
		NoSQLUtils::checkPrimaryKey(alloc, createOption, primaryKeyList);
		int16_t rowKeyNum = static_cast<int16_t>(primaryKeyList.size());
		for (size_t j = 0; j < primaryKeyList.size(); j++) {
			(*createOption.columnInfoList_)[primaryKeyList[j]]->option_
				|= SyntaxTree::COLUMN_OPT_NOT_NULL;
		}
		containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&columnNum), sizeof(int32_t));
		for (int32_t i = 0; i < columnNum; i++) {
			char* columnName = const_cast<char*>(
				(*createOption.columnInfoList_)[i]->columnName_->name_->c_str());
			int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
			containerSchema.push_back(
				reinterpret_cast<uint8_t*>(&columnNameLen), sizeof(int32_t));
			containerSchema.push_back(
				reinterpret_cast<uint8_t*>(columnName), columnNameLen);

			const ColumnType columnType = convertTupleTypeToNoSQLType(
					(*createOption.columnInfoList_)[i]->type_);
			const int8_t typeOrdinal =
					ValueProcessor::getPrimitiveColumnTypeOrdinal(
							columnType, false);
			containerSchema.push_back(
					reinterpret_cast<const uint8_t*>(&typeOrdinal),
					sizeof(typeOrdinal));

			const uint8_t flags = MessageSchema::makeColumnFlags(
					ValueProcessor::isArray(columnType), false,
					(*createOption.columnInfoList_)[i]->hasNotNullConstraint());
			containerSchema.push_back(&flags, sizeof(flags));
			optionList.push_back((*createOption.columnInfoList_)[i]->option_);
		}
		containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&rowKeyNum), sizeof(int16_t));
		for (int16_t pos = 0; pos < rowKeyNum; pos++) {
			int16_t currentColumnId = static_cast<int16_t>(primaryKeyList[pos]);
			containerSchema.push_back(
				reinterpret_cast<uint8_t*>(&currentColumnId), sizeof(int16_t));
		}

		util::String affinityValue(alloc);
		int32_t affinityStrLen = 0;
		NoSQLUtils::getAffinityValue(alloc, createOption, affinityValue);
		affinityStrLen = static_cast<int32_t>(affinityValue.size());
		containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&affinityStrLen), sizeof(int32_t));
		if (affinityStrLen > 0) {
			void* valuePtr = reinterpret_cast<void*>(const_cast<char8_t*>(
				affinityValue.c_str()));
			containerSchema.push_back(
				reinterpret_cast<uint8_t*>(valuePtr), affinityStrLen);
		}

		bool useExpiration = false;
		bool isSetted = false;
		bool useDevisionCount = false;
		bool withTimeSeriesOption = false;
		if (createOption.propertyMap_) {
			SyntaxTree::DDLWithParameterMap& paramMap = *createOption.propertyMap_;
			SyntaxTree::DDLWithParameterMap::iterator it;
			it = paramMap.find(DDLWithParameter::EXPIRATION_TIME);
			if (it != paramMap.end()) {
				TupleValue& value = (*it).second;
				if (value.getType() != TupleList::TYPE_LONG) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid format type");
				}
				int64_t tmpValue = value.get<int64_t>();
				if (tmpValue <= 0) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid value(> 0)");
				}
				if (tmpValue > INT32_MAX) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid value(integer overflow)");
				}
				isSetted = true;
				withTimeSeriesOption = true;
				useExpiration = true;
				partitioningInfo.timeSeriesProperty_.elapsedTime_ = static_cast<int32_t>(tmpValue);
			}
			it = paramMap.find(DDLWithParameter::EXPIRATION_TIME_UNIT);
			if (it != paramMap.end()) {
				withTimeSeriesOption = true;
				TupleValue& value = (*it).second;
				if (value.getType() != TupleList::TYPE_STRING) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid format type");
				}
				const char* valueStr = static_cast<const char*>(value.varData());
				assert(valueStr != NULL);
				util::String valueUnit(valueStr, value.varSize(), alloc);
				const util::String& normalizeValueUnit =
					normalizeName(alloc, valueUnit.c_str());
				TimeUnit targetUnitType;
				if (!strcmp(normalizeValueUnit.c_str(), "DAY")) {
					targetUnitType = TIME_UNIT_DAY;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "HOUR")) {
					targetUnitType = TIME_UNIT_HOUR;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "MINUTE")) {
					targetUnitType = TIME_UNIT_MINUTE;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "SECOND")) {
					targetUnitType = TIME_UNIT_SECOND;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "MILLISECOND")) {
					targetUnitType = TIME_UNIT_MILLISECOND;
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Target time unit '" << valueUnit << "' not supported");
				}
				isSetted = true;
				partitioningInfo.timeSeriesProperty_.timeUnit_ = targetUnitType;
			}
			it = paramMap.find(DDLWithParameter::EXPIRATION_DIVISION_COUNT);
			if (it != paramMap.end()) {
				useDevisionCount = true;
				TupleValue& value = (*it).second;
				if (value.getType() != TupleList::TYPE_LONG) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid format type");
				}
				int64_t tmpValue = value.get<int64_t>();
				if (tmpValue <= 0) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid value(> 0)");
				}
				if (tmpValue > INT32_MAX) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid value(integer overflow)");
				}
				withTimeSeriesOption = true;
				partitioningInfo.timeSeriesProperty_.dividedNum_ = static_cast<uint16_t>(tmpValue);
				isSetted = true;
			}
			ExpirationType targetExpirationType = EXPIRATION_TYPE_PARTITION;
			it = paramMap.find(DDLWithParameter::EXPIRATION_TYPE);
			if (it != paramMap.end()) {
				TupleValue& value = (*it).second;
				if (value.getType() != TupleList::TYPE_STRING) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Invalid format type");
				}
				withTimeSeriesOption = true;
				const char* valueStr = static_cast<const char*>(value.varData());
				assert(valueStr != NULL);
				util::String valueType(valueStr, value.varSize(), alloc);
				const util::String& normalizeValueType =
					normalizeName(alloc, valueType.c_str());
				if (!strcmp(normalizeValueType.c_str(), "ROW")) {
					targetExpirationType = EXPIRATION_TYPE_ROW;
				}
				else if (!strcmp(normalizeValueType.c_str(), "PARTITION")) {
					targetExpirationType = EXPIRATION_TYPE_PARTITION;
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Target expiration type '" << valueType << "' not supported");
				}
				isSetted = true;
				partitioningInfo.tableStatus_ = static_cast<uint8_t>(targetExpirationType);
			}
			else {
				isSetted = true;
				partitioningInfo.tableStatus_ = static_cast<uint8_t>(targetExpirationType);
			}
		}
		if (useDevisionCount && partitioningInfo.tableStatus_ == EXPIRATION_TYPE_PARTITION) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Division Count must be row expiration");
		}

		if (!createOption.isTimeSeries() && partitioningInfo.tableStatus_ == EXPIRATION_TYPE_ROW) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
				"Row expiration definition must be timeseries container");
		}

		int8_t existTimeSeriesOptionTmp = 0;
		if (createOption.isTimeSeries()) {
			if (isSetted && !useExpiration && withTimeSeriesOption) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"TimeSeries property must be setted elapsed time");
			}
			containerSchema.push_back(
				reinterpret_cast<uint8_t*>(
					&existTimeSeriesOptionTmp),
				sizeof(int8_t));
		}

		int32_t containerAttribute;
		if (createOption.isPartitioning()) {
			containerAttribute = CONTAINER_ATTR_SUB;
		}
		else {
			containerAttribute = CONTAINER_ATTR_SINGLE;
		}
		containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&containerAttribute), sizeof(int32_t));
		TablePartitioningVersionId versionId = static_cast<TablePartitioningVersionId>(
			partitioningInfo.partitioningVersionId_);
		containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&versionId), sizeof(TablePartitioningVersionId));
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void NoSQLUtils::checkSchemaValidation(util::StackAllocator& alloc,
	const DataStoreConfig& config, const char* containerName,
	util::XArray<uint8_t>& binarySchema, ContainerType containerType) {

	util::ArrayByteInStream normalIn = util::ArrayByteInStream(
		util::ArrayInStream(binarySchema.data(), binarySchema.size()));
	if (containerType == COLLECTION_CONTAINER) {
		MessageCollectionSchema messageSchema(alloc,
			config, containerName, normalIn, StatementMessage::FEATURE_V4_3);
	}
	else {
		MessageTimeSeriesSchema messageSchema(alloc,
			config, containerName, normalIn, StatementMessage::FEATURE_V4_3);
	}
}

void NoSQLUtils::makeContainerColumns(
	util::StackAllocator& alloc, util::XArray<uint8_t>& containerSchema,
	util::Vector<util::String>& columnNameList,
	util::Vector<ColumnType>& columnTypeList,
	util::Vector<uint8_t>& columnOptionList) {
	util::ArrayByteInStream in = util::ArrayByteInStream(
		util::ArrayInStream(containerSchema.data(), containerSchema.size()));
	int32_t columnNum;
	int16_t keyColumnId;
	in >> columnNum;
	int8_t typeOrdinal;
	uint8_t flags;
	for (int32_t i = 0; i < columnNum; i++) {
		util::String columnName(alloc);
		in >> columnName;
		columnNameList.push_back(columnName);
		in >> typeOrdinal;
		in >> flags;
		columnOptionList.push_back(flags);

		const bool forArray = MessageSchema::getIsArrayByFlags(flags);
		ColumnType columnType;
		if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
				typeOrdinal, forArray, false, columnType)) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}
		columnTypeList.push_back(columnType);
	}
	int16_t rowKeyNum;
	in >> rowKeyNum;
	for (int16_t pos = 0; pos < rowKeyNum; pos++) {
		in >> keyColumnId;
	}
}

void NoSQLUtils::makeLargeContainerSchema(
		util::StackAllocator& alloc, util::XArray<uint8_t>& binarySchema,
		bool isView, util::String& affinityStr) {
	UNUSED_VARIABLE(alloc);

	int32_t columnNum = 2;
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&columnNum), sizeof(int32_t));
	{
		const char8_t *columnName = "key";
		const int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		binarySchema.push_back(
			reinterpret_cast<const uint8_t*>(&columnNameLen), sizeof(int32_t));
		binarySchema.push_back(
			reinterpret_cast<const uint8_t*>(columnName), columnNameLen);
		const ColumnType columnType = COLUMN_TYPE_STRING;
		const int8_t typeOrdinal =
				ValueProcessor::getPrimitiveColumnTypeOrdinal(
						columnType, false);
		binarySchema.push_back(
				reinterpret_cast<const uint8_t*>(&typeOrdinal),
				sizeof(typeOrdinal));
		const bool notNull = true;
		const uint8_t flags = MessageSchema::makeColumnFlags(
				ValueProcessor::isArray(columnType), false, notNull);
		binarySchema.push_back(&flags, sizeof(flags));
	}
	const char8_t *columnName = "value";
	const int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
	binarySchema.push_back(
		reinterpret_cast<const uint8_t*>(&columnNameLen), sizeof(int32_t));
	binarySchema.push_back(
		reinterpret_cast<const uint8_t*>(columnName), columnNameLen);
	const ColumnType columnType = COLUMN_TYPE_BLOB;
	const int8_t typeOrdinal =
			ValueProcessor::getPrimitiveColumnTypeOrdinal(columnType, false);
	binarySchema.push_back(
			reinterpret_cast<const uint8_t*>(&typeOrdinal),
			sizeof(typeOrdinal));
	const uint8_t flags = MessageSchema::makeColumnFlags(
			ValueProcessor::isArray(columnType), false, false);
	binarySchema.push_back(&flags, sizeof(flags));
	int16_t rowKeyNum = 1;
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&rowKeyNum), sizeof(int16_t));
	int16_t keyColumnId = static_cast<int16_t>(ColumnInfo::ROW_KEY_COLUMN_ID);
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&keyColumnId), sizeof(int16_t));

	int32_t affinityStrLen = static_cast<int32_t>(affinityStr.size());
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&affinityStrLen), sizeof(int32_t));
	if (affinityStrLen > 0) {
		void* valuePtr = reinterpret_cast<void*>(const_cast<char8_t*>(
			affinityStr.c_str()));
		binarySchema.push_back(
			reinterpret_cast<uint8_t*>(valuePtr), affinityStrLen);
	}

	ContainerAttribute containerAttribute = CONTAINER_ATTR_LARGE;
	if (isView) {
		containerAttribute = CONTAINER_ATTR_VIEW;
	}
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&containerAttribute), sizeof(int32_t));
	TablePartitioningVersionId versionId = 0;
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&versionId), sizeof(TablePartitioningVersionId));
	int32_t optionType = static_cast<int32_t>(MessageSchema::OPTION_END);
	binarySchema.push_back(
		reinterpret_cast<uint8_t*>(&optionType), sizeof(int32_t));
}

void NoSQLUtils::makeLargeContainerColumn(
		util::XArray<ColumnInfo>& columnInfoList) {
	uint16_t varDataPos = 0;
	ColumnInfo columnInfo;
	columnInfo.setType(COLUMN_TYPE_STRING);
	columnInfo.setColumnId(0);
	columnInfo.setOffset(varDataPos++); 
	columnInfoList.push_back(columnInfo);

	columnInfo.setType(COLUMN_TYPE_BLOB);
	columnInfo.setColumnId(1);
	columnInfo.setOffset(varDataPos); 
	columnInfoList.push_back(columnInfo);
}

/*!
	@brief パーティショニングIDの計算
	@note utility関数
	@note 新規計算方法に対応
*/
PartitionId NoSQLUtils::resolvePartitionId(util::StackAllocator& alloc,
	PartitionId partitionCount,
	const FullContainerKey& containerKey,
	ContainerHashMode hashMode) {
	return KeyDataStore::resolvePartitionId(
		alloc, containerKey, partitionCount, hashMode);
}

/*!
	@brief サブコンテナID計算
*/
uint32_t NoSQLUtils::resolveSubContainerId(
	uint32_t hashValue, uint32_t tablePartitionCount) {
	return hashValue % tablePartitionCount;
}

/*!
	@brief	コンテナを取得する
*/
void NoSQLContainer::getContainerCount(uint64_t& containerCount,
	NoSQLStoreOption& option) {
	try {
		EventType eventType = GET_PARTITION_CONTAINER_NAMES;
		containerCount = 0;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		int64_t start = 0, limit = 0;
		out << start;
		out << limit;
		ContainerAttribute targetAttrs[] = {
			CONTAINER_ATTR_SINGLE,
			CONTAINER_ATTR_LARGE,
			CONTAINER_ATTR_SUB,
			CONTAINER_ATTR_SINGLE_SYSTEM
		};
		int32_t count = sizeof(targetAttrs) / sizeof(ContainerAttribute);
		out << count;
		for (size_t pos = 0; pos < static_cast<size_t>(count); pos++) {
			out << static_cast<int32_t>(targetAttrs[pos]);
		}
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();
		if (syncBinarySize == 0) {
			return;
		}
		util::ArrayByteInStream in =
			util::ArrayByteInStream(util::ArrayInStream(syncBinaryData, syncBinarySize));
		in >> containerCount;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "NoSQL get container count failed");
	}
}

int32_t NoSQLUtils::checkPrimaryKey(const CreateTableOption& createTableOption) {
	int32_t primaryKeyColumnId = -1;
	for (int32_t i = 0; i < static_cast<int32_t>(createTableOption.columnInfoList_->size()); i++) {
		if ((*createTableOption.columnInfoList_)[i]->isPrimaryKey()) {
			if (primaryKeyColumnId != -1) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"PRIMARY KEY must be only first column (columnId=0)");
			}
			primaryKeyColumnId = i;
		}
	}
	if (primaryKeyColumnId > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
			"PRIMARY KEY must be first column (columnId=0). Specified columnId="
			<< primaryKeyColumnId);
	}
	if (createTableOption.tableConstraintList_) {
		SyntaxTree::ExprList::iterator itr =
			createTableOption.tableConstraintList_->begin();
		for (; itr != createTableOption.tableConstraintList_->end(); ++itr) {
			SyntaxTree::Expr* constraint = *itr;
			if (constraint->op_ != SQLType::EXPR_COLUMN
				|| constraint->next_ == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"Invalid table constraint");
			}
			if (constraint->next_->size() != 1) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"Just one column must be specified for primary key constraint");
			}
			SyntaxTree::Expr* primaryKey = constraint->next_->front();
			if (createTableOption.columnInfoList_->size() == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"No table columns");
			}
			SyntaxTree::ColumnInfo*& firstColumn = createTableOption.columnInfoList_->front();
			if (createTableOption.columnInfoList_->size() == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"No table columns");
			}
			if (primaryKey->qName_ == NULL ||
				primaryKey->qName_->name_ == NULL ||
				primaryKey->qName_->name_->size() == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"No table constraint primary key");
			}
			assert(firstColumn->columnName_ && firstColumn->columnName_->name_);
			if (SQLCompiler::isNameOnlyMatched(
				*primaryKey->qName_, *firstColumn->columnName_)) {
				primaryKeyColumnId = 0;
				firstColumn->setPrimaryKey();
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"PRIMARY KEY must be first column. Specified primary key=" <<
					primaryKey->qName_->name_->c_str() <<
					", firstColumnName=" <<
					firstColumn->columnName_->name_->c_str());
			}
		}
	}
	return primaryKeyColumnId;
}

void NoSQLUtils::checkPrimaryKey(util::StackAllocator& alloc,
	const CreateTableOption& createTableOption, util::Vector<ColumnId>& columnIds) {
	ColumnId checkId = 0;
	util::Map<util::String, int32_t> columnMap(alloc);
	util::Map<util::String, int32_t>::iterator it;
	for (int32_t i = 0; i < static_cast<int32_t>(
		createTableOption.columnInfoList_->size()); i++) {
		util::String columnName((*createTableOption.columnInfoList_)[i]->
			columnName_->name_->c_str(), alloc);
		columnMap.insert(std::make_pair(columnName, i));
		if ((*createTableOption.columnInfoList_)[i]->isPrimaryKey()) {
			if (i != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"PRIMARY KEY must be only first column (columnId=0)");
			}
			columnIds.push_back(i);
			checkId++;
		}
	}

	if (createTableOption.tableConstraintList_) {
		SyntaxTree::ExprList::iterator itr =
			createTableOption.tableConstraintList_->begin();
		for (; itr != createTableOption.tableConstraintList_->end(); ++itr) {
			SyntaxTree::Expr* constraint = *itr;
			if (constraint->op_ != SQLType::EXPR_COLUMN
				|| constraint->next_ == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
					"Invalid table constraint");
			}
			for (size_t pos = 0; pos < constraint->next_->size(); pos++) {
				SyntaxTree::Expr* primaryKey = (*constraint->next_)[pos];
				if (primaryKey->qName_ == NULL ||
					primaryKey->qName_->name_ == NULL ||
					primaryKey->qName_->name_->size() == 0) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
						"No table constraint primary key");
				}
				util::String checkColumn(primaryKey->qName_->name_->c_str(), alloc);
				it = columnMap.find(checkColumn);
				if (it != columnMap.end()) {
					if (columnIds.size() == 0 && (*it).second != 0) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
							"Invalid primary key column definition");
					}
					columnIds.push_back((*it).second);
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
						"Primary key column not found");
				}
			}
		}
	}
}

void NoSQLUtils::decodePartitioningTableIndexInfo(util::StackAllocator& alloc,
	InputMessageRowStore& rowStore,
	util::Vector<IndexInfo>& indexInfoList) {
	while (rowStore.next()) {
		const void* keyData = NULL;
		uint32_t keySize = 0;
		const ColumnId keyColumnNo = 0;
		rowStore.getField(keyColumnNo, keyData, keySize);

		const void* valueData = NULL;
		uint32_t valueSize = 0;
		const ColumnId valueColumnNo = 1;
		rowStore.getField(valueColumnNo, valueData, valueSize);
		const uint8_t* value =
			static_cast<const uint8_t*>(valueData) + ValueProcessor::getEncodedVarSize(valueData);

		util::ArrayByteInStream inStream(util::ArrayInStream(value, valueSize));

		uint32_t indexListSize;
		inStream >> indexListSize;
		for (size_t pos = 0; pos < indexListSize; pos++) {
			IndexInfo indexInfo(alloc);
			StatementHandler::decodeIndexInfo<util::ArrayByteInStream>(inStream, indexInfo);
			indexInfoList.push_back(indexInfo);
		}
	}
}

void TableSchemaInfo::setPrimaryKeyIndex(int32_t primaryKeyColumnId) {
	if (indexInfoList_.empty()) {
		indexInfoList_.resize(columnInfoList_.size(), 0);
	}
	const int32_t flags = NoSQLCommonUtils::mapTypeToSQLIndexFlags(
		MAP_TYPE_DEFAULT, columnInfoList_[primaryKeyColumnId].type_);
	indexInfoList_[primaryKeyColumnId] = flags;
}

TablePartitioningIndexInfoEntry* TablePartitioningIndexInfo::find(
	const NameWithCaseSensitivity& indexName,
	util::Vector<ColumnId>& columnIdList, MapType indexType) {
	TablePartitioningIndexInfoEntry* retEntry = NULL;
	if (indexName.isCaseSensitive_) {
		for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
			if (!strcmp(indexName.name_, indexEntryList_[pos]->indexName_.c_str())) {
				if (isSame(pos, columnIdList, indexType)) {
					retEntry = indexEntryList_[pos];
					retEntry->pos_ = static_cast<int32_t>(pos);
					break;
				}
			}
		}
	}
	else {
		const util::String& normalizeSpecifiedIndexName =
			normalizeName(eventStackAlloc_, indexName.name_);
		for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
			const util::String& normalizeTargetIndexName =
				normalizeName(eventStackAlloc_,
					indexEntryList_[pos]->indexName_.c_str());
			if (!strcmp(normalizeSpecifiedIndexName.c_str(),
				normalizeTargetIndexName.c_str())) {
				if (isSame(pos, columnIdList, indexType)) {
					retEntry = indexEntryList_[pos];
					retEntry->pos_ = static_cast<int32_t>(pos);
					break;
				}
			}
		}
	}
	return retEntry;
}

TablePartitioningIndexInfoEntry* TablePartitioningIndexInfo::check(
	util::Vector<ColumnId>& columnIdList, MapType indexType) {
	for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
		if (isSame(pos, columnIdList, indexType)) {
			return indexEntryList_[pos];
		}
	}
	return NULL;
}

TablePartitioningIndexInfoEntry* TablePartitioningIndexInfo::find(
	const NameWithCaseSensitivity& indexName) {
	TablePartitioningIndexInfoEntry* retEntry = NULL;
	if (indexName.isCaseSensitive_) {
		for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
			if (!strcmp(indexName.name_,
				indexEntryList_[pos]->indexName_.c_str())) {
				retEntry = indexEntryList_[pos];
				retEntry->pos_ = static_cast<int32_t>(pos);
				break;
			}
		}
	}
	else {
		const util::String& normalizeSpecifiedIndexName =
			normalizeName(eventStackAlloc_, indexName.name_);
		for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
			const util::String& normalizeTargetIndexName =
				normalizeName(eventStackAlloc_,
					indexEntryList_[pos]->indexName_.c_str());
			if (!strcmp(normalizeSpecifiedIndexName.c_str(),
				normalizeTargetIndexName.c_str())) {
				retEntry = indexEntryList_[pos];
				retEntry->pos_ = static_cast<int32_t>(pos);
				break;
			}
		}
	}
	return retEntry;
}

TablePartitioningIndexInfoEntry* TablePartitioningIndexInfo::add(
	const NameWithCaseSensitivity& indexName,
	util::Vector<ColumnId>& columnIdList, MapType indexType) {
	util::String indexNameStr(indexName.name_, eventStackAlloc_);
	TablePartitioningIndexInfoEntry* entry
		= ALLOC_NEW(eventStackAlloc_) TablePartitioningIndexInfoEntry(
			eventStackAlloc_, indexNameStr, columnIdList, indexType);
	indexEntryList_.push_back(entry);
	entry->pos_ = static_cast<int32_t>(indexEntryList_.size());
	return entry;
}

void TablePartitioningIndexInfo::remove(int32_t pos) {
	if (pos >= static_cast<int32_t>(indexEntryList_.size())) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
			"Target pos=" << pos
			<< " is out of range, max=" << indexEntryList_.size());
	}
	indexEntryList_.erase(indexEntryList_.begin() + pos);
}

int32_t NoSQLUtils::getColumnId(util::StackAllocator& alloc,
	util::Vector<util::String> columnNameList,
	const NameWithCaseSensitivity& columnName) {
	int32_t indexColId = -1;
	int32_t realIndexColId = 0;
	if (columnName.isCaseSensitive_) {
		for (int32_t col = 0; col < static_cast<int32_t>(
			columnNameList.size()); col++) {
			if (!strcmp(columnNameList[col].c_str(), columnName.name_)) {
				indexColId = realIndexColId;
				break;
			}
			realIndexColId++;
		}
	}
	else {
		const util::String& normalizeSpecifiedColumnName =
			normalizeName(alloc, columnName.name_);
		for (int32_t col = 0; col < static_cast<int32_t>(
			columnNameList.size()); col++) {
			util::String normalizeColumnName =
				normalizeName(alloc, columnNameList[col].c_str());
			if (!normalizeColumnName.compare(
				normalizeSpecifiedColumnName)) {
				indexColId = realIndexColId;
				break;
			}
			realIndexColId++;
		}
	}
	return indexColId;
}

void NoSQLContainer::updateContainerProperty(
	TablePartitioningVersionId versionId,
	NoSQLStoreOption& option) {
	try {
		EventType eventType = UPDATE_CONTAINER_STATUS;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeContainerKey(out, *containerKey_);

		out << TABLE_PARTITIONING_VERSION;
		const size_t startPos = out.base().position();
		int32_t bodySize = 0;
		out << bodySize;
		out << versionId;
		const size_t endPos = out.base().position();
		out.base().position(startPos);
		StatementHandler::encodeIntData<EventByteOutStream, int32_t>(
			out, static_cast<int32_t>(endPos - startPos - sizeof(int32_t)));
		out.base().position(endPos);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

void NoSQLContainer::updateLargeContainerStatus(
	LargeContainerStatusType partitionStatus,
	NodeAffinityNumber affinity, NoSQLStoreOption& option,
	LargeExecStatus& execStatus, IndexInfo* indexInfo) {
	try {
		EventType eventType = UPDATE_CONTAINER_STATUS;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
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
		StatementHandler::encodeIntData<EventByteOutStream, int32_t>(
			out, static_cast<int32_t>(endPos - startPos - sizeof(int32_t)));
		out.base().position(endPos);
		util::XArray<uint8_t> response(eventStackAlloc_);
		executeCommand(request, true, response);

		uint8_t* syncBinaryData = response.data();
		size_t syncBinarySize = response.size();

		if (syncBinarySize == 0) {
			return;
		}
		util::ArrayByteInStream in =
			util::ArrayByteInStream(util::ArrayInStream(syncBinaryData, syncBinarySize));
		execStatus.reset();
		in >> execStatus.currentStatus_;
		in >> execStatus.affinityNumber_;
		if (in.base().remaining()) {
			StatementHandler::decodeIndexInfo<util::ArrayByteInStream>(in, execStatus.indexInfo_);
		}
	}
	catch (std::exception& e) {
		checkException(statementType_, e);
	}
}

TableContainerInfo* TableSchemaInfo::getTableContainerInfo(
	uint32_t partitionNum,
	const TupleValue* value1,
	const TupleValue* value2,
	NodeAffinityNumber& affinity,
	int64_t& baseValue,
	NodeAffinityNumber& baseAffinity) {

	affinity = partitionInfo_.getAffinityNumber(
		partitionNum, value1, value2, -1, baseValue, baseAffinity);
	switch (partitionInfo_.partitionType_) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
	{
		if (affinity >= containerInfoList_.size()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
				"Target pos=" << static_cast<int32_t>(affinity)
				<< " is out of range, max=" << containerInfoList_.size());
		}
		return &containerInfoList_[static_cast<size_t>(affinity)];
	}
	break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
	{
		AffinityContainerMap::iterator it = affinityMap_.find(affinity);
		if (it == affinityMap_.end()) {
			return NULL;
		}
		else {
			return &containerInfoList_[static_cast<size_t>((*it).second)];
		}
	}
	break;
	default:
	{
		return &containerInfoList_[0];
	}
	}
}

TableContainerInfo* TableSchemaInfo::getTableContainerInfo(
	NodeAffinityNumber affinity) {

	switch (partitionInfo_.partitionType_) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
	{
		if (affinity >= containerInfoList_.size()) {
			return NULL;
		}
		return &containerInfoList_[static_cast<size_t>(affinity)];
	}
	break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
	{
		AffinityContainerMap::iterator it = affinityMap_.find(affinity);
		if (it == affinityMap_.end()) {
			return NULL;
		}
		else {
			return &containerInfoList_[static_cast<size_t>((*it).second)];
		}
	}
	break;
	default:
	{
		return &containerInfoList_[0];
	}
	}
}


void TargetContainerInfo::set(TableContainerInfo* containerInfo,
	NodeAffinityNumber affinity) {
	containerId_ = containerInfo->containerId_;
	pId_ = containerInfo->pId_;
	versionId_ = containerInfo->versionId_;
	affinity_ = affinity;
	pos_ = containerInfo->pos_;
}


void resolveTargetContainer(
	EventContext& ec,
	const TupleValue* value1,
	const TupleValue* value2,
	DBConnection* conn,
	TableSchemaInfo* origTableSchema,
	SQLExecution* execution,
	NameWithCaseSensitivity& dbName,
	NameWithCaseSensitivity& tableName,
	TargetContainerInfo& targetInfo,
	bool& needRefresh) {

	util::StackAllocator& eventStackAlloc = ec.getAllocator();
	TableContainerInfo* containerInfo = NULL;
	NodeAffinityNumber affinity = UNDEF_NODE_AFFINITY_NUMBER;
	NoSQLStoreOption cmdOption(execution);
	if (tableName.isCaseSensitive_) {
		cmdOption.caseSensitivity_.setContainerNameCaseSensitive();
	}

	NoSQLContainer* currentContainer = NULL;
	SQLExecution::SQLExecutionContext& sqlContext = execution->getContext();
	NoSQLStore* store = conn->getNoSQLStore(
		sqlContext.getDBId(), sqlContext.getDBName());
	TableSchemaInfo* tableSchema = origTableSchema;
	int64_t baseValue;
	NodeAffinityNumber baseAffinity;
	uint32_t partitionNum = conn->getPartitionTable()->getPartitionNum();

	if (value1 != NULL) {
		containerInfo = tableSchema->getTableContainerInfo(
			partitionNum, value1, value2,
			affinity, baseValue, baseAffinity);
		if (containerInfo != NULL) {
			targetInfo.set(containerInfo, affinity);
		}
		else {
			if (affinity == UNDEF_NODE_AFFINITY_NUMBER) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
					"Affinity must be not null");
			}
			NoSQLContainer targetContainer(ec, tableName,
				sqlContext.getSyncContext(), execution);
			targetContainer.getContainerInfo(cmdOption);
			if (!targetContainer.isExists()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
					"Target table is not found");
			}
			util::XArray<ColumnInfo> columnInfoList(eventStackAlloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);
			LargeExecStatus execStatus(eventStackAlloc);
			cmdOption.sendBaseValue_ = true;
			cmdOption.baseValue_ = baseValue;

			TablePartitioningInfo<util::StackAllocator> partitioningInfo(eventStackAlloc);

			NoSQLStoreOption option(execution);
			store->getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
				eventStackAlloc,
				NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				targetContainer, columnInfoList, partitioningInfo, option);
			partitioningInfo.init();
			if (partitioningInfo.partitionType_
				<= SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
			}
			affinity = partitioningInfo.getAffinityNumber(partitionNum,
				value1, value2, -1, baseValue, baseAffinity);
			size_t targetAffinityPos = partitioningInfo.findEntry(affinity);
			if (targetAffinityPos == SIZE_MAX) {
			}
			else if (partitioningInfo.assignStatusList_[targetAffinityPos]
				== PARTITION_STATUS_DROP_START) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_ALREADY_REMOVED,
					"Target partition is already removed, name="
					<< tableName.name_ << "@" << partitioningInfo.largeContainerId_
					<< "@" << affinity);
			}
			cmdOption.isSync_ = true;
			cmdOption.ifNotExistsOrIfExists_ = true;

			execPartitioningOperation(
				ec, conn, PARTITION_STATUS_CREATE_START,
				execStatus, targetContainer, baseAffinity, affinity,
				cmdOption, partitioningInfo, tableName, execution, targetInfo);

			if (targetInfo.affinity_ == UNDEF_NODE_AFFINITY_NUMBER) {
				TableLatch latch(
					ec, conn, execution, dbName, tableName, false, true);
				TableSchemaInfo* currentSchema = latch.get();
				if (currentSchema) {
					containerInfo = currentSchema->getTableContainerInfo(
						partitionNum, value1, value2,
						affinity, baseValue, baseAffinity);
					assert(containerInfo != NULL);
					if (containerInfo != NULL) {
						targetInfo.set(containerInfo, affinity);
					}
					else {
						GS_THROW_USER_ERROR(
							GS_ERROR_DS_CONTAINER_NOT_FOUND,
							"Table " << tableName.name_
							<< " (affinity=" << affinity << ") not found");
					}
				}
			}
			if (targetInfo.affinity_ == UNDEF_NODE_AFFINITY_NUMBER) {
				GS_THROW_USER_ERROR(
					GS_ERROR_DS_CONTAINER_NOT_FOUND,
					"Table " << tableName.name_
					<< " (affinity=" << affinity << ") not found");
			}
			try {
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					targetInfo.affinity_, execution);
				currentContainer->getContainerInfo(cmdOption);
				if (!currentContainer->isExists()) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_CONTAINER_NOT_FOUND, "");
				}
			}
			catch (std::exception& e) {
				if (currentContainer == NULL) {
					GS_RETHROW_USER_OR_SYSTEM(e, "");
				}
				size_t targetAffinityPos = partitioningInfo.findEntry(targetInfo.affinity_);
				if (targetAffinityPos == SIZE_MAX) {
					return;
				}
				else if (partitioningInfo.assignStatusList_[targetAffinityPos]
					== PARTITION_STATUS_DROP_START) {
					return;
				}
				TablePartitioningIndexInfo tablePartitioningIndexInfo(eventStackAlloc);
				store->getLargeRecord<TablePartitioningIndexInfo>(
					eventStackAlloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					targetContainer, columnInfoList, tablePartitioningIndexInfo, option);
				util::XArray<uint8_t> containerSchema(eventStackAlloc);
				store->getLargeBinaryRecord(
					eventStackAlloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
					targetContainer, columnInfoList, containerSchema, option);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(
					info, baseAffinity, partitionNum);
				createSubContainer(info, eventStackAlloc, store,
					targetContainer, currentContainer, containerSchema,
					partitioningInfo, tablePartitioningIndexInfo,
					tableName, partitionNum, targetInfo.affinity_);
				dumpRecoverContainer(
					eventStackAlloc, tableName.name_, currentContainer);
			}
			needRefresh = true;
		}
	}
	else {
		assert(tableSchema->containerInfoList_.size() != 0);
		containerInfo = &tableSchema->containerInfoList_[0];
		targetInfo.set(containerInfo, affinity);
	}
}

template<typename Alloc>
bool execPartitioningOperation(EventContext& ec,
	DBConnection* conn,
	LargeContainerStatusType targetOperation,
	LargeExecStatus& execStatus,
	NoSQLContainer& targetContainer,
	NodeAffinityNumber baseAffinity,
	NodeAffinityNumber targetAffinity,
	NoSQLStoreOption& cmdOption,
	TablePartitioningInfo<Alloc>& partitioningInfo,
	const NameWithCaseSensitivity& tableName,
	SQLExecution* execution,
	TargetContainerInfo& targetContainerInfo) {
	util::StackAllocator& eventStackAlloc = ec.getAllocator();
	NoSQLContainer* currentContainer = NULL;
	IndexInfo* indexInfo = &execStatus.indexInfo_;
	uint32_t partitionNum = conn->getPartitionTable()->getPartitionNum();
	NoSQLStore* store = conn->getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());

	util::XArray<ColumnInfo> columnInfoList(eventStackAlloc);
	NoSQLUtils::makeLargeContainerColumn(columnInfoList);

	switch (targetOperation) {
	case PARTITION_STATUS_CREATE_START:
	case PARTITION_STATUS_DROP_START: {
		util::Vector<NodeAffinityNumber> affinityList(eventStackAlloc);
		size_t targetHashPos = 0;
		if (partitioningInfo.partitionType_
			== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
			for (size_t hashPos = 0;
				hashPos < partitioningInfo.partitioningNum_; hashPos++) {
				affinityList.push_back(baseAffinity + hashPos);
				if (baseAffinity + hashPos == targetAffinity) {
					targetHashPos = hashPos;
				}
			}
		}
		else {
			affinityList.push_back(targetAffinity);
			targetHashPos = 0;
		}
		if (targetOperation == PARTITION_STATUS_CREATE_START) {
			targetContainer.updateLargeContainerStatus(
				PARTITION_STATUS_CREATE_START,
				baseAffinity, cmdOption, execStatus, indexInfo);
			util::XArray<uint8_t> subContainerSchema(eventStackAlloc);
			store->getLargeBinaryRecord(
				eventStackAlloc,
				NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
				targetContainer, columnInfoList, subContainerSchema, cmdOption);

			for (size_t pos = 0; pos < affinityList.size(); pos++) {
				size_t targetAffinityPos = partitioningInfo.findEntry(affinityList[pos]);
				if (targetAffinityPos == SIZE_MAX) {
				}
				else if (partitioningInfo.assignStatusList_[targetAffinityPos]
					== PARTITION_STATUS_DROP_START) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_ALREADY_REMOVED,
						"Target partition is already removed, name="
						<< tableName.name_ << "@" << partitioningInfo.largeContainerId_
						<< "@" << affinityList[pos]);
				}
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					affinityList[pos], execution);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(
					info, baseAffinity, partitionNum);
				RenameColumnSchemaInfo renameColInfo;
				store->putContainer(info, renameColInfo, subContainerSchema, 0,
					partitioningInfo.containerType_,
					CONTAINER_ATTR_SUB, false, *currentContainer,
					NULL, NULL, cmdOption);

				if (pos == targetHashPos) {
					targetContainerInfo.containerId_
						= currentContainer->getContainerId();
					targetContainerInfo.pId_
						= currentContainer->getPartitionId();
					targetContainerInfo.versionId_
						= currentContainer->getSchemaVersionId();
					targetContainerInfo.affinity_ = affinityList[pos];
					targetContainerInfo.pos_ = 0;
				}
				TablePartitioningIndexInfo tablePartitioningIndexInfo(eventStackAlloc);
				NoSQLStoreOption option(execution);
				store->getLargeRecord<TablePartitioningIndexInfo>(
					eventStackAlloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					targetContainer, columnInfoList,
					tablePartitioningIndexInfo, option);
				TablePartitioningIndexInfoEntry* entry;
				for (size_t indexPos = 0;
					indexPos < tablePartitioningIndexInfo.indexEntryList_.size();
					indexPos++) {
					entry = tablePartitioningIndexInfo.indexEntryList_[indexPos];
					if (!entry->indexName_.empty()) {
						currentContainer->createIndex(
							entry->indexName_.c_str(), entry->indexType_, entry->columnIds_, cmdOption);
					}
				}
			}
			{
				TablePartitioningInfo<util::StackAllocator> partitioningInfo(eventStackAlloc);
				NoSQLStoreOption option(execution);
				store->getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
					eventStackAlloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					targetContainer, columnInfoList, partitioningInfo, option);
				partitioningInfo.init();
				util::Set<NodeAffinityNumber> candAffinityNumberList(eventStackAlloc);
				uint32_t partitioningNum
					= (partitioningInfo.partitioningNum_ == 0 ? 1 :
						partitioningInfo.partitioningNum_);
				for (size_t hashPos = 0; hashPos < partitioningNum; hashPos++) {
					candAffinityNumberList.insert(baseAffinity + hashPos);
				}

				if (partitioningInfo.assignNumberList_.size() == 1) {
					candAffinityNumberList.insert(
						partitioningInfo.assignNumberList_[0]);
				}

				partitioningInfo.findNeighbor(
					eventStackAlloc, cmdOption.baseValue_, candAffinityNumberList);
				TablePartitioningVersionId tablePartitioningVersionId
					= partitioningInfo.partitioningVersionId_;
				for (util::Set<NodeAffinityNumber>::iterator
					it = candAffinityNumberList.begin();
					it != candAffinityNumberList.end(); it++) {
					currentContainer = createNoSQLContainer(ec,
						tableName, partitioningInfo.largeContainerId_,
						(*it), execution);
					currentContainer->getContainerInfo(cmdOption);
					if (!currentContainer->isExists()) {
						size_t targetAffinityPos = partitioningInfo.findEntry((*it));
						if (targetAffinityPos == SIZE_MAX) {
							continue;
						}
						else if (partitioningInfo.assignStatusList_[targetAffinityPos]
							== PARTITION_STATUS_DROP_START) {
							continue;
						}
						TablePartitioningIndexInfo tablePartitioningIndexInfo(eventStackAlloc);
						store->getLargeRecord<TablePartitioningIndexInfo>(
							eventStackAlloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
							targetContainer, columnInfoList, tablePartitioningIndexInfo, option);
						util::XArray<uint8_t> containerSchema(eventStackAlloc);
						store->getLargeBinaryRecord(
							eventStackAlloc,
							NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
							targetContainer, columnInfoList, containerSchema, option);
						NodeAffinityNumber currentAffinity = (*it);
						TableExpirationSchemaInfo info;
						partitioningInfo.checkTableExpirationSchema(info, baseAffinity, partitionNum);
						createSubContainer<util::StackAllocator>(info,
							eventStackAlloc, store, targetContainer,
							currentContainer, containerSchema,
							partitioningInfo, tablePartitioningIndexInfo,
							tableName, partitionNum, currentAffinity);
						dumpRecoverContainer(eventStackAlloc, tableName.name_, currentContainer);
					}
					currentContainer->updateContainerProperty(
						tablePartitioningVersionId, cmdOption);
				}
			}
		}
		else {
			for (size_t pos = 0; pos < affinityList.size(); pos++) {
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					affinityList[pos], execution);
				currentContainer->setContainerType(partitioningInfo.containerType_);
				currentContainer->getContainerInfo(cmdOption);
				currentContainer->dropContainer(cmdOption);
			}
		}
	}
									break;
	}
	return true;
}

void dumpRecoverContainer(util::StackAllocator& alloc,
	const char* tableName, NoSQLContainer* container) {
	if (container != NULL) {
		util::String subNameStr(alloc);
		container->getContainerKey()->toString(alloc, subNameStr);
		GS_TRACE_WARNING(SQL_SERVICE, GS_TRACE_SQL_RECOVER_CONTAINER,
			"Recover table '" << tableName << "', partition '" << subNameStr << "'");
	}
}

NoSQLContainer* createNoSQLContainer(EventContext& ec,
	const NameWithCaseSensitivity tableName,
	ContainerId largeContainerId,
	NodeAffinityNumber affinityNumber,
	SQLExecution* execution) {

	util::StackAllocator& eventStackAlloc = ec.getAllocator();
	FullContainerKeyComponents components;
	components.dbId_ = execution->getContext().getDBId();
	components.affinityNumber_ = affinityNumber;
	components.baseName_ = tableName.name_;
	components.baseNameSize_ = strlen(tableName.name_);
	components.largeContainerId_ = largeContainerId;
	KeyConstraint keyConstraint(KeyConstraint::getNoLimitKeyConstraint());
	FullContainerKey* containerKey
		= ALLOC_NEW(eventStackAlloc) FullContainerKey(
			eventStackAlloc, keyConstraint, components);

	return  ALLOC_NEW(eventStackAlloc) NoSQLContainer(ec,
		NameWithCaseSensitivity(
			tableName.name_, tableName.isCaseSensitive_),
		containerKey,
		execution->getContext().getSyncContext(),
		execution);
}

template <typename Alloc>
void TablePartitioningInfo<Alloc>::findNeighbor(
	util::StackAllocator& alloc, int64_t value,
	util::Set<NodeAffinityNumber>& neighborAssignList) {
	util::Map<int64_t, NodeAffinityNumber> tmpMap(alloc);
	int32_t divideCount = 1;
	size_t pos;
	int64_t currentValue;
	if (intervalValue_ == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_INTERNAL, "");
	}
	if (value >= 0) {
		currentValue = (value / intervalValue_) * intervalValue_;
	}
	else {
		if (((value + 1) / intervalValue_ - 1) < (INT64_MIN / intervalValue_)) {
			currentValue = INT64_MIN;
		}
		else {
			currentValue = ((value + 1) / intervalValue_ - 1) * intervalValue_;
		}
	}

	if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		divideCount = partitioningNum_;
	}
	for (pos = 1; pos < assignNumberList_.size(); pos++) {
		if (assignStatusList_[pos] == PARTITION_STATUS_DROP_START) continue;
		if (divideCount == 1
			|| (divideCount > 1 && (pos % divideCount) == 1)) {
			tmpMap.insert(std::make_pair(
				assignValueList_[pos], assignNumberList_[pos]));
		}
	}

	util::Map<int64_t, NodeAffinityNumber>::iterator
		lowItr = tmpMap.lower_bound(currentValue);
	if (lowItr == tmpMap.end() && !tmpMap.empty()) {
		lowItr = tmpMap.begin();
	}
	if (lowItr != tmpMap.end()) {
		if (lowItr != tmpMap.begin()) {
			lowItr--;
			typename PartitionAssignNumberMapList::iterator
				it = assignNumberMapList_.find((*lowItr).second);
			if (it != assignNumberMapList_.end()) {
				neighborAssignList.insert(assignNumberList_[(*it).second]);
				for (pos = 1; static_cast<ptrdiff_t>(pos) < divideCount; pos++) {
					neighborAssignList.insert(
						assignNumberList_[(*it).second] + pos);
				}
			}
			lowItr++;
		}
		lowItr++;
		if (lowItr != tmpMap.end()) {
			typename PartitionAssignNumberMapList::iterator
				it = assignNumberMapList_.find((*lowItr).second);
			if (it != assignNumberMapList_.end()) {
				neighborAssignList.insert(assignNumberList_[(*it).second]);
				for (pos = 1; static_cast<ptrdiff_t>(pos) < divideCount; pos++) {
					neighborAssignList.insert(
						assignNumberList_[(*it).second] + pos);
				}
			}
		}
	}
}


MapType getAvailableIndex(const DataStoreConfig& dsConfig, const char* indexName,
	ColumnType targetColumnType,
	ContainerType targetContainerType, bool primaryCheck) {
	int32_t isArray = 0;
	try {
		if (strlen(indexName) > 0) {
			EmptyAllowedKey::validate(
				KeyConstraint::getUserKeyConstraint(
					dsConfig.getLimitContainerNameSize()),
				indexName, strlen(indexName),
				"indexName");
		}

		MapType realMapType;
		if (!IndexSchema::findDefaultIndexType(targetColumnType, realMapType)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CM_NOT_SUPPORTED,
					"Index is not supported for target column");
		}

		BaseContainer::validateIndexInfo(
				targetContainerType, targetColumnType, realMapType);

		return realMapType;
	}
	catch (std::exception& e) {
		if (primaryCheck) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Unsupported Rowkey Column type = "
				<< static_cast<int32_t>(targetColumnType) << ", array=" << isArray);
		}
		else {
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
}

void NoSQLContainer::createNoSQLClientId() {
	UUIDUtils::generate(nosqlClientId_.uuid_);
	nosqlClientId_.sessionId_ = 1;
}

void NoSQLContainer::encodeIndexEstimationKeyList(
		EventByteOutStream &out,
		const SQLIndexStatsCache::KeyList &keyList) {
	const uint32_t count = static_cast<uint32_t>(keyList.size());
	out << count;

	for (uint32_t i = 0; i < count; i++) {
		encodeIndexEstimationKey(out, keyList[i]);
	}
}

void NoSQLContainer::encodeIndexEstimationKey(
		EventByteOutStream &out, const SQLIndexStatsCache::Key &key) {
	out << key.containerId_;

	encodeIndexEstimationKeyColumns(out, key);

	uint32_t count = 0;
	count += (key.lower_.getType() == TupleList::TYPE_NULL ? 0 : 1);
	count += (key.upper_.getType() == TupleList::TYPE_NULL ? 0 : 1);
	out << count;

	for (uint32_t i = 0; i < 2; i++) {
		const bool upper = (i != 0);
		const TupleValue &value = (upper ? key.upper_ : key.lower_);
		if (value.getType() == TupleList::TYPE_NULL) {
			continue;
		}
		encodeIndexEstimationKeyCondition(out, key, upper);
	}
}

void NoSQLContainer::encodeIndexEstimationKeyColumns(
		EventByteOutStream &out, const SQLIndexStatsCache::Key &key) {
	uint32_t count = 1;
	out << count;
	out << key.column_;
}

void NoSQLContainer::encodeIndexEstimationKeyCondition(
		EventByteOutStream &out, const SQLIndexStatsCache::Key &key,
		bool upper) {
	const int32_t opType = (upper ?
			(key.upperInclusive_ ? DSExpression::LE : DSExpression::LT) :
			(key.lowerInclusive_ ? DSExpression::GE : DSExpression::GT));
	const TupleValue &value = (upper ? key.upper_ : key.lower_);
	const ColumnType columnType = convertTupleTypeToNoSQLType(value.getType());

	size_t valueSize;
	const uint8_t *valueBody = static_cast<const uint8_t*>(
			SQLProcessor::ValueUtils::getValueBody(value, valueSize));

	out << opType;
	out << columnType;
	out << static_cast<uint32_t>(valueSize);
	out << std::make_pair(valueBody, valueSize);
}

NoSQLRequest::NoSQLRequest(SQLVariableSizeGlobalAllocator& varAlloc,
	SQLService* sqlSvc, TransactionService* txnSvc, ClientId* clientId) :
	varAlloc_(varAlloc), requestId_(0),
	syncBinaryData_(NULL), syncBinarySize_(0),
	sqlSvc_(sqlSvc), txnSvc_(txnSvc), container_(NULL), clientId_(clientId) {
};

NoSQLRequest::~NoSQLRequest() {
	clear();
};

void NoSQLRequest::clear() {
	if (syncBinaryData_) {
		varAlloc_.deallocate(syncBinaryData_);
		syncBinaryData_ = NULL;
		syncBinarySize_ = 0;
	}
	eventType_ = UNDEF_EVENT_TYPE;
}

void NoSQLRequest::wait(int32_t waitInterval) {
	util::LockGuard<util::Condition> guard(condition_);
	requestId_++;
	condition_.wait(waitInterval);
}

void NoSQLContainer::encodeRequestId(
	util::StackAllocator& alloc, Event& request,
	NoSQLRequestId requestId) {
	util::StackAllocator::Scope scope(alloc);
	StatementHandler::updateRequestOption<
		StatementMessage::Options::NOSQL_SYNC_ID>(
			alloc, request, requestId);
}

void NoSQLRequest::get(Event& request,
	util::StackAllocator& alloc, NoSQLContainer* container,
	int32_t waitInterval, int32_t timeoutInterval,
	util::XArray<uint8_t>& response) {
	util::LockGuard<util::Condition> guard(condition_);
	container_ = container;
	try {
		state_ = STATE_NONE;
		clear();

		requestId_++;
		eventType_ = request.getType();

		container->encodeRequestId(alloc, request, requestId_);

		container->sendMessage(*txnSvc_->getEE(), request);

		util::Stopwatch watch;
		watch.start();

		for (;;) {
			condition_.wait(waitInterval);
			if (state_ != STATE_NONE) {
				break;
			}
			int32_t currentTime = watch.elapsedMillis();
			if (currentTime >= timeoutInterval) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
					"Elapsed time expired limit interval=" << timeoutInterval);
			}
			sqlSvc_->checkActiveStatus();
			container->checkCondition(watch.elapsedMillis(), timeoutInterval);
			const NodeDescriptor& nd = txnSvc_->getEE()->getServerND(container->getNodeId());

			bool isTrace = false;
			if (clientId_ != NULL) {
				SQLExecutionManager::Latch latch(
					*clientId_, sqlSvc_->getExecutionManager());
				SQLExecution* execution = latch.get();
				if (execution == NULL
					|| (execution && execution->isCancelled())) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
						"Cancel SQL, clientId=" << *clientId_ << ", location=NoSQL Send");
				}
				JobId jobId;
				execution->getContext().getCurrentJobId(jobId);
				JobManager::Latch jobLatch(jobId, "NoSQLRequest::get",
					sqlSvc_->getExecutionManager()->getJobManager());
				Job* job = jobLatch.get();
				if (job) {
					RAISE_EXCEPTION(SQLFailureSimulator::TARGET_POINT_10);
					job->checkCancel("NoSQL Send", false);
					GS_TRACE_WARNING(
						SQL_SERVICE, GS_TRACE_SQL_LONG_EVENT_WAITING,
						"Long event waiting (eventType=" << EventTypeUtility::getEventTypeName(eventType_) <<
						", pId=" << request.getPartitionId() <<
						", clientId=" << *clientId_ <<
						", jobId=" << jobId <<
						", node=" << nd <<
						", elapsedMillis=" << watch.elapsedMillis() << ")");
				}
				else {
					GS_TRACE_WARNING(
						SQL_SERVICE, GS_TRACE_SQL_LONG_EVENT_WAITING,
						"Long event waiting (eventType=" << EventTypeUtility::getEventTypeName(eventType_) <<
						", pId=" << request.getPartitionId() <<
						", clientId=" << *clientId_ <<
						", node=" << nd <<
						", elapsedMillis=" << watch.elapsedMillis() << ")");
				}
				isTrace = true;
			}

			if (!isTrace) {
				GS_TRACE_WARNING(
					SQL_SERVICE, GS_TRACE_SQL_LONG_EVENT_WAITING,
					"Long event waiting (eventType=" << EventTypeUtility::getEventTypeName(eventType_) <<
					", pId=" << request.getPartitionId() <<
					", node=" << nd <<
					", elapsedMillis=" << watch.elapsedMillis() << ")");
			}
		}
		if (state_ != STATE_SUCCEEDED) {
			try {
				throw exception_;
			}
			catch (std::exception& e) {
				GS_RETHROW_USER_ERROR(e, "");
			}
		}
		else {
			getResponse(response);
		}
	}
	catch (...) {
		container_ = NULL;
		state_ = STATE_NONE;
		exception_ = util::Exception();
		clear();
		throw;
	}
	container_ = NULL;
	clear();
	state_ = STATE_NONE;
}

void NoSQLRequest::cancel() {
	util::LockGuard<util::Condition> guard(condition_);
	try {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
			"Cancel SQL, clientId=" << *clientId_ << ", location=NoSQL Request");
	}
	catch (util::Exception& e) {
		exception_ = e;
		state_ = STATE_FAILED;
	}
	container_ = NULL;
	condition_.signal();
}

void NoSQLRequest::put(NoSQLRequestId requestId, StatementExecStatus status,
	EventByteInStream& in, util::Exception* exception, StatementMessage::Request& request) {

	util::LockGuard<util::Condition> guard(condition_);
	if (requestId_ != requestId) {
		return;
	}
	bool success = (status == StatementHandler::TXN_STATEMENT_SUCCESS);
	if (success) {
		if (container_) {
			container_->setRequestOption(request);
		}
		syncBinarySize_ = in.base().remaining();
		if (syncBinarySize_ != 0) {
			syncBinaryData_ = static_cast<uint8_t*>(
				varAlloc_.allocate(syncBinarySize_));
			in.base().read(syncBinaryData_, syncBinarySize_);
		}
		state_ = STATE_SUCCEEDED;
	}
	else {
		try {
			throw* exception;
		}
		catch (std::exception& e) {
			exception_ = GS_EXCEPTION_CONVERT(e, "");
			state_ = STATE_FAILED;
		}
	}
	condition_.signal();
}

void NoSQLRequest::getResponse(util::XArray<uint8_t>& buffer) {
	if (syncBinarySize_ > 0) {
		buffer.resize(syncBinarySize_);
		util::ArrayByteInStream in =
			util::ArrayByteInStream(
				util::ArrayInStream(syncBinaryData_, syncBinarySize_));
		in >> std::make_pair(buffer.data(), syncBinarySize_);
		clear();
	}
}

bool NoSQLRequest::isRunning() {
	util::LockGuard<util::Condition> guard(condition_);
	return (requestId_ != UNDEF_NOSQL_REQUESTID
		&& state_ == STATE_NONE
		&& eventType_ != UNDEF_EVENT_TYPE);
}

const char8_t* timeUnitToName(TimeUnit unit) {
	switch (unit) {
	case TIME_UNIT_DAY:
		return "DAY";
	case TIME_UNIT_HOUR:
		return "HOUR";
	case TIME_UNIT_MINUTE:
		return "MINUTE";
	case TIME_UNIT_SECOND:
		return "SECOND";
	case TIME_UNIT_MILLISECOND:
		return "MILLISECOND";
	default:
		assert(false);
		return "";
	}
}

template<typename Alloc>
std::string TablePartitioningInfo<Alloc>::dumpExpirationInfo(
	const char* containerName,
	TableExpirationSchemaInfo& info) {
	util::NormalOStringStream oss;
	oss << "CONTAINER_NAME=" << containerName << std::endl;
	oss << getContainerTypeName(containerType_) << std::endl;
	if (timeSeriesProperty_.elapsedTime_ != -1) {
		if (tableStatus_ == EXPIRATION_TYPE_ROW) {
			oss << "EXPIRATION_ROW" << std::endl;
			oss << "ELAPSED_TIME=" << timeSeriesProperty_.elapsedTime_ << std::endl;
			oss << "TIME_UNIT=" << timeUnitToName(timeSeriesProperty_.timeUnit_) << std::endl;
			oss << "DIVIDE_NUM=" << timeSeriesProperty_.dividedNum_ << std::endl;
		}
		else if (tableStatus_ == EXPIRATION_TYPE_PARTITION && info.elapsedTime_ != -1) {
			oss << "EXPIRATION_ROW" << std::endl;
			oss << "ELAPSED_TIME=" << info.elapsedTime_ << std::endl;
			oss << "TIME_UNIT=" << timeUnitToName(info.timeUnit_) << std::endl;
			oss << "START_VALUE=" << CommonUtility::getTimeStr(info.startValue_) << std::endl;
			oss << "LIMIT_VALUE=" << CommonUtility::getTimeStr(info.limitValue_) << std::endl;
		}
	}
	oss << std::endl;
	return oss.str().c_str();
}

void NoSQLContainer::getContainerBinary(
	NoSQLStoreOption& option, util::XArray<uint8_t>& response) {
	try {
		EventType eventType = GET_CONTAINER;
		ClientSession::Builder sessionBuilder(this, eventType);
		start(sessionBuilder);
		Event request(*ec_, eventType, getPartitionId());
		EventByteOutStream out = request.getOutStream();
		encodeFixedPart(out, eventType);
		encodeOptionPart(out, eventType, option);
		StatementHandler::encodeContainerKey(out, *containerKey_);
		out << containerType_;
		executeCommand(request, true, response);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "NoSQL get container failed");
	}
}

void RenameColumnSchemaInfo::encode(
	EventByteOutStream& out) {

	if (isRenameColumn_) {
		out << static_cast<int32_t>(
			MessageSchema::RENAME_COLUMN);
		size_t startPos = out.base().position();

		out << static_cast<int32_t>(0);
		size_t startDataPos = out.base().position();

		size_t endDataPos = out.base().position();
		size_t encodeSize = endDataPos - startDataPos;
		out.base().position(startPos);
		out << static_cast<int32_t>(encodeSize);
		assert(encodeSize == 0);

		out.base().position(endDataPos);
	}
}

void TableExpirationSchemaInfo::encode(EventByteOutStream& out) {

	if (isTableExpiration_) {
		if (startValue_ != -1 && limitValue_ != -1 &&
				elapsedTime_ != -1 && timeUnit_ != UINT8_MAX) {
			out << static_cast<int32_t>(MessageSchema::PARTITION_EXPIRATION);
			size_t startPos = out.base().position();
			out << static_cast<int32_t>(0);
			size_t startDataPos = out.base().position();
			assert(startValue_ != -1);
			assert(limitValue_ != -1);
			out << startValue_;
			out << limitValue_;
			int64_t duration = MessageSchema::getTimestampDuration(
				elapsedTime_, timeUnit_);
			out << duration;
			size_t endDataPos = out.base().position();
			size_t encodeSize = endDataPos - startDataPos;
			out.base().position(startPos);
			out << static_cast<int32_t>(encodeSize);
			assert(encodeSize == 24);
			out.base().position(endDataPos);
		}
	}
}

void TableSchemaInfo::checkWritableContainer() {
	for (size_t pos = 0; pos < getColumnSize(); pos++) {
		if (!checkNoSQLTypeToTupleType(
			nosqlColumnInfoList_[pos].getColumnType())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"Target column type='" << ValueProcessor::getTypeName(
					nosqlColumnInfoList_[pos].getColumnType())
				<< "' is not writable in current version");
		}
	}
}

void checkConnectedDbName(util::StackAllocator& alloc,
	const char* connectedDbName,
	const char* currentDbName, bool isCaseSensitive) {
	if (currentDbName == NULL
		|| (currentDbName && strlen(currentDbName) == 0)) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_INVALID_NAME, "Invalid database name=" << currentDbName);
	}
	if (!isCaseSensitive) {
		const util::String& currentNormalizedDbName
			= normalizeName(alloc, currentDbName);
		currentDbName = currentNormalizedDbName.c_str();
		if (strcmp(connectedDbName, currentDbName) != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
				"Connected database is not same as the target database, connected='"
				<< connectedDbName << "', specified='"
				<< currentDbName << "'");
		}
	}
	else {
		if (strcmp(connectedDbName, currentDbName) != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
				"Connected database is not same as the target database, connected='"
				<< connectedDbName << "', specified='"
				<< currentDbName << "'");
		}
	}
}

void TableSchemaInfo::checkSubContainer(size_t nth) {
	if (partitionInfo_.isPartitioning()) {
		if (containerInfoList_[nth].affinity_ != partitionInfo_.assignNumberList_[nth]) {
			util::NormalOStringStream strstrm;
			strstrm << tableName_ << "@" << partitionInfo_.largeContainerId_
				<< "@" << partitionInfo_.assignNumberList_[nth];
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
				"Table '" << tableName_
				<< "', partition '" << strstrm.str().c_str() << "' is not found");
		}
	}
}

void TableSchemaInfo::setupSQLTableInfo(
		util::StackAllocator& alloc, SQLTableInfo& tableInfo,
		const char* tableName, DatabaseId dbId, bool withVersion,
		int32_t clusterPartitionCount, bool isFirst, bool withoutCache,
		int64_t startTime, const char* viewSqlString) {
	UNUSED_VARIABLE(isFirst);

	tableInfo.tableName_ = tableName;
	tableInfo.idInfo_.dbId_ = dbId;
	tableInfo.hasRowKey_ = hasRowKey_;
	tableInfo.sqlString_ = viewSqlString;
	if (containerAttr_ == CONTAINER_ATTR_VIEW) {
		tableInfo.isView_ = true;
	}
	for (size_t columnId = 0;
		columnId < columnInfoList_.size(); columnId++) {
		const TableColumnInfo& columnInfo =
			columnInfoList_[columnId];
		SQLTableInfo::SQLColumnInfo column(
			columnInfo.tupleType_,
			util::String(columnInfo.name_.c_str(), alloc));
		tableInfo.columnInfoList_.push_back(column);
		tableInfo.nosqlColumnInfoList_.push_back(columnInfo.type_);
		tableInfo.nosqlColumnOptionList_.push_back(columnInfo.option_);
	}

	{
		SQLTableInfo::IndexInfoList *indexInfoList =
				ALLOC_NEW(alloc) SQLTableInfo::IndexInfoList(alloc);
		for (TableIndexInfoList::const_iterator it = indexInfoList_.begin();
				it != indexInfoList_.end(); ++it) {
			const int32_t indexFlags = *it;
			if (indexFlags == 0) {
				continue;
			}
			indexInfoList->push_back(SQLTableInfo::SQLIndexInfo(alloc));
			indexInfoList->back().columns_.push_back(
					static_cast<ColumnId>(it - indexInfoList_.begin()));
		}
		for (CompositeColumnIdList::const_iterator it =
				compositeColumnIdList_.begin();
				it != compositeColumnIdList_.end(); ++it) {
			indexInfoList->push_back(SQLTableInfo::SQLIndexInfo(alloc));
			const ColumnIdList *columns = *it;
			indexInfoList->back().columns_.assign(columns->begin(), columns->end());
		}
		if (indexInfoList->empty()) {
			indexInfoList = NULL;
		}
		tableInfo.indexInfoList_ = indexInfoList;
	}

	int32_t partitioningCount
		= partitionInfo_.getCurrentPartitioningCount();
	if (partitioningCount > 0) {
		if (SyntaxTree::isRangePartitioningType(partitionInfo_.partitionType_)) {
			if (withoutCache) {
				tableInfo.isExpirable_ = true;
			}
		}
		if (withVersion) {
			tableInfo.idInfo_.partitioningVersionId_ =
				partitionInfo_.partitioningVersionId_;
		}
		else {
			tableInfo.idInfo_.partitioningVersionId_ =
				MAX_TABLE_PARTITIONING_VERSIONID;
		}
		SQLTableInfo::PartitioningInfo* partitioning =
			ALLOC_NEW(alloc) SQLTableInfo::PartitioningInfo(alloc);
		tableInfo.partitioning_ = partitioning;
		partitioning->partitioningColumnId_ =
			partitionInfo_.partitioningColumnId_;
		partitioning->subPartitioningColumnId_ =
			partitionInfo_.subPartitioningColumnId_;
		partitioning->partitioningType_ = partitionInfo_.partitionType_;
		int32_t startSubContainerId = 0;
		if (containerInfoList_.size() == 1) {
			startSubContainerId = 0;
		}
		else {
			startSubContainerId = 1;
		}
		SQLTableInfo::SubInfoList& subInfoList = partitioning->subInfoList_;
		for (int32_t subContainerId = startSubContainerId;
				subContainerId < static_cast<ptrdiff_t>(
						containerInfoList_.size());
				subContainerId++) {
			const TableContainerInfo& containerInfo = containerInfoList_[subContainerId];

			SQLTableInfo::SubInfo subInfo;
			subInfo.partitionId_ = containerInfo.pId_;
			subInfo.containerId_ = containerInfo.containerId_;
			subInfo.schemaVersionId_ = containerInfo.versionId_;
			subInfo.approxSize_ = containerInfo.approxSize_;

			if (partitionInfo_.partitionType_ ==
				SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
				const size_t idListSize = partitionInfo_.condensedPartitionIdList_.size();
				assert(static_cast<ptrdiff_t>(idListSize) <= clusterPartitionCount);
				if (startSubContainerId == 0) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
						"Table name '" << tableName <<
						"' is not found or already removed or under removing");
				}
				assert(startSubContainerId > 0);
				subInfo.nodeAffinity_ =
					(subContainerId - 1) / clusterPartitionCount *
					clusterPartitionCount +
					partitionInfo_.condensedPartitionIdList_[
						(subContainerId - 1) % idListSize] +
					clusterPartitionCount;
						if (static_cast<int64_t>(containerInfo.affinity_) == -1 ||
								static_cast<int64_t>(containerInfo.affinity_) ==
										subInfo.nodeAffinity_) {
						}
						else {
							GS_THROW_USER_ERROR(
								GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
						}
			}
			else {
				subInfo.nodeAffinity_ = containerInfo.affinity_;
			}
			assert(startSubContainerId == 0 ||
				subInfo.nodeAffinity_ >= clusterPartitionCount);
			subInfoList.push_back(subInfo);
		}
		partitioning->partitioningCount_ =
			partitionInfo_.partitioningNum_;
		partitioning->clusterPartitionCount_ = clusterPartitionCount;
		partitioning->intervalValue_ =
			partitionInfo_.intervalValue_;
		partitioning->nodeAffinityList_.assign(
			partitionInfo_.condensedPartitionIdList_.begin(),
			partitionInfo_.condensedPartitionIdList_.end());

		if (partitionInfo_.partitionType_ !=
			SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			util::Vector<int64_t> availableList(alloc);
			util::Vector<int64_t> availableCountList(alloc);
			util::Vector<int64_t> unavailableList(alloc);
			util::Vector<int64_t> unavailableCountList(alloc);
			partitionInfo_.getSubIdList(
				alloc, availableList, availableCountList,
				unavailableList, unavailableCountList
				, startTime
			);
			util::Vector<int64_t>::iterator listIt = availableList.begin();
			util::Vector<int64_t>::iterator listEnd = availableList.end();
			util::Vector<int64_t>::iterator countIt = availableCountList.begin();
			util::Vector<int64_t>::iterator countEnd = availableCountList.end();
			while (listIt != listEnd && countIt != countEnd) {
				partitioning->availableList_.push_back(
					std::make_pair(*listIt, *countIt));
				++listIt;
				++countIt;
			}
		}
	}
	else {
		const TableContainerInfo& containerInfo = containerInfoList_[0];
		tableInfo.idInfo_.partitionId_ = containerInfo.pId_;
		tableInfo.idInfo_.containerId_ = containerInfo.containerId_;
		tableInfo.idInfo_.schemaVersionId_ = containerInfo.versionId_;
		tableInfo.idInfo_.approxSize_ = containerInfo.approxSize_;
	}
}

NoSQLStoreOption::NoSQLStoreOption(
	SQLExecution* execution,
	const NameWithCaseSensitivity* dbName,
	const NameWithCaseSensitivity* tableName) {

	clear();
	if (execution) {
		storeMemoryAgingSwapRate_
			= execution->getContext().getStoreMemoryAgingSwapRate();
		timezone_ = execution->getContext().getTimezone();
		const char* applicationName
			= execution->getContext().getApplicationName();
		if (applicationName != NULL && strlen(applicationName) > 0) {
			applicationName_ = applicationName;
		}
	}
	if (dbName && dbName->isCaseSensitive_) {
		caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName && tableName->isCaseSensitive_) {
		caseSensitivity_.setContainerNameCaseSensitive();
	}
}

void TableSchemaInfo::copyPartitionInfo(TableSchemaInfo& info) {
	partitionInfo_.partitionType_ = info.partitionInfo_.partitionType_;
	partitionInfo_.partitioningNum_ = info.partitionInfo_.partitioningNum_;
	partitionInfo_.partitionColumnName_ = info.partitionInfo_.partitionColumnName_;
	partitionInfo_.partitioningColumnId_ = info.partitionInfo_.partitioningColumnId_;
	partitionInfo_.partitionColumnType_ = info.partitionInfo_.partitionColumnType_;
	partitionInfo_.subPartitioningColumnName_ = info.partitionInfo_.subPartitioningColumnName_;
	partitionInfo_.subPartitioningColumnName_ = info.partitionInfo_.subPartitioningColumnName_;
	partitionInfo_.subPartitioningColumnId_ = info.partitionInfo_.subPartitioningColumnId_;
	partitionInfo_.intervalUnit_ = info.partitionInfo_.intervalUnit_;
	partitionInfo_.intervalValue_ = info.partitionInfo_.intervalValue_;
	partitionInfo_.largeContainerId_ = info.partitionInfo_.largeContainerId_;
	partitionInfo_.intervalAffinity_ = info.partitionInfo_.intervalAffinity_;
	partitionInfo_.subContainerNameList_ = info.partitionInfo_.subContainerNameList_;
	partitionInfo_.condensedPartitionIdList_ = info.partitionInfo_.condensedPartitionIdList_;
	partitionInfo_.assignNumberList_ = info.partitionInfo_.assignNumberList_;
	partitionInfo_.assignStatusList_ = info.partitionInfo_.assignStatusList_;
	partitionInfo_.assignValueList_ = info.partitionInfo_.assignValueList_;
	partitionInfo_.assignNumberMapList_ = info.partitionInfo_.assignNumberMapList_;
	partitionInfo_.assignCountMax_ = info.partitionInfo_.assignCountMax_;
	partitionInfo_.tableStatus_ = info.partitionInfo_.tableStatus_;
	partitionInfo_.dividePolicy_ = info.partitionInfo_.dividePolicy_;
	partitionInfo_.distributedfPolicy_ = info.partitionInfo_.distributedfPolicy_;
	partitionInfo_.partitioningVersionId_ = info.partitionInfo_.partitioningVersionId_;
	partitionInfo_.primaryColumnId_ = info.partitionInfo_.primaryColumnId_;
	partitionInfo_.containerType_ = info.partitionInfo_.containerType_;
	partitionInfo_.currentStatus_ = info.partitionInfo_.currentStatus_;
	partitionInfo_.currentAffinityNumber_ = info.partitionInfo_.currentAffinityNumber_;
	partitionInfo_.currentIndexName_ = info.partitionInfo_.currentIndexName_;
	partitionInfo_.currentIndexType_ = info.partitionInfo_.currentIndexType_;
	partitionInfo_.currentIndexColumnId_ = info.partitionInfo_.currentIndexColumnId_;
	partitionInfo_.anyNameMatches_ = info.partitionInfo_.anyNameMatches_;
	partitionInfo_.anyTypeMatches_ = info.partitionInfo_.anyTypeMatches_;
	partitionInfo_.currentIndexCaseSensitive_ = info.partitionInfo_.currentIndexCaseSensitive_;
	partitionInfo_.subPartitionColumnType_ = info.partitionInfo_.subPartitionColumnType_;
	partitionInfo_.activeContainerCount_ = info.partitionInfo_.activeContainerCount_;
	partitionInfo_.timeSeriesProperty_ = info.partitionInfo_.timeSeriesProperty_;
	partitionInfo_.subIdListCached_ = info.partitionInfo_.subIdListCached_;
	partitionInfo_.availableList_ = info.partitionInfo_.availableList_;
	partitionInfo_.availableCountList_ = info.partitionInfo_.availableCountList_;
	partitionInfo_.disAvailableList_ = info.partitionInfo_.disAvailableList_;
	partitionInfo_.disAvailableCountList_ = info.partitionInfo_.disAvailableCountList_;
	partitionInfo_.currentIntervalValue_ = info.partitionInfo_.currentIntervalValue_;
}

void TableSchemaInfo::copy(TableSchemaInfo& info) {
	columnInfoList_ = info.columnInfoList_;
	containerInfoList_ = info.containerInfoList_;
	founded_ = info.founded_;
	hasRowKey_ = info.hasRowKey_;
	containerType_ = info.containerType_;
	containerAttr_ = info.containerAttr_;
	columnSize_ = info.columnSize_;
	nosqlColumnInfoList_ = static_cast<ColumnInfo*>(
		globalVarAlloc_.allocate(sizeof(ColumnInfo) * columnSize_));
	memcpy((char*)nosqlColumnInfoList_, (char*)info.nosqlColumnInfoList_,
		sizeof(ColumnInfo) * columnSize_);
	indexInfoList_ = info.indexInfoList_;
	tableName_ = info.tableName_;
	affinityMap_ = info.affinityMap_;
	lastExecutedTime_ = info.lastExecutedTime_;
	sqlString_ = info.sqlString_;
	compositeColumnIdList_ = info.compositeColumnIdList_;
	copyPartitionInfo(info);
}

template<typename Alloc>
TablePartitioningInfo<Alloc>::TablePartitioningInfo(Alloc& alloc) :
	alloc_(alloc),
	partitionType_(0),
	partitioningNum_(0),
	partitionColumnName_(alloc),
	partitioningColumnId_(UNDEF_COLUMNID),
	partitionColumnType_(0),
	subPartitioningColumnName_(alloc),
	subPartitioningColumnId_(UNDEF_COLUMNID),
	intervalUnit_(UINT8_MAX),
	intervalValue_(0),
	largeContainerId_(UNDEF_CONTAINERID),
	intervalAffinity_(UNDEF_NODE_AFFINITY_NUMBER),
	subContainerNameList_(alloc), 
	condensedPartitionIdList_(alloc),
	assignNumberList_(alloc),
	assignStatusList_(alloc),
	assignValueList_(alloc),
	assignNumberMapList_(
		typename PartitionAssignNumberMapList::key_compare(), alloc),
	assignCountMax_(TABLE_PARTITIONING_DEFAULT_MAX_ASSIGN_NUM),
	tableStatus_(0),
	dividePolicy_(0),
	distributedfPolicy_(0),
	partitioningVersionId_(0),
	primaryColumnId_(UNDEF_COLUMNID),
	containerType_(COLLECTION_CONTAINER),
	currentStatus_(PARTITION_STATUS_NONE),
	currentAffinityNumber_(UNDEF_NODE_AFFINITY_NUMBER),
	currentIndexName_(alloc),
	currentIndexType_(MAP_TYPE_DEFAULT),
	currentIndexColumnId_(UNDEF_COLUMNID),
	anyNameMatches_(0), anyTypeMatches_(0),
	currentIndexCaseSensitive_(false),
	subPartitionColumnType_(0),
	activeContainerCount_(0),
	subIdListCached_(false),
	availableList_(alloc), availableCountList_(alloc),
	disAvailableList_(alloc), disAvailableCountList_(alloc),
	currentIntervalValue_(-1), opt_(NULL), property_(NULL)
{
	if (partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
		for (size_t i = 0; i < assignNumberList_.size(); i++) {
			assignNumberMapList_.insert(std::make_pair(assignNumberList_[i], i));
			}
		}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::init() {
	if (partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
		for (size_t i = 0; i < assignNumberList_.size(); i++) {
			assignNumberMapList_.insert(std::make_pair(assignNumberList_[i], i));
		}
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkIntervalList(
	int64_t interval, int64_t currentValue,
	util::Vector<int64_t>& intervalList,
	util::Vector<int64_t>& intervalCountList, int64_t& prevInterval) {
	int64_t offset;
	if (currentValue < 0) {
		offset = -CURRENT_INTERVAL_UNIT;
	}
	else {
		if (interval == MIN_PLUS_INTERVAL
			&& prevInterval == MAX_MINUS_INTERVAL) {
			offset = -CURRENT_BOUNDARY_UNIT;
		}
		else {
			offset = CURRENT_INTERVAL_UNIT;
		}
	}
	if (intervalList.size() == 0) {
		intervalList.push_back(interval);
		intervalCountList.push_back(1);
	}
	else {
		if (interval == prevInterval) {
			return;
		}
		if (interval == prevInterval + offset) {
			intervalCountList.back()++;
		}
		else {
			intervalList.push_back(interval);
			intervalCountList.push_back(1);
		}
	}
	prevInterval = interval;
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkExpirableInterval(
	int64_t currentTime, int64_t currentErasableTimestamp,
	int64_t duration,
	util::Vector<NodeAffinityNumber>& expiredAffinityNumbers,
	util::Vector<size_t>& expiredAffinityPos) {
	if (isTableExpiration()) {
		if (assignValueList_.size() < 2) return;
		for (size_t pos = 1; pos < assignValueList_.size(); pos++) {
			if (assignStatusList_[pos] != PARTITION_STATUS_CREATE_END) continue;
			int64_t startTime, endTime;
			if (assignValueList_[pos] - 1 > INT64_MAX - intervalValue_) {
				endTime = INT64_MAX;
			}
			else {
				endTime = assignValueList_[pos] + intervalValue_ - 1;
			}
			startTime = (
				(currentTime - duration) / intervalValue_
				* intervalValue_ - intervalValue_);
			int64_t erasableTime
				= BaseContainer::calcErasableTime(endTime, duration);
			if (erasableTime < currentErasableTimestamp
				&& currentTime > startTime) {
				expiredAffinityNumbers.push_back(assignNumberList_[pos]);
				expiredAffinityPos.push_back(pos);
			}
		}
	}
}

template void TablePartitioningInfo<util::StackAllocator>::getIntervalInfo(
	int64_t currentTime, size_t pos, int64_t& erasableTime, int64_t& startTime);

template<typename Alloc>
void TablePartitioningInfo<Alloc>::getIntervalInfo(int64_t currentTime, size_t pos,
	int64_t& erasableTime, int64_t& startTime) {
	startTime = 0;
	erasableTime = 0;
	int64_t endTime;
	if (isTableExpiration()) {
		if (assignValueList_.size() < 2 || assignValueList_.size() <= pos) return;
		if (assignStatusList_[pos] != PARTITION_STATUS_CREATE_END) return;
		if (assignValueList_[pos] - 1 > INT64_MAX - intervalValue_) {
			endTime = INT64_MAX;
		}
		else {
			endTime = assignValueList_[pos] + intervalValue_ - 1;
		}
		int64_t duration = MessageSchema::getTimestampDuration(
			timeSeriesProperty_.elapsedTime_, timeSeriesProperty_.timeUnit_);
		startTime = (
			(currentTime - duration) / intervalValue_
			* intervalValue_ - intervalValue_);
		erasableTime = BaseContainer::calcErasableTime(endTime, duration);
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::getSubIdList(
	util::StackAllocator& alloc, util::Vector<int64_t>& availableList,
	util::Vector<int64_t>& availableCountList,
	util::Vector<int64_t>& disAvailableList,
	util::Vector<int64_t>& disAvailableCountList,
	int64_t currentTime) {
	if (assignNumberList_.size() == 1) {
		return;
	}
	bool calculated = false;
	if (subIdListCached_) {
		bool useCache = true;
		if (isTableExpiration()) {
			int64_t tmpCurrentIntervalValue =
				((currentTime - MessageSchema::getTimestampDuration(
					timeSeriesProperty_.elapsedTime_,
					timeSeriesProperty_.timeUnit_)) / intervalValue_)
				* intervalValue_ - intervalValue_;
			calculated = true;
			if (tmpCurrentIntervalValue != currentIntervalValue_) {
				useCache = false;
				currentIntervalValue_ = tmpCurrentIntervalValue;
			}
		}
		if (useCache) {
			size_t pos;
			availableList.resize(availableList_.size());
			availableCountList.resize(availableCountList_.size());
			disAvailableList.resize(disAvailableList_.size());
			disAvailableCountList.resize(disAvailableCountList_.size());
			for (pos = 0; pos < availableList_.size(); pos++) {
				availableList[pos] = availableList_[pos];
				availableCountList[pos] = availableCountList_[pos];
			}
			for (pos = 0; pos < disAvailableList_.size(); pos++) {
				disAvailableList[pos] = disAvailableList_[pos];
				disAvailableCountList[pos] = disAvailableCountList_[pos];
			}
			return;
		}
	}
	util::Vector<std::pair<int64_t, LargeContainerStatusType> > tmpList(alloc);
	size_t pos;
	for (pos = 1; pos < assignNumberList_.size(); pos++) {
		tmpList.push_back(std::make_pair(
			assignValueList_[pos], assignStatusList_[pos]));
	}
	std::sort(tmpList.begin(), tmpList.end());
	int64_t prevAvailableInterval = INT64_MAX;
	int64_t prevDisAvailableInterval = INT64_MAX;
	if (isTableExpiration()) {
		if (!calculated) {
			currentIntervalValue_
				= ((currentTime - MessageSchema::getTimestampDuration(
					timeSeriesProperty_.elapsedTime_,
					timeSeriesProperty_.timeUnit_)) / intervalValue_)
				* intervalValue_ - intervalValue_;
		}
	}
	for (size_t pos = 0; pos < tmpList.size(); pos++) {
		int64_t tmpValue = tmpList[pos].first;
		int64_t baseValue = tmpValue;
		uint64_t shiftValue;
		shiftValue = 1;
		if (baseValue < 0) {
			baseValue = -(baseValue / intervalValue_);
			shiftValue = 1;
		}
		else {
			baseValue = baseValue / intervalValue_;
			shiftValue = 0;
		}
		baseValue = (baseValue << 1) | shiftValue;
		checkMaxIntervalValue(tmpValue, baseValue);
		if (isTableExpiration() && tmpList[pos].first <= currentIntervalValue_) {
			checkIntervalList(baseValue, tmpValue,
				disAvailableList, disAvailableCountList, prevDisAvailableInterval);
		}
		else
			if (tmpList[pos].second == PARTITION_STATUS_CREATE_END) {
				checkIntervalList(baseValue, tmpValue,
					availableList, availableCountList, prevAvailableInterval);
			}
			else if (tmpList[pos].second == PARTITION_STATUS_DROP_START) {
				checkIntervalList(baseValue, tmpValue,
					disAvailableList, disAvailableCountList, prevDisAvailableInterval);
			}
	}
	if (!subIdListCached_) {
		util::LockGuard<util::Mutex> guard(mutex_);
		if (subIdListCached_) return;
		availableList_.resize(availableList.size());
		availableCountList_.resize(availableCountList.size());
		disAvailableList_.resize(disAvailableList.size());
		disAvailableCountList_.resize(disAvailableCountList.size());
		assert(availableList.size() == availableCountList.size());
		assert(disAvailableList.size() == disAvailableCountList.size());
		size_t pos;
		for (pos = 0; pos < availableList.size(); pos++) {
			availableList_[pos] = availableList[pos];
			availableCountList_[pos] = availableCountList[pos];
		}
		for (pos = 0; pos < disAvailableList.size(); pos++) {
			disAvailableList_[pos] = disAvailableList[pos];
			disAvailableCountList_[pos] = disAvailableCountList[pos];
		}
		subIdListCached_ = true;
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::setSubContainerPartitionIdList(
	util::Vector<NodeAffinityNumber>& assignNumberList) {
	if (assignCountMax_ < static_cast<ptrdiff_t>(assignNumberList.size())) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
			"Max partitioning assign count");
	}
	assignNumberList_.assign(assignNumberList.size(), 0);
	assignStatusList_.assign(assignNumberList.size(), 0);
	if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
		for (size_t i = 0; i < assignNumberList.size(); i++) {
			assignNumberList_[i] = assignNumberList[i];
			assignStatusList_[i] = PARTITION_STATUS_CREATE_END;
		}
	}
	else {
		for (size_t i = 0; i < assignNumberList.size(); i++) {
			assignNumberList_[i] = assignNumberList[i];
			assignStatusList_[i] = PARTITION_STATUS_CREATE_END;
			assignNumberMapList_.insert(
				std::make_pair(assignNumberList_[i], i));
		}
	}
}

template<typename Alloc>
size_t TablePartitioningInfo<Alloc>::findEntry(NodeAffinityNumber affinity) {
	typename PartitionAssignNumberMapList::iterator
		it = assignNumberMapList_.find(affinity);
	if (it == assignNumberMapList_.end()) {
		return SIZE_MAX;
	}
	else {
		return (*it).second;
	}
}

template<typename Alloc>
size_t TablePartitioningInfo<Alloc>::newEntry(
	NodeAffinityNumber affinity, LargeContainerStatusType status,
	int32_t partitioningNum, int64_t baseValue, size_t maxAssginedEntryNum) {

	if (assignNumberList_.size() > maxAssginedEntryNum) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_MAX_ASSIGN_COUNT, 
			"Total number of table partitions exceeds maximum limit(" << maxAssginedEntryNum << ")");
	}
	size_t pos, retPos = 0;
	for (pos = 0; pos < static_cast<size_t>(partitioningNum); pos++) {
		assignNumberList_.push_back(affinity + pos);
		assignStatusList_.push_back(status);
		retPos = assignNumberList_.size() - 1;
		assignNumberMapList_.insert(std::make_pair(affinity, retPos));
	}

	if (assignValueList_.size() == 0) {
		assignValueList_.push_back(0);
	}
	int64_t tmpValue;
	if (baseValue >= 0) {
		tmpValue = (baseValue / intervalValue_) * intervalValue_;
	}
	else {
		if (((baseValue + 1) / intervalValue_ - 1) < (INT64_MIN / intervalValue_)) {
			tmpValue = INT64_MIN;
		}
		else {
			tmpValue = ((baseValue + 1) / intervalValue_ - 1) * intervalValue_;
		}
	}
	for (pos = 0; pos < static_cast<uint32_t>(partitioningNum); pos++) {
		assignValueList_.push_back(tmpValue);
	}
	return retPos;
}

template<typename Alloc>
NodeAffinityNumber TablePartitioningInfo<Alloc>::getAffinityNumber(
	uint32_t partitionNum,
	const TupleValue* value1,
	const TupleValue* value2,
	int32_t position,
	int64_t& baseValue,
	NodeAffinityNumber& baseAffinity) {
	NodeAffinityNumber affinity = UNDEF_NODE_AFFINITY_NUMBER;
	int64_t tmpValue;
	uint64_t shiftValue = 0;
	switch (partitionType_) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
	{
		affinity = (SQLProcessor::ValueUtils::hashValue(*value1)
			% partitioningNum_) + 1;
		baseValue = affinity;
	}
	break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
	{
		TupleList::TupleColumnType type = value1->getType();
		size_t size = TupleColumnTypeUtils::getFixedSize(type);
		switch (type) {
		case TupleList::TYPE_BYTE: {
			int8_t byteValue;
			memcpy(&byteValue, value1->fixedData(), size);
			tmpValue = byteValue;
			break;
		}
		case TupleList::TYPE_SHORT: {
			int16_t shortValue;
			memcpy(&shortValue, value1->fixedData(), size);
			tmpValue = shortValue;
			break;
		}
		case TupleList::TYPE_INTEGER: {
			int32_t intValue;
			memcpy(&intValue, value1->fixedData(), size);
			tmpValue = intValue;
			break;
		}
		case TupleList::TYPE_TIMESTAMP:
		case TupleList::TYPE_LONG: {
			int64_t longValue;
			memcpy(&longValue, value1->fixedData(), size);
			tmpValue = longValue;
			break;
		}
		case TupleList::TYPE_MICRO_TIMESTAMP:
			tmpValue = ValueProcessor::getTimestamp(value1->get<MicroTimestamp>());
			break;
		case TupleList::TYPE_NANO_TIMESTAMP:
			tmpValue = ValueProcessor::getTimestamp(value1->get<TupleNanoTimestamp>());
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INVALID_CONSTRAINT_NULL,
				"NULL value is not allowed, partitioning column='"
				<< partitionColumnName_ << "'");
		}
		baseValue = tmpValue;
		if (tmpValue < 0) {
			shiftValue = 1;
			tmpValue++;
		}
		tmpValue = tmpValue / intervalValue_;
		if (shiftValue == 1) {
			tmpValue *= -1;
		}
		uint64_t rangeBase = ((tmpValue << 1) | shiftValue);
		if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_RANGE) {
			affinity =
				((rangeBase / (partitionNum * 2)) * (partitionNum * 2)
					+ condensedPartitionIdList_[
						static_cast<size_t>(tmpValue % (partitionNum))]
					+ shiftValue * partitionNum
							+ partitionNum);
			baseAffinity = affinity;
		}
		else {
			if (position == -1 && value2 == NULL) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
			}
			if (position == -1) {
				position =
					(SQLProcessor::ValueUtils::hashValue(*value2) % partitioningNum_);
			}

			affinity =
				(rangeBase / (partitionNum * 2)
					* partitionNum * 2 * partitioningNum_
					+ condensedPartitionIdList_[
						static_cast<size_t>(tmpValue % partitionNum)]
					+ shiftValue * partitionNum * partitioningNum_
							+ position
							+ partitionNum);

			baseAffinity =
				(rangeBase / (partitionNum * 2) * partitionNum * 2 * partitioningNum_
					+ condensedPartitionIdList_[
						static_cast<size_t>(tmpValue % partitionNum)]
					+ shiftValue * partitionNum * partitioningNum_
							+ partitionNum);
		}
	}
	}
	return affinity;
}

template<typename Alloc>
int64_t TablePartitioningInfo<Alloc>::calcValueFromAffinity(
	uint32_t partitionNum, NodeAffinityNumber affinity) {
	int64_t decodeValue = -1;
	int32_t partitioningNum = 1;
	if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		partitioningNum = partitioningNum_;
	}
	NodeAffinityNumber beforeAffinity = affinity;
	beforeAffinity -= partitionNum;
	int64_t start =
		((beforeAffinity / (partitionNum * 2 * partitioningNum))
			* (partitionNum * 2 * partitioningNum));
	int64_t startPos = start / (
		partitionNum * partitioningNum * 2) * partitionNum;
	int64_t targetValue = (beforeAffinity / partitioningNum) * partitioningNum;

	for (int64_t pos = 0; pos < static_cast<int64_t>(
		condensedPartitionIdList_.size()); pos++) {
		if (targetValue == start + condensedPartitionIdList_[pos]) {
			decodeValue = (startPos + pos) * intervalValue_;
			break;
		}
		else if (targetValue == start + condensedPartitionIdList_[pos]
			+ partitionNum * partitioningNum) {
			if ((-startPos - (pos + 1)) < INT64_MIN / intervalValue_) {
				decodeValue = INT64_MIN;
			}
			else {
				decodeValue = (-startPos - (pos + 1)) * intervalValue_;
			}
			break;
		}
	}
	return decodeValue;
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkTableExpirationSchema(
	TableExpirationSchemaInfo& info,
	NodeAffinityNumber affinity, uint32_t partitionNum) {

	if (isTableExpiration() && assignNumberList_[0] != affinity) {
		info.isTableExpiration_ = true;
		info.elapsedTime_ = timeSeriesProperty_.elapsedTime_;
		info.timeUnit_ = timeSeriesProperty_.timeUnit_;
		info.startValue_ = calcValueFromAffinity(partitionNum, affinity);
		assert(info.startValue_ != -1);
		info.limitValue_ = intervalValue_;
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkTableExpirationSchema(
	TableExpirationSchemaInfo& info, size_t pos) {

	if (isTableExpiration() && pos != 0 && pos < assignValueList_.size()) {
		info.isTableExpiration_ = true;
		info.elapsedTime_ = timeSeriesProperty_.elapsedTime_;
		info.timeUnit_ = timeSeriesProperty_.timeUnit_;
		info.startValue_ = assignValueList_[pos];
		assert(info.startValue_ != -1);
		info.limitValue_ = intervalValue_;
		}
}

template<typename Alloc>
bool TablePartitioningInfo<Alloc>::checkSchema(
	TablePartitioningInfo& target,
	util::XArray<uint8_t>& targetSchema,
	util::XArray<uint8_t>& currentSchema) {
	if (partitionType_ == target.partitionType_
		&& partitioningNum_ == target.partitioningNum_
		&& partitioningColumnId_ == target.partitioningColumnId_
		&& subPartitioningColumnId_ == target.subPartitioningColumnId_
		&& intervalUnit_ == target.intervalUnit_
		&& intervalValue_ == target.intervalValue_
		&& dividePolicy_ == target.dividePolicy_
		&& distributedfPolicy_ == target.distributedfPolicy_
		&& containerType_ == target.containerType_
		&& subPartitionColumnType_ == target.subPartitionColumnType_
		&& timeSeriesProperty_ == target.timeSeriesProperty_
		&& targetSchema.size() == currentSchema.size()
		&& !memcmp(
			targetSchema.data(), currentSchema.data(), targetSchema.size())) {
		return true;
	}
	else {
		return false;
	}
}

void NoSQLContainer::setRequestOption(StatementMessage::Request& request) {
	StatementHandler::CompositeIndexInfos* compositeIndexInfos =
		request.optional_.get<StatementMessage::Options::COMPOSITE_INDEX>();
	if (compositeIndexInfos) {
		util::Vector<IndexInfo>& indexInfoList = compositeIndexInfos->indexInfoList_;
		for (size_t pos = 0; pos < indexInfoList.size(); pos++) {
			IndexInfo info(eventStackAlloc_);
			info = indexInfoList[pos];
			compositeIndexEntryList_.push_back(info);
		}
	}
}
util::Vector<IndexInfo>& NoSQLContainer::getCompositeIndex() {
	return compositeIndexEntryList_;
}

void TablePartitioningIndexInfo::getIndexInfoList(
	util::StackAllocator& alloc, util::Vector<IndexInfo>& indexInfoList) {
	for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
		IndexInfo indexInfo(alloc, indexEntryList_[pos]->indexName_,
			indexEntryList_[pos]->columnIds_, indexEntryList_[pos]->indexType_);
		indexInfoList.push_back(indexInfo);
	}
}

bool TablePartitioningIndexInfo::isSame(size_t pos,
	util::Vector<ColumnId>& columnIdList, MapType indexType) {
	if (columnIdList.size() != indexEntryList_[pos]->columnIds_.size()) {
		return false;
	}
	for (size_t columnId = 0; columnId < columnIdList.size(); columnId++) {
		if (columnIdList[columnId]
			!= indexEntryList_[pos]->columnIds_[columnId]) {
			return false;
		}
	}
	return (indexEntryList_[pos]->indexType_ == indexType);
}

std::string TablePartitioningIndexInfo::dumpStatus(
		util::StackAllocator& alloc) {
	UNUSED_VARIABLE(alloc);

	util::NormalOStringStream oss;
	picojson::value jsonOutValue;
	JsonUtils::OutStream out(jsonOutValue);
	TupleValue::coder(util::ObjectCoder(), NULL).encode(out, this);
	oss << jsonOutValue.serialize();
	return oss.str().c_str();
}

template<typename T>
void NoSQLUtils::makeLargeContainerRow(util::StackAllocator& alloc,
	const char* key,
	OutputMessageRowStore& outputMrs,
	T& targetValue) {
	util::XArray<uint8_t> encodeValue(alloc);
	util::XArrayByteOutStream outStream =
		util::XArrayByteOutStream(util::XArrayOutStream<>(encodeValue));
	util::ObjectCoder().encode(outStream, targetValue);
	outputMrs.beginRow();
	outputMrs.setFieldForRawData(0, key, static_cast<uint32_t>(strlen(key)));
	outputMrs.setFieldForRawData(1,
		encodeValue.data(), static_cast<uint32_t>(encodeValue.size()));
	outputMrs.next();
}

void NoSQLUtils::makeLargeContainerRowBinary(
		util::StackAllocator& alloc,
		const char* key,
		OutputMessageRowStore& outputMrs,
		util::XArray<uint8_t>& targetValue) {
	UNUSED_VARIABLE(alloc);

	outputMrs.beginRow();
	outputMrs.setFieldForRawData(0, key, static_cast<uint32_t>(strlen(key)));
	outputMrs.setFieldForRawData(1,
		targetValue.data(), static_cast<uint32_t>(targetValue.size()));
	outputMrs.next();
}

void NoSQLUtils::makeLargeContainerIndexRow(
	util::StackAllocator& alloc,
	const char* key, OutputMessageRowStore& outputMrs,
	util::Vector<IndexInfo>& indexInfoList) {
	util::XArray<uint8_t> encodeValue(alloc);
	util::XArrayByteOutStream outStream =
		util::XArrayByteOutStream(util::XArrayOutStream<>(encodeValue));
	outStream << static_cast<uint32_t>(indexInfoList.size());
	for (size_t pos = 0; pos < indexInfoList.size(); pos++) {
		StatementHandler::encodeIndexInfo(outStream, indexInfoList[pos]);
	}
	outputMrs.beginRow();
	outputMrs.setFieldForRawData(0, key, static_cast<uint32_t>(strlen(key)));
	outputMrs.setFieldForRawData(1,
		encodeValue.data(), static_cast<uint32_t>(encodeValue.size()));
	outputMrs.next();
}

bool NoSQLUtils::checkAcceptableTupleType(TupleList::TupleColumnType type) {
	switch (type & ~TupleList::TYPE_MASK_NULLABLE) {
	case TupleList::TYPE_BOOL:
	case TupleList::TYPE_BYTE:
	case TupleList::TYPE_SHORT:
	case TupleList::TYPE_INTEGER:
	case TupleList::TYPE_LONG:
	case TupleList::TYPE_FLOAT:
	case TupleList::TYPE_NUMERIC:
	case TupleList::TYPE_DOUBLE:
	case TupleList::TYPE_TIMESTAMP:
	case TupleList::TYPE_MICRO_TIMESTAMP:
	case TupleList::TYPE_NANO_TIMESTAMP:
	case TupleList::TYPE_NULL:
	case TupleList::TYPE_STRING:
	case TupleList::TYPE_BLOB:
	case TupleList::TYPE_ANY:
		return true;
	default:
		return false;
	}
}

bool NoSQLUtils::isVariableType(ColumnType type) {
	return ValueProcessor::isVariable(type);
};

uint32_t NoSQLUtils::getFixedSize(ColumnType type) {
	return FixedSizeOfColumnType[type];
}

template<typename T>
void decodeRow(InputMessageRowStore& rowStore, T& record,
	VersionInfo& versionInfo, const char* rowKey) {

	bool decodeVersion = false;

	while (rowStore.next()) {
		const void* keyData = NULL;
		uint32_t keySize = 0;
		const ColumnId keyColumnNo = 0;
		rowStore.getField(keyColumnNo, keyData, keySize);
		const char8_t* key =
			static_cast<const char8_t*>(keyData)
			+ ValueProcessor::getEncodedVarSize(keyData);

		const size_t versionKeyLength =
			std::min(strlen(NoSQLUtils::LARGE_CONTAINER_KEY_VERSION),
				static_cast<size_t>(keySize));
		const size_t infoKeyLength =
			std::min(strlen(rowKey), static_cast<size_t>(keySize));

		const void* valueData = NULL;
		uint32_t valueSize = 0;
		const ColumnId valueColumnNo = 1;
		rowStore.getField(valueColumnNo, valueData, valueSize);
		const uint8_t* value =
			static_cast<const uint8_t*>(valueData)
			+ ValueProcessor::getEncodedVarSize(valueData);

		util::ArrayByteInStream inStream(util::ArrayInStream(value, valueSize));

		if (strncmp(key,
			NoSQLUtils::LARGE_CONTAINER_KEY_VERSION, versionKeyLength) == 0) {
			util::ObjectCoder().decode(inStream, versionInfo);
			decodeVersion = true;
		}
		else if (strncmp(key, rowKey, infoKeyLength) == 0) {
			util::ObjectCoder::withAllocator(record.getAllocator()).decode(
				inStream, record);
		}
	}
	if (!decodeVersion) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_TABLE_FORMAT,
			"Invalid format, version not found.");
	}
}

template
void decodeRow(InputMessageRowStore& rowStore, TablePartitioningInfo<util::StackAllocator>& record,
	VersionInfo& versionInfo, const char* rowKey);

template
void decodeRow(InputMessageRowStore& rowStore, TablePartitioningIndexInfo& record,
	VersionInfo& versionInfo, const char* rowKey);

template
void decodeRow(InputMessageRowStore& rowStore, TableProperty& record,
	VersionInfo& versionInfo, const char* rowKey);


template<typename T>
void decodeBinaryRow(InputMessageRowStore& rowStore, T& record,
	VersionInfo& versionInfo, const char* rowKey) {

	bool decodeVersion = false;
	bool decodeInfo = false;

	while (rowStore.next()) {
		const void* keyData = NULL;
		uint32_t keySize = 0;
		const ColumnId keyColumnNo = 0;
		rowStore.getField(keyColumnNo, keyData, keySize);
		const char8_t* key =
			static_cast<const char8_t*>(keyData)
			+ ValueProcessor::getEncodedVarSize(keyData);

		const size_t versionKeyLength =
			std::min(strlen(NoSQLUtils::LARGE_CONTAINER_KEY_VERSION),
				static_cast<size_t>(keySize));
		const size_t infoKeyLength =
			std::min(strlen(rowKey), static_cast<size_t>(keySize));

		const void* valueData = NULL;
		uint32_t valueSize = 0;
		const ColumnId valueColumnNo = 1;
		rowStore.getField(valueColumnNo, valueData, valueSize);
		const uint8_t* value =
			static_cast<const uint8_t*>(valueData)
			+ ValueProcessor::getEncodedVarSize(valueData);

		util::ArrayByteInStream inStream(util::ArrayInStream(value, valueSize));

		if (strncmp(key,
			NoSQLUtils::LARGE_CONTAINER_KEY_VERSION, versionKeyLength) == 0) {
			util::ObjectCoder().decode(inStream, versionInfo);
			decodeVersion = true;
		}
		else if (strncmp(key, rowKey, infoKeyLength) == 0) {
			record.resize(valueSize);
			inStream >> std::make_pair(record.data(), valueSize);
			decodeInfo = true;
		}
	}

	if (!(decodeVersion || decodeInfo)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_TABLE_FORMAT,
			"Invalid format, both version and record were not found.");
	}
	else if (!decodeVersion) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_TABLE_FORMAT,
			"Invalid format, version not found.");
	}
	else if (!decodeInfo) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_TABLE_FORMAT,
			"Invalid format, record not found.");
	}
}

template<typename T>
void getTablePartitioningInfo(util::StackAllocator& alloc,
	const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList,
	T& partitioningInfo, NoSQLStoreOption& option) {

	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO
		<< "'";
	container.executeSyncQuery(queryStr.str().c_str(), option,
		fixedPart, varPart, rowCount, hasRowKey);

	InputMessageRowStore rowStore(dsConfig,
		columnInfoList.data(), static_cast<uint32_t>(columnInfoList.size()),
		fixedPart.data(), static_cast<uint32_t>(fixedPart.size()), varPart.data(),
		static_cast<uint32_t>(varPart.size()), rowCount, true, false);

	VersionInfo versionInfo;
	decodeRow<T>(
		rowStore, partitioningInfo, versionInfo,
		NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO);
}

template<typename T>
void getLargeContainerInfo(util::StackAllocator& alloc, const char* key,
	const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList, T& partitioningInfo) {
	NoSQLStoreOption option;
	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='" << key << "'";
	container.executeSyncQuery(queryStr.str().c_str(), option,
		fixedPart, varPart, rowCount, hasRowKey);

	InputMessageRowStore rowStore(dsConfig,
		columnInfoList.data(), static_cast<uint32_t>(columnInfoList.size()),
		fixedPart.data(), static_cast<uint32_t>(fixedPart.size()), varPart.data(),
		static_cast<uint32_t>(varPart.size()), rowCount, true, false);

	VersionInfo versionInfo;
	decodeRow<T>(
		rowStore, partitioningInfo, versionInfo, key);
}

template<typename T>
void getLargeContainerInfoBinary(util::StackAllocator& alloc, const char* key,
	const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList, T& partitioningInfo) {
	NoSQLStoreOption option;
	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='" << key << "'";
	container.executeSyncQuery(queryStr.str().c_str(), option,
		fixedPart, varPart, rowCount, hasRowKey);

	InputMessageRowStore rowStore(dsConfig,
		columnInfoList.data(), static_cast<uint32_t>(columnInfoList.size()),
		fixedPart.data(), static_cast<uint32_t>(
			fixedPart.size()), varPart.data(),
		static_cast<uint32_t>(varPart.size()), rowCount, true, false);

	VersionInfo versionInfo;
	decodeBinaryRow<T>(
		rowStore, partitioningInfo, versionInfo, key);
}

template <typename Alloc>
void TablePartitioningInfo<Alloc>::checkMaxAssigned(
	TransactionContext& txn,
	const FullContainerKey& containerKey,
	int32_t partitioningNum, size_t maxAssignedNum) {

	if (activeContainerCount_ + partitioningNum >
			static_cast<ptrdiff_t>(maxAssignedNum) + 1) {

		util::String containerName(txn.getDefaultAllocator());
		containerKey.toString(
			txn.getDefaultAllocator(), containerName);

		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_MAX_ASSIGN_COUNT,
			"Number of table partitions exceeds maximum limit("
			<< maxAssignedNum << ")");
	}
}

template<typename Alloc>
NoSQLContainer* recoveryContainer(util::StackAllocator& alloc,
	EventContext& ec,
	NoSQLStore* store,
	TablePartitioningInfo<Alloc>& partitioningInfo,
	NoSQLContainer& targetContainer,
	SQLExecution* execution,
	int32_t subContainerId,
	const NameWithCaseSensitivity& tableName,
	int32_t partitionNum,
	NodeAffinityNumber affinity) {

	NoSQLContainer* currentContainer = NULL;

	try {

		if (execution->getExecutionManager()->getSQLService()->getClusterService()->getManager()->getStandbyInfo().isStandby()) {
			return NULL;
		}

		NoSQLStoreOption option(execution);

		util::XArray<uint8_t> containerSchema(alloc);
		util::XArray<ColumnInfo> largeColumnInfoList(alloc);

		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
		NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
		store->getLargeRecord<TablePartitioningIndexInfo>(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
			targetContainer, largeColumnInfoList, tablePartitioningIndexInfo, option);
		store->getLargeBinaryRecord(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
			targetContainer, largeColumnInfoList, containerSchema, option);
		currentContainer = createNoSQLContainer(ec,
			tableName, partitioningInfo.getLargeContainerId(),
			affinity, execution);
		TableExpirationSchemaInfo info;
		if (subContainerId >= 0) {
			partitioningInfo.checkTableExpirationSchema(info, subContainerId);
		}
		else {
			partitioningInfo.checkTableExpirationSchema(info, affinity, partitionNum);
		}
		createSubContainer(info, alloc, store, targetContainer,
			currentContainer, containerSchema,
			partitioningInfo, tablePartitioningIndexInfo,
			tableName, partitionNum,
			affinity);


		ALLOC_DELETE(alloc, currentContainer);
		currentContainer = NULL;
		currentContainer = createNoSQLContainer(ec,
			tableName, partitioningInfo.getLargeContainerId(),
			affinity, execution);
		store->getContainer(*currentContainer, false, option);
		if (!currentContainer->isExists()) {
			util::String subNameStr(alloc);
			currentContainer->getContainerKey()->toString(alloc, subNameStr);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
				"Table '" << tableName.name_ << "', partition '" << subNameStr << "' is not found");
		}

		dumpRecoverContainer(alloc, tableName.name_, currentContainer);
		return currentContainer;
	}
	catch (std::exception& e) {
		ALLOC_DELETE(alloc, currentContainer);
		return NULL;
	}
}

template NoSQLContainer* recoveryContainer(util::StackAllocator& alloc,
	EventContext& ec,
	NoSQLStore* store,
	TablePartitioningInfo<SQLVariableSizeGlobalAllocator>& partitioningInfo,
	NoSQLContainer& targetContainer,
	SQLExecution* execution,
	int32_t subContainerId,
	const NameWithCaseSensitivity& tableName,
	int32_t partitionNum,
	NodeAffinityNumber affinity);

template void getLargeContainerInfo(util::StackAllocator& alloc,
	const char* key, const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList,
	TablePartitioningInfo<class util::StackAllocator>& partitioningInfo);

template void getLargeContainerInfo(util::StackAllocator& alloc,
	const char* key, const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList,
	TableProperty& tableProperty);


template void decodeRow(
	InputMessageRowStore& rowStore, ViewInfo& record,
	VersionInfo& versionInfo, const char* rowKey);

template void decodeRow(
	InputMessageRowStore& rowStore,
	TablePartitioningInfo<SQLVariableSizeGlobalAllocator>& record,
	VersionInfo& versionInfo, const char* rowKey);

template void decodeBinaryRow(
	InputMessageRowStore& rowStore,
	util::XArray<uint8_t>& record,
	VersionInfo& versionInfo, const char* rowKey);

template void getTablePartitioningInfo(util::StackAllocator& alloc,
	const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList,
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo,
	NoSQLStoreOption& option);

template void NoSQLUtils::makeLargeContainerRow(
	util::StackAllocator& alloc,
	const char* key, OutputMessageRowStore& outputMrs,
	ViewInfo& targetValue);

template void NoSQLUtils::makeLargeContainerRow(
	util::StackAllocator& alloc,
	const char* key, OutputMessageRowStore& outputMrs,
	VersionInfo& targetValue);

template void NoSQLUtils::makeLargeContainerRow(
	util::StackAllocator& alloc,
	const char* key, OutputMessageRowStore& outputMrs,
	TablePartitioningInfo<class util::StackAllocator>& targetValue);

template void NoSQLUtils::makeLargeContainerRow(
	util::StackAllocator& alloc,
	const char* key, OutputMessageRowStore& outputMrs,
	TablePartitioningIndexInfo& targetValue);

template void NoSQLUtils::makeLargeContainerRow(
	util::StackAllocator& alloc,
	const char* key, OutputMessageRowStore& outputMrs,
	TableProperty& targetValue);

template void TablePartitioningInfo<util::StackAllocator>::init();

template void TablePartitioningInfo<
	util::StackAllocator>::checkIntervalList(
		int64_t interval, int64_t currentValue,
		util::Vector<int64_t>& intervalList,
		util::Vector<int64_t>& intervalCountList, int64_t& prevInterval);

template void TablePartitioningInfo<
	util::StackAllocator>::checkExpirableInterval(
		int64_t currentTime, int64_t currentErasableTimestamp,
		int64_t duration,
		util::Vector<NodeAffinityNumber>& expiredAffinityNumbers,
		util::Vector<size_t>& expiredAffinityPos);

template void TablePartitioningInfo<util::StackAllocator>::getSubIdList(
	util::StackAllocator& alloc, util::Vector<int64_t>& availableList,
	util::Vector<int64_t>& availableCountList,
	util::Vector<int64_t>& disAvailableList,
	util::Vector<int64_t>& disAvailableCountList, int64_t currentTime);

template void TablePartitioningInfo<
	util::StackAllocator>::setSubContainerPartitionIdList(
		util::Vector<NodeAffinityNumber>& assignNumberList);

template size_t TablePartitioningInfo<util::StackAllocator>::newEntry(
	NodeAffinityNumber affinity, LargeContainerStatusType status,
	int32_t partitioningNum, int64_t baseValue, size_t maxAssignedEntryNum);

template size_t TablePartitioningInfo<
	SQLVariableSizeGlobalAllocator>::findEntry(
		NodeAffinityNumber affinity);

template size_t TablePartitioningInfo<util::StackAllocator>::findEntry(
	NodeAffinityNumber affinity);

template bool TablePartitioningInfo<util::StackAllocator>::checkSchema(
	TablePartitioningInfo& target,
	util::XArray<uint8_t>& targetSchema,
	util::XArray<uint8_t>& currentSchema);

template void TablePartitioningInfo<
	SQLVariableSizeGlobalAllocator>::checkTableExpirationSchema(
		TableExpirationSchemaInfo& info, NodeAffinityNumber affinity,
		uint32_t partitionNum);

template void TablePartitioningInfo<
	SQLVariableSizeGlobalAllocator>::checkTableExpirationSchema(
		TableExpirationSchemaInfo& info, size_t pos);

template void TablePartitioningInfo<
	util::StackAllocator>::checkTableExpirationSchema(
		TableExpirationSchemaInfo& info,
		NodeAffinityNumber affinity, uint32_t partitionNum);

template void TablePartitioningInfo<
	util::StackAllocator>::checkTableExpirationSchema(
		TableExpirationSchemaInfo& info, size_t pos);

template int64_t TablePartitioningInfo<
	util::StackAllocator>::calcValueFromAffinity(
		uint32_t partitionNum, NodeAffinityNumber affinity);

template TablePartitioningInfo<
	util::StackAllocator>::TablePartitioningInfo(util::StackAllocator&);
template TablePartitioningInfo<
	SQLVariableSizeGlobalAllocator>::TablePartitioningInfo(
		SQLVariableSizeGlobalAllocator&);

template bool execPartitioningOperation(EventContext& ec,
	DBConnection* conn,
	LargeContainerStatusType targetOperation,
	LargeExecStatus& execStatus,
	NoSQLContainer& targetContainer,
	NodeAffinityNumber baseAffinity, NodeAffinityNumber targetAffinity,
	NoSQLStoreOption& cmdOption,
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo,
	const NameWithCaseSensitivity& tableName,
	SQLExecution* execution,
	TargetContainerInfo& targetContainerInfo);

template NodeAffinityNumber TablePartitioningInfo<
	util::StackAllocator>::getAffinityNumber(
		uint32_t partitionNum, const TupleValue* value1,
		const TupleValue* value2, int32_t position,
		int64_t& baseValue, NodeAffinityNumber& baseAffinity);

template void TablePartitioningInfo<
	util::StackAllocator>::checkMaxAssigned(
		TransactionContext& txn,
		const FullContainerKey& containerKey,
		int32_t partitioningNum, size_t maxAssignedNum);

NoSQLContainer* recoverySubContainer(util::StackAllocator& alloc,
	EventContext& ec,
	NoSQLStore* store,
	SQLExecution* execution,
	const NameWithCaseSensitivity& tableName,
	int32_t partitionNum,
	NodeAffinityNumber affinity) {

	NoSQLContainer* currentContainer = NULL;

	try {

		SQLExecution::SQLExecutionContext& sqlContext = execution->getContext();
		NoSQLStoreOption option(execution);

		NoSQLContainer targetContainer(ec, tableName,
			sqlContext.getSyncContext(), execution);
		targetContainer.getContainerInfo(option);

		if (!targetContainer.isLargeContainer()) {
			return NULL;
		}

		util::XArray<ColumnInfo> columnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(columnInfoList);

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		store->getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
			alloc,
			NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
			targetContainer, columnInfoList, partitioningInfo, option);
		partitioningInfo.init();
		size_t targetAffinityPos = partitioningInfo.findEntry(affinity);
		if (targetAffinityPos == SIZE_MAX) {
			return NULL;
		}
		else if (partitioningInfo.assignStatusList_[targetAffinityPos]
			== PARTITION_STATUS_DROP_START) {
			return NULL;
		}

		util::XArray<uint8_t> containerSchema(alloc);
		util::XArray<ColumnInfo> largeColumnInfoList(alloc);

		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
		NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
		store->getLargeRecord<TablePartitioningIndexInfo>(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
			targetContainer, largeColumnInfoList, tablePartitioningIndexInfo, option);
		store->getLargeBinaryRecord(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
			targetContainer, largeColumnInfoList, containerSchema, option);

		currentContainer = createNoSQLContainer(ec,
			tableName, partitioningInfo.getLargeContainerId(),
			affinity, execution);

		TableExpirationSchemaInfo info;
		partitioningInfo.checkTableExpirationSchema(info, affinity, partitionNum);

		createSubContainer(info, alloc, store, targetContainer,
			currentContainer, containerSchema,
			partitioningInfo, tablePartitioningIndexInfo,
			tableName, partitionNum,
			affinity);

		dumpRecoverContainer(alloc, tableName.name_, currentContainer);

		return currentContainer;
	}
	catch (std::exception& e) {
		ALLOC_DELETE(alloc, currentContainer);
		return NULL;
	}
}

void NoSQLContainer::updateInfo(ContainerId containerId, SchemaVersionId versionId) {
	containerId_ = containerId;
	versionId_ = versionId;
	nodeId_ = context_->pt_->getNewSQLOwner(txnPId_);
}

const char8_t* getPartitionStatusRowName(LargeContainerStatusType type) {
	switch (type) {
	case PARTITION_STATUS_CREATE_START: return "CREATE SUB START";
	case PARTITION_STATUS_CREATE_END: return "CREATE SUB END";
	case PARTITION_STATUS_DROP_START: return "DROP SUB START";
	case PARTITION_STATUS_DROP_END: return "DROP SUB END";
	case INDEX_STATUS_CREATE_START: return "CREATE INDEX START";
	case INDEX_STATUS_CREATE_END: return "CREATE INDEX END";
	case INDEX_STATUS_DROP_START: return "DROP INDEX START";
	case INDEX_STATUS_DROP_END: return "DROP INDEX END";
	default: return "NONE";
	}
}

const char8_t* getTablePartitionTypeName(uint8_t type) {
	SyntaxTree::TablePartitionType partitionType
		= static_cast<SyntaxTree::TablePartitionType>(type);
	switch (partitionType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH: return "HASH";
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE: return "INTERVAL";
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: return "INTERVAL_HASH";
	default:
		return "NOT PARTITIONING";
	}
}

const char8_t* getTablePartitionTypeName(
		SyntaxTree::TablePartitionType partitionType) {
	switch (partitionType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH: return "HASH";
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE: return "INTERVAL";
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: return "INTERVAL_HASH";
	default:
		return "NOT PARTITIONING";
	}
}

const char8_t* getContainerTypeName(uint8_t type) {
	ContainerType containerType = static_cast<ContainerType>(type);
	switch (containerType) {
	case COLLECTION_CONTAINER: return "COLLECTION";
	case TIME_SERIES_CONTAINER: return "TIMESERIES";
	default:
		return "NOT SUPPORTED CONTAINER";
	}
}

const char8_t* getDateTimeName(uint8_t type) {
	util::DateTime::FieldType dateType = static_cast<util::DateTime::FieldType>(type);
	switch (dateType) {
	case util::DateTime::FIELD_YEAR: return "YEAR";
	case util::DateTime::FIELD_MONTH: return "MONTH";
	case util::DateTime::FIELD_DAY_OF_MONTH: return "DAY";
	case util::DateTime::FIELD_HOUR: return "HOUR";
	case util::DateTime::FIELD_MINUTE: return "MINUTE";
	case util::DateTime::FIELD_SECOND: return "SECOND";
	case util::DateTime::FIELD_MILLISECOND: return "MILLISECOND";
	default:
		return "NOT SUPPORTED DATE TYPE";
	}
}

const char8_t* getPartitionStatusName(LargeContainerStatusType type) {
	switch (type) {
	case PARTITION_STATUS_CREATE_START: return "CREATING PARTITION";
	case PARTITION_STATUS_CREATE_END: return "NORMAL";
	case PARTITION_STATUS_DROP_START: return "DROPPED";
	case PARTITION_STATUS_DROP_END: return "REMOVED";
	case INDEX_STATUS_CREATE_START: return "CREATING INDEX";
	case INDEX_STATUS_CREATE_END: return "NORMAL";
	case INDEX_STATUS_DROP_START: return "DROPPED INDEX";
	case INDEX_STATUS_DROP_END: return "NORMAL";
	default: return "NONE";
	}
}

bool isDenyException(int32_t errorCode) {
	return (errorCode == GS_ERROR_TXN_PARTITION_ROLE_UNMATCH
		|| errorCode == GS_ERROR_TXN_PARTITION_STATE_UNMATCH
		|| errorCode == GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH
		|| errorCode == GS_ERROR_DS_COL_LOCK_CONFLICT
		|| errorCode == GS_ERROR_DS_TIM_LOCK_CONFLICT
		|| errorCode == GS_ERROR_NOSQL_FAILOVER_TIMEOUT
		|| errorCode == GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN
		|| errorCode == GS_ERROR_TXN_REAUTHENTICATION_FIRED
		);
}

TupleList::TupleColumnType setColumnTypeNullable(
	TupleList::TupleColumnType type, bool nullable) {
	if (TupleColumnTypeUtils::isNull(type) || TupleColumnTypeUtils::isAny(type)) {
		return type;
	}
	if (nullable) {
		return static_cast<TupleList::TupleColumnType>(type | TupleList::TYPE_MASK_NULLABLE);
	}
	else {
		return static_cast<TupleList::TupleColumnType>(type & ~TupleList::TYPE_MASK_NULLABLE);
	}
}

void checkException(std::exception& e) {
	const util::Exception check = GS_EXCEPTION_CONVERT(e, "");
	int32_t errorCode = check.getErrorCode();

	if (errorCode == GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED
		|| errorCode == GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
	}
	else {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void checkException(EventType type, std::exception& e) {
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
		break;
	}
	default: {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	}
}
