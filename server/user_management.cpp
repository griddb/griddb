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
	@brief Implementation of TransactionService for user/database management
*/
#include "transaction_service.h"

#include "gs_error.h"

#include "log_manager.h"
#include "transaction_context.h"
#include "transaction_manager.h"
#include "key_data_store.h"

#include "util/container.h"

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

#include "base_container.h"
#include "data_store_v4.h"
#include "message_row_store.h"  
#include "query_processor.h"
#include "result_set.h"
#include "message_schema.h"

#include "sql_service.h"

#ifndef _WIN32
#include <signal.h>  
#endif

#define SET_USER_NAME(dbInfo)  (dbInfo.privilegeInfoList_.size() > 0 ? dbInfo.privilegeInfoList_[0]->userName_.c_str() : NULL)

#define TXN_THROW_DENY_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(DenyException, errorCode, message)

#define TXN_TRACE_HANDLER_CALLED(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler called. (nd=" << ev.getSenderND() \
							   << ", pId=" << ev.getPartitionId() << ")")

template<>
template<>
void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		EventByteOutStream& out, const ValueType& value) const;
template<>
template<>
void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		OutStream& out, const ValueType& value) const;

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		S& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}

template<>
template<typename S>
inline StatementMessage::CompositeIndexInfos*
StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	CompositeIndexInfos* value = ALLOC_NEW(alloc) CompositeIndexInfos(alloc);
	uint64_t size;
	in >> size;
	value->indexInfoList_.resize(static_cast<size_t>(size), IndexInfo(alloc));
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::decodeIndexInfo(in, value->indexInfoList_[pos]);
	}
	return value;
}


template<>
template<>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		EventByteOutStream& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}
template<>
template<>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		OutStream& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}




const ColumnId COLUMN_ID_USERS_USERNAME = 0;
const ColumnId COLUMN_ID_USERS_DIGEST   = 1;
const ColumnId COLUMN_ID_USERS_PROPERTY = 2;
const ColumnId COLUMN_ID_USERS_MAX      = 3;

static const DbUserHandler::DUColumnInfo USER_COLUMN_LIST[] = { 
	{"userName", COLUMN_TYPE_STRING},
	{"digest", COLUMN_TYPE_STRING},
	{"property", COLUMN_TYPE_BYTE},
};

const ColumnId COLUMN_ID_DBS_DBUSERNAME = 0;
const ColumnId COLUMN_ID_DBS_DBNAME     = 1;
const ColumnId COLUMN_ID_DBS_PROPERTY   = 2;
const ColumnId COLUMN_ID_DBS_USERNAME   = 3;
const ColumnId COLUMN_ID_DBS_PRIVILEGE  = 4;
const ColumnId COLUMN_ID_DBS_MAX        = 5;

static const DbUserHandler::DUColumnInfo DB_COLUMN_LIST[] = { 
	{"dbUserName", COLUMN_TYPE_STRING},
	{"dbName", COLUMN_TYPE_STRING},
	{"property", COLUMN_TYPE_BYTE},
	{"userName", COLUMN_TYPE_STRING},
	{"privilege", COLUMN_TYPE_STRING},
};

static const char *const PROPERTY_USER_NORMAL = "0";
static const char *const PROPERTY_USER_ADMIN  = "1";

template void StatementMessage::OptionSet::encode<EventByteOutStream>(EventByteOutStream& out) const;
template void StatementMessage::OptionSet::encode<OutStream>(OutStream& out) const;


template<typename S>
void DbUserHandler::decodeUserInfo(
		S &in, UserInfo &userInfo) {

	try {
		decodeStringData<S, util::String>(in, userInfo.userName_);
		in >> userInfo.property_;
		decodeBooleanData<S>(in, userInfo.withDigest_);
		if (userInfo.withDigest_) {
			decodeStringData<S, util::String>(in, userInfo.digest_);
		}
		else {
			userInfo.digest_ = "";
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}
template<typename S>
void DbUserHandler::decodeDatabaseInfo(S &in,
		DatabaseInfo &dbInfo, util::StackAllocator &alloc) {
	try {
		decodeStringData<S, util::String>(in, dbInfo.dbName_);
		in >> dbInfo.property_;
		int32_t num;
		in >> num;
		for (int32_t i = 0; i < num; i++) {
			PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

			decodeStringData<S, util::String>(in, prInfo->userName_);
			decodeStringData<S, util::String>(in, prInfo->privilege_);

			dbInfo.privilegeInfoList_.push_back(prInfo);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}
		
void DbUserHandler::makeSchema(util::XArray<uint8_t> &containerSchema, const DUColumnInfo *columnInfoList, int n) {
	TEST_PRINT("makeSchema() S\n");

	{
		uint32_t columnNum = n;
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNum), sizeof(uint32_t));
	}
	for (int i = 0; i < n; i++) {
		char *columnName = const_cast<char *>(columnInfoList[i].name);
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		const ColumnType columnType = columnInfoList[i].type;
		const int8_t typeOrdinal =
				ValueProcessor::getPrimitiveColumnTypeOrdinal(
						columnType, false);
		containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&typeOrdinal),
				sizeof(typeOrdinal));

		const bool notNull = true;
		const uint8_t flags = MessageSchema::makeColumnFlags(
				ValueProcessor::isArray(columnType), false, notNull);
		containerSchema.push_back(&flags, sizeof(flags));
	}
	{
		int16_t rowKeyNum = 1;
		containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&rowKeyNum), sizeof(int16_t));
		int16_t keyColumnId = static_cast<int16_t>(ColumnInfo::ROW_KEY_COLUMN_ID);
		containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&keyColumnId),
				sizeof(int16_t));
	}

	{
		char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
		memset(affinityStr, 0, AFFINITY_STRING_MAX_LENGTH + 1); 
		int32_t affinityStrLen =
			static_cast<int32_t>(strlen(reinterpret_cast<char *>(affinityStr)));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&affinityStrLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(affinityStr), affinityStrLen);
	}
	{
		int32_t containerAttribute =
			static_cast<int32_t>(CONTAINER_ATTR_SINGLE_SYSTEM);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&containerAttribute), sizeof(int32_t));
	}
	TEST_PRINT("makeSchema() E\n");
}

void DbUserHandler::makeRow(PartitionId pId,
		util::StackAllocator &alloc, const ColumnInfo *columnInfoList,
		uint32_t columnNum, DUColumnValue *valueList, RowData &rowData) {
	TEST_PRINT("makeRow() S\n");

	util::XArray<uint8_t> list1(alloc);
	util::XArray<uint8_t> list2(alloc);
	OutputMessageRowStore messageRowStore(getDataStore(pId)->getConfig(),
		columnInfoList, columnNum, list1, list2, false);

	messageRowStore.beginRow();
	for (uint32_t i = 0; i < columnNum; i++) {
		if (valueList[i].type == COLUMN_TYPE_STRING) {
			char *elemData = const_cast<char *>(valueList[i].sval);
			int32_t elemSize = static_cast<int32_t>(strlen(elemData));
			messageRowStore.setFieldForRawData(i, elemData, elemSize);
		} else { 
			char elemData = valueList[i].bval;
			messageRowStore.setField(
				i, &elemData, FixedSizeOfColumnType[COLUMN_TYPE_BYTE]);
		}
	}
	messageRowStore.next();

	list1.push_back(list2.data(), list2.size());
	rowData.swap(list1);
	TEST_PRINT("makeRow() E\n");
}

void DbUserHandler::makeRowKey(const char *name, RowKeyData &rowKey) {
	uint32_t varSize = static_cast<uint32_t>(strlen(name));
	if (varSize < VAR_SIZE_1BYTE_THRESHOLD) {
		uint8_t varSize8 = ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(varSize));
		rowKey.push_back(
			reinterpret_cast<uint8_t *>(&varSize8), sizeof(uint8_t));
	}
	else if (varSize < VAR_SIZE_4BYTE_THRESHOLD) {
		uint32_t varSize32 = ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(varSize));
		rowKey.push_back(
			reinterpret_cast<uint8_t *>(&varSize32), sizeof(uint32_t));
	}
	else {
		if (varSize > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
			UTIL_THROW_ERROR(GS_ERROR_DS_OUT_OF_RANGE, "");
		}
		uint64_t varSize64 = ValueProcessor::encode8ByteVarSize(static_cast<uint64_t>(varSize));
		rowKey.push_back(
			reinterpret_cast<uint8_t *>(&varSize64), sizeof(uint64_t));
	}
	rowKey.push_back(
		reinterpret_cast<uint8_t *>(const_cast<char *>(name)), varSize);
}

void DbUserHandler::makeRowKey(DatabaseInfo &dbInfo, RowKeyData &rowKey) {
	assert(dbInfo.privilegeInfoList_.size() == 1);
	const char *userName = dbInfo.privilegeInfoList_[0]->userName_.c_str();
	{
		size_t dbNameSize = strlen(dbInfo.dbName_.c_str());
		size_t userNameSize = strlen(userName);
		uint32_t varSize = static_cast<uint32_t>(dbNameSize + 1 + userNameSize);
		char tmp = ':';
		if (varSize < VAR_SIZE_1BYTE_THRESHOLD) {
			uint8_t varSize8 = ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(varSize));
			rowKey.push_back(
				reinterpret_cast<uint8_t *>(&varSize8), sizeof(uint8_t));
		}
		else if (varSize < VAR_SIZE_4BYTE_THRESHOLD) {
			uint32_t varSize32 = ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(varSize));
			rowKey.push_back(
				reinterpret_cast<uint8_t *>(&varSize32), sizeof(uint32_t));
		}
		else {
			if (varSize > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
				UTIL_THROW_ERROR(GS_ERROR_DS_OUT_OF_RANGE, "");
			}
			uint64_t varSize64 = ValueProcessor::encode8ByteVarSize(static_cast<uint64_t>(varSize));
			rowKey.push_back(
				reinterpret_cast<uint8_t *>(&varSize64), sizeof(uint64_t));
		}
		rowKey.push_back(reinterpret_cast<uint8_t *>(
							 const_cast<char *>(dbInfo.dbName_.c_str())),
			dbNameSize);
		rowKey.push_back(reinterpret_cast<uint8_t *>(&tmp), 1);
		rowKey.push_back(
			reinterpret_cast<uint8_t *>(const_cast<char *>(userName)),
			userNameSize);
	}
}

void DbUserHandler::putContainer(util::StackAllocator &alloc, 
		util::XArray<uint8_t> &containerInfo, const char *name,
		const util::DateTime now, const EventMonotonicTime emNow, const Request &request,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {

	Response response(alloc);

	TransactionContext &txn = transactionManager_->put(
			alloc, 0 /*pId*/,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	txn.setAuditInfo(NULL, NULL, NULL);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM), GS_SYSTEM_DB_ID,
		name, static_cast<uint32_t>(strlen(name)));
	bool modifiable = true /*modifiable*/;
	bool isCaseSensitive = false;

	KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
	KeyDataStoreValue keyStoreValue = keyStore->get(txn, containerKey, isCaseSensitive);

	DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
	const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

	DSInputMes input(alloc, DS_PUT_CONTAINER, &containerKey, COLLECTION_CONTAINER, &containerInfo,
		modifiable, MessageSchema::DEFAULT_VERSION, isCaseSensitive);
	StackAllocAutoPtr<DSPutContainerOutputMes> ret(alloc, static_cast<DSPutContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));

	if (ret.get()->status_ != PutStatus::NOT_EXECUTED) {
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		logRecordList.push_back(logBinary);
	}

	transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
}

bool DbUserHandler::checkContainer(
		EventContext &ec, Request &request, const char8_t *containerName) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	txn.setAuditInfo(NULL, NULL, NULL);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		containerName, static_cast<uint32_t>(strlen(containerName)));
	bool isCaseSensitive = false;

	KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
	KeyDataStoreValue keyStoreValue = keyStore->get(txn, containerKey, isCaseSensitive);

	if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
		TEST_PRINT1("%s not found.\n", containerName);
		return true;
	}
	else {
		return false;
	}

}

void DbUserHandler::putRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *containerName, DUColumnValue *cvList,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	UNUSED_VARIABLE(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const uint64_t numRow = 1;
	UNUSED_VARIABLE(numRow); 

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	txn.setAuditInfo(NULL, NULL, NULL);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		containerName, static_cast<uint32_t>(strlen(containerName)));
	bool isCaseSensitive = false;

	KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
		containerKey, isCaseSensitive);
	if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
	const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

	RowData rowData(alloc);
	{
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		DSContainerOutputMes* ret = static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));
		BaseContainer* container = ret->container_;
		makeRow(txn.getPartitionId(), alloc, container->getColumnInfoList(),
			container->getColumnNum(), cvList, rowData);
	}

	DSInputMes input(alloc, DS_PUT_ROW, &(rowData), PUT_INSERT_OR_UPDATE,
		MAX_SCHEMAVERSIONID);
	DSOutputMes* ret = static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));
	util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
		request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
		keyStoreValue.storeType_, ret->dsLog_);
	logRecordList.push_back(logBinary);

	transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
}

void DbUserHandler::fetchRole(TransactionContext &txn, util::StackAllocator &alloc, 
	BaseContainer *container, ResultSet *rs, PrivilegeType &role) {
		OutputMessageRowStore outputMessageRowStore(
			getDataStore(txn.getPartitionId())->getConfig(), container->getColumnInfoList(),
			container->getColumnNum(), *(rs->getRowDataFixedPartBuffer()),
			*(rs->getRowDataVarPartBuffer()), false /*isRowIdIncluded*/);
		ResultSize resultNum;
		container->getRowList(txn, *(rs->getOIdList()), rs->getResultNum(),
			resultNum, &outputMessageRowStore, false /*isRowIdIncluded*/, 0);

		const uint8_t *data1;
		uint32_t size1;
		outputMessageRowStore.getAllFixedPart(data1, size1);
		const uint8_t *data2;
		uint32_t size2;
		outputMessageRowStore.getAllVariablePart(data2, size2);

		InputMessageRowStore inputMessageRowStore(
			getDataStore(txn.getPartitionId())->getConfig(), container->getColumnInfoList(),
			container->getColumnNum(),
			reinterpret_cast<void *>(const_cast<uint8_t *>(data1)), size1,
			reinterpret_cast<void *>(const_cast<uint8_t *>(data2)), size2, 1,
			false);

		util::String targetPrivilege(alloc);
		const uint8_t *field;
		uint32_t fieldSize;

		inputMessageRowStore.next();  

		inputMessageRowStore.getField(COLUMN_ID_DBS_PRIVILEGE, field, fieldSize);  
		targetPrivilege.append(
			reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
		targetPrivilege.append("\0");

		if (strcmp(targetPrivilege.c_str(), "ALL") == 0) {
			role = ALL;
		} else {
			role = READ;
		}
}

void DbUserHandler::removeRowWithRowKey(TransactionContext& txn, util::StackAllocator& alloc,
	const TransactionManager::ContextSource& cxtSrc,
	KeyDataStoreValue& keyStoreValue, const RowKeyData& rowKey,
	util::XArray<const util::XArray<uint8_t>*>& logRecordList) {
	DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
	const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
	DSInputMes input(alloc, DS_REMOVE_ROW, const_cast<RowKeyData*>(&(rowKey)),
		MAX_SCHEMAVERSIONID);
	DSOutputMes* ret = static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));

	util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
		cxtSrc, cxtSrc.stmtId_, keyStoreValue.storeType_, ret->dsLog_);
	logRecordList.push_back(logBinary);

	transactionManager_->update(txn, cxtSrc.stmtId_);
}

bool DbUserHandler::runWithRowKey(
		EventContext &ec, const TransactionManager::ContextSource &cxtSrc, 
		const char8_t *containerName, const RowKeyData &rowKey,
		DUGetInOut *option) {
	TEST_PRINT("runWithRowKey() S\n");
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, 0 /*pId*/,
			TXN_EMPTY_CLIENTID, cxtSrc, now, emNow);
	txn.setAuditInfo(NULL, NULL, NULL);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		containerName, static_cast<uint32_t>(strlen(containerName)));
	bool isCaseSensitive = false;

	KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
		containerKey, isCaseSensitive);
	if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
		switch (option->type) {
		case DUGetInOut::DBID:
		case DUGetInOut::AUTH://[LDAP]
			assert(option->s);
			TEST_PRINT1("%s not found.\n", containerName);
			return false;
		case DUGetInOut::RESULT_NUM:
		case DUGetInOut::USER://[LDAP]
			return false;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	if (option->type != DUGetInOut::REMOVE) {
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		DSContainerOutputMes* ret = static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));
		BaseContainer* container = ret->container_;
		ResultSet *rs = getDataStore(txn.getPartitionId())->getResultSetManager()->create(
			txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *getDataStore(txn.getPartitionId()), *rs);
		QueryProcessor::get(txn, *container, static_cast<uint32_t>(rowKey.size()),
			rowKey.data(), *rs);
		rs->setResultType(RESULT_ROWSET);
		TEST_PRINT1("getResultNum() %d\n", rs->getResultNum());

		switch (option->type) {
		case DUGetInOut::RESULT_NUM: {
			option->count = rs->getResultNum();
			break;
		}
		case DUGetInOut::DBID: {
			if (rs->getResultNum() != 1) {
				return false;
			}
			if (option->userType != Message::USER_NORMAL) {
				util::XArray<RowId> rowIdList(alloc);
				container->getRowIdList(txn, *(rs->getOIdList()), rowIdList);
				int64_t dbId = rowIdList[0] + DBID_RESERVED_RANGE;
				option->dbId = dbId;
			} else {
				option->dbId = UNDEF_DBID;
			}
			break;
		}
		case DUGetInOut::ROLE: {
			fetchRole(txn, alloc, container, rs, option->priv);
			break;
		}
		case DUGetInOut::AUTH: {
			if (rs->getResultNum() != 1) {
				return false;
			}
			return checkDigest(txn, alloc, container, rs, option->s);
		}
		case DUGetInOut::USER: {
			if (rs->getResultNum() != 1) {
				return false;
			}
			option->count = rs->getResultNum();
			return (checkDigest(txn, alloc, container, rs, "") == false);
		}
		}
	} else {
		removeRowWithRowKey(txn, alloc, cxtSrc, keyStoreValue, rowKey, *(option->logRecordList));
	}
	TEST_PRINT("runWithRowKey() E\n");
	return true;
}

bool DbUserHandler::checkDigest(TransactionContext &txn, util::StackAllocator &alloc, 
	BaseContainer *container, ResultSet *rs, const char8_t *digest) {

	OutputMessageRowStore outputMessageRowStore(
		getDataStore(txn.getPartitionId())->getConfig(), container->getColumnInfoList(),
		container->getColumnNum(), *(rs->getRowDataFixedPartBuffer()),
		*(rs->getRowDataVarPartBuffer()), false /*isRowIdIncluded*/);
	ResultSize resultNum;
	container->getRowList(txn, *(rs->getOIdList()), rs->getResultNum(),
		resultNum, &outputMessageRowStore, false /*isRowIdIncluded*/, 0);

	const uint8_t *data1;
	uint32_t size1;
	outputMessageRowStore.getAllFixedPart(data1, size1);
	const uint8_t *data2;
	uint32_t size2;
	outputMessageRowStore.getAllVariablePart(data2, size2);

	InputMessageRowStore inputMessageRowStore(
		getDataStore(txn.getPartitionId())->getConfig(), container->getColumnInfoList(),
		container->getColumnNum(),
		reinterpret_cast<void *>(const_cast<uint8_t *>(data1)), size1,
		reinterpret_cast<void *>(const_cast<uint8_t *>(data2)), size2, 1,
		false);

	util::String targetDigest(alloc);
	const uint8_t *field;
	uint32_t fieldSize;

	inputMessageRowStore.next();  

	inputMessageRowStore.getField(COLUMN_ID_USERS_DIGEST, field, fieldSize);  
	targetDigest.append(
		reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
	targetDigest.append("\0");

	if (strcmp(digest, targetDigest.c_str()) != 0) {
		return false;
	}
	return true;
}

void DbUserHandler::makeDatabaseInfoList(const DataStoreConfig& dsConfig,
		TransactionContext &txn, util::StackAllocator &alloc,
		BaseContainer &container, ResultSet &rs,
		util::XArray<DatabaseInfo*> &dbInfoList, util::Vector<DatabaseId> *dbIdList) {
	TEST_PRINT("makeDatabaseInfoList() S\n");

	OutputMessageRowStore outputMessageRowStore(
		dsConfig, container.getColumnInfoList(),
		container.getColumnNum(), *(rs.getRowDataFixedPartBuffer()),
		*(rs.getRowDataVarPartBuffer()), false /*isRowIdIncluded*/);
	ResultSize resultNum;
	container.getRowList(txn, *(rs.getOIdList()), rs.getResultNum(), resultNum,
		&outputMessageRowStore, false /*isRowIdIncluded*/, 0);

	const uint8_t *data1;
	uint32_t size1;
	outputMessageRowStore.getAllFixedPart(data1, size1);
	const uint8_t *data2;
	uint32_t size2;
	outputMessageRowStore.getAllVariablePart(data2, size2);

	InputMessageRowStore inputMessageRowStore(dsConfig,
		container.getColumnInfoList(), container.getColumnNum(),
		reinterpret_cast<void *>(const_cast<uint8_t *>(data1)), size1,
		reinterpret_cast<void *>(const_cast<uint8_t *>(data2)), size2,
		resultNum, false);

	util::Map<util::String, DatabaseInfo *> dbMap(alloc);
	util::Map<util::String, DatabaseInfo *>::iterator it;

	const uint8_t *field;
	uint32_t fieldSize;
	util::String dbName(alloc);
	DatabaseInfo *dbInfo;
	int8_t property;

	util::XArray<RowId> rowIdList(alloc);
	if (dbIdList) {
		container.getRowIdList(txn, *(rs.getOIdList()), rowIdList);
	}

	for (size_t i = 0; i < rs.getResultNum(); i++) {
		inputMessageRowStore.next();  
		inputMessageRowStore.getField(COLUMN_ID_DBS_DBUSERNAME, field, fieldSize);  
		util::String dbUserName(alloc);
		dbUserName.append(
			reinterpret_cast<const char*>(field), (size_t)fieldSize);

		inputMessageRowStore.getField(COLUMN_ID_DBS_DBNAME, field, fieldSize);  
		dbName.append(
			reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
		dbName.append("\0");

		it = dbMap.find(dbName);
		if (it == dbMap.end()) {
			dbInfo = ALLOC_NEW(alloc) DatabaseInfo(alloc);
			dbInfo->dbName_.append(dbName);
			dbInfo->property_ = 0;
			dbInfoList.push_back(dbInfo);
			if (dbIdList) {
				dbIdList->push_back(rowIdList[i] + DBID_RESERVED_RANGE);
			}
			dbMap.insert(std::make_pair(dbName, dbInfo));
		}
		else {
			dbInfo = it->second;
		}
		dbName.clear();

		inputMessageRowStore.getField(COLUMN_ID_DBS_PROPERTY, field, fieldSize);  
		property = *reinterpret_cast<int8_t *>(const_cast<uint8_t *>(field));

		if (property == 0) {  
			PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

			inputMessageRowStore.getField(COLUMN_ID_DBS_USERNAME, field, fieldSize);  
			prInfo->userName_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			prInfo->userName_.append("\0");

			inputMessageRowStore.getField(COLUMN_ID_DBS_PRIVILEGE, field, fieldSize);  
			prInfo->privilege_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			prInfo->privilege_.append("\0");

			dbInfo->privilegeInfoList_.push_back(prInfo);
		}
	}
	TEST_PRINT("makeDatabaseInfoList() E\n");
}

bool DbUserHandler::checkPrivilege(const DataStoreConfig& dsConfig, TransactionContext &txn, util::StackAllocator &alloc,
	BaseContainer *container, ResultSet *rs, DatabaseInfo &dbInfo) {

	ResultSize resultNum = rs->getResultNum();
	
	util::XArray<DatabaseInfo *> dbInfoList(alloc);
	makeDatabaseInfoList(dsConfig, txn, alloc, *container, *rs, dbInfoList);
	if (dbInfoList.size() > 1) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	} else if (dbInfoList.size() == 1) {
		if (dbInfoList[0]->privilegeInfoList_.size() != resultNum) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}
	const char *userName = dbInfo.privilegeInfoList_[0]->userName_.c_str();
	const char *privilege = dbInfo.privilegeInfoList_[0]->privilege_.c_str();

	for (ResultSize i = 0; i < resultNum; i++) {
		const char *targetUserName =
			dbInfoList[0]->privilegeInfoList_[i]->userName_.c_str();
		if (strcmp(userName, targetUserName) == 0) {
			const char *targetPrivilege =
				dbInfoList[0]->privilegeInfoList_[i]->privilege_.c_str();
			if (strcmp(privilege, targetPrivilege) == 0) {
				return true;
			} else {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_OTHER_PRIVILEGE_EXISTS,
					"(input user name = " << userName << ", input database name = "
										  << dbInfo.dbName_.c_str() << ")");  
			}
		}
	}

	return false;
}

void DbUserHandler::makeUserInfoList(
		TransactionContext &txn, util::StackAllocator &alloc,
		BaseContainer &container, ResultSet &rs,
		util::XArray<UserInfo*> &userInfoList) {
		OutputMessageRowStore outputMessageRowStore(
			getDataStore(txn.getPartitionId())->getConfig(), container.getColumnInfoList(),
			container.getColumnNum(), *(rs.getRowDataFixedPartBuffer()),
			*(rs.getRowDataVarPartBuffer()), false /*isRowIdIncluded*/);
		ResultSize resultNum;
		container.getRowList(txn, *(rs.getOIdList()), rs.getResultNum(),
			resultNum, &outputMessageRowStore, false /*isRowIdIncluded*/, 0);

		const uint8_t *data1;
		uint32_t size1;
		outputMessageRowStore.getAllFixedPart(data1, size1);
		const uint8_t *data2;
		uint32_t size2;
		outputMessageRowStore.getAllVariablePart(data2, size2);

		InputMessageRowStore inputMessageRowStore(
			getDataStore(txn.getPartitionId())->getConfig(), container.getColumnInfoList(),
			container.getColumnNum(),
			reinterpret_cast<void *>(const_cast<uint8_t *>(data1)), size1,
			reinterpret_cast<void *>(const_cast<uint8_t *>(data2)), size2,
			resultNum, false);

		const uint8_t *field;
		uint32_t fieldSize;
		util::String userName(alloc);
		for (ResultSize i = 0; i < rs.getResultNum(); i++) {
			inputMessageRowStore.next();  

			UserInfo *outUserInfo = ALLOC_NEW(alloc) UserInfo(alloc);

			inputMessageRowStore.getField(COLUMN_ID_USERS_USERNAME, field, fieldSize);  
			outUserInfo->userName_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			outUserInfo->userName_.append("\0");

			inputMessageRowStore.getField(COLUMN_ID_USERS_DIGEST, field, fieldSize);  
			outUserInfo->digest_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			outUserInfo->digest_.append("\0");

			inputMessageRowStore.getField(COLUMN_ID_USERS_PROPERTY, field, fieldSize);  
			outUserInfo->property_ =
				(*reinterpret_cast<int8_t *>(const_cast<uint8_t *>(field)));

			if (strcmp(outUserInfo->digest_.c_str(), "") != 0) {
				outUserInfo->withDigest_ = true;
			} else {
				outUserInfo->withDigest_ = false;
				outUserInfo->isGroupMapping_ = transactionService_->getManager()->isRoleMappingByGroup();
				outUserInfo->roleName_.swap(outUserInfo->userName_);
			}

			userInfoList.push_back(outUserInfo);
		}

}

void DbUserHandler::removeRowWithRS(TransactionContext& txn, util::StackAllocator& alloc,
	const TransactionManager::ContextSource& cxtSrc,
	KeyDataStoreValue& keyStoreValue,
	BaseContainer& container, ResultSet& rs,
	util::XArray<const util::XArray<uint8_t>*>& logRecordList) {

	util::XArray<RowId> rowIdList(alloc);
	container.getRowIdList(txn, *(rs.getOIdList()), rowIdList);

	util::XArray<RowId> rowIds(alloc);
	for (size_t i = 0; i < rowIdList.size(); i++) {
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		DSInputMes input(alloc, DS_REMOVE_ROW_BY_ID, rowIdList[i],
			MAX_SCHEMAVERSIONID);
		DSOutputMes* ret = static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));

		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			cxtSrc, cxtSrc.stmtId_, keyStoreValue.storeType_, ret->dsLog_);
		logRecordList.push_back(logBinary);

		transactionManager_->update(txn, cxtSrc.stmtId_);

		rowIds.clear();
	}
}

void DbUserHandler::runWithTQL(
		EventContext &ec, const TransactionManager::ContextSource &cxtSrc, 
		const char8_t *containerName, const char8_t *tql,
		DUQueryInOut *option) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, 0 /*pId*/,
			TXN_EMPTY_CLIENTID, cxtSrc, now, emNow);
	txn.setAuditInfo(NULL, NULL, NULL);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		containerName, static_cast<uint32_t>(strlen(containerName)));
	bool isCaseSensitive = false;

	KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
		containerKey, isCaseSensitive);
	if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
		switch (option->phase) {
		case DUQueryInOut::AUTH:
			assert(option->type == DUQueryInOut::AGG);
			assert(option->s);
			assert(strcmp(containerName, GS_USERS) == 0);
			TEST_PRINT1("%s not found.\n", containerName);
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"user name invalid (" << option->s << ")");
		case DUQueryInOut::GET:
			return;
		case DUQueryInOut::NORMAL:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
	const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
	DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
	DSContainerOutputMes* ret = static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));
	BaseContainer* container = ret->container_;

	ResultSet *rs = getDataStore(txn.getPartitionId())->getResultSetManager()->create(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *getDataStore(txn.getPartitionId()), *rs);

	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, tql), *rs);

	rs->setResultType(RESULT_ROWSET);

	TEST_PRINT1("getResultNum() %d\n", rs->getResultNum());
	switch (option->type) {
	case DUQueryInOut::RESULT_NUM:
		option->count = rs->getResultNum();
		break;
	case DUQueryInOut::AGG: {
		int8_t *tmp = reinterpret_cast<int8_t *>(
			const_cast<uint8_t *>(rs->getFixedStartData()));
		tmp++;
		option->count = *(reinterpret_cast<int64_t *>(tmp));
		break;
	}
	case DUQueryInOut::USER_DETAILS: {
		int64_t count = rs->getResultNum();
		if (count > 0) {
			option->flag = true;
			if (checkDigest(txn, alloc, container, rs, option->s) == false) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NAME_ALREADY_EXISTS, "");
			}
		} else {
			option->flag = false;
		}
		break;
	}
	case DUQueryInOut::DB_DETAILS: {
		option->flag = checkPrivilege(getDataStore(txn.getPartitionId())->getConfig(), txn, alloc, container, rs, *(option->dbInfo));
		break;
	}
	case DUQueryInOut::USER_INFO: {
		makeUserInfoList(txn, alloc, *container, *rs, *(option->userInfoList));
		break;
	}
	case DUQueryInOut::DB_INFO: {
		if (option->dbNameSpecified && rs->getResultNum() == 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TXN_CURRENT_DATABASE_REMOVED, "[GetDatabases]");
		}
		makeDatabaseInfoList(getDataStore(txn.getPartitionId())->getConfig(), txn, alloc, *container, *rs, *(option->dbInfoList));
		break;
	}
	case DUQueryInOut::REMOVE: {
		removeRowWithRS(txn, alloc, cxtSrc, keyStoreValue,
			*container, *rs, *(option->logRecordList));
		break;
	}
	}
}


/*!
	@brief Checks if a specified password(not digest) is proper length
*/
void DbUserHandler::checkPasswordLength(const char8_t *password) {
	if (strlen(password) > PASSWORD_SIZE_MAX) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"password length exceeds maximum size.");
	}
}

void DbUserHandler::checkUserName(const char8_t *userName, bool detailed) {
	if (detailed) {
		NoEmptyKey::validate(
			KeyConstraint::getUserKeyConstraint(USER_NAME_SIZE_MAX),
			userName, static_cast<uint32_t>(strlen(userName)),
			"userName");
	}

	size_t len = strlen(userName);
	if (len == 5) {
		if (util::stricmp(userName, GS_CAPITAL_ADMIN_USER) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NAME_INVALID,
				"(user name = " << userName << ")");
		}
	}
	else if (len == 6) {
		if (util::stricmp(userName, GS_CAPITAL_SYSTEM_USER) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NAME_INVALID,
				"(user name = " << userName << ")");
		}
	}

	if (len >= 3) {
		char head[4];
		memset(head, 0, 4);
		memcpy(head, userName, 3);
		if (util::stricmp(head, GS_CAPITAL_PREFIX) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NAME_INVALID,
				"(user name = " << userName << ")");
		}
	}
}

void DbUserHandler::checkAdminUser(UserType userType) {
	if (userType == Message::USER_NORMAL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_OPERATION_NOT_ALLOWED,
			"only administrator can execute the statement");
	}
}

void DbUserHandler::checkDatabaseName(const char8_t *dbName) {
	NoEmptyKey::validate(
		KeyConstraint::getUserKeyConstraint(DATABASE_NAME_SIZE_MAX),
		dbName, static_cast<uint32_t>(strlen(dbName)),
		"databaseName");

	size_t len;
	len = strlen(dbName);
	if (len == 6) {
		if (util::stricmp(dbName, GS_CAPITAL_PUBLIC) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_NAME_INVALID,
				"(database name = " << dbName << ")");
		}
	}
	else if (len == 18) {
		if (util::stricmp(dbName, GS_CAPITAL_INFO_SCHEMA) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_NAME_INVALID,
				"(database name = " << dbName << ")");
		}
	}
	if (len >= 3) {
		char head[4];
		memset(head, 0, 4);
		memcpy(head, dbName, 3);
		if (util::stricmp(head, GS_CAPITAL_PREFIX) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_NAME_INVALID,
				"(database name = " << dbName << ")");
		}
	}
}

void DbUserHandler::checkConnectedDatabaseName(
		ConnectionOption &connOption, const char8_t *dbName) {
	if ((connOption.requestType_ != Message::REQUEST_NEWSQL) && 
			strcmp(dbName, connOption.dbName_.c_str()) != 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_DATABASE_NAME_INVALID,
				"database name invalid (" << dbName << ")");
	}
}

void DbUserHandler::checkConnectedUserName(
		ConnectionOption &connOption, const char8_t *userName) {
	if ((connOption.requestType_ != Message::REQUEST_NEWSQL) &&
			(strcmp(userName, connOption.userName_.c_str()) != 0)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
				"user name invalid (" << userName << ")");
	}
}

void DbUserHandler::checkPartitionIdForUsers(PartitionId pId) {
	if (pId != 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CM_INTERNAL_ERROR, "partition id is not 0");
	}
}

/*!
	@brief Checks digest
*/
void DbUserHandler::checkDigest(const char8_t *digest, size_t maxStrLength) {
	size_t len;
	len = strlen(digest);
	if (len <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"invalid digest.");
	}
	else if (len > maxStrLength) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"digest length exceeds maximum size.");
	}
}

/*!
	@brief Checks the size of privilege information
*/
void DbUserHandler::checkPrivilegeSize(DatabaseInfo &dbInfo, size_t size) {
	if (dbInfo.privilegeInfoList_.size() != size) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"privilege number invalid (" << dbInfo.privilegeInfoList_.size()
										 << ")");
	}
}

/*!
	@brief Checks modifiable value
*/
void DbUserHandler::checkModifiable(bool modifiable, bool value) {
	if (modifiable != value) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"modifiable invalid (" << modifiable << ")");
	}
}


void DbUserHandler::initializeMetaContainer(
		EventContext &ec, Event &ev, const Request &request,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	UNUSED_VARIABLE(ev);

	TEST_PRINT("initializeMetaContainer() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	{
		TEST_PRINT("initializeMetaContainer() gs_users\n");

		util::XArray<uint8_t> containerInfo(alloc);
		makeSchema(containerInfo, USER_COLUMN_LIST, COLUMN_ID_USERS_MAX);
		
		putContainer(alloc, containerInfo, GS_USERS, now, emNow, request, logRecordList);
	}
	{
		TEST_PRINT("initializeMetaContainer() gs_databases\n");

		util::XArray<uint8_t> containerInfo(alloc);
		makeSchema(containerInfo, DB_COLUMN_LIST, COLUMN_ID_DBS_MAX);
		
		putContainer(alloc, containerInfo, GS_DATABASES, now, emNow, request, logRecordList);
	}
	TEST_PRINT("initializeMetaContainer() E\n");
}

int64_t DbUserHandler::getCount(
		EventContext &ec, const Request &request, const char8_t *containerName) {
	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	if (strcmp(containerName, GS_DATABASES) == 0) {  
		query.append("select count(*) where property=");
		query.append(PROPERTY_USER_ADMIN);
	}
	else {
		query.append("select count(*)");
	}

	DUQueryInOut option;
	option.setForAgg();
	runWithTQL(ec, request.fixed_.cxtSrc_, containerName, query.c_str(), &option);
	return option.count;
}

void DbUserHandler::checkUser(
		EventContext &ec, const Request &request, const char8_t *userName,
		bool &existFlag, bool isRole) {
	existFlag = false;

	util::StackAllocator &alloc = ec.getAllocator();

	RowKeyData rowKey(alloc);
	makeRowKey(userName, rowKey);
		
	DUGetInOut option;
	if (isRole) {
		option.type = DUGetInOut::USER;
	}
	if (runWithRowKey(ec, request.fixed_.cxtSrc_, GS_USERS, rowKey, &option) == false) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
	}
	existFlag = (option.count > 0);
}

void DbUserHandler::checkDatabase(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo) {
	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	query.append("select * where dbName='");
	query.append(dbInfo.dbName_.c_str());
	query.append("'");
	
	DUQueryInOut option;
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_DATABASES, query.c_str(), &option);
	if (option.count <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_NOT_EXIST,
			"(database name = " << dbInfo.dbName_.c_str() << ")");  
	}			
}

void DbUserHandler::checkDatabaseDetails(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo,
		bool &existFlag) {
	existFlag = false;
	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	query.append("select * where dbName='");
	query.append(dbInfo.dbName_.c_str());
	query.append("' and property=");
	query.append(PROPERTY_USER_NORMAL);

	DUQueryInOut option;
	option.setForDatabaseDetails(&dbInfo);
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_DATABASES, query.c_str(), &option);
	existFlag = option.flag;
}

void DbUserHandler::putDatabaseRow(
		EventContext &ec, Event &ev, const Request &request,
		DatabaseInfo &dbInfo, bool isCreate,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {

	assert(dbInfo.privilegeInfoList_.size() == 1);

	util::StackAllocator &alloc = ec.getAllocator();

	util::String dbUserName(alloc);
	dbUserName.append(dbInfo.dbName_);
	dbUserName.append(":");
	if (isCreate) {
		dbUserName.append(
			"admin");  
	}
	else {
		dbUserName.append(dbInfo.privilegeInfoList_[0]->userName_);
	}

	char elemData = 0;
	if (isCreate) {
		elemData = 1;
	}

	DUColumnValue cvList[5] = { 
		{COLUMN_TYPE_STRING, dbUserName.c_str(), 0},
		{COLUMN_TYPE_STRING, dbInfo.dbName_.c_str(), 0},
		{COLUMN_TYPE_BYTE, NULL, elemData},
		{COLUMN_TYPE_STRING, dbInfo.privilegeInfoList_[0]->userName_.c_str(), 0},
		{COLUMN_TYPE_STRING, dbInfo.privilegeInfoList_[0]->privilege_.c_str(), 0},
	};

	putRow(ec, ev, request, GS_DATABASES, cvList, logRecordList);
}

void DbUserHandler::getUserInfoList(
		EventContext &ec, 
		const Request &request, const char8_t *userName,
		UserType userType, util::XArray<UserInfo*> &userInfoList) {

	util::StackAllocator &alloc = ec.getAllocator();
			
	util::String query(alloc);
	if (userType != Message::USER_NORMAL) {  
		query.append("select *");
	}
	else {
		query.append("select * where userName='");
		query.append(userName);
		query.append("'");
	}

	DUQueryInOut option;
	option.setForUserInfoList(&userInfoList);
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_USERS, query.c_str(), &option);
}


void PutUserHandler::checkUserDetails(
		EventContext &ec, const Request &request, const char8_t *userName,
		const char8_t *digest, bool detailFlag, bool &existFlag) {
	TEST_PRINT("checkUserDetails() S\n");
	existFlag = false;

	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	query.append("select * where UPPER(userName)=UPPER('");
	query.append(userName);
	query.append("')");
	
	DUQueryInOut option;
	if (detailFlag) {
		option.setForUserDetails(digest);
		runWithTQL(ec, request.fixed_.cxtSrc_, GS_USERS, query.c_str(), &option);
		existFlag = option.flag;
	} else {
		runWithTQL(ec, request.fixed_.cxtSrc_, GS_USERS, query.c_str(), &option);
		existFlag = (option.count > 0);
	}
	TEST_PRINT("checkUserDetails() E\n");
}


void PutUserHandler::putUserRow(
		EventContext &ec, Event &ev, const Request &request,
		UserInfo &userInfo,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	TEST_PRINT("putUserRow() S\n");
	util::StackAllocator &alloc = ec.getAllocator();
	UNUSED_VARIABLE(alloc); 

	DUColumnValue cvList[3] = { 
		{COLUMN_TYPE_STRING, userInfo.userName_.c_str(), 0},
		{COLUMN_TYPE_STRING, userInfo.digest_.c_str(), 0},
		{COLUMN_TYPE_BYTE, 0, 0},
	};

	putRow(ec, ev, request, GS_USERS, cvList, logRecordList);
	TEST_PRINT("putUserRow() E\n");
}

void PutUserHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<PutUserHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		UserInfo& userInfo = inMes.userInfo_;
		bool modifiable = inMes.modifiable_;

		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("\t%s\n", userInfo.dump().c_str());
		TEST_PRINT1("\tmodifiable = %d\n", modifiable);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkPartitionIdForUsers(request.fixed_.pId_);
		if (connOption.userType_ == Message::USER_NORMAL) {
			checkModifiable(modifiable, true);
			checkConnectedUserName(connOption, userInfo.userName_.c_str());
			if (connOption.isLDAPAuthentication_) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_OPERATION_NOT_ALLOWED,
					"LDAP user can't execute the statement");
			}
		}
		checkUserName(userInfo.userName_.c_str(), true);
		if (strlen(userInfo.digest_.c_str()) == 0) {
			checkModifiable(modifiable, false);
		} else {
			checkDigest(userInfo.digest_.c_str(),
				getDataStore(request.fixed_.pId_)->getConfig().getLimitSmallSize());
		}

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			if (checkContainer(ec, request, GS_USERS) ||
				checkContainer(ec, request, GS_DATABASES)) {
				initializeMetaContainer(ec, ev, request, logRecordList);
			}
			bool existFlag;
			if (modifiable == false) {  
				if (strlen(userInfo.digest_.c_str()) == 0) {
					checkUser(ec, request, userInfo.userName_.c_str(), existFlag);
				} else {
					checkUserDetails(ec, request, userInfo.userName_.c_str(),
						userInfo.digest_.c_str(), true, existFlag);  
				}
				if (existFlag == false) {
					if (getCount(ec, request, GS_USERS) >=
						static_cast<int64_t>(USER_NUM_MAX)) {
						GS_THROW_USER_ERROR(
							GS_ERROR_TXN_USER_LIMIT_OVER, "[PutUser]");
					}
					putUserRow(ec, ev, request, userInfo, logRecordList);
				}
			}
			else {  
				checkUser(ec, request, userInfo.userName_.c_str(), existFlag, true);
				if (existFlag == false) {
					if (connOption.userType_ == Message::USER_NORMAL) {
						GS_THROW_USER_ERROR(
							GS_ERROR_TXN_CURRENT_USER_REMOVED, "[PutUser]");
					}
					else {
						GS_THROW_USER_ERROR(
							GS_ERROR_TXN_USER_NOT_EXIST, "[PutUser]");
					}
				}
				putUserRow(ec, ev, request, userInfo, logRecordList);
			}
		}
		catch (std::exception &) {
			const util::DateTime now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
			txn.setAuditInfo(NULL, NULL, NULL);

			Response response(alloc);
			executeReplication(
					request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
					ev.getType(), txn.getLastStatementId(),
					TransactionManager::REPLICATION_ASYNC, NULL, 0,
					logRecordList.data(), logRecordList.size(), NULL);

			throw;
		}

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(NULL, NULL, NULL);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false));
		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		TEST_PRINT("<<<PutUserHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}

}

void DropUserHandler::removeUserRowInDB(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *userName,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList) {
	UNUSED_VARIABLE(ev);

	TEST_PRINT("removeUserRowInDB() S\n");
	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	query.append("select * where userName='");
	query.append(userName);
	query.append("'");

	DUQueryInOut option;
	option.setForRemove(&logRecordList);
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_DATABASES, query.c_str(), &option);
	TEST_PRINT("removeUserRowInDB() E\n");
}

void DropUserHandler::removeUserRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *userName,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	UNUSED_VARIABLE(ev);

	TEST_PRINT("removeUserRow() S\n");
	util::StackAllocator &alloc = ec.getAllocator();

	RowKeyData rowKey(alloc);
	makeRowKey(userName, rowKey);
			
	DUGetInOut option;
	option.setForRemove(&logRecordList);
	runWithRowKey(ec, request.fixed_.cxtSrc_, GS_USERS, rowKey, &option);
	TEST_PRINT("removeUserRow() E\n");
}

void DropUserHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<DropUserHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		util::String& userName = inMes.userName_;
		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("\tuserName=%s\n", userName.c_str());

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkAdminUser(connOption.userType_);
		checkPartitionIdForUsers(request.fixed_.pId_);
		checkUserName(userName.c_str(), false);


		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			bool existFlag;
			checkUser(ec, request, userName.c_str(), existFlag);

			if (existFlag == true) {
				removeUserRowInDB(
					ec, ev, request, userName.c_str(), logRecordList);

				removeUserRow(ec, ev, request, userName.c_str(), logRecordList);
			}

		}
		catch (std::exception &) {
			const util::DateTime now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
			txn.setAuditInfo(NULL, NULL, NULL);

			Response response(alloc);
			executeReplication(
					request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
					ev.getType(), txn.getLastStatementId(),
					TransactionManager::REPLICATION_ASYNC, NULL, 0,
					logRecordList.data(), logRecordList.size(), NULL);

			throw;
		}

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(NULL, NULL, NULL);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false));
		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
		
		TEST_PRINT("<<<DropUserHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void GetUsersHandler::checkNormalUser(UserType userType, const char8_t *userName) {
	if ((userType == Message::USER_NORMAL) &&
			(strcmp(userName, "") == 0)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"[GetUsers] user name must specified");
	}
}

void GetUsersHandler::makeUserInfoListForAdmin(util::StackAllocator &alloc,
	const char8_t *userName, util::XArray<UserInfo*> &userInfoList) {
	TEST_PRINT("makeUserInfoListForAdmin() S\n");
	UserInfo *outUserInfo = ALLOC_NEW(alloc) UserInfo(alloc);
	outUserInfo->userName_.append(userName);
	outUserInfo->property_ = 1;
	outUserInfo->withDigest_ = false;
	userInfoList.push_back(outUserInfo);
	TEST_PRINT("makeUserInfoListForAdmin() E\n");
}

void GetUsersHandler::makeUserInfoListForLDAPUser(util::StackAllocator &alloc,
	ConnectionOption &connOption, util::XArray<UserInfo*> &userInfoList) {
	TEST_PRINT("makeUserInfoListForLDAPUser() S\n");
	UserInfo *outUserInfo = ALLOC_NEW(alloc) UserInfo(alloc);
	outUserInfo->userName_.append(connOption.userName_.c_str());
	outUserInfo->property_ = 0;
	outUserInfo->withDigest_ = false;
	outUserInfo->isGroupMapping_ = transactionService_->getManager()->isRoleMappingByGroup();
	outUserInfo->roleName_.append(connOption.roleName_.c_str());
	userInfoList.push_back(outUserInfo);
	TEST_PRINT("makeUserInfoListForLDAPUser() E\n");
}


void GetUsersHandler::checkUserInfoList(int32_t featureVersion, util::XArray<UserInfo *> &userInfoList) {
	if (featureVersion < StatementMessage::FEATURE_V4_5) {
		for (size_t i = 0; i < userInfoList.size(); i++) {
			if (strcmp(userInfoList[i]->userName_.c_str(), "") == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create user information list");
			}
		}
	}
}

void GetUsersHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<GetUsersHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		util::String userName = inMes.userName_;
		bool withFilter = inMes.withFilter_;
		int8_t property = inMes.property_;  
		UNUSED_VARIABLE(property); 

		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("\twithFilter=%d\n", withFilter);
		TEST_PRINT1("\tuserName=%s\n", userName.c_str());
		TEST_PRINT1("\tproperty=%d\n", property);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkPartitionIdForUsers(request.fixed_.pId_);
		if (withFilter) {
			checkConnectedUserName(connOption, userName.c_str());
		}
		checkNormalUser(connOption.userType_, userName.c_str());

		TransactionManager* txnMgr = transactionService_->getManager();
		

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		if (withFilter && (connOption.userType_ != Message::USER_NORMAL)) {
			makeUserInfoListForAdmin(alloc, userName.c_str(), response.userInfoList_);
		}
		else if (withFilter && connOption.isLDAPAuthentication_) {
			makeUserInfoListForLDAPUser(alloc, connOption, response.userInfoList_);
		}
		else {
			if (connOption.isLDAPAuthentication_ && txnMgr->isRoleMappingByGroup()) {
				getUserInfoList(ec, request, connOption.roleName_.c_str(),
					connOption.userType_, response.userInfoList_);
			} else {
				getUserInfoList(ec, request, connOption.userName_.c_str(),
					connOption.userType_, response.userInfoList_);
			}
		}

		checkUserInfoList(request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(), response.userInfoList_);

		if (connOption.userType_ == Message::USER_NORMAL) {
			if (response.userInfoList_.size() == 0) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TXN_CURRENT_USER_REMOVED, "[GetUsers]");
			}
			else if (response.userInfoList_.size() > 1) {
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
					"(userInfoList size = " << response.userInfoList_.size()
											<< ")");
			}
		}

		TEST_PRINT("[ResponseMesg]\n");
		TEST_PRINT1(
			"\tuserInfoList.size() %d\n", response.userInfoList_.size());
		for (size_t i = 0; i < response.userInfoList_.size(); i++) {
			TEST_PRINT1("%s\n", response.userInfoList_[i]->dump().c_str());
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false),
			&(response.userInfoList_), request);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, false);
		
		TEST_PRINT("<<<GetUsersHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void PutDatabaseHandler::setPrivilegeInfoListForAdmin(util::StackAllocator &alloc,
	const char8_t *userName, util::XArray<PrivilegeInfo *> &privilegeInfoList) {
	TEST_PRINT("setPrivilegeInfoListForAdmin() S\n");
	PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

	prInfo->userName_.append(userName);
	prInfo->privilege_.append("ALL");

	privilegeInfoList.push_back(prInfo);
	TEST_PRINT("setPrivilegeInfoListForAdmin() E\n");
}

void PutDatabaseHandler::checkDatabase(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo,
	bool &existFlag) {
	TEST_PRINT("checkDatabase() S\n");
	existFlag = false;
	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	query.append("select * where UPPER(dbName)=UPPER('");
	query.append(dbInfo.dbName_.c_str());
	query.append("') and property=");
	query.append(PROPERTY_USER_ADMIN);
		
	DUQueryInOut option;
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_DATABASES, query.c_str(), &option);
	existFlag = (option.count > 0);
	TEST_PRINT("checkDatabase() E\n");
}

void PutDatabaseHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<PutDatabaseHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		DatabaseInfo& dbInfo = inMes.dbInfo_;
		bool modifiable = inMes.modifiable_;

		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("%s\n", dbInfo.dump().c_str());
		TEST_PRINT1("\tmodifiable=%d\n", modifiable);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkAdminUser(connOption.userType_);
		checkPartitionIdForUsers(request.fixed_.pId_);
		checkModifiable(modifiable, false);
		checkDatabaseName(dbInfo.dbName_.c_str());
		checkPrivilegeSize(dbInfo, 0);


		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			setPrivilegeInfoListForAdmin(alloc, connOption.userName_.c_str(), dbInfo.privilegeInfoList_);

			if (checkContainer(ec, request, GS_USERS) ||
				checkContainer(ec, request, GS_DATABASES)) {
				initializeMetaContainer(ec, ev, request, logRecordList);
			}
			bool existFlag;
			checkDatabase(ec, request, dbInfo, existFlag);
			if (existFlag == false) {
				if (getCount(ec, request, GS_DATABASES) >=
					static_cast<int64_t>(DATABASE_NUM_MAX)) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TXN_DATABASE_LIMIT_OVER, "[PutDatabase]");
				}
				putDatabaseRow(
					ec, ev, request, dbInfo, true, logRecordList);  
			}

		}
		catch (std::exception &) {
			const util::DateTime now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
			txn.setAuditInfo(NULL, NULL, NULL);

			Response response(alloc);
			executeReplication(
					request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
					ev.getType(), txn.getLastStatementId(),
					TransactionManager::REPLICATION_ASYNC, NULL, 0,
					logRecordList.data(), logRecordList.size(), NULL);

			throw;
		}
		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(NULL, NULL, NULL);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false));
		bool ackWait = false;
		ackWait = executeReplication(
				request, ec, alloc, ev.getSenderND(), txn,
				ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		TEST_PRINT("<<<PutDatabaseHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void DropDatabaseHandler::removeDatabaseRow(
		EventContext &ec, Event &ev,
		const Request &request, const char8_t *dbName, bool isAdmin,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList) {
	UNUSED_VARIABLE(ev);

	TEST_PRINT("removeDatabaseRow() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
			
	util::String query(alloc);
	query.append("select * where dbName='");
	query.append(dbName);
	query.append("' and property=");
	if (isAdmin) {
		query.append(PROPERTY_USER_ADMIN);
	}
	else {
		query.append(PROPERTY_USER_NORMAL);
	}

	DUQueryInOut option;
	option.setForRemove(&logRecordList);
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_DATABASES, query.c_str(), &option);
	TEST_PRINT("removeDatabaseRow() E\n");
}

void DropDatabaseHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<DropDatabaseHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		util::String& dbName = inMes.dbName_;

		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("\tdbName=%s\n", dbName.c_str());

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkAdminUser(connOption.userType_);
		checkPartitionIdForUsers(request.fixed_.pId_);
		checkConnectedDatabaseName(connOption, dbName.c_str());
		checkDatabaseName(dbName.c_str());


		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			removeDatabaseRow(
				ec, ev, request, dbName.c_str(), false, logRecordList);
			removeDatabaseRow(
				ec, ev, request, dbName.c_str(), true, logRecordList);
		}
		catch (std::exception &) {
			const util::DateTime now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
			txn.setAuditInfo(NULL, NULL, NULL);

			Response response(alloc);
			executeReplication(
					request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
					ev.getType(), txn.getLastStatementId(),
					TransactionManager::REPLICATION_ASYNC, NULL, 0,
					logRecordList.data(), logRecordList.size(), NULL);

			throw;
		}

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(NULL, NULL, NULL);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false));
		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		TEST_PRINT("<<<DropDatabaseHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void GetDatabasesHandler::makeDatabaseInfoListForPublic(
		util::StackAllocator &alloc,
		util::XArray<UserInfo *> &userInfoList,
		util::XArray<DatabaseInfo *> &dbInfoList) {
	TEST_PRINT("makeDatabaseInfoListForPublic() S\n");
	DatabaseInfo *dbInfo = ALLOC_NEW(alloc) DatabaseInfo(alloc);
	dbInfo->dbName_.append(GS_PUBLIC);
	dbInfo->property_ = 0;
	dbInfoList.push_back(dbInfo);

	for (size_t i = 0; i < userInfoList.size(); i++) {
		PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

		prInfo->userName_ = userInfoList[i]->userName_;
		prInfo->privilege_.append("ALL");

		dbInfo->privilegeInfoList_.push_back(prInfo);
	}
	TEST_PRINT("makeDatabaseInfoListForPublic() E\n");
}

void GetDatabasesHandler::getDatabaseInfoList(
		EventContext &ec, const Request &request,
		const char8_t *dbName, const char8_t *userName,
		util::XArray<DatabaseInfo *> &dbInfoList) {
	TEST_PRINT("getDatabaseInfoList() S\n");
			
	bool dbNameSpecified = false;
	util::StackAllocator &alloc = ec.getAllocator();

	util::String query(alloc);
	if (strcmp(userName, "") == 0) {
		if (strcmp(dbName, "") == 0) {
			TEST_PRINT("TQL ALL\n");
			query.append("select *");
		}
		else {
			dbNameSpecified = true;
			TEST_PRINT("TQL with dbName\n");
			query.append("select * where dbName='");
			query.append(dbName);
			query.append("'");
		}
	}
	else {
		if (strcmp(dbName, "") == 0) {
			TEST_PRINT("TQL with userName\n");
			query.append("select * where userName='");
			query.append(userName);
			query.append("'");
		}
		else {
			dbNameSpecified = true;
			TEST_PRINT("TQL with dbName and userName\n");
			query.append("select * where dbName='");
			query.append(dbName);
			query.append("' and userName='");
			query.append(userName);
			query.append("'");
		}
	}

	DUQueryInOut option;
	option.setForDbInfoList(dbNameSpecified, &dbInfoList);
	runWithTQL(ec, request.fixed_.cxtSrc_, GS_DATABASES, query.c_str(), &option);
	TEST_PRINT("getDatabaseInfoList() E\n");
}

void GetDatabasesHandler::checkDatabaseInfoList(int32_t featureVersion, util::XArray<DatabaseInfo *> &dbInfoList) {
	if (featureVersion < StatementMessage::FEATURE_V4_3) {
		for (size_t i = 0; i < dbInfoList.size(); i++) {
			if (dbInfoList[i]->privilegeInfoList_.size() > 1) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create database information list");
			}
			
			for (size_t j = 0; j < dbInfoList[i]->privilegeInfoList_.size(); j++) {
				if (strcmp(dbInfoList[i]->privilegeInfoList_[j]->privilege_.c_str(), "READ") == 0) {
					GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
						"Can not create database information list");
				}
			}
		}
	}
}

void GetDatabasesHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<GetDatabasesHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		util::String& dbName = inMes.dbName_;
		bool withFilter = inMes.withFilter_;
		int8_t property = inMes.property_;  
		UNUSED_VARIABLE(property); 

		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("\twithFilter=%d\n", withFilter);
		TEST_PRINT1("\tdbName=%s\n", dbName.c_str());
		TEST_PRINT1("\tproperty=%d\n", property);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkPartitionIdForUsers(request.fixed_.pId_);
		if (withFilter) {
			checkConnectedDatabaseName(connOption, dbName.c_str());
		}

		TransactionManager* txnMgr = transactionService_->getManager();

		
		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		if (strcmp(dbName.c_str(), GS_PUBLIC) == 0) {
			getUserInfoList(ec, request, connOption.userName_.c_str(),
				connOption.userType_, response.userInfoList_);
			makeDatabaseInfoListForPublic(
				alloc, response.userInfoList_, response.databaseInfoList_);
		}
		else {
			if (connOption.userType_ != Message::USER_NORMAL) {
				getDatabaseInfoList(ec, request, dbName.c_str(), "",
					response.databaseInfoList_);
			}
			else {
				if (connOption.isLDAPAuthentication_ && txnMgr->isRoleMappingByGroup()) {
					getDatabaseInfoList(ec, request, dbName.c_str(),
						connOption.roleName_.c_str(), response.databaseInfoList_);
				} else {
					getDatabaseInfoList(ec, request, dbName.c_str(),
						connOption.userName_.c_str(), response.databaseInfoList_);
				}
			}
		}
		if (request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>() < StatementMessage::FEATURE_V4_5) {
			if (connOption.isLDAPAuthentication_) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create database information list");
			}
		}
		checkDatabaseInfoList(request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(), response.databaseInfoList_);

		TEST_PRINT("[ResponseMesg]\n");
		TEST_PRINT1(
			"\tdbInfoList.size() %d\n", response.databaseInfoList_.size());
		for (size_t i = 0; i < response.databaseInfoList_.size(); i++) {
			TEST_PRINT1("%s\n", response.databaseInfoList_[i]->dump().c_str());
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false),
			&(response.databaseInfoList_));
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, false);

		TEST_PRINT("<<<GetDatabasesHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void PutPrivilegeHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<PutPrivilegeHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		DatabaseInfo& dbInfo = inMes.dbInfo_;

		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("dbInfo=%s\n", dbInfo.dump().c_str());

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkAdminUser(connOption.userType_);
		checkPartitionIdForUsers(request.fixed_.pId_);
		checkPrivilegeSize(dbInfo, 1); 
		
		
		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		bool ackWait = false;

		bool existFlag;
		checkUser(ec, request, dbInfo.privilegeInfoList_[0]->userName_.c_str(),
			existFlag);
		if (existFlag == false) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NOT_EXIST,
				"(user name = "
					<< dbInfo.privilegeInfoList_[0]->userName_.c_str() << ")");
		}

		checkDatabase(ec, request, dbInfo);

		checkDatabaseDetails(ec, request, dbInfo, existFlag);
		if (existFlag == false) {
			putDatabaseRow(ec, ev, request, dbInfo, false,
				logRecordList);  
		}
		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(NULL, NULL, NULL);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false));
		ackWait = executeReplication(
				request, ec, alloc, ev.getSenderND(), txn,
				ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		TEST_PRINT("<<<PutPrivilegeHandler>>> END\n");

	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void DropPrivilegeHandler::removeDatabaseRow(
		EventContext &ec, Event &ev, const Request &request, 
		DatabaseInfo &dbInfo,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList) {
	UNUSED_VARIABLE(ev);

	TEST_PRINT("removeDatabaseRow() S\n");
	util::StackAllocator &alloc = ec.getAllocator();

	RowKeyData rowKey(alloc);
	makeRowKey(dbInfo, rowKey);

	DUGetInOut option;
	option.setForRemove(&logRecordList);
	runWithRowKey(ec, request.fixed_.cxtSrc_, GS_DATABASES, rowKey, &option);
	TEST_PRINT("removeDatabaseRow() E\n");
}

void DropPrivilegeHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<DropPrivilegeHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();
	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		DatabaseInfo& dbInfo = inMes.dbInfo_;
		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("dbInfo=%s\n", dbInfo.dump().c_str());

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkAdminUser(connOption.userType_);
		checkPartitionIdForUsers(request.fixed_.pId_);
		checkPrivilegeSize(dbInfo, 1); 
		checkDatabaseName(dbInfo.dbName_.c_str());

		
		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		bool ackWait = false;

		bool existFlag;
		checkUser(ec, request, dbInfo.privilegeInfoList_[0]->userName_.c_str(),
			existFlag);
		if (existFlag == false) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NOT_EXIST,
				"(user name = "
					<< dbInfo.privilegeInfoList_[0]->userName_.c_str() << ")");
		}

		checkDatabase(ec, request, dbInfo);

		checkDatabaseDetails(ec, request, dbInfo, existFlag);
		if (existFlag) {
			removeDatabaseRow(ec, ev, request, dbInfo, logRecordList);
		}

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(NULL, NULL, NULL);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, response.compositeIndexInfos_, false));
		ackWait = executeReplication(
				request, ec, alloc, ev.getSenderND(), txn,
				ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		TEST_PRINT("<<<DropPrivilegeHandler>>> END\n");

	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}
