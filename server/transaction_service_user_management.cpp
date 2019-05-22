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

#include "util/container.h"

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

#include "base_container.h"
#include "data_store.h"
#include "message_row_store.h"  
#include "query_processor.h"
#include "result_set.h"
#include "message_schema.h"


#ifndef _WIN32
#include <signal.h>  
#endif

#define TXN_THROW_DENY_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(DenyException, errorCode, message)

#define TXN_TRACE_HANDLER_CALLED(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler called. (nd=" << ev.getSenderND() \
							   << ", pId=" << ev.getPartitionId() << ")")


/*!
	@brief Checks user name
*/
void StatementHandler::checkUserName(const char8_t *userName, bool detailed) {
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

/*!
	@brief Checks if user is admin
*/
void StatementHandler::checkAdminUser(UserType userType) {
	if (userType == Message::USER_NORMAL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_OPERATION_NOT_ALLOWED,
			"only administrator can execute the statement");
	}
}

/*!
	@brief Checks database name
*/
void StatementHandler::checkDatabaseName(const char8_t *dbName) {
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

/*!
	@brief Checks connected database name
*/
void StatementHandler::checkConnectedDatabaseName(
		ConnectionOption &connOption, const char8_t *dbName) {
	if ((connOption.requestType_ != Message::REQUEST_NEWSQL) && 
			strcmp(dbName, connOption.dbName_.c_str()) != 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_DATABASE_NAME_INVALID,
				"database name invalid (" << dbName << ")");
	}
}

/*!
	@brief Checks connected user name
*/
void StatementHandler::checkConnectedUserName(
		ConnectionOption &connOption, const char8_t *userName) {
	if ((connOption.requestType_ != Message::REQUEST_NEWSQL) &&
			(strcmp(userName, connOption.userName_.c_str()) != 0)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
				"user name invalid (" << userName << ")");
	}
}

/*!
	@brief Checks partition id
*/
void StatementHandler::checkPartitionIdZero(PartitionId pId) {
	if (pId != 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CM_INTERNAL_ERROR, "partition id is not 0");
	}
}

/*!
	@brief Checks digest
*/
void StatementHandler::checkDigest(const char8_t *digest, size_t maxStrLength) {
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
void StatementHandler::checkPrivilegeSize(DatabaseInfo &dbInfo, size_t size) {
	if (dbInfo.privilegeInfoList_.size() != size) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"privilege number invalid (" << dbInfo.privilegeInfoList_.size()
										 << ")");
	}
}

/*!
	@brief Checks modifiable value
*/
void StatementHandler::checkModifiable(bool modifiable, bool value) {
	if (modifiable != value) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
			"modifiable invalid (" << modifiable << ")");
	}
}


/*!
	@brief Decodes UserInfo
*/
void StatementHandler::decodeUserInfo(
		util::ByteStream<util::ArrayInStream> &in, UserInfo &userInfo) {

	try {
		decodeStringData<util::String>(in, userInfo.userName_);
		in >> userInfo.property_;
		decodeBooleanData(in, userInfo.withDigest_);
		if (userInfo.withDigest_) {
			decodeStringData<util::String>(in, userInfo.digest_);
		}
		else {
			userInfo.digest_ = "";
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes DatabaseInfo
*/
void StatementHandler::decodeDatabaseInfo(
		util::ByteStream<util::ArrayInStream> &in, DatabaseInfo &dbInfo,
		util::StackAllocator &alloc) {
	try {

		decodeStringData<util::String>(in, dbInfo.dbName_);
		in >> dbInfo.property_;
		int32_t num;
		in >> num;
		for (int32_t i = 0; i < num; i++) {
			PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

			decodeStringData<util::String>(in, prInfo->userName_);
			decodeStringData<util::String>(in, prInfo->privilege_);

			dbInfo.privilegeInfoList_.push_back(prInfo);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Makes schema data about USERS Container
*/
void StatementHandler::makeUsersSchema(util::XArray<uint8_t> &containerSchema) {
	TEST_PRINT("makeUsersSchema() S\n");


	{
		uint32_t columnNum = 3;
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNum), sizeof(uint32_t));
	}
	{
		char *columnName = const_cast<char *>("userName");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_STRING));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		flag |= ColumnInfo::COLUMN_FLAG_NOT_NULL;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		char *columnName = const_cast<char *>("digest");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_STRING));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		char *columnName = const_cast<char *>("property");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_BYTE));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		int16_t rowKeyNum = 1;
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&rowKeyNum), sizeof(int16_t));
		int16_t keyColumnId = static_cast<int16_t>(ColumnInfo::ROW_KEY_COLUMN_ID);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&keyColumnId), sizeof(int16_t));
	}

	{
		char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
		memcpy(
			affinityStr, DEFAULT_AFFINITY_STRING, AFFINITY_STRING_MAX_LENGTH);
		affinityStr[AFFINITY_STRING_MAX_LENGTH] =
			'\0';  
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
	TEST_PRINT("makeUsersSchema() E\n");
}

/*!
	@brief Makes schema data about DATABASES Container
*/
void StatementHandler::makeDatabasesSchema(
		util::XArray<uint8_t> &containerSchema) {
	TEST_PRINT("makeDatabasesSchema() S\n");


	{
		uint32_t columnNum = 5;
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNum), sizeof(uint32_t));
	}
	{
		char *columnName = const_cast<char *>("dbUserName");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_STRING));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		flag |= ColumnInfo::COLUMN_FLAG_NOT_NULL;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		char *columnName = const_cast<char *>("dbName");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_STRING));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		char *columnName = const_cast<char *>("property");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_BYTE));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		char *columnName = const_cast<char *>("userName");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_STRING));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		char *columnName = const_cast<char *>("privilege");
		int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(columnName), columnNameLen);

		int8_t tmp = static_cast<int8_t>(
			ValueProcessor::getSimpleColumnType(COLUMN_TYPE_STRING));
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

		uint8_t flag = 0;
		containerSchema.push_back(&flag, sizeof(uint8_t));
	}
	{
		int16_t rowKeyNum = 1;
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&rowKeyNum), sizeof(int16_t));
		int16_t keyColumnId = static_cast<int16_t>(ColumnInfo::ROW_KEY_COLUMN_ID);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&keyColumnId), sizeof(int16_t));
	}
	{
		char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
		memcpy(
			affinityStr, DEFAULT_AFFINITY_STRING, AFFINITY_STRING_MAX_LENGTH);
		affinityStr[AFFINITY_STRING_MAX_LENGTH] =
			'\0';  
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
	TEST_PRINT("makeDatabasesSchema() E\n");
}

/*!
	@brief Makes Row data in USERS Container
*/
void StatementHandler::makeUsersRow(
		util::StackAllocator &alloc, const ColumnInfo *columnInfoList,
		uint32_t columnNum, UserInfo &userInfo, RowData &rowData) {
	TEST_PRINT("makeUsersRow(userInfo) S\n");

	util::XArray<uint8_t> list1(alloc);
	util::XArray<uint8_t> list2(alloc);
	OutputMessageRowStore messageRowStore(dataStore_->getValueLimitConfig(),
		columnInfoList, columnNum, list1, list2, false);

	messageRowStore.beginRow();

	{
		char *elemData = const_cast<char *>(userInfo.userName_.c_str());
		int32_t elemSize = static_cast<int32_t>(strlen(elemData));
		messageRowStore.setFieldForRawData(0, elemData, elemSize);
	}
	{
		char *elemData = const_cast<char *>(userInfo.digest_.c_str());
		int32_t elemSize = static_cast<int32_t>(strlen(elemData));
		messageRowStore.setFieldForRawData(1, elemData, elemSize);
	}
	{
		char elemData = 0;
		messageRowStore.setField(
			2, &elemData, FixedSizeOfColumnType[COLUMN_TYPE_BYTE]);
	}
	messageRowStore.next();

	list1.push_back(list2.data(), list2.size());
	rowData.swap(list1);

	TEST_PRINT("makeUsersRow() E\n");
}

/*!
	@brief Makes Row data in DATABASES Container
*/
void StatementHandler::makeDatabasesRow(
		util::StackAllocator &alloc, const ColumnInfo *columnInfoList,
		uint32_t columnNum, DatabaseInfo &dbInfo, bool isCreate,
		RowData &rowData) {
	TEST_PRINT("makeDatabasesRow(dbInfo) S\n");

	util::XArray<uint8_t> list1(alloc);
	util::XArray<uint8_t> list2(alloc);
	OutputMessageRowStore messageRowStore(dataStore_->getValueLimitConfig(),
		columnInfoList, columnNum, list1, list2, false);

	messageRowStore.beginRow();

	{
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

		char *elemData = const_cast<char *>(dbUserName.c_str());
		int32_t elemSize = static_cast<int32_t>(strlen(elemData));
		messageRowStore.setFieldForRawData(0, elemData, elemSize);
	}
	{
		char *elemData = const_cast<char *>(dbInfo.dbName_.c_str());
		int32_t elemSize = static_cast<int32_t>(strlen(elemData));
		messageRowStore.setFieldForRawData(1, elemData, elemSize);
	}
	{
		char elemData = 0;
		if (isCreate) {
			elemData = 1;
		}
		messageRowStore.setField(
			2, &elemData, FixedSizeOfColumnType[COLUMN_TYPE_BYTE]);
	}
	{
		char *elemData =
			const_cast<char *>(dbInfo.privilegeInfoList_[0]->userName_.c_str());
		int32_t elemSize = static_cast<int32_t>(strlen(elemData));
		messageRowStore.setFieldForRawData(3, elemData, elemSize);
	}
	{
		char *elemData = const_cast<char *>("ALL");
		int32_t elemSize = static_cast<int32_t>(strlen(elemData));
		messageRowStore.setFieldForRawData(4, elemData, elemSize);
	}

	messageRowStore.next();

	list1.push_back(list2.data(), list2.size());
	rowData.swap(list1);

	TEST_PRINT("makeDatabasesRow() E\n");
}

/*!
	@brief Makes DatabaseInfo list
*/
void StatementHandler::makeDatabaseInfoList(
		TransactionContext &txn, util::StackAllocator &alloc,
		BaseContainer &container, ResultSet &rs,
		util::XArray<DatabaseInfo*> &dbInfoList) {
	TEST_PRINT("makeDatabaseInfoList() S\n");

	OutputMessageRowStore outputMessageRowStore(
		dataStore_->getValueLimitConfig(), container.getColumnInfoList(),
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

	InputMessageRowStore inputMessageRowStore(dataStore_->getValueLimitConfig(),
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

	for (size_t i = 0; i < rs.getResultNum(); i++) {
		inputMessageRowStore
			.next();  

		inputMessageRowStore.getField(1, field, fieldSize);  
		dbName.append(
			reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
		dbName.append("\0");

		it = dbMap.find(dbName);
		if (it == dbMap.end()) {
			dbInfo = ALLOC_NEW(alloc) DatabaseInfo(alloc);
			dbInfo->dbName_.append(dbName);
			dbInfo->property_ = 0;
			dbInfoList.push_back(dbInfo);

			dbMap.insert(std::make_pair(dbName, dbInfo));
		}
		else {
			dbInfo = it->second;
		}
		dbName.clear();

		inputMessageRowStore.getField(2, field, fieldSize);  
		property = *reinterpret_cast<int8_t *>(const_cast<uint8_t *>(field));

		if (property == 0) {  

			PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

			inputMessageRowStore.getField(3, field, fieldSize);  
			prInfo->userName_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			prInfo->userName_.append("\0");

			inputMessageRowStore.getField(4, field, fieldSize);  
			prInfo->privilege_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			prInfo->privilege_.append("\0");

			dbInfo->privilegeInfoList_.push_back(prInfo);
		}

	}

	TEST_PRINT("makeDatabaseInfoList() E\n");
}

/*!
	@brief Makes Row key data
*/
void StatementHandler::makeRowKey(const char *name, RowKeyData &rowKey) {
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

/*!
	@brief Makes Row key data in DATABASES Container
*/
void StatementHandler::makeDatabaseRowKey(
		DatabaseInfo &dbInfo, RowKeyData &rowKey) {
	const char *userName = dbInfo.privilegeInfoList_[0]->userName_.c_str();
	{
		size_t dbNameSize = strlen(dbInfo.dbName_.c_str());
		size_t userNameSize = strlen(userName);
		uint32_t varSize = static_cast<uint32_t>(dbNameSize + 1 + userNameSize);
//		uint32_t encodedVarSize = ValueProcessor::encodeVarSize(varSize);
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

/*!
	@brief Initializes SYSTEM Container
*/
void StatementHandler::initializeMetaContainer(
		EventContext &ec, Event &ev, const Request &request,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	TEST_PRINT("initializeMetaContainer() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	try {
		{
			TEST_PRINT("initializeMetaContainer() gs_users\n");

			Response response(alloc);

			util::XArray<uint8_t> containerInfo(alloc);
			makeUsersSchema(containerInfo);

			TransactionContext &txn = transactionManager_->put(
					alloc, 0 /*pId*/,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
			const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

			DataStore::PutStatus putStatus;
			const FullContainerKey containerKey(
				alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM), GS_SYSTEM_DB_ID,
				GS_USERS, strlen(GS_USERS));
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), containerKey, COLLECTION_CONTAINER,
				static_cast<uint32_t>(containerInfo.size()),
				containerInfo.data(), true /*modifiable*/, 
				MessageSchema::DEFAULT_VERSION, putStatus);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			const void *createdContainerNameBinary;
			size_t createdContainerNameBinarySize;
			containerKey.toBinary(
				createdContainerNameBinary, createdContainerNameBinarySize);

			response.binaryData2_.assign(
				static_cast<const uint8_t*>(createdContainerNameBinary),
				static_cast<const uint8_t*>(createdContainerNameBinary) + createdContainerNameBinarySize);

			const bool optionIncluded = true;
			bool internalOptionIncluded = true;
			container->getContainerInfo(
				txn, response.binaryData_, optionIncluded, internalOptionIncluded);

			if (putStatus != DataStore::NOT_EXECUTED) {
				const util::String emptyExtensionName(alloc);
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutContainerLog(
						*log,
						txn.getPartitionId(), txn.getClientId(), UNDEF_TXNID,
						response.containerId_, request.fixed_.cxtSrc_.stmtId_,
						static_cast<uint32_t>(response.binaryData2_.size()),
						response.binaryData2_.data(),
						response.binaryData_,
						container->getContainerType(),
						static_cast<uint32_t>(emptyExtensionName.size()),
						emptyExtensionName.c_str(),
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_, false, true, MAX_ROWID,
						(container->getExpireType() == TABLE_EXPIRE));
				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}
		{
			TEST_PRINT("initializeMetaContainer() gs_databases\n");

			Response response(alloc);

			util::XArray<uint8_t> containerInfo(alloc);
			makeDatabasesSchema(containerInfo);

			TransactionContext &txn = transactionManager_->put(
					alloc, 0 /*pId*/,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

			const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

			DataStore::PutStatus putStatus;
			const FullContainerKey containerKey(
				alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM), GS_SYSTEM_DB_ID,
				GS_DATABASES, strlen(GS_DATABASES));
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), containerKey, COLLECTION_CONTAINER,
				static_cast<uint32_t>(containerInfo.size()),
				containerInfo.data(), true /*modifiable*/, 
				MessageSchema::DEFAULT_VERSION, putStatus);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			const void *createdContainerNameBinary;
			size_t createdContainerNameBinarySize;
			containerKey.toBinary(
				createdContainerNameBinary, createdContainerNameBinarySize);

			response.binaryData2_.assign(
				static_cast<const uint8_t*>(createdContainerNameBinary),
				static_cast<const uint8_t*>(createdContainerNameBinary) + createdContainerNameBinarySize);

			const bool optionIncluded = true;
			bool internalOptionIncluded = true;
			container->getContainerInfo(
				txn, response.binaryData_, optionIncluded, internalOptionIncluded);

			if (putStatus != DataStore::NOT_EXECUTED) {
				const util::String emptyExtensionName(alloc);
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutContainerLog(
						*log,
						txn.getPartitionId(), txn.getClientId(), UNDEF_TXNID,
						response.containerId_, request.fixed_.cxtSrc_.stmtId_,
						static_cast<uint32_t>(response.binaryData2_.size()),
						response.binaryData2_.data(),
						response.binaryData_,
						container->getContainerType(),
						static_cast<uint32_t>(emptyExtensionName.size()),
						emptyExtensionName.c_str(),
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_, false, true, MAX_ROWID,
						(container->getExpireType() == TABLE_EXPIRE));

				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}

		TEST_PRINT("initializeMetaContainer() E\n");
	}
	catch (std::exception &) {
		TEST_PRINT("initializeMetaContainer() exception\n");
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		Response response(alloc);
		executeReplication(
				request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
				ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		throw;
	}
}

/*!
	@brief Internal method for Authentication
*/
void StatementHandler::executeAuthenticationInternal(
		EventContext &ec, util::StackAllocator &alloc,
		TransactionManager::ContextSource &cxtSrc,
		const char8_t *userName, const char8_t *digest, const char8_t *dbName,
		UserType userType, int checkLevel, DatabaseId &dbId) {
	TEST_PRINT("executeAuthenticationInternal() S\n");

	if (strcmp(dbName, GS_PUBLIC) == 0) {
		dbId = GS_PUBLIC_DB_ID;
	}
	else if (strcmp(dbName, GS_SYSTEM) == 0) {
		dbId = GS_SYSTEM_DB_ID;
	}
	else {
		dbId = UNDEF_DBID;
	}

	if ((checkLevel & 0x01) && (userType == Message::USER_NORMAL)) {
		TEST_PRINT("executeAuthenticationInterval() (Ph1) S\n");

		const util::StackAllocator::Scope scope(alloc);

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

		TransactionContext &txn = transactionManager_->put(
				alloc, 0 /*pId*/, TXN_EMPTY_CLIENTID, cxtSrc, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
			GS_USERS, strlen(GS_USERS));
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			TEST_PRINT("gs_users not found.\n");
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"user name invalid (" << userName << ")");
		}
		else {
			TEST_PRINT("gs_users found.\n");
		}

		util::String query(alloc);
		query.append("select count(*) where userName='");
		query.append(userName);
		query.append("' and digest='");
		query.append(digest);
		query.append("'");

		ResultSet *rs = dataStore_->createResultSet(
			txn, container->getContainerId(), container->getVersionId(), emNow,
			NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
		QueryProcessor::executeTQL(
			txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);  
		rs->setResultType(RESULT_ROWSET);

		TEST_PRINT1("getResultNum() %d\n", rs->getResultNum());
		int8_t *tmp = reinterpret_cast<int8_t *>(
			const_cast<uint8_t *>(rs->getFixedStartData()));
		tmp++;
		int64_t *count = reinterpret_cast<int64_t *>(tmp);
		if (*count != 1) {
			TEST_PRINT("count!=1(userName)\n");
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"invalid user name or password (user name = " << userName
															  << ")");
		}

		TEST_PRINT("executeAuthenticationInterval() (Ph1) E\n");
	}
	if (checkLevel & 0x02) {
		TEST_PRINT("executeAuthenticationInterval() (Ph2) S\n");

		const util::StackAllocator::Scope scope(alloc);

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

		TransactionContext &txn = transactionManager_->put(
			alloc, 0 /*pId*/, TXN_EMPTY_CLIENTID, cxtSrc, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
			GS_DATABASES, strlen(GS_DATABASES));
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			TEST_PRINT("gs_databases not found.\n");
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"database name invalid (" << dbName << ")");
		}
		else {
			TEST_PRINT("gs_databases found.\n");
		}

		util::String dbUserName(alloc);
		dbUserName.append(dbName);
		dbUserName.append(":");
		if (userType == Message::USER_NORMAL) {
			dbUserName.append(userName);
		}
		else {
			dbUserName.append(
				"admin");  
		}

		RowKeyData rowKey(alloc);
		makeRowKey(dbUserName.c_str(), rowKey);

		ResultSet *rs = dataStore_->createResultSet(
			txn, container->getContainerId(), container->getVersionId(), emNow,
			NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
		QueryProcessor::get(txn, *container,
			static_cast<uint32_t>(rowKey.size()), rowKey.data(), *rs);
		rs->setResultType(RESULT_ROWSET);

		TEST_PRINT1("getResultNum() %d\n", rs->getResultNum());

		if (rs->getResultNum() != 1) {
			TEST_PRINT("count!=1(dbName)\n");
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"database name invalid (" << dbName << ")");
		}

		if (userType != Message::USER_NORMAL) {
			util::XArray<RowId> rowIdList(alloc);
			container->getRowIdList(txn, *(rs->getOIdList()), rowIdList);
			dbId = rowIdList[0] + DBID_RESERVED_RANGE;
		}

		TEST_PRINT("executeAuthenticationInterval() (Ph2) E\n");
	}
	if ((checkLevel & 0x02) && (userType == Message::USER_NORMAL)) {
		TEST_PRINT("executeAuthenticationInterval() (Ph3) S\n");

		const util::StackAllocator::Scope scope(alloc);

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

		TransactionContext &txn = transactionManager_->put(
			alloc, 0 /*pId*/, TXN_EMPTY_CLIENTID, cxtSrc, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
			GS_DATABASES, strlen(GS_DATABASES));
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			TEST_PRINT("gs_databases not found.\n");
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
		else {
			TEST_PRINT("gs_databases found.\n");
		}

		util::String dbUserName(alloc);
		dbUserName.append(dbName);
		dbUserName.append(":admin");

		RowKeyData rowKey(alloc);
		makeRowKey(dbUserName.c_str(), rowKey);

		ResultSet *rs = dataStore_->createResultSet(
			txn, container->getContainerId(), container->getVersionId(), emNow,
			NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
		QueryProcessor::get(txn, *container,
			static_cast<uint32_t>(rowKey.size()), rowKey.data(), *rs);
		rs->setResultType(RESULT_ROWSET);

		TEST_PRINT1("getResultNum() %d\n", rs->getResultNum());

		if (rs->getResultNum() != 1) {
			TEST_PRINT("count!=1(dbName)\n");
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"database name invalid (" << dbName << ")");
		}

		util::XArray<RowId> rowIdList(alloc);
		container->getRowIdList(txn, *(rs->getOIdList()), rowIdList);
		dbId = rowIdList[0] + DBID_RESERVED_RANGE;

		TEST_PRINT("executeAuthenticationInterval() (Ph3) E\n");
	}
	TEST_PRINT("executeAuthenticationInternal() E\n");
}


/*!
	@brief Checks if a specified password(not digest) is proper length
*/
void StatementHandler::checkPasswordLength(const char8_t *password) {
	if (strlen(password) > PASSWORD_SIZE_MAX) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"password length exceeds maximum size.");
	}
}

/*!
	@brief Checks if a specified Container exists
*/
bool StatementHandler::checkContainer(
		EventContext &ec, Request &request, const char8_t *containerName) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		containerName, strlen(containerName));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		TEST_PRINT1("%s not found.\n", containerName);
		return true;
	}
	else {
		return false;
	}
}

/*!
	@brief Counts the number of Rows in a specified Container
*/
int64_t StatementHandler::count(
		EventContext &ec, const Request &request, const char8_t *containerName) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		containerName, strlen(containerName));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	util::String query(alloc);
	if (strcmp(containerName, GS_DATABASES) == 0) {  
		query.append("select count(*) where property=1");
	}
	else {
		query.append("select count(*)");
	}
	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);

	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);

	TEST_PRINT1("getResultNum() %d\n", rs->getResultNum());
	int8_t *tmp = reinterpret_cast<int8_t *>(
		const_cast<uint8_t *>(rs->getFixedStartData()));
	tmp++;
	int64_t *count = reinterpret_cast<int64_t *>(tmp);

	return (*count);
}

/*!
	@brief Checks if a specifed user exists
*/
void StatementHandler::checkUser(
		EventContext &ec, const Request &request, const char8_t *userName,
		bool &existFlag) {
	existFlag = false;

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_USERS, strlen(GS_USERS));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
	}

	RowKeyData rowKey(alloc);
	makeRowKey(userName, rowKey);

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::get(txn, *container, static_cast<uint32_t>(rowKey.size()),
		rowKey.data(), *rs);
	rs->setResultType(RESULT_ROWSET);
	existFlag = (rs->getResultNum() > 0);
}

/*!
	@brief Checks with TQL if a specifed user exists
*/
void StatementHandler::checkUserWithTQL(
		EventContext &ec, const Request &request, const char8_t *userName,
		const char8_t *digest, bool detailFlag, bool &existFlag) {
	existFlag = false;

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_USERS, strlen(GS_USERS));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
	}

	util::String query(alloc);
	query.append("select * where UPPER(userName)=UPPER('");
	query.append(userName);
	query.append("')");

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);

	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);
	existFlag = (rs->getResultNum() > 0);

	if (detailFlag && existFlag) {
		OutputMessageRowStore outputMessageRowStore(
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
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
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
			container->getColumnNum(),
			reinterpret_cast<void *>(const_cast<uint8_t *>(data1)), size1,
			reinterpret_cast<void *>(const_cast<uint8_t *>(data2)), size2, 1,
			false);

		util::String targetDigest(alloc);
		const uint8_t *field;
		uint32_t fieldSize;

		inputMessageRowStore
			.next();  

		inputMessageRowStore.getField(1, field, fieldSize);  
		targetDigest.append(
			reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
		targetDigest.append("\0");

		if (strcmp(digest, targetDigest.c_str()) != 0) {
			TEST_PRINT("digest unmatch\n");
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NAME_ALREADY_EXISTS, "");
		}
	}
}

/*!
	@brief Puts Row in USERS Container
*/
void StatementHandler::putUserRow(
		EventContext &ec, Event &ev, const Request &request,
		UserInfo &userInfo,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	TEST_PRINT("executeUserPutRow() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const uint64_t numRow = 1;

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	try {
		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
			GS_USERS, strlen(GS_USERS));
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
		}

		RowData rowData(alloc);
		makeUsersRow(alloc, container->getColumnInfoList(),
			container->getColumnNum(), userInfo, rowData);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		DataStore::PutStatus putStatus;
		container->putRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowIds[0], putStatus,
			PUT_INSERT_OR_UPDATE);  
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);

		{
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert((executed && rowIds[0] != UNDEF_ROWID) ||
				   (!executed && rowIds[0] == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putPutRowLog(
					*log,
					txn.getPartitionId(), TXN_EMPTY_CLIENTID, txn.getId(),
					container->getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
					rowData, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		TEST_PRINT("executeUserPutRow() E\n");
	}
	catch (std::exception &) {
		TEST_PRINT("executeUserPutRow() exception\n");
		Response response(alloc);
		executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
			ev.getType(), txn.getLastStatementId(),
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size(), response);

		throw;
	}
}

/*!
	@brief Handler Operator
*/
void PutUserHandler::operator()(EventContext &ec, Event &ev) {
	TEST_PRINT("<<<PutUserHandler>>> START\n");

	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		UserInfo userInfo(alloc);
		bool modifiable;

		decodeUserInfo(in, userInfo);
		decodeBooleanData(in, modifiable);
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

		checkPartitionIdZero(request.fixed_.pId_);
		if (connOption.userType_ == Message::USER_NORMAL) {
			checkModifiable(modifiable, true);
			checkConnectedUserName(connOption, userInfo.userName_.c_str());
		}
		checkUserName(userInfo.userName_.c_str(), true);
		checkDigest(userInfo.digest_.c_str(),
			dataStore_->getValueLimitConfig().getLimitSmallSize());


		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		if (checkContainer(ec, request, GS_USERS) ||
			checkContainer(ec, request, GS_DATABASES)) {
			initializeMetaContainer(ec, ev, request, logRecordList);
		}
		bool existFlag;
		if (modifiable == false) {  
			checkUserWithTQL(ec, request, userInfo.userName_.c_str(),
				userInfo.digest_.c_str(), true, existFlag);  
			if (existFlag == false) {
				if (count(ec, request, GS_USERS) >=
					static_cast<int64_t>(USER_NUM_MAX)) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TXN_USER_LIMIT_OVER, "[PutUser]");
				}
				putUserRow(ec, ev, request, userInfo, logRecordList);
			}
		}
		else {  
			checkUser(ec, request, userInfo.userName_.c_str(), existFlag);
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

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TEST_PRINT("<<<PutUserHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}

}

/*!
	@brief Removes Row in DATABASES Container with TQL
*/
void DropUserHandler::executeTQLAndRemoveDatabaseRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *userName,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList) {
	TEST_PRINT("executeDatabaseTQLAndRemoveRow() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	try {
		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
			GS_DATABASES, strlen(GS_DATABASES));
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
		}

		util::String query(alloc);
		query.append("select * where userName='");
		query.append(userName);
		query.append("'");

		ResultSet *rs = dataStore_->createResultSet(
			txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *rs);

		QueryProcessor::executeTQL(
			txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
		rs->setResultType(RESULT_ROWSET);


		util::XArray<RowId> rowIdList(alloc);
		container->getRowIdList(txn, *(rs->getOIdList()), rowIdList);

		util::XArray<RowId> rowIds(alloc);
		for (size_t i = 0; i < rowIdList.size(); i++) {
			bool existFlag;
			container->redoDeleteRow(txn, rowIdList[i], existFlag);
			const bool executed = existFlag;

			rowIds.push_back(rowIdList[i]);

			{
				const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
						TransactionManager::NO_AUTO_COMMIT_BEGIN);
				const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
						TransactionManager::AUTO_COMMIT);
				assert(!(withBegin && isAutoCommit));
				assert(1 == rowIds.size());
				assert(rowIds[0] != UNDEF_ROWID);
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putRemoveRowLog(
						*log, txn.getPartitionId(), txn.getClientId(), txn.getId(),
						container->getContainerId(), request.fixed_.cxtSrc_.stmtId_,
						(executed ? rowIds.size() : 0), rowIds,
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

			rowIds.clear();
		}

		TEST_PRINT("executeDatabaseTQLAndRemoveRow() E\n");
	}
	catch (std::exception &) {
		TEST_PRINT("executeDatabaseTQLAndRemoveRow() exception\n");
		Response response(alloc);
		executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
			ev.getType(), txn.getLastStatementId(),
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size(), response);

		throw;
	}
}

/*!
	@brief Removes Row in USERS Container
*/
void DropUserHandler::removeUserRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *userName,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	TEST_PRINT("executeUserRemoveRow() S\n");

	const uint64_t numRow = 1;

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, 0 /*pId*/,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	try {
		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
			GS_USERS, strlen(GS_USERS));
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
		}

		RowKeyData rowKey(alloc);
		makeRowKey(userName, rowKey);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		bool existFlag;
		container->deleteRow(txn, static_cast<uint32_t>(rowKey.size()),
			rowKey.data(), rowIds[0], existFlag);
		const bool executed = existFlag;

		{
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert((executed && rowIds[0] != UNDEF_ROWID) ||
				   (!executed && rowIds[0] == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putRemoveRowLog(
					*log,
					txn.getPartitionId(), TXN_EMPTY_CLIENTID, txn.getId(),
					container->getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					(executed ? rowIds.size() : 0), rowIds,
					txn.getTransationTimeoutInterval(), request.fixed_.cxtSrc_.getMode_,
					withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		TEST_PRINT("executeUserRemoveRow() E\n");
	}
	catch (std::exception &) {
		TEST_PRINT("executeUserRemoveRow() exception\n");
		Response response(alloc);
		executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
			ev.getType(), txn.getLastStatementId(),
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size(), response);

		throw;
	}
}

/*!
	@brief Handler Operator
*/
void DropUserHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<DropUserHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		util::String userName(alloc);
		decodeStringData<util::String>(in, userName);

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
		checkPartitionIdZero(request.fixed_.pId_);
		checkUserName(userName.c_str(), false);


		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		bool existFlag;
		checkUser(ec, request, userName.c_str(), existFlag);

		if (existFlag == true) {
			executeTQLAndRemoveDatabaseRow(
				ec, ev, request, userName.c_str(), logRecordList);

			removeUserRow(ec, ev, request, userName.c_str(), logRecordList);
		}


		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TEST_PRINT("<<<DropUserHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Searches USERS Container
*/
void StatementHandler::executeTQLUser(
		EventContext &ec, const Request &request, const char8_t *userName,
		UserType userType,
		util::XArray<UserInfo*> &userInfoList) {
	TEST_PRINT("executeTQL() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_USERS, strlen(GS_USERS));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		return;
	}

	util::String query(alloc);
	if (userType != Message::USER_NORMAL) {  
		query.append("select *");
	}
	else {
		query.append("select * where userName='");
		query.append(userName);
		query.append("'");
	}


	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);


	if (rs->getResultNum() > 0) {
		OutputMessageRowStore outputMessageRowStore(
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
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
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
			container->getColumnNum(),
			reinterpret_cast<void *>(const_cast<uint8_t *>(data1)), size1,
			reinterpret_cast<void *>(const_cast<uint8_t *>(data2)), size2,
			resultNum, false);

		const uint8_t *field;
		uint32_t fieldSize;
		util::String userName(alloc);
		for (ResultSize i = 0; i < rs->getResultNum(); i++) {
			inputMessageRowStore
				.next();  

			UserInfo *outUserInfo = ALLOC_NEW(alloc) UserInfo(alloc);

			inputMessageRowStore.getField(0, field, fieldSize);  
			outUserInfo->userName_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			outUserInfo->userName_.append("\0");

			inputMessageRowStore.getField(1, field, fieldSize);  
			outUserInfo->digest_.append(
				reinterpret_cast<const char *>(field + 1), (size_t)fieldSize);
			outUserInfo->digest_.append("\0");

			inputMessageRowStore.getField(2, field, fieldSize);  
			outUserInfo->property_ =
				(*reinterpret_cast<int8_t *>(const_cast<uint8_t *>(field)));

			outUserInfo->withDigest_ = true;

			userInfoList.push_back(outUserInfo);

		}
	}

	TEST_PRINT("executeTQL() E\n");
}

/*!
	@brief Handler Operator
*/
void GetUsersHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<GetUsersHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		bool withFilter;
		UserInfo userInfo(alloc);
		util::String userName(alloc);
		int8_t property = 0;  

		decodeBooleanData(in, withFilter);
		if (withFilter) {
			decodeStringData<util::String>(in, userName);
			in >> property;
		}

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

		checkPartitionIdZero(request.fixed_.pId_);
		if (withFilter) {
			checkConnectedUserName(connOption, userName.c_str());
		}
		if ((connOption.userType_ == Message::USER_NORMAL) &&
				(strcmp(userName.c_str(), "") == 0)) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID,
				"[GetUsers] user name must specified");
		}

		if (withFilter && (connOption.userType_ != Message::USER_NORMAL)) {
			UserInfo *outUserInfo = ALLOC_NEW(alloc) UserInfo(alloc);
			outUserInfo->userName_.append(userName.c_str());
			outUserInfo->property_ = 1;
			outUserInfo->withDigest_ = false;
			response.userInfoList_.push_back(outUserInfo);
		}
		else {
			executeTQLUser(ec, request, connOption.userName_.c_str(),
				connOption.userType_, response.userInfoList_);
		}

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

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, false);

		TEST_PRINT("<<<GetUsersHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Checks with TQL if the Row corresponds a specified Database exists in
   DATABASES Container
*/
void PutDatabaseHandler::checkDatabaseWithTQL(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo,
		bool &existFlag) {
	existFlag = false;

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	util::String query(alloc);
	query.append("select * where UPPER(dbName)=UPPER('");
	query.append(dbInfo.dbName_.c_str());
	query.append("') and property=1");

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);
	existFlag = (rs->getResultNum() > 0);
}

/*!
	@brief Put Row in DATABASES Container
*/
void StatementHandler::putDatabaseRow(
		EventContext &ec, Event &ev, const Request &request,
		DatabaseInfo &dbInfo, bool isCreate,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	TEST_PRINT("executeDatabasePutRow() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const uint64_t numRow = 1;

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	try {
	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == 0) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}

		RowData rowData(alloc);
		makeDatabasesRow(alloc, container->getColumnInfoList(),
			container->getColumnNum(), dbInfo, isCreate, rowData);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		DataStore::PutStatus putStatus;
		container->putRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowIds[0], putStatus,
			PUT_INSERT_OR_UPDATE);  
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);

		{
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert((executed && rowIds[0] != UNDEF_ROWID) ||
				   (!executed && rowIds[0] == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putPutRowLog(
					*log,
					txn.getPartitionId(), TXN_EMPTY_CLIENTID, txn.getId(),
					container->getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
					rowData, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		TEST_PRINT("executeDatabasePutRow() E\n");
	}
	catch (std::exception &) {
		TEST_PRINT("executeDatabasePutRow() exception\n");

		Response response(alloc);
		executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND, txn,
			ev.getType(), txn.getLastStatementId(),
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size(), response);

		throw;
	}
}

/*!
	@brief Handler Operator
*/
void PutDatabaseHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<PutDatabaseHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		DatabaseInfo dbInfo(alloc);
		bool modifiable;  

		decodeDatabaseInfo(in, dbInfo, alloc);
		decodeBooleanData(in, modifiable);

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
		checkPartitionIdZero(request.fixed_.pId_);
		checkModifiable(modifiable, false);
		checkDatabaseName(dbInfo.dbName_.c_str());
		checkPrivilegeSize(dbInfo, 0);


		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		{
			PrivilegeInfo *prInfo = ALLOC_NEW(alloc) PrivilegeInfo(alloc);

			prInfo->userName_.append(connOption.userName_.c_str());
			prInfo->privilege_.append("ALL");

			dbInfo.privilegeInfoList_.push_back(prInfo);
		}

		if (checkContainer(ec, request, GS_USERS) ||
			checkContainer(ec, request, GS_DATABASES)) {
			initializeMetaContainer(ec, ev, request, logRecordList);
		}
		bool existFlag;
		checkDatabaseWithTQL(ec, request, dbInfo, existFlag);
		if (existFlag == false) {
			if (count(ec, request, GS_DATABASES) >=
				static_cast<int64_t>(DATABASE_NUM_MAX)) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TXN_DATABASE_LIMIT_OVER, "[PutDatabase]");
			}
			putDatabaseRow(
				ec, ev, request, dbInfo, true, logRecordList);  
		}

		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		bool ackWait = false;
		ackWait = executeReplication(
				request, ec, alloc, ev.getSenderND(), txn,
				ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TEST_PRINT("<<<PutDatabaseHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Removes Row in DATABASES Container with TQL
*/
void DropDatabaseHandler::executeTQLAndRemoveDatabaseRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *dbName, bool isAdmin,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList) {
	TEST_PRINT("executeDatabaseTQLAndRemoveRow() S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_OR_DATABASE_NOT_EXIST, "");
	}

	util::String query(alloc);
	query.append("select * where dbName='");
	query.append(dbName);
	query.append("'");
	if (isAdmin) {
		query.append(" and property=1");
	}
	else {
		query.append(" and property=0");
	}

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);


	util::XArray<RowId> rowIdList(alloc);
	container->getRowIdList(txn, *(rs->getOIdList()), rowIdList);

	util::XArray<RowId> rowIds(alloc);
	for (size_t i = 0; i < rowIdList.size(); i++) {
		bool existFlag;
		container->redoDeleteRow(txn, rowIdList[i], existFlag);
		const bool executed = existFlag;

		rowIds.push_back(rowIdList[i]);

		{
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(1 == rowIds.size());
			assert(rowIds[0] != UNDEF_ROWID);
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putRemoveRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), container->getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, (executed ? rowIds.size() : 0),
					rowIds, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		rowIds.clear();
	}
	TEST_PRINT("executeDatabaseTQLAndRemoveRow() E\n");
}

/*!
	@brief Handler Operator
*/
void DropDatabaseHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<DropDatabaseHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		util::String dbName(alloc);
		decodeStringData<util::String>(in, dbName);

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
		checkPartitionIdZero(request.fixed_.pId_);
		checkConnectedDatabaseName(connOption, dbName.c_str());
		checkDatabaseName(dbName.c_str());


		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		executeTQLAndRemoveDatabaseRow(
			ec, ev, request, dbName.c_str(), false, logRecordList);
		executeTQLAndRemoveDatabaseRow(
			ec, ev, request, dbName.c_str(), true, logRecordList);


		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TEST_PRINT("<<<DropDatabaseHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Makes PUBLIC DatabaseInfo list
*/
void GetDatabasesHandler::makePublicDatabaseInfoList(
	util::StackAllocator &alloc, util::XArray<UserInfo *> &userInfoList,
	util::XArray<DatabaseInfo *> &dbInfoList) {
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
}

/*!
	@brief Searches DATABASES Container
*/
void GetDatabasesHandler::executeTQLDatabase(
		EventContext &ec, const Request &request, const char8_t *dbName,
		const char8_t *userName, util::XArray<DatabaseInfo*> &dbInfoList) {
	TEST_PRINT("executeTQL() S\n");

	bool dbNameSpecified = false;
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		return;
	}

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

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);

	TEST_PRINT1("rs resultNum=%d\n", rs->getResultNum());

	if (dbNameSpecified && rs->getResultNum() == 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TXN_CURRENT_DATABASE_REMOVED, "[GetDatabases]");
	}

	makeDatabaseInfoList(txn, alloc, *container, *rs, dbInfoList);
}

/*!
	@brief Handler Operator
*/
void GetDatabasesHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<GetDatabasesHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		bool withFilter;
		util::String dbName(alloc);
		int8_t property = 0;  

		decodeBooleanData(in, withFilter);
		if (withFilter) {
			decodeStringData<util::String>(in, dbName);
			if (dbName.empty()) {
				dbName = GS_PUBLIC;
			}
			in >> property;
		}

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

		checkPartitionIdZero(request.fixed_.pId_);
		if (withFilter) {
			checkConnectedDatabaseName(connOption, dbName.c_str());
		}

		if (strcmp(dbName.c_str(), GS_PUBLIC) == 0) {
			executeTQLUser(ec, request, connOption.userName_.c_str(),
				connOption.userType_, response.userInfoList_);
			makePublicDatabaseInfoList(
				alloc, response.userInfoList_, response.databaseInfoList_);
		}
		else {
			if (connOption.userType_ != Message::USER_NORMAL) {
				executeTQLDatabase(ec, request, dbName.c_str(), "",
					response.databaseInfoList_);
			}
			else {
				executeTQLDatabase(ec, request, dbName.c_str(),
					connOption.userName_.c_str(), response.databaseInfoList_);
			}
		}

		TEST_PRINT("[ResponseMesg]\n");
		TEST_PRINT1(
			"\tdbInfoList.size() %d\n", response.databaseInfoList_.size());
		for (size_t i = 0; i < response.databaseInfoList_.size(); i++) {
			TEST_PRINT1("%s\n", response.databaseInfoList_[i]->dump().c_str());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, false);

		TEST_PRINT("<<<GetDatabasesHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Checks with TQL if a specified database exists
*/
void StatementHandler::checkDatabaseWithTQL(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	util::String query(alloc);
	query.append("select * where dbName='");
	query.append(dbInfo.dbName_.c_str());
	query.append("'");

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);

	bool existFlag = (rs->getResultNum() > 0);
	if (existFlag == false) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_NOT_EXIST,
			"(database name = " << dbInfo.dbName_.c_str() << ")");  
	}
}

/*!
	@brief Checks with TQL if a specified database and normal user exists
*/
void StatementHandler::checkDetailDatabaseWithTQL(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo,
		bool &existFlag) {
	existFlag = false;

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	util::String query(alloc);
	query.append("select * where dbName='");
	query.append(dbInfo.dbName_.c_str());
	query.append("' and property=0");

	ResultSet *rs = dataStore_->createResultSet(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);

	ResultSize resultNum = rs->getResultNum();
	if (resultNum == 1) {
		util::XArray<DatabaseInfo *> dbInfoList(alloc);
		makeDatabaseInfoList(txn, alloc, *container, *rs, dbInfoList);
		const char *userName = dbInfo.privilegeInfoList_[0]->userName_.c_str();
		const char *targetUserName =
			dbInfoList[0]->privilegeInfoList_[0]->userName_.c_str();
		if (strcmp(userName, targetUserName) != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_OTHER_PRIVILEGE_EXISTS,
				"(input user name = " << userName << ", input database name = "
									  << dbInfo.dbName_.c_str() << ")");  
		}
		else {
			existFlag = true;
		}
	}
	else if (resultNum > 1) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
}

/*!
	@brief Handler Operator
*/
void PutPrivilegeHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<PutPrivilegeHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		DatabaseInfo dbInfo(alloc);

		decodeDatabaseInfo(in, dbInfo, alloc);

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
		checkPartitionIdZero(request.fixed_.pId_);
		checkPrivilegeSize(dbInfo, 1);

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

		checkDatabaseWithTQL(ec, request, dbInfo);

		checkDetailDatabaseWithTQL(ec, request, dbInfo, existFlag);
		if (existFlag == false) {
			putDatabaseRow(ec, ev, request, dbInfo, false,
				logRecordList);  
		}
		const util::DateTime now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		ackWait = executeReplication(
				request, ec, alloc, ev.getSenderND(), txn,
				ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TEST_PRINT("<<<PutPrivilegeHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Removes Row in DATABASES Container
*/
bool DropPrivilegeHandler::removeDatabaseRow(
		EventContext &ec, Event &ev, const Request &request,
		DatabaseInfo &dbInfo) {
	TEST_PRINT("executeDatabaseRemoveRow() xxx S\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	TransactionContext &txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
	const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

	const FullContainerKey containerKey(
		alloc, getKeyConstraint(CONTAINER_ATTR_SINGLE_SYSTEM, false), GS_SYSTEM_DB_ID,
		GS_DATABASES, strlen(GS_DATABASES));
	ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
		containerKey, COLLECTION_CONTAINER);
	BaseContainer *container = containerAutoPtr.getBaseContainer();
	if (container == 0) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	RowKeyData rowKey(alloc);
	makeDatabaseRowKey(dbInfo, rowKey);

	util::XArray<RowId> rowIds(alloc);
	const uint64_t numRow = 1;
	rowIds.assign(numRow, UNDEF_ROWID);
	bool existFlag;
	container->deleteRow(txn, static_cast<uint32_t>(rowKey.size()),
		rowKey.data(), rowIds[0], existFlag);
	const bool executed = existFlag;

	util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
	{
		const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
				TransactionManager::NO_AUTO_COMMIT_BEGIN);
		const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
				TransactionManager::AUTO_COMMIT);
		assert(!(withBegin && isAutoCommit));
		assert(numRow == rowIds.size());
		assert((executed && rowIds[0] != UNDEF_ROWID) ||
			   (!executed && rowIds[0] == UNDEF_ROWID));
		util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		const LogSequentialNumber lsn = logManager_->putRemoveRowLog(
				*log, txn.getPartitionId(),
				TXN_EMPTY_CLIENTID, txn.getId(), container->getContainerId(),
				request.fixed_.cxtSrc_.stmtId_, (executed ? rowIds.size() : 0), rowIds,
				txn.getTransationTimeoutInterval(), request.fixed_.cxtSrc_.getMode_,
				withBegin, isAutoCommit);
		partitionTable_->setLSN(txn.getPartitionId(), lsn);
		logRecordList.push_back(log);
	}

	transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

	Response response(alloc);
	response.existFlag_ = existFlag;
	const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), response);

	TEST_PRINT("executeDatabaseRemoveRow() E\n");
	return ackWait;
}

/*!
	@brief Handler Operator
*/
void DropPrivilegeHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<DropPrivilegeHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		DatabaseInfo dbInfo(alloc);

		decodeDatabaseInfo(in, dbInfo, alloc);

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
		checkPartitionIdZero(request.fixed_.pId_);
		checkPrivilegeSize(dbInfo, 1);
		checkDatabaseName(dbInfo.dbName_.c_str());

		bool ackWait = false;

		bool existFlag;
		checkUser(ec, request, dbInfo.privilegeInfoList_[0]->userName_.c_str(),
			existFlag);
		if (existFlag == false) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_USER_NOT_EXIST,
				"(user name = "
					<< dbInfo.privilegeInfoList_[0]->userName_.c_str() << ")");
		}

		checkDatabaseWithTQL(ec, request, dbInfo);

		checkDetailDatabaseWithTQL(ec, request, dbInfo, existFlag);
		if (existFlag) {
			ackWait = removeDatabaseRow(ec, ev, request, dbInfo);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TEST_PRINT("<<<DropPrivilegeHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}


/*!
	@brief Decodes AuthenticationAck
*/
void StatementHandler::decodeAuthenticationAck(
		util::ByteStream<util::ArrayInStream> &in, AuthenticationAck &ack) {


	try {
		in >> ack.clusterVer_;
		in >> ack.authId_;
		in >> ack.authPId_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes AuthenticationAck
*/
void StatementHandler::encodeAuthenticationAckPart(
		EventByteOutStream &out, ClusterVersionId clusterVer,
		AuthenticationId authId, PartitionId authPId) {


	try {
		out << clusterVer;
		out << authId;
		out << authPId;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Replies success message in authentication
*/
void StatementHandler::replySuccess(
		EventContext &ec, util::StackAllocator &alloc,
		StatementExecStatus status, const Request &request,
		const AuthenticationContext &authContext)  
{
#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)     \
	"(nd=" << authContext.getConnectionND()            \
		   << ", pId=" << authContext.getPartitionId() \
		   << ", stmtId=" << authContext.getStatementId() << ")"

	TEST_PRINT("replySuccess() START\n");

	util::StackAllocator::Scope scope(alloc);

	if (authContext.getConnectionND().isEmpty()) {
		return;
	}

	TEST_PRINT1("pId=%d\n", authContext.getPartitionId());
	TEST_PRINT1("stmtId=%d\n", authContext.getStatementId());

	try {
		Response response(alloc);
		response.connectionOption_ = &(authContext.getConnectionND().getUserData<ConnectionOption>());

		Event ev(ec, LOGIN, authContext.getPartitionId());
		setSuccessReply(alloc, ev, authContext.getStatementId(), status,
			response);  
		ec.getEngine().send(ev, authContext.getConnectionND());

		TEST_PRINT("replySuccess() END\n");
		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_REPLY_CLIENT,
			TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}
	catch (EncodeDecodeException &e) {
		GS_RETHROW_USER_ERROR(e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}

#undef TXN_TRACE_REPLY_SUCCESS_ERROR
}

/*!
	@brief Sends AuthenticationACK to a request node
*/
void StatementHandler::replyAuthenticationAck(
		EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, const AuthenticationAck &request,
		DatabaseId dbId) {
#define TXN_TRACE_REPLY_ACK_ERROR(request, ND)        \
	"(owner=" << ND << ", authId=" << request.authId_ \
			  << ", pId=" << request.pId_ << ")"

	TEST_PRINT("replyAuthenticationAck() START\n");

	util::StackAllocator::Scope scope(alloc);

	util::String dbName(alloc);  

	try {
		TEST_PRINT1("request.pId=%d\n", request.pId_);
		TEST_PRINT1("request.authPId=%d\n", request.authPId_);
		Event authAckEvent(ec, AUTHENTICATION_ACK, request.authPId_);
		EventByteOutStream out = authAckEvent.getOutStream();
		encodeAuthenticationAckPart(out, request.clusterVer_, request.authId_,
			request.authPId_);  
		int32_t num = 0;
		if (dbId == UNDEF_DBID) {
			out << num;
		}
		else {
			num = 1;
			out << num;
			out << dbName;  
			out << dbId;
		}
		TEST_PRINT1("num=%d\n", num);
		TEST_PRINT1("dbId=%d\n", dbId);

		ec.getEngine().send(authAckEvent, ND);

		TEST_PRINT("replyAuthenticationAck() END\n");
		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_SEND_ACK,
			TXN_TRACE_REPLY_ACK_ERROR(request, ND));
	}
	catch (EncodeDecodeException &e) {
		TXN_RETHROW_ENCODE_ERROR(e, TXN_TRACE_REPLY_ACK_ERROR(request, ND));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, TXN_TRACE_REPLY_ACK_ERROR(request, ND));
	}

#undef TXN_TRACE_REPLY_ACK_ERROR
}

/*!
	@brief Request to an authentication node
*/
void StatementHandler::executeAuthentication(
		EventContext &ec, Event &ev,
		const NodeDescriptor &clientND, StatementId authStmtId,
		const char8_t *userName, const char8_t *digest,
		const char8_t *dbName, UserType userType) {
	TEST_PRINT("executeAuthentication() S\n");
	TEST_PRINT1("ev.pId=%d\n", ev.getPartitionId());

	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	AuthenticationId authId = 0;

	AuthenticationContext &authContext =
		transactionManager_->putAuth(ev.getPartitionId(),
			authStmtId, clientND, emNow
			);
	authId = authContext.getAuthenticationId();
	TEST_PRINT1("authId=%d\n", authId);


	Event authEvent(ec, AUTHENTICATION, 0 /*pId*/);
	EventByteOutStream out = authEvent.getOutStream();
	encodeAuthenticationAckPart(
		out, GS_CLUSTER_MESSAGE_CURRENT_VERSION, authId, ev.getPartitionId());
	TEST_PRINT1("userName=%s\n", userName.c_str());
	TEST_PRINT1("digest=%s\n", digest.c_str());
	TEST_PRINT1("dbName=%s\n", dbName.c_str());
	TEST_PRINT1("userType=%d\n", userType);
	encodeStringData(out, userName);
	encodeStringData(out, digest);
	encodeStringData(out, dbName);
	char8_t byteData = userType;
	out << byteData;


	NodeAddress nodeAddress = clusterManager_->getPartition0NodeAddr();
	TEST_PRINT1("NodeAddress(TS) %s\n", nodeAddress.dump().c_str());
	TEST_PRINT1("address =%s\n", nodeAddress.toString(false).c_str());
	const NodeDescriptor &nd =
		transactionService_->getEE()->getServerND(util::SocketAddress(
			nodeAddress.toString(false).c_str(), nodeAddress.port_));

	transactionService_->getEE()->send(authEvent, nd);

	TEST_PRINT("executeAuthentication() E\n");
}

/*!
	@brief Handler Operator
*/
void AuthenticationHandler::operator()(EventContext &ec, Event &ev) {
	TEST_PRINT("<<<AuthenticationHandler>>> START\n");

	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	AuthenticationAck request(ev.getPartitionId());

	TransactionManager::ContextSource cxtSrc;

	try {
		EventByteInStream in(ev.getInStream());

		util::String userName(alloc);
		util::String digest(alloc);
		util::String dbName(alloc);
		char8_t byteData;
		UserType userType;

		decodeAuthenticationAck(in, request);
		TEST_PRINT1("(authId=%d\n)", request.authId_);
		TEST_PRINT1("(authPId=%d\n)", request.authPId_);
		decodeStringData<util::String>(in, userName);
		decodeStringData<util::String>(in, digest);
		decodeStringData<util::String>(in, dbName);
		in >> byteData;
		userType = static_cast<UserType>(byteData);


		TEST_PRINT1("userName=%s\n", userName.c_str());
		TEST_PRINT1("digest=%s\n", digest.c_str());
		TEST_PRINT1("dbName=%s\n", dbName.c_str());
		TEST_PRINT1("userType=%d\n", userType);

		clusterService_->checkVersion(request.clusterVer_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkPartitionIdZero(request.pId_);

		DatabaseId dbId = UNDEF_DBID;

		{
			TEST_PRINT("LOGIN\n");
			try {
				if (strcmp(dbName.c_str(), GS_PUBLIC) != 0) {
					StatementHandler::executeAuthenticationInternal(ec, alloc,
						cxtSrc, userName.c_str(), digest.c_str(),
						dbName.c_str(), userType, 3, dbId);  
				}
				else {
					executeAuthenticationInternal(ec, alloc, cxtSrc,
						userName.c_str(), digest.c_str(), dbName.c_str(),
						userType, 1, dbId);  
				}
			}
			catch (std::exception &) {
				dbId = UNDEF_DBID;
			}

			replyAuthenticationAck(ec, alloc, ev.getSenderND(), request, dbId);
		}
		TEST_PRINT("<<<AuthenticationHandler>>> END\n");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to accept authentication "
			"(nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Handles error in authentication
*/
void AuthenticationAckHandler::authHandleError(
		EventContext &ec, AuthenticationAck &ack, std::exception &e) {
	try {
		TEST_PRINT("authHandleError() S\n");

		AuthenticationContext &authContext = transactionManager_->getAuth(
			ack.pId_, ack.authId_);

		Event ev(ec, LOGIN, ack.pId_);

		try {
			throw;
		}
		catch (DenyException &e) {
			setErrorReply(ev, authContext.getStatementId(), TXN_STATEMENT_DENY,
				e,
				authContext.getConnectionND());  
		}
		catch (std::exception &e) {
			setErrorReply(ev, authContext.getStatementId(), TXN_STATEMENT_ERROR,
				e,
				authContext.getConnectionND());  
		}

		if (!authContext.getConnectionND().isEmpty()) {
			TEST_PRINT("send\n");
			ec.getEngine().send(ev, authContext.getConnectionND());
		}
		transactionManager_->removeAuth(ack.pId_, ack.authId_);

		TEST_PRINT("authHandleError() E\n");
	}
	catch (ContextNotFoundException &) {
		TEST_PRINT("authHandleError() ContextNotFoundException\n");
	}
	catch (std::exception &) {
	}
}

/*!
	@brief Handler Operator
*/
void AuthenticationAckHandler::operator()(EventContext &ec, Event &ev) {
	TEST_PRINT("<<<AuthenticationAckHandler>>> START\n");

	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	AuthenticationAck ack(ev.getPartitionId());
	Request request(alloc, getRequestSource(ev));

	try {
		int32_t num;
		DatabaseId dbId = UNDEF_DBID;

		EventByteInStream in(ev.getInStream());

		util::String dbName(alloc);

		decodeAuthenticationAck(in, ack);
		TEST_PRINT1("(authId=%d\n)", ack.authId_);
		TEST_PRINT1("(authPId=%d\n)", ack.authPId_);
		in >> num;
		if (num > 0) {
			decodeStringData<util::String>(in, dbName);
			in >> dbId;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
		}
		TEST_PRINT1("dbId=%d\n", dbId);

		clusterService_->checkVersion(ack.clusterVer_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		AuthenticationContext &authContext = transactionManager_->getAuth(
				ack.pId_, ack.authId_);

		ConnectionOption &connOption =
				authContext.getConnectionND().getUserData<ConnectionOption>();
		connOption.isAuthenticated_ = true;
		connOption.dbId_ = dbId;
		connOption.authenticationTime_ = emNow;

		replySuccess(
				ec, alloc, TXN_STATEMENT_SUCCESS, request,
				authContext);  
		transactionManager_->removeAuth(ack.pId_, ack.authId_);

		TEST_PRINT("<<<AuthenticationAckHandler>>> END\n");
	}
	catch (ContextNotFoundException &e) {
		UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
			"Authentication timed out (nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		authHandleError(ec, ack, e);
	}
}


/*!
	@brief Checks authentication timeout
*/
void CheckTimeoutHandler::checkAuthenticationTimeout(EventContext &ec) {

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ReplicationId> timeoutResourceIds(alloc);

		transactionManager_->getAuthenticationTimeoutContextId(
			pgId, emNow, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			try {
				AuthenticationContext &authContext =
					transactionManager_->getAuth(
						pIds[i], timeoutResourceIds[i]);

				try {
					TXN_THROW_DENY_ERROR(
						GS_ERROR_TXN_AUTHENTICATION_TIMEOUT, "");
				}
				catch (std::exception &e) {
					Event ev(ec, LOGIN, authContext.getPartitionId());
					setErrorReply(ev, authContext.getStatementId(),
						TXN_STATEMENT_DENY, e,
						authContext.getConnectionND());  

					if (!authContext.getConnectionND().isEmpty()) {
						TEST_PRINT("send\n");
						ec.getEngine().send(ev, authContext.getConnectionND());
					}
				}

				transactionManager_->removeAuth(
					pIds[i], timeoutResourceIds[i]);
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
							<< ", contextId=" << timeoutResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			TEST_PRINT1("TIMEOUT count=%d\n", timeoutResourceCount);
			GS_TRACE_WARNING(AUTHENTICATION_TIMEOUT,
				GS_TRACE_TXN_AUTHENTICATION_TIMEOUT,
				"(pgId=" << pgId << ", timeoutResourceCount="
						 << timeoutResourceCount << ")");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"(pId=" << pgId << ", timeoutResourceCount=" << timeoutResourceCount
					<< ")");
	}
}
