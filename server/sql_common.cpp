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
#include "sql_common.h"
#include "sql_utils.h"

#include "transaction_service.h"

const uint32_t SQLState::EXCEPTION_CODE_STATE_BITS = 24;

#define SQL_STATE_DECLARE_INFO(state) { state, "SQL_" #state }

const SQLState::StateInfo SQLState::STATE_INFO_LIST[] = {
	SQL_STATE_DECLARE_INFO(STATE_SUCCESSFUL_COMPLETION), 
	SQL_STATE_DECLARE_INFO(STATE_DATA_EXCEPTION), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_ACCESS_RULE_VIOLATION), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_INVALID_SYSTEM_STATE), 
	SQL_STATE_DECLARE_INFO(STATE_OUT_OF_MEMORY), 
	SQL_STATE_DECLARE_INFO(STATE_DATA_EXCEPTION), 
	SQL_STATE_DECLARE_INFO(STATE_TRANSACTION_ROLLBACK), 
	SQL_STATE_DECLARE_INFO(STATE_IO_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_INSUFFICIENT_RESOURCES), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN), 
	SQL_STATE_DECLARE_INFO(STATE_NO_DATA), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN), 
	SQL_STATE_DECLARE_INFO(STATE_DATA_EXCEPTION), 
	SQL_STATE_DECLARE_INFO(STATE_INTEGRITY_CONSTRAINT_VIOLATION), 
	SQL_STATE_DECLARE_INFO(STATE_DATA_EXCEPTION), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN), 
	SQL_STATE_DECLARE_INFO(STATE_INVALID_AUTHORIZATION_SPECIFICATION), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_DATA_EXCEPTION), 
	SQL_STATE_DECLARE_INFO(STATE_SYSTEM_ERROR), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN), 
	SQL_STATE_DECLARE_INFO(STATE_UNKNOWN) 
};

util::Exception::NamedErrorCode SQLState::sqlErrorToCode(
		int32_t sqliteErrorCode) throw() {

	const size_t infoCount
			= sizeof(STATE_INFO_LIST) / sizeof(*STATE_INFO_LIST);
	StateInfo info = SQL_STATE_DECLARE_INFO(STATE_UNKNOWN);

	if (0 <= sqliteErrorCode
			&& sqliteErrorCode < static_cast<int32_t>(infoCount)) {
		info = STATE_INFO_LIST[sqliteErrorCode];
	}

	return stateInfoToCode(info);
}

util::Exception::NamedErrorCode
		SQLState::sqlStateToCode(uint32_t sqlState) throw() {

	StateInfo info = SQL_STATE_DECLARE_INFO(STATE_UNKNOWN);

	for (size_t i = 0;
			i < sizeof(STATE_INFO_LIST) / sizeof(*STATE_INFO_LIST); i++) {
		const StateInfo &entry = STATE_INFO_LIST[i];
		if (sqlState == entry.code_) {
			info = entry;
			break;
		}
	}

	return stateInfoToCode(info);
}

util::Exception::NamedErrorCode
		SQLState::stateInfoToCode(const StateInfo &info) throw() {

	const uint32_t mask
			= ~((UINT32_C(1) << EXCEPTION_CODE_STATE_BITS) - 1);
	const uint32_t base
			= static_cast<uint32_t>(GS_ERROR_SQL_PROC_BASE + info.code_);
	const uint32_t state = static_cast<uint32_t>(info.code_);

	assert((base & mask) == 0);
	assert((state & ~(mask >> EXCEPTION_CODE_STATE_BITS)) == 0);

	return util::Exception::NamedErrorCode(
			static_cast<int32_t>(
					(state << EXCEPTION_CODE_STATE_BITS) | (base & ~mask)),
					info.name_);
}

void SQLErrorUtils::decodeException(
		util::StackAllocator &alloc,
		util::ArrayByteInStream &in,
		util::Exception &dest) throw() {

	using util::Exception;
	try {
		dest = Exception();

		util::Exception top;
		util::Exception following;

		int32_t count;
		in >> count;

		for (int32_t i = 0; i < count; i++) {
			util::StackAllocator::Scope scope(alloc);

			int32_t errorCode;
			util::String optionMessage(alloc);
			util::String type(alloc);
			util::String fileName(alloc);
			util::String functionName(alloc);
			int32_t lineNumber;

			in >> errorCode;
			in >> optionMessage;
			in >> type;
			in >> fileName;
			in >> functionName;
			in >> lineNumber;

			(i == 0 ? top : following).append(Exception(
					Exception::NamedErrorCode(errorCode),
					optionMessage.c_str(),
					fileName.c_str(),
					functionName.c_str(),
					lineNumber,
					NULL,
					type.c_str(),
					Exception::STACK_TRACE_NONE,
					Exception::LITERAL_ALL_DUPLICATED));
		}

		if (in.base().remaining() > 0) {
			util::StackAllocator::Scope scope(alloc);

			util::String errorCodeName(alloc);
			in >> errorCodeName;

			dest.append(Exception(
					Exception::NamedErrorCode(
							top.getErrorCode(), errorCodeName.c_str()),
					UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
							top.getField(Exception::FIELD_MESSAGE)),
					UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
							top.getField(Exception::FIELD_FILE_NAME)),
					UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
							top.getField(Exception::FIELD_FUNCTION_NAME)),
					top.getLineNumber(),
					NULL,
					UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
							top.getField(Exception::FIELD_TYPE_NAME)),
					Exception::STACK_TRACE_NONE,
					Exception::LITERAL_ALL_DUPLICATED));
		}
		else {
			dest.append(top);
		}

		if (count > 1) {
			dest.append(following);
		}

		if (in.base().remaining() > 0) {
			int32_t paramCount;
			in >> paramCount;

			for (int32_t i = 0; i < paramCount; i++) {
				util::StackAllocator::Scope scope(alloc);

				util::String name(alloc);
				util::String value(alloc);

				in >> name;
				in >> value;
			}
		}
	}
	catch (...) {
		std::exception e;
		dest = GS_EXCEPTION_CONVERT(
				e, GS_EXCEPTION_MERGE_MESSAGE(
						e, "Failed to parse error information from NoSQL server"));
	}
}

const char8_t SQLPragma::VALUE_TRUE[] = "1";
const char8_t SQLPragma::VALUE_FALSE[] = "0";

TaskProfiler::TaskProfiler() :
		leadTime_(0),
		actualTime_(0),
		executionCount_(0),
		worker_(0),
		rows_(NULL),
		address_(NULL),
		customData_(NULL) {
}

void TaskProfiler::copy(
		util::StackAllocator &alloc,
		const TaskProfiler &target) {

	leadTime_ = target.leadTime_;
	actualTime_ = target.actualTime_;
	executionCount_ = target.executionCount_;
	worker_ = target.worker_;

	if (target.rows_ == NULL) {
		rows_ = NULL;
	}
	else {
		rows_ = ALLOC_NEW(alloc) util::Vector<int64_t>(
				target.rows_->begin(), target.rows_->end(), alloc);
	}

	if (target.address_ == NULL) {
		address_ = NULL;
	}
	else {
		address_ = ALLOC_NEW(alloc) util::String(*target.address_);
	}

	if (target.customData_ == NULL) {
		customData_ = NULL;
	}
	else {
		customData_ = ALLOC_NEW(alloc) util::XArray<uint8_t>(
				target.customData_->begin(),
				target.customData_->end(), alloc);
	}
}


ClientInfo::ClientInfo(
		util::StackAllocator &alloc,
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		const NodeDescriptor &clientNd,
		ClientId &clientId) :
				userName_(globalVarAlloc),
				dbName_(globalVarAlloc),
				dbId_(0),
				normalizedUserName_(globalVarAlloc), 
				normalizedDbName_(globalVarAlloc), 
				applicationName_(globalVarAlloc),
				isAdministrator_(false),
				clientId_(clientId),
				clientNd_(clientNd), 
				storeMemoryAgingSwapRate_(
						TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE),
				timezone_(util::TimeZone()) {

		init(alloc);
}

void ClientInfo::init(util::StackAllocator &alloc) {

	util::StackAllocator::Scope scope(alloc);

	StatementHandler::ConnectionOption &connOption
			= clientNd_.getUserData<
					StatementHandler::ConnectionOption>();

	dbId_ = connOption.dbId_;
	isAdministrator_
			= (connOption.userType_ != StatementMessage::USER_NORMAL);
	
	connOption.getLoginInfo(
				userName_, dbName_, applicationName_);

	connOption.setHandlingClientId(clientId_);

	normalizedDbName_ = SQLUtils::normalizeName(
			alloc, dbName_.c_str()).c_str();

	storeMemoryAgingSwapRate_
			= connOption.storeMemoryAgingSwapRate_;
	timezone_ = connOption.timeZone_;
}

int64_t SQLUtils::convertToTime(util::String &timeStr) {

	const size_t size = timeStr.size();
	if (size == 10) {
		if (timeStr.find('-') != util::String::npos) {
			timeStr.append("T00:00:00Z");
		}
	}
	else if (size == 12 || size == 8) {
		if (timeStr.find(':') != util::String::npos) {
			timeStr.insert(0, "1970-01-01T");
			timeStr.append("Z");
		}
	}
	const bool trimMilliseconds = false;
	int64_t targetValue;
	try {
		targetValue = util::DateTime(
				timeStr.c_str(), trimMilliseconds).getUnixTime();
	}
	catch (std::exception &e) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR, 
				GS_EXCEPTION_MERGE_MESSAGE(e, "Invalid date format"));
	}
	return targetValue;
}

bool SQLUtils::checkNoSQLTypeToTupleType(ColumnType type) {

	switch (type & ~TupleList::TYPE_MASK_NULLABLE) {
		case COLUMN_TYPE_BOOL :
		case COLUMN_TYPE_BYTE :
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT :
		case COLUMN_TYPE_LONG :
		case COLUMN_TYPE_FLOAT :
		case COLUMN_TYPE_DOUBLE :
		case COLUMN_TYPE_TIMESTAMP :
		case COLUMN_TYPE_NULL :
		case COLUMN_TYPE_STRING :
		case COLUMN_TYPE_BLOB :
			return true;
		case COLUMN_TYPE_GEOMETRY:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			return false;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
					"Unsupported type, type="
					<< static_cast<int32_t>(type));
	}
}

void SQLUtils::convertUpper(
		char8_t const *p, size_t size, char8_t *out) {

	char c;
	for (size_t i = 0; i < size; i++) {
		c = *(p + i);
		if ((c >= 'a') && (c <= 'z')) {
			*(out + i) = static_cast<char>(c - 32);
		}
		else {
			*(out + i) = c;
		}
	}
}

util::String SQLUtils::normalizeName(
		util::StackAllocator &alloc,
		const char8_t *src, bool isCaseSensitive) {

	if (isCaseSensitive) {
		return util::String(src, alloc);
	}

	util::XArray<char8_t> dest(alloc);
	dest.resize(strlen(src) + 1);

	SQLUtils::convertUpper(
			src, static_cast<uint32_t>(dest.size()), &dest[0]);

	return util::String(&dest[0], alloc);
}

ColumnType SQLUtils::convertTupleTypeToNoSQLType(
		TupleList::TupleColumnType type) {

	switch (type & ~TupleList::TYPE_MASK_NULLABLE) {
		case TupleList::TYPE_BOOL: return COLUMN_TYPE_BOOL;
		case TupleList::TYPE_BYTE: return COLUMN_TYPE_BYTE;
		case TupleList::TYPE_SHORT: return COLUMN_TYPE_SHORT;
		case TupleList::TYPE_INTEGER: return COLUMN_TYPE_INT;
		case TupleList::TYPE_LONG: return COLUMN_TYPE_LONG;
		case TupleList::TYPE_FLOAT: return COLUMN_TYPE_FLOAT;
		case TupleList::TYPE_NUMERIC: return COLUMN_TYPE_DOUBLE;
		case TupleList::TYPE_DOUBLE: return COLUMN_TYPE_DOUBLE;
		case TupleList::TYPE_TIMESTAMP: return COLUMN_TYPE_TIMESTAMP;
		case TupleList::TYPE_NULL: return COLUMN_TYPE_NULL;
		case TupleList::TYPE_STRING: return COLUMN_TYPE_STRING;
		case TupleList::TYPE_GEOMETRY: return COLUMN_TYPE_GEOMETRY;
		case TupleList::TYPE_BLOB: return COLUMN_TYPE_BLOB;
		case TupleList::TYPE_ANY: return COLUMN_TYPE_ANY;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
					"Unsupported type, type=" << static_cast<int32_t>(type));
	}
}
