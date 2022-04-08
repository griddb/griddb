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
#include "sql_type.h"
#include "sql_common.h"
#include "data_store_common.h"

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
	const size_t infoCount = sizeof(STATE_INFO_LIST) / sizeof(*STATE_INFO_LIST);
	StateInfo info = SQL_STATE_DECLARE_INFO(STATE_UNKNOWN);

	if (0 <= sqliteErrorCode && sqliteErrorCode < static_cast<int32_t>(infoCount)) {
		info = STATE_INFO_LIST[sqliteErrorCode];
	}

	return stateInfoToCode(info);
}

util::Exception::NamedErrorCode SQLState::sqlStateToCode(
	uint32_t sqlState) throw() {
	StateInfo info = SQL_STATE_DECLARE_INFO(STATE_UNKNOWN);

	for (size_t i = 0;
		i < sizeof(STATE_INFO_LIST) / sizeof(*STATE_INFO_LIST); i++) {
		const StateInfo& entry = STATE_INFO_LIST[i];
		if (sqlState == entry.code_) {
			info = entry;
			break;
		}
	}

	return stateInfoToCode(info);
}

util::Exception::NamedErrorCode SQLState::stateInfoToCode(
	const StateInfo& info) throw() {
	const uint32_t mask = ~((UINT32_C(1) << EXCEPTION_CODE_STATE_BITS) - 1);
	const uint32_t base =
		static_cast<uint32_t>(GS_ERROR_SQL_PROC_BASE + info.code_);
	const uint32_t state = static_cast<uint32_t>(info.code_);

	assert((base & mask) == 0);
	assert((state & ~(mask >> EXCEPTION_CODE_STATE_BITS)) == 0);

	return util::Exception::NamedErrorCode(static_cast<int32_t>(
		(state << EXCEPTION_CODE_STATE_BITS) | (base & ~mask)),
		info.name_);
}

void SQLErrorUtils::decodeException(
	util::StackAllocator& alloc, util::ArrayByteInStream& in,
	util::Exception& dest) throw() {
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
		dest = GS_EXCEPTION_CONVERT(e, GS_EXCEPTION_MERGE_MESSAGE(
			e, "Failed to parse error information from NoSQL server"));
	}
}

bool NoSQLCommonUtils::isAccessibleContainer(uint8_t attribute, bool& isWritable) {
	switch (static_cast<ContainerAttribute>(attribute)) {
	case CONTAINER_ATTR_SINGLE:
	case CONTAINER_ATTR_LARGE:
	case CONTAINER_ATTR_VIEW:
		isWritable = true;
		return true;
	default:
		return false;
	}
}

bool NoSQLCommonUtils::isWritableContainer(uint8_t attribute, uint8_t type) {
	switch (static_cast<ContainerAttribute>(attribute)) {
	case CONTAINER_ATTR_SINGLE:
	case CONTAINER_ATTR_LARGE:
	case CONTAINER_ATTR_VIEW:
		return true;
	default:
		return false;
	}
}

void NoSQLCommonUtils::splitContainerName(
	const std::pair<const char8_t*, const char8_t*>& src,
	std::pair<const char8_t*, const char8_t*>* base,
	std::pair<const char8_t*, const char8_t*>* sub,
	std::pair<const char8_t*, const char8_t*>* affinity,
	std::pair<const char8_t*, const char8_t*>* type) {

	const char8_t* subSep = NULL;
	const char8_t* affinitySep = NULL;
	const char8_t* typeSep = NULL;

	const char8_t* baseEnd = NULL;
	const char8_t* subEnd = NULL;
	const char8_t* affinityEnd = NULL;

	for (const char8_t* it = src.first; it != src.second; ++it) {
		if (*it == '/' && typeSep == NULL && affinitySep == NULL &&
			subSep == NULL) {
			subSep = it;
			if (baseEnd == NULL) {
				baseEnd = it;
			}
		}
		else if (*it == '@' && typeSep == NULL && affinitySep == NULL) {
			affinitySep = it;
			if (baseEnd == NULL) {
				baseEnd = it;
			}
			if (subEnd == NULL) {
				subEnd = it;
			}
		}
		else if (*it == '#' && typeSep == NULL) {
			typeSep = it;
			if (baseEnd == NULL) {
				baseEnd = it;
			}
			if (subEnd == NULL) {
				subEnd = it;
			}
			if (affinityEnd == NULL) {
				affinityEnd = it;
			}
		}
	}

	if (base != NULL) {
		if (baseEnd == NULL) {
			baseEnd = src.second;
		}
		*base = std::make_pair(src.first, baseEnd);
	}

	if (sub != NULL) {
		if (subSep == NULL) {
			*sub = std::pair<const char8_t*, const char8_t*>();
		}
		else {
			if (subEnd == NULL) {
				subEnd = src.second;
			}
			*sub = std::make_pair(subSep + 1, subEnd);
		}
	}

	if (affinity != NULL) {
		if (affinitySep == NULL) {
			*affinity = std::pair<const char8_t*, const char8_t*>();
		}
		else {
			if (affinityEnd == NULL) {
				affinityEnd = src.second;
			}
			*affinity = std::make_pair(affinitySep + 1, affinityEnd);
		}
	}

	if (type != NULL) {
		if (typeSep == NULL) {
			*type = std::pair<const char8_t*, const char8_t*>();
		}
		else {
			*type = std::make_pair(typeSep + 1, src.second);
		}
	}
}

void NoSQLCommonUtils::makeSubContainerName(
	const char8_t* name, PartitionId partitionId, int32_t subContainerId,
	int32_t tablePartitioningCount, util::String& subName) {

	std::pair<const char8_t*, const char8_t*> baseName;
	splitContainerName(
		std::make_pair(name, name + strlen(name)),
		&baseName, NULL, NULL, NULL);

	if (baseName.first == baseName.second) {
		assert(false);
		return;
	}

	util::NormalOStringStream oss;
	oss.write(
		baseName.first,
		static_cast<std::streamsize>(baseName.second - baseName.first));
	oss << "/";
	oss << subContainerId;

	oss << "@";
	oss << partitionId;
	static_cast<void>(tablePartitioningCount);

	subName = oss.str().c_str();
}

int32_t NoSQLCommonUtils::mapTypeToSQLIndexFlags(
	int8_t mapType, uint8_t nosqlColumnType) {
	int32_t flags = 0;
	switch (mapType) {
	case MAP_TYPE_BTREE:
		flags |= 1 << SQLType::INDEX_TREE_EQ;
		if (nosqlColumnType != COLUMN_TYPE_STRING) {
			flags |= 1 << SQLType::INDEX_TREE_RANGE;
		}
		break;
	case MAP_TYPE_DEFAULT:
		switch (nosqlColumnType) {
		case COLUMN_TYPE_BOOL:
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG:
		case COLUMN_TYPE_FLOAT:
		case COLUMN_TYPE_DOUBLE:
		case COLUMN_TYPE_TIMESTAMP:
		case COLUMN_TYPE_STRING:
		case COLUMN_TYPE_GEOMETRY:
		case COLUMN_TYPE_BLOB:
			flags |= mapTypeToSQLIndexFlags(MAP_TYPE_BTREE, nosqlColumnType);
			break;
		default:
			break;
		}
		break;
	default:
		break;
	}
	return flags;
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

void TaskProfiler::copy(util::StackAllocator& alloc, const TaskProfiler& target) {
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
			target.customData_->begin(), target.customData_->end(), alloc);
	}
}
