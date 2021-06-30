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
	@brief Implementation of TransactionService
*/
#include "transaction_service.h"

#include "gs_error.h"

#include "log_manager.h"
#include "transaction_context.h"
#include "transaction_manager.h"
#include "cluster_manager.h"

#include "util/container.h"
#include "sql_compiler.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "sql_utils.h"
#include "nosql_utils.h"


#include "resource_set.h"

template<typename T>
void decodeLargeRow(
		const char *key, util::StackAllocator &alloc, TransactionContext &txn,
		DataStore *dataStore, const char8_t *dbName, BaseContainer *container,
		T &record, const EventMonotonicTime emNow);

void decodeLargeBinaryRow(
		const char *key, util::StackAllocator &alloc, TransactionContext &txn,
		DataStore *dataStore, const char *dbName, BaseContainer *container,
		util::XArray<uint8_t> &record, const EventMonotonicTime emNow);


#include "sql_service.h"

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

#include "base_container.h"
#include "data_store.h"
#include "message_row_store.h"  
#include "message_schema.h"
#include "query_processor.h"
#include "result_set.h"
#include "meta_store.h"

#include "cluster_service.h"
#include "partition_table.h"
#include "recovery_manager.h"
#include "system_service.h"
#include "trigger_service.h"

extern std::string dumpUUID(uint8_t *uuid);

#ifndef _WIN32
#include <signal.h>  
#endif

#define TXN_THROW_DENY_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(DenyException, errorCode, message)

#define TXN_TRACE_HANDLER_CALLED(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler called. (type=" << getEventTypeName(ev.getType()) \
							<< "[" << ev.getType() << "]" \
							<< ", nd=" << ev.getSenderND() \
							<< ", pId=" << ev.getPartitionId() << ")")

#define TXN_TRACE_HANDLER_CALLED_END(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler finished. (type=" << getEventTypeName(ev.getType()) \
							<< "[" << ev.getType() << "]" \
							<< ", nd=" << ev.getSenderND() \
							<< ", pId=" << ev.getPartitionId() << ")")

UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(TRANSACTION_DETAIL);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);

void StatementMessage::Utils::encodeBool(EventByteOutStream &out, bool value) {
	out << static_cast<int8_t>(value);
}

bool StatementMessage::Utils::decodeBool(EventByteInStream &in) {
	int8_t value;
	in >> value;
	return !!value;
}

void StatementMessage::Utils::encodeVarSizeBinary(
		EventByteOutStream &out, const util::XArray<uint8_t> &binary) {
	encodeVarSizeBinary(out, binary.data(), binary.size());
}

void StatementMessage::Utils::encodeVarSizeBinary(
		EventByteOutStream &out, const void *data, size_t size) {
	const size_t encodedVarSize = ValueProcessor::getEncodedVarSize(size);
	switch (encodedVarSize) {
	case 1:
		out << ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(size));
		break;
	case 4:
		out << ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(size));
		break;
	default:
		assert(encodedVarSize == 8);
		out << ValueProcessor::encode8ByteVarSize(size);
		break;
	}
	out.writeAll(data, size);
}

void StatementMessage::Utils::decodeVarSizeBinary(
		EventByteInStream &in, util::XArray<uint8_t> &binary) {
	uint32_t size = ValueProcessor::getVarSize(in);
	binary.resize(size);
	in.readAll(binary.data(), size);
}

template<StatementMessage::FixedType T>
void StatementMessage::FixedCoder<T>::encode(
		EventByteOutStream &out, const FixedRequest &value) const {
	const bool forClientId =
			(T == FixedTypes::SESSION || T == FixedTypes::ROW ||
			T == FixedTypes::PARTIAL_DDL);

	out << value.cxtSrc_.stmtId_;
	if (T != FixedTypes::BASIC && T != FixedTypes::MULTI) {
		out << value.cxtSrc_.containerId_;
	}
	if (forClientId) {
		out << value.clientId_.sessionId_;
	}
	if (forClientId || T == FixedTypes::MULTI) {
		out.writeAll(value.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	}
	if (T == FixedTypes::SCHEMA || T == FixedTypes::ROW) {
		out << value.schemaVersionId_;
	}
	if (T == FixedTypes::ROW) {
		out << value.cxtSrc_.getMode_;
		out << value.cxtSrc_.txnMode_;
	}
}

template<StatementMessage::FixedType T>
void StatementMessage::FixedCoder<T>::decode(
		EventByteInStream &in, FixedRequest &value) const {
	const bool forClientId =
			(T == FixedTypes::SESSION || T == FixedTypes::ROW ||
			T == FixedTypes::PARTIAL_DDL);

	in >> value.cxtSrc_.stmtId_;
	if (T != FixedTypes::BASIC && T != FixedTypes::MULTI) {
		in >> value.cxtSrc_.containerId_;
	}
	if (forClientId) {
		in >> value.clientId_.sessionId_;
	}
	if (forClientId || T == FixedTypes::MULTI) {
		in.readAll(value.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	}
	if (T == FixedTypes::SCHEMA || T == FixedTypes::ROW) {
		in >> value.schemaVersionId_;
	}
	if (T == FixedTypes::ROW) {
		in >> value.cxtSrc_.getMode_;
		in >> value.cxtSrc_.txnMode_;
	}
}

template<typename Action>
bool StatementMessage::OptionCategorySwitcher::switchByCategory(
		OptionCategory category, Action &action) const {
	switch (category) {
	case OptionCategories::CLIENT_CLASSIC:
		action.template opCategory<OptionCategories::CLIENT_CLASSIC>();
		break;
	case OptionCategories::BOTH_CLASSIC:
		action.template opCategory<OptionCategories::BOTH_CLASSIC>();
		break;
	case OptionCategories::CLIENT:
		action.template opCategory<OptionCategories::CLIENT>();
		break;
	case OptionCategories::SERVER_TXN:
		action.template opCategory<OptionCategories::SERVER_TXN>();
		break;
	case OptionCategories::SERVER_SQL_CLASSIC:
		action.template opCategory<OptionCategories::SERVER_SQL_CLASSIC>();
		break;
	case OptionCategories::SERVER_SQL:
		action.template opCategory<OptionCategories::SERVER_SQL>();
		break;
	case OptionCategories::CLIENT_ADVANCED:
		action.template opCategory<OptionCategories::CLIENT_ADVANCED>();
		break;
	case OptionCategories::CLIENT_SQL:
		action.template opCategory<OptionCategories::CLIENT_SQL>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::CLIENT_CLASSIC>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::LEGACY_VERSION_BLOCK:
		action.template opType<Options::LEGACY_VERSION_BLOCK>();
		break;
	case Options::TXN_TIMEOUT_INTERVAL:
		action.template opType<Options::TXN_TIMEOUT_INTERVAL>();
		break;
	case Options::FOR_UPDATE:
		action.template opType<Options::FOR_UPDATE>();
		break;
	case Options::SYSTEM_MODE:
		action.template opType<Options::SYSTEM_MODE>();
		break;
	case Options::DB_NAME:
		action.template opType<Options::DB_NAME>();
		break;
	case Options::CONTAINER_ATTRIBUTE:
		action.template opType<Options::CONTAINER_ATTRIBUTE>();
		break;
	case Options::PUT_ROW_OPTION:
		action.template opType<Options::PUT_ROW_OPTION>();
		break;
	case Options::REQUEST_MODULE_TYPE:
		action.template opType<Options::REQUEST_MODULE_TYPE>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::SERVER_SQL_CLASSIC>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::REPLY_PID:
		action.template opType<Options::REPLY_PID>();
		break;
	case Options::REPLY_EVENT_TYPE:
		action.template opType<Options::REPLY_EVENT_TYPE>();
		break;
	case Options::UUID:
		action.template opType<Options::UUID>();
		break;
	case Options::QUERY_ID:
		action.template opType<Options::QUERY_ID>();
		break;
	case Options::CONTAINER_ID:
		action.template opType<Options::CONTAINER_ID>();
		break;
	case Options::USER_TYPE:
		action.template opType<Options::USER_TYPE>();
		break;
	case Options::DB_VERSION_ID:
		action.template opType<Options::DB_VERSION_ID>();
		break;
	case Options::EXTENSION_NAME:
		action.template opType<Options::EXTENSION_NAME>();
		break;
	case Options::EXTENSION_PARAMS:
		action.template opType<Options::EXTENSION_PARAMS>();
		break;
	case Options::INDEX_NAME:
		action.template opType<Options::INDEX_NAME>();
		break;
	case Options::EXTENSION_TYPE_LIST:
		action.template opType<Options::EXTENSION_TYPE_LIST>();
		break;
	case Options::FOR_SYNC:
		action.template opType<Options::FOR_SYNC>();
		break;
	case Options::ACK_EVENT_TYPE:
		action.template opType<Options::ACK_EVENT_TYPE>();
		break;
	case Options::SUB_CONTAINER_ID:
		action.template opType<Options::SUB_CONTAINER_ID>();
		break;
	case Options::JOB_EXEC_ID:
		action.template opType<Options::JOB_EXEC_ID>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::BOTH_CLASSIC>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::STATEMENT_TIMEOUT_INTERVAL:
		action.template opType<Options::STATEMENT_TIMEOUT_INTERVAL>();
		break;
	case Options::MAX_ROWS:
		action.template opType<Options::MAX_ROWS>();
		break;
	case Options::FETCH_SIZE:
		action.template opType<Options::FETCH_SIZE>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::CLIENT>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::CLIENT_ID:
		action.template opType<Options::CLIENT_ID>();
		break;
	case Options::FETCH_BYTES_SIZE:
		action.template opType<Options::FETCH_BYTES_SIZE>();
		break;
	case Options::META_CONTAINER_ID:
		action.template opType<Options::META_CONTAINER_ID>();
		break;
	case Options::FEATURE_VERSION:
		action.template opType<Options::FEATURE_VERSION>();
		break;
	case Options::ACCEPTABLE_FEATURE_VERSION:
		action.template opType<Options::ACCEPTABLE_FEATURE_VERSION>();
		break;
	case Options::CONTAINER_VISIBILITY:
		action.template opType<Options::CONTAINER_VISIBILITY>();
		break;
	case Options::META_NAMING_TYPE:
		action.template opType<Options::META_NAMING_TYPE>();
		break;
	case Options::QUERY_CONTAINER_KEY:
		action.template opType<Options::QUERY_CONTAINER_KEY>();
		break;
	case Options::APPLICATION_NAME:
		action.template opType<Options::APPLICATION_NAME>();
		break;
	case Options::STORE_MEMORY_AGING_SWAP_RATE:
		action.template opType<Options::STORE_MEMORY_AGING_SWAP_RATE>();
		break;
	case Options::TIME_ZONE_OFFSET:
		action.template opType<Options::TIME_ZONE_OFFSET>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::SERVER_TXN>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::LOCK_CONFLICT_START_TIME:
		action.template opType<Options::LOCK_CONFLICT_START_TIME>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::SERVER_SQL>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::CREATE_DROP_INDEX_MODE:
		action.template opType<Options::CREATE_DROP_INDEX_MODE>();
		break;
	case Options::RETRY_MODE:
		action.template opType<Options::RETRY_MODE>();
		break;
	case Options::DDL_TRANSACTION_MODE:
		action.template opType<Options::DDL_TRANSACTION_MODE>();
		break;
	case Options::SQL_CASE_SENSITIVITY:
		action.template opType<Options::SQL_CASE_SENSITIVITY>();
		break;
	case Options::INTERVAL_BASE_VALUE:
		action.template opType<Options::INTERVAL_BASE_VALUE>();
		break;
	case Options::JOB_VERSION:
		action.template opType<Options::JOB_VERSION>();
		break;
	case Options::NOSQL_SYNC_ID:
		action.template opType<Options::NOSQL_SYNC_ID>();
		break;
	case Options::COMPOSITE_INDEX:
		action.template opType<Options::COMPOSITE_INDEX>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::CLIENT_ADVANCED>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::SUB_CONTAINER_VERSION:
		action.template opType<Options::SUB_CONTAINER_VERSION>();
		break;
	case Options::SUB_CONTAINER_EXPIRABLE:
		action.template opType<Options::SUB_CONTAINER_EXPIRABLE>();
		break;
	case Options::DIST_QUERY:
		action.template opType<Options::DIST_QUERY>();
		break;
	case Options::LARGE_INFO:
		action.template opType<Options::LARGE_INFO>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
bool StatementMessage::OptionTypeSwitcher<
		StatementMessage::OptionCategories::CLIENT_SQL>::switchByType(
		OptionType type, Action &action) const {
	switch (type) {
	case Options::PRAGMA_LIST:
		action.template opType<Options::PRAGMA_LIST>();
		break;
	default:
		return false;
	}
	return true;
}

template<typename Action>
bool StatementMessage::FixedTypeSwitcher::switchByFixedType(
		FixedType type, Action &action) const {
	switch (type) {
	case FixedTypes::BASIC:
		action.template opFixedType<FixedTypes::BASIC>();
		break;
	case FixedTypes::CONTAINER:
		action.template opFixedType<FixedTypes::CONTAINER>();
		break;
	case FixedTypes::SCHEMA:
		action.template opFixedType<FixedTypes::SCHEMA>();
		break;
	case FixedTypes::SESSION:
		action.template opFixedType<FixedTypes::SESSION>();
		break;
	case FixedTypes::ROW:
		action.template opFixedType<FixedTypes::ROW>();
		break;
	case FixedTypes::MULTI:
		action.template opFixedType<FixedTypes::MULTI>();
		break;
	case FixedTypes::PARTIAL_DDL:
		action.template opFixedType<FixedTypes::PARTIAL_DDL>();
		break;
	default:
		return false;
	}
	return true;
}

template<typename Action>
bool StatementMessage::EventTypeSwitcher::switchByEventType(
		EventType type, Action &action) const {
	switch (type) {
	case CONNECT:
		action.template opEventType<CONNECT>();
		break;
	case DISCONNECT:
		action.template opEventType<DISCONNECT>();
		break;
	case LOGIN:
		action.template opEventType<LOGIN>();
		break;
	case LOGOUT:
		action.template opEventType<LOGOUT>();
		break;
	case GET_PARTITION_ADDRESS:
		action.template opEventType<GET_PARTITION_ADDRESS>();
		break;
	case GET_PARTITION_CONTAINER_NAMES:
		action.template opEventType<GET_PARTITION_CONTAINER_NAMES>();
		break;
	case GET_CONTAINER_PROPERTIES:
		action.template opEventType<GET_CONTAINER_PROPERTIES>();
		break;
	case GET_CONTAINER:
		action.template opEventType<GET_CONTAINER>();
		break;
	case PUT_CONTAINER:
		action.template opEventType<PUT_CONTAINER>();
		break;
	case DROP_CONTAINER:
		action.template opEventType<DROP_CONTAINER>();
		break;
	case PUT_USER:
		action.template opEventType<PUT_USER>();
		break;
	case DROP_USER:
		action.template opEventType<DROP_USER>();
		break;
	case GET_USERS:
		action.template opEventType<GET_USERS>();
		break;
	case PUT_DATABASE:
		action.template opEventType<PUT_DATABASE>();
		break;
	case DROP_DATABASE:
		action.template opEventType<DROP_DATABASE>();
		break;
	case GET_DATABASES:
		action.template opEventType<GET_DATABASES>();
		break;
	case PUT_PRIVILEGE:
		action.template opEventType<PUT_PRIVILEGE>();
		break;
	case DROP_PRIVILEGE:
		action.template opEventType<DROP_PRIVILEGE>();
		break;
	case FLUSH_LOG:
		action.template opEventType<FLUSH_LOG>();
		break;
	case CLOSE_RESULT_SET:
		action.template opEventType<CLOSE_RESULT_SET>();
		break;
	case CREATE_INDEX:
		action.template opEventType<CREATE_INDEX>();
		break;
	case DELETE_INDEX:
		action.template opEventType<DELETE_INDEX>();
		break;
	case CREATE_TRIGGER:
		action.template opEventType<CREATE_TRIGGER>();
		break;
	case DELETE_TRIGGER:
		action.template opEventType<DELETE_TRIGGER>();
		break;
	case CREATE_TRANSACTION_CONTEXT:
		action.template opEventType<CREATE_TRANSACTION_CONTEXT>();
		break;
	case CLOSE_TRANSACTION_CONTEXT:
		action.template opEventType<CLOSE_TRANSACTION_CONTEXT>();
		break;
	case COMMIT_TRANSACTION:
		action.template opEventType<COMMIT_TRANSACTION>();
		break;
	case ABORT_TRANSACTION:
		action.template opEventType<ABORT_TRANSACTION>();
		break;
	case GET_ROW:
		action.template opEventType<GET_ROW>();
		break;
	case QUERY_TQL:
		action.template opEventType<QUERY_TQL>();
		break;
	case PUT_ROW:
		action.template opEventType<PUT_ROW>();
		break;
	case PUT_MULTIPLE_ROWS:
		action.template opEventType<PUT_MULTIPLE_ROWS>();
		break;
	case UPDATE_ROW_BY_ID:
		action.template opEventType<UPDATE_ROW_BY_ID>();
		break;
	case REMOVE_ROW:
		action.template opEventType<REMOVE_ROW>();
		break;
	case REMOVE_ROW_BY_ID:
		action.template opEventType<REMOVE_ROW_BY_ID>();
		break;
	case GET_MULTIPLE_ROWS:
		action.template opEventType<GET_MULTIPLE_ROWS>();
		break;
	case QUERY_COLLECTION_GEOMETRY_RELATED:
		action.template opEventType<QUERY_COLLECTION_GEOMETRY_RELATED>();
		break;
	case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION:
		action.template opEventType<QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION>();
		break;
	case APPEND_TIME_SERIES_ROW:
		action.template opEventType<APPEND_TIME_SERIES_ROW>();
		break;
	case GET_TIME_SERIES_ROW_RELATED:
		action.template opEventType<GET_TIME_SERIES_ROW_RELATED>();
		break;
	case INTERPOLATE_TIME_SERIES_ROW:
		action.template opEventType<INTERPOLATE_TIME_SERIES_ROW>();
		break;
	case AGGREGATE_TIME_SERIES:
		action.template opEventType<AGGREGATE_TIME_SERIES>();
		break;
	case QUERY_TIME_SERIES_RANGE:
		action.template opEventType<QUERY_TIME_SERIES_RANGE>();
		break;
	case QUERY_TIME_SERIES_SAMPLING:
		action.template opEventType<QUERY_TIME_SERIES_SAMPLING>();
		break;
	case FETCH_RESULT_SET:
		action.template opEventType<FETCH_RESULT_SET>();
		break;
	case CREATE_MULTIPLE_TRANSACTION_CONTEXTS:
		action.template opEventType<CREATE_MULTIPLE_TRANSACTION_CONTEXTS>();
		break;
	case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		action.template opEventType<CLOSE_MULTIPLE_TRANSACTION_CONTEXTS>();
		break;
	case EXECUTE_MULTIPLE_QUERIES:
		action.template opEventType<EXECUTE_MULTIPLE_QUERIES>();
		break;
	case GET_MULTIPLE_CONTAINER_ROWS:
		action.template opEventType<GET_MULTIPLE_CONTAINER_ROWS>();
		break;
	case PUT_MULTIPLE_CONTAINER_ROWS:
		action.template opEventType<PUT_MULTIPLE_CONTAINER_ROWS>();
		break;
	case CONTINUE_CREATE_INDEX:
		action.template opEventType<CONTINUE_CREATE_INDEX>();
		break;
	case CONTINUE_ALTER_CONTAINER:
		action.template opEventType<CONTINUE_ALTER_CONTAINER>();
		break;
	case UPDATE_DATA_STORE_STATUS:
		action.template opEventType<UPDATE_DATA_STORE_STATUS>();
		break;
	case PUT_LARGE_CONTAINER:
		action.template opEventType<PUT_LARGE_CONTAINER>();
		break;
	case UPDATE_CONTAINER_STATUS:
		action.template opEventType<UPDATE_CONTAINER_STATUS>();
		break;
	case CREATE_LARGE_INDEX:
		action.template opEventType<CREATE_LARGE_INDEX>();
		break;
	case DROP_LARGE_INDEX:
		action.template opEventType<DROP_LARGE_INDEX>();
		break;
	case SQL_GET_CONTAINER:
		action.template opEventType<SQL_GET_CONTAINER>();
		break;
	case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
		action.template opEventType<REMOVE_MULTIPLE_ROWS_BY_ID_SET>();
		break;
	case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		action.template opEventType<UPDATE_MULTIPLE_ROWS_BY_ID_SET>();
		break;
	case REPLICATION_LOG2:
		action.template opEventType<REPLICATION_LOG2>();
		break;
	case REPLICATION_ACK2:
		action.template opEventType<REPLICATION_ACK2>();
		break;
	default:
		return false;
	}
	return true;
}

StatementMessage::OptionSet::OptionSet(util::StackAllocator &alloc) :
		entryMap_(alloc) {
	setLegacyVersionBlock(true);
}

void StatementMessage::OptionSet::setLegacyVersionBlock(bool enabled) {
	set<Options::LEGACY_VERSION_BLOCK>((enabled ? 1 : 0));
}

void StatementMessage::OptionSet::checkFeature() {
	const int32_t featureVersion = get<Options::FEATURE_VERSION>();
	const int32_t supportedVersion = FEATURE_SUPPORTED_MAX;

	if (featureVersion > FEATURE_SUPPORTED_MAX) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_OPTION_TYPE_INVALID,
				"Unsupported feature requested "
				"(requestedVersion=" << featureVersion <<
				", supportedVersion" << supportedVersion << ")");
	}
}

void StatementMessage::OptionSet::encode(EventByteOutStream &out) const {
	const size_t totalHeadPos = out.base().position();
	uint32_t totalSize = 0;
	out << totalSize;
	const size_t totalBodyPos = out.base().position();

	size_t rangeHeadPos = 0;
	size_t rangeBodyPos = 0;
	bool rangeStarted = false;
	OptionCategory lastCategory = OptionCategories::CLIENT_CLASSIC;

	for (EntryMap::const_iterator it = entryMap_.begin();; ++it) {
		const bool found = (it != entryMap_.end());
		const OptionType optionType = (found ? it->first : -1);

		const OptionCategory category = toOptionCategory(optionType);
		const bool classic = isOptionClassic(optionType);
		const bool rangeChanged = (!classic && category != lastCategory);
		lastCategory = category;

		if (rangeStarted && (!found || rangeChanged)) {
			const size_t rangeEndPos = out.base().position();
			const uint32_t rangeSize =
					static_cast<uint32_t>(rangeEndPos - rangeBodyPos);
			out.base().position(rangeHeadPos);
			out << rangeSize;
			out.base().position(rangeEndPos);
			rangeStarted = false;
		}

		if (!found) {
			break;
		}

		if (rangeChanged) {
			const OptionType rangeType = static_cast<OptionType>(
					category * Options::TYPE_RANGE_BASE);
			out << rangeType;

			rangeHeadPos = out.base().position();
			const uint32_t rangeSize = 0;
			out << rangeSize;
			rangeBodyPos = out.base().position();
			rangeStarted = true;
		}

		out << optionType;

		EntryEncoder<UNDEF_CATEGORY> encoder(out, *this, optionType);
		if (!OptionCategorySwitcher().switchByCategory(category, encoder)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_TXN_OPTION_TYPE_INVALID,
					"(optionType=" << static_cast<int32_t>(optionType) << ")");
		}
	}

	const size_t totalEndPos = out.base().position();
	out.base().position(totalHeadPos);
	totalSize = static_cast<uint32_t>(totalEndPos - totalBodyPos);
	out << totalSize;
	out.base().position(totalEndPos);
}

void StatementMessage::OptionSet::decode(EventByteInStream &in) {
	uint32_t totalSize;
	in >> totalSize;

	const size_t endPos = in.base().position() + totalSize;

	size_t rangeEndPos = 0;

	while (in.base().position() < endPos) {
		OptionType optionType;
		in >> optionType;

		const OptionCategory category = toOptionCategory(optionType);
		const bool classic = isOptionClassic(optionType);

		if (!classic && optionType % Options::TYPE_RANGE_BASE == 0) {
			uint32_t rangeSize;
			in >> rangeSize;
			rangeEndPos = in.base().position() + rangeSize;
			continue;
		}

		EntryDecoder<UNDEF_CATEGORY> decoder(in, *this, optionType);
		if (!OptionCategorySwitcher().switchByCategory(category, decoder)) {
			if (classic) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_OPTION_TYPE_INVALID,
						"(optionType=" << static_cast<int32_t>(optionType) << ")");
			}
			else if (rangeEndPos == 0) {
				TXN_THROW_DECODE_ERROR(
						GS_ERROR_TXN_DECODE_FAILED,
						"(optionType =" << static_cast<int32_t>(optionType) << ")");
			}
			else {
				in.base().position(rangeEndPos);
			}
		}
	}
	in.base().position(endPos);

	checkFeature();
}

util::StackAllocator& StatementMessage::OptionSet::getAllocator() {
	return *entryMap_.get_allocator().base();
}

StatementMessage::OptionCategory StatementMessage::OptionSet::toOptionCategory(
		OptionType optionType) {
	const int32_t base = optionType / Options::TYPE_RANGE_BASE;
	if (base == 0 && optionType >= Options::TYPE_SQL_CLASSIC_START) {
		return OptionCategories::SERVER_SQL_CLASSIC;
	}
	return static_cast<OptionCategory>(base);
}

bool StatementMessage::OptionSet::isOptionClassic(OptionType optionType) {
	return (optionType < Options::TYPE_RANGE_START);
}

template<StatementMessage::OptionCategory C>
StatementMessage::OptionSet::EntryEncoder<C>::EntryEncoder(
		EventByteOutStream &out, const OptionSet &optionSet,
		OptionType type) :
		out_(out), optionSet_(optionSet), type_(type) {
}

template<StatementMessage::OptionCategory C>
template<StatementMessage::OptionCategory T>
void StatementMessage::OptionSet::EntryEncoder<C>::opCategory() {
	UTIL_STATIC_ASSERT(C == UNDEF_CATEGORY);
	EntryEncoder<T> sub(out_, optionSet_, type_);
	OptionTypeSwitcher<T>().switchByType(type_, sub);
}

template<StatementMessage::OptionCategory C>
template<StatementMessage::OptionType T>
void StatementMessage::OptionSet::EntryEncoder<C>::opType() {
	OptionCoder<T>().encode(out_, optionSet_.get<T>());
}

template<StatementMessage::OptionCategory C>
StatementMessage::OptionSet::EntryDecoder<C>::EntryDecoder(
		EventByteInStream &in, OptionSet &optionSet, OptionType type) :
		in_(in), optionSet_(optionSet), type_(type) {
}

template<StatementMessage::OptionCategory C>
template<StatementMessage::OptionCategory T>
void StatementMessage::OptionSet::EntryDecoder<C>::opCategory() {
	UTIL_STATIC_ASSERT(C == UNDEF_CATEGORY);
	EntryDecoder<T> sub(in_, optionSet_, type_);
	OptionTypeSwitcher<T>().switchByType(type_, sub);
}

template<StatementMessage::OptionCategory C>
template<StatementMessage::OptionType T>
void StatementMessage::OptionSet::EntryDecoder<C>::opType() {
	optionSet_.set<T>(OptionCoder<T>().decode(in_, optionSet_.getAllocator()));
}

StatementMessage::FixedRequest::FixedRequest(const Source &src) :
		pId_(src.pId_),
		stmtType_(src.stmtType_),
		clientId_(TXN_EMPTY_CLIENTID),
		cxtSrc_(stmtType_, StatementHandler::isUpdateStatement(stmtType_)),
		schemaVersionId_(UNDEF_SCHEMAVERSIONID),
		startStmtId_(UNDEF_STATEMENTID) {
}

void StatementMessage::FixedRequest::encode(EventByteOutStream &out) const {
	const FixedType fixedType = toFixedType(stmtType_);
	Encoder encoder(out, *this);
	if (!FixedTypeSwitcher().switchByFixedType(fixedType, encoder)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(type=" << stmtType_ << ")");
	}
}

void StatementMessage::FixedRequest::decode(EventByteInStream &in) {
	const FixedType fixedType = toFixedType(stmtType_);
	Decoder decoder(in, *this);
	if (!FixedTypeSwitcher().switchByFixedType(fixedType, decoder)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(type=" << stmtType_ << ")");
	}
}

StatementMessage::FixedType StatementMessage::FixedRequest::toFixedType(
		EventType type) {
	EventToFixedType action;
	if (!EventTypeSwitcher().switchByEventType(type, action)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(type=" << type << ")");
	}
	return action.fixedType_;
}

StatementMessage::FixedRequest::Source::Source(
		PartitionId pId, EventType stmtType) :
		pId_(pId),
		stmtType_(stmtType) {
}

StatementMessage::FixedRequest::Encoder::Encoder(
		EventByteOutStream &out, const FixedRequest &request) :
		out_(out),
		request_(request) {
}

template<StatementMessage::FixedType T>
void StatementMessage::FixedRequest::Encoder::opFixedType() {
	FixedCoder<T>().encode(out_, request_);
}

StatementMessage::FixedRequest::Decoder::Decoder(
		EventByteInStream &in, FixedRequest &request) :
		in_(in),
		request_(request) {
}

template<StatementMessage::FixedType T>
void StatementMessage::FixedRequest::Decoder::opFixedType() {
	FixedCoder<T>().decode(in_, request_);
}

StatementMessage::FixedRequest::EventToFixedType::EventToFixedType() :
		fixedType_(-1) {
}

template<EventType T>
void StatementMessage::FixedRequest::EventToFixedType::opEventType() {
	fixedType_ = FixedTypeResolver<T>::FIXED_TYPE;
}

StatementMessage::Request::Request(
		util::StackAllocator &alloc, const FixedRequest::Source &src) :
		fixed_(src),
		optional_(alloc) {
}

void StatementMessage::Request::encode(EventByteOutStream &out) const {
	fixed_.encode(out);
	optional_.encode(out);
}

void StatementMessage::Request::decode(EventByteInStream &in) {
	fixed_.decode(in);
	optional_.decode(in);
}

StatementMessage::QueryContainerKey::QueryContainerKey(
		util::StackAllocator &alloc) :
		containerKeyBinary_(alloc),
		enabled_(false) {
}

StatementMessage::UUIDObject::UUIDObject() {
	memset(uuid_, 0, TXN_CLIENT_UUID_BYTE_SIZE);
}

StatementMessage::UUIDObject::UUIDObject(
		const uint8_t (&uuid)[TXN_CLIENT_UUID_BYTE_SIZE]) {
	memcpy(uuid_, uuid, sizeof(uuid));
}

void StatementMessage::UUIDObject::get(
		uint8_t (&uuid)[TXN_CLIENT_UUID_BYTE_SIZE]) const {
	memcpy(uuid, uuid_, sizeof(uuid));
}

StatementMessage::ExtensionParams::ExtensionParams(
		util::StackAllocator &alloc) :
		fixedPart_(alloc),
		varPart_(alloc) {
}

StatementMessage::ExtensionColumnTypes::ExtensionColumnTypes(
		util::StackAllocator &alloc) :
		columnTypeList_(alloc) {
}

StatementMessage::IntervalBaseValue::IntervalBaseValue() :
		baseValue_(0),
		enabled_(false) {
}

StatementMessage::PragmaList::PragmaList(util::StackAllocator &alloc) :
		list_(alloc) {
}

StatementMessage::CompositeIndexInfos::CompositeIndexInfos(
		util::StackAllocator &alloc) :
		indexInfoList_(alloc) {
}

void StatementMessage::CompositeIndexInfos::setIndexInfos(
		util::Vector<IndexInfo> &indexInfoList) {
	for (size_t pos = 0; pos < indexInfoList.size(); pos++) {
		IndexInfo &indexInfo = indexInfoList[pos];
		if (indexInfo.isComposite()) {
			indexInfoList_.push_back(indexInfo);
		}
	}
}


template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::UUIDObject, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	out.writeAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
}

template<>
StatementMessage::UUIDObject
StatementMessage::CustomOptionCoder<
		StatementMessage::UUIDObject, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	static_cast<void>(alloc);
	UUIDObject value;
	in.readAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::ExtensionParams*, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	assert(value != NULL);
	for (size_t i = 0; i < 2; i++) {
		const util::XArray<uint8_t> &part =
				(i == 0 ? value->fixedPart_ : value->varPart_);
		const size_t size = part.size();
		out << static_cast<uint64_t>(size);
		out.writeAll(part.data(), size);
	}
}

template<>
StatementMessage::ExtensionParams*
StatementMessage::CustomOptionCoder<
		StatementMessage::ExtensionParams*, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	ExtensionParams *value = ALLOC_NEW(alloc) ExtensionParams(alloc);
	for (size_t i = 0; i < 2; i++) {
		util::XArray<uint8_t> &part =
				(i == 0 ? value->fixedPart_ : value->varPart_);
		uint64_t size;
		in >> size;
		part.resize(static_cast<size_t>(size));
		in.readAll(part.data(), static_cast<size_t>(size));
	}
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::ExtensionColumnTypes*, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	assert(value != NULL);
	const size_t size = value->columnTypeList_.size();
	out << static_cast<uint64_t>(size);
	out.writeAll(value->columnTypeList_.data(), size);
}

template<>
StatementMessage::ExtensionColumnTypes*
StatementMessage::CustomOptionCoder<
		StatementMessage::ExtensionColumnTypes*, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	ExtensionColumnTypes *value = ALLOC_NEW(alloc) ExtensionColumnTypes(alloc);
	uint64_t size;
	in >> size;
	value->columnTypeList_.resize(static_cast<size_t>(size));
	in.readAll(
			value->columnTypeList_.data(),
			sizeof(ColumnType) * static_cast<size_t>(size));
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::CompositeIndexInfos*, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}

template<>
StatementMessage::CompositeIndexInfos*
StatementMessage::CustomOptionCoder<
		StatementMessage::CompositeIndexInfos*, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	CompositeIndexInfos *value = ALLOC_NEW(alloc) CompositeIndexInfos(alloc);
	uint64_t size;
	in >> size;
	value->indexInfoList_.resize(static_cast<size_t>(size), IndexInfo(alloc));
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::decodeIndexInfo(in, value->indexInfoList_[pos]);
	}
	return value;
}


template<>
void StatementMessage::CustomOptionCoder<ClientId, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	out << value.sessionId_;
	out.writeAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
}

template<>
ClientId StatementMessage::CustomOptionCoder<ClientId, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	static_cast<void>(alloc);
	ClientId value;
	in >> value.sessionId_;
	in.readAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::QueryContainerKey*, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	Utils::encodeBool(out, value->enabled_);
	Utils::encodeVarSizeBinary(out, value->containerKeyBinary_);
}

template<>
StatementMessage::QueryContainerKey*
StatementMessage::CustomOptionCoder<
		StatementMessage::QueryContainerKey*, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	QueryContainerKey *value = ALLOC_NEW(alloc) QueryContainerKey(alloc);
	value->enabled_ = Utils::decodeBool(in);
	Utils::decodeVarSizeBinary(in, value->containerKeyBinary_);
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<util::TimeZone, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	out << value.getOffsetMillis();
}

template<>
util::TimeZone StatementMessage::CustomOptionCoder<util::TimeZone, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	util::TimeZone::Offset offsetMillis;
	in >> offsetMillis;

	util::TimeZone zone;
	zone.setOffsetMillis(offsetMillis);

	return zone;
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::CaseSensitivity, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	out << value.flags_;
}

template<>
StatementMessage::CaseSensitivity
StatementMessage::CustomOptionCoder<
		StatementMessage::CaseSensitivity, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	static_cast<void>(alloc);
	CaseSensitivity value;
	in >> value.flags_;
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::IntervalBaseValue, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	assert(value.enabled_);
	out << value.baseValue_;
}

template<>
StatementMessage::IntervalBaseValue
StatementMessage::CustomOptionCoder<
		StatementMessage::IntervalBaseValue, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	static_cast<void>(alloc);
	IntervalBaseValue value;
	in >> value.baseValue_;
	value.enabled_ = true;
	return value;
}

template<>
void StatementMessage::CustomOptionCoder<SQLTableInfo*, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_ENCODE_FAILED, "");
}

template<>
SQLTableInfo* StatementMessage::CustomOptionCoder<SQLTableInfo*, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	return SQLExecution::decodeTableInfo(alloc, in);
}

template<>
void StatementMessage::CustomOptionCoder<
		StatementMessage::PragmaList*, 0>::encode(
		EventByteOutStream &out, const ValueType &value) const {
	const int32_t listSize = static_cast<int32_t>(value->list_.size());
	out << listSize;
	for (int32_t pos = 0; pos < listSize; pos++) {
		out << value->list_[pos].first;
		out << value->list_[pos].second;
	}
}

template<>
StatementMessage::PragmaList*
StatementMessage::CustomOptionCoder<
		StatementMessage::PragmaList*, 0>::decode(
		EventByteInStream &in, util::StackAllocator &alloc) const {
	PragmaList *value = ALLOC_NEW(alloc) PragmaList(alloc);
	int32_t listSize;
	in >> listSize;
	for (int32_t pos = 0; pos < listSize; pos++) {
		value->list_.push_back(std::make_pair(
				util::String(alloc), util::String(alloc)));
		in >> value->list_.back().first;
		in >> value->list_.back().second;
	}
	return value;
}

Sampling StatementHandler::SamplingQuery::toSamplingOption() const {
	Sampling sampling;
	sampling.interval_ = interval_;
	sampling.timeUnit_ = timeUnit_;

	sampling.interpolatedColumnIdList_.resize(interpolatedColumnIdList_.size());
	std::copy(interpolatedColumnIdList_.begin(),
		interpolatedColumnIdList_.end(),
		sampling.interpolatedColumnIdList_.begin());
	sampling.mode_ = mode_;

	return sampling;
}

const StatementHandler::ClusterRole StatementHandler::CROLE_UNKNOWN = 0;
const StatementHandler::ClusterRole StatementHandler::CROLE_SUBMASTER = 1 << 2;
const StatementHandler::ClusterRole StatementHandler::CROLE_MASTER = 1 << 0;
const StatementHandler::ClusterRole StatementHandler::CROLE_FOLLOWER = 1 << 1;
const StatementHandler::ClusterRole StatementHandler::CROLE_ANY = ~0;
const char8_t* const StatementHandler::clusterRoleStr[8] = {"UNKNOWN",
	"MASTER", "FOLLOWER", "MASTER, FOLLOWER", "SUB_CLUSTER",
	"MASTER, SUB_CLUSTER", "FOLLOWER, SUB_CLUSTER",
	"MASTER, FOLLOWER, SUB_CLUSTER"
};

const StatementHandler::PartitionRoleType StatementHandler::PROLE_UNKNOWN = 0;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_OWNER = 1
																		  << 0;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_BACKUP = 1
																		   << 1;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_CATCHUP =
	1 << 2;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_NONE = 1 << 3;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_ANY = ~0;
const char8_t* const StatementHandler::partitionRoleTypeStr[16] = {"UNKNOWN",
	"OWNER", "BACKUP", "OWNER, BACKUP", "CATCHUP", "OWNER, CATCHUP",
	"BACKUP, CATCHUP", "OWNER, BACKUP, CATCHUP", "NONE", "OWNER, NONE",
	"BACKUP, NONE", "OWNER, BACKUP, NONE", "CATCHUP, NONE",
	"OWNER, CATCHUP, NONE", "BACKUP, CATCHUP, NONE",
	"OWNER, BACKUP, CATCHUP, NONE"
};

const StatementHandler::PartitionStatus StatementHandler::PSTATE_UNKNOWN = 0;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_ON = 1 << 0;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_SYNC = 1 << 1;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_OFF = 1 << 2;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_STOP = 1 << 3;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_ANY = ~0;
const char8_t* const StatementHandler::partitionStatusStr[16] = {"UNKNOWN",
	"ON", "SYNC", "ON, SYNC", "OFF", "ON, OFF", "SYNC, OFF", "ON, SYNC, OFF",
	"STOP", "ON, STOP", "SYNC, STOP", "ON, SYNC, STOP", "OFF, STOP",
	"ON, OFF, STOP", "SYNC, OFF, STOP", "ON, SYNC, OFF, STOP"
};

const StatementHandler::StatementExecStatus
	StatementHandler::TXN_STATEMENT_SUCCESS = 0;
const StatementHandler::StatementExecStatus
	StatementHandler::TXN_STATEMENT_ERROR = 1;
const StatementHandler::StatementExecStatus
	StatementHandler::TXN_STATEMENT_NODE_ERROR = 2;
const StatementHandler::StatementExecStatus
	StatementHandler::TXN_STATEMENT_DENY = 3;
const StatementHandler::StatementExecStatus
	StatementHandler::TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT =
		TXN_STATEMENT_SUCCESS;

const StatementHandler::ProtocolVersion
	StatementHandler::PROTOCOL_VERSION_UNDEFINED = 0;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V1_0_X_CLIENT_VERSION = 1;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V1_1_X_CLIENT_VERSION = TXN_V1_0_X_CLIENT_VERSION;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V1_5_X_CLIENT_VERSION = 2;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V2_0_X_CLIENT_VERSION = 3;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V2_1_X_CLIENT_VERSION = 4;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V2_5_X_CLIENT_VERSION = 5;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V2_7_X_CLIENT_VERSION = 6;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V2_8_X_CLIENT_VERSION = 7;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V2_9_X_CLIENT_VERSION = 8;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V3_0_X_CLIENT_VERSION = 9;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V3_0_X_CE_CLIENT_VERSION = 10;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V3_1_X_CLIENT_VERSION = 11;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V3_2_X_CLIENT_VERSION = 12;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V3_5_X_CLIENT_VERSION = 13;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V4_0_0_CLIENT_VERSION = 14;
const StatementHandler::ProtocolVersion
	StatementHandler::TXN_V4_0_1_CLIENT_VERSION = 15;
const StatementHandler::ProtocolVersion StatementHandler::TXN_CLIENT_VERSION =
	TXN_V4_0_1_CLIENT_VERSION;

static const StatementHandler::ProtocolVersion
	ACCEPTABLE_NOSQL_CLIENT_VERSIONS[] = {
		StatementHandler::TXN_CLIENT_VERSION,
		StatementHandler::PROTOCOL_VERSION_UNDEFINED /* sentinel */
};

StatementHandler::StatementHandler()
	: clusterService_(NULL),
	  clusterManager_(NULL),
	  chunkManager_(NULL),
	  dataStore_(NULL),
	  logManager_(NULL),
	  partitionTable_(NULL),
	  transactionService_(NULL),
	  transactionManager_(NULL),
	  triggerService_(NULL),
	  systemService_(NULL),
	  recoveryManager_(NULL)
	  ,
	  sqlService_(NULL),
	  isNewSQL_(false)
{}

StatementHandler::~StatementHandler() {}

void StatementHandler::initialize(const ResourceSet &resourceSet) {

	resourceSet_ = &resourceSet;

	clusterService_ = resourceSet.clsSvc_;
	clusterManager_ = resourceSet.clsMgr_;
	chunkManager_ = resourceSet.chunkMgr_;
	dataStore_ = resourceSet.ds_;
	logManager_ = resourceSet.logMgr_;
	partitionTable_ = resourceSet.pt_;
	transactionService_ = resourceSet.txnSvc_;
	transactionManager_ = resourceSet.txnMgr_;
	triggerService_ = resourceSet.trgSvc_;
	systemService_ = resourceSet.sysSvc_;
	recoveryManager_ = resourceSet.recoveryMgr_;
	sqlService_ = resourceSet.sqlSvc_;

	const size_t forNonSystem = 0;
	const size_t forSystem = 1;
	const size_t forNonPartitioning = 0;
	const size_t forPartitioning = 1;
	const size_t noCheckLength = 0;
	const size_t checkLength = 1;

	for (size_t i = forNonSystem; i <= forSystem; i++) {
		for (size_t j = forNonPartitioning; j <= forPartitioning; j++) {
			for (size_t k = noCheckLength; k <= checkLength; k++) {
				keyConstraint_[i][j][k].systemPartAllowed_ = (i == forSystem);
				keyConstraint_[i][j][k].largeContainerIdAllowed_ = (j == forPartitioning);
				keyConstraint_[i][j][k].maxTotalLength_ = (k == checkLength ?
					dataStore_->getValueLimitConfig().getLimitContainerNameSize() : std::numeric_limits<uint32_t>::max());
			}
		}
	}
}

void StatementHandler::setNewSQL() {
	isNewSQL_ = true;
	transactionManager_ = sqlService_->getTransactionManager();
}

/*!
	@brief Sets the information about success reply to an client
*/
void StatementHandler::setSuccessReply(
		util::StackAllocator &alloc, Event &ev, StatementId stmtId,
		StatementExecStatus status, const Response &response,
		const Request &request)
{
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);
		EventType eventType = ev.getType();
		if (isNewSQL(request)) {
			OptionSet optionSet(alloc);
			if (response.compositeIndexInfos_) {
				optionSet.set<Options::COMPOSITE_INDEX>(response.compositeIndexInfos_);
			}
			setReplyOption(optionSet, request);
			optionSet.encode(out);
			eventType = request.fixed_.stmtType_;
		}

		switch (eventType) {
		case CONNECT:
		case DISCONNECT:
		case LOGOUT:
		case DROP_CONTAINER:
		case CREATE_TRANSACTION_CONTEXT:
		case CLOSE_TRANSACTION_CONTEXT:
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
		case DELETE_INDEX:
		case CREATE_TRIGGER:
		case DELETE_TRIGGER:
		case FLUSH_LOG:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW_BY_ID:
		case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
		case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
		case CLOSE_RESULT_SET:
		case PUT_USER:
		case DROP_USER:
		case PUT_DATABASE:
		case DROP_DATABASE:
		case PUT_PRIVILEGE:
		case DROP_PRIVILEGE:
		case SQL_GET_CONTAINER:
		case SQL_RECV_SYNC_REQUEST:
			if (response.schemaMessage_) {
				sqlService_->encode(ev, *response.schemaMessage_);
			}
			break;
		case LOGIN:
			if (response.connectionOption_ != NULL) {
				ConnectionOption &connOption = *response.connectionOption_;
				encodeIntData<int8_t>(out, connOption.authMode_);
				encodeIntData<DatabaseId>(out, connOption.dbId_);
			}
			break;
		case SQL_ACK_GET_CONTAINER:
			break;
		case GET_PARTITION_ADDRESS:
			encodeBinaryData(
				out, response.binaryData_.data(), response.binaryData_.size());
			break;
		case GET_USERS: {
			out << static_cast<uint32_t>(response.userInfoList_.size());
			for (size_t i = 0; i < response.userInfoList_.size(); i++) {
				encodeStringData(out, response.userInfoList_[i]->userName_);
				out << response.userInfoList_[i]->property_;
				encodeBooleanData(out, response.userInfoList_[i]->withDigest_);
				encodeStringData(out, response.userInfoList_[i]->digest_);
			}
			if (request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>() >= StatementMessage::FEATURE_V4_5) {
				bool dummy = false;
				util::String dummyString(alloc);
				for (size_t i = 0; i < response.userInfoList_.size(); i++) {
					encodeBooleanData(out, dummy); 
					encodeStringData(out, dummyString);
				}
			}
		} break;
		case GET_DATABASES: {
			out << static_cast<uint32_t>(response.databaseInfoList_.size());
			for (size_t i = 0; i < response.databaseInfoList_.size(); i++) {
				encodeStringData(out, response.databaseInfoList_[i]->dbName_);
				out << response.databaseInfoList_[i]->property_;
				out << static_cast<uint32_t>(
					response.databaseInfoList_[i]
						->privilegeInfoList_.size());  
				for (size_t j = 0;
					 j <
					 response.databaseInfoList_[i]->privilegeInfoList_.size();
					 j++) {
					encodeStringData(
							out, response.databaseInfoList_[i]
									 ->privilegeInfoList_[j]
									 ->userName_);
					encodeStringData(
							out, response.databaseInfoList_[i]
									 ->privilegeInfoList_[j]
									 ->privilege_);
				}
			}
		} break;
		case GET_PARTITION_CONTAINER_NAMES: {
			out << response.containerNum_;
			out << static_cast<uint32_t>(response.containerNameList_.size());
			for (size_t i = 0; i < response.containerNameList_.size(); i++) {
				encodeContainerKey(out, response.containerNameList_[i]);
			}
		} break;

		case GET_CONTAINER_PROPERTIES:
			ev.addExtraMessage(
				response.binaryData_.data(), response.binaryData_.size());
			break;

		case GET_CONTAINER:
			encodeBooleanData(out, response.existFlag_);
			if (response.existFlag_) {
				out << response.schemaVersionId_;
				out << response.containerId_;
				encodeVarSizeBinaryData(out, response.binaryData2_.data(),
					response.binaryData2_.size());
				encodeBinaryData(out, response.binaryData_.data(),
					response.binaryData_.size());
				{
					int32_t containerAttribute =
						static_cast<int32_t>(response.containerAttribute_);
					out << containerAttribute;
				}
			}
			break;
		case PUT_LARGE_CONTAINER:
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER:
			out << response.schemaVersionId_;
			out << response.containerId_;
			encodeVarSizeBinaryData(out, response.binaryData2_.data(),
				response.binaryData2_.size());
			encodeBinaryData(
				out, response.binaryData_.data(), response.binaryData_.size());
			{
				int32_t containerAttribute =
					static_cast<int32_t>(response.containerAttribute_);
				out << containerAttribute;
			}
			break;

		case FETCH_RESULT_SET:
			assert(response.rs_ != NULL);
			{
				encodeBooleanData(out, response.rs_->isRelease());
				out << response.rs_->getVarStartPos();
				out << response.rs_->getFetchNum();
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case GET_ROW:
		case GET_TIME_SERIES_ROW_RELATED:
		case INTERPOLATE_TIME_SERIES_ROW:
			assert(response.rs_ != NULL);
			encodeBooleanData(out, response.existFlag_);
			if (response.existFlag_) {
				encodeBinaryData(out, response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				encodeBinaryData(out, response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case QUERY_TQL:
		case QUERY_COLLECTION_GEOMETRY_RELATED:
		case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION:
		case QUERY_TIME_SERIES_RANGE:
		case QUERY_TIME_SERIES_SAMPLING:
		assert(response.rs_ != NULL);
			if (isNewSQL(request)) {
				encodeEnumData<ResultType>(out, response.rs_->getResultType());
				out << response.rs_->getResultNum();
				out << response.rs_->getFixedOffsetSize();
					encodeBinaryData(out, response.rs_->getFixedStartData(), static_cast<size_t>(response.rs_->getFixedOffsetSize()));
					out << response.rs_->getVarOffsetSize();
					encodeBinaryData(out, response.rs_->getVarStartData(), static_cast<size_t>(response.rs_->getVarOffsetSize()));
					break;
			}
			switch (response.rs_->getResultType()) {
			case RESULT_ROWSET: {
				PartialQueryOption partialQueryOption(alloc);
				response.rs_->getQueryOption().encodePartialQueryOption(alloc, partialQueryOption);

				const size_t entryNumPos = out.base().position();
				int32_t entryNum = 0;
				out << entryNum;

				if (!partialQueryOption.empty()) {
					encodeEnumData<QueryResponseType>(out, PARTIAL_EXECUTION_STATE);

					const size_t entrySizePos = out.base().position();
					int32_t entrySize = 0;
					out << entrySize;

					encodeBooleanData(out, true);
					entrySize += sizeof(int8_t);

					int32_t partialEntryNum = static_cast<int32_t>(partialQueryOption.size());
					out << partialEntryNum;
					entrySize += sizeof(int32_t);

					PartialQueryOption::const_iterator itr;
					for (itr = partialQueryOption.begin(); itr != partialQueryOption.end(); itr++) {
						encodeIntData<int8_t>(out, itr->first);
						entrySize += sizeof(int8_t);

						int32_t partialEntrySize = itr->second->size();
						out << partialEntrySize;
						entrySize += sizeof(int32_t);
						encodeBinaryData(out, itr->second->data(), itr->second->size());
						entrySize += itr->second->size();
					}
					const size_t entryLastPos = out.base().position();
					out.base().position(entrySizePos);
					out << entrySize;
					out.base().position(entryLastPos);
					entryNum++;
				}

				entryNum += encodeDistributedResult(out, *response.rs_, NULL);

				{
					encodeEnumData<QueryResponseType>(out, ROW_SET);

					int32_t entrySize = sizeof(ResultSize) +
						response.rs_->getFixedOffsetSize() +
						response.rs_->getVarOffsetSize();
					out << entrySize;
					out << response.rs_->getFetchNum();
					ev.addExtraMessage(response.rs_->getFixedStartData(),
						response.rs_->getFixedOffsetSize());
					ev.addExtraMessage(response.rs_->getVarStartData(),
						response.rs_->getVarOffsetSize());
					entryNum++;
				}

				const size_t lastPos = out.base().position();
				out.base().position(entryNumPos);
				out << entryNum;
				out.base().position(lastPos);
			} break;
			case RESULT_EXPLAIN: {
				const size_t entryNumPos = out.base().position();
				int32_t entryNum = 0;
				out << entryNum;

				entryNum += encodeDistributedResult(out, *response.rs_, NULL);

				encodeEnumData<QueryResponseType>(out, QUERY_ANALYSIS);
				int32_t entrySize = sizeof(ResultSize) +
					response.rs_->getFixedOffsetSize() +
					response.rs_->getVarOffsetSize();
				out << entrySize;

				out << response.rs_->getResultNum();
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
				entryNum++;

				const size_t lastPos = out.base().position();
				out.base().position(entryNumPos);
				out << entryNum;
				out.base().position(lastPos);
			} break;
			case RESULT_AGGREGATE: {
				int32_t entryNum = 1;
				out << entryNum;

				encodeEnumData<QueryResponseType>(out, AGGREGATION);
				const util::XArray<uint8_t> *rowDataFixedPart =
					response.rs_->getRowDataFixedPartBuffer();
				int32_t entrySize = sizeof(ResultSize) +
					rowDataFixedPart->size();
				out << entrySize;

				out << response.rs_->getResultNum();
				encodeBinaryData(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());
			} break;
			case PARTIAL_RESULT_ROWSET: {
				int32_t entryNum = 2;
				out << entryNum;
				{
					encodeEnumData<QueryResponseType>(out, PARTIAL_FETCH_STATE);
					int32_t entrySize = sizeof(ResultSize) + sizeof(ResultSetId);
					out << entrySize;

					out << response.rs_->getResultNum();
					out << response.rs_->getId();
				}
				{
					encodeEnumData<QueryResponseType>(out, ROW_SET);
					int32_t entrySize = sizeof(ResultSize) +
						response.rs_->getFixedOffsetSize() +
						response.rs_->getVarOffsetSize();
					out << entrySize;

					out << response.rs_->getFetchNum();
					ev.addExtraMessage(response.rs_->getFixedStartData(),
						response.rs_->getFixedOffsetSize());
					ev.addExtraMessage(response.rs_->getVarStartData(),
						response.rs_->getVarOffsetSize());
				}
			} break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
					"(resultType=" << response.rs_->getResultType() << ")");
			}
			break;
		case UPDATE_CONTAINER_STATUS:
			out << response.currentStatus_;
			out << response.currentAffinity_;
			if (!response.indexInfo_.indexName_.empty()) {
				encodeIndexInfo(out, response.indexInfo_);
			}
			break;
		case CREATE_LARGE_INDEX:
			out << response.existIndex_;
			break;
		case DROP_LARGE_INDEX:
			break;
		case PUT_ROW:
		case PUT_MULTIPLE_ROWS:
		case REMOVE_ROW:
		case APPEND_TIME_SERIES_ROW:
			encodeBooleanData(out, response.existFlag_);
			break;

		case GET_MULTIPLE_ROWS:
			assert(response.rs_ != NULL);
			{
				out << response.last_;
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case AGGREGATE_TIME_SERIES:
			assert(response.rs_ != NULL);
			encodeBooleanData(out, response.existFlag_);
			if (response.existFlag_) {
				const util::XArray<uint8_t> *rowDataFixedPart =
					response.rs_->getRowDataFixedPartBuffer();
				encodeBinaryData(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());
			}
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << ev.getType() << ")");
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Sets the information about error reply to an client
*/
void StatementHandler::setErrorReply(
		Event &ev, StatementId stmtId,
		StatementExecStatus status, const std::exception &exception,
		const NodeDescriptor &nd) {
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);

		encodeException(out, exception, nd);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void StatementHandler::setReplyOption(
		OptionSet &optionSet, const Request &request) {
	optionSet.set<Options::UUID>(request.optional_.get<Options::UUID>());

	optionSet.set<Options::QUERY_ID>(
			request.optional_.get<Options::QUERY_ID>());
	optionSet.set<Options::FOR_SYNC>(
			request.optional_.get<Options::FOR_SYNC>());

	if (!request.optional_.get<Options::FOR_SYNC>()) {
		optionSet.set<Options::ACK_EVENT_TYPE>(
				request.optional_.get<Options::ACK_EVENT_TYPE>());
		optionSet.set<Options::SUB_CONTAINER_ID>(
				request.optional_.get<Options::SUB_CONTAINER_ID>());
		optionSet.set<Options::JOB_EXEC_ID>(
				request.optional_.get<Options::JOB_EXEC_ID>());
		optionSet.set<Options::JOB_VERSION>(
				request.optional_.get<Options::JOB_VERSION>());
	}
	else {
		optionSet.set<Options::ACK_EVENT_TYPE>(request.fixed_.stmtType_);
		if (request.optional_.get<Options::NOSQL_SYNC_ID>() !=
				UNDEF_NOSQL_REQUESTID) {
			optionSet.set<Options::NOSQL_SYNC_ID>(
					request.optional_.get<Options::NOSQL_SYNC_ID>());
		}
	}
	optionSet.set<StatementMessage::Options::FEATURE_VERSION>(MessageSchema::V4_1_VERSION);
}

void StatementHandler::setReplyOption(
		OptionSet &optionSet, const ReplicationContext &replContext) {
	ClientId ackClientId;
	replContext.copyAckClientId(ackClientId);

	optionSet.set<Options::UUID>(UUIDObject(ackClientId.uuid_));

	optionSet.set<Options::QUERY_ID>(replContext.getQueryId());
	optionSet.set<Options::FOR_SYNC>(replContext.getSyncFlag());
	if (replContext.getSyncFlag()) {
		if (replContext.getSyncId() != UNDEF_NOSQL_REQUESTID) {
			optionSet.set<Options::NOSQL_SYNC_ID>(
					replContext.getSyncId());
		}
	}
	
	optionSet.set<Options::REPLY_PID>(replContext.getReplyPId());

	optionSet.set<Options::SUB_CONTAINER_ID>(
			replContext.getSubContainerId());
	optionSet.set<Options::JOB_EXEC_ID>(replContext.getExecId());
	optionSet.set<Options::JOB_VERSION>(replContext.getJobVersionId());
}

void StatementHandler::setReplyOptionForContinue(
		OptionSet &optionSet, const Request &request) {
	optionSet.set<Options::UUID>(request.optional_.get<Options::UUID>());

	optionSet.set<Options::QUERY_ID>(
			request.optional_.get<Options::QUERY_ID>());
	optionSet.set<Options::FOR_SYNC>(
			request.optional_.get<Options::FOR_SYNC>());

	optionSet.set<Options::REPLY_PID>(
			request.optional_.get<Options::REPLY_PID>());
	optionSet.set<Options::REPLY_EVENT_TYPE>(
			request.optional_.get<Options::REPLY_EVENT_TYPE>());

	if (!request.optional_.get<Options::FOR_SYNC>()) {
		optionSet.set<Options::ACK_EVENT_TYPE>(
				request.optional_.get<Options::ACK_EVENT_TYPE>());
		optionSet.set<Options::SUB_CONTAINER_ID>(
				request.optional_.get<Options::SUB_CONTAINER_ID>());
		optionSet.set<Options::JOB_EXEC_ID>(
				request.optional_.get<Options::JOB_EXEC_ID>());
		optionSet.set<Options::JOB_VERSION>(
				request.optional_.get<Options::JOB_VERSION>());
	}
	else {
		if (request.optional_.get<Options::NOSQL_SYNC_ID>() !=
				UNDEF_NOSQL_REQUESTID) {
			optionSet.set<Options::NOSQL_SYNC_ID>(
					request.optional_.get<Options::NOSQL_SYNC_ID>());
		}
	}
}

void StatementHandler::setReplyOptionForContinue(
		OptionSet &optionSet, const ReplicationContext &replContext) {
	ClientId ackClientId;
	replContext.copyAckClientId(ackClientId);

	optionSet.set<Options::UUID>(UUIDObject(ackClientId.uuid_));

	optionSet.set<Options::QUERY_ID>(replContext.getQueryId());
	optionSet.set<Options::FOR_SYNC>(replContext.getSyncFlag());
	
	optionSet.set<Options::REPLY_PID>(replContext.getReplyPId());
	optionSet.set<Options::REPLY_EVENT_TYPE>(replContext.getReplyEventType());

	if (!replContext.getSyncFlag()) {
		optionSet.set<Options::SUB_CONTAINER_ID>(
				replContext.getSubContainerId());
		optionSet.set<Options::JOB_EXEC_ID>(replContext.getExecId());
		optionSet.set<Options::JOB_VERSION>(replContext.getJobVersionId());
	}
}

void StatementHandler::setSQLResonseInfo(
		ReplicationContext &replContext, const Request &request,
		const Response &response) {
	ClientId ackClientId;
	request.optional_.get<Options::UUID>().get(ackClientId.uuid_);
	
	replContext.setSQLResonseInfo(
			request.optional_.get<Options::REPLY_PID>(),
			request.optional_.get<Options::REPLY_EVENT_TYPE>(),
			request.optional_.get<Options::QUERY_ID>(),
			request.fixed_.clientId_,
			request.optional_.get<Options::FOR_SYNC>(),
			request.optional_.get<Options::JOB_EXEC_ID>(),
			request.optional_.get<Options::SUB_CONTAINER_ID>(),
			ackClientId,
			response.existIndex_,
			request.optional_.get<Options::NOSQL_SYNC_ID>(),
			request.optional_.get<Options::JOB_VERSION>()
			);
}

EventType StatementHandler::resolveReplyEventType(
		EventType stmtType, const OptionSet &optionSet) {
	const EventType replyEventType =
			optionSet.get<Options::REPLY_EVENT_TYPE>();
	if (replyEventType == UNDEF_EVENT_TYPE) {
		return stmtType;
	}
	else {
		return replyEventType;
	}
}

/*!
	@brief Checks if a user is authenticated
*/
void StatementHandler::checkAuthentication(
		const NodeDescriptor &ND, EventMonotonicTime emNow) {
	TEST_PRINT("checkAuthentication() S\n");

	const ConnectionOption &connOption = ND.getUserData<ConnectionOption>();

	if (!connOption.isAuthenticated_) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_REQUIRED, "");
	}
	if (!connOption.isAdminAndPublicDB_) {
		EventMonotonicTime interval =
			transactionService_->getManager()->getReauthenticationInterval();
		if (interval != 0) {
			TEST_PRINT1("ReauthenticationInterval=%d\n", interval);

			TEST_PRINT1("emNow=%d\n", emNow);
			TEST_PRINT1(
				"authenticationTime=%d\n", connOption.authenticationTime_);
			TEST_PRINT1("diff=%d\n", emNow - connOption.authenticationTime_);
			if (emNow - connOption.authenticationTime_ >
				interval * 1000) {  
				TEST_PRINT("ReAuth fired\n");
				TXN_THROW_DENY_ERROR(GS_ERROR_TXN_REAUTHENTICATION_FIRED, "");
			}
		}
	}
	TEST_PRINT("checkAuthentication() E\n");
}

/*!
	@brief Checks if immediate consistency is required
*/
void StatementHandler::checkConsistency(
		const NodeDescriptor &ND, bool requireImmediate) {
	const ConnectionOption &connOption = ND.getUserData<ConnectionOption>();
	if (requireImmediate && (connOption.isImmediateConsistency_ != true)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONSISTENCY_TYPE_UNMATCH,
			"immediate consistency required");
	}
}

/*!
	@brief Checks executable status
*/
void StatementHandler::checkExecutable(
		PartitionId pId,
		ClusterRole requiredClusterRole, PartitionRoleType requiredPartitionRole,
		PartitionStatus requiredPartitionStatus, PartitionTable *pt) {
#define TXN_THROW_PT_STATE_UNMATCH_ERROR(                           \
	pId,errorCode, requiredPartitionStatus, actualPartitionStatus,pt)       \
	TXN_THROW_DENY_ERROR(errorCode,                                 \
		"(pId=" << pId << ", required={" << partitionStatusToStr(requiredPartitionStatus) << "}" \
					<< ", actual="                                  \
					<< partitionStatusToStr(actualPartitionStatus) \
					<< ", revision=" << pt->getPartitionRevision(pId, 0) \
					<< ", LSN=" << pt->getLSN(pId) \
					<< ")")

	const PartitionTable::PartitionStatus actualPartitionStatus =
		partitionTable_->getPartitionStatus(pId);
	switch (actualPartitionStatus) {
	case PartitionTable::PT_ON:
		if (!(requiredPartitionStatus & PSTATE_ON)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_ON, pt);
		}
		break;
	case PartitionTable::PT_SYNC:
		if (!(requiredPartitionStatus & PSTATE_SYNC)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_SYNC, pt);
		}
		break;
	case PartitionTable::PT_OFF:
		if (!(requiredPartitionStatus & PSTATE_OFF)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_OFF, pt);
		}
		break;
	case PartitionTable::PT_STOP:
		if (!(requiredPartitionStatus & PSTATE_STOP)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_STOP, pt);
		}
		break;
	default:
		TXN_THROW_PT_STATE_UNMATCH_ERROR(pId, GS_ERROR_TXN_PARTITION_STATE_INVALID,
			requiredPartitionStatus, PSTATE_UNKNOWN, pt);
	}

	const NodeId myself = 0;
	const bool isOwner = partitionTable_->isOwner(pId, myself,
		PT_CURRENT_OB);
	const bool isBackup = partitionTable_->isBackup(pId, myself,
		PT_CURRENT_OB);
	const bool isCatchup = partitionTable_->isCatchup(pId, myself,
		PT_CURRENT_OB);

	assert((isOwner && isBackup) == false);
	assert((isOwner && isCatchup) == false);
	assert((isBackup && isCatchup) == false);
	PartitionRoleType actualPartitionRole = PROLE_UNKNOWN;
	actualPartitionRole |= isOwner ? PROLE_OWNER : PROLE_UNKNOWN;
	actualPartitionRole |= isBackup ? PROLE_BACKUP : PROLE_UNKNOWN;
	actualPartitionRole |= isCatchup ? PROLE_CATCHUP : PROLE_UNKNOWN;
	actualPartitionRole |=
		(!isOwner && !isBackup && !isCatchup) ? PROLE_NONE : PROLE_UNKNOWN;
	assert(actualPartitionRole != PROLE_UNKNOWN);
	if (!(actualPartitionRole & requiredPartitionRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
			"(pId=" << pId << ", required={" << partitionRoleTypeToStr(requiredPartitionRole) << "}"
						<< ", actual="
						<< partitionRoleTypeToStr(actualPartitionRole) 
						<< ", revision=" << pt->getPartitionRevision(pId, 0) << ", LSN=" << pt->getLSN(pId) << ")");
	}

	const bool isMaster = partitionTable_->isMaster();
	const bool isFollower = partitionTable_->isFollower();
	const bool isSubmaster = (!isMaster && !isFollower);
	assert((isMaster && isFollower) == false);
	ClusterRole actualClusterRole = CROLE_UNKNOWN;
	actualClusterRole |= isMaster ? CROLE_MASTER : CROLE_UNKNOWN;
	actualClusterRole |= isFollower ? CROLE_FOLLOWER : CROLE_UNKNOWN;
	actualClusterRole |= isSubmaster ? CROLE_SUBMASTER : CROLE_UNKNOWN;
	assert(actualClusterRole != CROLE_UNKNOWN);
	if (!(actualClusterRole & requiredClusterRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(pId=" << pId << ", required={" << clusterRoleToStr(requiredClusterRole) << "}"
						<< ", actual="
						<< clusterRoleToStr(actualClusterRole) << ", LSN=" << pt->getLSN(pId) << ")");
	}

#undef TXN_THROW_PT_STATE_UNMATCH_ERROR
}

void StatementHandler::checkExecutable(ClusterRole requiredClusterRole) {
#define TXN_THROW_PT_STATE_UNMATCH_ERROR(                           \
	errorCode, requiredPartitionStatus, actualPartitionStatus)       \
	TXN_THROW_DENY_ERROR(errorCode,                                 \
		"(required={" << partitionStatusToStr(requiredPartitionStatus) << "}" \
					<< ", actual="                                  \
					<< partitionStatusToStr(actualPartitionStatus) << ")")
	const bool isMaster = partitionTable_->isMaster();
	const bool isFollower = partitionTable_->isFollower();
	const bool isSubmaster = (!isMaster && !isFollower);
	assert((isMaster && isFollower) == false);
	ClusterRole actualClusterRole = CROLE_UNKNOWN;
	actualClusterRole |= isMaster ? CROLE_MASTER : CROLE_UNKNOWN;
	actualClusterRole |= isFollower ? CROLE_FOLLOWER : CROLE_UNKNOWN;
	actualClusterRole |= isSubmaster ? CROLE_SUBMASTER : CROLE_UNKNOWN;
	assert(actualClusterRole != CROLE_UNKNOWN);
	if (!(actualClusterRole & requiredClusterRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(required={" << clusterRoleToStr(requiredClusterRole) << "}"
						<< ", actual="
						<< clusterRoleToStr(actualClusterRole) << ")");
	}

#undef TXN_THROW_PT_STATE_UNMATCH_ERROR
}

void StatementHandler::checkExecutable(ClusterRole requiredClusterRole, PartitionTable *pt) {
	const bool isMaster = pt->isMaster();
	const bool isFollower = pt->isFollower();
	const bool isSubmaster = (!isMaster && !isFollower);
	assert((isMaster && isFollower) == false);
	ClusterRole actualClusterRole = CROLE_UNKNOWN;
	actualClusterRole |= isMaster ? CROLE_MASTER : CROLE_UNKNOWN;
	actualClusterRole |= isFollower ? CROLE_FOLLOWER : CROLE_UNKNOWN;
	actualClusterRole |= isSubmaster ? CROLE_SUBMASTER : CROLE_UNKNOWN;
	assert(actualClusterRole != CROLE_UNKNOWN);
	if (!(actualClusterRole & requiredClusterRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(required={" << clusterRoleToStr(requiredClusterRole) << "}"
						<< ", actual="
						<< clusterRoleToStr(actualClusterRole) << ")");
	}
}

/*!
	@brief Checks if transaction timeout
*/
void StatementHandler::checkTransactionTimeout(
		EventMonotonicTime now,
		EventMonotonicTime queuedTime, int32_t txnTimeoutIntervalSec,
		uint32_t queueingCount) {
	static_cast<void>(now);
	static_cast<void>(queuedTime);
	static_cast<void>(txnTimeoutIntervalSec);
	static_cast<void>(queueingCount);
}

/*!
	@brief Checks if a specified container exists
*/
void StatementHandler::checkContainerExistence(BaseContainer *container) {
	if (container == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_NOT_FOUND, "");
	}
}

/*!
	@brief Checks the schema version of container
*/
void StatementHandler::checkContainerSchemaVersion(
		BaseContainer *container, SchemaVersionId schemaVersionId) {
	checkContainerExistence(container);

	if (container->getVersionId() != schemaVersionId) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH,
			"(containerId=" << container->getContainerId()
							<< ", expectedVer=" << schemaVersionId
							<< ", actualVer=" << container->getVersionId()
							<< ")");
	}
}

/*!
	@brief Checks fetch option
*/
void StatementHandler::checkFetchOption(FetchOption fetchOption) {
	int64_t inputFetchSize = static_cast<int64_t>(fetchOption.size_);
	if (inputFetchSize < 1) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
			"Fetch Size = " << inputFetchSize
							<< ". Fetch size must be greater than 1");
	}
	checkSizeLimit(fetchOption.limit_);
}

/*!
	@brief Checks result size limit
*/
void StatementHandler::checkSizeLimit(ResultSize limit) {
	int64_t inputLimit = static_cast<int64_t>(limit);
	if (inputLimit < 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
			"Limit Size = " << inputLimit
							<< ". Limit size must be greater than 0");
	}
}

void StatementHandler::checkLoggedInDatabase(
		DatabaseId loginDbId, const char8_t *loginDbName,
		DatabaseId specifiedDbId, const char8_t *specifiedDbName
		, bool isNewSQL
		)  {
		if (isNewSQL) {
			return;
		}
	if (specifiedDbId != loginDbId ||
			strcmp(specifiedDbName, loginDbName) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_UNMATCH,
				"Specified db and logged in db are different. " <<
				"specified = " << specifiedDbName << "("<< specifiedDbId  << ")" <<
				", logged in = " << loginDbName << "(" << loginDbId << ")");
	}
}

void StatementHandler::checkQueryOption(
		const OptionSet &optionSet,
		const FetchOption &fetchOption, bool isPartial, bool isTQL) {

	checkFetchOption(fetchOption);

	const QueryContainerKey *distInfo = optionSet.get<Options::DIST_QUERY>();
	const bool distributed = (distInfo != NULL && distInfo->enabled_);

	if (optionSet.get<Options::FETCH_BYTES_SIZE>() < 1) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
				"Fetch byte size = " << optionSet.get<Options::FETCH_BYTES_SIZE>() <<
				". Fetch byte size must be greater than 1");
	}
	if (!isTQL && (distributed || isPartial)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"Partial query option is only applicable in TQL");
	}
	if (fetchOption.size_ != INT64_MAX && (distributed || isPartial)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"partial fetch is not applicable in distribute / partial execute mode");
	}
	if (optionSet.get<Options::FOR_UPDATE>() && (distributed || isPartial)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable in distribute / partial execute mode");
	}
}

void StatementHandler::createResultSetOption(
		const OptionSet &optionSet,
		const int32_t *fetchBytesSize, bool partial,
		const PartialQueryOption &partialOption,
		ResultSetOption &queryOption) {

	const QueryContainerKey *distInfo = optionSet.get<Options::DIST_QUERY>();
	const bool distributed = (distInfo != NULL && distInfo->enabled_);

	queryOption.set(
			(fetchBytesSize == NULL ?
					optionSet.get<Options::FETCH_BYTES_SIZE>() :
					*fetchBytesSize),
			distributed, partial, partialOption);

	SQLTableInfo *largeInfo = optionSet.get<Options::LARGE_INFO>();
	if (largeInfo != NULL) {
		largeInfo->setPartitionCount(
				partitionTable_->getPartitionNum());

		queryOption.setLargeInfo(largeInfo);
	}
}

void StatementHandler::checkLogVersion(uint16_t logVersion) {
	if (!LogManager::isAcceptableVersion(logVersion)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_REPLICATION_LOG_VERSION_NOT_ACCEPTABLE,
			"(receiveVersion=" << static_cast<uint32_t>(logVersion) <<
			", currentVersion=" << static_cast<uint32_t>(LogManager::getBaseVersion()) << ")");
	}
}

StatementMessage::FixedRequest::Source StatementHandler::getRequestSource(
		const Event &ev) {
	return FixedRequest::Source(ev.getPartitionId(), ev.getType());
}

void StatementHandler::decodeRequestCommonPart(
		EventByteInStream &in, Request &request,
		ConnectionOption &connOption) {
	try {
		request.fixed_.decode(in);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
	decodeRequestOptionPart(in, request, connOption);
}

/*!
	@brief Decodes option part of message
*/
void StatementHandler::decodeRequestOptionPart(
		EventByteInStream &in, Request &request,
		ConnectionOption &connOption) {
	try {
		request.optional_.set<Options::TXN_TIMEOUT_INTERVAL>(-1);

		decodeOptionPart(in, request.optional_);

		if (strlen(request.optional_.get<Options::DB_NAME>()) == 0) {
			bool useConnDBName = !isNewSQL(request);
			useConnDBName &= !connOption.dbName_.empty();
			request.optional_.set<Options::DB_NAME>(
					!useConnDBName ? GS_PUBLIC : connOption.dbName_.c_str());
		}

		if (isNewSQL(request)) {
			connOption.connected_ = true;
			connOption.isAuthenticated_ = true;
			connOption.isImmediateConsistency_ = true;
			connOption.requestType_ = Message::REQUEST_NEWSQL;
			connOption.userType_ = StatementMessage::USER_ADMIN;
			connOption.isAdminAndPublicDB_ = true;
			connOption.dbId_ = UNDEF_DBID;
			connOption.role_ = ALL;
		}

		if (request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() >= 0) {
			request.fixed_.cxtSrc_.txnTimeoutInterval_ =
					request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>();
		}
		else {
			request.fixed_.cxtSrc_.txnTimeoutInterval_ =
					connOption.txnTimeoutInterval_;
			request.optional_.set<Options::TXN_TIMEOUT_INTERVAL>(
					TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);
		}

		request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_ =
				request.optional_.get<Options::STORE_MEMORY_AGING_SWAP_RATE>();
		if (!TransactionContext::isStoreMemoryAgingSwapRateSpecified(
				request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_)) {
			request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_ =
					connOption.storeMemoryAgingSwapRate_;
		}

		request.fixed_.cxtSrc_.timeZone_ =
				request.optional_.get<Options::TIME_ZONE_OFFSET>();
		if (request.fixed_.cxtSrc_.timeZone_.isEmpty()) {
			request.fixed_.cxtSrc_.timeZone_ = connOption.timeZone_;
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void StatementHandler::decodeOptionPart(
		EventByteInStream &in, OptionSet &optionSet) {
	try {
		optionSet.decode(in);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

bool StatementHandler::isNewSQL(const Request &request) {
	return request.optional_.get<Options::REPLY_PID>() != UNDEF_PARTITIONID;
}

bool StatementHandler::isSkipReply(const Request &request) {
	return request.optional_.get<Options::FOR_SYNC>() &&
			request.optional_.get<Options::SUB_CONTAINER_ID>() == -1;
}

StatementHandler::CaseSensitivity StatementHandler::getCaseSensitivity(
		const Request &request) {
	return getCaseSensitivity(request.optional_);
}

StatementHandler::CaseSensitivity StatementHandler::getCaseSensitivity(
		const OptionSet &optionSet) {
	return optionSet.get<Options::SQL_CASE_SENSITIVITY>();
}

StatementMessage::CreateDropIndexMode
StatementHandler::getCreateDropIndexMode(const OptionSet &optionSet) {
	return optionSet.get<Options::CREATE_DROP_INDEX_MODE>();
}

bool StatementHandler::isDdlTransaction(const OptionSet &optionSet) {
	return optionSet.get<Options::DDL_TRANSACTION_MODE>();
}

void StatementHandler::assignIndexExtension(
		IndexInfo &indexInfo, const OptionSet &optionSet) {
	indexInfo.extensionName_.append(getExtensionName(optionSet));

	const ExtensionColumnTypes *types =
			optionSet.get<Options::EXTENSION_TYPE_LIST>();
	if (types != NULL) {
		indexInfo.extensionOptionSchema_.assign(
				types->columnTypeList_.begin(),
				types->columnTypeList_.end());
	}

	const ExtensionParams *params =
			optionSet.get<Options::EXTENSION_PARAMS>();
	if (params != NULL) {
		indexInfo.extensionOptionFixedPart_.assign(
				params->fixedPart_.begin(),
				params->fixedPart_.end());
		indexInfo.extensionOptionVarPart_.assign(
				params->varPart_.begin(),
				params->varPart_.end());;
	}
}

const char8_t* StatementHandler::getExtensionName(const OptionSet &optionSet) {
	return optionSet.get<Options::EXTENSION_NAME>();
}

/*!
	@brief Decodes index information
*/
void StatementHandler::decodeIndexInfo(
	util::ByteStream<util::ArrayInStream> &in, IndexInfo &indexInfo) {
	try {
		uint32_t size;
		in >> size;

		const size_t startPos = in.base().position();

		in >> indexInfo.indexName_;

		uint32_t columnIdCount;
		in >> columnIdCount;
		for (uint32_t i = 0; i < columnIdCount; i++) {
			uint32_t columnId;
			in >> columnId;
			indexInfo.columnIds_.push_back(columnId);
		}

		decodeEnumData<MapType>(in, indexInfo.mapType);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes trigger information
*/
void StatementHandler::decodeTriggerInfo(
	util::ByteStream<util::ArrayInStream> &in, TriggerInfo &triggerInfo) {
	try {
		decodeStringData<util::String>(in, triggerInfo.name_);

		in >> triggerInfo.type_;

		decodeStringData<util::String>(in, triggerInfo.uri_);

		in >> triggerInfo.operation_;

		int32_t columnCount;
		in >> columnCount;

		std::set<ColumnId> columnIdSet;
		for (int32_t i = 0; i < columnCount; i++) {
			ColumnId columnId;
			in >> columnId;
			std::pair<std::set<ColumnId>::iterator, bool> p =
				columnIdSet.insert(columnId);
			if (p.second) {
				triggerInfo.columnIds_.push_back(columnId);
			}
		}

		decodeStringData<util::String>(in, triggerInfo.jmsProviderTypeName_);

		decodeStringData<util::String>(in, triggerInfo.jmsDestinationTypeName_);

		decodeStringData<util::String>(in, triggerInfo.jmsDestinationName_);

		decodeStringData<util::String>(in, triggerInfo.jmsUser_);

		decodeStringData<util::String>(in, triggerInfo.jmsPassword_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes multiple row data
*/
void StatementHandler::decodeMultipleRowData(
	util::ByteStream<util::ArrayInStream> &in, uint64_t &numRow,
	RowData &rowData) {
	try {
		in >> numRow;
		decodeBinaryData(in, rowData, true);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes fetch option
*/
void StatementHandler::decodeFetchOption(
	util::ByteStream<util::ArrayInStream> &in, FetchOption &fetchOption) {
	try {
		in >> fetchOption.limit_;
		in >> fetchOption.size_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes TQL partial execute option
*/
void StatementHandler::decodePartialQueryOption(
	util::ByteStream<util::ArrayInStream> &in, util::StackAllocator &alloc,
	bool &isPartial, PartialQueryOption &partialQueryOption) {
	try {
		decodeBooleanData(in, isPartial);
		int32_t entryNum;
		in >> entryNum;
		if (isPartial) {
			for (int32_t i = 0; i < entryNum; i++) {
				int8_t entryType;
				in >> entryType;

				util::XArray<uint8_t> *entryBody = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeBinaryData(in, *entryBody, false);
				partialQueryOption.insert(std::make_pair(entryType, entryBody));
			}
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void StatementHandler::decodeGeometryRelatedQuery(
	util::ByteStream<util::ArrayInStream> &in, GeometryQuery &query) {
	try {
		in >> query.columnId_;
		decodeVarSizeBinaryData(in, query.intersection_);
		decodeEnumData<GeometryOperator>(in, query.operator_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void StatementHandler::decodeGeometryWithExclusionQuery(
	util::ByteStream<util::ArrayInStream> &in, GeometryQuery &query) {
	try {
		in >> query.columnId_;
		decodeVarSizeBinaryData(in, query.intersection_);
		decodeVarSizeBinaryData(in, query.disjoint_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes TimeRelatedCondition
*/
void StatementHandler::decodeTimeRelatedConditon(
	util::ByteStream<util::ArrayInStream> &in,
	TimeRelatedCondition &condition) {
	try {
		in >> condition.rowKey_;
		decodeEnumData<TimeOperator>(in, condition.operator_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes InterpolateCondition
*/
void StatementHandler::decodeInterpolateConditon(
	util::ByteStream<util::ArrayInStream> &in,
	InterpolateCondition &condition) {
	try {
		in >> condition.rowKey_;
		in >> condition.columnId_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes AggregateQuery
*/
void StatementHandler::decodeAggregateQuery(
	util::ByteStream<util::ArrayInStream> &in, AggregateQuery &query) {
	try {
		in >> query.start_;
		in >> query.end_;
		in >> query.columnId_;
		decodeEnumData<AggregationType>(in, query.aggregationType_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes RangeQuery
*/
void StatementHandler::decodeRangeQuery(
	util::ByteStream<util::ArrayInStream> &in, RangeQuery &query) {
	try {
		in >> query.start_;
		in >> query.end_;
		decodeEnumData<OutputOrder>(in, query.order_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes SamplingQuery
*/
void StatementHandler::decodeSamplingQuery(
	util::ByteStream<util::ArrayInStream> &in, SamplingQuery &query) {
	try {
		in >> query.start_;
		in >> query.end_;

		int32_t columnCount;
		in >> columnCount;
		for (int32_t i = 0; i < columnCount; i++) {
			ColumnId columnId;
			in >> columnId;
			query.interpolatedColumnIdList_.push_back(columnId);
		}

		in >> query.interval_;
		decodeEnumData<TimeUnit>(in, query.timeUnit_);
		decodeEnumData<InterpolationMode>(in, query.mode_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes ContainerConditionData
*/
void StatementHandler::decodeContainerConditionData(
	util::ByteStream<util::ArrayInStream> &in,
	DataStore::ContainerCondition &containerCondition) {
	try {
		if (in.base().remaining() != 0) {
			int32_t num;
			in >> num;

			for (int32_t i = 0; i < num; i++) {
				int32_t containerAttribute;
				in >> containerAttribute;
				containerCondition.insertAttribute(
					static_cast<ContainerAttribute>(containerAttribute));
			}
		}
		else {
			containerCondition.insertAttribute(
				static_cast<ContainerAttribute>(CONTAINER_ATTR_SINGLE));  
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void StatementHandler::decodeResultSetId(
	util::ByteStream<util::ArrayInStream> &in, ResultSetId &resultSetId) {
	try {
		if (in.base().remaining() != 0) {
			in >> resultSetId;
		}
		else {
			resultSetId = UNDEF_RESULTSETID;
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes boolean data
*/
void StatementHandler::decodeBooleanData(
	util::ByteStream<util::ArrayInStream> &in, bool &boolData) {
	try {
		int8_t tmp;
		in >> tmp;
		boolData = tmp ? true : false;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes binary data
*/
void StatementHandler::decodeBinaryData(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<uint8_t> &binaryData, bool readAll) {
	try {
		uint32_t size;
		if (readAll) {
			size = static_cast<uint32_t>(in.base().remaining());
		}
		else {
			in >> size;
		}
		binaryData.resize(size);
		in >> std::make_pair(binaryData.data(), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes VarSize-typed binary data
*/
void StatementHandler::decodeVarSizeBinaryData(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<uint8_t> &binaryData) {
	try {
		uint32_t size = ValueProcessor::getVarSize(in);
		binaryData.resize(size);
		in >> std::make_pair(binaryData.data(), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes UUID
*/
void StatementHandler::decodeUUID(util::ByteStream<util::ArrayInStream> &in,
	uint8_t *uuid_, size_t uuidSize) {
	try {
		assert(uuid_ != NULL);
		in.readAll(uuid_, uuidSize);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes ReplicationAck
*/
void StatementHandler::decodeReplicationAck(
	util::ByteStream<util::ArrayInStream> &in, ReplicationAck &ack) {


	try {
		in >> ack.clusterVer_;
		in >> ack.replStmtType_;
		in >> ack.replStmtId_;
		in >> ack.clientId_.sessionId_;
		decodeUUID(in, ack.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		in >> ack.replId_;
		in >> ack.replMode_;
		in >> ack.taskStatus_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes common part of message
*/
EventByteOutStream StatementHandler::encodeCommonPart(
	Event &ev, StatementId stmtId, StatementExecStatus status) {
	try {
		ev.setPartitionIdSpecified(false);

		EventByteOutStream out = ev.getOutStream();

		out << stmtId;
		out << status;

		return out;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes index information
*/
void StatementHandler::encodeIndexInfo(
	EventByteOutStream &out, const IndexInfo &indexInfo) {
	try {
		const size_t sizePos = out.base().position();

		uint32_t size = 0;
		out << size;

		out << indexInfo.indexName_;

		out << static_cast<uint32_t>(indexInfo.columnIds_.size());
		for (size_t i = 0; i < indexInfo.columnIds_.size(); i++) {
			out << indexInfo.columnIds_[i];
		}

		encodeEnumData<MapType>(out, indexInfo.mapType);

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void StatementHandler::encodeIndexInfo(
	util::XArrayByteOutStream &out, const IndexInfo &indexInfo) {
	try {
		const size_t sizePos = out.base().position();

		uint32_t size = 0;
		out << size;

		out << indexInfo.indexName_;

		out << static_cast<uint32_t>(indexInfo.columnIds_.size());
		for (size_t i = 0; i < indexInfo.columnIds_.size(); i++) {
			out << indexInfo.columnIds_[i];
		}
		const uint8_t tmp = static_cast<uint8_t>(indexInfo.mapType);
		out << tmp;
		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}
/*!
	@brief Encodes const string data
*/
void StatementHandler::encodeStringData(
	EventByteOutStream &out, const char *str) {
	try {
		size_t size = strlen(str);
		out << static_cast<uint32_t>(size);
		out << std::pair<const uint8_t *, size_t>(
			reinterpret_cast<const uint8_t *>(str), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes boolean data
*/
void StatementHandler::encodeBooleanData(
	EventByteOutStream &out, bool boolData) {
	try {
		const uint8_t tmp = boolData ? 1 : 0;
		out << tmp;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes binary data
*/
void StatementHandler::encodeBinaryData(
	EventByteOutStream &out, const uint8_t *data, size_t size) {
	try {
		out << std::pair<const uint8_t *, size_t>(data, size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes UUID
*/
void StatementHandler::encodeUUID(
	EventByteOutStream &out, const uint8_t *uuid, size_t uuidSize) {
	try {
		assert(uuid != NULL);
		out << std::pair<const uint8_t *, size_t>(uuid, uuidSize);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void StatementHandler::encodeVarSizeBinaryData(
	EventByteOutStream &out, const uint8_t *data, size_t size) {
	try {
		switch (ValueProcessor::getEncodedVarSize(static_cast<uint32_t>(size))) {
			case 1:
				out << ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(size));
				break;
			case 4:
				out << ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(size));
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED, "");
		}
		out << std::pair<const uint8_t *, size_t>(data, size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void StatementHandler::encodeContainerKey(
	EventByteOutStream &out, const FullContainerKey &containerKey) {
	try {
		const void *body;
		size_t size;
		containerKey.toBinary(body, size);
		encodeVarSizeBinaryData(out, static_cast<const uint8_t*>(body), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes exception infomation
*/
template <typename ByteOutStream>
void StatementHandler::encodeException(
	ByteOutStream &out, const std::exception &exception, bool detailsHidden,
	const ExceptionParameterList *paramList) {
	const util::Exception e = GS_EXCEPTION_CONVERT(exception, "");

	const uint32_t maxDepth =
		(detailsHidden ? 1 : static_cast<uint32_t>(e.getMaxDepth() + 1));

	out << maxDepth;

	for (uint32_t depth = 0; depth < maxDepth; ++depth) {
		out << e.getErrorCode(depth);

		util::NormalOStringStream oss;

		e.formatMessage(oss, depth);
		out << oss.str().c_str();
		oss.str("");

		e.formatTypeName(oss, depth);
		out << (detailsHidden ? "" : oss.str().c_str());
		oss.str("");

		e.formatFileName(oss, depth);
		out << (detailsHidden ? "" : oss.str().c_str());
		oss.str("");

		e.formatFunctionName(oss, depth);
		out << (detailsHidden ? "" : oss.str().c_str());
		oss.str("");

		out << static_cast<int32_t>(
			detailsHidden ? -1 : e.getLineNumber(depth));
	}

	{
		const char8_t *errorName = e.getNamedErrorCode().getName();
		out << (errorName == NULL ? "" : errorName);
	}

	{
		const int32_t paramCount = (paramList == NULL ? 0 : paramList->size());
		out << paramCount;

		for (int32_t i = 0; i < paramCount; i++) {
			const ExceptionParameterEntry &entry = (*paramList)[i];
			out << entry.first;
			out << entry.second;
		}
	}
}

/*!
	@brief Encodes exception information
*/
template <typename ByteOutStream>
void StatementHandler::encodeException(ByteOutStream &out,
	const std::exception &exception, const NodeDescriptor &nd) {
	bool detailsHidden;
	if (!nd.isEmpty() && nd.getType() == NodeDescriptor::ND_TYPE_CLIENT) {
		detailsHidden = TXN_DETAIL_EXCEPTION_HIDDEN;
	}
	else {
		detailsHidden = false;
	}

	encodeException(out, exception, detailsHidden);
}

/*!
	@brief Encodes ReplicationAck
*/
void StatementHandler::encodeReplicationAckPart(EventByteOutStream &out,
	ClusterVersionId clusterVer, int32_t replMode, const ClientId &clientId,
	ReplicationId replId, EventType replStmtType, StatementId replStmtId,
	int32_t taskStatus) {


	try {
		out << clusterVer;
		out << replStmtType;
		out << replStmtId;
		out << clientId.sessionId_;
		encodeUUID(out, clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << replId;
		out << replMode;
		out << taskStatus;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

int32_t StatementHandler::encodeDistributedResult(
		EventByteOutStream &out, const ResultSet &rs, int64_t *encodedSize) {
	int32_t entryCount = 0;
	const size_t startPos = out.base().position();

	const util::Vector<int64_t> *distributedTarget =
			static_cast<const ResultSet&>(rs).getDistributedTarget();
	if (distributedTarget != NULL) {
		encodeEnumData<QueryResponseType>(out, DIST_TARGET);

		const size_t entryHeadPos = out.base().position();
		out << static_cast<int32_t>(0);
		const size_t entryBodyPos = out.base().position();

		bool uncovered;
		bool reduced;
		rs.getDistributedTargetStatus(uncovered, reduced);
		encodeBooleanData(out, uncovered);
		encodeBooleanData(out, reduced);

		out << static_cast<int32_t>(distributedTarget->size());
		for (util::Vector<int64_t>::const_iterator it = distributedTarget->begin();
				it != distributedTarget->end(); ++it) {
			out << *it;
		}

		const size_t entryEndPos = out.base().position();
		out.base().position(entryHeadPos);
		out << static_cast<int32_t>(entryEndPos - entryBodyPos);
		out.base().position(entryEndPos);

		entryCount++;
	}

	const ResultSetOption &quetyOption = rs.getQueryOption();
	if (quetyOption.isDistribute() && quetyOption.existLimitOffset()) {
		encodeEnumData<QueryResponseType>(out, DIST_LIMIT);
		int32_t entrySize = sizeof(int64_t) + sizeof(int64_t);
		out << entrySize;
		encodeLongData<int64_t>(out, quetyOption.getDistLimit());
		encodeLongData<int64_t>(out, quetyOption.getDistOffset());
		entryCount++;
	}

	if (encodedSize != NULL) {
		*encodedSize += static_cast<int64_t>(out.base().position() - startPos);
	}

	return entryCount;
}

/*!
	@brief Checks if UPDATE statement is requested
*/
bool StatementHandler::isUpdateStatement(EventType stmtType) {
	switch (stmtType) {
	case COMMIT_TRANSACTION:
	case ABORT_TRANSACTION:
	case APPEND_TIME_SERIES_ROW:
	case PUT_MULTIPLE_CONTAINER_ROWS:
	case PUT_ROW:
	case PUT_MULTIPLE_ROWS:
	case UPDATE_ROW_BY_ID:
	case REMOVE_ROW:
	case REMOVE_ROW_BY_ID:
	case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
	case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
		return true;
		break;
	default:
		return false;
	}
}

/*!
	@brief Replies success message
*/
void StatementHandler::replySuccess(
		EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const Request &request,
		const Response &response, bool ackWait) {

	util::StackAllocator::Scope scope(alloc);

	if (!isNewSQL(request)) {
		if (!ackWait) {
			Event ev(ec, stmtType, request.fixed_.pId_);
			setSuccessReply(
					alloc, ev, request.fixed_.cxtSrc_.stmtId_,
					status, response, request);

			if (ev.getExtraMessageCount() > 0) {
				EventEngine::EventRequestOption option;
				option.timeoutMillis_ = 0;
				ec.getEngine().send(ev, ND, &option);
			}
			else {
				ec.getEngine().send(ev, ND);
			}
		}
	}
	else {
			if (isSkipReply(request)) {
				return;
			}
		if (!ackWait) {
			const EventType replyEventType =
					resolveReplyEventType(stmtType, request.optional_);
			const PartitionId replyPId =
					request.optional_.get<Options::REPLY_PID>();

			Event ev(ec, replyEventType, replyPId);
			setSuccessReply(
					alloc, ev, request.fixed_.cxtSrc_.stmtId_,
					status, response, request);
			ev.setPartitionId(replyPId);

			EventEngine *replyEE;
			replyEE = sqlService_->getEE();
			NodeDescriptor replyNd = replyEE->getServerND(ND.getId());
			if (ev.getExtraMessageCount() > 0) {
				EventEngine::EventRequestOption option;
				option.timeoutMillis_ = 0;
				replyEE->send(ev, replyNd, &option);
			}
			else {
				replyEE->send(ev, replyNd);
			}
		}
	}
}

/*!
	@brief Replies success message
*/
void StatementHandler::replySuccess(
		EventContext &ec,
		util::StackAllocator &alloc, StatementExecStatus status,
		const ReplicationContext &replContext) {
#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)             \
	"(nd=" << replContext.getConnectionND()                    \
		   << ", pId=" << replContext.getPartitionId()         \
		   << ", eventType=" << replContext.getStatementType() \
		   << ", stmtId=" << replContext.getStatementId()      \
		   << ", clientId=" << replContext.getClientId()       \
		   << ", containerId=" << replContext.getContainerId() << ")"

	util::StackAllocator::Scope scope(alloc);
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	if (replContext.getConnectionND().isEmpty()) {
		return;
	}

	try {
		Response response(alloc);

		switch (replContext.getStatementType()) {
		case PUT_LARGE_CONTAINER:
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER: {
			response.schemaVersionId_ = replContext.getContainerSchemaVersionId();
			response.containerId_ = replContext.getContainerId();

			const std::vector<ReplicationContext::BinaryData*> &binaryDatas = replContext.getBinaryDatas();
			response.binaryData2_.assign(binaryDatas[0]->begin(), binaryDatas[0]->end());
			response.binaryData_.assign(binaryDatas[1]->begin(), binaryDatas[1]->end());
		} break;

		case DROP_CONTAINER:
		case CLOSE_TRANSACTION_CONTEXT:
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
		case DELETE_INDEX:
		case CREATE_TRIGGER:
		case DELETE_TRIGGER:
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW_BY_ID:
		case PUT_USER:
		case DROP_USER:
		case PUT_DATABASE:
		case DROP_DATABASE:
		case PUT_PRIVILEGE:
		case DROP_PRIVILEGE:
			break;

		case PUT_ROW:
		case REMOVE_ROW:
		case PUT_MULTIPLE_ROWS:
		case APPEND_TIME_SERIES_ROW:
			{ response.existFlag_ = replContext.getExistFlag(); }
			break;
		case DROP_LARGE_INDEX:
			{ response.existFlag_ = replContext.getExistFlag(); }
			break;
		case CREATE_LARGE_INDEX:
			{
				response.existIndex_ = replContext.getExistsIndex();
			}
			break;
		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
			break;
		case UPDATE_CONTAINER_STATUS:
			replContext.setContainerStatus(response.currentStatus_, response.currentAffinity_, response.indexInfo_);
			break;
		case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << replContext.getStatementType() << ")");
		}

		if (!replContext.isNewSQL()) {
			Event ev(
					ec, replContext.getStatementType(),
					replContext.getPartitionId());
			const Request request(alloc, getRequestSource(ev));

			setSuccessReply(
					alloc, ev, replContext.getOriginalStatementId(),
					status, response, request);

			ec.getEngine().send(ev, replContext.getConnectionND());

			GS_TRACE_INFO(
					REPLICATION, GS_TRACE_TXN_REPLY_CLIENT,
					TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
		}
		else {
			if (!replContext.getSyncFlag() && replContext.getSubContainerId() == -1) {
				return;
			}
			Event ev(
					ec, replContext.getReplyEventType(),
					replContext.getReplyPId());

			const FixedRequest::Source source(ev.getPartitionId(), replContext.getStatementType());
			Request request(alloc, source);
			setReplyOption(request.optional_, replContext);

			setSuccessReply(
					alloc, ev, replContext.getOriginalStatementId(),
					status, response, request);
			ev.setPartitionId(replContext.getReplyPId());

			EventEngine *replyEE;
			replyEE = sqlService_->getEE();
			NodeDescriptor replyNd =
				replyEE->getServerND(replContext.getReplyNodeId());
			if (ev.getExtraMessageCount() > 0) {
				EventEngine::EventRequestOption option;
				option.timeoutMillis_ = 0;
				replyEE->send(ev, replyNd, &option);
			}
			else {
				replyEE->send(ev, replyNd);
			}
		}
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
	@brief Replies error message
*/
void StatementHandler::replyError(
		EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType, StatementExecStatus status,
		const Request &request, const std::exception &e) {
	util::StackAllocator::Scope scope(alloc);

	StatementId stmtId = request.fixed_.cxtSrc_.stmtId_;
	if (!isNewSQL(request)) {
		if (stmtType == CONTINUE_ALTER_CONTAINER) {
			stmtType = PUT_CONTAINER;
			stmtId = request.fixed_.startStmtId_;
		}
		else if (stmtType == CONTINUE_CREATE_INDEX) {
			stmtType = CREATE_INDEX;
			stmtId = request.fixed_.startStmtId_;
		}
		Event ev(ec, stmtType, request.fixed_.pId_);
		setErrorReply(ev, stmtId, status, e, ND);

		if (!ND.isEmpty()) {
			ec.getEngine().send(ev, ND);
		}
	}
	else {
		if (isSkipReply(request)) {
			return;
		}

		EventType replyEventType =
				resolveReplyEventType(stmtType, request.optional_);
		const PartitionId replyPId = request.optional_.get<Options::REPLY_PID>();

		if (replyEventType == CONTINUE_ALTER_CONTAINER) {
			replyEventType = PUT_CONTAINER;
			stmtId = request.fixed_.startStmtId_;
		}
		else if (replyEventType == CONTINUE_CREATE_INDEX) {
			replyEventType = CREATE_INDEX;
			stmtId = request.fixed_.startStmtId_;
		}
		Event ev(ec, replyEventType, replyPId);
		setErrorReply(ev, stmtId, status, e, ND);
		ev.setPartitionId(replyPId);

		EventByteOutStream out = ev.getOutStream();
		OptionSet optionSet(alloc);
		setReplyOption(optionSet, request);
		optionSet.encode(out);

		EventEngine *replyEE;
		replyEE = sqlService_->getEE();
		NodeDescriptor replyNd = replyEE->getServerND(ND.getId());
		if (!replyNd.isEmpty()) {
			replyEE->send(ev, replyNd);
		}
	}
}



/*!
	@brief Handles error
*/
void StatementHandler::handleError(
		EventContext &ec, util::StackAllocator &alloc, Event &ev,
		const Request &request, std::exception &e) {
	util::StackAllocator::Scope scope(alloc);
	ErrorMessage errorMessage(e, "", ev, request);

	try {
		try {
			throw;
		}
		catch (StatementAlreadyExecutedException&) {
			UTIL_TRACE_EXCEPTION_INFO(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Statement already executed"));
			Response response(alloc);
			response.existFlag_ = false;
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
		}
		catch (EncodeDecodeException&) {

			UTIL_TRACE_EXCEPTION(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
								"Failed to encode or decode message"));

			replyError(
					ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_ERROR, request, e);
		}
		catch (LockConflictException &e2) {
			UTIL_TRACE_EXCEPTION_INFO(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Lock conflicted"));
			try {
				TransactionContext &txn = transactionManager_->get(
					alloc, ev.getPartitionId(), request.fixed_.clientId_);

				util::XArray<const util::XArray<uint8_t> *> logRecordList(
					alloc);

				util::XArray<ClientId> closedResourceIds(alloc);
				if (request.fixed_.cxtSrc_.txnMode_ ==
						TransactionManager::NO_AUTO_COMMIT_BEGIN) {
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						logRecordList.push_back(log);
						if (ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
							closedResourceIds.push_back(request.fixed_.clientId_);
						} else
						if (ev.getType() == PUT_MULTIPLE_ROWS) {
							assert(request.fixed_.startStmtId_ != UNDEF_STATEMENTID);
							transactionManager_->update(
								txn, request.fixed_.startStmtId_);
						}
					}
				}

				Response response(alloc);
				executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
					txn, ABORT_TRANSACTION, request.fixed_.cxtSrc_.stmtId_,
					TransactionManager::REPLICATION_ASYNC,
					closedResourceIds.data(), closedResourceIds.size(),
					logRecordList.data(), logRecordList.size(),
					response);

				if (request.fixed_.cxtSrc_.getMode_ == TransactionManager::CREATE ||
					ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
					transactionManager_->remove(
						request.fixed_.pId_, request.fixed_.clientId_);
				}
			}
			catch (ContextNotFoundException&) {
				UTIL_TRACE_EXCEPTION_INFO(
						TRANSACTION_SERVICE, e, errorMessage.withDescription(
								"Context not found"));
			}

			const LockConflictStatus lockConflictStatus(getLockConflictStatus(
					ev, ec.getHandlerStartMonotonicTime(), request));
			try {
				checkLockConflictStatus(lockConflictStatus, e2);
			}
			catch (LockConflictException&) {
				assert(false);
				throw;
			}
			catch (...) {
				handleError(ec, alloc, ev, request, e);
				return;
			}
			retryLockConflictedRequest(ec, ev, lockConflictStatus);
		}
		catch (DenyException&) {
			UTIL_TRACE_EXCEPTION(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Request denied"));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_DENY, request, e);
		}
		catch (ContextNotFoundException&) {
			UTIL_TRACE_EXCEPTION_WARNING(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Context not found"));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_ERROR, request, e);
		}
		catch (std::exception&) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				const util::Exception utilException =
						GS_EXCEPTION_CONVERT(e, "");

				Response response(alloc);

				if (ev.getType() == ABORT_TRANSACTION &&
					utilException.getErrorCode(utilException.getMaxDepth()) ==
						GS_ERROR_TM_TRANSACTION_NOT_FOUND) {
					UTIL_TRACE_EXCEPTION_WARNING(
						TRANSACTION_SERVICE, e, errorMessage.withDescription(
								"Transaction is already timed out"));
					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
				}
				else {
					UTIL_TRACE_EXCEPTION(
							TRANSACTION_SERVICE, e, errorMessage.withDescription(
									"User error"));
					try {
						TransactionContext &txn = transactionManager_->get(
							alloc, ev.getPartitionId(), request.fixed_.clientId_);

						util::XArray<uint8_t> *log =
							ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						if (abortOnError(txn, *log)) {
							util::XArray<ClientId> closedResourceIds(alloc);
							if (ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
								closedResourceIds.push_back(request.fixed_.clientId_);
							}
							util::XArray<const util::XArray<uint8_t> *>
								logRecordList(alloc);
							logRecordList.push_back(log);

							executeReplication(request, ec, alloc,
								NodeDescriptor::EMPTY_ND, txn,
								ABORT_TRANSACTION, request.fixed_.cxtSrc_.stmtId_,
								TransactionManager::REPLICATION_ASYNC,
								closedResourceIds.data(), closedResourceIds.size(),
								logRecordList.data(), logRecordList.size(),
								response);
						}
						if (ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
							transactionManager_->remove(
								request.fixed_.pId_, request.fixed_.clientId_);
						}
					}
					catch (ContextNotFoundException&) {
						UTIL_TRACE_EXCEPTION_INFO(
								TRANSACTION_SERVICE, e, errorMessage.withDescription(
										"Context not found"));
					}

					replyError(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_ERROR, request, e);
				}
			}
		}
	}
	catch (std::exception&) {
		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
						"System error"));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setSystemError(&e);
	}
}

/*!
	@brief Aborts on error
*/
bool StatementHandler::abortOnError(
	TransactionContext &txn, util::XArray<uint8_t> &log) {
	log.clear();

	if (txn.isActive()) {
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		BaseContainer *container = NULL;

		try {
			container = dataStore_->getContainer(
				txn, txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER);
		}
		catch (UserException &e) {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Container not found. (pId="
					<< txn.getPartitionId()
					<< ", clientId=" << txn.getClientId()
					<< ", containerId=" << txn.getContainerId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		StackAllocAutoPtr<BaseContainer> containerAutoPtr(
			txn.getDefaultAllocator(), container);

		if (containerAutoPtr.get() != NULL) {

			const LogSequentialNumber lsn = logManager_->putAbortTransactionLog(
				log, txn.getPartitionId(), txn.getClientId(), txn.getId(),
				txn.getContainerId(), txn.getLastStatementId());
			partitionTable_->setLSN(txn.getPartitionId(), lsn);

			transactionManager_->abort(txn, *containerAutoPtr.get());

		}
		else {
			transactionManager_->remove(txn);
		}
	}

	return !log.empty();
}

void StatementHandler::checkLockConflictStatus(
		const LockConflictStatus &status, LockConflictException &e) {
	const EventMonotonicTime elapsedMillis =
			status.emNow_ - status.initialConflictMillis_;
	const EventMonotonicTime timeoutMillis =
			static_cast<EventMonotonicTime>(status.txnTimeoutInterval_) * 1000;

	if (elapsedMillis >= timeoutMillis) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"Transaction timed out by lock conflict (" <<
				"elapsedMillis=" << elapsedMillis <<
				", timeoutMillis=" << timeoutMillis << ")"));
	}
}

void StatementHandler::retryLockConflictedRequest(
		EventContext &ec, Event &ev, const LockConflictStatus &status) {
	updateLockConflictStatus(ec, ev, status);
	ec.getEngine().addPending(
			ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
}

StatementHandler::LockConflictStatus StatementHandler::getLockConflictStatus(
		const Event &ev, EventMonotonicTime emNow, const Request &request) {
	return getLockConflictStatus(
			ev, emNow, request.fixed_.cxtSrc_, request.optional_);
}

StatementHandler::LockConflictStatus StatementHandler::getLockConflictStatus(
		const Event &ev, EventMonotonicTime emNow,
		const TransactionManager::ContextSource &cxtSrc,
		const OptionSet &optionSet) {
	EventMonotonicTime initialConflictMillis;
	if (ev.getQueueingCount() <= 1) {
		initialConflictMillis = emNow;
	}
	else {
		initialConflictMillis =
				optionSet.get<Options::LOCK_CONFLICT_START_TIME>();
	}

	return LockConflictStatus(
			cxtSrc.txnTimeoutInterval_, initialConflictMillis, emNow);
}

void StatementHandler::updateLockConflictStatus(
		EventContext &ec, Event &ev, const LockConflictStatus &status) {
	if (ev.getQueueingCount() <= 1) {
		assert(ev.getQueueingCount() == 1);
		ev.incrementQueueingCount();
	}

	updateRequestOption<Options::LOCK_CONFLICT_START_TIME>(
			ec.getAllocator(), ev, status.initialConflictMillis_);
}

void StatementHandler::ConnectionOption::checkPrivilegeForOperator() {
	if (dbId_ != GS_PUBLIC_DB_ID && role_ != ALL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DB_ACCESS_INVALID, "");
	}
}

void StatementHandler::ConnectionOption::checkForUpdate(bool forUpdate) {
	if (dbId_ != GS_PUBLIC_DB_ID && role_ == READ && forUpdate == true) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DB_ACCESS_INVALID, "");
	}
}

void StatementHandler::ConnectionOption::checkSelect(SQLParsedInfo &parsedInfo) {
	if (role_ != ALL) {
		for (size_t i = 0; i < parsedInfo.syntaxTreeList_.size(); i++) {
			if (parsedInfo.syntaxTreeList_[i]->calcCommandType() != SyntaxTree::COMMAND_SELECT) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DB_ACCESS_INVALID,"");
			}
		}
	}
}

bool StatementHandler::checkPrivilege(
		EventType command,
		UserType userType, RequestType requestType, bool isSystemMode,
		int32_t featureVersion,
		ContainerType resourceType, ContainerAttribute resourceSubType,
		ContainerAttribute expectedResourceSubType) {

	bool isWriteMode = false;
	switch (command) {
		case PUT_CONTAINER:
		case DROP_CONTAINER:
			isWriteMode = true;
			break;
		default:
			isWriteMode = false;
	}


	bool granted = false;

	switch (requestType) {
	case Message::REQUEST_NOSQL:
		switch (resourceSubType) {
		case CONTAINER_ATTR_LARGE:
			granted = (isSystemMode == true);
			break;
		case CONTAINER_ATTR_VIEW:
			granted = (featureVersion >= StatementMessage::FEATURE_V4_2)
					&& (isSystemMode == true);
			break;
		case CONTAINER_ATTR_SINGLE:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE_SYSTEM:
			granted = (isSystemMode == true);
			break;
		case CONTAINER_ATTR_SUB:
			granted = (isSystemMode == true);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
				"Illeagal parameter. ContainerAttribute = " << resourceSubType);
		}
		break;

	case Message::REQUEST_NEWSQL:
		switch (resourceSubType) {
		case CONTAINER_ATTR_LARGE:
		case CONTAINER_ATTR_VIEW:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE_SYSTEM:
			granted = (isSystemMode == true);
			break;
		case CONTAINER_ATTR_SUB:
			granted = (isSystemMode == true);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
				"Illeagal parameter. ContainerAttribute = " << resourceSubType);
		}
		break;

	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
			"Illeagal parameter. RequestType = " << requestType);
	}

	if (expectedResourceSubType == CONTAINER_ATTR_ANY) {
		return granted;
	}
	else if (granted && (expectedResourceSubType != resourceSubType)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_ATTRIBUTE_UNMATCH,
			"Container attribute not match."
			<< " (expected=" << expectedResourceSubType
			<< ", actual=" << resourceSubType << ")");
	}

	return granted;
}

bool StatementHandler::isMetaContainerVisible(
		const MetaContainer &metaContainer, int32_t visibility) {
	if ((visibility & StatementMessage::CONTAINER_VISIBILITY_META) == 0) {
		return false;
	}

	if ((visibility &
			StatementMessage::CONTAINER_VISIBILITY_INTERNAL_META) != 0) {
		return true;
	}

	const MetaContainerInfo &info = metaContainer.getMetaContainerInfo();
	return !info.internal_;
}

bool StatementHandler::isSupportedContainerAttribute(ContainerAttribute attribute) {
	switch (attribute) {
	case CONTAINER_ATTR_SINGLE:
	case CONTAINER_ATTR_SINGLE_SYSTEM:
	case CONTAINER_ATTR_LARGE:
	case CONTAINER_ATTR_SUB:
	case CONTAINER_ATTR_VIEW:
		return true;
	default:
		return false;
	}
}

void StatementHandler::checkDbAccessible(
		const char8_t *loginDbName, const char8_t *specifiedDbName
		, bool isNewSql
		) {
	if (isNewSql) {
		return;
	}
	if (strcmp(loginDbName, specifiedDbName) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
				"Only can access connect db :" << loginDbName <<
				", can not access db : " << specifiedDbName);
	}
}

const char8_t *StatementHandler::clusterRoleToStr(
	StatementHandler::ClusterRole role) {
	const ClusterRole allStatesSet =
		CROLE_MASTER + CROLE_FOLLOWER + CROLE_SUBMASTER;
	return role > allStatesSet ? clusterRoleStr[allStatesSet]
							   : clusterRoleStr[role];
}

const char8_t *StatementHandler::partitionRoleTypeToStr(
	StatementHandler::PartitionRoleType role) {
	const PartitionRoleType allStatesSet =
		PROLE_OWNER + PROLE_BACKUP + PROLE_CATCHUP + PROLE_NONE;
	return role > allStatesSet ? partitionRoleTypeStr[allStatesSet]
							   : partitionRoleTypeStr[role];
}

const char8_t *StatementHandler::partitionStatusToStr(
	StatementHandler::PartitionStatus status) {
	const PartitionStatus allStatesSet =
		PSTATE_ON + PSTATE_SYNC + PSTATE_OFF + PSTATE_STOP;
	return status > allStatesSet ? partitionStatusStr[allStatesSet]
								 : partitionStatusStr[status];
}

bool StatementHandler::getApplicationNameByOptionsOrND(
		const OptionSet *optionSet, const NodeDescriptor *nd,
		util::String *nameStr, std::ostream *os) {

	const char8_t *name = (optionSet == NULL ?
			"" : optionSet->get<Options::APPLICATION_NAME>());
	bool found = (strlen(name) > 0);

	ConnectionOption *connOption = NULL;
	if (!found && nd != NULL) {
		connOption = &nd->getUserData<ConnectionOption>();
		if (connOption->getApplicationName(NULL)) {
			found = true;
		}
	}

	if (found) {
		if (connOption == NULL) {
			if (os != NULL) {
				*os << name;
			}
			if (nameStr != NULL) {
				*nameStr = name;
			}
		}
		else {
			if (os != NULL) {
				connOption->getApplicationName(*os);
			}
			if (nameStr != NULL) {
				connOption->getApplicationName(*nameStr);
			}
		}
	}

	return found;
}

const KeyConstraint& StatementHandler::getKeyConstraint(
		const OptionSet &optionSet, bool checkLength) const {
	return getKeyConstraint(
			optionSet.get<Options::CONTAINER_ATTRIBUTE>(), checkLength);
}

const KeyConstraint& StatementHandler::getKeyConstraint(
		ContainerAttribute containerAttribute, bool checkLength) const {
	const uint32_t CONTAINER_ATTR_IS_SYSTEM = 0x00000001;
	const uint32_t CONTAINER_ATTR_FOR_PARTITIONING = (CONTAINER_ATTR_LARGE & CONTAINER_ATTR_SUB);
	return keyConstraint_
		[((containerAttribute & CONTAINER_ATTR_IS_SYSTEM) != 0 ? 1 : 0)]
		[((containerAttribute & CONTAINER_ATTR_FOR_PARTITIONING) != 0 ? 1 : 0)]
		[(checkLength ? 1 : 0)];
}


bool StatementHandler::ConnectionOption::getApplicationName(
		util::BasicString<
				char8_t, std::char_traits<char8_t>,
				util::StdAllocator<char8_t, void> > *name) {
	util::LockGuard<util::Mutex> guard(mutex_);
	if (name != NULL) {
		*name = applicationName_.c_str();
	}
	return !applicationName_.empty();
}

void StatementHandler::ConnectionOption::setFirstStep(ClientId &clientId, double storeMemoryAgingSwapRate,
	const util::TimeZone &timeZone) {
	clientId_ = clientId;
	storeMemoryAgingSwapRate_ = storeMemoryAgingSwapRate;
	timeZone_ = timeZone;
}

void StatementHandler::ConnectionOption::setBeforeAuth(const char8_t *userName, const char8_t *dbName, const char8_t *applicationName,
			bool isImmediateConsistency, int32_t txnTimeoutInterval, UserType userType,
			RequestType requestType, bool isAdminAndPublicDB) {
	util::LockGuard<util::Mutex> guard(mutex_);
	userName_ = userName;
	dbName_ = dbName;
	applicationName_ = applicationName;
	isImmediateConsistency_ = isImmediateConsistency;
	txnTimeoutInterval_ = txnTimeoutInterval;
	userType_ = userType;
	requestType_ = requestType;
	isAdminAndPublicDB_ = isAdminAndPublicDB;
}

void StatementHandler::ConnectionOption::setAfterAuth(DatabaseId dbId, EventMonotonicTime authenticationTime, RoleType role) {
	dbId_ = dbId;
	authenticationTime_ = authenticationTime;
	role_ = role;
	
	isAuthenticated_ = true;
}

void StatementHandler::ConnectionOption::getHandlingClientId(ClientId &clientId) {
		util::LockGuard<util::Mutex> guard(mutex_);
	clientId = handlingClientId_;
}

void StatementHandler::ConnectionOption::setHandlingClientId(ClientId &clientId) {
		util::LockGuard<util::Mutex> guard(mutex_);
	handlingClientId_ = clientId;
	statementList_.insert(clientId.sessionId_);
}

void StatementHandler::ConnectionOption::getLoginInfo(
		SQLString &userName, SQLString &dbName, SQLString &applicationName) {
	util::LockGuard<util::Mutex> guard(mutex_);
	userName = userName_.c_str();
	dbName = dbName_.c_str();
	applicationName = applicationName_.c_str();
}

void StatementHandler::ConnectionOption::getSessionIdList(
		ClientId &clientId,
		util::XArray<SessionId> &sessionIdList) {
	util::LockGuard<util::Mutex> guard(mutex_);
	clientId = handlingClientId_;
	for (std::set<SessionId>::iterator idItr = statementList_.begin();
			idItr != statementList_.end(); idItr++) {
		sessionIdList.push_back(*idItr);
	}
}

void StatementHandler::ConnectionOption::setConnectionEnv(
		uint32_t paramId, const char8_t *value) {
	util::LockGuard<util::Mutex> guard(mutex_);
	envMap_[paramId] = value;
	updatedEnvBits_ |= (1 << paramId);
}

void StatementHandler::ConnectionOption::removeSessionId(ClientId &clientId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	statementList_.erase(clientId.sessionId_);
}

bool StatementHandler::ConnectionOption::getConnectionEnv(
		uint32_t paramId, std::string &value, bool &hasData) {
	util::LockGuard<util::Mutex> guard(mutex_);
	if (envMap_.size () == 0) {
		hasData = false;
		return false;
	}
	else {
		hasData = true;
	}
	std::map<uint32_t, std::string>::const_iterator it =
			envMap_.find(paramId);
	if (it != envMap_.end()) {
		value = (*it).second;
		return true;
	}
	else {
		return false;
	}
}


void StatementHandler::ConnectionOption::getApplicationName(
		std::ostream &os) {
	util::LockGuard<util::Mutex> guard(mutex_);
	os << applicationName_;
}

void StatementHandler::ErrorMessage::format(std::ostream &os) const {
	os << GS_EXCEPTION_MESSAGE(elements_.cause_);
	os << " (" << elements_.description_;
	formatParameters(os);
	os << ")";
}

void StatementHandler::ConnectionOption::initializeCoreInfo() {
	util::LockGuard<util::Mutex> guard(mutex_);

	userName_.clear();
	dbName_.clear();
	applicationName_ = "";

	clientId_ = ClientId();

	handlingClientId_ = ClientId();
	statementList_.clear();
	envMap_.clear();

}

void StatementHandler::ErrorMessage::formatParameters(std::ostream &os) const {
	const NodeDescriptor &nd =  elements_.ev_.getSenderND();

	if (getApplicationNameByOptionsOrND(
			elements_.options_, &nd, NULL, NULL)) {
		os << ", application=";
		getApplicationNameByOptionsOrND(
				elements_.options_, &nd, NULL, &os);
	}

	os << ", nd=" << nd;
	os << ", eventType=" << elements_.stmtType_;
	os << ", partition=" << elements_.pId_;

	if (elements_.stmtId_ != UNDEF_STATEMENTID) {
		os << ", statementId=" << elements_.stmtId_;
	}

	if (elements_.clientId_ != ClientId()) {
		os << ", transaction=" << elements_.clientId_;
	}

	if (elements_.containerId_ != UNDEF_CONTAINERID) {
		os << ", containerId=" << elements_.containerId_;
	}
}

ConnectHandler::ConnectHandler(ProtocolVersion currentVersion,
							   const ProtocolVersion *acceptableProtocolVersions) :
	currentVersion_(currentVersion),
	acceptableProtocolVersions_(acceptableProtocolVersions) {
}

/*!
	@brief Handler Operator
*/
void ConnectHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	ConnectRequest request(ev.getPartitionId(), ev.getType());
	Response response(alloc);
	try {

		EventByteInStream in(ev.getInStream());

		decodeIntData<OldStatementId>(in, request.oldStmtId_);
		decodeIntData<ProtocolVersion>(in, request.clientVersion_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		if (isNewSQL_) {
			checkExecutable(clusterRole);
		}
		else {
			checkExecutable(
				request.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		}

		checkClientVersion(request.clientVersion_);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clientVersion_ = request.clientVersion_;

		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_ = ec.getHandlerStartMonotonicTime();
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void ConnectHandler::checkClientVersion(ProtocolVersion clientVersion) {
	for (const ProtocolVersion *acceptableVersions = acceptableProtocolVersions_;;
			acceptableVersions++) {

		if (clientVersion == *acceptableVersions) {
			return;
		} else if (*acceptableVersions == PROTOCOL_VERSION_UNDEFINED) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
					"(connectClientVersion=" << clientVersion
											 << ", serverVersion="
											 << currentVersion_ << ")");
		}
	}
}

EventByteOutStream ConnectHandler::encodeCommonPart(
	Event &ev, OldStatementId stmtId, StatementExecStatus status) {
	try {
		ev.setPartitionIdSpecified(false);

		EventByteOutStream out = ev.getOutStream();

		out << stmtId;
		out << status;

		return out;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void ConnectHandler::replySuccess(EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &ND, EventType stmtType, StatementExecStatus status,
	const ConnectRequest &request, const Response &, bool) {
	util::StackAllocator::Scope scope(alloc);

	Event ev(ec, stmtType, request.pId_);
	encodeCommonPart(ev, request.oldStmtId_, status);

	EventByteOutStream out = ev.getOutStream();

	ConnectionOption &connOption = ND.getUserData<ConnectionOption>();
	encodeIntData<int8_t>(out, connOption.authMode_);

	encodeIntData(out, currentVersion_);

	if (ev.getExtraMessageCount() > 0) {
		EventEngine::EventRequestOption option;
		option.timeoutMillis_ = 0;
		ec.getEngine().send(ev, ND, &option);
	}
	else {
		ec.getEngine().send(ev, ND);
	}
}

void ConnectHandler::replyError(EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &ND, EventType stmtType, StatementExecStatus status,
	const ConnectRequest &request, const std::exception &e) {
	util::StackAllocator::Scope scope(alloc);

	Event ev(ec, stmtType, request.pId_);
	EventByteOutStream out = encodeCommonPart(ev, request.oldStmtId_, status);
	encodeException(out, e, ND);

	if (!ND.isEmpty()) {
		ec.getEngine().send(ev, ND);
	}
}

void ConnectHandler::handleError(
		EventContext &ec, util::StackAllocator &alloc,
		Event &ev, const ConnectRequest &request, std::exception &e) {
	util::StackAllocator::Scope scope(alloc);

	FixedRequest fixedRequest(getRequestSource(ev));
	fixedRequest.cxtSrc_.stmtId_ = request.oldStmtId_;
	ErrorMessage errorMessage(e, "", ev, fixedRequest);

	try {
		try {
			throw;

		}
		catch (EncodeDecodeException&) {
			UTIL_TRACE_EXCEPTION(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Failed to encode or decode message"));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_ERROR, request, e);

		}
		catch (DenyException&) {
			UTIL_TRACE_EXCEPTION(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Request denied"));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_DENY, request, e);

		}
		catch (std::exception&) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				UTIL_TRACE_EXCEPTION(
						TRANSACTION_SERVICE, e, errorMessage.withDescription(
								"User error"));
				replyError(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_ERROR, request, e);
			}
		}
	}
	catch (std::exception&) {
		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
						"System error"));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setSystemError(&e);
	}
}

/*!
	@brief Handler Operator
*/
void DisconnectHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	Request request(alloc, getRequestSource(ev));

	try {

		resourceSet_->getSQLExecutionManager()->clearConnection(ev, isNewSQL_);

	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void LoginHandler::checkClusterName(std::string &clusterName) {
	if (clusterName.size() > 0) {
		const std::string currentClusterName =
			clusterService_->getManager()->getClusterName();
		if (clusterName.compare(currentClusterName) != 0) {
			TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_NAME_INVALID,
				"cluster name invalid (input="
					<< clusterName << ", current=" << currentClusterName
					<< ")");
		}
	}
}

void LoginHandler::checkApplicationName(const char8_t *applicationName) {
	if (strlen(applicationName) != 0) {
		try {
			NoEmptyKey::validate(
					KeyConstraint::getUserKeyConstraint(MAX_APPLICATION_NAME_LEN),
					applicationName, static_cast<uint32_t>(strlen(applicationName)),
					"applicationName");
		}
		catch (UserException &e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
					"Application name invalid (input=" <<
					applicationName << ")"));
		}
	}
}

bool LoginHandler::checkPublicDB(const char8_t *dbName) {
	if (strcmp(dbName, GS_PUBLIC) == 0) {
		return true;
	}
	return false;
}

bool LoginHandler::checkSystemDB(const char8_t *dbName) {
	if (strcmp(dbName, GS_SYSTEM) == 0) {
		return true;
	}
	return false;
}

bool LoginHandler::checkAdmin(util::String &userName) {
	if ((strcmp(userName.c_str(), GS_ADMIN_USER) == 0) ||
		(strcmp(userName.c_str(), GS_SYSTEM_USER) == 0) ||
		(userName.find('#') != std::string::npos)) {
			return true;
	}
	return false;
}

bool LoginHandler::checkLocalAuthNode() {
	NodeAddress nodeAddress =
			clusterManager_->getPartition0NodeAddr();
	TEST_PRINT1(
			"NodeAddress(TxnServ) %s\n", nodeAddress.dump().c_str());
	if (nodeAddress.port_ == 0) {
		TXN_THROW_DENY_ERROR(
				GS_ERROR_TXN_AUTHENTICATION_SERVICE_NOT_READY, "");
	}

	const NodeDescriptor &nd0 =
		transactionService_->getEE()->getServerND(
			util::SocketAddress(nodeAddress.toString(false).c_str(),
				nodeAddress.port_));

	if (nd0.isSelf() && !isNewSQL_) {
		return true;
	} else {
		return false;
	}
}

/*!
	@brief Handler Operator
*/
void LoginHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<LoginHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
		resourceSet_->getSQLExecutionManager()->clearConnection(ev, isNewSQL_);

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		const RequestType requestType =
				request.optional_.get<Options::REQUEST_MODULE_TYPE>();
		const char8_t *dbName = request.optional_.get<Options::DB_NAME>();
		const bool systemMode = request.optional_.get<Options::SYSTEM_MODE>();
		const int32_t txnTimeoutInterval =
				(request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() >= 0 ?
				request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() :
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);
		const char8_t *applicationName =
				request.optional_.get<Options::APPLICATION_NAME>();
		const double storeMemoryAgingSwapRate =
				request.optional_.get<Options::STORE_MEMORY_AGING_SWAP_RATE>();
		const util::TimeZone timeZone =
				request.optional_.get<Options::TIME_ZONE_OFFSET>();

		util::String userName(alloc);
		util::String digest(alloc);
		std::string clusterName;
		int32_t stmtTimeoutInterval;
		bool isImmediateConsistency;

		decodeStringData<util::String>(in, userName);
		decodeStringData<util::String>(in, digest);
		decodeIntData<int32_t>(in,
			stmtTimeoutInterval);  
		decodeBooleanData(in, isImmediateConsistency);
		if (in.base().remaining() > 0) {
			decodeStringData<std::string>(in, clusterName);
		}
		response.connectionOption_ = &connOption;
		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("userName=%s\n", userName.c_str());
		TEST_PRINT1("digest=%s\n", digest.c_str());
		TEST_PRINT1("stmtTimeoutInterval=%d\n", stmtTimeoutInterval);
		TEST_PRINT1("isImmediateConsistency=%d\n", isImmediateConsistency);
		TEST_PRINT1("clusterName=%s\n", clusterName.c_str());

		TEST_PRINT1("requestType=%d\n", requestType);
		TEST_PRINT1("dbName=%s\n", dbName);
		TEST_PRINT1("systemMode=%d\n", systemMode);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkClusterName(clusterName);
		checkApplicationName(applicationName);

		connOption.setFirstStep(request.fixed_.clientId_, storeMemoryAgingSwapRate, timeZone);

		DatabaseId dbId = UNDEF_DBID;
		RoleType role = READ;

		if (checkPublicDB(dbName) || checkSystemDB(dbName)) {
			if (checkAdmin(userName)) {
				TEST_PRINT("LoginHandler::operator() public&admin [A1] S\n");
				if (systemService_->getUserTable().isValidUser(
						userName.c_str(), digest.c_str())) {  
					UserType userType;

					userType = Message::USER_ADMIN;
					if (checkSystemDB(dbName)) {
						GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
								"database name invalid (" << dbName << ")");
					}
					connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, userType,
							requestType, true);
					if (checkPublicDB(dbName)) {
						dbId = GS_PUBLIC_DB_ID;
					}
					else {
						dbId = GS_SYSTEM_DB_ID;
					}
					connOption.setAfterAuth(dbId, emNow, ALL);

					replySuccess(
							ec, alloc, ev.getSenderND(), ev.getType(),
							TXN_STATEMENT_SUCCESS, request, response,
							NO_REPLICATION);
					TEST_PRINT("LoginHandler::operator() public&admin  [A1] E\n");
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
							"invalid user name or password (user name = " <<
							userName.c_str() << ")");
				}
			}
			else {  
				TEST_PRINT("LoginHandler::operator() public&normal S\n");


				if (checkSystemDB(dbName)) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
							"database name invalid (" << dbName << ")");
				}

				if (checkLocalAuthNode()) {
					TEST_PRINT("Self\n");
					TEST_PRINT("[A2] S\n");
					
					connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, Message::USER_NORMAL,
							requestType, false);

					executeAuthenticationInternal(
							ec, alloc, request.fixed_.cxtSrc_,
							userName.c_str(), digest.c_str(),
							dbName, Message::USER_NORMAL, 1,
							dbId, role);  

					connOption.setAfterAuth(dbId, emNow, role);
					
					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
					TEST_PRINT("[A2] E\n");
				}
				else {
					TEST_PRINT("Other\n");
					TEST_PRINT("[A3] S\n");

					connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, Message::USER_NORMAL,
							requestType, false);

					executeAuthentication(
							ec, ev, ev.getSenderND(),
							request.fixed_.cxtSrc_.stmtId_,
							userName.c_str(), digest.c_str(),
							dbName, connOption.userType_);

				}
				TEST_PRINT("LoginHandler::operator() public&normal E\n");
			}
		}
		else {
			bool isLocalAuthNode = checkLocalAuthNode();

			if (checkAdmin(userName)) {
				if (systemService_->getUserTable().isValidUser(
						userName.c_str(), digest.c_str())) {  
					TEST_PRINT("LoginHandler::operator() admin S\n");

					
					if (isLocalAuthNode) {
						TEST_PRINT("Self\n");
						TEST_PRINT("[A4] S\n");

						UserType userType;
						userType = Message::USER_ADMIN;
						connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, userType,
							requestType, false);

						executeAuthenticationInternal(
								ec, alloc,
								request.fixed_.cxtSrc_, userName.c_str(), digest.c_str(),
								dbName, connOption.userType_, 2,
								dbId, role);  

						connOption.setAfterAuth(dbId, emNow, role);

						replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
							TXN_STATEMENT_SUCCESS, request, response,
							NO_REPLICATION);
						TEST_PRINT("[A4] E\n");
					}
					else {
						TEST_PRINT("Other\n");
						TEST_PRINT("[A5] S\n");

						UserType userType;
						userType = Message::USER_ADMIN;
						connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, userType,
							requestType, false);

						executeAuthentication(
								ec, ev, ev.getSenderND(),
								request.fixed_.cxtSrc_.stmtId_,
								userName.c_str(), digest.c_str(),
								dbName, connOption.userType_);

					}
					TEST_PRINT("LoginHandler::operator() admin E\n");
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
						"invalid user name or password (user name = "
							<< userName.c_str() << ")");
				}
			}
			else {  
				TEST_PRINT("LoginHandler::operator() normal S\n");


				if (isLocalAuthNode) {
					TEST_PRINT("Self\n");
					TEST_PRINT("[A6] S\n");
					
					connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, Message::USER_NORMAL,
							requestType, false);

					executeAuthenticationInternal(
							ec, alloc, request.fixed_.cxtSrc_,
							userName.c_str(), digest.c_str(),
							dbName, Message::USER_NORMAL, 3,
							dbId, role);  

					connOption.setAfterAuth(dbId, emNow, role);

					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
					TEST_PRINT("[A6] E\n");
				}
				else {
					TEST_PRINT("Other\n");
					TEST_PRINT("[A7] S\n");
					
					connOption.setBeforeAuth(userName.c_str(), dbName, applicationName,
							isImmediateConsistency, txnTimeoutInterval, Message::USER_NORMAL,
							requestType, false);

					executeAuthentication(
							ec, ev, ev.getSenderND(),
							request.fixed_.cxtSrc_.stmtId_,
							userName.c_str(), digest.c_str(),
							dbName, connOption.userType_);

				}

				TEST_PRINT("LoginHandler::operator() normal E\n");
			}
		}

		TEST_PRINT("<<<LoginHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void LogoutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);


		resourceSet_->getSQLExecutionManager()->clearConnection(ev, isNewSQL_);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetPartitionAddressHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		bool masterResolving = false;
		if (in.base().remaining() > 0) {
			decodeBooleanData(in, masterResolving);
		}

		const ClusterRole clusterRole =
			CROLE_MASTER | (masterResolving ? CROLE_FOLLOWER : 0);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::XArrayOutStream<> arrayOut(response.binaryData_);
		util::ByteStream<util::XArrayOutStream<> > out(arrayOut);

		out << partitionTable_->getPartitionNum();

		const NodeId ownerNodeId = (masterResolving ?
				UNDEF_NODEID : partitionTable_->getOwner(request.fixed_.pId_));
		if (ownerNodeId != UNDEF_NODEID) {
			NodeAddress &address = partitionTable_->getPublicNodeAddress(
					ownerNodeId, TRANSACTION_SERVICE);
			out << static_cast<uint8_t>(1);
			out << std::pair<const uint8_t*, size_t>(
					reinterpret_cast<const uint8_t*>(&address.address_),
					sizeof(AddressType));
			out << static_cast<uint32_t>(address.port_);
		}
		else {
			out << static_cast<uint8_t>(0);
		}

		util::XArray<NodeId> backupNodeIds(alloc);
		if (!masterResolving) {
			partitionTable_->getBackup(request.fixed_.pId_, backupNodeIds);
		}

		const uint8_t numBackup = static_cast<uint8_t>(backupNodeIds.size());
		out << numBackup;
		for (uint8_t i = 0; i < numBackup; i++) {

			NodeAddress &address = partitionTable_->getPublicNodeAddress(
					backupNodeIds[i], TRANSACTION_SERVICE);
			out << std::pair<const uint8_t*, size_t>(
					reinterpret_cast<const uint8_t*>(&address.address_),
					sizeof(AddressType));
			out << static_cast<uint32_t>(address.port_);
		}

		if (masterResolving) {
			encodeClusterInfo(
					out, ec.getEngine(), *partitionTable_,
					CONTAINER_HASH_MODE_CRC32);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void GetPartitionAddressHandler::encodeClusterInfo(
		util::XArrayByteOutStream &out, EventEngine &ee,
		PartitionTable &partitionTable, ContainerHashMode hashMode) {
	const NodeId masterNodeId = partitionTable.getMaster();
	if (masterNodeId == UNDEF_NODEID) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH, 
			"(required={MASTER}, actual={SUB_CLUSTER})");
	}
	const NodeDescriptor &masterND = ee.getServerND(masterNodeId);
	if (masterND.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
	}

	const int8_t masterMatched = masterND.isSelf();
	out << masterMatched;

	out << hashMode;

	util::SocketAddress::Inet addr;
	uint16_t port;
	masterND.getAddress().getIP(&addr, &port);
	out << std::pair<const uint8_t *, size_t>(
			reinterpret_cast<const uint8_t *>(&addr), sizeof(addr));
	out << static_cast<uint32_t>(port);
}

/*!
	@brief Handler Operator
*/
void GetPartitionContainerNamesHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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
		bool isNewSql = isNewSQL(request);
		DatabaseId dbId = connOption.dbId_;
		if (isNewSql) {
			dbId = request.optional_.get<Options::DB_VERSION_ID>();
		}

		int64_t start;
		ResultSize limit;
		decodeLongData<int64_t>(in, start);
		decodeLongData<ResultSize>(in, limit);
		checkSizeLimit(limit);

		DataStore::ContainerCondition containerCondition(alloc);  
		decodeContainerConditionData(in, containerCondition);	 

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);
		checkDbAccessible(
				connOption.dbName_.c_str(),			
				request.optional_.get<Options::DB_NAME>()
				, isNewSql
				);
		DataStore::ContainerCondition validContainerCondition(alloc);
		for (util::Set<ContainerAttribute>::const_iterator itr =
				 containerCondition.getAttributes().begin();
				 itr != containerCondition.getAttributes().end(); itr++) {
			if (!isSupportedContainerAttribute(*itr)) {
				continue;
			}
			if (checkPrivilege(
					ev.getType(),
					connOption.userType_, connOption.requestType_,
					request.optional_.get<Options::SYSTEM_MODE>(),
					request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
					ANY_CONTAINER, *itr)) {
				validContainerCondition.insertAttribute(*itr);
			}
		}

		response.containerNum_ = dataStore_->getContainerCount(txn,
			txn.getPartitionId(), dbId, validContainerCondition);
		dataStore_->getContainerNameList(txn, txn.getPartitionId(), start,
			limit, dbId, validContainerCondition,
			response.containerNameList_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetContainerPropertiesHandler::operator()(EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const char8_t *dbName = request.optional_.get<Options::DB_NAME>();
		const int32_t visibility =
				request.optional_.get<Options::CONTAINER_VISIBILITY>();

		ContainerNameList containerNameList(alloc);
		uint32_t propFlags = 0;
		try {
			for (uint32_t remaining = 1; remaining > 0; remaining--) {
				util::XArray<uint8_t> *name = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeVarSizeBinaryData(in, *name);
				containerNameList.push_back(name);

				if (containerNameList.size() == 1) {
					uint32_t propTypeCount;
					in >> propTypeCount;

					for (uint32_t i = 0; i < propTypeCount; i++) {
						uint8_t propType;
						in >> propType;
						propFlags |= (1 << propType);
					}

					if (in.base().remaining() > 0) {
						in >> remaining;
					}
				}
			}
		}
		catch (std::exception &e) {
			TXN_RETHROW_DECODE_ERROR(e, "");
		}
		const uint32_t propTypeCount = util::countNumOfBits(propFlags);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
				reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		const int32_t acceptableFeatureVersion =
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>();

		int64_t currentTime = ec.getHandlerStartTime().getUnixTime();
		util::String containerNameStr(alloc);
		bool isNewSql = isNewSQL(request);
		for (ContainerNameList::iterator it = containerNameList.begin();
			 it != containerNameList.end(); ++it) {
			const FullContainerKey queryContainerKey(
					alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false), (*it)->data(), (*it)->size());
			checkLoggedInDatabase(
					connOption.dbId_, connOption.dbName_.c_str(),
					queryContainerKey.getComponents(alloc).dbId_,
					request.optional_.get<Options::DB_NAME>()
					, isNewSql
					);
			bool isAllowExpiration = true;
			ContainerAutoPtr containerAutoPtr(
					txn, dataStore_,
					txn.getPartitionId(), queryContainerKey, ANY_CONTAINER,
					getCaseSensitivity(request).isContainerNameCaseSensitive(), isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			if (container != NULL && !checkPrivilege(
					ev.getType(),
					connOption.userType_, connOption.requestType_,
					request.optional_.get<Options::SYSTEM_MODE>(),
					request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
					container->getContainerType(),
					container->getAttribute(),
					request.optional_.get<Options::CONTAINER_ATTRIBUTE>())) {
				container = NULL;
			}

			if (it - containerNameList.begin() == 1) {
				encodeResultListHead(
						out, static_cast<uint32_t>(containerNameList.size()));
			}

			if (container == NULL) {
				MetaStore metaStore(*dataStore_);

				MetaType::NamingType namingType;
				namingType = static_cast<MetaType::NamingType>(
						request.optional_.get<Options::META_NAMING_TYPE>());

				MetaContainer *metaContainer = metaStore.getContainer(
						txn, queryContainerKey, namingType);

				if (metaContainer != NULL && !isMetaContainerVisible(
						*metaContainer, visibility)) {
					metaContainer = NULL;
				}

				if (metaContainer != NULL &&
						metaContainer->getMetaContainerInfo().nodeDistribution_ &&
						acceptableFeatureVersion < Message::FEATURE_V4_2) {
					metaContainer = NULL;
				}

				const bool forMeta = true;
				encodeContainerProps(
						txn, out, metaContainer, propFlags, propTypeCount,
						forMeta, emNow, containerNameStr, dbName
						, currentTime
						, acceptableFeatureVersion
						);

				continue;
			}

			{
				const bool forMeta = false;
				encodeContainerProps(
						txn, out, container, propFlags, propTypeCount,
						forMeta, emNow, containerNameStr, dbName
						, currentTime
						, acceptableFeatureVersion
				);
			}
		}

		ec.getEngine().send(reply, ev.getSenderND());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

template<typename C>
void GetContainerPropertiesHandler::encodeContainerProps(
		TransactionContext &txn, EventByteOutStream &out, C *container,
		uint32_t propFlags, uint32_t propTypeCount, bool forMeta,
		EventMonotonicTime emNow, util::String &containerNameStr,
		const char8_t *dbName
		, int64_t currentTime
		, int32_t acceptableFeatureVersion
) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	const bool found = (container != NULL);
	out << static_cast<uint8_t>(found);

	if (!found) {
		return;
	}

	const FullContainerKey realContainerKey = container->getContainerKey(txn);
	containerNameStr.clear();
	realContainerKey.toString(alloc, containerNameStr);

	encodePropsHead(out, propTypeCount);

	if ((propFlags & (1 << CONTAINER_PROPERTY_ID)) != 0) {
		const ContainerId containerId = container->getContainerId();
		const SchemaVersionId schemaVersionId =
				container->getVersionId();

		MetaDistributionType metaDistType = META_DIST_NONE;
		ContainerId metaContainerId = UNDEF_CONTAINERID;
		int8_t metaNamingType = -1;
		if (forMeta) {
			MetaContainer *metaContainer =
					static_cast<MetaContainer*>(container);
			metaDistType = META_DIST_FULL;
			if (metaContainer->getMetaContainerInfo().nodeDistribution_) {
				metaDistType = META_DIST_NODE;
			}
			metaContainerId = metaContainer->getMetaContainerId();
			metaNamingType = metaContainer->getColumnNamingType();
		}

		encodeId(
				out, schemaVersionId, containerId, realContainerKey,
				metaContainerId, metaDistType, metaNamingType);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_SCHEMA)) != 0) {
		if (acceptableFeatureVersion < StatementMessage::FEATURE_V4_3) {
			if (container->getRowKeyColumnNum() > 1) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
						"ConnectClientVersion not support composite row key");
			}
		}
		const ContainerType containerType =
				container->getContainerType();

		const bool optionIncluded = true;
		util::XArray<uint8_t> containerInfo(alloc);
		container->getContainerInfo(txn, containerInfo, optionIncluded, false);
		encodeSchema(out, containerType, containerInfo);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_INDEX)) != 0) {
		util::Vector<IndexInfo> indexInfoList(alloc);
		container->getIndexInfoList(txn, indexInfoList);

		for (util::Vector<IndexInfo>::iterator itr = indexInfoList.begin();
				itr != indexInfoList.end();) {
			checkIndexInfoVersion(*itr, acceptableFeatureVersion);

			if (itr->columnIds_.size() > 1) {
				itr = indexInfoList.erase(itr);
			}
			else {
				++itr;
			}
		}

		encodeIndex(out, indexInfoList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_EVENT_NOTIFICATION)) !=
			0) {
		util::XArray<char *> urlList(alloc);
		util::XArray<uint32_t> urlLenList(alloc);


		encodeEventNotification(out, urlList, urlLenList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_TRIGGER)) != 0) {
		util::XArray<const uint8_t *> triggerList(alloc);
		container->getTriggerList(txn, triggerList);

		encodeTrigger(out, triggerList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_ATTRIBUTES)) != 0) {
		encodeAttributes(out, container->getAttribute());
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_INDEX_DETAIL)) != 0) {
		util::Vector<IndexInfo> indexInfoList(alloc);
		if (container->getAttribute() == CONTAINER_ATTR_LARGE) {
			util::StackAllocator &alloc = txn.getDefaultAllocator();
			TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
			decodeLargeRow(
					NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					alloc, txn, container->getDataStore(),
					dbName, container, tablePartitioningIndexInfo, emNow);
			tablePartitioningIndexInfo.getIndexInfoList(alloc, indexInfoList);
		}
		else {
			container->getIndexInfoList(txn, indexInfoList);
		}

		checkIndexInfoVersion(indexInfoList, acceptableFeatureVersion);
		encodeIndexDetail(out, indexInfoList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_PARTITIONING_METADATA)) != 0) {
		encodePartitioningMetaData(
				out, txn, emNow, *container, container->getAttribute(),
				dbName, containerNameStr.c_str()
				, currentTime
				);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_NULLS_STATISTICS)) != 0) {
		util::XArray<uint8_t> nullsList(alloc);
		container->getNullsStats(nullsList);
		encodeNulls(out, nullsList);
	}
}

void GetContainerPropertiesHandler::getPartitioningTableIndexInfo(
	TransactionContext &txn, EventMonotonicTime emNow, DataStore &dataStore,
	BaseContainer &largeContainer, const char8_t *containerName,
	const char *dbName, util::Vector<IndexInfo> &indexInfoList) {
	try {
		util::NormalOStringStream queryStr;
		queryStr << "SELECT * WHERE key = '"
			<< NoSQLUtils::LARGE_CONTAINER_KEY_INDEX
			<< "'";
		FetchOption fetchOption;
		fetchOption.limit_ = UINT64_MAX;
		fetchOption.size_ = UINT64_MAX;

		ResultSet *rs = dataStore.createResultSet(
			txn, largeContainer.getContainerId(), 
			largeContainer.getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, dataStore, *rs);
		QueryProcessor::executeTQL(
			txn, largeContainer, fetchOption.limit_, TQLInfo(dbName, NULL, queryStr.str().c_str()), *rs);

		QueryProcessor::fetch(
			txn, largeContainer, 0, fetchOption.size_, rs);
		const uint64_t rowCount = rs->getResultNum();

		InputMessageRowStore rowStore(
			dataStore.getValueLimitConfig(),
			largeContainer.getColumnInfoList(), largeContainer.getColumnNum(),
			rs->getRowDataFixedPartBuffer()->data(),
			static_cast<uint32_t>(rs->getRowDataFixedPartBuffer()->size()),
			rs->getRowDataVarPartBuffer()->data(),
			static_cast<uint32_t>(rs->getRowDataVarPartBuffer()->size()),
			rowCount, rs->isRowIdInclude());

		NoSQLUtils::decodePartitioningTableIndexInfo(
			txn.getDefaultAllocator(), rowStore, indexInfoList);

	} catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"Failed to get partitioning table index metadata. containerName="
			<< containerName);
	}
}

void GetContainerPropertiesHandler::encodeResultListHead(
	EventByteOutStream &out, uint32_t totalCount) {
	try {
		out << (totalCount - 1);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodePropsHead(
	EventByteOutStream &out, uint32_t propTypeCount) {
	try {
		out << propTypeCount;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeId(
		EventByteOutStream &out, SchemaVersionId schemaVersionId,
		ContainerId containerId, const FullContainerKey &containerKey,
		ContainerId metaContainerId, MetaDistributionType metaDistType,
		int8_t metaNamingType) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_ID);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		out << schemaVersionId;
		out << containerId;
		encodeContainerKey(out, containerKey);
		encodeMetaId(out, metaContainerId, metaDistType, metaNamingType);

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeMetaId(
		EventByteOutStream &out, ContainerId metaContainerId,
		MetaDistributionType metaDistType, int8_t metaNamingType) {
	if (metaContainerId == UNDEF_CONTAINERID) {
		return;
	}

	try {
		encodeEnumData<MetaDistributionType>(out, metaDistType);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		out << metaContainerId;
		out << metaNamingType;

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeSchema(EventByteOutStream &out,
	ContainerType containerType,
	const util::XArray<uint8_t> &serializedCollectionInfo) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_SCHEMA);

		const uint32_t size = static_cast<uint32_t>(
			sizeof(uint8_t) + serializedCollectionInfo.size());
		out << size;
		out << containerType;
		out << std::pair<const uint8_t *, size_t>(
			serializedCollectionInfo.data(), serializedCollectionInfo.size());
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeIndex(
	EventByteOutStream &out, const util::Vector<IndexInfo> &indexInfoList) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_INDEX);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(indexInfoList.size());
		out << num;
		for (uint32_t i = 0; i < num; i++) {
			out << indexInfoList[i].columnIds_[0];
			encodeEnumData<MapType>(out, indexInfoList[i].mapType);
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeEventNotification(
	EventByteOutStream &out, const util::XArray<char *> &urlList,
	const util::XArray<uint32_t> &urlLenList) {
	try {
		encodeEnumData<ContainerProperty>(
			out, CONTAINER_PROPERTY_EVENT_NOTIFICATION);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(urlLenList.size());
		out << num;
		for (uint32_t i = 0; i < num; i++) {
			out << urlLenList[i] + 1;
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(urlList[i]),
				static_cast<size_t>(urlLenList[i] + 1));
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeTrigger(
	EventByteOutStream &out, const util::XArray<const uint8_t *> &triggerList) {
	try {
		util::StackAllocator &alloc = *triggerList.get_allocator().base();

		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_TRIGGER);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(triggerList.size());
		out << num;
		for (size_t i = 0; i < triggerList.size(); i++) {
			TriggerInfo info(alloc);
			TriggerInfo::decode(triggerList[i], info);

			out << info.name_;
			out << static_cast<int8_t>(info.type_);
			out << info.uri_;
			out << info.operation_;
			out << static_cast<uint32_t>(info.columnIds_.size());
			for (size_t i = 0; i < info.columnIds_.size(); i++) {
				out << info.columnIds_[i];
			}
			const std::string jmsProviderName(
				TriggerService::jmsProviderTypeToStr(info.jmsProviderType_));
			out << jmsProviderName;
			const std::string jmsDestinationType(
				TriggerService::jmsDestinationTypeToStr(
					info.jmsDestinationType_));
			out << jmsDestinationType;
			out << info.jmsDestinationName_;
			out << info.jmsUser_;
			out << info.jmsPassword_;
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeAttributes(
	EventByteOutStream &out, const ContainerAttribute containerAttribute) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_ATTRIBUTES);
		int32_t attribute = static_cast<int32_t>(containerAttribute);

		const uint32_t size = sizeof(ContainerAttribute);
		out << size;
		out << attribute;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeIndexDetail(
	EventByteOutStream &out, const util::Vector<IndexInfo> &indexInfoList) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_INDEX_DETAIL);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(indexInfoList.size());
		out << num;

		for (size_t i = 0; i < indexInfoList.size(); i++) {
			encodeIndexInfo(out, indexInfoList[i]);
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeNulls(
	EventByteOutStream &out, const util::XArray<uint8_t> &nullsList) {
	try {
		util::StackAllocator &alloc = *nullsList.get_allocator().base();

		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_NULLS_STATISTICS);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(nullsList.size());
		out << num;
		for (size_t i = 0; i < nullsList.size(); i++) {
			out << nullsList[i];
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::checkIndexInfoVersion(
		const util::Vector<IndexInfo> indexInfoList,
		int32_t acceptableFeatureVersion) {
	for (util::Vector<IndexInfo>::const_iterator itr = indexInfoList.begin();
			itr != indexInfoList.end(); ++itr) {
		checkIndexInfoVersion(*itr, acceptableFeatureVersion);
	}
}

void GetContainerPropertiesHandler::checkIndexInfoVersion(
		const IndexInfo &info, int32_t acceptableFeatureVersion) {
	if (acceptableFeatureVersion < StatementMessage::FEATURE_V4_3 &&
			info.columnIds_.size() > 1) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
				"ConnectClientVersion not support composite column index");
	}
}

void GetContainerPropertiesHandler::encodePartitioningMetaData(
	EventByteOutStream &out,
	TransactionContext &txn, EventMonotonicTime emNow,
	BaseContainer &largeContainer, ContainerAttribute attribute,
	const char8_t *dbName, const char8_t *containerName
	, int64_t currentTime
	) {
	try {
		util::StackAllocator &alloc = txn.getDefaultAllocator();

		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_PARTITIONING_METADATA);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		int32_t distributedPolicy = 1;

		if (attribute != CONTAINER_ATTR_LARGE) {
			return;
		}

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		decodeLargeRow(NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				alloc, txn, dataStore_, dbName,
				&largeContainer, partitioningInfo, emNow);

		bool isTableExpiration = partitioningInfo.isTableExpiration();
		if (isTableExpiration) {
			distributedPolicy = 2;
		}

		out << distributedPolicy;

		int32_t partitioningType = static_cast<int32_t>(partitioningInfo.partitionType_);
		out << partitioningType;

		int32_t distributedMethod = 0;
		out << distributedMethod;

		if (SyntaxTree::isInlcludeHashPartitioningType(partitioningInfo.partitionType_)) {
				out << static_cast<int32_t>(partitioningInfo.partitioningNum_);
		}
		if (SyntaxTree::isRangePartitioningType(partitioningInfo.partitionType_)) {
			TupleList::TupleColumnType partitionColumnOriginalType
					= static_cast<TupleList::TupleColumnType>(partitioningInfo.partitionColumnType_);

			int8_t partitionColumnType = static_cast<int8_t>(
					SQLUtils::convertTupleTypeToNoSQLType(
							partitionColumnOriginalType));
			out << partitionColumnType;

			switch (partitionColumnOriginalType)
			{
				case TupleList::TYPE_BYTE:
			  {
					int8_t value = static_cast<int8_t>(partitioningInfo.intervalValue_);
					out << value;
				}
			  break;
				case TupleList::TYPE_SHORT:
			  {
					int16_t value = static_cast<int16_t>(partitioningInfo.intervalValue_);
					out << value;
				}
				break;
				case TupleList::TYPE_INTEGER:
			  {
					int32_t value = static_cast<int32_t>(partitioningInfo.intervalValue_);
					out << value;
				}
				break;
				case TupleList::TYPE_LONG:
				case TupleList::TYPE_TIMESTAMP:
			  {
					int64_t value = static_cast<int64_t>(partitioningInfo.intervalValue_);
					out << value;
				}
				break;
				default:
					GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_PROPERTY_INVALID,
							"Not supported type=" << partitionColumnOriginalType);
			}
			int8_t unitValue = -1;
			if (partitionColumnOriginalType == TupleList::TYPE_TIMESTAMP) {
				unitValue = static_cast<int8_t>(partitioningInfo.intervalUnit_);
			}
			out << unitValue;
		}
		out << static_cast<int64_t>(partitioningInfo.partitioningVersionId_);

		out << static_cast<int32_t>(partitioningInfo.condensedPartitionIdList_.size());

		for (size_t pos = 0; pos < partitioningInfo.condensedPartitionIdList_.size(); pos++) {
			out << static_cast<int64_t>(partitioningInfo.condensedPartitionIdList_[pos]);
		}

		if (partitioningInfo.partitioningColumnId_ != UNDEF_COLUMNID) {
			int32_t partitionColumnSize = 1;
			out << partitionColumnSize;
			out << partitioningInfo.partitioningColumnId_;
		}

		if (partitioningInfo.subPartitioningColumnId_ != UNDEF_COLUMNID) {
			int32_t partitionColumnSize = 1;
			partitionColumnSize = 1;
			out << partitionColumnSize;
			out << partitioningInfo.subPartitioningColumnId_;
		}

		util::Vector<int64_t> availableList(alloc);
		util::Vector<int64_t> availableCountList(alloc);
		util::Vector<int64_t> disAvailableList(alloc);
		util::Vector<int64_t> disAvailableCountList(alloc);
		if (partitioningInfo.partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			partitioningInfo.getSubIdList(alloc, availableList, availableCountList, disAvailableList, disAvailableCountList
				, currentTime
				);
		}

		out << static_cast<int32_t>(availableList.size());

		for (size_t pos = 0; pos < availableList.size(); pos++) {
			out << static_cast<int64_t>(availableList[pos]);
			out << static_cast<int64_t>(availableCountList[pos]);
		}

		out << static_cast<int32_t>(disAvailableList.size());

		for (size_t pos = 0; pos < disAvailableList.size(); pos++) {
			out << static_cast<int64_t>(disAvailableList[pos]);
			out << static_cast<int64_t>(disAvailableCountList[pos]);
		}


		out << static_cast<int8_t>(partitioningInfo.currentStatus_);

		if (partitioningInfo.currentStatus_ == PARTITION_STATUS_CREATE_START ||
				partitioningInfo.currentStatus_ == PARTITION_STATUS_DROP_START) {
			out << static_cast<int64_t>(partitioningInfo.currentAffinityNumber_);
		}

		if (partitioningInfo.currentStatus_ == INDEX_STATUS_CREATE_START) {
			encodeStringData(out, partitioningInfo.currentIndexName_);
			out << static_cast<int32_t>(partitioningInfo.currentIndexType_);
			int32_t partitionColumnSize = 1;
			out << partitionColumnSize;
			out << static_cast<int32_t>(partitioningInfo.currentIndexColumnId_);
		}
		else if (partitioningInfo.currentStatus_ == INDEX_STATUS_DROP_START) {
			encodeStringData(out, partitioningInfo.currentIndexName_);
		}

		if (distributedPolicy >= 2) {
			out << currentTime;
			out << partitioningInfo.timeSeriesProperty_.elapsedTime_;
			out << partitioningInfo.timeSeriesProperty_.timeUnit_;
		}


		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR(e,
			"Failed to get partitioning metadata. containerName="
			<< containerName);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e,
			"Failed to get partitioning metadata. containerName="
			<< containerName);
	}
}

/*!
	@brief Handler Operator
*/
void PutContainerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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
		decodeBinaryData(in, containerInfo, true);

		setContainerAttributeForCreate(request.optional_);

		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_ = ec.getHandlerStartMonotonicTime();
		}
		request.fixed_.clientId_ = request.optional_.get<Options::CLIENT_ID>();

		const util::String extensionName(
				getExtensionName(request.optional_), alloc);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		GS_TRACE_INFO(
				DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"[PutContainerHandler PartitionId = " << request.fixed_.pId_ <<
				", stmtId = " << request.fixed_.cxtSrc_.stmtId_);


		request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT; 
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;

		TransactionContext &txn = transactionManager_->putNoExpire(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow);
		if (txn.getPartitionId() != request.fixed_.pId_) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_TM_TRANSACTION_MODE_INVALID,
					"Invalid context exist request pId=" << request.fixed_.pId_ <<
					", txn pId=" << txn.getPartitionId());
		}

		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);
		if (!checkPrivilege(
				ev.getType(),
				connOption.userType_, connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
				ANY_CONTAINER,
				request.optional_.get<Options::CONTAINER_ATTRIBUTE>())) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container attribute : " <<
					request.optional_.get<Options::CONTAINER_ATTRIBUTE>());
		}
		const FullContainerKey containerKey(
				alloc, getKeyConstraint(request.optional_),
				containerNameBinary.data(), containerNameBinary.size());
		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
				, isNewSql
				);
		if (containerName.compare(GS_USERS) == 0 ||
				containerName.compare(GS_DATABASES) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container name : " <<
					request.optional_.get<Options::DB_NAME>());
		}

		{
			bool isAllowExpiration = true;
			ContainerAutoPtr containerAutoPtr(
					txn, dataStore_,
					txn.getPartitionId(), containerKey, containerType,
					getCaseSensitivity(request).isContainerNameCaseSensitive(), isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			if (container != NULL && !container->isExpired(txn) && container->hasUncommitedTransaction(txn)) {
				try {
					container->getContainerCursor(txn);
					GS_TRACE_INFO(
						DATASTORE_BACKGROUND, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Continue to change shema");
				}
				catch (UserException &e) {
					GS_TRACE_INFO(TRANSACTION_SERVICE,
							GS_TRACE_TXN_WAIT_FOR_TRANSACTION_END,
							"Container has uncommited transactions" <<
							" (pId=" << request.fixed_.pId_ <<
							", stmtId=" << request.fixed_.cxtSrc_.stmtId_ <<
							", eventType=" << request.fixed_.stmtType_ <<
							", clientId=" << request.fixed_.clientId_ <<
							", containerId=" << container->getContainerId() <<
							", pendingCount=" << ev.getQueueingCount() << ")");
					DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
						"change schema(pId=" << txn.getPartitionId()
							<< ", containerId=" << container->getContainerId()
							<< ", txnId=" << txn.getId() << ")");
				}
			}
		}


		DataStore::PutStatus putStatus;
		{
			if (request.optional_.get<Options::FEATURE_VERSION>() == MessageSchema::DEFAULT_VERSION) {
			util::XArrayOutStream<> arrayOut(containerInfo);
			util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
			int32_t containerAttribute =
					request.optional_.get<Options::CONTAINER_ATTRIBUTE>();
			out << containerAttribute;
			}
		}
		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				containerKey, containerType,
				static_cast<uint32_t>(containerInfo.size()), containerInfo.data(),
				modifiable,
				request.optional_.get<Options::FEATURE_VERSION>(),
				putStatus,
				getCaseSensitivity(request).isContainerNameCaseSensitive());
		BaseContainer *container = containerAutoPtr.getBaseContainer();

		request.fixed_.cxtSrc_.containerId_ = container->getContainerId();
		txn.setContainerId(container->getContainerId());
		txn.setSenderND(ev.getSenderND());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);
		bool isImmediate = false;
		ContainerCursor containerCursor(isImmediate);
		if (putStatus != DataStore::UPDATE) {
			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			response.binaryData2_.assign(
				static_cast<const uint8_t*>(containerNameBinary.data()),
				static_cast<const uint8_t*>(containerNameBinary.data()) + containerNameBinary.size());

			const bool optionIncluded = true;
			bool internalOptionIncluded = true;
			container->getContainerInfo(txn, response.binaryData_, optionIncluded, internalOptionIncluded);

			if (putStatus == DataStore::CREATE || putStatus == DataStore::CHANGE_PROPERY) {
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutContainerLog(
						*log,
						txn.getPartitionId(), txn.getClientId(), txn.getId(),
						response.containerId_, request.fixed_.cxtSrc_.stmtId_,
						static_cast<uint32_t>(containerNameBinary.size()),
						static_cast<const uint8_t*>(containerNameBinary.data()),
						response.binaryData_,
						container->getContainerType(),
						static_cast<uint32_t>(extensionName.size()),
						extensionName.c_str(),
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_, true, false, MAX_ROWID,
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

			containerCursor = container->getContainerCursor(txn);
			container->continueChangeSchema(txn, containerCursor);
			ContainerAutoPtr newContainerAutoPtr(txn, dataStore_, txn.getPartitionId(),
				containerCursor);
			BaseContainer *newContainer = newContainerAutoPtr.getBaseContainer();
			checkContainerExistence(newContainer);
			response.schemaVersionId_ = newContainer->getVersionId();
			response.containerId_ = newContainer->getContainerId();
			response.binaryData2_.assign(
				static_cast<const uint8_t*>(containerNameBinary.data()),
				static_cast<const uint8_t*>(containerNameBinary.data()) + containerNameBinary.size());

			const bool optionIncluded = true;
			bool internalOptionIncluded = true;
			newContainer->getContainerInfo(txn, response.binaryData_, optionIncluded, internalOptionIncluded);
			{
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutContainerLog(
						*log,
						txn.getPartitionId(), txn.getClientId(), txn.getId(),
						response.containerId_, request.fixed_.cxtSrc_.stmtId_,
						static_cast<uint32_t>(containerNameBinary.size()),
						static_cast<const uint8_t*>(containerNameBinary.data()),
						response.binaryData_,
						newContainer->getContainerType(),
						static_cast<uint32_t>(extensionName.size()),
						extensionName.c_str(),
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_, true, false, containerCursor.getRowId(),
						(newContainer->getExpireType() == TABLE_EXPIRE));

				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}
			{
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putContinueAlterContainerLog(
						*log,
						txn.getPartitionId(), txn.getClientId(), txn.getId(),
						txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
						txn.getTransationTimeoutInterval(),
						request.fixed_.cxtSrc_.getMode_, true, false, containerCursor.getRowId());

				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				logRecordList.push_back(log);
			}
		}

		int32_t replicationMode = transactionManager_->getReplicationMode();
		ReplicationContext::TaskStatus taskStatus = ReplicationContext::TASK_FINISHED;
		if (putStatus == DataStore::CREATE || putStatus == DataStore::CHANGE_PROPERY ||
			containerCursor.isFinished()) {
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putCommitTransactionLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->commit(txn, *container);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);

			logRecordList.push_back(log);

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		}
		else if (putStatus == DataStore::NOT_EXECUTED) {
			transactionManager_->commit(txn, *container);
			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		}
		else {
			replicationMode = TransactionManager::REPLICATION_SEMISYNC;
			taskStatus = ReplicationContext::TASK_CONTINUE;

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				replicationMode, taskStatus, closedResourceIds.data(),
				closedResourceIds.size(), logRecordList.data(),
				logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
				response);

		if (putStatus != DataStore::UPDATE || containerCursor.isFinished()) {
			replySuccess(
					ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response, ackWait);
		}
		else {
			continueEvent(
					ec, alloc, ev.getSenderND(), ev.getType(),
					request.fixed_.cxtSrc_.stmtId_, request, response, ackWait);
		}
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void PutContainerHandler::setContainerAttributeForCreate(OptionSet &optionSet) {
	if (optionSet.get<Options::CONTAINER_ATTRIBUTE>() >= CONTAINER_ATTR_ANY) {
		optionSet.set<Options::CONTAINER_ATTRIBUTE>(CONTAINER_ATTR_SINGLE);
	}
}

void PutLargeContainerHandler::setContainerAttributeForCreate(OptionSet &optionSet) {
	if (optionSet.get<Options::CONTAINER_ATTRIBUTE>() >= CONTAINER_ATTR_ANY) {
		optionSet.set<Options::CONTAINER_ATTRIBUTE>(CONTAINER_ATTR_SINGLE);
	}
}

/*!
	@brief Handler Operator
*/
void DropContainerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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
		ContainerType containerType;

		decodeVarSizeBinaryData(in, containerNameBinary);
		decodeEnumData<ContainerType>(in, containerType);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);
		const FullContainerKey containerKey(
				alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false),
				containerNameBinary.data(), containerNameBinary.size());
		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
				, isNewSql
				);

		{
			bool isAllowExpiration = true;
			ContainerAutoPtr containerAutoPtr(
					txn, dataStore_,
					txn.getPartitionId(), containerKey, containerType,
					getCaseSensitivity(request).isContainerNameCaseSensitive(), isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			if (container != NULL && !checkPrivilege(
					ev.getType(),
					connOption.userType_, connOption.requestType_,
					request.optional_.get<Options::SYSTEM_MODE>(),
					request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
					container->getContainerType(), container->getAttribute())) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
						"Can not drop container attribute : " <<
						container->getAttribute());
			}
			if (container == NULL) {
				replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
				return;
			}

			response.containerId_ = container->getContainerId();
			response.binaryData2_.assign(
				containerNameBinary.data(),
				containerNameBinary.data() + containerNameBinary.size());
		}


		dataStore_->dropContainer(
				txn, txn.getPartitionId(), containerKey, containerType,
				getCaseSensitivity(request).isContainerNameCaseSensitive());

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		const LogSequentialNumber lsn = logManager_->putDropContainerLog(*log,
			txn.getPartitionId(), response.containerId_,
			static_cast<uint32_t>(containerNameBinary.size()),
			containerNameBinary.data()
		);

		partitionTable_->setLSN(txn.getPartitionId(), lsn);
		logRecordList.push_back(log);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetContainerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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
		ContainerType containerType;

		decodeVarSizeBinaryData(in, containerNameBinary);
		decodeEnumData<ContainerType>(in, containerType);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		const FullContainerKey containerKey(
				alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false),
				containerNameBinary.data(), containerNameBinary.size());
		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
				, isNewSql
				);
		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				containerKey, containerType,
				getCaseSensitivity(request).isContainerNameCaseSensitive());
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container != NULL && !checkPrivilege(
				ev.getType(),
				connOption.userType_, connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
				container->getContainerType(),
				container->getAttribute(),
				request.optional_.get<Options::CONTAINER_ATTRIBUTE>())) {
			container = NULL;
		}
		response.existFlag_ = (container != NULL);

		if (response.existFlag_) {
			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			response.binaryData2_.assign(
				containerNameBinary.data(),
				containerNameBinary.data() + containerNameBinary.size());

			const bool optionIncluded = true;
			container->getContainerInfo(
				txn, response.binaryData_, optionIncluded);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void CreateIndexHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_ = ec.getHandlerStartMonotonicTime();
		}
		request.fixed_.clientId_ = request.optional_.get<Options::CLIENT_ID>();

		IndexInfo indexInfo(alloc);

		decodeIndexInfo(in, indexInfo);
		if (ev.getType() == DELETE_INDEX) {
			decodeIntData<uint8_t>(in, indexInfo.anyNameMatches_);
			decodeIntData<uint8_t>(in, indexInfo.anyTypeMatches_);
		}

		assignIndexExtension(indexInfo, request.optional_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		EmptyAllowedKey::validate(
			KeyConstraint::getUserKeyConstraint(
				dataStore_->getValueLimitConfig().getLimitContainerNameSize()),
			indexInfo.indexName_.c_str(),
			static_cast<uint32_t>(indexInfo.indexName_.size()),
			"indexName");

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT; 
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;

		TransactionContext &txn = transactionManager_->putNoExpire(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow);
		if (txn.getPartitionId() != request.fixed_.pId_) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_TM_TRANSACTION_MODE_INVALID,
					"Invalid context exist request pId=" << request.fixed_.pId_ <<
					", txn pId=" << txn.getPartitionId());
		}

		txn.setSenderND(ev.getSenderND());

		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		bool allowExpiration = true;
		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, allowExpiration);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		const bool execute = getExecuteFlag(
				ev.getType(), getCreateDropIndexMode(request.optional_),
				txn, *container, indexInfo,
				getCaseSensitivity(request).isIndexNameCaseSensitive());

		if (!execute) {
			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
			return;
		}

		util::XArray<uint32_t> paramDataLen(alloc);
		util::XArray<const uint8_t*> paramData(alloc);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);

		bool isImmediate = false;
		IndexCursor indexCursor = IndexCursor(isImmediate);
		{
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			paramDataLen.push_back(
				static_cast<uint32_t>(indexInfo.extensionOptionSchema_.size()));
			paramData.push_back(
				indexInfo.extensionOptionSchema_.data());
			paramDataLen.push_back(
				static_cast<uint32_t>(indexInfo.extensionOptionFixedPart_.size()));
			paramData.push_back(
				indexInfo.extensionOptionFixedPart_.data());
			paramDataLen.push_back(
				static_cast<uint32_t>(indexInfo.extensionOptionVarPart_.size()));
			paramData.push_back(
				indexInfo.extensionOptionVarPart_.data());

			container->createIndex(
					txn, indexInfo, indexCursor,
					getCaseSensitivity(request).isIndexNameCaseSensitive());

			const LogSequentialNumber lsn = logManager_->putCreateIndexLog(
					*log,
					txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					indexInfo.columnIds_, indexInfo.mapType,
					static_cast<uint32_t>(indexInfo.indexName_.size()),
					indexInfo.indexName_.c_str(),
					static_cast<uint32_t>(indexInfo.extensionName_.size()),
					indexInfo.extensionName_.c_str(),
					paramDataLen, paramData,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, true, false, indexCursor.getRowId());

			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		int32_t replicationMode = transactionManager_->getReplicationMode();
		ReplicationContext::TaskStatus taskStatus = ReplicationContext::TASK_FINISHED;
		if (indexCursor.isFinished()) {
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putCommitTransactionLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->commit(txn, *container);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);

			logRecordList.push_back(log);

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		}
		else {
			replicationMode = TransactionManager::REPLICATION_SEMISYNC;
			taskStatus = ReplicationContext::TASK_CONTINUE;

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}
		GS_TRACE_INFO(
				DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"[CreateIndexHandler PartitionId = " << txn.getPartitionId() <<
				", tId = " << txn.getId() <<
				", stmtId = " << txn.getLastStatementId() <<
				", orgStmtId = " << request.fixed_.cxtSrc_.stmtId_ <<
				", createIndex isFinished = " <<
				static_cast<int32_t>(indexCursor.isFinished()));

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				replicationMode, taskStatus, closedResourceIds.data(),
				closedResourceIds.size(), logRecordList.data(),
				logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
				response);

		if (indexCursor.isFinished()) {
			replySuccess(
					ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response, ackWait);
		}
		else {
			continueEvent(
					ec, alloc, ev.getSenderND(), ev.getType(),
					request.fixed_.cxtSrc_.stmtId_, request, response, ackWait);
		}
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Continue current event
*/
void StatementHandler::continueEvent(
		EventContext &ec,
		util::StackAllocator &alloc, const NodeDescriptor &ND, EventType stmtType,
		StatementId originalStmtId, const Request &request,
		const Response &response, bool ackWait) {

	util::StackAllocator::Scope scope(alloc);

	if (!ackWait) {
		EventType continueStmtType = stmtType;
		switch (stmtType) {
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER:
			continueStmtType = CONTINUE_ALTER_CONTAINER;
			break;
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
			continueStmtType = CONTINUE_CREATE_INDEX;
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << stmtType << ")");
		}

		Event continueEvent(ec, continueStmtType, request.fixed_.pId_);
		continueEvent.setSenderND(ND);
		EventByteOutStream out = continueEvent.getOutStream();

		out << (request.fixed_.cxtSrc_.stmtId_ + 1); 
		out << request.fixed_.cxtSrc_.containerId_;
		out << request.fixed_.clientId_.sessionId_;
		encodeUUID(out, request.fixed_.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << originalStmtId;

		if (continueStmtType == CONTINUE_CREATE_INDEX) {
			OptionSet optionSet(alloc);
			setReplyOptionForContinue(optionSet, request);
			optionSet.encode(out);
		}

		ec.getEngine().add(continueEvent);
	}
}

/*!
	@brief Replies success message
*/
void StatementHandler::continueEvent(
		EventContext &ec,
		util::StackAllocator &alloc, StatementExecStatus status,
		const ReplicationContext &replContext) {
#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)             \
	"(nd=" << replContext.getConnectionND()                    \
		   << ", pId=" << replContext.getPartitionId()         \
		   << ", eventType=" << replContext.getStatementType() \
		   << ", stmtId=" << replContext.getStatementId()      \
		   << ", clientId=" << replContext.getClientId()       \
		   << ", containerId=" << replContext.getContainerId() << ")"

	util::StackAllocator::Scope scope(alloc);
	if (replContext.getConnectionND().isEmpty()) {
		return;
	}
	try {
		EventType continueStmtType = replContext.getStatementType();
		switch (replContext.getStatementType()) {
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER:
			continueStmtType = CONTINUE_ALTER_CONTAINER;
			break;
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
			continueStmtType = CONTINUE_CREATE_INDEX;
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << replContext.getStatementType() << ")");
		}

		Event continueEvent(ec, continueStmtType,
			replContext.getPartitionId());
		continueEvent.setSenderND(replContext.getConnectionND());
		EventByteOutStream out = continueEvent.getOutStream();

		out << (replContext.getStatementId() + 1);
		out << replContext.getContainerId();
		out << replContext.getClientId().sessionId_;
		encodeUUID(out, replContext.getClientId().uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << replContext.getOriginalStatementId();

		if (continueStmtType == CONTINUE_CREATE_INDEX) {
			OptionSet optionSet(alloc);
			setReplyOptionForContinue(optionSet, replContext);
			optionSet.encode(out);
		}
		ec.getEngine().add(continueEvent);
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

void DropIndexHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		IndexInfo indexInfo(alloc);

		decodeIndexInfo(in, indexInfo);
		if (ev.getType() == DELETE_INDEX) {
			decodeIntData<uint8_t>(in, indexInfo.anyNameMatches_);
			decodeIntData<uint8_t>(in, indexInfo.anyTypeMatches_);
		}

		assignIndexExtension(indexInfo, request.optional_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		bool allowExpiration = true;
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER, allowExpiration);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		const bool execute = getExecuteFlag(
				ev.getType(), getCreateDropIndexMode(request.optional_),
				txn, *container, indexInfo,
				getCaseSensitivity(request).isIndexNameCaseSensitive());

		if (!execute) {
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
			return;
		}

		util::XArray<uint32_t> paramDataLen(alloc);
		util::XArray<const uint8_t*> paramData(alloc);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		if (ev.getType() != DELETE_INDEX) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}

		paramDataLen.push_back(
			static_cast<uint32_t>(sizeof(indexInfo.anyNameMatches_)));
		paramData.push_back(&indexInfo.anyNameMatches_);
		paramDataLen.push_back(
			static_cast<uint32_t>(sizeof(indexInfo.anyTypeMatches_)));
		paramData.push_back(&indexInfo.anyTypeMatches_);

		container->dropIndex(
				txn, indexInfo,
				getCaseSensitivity(request).isIndexNameCaseSensitive());
		const LogSequentialNumber lsn = logManager_->putDropIndexLog(
				*log,
				txn.getPartitionId(), txn.getContainerId(), indexInfo.columnIds_,
				indexInfo.mapType,
				static_cast<uint32_t>(indexInfo.indexName_.size()),
				indexInfo.indexName_.c_str(),
				static_cast<uint32_t>(indexInfo.extensionName_.size()),
				indexInfo.extensionName_.c_str(), paramDataLen, paramData);
		partitionTable_->setLSN(txn.getPartitionId(), lsn);

		logRecordList.push_back(log);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

bool CreateDropIndexHandler::getExecuteFlag(EventType eventType,
											CreateDropIndexMode mode,
											TransactionContext &txn,
											BaseContainer &container,
											const IndexInfo &info,
											bool isCaseSensitive) {
	if (mode == Message::INDEX_MODE_NOSQL) {
		return true;
	}
	if (info.indexName_.size() == 0) {
		return true;
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	try {
		bool execute = true;

		const util::StackAllocator::Scope scope(alloc);

		util::Vector<IndexInfo> indexInfoList(alloc);
		container.getIndexInfoList(txn, indexInfoList);

		bool exists = false;
		for (size_t i = 0; i < indexInfoList.size() && !exists; i++) {
			exists = compareIndexName(alloc,
				info.indexName_, indexInfoList[i].indexName_,
				(eventType == CREATE_INDEX ? true : isCaseSensitive));
		}

		switch (mode) {
		case Message::INDEX_MODE_SQL_DEFAULT:
			switch (eventType) {
			case CREATE_INDEX:
				if (exists) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_INDEX_ALREADY_EXISTS,
						"Index with specified name '"
						<< info.indexName_
						<< "' already exists.");
				}
				execute = true;
				break;
			case DELETE_INDEX:
				if (!exists) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_INDEX_NOT_FOUND,
						"Index with specified name '"
						<< info.indexName_
						<< "' is not found.");
				}
				execute = true;
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
			}
			break;

		case Message::INDEX_MODE_SQL_EXISTS:
			switch (eventType) {
			case CREATE_INDEX:
				execute = !exists;
				break;
			case DELETE_INDEX:
				execute = true;
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
			}
			break;

		default:
			execute = true;
			break;
		}

		return execute;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

bool CreateDropIndexHandler::compareIndexName(util::StackAllocator &alloc,
											  const util::String &specifiedName,
											  const util::String &existingName,
											  bool isCaseSensitive) {
	if (isCaseSensitive) {
		return strcmp(specifiedName.c_str(), existingName.c_str()) == 0;
	}
	else {
		util::XArray<char8_t> normalizedSpecifiedName(alloc);
		normalizedSpecifiedName.assign(specifiedName.size()+1, 0);

		util::XArray<char8_t> normalizedExistingName(alloc);
		normalizedExistingName.assign(existingName.size()+1, 0);

		ValueProcessor::convertUpperCase(specifiedName.c_str(),
			specifiedName.size(), normalizedSpecifiedName.data());

		ValueProcessor::convertUpperCase(existingName.c_str(),
			existingName.size(), normalizedExistingName.data());

		return strcmp(normalizedSpecifiedName.data(), normalizedExistingName.data()) == 0;
	}
}

/*!
	@brief Handler Operator
*/
void CreateDropTriggerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		switch (ev.getType()) {
		case CREATE_TRIGGER: {
			TriggerInfo info(alloc);
			decodeTriggerInfo(in, info);

			TriggerHandler::checkTriggerRegistration(alloc, info);
			container->putTrigger(txn, info);

			util::XArray<uint8_t> binaryTrigger(alloc);
			TriggerInfo::encode(info, binaryTrigger);

			const LogSequentialNumber lsn = logManager_->putCreateTriggerLog(
				*log, txn.getPartitionId(), txn.getContainerId(),
				static_cast<uint32_t>(info.name_.size()), info.name_.c_str(),
				binaryTrigger);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
		} break;
		case DELETE_TRIGGER: {
			util::String triggerName(alloc);
			decodeStringData<util::String>(in, triggerName);

			container->deleteTrigger(txn, triggerName.c_str());

			const LogSequentialNumber lsn = logManager_->putDropTriggerLog(*log,
				txn.getPartitionId(), txn.getContainerId(),
				static_cast<uint32_t>(triggerName.size()), triggerName.c_str());
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		logRecordList.push_back(log);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void FlushLogHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		const PartitionGroupId pgId =
				logManager_->calcPartitionGroupId(request.fixed_.pId_);
		logManager_->writeBuffer(pgId);
		logManager_->flushFile(pgId);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void WriteLogPeriodicallyHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	EVENT_START(ec, ev, transactionManager_);

	try {
		const PartitionGroupId pgId = ec.getWorkerId();

		if (logManager_ && clusterManager_->checkRecoveryCompleted()) {
			logManager_->writeBuffer(pgId);  
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to write log periodically (pgId="
				<< ec.getWorkerId() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				<< ")");
		clusterService_->setSystemError(&e);
	}
}

/*!
	@brief Handler Operator
*/
void CreateTransactionContextHandler::operator()(EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

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

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::CREATE;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		transactionManager_->put(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CloseTransactionContextHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		util::XArray<ClientId> closedResourceIds(alloc);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			TransactionContext &txn = transactionManager_->get(
					alloc, request.fixed_.pId_, request.fixed_.clientId_);
			const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

			if (txn.isActive()) {
				BaseContainer *container = NULL;

				try {
					container =
						dataStore_->getContainer(txn, txn.getPartitionId(),
							txn.getContainerId(), ANY_CONTAINER);
				}
				catch (UserException &e) {
					UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
						"Container not found (pId="
							<< txn.getPartitionId()
							<< ", clientId=" << txn.getClientId()
							<< ", containerId=" << txn.getContainerId()
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				}
				StackAllocAutoPtr<BaseContainer> containerAutoPtr(
					txn.getDefaultAllocator(), container);

				if (containerAutoPtr.get() != NULL) {
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					const LogSequentialNumber lsn = logManager_->putAbortTransactionLog(
							*log,
							txn.getPartitionId(), txn.getClientId(),
							txn.getId(), txn.getContainerId(),
							request.fixed_.cxtSrc_.stmtId_);
					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					transactionManager_->abort(txn, *containerAutoPtr.get());
					logRecordList.push_back(log);
				}

				ec.setPendingPartitionChanged(txn.getPartitionId());
			}

			transactionManager_->remove(
					request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);
		}
		catch (ContextNotFoundException &) {
		}

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(),
				closedResourceIds.data(), closedResourceIds.size(),
				logRecordList.data(), logRecordList.size(),
				response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CommitAbortTransactionHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (isDdlTransaction(request.optional_)) {
			request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT;
		}
		else {
			request.fixed_.cxtSrc_.getMode_ = TransactionManager::GET;
		}
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_CONTINUE;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		switch (ev.getType()) {
		case COMMIT_TRANSACTION: {
			const LogSequentialNumber lsn = logManager_->putCommitTransactionLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->commit(txn, *container);
		} break;

		case ABORT_TRANSACTION: {
			const LogSequentialNumber lsn = logManager_->putAbortTransactionLog(
					*log, txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->abort(txn, *container);
		} break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		logRecordList.push_back(log);

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		util::XArray<ClientId> closedResourceIds(alloc);
		if (isDdlTransaction(request.optional_)) {
			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);
			transactionService_->incrementAbortDDLCount(ev.getPartitionId());
		}

		ec.setPendingPartitionChanged(request.fixed_.pId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(),
				closedResourceIds.data(), closedResourceIds.size(),
				logRecordList.data(), logRecordList.size(),
				response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void PutRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const uint64_t numRow = 1;
		RowData rowData(alloc);

		decodeBinaryData(in, rowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);


		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);


		const PutRowOption putRowOption =
				request.optional_.get<Options::PUT_ROW_OPTION>();

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		DataStore::PutStatus putStatus;
		container->putRow(
				txn, static_cast<uint32_t>(rowData.size()),
				rowData.data(), rowIds[0], putStatus, putRowOption);
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);
		response.existFlag_ = (putStatus == DataStore::UPDATE);

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
			const LogSequentialNumber lsn = logManager_->putPutRowLog(
					*log,
					txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
					rowData, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.push_back(TriggerService::PUT_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, 1, rowData.data(), rowData.size());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void PutRowSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		uint64_t numRow;
		RowData multiRowData(alloc);

		decodeMultipleRowData(in, numRow, multiRowData);
		assert(numRow > 0);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.fixed_.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(txn,
				request.fixed_.cxtSrc_.stmtId_, request.fixed_.cxtSrc_.getMode_,
				request.fixed_.cxtSrc_.txnMode_);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		const PutRowOption putRowOption =
				request.optional_.get<Options::PUT_ROW_OPTION>();

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
				dataStore_->getValueLimitConfig(), container->getRealColumnInfoList(txn),
				container->getRealColumnNum(txn), multiRowData.data(),
				static_cast<uint32_t>(multiRowData.size()), numRow,
				container->getRealRowFixedDataSize(txn), NO_VALIDATE_ROW_IMAGE);
		response.existFlag_ = false;

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(static_cast<size_t>(1), UNDEF_ROWID);
		util::XArray<TriggerService::OperationType> operationTypeList(alloc);
		operationTypeList.assign(
				static_cast<size_t>(1), TriggerService::PUT_ROW);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		RowData rowData(alloc);

		try {
			for (StatementId rowStmtId = request.fixed_.cxtSrc_.stmtId_;
					 inputMessageRowStore.next(); rowStmtId++) {
				try {
					transactionManager_->checkStatementAlreadyExecuted(
						txn, rowStmtId, request.fixed_.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException &e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
							"Row already put (pId=" << ev.getPartitionId() <<
							", eventType=" << ev.getType() <<
							", stmtId=" << rowStmtId <<
							", clientId=" << request.fixed_.clientId_ <<
							", containerId=" <<
							((request.fixed_.cxtSrc_.containerId_ ==
									UNDEF_CONTAINERID) ?
									0 : request.fixed_.cxtSrc_.containerId_) <<
							", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				rowData.clear();
				inputMessageRowStore.getCurrentRowData(rowData);

				rowIds[0] = UNDEF_ROWID;
				DataStore::PutStatus putStatus;
				container->putRow(txn, static_cast<uint32_t>(rowData.size()),
					rowData.data(), rowIds[0], putStatus, putRowOption);
				const bool executed = (putStatus != DataStore::NOT_EXECUTED);

				{
					const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
							TransactionManager::NO_AUTO_COMMIT_BEGIN);
					const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
							TransactionManager::AUTO_COMMIT);
					assert(!(withBegin && isAutoCommit));
					assert(rowIds.size() == 1);
					assert((executed && rowIds[0] != UNDEF_ROWID) ||
						   (!executed && rowIds[0] == UNDEF_ROWID));
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					const LogSequentialNumber lsn = logManager_->putPutRowLog(
							*log, txn.getPartitionId(), txn.getClientId(),
							txn.getId(), txn.getContainerId(), rowStmtId,
							(executed ? rowIds.size() : 0), rowIds,
							(executed ? 1 : 0), rowData,
							txn.getTransationTimeoutInterval(),
							request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					logRecordList.push_back(log);
				}

				transactionManager_->update(txn, rowStmtId);

				if (executed) {
					TriggerHandler::checkTrigger(*triggerService_, txn,
						*container, ec, operationTypeList, 1, rowData.data(),
						rowData.size());
				}
			}
		}
		catch (std::exception &) {
			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

			throw;
		}

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void RemoveRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		RowKeyData rowKey(alloc);

		decodeBinaryData(in, rowKey, true);

		const uint64_t numRow = 1;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);


		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		container->deleteRow(txn, static_cast<uint32_t>(rowKey.size()),
			rowKey.data(), rowIds[0], response.existFlag_);
		const bool executed = response.existFlag_;

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
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, (executed ? rowIds.size() : 0),
					rowIds, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::DELETE_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, NULL, 0);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void UpdateRowByIdHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const uint64_t numRow = 1;
		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		RowData rowData(alloc);

		decodeLongData<RowId>(in, rowIds[0]);
		decodeBinaryData(in, rowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);


		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);


		DataStore::PutStatus putStatus;

		container->updateRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowIds[0], putStatus);

		const bool executed = (putStatus != DataStore::NOT_EXECUTED);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert(rowIds[0] != UNDEF_ROWID);
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putUpdateRowLog(
					*log,
					txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
					rowData, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::PUT_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, rowData.data(), rowData.size());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void RemoveRowByIdHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const uint64_t numRow = 1;
		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);

		decodeLongData<RowId>(in, rowIds[0]);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);


		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		container->deleteRow(txn, rowIds[0], response.existFlag_);

		const bool executed = response.existFlag_;

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert(rowIds[0] != UNDEF_ROWID);
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putRemoveRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, (executed ? rowIds.size() : 0),
					rowIds, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::DELETE_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, NULL, 0);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void RemoveRowSetByIdHandler::operator()(EventContext &ec, Event &ev) {
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

		uint64_t numRow;
		util::XArray<RowId> rowIds(alloc);
		in >> numRow;
		rowIds.resize(static_cast<size_t>(numRow));
		for (uint64_t i = 0; i < numRow; i++) {
			in >> rowIds[i];
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.fixed_.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(
				txn,
				request.fixed_.cxtSrc_.stmtId_, request.fixed_.cxtSrc_.getMode_,
				request.fixed_.cxtSrc_.txnMode_);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();

		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);
		util::XArray<TriggerService::OperationType> operationTypeList(alloc);
		operationTypeList.assign(
			static_cast<size_t>(1), TriggerService::DELETE_ROW);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			StatementId rowCounter = 0;
			for (StatementId rowStmtId = request.fixed_.cxtSrc_.stmtId_, rowCounter = 0;
					rowCounter < numRow; rowCounter++, rowStmtId++) {
				try {
					transactionManager_->checkStatementAlreadyExecuted(
							txn, rowStmtId, request.fixed_.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException &e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
							"Row already put (pId=" << ev.getPartitionId() <<
							", eventType=" << ev.getType() <<
							", stmtId=" << rowStmtId <<
							", clientId=" << request.fixed_.clientId_ <<
							", containerId=" <<
							((request.fixed_.cxtSrc_.containerId_ ==
									UNDEF_CONTAINERID) ?
									0 : request.fixed_.cxtSrc_.containerId_) <<
							", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				container->deleteRow(txn, rowIds[rowCounter], response.existFlag_);

				const bool executed = response.existFlag_;
				{
					const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
							TransactionManager::NO_AUTO_COMMIT_BEGIN);
					const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
							TransactionManager::AUTO_COMMIT);
					assert(!(withBegin && isAutoCommit));
					assert(numRow == rowIds.size());
					assert(rowIds[0] != UNDEF_ROWID);
					util::XArray<uint8_t> *log =
							ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					util::XArray<RowId> tmpRowIds(alloc);
					tmpRowIds.push_back(rowIds[rowCounter]);
					const LogSequentialNumber lsn = logManager_->putRemoveRowLog(
							*log, txn.getPartitionId(),
							txn.getClientId(), txn.getId(), txn.getContainerId(),
							request.fixed_.cxtSrc_.stmtId_, (executed ? 1 : 0),
							tmpRowIds, txn.getTransationTimeoutInterval(),
							request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					logRecordList.push_back(log);
				}

				transactionManager_->update(txn, rowStmtId);
				if (executed) {
					util::XArray<TriggerService::OperationType> operationTypeList(
						alloc);
					size_t checkSize = 1;
					operationTypeList.assign(checkSize, TriggerService::DELETE_ROW);
					TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
						operationTypeList, 1, NULL, 0);
				}
			}
		}
		catch (std::exception &) {
			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);
			throw;
		}
		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void UpdateRowSetByIdHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		uint64_t numRow;
		RowData multiRowData(alloc);
		util::XArray<RowId> targetRowIds(alloc);
		in >> numRow;
		targetRowIds.resize(static_cast<size_t>(numRow));
		for (uint64_t i = 0; i < numRow; i++) {
			in >> targetRowIds[i];
		}
		decodeBinaryData(in, multiRowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.fixed_.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(txn,
				request.fixed_.cxtSrc_.stmtId_, request.fixed_.cxtSrc_.getMode_,
				request.fixed_.cxtSrc_.txnMode_);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
			dataStore_->getValueLimitConfig(), container->getRealColumnInfoList(txn),
			container->getRealColumnNum(txn), multiRowData.data(),
			static_cast<uint32_t>(multiRowData.size()), numRow,
			container->getRealRowFixedDataSize(txn), NO_VALIDATE_ROW_IMAGE);
		response.existFlag_ = false;

		util::XArray<TriggerService::OperationType> operationTypeList(alloc);
		operationTypeList.assign(
			static_cast<size_t>(1), TriggerService::PUT_ROW);
		util::XArray<RowId> rowIds(alloc);
		RowData rowData(alloc);
		StatementId rowPos = 0;
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			for (StatementId rowStmtId = request.fixed_.cxtSrc_.stmtId_;
					 inputMessageRowStore.next(); rowStmtId++, rowPos++) {
				try {
					transactionManager_->checkStatementAlreadyExecuted(
							txn, rowStmtId, request.fixed_.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException &e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
							"Row already put (pId=" << ev.getPartitionId() <<
							", eventType=" << ev.getType() <<
							", stmtId=" << rowStmtId <<
							", clientId=" << request.fixed_.clientId_ <<
							", containerId=" <<
							((request.fixed_.cxtSrc_.containerId_ ==
									UNDEF_CONTAINERID) ?
									0 : request.fixed_.cxtSrc_.containerId_) <<
							", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				rowIds.clear();
				rowIds.push_back(targetRowIds[rowPos]);
				rowData.clear();
				inputMessageRowStore.getCurrentRowData(rowData);

				DataStore::PutStatus putStatus;

				container->updateRow(txn, static_cast<uint32_t>(rowData.size()),
					rowData.data(), rowIds[0], putStatus);

				const bool executed = (putStatus != DataStore::NOT_EXECUTED);

				{
					const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
							TransactionManager::NO_AUTO_COMMIT_BEGIN);
					const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
							TransactionManager::AUTO_COMMIT);
					util::XArray<uint8_t> *log =
							ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					const LogSequentialNumber lsn = logManager_->putUpdateRowLog(
							*log, txn.getPartitionId(), txn.getClientId(),
							txn.getId(), txn.getContainerId(), rowStmtId,
							(executed ? 1 : 0), rowIds,
							(executed ? 1 : 0), rowData,
							txn.getTransationTimeoutInterval(),
							request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					logRecordList.push_back(log);
				}

				transactionManager_->update(txn, rowStmtId);

				if (executed) {
					TriggerHandler::checkTrigger(*triggerService_, txn,
						*container, ec, operationTypeList, 1, rowData.data(),
						rowData.size());
				}
			}
		}
		catch (std::exception &) {
			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

			throw;
		}

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		RowKeyData rowKey(alloc);

		decodeBinaryData(in, rowKey, true);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::get(txn, *container,
				static_cast<uint32_t>(rowKey.size()), rowKey.data(), *response.rs_);
			if (forUpdate) {
				container->getLockRowIdList(txn, *response.rs_, lockedRowId);
			}
			QueryProcessor::fetch(
				txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
			response.existFlag_ = (response.rs_->getResultNum() > 0);
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (forUpdate) {
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putLockRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		FetchOption fetchOption;
		decodeFetchOption(in, fetchOption);

		bool isPartial;
		PartialQueryOption partialQueryOption(alloc);
		decodePartialQueryOption(in, alloc, isPartial, partialQueryOption);

		RowId position;
		decodeLongData<RowId>(in, position);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::get(txn, *container, position, fetchOption.limit_,
				response.last_, *response.rs_);

			QueryProcessor::fetch(
				txn, *container, 0, fetchOption.size_, response.rs_);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTqlHandler::operator()(EventContext &ec, Event &ev)
{
	TXN_TRACE_HANDLER_CALLED(ev);

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

		FetchOption fetchOption;
		bool isPartial;
		PartialQueryOption partialQueryOption(alloc);
		util::String query(alloc);

		decodeFetchOption(in, fetchOption);
		decodePartialQueryOption(in, alloc, isPartial, partialQueryOption);
		decodeStringData<util::String>(in, query);

	    FullContainerKey *containerKey = NULL;
		const QueryContainerKey *containerKeyOption =
				request.optional_.get<Options::QUERY_CONTAINER_KEY>();
		const QueryContainerKey *distInfo =
				request.optional_.get<Options::DIST_QUERY>();
		if (distInfo != NULL && distInfo->enabled_) {
			containerKeyOption = distInfo;
		}
		if (containerKeyOption != NULL && containerKeyOption->enabled_) {
			const util::XArray<uint8_t> &keyBinary =
					containerKeyOption->containerKeyBinary_;
			containerKey = ALLOC_NEW(alloc) FullContainerKey(
					alloc, getKeyConstraint(request.optional_),
					keyBinary.data(), keyBinary.size());
		}

		const ContainerId metaContainerId =
				request.optional_.get<Options::META_CONTAINER_ID>();
		bool nodeDistribution = false;
		if (metaContainerId != UNDEF_CONTAINERID) {
			const bool forCore = true;
			const MetaType::InfoTable &infoTable =
					MetaType::InfoTable::getInstance();
			const MetaContainerInfo *info =
					infoTable.findInfo(metaContainerId, forCore);
			if (info != NULL) {
				nodeDistribution = info->nodeDistribution_;
			}
		}

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = nodeDistribution ?
				PROLE_ANY :
				(forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP));
		const PartitionStatus partitionStatus = nodeDistribution ?
				PSTATE_ANY : PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkQueryOption(request.optional_, fetchOption, isPartial, true);



		if (metaContainerId != UNDEF_CONTAINERID) {
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

			util::AllocUniquePtr<DataStore::Latch> latch(NULL, alloc);
			if (!nodeDistribution) {
				latch.reset(ALLOC_NEW(alloc) DataStore::Latch(
						txn, txn.getPartitionId(), dataStore_, clusterService_));
			}
			MetaStore metaStore(*dataStore_);

			MetaType::NamingType columnNamingType;
			columnNamingType = static_cast<MetaType::NamingType>(
					request.optional_.get<Options::META_NAMING_TYPE>());

			MetaContainer *metaContainer = metaStore.getContainer(
					txn, connOption.dbId_, metaContainerId,
					MetaType::NAMING_NEUTRAL, columnNamingType);

			response.rs_ = dataStore_->createResultSet(
					txn, txn.getContainerId(),
					metaContainer->getVersionId(), emNow, NULL);
			const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

			MetaProcessorSource source(
					connOption.dbId_, connOption.dbName_.c_str());
			source.dataStore_ = dataStore_;
			source.eventContext_= &ec;
			source.transactionManager_ = transactionManager_;
			source.transactionService_ = transactionService_;
			source.partitionTable_ = partitionTable_;
			source.sqlService_ = sqlService_;
			source.sqlExecutionManager_ = resourceSet_->getSQLExecutionManager();
			source.resourceSet_ = resourceSet_;

			QueryProcessor::executeMetaTQL(
					txn, *metaContainer, source, fetchOption.limit_,
					TQLInfo(
							request.optional_.get<Options::DB_NAME>(),
							containerKey, query.c_str()),
					*response.rs_);

			replySuccess(
					ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
			return;
		}

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		ResultSetOption queryOption;
		createResultSetOption(
				request.optional_, NULL, isPartial, partialQueryOption,
				queryOption);
		response.rs_ = dataStore_->createResultSet(
				txn, txn.getContainerId(), container->getVersionId(), emNow,
				&queryOption);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::executeTQL(
					txn, *container, fetchOption.limit_,
					TQLInfo(
							request.optional_.get<Options::DB_NAME>(),
							containerKey, query.c_str()),
					*response.rs_);

			if (forUpdate) {
				container->getLockRowIdList(txn, *response.rs_, lockedRowId);
			}

			QueryProcessor::fetch(
				txn, *container, 0, fetchOption.size_, response.rs_);
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (forUpdate) {
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putLockRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}


/*!
	@brief Handler Operator
*/
void AppendRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		const uint64_t numRow = 1;
		RowData rowData(alloc);

		decodeBinaryData(in, rowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		RowId rowKey = UNDEF_ROWID;
		DataStore::PutStatus putStatus;
		container->appendRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowKey, putStatus);
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);
		response.existFlag_ = (putStatus == DataStore::UPDATE);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			util::XArray<RowId> rowIds(alloc);
			rowIds.push_back(rowKey);
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(rowIds.size() == numRow);
			assert((executed && rowKey != UNDEF_ROWID) ||
				   (!executed && rowKey == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putPutRowLog(
					*log,
					txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
					(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
					rowData, txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(), NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::PUT_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, rowData.data(), rowData.size());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void QueryGeometryRelatedHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		FetchOption fetchOption;
		GeometryQuery query(alloc);

		decodeFetchOption(in, fetchOption);
		bool isPartial;
		PartialQueryOption partialQueryOption(alloc);
		decodePartialQueryOption(in, alloc, isPartial, partialQueryOption);
		decodeGeometryRelatedQuery(in, query);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, COLLECTION_CONTAINER);
		Collection *container = containerAutoPtr.getCollection();

		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::searchGeometryRelated(txn, *container,
				fetchOption.limit_, query.columnId_,
				static_cast<uint32_t>(query.intersection_.size()),
				query.intersection_.data(), query.operator_, *response.rs_);

			if (forUpdate) {
				container->getLockRowIdList(txn, *response.rs_, lockedRowId);
			}

			QueryProcessor::fetch(
				txn, *container, 0, fetchOption.size_, response.rs_);
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (forUpdate) {
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putLockRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void QueryGeometryWithExclusionHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		FetchOption fetchOption;
		GeometryQuery query(alloc);

		decodeFetchOption(in, fetchOption);
		bool isPartial;
		PartialQueryOption partialQueryOption(alloc);
		decodePartialQueryOption(in, alloc, isPartial, partialQueryOption);
		decodeGeometryWithExclusionQuery(in, query);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, COLLECTION_CONTAINER);
		Collection *container = containerAutoPtr.getCollection();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::searchGeometry(txn, *container, fetchOption.limit_,
				query.columnId_, static_cast<uint32_t>(query.intersection_.size()),
				query.intersection_.data(),
				static_cast<uint32_t>(query.disjoint_.size()),
				query.disjoint_.data(), *response.rs_);

			if (forUpdate) {
				container->getLockRowIdList(txn, *response.rs_, lockedRowId);
			}

			QueryProcessor::fetch(
				txn, *container, 0, fetchOption.size_, response.rs_);
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (forUpdate) {
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putLockRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowTimeRelatedHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		TimeRelatedCondition condition;

		decodeTimeRelatedConditon(in, condition);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::get(txn, *container, condition.rowKey_,
				condition.operator_, *response.rs_);

			QueryProcessor::fetch(
				txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
			response.existFlag_ = (response.rs_->getResultNum() > 0);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowInterpolateHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		InterpolateCondition condition;

		decodeInterpolateConditon(in, condition);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::interpolate(txn, *container, condition.rowKey_,
				condition.columnId_, *response.rs_);

			QueryProcessor::fetch(
				txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
			response.existFlag_ = (response.rs_->getResultNum() > 0);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void AggregateHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		AggregateQuery query;

		decodeAggregateQuery(in, query);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::aggregate(txn, *container, query.start_, query.end_,
			query.columnId_, query.aggregationType_, *response.rs_);

			QueryProcessor::fetch(
				txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
			response.existFlag_ = (response.rs_->getResultNum() > 0);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTimeRangeHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		FetchOption fetchOption;
		RangeQuery query;

		decodeFetchOption(in, fetchOption);
		bool isPartial;
		PartialQueryOption partialQueryOption(alloc);
		decodePartialQueryOption(in, alloc, isPartial, partialQueryOption);
		decodeRangeQuery(in, query);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::search(txn, *container, query.order_,
				fetchOption.limit_, query.start_, query.end_, *response.rs_);

			if (forUpdate) {
				container->getLockRowIdList(txn, *response.rs_, lockedRowId);
			}

			QueryProcessor::fetch(
				txn, *container, 0, fetchOption.size_, response.rs_);
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (forUpdate) {
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putLockRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTimeSamplingHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		FetchOption fetchOption;
		SamplingQuery query(alloc);

		decodeFetchOption(in, fetchOption);
		bool isPartial;
		PartialQueryOption partialQueryOption(alloc);
		decodePartialQueryOption(in, alloc, isPartial, partialQueryOption);
		decodeSamplingQuery(in, query);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				request.fixed_.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::sample(txn, *container, fetchOption.limit_,
			query.start_, query.end_, query.toSamplingOption(), *response.rs_);

			if (forUpdate) {
				container->getLockRowIdList(txn, *response.rs_, lockedRowId);
			}

			QueryProcessor::fetch(
				txn, *container, 0, fetchOption.size_, response.rs_);
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (forUpdate) {
			const bool withBegin = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit = (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putLockRowLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.fixed_.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(), response);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void FetchResultSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		ResultSetId rsId;
		decodeLongData<ResultSetId>(in, rsId);

		ResultSize startPos, fetchNum;
		decodeLongData<ResultSize>(in, startPos);
		decodeLongData<ResultSize>(in, fetchNum);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.fixed_.schemaVersionId_);

		response.rs_ = dataStore_->getResultSet(txn, rsId);
		if (response.rs_ == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_DS_RESULT_ID_INVALID, "(rsId=" << rsId << ")");
		}

		const ResultSetGuard rsGuard(txn, *dataStore_, *response.rs_);

		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			QueryProcessor::fetch(
				txn, *container, startPos, fetchNum, response.rs_);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->addRowReadCount(
				ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CloseResultSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		ResultSetId rsId;
		decodeLongData<ResultSetId>(in, rsId);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		dataStore_->closeResultSet(request.fixed_.pId_, rsId);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void MultiCreateTransactionContextHandler::operator()(
	EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	util::TimeZone timeZone;

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		util::XArray<SessionCreationEntry> entryList(alloc);

		const bool withId = decodeMultiTransactionCreationEntry(in, entryList);

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

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
				reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);
		encodeIntData<uint32_t>(out, static_cast<uint32_t>(entryList.size()));
		bool isNewSql = isNewSQL(request);
		for (size_t i = 0; i < entryList.size(); i++) {
			SessionCreationEntry &entry = entryList[i];

			const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);

			if (!withId) {
				const util::StackAllocator::Scope scope(alloc);

				assert(entry.containerName_ != NULL);
				const TransactionManager::ContextSource src(request.fixed_.stmtType_);
				TransactionContext &txn = transactionManager_->put(
						alloc, request.fixed_.pId_, clientId, src, now, emNow);
				const FullContainerKey containerKey(
						alloc, getKeyConstraint(CONTAINER_ATTR_ANY),
						entry.containerName_->data(), entry.containerName_->size());

				checkLoggedInDatabase(
						connOption.dbId_, connOption.dbName_.c_str(),
						containerKey.getComponents(alloc).dbId_,
						request.optional_.get<Options::DB_NAME>()
						, isNewSql
						);
				ContainerAutoPtr containerAutoPtr(
						txn, dataStore_,
						txn.getPartitionId(), containerKey, ANY_CONTAINER,
						getCaseSensitivity(request).isContainerNameCaseSensitive());
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				if (container != NULL && !checkPrivilege(
						ev.getType(),
						connOption.userType_, connOption.requestType_,
						request.optional_.get<Options::SYSTEM_MODE>(),
						request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
						container->getContainerType(),
						container->getAttribute(), entry.containerAttribute_)) {
					container = NULL;
				}
				checkContainerExistence(container);
				entry.containerId_ = container->getContainerId();
			}

			const TransactionManager::ContextSource cxtSrc(
					request.fixed_.stmtType_,
					request.fixed_.cxtSrc_.stmtId_, entry.containerId_,
					TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
					TransactionManager::CREATE, TransactionManager::AUTO_COMMIT,
					false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_, clientId, cxtSrc, now, emNow);

			encodeLongData<ContainerId>(out, txn.getContainerId());
		}

		ec.getEngine().send(reply, ev.getSenderND());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

bool MultiCreateTransactionContextHandler::decodeMultiTransactionCreationEntry(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<SessionCreationEntry> &entryList) {
	try {
		util::StackAllocator &alloc = *entryList.get_allocator().base();

		bool withId;
		decodeBooleanData(in, withId);

		int32_t entryCount;
		in >> entryCount;
		entryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			SessionCreationEntry entry;

			OptionSet optionSet(alloc);
			decodeOptionPart(in, optionSet);
			entry.containerAttribute_ =
					optionSet.get<Options::CONTAINER_ATTRIBUTE>();

			if (withId) {
				in >> entry.containerId_;
			}
			else {
				entry.containerName_ = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeVarSizeBinaryData(in, *entry.containerName_);
			}

			in >> entry.sessionId_;

			entryList.push_back(entry);
		}

		return withId;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiCloseTransactionContextHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		util::XArray<SessionCloseEntry> entryList(alloc);

		decodeMultiTransactionCloseEntry(in, entryList);

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

		Event reply(ec, ev.getType(), ev.getPartitionId());

		encodeCommonPart(
				reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		util::XArray<ClientId> closedResourceIds(alloc);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		for (size_t i = 0; i < entryList.size(); i++) {
			const SessionCloseEntry &entry = entryList[i];

			const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);

			try {
				TransactionContext &txn = transactionManager_->get(
						alloc, request.fixed_.pId_, clientId);
				const DataStore::Latch latch(
						txn, txn.getPartitionId(), dataStore_, clusterService_);

				if (txn.isActive()) {
					BaseContainer *container = NULL;

					try {
						container =
							dataStore_->getContainer(txn, txn.getPartitionId(),
								txn.getContainerId(), ANY_CONTAINER);
					}
					catch (UserException &e) {
						UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
							"Container not found. (pId="
								<< txn.getPartitionId()
								<< ", clientId=" << txn.getClientId()
								<< ", containerId=" << txn.getContainerId()
								<< ", reason=" << GS_EXCEPTION_MESSAGE(e)
								<< ")");
					}
					StackAllocAutoPtr<BaseContainer> containerAutoPtr(
						txn.getDefaultAllocator(), container);
					if (containerAutoPtr.get() != NULL) {
						util::XArray<uint8_t> *log =
							ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						const LogSequentialNumber lsn =
							logManager_->putAbortTransactionLog(*log,
								txn.getPartitionId(), txn.getClientId(),
								txn.getId(), txn.getContainerId(),
								entry.stmtId_);
						partitionTable_->setLSN(txn.getPartitionId(), lsn);
						logRecordList.push_back(log);

						transactionManager_->abort(
							txn, *containerAutoPtr.get());
					}

					ec.setPendingPartitionChanged(txn.getPartitionId());
				}

				transactionManager_->remove(request.fixed_.pId_, clientId);
				closedResourceIds.push_back(clientId);
			}
			catch (ContextNotFoundException &) {
			}
		}

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(),
				closedResourceIds.data(), closedResourceIds.size(),
				logRecordList.data(), logRecordList.size(), response);

		if (!ackWait) {
			ec.getEngine().send(reply, ev.getSenderND());
		}
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void MultiCloseTransactionContextHandler::decodeMultiTransactionCloseEntry(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<SessionCloseEntry> &entryList) {
	try {
		int32_t entryCount;
		in >> entryCount;
		entryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			SessionCloseEntry entry;

			in >> entry.stmtId_;
			in >> entry.containerId_;
			in >> entry.sessionId_;

			entryList.push_back(entry);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleExecuteError(
		util::StackAllocator &alloc,
		const Event &ev, const Request &wholeRequest,
		EventType stmtType, PartitionId pId, const ClientId &clientId,
		const TransactionManager::ContextSource &src, Progress &progress,
		std::exception &e, const char8_t *executionName) {

	util::TimeZone timeZone;
	util::String containerName(alloc);
	ExceptionParameterList paramList(alloc);
	try {
		const TransactionManager::ContextSource subSrc(
				stmtType, 0, src.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
		TransactionContext &txn = transactionManager_->put(
				alloc, pId, TXN_EMPTY_CLIENTID, subSrc, 0, 0);

		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);
		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);

		BaseContainer *container = containerAutoPtr.getBaseContainer();

		if (container != NULL) {
			container->getContainerKey(txn).toString(alloc, containerName);
			paramList.push_back(ExceptionParameterEntry(
					"container", containerName.c_str()));
		}
	}
	catch (std::exception &e) {
		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e, "");
	}

	try {
		MultiOpErrorMessage errorMessage(
				e, "", ev, wholeRequest,
				stmtType, pId, clientId, src,
				containerName.c_str(), executionName);

		try {
			throw;
		}
		catch (StatementAlreadyExecutedException&) {
			UTIL_TRACE_EXCEPTION_INFO(
					TRANSACTION_SERVICE, e,
					errorMessage.withDescription("Statement already executed"));
			progress.containerResult_.push_back(
					CONTAINER_RESULT_ALREADY_EXECUTED);
		}
		catch (LockConflictException &e2) {
			UTIL_TRACE_EXCEPTION_INFO(
					TRANSACTION_SERVICE, e,
					errorMessage.withDescription("Lock conflicted"));

			if (src.txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN) {
				try {
					TransactionContext &txn =
						transactionManager_->get(alloc, pId, clientId);
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						progress.logRecordList_.push_back(log);

						if (stmtType == PUT_MULTIPLE_CONTAINER_ROWS) {
							assert(
								progress.mputStartStmtId_ != UNDEF_STATEMENTID);
							transactionManager_->update(
								txn, progress.mputStartStmtId_);
						}
					}
				}
				catch (ContextNotFoundException&) {
					UTIL_TRACE_EXCEPTION_INFO(
							TRANSACTION_SERVICE, e,
							errorMessage.withDescription("Context not found"));
				}
			}

			try {
				checkLockConflictStatus(progress.lockConflictStatus_, e2);
			}
			catch (LockConflictException&) {
				assert(false);
				throw;
			}
			catch (...) {
				handleExecuteError(
						alloc, ev, wholeRequest, stmtType, pId, clientId, src,
						progress, e, executionName);
				return;
			}
			progress.lockConflicted_ = true;
		}
		catch (std::exception&) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				GS_RETHROW_SYSTEM_ERROR(
						e, errorMessage.withDescription("System error"));
			}
			else {
				UTIL_TRACE_EXCEPTION(
						TRANSACTION_SERVICE, e,
						errorMessage.withDescription("User error"));

				try {
					TransactionContext &txn =
						transactionManager_->get(alloc, pId, clientId);
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						progress.logRecordList_.push_back(log);
					}
				}
				catch (ContextNotFoundException &) {
					UTIL_TRACE_EXCEPTION_INFO(
							TRANSACTION_SERVICE, e,
							errorMessage.withDescription("Context not found"));
				}

				progress.lastExceptionData_.clear();
				util::XArrayByteOutStream out(
						util::XArrayOutStream<>(progress.lastExceptionData_));
				encodeException(
						out, e, TXN_DETAIL_EXCEPTION_HIDDEN, &paramList);

				progress.containerResult_.push_back(CONTAINER_RESULT_FAIL);
			}
		}
	}
	catch (std::exception&) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleWholeError(
		EventContext &ec, util::StackAllocator &alloc,
		const Event &ev, const Request &request, std::exception &e) {
	ErrorMessage errorMessage(e, "", ev, request);
	try {
		throw;
	}
	catch (EncodeDecodeException &) {
		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
						"Failed to encode or decode message"));
		replyError(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_ERROR, request, e);
	}
	catch (DenyException &) {
		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
						"Request denied"));
		replyError(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);
	}
	catch (std::exception &) {
		const bool isCritical = GS_EXCEPTION_CHECK_CRITICAL(e);
		const char8_t *description = (isCritical ? "System error" : "User error");
		const StatementExecStatus returnStatus =
				(isCritical ? TXN_STATEMENT_NODE_ERROR : TXN_STATEMENT_ERROR);

		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
						description));
		replyError(
				ec, alloc, ev.getSenderND(), ev.getType(), returnStatus,
				request, e);

		if (isCritical) {
			clusterService_->setSystemError(&e);
		}
	}
}

void MultiStatementHandler::decodeContainerOptionPart(
		EventByteInStream &in, const FixedRequest &fixedRequest,
		OptionSet &optionSet) {
	try {
		optionSet.set<Options::TXN_TIMEOUT_INTERVAL>(-1);
		decodeOptionPart(in, optionSet);

		if (optionSet.get<Options::TXN_TIMEOUT_INTERVAL>() < 0) {
			optionSet.set<Options::TXN_TIMEOUT_INTERVAL>(
					fixedRequest.cxtSrc_.txnTimeoutInterval_);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiStatementHandler::MultiOpErrorMessage::format(
		std::ostream &os) const {
	const ErrorMessage::Elements &elements = base_.elements_;

	os << GS_EXCEPTION_MESSAGE(elements.cause_);
	os << " (" << elements.description_;
	os << ", containerName=" << containerName_;
	os << ", executionName=" << executionName_;
	base_.formatParameters(os);
	os << ")";
}

/*!
	@brief Handler Operator
*/
void MultiPutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		util::XArray<const MessageSchema *> schemaList(alloc);
		util::XArray<const RowSetRequest *> rowSetList(alloc);

		decodeMultiSchema(in, schemaList);
		decodeMultiRowSet(in, request, schemaList, rowSetList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		Event reply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
		encodeCommonPart(reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		CheckedSchemaIdSet checkedSchemaIdSet(alloc);
		checkedSchemaIdSet.reserve(rowSetList.size());
		Progress progress(alloc, getLockConflictStatus(ev, emNow, request));

		const PutRowOption putRowOption =
				request.optional_.get<Options::PUT_ROW_OPTION>();

		bool succeeded = true;
		for (size_t i = 0; i < rowSetList.size(); i++) {
			const RowSetRequest &rowSetRequest = *(rowSetList[i]);
			const MessageSchema &messageSchema =
				*(schemaList[rowSetRequest.schemaIndex_]);

			execute(
					ec, ev, request, rowSetRequest, messageSchema,
					checkedSchemaIdSet, progress, putRowOption);

			if (!progress.lastExceptionData_.empty() ||
				progress.lockConflicted_) {
				succeeded = false;
				break;
			}
		}

		bool ackWait = false;
		{
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_, request.fixed_.clientId_,
					request.fixed_.cxtSrc_, now, emNow);

			const int32_t replMode = succeeded ?
					transactionManager_->getReplicationMode() :
					TransactionManager::REPLICATION_ASYNC;
			const NodeDescriptor &clientND = succeeded ?
					ev.getSenderND() : NodeDescriptor::EMPTY_ND;

			ackWait = executeReplication(
					request, ec, alloc, clientND, txn,
					request.fixed_.stmtType_, request.fixed_.cxtSrc_.stmtId_, replMode, NULL, 0,
					progress.logRecordList_.data(), progress.logRecordList_.size(),
					response);
		}

		if (progress.lockConflicted_) {
			retryLockConflictedRequest(ec, ev, progress.lockConflictStatus_);
		}
		else if (succeeded) {
			if (!ackWait) {
				ec.getEngine().send(reply, ev.getSenderND());
			}
		}
		else {
			Event errorReply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
			EventByteOutStream out = encodeCommonPart(
					errorReply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
			out.writeAll(progress.lastExceptionData_.data(),
					progress.lastExceptionData_.size());
			ec.getEngine().send(errorReply, ev.getSenderND());
		}

		if (succeeded) {
			transactionService_->incrementWriteOperationCount(
					ev.getPartitionId());
			transactionService_->addRowWriteCount(
					ev.getPartitionId(), progress.totalRowCount_);
		}
	}
	catch (std::exception &e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

void MultiPutHandler::execute(
		EventContext &ec, const Event &ev, const Request &request,
		const RowSetRequest &rowSetRequest, const MessageSchema &schema,
		CheckedSchemaIdSet &idSet, Progress &progress,
		PutRowOption putRowOption) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, rowSetRequest.sessionId_);
	const TransactionManager::ContextSource src =
			createContextSource(request, rowSetRequest);
	const util::XArray<uint8_t> &multiRowData = rowSetRequest.rowSetData_;
	const uint64_t numRow = rowSetRequest.rowCount_;
	assert(numRow > 0);
	progress.mputStartStmtId_ = UNDEF_STATEMENTID;

	try {
		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				clientId, src, now, emNow, STATEMENTID_NO_CHECK);
		progress.mputStartStmtId_ = txn.getLastStatementId();
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		const size_t capacityBefore = idSet.base().capacity();
		UNUSED_VARIABLE(capacityBefore);

		checkSchema(txn, *container, schema, rowSetRequest.schemaIndex_, idSet);
		assert(idSet.base().capacity() == capacityBefore);

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
			container->getColumnNum(),
			const_cast<uint8_t *>(multiRowData.data()),
			static_cast<uint32_t>(multiRowData.size()), numRow,
			container->getRowFixedDataSize(), NO_VALIDATE_ROW_IMAGE);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(static_cast<size_t>(1), UNDEF_ROWID);
		util::XArray<TriggerService::OperationType> operationTypeList(alloc);
		operationTypeList.assign(
			static_cast<size_t>(1), TriggerService::PUT_ROW);
		util::XArray<uint8_t> rowData(alloc);

		for (StatementId rowStmtId = src.stmtId_; inputMessageRowStore.next();
			 rowStmtId++) {
			try {
				TransactionManager::ContextSource rowSrc = src;
				rowSrc.stmtId_ = rowStmtId;
				txn = transactionManager_->put(
						alloc, request.fixed_.pId_, clientId, rowSrc, now, emNow);
			}
			catch (StatementAlreadyExecutedException &e) {
				UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
						"Row already put. (pId=" <<
						request.fixed_.pId_ <<
						", eventType=" << PUT_MULTIPLE_CONTAINER_ROWS <<
						", stmtId=" << rowStmtId << ", clientId=" << clientId <<
						", containerId=" <<
						((src.containerId_ == UNDEF_CONTAINERID) ?
								0 : src.containerId_) <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

				continue;
			}

			rowData.clear();
			inputMessageRowStore.getCurrentRowData(rowData);

			rowIds[0] = UNDEF_ROWID;
			DataStore::PutStatus putStatus;
			container->putRow(txn, static_cast<uint32_t>(rowData.size()),
				rowData.data(), rowIds[0], putStatus, putRowOption);
			const bool executed = (putStatus != DataStore::NOT_EXECUTED);

			{
				const bool withBegin =
					(src.txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN);
				const bool isAutoCommit =
					(src.txnMode_ == TransactionManager::AUTO_COMMIT);
				assert(!(withBegin && isAutoCommit));
				assert(rowIds.size() == 1);
				assert((executed && rowIds[0] != UNDEF_ROWID) ||
					   (!executed && rowIds[0] == UNDEF_ROWID));
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutRowLog(*log,
					txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), rowStmtId,
					(executed ? rowIds.size() : 0), rowIds, (executed ? 1 : 0),
					rowData, txn.getTransationTimeoutInterval(), src.getMode_,
					withBegin, isAutoCommit);
				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				progress.logRecordList_.push_back(log);
			}

			transactionManager_->update(txn, rowStmtId);

			if (executed) {
				TriggerHandler::checkTrigger(*triggerService_, txn, *container,
					ec, operationTypeList, 1, rowData.data(), rowData.size());
			}
		}

		progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
		progress.totalRowCount_ += numRow;
	}
	catch (std::exception &e) {
		try {
			throw;
		}
		catch (UserException &e2) {
			if (e2.getErrorCode() == GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
				progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
				return;
			}
		}
		catch (...) {
		}
		handleExecuteError(
				alloc, ev, request, PUT_MULTIPLE_CONTAINER_ROWS,
				request.fixed_.pId_, clientId, src, progress, e, "put");
	}
}

void MultiPutHandler::checkSchema(
		TransactionContext &txn,
		BaseContainer &container, const MessageSchema &schema,
		int32_t localSchemaId, CheckedSchemaIdSet &idSet) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	ColumnSchemaId schemaId = container.getColumnSchemaId();

	if (schemaId != UNDEF_COLUMN_SCHEMAID &&
		idSet.find(CheckedSchemaId(localSchemaId, schemaId)) != idSet.end()) {
		return;
	}


	const uint32_t columnCount = container.getColumnNum();
	ColumnInfo *infoList = container.getColumnInfoList();

	if (columnCount != schema.getColumnCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
	}

	util::XArray<char8_t> name1(alloc);
	util::XArray<char8_t> name2(alloc);

	for (uint32_t i = 0; i < columnCount; i++) {
		ColumnInfo &info = infoList[i];

		if (info.getColumnType() != schema.getColumnFullType(i)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}

		const char8_t *name1Ptr =
			info.getColumnName(txn, *(container.getObjectManager()));
		const char8_t *name2Ptr = schema.getColumnName(i).c_str();

		name1.resize(strlen(name1Ptr));
		name2.resize(strlen(name2Ptr));

		if (name1.size() != name2.size()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}

		if (memcmp(name1Ptr, name2Ptr, name1.size()) != 0) {
			ValueProcessor::convertUpperCase(
				name1Ptr, name1.size(), name1.data());

			ValueProcessor::convertUpperCase(
				name2Ptr, name2.size(), name2.data());

			if (memcmp(name1.data(), name2.data(), name1.size()) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
			}
		}
	}
	util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
	container.getKeyColumnIdList(keyColumnIdList);
	const util::Vector<ColumnId> &schemaKeyColumnIdList = schema.getRowKeyColumnIdList();
	if (keyColumnIdList.size() != schemaKeyColumnIdList.size()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
	}
	for (size_t i = 0; i < keyColumnIdList.size(); i++) {
		if (keyColumnIdList[i] != schemaKeyColumnIdList[i]) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}
	}
	if (schemaId != UNDEF_COLUMN_SCHEMAID) {
		idSet.insert(CheckedSchemaId(localSchemaId, schemaId));
	}
}

TransactionManager::ContextSource MultiPutHandler::createContextSource(
		const Request &request, const RowSetRequest &rowSetRequest) {
	return TransactionManager::ContextSource(
			request.fixed_.stmtType_, rowSetRequest.stmtId_,
			rowSetRequest.containerId_,
			rowSetRequest.option_.get<Options::TXN_TIMEOUT_INTERVAL>(),
			rowSetRequest.getMode_,
			rowSetRequest.txnMode_,
			false,
			request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_,
			request.fixed_.cxtSrc_.timeZone_);
}

void MultiPutHandler::decodeMultiSchema(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<const MessageSchema *> &schemaList) {
	try {
		util::StackAllocator &alloc = *schemaList.get_allocator().base();

		int32_t headCount;
		in >> headCount;
		schemaList.reserve(static_cast<size_t>(headCount));

		for (int32_t i = 0; i < headCount; i++) {
			ContainerType containerType;
			decodeEnumData<ContainerType>(in, containerType);

			const char8_t *dummyContainerName = "_";
			MessageSchema *schema = ALLOC_NEW(alloc) MessageSchema(alloc,
				dataStore_->getValueLimitConfig(), dummyContainerName, in, MessageSchema::DEFAULT_VERSION);

			schemaList.push_back(schema);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiPutHandler::decodeMultiRowSet(
		util::ByteStream<util::ArrayInStream> &in, const Request &request,
		const util::XArray<const MessageSchema *> &schemaList,
		util::XArray<const RowSetRequest *> &rowSetList) {
	try {
		util::StackAllocator &alloc = *rowSetList.get_allocator().base();

		int32_t bodyCount;
		in >> bodyCount;
		rowSetList.reserve(static_cast<size_t>(bodyCount));

		for (int32_t i = 0; i < bodyCount; i++) {
			RowSetRequest *subRequest = ALLOC_NEW(alloc) RowSetRequest(alloc);

			in >> subRequest->stmtId_;
			in >> subRequest->containerId_;
			in >> subRequest->sessionId_;

			in >> subRequest->getMode_;
			in >> subRequest->txnMode_;

			decodeContainerOptionPart(in, request.fixed_, subRequest->option_);

			in >> subRequest->schemaIndex_;

			if (subRequest->schemaIndex_ < 0 ||
					static_cast<size_t>(subRequest->schemaIndex_) >=
							schemaList.size()) {
				TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}

			uint32_t bytesSize;
			in >> bytesSize;
			subRequest->rowSetData_.resize(
				bytesSize - sizeof(subRequest->rowCount_));

			in >> subRequest->rowCount_;
			in.readAll(
				subRequest->rowSetData_.data(), subRequest->rowSetData_.size());

			rowSetList.push_back(subRequest);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiGetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		util::XArray<SearchEntry> searchList(alloc);

		decodeMultiSearchEntry(
				in, request.fixed_.pId_,
				connOption.dbId_, connOption.dbName_.c_str(),
				request.optional_.get<Options::DB_NAME>(),
				connOption.userType_, connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				searchList);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
				reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		SchemaMap schemaMap(alloc);
		buildSchemaMap(request.fixed_.pId_, searchList, schemaMap, out);

		const size_t bodyTopPos = out.base().position();
		uint32_t entryCount = 0;
		encodeIntData<uint32_t>(out, entryCount);

		Progress progress(alloc, getLockConflictStatus(ev, emNow, request));

		bool succeeded = true;
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			for (size_t i = 0; i < searchList.size(); i++) {
				const SearchEntry &entry = searchList[i];

				entryCount += execute(
						ec, ev, request, entry, schemaMap, out, progress);

				if (!progress.lastExceptionData_.empty() ||
					progress.lockConflicted_) {
					succeeded = false;
					break;
				}
			}
		}


		if (progress.lockConflicted_) {
			retryLockConflictedRequest(ec, ev, progress.lockConflictStatus_);
		}
		else if (succeeded) {
			const size_t bodyEndPos = out.base().position();
			out.base().position(bodyTopPos);
			encodeIntData<uint32_t>(out, entryCount);
			out.base().position(bodyEndPos);
			ec.getEngine().send(reply, ev.getSenderND());
		}
		else {
			Event errorReply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
			EventByteOutStream out = encodeCommonPart(
					errorReply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
			out.writeAll(progress.lastExceptionData_.data(),
					progress.lastExceptionData_.size());
			ec.getEngine().send(errorReply, ev.getSenderND());
		}

		if (succeeded) {
			transactionService_->incrementReadOperationCount(
					ev.getPartitionId());
			transactionService_->addRowReadCount(
					ev.getPartitionId(), progress.totalRowCount_);
		}
	}
	catch (std::exception &e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

uint32_t MultiGetHandler::execute(
		EventContext &ec, const Event &ev, const Request &request,
		const SearchEntry &entry, const SchemaMap &schemaMap,
		EventByteOutStream &replyOut, Progress &progress) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);
	const TransactionManager::ContextSource src =
			createContextSource(request, entry);

	const char8_t *getType =
		(entry.predicate_->predicateType_ == PREDICATE_TYPE_RANGE) ? 
		"rangeKey" : "distinctKey";

	uint32_t entryCount = 0;

	try {
		const util::StackAllocator::Scope scope(alloc);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_, clientId, src, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(
				txn, dataStore_, txn.getPartitionId(),
				entry.containerId_, ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();

		const RowKeyPredicate &predicate = *entry.predicate_;
		checkContainerRowKey(container, predicate);

		if (predicate.predicateType_ == PREDICATE_TYPE_RANGE) {
			RowData *startRowKey = NULL;
			RowData *finishRowKey = NULL;

			if (predicate.keyType_ == 255) {
				const bool NO_VALIDATE_ROW_IMAGE = true;
				InputMessageRowStore inputMessageRowStore(
					dataStore_->getValueLimitConfig(), container->getRowKeyColumnInfoList(txn),
					container->getRowKeyColumnNum(),
					const_cast<uint8_t *>(predicate.compositeKeys_->data()),
					static_cast<uint32_t>(predicate.compositeKeys_->size()), 
					predicate.rowCount_, container->getRowKeyFixedDataSize(alloc), 
					NO_VALIDATE_ROW_IMAGE);

				if (predicate.rangeFlags_[0]) {
					if (inputMessageRowStore.next()) {
						startRowKey = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						inputMessageRowStore.getCurrentRowData(*startRowKey);
					} else {
						assert(false);
					}
				}
				if (predicate.rangeFlags_[1]) {
					if (inputMessageRowStore.next()) {
						finishRowKey = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						inputMessageRowStore.getCurrentRowData(*finishRowKey);
					} else {
						assert(false);
					}
				}
			} else {
				assert(predicate.keys_->size() == 2);
				startRowKey = (*predicate.keys_)[0]; 
				finishRowKey = (*predicate.keys_)[1]; 
			}
			executeRange(ec, schemaMap, txn, container, startRowKey, 
				finishRowKey, replyOut, progress);
			entryCount++;
		}
		else {
			if (predicate.keyType_ == 255) {
				RowData rowKey(alloc);

				const bool NO_VALIDATE_ROW_IMAGE = true;
				InputMessageRowStore inputMessageRowStore(
					dataStore_->getValueLimitConfig(), container->getRowKeyColumnInfoList(txn),
					container->getRowKeyColumnNum(),
					const_cast<uint8_t *>(predicate.compositeKeys_->data()),
					static_cast<uint32_t>(predicate.compositeKeys_->size()), 
					predicate.rowCount_, container->getRowKeyFixedDataSize(alloc), 
					NO_VALIDATE_ROW_IMAGE);
				while (inputMessageRowStore.next()) {
					rowKey.clear();
					inputMessageRowStore.getCurrentRowData(rowKey);
					executeGet(ec, schemaMap, txn, container, rowKey,
						replyOut, progress);
					entryCount++;
				}
			} else {
				const RowKeyDataList &distinctKeys = *predicate.keys_;
				for (RowKeyDataList::const_iterator it = distinctKeys.begin();
					 it != distinctKeys.end(); ++it) {
					const RowKeyData &rowKey = **it;
					executeGet(ec, schemaMap, txn, container, rowKey,
						replyOut, progress);
					entryCount++;
				}
			}
		}

	}
	catch (std::exception &e) {
		handleExecuteError(
				alloc, ev, request, GET_MULTIPLE_CONTAINER_ROWS,
				request.fixed_.pId_, clientId, src, progress, e, getType);
	}

	return entryCount;
}

void MultiGetHandler::executeRange(
		EventContext &ec, const SchemaMap &schemaMap,
		TransactionContext &txn, BaseContainer *container, const RowKeyData *startKey,
		const RowKeyData *finishKey, EventByteOutStream &replyOut, Progress &progress) {
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	ResultSet *rs = dataStore_->createResultSet(txn,
		container->getContainerId(), container->getVersionId(), emNow,
		NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);

	QueryProcessor::search(txn, *container, MAX_RESULT_SIZE,
		startKey, finishKey, *rs);
	rs->setResultType(RESULT_ROWSET);


	QueryProcessor::fetch(txn, *container, 0, MAX_RESULT_SIZE, rs);

	encodeEntry(
		container->getContainerKey(txn), container->getContainerId(),
		schemaMap, *rs, replyOut);

	progress.totalRowCount_ += rs->getResultNum();
}

void MultiGetHandler::executeGet(
		EventContext &ec, const SchemaMap &schemaMap,
		TransactionContext &txn, BaseContainer *container, const RowKeyData &rowKey,
		EventByteOutStream &replyOut, Progress &progress) {
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	ResultSet *rs = dataStore_->createResultSet(txn,
		container->getContainerId(), container->getVersionId(),
		emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore_, *rs);

	QueryProcessor::get(txn, *container,
		static_cast<uint32_t>(rowKey.size()), rowKey.data(), *rs);
	rs->setResultType(RESULT_ROWSET);


	QueryProcessor::fetch(
		txn, *container, 0, MAX_RESULT_SIZE, rs);

	encodeEntry(
		container->getContainerKey(txn),
		container->getContainerId(), schemaMap, *rs, replyOut);

	progress.totalRowCount_ += rs->getFetchNum();
}

void MultiGetHandler::buildSchemaMap(
		PartitionId pId,
		const util::XArray<SearchEntry> &searchList, SchemaMap &schemaMap,
		EventByteOutStream &out) {
	typedef util::XArray<uint8_t> SchemaData;
	typedef util::Map<ColumnSchemaId, LocalSchemaId> LocalIdMap;
	typedef LocalIdMap::iterator LocalIdMapItr;

	util::TimeZone timeZone;
	util::StackAllocator &alloc = *schemaMap.get_allocator().base();

	SchemaData schemaBuf(alloc);


	const size_t outStartPos = out.base().position();
	int32_t schemaCount = 0;
	out << schemaCount;

	LocalIdMap localIdMap(alloc);
	for (util::XArray<SearchEntry>::const_iterator it = searchList.begin();;
		 ++it) {
		if (it == searchList.end()) {
			const size_t outEndPos = out.base().position();
			out.base().position(outStartPos);
			out << schemaCount;
			out.base().position(outEndPos);
			return;
		}

		const ContainerId containerId = it->containerId_;
		const TransactionManager::ContextSource src(
				GET_MULTIPLE_CONTAINER_ROWS,
				0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
		TransactionContext &txn =
			transactionManager_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);

		ContainerAutoPtr containerAutoPtr(
			txn, dataStore_, pId, containerId, ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		const ContainerType containerType = container->getContainerType();
		ColumnSchemaId columnSchemaId = container->getColumnSchemaId();


		if (columnSchemaId == UNDEF_COLUMN_SCHEMAID) {
			break;
		}

		LocalIdMapItr idIt = localIdMap.find(columnSchemaId);
		LocalSchemaId localId;
		if (idIt == localIdMap.end()) {
			localId = schemaCount;

			schemaBuf.clear();
			{
				util::XArrayByteOutStream schemaOut = util::XArrayByteOutStream(
					util::XArrayOutStream<>(schemaBuf));
				schemaOut << static_cast<uint8_t>(containerType);
			}

			const bool optionIncluded = false;
			container->getContainerInfo(txn, schemaBuf, optionIncluded);

			out.writeAll(schemaBuf.data(), schemaBuf.size());

			schemaCount++;
		}
		else {
			localId = idIt->second;
		}

		localIdMap.insert(std::make_pair(columnSchemaId, localId));
		schemaMap.insert(std::make_pair(containerId, localId));
	}


	out.base().position(outStartPos);
	schemaMap.clear();


	typedef util::XArray<SchemaData *> SchemaList;
	typedef util::MultiMap<uint32_t, LocalSchemaId> DigestMap;
	typedef DigestMap::iterator DigestMapItr;

	SchemaList schemaList(alloc);
	DigestMap digestMap(alloc);

	for (util::XArray<SearchEntry>::const_iterator it = searchList.begin();
		 it != searchList.end(); ++it) {
		const ContainerId containerId = it->containerId_;
		const TransactionManager::ContextSource src(
				GET_MULTIPLE_CONTAINER_ROWS,
				0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
		TransactionContext &txn =
			transactionManager_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);

		ContainerAutoPtr containerAutoPtr(
			txn, dataStore_, pId, containerId, ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		const ContainerType containerType = container->getContainerType();

		schemaBuf.clear();
		{
			util::XArrayByteOutStream schemaOut =
				util::XArrayByteOutStream(util::XArrayOutStream<>(schemaBuf));
			schemaOut << static_cast<uint8_t>(containerType);
		}

		const bool optionIncluded = false;
		container->getContainerInfo(txn, schemaBuf, optionIncluded);

		uint32_t digest = 1;
		for (SchemaData::iterator schemaIt = schemaBuf.begin();
			 schemaIt != schemaBuf.end(); ++schemaIt) {
			digest = 31 * digest + (*schemaIt);
		}

		LocalSchemaId schemaId = -1;
		std::pair<DigestMapItr, DigestMapItr> range =
			digestMap.equal_range(digest);

		for (DigestMapItr rangeIt = range.first; rangeIt != range.second;
			 ++rangeIt) {
			SchemaData &targetSchema = *schemaList[rangeIt->second];

			if (targetSchema.size() == schemaBuf.size() &&
				memcmp(targetSchema.data(), schemaBuf.data(),
					schemaBuf.size()) == 0) {
				schemaId = rangeIt->second;
				break;
			}
		}

		if (schemaId < 0) {
			SchemaData *targetSchema = ALLOC_NEW(alloc) SchemaData(alloc);
			targetSchema->resize(schemaBuf.size());
			memcpy(targetSchema->data(), schemaBuf.data(), schemaBuf.size());

			schemaId = static_cast<int32_t>(schemaList.size());
			digestMap.insert(std::make_pair(digest, schemaId));
			schemaList.push_back(targetSchema);
		}

		schemaMap.insert(std::make_pair(containerId, schemaId));
	}

	out << static_cast<int32_t>(schemaList.size());
	for (SchemaList::iterator it = schemaList.begin(); it != schemaList.end();
		 ++it) {
		SchemaData &targetSchema = **it;
		out.writeAll(targetSchema.data(), targetSchema.size());
	}
}

void MultiGetHandler::checkContainerRowKey(
		BaseContainer *container, const RowKeyPredicate &predicate) {
	checkContainerExistence(container);

	if (!container->definedRowKey()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_UNDEFINED, "");
	}

	if (predicate.compositeColumnTypes_ != NULL) {
		bool isMatch = container->checkRowKeySchema(*(predicate.compositeColumnTypes_));
		if (!isMatch) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
		}
	} else {
	const ColumnInfo &keyColumnInfo =
		container->getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
	if (predicate.keyType_ != keyColumnInfo.getColumnType()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
	}
	}
}

TransactionManager::ContextSource MultiGetHandler::createContextSource(
		const Request &request, const SearchEntry &entry) {
	return TransactionManager::ContextSource(
			request.fixed_.stmtType_, entry.stmtId_,
			entry.containerId_,
			request.fixed_.cxtSrc_.txnTimeoutInterval_,
			entry.getMode_,
			entry.txnMode_,
			false,
			request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_,
			request.fixed_.cxtSrc_.timeZone_);
}

void MultiGetHandler::decodeMultiSearchEntry(
		util::ByteStream<util::ArrayInStream> &in, PartitionId pId,
		DatabaseId loginDbId,
		const char8_t *loginDbName, const char8_t *specifiedDbName,
		UserType userType, RequestType requestType, bool isSystemMode,
		util::XArray<SearchEntry> &searchList) {
	try {
		util::StackAllocator &alloc = *searchList.get_allocator().base();
		util::XArray<const RowKeyPredicate *> predicareList(alloc);

		{
			int32_t headCount;
			in >> headCount;
			predicareList.reserve(static_cast<size_t>(headCount));

			for (int32_t i = 0; i < headCount; i++) {
				const RowKeyPredicate *predicate = ALLOC_NEW(alloc)
					RowKeyPredicate(decodePredicate(in, alloc));
				predicareList.push_back(predicate);
			}
		}

		{
			int32_t bodyCount;
			in >> bodyCount;
			searchList.reserve(static_cast<size_t>(bodyCount));
			bool isNewSql = false;
			for (int32_t i = 0; i < bodyCount; i++) {
				OptionSet optionSet(alloc);
				decodeOptionPart(in, optionSet);

				util::XArray<uint8_t> *containerName =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeVarSizeBinaryData(in, *containerName);

				int32_t predicareIndex;
				in >> predicareIndex;

				if (predicareIndex < 0 ||
					static_cast<size_t>(predicareIndex) >=
						predicareList.size()) {
					TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED,
						"(predicareIndex=" << predicareIndex << ")");
				}

				const TransactionManager::ContextSource src(
					GET_MULTIPLE_CONTAINER_ROWS);
				TransactionContext &txn = transactionManager_->put(
					alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);
				const FullContainerKey *containerKey =
						ALLOC_NEW(alloc) FullContainerKey(
								alloc, getKeyConstraint(CONTAINER_ATTR_ANY),
								containerName->data(), containerName->size());
				checkLoggedInDatabase(
						loginDbId, loginDbName,
						containerKey->getComponents(alloc).dbId_, specifiedDbName
						, isNewSql
						);
				ContainerAutoPtr containerAutoPtr(
						txn, dataStore_, pId, *containerKey, ANY_CONTAINER,
						getCaseSensitivity(optionSet).isContainerNameCaseSensitive());
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				if (container != NULL && !checkPrivilege(
						GET_MULTIPLE_CONTAINER_ROWS,
						userType, requestType, isSystemMode,
						optionSet.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
						container->getContainerType(),
						container->getAttribute(),
						optionSet.get<Options::CONTAINER_ATTRIBUTE>())) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"Forbidden Container Attribute");
				}

				if (container != NULL) {
					const TablePartitioningVersionId expectedVersion =
							optionSet.get<Options::SUB_CONTAINER_VERSION>();
					const TablePartitioningVersionId actualVersion =
							container->getTablePartitioningVersionId();
					if (expectedVersion >= 0 && expectedVersion < actualVersion) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION,
								"Sub container version expired ("
								"expected=" << expectedVersion <<
								", actual=" << actualVersion << ")");
					}
				}

				const ContainerId containerId = (container != NULL) ?
						container->getContainerId() : UNDEF_CONTAINERID;
				if (containerId == UNDEF_CONTAINERID) {
					continue;
				}

				SearchEntry entry(containerId, containerKey,
					predicareList[static_cast<size_t>(predicareIndex)]);

				searchList.push_back(entry);
			}
		}
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

MultiGetHandler::RowKeyPredicate MultiGetHandler::decodePredicate(
		util::ByteStream<util::ArrayInStream> &in, util::StackAllocator &alloc) {
	int8_t keyType;
	in >> keyType;

	int8_t predicateType;
	in >> predicateType;

	if (predicateType != PREDICATE_TYPE_RANGE && predicateType != PREDICATE_TYPE_DISTINCT) {
		TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}
	RowKeyPredicate predicate = {
		static_cast<ColumnType>(keyType), static_cast<RowKeyPredicateType>(predicateType), {0, 0}, 0, NULL, NULL, NULL};

	if (keyType == -1) {
		int32_t columnCount;
		in >> columnCount;

		RowKeyColumnTypeList *columnTypes =
			ALLOC_NEW(alloc) util::XArray<ColumnType>(alloc);
		columnTypes->reserve(static_cast<size_t>(columnCount));

		for (int32_t i = 0; i < columnCount; i++) {
			int8_t columnType;
			in >> columnType;
			columnTypes->push_back(static_cast<ColumnType>(columnType));
		}
		predicate.compositeColumnTypes_ = columnTypes;


		if (predicateType == PREDICATE_TYPE_RANGE) {
			for (size_t i = 0; i < 2; i++) {
				in >> predicate.rangeFlags_[i];
			}
		}
		uint32_t bytesSize;
		in >> bytesSize;

		predicate.compositeKeys_ = 
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		predicate.compositeKeys_->resize(
			bytesSize - sizeof(predicate.rowCount_));

		in >> predicate.rowCount_;
		in.readAll(
			predicate.compositeKeys_->data(), predicate.compositeKeys_->size());
	} else {
		const MessageRowKeyCoder keyCoder(static_cast<ColumnType>(keyType));
		if (predicateType == PREDICATE_TYPE_RANGE) {
			RowKeyDataList *startFinishKeys =
				ALLOC_NEW(alloc) util::XArray<RowKeyData *>(alloc);

			for (size_t i = 0; i < 2; i++) {
				in >> predicate.rangeFlags_[i];

				if (predicate.rangeFlags_[i]) {
					RowKeyData *key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					keyCoder.decode(in, *key);
					startFinishKeys->push_back(key);
				} else {
					startFinishKeys->push_back(NULL);
				}
			}
			predicate.keys_ = startFinishKeys;
		}
		else {
			int32_t keyCount;
			in >> keyCount;

			RowKeyDataList *distinctKeys =
				ALLOC_NEW(alloc) util::XArray<RowKeyData *>(alloc);
			distinctKeys->reserve(static_cast<size_t>(keyCount));

			for (int32_t i = 0; i < keyCount; i++) {
				RowKeyData *key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				keyCoder.decode(in, *key);
				distinctKeys->push_back(key);
			}
			predicate.keys_ = distinctKeys;
		}
	}

	return predicate;
}

void MultiGetHandler::encodeEntry(
		const FullContainerKey &containerKey,
		ContainerId containerId, const SchemaMap &schemaMap, ResultSet &rs,
		EventByteOutStream &out) {
	encodeContainerKey(out, containerKey);

	LocalSchemaId schemaId;
	{
		SchemaMap::const_iterator it = schemaMap.find(containerId);
		if (it == schemaMap.end()) {
			assert(false);
			GS_THROW_SYSTEM_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		schemaId = it->second;
	}
	out << schemaId;

	out << rs.getResultNum();

	if (rs.getResultNum() > 0) {
		const util::XArray<uint8_t> *rowDataFixedPart =
			rs.getRowDataFixedPartBuffer();
		const util::XArray<uint8_t> *rowDataVarPart = rs.getRowDataVarPartBuffer();

		out.writeAll(rowDataFixedPart->data(), rowDataFixedPart->size());
		out.writeAll(rowDataVarPart->data(), rowDataVarPart->size());
	}
}

/*!
	@brief Handler Operator
*/
void MultiQueryHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

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

		QueryRequestList queryList(alloc);

		decodeMultiQuery(in, request, queryList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkTransactionTimeout(
				emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());

		int32_t fetchByteSize =
				request.optional_.get<Options::FETCH_BYTES_SIZE>();

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
				reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);
		encodeMultiSearchResultHead(
				out, static_cast<uint32_t>(queryList.size()));

		Progress progress(alloc, getLockConflictStatus(ev, emNow, request));

		bool succeeded = true;
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			for (size_t i = 0; i < queryList.size(); i++) {
				const QueryRequest &queryRequest = *(queryList[i]);

				const bool forUpdate =
						queryRequest.optionSet_.get<Options::FOR_UPDATE>();
				if (forUpdate) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
						"row lock (forUpdate) is not applicable");
				}

				checkConsistency(ev.getSenderND(), forUpdate);
				const PartitionRoleType partitionRole = forUpdate ?
						PROLE_OWNER :
						(PROLE_OWNER | PROLE_BACKUP);
				checkExecutable(
						request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
				bool isTQL = (queryRequest.stmtType_ == QUERY_TQL) ? true : false;
				checkQueryOption(
						queryRequest.optionSet_,
						queryRequest.fetchOption_, queryRequest.isPartial_, isTQL);
				execute(
						ec, ev, request, fetchByteSize, queryRequest, out,
						progress);

				if (!progress.lastExceptionData_.empty() ||
						progress.lockConflicted_) {
					succeeded = false;
					break;
				}
			}
		}

		{
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_, request.fixed_.clientId_,
					request.fixed_.cxtSrc_, now, emNow);

			const NodeDescriptor &clientND = succeeded ?
					ev.getSenderND() : NodeDescriptor::EMPTY_ND;

			executeReplication(
					request, ec, alloc, clientND, txn,
					request.fixed_.stmtType_, request.fixed_.cxtSrc_.stmtId_,
					TransactionManager::REPLICATION_ASYNC, NULL, 0,
					progress.logRecordList_.data(), progress.logRecordList_.size(),
					response);
		}

		if (progress.lockConflicted_) {
			retryLockConflictedRequest(ec, ev, progress.lockConflictStatus_);
		}
		else if (succeeded) {
			ec.getEngine().send(reply, ev.getSenderND());
		}
		else {
			Event errorReply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
			EventByteOutStream out = encodeCommonPart(
					errorReply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
			out.writeAll(
					progress.lastExceptionData_.data(),
					progress.lastExceptionData_.size());
			ec.getEngine().send(errorReply, ev.getSenderND());
		}

		if (succeeded) {
			transactionService_->incrementReadOperationCount(
					ev.getPartitionId());
			transactionService_->addRowReadCount(
					ev.getPartitionId(), progress.totalRowCount_);
		}
	}
	catch (std::exception &e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

void MultiQueryHandler::execute(
		EventContext &ec, const Event &ev, const Request &request,
		int32_t &fetchByteSize, const QueryRequest &queryRequest,
		EventByteOutStream &replyOut, Progress &progress) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, queryRequest.sessionId_);
	const TransactionManager::ContextSource src =
			createContextSource(request, queryRequest);

	const char8_t *queryType = getQueryTypeName(queryRequest.stmtType_);

	try {
		const util::StackAllocator::Scope scope(alloc);

		TransactionContext &txn = transactionManager_->put(
				alloc, request.fixed_.pId_, clientId, src, now, emNow);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		const FetchOption &fetchOption = queryRequest.fetchOption_;
		ResultSetOption queryOption;
		createResultSetOption(
				queryRequest.optionSet_, &fetchByteSize,
				queryRequest.isPartial_, queryRequest.partialQueryOption_,
				queryOption);
		ResultSet *rs = dataStore_->createResultSet(
				txn, txn.getContainerId(), UNDEF_SCHEMAVERSIONID, emNow,
				&queryOption);
		const ResultSetGuard rsGuard(txn, *dataStore_, *rs);

		switch (queryRequest.stmtType_) {
		case QUERY_TQL: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			const util::String *query = queryRequest.query_.tqlQuery_;
			FullContainerKey *containerKey = NULL;
			const QueryContainerKey *distInfo =
					queryRequest.optionSet_.get<Options::DIST_QUERY>();
			if (distInfo != NULL && distInfo->enabled_) {
				const TablePartitioningVersionId expectedVersion =
						queryRequest.optionSet_.get<Options::SUB_CONTAINER_VERSION>();
				const TablePartitioningVersionId actualVersion =
						container->getTablePartitioningVersionId();
				if (expectedVersion >= 0 && expectedVersion < actualVersion) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION,
							"Sub container version expired ("
							"expected=" << expectedVersion <<
							", actual=" << actualVersion << ")");
				}

				const util::XArray<uint8_t> &keyBinary =
						distInfo->containerKeyBinary_;
				containerKey = ALLOC_NEW(alloc) FullContainerKey(
						alloc, getKeyConstraint(queryRequest.optionSet_),
						keyBinary.data(), keyBinary.size());
			}
			QueryProcessor::executeTQL(
					txn, *container, fetchOption.limit_,
					TQLInfo(
							request.optional_.get<Options::DB_NAME>(),
							containerKey, query->c_str()), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_COLLECTION_GEOMETRY_RELATED: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(),
				COLLECTION_CONTAINER);
			Collection *container = containerAutoPtr.getCollection();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			GeometryQuery *query = queryRequest.query_.geometryQuery_;
			QueryProcessor::searchGeometryRelated(txn, *container,
				fetchOption.limit_, query->columnId_,
				static_cast<uint32_t>(query->intersection_.size()),
				query->intersection_.data(), query->operator_, *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(),
				COLLECTION_CONTAINER);
			Collection *container = containerAutoPtr.getCollection();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			GeometryQuery *query = queryRequest.query_.geometryQuery_;
			QueryProcessor::searchGeometry(txn, *container, fetchOption.limit_,
				query->columnId_,
				static_cast<uint32_t>(query->intersection_.size()),
				query->intersection_.data(),
				static_cast<uint32_t>(query->disjoint_.size()),
				query->disjoint_.data(), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_TIME_SERIES_RANGE: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(),
				TIME_SERIES_CONTAINER);
			TimeSeries *container = containerAutoPtr.getTimeSeries();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			RangeQuery *query = queryRequest.query_.rangeQuery_;
			QueryProcessor::search(txn, *container, query->order_,
				fetchOption.limit_, query->start_, query->end_, *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_TIME_SERIES_SAMPLING: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(),
				TIME_SERIES_CONTAINER);
			TimeSeries *container = containerAutoPtr.getTimeSeries();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			SamplingQuery *query = queryRequest.query_.samplingQuery_;
			QueryProcessor::sample(txn, *container, fetchOption.limit_,
				query->start_, query->end_, query->toSamplingOption(), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNSUPPORTED,
					"Unsupported query (statement type=" <<
					queryRequest.stmtType_ << ")");
		}


		encodeSearchResult(alloc, replyOut, *rs);

		transactionManager_->update(txn, queryRequest.stmtId_);

		int32_t rsUseSize = rs->getSerializedSize();
		if (fetchByteSize > rsUseSize) {
			fetchByteSize -= rsUseSize;
		} else {
			fetchByteSize = 1;
		}

		progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
		progress.totalRowCount_ += rs->getFetchNum();
	}
	catch (std::exception &e) {
		do {
			if (!queryRequest.optionSet_.get<Options::SUB_CONTAINER_EXPIRABLE>()) {
				break;
			}

			try {
				throw;
			}
			catch (UserException &e2) {
				if (e2.getErrorCode() !=
						GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED && 
					e2.getErrorCode() !=
						GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
					break;
				}
			}

			try {
				encodeEmptySearchResult(alloc, replyOut);
				progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
			}
			catch (std::exception &e2) {
				handleExecuteError(
						alloc, ev, request, EXECUTE_MULTIPLE_QUERIES,
						request.fixed_.pId_, clientId, src, progress, e2,
						queryType);
			}
			return;
		}
		while (false);

		handleExecuteError(
				alloc, ev, request, EXECUTE_MULTIPLE_QUERIES,
				request.fixed_.pId_, clientId, src, progress, e, queryType);
	}
}

TransactionManager::ContextSource MultiQueryHandler::createContextSource(
		const Request &request, const QueryRequest &queryRequest) {
	return TransactionManager::ContextSource(
			request.fixed_.stmtType_, queryRequest.stmtId_,
			queryRequest.containerId_,
			queryRequest.optionSet_.get<Options::TXN_TIMEOUT_INTERVAL>(),
			queryRequest.getMode_,
			queryRequest.txnMode_,
			false,
			request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_,
			request.fixed_.cxtSrc_.timeZone_);
}

void MultiQueryHandler::decodeMultiQuery(
		util::ByteStream<util::ArrayInStream> &in, const Request &request,
		util::XArray<const QueryRequest *> &queryList) {
	util::StackAllocator &alloc = *queryList.get_allocator().base();

	try {
		int32_t entryCount;
		in >> entryCount;
		queryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			QueryRequest *subRequest = ALLOC_NEW(alloc) QueryRequest(alloc);

			int32_t stmtType;
			in >> stmtType;
			subRequest->stmtType_ = static_cast<EventType>(stmtType);

			in >> subRequest->stmtId_;
			in >> subRequest->containerId_;
			in >> subRequest->sessionId_;
			in >> subRequest->schemaVersionId_;

			in >> subRequest->getMode_;
			in >> subRequest->txnMode_;

			decodeContainerOptionPart(in, request.fixed_, subRequest->optionSet_);

			decodeFetchOption(in, subRequest->fetchOption_);
			decodePartialQueryOption(
					in, alloc, subRequest->isPartial_,
					subRequest->partialQueryOption_);

			switch (subRequest->stmtType_) {
			case QUERY_TQL:
				subRequest->query_.tqlQuery_ =
					ALLOC_NEW(alloc) util::String(alloc);
				in >> *subRequest->query_.tqlQuery_;
				break;
			case QUERY_COLLECTION_GEOMETRY_RELATED: {
				GeometryQuery *query = ALLOC_NEW(alloc) GeometryQuery(alloc);

				decodeGeometryRelatedQuery(in, *query);

				subRequest->query_.geometryQuery_ = query;
			} break;
			case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: {
				GeometryQuery *query = ALLOC_NEW(alloc) GeometryQuery(alloc);

				decodeGeometryWithExclusionQuery(in, *query);

				subRequest->query_.geometryQuery_ = query;
			} break;
			case QUERY_TIME_SERIES_RANGE: {
				RangeQuery *query = ALLOC_NEW(alloc) RangeQuery();

				decodeRangeQuery(in, *query);

				subRequest->query_.rangeQuery_ = query;
			} break;
			case QUERY_TIME_SERIES_SAMPLING: {
				SamplingQuery *query = ALLOC_NEW(alloc) SamplingQuery(alloc);

				decodeSamplingQuery(in, *query);

				subRequest->query_.samplingQuery_ = query;
			} break;
			default:
				TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}

			queryList.push_back(subRequest);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeMultiSearchResultHead(
		EventByteOutStream &out, uint32_t queryCount) {
	try {
		out << queryCount;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeEmptySearchResult(
		util::StackAllocator &alloc, EventByteOutStream &out) {
	try {
		int64_t allSize = 0;
		int32_t entryNum = 0;
		allSize += sizeof(int32_t);

		out << allSize;
		out << entryNum;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeSearchResult(
		util::StackAllocator &alloc,
		EventByteOutStream &out, const ResultSet &rs) {
	try {
		const size_t allPos = out.base().position();
		int64_t allSize = 0;
		out << allSize;

		switch (rs.getResultType()) {
		case RESULT_ROWSET: {
			PartialQueryOption partialQueryOption(alloc);
			rs.getQueryOption().encodePartialQueryOption(alloc, partialQueryOption);

			const size_t entryNumPos = out.base().position();
			int32_t entryNum = 0;
			out << entryNum;

			allSize += sizeof(int32_t);

			if (!partialQueryOption.empty()) {
				encodeEnumData<QueryResponseType>(out, PARTIAL_EXECUTION_STATE);

				const size_t entrySizePos = out.base().position();
				int32_t entrySize = 0;
				out << entrySize;

				encodeBooleanData(out, true);
				entrySize += sizeof(int8_t);

				int32_t partialEntryNum = static_cast<int32_t>(partialQueryOption.size());
				out << partialEntryNum;
				entrySize += sizeof(int32_t);

				PartialQueryOption::const_iterator itr;
				for (itr = partialQueryOption.begin(); itr != partialQueryOption.end(); itr++) {
					encodeIntData<int8_t>(out, itr->first);
					entrySize += sizeof(int8_t);

					int32_t partialEntrySize = itr->second->size();
					out << partialEntrySize;
					entrySize += sizeof(int32_t);
					encodeBinaryData(out, itr->second->data(), itr->second->size());
					entrySize += itr->second->size();
				}
				const size_t entryLastPos = out.base().position();
				out.base().position(entrySizePos);
				out << entrySize;
				out.base().position(entryLastPos);
				entryNum++;

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}

			entryNum += encodeDistributedResult(out, rs, &allSize);

			{
				encodeEnumData<QueryResponseType>(out, ROW_SET);

				int32_t entrySize = sizeof(ResultSize) +
					rs.getFixedOffsetSize() +
					rs.getVarOffsetSize();
				out << entrySize;

				out << rs.getFetchNum();
				out << std::make_pair(rs.getFixedStartData(),
					static_cast<size_t>(rs.getFixedOffsetSize()));
				out << std::make_pair(rs.getVarStartData(),
					static_cast<size_t>(rs.getVarOffsetSize()));
				entryNum++;

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}

			const size_t lastPos = out.base().position();
			out.base().position(entryNumPos);
			out << entryNum;
			out.base().position(lastPos);
		} break;
		case RESULT_EXPLAIN: {
			const size_t entryNumPos = out.base().position();
			int32_t entryNum = 0;
			out << entryNum;

			entryNum += encodeDistributedResult(out, rs, &allSize);

			allSize += sizeof(int32_t);
			{
				encodeEnumData<QueryResponseType>(out, QUERY_ANALYSIS);
				int32_t entrySize = sizeof(ResultSize) +
					rs.getFixedOffsetSize() +
					rs.getVarOffsetSize();
				out << entrySize;

				out << rs.getResultNum();
				out << std::make_pair(rs.getFixedStartData(),
					static_cast<size_t>(rs.getFixedOffsetSize()));
				out << std::make_pair(rs.getVarStartData(),
					static_cast<size_t>(rs.getVarOffsetSize()));
				entryNum++;

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}

			const size_t lastPos = out.base().position();
			out.base().position(entryNumPos);
			out << entryNum;
			out.base().position(lastPos);
		} break;
		case RESULT_AGGREGATE: {
			int32_t entryNum = 1;
			out << entryNum;

			allSize += sizeof(int32_t);
			{
				encodeEnumData<QueryResponseType>(out, AGGREGATION);
				const util::XArray<uint8_t> *rowDataFixedPart =
					rs.getRowDataFixedPartBuffer();
				int32_t entrySize = sizeof(ResultSize) +
					rowDataFixedPart->size();
				out << entrySize;

				out << rs.getResultNum();
				encodeBinaryData(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}
		} break;
		case PARTIAL_RESULT_ROWSET: {
			assert(rs.getId() != UNDEF_RESULTSETID);
			int32_t entryNum = 2;
			out << entryNum;

			allSize += sizeof(int32_t);
			{
				encodeEnumData<QueryResponseType>(out, PARTIAL_FETCH_STATE);
				int32_t entrySize = sizeof(ResultSize) + sizeof(ResultSetId);
				out << entrySize;

				out << rs.getResultNum();
				out << rs.getId();

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}
			{
				encodeEnumData<QueryResponseType>(out, ROW_SET);
				int32_t entrySize = sizeof(ResultSize) +
					rs.getFixedOffsetSize() +
					rs.getVarOffsetSize();
				out << entrySize;

				out << rs.getFetchNum();
				out << std::make_pair(rs.getFixedStartData(),
					static_cast<size_t>(rs.getFixedOffsetSize()));
				out << std::make_pair(rs.getVarStartData(),
					static_cast<size_t>(rs.getVarOffsetSize()));

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
				"(resultType=" << rs.getResultType() << ")");
		}

		const size_t lastPos = out.base().position();
		out.base().position(allPos);
		out << allSize;

		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

const char8_t *MultiQueryHandler::getQueryTypeName(EventType queryStmtType) {
	switch (queryStmtType) {
	case QUERY_TQL:
		return "tql";
	case QUERY_COLLECTION_GEOMETRY_RELATED:
		return "geometryRelated";
	case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION:
		return "geometryWithExlusion";
	case QUERY_TIME_SERIES_RANGE:
		return "timeRange";
	case QUERY_TIME_SERIES_SAMPLING:
		return "timeSampling";
	default:
		return "unknown";
	}
}


void CheckTimeoutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	try {
		{
			const util::StackAllocator::Scope scope(alloc);
			if (isNewSQL_) {
				checkAuthenticationTimeout(ec);
				return;
			}

			util::XArray<bool> checkPartitionFlagList(alloc);
			if (isTransactionTimeoutCheckEnabled(
					ec.getWorkerId(), checkPartitionFlagList)) {

				checkRequestTimeout(ec, checkPartitionFlagList);
				checkTransactionTimeout(ec, checkPartitionFlagList);
				checkKeepaliveTimeout(ec, checkPartitionFlagList);
			}
		}

		checkReplicationTimeout(ec);
		checkAuthenticationTimeout(ec);
		checkResultSetTimeout(ec);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"resource timeout check failed (nd="
				<< ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setSystemError(&e);
		}
	}
}

void CheckTimeoutHandler::checkReplicationTimeout(EventContext &ec) {
	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ReplicationId> timeoutResourceIds(alloc);

		transactionManager_->getReplicationTimeoutContextId(
			pgId, emNow, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				ReplicationContext &replContext =
					transactionManager_->get(pIds[i], timeoutResourceIds[i]);

				if (replContext.getTaskStatus() == ReplicationContext::TASK_CONTINUE) {
					continueEvent(ec, alloc, TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT, replContext);
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				} else {
					replySuccess(ec, alloc, TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT,
						replContext);
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
							<< ", contextId=" << timeoutResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			GS_TRACE_WARNING(REPLICATION_TIMEOUT,
				GS_TRACE_TXN_REPLICATION_TIMEOUT,
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

void CheckTimeoutHandler::checkTransactionTimeout(
	EventContext &ec, const util::XArray<bool> &checkPartitionFlagList) {

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ClientId> timeoutResourceIds(alloc);

		transactionManager_->getTransactionTimeoutContextId(alloc, pgId, emNow,
			checkPartitionFlagList, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				TransactionContext &txn = transactionManager_->get(
					alloc, pIds[i], timeoutResourceIds[i]);
				const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

				if (partitionTable_->isOwner(pIds[i])) {
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						util::XArray<const util::XArray<uint8_t> *>
							logRecordList(alloc);
						logRecordList.push_back(log);

						Request dummyRequest(alloc, FixedRequest::Source(
								UNDEF_PARTITIONID, UNDEF_EVENT_TYPE));

						Response response(alloc);
						executeReplication(dummyRequest, ec, alloc,
							NodeDescriptor::EMPTY_ND, txn,
							TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
							transactionManager_->getReplicationMode(), NULL, 0,
							logRecordList.data(), logRecordList.size(),
							response);
					}
				}
				else {
				}
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
							<< ", contextId=" << timeoutResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				 i < timeoutResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << timeoutResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(TRANSACTION_TIMEOUT,
				GS_TRACE_TXN_TRANSACTION_TIMEOUT,
				"(pgId=" << pgId
						 << ", timeoutResourceCount=" << timeoutResourceCount
						 << ", clientId=" << ss.str() << ")");
		}

		const PartitionId beginPId =
			transactionManager_->getPartitionGroupConfig()
				.getGroupBeginPartitionId(pgId);
		const PartitionId endPId =
			transactionManager_->getPartitionGroupConfig()
				.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId < endPId; pId++) {
			ec.setPendingPartitionChanged(pId);
		}
	}
	catch (DenyException &) {
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
											  << ", timeoutResourceCount="
											  << timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkRequestTimeout(
	EventContext &ec, const util::XArray<bool> &checkPartitionFlagList) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();
	util::TimeZone timeZone;

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ClientId> timeoutResourceIds(alloc);

		transactionManager_->getRequestTimeoutContextId(alloc, pgId, emNow,
			checkPartitionFlagList, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				if (partitionTable_->isOwner(pIds[i])) {
					TransactionContext &txn = transactionManager_->get(
						alloc, pIds[i], timeoutResourceIds[i]);
					const DataStore::Latch latch(
						txn, txn.getPartitionId(), dataStore_, clusterService_);

					util::XArray<ClientId> closedResourceIds(alloc);
					util::XArray<const util::XArray<uint8_t> *> logRecordList(
						alloc);

					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						logRecordList.push_back(log);

					}

					const TransactionManager::ContextSource src(
							TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
							txn.getContainerId(),
							txn.getTransationTimeoutInterval(),
							TransactionManager::AUTO,
							TransactionManager::AUTO_COMMIT,
							false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE,
							timeZone);

					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
					closedResourceIds.push_back(timeoutResourceIds[i]);

					txn = transactionManager_->put(
						alloc, pIds[i], timeoutResourceIds[i], src, now, emNow);
					Request dummyRequest(alloc, FixedRequest::Source(
							UNDEF_PARTITIONID, UNDEF_EVENT_TYPE));

					Response response(alloc);
					executeReplication(dummyRequest, ec, alloc,
						NodeDescriptor::EMPTY_ND, txn,
						TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
						transactionManager_->getReplicationMode(),
						closedResourceIds.data(), closedResourceIds.size(),
						logRecordList.data(), logRecordList.size(), response);

				}
				else {
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
							<< ", contextId=" << timeoutResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				 i < timeoutResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << timeoutResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(SESSION_TIMEOUT, GS_TRACE_TXN_SESSION_TIMEOUT,
				"(pgId=" << pgId
						 << ", timeoutResourceCount=" << timeoutResourceCount
						 << ", clientId=" << ss.str() << ")");
		}

		const PartitionId beginPId =
			transactionManager_->getPartitionGroupConfig()
				.getGroupBeginPartitionId(pgId);
		const PartitionId endPId =
			transactionManager_->getPartitionGroupConfig()
				.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId < endPId; pId++) {
			ec.setPendingPartitionChanged(pId);
		}
	}
	catch (DenyException &) {
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
											  << ", timeoutResourceCount="
											  << timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkKeepaliveTimeout(
	EventContext &ec, const util::XArray<bool> &checkPartitionFlagList) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();
	util::TimeZone timeZone;

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ClientId> timeoutResourceIds(alloc);

		transactionManager_->getKeepaliveTimeoutContextId(alloc, pgId, emNow,
			checkPartitionFlagList, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				if (partitionTable_->isOwner(pIds[i])) {
					TransactionContext &txn = transactionManager_->get(
						alloc, pIds[i], timeoutResourceIds[i]);
					const DataStore::Latch latch(
						txn, txn.getPartitionId(), dataStore_, clusterService_);

					util::XArray<ClientId> closedResourceIds(alloc);
					util::XArray<const util::XArray<uint8_t> *> logRecordList(
						alloc);

					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						logRecordList.push_back(log);

					}

					const TransactionManager::ContextSource src(
							TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
							txn.getContainerId(),
							txn.getTransationTimeoutInterval(),
							TransactionManager::AUTO,
							TransactionManager::AUTO_COMMIT,
							false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE,
							timeZone);

					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
					closedResourceIds.push_back(timeoutResourceIds[i]);

					txn = transactionManager_->put(
						alloc, pIds[i], timeoutResourceIds[i], src, now, emNow);
					Request dummyRequest(alloc, FixedRequest::Source(
							UNDEF_PARTITIONID, UNDEF_EVENT_TYPE));

					Response response(alloc);
					executeReplication(dummyRequest, ec, alloc,
						NodeDescriptor::EMPTY_ND, txn,
						TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
						transactionManager_->getReplicationMode(),
						closedResourceIds.data(), closedResourceIds.size(),
						logRecordList.data(), logRecordList.size(), response);
					transactionService_->incrementAbortDDLCount(pIds[i]);
				}
				else {
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
							<< ", contextId=" << timeoutResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				 i < timeoutResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << timeoutResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(TRANSACTION_TIMEOUT,
				GS_TRACE_TXN_KEEPALIVE_TIMEOUT,
				"(pgId=" << pgId
						 << ", timeoutResourceCount=" << timeoutResourceCount
						 << ", clientId=" << ss.str() << ")");
		}

		const PartitionId beginPId =
			transactionManager_->getPartitionGroupConfig()
				.getGroupBeginPartitionId(pgId);
		const PartitionId endPId =
			transactionManager_->getPartitionGroupConfig()
				.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId < endPId; pId++) {
			ec.setPendingPartitionChanged(pId);
		}
	}
	catch (DenyException &) {
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
											  << ", timeoutResourceCount="
											  << timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkNoExpireTransaction(util::StackAllocator &alloc, PartitionId pId) {

	util::TimeZone timeZone;
	util::StackAllocator::Scope scope(alloc);
	size_t noExpireResourceCount = 0;

	try {
		util::XArray<ClientId> noExpireResourceIds(alloc);

		transactionManager_->getNoExpireTransactionContextId(alloc, pId, noExpireResourceIds);
		noExpireResourceCount = noExpireResourceIds.size();

		for (size_t i = 0; i < noExpireResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				TransactionContext &txn = transactionManager_->get(
					alloc, pId, noExpireResourceIds[i]);
				const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

				util::XArray<ClientId> closedResourceIds(alloc);
				util::XArray<const util::XArray<uint8_t> *> logRecordList(
					alloc);

				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				if (abortOnError(txn, *log)) {
					logRecordList.push_back(log);

				}

				const TransactionManager::ContextSource src(
						TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
						txn.getContainerId(),
						txn.getTransationTimeoutInterval(),
						TransactionManager::AUTO,
						TransactionManager::AUTO_COMMIT,
						false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE,
						timeZone);

				transactionManager_->remove(pId, noExpireResourceIds[i]);
				closedResourceIds.push_back(noExpireResourceIds[i]);

				transactionService_->incrementAbortDDLCount(pId);
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pId
							<< ", contextId=" << noExpireResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (noExpireResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				 i < noExpireResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << noExpireResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(TRANSACTION_TIMEOUT,
				GS_TRACE_TXN_KEEPALIVE_TIMEOUT,
				"(pId=" << pId
						 << ", no expire noExpireResourceCount=" << noExpireResourceCount
						 << ", clientId=" << ss.str() << ")");
		}

	}
	catch (DenyException &) {
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pId=" << pId
											  << ", noExpireResourceCount="
											  << noExpireResourceCount << ")");
	}
}

void TransactionService::checkNoExpireTransaction(util::StackAllocator &alloc, PartitionId pId) {

	checkTimeoutHandler_.checkNoExpireTransaction(alloc, pId);

}

void CheckTimeoutHandler::checkResultSetTimeout(EventContext &ec) {
	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		transactionService_->getResultSetHolderManager().closeAll(
				alloc, pgId, *dataStore_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	try {
		dataStore_->checkTimeoutResultSet(pgId, emNow);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"(pId=" << pgId << ", timeoutResourceCount=" << timeoutResourceCount
					<< ")");
	}
}

bool CheckTimeoutHandler::isTransactionTimeoutCheckEnabled(
	PartitionGroupId pgId, util::XArray<bool> &checkPartitionFlagList) {
	const PartitionId beginPId =
		transactionManager_->getPartitionGroupConfig().getGroupBeginPartitionId(
			pgId);
	const PartitionId endPId =
		transactionManager_->getPartitionGroupConfig().getGroupEndPartitionId(
			pgId);

	bool existTarget = false;
	for (PartitionId pId = beginPId; pId < endPId; pId++) {
		const bool flag =
			transactionService_->isTransactionTimeoutCheckEnabled(pId);
		checkPartitionFlagList.push_back(flag);
		existTarget |= flag;
	}
	return existTarget;
}

DataStorePeriodicallyHandler::DataStorePeriodicallyHandler()
	: StatementHandler(), pIdCursor_(NULL) {
	pIdCursor_ = UTIL_NEW PartitionId[DataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < DataStore::MAX_PARTITION_NUM;
		 pgId++) {
		pIdCursor_[pgId] = UNDEF_PARTITIONID;
	}

	currentLimit_ = 0;
	currentPos_ = 0;
	expiredCheckCount_ = 3600*6;
	expiredCounter_ = 0;
}

DataStorePeriodicallyHandler::~DataStorePeriodicallyHandler() {
	delete[] pIdCursor_;
}

/*!
	@brief Handler Operator
*/
void DataStorePeriodicallyHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		if (clusterManager_->checkRecoveryCompleted()) {
			WATCHER_START;
			util::StackAllocator &alloc = ec.getAllocator();
			const util::DateTime now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig &pgConfig =
				transactionManager_->getPartitionGroupConfig();
			if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
			}

				expiredCounterList_[pgId]++;
				expiredCheckCount_ = dataStore_->getConfig().getCheckErasableExpiredInterval();
				bool newCheck = (expiredCounterList_[pgId] % expiredCheckCount_ == 0);
				bool continuosCheck = (startPosList_[pgId] != 0 
					&& (expiredCheckCount_ % PERIODICAL_TABLE_SCAN_WAIT_COUNT  == 0));
				if (newCheck || continuosCheck) {
					try {
						util::StackAllocator &alloc = ec.getAllocator();
						const util::DateTime now = ec.getHandlerStartTime();
						const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
						const PartitionGroupConfig &pgConfig =
								transactionManager_->getPartitionGroupConfig();
						PartitionId startPId = pgConfig.getGroupBeginPartitionId(pgId);
						PartitionId endPId = pgConfig.getGroupEndPartitionId(pgId);
						PartitionId currentPId;
						for (currentPId = startPId; currentPId < endPId; currentPId++) {
							if (!partitionTable_->isOwner(currentPId)) {
								continue;
							}
							TransactionManager::ContextSource src;
							TransactionContext &txn =
								transactionManager_->put(alloc, currentPId,
									TXN_EMPTY_CLIENTID, src, now, emNow);
							const DataStore::Latch latch(
									txn, txn.getPartitionId(), dataStore_, clusterService_);
							DataStore::ContainerCondition validContainerCondition(alloc);
							validContainerCondition.insertAttribute(CONTAINER_ATTR_LARGE);	
							Response response(alloc);
							util::XArray<FullContainerKey> &containerNameList = response.containerNameList_;
							int64_t start = startPosList_[pgId];
							ResultSize limit = PERIODICAL_TABLE_SCAN_MAX_COUNT;

							dataStore_->getContainerNameList(txn, currentPId, start,
								limit, UNDEF_DBID, validContainerCondition, containerNameList);
							size_t getSize = containerNameList.size();

							if (getSize == limit) {
								startPosList_[pgId] += limit; 
							}
							else if (start != 0 && getSize == 0) {
								startPosList_[pgId] = 0;
								start = startPosList_[pgId];
								if (newCheck) {
									dataStore_->getContainerNameList(txn, currentPId, start,
											limit, UNDEF_DBID, validContainerCondition, containerNameList);
									getSize = containerNameList.size();
									if (getSize == limit) {
										startPosList_[pgId] += limit; 
									}
									else {
										startPosList_[pgId] = 0;
									}
								}
							}
							else {
								startPosList_[pgId] = 0;
							}

							EventEngine &ee = ec.getEngine();
							for (size_t pos = 0; pos < containerNameList.size(); pos++) {
								util::String keyStr(alloc);
								containerNameList[pos].toString(alloc, keyStr) ;
								Event requestEv(ec, UPDATE_CONTAINER_STATUS, currentPId);
								EventByteOutStream out = requestEv.getOutStream();
								StatementMessage::FixedRequest::Source source(currentPId, UPDATE_CONTAINER_STATUS);
								StatementMessage::FixedRequest request(source);
								request.cxtSrc_.stmtId_ = 0;
								request.encode(out);
								StatementMessage::OptionSet optionalRequest(alloc);
								optionalRequest.set<StatementMessage::Options::REPLY_PID>(currentPId);
								bool systemMode = true;
								optionalRequest.set<StatementMessage::Options::SYSTEM_MODE>(systemMode);
								bool forSync = true;
								optionalRequest.set<StatementMessage::Options::FOR_SYNC>(forSync);
								ContainerId containerId = -1;
								optionalRequest.set<StatementMessage::Options::SUB_CONTAINER_ID>(containerId);
								optionalRequest.encode(out);
								encodeContainerKey(out, containerNameList[pos]);
								out << TABLE_PARTITIONING_CHECK_EXPIRED;
								int32_t bodySize = 0;
								out << bodySize;
								const NodeDescriptor &nd =  ee.getSelfServerND();
								requestEv.setSenderND(nd);
								ee.add(requestEv);
							}
						}
					} catch (std::exception &e) {
						UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e, "");
					}
				}

			BGTask nextTask;
			if (dataStore_ && !dataStore_->getCurrentBGTask(pgId, nextTask)) {
				PartitionId startPId = pgConfig.getGroupBeginPartitionId(pgId);
				PartitionId endPId = pgConfig.getGroupEndPartitionId(pgId);
				PartitionId currentPId;
				for (currentPId = startPId; currentPId < endPId; currentPId++) {
					if (dataStore_->isRestored(currentPId) && dataStore_->getObjectManager()->existPartition(currentPId)) {
						util::StackAllocator &alloc = ec.getAllocator();
						const util::DateTime now = ec.getHandlerStartTime();
						const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
						TransactionManager::ContextSource src;
						TransactionContext &txn =
							transactionManager_->put(alloc, currentPId,
								TXN_EMPTY_CLIENTID, src, now, emNow);


						const DataStore::Latch latch(
							txn, txn.getPartitionId(), dataStore_, clusterService_);
						if (dataStore_->searchBGTask(txn, txn.getPartitionId(), nextTask)) {
							Event continueEvent(ec, BACK_GROUND, nextTask.pId_);
							EventByteOutStream out = continueEvent.getOutStream();
							out << nextTask.bgId_;

							ec.getEngine().add(continueEvent);
							dataStore_->setCurrentBGTask(pgId, nextTask);
							break;
						}
					}
				}
			}

			if (dataStore_ &&
				dataStore_->isRestored(pIdCursor_[pgId]) &&
				dataStore_->getObjectManager()->existPartition(pIdCursor_[pgId])) {
				uint64_t periodMaxScanCount = dataStore_->getConfig().getBatchScanNum();

				uint64_t count = 0;
				PartitionId startPId = pIdCursor_[pgId];
				while (count < periodMaxScanCount) {
					Timestamp timestamp;
					if (dataStore_->getConfig().isAutoExpire()) {
						timestamp = ec.getHandlerStartTime().getUnixTime();
					} else {
						timestamp = dataStore_->getConfig().getErasableExpiredTime();
					}
					uint64_t maxScanNum = periodMaxScanCount - count;
					uint64_t scanNum = 0;
					bool isTail = dataStore_->executeBatchFree(
						ec.getAllocator(), pIdCursor_[pgId], timestamp,
						maxScanNum, scanNum);
					count += scanNum;
					if (isTail) {
						pIdCursor_[pgId]++;
						if (pIdCursor_[pgId] ==
							pgConfig.getGroupEndPartitionId(pgId)) {
							pIdCursor_[pgId] =
								pgConfig.getGroupBeginPartitionId(pgId);
						}
						if (pIdCursor_[pgId] == startPId) {
							break;
						}
					}
				}  
				dataStore_->getObjectManager()->resetRefCounter(
					pIdCursor_[pgId]);
			}
			else {
				pIdCursor_[pgId]++;
				if (pIdCursor_[pgId] == pgConfig.getGroupEndPartitionId(pgId)) {
					pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
				}
			}
			WATCHER_END_NORMAL(getEventTypeName(CHUNK_EXPIRE_PERIODICALLY));
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				   << ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setSystemError(&e);
		}
	}
}

AdjustStoreMemoryPeriodicallyHandler::AdjustStoreMemoryPeriodicallyHandler()
	: StatementHandler(), pIdCursor_(NULL) {
	pIdCursor_ = UTIL_NEW PartitionId[DataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < DataStore::MAX_PARTITION_NUM;
		 pgId++) {
		pIdCursor_[pgId] = UNDEF_PARTITIONID;
	}
}

AdjustStoreMemoryPeriodicallyHandler::~AdjustStoreMemoryPeriodicallyHandler() {
	delete[] pIdCursor_;
}

/*!
	@brief Handler Operator
*/
void AdjustStoreMemoryPeriodicallyHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		if (clusterManager_->checkRecoveryCompleted()) {
			WATCHER_START;
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig &pgConfig =
				transactionManager_->getPartitionGroupConfig();
			if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
			}

			chunkManager_->redistributeMemoryLimit(
				pIdCursor_[pgId]);  
			if (chunkManager_ &&
				chunkManager_->existPartition(pIdCursor_[pgId])) {
				chunkManager_->reconstructAffinityTable(pIdCursor_[pgId]);
				chunkManager_->adjustStoreMemory(
					pIdCursor_[pgId]);  
			}

			chunkManager_->adjustCheckpointMemoryPool();  

			pIdCursor_[pgId]++;
			if (pIdCursor_[pgId] == pgConfig.getGroupEndPartitionId(pgId)) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
			}

			WATCHER_END_NORMAL(getEventTypeName(ADJUST_STORE_MEMORY_PERIODICALLY));
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				   << ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setSystemError(&e);
		}
	}
}

BackgroundHandler::BackgroundHandler()
	: StatementHandler() {
	lsnCounter_ = UTIL_NEW LogSequentialNumber[DataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < DataStore::MAX_PARTITION_NUM;
		 pgId++) {
		lsnCounter_[pgId] = 0;
	}
}

BackgroundHandler::~BackgroundHandler() {
	delete[] lsnCounter_;
}

/*!
	@brief Handler Operator
*/
void BackgroundHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	EVENT_START(ec, ev, transactionManager_);

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	BGTask currentTask;
	currentTask.pId_ = request.fixed_.pId_;


	if (clusterManager_->isSystemError() || clusterManager_->isNormalShutdownCall()) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[BackgroundHandler::operator] stop BGService");
		return;
	}

	try {
		EventByteInStream in(ev.getInStream());
		decodeIntData<BackgroundId>(in, currentTask.bgId_);
		const PartitionGroupId pgId = ec.getWorkerId();
		if (clusterManager_->checkRecoveryCompleted() && dataStore_) {
			bool isFinished = true;

			const uint64_t startClock =
				util::Stopwatch::currentClock();
			if (dataStore_->isRestored(request.fixed_.pId_) &&
				dataStore_->getObjectManager()->existPartition(request.fixed_.pId_)) {
				TransactionContext &txn = transactionManager_->put(
						alloc, request.fixed_.pId_,
						TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
				const DataStore::Latch latch(
						txn, txn.getPartitionId(), dataStore_, clusterService_);

				isFinished = dataStore_->executeBGTask(txn, currentTask.bgId_);
				transactionService_->incrementBackgroundOperationCount(request.fixed_.pId_);
			}
			const uint32_t lap = util::Stopwatch::clockToMillis(
				util::Stopwatch::currentClock() - startClock);
			int32_t waitTime = getWaitTime(ec, ev, lap);
			if (isFinished) {
				dataStore_->resetCurrentBGTask(pgId);
				BGTask nextTask;
				PartitionId currentPId = request.fixed_.pId_;
				const PartitionGroupConfig &pgConfig = transactionManager_->getPartitionGroupConfig();
				for (uint32_t i = 0; i < pgConfig.getGroupPartitonCount(pgId); i++) {
					TransactionManager::ContextSource src;
					TransactionContext &txn =
						transactionManager_->put(alloc, currentPId,
							TXN_EMPTY_CLIENTID, src, now, emNow);
					if (dataStore_->isRestored(currentPId) &&
						dataStore_->getObjectManager()->existPartition(currentPId) &&
					    dataStore_->searchBGTask(txn, currentPId, nextTask)) {
						Event continueEvent(ec, BACK_GROUND, nextTask.pId_);
						EventByteOutStream out = continueEvent.getOutStream();
						out << nextTask.bgId_;

						ec.getEngine().addTimer(continueEvent, waitTime);
						dataStore_->setCurrentBGTask(pgId, nextTask);
						break;
					}
					currentPId++;
					if (currentPId == pgConfig.getGroupEndPartitionId(pgId)) {
						currentPId = pgConfig.getGroupBeginPartitionId(pgId);
					}
				}
			} else {
				dataStore_->setCurrentBGTask(pgId, currentTask);
				ec.getEngine().addTimer(ev, waitTime);
			}
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				   << ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setSystemError(&e);
		}
	}
}

int32_t BackgroundHandler::getWaitTime(EventContext &ec, Event &ev, int32_t opeTime) {
	int64_t executableCount = 0;
	int64_t afterCount = 0;
	int32_t waitTime = transactionService_->getWaitTime(
			ec, &ev, opeTime, executableCount, afterCount, TXN_BACKGROUND);
	GS_TRACE_INFO(
		DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[BackgroundHandler PartitionId = " << ev.getPartitionId()
			<< ", lsnDiff = " << diffLsn(ev.getPartitionId())
			<< ", opeTime = " << opeTime
			<< ", queueSize = " << executableCount + afterCount
			<< ", waitTime = " << waitTime);

	return waitTime;
}

uint64_t BackgroundHandler::diffLsn(PartitionId pId) {
	const PartitionGroupConfig &pgConfig = transactionManager_->getPartitionGroupConfig();
	PartitionGroupId pgId = pgConfig.getPartitionGroupId(pId);

	uint64_t prev = lsnCounter_[pgId];
	uint64_t current = 0;
	for (PartitionId currentPId = pgConfig.getGroupBeginPartitionId(pgId);
		currentPId < pgConfig.getGroupEndPartitionId(pgId); currentPId++) {
		current += logManager_->getLSN(currentPId);
	}
	uint64_t diff = current - prev;
	lsnCounter_[pgId] = current;
	return diff;
}

ContinueCreateDDLHandler::ContinueCreateDDLHandler()
	: StatementHandler() {
}

ContinueCreateDDLHandler::~ContinueCreateDDLHandler() {
}

/*!
	@brief Handler Operator
*/
void ContinueCreateDDLHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	EVENT_START(ec, ev, transactionManager_);


	if (clusterManager_->isSystemError() || clusterManager_->isNormalShutdownCall()) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[ContinueCreateDDLHandler::operator] stop Service");
		return;
	}

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		request.fixed_.decode(in);

		in >> request.fixed_.startStmtId_;
		if (in.base().remaining() != 0) {
			decodeRequestOptionPart(in, request, connOption);
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::GET;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_CONTINUE;
		TransactionContext &txn = transactionManager_->get(
				alloc, request.fixed_.pId_, request.fixed_.clientId_);
		const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		NodeDescriptor originalND = txn.getSenderND();
		EventType replyEventType = UNDEF_EVENT_TYPE;
		bool isFinished = false;
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);
		{
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);

			switch (ev.getType()) {
			case CONTINUE_CREATE_INDEX:	{
					IndexCursor indexCursor = container->getIndexCursor(txn);
					container->continueCreateIndex(txn, indexCursor);
					isFinished = indexCursor.isFinished();
					const LogSequentialNumber lsn = logManager_->putContinueCreateIndexLog(
							*log,
							txn.getPartitionId(), txn.getClientId(), txn.getId(),
							txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
							txn.getTransationTimeoutInterval(),
							request.fixed_.cxtSrc_.getMode_, true, false, indexCursor.getRowId());

					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					replyEventType = CREATE_INDEX;
				}
				break;
			case CONTINUE_ALTER_CONTAINER: {
					ContainerCursor containerCursor = container->getContainerCursor(txn);
					container->continueChangeSchema(txn, containerCursor);
					isFinished = containerCursor.isFinished();

					if (isFinished) {
						ContainerAutoPtr newContainerAutoPtr(txn, dataStore_, txn.getPartitionId(),
							containerCursor);
						BaseContainer *newContainer = newContainerAutoPtr.getBaseContainer();
						checkContainerExistence(newContainer);
						response.schemaVersionId_ = newContainer->getVersionId();
						response.containerId_ = newContainer->getContainerId();
						const FullContainerKey containerKey =
							newContainer->getContainerKey(txn);
						const void *createdContainerNameBinary;
						size_t createdContainerNameBinarySize;
						containerKey.toBinary(
							createdContainerNameBinary, createdContainerNameBinarySize);
						response.binaryData2_.assign(
							static_cast<const uint8_t*>(createdContainerNameBinary),
							static_cast<const uint8_t*>(createdContainerNameBinary) + createdContainerNameBinarySize);

						const bool optionIncluded = true;
						bool internalOptionIncluded = true;
						newContainer->getContainerInfo(txn, response.binaryData_, optionIncluded, internalOptionIncluded);
					}

					const LogSequentialNumber lsn = logManager_->putContinueAlterContainerLog(
							*log,
							txn.getPartitionId(), txn.getClientId(), txn.getId(),
							txn.getContainerId(), request.fixed_.cxtSrc_.stmtId_,
							txn.getTransationTimeoutInterval(),
							request.fixed_.cxtSrc_.getMode_, true, false, containerCursor.getRowId());

					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					replyEventType = PUT_CONTAINER;
				}
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
			}
			logRecordList.push_back(log);
		}
		transactionService_->incrementNoExpireOperationCount(request.fixed_.pId_);

		int32_t delayTime = 0;
		ReplicationContext::TaskStatus taskStatus = ReplicationContext::TASK_CONTINUE;
		if (isFinished) {
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);

			const LogSequentialNumber lsn = logManager_->putCommitTransactionLog(
					*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.fixed_.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->commit(txn, *container);
			logRecordList.push_back(log);

			ec.setPendingPartitionChanged(request.fixed_.pId_);

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

			taskStatus = ReplicationContext::TASK_FINISHED;
		} else {
			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}

		const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, replyEventType, request.fixed_.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_SEMISYNC, taskStatus,
				closedResourceIds.data(), closedResourceIds.size(),
				logRecordList.data(), logRecordList.size(), request.fixed_.startStmtId_, delayTime,
				response);

		if (isFinished) {

			request.fixed_.cxtSrc_.stmtId_ = request.fixed_.startStmtId_;
			replySuccess(
					ec, alloc, originalND, replyEventType,
					TXN_STATEMENT_SUCCESS, request, response, ackWait);
		} else {
			continueEvent(
					ec, alloc, ev.getSenderND(), replyEventType, request.fixed_.startStmtId_,
					request, response, ackWait);
		}
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

UpdateDataStoreStatusHandler::UpdateDataStoreStatusHandler()
	: StatementHandler() {
}

UpdateDataStoreStatusHandler::~UpdateDataStoreStatusHandler() {
}

/*!
	@brief Handler Operator
*/
void UpdateDataStoreStatusHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	BGTask currentTask;
	currentTask.pId_ = request.fixed_.pId_;
	EVENT_START(ec, ev, transactionManager_);

	try {
		EventByteInStream in(ev.getInStream());
		Timestamp inputTime;
		decodeLongData<Timestamp>(in, inputTime);
		bool force;
		decodeBooleanData(in, force);

		if (force) {
			GS_TRACE_WARNING(DATA_STORE,
				GS_TRACE_DS_DS_CHANGE_STATUS,
				"Force Update Latest Expration Check Time :" << inputTime);
		} else {
			GS_TRACE_INFO(DATA_STORE,
				GS_TRACE_DS_DS_CHANGE_STATUS,
				"Update Latest Expration Check Time :" << inputTime);
		}

		const PartitionGroupId pgId = ec.getWorkerId();
		if (clusterManager_->checkRecoveryCompleted() && dataStore_) {
			bool isFinished = true;

			const PartitionGroupConfig &pgConfig = transactionManager_->getPartitionGroupConfig();
			for (PartitionId currentPId = pgConfig.getGroupBeginPartitionId(pgId);
				currentPId < pgConfig.getGroupEndPartitionId(pgId); currentPId++) {

				if (dataStore_->isRestored(currentPId) &&
					dataStore_->getObjectManager()->existPartition(currentPId)) {
					TransactionContext &txn = transactionManager_->put(
							alloc, currentPId,
							TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
					const DataStore::Latch latch(
							txn, txn.getPartitionId(), dataStore_, clusterService_);

					dataStore_->setLastExpiredTime(txn.getPartitionId(), inputTime, force);
				}
			}
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				   << ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setSystemError(&e);
		}
	}
}


void TransactionService::requestUpdateDataStoreStatus(
	const Event::Source &eventSource, Timestamp time, bool force) {
	try {
		for (PartitionGroupId pgId = 0;
			 pgId < pgConfig_.getPartitionGroupCount(); pgId++) {
			
			Event requestEvent(eventSource,	UPDATE_DATA_STORE_STATUS, 
				pgConfig_.getGroupBeginPartitionId(pgId));
			EventByteOutStream out = requestEvent.getOutStream();
			StatementHandler::encodeLongData<Timestamp>(out, time);
			StatementHandler::encodeBooleanData(out, force);

			getEE()->add(requestEvent);
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Handler Operator
*/
void UnsupportedStatementHandler::operator()(EventContext &ec, Event &ev) {
	util::StackAllocator &alloc = ec.getAllocator();
	EVENT_START(ec, ev, transactionManager_);

	if (ev.getSenderND().isEmpty()) return;

	Request request(alloc, getRequestSource(ev));

	try {
		EventByteInStream in(ev.getInStream());

		decodeLongData<StatementId>(in, request.fixed_.cxtSrc_.stmtId_);

		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNSUPPORTED,
			"Unsupported statement type");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void UnknownStatementHandler::operator()(EventContext &ec, Event &ev) {
	EVENT_START(ec, ev, transactionManager_);
	try {
		GS_THROW_USER_ERROR(
			GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "Unknown statement type");
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e) << " (nd=" << ev.getSenderND()
									<< ", pId=" << ev.getPartitionId()
									<< ", eventType=" << ev.getType() << ")");
		EventType eventType = ev.getType();
		if (eventType >= V_1_5_SYNC_START && eventType <= V_1_5_SYNC_END) {
			GS_TRACE_WARNING(TRANSACTION_SERVICE,
				GS_TRACE_TXN_CLUSTER_VERSION_UNMATCHED,
				"Cluster version unmatched, eventType:" << eventType
														<< " is v1.5");
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e) << " (nd=" << ev.getSenderND()
									<< ", pId=" << ev.getPartitionId()
									<< ", eventType=" << ev.getType() << ")");
		clusterService_->setSystemError(&e);
	}
}

/*!
	@brief Handler Operator
*/
void IgnorableStatementHandler::operator()(EventContext &, Event &ev) {
	GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_REQUEST_IGNORED,
		"(nd=" << ev.getSenderND() << ", type=" << ev.getType() << ")");
}

TransactionService::TransactionService(ConfigTable &config,
	const EventEngine::Config &, const EventEngine::Source &eeSource,
	const char *name)
	: eeSource_(eeSource),
	  ee_(NULL),
	  initalized_(false),
	  pgConfig_(config),
	  serviceConfig_(config),
	  enableTxnTimeoutCheck_(pgConfig_.getPartitionCount(),
		  static_cast< util::Atomic<bool> >(false)),
	  readOperationCount_(pgConfig_.getPartitionCount(), 0),
	  writeOperationCount_(pgConfig_.getPartitionCount(), 0),
	  rowReadCount_(pgConfig_.getPartitionCount(), 0),
	  rowWriteCount_(pgConfig_.getPartitionCount(), 0),
	  backgroundOperationCount_(pgConfig_.getPartitionCount(), 0),
	  noExpireOperationCount_(pgConfig_.getPartitionCount(), 0),
	  abortDDLCount_(pgConfig_.getPartitionCount(), 0),
	  connectHandler_(StatementHandler::TXN_CLIENT_VERSION, ACCEPTABLE_NOSQL_CLIENT_VERSIONS)
{
	try {
		eeConfig_.setServerNDAutoNumbering(false);
		eeConfig_.setClientNDEnabled(true);

		eeConfig_.setConcurrency(config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));
		eeConfig_.setPartitionCount(
			config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

		ClusterAdditionalServiceConfig addConfig(config);
		const char *targetAddress = addConfig.getServiceAddress(TRANSACTION_SERVICE);
		eeConfig_.setServerAddress(targetAddress,
			config.getUInt16(CONFIG_TABLE_TXN_SERVICE_PORT));

		if (ClusterService::isMulticastMode(config)) {
			eeConfig_.setMulticastAddress(
				config.get<const char8_t *>(
					CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
				config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
			if (strlen(config.get<const char8_t *>(
					CONFIG_TABLE_TXN_NOTIFICATION_INTERFACE_ADDRESS)) != 0) {
				eeConfig_.setMulticastIntefaceAddress(
						config.get<const char8_t *>(
							CONFIG_TABLE_TXN_NOTIFICATION_INTERFACE_ADDRESS),
						config.getUInt16(CONFIG_TABLE_TXN_SERVICE_PORT));
			}
		}

		eeConfig_.connectionCountLimit_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_CONNECTION_LIMIT);

		eeConfig_.keepaliveEnabled_ =
			config.get<bool>(CONFIG_TABLE_TXN_USE_KEEPALIVE);
		eeConfig_.keepaliveIdle_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_KEEPALIVE_IDLE);
		eeConfig_.keepaliveCount_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_KEEPALIVE_COUNT);
		eeConfig_.keepaliveInterval_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_KEEPALIVE_INTERVAL);

		eeConfig_.eventBufferSizeLimit_ = ConfigTable::megaBytesToBytes(
			config.getUInt32(CONFIG_TABLE_TXN_TOTAL_MESSAGE_MEMORY_LIMIT));

		eeConfig_.setAllAllocatorGroup(ALLOCATOR_GROUP_TXN_MESSAGE);
		eeConfig_.workAllocatorGroupId_ = ALLOCATOR_GROUP_TXN_WORK;

		ee_ = UTIL_NEW EventEngine(eeConfig_, eeSource_, name);


	}
	catch (std::exception &e) {
		delete ee_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

TransactionService::~TransactionService() {
	ee_->shutdown();
	ee_->waitForShutdown();

	TransactionManager *transactionManager =
		connectHandler_.transactionManager_;
	if (transactionManager != NULL) {
		for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
			transactionManager->removePartition(pId);
		}

		transactionManager->removeAllReplicationContext();
		transactionManager->removeAllAuthenticationContext();

		for (PartitionGroupId pgId = 0;
			 pgId < pgConfig_.getPartitionGroupCount(); pgId++) {
			assert(transactionManager->getTransactionContextCount(pgId) == 0);
			assert(transactionManager->getReplicationContextCount(pgId) == 0);
			assert(
				transactionManager->getAuthenticationContextCount(pgId) == 0);
		}
	}

	delete ee_;
}

void TransactionService::initialize(const ResourceSet &resourceSet) {
	try {
		ee_->setUserDataType<StatementHandler::ConnectionOption>();

		ee_->setThreadErrorHandler(serviceThreadErrorHandler_);
		serviceThreadErrorHandler_.initialize(resourceSet);

		connectHandler_.initialize(resourceSet);
		ee_->setHandler(CONNECT, connectHandler_);
		ee_->setHandlingMode(CONNECT, EventEngine::HANDLING_IMMEDIATE);

		disconnectHandler_.initialize(resourceSet);
		ee_->setCloseEventHandler(DISCONNECT, disconnectHandler_);
		ee_->setHandlingMode(
			DISCONNECT, EventEngine::HANDLING_PARTITION_SERIALIZED);

		loginHandler_.initialize(resourceSet);
		ee_->setHandler(LOGIN, loginHandler_);
		ee_->setHandlingMode(LOGIN, EventEngine::HANDLING_PARTITION_SERIALIZED);

		logoutHandler_.initialize(resourceSet);
		ee_->setHandler(LOGOUT, logoutHandler_);
		ee_->setHandlingMode(
			LOGOUT, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getPartitionAddressHandler_.initialize(resourceSet);
		ee_->setHandler(GET_PARTITION_ADDRESS, getPartitionAddressHandler_);
		ee_->setHandlingMode(
			GET_PARTITION_ADDRESS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getPartitionContainerNamesHandler_.initialize(resourceSet);
		ee_->setHandler(
			GET_PARTITION_CONTAINER_NAMES, getPartitionContainerNamesHandler_);
		ee_->setHandlingMode(GET_PARTITION_CONTAINER_NAMES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerPropertiesHandler_.initialize(resourceSet);
		ee_->setHandler(
			GET_CONTAINER_PROPERTIES, getContainerPropertiesHandler_);
		ee_->setHandlingMode(GET_CONTAINER_PROPERTIES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		putContainerHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_CONTAINER, putContainerHandler_);
		ee_->setHandlingMode(
			PUT_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putLargeContainerHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_LARGE_CONTAINER, putLargeContainerHandler_);
		ee_->setHandlingMode(
			PUT_LARGE_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateContainerStatusHandler_.initialize(resourceSet);
		ee_->setHandler(UPDATE_CONTAINER_STATUS, updateContainerStatusHandler_);
		ee_->setHandlingMode(
			UPDATE_CONTAINER_STATUS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createLargeIndexHandler_.initialize(resourceSet);
		ee_->setHandler(CREATE_LARGE_INDEX, createLargeIndexHandler_);
		ee_->setHandlingMode(
			CREATE_LARGE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);


		dropContainerHandler_.initialize(resourceSet);
		ee_->setHandler(DROP_CONTAINER, dropContainerHandler_);
		ee_->setHandlingMode(
			DROP_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerHandler_.initialize(resourceSet);
		ee_->setHandler(GET_COLLECTION, getContainerHandler_);
		ee_->setHandlingMode(
			GET_COLLECTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createIndexHandler_.initialize(resourceSet);
		ee_->setHandler(CREATE_INDEX, createIndexHandler_);
		ee_->setHandlingMode(
			CREATE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropIndexHandler_.initialize(resourceSet);
		ee_->setHandler(DELETE_INDEX, dropIndexHandler_);
		ee_->setHandlingMode(
			DELETE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDropTriggerHandler_.initialize(resourceSet);
		ee_->setHandler(CREATE_TRIGGER, createDropTriggerHandler_);
		ee_->setHandlingMode(
			CREATE_TRIGGER, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(DELETE_TRIGGER, createDropTriggerHandler_);
		ee_->setHandlingMode(
			DELETE_TRIGGER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		flushLogHandler_.initialize(resourceSet);
		ee_->setHandler(FLUSH_LOG, flushLogHandler_);
		ee_->setHandlingMode(
			FLUSH_LOG, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createTransactionContextHandler_.initialize(resourceSet);
		ee_->setHandler(
			CREATE_TRANSACTION_CONTEXT, createTransactionContextHandler_);
		ee_->setHandlingMode(CREATE_TRANSACTION_CONTEXT,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		closeTransactionContextHandler_.initialize(resourceSet);
		ee_->setHandler(
			CLOSE_TRANSACTION_CONTEXT, closeTransactionContextHandler_);
		ee_->setHandlingMode(CLOSE_TRANSACTION_CONTEXT,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		commitAbortTransactionHandler_.initialize(resourceSet);
		ee_->setHandler(COMMIT_TRANSACTION, commitAbortTransactionHandler_);
		ee_->setHandlingMode(
			COMMIT_TRANSACTION, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(ABORT_TRANSACTION, commitAbortTransactionHandler_);
		ee_->setHandlingMode(
			ABORT_TRANSACTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putRowHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_ROW, putRowHandler_);
		ee_->setHandlingMode(
			PUT_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putRowSetHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_MULTIPLE_ROWS, putRowSetHandler_);
		ee_->setHandlingMode(
			PUT_MULTIPLE_ROWS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowHandler_.initialize(resourceSet);
		ee_->setHandler(REMOVE_ROW, removeRowHandler_);
		ee_->setHandlingMode(
			REMOVE_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateRowByIdHandler_.initialize(resourceSet);
		ee_->setHandler(UPDATE_ROW_BY_ID, updateRowByIdHandler_);
		ee_->setHandlingMode(
			UPDATE_ROW_BY_ID, EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowByIdHandler_.initialize(resourceSet);
		ee_->setHandler(REMOVE_ROW_BY_ID, removeRowByIdHandler_);
		ee_->setHandlingMode(
			REMOVE_ROW_BY_ID, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowHandler_.initialize(resourceSet);
		ee_->setHandler(GET_ROW, getRowHandler_);
		ee_->setHandlingMode(
			GET_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowSetHandler_.initialize(resourceSet);
		ee_->setHandler(GET_MULTIPLE_ROWS, getRowSetHandler_);
		ee_->setHandlingMode(
			GET_MULTIPLE_ROWS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTqlHandler_.initialize(resourceSet);
		ee_->setHandler(QUERY_TQL, queryTqlHandler_);
		ee_->setHandlingMode(
			QUERY_TQL, EventEngine::HANDLING_PARTITION_SERIALIZED);

		appendRowHandler_.initialize(resourceSet);
		ee_->setHandler(APPEND_TIME_SERIES_ROW, appendRowHandler_);
		ee_->setHandlingMode(
			APPEND_TIME_SERIES_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryGeometryRelatedHandler_.initialize(resourceSet);
		ee_->setHandler(
			QUERY_COLLECTION_GEOMETRY_RELATED, queryGeometryRelatedHandler_);
		ee_->setHandlingMode(QUERY_COLLECTION_GEOMETRY_RELATED,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryGeometryWithExclusionHandler_.initialize(resourceSet);
		ee_->setHandler(QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
			queryGeometryWithExclusionHandler_);
		ee_->setHandlingMode(QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowTimeRelatedHandler_.initialize(resourceSet);
		ee_->setHandler(GET_TIME_SERIES_ROW_RELATED, getRowTimeRelatedHandler_);
		ee_->setHandlingMode(GET_TIME_SERIES_ROW_RELATED,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowInterpolateHandler_.initialize(resourceSet);
		ee_->setHandler(INTERPOLATE_TIME_SERIES_ROW, getRowInterpolateHandler_);
		ee_->setHandlingMode(INTERPOLATE_TIME_SERIES_ROW,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		aggregateHandler_.initialize(resourceSet);
		ee_->setHandler(AGGREGATE_TIME_SERIES, aggregateHandler_);
		ee_->setHandlingMode(
			AGGREGATE_TIME_SERIES, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTimeRangeHandler_.initialize(resourceSet);
		ee_->setHandler(QUERY_TIME_SERIES_RANGE, queryTimeRangeHandler_);
		ee_->setHandlingMode(QUERY_TIME_SERIES_RANGE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTimeSamplingHandler_.initialize(resourceSet);
		ee_->setHandler(QUERY_TIME_SERIES_SAMPLING, queryTimeSamplingHandler_);
		ee_->setHandlingMode(QUERY_TIME_SERIES_SAMPLING,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		fetchResultSetHandler_.initialize(resourceSet);
		ee_->setHandler(FETCH_RESULT_SET, fetchResultSetHandler_);
		ee_->setHandlingMode(
			FETCH_RESULT_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		closeResultSetHandler_.initialize(resourceSet);
		ee_->setHandler(CLOSE_RESULT_SET, closeResultSetHandler_);
		ee_->setHandlingMode(
			CLOSE_RESULT_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiCreateTransactionContextHandler_.initialize(resourceSet);
		ee_->setHandler(CREATE_MULTIPLE_TRANSACTION_CONTEXTS,
			multiCreateTransactionContextHandler_);
		ee_->setHandlingMode(CREATE_MULTIPLE_TRANSACTION_CONTEXTS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiCloseTransactionContextHandler_.initialize(resourceSet);
		ee_->setHandler(CLOSE_MULTIPLE_TRANSACTION_CONTEXTS,
			multiCloseTransactionContextHandler_);
		ee_->setHandlingMode(CLOSE_MULTIPLE_TRANSACTION_CONTEXTS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiPutHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_MULTIPLE_CONTAINER_ROWS, multiPutHandler_);
		ee_->setHandlingMode(PUT_MULTIPLE_CONTAINER_ROWS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiGetHandler_.initialize(resourceSet);
		ee_->setHandler(GET_MULTIPLE_CONTAINER_ROWS, multiGetHandler_);
		ee_->setHandlingMode(GET_MULTIPLE_CONTAINER_ROWS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiQueryHandler_.initialize(resourceSet);
		ee_->setHandler(EXECUTE_MULTIPLE_QUERIES, multiQueryHandler_);
		ee_->setHandlingMode(EXECUTE_MULTIPLE_QUERIES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationLogHandler_.initialize(resourceSet);

		replicationAckHandler_.initialize(resourceSet);

		replicationLog2Handler_.initialize(resourceSet);
		replicationAck2Handler_.initialize(resourceSet);

		authenticationHandler_.initialize(resourceSet);
		ee_->setHandler(AUTHENTICATION, authenticationHandler_);
		ee_->setHandlingMode(
			AUTHENTICATION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		authenticationAckHandler_.initialize(resourceSet);
		ee_->setHandler(AUTHENTICATION_ACK, authenticationAckHandler_);
		ee_->setHandlingMode(
			AUTHENTICATION_ACK, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putUserHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_USER, putUserHandler_);
		ee_->setHandlingMode(
			PUT_USER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropUserHandler_.initialize(resourceSet);
		ee_->setHandler(DROP_USER, dropUserHandler_);
		ee_->setHandlingMode(
			DROP_USER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getUsersHandler_.initialize(resourceSet);
		ee_->setHandler(GET_USERS, getUsersHandler_);
		ee_->setHandlingMode(
			GET_USERS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putDatabaseHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_DATABASE, putDatabaseHandler_);
		ee_->setHandlingMode(
			PUT_DATABASE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropDatabaseHandler_.initialize(resourceSet);
		ee_->setHandler(DROP_DATABASE, dropDatabaseHandler_);
		ee_->setHandlingMode(
			DROP_DATABASE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getDatabasesHandler_.initialize(resourceSet);
		ee_->setHandler(GET_DATABASES, getDatabasesHandler_);
		ee_->setHandlingMode(
			GET_DATABASES, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putPrivilegeHandler_.initialize(resourceSet);
		ee_->setHandler(PUT_PRIVILEGE, putPrivilegeHandler_);
		ee_->setHandlingMode(
			PUT_PRIVILEGE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropPrivilegeHandler_.initialize(resourceSet);
		ee_->setHandler(DROP_PRIVILEGE, dropPrivilegeHandler_);
		ee_->setHandlingMode(
			DROP_PRIVILEGE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		checkTimeoutHandler_.initialize(resourceSet);
		ee_->setHandler(TXN_COLLECT_TIMEOUT_RESOURCE, checkTimeoutHandler_);
		ee_->setHandlingMode(TXN_COLLECT_TIMEOUT_RESOURCE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.initialize(resourceSet);
		ee_->setHandler(
			CHUNK_EXPIRE_PERIODICALLY, dataStorePeriodicallyHandler_);
		ee_->setHandlingMode(CHUNK_EXPIRE_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.setConcurrency(resourceSet.txnSvc_->pgConfig_.getPartitionGroupCount());

		adjustStoreMemoryPeriodicallyHandler_.initialize(resourceSet);
		ee_->setHandler(ADJUST_STORE_MEMORY_PERIODICALLY,
			adjustStoreMemoryPeriodicallyHandler_);
		ee_->setHandlingMode(ADJUST_STORE_MEMORY_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		backgroundHandler_.initialize(resourceSet);
		ee_->setHandler(BACK_GROUND, backgroundHandler_);
		ee_->setHandlingMode(BACK_GROUND,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDDLContinueHandler_.initialize(resourceSet);
		ee_->setHandler(CONTINUE_CREATE_INDEX, createDDLContinueHandler_);
		ee_->setHandlingMode(CONTINUE_CREATE_INDEX,
			EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(CONTINUE_ALTER_CONTAINER, createDDLContinueHandler_);
		ee_->setHandlingMode(CONTINUE_ALTER_CONTAINER,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateDataStoreStatusHandler_.initialize(resourceSet);
		ee_->setHandler(UPDATE_DATA_STORE_STATUS, updateDataStoreStatusHandler_);
		ee_->setHandlingMode(UPDATE_DATA_STORE_STATUS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowSetByIdHandler_.initialize(resourceSet);
		ee_->setHandler(REMOVE_MULTIPLE_ROWS_BY_ID_SET, removeRowSetByIdHandler_);
		ee_->setHandlingMode(
			REMOVE_MULTIPLE_ROWS_BY_ID_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);
		updateRowSetByIdHandler_.initialize(resourceSet);
		ee_->setHandler(UPDATE_MULTIPLE_ROWS_BY_ID_SET, updateRowSetByIdHandler_);
		ee_->setHandlingMode(
			UPDATE_MULTIPLE_ROWS_BY_ID_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);
		writeLogPeriodicallyHandler_.initialize(resourceSet);
		ee_->setHandler(WRITE_LOG_PERIODICALLY, writeLogPeriodicallyHandler_);
		ee_->setHandlingMode(
			WRITE_LOG_PERIODICALLY, EventEngine::HANDLING_PARTITION_SERIALIZED);

		unsupportedStatementHandler_.initialize(resourceSet);
		for (EventType stmtType = V_1_1_STATEMENT_START;
			 stmtType <= V_1_1_STATEMENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		const EventType V1_5_X_STATEMENTS[] = {GET_TIME_SERIES, PUT_TIME_SERIES,
			DELETE_COLLECTION, CREATE_EVENT_NOTIFICATION,
			DELETE_EVENT_NOTIFICATION, GET_TIME_SERIES_ROW,
			QUERY_TIME_SERIES_TQL, PUT_TIME_SERIES_ROW, PUT_TIME_SERIES_ROWSET,
			DELETE_TIME_SERIES_ROW, GET_TIME_SERIES_MULTIPLE_ROWS,
			UNDEF_EVENT_TYPE};
		for (size_t i = 0; V1_5_X_STATEMENTS[i] != UNDEF_EVENT_TYPE; i++) {
			ee_->setHandler(V1_5_X_STATEMENTS[i], unsupportedStatementHandler_);
			ee_->setHandlingMode(V1_5_X_STATEMENTS[i],
				EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		for (EventType stmtType = V_1_X_REPLICATION_EVENT_START;
			 stmtType <= V_1_X_REPLICATION_EVENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}

		unsupportedStatementHandler_.initialize(resourceSet);
		for (EventType stmtType = V_1_1_STATEMENT_START;
			 stmtType <= V_1_1_STATEMENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		for (EventType stmtType = V_1_X_REPLICATION_EVENT_START;
			 stmtType <= V_1_X_REPLICATION_EVENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}

		unknownStatementHandler_.initialize(resourceSet);
		ee_->setUnknownEventHandler(unknownStatementHandler_);

		ignorableStatementHandler_.initialize(resourceSet);
		ee_->setHandler(RECV_NOTIFY_MASTER, ignorableStatementHandler_);
		ee_->setHandlingMode(
			RECV_NOTIFY_MASTER, EventEngine::HANDLING_IMMEDIATE);

		shortTermSyncHandler_.initialize(resourceSet);
		ee_->setHandler(TXN_SHORTTERM_SYNC_REQUEST, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_START, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_START_ACK, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_LOG, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_LOG_ACK, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_END, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_END_ACK, shortTermSyncHandler_);

		longTermSyncHandler_.initialize(resourceSet);
		setClusterHandler();

		syncCheckEndHandler_.initialize(resourceSet);
		ee_->setHandler(TXN_SYNC_TIMEOUT, syncCheckEndHandler_);
		ee_->setHandler(TXN_SYNC_CHECK_END, syncCheckEndHandler_);

		changePartitionTableHandler_.initialize(resourceSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_TABLE, changePartitionTableHandler_);
		changePartitionStateHandler_.initialize(resourceSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_STATE, changePartitionStateHandler_);
		dropPartitionHandler_.initialize(resourceSet);
		ee_->setHandler(TXN_DROP_PARTITION, dropPartitionHandler_);

		checkpointOperationHandler_.initialize(resourceSet);
		ee_->setHandler(PARTITION_GROUP_START, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_START, checkpointOperationHandler_);
		ee_->setHandler(COPY_CHUNK, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_END, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_GROUP_END, checkpointOperationHandler_);
		ee_->setHandler(CLEANUP_CP_DATA, checkpointOperationHandler_);
		ee_->setHandler(CLEANUP_LOG_FILES, checkpointOperationHandler_);
		ee_->setHandler(CP_TXN_PREPARE_LONGTERM_SYNC, checkpointOperationHandler_);
		ee_->setHandler(CP_TXN_STOP_LONGTERM_SYNC, checkpointOperationHandler_);

		ee_->setHandler(SQL_GET_CONTAINER, sqlGetContainerHandler_);
		sqlGetContainerHandler_.initialize(resourceSet);


		const PartitionGroupConfig &pgConfig =
			resourceSet.txnMgr_->getPartitionGroupConfig();

		const int32_t writeLogInterval =
			resourceSet.logMgr_->getConfig().logWriteMode_;
		for (PartitionGroupId pgId = 0;
			 pgId < pgConfig.getPartitionGroupCount(); pgId++) {
			const PartitionId beginPId =
				pgConfig.getGroupBeginPartitionId(pgId);
			Event timeoutCheckEvent(
				eeSource_, TXN_COLLECT_TIMEOUT_RESOURCE, beginPId);
			ee_->addPeriodicTimer(
				timeoutCheckEvent, TXN_TIMEOUT_CHECK_INTERVAL * 1000);
			Event dataStorePeriodicEvent(
				eeSource_, CHUNK_EXPIRE_PERIODICALLY, beginPId);
			ee_->addPeriodicTimer(
				dataStorePeriodicEvent, CHUNK_EXPIRE_CHECK_INTERVAL * 1000);
			Event adjustStoreMemoryPeriodicEvent(
				eeSource_, ADJUST_STORE_MEMORY_PERIODICALLY, beginPId);
			ee_->addPeriodicTimer(adjustStoreMemoryPeriodicEvent,
				ADJUST_STORE_MEMORY_CHECK_INTERVAL * 1000);
			if (writeLogInterval > 0 && writeLogInterval < INT32_MAX) {
				Event writeLogPeriodicallyEvent(
					eeSource_, WRITE_LOG_PERIODICALLY, beginPId);
				ee_->addPeriodicTimer(writeLogPeriodicallyEvent,
					changeTimeSecondToMilliSecond(writeLogInterval));
			}
		}

		ee_->setHandler(EXECUTE_JOB, executeHandler_);
		executeHandler_.initialize(resourceSet);
		executeHandler_.setServiceType(TRANSACTION_SERVICE);
		transactionManager_ = resourceSet.txnMgr_;

		resultSetHolderManager_.initialize(
				resourceSet.txnMgr_->getPartitionGroupConfig());

		statUpdator_.service_ = this;
		statUpdator_.manager_ = resourceSet.txnMgr_;
		resourceSet.stats_->addUpdator(&statUpdator_);

		syncManager_ = resourceSet.syncMgr_;
		dataStore_ = resourceSet.ds_;
		checkpointService_ = resourceSet.cpSvc_;

		initalized_ = true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TransactionService::start() {
	try {
		if (!initalized_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_SERVICE_NOT_INITIALIZED, "");
		}

		ee_->start();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TransactionService::shutdown() {
	ee_->shutdown();
}

void TransactionService::waitForShutdown() {
	ee_->waitForShutdown();
}

EventEngine *TransactionService::getEE() {
	return ee_;
}

void TransactionService::changeTimeoutCheckMode(PartitionId pId,
	PartitionTable::PartitionStatus after, ChangePartitionType changeType,
	bool isToSubMaster, ClusterStats &stats) {
	const bool isShortTermSyncBegin = (changeType == PT_CHANGE_SYNC_START);
	const bool isShortTermSyncEnd = (changeType == PT_CHANGE_SYNC_END);

	assert(!(isShortTermSyncBegin && isShortTermSyncEnd));
	assert(!(isToSubMaster && isShortTermSyncBegin));
	assert(!(isToSubMaster && isShortTermSyncEnd));


	switch (after) {
	case PartitionTable::PT_ON:
		assert(!isShortTermSyncBegin);
		assert(!isToSubMaster);
		enableTransactionTimeoutCheck(pId);
		stats.setTransactionTimeoutCheck(pId, 1);
		break;

	case PartitionTable::PT_OFF:
		if (isToSubMaster) {
			disableTransactionTimeoutCheck(pId);
			stats.setTransactionTimeoutCheck(pId, 0);
		}
		else if (isShortTermSyncBegin) {
			enableTransactionTimeoutCheck(pId);
			stats.setTransactionTimeoutCheck(pId, 1);
		}
		else if (isShortTermSyncEnd) {
			enableTransactionTimeoutCheck(pId);
			stats.setTransactionTimeoutCheck(pId, 1);
		}
		else {
		}
		break;

	case PartitionTable::PT_SYNC:
		assert(isShortTermSyncBegin);
		assert(!isShortTermSyncEnd);
		assert(!isToSubMaster);
		disableTransactionTimeoutCheck(pId);
		stats.setTransactionTimeoutCheck(pId, 0);
		break;

	case PartitionTable::PT_STOP:
		assert(!isShortTermSyncBegin);
		assert(!isShortTermSyncEnd);
		disableTransactionTimeoutCheck(pId);
		stats.setTransactionTimeoutCheck(pId, 0);
		break;

	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INVALID_ARGS,
			"(pId=" << pId << ", state=" << after
					<< ", changeType=" << static_cast<int32_t>(changeType)
					<< ", isToSubMaster=" << isToSubMaster << ")");
	}
}

void TransactionService::enableTransactionTimeoutCheck(PartitionId pId) {
	const bool before = enableTxnTimeoutCheck_[pId];
	enableTxnTimeoutCheck_[pId] = true;

	if (!before) {
		GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_CHECK_TIMEOUT,
			"timeout check enabled. (pId=" << pId << ")");
	}
}

void TransactionService::disableTransactionTimeoutCheck(PartitionId pId) {
	const bool before = enableTxnTimeoutCheck_[pId];
	enableTxnTimeoutCheck_[pId] = false;

	if (before) {
		GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_CHECK_TIMEOUT,
			"timeout check disabled. (pId=" << pId << ")");
	}
}

bool TransactionService::isTransactionTimeoutCheckEnabled(
	PartitionId pId) const {
	return enableTxnTimeoutCheck_[pId];
}

size_t TransactionService::getWorkMemoryByteSizeLimit() const {
	return serviceConfig_.getWorkMemoryByteSizeLimit();
}

ResultSetHolderManager& TransactionService::getResultSetHolderManager() {
	return resultSetHolderManager_;
}

TransactionService::Config::Config(ConfigTable &configTable) :
	workMemoryByteSizeLimit_(
		ConfigTable::megaBytesToBytes(
		configTable.get<int32_t>(CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT)))
{
	setUpConfigHandler(configTable);
}

size_t TransactionService::Config::getWorkMemoryByteSizeLimit() const {
	return workMemoryByteSizeLimit_;
}

void TransactionService::Config::setUpConfigHandler(ConfigTable &configTable) {
	configTable.setParamHandler(CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT, *this);
}

void TransactionService::Config::operator()(
	ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT:
		workMemoryByteSizeLimit_ = ConfigTable::megaBytesToBytes(value.get<int32_t>());
		break;
	}
}

uint64_t TransactionService::getTotalReadOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += readOperationCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalWriteOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += writeOperationCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalRowReadCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += rowReadCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalRowWriteCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += rowWriteCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalBackgroundOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += backgroundOperationCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalNoExpireOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += noExpireOperationCount_[pId];
	}
	return totalCount;
}
uint64_t TransactionService::getTotalAbortDDLCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += abortDDLCount_[pId];
	}
	return totalCount;
}

TransactionService::StatSetUpHandler TransactionService::statSetUpHandler_;

#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)

void TransactionService::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_CONNECTION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_SESSION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_TXN);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_LOCK_CONFLICT_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_READ_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_WRITE_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_ROW_READ);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_ROW_WRITE);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_NO_EXPIRE_TXN);

	stat.resolveGroup(parentId, STAT_TABLE_PERF_TXN_DETAIL, "txnDetail");
	parentId = STAT_TABLE_PERF_TXN_DETAIL;
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_BACKGROUND_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_NO_EXPIRE_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_ABORT_DDL);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_REP_TIMEOUT);
}

bool TransactionService::StatUpdator::operator()(StatTable &stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	TransactionService &svc = *service_;
	TransactionManager &mgr = *manager_;

	EventEngine::Stats txnSvcStats;
	svc.getEE()->getStats(txnSvcStats);

	const uint64_t numClientTCPConnection =
		txnSvcStats.get(EventEngine::Stats::ND_CLIENT_CREATE_COUNT) -
		txnSvcStats.get(EventEngine::Stats::ND_CLIENT_REMOVE_COUNT);
	const uint64_t numReplicationTCPConnection =
		txnSvcStats.get(EventEngine::Stats::ND_SERVER_CREATE_COUNT) -
		txnSvcStats.get(EventEngine::Stats::ND_SERVER_REMOVE_COUNT);
	const uint64_t numClientUDPConnection =
		txnSvcStats.get(EventEngine::Stats::ND_MCAST_CREATE_COUNT) -
		txnSvcStats.get(EventEngine::Stats::ND_MCAST_REMOVE_COUNT);
	const uint64_t numConnection = numClientTCPConnection +
								   numReplicationTCPConnection +
								   numClientUDPConnection;
	stat.set(STAT_TABLE_PERF_TXN_NUM_CONNECTION, numConnection);

	const uint32_t concurrency =
		mgr.getPartitionGroupConfig().getPartitionGroupCount();
	uint64_t numTxn = 0;
	uint64_t numSession = 0;
	uint64_t numNoExpireTxn = 0;
	uint64_t numReplicationTimeout = 0;
	for (PartitionGroupId pgId = 0; pgId < concurrency; pgId++) {
		numTxn += mgr.getActiveTransactionCount(pgId);
		numSession += mgr.getTransactionContextCount(pgId);
		numNoExpireTxn += mgr.getNoExpireTransactionCount(pgId);
	}
	for (PartitionId pId = 0; pId < mgr.getPartitionGroupConfig().getPartitionCount(); pId++) {
		numReplicationTimeout += mgr.getReplicationTimeoutCount(pId);
	}

	stat.set(STAT_TABLE_PERF_TXN_NUM_SESSION, numSession);
	stat.set(STAT_TABLE_PERF_TXN_NUM_TXN, numTxn);
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_LOCK_CONFLICT_COUNT,
		txnSvcStats.get(EventEngine::Stats::EVENT_PENDING_ADD_COUNT));
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_READ_OPERATION,
		svc.getTotalReadOperationCount());
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_WRITE_OPERATION,
		svc.getTotalWriteOperationCount());
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_ROW_READ, svc.getTotalRowReadCount());
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_ROW_WRITE, svc.getTotalRowWriteCount());

	stat.set(STAT_TABLE_PERF_TXN_TOTAL_BACKGROUND_OPERATION, svc.getTotalBackgroundOperationCount());

	stat.set(STAT_TABLE_PERF_TXN_NUM_NO_EXPIRE_TXN, numNoExpireTxn);
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_TXN)) {
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_NO_EXPIRE_OPERATION, svc.getTotalNoExpireOperationCount());
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_ABORT_DDL, svc.getTotalAbortDDLCount());
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_REP_TIMEOUT, numReplicationTimeout);
	}

	return true;
}

int32_t TransactionService::getWaitTime(EventContext &ec, Event *ev, int32_t opeTime,
		int64_t &executableCount, int64_t &afterCount, BackgroundEventType type) {
	int32_t waitTime = 0;
	const PartitionGroupId pgId = ec.getWorkerId();
	if (type == TXN_BACKGROUND) {
		PartitionId pId = (*ev).getPartitionId();
		EventType eventType = (*ev).getType();
		EventEngine::Tool::getLiveStats(ec.getEngine(), pgId,
			EventEngine::Stats::EVENT_ACTIVE_EXECUTABLE_ONE_SHOT_COUNT,
			executableCount, &ec, ev);
		EventEngine::Tool::getLiveStats(ec.getEngine(), pgId,
			EventEngine::Stats::EVENT_CYCLE_HANDLING_AFTER_ONE_SHOT_COUNT,
			afterCount, &ec, ev);
		if (executableCount + afterCount > 0) {
			double baseTime = (opeTime == 0) ? 0.5 : static_cast<double>(opeTime);
			waitTime = static_cast<uint32_t>(
				ceil(baseTime * dataStore_->getConfig().getBackgroundWaitWeight()));
		}
	}
	else {
		EventEngine::Stats stats;
		if (ec.getEngine().getStats(pgId, stats)) {
			int32_t queueSizeLimit;
			int32_t interval = 0;
			switch (type) {
				case SYNC_EXEC: {
					EventType eventType = (*ev).getType();
					if (eventType != TXN_LONGTERM_SYNC_CHUNK
							&& eventType != TXN_LONGTERM_SYNC_LOG) {
						return 0;
					}
					queueSizeLimit = syncManager_->getExtraConfig().getLimitLongtermQueueSize();
					interval = syncManager_->getExtraConfig().getLongtermHighLoadInterval();
					break;
				}
				case CP_CHUNKCOPY:
					queueSizeLimit = checkpointService_->getChunkCopyLimitQueueSize();
					interval = checkpointService_->getChunkCopyInterval();
					break;
				default:
					return 0;
			}
			int32_t queueSize =  static_cast<int32_t>(
				stats.get(EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));
			if (queueSize > queueSizeLimit) {
				return interval;
			}
		}
	}
	return waitTime;
}


TransactionService::StatUpdator::StatUpdator()
	: service_(NULL), manager_(NULL) {}

void EventMonitor::reset(EventStart &eventStart) {
	PartitionGroupId pgId = eventStart.getEventContext().getWorkerId();
	eventInfoList_[pgId].init();
}

void EventMonitor::set(EventStart &eventStart) {
	PartitionGroupId pgId = eventStart.getEventContext().getWorkerId();
	eventInfoList_[pgId].eventType_ = eventStart.getEvent().getType();
	eventInfoList_[pgId].pId_ = eventStart.getEvent().getPartitionId();
	eventInfoList_[pgId].startTime_
			= eventStart.getEventContext().getHandlerStartTime().getUnixTime();
}
std::string EventMonitor::dump() {
	util::NormalOStringStream strstrm;
	for (size_t pos = 0; pos < eventInfoList_.size(); pos++) {
		strstrm << "pos=" << pos 
			<< ", event=" << getEventTypeName(eventInfoList_[pos].eventType_)
			<< ", pId=" << eventInfoList_[pos].pId_
			<< ", startTime=" << getTimeStr(eventInfoList_[pos].startTime_)
			<< std::endl;
	}
	return strstrm.str().c_str();
}

