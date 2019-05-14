﻿/*
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

#include "util/container.h"

template<typename T>
void decodeLargeRow(
		const char *key, util::StackAllocator &alloc, TransactionContext &txn,
		DataStore *dataStore, const char8_t *dbName, BaseContainer *container,
		T &record, const EventMonotonicTime emNow);



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
	case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
		action.template opEventType<REMOVE_MULTIPLE_ROWS_BY_ID_SET>();
		break;
	case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		action.template opEventType<UPDATE_MULTIPLE_ROWS_BY_ID_SET>();
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
{}

StatementHandler::~StatementHandler() {}

void StatementHandler::initialize(const ManagerSet &mgrSet) {
	clusterService_ = mgrSet.clsSvc_;
	clusterManager_ = mgrSet.clsMgr_;
	chunkManager_ = mgrSet.chunkMgr_;
	dataStore_ = mgrSet.ds_;
	logManager_ = mgrSet.logMgr_;
	partitionTable_ = mgrSet.pt_;
	transactionService_ = mgrSet.txnSvc_;
	transactionManager_ = mgrSet.txnMgr_;
	triggerService_ = mgrSet.trgSvc_;
	systemService_ = mgrSet.sysSvc_;
	recoveryManager_ = mgrSet.recoveryMgr_;

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

/*!
	@brief Sets the information about success reply to an client
*/
void StatementHandler::setSuccessReply(
		util::StackAllocator &alloc, Event &ev, StatementId stmtId,
		StatementExecStatus status, const Response &response)
{
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);
		EventType eventType = ev.getType();

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
		case LOGIN:
			if (response.connectionOption_ != NULL) {
				ConnectionOption &connOption = *response.connectionOption_;
				encodeIntData<int8_t>(out, connOption.authMode_);
				encodeIntData<DatabaseId>(out, connOption.dbId_);
			}
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
			switch (response.rs_->getResultType()) {
			case RESULT_ROWSET: {
				PartialQueryOption partialQueryOption(alloc);
				response.rs_->encodePartialQueryOption(alloc, partialQueryOption);

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
			transactionManager_->getReauthenticationInterval();
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
		PartitionTable::PT_CURRENT_OB);
	const bool isBackup = partitionTable_->isBackup(pId, myself,
		PartitionTable::PT_CURRENT_OB);
	const bool isCatchup = partitionTable_->isCatchup(pId, myself,
		PartitionTable::PT_CURRENT_OB);

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
		)  {
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

	const bool distributed = false;

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

void StatementHandler::applyQueryOption(
		ResultSet &rs, const OptionSet &optionSet,
		const int32_t *fetchBytesSize, bool partial,
		const PartialQueryOption &partialOption) {

	const bool distributed = false;

	rs.setQueryOption(
			(fetchBytesSize == NULL ?
					optionSet.get<Options::FETCH_BYTES_SIZE>() :
					*fetchBytesSize),
			distributed, partial, partialOption);

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
			bool useConnDBName = true;
			useConnDBName &= !connOption.dbName_.empty();
			request.optional_.set<Options::DB_NAME>(
					!useConnDBName ? GS_PUBLIC : connOption.dbName_.c_str());
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



StatementHandler::CaseSensitivity StatementHandler::getCaseSensitivity(
		const Request &request) {
	return getCaseSensitivity(request.optional_);
}

StatementHandler::CaseSensitivity StatementHandler::getCaseSensitivity(
		const OptionSet &optionSet) {
	return CaseSensitivity();
}

StatementMessage::CreateDropIndexMode
StatementHandler::getCreateDropIndexMode(const OptionSet &optionSet) {
	return Message::INDEX_MODE_NOSQL;
}

bool StatementHandler::isDdlTransaction(const OptionSet &optionSet) {
	return false;
}

void StatementHandler::assignIndexExtension(
		IndexInfo &indexInfo, const OptionSet &optionSet) {
	indexInfo.extensionName_.append(getExtensionName(optionSet));

}

const char8_t* StatementHandler::getExtensionName(const OptionSet &optionSet) {
	return "";
}

/*!
	@brief Decodes index information
*/
void StatementHandler::decodeIndexInfo(
	util::ByteStream<util::ArrayInStream> &in, IndexInfo &indexInfo) {
	try {
		uint32_t size;
		in >> size;

//		const size_t startPos = in.base().position();

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

	{
		if (!ackWait) {
			Event ev(ec, stmtType, request.fixed_.pId_);
			setSuccessReply(alloc, ev, request.fixed_.cxtSrc_.stmtId_, status, response);

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
//	const util::DateTime &now = ec.getHandlerStartTime();
//	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

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
		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << replContext.getStatementType() << ")");
		}

		{
			Event ev(
					ec, replContext.getStatementType(),
					replContext.getPartitionId());
			Request request(alloc, getRequestSource(ev));

			setSuccessReply(alloc, ev, replContext.getOriginalStatementId(), status, response);

			ec.getEngine().send(ev, replContext.getConnectionND());

			GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_REPLY_CLIENT,
				TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
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

//	StatementId stmtId = request.fixed_.cxtSrc_.stmtId_;
	{
		Event ev(ec, stmtType, request.fixed_.pId_);
		setErrorReply(ev, request.fixed_.cxtSrc_.stmtId_, status, e, ND);

		if (!ND.isEmpty()) {
			ec.getEngine().send(ev, ND);
		}
	}
}

/*!
	@brief Executes replication
*/
bool StatementHandler::executeReplication(
	const Request &request,
	EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &clientND, TransactionContext &txn,
	EventType replStmtType, StatementId replStmtId, int32_t replMode,
	ReplicationContext::TaskStatus taskStatus,
	const ClientId *closedResourceIds, size_t closedResourceIdCount,
	const util::XArray<uint8_t> **logRecordList, size_t logRecordCount,
	StatementId originalStmtId, int32_t delayTime,
	const Response &response) {
#define TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType, replStmtId, \
									closedResourceIdCount, logRecordCount)   \
	"(mode=" << replMode << ", pId=" << txn.getPartitionId()                 \
			 << ", eventType=" << replStmtType << ", stmtId=" << replStmtId  \
			 << ", clientId=" << txn.getClientId()                           \
			 << ", containerId=" << txn.getContainerId()                     \
			 << ", closedResourceIdCount=" << closedResourceIdCount          \
			 << ", logRecordCount=" << logRecordCount << ")"

	util::StackAllocator::Scope scope(alloc);
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	ReplicationId replId = 0;

	if (logRecordCount == 0 && closedResourceIdCount == 0) {
		return false;
	}

	try {
		util::XArray<NodeId> backupNodeIdList(alloc);
		partitionTable_->getBackup(txn.getPartitionId(), backupNodeIdList);

		if (backupNodeIdList.empty()) {
			return false;
		}

		if (replMode == TransactionManager::REPLICATION_SEMISYNC) {
			TransactionManager::ContextSource ctxSrc(replStmtType);
			ctxSrc.containerId_ = txn.getContainerId();
			ctxSrc.stmtId_ = replStmtId;


			ReplicationContext &replContext =
				transactionManager_->put(txn.getPartitionId(),
					txn.getClientId(), ctxSrc, clientND, emNow);


			replId = replContext.getReplicationId();
			replContext.setExistFlag(response.existFlag_);
			replContext.incrementAckCounter(
				static_cast<uint32_t>(backupNodeIdList.size()));
			replContext.setOriginalStmtId(originalStmtId);
			replContext.setTaskStatus(taskStatus);
			replContext.setContainerSchemaVersionId(response.schemaVersionId_);
			replContext.setBinaryData(response.binaryData2_.data(), response.binaryData2_.size());
			replContext.setBinaryData(response.binaryData_.data(), response.binaryData_.size());
		}

		Event replEvent(ec, REPLICATION_LOG, txn.getPartitionId());
		EventByteOutStream out = replEvent.getOutStream();
		encodeReplicationAckPart(out, GS_CLUSTER_MESSAGE_CURRENT_VERSION, replMode,
			txn.getClientId(), replId, replStmtType, replStmtId,
			taskStatus);
		encodeIntData<uint16_t>(out, LogManager::getBaseVersion());
		encodeIntData<uint32_t>(
			out, static_cast<uint32_t>(closedResourceIdCount));
		for (size_t i = 0; i < closedResourceIdCount; i++) {
			encodeLongData<SessionId>(out, closedResourceIds[i].sessionId_);
			encodeUUID(
				out, closedResourceIds[i].uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		}
		encodeIntData<uint32_t>(out, static_cast<uint32_t>(logRecordCount));
		for (size_t i = 0; i < logRecordCount; i++) {
			encodeBinaryData(
				out, logRecordList[i]->data(), logRecordList[i]->size());
		}

		for (size_t i = 0; i < backupNodeIdList.size(); i++) {
			const NodeDescriptor &backupND =
				transactionService_->getEE()->getServerND(backupNodeIdList[i]);
			ec.getEngine().send(replEvent, backupND);
		}

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_SEND_LOG,
			TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType, replStmtId,
				closedResourceIdCount, logRecordCount)
				<< " (numBackup=" << backupNodeIdList.size()
				<< ", replId=" << replId << ")");

		return (replMode == TransactionManager::REPLICATION_SEMISYNC);
	}
	catch (EncodeDecodeException &e) {
		TXN_RETHROW_ENCODE_ERROR(
			e, TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType,
				   replStmtId, closedResourceIdCount, closedResourceIdCount));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType,
				   replStmtId, closedResourceIdCount, closedResourceIdCount));
	}

#undef TXN_TRACE_REPLICATION_ERROR
}

/*!
	@brief Replies ReplicationAck
*/
void StatementHandler::replyReplicationAck(
		EventContext &ec,
		util::StackAllocator &alloc, const NodeDescriptor &ND,
		const ReplicationAck &request) {

#define TXN_TRACE_REPLY_ACK_ERROR(request, ND)                             \
	"(mode=" << request.replMode_ << ", owner=" << ND                      \
			 << ", replId=" << request.replId_ << ", pId=" << request.pId_ \
			 << ", eventType=" << request.replStmtType_                    \
			 << ", stmtId=" << request.replStmtId_                         \
			 << ", clientId=" << request.clientId_                         \
			 << ", taskStatus=" << request.taskStatus_ << ")"

	util::StackAllocator::Scope scope(alloc);

	if (request.replMode_ != TransactionManager::REPLICATION_SEMISYNC) {
		return;
	}

	try {
		Event replAckEvent(ec, REPLICATION_ACK, request.pId_);
		EventByteOutStream out = replAckEvent.getOutStream();
		encodeReplicationAckPart(out, request.clusterVer_, request.replMode_,
			request.clientId_, request.replId_, request.replStmtType_,
			request.replStmtId_, request.taskStatus_);

		ec.getEngine().send(replAckEvent, ND);

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
	@brief Handles error
*/
void StatementHandler::handleError(
		EventContext &ec,
		util::StackAllocator &alloc, Event &ev, const Request &request,
		std::exception &) {
#define TXN_TRACE_ON_ERROR_MINIMUM(cause, category, ev, request, extra)      \
	GS_EXCEPTION_MESSAGE(cause)                                              \
		<< " (" << category << ", nd=" << ev.getSenderND()                   \
		<< ", pId=" << ev.getPartitionId() << ", eventType=" << ev.getType() \
		<< extra << ")"

#define TXN_TRACE_ON_ERROR(cause, category, ev, request)                      \
	TXN_TRACE_ON_ERROR_MINIMUM(cause, category, ev, request,                  \
		", stmtId=" << request.fixed_.cxtSrc_.stmtId_                                \
					<< ", clientId=" << request.fixed_.clientId_ << ", containerId=" \
					<< ((request.fixed_.cxtSrc_.containerId_ == UNDEF_CONTAINERID)   \
							   ? 0                                            \
							   : request.fixed_.cxtSrc_.containerId_))

	util::StackAllocator::Scope scope(alloc);

	try {
		try {
			throw;
		}
		catch (StatementAlreadyExecutedException &e) {
			UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Statement already executed", ev,
										  request));
			Response response(alloc);
			response.existFlag_ = false;
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
		}
		catch (EncodeDecodeException &e) {

			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR_MINIMUM(
					e, "Failed to encode or decode message", ev, request, ""));
		}
		catch (LockConflictException &e) {
			UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Lock conflicted", ev, request));
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
			catch (ContextNotFoundException &) {
				UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
					TXN_TRACE_ON_ERROR(e, "Context not found", ev, request));
			}

			const LockConflictStatus lockConflictStatus(getLockConflictStatus(
					ev, ec.getHandlerStartMonotonicTime(), request));
			try {
				checkLockConflictStatus(lockConflictStatus, e);
			}
			catch (LockConflictException&) {
				assert(false);
				throw;
			}
			catch (...) {
				std::exception e2;
				handleError(ec, alloc, ev, request, e2);
				return;
			}
			retryLockConflictedRequest(ec, ev, lockConflictStatus);
		}
		catch (DenyException &e) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Request denied", ev, request));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);
		}
		catch (ContextNotFoundException &e) {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Context not found", ev, request));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_ERROR, request, e);
		}
		catch (std::exception &e) {
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
						TRANSACTION_SERVICE, e,
						TXN_TRACE_ON_ERROR(e,
							"Transaction is already timed out", ev, request));
					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
				}
				else {
					UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
						TXN_TRACE_ON_ERROR(e, "User error", ev, request));
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
					catch (ContextNotFoundException &) {
						UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
							TXN_TRACE_ON_ERROR(e, "Context not found", ev,
													  request));
					}

					replyError(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_ERROR, request, e);
				}
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			TXN_TRACE_ON_ERROR(e, "System error", ev, request));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setError(ec, &e);
	}

#undef TXN_TRACE_ON_ERROR
#undef TXN_TRACE_ON_ERROR_MINIMUM
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

bool StatementHandler::checkPrivilege(EventType command,
									  UserType userType, RequestType requestType, bool isSystemMode,
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
			break;
		case CONTAINER_ATTR_SINGLE:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE_SYSTEM:
		case CONTAINER_ATTR_SUB:
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
		return true;
	default:
		return false;
	}
}

void StatementHandler::checkDbAccessible(
		const char8_t *loginDbName, const char8_t *specifiedDbName
		) {
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

ConnectHandler::ConnectHandler(ProtocolVersion currentVersion,
							   const ProtocolVersion *acceptableProtocolVersons) :
	currentVersion_(currentVersion),
	acceptableProtocolVersons_(acceptableProtocolVersons) {
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
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

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
	for (const ProtocolVersion *acceptableVersions = acceptableProtocolVersons_;;
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

void ConnectHandler::handleError(EventContext &ec, util::StackAllocator &alloc,
	Event &ev, const ConnectRequest &request, std::exception &) {
#define TXN_TRACE_ON_ERROR_MINIMUM(cause, category, ev, request, extra)      \
	GS_EXCEPTION_MESSAGE(cause)                                              \
		<< " (" category << ", nd=" << ev.getSenderND()                      \
		<< ", pId=" << ev.getPartitionId() << ", eventType=" << ev.getType() \
		<< extra << ")"

#define TXN_TRACE_ON_ERROR(cause, category, ev, request) \
	TXN_TRACE_ON_ERROR_MINIMUM(                          \
		cause, category, ev, request, ", stmtId=" << request.oldStmtId_)

	util::StackAllocator::Scope scope(alloc);

	try {
		try {
			throw;

		}
		catch (EncodeDecodeException &e) {
			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR_MINIMUM(
					e, "Failed to encode or decode message", ev, request, ""));

		}
		catch (DenyException &e) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Request denied", ev, request));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);

		}
		catch (std::exception &e) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
					TXN_TRACE_ON_ERROR(e, "User error", ev, request));
				replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_ERROR, request, e);
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			TXN_TRACE_ON_ERROR(e, "System error", ev, request));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setError(ec, &e);
	}

#undef TXN_TRACE_ON_ERROR
#undef TXN_TRACE_ON_ERROR_MINIMUM
}

/*!
	@brief Handler Operator
*/
void DisconnectHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	Request request(alloc, getRequestSource(ev));

	try {

		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clear();

	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
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

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clear();
		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		const RequestType requestType =
				request.optional_.get<Options::REQUEST_MODULE_TYPE>();
		const char8_t *dbName = request.optional_.get<Options::DB_NAME>();
//		const bool systemMode = request.optional_.get<Options::SYSTEM_MODE>();
		const int32_t txnTimeoutInterval =
				(request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() >= 0 ?
				request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() :
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);

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
		connOption.clientId_ = request.fixed_.clientId_;
		DatabaseId dbId = UNDEF_DBID;

		if ((strcmp(dbName, GS_PUBLIC) == 0) ||
				(strcmp(dbName, GS_SYSTEM) == 0)) {
			if ((strcmp(userName.c_str(), GS_ADMIN_USER) == 0) ||
					(strcmp(userName.c_str(), GS_SYSTEM_USER) == 0) ||
				(userName.find('#') != std::string::npos)) {
				TEST_PRINT("LoginHandler::operator() public&admin S\n");
				if (systemService_->getUserTable().isValidUser(
						userName.c_str(), digest.c_str())) {  

					connOption.isImmediateConsistency_ = isImmediateConsistency;
					connOption.txnTimeoutInterval_ = txnTimeoutInterval;
					connOption.userType_ = Message::USER_ADMIN;
					if (strcmp(dbName, GS_SYSTEM) == 0) {
						GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
								"database name invalid (" << dbName << ")");
					}
					connOption.userName_ = userName.c_str();
					connOption.dbName_ = dbName;
					connOption.requestType_ = requestType;
					connOption.isAdminAndPublicDB_ = true;
					connOption.isAuthenticated_ = true;
					if (strcmp(dbName, GS_PUBLIC) == 0) {
						connOption.dbId_ = GS_PUBLIC_DB_ID;
					}
					else {
						connOption.dbId_ = GS_SYSTEM_DB_ID;
					}
					connOption.authenticationTime_ = emNow;

					replySuccess(
							ec, alloc, ev.getSenderND(), ev.getType(),
							TXN_STATEMENT_SUCCESS, request, response,
							NO_REPLICATION);
					TEST_PRINT("LoginHandler::operator() public&admin E\n");
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
							"invalid user name or password (user mame = " <<
							userName.c_str() << ")");
				}
			}
			else {  

				TEST_PRINT("LoginHandler::operator() public&normal S\n");


				if (strcmp(dbName, GS_SYSTEM) == 0) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
							"database name invalid (" << dbName << ")");
				}

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
				if (nd0.isSelf()) {
					TEST_PRINT("Self\n");

					connOption.isImmediateConsistency_ = isImmediateConsistency;
					connOption.txnTimeoutInterval_ = txnTimeoutInterval;
					connOption.userType_ = Message::USER_NORMAL;
					connOption.userName_ = userName.c_str();
					connOption.dbName_ = dbName;
					connOption.requestType_ = requestType;
					connOption.isAdminAndPublicDB_ = false;

					executeAuthenticationInternal(
							ec, alloc, request.fixed_.cxtSrc_,
							userName.c_str(), digest.c_str(),
							dbName, Message::USER_NORMAL, 1,
							dbId);  

					connOption.isAuthenticated_ = true;
					connOption.dbId_ = dbId;
					connOption.authenticationTime_ = emNow;

					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
				}
				else {
					TEST_PRINT("Other\n");

					connOption.isImmediateConsistency_ = isImmediateConsistency;
					connOption.txnTimeoutInterval_ = txnTimeoutInterval;
					connOption.userType_ = Message::USER_NORMAL;
					connOption.userName_ = userName.c_str();
					connOption.dbName_ = dbName;
					connOption.requestType_ = requestType;
					connOption.isAdminAndPublicDB_ = false;

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
			NodeAddress nodeAddress = clusterManager_->getPartition0NodeAddr();
			TEST_PRINT1(
				"NodeAddress(TxnServ) %s\n", nodeAddress.dump().c_str());

			if (nodeAddress.port_ == 0) {
				TXN_THROW_DENY_ERROR(
					GS_ERROR_TXN_AUTHENTICATION_SERVICE_NOT_READY, "");
			}

			const NodeDescriptor &nd0 =
				transactionService_->getEE()->getServerND(util::SocketAddress(
					nodeAddress.toString(false).c_str(), nodeAddress.port_));

			if ((strcmp(userName.c_str(), GS_ADMIN_USER) == 0) ||
				(strcmp(userName.c_str(), GS_SYSTEM_USER) == 0) ||
				(userName.find('#') != std::string::npos)) {
				TEST_PRINT("LoginHandler::operator() admin S\n");

				if (systemService_->getUserTable().isValidUser(
						userName.c_str(), digest.c_str())) {  

						if (nd0.isSelf()) {
						TEST_PRINT("Self\n");

						connOption.isImmediateConsistency_ =
								isImmediateConsistency;
						connOption.txnTimeoutInterval_ = txnTimeoutInterval;
						connOption.userType_ = Message::USER_ADMIN;
						connOption.userName_ = userName.c_str();
						connOption.dbName_ = dbName;
						connOption.requestType_ = requestType;
						connOption.isAdminAndPublicDB_ = false;

						executeAuthenticationInternal(
								ec, alloc,
								request.fixed_.cxtSrc_, userName.c_str(), digest.c_str(),
								dbName, connOption.userType_, 2,
								dbId);  

						connOption.isAuthenticated_ = true;
						connOption.dbId_ = dbId;
						connOption.authenticationTime_ = emNow;

						replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
							TXN_STATEMENT_SUCCESS, request, response,
							NO_REPLICATION);
					}
					else {
						TEST_PRINT("Other\n");

						connOption.isImmediateConsistency_ =
							isImmediateConsistency;
						connOption.txnTimeoutInterval_ = txnTimeoutInterval;
						connOption.userType_ = Message::USER_ADMIN;
						connOption.userName_ = userName.c_str();
						connOption.dbName_ = dbName;
						connOption.requestType_ = requestType;
						connOption.isAdminAndPublicDB_ = false;

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
						"invalid user name or password (user mame = "
							<< userName.c_str() << ")");
				}
			}
			else {  
				TEST_PRINT("LoginHandler::operator() normal S\n");


					if (nd0.isSelf()) {
					TEST_PRINT("Self\n");

					connOption.isImmediateConsistency_ = isImmediateConsistency;
					connOption.txnTimeoutInterval_ = txnTimeoutInterval;
					connOption.userType_ = Message::USER_NORMAL;
					connOption.userName_ = userName.c_str();
					connOption.dbName_ = dbName;
					connOption.requestType_ = requestType;
					connOption.isAdminAndPublicDB_ = false;

					executeAuthenticationInternal(
							ec, alloc, request.fixed_.cxtSrc_,
							userName.c_str(), digest.c_str(),
							dbName, Message::USER_NORMAL, 3,
							dbId);  

					connOption.isAuthenticated_ = true;
					connOption.dbId_ = dbId;
					connOption.authenticationTime_ = emNow;

					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
				}
				else {
					TEST_PRINT("Other\n");

					connOption.isImmediateConsistency_ = isImmediateConsistency;
					connOption.txnTimeoutInterval_ = txnTimeoutInterval;
					connOption.userType_ = Message::USER_NORMAL;
					connOption.userName_ = userName.c_str();
					connOption.dbName_ = dbName;
					connOption.requestType_ = requestType;
					connOption.isAdminAndPublicDB_ = false;

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

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);


		connOption.clear();
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
			out << static_cast<uint8_t>(1);
			const NodeDescriptor &ownerND =
				transactionService_->getEE()->getServerND(ownerNodeId);
			util::SocketAddress::Inet addr;
			uint16_t port;
			ownerND.getAddress().getIP(&addr, &port);
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(&addr), sizeof(addr));
			out << static_cast<uint32_t>(port);
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
			const NodeDescriptor &backupND =
				transactionService_->getEE()->getServerND(backupNodeIds[i]);
			util::SocketAddress::Inet addr;
			uint16_t port;
			backupND.getAddress().getIP(&addr, &port);
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(&addr), sizeof(addr));
			out << static_cast<uint32_t>(port);
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
	const NodeDescriptor &masterND = ee.getServerND(masterNodeId);

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

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

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
					ANY_CONTAINER, *itr)) {
				validContainerCondition.insertAttribute(*itr);
			}
		}

		response.containerNum_ = dataStore_->getContainerCount(txn,
			txn.getPartitionId(), connOption.dbId_, validContainerCondition);
		dataStore_->getContainerNameList(txn, txn.getPartitionId(), start,
			limit, connOption.dbId_, validContainerCondition,
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

		int64_t currentTime = ec.getHandlerStartTime().getUnixTime();

		util::String containerNameStr(alloc);
		for (ContainerNameList::iterator it = containerNameList.begin();
			 it != containerNameList.end(); ++it) {
			const FullContainerKey queryContainerKey(
					alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false), (*it)->data(), (*it)->size());
			checkLoggedInDatabase(
					connOption.dbId_, connOption.dbName_.c_str(),
					queryContainerKey.getComponents(alloc).dbId_,
					request.optional_.get<Options::DB_NAME>()
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

				const bool forMeta = true;
				encodeContainerProps(
						txn, out, metaContainer, propFlags, propTypeCount,
						forMeta, emNow, containerNameStr, dbName
						, currentTime
						);

				continue;
			}

			{
				const bool forMeta = false;
				encodeContainerProps(
						txn, out, container, propFlags, propTypeCount,
						forMeta, emNow, containerNameStr, dbName
						, currentTime
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

		ContainerId metaContainerId = UNDEF_CONTAINERID;
		int8_t metaNamingType = -1;
		if (forMeta) {
			MetaContainer *metaContainer =
					static_cast<MetaContainer*>(container);
			metaContainerId = metaContainer->getMetaContainerId();
			metaNamingType = metaContainer->getColumnNamingType();
		}

		encodeId(
				out, schemaVersionId, containerId, realContainerKey,
				metaContainerId, metaNamingType);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_SCHEMA)) != 0) {
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
		container->getIndexInfoList(txn, indexInfoList);

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
		ContainerId metaContainerId, int8_t metaNamingType) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_ID);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		out << schemaVersionId;
		out << containerId;
		encodeContainerKey(out, containerKey);
		encodeMetaId(out, metaContainerId, metaNamingType);

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
		int8_t metaNamingType) {
	if (metaContainerId == UNDEF_CONTAINERID) {
		return;
	}

	try {
		const MetaContainerType type = META_FULL;
		encodeEnumData<MetaContainerType>(out, type);

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
//		util::StackAllocator &alloc = *nullsList.get_allocator().base();

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

void GetContainerPropertiesHandler::encodePartitioningMetaData(
	EventByteOutStream &out,
	TransactionContext &txn, EventMonotonicTime emNow,
	BaseContainer &largeContainer, ContainerAttribute attribute,
	const char8_t *dbName, const char8_t *containerName
	, int64_t currentTime
	) {
	try {
//		util::StackAllocator &alloc = txn.getDefaultAllocator();

		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_PARTITIONING_METADATA);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;


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
				ANY_CONTAINER,
				request.optional_.get<Options::CONTAINER_ATTRIBUTE>())) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container attribute : " <<
					request.optional_.get<Options::CONTAINER_ATTRIBUTE>());
		}
		const FullContainerKey containerKey(
				alloc, getKeyConstraint(request.optional_),
				containerNameBinary.data(), containerNameBinary.size());
		checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
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
		checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
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
		checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				containerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
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
		clusterService_->setError(ec, &e);
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
//			StatementId rowCounter = 0;
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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
		if (containerKeyOption != NULL && containerKeyOption->enabled_) {
			const util::XArray<uint8_t> &keyBinary =
					containerKeyOption->containerKeyBinary_;
			containerKey = ALLOC_NEW(alloc) FullContainerKey(
					alloc, getKeyConstraint(request.optional_),
					keyBinary.data(), keyBinary.size());
		}

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
				forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkQueryOption(request.optional_, fetchOption, isPartial, true);



		const ContainerId metaContainerId =
				request.optional_.get<Options::META_CONTAINER_ID>();
		if (metaContainerId != UNDEF_CONTAINERID) {
			TransactionContext &txn = transactionManager_->put(
					alloc, request.fixed_.pId_,
					TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
			const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);
			MetaStore metaStore(*dataStore_);

			MetaType::NamingType columnNamingType;
			columnNamingType = static_cast<MetaType::NamingType>(
					request.optional_.get<Options::META_NAMING_TYPE>());

			MetaContainer *metaContainer = metaStore.getContainer(
					txn, connOption.dbId_, metaContainerId,
					MetaType::NAMING_NEUTRAL, columnNamingType);

			response.rs_ = dataStore_->createResultSet(
					txn, txn.getContainerId(),
					metaContainer->getVersionId(), emNow);
			const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

			MetaProcessorSource source(
					connOption.dbId_, connOption.dbName_.c_str());
			source.dataStore_ = dataStore_;
			source.eventContext_= &ec;
			source.transactionManager_ = transactionManager_;
			source.partitionTable_ = partitionTable_;

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

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			applyQueryOption(
					*response.rs_, request.optional_, NULL, isPartial, partialQueryOption);
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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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

		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

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

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

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
					TransactionManager::CREATE, TransactionManager::AUTO_COMMIT);
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
void MultiStatementHandler::handleExecuteError(util::StackAllocator &alloc,
	PartitionId pId, const ClientId &clientId,
	const TransactionManager::ContextSource &src, Progress &progress,
	std::exception &, EventType stmtType, const char8_t *executionName) {
#define TXN_TRACE_MULTI_OP_ERROR_MESSAGE(                                     \
	category, pId, clientId, src, stmtType, executionName, containerName) \
		" (" << category \
		<< ", containerName=" << containerName \
		<< ", stmtType=" << stmtType \
		<< ", executionName=" << executionName << ", pId=" << pId \
		<< ", clientId=" << clientId << ", containerId=" << src.containerId_ \
		<< ")"

#define TXN_TRACE_MULTI_OP_ERROR(                                    \
	cause, category, pId, clientId, src, stmtType, executionName, containerName)    \
	GS_EXCEPTION_MESSAGE(cause) << TXN_TRACE_MULTI_OP_ERROR_MESSAGE( \
		category, pId, clientId, src, stmtType, executionName, containerName)

	util::String containerName(alloc);
	ExceptionParameterList paramList(alloc);
	try {
		const TransactionManager::ContextSource subSrc(
				stmtType, 0, src.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
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
		try {
			throw;
		}
		catch (StatementAlreadyExecutedException &e) {
			UTIL_TRACE_EXCEPTION_INFO(
				TRANSACTION_SERVICE, e,
				TXN_TRACE_MULTI_OP_ERROR(e, "Statement already executed", pId,
						clientId, src, stmtType, executionName, containerName));
			progress.containerResult_.push_back(
				CONTAINER_RESULT_ALREADY_EXECUTED);
		}
		catch (LockConflictException &e) {
			UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
				TXN_TRACE_MULTI_OP_ERROR(e, "Lock conflicted", pId, clientId,
						src, stmtType, executionName, containerName));

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
				catch (ContextNotFoundException &) {
					UTIL_TRACE_EXCEPTION_INFO(
						TRANSACTION_SERVICE, e,
						TXN_TRACE_MULTI_OP_ERROR(e, "Context not found", pId,
								clientId, src, stmtType, executionName, containerName));
				}
			}

			try {
				checkLockConflictStatus(progress.lockConflictStatus_, e);
			}
			catch (LockConflictException&) {
				assert(false);
				throw;
			}
			catch (...) {
				std::exception e2;
				handleExecuteError(
						alloc, pId, clientId, src, progress, e2, stmtType,
						executionName);
				return;
			}
			progress.lockConflicted_ = true;
		}
		catch (std::exception &e) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				GS_RETHROW_SYSTEM_ERROR(
					e, TXN_TRACE_MULTI_OP_ERROR_MESSAGE("System error", pId,
						   clientId, src, stmtType, executionName, containerName));
			}
			else {
				UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
					TXN_TRACE_MULTI_OP_ERROR(e, "User error", pId, clientId,
							src, stmtType, executionName, containerName));

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
						TXN_TRACE_MULTI_OP_ERROR(e, "Context not found", pId,
								clientId, src, stmtType, executionName, containerName));
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
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}

#undef TXN_TRACE_MULTI_OP_ERROR
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleWholeError(
		EventContext &ec, util::StackAllocator &alloc,
		const Event &ev, const Request &request, std::exception &e) {
	try {
		throw;
	}
	catch (EncodeDecodeException &) {
		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				GS_EXCEPTION_MESSAGE(e) <<
				" (Failed to encode or decode message"
				", nd=" << ev.getSenderND() <<
				", pId=" << ev.getPartitionId() << 
				", eventType=" << ev.getType() << ")");
	}
	catch (DenyException &) {
		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				GS_EXCEPTION_MESSAGE(e) <<
				" (Request denied"
				", nd=" << ev.getSenderND() <<
				", pId=" << ev.getPartitionId() <<
				", eventType=" << ev.getType() <<
				", stmtId=" << request.fixed_.cxtSrc_.stmtId_ << ")");
		replyError(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);
	}
	catch (std::exception &) {
		const bool isCritical = GS_EXCEPTION_CHECK_CRITICAL(e);
		const char8_t *category = (isCritical ? "System error" : "User error");
		const StatementExecStatus returnStatus =
				(isCritical ? TXN_STATEMENT_NODE_ERROR : TXN_STATEMENT_ERROR);

		UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				GS_EXCEPTION_MESSAGE(e) <<
				" (" << category <<
				", nd=" << ev.getSenderND() <<
				", pId=" << ev.getPartitionId() <<
				", eventType=" << ev.getType() <<
				", stmtId=" << request.fixed_.cxtSrc_.stmtId_ << ")");
		replyError(
				ec, alloc, ev.getSenderND(), ev.getType(), returnStatus,
				request, e);

		if (isCritical) {
			clusterService_->setError(ec, &e);
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

			execute(ec, request, rowSetRequest, messageSchema,
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
		EventContext &ec, const Request &request,
		const RowSetRequest &rowSetRequest, const MessageSchema &schema,
		CheckedSchemaIdSet &idSet, Progress &progress, PutRowOption putRowOption) {
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
				alloc, request.fixed_.pId_, clientId, src, progress, e,
				PUT_MULTIPLE_CONTAINER_ROWS, "put");
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
	util::XArray<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
	container.getKeyColumnIdList(keyColumnIdList);
	const util::XArray<ColumnId> &schemaKeyColumnIdList = schema.getRowKeyColumnIdList();
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
			request.fixed_.stmtType_,
			rowSetRequest.stmtId_, rowSetRequest.containerId_,
			rowSetRequest.option_.get<Options::TXN_TIMEOUT_INTERVAL>(),
			rowSetRequest.getMode_,
			rowSetRequest.txnMode_);
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

				entryCount += execute(ec, request, entry, schemaMap, out, progress);

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
		EventContext &ec, const Request &request,
		const SearchEntry &entry, const SchemaMap &schemaMap,
		EventByteOutStream &replyOut, Progress &progress) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);
	const TransactionManager::ContextSource src =
			createContextSource(request, entry);

	const char8_t *getType =
		(entry.predicate_->distinctKeys_ == NULL) ? "rangeKey" : "distinctKey";

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

		if (entry.predicate_->distinctKeys_ == NULL) {
			ResultSet *rs = dataStore_->createResultSet(txn,
				container->getContainerId(), container->getVersionId(), emNow);
			const ResultSetGuard rsGuard(*dataStore_, *rs);

			QueryProcessor::search(txn, *container, MAX_RESULT_SIZE,
				predicate.startKey_, predicate.finishKey_, *rs);
			rs->setResultType(RESULT_ROWSET);


			QueryProcessor::fetch(txn, *container, 0, MAX_RESULT_SIZE, rs);

			encodeEntry(
				container->getContainerKey(txn), container->getContainerId(),
				schemaMap, *rs, replyOut);

			entryCount++;
			progress.totalRowCount_ += rs->getResultNum();
		}
		else {
			const RowKeyDataList &distinctKeys = *predicate.distinctKeys_;

			for (RowKeyDataList::const_iterator it = distinctKeys.begin();
				 it != distinctKeys.end(); ++it) {
				const util::StackAllocator::Scope scope(alloc);

				const RowKeyData &rowKey = **it;

				ResultSet *rs = dataStore_->createResultSet(txn,
					container->getContainerId(), container->getVersionId(),
					emNow);
				const ResultSetGuard rsGuard(*dataStore_, *rs);

				QueryProcessor::get(txn, *container,
					static_cast<uint32_t>(rowKey.size()), rowKey.data(), *rs);
				rs->setResultType(RESULT_ROWSET);


				QueryProcessor::fetch(
					txn, *container, 0, MAX_RESULT_SIZE, rs);

				encodeEntry(
					container->getContainerKey(txn),
					container->getContainerId(), schemaMap, *rs, replyOut);

				entryCount++;
				progress.totalRowCount_ += rs->getFetchNum();
			}
		}

	}
	catch (std::exception &e) {
		handleExecuteError(
				alloc, request.fixed_.pId_, clientId, src, progress, e,
				GET_MULTIPLE_CONTAINER_ROWS, getType);
	}

	return entryCount;
}

void MultiGetHandler::buildSchemaMap(
		PartitionId pId,
		const util::XArray<SearchEntry> &searchList, SchemaMap &schemaMap,
		EventByteOutStream &out) {
	typedef util::XArray<uint8_t> SchemaData;
	typedef util::Map<ColumnSchemaId, LocalSchemaId> LocalIdMap;
	typedef LocalIdMap::iterator LocalIdMapItr;

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
		const TransactionManager::ContextSource src(GET_MULTIPLE_CONTAINER_ROWS,
			0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
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
		const TransactionManager::ContextSource src(GET_MULTIPLE_CONTAINER_ROWS,
			0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
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

	const ColumnInfo &keyColumnInfo =
		container->getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
	if (predicate.keyType_ != keyColumnInfo.getColumnType()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
	}
}

TransactionManager::ContextSource MultiGetHandler::createContextSource(
		const Request &request, const SearchEntry &entry) {
	return TransactionManager::ContextSource(
				request.fixed_.stmtType_, entry.stmtId_,
				entry.containerId_, request.fixed_.cxtSrc_.txnTimeoutInterval_, entry.getMode_,
				entry.txnMode_);
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
						);
				ContainerAutoPtr containerAutoPtr(
						txn, dataStore_, pId, *containerKey, ANY_CONTAINER,
						getCaseSensitivity(optionSet).isContainerNameCaseSensitive());
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				if (container != NULL && !checkPrivilege(
						GET_MULTIPLE_CONTAINER_ROWS,
						userType, requestType, isSystemMode,
						container->getContainerType(),
						container->getAttribute(),
						optionSet.get<Options::CONTAINER_ATTRIBUTE>())) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"Forbidden Container Attribute");
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

	const MessageRowKeyCoder keyCoder(static_cast<ColumnType>(keyType));
	RowKeyPredicate predicate = {
		static_cast<ColumnType>(keyType), NULL, NULL, NULL};

	int8_t predicateType;
	in >> predicateType;

	if (predicateType == PREDICATE_TYPE_RANGE) {
		RowKeyData **keyList[] = {
			&predicate.startKey_, &predicate.finishKey_, NULL};

		for (RowKeyData ***keyPtr = keyList; *keyPtr != NULL; ++keyPtr) {
			int8_t keyIncluded;
			in >> keyIncluded;

			if (keyIncluded) {
				RowKeyData *key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				keyCoder.decode(in, *key);
				**keyPtr = key;
			}
		}
	}
	else if (predicateType == PREDICATE_TYPE_DISTINCT) {
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
		predicate.distinctKeys_ = distinctKeys;
	}
	else {
		TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
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
						ec, request,
						fetchByteSize,
						queryRequest, out, progress);

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
		EventContext &ec, const Request &request,
		int32_t &fetchByteSize,
		const QueryRequest &queryRequest, EventByteOutStream &replyOut,
		Progress &progress) {
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
		ResultSet *rs = dataStore_->createResultSet(
				txn, txn.getContainerId(), UNDEF_SCHEMAVERSIONID, emNow);
		const ResultSetGuard rsGuard(*dataStore_, *rs);
		applyQueryOption(
				*rs, queryRequest.optionSet_, &fetchByteSize,
				queryRequest.isPartial_, queryRequest.partialQueryOption_);

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

		handleExecuteError(
				alloc, request.fixed_.pId_, clientId, src, progress, e,
				EXECUTE_MULTIPLE_QUERIES, queryType);
	}
}

TransactionManager::ContextSource MultiQueryHandler::createContextSource(
		const Request &request, const QueryRequest &queryRequest) {
	return TransactionManager::ContextSource(
			request.fixed_.stmtType_,
			queryRequest.stmtId_, queryRequest.containerId_,
			queryRequest.optionSet_.get<Options::TXN_TIMEOUT_INTERVAL>(),
			queryRequest.getMode_,
			queryRequest.txnMode_);
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
			rs.encodePartialQueryOption(alloc, partialQueryOption);

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

/*!
	@brief Handler Operator
*/
void ReplicationLogHandler::operator()(EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	ReplicationAck request(ev.getPartitionId());

	try {
		EventByteInStream in(ev.getInStream());

		decodeReplicationAck(in, request);

		clusterService_->checkVersion(request.clusterVer_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = (PSTATE_ON | PSTATE_SYNC);
		checkExecutable(
				request.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		uint16_t logVer;
		decodeIntData<uint16_t>(in, logVer);
		checkLogVersion(logVer);

		uint32_t closedResourceIdCount;
		util::XArray<ClientId> closedResourceIds(alloc);
		decodeIntData<uint32_t>(in, closedResourceIdCount);
		closedResourceIds.assign(
			static_cast<size_t>(closedResourceIdCount), TXN_EMPTY_CLIENTID);
		for (uint32_t i = 0; i < closedResourceIdCount; i++) {
			decodeLongData<SessionId>(in, closedResourceIds[i].sessionId_);
			decodeUUID(
				in, closedResourceIds[i].uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		}

		uint32_t logRecordCount;
		util::XArray<uint8_t> logRecordList(alloc);
		decodeIntData<uint32_t>(in, logRecordCount);
		decodeBinaryData(in, logRecordList, true);

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_RECEIVE_LOG,
			"(mode=" << request.replMode_ << ", owner=" << ev.getSenderND()
					 << ", replId=" << request.replId_ << ", pId="
					 << request.pId_ << ", eventType=" << request.replStmtType_
					 << ", stmtId=" << request.replStmtId_
					 << ", clientId=" << request.clientId_
					 << ", logVer=" << logVer
					 << ", closedResourceIdCount=" << closedResourceIdCount
					 << ", logRecordCount=" << logRecordCount << ")");

		replyReplicationAck(ec, alloc, ev.getSenderND(), request);

		recoveryManager_->redoLogList(alloc, RecoveryManager::MODE_REPLICATION,
			request.pId_, now, emNow, logRecordList.data(),
			logRecordList.size());

		for (size_t i = 0; i < closedResourceIds.size(); i++) {
			transactionManager_->remove(request.pId_, closedResourceIds[i]);
		}
	}
	catch (DenyException &e) {
		int32_t errorCode = e.getErrorCode();
		bool isTrace = false;
		switch (errorCode) {
		case GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH:
		case GS_ERROR_TXN_PARTITION_ROLE_UNMATCH:
		case GS_ERROR_TXN_PARTITION_STATE_UNMATCH:
			if (clusterManager_->reportError(errorCode)) {
				isTrace = true;
			}
			break;
		default:
			isTrace = true;
			break;
		}
		if (isTrace) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				"Replication denied (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Replication denied (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	catch (LogRedoException &e) {
		int32_t errorCode = e.getErrorCode();
		bool isTrace = false;
		switch (errorCode) {
		case GS_ERROR_TXN_REPLICATION_LOG_APPLY_FAILED:
			errorCode = e.getErrorCode(1);
			if (errorCode == GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID) {
				if (clusterManager_->reportError(errorCode)) {
					isTrace = true;
				}
			}
			break;
		default: {
			isTrace = true;
			PartitionId pId = ev.getPartitionId();
			if (partitionTable_->checkClearRole(pId)) {
				clusterService_->requestChangePartitionStatus(ec,
						alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_NORMAL);
			}
		}
			break;
		}
		if (isTrace && partitionTable_->getPartitionStatus(ev.getPartitionId()) == PartitionTable::PT_ON) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				"Failed to redo log (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Failed to redo log (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to accept replication log "
			"(nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Handler Operator
*/
void ReplicationAckHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	ReplicationAck ack(ev.getPartitionId());

	try {
		EventByteInStream in(ev.getInStream());

		decodeReplicationAck(in, ack);

		clusterService_->checkVersion(ack.clusterVer_);
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(ack.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_RECEIVE_ACK,
			"(mode=" << ack.replMode_ << ", backup=" << ev.getSenderND()
					 << ", replId=" << ack.replId_ << ", pId=" << ack.pId_
					 << ", eventType=" << ack.replStmtType_
					 << ", stmtId=" << ack.replStmtId_
					 << ", clientId=" << ack.clientId_
					 << ", taskStatus_=" << ack.taskStatus_ << ")");

		ReplicationContext &replContext =
			transactionManager_->get(ack.pId_, ack.replId_);

		if (replContext.decrementAckCounter()) {
			if (ack.taskStatus_ == ReplicationContext::TASK_CONTINUE) {
				continueEvent(ec, alloc, TXN_STATEMENT_SUCCESS, replContext);
				transactionManager_->remove(ack.pId_, ack.replId_);
			} else {
				replySuccess(ec, alloc, TXN_STATEMENT_SUCCESS, replContext);
				transactionManager_->remove(ack.pId_, ack.replId_);
			}
		}
	}
	catch (ContextNotFoundException &e) {
		UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
			"Replication timed out (nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", replEventType=" << ack.replStmtType_
				<< ", replStmtId=" << ack.replStmtId_
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to receive ack (nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void CheckTimeoutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	try {
		{
			const util::StackAllocator::Scope scope(alloc);

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
			clusterService_->setError(ec, &e);
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
					transactionManager_->remove(txn);
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
						TransactionManager::AUTO_COMMIT);

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
						TransactionManager::AUTO_COMMIT);

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
					TransactionManager::AUTO_COMMIT);

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
//			util::StackAllocator &alloc = ec.getAllocator();
			const util::DateTime now = ec.getHandlerStartTime();
//			const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig &pgConfig =
				transactionManager_->getPartitionGroupConfig();
			if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
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
						pIdCursor_[pgId], timestamp, maxScanNum, scanNum);
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
			clusterService_->setError(ec, &e);
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
			clusterService_->setError(ec, &e);
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
			clusterService_->setError(ec, &e);
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
//			bool isFinished = true;

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
			clusterService_->setError(ec, &e);
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
		clusterService_->setError(ec, &e);
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

		eeConfig_.setServerAddress(
			config.get<const char8_t *>(CONFIG_TABLE_TXN_SERVICE_ADDRESS),
			config.getUInt16(CONFIG_TABLE_TXN_SERVICE_PORT));

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		if (ClusterService::isMulticastMode(config)) {
			eeConfig_.setMulticastAddress(
				config.get<const char8_t *>(
					CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
				config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
		}
#else
		eeConfig_.setMulticastAddress(
			config.get<const char8_t *>(CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
			config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
#endif

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

void TransactionService::initialize(const ManagerSet &mgrSet) {
	try {
		ee_->setUserDataType<StatementHandler::ConnectionOption>();

		ee_->setThreadErrorHandler(serviceThreadErrorHandler_);
		serviceThreadErrorHandler_.initialize(mgrSet);

		connectHandler_.initialize(mgrSet);
		ee_->setHandler(CONNECT, connectHandler_);
		ee_->setHandlingMode(CONNECT, EventEngine::HANDLING_IMMEDIATE);

		disconnectHandler_.initialize(mgrSet);
		ee_->setCloseEventHandler(DISCONNECT, disconnectHandler_);
		ee_->setHandlingMode(
			DISCONNECT, EventEngine::HANDLING_PARTITION_SERIALIZED);

		loginHandler_.initialize(mgrSet);
		ee_->setHandler(LOGIN, loginHandler_);
		ee_->setHandlingMode(LOGIN, EventEngine::HANDLING_PARTITION_SERIALIZED);

		logoutHandler_.initialize(mgrSet);
		ee_->setHandler(LOGOUT, logoutHandler_);
		ee_->setHandlingMode(
			LOGOUT, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getPartitionAddressHandler_.initialize(mgrSet);
		ee_->setHandler(GET_PARTITION_ADDRESS, getPartitionAddressHandler_);
		ee_->setHandlingMode(
			GET_PARTITION_ADDRESS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getPartitionContainerNamesHandler_.initialize(mgrSet);
		ee_->setHandler(
			GET_PARTITION_CONTAINER_NAMES, getPartitionContainerNamesHandler_);
		ee_->setHandlingMode(GET_PARTITION_CONTAINER_NAMES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerPropertiesHandler_.initialize(mgrSet);
		ee_->setHandler(
			GET_CONTAINER_PROPERTIES, getContainerPropertiesHandler_);
		ee_->setHandlingMode(GET_CONTAINER_PROPERTIES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		putContainerHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_CONTAINER, putContainerHandler_);
		ee_->setHandlingMode(
			PUT_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);


		dropContainerHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_CONTAINER, dropContainerHandler_);
		ee_->setHandlingMode(
			DROP_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerHandler_.initialize(mgrSet);
		ee_->setHandler(GET_COLLECTION, getContainerHandler_);
		ee_->setHandlingMode(
			GET_COLLECTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createIndexHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_INDEX, createIndexHandler_);
		ee_->setHandlingMode(
			CREATE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropIndexHandler_.initialize(mgrSet);
		ee_->setHandler(DELETE_INDEX, dropIndexHandler_);
		ee_->setHandlingMode(
			DELETE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDropTriggerHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_TRIGGER, createDropTriggerHandler_);
		ee_->setHandlingMode(
			CREATE_TRIGGER, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(DELETE_TRIGGER, createDropTriggerHandler_);
		ee_->setHandlingMode(
			DELETE_TRIGGER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		flushLogHandler_.initialize(mgrSet);
		ee_->setHandler(FLUSH_LOG, flushLogHandler_);
		ee_->setHandlingMode(
			FLUSH_LOG, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(
			CREATE_TRANSACTION_CONTEXT, createTransactionContextHandler_);
		ee_->setHandlingMode(CREATE_TRANSACTION_CONTEXT,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		closeTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(
			CLOSE_TRANSACTION_CONTEXT, closeTransactionContextHandler_);
		ee_->setHandlingMode(CLOSE_TRANSACTION_CONTEXT,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		commitAbortTransactionHandler_.initialize(mgrSet);
		ee_->setHandler(COMMIT_TRANSACTION, commitAbortTransactionHandler_);
		ee_->setHandlingMode(
			COMMIT_TRANSACTION, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(ABORT_TRANSACTION, commitAbortTransactionHandler_);
		ee_->setHandlingMode(
			ABORT_TRANSACTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putRowHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_ROW, putRowHandler_);
		ee_->setHandlingMode(
			PUT_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putRowSetHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_MULTIPLE_ROWS, putRowSetHandler_);
		ee_->setHandlingMode(
			PUT_MULTIPLE_ROWS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowHandler_.initialize(mgrSet);
		ee_->setHandler(REMOVE_ROW, removeRowHandler_);
		ee_->setHandlingMode(
			REMOVE_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateRowByIdHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_ROW_BY_ID, updateRowByIdHandler_);
		ee_->setHandlingMode(
			UPDATE_ROW_BY_ID, EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowByIdHandler_.initialize(mgrSet);
		ee_->setHandler(REMOVE_ROW_BY_ID, removeRowByIdHandler_);
		ee_->setHandlingMode(
			REMOVE_ROW_BY_ID, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowHandler_.initialize(mgrSet);
		ee_->setHandler(GET_ROW, getRowHandler_);
		ee_->setHandlingMode(
			GET_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowSetHandler_.initialize(mgrSet);
		ee_->setHandler(GET_MULTIPLE_ROWS, getRowSetHandler_);
		ee_->setHandlingMode(
			GET_MULTIPLE_ROWS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTqlHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_TQL, queryTqlHandler_);
		ee_->setHandlingMode(
			QUERY_TQL, EventEngine::HANDLING_PARTITION_SERIALIZED);

		appendRowHandler_.initialize(mgrSet);
		ee_->setHandler(APPEND_TIME_SERIES_ROW, appendRowHandler_);
		ee_->setHandlingMode(
			APPEND_TIME_SERIES_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryGeometryRelatedHandler_.initialize(mgrSet);
		ee_->setHandler(
			QUERY_COLLECTION_GEOMETRY_RELATED, queryGeometryRelatedHandler_);
		ee_->setHandlingMode(QUERY_COLLECTION_GEOMETRY_RELATED,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryGeometryWithExclusionHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
			queryGeometryWithExclusionHandler_);
		ee_->setHandlingMode(QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowTimeRelatedHandler_.initialize(mgrSet);
		ee_->setHandler(GET_TIME_SERIES_ROW_RELATED, getRowTimeRelatedHandler_);
		ee_->setHandlingMode(GET_TIME_SERIES_ROW_RELATED,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowInterpolateHandler_.initialize(mgrSet);
		ee_->setHandler(INTERPOLATE_TIME_SERIES_ROW, getRowInterpolateHandler_);
		ee_->setHandlingMode(INTERPOLATE_TIME_SERIES_ROW,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		aggregateHandler_.initialize(mgrSet);
		ee_->setHandler(AGGREGATE_TIME_SERIES, aggregateHandler_);
		ee_->setHandlingMode(
			AGGREGATE_TIME_SERIES, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTimeRangeHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_TIME_SERIES_RANGE, queryTimeRangeHandler_);
		ee_->setHandlingMode(QUERY_TIME_SERIES_RANGE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTimeSamplingHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_TIME_SERIES_SAMPLING, queryTimeSamplingHandler_);
		ee_->setHandlingMode(QUERY_TIME_SERIES_SAMPLING,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		fetchResultSetHandler_.initialize(mgrSet);
		ee_->setHandler(FETCH_RESULT_SET, fetchResultSetHandler_);
		ee_->setHandlingMode(
			FETCH_RESULT_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		closeResultSetHandler_.initialize(mgrSet);
		ee_->setHandler(CLOSE_RESULT_SET, closeResultSetHandler_);
		ee_->setHandlingMode(
			CLOSE_RESULT_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiCreateTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_MULTIPLE_TRANSACTION_CONTEXTS,
			multiCreateTransactionContextHandler_);
		ee_->setHandlingMode(CREATE_MULTIPLE_TRANSACTION_CONTEXTS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiCloseTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(CLOSE_MULTIPLE_TRANSACTION_CONTEXTS,
			multiCloseTransactionContextHandler_);
		ee_->setHandlingMode(CLOSE_MULTIPLE_TRANSACTION_CONTEXTS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiPutHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_MULTIPLE_CONTAINER_ROWS, multiPutHandler_);
		ee_->setHandlingMode(PUT_MULTIPLE_CONTAINER_ROWS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiGetHandler_.initialize(mgrSet);
		ee_->setHandler(GET_MULTIPLE_CONTAINER_ROWS, multiGetHandler_);
		ee_->setHandlingMode(GET_MULTIPLE_CONTAINER_ROWS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiQueryHandler_.initialize(mgrSet);
		ee_->setHandler(EXECUTE_MULTIPLE_QUERIES, multiQueryHandler_);
		ee_->setHandlingMode(EXECUTE_MULTIPLE_QUERIES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationLogHandler_.initialize(mgrSet);
		ee_->setHandler(REPLICATION_LOG, replicationLogHandler_);
		ee_->setHandlingMode(
			REPLICATION_LOG, EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationAckHandler_.initialize(mgrSet);
		ee_->setHandler(REPLICATION_ACK, replicationAckHandler_);
		ee_->setHandlingMode(
			REPLICATION_ACK, EventEngine::HANDLING_PARTITION_SERIALIZED);

		authenticationHandler_.initialize(mgrSet);
		ee_->setHandler(AUTHENTICATION, authenticationHandler_);
		ee_->setHandlingMode(
			AUTHENTICATION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		authenticationAckHandler_.initialize(mgrSet);
		ee_->setHandler(AUTHENTICATION_ACK, authenticationAckHandler_);
		ee_->setHandlingMode(
			AUTHENTICATION_ACK, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putUserHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_USER, putUserHandler_);
		ee_->setHandlingMode(
			PUT_USER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropUserHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_USER, dropUserHandler_);
		ee_->setHandlingMode(
			DROP_USER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getUsersHandler_.initialize(mgrSet);
		ee_->setHandler(GET_USERS, getUsersHandler_);
		ee_->setHandlingMode(
			GET_USERS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putDatabaseHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_DATABASE, putDatabaseHandler_);
		ee_->setHandlingMode(
			PUT_DATABASE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropDatabaseHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_DATABASE, dropDatabaseHandler_);
		ee_->setHandlingMode(
			DROP_DATABASE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getDatabasesHandler_.initialize(mgrSet);
		ee_->setHandler(GET_DATABASES, getDatabasesHandler_);
		ee_->setHandlingMode(
			GET_DATABASES, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putPrivilegeHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_PRIVILEGE, putPrivilegeHandler_);
		ee_->setHandlingMode(
			PUT_PRIVILEGE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropPrivilegeHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_PRIVILEGE, dropPrivilegeHandler_);
		ee_->setHandlingMode(
			DROP_PRIVILEGE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		checkTimeoutHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_COLLECT_TIMEOUT_RESOURCE, checkTimeoutHandler_);
		ee_->setHandlingMode(TXN_COLLECT_TIMEOUT_RESOURCE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(
			CHUNK_EXPIRE_PERIODICALLY, dataStorePeriodicallyHandler_);
		ee_->setHandlingMode(CHUNK_EXPIRE_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.setConcurrency(mgrSet.txnSvc_->pgConfig_.getPartitionGroupCount());

		adjustStoreMemoryPeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(ADJUST_STORE_MEMORY_PERIODICALLY,
			adjustStoreMemoryPeriodicallyHandler_);
		ee_->setHandlingMode(ADJUST_STORE_MEMORY_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		backgroundHandler_.initialize(mgrSet);
		ee_->setHandler(BACK_GROUND, backgroundHandler_);
		ee_->setHandlingMode(BACK_GROUND,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDDLContinueHandler_.initialize(mgrSet);
		ee_->setHandler(CONTINUE_CREATE_INDEX, createDDLContinueHandler_);
		ee_->setHandlingMode(CONTINUE_CREATE_INDEX,
			EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(CONTINUE_ALTER_CONTAINER, createDDLContinueHandler_);
		ee_->setHandlingMode(CONTINUE_ALTER_CONTAINER,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateDataStoreStatusHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_DATA_STORE_STATUS, updateDataStoreStatusHandler_);
		ee_->setHandlingMode(UPDATE_DATA_STORE_STATUS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowSetByIdHandler_.initialize(mgrSet);
		ee_->setHandler(REMOVE_MULTIPLE_ROWS_BY_ID_SET, removeRowSetByIdHandler_);
		ee_->setHandlingMode(
			REMOVE_MULTIPLE_ROWS_BY_ID_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);
		updateRowSetByIdHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_MULTIPLE_ROWS_BY_ID_SET, updateRowSetByIdHandler_);
		ee_->setHandlingMode(
			UPDATE_MULTIPLE_ROWS_BY_ID_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);
		writeLogPeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(WRITE_LOG_PERIODICALLY, writeLogPeriodicallyHandler_);
		ee_->setHandlingMode(
			WRITE_LOG_PERIODICALLY, EventEngine::HANDLING_PARTITION_SERIALIZED);

		unsupportedStatementHandler_.initialize(mgrSet);
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

		unsupportedStatementHandler_.initialize(mgrSet);
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

		unknownStatementHandler_.initialize(mgrSet);
		ee_->setUnknownEventHandler(unknownStatementHandler_);

		ignorableStatementHandler_.initialize(mgrSet);
		ee_->setHandler(RECV_NOTIFY_MASTER, ignorableStatementHandler_);
		ee_->setHandlingMode(
			RECV_NOTIFY_MASTER, EventEngine::HANDLING_IMMEDIATE);

		shortTermSyncHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_SHORTTERM_SYNC_REQUEST, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_START, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_START_ACK, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_LOG, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_LOG_ACK, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_END, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_END_ACK, shortTermSyncHandler_);

		longTermSyncHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_LONGTERM_SYNC_REQUEST, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_START, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_START_ACK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_CHUNK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_CHUNK_ACK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_LOG, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_LOG_ACK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_PREPARE_ACK, longTermSyncHandler_);

		syncCheckEndHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_SYNC_TIMEOUT, syncCheckEndHandler_);
		ee_->setHandler(TXN_SYNC_CHECK_END, syncCheckEndHandler_);

		changePartitionTableHandler_.initialize(mgrSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_TABLE, changePartitionTableHandler_);
		changePartitionStateHandler_.initialize(mgrSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_STATE, changePartitionStateHandler_);
		dropPartitionHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_DROP_PARTITION, dropPartitionHandler_);

		checkpointOperationHandler_.initialize(mgrSet);
		ee_->setHandler(PARTITION_GROUP_START, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_START, checkpointOperationHandler_);
		ee_->setHandler(COPY_CHUNK, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_END, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_GROUP_END, checkpointOperationHandler_);
		ee_->setHandler(CLEANUP_CP_DATA, checkpointOperationHandler_);
		ee_->setHandler(CLEANUP_LOG_FILES, checkpointOperationHandler_);
		ee_->setHandler(CP_TXN_PREPARE_LONGTERM_SYNC, checkpointOperationHandler_);
		ee_->setHandler(CP_TXN_STOP_LONGTERM_SYNC, checkpointOperationHandler_);



		const PartitionGroupConfig &pgConfig =
			mgrSet.txnMgr_->getPartitionGroupConfig();

		const int32_t writeLogInterval =
			mgrSet.logMgr_->getConfig().logWriteMode_;
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


		resultSetHolderManager_.initialize(
				mgrSet.txnMgr_->getPartitionGroupConfig());

		statUpdator_.service_ = this;
		statUpdator_.manager_ = mgrSet.txnMgr_;
		mgrSet.stats_->addUpdator(&statUpdator_);

		syncManager_ = mgrSet.syncMgr_;
		dataStore_ = mgrSet.ds_;
		checkpointService_ = mgrSet.cpSvc_;

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
//		PartitionId pId = (*ev).getPartitionId();
//		EventType eventType = (*ev).getType();
		EventEngine::Tool::getLiveStats(ec.getEngine(), pgId,
			EventEngine::Stats::EVENT_ACTIVE_EXECUTABLE_COUNT,
			executableCount, &ec, ev);
		EventEngine::Tool::getLiveStats(ec.getEngine(), pgId,
			EventEngine::Stats::EVENT_CYCLE_HANDLING_AFTER_COUNT,
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
						|| eventType != TXN_LONGTERM_SYNC_LOG) {
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

