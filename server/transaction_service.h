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
	@brief Definition of TransactionService
*/
#ifndef TRANSACTION_SERVICE_H_
#define TRANSACTION_SERVICE_H_

#include "util/trace.h"
#include "data_type.h"

#include "cluster_event_type.h"
#include "event_engine.h"
#include "transaction_manager.h"
#include "checkpoint_service.h"
#include "data_store.h"
#include "sync_service.h"
#include "result_set.h"




#define TXN_PRIVATE private
#define TXN_PROTECTED protected

typedef StatementId ExecId;

#define TXN_THROW_DECODE_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(EncodeDecodeException, errorCode, message)

#define TXN_THROW_ENCODE_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(EncodeDecodeException, errorCode, message)

#define TXN_RETHROW_DECODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		EncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

#define TXN_RETHROW_ENCODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		EncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

const bool TXN_DETAIL_EXCEPTION_HIDDEN = true;

class SystemService;
class ClusterService;
class TriggerService;
class PartitionTable;
class DataStore;
class LogManager;
class ResultSet;
class MetaContainer;
struct ManagerSet;

UTIL_TRACER_DECLARE(TRANSACTION_SERVICE);
UTIL_TRACER_DECLARE(REPLICATION);
UTIL_TRACER_DECLARE(SESSION_TIMEOUT);
UTIL_TRACER_DECLARE(TRANSACTION_TIMEOUT);
UTIL_TRACER_DECLARE(REPLICATION_TIMEOUT);
UTIL_TRACER_DECLARE(AUTHENTICATION_TIMEOUT);

/*!
	@brief Exception class for denying the statement execution
*/
class DenyException : public util::Exception {
public:
	DenyException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~DenyException() throw() {}
};

/*!
	@brief Exception class to notify encoding/decoding failure
*/
class EncodeDecodeException : public util::Exception {
public:
	EncodeDecodeException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~EncodeDecodeException() throw() {}
};

enum BackgroundEventType {
	TXN_BACKGROUND,
	SYNC_EXEC,
	CP_CHUNKCOPY
};


struct StatementMessage {
	typedef int32_t FixedType;
	typedef int16_t OptionCategory;
	typedef int16_t OptionType;

	struct Utils;

	template<EventType T> struct FixedTypeResolver;

	template<typename T, T D, int> struct PrimitiveOptionCoder;
	template<typename T, T D, int, typename S = int8_t> struct EnumOptionCoder;
	template<int> struct StringOptionCoder;
	template<typename T, int> struct CustomOptionCoder;

	template<OptionType, int = 0> struct OptionCoder;

	template<EventType, int = 0> struct EventTypeTraitsBase;
	template<EventType, int = 0> struct EventTypeTraits;

	struct OptionCategorySwitcher;
	template<OptionCategory> struct OptionTypeSwitcher;

	struct FixedTypeSwitcher;
	struct EventTypeSwitcher;

	template<FixedType> struct FixedCoder;

	struct OptionSet;

	struct FixedRequest;
	struct Request;

	enum FeatureTypes {
		FEATURE_V40 = 0,
		FEATURE_V41 = 1,

		FEATURE_SUPPORTED_MAX = FEATURE_V41
	};

	struct FixedTypes {
		static const FixedType BASIC = 1;
		static const FixedType CONTAINER = 2;
		static const FixedType SCHEMA = 3;
		static const FixedType SESSION = 4;
		static const FixedType ROW = 6;
		static const FixedType MULTI = 7;
		static const FixedType PARTIAL_DDL = 8;
	};

	struct OptionCategories {
		static const OptionCategory CLIENT_CLASSIC = 0;
		static const OptionCategory BOTH_CLASSIC = 10;
		static const OptionCategory CLIENT = 11;
		static const OptionCategory SERVER_TXN = 15;

	};

	struct Options {
		static const OptionType LEGACY_VERSION_BLOCK = 0;


		static const OptionType TXN_TIMEOUT_INTERVAL = 1;
		static const OptionType FOR_UPDATE = 2;
		static const OptionType CONTAINER_LOCK_CONTINUE = 3;
		static const OptionType SYSTEM_MODE = 4;
		static const OptionType DB_NAME = 5;
		static const OptionType CONTAINER_ATTRIBUTE = 6;
		static const OptionType PUT_ROW_OPTION = 7;
		static const OptionType REQUEST_MODULE_TYPE = 8;

		static const OptionType TYPE_SQL_CLASSIC_START = 9;

		static const OptionType REPLY_PID = 9;
		static const OptionType REPLY_SERVICE_TYPE = 10; 
		static const OptionType REPLY_EVENT_TYPE = 11;
		static const OptionType UUID = 12;
		static const OptionType QUERY_ID = 13;
		static const OptionType CONTAINER_ID = 14; 
		static const OptionType QUERY_VERSION_ID = 15; 
		static const OptionType USER_TYPE = 16;
		static const OptionType DB_VERSION_ID = 17;
		static const OptionType EXTENSION_NAME = 18;
		static const OptionType EXTENSION_PARAMS = 19;
		static const OptionType INDEX_NAME = 20;
		static const OptionType EXTENSION_TYPE_LIST = 21;
		static const OptionType FOR_SYNC = 22;
		static const OptionType ACK_EVENT_TYPE = 23;
		static const OptionType SUB_CONTAINER_ID = 24;
		static const OptionType JOB_EXEC_ID = 25;



		static const OptionType STATEMENT_TIMEOUT_INTERVAL = 10001;
		static const OptionType MAX_ROWS = 10002;
		static const OptionType FETCH_SIZE = 10003;
		static const OptionType RESERVED_RETRY_MODE = 10004; 


		static const OptionType TYPE_RANGE_BASE = 1000;
		static const OptionType TYPE_RANGE_START = 11000;

		static const OptionType CLIENT_ID = 11001;
		static const OptionType FETCH_BYTES_SIZE = 11002;
		static const OptionType META_CONTAINER_ID = 11003;
		static const OptionType FEATURE_VERSION = 11004;
		static const OptionType ACCEPTABLE_FEATURE_VERSION = 11005;
		static const OptionType CONTAINER_VISIBILITY = 11006;
		static const OptionType META_NAMING_TYPE = 11007;
		static const OptionType QUERY_CONTAINER_KEY = 11008;




		static const OptionType LOCK_CONFLICT_START_TIME = 15001;
	};

	/*!
		@brief Client request type
	*/
	enum RequestType {
		REQUEST_NOSQL = 0,
		REQUEST_NEWSQL = 1,
	};

	/*!
		@brief User type
	*/
	enum UserType {
		USER_ADMIN,
		USER_NORMAL,
	};

	enum CreateDropIndexMode {
		INDEX_MODE_NOSQL = 0,
		INDEX_MODE_SQL_DEFAULT = 1,
		INDEX_MODE_SQL_EXISTS = 2
	};

	enum ContainerVisibility {
		CONTAINER_VISIBILITY_META = 1 << 0,
		CONTAINER_VISIBILITY_INTERNAL_META = 1 << 1
	};

	static const int32_t DEFAULT_TQL_PARTIAL_EXEC_FETCH_BYTE_SIZE =
			1 * 1024 * 1024;

	struct CaseSensitivity;
	struct QueryContainerKey;


	template<int C> struct OptionCoder<Options::LEGACY_VERSION_BLOCK, C> :
			public PrimitiveOptionCoder<int8_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::TXN_TIMEOUT_INTERVAL, C> :
			public PrimitiveOptionCoder<
					int32_t, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL, C> {
	};
	template<int C> struct OptionCoder<Options::FOR_UPDATE, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::CONTAINER_LOCK_CONTINUE, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::SYSTEM_MODE, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::DB_NAME, C> :
			public StringOptionCoder<C> {
	};
	template<int C> struct OptionCoder<Options::CONTAINER_ATTRIBUTE, C> :
			public EnumOptionCoder<
					ContainerAttribute, CONTAINER_ATTR_ANY, C, int32_t> {
	};
	template<int C> struct OptionCoder<Options::PUT_ROW_OPTION, C> :
			public EnumOptionCoder<PutRowOption, PUT_INSERT_OR_UPDATE, C> {
	};
	template<int C> struct OptionCoder<Options::REQUEST_MODULE_TYPE, C> :
			public EnumOptionCoder<RequestType, REQUEST_NOSQL, C> {
	};


	template<int C> struct OptionCoder<
			Options::STATEMENT_TIMEOUT_INTERVAL, C> :
			public PrimitiveOptionCoder<
						int32_t, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL, C> {
	};
	template<int C> struct OptionCoder<Options::MAX_ROWS, C> :
			public PrimitiveOptionCoder<int64_t, INT64_MAX, C> {
	};
	template<int C> struct OptionCoder<Options::FETCH_SIZE, C> :
			public PrimitiveOptionCoder<int64_t, 1 << 16, C> {
	};

	template<int C> struct OptionCoder<Options::CLIENT_ID, C> :
			public CustomOptionCoder<ClientId, C> {
	};
	template<int C> struct OptionCoder<Options::FETCH_BYTES_SIZE, C> :
			public PrimitiveOptionCoder<
					int32_t, DEFAULT_TQL_PARTIAL_EXEC_FETCH_BYTE_SIZE, C> {
	};
	template<int C> struct OptionCoder<Options::META_CONTAINER_ID, C> :
			public PrimitiveOptionCoder<ContainerId, UNDEF_CONTAINERID, C> {
	};
	template<int C> struct OptionCoder<Options::FEATURE_VERSION, C> :
			public PrimitiveOptionCoder<int32_t, 0, C> {
	};
	template<int C> struct OptionCoder<
			Options::ACCEPTABLE_FEATURE_VERSION, C> :
			public PrimitiveOptionCoder<int32_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::CONTAINER_VISIBILITY, C> :
			public PrimitiveOptionCoder<int32_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::META_NAMING_TYPE, C> :
			public PrimitiveOptionCoder<int8_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::QUERY_CONTAINER_KEY, C> :
			public CustomOptionCoder<QueryContainerKey*, C> {
	};




	template<int C> struct OptionCoder<Options::LOCK_CONFLICT_START_TIME, C> :
			public PrimitiveOptionCoder<EventMonotonicTime, -1, C> {
	};
};

template<EventType T> struct StatementMessage::FixedTypeResolver {
private:
	static const EventType FIXED_TYPE_BASE =
			(T == CONNECT ||
			T == DISCONNECT ||
			T == LOGIN ||
			T == LOGOUT ||
			T == GET_PARTITION_ADDRESS ||
			T == GET_PARTITION_CONTAINER_NAMES ||
			T == GET_CONTAINER_PROPERTIES ||
			T == GET_CONTAINER ||
			T == PUT_CONTAINER ||
			T == PUT_LARGE_CONTAINER ||
			T == DROP_CONTAINER ||
			T == PUT_USER ||
			T == DROP_USER ||
			T == GET_USERS ||
			T == PUT_DATABASE ||
			T == DROP_DATABASE ||
			T == GET_DATABASES ||
			T == PUT_PRIVILEGE ||
			T == DROP_PRIVILEGE ||
			T == UPDATE_DATA_STORE_STATUS ||
			T == UPDATE_CONTAINER_STATUS ?
			FixedTypes::BASIC :

			T == FLUSH_LOG ||
			T == CLOSE_RESULT_SET ?
			FixedTypes::CONTAINER :

			T == CREATE_INDEX ||
			T == DELETE_INDEX ||
			T == CREATE_TRIGGER ||
			T == DELETE_TRIGGER ?
			FixedTypes::SCHEMA :

			T == CREATE_TRANSACTION_CONTEXT ||
			T == CLOSE_TRANSACTION_CONTEXT ||
			T == COMMIT_TRANSACTION ||
			T == ABORT_TRANSACTION ?
			FixedTypes::SESSION :

			T == GET_ROW ||
			T == QUERY_TQL ||
			T == PUT_ROW ||
			T == PUT_MULTIPLE_ROWS ||
			T == UPDATE_ROW_BY_ID ||
			T == REMOVE_ROW ||
			T == REMOVE_ROW_BY_ID ||
			T == REMOVE_MULTIPLE_ROWS_BY_ID_SET ||
			T == UPDATE_MULTIPLE_ROWS_BY_ID_SET ||
			T == GET_MULTIPLE_ROWS ||
			T == QUERY_COLLECTION_GEOMETRY_RELATED ||
			T == QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION ||
			T == APPEND_TIME_SERIES_ROW ||
			T == GET_TIME_SERIES_ROW_RELATED ||
			T == INTERPOLATE_TIME_SERIES_ROW ||
			T == AGGREGATE_TIME_SERIES ||
			T == QUERY_TIME_SERIES_RANGE ||
			T == QUERY_TIME_SERIES_SAMPLING ||
			T == FETCH_RESULT_SET

			?
			FixedTypes::ROW :

			T == CREATE_MULTIPLE_TRANSACTION_CONTEXTS ||
			T == CLOSE_MULTIPLE_TRANSACTION_CONTEXTS ||
			T == EXECUTE_MULTIPLE_QUERIES ||
			T == GET_MULTIPLE_CONTAINER_ROWS ||
			T == PUT_MULTIPLE_CONTAINER_ROWS ?
			FixedTypes::MULTI :

			T == CONTINUE_CREATE_INDEX ||
			T == CONTINUE_ALTER_CONTAINER ?
			FixedTypes::PARTIAL_DDL : -1);

public:
	static const typename util::Conditional<
			(FIXED_TYPE_BASE >= 0), EventType, void>::Type FIXED_TYPE =
			FIXED_TYPE_BASE;
};

struct StatementMessage::Utils {
	static void encodeBool(EventByteOutStream &out, bool value);
	static bool decodeBool(EventByteInStream &in);

	static void encodeVarSizeBinary(
			EventByteOutStream &out, const util::XArray<uint8_t> &binary);
	static void encodeVarSizeBinary(
			EventByteOutStream &out, const void *data, size_t size);
	static void decodeVarSizeBinary(
			EventByteInStream &in, util::XArray<uint8_t> &binary);
};

template<typename T, T D, int> struct StatementMessage::PrimitiveOptionCoder {
	typedef T ValueType;
	typedef ValueType StorableType;
	ValueType getDefault() const { return D; }
	void encode(EventByteOutStream &out, const ValueType &value) const {
		out << value;
	}
	ValueType decode(EventByteInStream &in, util::StackAllocator&) const {
		ValueType value;
		in >> value;
		return value;
	}
};

template<bool D, int C>
struct StatementMessage::PrimitiveOptionCoder<bool, D, C> {
	typedef bool ValueType;
	typedef ValueType StorableType;
	ValueType getDefault() const { return D; }
	void encode(EventByteOutStream &out, const ValueType &value) const {
		Utils::encodeBool(out, value);
	}
	ValueType decode(EventByteInStream &in, util::StackAllocator&) const {
		return Utils::decodeBool(in);
	}
};

template<typename T, T D, int, typename S>
struct StatementMessage::EnumOptionCoder {
	typedef T ValueType;
	typedef S StorableType;
	ValueType getDefault() const { return D; }
	void encode(EventByteOutStream &out, const ValueType &value) const {
		const StorableType storableValue = value;
		out << storableValue;
	}
	ValueType decode(EventByteInStream &in, util::StackAllocator&) const {
		StorableType storableValue;
		in >> storableValue;
		return static_cast<ValueType>(storableValue);
	}

	ValueType toValue(const StorableType &src) const {
		return static_cast<ValueType>(src);
	}
	StorableType toStorable(
			const ValueType &src, util::StackAllocator&) const {
		return static_cast<StorableType>(src);
	}
};

template<int> struct StatementMessage::StringOptionCoder {
	typedef const char8_t *ValueType;
	typedef util::String *StorableType;
	ValueType getDefault() const { return ""; }
	void encode(EventByteOutStream &out, const ValueType &value) const {
		assert(value != NULL);
		out << value;
	}
	ValueType decode(
			EventByteInStream &in, util::StackAllocator &alloc) const {
		util::String *strValue = ALLOC_NEW(alloc) util::String(alloc);
		in >> *strValue;
		return strValue->c_str();
	}

	ValueType toValue(const StorableType &src) const {
		assert(src != NULL);
		return src->c_str();
	}
	StorableType toStorable(
			const ValueType &src, util::StackAllocator &alloc) const {
		assert(src != NULL);
		return ALLOC_NEW(alloc) util::String(src, alloc);
	}
};

template<typename T, int> struct StatementMessage::CustomOptionCoder {
	typedef T ValueType;
	typedef ValueType StorableType;
	ValueType getDefault() const { return ValueType(); }
	void encode(EventByteOutStream &out, const ValueType &value) const;
	ValueType decode(EventByteInStream &in, util::StackAllocator &alloc) const;
};

template<StatementMessage::FixedType>
struct StatementMessage::FixedCoder {
	void encode(EventByteOutStream &out, const FixedRequest &value) const;
	void decode(EventByteInStream &in, FixedRequest &value) const;
};

struct StatementMessage::OptionCategorySwitcher {
	template<typename Action>
	bool switchByCategory(OptionCategory category, Action &action) const;
};

template<StatementMessage::OptionCategory>
struct StatementMessage::OptionTypeSwitcher {
	template<typename Action>
	bool switchByType(OptionType type, Action &action) const;
};

struct StatementMessage::FixedTypeSwitcher {
	template<typename Action>
	bool switchByFixedType(FixedType type, Action &action) const;
};

struct StatementMessage::EventTypeSwitcher {
	template<typename Action>
	bool switchByEventType(EventType type, Action &action) const;
};

struct StatementMessage::OptionSet {
public:
	template<OptionCategory C> struct EntryEncoder;
	template<OptionCategory C> struct EntryDecoder;

	explicit OptionSet(util::StackAllocator &alloc);

	void setLegacyVersionBlock(bool enabled);
	void checkFeature();

	void encode(EventByteOutStream &out) const;
	void decode(EventByteInStream &in);

	template<OptionType T>
	typename OptionCoder<T>::ValueType get() const;

	template<OptionType T>
	void set(const typename OptionCoder<T>::ValueType &value);

	util::StackAllocator& getAllocator();

private:
	static const OptionCategory UNDEF_CATEGORY = -1;

	typedef util::FalseType BoolTag;
	typedef std::pair<util::TrueType, util::FalseType> IntTag;
	typedef std::pair<util::FalseType, util::TrueType> PtrTag;
	typedef std::pair<util::FalseType, util::FalseType> OtherTag;

	template<typename T> struct TagResolver {
		typedef typename util::BoolType<
				std::numeric_limits<T>::is_integer>::Result ForInt;
		typedef typename util::IsPointer<T>::Type ForPtr;
		typedef typename util::Conditional<
				(util::IsSame<T, bool>::VALUE),
				BoolTag, std::pair<ForInt, ForPtr> >::Type Result;
	};

	union ValueStorage {
		template<typename T> T get() const {
			typedef typename TagResolver<T>::Result Tag;
			return get<T>(Tag());
		}
		template<typename T> T get(const BoolTag&) const {
			return !!intValue_;
		}
		template<typename T> T get(const IntTag&) const {
			return static_cast<T>(intValue_);
		}
		template<typename T> T get(const PtrTag&) const {
			return static_cast<T>(ptrValue_);
		}
		template<typename T> T get(const OtherTag&) const {
			assert(ptrValue_ != NULL);
			return *static_cast<T*>(ptrValue_);
		}

		template<typename T>
		void set(const T &value, util::StackAllocator &alloc) {
			typedef typename TagResolver<T>::Result Tag;
			set(value, alloc, Tag());
		}
		template<typename T, typename U> void set(
				const T &value, util::StackAllocator&, const U&) {
			intValue_ = value;
		}
		template<typename T> void set(
				const T &value, util::StackAllocator&, const PtrTag&) {
			ptrValue_ = value;
		}
		template<typename T> void set(
				const T &value, util::StackAllocator &alloc, const OtherTag&) {
			ptrValue_ = ALLOC_NEW(alloc) T(value);
		}

		int64_t intValue_;
		void *ptrValue_;
	};

	template<typename C>
	typename C::StorableType toStorable(
			const typename C::ValueType &value, const util::TrueType&) {
		return value;
	}

	template<typename C>
	typename C::StorableType toStorable(
			const typename C::ValueType &value, const util::FalseType&) {
		return C().toStorable(value, getAllocator());
	}

	template<typename C>
	typename C::ValueType toValue(
			const typename C::StorableType &value, const util::TrueType&) const {
		return value;
	}

	template<typename C>
	typename C::ValueType toValue(
			const typename C::StorableType &value, const util::FalseType&) const {
		return C().toValue(value);
	}

	static OptionCategory toOptionCategory(OptionType optionType);
	static bool isOptionClassic(OptionType optionType);

	typedef util::Map<OptionType, ValueStorage> EntryMap;
	EntryMap entryMap_;
};

template<StatementMessage::OptionCategory C>
struct StatementMessage::OptionSet::EntryEncoder {
	EntryEncoder(
			EventByteOutStream &out, const OptionSet &optionSet,
			OptionType type);

	template<OptionCategory T> void opCategory();
	template<OptionType T> void opType();

	EventByteOutStream &out_;
	const OptionSet &optionSet_;
	OptionType type_;
};

template<StatementMessage::OptionCategory C>
struct StatementMessage::OptionSet::EntryDecoder {
	EntryDecoder(EventByteInStream &in, OptionSet &optionSet, OptionType type);

	template<OptionCategory T> void opCategory();
	template<OptionType T> void opType();

	EventByteInStream &in_;
	OptionSet &optionSet_;
	OptionType type_;
};

struct StatementMessage::FixedRequest {
	struct Source;
	struct Encoder;
	struct Decoder;
	struct EventToFixedType;

	explicit FixedRequest(const Source &src);

	void encode(EventByteOutStream &out) const;
	void decode(EventByteInStream &in);

	static FixedType toFixedType(EventType type);

	const PartitionId pId_;
	const EventType stmtType_;
	ClientId clientId_;
	TransactionManager::ContextSource cxtSrc_;
	SchemaVersionId schemaVersionId_;
	StatementId startStmtId_;
};

struct StatementMessage::FixedRequest::Source {
	Source(PartitionId pId, EventType stmtType);

	PartitionId pId_;
	EventType stmtType_;
};

struct StatementMessage::FixedRequest::Encoder {
	Encoder(EventByteOutStream &out, const FixedRequest &request);
	template<FixedType T> void opFixedType();

	EventByteOutStream &out_;
	const FixedRequest &request_;
};

struct StatementMessage::FixedRequest::Decoder {
	Decoder(EventByteInStream &in, FixedRequest &request);
	template<FixedType T> void opFixedType();

	EventByteInStream &in_;
	FixedRequest &request_;
};

struct StatementMessage::FixedRequest::EventToFixedType {
	EventToFixedType();
	template<EventType T> void opEventType();

	FixedType fixedType_;
};

struct StatementMessage::Request {
	Request(util::StackAllocator &alloc, const FixedRequest::Source &src);

	void encode(EventByteOutStream &out) const;
	void decode(EventByteInStream &in);

	FixedRequest fixed_;
	OptionSet optional_;
};

struct StatementMessage::CaseSensitivity {
	uint8_t flags_;

	CaseSensitivity() : flags_(0) {}
	CaseSensitivity(const CaseSensitivity &another) : flags_(another.flags_) {}
	CaseSensitivity& operator=(const CaseSensitivity &another) {
		if (this == &another) {
			return *this;
		}
		flags_ = another.flags_;
		return *this;
	}
	bool isDatabaseNameCaseSensitive() const {
		return ((flags_ & 0x80) != 0);
	}
	bool isUserNameCaseSensitive() const {
		return ((flags_ & 0x40) != 0);
	}
	bool isContainerNameCaseSensitive() const {
		return ((flags_ & 0x20) != 0);
	}
	bool isIndexNameCaseSensitive() const {
		return ((flags_ & 0x10) != 0);
	}
	bool isColumnNameCaseSensitive() const {
		return ((flags_ & 0x08) != 0);
	}
	void setDatabaseNameCaseSensitive() {
		flags_ |= 0x80;
	}
	void setUserNameCaseSensitive() {
		flags_ |= 0x40;
	}
	void setContainerNameCaseSensitive() {
		flags_ |= 0x20;
	}
	void setIndexNameCaseSensitive() {
		flags_ |= 0x10;
	}
	void setColumnNameCaseSensitive() {
		flags_ |= 0x08;
	}
	bool isAllNameCaseInsensitive() const {
		return (flags_ == 0);
	}
	void clear() {
		flags_ = 0;
	}
};

struct StatementMessage::QueryContainerKey {
	explicit QueryContainerKey(util::StackAllocator &alloc);

	util::XArray<uint8_t> containerKeyBinary_;
	bool enabled_;
};


/*!
	@brief Handles the statement(event) requested from a client or another node
*/
class StatementHandler : public EventHandler {
	friend struct ScenarioConfig;
	friend class TransactionHandlerTest;

public:
	typedef StatementMessage Message;

	typedef Message::OptionType OptionType;
	typedef Message::Utils MessageUtils;
	typedef Message::OptionSet OptionSet;
	typedef Message::FixedRequest FixedRequest;
	typedef Message::Request Request;
	typedef Message::Options Options;

	typedef Message::RequestType RequestType;
	typedef Message::UserType UserType;
	typedef Message::CreateDropIndexMode CreateDropIndexMode;
	typedef Message::CaseSensitivity CaseSensitivity;
	typedef Message::QueryContainerKey QueryContainerKey;


	StatementHandler();

	virtual ~StatementHandler();
	void initialize(const ManagerSet &mgrSet);

	static const size_t USER_NAME_SIZE_MAX = 64;  
	static const size_t PASSWORD_SIZE_MAX = 64;  
	static const size_t DATABASE_NAME_SIZE_MAX = 64;  

	static const size_t USER_NUM_MAX = 128;		 
	static const size_t DATABASE_NUM_MAX = 128;  

	enum QueryResponseType {
		ROW_SET					= 0,
		AGGREGATION				= 1,
		QUERY_ANALYSIS			= 2,
		PARTIAL_FETCH_STATE		= 3,	
		PARTIAL_EXECUTION_STATE = 4,	
		DIST_TARGET		= 32,	
	};

	typedef int32_t ProtocolVersion;

	static const ProtocolVersion PROTOCOL_VERSION_UNDEFINED;

	static const ProtocolVersion TXN_V1_0_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V1_1_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V1_5_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_0_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_1_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_5_X_CLIENT_VERSION;

	static const ProtocolVersion TXN_V2_7_X_CLIENT_VERSION;

	static const ProtocolVersion TXN_V2_8_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_9_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_0_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_0_X_CE_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_1_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_2_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_5_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V4_0_0_CLIENT_VERSION;
	static const ProtocolVersion TXN_V4_0_1_CLIENT_VERSION;
	static const ProtocolVersion TXN_CLIENT_VERSION;


	typedef uint8_t StatementExecStatus;  
	static const StatementExecStatus TXN_STATEMENT_SUCCESS;  
	static const StatementExecStatus
		TXN_STATEMENT_ERROR;  
	static const StatementExecStatus
		TXN_STATEMENT_NODE_ERROR;  
	static const StatementExecStatus
		TXN_STATEMENT_DENY;  
	static const StatementExecStatus
		TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT;  

	typedef util::XArray<uint8_t> RowKeyData;  
	typedef util::XArray<uint8_t> RowData;	 
	typedef util::Map<int8_t, util::XArray<uint8_t> *> PartialQueryOption;	 

	/*!
		@brief Represents fetch setting
	*/
	struct FetchOption {
		FetchOption() : limit_(0), size_(0) {}

		ResultSize limit_;
		ResultSize size_;
	};

	/*!
		@brief Represents geometry query
	*/
	struct GeometryQuery {
		explicit GeometryQuery(util::StackAllocator &alloc)
			: columnId_(UNDEF_COLUMNID),
			  intersection_(alloc),
			  disjoint_(alloc),
			  operator_(GEOMETRY_INTERSECT) {}

		ColumnId columnId_;
		util::XArray<uint8_t> intersection_;
		util::XArray<uint8_t> disjoint_;
		GeometryOperator operator_;
	};

	/*!
		@brief Represents time-related condition
	*/
	struct TimeRelatedCondition {
		TimeRelatedCondition()
			: rowKey_(UNDEF_TIMESTAMP), operator_(TIME_PREV) {}

		Timestamp rowKey_;
		TimeOperator operator_;
	};

	/*!
		@brief Represents interpolation condition
	*/
	struct InterpolateCondition {
		InterpolateCondition()
			: rowKey_(UNDEF_TIMESTAMP), columnId_(UNDEF_COLUMNID) {}

		Timestamp rowKey_;
		ColumnId columnId_;
	};

	/*!
		@brief Represents aggregate condition
	*/
	struct AggregateQuery {
		AggregateQuery()
			: start_(UNDEF_TIMESTAMP),
			  end_(UNDEF_TIMESTAMP),
			  columnId_(UNDEF_COLUMNID),
			  aggregationType_(AGG_MIN) {}

		Timestamp start_;
		Timestamp end_;
		ColumnId columnId_;
		AggregationType aggregationType_;
	};

	/*!
		@brief Represents time range condition
	*/
	struct RangeQuery {
		RangeQuery()
			: start_(UNDEF_TIMESTAMP),
			  end_(UNDEF_TIMESTAMP),
			  order_(ORDER_ASCENDING) {}

		Timestamp start_;
		Timestamp end_;
		OutputOrder order_;
	};

	/*!
		@brief Represents sampling condition
	*/
	struct SamplingQuery {
		explicit SamplingQuery(util::StackAllocator &alloc)
			: start_(UNDEF_TIMESTAMP),
			  end_(UNDEF_TIMESTAMP),
			  timeUnit_(TIME_UNIT_YEAR),
			  interpolatedColumnIdList_(alloc),
			  mode_(INTERP_MODE_LINEAR_OR_PREVIOUS) {}

		Sampling toSamplingOption() const;

		Timestamp start_;
		Timestamp end_;
		uint32_t interval_;
		TimeUnit timeUnit_;
		util::XArray<uint32_t> interpolatedColumnIdList_;
		InterpolationMode mode_;
	};


	/*!
		@brief Represents the information about a user
	*/
	struct UserInfo {
		explicit UserInfo(util::StackAllocator &alloc)
			: userName_(alloc),
			  property_(0),
			  withDigest_(false),
			  digest_(alloc) {}

		util::String userName_;  
		int8_t property_;  
		bool withDigest_;	  
		util::String digest_;  

		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << "\tuserName=" << userName_ << std::endl;
			strstrm << "\tproperty=" << static_cast<int>(property_)
					<< std::endl;
			if (withDigest_) {
				strstrm << "\twithDigest=true" << std::endl;
			}
			else {
				strstrm << "\twithDigest=false" << std::endl;
			}
			strstrm << "\tdigest=" << digest_ << std::endl;
			return strstrm.str();
		}
	};

	/*!
		@brief Represents the privilege information about a user
	*/
	struct PrivilegeInfo {
		explicit PrivilegeInfo(util::StackAllocator &alloc)
			: userName_(alloc), privilege_(alloc) {}

		util::String userName_;   
		util::String privilege_;  

		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << "\tuserName=" << userName_ << std::endl;
			strstrm << "\tprivilege=" << privilege_ << std::endl;
			return strstrm.str();
		}
	};

	/*!
		@brief Represents the information about a database
	*/
	struct DatabaseInfo {
		explicit DatabaseInfo(util::StackAllocator &alloc)
			: dbName_(alloc), property_(0), privilegeInfoList_(alloc) {}

		util::String dbName_;  
		int8_t property_;	  
		util::XArray<PrivilegeInfo *> privilegeInfoList_;  

		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << "\tdbName=" << dbName_ << std::endl;
			strstrm << "\tproperty=" << static_cast<int>(property_)
					<< std::endl;
			strstrm << "\tprivilegeNum=" << privilegeInfoList_.size()
					<< std::endl;
			for (size_t i = 0; i < privilegeInfoList_.size(); i++) {
				strstrm << privilegeInfoList_[i]->dump();
			}
			return strstrm.str();
		}
	};

	struct ConnectionOption;

	/*!
		@brief Represents response to a client
	*/
	struct Response {
		explicit Response(util::StackAllocator &alloc)
			: binaryData_(alloc),
			  containerNum_(0),
			  containerNameList_(alloc),
			  existFlag_(false),
			  schemaVersionId_(UNDEF_SCHEMAVERSIONID),
			  containerId_(UNDEF_CONTAINERID),
			  binaryData2_(alloc),
			  rs_(NULL),
			  last_(0),
			  userInfoList_(alloc),
			  databaseInfoList_(alloc),
			  containerAttribute_(CONTAINER_ATTR_SINGLE),
			  putRowOption_(0)
			  ,
			  connectionOption_(NULL)
		{
		}


		util::XArray<uint8_t> binaryData_;

		uint64_t containerNum_;
		util::XArray<FullContainerKey> containerNameList_;


		bool existFlag_;
		SchemaVersionId schemaVersionId_;
		ContainerId containerId_;


		util::XArray<uint8_t> binaryData2_;


		ResultSet *rs_;




		RowId last_;



		util::XArray<UserInfo *> userInfoList_;

		util::XArray<DatabaseInfo *> databaseInfoList_;
		ContainerAttribute containerAttribute_;

		uint8_t putRowOption_;
		ConnectionOption *connectionOption_;
	};

	/*!
		@brief Represents the information about ReplicationAck
	*/
	struct ReplicationAck {
		explicit ReplicationAck(PartitionId pId) : pId_(pId) {}

		const PartitionId pId_;
		ClusterVersionId clusterVer_;
		ReplicationId replId_;

		int32_t replMode_;
		int32_t replStmtType_;
		StatementId replStmtId_;
		ClientId clientId_;
		int32_t taskStatus_;
	};

	/*!
		@brief Represents the information about AuthenticationAck
	*/
	struct AuthenticationAck {
		explicit AuthenticationAck(PartitionId pId) : pId_(pId) {}

		PartitionId pId_;
		ClusterVersionId clusterVer_;
		AuthenticationId authId_;

		PartitionId authPId_;
	};

	void executeAuthentication(
			EventContext &ec, Event &ev,
			const NodeDescriptor &clientND, StatementId authStmtId,
			const char8_t *userName, const char8_t *digest,
			const char8_t *dbName, UserType userType);

	void executeAuthenticationInternal(
			EventContext &ec,
			util::StackAllocator &alloc, TransactionManager::ContextSource &cxtSrc,
			const char8_t *userName, const char8_t *digest, const char8_t *dbName,
			UserType userType, int checkLevel, DatabaseId &dbId);

	void replyAuthenticationAck(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, const AuthenticationAck &request,
		DatabaseId dbId);

	void decodeAuthenticationAck(
		util::ByteStream<util::ArrayInStream> &in, AuthenticationAck &ack);
	void encodeAuthenticationAckPart(EventByteOutStream &out,
		ClusterVersionId clusterVer, AuthenticationId authId, PartitionId authPId);

	void makeUsersSchema(util::XArray<uint8_t> &containerSchema);
	void makeDatabasesSchema(util::XArray<uint8_t> &containerSchema);
	void makeUsersRow(util::StackAllocator &alloc,
		const ColumnInfo *columnInfoList, RowData &rowData);
	void makeUsersRow(util::StackAllocator &alloc,
		const ColumnInfo *columnInfoList, uint32_t columnNum,
		UserInfo &userInfo, RowData &rowData);
	void makeDatabasesRow(util::StackAllocator &alloc,
		const ColumnInfo *columnInfoList, RowData &rowData);
	void makeDatabasesRow(util::StackAllocator &alloc,
		const ColumnInfo *columnInfoList, uint32_t columnNum,
		DatabaseInfo &dbInfo, bool isCreate, RowData &rowData);

	void makeDatabaseInfoList(TransactionContext &txn,
		util::StackAllocator &alloc, BaseContainer &container, ResultSet &rs,
		util::XArray<DatabaseInfo *> &dbInfoList);

	void makeRowKey(const char *name, RowKeyData &rowKey);
	void makeDatabaseRowKey(DatabaseInfo &dbInfo, RowKeyData &rowKey);

	void initializeMetaContainer(
			EventContext &ec, Event &ev, const Request &request,
			util::XArray<const util::XArray<uint8_t>*> &logRecordList);

	static void checkPasswordLength(const char8_t *password);

	bool checkContainer(
			EventContext &ec, Request &request, const char8_t *containerName);
	int64_t count(
			EventContext &ec, const Request &request,
			const char8_t *containerName);

	void checkUser(
			EventContext &ec, const Request &request,
			const char8_t *userName, bool &existFlag);

	void putUserRow(
			EventContext &ec, Event &ev, const Request &request,
			UserInfo &userInfo,
			util::XArray<const util::XArray<uint8_t> *> &logRecordList);
	void putDatabaseRow(
			EventContext &ec, Event &ev, const Request &request,
			DatabaseInfo &dbInfo, bool isCreate,
			util::XArray<const util::XArray<uint8_t> *> &logRecordList);

	void checkUserWithTQL(
			EventContext &ec, const Request &request,
			const char8_t *userName, const char8_t *digest, bool detailFlag,
			bool &existFlag);
	void executeTQLUser(
			EventContext &ec, const Request &request,
			const char8_t *userName, UserType userType,
			util::XArray<UserInfo *> &userInfoList);

	void checkDatabaseWithTQL(
			EventContext &ec, const Request &request, DatabaseInfo &dbInfo);
	void checkDetailDatabaseWithTQL(
			EventContext &ec, const Request &request,
			DatabaseInfo &dbInfo, bool &existFlag);

	void setSuccessReply(
			util::StackAllocator &alloc, Event &ev, StatementId stmtId,
			StatementExecStatus status, const Response &response);
	static void setErrorReply(
			Event &ev, StatementId stmtId,
			StatementExecStatus status, const std::exception &exception,
			const NodeDescriptor &nd);


	typedef uint32_t
		ClusterRole;  
	static const ClusterRole CROLE_UNKNOWN;	
	static const ClusterRole CROLE_SUBMASTER;  
	static const ClusterRole CROLE_MASTER;	 
	static const ClusterRole CROLE_FOLLOWER;   
	static const ClusterRole CROLE_ANY;		   
	static const char8_t *const clusterRoleStr[8];

	typedef uint32_t PartitionRoleType;			   
	static const PartitionRoleType PROLE_UNKNOWN;  
	static const PartitionRoleType
		PROLE_NONE;  
	static const PartitionRoleType
		PROLE_OWNER;  
	static const PartitionRoleType
		PROLE_BACKUP;  
	static const PartitionRoleType
		PROLE_CATCHUP;  
	static const PartitionRoleType PROLE_ANY;  
	static const char8_t *const partitionRoleTypeStr[16];

	typedef uint32_t PartitionStatus;  
	static const PartitionStatus PSTATE_UNKNOWN;  
	static const PartitionStatus
		PSTATE_ON;  
	static const PartitionStatus
		PSTATE_SYNC;  
	static const PartitionStatus
		PSTATE_OFF;  
	static const PartitionStatus
		PSTATE_STOP;  
	static const PartitionStatus PSTATE_ANY;  
	static const char8_t *const partitionStatusStr[16];

	static const bool IMMEDIATE_CONSISTENCY =
		true;  
	static const bool ANY_CONSISTENCY = false;  

	static const bool NO_REPLICATION =
		false;  

	typedef std::pair<const char8_t*, const char8_t*> ExceptionParameterEntry;
	typedef util::Vector<ExceptionParameterEntry> ExceptionParameterList;

	ClusterService *clusterService_;
	ClusterManager *clusterManager_;
	ChunkManager *chunkManager_;
	DataStore *dataStore_;
	LogManager *logManager_;
	PartitionTable *partitionTable_;
	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	TriggerService *triggerService_;
	SystemService *systemService_;
	RecoveryManager *recoveryManager_;

	/*!
		@brief Represents the information about a connection
	*/
	struct ConnectionOption {
		ConnectionOption()
			: clientVersion_(PROTOCOL_VERSION_UNDEFINED),
			  txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
			  isAuthenticated_(false),
			  isImmediateConsistency_(false),
			  dbId_(0),
			  isAdminAndPublicDB_(true),
			  userType_(Message::USER_NORMAL),
			  authenticationTime_(0),
			  requestType_(Message::REQUEST_NOSQL)
			  ,
			  authMode_(0)
			  ,
				clientId_()
			  ,
			  keepaliveTime_(0)
		{
		}

			void clear() {
			clientVersion_ = PROTOCOL_VERSION_UNDEFINED;
			txnTimeoutInterval_ = TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL;
			isAuthenticated_ = false;
			isImmediateConsistency_ = false;
			dbId_ = 0;
			isAdminAndPublicDB_ = true;
			userType_ = Message::USER_NORMAL;
			authenticationTime_ = 0;
			requestType_ = Message::REQUEST_NOSQL;

			userName_.clear();
			dbName_.clear();
			clientId_ = ClientId();
			keepaliveTime_ = 0;
			currentSessionId_ = 0;
		}

		ProtocolVersion clientVersion_;
		int32_t txnTimeoutInterval_;
		bool isAuthenticated_;
		bool isImmediateConsistency_;

		DatabaseId dbId_;
		bool isAdminAndPublicDB_;
		UserType userType_;  
		EventMonotonicTime authenticationTime_;
		RequestType requestType_;  

		std::string userName_;
		std::string dbName_;
		const int8_t authMode_;
		ClientId clientId_;
		EventMonotonicTime keepaliveTime_;
		SessionId currentSessionId_;
	};

	struct LockConflictStatus {
		LockConflictStatus(
				int32_t txnTimeoutInterval,
				EventMonotonicTime initialConflictMillis,
				EventMonotonicTime emNow) :
				txnTimeoutInterval_(txnTimeoutInterval),
				initialConflictMillis_(initialConflictMillis),
				emNow_(emNow) {
		}

		int32_t txnTimeoutInterval_;
		EventMonotonicTime initialConflictMillis_;
		EventMonotonicTime emNow_;
	};


	void checkAuthentication(
			const NodeDescriptor &ND, EventMonotonicTime emNow);
	void checkConsistency(const NodeDescriptor &ND, bool requireImmediate);
	void checkExecutable(
			PartitionId pId, ClusterRole requiredClusterRole,
			PartitionRoleType requiredPartitionRole,
			PartitionStatus requiredPartitionStatus, PartitionTable *pt);

	void checkExecutable(ClusterRole requiredClusterRole);

	void checkTransactionTimeout(
			EventMonotonicTime now,
			EventMonotonicTime queuedTime, int32_t txnTimeoutIntervalSec,
			uint32_t queueingCount);
	void checkContainerExistence(BaseContainer *container);
	void checkContainerSchemaVersion(
			BaseContainer *container, SchemaVersionId schemaVersionId);
	void checkUserName(const char8_t *userName, bool detailed);
	void checkAdminUser(UserType userType);
	void checkDatabaseName(const char8_t *dbName);
	void checkConnectedDatabaseName(
			ConnectionOption &connOption, const char8_t *dbName);
	void checkConnectedUserName(
			ConnectionOption &connOption, const char8_t *userName);
	void checkPartitionIdZero(PartitionId pId);
	void checkDigest(const char8_t *digest, size_t maxStrLength);
	void checkPrivilegeSize(DatabaseInfo &dbInfo, size_t size);
	void checkModifiable(bool modifiable, bool value);
	void checkFetchOption(FetchOption fetchOption);
	void checkSizeLimit(ResultSize limit);
	static void checkLoggedInDatabase(
			DatabaseId loginDbId, const char8_t *loginDbName,
			DatabaseId specifiedDbId, const char8_t *specifiedDbName
			);
	void checkQueryOption(
			const OptionSet &optionSet,
			const FetchOption &fetchOption, bool isPartial, bool isTQL);
	void applyQueryOption(
			ResultSet &rs, const OptionSet &optionSet,
			const int32_t *fetchBytesSize, bool partial,
			const PartialQueryOption &partialOption);
	static void checkLogVersion(uint16_t logVersion);


	static FixedRequest::Source getRequestSource(const Event &ev);

	static void decodeRequestCommonPart(
			EventByteInStream &in, Request &request,
			ConnectionOption &connOption);
	static void decodeRequestOptionPart(
			EventByteInStream &in, Request &request,
			ConnectionOption &connOption);

	static void decodeOptionPart(
			EventByteInStream &in, OptionSet &optionSet);


	static CaseSensitivity getCaseSensitivity(const Request &request);
	static CaseSensitivity getCaseSensitivity(const OptionSet &optionSet);

	static CreateDropIndexMode getCreateDropIndexMode(
			const OptionSet &optionSet);
	static bool isDdlTransaction(const OptionSet &optionSet);

	static void assignIndexExtension(
			IndexInfo &indexInfo, const OptionSet &optionSet);
	static const char8_t* getExtensionName(const OptionSet &optionSet);

	static void decodeIndexInfo(
		util::ByteStream<util::ArrayInStream> &in, IndexInfo &indexInfo);

	static void decodeTriggerInfo(
		util::ByteStream<util::ArrayInStream> &in, TriggerInfo &triggerInfo);
	static void decodeMultipleRowData(util::ByteStream<util::ArrayInStream> &in,
		uint64_t &numRow, RowData &rowData);
	static void decodeFetchOption(
		util::ByteStream<util::ArrayInStream> &in, FetchOption &fetchOption);
	static void decodePartialQueryOption(
		util::ByteStream<util::ArrayInStream> &in, util::StackAllocator &alloc,
		bool &isPartial, PartialQueryOption &partitalQueryOption);

	static void decodeGeometryRelatedQuery(
		util::ByteStream<util::ArrayInStream> &in, GeometryQuery &query);
	static void decodeGeometryWithExclusionQuery(
		util::ByteStream<util::ArrayInStream> &in, GeometryQuery &query);
	static void decodeTimeRelatedConditon(util::ByteStream<util::ArrayInStream> &in,
		TimeRelatedCondition &condition);
	static void decodeInterpolateConditon(util::ByteStream<util::ArrayInStream> &in,
		InterpolateCondition &condition);
	static void decodeAggregateQuery(
		util::ByteStream<util::ArrayInStream> &in, AggregateQuery &query);
	static void decodeRangeQuery(
		util::ByteStream<util::ArrayInStream> &in, RangeQuery &query);
	static void decodeSamplingQuery(
		util::ByteStream<util::ArrayInStream> &in, SamplingQuery &query);
	static void decodeContainerConditionData(util::ByteStream<util::ArrayInStream> &in,
		DataStore::ContainerCondition &containerCondition);

	template <typename IntType>
	static void decodeIntData(
		util::ByteStream<util::ArrayInStream> &in, IntType &intData);
	template <typename LongType>
	static void decodeLongData(
		util::ByteStream<util::ArrayInStream> &in, LongType &longData);
	template <typename StringType>
	static void decodeStringData(
		util::ByteStream<util::ArrayInStream> &in, StringType &strData);
	static void decodeBooleanData(
		util::ByteStream<util::ArrayInStream> &in, bool &boolData);
	static void decodeBinaryData(util::ByteStream<util::ArrayInStream> &in,
		util::XArray<uint8_t> &binaryData, bool readAll);
	static void decodeVarSizeBinaryData(util::ByteStream<util::ArrayInStream> &in,
		util::XArray<uint8_t> &binaryData);
	template <typename EnumType>
	static void decodeEnumData(
		util::ByteStream<util::ArrayInStream> &in, EnumType &enumData);
	static void decodeUUID(util::ByteStream<util::ArrayInStream> &in, uint8_t *uuid,
		size_t uuidSize);

	static void decodeReplicationAck(
		util::ByteStream<util::ArrayInStream> &in, ReplicationAck &ack);
	static void decodeUserInfo(
		util::ByteStream<util::ArrayInStream> &in, UserInfo &userInfo);
	static void decodeDatabaseInfo(util::ByteStream<util::ArrayInStream> &in,
		DatabaseInfo &dbInfo, util::StackAllocator &alloc);

	static EventByteOutStream encodeCommonPart(
		Event &ev, StatementId stmtId, StatementExecStatus status);

	static void encodeIndexInfo(
		EventByteOutStream &out, const IndexInfo &indexInfo);
	static void encodeIndexInfo(
		util::XArrayByteOutStream &out, const IndexInfo &indexInfo);

	template <typename IntType>
	static void encodeIntData(EventByteOutStream &out, IntType intData);
	template <typename LongType>
	static void encodeLongData(EventByteOutStream &out, LongType longData);
	template<typename CharT, typename Traits, typename Alloc>
	static void encodeStringData(
			EventByteOutStream &out,
			const std::basic_string<CharT, Traits, Alloc> &strData);
	static void encodeStringData(
			EventByteOutStream &out, const char *strData);

	static void encodeBooleanData(EventByteOutStream &out, bool boolData);
	static void encodeBinaryData(
		EventByteOutStream &out, const uint8_t *data, size_t size);
	template <typename EnumType>
	static void encodeEnumData(EventByteOutStream &out, EnumType enumData);
	static void encodeUUID(
		EventByteOutStream &out, const uint8_t *uuid, size_t uuidSize);
	static void encodeVarSizeBinaryData(
		EventByteOutStream &out, const uint8_t *data, size_t size);
	static void encodeContainerKey(EventByteOutStream &out,
		const FullContainerKey &containerKey);

	template <typename ByteOutStream>
	static void encodeException(ByteOutStream &out,
		const std::exception &exception, bool detailsHidden,
		const ExceptionParameterList *paramList = NULL);
	template <typename ByteOutStream>
	static void encodeException(ByteOutStream &out,
		const std::exception &exception, const NodeDescriptor &nd);

	static void encodeReplicationAckPart(EventByteOutStream &out,
		ClusterVersionId clusterVer, int32_t replMode, const ClientId &clientId,
		ReplicationId replId, EventType replStmtType, StatementId replStmtId,
		int32_t taskStatus);
	static int32_t encodeDistributedResult(
		EventByteOutStream &out, const ResultSet &rs, int64_t *encodedSize);


	static bool isUpdateStatement(EventType stmtType);

	void replySuccess(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, EventType stmtType,
			StatementExecStatus status, const Request &request,
			const Response &response, bool ackWait);
	void continueEvent(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, EventType stmtType, StatementId originalStmtId,
			const Request &request, const Response &response, bool ackWait);
	void continueEvent(
			EventContext &ec, util::StackAllocator &alloc,
			StatementExecStatus status, const ReplicationContext &replContext);
	void replySuccess(
			EventContext &ec, util::StackAllocator &alloc,
			StatementExecStatus status, const ReplicationContext &replContext);
	void replySuccess(
			EventContext &ec, util::StackAllocator &alloc,
			StatementExecStatus status, const Request &request,
			const AuthenticationContext &authContext);
	void replyError(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, EventType stmtType,
			StatementExecStatus status, const Request &request,
			const std::exception &e);

	bool executeReplication(
			const Request &request, EventContext &ec,
			util::StackAllocator &alloc, const NodeDescriptor &clientND,
			TransactionContext &txn, EventType replStmtType, StatementId replStmtId,
			int32_t replMode,
			const ClientId *closedResourceIds, size_t closedResourceIdCount,
			const util::XArray<uint8_t> **logRecordList, size_t logRecordCount,
			const Response &response) {
		return executeReplication(request, ec, alloc, clientND, txn, replStmtType,
				replStmtId, replMode, ReplicationContext::TASK_FINISHED,
				closedResourceIds, closedResourceIdCount,
				logRecordList, logRecordCount, replStmtId, 0, response);
	}
	bool executeReplication(
			const Request &request, EventContext &ec,
			util::StackAllocator &alloc, const NodeDescriptor &clientND,
			TransactionContext &txn, EventType replStmtType, StatementId replStmtId,
			int32_t replMode, ReplicationContext::TaskStatus taskStatus,
			const ClientId *closedResourceIds, size_t closedResourceIdCount,
			const util::XArray<uint8_t> **logRecordList, size_t logRecordCount,
			StatementId originalStmtId, int32_t delayTime,
			const Response &response);
	void replyReplicationAck(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, const ReplicationAck &request);

	void handleError(
			EventContext &ec, util::StackAllocator &alloc, Event &ev,
			const Request &request, std::exception &e);

	bool abortOnError(TransactionContext &txn, util::XArray<uint8_t> &log);

	static void checkLockConflictStatus(
			const LockConflictStatus &status, LockConflictException &e);
	static void retryLockConflictedRequest(
			EventContext &ec, Event &ev, const LockConflictStatus &status);
	static LockConflictStatus getLockConflictStatus(
			const Event &ev, EventMonotonicTime emNow, const Request &request);
	static LockConflictStatus getLockConflictStatus(
			const Event &ev, EventMonotonicTime emNow,
			const TransactionManager::ContextSource &cxtSrc,
			const OptionSet &optionSet);
	static void updateLockConflictStatus(
			EventContext &ec, Event &ev, const LockConflictStatus &status);

	template<OptionType T>
	static void updateRequestOption(
			util::StackAllocator &alloc, Event &ev,
			const typename Message::OptionCoder<T>::ValueType &value);

	bool checkPrivilege(
			EventType command,
			UserType userType, RequestType requestType, bool isSystemMode,
			ContainerType resourceType, ContainerAttribute resourceSubType,
			ContainerAttribute expectedResourceSubType = CONTAINER_ATTR_ANY);

	bool isMetaContainerVisible(
			const MetaContainer &metaContainer, int32_t visibility);

	static bool isSupportedContainerAttribute(ContainerAttribute attribute);


	static void checkDbAccessible(
			const char8_t *loginDbName, const char8_t *specifiedDbName
	);

	static const char8_t *clusterRoleToStr(ClusterRole role);
	static const char8_t *partitionRoleTypeToStr(PartitionRoleType role);
	static const char8_t *partitionStatusToStr(PartitionStatus status);

protected:
	const KeyConstraint& getKeyConstraint(
			const OptionSet &optionSet, bool checkLength = true) const;
	const KeyConstraint& getKeyConstraint(
			ContainerAttribute containerAttribute, bool checkLength = true) const;

	KeyConstraint keyConstraint_[2][2][2];
};

template <typename IntType>
void StatementHandler::decodeIntData(
	util::ByteStream<util::ArrayInStream> &in, IntType &intData) {
	try {
		in >> intData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename LongType>
void StatementHandler::decodeLongData(
	util::ByteStream<util::ArrayInStream> &in, LongType &longData) {
	try {
		in >> longData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename StringType>
void StatementHandler::decodeStringData(
	util::ByteStream<util::ArrayInStream> &in, StringType &strData) {
	try {
		in >> strData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename EnumType>
void StatementHandler::decodeEnumData(
	util::ByteStream<util::ArrayInStream> &in, EnumType &enumData) {
	try {
		int8_t tmp;
		in >> tmp;
		enumData = static_cast<EnumType>(tmp);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename IntType>
void StatementHandler::encodeIntData(EventByteOutStream &out, IntType intData) {
	try {
		out << intData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename LongType>
void StatementHandler::encodeLongData(
	EventByteOutStream &out, LongType longData) {
	try {
		out << longData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template<typename CharT, typename Traits, typename Alloc>
void StatementHandler::encodeStringData(
		EventByteOutStream &out,
		const std::basic_string<CharT, Traits, Alloc> &strData) {
	try {
		const uint32_t size = static_cast<uint32_t>(strData.size());
		out << size;
		out << std::pair<const uint8_t *, size_t>(
			reinterpret_cast<const uint8_t *>(strData.c_str()), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename EnumType>
void StatementHandler::encodeEnumData(
	EventByteOutStream &out, EnumType enumData) {
	try {
		const uint8_t tmp = static_cast<uint8_t>(enumData);
		out << tmp;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Handles CONNECT statement
*/
class ConnectHandler : public StatementHandler {
public:
	ConnectHandler(ProtocolVersion currentVersion,
		const ProtocolVersion *acceptableProtocolVersons);

	void operator()(EventContext &ec, Event &ev);

private:
	typedef uint32_t OldStatementId;  
	static const OldStatementId UNDEF_OLD_STATEMENTID = UINT32_MAX;

	const ProtocolVersion currentVersion_;
	const ProtocolVersion *acceptableProtocolVersons_;

	struct ConnectRequest {
		ConnectRequest(PartitionId pId, EventType stmtType)
			: pId_(pId),
			  stmtType_(stmtType),
			  oldStmtId_(UNDEF_OLD_STATEMENTID),
			  clientVersion_(PROTOCOL_VERSION_UNDEFINED) {}

		const PartitionId pId_;
		const EventType stmtType_;
		OldStatementId oldStmtId_;
		ProtocolVersion clientVersion_;
	};

	void checkClientVersion(ProtocolVersion clientVersion);

	EventByteOutStream encodeCommonPart(
		Event &ev, OldStatementId stmtId, StatementExecStatus status);

	void replySuccess(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const ConnectRequest &request,
		const Response &response, bool ackWait);

	void replyError(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const ConnectRequest &request,
		const std::exception &e);

	void handleError(EventContext &ec, util::StackAllocator &alloc, Event &ev,
		const ConnectRequest &request, std::exception &e);
};

/*!
	@brief Handles DISCONNECT statement
*/
class DisconnectHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles LOGIN statement
*/
class LoginHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles LOGOUT statement
*/
class LogoutHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_PARTITION_ADDRESS statement
*/
class GetPartitionAddressHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

	static void encodeClusterInfo(
			util::XArrayByteOutStream &out, EventEngine &ee,
			PartitionTable &partitionTable, ContainerHashMode hashMode);
};

/*!
	@brief Handles GET_PARTITION_CONTAINER_NAMES statement
*/
class GetPartitionContainerNamesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_CONTAINER_PROPERTIES statement
*/
class GetContainerPropertiesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

	template<typename C>
	void encodeContainerProps(
			TransactionContext &txn, EventByteOutStream &out, C *container,
			uint32_t propFlags, uint32_t propTypeCount, bool forMeta,
			EventMonotonicTime emNow, util::String &containerNameStr,
			const char8_t *dbName
		, int64_t currentTime
			);


private:
	typedef util::XArray<util::XArray<uint8_t> *> ContainerNameList;

	enum ContainerProperty {
		CONTAINER_PROPERTY_ID,
		CONTAINER_PROPERTY_SCHEMA,
		CONTAINER_PROPERTY_INDEX,
		CONTAINER_PROPERTY_EVENT_NOTIFICATION,
		CONTAINER_PROPERTY_TRIGGER,
		CONTAINER_PROPERTY_ATTRIBUTES,
		CONTAINER_PROPERTY_INDEX_DETAIL,
		CONTAINER_PROPERTY_NULLS_STATISTICS,
		CONTAINER_PROPERTY_PARTITIONING_METADATA = 16
	};

	enum MetaContainerType {
		META_NONE,
		META_FULL
	};

	void encodeResultListHead(EventByteOutStream &out, uint32_t totalCount);
	void encodePropsHead(EventByteOutStream &out, uint32_t propTypeCount);
	void encodeId(
			EventByteOutStream &out, SchemaVersionId schemaVersionId,
			ContainerId containerId, const FullContainerKey &containerKey,
			ContainerId metaContainerId, int8_t metaNamingType);
	void encodeMetaId(
			EventByteOutStream &out, ContainerId metaContainerId,
			int8_t metaNamingType);
	void encodeSchema(EventByteOutStream &out, ContainerType containerType,
			const util::XArray<uint8_t> &serializedCollectionInfo);
	void encodeIndex(
			EventByteOutStream &out, const util::Vector<IndexInfo> &indexInfoList);
	void encodeEventNotification(EventByteOutStream &out,
			const util::XArray<char *> &urlList,
			const util::XArray<uint32_t> &urlLenList);
	void encodeTrigger(EventByteOutStream &out,
			const util::XArray<const uint8_t *> &triggerList);
	void encodeAttributes(
			EventByteOutStream &out, const ContainerAttribute containerAttribute);
	void encodeIndexDetail(
			EventByteOutStream &out, const util::Vector<IndexInfo> &indexInfoList);
	void encodePartitioningMetaData(
			EventByteOutStream &out,
			TransactionContext &txn, EventMonotonicTime emNow,
			BaseContainer &largeContainer,  ContainerAttribute attribute,
			const char *dbName, const char8_t *containerName
			, int64_t currentTime
			);
	void encodeNulls(
			EventByteOutStream &out,
			const util::XArray<uint8_t> &nullsList);
};

/*!
	@brief Handles PUT_CONTAINER statement
*/
class PutContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	void setContainerAttributeForCreate(OptionSet &optionSet);
};

class PutLargeContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	void setContainerAttributeForCreate(OptionSet &optionSet);
};


class CreateLargeIndexHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
	bool checkCreateIndex(IndexInfo &indexInfo,
			ColumnType targetColumnType, ContainerType targetContainerType);

private:
};

/*!
	@brief Handles DROP_CONTAINER statement
*/
class DropContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles GET_CONTAINER statement
*/
class GetContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class CreateDropIndexHandler : public StatementHandler {
public:

protected:
	bool getExecuteFlag(
		EventType eventType, CreateDropIndexMode mode,
		TransactionContext &txn, BaseContainer &container,
		const IndexInfo &info, bool isCaseSensitive);

	bool compareIndexName(util::StackAllocator &alloc,
		const util::String &specifiedName, const util::String &existingName,
		bool isCaseSensitive);
};

/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class CreateIndexHandler : public CreateDropIndexHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class DropIndexHandler : public CreateDropIndexHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles DROP_TRIGGER statement
*/
class CreateDropTriggerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles FLUSH_LOG statement
*/
class FlushLogHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles WRITE_LOG_PERIODICALLY event
*/
class WriteLogPeriodicallyHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles CREATE_TRANSACTIN_CONTEXT statement
*/
class CreateTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles CLOSE_TRANSACTIN_CONTEXT statement
*/
class CloseTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles COMMIT_TRANSACTIN statement
*/
class CommitAbortTransactionHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles PUT_ROW statement
*/
class PutRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles PUT_ROW_SET statement
*/
class PutRowSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles REMOVE_ROW statement
*/
class RemoveRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles UPDATE_ROW_BY_ID statement
*/
class UpdateRowByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles REMOVE_ROW_BY_ID statement
*/
class RemoveRowByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_ROW statement
*/
class GetRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles GET_ROW_SET statement
*/
class GetRowSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_TQL statement
*/
class QueryTqlHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles APPEND_ROW statement
*/
class AppendRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles QUERY_GEOMETRY_RELATED statement
*/
class QueryGeometryRelatedHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_GEOMETRY_WITH_EXCLUSION statement
*/
class QueryGeometryWithExclusionHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles GET_ROW_TIME_RELATED statement
*/
class GetRowTimeRelatedHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles ROW_INTERPOLATE statement
*/
class GetRowInterpolateHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles AGGREGATE statement
*/
class AggregateHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_TIME_RANGE statement
*/
class QueryTimeRangeHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_TIME_SAMPING statement
*/
class QueryTimeSamplingHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles FETCH_RESULT_SET statement
*/
class FetchResultSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles CLOSE_RESULT_SET statement
*/
class CloseResultSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles MULTI_CREATE_TRANSACTION_CONTEXT statement
*/
class MultiCreateTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct SessionCreationEntry {
		SessionCreationEntry() :
			containerAttribute_(CONTAINER_ATTR_ANY),
			containerName_(NULL),
			containerId_(UNDEF_CONTAINERID),
			sessionId_(UNDEF_SESSIONID) {}

		ContainerAttribute containerAttribute_;
		util::XArray<uint8_t> *containerName_;
		ContainerId containerId_;
		SessionId sessionId_;
	};

	bool decodeMultiTransactionCreationEntry(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<SessionCreationEntry> &entryList);
};
/*!
	@brief Handles MULTI_CLOSE_TRANSACTION_CONTEXT statement
*/
class MultiCloseTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct SessionCloseEntry {
		SessionCloseEntry()
			: stmtId_(UNDEF_STATEMENTID),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID) {}

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
	};

	void decodeMultiTransactionCloseEntry(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<SessionCloseEntry> &entryList);
};

/*!
	@brief Handles multi-type statement
*/
class MultiStatementHandler : public StatementHandler {
	TXN_PROTECTED : enum ContainerResult {
		CONTAINER_RESULT_SUCCESS,
		CONTAINER_RESULT_ALREADY_EXECUTED,
		CONTAINER_RESULT_FAIL
	};

	struct Progress {
		util::XArray<ContainerResult> containerResult_;
		util::XArray<uint8_t> lastExceptionData_;
		util::XArray<const util::XArray<uint8_t> *> logRecordList_;
		LockConflictStatus lockConflictStatus_;
		bool lockConflicted_;
		StatementId mputStartStmtId_;
		uint64_t totalRowCount_;

		Progress(
				util::StackAllocator &alloc,
				const LockConflictStatus &lockConflictStatus) :
				containerResult_(alloc),
				lastExceptionData_(alloc),
				logRecordList_(alloc),
				lockConflictStatus_(lockConflictStatus),
				lockConflicted_(false),
				mputStartStmtId_(UNDEF_STATEMENTID),
				totalRowCount_(0) {
		}
	};

	void handleExecuteError(
			util::StackAllocator &alloc, PartitionId pId,
			const ClientId &clientId, const TransactionManager::ContextSource &src,
			Progress &progress, std::exception &e, EventType stmtType,
			const char8_t *executionName);

	void handleWholeError(
			EventContext &ec, util::StackAllocator &alloc,
			const Event &ev, const Request &request, std::exception &e);

	void decodeContainerOptionPart(
			EventByteInStream &in, const FixedRequest &fixedRequest,
			OptionSet &optionSet);
};

/*!
	@brief Handles MULTI_PUT statement
*/
class MultiPutHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct RowSetRequest {
		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		OptionSet option_;

		int32_t schemaIndex_;
		uint64_t rowCount_;
		util::XArray<uint8_t> rowSetData_;

		explicit RowSetRequest(util::StackAllocator &alloc)
			: stmtId_(UNDEF_STATEMENTID),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  option_(alloc),
			  schemaIndex_(-1),
			  rowCount_(0),
			  rowSetData_(alloc) {}
	};

	typedef std::pair<int32_t, ColumnSchemaId> CheckedSchemaId;
	typedef util::SortedList<CheckedSchemaId> CheckedSchemaIdSet;

	void execute(
			EventContext &ec, const Request &request,
			const RowSetRequest &rowSetRequest, const MessageSchema &schema,
			CheckedSchemaIdSet &idSet, Progress &progress,
			PutRowOption putRowOption);

	void checkSchema(TransactionContext &txn, BaseContainer &container,
		const MessageSchema &schema, int32_t localSchemaId,
		CheckedSchemaIdSet &idSet);

	TransactionManager::ContextSource createContextSource(
			const Request &request, const RowSetRequest &rowSetRequest);

	void decodeMultiSchema(
			util::ByteStream<util::ArrayInStream> &in,
			util::XArray<const MessageSchema *> &schemaList);
	void decodeMultiRowSet(
			util::ByteStream<util::ArrayInStream> &in, const Request &request,
			const util::XArray<const MessageSchema *> &schemaList,
			util::XArray<const RowSetRequest *> &rowSetList);
};
/*!
	@brief Handles MULTI_GET statement
*/
class MultiGetHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	typedef util::XArray<RowKeyData *> RowKeyDataList;
	typedef int32_t LocalSchemaId;
	typedef util::Map<ContainerId, LocalSchemaId> SchemaMap;

	enum RowKeyPredicateType { PREDICATE_TYPE_RANGE, PREDICATE_TYPE_DISTINCT };

	struct RowKeyPredicate {
		ColumnType keyType_;
		RowKeyData *startKey_;
		RowKeyData *finishKey_;
		RowKeyDataList *distinctKeys_;
	};

	struct SearchEntry {
		SearchEntry(ContainerId containerId, const FullContainerKey *containerKey,
			const RowKeyPredicate *predicate)
			: stmtId_(0),
			  containerId_(containerId),
			  sessionId_(TXN_EMPTY_CLIENTID.sessionId_),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  containerKey_(containerKey),
			  predicate_(predicate) {}

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		const FullContainerKey *containerKey_;
		const RowKeyPredicate *predicate_;
	};

	uint32_t execute(
			EventContext &ec, const Request &request,
			const SearchEntry &entry, const SchemaMap &schemaMap,
			EventByteOutStream &replyOut, Progress &progress);

	void buildSchemaMap(
			PartitionId pId,
			const util::XArray<SearchEntry> &searchList, SchemaMap &schemaMap,
			EventByteOutStream &out);

	void checkContainerRowKey(
			BaseContainer *container, const RowKeyPredicate &predicate);

	TransactionManager::ContextSource createContextSource(
			const Request &request, const SearchEntry &entry);

	void decodeMultiSearchEntry(
			util::ByteStream<util::ArrayInStream> &in,
			PartitionId pId, DatabaseId loginDbId,
			const char8_t *loginDbName, const char8_t *specifiedDbName,
			UserType userType, RequestType requestType, bool isSystemMode,
			util::XArray<SearchEntry> &searchList);
	RowKeyPredicate decodePredicate(
			util::ByteStream<util::ArrayInStream> &in, util::StackAllocator &alloc);

	void encodeEntry(
			const FullContainerKey &containerKey, ContainerId containerId,
			const SchemaMap &schemaMap, ResultSet &rs, EventByteOutStream &out);
};
/*!
	@brief Handles MULTI_QUERY statement
*/
class MultiQueryHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct QueryRequest {
		explicit QueryRequest(util::StackAllocator &alloc)
			: stmtType_(UNDEF_EVENT_TYPE),
			  stmtId_(UNDEF_STATEMENTID),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID),
			  schemaVersionId_(UNDEF_SCHEMAVERSIONID),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  optionSet_(alloc),
			  isPartial_(false),
			  partialQueryOption_(alloc) {
			query_.ptr_ = NULL;
		}

		EventType stmtType_;

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		SchemaVersionId schemaVersionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		OptionSet optionSet_;

		FetchOption fetchOption_;
		bool isPartial_;
		PartialQueryOption partialQueryOption_;

		union {
			void *ptr_;
			util::String *tqlQuery_;
			GeometryQuery *geometryQuery_;
			RangeQuery *rangeQuery_;
			SamplingQuery *samplingQuery_;
		} query_;
	};

	enum PartialResult { PARTIAL_RESULT_SUCCESS, PARTIAL_RESULT_FAIL };

	typedef util::XArray<const QueryRequest *> QueryRequestList;

	void execute(
			EventContext &ec, const Request &request,
			int32_t &fetchByteSize,
			const QueryRequest &queryRequest, EventByteOutStream &replyOut,
			Progress &progress);

	TransactionManager::ContextSource createContextSource(
			const Request &request, const QueryRequest &queryRequest);

	void decodeMultiQuery(
			util::ByteStream<util::ArrayInStream> &in, const Request &request,
			util::XArray<const QueryRequest *> &queryList);

	void encodeMultiSearchResultHead(
			EventByteOutStream &out, uint32_t queryCount);
	void encodeEmptySearchResult(
			util::StackAllocator &alloc, EventByteOutStream &out);
	void encodeSearchResult(util::StackAllocator &alloc,
			EventByteOutStream &out, const ResultSet &rs);

	const char8_t *getQueryTypeName(EventType queryStmtType);
};

/*!
	@brief Handles REPLICATION_LOG statement requested from another node
*/
class ReplicationLogHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles REPLICATION_ACK statement requested from another node
*/
class ReplicationAckHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles AUTHENTICATION statement requested from another node
*/
class AuthenticationHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles AUTHENTICATION_ACK statement requested from another node
*/
class AuthenticationAckHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

	void authHandleError(
		EventContext &ec, AuthenticationAck &ack, std::exception &e);
};

/*!
	@brief Handles PUT_USER statement
*/
class PutUserHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles DROP_USER statement
*/
class DropUserHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	void executeTQLAndRemoveDatabaseRow(
			EventContext &ec, Event &ev,
			const Request &request, const char8_t *userName,
			util::XArray<const util::XArray<uint8_t> *> &logRecordList);
	void removeUserRow(
			EventContext &ec, Event &ev, const Request &request,
			const char8_t *userName,
			util::XArray<const util::XArray<uint8_t> *> &logRecordList);
};

/*!
	@brief Handles GET_USERS statement
*/
class GetUsersHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles PUT_DATABASE statement
*/
class PutDatabaseHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	void checkDatabaseWithTQL(
			EventContext &ec, const Request &request,
			DatabaseInfo &dbInfo, bool &existFlag);
};

/*!
	@brief Handles DROP_DATABASE statement
*/
class DropDatabaseHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	void executeTQLAndRemoveDatabaseRow(
			EventContext &ec, Event &ev,
			const Request &request, const char8_t *dbName, bool isAdmin,
			util::XArray<const util::XArray<uint8_t> *> &logRecordList);
};

/*!
	@brief Handles GET_DATABASES statement
*/
class GetDatabasesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	void makePublicDatabaseInfoList(
			util::StackAllocator &alloc,
			util::XArray<UserInfo *> &userInfoList,
			util::XArray<DatabaseInfo *> &dbInfoList);
	void executeTQLDatabase(
			EventContext &ec, const Request &request,
			const char8_t *dbName, const char8_t *userName,
			util::XArray<DatabaseInfo *> &dbInfoList);
};

/*!
	@brief Handles PUT_PRIVILEGE statement
*/
class PutPrivilegeHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles DROP_PRIVILEGE statement
*/
class DropPrivilegeHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	bool removeDatabaseRow(
			EventContext &ec, Event &ev,
			const Request &request, DatabaseInfo &dbInfo);
};

/*!
	@brief Handles CHECK_TIMEOUT event
*/
class CheckTimeoutHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
	void checkNoExpireTransaction(util::StackAllocator &alloc, PartitionId pId);

private:
	static const size_t MAX_OUTPUT_COUNT =
		100;  
	static const uint64_t TIMEOUT_CHECK_STATE_TRACE_COUNT =
		100;  

	std::vector<uint64_t> timeoutCheckCount_;

	void checkReplicationTimeout(EventContext &ec);
	void checkAuthenticationTimeout(EventContext &ec);
	void checkTransactionTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);
	void checkRequestTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);
	void checkResultSetTimeout(EventContext &ec);
	void checkKeepaliveTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);

	bool isTransactionTimeoutCheckEnabled(
		PartitionGroupId pgId, util::XArray<bool> &checkPartitionFlagList);
};

/*!
	@brief Handles unknown statement
*/
class UnknownStatementHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles unsupported statement
*/
class UnsupportedStatementHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles ignorable statement
*/
class IgnorableStatementHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles DATA_STORE_PERIODICALLY event
*/
class DataStorePeriodicallyHandler : public StatementHandler {
public:
	DataStorePeriodicallyHandler();
	~DataStorePeriodicallyHandler();

	void operator()(EventContext &ec, Event &ev);

	

	void setConcurrency(int64_t concurrency) {
		concurrency_ = concurrency;
		expiredCounterList_.assign(concurrency, 0);
		startPosList_.assign(concurrency, 0);
	}


private:
	static const uint64_t PERIODICAL_MAX_SCAN_COUNT =
		500;  

	static const uint64_t PERIODICAL_TABLE_SCAN_MAX_COUNT =
		5000;  

	static const uint64_t PERIODICAL_TABLE_SCAN_WAIT_COUNT =
		30;  

	PartitionId *pIdCursor_;
	int64_t currentLimit_;
	int64_t currentPos_;
	int64_t expiredCheckCount_;
	int64_t expiredCounter_;
	int32_t concurrency_;
	std::vector<int64_t> expiredCounterList_;
	std::vector<int64_t> startPosList_;
};

/*!
	@brief Handles ADJUST_STORE_MEMORY_PERIODICALLY event
*/
class AdjustStoreMemoryPeriodicallyHandler : public StatementHandler {
public:
	AdjustStoreMemoryPeriodicallyHandler();
	~AdjustStoreMemoryPeriodicallyHandler();

	void operator()(EventContext &ec, Event &ev);

private:
	PartitionId *pIdCursor_;
};

/*!
	@brief Handles BACK_GROUND event
*/
class BackgroundHandler : public StatementHandler {
public:
	BackgroundHandler();
	~BackgroundHandler();

	void operator()(EventContext &ec, Event &ev);
	int32_t getWaitTime(EventContext &ec, Event &ev, int32_t opeTime);
	uint64_t diffLsn(PartitionId pId);
private:
	LogSequentialNumber *lsnCounter_;
};

/*!
	@brief Handles CONTINUE_CREATE_INDEX/CONTINUE_ALTER_CONTAINER event
*/
class ContinueCreateDDLHandler : public StatementHandler {
public:
	ContinueCreateDDLHandler();
	~ContinueCreateDDLHandler();

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles UPDATE_DATA_STORE_STATUS event
*/
class UpdateDataStoreStatusHandler : public StatementHandler {
public:
	UpdateDataStoreStatusHandler();
	~UpdateDataStoreStatusHandler();

	void operator()(EventContext &ec, Event &ev);
};


/*!
	@brief Handles REMOVE_ROW_BY_ID_SET statement
*/
class RemoveRowSetByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles REMOVE_ROW_BY_ID_SET statement
*/
class UpdateRowSetByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief TransactionService
*/
class TransactionService {
public:
	TransactionService(ConfigTable &config,
		const EventEngine::Config &eeConfig,
		const EventEngine::Source &eeSource, const char *name);
	~TransactionService();

	void initialize(const ManagerSet &mgrSet);

	void start();
	void shutdown();
	void waitForShutdown();

	EventEngine *getEE();

	int32_t getWaitTime(EventContext &ec, Event *ev, int32_t opeTime,
			int64_t &executableCount, int64_t &afterCount,
			BackgroundEventType type);

	void checkNoExpireTransaction(util::StackAllocator &alloc, PartitionId pId);
	void changeTimeoutCheckMode(PartitionId pId,
		PartitionTable::PartitionStatus after, ChangePartitionType changeType,
		bool isToSubMaster, ClusterStats &stats);

	void enableTransactionTimeoutCheck(PartitionId pId);
	void disableTransactionTimeoutCheck(PartitionId pId);
	bool isTransactionTimeoutCheckEnabled(PartitionId pId) const;

	size_t getWorkMemoryByteSizeLimit() const;

	uint64_t getTotalReadOperationCount() const;
	uint64_t getTotalWriteOperationCount() const;

	uint64_t getTotalRowReadCount() const;
	uint64_t getTotalRowWriteCount() const;

	uint64_t getTotalBackgroundOperationCount() const;
	uint64_t getTotalNoExpireOperationCount() const;
	uint64_t getTotalAbortDDLCount() const;

	void incrementReadOperationCount(PartitionId pId);
	void incrementWriteOperationCount(PartitionId pId);

	void incrementBackgroundOperationCount(PartitionId pId);
	void incrementNoExpireOperationCount(PartitionId pId);
	void incrementAbortDDLCount(PartitionId pId);

	void addRowReadCount(PartitionId pId, uint64_t count);
	void addRowWriteCount(PartitionId pId, uint64_t count);

	ResultSetHolderManager& getResultSetHolderManager();

	void requestUpdateDataStoreStatus(const Event::Source &eventSource, Timestamp time, bool force);

private:
	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		StatUpdator();
		TransactionService *service_;
		TransactionManager *manager_;
	} statUpdator_;

	EventEngine::Config eeConfig_;
	const EventEngine::Source eeSource_;
	EventEngine *ee_;

	bool initalized_;

	const PartitionGroupConfig pgConfig_;

	class Config : public ConfigTable::ParamHandler {
	public:
		Config(ConfigTable &configTable);

		size_t getWorkMemoryByteSizeLimit() const;

	private:
		void setUpConfigHandler(ConfigTable &configTable);
		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue &value);

		util::Atomic<size_t> workMemoryByteSizeLimit_;
	} serviceConfig_;

	std::vector< util::Atomic<bool> > enableTxnTimeoutCheck_;

	std::vector<uint64_t> readOperationCount_;
	std::vector<uint64_t> writeOperationCount_;
	std::vector<uint64_t> rowReadCount_;
	std::vector<uint64_t> rowWriteCount_;
	std::vector<uint64_t> backgroundOperationCount_;
	std::vector<uint64_t> noExpireOperationCount_;
	std::vector<uint64_t> abortDDLCount_;

	static const int32_t TXN_TIMEOUT_CHECK_INTERVAL =
		3;  
	static const int32_t CHUNK_EXPIRE_CHECK_INTERVAL =
		1;  
	static const int32_t ADJUST_STORE_MEMORY_CHECK_INTERVAL =
		1; 

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	ConnectHandler connectHandler_;
	DisconnectHandler disconnectHandler_;
	LoginHandler loginHandler_;
	LogoutHandler logoutHandler_;
	GetPartitionAddressHandler getPartitionAddressHandler_;
	GetPartitionContainerNamesHandler getPartitionContainerNamesHandler_;
	GetContainerPropertiesHandler getContainerPropertiesHandler_;
	PutContainerHandler putContainerHandler_;
	DropContainerHandler dropContainerHandler_;
	GetContainerHandler getContainerHandler_;
	CreateIndexHandler createIndexHandler_;
	DropIndexHandler dropIndexHandler_;
	CreateDropTriggerHandler createDropTriggerHandler_;
	FlushLogHandler flushLogHandler_;
	WriteLogPeriodicallyHandler writeLogPeriodicallyHandler_;
	CreateTransactionContextHandler createTransactionContextHandler_;
	CloseTransactionContextHandler closeTransactionContextHandler_;
	CommitAbortTransactionHandler commitAbortTransactionHandler_;
	PutRowHandler putRowHandler_;
	PutRowSetHandler putRowSetHandler_;
	RemoveRowHandler removeRowHandler_;
	UpdateRowByIdHandler updateRowByIdHandler_;
	RemoveRowByIdHandler removeRowByIdHandler_;
	GetRowHandler getRowHandler_;
	GetRowSetHandler getRowSetHandler_;
	QueryTqlHandler queryTqlHandler_;

	AppendRowHandler appendRowHandler_;
	QueryGeometryRelatedHandler queryGeometryRelatedHandler_;
	QueryGeometryWithExclusionHandler queryGeometryWithExclusionHandler_;
	GetRowTimeRelatedHandler getRowTimeRelatedHandler_;
	GetRowInterpolateHandler getRowInterpolateHandler_;
	AggregateHandler aggregateHandler_;
	QueryTimeRangeHandler queryTimeRangeHandler_;
	QueryTimeSamplingHandler queryTimeSamplingHandler_;
	FetchResultSetHandler fetchResultSetHandler_;
	CloseResultSetHandler closeResultSetHandler_;
	MultiCreateTransactionContextHandler multiCreateTransactionContextHandler_;
	MultiCloseTransactionContextHandler multiCloseTransactionContextHandler_;
	MultiPutHandler multiPutHandler_;
	MultiGetHandler multiGetHandler_;
	MultiQueryHandler multiQueryHandler_;
	ReplicationLogHandler replicationLogHandler_;
	ReplicationAckHandler replicationAckHandler_;
	AuthenticationHandler authenticationHandler_;
	AuthenticationAckHandler authenticationAckHandler_;
	PutUserHandler putUserHandler_;
	DropUserHandler dropUserHandler_;
	GetUsersHandler getUsersHandler_;
	PutDatabaseHandler putDatabaseHandler_;
	DropDatabaseHandler dropDatabaseHandler_;
	GetDatabasesHandler getDatabasesHandler_;
	PutPrivilegeHandler putPrivilegeHandler_;
	DropPrivilegeHandler dropPrivilegeHandler_;
	CheckTimeoutHandler checkTimeoutHandler_;

	UnsupportedStatementHandler unsupportedStatementHandler_;
	UnknownStatementHandler unknownStatementHandler_;
	IgnorableStatementHandler ignorableStatementHandler_;

	DataStorePeriodicallyHandler dataStorePeriodicallyHandler_;
	AdjustStoreMemoryPeriodicallyHandler adjustStoreMemoryPeriodicallyHandler_;
	BackgroundHandler backgroundHandler_;
	ContinueCreateDDLHandler createDDLContinueHandler_;

	UpdateDataStoreStatusHandler updateDataStoreStatusHandler_;

	ShortTermSyncHandler shortTermSyncHandler_;
	LongTermSyncHandler longTermSyncHandler_;
	SyncCheckEndHandler syncCheckEndHandler_;
	DropPartitionHandler dropPartitionHandler_;
	ChangePartitionStateHandler changePartitionStateHandler_;
	ChangePartitionTableHandler changePartitionTableHandler_;

	CheckpointOperationHandler checkpointOperationHandler_;

	SyncManager *syncManager_;
	DataStore *dataStore_;
	CheckpointService *checkpointService_;

	RemoveRowSetByIdHandler removeRowSetByIdHandler_;
	UpdateRowSetByIdHandler updateRowSetByIdHandler_;



	ResultSetHolderManager resultSetHolderManager_;
};


template<StatementMessage::OptionType T>
typename StatementMessage::OptionCoder<T>::ValueType
StatementMessage::OptionSet::get() const {
	typedef OptionCoder<T> Coder;
	typedef typename util::IsSame<
			typename Coder::ValueType, typename Coder::StorableType>::Type Same;
	EntryMap::const_iterator it = entryMap_.find(T);

	if (it == entryMap_.end()) {
		return Coder().getDefault();
	}

	return toValue<Coder>(
			it->second.get<typename Coder::StorableType>(), Same());
}

template<StatementMessage::OptionType T>
void StatementMessage::OptionSet::set(
		const typename OptionCoder<T>::ValueType &value) {
	typedef OptionCoder<T> Coder;
	typedef typename util::IsSame<
			typename Coder::ValueType, typename Coder::StorableType>::Type Same;
	ValueStorage storage;
	storage.set(toStorable<Coder>(value, Same()), getAllocator());
	entryMap_[T] = storage;
}

template<StatementMessage::OptionType T>
void StatementHandler::updateRequestOption(
		util::StackAllocator &alloc, Event &ev,
		const typename Message::OptionCoder<T>::ValueType &value) {
	Request request(alloc, getRequestSource(ev));
	util::XArray<uint8_t> remaining(alloc);

	try {
		EventByteInStream in(ev.getInStream());
		request.decode(in);

		decodeBinaryData(in, remaining, true);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}

	try {
		request.optional_.set<T>(value);
		ev.getMessageBuffer().clear();

		EventByteOutStream out(ev.getOutStream());
		request.encode(out);

		encodeBinaryData(out, remaining.data(), remaining.size());
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

inline void TransactionService::incrementReadOperationCount(PartitionId pId) {
	++readOperationCount_[pId];
}

inline void TransactionService::incrementWriteOperationCount(PartitionId pId) {
	++writeOperationCount_[pId];
}

inline void TransactionService::addRowReadCount(
	PartitionId pId, uint64_t count) {
	rowReadCount_[pId] += count;
}

inline void TransactionService::addRowWriteCount(
	PartitionId pId, uint64_t count) {
	rowWriteCount_[pId] += count;
}

inline void TransactionService::incrementBackgroundOperationCount(PartitionId pId) {
	++backgroundOperationCount_[pId];
}
inline void TransactionService::incrementNoExpireOperationCount(PartitionId pId) {
	++noExpireOperationCount_[pId];
}
inline void TransactionService::incrementAbortDDLCount(PartitionId pId) {
	++abortDDLCount_[pId];
}



#endif
