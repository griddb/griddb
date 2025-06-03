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
#ifndef TRANSACTION_STATEMENT_MESSAGE_H_
#define TRANSACTION_STATEMENT_MESSAGE_H_


#include "transaction_manager.h"

/*!
	@brief Exception class to notify encoding/decoding failure
*/
class EncodeDecodeException : public util::Exception {
public:
	explicit EncodeDecodeException(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
		Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~EncodeDecodeException() throw() {}
};

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


typedef StatementId JobExecutionId;
struct SQLTableInfo;

struct StatementMessage {
	typedef int32_t FixedType;
	typedef int16_t OptionCategory;
	typedef int16_t OptionType;

	struct Utils;

	template<EventType T> struct FixedTypeResolver;

	template<typename T, T D, int> struct PrimitiveOptionCoder;
	template<typename T, int> struct FloatingOptionCoder;
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

	enum FeatureVersion {
		FEATURE_V4_0 = 0,
		FEATURE_V4_1 = 1,
		FEATURE_V4_2 = 2,
		FEATURE_V4_3 = 3,
		FEATURE_V4_5 = 4,
		FEATURE_V5_3 = 5,
		FEATURE_V5_5 = 6,
		FEATURE_V5_6 = 7,

		FEATURE_SUPPORTED_MAX = FEATURE_V5_6
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

		static const OptionCategory SERVER_SQL_CLASSIC = 1;
		static const OptionCategory SERVER_SQL = 12;
		static const OptionCategory CLIENT_ADVANCED = 13;
		static const OptionCategory CLIENT_SQL = 14;
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
		static const OptionType APPLICATION_NAME = 11009; 
		static const OptionType STORE_MEMORY_AGING_SWAP_RATE = 11010; 
		static const OptionType TIME_ZONE_OFFSET = 11011;
		static const OptionType AUTHENTICATION_TYPE = 11012; 
		static const OptionType CONNECTION_ROUTE = 11013; 

		static const OptionType CREATE_DROP_INDEX_MODE = 12001;
		static const OptionType RETRY_MODE = 12002;
		static const OptionType DDL_TRANSACTION_MODE = 12003;
		static const OptionType SQL_CASE_SENSITIVITY = 12004;
		static const OptionType INTERVAL_BASE_VALUE = 12005;
		static const OptionType JOB_VERSION = 12006;
		static const OptionType NOSQL_SYNC_ID = 12007;
		static const OptionType COMPOSITE_INDEX = 12008;

		static const OptionType SUB_CONTAINER_VERSION = 13001;
		static const OptionType SUB_CONTAINER_EXPIRABLE = 13002;
		static const OptionType DIST_QUERY = 13003;
		static const OptionType LARGE_INFO = 13004;

		static const OptionType PRAGMA_LIST = 14001;

		static const OptionType LOCK_CONFLICT_START_TIME = 15001;
		static const OptionType SITE_OWNER_ADDRESS = 15002;
		static const OptionType HANDLING_SUSPENDED = 15003;
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
		USER_ADMIN = 0,
		USER_NORMAL = 1,
	};

	enum ContainerVisibility {
		CONTAINER_VISIBILITY_META = 1 << 0,
		CONTAINER_VISIBILITY_INTERNAL_META = 1 << 1
	};

	static const int32_t DEFAULT_TQL_PARTIAL_EXEC_FETCH_BYTE_SIZE =
			1 * 1024 * 1024;

	struct CaseSensitivity;
	struct QueryContainerKey;

	struct UUIDObject;
	struct ExtensionParams;
	struct ExtensionColumnTypes;
	struct IntervalBaseValue;
	struct DistributedQueryInfo;
	struct PragmaList;
	struct CompositeIndexInfos;



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

	template<int C> struct OptionCoder<Options::REPLY_PID, C> :
			public PrimitiveOptionCoder<PartitionId, UNDEF_PARTITIONID, C> {
	};
	template<int C> struct OptionCoder<Options::REPLY_EVENT_TYPE, C> :
			public PrimitiveOptionCoder<EventType, UNDEF_EVENT_TYPE, C> {
	};
	template<int C> struct OptionCoder<Options::UUID, C> :
			public CustomOptionCoder<UUIDObject, C> {
	};
	template<int C> struct OptionCoder<Options::QUERY_ID, C> :
			public PrimitiveOptionCoder<SessionId, 0, C> {
	};
	template<int C> struct OptionCoder<Options::CONTAINER_ID, C> :
			public PrimitiveOptionCoder<int32_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::USER_TYPE, C> :
			public EnumOptionCoder<UserType, USER_ADMIN, C> {
	};
	template<int C> struct OptionCoder<Options::DB_VERSION_ID, C> :
			public PrimitiveOptionCoder<DatabaseId, 0, C> {
	};
	template<int C> struct OptionCoder<Options::EXTENSION_NAME, C> :
			public StringOptionCoder<C> {
	};
	template<int C> struct OptionCoder<Options::EXTENSION_PARAMS, C> :
			public CustomOptionCoder<ExtensionParams*, C> {
	};
	template<int C> struct OptionCoder<Options::INDEX_NAME, C> :
			public StringOptionCoder<C> {
	};
	template<int C> struct OptionCoder<Options::EXTENSION_TYPE_LIST, C> :
			public CustomOptionCoder<ExtensionColumnTypes*, C> {
	};
	template<int C> struct OptionCoder<Options::FOR_SYNC, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::ACK_EVENT_TYPE, C> :
			public PrimitiveOptionCoder<EventType, UNDEF_EVENT_TYPE, C> {
	};
	template<int C> struct OptionCoder<Options::SUB_CONTAINER_ID, C> :
			public PrimitiveOptionCoder<int32_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::JOB_EXEC_ID, C> :
			public PrimitiveOptionCoder<JobExecutionId, 0, C> {
	};

	template<int C> struct OptionCoder<Options::COMPOSITE_INDEX, C> :
			public CustomOptionCoder<CompositeIndexInfos*, C> {
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
	template<int C> struct OptionCoder<Options::APPLICATION_NAME, C> :
			public StringOptionCoder<C> {
	};
	template<int C> struct OptionCoder<Options::STORE_MEMORY_AGING_SWAP_RATE, C> :
			public FloatingOptionCoder<double, C> {
		OptionCoder() :
					FloatingOptionCoder<double, C>(TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE) {}
	};
	template<int C> struct OptionCoder<Options::TIME_ZONE_OFFSET, C> :
			public CustomOptionCoder<util::TimeZone, C> {
	};
	template<int C> struct OptionCoder<Options::AUTHENTICATION_TYPE, C> :
		public PrimitiveOptionCoder<int8_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::CONNECTION_ROUTE, C> :
		public PrimitiveOptionCoder<int8_t, 0, C> {
	};

	template<int C> struct OptionCoder<Options::CREATE_DROP_INDEX_MODE, C> :
			public EnumOptionCoder<CreateDropIndexMode, INDEX_MODE_NOSQL, C> {
	};
	template<int C> struct OptionCoder<Options::RETRY_MODE, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::DDL_TRANSACTION_MODE, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::SQL_CASE_SENSITIVITY, C> :
			public CustomOptionCoder<CaseSensitivity, C> {
	};
	template<int C> struct OptionCoder<Options::INTERVAL_BASE_VALUE, C> :
			public CustomOptionCoder<IntervalBaseValue, C> {
	};
	template<int C> struct OptionCoder<Options::JOB_VERSION, C> :
			public PrimitiveOptionCoder<uint8_t, 0, C> {
	};
	template<int C> struct OptionCoder<Options::NOSQL_SYNC_ID, C> :
			public PrimitiveOptionCoder<int64_t, UNDEF_NOSQL_REQUESTID, C> {
	};

	template<int C> struct OptionCoder<Options::SUB_CONTAINER_VERSION, C> :
			public PrimitiveOptionCoder<int64_t, -1, C> {
	};
	template<int C> struct OptionCoder<Options::SUB_CONTAINER_EXPIRABLE, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};
	template<int C> struct OptionCoder<Options::DIST_QUERY, C> :
			public CustomOptionCoder<QueryContainerKey*, C> {
	};
	template<int C> struct OptionCoder<Options::LARGE_INFO, C> :
			public CustomOptionCoder<SQLTableInfo*, C> {
	};

	template<int C> struct OptionCoder<Options::PRAGMA_LIST, C> :
			public CustomOptionCoder<PragmaList*, C> {
	};

	template<int C> struct OptionCoder<Options::LOCK_CONFLICT_START_TIME, C> :
			public PrimitiveOptionCoder<EventMonotonicTime, -1, C> {
	};

	template<int C> struct OptionCoder<Options::SITE_OWNER_ADDRESS, C> :
		public CustomOptionCoder<NodeAddress, C> {
	};

	template<int C> struct OptionCoder<Options::HANDLING_SUSPENDED, C> :
			public PrimitiveOptionCoder<bool, false, C> {
	};

	static bool isUpdateStatement(EventType stmtType);
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
			T == SQL_GET_CONTAINER ||
			T == PUT_USER ||
			T == DROP_USER ||
			T == GET_USERS ||
			T == PUT_DATABASE ||
			T == DROP_DATABASE ||
			T == GET_DATABASES ||
			T == PUT_PRIVILEGE ||
			T == DROP_PRIVILEGE ||
			T == REPLICATION_LOG2 ||
			T == REPLICATION_ACK2 ||
			T == UPDATE_DATA_STORE_STATUS ||
			T == TXN_SITE_REPLICATION_REDO_ASYNC ||
			T == TXN_SITE_REPLICATION_REDO_SEMISYNC ||
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

			T == CREATE_LARGE_INDEX ||
			T == DROP_LARGE_INDEX ||
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
	template<typename S>
	static void encodeBool(S &out, bool value);
	template<typename S>
	static bool decodeBool(S&in);

	template<typename S>
	static void encodeVarSizeBinary(
		S &out, const util::XArray<uint8_t> &binary);
	template<typename S>
	static void encodeVarSizeBinary(
		S &out, const void *data, size_t size);
	template<typename S>
	static void decodeVarSizeBinary(
		S &in, util::XArray<uint8_t> &binary);
};

template<typename T, T D, int> struct StatementMessage::PrimitiveOptionCoder {
	typedef T ValueType;
	typedef ValueType StorableType;
	ValueType getDefault() const { return D; }
	template<typename S>
	void encode(S&out, const ValueType &value) const {
		out << value;
	}
	template<typename S>
	ValueType decode(S&in, util::StackAllocator&) const {
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
	template<typename S>
	void encode(S &out, const ValueType &value) const {
		Utils::encodeBool(out, value);
	}
	template<typename S>
	ValueType decode(S &in, util::StackAllocator&) const {
		return Utils::decodeBool(in);
	}
};

template<typename T, int> struct StatementMessage::FloatingOptionCoder {
	typedef T ValueType;
	typedef ValueType StorableType;
	explicit FloatingOptionCoder(const T defaultValue) : defaultValue_(defaultValue) {
		UTIL_STATIC_ASSERT((std::numeric_limits<T>::has_infinity));
	}
	ValueType getDefault() const { return defaultValue_; }
	template<typename S>
	void encode(S &out, const ValueType &value) const {
		out << value;
	}
	template<typename S>
	ValueType decode(S&in, util::StackAllocator&) const {
		ValueType value;
		in >> value;
		return value;
	}
	T defaultValue_;
};

template<typename T, T D, int, typename S>
struct StatementMessage::EnumOptionCoder {
	typedef T ValueType;
	typedef S StorableType;
	ValueType getDefault() const { return D; }
	template<typename Stream>
	void encode(Stream &out, const ValueType &value) const {
		const StorableType storableValue = value;
		out << storableValue;
	}
	template<typename Stream>
	ValueType decode(Stream &in, util::StackAllocator&) const {
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
	template<typename S>
	void encode(S &out, const ValueType &value) const {
		assert(value != NULL);
		out << value;
	}
	template<typename S>
	ValueType decode(
		S &in, util::StackAllocator &alloc) const {
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
	template<typename S>
	void encode(S &out, const ValueType &value) const;
	template<typename S>
	ValueType decode(S &in, util::StackAllocator &alloc) const;
};

template<StatementMessage::FixedType>
struct StatementMessage::FixedCoder {
	template<typename S>
	void encode(S &out, const FixedRequest &value) const;
	template<typename S>
	void decode(S &in, FixedRequest &value) const;
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
	template<OptionCategory C, typename S> struct EntryEncoder;
	template<OptionCategory C, typename S> struct EntryDecoder;

	explicit OptionSet(util::StackAllocator &alloc);

	void setLegacyVersionBlock(bool enabled);
	void checkFeature();

	template<typename S>
	void encode(S &out) const;
	template<typename S>
	void decode(S &in);

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


template<StatementMessage::OptionCategory C, typename S>
struct StatementMessage::OptionSet::EntryEncoder {
	EntryEncoder(
		S &out, const OptionSet &optionSet,
			OptionType type);

	template<OptionCategory T> void opCategory();
	template<OptionType T> void opType();

	S &out_;
	const OptionSet &optionSet_;
	OptionType type_;
};

template<StatementMessage::OptionCategory C, typename S>
struct StatementMessage::OptionSet::EntryDecoder {
	EntryDecoder(S &in, OptionSet &optionSet, OptionType type);

	template<OptionCategory T> void opCategory();
	template<OptionType T> void opType();

	S &in_;
	OptionSet &optionSet_;
	OptionType type_;
};

struct StatementMessage::FixedRequest {
	struct Source;
	template<typename S>
	struct Encoder;
	template<typename S>
	struct Decoder;
	struct EventToFixedType;

	explicit FixedRequest(const Source &src);

	template<typename S>
	void encode(S& out) const;
	template<typename S>
	void decode(S& in);

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

template<typename S>
struct StatementMessage::FixedRequest::Encoder {
	Encoder(S &out, const FixedRequest &request);
	template<FixedType T> void opFixedType();

	S &out_;
	const FixedRequest &request_;
};

template<typename S>
struct StatementMessage::FixedRequest::Decoder {
	Decoder(S &in, FixedRequest &request);
	template<FixedType T> void opFixedType();

	S &in_;
	FixedRequest &request_;
};

struct StatementMessage::FixedRequest::EventToFixedType {
	EventToFixedType();
	template<EventType T> void opEventType();

	FixedType fixedType_;
};

struct StatementMessage::Request {
	Request(util::StackAllocator &alloc, const FixedRequest::Source &src);

	template<typename S>
	void encode(S &out) const;
	template<typename S>
	void decode(S &in);

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

struct StatementMessage::UUIDObject {
	UUIDObject();
	explicit UUIDObject(const uint8_t (&uuid)[TXN_CLIENT_UUID_BYTE_SIZE]);

	void get(uint8_t (&uuid)[TXN_CLIENT_UUID_BYTE_SIZE]) const;

	uint8_t uuid_[TXN_CLIENT_UUID_BYTE_SIZE];
};

struct StatementMessage::ExtensionParams {
	explicit ExtensionParams(util::StackAllocator &alloc);

	util::XArray<uint8_t> fixedPart_;
	util::XArray<uint8_t> varPart_;
};

struct StatementMessage::ExtensionColumnTypes {
	explicit ExtensionColumnTypes(util::StackAllocator &alloc);

	util::XArray<ColumnType> columnTypeList_;
};

struct StatementMessage::IntervalBaseValue {
	IntervalBaseValue();

	int64_t baseValue_;
	bool enabled_;
};

struct StatementMessage::PragmaList {
	explicit PragmaList(util::StackAllocator &alloc);

	typedef util::Vector< std::pair<util::String, util::String> > List;

	List list_;
};

struct StatementMessage::CompositeIndexInfos {
	explicit CompositeIndexInfos(util::StackAllocator &alloc);
	bool isExists() {
		return (indexInfoList_.size() > 0);
	}
	void setIndexInfos(util::Vector<IndexInfo> &indexInfoList);
	util::Vector<IndexInfo> indexInfoList_;
};


template<typename S>
inline void StatementMessage::Utils::encodeBool(S& out, bool value) {
	out << static_cast<int8_t>(value);
}

template<typename S>
inline bool StatementMessage::Utils::decodeBool(S& in) {
	int8_t value;
	in >> value;
	return !!value;
}

template<typename S>
inline void StatementMessage::Utils::encodeVarSizeBinary(
	S& out, const util::XArray<uint8_t>& binary) {
	encodeVarSizeBinary(out, binary.data(), binary.size());
}

template<typename S>
inline void StatementMessage::Utils::encodeVarSizeBinary(
	S& out, const void* data, size_t size) {
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

template<typename S>
inline void StatementMessage::Utils::decodeVarSizeBinary(
	S& in, util::XArray<uint8_t>& binary) {
	uint32_t size = ValueProcessor::getVarSize(in);
	binary.resize(size);
	in.readAll(binary.data(), size);
}

template<StatementMessage::FixedType T>
template<typename S>
inline void StatementMessage::FixedCoder<T>::encode(
	S& out, const FixedRequest& value) const {
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
template<typename S>
inline void StatementMessage::FixedCoder<T>::decode(
	S& in, FixedRequest& value) const {
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
inline bool StatementMessage::OptionCategorySwitcher::switchByCategory(
	OptionCategory category, Action& action) const {
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
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::CLIENT_CLASSIC>::switchByType(
		OptionType type, Action& action) const {
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
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::SERVER_SQL_CLASSIC>::switchByType(
		OptionType type, Action& action) const {
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
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::BOTH_CLASSIC>::switchByType(
		OptionType type, Action& action) const {
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
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::CLIENT>::switchByType(
		OptionType type, Action& action) const {
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
	case Options::AUTHENTICATION_TYPE:
		action.template opType<Options::AUTHENTICATION_TYPE>();
		break;
	case Options::CONNECTION_ROUTE:
		action.template opType<Options::CONNECTION_ROUTE>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::SERVER_TXN>::switchByType(
		OptionType type, Action& action) const {
	switch (type) {
	case Options::LOCK_CONFLICT_START_TIME:
		action.template opType<Options::LOCK_CONFLICT_START_TIME>();
		break;
	case Options::SITE_OWNER_ADDRESS:
		action.template opType<Options::SITE_OWNER_ADDRESS>();
		break;
	case Options::HANDLING_SUSPENDED:
		action.template opType<Options::HANDLING_SUSPENDED>();
		break;
	default:
		return false;
	}
	return true;
}

template<>
template<typename Action>
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::SERVER_SQL>::switchByType(
		OptionType type, Action& action) const {
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
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::CLIENT_ADVANCED>::switchByType(
		OptionType type, Action& action) const {
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
inline bool StatementMessage::OptionTypeSwitcher<
	StatementMessage::OptionCategories::CLIENT_SQL>::switchByType(
		OptionType type, Action& action) const {
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
inline bool StatementMessage::FixedTypeSwitcher::switchByFixedType(
	FixedType type, Action& action) const {
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
inline bool StatementMessage::EventTypeSwitcher::switchByEventType(
	EventType type, Action& action) const {
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
	case TXN_SITE_REPLICATION_REDO_ASYNC:
		action.template opEventType<TXN_SITE_REPLICATION_REDO_ASYNC>();
		break;
	case TXN_SITE_REPLICATION_REDO_SEMISYNC:
		action.template opEventType<TXN_SITE_REPLICATION_REDO_SEMISYNC>();
		break;
	default:
		return false;
	}
	return true;
}

inline StatementMessage::OptionSet::OptionSet(util::StackAllocator& alloc) :
	entryMap_(alloc) {
	setLegacyVersionBlock(true);
}

inline void StatementMessage::OptionSet::setLegacyVersionBlock(bool enabled) {
	set<Options::LEGACY_VERSION_BLOCK>((enabled ? 1 : 0));
}

inline void StatementMessage::OptionSet::checkFeature() {
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

template<typename S>
inline void StatementMessage::OptionSet::encode(S& out) const {
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

		EntryEncoder<UNDEF_CATEGORY, S> encoder(out, *this, optionType);
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

template<typename S>
inline void StatementMessage::OptionSet::decode(S& in) {
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

		EntryDecoder<UNDEF_CATEGORY, S> decoder(in, *this, optionType);
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

inline util::StackAllocator& StatementMessage::OptionSet::getAllocator() {
	return *entryMap_.get_allocator().base();
}

inline StatementMessage::OptionCategory StatementMessage::OptionSet::toOptionCategory(
	OptionType optionType) {
	const int32_t base = optionType / Options::TYPE_RANGE_BASE;
	if (base == 0 && optionType >= Options::TYPE_SQL_CLASSIC_START) {
		return OptionCategories::SERVER_SQL_CLASSIC;
	}
	return static_cast<OptionCategory>(base);
}

inline bool StatementMessage::OptionSet::isOptionClassic(OptionType optionType) {
	return (optionType < Options::TYPE_RANGE_START);
}

template<StatementMessage::OptionCategory C, typename S>
inline StatementMessage::OptionSet::EntryEncoder<C, S>::EntryEncoder(
	S& out, const OptionSet& optionSet,
	OptionType type) :
	out_(out), optionSet_(optionSet), type_(type) {
}

template<StatementMessage::OptionCategory C, typename S>
template<StatementMessage::OptionCategory T>
inline void StatementMessage::OptionSet::EntryEncoder<C, S>::opCategory() {
	UTIL_STATIC_ASSERT(C == UNDEF_CATEGORY);
	EntryEncoder<T, S> sub(out_, optionSet_, type_);
	OptionTypeSwitcher<T>().switchByType(type_, sub);
}

template<StatementMessage::OptionCategory C, typename S>
template<StatementMessage::OptionType T>
inline void StatementMessage::OptionSet::EntryEncoder<C, S>::opType() {
	OptionCoder<T>().template encode<S>(out_, optionSet_.get<T>());
}

template<StatementMessage::OptionCategory C, typename S>
StatementMessage::OptionSet::EntryDecoder<C, S>::EntryDecoder(
	S& in, OptionSet& optionSet, OptionType type) :
	in_(in), optionSet_(optionSet), type_(type) {
}

template<StatementMessage::OptionCategory C, typename S>
template<StatementMessage::OptionCategory T>
inline void StatementMessage::OptionSet::EntryDecoder<C, S>::opCategory() {
	UTIL_STATIC_ASSERT(C == UNDEF_CATEGORY);
	EntryDecoder<T, S> sub(in_, optionSet_, type_);
	OptionTypeSwitcher<T>().switchByType(type_, sub);
}

template<StatementMessage::OptionCategory C, typename S>
template<StatementMessage::OptionType T>
inline void StatementMessage::OptionSet::EntryDecoder<C, S>::opType() {
	optionSet_.set<T>(OptionCoder<T>().decode(in_, optionSet_.getAllocator()));
}

inline StatementMessage::FixedRequest::FixedRequest(const Source& src) :
	pId_(src.pId_),
	stmtType_(src.stmtType_),
	clientId_(TXN_EMPTY_CLIENTID),
	cxtSrc_(stmtType_, isUpdateStatement(stmtType_)),
	schemaVersionId_(UNDEF_SCHEMAVERSIONID),
	startStmtId_(UNDEF_STATEMENTID) {
}

template<typename S>
inline void StatementMessage::FixedRequest::encode(S& out) const {
	const FixedType fixedType = toFixedType(stmtType_);
	Encoder<S> encoder(out, *this);
	if (!FixedTypeSwitcher().switchByFixedType(fixedType, encoder)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
			"(type=" << stmtType_ << ")");
	}
}

template<typename S>
inline void StatementMessage::FixedRequest::decode(S& in) {
	const FixedType fixedType = toFixedType(stmtType_);
	Decoder<S> decoder(in, *this);
	if (!FixedTypeSwitcher().switchByFixedType(fixedType, decoder)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
			"(type=" << stmtType_ << ")");
	}
}

inline StatementMessage::FixedType StatementMessage::FixedRequest::toFixedType(
	EventType type) {
	EventToFixedType action;
	if (!EventTypeSwitcher().switchByEventType(type, action)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
			"(type=" << type << ")");
	}
	return action.fixedType_;
}

inline StatementMessage::FixedRequest::Source::Source(
	PartitionId pId, EventType stmtType) :
	pId_(pId),
	stmtType_(stmtType) {
}

template<typename S>
inline StatementMessage::FixedRequest::Encoder<S>::Encoder(
	S& out, const FixedRequest& request) :
	out_(out),
	request_(request) {
}

template<typename S>
template<StatementMessage::FixedType T>
inline void StatementMessage::FixedRequest::Encoder<S>::opFixedType() {
	FixedCoder<T>().encode(out_, request_);
}

template<typename S>
inline StatementMessage::FixedRequest::Decoder<S>::Decoder(
	S& in, FixedRequest& request) :
	in_(in),
	request_(request) {
}

template<typename S>
template<StatementMessage::FixedType T>
inline void StatementMessage::FixedRequest::Decoder<S>::opFixedType() {
	FixedCoder<T>().decode(in_, request_);
}

inline StatementMessage::FixedRequest::EventToFixedType::EventToFixedType() :
	fixedType_(-1) {
}

template<EventType T>
inline void StatementMessage::FixedRequest::EventToFixedType::opEventType() {
	fixedType_ = FixedTypeResolver<T>::FIXED_TYPE;
}

inline StatementMessage::Request::Request(
	util::StackAllocator& alloc, const FixedRequest::Source& src) :
	fixed_(src),
	optional_(alloc) {
}

template<typename S>
inline void StatementMessage::Request::encode(S& out) const {
	fixed_.encode(out);
	optional_.encode(out);
}

template<typename S>
inline void StatementMessage::Request::decode(S& in) {
	fixed_.decode(in);
	optional_.decode(in);
}

inline StatementMessage::QueryContainerKey::QueryContainerKey(
	util::StackAllocator& alloc) :
	containerKeyBinary_(alloc),
	enabled_(false) {
}

inline StatementMessage::UUIDObject::UUIDObject() {
	memset(uuid_, 0, TXN_CLIENT_UUID_BYTE_SIZE);
}

inline StatementMessage::UUIDObject::UUIDObject(
	const uint8_t(&uuid)[TXN_CLIENT_UUID_BYTE_SIZE]) {
	memcpy(uuid_, uuid, sizeof(uuid));
}

inline void StatementMessage::UUIDObject::get(
	uint8_t(&uuid)[TXN_CLIENT_UUID_BYTE_SIZE]) const {
	memcpy(uuid, uuid_, sizeof(uuid));
}

inline StatementMessage::ExtensionParams::ExtensionParams(
	util::StackAllocator& alloc) :
	fixedPart_(alloc),
	varPart_(alloc) {
}

inline StatementMessage::ExtensionColumnTypes::ExtensionColumnTypes(
	util::StackAllocator& alloc) :
	columnTypeList_(alloc) {
}

inline StatementMessage::IntervalBaseValue::IntervalBaseValue() :
	baseValue_(0),
	enabled_(false) {
}

inline StatementMessage::PragmaList::PragmaList(util::StackAllocator& alloc) :
	list_(alloc) {
}

inline StatementMessage::CompositeIndexInfos::CompositeIndexInfos(
	util::StackAllocator& alloc) :
	indexInfoList_(alloc) {
}

inline void StatementMessage::CompositeIndexInfos::setIndexInfos(
	util::Vector<IndexInfo>& indexInfoList) {
	for (size_t pos = 0; pos < indexInfoList.size(); pos++) {
		IndexInfo& indexInfo = indexInfoList[pos];
		if (indexInfo.isComposite()) {
			indexInfoList_.push_back(indexInfo);
		}
	}
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::UUIDObject, 0>::encode(
		S& out, const ValueType& value) const {
	out.writeAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
}

template<>
template<typename S>
inline StatementMessage::UUIDObject
StatementMessage::CustomOptionCoder<
	StatementMessage::UUIDObject, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	static_cast<void>(alloc);
	UUIDObject value;
	in.readAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	return value;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::ExtensionParams*, 0>::encode(
		S& out, const ValueType& value) const {
	assert(value != NULL);
	for (size_t i = 0; i < 2; i++) {
		const util::XArray<uint8_t>& part =
			(i == 0 ? value->fixedPart_ : value->varPart_);
		const size_t size = part.size();
		out << static_cast<uint64_t>(size);
		out.writeAll(part.data(), size);
	}
}

template<>
template<typename S>
inline StatementMessage::ExtensionParams*
StatementMessage::CustomOptionCoder<
	StatementMessage::ExtensionParams*, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	ExtensionParams* value = ALLOC_NEW(alloc) ExtensionParams(alloc);
	for (size_t i = 0; i < 2; i++) {
		util::XArray<uint8_t>& part =
			(i == 0 ? value->fixedPart_ : value->varPart_);
		uint64_t size;
		in >> size;
		part.resize(static_cast<size_t>(size));
		in.readAll(part.data(), static_cast<size_t>(size));
	}
	return value;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::ExtensionColumnTypes*, 0>::encode(
		S& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->columnTypeList_.size();
	out << static_cast<uint64_t>(size);
	out.writeAll(value->columnTypeList_.data(), size);
}

template<>
template<typename S>
inline StatementMessage::ExtensionColumnTypes*
StatementMessage::CustomOptionCoder<
	StatementMessage::ExtensionColumnTypes*, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	ExtensionColumnTypes* value = ALLOC_NEW(alloc) ExtensionColumnTypes(alloc);
	uint64_t size;
	in >> size;
	value->columnTypeList_.resize(static_cast<size_t>(size));
	in.readAll(
		value->columnTypeList_.data(),
		sizeof(ColumnType) * static_cast<size_t>(size));
	return value;
}



template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<ClientId, 0>::encode(
	S& out, const ValueType& value) const {
	out << value.sessionId_;
	out.writeAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
}

template<>
template<typename S>
inline ClientId StatementMessage::CustomOptionCoder<ClientId, 0>::decode(
	S& in, util::StackAllocator& alloc) const {
	static_cast<void>(alloc);
	ClientId value;
	in >> value.sessionId_;
	in.readAll(value.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	return value;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::QueryContainerKey*, 0>::encode(
		S& out, const ValueType& value) const {
	Utils::encodeBool(out, value->enabled_);
	Utils::encodeVarSizeBinary(out, value->containerKeyBinary_);
}

template<>
template<typename S>
inline StatementMessage::QueryContainerKey*
StatementMessage::CustomOptionCoder<
	StatementMessage::QueryContainerKey*, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	QueryContainerKey* value = ALLOC_NEW(alloc) QueryContainerKey(alloc);
	value->enabled_ = Utils::decodeBool(in);
	Utils::decodeVarSizeBinary(in, value->containerKeyBinary_);
	return value;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<util::TimeZone, 0>::encode(
		S& out, const ValueType& value) const {
	out << value.getOffsetMillis();
}

template<>
template<typename S>
inline util::TimeZone StatementMessage::CustomOptionCoder<util::TimeZone, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	UNUSED_VARIABLE(alloc);

	util::TimeZone::Offset offsetMillis;
	in >> offsetMillis;

	util::TimeZone zone;
	zone.setOffsetMillis(offsetMillis);

	return zone;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CaseSensitivity, 0>::encode(
		S& out, const ValueType& value) const {
	out << value.flags_;
}

template<>
template<typename S>
inline StatementMessage::CaseSensitivity
StatementMessage::CustomOptionCoder<
	StatementMessage::CaseSensitivity, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	static_cast<void>(alloc);
	CaseSensitivity value;
	in >> value.flags_;
	return value;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::IntervalBaseValue, 0>::encode(
		S& out, const ValueType& value) const {
	assert(value.enabled_);
	out << value.baseValue_;
}

template<>
template<typename S>
inline StatementMessage::IntervalBaseValue
StatementMessage::CustomOptionCoder<
	StatementMessage::IntervalBaseValue, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	static_cast<void>(alloc);
	IntervalBaseValue value;
	in >> value.baseValue_;
	value.enabled_ = true;
	return value;
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<SQLTableInfo*, 0>::encode(
		S& out, const ValueType& value) const {
	UNUSED_VARIABLE(out);
	UNUSED_VARIABLE(value);

	TXN_THROW_ENCODE_ERROR(GS_ERROR_TXN_ENCODE_FAILED, "");
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::PragmaList*, 0>::encode(
		S& out, const ValueType& value) const {
	const int32_t listSize = static_cast<int32_t>(value->list_.size());
	out << listSize;
	for (int32_t pos = 0; pos < listSize; pos++) {
		out << value->list_[pos].first;
		out << value->list_[pos].second;
	}
}

template<>
template<typename S>
inline StatementMessage::PragmaList*
StatementMessage::CustomOptionCoder<
	StatementMessage::PragmaList*, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	PragmaList* value = ALLOC_NEW(alloc) PragmaList(alloc);
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

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<NodeAddress, 0>::encode(
	S& out, const ValueType& value) const {
	out << value.address_;
	out << value.port_;
}

template<>
template<typename S>
inline NodeAddress StatementMessage::CustomOptionCoder<NodeAddress, 0>::decode(
	S& in, util::StackAllocator& alloc) const {
	UNUSED_VARIABLE(alloc);

	NodeAddress nodeAddress;
	in >> nodeAddress.address_;
	in >> nodeAddress.port_;
	return nodeAddress;
}



/*!
	@brief Checks if UPDATE statement is requested
*/
inline bool StatementMessage::isUpdateStatement(EventType stmtType) {
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

#endif
