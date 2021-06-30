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
		FEATURE_V4_5 = 4,//

		FEATURE_SUPPORTED_MAX = FEATURE_V4_5//
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

template<typename T, int> struct StatementMessage::FloatingOptionCoder {
	typedef T ValueType;
	typedef ValueType StorableType;
	explicit FloatingOptionCoder(const T defaultValue) : defaultValue_(defaultValue) {
		UTIL_STATIC_ASSERT((std::numeric_limits<T>::has_infinity));
	}
	ValueType getDefault() const { return defaultValue_; }
	void encode(EventByteOutStream &out, const ValueType &value) const {
		out << value;
	}
	ValueType decode(EventByteInStream &in, util::StackAllocator&) const {
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


#endif
