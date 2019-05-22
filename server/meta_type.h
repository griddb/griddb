/*
	Copyright (c) 2018 TOSHIBA Digital Solutions Corporation

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
	@brief Definition of metadata type
*/
#ifndef META_TYPE_H_
#define META_TYPE_H_

#include "util/code.h"
#include "data_type.h"

typedef ContainerId MetaContainerId;

struct MetaColumnInfo {
	struct NameInfo {
		NameInfo();

		const char8_t *forContainer_;
		const char8_t *forTable_;
	};

	MetaColumnInfo();

	ColumnId id_;
	ColumnId refId_;
	uint8_t type_;
	bool nullable_;
	NameInfo name_;
};

struct MetaContainerInfo {
	struct NameInfo {
		NameInfo();

		const char8_t *neutral_;
		const char8_t *forContainer_;
		const char8_t *forTable_;
	};

	struct CommonColumnInfo {
		CommonColumnInfo();

		ColumnId dbNameColumn_;
		ColumnId containerNameColumn_;
		ColumnId partitionIndexColumn_;
		ColumnId containerIdColumn_;
	};

	MetaContainerInfo();
	bool isEmpty() const;

	MetaContainerId id_;
	MetaContainerId refId_;
	SchemaVersionId versionId_;
	bool forCore_;
	bool internal_;
	bool adminOnly_;
	bool nodeDistribution_;
	NameInfo name_;
	const MetaColumnInfo *columnList_;
	size_t columnCount_;
	CommonColumnInfo commonInfo_;
};

struct MetaType {
	struct Coders;
	struct CoreColumns;
	struct RefColumns;
	struct Containers;
	class InfoTable;

	enum MetaContainerType {
		TYPE_CONTAINER,
		TYPE_COLUMN,
		TYPE_KEY,
		TYPE_INDEX,
		TYPE_TRIGGER,
		TYPE_ERASABLE,
		TYPE_EVENT,
		TYPE_SOCKET,
		END_TYPE

	};

	enum NamingType {
		NAMING_NEUTRAL,
		NAMING_CONTAINER,
		NAMING_TABLE,
		END_NAMING
	};

	enum CommonMetaType {
		COMMON_DATABASE_NAME,
		COMMON_CONTAINER_NAME,
		COMMON_PARTITION_INDEX,
		COMMON_CONTAINER_ID,
		END_COMMON
	};

	enum ContainerMeta {
		CONTAINER_DATABASE_ID,
		CONTAINER_DATABASE_NAME,
		CONTAINER_ATTRIBUTE,
		CONTAINER_TYPE_NAME,
		CONTAINER_NAME,
		CONTAINER_DATA_AFFINITY,
		CONTAINER_EXPIRATION_TIME,
		CONTAINER_EXPIRATION_UNIT,
		CONTAINER_EXPIRATION_DIVISION,
		CONTAINER_COMPRESSION_METHOD,
		CONTAINER_COMPRESSION_SIZE,
		CONTAINER_COMPRESSION_UNIT,
		CONTAINER_CLUSTER_PARTITION,
		CONTAINER_EXPIRATION_TYPE,
		END_CONTAINER
	};

	enum ColumnMeta {
		COLUMN_DATABASE_ID,
		COLUMN_DATABASE_NAME,
		COLUMN_CONTAINER_ATTRIBUTE,
		COLUMN_CONTAINER_NAME,
		COLUMN_ORDINAL,
		COLUMN_TYPE_NAME,
		COLUMN_NAME,
		COLUMN_KEY,
		COLUMN_NULLABLE,
		COLUMN_KEY_SEQUENCE,
		COLUMN_COMPRESSION_RELATIVE,
		COLUMN_COMPRESSION_RATE,
		COLUMN_COMPRESSION_SPAN,
		COLUMN_COMPRESSION_WIDTH,
		END_COLUMN
	};

	enum IndexMeta {
		INDEX_DATABASE_ID,
		INDEX_DATABASE_NAME,
		INDEX_CONTAINER_NAME,
		INDEX_NAME,
		INDEX_ORDINAL,
		INDEX_COLUMN_NAME,
		INDEX_TYPE,
		INDEX_TYPE_NAME,
		END_INDEX
	};

	enum TriggerMeta {
		TRIGGER_DATABASE_ID,
		TRIGGER_DATABASE_NAME,
		TRIGGER_CONTAINER_NAME,
		TRIGGER_ORDINAL,
		TRIGGER_NAME,
		TRIGGER_EVENT_TYPE,
		TRIGGER_COLUMN_NAME,
		TRIGGER_TYPE,
		TRIGGER_URI,
		TRIGGER_JMS_DESTINATION_TYPE,
		TRIGGER_JMS_DESTINATION_NAME,
		TRIGGER_USER,
		TRIGGER_PASSWORD,
		END_TRIGGER
	};

	enum ErasableMeta {
		ERASABLE_DATABASE_ID,
		ERASABLE_DATABASE_NAME,
		ERASABLE_TYPE_NAME,
		ERASABLE_CONTAINER_ID,
		ERASABLE_CONTAINER_NAME,
		ERASABLE_PARTITION_NAME,
		ERASABLE_CLUSTER_PARTITION,
		ERASABLE_LARGE_CONTAINER_ID,
		ERASABLE_SCHEMA_VERSION_ID,
		ERASABLE_INIT_SCHEMA_STATUS,
		ERASABLE_EXPIRATION_TYPE,
		ERASABLE_LOWER_BOUNDARY_TIME,
		ERASABLE_UPPER_BOUNDARY_TIME,
		ERASABLE_EXPIRATION_TIME,
		ERASABLE_ERASABLE_TIME,
		ERASABLE_ROW_INDEX_OID,
		ERASABLE_MVCC_INDEX_OID,
		END_ERASABLE
	};

	enum EventMeta {
		EVENT_NODE_ADDRESS,
		EVENT_NODE_PORT,
		EVENT_START_TIME,
		EVENT_APPLICATION_NAME,
		EVENT_SERVICE_TYPE,
		EVENT_EVENT_TYPE,
		EVENT_WORKER_INDEX,
		EVENT_CLUSTER_PARTITION_INDEX,
		END_EVENT
	};

	enum SocketMeta {
		SOCKET_SERVICE_TYPE,
		SOCKET_TYPE,
		SOCKET_NODE_ADDRESS,
		SOCKET_NODE_PORT,
		SOCKET_REMOTE_ADDRESS,
		SOCKET_REMOTE_PORT,
		SOCKET_APPLICATION_NAME,
		SOCKET_CREATION_TIME,
		SOCKET_DISPATCHING_EVENT_COUNT,
		SOCKET_SENDING_EVENT_COUNT,
		END_SOCKET,
	};



	enum StringConstants {
		STR_CONTAINER_NAME,
		STR_CONTAINER_OPTIONAL_TYPE,
		STR_TABLE_NAME,
		STR_TABLE_OPTIONAL_TYPE,

		STR_KEY_SEQ,
		STR_COLUMN_NAME,
		STR_NULLABLE,
		STR_ORDINAL_POSITION,
		STR_TYPE_NAME,
		STR_INDEX_NAME,

		STR_DATABASE_NAME,
		STR_DATA_AFFINITY,
		STR_EXPIRATION_TIME,
		STR_EXPIRATION_TIME_UNIT,
		STR_EXPIRATION_DIVISION_COUNT,
		STR_COMPRESSION_METHOD,
		STR_COMPRESSION_WINDOW_SIZE,
		STR_COMPRESSION_WINDOW_SIZE_UNIT,
		STR_CLUSTER_PARTITION_INDEX,
		STR_EXPIRATION_TYPE,
		STR_PARTITION_SEQ,
		STR_PARTITION_NAME,
		STR_PARTITION_BOUNDARY_VALUE,
		STR_SUBPARTITION_BOUNDARY_VALUE,
		STR_PARTITION_NODE_AFFINITY,
		STR_DATABASE_MAJOR_VERSION,
		STR_DATABASE_MINOR_VERSION,
		STR_EXTRA_NAME_CHARACTERS,
		STR_COMPRESSION_RELATIVE,
		STR_COMPRESSION_RATE,
		STR_COMPRESSION_SPAN,
		STR_COMPRESSION_WIDTH,
		STR_INDEX_TYPE,
		STR_TRIGGER_NAME,
		STR_EVENT_TYPE,
		STR_TRIGGER_TYPE,
		STR_URI,
		STR_JMS_DESTINATION_TYPE,
		STR_JMS_DESTINATION_NAME,
		STR_USER,
		STR_PASSWORD,
		STR_ATTRIBUTE,
		STR_DATABASE_ID,
		STR_CONTAINER_ID,
		STR_LARGE_CONTAINER_ID,
		STR_SCHEMA_VERSION_ID,
		STR_INIT_SCHEMA_STATUS,
		STR_LOWER_BOUNDARY_TIME,
		STR_UPPER_BOUNDARY_TIME,
		STR_ERASABLE_TIME,
		STR_ROW_INDEX_OID,
		STR_MVCC_INDEX_OID,
		STR_NODE_ADDRESS,

		STR_NODE_PORT,
		STR_START_TIME,
		STR_APPLICATION_NAME,
		STR_SERVICE_TYPE,
		STR_WORKER_INDEX,

		STR_SOCKET_TYPE,
		STR_REMOTE_ADDRESS,
		STR_REMOTE_PORT,
		STR_CREATION_TIME,
		STR_DISPATCHING_EVENT_COUNT,
		STR_SENDING_EVENT_COUNT,


		END_STR
	};
};

struct MetaType::Coders {
	static const util::NameCoderEntry<ContainerMeta> LIST_CONTAINER[];
	static const util::NameCoderEntry<ColumnMeta> LIST_COLUMN[];
	static const util::NameCoderEntry<IndexMeta> LIST_INDEX[];
	static const util::NameCoderEntry<TriggerMeta> LIST_TRIGGER[];
	static const util::NameCoderEntry<ErasableMeta> LIST_ERASABLE[];
	static const util::NameCoderEntry<EventMeta> LIST_EVENT[];
	static const util::NameCoderEntry<SocketMeta> LIST_SOCKET[];

	static const util::NameCoder<ContainerMeta, END_CONTAINER> CODER_CONTAINER;
	static const util::NameCoder<ColumnMeta, END_COLUMN> CODER_COLUMN;
	static const util::NameCoder<IndexMeta, END_INDEX> CODER_INDEX;
	static const util::NameCoder<TriggerMeta, END_TRIGGER> CODER_TRIGGER;
	static const util::NameCoder<ErasableMeta, END_ERASABLE> CODER_ERASABLE;
	static const util::NameCoder<EventMeta, END_EVENT> CODER_EVENT;
	static const util::NameCoder<SocketMeta, END_SOCKET> CODER_SOCKET;

	static const util::NameCoderEntry<StringConstants> LIST_STR[];
	static const util::NameCoder<StringConstants, END_STR> CODER_STR;
};

struct MetaType::CoreColumns {
	template<typename T> struct Entry {
		explicit Entry(T id);
		Entry asType(uint8_t type, bool nullable) const;
		Entry asCommon(CommonMetaType common) const;

		Entry asString(bool nullable = false) const;
		Entry asBool(bool nullable = false) const;
		Entry asShort(bool nullable = false) const;
		Entry asInteger(bool nullable = false) const;
		Entry asLong(bool nullable = false) const;
		Entry asDouble(bool nullable = false) const;
		Entry asTimestamp(bool nullable = false) const;

		Entry asDbName() const;
		Entry asContainerName() const;
		Entry asPartitionIndex() const;
		Entry asContainerId() const;

		T id_;
		uint8_t type_;
		bool nullable_;
		CommonMetaType common_;
	};

	static const Entry<ContainerMeta> COLUMNS_CONTAINER[];
	static const Entry<ColumnMeta> COLUMNS_COLUMN[];
	static const Entry<IndexMeta> COLUMNS_INDEX[];
	static const Entry<TriggerMeta> COLUMNS_TRIGGER[];
	static const Entry<ErasableMeta> COLUMNS_ERASABLE[];
	static const Entry<EventMeta> COLUMNS_EVENT[]; 
	static const Entry<SocketMeta> COLUMNS_SOCKET[]; 

	template<typename T>
	static Entry<T> of(T id);
};

struct MetaType::RefColumns {
	template<typename T> struct Entry {
		Entry(T refId, StringConstants name, StringConstants nameForTable);

		T refId_;
		StringConstants nameForContainer_;
		StringConstants nameForTable_;
	};

	static const Entry<ContainerMeta> COLUMNS_CONTAINER[];
	static const Entry<ColumnMeta> COLUMNS_COLUMN[];
	static const Entry<ColumnMeta> COLUMNS_KEY[];
	static const Entry<IndexMeta> COLUMNS_INDEX[];
	static const Entry<TriggerMeta> COLUMNS_TRIGGER[];
	static const Entry<ErasableMeta> COLUMNS_ERASABLE[];
	static const Entry<EventMeta> COLUMNS_EVENT[]; 
	static const Entry<SocketMeta> COLUMNS_SOCKET[]; 

	template<typename T>
	static Entry<T> of(
			T refId,
			StringConstants nameForContainer,
			StringConstants nameForTable = END_STR);
};

struct MetaType::Containers {
public:
	static const size_t TYPE_COUNT = END_TYPE;

	static const MetaContainerInfo CONTAINERS_CORE[TYPE_COUNT];
	static const MetaContainerInfo CONTAINERS_REF[TYPE_COUNT];

	static MetaContainerId typeToId(MetaContainerType type);
	static bool idToIndex(MetaContainerId id, size_t &index);

private:
	template<typename T, size_t N>
	static MetaContainerInfo coreOf(
			MetaContainerType type, const char8_t *name,
			const CoreColumns::Entry<T> (&columnList)[N],
			const util::NameCoder<T, N> &coder, uint16_t version);
	template<typename T, size_t N>
	static MetaContainerInfo refOf(
			MetaContainerType type, const char8_t *name,
			const char8_t *nameForContainer, const char8_t *nameForTable,
			const RefColumns::Entry<T> (&columnList)[N], uint16_t version,
			MetaContainerType refType = END_TYPE);
	static MetaContainerInfo noneOf(MetaContainerType type);

	static MetaContainerInfo toInternal(const MetaContainerInfo &src);
	static MetaContainerInfo toNodeDistribution(const MetaContainerInfo &src);

	static void setUpCoreColumnInfo(
			size_t index, ColumnId id, uint8_t type, bool nullable,
			const char8_t *name, MetaColumnInfo &info);
	static void setUpRefColumnInfo(
			MetaContainerType type, size_t index, ColumnId refId,
			StringConstants nameForContainer, StringConstants nameForTable,
			MetaColumnInfo &info);

	static void setUpCommonColumnInfo(
			ColumnId id, CommonMetaType type,
			MetaContainerInfo::CommonColumnInfo &commonInfo);

	static SchemaVersionId makeSchemaVersionId(
			uint16_t base, size_t columnCount);
};

class MetaType::InfoTable {
public:
	static const InfoTable& getInstance();

	const MetaContainerInfo& resolveInfo(
			MetaContainerId id, bool forCore) const;

	const MetaContainerInfo* findInfo(
			const char8_t *name, bool forCore, NamingType &namingType) const;
	const MetaContainerInfo* findInfo(MetaContainerId id, bool forCore) const;

	static NamingType resolveNaming(
			NamingType specifiedType, NamingType defaultType);

	static const char8_t* nameOf(
			const MetaColumnInfo &columnInfo, NamingType namingType,
			bool exact);
	static const char8_t* nameOf(
			const MetaContainerInfo &containerInfo, NamingType namingType,
			bool exact);

private:
	typedef std::pair<uint64_t, NamingType> EntryValue;
	typedef std::pair<const char8_t*, EntryValue> NameEntry;

	struct EntryPred {
		bool operator()(
				const NameEntry &entry1, const NameEntry &entry2) const;
	};

	static const InfoTable TABLE_INSTANCE;

	InfoTable();

	InfoTable(const InfoTable &another);
	InfoTable& operator=(const InfoTable &another);

	static const MetaContainerInfo* findInfoByIndex(
			size_t index, bool forCore);

	template<size_t N, size_t M>
	static void setUpNameEntries(
			const MetaContainerInfo (&infoList)[N], NameEntry (&entryList)[M]);

	template<size_t M>
	static size_t getNameEntryCount(const NameEntry (&)[M]);

	NameEntry coreList_[Containers::TYPE_COUNT];
	NameEntry refList_[Containers::TYPE_COUNT * END_NAMING];
};

#endif
