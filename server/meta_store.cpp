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
	@brief Implementation of metadata store
*/

#include "meta_store.h"

#include "time_series.h"
#include "trigger_service.h"
#include "lexer.h"

#include "result_set.h"


MetaColumnInfo::MetaColumnInfo() :
		id_(UNDEF_COLUMNID),
		refId_(UNDEF_COLUMNID),
		type_(COLUMN_TYPE_NULL),
		nullable_(false) {
}


MetaColumnInfo::NameInfo::NameInfo() :
		forContainer_(NULL),
		forTable_(NULL) {
}


MetaContainerInfo::MetaContainerInfo() :
		id_(UNDEF_CONTAINERID),
		refId_(UNDEF_CONTAINERID),
		versionId_(UNDEF_SCHEMAVERSIONID),
		forCore_(false),
		internal_(false),
		adminOnly_(false),
		columnList_(NULL),
		columnCount_(0) {
}

bool MetaContainerInfo::isEmpty() const {
	return (columnCount_ == 0);
}


MetaContainerInfo::NameInfo::NameInfo() :
		neutral_(NULL),
		forContainer_(NULL),
		forTable_(NULL) {
}


MetaContainerInfo::CommonColumnInfo::CommonColumnInfo() :
		dbNameColumn_(UNDEF_COLUMNID),
		containerNameColumn_(UNDEF_COLUMNID),
		partitionIndexColumn_(UNDEF_COLUMNID),
		containerIdColumn_(UNDEF_COLUMNID) {
}


const util::NameCoderEntry<MetaType::ContainerMeta>
		MetaType::Coders::LIST_CONTAINER[] = {
	UTIL_NAME_CODER_ENTRY(CONTAINER_DATABASE_ID),
	UTIL_NAME_CODER_ENTRY(CONTAINER_DATABASE_NAME),
	UTIL_NAME_CODER_ENTRY(CONTAINER_ATTRIBUTE),
	UTIL_NAME_CODER_ENTRY(CONTAINER_TYPE_NAME),
	UTIL_NAME_CODER_ENTRY(CONTAINER_NAME),
	UTIL_NAME_CODER_ENTRY(CONTAINER_DATA_AFFINITY),
	UTIL_NAME_CODER_ENTRY(CONTAINER_EXPIRATION_TIME),
	UTIL_NAME_CODER_ENTRY(CONTAINER_EXPIRATION_UNIT),
	UTIL_NAME_CODER_ENTRY(CONTAINER_EXPIRATION_DIVISION),
	UTIL_NAME_CODER_ENTRY(CONTAINER_COMPRESSION_METHOD),
	UTIL_NAME_CODER_ENTRY(CONTAINER_COMPRESSION_SIZE),
	UTIL_NAME_CODER_ENTRY(CONTAINER_COMPRESSION_UNIT),
	UTIL_NAME_CODER_ENTRY(CONTAINER_CLUSTER_PARTITION),
	UTIL_NAME_CODER_ENTRY(CONTAINER_EXPIRATION_TYPE)
};
const util::NameCoderEntry<MetaType::ColumnMeta>
		MetaType::Coders::LIST_COLUMN[] = {
	UTIL_NAME_CODER_ENTRY(COLUMN_DATABASE_ID),
	UTIL_NAME_CODER_ENTRY(COLUMN_DATABASE_NAME),
	UTIL_NAME_CODER_ENTRY(COLUMN_CONTAINER_ATTRIBUTE),
	UTIL_NAME_CODER_ENTRY(COLUMN_CONTAINER_NAME),
	UTIL_NAME_CODER_ENTRY(COLUMN_ORDINAL),
	UTIL_NAME_CODER_ENTRY(COLUMN_TYPE_NAME),
	UTIL_NAME_CODER_ENTRY(COLUMN_NAME),
	UTIL_NAME_CODER_ENTRY(COLUMN_KEY),
	UTIL_NAME_CODER_ENTRY(COLUMN_NULLABLE),
	UTIL_NAME_CODER_ENTRY(COLUMN_KEY_SEQUENCE),
	UTIL_NAME_CODER_ENTRY(COLUMN_COMPRESSION_RELATIVE),
	UTIL_NAME_CODER_ENTRY(COLUMN_COMPRESSION_RATE),
	UTIL_NAME_CODER_ENTRY(COLUMN_COMPRESSION_SPAN),
	UTIL_NAME_CODER_ENTRY(COLUMN_COMPRESSION_WIDTH)
};
const util::NameCoderEntry<MetaType::IndexMeta>
		MetaType::Coders::LIST_INDEX[] = {
	UTIL_NAME_CODER_ENTRY(INDEX_DATABASE_ID),
	UTIL_NAME_CODER_ENTRY(INDEX_DATABASE_NAME),
	UTIL_NAME_CODER_ENTRY(INDEX_CONTAINER_NAME),
	UTIL_NAME_CODER_ENTRY(INDEX_NAME),
	UTIL_NAME_CODER_ENTRY(INDEX_ORDINAL),
	UTIL_NAME_CODER_ENTRY(INDEX_COLUMN_NAME),
	UTIL_NAME_CODER_ENTRY(INDEX_TYPE),
	UTIL_NAME_CODER_ENTRY(INDEX_TYPE_NAME)
};
const util::NameCoderEntry<MetaType::TriggerMeta>
		MetaType::Coders::LIST_TRIGGER[] = {
	UTIL_NAME_CODER_ENTRY(TRIGGER_DATABASE_ID),
	UTIL_NAME_CODER_ENTRY(TRIGGER_DATABASE_NAME),
	UTIL_NAME_CODER_ENTRY(TRIGGER_CONTAINER_NAME),
	UTIL_NAME_CODER_ENTRY(TRIGGER_ORDINAL),
	UTIL_NAME_CODER_ENTRY(TRIGGER_NAME),
	UTIL_NAME_CODER_ENTRY(TRIGGER_EVENT_TYPE),
	UTIL_NAME_CODER_ENTRY(TRIGGER_COLUMN_NAME),
	UTIL_NAME_CODER_ENTRY(TRIGGER_TYPE),
	UTIL_NAME_CODER_ENTRY(TRIGGER_URI),
	UTIL_NAME_CODER_ENTRY(TRIGGER_JMS_DESTINATION_TYPE),
	UTIL_NAME_CODER_ENTRY(TRIGGER_JMS_DESTINATION_NAME),
	UTIL_NAME_CODER_ENTRY(TRIGGER_USER),
	UTIL_NAME_CODER_ENTRY(TRIGGER_PASSWORD)
};

const util::NameCoderEntry<MetaType::ErasableMeta>
		MetaType::Coders::LIST_ERASABLE[] = {
	UTIL_NAME_CODER_ENTRY(ERASABLE_DATABASE_ID),
	UTIL_NAME_CODER_ENTRY(ERASABLE_DATABASE_NAME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_TYPE_NAME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_CONTAINER_ID),
	UTIL_NAME_CODER_ENTRY(ERASABLE_CONTAINER_NAME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_PARTITION_NAME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_CLUSTER_PARTITION),
	UTIL_NAME_CODER_ENTRY(ERASABLE_LARGE_CONTAINER_ID),
	UTIL_NAME_CODER_ENTRY(ERASABLE_SCHEMA_VERSION_ID),
	UTIL_NAME_CODER_ENTRY(ERASABLE_INIT_SCHEMA_STATUS),
	UTIL_NAME_CODER_ENTRY(ERASABLE_EXPIRATION_TYPE),
	UTIL_NAME_CODER_ENTRY(ERASABLE_LOWER_BOUNDARY_TIME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_UPPER_BOUNDARY_TIME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_EXPIRATION_TIME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_ERASABLE_TIME),
	UTIL_NAME_CODER_ENTRY(ERASABLE_ROW_INDEX_OID),
	UTIL_NAME_CODER_ENTRY(ERASABLE_MVCC_INDEX_OID)
};


const util::NameCoder<MetaType::ContainerMeta, MetaType::END_CONTAINER>
		MetaType::Coders::CODER_CONTAINER(LIST_CONTAINER, 1);
const util::NameCoder<MetaType::ColumnMeta, MetaType::END_COLUMN>
		MetaType::Coders::CODER_COLUMN(LIST_COLUMN, 1);
const util::NameCoder<MetaType::IndexMeta, MetaType::END_INDEX>
		MetaType::Coders::CODER_INDEX(LIST_INDEX, 1);
const util::NameCoder<MetaType::TriggerMeta, MetaType::END_TRIGGER>
		 MetaType::Coders::CODER_TRIGGER(LIST_TRIGGER, 1);
const util::NameCoder<MetaType::ErasableMeta, MetaType::END_ERASABLE>
		 MetaType::Coders::CODER_ERASABLE(LIST_ERASABLE, 1);

const util::NameCoderEntry<MetaType::StringConstants>
		MetaType::Coders::LIST_STR[] = {
	UTIL_NAME_CODER_ENTRY(STR_CONTAINER_NAME),
	UTIL_NAME_CODER_ENTRY(STR_CONTAINER_OPTIONAL_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_TABLE_NAME),
	UTIL_NAME_CODER_ENTRY(STR_TABLE_OPTIONAL_TYPE),

	UTIL_NAME_CODER_ENTRY(STR_KEY_SEQ),
	UTIL_NAME_CODER_ENTRY(STR_COLUMN_NAME),
	UTIL_NAME_CODER_ENTRY(STR_NULLABLE),
	UTIL_NAME_CODER_ENTRY(STR_ORDINAL_POSITION),
	UTIL_NAME_CODER_ENTRY(STR_TYPE_NAME),
	UTIL_NAME_CODER_ENTRY(STR_INDEX_NAME),

	UTIL_NAME_CODER_ENTRY(STR_DATABASE_NAME),
	UTIL_NAME_CODER_ENTRY(STR_DATA_AFFINITY),
	UTIL_NAME_CODER_ENTRY(STR_EXPIRATION_TIME),
	UTIL_NAME_CODER_ENTRY(STR_EXPIRATION_TIME_UNIT),
	UTIL_NAME_CODER_ENTRY(STR_EXPIRATION_DIVISION_COUNT),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_METHOD),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_WINDOW_SIZE),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_WINDOW_SIZE_UNIT),
	UTIL_NAME_CODER_ENTRY(STR_CLUSTER_PARTITION_INDEX),
	UTIL_NAME_CODER_ENTRY(STR_EXPIRATION_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_PARTITION_SEQ),
	UTIL_NAME_CODER_ENTRY(STR_PARTITION_NAME),
	UTIL_NAME_CODER_ENTRY(STR_PARTITION_BOUNDARY_VALUE),
	UTIL_NAME_CODER_ENTRY(STR_SUBPARTITION_BOUNDARY_VALUE),
	UTIL_NAME_CODER_ENTRY(STR_PARTITION_NODE_AFFINITY),
	UTIL_NAME_CODER_ENTRY(STR_DATABASE_MAJOR_VERSION),
	UTIL_NAME_CODER_ENTRY(STR_DATABASE_MINOR_VERSION),
	UTIL_NAME_CODER_ENTRY(STR_EXTRA_NAME_CHARACTERS),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_RELATIVE),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_RATE),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_SPAN),
	UTIL_NAME_CODER_ENTRY(STR_COMPRESSION_WIDTH),
	UTIL_NAME_CODER_ENTRY(STR_INDEX_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_TRIGGER_NAME),
	UTIL_NAME_CODER_ENTRY(STR_EVENT_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_TRIGGER_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_URI),
	UTIL_NAME_CODER_ENTRY(STR_JMS_DESTINATION_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_JMS_DESTINATION_NAME),
	UTIL_NAME_CODER_ENTRY(STR_USER),
	UTIL_NAME_CODER_ENTRY(STR_PASSWORD)
	,
	UTIL_NAME_CODER_ENTRY(STR_ATTRIBUTE),
	UTIL_NAME_CODER_ENTRY(STR_DATABASE_ID),
	UTIL_NAME_CODER_ENTRY(STR_CONTAINER_ID),
	UTIL_NAME_CODER_ENTRY(STR_LARGE_CONTAINER_ID),
	UTIL_NAME_CODER_ENTRY(STR_SCHEMA_VERSION_ID),
	UTIL_NAME_CODER_ENTRY(STR_INIT_SCHEMA_STATUS),
	UTIL_NAME_CODER_ENTRY(STR_LOWER_BOUNDARY_TIME),
	UTIL_NAME_CODER_ENTRY(STR_UPPER_BOUNDARY_TIME),
	UTIL_NAME_CODER_ENTRY(STR_ERASABLE_TIME),
	UTIL_NAME_CODER_ENTRY(STR_ROW_INDEX_OID),
	UTIL_NAME_CODER_ENTRY(STR_MVCC_INDEX_OID)

};
const util::NameCoder<MetaType::StringConstants, MetaType::END_STR>
		MetaType::Coders::CODER_STR(LIST_STR, 1);


const MetaType::CoreColumns::Entry<MetaType::ContainerMeta>
		MetaType::CoreColumns::COLUMNS_CONTAINER[] = {
	of(CONTAINER_DATABASE_ID).asLong(),
	of(CONTAINER_DATABASE_NAME).asString().asDbName(),
	of(CONTAINER_ATTRIBUTE).asInteger(),
	of(CONTAINER_TYPE_NAME).asString(),
	of(CONTAINER_NAME).asString().asContainerName(),
	of(CONTAINER_DATA_AFFINITY).asString(true),
	of(CONTAINER_EXPIRATION_TIME).asInteger(true),
	of(CONTAINER_EXPIRATION_UNIT).asString(true),
	of(CONTAINER_EXPIRATION_DIVISION).asInteger(true),
	of(CONTAINER_COMPRESSION_METHOD).asString(true),
	of(CONTAINER_COMPRESSION_SIZE).asInteger(true),
	of(CONTAINER_COMPRESSION_UNIT).asString(true),
	of(CONTAINER_CLUSTER_PARTITION).asInteger(),
	of(CONTAINER_EXPIRATION_TYPE).asString(true)
};
const MetaType::CoreColumns::Entry<MetaType::ColumnMeta>
		MetaType::CoreColumns::COLUMNS_COLUMN[] = {
	of(COLUMN_DATABASE_ID).asLong(),
	of(COLUMN_DATABASE_NAME).asString().asDbName(),
	of(COLUMN_CONTAINER_ATTRIBUTE).asInteger(),
	of(COLUMN_CONTAINER_NAME).asString().asContainerName(),
	of(COLUMN_ORDINAL).asInteger(),
	of(COLUMN_TYPE_NAME).asString(),
	of(COLUMN_NAME).asString(),
	of(COLUMN_KEY).asBool(),
	of(COLUMN_NULLABLE).asBool(),
	of(COLUMN_KEY_SEQUENCE).asShort(),
	of(COLUMN_COMPRESSION_RELATIVE).asBool(true),
	of(COLUMN_COMPRESSION_RATE).asDouble(true),
	of(COLUMN_COMPRESSION_SPAN).asDouble(true),
	of(COLUMN_COMPRESSION_WIDTH).asDouble(true)
};
const MetaType::CoreColumns::Entry<MetaType::IndexMeta>
		MetaType::CoreColumns::COLUMNS_INDEX[] = {
	of(INDEX_DATABASE_ID).asLong(),
	of(INDEX_DATABASE_NAME).asString().asDbName(),
	of(INDEX_CONTAINER_NAME).asString().asContainerName(),
	of(INDEX_NAME).asString(true),
	of(INDEX_ORDINAL).asShort(),
	of(INDEX_COLUMN_NAME).asString(),
	of(INDEX_TYPE).asShort(),
	of(INDEX_TYPE_NAME).asString()
};
const MetaType::CoreColumns::Entry<MetaType::TriggerMeta>
		MetaType::CoreColumns::COLUMNS_TRIGGER[] = {
	of(TRIGGER_DATABASE_ID).asLong(),
	of(TRIGGER_DATABASE_NAME).asString().asDbName(),
	of(TRIGGER_CONTAINER_NAME).asString().asContainerName(),
	of(TRIGGER_ORDINAL).asShort(),
	of(TRIGGER_NAME).asString(),
	of(TRIGGER_EVENT_TYPE).asString(true),
	of(TRIGGER_COLUMN_NAME).asString(true),
	of(TRIGGER_TYPE).asString(true),
	of(TRIGGER_URI).asString(true),
	of(TRIGGER_JMS_DESTINATION_TYPE).asString(true),
	of(TRIGGER_JMS_DESTINATION_NAME).asString(true),
	of(TRIGGER_USER).asString(true),
	of(TRIGGER_PASSWORD).asString(true)
};
const MetaType::CoreColumns::Entry<MetaType::ErasableMeta>
		MetaType::CoreColumns::COLUMNS_ERASABLE[] = {
	of(ERASABLE_DATABASE_ID).asLong(),
	of(ERASABLE_DATABASE_NAME).asString().asDbName(),
	of(ERASABLE_TYPE_NAME).asString(),
	of(ERASABLE_CONTAINER_ID).asLong().asContainerId(),
	of(ERASABLE_CONTAINER_NAME).asString(),
	of(ERASABLE_PARTITION_NAME).asString(true),
	of(ERASABLE_CLUSTER_PARTITION).asInteger().asPartitionIndex(),
	of(ERASABLE_LARGE_CONTAINER_ID).asLong(true),
	of(ERASABLE_SCHEMA_VERSION_ID).asInteger(),
	of(ERASABLE_INIT_SCHEMA_STATUS).asLong(true),
	of(ERASABLE_EXPIRATION_TYPE).asString(),
	of(ERASABLE_LOWER_BOUNDARY_TIME).asTimestamp(),
	of(ERASABLE_UPPER_BOUNDARY_TIME).asTimestamp(),
	of(ERASABLE_EXPIRATION_TIME).asTimestamp(),
	of(ERASABLE_ERASABLE_TIME).asTimestamp(),
	of(ERASABLE_ROW_INDEX_OID).asLong(),
	of(ERASABLE_MVCC_INDEX_OID).asLong()
};

template<typename T>
MetaType::CoreColumns::Entry<T> MetaType::CoreColumns::of(T id) {
	return Entry<T>(id);
}

template<typename T>
MetaType::CoreColumns::Entry<T>::Entry(T id) :
		id_(id),
		type_(COLUMN_TYPE_NULL),
		nullable_(false),
		common_(END_COMMON) {
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asType(uint8_t type, bool nullable) const {
	Entry dest = *this;
	dest.type_ = type;
	dest.nullable_ = nullable;
	return dest;
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asCommon(CommonMetaType common) const {
	Entry dest = *this;
	dest.common_ = common;
	return dest;
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asString(bool nullable) const {
	return asType(COLUMN_TYPE_STRING, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asBool(bool nullable) const {
	return asType(COLUMN_TYPE_BOOL, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asShort(bool nullable) const {
	return asType(COLUMN_TYPE_SHORT, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asInteger(bool nullable) const {
	return asType(COLUMN_TYPE_INT, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asLong(bool nullable) const {
	return asType(COLUMN_TYPE_LONG, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asDouble(bool nullable) const {
	return asType(COLUMN_TYPE_DOUBLE, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asTimestamp(bool nullable) const {
	return asType(COLUMN_TYPE_TIMESTAMP, nullable);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asDbName() const {
	return asCommon(COMMON_DATABASE_NAME);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asContainerName() const {
	return asCommon(COMMON_CONTAINER_NAME);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asPartitionIndex() const {
	return asCommon(COMMON_PARTITION_INDEX);
}

template<typename T> MetaType::CoreColumns::Entry<T>
MetaType::CoreColumns::Entry<T>::asContainerId() const {
	return asCommon(COMMON_CONTAINER_ID);
}


const MetaType::RefColumns::Entry<MetaType::ContainerMeta>
		MetaType::RefColumns::COLUMNS_CONTAINER[] = {
	of(CONTAINER_DATABASE_NAME, STR_DATABASE_NAME),
	of(CONTAINER_NAME, STR_CONTAINER_NAME, STR_TABLE_NAME),
	of(CONTAINER_TYPE_NAME,
			STR_CONTAINER_OPTIONAL_TYPE,
			STR_TABLE_OPTIONAL_TYPE),
	of(CONTAINER_DATA_AFFINITY, STR_DATA_AFFINITY),

	of(CONTAINER_EXPIRATION_TIME, STR_EXPIRATION_TIME),
	of(CONTAINER_EXPIRATION_UNIT, STR_EXPIRATION_TIME_UNIT),
	of(CONTAINER_EXPIRATION_DIVISION, STR_EXPIRATION_DIVISION_COUNT),

	of(CONTAINER_COMPRESSION_METHOD, STR_COMPRESSION_METHOD),
	of(CONTAINER_COMPRESSION_SIZE, STR_COMPRESSION_WINDOW_SIZE),
	of(CONTAINER_COMPRESSION_UNIT, STR_COMPRESSION_WINDOW_SIZE_UNIT),


	of(CONTAINER_CLUSTER_PARTITION, STR_CLUSTER_PARTITION_INDEX),
	of(CONTAINER_EXPIRATION_TYPE, STR_EXPIRATION_TYPE)
};
const MetaType::RefColumns::Entry<MetaType::ColumnMeta>
		MetaType::RefColumns::COLUMNS_COLUMN[] = {
	of(COLUMN_DATABASE_NAME, STR_DATABASE_NAME),
	of(COLUMN_CONTAINER_NAME, STR_CONTAINER_NAME, STR_TABLE_NAME),
	of(COLUMN_ORDINAL, STR_ORDINAL_POSITION),
	of(COLUMN_NAME, STR_COLUMN_NAME),
	of(COLUMN_TYPE_NAME, STR_TYPE_NAME),
	of(COLUMN_NULLABLE, STR_NULLABLE),
	of(COLUMN_COMPRESSION_RELATIVE, STR_COMPRESSION_RELATIVE),
	of(COLUMN_COMPRESSION_RATE, STR_COMPRESSION_RATE),
	of(COLUMN_COMPRESSION_SPAN, STR_COMPRESSION_SPAN),
	of(COLUMN_COMPRESSION_WIDTH, STR_COMPRESSION_WIDTH)
};
const MetaType::RefColumns::Entry<MetaType::ColumnMeta>
		MetaType::RefColumns::COLUMNS_KEY[] = {
	of(COLUMN_DATABASE_NAME, STR_DATABASE_NAME),
	of(COLUMN_CONTAINER_NAME, STR_CONTAINER_NAME, STR_TABLE_NAME),
	of(COLUMN_NAME, STR_COLUMN_NAME),
	of(COLUMN_KEY_SEQUENCE, STR_KEY_SEQ)
};
const MetaType::RefColumns::Entry<MetaType::IndexMeta>
		MetaType::RefColumns::COLUMNS_INDEX[] = {
	of(INDEX_DATABASE_NAME, STR_DATABASE_NAME),
	of(INDEX_CONTAINER_NAME, STR_CONTAINER_NAME, STR_TABLE_NAME),
	of(INDEX_NAME, STR_INDEX_NAME),
	of(INDEX_TYPE_NAME, STR_INDEX_TYPE),
	of(INDEX_ORDINAL, STR_ORDINAL_POSITION),
	of(INDEX_COLUMN_NAME, STR_COLUMN_NAME)
};
const MetaType::RefColumns::Entry<MetaType::TriggerMeta>
		MetaType::RefColumns::COLUMNS_TRIGGER[] = {
	of(TRIGGER_DATABASE_NAME, STR_DATABASE_NAME),
	of(TRIGGER_CONTAINER_NAME, STR_CONTAINER_NAME, STR_TABLE_NAME),
	of(TRIGGER_ORDINAL, STR_ORDINAL_POSITION),
	of(TRIGGER_NAME, STR_TRIGGER_NAME),
	of(TRIGGER_EVENT_TYPE, STR_EVENT_TYPE),
	of(TRIGGER_COLUMN_NAME, STR_COLUMN_NAME),
	of(TRIGGER_TYPE, STR_TRIGGER_TYPE),
	of(TRIGGER_URI, STR_URI),
	of(TRIGGER_JMS_DESTINATION_TYPE, STR_JMS_DESTINATION_TYPE),
	of(TRIGGER_JMS_DESTINATION_NAME, STR_JMS_DESTINATION_NAME),
	of(TRIGGER_USER, STR_USER),
	of(TRIGGER_PASSWORD, STR_PASSWORD)
};
const MetaType::RefColumns::Entry<MetaType::ErasableMeta>
		MetaType::RefColumns::COLUMNS_ERASABLE[] = {
	of(ERASABLE_DATABASE_NAME, STR_DATABASE_NAME),
	of(ERASABLE_CONTAINER_NAME, STR_CONTAINER_NAME, STR_TABLE_NAME),
	of(ERASABLE_PARTITION_NAME, STR_PARTITION_NAME),
	of(ERASABLE_TYPE_NAME, STR_TABLE_OPTIONAL_TYPE,
			STR_CONTAINER_OPTIONAL_TYPE),
	of(ERASABLE_EXPIRATION_TYPE, STR_EXPIRATION_TYPE),
	of(ERASABLE_DATABASE_ID, STR_DATABASE_ID),
	of(ERASABLE_CONTAINER_ID, STR_CONTAINER_ID),
	of(ERASABLE_CLUSTER_PARTITION, STR_CLUSTER_PARTITION_INDEX),
	of(ERASABLE_LARGE_CONTAINER_ID, STR_LARGE_CONTAINER_ID),
	of(ERASABLE_SCHEMA_VERSION_ID, STR_SCHEMA_VERSION_ID),
	of(ERASABLE_INIT_SCHEMA_STATUS, STR_INIT_SCHEMA_STATUS),
	of(ERASABLE_LOWER_BOUNDARY_TIME, STR_LOWER_BOUNDARY_TIME),
	of(ERASABLE_UPPER_BOUNDARY_TIME, STR_UPPER_BOUNDARY_TIME),
	of(ERASABLE_EXPIRATION_TIME, STR_EXPIRATION_TIME),
	of(ERASABLE_ERASABLE_TIME, STR_ERASABLE_TIME),
	of(ERASABLE_ROW_INDEX_OID, STR_ROW_INDEX_OID),
	of(ERASABLE_MVCC_INDEX_OID, STR_MVCC_INDEX_OID)
};

template<typename T>
MetaType::RefColumns::Entry<T> MetaType::RefColumns::of(
		T refId,
		StringConstants nameForContainer, StringConstants nameForTable) {
	return Entry<T>(refId, nameForContainer, nameForTable);
}

template<typename T>
MetaType::RefColumns::Entry<T>::Entry(
		T refId,
		StringConstants nameForContainer, StringConstants nameForTable) :
		refId_(refId),
		nameForContainer_(nameForContainer),
		nameForTable_(nameForTable) {
}


const MetaContainerInfo MetaType::Containers::CONTAINERS_CORE[] = {
	coreOf(
			TYPE_CONTAINER, "_core_containers",
			CoreColumns::COLUMNS_CONTAINER, Coders::CODER_CONTAINER, 0),
	coreOf(
			TYPE_COLUMN, "_core_columns",
			CoreColumns::COLUMNS_COLUMN, Coders::CODER_COLUMN, 0),
	noneOf(TYPE_KEY),
	coreOf(
			TYPE_INDEX, "_core_index_info",
			CoreColumns::COLUMNS_INDEX, Coders::CODER_INDEX, 0),
	coreOf(
			TYPE_TRIGGER, "_core_event_triggers",
			CoreColumns::COLUMNS_TRIGGER, Coders::CODER_TRIGGER, 0)
	,
	coreOf(
			TYPE_ERASABLE, "_core_internal_erasables",
			CoreColumns::COLUMNS_ERASABLE, Coders::CODER_ERASABLE, 0)
};

const MetaContainerInfo MetaType::Containers::CONTAINERS_REF[] = {
	refOf(TYPE_CONTAINER, NULL, "containers", "tables",
			RefColumns::COLUMNS_CONTAINER, 0),
	refOf(TYPE_COLUMN,
			"columns", "container_columns", "table_columns",
			RefColumns::COLUMNS_COLUMN, 0),
	refOf(TYPE_KEY,
			"primary_keys", "container_primary_keys", "table_primary_keys",
			RefColumns::COLUMNS_KEY, 0, TYPE_COLUMN),
	refOf(TYPE_INDEX,
			"index_info", "container_index_info", "table_index_info",
			RefColumns::COLUMNS_INDEX, 0),
	refOf(TYPE_TRIGGER,
			"event_triggers", "container_event_triggers", "table_event_triggers",
			RefColumns::COLUMNS_TRIGGER, 0)
	,
	toInternal(refOf(
			TYPE_ERASABLE,
			"_internal_erasables",
			"_internal_container_erasables", "_internal_table_erasables",
			RefColumns::COLUMNS_ERASABLE, 0))
};

MetaContainerId MetaType::Containers::typeToId(MetaContainerType type) {
	if (0 <= type && type < END_TYPE) {
		return static_cast<MetaContainerId>(type);
	}
	else {
		return UNDEF_CONTAINERID;
	}
}

bool MetaType::Containers::idToIndex(MetaContainerId id, size_t &index) {
	if (id < END_TYPE) {
		index = static_cast<size_t>(id);
		return true;
	}
	else {
		index = std::numeric_limits<size_t>::max();
		return false;
	}
}

template<typename T, size_t N>
MetaContainerInfo MetaType::Containers::coreOf(
		MetaContainerType type, const char8_t *name,
		const CoreColumns::Entry<T> (&columnList)[N],
		const util::NameCoder<T, N> &coder, uint16_t version) {
	static MetaColumnInfo destColumnList[N];

	MetaContainerInfo::CommonColumnInfo commonInfo;
	for (size_t i = 0; i < N; i++) {
		const CoreColumns::Entry<T> &srcInfo = columnList[i];
		MetaColumnInfo &destInfo = destColumnList[i];

		setUpCoreColumnInfo(
				i, srcInfo.id_, srcInfo.type_, srcInfo.nullable_,
				coder(srcInfo.id_), destInfo);
		setUpCommonColumnInfo(srcInfo.id_, srcInfo.common_, commonInfo);
	}

	{
		MetaContainerInfo info;
		info.id_ = typeToId(type);
		info.versionId_ = makeSchemaVersionId(version, N);
		info.forCore_ = true;
		info.name_.neutral_ = name;
		info.columnList_ = destColumnList;
		info.columnCount_ = N;
		info.commonInfo_ = commonInfo;
		return info;
	}
}

template<typename T, size_t N>
MetaContainerInfo MetaType::Containers::refOf(
		MetaContainerType type, const char8_t *name,
		const char8_t *nameForContainer, const char8_t *nameForTable,
		const RefColumns::Entry<T> (&columnList)[N], uint16_t version,
		MetaContainerType refType) {
	static MetaColumnInfo destColumnList[N];

	const MetaContainerType resolvedRefType =
			(refType == END_TYPE ? type : refType);
	for (size_t i = 0; i < N; i++) {
		const RefColumns::Entry<T> &srcInfo = columnList[i];
		MetaColumnInfo &destInfo = destColumnList[i];

		setUpRefColumnInfo(
				resolvedRefType, i, srcInfo.refId_, srcInfo.nameForContainer_,
				srcInfo.nameForTable_, destInfo);
	}

	{
		MetaContainerInfo info;
		info.id_ = typeToId(type);
		info.refId_ = typeToId(resolvedRefType);
		info.versionId_ = makeSchemaVersionId(version, N);
		info.forCore_ = false;
		info.name_.neutral_ = name;
		info.name_.forContainer_ = nameForContainer;
		info.name_.forTable_ = nameForTable;
		info.columnList_ = destColumnList;
		info.columnCount_ = N;
		return info;
	}
}

MetaContainerInfo MetaType::Containers::noneOf(MetaContainerType type) {
	MetaContainerInfo info;
	info.id_ = typeToId(type);
	return info;
}

MetaContainerInfo MetaType::Containers::toInternal(
		const MetaContainerInfo &src) {
	MetaContainerInfo info = src;
	info.internal_ = true;
	return info;
}

void MetaType::Containers::setUpCoreColumnInfo(
		size_t index, ColumnId id, uint8_t type, bool nullable,
		const char8_t *name, MetaColumnInfo &info) {
	assert(info.id_ == UNDEF_COLUMNID);
	assert(index == id);
	assert(name != NULL);

	info.id_ = id;
	info.type_ = type;
	info.nullable_ = nullable;
	info.name_.forContainer_ = name;
}

void MetaType::Containers::setUpRefColumnInfo(
		MetaContainerType type, size_t index, ColumnId refId,
		StringConstants nameForContainer, StringConstants nameForTable,
		MetaColumnInfo &info) {
	size_t typeIndex;
	if (!idToIndex(typeToId(type), typeIndex)) {
		assert(false);
		return;
	}
	assert(typeIndex <
			sizeof(CONTAINERS_CORE) / sizeof(*CONTAINERS_CORE));
	const MetaContainerInfo &refContainerInfo = CONTAINERS_CORE[typeIndex];

	assert(refId < refContainerInfo.columnCount_);
	const MetaColumnInfo &refInfo = refContainerInfo.columnList_[refId];
	assert(refInfo.id_ != UNDEF_COLUMNID);

	const char8_t *nameStrForContainer = Coders::CODER_STR(nameForContainer);
	const char8_t *nameStrForTable = Coders::CODER_STR(nameForTable);

	assert(info.id_ == UNDEF_COLUMNID);
	assert(nameStrForContainer != NULL);

	info.id_ = static_cast<ColumnId>(index);
	info.refId_ = refId;
	info.type_ = refInfo.type_;
	info.nullable_ = refInfo.nullable_;
	info.name_.forContainer_ = nameStrForContainer;
	info.name_.forTable_ = nameStrForTable;
}

void MetaType::Containers::setUpCommonColumnInfo(
		ColumnId id, CommonMetaType type,
		MetaContainerInfo::CommonColumnInfo &commonInfo) {
	switch (type) {
	case COMMON_DATABASE_NAME:
		commonInfo.dbNameColumn_ = id;
		break;
	case COMMON_CONTAINER_NAME:
		commonInfo.containerNameColumn_ = id;
		break;
	case COMMON_PARTITION_INDEX:
		commonInfo.partitionIndexColumn_ = id;
		break;
	case COMMON_CONTAINER_ID:
		commonInfo.containerIdColumn_ = id;
		break;
	default:
		break;
	}
}

SchemaVersionId MetaType::Containers::makeSchemaVersionId(
		uint16_t base, size_t columnCount) {
	const SchemaVersionId shiftBits = (CHAR_BIT * sizeof(uint16_t));
	assert(columnCount < (1U << shiftBits));

	return (base << shiftBits) | static_cast<uint16_t>(columnCount);
}


const MetaType::InfoTable MetaType::InfoTable::TABLE_INSTANCE;

const MetaType::InfoTable& MetaType::InfoTable::getInstance() {
	return TABLE_INSTANCE;
}

const MetaContainerInfo& MetaType::InfoTable::resolveInfo(
		MetaContainerId id, bool forCore) const {
	const MetaContainerInfo *info = findInfo(id, forCore);
	if (info == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_NOT_FOUND, "");
	}
	return *info;
}

const MetaContainerInfo* MetaType::InfoTable::findInfo(
		const char8_t *name, bool forCore, NamingType &namingType) const {
	assert(name != NULL);
	namingType = NAMING_NEUTRAL;

	const NameEntry *entryList = (forCore ? coreList_ : refList_);
	const NameEntry *entryEnd = entryList + (forCore ?
			getNameEntryCount(coreList_) : getNameEntryCount(refList_));
	const NameEntry key(name, NameEntry::second_type());
	const std::pair<const NameEntry*, const NameEntry*> &range =
			std::equal_range<const NameEntry*>(
					entryList, entryEnd, key, EntryPred());

	if (range.first == range.second) {
		return NULL;
	}

	const EntryValue &entryValue = range.first->second;

	const MetaContainerInfo *info = findInfoByIndex(
			static_cast<size_t>(entryValue.first), forCore);
	if (info == NULL) {
		return NULL;
	}

	namingType = entryValue.second;
	return info;
}

const MetaContainerInfo* MetaType::InfoTable::findInfo(
		MetaContainerId id, bool forCore) const {
	size_t index;
	if (!Containers::idToIndex(id, index)) {
		return NULL;
	}

	return findInfoByIndex(index, forCore);
}

MetaType::NamingType MetaType::InfoTable::resolveNaming(
		NamingType specifiedType, NamingType defaultType) {
	if (specifiedType == NAMING_NEUTRAL) {
		return defaultType;
	}
	else {
		return specifiedType;
	}
}

const char8_t* MetaType::InfoTable::nameOf(
		const MetaColumnInfo &columnInfo, NamingType namingType,
		bool exact) {
	const char8_t *name;
	if (namingType == NAMING_TABLE) {
		name = columnInfo.name_.forTable_;
	}
	else {
		name = columnInfo.name_.forContainer_;
	}

	if (name == NULL && !exact) {
		name = columnInfo.name_.forContainer_;
		assert(name != NULL);
	}

	return name;
}

const char8_t* MetaType::InfoTable::nameOf(
		const MetaContainerInfo &containerInfo, NamingType namingType,
		bool exact) {
	const char8_t *name;
	if (namingType == NAMING_TABLE) {
		name = containerInfo.name_.forTable_;
	}
	else if (namingType == NAMING_CONTAINER) {
		name = containerInfo.name_.forContainer_;
	}
	else {
		name = containerInfo.name_.neutral_;
	}

	if (name == NULL && !exact) {
		if (namingType == NAMING_NEUTRAL) {
			name = containerInfo.name_.forContainer_;
		}
		else {
			name = containerInfo.name_.neutral_;
		}
	}

	return name;
}

MetaType::InfoTable::InfoTable() {
	setUpNameEntries(Containers::CONTAINERS_CORE, coreList_);
	setUpNameEntries(Containers::CONTAINERS_REF, refList_);
}

const MetaContainerInfo* MetaType::InfoTable::findInfoByIndex(
		size_t index, bool forCore) {
	if (index >= Containers::TYPE_COUNT) {
		assert(false);
		return NULL;
	}

	const MetaContainerInfo *info;
	if (forCore) {
		info = &Containers::CONTAINERS_CORE[index];
	}
	else {
		info = &Containers::CONTAINERS_REF[index];
	}

	if (info->isEmpty()) {
		return NULL;
	}

	return info;
}

template<size_t N, size_t M>
void MetaType::InfoTable::setUpNameEntries(
		const MetaContainerInfo (&infoList)[N], NameEntry (&entryList)[M]) {
	const size_t namingCount = M / N;
	const bool forCore = (namingCount == 1);
	UTIL_STATIC_ASSERT(N * namingCount == M);
	UTIL_STATIC_ASSERT(forCore || namingCount == END_NAMING);

	for (size_t i = 0; i < N; i++) {
		const MetaContainerInfo &info = infoList[i];
		assert(info.isEmpty() || !info.forCore_ == !forCore);

		size_t expectedIndex;
		Containers::idToIndex(info.id_, expectedIndex);
		assert(i == expectedIndex);

		for (size_t j = 0; j < namingCount; j++) {
			NameEntry &entry = entryList[namingCount * i + j];
			const NamingType namingType = static_cast<NamingType>(j);

			const char8_t *name =
					(info.isEmpty() ? NULL : nameOf(info, namingType, true));
			entry.first = (name == NULL ? "" : name);
			entry.second.first = i;
			entry.second.second = namingType;
		}
	}
	std::sort(entryList, entryList + M, EntryPred());
}

template<size_t M>
size_t MetaType::InfoTable::getNameEntryCount(const NameEntry (&)[M]) {
	return M;
}

bool MetaType::InfoTable::EntryPred::operator()(
		const NameEntry &entry1, const NameEntry &entry2) const {
	return util::stricmp(entry1.first, entry2.first) < 0;
}


MetaProcessor::MetaProcessor(
		TransactionContext &txn, MetaContainerId id, bool forCore) :
		info_(MetaType::InfoTable::getInstance().resolveInfo(id, forCore)),
		nextContainerId_(0),
		containerLimit_(std::numeric_limits<uint64_t>::max()),
		containerKey_(NULL) {
	static_cast<void>(txn);
}

void MetaProcessor::scan(
		TransactionContext &txn, const Source &source, RowHandler &handler) {
	if (info_.forCore_) {
		Context cxt(txn, *this, source, handler);
		switch (info_.id_) {
		case MetaType::TYPE_CONTAINER:
			scanCore<ContainerHandler>(txn, cxt);
			break;
		case MetaType::TYPE_COLUMN:
			scanCore<ColumnHandler>(txn, cxt);
			break;
		case MetaType::TYPE_INDEX:
			scanCore<IndexHandler>(txn, cxt);
			break;
		case MetaType::TYPE_TRIGGER:
			scanCore<MetaTriggerHandler>(txn, cxt);
			break;
		case MetaType::TYPE_ERASABLE:
			scanCore<ErasableHandler>(txn, cxt);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}
	else {
		MetaProcessor subProcessor(txn, info_.refId_, true);
		subProcessor.setState(*this);
		assert(subProcessor.info_.forCore_);

		if (info_.id_ == MetaType::TYPE_KEY) {
			KeyRefHandler refHandler(txn, info_, handler);
			subProcessor.scan(txn, source, refHandler);
		}
		else {
			RefHandler refHandler(txn, info_, handler);
			subProcessor.scan(txn, source, refHandler);
		}

		setState(subProcessor);
	}
}

bool MetaProcessor::isSuspended() const {
	return (nextContainerId_ != UNDEF_CONTAINERID);
}

ContainerId MetaProcessor::getNextContainerId() const {
	return nextContainerId_;
}

void MetaProcessor::setNextContainerId(ContainerId containerId) {
	nextContainerId_ = containerId;
}

void MetaProcessor::setContainerLimit(uint64_t limit) {
	containerLimit_ = limit;
}

void MetaProcessor::setContainerKey(const FullContainerKey *containerKey) {
	containerKey_ = containerKey;
}

template<typename HandlerType>
void MetaProcessor::scanCore(TransactionContext &txn, Context &cxt) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	DataStore::ContainerCondition condition(alloc);
	const bool singleRequired = true;
	const bool largeRequired = (info_.id_ != MetaType::TYPE_ERASABLE);
	const bool subRequired = (info_.id_ == MetaType::TYPE_ERASABLE);
	if (singleRequired) {
		condition.insertAttribute(CONTAINER_ATTR_SINGLE);
	}
	if (largeRequired) {
		condition.insertAttribute(CONTAINER_ATTR_LARGE);
	}
	if (subRequired) {
		condition.insertAttribute(CONTAINER_ATTR_SUB);
	}

	HandlerType handler(cxt);

	DataStore *dataStore = cxt.getSource().dataStore_;
	assert(dataStore != NULL);
	const DatabaseId dbId = cxt.getSource().dbId_;

	if (containerKey_ == NULL) {
		if (!dataStore->scanContainerList(
				txn, txn.getPartitionId(), nextContainerId_,
				containerLimit_, &dbId, condition, handler)) {
			setNextContainerId(UNDEF_CONTAINERID);
		}
	}
	else {
		ContainerAutoPtr containerPtr(
				txn, dataStore, txn.getPartitionId(), *containerKey_,
				ANY_CONTAINER);
		BaseContainer *container = containerPtr.getBaseContainer();
		if (container != NULL) {
			handler(
					txn, container->getContainerId(), dbId,
					container->getAttribute(), *container);
		}
		setNextContainerId(UNDEF_CONTAINERID);
	}
}

void MetaProcessor::setState(const MetaProcessor &src) {
	nextContainerId_ = src.nextContainerId_;
	containerLimit_ = src.containerLimit_;
	containerKey_ = src.containerKey_;
}


Value MetaProcessor::ValueUtils::makeNull() {
	Value dest;
	dest.setNull();
	return dest;
}

Value MetaProcessor::ValueUtils::makeString(
		util::StackAllocator &alloc, const char8_t *src) {
	char8_t *addr = const_cast<char8_t*>(src);
	const uint32_t size = static_cast<uint32_t>(strlen(src));

	Value dest;
	dest.set(alloc, addr, size);
	return dest;
}

Value MetaProcessor::ValueUtils::makeBool(bool src) {
	return Value(src);
}

Value MetaProcessor::ValueUtils::makeShort(int16_t src) {
	return Value(src);
}

Value MetaProcessor::ValueUtils::makeInteger(int32_t src) {
	return Value(src);
}

Value MetaProcessor::ValueUtils::makeLong(int64_t src) {
	return Value(src);
}

Value MetaProcessor::ValueUtils::makeDouble(double src) {
	return Value(src);
}

Value MetaProcessor::ValueUtils::makeTimestamp(Timestamp src) {
	Value value;
	value.setTimestamp(src);
	return value;
}

void MetaProcessor::ValueUtils::toUpperString(util::String &str) {
	for (util::String::iterator it = str.begin(); it != str.end(); ++it) {
		*it = Lexer::ToUpper(*it);
	}
}


MetaProcessor::ValueListSource::ValueListSource(
		ValueList &valueList, ValueCheckList &valueCheckList,
		const MetaContainerInfo &info) :
		valueList_(valueList),
		valueCheckList_(valueCheckList),
		info_(info) {
}


template<typename T>
MetaProcessor::ValueListBuilder<T>::ValueListBuilder(
		const ValueListSource &source) :
		source_(source) {
	assert(source_.valueList_.size() == source_.valueCheckList_.size());
	assert(source_.valueList_.size() == source_.info_.columnCount_);

	source_.valueCheckList_.assign(source_.valueCheckList_.size(), false);
}

template<typename T>
const MetaProcessor::ValueList& MetaProcessor::ValueListBuilder<T>::build() {
	if (std::find(
			source_.valueCheckList_.begin(),
			source_.valueCheckList_.end(), false) !=
			source_.valueCheckList_.end()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	return source_.valueList_;
}

template<typename T>
void MetaProcessor::ValueListBuilder<T>::set(T column, const Value &value) {
	assert(0 <= column);
	assert(static_cast<size_t>(column) < source_.info_.columnCount_);

	const MetaColumnInfo &info = source_.info_.columnList_[column];
	if (value.getType() != info.type_) {
		if (value.isNullValue()) {
			if (!info.nullable_) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}
		}
		else {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	source_.valueList_[column] = value;
	source_.valueCheckList_[column] = true;
}


MetaProcessor::Context::Context(
		TransactionContext &txn, MetaProcessor &processor,
		const Source &source, RowHandler &coreRowHandler) :
		processor_(processor),
		source_(source),
		valueList_(txn.getDefaultAllocator()),
		valueCheckList_(txn.getDefaultAllocator()),
		coreInfo_(processor.info_),
		valueListSource_(valueList_, valueCheckList_, coreInfo_),
		coreRowHandler_(coreRowHandler) {
	assert(processor.info_.forCore_);

	valueList_.assign(coreInfo_.columnCount_, ValueUtils::makeNull());
	valueCheckList_.assign(coreInfo_.columnCount_, false);
}

const MetaProcessorSource& MetaProcessor::Context::getSource() const {
	return source_;
}

const MetaProcessor::ValueListSource&
MetaProcessor::Context::getValueListSource() const {
	return valueListSource_;
}

MetaProcessor::RowHandler& MetaProcessor::Context::getRowHandler() const {
	return coreRowHandler_;
}

void MetaProcessor::Context::stepContainerListing(
		ContainerId lastContainerId) {
	assert(lastContainerId != UNDEF_CONTAINERID);
	processor_.nextContainerId_ = lastContainerId + 1;
}



MetaProcessor::StoreCoreHandler::StoreCoreHandler(Context &cxt) :
		cxt_(cxt) {
}

void MetaProcessor::StoreCoreHandler::operator()(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container) const {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	if (attribute == CONTAINER_ATTR_SUB) {
		cxt_.stepContainerListing(id);
		return;
	}
	else if (attribute != CONTAINER_ATTR_LARGE) {
		execute(txn, id, dbId, attribute, container, NULL);
		cxt_.stepContainerListing(id);
		return;
	}

	BaseContainer *resolvedSubContainer;
	const FullContainerKey &key = container.getContainerKey(txn);
	const bool unnormalized = true;
	FullContainerKeyComponents components =
			key.getComponents(alloc, unnormalized);

	components.affinityString_ = NULL;
	components.affinityStringSize_ = 0;
	components.affinityNumber_ = txn.getPartitionId();
	components.largeContainerId_ = id;

	const FullContainerKey subKey(
		alloc, KeyConstraint::getNoLimitKeyConstraint(), components);

	const bool caseSensitive = true;
	ContainerAutoPtr subContainerPtr(
			txn, container.getDataStore(), txn.getPartitionId(),
			subKey, ANY_CONTAINER, caseSensitive);

	resolvedSubContainer = subContainerPtr.getBaseContainer();
	if (resolvedSubContainer == NULL) {
		cxt_.stepContainerListing(id);
		return;
	}

	if (resolvedSubContainer->getAttribute() != CONTAINER_ATTR_SUB) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	execute(txn, id, dbId, attribute, container, resolvedSubContainer);
	cxt_.stepContainerListing(id);
}

void MetaProcessor::StoreCoreHandler::execute(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container,
		BaseContainer *subContainer) const {
	static_cast<void>(txn);
	static_cast<void>(id);
	static_cast<void>(dbId);
	static_cast<void>(attribute);
	static_cast<void>(container);
	static_cast<void>(subContainer);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
}

MetaProcessor::Context& MetaProcessor::StoreCoreHandler::getContext() const {
	return cxt_;
}

void MetaProcessor::StoreCoreHandler::getNames(
		TransactionContext &txn, BaseContainer &container,
		const char8_t *&dbName, const char8_t *&containerName) const {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::String *nameStr = ALLOC_NEW(alloc) util::String(alloc);
	container.getContainerKey(txn).toString(alloc, *nameStr);

	dbName = cxt_.getSource().dbName_;
	containerName = nameStr->c_str();
}


MetaProcessor::RefHandler::RefHandler(
		TransactionContext &txn, const MetaContainerInfo &refInfo,
		RowHandler &rowHandler) :
		refInfo_(refInfo),
		destValueList_(txn.getDefaultAllocator()),
		rowHandler_(rowHandler) {
	destValueList_.assign(refInfo_.columnCount_, ValueUtils::makeNull());
}

void MetaProcessor::RefHandler::operator()(
		TransactionContext &txn, const ValueList &valueList) {
	if (filter(txn, valueList)) {
		return;
	}

	for (size_t i = 0; i < refInfo_.columnCount_; i++) {
		const ColumnId refId = refInfo_.columnList_[i].refId_;
		assert(refId < valueList.size());

		destValueList_[i] = valueList[refId];
	}

	rowHandler_(txn, destValueList_);
}

bool MetaProcessor::RefHandler::filter(
		TransactionContext &txn, const ValueList &valueList) {
	static_cast<void>(txn);
	static_cast<void>(valueList);
	return false;
}



const int8_t MetaProcessor::ContainerHandler::META_EXPIRATION_TYPE_ROW = 0;

MetaProcessor::ContainerHandler::ContainerHandler(Context &cxt) :
		StoreCoreHandler(cxt) {
}

void MetaProcessor::ContainerHandler::execute(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container,
		BaseContainer *subContainer) const {
	static_cast<void>(id);

	ValueListBuilder<MetaType::ContainerMeta> builder(
			getContext().getValueListSource());
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	const BaseContainer &schemaContainer =
			(subContainer == NULL ? container : *subContainer);

	const ContainerType type = schemaContainer.getContainerType();

	const char8_t *dbName;
	const char8_t *name;
	getNames(txn, container, dbName, name);


	builder.set(
			MetaType::CONTAINER_DATABASE_ID,
			ValueUtils::makeLong(dbId));
	builder.set(
			MetaType::CONTAINER_DATABASE_NAME,
			ValueUtils::makeString(alloc, dbName));
	builder.set(
			MetaType::CONTAINER_ATTRIBUTE,
			ValueUtils::makeInteger(attribute));
	builder.set(
			MetaType::CONTAINER_TYPE_NAME,
			ValueUtils::makeString(alloc, containerTypeToName(type)));
	builder.set(
			MetaType::CONTAINER_NAME,
			ValueUtils::makeString(alloc, name));

	const char8_t *dataAffinity = container.getAffinity();
	builder.set(
			MetaType::CONTAINER_DATA_AFFINITY,
			(strlen(dataAffinity) == 0 ? ValueUtils::makeNull() :
					ValueUtils::makeString(alloc, dataAffinity)));

	const BaseContainer::ExpirationInfo *expirationInfo = NULL;
	const CompressionSchema *compression = NULL;
	int32_t expirationDivision = -1;
	if (type == TIME_SERIES_CONTAINER) {
		const TimeSeries &timeSeries =
				static_cast<const TimeSeries&>(schemaContainer);

		expirationInfo = &timeSeries.getExpirationInfo();
		if (expirationInfo->elapsedTime_ < 0) {
			expirationInfo = NULL;
		}
		else {
			expirationDivision = expirationInfo->dividedNum_;
		}

		compression = &timeSeries.getCompressionSchema();
		if (compression->getCompressionType() == NO_COMPRESSION) {
			compression = NULL;
		}
	}


	builder.set(
			MetaType::CONTAINER_EXPIRATION_TIME,
			expirationInfo == NULL ? ValueUtils::makeNull() :
					ValueUtils::makeInteger(expirationInfo->elapsedTime_));
	builder.set(
			MetaType::CONTAINER_EXPIRATION_UNIT,
			expirationInfo == NULL ? ValueUtils::makeNull() : ValueUtils::makeString(
					alloc, timeUnitToName(expirationInfo->timeUnit_)));
	builder.set(
			MetaType::CONTAINER_EXPIRATION_DIVISION,
			expirationDivision < 0 ? ValueUtils::makeNull() :
					ValueUtils::makeInteger(expirationDivision));
	builder.set(
			MetaType::CONTAINER_COMPRESSION_METHOD,
			compression == NULL ? ValueUtils::makeNull() : ValueUtils::makeString(
					alloc, compressionToName(compression->getCompressionType())));

	const bool windowSizeSpecified = (compression != NULL &&
			compression->getDurationInfo().timeDuration_ > 0);
	builder.set(
			MetaType::CONTAINER_COMPRESSION_SIZE,
			!windowSizeSpecified ? ValueUtils::makeNull() :
					ValueUtils::makeInteger(
							compression->getDurationInfo().timeDuration_));
	builder.set(
			MetaType::CONTAINER_COMPRESSION_UNIT,
			!windowSizeSpecified ? ValueUtils::makeNull() : ValueUtils::makeString(
					alloc, timeUnitToName(
							compression->getDurationInfo().timeUnit_)));


	builder.set(
			MetaType::CONTAINER_CLUSTER_PARTITION,
			ValueUtils::makeInteger(txn.getPartitionId()));

	{
		int8_t expirationType = -1;
		if (expirationInfo != NULL) {
			expirationType = META_EXPIRATION_TYPE_ROW;
		}
		builder.set(
				MetaType::CONTAINER_EXPIRATION_TYPE,
				expirationType < 0 ?
				ValueUtils::makeNull() : ValueUtils::makeString(
						alloc, expirationTypeToName(expirationType)));
	}

	getContext().getRowHandler()(txn, builder.build());
}

const char8_t* MetaProcessor::ContainerHandler::containerTypeToName(
		ContainerType type) {
	switch (type) {
	case TIME_SERIES_CONTAINER:
		return "TIMESERIES";
	case COLLECTION_CONTAINER:
		return "COLLECTION";
	default:
		assert(false);
		return "";
	}
}

const char8_t* MetaProcessor::ContainerHandler::timeUnitToName(
		TimeUnit unit) {
	switch (unit) {
	case TIME_UNIT_DAY:
		return "DAY";
	case TIME_UNIT_HOUR:
		return "HOUR";
	case TIME_UNIT_MINUTE:
		return "MINUTE";
	case TIME_UNIT_SECOND:
		return "SECOND";
	case TIME_UNIT_MILLISECOND:
		return "MILLISECOND";
	default:
		assert(false);
		return "";
	}
}

const char8_t* MetaProcessor::ContainerHandler::compressionToName(
		int8_t type) {
	switch (type) {
	case SS_COMPRESSION:
		return "SS";
	case HI_COMPRESSION:
		return "HI";
	default:
		assert(false);
		return "";
	}
}


const char8_t* MetaProcessor::ContainerHandler::expirationTypeToName(
		int8_t type) {
	switch (type) {
	case META_EXPIRATION_TYPE_ROW:
		return "ROW";
	default:
		assert(false);
		return "";
	}
}


MetaProcessor::ColumnHandler::ColumnHandler(Context &cxt) :
		StoreCoreHandler(cxt) {
}

void MetaProcessor::ColumnHandler::execute(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container,
		BaseContainer *subContainer) const {
	static_cast<void>(id);

	ValueListBuilder<MetaType::ColumnMeta> builder(
			getContext().getValueListSource());
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	const BaseContainer &schemaContainer =
			(subContainer == NULL ? container : *subContainer);

	const ContainerType type = schemaContainer.getContainerType();

	const char8_t *dbName;
	const char8_t *name;
	getNames(txn, container, dbName, name);

	builder.set(
			MetaType::COLUMN_DATABASE_ID,
			ValueUtils::makeLong(dbId));
	builder.set(
			MetaType::COLUMN_DATABASE_NAME,
			ValueUtils::makeString(alloc, dbName));
	builder.set(
			MetaType::COLUMN_CONTAINER_ATTRIBUTE,
			ValueUtils::makeInteger(attribute));
	builder.set(
			MetaType::COLUMN_CONTAINER_NAME,
			ValueUtils::makeString(alloc, name));

	const CompressionSchema *compression = NULL;
	if (type == TIME_SERIES_CONTAINER) {
		const TimeSeries &timeSeries =
				static_cast<const TimeSeries&>(schemaContainer);

		compression = &timeSeries.getCompressionSchema();
		if (compression->getCompressionType() == NO_COMPRESSION) {
			compression = NULL;
		}
	}

	ObjectManager *objectManager =
			container.getDataStore()->getObjectManager();

	const uint32_t columnCount = schemaContainer.getColumnNum();
	int16_t keyCount = 0;
	for (uint32_t i = 0; i < columnCount; i++) {
		const ColumnInfo &info = schemaContainer.getColumnInfo(i);

		const ColumnType columnType = info.getColumnType();

		const bool nullable = !info.isNotNull();

		builder.set(
				MetaType::COLUMN_ORDINAL,
				ValueUtils::makeInteger(i + 1));


		const char8_t *columnTypeName =
				ValueProcessor::getTypeNameChars(columnType);

		builder.set(
				MetaType::COLUMN_TYPE_NAME,
				ValueUtils::makeString(alloc, columnTypeName));

		const char8_t *columnName =
				info.getColumnName(txn, *objectManager);
		builder.set(
				MetaType::COLUMN_NAME,
				ValueUtils::makeString(alloc, columnName));

		builder.set(
				MetaType::COLUMN_KEY,
				ValueUtils::makeBool(info.isKey()));
		builder.set(
				MetaType::COLUMN_NULLABLE,
				ValueUtils::makeBool(nullable));

		if (info.isKey()) {
			keyCount++;
		}
		builder.set(
				MetaType::COLUMN_KEY_SEQUENCE,
				ValueUtils::makeShort(info.isKey() ?
						keyCount : static_cast<int16_t>(-1)));

		Value compressionRelative = ValueUtils::makeNull();
		Value compressionRate = ValueUtils::makeNull();
		Value compressionSpan = ValueUtils::makeNull();
		Value compressionWidth = ValueUtils::makeNull();
		if (compression != NULL && compression->isHiCompression(i)) {
			double threshold;
			double rate;
			double span;
			bool thresholdRelative;
			uint16_t compressionPos;
			compression->getHiCompressionProperty(
					i, threshold, rate, span, thresholdRelative,
					compressionPos);
			compressionRelative = ValueUtils::makeBool(thresholdRelative);

			if (thresholdRelative) {
				compressionRate = ValueUtils::makeDouble(rate);
				compressionSpan = ValueUtils::makeDouble(span);
			}
			else {
				compressionWidth = ValueUtils::makeDouble(threshold);
			}
		}
		builder.set(
				MetaType::COLUMN_COMPRESSION_RELATIVE,
				compressionRelative);
		builder.set(
				MetaType::COLUMN_COMPRESSION_RATE,
				compressionRate);
		builder.set(
				MetaType::COLUMN_COMPRESSION_SPAN,
				compressionSpan);
		builder.set(
				MetaType::COLUMN_COMPRESSION_WIDTH,
				compressionWidth);

		getContext().getRowHandler()(txn, builder.build());
	}
}


MetaProcessor::IndexHandler::IndexHandler(Context &cxt) :
		StoreCoreHandler(cxt) {
}

void MetaProcessor::IndexHandler::execute(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container,
		BaseContainer *subContainer) const {
	static_cast<void>(id);

	ValueListBuilder<MetaType::IndexMeta> builder(
			getContext().getValueListSource());
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	const BaseContainer &schemaContainer =
			(subContainer == NULL ? container : *subContainer);

	const char8_t *dbName;
	const char8_t *name;
	getNames(txn, container, dbName, name);

	typedef util::Vector<IndexInfo> IndexInfoList;
	IndexInfoList indexInfoList(alloc);
	{
		container.getIndexInfoList(txn, indexInfoList);
	}

	builder.set(
			MetaType::INDEX_DATABASE_ID,
			ValueUtils::makeLong(dbId));
	builder.set(
			MetaType::INDEX_DATABASE_NAME,
			ValueUtils::makeString(alloc, dbName));
	builder.set(
			MetaType::INDEX_CONTAINER_NAME,
			ValueUtils::makeString(alloc, name));

	ObjectManager *objectManager =
			container.getDataStore()->getObjectManager();

	for (IndexInfoList::const_iterator it = indexInfoList.begin();
			it != indexInfoList.end(); ++it) {
		const IndexInfo &info = *it;

		const util::String &indexName = info.indexName_;
		const Value indexNameValue = indexName.empty() ?
				ValueUtils::makeNull() :
				ValueUtils::makeString(alloc, indexName.c_str());

		const int16_t indexType = static_cast<int16_t>(info.mapType);

		builder.set(
				MetaType::INDEX_NAME,
				indexNameValue);
		builder.set(
				MetaType::INDEX_TYPE,
				ValueUtils::makeShort(indexType));
		builder.set(
				MetaType::INDEX_TYPE_NAME,
				ValueUtils::makeString(alloc, getMapTypeStr(info.mapType)));

		const util::Vector<uint32_t> &columnList = info.columnIds_;
		assert(!columnList.empty());

		for (util::Vector<uint32_t>::const_iterator columnIt =
				columnList.begin(); columnIt != columnList.end(); ++columnIt) {
			const ColumnId columnId = *columnIt;
			const char8_t *columnName = schemaContainer.getColumnInfo(
					columnId).getColumnName(txn, *objectManager);

			const int16_t ordinal =
					static_cast<int16_t>(columnIt - columnList.begin() + 1);

			builder.set(
					MetaType::INDEX_ORDINAL,
					ValueUtils::makeShort(ordinal));
			builder.set(
					MetaType::INDEX_COLUMN_NAME,
					ValueUtils::makeString(alloc, columnName));

			getContext().getRowHandler()(txn, builder.build());
		}
	}
}


MetaProcessor::MetaTriggerHandler::MetaTriggerHandler(Context &cxt) :
		StoreCoreHandler(cxt) {
}

void MetaProcessor::MetaTriggerHandler::execute(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container,
		BaseContainer *subContainer) const {
	static_cast<void>(id);
	static_cast<void>(attribute);

	ValueListBuilder<MetaType::TriggerMeta> builder(
			getContext().getValueListSource());
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	BaseContainer &schemaContainer =
			(subContainer == NULL ? container : *subContainer);

	typedef util::XArray<const uint8_t*> TriggerArray;
	TriggerArray triggerList(alloc);
	schemaContainer.getTriggerList(txn, triggerList);

	if (triggerList.empty()) {
		return;
	}

	const char8_t *dbName;
	const char8_t *name;
	getNames(txn, container, dbName, name);

	builder.set(
			MetaType::TRIGGER_DATABASE_ID,
			ValueUtils::makeLong(dbId));
	builder.set(
			MetaType::TRIGGER_DATABASE_NAME,
			ValueUtils::makeString(alloc, dbName));
	builder.set(
			MetaType::TRIGGER_CONTAINER_NAME,
			ValueUtils::makeString(alloc, name));

	ObjectManager *objectManager =
			container.getDataStore()->getObjectManager();

	for (TriggerArray::iterator triggerIt = triggerList.begin();
			triggerIt != triggerList.end(); ++triggerIt) {
		TriggerInfo info(alloc);
		TriggerInfo::decode(*triggerIt, info);

		const int16_t ordinal =
				static_cast<int16_t>(triggerIt - triggerList.begin() + 1);

		builder.set(
				MetaType::TRIGGER_ORDINAL,
				ValueUtils::makeShort(ordinal));
		builder.set(
				MetaType::TRIGGER_NAME,
				ValueUtils::makeString(alloc, info.name_.c_str()));

		bool top = true;
		uint32_t eventFlags = static_cast<uint32_t>(info.operation_);
		int32_t eventBit = 0;
		util::XArray<ColumnId>::iterator columnIt = info.columnIds_.begin();
		for (;;) {
			bool found = false;
			util::StackAllocator::Scope allocScope(alloc);

			Value triggerType = ValueUtils::makeNull();
			Value uri = ValueUtils::makeNull();
			Value jmsDestinationType = ValueUtils::makeNull();
			Value jmsDestinationName = ValueUtils::makeNull();
			Value user = ValueUtils::makeNull();
			Value password = ValueUtils::makeNull();
			if (top) {
				const bool forJms =
						(info.type_ == TriggerService::TRIGGER_JMS);
				if (forJms) {
					triggerType = ValueUtils::makeString(alloc, "JMS");
				}
				else {
					assert(info.type_ == TriggerService::TRIGGER_REST);
					triggerType = ValueUtils::makeString(alloc, "REST");
				}

				uri = ValueUtils::makeString(alloc, info.uri_.c_str());

				if (forJms) {
					jmsDestinationType = ValueUtils::makeString(
							alloc, TriggerService::jmsDestinationTypeToStr(
									info.jmsDestinationType_));
					jmsDestinationName = ValueUtils::makeString(
							alloc, info.jmsDestinationName_.c_str());
					user = ValueUtils::makeString(
							alloc, info.jmsUser_.c_str());
					password = ValueUtils::makeString(
							alloc, info.jmsPassword_.c_str());
				}
				top = false;
				found = true;
			}

			Value eventType = ValueUtils::makeNull();
			if (!found && eventBit >= 0) {
				uint32_t flag = 0;
				for (; eventFlags != 0; eventBit++) {
					flag = (1 << static_cast<uint32_t>(eventBit));
					if ((eventFlags & flag) != 0) {
						eventFlags &= ~flag;
						found = true;
						break;
					}
				}
				if (found) {
					util::String eventStr(alloc);
					TriggerHandler::convertOperationTypeToString(
							static_cast<TriggerService::OperationType>(flag),
							eventStr);
					ValueUtils::toUpperString(eventStr);
					eventType = ValueUtils::makeString(alloc, eventStr.c_str());
				}
				else {
					eventBit = -1;
				}
			}

			Value columnName = ValueUtils::makeNull();
			if (!found && columnIt != info.columnIds_.end()) {
				const ColumnInfo &columnInfo =
						schemaContainer.getColumnInfo(*columnIt);
				const char8_t *columnNameStr =
						columnInfo.getColumnName(txn, *objectManager);
				columnName = ValueUtils::makeString(alloc, columnNameStr);

				++columnIt;
				found = true;
			}

			if (!found) {
				break;
			}

			builder.set(MetaType::TRIGGER_EVENT_TYPE, eventType);
			builder.set(MetaType::TRIGGER_COLUMN_NAME, columnName);
			builder.set(MetaType::TRIGGER_TYPE, triggerType);
			builder.set(MetaType::TRIGGER_URI, uri);
			builder.set(
					MetaType::TRIGGER_JMS_DESTINATION_TYPE,
					jmsDestinationType);
			builder.set(
					MetaType::TRIGGER_JMS_DESTINATION_NAME,
					jmsDestinationName);
			builder.set(MetaType::TRIGGER_USER, user);
			builder.set(MetaType::TRIGGER_PASSWORD, password);

			getContext().getRowHandler()(txn, builder.build());
		}
	}
}


MetaProcessor::ErasableHandler::ErasableHandler(Context &cxt) :
		StoreCoreHandler(cxt), erasableTimeLimit_(MAX_TIMESTAMP) {
}

void MetaProcessor::ErasableHandler::operator()(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container) const {

	const ExpireType expireType = container.getExpireType();
	if (expireType != NO_EXPIRE) {
		execute(txn, id, dbId, attribute, container, NULL);
	}
	cxt_.stepContainerListing(id);
}

void MetaProcessor::ErasableHandler::execute(
		TransactionContext &txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer &container,
		BaseContainer *subContainer) const {
	static_cast<void>(id);

	ValueListBuilder<MetaType::ErasableMeta> builder(
			getContext().getValueListSource());
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	const BaseContainer &schemaContainer =
			(subContainer == NULL ? container : *subContainer);

	const ContainerType type = schemaContainer.getContainerType();
	const ContainerId containerId = schemaContainer.getContainerId();
	const PartitionId pId = txn.getPartitionId();

	BaseContainer *archiveContainer = const_cast<BaseContainer *>(&schemaContainer);
	const FullContainerKey &cotainerKey = archiveContainer->getContainerKey(txn);
	FullContainerKeyComponents component = cotainerKey.getComponents(alloc);
//	const ContainerId largeContainerId = component.largeContainerId_;
	uint32_t schemaVersionId = schemaContainer.getVersionId();
	int64_t initSchemaStatus = schemaContainer.getInitSchemaStatus();

	const char8_t *dbName;
	const char8_t *name;
	getNames(txn, container, dbName, name);

	ExpireType expireType = container.getExpireType();

	builder.set(
			MetaType::ERASABLE_DATABASE_ID,
			ValueUtils::makeLong(dbId));
	builder.set(
			MetaType::ERASABLE_DATABASE_NAME,
			ValueUtils::makeString(alloc, dbName));
	builder.set(
			MetaType::ERASABLE_TYPE_NAME,
			ValueUtils::makeString(alloc, containerTypeToName(type)));
	builder.set(
			MetaType::ERASABLE_CONTAINER_ID,
			ValueUtils::makeLong(containerId));
		builder.set(
				MetaType::ERASABLE_CONTAINER_NAME,
				ValueUtils::makeString(alloc, name));
		builder.set(
				MetaType::ERASABLE_PARTITION_NAME,
				ValueUtils::makeNull());
	builder.set(
			MetaType::ERASABLE_CLUSTER_PARTITION,
			ValueUtils::makeInteger(pId));
		builder.set(
				MetaType::ERASABLE_LARGE_CONTAINER_ID,
				ValueUtils::makeNull());
	builder.set(
			MetaType::ERASABLE_SCHEMA_VERSION_ID,
			ValueUtils::makeInteger(schemaVersionId));
	builder.set(
			MetaType::ERASABLE_INIT_SCHEMA_STATUS,
			ValueUtils::makeLong(initSchemaStatus));
	builder.set(
			MetaType::ERASABLE_EXPIRATION_TYPE,
			ValueUtils::makeString(alloc, expirationTypeToName(expireType)));
	util::XArray< BaseContainer::ArchiveInfo > erasableList(alloc);
	container.getErasableList(txn, erasableTimeLimit_, erasableList);
	util::XArray< BaseContainer::ArchiveInfo >::iterator itr;
	for (itr = erasableList.begin(); itr != erasableList.end(); itr++) {
		builder.set(
				MetaType::ERASABLE_LOWER_BOUNDARY_TIME,
				ValueUtils::makeTimestamp(itr->start_));
		builder.set(
				MetaType::ERASABLE_UPPER_BOUNDARY_TIME,
				ValueUtils::makeTimestamp(itr->end_));
		builder.set(
				MetaType::ERASABLE_EXPIRATION_TIME,
				ValueUtils::makeTimestamp(itr->expired_));
		builder.set(
				MetaType::ERASABLE_ERASABLE_TIME,
				ValueUtils::makeTimestamp(itr->erasable_));
		builder.set(
				MetaType::ERASABLE_ROW_INDEX_OID,
				ValueUtils::makeLong(itr->rowIdMapOId_));
		builder.set(
				MetaType::ERASABLE_MVCC_INDEX_OID,
				ValueUtils::makeLong(itr->mvccMapOId_));

		getContext().getRowHandler()(txn, builder.build());
	}
}

const char8_t* MetaProcessor::ErasableHandler::containerTypeToName(
		ContainerType type) {
	return ContainerHandler::containerTypeToName(type);
}

const char8_t* MetaProcessor::ErasableHandler::expirationTypeToName(
		ExpireType type) {
	switch (type) {
	case ROW_EXPIRE:
		return ContainerHandler::expirationTypeToName(
				ContainerHandler::META_EXPIRATION_TYPE_ROW);
	default:
		assert(false);
		return "";
	}
}



MetaProcessor::KeyRefHandler::KeyRefHandler(
		TransactionContext &txn, const MetaContainerInfo &refInfo,
		RowHandler &rowHandler) :
		RefHandler(txn, refInfo, rowHandler) {
	assert(refInfo.id_ == MetaType::TYPE_KEY);
}

bool MetaProcessor::KeyRefHandler::filter(
		TransactionContext &txn, const ValueList &valueList) {
	static_cast<void>(txn);

	assert(MetaType::COLUMN_KEY < valueList.size());
	const Value &keyValue = valueList[MetaType::COLUMN_KEY];

	assert(keyValue.getType() == COLUMN_TYPE_BOOL);
	return !keyValue.getBool();
}


MetaProcessorSource::MetaProcessorSource(
		DatabaseId dbId, const char8_t *dbName) :
		dbId_(dbId),
		dbName_(dbName),
		eventContext_(NULL),
		transactionManager_(NULL),
		partitionTable_(NULL) {
}


MetaContainer::MetaContainer(
		TransactionContext &txn, DataStore *dataStore, DatabaseId dbId,
		MetaContainerId id, NamingType containerNamingType,
		NamingType columnNamingType) :
		BaseContainer(txn, dataStore),
		info_(MetaType::InfoTable::getInstance().resolveInfo(id, false)),
		containerNamingType_(containerNamingType),
		columnNamingType_(columnNamingType),
		dbId_(dbId),
		columnInfoList_(NULL) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	baseContainerImage_ = ALLOC_NEW(alloc) BaseContainerImage;
	memset(baseContainerImage_, 0, sizeof(BaseContainerImage));
	baseContainerImage_->containerId_ = UNDEF_CONTAINERID;
	baseContainerImage_->versionId_ = info_.versionId_;
	baseContainerImage_->containerType_ = COLLECTION_CONTAINER;

	columnInfoList_ = ALLOC_NEW(alloc) util::XArray<ColumnInfo>(alloc);
}

MetaContainerId MetaContainer::getMetaContainerId() const {
	return info_.id_;
}

MetaType::NamingType MetaContainer::getContainerNamingType() const {
	return containerNamingType_;
}

MetaType::NamingType MetaContainer::getColumnNamingType() const {
	return columnNamingType_;
}

FullContainerKey MetaContainer::getContainerKey(TransactionContext &txn) {
	const char8_t *name =
			MetaType::InfoTable::nameOf(info_, containerNamingType_, false);
	FullContainerKeyComponents components;
	components.dbId_ = dbId_;
	components.systemPart_ = name;
	components.systemPartSize_ = strlen(name);
	return FullContainerKey(
			txn.getDefaultAllocator(),
			KeyConstraint::getNoLimitKeyConstraint(), components);
}

const MetaContainerInfo& MetaContainer::getMetaContainerInfo() const {
	return info_;
}

void MetaContainer::getContainerInfo(
		TransactionContext &txn,
		util::XArray<uint8_t> &containerSchema, bool optionIncluded, bool internalOptionIncluded) {

	uint32_t columnNum = getColumnNum();
	containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&columnNum), sizeof(uint32_t));

	for (uint32_t i = 0; i < columnNum; i++) {
		getColumnSchema(
				txn, i, *getObjectManager(), containerSchema);
	}

	{
		util::XArray<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
		getKeyColumnIdList(keyColumnIdList);
		int16_t rowKeyNum = static_cast<int16_t>(keyColumnIdList.size());
		containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&rowKeyNum), sizeof(int16_t));
		util::XArray<ColumnId>::iterator itr;
		for (itr = keyColumnIdList.begin(); itr != keyColumnIdList.end(); itr++) {
			int16_t rowKeyColumnId = static_cast<int16_t>(*itr);
			containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&rowKeyColumnId), sizeof(int16_t));
		}
	}

	if (optionIncluded) {
		getCommonContainerOptionInfo(containerSchema);
		getContainerOptionInfo(txn, containerSchema);
		if (internalOptionIncluded) {
			int32_t containerAttribute = getAttribute();
			containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&containerAttribute), sizeof(int32_t));
			TablePartitioningVersionId tablePartitioningVersionId = -1;
			containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&tablePartitioningVersionId),
				sizeof(TablePartitioningVersionId));
			int32_t elapsedTime = -1;
			TimeUnit timeUnit = UINT8_MAX;
			int64_t startValue = -1;
			int64_t limitValue = -1;

			containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&(elapsedTime)),
				sizeof(int32_t));
			int8_t tmp = static_cast<int8_t>(timeUnit);
			containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&tmp), sizeof(int8_t));
			containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&startValue),
				sizeof(Timestamp));
			containerSchema.push_back(
				reinterpret_cast<const uint8_t *>(&(limitValue)),
				sizeof(int64_t));
		}
	}
}

void MetaContainer::getIndexInfoList(
		TransactionContext &txn, util::Vector<IndexInfo> &indexInfoList) {
	static_cast<void>(txn);
	static_cast<void>(indexInfoList);
}

uint32_t MetaContainer::getColumnNum() const {
	return static_cast<uint32_t>(info_.columnCount_);
}

void MetaContainer::getKeyColumnIdList(
		util::XArray<ColumnId> &keyColumnIdList) {
	static_cast<void>(keyColumnIdList);
}

void MetaContainer::getCommonContainerOptionInfo(
		util::XArray<uint8_t> &containerSchema) {

	const char8_t *affinityStr = "";
	int32_t affinityStrLen = static_cast<int32_t>(strlen(affinityStr));
	containerSchema.push_back(
			reinterpret_cast<uint8_t*>(&affinityStrLen), sizeof(int32_t));
	containerSchema.push_back(
			reinterpret_cast<const uint8_t*>(affinityStr), affinityStrLen);
}

void MetaContainer::getColumnSchema(
		TransactionContext &txn, uint32_t columnId,
		ObjectManager &objectManager, util::XArray<uint8_t> &schema) {

	const char8_t *columnName = const_cast<char *>(
			getColumnName(txn, columnId, objectManager));
	int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
	schema.push_back(
			reinterpret_cast<uint8_t*>(&columnNameLen), sizeof(int32_t));
	schema.push_back(
			reinterpret_cast<const uint8_t*>(columnName), columnNameLen);

	int8_t tmp = static_cast<int8_t>(getSimpleColumnType(columnId));
	schema.push_back(reinterpret_cast<uint8_t*>(&tmp), sizeof(int8_t));

	uint8_t flag = (isArrayColumn(columnId) ? 1 : 0);
	flag |= (isVirtualColumn(columnId) ? ColumnInfo::COLUMN_FLAG_VIRTUAL : 0);
	flag |= (isNotNullColumn(columnId) ? ColumnInfo::COLUMN_FLAG_NOT_NULL : 0);
	schema.push_back(reinterpret_cast<uint8_t*>(&flag), sizeof(uint8_t));
}

const char8_t* MetaContainer::getColumnName(
		TransactionContext &txn, uint32_t columnId,
		ObjectManager &objectManager) const {
	static_cast<void>(txn);
	static_cast<void>(objectManager);
	assert(columnId < info_.columnCount_);
	return MetaType::InfoTable::nameOf(
			info_.columnList_[columnId], columnNamingType_, false);
}

ColumnType MetaContainer::getSimpleColumnType(uint32_t columnId) const {
	assert(columnId < info_.columnCount_);
	return info_.columnList_[columnId].type_;
}

bool MetaContainer::isArrayColumn(uint32_t columnId) const {
	static_cast<void>(columnId);
	return false;
}

bool MetaContainer::isVirtualColumn(uint32_t columnId) const {
	static_cast<void>(columnId);
	return false;
}

bool MetaContainer::isNotNullColumn(uint32_t columnId) const {
	assert(columnId < info_.columnCount_);
	return !info_.columnList_[columnId].nullable_;
}

void MetaContainer::getTriggerList(
		TransactionContext &txn, util::XArray<const uint8_t*> &triggerList) {
	static_cast<void>(txn);
	static_cast<void>(triggerList);
}

ContainerAttribute MetaContainer::getAttribute() const {
	return CONTAINER_ATTR_SINGLE;
}

void MetaContainer::getNullsStats(util::XArray<uint8_t> &nullsList) const {
	static_cast<void>(nullsList);
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
}

void MetaContainer::getColumnInfoList(
		util::XArray<ColumnInfo> &columnInfoList) const {

	uint32_t columnCount = getColumnNum();
	uint16_t variableColumnIndex = 0;
	uint32_t nullsAndVarOffset =
			ValueProcessor::calcNullsByteSize(columnCount);
	uint32_t rowFixedSize = 0;
	for (uint32_t i = 0; i < columnCount; i++) {
		ColumnInfo info;
		info.initialize();
		info.setType(getSimpleColumnType(i), false);
		if (info.isVariable()) {
			info.setOffset(variableColumnIndex);
			variableColumnIndex++;
		}
		else {
			rowFixedSize += info.getColumnSize();
		}
		columnInfoList.push_back(info);
	}
	if (variableColumnIndex > 0) {
		nullsAndVarOffset += sizeof(OId);
		rowFixedSize = 0;
		for (uint32_t i = 0; i < columnCount; i++) {
			ColumnInfo &info = columnInfoList[i];
			if (!info.isVariable()) {
				info.setOffset(nullsAndVarOffset + rowFixedSize);
				rowFixedSize += info.getColumnSize();
			}
		}
	}
}

void MetaContainer::getColumnInfo(
		TransactionContext &txn, ObjectManager &objectManager,
		const char8_t *name, uint32_t &columnId, ColumnInfo *&columnInfo,
		bool isCaseSensitive) const {

	columnId = UNDEF_COLUMNID;
	columnInfo = NULL;

	const uint32_t columnNum = getColumnNum();
	for (uint32_t i = 0; i < columnNum; i++) {
		const char8_t *columnName = getColumnName(txn, i, objectManager);
		uint32_t columnNameSize = static_cast<uint32_t>(strlen(columnName));
		bool isExist = eqCaseStringString(
				txn, name, static_cast<uint32_t>(strlen(name)),
				columnName, columnNameSize, isCaseSensitive);
		if (isExist) {
			columnId = i;
			columnInfo = &getColumnInfo( i);
			return;
		}
	}
}

ColumnInfo& MetaContainer::getColumnInfo(uint32_t columnId) const {
	if (columnInfoList_->empty()) {
		util::XArray<ColumnInfo> columnInfoList(
				*columnInfoList_->get_allocator().base());
		getColumnInfoList(columnInfoList);
		columnInfoList_->swap(columnInfoList);
	}

	return (*columnInfoList_)[columnId];
}

void MetaContainer::getContainerOptionInfo(
		TransactionContext &txn, util::XArray<uint8_t> &containerSchema) {
	static_cast<void>(txn);
	static_cast<void>(containerSchema);
}


MetaStore::MetaStore(DataStore &dataStore) : dataStore_(dataStore) {
}

MetaContainer* MetaStore::getContainer(
		TransactionContext &txn, const FullContainerKey &key,
		MetaType::NamingType defaultNamingType) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	FullContainerKeyComponents components =
			key.getComponents(alloc);
	if (components.systemPartSize_ == 0) {
		return NULL;
	}

	const DatabaseId dbId = components.dbId_;
	const util::String systemName(
			components.systemPart_, components.systemPartSize_,
			alloc);

	components = FullContainerKeyComponents();
	components.dbId_ = dbId;
	components.systemPart_ = systemName.c_str();
	components.systemPartSize_ = systemName.size();
	const FullContainerKey systemKey(
			alloc, KeyConstraint::getNoLimitKeyConstraint(), components);

	if (key.compareTo(alloc, systemKey, true) != 0) {
		return NULL;
	}

	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	MetaType::NamingType namingType;
	const MetaContainerInfo *info =
			infoTable.findInfo(systemName.c_str(), false, namingType);
	if (info == NULL) {
		return NULL;
	}

	const MetaType::NamingType columnNamingType =
			MetaType::InfoTable::resolveNaming(namingType, defaultNamingType);
	return ALLOC_NEW(alloc) MetaContainer(
			txn, &dataStore_, dbId, info->id_, namingType, columnNamingType);
}

MetaContainer* MetaStore::getContainer(
		TransactionContext &txn, DatabaseId dbId, MetaContainerId id,
		MetaType::NamingType containerNamingType,
		MetaType::NamingType columnNamingType) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	return ALLOC_NEW(alloc) MetaContainer(
			txn, &dataStore_, dbId, id, containerNamingType, columnNamingType);
}
