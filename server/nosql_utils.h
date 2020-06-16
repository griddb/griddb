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
#ifndef NOSQL_UTILS_H_
#define NOSQL_UTILS_H_

#include "sql_table_schema.h"

class NoSQLDB;
class NoSQLContainer;
struct NoSQLStoreOption;
class OutputMessageRowStore;
class DataStoreValueLimitConfig;
class InputMessageRowStore;
class DataStore;

struct LargeExecStatus {

	LargeExecStatus(util::StackAllocator &alloc) :
			alloc_(alloc),
			currentStatus_(PARTITION_STATUS_NONE),
			affinityNumber_(UNDEF_NODE_AFFINITY_NUMBER),
			indexInfo_(alloc) {}

	void reset() {

		currentStatus_ = PARTITION_STATUS_NONE;
		affinityNumber_ = UNDEF_NODE_AFFINITY_NUMBER;
		
		indexInfo_.indexName_.clear();
		indexInfo_.mapType = MAP_TYPE_DEFAULT;
		indexInfo_.columnIds_.clear();
	}

	util::StackAllocator &alloc_;
	LargeContainerStatusType currentStatus_;
	NodeAffinityNumber affinityNumber_;
	IndexInfo indexInfo_;
};

struct NoSQLUtils {
	
	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

	static const uint8_t INDEX_META_INDEX_TYPE_DEFAULT =	MAP_TYPE_HASH;
	static const ColumnId INDEX_META_COLID_INDEX_KEY_NAME = 0;
	static const ColumnId INDEX_META_COLID_INDEX_NAME = 1;
	static const ColumnId INDEX_META_COLID_TABLE_NAME = 2;
	static const ColumnId INDEX_META_COLID_TABLE_ID = 3;
	static const ColumnId INDEX_META_COLID_SCHEMA_VERSION = 4;
	static const ColumnId INDEX_META_COLID_COLUMN_ID = 5;
	static const ColumnId INDEX_META_COLID_COLUMN_NAME = 6;
	static const ColumnId INDEX_META_COLID_INDEX_TYPE = 7;
	static const ColumnId INDEX_META_COLID_OPTION = 8;
	static const ColumnId INDEX_META_COLID_MAX = 9;

	static const ColumnId DATABASE_META_COLID_DB_NAME = 0;
	static const ColumnId DATABASE_META_COLID_DB_ID = 1;
	static const ColumnId DATABASE_META_COLID_CONTAINER_NAME = 2;
	static const ColumnId DATABASE_META_COLID_CONTAINER_TYPE = 3;
	static const uint8_t NULLABLE_MASK = 0x80;
	static const char8_t *const LARGE_CONTAINER_KEY_VERSION;
	static const char8_t *const LARGE_CONTAINER_KEY_PARTITIONING_INFO;
	static const char8_t *const LARGE_CONTAINER_KEY_EXECUTING;
	static const char8_t *const LARGE_CONTAINER_KEY_INDEX;
	static const char8_t *const LARGE_CONTAINER_KEY_PARTITIONING_ASSIGN_INFO;
	static const char8_t *const LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA;
	static const char8_t *const LARGE_CONTAINER_KEY_VIEW_INFO;
	static const char8_t *const LARGE_CONTAINER_KEY_TABLE_PROPERTY;

	void checkWritableContainer();

	static PartitionId resolvePartitionId(
			util::StackAllocator &alloc,
			PartitionId partitionCount,
			const FullContainerKey &containerKey,
			ContainerHashMode hashMode
					= CONTAINER_HASH_MODE_CRC32);

	template<typename T>
	static void normalizeString(T &str, const char *src) {
		
		str.resize(strlen(src) + 1);
		char c;
		
		for (uint32_t i = 0; i < strlen(src); i++) {
			c = *(src + i);
			
			if ((c >= 'a') && (c <= 'z')){
				str[i] = static_cast<char>(c - 32);
			}
			else {
				str[i] = c;
			}
		}
	}

	static void getAffinityValue(
			CreateTableOption &createOpton,
			util::String &value);

	static void makeLargeContainerColumn(
			util::XArray<ColumnInfo> &columnInfoList);

	template<typename Alloc>
	static void makeNormalContainerSchema(
			util::StackAllocator &alloc,
			CreateTableOption &createOption,
			util::XArray<uint8_t> &containerSchema,
			util::XArray<uint8_t> &optionList,
			TablePartitioningInfo<Alloc> &partitioningInfo);

	static void makeContainerColumns(
			util::StackAllocator &alloc,
			util::XArray<uint8_t> &containerSchema,
			util::Vector<util::String> &columnNameList,
			util::Vector<ColumnType> &columnTypeList,
			util::Vector<uint8_t> &columnOptionList);

	static void makeLargeContainerSchema(
			util::XArray<uint8_t> &binarySchema,
			bool isView, util::String &affinityValue);
	
	static void checkSchemaValidation(
			util::StackAllocator &alloc,
			const DataStoreValueLimitConfig &config,
			const char *containerName, 
			util::XArray<uint8_t> &binarySchema,
			ContainerType containerType);

	static int32_t getColumnId(
			util::StackAllocator &alloc,
			util::Vector<util::String> columnNameList,
			const NameWithCaseSensitivity &columnName);

	template<typename T>
	static void makeLargeContainerRow(
			util::StackAllocator &alloc,
			const char *key,
			OutputMessageRowStore &outputMrs,
			T &targetValue);

	static void makeLargeContainerRowBinary(
			const char *key,
			OutputMessageRowStore &outputMrs,
			util::XArray<uint8_t> &targetValue);

	static void checkPrimaryKey(
			util::StackAllocator &alloc,
			CreateTableOption &option,
			util::Vector<ColumnId> &columnIds);

	static void decodePartitioningTableIndexInfo(
			util::StackAllocator &alloc,
			InputMessageRowStore &rowStore,
			util::Vector<IndexInfo> &indexInfoList);

	static bool checkAcceptableTupleType(
			TupleList::TupleColumnType type);

	template<typename T>
	static void initializeBits(
			T &statList, size_t columnCount) {

		if (statList.empty()) {
			const uint8_t unitBits = 0;
			
			statList.assign(
					(columnCount + CHAR_BIT - 1) / CHAR_BIT,
					unitBits);
		}
	}

	static size_t getFixedSize(ColumnType type) {

		switch (type) {
			case COLUMN_TYPE_BOOL: return sizeof(bool);
			case COLUMN_TYPE_BYTE: return sizeof(int8_t);
			case COLUMN_TYPE_SHORT: return sizeof(int16_t);
			case COLUMN_TYPE_INT: return sizeof(int32_t);
			case COLUMN_TYPE_LONG: return sizeof(int64_t);
			case COLUMN_TYPE_FLOAT: return sizeof(float);
			case COLUMN_TYPE_DOUBLE: return sizeof(double);
			case COLUMN_TYPE_TIMESTAMP: return sizeof(int64_t);
			case COLUMN_TYPE_GEOMETRY: return sizeof(double);
			case COLUMN_TYPE_NULL: return sizeof(int64_t);			
				break;
			default:
				return static_cast<size_t>(-1);
		}
	}

	static MapType getAvailableIndex(
			DataStore *dataStore,
			const char *indexName,
			ColumnType targetColumnType,
			ContainerType targetContainerType,
			bool primaryCheck);

	static void dumpRecoverContainer(
			util::StackAllocator &alloc,
			const char *tableName,
			NoSQLContainer *container);

	static const char *getParitionStatusName(
			LargeContainerStatusType type) {

		switch (type) {
			case PARTITION_STATUS_CREATE_START: return "CREATING PARTITION";
			case PARTITION_STATUS_CREATE_END: return "NORMAL";
			case PARTITION_STATUS_DROP_START: return "DROPPING PARTITION";
			case PARTITION_STATUS_DROP_END: return "REMOVED";
			case INDEX_STATUS_CREATE_START: return "CREATING INDEX";
			case INDEX_STATUS_CREATE_END: return "NORMAL";
			case INDEX_STATUS_DROP_START: return "DROPPING INDEX";
			case INDEX_STATUS_DROP_END: return "NORMAL";
			default: return "NONE";
		}
	}

	static TupleList::TupleColumnType
		convertNoSQLTypeToTupleType(ColumnType type) {

		switch (type) {
			case COLUMN_TYPE_BOOL : return TupleList::TYPE_BOOL;
			case COLUMN_TYPE_BYTE : return TupleList::TYPE_BYTE;
			case COLUMN_TYPE_SHORT: return TupleList::TYPE_SHORT;
			case COLUMN_TYPE_INT : return TupleList::TYPE_INTEGER;
			case COLUMN_TYPE_LONG : return TupleList::TYPE_LONG;
			case COLUMN_TYPE_FLOAT : return TupleList::TYPE_FLOAT;
			case COLUMN_TYPE_DOUBLE : return TupleList::TYPE_DOUBLE;
			case COLUMN_TYPE_TIMESTAMP : return TupleList::TYPE_TIMESTAMP;
			case COLUMN_TYPE_NULL : return TupleList::TYPE_NULL;
			case COLUMN_TYPE_STRING : return TupleList::TYPE_STRING;
			case COLUMN_TYPE_BLOB : return  TupleList::TYPE_BLOB;
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
				return TupleList::TYPE_ANY;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
						"Unsupported type, type=" << static_cast<int32_t>(type));
			}
	}

	static ColumnType convertTupleTypeToNoSQLTypeWithNullable(
			TupleList::TupleColumnType type) {

		ColumnType returnType;
		
		TupleList::TupleColumnType tmpType
				= static_cast<TupleList::TupleColumnType>(
						(type & ~TupleList::TYPE_MASK_NULLABLE));
		
		switch (tmpType) {
			
			case TupleList::TYPE_BOOL:
					returnType = COLUMN_TYPE_BOOL;
			break;

			case TupleList::TYPE_BYTE:
					returnType = COLUMN_TYPE_BYTE;
			break;
			
			case TupleList::TYPE_SHORT:
					returnType = COLUMN_TYPE_SHORT;
			break;

			case TupleList::TYPE_INTEGER:
					returnType = COLUMN_TYPE_INT;
			break;

			case TupleList::TYPE_LONG:
					returnType = COLUMN_TYPE_LONG;
			break;

			case TupleList::TYPE_FLOAT:
					returnType = COLUMN_TYPE_FLOAT;
			break;
			
			case TupleList::TYPE_NUMERIC:
					returnType = COLUMN_TYPE_DOUBLE;
			break;

			case TupleList::TYPE_DOUBLE:
					returnType = COLUMN_TYPE_DOUBLE;
			break;

			case TupleList::TYPE_TIMESTAMP:
					returnType = COLUMN_TYPE_TIMESTAMP;
			break;
			
			case TupleList::TYPE_NULL:
					returnType = COLUMN_TYPE_NULL;
			break;
			
			case TupleList::TYPE_STRING:
					returnType = COLUMN_TYPE_STRING;
			break;
			
			case TupleList::TYPE_GEOMETRY:
					returnType = COLUMN_TYPE_GEOMETRY;
			break;
			
			case TupleList::TYPE_BLOB:
						returnType = COLUMN_TYPE_BLOB;
			break;
			
			case TupleList::TYPE_ANY:
					returnType = COLUMN_TYPE_ANY;
			break;
			
			default:
				GS_THROW_USER_ERROR(
						GS_ERROR_NOSQL_INTERNAL,
						"Unsupported type, type="
						<< static_cast<int32_t>(type));
		}

		if (TupleColumnTypeUtils::isNullable(type)) {
			returnType |= NULLABLE_MASK;
		}

		return returnType;
	}



	static bool isVariableType(ColumnType type) {

		if (type == COLUMN_TYPE_ANY) {
			return true;
		}

		ColumnType tmp = static_cast<ColumnType>(
				type & ~NULLABLE_MASK);

		switch (tmp) {
			case COLUMN_TYPE_STRING:
			case COLUMN_TYPE_GEOMETRY:
			case COLUMN_TYPE_BLOB:
				return true;
			default:
				return false;
		}
	};

	static bool isAccessibleContainer(
			ContainerAttribute attribute, bool &isWritable) {

		switch (attribute) {
			
			case CONTAINER_ATTR_SINGLE:
			case CONTAINER_ATTR_LARGE:
			case CONTAINER_ATTR_VIEW:
				isWritable = true;
				return true;

			default:
				return false;
			}
	}

	static bool isWritableContainer(
			ContainerAttribute attribute) {

		switch (attribute) {
			
			case CONTAINER_ATTR_SINGLE:
			case CONTAINER_ATTR_LARGE:
			case CONTAINER_ATTR_VIEW:
				return true;
			
			default:
				return false;
			}
	}

	static int32_t mapTypeToSQLIndexFlags(
			int8_t mapType,
			uint8_t nosqlColumnType) {

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

						flags |= mapTypeToSQLIndexFlags(
								MAP_TYPE_BTREE, nosqlColumnType);

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


	static void checkConnectedDbName(
			util::StackAllocator &alloc,
			const char *connectedDb,
			const char *currentDb,
			bool isCaseSensitive);

	static bool isDenyException(int32_t errorCode) {

		return (
				errorCode == GS_ERROR_TXN_PARTITION_ROLE_UNMATCH ||
				errorCode == GS_ERROR_TXN_PARTITION_STATE_UNMATCH ||
				errorCode == GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH ||
				errorCode == GS_ERROR_DS_COL_LOCK_CONFLICT ||
				errorCode == GS_ERROR_DS_TIM_LOCK_CONFLICT ||
				errorCode == GS_ERROR_NOSQL_FAILOVER_TIMEOUT ||
				errorCode == GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN ||
				errorCode == GS_ERROR_TXN_REAUTHENTICATION_FIRED
		);
	}

	static TupleList::TupleColumnType setColumnTypeNullable(
			TupleList::TupleColumnType type,
			bool nullable) {

		if (TupleColumnTypeUtils::isNull(type)
				|| TupleColumnTypeUtils::isAny(type)) {
			return type;
		}

		if (nullable) {
			return static_cast<TupleList::TupleColumnType>(
					type | TupleList::TYPE_MASK_NULLABLE);
		}
		else {
			return static_cast<TupleList::TupleColumnType>(
					type & ~TupleList::TYPE_MASK_NULLABLE);
		}
	}

	template<typename Alloc>
	static bool execPartitioningOperation(
			EventContext &ec,
			NoSQLDB *db,
			LargeContainerStatusType targetOperation,
			LargeExecStatus &status,
			NoSQLContainer &container,
			NodeAffinityNumber baseAffinity,
			NodeAffinityNumber targetAffinity,
			NoSQLStoreOption &option,
			TablePartitioningInfo<Alloc> &partitioningInfo,
			const NameWithCaseSensitivity &tableName,
			SQLExecution *execution,
			TargetContainerInfo &targetContainerInfo);

	static void resolveTargetContainer(
			EventContext &ec,
			const TupleValue *value1,
			const TupleValue *value2, 
			NoSQLDB *db,
			TableSchemaInfo *origTableSchema,
			SQLExecution *execution,
			NameWithCaseSensitivity &dbName,
			NameWithCaseSensitivity &tableName, 
			TargetContainerInfo &targetInfo,
			bool &needRefresh);

	template<typename T>
	static void getTablePartitioningInfo(
			util::StackAllocator &alloc,
			DataStore *ds,
			NoSQLContainer &container,
			util::XArray<ColumnInfo> &columnInfoList,
			T &partitioningInfo,
			NoSQLStoreOption &option);

	static NoSQLContainer *createNoSQLContainer(
			EventContext &ec,
			const NameWithCaseSensitivity tableName,
			ContainerId largeContainerId,
			NodeAffinityNumber affnitiyNumber,
			SQLExecution *execution);

	template<typename T>
	static void getLargeContainerInfo(
			util::StackAllocator &alloc,
			const char *key, 
			DataStore *ds,
			NoSQLContainer &container,
			util::XArray<ColumnInfo> &columnInfoList,
			T &partitioningInfo);

	template<typename T>
	static void getLargeContainerInfoBinary(
			util::StackAllocator &alloc,
			const char *key,
			DataStore *ds,
			NoSQLContainer &container,
			util::XArray<ColumnInfo> &columnInfoList,
			T &partitioningInfo);

	template<typename T>
	static void decodeRow(
			InputMessageRowStore &rowStore,
			T &record,
			VersionInfo &versionInfo,
			const char *rowKey);

	template<typename T>
	static void decodeBinaryRow(
			InputMessageRowStore &rowStore,
			T &record,
			VersionInfo &versionInfo,
			const char *rowKey);
};

#endif
