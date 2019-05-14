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
	@brief Definition that are widely needed in DataStore, Container,
   ObjectManager
*/
#ifndef DATA_STORE_COMMON_H_
#define DATA_STORE_COMMON_H_

#include "data_type.h"
#include "gs_error.h"


class ObjectManager;

static const uint32_t VAR_SIZE_1BYTE_THRESHOLD = 128;
static const uint32_t VAR_SIZE_4BYTE_THRESHOLD = UINT32_C(1) << 30;
static const uint64_t VAR_SIZE_8BYTE_THRESHOLD = UINT64_C(1) << 62;

static const uint32_t NEXT_OBJECT_LINK_INFO_SIZE = 8;  
static const uint32_t LINK_VARIABLE_COLUMN_DATA_SIZE =
	sizeof(uint32_t) + sizeof(uint64_t);  
static const uint32_t AFFINITY_STRING_MAX_LENGTH =
	8;  
static const char *const DEFAULT_AFFINITY_STRING =
	"";  

static const Timestamp MINIMUM_EXPIRED_TIMESTAMP = -1;

typedef uint8_t RowHeader;

const uint8_t DS_CHUNK_EXP_SIZE = 16;
const ChunkCategoryId DS_CHUNK_CATEGORY_SIZE =
	5;  
const bool DS_CHUNK_CATEGORY_RANGE_BATCH_FREE[] = {
	false, false, false, true, true};
const int16_t DS_ADDITIONAL_CHUNK_CATEGORY_NUM =
	17;  

static const uint32_t CHUNK_SIZE = 1 << DS_CHUNK_EXP_SIZE;  
static const uint32_t OBJECT_BLOCK_HEADER_SIZE =
	4;  

static const ChunkCategoryId ALLOCATE_META_CHUNK =
	0;  
static const ChunkCategoryId ALLOCATE_NO_EXPIRE_MAP = 1;  
static const ChunkCategoryId ALLOCATE_NO_EXPIRE_ROW = 2;  
static const ChunkCategoryId ALLOCATE_EXPIRE_MAP = 3;	 
static const ChunkCategoryId ALLOCATE_EXPIRE_ROW = 4;	 

static const AffinityGroupId DEFAULT_AFFINITY_GROUP_ID = 0;

static const ExpireIntervalCategoryId DEFAULT_EXPIRE_CATEGORY_ID = 0;

/*!
	@brief Strategy for allocating object
*/
struct AllocateStrategy {
	ChunkKey chunkKey_;
	ChunkCategoryId categoryId_;
	ExpireIntervalCategoryId expireCategoryId_; 
	AffinityGroupId affinityGroupId_;
	AllocateStrategy()
		: chunkKey_(UNDEF_CHUNK_KEY),
		  categoryId_(ALLOCATE_META_CHUNK),
		  expireCategoryId_(DEFAULT_EXPIRE_CATEGORY_ID),
		  affinityGroupId_(DEFAULT_AFFINITY_GROUP_ID) {}
	AllocateStrategy(ChunkCategoryId categoryId)
		: chunkKey_(UNDEF_CHUNK_KEY),
		  categoryId_(categoryId),
		  expireCategoryId_(DEFAULT_EXPIRE_CATEGORY_ID),
		  affinityGroupId_(DEFAULT_AFFINITY_GROUP_ID) {}
	AllocateStrategy(ChunkCategoryId categoryId, AffinityGroupId groupId)
		: chunkKey_(UNDEF_CHUNK_KEY),
		  categoryId_(categoryId),
		  expireCategoryId_(DEFAULT_EXPIRE_CATEGORY_ID),
		  affinityGroupId_(groupId) {}
	AllocateStrategy(ChunkCategoryId categoryId, AffinityGroupId groupId, 
		ChunkKey chunkKey, ExpireIntervalCategoryId expireCategoryId)
		: chunkKey_(chunkKey),
		  categoryId_(categoryId),
		  expireCategoryId_(expireCategoryId),
		  affinityGroupId_(groupId) {}
};

static const uint32_t LIMIT_EXPIRATION_DIVIDE_NUM = 160;  
static const uint32_t LIMIT_HICOMPRESSION_COLUMN_NUM =
	100;  
static const uint32_t LIMIT_COLUMN_NAME_SIZE = 256;  

const ResultSize PARTIAL_RESULT_SIZE =
	1 * 1000;  

typedef int8_t ObjectType;
const ObjectType OBJECT_TYPE_UNKNOWN = 0;
const ObjectType OBJECT_TYPE_CHUNK_HEADER = 1;
const ObjectType OBJECT_TYPE_COLLECTION = 2;
const ObjectType OBJECT_TYPE_TIME_SERIES = 3;
const ObjectType OBJECT_TYPE_COLUMNINFO = 4;
const ObjectType OBJECT_TYPE_ROW = 5;
const ObjectType OBJECT_TYPE_TIME_SERIES_ROW = 6;
const ObjectType OBJECT_TYPE_ROW_ARRAY = 7;
const ObjectType OBJECT_TYPE_BTREE_MAP = 8;
const ObjectType OBJECT_TYPE_HASH_MAP = 9;
const ObjectType OBJECT_TYPE_COMPRESSIONINFO = 10;
const ObjectType OBJECT_TYPE_EVENTLIST = 11;
const ObjectType OBJECT_TYPE_VARIANT = 12;
const ObjectType OBJECT_TYPE_CONTAINER_ID = 13;
const ObjectType OBJECT_TYPE_RTREE_MAP = 14;
const ObjectType OBJECT_TYPE_DSDC_VAL = 15;
const ObjectType OBJECT_TYPE_VALUE_LIST = 16;
const ObjectType OBJECT_TYPE_UNDEF = INT8_MAX;

/*!
	@brief Represents the mode of access for Object
*/
enum AccessMode { OBJECT_READ_ONLY = 0, OBJECT_FOR_UPDATE = 1 };

/*!
	@brief Represents the mode of checkpoint
*/
enum CheckpointMode {
	CP_UNDEF,
	CP_NORMAL,
	CP_REQUESTED,
	CP_BACKUP,
	CP_BACKUP_START,
	CP_BACKUP_END,
	CP_AFTER_RECOVERY,
	CP_SHUTDOWN,
	CP_BACKUP_WITH_LOG_ARCHIVE,
	CP_BACKUP_WITH_LOG_DUPLICATE,
	CP_ARCHIVE_LOG_START,
	CP_ARCHIVE_LOG_END,
	CP_INCREMENTAL_BACKUP_LEVEL_0,
	CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE,
	CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL,
	CP_PREPARE_LONG_ARCHIVE,	
	CP_PREPARE_LONGTERM_SYNC,	
	CP_STOP_LONGTERM_SYNC		
};

typedef uint8_t ColumnType;
const ColumnType COLUMN_TYPE_STRING = 0;
const ColumnType COLUMN_TYPE_BOOL = 1;
const ColumnType COLUMN_TYPE_BYTE = 2;
const ColumnType COLUMN_TYPE_SHORT = 3;
const ColumnType COLUMN_TYPE_INT = 4;
const ColumnType COLUMN_TYPE_LONG = 5;
const ColumnType COLUMN_TYPE_FLOAT = 6;
const ColumnType COLUMN_TYPE_DOUBLE = 7;
const ColumnType COLUMN_TYPE_TIMESTAMP = 8;
const ColumnType COLUMN_TYPE_GEOMETRY = 9;
const ColumnType COLUMN_TYPE_BLOB = 10;
const ColumnType COLUMN_TYPE_OID = 11;  
const ColumnType COLUMN_TYPE_STRING_ARRAY = 12;
const ColumnType COLUMN_TYPE_BOOL_ARRAY = 13;
const ColumnType COLUMN_TYPE_BYTE_ARRAY = 14;
const ColumnType COLUMN_TYPE_SHORT_ARRAY = 15;
const ColumnType COLUMN_TYPE_INT_ARRAY = 16;
const ColumnType COLUMN_TYPE_LONG_ARRAY = 17;
const ColumnType COLUMN_TYPE_FLOAT_ARRAY = 18;
const ColumnType COLUMN_TYPE_DOUBLE_ARRAY = 19;
const ColumnType COLUMN_TYPE_TIMESTAMP_ARRAY = 20;

const ColumnType COLUMN_TYPE_ROWID = COLUMN_TYPE_LONG;
const ColumnType COLUMN_TYPE_WITH_BEGIN = 0xff;
const ColumnType COLUMN_TYPE_NULL = 0xff;  
const ColumnType COLUMN_TYPE_ANY = 0xff;

const uint16_t FixedSizeOfColumnType[] = {
	0,					
	sizeof(bool),		
	sizeof(int8_t),		
	sizeof(int16_t),	
	sizeof(int32_t),	
	sizeof(int64_t),	
	sizeof(float),		
	sizeof(double),		
	sizeof(Timestamp),  
	0,					
	0,					
	sizeof(uint64_t),   
	0,					
	0,					
	0,					
	0,					
	0,					
	0,					
	0,					
	0,					
	0,					
};

typedef uint8_t ContainerType;
static const ContainerType COLLECTION_CONTAINER = 0;
static const ContainerType TIME_SERIES_CONTAINER = 1;
static const ContainerType UNDEF_CONTAINER = 0xfe;
static const ContainerType ANY_CONTAINER = 0xff;

static const char *const GS_CAPITAL_PREFIX = "GS#";
static const char *const GS_CAPITAL_PUBLIC = "PUBLIC";  
static const char *const GS_CAPITAL_INFO_SCHEMA =
	"INFORMATION_SCHEMA";  
static const char *const GS_CAPITAL_ADMIN_USER =
	"ADMIN";  
static const char *const GS_CAPITAL_SYSTEM_USER =
	"SYSTEM";  

static const char *const GS_SYSTEM = "gs#system";  
static const char *const GS_USERS = "#_users@0";
static const char *const GS_DATABASES = "#_databases@0";
static const char *const GS_PUBLIC = "public";  
static const char *const GS_INFO_SCHEMA =
	"information_schema";							 
static const char *const GS_ADMIN_USER = "admin";	
static const char *const GS_SYSTEM_USER = "system";  
static const char *const GS_PREFIX = "gs#";			 

static const DatabaseId UNDEF_DBID = UNDEF_ROWID;
static const DatabaseId MAX_DBID = MAX_ROWID;
static const DatabaseId DBID_RESERVED_RANGE = 100;
static const DatabaseId GS_PUBLIC_DB_ID = 0;
static const DatabaseId GS_SYSTEM_DB_ID = 1;

/*!
	@brief Represents the attribute of container
*/
enum ContainerAttribute {
	CONTAINER_ATTR_SINGLE =
		0x00000010,  
	CONTAINER_ATTR_SINGLE_SYSTEM =
		0x00000011,  
	CONTAINER_ATTR_LARGE =
		0x00000020,  
	CONTAINER_ATTR_SUB =
		0x00000030,  
	CONTAINER_ATTR_ANY = 0x0000007f
};

/*!
	@brief The option for updating the row
*/
enum PutRowOption {
	PUT_INSERT_OR_UPDATE,  
	PUT_INSERT_ONLY,	   
	PUT_UPDATE_ONLY,	   
};

typedef int8_t MapType;
static const MapType MAP_TYPE_BTREE = 0;
static const MapType MAP_TYPE_HASH = 1;
static const MapType MAP_TYPE_SPATIAL = 2;
static const MapType MAP_TYPE_NUM = 3;

static const MapType MAP_TYPE_DEFAULT = -1;
static const MapType MAP_TYPE_VECTOR = -2;

const MapType defaultIndexType[] = {
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_BTREE,		
	MAP_TYPE_SPATIAL,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
	MAP_TYPE_DEFAULT,	
};

inline const char8_t* getMapTypeStr(MapType type) {
	switch (type) {
	case MAP_TYPE_BTREE:
		return "TREE";
	case MAP_TYPE_HASH:
		return "HASH";
	case MAP_TYPE_SPATIAL:
		return "SPATIAL";
	case MAP_TYPE_DEFAULT:
		return "DEFAULT";
	default:
		return "UNKNOWN";
	}
}

typedef uint8_t TimeUnit;
static const TimeUnit TIME_UNIT_YEAR = 0;
static const TimeUnit TIME_UNIT_MONTH = 1;
static const TimeUnit TIME_UNIT_DAY = 2;
static const TimeUnit TIME_UNIT_HOUR = 3;
static const TimeUnit TIME_UNIT_MINUTE = 4;
static const TimeUnit TIME_UNIT_SECOND = 5;
static const TimeUnit TIME_UNIT_MILLISECOND = 6;

enum ExpireType {
	NO_EXPIRE,
	ROW_EXPIRE,
	TABLE_EXPIRE,
	UNDEF_EXPIRE
};

/*!
	@brief Represents the method(s) of aggregation operation on a set of Rows or
   their specific Columns
*/
enum AggregationType {
	AGG_MIN,
	AGG_MAX,
	AGG_SUM,
	AGG_AVG,
	AGG_VARIANCE,
	AGG_STDDEV,
	AGG_COUNT,
	AGG_TIME_AVG,
	AGG_UNSUPPORTED_TYPE
};

/*!
	@brief Represents the type(s) of ResultSet
*/
enum ResultType {
	RESULT_NONE = -1,
	RESULT_ROW_ID_SET = -2,		
	RESULT_ROWSET = 0,			
	RESULT_AGGREGATE = 1,		
	RESULT_EXPLAIN = 2,			
	PARTIAL_RESULT_ROWSET = 3,  
};

/*!
	@brief Defines the constraints on the relation between spatial ranges
*/
enum GeometryOperator {
	GEOMETRY_INTERSECT = 0,			
	GEOMETRY_INCLUDE = 1,			
	GEOMETRY_DIFFERENTIAL = 2,		
	GEOMETRY_QSF_INTERSECT = 0xF0,  
};

/*!
	@brief Represents how to specify a Row based on a time-type key in a
   TimeSeries
*/
enum TimeOperator {
	TIME_PREV,
	TIME_PREV_ONLY,
	TIME_NEXT,
	TIME_NEXT_ONLY,
};

/*!
	@brief Represents the order of Rows requested by a query
*/
enum OutputOrder {
	ORDER_ASCENDING = 0,
	ORDER_DESCENDING = 1,
	ORDER_UNDEFINED = 2,
};

typedef int8_t MVCC_IMAGE_TYPE;
const MVCC_IMAGE_TYPE MVCC_CREATE = 0;
const MVCC_IMAGE_TYPE MVCC_UPDATE = 1;
const MVCC_IMAGE_TYPE MVCC_DELETE = 2;
const MVCC_IMAGE_TYPE MVCC_SELECT = 3;
const MVCC_IMAGE_TYPE MVCC_INDEX = 4;
const MVCC_IMAGE_TYPE MVCC_CONTAINER = 5;
const MVCC_IMAGE_TYPE MVCC_UNDEF = INT8_MAX;

/*!
	@brief MvccRow format
*/
struct MvccRowImage {
	union {
		RowId firstCreateRowId_;  
		OId snapshotRowOId_;  
		RowId cursor_;			
	};
	union {
		RowId lastCreateRowId_;  
		OId containerOId_;		
	};
	MVCC_IMAGE_TYPE type_;
	union {
		uint8_t padding1_;
		MapType mapType_;
	};
	uint16_t padding2_;
	union {
		uint32_t padding3_;
		ColumnId columnId_;
	};

	MvccRowImage()
		: firstCreateRowId_(INITIAL_ROWID),
		  lastCreateRowId_(INITIAL_ROWID),
		  type_(MVCC_UNDEF),
		  padding1_(0),
		  padding2_(0),
		  padding3_(0) {}
	MvccRowImage(MVCC_IMAGE_TYPE type, RowId rowId)
		: type_(type), padding1_(0), padding2_(0), padding3_(0) {
		if (type_ == MVCC_CREATE || type_ == MVCC_SELECT) {
			firstCreateRowId_ = rowId;
			lastCreateRowId_ = rowId;
		}
		else {
			snapshotRowOId_ = rowId;
			lastCreateRowId_ = INITIAL_ROWID;
		}
	}
	MvccRowImage(RowId firstRowId, RowId lastRowId)
		: type_(MVCC_CREATE), padding1_(0), padding2_(0), padding3_(0) {
		firstCreateRowId_ = firstRowId;
		lastCreateRowId_ = lastRowId;
	}

	/*!
		@brief Update min/max RowId
	*/
	void updateRowId(RowId input) {
		type_ = MVCC_CREATE;
		if (firstCreateRowId_ == INITIAL_ROWID) {
			firstCreateRowId_ = input;
			lastCreateRowId_ = input;
		}
		else {
			if (firstCreateRowId_ > input) {
				firstCreateRowId_ = input;
			}
			if (lastCreateRowId_ < input) {
				lastCreateRowId_ = input;
			}
		}
	}
	bool operator==(const MvccRowImage &b) const {
		if (memcmp(this, &b, sizeof(MvccRowImage)) == 0) {
			return true;
		}
		else {
			return false;
		}
	}
	bool operator<(const MvccRowImage &b) const {
		if (memcmp(this, &b, sizeof(MvccRowImage)) < 0) {
			return true;
		}
		else {
			return false;
		}
	}

	static std::string getTypeStr(MVCC_IMAGE_TYPE type) {
		std::string str;
		switch (type) {
		case MVCC_CREATE:
			str = "CREATE";
			break;
		case MVCC_UPDATE:
			str = "UPDATE";
			break;
		case MVCC_DELETE:
			str = "DELETE";
			break;
		case MVCC_SELECT:
			str = "SELECT";
			break;
		case MVCC_INDEX:
			str = "INDEX";
			break;
		case MVCC_CONTAINER:
			str = "CONTAINER";
			break;
		default:
			str = "UNKNOWN";
			break;
		}
		return str;
	}
	std::string dump() {
		util::NormalOStringStream out;
		out << "(@@MvccRow@@)";
		out << "(type_=" << getTypeStr(type_);
		out << ",first_=" << firstCreateRowId_;
		out << ",last=" << lastCreateRowId_;
		out << ")";
		return out.str();
	}

private:
	friend std::ostream &operator<<(
		std::ostream &output, const MvccRowImage &image) {
		output << static_cast<MvccRowImage>(image).dump();
		return output;
	}
};

class FullContainerKey;
struct TQLInfo {
	TQLInfo() : dbName_(GS_PUBLIC),	containerKey_(NULL), query_("") {}
	TQLInfo(const char *dbName, const FullContainerKey *containerKey, 
		const char *query) : 
		dbName_((dbName == NULL) ? GS_PUBLIC : dbName), 
		containerKey_(containerKey),
		query_((query == NULL) ? "" : query) {} 
	const char *dbName_;
	const FullContainerKey *containerKey_;
	const char *query_;
};






#endif
