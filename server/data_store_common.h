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
#include "utility_v5.h"


class ObjectManagerV4;

const uint32_t VAR_SIZE_1BYTE_THRESHOLD = 128;
const uint32_t VAR_SIZE_4BYTE_THRESHOLD = UINT32_C(1) << 30;
const uint64_t VAR_SIZE_8BYTE_THRESHOLD = UINT64_C(1) << 62;

const uint32_t NEXT_OBJECT_LINK_INFO_SIZE = 8;  
const uint32_t LINK_VARIABLE_COLUMN_DATA_SIZE =
	static_cast<uint32_t>(sizeof(uint32_t) + sizeof(uint64_t));  
const uint32_t AFFINITY_STRING_MAX_LENGTH =
	8;  
const char *const DEFAULT_AFFINITY_STRING =
	"";  

const Timestamp MINIMUM_EXPIRED_TIMESTAMP = -1;

typedef uint8_t RowHeader;

const uint8_t DS_CHUNK_EXP_SIZE = 16;
const ChunkCategoryId DS_CHUNK_CATEGORY_SIZE =
	5;  
const bool DS_CHUNK_CATEGORY_RANGE_BATCH_FREE[] = {
	false, false, false, true, true};
const bool DS_CHUNK_CATEGORY_SMALL_SIZE_SEARCH[] = {
	true, false, false, false, false};
const int16_t DS_ADDITIONAL_CHUNK_CATEGORY_NUM =
	17;  

const uint32_t CHUNK_SIZE = 1 << DS_CHUNK_EXP_SIZE;  
const uint32_t OBJECT_BLOCK_HEADER_SIZE =
	4;  

const ChunkCategoryId ALLOCATE_META_CHUNK =
	0;  
const ChunkCategoryId ALLOCATE_NO_EXPIRE_MAP = 1;  
const ChunkCategoryId ALLOCATE_NO_EXPIRE_ROW = 2;  
const ChunkCategoryId ALLOCATE_EXPIRE_MAP = 3;	 
const ChunkCategoryId ALLOCATE_EXPIRE_ROW = 4;	 

const AffinityGroupId DEFAULT_AFFINITY_GROUP_ID = 0;

const ExpireIntervalCategoryId DEFAULT_EXPIRE_CATEGORY_ID = 0;

enum StoreType {
	V4_COMPATIBLE = 0,
	KEY_STORE = 1,
	SAMPLE_STORE,
	UNDEF_STORE
};

const uint32_t LIMIT_COLUMN_NAME_SIZE = 256;  

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
const ObjectType OBJECT_TYPE_RESERVED = 10;
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
	CP_UNKNOWN,
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
	CP_STOP_LONGTERM_SYNC,		
	CP_AFTER_LONGTERM_SYNC,		
	CP_MODE_END
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
const ColumnType COLUMN_TYPE_MICRO_TIMESTAMP = 21;
const ColumnType COLUMN_TYPE_NANO_TIMESTAMP = 22;

const ColumnType COLUMN_TYPE_ROWID = COLUMN_TYPE_LONG;
const ColumnType COLUMN_TYPE_COMPOSITE = 0xfe;
const ColumnType COLUMN_TYPE_WITH_BEGIN = 0xff;
const ColumnType COLUMN_TYPE_NULL = 0xff;  
const ColumnType COLUMN_TYPE_ANY = 0xff;

const ColumnType COLUMN_TYPE_ARRAY_BEGIN = COLUMN_TYPE_STRING_ARRAY;
const ColumnType COLUMN_TYPE_ARRAY_TAIL = COLUMN_TYPE_TIMESTAMP_ARRAY;

const ColumnType COLUMN_TYPE_PRIMITIVE_BEGIN1 = COLUMN_TYPE_STRING;
const ColumnType COLUMN_TYPE_PRIMITIVE_TAIL1 = COLUMN_TYPE_BLOB;
const ColumnType COLUMN_TYPE_PRIMITIVE_BEGIN2 = COLUMN_TYPE_MICRO_TIMESTAMP;
const ColumnType COLUMN_TYPE_PRIMITIVE_TAIL2 = COLUMN_TYPE_NANO_TIMESTAMP;

const ColumnType COLUMN_TYPE_SIMPLE_BEGIN1 = COLUMN_TYPE_BOOL;
const ColumnType COLUMN_TYPE_SIMPLE_TAIL1 = COLUMN_TYPE_TIMESTAMP;
const ColumnType COLUMN_TYPE_SIMPLE_BEGIN2 = COLUMN_TYPE_MICRO_TIMESTAMP;
const ColumnType COLUMN_TYPE_SIMPLE_TAIL2 = COLUMN_TYPE_NANO_TIMESTAMP;

const ColumnType COLUMN_TYPE_NUMERIC_BEGIN = COLUMN_TYPE_BYTE;
const ColumnType COLUMN_TYPE_NUMERIC_TAIL = COLUMN_TYPE_DOUBLE;
const ColumnType COLUMN_TYPE_INTEGRAL_BEGIN = COLUMN_TYPE_BYTE;
const ColumnType COLUMN_TYPE_INTEGRAL_TAIL = COLUMN_TYPE_LONG;
const ColumnType COLUMN_TYPE_FLOATING_BEGIN = COLUMN_TYPE_FLOAT;
const ColumnType COLUMN_TYPE_FLOATING_TAIL = COLUMN_TYPE_DOUBLE;

const ColumnType COLUMN_TYPE_PRIMITIVE_COUNT = (
		(COLUMN_TYPE_PRIMITIVE_TAIL1 - COLUMN_TYPE_PRIMITIVE_BEGIN1 + 1) +
		(COLUMN_TYPE_PRIMITIVE_TAIL2 - COLUMN_TYPE_PRIMITIVE_BEGIN2 + 1));
const ColumnType COLUMN_TYPE_TOTAL_TAIL = COLUMN_TYPE_PRIMITIVE_TAIL2;

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
	sizeof(MicroTimestamp), 
	sizeof(NanoTimestamp), 
};

typedef int32_t SchemaFeatureLevel;

typedef uint8_t ContainerType;
const ContainerType COLLECTION_CONTAINER = 0;
const ContainerType TIME_SERIES_CONTAINER = 1;
const ContainerType UNDEF_CONTAINER = 0xfe;
const ContainerType ANY_CONTAINER = 0xff;

const char *const GS_CAPITAL_PREFIX = "GS#";
const char *const GS_CAPITAL_PUBLIC = "PUBLIC";  
const char *const GS_CAPITAL_INFO_SCHEMA =
	"INFORMATION_SCHEMA";  
const char *const GS_CAPITAL_ADMIN_USER =
	"ADMIN";  
const char *const GS_CAPITAL_SYSTEM_USER =
	"SYSTEM";  

const char *const GS_SYSTEM = "gs#system";  
const char *const GS_USERS = "#_users@0";
const char *const GS_DATABASES = "#_databases@0";
const char *const GS_PUBLIC = "public";  
const char *const GS_INFO_SCHEMA =
	"information_schema";							 
const char *const GS_ADMIN_USER = "admin";	
const char *const GS_SYSTEM_USER = "system";  
const char *const GS_PREFIX = "gs#";			 

const DatabaseId UNDEF_DBID = UNDEF_ROWID;
const DatabaseId MAX_DBID = MAX_ROWID;
const DatabaseId DBID_RESERVED_RANGE = 100;
const DatabaseId GS_PUBLIC_DB_ID = 0;
const DatabaseId GS_SYSTEM_DB_ID = 1;

const char *const UNIQUE_GROUP_ID_KEY = "#unique";

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
	CONTAINER_ATTR_VIEW =
		0x00000040,  
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
const MapType MAP_TYPE_BTREE = 0;
const MapType MAP_TYPE_HASH = 1;
const MapType MAP_TYPE_SPATIAL = 2;
const MapType MAP_TYPE_NUM = 3;

const MapType MAP_TYPE_DEFAULT = -1;
const MapType MAP_TYPE_VECTOR = -2;
const MapType MAP_TYPE_ANY = -3;

const MapType DEFAULT_INDEX_TYPE[] = {
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
	MAP_TYPE_BTREE, 
	MAP_TYPE_BTREE, 
};

inline const char8_t* getMapTypeStr(MapType type) {
	switch (type) {
	case MAP_TYPE_BTREE:
		return "TREE";
	case MAP_TYPE_SPATIAL:
		return "SPATIAL";
	case MAP_TYPE_DEFAULT:
		return "DEFAULT";
	default:
		return "UNKNOWN";
	}
}

typedef uint8_t TimeUnit;
const TimeUnit TIME_UNIT_YEAR = 0;
const TimeUnit TIME_UNIT_MONTH = 1;
const TimeUnit TIME_UNIT_DAY = 2;
const TimeUnit TIME_UNIT_HOUR = 3;
const TimeUnit TIME_UNIT_MINUTE = 4;
const TimeUnit TIME_UNIT_SECOND = 5;
const TimeUnit TIME_UNIT_MILLISECOND = 6;

enum ExpireType {
	NO_EXPIRE,
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

class Cursor {
	virtual bool isFinished() const = 0;
public:
	static const uint64_t NUM_PER_EXEC = 500;
};

class IndexCursor : Cursor {
public:
	IndexCursor() {};
	IndexCursor(const MvccRowImage &image) {
		setMvccImage(image);
	};
	IndexCursor(bool isImmediate) {
		if (isImmediate) {
			setImmediateMode();
		}
	};

	bool isFinished() const {
		return data_.rowId_ == MAX_ROWID;
	}
	bool isImmediateMode() const {
		return data_.type_ == MVCC_UNDEF;
	}

	void setImmediateMode() {
		data_.type_ = MVCC_UNDEF;
	}
	void setMvccImage(const MvccRowImage &image) {
		memcpy(&data_, &image, sizeof(Data));
	}
	MvccRowImage getMvccImage() const {
		MvccRowImage image;
		memcpy(&image, &data_, sizeof(Data));
		return image;
	}
	MapType getMapType() const {return data_.mapType_;}
	ColumnId getColumnId() const {return data_.columnId_;}
	RowId getRowId() const {return data_.rowId_;}
	OId getOId() const {return data_.optionOId_;} 
	static uint64_t getNum() {return NUM_PER_EXEC;}
	void setMapType(MapType mapType) {data_.mapType_ = mapType;}
	void setColumnId(ColumnId columnId) {data_.columnId_ = columnId;}
	void setRowId(RowId rowId) {data_.rowId_ = rowId;}
	void setOption(OId oId) {data_.optionOId_ = oId;} 
private:
	struct Data {
		Data() {
			rowId_ = INITIAL_ROWID;
			optionOId_ = 0; 
			type_ = MVCC_INDEX;
			mapType_ = MAP_TYPE_DEFAULT;
			padding2_ = 0;
			columnId_ = UNDEF_COLUMNID;
		}

		RowId rowId_; 
		OId optionOId_; 
		MVCC_IMAGE_TYPE type_;
		MapType mapType_;
		uint16_t padding2_;
		ColumnId columnId_;
	};
	Data data_;
};

class ContainerCursor : Cursor {
public:
	ContainerCursor() {};
	ContainerCursor(const MvccRowImage &image) {
		setMvccImage(image);
	};
	ContainerCursor(bool isImmediate) {
		if (isImmediate) {
			setImmediateMode();
		}
	};

	ContainerCursor(bool isImmediate, OId oId) {
		if (isImmediate) {
			setImmediateMode();
		}
		setContainerOId(oId);
	};

	bool isFinished() const {
		return data_.rowId_ == MAX_ROWID;
	}
	bool isImmediateMode() const {
		return data_.type_ == MVCC_UNDEF;
	}

	void setImmediateMode() {
		data_.type_ = MVCC_UNDEF;
	}
	void setMvccImage(const MvccRowImage &image) {
		memcpy(&data_, &image, sizeof(Data));
	}
	MvccRowImage getMvccImage() const {
		MvccRowImage image;
		memcpy(&image, &data_, sizeof(Data));
		return image;
	}
	static uint64_t getNum() {return NUM_PER_EXEC;}
	RowId getRowId() const {return data_.rowId_;}
	void setRowId(RowId rowId) {data_.rowId_ = rowId;}
	OId getContainerOId() const {return data_.containerOId_;}
	void setContainerOId(OId oId) {data_.containerOId_ = oId;}
private:
	struct Data {
		Data() {
			rowId_ = INITIAL_ROWID;
			containerOId_ = UNDEF_OID;
			type_ = MVCC_CONTAINER;
			padding1_ = 0;
			padding2_ = 0;
			padding3_ = 0;
		}

		RowId rowId_; 
		OId containerOId_; 
		MVCC_IMAGE_TYPE type_;
		uint8_t padding1_;
		uint16_t padding2_;
		uint32_t padding3_;
	};
	Data data_;
};

struct KeyData {
	KeyData() : data_(NULL), size_(0) {}
	KeyData(const void *data, uint32_t size) : data_(data), size_(size) {}
	const void *data_;
	uint32_t size_;
};

struct ChunkCompressionTypes {
	/*!
		@brief Chunk compression mode
	*/
	enum Mode {
		NO_BLOCK_COMPRESSION,   
		BLOCK_COMPRESSION,      
		BLOCK_COMPRESSION_ZLIB, 
		BLOCK_COMPRESSION_ZSTD, 

		MODE_END
	};
};

struct LRUFrame;
class ChunkBuffer;
class ChunkBufferFrameRef {
public:
	ChunkBufferFrameRef() :
			base_(nullptr), pos_(-1) {
	}

private:
	friend class ChunkBuffer;

	ChunkBufferFrameRef(LRUFrame *base, int32_t pos) :
			base_(base), pos_(pos) {
	}

	LRUFrame* get() const { return base_; }
	int32_t getPosition() const { return pos_; }

	LRUFrame *base_;
	int32_t pos_;
};






enum class PutStatus {
	NOT_EXECUTED,
	CREATE,
	UPDATE,
	CHANGE_PROPERTY,
};

enum DSOperationType {
	DS_CONTAINER_COUNT,
	DS_CONTAINER_NAME_LIST,
	DS_GET_CONTAINER,
	DS_GET_CONTAINER_BY_ID,
	DS_PUT_CONTAINER,
	DS_UPDATE_TABLE_PARTITIONING_ID,
	DS_DROP_CONTAINER,

	DS_PUT_ROW,
	DS_UPDATE_ROW_BY_ID,
	DS_REMOVE_ROW,
	DS_REMOVE_ROW_BY_ID,
	DS_CONTINUE_CREATE_INDEX,
	DS_CONTINUE_ALTER_CONTAINER,
	DS_CREATE_INDEX,
	DS_DROP_INDEX,
	DS_COMMIT,
	DS_ABORT,
	DS_LOCK,
	DS_APPEND_ROW,
	DS_GET_ROW,
	DS_GET_ROW_SET,
	DS_GET_TIME_RELATED,
	DS_GET_INTERPOLATE,
	DS_QUERY_TQL,
	DS_QUERY_SAMPLE,
	DS_QUERY_AGGREGATE,
	DS_QUERY_TIME_RANGE,
	DS_QUERY_FETCH_RESULT_SET,
	DS_QUERY_CLOSE_RESULT_SET,
	DS_QUERY_GEOMETRY_RELATED,
	DS_QUERY_GEOMETRY_WITH_EXCLUSION,
	DS_GET_CONTAINER_OBJECT,
	DS_SCAN_CHUNK_GROUP,
	DS_SEARCH_BACKGROUND_TASK,
	DS_EXECUTE_BACKGROUND_TASK,
	DS_CHECK_TIMEOUT_RESULT_SET,
	DS_UNDEF,

};

const DSGroupId META_GROUP_ID = 0; 

struct Serializable;

struct KeyDataStoreValue : public Serializable {
	KeyDataStoreValue() : Serializable(NULL), containerId_(UNDEF_CONTAINERID),
		oId_(UNDEF_OID),
		storeType_(UNDEF_STORE),
		attribute_(CONTAINER_ATTR_ANY) {}
	KeyDataStoreValue(
		ContainerId containerId, OId oId, StoreType storeType, ContainerAttribute attribute)
		: Serializable(NULL), containerId_(containerId),
		oId_(oId),
		storeType_(storeType),
		attribute_(attribute) {}
	KeyDataStoreValue(const KeyDataStoreValue& src) :
		Serializable(NULL), containerId_(src.containerId_), oId_(src.oId_),
		storeType_(src.storeType_), attribute_(src.attribute_) {}

	bool operator==(const KeyDataStoreValue& b) const {
		bool isEqual = (this->containerId_ == b.containerId_) &&
			(this->oId_ == b.oId_) &&
			(this->storeType_ == b.storeType_) &&
			(this->attribute_ == b.attribute_);
		if (isEqual) {
			return true;
		}
		else {
			return false;
		}
	}
	bool operator<(const KeyDataStoreValue& b) const {
		if (this->containerId_ < b.containerId_) {
			return true;
		}
		else if (this->containerId_ > b.containerId_) {
			return false;
		}
		else if (this->oId_ < b.oId_) {
			return true;
		}
		else if (this->oId_ > b.oId_) {
			return false;
		}
		else if (this->storeType_ < b.storeType_) {
			return true;
		}
		else if (this->storeType_ > b.storeType_) {
			return false;
		}
		else {
			return this->attribute_ < b.attribute_;
		}
	}
	friend std::ostream& operator<<(std::ostream& output, const KeyDataStoreValue& val) {
		output << "(";
		output << val.containerId_ << ",";
		output << val.oId_ << ",";
		output << static_cast<int32_t>(val.storeType_) << ",";
		output << static_cast<int32_t>(val.attribute_) << ")";
		return output;
	}

	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}

	template <typename S>
	void encode(S& out) {
		out << containerId_;
		out << oId_;
		out << static_cast<int32_t>(storeType_);
		out << static_cast<int32_t>(attribute_);
	}
	template <typename S>
	void decode(S& in) {
		in >> containerId_;
		in >> oId_;
		int32_t tmpStoreType;
		in >> tmpStoreType;
		storeType_ = static_cast<StoreType>(tmpStoreType);
		int32_t tmpAttr;
		in >> tmpAttr;
		attribute_ = static_cast<ContainerAttribute>(tmpAttr);
	}
	ContainerId containerId_;
	OId oId_;
	StoreType storeType_;
	ContainerAttribute attribute_;
};

class DataStoreBase;
class Log;

/*!
	@brief Exception for lock conflict
*/
class LockConflictException : public util::Exception {
public:
	explicit LockConflictException(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
		Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~LockConflictException() throw() {}
};
#define DS_RETHROW_LOCK_CONFLICT_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                           \
		LockConflictException, GS_ERROR_DEFAULT, cause, message)

#define DS_THROW_LOCK_CONFLICT_EXCEPTION(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LockConflictException, errorCode, message)

class DSEncodeDecodeException : public util::Exception {
public:
	explicit DSEncodeDecodeException(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
		Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~DSEncodeDecodeException() throw() {}
};

#define DS_RETHROW_DECODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		DSEncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

#define DS_RETHROW_ENCODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		DSEncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

class DataStoreUtil {
public:
	/*
	 * VarSize format
	 * first 1byte
	 * xxxxxx00 4byte(to 2^30=1G-1)
	 * xxxxxx10 8byte(OID)
	 * xxxxxxx1 1byte(to 127)
	*/

	static const char8_t* dumpContainerAttribute(ContainerAttribute attr) {
		switch (attr) {
		case CONTAINER_ATTR_SINGLE:
			return "SINGLE";
		case CONTAINER_ATTR_SINGLE_SYSTEM:
			return "SINGLE_SYSTEM";
		case CONTAINER_ATTR_LARGE:
			return "LARGE";
		case CONTAINER_ATTR_SUB:
			return "SUB";
		case CONTAINER_ATTR_VIEW:
			return "VIEW";
		default:
			return "ANY";
		}
	}

	/*!
		@brief Checks if variable size is 1 byte
	*/
	static inline bool varSizeIs1Byte(uint8_t val) {
		return ((val & 0x01) == 0x01);
	}
	/*!
		@brief Checks if variable size is 4 bytes
	*/
	static inline bool varSizeIs4Byte(uint8_t val) {
		return ((val & 0x03) == 0x00);
	}
	/*!
		@brief Checks if variable size is 8 bytes
	*/
	static inline bool varSizeIs8Byte(uint8_t val) {
		return ((val & 0x03) == 0x02);
	}

	/*!
		@brief Decodes variable size (1 byte)
	*/
	static inline uint32_t decode1ByteVarSize(uint8_t val) {
		assert(val != 0);
		return val >> 1;
	}
	/*!
		@brief Decodes variable size (4 bytes)
	*/
	static inline uint32_t decode4ByteVarSize(uint32_t val) {
		assert(val != 0);
		return val >> 2;
	}
	/*!
		@brief Decodes variable size (8 bytes)
	*/
	static inline uint64_t decode8ByteVarSize(uint64_t val) {
		assert(val != 0);
		return val >> 2;
	}

	/*!
		@brief Decodes variable size (1 or 4 or 8 bytes (0 to 2^31-1))
	*/
	static inline uint32_t decodeVarSize(const void* ptr) {
		const uint8_t val1byte = *static_cast<const uint8_t*>(ptr);
		if (varSizeIs1Byte(val1byte)) {
			return decode1ByteVarSize(val1byte);
		}
		else if (varSizeIs4Byte(val1byte)) {
			const uint32_t val4byte = *static_cast<const uint32_t*>(ptr);
			return decode4ByteVarSize(val4byte);
		}
		else {
			assert(varSizeIs8Byte(val1byte));
			const uint64_t decodedVal =
					decode8ByteVarSize(*static_cast<const uint64_t*>(ptr));
			if (decodedVal > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
				UTIL_THROW_ERROR(
						GS_ERROR_DS_OUT_OF_RANGE,
						"Decoded size = " << decodedVal);
			}
			return static_cast<uint32_t>(decodedVal);
		}
	}

	/*!
		@brief Decodes variable size (1 or 4 or 8 bytes (0 to 2^62-1))
	*/
	static inline uint64_t decodeVarSize64(const void* ptr) {
		bool isOId;
		return decodeVarSizeOrOId(ptr, isOId);
	}

	/*!
		@brief Decodes variable size or OId (1 or 4 or 8 bytes (0 to 2^62-1))
		@attention 関数名にOIdとあるが、一部ビットが欠損するためOIdには使えない
	*/
	static inline uint64_t decodeVarSizeOrOId(const void* ptr, bool& isOId) {
		isOId = false;
		const uint8_t val1byte = *static_cast<const uint8_t*>(ptr);
		if (varSizeIs1Byte(val1byte)) {
			return decode1ByteVarSize(val1byte);
		}
		else if (varSizeIs4Byte(val1byte)) {
			const uint32_t val4byte = *static_cast<const uint32_t*>(ptr);
			return decode4ByteVarSize(val4byte);
		}
		else {
			assert(varSizeIs8Byte(val1byte));
			const uint64_t val8byte = *static_cast<const uint64_t*>(ptr);
			isOId = true;
			return decode8ByteVarSize(val8byte);
		}
	}

	/*!
		@brief Reads variable size from byte stream (1 or 4 or 8 bytes (0 to 2^31-1))
		@note 読み出し後、入力ストリームは可変長バイトの次を指す
	*/
	static inline uint32_t getVarSize(util::ByteStream<util::ArrayInStream>& in) {
		const size_t currentPos = in.base().position();
		uint8_t byteData;
		in >> byteData;
		if (varSizeIs1Byte(byteData)) {
			return decode1ByteVarSize(byteData);
		}
		else if (varSizeIs4Byte(byteData)) {
			in.base().position(currentPos);
			uint32_t rawData;
			in >> rawData;
			return decode4ByteVarSize(rawData);
		}
		else {
			in.base().position(currentPos);
			uint64_t rawData;
			in >> rawData;
			const uint64_t decodedVal = decode8ByteVarSize(rawData);
			if (decodedVal >
					static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
				GS_THROW_USER_ERROR(
						GS_ERROR_DS_OUT_OF_RANGE,
						"Decoded size = " << decodedVal);
			}
			return static_cast<uint32_t>(decodedVal);
		}
	}

	/*!
		@brief Returns encoded size (1 or 4 or 8 bytes)
	*/
	static inline uint32_t getEncodedVarSize(const void* ptr) {
		const uint8_t val = *static_cast<const uint8_t*>(ptr);
		if (varSizeIs1Byte(val)) {
			return 1;
		}
		else if (varSizeIs4Byte(val)) {
			return 4;
		}
		else {
			assert(varSizeIs8Byte(val));
			return 8;
		}
	}

	/*!
		@brief Returns encoded size (1 or 4 or 8 bytes)
	*/
	static inline uint32_t getEncodedVarSize(uint64_t val) {
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return 1;
		}
		else if (val < VAR_SIZE_4BYTE_THRESHOLD) {
			return 4;
		}
		else {
			return 8;
		}
	}

	/*!
		@brief Encodes variable size (0 to 2^62-1)
	*/
	static inline uint64_t encodeVarSize(uint64_t val) {
		assert(val < UINT64_C(0x4000000000000000));
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return encode1ByteVarSize(static_cast<uint8_t>(val));
		}
		else if (val < VAR_SIZE_4BYTE_THRESHOLD) {
			return encode4ByteVarSize(static_cast<uint32_t>(val));
		}
		else {
			return encode8ByteVarSize(val);
		}
	}

	/*!
		@brief Encodes variable size (1 byte)
		@note 上位1ビットは捨てられる
	*/
	static inline uint8_t encode1ByteVarSize(uint8_t val) {
		return static_cast<uint8_t>(((val << 1) | 0x01));
	}

	/*!
		@brief Encodes variable size (4 bytes)
		@note 上位2ビットは捨てられる
	*/
	static inline uint32_t encode4ByteVarSize(uint32_t val) {
		return (val << 2);
	}

	/*!
		@brief Encodes variable size (8 bytes)
		@note 上位2ビットは捨てられる
	*/
	static inline uint64_t encode8ByteVarSize(uint64_t val) {
		return (val << 2) | 0x02;
	}

	/*!
		@brief Encodes variable size (8 bytes)
		@attention 関数名にOIdとあるが、一部ビットが欠損するためOIdには使えない
	*/
	static inline uint64_t encodeVarSizeOId(uint64_t val) {
		return encode8ByteVarSize(val);
	}

	template <typename S>
	static void encodeVarSizeBinaryData(
			S& out, const uint8_t* data, size_t size) {
		try {
			switch (getEncodedVarSize(size)) {
			case 1:
				out << encode1ByteVarSize(static_cast<uint8_t>(size));
				break;
			case 4:
				out << encode4ByteVarSize(static_cast<uint32_t>(size));
				break;
			default:
				if (size > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
					GS_THROW_USER_ERROR(
							GS_ERROR_DS_OUT_OF_RANGE,
							"Binary data size out of range");
				}
				out << encode8ByteVarSize(size);
				break;
			}
			out << std::pair<const uint8_t*, size_t>(data, size);
		}
		catch (std::exception& e) {
			DS_RETHROW_ENCODE_ERROR(e, "");
		}
	}

	template <typename S>
	static void decodeVarSizeBinaryData(
			S& in, util::StackAllocator& alloc, uint8_t*& data, size_t& dataSize) {
		try {
			dataSize = getVarSize(in);
			data = ALLOC_NEW(alloc) uint8_t[dataSize];
			in >> std::make_pair(data, dataSize);
		}
		catch (std::exception& e) {
			DS_RETHROW_DECODE_ERROR(e, "");
		}
	}

	template <typename S>
	static void decodeVarSizeBinaryData(S& in, util::XArray<uint8_t>& binaryData) {
		try {
			const uint32_t size = getVarSize(in);
			binaryData.resize(size);
			in >> std::make_pair(binaryData.data(), size);
		}
		catch (std::exception& e) {
			DS_RETHROW_DECODE_ERROR(e, "");
		}
	}

	template <typename S>
	static void encodeBinaryData(S& out, const uint8_t* data, size_t size) {
		try {
			out << std::pair<const uint8_t*, size_t>(data, size);
		}
		catch (std::exception& e) {
			DS_RETHROW_ENCODE_ERROR(e, "");
		}
	}

	template <typename S>
	static void decodeBinaryData(
			S& in, util::XArray<uint8_t>& binaryData, bool readAll) {
		try {
			size_t size;
			if (readAll) {
				size = in.base().remaining();
			}
			else {
				uint32_t baseSize;
				in >> baseSize;
				size = baseSize;
			}
			binaryData.resize(size);
			in >> std::make_pair(binaryData.data(), size);
		}
		catch (std::exception& e) {
			DS_RETHROW_DECODE_ERROR(e, "");
		}
	}

	template<typename S>
	static void encodeBooleanData(S& out, bool boolData) {
		try {
			const uint8_t tmp = boolData ? 1 : 0;
			out << tmp;
		}
		catch (std::exception& e) {
			DS_RETHROW_ENCODE_ERROR(e, "");
		}
	}
	template <typename S, typename EnumType>
	static void decodeEnumData(S& in, EnumType& enumData) {
		try {
			int32_t tmp;
			in >> tmp;
			enumData = static_cast<EnumType>(tmp);
		}
		catch (std::exception& e) {
			DS_RETHROW_DECODE_ERROR(e, "");
		}
	}
	template <typename S, typename EnumType>
	static void encodeEnumData(S& out, EnumType enumData) {
		try {
			const int32_t tmp = static_cast<int32_t>(enumData);
			out << tmp;
		}
		catch (std::exception& e) {
			DS_RETHROW_ENCODE_ERROR(e, "");
		}
	}

};

#endif
