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
	@brief Definition of common data type
*/
#ifndef DATA_TYPE_H_
#define DATA_TYPE_H_

#include "util/type.h"

typedef float float32;
typedef double float64;

const float64 FLOAT64_MAX = 1.7976931348623158e+308;
const float64 FLOAT64_MIN = -1.7976931348623158e+308;

#define MAX_STRING_LENGTH 0x7fffffffU



typedef uint64_t OId; /*!< Object ID */

typedef uint32_t PartitionId;	  /*!< Partition ID */
typedef uint32_t PartitionGroupId; /*!< Partition group ID */

typedef int32_t ChunkId; /*!< Chunk ID (unique witihin a partition) */

typedef uint64_t ContainerId; /*!< Container ID (unique witihin a partition) */

typedef int64_t RowId; /*!< Row ID (unique witihin a container) */

typedef uint32_t ColumnId; /*!< Column ID */

typedef uint32_t SchemaVersionId; /*!< ContainerSchema version ID (unique
									 witihin a container) */

typedef uint64_t LogSequentialNumber; /*!< Log sequential Number (unique witihin
										 a partition) */

typedef uint64_t
	TransactionId;			/*!< Transaction ID (unique witihin a partition) */

typedef uint64_t SessionId; /*!< Session ID (decide by client) */

typedef uint64_t StatementId; /*!< Statement ID (decide by client) */

typedef int64_t Timestamp; /*!< Timestamp */

typedef uint32_t UserId; /*!< User ID */

typedef uint64_t
	CheckpointId; /*!< Checkpoint ID (unique witihin a partition group) */

typedef uint64_t ResultSize;  

typedef uint64_t BackgroundId; /*! Background Id (unique witihin a partition) */

typedef uint32_t Size_t;
typedef int32_t Offset_t;

typedef OId PointRowId;  

const OId UNDEF_OID = UINT64_MAX;
const OId MAX_OID = UNDEF_OID - 1;

const ChunkId UNDEF_CHUNKID = -1;
const ChunkId MAX_CHUNKID = INT32_MAX;

const PartitionId UNDEF_PARTITIONID = UINT32_MAX;
const PartitionId MAX_PARTITIONID = UNDEF_PARTITIONID - 1;

const PartitionGroupId UNDEF_PARTITIONGROUPID = UINT32_MAX;
const PartitionGroupId MAX_PARTITIONGROUPID = UNDEF_PARTITIONGROUPID - 1;

const ContainerId UNDEF_CONTAINERID = UINT32_MAX;
const ContainerId MAX_CONTAINERID = UNDEF_CONTAINERID - 1;

const RowId UNDEF_ROWID = INT64_MAX;
const RowId MAX_ROWID = UNDEF_ROWID - 1;
const int64_t INITIAL_ROWID = -1;

const ColumnId UNDEF_COLUMNID = UINT32_MAX;
const ColumnId MAX_COLUMNID = UNDEF_COLUMNID - 1;

const SchemaVersionId UNDEF_SCHEMAVERSIONID = UINT32_MAX;
const SchemaVersionId MAX_SCHEMAVERSIONID = UNDEF_SCHEMAVERSIONID - 1;

const LogSequentialNumber UNDEF_LSN = UINT64_MAX;
const LogSequentialNumber MAX_LSN = UNDEF_LSN - 1;

const TransactionId UNDEF_TXNID = 0;
const TransactionId MAX_TXNID = UINT64_MAX;

const SessionId UNDEF_SESSIONID = 0;
const SessionId MAX_SESSIONID = UNDEF_SESSIONID - 1;

const StatementId INITIAL_STATEMENTID = 0;
const StatementId UNDEF_STATEMENTID = UINT64_MAX;
const StatementId MAX_STATEMENTID = UNDEF_STATEMENTID - 1;

const Timestamp UNDEF_TIMESTAMP = INT64_MAX;
const Timestamp MAX_TIMESTAMP = UNDEF_TIMESTAMP - 1;

const UserId UNDEF_USERID = INT32_MAX;
const UserId MAX_USERID = INT32_MAX - 1;
const UserId ADMINISTRATOR_USERID = 0;

const CheckpointId UNDEF_CHECKPOINT_ID = UINT64_MAX;
const CheckpointId MAX_CHECKPOINT_ID = UNDEF_CHECKPOINT_ID - 1;

const ResultSize UNDEF_RESULT_SIZE = UINT64_MAX;
const ResultSize MAX_RESULT_SIZE = UNDEF_RESULT_SIZE - 1;

const BackgroundId UNDEF_BACKGROUND_ID = UINT64_MAX;

const bool TRIM_MILLISECONDS = false;  

typedef int64_t TablePartitioningVersionId;
const TablePartitioningVersionId UNDEF_TABLE_PARTITIONING_VERSIONID = -1;
const TablePartitioningVersionId MAX_TABLE_PARTITIONING_VERSIONID = INT64_MAX;

const uint32_t IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS = 5000;

/*!
	@brief Represents the value of return code
*/
enum GSStatus {
	GS_SUCCESS,
	GS_ERROR,
	GS_TIMEOUT,
	GS_FAIL  
};

typedef int64_t ResultSetId;
typedef int64_t ResultSetVersionId;
const ResultSetId UNDEF_RESULTSETID = -1;

typedef int64_t NodeDescriptorId;

typedef int8_t ChunkCategoryId;
const ChunkCategoryId UNDEF_CHUNK_CATEGORY_ID = INT8_MAX;
const ChunkCategoryId MAX_CHUNK_CATEGORY_ID = UNDEF_CHUNK_CATEGORY_ID - 1;

typedef int32_t ChunkKey;
const ChunkKey UNDEF_CHUNK_KEY = INT32_MAX;
const ChunkKey MAX_CHUNK_KEY = UNDEF_CHUNK_KEY - 1;
const ChunkKey MIN_CHUNK_KEY = INT32_MIN;

typedef uint8_t ContainerHashMode;
const ContainerHashMode CONTAINER_HASH_MODE_CRC32 = 0;

typedef uint64_t AffinityGroupId;
typedef uint16_t ExpireIntervalCategoryId;  
typedef uint16_t UpdateIntervalCategoryId;  

typedef RowId DatabaseId;  

//#define GD_ENABLE_UNICAST_NOTIFICATION


#endif
