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
	@brief Definition of event type communicated on the same node or between
   nodes
*/
#ifndef CLUSTER_EVENT_TYPE_H_
#define CLUSTER_EVENT_TYPE_H_

#include "util/type.h"
#include "util/container.h"


static const int32_t V_1_5_CLUSTER_START = 1000;
static const int32_t V_1_5_CLUSTER_END = 1019;
static const int32_t V_1_5_SYNC_START = 2000;
static const int32_t V_1_5_SYNC_END = 2020;

typedef int32_t EventType;

/*!
	@brief Error type of WebAPI operation
*/
enum WebAPIErrorType {
	WEBAPI_NORMAL = 0,

	WEBAPI_CS_CLUSTERNAME_UNMATCH = 100,
	WEBAPI_CS_CLUSTERNAME_SIZE_LIMIT,
	WEBAPI_CS_CLUSTERNAME_INVALID,
	WEBAPI_CS_OTHER_REASON,
	WEBAPI_CS_CLUSTERNAME_SIZE_ZERO,
	WEBAPI_CS_CLUSTER_IS_STABLE,
	WEBAPI_CS_LEAVE_NOT_SAFETY_NODE,

	WEBAPI_CS_GOAL_DUPLICATE_PARTITION,
	WEBAPI_CS_GOAL_NOT_OWNER,
	WEBAPI_CS_GOAL_RESOLVE_NODE_FAILED,
	WEBAPI_CS_GOAL_NOT_CLUSTERED_NODE,
	WEBAPI_CS_GOAL_INVALID_FORMAT,
	WEBAPI_CS_STANDBY_NOT_SUBMASTER,
	WEBAPI_CS_STANDBY_INVALID_SETTING,
	WEBAPI_CS_STANDBY_INTERNAL,
	WEBAPI_CS_STANDBY_REMAIN_REDO_OPERATION,
	WEBAPI_CS_STANDBY_REMAIN_BACKGROUND_OPERATION,

	WEBAPI_CP_OTHER_REASON = 200,
	WEBAPI_CP_BACKUPNAME_IS_EMPTY,
	WEBAPI_CP_CREATE_BACKUP_PATH_FAILED,
	WEBAPI_CP_BACKUPNAME_ALREADY_EXIST,
	WEBAPI_CP_SHUTDOWN_IS_ALREADY_IN_PROGRESS,
	WEBAPI_CP_BACKUP_MODE_IS_INVALID,
	WEBAPI_CP_PREVIOUS_BACKUP_MODE_IS_NOT_INCREMENTAL_MODE,
	WEBAPI_CP_INCREMENTAL_BACKUP_LEVEL_IS_INVALID,
	WEBAPI_CP_INCREMENTAL_BACKUP_COUNT_EXCEEDS_LIMIT,
	WEBAPI_CP_BACKUPNAME_IS_INVALID,
	WEBAPI_CP_BACKUPNAME_UNMATCH = 210,
	WEBAPI_CP_BACKUP_MODE_IS_NOT_ARCHIVELOG_MODE,
	WEBAPI_CP_PARTITION_STATUS_IS_CHANGED,
	WEBAPI_CP_ARCHIVELOG_MODE_IS_INVALID,
	WEBAPI_CP_LONGTERM_SYNC_IS_NOT_READY,
	WEBAPI_CP_LONG_ARCHIVE_NAME_IS_EMPTY,
	WEBAPI_CP_LONG_ARCHIVE_NAME_ALREADY_EXIST,
	WEBAPI_CP_CREATE_LONG_ARCHIVE_PATH_FAILED,
	WEBAPI_REDO_INVALID_PARTITION_ID,
	WEBAPI_REDO_INVALID_REQUEST_ID,
	WEBAPI_REDO_INVALID_UUID,
	WEBAPI_REDO_REQUEST_ID_IS_NOT_FOUND,
	WEBAPI_REDO_NOT_STANDBY_MODE,
	WEBAPI_REDO_INVALID_CLUSTER_STATUS,
	WEBAPI_REDO_NOT_OWNER,

	WEBAPI_NOT_RECOVERY_CHECKPOINT = 400,
	WEBAPI_CONFIG_SET_ERROR,
	WEBAPI_CONFIG_GET_ERROR,

	WEBAPI_DATABASE = 500,
	WEBAPI_DB_INVALID_PARAMETER
};

/*!
	@brief Event type requested from a client or, another node or, oneself node
*/
enum GSEventType {
	UNDEF_EVENT_TYPE = -1,  

	V_1_1_STATEMENT_START = 0,
	V_1_1_STATEMENT_END = 42,


	CONNECT = 100,		   /*!< connect */
	DISCONNECT,			   /*!< disconnect */
	LOGIN,				   /*!< login */
	LOGOUT,				   /*!< logout */
	GET_PARTITION_ADDRESS, /*!< get owner and backup addresses about the
							  specified partition */
	GET_COLLECTION,		   /*!< same as GET_CONTAINER */
	GET_TIME_SERIES, /*!< obsolete event, not supported for version 2.0 or later
						*/
	PUT_COLLECTION,  /*!< same as PUT_CONTAINER */
	PUT_TIME_SERIES, /*!< obsolete event, not supported for version 2.0 or later
						*/
	DELETE_COLLECTION, /*!< obsolete event, not supported for version 2.0 or
						  later */
	DELETE_TIME_SERIES, /*!< obsolete event, not supported for version 2.0 or
						   later */
	CREATE_SESSION,		/*!< same as CREATE_TRANSACTION_CONTEXT */
	CLOSE_SESSION,		/*!< same as CLOSE_TRANSACTION_CONTEXT */
	V_3_2_CREATE_INDEX,	/*!< obsolete event, not supported for version 3.5 or
						  later */
	V_3_2_DELETE_INDEX,	/*!< obsolete event, not supported for version 3.5 or
						  later */
	CREATE_EVENT_NOTIFICATION, /*!< obsolete event, not supported for version
								  2.0 or later */
	DELETE_EVENT_NOTIFICATION, /*!< obsolete event, not supported for version
								  2.0 or later */
	FLUSH_LOG, /*!< write the results of earlier updates to a non-volatile
				  storage medium */
	COMMIT_TRANSACTION, /*!< commit transaction */
	ABORT_TRANSACTION,  /*!< abort transaction */
	GET_COLLECTION_ROW,   /*!< same as GET_ROW */
	QUERY_COLLECTION_TQL, /*!< same as QUERY_TQL */
	QUERY_COLLECTION_GEOMETRY_RELATED,
	QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
	PUT_COLLECTION_ROW,			 /*!< same as PUT_ROW */
	PUT_COLLECTION_ROWSET,		 /*!< same as PUT_MULTIPLE_ROWS */
	UPDATE_COLLECTION_ROW_BY_ID, /*!< same as UPDATE_ROW_BY_ID */
	DELETE_COLLECTION_ROW,		 /*!< same as REMOVE_ROW */
	DELETE_COLLECTION_ROW_BY_ID, /*!< same as REMOVE_ROW_BY_ID */
	APPEND_TIME_SERIES_ROW, /*!< append a Row with a Row key of the current time
							   in TimeSeries container */
	GET_TIME_SERIES_ROW, /*!< obsolete event, not supported for version 2.0 or
							later */
	GET_TIME_SERIES_ROW_RELATED, /*!< get Row related with the specified time */
	INTERPOLATE_TIME_SERIES_ROW, /*!< perform linear interpolation etc. of a Row
									object corresponding to the specified time
									*/
	AGGREGATE_TIME_SERIES, /*!< perform an aggregation operation on a Row set or
							  its specific Columns, based on the specified start
							  and end times */
	QUERY_TIME_SERIES_TQL, /*!< obsolete event, not supported for version 2.0 or
							  later */
	QUERY_TIME_SERIES_RANGE, /*!< obtain a set of Rows within a specific range
								between the specified start and end times */
	QUERY_TIME_SERIES_SAMPLING, /*!< take a sampling of Rows within a specific
								   range */
	PUT_TIME_SERIES_ROW, /*!< obsolete event, not supported for version 2.0 or
							later */
	PUT_TIME_SERIES_ROWSET, /*!< obsolete event, not supported for version 2.0
							   or later */
	DELETE_TIME_SERIES_ROW, /*!< obsolete event, not supported for version 2.0
							   or later */
	GET_CONTAINER_PROPERTIES,	  /*!< get Container properties */
	GET_COLLECTION_MULTIPLE_ROWS,  /*!< same as GET_MULTIPLE_ROWS */
	GET_TIME_SERIES_MULTIPLE_ROWS, /*!< obsolete event, not supported for
									  version 2.0 or later */
	GET_PARTITION_CONTAINER_NAMES, /*!< get container names in the specified
									  partition */
	DROP_CONTAINER,				   /*!< delete a container */
	CREATE_MULTIPLE_SESSIONS, /*!< same as CREATE_MULTIPLE_TRANSACTION_CONTEXTS
								 */
	CLOSE_MULTIPLE_SESSIONS, /*!< same as CLOSE_MULTIPLE_TRANSACTION_CONTEXTS */
	EXECUTE_MULTIPLE_QUERIES,	/*!< execute multiple queries */
	GET_MULTIPLE_CONTAINER_ROWS, /*!< get Rows in multiple Containers */
	PUT_MULTIPLE_CONTAINER_ROWS, /*!< put Rows in multiple Containers */
	CLOSE_RESULT_SET, /*!< close ResultSet */
	FETCH_RESULT_SET, /*!< fetch ResultSet */
	CREATE_TRIGGER,   /*!< set trigger */
	DELETE_TRIGGER,   /*!< remove trigger */
	GET_CONTAINER = GET_COLLECTION,   /*!< get Container property */
	PUT_CONTAINER = PUT_COLLECTION,   /*!< put Container */
	GET_ROW = GET_COLLECTION_ROW,	 /*!< get Row */
	QUERY_TQL = QUERY_COLLECTION_TQL, /*!< execute TQL */
	PUT_ROW = PUT_COLLECTION_ROW,	 /*!< put Row */
	PUT_MULTIPLE_ROWS = PUT_COLLECTION_ROWSET,
	/*!< put Rows in Collection */
	UPDATE_ROW_BY_ID = UPDATE_COLLECTION_ROW_BY_ID,
	/*!< update Row by Row ID */
	REMOVE_ROW = DELETE_COLLECTION_ROW, /*!< remove Row */
	REMOVE_ROW_BY_ID = DELETE_COLLECTION_ROW_BY_ID,
	/*!< remove Row by Row ID */
	GET_MULTIPLE_ROWS = GET_COLLECTION_MULTIPLE_ROWS,
	/*!< get Rows in Collection */
	CREATE_TRANSACTION_CONTEXT = CREATE_SESSION,
	/*!< create transaction context */
	CLOSE_TRANSACTION_CONTEXT = CLOSE_SESSION,
	/*!< close transaction context */
	CREATE_MULTIPLE_TRANSACTION_CONTEXTS = CREATE_MULTIPLE_SESSIONS,
	/*!< create multiple transaction context */
	CLOSE_MULTIPLE_TRANSACTION_CONTEXTS = CLOSE_MULTIPLE_SESSIONS,
	/*!< close multiple transaction context */
	GET_USERS = 154, /*!< get the information of users */
	PUT_USER,		 /*!< put user */
	DROP_USER,		 /*!< remove user */
	GET_DATABASES,   /*!< get the information of databases */
	PUT_DATABASE,	/*!< put database */
	DROP_DATABASE,   /*!< remove database */
	PUT_PRIVILEGE,   /*!< put privilege */
	DROP_PRIVILEGE,  /*!< remove privilege */
	CREATE_INDEX,		/*!< create index */
	DELETE_INDEX,		/*!< delete index */

	PUT_LARGE_CONTAINER = 300,
	UPDATE_CONTAINER_STATUS,
	CREATE_LARGE_INDEX,
	DROP_LARGE_INDEX,

	SQL_EXECUTE_QUERY = 400,
	SQL_CANCEL_QUERY,
	SQL_TEST,
	SQL_ACK_GET_CONTAINER,
	SQL_RECV_SYNC_REQUEST,
	SQL_CHECK_STATUS,
	FW_DATA,
	FW_CONTROL,
	FW_RESOURCE_CHECK,
	FW_CANCEL,
	FW_DATA_IMMEDIATE,
	SQL_GET_CONTAINER,
	PARTIAL_TQL,

	EXECUTE_JOB,
	CONTROL_JOB,
	CHECK_TIMEOUT_JOB,
	DISPATCH_JOB,
	REMOVE_JOB_TASK,
	SQL_REQUEST_NOSQL_CLIENT,
	REQUEST_CANCEL,
	REMOVE_MULTIPLE_ROWS_BY_ID_SET,
	UPDATE_MULTIPLE_ROWS_BY_ID_SET,
	UPDATE_TABLE_CACHE,
	SEND_EVENT,

	CLIENT_STATEMENT_TYPE_MAX =
		499,  

	REPLICATION_LOG = 500, /*!< replication */
	REPLICATION_ACK,	   /*!< replication ack */
	REPLICATION_LOG2,	   /*!< replication */
	REPLICATION_ACK2,	   /*!< replication ack */

	TXN_COLLECT_TIMEOUT_RESOURCE = 600, /*!< collect timeout resource */
	TXN_CHANGE_PARTITION_TABLE, /*!< change of partition table */
	TXN_CHANGE_PARTITION_STATE, /*!< change of partition state */
	TXN_DROP_PARTITION,			/*!< remove partition */
	RESERVED_EVENT601,			
	CHUNK_EXPIRE_PERIODICALLY,  /*!< periodic removal of expired chunk blocks */
	ADJUST_STORE_MEMORY_PERIODICALLY, /*!< periodic adjustment of store block
										 memory */
	BACK_GROUND,				
	CONTINUE_CREATE_INDEX,
	CONTINUE_ALTER_CONTAINER,
	UPDATE_DATA_STORE_STATUS, 
	PURGE_DATA_AFFINITY_INFO_PERIODICALLY,
	TXN_KEEP_LOG_CHECK_PERIODICALLY,
	TXN_KEEP_LOG_UPDATE,
	TXN_KEEP_LOG_RESET,

	AUTHENTICATION = 700, /*!< authentication */
	AUTHENTICATION_ACK,   /*!< authentication ack */

	V_1_X_REPLICATION_EVENT_START = 1000,
	V_1_X_REPLICATION_EVENT_END = 1005,

	CS_HEARTBEAT = 1100,   /*!< heartbeat */
	CS_HEARTBEAT_RES,	  /*!< heartbeat response */
	CS_NOTIFY_CLUSTER,	 /*!< notify */
	CS_NOTIFY_CLUSTER_RES, /*!< notify response */
	CS_UPDATE_PARTITION,   
	CS_JOIN_CLUSTER,	   /*!< join cluster (Web API) */
	CS_LEAVE_CLUSTER,	  /*!< leave cluster (Web API) */
	CS_INCREASE_CLUSTER, /*!< increase cluster (Web API) */
	CS_DECREASE_CLUSTER, /*!< decrease cluster (Web API) */
	CS_SHUTDOWN_NODE_NORMAL,			 /*!< shutdown node (Web API) */
	CS_SHUTDOWN_NODE_FORCE,				 /*!< force shutdown node (Web API) */
	CS_GOSSIP,							 /*!< gossip */
	CS_SHUTDOWN_CLUSTER,				 /*!< shutdown cluster (Web API) */
	CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN, /*!< checkpoint completed for shutdown
											*/
	CS_COMPLETE_CHECKPOINT_FOR_RECOVERY, /*!< checkpoint completed for recovery
											*/
	CS_ORDER_DROP_PARTITION, /*!< DropPartition event from master node to
								follower nodes */
	CS_SYNC_PARTITION,
	CS_SYNC_PARTITION_RES,


	CS_TIMER_CHECK_CLUSTER,		 /*!< periodic check of cluster status */
	CS_TIMER_NOTIFY_CLUSTER,	 /*!< periodic notification event */
	CS_TIMER_NOTIFY_CLIENT,		 /*!< periodic notification event into client */
	CS_TIMER_CHECK_LOAD_BALANCE, /*!< periodic check of load balancing */
	CS_SYNC_TEST,

	CS_UPDATE_PROVIDER,
	CS_CHECK_PROVIDER,

	CS_TIMER_REQUEST_SQL_NOTIFY_CLIENT,
	CS_TIMER_REQUEST_SQL_CHECK_TIMEOUT,

	CS_NEWSQL_PARTITION_REFRESH,
	CS_NEWSQL_PARTITION_REFRESH_ACK,
	
	CS_SEND_MASTER_INFO,
	CS_UPDATE_LONGTERM_INFO,   


	TXN_SHORTTERM_SYNC_REQUEST =
		2100,					  /*!< request of short-term synchronization */
	TXN_SHORTTERM_SYNC_START,	 /*!< start of short-term synchronization */
	TXN_SHORTTERM_SYNC_START_ACK, /*!< SHORTTERM_SYNC_START ack */
	TXN_SHORTTERM_SYNC_LOG,		  /*!< shor-term synchronization */
	TXN_SHORTTERM_SYNC_LOG_ACK,   /*!< shor-term synchronization ack */
	TXN_SHORTTERM_SYNC_END,		  /*!< end of short-term synchronization */
	TXN_SHORTTERM_SYNC_END_ACK,   /*!< SHORTTERM_SYNC_END ack */
	TXN_LONGTERM_SYNC_REQUEST,	/*!< request of long-term synchronization */
	TXN_LONGTERM_SYNC_START,	  /*!< start of long-term synchronization */
	TXN_LONGTERM_SYNC_START_ACK,  /*!< LONGTERM_SYNC_START ack */
	TXN_LONGTERM_SYNC_CHUNK,	  /*!< long-term chunk synchronization */
	TXN_LONGTERM_SYNC_CHUNK_ACK,  /*!< LONGTERM_SYNC_CHUNK ack */
	TXN_LONGTERM_SYNC_LOG,		  /*!< long-term log synchronization */
	TXN_LONGTERM_SYNC_LOG_ACK,	/*!< LONGTERM_SYNC_LOG ack */
	TXN_SYNC_TIMEOUT,			  /*!< synchronization timeout */
	TXN_LONGTERM_SYNC_PREPARE_ACK,
	TXN_LONGTERM_SYNC_RECOVERY_ACK,

	SYC_SHORTTERM_SYNC_LOG =
		2500,			   /*!< shor-term synchronization via SyncService */
	SYC_LONGTERM_SYNC_LOG, /*!< long-term log synchronization via SyncService */
	SYC_LONGTERM_SYNC_CHUNK, /*!< long-term chunk synchronization via
								SyncService */
	TXN_SYNC_CHECK_END,

	TXN_SYNC_REDO_LOG,
	TXN_SYNC_REDO_LOG_REPLICATION,
	TXN_SYNC_REDO_REMOVE,
	TXN_SYNC_REDO_CHECK,
	TXN_SYNC_REDO_ERROR,

	TXN_SITE_REPLICATION_CONNECT_CHECK,
	TXN_SITE_REPLICATION_CONNECT,
	TXN_SITE_REPLICATION_CONNECT_ACK,
	TXN_SITE_REPLICATION_HEARTBEAT,
	TXN_SITE_REPLICATION_HEARTBEAT_ACK,
	TXN_SITE_REPLICATION_GET_PARTITION_OWNER,
	TXN_SITE_REPLICATION_SET_PARTITION_OWNER,

	TXN_SITE_REPLICATION_REDO_ASYNC,
	TXN_SITE_REPLICATION_REDO_SEMISYNC,
	TXN_SITE_REPLICATION_REDO_SYNC,
	TXN_SITE_REPLICATION_REDO_SYNC_ACK,
	TXN_SITE_REPLICATION_STATUS_CHECK,
	TXN_SITE_REPLICATION_PARTITION_STATUS_CHECK,
	TXN_SITE_REPLICATION_PARTITION_STATUS_CHECK_ACK,



	PARTITION_GROUP_START =
		3000,			 /*!< start of checkpoint among a group of Partitions */
	PARTITION_START,	 /*!< start of Partition checkpoint */
 	EXECUTE_CP,			 /*!< execute (full or partial) checkpoint */
	PARTITION_END,		 /*!< end of Partition checkpoint */
	PARTITION_GROUP_END, /*!< end of checkpoint among a group of Partitions */
	WRITE_LOG_PERIODICALLY, /*!< periodic write to log */
	CLEANUP_CP_DATA,		/*!< cleanup checkpoint information */
	CLEANUP_LOG_FILES,				
	CP_TXN_PREPARE_LONGTERM_SYNC,	
	CP_TXN_STOP_LONGTERM_SYNC,		

	SYS_EVENT_SIMULATE_FAILURE = 3500,
	SYS_EVENT_OUTPUT_STATS, /*!< get statistics (Web API) */


	SQL_NOTIFY_CLIENT = 4000,  
	SQL_TIMER_NOTIFY_CLIENT,  
	SQL_COLLECT_TIMEOUT_CONTEXT,
	SQL_SCAN_CANCELLABLE_EVENT,
	SQL_ACK_AUTHENTICATION,
	SQL_TIMER_CHECK_AUTHENTICATION,
	SQL_CHECK_SQL_CONTAINER,
	SQL_RECV_DDL_CREATE_INDEX,
	SQL_MAIN_PROCESS,
	NOSQL_CANCEL,
	SQL_DDL_RESULT,

	RECV_NOTIFY_MASTER = 5000 /*!< multicasts master address for NoSQL client */
	,
	EVENT_TYPE_MAX
};


class EventTypeUtility {
public:
	static const char8_t* getEventTypeName(EventType eventType);
	static EventType getCategoryEventType(EventType eventType);
	static std::set<EventType> CATEGORY_EVENT_TYPES;
};

#endif
