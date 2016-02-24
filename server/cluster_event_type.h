/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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

	WEBAPI_CP_OTHER_REASON = 200,
	WEBAPI_RESERVED_ETYPE201,
	WEBAPI_RESERVED_ETYPE202,
	WEBAPI_RESERVED_ETYPE203,
	WEBAPI_CP_SHUTDOWN_IS_ALREADY_IN_PROGRESS,
	WEBAPI_RESERVED_ETYPE204,
	WEBAPI_RESERVED_ETYPE205,
	WEBAPI_RESERVED_ETYPE206,
	WEBAPI_RESERVED_ETYPE207,

	WEBAPI_RESERVED_ETYPE210 = 210,
	WEBAPI_RESERVED_ETYPE211,
	WEBAPI_CP_PARTITION_STATUS_IS_CHANGED,
	WEBAPI_RESERVED_ETYPE212,

	WEBAPI_NOT_RECOVERY_CHECKPOINT = 400,
	WEBAPI_CONFIG_SET_ERROR,
	WEBAPI_CONFIG_GET_ERROR
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
	CREATE_SESSION,		/*!< same as CREATE_TRSANSACTION_CONTEXT */
	CLOSE_SESSION,		/*!< same as CLOSE_TRSANSACTION_CONTEXT */
	CREATE_INDEX,		/*!< create index */
	DELETE_INDEX,		/*!< delete index */
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
	RESERVED_EVENT1,
	RESERVED_EVENT2,
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
	RESERVED_EVENT162 = 162,
	RESERVED_EVENT163,

	CLIENT_STATEMENT_TYPE_MAX =
		499,  

	REPLICATION_LOG = 500, /*!< replication */
	REPLICATION_ACK,	   /*!< replication ack */

	TXN_COLLECT_TIMEOUT_RESOURCE = 600, /*!< collect timeout resource */
	TXN_CHANGE_PARTITION_TABLE, /*!< change of partition table */
	TXN_CHANGE_PARTITION_STATE, /*!< change of partition state */
	TXN_DROP_PARTITION,			/*!< remove partition */
	RESERVED_EVENT601,			
	CHUNK_EXPIRE_PERIODICALLY,  /*!< periodic removal of expired chunk blocks */
	ADJUST_STORE_MEMORY_PERIODICALLY, /*!< periodic adjustment of store block
										 memory */

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
	RESERVED_EVENT1101,
	RESERVED_EVENT1102,
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
	RESERVED_EVENT1103,
	RESERVED_EVENT1104,


	CS_TIMER_CHECK_CLUSTER,		 /*!< periodic check of cluster status */
	CS_TIMER_NOTIFY_CLUSTER,	 /*!< periodic notification event */
	CS_TIMER_NOTIFY_CLIENT,		 /*!< periodic notification event into client */
	CS_TIMER_CHECK_LOAD_BALANCE, /*!< periodic check of load balancing */
	CS_SYNC_TEST,


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

	SYC_SHORTTERM_SYNC_LOG =
		2500,			   /*!< shor-term synchronization via SyncService */
	SYC_LONGTERM_SYNC_LOG, /*!< long-term log synchronization via SyncService */
	SYC_LONGTERM_SYNC_CHUNK, /*!< long-term chunk synchronization via
								SyncService */

	PARTITION_GROUP_START =
		3000,			 /*!< start of checkpoint among a group of Partitions */
	PARTITION_START,	 /*!< start of Partition checkpoint */
	COPY_CHUNK,			 /*!< copy chunk */
	PARTITION_END,		 /*!< end of Partition checkpoint */
	PARTITION_GROUP_END, /*!< end of checkpoint among a group of Partitions */
	WRITE_LOG_PERIODICALLY, /*!< periodic write to log */
	CLEANUP_CP_DATA,		/*!< cleanup checkpoint information */
	RESERVED_EVENT3001,

	SYS_EVENT_SIMULATE_FAILURE = 3500,
	SYS_EVENT_OUTPUT_STATS, /*!< get statistics (Web API) */



	RECV_NOTIFY_MASTER = 5000 /*!< multicasts master address for NoSQL client */
	,
	EVENT_TYPE_MAX
};

#define CASE_EVENT(eventType) \
	case eventType:           \
		return #eventType

static inline const std::string getEventTypeName(EventType eventType) {
	switch (eventType) {
		CASE_EVENT(CS_HEARTBEAT);
		CASE_EVENT(CS_HEARTBEAT_RES);
		CASE_EVENT(CS_NOTIFY_CLUSTER);
		CASE_EVENT(CS_NOTIFY_CLUSTER_RES);
		CASE_EVENT(CS_UPDATE_PARTITION);
		CASE_EVENT(CS_JOIN_CLUSTER);
		CASE_EVENT(CS_LEAVE_CLUSTER);
		CASE_EVENT(CS_SHUTDOWN_NODE_NORMAL);
		CASE_EVENT(CS_SHUTDOWN_NODE_FORCE);
		CASE_EVENT(CS_GOSSIP);
		CASE_EVENT(CS_SHUTDOWN_CLUSTER);
		CASE_EVENT(CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);
		CASE_EVENT(CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
		CASE_EVENT(CS_TIMER_CHECK_CLUSTER);
		CASE_EVENT(CS_TIMER_NOTIFY_CLUSTER);
		CASE_EVENT(CS_TIMER_NOTIFY_CLIENT);
		CASE_EVENT(CS_TIMER_CHECK_LOAD_BALANCE);
		CASE_EVENT(CS_ORDER_DROP_PARTITION);
		CASE_EVENT(TXN_SHORTTERM_SYNC_REQUEST);
		CASE_EVENT(TXN_SHORTTERM_SYNC_START);
		CASE_EVENT(TXN_SHORTTERM_SYNC_START_ACK);
		CASE_EVENT(TXN_SHORTTERM_SYNC_LOG);
		CASE_EVENT(TXN_SHORTTERM_SYNC_LOG_ACK);
		CASE_EVENT(TXN_SHORTTERM_SYNC_END);
		CASE_EVENT(TXN_SHORTTERM_SYNC_END_ACK);
		CASE_EVENT(TXN_LONGTERM_SYNC_REQUEST);
		CASE_EVENT(TXN_LONGTERM_SYNC_START);
		CASE_EVENT(TXN_LONGTERM_SYNC_START_ACK);
		CASE_EVENT(TXN_LONGTERM_SYNC_CHUNK);
		CASE_EVENT(TXN_LONGTERM_SYNC_CHUNK_ACK);
		CASE_EVENT(TXN_LONGTERM_SYNC_LOG);
		CASE_EVENT(TXN_LONGTERM_SYNC_LOG_ACK);
		CASE_EVENT(TXN_SYNC_TIMEOUT);
		CASE_EVENT(SYC_SHORTTERM_SYNC_LOG);
		CASE_EVENT(SYC_LONGTERM_SYNC_LOG);
		CASE_EVENT(SYC_LONGTERM_SYNC_CHUNK);
		CASE_EVENT(TXN_CHANGE_PARTITION_TABLE);
		CASE_EVENT(TXN_CHANGE_PARTITION_STATE);
		CASE_EVENT(TXN_DROP_PARTITION);
		CASE_EVENT(CLEANUP_CP_DATA);
		CASE_EVENT(PARTITION_GROUP_START);
		CASE_EVENT(PARTITION_START);
		CASE_EVENT(COPY_CHUNK);
		CASE_EVENT(PARTITION_END);
		CASE_EVENT(PARTITION_GROUP_END);
		CASE_EVENT(CHUNK_EXPIRE_PERIODICALLY);
	default:
		break;
	}
	return std::string("UNDEF_EVENT_TYPE");
}

#endif
