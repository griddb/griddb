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
	@brief Common Definition of ClusterManager and ClusterService
*/

#ifndef CLUSTER_COMMON_H_
#define CLUSTER_COMMON_H_

#include "data_type.h"

typedef uint8_t ClusterVersionId;
typedef int32_t NodeId;
typedef int32_t AddressType;
static const ClusterVersionId GS_CLUSTER_MESSAGE_BEFORE_V_1_5 = 1;

typedef int64_t PartitionRevisionNo;

const ClusterVersionId GS_CLUSTER_MESSAGE_CURRENT_VERSION = 31;

static const int32_t SERVICE_MAX = 4;

/*!
	@brief Status of ChangePartition
*/
enum ChangePartitionType {
	PT_CHANGE_NORMAL,
	PT_CHANGE_SYNC_START,
	PT_CHANGE_SYNC_END
};
/*!
	@brief Synchronization mode
*/
enum SyncMode {
	MODE_SHORTTERM_SYNC,
	MODE_LONGTERM_SYNC,
	MODE_CHANGE_PARTITION,
	MODE_SYNC_TIMEOUT
};
/*!
	@brief Synchronization type
*/
enum SyncType {
	LOG_SYNC,	
	CHUNK_SYNC,  
};
/*!
	@brief Service type
*/
enum ServiceType {
	CLUSTER_SERVICE = 0,
	TRANSACTION_SERVICE = 1,
	SYNC_SERVICE = 2,
	SYSTEM_SERVICE = 3,
};

static const char8_t *const SERVICE_TYPE_NAMES[] = {
	"cluster", "transaction", "sync", "system"
};

static void clearStringStream(util::NormalOStringStream &oss) {
	static std::string emptyStr;
	oss.clear();
	oss.str(emptyStr);
}

template <typename T>
static const std::string makeString(
	util::NormalOStringStream &s, const T &value) {
	clearStringStream(s);
	s << value;
	return s.str();
}

static const int32_t EE_PRIORITY_HIGH = static_cast<int32_t>(-2147483647);

static inline std::string getTimeStr(int64_t timeval) {
	util::NormalOStringStream oss;
	oss.clear();
	util::DateTime dtime(timeval);
	dtime.format(oss, true);
	return oss.str();
}

static const int32_t secLimit = INT32_MAX / 1000;
static inline int32_t changeTimeSecToMill(int32_t sec) {
	if (sec > secLimit) {
		return INT32_MAX;
	}
	else {
		return sec * 1000;
	}
}

#define TRACE_CLUSTER_EXCEPTION_FORCE(errorCode, message)       \
	try {                                                       \
		GS_THROW_USER_ERROR(errorCode, message);                \
	}                                                           \
	catch (std::exception & e) {                                \
		UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, ""); \
	}

#define TRACE_CLUSTER_EXCEPTION_FORCE_ERROR(errorCode, message)       \
	try {                                                       \
		GS_THROW_USER_ERROR(errorCode, message);                \
	}                                                           \
	catch (std::exception & e) {                                \
		UTIL_TRACE_EXCEPTION_ERROR(CLUSTER_OPERATION, e, ""); \
	}


#define TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, level, str)   \
	GS_TRACE_##level(CLUSTER_SERVICE, GS_TRACE_CS_HANDLER_DETAIL, \
		str << ", eventType:" << getEventTypeName(eventType)      \
			<< ", sender:" << ev.getSenderND());

#define TRACE_SYNC_HANDLER_DETAIL(ev, eventType, pId, level, str) \
	GS_TRACE_##level(SYNC_SERVICE, GS_TRACE_SYNC_HANDLER_DETAIL,  \
		str << ", eventType:" << getEventTypeName(eventType)      \
			<< ", pId=" << pId << ", sender:" << ev.getSenderND());

#define TRACE_CLUSTER_HANDLER(eventType, level, str)       \
	GS_TRACE_##level(CLUSTER_SERVICE, GS_TRACE_CS_HANDLER, \
		"{" << str << "}, eventType:" << getEventTypeName(eventType));

#define TRACE_SYNC_HANDLER(eventType, pId, level, str)               \
	GS_TRACE_##level(SYNC_SERVICE, GS_TRACE_SYNC_HANDLER,            \
		"{" << str << "}, eventType:" << getEventTypeName(eventType) \
			<< ", pId=" << pId);

#define TRACE_CLUSTER_OPERATION(eventType, level, str)         \
	GS_TRACE_##level(CLUSTER_OPERATION, GS_TRACE_CS_OPERATION, \
		"{" << str << "}, eventType:" << getEventTypeName(eventType));

#define TRACE_CLUSTER_OPERATION_CONDITION(eventType, level, str, cond)    \
	if (cond) {                                                           \
		GS_TRACE_##level(CLUSTER_OPERATION, GS_TRACE_CS_OPERATION,        \
			"{" << str << "}, eventType:" << getEventTypeName(eventType)) \
	};

#define TRACE_CLUSTER_NORMAL(level, str) \
	GS_TRACE_##level(CLUSTER_SERVICE, GS_TRACE_CS_NORMAL, str);

#define TRACE_CLUSTER_NORMAL_CONDITION(cond, level1, level2, str)    \
	if (cond) {                                                      \
		GS_TRACE_##level1(CLUSTER_SERVICE, GS_TRACE_CS_NORMAL, str); \
	}                                                                \
	else {                                                           \
		GS_TRACE_##level2(CLUSTER_SERVICE, GS_TRACE_CS_NORMAL, str); \
	}

#define TRACE_CLUSTER_NORMAL_OPERATION(level, str) \
	GS_TRACE_##level(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, str);

#define TRACE_SYNC_NORMAL(level, str) \
	GS_TRACE_##level(SYNC_SERVICE, GS_TRACE_SYNC_NORMAL, str);

#define UTIL_TRACE_EXCEPTION_ERROR(tracer, cause, message) \
	UTIL_TRACER_PUT(tracer, LEVEL_ERROR, message, &(cause))

#define TRACE_CLUSTER_EXCEPTION(e, eventType, level, str) \
	UTIL_TRACE_EXCEPTION_##level(CLUSTER_SERVICE, e,      \
		str << ", eventType=" << getEventTypeName(eventType));

#define TRACE_SYNC_EXCEPTION(e, eventType, pId, level, str)                 \
	UTIL_TRACE_EXCEPTION_##level(SYNC_SERVICE, e,                           \
		str << ", eventType=" << getEventTypeName(eventType) \
				<< ", pId=" << pId);

#define TRACE_CLUSTER_EE_SEND(eventType, nd, level, str)             \
	GS_TRACE_##level(CLUSTER_SERVICE, GS_TRACE_CS_EVENT_SEND,        \
		str << ", eventType=" << getEventTypeName(eventType) \
			<< ", nd=" << nd);

#define WATCHER_START util::Stopwatch watch(util::Stopwatch::STATUS_STARTED);

#define WATCHER_END_NORMAL(eventType)                                 \
	{                                                            \
		const uint32_t lap = watch.elapsedMillis();              \
		if (lap > IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS) { \
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_EVENT, \
				"eventType=" << eventType << ", time=" << lap);  \
		}                                                        \
	}

#define WATCHER_END_SYNC(eventType, pId)                            \
	{                                                            \
		const uint32_t lap = watch.elapsedMillis();              \
		if (lap > IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS) { \
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_EVENT, \
				"eventType=" << eventType << ", pId=" << pId     \
							<< ", time=" << lap);               \
		}                                                        \
	}

#define WATCHER_END_DETAIL(eventType, pId, pgId)                             \
	{                                                                   \
		const uint32_t lap = watch.elapsedMillis();                     \
		if (lap > IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS) {        \
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_EVENT,        \
				"eventType=" << eventType << ", pId=" << pId            \
							<< ", pgId=" << pgId << ", time=" << lap); \
		}                                                               \
	}

static inline const std::string dumpChangePartitionType(
	ChangePartitionType type) {
	switch (type) {
	case PT_CHANGE_NORMAL:
		return "NORMAL";
	case PT_CHANGE_SYNC_START:
		return "SYNC_START";
	case PT_CHANGE_SYNC_END:
		return "SYNC_END";
	default:
		return "";
	}
}

template <class T>
static inline void mergeList(std::ostream &ss, T &list) {
	ss << "[";
	for (size_t pos = 0; pos < list.size(); pos++) {
		ss << list[pos].dump();
		if (pos != list.size() - 1) {
			ss << ",";
		}
	}
	ss << "]";
}

template <class T>
static inline std::string dumpList(T &list) {
	util::NormalOStringStream ss;
	ss << "[";
	for (size_t pos = 0; pos < list.size(); pos++) {
		ss << list[pos].dump();
		if (pos != list.size() - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss.str();
}

template <class T>
static inline std::string dumpArray(T &list) {
	util::NormalOStringStream ss;
	ss << "[";
	for (size_t pos = 0; pos < list.size(); pos++) {
		ss << list[pos];
		if (pos != list.size() - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss.str();
}

static inline std::ostream &operator<<(
	std::ostream &ss, util::XArray<PartitionId> &list) {
	ss << "[";
	for (size_t pos = 0; pos < list.size(); pos++) {
		ss << list[pos];
		if (pos != list.size() - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss;
}

static inline std::ostream &operator<<(
	std::ostream &ss, std::vector<PartitionId> &list) {
	ss << "[";
	for (size_t pos = 0; pos < list.size(); pos++) {
		ss << list[pos];
		if (pos != list.size() - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss;
}

static inline std::ostream &operator<<(
	std::ostream &ss, std::vector<NodeId> &list) {
	ss << "[";
	for (size_t pos = 0; pos < list.size(); pos++) {
		ss << list[pos];
		if (pos != list.size() - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss;
}

static inline std::ostream &operator<<(
	std::ostream &ss, std::set<NodeId> &list) {
	size_t size = list.size();
	size_t pos = 0;
	ss << "[";
	for (std::set<NodeId>::iterator it = list.begin(); it != list.end(); it++) {
		ss << *it;
		if (pos != size - 1) {
			ss << ",";
		}
		pos++;
	}
	ss << "]";
	return ss;
}

extern bool g_clusterDump;

#define SET_CLUSTER_DUMP(flag) {g_clusterDump = flag;}

#define DUMP_CLUSTER(s) {if (g_clusterDump) { \
	GS_TRACE_INFO(CLUSTER_DUMP, GS_TRACE_CS_TRACE_DUMP, s); \
	std::cout << s << std::endl; \
	}}


#endif
