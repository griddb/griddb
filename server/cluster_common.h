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
#include "cluster_event_type.h"

#include "lru_cache.h"

#include <numeric>

typedef int64_t EventMonotonicTime;
struct UserValue;

typedef util::VariableSizeAllocator<util::Mutex> GlobalVariableSizeAllocator;

typedef util::BasicString< char8_t, std::char_traits<char8_t>,
	util::StdAllocator<char8_t, GlobalVariableSizeAllocator> > UserString; 

typedef CacheNode<UserString, UserValue*,
	GlobalVariableSizeAllocator> UserCacheEntry; 

typedef LruCacheWithMonitor<UserCacheEntry,
	UserString, UserValue*,
	GlobalVariableSizeAllocator> UserCache; 

struct UserValue {
	UserValue(GlobalVariableSizeAllocator& allocator) :
		alloc_(allocator),
		userName_(allocator),
		digest_(allocator),
		dbName_(allocator),
		roleName_(allocator) {}

	GlobalVariableSizeAllocator& alloc_;
	UserString userName_; 
	UserString digest_; 
	UserString dbName_; 
	DatabaseId dbId_; 
	PrivilegeType priv_; 
	UserString roleName_; 
	bool isLDAPAuthenticate_;
	EventMonotonicTime time_; 

	int checkAndGet(UserValue* userValue, int32_t updateInterval) {
		if (userValue->time_ - time_ > updateInterval * 1000) {
			return 2;
		}
		if (userValue->isLDAPAuthenticate_ != isLDAPAuthenticate_) {
			return 2;
		}
		if (strcmp(userValue->digest_.c_str(), digest_.c_str()) == 0) {
			userValue->dbId_ = dbId_;
			userValue->priv_ = priv_;
			userValue->roleName_ = roleName_.c_str();
			return 0;
		}
		else {
			return 1;
		}
	}

	void clear(UserString& name, bool isDatabase) {
		if (isDatabase) {
			if (strcmp(dbName_.c_str(), name.c_str()) == 0) {
				time_ = 0;
			}
		}
		else {
			if (strcmp(userName_.c_str(), name.c_str()) == 0) {
				time_ = 0;
			}
		}
	}

	bool scan(UserString* name, bool isDatabase, picojson::object& cacheInfo) {
		if (name == NULL) {
			cacheInfo["dbName"] = picojson::value(std::string(dbName_.c_str()));
			cacheInfo["userName"] = picojson::value(std::string(userName_.c_str()));
			return true;
		}
		else {
			if (isDatabase) {
				if (strcmp(dbName_.c_str(), name->c_str()) == 0) {
					cacheInfo["dbName"] = picojson::value(std::string(dbName_.c_str()));
					cacheInfo["userName"] = picojson::value(std::string(userName_.c_str()));
					return true;
				}
			}
			else {
				if (strcmp(userName_.c_str(), name->c_str()) == 0) {
					cacheInfo["dbName"] = picojson::value(std::string(dbName_.c_str()));
					cacheInfo["userName"] = picojson::value(std::string(userName_.c_str()));
					return true;
				}
			}
		}
		return false;
	}
};

typedef uint8_t ClusterVersionId;
typedef int32_t NodeId;
typedef int32_t AddressType;
static const ClusterVersionId GS_CLUSTER_MESSAGE_BEFORE_V_1_5 = 1;

typedef std::vector<PartitionId> PartitionIdList;
typedef std::vector<NodeId> NodeIdList;
typedef std::vector<LogSequentialNumber> LsnList;

typedef int64_t PartitionRevisionNo;


const ClusterVersionId GS_CLUSTER_MESSAGE_CURRENT_VERSION = 33;
static const int32_t SERVICE_MAX = 5;

static const uint32_t IMMEDIATE_PARTITION_ID = 0;
static const uint32_t SYSTEM_CONTAINER_PARTITION_ID = 0;

static const int32_t EE_PRIORITY_HIGH = static_cast<int32_t>(-2147483647);

/*!
	@brief Service type
*/
enum ServiceType {
	CLUSTER_SERVICE = 0,
	TRANSACTION_SERVICE = 1,
	SYNC_SERVICE = 2,
	SYSTEM_SERVICE = 3,
	SQL_SERVICE = 4
};

class CommonUtility {
public:

	static void clearStringStream(util::NormalOStringStream& oss) {
		static std::string emptyStr;
		oss.clear();
		oss.str(emptyStr);
	}

	template <typename T>
	static const std::string makeString(util::NormalOStringStream& s, const T& value) {
		clearStringStream(s);
		s << value;
		return s.str();
	}

	static inline std::string getTimeStr(int64_t timeval, bool trim = false) {
		if (timeval == -1) timeval = 0;
		util::NormalOStringStream oss;
		oss.clear();
		util::DateTime dtime(timeval);
		dtime.format(oss, trim);
		return oss.str();
	}

	static const int32_t secLimit = INT32_MAX / 1000;
	static inline int32_t changeTimeSecondToMilliSecond(int32_t sec) {
		if (sec < 0) {
			return sec;
		}
		if (sec > secLimit) {
			return INT32_MAX;
		}
		else {
			return sec * 1000;
		}
	}

	template <class T>
	static void dumpValueList(util::NormalOStringStream& oss, T& list) {
		oss << "(";
		for (size_t pos = 0; pos < list.size(); pos++) {
			oss << list[pos];
			if (pos != list.size() - 1) oss << ",";
		}
		oss << ")";
	}

	template <class T>
	static void dumpValueSet(util::NormalOStringStream& oss, T& list) {
		oss << "(";
		size_t size = list.size();
		size_t count = 0;
		for (auto& entry : list) {
			oss << entry;
			if (count != size - 1) oss << ",";
			count++;
		}
		oss << ")";
	}

	template <class T>
	static void dumpValueByFunction(util::NormalOStringStream& oss, T& list) {
		oss << "(";
		size_t size = list.size();
		size_t count = 0;
		for (auto& entry : list) {
			oss << entry.dump();
			if (count != size - 1) oss << ",";
			count++;
		}
		oss << ")";
	}

	template <class T>
	static double average(T& list) {
		size_t size = list.size();
		if (size == 0) return -1;
		return std::accumulate(std::begin(list), std::end(list), 0.0) /
				static_cast<double>(size);
	}

	template <class T>
	static double variance(T& list) {
		size_t size = list.size();
		if (size == 0) return -1;
		const double ave = average(list);
		return std::accumulate(std::begin(list), std::end(list), 0.0, [ave](double sum, const int32_t& e) {
			const double temp = e - ave;
			return sum + temp * temp;
		}) / static_cast<double>(size);
	}

	template <class T>
	static double standardDeviation(T& list) {
		size_t size = list.size();
		if (size == 0) return -1;
		return std::sqrt(variance(list));
	}

	static void replaceAll(
		std::string& str, const std::string& from, const std::string& to) {
		std::string::size_type pos = 0;
		while (pos = str.find(from, pos), pos != std::string::npos) {
			str.replace(pos, from.size(), to);
			pos += to.size();
		}
	}

	static const char* getServiceTypeNameByEvent(EventType eventType, bool isTransaction) {
		EventType categoryEventType = EventTypeUtility::getCategoryEventType(eventType);
		switch (categoryEventType) {
		case V_1_1_STATEMENT_START:
		case CONNECT:
		case GET_USERS:
		case PUT_LARGE_CONTAINER:
		case REPLICATION_LOG:
		case TXN_COLLECT_TIMEOUT_RESOURCE:
		case AUTHENTICATION:
			return "TRANSACTION_SERVICE";
		case SQL_EXECUTE_QUERY:
		case SQL_NOTIFY_CLIENT:
			return isTransaction ? "TRANSACTION_SERVICE" : "SQL_SERVICE";
		case CS_HEARTBEAT:
		case RECV_NOTIFY_MASTER:
			return "CLUSTER_SERVICE";
		case TXN_SHORTTERM_SYNC_REQUEST:
		case SYC_SHORTTERM_SYNC_LOG:
			return "SYNC_SERVICE";
		case PARTITION_GROUP_START:
			return "CHECKPOINT_SERVICE";
		case SYS_EVENT_SIMULATE_FAILURE:
			return "SYSTEM_SERVICE";
		default:
			return "UNDEF_SERVICE";
		}
	}
};

#define TRACE_CLUSTER_NORMAL_OPERATION(level, str) \
	GS_TRACE_##level(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, str);

#define UTIL_TRACE_EXCEPTION_ERROR(tracer, cause, message) \
	UTIL_TRACER_PUT(tracer, LEVEL_ERROR, message, &(cause))

#define WATCHER_START util::Stopwatch watch(util::Stopwatch::STATUS_STARTED);

#define WATCHER_END_NORMAL(eventType, pgId)                                 \
	{                                                            \
		const uint32_t lap = watch.elapsedMillis();              \
		if (lap > IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS) { \
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_EVENT, \
				"eventType=" << EventTypeUtility::getEventTypeName(eventType) << ", pgId=" << pgId << ", elapsedMillis=" << lap);  \
		}                                                        \
	}

#define WATCHER_END_DETAIL(eventType, pId, pgId)                             \
	{                                                                   \
		const uint32_t lap = watch.elapsedMillis();                     \
		if (lap > IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS) {        \
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_EVENT,        \
				"eventType=" << EventTypeUtility::getEventTypeName(eventType) << ", pId=" << pId            \
							<< ", pgId=" << pgId << ", elapsedMillis=" << lap); \
		}                                                               \
	}

UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
#define GS_TRACE_CLUSTER_INFO(s) \
	GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); \
UTIL_TRACER_DECLARE(CLUSTER_DUMP);

typedef std::pair<int32_t, bool> ServiceTypeInfo;

class picojsonUtil {
public:
	static picojson::array& setJsonArray(picojson::value& value) {
		value = picojson::value(picojson::array_type, false);
		return value.get<picojson::array>();
	}

	static picojson::object& setJsonObject(picojson::value& value) {
		value = picojson::value(picojson::object_type, false);
		return value.get<picojson::object>();
	}
};



#endif
