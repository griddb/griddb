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
    @brief Definition of DatabaseManager
*/
#ifndef DATABASE_MANAGER_H_
#define DATABASE_MANAGER_H_

#include "cluster_common.h"
#include "chunk_manager.h"

class DatabaseManager;

struct DatabaseInfoEntry {
    std::set<DSGroupId> groupIdList_;
    ChunkGroupStats stats_;
};

struct ConnectionStats  {
    ConnectionStats() : sqlConnectionCount_(0), nosqlConnectionCount_(0), sqlRequestCount_(0), nosqlRequestCount_(0) {
    }
    int32_t sqlConnectionCount_;
    int32_t nosqlConnectionCount_;
    int32_t sqlRequestCount_;
    int32_t nosqlRequestCount_;
};

struct JobStats {
    JobStats() : allocateMemorySize_(0), sqlStoreSwapReadSize_(0), sqlStoreSwapWriteSize_(0),
        sqlStoreSize_(0), taskCount_(0), pendingJobCount_(0), sendMessageSize_(0) {}
    void dump(util::NormalOStringStream & oss);
    int64_t allocateMemorySize_;
    int64_t sqlStoreSwapReadSize_;
    int64_t sqlStoreSwapWriteSize_;
    int64_t sqlStoreSize_;
    int32_t taskCount_;
    int32_t pendingJobCount_;
    int64_t sendMessageSize_;
};

struct DatabaseStats {
    DatabaseStats() : chunkStats_(NULL) {}
    ChunkGroupStats chunkStats_;
    ConnectionStats connectionStats_;
    JobStats jobStats_;
};

class Database {
public:
    Database(GlobalVariableSizeAllocator& varAlloc, PartitionId pId, DatabaseManager* dbManager) :
        varAlloc_(varAlloc), pId_(pId), dbMap_(varAlloc), reverseDbMap_(varAlloc_), dbManager_(dbManager) {}
    ~Database();

    void put(DatabaseId dbId, DSGroupId groupId);
    void remove(DatabaseId dbId, DSGroupId groupId);
    void drop(DatabaseId dbId);
    void beginStats();
    void endStats();
    void setStats(DSGroupId groupId, const ChunkGroupStats::Table& stats);
    void getStats(DatabaseId dbId, util::Map<DatabaseId, DatabaseStats*>& statsMap);
    void getStats(util::StackAllocator& alloc, util::Map<DatabaseId, DatabaseStats*> &statsMap);

private:

    struct DabaseEntry {
        DabaseEntry(GlobalVariableSizeAllocator& varAlloc) : varAlloc_(varAlloc), groupIdList_(varAlloc), stats_(NULL), prevStats_(NULL) {}
        GlobalVariableSizeAllocator& varAlloc_;
        util::AllocSet<DSGroupId> groupIdList_;
        ChunkGroupStats stats_;
        ChunkGroupStats prevStats_;
        void dump(util::NormalOStringStream& oss, bool prev = false);
        void dump(util::NormalOStringStream& oss, ChunkGroupStats& stats);
    };

    util::RWLock lock_;
    GlobalVariableSizeAllocator& varAlloc_;
    PartitionId pId_;
    util::AllocMap<DatabaseId, DabaseEntry*> dbMap_;
    util::AllocMap<DSGroupId, DatabaseId> reverseDbMap_;
    DatabaseManager* dbManager_;
};

class DatabaseManager {
public:
    class DatabaseStatScope {
    public:
        DatabaseStatScope(DatabaseManager& dbManager, PartitionId pId);
        ~DatabaseStatScope();
        void set(DSGroupId groupId, const ChunkGroupStats::Table& stats);

    private:
        PartitionId pId_;
        bool setted_;
        DatabaseManager& dbManager_;
    };
    DatabaseManager(ConfigTable& config, GlobalVariableSizeAllocator& varAlloc, const PartitionGroupConfig& groupConfig);
    ~DatabaseManager();

    bool isUseDatabaseStats() {
        return enable_;
    }

    bool isScanStats() {
        return useScanStat_;
    }

    void print(const char* str);
    void print(util::NormalOStringStream& oss);

    void put(PartitionId pId, DatabaseId dbId, DSGroupId groupId);
    void remove(PartitionId pId, DatabaseId dbId, DSGroupId groupId);
    void drop(DatabaseId dbId);
    void beginStats(PartitionId pId);
    void endStats(PartitionId pId);
    void setStats(PartitionId pId, DSGroupId groupId, const ChunkGroupStats::Table& stats);
    void getStats(DatabaseId dbId, util::Map<DatabaseId, DatabaseStats* > & statsMap);
    void getStats(util::StackAllocator& alloc, util::Map<DatabaseId, DatabaseStats*> &statsMap);

    struct ExecutionConstraint {
        static const int32_t MODE_DELAY = 0;
        static const int32_t MODE_DENY = 1;
        static const int32_t MODE_CLEAR = 2;
        static const int32_t MODE_DROP_ONLY = 3;
        static const int32_t MODE_UNDEF = -1;
 
        ExecutionConstraint(int32_t mode, uint32_t delayStartInterval, uint32_t delayExecutionInterval) :
            mode_(mode), delayStartInterval_(delayStartInterval), delayExecutionInterval_(delayExecutionInterval) {}

        static int32_t getMode(const std::string &name) {
            if (name == "DELAY") {
                return MODE_DELAY;
            }
            if (name == "DENY") {
                return MODE_DENY;
            }
            else if (name == "CLEAR") {
                return MODE_CLEAR;
            }
            else {
                return MODE_UNDEF;
            }
        }

        static int32_t getDenyMode(const std::string& name) {
            if (name == "ALL") {
                return MODE_DENY;
            }
            if (name == "UPDATE") {
                return MODE_DROP_ONLY;
            }
            else {
                return MODE_UNDEF;
            }
        }

        static int32_t getDefaultDenyMode() {
            return MODE_DENY;
        }

        const char* getModeName() {
            switch (mode_) {
            case MODE_DELAY: return "DELAY";
            case MODE_DENY: return "DENY";
            case MODE_CLEAR: return "CLEAR";
            default: return "";
            }
        };

        int32_t mode_;
        uint32_t delayStartInterval_;
        uint32_t delayExecutionInterval_;
        void convert(DatabaseId dbId, picojson::object& dbConstraint);

    };
    struct Option {
        Option() : dropOnly_(false) {}
        bool dropOnly_;
    };

    void putExecutionConsraint(DatabaseId dbId, int32_t mode, uint32_t delayStartInterval, uint32_t delayExecutionInterval);
    bool getExecutionConstraint(
        DatabaseId dbId, uint32_t& delayStartInterval, uint32_t& delayExecutionInterval, bool isSQL, Option* opton = NULL);
    void getExecutionConstraintList(DatabaseId dbId, picojson::value& result);
    bool isEnableRequestConstraint() {
        return useRequestConstraint_;
    }

    bool incrementCounter(PartitionId pId) {
        databaseCounterList_[pId]++;
        if (databaseCounterList_[pId] >= checkInterval_) {
            databaseCounterList_[pId] = 0;
            return true;
        }
        else {
            return false;
        }
    }

    bool checkCounter(PartitionId pId) {
        if (databaseCounterList_[pId] >= checkInterval_) {
            databaseCounterList_[pId] = 0;
            return true;
        }
        else {
            return false;
        }
    }

    void incrementRequest(DatabaseId dbId, bool isSQL) {
        if (dbId == UNDEF_DBID) return;
        if (dbId < directMaxDbId_) {
            if (isSQL) {
                sqlDirectRequestCounter_[dbId]++;
            }
            else {
                nosqlDirectRequestCounter_[dbId]++;
            }
        }
        else {
            util::LockGuard<util::Mutex> guard(requestLock_);
            decltype(requestCountMap_)::iterator it = requestCountMap_.find(dbId);
            if (it != requestCountMap_.end()) {
                (*it).second++;
            }
            else {
                requestCountMap_[dbId] = 0;
            }
        }
    }

    int64_t getRequestCount(DatabaseId dbId, bool isSQL) {
        if (dbId == UNDEF_DBID) return 0;
        if (dbId < directMaxDbId_) {
            if (isSQL) {
                return sqlDirectRequestCounter_[dbId];
            }
            else {
               return  nosqlDirectRequestCounter_[dbId];
            }
        }
        else {
            util::LockGuard<util::Mutex> guard(requestLock_);
            decltype(requestCountMap_)::iterator it = requestCountMap_.find(dbId);
            if (it != requestCountMap_.end()) {
                return (*it).second;
            }
            else {
                return 0;
            }
        }
    }

private:

    util::Mutex mutex_;
    GlobalVariableSizeAllocator& varAlloc_;
    const PartitionGroupConfig& config_;
    std::vector<Database*> databaseList_;
    std::vector<int32_t> databaseCounterList_;
    bool enable_;
    bool useScanStat_;

    bool useRequestConstraint_;
    int32_t checkInterval_;
    int32_t limitDelayTime_;
    util::Mutex constraintLock_;
    util::Mutex dumpLock_;
    util::Mutex requestLock_;

    util::AllocMap< DatabaseId, ExecutionConstraint*> constraintMap_;
    std::vector<util::Atomic<int64_t>> nosqlDirectRequestCounter_;
    std::vector<util::Atomic<int64_t>> sqlDirectRequestCounter_;
    std::vector<int64_t> nosqlRequestCounter_;
    std::vector<int64_t> sqlRequestCounter_;
    int32_t directMaxDbId_;
    util::AllocMap< DatabaseId, int64_t> requestCountMap_;
};

class DatabaseStatsMap {
public:
    DatabaseStatsMap(util::StackAllocator& alloc) : statsMap_(alloc), alloc_(alloc) {
    }

    ~DatabaseStatsMap() {
        for (auto& stats : statsMap_) {
            ALLOC_DELETE(alloc_, stats.second);
        }
    }

    util::Map<DatabaseId, DatabaseStats*>& get() {
        return statsMap_;
    }

    util::StackAllocator& getAllocator() {
        return alloc_;
    }

private:
    util::Map<DatabaseId, DatabaseStats*> statsMap_;
    util::StackAllocator& alloc_;
};

#endif
