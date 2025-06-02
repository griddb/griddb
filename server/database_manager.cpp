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
	@brief Implementation of DatabaseManager
*/
#include "database_manager.h"
#include "sql_common.h"
#include "transaction_context.h"

Database::~Database() {
	for (auto& dbEntry : dbMap_) {
		ALLOC_VAR_SIZE_DELETE(varAlloc_, dbEntry.second);
	}
}

void Database::put(DatabaseId dbId, DSGroupId groupId) {
	if (dbId == UNDEF_DBID || groupId == UNDEF_DS_GROUPID) {
		return;
	}
	util::LockGuard<util::WriteLock> guard(lock_);
	auto db = dbMap_.find(dbId);
	if (db != dbMap_.end()) {
		return;
	}
	dbMap_[dbId] = ALLOC_VAR_SIZE_NEW(varAlloc_) DabaseEntry(varAlloc_);
	for (DSGroupId pos = 0; pos < 2; pos++) {
		DSGroupId currentGroupId = groupId + pos;
		dbMap_[dbId]->groupIdList_.insert(currentGroupId);
		reverseDbMap_[currentGroupId] = dbId;
	}
}

void Database::remove(DatabaseId dbId, DSGroupId groupId) {
	if (dbId == UNDEF_DBID || groupId == UNDEF_DS_GROUPID) {
		return;
	}
	util::LockGuard<util::WriteLock> guard(lock_);
	auto db = dbMap_.find(dbId);
	if (db == dbMap_.end()) {
		return;
	}
	for (DSGroupId pos = 0; pos < 2; pos++) {
		DSGroupId currentGroupId = groupId + pos;
		db->second->groupIdList_.erase(currentGroupId);
		reverseDbMap_.erase(currentGroupId);
	}
	if (db->second->groupIdList_.empty()) {
		ALLOC_VAR_SIZE_DELETE(varAlloc_, db->second);
		dbMap_.erase(db);
	}
}

void Database::drop(DatabaseId dbId) {
	if (dbId == UNDEF_DBID) {
		return;
	}
	util::LockGuard<util::WriteLock> guard(lock_);
	auto db = dbMap_.find(dbId);
	if (db == dbMap_.end()) {
		return;
	}
	for (auto& group : db->second->groupIdList_) {
		reverseDbMap_.erase(group);
	}
	ALLOC_VAR_SIZE_DELETE(varAlloc_, db->second);
	dbMap_.erase(db);
}

void Database::setStats(DSGroupId groupId, const ChunkGroupStats::Table& stats) {
	if (groupId == UNDEF_DS_GROUPID) {
		return;
	}
	util::LockGuard<util::WriteLock> guard(lock_);
	auto group = reverseDbMap_.find(groupId);
	if (group != reverseDbMap_.end()) {
		auto db = dbMap_.find(group->second);
		if (db != dbMap_.end()) {
				db->second->prevStats_.table_.merge(stats);
		}
	}
}

void Database::beginStats() {
	util::LockGuard<util::WriteLock> guard(lock_);
	for (auto& db : dbMap_) {
		ChunkGroupStats::Table  table(NULL);
		db.second->prevStats_.table_.assign(table);
	}
}

void Database::endStats() {
	util::LockGuard<util::WriteLock> guard(lock_);
	for (auto& db : dbMap_) {
		db.second->stats_.table_.assign(db.second->prevStats_.table_);
	}
}

void Database::getStats(DatabaseId dbId, util::Map<DatabaseId, DatabaseStats*>& statsMap) {
	if (dbId == UNDEF_DBID) {
		return;
	}
	util::LockGuard<util::ReadLock> guard(lock_);
	auto db = dbMap_.find(dbId);
	if (db != dbMap_.end()) {
		statsMap[dbId]->chunkStats_.table_.merge(db->second->stats_.table_);
	}
}

void Database::getStats(util::StackAllocator& alloc, util::Map<DatabaseId, DatabaseStats*>& statsMap) {
	util::LockGuard<util::ReadLock> guard(lock_);
	for (auto& db : dbMap_) {
		DatabaseId dbId = db.first;
		auto stats = statsMap.find(dbId);
		if (stats == statsMap.end()) {
			statsMap[dbId] = ALLOC_NEW(alloc) DatabaseStats;
		}
		statsMap[dbId]->chunkStats_.table_.merge(db.second->stats_.table_);
	}
}


DatabaseManager::DatabaseManager(ConfigTable& configTable, GlobalVariableSizeAllocator& varAlloc, const PartitionGroupConfig& groupConfig) :
	varAlloc_(varAlloc), config_(groupConfig),
	enable_(configTable.get<bool>(CONFIG_TABLE_TXN_USE_MULTITENANT_MODE)), 
	useScanStat_(configTable.get<bool>(CONFIG_TABLE_TXN_USE_SCAN_STAT)),
	useRequestConstraint_(configTable.get<bool>(CONFIG_TABLE_TXN_USE_REQUEST_CONSTRAINT)),
	checkInterval_(configTable.get<int32_t>(CONFIG_TABLE_TXN_CHECK_DATABASE_STATS_INTERVAL)),
	limitDelayTime_(1000 * configTable.get<int32_t>(CONFIG_TABLE_TXN_LIMIT_DELAY_TIME)),
	constraintMap_(varAlloc), requestCountMap_(varAlloc) {
	for (PartitionId pId = 0; pId < groupConfig.getPartitionCount(); pId++) {
		databaseList_.push_back(ALLOC_VAR_SIZE_NEW(varAlloc_) Database(varAlloc_, pId, this));
	}
	databaseCounterList_.assign(groupConfig.getPartitionCount(), 0);
	directMaxDbId_ = TXN_DATABASE_NUM_MAX;
	util::Atomic<int64_t> initValue;
	initValue = 0;
	nosqlDirectRequestCounter_.assign(directMaxDbId_, initValue);
	sqlDirectRequestCounter_.assign(directMaxDbId_, initValue);
}

DatabaseManager::~DatabaseManager()  {
	for (auto& db : databaseList_) {
		ALLOC_VAR_SIZE_DELETE(varAlloc_, db);
	}
	for (auto& dbConstraint : constraintMap_) {
		ALLOC_VAR_SIZE_DELETE(varAlloc_, dbConstraint.second);
	}
}

DatabaseManager::DatabaseStatScope::DatabaseStatScope(
		DatabaseManager& dbManager, PartitionId pId) :
		pId_(pId), setted_(false), dbManager_(dbManager) {
	if (dbManager_.isUseDatabaseStats() && dbManager_.incrementCounter(pId)) {
		setted_ = true;
		dbManager_.beginStats(pId_);
	}
}

DatabaseManager::DatabaseStatScope::~DatabaseStatScope() {
	if (dbManager_.isUseDatabaseStats() && setted_) {
		dbManager_.endStats(pId_);
	}
}

void DatabaseManager::DatabaseStatScope::set(DSGroupId groupId, const ChunkGroupStats::Table & stats) {
	dbManager_.setStats(pId_, groupId, stats);
}

void DatabaseManager::put(PartitionId pId, DatabaseId dbId, DSGroupId groupId) {
	if (!enable_) return;
	databaseList_[pId]->put(dbId, groupId);
}

void DatabaseManager::remove(PartitionId pId, DatabaseId dbId, DSGroupId groupId) {
	if (!enable_) return;
	databaseList_[pId]->remove(dbId, groupId);
}

void DatabaseManager::drop(DatabaseId dbId) {
	if (!enable_) return;
	for (size_t pId = 0; pId < databaseList_.size(); pId++) {
		databaseList_[pId]->drop(dbId);
	}
}

void DatabaseManager::beginStats(PartitionId pId) {
	if (!enable_) return;
	databaseList_[pId]->beginStats();
}

void DatabaseManager::endStats(PartitionId pId) {
	if (!enable_) return;
	databaseList_[pId]->endStats();
}

void DatabaseManager::setStats(PartitionId pId, DSGroupId groupId, const ChunkGroupStats::Table& stats) {
	if (!enable_) return;
	databaseList_[pId]->setStats(groupId, stats);
}

void DatabaseManager::getStats(DatabaseId dbId, util::Map<DatabaseId, DatabaseStats*>& statsMap) {
	if (!enable_) return;
	for (auto& db : databaseList_) {
		db->getStats(dbId, statsMap);
	}
}

void DatabaseManager::getStats(util::StackAllocator& alloc, util::Map<DatabaseId, DatabaseStats*> &statsMap) {
	if (!enable_) return;
	for (auto& db : databaseList_) {
		db->getStats(alloc, statsMap);
	}
}

void DatabaseManager:: putExecutionConsraint(DatabaseId dbId, int32_t mode, uint32_t delayStartInterval, uint32_t delayExecutionInterval) {
	if (!useRequestConstraint_) return;
	util::LockGuard<util::Mutex> guard(constraintLock_);
	if (dbId == UNDEF_DBID) {
		if (mode == ExecutionConstraint::MODE_CLEAR) {
			for (auto& constEntry : constraintMap_) {
				ALLOC_VAR_SIZE_DELETE(varAlloc_, constEntry.second);
			}
			constraintMap_.clear();
			return;
		}
		else {
			return;
		}
	}
	decltype(constraintMap_)::iterator it = constraintMap_.find(dbId);
	if (it != constraintMap_.end()) {
		ALLOC_VAR_SIZE_DELETE(varAlloc_, (*it).second);
		constraintMap_.erase(it);
		if (mode == ExecutionConstraint::MODE_CLEAR) {
			return;
		}
	}
	if (mode != ExecutionConstraint::MODE_CLEAR) {
		if (delayStartInterval > static_cast<uint32_t>(limitDelayTime_)) {
			delayStartInterval = limitDelayTime_;
		}
		if (delayExecutionInterval > static_cast<uint32_t>(limitDelayTime_)) {
			delayExecutionInterval = limitDelayTime_;
		}
		constraintMap_.insert(std::make_pair(dbId, ALLOC_VAR_SIZE_NEW(varAlloc_)
			ExecutionConstraint(mode, delayStartInterval, delayExecutionInterval)));
	}
}

bool DatabaseManager::getExecutionConstraint(
	DatabaseId dbId,  uint32_t& delayStartInterval, uint32_t& delayExecutionInterval, bool isSQL, Option* option) {
	
	if (!useRequestConstraint_) return false;
	if (dbId < 0 && dbId > MAX_DBID) {
		return false;
	}
	util::LockGuard<util::Mutex> guard(constraintLock_);
	decltype(constraintMap_)::iterator it = constraintMap_.find(dbId);
	if (it != constraintMap_.end()) {
		if ((*it).second->mode_ == ExecutionConstraint::MODE_DENY) {
			if (isSQL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DENY_REQUEST,
					"Request constraints occured, rule is DENY(SQL)");
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DENY_REQUEST,
					"Request constraints occured, rule is DENY(NoSQL)");
			}
		}
		if ((*it).second->mode_ == ExecutionConstraint::MODE_DROP_ONLY) {
			if (!option || (option && !option->dropOnly_)) {
				if (isSQL) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DENY_REQUEST,
						"Request constraints occured, rule is DENY_UPDATE(SQL)");
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_DENY_REQUEST,
						"Request constraints occured, No nosql request is denied on rule of DENY_UPDATE(NoSQL)");
				}
			}
		}
		delayStartInterval = (*it).second->delayStartInterval_;
		delayExecutionInterval = (*it).second->delayExecutionInterval_;
		return true;
	}
	return false;
}

void DatabaseManager::ExecutionConstraint::convert(DatabaseId dbId, picojson::object& dbConstraint) {
	util::NormalOStringStream oss;
	dbConstraint["dbId"] = picojson::value(static_cast<double>(dbId));
	switch (mode_) {
	case MODE_DENY: {
		dbConstraint["mode"] = picojson::value("DENY");
		dbConstraint["denyCommand"] = picojson::value("ALL");
		break;
	}
	case MODE_DROP_ONLY: {
		dbConstraint["mode"] = picojson::value("DENY");
		dbConstraint["denyCommand"] = picojson::value("UPDATE");
		break;
	}
	case MODE_DELAY: {
		dbConstraint["mode"] = picojson::value("DELAY");
		if (delayStartInterval_ > 0) {
			dbConstraint["requestDelayTime"] = picojson::value(static_cast<double>(delayStartInterval_));
		}
		if (delayExecutionInterval_ > 0) {
			dbConstraint["eventDelayTime"] = picojson::value(static_cast<double>(delayExecutionInterval_));
		}
		break;
	}
	case MODE_CLEAR: {
		dbConstraint["mode"] = picojson::value("CLEAR");
		break;
	}
	default:
		break;
	}
}

void DatabaseManager::getExecutionConstraintList(DatabaseId dbId, picojson::value& result) {
	if (!useRequestConstraint_) return;
	util::LockGuard<util::Mutex> guard(constraintLock_);
	picojson::array& constraintList = picojsonUtil::setJsonArray(result);
	if (dbId != UNDEF_DBID) {
		decltype(constraintMap_)::iterator it = constraintMap_.find(dbId);
		if (it != constraintMap_.end()) {
			picojson::object dbConstraint;
			(*it).second->convert(dbId, dbConstraint);
			constraintList.push_back(picojson::value(dbConstraint));
		}
	}
	else {
		for (auto& entry : constraintMap_) {
			picojson::object dbConstraint;
			entry.second->convert(entry.first, dbConstraint);
			constraintList.push_back(picojson::value(dbConstraint));
		}
	}
}

void JobStats::dump(util::NormalOStringStream& oss) {
	oss << "jobstats [";
	oss << allocateMemorySize_ << "," << sqlStoreSwapReadSize_ << "," << sqlStoreSwapWriteSize_ << ","
		<< "," << sqlStoreSize_ << "," << taskCount_ << "," << pendingJobCount_ << "," << sendMessageSize_;
	oss << "]";
}

void DatabaseManager::print(util::NormalOStringStream& oss) {
	print(oss.str().c_str());
}

void DatabaseManager::print(const char* str) {
	util::LockGuard<util::Mutex> guard(dumpLock_);
	std::cout << str << std::endl;
}

void Database::DabaseEntry::dump(util::NormalOStringStream &oss, bool prev) {
	oss << "[DatabaseEntry]" << std::endl;
	oss << "[GroupIdList] : ";
	CommonUtility::dumpValueSet(oss, groupIdList_);
	oss << std::endl;
	oss << (prev ? "[PREV]" : "[CURRENT]");
	dump(oss, prev ? prevStats_ : stats_);
}

void Database::DabaseEntry::dump(util::NormalOStringStream& oss, ChunkGroupStats& stats) {
	oss << "[ChunkStat] : ";
	oss
		<< stats.table_(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT).get() << ","
		<< stats.table_(ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT).get() << ","
		<< stats.table_(ChunkGroupStats::GROUP_STAT_SWAP_READ_COUNT).get() << ","
		<< stats.table_(ChunkGroupStats::GROUP_STAT_SWAP_WRITE_COUNT).get() << std::endl;
}
