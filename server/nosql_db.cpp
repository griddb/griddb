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
#include "nosql_db.h"
#include "sql_execution.h"
#include "sql_service.h"
#include "nosql_utils.h"
#include "nosql_common.h"

NoSQLDB::NoSQLDB(
		const ResourceSet *resourceSet,
		ConfigTable &config,
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		int32_t cacheSize) :
				resourceSet_(resourceSet),
				globalVarAlloc_(globalVarAlloc),
				cacheSize_(cacheSize), 
				basicStoreList_(globalVarAlloc),
				storeMap_(globalVarAlloc) {

	UNUSED_VARIABLE(config);

	NoSQLStore *store = NULL;

	try {
		DatabaseId dbId = GS_PUBLIC_DB_ID;
		const char *dbName = GS_PUBLIC;
		store = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				NoSQLStore(resourceSet_, globalVarAlloc,
						cacheSize, dbId, dbName, this);
		basicStoreList_.push_back(store);
		
		dbId = GS_SYSTEM_DB_ID;
		dbName = GS_SYSTEM;
		store = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				NoSQLStore(resourceSet, globalVarAlloc,
						cacheSize, dbId, dbName, this);
		basicStoreList_.push_back(store);

	}
	catch (std::exception &e) {
		if (store) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, store);
		}
		GS_RETHROW_USER_OR_SYSTEM(
				e, "NoSQLDB initialize failed");
	}
}

NoSQLDB::~NoSQLDB() {

	for (size_t pos = 0; pos < basicStoreList_.size(); pos++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, basicStoreList_[pos]);
	}

	for (NoSQLStoreMapItr it = storeMap_.begin();
			it != storeMap_.end(); it++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, (*it).second);
	}
}

void NoSQLDB::dump(util::NormalOStringStream &oss,
		const char *dbName) {

	NoSQLStore *store = getNoSQLStore(dbName);
	if (store) {
		store->dump(oss);
	}
}

void NoSQLDB::resizeTableCache(
		const char *dbName, int32_t cacheSize) {

	NoSQLStore *store = getNoSQLStore(dbName);
	if (store) {
		store->resizeTableCache(cacheSize);
	}
}

NoSQLStore *NoSQLDB::getNoSQLStore(
		DatabaseId dbId, const char *dbName) {

	if (dbId <= GS_SYSTEM_DB_ID) {
		return basicStoreList_[static_cast<size_t>(dbId)];
	}

	util::LockGuard<util::Mutex> guard(lock_);
	NoSQLStoreMapItr it = storeMap_.find(dbId);
	if (it != storeMap_.end()) {
		return (*it).second;
	}
	else {
		NoSQLStore *store = NULL;

		try {
			NoSQLStore *store = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
					NoSQLStore(resourceSet_, globalVarAlloc_, cacheSize_,
							dbId, dbName, this);
			storeMap_.insert(std::make_pair(dbId, store));
			return store;
		}
		catch (std::exception &e) {
			if (store) {
				ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, store);
			}
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
}

NoSQLStore *NoSQLDB::getNoSQLStore(
		const char *dbName) {

	NoSQLStore *targetStore = NULL;
	if (!strcmp(dbName, GS_PUBLIC)) {
		targetStore = basicStoreList_[GS_PUBLIC_DB_ID];
	}
	else {
		util::LockGuard<util::Mutex> guard(lock_);
		for (NoSQLStoreMapItr it = storeMap_.begin();
				it != storeMap_.end(); it++) {
			
			NoSQLStore *currentStore = (*it).second;
			const char *currentDbName = currentStore->getDbName();
			if (!strcmp(dbName, currentDbName)) {
				targetStore = currentStore;
				break;
			}
		}
	}
	return targetStore;
}
void NoSQLDB::createUser(
	EventContext &ec, SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &password) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->createUser(ec, execution, userName, password);
}

void NoSQLDB::dropUser(
	EventContext &ec,
	SQLExecution *execution,
	const NameWithCaseSensitivity &userName) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropUser(ec, execution, userName);
}

void NoSQLDB::setPassword(
	EventContext &ec,
	SQLExecution *execution,
	const NameWithCaseSensitivity &userName,
	const NameWithCaseSensitivity &password) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->setPassword(ec, execution, userName, password);
}

void NoSQLDB::grant(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &dbName,
		AccessControlType type) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->grant(ec, execution, userName, dbName, type);
}

void NoSQLDB::revoke(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &dbName,
		AccessControlType type) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->revoke(ec, execution, userName, dbName, type);
}

void NoSQLDB::createDatabase(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->createDatabase(ec, execution, dbName);
}

void NoSQLDB::dropDatabase(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropDatabase(ec, execution, dbName);
}
void NoSQLDB::createTable(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->createTable(ec, execution, dbName,
			tableName, createTableOption, baseInfo);
}

void NoSQLDB::createView(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->createView(ec, execution, dbName,
			tableName, createTableOption, baseInfo);
}

void NoSQLDB::dropTable(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		bool ifExists, DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropTable(ec, execution, dbName,
			tableName, ifExists, baseInfo);
}

void NoSQLDB::dropView(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		bool ifExists) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropView(ec, execution, dbName,
			tableName, ifExists);
}

void NoSQLDB::getTable(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		TableSchemaInfo *&schemaInfo,
		bool withCache) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());

	SQLTableCacheEntry *entry = NULL;
	SQLService *sqlService
			= execution->getResourceSet()->getSQLService();
	int64_t emNow = sqlService->getEE()->getMonotonicTime();
	store->getTable(ec, execution, dbName,
			tableName, schemaInfo, withCache, entry, emNow);
}

void NoSQLDB::getContainer(
		SQLExecution *execution,
		NoSQLContainer &container) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	NoSQLStoreOption option(execution);
	store->getContainer(container, false, option);
}

void NoSQLDB::createIndex(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		CreateIndexOption &option,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->createIndex(ec, execution, dbName,
			indexName, tableName, option, baseInfo);
}

void NoSQLDB::createIndexPost(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->createIndexPost(ec, execution, dbName,
			indexName, tableName, baseInfo);
}

void NoSQLDB::dropIndex(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		bool ifExists,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropIndex(ec, execution, dbName,
			indexName, tableName, ifExists, baseInfo);
}

void NoSQLDB::dropIndexPost(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropIndexPost(ec, execution, dbName,
			indexName, tableName, baseInfo);
}

void NoSQLDB::begin(SQLExecution *execution) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->begin();
}

void NoSQLDB::commit(SQLExecution *execution) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->commit();
}

void NoSQLDB::rollback(SQLExecution *execution) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->rollback();
}

void NoSQLStore::removeCache(const char *tableName) {

	try {
		SQLString key(globalVarAlloc_);
		NoSQLUtils::normalizeString(key, tableName);
		removeCacheEntry<SQLString>(key);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

NoSQLDB::TableLatch::TableLatch(
		EventContext &ec,
		NoSQLDB *db,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		bool withCache) :
				cache_(NULL),
				entry_(NULL),
				schemaInfo_(NULL),
				useCache_(false) {

	NoSQLStore *store = db->getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	SQLService *sqlService
			= execution->getResourceSet()->getSQLService();
	int64_t emNow = sqlService->getEE()->getMonotonicTime();

	cache_ = &(store->getCache());
	useCache_ = store->getTable(ec, execution, dbName,
			tableName, schemaInfo_, 
			withCache, entry_, emNow);

	if (schemaInfo_ == NULL || entry_ == NULL) {
		GS_THROW_USER_ERROR(
				GS_ERROR_NOSQL_INTERNAL, "");
	}
}

NoSQLDB::TableLatch::~TableLatch() {
	cache_->release(entry_);
}

void NoSQLDB::dropTablePartition(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropTablePartition(ec, execution, dbName,
			tableName, baseInfo);
}

void NoSQLDB::dropTablePartitionPost(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->dropTablePartitionPost(ec, execution, dbName,
			tableName);
}
void NoSQLDB::addColumn(
		EventContext &ec, SQLExecution *execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) {

	NoSQLStore *store = getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());
	store->addColumn(ec, execution, dbName,
			tableName, createTableOption, baseInfo);
}

int64_t NoSQLDB::getMonotonicTime() {

	return resourceSet_->getSQLService()
			->getEE()->getMonotonicTime();
}