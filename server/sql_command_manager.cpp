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
#include "sql_command_manager.h"
#include "sql_execution.h"
#include "sql_job_manager.h"
#include "database_manager.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(SQL_DETAIL);

#include "sql_processor_ddl.h"
#include "sql_utils.h"
#include "sql_service.h"
#include "qp_def.h"

typedef TableLatch TableLatch;

const char8_t* getCommandString(DDLCommandType type) {
	switch (type) {
	case DDL_CREATE_DATABASE: return "CREATE DATABASE";
	case DDL_DROP_DATABASE: return "DROP DATABASE";
	case DDL_CREATE_TABLE: return "CREATE TABLE";
	case DDL_DROP_TABLE: return "DROP TABLE";
	case DDL_CREATE_INDEX: return "CREATE INDEX";
	case DDL_DROP_INDEX: return "DROP INDEX";
	case DDL_CREATE_USER: return "CREATE USER";
	case DDL_DROP_USER: return "DROP USER";
	case DDL_SET_PASSWORD: return "SET PASSWORD";
	case DDL_GRANT: return "GRANT";
	case DDL_REVOKE: return "REVOKE";
	case DDL_BEGIN: return "BEGIN";
	case DDL_COMMIT: return "COMMIT";
	case DDL_ROLLBACK: return "ROLLBACK";
	case CMD_GET_TABLE: return "GET TABLE";
	case DDL_DROP_PARTITION: return "DROP PARTITION";
	case DDL_ADD_COLUMN: return "ADD COLUMN";
	case DDL_RENAME_COLUMN: return "RENAME COLUMN";
	case DDL_CREATE_VIEW: return "CREATE VIEW";
	case DDL_DROP_VIEW: return "DROP VIEW";
	default: return "UNSUPPORTED";
	}
}

DBConnection::DBConnection(
		ConfigTable& config,
		SQLVariableSizeGlobalAllocator& globalVarAlloc,
		int32_t cacheSize,
		PartitionTable* pt,
		PartitionList* partitionList,
		SQLService* sqlSvc) :
		globalVarAlloc_(globalVarAlloc),
		cacheSize_(cacheSize),
		basicStoreList_(globalVarAlloc),
		storeMap_(
			NoSQLStoreMap::key_compare(), globalVarAlloc),
		pt_(pt),
		partitionList_(partitionList),
		dsConfig_(NULL),
		sqlSvc_(sqlSvc) {
	UNUSED_VARIABLE(config);

	NoSQLStore* store = NULL;

	try {
		DatabaseId dbId = GS_PUBLIC_DB_ID;
		const char* dbName = GS_PUBLIC;
		JobManager* jobManager
			= sqlSvc->getExecutionManager()->getJobManager();
		dsConfig_ = sqlSvc->getExecutionManager()->getManagerSet()->dsConfig_;
		store = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			NoSQLStore(globalVarAlloc, cacheSize, dbId, dbName,
				pt_, partitionList_, dsConfig_, this, jobManager);
		basicStoreList_.push_back(store);

		dbId = GS_SYSTEM_DB_ID;
		dbName = GS_SYSTEM;
		store = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			NoSQLStore(globalVarAlloc, cacheSize, dbId, dbName,
				pt_, partitionList_, dsConfig_, this, jobManager);
		basicStoreList_.push_back(store);

	}
	catch (std::exception& e) {
		if (store) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, store);
		}
		GS_RETHROW_USER_OR_SYSTEM(
			e, "DBConnection initialize failed");
	}
}

DBConnection::~DBConnection() {

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

void DBConnection::refreshSchemaCache() {
	util::LockGuard<util::Mutex> guard(lock_);

	try {
		if (basicStoreList_.size() > 0) {
			basicStoreList_[0]->refresh();
		}
		for (NoSQLStoreMapItr it = storeMap_.begin();
			it != storeMap_.end(); it++) {
			(*it).second->refresh();
		}
	}
	catch (std::exception& e) {
	}
}

void DBConnection::dump(util::NormalOStringStream& oss,
	const char* dbName) {

	NoSQLStore* store = getNoSQLStore(dbName);
	if (store) {
		store->dump(oss);
	}
};

void DBConnection::resizeTableCache(
	const char* dbName, int32_t cacheSize) {

	NoSQLStore* store = getNoSQLStore(dbName);
	if (store) {
		store->resizeTableCache(cacheSize);
	}
};

NoSQLStore::NoSQLStore(
	SQLVariableSizeGlobalAllocator& globalVarAlloc,
	size_t cacheSize,
	DatabaseId dbId,
	const char* dbname,
	PartitionTable* pt,
	PartitionList* partitionList,
	DataStoreConfig* dsConfig,
	DBConnection* dbConnection,
	JobManager* jobManager) :
	globalVarAlloc_(globalVarAlloc),
	dbId_(dbId),
	dbName_(dbname, globalVarAlloc),
	normalizedDbName_(globalVarAlloc),
	cacheSize_(cacheSize),
	cache_(NULL),
	pt_(pt),
	partitionList_(partitionList),
	dsConfig_(dsConfig),
	dbConnection_(dbConnection),
	jobManager_(jobManager),
	storeAccessCount_(0),
	storeSkipCount_(0),
	storeRecoveryCount_(0) {

	NoSQLUtils::normalizeString(
		normalizedDbName_, dbName_.c_str());
	createCache();
}

NoSQLStore::~NoSQLStore() {
	removeCache();
}

DBConnection* NoSQLStore::getDbConnection() {
	return dbConnection_;
}

DataStoreV4* NoSQLStore::getDataStore(PartitionId pId) {
	const ManagerSet* mgrSet = this->getDbConnection()->getExecutionManager()->getManagerSet();
	DataStoreBase& dsBase =
		mgrSet->partitionList_->partition(pId).dataStore();
	return static_cast<DataStoreV4*>(&dsBase);
}

NoSQLStore* DBConnection::getNoSQLStore(
	DatabaseId dbId, const char* dbName) {

	if (dbId <= GS_SYSTEM_DB_ID) {
		return basicStoreList_[dbId];
	}

	util::LockGuard<util::Mutex> guard(lock_);
	NoSQLStoreMapItr it = storeMap_.find(dbId);
	if (it != storeMap_.end()) {
		return (*it).second;
	}
	else {
		NoSQLStore* store = NULL;
		try {
			NoSQLStore* store = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				NoSQLStore(globalVarAlloc_, cacheSize_,
					dbId, dbName, pt_, partitionList_, dsConfig_, this,
					sqlSvc_->getExecutionManager()->getJobManager());
			storeMap_.insert(std::make_pair(dbId, store));
			return store;
		}
		catch (std::exception& e) {
			if (store) {
				ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, store);
			}
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
}

NoSQLStore* DBConnection::getNoSQLStore(
	const char* dbName) {
	NoSQLStore* targetStore = NULL;
	if (!strcmp(dbName, GS_PUBLIC)) {
		targetStore = basicStoreList_[GS_PUBLIC_DB_ID];
	}
	else {
		util::LockGuard<util::Mutex> guard(lock_);
		for (NoSQLStoreMapItr it = storeMap_.begin();
			it != storeMap_.end(); it++) {
			NoSQLStore* currentStore = (*it).second;
			const char* currentDbName = currentStore->getDbName();
			if (!strcmp(dbName, currentDbName)) {
				targetStore = currentStore;
				break;
			}
		}
	}
	return targetStore;
}

void NoSQLStore::createCache() {
	if (cache_ == NULL) {
		cache_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) SQLTableCache(
				static_cast<int32_t>(cacheSize_), globalVarAlloc_);
	}
}

NoSQLStore::SQLTableCache& NoSQLStore::getCache() {
	return *reinterpret_cast<SQLTableCache*>(cache_);
}

void NoSQLStore::removeCache() {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	if (cache) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, cache);
	}
}

template <class Node, class Key, class Value>
Node* NoSQLStore::putCacheEntry(
	Key& key, Value value, bool withGet) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	return cache->put(key, value, withGet);
}

template <class Node, class Key>
Node* NoSQLStore::getCacheEntry(Key& key) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	return cache->get(key);
}

template <class Key>
void NoSQLStore::removeCacheEntry(Key& key) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	cache->remove(key);
}

template <class Key>
bool NoSQLStore::setDiffLoad(Key& key) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	return cache->setDiffLoad(key);
}

template bool NoSQLStore::setDiffLoad(SQLString& key);

template <class Node>
void NoSQLStore::releaseCacheEntry(Node* node) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	cache->release(node);
}

void NoSQLStore::dump(util::NormalOStringStream& oss) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	if (cache) {
		cache->dump(oss, true);
	}
}

void NoSQLStore::resizeTableCache(int32_t resizeCacheSize) {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	if (cache) {
		cache->resize(resizeCacheSize);
	}
}

void NoSQLStore::refresh() {
	SQLTableCache* cache
		= reinterpret_cast<SQLTableCache*>(cache_);
	if (cache) {
		cache->refresh();
	}
}

/*!
	@brief 実行可否判定
	@note コマンド引数と、実行状態から判定する
*/
void NoSQLStore::checkExecutable(util::StackAllocator& alloc,
	DDLCommandType commandType, SQLExecution* execution,
	bool isAdjustDb, ExecuteCondition& condition) {

	const NameWithCaseSensitivity* userName = condition.userName_;
	const NameWithCaseSensitivity* tableName = condition.tableName_;
	const NameWithCaseSensitivity* dbName = condition.dbName_;
	const NameWithCaseSensitivity* indexName = condition.indexName_;

	SQLExecution::SQLExecutionContext& sqlContext = execution->getContext();
	switch (commandType) {
	case DDL_CREATE_USER:
	case DDL_DROP_USER:
	case DDL_GRANT:
	case DDL_REVOKE:
		checkAdministrator(execution);
		break;
	case DDL_SET_PASSWORD: {
		if (!sqlContext.isAdministrator()) {
			if (userName) {
				if (userName->isCaseSensitive_) {
					if (sqlContext.getUserName().compare(userName->name_) != 0) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_USER,
							"Normal user may only change one's own password");
					}
				}
				else {
					const util::String& normalizedauthUserName =
						normalizeName(alloc, sqlContext.getUserName().c_str());
					const util::String& normalizedUserName =
						normalizeName(alloc, userName->name_);
					if (normalizedUserName.compare(normalizedauthUserName) != 0) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_USER,
							"Normal user may only change one's own password");
					}
				}
			}
		}
	}
						 break;
	case DDL_CREATE_DATABASE: {
		if (dbName) {
			if (dbName->isCaseSensitive_) {
				checkValidName(commandType, dbName->name_);
			}
			else {
				const util::String& normalizeDbName
					= normalizeName(alloc, dbName->name_);
				checkValidName(commandType, normalizeDbName.c_str());
			}
		}
		checkAdministrator(execution);
	}
							break;
	case DDL_DROP_DATABASE: {
		if (dbName) {
			if (!isAdjustDb) {
				if (dbName->isCaseSensitive_) {
					checkConnectedDbName(
						alloc, dbName_.c_str(), dbName->name_, dbName->isCaseSensitive_);
				}
				else {
					checkConnectedDbName(
						alloc, normalizedDbName_.c_str(),
						dbName->name_, dbName->isCaseSensitive_);
				}
			}
		}
		checkAdministrator(execution);
	}
						  break;
	case CMD_GET_TABLE:
	case DDL_CREATE_TABLE:
	case DDL_DROP_TABLE:
	case DDL_CREATE_VIEW:
	case DDL_DROP_VIEW:
	case DDL_DROP_PARTITION:
	case DDL_ADD_COLUMN:
	case DDL_RENAME_COLUMN://
	case DDL_CREATE_INDEX:
	case DDL_DROP_INDEX: {
		if (dbName) {
			if (!isAdjustDb) {
				if (dbName->isCaseSensitive_) {
					checkConnectedDbName(alloc, dbName_.c_str(), dbName->name_, dbName->isCaseSensitive_);
				}
				else {
					checkConnectedDbName(alloc,
						normalizedDbName_.c_str(), dbName->name_, dbName->isCaseSensitive_);
				}
			}
		}
		if (tableName) {
			if (tableName->isCaseSensitive_) {
				checkValidName(commandType, tableName->name_);
			}
			else {
				const util::String& normalizeTableName
					= normalizeName(alloc, tableName->name_);
				checkValidName(commandType, normalizeTableName.c_str());
			}
		}
		if (indexName) {
			if (indexName->isCaseSensitive_) {
				checkValidName(commandType, indexName->name_);
			}
			else {
				const util::String& normalizeIndexName
					= normalizeName(alloc, indexName->name_);
				checkValidName(commandType, normalizeIndexName.c_str());
			}
		}
	}
					   break;
	case DDL_BEGIN:
	case DDL_COMMIT:
	case DDL_ROLLBACK:
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_UNSUPPORTED_COMMAND_TYPE,
			"Unsupported DDL command=" << static_cast<int32_t>(commandType));
		break;
	}
}

/*!
	@brief 管理者権限を持つか
*/
void NoSQLStore::checkAdministrator(SQLExecution* execution) {
	if (!execution->getContext().isAdministrator()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_USER,
			"Insufficient privilege, only admin");
	}
}

/*!
	@brief 指定名称が妥当(NULL或いは空でない)か
*/
void NoSQLStore::checkValidName(
		DDLCommandType commandType, const char* name) {
	UNUSED_VARIABLE(commandType);

	if (name == NULL || (name && strlen(name) == 0)) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_INVALID_NAME, "Invalid symbol name=" << name);
	}
}

/*!
	@brief ユーザ作成
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] userName ユーザ名
	@param [in] password パスワード文字列
	@note CREATE USER userName [IDENTIFIED BY 'password']
	@note 管理者しか実行できない
*/
void NoSQLStore::createUser(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& password,
	bool isRole) {
	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_CREATE_USER;
	try {
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		ExecuteCondition condition(&gsUsers, NULL, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		NoSQLContainer container(ec, gsUsers,
			execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);

		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		container.putUser(userName.name_, password.name_, false, option, isRole);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief ユーザ削除
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] userName ユーザ名
	@note DROP USER userName
	@note 管理者しか実行できない
*/
void NoSQLStore::dropUser(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName) {
	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_USER;
	try {
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		ExecuteCondition condition(&gsUsers, NULL, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		NoSQLContainer container(ec, gsUsers,
			execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		container.dropUser(userName.name_, option);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief パスワード変更
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] userName ユーザ名
	@param [in] password パスワード文字列
	@note SET PASSWORD [FOR username] PASSWORD('password')
	@note 通常ユーザは自分のみ、管理者は任意のユーザの設定ができる
*/
void NoSQLStore::setPassword(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& password) {
	DDLCommandType commandType = DDL_SET_PASSWORD;
	util::StackAllocator& alloc = ec.getAllocator();
	try {
		const char8_t* targetUserName = userName.name_;
		if (userName.name_ == NULL
			|| (userName.name_ != NULL && strlen(userName.name_) == 0)) {
			targetUserName = execution->getContext().getUserName().c_str();
		}
		NameWithCaseSensitivity actualUserName(targetUserName, userName.isCaseSensitive_);
		ExecuteCondition condition(&actualUserName, NULL, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(ec, gsUsers, execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		container.putUser(actualUserName.name_, password.name_, true, option, false);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief ユーザ権限付与
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] userName ユーザ名
	@param [in] dbName DB名
	@param [in] accessControlType 権限種別
	@note GRANT ALL ON dbName TO userName
	@note 通常ユーザは自分のみ、管理者は任意のユーザの設定ができる
	@note 権限はV1.5では"ALL"のみ
*/
void NoSQLStore::grant(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& dbName,
	AccessControlType type) {
	DDLCommandType commandType = DDL_GRANT;
	util::StackAllocator& alloc = ec.getAllocator();
	try {
		const char* connectedDbName = dbName.name_;
		adjustConnectedDb(connectedDbName);
		ExecuteCondition condition(&userName, &dbName, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(ec, gsUsers, execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}
		container.grant(userName.name_, connectedDbName, type, option);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief ユーザ権限剥奪
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] userName ユーザ名
	@param [in] dbName DB名
	@param [in] accessControlType 権限種別
	@note REVOKE ALL ON dbName FROM userName
	@note 通常ユーザは自分のみ、管理者は任意のユーザの設定ができる
	@note 権限はV1.5では"ALL"のみ
*/
void NoSQLStore::revoke(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& dbName,
	AccessControlType type) {
	DDLCommandType commandType = DDL_REVOKE;
	util::StackAllocator& alloc = ec.getAllocator();
	try {
		const char* connectedDbName = dbName.name_;
		adjustConnectedDb(connectedDbName);
		ExecuteCondition condition(&userName, &dbName, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(ec, gsUsers, execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}
		container.revoke(userName.name_, connectedDbName, type, option);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief データベース作成
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] dbName DB名
	@note CREATE DATABASE dbName
	@note 管理者しか実行できない
*/
void NoSQLStore::createDatabase(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName) {
	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_CREATE_DATABASE;
	try {
		ExecuteCondition condition(NULL, &dbName, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(ec, gsUsers, execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}
		container.createDatabase(dbName.name_, option);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief データベース削除
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] dbName DB名
	@note DROP DATABASE dbName
	@note 管理者しか実行できない
*/
void NoSQLStore::dropDatabase(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName) {
	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_DATABASE;
	try {
		ExecuteCondition condition(NULL, &dbName, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		uint64_t containerCount = 0;
		NoSQLStoreOption option(execution);
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			NoSQLContainer container(ec, pId, execution->getContext().getSyncContext(), execution);
			container.getContainerCount(containerCount, option);
			if (containerCount > 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_DATABASE_NOT_EMPTY,
					"Not empty database, db=" << dbName.name_ << ", pId=" << pId);
			}
		}
		const NameWithCaseSensitivity gsDatabases(GS_DATABASES);
		NoSQLContainer dropContainer(ec, gsDatabases, execution->getContext().getSyncContext(), execution);
		dropContainer.dropDatabase(dbName.name_, option);
		jobManager_->getTransactionService()->getManager()->getDatabaseManager().drop(execution->getContext().getDBId());
		SQLExecution::updateRemoteCache(ec, jobManager_->getEE(SQL_SERVICE), pt_, execution->getContext().getDBId(), NULL, NULL, 0);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief テーブル作成
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] dbName DB名
	@param [in] tableName テーブル名
	@param [in] createTableOption オプション
	@note CREATE TABLE [IF NOT EXISTS] [データベース名.]テーブル名 (列定義※[,列定義,…])
[PARTITION BY HASH (列名) PARTITIONS 分割数];
	@note ※列定義: 列名 型名 [列制約※[, 列制約,…]
	@note ※列制約: [PRIMARY KEY]
*/
class ClusterStatistics {

	static const int32_t DEFAULT_OWNER_NOT_BALANCE_GAP = 5;
	static const int32_t INTERVAL_ASSIGN_MODE_NORMAL = 0;
	static const int32_t INTERVAL_ASSIGN_MODE_WORKER = 1;
	static const int32_t INTERVAL_ASSIGN_MODE_OPTIMIZE = 2;
	static const int32_t UNDEF_WORKER_GROUP_NO = -1;
	static const int32_t UNDEF_WORKER_GROUP_POS = -1;

public:

	ClusterStatistics(util::StackAllocator& alloc,
		PartitionTable* pt, int32_t tablePartitioningNum, int64_t seed) :
		eventStackAlloc_(alloc), pt_(pt), tablePartitioningNum_(tablePartitioningNum),
		partitionNum_(pt->getPartitionNum()), assignCountList_(alloc),
		ownerNodeIdList_(alloc), divideList_(alloc),
		activeNodeList_(alloc), activeNodeMap_(alloc),
		random_(seed),
		useRandom_(false), 
		ownerLoss_(false),
		ownerNotBalanceGap_(DEFAULT_OWNER_NOT_BALANCE_GAP),
		largeContainerPId_(UNDEF_CONTAINERID),
		partitionDevideList_(alloc),
		workerNum_(0), 
		workerGroupNo_(UNDEF_WORKER_GROUP_NO),
		workerGroupPos_(UNDEF_WORKER_GROUP_POS),
		intervalAssignMode_(INTERVAL_ASSIGN_MODE_NORMAL) {

		if (pt == NULL || partitionNum_ == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INTERNAL,
				"Invalid cluster statistics parameter");
		}
		ownerNodeIdList_.assign(pt_->getPartitionNum(), UNDEF_NODEID);
		nodeNum_ = pt_->getNodeNum();
		pt_->getLiveNodeIdList(activeNodeList_);
		activeNodeNum_ = static_cast<int32_t>(activeNodeList_.size());
		if (nodeNum_ == 0 || activeNodeNum_ == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN, "Active node is not found");
		}
		assignCountList_.assign(activeNodeNum_, 0);
		NodeId ownerNodeId;
		int32_t nodePos;
		activeNodeMap_.assign(nodeNum_, -1);
		assert(activeNodeNum_ != 0);
		for (size_t i = 0;i < activeNodeList_.size(); i++) {
			NodeId nodeId = activeNodeList_[i];
			if (nodeId >= nodeNum_) {
				continue;
			}
			activeNodeMap_[nodeId] = static_cast<NodeId>(i);
		}
		for (nodePos = 0;nodePos < activeNodeNum_;nodePos++) {
			util::Vector<PartitionId> tmpIdList(alloc);
			divideList_.push_back(tmpIdList);
		}
		for (uint32_t pId = 0;
				pId < static_cast<PartitionId>(partitionNum_); pId++) {
			ownerNodeId = pt_->getNewSQLOwner(pId);
			ownerNodeIdList_[pId] = ownerNodeId;
			if (ownerNodeId == UNDEF_NODEID || ownerNodeId >= nodeNum_) {
				useRandom_ = true;
				ownerLoss_ = true;
				continue;
			}
			nodePos = activeNodeMap_[ownerNodeId];
			if (nodePos == -1) {
				useRandom_ = true;
				ownerLoss_ = true;
				continue;
			}
			assert(nodePos != -1);
			divideList_[nodePos].push_back(pId);
		}

		int32_t max = INT32_MIN;
		int32_t min = INT32_MAX;
		int32_t count = 0;
		for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
			count = static_cast<int32_t>(divideList_.size());
			if (count > max) {
				max = count;
			}
			if (count < min) {
				min = count;
			}
		}
		if ((max - min) > ownerNotBalanceGap_) {
			useRandom_ = true;
		}
	}

	void setOptionalIntervalAssignment(
			const PartitionGroupConfig& config, int32_t workerGroupNo,
			int32_t workerGroupPos, bool setGroupPositionOption) {
		UNUSED_VARIABLE(setGroupPositionOption);

		assert(workerNum_ >= 0);
		if (ownerLoss_) {
			return;
		}
		workerNum_ = config.getPartitionGroupCount();
		workerGroupNo_ = workerGroupNo % workerNum_;
		workerGroupPos_ = workerGroupPos;

		if (partitionNum_ < workerNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
				"Worker number(" << workerNum_ << ") must be less than cluster partition number(" << partitionNum_ << ") in case of using interval_worker_group");
		}
		intervalAssignMode_ = INTERVAL_ASSIGN_MODE_WORKER;

		for (int32_t i = 0; i < workerNum_; i++) {
			util::Vector<PartitionId> tmpList(eventStackAlloc_);
			util::Vector<PartitionId> ownerCountList(nodeNum_, eventStackAlloc_);
			partitionDevideList_.push_back(tmpList);

			PartitionId startPId = config.getGroupBeginPartitionId(i);
			PartitionId endPId = config.getGroupEndPartitionId(i);
			int32_t groupSize = endPId - startPId;
			int32_t groupBasePId = (workerGroupPos != UNDEF_WORKER_GROUP_POS) ? startPId + workerGroupPos % groupSize : startPId;

			for (PartitionId pId = groupBasePId; pId < endPId; pId++) {
				partitionDevideList_[i].push_back(pId);
				ownerCountList[ownerNodeIdList_[pId]]++;
			}
			for (PartitionId pId = startPId;
					pId < static_cast<PartitionId>(groupBasePId); pId++) {
				partitionDevideList_[i].push_back(pId);
				ownerCountList[ownerNodeIdList_[pId]]++;
			}

			if (workerGroupPos == UNDEF_WORKER_GROUP_POS) {
				randomShuffle(
						partitionDevideList_[i],
						static_cast<int32_t>(partitionDevideList_[i].size()),
						random_);
			}
		}
	}

	void generateStatistics(SyntaxTree::TablePartitionType type,
		PartitionId largeContainerPId,
		util::Vector<PartitionId>& condenseIdList,
		util::Vector<NodeAffinityNumber>& assignIdList) {
		largeContainerPId_ = largeContainerPId;
		switch (type) {
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
			generateHashAssignment(condenseIdList, assignIdList);
			break;
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
			generateIntervalAssignment(condenseIdList, assignIdList);
			break;
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
			generateIntervalHashAssignment(condenseIdList, assignIdList);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_UNSUPPORTED_PARTITIONING_INFO,
				"Unsupported table partitioning type");
			break;
		}
	}

private:

	void generateHashAssignment(util::Vector<PartitionId>& condenseIdList,
		util::Vector<NodeAffinityNumber>& assignIdList) {
		int32_t currentTablePartitioningNum = tablePartitioningNum_;
		bool isOverPartitionNum = false;
		assignIdList.assign(tablePartitioningNum_ + 1, 0);
		if (tablePartitioningNum_ >= partitionNum_) {
			currentTablePartitioningNum = partitionNum_;
			isOverPartitionNum = true;
			condenseIdList.assign(partitionNum_, 0);
		}
		else {
			condenseIdList.assign(tablePartitioningNum_, 0);
		}
		PartitionRangeList ranges(eventStackAlloc_,
			partitionNum_, currentTablePartitioningNum);
		PartitionId startPId, endPId;
		util::Vector<int32_t> rangeNoList(eventStackAlloc_);
		rangeNoList.assign(currentTablePartitioningNum, 0);
		int32_t rangePos;
		for (rangePos = 0; rangePos < currentTablePartitioningNum; rangePos++) {
			rangeNoList[rangePos] = rangePos;
		}
		randomShuffle(rangeNoList, currentTablePartitioningNum, random_);
		PartitionId targetPId;
		int32_t nodePos = 0;
		for (rangePos = 0; rangePos < currentTablePartitioningNum; rangePos++) {
			ranges.get(rangeNoList[rangePos], startPId, endPId);
			uint32_t minVal = UINT32_MAX;
			PartitionId target;
			bool assigned = false;
			util::Vector<int32_t> candList(eventStackAlloc_);
			if (!useRandom_) {
				for (target = startPId; target <= endPId; target++) {
					nodePos = activeNodeMap_[ownerNodeIdList_[target]];
					if (nodePos == -1) continue;
					if (minVal > assignCountList_[nodePos]) {
						minVal = assignCountList_[nodePos];
					}
				}
				if (minVal != UINT32_MAX) {
					for (target = startPId; target <= endPId; target++) {
						nodePos = activeNodeMap_[ownerNodeIdList_[target]];
						if (nodePos == -1) continue;
						if (minVal == assignCountList_[nodePos]) {
							candList.push_back(target);
							assigned = true;
						}
					}
				}
			}
			if (!assigned) {
				int32_t rangeSize = endPId - startPId;
				int32_t randomPos = random_.nextInt32(rangeSize);
				targetPId = startPId + randomPos;
			}
			else {
				int32_t randomPos = random_.nextInt32(static_cast<int32_t>(candList.size()));
				targetPId = candList[randomPos];
			}
			assert(targetPId != static_cast<PartitionId>(-1));
			if (ownerNodeIdList_[targetPId] != UNDEF_NODEID) {
				nodePos = activeNodeMap_[ownerNodeIdList_[targetPId]];
				if (nodePos != -1) {
					assignCountList_[nodePos]++;
				}
			}
			condenseIdList[rangePos] = targetPId;
		}
		int64_t assignPos = 0;
		assignIdList[0] = largeContainerPId_;
		assignPos++;
		if (!isOverPartitionNum) {
			for (size_t pos = 0; pos < condenseIdList.size(); pos++) {
				assignIdList[assignPos++] = (condenseIdList[pos] + partitionNum_);
			}
		}
		else {
			for (rangePos = 0; rangePos < tablePartitioningNum_; rangePos++) {
				assignIdList[assignPos++] = ((rangePos / currentTablePartitioningNum) * partitionNum_
					+ condenseIdList[rangePos % currentTablePartitioningNum] + partitionNum_);
			}
		}
	};

	void generateIntervalBaseAssignment(util::Vector<PartitionId>& condenseIdList,
		util::Vector<NodeAffinityNumber>& assignIdList) {

		assignIdList.push_back(largeContainerPId_);
		condenseIdList.assign(partitionNum_, 0);
		int32_t nodePos;
		if (useRandom_) {
			for (PartitionId pId = 0;
					pId < static_cast<PartitionId>(partitionNum_); pId++) {
				condenseIdList[pId] = pId;
			}
			randomShuffle(condenseIdList, partitionNum_, random_);
			return;
		}
		for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
			if (divideList_[nodePos].size() != 0) {
				randomShuffle(
						divideList_[nodePos],
						static_cast<int32_t>(divideList_[nodePos].size()),
						random_);
			}
		}
		uint32_t minVal = UINT32_MAX;
		util::Vector<int32_t> candList(eventStackAlloc_);
		int32_t emptyCount = 0;
		int32_t assignedCount = 0;
		int32_t targetNodePos;
		while (1) {
			minVal = UINT32_MAX;
			emptyCount = 0;
			candList.clear();
			for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
				if (divideList_[nodePos].size() == 0) {
					emptyCount++;
					continue;
				}
				if (minVal > assignCountList_[nodePos]) {
					minVal = assignCountList_[nodePos];
				}
			}
			if (emptyCount == activeNodeNum_) {
				break;
			}
			for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
				if (divideList_[nodePos].size() == 0) continue;
				if (minVal == assignCountList_[nodePos]) {
					candList.push_back(nodePos);
				}
			}
			targetNodePos = candList[random_.nextInt32(static_cast<int32_t>(candList.size()))];
			assert(targetNodePos != -1);
			condenseIdList[assignedCount] = divideList_[targetNodePos].back();
			assignCountList_[targetNodePos]++;
			assignedCount++;
			divideList_[targetNodePos].pop_back();
		}
	};

	void generateIntervalGroupAssignment(util::Vector<PartitionId>& condenseIdList,
		util::Vector<NodeAffinityNumber>& assignIdList) {

		assignIdList.push_back(largeContainerPId_);
		condenseIdList.assign(partitionNum_, 0);

		int32_t groupPos = workerGroupNo_;
		int32_t currentWorkerGroupNo = 0;
		int32_t nodePos = 0;

		uint32_t minVal = UINT32_MAX;
		util::Vector<int32_t> candList(eventStackAlloc_);

		for (int32_t pos = 0; pos < partitionNum_; pos++) {
			bool assigned = false;
			minVal = UINT32_MAX;
			candList.clear();
			if (workerGroupPos_ == UNDEF_WORKER_GROUP_POS) {
				for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
					if (minVal > assignCountList_[nodePos]) {
						minVal = assignCountList_[nodePos];
					}
				}
				for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
					if (minVal == assignCountList_[nodePos]) {
						candList.push_back(nodePos);
					}
				}
				randomShuffle(
						candList, static_cast<int32_t>(candList.size()),
						random_);
			}
			for (int32_t i = 0; i < workerNum_; i++) {
				currentWorkerGroupNo = groupPos++ % workerNum_;
				if (partitionDevideList_[currentWorkerGroupNo].empty()) continue;
				if (workerGroupPos_ != UNDEF_WORKER_GROUP_POS) {
					condenseIdList[pos] = partitionDevideList_[currentWorkerGroupNo].front();
					partitionDevideList_[currentWorkerGroupNo].erase(partitionDevideList_[currentWorkerGroupNo].begin());
				}
				else {
					for (size_t j = 0; j < candList.size(); j++) {
						for (size_t k = 0; k < partitionDevideList_[currentWorkerGroupNo].size(); k++) {
							if (ownerNodeIdList_[partitionDevideList_[currentWorkerGroupNo][k]] == candList[j]) {
								condenseIdList[pos] = partitionDevideList_[currentWorkerGroupNo][k];
								partitionDevideList_[currentWorkerGroupNo].erase((partitionDevideList_[currentWorkerGroupNo].begin() + k));
								assigned = true;
								break;
							}
						}
						if (assigned) break;
					}
					if (!assigned) {
						int32_t targetPos = random_.nextInt32(static_cast<int32_t>(partitionDevideList_[currentWorkerGroupNo].size()));
						condenseIdList[pos] = partitionDevideList_[currentWorkerGroupNo][targetPos];
						partitionDevideList_[currentWorkerGroupNo].erase((partitionDevideList_[currentWorkerGroupNo].begin() + targetPos));
					}
				}
				assigned = true;
				break;
			}
			if (!assigned) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INTERNAL, "");
			}
		}
	};

	void generateIntervalAssignment(util::Vector<PartitionId>& condenseIdList,
		util::Vector<NodeAffinityNumber>& assignIdList) {
		switch (intervalAssignMode_) {
		case INTERVAL_ASSIGN_MODE_NORMAL:
			generateIntervalBaseAssignment(condenseIdList, assignIdList);
			break;
		case INTERVAL_ASSIGN_MODE_WORKER:
			generateIntervalGroupAssignment(condenseIdList, assignIdList);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INTERNAL, "");
			break;
		}
	}

	void generateIntervalHashAssignment(util::Vector<PartitionId>& condenseIdList,
		util::Vector<NodeAffinityNumber>& assignIdList) {
		generateIntervalAssignment(condenseIdList, assignIdList);
		for (PartitionId pId = 0;
				pId < static_cast<PartitionId>(partitionNum_); pId++) {
			condenseIdList[pId] *= tablePartitioningNum_;
		}
	}

	util::StackAllocator& eventStackAlloc_;
	PartitionTable* pt_;
	int32_t tablePartitioningNum_;
	int32_t partitionNum_;
	int32_t nodeNum_;
	int32_t activeNodeNum_;
	util::Vector<uint32_t> assignCountList_;
	util::Vector<NodeId> ownerNodeIdList_;
	util::Vector<util::Vector<PartitionId>> divideList_;
	util::Vector<PartitionId> activeNodeList_;
	util::Vector<PartitionId> activeNodeMap_;
	util::Random random_;
	bool useRandom_;
	bool ownerLoss_;
	int32_t ownerNotBalanceGap_;
	PartitionId largeContainerPId_;
	util::Vector<util::Vector<PartitionId>> partitionDevideList_;
	int32_t workerNum_;
	int32_t workerGroupNo_;
	int32_t workerGroupPos_;
	int32_t intervalAssignMode_;
};

void NoSQLStore::createTable(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	DDLCommandType commandType = DDL_CREATE_TABLE;
	TableSchemaInfo* schemaInfo = NULL;
	NoSQLStoreOption option(execution);
	util::StackAllocator& alloc = ec.getAllocator();
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	bool appendCache = false;
	bool appendSchema = false;
	typedef std::vector<ColumnId, util::StdAllocator<
		ColumnId, SQLVariableSizeGlobalAllocator> > ColumnIdList;
	ColumnIdList* columnIdList = NULL;
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
			tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		ContainerType containerType = (createTableOption.isTimeSeries())
			? TIME_SERIES_CONTAINER : COLLECTION_CONTAINER;
		bool isPartitioning = createTableOption.isPartitioning();
		SyntaxTree::TablePartitionType partitioningType = createTableOption.partitionType_;
		uint32_t partitionNum = dbConnection_->getPartitionTable()->getPartitionNum();

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		TablePartitioningInfo<util::StackAllocator>* currentPartitioningInfo = &partitioningInfo;
		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);

		TableProperty tableProperty(alloc);
		const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;

		util::XArray<uint8_t> containerSchema(alloc);
		util::XArray<uint8_t> optionList(alloc);
		util::XArray<uint8_t> fixedPart(alloc);
		util::XArray<uint8_t> varPart(alloc);
		util::XArray<NoSQLContainer*> containerList(alloc);
		NoSQLContainer* currentContainer = NULL;
		NoSQLUtils::makeNormalContainerSchema(alloc, tableName.name_,
			createTableOption, containerSchema, optionList, partitioningInfo);
		util::XArray<uint8_t> metaContainerSchema(alloc);
		util::String affinityStr(alloc);
		NoSQLUtils::getAffinityValue(alloc, createTableOption, affinityStr);
		NoSQLUtils::makeLargeContainerSchema(alloc, metaContainerSchema,
			false, affinityStr);

		util::XArray<ColumnInfo> columnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(columnInfoList);

		NoSQLContainer targetContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);

		bool idempotenceCheck = false;
		bool isSameName = false;
		if (targetContainer.isExists()) {
			if (!createTableOption.ifNotExists_) {
				bool isValid = true;
				if ((targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE)) {
					isSameName = true;
					if (partitioningType == SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
						isValid = false;
					}
				}
				else {
					isValid = false;
				}
				if (!isValid) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS,
						"Specified create table '" << tableName.name_ << "' already exists");
				}
				idempotenceCheck = true;
			}
			else {
				if ((targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE)) {
					getTablePartitioningInfo(alloc, *dsConfig, targetContainer, columnInfoList, partitioningInfo, option);
					assert(partitioningInfo.assignNumberList_.size() > 0);
					NodeAffinityNumber fixedSubContainerId = 0;
					NoSQLContainer* fixedSubContainer =  createNoSQLContainer(ec,
						tableName, partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[fixedSubContainerId], execution);
					getContainer(*fixedSubContainer, false, option);
					if (fixedSubContainer->isExists()) {
						return;
					}
					util::XArray<uint8_t> subContainerSchema(alloc);
					getLargeBinaryRecord(
						alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
						targetContainer, columnInfoList, subContainerSchema, option);
					TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
					getLargeRecord<TablePartitioningIndexInfo>(
						alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
						targetContainer, columnInfoList, tablePartitioningIndexInfo, option);
					TableExpirationSchemaInfo info;
					createSubContainer(info, alloc, this, targetContainer, fixedSubContainer, subContainerSchema,
						partitioningInfo, tablePartitioningIndexInfo, tableName, partitionNum,
						partitioningInfo.assignNumberList_[fixedSubContainerId]);
					dumpRecoverContainer(alloc, tableName.name_, fixedSubContainer);
				}
				return;
			}
		}

		int32_t partitionColumnId = -1;
		int32_t subPartitionColumnId = -1;
		int64_t intervalValue = 0;
		uint8_t intervalUnit = UINT8_MAX;

		int32_t primaryKeyColumnId = -1;
		util::Vector<ColumnId> primaryKeyList(alloc);
		NoSQLUtils::checkPrimaryKey(alloc, createTableOption, primaryKeyList);
		bool isTimestampFamily = false;

		if (isPartitioning) {
			const util::String& partitioningColumnName =
				*createTableOption.partitionColumn_->name_;
			if (createTableOption.partitionColumn_->nameCaseSensitive_) {
				for (int32_t i = 0; i < static_cast<int32_t>(
					createTableOption.columnInfoList_->size()); i++) {
					const util::String& columnName =
						*((*createTableOption.columnInfoList_)[i]->columnName_->name_);
					if (columnName.compare(partitioningColumnName) == 0) {
						partitionColumnId = i;
						break;
					}
				}
			}
			else {
				const util::String& normalizePartitiongColumnName
					= normalizeName(alloc, partitioningColumnName.c_str());
				for (int32_t i = 0; i < static_cast<int32_t>(
					createTableOption.columnInfoList_->size()); i++) {
					const util::String normalizeColumnName =
						normalizeName(alloc,
							(*createTableOption.columnInfoList_)[i]->columnName_->name_->c_str());
					if (normalizeColumnName.compare(normalizePartitiongColumnName) == 0) {
						partitionColumnId = i;
						break;
					}
				}
			}
			if (partitionColumnId == -1) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
					"Partitioning column='" << partitioningColumnName
					<< "' is not exists on table='" << tableName.name_ << "'");
			}

			if (partitioningType == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
				if (createTableOption.subPartitionColumn_ == NULL) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
						"SubPartitioning column is not specified on table='" << tableName.name_ << "'");
				}
				const util::String& subPartitioningColumnName =
					*createTableOption.subPartitionColumn_->name_;
				if (createTableOption.subPartitionColumn_->nameCaseSensitive_) {
					for (int32_t i = 0; i < static_cast<int32_t>(
						createTableOption.columnInfoList_->size()); i++) {
						const util::String& columnName =
							*((*createTableOption.columnInfoList_)[i]->columnName_->name_);
						if (columnName.compare(subPartitioningColumnName) == 0) {
							subPartitionColumnId = i;
							break;
						}
					}
				}
				else {
					const util::String normalizeSubPartitiongColumnName =
						normalizeName(alloc, subPartitioningColumnName.c_str());
					for (int32_t i = 0; i < static_cast<int32_t>(
						createTableOption.columnInfoList_->size()); i++) {
						const util::String normalizeColumnName =
							normalizeName(alloc,
								(*createTableOption.columnInfoList_)[i]->columnName_->name_->c_str());
						if (normalizeColumnName.compare(normalizeSubPartitiongColumnName) == 0) {
							subPartitionColumnId = i;
							break;
						}
					}
				}
				if (subPartitionColumnId == -1) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
						"SubPartitioning column='" << subPartitioningColumnName
						<< "' is not exists on table='" << tableName.name_ << "'");
				}
			}
			if (primaryKeyList.size() > 0) {
				bool find = false;
				for (size_t pos = 0; pos < primaryKeyList.size(); pos++) {
					if (primaryKeyList[pos] ==
							static_cast<ColumnId>(partitionColumnId)) {
						find = true;
						break;
					}
				}
				if (find && static_cast<ColumnId>(subPartitionColumnId) !=
						UNDEF_PARTITIONID) {
					find = false;
					for (size_t pos = 0; pos < primaryKeyList.size(); pos++) {
						if (primaryKeyList[pos] ==
								static_cast<ColumnId>(subPartitionColumnId)) {
							find = true;
							break;
						}
					}
				}
				if (!find) {
					bool isException = jobManager_->getExecutionManager()
						->getSQLConfig().isPartitioningRowKeyConstraint();
					if (!isException) {
						GS_TRACE_WARNING(SQL_SERVICE,
							GS_TRACE_SQL_DDL_TABLE_PARTITIONING_PRIMARY_KEY,
							"Partitioning keys must be included in primary keys");
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
							"Partitioning keys must be included in primary keys");
					}
				}
			}

			if (SyntaxTree::isIncludeHashPartitioningType(partitioningType)) {
				if (createTableOption.partitionInfoNum_ < 1
					|| createTableOption.partitionInfoNum_ >
					TABLE_PARTITIONING_MAX_HASH_PARTITIONING_NUM) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Hash partitioning count=" << createTableOption.partitionInfoNum_ << " is out of range");
				}
			}
			if (SyntaxTree::isRangePartitioningType(partitioningType)) {
				if (!(*createTableOption.columnInfoList_)[partitionColumnId]->hasNotNullConstraint()) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
						"Interval partitioning column='"
						<< (*createTableOption.columnInfoList_)
						[partitionColumnId]->columnName_->name_->c_str()
						<< "' must be specifed 'NOT NULL' constraint");
				}
				int64_t intervalValueBase = createTableOption.optInterval_;
				assert(intervalValueBase != 0);
				TupleList::TupleColumnType columnType
					= (*createTableOption.columnInfoList_)[partitionColumnId]->type_;
				if (TupleColumnTypeUtils::isTimestampFamily(columnType)) {
					isTimestampFamily = true;
					switch (createTableOption.optIntervalUnit_) {
					case util::DateTime::FIELD_DAY_OF_MONTH :
						intervalValue = 24 * 3600 * 1000;
						intervalUnit = static_cast<uint8_t>(util::DateTime::FIELD_DAY_OF_MONTH);
						break;
					case util::DateTime::FIELD_HOUR:
						intervalValue = 1 * 3600 * 1000;
						intervalUnit = static_cast<uint8_t>(util::DateTime::FIELD_HOUR);
						break;
					default:
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"You can only specify 'day' or 'hour' for interval time unit");
						break;
					}

					if (intervalValue > INT64_MAX / intervalValueBase) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_OVERFLOW, "Interval value overflow as long value");
					}
					intervalValue = intervalValue * intervalValueBase;
				}
				else {
					intervalValue = createTableOption.optInterval_;
					if (createTableOption.optIntervalUnit_ != -1) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval time unit can be specified only when partitioning key column type is timestamp");
					}
					switch (columnType) {
					case TupleList::TYPE_BYTE:
						if (intervalValue >= static_cast<int64_t>(INT8_MAX) + 1) {
							GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
								"Interval value for byte type partitioning key column must be less than "
								<< (static_cast<int64_t>(INT8_MAX) + 1));
						}
						break;
					case TupleList::TYPE_SHORT:
						if (intervalValue >= static_cast<int64_t>(INT16_MAX) + 1) {
							GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
								"Interval value for short type partitioning key column must be less than "
								<< (static_cast<int64_t>(INT16_MAX) + 1));
						}
						break;
					case TupleList::TYPE_INTEGER:
						if (intervalValue >= static_cast<int64_t>(INT32_MAX) + 1) {
							GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
								"Interval value for integer type partitioning key column must be less than "
								<< (static_cast<int64_t>(INT32_MAX) + 1));
						}
						break;
					case TupleList::TYPE_LONG:
						if (partitioningType == SyntaxTree::TABLE_PARTITION_TYPE_RANGE) {
							if (intervalValue < TABLE_PARTITIONING_MIN_INTERVAL_VALUE) {
								GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
									"Interval value for long type partitioning key column must be greater than or equal "
									<< TABLE_PARTITIONING_MIN_INTERVAL_VALUE);
							}
						}
						if (partitioningType == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
							int32_t limitValue =
								TABLE_PARTITIONING_MIN_INTERVAL_VALUE * createTableOption.partitionInfoNum_;
							if (intervalValue < limitValue) {
								GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
									"Interval value for long type partitioning key column must be greater than or equal "
									<< limitValue << "(" << TABLE_PARTITIONING_MIN_INTERVAL_VALUE
									<< "*" << createTableOption.partitionInfoNum_ << ")");
							}
						}
						break;
					default:
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval partitioning is not supported for specified column '"
							<< (*createTableOption.columnInfoList_)[partitionColumnId]->columnName_->name_->c_str()
							<< "' data type");
					}
				}
			}
		}
		if (partitioningInfo.isRowExpiration()) {
			if (!createTableOption.isTimeSeries()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Row expiration definition must be timeseries container");
			}
		}
		else if (partitioningInfo.isTableExpiration()) {
			if (SyntaxTree::isRangePartitioningType(partitioningType)) {
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Table expiration definition must be interval or interval-hash partitioning");
			}
			if (partitionColumnId == -1) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Partitioning columnId of partition expiration must be defined");
			}
			TupleList::TupleColumnType partitioningColumnType
				= (*createTableOption.columnInfoList_)[partitionColumnId]->type_;
			if (!TupleColumnTypeUtils::isTimestampFamily(partitioningColumnType)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Expiration definition column must be timestamp type");
			}
		}
		NoSQLUtils::checkSchemaValidation(alloc, *dsConfig,
			tableName.name_, containerSchema, containerType);

		schemaInfo = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) TableSchemaInfo(globalVarAlloc_);

		if (isPartitioning) {
			schemaInfo->containerAttr_ = CONTAINER_ATTR_LARGE;
			KeyConstraint keyConstraint = KeyConstraint::getNoLimitKeyConstraint();
			FullContainerKey metaContainerKey(alloc, keyConstraint, 0,
				tableName.name_, strlen(tableName.name_));
			const FullContainerKeyComponents normalizedComponents =
				metaContainerKey.getComponents(alloc, false);
			if (normalizedComponents.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER
				|| normalizedComponents.affinityStringSize_ > 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITIONING_TABLE_NAME,
					"Table partitioning and node affinity cannot be specified at the same time, "
					<< "tableName='" << tableName.name_ << "'");
			}
			partitioningInfo.partitionColumnName_
				= createTableOption.partitionColumn_->name_->c_str();
			assert(partitionColumnId != -1);
			partitioningInfo.partitioningColumnId_ = partitionColumnId;
			partitioningInfo.partitionType_ = createTableOption.partitionType_;
			partitioningInfo.partitioningNum_ = createTableOption.partitionInfoNum_;
			if (partitionColumnId != -1) {
				partitioningInfo.partitionColumnType_
					= (*createTableOption.columnInfoList_)[partitionColumnId]->type_;
			}
			if (subPartitionColumnId != -1) {
				partitioningInfo.subPartitionColumnType_
					= (*createTableOption.columnInfoList_)[subPartitionColumnId]->type_;
				partitioningInfo.subPartitioningColumnId_ = subPartitionColumnId;
				partitioningInfo.subPartitioningColumnName_
					= createTableOption.subPartitionColumn_->name_->c_str();
			}
			partitioningInfo.primaryColumnId_ = primaryKeyColumnId;
			partitioningInfo.containerType_ = containerType;
			partitioningInfo.intervalValue_ = intervalValue;
			partitioningInfo.intervalUnit_ = intervalUnit;

			bool setIntervalOption = false;
			bool setIntervalPosition = false;
			if (createTableOption.propertyMap_) {
				SyntaxTree::DDLWithParameterMap& paramMap = *createTableOption.propertyMap_;
				SyntaxTree::DDLWithParameterMap::iterator it;
				it = paramMap.find(DDLWithParameter::INTERVAL_WORKER_GROUP);
				if (it != paramMap.end()) {
					if (partitioningType != SyntaxTree::TABLE_PARTITION_TYPE_RANGE) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval_worker_group must be specified for interval partitioning table");
					}
					if (!isTimestampFamily) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Partitioning column type of interval_worker_group must be TIMESTAMP");
					}
					TupleValue& value = (*it).second;
					if (value.getType() != TupleList::TYPE_LONG) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Partitioning column type of interval_worker_group must be TIMESTAMP");
					}
					int64_t currentValue = value.get<int64_t>();
					if (currentValue > INT32_MAX - 1) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Value of Interval_worker_group(" << currentValue << ") must be less than max value of integer");
					}
					if (currentValue < 0) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Value of Interval_worker_group(" << currentValue << ") must be positive value");
					}
					partitioningInfo.setIntervalWorkerGroup(static_cast<uint32_t>(currentValue));
					setIntervalOption = true;
				}
				it = paramMap.find(DDLWithParameter::INTERVAL_WORKER_GROUP_POSITION);
				if (it != paramMap.end()) {
					if (partitioningType != SyntaxTree::TABLE_PARTITION_TYPE_RANGE) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval_worker_group_position must be specified for interval partitioning table");
					}
					if (!setIntervalOption) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER, 
							"'Interval_worker_group_position  must be specified with interval_worker_group");
					}
					TupleValue& value = (*it).second;
					if (value.getType() != TupleList::TYPE_LONG) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Partitioning column type of interval_worker_group_position must be TIMESTAMP");
					}
					int64_t currentValue = value.get<int64_t>();
					if (currentValue > INT32_MAX - 1) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Value of Interval_worker_group_position(" << currentValue << ") must be less than max value of integer");
					}
					int32_t groupPos = static_cast<uint32_t>(currentValue);
					if (groupPos < -1) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Value of Interval_worker_group_position(" << currentValue << ") must be larger than minus 1");
					}
					partitioningInfo.setIntervalWorkerGroupPosition(groupPos);
					setIntervalOption = true;
					setIntervalPosition = true;
				}
			}

			int64_t seed = util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
			ClientId& clientId = execution->getContext().getId();
			const uint32_t crcValue1 = util::CRC32::calculate(
				tableName.name_, strlen(tableName.name_));
			const uint32_t crcValue2 = util::CRC32::calculate(
				clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
			seed += (crcValue1 + crcValue2 + clientId.sessionId_ + baseInfo->execId_);

			ClusterStatistics clusterStat(alloc, pt_, partitioningInfo.partitioningNum_, seed);
			if (setIntervalOption) {
				clusterStat.setOptionalIntervalAssignment(jobManager_->getTransactionService()->getManager()->getPartitionGroupConfig(),
					partitioningInfo.getIntervalWorkerGroup(), partitioningInfo.getIntervalWorkerGroupPosition(), setIntervalPosition);
			}
			util::Vector<PartitionId> condensedPartitionIdList(alloc);
			util::Vector<NodeAffinityNumber> assignNumberList(alloc);
			clusterStat.generateStatistics(createTableOption.partitionType_,
				targetContainer.getPartitionId(),
				condensedPartitionIdList, assignNumberList);
			partitioningInfo.setCondensedPartitionIdList(condensedPartitionIdList);
			partitioningInfo.setSubContainerPartitionIdList(assignNumberList);
			partitioningInfo.activeContainerCount_ = assignNumberList.size();
			if (partitioningInfo.partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
				partitioningInfo.assignValueList_.push_back(0);
			}
			TablePartitioningInfo<util::StackAllocator> checkPartitioningInfo(alloc);
			if (idempotenceCheck) {
				getTablePartitioningInfo(alloc, *dsConfig, targetContainer, columnInfoList, checkPartitioningInfo, option);
				util::XArray<uint8_t> subContainerSchema(alloc);
				getLargeBinaryRecord(
					alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
					targetContainer, columnInfoList, subContainerSchema, option);
				if (!checkPartitioningInfo.checkSchema(
					partitioningInfo, containerSchema, subContainerSchema)) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS,
						"Specified create table '" << tableName.name_ << "' already exists");
				}
				currentPartitioningInfo = &checkPartitioningInfo;
				getLargeRecord<TablePartitioningIndexInfo>(
					alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					targetContainer, columnInfoList, tablePartitioningIndexInfo, option);
			}

			util::XArray<uint8_t> partitioningInfoBinary(alloc);
			util::XArrayByteOutStream outStream =
				util::XArrayByteOutStream(util::XArrayOutStream<>(partitioningInfoBinary));
			util::ObjectCoder().encode(outStream, partitioningInfo);

			OutputMessageRowStore outputMrs(*dsConfig, &columnInfoList[0],
				static_cast<uint32_t>(columnInfoList.size()), fixedPart, varPart, false);

			baseInfo->startSync(PUT_CONTAINER);

			if (!idempotenceCheck) {
				NoSQLUtils::checkSchemaValidation(alloc, *dsConfig,
					tableName.name_, metaContainerSchema, COLLECTION_CONTAINER);

				TableExpirationSchemaInfo info;
				RenameColumnSchemaInfo renameColInfo;
				putContainer(info, renameColInfo, metaContainerSchema, 0, COLLECTION_CONTAINER,
					CONTAINER_ATTR_LARGE, false, targetContainer,
					&containerSchema, &partitioningInfoBinary, option);
				partitioningInfo.largeContainerId_ = targetContainer.getContainerId();
			}
			baseInfo->endSync();

			int32_t currentPartitioningCount = currentPartitioningInfo->getCurrentPartitioningCount();
			baseInfo->updateAckCount(currentPartitioningCount);
			NodeAffinityNumber subContainerId;
			try {
				for (subContainerId = 0;
						subContainerId < static_cast<NodeAffinityNumber>(
								currentPartitioningCount);
						subContainerId++) {
					if (subContainerId != 0
						&& currentPartitioningInfo->assignStatusList_[subContainerId] == INDEX_STATUS_DROP_START) {
						continue;
					}
					currentContainer = createNoSQLContainer(ec,
						tableName, currentPartitioningInfo->largeContainerId_,
						currentPartitioningInfo->assignNumberList_[subContainerId], execution);
					getContainer(*currentContainer, false, option);
					if (currentContainer->isExists()) {
					}
					else {
						TableExpirationSchemaInfo info;
						partitioningInfo.checkTableExpirationSchema(info, subContainerId);
						createSubContainer(info, alloc, this, targetContainer, currentContainer, containerSchema,
							*currentPartitioningInfo, tablePartitioningIndexInfo, tableName, partitionNum,
							currentPartitioningInfo->assignNumberList_[subContainerId]);
					}
					schemaInfo->setContainerInfo(
							static_cast<int32_t>(subContainerId),
							*currentContainer);
					if (subContainerId == 0) {
						schemaInfo->setOptionList(optionList);
					}
					if (currentPartitioningInfo->partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
						schemaInfo->affinityMap_.insert(
							std::make_pair(currentPartitioningInfo->assignNumberList_[
								subContainerId], subContainerId));
					}
					schemaInfo->containerInfoList_.back().affinity_
						= currentPartitioningInfo->assignNumberList_[subContainerId];
					containerList.push_back(currentContainer);
					currentContainer = NULL;
				}
			}
			catch (std::exception& e) {
				if (subContainerId == 0) {
					targetContainer.dropContainer(option);
				}
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}


			if (primaryKeyList.size() > 0) {
				if (primaryKeyList.size() == 1) {
					schemaInfo->setPrimaryKeyIndex(primaryKeyList[0]);
				}
				else {
					columnIdList
						= ALLOC_VAR_SIZE_NEW(globalVarAlloc_) ColumnIdList(globalVarAlloc_);
					for (size_t columnPos = 0;
						columnPos < primaryKeyList.size(); columnPos++) {
						columnIdList->push_back(primaryKeyList[columnPos]);
					}
					schemaInfo->compositeColumnIdList_.push_back(columnIdList);
					appendSchema = true;
				}
			}
		}
		else {
			if (createTableOption.propertyMap_) {
				SyntaxTree::DDLWithParameterMap& paramMap = *createTableOption.propertyMap_;
				SyntaxTree::DDLWithParameterMap::iterator it;
				it = paramMap.find(DDLWithParameter::INTERVAL_WORKER_GROUP);
				if (it != paramMap.end()) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER, "interval_worker_group must be specified for interval partitioning");
				}
				it = paramMap.find(DDLWithParameter::INTERVAL_WORKER_GROUP_POSITION);
				if (it != paramMap.end()) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER, "interval_worker_group_position must be specified for interval partitioning");
				}
			}
			
			currentContainer = ALLOC_NEW(alloc) NoSQLContainer(ec,
				tableName, execution->getContext().getSyncContext(), execution);

			baseInfo->startSync(DROP_CONTAINER);

			TableExpirationSchemaInfo info;
			RenameColumnSchemaInfo renameColInfo;
			putContainer(info, renameColInfo, containerSchema, 0, containerType, CONTAINER_ATTR_SINGLE,
				false, *currentContainer, NULL, NULL, option);
			baseInfo->endSync();

			schemaInfo->containerAttr_ = CONTAINER_ATTR_SINGLE;
			schemaInfo->setContainerInfo(0, *currentContainer);
			containerList.push_back(currentContainer);
			currentContainer = NULL;
			schemaInfo->setOptionList(optionList);

			util::Vector<ColumnId> primaryKeyList(alloc);
			NoSQLUtils::checkPrimaryKey(alloc, createTableOption, primaryKeyList);
			if (primaryKeyList.size() > 0) {
				if (primaryKeyList.size() == 1) {
					schemaInfo->setPrimaryKeyIndex(primaryKeyList[0]);
				}
				else {
					columnIdList
						= ALLOC_VAR_SIZE_NEW(globalVarAlloc_) ColumnIdList(globalVarAlloc_);
					for (size_t columnPos = 0;
						columnPos < primaryKeyList.size(); columnPos++) {
						columnIdList->push_back(primaryKeyList[columnPos]);
					}
					schemaInfo->compositeColumnIdList_.push_back(columnIdList);
					appendSchema = true;
				}
			}
		}


		if (isPartitioning) {
			getLargeRecord<TablePartitioningInfo<SQLVariableSizeGlobalAllocator> >(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				targetContainer, columnInfoList, schemaInfo->getTablePartitioningInfo(), option);
		}

		SQLString key(globalVarAlloc_);
		NoSQLUtils::normalizeString(key, tableName.name_);

		schemaInfo->tableName_ = tableName.name_;
		putCacheEntry<
			SQLTableCacheEntry,
			SQLString,
			TableSchemaInfo*>(key, schemaInfo, false);

		appendCache = true;

		if (isSameName) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS_WITH_EXECUTION,
				"Specified create table '" << tableName.name_ << "' already exists");
		}
	}
	catch (std::exception& e) {
		removeCache(tableName.name_);
		if (!appendCache) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, schemaInfo);
			if (!appendSchema) {
				ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, columnIdList);
			}
		}
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::createView(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	DDLCommandType commandType = DDL_CREATE_VIEW;
	TableSchemaInfo* schemaInfo = NULL;
	NoSQLStoreOption option(execution);
	util::StackAllocator& alloc = ec.getAllocator();
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	bool appendCache = false;
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		util::XArray<uint8_t> containerSchema(alloc);
		util::XArray<uint8_t> metaContainerSchema(alloc);
		util::String affinityStr(alloc);
		NoSQLUtils::getAffinityValue(alloc, createTableOption, affinityStr);
		NoSQLUtils::makeLargeContainerSchema(alloc, metaContainerSchema,
			true, affinityStr);
		util::XArray<ColumnInfo> columnInfoList(alloc);

		NoSQLUtils::makeLargeContainerColumn(columnInfoList);
		NoSQLContainer targetContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS,
				"Specified create view '" << tableName.name_ << "' already exists");
		}

		schemaInfo = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) TableSchemaInfo(globalVarAlloc_);
		util::XArray<uint8_t> fixedPart(alloc);
		util::XArray<uint8_t> varPart(alloc);
		util::XArray<uint8_t> partitioningInfoBinary(alloc);
		baseInfo->startSync(PUT_CONTAINER);
		TableExpirationSchemaInfo info;
		RenameColumnSchemaInfo renameColInfo;
		putContainer(info, renameColInfo, metaContainerSchema, 0, COLLECTION_CONTAINER,
			CONTAINER_ATTR_VIEW, false, targetContainer,
			&containerSchema, &partitioningInfoBinary, option);
		baseInfo->endSync();
		NoSQLContainer viewContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		getContainer(viewContainer, false, option);

		if (targetContainer.isExists()) {
			ContainerAttribute attr = targetContainer.getContainerAttribute();
			if (attr != CONTAINER_ATTR_VIEW) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_MISMATCH_CONTAINER_ATTRIBUTE,
					"Target table must be VIEW, but current is TABLE");
			}
			util::XArray<uint8_t> viewInfoBinary(alloc);
			util::XArrayByteOutStream outStream =
				util::XArrayByteOutStream(util::XArrayOutStream<>(viewInfoBinary));
			ViewInfo viewInfo(alloc);
			if (createTableOption.optionString_ != NULL) {
				viewInfo.sqlString_ = *createTableOption.optionString_;
			}
			util::ObjectCoder().encode(outStream, viewInfo);
			util::XArray<uint8_t> fixedPart(alloc);
			util::XArray<uint8_t> varPart(alloc);
			OutputMessageRowStore outputMrs(*dsConfig, &columnInfoList[0],
				static_cast<uint32_t>(columnInfoList.size()), fixedPart, varPart, false);
			VersionInfo versionInfo;
			NoSQLUtils::makeLargeContainerRow(alloc, NoSQLUtils::LARGE_CONTAINER_KEY_VERSION,
				outputMrs, versionInfo);
			NoSQLUtils::makeLargeContainerRow(alloc, NoSQLUtils::LARGE_CONTAINER_KEY_VIEW_INFO,
				outputMrs, viewInfo);
			NoSQLStoreOption option(execution);
			viewContainer.putRowSet(fixedPart, varPart, 2, option);
			SQLString key(globalVarAlloc_);
			NoSQLUtils::normalizeString(key, tableName.name_);
			removeCache(tableName.name_);
			schemaInfo->tableName_ = tableName.name_;
			schemaInfo->containerAttr_ = CONTAINER_ATTR_VIEW;
			schemaInfo->setContainerInfo(0, viewContainer);
			schemaInfo->sqlString_ = viewInfo.sqlString_.c_str();

			putCacheEntry<
				SQLTableCacheEntry,
				SQLString,
				TableSchemaInfo*>(key, schemaInfo, false);

			appendCache = true;
		}
	}
	catch (std::exception& e) {
		removeCache(tableName.name_);
		if (!appendCache) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, schemaInfo);
		}
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropView(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	bool ifExists) {

	DDLCommandType commandType = DDL_DROP_VIEW;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);

		NoSQLContainer targetContainer(ec, tableName, execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (targetContainer.isExists()) {
			ContainerAttribute attr = targetContainer.getContainerAttribute();
			if (attr != CONTAINER_ATTR_VIEW) {
				if (!ifExists) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
						"Target table must be VIEW, but current is TABLE");
				}
				else {
					return;
				}
			}
			NoSQLStoreOption subOption(execution);
			targetContainer.dropContainer(subOption);
			removeCache(tableName.name_);
		}
		else {
			if (!ifExists) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_VIEW_NOT_EXISTS,
					"Specified view '" << tableName.name_ << "' is not exists");
			}
		}
	}
	catch (std::exception& e) {
		removeCache(tableName.name_);
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief テーブル削除
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] dbName DB名
	@param [in] tableName テーブル名
	@param [in] ifExists "IF EXISTS"指定の有無
	@note DROP TABLE テーブル名
*/
void NoSQLStore::dropTable(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	bool ifExists, DDLBaseInfo* baseInfo) {
	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_TABLE;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);

		NoSQLContainer targetContainer(ec, tableName, execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);

		if (!targetContainer.isExists()) {
			if (!ifExists) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
					"Specified drop table '" << tableName.name_ << "' is not exists");
			}
			else {
				baseInfo->isFinished_ = true;
				return;
			}
		}
		if (!NoSQLCommonUtils::isWritableContainer(targetContainer.getContainerAttribute(),
			targetContainer.getContainerType())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is read-only");
		}
		ContainerAttribute attr = targetContainer.getContainerAttribute();
		if (attr == CONTAINER_ATTR_VIEW) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}

		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);

		if (targetContainer.isLargeContainer()) {
			baseInfo->largeContainerId_ = targetContainer.getContainerId();

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);
			util::Vector<NoSQLContainer*> containerList(alloc);
			try {
				getTablePartitioningInfo(alloc, *dsConfig, targetContainer,
					columnInfoList, partitioningInfo, option);
			}
			catch (std::exception& e) {
				targetContainer.dropContainer(option);
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}

			targetContainer.dropContainer(option);

			int32_t currentPartitioningCount = 0;
			for (size_t pos = 0; pos < partitioningInfo.assignNumberList_.size(); pos++) {
				if (partitioningInfo.assignStatusList_[pos] != PARTITION_STATUS_DROP_START) {
					currentPartitioningCount++;
				}
			}
			if (currentPartitioningCount == 0) {
				baseInfo->startSync(DROP_CONTAINER);
				targetContainer.dropContainer(option);
				baseInfo->endSync();
				baseInfo->isFinished_ = true;
				return;
			}

			baseInfo->updateAckCount(currentPartitioningCount);
			NoSQLContainer* subContainer = NULL;
			baseInfo->processor_->setAsyncStart();
			uint32_t assignNo = 0;

			for (uint32_t subContainerId = 0;
				subContainerId < partitioningInfo.assignNumberList_.size(); subContainerId++) {
				if (partitioningInfo.assignStatusList_[subContainerId] == PARTITION_STATUS_DROP_START) {
					continue;
				}
				subContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[subContainerId], execution);
				try {
					getContainer(*subContainer, false, option);
				}
				catch (std::exception& e) {
				}
				subOption.subContainerId_ = assignNo;
				baseInfo->processor_->setAckStatus(subContainer, assignNo, 1);
				subContainer->dropContainer(subOption);
				assignNo++;
			}
		}
		else {
			baseInfo->updateAckCount(1);
			baseInfo->processor_->setAckStatus(&targetContainer, 0, 1);
			subOption.subContainerId_ = 0;
			targetContainer.dropContainer(subOption);
		}
	}
	catch (std::exception& e) {
		removeCache(tableName.name_);
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropTablePost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo) {
	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(execution);
	UNUSED_VARIABLE(dbName);
	UNUSED_VARIABLE(tableName);
	UNUSED_VARIABLE(baseInfo);

}

void NoSQLStore::createIndex(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	const CreateIndexOption& indexOption,
	DDLBaseInfo* baseInfo) {

	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_CREATE_INDEX;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (indexName.isCaseSensitive_) {
		option.caseSensitivity_.setIndexNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	option.ifNotExistsOrIfExists_ = indexOption.ifNotExists_;
	CreateIndexInfo* createIndexInfo
		= static_cast<CreateIndexInfo*>(baseInfo);
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
			tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(
			NULL, &connectedDbName, &tableName, &indexName);
		checkExecutable(alloc, commandType, execution, false, condition);
		uint32_t partitionNum = dbConnection_->getPartitionTable()->getPartitionNum();

		NoSQLContainer targetContainer(ec,
			NameWithCaseSensitivity(tableName.name_,
				tableName.isCaseSensitive_),
			execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);

		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_ << "' is not exists");
		}
		if (targetContainer.getContainerAttribute() == CONTAINER_ATTR_VIEW) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}
		if (!NoSQLCommonUtils::isWritableContainer(
			targetContainer.getContainerAttribute(),
			targetContainer.getContainerType())) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is read-only");
		}
		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);

		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);
		subOption.ifNotExistsOrIfExists_ = indexOption.ifNotExists_;

		util::Vector<ColumnId> indexColumnIdList(alloc);
		util::Vector<util::String> indexColumnNameStrList(alloc);
		util::Vector<NameWithCaseSensitivity> indexColumnNameList(alloc);

		for (size_t pos = 0; pos < indexOption.columnNameList_->size(); pos++) {
			const NameWithCaseSensitivity columnName(
				(*(indexOption.columnNameList_))[pos]->name_->c_str(),
				(*(indexOption.columnNameList_))[pos]->nameCaseSensitive_);
			indexColumnNameList.push_back(columnName);
			indexColumnNameStrList.push_back(util::String(columnName.name_, alloc));
			if (columnName.isCaseSensitive_) {
				option.caseSensitivity_.setColumnNameCaseSensitive();
			}
		}
		NoSQLContainer* currentContainer = NULL;

		if (targetContainer.isLargeContainer()) {
			baseInfo->largeContainerId_ = targetContainer.getContainerId();
			util::XArray<ColumnInfo> largeColumnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);

			getLargeContainerInfo<TablePartitioningInfo<util::StackAllocator>>(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				*dsConfig, targetContainer, largeColumnInfoList, partitioningInfo);

			int64_t currentPartitioningCount = partitioningInfo.getActiveContainerCount();
			if (currentPartitioningCount == 0) {
				baseInfo->isFinished_ = true;
				return;
			}
			currentContainer = createNoSQLContainer(ec,
				tableName, partitioningInfo.largeContainerId_,
				partitioningInfo.assignNumberList_[0], execution);
			getContainer(*currentContainer, false, option);
			TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
			util::XArray<uint8_t> containerSchema(alloc);
			bool isFirst = true;

			if (!currentContainer->isExists()) {
				isFirst = false;
				TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
				util::XArray<uint8_t> containerSchema(alloc);

				getLargeRecord<TablePartitioningIndexInfo>(
					alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					targetContainer, largeColumnInfoList, tablePartitioningIndexInfo, option);
				getLargeBinaryRecord(
					alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
					targetContainer, largeColumnInfoList, containerSchema, option);
				TableExpirationSchemaInfo info;
				createSubContainer(info, alloc, this, targetContainer, currentContainer, containerSchema,
					partitioningInfo, tablePartitioningIndexInfo, tableName, partitionNum,
					partitioningInfo.assignNumberList_[0]);
				dumpRecoverContainer(alloc, tableName.name_, currentContainer);
			}
			for (size_t pos = 0; pos < indexColumnNameList.size(); pos++) {
				ColumnId indexColId = currentContainer->getColumnId(
					alloc, indexColumnNameList[pos]);
				if (indexColId == static_cast<ColumnId>(-1)) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INDEX_NOT_EXISTS,
						"Specified column '" << indexColumnNameList[pos].name_ << "' is not exists");
				}
				indexColumnIdList.push_back(indexColId);
			}
			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = indexOption.ifNotExists_;
			retryOption.caseSensitivity_ = option.caseSensitivity_;

			if (execution->getContext().isRetryStatement()) {
				retryOption.ifNotExistsOrIfExists_ = true;
			}
			LargeExecStatus execStatus(alloc);
			TargetContainerInfo targetContainerInfo;
			execPartitioningOperation(ec, dbConnection_, INDEX_STATUS_CREATE_START, execStatus,
				targetContainer, UNDEF_NODE_AFFINITY_NUMBER, UNDEF_NODE_AFFINITY_NUMBER,
				retryOption, partitioningInfo, tableName, execution, targetContainerInfo);

			if (targetContainer.createLargeIndex(indexName.name_,
				indexColumnNameStrList,
				MAP_TYPE_DEFAULT,
				indexColumnIdList,
				option)) {
				createIndexInfo->isSameName_ = true;
			}
			uint32_t subContainerId = 0;

			util::String normalizedIndexName = normalizeName(alloc, indexName.name_);

			for (subContainerId = 0;
				subContainerId < currentPartitioningCount; subContainerId++) {
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[subContainerId], execution);
				getContainer(*currentContainer, false, option);
				if (!currentContainer->isExists()
					&& partitioningInfo.assignStatusList_[subContainerId] != PARTITION_STATUS_DROP_START) {
					if (isFirst) {
						getLargeRecord<TablePartitioningIndexInfo>(
							alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
							targetContainer, largeColumnInfoList, tablePartitioningIndexInfo, option);
						getLargeBinaryRecord(
							alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
							targetContainer, largeColumnInfoList, containerSchema, option);
						if (containerSchema.size() == 0) {
							GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
						}
						isFirst = false;
					}
					TableExpirationSchemaInfo info;
					partitioningInfo.checkTableExpirationSchema(info, subContainerId);
					createSubContainer(info, alloc, this, targetContainer, currentContainer, containerSchema,
						partitioningInfo, tablePartitioningIndexInfo, tableName, partitionNum,
						partitioningInfo.assignNumberList_[subContainerId]);
					dumpRecoverContainer(alloc, tableName.name_, currentContainer);
				}
			}
			try {
				NoSQLStoreOption cmdOption(execution);
				cmdOption.isSync_ = true;
				cmdOption.ifNotExistsOrIfExists_ = true;
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[0], execution);
				getContainer(*currentContainer, false, option);
				currentContainer->createNoSQLClientId();
				baseInfo->startSync(CREATE_INDEX);
				currentContainer->createIndex(
					indexName.name_, MAP_TYPE_DEFAULT, indexColumnIdList, cmdOption);
				baseInfo->endSync();
			}
			catch (std::exception& e) {
				LargeExecStatus execStatus(alloc);
				IndexInfo indexInfo(alloc);
				indexInfo.indexName_ = indexName.name_;
				targetContainer.updateLargeContainerStatus(INDEX_STATUS_DROP_END,
					UNDEF_NODE_AFFINITY_NUMBER, option, execStatus, &indexInfo);
				GS_RETHROW_USER_ERROR(e, "");
			}

			baseInfo->updateAckCount(
					static_cast<int32_t>(currentPartitioningCount));
			subOption.ifNotExistsOrIfExists_ = true;
			for (uint32_t subContainerId = 0;
				subContainerId < currentPartitioningCount; subContainerId++) {
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[subContainerId], execution);
				getContainer(*currentContainer, false, option);
				currentContainer->createNoSQLClientId();
				baseInfo->processor_->setAckStatus(currentContainer, subContainerId, 1);
				subOption.subContainerId_ = subContainerId;
				currentContainer->createIndex(
					indexName.name_, MAP_TYPE_DEFAULT, indexColumnIdList, subOption);
			}
		}
		else {
			for (size_t pos = 0; pos < indexColumnNameList.size(); pos++) {
				ColumnId indexColId = targetContainer.getColumnId(
					alloc, indexColumnNameList[pos]);
				if (indexColId == static_cast<ColumnId>(-1)) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INDEX_NOT_EXISTS,
						"Specified column '" << indexColumnNameList[pos].name_ << "' is not exists");
				}
				indexColumnIdList.push_back(indexColId);
			}
			baseInfo->updateAckCount(1);
			targetContainer.createNoSQLClientId();
			baseInfo->processor_->setAckStatus(&targetContainer, 0, 1);

			if (execution->getContext().isRetryStatement()) {
				subOption.ifNotExistsOrIfExists_ = true;
			}
			subOption.subContainerId_ = 0;
			targetContainer.createIndex(indexName.name_,
				MAP_TYPE_DEFAULT, indexColumnIdList, subOption);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::createIndexPost(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {

	util::StackAllocator& alloc = ec.getAllocator();

	DDLCommandType commandType = DDL_CREATE_INDEX;
	DDLProcessor* ddlProcessor = static_cast<DDLProcessor*>(baseInfo->processor_);
	CreateIndexInfo* createIndexInfo = static_cast<CreateIndexInfo*>(baseInfo);

	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (indexName.isCaseSensitive_) {
		option.caseSensitivity_.setIndexNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}

	try {
		NoSQLContainer targetContainer(ec,
			NameWithCaseSensitivity(tableName.name_, tableName.isCaseSensitive_),
			execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_ << "' is already dropped");
		}

		if (targetContainer.isViewContainer()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}

		if (targetContainer.isLargeContainer()) {
			if (baseInfo->largeContainerId_ != targetContainer.getContainerId()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
					"Target large container '" << tableName.name_ << "'is already removed");
			}
			ddlProcessor->setAsyncCompleted();
			LargeExecStatus execStatus(alloc);
			IndexInfo indexInfo(alloc);
			indexInfo.indexName_ = indexName.name_;
			targetContainer.updateLargeContainerStatus(INDEX_STATUS_CREATE_END,
				UNDEF_NODE_AFFINITY_NUMBER, option, execStatus, &indexInfo);
		}
		if (createIndexInfo->isSameName_) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS_WITH_EXECUTION,
				"Specified table '" << tableName.name_ << "', indexName '" << indexName.name_ << "' already exists");
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief 索引削除
	@param [in] alloc アロケータ
	@param [in] execution SQLExecution
	@param [in] indexName 索引名
	@param [in] dbName DB名
	@note DROP INDEX [IF EXISTS] [データベース名.]インデックス名
*/
void NoSQLStore::dropIndex(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	bool ifExists, DDLBaseInfo* baseInfo) {
	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_INDEX;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (indexName.isCaseSensitive_) {
		option.caseSensitivity_.setIndexNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	option.ifNotExistsOrIfExists_ = ifExists;
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, &indexName);
		checkExecutable(alloc, commandType, execution, false, condition);
		uint32_t partitionNum = dbConnection_->getPartitionTable()->getPartitionNum();

		NoSQLContainer targetContainer(ec,
			NameWithCaseSensitivity(tableName.name_, tableName.isCaseSensitive_),
			execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified create index table '" << tableName.name_ << "' is not exists");
		}
		if (!NoSQLCommonUtils::isWritableContainer(
			targetContainer.getContainerAttribute(),
			targetContainer.getContainerType())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << targetContainer.getName() << "' is read-only");
		}
		if (targetContainer.isViewContainer()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);

		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);
		subOption.ifNotExistsOrIfExists_ = ifExists;

		if (targetContainer.isLargeContainer()) {
			baseInfo->largeContainerId_ = targetContainer.getContainerId();
			util::XArray<ColumnInfo> largeColumnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
			getLargeContainerInfo<TablePartitioningInfo<util::StackAllocator>>(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				*dsConfig, targetContainer, largeColumnInfoList, partitioningInfo);

			int64_t currentPartitioningCount = partitioningInfo.getActiveContainerCount();
			if (currentPartitioningCount == 0) {
				baseInfo->isFinished_ = true;
				return;
			}

			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = option.ifNotExistsOrIfExists_;
			LargeExecStatus execStatus(alloc);
			TargetContainerInfo targetContainerInfo;
			IndexInfo* indexInfo = &execStatus.indexInfo_;
			execStatus.indexInfo_.indexName_ = indexName.name_;
			targetContainer.updateLargeContainerStatus(INDEX_STATUS_DROP_START,
				UNDEF_NODE_AFFINITY_NUMBER, retryOption, execStatus, indexInfo);

			baseInfo->updateAckCount(
					static_cast<int32_t>(currentPartitioningCount));
			subOption.ifNotExistsOrIfExists_ = true;

			NoSQLContainer* currentContainer;
			TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
			util::XArray<uint8_t> containerSchema(alloc);
			bool isFirst = true;
			for (uint32_t subContainerId = 0;
				subContainerId < currentPartitioningCount; subContainerId++) {
				currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[subContainerId], execution);
				getContainer(*currentContainer, false, option);
				if (!currentContainer->isExists()
					&& partitioningInfo.assignStatusList_[subContainerId] != PARTITION_STATUS_DROP_START) {
					if (isFirst) {
						getLargeRecord<TablePartitioningIndexInfo>(
							alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
							targetContainer, largeColumnInfoList, tablePartitioningIndexInfo, option);
						getLargeBinaryRecord(
							alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
							targetContainer, largeColumnInfoList, containerSchema, option);
						isFirst = false;
					}
					TableExpirationSchemaInfo info;
					partitioningInfo.checkTableExpirationSchema(info, subContainerId);
					createSubContainer(info, alloc, this, targetContainer, currentContainer, containerSchema,
						partitioningInfo, tablePartitioningIndexInfo, tableName, partitionNum,
						partitioningInfo.assignNumberList_[subContainerId]);
					dumpRecoverContainer(alloc, tableName.name_, currentContainer);
				}
				baseInfo->processor_->setAckStatus(currentContainer, subContainerId, 1);
				subOption.subContainerId_ = subContainerId;
				currentContainer->dropIndex(indexName.name_, subOption);
			}
		}
		else {
			baseInfo->updateAckCount(1);
			baseInfo->processor_->setAckStatus(&targetContainer, 0, 1);
			subOption.subContainerId_ = 0;
			targetContainer.dropIndex(indexName.name_, subOption);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropIndexPost(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {

	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_INDEX;
	const char8_t* connectedDbName = dbName.name_;
	adjustConnectedDb(connectedDbName);

	DDLProcessor* ddlProcessor = static_cast<DDLProcessor*>(baseInfo->processor_);
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (indexName.isCaseSensitive_) {
		option.caseSensitivity_.setIndexNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}

	try {
		NoSQLContainer targetContainer(ec,
			NameWithCaseSensitivity(tableName.name_, tableName.isCaseSensitive_),
			execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_ << "' is already dropped");
		}

		if (targetContainer.isViewContainer()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}

		if (targetContainer.isLargeContainer()) {
			if (baseInfo->largeContainerId_ != targetContainer.getContainerId()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
					"Target large container '" << tableName.name_ << "'is already removed");
			}

			ddlProcessor->setAsyncCompleted();
			LargeExecStatus execStatus(alloc);
			IndexInfo indexInfo(alloc);
			indexInfo.indexName_ = indexName.name_;
			targetContainer.updateLargeContainerStatus(INDEX_STATUS_DROP_END,
				UNDEF_NODE_AFFINITY_NUMBER, option, execStatus, &indexInfo);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief テーブルを取得する
*/
void NoSQLStore::checkDatabase(
	util::StackAllocator& alloc,
	DDLCommandType commandType,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName) {

	const char8_t* tmpDbName = dbName.name_;
	adjustConnectedDb(tmpDbName);

	NameWithCaseSensitivity connectedDbName(
		tmpDbName, dbName.isCaseSensitive_);

	ExecuteCondition condition(
		NULL, &connectedDbName, &tableName, NULL);

	checkExecutable(
		alloc, commandType, execution, false, condition);
}

TableLatch::TableLatch(
	EventContext& ec,
	DBConnection* conn,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	bool withCache,
	bool differenceCheck) : cache_(NULL), entry_(NULL),
	schemaInfo_(NULL), useCache_(false) {

	int64_t emNow = conn->getSQLService()->getEE()->getMonotonicTime();
	SQLVariableSizeGlobalAllocator& globalVarAlloc
		= conn->getVarAllocator();
	DDLCommandType commandType = CMD_GET_TABLE;
	NoSQLStore::SQLTableCacheEntry* prevEntry = NULL;
	TableSchemaInfo* prevSchemaInfo = NULL;

	NoSQLStore* store = conn->getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());

	cache_ = &(store->getCache());
	SQLString key(globalVarAlloc);
	NoSQLUtils::normalizeString(key, tableName.name_);

	try {
		bool searchOnly = (withCache && !differenceCheck);
		bool cacheSearch = (withCache || differenceCheck);
		bool findCache = false;
		bool diffLoad = differenceCheck;

		if (cacheSearch) {
			entry_ = cache_->get(key);
			if (entry_) {
				schemaInfo_ = entry_->getValue();
				if (!diffLoad) {
					diffLoad = schemaInfo_->isDiffLoad();
				}

				if (schemaInfo_->isDisableCache()) {
					schemaInfo_ = NULL;
				}
				else if (tableName.isCaseSensitive_
					&& strcmp(schemaInfo_->tableName_.c_str(), tableName.name_)) {
					if (searchOnly && !diffLoad) {
						bool isExists = store->checkContainerExist(ec, execution, tableName);
						if (isExists) {
							findCache = true;
						}
					}
				}
				else {
					if (searchOnly && !diffLoad) {
						findCache = true;
					}
				}
			}
		}

		if (findCache) {
			conn->getExecutionManager()->incCacheHit();
			useCache_ = true;
			schemaInfo_->setLastExecutedTime(emNow);
			return;
		}

		conn->getExecutionManager()->incCacheMissHit();

		prevEntry = entry_;
		entry_ = NULL;
		prevSchemaInfo = schemaInfo_;
		useCache_ = false;

		cache_->remove(key);

		schemaInfo_ = NULL;

		try {
			schemaInfo_ =
				ALLOC_VAR_SIZE_NEW(globalVarAlloc) TableSchemaInfo(globalVarAlloc);

			store->getTable(ec,
				execution, dbName, tableName, emNow, schemaInfo_, prevSchemaInfo);

			if (prevEntry) {
				cache_->release(prevEntry);
			}
			prevEntry = NULL;
			schemaInfo_->setLastExecutedTime(emNow);
		}
		catch (std::exception& e) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc, schemaInfo_);
			schemaInfo_ = NULL;
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}

		entry_ = cache_->put(key, schemaInfo_, true);
	}
	catch (std::exception& e) {
		cache_->remove(key);
		if (prevEntry) {
			cache_->release(prevEntry);
		}
		entry_ = NULL;
		GS_RETHROW_USER_OR_SYSTEM(
			e, getCommandString(commandType)
			<< " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

class LocalWatcher {
public:
	LocalWatcher(SQLExecutionManager* executionManager,
		const char* tableName, int32_t timeout) :
		executionManager_(executionManager),
		tableName_(tableName), timeout_(timeout), searchCount_(0),
		skipCount_(0), type_(SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
		watch_.start();
	}

	~LocalWatcher() {
		uint32_t lap = watch_.elapsedMillis();
		executionManager_->setCacheLoadStat(searchCount_, skipCount_, lap);
		if (static_cast<int64_t>(lap) >= timeout_) {
			if (type_ == SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
				GS_TRACE_WARNING(
					SQL_SERVICE, GS_TRACE_SQL_LONG_UPDATING_CACHE_TABLE,
					"Long updating cache, table name=" << tableName_ <<
					", elapsedMillis=" << lap);
			}
			else {
				GS_TRACE_WARNING(
					SQL_SERVICE, GS_TRACE_SQL_LONG_UPDATING_CACHE_TABLE,
					"Long updating cache, table name=" << tableName_ <<
					", elapsedMillis=" << lap << ", partitioningType=" <<
					getTablePartitionTypeName(type_) << ",searchCount=" <<
					searchCount_ << ",skipCount=" << skipCount_);
			}
		}
	}

	void setPartitioningType(SyntaxTree::TablePartitionType type) {
		type_ = type;
	}

	void setSearch() { searchCount_++; }
	void setSkip() { skipCount_++; }

private:
	util::Stopwatch watch_;
	SQLExecutionManager* executionManager_;
	const char* tableName_;
	int32_t timeout_;
	int32_t searchCount_;
	int32_t skipCount_;
	SyntaxTree::TablePartitionType type_;
};

bool NoSQLStore::getTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		int64_t emNow,
		TableSchemaInfo*& schemaInfo,
		TableSchemaInfo* prevSchemaInfo) {
	UNUSED_VARIABLE(emNow);

	util::StackAllocator& alloc = ec.getAllocator();
	DDLCommandType commandType = CMD_GET_TABLE;

	SQLExecutionManager* executionManager = dbConnection_->getExecutionManager();
	LocalWatcher watcher(executionManager,
		tableName.name_, executionManager->getTableCacheDumpLimitTime());

	try {

		NoSQLStoreOption option(execution, &dbName, &tableName);
		checkDatabase(alloc, commandType, execution, dbName, tableName);

		int32_t skipCount = 0;
		int32_t searchCount = 0;
		int32_t recoveryCount = 0;

		NoSQLContainer targetContainer(ec, tableName,
			execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);

		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_ << "' is not found");
		}

		bool isPartitioning
			= (targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE);

		TablePartitioningInfo<SQLVariableSizeGlobalAllocator>
			& currentPartitioningInfo = schemaInfo->getTablePartitioningInfo();

		if (isPartitioning) {
			schemaInfo->containerAttr_ = CONTAINER_ATTR_LARGE;

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);

			getLargeRecord<TablePartitioningInfo<SQLVariableSizeGlobalAllocator> >(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				targetContainer, columnInfoList, currentPartitioningInfo, option);
			uint32_t partitionNum = dbConnection_->getPartitionTable()->getPartitionNum();
			bool isRangePartition = SyntaxTree::isRangePartitioningType(
				currentPartitioningInfo.getPartitioningType());
			watcher.setPartitioningType(
				static_cast<SyntaxTree::TablePartitionType>
				(currentPartitioningInfo.getPartitioningType()));

			int32_t actualPos = 0;

			for (uint32_t subContainerId = 0;
					subContainerId < static_cast<uint32_t>(
							currentPartitioningInfo.getCurrentPartitioningCount());
					subContainerId++) {

				LargeContainerStatusType status
					= currentPartitioningInfo.getPartitionStatus(subContainerId);
				if (status != PARTITION_STATUS_CREATE_END) {
					continue;
				}

				NodeAffinityNumber affinity = currentPartitioningInfo.getAffinity(subContainerId);
				schemaInfo->setAffinity(affinity, actualPos);

				if (prevSchemaInfo && subContainerId != 0 && isRangePartition) {
					TableContainerInfo* currentContainerInfo
						= prevSchemaInfo->getTableContainerInfo(affinity);
					if (currentContainerInfo) {
						schemaInfo->setContainerInfo(actualPos, *currentContainerInfo, affinity);
						actualPos++;
						skipCount++;
						storeSkipCount_++;
						watcher.setSkip();
						continue;
					}
				}

				NoSQLContainer* currentContainer = createNoSQLContainer(ec,
					tableName, currentPartitioningInfo.getLargeContainerId(),
					affinity, execution);

				currentContainer->getContainerInfo(option);
				if (!currentContainer->isExists()) {
					if (currentPartitioningInfo.checkRecoveryPartitioningType(subContainerId)) {
						currentContainer = recoveryContainer(alloc, ec, this, currentPartitioningInfo,
							targetContainer, execution, subContainerId, tableName,
							partitionNum, affinity);
						if (!currentContainer) {
							continue;
						}
						recoveryCount++;
					}
				}

				schemaInfo->setContainerInfo(actualPos, *currentContainer, affinity);
				actualPos++;
				searchCount++;
				storeAccessCount_++;
				watcher.setSearch();
			}
		}
		else {
			schemaInfo->containerAttr_ = targetContainer.getContainerAttribute();
			schemaInfo->setContainerInfo(0, targetContainer);
		}
		setStat(searchCount, skipCount, recoveryCount, false);

		return true;
	}
	catch (std::exception& e) {

		GS_RETHROW_USER_OR_SYSTEM(
			e, getCommandString(commandType)
			<< " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::estimateIndexSearchSize(
		EventContext &ec, SQLExecution &execution,
		SQLIndexStatsCache &indexStats, uint64_t requester) {
	util::StackAllocator &alloc = ec.getAllocator();

	SQLIndexStatsCache::KeyList keyList(alloc);
	util::Vector<int64_t> estimationList(alloc);

	NoSQLStoreOption option(&execution, NULL, NULL);
	NoSQLSyncContext &syncContext = execution.getContext().getSyncContext();

	PartitionId partitionId = 0;
	for (;;) {
		partitionId = indexStats.getMissedKeys(
				alloc, requester, partitionId, keyList);
		if (keyList.empty()) {
			break;
		}

		const PartitionId curPartitionId = keyList.front().partitionId_;
		NoSQLContainer targetContainers(
				ec, curPartitionId, syncContext, &execution);
		try {
			targetContainers.estimateIndexSearchSize(
					keyList, option, estimationList);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_OR_SYSTEM(
					e, GS_EXCEPTION_MERGE_MESSAGE(
							e, "Failed to estimate index search size"));
		}

		applyIndexSearchSize(keyList, estimationList, indexStats, requester);
	}
}

void NoSQLStore::applyIndexSearchSize(
		const SQLIndexStatsCache::KeyList &keyList,
		const util::Vector<int64_t> &estimationList,
		SQLIndexStatsCache &indexStats, uint64_t requester) {
	assert(keyList.size() == estimationList.size());

	typedef SQLIndexStatsCache::KeyList::const_iterator KeyIt;
	typedef util::Vector<int64_t>::const_iterator EstimationIt;

	KeyIt keyIt = keyList.begin();
	EstimationIt estimationIt = estimationList.begin();

	while (
			keyIt != keyList.end() &&
			estimationIt != estimationList.end()) {
		SQLIndexStatsCache::Value statsValue;
		statsValue.approxSize_ = *estimationIt;

		indexStats.put(*keyIt, requester, statsValue);

		++keyIt;
		++estimationIt;
	}
}

/*!
	@brief beginコマンドを実行する
*/
void NoSQLStore::begin() {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_FAILED,
		getCommandString(DDL_BEGIN) << " not supported");
}

/*!
	@brief commitコマンドをを実行する
*/
void NoSQLStore::commit() {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_FAILED,
		getCommandString(DDL_COMMIT) << " not supported");
}

/*!
	@brief rollbackコマンドをを実行する
*/
void NoSQLStore::rollback() {
	GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_UNSUPPORTED_COMMAND_TYPE,
		getCommandString(DDL_ROLLBACK) << " not supported");
}

void NoSQLStore::getContainer(
	NoSQLContainer& container,
	bool isSystemDb,
	NoSQLStoreOption& option) {

	if (isSystemDb) {
		option.isSystemDb_ = true;
	}
	container.getContainerInfo(option);
}

void NoSQLStore::putContainer(
	TableExpirationSchemaInfo& info,
	RenameColumnSchemaInfo renameColInfo,
	util::XArray<uint8_t>& binarySchemaInfo,
	int32_t containerPos,
	ContainerType containerType,
	ContainerAttribute containerAttr, bool modifiable,
	NoSQLContainer& container
	, util::XArray<uint8_t>* containerData
	, util::XArray<uint8_t>* binayData
	, NoSQLStoreOption& targetOption) {
	NoSQLStoreOption option;
	option.containerType_ = containerType;
	option.subContainerId_ = containerPos;
	option.containerAttr_ = containerAttr;
	option.caseSensitivity_ = targetOption.caseSensitivity_;
	if (containerAttr == CONTAINER_ATTR_LARGE) {
		container.putLargeContainer(binarySchemaInfo, false, option, containerData, binayData);
	}
	else {
		container.putContainer(info, renameColInfo, binarySchemaInfo, modifiable, option);
	}
}

/*!
	@brief ユーザ生成
*/
void DBConnection::createUser(
	EventContext& ec, SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& password,
	bool isRole) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->createUser(ec, execution, userName, password, isRole);
}

/*!
	@brief ユーザ削除
*/
void DBConnection::dropUser(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->dropUser(ec, execution, userName);
}

/*!
	@brief パスワード設定
*/
void DBConnection::setPassword(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& password) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->setPassword(ec, execution, userName, password);
}

/*!
	@brief 権限付与
*/
void DBConnection::grant(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& dbName,
	AccessControlType type) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->grant(ec, execution, userName, dbName, type);
}

/*!
	@brief 権限はく奪
*/
void DBConnection::revoke(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& userName,
	const NameWithCaseSensitivity& dbName,
	AccessControlType type) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->revoke(ec, execution, userName, dbName, type);
}

/*!
	@brief DB生成
*/
void DBConnection::createDatabase(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->createDatabase(ec, execution, dbName);
}

/*!
	@brief DB削除
*/
void DBConnection::dropDatabase(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->dropDatabase(ec, execution, dbName);
}
/*!
	@brief テーブル生成
*/
void DBConnection::createTable(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->createTable(ec,
		execution, dbName, tableName, createTableOption, baseInfo);
}

void DBConnection::createView(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->createView(ec,
		execution, dbName, tableName, createTableOption, baseInfo);
}

/*!
	@brief テーブル削除
*/
void DBConnection::dropTable(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	bool ifExists, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->dropTable(ec,
		execution, dbName, tableName, ifExists, baseInfo);
}

void DBConnection::dropView(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		bool ifExists, DDLBaseInfo* baseInfo) {
	UNUSED_VARIABLE(baseInfo);

	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());
	store->dropView(ec, execution, dbName, tableName, ifExists);
	store->removeCache(tableName.name_);
	SQLExecution::updateRemoteCache(ec,
		getJobManager()->getEE(SQL_SERVICE),
		pt_,
		execution->getContext().getDBId(), execution->getContext().getDBName(),
		tableName.name_,
		MAX_TABLE_PARTITIONING_VERSIONID);
}

void DBConnection::dropTablePost(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {
	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	store->dropTablePost(ec,
		execution, dbName, tableName, baseInfo);
	store->removeCache(tableName.name_);
	SQLExecution::SQLExecutionContext& sqlContext = execution->getContext();
	SQLExecution::updateRemoteCache(ec,
		getJobManager()->getEE(SQL_SERVICE),
		pt_,
		sqlContext.getDBId(), sqlContext.getDBName(), tableName.name_,
		MAX_TABLE_PARTITIONING_VERSIONID);
}

void DBConnection::getContainer(
	SQLExecution* execution,
	NoSQLContainer& container) {

	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());
	NoSQLStoreOption option(execution);
	store->getContainer(container, false, option);
}

/*!
	@brief 索引生成
*/
void DBConnection::createIndex(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	const CreateIndexOption& option, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->createIndex(ec,
		execution, dbName, indexName, tableName, option, baseInfo);
}

void DBConnection::createIndexPost(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {

	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());
	try {
		store->createIndexPost(ec,
			execution, dbName, indexName, tableName, baseInfo);
		TableLatch latch(ec, this, execution, dbName, tableName, false);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


/*!
	@brief 索引削除
*/
void DBConnection::dropIndex(EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	bool ifExists, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->dropIndex(ec,
		execution, dbName, indexName, tableName, ifExists, baseInfo);
}

void DBConnection::dropIndexPost(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& indexName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {

	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());
	try {
		store->dropIndexPost(ec,
			execution, dbName, indexName, tableName, baseInfo);
		TableLatch latch(ec, this, execution, dbName, tableName, false);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief トランザクション開始
*/
void DBConnection::begin(SQLExecution* execution) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->begin();
}

/*!
	@brief トランザクションコミット
*/
void DBConnection::commit(SQLExecution* execution) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->commit();
}

/*!
	@brief トランザクションアボート
*/
void DBConnection::rollback(SQLExecution* execution) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->rollback();
}

void NoSQLStore::removeCache(const char* tableName) {
	try {
		SQLString key(globalVarAlloc_);
		NoSQLUtils::normalizeString(key, tableName);
		removeCacheEntry<SQLString>(key);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


TableLatch::~TableLatch() {
	cache_->release(entry_);
}

void DBConnection::dropTablePartition(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {

	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());
	store->dropTablePartition(ec,
		execution, dbName, tableName, baseInfo);
}

void DBConnection::dropTablePartitionPost(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName) {

	NoSQLStore* store = getNoSQLStore(
		execution->getContext().getDBId(),
		execution->getContext().getDBName());
	store->dropTablePartitionPost(ec,
		execution, dbName, tableName);
	TableLatch latch(ec, this, execution, dbName, tableName, false);
}

bool NoSQLStore::checkContainerExist(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& tableName) {

	NoSQLStoreOption option(execution, NULL, &tableName);
	NoSQLContainer targetContainer(ec, tableName,
		execution->getContext().getSyncContext(), execution);
	getContainer(targetContainer, false, option);
	return targetContainer.isExists();
}

void NoSQLStore::dropTablePartition(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	DDLBaseInfo* baseInfo) {

	util::StackAllocator& alloc = ec.getAllocator();
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;

	DDLCommandType commandType = DDL_DROP_PARTITION;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	DropTablePartitionInfo* dropPartitionTableInfo
		= static_cast<DropTablePartitionInfo*>(baseInfo);
	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);

		if (dropPartitionTableInfo->cmdOptionList_ != NULL
			&& dropPartitionTableInfo->cmdOptionList_->size() == 1) {
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
				"Target partition constraints is not specifed");
		}

		NoSQLContainer targetContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_ << "' is not exists");
		}
		if (!NoSQLCommonUtils::isWritableContainer(targetContainer.getContainerAttribute(),
			targetContainer.getContainerType())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is read-only");
		}
		if (targetContainer.isViewContainer()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}
		if (!targetContainer.isLargeContainer()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARTITIONING_TABLE_TYPE,
				"Target table='" << tableName.name_ << "' must be partitioning table");
		}
		int64_t targetValue;
		const SyntaxTree::Expr& expr = *(*dropPartitionTableInfo->cmdOptionList_)[0];
		if (expr.value_.getType() == TupleList::TYPE_STRING) {
			util::String str(static_cast<const char*>(expr.value_.varData()),
				expr.value_.varSize(), alloc);
			targetValue = convertToTime(str);
		}
		else if (expr.value_.getType() == TupleList::TYPE_LONG) {
			targetValue = expr.value_.get<int64_t>();
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
				"Invalid drop partition constraint");
		}

		util::XArray<ColumnInfo> largeColumnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		getLargeContainerInfo<TablePartitioningInfo<util::StackAllocator>>(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
			*dsConfig, targetContainer, largeColumnInfoList, partitioningInfo);
		partitioningInfo.init();

		TupleList::TupleColumnType partitioningColumnType = partitioningInfo.partitionColumnType_;
		const bool timestampFamily =
				TupleColumnTypeUtils::isTimestampFamily(partitioningColumnType);

		if (expr.value_.getType() == TupleList::TYPE_STRING && !timestampFamily) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
				"Invalid data format, specified partitioning column type must be timestamp");
		}

		if (expr.value_.getType() != TupleList::TYPE_STRING && timestampFamily) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
				"Invalid data format, specified partitioning column type must be numeric");
		}

		TupleValue* tupleValue = NULL;
		switch (partitioningColumnType) {
		case TupleList::TYPE_BYTE:
		{
			if (static_cast<int64_t>(targetValue) > INT8_MAX || static_cast<int64_t>(targetValue) < INT8_MIN) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Target value is over byte limit value");
			}
			int8_t value = static_cast<int8_t>(targetValue);
			tupleValue = ALLOC_NEW(alloc) TupleValue(&value, TupleList::TYPE_BYTE);
		}
		break;
		case TupleList::TYPE_SHORT:
		{
			if (static_cast<int64_t>(targetValue) > INT16_MAX || static_cast<int64_t>(targetValue) < INT16_MIN) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Target value is over short limit value");
			}
			int16_t value = static_cast<int16_t>(targetValue);
			tupleValue = ALLOC_NEW(alloc) TupleValue(&value, TupleList::TYPE_SHORT);
		}
		break;
		case TupleList::TYPE_INTEGER:
		{
			if (static_cast<int64_t>(targetValue) > INT32_MAX || static_cast<int64_t>(targetValue) < INT32_MIN) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Target value is over integer limit value");
			}
			int32_t value = static_cast<int32_t>(targetValue);
			tupleValue = ALLOC_NEW(alloc) TupleValue(&value, TupleList::TYPE_INTEGER);
		}
		break;
		case TupleList::TYPE_LONG:
		case TupleList::TYPE_TIMESTAMP:
		{
			int64_t value = static_cast<int64_t>(targetValue);
			tupleValue = ALLOC_NEW(alloc) TupleValue(&value, TupleList::TYPE_LONG);
		}
		break;
		case TupleList::TYPE_MICRO_TIMESTAMP:
			tupleValue = ALLOC_NEW(alloc) TupleValue(
					ValueProcessor::getMicroTimestamp(targetValue));
			break;
		case TupleList::TYPE_NANO_TIMESTAMP:
			tupleValue = ALLOC_NEW(alloc) TupleValue(TupleNanoTimestamp(
					ALLOC_NEW(alloc) NanoTimestamp(
							ValueProcessor::getNanoTimestamp(targetValue))));
			break;
		}

		if (SyntaxTree::isRangePartitioningType(partitioningInfo.partitionType_)) {
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
				"Table partitioning type must be interval or interval-hash");
		}

		baseInfo->largeContainerId_ = targetContainer.getContainerId();
		uint32_t partitionNum = dbConnection_->getPartitionTable()->getPartitionNum();
		NodeAffinityNumber affinity;
		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);
		int64_t baseValue;
		NodeAffinityNumber baseAffinity;
		TargetContainerInfo targetContainerInfo;

		if (partitioningInfo.subPartitioningColumnId_ !=
				static_cast<ColumnId>(-1)) {
			partitioningInfo.getAffinityNumber(partitionNum, tupleValue, NULL, 0, baseValue, baseAffinity);
			size_t removedCount = 0;
			NodeAffinityNumber currentAffinity;
			util::Vector<NodeAffinityNumber> affinityList(alloc);
			for (size_t hashPos = 0; hashPos < partitioningInfo.partitioningNum_; hashPos++) {
				currentAffinity = baseAffinity + hashPos;
				size_t targetAffinityPos = partitioningInfo.findEntry(currentAffinity);
				if (targetAffinityPos != std::numeric_limits<size_t>::max()
					&& targetAffinityPos < partitioningInfo.assignStatusList_.size()) {
					if (partitioningInfo.assignStatusList_[targetAffinityPos] == PARTITION_STATUS_DROP_START) {
						removedCount++;
					}
				}
				affinityList.push_back(currentAffinity);
			}
			if (removedCount == affinityList.size()) {
				baseInfo->isFinished_ = true;
				return;
			}
			LargeExecStatus execStatus(alloc);
			IndexInfo* indexInfo = &execStatus.indexInfo_;
			option.sendBaseValue_ = true;
			option.baseValue_ = baseValue;

			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = true;
			targetContainer.updateLargeContainerStatus(PARTITION_STATUS_DROP_START,
				baseAffinity, retryOption, execStatus, indexInfo);
			dropPartitionTableInfo->affinity_ = baseAffinity;
			baseInfo->updateAckCount(static_cast<int32_t>(affinityList.size()));
			baseInfo->processor_->setAsyncStart();
			for (size_t pos = 0; pos < affinityList.size(); pos++) {
				subOption.subContainerId_ = static_cast<int32_t>(pos);
				NoSQLContainer* currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_, affinityList[pos], execution);
				getContainer(*currentContainer, false, option);
				currentContainer->setContainerType(partitioningInfo.containerType_);
				baseInfo->processor_->setAckStatus(currentContainer, 0, 1);
				currentContainer->dropContainer(subOption);
			}
		}
		else {
			affinity = partitioningInfo.getAffinityNumber(partitionNum, tupleValue, NULL, -1, baseValue, baseAffinity);
			size_t targetAffinityPos = partitioningInfo.findEntry(affinity);
			if (targetAffinityPos != std::numeric_limits<size_t>::max()
				&& partitioningInfo.assignStatusList_[targetAffinityPos] == PARTITION_STATUS_DROP_START) {
				baseInfo->isFinished_ = true;
				return;
			}
			LargeExecStatus execStatus(alloc);
			IndexInfo* indexInfo = &execStatus.indexInfo_;
			option.sendBaseValue_ = true;
			option.baseValue_ = baseValue;

			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = true;
			targetContainer.updateLargeContainerStatus(PARTITION_STATUS_DROP_START,
				affinity, retryOption, execStatus, indexInfo);

			dropPartitionTableInfo->affinity_ = affinity;
			baseInfo->processor_->setAsyncStart();
			baseInfo->updateAckCount(1);
			subOption.subContainerId_ = 0;
			NoSQLContainer* currentContainer = createNoSQLContainer(ec,
				tableName, partitioningInfo.largeContainerId_, affinity, execution);
			getContainer(*currentContainer, false, option);
			currentContainer->setContainerType(partitioningInfo.containerType_);
			baseInfo->processor_->setAckStatus(currentContainer, 0, 1);
			currentContainer->dropContainer(subOption);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropTablePartitionPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName) {
	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(execution);
	UNUSED_VARIABLE(dbName);
	UNUSED_VARIABLE(tableName);

	return;
}
void NoSQLStoreOption::setAsyncOption(
	DDLBaseInfo* baseInfo, StatementMessage::CaseSensitivity caseSensitive) {
	caseSensitivity_ = caseSensitive;
	isSync_ = false;
	execId_ = baseInfo->execId_;
	jobVersionId_ = baseInfo->jobVersionId_;
};

void NoSQLStore::getLargeBinaryRecord(util::StackAllocator& alloc,
	const char* key, NoSQLContainer& container, util::XArray<ColumnInfo>& columnInfoList,
	util::XArray<uint8_t>& subContainerSchema, NoSQLStoreOption& option) {
	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	const DataStoreConfig* dsConfig = jobManager_->getExecutionManager()->getManagerSet()->dsConfig_;
	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='" << key << "'";
	container.executeSyncQuery(queryStr.str().c_str(), option,
		fixedPart, varPart, rowCount, hasRowKey);

	InputMessageRowStore rowStore(*dsConfig,
		columnInfoList.data(), static_cast<uint32_t>(columnInfoList.size()),
		fixedPart.data(), static_cast<uint32_t>(fixedPart.size()), varPart.data(),
		static_cast<uint32_t>(varPart.size()), rowCount, true, false);

	VersionInfo versionInfo;
	decodeBinaryRow<util::XArray<uint8_t> >(rowStore, subContainerSchema, versionInfo, key);
}

void NoSQLStore::addColumn(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	DDLCommandType commandType = DDL_ADD_COLUMN;
	NoSQLStoreOption option(execution);
	util::StackAllocator& alloc = ec.getAllocator();
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	AddColumnInfo* addColumnInfo = static_cast<AddColumnInfo*>(baseInfo);
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;

	try {
		const char8_t* tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		if (addColumnInfo->createTableOpt_->columnInfoList_ == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
		}

		NoSQLContainer targetContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		getContainer(targetContainer, false, option);
		if (!targetContainer.isExists()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
				"Specified table '" << tableName.name_ << "' is not exists");
		}
		if (!NoSQLCommonUtils::isWritableContainer(targetContainer.getContainerAttribute(),
			targetContainer.getContainerType())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is read-only");
		}
		if (targetContainer.isViewContainer()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << tableName.name_ << "' is VIEW");
		}
		bool isPartitioning = (targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE);
		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);

		util::XArray<uint8_t> orgContainerSchema(alloc);
		util::XArray<uint8_t> newContainerSchema(alloc);
		util::XArray<uint8_t> optionList(alloc);
		util::XArray<uint8_t> fixedPart(alloc);
		util::XArray<uint8_t> varPart(alloc);

		util::XArray<ColumnInfo> orgColumnInfoList(alloc);
		util::XArrayOutStream<> arrayOut(newContainerSchema);
		util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
		ContainerType containerType;
		bool isRecover = false;
		util::Vector<NoSQLContainer*> containerList(alloc);

		if (isPartitioning) {
			NoSQLUtils::makeLargeContainerColumn(orgColumnInfoList);
			getLargeRecord<TablePartitioningIndexInfo>(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
				targetContainer, orgColumnInfoList, tablePartitioningIndexInfo, option);
			getLargeBinaryRecord(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
				targetContainer, orgColumnInfoList, orgContainerSchema, option);
			getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
				alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				targetContainer, orgColumnInfoList, partitioningInfo, option);

			containerType = partitioningInfo.containerType_;
			bool isSuccess = false;
			bool isError = false;
			for (NodeAffinityNumber subContainerId = 0;
					subContainerId < static_cast<NodeAffinityNumber>(
							partitioningInfo.getCurrentPartitioningCount());
					subContainerId++) {
				if (partitioningInfo.assignStatusList_[subContainerId] == PARTITION_STATUS_DROP_START) {
					continue;
				}
				NoSQLContainer* currentContainer =
					createNoSQLContainer(ec,
						tableName, partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId], execution);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(info, subContainerId);
				RenameColumnSchemaInfo renameColInfo;
				try {
					putContainer(
							info, renameColInfo, orgContainerSchema,
							static_cast<int32_t>(subContainerId),
							containerType, CONTAINER_ATTR_SUB,
							true, *currentContainer, NULL, NULL, option);
					if (currentContainer != NULL) {
						containerList.push_back(currentContainer);
					}
					isSuccess = true;
				}
				catch (std::exception& e) {
					isError = true;
					UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
				}
			}
			if (isSuccess && isError) {
				isRecover = true;
			}
		}
		else {
			targetContainer.getContainerBinary(option, orgContainerSchema);
			containerType = targetContainer.getContainerType();
		}

		if (isRecover) {
			util::NormalOStringStream oss;
			oss << "Recover container schema=";
			for (size_t i = 0; i < containerList.size(); i++) {
				const FullContainerKey* containerKey = containerList[i]->getContainerKey();
				util::String containerName(alloc);
				containerKey->toString(alloc, containerName);
				oss << containerName;
				if (i != containerList.size() - 1) {
					oss << ",";
				}
			}
			GS_TRACE_WARNING(SQL_SERVICE, GS_TRACE_SQL_RECOVER_CONTAINER, oss.str().c_str());
			return;
		}

		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(orgContainerSchema.data(), orgContainerSchema.size()));
		int32_t columnNum = 0;

		ContainerAttribute containerAttribute = CONTAINER_ATTR_SINGLE;
		if (!isPartitioning) {
			bool existFlag;
			StatementHandler::decodeBooleanData<util::ArrayByteInStream>(in, existFlag);
			SchemaVersionId versionId;
			in >> versionId;
			ContainerId containerId;
			in >> containerId;
			util::XArray<uint8_t> containerName(alloc);
			StatementHandler::decodeVarSizeBinaryData<EventByteInStream>(in, containerName);
			in >> columnNum;
			containerAttribute = CONTAINER_ATTR_SINGLE;
		}
		else {
			in >> columnNum;
			containerAttribute = CONTAINER_ATTR_SUB;

		}
		int32_t newColumnNum = static_cast<int32_t>(
				columnNum + (*createTableOption.columnInfoList_).size());
		out << newColumnNum;
		for (int32_t i = 0; i < columnNum; i++) {
			int32_t columnNameLen;
			in >> columnNameLen;
			out << columnNameLen;
			util::XArray<uint8_t> columnName(alloc);
			columnName.resize(static_cast<size_t>(columnNameLen));
			in >> std::make_pair(columnName.data(), columnNameLen);
			out << std::make_pair(columnName.data(), columnNameLen);
			int8_t typeOrdinal;
			in >> typeOrdinal;
			out << typeOrdinal;
			uint8_t flags;
			in >> flags;
			out << flags;
			optionList.push_back(flags);
		}
		for (size_t i = 0; i < (*createTableOption.columnInfoList_).size(); i++) {
			char* columnName = const_cast<char*>(
				(*createTableOption.columnInfoList_)[i]->columnName_->name_->c_str());
			int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
			out << columnNameLen;
			out << std::make_pair(columnName, columnNameLen);
			const ColumnType columnType = convertTupleTypeToNoSQLType(
					(*createTableOption.columnInfoList_)[i]->type_);
			const int8_t typeOrdinal =
					ValueProcessor::getPrimitiveColumnTypeOrdinal(
							columnType, false);
			out << typeOrdinal;
			const uint8_t flags = MessageSchema::makeColumnFlags(
					ValueProcessor::isArray(columnType), false,
					(*createTableOption.columnInfoList_)[i]->hasNotNullConstraint());
			out << flags;
			optionList.push_back(flags);
		}
		int16_t rowKeyNum = 0;
		in >> rowKeyNum;
		out << rowKeyNum;
		int16_t keyColumnId = 0;
		for (int16_t pos = 0; pos < rowKeyNum; pos++) {
			in >> keyColumnId;
			out << keyColumnId;
		}
		int32_t affinityStrLen = 0;
		in >> affinityStrLen;
		out << affinityStrLen;
		if (affinityStrLen > 0) {
			util::XArray<uint8_t> affinityStr(alloc);
			affinityStr.resize(static_cast<size_t>(affinityStrLen));
			in >> std::make_pair(affinityStr.data(), affinityStrLen);
			out << std::make_pair(affinityStr.data(), affinityStrLen);
		}

		if (in.base().remaining() > 0 && containerType == TIME_SERIES_CONTAINER) {
			int8_t tsOption = 0;
			in >> tsOption;
			out << tsOption;
			if (tsOption != 0) {
				int32_t elapsedTime;
				in >> elapsedTime;
				out << elapsedTime;
				int8_t unit;
				in >> unit;
				out << unit;
				int32_t division;
				in >> division;
				out << division;
				int32_t timeDuration;
				in >> timeDuration;
				out << timeDuration;
				in >> unit;
				out << unit;
				int8_t compressionType;
				in >> compressionType;
				out << compressionType;
				uint32_t compressionInfoNum;
				in >> compressionInfoNum;
				out << compressionInfoNum;
			}
		}

		out << static_cast<int32_t>(containerAttribute);
		out << partitioningInfo.partitioningVersionId_;

		if (isPartitioning) {
			NoSQLUtils::checkSchemaValidation(alloc, *dsConfig,
				tableName.name_, newContainerSchema,
				containerType);

			int32_t errorPos = -1;
			util::Exception pendingException;
			for (NodeAffinityNumber subContainerId = 0;
					subContainerId < static_cast<NodeAffinityNumber>(
							partitioningInfo.getCurrentPartitioningCount());
					subContainerId++) {
				if (partitioningInfo.assignStatusList_[subContainerId] == PARTITION_STATUS_DROP_START) {
					continue;
				}
				NoSQLContainer* currentContainer = createNoSQLContainer(ec,
					tableName, partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[subContainerId], execution);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(info, subContainerId);
				RenameColumnSchemaInfo renameColInfo;
				try {
					putContainer(
							info, renameColInfo, newContainerSchema,
							static_cast<int32_t>(subContainerId),
							containerType, containerAttribute,
							true, *currentContainer, NULL, NULL, option);
				}
				catch (std::exception& e) {
					UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
					errorPos = static_cast<int32_t>(subContainerId);
					pendingException = GS_EXCEPTION_CONVERT(e, "");
				}

			}
			if (errorPos != -1) {
				NoSQLContainer* currentContainer =
					createNoSQLContainer(ec,
						tableName, partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[errorPos], execution);
				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(info, errorPos);
				RenameColumnSchemaInfo renameColInfo;
				putContainer(info, renameColInfo, newContainerSchema, errorPos,
					containerType, containerAttribute,
					true, *currentContainer, NULL, NULL, option);
			}
			{
				util::XArray<ColumnInfo> newColumnInfoList(alloc);
				NoSQLUtils::makeLargeContainerColumn(newColumnInfoList);
				util::XArray<uint8_t> fixedPart(alloc);
				util::XArray<uint8_t> varPart(alloc);
				OutputMessageRowStore outputMrs(*dsConfig,
					&newColumnInfoList[0], static_cast<uint32_t>(newColumnInfoList.size()),
					fixedPart, varPart, false);
				NoSQLUtils::makeLargeContainerRowBinary(alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA, outputMrs, newContainerSchema);
				NoSQLStoreOption option(execution);
				option.putRowOption_ = PUT_INSERT_OR_UPDATE;
				targetContainer.putRow(fixedPart, varPart, UNDEF_ROWID, option);
			}
			if (errorPos != -1) {
				throw pendingException;
			}
		}
		else {
			NoSQLContainer* currentContainer = ALLOC_NEW(alloc) NoSQLContainer(ec,
				tableName, execution->getContext().getSyncContext(), execution);
			TableExpirationSchemaInfo info;
			RenameColumnSchemaInfo renameColInfo;
			putContainer(info, renameColInfo, newContainerSchema, 0,
				containerType, CONTAINER_ATTR_SINGLE,
				true, *currentContainer, NULL, NULL, option);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


void NoSQLStore::checkPre(DDLSource& ddlSource) {
	const NameWithCaseSensitivity& dbName = ddlSource.dbName_;
	const NameWithCaseSensitivity& tableName = ddlSource.tableName_;
	util::StackAllocator& alloc = ddlSource.alloc_;
	SQLExecution* execution = ddlSource.execution_;
	DDLBaseInfo* baseInfo = ddlSource.baseInfo_;
	DDLCommandType commandType = ddlSource.commandType_;

	const char8_t* tmpDbName = dbName.name_;
	adjustConnectedDb(tmpDbName);
	NameWithCaseSensitivity connectedDbName(tmpDbName, dbName.isCaseSensitive_);
	ExecuteCondition condition(NULL, &connectedDbName, &tableName, NULL);
	checkExecutable(alloc, commandType, execution, false, condition);

	if (commandType == DDL_RENAME_COLUMN) {
		RenameColumnInfo* renameColumnInfo = static_cast<RenameColumnInfo*>(baseInfo);
		if (renameColumnInfo->createTableOpt_->columnInfoList_ == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
		}
	}
	else { 
		AddColumnInfo* addColumnInfo = static_cast<AddColumnInfo*>(baseInfo);
		if (addColumnInfo->createTableOpt_->columnInfoList_ == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
		}
	}
}

void NoSQLStore::checkContainer(DDLSource& ddlSource, NoSQLContainer& targetContainer) {
	const NameWithCaseSensitivity& tableName = ddlSource.tableName_;
	NoSQLStoreOption& option = ddlSource.option_;

	getContainer(targetContainer, false, option);
	if (!targetContainer.isExists()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
			"Specified table '" << tableName.name_ << "' is not exists");
	}
	if (!NoSQLCommonUtils::isWritableContainer(targetContainer.getContainerAttribute(),
		targetContainer.getContainerType())) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
			"Target table '" << tableName.name_ << "' is read-only");
	}
	if (targetContainer.isViewContainer()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
			"Target table '" << tableName.name_ << "' is VIEW");
	}
	ddlSource.isPartitioning_ = (targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE);
}

void NoSQLStore::setDDL(DDLSource& ddlSource, NoSQLContainer& targetContainer) {
	bool isPartitioning = ddlSource.isPartitioning_;
	util::StackAllocator& alloc = ddlSource.alloc_;
	TablePartitioningIndexInfo& tablePartitioningIndexInfo = ddlSource.tablePartitioningIndexInfo_;
	NoSQLStoreOption& option = ddlSource.option_;
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo = ddlSource.partitioningInfo_;
	util::XArray<uint8_t>& orgContainerSchema = ddlSource.orgContainerSchema_;

	util::XArray<ColumnInfo> orgColumnInfoList(alloc);

	if (isPartitioning) {
		NoSQLUtils::makeLargeContainerColumn(orgColumnInfoList);
		getLargeRecord<TablePartitioningIndexInfo>(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
			targetContainer, orgColumnInfoList, tablePartitioningIndexInfo, option);
		getLargeBinaryRecord(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
			targetContainer, orgColumnInfoList, orgContainerSchema, option);
		getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
			alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
			targetContainer, orgColumnInfoList, partitioningInfo, option);
		ddlSource.containerType_ = partitioningInfo.containerType_;
	}
	else {
		targetContainer.getContainerBinary(option, orgContainerSchema);
		ddlSource.containerType_ = targetContainer.getContainerType();
	}
}

void NoSQLStore::makeSchema(DDLSource& ddlSource, RenameColumnContext* rcCxt) {
	util::StackAllocator& alloc = ddlSource.alloc_;
	util::XArray<uint8_t>& orgContainerSchema = ddlSource.orgContainerSchema_;
	util::XArray<uint8_t>& newContainerSchema = ddlSource.newContainerSchema_;
	bool isPartitioning = ddlSource.isPartitioning_;
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo = ddlSource.partitioningInfo_;
	ContainerType containerType = ddlSource.containerType_;

	util::ArrayByteInStream in = util::ArrayByteInStream(
		util::ArrayInStream(orgContainerSchema.data(), orgContainerSchema.size()));
	int32_t columnNum = 0;
	ContainerAttribute containerAttribute = CONTAINER_ATTR_SINGLE;
	if (!isPartitioning) {
		bool existFlag;
		StatementHandler::decodeBooleanData<util::ArrayByteInStream>(in, existFlag);
		SchemaVersionId versionId;
		in >> versionId;
		ContainerId containerId;
		in >> containerId;
		util::XArray<uint8_t> containerName(alloc);
		StatementHandler::decodeVarSizeBinaryData<EventByteInStream>(in, containerName);
		in >> columnNum;
		containerAttribute = CONTAINER_ATTR_SINGLE;
	}
	else {
		in >> columnNum;
		containerAttribute = CONTAINER_ATTR_SUB;

	}
	ddlSource.containerAttribute_ = containerAttribute;

	util::XArrayOutStream<> arrayOut(newContainerSchema);
	util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
	out << columnNum;

	for (int32_t i = 0; i < columnNum; i++) {
		int32_t columnNameLen;
		in >> columnNameLen;
		if (rcCxt) {
			if (i == rcCxt->oldColumnNameId_) {
				out << rcCxt->newColumnNameLen_;
			}
			else {
				out << columnNameLen;
			}
		}
		else {
			out << columnNameLen;
		}
		util::XArray<uint8_t> columnName(alloc);
		columnName.resize(static_cast<size_t>(columnNameLen));
		in >> std::make_pair(columnName.data(), columnNameLen);
		if (rcCxt) {
			if (i == rcCxt->oldColumnNameId_) {
				out << std::make_pair(rcCxt->newColumnName_, rcCxt->newColumnNameLen_);
			}
			else {
				out << std::make_pair(columnName.data(), columnNameLen);
			}
		}
		else {
			out << std::make_pair(columnName.data(), columnNameLen);
		}
		int8_t typeOrdinal;
		in >> typeOrdinal;
		out << typeOrdinal;
		uint8_t flags;
		in >> flags;
		out << flags;
	}

	int16_t rowKeyNum = 0;
	in >> rowKeyNum;
	out << rowKeyNum;
	int16_t keyColumnId = 0;
	for (int16_t pos = 0; pos < rowKeyNum; pos++) {
		in >> keyColumnId;
		out << keyColumnId;
	}
	int32_t affinityStrLen = 0;
	in >> affinityStrLen;
	out << affinityStrLen;
	if (affinityStrLen > 0) {
		util::XArray<uint8_t> affinityStr(alloc);
		affinityStr.resize(static_cast<size_t>(affinityStrLen));
		in >> std::make_pair(affinityStr.data(), affinityStrLen);
		out << std::make_pair(affinityStr.data(), affinityStrLen);
	}

	if (in.base().remaining() > 0 && containerType == TIME_SERIES_CONTAINER) {
		int8_t tsOption = 0;
		in >> tsOption;
		out << tsOption;
		if (tsOption != 0) {
			int32_t elapsedTime;
			in >> elapsedTime;
			out << elapsedTime;
			int8_t unit;
			in >> unit;
			out << unit;
			int32_t division;
			in >> division;
			out << division;
			int32_t timeDuration;
			in >> timeDuration;
			out << timeDuration;
			in >> unit;
			out << unit;
			int8_t compressionType;
			in >> compressionType;
			out << compressionType;
			uint32_t compressionInfoNum;
			in >> compressionInfoNum;
			out << compressionInfoNum;
		}
	}

	out << static_cast<int32_t>(containerAttribute);
	out << partitioningInfo.partitioningVersionId_;
}

void NoSQLStore::execNoSQL(DDLSource& ddlSource, NoSQLContainer& targetContainer, bool isRenameColumn) {

	bool isPartitioning = ddlSource.isPartitioning_;
	util::StackAllocator& alloc = ddlSource.alloc_;
	const NameWithCaseSensitivity& tableName = ddlSource.tableName_;
	util::XArray<uint8_t>& newContainerSchema = ddlSource.newContainerSchema_;
	ContainerType containerType = ddlSource.containerType_;
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo = ddlSource.partitioningInfo_;
	EventContext& ec = ddlSource.ec_;
	ContainerAttribute containerAttribute = ddlSource.containerAttribute_;
	SQLExecution* execution = ddlSource.execution_;
	NoSQLStoreOption& option = ddlSource.option_;
	const DataStoreConfig* dsConfig = execution->getExecutionManager()->getManagerSet()->dsConfig_;
	if (isPartitioning) {

		NoSQLUtils::checkSchemaValidation(alloc, *dsConfig,
			tableName.name_, newContainerSchema,
			containerType);

		int32_t errorPos = -1;
		for (NodeAffinityNumber subContainerId = 0;
				subContainerId < static_cast<NodeAffinityNumber>(
						partitioningInfo.getCurrentPartitioningCount());
				subContainerId++) {
			if (partitioningInfo.assignStatusList_[subContainerId] == PARTITION_STATUS_DROP_START) {
				continue;
			}
			NoSQLContainer* currentContainer = createNoSQLContainer(ec,
				tableName, partitioningInfo.largeContainerId_,
				partitioningInfo.assignNumberList_[subContainerId], execution);

			TableExpirationSchemaInfo info;
			partitioningInfo.checkTableExpirationSchema(info, subContainerId);
			RenameColumnSchemaInfo renameColInfo;
			renameColInfo.isRenameColumn_ = isRenameColumn;
			try {
				putContainer(
						info, renameColInfo, newContainerSchema,
						static_cast<int32_t>(subContainerId),
						containerType, containerAttribute,
						true, *currentContainer, NULL, NULL, option);
			}
			catch (std::exception& e) {
				UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
				errorPos = static_cast<int32_t>(subContainerId);
			}
		}
		if (errorPos != -1) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_FAILED, "NoSQL operation failed");
		}
		{
			util::XArray<ColumnInfo> newColumnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(newColumnInfoList);
			util::XArray<uint8_t> fixedPart(alloc);
			util::XArray<uint8_t> varPart(alloc);
			OutputMessageRowStore outputMrs(*dsConfig,
				&newColumnInfoList[0], static_cast<uint32_t>(newColumnInfoList.size()),
				fixedPart, varPart, false);
			NoSQLUtils::makeLargeContainerRowBinary(alloc,
				NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA, outputMrs, newContainerSchema);
			uint64_t numRow = 1;
			NoSQLStoreOption option(execution);
			option.putRowOption_ = PUT_INSERT_OR_UPDATE;
			if (ddlSource.isRenamed()) {
				NoSQLUtils::makeLargeContainerRow(alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					outputMrs, partitioningInfo);
				numRow++;
			}
			targetContainer.putRowSet(fixedPart, varPart, numRow, option);
		}
	}
	else {
		NoSQLContainer* currentContainer = ALLOC_NEW(alloc) NoSQLContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		TableExpirationSchemaInfo info;
		RenameColumnSchemaInfo renameColInfo;
		renameColInfo.isRenameColumn_ = isRenameColumn;
		putContainer(info, renameColInfo, newContainerSchema, 0,
			containerType, CONTAINER_ATTR_SINGLE,
			true, *currentContainer, NULL, NULL, option);
	}
}

NoSQLStore::RenameColumnContext::RenameColumnContext(const CreateTableOption& createTableOption) :
	oldColumnName_(NULL), newColumnName_(NULL),
	oldColumnNameId_(-1), newColumnNameId_(-1) {

	if ((*createTableOption.columnInfoList_).size() != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INTERNAL, "");
	}
	oldColumnName_ = const_cast<char*>(
		(*createTableOption.columnInfoList_)[0]
		->columnName_->name_->c_str());
	newColumnName_ = const_cast<char*>(
		(*createTableOption.columnInfoList_)[1]
		->columnName_->name_->c_str());
	oldColumnNameLen_ = static_cast<int32_t>(strlen(oldColumnName_));
	if (oldColumnNameLen_ > MAX_COLUMN_NAME_LEN) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_COLUMN, "The length of old column name exceeds the upper limit.");
	}
	newColumnNameLen_ = static_cast<int32_t>(strlen(newColumnName_));
	if (newColumnNameLen_ > MAX_COLUMN_NAME_LEN) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_COLUMN, "The length of new column name exceeds the upper limit.");
	}
	if (strcmp(oldColumnName_, newColumnName_) == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_COLUMN, "New column name is the same as old column name");
	}

}

void NoSQLStore::RenameColumnContext::checkColumnName(DDLSource& ddlSource) {

	util::XArray<uint8_t>& orgContainerSchema = ddlSource.orgContainerSchema_;
	bool isPartitioning = ddlSource.isPartitioning_;
	util::StackAllocator& alloc = ddlSource.alloc_;
	TablePartitioningInfo<util::StackAllocator>& partitioningInfo = ddlSource.partitioningInfo_;

	{
		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(orgContainerSchema.data(), orgContainerSchema.size()));
		int32_t columnNum = 0;

		if (!isPartitioning) {
			bool existFlag;
			StatementHandler::decodeBooleanData<util::ArrayByteInStream>(in, existFlag);
			SchemaVersionId versionId;
			in >> versionId;
			ContainerId containerId;
			in >> containerId;
			util::XArray<uint8_t> containerName(alloc);
			StatementHandler::decodeVarSizeBinaryData<EventByteInStream>(in, containerName);
			in >> columnNum;
		}
		else {
			in >> columnNum;
		}
		ddlSource.columnNum_ = columnNum;

		util::XArray<char8_t> buffer1(alloc);
		buffer1.resize(oldColumnNameLen_ + 1);
		ValueProcessor::convertUpperCase(
			oldColumnName_, oldColumnNameLen_ + 1, buffer1.data());
		util::String oldCaseColumnName(buffer1.data(), alloc);

		util::XArray<char8_t> buffer2(alloc);
		buffer2.resize(newColumnNameLen_ + 1);
		ValueProcessor::convertUpperCase(
			newColumnName_, newColumnNameLen_ + 1, buffer2.data());
		util::String newCaseColumnName(buffer2.data(), alloc);

		for (int32_t i = 0; i < columnNum; i++) {

			int32_t columnNameLen;
			in >> columnNameLen;
			util::XArray<char8_t> columnName(alloc);
			columnName.resize(static_cast<size_t>(columnNameLen));
			in >> std::make_pair(columnName.data(), columnNameLen);

			util::XArray<char8_t> buffer(alloc);
			buffer.resize(columnNameLen);
			ValueProcessor::convertUpperCase(
				columnName.data(), columnNameLen, buffer.data());
			buffer.push_back('\0');
			util::String caseColumnName(buffer.data(), alloc);

			if (strcmp(caseColumnName.c_str(), oldCaseColumnName.c_str()) == 0) {
				oldColumnNameId_ = i;
			}
			else if (strcmp(caseColumnName.c_str(), newCaseColumnName.c_str()) == 0) {
				newColumnNameId_ = i;
			}
			int8_t typeOrdinal;
			in >> typeOrdinal;
			uint8_t flags;
			in >> flags;
		}

		if (((oldColumnNameId_ != -1) && (newColumnNameId_ != -1)) ||
			((oldColumnNameId_ == -1) && (newColumnNameId_ == -1))) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_COLUMN, "Invalid column name");
		}
		
		if (isPartitioning) {
			if (partitioningInfo.partitioningColumnId_ ==
					static_cast<ColumnId>(oldColumnNameId_)) {
				partitioningInfo.partitionColumnName_ = newColumnName_;
				ddlSource.setRenamed();
			}
			else if (partitioningInfo.subPartitioningColumnId_ ==
					static_cast<ColumnId>(oldColumnNameId_)) {
				partitioningInfo.subPartitioningColumnName_ = newColumnName_;
				ddlSource.setRenamed();
			}
		}
	}

}

void NoSQLStore::renameColumn(
	EventContext& ec,
	SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {

	DDLCommandType commandType = DDL_RENAME_COLUMN;
	DDLSource ddlSource(ec.getAllocator(), ec, execution, dbName, tableName,
		createTableOption, baseInfo, commandType);

	try {
		checkPre(ddlSource);
		NoSQLContainer targetContainer(ec,
			tableName, execution->getContext().getSyncContext(), execution);
		checkContainer(ddlSource, targetContainer);

		RenameColumnContext rcCxt(createTableOption);

		setDDL(ddlSource, targetContainer);

		rcCxt.checkColumnName(ddlSource);

		makeSchema(ddlSource, &rcCxt);

		execNoSQL(ddlSource, targetContainer, true);

	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
			<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void DBConnection::addColumn(EventContext& ec, SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->addColumn(ec,
		execution, dbName, tableName, createTableOption, baseInfo);
	TableLatch latch(ec, this, execution, dbName, tableName, false);
	SQLExecution::updateRemoteCache(ec,
		getJobManager()->getEE(SQL_SERVICE),
		pt_,
		execution->getContext().getDBId(), execution->getContext().getDBName(),
		tableName.name_,
		MAX_TABLE_PARTITIONING_VERSIONID);
}

void DBConnection::renameColumn(EventContext& ec, SQLExecution* execution,
	const NameWithCaseSensitivity& dbName,
	const NameWithCaseSensitivity& tableName,
	const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo) {
	NoSQLStore* command = getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	command->renameColumn(ec,
		execution, dbName, tableName, createTableOption, baseInfo);
	TableLatch latch(ec, this, execution, dbName, tableName, false);
	SQLExecution::updateRemoteCache(ec,
		getJobManager()->getEE(SQL_SERVICE),
		pt_,
		execution->getContext().getDBId(), execution->getContext().getDBName(),
		tableName.name_,
		MAX_TABLE_PARTITIONING_VERSIONID);
}

int64_t DBConnection::getMonotonicTime() {
	return sqlSvc_->getEE()->getMonotonicTime();
}

SQLExecutionManager* DBConnection::getExecutionManager() {
	return sqlSvc_->getExecutionManager();
}

JobManager* DBConnection::getJobManager() {
	return sqlSvc_->getExecutionManager()->getJobManager();
}
