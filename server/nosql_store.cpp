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
#include "nosql_store.h"
#include "nosql_container.h"
#include "nosql_utils.h"
#include "sql_utils.h"
#include "nosql_db.h"
#include "resource_set.h"
#include "sql_execution.h"
#include "resource_set.h"
#include "data_store.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(SQL_DETAIL);

typedef NoSQLDB::TableLatch TableLatch;

SQLTableCache &NoSQLStore::getCache() {
	return *reinterpret_cast<SQLTableCache *>(cache_);
}

void NoSQLStore::removeCache() {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	if (cache) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, cache);
	}
}

template <class Node, class Key>
Node* NoSQLStore::getCacheEntry(Key &key) {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	return cache->get(key);
}

template SQLTableCacheEntry* NoSQLStore::getCacheEntry(SQLString &key);

template <class Key>
void NoSQLStore::removeCacheEntry(Key &key) {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	cache->remove(key);
}

template
void NoSQLStore::removeCacheEntry(SQLString &key);

void NoSQLStore::dump(util::NormalOStringStream &oss) {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	if (cache) {
		cache->dump(oss, true);
	}	
}

template <class Node, class Key, class Value>
Node* NoSQLStore::putCacheEntry(
		Key &key, Value value, bool withGet) {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	return cache->put(key, value, withGet);
}

template SQLTableCacheEntry* NoSQLStore::putCacheEntry(
		SQLString &key, TableSchemaInfo *value, bool withGet);

template <class Node>
void NoSQLStore::releaseCacheEntry(Node *node) {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	cache->release(node);
}

template void NoSQLStore::releaseCacheEntry(SQLTableCacheEntry *node);

void NoSQLStore::resizeTableCache(int32_t resizeCacheSize) {
	SQLTableCache *cache
			= reinterpret_cast<SQLTableCache *>(cache_);
	if (cache) {
		cache->resize(resizeCacheSize);
	}
}

void NoSQLStore::setDbName(const char *dbName) {
	if (dbName_.empty()) {
		dbName_ = dbName;
		NoSQLUtils::normalizeString(
				normalizedDbName_,  dbName_.c_str());
	}
}

template<typename T>
void NoSQLStore::getLargeRecord(
		util::StackAllocator &alloc,
		const char *key,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		T &record,
		NoSQLStoreOption &option) {

	int64_t rowCount;
	bool hasRowKey = false;
	DataStore *dataStore = resourceSet_->getDataStore();
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute()
			!= CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='" 
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION  
			<< "' OR key='" << key << "'";

	container.executeSyncQuery(
			queryStr.str().c_str(),
			option,
			fixedPart,
			varPart,
			rowCount,
			hasRowKey);

	if (rowCount == 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND, 
			"Tareget table is not found or change to schema, table name="
			<< container.getName());
	}

	InputMessageRowStore rowStore(
			dataStore->getValueLimitConfig(),
			columnInfoList.data(),
			static_cast<uint32_t>(columnInfoList.size()),
			fixedPart.data(),
			static_cast<uint32_t>(fixedPart.size()),
			varPart.data(),
			static_cast<uint32_t>(varPart.size()),
			rowCount,
			true,
			false);

	VersionInfo versionInfo;
	NoSQLUtils::decodeRow<T>(rowStore, record, versionInfo, key);
}

template
void NoSQLStore::getLargeRecord(
		util::StackAllocator &alloc,
		const char *key,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		TablePartitioningIndexInfo &record,
		NoSQLStoreOption &option);

template
void NoSQLStore::getLargeRecord(
		util::StackAllocator &alloc,
		const char *key,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		TablePartitioningInfo<util::StackAllocator> &record,
		NoSQLStoreOption &option);

template
void NoSQLStore::getLargeRecord(
		util::StackAllocator &alloc,
		const char *key, NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		TablePartitioningInfo<SQLVariableSizeGlobalAllocator> &record,
		NoSQLStoreOption &option);

NoSQLStore::NoSQLStore(
		const ResourceSet *resourceSet,
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		size_t cacheSize,
		DatabaseId dbId,
		const char *dbname,
		NoSQLDB *db) :
				resourceSet_(resourceSet),
				globalVarAlloc_(globalVarAlloc),
				dbId_(dbId),
				dbName_(dbname, globalVarAlloc),
				normalizedDbName_(globalVarAlloc),
				cacheSize_(cacheSize),
				cache_(NULL),
				db_(db) {

		NoSQLUtils::normalizeString(
				normalizedDbName_,  dbName_.c_str());
		createCache();
}

NoSQLStore::~NoSQLStore() {
	removeCache();
}

NoSQLDB *NoSQLStore::getDB() {
	return db_;
}

void NoSQLStore::createCache() {
	if (cache_ == NULL) {
		cache_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				SQLTableCache(
						static_cast<int32_t>(cacheSize_), globalVarAlloc_);
	}
}

void NoSQLStore::checkExecutable(
		util::StackAllocator &alloc,
		DDLCommandType commandType,
		SQLExecution *execution,
		bool isAdjustDb,
		ExecuteCondition &condition) {

	const NameWithCaseSensitivity *userName
			= condition.userName_;
	const NameWithCaseSensitivity *tableName
			= condition.tableName_;
	const NameWithCaseSensitivity *dbName
			= condition.dbName_;
	const NameWithCaseSensitivity *indexName
			= condition.indexName_;

	SQLExecution::SQLExecutionContext &sqlContext
			= execution->getContext();

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
				const util::String &normalizedauthUserName =
						SQLUtils::normalizeName(
								alloc, sqlContext.getUserName().c_str(),
								userName->isCaseSensitive_);
				const util::String &normalizedUserName =
						SQLUtils::normalizeName(
								alloc, userName->name_,
								userName->isCaseSensitive_);

				if (normalizedUserName.compare(
						normalizedauthUserName) != 0) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_USER,
							"Normal user may only change one's own password");
				}
			}
		}
	}
 break;
	case DDL_CREATE_DATABASE: {
		if (dbName) {
			const util::String &normalizeDbName
					= SQLUtils::normalizeName(
							alloc, dbName->name_, dbName->isCaseSensitive_);
			checkValidName(normalizeDbName.c_str());
		}
		checkAdministrator(execution);
	}
	break;
	case DDL_DROP_DATABASE: {
		if (dbName) {
			if (!isAdjustDb) {
				const char *currentDBName
						= dbName->isCaseSensitive_ ? dbName_.c_str()
								: normalizedDbName_.c_str();
				NoSQLUtils::checkConnectedDbName(
						alloc, currentDBName,
						dbName->name_, dbName->isCaseSensitive_);
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
	case DDL_CREATE_INDEX:
	case DDL_DROP_INDEX: {

		if (dbName) {
			if (!isAdjustDb) {
				const char *currentDBName
						= dbName->isCaseSensitive_ ? dbName_.c_str()
								: normalizedDbName_.c_str();
				NoSQLUtils::checkConnectedDbName(
						alloc, currentDBName,
						dbName->name_, dbName->isCaseSensitive_);
			}
		}

		if (tableName) {
			const util::String &normalizeTableName
					= SQLUtils::normalizeName(alloc, tableName->name_,
							tableName->isCaseSensitive_);
			checkValidName(normalizeTableName.c_str());
		}

		if (indexName) {
			const util::String &normalizeIndexName
					= SQLUtils::normalizeName(alloc, indexName->name_,
							indexName->isCaseSensitive_);
			checkValidName(normalizeIndexName.c_str());
		}
	}
	break;
	case DDL_BEGIN:
	case DDL_COMMIT:
	case DDL_ROLLBACK:
		break;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_UNSUPPORTED_COMMAND_TYPE,
				"Unsupported DDL command="
				<< static_cast<int32_t>(commandType));
		break;
	 }
}

void NoSQLStore::checkAdministrator(SQLExecution *execution) {

	if (!execution->getContext().isAdministrator()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_USER,
				"Insufficient privilege, only admin");
	}
}

void NoSQLStore::checkValidName(const char *name) {

	if (name == NULL || (name && strlen(name) == 0)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_NAME,
				"Invalid symbol name=" << name);
	}
}

template<typename Alloc>
void NoSQLStore::createSubContainer(
		TableExpirationSchemaInfo &info,
		util::StackAllocator &alloc,
		NoSQLContainer &targetContainer,
		NoSQLContainer *currentContainer,
		util::XArray<uint8_t> &subContainerSchema,
		TablePartitioningInfo<Alloc>  &partitioningInfo,
		TablePartitioningIndexInfo &tablePartitioningIndexInfo,
		const NameWithCaseSensitivity &tableName,
		int32_t partitionNum,
		NodeAffinityNumber &affinity) {

	UNUSED_VARIABLE(alloc);
	UNUSED_VARIABLE(targetContainer);

	NoSQLStoreOption cmdOption;
	cmdOption.ifNotExistsOrIfExists_ = true;
	if (tableName.isCaseSensitive_) {
		cmdOption.caseSensitivity_.setContainerNameCaseSensitive();
	}

	partitioningInfo.checkTableExpirationSchema(
			info, affinity, partitionNum);
	
	putContainer(
			info,
			subContainerSchema,
			0,
			partitioningInfo.containerType_,
			CONTAINER_ATTR_SUB,
			false,
			*currentContainer,
			NULL,
			NULL,
			cmdOption);
	
	currentContainer->updateContainerProperty(
			partitioningInfo.partitioningVersionId_, cmdOption);

	TablePartitioningIndexInfoEntry *entry;
	for (size_t indexPos = 0;
			indexPos < tablePartitioningIndexInfo.indexEntryList_.size();
			indexPos++) {
	
		entry = tablePartitioningIndexInfo.indexEntryList_[indexPos];

		try {
			if (!entry->indexName_.empty()) {
				currentContainer->createIndex(
					entry->indexName_.c_str(),
					entry->indexType_, entry->columnIds_, cmdOption);
			}
		}
		catch (std::exception &) {
		}
	}
}

template
void NoSQLStore::createSubContainer(
		TableExpirationSchemaInfo &info,
		util::StackAllocator &alloc,
		NoSQLContainer &targetContainer,
		NoSQLContainer *currentContainer,
		util::XArray<uint8_t> &subContainerSchema,
		TablePartitioningInfo<util::StackAllocator>  &partitioningInfo,
		TablePartitioningIndexInfo &tablePartitioningIndexInfo,
		const NameWithCaseSensitivity &tableName,
		int32_t partitionNum,
		NodeAffinityNumber &affinity);

template
void NoSQLStore::createSubContainer(
		TableExpirationSchemaInfo &info,
		util::StackAllocator &alloc,
		NoSQLContainer &targetContainer,
		NoSQLContainer *currentContainer,
		util::XArray<uint8_t> &subContainerSchema,
		TablePartitioningInfo<SQLVariableSizeGlobalAllocator>  &partitioningInfo,
		TablePartitioningIndexInfo &tablePartitioningIndexInfo,
		const NameWithCaseSensitivity &tableName,
		int32_t partitionNum,
		NodeAffinityNumber &affinity);

const char *NoSQLStore::getCommandString(DDLCommandType type) {

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
		case DDL_CREATE_VIEW: return "CREATE VIEW";
		case DDL_DROP_VIEW: return "DROP VIEW";
		default : return "UNSUPPORTED";
	}
}

void NoSQLStore::checkWritableContainer(NoSQLContainer &container) {
	if (!NoSQLUtils::isWritableContainer(
			container.getContainerAttribute())) {

		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << container.getName()
				<< "' is read-only");
	}
}

void NoSQLStore::checkViewContainer(NoSQLContainer &container) {
	if (container.isViewContainer()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
				"Target table '" << container.getName() << "' is VIEW");
	}
}

void NoSQLStore::checkExistContainer(NoSQLContainer &container) {
	if (!container.isExists()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
					"Specified table '"
					<< container.getName() << "' is not exists");
	}
}
