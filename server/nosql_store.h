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
#ifndef NOSQL_STORE_H_
#define NOSQL_STORE_H_

#include "lru_cache.h"
#include "sql_parser.h"
#include "data_store_common.h"
#include "schema.h"
#include "transaction_statement_message.h"

typedef int32_t AccessControlType;

struct TableSchemaInfo;
struct DDLBaseInfo;
class NoSQLContainer;
struct TableExpirationSchemaInfo;

template<typename Alloc>
struct TablePartitioningInfo;

struct TablePartitioningIndexInfo;
class NoSQLDB;

typedef CacheNode<SQLString, TableSchemaInfo*,
		SQLVariableSizeGlobalAllocator> SQLTableCacheEntry;

typedef LruCacheCocurrency<SQLTableCacheEntry,
		SQLString, TableSchemaInfo*,
		SQLVariableSizeGlobalAllocator> SQLTableCache;

struct NoSQLStoreOption;
class ResourceSet;

class NoSQLStore {

	enum DDLCommandType {
		DDL_NONE,
		DDL_CREATE_DATABASE,
		DDL_DROP_DATABASE,
		DDL_CREATE_TABLE,
		DDL_DROP_TABLE,
		DDL_CREATE_INDEX,
		DDL_DROP_INDEX,
		DDL_CREATE_USER,
		DDL_DROP_USER,
		DDL_SET_PASSWORD,
		DDL_GRANT,
		DDL_REVOKE,
		DDL_BEGIN,
		DDL_COMMIT,
		DDL_ROLLBACK,
		CMD_GET_TABLE,
		DDL_DROP_PARTITION,
		DDL_ADD_COLUMN,
		DDL_CREATE_VIEW,
		DDL_DROP_VIEW
	};

public:
	
	static const char* const SYSTEM_TABLE_INDEXES_PREFIX;
	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;


/*!
	@brief Constructor
*/
NoSQLStore(
		const ResourceSet *resourceSet,
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		size_t cacheSize,
		DatabaseId dbId,
		const char *dbName,
		NoSQLDB *db);

/*!
	@brief Destructor
*/
~NoSQLStore();

	/*!
		@brief get all db manager
	*/
	NoSQLDB *getDB();

	void createCache();

	SQLTableCache &getCache();

	void removeCache();

	template <class Node, class Key, class Value>
	Node* putCacheEntry(Key &key, Value value, bool withGet);

	template <class Node, class Key>
	Node* getCacheEntry(Key &key);

	template <class Key>
	void removeCacheEntry(Key &key);

	template <class Node>
	void releaseCacheEntry(Node *node);

	void dump(util::NormalOStringStream &oss);

	void resizeTableCache(int32_t resizeCacheSize);

	void createUser(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &userName,
			const NameWithCaseSensitivity &password);

	void dropUser(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &userName);

	void setPassword(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &userName,
			const NameWithCaseSensitivity &password);

	void grant(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &userName,
			const NameWithCaseSensitivity &dbName,
			AccessControlType type);

	void revoke(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &userName,
			const NameWithCaseSensitivity &dbName,
			AccessControlType type);

	void createDatabase(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName);

	void dropDatabase(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName);

	void createTable(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			CreateTableOption &createTableOption,
			DDLBaseInfo *baseInfo
	);

	void dropTable(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			bool ifExists,
			DDLBaseInfo *baseInfo);

	void createView(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			CreateTableOption &createTableOption,
			DDLBaseInfo *baseInfo);

	void dropView(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			bool ifExists);

	void addColumn(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			CreateTableOption &createTableOption,
			DDLBaseInfo *baseInfo
	);

	void createIndex(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &indexName,
			const NameWithCaseSensitivity &tableName,
			CreateIndexOption &option,
			DDLBaseInfo *baseInfo);

	void createIndexPost(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &indexName,
			const NameWithCaseSensitivity &tableName,
			DDLBaseInfo *baseInfo);

	void dropIndex(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &indexName,
			const NameWithCaseSensitivity &tableName,
			bool ifExists,
			DDLBaseInfo *baseInfo);

	void dropIndexPost(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &indexName,
			const NameWithCaseSensitivity &tableName,
			DDLBaseInfo *baseInfo);

	void dropTablePartition(
			EventContext &ec, 
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			DDLBaseInfo *baseInfo);

	void dropTablePartitionPost(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName);

	bool getTable(
			EventContext &ec,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			TableSchemaInfo *&schemaInfo,
			bool withCache,
			SQLTableCacheEntry *&entry,
			int64_t emNow);

	void begin();

	void commit();

	void rollback();

	void getContainer(
			NoSQLContainer &container,
			bool isSystemDb,
			NoSQLStoreOption &option);

void putContainer(
		TableExpirationSchemaInfo	&info,
		util::XArray<uint8_t> &binarySchemaInfo,
		size_t containerPos,
		ContainerType containerType,
		ContainerAttribute containerAttr,
		bool modifiable,
		NoSQLContainer &container,
		util::XArray<uint8_t> *schema,
		util::XArray<uint8_t> *binary,
		NoSQLStoreOption &option);

	void removeCache(const char *tableName);

	template<typename T>
	void getLargeRecord(
			util::StackAllocator &alloc,
			const char *key,
			NoSQLContainer &container,
			util::XArray<ColumnInfo> &columnInfoList,
			T &record,
			NoSQLStoreOption &option);

void getLargeBinaryRecord(
		util::StackAllocator &alloc,
		const char *key,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList, 
		util::XArray<uint8_t> &record, 
		NoSQLStoreOption &option);

		template<typename Alloc>
		void createSubContainer(
				TableExpirationSchemaInfo &info,
				util::StackAllocator &alloc,
				NoSQLContainer &targetContainer, NoSQLContainer *currentContainer,
				util::XArray<uint8_t> &subContainerSchema,
				TablePartitioningInfo<Alloc>  &partitioningInfo,
				TablePartitioningIndexInfo &tablePartitioningIndexInfo,
				const NameWithCaseSensitivity &tableName,
				int32_t partitionNum, NodeAffinityNumber &affinity);

	void setDbName(const char *dbName);

	const char *getDbName() {
		return dbName_.c_str();
	}

	const char *getNormalizedDbName() {
		return normalizedDbName_.c_str();
	}

	SQLVariableSizeGlobalAllocator &getAllocator() {
		return globalVarAlloc_;
	}

	const ResourceSet *getResourceSet() {
		return resourceSet_;
	}

	void checkWritableContainer(NoSQLContainer &container);
	void checkViewContainer(NoSQLContainer &container);
	void checkExistContainer(NoSQLContainer &container);

private:

	static const char *getCommandString(DDLCommandType type);

	struct ExecuteCondition {
		
		ExecuteCondition() :
				userName_(NULL),
				dbName_(NULL),
				tableName_(NULL),
				indexName_(NULL) {}
	
		ExecuteCondition(
				const NameWithCaseSensitivity *userName,
				const NameWithCaseSensitivity *dbName,
				const NameWithCaseSensitivity *tableName,
				const NameWithCaseSensitivity *indexName) :
						userName_(userName),
						dbName_(dbName),
						tableName_(tableName),
						indexName_(indexName) {};
	
		const NameWithCaseSensitivity *userName_;
		const NameWithCaseSensitivity *dbName_;
		const NameWithCaseSensitivity *tableName_;
		const NameWithCaseSensitivity *indexName_;
	};	

	void checkExecutable(
			util::StackAllocator &alloc, 
			DDLCommandType CommandType,
			SQLExecution *execution,
			bool isAdjustDb,
			ExecuteCondition &condition);

	void checkAdministrator(SQLExecution *execution);

	void checkValidName(const char *name);

	void checkConnectedDb(
			DDLCommandType commandType,
			const char *dbName,
			bool caseSensitive) {

		UNUSED_VARIABLE(commandType);

		if (caseSensitive) {
			if (strcmp(dbName_.c_str(), dbName) != 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
						"Connected database is not same as the target database, connected='"
						<< dbName_.c_str() << "', specified='" << dbName << "'");
			}
		}
		else {
			if (strcmp(normalizedDbName_.c_str(), dbName) != 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
						"Connected database is not same as the target database, connected='"
						<< dbName_.c_str() << "', specified='" << dbName << "'");
			}
		}
	}

	bool adjustConnectedDb(const char *&dbName) {

		if (dbName == NULL || (dbName != NULL && strlen(dbName) == 0)) {
			dbName = dbName_.c_str();
			return true;
		}
		else {
			return false;
		}
	}

	util::Mutex lock_;
	const ResourceSet *resourceSet_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;

	DatabaseId dbId_;
	SQLString dbName_;
	SQLString normalizedDbName_;
	size_t cacheSize_;
	void *cache_;
	NoSQLDB *db_;
};


#endif
