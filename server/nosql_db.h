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
#ifndef NOSQL_DB_H_
#define NOSQL_DB_H_

#include "sql_parser.h"
#include "nosql_store.h"

typedef int32_t AccessControlType;

class ResourceSet;
struct DDLBaseInfo;
struct TableSchemaInfo;
class NoSQLContainer;

class NoSQLDB {

	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

public:

	static const int32_t SYSTEM_DB_RESERVE_NUM = 2;

	NoSQLDB(
			const ResourceSet *resourceSet_,
			ConfigTable &config,
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			int32_t cacheSize);

	~NoSQLDB();

		void dump(util::NormalOStringStream &oss, const char *dbName);
		void resizeTableCache(const char *dbName, int32_t cacheSize);

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
				DDLBaseInfo *baseInfo);

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
				bool ifExist);

		void addColumn(
				EventContext &ec,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName,
				CreateTableOption &createTableOption,
				DDLBaseInfo *baseInfo);

		void createIndex(
				EventContext &ec,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName,
				const NameWithCaseSensitivity &indexName,
				CreateIndexOption &option,
				DDLBaseInfo *baseInfo);
		
		void createIndexPost(
				EventContext &ec,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName,
				const NameWithCaseSensitivity &indexName,
				DDLBaseInfo *baseInfo);

		void dropIndex(
				EventContext &ec,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName,
				const NameWithCaseSensitivity &indexName,
				bool ifExists,
				DDLBaseInfo *baseInfo);

		void dropIndexPost(
				EventContext &ec,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName, 
				const NameWithCaseSensitivity &indexName,
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

		void getTable(
				EventContext &ec,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName,
				TableSchemaInfo *&schemaInfo,
				bool withCache);

		void begin(SQLExecution *execution);

		void commit(SQLExecution *execution);

		void rollback(SQLExecution *execution);

		void getContainer(
				SQLExecution *execution,
				NoSQLContainer &container);

		int64_t getMonotonicTime();

public:

	class TableLatch {
	public:
		TableLatch(
				EventContext &ec,
				NoSQLDB *db,
				SQLExecution *execution,
				const NameWithCaseSensitivity &dbName,
				const NameWithCaseSensitivity &tableName,
				bool withCache);
		~TableLatch();
		TableSchemaInfo *get() {
			return schemaInfo_;
		}

		bool isUseCache() {
			return useCache_;
		}

	private:

		SQLTableCache *cache_;
		SQLTableCacheEntry *entry_;
		TableSchemaInfo *schemaInfo_;
		bool useCache_;
	};

	NoSQLStore *getNoSQLStore(
			DatabaseId pos, const char *dbName);
	
	SQLVariableSizeGlobalAllocator &getVarAllocator() {
		return globalVarAlloc_;
	}

private:

	NoSQLStore *getNoSQLStore(const char *dbName);

	typedef util::AllocMap<DatabaseId, NoSQLStore*>::iterator
			NoSQLStoreMapItr;

	util::Mutex lock_;
	const ResourceSet *resourceSet_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	size_t cacheSize_;
	util::AllocVector<NoSQLStore*> basicStoreList_;
	util::AllocMap<DatabaseId, NoSQLStore*> storeMap_;
	int32_t refCount_;
};

#endif
