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
	@brief Definition of nosql command
*/

/*!
	@brief SQLコマンド管理
	@note DB単位
*/
#ifndef SQL_COMMAND_MANAGER_H_
#define SQL_COMMAND_MANAGER_H_

#include "nosql_command.h"
#include "sql_parser.h"
#include "lru_cache.h"

class SQLExecution;
class DataStoreV4;
struct DDLBaseInfo;
class DataStoreBase;

typedef int32_t AccessControlType;
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
	DDL_RENAME_COLUMN, 
	DDL_CREATE_VIEW,
	DDL_DROP_VIEW
};

const char8_t* getCommandString(DDLCommandType type);

struct PartitionRangeList {
	struct PartitionRange {
		PartitionRange(PartitionId startPId, PartitionId endPId, int32_t range) :
			startPId_(startPId), endPId_(endPId), range_(range) {}
		PartitionId startPId_;
		PartitionId endPId_;
		uint32_t range_;
	};

	PartitionId set(PartitionId startPId, int32_t rangeVal) {
		PartitionRange range(startPId, startPId + rangeVal - 1, rangeVal);
		ranges_.push_back(range);
		return (startPId + rangeVal);
	}

	void get(int32_t pos, PartitionId& startPId, PartitionId& endPId) {
		startPId = ranges_[pos].startPId_;
		endPId = ranges_[pos].endPId_;
	}

	PartitionRangeList(util::StackAllocator& alloc, int32_t partitionNum,
		uint32_t tablePartitioningNum) : alloc_(alloc), ranges_(alloc) {
		base_ = (partitionNum / tablePartitioningNum);
		mod_ = (partitionNum % tablePartitioningNum);
		uint32_t range = 0;
		PartitionId startPId = 0;
		for (uint32_t pos = 0; pos < tablePartitioningNum; pos++) {
			if (pos < mod_) {
				range = base_ + 1;
			}
			else {
				range = base_;
			}
			startPId = set(startPId, range);
		}
	}
	util::StackAllocator& alloc_;
	util::Vector<PartitionRange> ranges_;
	uint32_t base_;
	uint32_t mod_;
	int32_t currentPos_;
};

template<class T> void randomShuffle(
	util::Vector<T>& ary, int32_t size, util::Random& random) {
	for (int32_t i = 0; i < size; i++) {
		uint32_t j = (uint32_t)(random.nextInt32()) % size;
		T t = ary[i];
		ary[i] = ary[j];
		ary[j] = t;
	}
}

class NoSQLStore {
public:
	static const char* const SYSTEM_TABLE_INDEXES_PREFIX;
	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

	typedef std::vector<ColumnId, util::StdAllocator<
		ColumnId, SQLVariableSizeGlobalAllocator> > ColumnIdList;

	typedef CacheNode<SQLString, TableSchemaInfo*,
		SQLVariableSizeGlobalAllocator> SQLTableCacheEntry;

	typedef LruCacheConcurrency<SQLTableCacheEntry,
		SQLString, TableSchemaInfo*,
		SQLVariableSizeGlobalAllocator> SQLTableCache;

	struct DDLSource {
		util::StackAllocator& alloc_;
		EventContext& ec_; 
		SQLExecution* execution_; 
		NoSQLStoreOption option_; 
		const NameWithCaseSensitivity& dbName_; 
		const NameWithCaseSensitivity& tableName_; 
		const CreateTableOption& createTableOption_; 
		DDLBaseInfo* baseInfo_; 

		DDLCommandType commandType_; 

		NoSQLContainer* targetContainer_; 
		TablePartitioningInfo<util::StackAllocator> partitioningInfo_; 
		TablePartitioningIndexInfo tablePartitioningIndexInfo_; 
		util::XArray<uint8_t> orgContainerSchema_; 
		util::XArray<uint8_t> newContainerSchema_; 

		bool isPartitioning_; 
		ContainerType containerType_; 
		int32_t columnNum_; 
		ContainerAttribute containerAttribute_; 
		bool renamed_;
		
		void setRenamed() {
			renamed_ = true;
		}
		
		bool isRenamed() {
			return renamed_;
		}

		DDLSource(util::StackAllocator& alloc, EventContext& ec, SQLExecution* execution,
			const NameWithCaseSensitivity& dbName, const NameWithCaseSensitivity& tableName,
			const CreateTableOption& createTableOption, DDLBaseInfo* baseInfo, DDLCommandType commandType) :
			alloc_(alloc), ec_(ec), execution_(execution), option_(execution),
			dbName_(dbName), tableName_(tableName), createTableOption_(createTableOption),
			baseInfo_(baseInfo), commandType_(commandType), targetContainer_(NULL),
			partitioningInfo_(alloc), tablePartitioningIndexInfo_(alloc),
			orgContainerSchema_(alloc), newContainerSchema_(alloc),
			isPartitioning_(false), containerType_(CONTAINER_ATTR_SINGLE), columnNum_(0), renamed_(false) {
			if (dbName_.isCaseSensitive_) {
				option_.caseSensitivity_.setDatabaseNameCaseSensitive();
			}
			if (tableName_.isCaseSensitive_) {
				option_.caseSensitivity_.setContainerNameCaseSensitive();
			}
		}
	};

	struct RenameColumnContext {
		char* oldColumnName_; 
		char* newColumnName_; 
		int oldColumnNameId_; 
		int newColumnNameId_; 
		int32_t oldColumnNameLen_; 
		int32_t newColumnNameLen_; 

		RenameColumnContext(const CreateTableOption& createTableOption);
		void checkColumnName(DDLSource& ddlSource);
	};

		/*!
			@brief Constructor
		*/
	NoSQLStore(
		SQLVariableSizeGlobalAllocator& globalVarAlloc,
		size_t cacheSize,
		DatabaseId dbId,
		const char* dbName,
		PartitionTable* pt,
		PartitionList* partitionList,
		DataStoreConfig* dsConfig,
		DBConnection* dbConnection,
		JobManager* jobManager);

	/*!
		@brief Destructor
	*/
	~NoSQLStore();

	DataStoreV4* getDataStore(PartitionId pId);

	bool checkContainerExist(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& tableName);

	/*!
	@brief get all db manager
*/
	DBConnection* getDbConnection();

	void createCache();

	SQLTableCache& getCache();

	void removeCache();

	template <class Node, class Key, class Value>
	Node* putCacheEntry(Key& key, Value value, bool withGet);

	template <class Node, class Key>
	Node* getCacheEntry(Key& key);

	template <class Key>
	void removeCacheEntry(Key& key);

	template <class Node>
	void releaseCacheEntry(Node* node);

	template <class Key>
	bool setDiffLoad(Key& key);

	void dump(util::NormalOStringStream& oss);

	void resizeTableCache(int32_t resizeCacheSize);

	void refresh();

	void createUser(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& password,
		bool isRole);

	void dropUser(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName);

	void setPassword(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& password);

	void grant(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& dbName,
		AccessControlType type);

	void revoke(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& dbName,
		AccessControlType type);

	void createDatabase(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName);

	void dropDatabase(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName);

	void createTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo
	);

	void dropTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		bool ifExists,
		DDLBaseInfo* baseInfo);

	void dropTablePost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo);

	void createView(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo);

	void dropView(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		bool ifExists);

	void addColumn(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo
	);

	void renameColumn(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo
	);

	void createIndex(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& indexName,
		const NameWithCaseSensitivity& tableName,
		const CreateIndexOption& option,
		DDLBaseInfo* baseInfo);

	void createIndexPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& indexName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo);

	void dropIndex(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& indexName,
		const NameWithCaseSensitivity& tableName,
		bool ifExists,
		DDLBaseInfo* baseInfo);

	void dropIndexPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& indexName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo);

	void dropTablePartition(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo);

	void dropTablePartitionPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName);

	bool getTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		int64_t emNow,
		TableSchemaInfo*& schemaInfo,
		TableSchemaInfo* prevSchemaInfo);

	bool getTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		TableSchemaInfo*& schemaInfo,
		bool withCache,
		SQLTableCacheEntry*& entry,
		int64_t emNow,
		bool diff = false);

	void estimateIndexSearchSize(
			EventContext &ec, SQLExecution &execution,
			SQLIndexStatsCache &indexStats, uint64_t requester);

	void applyIndexSearchSize(
			const SQLIndexStatsCache::KeyList &keyList,
			const util::Vector<int64_t> &estimationList,
			SQLIndexStatsCache &indexStats, uint64_t requester);

	void begin();

	void commit();

	void rollback();

	void getContainer(
		NoSQLContainer& container,
		bool isSystemDb,
		NoSQLStoreOption& option);

	void putContainer(
		TableExpirationSchemaInfo& info,
		RenameColumnSchemaInfo renameColInfo,
		util::XArray<uint8_t>& binarySchemaInfo,
		int32_t containerPos,
		ContainerType containerType,
		ContainerAttribute containerAttr,
		bool modifiable,
		NoSQLContainer& container,
		util::XArray<uint8_t>* schema,
		util::XArray<uint8_t>* binary,
		NoSQLStoreOption& option);

	void removeCache(const char* tableName);

	template<typename T>
	void getLargeRecord(
		util::StackAllocator& alloc,
		const char* key,
		NoSQLContainer& container,
		util::XArray<ColumnInfo>& columnInfoList,
		T& record,
		NoSQLStoreOption& option);

	void getLargeBinaryRecord(
		util::StackAllocator& alloc,
		const char* key,
		NoSQLContainer& container,
		util::XArray<ColumnInfo>& columnInfoList,
		util::XArray<uint8_t>& record,
		NoSQLStoreOption& option);

	void setDbName(const char* dbName) {
		if (dbName_.empty()) {
			dbName_ = dbName;
			NoSQLUtils::normalizeString(normalizedDbName_, dbName_.c_str());
		}
	}

	const char* getDbName() {
		return dbName_.c_str();
	}

	const char* getNormalizedDbName() {
		return normalizedDbName_.c_str();
	}

	SQLVariableSizeGlobalAllocator& getAllocator() {
		return globalVarAlloc_;
	}

	void setStat(int32_t storeAccessCount, int32_t storeSkipCount, int32_t storeRecoveryCount, bool dump) {
		storeAccessCount_ += storeAccessCount;
		storeSkipCount_ += storeSkipCount;
		storeRecoveryCount_ += storeRecoveryCount;
		if (dump) {
			std::cout << "access=" << storeAccessCount << ",skip=" << storeSkipCount << ",recovery" << storeRecoveryCount_ << std::endl;
			std::cout << "[T] access=" << storeAccessCount_ << ",skip=" << storeSkipCount_ << ",recovery=" << storeRecoveryCount_ << std::endl;
		}
	}

private:

	void checkDatabase(
		util::StackAllocator& alloc,
		DDLCommandType commandType,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName);

	/*!
		@brief SQLコマンドの実行判定
		@note executionの状態をチェックして、不整合時に例外を発行する
	*/
	struct ExecuteCondition {

		ExecuteCondition() : userName_(NULL), dbName_(NULL),
			tableName_(NULL), indexName_(NULL) {}

		ExecuteCondition(const NameWithCaseSensitivity* userName,
			const NameWithCaseSensitivity* dbName, const NameWithCaseSensitivity* tableName,
			const NameWithCaseSensitivity* indexName) :
			userName_(userName), dbName_(dbName),
			tableName_(tableName), indexName_(indexName) {};

		const NameWithCaseSensitivity* userName_;
		const NameWithCaseSensitivity* dbName_;
		const NameWithCaseSensitivity* tableName_;
		const NameWithCaseSensitivity* indexName_;
	};

	void checkExecutable(util::StackAllocator& alloc,
		DDLCommandType CommandType,
		SQLExecution* execution, bool isAdjustDb, ExecuteCondition& condition);

	/*!
		@brief 管理者権限チェック
	*/
	void checkAdministrator(SQLExecution* execution);

	/*!
		@brief 指定名勝の妥当性チェック
	*/
	void checkValidName(DDLCommandType CommandType, const char* name);

	/*!
		@brief 指定DBが接続DBと異なるかの判定
		@note 接続DB以外のDB操作はできないためこれで判定可能
	*/
	void checkConnectedDb(
			DDLCommandType CommandType, const char* dbName,
			bool caseSensitive) {
		UNUSED_VARIABLE(CommandType);

		if (caseSensitive) {
			if (strcmp(dbName_.c_str(), dbName) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
					"Connected database is not same as the target database, connected='"
					<< dbName_.c_str() << "', specified='" << dbName << "'");
			}
		}
		else {
			if (strcmp(normalizedDbName_.c_str(), dbName) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
					"Connected database is not same as the target database, connected='"
					<< dbName_.c_str() << "', specified='" << dbName << "'");
			}
		}
	}

	/*!
		@brief データベース名指定無の補正
		@note 接続DBと同じにする
	*/
	bool adjustConnectedDb(const char*& dbName) {
		if (dbName == NULL || (dbName != NULL && strlen(dbName) == 0)) {
			dbName = dbName_.c_str();
			return true;
		}
		else {
			return false;
		}
	}

	void checkPre(DDLSource& ddlSource);
	void checkContainer(DDLSource& ddlSource, NoSQLContainer& targetContainer);
	void setDDL(DDLSource& ddlSource, NoSQLContainer& targetContainer);
	void makeSchema(DDLSource& ddlSource, RenameColumnContext* rcCxt = NULL);
	void execNoSQL(DDLSource& ddlSource, NoSQLContainer& targetContainer, bool isRenameColumn);

	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	util::Mutex lock_;
	DatabaseId dbId_;
	SQLString dbName_;
	SQLString normalizedDbName_;
	size_t cacheSize_;
	void* cache_;
	PartitionTable* pt_;
	PartitionList* partitionList_;
	DataStoreConfig* dsConfig_;
	DBConnection* dbConnection_;
	JobManager* jobManager_;
	util::Atomic<int64_t> storeAccessCount_;
	util::Atomic<int64_t> storeSkipCount_;
	util::Atomic<int64_t> storeRecoveryCount_;
};


/*!
	@brief SQLコマンド管理
	@note DB単位
*/
class DBConnection {

	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

public:
	static const int32_t SYSTEM_DB_RESERVE_NUM = 2;

	DBConnection(
		ConfigTable& config,
		SQLVariableSizeGlobalAllocator& globalVarAlloc,
		int32_t cacheSize,
		PartitionTable* pt,
		PartitionList* partitionList,
		SQLService* sqlSvc);

	~DBConnection();

	SQLService* getSQLService() {
		return sqlSvc_;
	}

	void dump(util::NormalOStringStream& oss, const char* dbName);
	void resizeTableCache(const char* dbName, int32_t cacheSize);

	void refreshSchemaCache();

	void createUser(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& password,
		bool isRole);

	void dropUser(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName);

	void setPassword(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& password);

	void grant(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& dbName,
		AccessControlType type);

	void revoke(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& userName,
		const NameWithCaseSensitivity& dbName,
		AccessControlType type);

	void createDatabase(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName);

	void dropDatabase(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName);

	void createTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo);

	void dropTable(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		bool ifExists,
		DDLBaseInfo* baseInfo);

	void dropTablePost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo);

	void createView(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo);

	void dropView(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		bool ifExists,
		DDLBaseInfo* baseInfo);

	void addColumn(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo);

	void renameColumn(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const CreateTableOption& createTableOption,
		DDLBaseInfo* baseInfo);

	void createIndex(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const NameWithCaseSensitivity& indexName,
		const CreateIndexOption& option,
		DDLBaseInfo* baseInfo);

	void createIndexPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const NameWithCaseSensitivity& indexName,
		DDLBaseInfo* baseInfo);

	void dropIndex(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const NameWithCaseSensitivity& indexName,
		bool ifExists,
		DDLBaseInfo* baseInfo);

	void dropIndexPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		const NameWithCaseSensitivity& indexName,
		DDLBaseInfo* baseInfo);

	void dropTablePartition(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName,
		DDLBaseInfo* baseInfo);

	void dropTablePartitionPost(
		EventContext& ec,
		SQLExecution* execution,
		const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName);

	void begin(SQLExecution* execution);

	void commit(SQLExecution* execution);

	void rollback(SQLExecution* execution);

	void getContainer(
		SQLExecution* execution,
		NoSQLContainer& container);

	int64_t getMonotonicTime();

	typedef std::vector<NoSQLStore*, util::StdAllocator<NoSQLStore*,
		SQLVariableSizeGlobalAllocator> > NoSQLStoreList;

public:

	/*!
		@brief テーブル取得用ラッチ
		@note getTableを直接呼ぶのはでなく、ラッチを確保した上で実施する
	*/

	NoSQLStore* getNoSQLStore(
		DatabaseId pos, const char* dbName);

	static const int32_t DEFAULT_DATABASE_NUM = 4;

	PartitionTable* getPartitionTable() {
		return pt_;
	}

	SQLVariableSizeGlobalAllocator& getVarAllocator() {
		return globalVarAlloc_;
	}

	SQLExecutionManager* getExecutionManager();
	JobManager* getJobManager();


private:
	NoSQLStore* getNoSQLStore(const char* dbName);

	typedef std::pair<DatabaseId, NoSQLStore*> NoSQLStoreMapEntry;
	typedef std::map<DatabaseId, NoSQLStore*, std::less<DatabaseId>,
		util::StdAllocator<NoSQLStoreMapEntry,
		SQLVariableSizeGlobalAllocator> > NoSQLStoreMap;
	typedef NoSQLStoreMap::iterator NoSQLStoreMapItr;

	util::Mutex lock_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	size_t cacheSize_;
	NoSQLStoreList basicStoreList_;
	NoSQLStoreMap storeMap_;

	PartitionTable* pt_;

	PartitionList* partitionList_;
	DataStoreConfig* dsConfig_;
	SQLService* sqlSvc_;
};

template<typename T>
void NoSQLStore::getLargeRecord(util::StackAllocator& alloc,
	const char* key, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList, T& record, NoSQLStoreOption& option) {
	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='" << key << "'";
	container.executeSyncQuery(queryStr.str().c_str(), option,
		fixedPart, varPart, rowCount, hasRowKey);

	if (rowCount == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
			"Target table is not found or change to schema, table name=" << container.getName());
	}
	InputMessageRowStore rowStore(*dsConfig_,
		columnInfoList.data(), static_cast<uint32_t>(columnInfoList.size()),
		fixedPart.data(), static_cast<uint32_t>(fixedPart.size()), varPart.data(),
		static_cast<uint32_t>(varPart.size()), rowCount, true, false);

	VersionInfo versionInfo;
	decodeRow<T>(rowStore, record, versionInfo, key);
}

template<typename Alloc>
void createSubContainer(
		TableExpirationSchemaInfo& info,
		util::StackAllocator& alloc, NoSQLStore* command,
		NoSQLContainer& targetContainer, NoSQLContainer* currentContainer,
		util::XArray<uint8_t>& subContainerSchema,
		TablePartitioningInfo<Alloc>& partitioningInfo,
		TablePartitioningIndexInfo& tablePartitioningIndexInfo,
		const NameWithCaseSensitivity& tableName,
		int32_t partitionNum, NodeAffinityNumber& affinity) {
	UNUSED_VARIABLE(alloc);
	UNUSED_VARIABLE(targetContainer);
	UNUSED_VARIABLE(partitionNum);
	UNUSED_VARIABLE(affinity);

	NoSQLStoreOption cmdOption;
	cmdOption.ifNotExistsOrIfExists_ = true;
	if (tableName.isCaseSensitive_) {
		cmdOption.caseSensitivity_.setContainerNameCaseSensitive();
	}

	RenameColumnSchemaInfo renameColInfo;
	command->putContainer(info, renameColInfo, subContainerSchema, 0,
		partitioningInfo.containerType_,
		CONTAINER_ATTR_SUB, false, *currentContainer, NULL, NULL, cmdOption);

	currentContainer->updateContainerProperty(
		partitioningInfo.partitioningVersionId_, cmdOption);
	TablePartitioningIndexInfoEntry* entry;
	for (size_t indexPos = 0; indexPos < tablePartitioningIndexInfo.indexEntryList_.size(); indexPos++) {
		entry = tablePartitioningIndexInfo.indexEntryList_[indexPos];
		try {
			if (!entry->indexName_.empty()) {
				currentContainer->createIndex(
					entry->indexName_.c_str(), entry->indexType_, entry->columnIds_, cmdOption);
			}
		}
		catch (std::exception& e) {
		}
	}
}
class TableLatch {
public:
	/*!
		@brief コンストラクタ
	*/
	TableLatch(EventContext& ec, DBConnection* conn,
		SQLExecution* execution, const NameWithCaseSensitivity& dbName,
		const NameWithCaseSensitivity& tableName, bool withCache, bool diff = false);

	/*!
		@brief デストラクタ
		@note ラッチオブジェクトを解放するだけ
	*/
	~TableLatch();
	/*!
		@brief スキーマ情報取得
	*/
	TableSchemaInfo* get() {
		return schemaInfo_;
	}
	bool isUseCache() {
		return useCache_;
	}
private:
	NoSQLStore::SQLTableCache* cache_;
	NoSQLStore::SQLTableCacheEntry* entry_;

	TableSchemaInfo* schemaInfo_;
	bool useCache_;
};


template<typename Alloc>
NoSQLContainer* recoveryContainer(
	util::StackAllocator& alloc,
	EventContext& ec,
	NoSQLStore* store,
	TablePartitioningInfo<Alloc>& partitioningInfo,
	NoSQLContainer& targetContainer,
	SQLExecution* execution,
	int32_t subContainerId,
	const NameWithCaseSensitivity& tableName,
	int32_t partitionNum,
	NodeAffinityNumber affinity);

#endif
