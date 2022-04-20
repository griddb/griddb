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
	@brief Definition of framework sql processor ddl
*/
#ifndef SQL_PROCESSOR_DDL_H_
#define SQL_PROCESSOR_DDL_H_

#include "sql_processor.h"
#include "sql_parser.h"
#include "sql_compiler.h"
#include <iostream>
#include "container_key.h"

typedef SQLContext Context;
typedef int32_t AccessControlType;
typedef SyntaxTree::CreateTableOption CreateTableOption;
typedef SyntaxTree::CreateIndexOption CreateIndexOption;
class NoSQLContainer;
class DDLProcessor;
typedef StatementId ExecId;

struct DDLBaseInfo {
	DDLBaseInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), execId_(0), isSync_(true), processor_(NULL), isFinished_(false)
		, checking_(false)
		, existJob_(false)
		, ec_(NULL)
		, currentEventType_(UNDEF_EVENT_TYPE)
		, currentContainerName_(NULL)
		, currentDbName_(NULL)
		, largeContainerId_(UNDEF_CONTAINERID)
	{}
	ExecId execId_;
	uint8_t jobVersionId_;
	bool isSync_;
	DDLProcessor* processor_;
	void updateAckCount(int32_t ackCount);
	bool isFinished_;
	bool checking_;
	bool existJob_;
	EventContext* ec_;
	EventType currentEventType_;
	ContainerId largeContainerId_;

	const char* currentContainerName_;
	const char* currentDbName_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;

	~DDLBaseInfo() {}

	void startSync(EventType eventType) {
		currentEventType_ = eventType;
	}
	void endSync() {
		if (currentEventType_ != UNDEF_EVENT_TYPE) {
			currentEventType_ = UNDEF_EVENT_TYPE;
		}
	}
	bool isSync() {
		return (UNDEF_EVENT_TYPE != currentEventType_);
	}
};

struct CreateUserInfo {
	CreateUserInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), userName_(globalVarAlloc), password_(globalVarAlloc)
		, userNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString userName_;
	SQLString password_;
	bool userNameCaseSensitive_;
};

struct DropUserInfo {
	DropUserInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), userName_(globalVarAlloc)
		, userNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString userName_;
	bool userNameCaseSensitive_;
};

struct SetPasswordInfo {
	SetPasswordInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), userName_(globalVarAlloc), password_(globalVarAlloc)
		, userNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString userName_;
	SQLString password_;
	bool userNameCaseSensitive_;
};

struct GrantInfo {
	GrantInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), userName_(globalVarAlloc), dbName_(globalVarAlloc), controlType_(0)
		, userNameCaseSensitive_(false), dbNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString userName_;
	SQLString dbName_;
	AccessControlType controlType_;
	bool userNameCaseSensitive_;
	bool dbNameCaseSensitive_;
};

struct RevokeInfo {
	RevokeInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), userName_(globalVarAlloc), dbName_(globalVarAlloc), controlType_(0)
		, userNameCaseSensitive_(false), dbNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString userName_;
	SQLString dbName_;
	AccessControlType controlType_;
	bool userNameCaseSensitive_;
	bool dbNameCaseSensitive_;
};

struct CreateDatabaseInfo {
	CreateDatabaseInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc)
		, dbNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	bool dbNameCaseSensitive_;
};

struct DropDatabaseInfo {
	DropDatabaseInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc)
		, dbNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	bool dbNameCaseSensitive_;
};

struct CreateTableInfo
	: public DDLBaseInfo
{
	CreateTableInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc),
		globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc), tableName_(globalVarAlloc), createTableOpt_(NULL)
		, dbNameCaseSensitive_(false),
		tableNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString tableName_;
	CreateTableOption* createTableOpt_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
};

struct DropTableInfo : public DDLBaseInfo {
	DropTableInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc),
		globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc), tableName_(globalVarAlloc), ifExists_(false)
		, dbNameCaseSensitive_(false),
		tableNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString tableName_;
	bool ifExists_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
};

struct CreateViewInfo
	: public DDLBaseInfo
{
	CreateViewInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc),
		globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc), tableName_(globalVarAlloc), createTableOpt_(NULL)
		, dbNameCaseSensitive_(false),
		tableNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString tableName_;
	CreateTableOption* createTableOpt_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
};

struct DropViewInfo : public DDLBaseInfo {
	DropViewInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc),
		globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc), tableName_(globalVarAlloc), ifExists_(false)
		, dbNameCaseSensitive_(false),
		tableNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString tableName_;
	bool ifExists_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
};

struct DropTablePartitionInfo : public DDLBaseInfo {
	DropTablePartitionInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc), globalVarAlloc_(globalVarAlloc),
		dbName_(globalVarAlloc), tableName_(globalVarAlloc),
		partitionName_(globalVarAlloc), cmdOptionList_(NULL),
		dbNameCaseSensitive_(false), tableNameCaseSensitive_(false),
		ifExists_(false), affinity_(UNDEF_NODE_AFFINITY_NUMBER)
	{}
	~DropTablePartitionInfo() {
	}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString tableName_;
	SQLString partitionName_;
	SyntaxTree::ExprList* cmdOptionList_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
	bool ifExists_;
	NodeAffinityNumber affinity_;
};

struct AddColumnInfo : public DDLBaseInfo {
	AddColumnInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc), globalVarAlloc_(globalVarAlloc),
		dbName_(globalVarAlloc), tableName_(globalVarAlloc),
		createTableOpt_(NULL),
		dbNameCaseSensitive_(false), tableNameCaseSensitive_(false)
	{}
	~AddColumnInfo() {
	}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString tableName_;
	CreateTableOption* createTableOpt_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
};

struct RenameColumnInfo : public DDLBaseInfo {
	RenameColumnInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc), globalVarAlloc_(globalVarAlloc),
		dbName_(globalVarAlloc), tableName_(globalVarAlloc),
		createTableOpt_(NULL),
		dbNameCaseSensitive_(false), tableNameCaseSensitive_(false)
	{}
	~RenameColumnInfo() {
	}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_; 
	SQLString tableName_; 
	CreateTableOption* createTableOpt_; 
	bool dbNameCaseSensitive_; 
	bool tableNameCaseSensitive_; 
};

struct CreateIndexInfo : public DDLBaseInfo {
	CreateIndexInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc), globalVarAlloc_(globalVarAlloc), dbName_(globalVarAlloc),
		indexName_(globalVarAlloc), tableName_(globalVarAlloc),
		columnName_(globalVarAlloc), createIndexOpt_(NULL)
		, versionId_(-1), containerId_(-1), indexColumnId_(-1),
		dbNameCaseSensitive_(false), tableNameCaseSensitive_(false),
		indexNameCaseSensitive_(false), columnNameCaseSensitive_(false), isSameName_(false)
	{}
	~CreateIndexInfo() {
	}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString indexName_;
	SQLString tableName_;
	SQLString columnName_;
	CreateIndexOption* createIndexOpt_;
	SchemaVersionId versionId_;
	ContainerId containerId_;
	int32_t indexColumnId_;
	bool isSameName_;

	void bindParamInfos(const char* dbName, const char* indexName, const char* tableName, const char* columnName,
		SchemaVersionId versionId,
		ContainerId containerId) {
		dbName_ = dbName_;
		tableName_ = tableName_;
		indexName_ = indexName_;
		columnName_ = columnName;
		containerId_ = containerId;
		versionId_ = versionId;
	}
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
	bool indexNameCaseSensitive_;
	bool columnNameCaseSensitive_;
};

struct DropIndexInfo : public DDLBaseInfo {
	DropIndexInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		DDLBaseInfo(globalVarAlloc),
		globalVarAlloc_(globalVarAlloc),
		dbName_(globalVarAlloc), indexName_(globalVarAlloc), ifExists_(false)
		, tableName_(globalVarAlloc), indexKey_(globalVarAlloc)
		, dbNameCaseSensitive_(false),
		tableNameCaseSensitive_(false),
		indexNameCaseSensitive_(false)
	{}
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLString dbName_;
	SQLString indexName_;
	SQLString tableName_;
	SQLString indexKey_;
	bool ifExists_;
	bool dbNameCaseSensitive_;
	bool tableNameCaseSensitive_;
	bool indexNameCaseSensitive_;
};

class DDLProcessor : public SQLProcessor {

public:

	struct AckContainerInfo {
		AckContainerInfo() : containerId_(-1), pId_(-1), stmtId_(0)
			, ptRev_(0), nodeId_(-1), masterNodeId_(-1), pos_(0)
		{}
		ContainerId containerId_;
		PartitionId pId_;
		StatementId stmtId_;
		SessionId sessionId_;
		PartitionRevisionNo ptRev_;
		NodeId nodeId_;
		NodeId masterNodeId_;
		int32_t pos_;
		ClientId clientId_;
	};

	typedef std::vector<uint8_t, util::StdAllocator<
		uint8_t, SQLVariableSizeGlobalAllocator> > AckList;
	typedef std::vector<AckContainerInfo, util::StdAllocator<
		AckContainerInfo, SQLVariableSizeGlobalAllocator> > AckContainerInfoList;


	DDLProcessor(Context& cxt, const TypeInfo& typeInfo);

	virtual ~DDLProcessor();

	virtual bool pipe(Context& cxt, InputId inputId, const Block& block);

	virtual bool finish(Context& cxt, InputId inputId);

	virtual bool applyInfo(
		Context& cxt, const Option& option,
		const TupleInfoList& inputInfo, TupleInfo& outputInfo);

	void exportTo(Context& cxt, const OutOption& option) const;

	void updateAckCount(int32_t ackCount) {
		targetAckCount_ = ackCount;
		ackList_.assign(ackCount, 0);
	}

	void setAckStatus(NoSQLContainer* containerInfo, int32_t pos, int32_t status);

	void setRetry() {
		isRetry_ = true;
	}
	void setAsyncStart() {
		isAsyncExecuting_ = true;
	}

	void setAsyncCompleted() {
		isAsyncExecuting_ = false;
	}

	void cleanupNoSQL();

	ClientId& getClientId() {
		return clientId_;
	}
	void setCheckExecutionAckCount(int32_t count) {
		targetCheckAckCount_ = count;
	}
	void checkPartitionStatus();

	void setPendingContainerInfo(ContainerId containerId,
		SchemaVersionId versionId, StatementId stmtId) {
		executingContainerId_ = containerId;
		executingVersionId_ = versionId;
		executingStatementId_ = stmtId;
	}
	void setCleanup() {
		isCleanup_ = true;
	}

private:
	typedef Factory::Registrar<DDLProcessor> Registrar;

	void setOptionString(
		const SQLPreparedPlan::Node& node, int32_t pos,
		SQLString& str, bool* isCaseSensitive = NULL);

	template <typename T>
	void setOptionValue(const SQLPreparedPlan::Node& node, int32_t pos, T& t);


	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	ClientId clientId_;
	static const Registrar registrar_;
	SyntaxTree::CommandType commandType_;

	CreateUserInfo* createUserInfo_;
	DropUserInfo* dropUserInfo_;
	SetPasswordInfo* setPasswordInfo_;
	GrantInfo* grantInfo_;
	RevokeInfo* revokeInfo_;
	CreateDatabaseInfo* createDatabaseInfo_;
	DropDatabaseInfo* dropDatabaseInfo_;
	CreateTableInfo* createTableInfo_;
	DropTableInfo* dropTableInfo_;
	CreateIndexInfo* createIndexInfo_;
	DropIndexInfo* dropIndexInfo_;
	DropTablePartitionInfo* dropPartitionInfo_;
	AddColumnInfo* addColumnInfo_;
	RenameColumnInfo* renameColumnInfo_; 
	CreateViewInfo* createViewInfo_;
	DropViewInfo* dropViewInfo_;

	int32_t targetAckCount_;
	int32_t currentAckCount_;
	int32_t phase_;
	AckList ackList_;
	AckContainerInfoList ackContainerInfoList_;

	ContainerId executingContainerId_;
	SchemaVersionId executingVersionId_;
	StatementId executingStatementId_;

	SQLExecutionManager* executionManager_;
	bool isRetry_;
	bool isAsyncExecuting_;
	SQLExecution* execution_;
	int32_t targetCheckAckCount_;
	int32_t currentCheckAckCount_;
	bool existJob_;
	DDLBaseInfo* baseInfo_;
	bool isCleanup_;
};

inline void DDLBaseInfo::updateAckCount(int32_t ackCount) {
	processor_->updateAckCount(ackCount);
}


#endif
