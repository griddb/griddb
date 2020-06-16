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
#include "resource_set.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "sql_allocator_manager.h"
#include "sql_processor_ddl.h"
#include "sql_utils.h"
#include "sql_service.h"
#include "sql_cluster_stat.h"
#include "nosql_store.h"
#include "nosql_db.h"
#include "nosql_container.h"
#include "nosql_utils.h"
#include "message_schema.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(SQL_DETAIL);

typedef NoSQLDB::TableLatch TableLatch;

void NoSQLStore::createUser(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &password) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_CREATE_USER;

	try {
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		ExecuteCondition contidion(&gsUsers, NULL, NULL, NULL);
		checkExecutable(
				alloc, commandType, execution, false, contidion);

		NoSQLContainer container(ec, gsUsers,
				execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);

		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}

		container.putUser(
				userName.name_, password.name_, false, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropUser(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_USER;

	try {
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		ExecuteCondition condition(&gsUsers, NULL, NULL, NULL);
		checkExecutable(
				alloc, commandType, execution, false, condition);

		NoSQLContainer container(ec, gsUsers,
			execution->getContext().getSyncContext(), execution);
		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}

		container.dropUser(userName.name_, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::setPassword(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &password) {

	DDLCommandType commandType = DDL_SET_PASSWORD;
	util::StackAllocator &alloc = ec.getAllocator();

	try {
		const char8_t *targetUserName = userName.name_;
		if (userName.name_ == NULL
				|| (userName.name_ != NULL
						&& strlen(userName.name_) == 0)) {
			targetUserName
					= execution->getContext().getUserName().c_str();
		}

		NameWithCaseSensitivity actualUserName(
				targetUserName, userName.isCaseSensitive_);
		ExecuteCondition condition(
				&actualUserName, NULL, NULL, NULL);
		checkExecutable(
				alloc, commandType, execution, false, condition);
		
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(
				ec, gsUsers, execution->getContext().getSyncContext(),
				execution);

		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}

		container.putUser(
				actualUserName.name_, password.name_, true, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::grant(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &dbName,
		AccessControlType type) {

	DDLCommandType commandType = DDL_GRANT;
	util::StackAllocator &alloc = ec.getAllocator();

	try {
		const char *connectedDbName = dbName.name_;
		adjustConnectedDb(connectedDbName);
		ExecuteCondition condition(&userName, &dbName, NULL, NULL);
		checkExecutable(
				alloc, commandType, execution, false, condition);

		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(
				ec, gsUsers, execution->getContext().getSyncContext(),
				execution);

		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}

		container.grant(
				userName.name_, connectedDbName, type, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::revoke(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &userName,
		const NameWithCaseSensitivity &dbName,
		AccessControlType type) {

	DDLCommandType commandType = DDL_REVOKE;
	util::StackAllocator &alloc = ec.getAllocator();

	try {
		const char *connectedDbName = dbName.name_;
		adjustConnectedDb(connectedDbName);
		ExecuteCondition condition(&userName, &dbName, NULL, NULL);
		checkExecutable(
				alloc, commandType, execution, false, condition);
	
		const NameWithCaseSensitivity gsUsers(GS_USERS);
		NoSQLContainer container(
				ec, gsUsers, execution->getContext().getSyncContext(),
				execution);

		NoSQLStoreOption option(execution);
		if (userName.isCaseSensitive_) {
			option.caseSensitivity_.setUserNameCaseSensitive();
		}
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}

		container.revoke(
				userName.name_, connectedDbName, type, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::createDatabase(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_CREATE_DATABASE;

	try {
		ExecuteCondition condition(NULL, &dbName, NULL, NULL);
		checkExecutable(alloc, commandType, execution, false, condition);
		const NameWithCaseSensitivity gsUsers(GS_USERS);

		NoSQLContainer container(
				ec, gsUsers, execution->getContext().getSyncContext(),
				execution);
		NoSQLStoreOption option(execution);

		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}

		container.createDatabase(dbName.name_, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropDatabase(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_DATABASE;
	PartitionTable *pt = resourceSet_->getPartitionTable();

	try {
		ExecuteCondition condition(NULL, &dbName, NULL, NULL);
		checkExecutable(
				alloc, commandType, execution, false, condition);

		uint64_t containerCount = 0;
		NoSQLStoreOption option(execution);
		if (dbName.isCaseSensitive_) {
			option.caseSensitivity_.setDatabaseNameCaseSensitive();
		}

		for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {

			NoSQLContainer container(
					ec, pId, execution->getContext().getSyncContext(),
					execution);
			container.getContainerCount(containerCount, option);

			if (containerCount > 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_DATABASE_NOT_EMPTY,
						"Not empty database, db=" << dbName.name_  << ", pId=" << pId);
			}
		}

		const NameWithCaseSensitivity gsDatabases(GS_DATABASES);
		NoSQLContainer dropContainer(
				ec, gsDatabases, execution->getContext().getSyncContext(),
				execution);

		dropContainer.dropDatabase(dbName.name_, option);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

class TableProcessor {

public:

	TableProcessor(
			EventContext &ec,
			NoSQLStore *store,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			DDLBaseInfo *baseInfo);

protected:

	EventContext &ec_;
	util::StackAllocator &alloc_;
	NoSQLStore *store_;
	SQLExecution *execution_;
	const NameWithCaseSensitivity &dbName_;
	const NameWithCaseSensitivity &tableName_;
	TablePartitioningInfo<util::StackAllocator> partitioningInfo_;
	DDLBaseInfo *baseInfo_;
	NoSQLContainer targetContainer_;
	NoSQLStoreOption option_;
};

TableProcessor::TableProcessor(
		EventContext &ec,
		NoSQLStore *store,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) :
				ec_(ec),
				alloc_(ec.getAllocator()),
				store_(store),
				execution_(execution),
				dbName_(dbName),
				tableName_(tableName),
				partitioningInfo_(alloc_),
				baseInfo_(baseInfo),
				targetContainer_(
					ec, tableName,
					execution->getContext().getSyncContext(),
					execution),
					option_(execution_) {
};

class CreateTableProcessor : public TableProcessor {

public:

	CreateTableProcessor(
			EventContext &ec,
			NoSQLStore *store,
			SQLExecution *execution,
			const NameWithCaseSensitivity &dbName,
			const NameWithCaseSensitivity &tableName,
			CreateTableOption &createTableOption,
			DDLBaseInfo *baseInfo);

private:

	static const int32_t DAY_INTERVAL = 24 * 3600 * 1000;

	void analyzeInitial();

	bool checkPreIdempotency();
	void putLargeContainer();
	TableSchemaInfo * putSubContainers();

	void checkPartitioiningColumnValidation();
	void checkPartitioiningTypeValidation();
	void checkExpirationValidation();

	void checkPrimaryKey();
	int64_t genSeed();

	void checkIntervalLimit(
			int64_t intervalValue, TupleList::TupleColumnType columnType);

	SyntaxTree::TablePartitionType getPartitioningType() {
		return static_cast<SyntaxTree::TablePartitionType>(
				partitioningInfo_.partitionType_);
	}

	CreateTableOption &createTableOption_;
	TablePartitioningInfo<util::StackAllocator> checkPartitioningInfo_;
	util::Vector<ColumnId> primaryKeyList_;
	TablePartitioningInfo<util::StackAllocator> *currentPartitioningInfo_;
	TablePartitioningIndexInfo tablePartitioningIndexInfo_;

	util::XArray<uint8_t> containerSchema_;
	util::XArray<uint8_t> optionList_;
	NoSQLContainer *currentContainer_;
	util::XArray<uint8_t> metaContainerSchema_;
	util::String affinityStr_;
	util::XArray<ColumnInfo> columnInfoList_;

	bool idempotenceCheck_;
	bool isValid_;
	bool isSameName_;
};

void CreateTableProcessor::checkExpirationValidation() {
	
	if (partitioningInfo_.isRowExpiration()) {
		if (!createTableOption_.isTimeSeries()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Row expriration definition must be timeseries container");
		}
	}
	else if (partitioningInfo_.isTableExpiration()) {

		if (!SyntaxTree::isRangePartitioningType(getPartitioningType())) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Table expriration definition must be interval"
						" or interval-hash partitioning");
		}
		if (partitioningInfo_.partitioningColumnId_ == UNDEF_COLUMNID) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Partitioning columnId of partition expiration must be defined");
		}

		TupleList::TupleColumnType partitioningColumnType
				= (*createTableOption_.columnInfoList_)
				[partitioningInfo_.partitioningColumnId_]->type_;

		if (partitioningColumnType != TupleList::TYPE_TIMESTAMP) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Expriration definition column must be timestamp type");
		}
	}
}

void CreateTableProcessor::checkIntervalLimit(
		int64_t intervalValue, TupleList::TupleColumnType columnType) {

	switch (columnType) {
		
		case TupleList::TYPE_BYTE:
			if (intervalValue >= static_cast<int64_t>(INT8_MAX) + 1) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Interval value for byte type partitioning "
						"key column must be less than "
						<< (static_cast<int64_t>(INT8_MAX) + 1));
			}
		break;

		case TupleList::TYPE_SHORT:
			if (intervalValue >= static_cast<int64_t>(INT16_MAX) + 1) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Interval value for short type partitioning "
						"key column must be less than "
						<< (static_cast<int64_t>(INT16_MAX) + 1));
			}
		break;
		
		case TupleList::TYPE_INTEGER:
			if (intervalValue >= static_cast<int64_t>(INT32_MAX) + 1) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Interval value for integer type partitioning "
						"key column must be less than "
						<< (static_cast<int64_t>(INT32_MAX) + 1));
			}
		break;
		
		case TupleList::TYPE_LONG: {
			SyntaxTree::TablePartitionType
					partitioningType = getPartitioningType();

			if (partitioningType
					== SyntaxTree::TABLE_PARTITION_TYPE_RANGE) {
				if (intervalValue < TABLE_PARTITIONING_MIN_INTERVAL_VALUE) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval value for long type partitioning key column "
							"must be greater than or equal "
							<< TABLE_PARTITIONING_MIN_INTERVAL_VALUE);
				}
			}

			if (partitioningType
					== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
				int32_t limitValue =
						TABLE_PARTITIONING_MIN_INTERVAL_VALUE
								* createTableOption_.partitionInfoNum_;

				if (intervalValue < limitValue) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval value for long type partitioning key column "
							"must be greater than or equal "
							<< limitValue << "(" << TABLE_PARTITIONING_MIN_INTERVAL_VALUE
							<< "*" << createTableOption_.partitionInfoNum_ << ")");
				}
			}
		}
		break;
		
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Interval partitioning is not supported for specified column '"
					<< (*createTableOption_.columnInfoList_)
							[partitioningInfo_.partitioningColumnId_]
									->columnName_->name_->c_str()
					<< "' data type");
	}
}

void CreateTableProcessor::checkPartitioiningTypeValidation() {

	if (createTableOption_.isPartitioning()) {
		partitioningInfo_.partitioningNum_
				= createTableOption_.partitionInfoNum_;
		ColumnId partitioningColumnId
				= partitioningInfo_.partitioningColumnId_;
		SyntaxTree::TablePartitionType
				partitioningType = getPartitioningType();
	
		if (SyntaxTree::isInlcludeHashPartitioningType(
				partitioningType)) {

			if (createTableOption_.partitionInfoNum_ < 1
					|| createTableOption_.partitionInfoNum_ >
							TABLE_PARTITIONING_MAX_HASH_PARTITIONING_NUM) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARAMETER,
						"Hash partitioning count="
						<< createTableOption_.partitionInfoNum_
						<< " is out of range");
			}
		}
		
		if (SyntaxTree::isRangePartitioningType(
				partitioningType)) {
			if (!(*createTableOption_.columnInfoList_)
					[partitioningColumnId]->hasNotNullConstraint()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
					"Interval partitioning column='" 
					<<  (*createTableOption_.columnInfoList_)
							[partitioningColumnId]->columnName_->name_->c_str()
					<< "' must be specifed 'NOT NULL' constraint");
			}
			
			int64_t intervalValue = createTableOption_.optInterval_;
			TupleList::TupleColumnType columnType
					= (*createTableOption_.columnInfoList_)
							[partitioningColumnId]->type_;

			if (columnType == TupleList::TYPE_TIMESTAMP) {
				if (createTableOption_.optIntervalUnit_ !=
						util::DateTime::FIELD_DAY_OF_MONTH) {	
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_DDL_INVALID_PARAMETER,
									"You can only specify 'day' for interval time unit");
				}

				int32_t partitioningCount = 1;
				int64_t dayInterval = DAY_INTERVAL;
				if (dayInterval >
							(INT64_MAX / intervalValue / partitioningCount)) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_PROC_VALUE_OVERFLOW,
							"Interval value overflow as long value");
				}
				
				partitioningInfo_.intervalValue_
						= dayInterval* intervalValue;
				partitioningInfo_.intervalUnit_
						= static_cast<uint8_t>(
								util::DateTime::FIELD_DAY_OF_MONTH);
			}
			else {
				if (createTableOption_.optIntervalUnit_ != -1) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Interval time unit can be specified only"
							" when partitioning key column type is timestamp");

				}			
				checkIntervalLimit(intervalValue, columnType);
				partitioningInfo_.intervalValue_ = intervalValue;
			}
		}

		KeyConstraint keyConstraint
				= KeyConstraint::getNoLimitKeyConstraint();
		FullContainerKey metaContainerKey(
				alloc_,  keyConstraint, 0,
				tableName_.name_, static_cast<uint32_t>(strlen(tableName_.name_)));
		const FullContainerKeyComponents normalizedComponents =
				metaContainerKey.getComponents(alloc_, false);
		
		if (normalizedComponents.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER
				|| normalizedComponents.affinityStringSize_ > 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARTITIONING_TABLE_NAME,
					"Table partitioning and node affinity"
					" cannot be specified at the same time, "
					<< "tableName='" << tableName_.name_ << "'");
		}
	}
}

void CreateTableProcessor::checkPartitioiningColumnValidation() {
	
	if (createTableOption_.isPartitioning()) {
		const util::String &partitioningColumnName =
				*createTableOption_.partitionColumn_->name_;
		partitioningInfo_.partitionColumnName_
				= partitioningColumnName.c_str();
		bool isCaseSensitive
				= createTableOption_.partitionColumn_->nameCaseSensitive_;

		const util::String &normalizePartitiongColumnName
				= SQLUtils::normalizeName(
						alloc_, partitioningColumnName.c_str(), isCaseSensitive);
		
		for (int32_t i = 0; i < static_cast<int32_t>(
				createTableOption_.columnInfoList_->size()); i++) {
			const util::String normalizeColumnName =
				SQLUtils::normalizeName(alloc_,
					(*createTableOption_.columnInfoList_)[i]
							->columnName_->name_->c_str(), isCaseSensitive);

			if (normalizeColumnName.compare(
					normalizePartitiongColumnName) == 0) {
				partitioningInfo_.partitioningColumnId_ = i;
				break;
			}
		}

		if (partitioningInfo_.partitioningColumnId_ == UNDEF_COLUMNID) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
					"Partitioning column='" << partitioningColumnName
					<< "' is not exists on table='" << tableName_.name_ << "'");
		}
		partitioningInfo_.partitionColumnType_
				= (*createTableOption_.columnInfoList_)
				[partitioningInfo_.partitioningColumnId_]->type_;

		if (getPartitioningType() 
				== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
			if (createTableOption_.subPartitionColumn_ == NULL) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
						"SubPartitioning column is not specified on table='"
						<< tableName_.name_ << "'");
			}
			const util::String &subPartitioningColumnName =
					*createTableOption_.subPartitionColumn_->name_;
			partitioningInfo_.subPartitioningColumnName_
					= subPartitioningColumnName.c_str();

			isCaseSensitive
				= createTableOption_.subPartitionColumn_->nameCaseSensitive_;

			const util::String normalizeSubPartitiongColumnName =
					SQLUtils::normalizeName(
							alloc_, subPartitioningColumnName.c_str(), isCaseSensitive);
			for (int32_t i = 0; i < static_cast<int32_t>(
					createTableOption_.columnInfoList_->size()); i++) {
				const util::String normalizeColumnName =
					SQLUtils::normalizeName(alloc_,
						(*createTableOption_.columnInfoList_)[i]
								->columnName_->name_->c_str(), isCaseSensitive);
				if (normalizeColumnName.compare(
						normalizeSubPartitiongColumnName) == 0) {
					partitioningInfo_.subPartitioningColumnId_ = i;
					break;
				}
			}

			if (partitioningInfo_.subPartitioningColumnId_ == UNDEF_COLUMNID) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
						"SubPartitioning column='"
						<< subPartitioningColumnName
						<< "' is not exists on table='"
						<< tableName_.name_ << "'");
			}
		}
	}
}

void CreateTableProcessor::checkPrimaryKey() {

	NoSQLUtils::checkPrimaryKey(
			alloc_, createTableOption_, primaryKeyList_);

	if (createTableOption_.isPartitioning()) {

		if (primaryKeyList_.size() > 0) {
			bool find = false;
			for (size_t pos = 0; pos < primaryKeyList_.size(); pos++) {
				if (primaryKeyList_[pos]
						== partitioningInfo_.partitioningColumnId_) {
					find = true;
					break;
				}
			}

			if (find && partitioningInfo_.subPartitioningColumnId_
					!= UNDEF_PARTITIONID) {
				find = false;
				for (size_t pos = 0; pos < primaryKeyList_.size(); pos++) {
					if (primaryKeyList_[pos]
							== partitioningInfo_.subPartitioningColumnId_) {
						find = true;
						break;
					}
				}
			}

			if (!find) {
					bool isException = execution_->getExecutionManager()
							->isPartitioningRowKeyConstraint();
				if (!isException) {
					GS_TRACE_WARNING(SQL_SERVICE,
							GS_TRACE_SQL_DDL_TABLE_PARTITIONING_PRIMARY_KEY,
					"Partitioning keys must be included in primary keys");
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARTITION_COLUMN,
							"Partitioning keys must be included in primary keys");
				}
			}
		}
	}
}

bool CreateTableProcessor::checkPreIdempotency() {

	if (targetContainer_.isExists()) {
		if (!createTableOption_.ifNotExists_) {
			bool isValid = true;
			if ((targetContainer_.getContainerAttribute()
					== CONTAINER_ATTR_LARGE)) {
				isSameName_ = true;
				if (getPartitioningType()
						== SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
					isValid = false;
				}
			}
			else {
				isValid = false;
			}
			if (!isValid) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS,
						"Specified create table '" << tableName_.name_
						<< "' already exists");
			}
			idempotenceCheck_ = true;
		}
		else {
			return false;
		}
	}
	return true;
}

void CreateTableProcessor::putLargeContainer() {

	DataStore *dataStore = store_->getResourceSet()->getDataStore();

	NoSQLUtils::makeLargeContainerSchema(
			metaContainerSchema_,
			false, affinityStr_);

	NoSQLUtils::makeLargeContainerColumn(
			columnInfoList_);

	if (idempotenceCheck_) {
		
		NoSQLUtils::getTablePartitioningInfo(
				alloc_,
				dataStore,
				targetContainer_,
				columnInfoList_,
				checkPartitioningInfo_,
				option_);

		util::XArray<uint8_t> subContainerSchema(alloc_);
		store_->getLargeBinaryRecord(
				alloc_, 
				NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
				targetContainer_,
				columnInfoList_,
				subContainerSchema,
				option_);
				
		if (!checkPartitioningInfo_.checkSchema(
				partitioningInfo_, containerSchema_, subContainerSchema)) {
			GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS,
						"Specified create table '" 
						<< tableName_.name_ << "' already exists");
		}
		currentPartitioningInfo_ = &checkPartitioningInfo_;

		store_->getLargeRecord<TablePartitioningIndexInfo>(
				alloc_,
				NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
				targetContainer_,
				columnInfoList_,
				tablePartitioningIndexInfo_,
				option_);
	}
	else {

		ClusterStatistics clusterStat(
				alloc_,
				store_->getResourceSet()->getPartitionTable(),
				partitioningInfo_.partitioningNum_,
				genSeed());

		partitioningInfo_.setAssignInfo(
				alloc_,
				clusterStat,
				createTableOption_.partitionType_,
				targetContainer_.getPartitionId());

		util::XArray<uint8_t> partitioningInfoBinary(alloc_);
		util::XArrayByteOutStream outStream =
				util::XArrayByteOutStream(
						util::XArrayOutStream<>(partitioningInfoBinary));
		util::ObjectCoder().encode(outStream, partitioningInfo_);
		
		util::XArray<uint8_t> fixedPart(alloc_);
		util::XArray<uint8_t> varPart(alloc_);
		OutputMessageRowStore outputMrs(
				dataStore->getValueLimitConfig(),
				&columnInfoList_[0],
				static_cast<uint32_t>(columnInfoList_.size()),
				fixedPart,
				varPart, 
				false);

		NoSQLUtils::checkSchemaValidation(
				alloc_, dataStore->getValueLimitConfig(),
				tableName_.name_,
				metaContainerSchema_, COLLECTION_CONTAINER);

		baseInfo_->startSync(
				PUT_CONTAINER, targetContainer_.getContainerKey());

		TableExpirationSchemaInfo info;
		store_->putContainer(
				info,
				metaContainerSchema_,
				0,
				COLLECTION_CONTAINER,
				CONTAINER_ATTR_LARGE,
				false,
				targetContainer_, 
				&containerSchema_,
				&partitioningInfoBinary,
				option_);

		baseInfo_->endSync();
	}
				
	currentPartitioningInfo_->largeContainerId_
			= targetContainer_.getContainerId();
}

TableSchemaInfo *CreateTableProcessor::putSubContainers() {

	PartitionTable *pt
			= store_->getResourceSet()->getPartitionTable();
	SQLVariableSizeGlobalAllocator &varAlloc = store_->getAllocator();

	size_t currentPartitioningCount
			= currentPartitioningInfo_->getCurrentPartitioningCount();
	baseInfo_->updateAckCount(currentPartitioningCount);

	size_t subContainerId;
	NoSQLContainer *currentContainer = NULL;
	TableSchemaInfo *schemaInfo = NULL;		
	util::AllocVector<ColumnId> *columnIdList = NULL;

	try {
		schemaInfo = ALLOC_VAR_SIZE_NEW(varAlloc) TableSchemaInfo(varAlloc);
		schemaInfo->tableName_ = tableName_.name_;

		if (createTableOption_.isPartitioning()) {

			try {

				for (subContainerId = 0; subContainerId <
						currentPartitioningCount; subContainerId++) {
					if (subContainerId != 0 
						&& currentPartitioningInfo_->assignStatusList_[subContainerId]
								== INDEX_STATUS_DROP_START) {
						continue;
					}

					currentContainer = NoSQLUtils::createNoSQLContainer(
							ec_,
							tableName_,
							currentPartitioningInfo_->largeContainerId_ ,
							currentPartitioningInfo_->assignNumberList_[subContainerId],
							execution_);

					store_->getContainer(*currentContainer, false, option_);
					if (currentContainer->isExists()) {
					}
					else {
						TableExpirationSchemaInfo info;
						currentPartitioningInfo_->checkTableExpirationSchema(
								info, subContainerId);

						store_->createSubContainer(
								info,
								alloc_,
								targetContainer_,
								currentContainer,
								containerSchema_,
								*currentPartitioningInfo_,
								tablePartitioningIndexInfo_,
								tableName_,
								pt->getPartitionNum(),
								currentPartitioningInfo_->assignNumberList_[subContainerId]);
					}

					schemaInfo->setContainerInfo(
							subContainerId, *currentContainer);
					if (subContainerId == 0) {
						schemaInfo->setOptionList(optionList_);
					}
					
					if (currentPartitioningInfo_->partitionType_
							!= SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
						schemaInfo->affinityMap_.insert(
								std::make_pair(
										currentPartitioningInfo_->assignNumberList_[
												subContainerId], subContainerId));
					}

					schemaInfo->containerInfoList_.back().affinity_
							= currentPartitioningInfo_
									->assignNumberList_[subContainerId];

					ALLOC_DELETE(alloc_, currentContainer);
					currentContainer = NULL;
				}

				store_->getLargeRecord<TablePartitioningInfo<
						SQLVariableSizeGlobalAllocator> >(
								alloc_,
								NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
								targetContainer_,
								columnInfoList_,
								schemaInfo->getTablePartitioningInfo(),
								option_);

			}
			catch (std::exception &e) {
				if (subContainerId == 0) {
					targetContainer_.dropContainer(option_);
				}
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}
		}
		else {
			currentContainer = ALLOC_NEW(alloc_) NoSQLContainer(
					ec_, tableName_, execution_->getContext().getSyncContext(),
					execution_);

			baseInfo_->startSync(
					DROP_CONTAINER, currentContainer->getContainerKey());
		
			TableExpirationSchemaInfo info;
			store_->putContainer(
					info, containerSchema_,
					0,
					currentPartitioningInfo_->containerType_,
					CONTAINER_ATTR_SINGLE,
					false,
					*currentContainer,
					NULL,
					NULL,
					option_);

			baseInfo_->endSync();

			schemaInfo->containerAttr_ = CONTAINER_ATTR_SINGLE;
			schemaInfo->setContainerInfo(0, *currentContainer);
			ALLOC_DELETE(alloc_, currentContainer);
			currentContainer = NULL;
			schemaInfo->setOptionList(optionList_);
		}
		if (primaryKeyList_.size() > 0) {
			if (primaryKeyList_.size() == 1) {
				schemaInfo->setPrimaryKeyIndex(primaryKeyList_[0]);
			}
		}
		else {
			columnIdList
				= ALLOC_VAR_SIZE_NEW(varAlloc)
						util::AllocVector<ColumnId>(varAlloc);

			for (size_t columnPos = 0;
					columnPos < primaryKeyList_.size(); columnPos++) {
				columnIdList->push_back(primaryKeyList_[columnPos]);
			}
			schemaInfo->compositeColumnIdList_.push_back(columnIdList);
		}
		return schemaInfo;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void CreateTableProcessor::analyzeInitial() {

	partitioningInfo_.containerType_
			= (createTableOption_.isTimeSeries()) 
					? TIME_SERIES_CONTAINER : COLLECTION_CONTAINER;

	partitioningInfo_.partitionType_
			= static_cast<uint8_t>(createTableOption_.partitionType_);

	partitioningInfo_.partitioningNum_
			= createTableOption_.partitionInfoNum_;

	NoSQLUtils::getAffinityValue(
			createTableOption_, affinityStr_);
}

CreateTableProcessor::CreateTableProcessor(
		EventContext &ec,
		NoSQLStore *store,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) :
				TableProcessor(
						ec,
						store,
						execution,
						dbName,
						tableName,
						baseInfo),
				createTableOption_(createTableOption),
				checkPartitioningInfo_(alloc_),
				primaryKeyList_(alloc_),
				currentPartitioningInfo_(&partitioningInfo_),
				tablePartitioningIndexInfo_(alloc_),
				containerSchema_(alloc_),
				optionList_(alloc_),
				metaContainerSchema_(alloc_),
				affinityStr_(alloc_),
				columnInfoList_(alloc_),
				idempotenceCheck_(false),
				isValid_(true),
				isSameName_(false) {

	if (dbName.isCaseSensitive_) {
		option_.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option_.caseSensitivity_.setContainerNameCaseSensitive();
	}

	analyzeInitial();

	NoSQLUtils::makeNormalContainerSchema(
			alloc_,
			createTableOption,
			containerSchema_,
			optionList_,
			partitioningInfo_);

	NoSQLUtils::checkSchemaValidation(
			alloc_,
			store_->getResourceSet()->getDataStore()->getValueLimitConfig(),
			tableName.name_,
			containerSchema_,
			partitioningInfo_.containerType_);

	bool isPartitioning = createTableOption.isPartitioning();

	store_->getContainer(targetContainer_, false, option_);

	if (checkPreIdempotency()) {

		checkPartitioiningColumnValidation();

		checkPrimaryKey();
		
		checkPartitioiningTypeValidation();

		checkExpirationValidation();

		if (isPartitioning) {

			putLargeContainer();
		}

		TableSchemaInfo *schemaInfo = putSubContainers();

		SQLVariableSizeGlobalAllocator &varAlloc
				= store_->getAllocator();

		SQLString key(varAlloc);
		NoSQLUtils::normalizeString(key, tableName_.name_);

		store_->putCacheEntry<
				SQLTableCacheEntry,
				SQLString,
				TableSchemaInfo*>(key, schemaInfo, false);

		if (isSameName_) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS_WITH_EXECUTION,
					"Specified create table '" << tableName_.name_ << "' already exists");
		}
	}
}

int64_t CreateTableProcessor::genSeed() {

	int64_t seed
			= util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
	ClientId &clientId = execution_->getContext().getId();
	const uint32_t crcValue1 = util::CRC32::calculate(
			tableName_.name_, strlen(tableName_.name_));
	const uint32_t crcValue2 = util::CRC32::calculate(
			clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	seed += (crcValue1 + crcValue2
			+ clientId.sessionId_ + baseInfo_->execId_);
	return seed;
}

void NoSQLStore::createTable(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) {

	DDLCommandType commandType = DDL_CREATE_TABLE;

	util::StackAllocator &alloc = ec.getAllocator();

	try {

		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);

		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, NULL);

		checkExecutable(
				alloc, commandType, execution, false, contidion);

		CreateTableProcessor createTableProcessor(
				ec,
				this,
				execution,
				dbName,
				tableName,
				createTableOption,
				baseInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::createView(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) {

	DDLCommandType commandType = DDL_CREATE_VIEW;
	TableSchemaInfo *schemaInfo = NULL;
	NoSQLStoreOption option(execution);
	util::StackAllocator &alloc = ec.getAllocator();

	DataStore *dataStore = resourceSet_->getDataStore();
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	bool appendCache = false;

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(
				alloc, commandType, execution, false, contidion);

		util::XArray<uint8_t> containerSchema(alloc);
		util::XArray<uint8_t> metaContainerSchema(alloc);
		util::String affinityStr(alloc);

		NoSQLUtils::getAffinityValue(
				createTableOption, affinityStr);
		NoSQLUtils::makeLargeContainerSchema(
				metaContainerSchema, true, affinityStr);

		util::XArray<ColumnInfo> columnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(columnInfoList);

		NoSQLContainer targetContainer(ec,
				tableName,
				execution->getContext().getSyncContext(), execution);	
		getContainer(targetContainer, false, option);
		
		if (targetContainer.isExists()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_TABLE_ALREADY_EXISTS,
					"Specified create view '"
					<< tableName.name_ << "' already exists");
		}

		schemaInfo = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TableSchemaInfo(globalVarAlloc_);

		util::XArray<uint8_t> partitioningInfoBinary(alloc);

		baseInfo->startSync(
				PUT_CONTAINER, targetContainer.getContainerKey());
		
		TableExpirationSchemaInfo info;
		putContainer(
				info,
				metaContainerSchema,
				0,
				COLLECTION_CONTAINER,
				CONTAINER_ATTR_VIEW,
				false,
				targetContainer, 
				&containerSchema,
				&partitioningInfoBinary,
				option);
		
		baseInfo->endSync();

		NoSQLContainer viewContainer(ec,
				tableName,
				execution->getContext().getSyncContext(),
				execution);
		getContainer(viewContainer, false, option);

		if (targetContainer.isExists()) {
			if (!targetContainer.isViewContainer()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_MISMATCH_CONTAINER_ATTRIBUTE,
					"Target table must be VIEW, but current is TABLE");
			}

			util::XArray<uint8_t> viewInfoBinary(alloc);
			util::XArrayByteOutStream outStream =
					util::XArrayByteOutStream(
							util::XArrayOutStream<>(viewInfoBinary));
			
			ViewInfo viewInfo(alloc);
			if (createTableOption.optionString_ != NULL) {
				viewInfo.sqlString_ = *createTableOption.optionString_;
			}
			util::ObjectCoder().encode(outStream, viewInfo);

			util::XArray<uint8_t> fixedPart(alloc);
			util::XArray<uint8_t> varPart(alloc);
			OutputMessageRowStore outputMrs(
					dataStore->getValueLimitConfig(),
					&columnInfoList[0],
					static_cast<uint32_t>(columnInfoList.size()),
					fixedPart,
					varPart,
					false);

			VersionInfo versionInfo;
			NoSQLUtils::makeLargeContainerRow(
					alloc, NoSQLUtils::LARGE_CONTAINER_KEY_VERSION,
					outputMrs, versionInfo);
			NoSQLUtils::makeLargeContainerRow(
					alloc, NoSQLUtils::LARGE_CONTAINER_KEY_VIEW_INFO,
					outputMrs, viewInfo);

			NoSQLStoreOption localOption(execution);
			viewContainer.putRowSet(fixedPart, varPart, 2, localOption);

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
	catch (std::exception &e) {
		removeCache(tableName.name_);
		if (!appendCache) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, schemaInfo);
		}
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropView(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		bool ifExists) {

	DDLCommandType commandType = DDL_DROP_VIEW;
	NoSQLStoreOption option(execution);
	SQLService *sqlService = resourceSet_->getSQLService();
	
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, NULL);

		NoSQLContainer targetContainer(
				ec,
				tableName,
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);

		if (targetContainer.isExists()) {
			if (!targetContainer.isViewContainer()) {
				if (!ifExists) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
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
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_VIEW_NOT_EXISTS,
						"Specified view '"
						<< tableName.name_ << "' is not exists");
			}
		}

		SQLExecution::updateRemoteCache(
				ec,
				sqlService->getEE(),
				resourceSet_,
				execution->getContext().getDBId(),
				execution->getContext().getDBName(),
				tableName.name_,
				MAX_TABLE_PARTITIONING_VERSIONID);
	}
	catch (std::exception &e) {
		removeCache(tableName.name_);
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropTable(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		bool ifExists, DDLBaseInfo *baseInfo) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_TABLE;
	NoSQLStoreOption option(execution);
	DataStore *dataStore = resourceSet_->getDataStore();
	SQLService *sqlService = resourceSet_->getSQLService();
	
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	TablePartitioningInfo<util::StackAllocator>
			partitioningInfo(alloc);
	
	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, NULL);

		NoSQLContainer targetContainer(
				ec, tableName,
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);

		if (!targetContainer.isExists()) {
			if (!ifExists) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
						"Specified drop table '"
						<< tableName.name_ << "' is not exists");
			}
			else {
				baseInfo->finish();
				return;
			}
		}
		checkWritableContainer(targetContainer);

		checkViewContainer(targetContainer);

		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(
				baseInfo, option.caseSensitivity_);

		if (targetContainer.isLargeContainer()) {

			baseInfo->largeContainerId_
					= targetContainer.getContainerId();

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);
			
			NoSQLUtils::getTablePartitioningInfo(
					alloc,
					dataStore,
					targetContainer,
					columnInfoList,
					partitioningInfo,
					option);

			targetContainer.dropContainer(option);

			int32_t currentPartitioningCount = 0;
			for (size_t pos = 0;
					pos < partitioningInfo.assignNumberList_.size(); pos++) {
				if (partitioningInfo.assignStatusList_[pos]
						!= PARTITION_STATUS_DROP_START) {
					currentPartitioningCount++;
				}
			}

			if (currentPartitioningCount == 0) {
				baseInfo->startSync(
						DROP_CONTAINER, targetContainer.getContainerKey());
				targetContainer.dropContainer(option);
				baseInfo->endSync();
				
				baseInfo->finish();
				return;
			}

			baseInfo->updateAckCount(currentPartitioningCount);
			NoSQLContainer *subContainer = NULL;
			uint32_t assignNo = 0;

			for (uint32_t subContainerId = 0;
					subContainerId < partitioningInfo.assignNumberList_.size();
					subContainerId++) {

				if (partitioningInfo.assignStatusList_[subContainerId]
						== PARTITION_STATUS_DROP_START) {
					continue;
				}

				subContainer = NoSQLUtils::createNoSQLContainer(ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);

				try {
					getContainer(*subContainer, false, option);
				}
				catch (std::exception &) {
				}
				
				subOption.subContainerId_ = assignNo;
				baseInfo->processor_->setAckStatus(
						subContainer, assignNo, ACK_STATUS_ON);

				subContainer->dropContainer(subOption);
				assignNo++;
			}
		}
		else {
			baseInfo->updateAckCount(1);
			baseInfo->processor_->setAckStatus(
					&targetContainer, 0, ACK_STATUS_ON);
			subOption.subContainerId_ = 0;

			targetContainer.dropContainer(subOption);
		}

		removeCache(tableName.name_);
		SQLExecution::SQLExecutionContext &sqlContext
				= execution->getContext();

		SQLExecution::updateRemoteCache(
				ec,
				sqlService->getEE(),
				resourceSet_,
				sqlContext.getDBId(),
				sqlContext.getDBName(),
				tableName.name_,
				MAX_TABLE_PARTITIONING_VERSIONID);
	}
	catch (std::exception &e) {
		removeCache(tableName.name_);
		GS_RETHROW_USER_OR_SYSTEM(
				e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::createIndex(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		CreateIndexOption &indexOption,
		DDLBaseInfo *baseInfo) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_CREATE_INDEX;
	NoSQLStoreOption option(execution);
	DataStore *dataStore = resourceSet_->getDataStore();
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

	CreateIndexInfo *createIndexInfo
			= static_cast<CreateIndexInfo*>(baseInfo);

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, &indexName);
		checkExecutable(
				alloc, commandType, execution, false, contidion);

		uint32_t partitionNum 
				= resourceSet_->getPartitionTable()->getPartitionNum();

		NoSQLContainer targetContainer(
				ec,
				NameWithCaseSensitivity(tableName.name_,
				tableName.isCaseSensitive_),
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);

		checkExistContainer(targetContainer);

		checkViewContainer(targetContainer);

		checkWritableContainer(targetContainer);

		TablePartitioningInfo<util::StackAllocator>
				partitioningInfo(alloc);		

		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);
		subOption.ifNotExistsOrIfExists_ = indexOption.ifNotExists_;

		util::Vector<ColumnId> indexColumnIdList(alloc);
		util::Vector<util::String> indexColumnNameStrList(alloc);
		util::Vector<NameWithCaseSensitivity> indexColumnNameList(alloc);

		for (size_t pos = 0;
				pos < indexOption.columnNameList_->size(); pos++) {
			const NameWithCaseSensitivity columnName(
					(*(indexOption.columnNameList_))[pos]->name_->c_str(),
					(*(indexOption.columnNameList_))[pos]->nameCaseSensitive_);
			indexColumnNameList.push_back(columnName);
			indexColumnNameStrList.push_back(
					util::String(columnName.name_, alloc));
			if (columnName.isCaseSensitive_) {
				option.caseSensitivity_.setColumnNameCaseSensitive();
			}
		}

		NoSQLContainer *currentContainer = NULL;
		
		if (targetContainer.isLargeContainer()) {
			baseInfo->largeContainerId_ = targetContainer.getContainerId();
			util::XArray<ColumnInfo> largeColumnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);

			NoSQLUtils::getLargeContainerInfo<
					TablePartitioningInfo<util::StackAllocator>>(
							alloc,
							NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
							dataStore,
							targetContainer,
							largeColumnInfoList,
							partitioningInfo);

			size_t currentPartitioningCount
					= partitioningInfo.getActiveContainerCount();
			if (currentPartitioningCount == 0) {
				baseInfo->finish();
				return;
			}

			currentContainer = NoSQLUtils::createNoSQLContainer(
					ec,
					tableName,
					partitioningInfo.largeContainerId_,
					partitioningInfo.assignNumberList_[0],
					execution);
			getContainer(*currentContainer, false, option);

			TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
			util::XArray<uint8_t> containerSchema(alloc);
			bool isFirst = true;

			if (!currentContainer->isExists()) {
				isFirst = false;
				TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
				util::XArray<uint8_t> containerSchema(alloc);

				getLargeRecord<TablePartitioningIndexInfo>(
						alloc,
						NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
						targetContainer,
						largeColumnInfoList,
						tablePartitioningIndexInfo,
						option);

				getLargeBinaryRecord(
						alloc,
						NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
						targetContainer,
						largeColumnInfoList,
						containerSchema,
						option);

				TableExpirationSchemaInfo info;
				createSubContainer(
						info,
						alloc,
						targetContainer,
						currentContainer,
						containerSchema,
						partitioningInfo,
						tablePartitioningIndexInfo,
						tableName,
						partitionNum,
						partitioningInfo.assignNumberList_[0]);

				NoSQLUtils::dumpRecoverContainer(
						alloc, tableName.name_, currentContainer);
			}

			for (size_t pos = 0; pos < indexColumnNameList.size(); pos++) {
				ColumnId indexColId = currentContainer->getColumnId(
						alloc, indexColumnNameList[pos]);
				if (indexColId == UNDEF_COLUMNID) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INDEX_NOT_EXISTS,
							"Specified column '" << indexColumnNameList[pos].name_
							<< "' is not exists");
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
			NoSQLUtils::execPartitioningOperation(
					ec,
					db_,
					INDEX_STATUS_CREATE_START,
					execStatus,
					targetContainer,
					UNDEF_NODE_AFFINITY_NUMBER,
					UNDEF_NODE_AFFINITY_NUMBER,
					retryOption,
					partitioningInfo, 
					tableName,
					execution,
					targetContainerInfo);

			if (targetContainer.createLargeIndex(
					indexName.name_,
					indexColumnNameStrList, 
					MAP_TYPE_DEFAULT,
					indexColumnIdList,
					option)) {
				createIndexInfo->isSameName_ = true;
			}

			uint32_t subContainerId = 0;
			util::String normalizedIndexName
					= SQLUtils::normalizeName(alloc, indexName.name_);

			for (subContainerId = 0;
					subContainerId < currentPartitioningCount; subContainerId++) {
				
				currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);
				getContainer(*currentContainer, false, option);
				
				if (!currentContainer->isExists() 
						&& partitioningInfo.assignStatusList_[subContainerId]
								!= PARTITION_STATUS_DROP_START) {

					if (isFirst) {
						getLargeRecord<TablePartitioningIndexInfo>(
								alloc,
								NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
								targetContainer,
								largeColumnInfoList,
								tablePartitioningIndexInfo,
								option);

						getLargeBinaryRecord(
								alloc,
								NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
								targetContainer,
								largeColumnInfoList,
								containerSchema,
								option);

						if (containerSchema.size() == 0) {
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
						}
						isFirst = false;
					}

					TableExpirationSchemaInfo info;
					partitioningInfo.checkTableExpirationSchema(info, subContainerId);
					createSubContainer(
							info,
							alloc,
							targetContainer,
							currentContainer,
							containerSchema,
							partitioningInfo,
							tablePartitioningIndexInfo,
							tableName,
							partitionNum,
							partitioningInfo.assignNumberList_[subContainerId]);

					NoSQLUtils::dumpRecoverContainer(
							alloc, tableName.name_, currentContainer);
				}
			}
			try {
				NoSQLStoreOption cmdOption(execution);
				cmdOption.isSync_ = true;
				cmdOption.ifNotExistsOrIfExists_ = true;	
				currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[0],
						execution);
				getContainer(*currentContainer, false, option);

				currentContainer->createIndex(
						indexName.name_,
						MAP_TYPE_DEFAULT,
						indexColumnIdList,
						cmdOption);
			}
			catch (std::exception &e) {
				
				LargeExecStatus execStatus(alloc);
				IndexInfo indexInfo(alloc);
				indexInfo.indexName_ = indexName.name_;

				targetContainer.updateLargeContainerStatus(
						INDEX_STATUS_DROP_END,
						UNDEF_NODE_AFFINITY_NUMBER,
						option,
						execStatus,
						&indexInfo);
				GS_RETHROW_USER_ERROR(e, "");
			}


			baseInfo->updateAckCount(currentPartitioningCount);
			subOption.ifNotExistsOrIfExists_ = true;
			for (uint32_t subContainerId = 0;
					subContainerId < currentPartitioningCount; subContainerId++) {
				currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);
				getContainer(*currentContainer, false, option);
				
				baseInfo->processor_->setAckStatus(
						currentContainer, subContainerId, ACK_STATUS_ON);
				subOption.subContainerId_ = subContainerId;
				currentContainer->createIndex(
						indexName.name_,
						MAP_TYPE_DEFAULT,
						indexColumnIdList,
						subOption);
			}
		}
		else {
			for (size_t pos = 0; pos < indexColumnNameList.size(); pos++) {
				ColumnId indexColId = targetContainer.getColumnId(
						alloc, indexColumnNameList[pos]);
				if (indexColId == UNDEF_COLUMNID) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INDEX_NOT_EXISTS,
							"Specified column '" 
							<< indexColumnNameList[pos].name_ << "' is not exists");
				}
				indexColumnIdList.push_back(indexColId);
			}
			baseInfo->updateAckCount(1);
			baseInfo->processor_->setAckStatus(
					&targetContainer, 0, ACK_STATUS_ON);

			if (execution->getContext().isRetryStatement()) {
				subOption.ifNotExistsOrIfExists_ = true;
			}
			subOption.subContainerId_ = 0;
			targetContainer.createIndex(indexName.name_,
					MAP_TYPE_DEFAULT, indexColumnIdList, subOption);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::createIndexPost(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) {

	util::StackAllocator &alloc = ec.getAllocator();

	DDLCommandType commandType = DDL_CREATE_INDEX;
	CreateIndexInfo *createIndexInfo
			= static_cast<CreateIndexInfo*>(baseInfo);

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
		TableLatch latch(
				ec, db_, execution, dbName, tableName, false);

		NoSQLContainer targetContainer(ec,
				NameWithCaseSensitivity(
						tableName.name_, tableName.isCaseSensitive_),
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);

		if (!targetContainer.isExists()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
					"Specified table '" << tableName.name_ << "' is already dropped");
		}

		checkViewContainer(targetContainer);

		if (targetContainer.isLargeContainer()) {
			if (baseInfo->largeContainerId_
					!= targetContainer.getContainerId()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
						"Target large container '"
						<< tableName.name_ << "'is already removed");
			}

			LargeExecStatus execStatus(alloc);
			IndexInfo indexInfo(alloc);
			indexInfo.indexName_ = indexName.name_;
			
			targetContainer.updateLargeContainerStatus(
					INDEX_STATUS_CREATE_END,
					UNDEF_NODE_AFFINITY_NUMBER,
					option,
					execStatus,
					&indexInfo);

				TableLatch latch(
						ec, db_, execution, dbName, tableName, false);
		}
		if (createIndexInfo->isSameName_) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS_WITH_EXECUTION,
					"Specified table '" << tableName.name_
					<< "', indexName '" << indexName.name_ << "' already exists");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropIndex(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		bool ifExists, DDLBaseInfo *baseInfo) {

	util::StackAllocator &alloc = ec.getAllocator();
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
	
	DataStore *dataStore = resourceSet_->getDataStore();

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, &indexName);
		checkExecutable(
				alloc, commandType, execution, false,contidion);
		
		uint32_t partitionNum
				= resourceSet_->getPartitionTable()->getPartitionNum();

		NoSQLContainer targetContainer(ec,
				NameWithCaseSensitivity(
						tableName.name_, tableName.isCaseSensitive_),
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);

		if (!targetContainer.isExists()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
						"Specified create index table '"
						<< tableName.name_ << "' is not exists");
		}

		checkWritableContainer(targetContainer);

		checkViewContainer(targetContainer);

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);

		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);
		subOption.ifNotExistsOrIfExists_ = ifExists;

		if (targetContainer.isLargeContainer()) {
			baseInfo->largeContainerId_
					= targetContainer.getContainerId();
			util::XArray<ColumnInfo> largeColumnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
		
			NoSQLUtils::getLargeContainerInfo<TablePartitioningInfo<util::StackAllocator>>(
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					dataStore,
					targetContainer,
					largeColumnInfoList,
					partitioningInfo);

			size_t currentPartitioningCount
					= partitioningInfo.getActiveContainerCount();

			if (currentPartitioningCount == 0) {
				baseInfo->finish();
				return;
			}

			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = option.ifNotExistsOrIfExists_;
			LargeExecStatus execStatus(alloc);
			TargetContainerInfo targetContainerInfo;
			IndexInfo *indexInfo = &execStatus.indexInfo_;
			execStatus.indexInfo_.indexName_ = indexName.name_;

			targetContainer.updateLargeContainerStatus(
					INDEX_STATUS_DROP_START, 
					UNDEF_NODE_AFFINITY_NUMBER,
					retryOption,
					execStatus,
					indexInfo);

			baseInfo->updateAckCount(currentPartitioningCount);
			subOption.ifNotExistsOrIfExists_ = true;

			NoSQLContainer *currentContainer;
			TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
			util::XArray<uint8_t> containerSchema(alloc);
			bool isFirst = true;
			for (uint32_t subContainerId = 0;
					subContainerId < currentPartitioningCount; subContainerId++) {
				currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);
				getContainer(*currentContainer, false, option);
				
				if (!currentContainer->isExists() 
						&& partitioningInfo.assignStatusList_[subContainerId]
								!= PARTITION_STATUS_DROP_START) {

					if (isFirst) {

						getLargeRecord<TablePartitioningIndexInfo>(
								alloc,
								NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
								targetContainer,
								largeColumnInfoList,
								tablePartitioningIndexInfo,
								option);

						getLargeBinaryRecord(
								alloc,
								NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
								targetContainer,
								largeColumnInfoList, 
								containerSchema,
								option);

						isFirst = false;
					}

					TableExpirationSchemaInfo info;
					partitioningInfo.checkTableExpirationSchema(info, subContainerId);
					
					createSubContainer(
						info,
						alloc,
						targetContainer,
						currentContainer,
						containerSchema,
						partitioningInfo,
						tablePartitioningIndexInfo,
						tableName,
						partitionNum,
						partitioningInfo.assignNumberList_[subContainerId]);

						NoSQLUtils::dumpRecoverContainer(
								alloc, tableName.name_, currentContainer);
				}

				baseInfo->processor_->setAckStatus(
						currentContainer, subContainerId, ACK_STATUS_ON);
				subOption.subContainerId_ = subContainerId;

				currentContainer->dropIndex(indexName.name_, subOption);
			}
		}
		else {
			baseInfo->updateAckCount(1);
			baseInfo->processor_->setAckStatus(
					&targetContainer, 0, ACK_STATUS_ON);
			subOption.subContainerId_ = 0;
			
			targetContainer.dropIndex(indexName.name_, subOption);
		}
	} catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::dropIndexPost(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &indexName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = DDL_DROP_INDEX;
	const char8_t *connectedDbName = dbName.name_;
	adjustConnectedDb(connectedDbName);

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
		TableLatch latch(
				ec, db_, execution, dbName, tableName, false);

		NoSQLContainer targetContainer(
				ec,
				NameWithCaseSensitivity(
						tableName.name_, tableName.isCaseSensitive_),
				execution->getContext().getSyncContext(),
				execution);

		getContainer(targetContainer, false, option);
		
		if (!targetContainer.isExists()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
						"Specified table '"
						<< tableName.name_ << "' is already dropped");
		}

		checkViewContainer(targetContainer);

		if (targetContainer.isLargeContainer()) {
			if (baseInfo->largeContainerId_
					!= targetContainer.getContainerId()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
						"Target large container '"
						<< tableName.name_ << "'is already removed");
			}

			LargeExecStatus execStatus(alloc);
			IndexInfo indexInfo(alloc);
			indexInfo.indexName_ = indexName.name_;
			
			targetContainer.updateLargeContainerStatus(
					INDEX_STATUS_DROP_END,
					UNDEF_NODE_AFFINITY_NUMBER,
					option,
					execStatus,
					&indexInfo);
		}
		{
			TableLatch latch(
					ec, db_, execution, dbName, tableName, false);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

bool NoSQLStore::getTable(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		TableSchemaInfo *&schemaInfo,
		bool withCache,
		SQLTableCacheEntry *&entry, int64_t emNow) {

	util::StackAllocator &alloc = ec.getAllocator();
	DDLCommandType commandType = CMD_GET_TABLE;
	schemaInfo = NULL;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	bool isCreated = false;

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity
				connectedDbName(tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(
				alloc, commandType, execution, false, contidion);

		uint32_t partitionNum
				= resourceSet_->getPartitionTable()->getPartitionNum();

		SQLString key(globalVarAlloc_);
		NoSQLUtils::normalizeString(key, tableName.name_);
		
		if (withCache) {
			entry = getCacheEntry<SQLTableCacheEntry, SQLString>(key);

			if (entry) {
				schemaInfo = entry->value_;
				if (schemaInfo) {
					if (schemaInfo->disableCache_) {
						releaseCacheEntry<SQLTableCacheEntry>(entry);
					}
					else if (tableName.isCaseSensitive_ 
							&& strcmp(schemaInfo->tableName_.c_str(), tableName.name_)) {
						releaseCacheEntry<SQLTableCacheEntry>(entry);
					}
					else {
						return true;
					}
				}
			}
		}

		removeCache(tableName.name_);

		NoSQLContainer targetContainer(
				ec,
				tableName,
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);
		
		if (!targetContainer.isExists()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
						"Specified table '" << tableName.name_ << "' is not found");
		}

		schemaInfo = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TableSchemaInfo(globalVarAlloc_);
		isCreated = true;
		
		util::XArray<uint8_t> containerSchema(alloc);
		bool isPartitioning = 
				(targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE);
		
		bool isView = 
				(targetContainer.getContainerAttribute() == CONTAINER_ATTR_VIEW);

		if (isPartitioning) {
			schemaInfo->containerAttr_ = CONTAINER_ATTR_LARGE;
			util::XArray<ColumnInfo> columnInfoList(alloc);
			TablePartitioningInfo<SQLVariableSizeGlobalAllocator> &partitioningInfo
					= schemaInfo->getTablePartitioningInfo();
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);
			getLargeRecord<TablePartitioningInfo<SQLVariableSizeGlobalAllocator> >(
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					targetContainer,
					columnInfoList,
					partitioningInfo,
					option);

			NoSQLContainer *currentContainer = NULL;
			size_t currentPartitioningCount
					= partitioningInfo.getCurrentPartitioningCount();
			size_t actualPos = 0;
		
			NoSQLStoreOption option(execution);
			if (partitioningInfo.isTableExpiration()) {
				option.featureVersion_ = MessageSchema::V4_1_VERSION;
				option.acceptableFeatureVersion_ = MessageSchema::V4_1_VERSION;
			}
			for (size_t subContainerId = 0;
					subContainerId < currentPartitioningCount; subContainerId++) {
				if (partitioningInfo.assignStatusList_[subContainerId]
						!= PARTITION_STATUS_CREATE_END) {
					continue;
				}

				currentContainer = NULL;
				currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);

				try {
					currentContainer->getContainerInfo(option);
				} 
				catch (std::exception &e) {
					const util::Exception
							checkException = GS_EXCEPTION_CONVERT(e, "");
					GS_RETHROW_USER_ERROR(e, "");
				}

				if (!currentContainer->isExists()) {
					if ((partitioningInfo.partitionType_
							== SyntaxTree::TABLE_PARTITION_TYPE_HASH
							|| (SyntaxTree::isRangePartitioningType(partitioningInfo.partitionType_)
									&& (subContainerId == 0)))) {

						util::XArray<uint8_t> containerSchema(alloc);
						util::XArray<ColumnInfo> largeColumnInfoList(alloc);
						TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
						NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
						
						getLargeRecord<TablePartitioningIndexInfo>(
								alloc,
								NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
								targetContainer,
								largeColumnInfoList,
								tablePartitioningIndexInfo,
								option);

						getLargeBinaryRecord(
								alloc,
								NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
								targetContainer,
								largeColumnInfoList,
								containerSchema,
								option);

						TableExpirationSchemaInfo info;
						partitioningInfo.checkTableExpirationSchema(
								info, subContainerId);
					
						createSubContainer(
								info,
								alloc,
								targetContainer,
								currentContainer,
								containerSchema,
								partitioningInfo,
								tablePartitioningIndexInfo,
								tableName,
								partitionNum,
								partitioningInfo.assignNumberList_[subContainerId]);

						ALLOC_DELETE(alloc, currentContainer);

						currentContainer = NoSQLUtils::createNoSQLContainer(
								ec,
								tableName,
								partitioningInfo.largeContainerId_,
								partitioningInfo.assignNumberList_[subContainerId],
								execution);
						getContainer(*currentContainer, false, option);
						
						if (!currentContainer->isExists()) {
							util::String subNameStr(alloc);
							currentContainer->getContainerKey()->toString(
									alloc, subNameStr);
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
									"Table '" << tableName.name_ << "', partition '"
									<< subNameStr << "' is not found");
						}
						NoSQLUtils::dumpRecoverContainer(
								alloc, tableName.name_, currentContainer);
					}
					else {
						continue;
					}
				}
				TableContainerInfo *containerInfo
						= schemaInfo->setContainerInfo(actualPos, *currentContainer);
				if (actualPos == 0) {
					schemaInfo->containerType_
							= currentContainer->getContainerType();
				}

				if (partitioningInfo.partitionType_
						!= SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
					schemaInfo->affinityMap_.insert(
							std::make_pair(
									partitioningInfo.assignNumberList_[subContainerId],
									actualPos));
				}

				containerInfo->affinity_
						= partitioningInfo.assignNumberList_[subContainerId];
				actualPos++;
			}

			if (actualPos == 0) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
					"Specified table '" << tableName.name_ << "' no available partition");
			}
		}
		else {
			schemaInfo->setContainerInfo(0, targetContainer);
			schemaInfo->containerType_ = targetContainer.getContainerType();
			schemaInfo->containerAttr_ = targetContainer.getContainerAttribute();
		}
		if (isView) {
			schemaInfo->sqlString_ = targetContainer.getSQLString();
		}

		schemaInfo->tableName_ = tableName.name_;
		schemaInfo->lastExecutedTime_ = emNow;
		schemaInfo->disableCache_ = false;

		entry = putCacheEntry<
				SQLTableCacheEntry,
				SQLString,
				TableSchemaInfo*>(key, schemaInfo, true);

		if (schemaInfo->containerType_ == TIME_SERIES_CONTAINER) {
			schemaInfo->setPrimaryKeyIndex(
					ColumnInfo::ROW_KEY_COLUMN_ID);
		}

		isCreated = false;
		return false;
	}
	catch (std::exception &e) {
		if (isCreated && schemaInfo) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, schemaInfo);
		}
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore::begin() {
	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_DDL_FAILED,
			getCommandString(DDL_BEGIN) << " not supported");
}

void NoSQLStore::commit() {
	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_DDL_FAILED,
			getCommandString(DDL_COMMIT) << " not supported");
}

void NoSQLStore::rollback() {
	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_DDL_UNSUPPORTED_COMMAND_TYPE,
			getCommandString(DDL_ROLLBACK) << " not supported");
}

void NoSQLStore::getContainer(
		NoSQLContainer &container,
		bool isSystemDb,
		NoSQLStoreOption &option) {
	
	if (isSystemDb) {
		option.isSystemDb_ = true;
	}
	container.getContainerInfo(option);
}

void NoSQLStore::putContainer(
		TableExpirationSchemaInfo &info,
		util::XArray<uint8_t> &binarySchemaInfo,
		size_t containerPos,
		ContainerType containerType,
		ContainerAttribute containerAttr,
		bool modifiable,
		NoSQLContainer &container,
		util::XArray<uint8_t> *containerData,
		util::XArray<uint8_t> *binayData,
		NoSQLStoreOption &targetOption) {

	NoSQLStoreOption option;
	option.containerType_ = containerType;
	option.subContainerId_ = containerPos;
	option.containerAttr_ = containerAttr;
	option.caseSensitivity_ = targetOption.caseSensitivity_;
	
	if (containerAttr == CONTAINER_ATTR_LARGE) {
		container.putLargeContainer(
				binarySchemaInfo, false, option, containerData, binayData);
	}
	else {
		container.putContainer(
				info, binarySchemaInfo, modifiable, option);
	}
}

void NoSQLStore::dropTablePartition(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		DDLBaseInfo *baseInfo) {

	util::StackAllocator &alloc = ec.getAllocator();

	DDLCommandType commandType = DDL_DROP_PARTITION;
	NoSQLStoreOption option(execution);
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	DropTablePartitionInfo *dropPartitionTableInfo
			= static_cast<DropTablePartitionInfo*>(baseInfo);
	DataStore *dataStore = resourceSet_->getDataStore();

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(
				NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(alloc, commandType, execution, false,contidion);

		if (dropPartitionTableInfo->cmdOptionList_ != NULL
			&& dropPartitionTableInfo->cmdOptionList_->size() == 1) {
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Target partition constraints is not specifed");
		}

		NoSQLContainer targetContainer(
				ec,
				tableName, 
				execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);
		
		checkExistContainer(targetContainer);

		checkWritableContainer(targetContainer);

		checkViewContainer(targetContainer);

		if (!targetContainer.isLargeContainer()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARTITIONING_TABLE_TYPE,
					"Target table='" << tableName.name_
					<< "' must be partitioning table");
		}
		int64_t targetValue;
		const SyntaxTree::Expr& expr
				= *(*dropPartitionTableInfo->cmdOptionList_)[0];
		
		if (expr.value_.getType() == TupleList::TYPE_STRING) {
			util::String str(
					static_cast<const char*>(expr.value_.varData()),
					expr.value_.varSize(), alloc);
			targetValue = SQLUtils::convertToTime(str);
		}
		else if (expr.value_.getType() == TupleList::TYPE_LONG) {
			targetValue = expr.value_.get<int64_t>();
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Invalid drop partition constraint");
		}
		TableLatch latch(
				ec,
				db_,
				execution,
				dbName,
				tableName,
				false);

		util::XArray<ColumnInfo> largeColumnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
		
		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		NoSQLUtils::getLargeContainerInfo<TablePartitioningInfo<util::StackAllocator>>(
				alloc,
				NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				dataStore,
				targetContainer,
				largeColumnInfoList,
				partitioningInfo);

		partitioningInfo.init();

		TupleList::TupleColumnType patitioningColumnType
				= partitioningInfo.partitionColumnType_;

		if (expr.value_.getType() == TupleList::TYPE_STRING
			&& patitioningColumnType != TupleList::TYPE_TIMESTAMP) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
					"Invalid data format, specified partitioning column type "
					"must be timestamp");
		}

		if (expr.value_.getType() != TupleList::TYPE_STRING
				&& patitioningColumnType == TupleList::TYPE_TIMESTAMP) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR,
					"Invalid data format, specified partitioning column type "
					"must be numeric");
		}

		TupleValue *tupleValue = NULL;
		switch (patitioningColumnType) {
			
			case TupleList::TYPE_BYTE: 
			{
				if (static_cast<int64_t>(targetValue) > INT8_MAX
						|| static_cast<int64_t>(targetValue) < INT8_MIN) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Target value is over byte limit value");
				}
				int8_t value = static_cast<int8_t>(targetValue);
				tupleValue = ALLOC_NEW(alloc)
						TupleValue(&value, TupleList::TYPE_BYTE);
			}
			break;
			
			case TupleList::TYPE_SHORT:
			{
				if (static_cast<int64_t>(targetValue) > INT16_MAX
						|| static_cast<int64_t>(targetValue) < INT16_MIN) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Target value is over short limit value");
				}
				int16_t value = static_cast<int16_t>(targetValue);
				tupleValue = ALLOC_NEW(alloc)
						TupleValue(&value, TupleList::TYPE_SHORT);
			}
			break;
			
			case TupleList::TYPE_INTEGER:
			{
				if (static_cast<int64_t>(targetValue) > INT32_MAX
						|| static_cast<int64_t>(targetValue) < INT32_MIN) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PARAMETER,
							"Target value is over integer limit value");
				}
				int32_t value = static_cast<int32_t>(targetValue);
				tupleValue = ALLOC_NEW(alloc)
						TupleValue(&value, TupleList::TYPE_INTEGER);
			}
			break;

			case TupleList::TYPE_LONG:
			case TupleList::TYPE_TIMESTAMP:
			{
				int64_t value = static_cast<int64_t>(targetValue);
				tupleValue = ALLOC_NEW(alloc)
						TupleValue(&value, TupleList::TYPE_LONG);
			}
			break;
		}

		if (SyntaxTree::isRangePartitioningType(
				partitioningInfo.partitionType_)) {
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Table partitioning type must be interval or interval-hash");
		}

		baseInfo->largeContainerId_ = targetContainer.getContainerId();
		const ResourceSet *resourceSet = execution->getResourceSet();
		uint32_t partitionNum
				= resourceSet->getPartitionTable()->getPartitionNum();

		NodeAffinityNumber affinity;
		NoSQLStoreOption subOption(execution);
		subOption.setAsyncOption(baseInfo, option.caseSensitivity_);
		int64_t baseValue;
		NodeAffinityNumber baseAffinity;
		TargetContainerInfo targetContainerInfo;

		if (partitioningInfo.subPartitioningColumnId_ != UNDEF_COLUMNID) {
			partitioningInfo.getAffinityNumber(
					partitionNum,
					tupleValue,
					NULL,
					0,
					baseValue,
					baseAffinity);

			int32_t removedCount = 0;
			NodeAffinityNumber currentAffinity;
			util::Vector<NodeAffinityNumber> affinityList(alloc);
			
			for (size_t hashPos = 0;
					hashPos < partitioningInfo.partitioningNum_; hashPos++) {
				currentAffinity = baseAffinity + hashPos;
				size_t targetAffinityPos
						= partitioningInfo.findEntry(currentAffinity);

				if (targetAffinityPos != std::numeric_limits<size_t>::max()
					&& targetAffinityPos < partitioningInfo.assignStatusList_.size()) {
					if (partitioningInfo.assignStatusList_[targetAffinityPos]
								== PARTITION_STATUS_DROP_START) {
						removedCount++;
					}
				}
				affinityList.push_back(currentAffinity);
			}

			if (removedCount == static_cast<int32_t>(affinityList.size())) {
				TableLatch latch(
						ec, db_, execution, dbName, tableName, false);
				baseInfo->finish();
				return;
			}

			LargeExecStatus execStatus(alloc);
			IndexInfo *indexInfo = &execStatus.indexInfo_;
			option.sendBaseValue_ = true;
			option.baseValue_ = baseValue;
			
			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = true;

			targetContainer.updateLargeContainerStatus(
					PARTITION_STATUS_DROP_START, 
					baseAffinity,
					retryOption,
					execStatus,
					indexInfo);

			dropPartitionTableInfo->affinity_ = baseAffinity;
			baseInfo->updateAckCount(affinityList.size());
			
			for (size_t pos = 0; pos < affinityList.size(); pos++) {
				subOption.subContainerId_ = pos;
				NoSQLContainer *currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						affinityList[pos],
						execution);
				getContainer(*currentContainer, false, option);

				currentContainer->setContainerType(
						partitioningInfo.containerType_);
				baseInfo->processor_->setAckStatus(
						currentContainer, 0, ACK_STATUS_ON);
				currentContainer->dropContainer(subOption);
			}
		}
		else {
			affinity = partitioningInfo.getAffinityNumber(
					partitionNum,
					tupleValue,
					NULL,
					-1,
					baseValue,
					baseAffinity);

			size_t targetAffinityPos = partitioningInfo.findEntry(affinity);
			if (targetAffinityPos != std::numeric_limits<size_t>::max()
				&& partitioningInfo.assignStatusList_[targetAffinityPos]
						== PARTITION_STATUS_DROP_START) {
				baseInfo->finish();
				return;
			}

			LargeExecStatus execStatus(alloc);
			IndexInfo *indexInfo = &execStatus.indexInfo_;
			option.sendBaseValue_ = true;
			option.baseValue_ = baseValue;
			
			NoSQLStoreOption retryOption(execution);
			retryOption.ifNotExistsOrIfExists_ = true;
			
			targetContainer.updateLargeContainerStatus(
					PARTITION_STATUS_DROP_START, 
					affinity,
					retryOption,
					execStatus,
					indexInfo);

			dropPartitionTableInfo->affinity_ = affinity;
			baseInfo->updateAckCount(1);
			subOption.subContainerId_ = 0;

			NoSQLContainer *currentContainer = NoSQLUtils::createNoSQLContainer(
					ec,
					tableName,
					partitioningInfo.largeContainerId_,
					affinity,
					execution);
			getContainer(*currentContainer, false, option);
			
			currentContainer->setContainerType(
					partitioningInfo.containerType_);
			baseInfo->processor_->setAckStatus(
					currentContainer, 0, ACK_STATUS_ON);
			
			currentContainer->dropContainer(subOption);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void NoSQLStore:: dropTablePartitionPost(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName) {

	DDLCommandType commandType = DDL_DROP_PARTITION;
	try {
		TableLatch latch(
				ec, db_, execution, dbName, tableName, false);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}
void NoSQLStoreOption::setAsyncOption(
		DDLBaseInfo *baseInfo,
		StatementMessage::CaseSensitivity caseSensitive) {

	caseSensitivity_ = caseSensitive;
	isSync_ = false;
	execId_ = baseInfo->execId_;
	jobVersionId_ = baseInfo->jobVersionId_;
};

void NoSQLStore::getLargeBinaryRecord(
		util::StackAllocator &alloc,
		const char *key,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		util::XArray<uint8_t> &subContainerSchema,
		NoSQLStoreOption &option) {

	int64_t rowCount;
	bool hasRowKey = false;
	DataStore *dataStore = resourceSet_->getDataStore();

	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='" 
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION 
			<< "' OR key='" << key << "'";
	container.executeSyncQuery(queryStr.str().c_str(), option,
			fixedPart,  varPart, rowCount, hasRowKey);

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
	NoSQLUtils::decodeBinaryRow<util::XArray<uint8_t> >(
			rowStore, subContainerSchema, versionInfo, key);
}

void NoSQLStore::addColumn(
		EventContext &ec,
		SQLExecution *execution,
		const NameWithCaseSensitivity &dbName,
		const NameWithCaseSensitivity &tableName,
		CreateTableOption &createTableOption,
		DDLBaseInfo *baseInfo) {

	DDLCommandType commandType = DDL_ADD_COLUMN;
	NoSQLStoreOption option(execution);
	util::StackAllocator &alloc = ec.getAllocator();
	DataStore *dataStore = execution->getResourceSet()->getDataStore();
	if (dbName.isCaseSensitive_) {
		option.caseSensitivity_.setDatabaseNameCaseSensitive();
	}
	if (tableName.isCaseSensitive_) {
		option.caseSensitivity_.setContainerNameCaseSensitive();
	}
	AddColumnInfo *addColumnInfo = static_cast<AddColumnInfo*>(baseInfo);

	try {
		const char8_t *tmpDbName = dbName.name_;
		adjustConnectedDb(tmpDbName);
		NameWithCaseSensitivity connectedDbName(
				tmpDbName, dbName.isCaseSensitive_);
		ExecuteCondition contidion(NULL, &connectedDbName, &tableName, NULL);
		checkExecutable(
				alloc, commandType, execution, false,contidion);
		if (addColumnInfo->createTableOpt_->columnInfoList_ == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
		}

		NoSQLContainer targetContainer(
				ec,
				tableName, execution->getContext().getSyncContext(),
				execution);
		getContainer(targetContainer, false, option);
		
		checkExistContainer(targetContainer);

		checkWritableContainer(targetContainer);

		checkViewContainer(targetContainer);

		bool isPartitioning
				= (targetContainer.getContainerAttribute() == CONTAINER_ATTR_LARGE);
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
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					targetContainer,
					orgColumnInfoList,
					tablePartitioningIndexInfo,
					option);
			
			getLargeBinaryRecord(
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
					targetContainer,
					orgColumnInfoList,
					orgContainerSchema,
					option);
			
			getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					targetContainer,
					orgColumnInfoList,
					partitioningInfo,
					option);

			containerType = partitioningInfo.containerType_;
			bool isSuccess = false;
			bool isError = false;
			for (size_t subContainerId = 0;
					subContainerId < static_cast<size_t>(
							partitioningInfo.getCurrentPartitioningCount());
							subContainerId++) {

				if (partitioningInfo.assignStatusList_[subContainerId]
						== PARTITION_STATUS_DROP_START) {
					continue;
				}

				NoSQLContainer *currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(info, subContainerId);

				try {
					putContainer(
							info,
							orgContainerSchema,
							subContainerId, 
							containerType,
							CONTAINER_ATTR_SUB,
							true,
							*currentContainer,
							NULL,
							NULL,
							option);

					if (currentContainer != NULL) {
						containerList.push_back(currentContainer);
					}
					isSuccess = true;
				}
				catch (std::exception &e) {
					isError = true;
					UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
				}
			}
			
			if (isSuccess && isError) {
				isRecover = true;
			}
		}
		else {
			targetContainer.getContainerBinary(
					option, orgContainerSchema);
			containerType = targetContainer.getContainerType();
		}

		if (isRecover) {
			util::NormalOStringStream oss;
			oss << "Recover container schema=";
			for (size_t i = 0; i < containerList.size(); i++) {
				const FullContainerKey *containerKey
						= containerList[i]->getContainerKey();
				util::String containerName(alloc);
				containerKey->toString(alloc, containerName);
				oss << containerName;
				if (i != containerList.size() - 1) {
					oss << ",";
				}
			}
			GS_TRACE_WARNING(
					SQL_SERVICE,
					GS_TRACE_SQL_RECOVER_CONTAINER, oss.str().c_str());
			return;
		}

		util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(
						orgContainerSchema.data(), orgContainerSchema.size()));
		int32_t columnNum = 0;

		ContainerAttribute containerAttribute = CONTAINER_ATTR_SINGLE;

		if (!isPartitioning) {
			bool existFlag;
			StatementHandler::decodeBooleanData(in, existFlag);
			SchemaVersionId versionId;
			in >> versionId;
			ContainerId containerId;
			in >> containerId;
			util::XArray<uint8_t> containerName(alloc);
			StatementHandler::decodeVarSizeBinaryData(in, containerName);
			in >> columnNum;
				containerAttribute = CONTAINER_ATTR_SINGLE;
		}
		else {
			in >> columnNum;
			containerAttribute = CONTAINER_ATTR_SUB;
		}		
		int32_t newColumnNum
				= columnNum +  static_cast<int32_t>(
						(*createTableOption.columnInfoList_).size());
		out << newColumnNum;
		
		for (int32_t i = 0; i < columnNum; i++) {
			int32_t columnNameLen;
			in >> columnNameLen;
			out << columnNameLen;
			util::XArray<uint8_t> columnName(alloc);
			columnName.resize(static_cast<size_t>(columnNameLen));
			in >> std::make_pair(columnName.data(), columnNameLen);
			out << std::make_pair(columnName.data(), columnNameLen);
			int8_t tmp;
			in >> tmp;
			out << tmp;
			uint8_t opt;
			in >> opt;
			out << opt;
			optionList.push_back(opt);
		}
		
		for (size_t i = 0;
				i < (*createTableOption.columnInfoList_).size(); i++) {

			char *columnName = const_cast<char*>(
					(*createTableOption.columnInfoList_)[i]
							->columnName_->name_->c_str());

			int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
			out << columnNameLen;
			out << std::make_pair(columnName, columnNameLen);
			int8_t tmp = static_cast<int8_t>(
				SQLUtils::convertTupleTypeToNoSQLType(
							(*createTableOption.columnInfoList_)[i]->type_));
			out << tmp;
			uint8_t opt = 0;
			if ((*createTableOption.columnInfoList_)[i]
						->hasNotNullConstraint()) {
				ColumnInfo::setNotNull(opt);
			}
			out << opt;
			optionList.push_back(opt);
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
		
		if (in.base().remaining() > 0
				&& containerType == TIME_SERIES_CONTAINER) {
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
			NoSQLUtils::checkSchemaValidation(
					alloc,
					dataStore->getValueLimitConfig(),
					tableName.name_,
					newContainerSchema,
					targetContainer.getContainerType());

			int32_t errorPos = -1;
			for (size_t subContainerId = 0; subContainerId <
					static_cast<size_t>(partitioningInfo.getCurrentPartitioningCount());
					subContainerId++) {

				if (partitioningInfo.assignStatusList_[subContainerId]
					== PARTITION_STATUS_DROP_START) {
					continue;
				}

				NoSQLContainer *currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[subContainerId],
						execution);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(info, subContainerId);
				
				try {
					putContainer(
							info,
							newContainerSchema,
							subContainerId, 
							containerType,
							containerAttribute,
							true,
							*currentContainer,
							NULL,
							NULL,
							option);
				}
				catch (std::exception &e) {
					UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
					errorPos = static_cast<int32_t>(subContainerId);
				}
			}
			if (errorPos != -1) {
				NoSQLContainer *currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						partitioningInfo.assignNumberList_[errorPos],
						execution);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(info, errorPos);
				
				putContainer(
						info,
						newContainerSchema,
						errorPos, 
						containerType,
						containerAttribute,
						true,
						*currentContainer,
						NULL,
						NULL,
						option);
			}
			{
				util::XArray<ColumnInfo> newColumnInfoList(alloc);
				NoSQLUtils::makeLargeContainerColumn(newColumnInfoList);
				util::XArray<uint8_t> fixedPart(alloc);
				util::XArray<uint8_t> varPart(alloc);
				OutputMessageRowStore outputMrs(
						dataStore->getValueLimitConfig(),
						&newColumnInfoList[0],
						static_cast<uint32_t>(newColumnInfoList.size()),
						fixedPart,
						varPart,
						false);

				NoSQLUtils::makeLargeContainerRowBinary(
						NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
						outputMrs,
						newContainerSchema);

				NoSQLStoreOption option(execution);
				option.putRowOption_ = PUT_INSERT_OR_UPDATE;
				targetContainer.putRow(
						fixedPart, varPart, UNDEF_ROWID, option);
			}
		}
		else {
			NoSQLContainer *currentContainer =
					ALLOC_NEW(alloc) NoSQLContainer(
							ec,
							tableName,
							execution->getContext().getSyncContext(),
							execution);

			TableExpirationSchemaInfo info;
			putContainer(
					info,
					newContainerSchema,
					0, 
					containerType,
					CONTAINER_ATTR_SINGLE,
					true,
					*currentContainer,
					NULL,
					NULL,
					option);
		}
		TableLatch latch(
				ec, db_, execution, dbName, tableName, false);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, getCommandString(commandType)
				<< " failed (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}
