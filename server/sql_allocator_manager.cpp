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
#include "sql_allocator_manager.h"
#include "sql_processor_config.h"

const int32_t SQLAllocatorManager::
		DEFAULT_ALLOCATOR_BLOCK_SIZE_EXP = 20;

const int32_t SQLAllocatorManager::
		DEFAULT_WORK_MEMORY_LIMIT_MB = 128;

const int32_t SQLAllocatorManager::
		DEFAULT_WORK_CACHE_MEMORY_MB = 128;

const int32_t SQLAllocatorManager::
		DEFAULT_TOTAL_WORK_MEMORY_LIMIT_MB = 128;

const int32_t SQLAllocatorManager::
		DEFAULT_POOL_SIZE = 64 * 1024 * 1024;

SQLAllocatorManager::ConfigSetUpHandler
		SQLAllocatorManager::configSetUpHandler_;

/*!
	@brief Constructor
*/
SQLAllocatorManager::SQLAllocatorManager(
		ConfigTable &config,
		int32_t blockSizeExp) :
				config_(config),
				globalVarAlloc_(
						util::AllocatorInfo(
								ALLOCATOR_GROUP_SQL_WORK, "SQLGlobalAllocator")),
				blockSizeExp_(blockSizeExp),
				sqlConcurrency_(
						config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY)),
				txnConcurrency_(
						config.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY)),
				fixedAllocator_(
						util::AllocatorInfo(
								ALLOCATOR_GROUP_SQL_WORK, "SQLFixedSizeAllocator"),
								1 << blockSizeExp),
				stackAllocatorPool_(
						util::AllocatorInfo(
								ALLOCATOR_GROUP_SQL_WORK, "SQLStackAllocator")),
				localVarAllocatorPool_(
						util::AllocatorInfo(
								ALLOCATOR_GROUP_SQL_WORK, "SQLLocalVarAllocator")) {
	
	try {
		config_.setUpConfigHandler(config, this);

		resizeConcurrency(SQL_SERVICE, sqlConcurrency_);
		resizeConcurrency(TRANSACTION_SERVICE, txnConcurrency_);
		uint64_t maxCount = config_.workCacheMemorySize_
				/  static_cast<uint32_t>(1 << blockSizeExp_);
		fixedAllocator_.setFreeElementLimit(
				static_cast<size_t>(maxCount));
		
		int32_t limit = DEFAULT_POOL_SIZE;

		stackAllocatorPool_.setLimit(
				util::AllocatorStats::STAT_STABLE_LIMIT, limit);
		
		localVarAllocatorPool_.setLimit(
				util::AllocatorStats::STAT_STABLE_LIMIT,limit);

	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Destructor
*/
SQLAllocatorManager::~SQLAllocatorManager() {

	for (size_t pos = 0;
			pos < sqlLocalAllocatorList_.size(); pos++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, sqlLocalAllocatorList_[pos]);
	}

	for (size_t pos = 0;
			pos < txnLocalAllocatorList_.size(); pos++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, txnLocalAllocatorList_[pos]);
	}
}

SQLVariableSizeGlobalAllocator *SQLAllocatorManager::
		getGlobalAllocator() {

	return &globalVarAlloc_;
}

/*!
	@brief Get new stack allocator
*/
util::StackAllocator *SQLAllocatorManager::
		getStackAllocator(bool withPool) {

	try {
		SQLAllocatorGroupId moduleId
				= ALLOCATOR_GROUP_SQL_WORK;
		const char *moduleName = "Local stack allocator";

		if (withPool) {
			util::LockGuard<util::Mutex> guard(lock_);
			return UTIL_OBJECT_POOL_NEW(stackAllocatorPool_)
					util::StackAllocator(
							util::AllocatorInfo(
									moduleId, moduleName), &fixedAllocator_);
		}
		else {
			return ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
					util::StackAllocator(
							util::AllocatorInfo(
									moduleId, moduleName), &fixedAllocator_);
		}
		return NULL;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Release stack allocator
*/
void SQLAllocatorManager::releaseStackAllocator(
		util::StackAllocator *alloc, bool withPool) {

	try {
		if (withPool) {
			
			util::LockGuard<util::Mutex> guard(lock_);
			
			UTIL_OBJECT_POOL_DELETE(stackAllocatorPool_, alloc);
		}
		else {
			if (alloc == NULL) return;

			util::LockGuard<util::Mutex> guard(lock_);
			
			util::StackAllocator::Tool::forceReset(*alloc);
			alloc->setFreeSizeLimit(1);
			alloc->trim();
			
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, alloc);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Get new local variable size allocator
*/
SQLVariableSizeLocalAllocator *SQLAllocatorManager::
		getVariableSizeLocalAllocator(bool withPool) {

	try {

		SQLAllocatorGroupId moduleId
				= ALLOCATOR_GROUP_SQL_WORK;
		const char *moduleName = "Local variable size allocator";

		util::LockGuard<util::Mutex> guard(lock_);
		if (withPool) {
			return UTIL_OBJECT_POOL_NEW(localVarAllocatorPool_)
					SQLVariableSizeLocalAllocator(
							util::AllocatorInfo(moduleId, moduleName));
		}
		else {
			return ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
					SQLVariableSizeLocalAllocator(
							util::AllocatorInfo(moduleId, moduleName));
		}
		return NULL;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Release local variable size allocator
*/
void SQLAllocatorManager::releaseVariableSizeLocalAllocator(
		SQLVariableSizeLocalAllocator *localVarAlloc,
		bool withPool) {

	try {
		if (withPool) {
			util::LockGuard<util::Mutex> guard(lock_);
			UTIL_OBJECT_POOL_DELETE(
					localVarAllocatorPool_, localVarAlloc);
		}
		else {
			if (localVarAlloc == NULL) return;
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, localVarAlloc);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Get common local variable size allocator
*/
SQLVariableSizeLocalAllocator
		*SQLAllocatorManager::getCommonVariableSizeLocalAllocator(
		ServiceType serviceType, int32_t workerId) {

	if (serviceType == SQL_SERVICE) {
		if (workerId >= static_cast<int32_t>(
				sqlLocalAllocatorList_.size())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
		}
		return sqlLocalAllocatorList_[workerId];
	}
	else if (serviceType == TRANSACTION_SERVICE) {
		if (workerId >= static_cast<int32_t>(
				txnLocalAllocatorList_.size())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
		}
		return txnLocalAllocatorList_[workerId];
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
	}
}

/*!
	@brief Release common local variable size allocator
*/
void SQLAllocatorManager::releseCommonVariableSizeLocalAllocator(
		SQLVariableSizeLocalAllocator *varAlloc) {
	UNUSED_VARIABLE(varAlloc);
}

/*!
	@brief Release common local variable size allocator
*/
void SQLAllocatorManager::resizeConcurrency(
		ServiceType serviceType, int32_t newConcurrency) {

	try {
		std::vector<SQLVariableSizeLocalAllocator*>
				*targetAllocatorList = NULL;
		int32_t targetConcurrency = -1;
		
		util::LockGuard<util::Mutex> guard(lock_);

		if (serviceType == SQL_SERVICE) {
			targetAllocatorList = &sqlLocalAllocatorList_;
			targetConcurrency
					= static_cast<int32_t>(sqlLocalAllocatorList_.size());
		}
		else if (serviceType == TRANSACTION_SERVICE) {
			targetAllocatorList = &txnLocalAllocatorList_;
			targetConcurrency
					= static_cast<int32_t>(txnLocalAllocatorList_.size());
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
		}

		if (targetConcurrency < newConcurrency) {
			int32_t diff = (newConcurrency - targetConcurrency);

			for (int32_t pos = 0; pos < diff; pos++) {
				targetAllocatorList->push_back(
						ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
								SQLVariableSizeLocalAllocator(
										util::AllocatorInfo(
												ALLOCATOR_GROUP_SQL_WORK,
												"VariableLocalAlloc")));
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Setup configuration
*/
void SQLAllocatorManager::ConfigSetUpHandler::operator()(
		ConfigTable &config) {

	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SQL, "sql");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(DEFAULT_WORK_MEMORY_LIMIT_MB);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_WORK_CACHE_MEMORY, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(DEFAULT_WORK_CACHE_MEMORY_MB);
}

/*!
	@brief Constuctor of sql stat
*/
SQLAllocatorManager::Config::Config(
		ConfigTable &configTable) : manager_(NULL) {
	
	workMemoryLimitSize_ = ConfigTable::megaBytesToBytes(
			static_cast<size_t>(
					configTable.getUInt32(
							CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT)));

	workCacheMemorySize_ = ConfigTable::megaBytesToBytes(
			static_cast<size_t>(
					configTable.getUInt32(
							CONFIG_TABLE_SQL_WORK_CACHE_MEMORY)));
}

/*!
	@brief Destructor of sql service configuration
*/
SQLAllocatorManager::Config::~Config() {
}

/*!
	@brief Setup configuration handler
*/
void SQLAllocatorManager::Config::setUpConfigHandler(
		ConfigTable &configTable, SQLAllocatorManager *manager) {
	
	manager_ = manager;

	configTable.setParamHandler(
			CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT, *this);

	configTable.setParamHandler(
			CONFIG_TABLE_SQL_WORK_CACHE_MEMORY, *this);
}

/*!
	@brief Operation of configuration
*/
void SQLAllocatorManager::Config::operator() (
		ConfigTable::ParamId id, const ParamValue &value) {

	switch (id) {

	case CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT: 

		workMemoryLimitSize_ = ConfigTable::megaBytesToBytes(
				static_cast<size_t>(value.get<int32_t>()));

		break;

	case CONFIG_TABLE_SQL_WORK_CACHE_MEMORY: 

		workCacheMemorySize_ = ConfigTable::megaBytesToBytes(
				static_cast<size_t>(value.get<int32_t>()));

		break;

	default:
		break;
	}
}

int64_t SQLAllocatorManager::getWorkMemoryLimitSize() {
	return config_.workMemoryLimitSize_;
}

int64_t SQLAllocatorManager::getWorkCacheMemorySize() {
	return config_.workCacheMemorySize_;
}