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
	@file sql_client_result_set.h
	@brief Definition of sql client result set
*/

#ifndef SQL_ALLOCATOR_MANAGER_H_
#define SQL_ALLOCATOR_MANAGER_H_

#include "util/allocator.h"
#include "sql_common.h"
#include "cluster_common.h"

/*!
	@brief SQL allocation manager
*/
class SQLAllocatorManager {
public:

	static const int32_t DEFAULT_ALLOCATOR_BLOCK_SIZE_EXP;
	static const int32_t DEFAULT_WORK_MEMORY_LIMIT_MB;
	static const int32_t DEFAULT_WORK_CACHE_MEMORY_MB;
	static const int32_t DEFAULT_TOTAL_WORK_MEMORY_LIMIT_MB;
	static const int32_t DEFAULT_POOL_SIZE;

	SQLAllocatorManager(
			ConfigTable &config,
			int32_t blockSizeExp);

	~SQLAllocatorManager();
	
	void resizeConcurrency(
			ServiceType serviceType, int32_t newConcurrency);

	SQLVariableSizeGlobalAllocator *getGlobalAllocator();
	
	util::StackAllocator *getStackAllocator(
			bool withPool = false);

	SQLVariableSizeLocalAllocator *getVariableSizeLocalAllocator(
			bool withPool = false);

	SQLVariableSizeLocalAllocator *getCommonVariableSizeLocalAllocator(
			ServiceType serviceType, int32_t workerId);

	void releaseStackAllocator(
		util::StackAllocator *alloc,
		bool withPool = false);
	
	void releaseVariableSizeLocalAllocator(
			SQLVariableSizeLocalAllocator *localVarAlloc,
			bool withPool = false);

	void releseCommonVariableSizeLocalAllocator(
			SQLVariableSizeLocalAllocator *varAlloc);
	
	/*!
		@brief Cofiguration setup handler
	*/
	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {

		virtual void operator() (ConfigTable &config);
	}
	configSetUpHandler_;


	int64_t getWorkMemoryLimitSize();

	int64_t getWorkCacheMemorySize();

private:

	/*!
		@brief Configuration
	*/
	struct Config : public ConfigTable::ParamHandler {

		Config(ConfigTable &configTable);

		~Config();

		void setUpConfigHandler(
				ConfigTable &configTable, SQLAllocatorManager *manager);
		
		virtual void operator()(
				ConfigTable::ParamId id, const ParamValue &value);

		int64_t workMemoryLimitSize_;
		int64_t workCacheMemorySize_;

		SQLAllocatorManager *manager_;
	};
	
	util::Mutex lock_;
	Config config_;
	SQLVariableSizeGlobalAllocator globalVarAlloc_;
	int32_t blockSizeExp_;
	int32_t sqlConcurrency_;
	int32_t txnConcurrency_;
	util::FixedSizeAllocator<util::Mutex> fixedAllocator_;
	std::vector<SQLVariableSizeLocalAllocator*> sqlLocalAllocatorList_;
	std::vector<SQLVariableSizeLocalAllocator*> txnLocalAllocatorList_;

	util::ObjectPool<util::StackAllocator> stackAllocatorPool_;
	util::ObjectPool<SQLVariableSizeLocalAllocator> localVarAllocatorPool_;
};

#endif
