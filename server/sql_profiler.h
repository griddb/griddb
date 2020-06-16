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
#ifndef SQL_PROFILER_H_
#define SQL_PROFILER_H_

#include "sql_common.h"

struct SQLDetailProfs {

	SQLDetailProfs(
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			int32_t limitInterval, int32_t queryLimitSize);
	~SQLDetailProfs();

	void set(
			const char *dbName,
			const char *applicationName, 
			const char *query);

	void complete(int32_t executionTime = INT32_MAX);

	std::string dump();
	std::string dumpQuery();

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	uint64_t startTime_;
	SQLString startTimeStr_;
	SQLString dbName_;
	SQLString applicationName_;
	SQLString query_;
	bool isOmmited_;
	bool isTrace_;

	util::Stopwatch watch_;
	int32_t execTime_;
	int32_t traceLimitInterval_;
	int32_t queryLimitSize_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(startTimeStr_,
			dbName_, applicationName_, query_);
};


#endif
