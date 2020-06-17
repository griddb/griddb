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
#include "sql_profiler.h"
#include "cluster_common.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(SQL_DETAIL);

SQLDetailProfs::SQLDetailProfs(
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		int32_t limitInterval, int32_t queryLimitSize) :
		globalVarAlloc_(globalVarAlloc),
		startTime_(util::DateTime::now(false).getUnixTime()),
		startTimeStr_(globalVarAlloc),
		dbName_(globalVarAlloc),
		applicationName_(globalVarAlloc),
		query_(globalVarAlloc),
		isOmmited_(false),
		isTrace_(false),
		execTime_(INT32_MAX),
		traceLimitInterval_(limitInterval),
		queryLimitSize_(queryLimitSize) {
	watch_.start();
}

SQLDetailProfs::~SQLDetailProfs() try {
	if (isTrace_) {
		if (execTime_ == INT32_MAX) {
			execTime_ = watch_.elapsedMillis();
		}
		if (execTime_ >= traceLimitInterval_) {
			GS_TRACE_WARNING(
					SQL_DETAIL, GS_TRACE_SQL_LONG_QUERY,
					dump());	
		}
	}
}
catch (...) {
	assert(false);
}

void SQLDetailProfs::set(
		const char *dbName,
		const char *applicationName, 
		const char *query) {

	isTrace_ = true;
	dbName_ = dbName;
	applicationName_ = applicationName;
	int32_t orgLength = static_cast<int32_t>(strlen(query));
	int32_t queryLength = orgLength;
	if (queryLength > queryLimitSize_) {
		isOmmited_ = true;
		queryLength = queryLimitSize_;
		std::string tmpQuery(query);
		query_ =  tmpQuery.substr(0, queryLength).c_str();
		util::NormalOStringStream ss;
		ss << "(ommited, limit=" << queryLimitSize_
				<< ", size=" << orgLength << ")";
		query_.append(ss.str().c_str());
	}
	else if (queryLength > 0) {
		query_ = query;
	}
}

void SQLDetailProfs::complete(int32_t executionTime) {
	if (executionTime == INT32_MAX) {
		execTime_ = watch_.elapsedMillis();
	}
	else {
		execTime_ = executionTime;
	}
}

std::string SQLDetailProfs::dump() {
	util::NormalOStringStream ss;
	ss << "startTime=" << getTimeStr(startTime_)
		<< ", dbName=" << dbName_.c_str()
		<< ", applicationName=" << applicationName_.c_str()
		<< ", query=" << query_.c_str();
	return ss.str().c_str();
}

std::string SQLDetailProfs::dumpQuery() {
	util::NormalOStringStream ss;
	ss << ", dbName=" << dbName_.c_str()
		<< ", applicationName=" << applicationName_.c_str()
		<< ", query=" << query_.c_str();
	return ss.str().c_str();
}