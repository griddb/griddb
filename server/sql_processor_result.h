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
	@brief Definition of sql processor result
*/
#ifndef SQL_PROCESSOR_RESULT_H_
#define SQL_PROCESSOR_RESULT_H_

#include "sql_processor.h"

struct SQLDetailProfs {

	SQLDetailProfs(
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			int32_t limitInterval, int32_t queryLimitSize);
	~SQLDetailProfs();
	SQLDetailProfs(SQLVariableSizeGlobalAllocator& globalVarAlloc, const SQLDetailProfs& another);

	void set(const char *dbName,const char *applicationName, const char *query);

	void complete(int32_t executionTime = INT32_MAX);

	bool check(int64_t &startTIme, uint32_t &elapsedMills);

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
	int64_t execTime_;
	int32_t traceLimitInterval_;
	int32_t queryLimitSize_;
	int64_t swapReadSize_;
	int64_t swapWriteSize_;
	int64_t sqlSwapReadSize_;
	int64_t sqlSwapWriteSize_;
	int32_t taskNum_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(startTimeStr_,
			dbName_, applicationName_, query_);
};

class ResultProcessor : public SQLProcessor {

	typedef SQLContext Context;
	typedef int32_t AccessControlType;

public:

	ResultProcessor(Context &cxt, const TypeInfo &typeInfo);

	virtual ~ResultProcessor();

	virtual bool pipe(Context &cxt, InputId inputId, const Block &block);

	virtual bool finish(Context &cxt, InputId inputId);

	virtual bool applyInfo(
			Context &cxt, const Option &option,
			const TupleInfoList &inputInfo, TupleInfo &outputInfo);

	void exportTo(Context &cxt, const OutOption &option) const;

	TupleList::Reader *getReader() {
		return reader_;
	}
	
	TupleList::Column *getColumnInfoList() {
		return columnInfoList_;
	}

	size_t getColumnSize() {
		return columnSize_;
	}

	void setExplainAnalyze() {
		isExplainAnalyze_ = true;
	}

	void setQueryType(bool isSQL) {
		isSQL_ = isSQL;
	}
	
	bool isFirst() {
		return isFirst_;
	}

	SQLDetailProfs &getProfs() {
		return profs_;
	}

private:
	typedef Factory::Registrar<ResultProcessor> Registrar;

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	ClientId clientId_;
	LocalTempStore::Group *destGroup_;
	TupleList::Info info_;
	TupleList::TupleColumnType *columnList_;
	TupleList *destTupleList_;
	TupleList::Reader *reader_;
	StatementId stmtId_;
	TupleList::Column *columnInfoList_;
	size_t columnSize_;
	bool isFirst_;
	bool isExplainAnalyze_;
	bool isSQL_;
	util::Stopwatch watch_;
	SQLExecutionManager *executionManager_;
	SQLDetailProfs profs_;
	static const Registrar registrar_;
};

