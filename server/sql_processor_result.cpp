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
#include "sql_processor_result.h"
#include "sql_execution.h"
#include "sql_service.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(SQL_DETAIL);

const ResultProcessor::Registrar ResultProcessor::registrar_(
	SQLType::EXEC_RESULT);

ResultProcessor::ResultProcessor(
	Context& cxt, const TypeInfo& typeInfo) :
	SQLProcessor(cxt, typeInfo),
	globalVarAlloc_(
		cxt.getExecutionManager()->getVarAllocator()),
	clientId_(*cxt.getClientId()),
	destGroup_(NULL),
	columnList_(NULL),
	destTupleList_(NULL),
	reader_(NULL),
	stmtId_(cxt.getStatementId()),
	columnInfoList_(NULL),
	columnSize_(0),
	isFirst_(true),
	isExplainAnalyze_(
		cxt.getTask()->getJob()->isExplainAnalyze()),
	isSQL_(cxt.getTask()->getJob()->isSQL()),
	executionManager_(cxt.getExecutionManager()),
	profs_(globalVarAlloc_,
		cxt.getExecutionManager()->
		getSQLService()->getTraceLimitTime(),
		cxt.getExecutionManager()->
		getSQLService()->getTraceLimitQuerySize()) {
}

ResultProcessor::~ResultProcessor() {

	if (columnInfoList_) {
		globalVarAlloc_.deallocate(columnInfoList_);
	}
	if (reader_) {
		reader_->close();
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, reader_);
	}
	if (destTupleList_) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, destTupleList_);
	}
	if (destGroup_) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, destGroup_);
	}
	if (columnList_) {
		globalVarAlloc_.deallocate(columnList_);
	}
}

bool ResultProcessor::applyInfo(
	Context& cxt, const Option& option,
	const TupleInfoList& inputInfo, TupleInfo& outputInfo) {

	try {
		if (inputInfo.size() != 1) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Result processur input size must be 1, but current=" << inputInfo.size());
		}

		size_t inputSize = inputInfo.size();
		const TupleInfo& tupleInfo = inputInfo[0];
		columnSize_ = tupleInfo.size();
		columnList_ = static_cast<TupleList::TupleColumnType*>(
			globalVarAlloc_.allocate(sizeof(TupleList::TupleColumnType) * columnSize_));
		for (size_t i = 0; i < columnSize_; i++) {
			columnList_[i] = tupleInfo[i];
		}

		info_.columnCount_ = columnSize_;
		info_.columnTypeList_ = columnList_;
		destGroup_ =
			ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			LocalTempStore::Group(cxt.getStore());
		destTupleList_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			TupleList(*destGroup_, info_);
		reader_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			TupleList::Reader(*destTupleList_,
				TupleList::Reader::ORDER_SEQUENTIAL);
		columnInfoList_ = static_cast<TupleList::Column*>(
			globalVarAlloc_.allocate(
				sizeof(TupleList::Column) * columnSize_));
		info_.getColumns(columnInfoList_, columnSize_);

		for (size_t pos = 0; pos < tupleInfo.size(); pos++) {
			outputInfo.push_back(tupleInfo[pos]);
		}

		SQLExecutionManager::Latch latch(
			clientId_, cxt.getExecutionManager());
		SQLExecution* execution = latch.get();
		if (execution) {
			SQLExecution::SQLExecutionContext& sqlContext = execution->getContext();
			profs_.set(
				sqlContext.getDBName(),
				sqlContext.getApplicationName(),
				sqlContext.getQuery());
		}
		return true;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool ResultProcessor::pipe(Context& cxt,
	InputId inputId, const Block& block) {
	try {
		if (isExplainAnalyze_) {
			return false;
		}
		SQLExecutionManager::Latch latch(
			clientId_, cxt.getExecutionManager());
		SQLExecution* execution = latch.get();
		if (execution) {
			if (block.data() != NULL) {
				destTupleList_->append(block);
				if (isFirst_) {
					isFirst_ = false;
					if (isSQL_) {
						cxt.getExecutionManager()->incOperation(
							true, cxt.getEventContext()->getWorkerId(), 1);
					}
					return false;
				}
			}
			Job* job = cxt.getTask()->getJob();
			if (!job->isEnableFetch()) {
				return false;
			}
			SQLFetchContext fetchContext(reader_, columnInfoList_,
				columnSize_, cxt.getTask()->isResultCompleted());
			if (execution->fetch(*cxt.getEventContext(),
				fetchContext, cxt.getExecId(), cxt.getVersionId())) {
				cxt.setFetchComplete();
			}
		}
		else {
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

bool ResultProcessor::finish(Context& cxt, InputId inputId) {

	try {
		cxt.getTask()->setResultCompleted();
		if (isExplainAnalyze_) {
			cxt.setFetchComplete();
			return false;
		}

		Job* job = cxt.getTask()->getJob();
		if (!job->checkAndSetPending()) {
			return false;
		}
		SQLExecutionManager::Latch latch(clientId_, executionManager_);
		SQLExecution* execution = latch.get();
		if (execution) {
			SQLFetchContext fetchContext(reader_, columnInfoList_,
				columnSize_, true);
			if (execution->fetch(*cxt.getEventContext(),
				fetchContext, cxt.getExecId(), cxt.getVersionId())) {
				profs_.complete();
				cxt.setFetchComplete();
				execution->getContext().setEndTime(util::DateTime::now(false).getUnixTime());
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

void ResultProcessor::exportTo(
	Context& cxt, const OutOption& option) const {
}

SQLDetailProfs::SQLDetailProfs(
	SQLVariableSizeGlobalAllocator& globalVarAlloc,
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
	const char* dbName,
	const char* applicationName,
	const char* query) {

	isTrace_ = true;
	dbName_ = dbName;
	applicationName_ = applicationName;
	int32_t orgLength = static_cast<int32_t>(strlen(query));
	int32_t queryLength = orgLength;
	if (queryLength > queryLimitSize_) {
		isOmmited_ = true;
		queryLength = queryLimitSize_;
		std::string tmpQuery(query);
		query_ = tmpQuery.substr(0, queryLength).c_str();
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
};

std::string SQLDetailProfs::dumpQuery() {
	util::NormalOStringStream ss;
	ss << ", dbName=" << dbName_.c_str()
		<< ", applicationName=" << applicationName_.c_str()
		<< ", query=" << query_.c_str();
	return ss.str().c_str();
};