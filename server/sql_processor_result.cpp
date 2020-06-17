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
#include "sql_execution_manager.h"
#include "sql_service.h"
#include "sql_job.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(SQL_DETAIL);

const ResultProcessor::Registrar ResultProcessor::registrar_(
		SQLType::EXEC_RESULT);

ResultProcessor::ResultProcessor(
		Context &cxt,
		const TypeInfo &typeInfo) :
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
						cxt.getTask()->getJob()
								->isExplainAnalyze()),
				isSQL_(
						cxt.getTask()->getJob()->isSQL()),
				executionManager_(
						cxt.getExecutionManager()),
				profs_(
						globalVarAlloc_,
						cxt.getExecutionManager()->getTraceLimitTime(),
						cxt.getExecutionManager()->getTraceLimitQuerySize())  {
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
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, destTupleList_);
	}

	if (destGroup_) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, destGroup_);
	}
	if (columnList_) {
		globalVarAlloc_.deallocate(columnList_);
	}
}

bool ResultProcessor::applyInfo(
		Context &cxt,
		const Option &option,
		const TupleInfoList &inputInfo,
		TupleInfo &outputInfo) {

	UNUSED_VARIABLE(option);

	try {

		if (inputInfo.size() != 1) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
					"Result processur input size must be 1, "
					"but current=" << inputInfo.size());
		}

		const TupleInfo &tupleInfo = inputInfo[0];
		columnSize_ = tupleInfo.size();
		columnList_ = static_cast<TupleList::TupleColumnType*>(
				globalVarAlloc_.allocate(
						sizeof(TupleList::TupleColumnType) * columnSize_));
	
		for (size_t i = 0; i < columnSize_; i++) {
			columnList_[i] = tupleInfo[i];
		}

		info_.columnCount_ = columnSize_;
		info_.columnTypeList_ = columnList_;
		
		destGroup_ =
			ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
					LocalTempStore::Group(cxt.getStore());
		
		destTupleList_ =	ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TupleList(*destGroup_, info_);
		
		reader_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				TupleList::Reader(*destTupleList_,
						TupleList::Reader::ORDER_SEQUENTIAL);
		
		columnInfoList_ = static_cast<TupleList::Column*>(
				globalVarAlloc_.allocate(
						sizeof(TupleList::Column) * columnSize_));
		info_.getColumns(
				columnInfoList_, columnSize_);

		for (size_t pos = 0;
				pos < tupleInfo.size(); pos++) {

			outputInfo.push_back(tupleInfo[pos]);
		}

		ExecutionLatch latch(
				clientId_,
				cxt.getExecutionManager()->getResourceManager(),
				NULL);

		SQLExecution *execution = latch.get();
		
		if (execution) {
			SQLExecution::SQLExecutionContext 
					&sqlContext = execution->getContext();

			profs_.set(
					sqlContext.getDBName(),
					sqlContext.getApplicationName(),
					sqlContext.getQuery()); 
		}
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool ResultProcessor::pipe(
		Context &cxt,
		InputId inputId,
		const Block &block) {

	try {
	
		if (isExplainAnalyze_) {
			return false;
		}

		ExecutionLatch latch(
				clientId_,
				cxt.getExecutionManager()->getResourceManager(),
				NULL);

		SQLExecution *execution = latch.get();
		if (execution) {
		
			if (block.data() != NULL) {
				destTupleList_->append(block);

				if (isFirst_) {
					isFirst_ = false;

					if (isSQL_) {

						cxt.getExecutionManager()->incOperation(
								true,
								cxt.getEventContext()->getWorkerId(),
								1);
					}
					return false;
				}
			}

			SQLFetchContext fetchContext(
					reader_,
					columnInfoList_,
					columnSize_,
					cxt.getTask()->isResultCompleted());

			if (execution->fetch(
					*cxt.getEventContext(), 
					fetchContext, stmtId_,
					cxt.getVersionId())) {

				cxt.setFetchComplete();
			}
		}
		else {
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

bool ResultProcessor::finish(
		Context &cxt, InputId inputId) {

	UNUSED_VARIABLE(inputId);

	try {
		
		cxt.getTask()->setResultCompleted();
		
		if (isExplainAnalyze_) {
			cxt.setFetchComplete();
			return false;
		}
		
		ExecutionLatch latch(
					clientId_,
					executionManager_->getResourceManager(),
					NULL);

		SQLExecution *execution = latch.get();
		if (execution) {
		
				SQLFetchContext fetchContext(
						reader_,
						columnInfoList_,
						columnSize_,
						true);

				if (execution->fetch(
						*cxt.getEventContext(),
						fetchContext,
						stmtId_,
						cxt.getVersionId())) {

					profs_.complete();
					cxt.setFetchComplete();
				}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	return false;
}

void ResultProcessor::exportTo(
		Context &cxt, const OutOption &option) const {

	UNUSED_VARIABLE(cxt);
	UNUSED_VARIABLE(option);
}