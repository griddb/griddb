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
#include "sql_service_handler.h"
#include "sql_service.h"
#include "sql_utils.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "nosql_utils.h"

void SQLRequestHandler::operator ()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	const EventMonotonicTime emNow 
			= ec.getHandlerStartMonotonicTime();

	RequestInfo request(
		alloc, ev.getPartitionId(), ec.getWorkerId());
	request.eventType_ = ev.getType();

	PartitionTable *pt = resourceSet_->getPartitionTable();
	JobManager *jobManager = resourceSet_->getJobManager();
	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();

	try {
		EventByteInStream in(ev.getInStream());
		decode(in, request);

		checkAuthentication(ev.getSenderND(), emNow);

		const StatementHandler::ClusterRole clusterRole
				= (StatementHandler::CROLE_MASTER 
						| StatementHandler::CROLE_FOLLOWER);
		StatementHandler::checkExecutable(clusterRole, pt);

		if (request.closeQueryId_ != UNDEF_SESSIONID) {

			ClientId closeClientId(
					request.clientId_.uuid_,
					request.closeQueryId_);

			ExecutionLatch latch(
					closeClientId,
					executionManager->getResourceManager(),
					NULL);

			SQLExecution *execution = latch.get();
			
			if (execution != NULL) {
				executionManager->remove(ec, closeClientId);
			}
		}

		executionManager->setRequestedConnectionEnv(
				request, ev.getSenderND());

		switch (request.requestType_) {

			case REQUEST_TYPE_QUERY:
			case REQUEST_TYPE_PRAGMA: 
			case REQUEST_TYPE_EXECUTE:
			case REQUEST_TYPE_UPDATE: {
				
				if (request.isBind()) {
	
					if (request.retrying_ && request.sqlString_.size() > 0) {

						SQLExecutionRequestInfo executionInfo(
								request,
								&ev.getSenderND(),
								*resourceSet_);

						ExecutionLatch latch(
								request.clientId_,
								executionManager->getResourceManager(),
								&executionInfo);

						SQLExecution *execution = latch.get();
						if (execution == NULL) {
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_CANCELLED,
									"Cancel SQL, clientId=" << request.clientId_
									<< ", location=clientRetry");
						}

						execution->execute(
								ec,
								request,
								true,
								NULL,
								UNDEF_JOB_VERSIONID,
								NULL);
					}
					else {

						ExecutionLatch latch(
								request.clientId_,
								executionManager->getResourceManager(),
								NULL);

						SQLExecution *execution = latch.get();
						if (execution == NULL) {
						
							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_CANCELLED,
									"Cancel SQL, clientId=" << request.clientId_ 
									<< ", location=Bind");
						}

						execution->execute(
								ec,
								request,
								false,
								NULL,
								UNDEF_JOB_VERSIONID,
								NULL);
					}
				}
				else {
					SQLExecutionRequestInfo executionInfo(
							request,
							&ev.getSenderND(),
							*resourceSet_);

					ExecutionLatch latch(
							request.clientId_,
							executionManager->getResourceManager(),
							&executionInfo);

					SQLExecution *execution = latch.get();
					if (execution == NULL) {

							GS_THROW_USER_ERROR(
									GS_ERROR_SQL_CANCELLED,
									"Cancel SQL, clientId="
									<< request.clientId_
									<< ", location=Query");
					}

					execution->execute(
							ec,
							request,
							true,
							NULL,
							UNDEF_JOB_VERSIONID,
							NULL);
				}
			}
			break;

			case REQUEST_TYPE_PREPARE: {

				SQLExecutionRequestInfo executionInfo(
						request,
						&ev.getSenderND(),
						*resourceSet_);

				ExecutionLatch latch(
						request.clientId_,
						executionManager->getResourceManager(),
						&executionInfo);

				SQLExecution *execution = latch.get();
				if (execution == NULL) {
				
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_CANCELLED,
							"Cancel SQL, clientId=" << request.clientId_
							<< ", location=Prepared statement");
				}

				execution->execute(
						ec,
						request,
						false,
						NULL,
						UNDEF_JOB_VERSIONID,
						NULL);
			}
			break;

			case REQUEST_TYPE_FETCH: {
				if (request.retrying_) {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_CLIENT_FAILOVER_FAILED,
								"Failover fetch operation is not supported");
				}

				ExecutionLatch latch(
						request.clientId_,
						executionManager->getResourceManager(),
						NULL);

				SQLExecution *execution = latch.get();
				if (execution == NULL) {

					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_CANCELLED,
							"Cancel SQL, clientId="
							<< request.clientId_ << ", location=fetch");
				}

				execution->fetch(ec, request);
			}
			break;

			case REQUEST_TYPE_CLOSE: {
			
				ExecutionLatch latch(
						request.clientId_,
						executionManager->getResourceManager(), 
						NULL);

				SQLExecution *execution = latch.get();
				if (execution == NULL) {

					Event replyEv(ec, SQL_EXECUTE_QUERY, 0);
					replyEv.setPartitionIdSpecified(false);
					EventByteOutStream out = replyEv.getOutStream();

					out << request.stmtId_;
					out << TXN_STATEMENT_SUCCESS;

					encodeBooleanData(out, request.transactionStarted_);
					encodeBooleanData(out, request.isAutoCommit_);
					encodeIntData<int32_t>(out, 0);

					encodeEnvList(
							out,
							alloc,
							*executionManager,
							ev.getSenderND());
					ec.getEngine().send(replyEv, ev.getSenderND());

					return;
				}

				JobId jobId;
				execution->getContext().getCurrentJobId(jobId);

				jobManager->cancel(ec, jobId, false);

				execution->close(ec, request);
				executionManager->remove(
						ec, request.clientId_);
			}
			break;

			default:
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_OPTYPE_UNSUPPORTED,
						"Invalid requestType = "
						<< static_cast<int32_t>(request.requestType_));
		}
	}
	catch (std::exception &e) {
		try {

			ExecutionLatch latch(
					request.clientId_,
					executionManager->getResourceManager(),
					NULL);
			SQLExecution *execution = latch.get();

			if (execution) {
				execution->setRequest(request);
				execution->execute(
						ec,
						request,
						true,
						&e,
						UNDEF_JOB_VERSIONID,
						NULL);
			}
			else {
			replyError(
					ev.getType(),
					ev.getPartitionId(),
					ec,
					ev.getSenderND(),
					request.stmtId_,
					e);
			}
		}
		catch (std::exception &e2) {
			replyError(
					ev.getType(),
					ev.getPartitionId(),
					ec,
					ev.getSenderND(),
					request.stmtId_,
					e2);
		}
	}
}
void SQLRequestHandler::decode(
			util::ByteStream<util::ArrayInStream> &in,
			RequestInfo &request) {

	try {

		decodeLongData<StatementId>(
				in, request.stmtId_);
		decodeUUID(
				in,
				request.clientId_.uuid_,
				TXN_CLIENT_UUID_BYTE_SIZE);
		decodeEnumData<SQLGetMode>(
				in, request.sessionMode_);
		int32_t tmpStatement;
		decodeIntData<int32_t>(in, tmpStatement);
		request.requestType_
				= static_cast<SQLRequestType>(tmpStatement);

		decodeLongData<SessionId>(
				in, request.clientId_.sessionId_);
		decodeLongData<SessionId>(
				in, request.closeQueryId_);
		decodeBooleanData(
				in, request.transactionStarted_);
		decodeBooleanData(
				in, request.retrying_);

		decodeBindInfo(in, request);

		decodeOptionPart(in, request.option_);
		setDefaultOptionValue(request.option_);
		decodeIntData<int32_t>(in, request.sqlCount_);

		util::String tmpString(request.alloc_);
		for (int32_t pos = 0; pos < request.sqlCount_; pos++) {

			decodeStringData<util::String>(in, tmpString);
			request.sqlString_ += tmpString;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "Client request decode failed, reason="
				<< GS_EXCEPTION_MESSAGE(e));
	}
}

void SQLRequestHandler::decodeBindInfo(
		util::ByteStream<util::ArrayInStream> &in,
		RequestInfo &request) {

	try {
		
		size_t startPos = in.base().position();
		decodeBooleanData(in, request.inTableExists_);
		if (request.inTableExists_) {
			int64_t rowCount = 0;
			in >> rowCount;
			decodeBinaryData(
					in, request.inTableSchema_, false);
			
			InStream fixedSchemaIn(
					util::ArrayInStream(
							request.inTableSchema_.data(),
							request.inTableSchema_.size()));
	
			int32_t columnCount;
			fixedSchemaIn >> columnCount;

			decodeBinaryData(
					in, request.inTableData_, false);

			if (columnCount > 0) {

				const char *fixedPartData
						= reinterpret_cast<const char *>(
								request.inTableData_.data());
				
				size_t fixedPartDataSize = request.inTableData_.size();
				int32_t nullsBytes
						= ValueProcessor::calcNullsByteSize(columnCount);
				int32_t columnDataSize
						= sizeof(uint8_t)/* type */ + sizeof(uint64_t); 
				InStream fixedPartIn(
						util::ArrayInStream(fixedPartData, fixedPartDataSize));

				uint64_t headerPos;
				fixedPartIn >> headerPos;

				size_t varPos = 8 + (8 + nullsBytes + columnDataSize * columnCount) * rowCount;
				InStream varPartIn(
						util::ArrayInStream(fixedPartData, fixedPartDataSize));

				request.bindParamSet_.prepare(columnCount, rowCount);

				for (int32_t i = 0; i < rowCount; i++) {

					uint64_t baseVarOffset;
					fixedPartIn >> baseVarOffset;

					fixedPartIn.base().position(
							fixedPartIn.base().position() + nullsBytes);

					varPartIn.base().position(varPos + baseVarOffset);

					ValueProcessor::getVarSize(varPartIn);

					for (int32_t columnNo = 0;
							columnNo < columnCount; columnNo++) {

						decodeBindColumnInfo(
								request,
								fixedPartIn,
								varPartIn);
					}
					request.bindParamSet_.next();
				}
			}

			size_t endPos = in.base().position();
			
			if (request.serialized_) {
			
				size_t serializedDataSize = endPos - startPos;
				request.serializedBindInfo_.resize(serializedDataSize);
				
				in.base().position(startPos);
				
				in >> std::make_pair(
						request.serializedBindInfo_.data(),
						serializedDataSize);
				
				in.base().position(endPos);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLRequestHandler::decodeBindColumnInfo(
		RequestInfo &request, InStream &fixedPartIn,
		InStream &varPartIn) {

	try {
		int64_t numValue;
		int32_t size;
		char8_t *data;

		ColumnType type;
		fixedPartIn >> type;

		BindParam *param = ALLOC_NEW(request.alloc_)
				BindParam(request.alloc_, type);

		switch (type) {

			case COLUMN_TYPE_BOOL: {
			
				fixedPartIn >> numValue;
				param->value_
						= TupleValue(
								reinterpret_cast<const char*>(&numValue),
								TupleList::TYPE_BOOL);
			}
			break;

		case COLUMN_TYPE_BYTE: {

			fixedPartIn >> numValue;
			param->value_ =
					TupleValue(
							reinterpret_cast<const char*>(&numValue),
							TupleList::TYPE_BYTE);
		}
		break;

		case COLUMN_TYPE_SHORT: {

			fixedPartIn >> numValue;
			param->value_
					= TupleValue(
							reinterpret_cast<const char*>(&numValue),
							TupleList::TYPE_SHORT);
		}
		break;

		case COLUMN_TYPE_INT: {
		
			fixedPartIn >> numValue;
			param->value_
					= TupleValue(
							reinterpret_cast<const char*>(&numValue),
							TupleList::TYPE_INTEGER);
		}
		break;

		case COLUMN_TYPE_LONG: {
		
			fixedPartIn >> numValue;
			param->value_
					= TupleValue(
							reinterpret_cast<const char*>(&numValue),
							TupleList::TYPE_LONG);
		}
		break;

		case COLUMN_TYPE_TIMESTAMP: {
		
			fixedPartIn >> numValue;
			param->value_
					= TupleValue(
								reinterpret_cast<const char*>(&numValue),
								TupleList::TYPE_TIMESTAMP);
		}
		break;

		case COLUMN_TYPE_NULL: {

			fixedPartIn >> numValue;
			param->value_
					= TupleValue(
							&SyntaxTree::NULL_VALUE_RAW_DATA,
							TupleList::TYPE_ANY);
		}
		break;

		case COLUMN_TYPE_FLOAT: {
			float floatValue;
			fixedPartIn >> floatValue;

			int32_t dummy;
			fixedPartIn >> dummy;

			param->value_
					= TupleValue(
							reinterpret_cast<const char*>(&floatValue),
							TupleList::TYPE_FLOAT);
		}
		break;

		case COLUMN_TYPE_DOUBLE: {
			
			double doubleValue;
			fixedPartIn >> doubleValue;
			
			param->value_
					= TupleValue(
							reinterpret_cast<const char*>(&doubleValue),
							TupleList::TYPE_DOUBLE);
		}
		break;

		case COLUMN_TYPE_STRING:
		case COLUMN_TYPE_GEOMETRY:
		case COLUMN_TYPE_BLOB: {
		
			TupleList::TupleColumnType tupleType
				= NoSQLUtils::convertNoSQLTypeToTupleType(type);
			int64_t fixedOffset;
			fixedPartIn >> fixedOffset;
			
			uint32_t varColumnDataSize
					= ValueProcessor::getVarSize(varPartIn);
			data = static_cast<char*>(
					request.alloc_.allocate(varColumnDataSize));
			
			varPartIn.readAll(data, varColumnDataSize);
			size = static_cast<int32_t>(varColumnDataSize);

			switch (tupleType) {
			
				case TupleList::TYPE_STRING: {

					param->value_
							= SyntaxTree::makeStringValue(
									request.alloc_, data, size);
				}
				break;

				case TupleList::TYPE_GEOMETRY:
				case TupleList::TYPE_BLOB: {

					TupleValue::VarContext cxt;
					cxt.setStackAllocator(&request.alloc_);
					
					param->value_
							= SyntaxTree::makeBlobValue(cxt, data, size);
				}
				break;

				default:
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_VALUETYPE_UNSUPPORTED, "");
			}
		}
		break;

		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY: {

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_VALUETYPE_UNSUPPORTED,
					"Unsupported bind type = "
					<< static_cast<int32_t>(type));
		}

		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INTERNAL,
					"Invalid bind type = "
					<< static_cast<int32_t>(type));
		}

		request.bindParamSet_.getParamList().push_back(param);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLRequestHandler::setDefaultOptionValue(
			OptionSet &optionSet) {

	if (optionSet.get<Options::STATEMENT_TIMEOUT_INTERVAL>() == 0) {

		const int32_t value
				= SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL;
		
		optionSet.set<
				Options::STATEMENT_TIMEOUT_INTERVAL>(value);

		optionSet.set<
				Options::TXN_TIMEOUT_INTERVAL>(
						TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);
	}
	else {

		optionSet.set<
				Options::TXN_TIMEOUT_INTERVAL>(
						optionSet.get<Options::STATEMENT_TIMEOUT_INTERVAL>() / 1000);
	}

	if (optionSet.get<Options::MAX_ROWS>() == 0) {

		const int64_t value = SQL_MAX_ROWS;
		optionSet.set<Options::MAX_ROWS>(value);
	}

	if (optionSet.get<Options::FETCH_SIZE>() == 0) {
	
		const int64_t value = SQL_DEFAULT_FETCH_SIZE;
		optionSet.set<Options::FETCH_SIZE>(value);
	}
}

void SQLRequestHandler::encodeEnvList(
		EventByteOutStream &out,
		util::StackAllocator &alloc,
		SQLExecutionManager &executionManager,
		const NodeDescriptor &nd) {

	const size_t startPos = out.base().position();
	int32_t count = 0;
	
	encodeIntData(out, count);
	
	uint32_t bits = executionManager.getConnectionEnvUpdates(nd);
	if (bits == 0) {
		return;
	}

	UTIL_STATIC_ASSERT(
			sizeof(bits) * CHAR_BIT > SQLPragma::PRAGMA_TYPE_MAX);

	for (uint32_t i = 0;
			i < SQLPragma::PRAGMA_TYPE_MAX; i++) {

		if ((bits & (1 << i)) != 0) {

			std::string value;
			bool hasData;
			
			if (executionManager.getConnectionEnv(
					nd, i, value, hasData)) {
				
				const util::String name(SQLPragma::getPragmaTypeName(
						static_cast<SQLPragma::PragmaType>(i)), alloc);

				StatementHandler::encodeStringData(out, name);
				StatementHandler::encodeStringData(out, value);

				count++;
			}
			bits &= ~(1 << i);

			if (bits == 0) {
				break;
			}
		}
	}

	const size_t endPos = out.base().position();
	out.base().position(startPos);
	encodeIntData(out, count);
	out.base().position(endPos);

	executionManager.completeConnectionEnvUpdates(nd);
}
