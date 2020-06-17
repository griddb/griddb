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
	@file sql_requeset_info.cpp
	@brief Implementation of sql request info
*/

#include "sql_service_handler.h"
#include "nosql_utils.h"

/*!
	@brief Reply error not via sql execution(direct)
*/
void SQLServiceHandler::replyError(
		EventType eventType,
		PartitionId pId,
		EventContext &ec,
		const NodeDescriptor &nd,
		StatementId stmtId,
		const std::exception &e) {

	EventRequestOption sendOption;
	StatementExecStatus status = TXN_STATEMENT_ERROR;

	const util::Exception 
			checkException = GS_EXCEPTION_CONVERT(e, "");
	int32_t errorCode = checkException.getErrorCode();
	
	if (NoSQLUtils::isDenyException(errorCode)) {
		status = TXN_STATEMENT_DENY;
	}

	Event replyEv(ec, eventType, pId);
	setErrorReply(replyEv, stmtId, status, e, nd);
	
	if (!nd.isEmpty()) {
		ec.getEngine().send(replyEv, nd, &sendOption);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
	}
}
