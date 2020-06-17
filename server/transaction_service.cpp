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
#include "transaction_service.h"


void TransactionService::setClusterHandler() {
}


void ReplicationLogHandler::operator()(EventContext &ec, Event &ev) {
	GS_THROW_USER_ERROR(
			GS_ERROR_CM_NOT_SUPPORTED, "not support cluster operation");
}

void ReplicationAckHandler::operator()(EventContext &ec, Event &ev) {
	GS_THROW_USER_ERROR(
			GS_ERROR_CM_NOT_SUPPORTED, "not support cluster operation");
}

bool StatementHandler::executeReplication(
	const Request &request,
	EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &clientND, TransactionContext &txn,
	EventType replStmtType, StatementId replStmtId, int32_t replMode,
	ReplicationContext::TaskStatus taskStatus,
	const ClientId *closedResourceIds, size_t closedResourceIdCount,
	const util::XArray<uint8_t> **logRecordList, size_t logRecordCount,
	StatementId originalStmtId, int32_t delayTime,
	const Response &response) {
	return false;
}

void StatementHandler::replyReplicationAck(
		EventContext &ec,
		util::StackAllocator &alloc, const NodeDescriptor &ND,
		const ReplicationAck &ack, bool optionalFormat) {
}

