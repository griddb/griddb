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
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "cluster_manager.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

/*!
	@brief Encode notification multi-calst message
*/
void SQLTimerNotifyClientHandler::encode(
		EventByteOutStream &out) {

	SQLService *sqlSvc = resourceSet_->getSQLService();

	try {
		
		const uint8_t hashType = 0;
		util::SocketAddress::Inet address;
		uint16_t port;

		const NodeDescriptor &nd
				= sqlSvc->getEE()->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_SENDER_ND,
					"Invalid Node descriptor");
		}

		nd.getAddress().getIP(&address, &port);
		out.writeAll(&address, sizeof(address));
		
		out << static_cast<uint32_t>(port);
		out << static_cast<PartitionId>(
					DataStore::MAX_PARTITION_NUM);
		out << hashType;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"SQL client notification encode failed"));
	}
}

/*!
	@brief Handler of notification to client
*/
void SQLTimerNotifyClientHandler::operator ()(
		EventContext &ec, Event &ev) {

	UNUSED_VARIABLE(ev);

	if (clsSvc_->getNotificationManager().getMode()
			!= NOTIFICATION_MULTICAST) {
		return;
	}

	SQLService *sqlSvc = resourceSet_->getSQLService();

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	try {
	
		if (!pt_->isMaster()) {
			return;
		}
		clsMgr_->checkNodeStatus();

		Event notifySqlClientEvent(ec, SQL_NOTIFY_CLIENT,
				CS_HANDLER_PARTITION_ID);

		EventByteOutStream out = notifySqlClientEvent.getOutStream();
		encode(out);

		const NodeDescriptor &multicastND
				= sqlSvc->getEE()->getMulticastND();

		if (!multicastND.isEmpty()) {
		
			notifySqlClientEvent.setPartitionIdSpecified(false);
			sqlSvc->getEE()->send(
					notifySqlClientEvent, multicastND);
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_INVALID_SENDER_ND,
					"Mutilcast send failed");
		}
	}
	catch (EncodeDecodeException &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}

/*!
	@brief Handler of sql connection info to client
*/
void SQLGetConnectionAddressHandler::operator () (
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	const EventMonotonicTime emNow
			= ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		bool masterResolving = false;
		if (in.base().remaining() > 0) {
			decodeBooleanData(in, masterResolving);
		}

		const ClusterRole clusterRole =
				CROLE_MASTER |
				(masterResolving ? CROLE_FOLLOWER : 0);

		checkAuthentication(ev.getSenderND(), emNow);
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);

		checkExecutable(clusterRole);

		util::XArrayOutStream<> arrayOut(response.binaryData_);
		util::ByteStream< util::XArrayOutStream<>> out(arrayOut);

		out << static_cast<PartitionId>(
				DataStore::MAX_PARTITION_NUM);

		util::XArray<NodeId> liveNodeList(alloc);
		partitionTable_->getLiveNodeIdList(liveNodeList);

		SQLService *sqlSvc = resourceSet_->getSQLService();
		uint32_t targetNodePos
				= sqlSvc->getTargetPosition(ev, liveNodeList);

		if (masterResolving) {
			out << static_cast<uint8_t>(0);
		}
		else {

			NodeAddress &address
					= partitionTable_->getPublicNodeAddress(
							liveNodeList[targetNodePos], SQL_SERVICE);

			out << static_cast<uint8_t>(1);
			
			out << std::pair<const uint8_t*, size_t>(
					reinterpret_cast<const uint8_t*>(&address.address_),
					sizeof(AddressType));

			out << static_cast<uint32_t>(address.port_);
		}

		out << static_cast<uint8_t>(0);

		if (masterResolving) {
			GetPartitionAddressHandler::encodeClusterInfo(
					out,
					ec.getEngine(),
					*partitionTable_,
					CONTAINER_HASH_MODE_CRC32);
		}

		replySuccess(
				ec,
				alloc,
				ev.getSenderND(),
				ev.getType(),
				TXN_STATEMENT_SUCCESS,
				request,
				response,
				NO_REPLICATION);
	}
	catch (std::exception &e) {

		try {
			replyError(
					ev.getType(),
					ev.getPartitionId(),
					ec,
					ev.getSenderND(),
					request.fixed_.cxtSrc_.stmtId_,
					e);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

template<typename T> void decodeValue(
		util::ByteStream<util::ArrayInStream> &in, T &value) {

	try {
		in >> value;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, "Decode numeric value failed");
	}
}

void SQLCancelHandler::operator ()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();
	JobManager *jobManager = resourceSet_->getJobManager();

	try {

		bool isClientCancel = true;
		
		if (!ev.getSenderND().isEmpty() &&
				ev.getSenderND().getId() >= 0) {
			isClientCancel = false;
		}

		ClientId cancelClientId;
		JobExecutionId execId;
		EventByteInStream in = ev.getInStream();

		decodeValue(in, execId);
		StatementHandler::decodeUUID(
				in,
				cancelClientId.uuid_,
				TXN_CLIENT_UUID_BYTE_SIZE);

		decodeValue(in, cancelClientId.sessionId_);

		ExecutionLatch latch(
				cancelClientId,
				executionManager->getResourceManager(),
				NULL);
		SQLExecution *execution = latch.get();

		if (execution) {
			JobId jobId;
			execution->getContext().getCurrentJobId(jobId);

			if (!isClientCancel) {
				GS_TRACE_WARNING(
						SQL_SERVICE, GS_TRACE_SQL_CANCEL,
						"Call cancel by node failure, jobId=" << jobId);
			}
			else {
				GS_TRACE_WARNING(
						SQL_SERVICE, GS_TRACE_SQL_CANCEL,
						"Call cancel by client, jobId=" << jobId);
			}

			jobManager->cancel(ec, jobId, true);
			execution->cancel(execId);
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_CANCELLED,
					"Cancel SQL, clientId="
					<< cancelClientId << ", location=cancel");
		}
	}
	catch (std::exception &e) {

		UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, e, "");
	}
}

void SQLSocketDisconnectHandler::operator ()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();

	try {
		executionManager->closeConnection(
				ec, ev.getSenderND());
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}