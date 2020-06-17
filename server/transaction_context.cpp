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
	@brief Implementation of TransactionContext
*/
#include "transaction_context.h"
#include "uuid_utils.h"

std::ostream &operator<<(std::ostream &stream, const ClientId &id) {
	stream << std::hex << static_cast<const uint32_t>(id.uuid_[0])
		   << static_cast<const uint32_t>(id.uuid_[1])
		   << static_cast<const uint32_t>(id.uuid_[2])
		   << static_cast<const uint32_t>(id.uuid_[3]) << "-"
		   << static_cast<const uint32_t>(id.uuid_[4])
		   << static_cast<const uint32_t>(id.uuid_[5]) << "-"
		   << static_cast<const uint32_t>(id.uuid_[6])
		   << static_cast<const uint32_t>(id.uuid_[7]) << "-"
		   << static_cast<const uint32_t>(id.uuid_[8])
		   << static_cast<const uint32_t>(id.uuid_[9]) << "-"
		   << static_cast<const uint32_t>(id.uuid_[10])
		   << static_cast<const uint32_t>(id.uuid_[11])
		   << static_cast<const uint32_t>(id.uuid_[12])
		   << static_cast<const uint32_t>(id.uuid_[13])
		   << static_cast<const uint32_t>(id.uuid_[14])
		   << static_cast<const uint32_t>(id.uuid_[15]) << std::dec << ":"
		   << id.sessionId_;
	return stream;
}

std::string ClientId::dump(util::StackAllocator &alloc) {

	util::NormalOStringStream strstrm;
	char tmpBuffer[UUID_STRING_SIZE];
	UUIDUtils::unparse(uuid_, tmpBuffer);
	util::String tmpUUIDStr(tmpBuffer, 36, alloc);	
	strstrm << tmpUUIDStr.c_str();
	strstrm << ":" << sessionId_;

	return strstrm.str().c_str();
}

TransactionContext::TransactionContext()
	: manager_(NULL),
	  alloc_(NULL),
	  clientId_(TXN_EMPTY_CLIENTID),
	  pId_(UNDEF_PARTITIONID),
	  containerId_(UNDEF_CONTAINERID),
	  txnId_(UNDEF_TXNID),
	  lastStmtId_(INITIAL_STATEMENTID),
	  state_(INACTIVE),
	  isExclusive_(false),
	  isRedo_(false),
	  cancelRequested_(false),
	  stmtStartTime_(0),
	  stmtExpireTime_(0),
	  txnStartTime_(0),
	  txnExpireTime_(0),
	  contextExpireTime_(0),
	  txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
	  storeMemoryAgingSwapRate_(TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE),
	  timeZone_(util::TimeZone()) {}

TransactionContext::TransactionContext(const TransactionContext &txn)
	: manager_(txn.manager_),
	  alloc_(txn.alloc_),
	  clientId_(txn.clientId_),
	  pId_(txn.pId_),
	  containerId_(txn.containerId_),
	  txnId_(txn.txnId_),
	  lastStmtId_(txn.lastStmtId_),
	  state_(txn.state_),
	  isExclusive_(txn.isExclusive_),
	  isRedo_(txn.isRedo_),
	  cancelRequested_(txn.cancelRequested_),
	  stmtStartTime_(txn.stmtStartTime_),
	  stmtExpireTime_(txn.stmtExpireTime_),
	  txnStartTime_(txn.txnStartTime_),
	  txnExpireTime_(txn.txnExpireTime_),
	  contextExpireTime_(txn.contextExpireTime_),
	  txnTimeoutInterval_(txn.txnTimeoutInterval_),
	  storeMemoryAgingSwapRate_(txn.storeMemoryAgingSwapRate_),
	  timeZone_(txn.timeZone_) {}

TransactionContext::~TransactionContext() {}

TransactionContext::TransactionContext(util::StackAllocator &alloc)
	: manager_(NULL),
	  alloc_(&alloc),
	  clientId_(TXN_EMPTY_CLIENTID),
	  pId_(UNDEF_PARTITIONID),
	  containerId_(UNDEF_CONTAINERID),
	  txnId_(UNDEF_TXNID),
	  lastStmtId_(INITIAL_STATEMENTID),
	  state_(INACTIVE),
	  isExclusive_(false),
	  isRedo_(false),
	  cancelRequested_(false),
	  stmtStartTime_(0),
	  stmtExpireTime_(0),
	  txnStartTime_(0),
	  txnExpireTime_(0),
	  contextExpireTime_(0),
	  txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
	  storeMemoryAgingSwapRate_(TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE),
	  timeZone_(util::TimeZone()) {}

