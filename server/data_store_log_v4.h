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
#ifndef V4_DATA_STORE_LOG_H_
#define V4_DATA_STORE_LOG_H_

#include "transaction_manager.h"
#include "cluster_event_type.h"

struct DataStoreLogV4 : public Serializable {
	struct EmptyDataLog : public Serializable {
		EmptyDataLog() : Serializable(NULL) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}
	};

	struct Commit : public EmptyDataLog {
	};
	struct Abort : public EmptyDataLog {
	};

	struct PutContainer : public Serializable {
		PutContainer(util::StackAllocator& alloc) : Serializable(&alloc),
			keyBinary_(NULL), keySize_(0), info_(NULL), type_(UNDEF_CONTAINER) {}
		PutContainer(util::StackAllocator& alloc, uint8_t* keyBinary, size_t keySize,
			util::XArray<uint8_t>* info, ContainerType type) : Serializable(&alloc),
			keyBinary_(keyBinary), keySize_(keySize), info_(info), type_(type) {}
		~PutContainer() {
			ALLOC_DELETE(*alloc_, keyBinary_);
			keySize_ = 0;
		}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
			DataStoreUtil::encodeVarSizeBinaryData<S>(out, keyBinary_, keySize_);
			DataStoreUtil::encodeVarSizeBinaryData<S>(out, info_->data(), info_->size());
			out << type_;
		}
		template <typename S>
		void decode(S& in) {
			DataStoreUtil::decodeVarSizeBinaryData<S>(in, *alloc_, keyBinary_, keySize_);
			info_ = ALLOC_NEW(*alloc_) util::XArray<uint8_t>(*alloc_);
			DataStoreUtil::decodeVarSizeBinaryData<S>(in, *info_);
			in >> type_;
		}
		FullContainerKey getContainerKey() {
			return FullContainerKey(*alloc_, KeyConstraint::getNoLimitKeyConstraint(), keyBinary_, keySize_);
		}

		uint8_t* keyBinary_;
		size_t keySize_;
		util::XArray<uint8_t>* info_;
		ContainerType type_;
	};

	struct DropContainer : public Serializable {
		DropContainer(util::StackAllocator& alloc) : Serializable(&alloc),
			keyBinary_(NULL), keySize_(0), type_(UNDEF_CONTAINER) {}
		DropContainer(util::StackAllocator& alloc, uint8_t* keyBinary, size_t keySize,
			ContainerType type) : Serializable(&alloc),
			keyBinary_(keyBinary), keySize_(keySize), type_(type) {}
		~DropContainer() {
			ALLOC_DELETE(*alloc_, keyBinary_);
			keySize_ = 0;
		}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
			DataStoreUtil::encodeVarSizeBinaryData<S>(out, keyBinary_, keySize_);
			out << type_;
		}
		template <typename S>
		void decode(S& in) {
			DataStoreUtil::decodeVarSizeBinaryData<S>(in, *alloc_, keyBinary_, keySize_);
			in >> type_;
		}

		FullContainerKey getContainerKey() {
			return FullContainerKey(*alloc_, KeyConstraint::getNoLimitKeyConstraint(), keyBinary_, keySize_);
		}

		uint8_t* keyBinary_;
		size_t keySize_;
		int32_t type_;
	};

	struct CreateIndex : public Serializable {
		CreateIndex(util::StackAllocator& alloc) : Serializable(&alloc), info_(NULL) {}
		CreateIndex(util::StackAllocator& alloc, IndexInfo* info) : Serializable(&alloc), info_(info) {}
		~CreateIndex() {
			ALLOC_DELETE(*alloc_, info_);
		}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
			info_->encode<S>(out);
		}
		template <typename S>
		void decode(S& in) {
			info_ = ALLOC_NEW(*alloc_) IndexInfo(*alloc_);
			info_->decode<S>(in);
		}
		IndexInfo* info_;
	};
	struct DropIndex : public Serializable {
		DropIndex(util::StackAllocator& alloc) : Serializable(&alloc), info_(NULL) {}
		DropIndex(util::StackAllocator& alloc, IndexInfo* info) : Serializable(&alloc), info_(info) {}
		~DropIndex() {
			ALLOC_DELETE(*alloc_, info_);
		}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
			info_->encode<S>(out);
		}
		template <typename S>
		void decode(S& in) {
			info_ = ALLOC_NEW(*alloc_) IndexInfo(*alloc_);
			info_->decode<S>(in);
		}
		IndexInfo* info_;
	};

	struct ContinueCreateIndex : public EmptyDataLog {
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}
	};
	struct ContinueAlterContainer : public EmptyDataLog {
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}
	};

	struct PutRow : Serializable {
		PutRow(util::StackAllocator& alloc) : Serializable(&alloc), rowId_(UNDEF_ROWID), rowData_(NULL) {}
		PutRow(util::StackAllocator& alloc, RowId rowId, util::XArray<uint8_t>* data) : Serializable(&alloc), rowId_(rowId), rowData_(data) {}
		~PutRow() {
			ALLOC_DELETE(*alloc_, rowData_);
		}

		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {
			out << rowId_;
			DataStoreUtil::encodeVarSizeBinaryData<S>(out, rowData_->data(), rowData_->size());
		}
		template <typename S>
		void decode(S& in) {
			in >> rowId_;
			rowData_ = ALLOC_NEW(*alloc_) util::XArray<uint8_t>(*alloc_);
			DataStoreUtil::decodeVarSizeBinaryData<S>(in, *rowData_);
		}
		RowId rowId_;
		util::XArray<uint8_t>* rowData_;
	};

	struct UpdateRow : Serializable {
		UpdateRow(util::StackAllocator& alloc) : Serializable(&alloc), rowId_(UNDEF_ROWID), rowData_(NULL) {}
		UpdateRow(util::StackAllocator& alloc, RowId rowId, util::XArray<uint8_t>* data) : Serializable(&alloc), rowId_(rowId), rowData_(data) {}
		~UpdateRow() {
			ALLOC_DELETE(*alloc_, rowData_);
		}

		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {
			out << rowId_;
			DataStoreUtil::encodeVarSizeBinaryData<S>(out, rowData_->data(), rowData_->size());
		}
		template <typename S>
		void decode(S& in) {
			in >> rowId_;
			rowData_ = ALLOC_NEW(*alloc_) util::XArray<uint8_t>(*alloc_);
			DataStoreUtil::decodeVarSizeBinaryData<S>(in, *rowData_);
		}
		RowId rowId_;
		util::XArray<uint8_t>* rowData_;
	};

	struct RemoveRow : public Serializable {
		RemoveRow(util::StackAllocator& alloc) : Serializable(&alloc), rowId_(UNDEF_ROWID) {}
		RemoveRow(util::StackAllocator& alloc, RowId rowId) : Serializable(&alloc), rowId_(rowId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
			out << rowId_;
		}
		template <typename S>
		void decode(S& in) {
			in >> rowId_;
		}
		RowId rowId_;
	};

	struct LockRow : public Serializable {
		LockRow(util::StackAllocator& alloc) : Serializable(&alloc), rowIds_(NULL) {}
		LockRow(util::StackAllocator& alloc, util::XArray<RowId>* data) : Serializable(&alloc), rowIds_(data) {}
		~LockRow() {
			ALLOC_DELETE(*alloc_, rowIds_);
		}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
			out << static_cast<uint64_t>(rowIds_->size());
			for (size_t i = 0; i < rowIds_->size(); i++) {
				out << (*rowIds_)[i];
			}
		}
		template <typename S>
		void decode(S& in) {
			uint64_t numRow;
			in >> numRow;
			rowIds_ = ALLOC_NEW(*alloc_) util::XArray<RowId>(*alloc_);
			rowIds_->resize(static_cast<size_t>(numRow));
			for (uint64_t i = 0; i < numRow; i++) {
				in >> (*rowIds_)[i];
			}
		}
		util::XArray<RowId>* rowIds_;
	};


	DataStoreLogV4(util::StackAllocator& alloc) :
		Serializable(&alloc), type_(UNDEF_EVENT_TYPE), containerId_(UNDEF_CONTAINERID) {
	}

	DataStoreLogV4(util::StackAllocator& alloc, GSEventType type,
		ContainerId containerId, Serializable* mess) :
		Serializable(&alloc), type_(type), containerId_(containerId) {
		switch (type_) {
		case COMMIT_TRANSACTION:
			value_.commit_ = static_cast<Commit*>(mess);
			break;
		case ABORT_TRANSACTION:
			value_.abort_ = static_cast<Abort*>(mess);
			break;
		case PUT_CONTAINER:
			value_.putContainer_ = static_cast<PutContainer*>(mess);
			break;
		case DROP_CONTAINER:
			value_.dropContainer_ = static_cast<DropContainer*>(mess);
			break;
		case CREATE_INDEX:
			value_.createIndex_ = static_cast<CreateIndex*>(mess);
			break;
		case DELETE_INDEX:
			value_.dropIndex_ = static_cast<DropIndex*>(mess);
			break;
		case CONTINUE_CREATE_INDEX:
			value_.continueCreateIndex_ = static_cast<ContinueCreateIndex*>(mess);
			break;
		case CONTINUE_ALTER_CONTAINER:
			value_.continueAlterContainer_ = static_cast<ContinueAlterContainer*>(mess);
			break;
		case PUT_ROW:
			value_.putRow_ = static_cast<PutRow*>(mess);
			break;
		case UPDATE_ROW_BY_ID:
			value_.updateRow_ = static_cast<UpdateRow*>(mess);
			break;
		case REMOVE_ROW_BY_ID:
			value_.removeRow_ = static_cast<RemoveRow*>(mess);
			break;
		case GET_ROW:
			value_.lockRow_ = static_cast<LockRow*>(mess);
			break;
		}
	}
	~DataStoreLogV4() {
		switch (type_) {
		case COMMIT_TRANSACTION:
			ALLOC_DELETE(*alloc_, value_.commit_);
			break;
		case ABORT_TRANSACTION:
			ALLOC_DELETE(*alloc_, value_.abort_);
			break;
		case PUT_CONTAINER:
			ALLOC_DELETE(*alloc_, value_.putContainer_);
			break;
		case DROP_CONTAINER:
			ALLOC_DELETE(*alloc_, value_.dropContainer_);
			break;
		case CREATE_INDEX:
			ALLOC_DELETE(*alloc_, value_.createIndex_);
			break;
		case DELETE_INDEX:
			ALLOC_DELETE(*alloc_, value_.dropIndex_);
			break;
		case CONTINUE_CREATE_INDEX:
			ALLOC_DELETE(*alloc_, value_.continueCreateIndex_);
			break;
		case CONTINUE_ALTER_CONTAINER:
			ALLOC_DELETE(*alloc_, value_.continueAlterContainer_);
			break;
		case PUT_ROW:
			ALLOC_DELETE(*alloc_, value_.putRow_);
			break;
		case UPDATE_ROW_BY_ID:
			ALLOC_DELETE(*alloc_, value_.updateRow_);
			break;
		case REMOVE_ROW_BY_ID:
			ALLOC_DELETE(*alloc_, value_.removeRow_);
			break;
		case GET_ROW:
			ALLOC_DELETE(*alloc_, value_.lockRow_);
			break;
		}
	}

	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}

	template <typename S>
	void encode(S& out) {
		DataStoreUtil::encodeEnumData<S, GSEventType>(out, type_);
		out << containerId_;

		switch (type_) {
		case COMMIT_TRANSACTION:
			value_.commit_->encode(out);
			break;
		case ABORT_TRANSACTION:
			value_.abort_->encode(out);
			break;
		case PUT_CONTAINER:
			value_.putContainer_->encode(out);
			break;
		case DROP_CONTAINER:
			value_.dropContainer_->encode(out);
			break;
		case CREATE_INDEX:
			value_.createIndex_->encode(out);
			break;
		case DELETE_INDEX:
			value_.dropIndex_->encode(out);
			break;
		case CONTINUE_CREATE_INDEX:
			value_.continueCreateIndex_->encode(out);
			break;
		case CONTINUE_ALTER_CONTAINER:
			value_.continueAlterContainer_->encode(out);
			break;
		case PUT_ROW:
			value_.putRow_->encode(out);
			break;
		case UPDATE_ROW_BY_ID:
			value_.updateRow_->encode(out);
			break;
		case REMOVE_ROW_BY_ID:
			value_.removeRow_->encode(out);
			break;
		case GET_ROW:
			value_.lockRow_->encode(out);
			break;
		}
	}
	template <typename S>
	void decode(S& in) {
		DataStoreUtil::decodeEnumData<S, GSEventType>(in, type_);
		in >> containerId_;
		switch (type_) {
		case COMMIT_TRANSACTION: {
			value_.commit_ = ALLOC_NEW(*alloc_) Commit();
			value_.commit_->decode(in);
		} break;
		case ABORT_TRANSACTION: {
			value_.abort_ = ALLOC_NEW(*alloc_) Abort();
			value_.abort_->decode(in);
		} break;
		case PUT_CONTAINER: {
			value_.putContainer_ = ALLOC_NEW(*alloc_) PutContainer(*alloc_);
			value_.putContainer_->decode(in);
		} break;
		case DROP_CONTAINER: {
			value_.dropContainer_ = ALLOC_NEW(*alloc_) DropContainer(*alloc_);
			value_.dropContainer_->decode(in);
		} break;
		case CREATE_INDEX: {
			value_.createIndex_ = ALLOC_NEW(*alloc_) CreateIndex(*alloc_);
			value_.createIndex_->decode(in);
		} break;
		case DELETE_INDEX: {
			value_.dropIndex_ = ALLOC_NEW(*alloc_) DropIndex(*alloc_);
			value_.dropIndex_->decode(in);
		} break;
		case CONTINUE_CREATE_INDEX: {
			value_.continueCreateIndex_ = ALLOC_NEW(*alloc_) ContinueCreateIndex();
			value_.continueCreateIndex_->decode(in);
		} break;
		case CONTINUE_ALTER_CONTAINER: {
			value_.continueAlterContainer_ = ALLOC_NEW(*alloc_) ContinueAlterContainer();
			value_.continueAlterContainer_->decode(in);
		} break;
		case PUT_ROW: {
			value_.putRow_ = ALLOC_NEW(*alloc_) PutRow(*alloc_);
			value_.putRow_->decode(in);
		} break;
		case UPDATE_ROW_BY_ID: {
			value_.updateRow_ = ALLOC_NEW(*alloc_) UpdateRow(*alloc_);
			value_.updateRow_->decode(in);
		} break;
		case REMOVE_ROW_BY_ID: {
			value_.removeRow_ = ALLOC_NEW(*alloc_) RemoveRow(*alloc_);
			value_.removeRow_->decode(in);
		} break;
		case GET_ROW: {
			value_.lockRow_ = ALLOC_NEW(*alloc_) LockRow(*alloc_);
			value_.lockRow_->decode(in);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}
	union UValue {
		Commit* commit_;
		Abort* abort_;
		PutContainer* putContainer_;
		DropContainer* dropContainer_;
		CreateIndex* createIndex_;
		DropIndex* dropIndex_;
		ContinueCreateIndex* continueCreateIndex_;
		ContinueAlterContainer* continueAlterContainer_;
		PutRow* putRow_;
		UpdateRow* updateRow_;
		RemoveRow* removeRow_;
		LockRow* lockRow_;
	};

	GSEventType type_;
	ContainerId containerId_;
	UValue value_;
};

struct TxnLog : public Serializable {
	TxnLog(util::StackAllocator& alloc)
		: Serializable(&alloc), clientId_(),
		txnId_(UNDEF_TXNID), containerId_(UNDEF_CONTAINERID),
		stmtId_(UNDEF_STATEMENTID), txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
		getMode_(TransactionManager::AUTO), txnMode_(TransactionManager::AUTO_COMMIT), storeType_(UNDEF_STORE), dsLog_(NULL) {}
	TxnLog(TransactionContext& txn, const TransactionManager::ContextSource& cxtSrc,
		StatementId stmtId, StoreType storeType, DataStoreLogV4* dsLog)
		: Serializable((&txn.getDefaultAllocator())), clientId_(txn.getClientId()),
		txnId_(txn.getId()), containerId_(txn.getContainerId()),
		stmtId_(stmtId), txnTimeoutInterval_(txn.getTransationTimeoutInterval()),
		getMode_(cxtSrc.getMode_), txnMode_(cxtSrc.txnMode_), storeType_(storeType), dsLog_(dsLog) {}
	TxnLog(TransactionContext& txn, const ClientId &clientId, const TransactionManager::ContextSource& cxtSrc,
		StatementId stmtId, StoreType storeType, DataStoreLogV4* dsLog)
		: Serializable(&(txn.getDefaultAllocator())), clientId_(clientId),
		txnId_(txn.getId()), containerId_(txn.getContainerId()),
		stmtId_(stmtId), txnTimeoutInterval_(txn.getTransationTimeoutInterval()),
		getMode_(cxtSrc.getMode_), txnMode_(cxtSrc.txnMode_), storeType_(storeType), dsLog_(dsLog) {}
	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}

	template <typename S>
	void encode(S& out) {
		DataStoreUtil::encodeEnumData<S, StoreType>(out, storeType_);

		out << clientId_.sessionId_;
		out.writeAll(clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << txnId_;
		out << containerId_;
		out << stmtId_;

		int8_t autoCommit = (txnMode_ == TransactionManager::AUTO_COMMIT) ? 1 : 0;
		out << autoCommit;
		if (autoCommit == 0) {
			out << txnTimeoutInterval_;
			out << getMode_;
			out << txnMode_;
		}

		dsLog_->encode(out);
	}
	template <typename S>
	void decode(S& in) {
		DataStoreUtil::decodeEnumData<S, StoreType>(in, storeType_);

		in >> clientId_.sessionId_;
		in.readAll(clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		in >> txnId_;
		in >> containerId_;
		in >> stmtId_;

		int8_t autoCommit;
		in >> autoCommit;
		if (autoCommit == 0) {
			in >> txnTimeoutInterval_;
			in >> getMode_;
			in >> txnMode_;
		}

		dsLog_ = ALLOC_NEW(*alloc_) DataStoreLogV4(*alloc_);
		dsLog_->decode(in);
	}
	void decode() {
		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(data_, size_));
		decode<util::ArrayByteInStream>(in);
	}
	bool isTransactionEnd() {
		if (!dsLog_) {
			return false;
		}
		switch (dsLog_->type_) {
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
		case PUT_CONTAINER:
		case DROP_CONTAINER:
		case CREATE_INDEX:
		case DELETE_INDEX:
			return true;
		case CONTINUE_CREATE_INDEX:
		case CONTINUE_ALTER_CONTAINER:
			return false;
		case PUT_ROW:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW_BY_ID:
			return (txnMode_ == TransactionManager::AUTO_COMMIT);
		case GET_ROW:
			return false;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Unknown event type: " << dsLog_->type_);
			return false;
		}
	}

	ClientId clientId_;
	TransactionId txnId_;
	ContainerId containerId_;
	StatementId stmtId_;

	int32_t txnTimeoutInterval_;
	TransactionManager::GetMode getMode_;
	TransactionManager::TransactionMode txnMode_;

	StoreType storeType_;
	DataStoreLogV4* dsLog_;
};

#endif
