/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	@brief Definition of SyncService
*/
#ifndef SYNC_SERVICE_H_
#define SYNC_SERVICE_H_

#include "util/container.h"
#include "cluster_manager.h"
#include "cluster_service.h"
#include "event_engine.h"
#include "gs_error.h"
#include "sync_manager.h"

class ClusterService;
class TransactionService;
class ClusterHandler;
class TriggerService;
class CheckpointService;
class SystemService;
class SyncService;
class SyncManager;
class RecoveryManager;
class ChunkManager;
class DataStore;
class LogManager;
class TransactionManager;
class StatementHandler;

class SyncResponseInfo;

/*!
	@brief Represents the information for SyncManager
*/
class SyncManagerInfo {
	friend class SyncManagerTest;

public:
	SyncManagerInfo(util::StackAllocator &alloc, EventType eventType,
		SyncManager *syncMgr, PartitionId pId)
		: alloc_(&alloc),
		  eventType_(eventType),
		  pId_(pId),
		  syncMgr_(syncMgr),
		  pt_(syncMgr_->getPartitionTable()),
		  validCheckValue_(0){};

	SyncId getSyncId() {
		return syncId_;
	}

	SyncId &getBackupSyncId() {
		return backupSyncId_;
	}

	PartitionId getPartitionId() {
		return pId_;
	}

	PartitionRole &getPartitionRole() {
		return partitionRole_;
	}

	PartitionRevision &getRevision() {
		return partitionRole_.getRevision();
	}

	void setPartitionRole(PartitionRole &role) {
		partitionRole_ = role;
	}

	EventType getEventType() {
		return eventType_;
	}

	void setEventType(EventType eventType) {
		eventType_ = eventType;
	}

	void encode(EventByteOutStream &out);
	void decode(EventByteInStream &in, const char8_t *bodyBuffer);

	std::string dump();

protected:
	util::StackAllocator *alloc_;
	EventType eventType_;
	PartitionId pId_;
	SyncManager *syncMgr_;
	PartitionTable *pt_;
	SyncId syncId_;
	SyncId backupSyncId_;
	PartitionRole partitionRole_;

private:
	static const uint8_t VALID_VALUE = 0;

	uint8_t validCheckValue_;
};

/*!
	@brief Represents the information for sync request
*/
class SyncRequestInfo : public SyncManagerInfo {
	friend class SyncResponseInfo;

public:
	SyncRequestInfo(util::StackAllocator &alloc, EventType eventType,
		SyncManager *syncMgr, PartitionId pId, EventEngine *ee,
		SyncMode mode = MODE_SHORTTERM_SYNC)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		  request_(mode),
		  binaryLogRecords_(alloc),
		  chunks_(alloc),
		  ee_(ee),
		  isDownNextOwner_(false) {
		request_.requestInfo_ = this;
	};

	void set(EventType eventType, SyncContext *context, LogSequentialNumber lsn,
		SyncId &backupSyncId) {
		eventType_ = eventType;
		request_.ptRev_ = context->getPartitionRevision();
		context->getSyncId(syncId_);
		backupSyncId_ = backupSyncId;
		request_.stmtId_ = context->createStatementId();
		request_.ownerLsn_ = lsn;
	}

	void set(EventType eventType) {
		eventType_ = eventType;
	}

	void noUseLog() {
		request_.startLsn_ = request_.ownerLsn_;
		request_.endLsn_ = request_.ownerLsn_;
		binaryLogRecords_.clear();
	}

	void setSearchLsnRange(
		LogSequentialNumber startLsn, LogSequentialNumber endLsn = UNDEF_LSN) {
		binaryLogRecords_.clear();
		request_.startLsn_ = startLsn;
		request_.endLsn_ = endLsn;
	}

	void setSearchChunkCondition(
		int32_t chunkNum, int32_t chunkSize, int32_t totalChunkNum) {
		condition_.chunkNum_ = chunkNum;
		condition_.chunkSize_ = chunkSize;
		request_.chunkSize_ = chunkSize;
		request_.totalChunkNum_ = totalChunkNum;
	}

	void clearSearchCondition() {
		request_.startLsn_ = UNDEF_LSN;
		request_.endLsn_ = UNDEF_LSN;
		request_.chunkSize_ = 0;
		request_.numChunk_ = 0;
		request_.binaryLogSize_ = 0;
	}

	util::XArray<uint8_t> &getLogRecords() {
		return binaryLogRecords_;
	}

	util::XArray<uint8_t *> &getChunkList() {
		return chunks_;
	}

	bool getLogs(PartitionId pId, LogManager *logMgr);

	bool getChunks(PartitionId pId, PartitionTable *pt, LogManager *logMgr,
		ChunkManager *chunkMgr, CheckpointService *cpSvc);

	void encode(EventByteOutStream &out);

	void decode(EventByteInStream &in, const char8_t *bodyBuffer);

	LogSequentialNumber getLsn() {
		return request_.ownerLsn_;
	}

	void setIsDownNextOwner() {
		isDownNextOwner_ = true;
	}

	bool getIsDownNextOwner() {
		return isDownNextOwner_;
	}

	std::string dump(int32_t mode = 0);

	/*!
		@brief Encodes/Decodes the message for sync request
	*/
	struct Request {
		Request(int32_t mode)
			: syncMode_(mode),
			  stmtId_(UNDEF_STATEMENTID),
			  ownerLsn_(UNDEF_LSN),
			  startLsn_(UNDEF_LSN),
			  endLsn_(UNDEF_LSN),
			  chunkSize_(0),
			  numChunk_(0),
			  binaryLogSize_(0),
			  totalChunkNum_(0) {}

		~Request(){};

		void encode(EventByteOutStream &out, bool isOwner = true) const;

		void decode(EventByteInStream &in);

		std::string dump();


		int32_t syncMode_;
		PartitionRevision ptRev_;
		StatementId stmtId_;
		LogSequentialNumber ownerLsn_;
		LogSequentialNumber startLsn_;
		LogSequentialNumber endLsn_;
		uint32_t chunkSize_;
		uint32_t numChunk_;
		uint32_t binaryLogSize_;
		uint32_t totalChunkNum_;
		SyncRequestInfo *requestInfo_;
	};

private:
	/*!
		@brief Represents the information for search condition
	*/
	struct SearchCondition {
		SearchCondition() : logSize_(0), chunkSize_(0), chunkNum_(0) {}

		uint32_t logSize_;
		uint32_t chunkSize_;
		uint32_t chunkNum_;
	};

	Request request_;
	SearchCondition condition_;
	util::XArray<uint8_t> binaryLogRecords_;
	util::XArray<uint8_t *> chunks_;
	EventEngine *ee_;
	bool isDownNextOwner_;
};

/*!
	@brief Represents the information for sync response
*/
class SyncResponseInfo : public SyncManagerInfo {
public:
	SyncResponseInfo(util::StackAllocator &alloc, EventType eventType,
		SyncManager *syncMgr, PartitionId pId, SyncMode mode,
		LogSequentialNumber lsn)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		  response_(mode),
		  lsn_(lsn),
		  binaryLogRecords_(alloc) {
		response_.responseInfo_ = this;
	};

	void set(EventType type, SyncRequestInfo &syncRequestInfo,
		LogSequentialNumber lsn, SyncContext *context) {
		eventType_ = type;
		response_.syncMode_ = syncRequestInfo.request_.syncMode_;
		response_.ptRev_ = syncRequestInfo.request_.ptRev_;
		syncId_ = syncRequestInfo.syncId_;
		backupSyncId_ = syncRequestInfo.backupSyncId_;
		response_.stmtId_ = syncRequestInfo.request_.stmtId_;
		response_.targetLsn_ = lsn;
		if (context != NULL) {
			context->getSyncId(backupSyncId_);
		}
	}

	LogSequentialNumber getTargetLsn() {
		return response_.targetLsn_;
	}

	void encode(EventByteOutStream &out);

	void decode(EventByteInStream &in, const char8_t *bodyBuffer);

	std::string dump();

	/*!
		@brief Encodes/Decodes the message for sync response
	*/
	struct Response {
		Response(int32_t mode)
			: syncMode_(mode),
			  stmtId_(UNDEF_STATEMENTID),
			  targetLsn_(UNDEF_LSN) {}

		~Response(){};

		void encode(EventByteOutStream &out) const;

		void decode(EventByteInStream &in);

		std::string dump();

		int32_t syncMode_;
		PartitionRevision ptRev_;
		StatementId stmtId_;
		LogSequentialNumber targetLsn_;
		SyncResponseInfo *responseInfo_;
	};

private:
	Response response_;
	LogSequentialNumber lsn_;
	util::XArray<uint8_t> binaryLogRecords_;
};

/*!
	@brief Represents the information for sync timeout
*/
class SyncTimeoutInfo : public SyncManagerInfo {
public:
	SyncTimeoutInfo(util::StackAllocator &alloc, EventType eventType,
		SyncManager *syncMgr, PartitionId pId, SyncMode mode,
		SyncContext *context)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		  syncMode_(mode),
		  context_(context) {
		if (context != NULL) {
			context->getSyncId(syncId_);
			ptRev_ = context->getPartitionRevision();
		}
	};

	bool check(PartitionId pId, PartitionTable *pt);

	void encode(EventByteOutStream &out);
	void decode(EventByteInStream &in, const char8_t *bodyBuffer);

	PartitionRevision &getRevision() {
		return ptRev_;
	}

	std::string dump();

private:
	SyncMode syncMode_;
	SyncContext *context_;
	PartitionRevision ptRev_;
};

/*!
	@brief Represents the information for drop partition
*/
class DropPartitionInfo : public SyncManagerInfo {
public:
	DropPartitionInfo(util::StackAllocator &alloc, EventType eventType,
		SyncManager *syncMgr, PartitionId pId, bool forceFlag = false)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		  forceFlag_(forceFlag){};

	bool isForce() {
		return forceFlag_;
	}

	bool check() {
		return true;
	}

	std::string dump();

	MSGPACK_DEFINE(forceFlag_);

private:
	bool forceFlag_;
};

/*!
	@brief Handles sync event
*/
class SyncHandler : public EventHandler {
public:
	SyncHandler(){};
	~SyncHandler(){};

	void initialize(const ManagerSet &mgrSet);

	void operator()(EventContext &, EventEngine::Event &){};

	void removePartition(EventContext &ec, EventEngine::Event &ev);

	/*!
		@brief Represents the context to control the synchronization
	*/
	struct SyncControlContext {
		SyncControlContext(
			EventType eventType, PartitionId pId, LogSequentialNumber lsn)
			: eventType_(eventType),
			  waitTime_(0),
			  sendLogSize_(0),
			  sendChunkNum_(0),
			  pId_(pId),
			  lsn_(lsn){};

		int32_t getWaitTime() {
			return waitTime_;
		}

		EventRequestOption *getOption() {
			return &option_;
		}

		EventType eventType_;
		int32_t waitTime_;
		int32_t sendLogSize_;
		int32_t sendChunkNum_;
		PartitionId pId_;
		LogSequentialNumber lsn_;
		EventRequestOption option_;
	};


	PartitionTable *pt_;
	ClusterManager *clsMgr_;
	ClusterService *clsSvc_;

	TransactionService *txnSvc_;
	TransactionManager *txnMgr_;
	EventEngine *txnEE_;

	SyncService *syncSvc_;
	SyncManager *syncMgr_;
	EventEngine *syncEE_;

	RecoveryManager *recoveryMgr_;
	LogManager *logMgr_;
	ChunkManager *chunkMgr_;
	DataStore *ds_;
	CheckpointService *cpSvc_;

protected:
	class SyncPartitionLock {
	public:
		SyncPartitionLock(TransactionManager *txnMgr, PartitionId pId);
		~SyncPartitionLock();

	private:
		TransactionManager *txnMgr_;
		PartitionId pId_;
	};

	int32_t getTransactionEEQueueSize(PartitionId pId);

	void addTimeoutEvent(EventContext &ec, PartitionId pId,
		util::StackAllocator &alloc, SyncMode mode, SyncContext *context,
		bool isImmediate = false);

	bool controlSyncLoad(SyncControlContext &control);
};

/*!
	@brief Handles short-term Sync
*/
class ShortTermSyncHandler : public SyncHandler {
public:
	ShortTermSyncHandler(){};

	void operator()(EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles long-term Sync
*/
class LongTermSyncHandler : public SyncHandler {
public:
	LongTermSyncHandler(){};

	void operator()(EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles Sync Timeout
*/
class SyncTimeoutHandler : public SyncHandler {
public:
	SyncTimeoutHandler(){};
	void operator()(EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles DropPartition
*/
class DropPartitionHandler : public SyncHandler {
public:
	DropPartitionHandler(){};

	void operator()(EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles receiving Sync message
*/
class RecvSyncMessageHandler : public SyncHandler {
public:
	RecvSyncMessageHandler(){};

	void operator()(EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles unknown Sync event
*/
class UnknownSyncEventHandler : public SyncHandler {
public:
	UnknownSyncEventHandler(){};

	void operator()(EventContext &ec, EventEngine::Event &ev);
};


/*!
	@brief SyncService
*/
class SyncService {
public:
	SyncService(const ConfigTable &config, EventEngine::Config &eeConfig,
		EventEngine::Source source, const char8_t *name, SyncManager &syncMgr,
		ClusterVersionId versionId,
		ServiceThreadErrorHandler &serviceThreadErrorHandler);

	~SyncService();

	static EventEngine::Config &createEEConfig(
		const ConfigTable &config, EventEngine::Config &eeConfig);
	void initialize(ManagerSet &mgrSet);

	void start();

	void shutdown();

	void waitForShutdown();

	EventEngine *getEE() {
		return &ee_;
	}

	template <class T>
	void decode(util::StackAllocator &alloc, EventEngine::Event &ev, T &t);

	template <class T>
	void encode(EventEngine::Event &ev, T &t);

	void requestDrop(const Event::Source &eventSource,
		util::StackAllocator &alloc, PartitionId pId, bool isForce = false);

	SyncManager *getManager() {
		return syncMgr_;
	}

	void setDumpTargetPId(PartitionId pId) {
		dumpTargetPId_ = pId;
	}

	bool checkDumpTargetPId(PartitionId pId) {
		if (dumpTargetPId_ == UNDEF_PARTITIONID || (dumpTargetPId_ == pId)) {
			return true;
		}
		else {
			return false;
		}
	}


private:
	void checkVersion(ClusterVersionId decodedVersion);

	EventEngine ee_;
	SyncManager *syncMgr_;
	TransactionService *txnSvc_;
	ClusterService *clsSvc_;

	const uint8_t versionId_;
	RecvSyncMessageHandler recvSyncMessageHandler_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;
	UnknownSyncEventHandler unknownSyncEventHandler_;

	bool initailized_;
	PartitionId dumpTargetPId_;
};

#endif
