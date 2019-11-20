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
	@brief Definition of SyncService
*/
#ifndef SYNC_SERVICE_H_
#define SYNC_SERVICE_H_

#include "cluster_service.h"
#include "event_engine.h"
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
		SyncManager *syncMgr, PartitionId pId) : alloc_(&alloc), eventType_(eventType),
		pId_(pId), syncMgr_(syncMgr), pt_(syncMgr_->getPartitionTable()),
		validCheckValue_(0) {};

	SyncId getSyncId() {
		return syncId_;
	}

	SyncId &getBackupSyncId() {
		return backupSyncId_;
	}

	void setBackupSyncId(SyncId &syncId) {
		backupSyncId_ = syncId;
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

	util::StackAllocator &getAllocator() {
		return *alloc_;
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
			request_(mode), binaryLogRecords_(alloc), chunks_(alloc),
			ee_(ee) {
		request_.requestInfo_ = this;
	};

	void set(EventType eventType, SyncContext *context, LogSequentialNumber lsn,
			SyncId &backupSyncId) {
		eventType_ = eventType;
		request_.ptRev_ = context->getPartitionRevision();
		context->getSyncId(syncId_);
		backupSyncId_ = backupSyncId;
		request_.ownerLsn_ = lsn;
	}

	void set(EventType eventType) {
		eventType_ = eventType;
	}
	bool check() {
		return true;
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
		int32_t chunkNum, int32_t chunkSize) {
		condition_.chunkNum_ = chunkNum;
		condition_.chunkSize_ = chunkSize;
		request_.chunkSize_ = chunkSize;
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
	uint32_t getBinaryLogSize() {
		return request_.binaryLogSize_;
	}
	int64_t getChunkNum() {
		return request_.numChunk_;
	}

	bool getLogs(PartitionId pId, int64_t sId, LogManager *logMgr,
			CheckpointService *cpSvc, bool useLongSyncLog, bool isCheck = false);
	bool getChunks(util::StackAllocator &alloc, PartitionId pId, PartitionTable *pt,
			LogManager *logMgr, ChunkManager *chunkMgr,
			CheckpointService *cpSvc, int64_t syncId, uint64_t &totalCount);

	void encode(EventByteOutStream &out);

	void decode(EventByteInStream &in, const char8_t *bodyBuffer);

	LogSequentialNumber getLsn() {
		return request_.ownerLsn_;
	}

	std::string dump(int32_t mode = 0);

	/*!
		@brief Encodes/Decodes the message for sync request
	*/
	struct Request {
		Request(int32_t mode) : syncMode_(mode),
				ownerLsn_(UNDEF_LSN), startLsn_(UNDEF_LSN), endLsn_(UNDEF_LSN),
				chunkSize_(0), numChunk_(0), binaryLogSize_(0)
		{}

		~Request(){};

		void encode(EventByteOutStream &out, bool isOwner = true) const;

		void decode(EventByteInStream &in);

		std::string dump();


		int32_t syncMode_;
		PartitionRevision ptRev_;
		LogSequentialNumber ownerLsn_;
		LogSequentialNumber startLsn_;
		LogSequentialNumber endLsn_;
		uint32_t chunkSize_;
		uint32_t numChunk_;
		uint32_t binaryLogSize_;
		SyncRequestInfo *requestInfo_;
	};

	LogSequentialNumber getStartLsn() {
		return request_.startLsn_;
	}
	LogSequentialNumber getEndLsn() {
		return request_.endLsn_;
	}

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
};

/*!
	@brief Represents the information for sync response
*/
class SyncResponseInfo : public SyncManagerInfo {
public:
	SyncResponseInfo(util::StackAllocator &alloc, EventType eventType,
			SyncManager *syncMgr, PartitionId pId, SyncMode mode, LogSequentialNumber lsn)
			: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		response_(mode), lsn_(lsn), binaryLogRecords_(alloc) {
		response_.responseInfo_ = this;
	};

	void set(EventType type, SyncRequestInfo &syncRequestInfo,
		LogSequentialNumber lsn, SyncContext *context) {
		eventType_ = type;
		response_.syncMode_ = syncRequestInfo.request_.syncMode_;
		response_.ptRev_ = syncRequestInfo.request_.ptRev_;
		syncId_ = syncRequestInfo.syncId_;
		backupSyncId_ = syncRequestInfo.backupSyncId_;
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
		Response(int32_t mode) : syncMode_(mode),
				targetLsn_(UNDEF_LSN) {}

		~Response(){};

		void encode(EventByteOutStream &out) const;

		void decode(EventByteInStream &in);

		std::string dump();

		int32_t syncMode_;
		PartitionRevision ptRev_;
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
class SyncCheckEndInfo : public SyncManagerInfo {
public:
	SyncCheckEndInfo(util::StackAllocator &alloc, EventType eventType,
			SyncManager *syncMgr, PartitionId pId, SyncMode mode, SyncContext *context)
			: SyncManagerInfo(alloc, eventType, syncMgr, pId),
			syncMode_(mode), context_(context) {
		if (context != NULL) {
			context->getSyncId(syncId_);
			ptRev_ = context->getPartitionRevision();
		}
	};

	void encode(EventByteOutStream &out);
	void decode(EventByteInStream &in, const char8_t *bodyBuffer);

	PartitionRevision &getRevision() {
		return ptRev_;
	}

	PartitionRevisionNo &getRevisionNo() {
		return ptRev_.sequentialNumber_;
	}

	SyncMode getMode() {
		return syncMode_;
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
			SyncManager *syncMgr, PartitionId pId, bool forceFlag, PartitionRevisionNo revision = 0)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId), forceFlag_(forceFlag), revision_(revision) {
	};

	bool isForce() {
		return forceFlag_;
	}

	bool check() {
		return true;
	}
	PartitionRevisionNo getPartitionRevision() {
		return revision_;
	}

	std::string dump();

	MSGPACK_DEFINE(forceFlag_,revision_);

private:
	bool forceFlag_;
	PartitionRevisionNo revision_;
};

/*!
	@brief Handles sync event
*/
class SyncHandler : public EventHandler {
public:
	static const int32_t UNDEF_DELAY_TIME = -1;
	SyncHandler() {};
	~SyncHandler() {};

	void initialize(const ManagerSet &mgrSet);

	void operator()(EventContext &, EventEngine::Event &) {};

	void removePartition(util::StackAllocator &alloc,
		PartitionId pId, EventType eventType, const util::DateTime &now,
		const EventMonotonicTime emNow);


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

	void sendResponse(EventContext &ec, EventType eventType,
			PartitionId pId, SyncRequestInfo &requestInfo,
			SyncResponseInfo &responseInfo, SyncContext *context,  NodeId senderNodeId);

	void sendEvent(EventContext &ec, EventEngine &ee, NodeId targetNodeId,
			Event &ev, int32_t delayTime = UNDEF_DELAY_TIME);

	void sendRequest(EventContext &ec, EventEngine &ee, EventType eventType,
			PartitionId pId, SyncRequestInfo &requestInfo,
			NodeId senderNodeId, int32_t waitTime = 0);

	template <typename T>
	void sendMultiRequest(EventContext &ec, EventEngine &ee, EventType eventType,
			PartitionId pId, SyncRequestInfo &requestInfo,
			T &t, SyncContext *context);

	void checkRestored(util::StackAllocator &alloc, PartitionId pId,
			EventType eventType, const util::DateTime &now,
			const EventMonotonicTime emNow);

	void addCheckEndEvent(EventContext &ec, PartitionId pId,
			util::StackAllocator &alloc, SyncMode mode, SyncContext *context,
			bool isImmediate);

	void addLongtermSyncCheckEvent(EventContext &ec, PartitionId pId,
			util::StackAllocator &alloc, SyncContext *context,
			int32_t longtermSyncType);
};

/*!
	@brief Handles short-term Sync
*/
class ShortTermSyncHandler : public SyncHandler {
public:
	ShortTermSyncHandler(){};

	void operator()(EventContext &ec, EventEngine::Event &ev);

private:
	void decodeLongSyncRequestInfo(
			PartitionId pId, EventByteInStream &in,
			SyncId &longSyncId, SyncContext *&longSyncContext);

	int64_t getTargetSSN(PartitionId pId, SyncContext *context);

	void undoPartition(util::StackAllocator &alloc, SyncContext *context, PartitionId pId);

	void executeSyncRequest(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo);

	void executeSyncStart(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncStartAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncLog(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncLogAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncEnd(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncEndAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void checkOwner(LogSequentialNumber ownerLsn, SyncContext *context, bool isOwner = true);
	void checkBackup(NodeId backupNodeId,
			LogSequentialNumber backupLsn, SyncContext *context);
};

/*!
	@brief Handles long-term Sync
*/
class LongTermSyncHandler : public SyncHandler {
public:
	LongTermSyncHandler(){};

	void operator() (EventContext &ec, EventEngine::Event &ev);

	void requestShortTermSync(EventContext &ec, PartitionId pId, SyncContext *context);

	void executeSyncRequest(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo);

	void executeSyncPrepareAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, LongtermSyncInfo &syncInfo);

	void executeSyncStart(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);
	
	void executeSyncStartAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncChunk(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncChunkAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncLog(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);

	void executeSyncLogAck(EventContext &ec, Event &ev, PartitionId pId,
			SyncContext *context, SyncRequestInfo &syncRequestInfo,
			SyncResponseInfo &syncResponseInfo, NodeId senderNodeId);
};


/*!
	@brief Handles Sync Timeout
*/
class SyncCheckEndHandler : public SyncHandler {
public:
	SyncCheckEndHandler(){};
	void operator() (EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles DropPartition
*/
class DropPartitionHandler : public SyncHandler {
public:
	DropPartitionHandler() {};

	void operator() (EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles receiving Sync message
*/
class RecvSyncMessageHandler : public SyncHandler {
public:
	RecvSyncMessageHandler() {};

	void operator()(EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief Handles unknown Sync event
*/
class UnknownSyncEventHandler : public SyncHandler {
public:
	UnknownSyncEventHandler() {};

	void operator() (EventContext &ec, EventEngine::Event &ev);
};

/*!
	@brief SyncService
*/
class SyncService {
public:
	SyncService(const ConfigTable &config, EventEngine::Config &eeConfig,
			EventEngine::Source source, const char8_t *name, SyncManager &syncMgr,
			ClusterVersionId versionId, ServiceThreadErrorHandler &serviceThreadErrorHandler);

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
			void decode(util::StackAllocator &alloc, EventEngine::Event &ev, T &t, EventByteInStream &in);

	template <class T>
			void encode(EventEngine::Event &ev, T &t, EventByteOutStream &out);

	void requestDrop(const Event::Source &eventSource,
		util::StackAllocator &alloc, PartitionId pId, bool isForce, PartitionRevisionNo revision = 0);

	void notifyCheckpointLongSyncReady(EventContext &ec, PartitionId pId,
			LongtermSyncInfo *syncInfo, bool errorOccured);

	SyncManager *getManager() {
		return syncMgr_;
	}

private:
	void checkVersion(ClusterVersionId decodedVersion);
	uint16_t getLogVersion(PartitionId pId);

	EventEngine ee_;
	SyncManager *syncMgr_;
	TransactionService *txnSvc_;
	ClusterService *clsSvc_;
	LogManager *logMgr_;

	const uint8_t versionId_;
	RecvSyncMessageHandler recvSyncMessageHandler_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;
	UnknownSyncEventHandler unknownSyncEventHandler_;


	bool initailized_;
};





#endif
