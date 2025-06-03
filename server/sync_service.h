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
#include "log_manager.h"

class ClusterService;
class TransactionService;
class ClusterHandler;
class TriggerService;
class CheckpointService;
class SystemService;
class SyncService;
class SyncManager;
class RecoveryManager;
class TransactionManager;
class StatementHandler;
class SyncResponseInfo;
class NoLocker;
class MutexLocker;
class Partition;
template <class L> class LogManager;
class PartitionList;
struct SyncRedoRequestInfo;

#define TRACE_SYNC_EXCEPTION(e, eventType, pId, level, str)                 \
	UTIL_TRACE_EXCEPTION_##level(SYNC_SERVICE, e,                           \
		str << ", eventType=" << EventTypeUtility::getEventTypeName(eventType) \
				<< ", pId=" << pId << ", reason=" << ", reason=" << GS_EXCEPTION_MESSAGE(e))

#include "uuid_utils.h"
typedef util::VariableSizeAllocator<util::Mutex> GlobalRedoVariableSizeAllocator;
/*!
	@brief Represents the information for SyncManager
*/
class SyncManagerInfo {
	friend class SyncManagerTest;
public:
	SyncManagerInfo(util::StackAllocator& alloc, EventType eventType,
		SyncManager* syncMgr, PartitionId pId) :
		alloc_(&alloc), eventType_(eventType),
		pId_(pId), syncMgr_(syncMgr),
		pt_(syncMgr_->getPartitionTable()),
		validCheckValue_(0) {};

	SyncId getSyncId() {
		return syncId_;
	}

	SyncId& getBackupSyncId() {
		return backupSyncId_;
	}

	void setSyncId(SyncId& syncId) {
		syncId_ = syncId;
	}

	void setBackupSyncId(SyncId& syncId) {
		backupSyncId_ = syncId;
	}

	PartitionId getPartitionId() {
		return pId_;
	}

	PartitionRole& getPartitionRole() {
		return partitionRole_;
	}

	PartitionRevision& getRevision() {
		return partitionRole_.getRevision();
	}

	void setPartitionRole(PartitionRole& role) {
		partitionRole_ = role;
	}

	EventType getEventType() {
		return eventType_;
	}

	void setEventType(EventType eventType) {
		eventType_ = eventType;
	}

	util::StackAllocator& getAllocator() {
		return *alloc_;
	}

	void encode(EventByteOutStream& out);
	void decode(
		EventByteInStream& in, const char8_t* bodyBuffer);

	std::string dump();

protected:
	util::StackAllocator* alloc_;
	EventType eventType_;
	PartitionId pId_;
	SyncManager* syncMgr_;
	PartitionTable* pt_;
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
	SyncRequestInfo(
		util::StackAllocator& alloc, EventType eventType,
		SyncManager* syncMgr, PartitionId pId, EventEngine* ee,
		SyncMode mode = MODE_SHORTTERM_SYNC)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		request_(mode), binaryLogRecords_(alloc), chunks_(alloc),
		ee_(ee) {
		request_.requestInfo_ = this;
	};

	void set(
		EventType eventType, SyncContext* context, LogSequentialNumber lsn,
		SyncId& backupSyncId) {

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
		request_.startLsnNext_ = false;
		request_.endLsnNext_ = false;
		binaryLogRecords_.clear();
	}

	void setSearchLsnRange(
		LogSequentialNumber startLsn, 
		LogSequentialNumber endLsn) {

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
		request_.startLsnNext_ = false;
		request_.endLsn_ = UNDEF_LSN;
		request_.endLsnNext_ = false;
		request_.chunkSize_ = 0;
		request_.numChunk_ = 0;
		request_.binaryLogSize_ = 0;
	}

	util::XArray<uint8_t>& getLogRecords() {
		return binaryLogRecords_;
	}

	util::XArray<uint8_t*>& getChunkList() {
		return chunks_;
	}
	uint32_t getBinaryLogSize() {
		return request_.binaryLogSize_;
	}
	int64_t getChunkNum() {
		return request_.numChunk_;
	}

	void setSyncMode(SyncMode mode) {
		request_.syncMode_ = mode;
	}

	void setPartitionRevision(PartitionRevision& ptRev) {
		request_.ptRev_ = ptRev;
	}


	bool getLogs(
		PartitionId pId, int64_t sId, LogManager<MutexLocker>* logMgr,
		CheckpointService* cpSvc,
		bool useLongSyncLog, bool isCheck, SearchLogInfo& logInfo);

	bool getLogs(PartitionId pId, int64_t startLogVersion, int64_t startOffset, LogManager<MutexLocker>* logMgr);


	bool getChunks(
		util::StackAllocator& alloc, PartitionId pId,
		PartitionTable* pt, CheckpointService* cpSvc,
		Partition* partition,
		int64_t syncId, uint64_t& totalCount);

	void encode(EventByteOutStream& out);

	void decode(
		EventByteInStream& in, const char8_t* bodyBuffer);

	LogSequentialNumber getLsn() {
		return request_.ownerLsn_;
	}

	std::string dump(int32_t mode = 0);

	/*!
		@brief Encodes/Decodes the message for sync request
	*/
	struct Request {
		Request(int32_t mode) : syncMode_(mode),
			ownerLsn_(UNDEF_LSN),
			startLsn_(UNDEF_LSN), endLsn_(UNDEF_LSN),
			chunkSize_(0), numChunk_(0), binaryLogSize_(0),
			startLsnNext_(false), endLsnNext_(false)
		{}

		~Request() {};

		void encode(
			EventByteOutStream& out, bool isOwner = true);

		void decode(EventByteInStream& in);

		std::string dump();


		int32_t syncMode_;
		PartitionRevision ptRev_;
		LogSequentialNumber ownerLsn_;
		LogSequentialNumber startLsn_;
		LogSequentialNumber endLsn_;
		uint32_t chunkSize_;
		uint32_t numChunk_;
		uint32_t binaryLogSize_;
		SyncRequestInfo* requestInfo_;
		bool startLsnNext_;  
		bool endLsnNext_;    
	};

	LogSequentialNumber getStartLsn() {
		return request_.startLsn_;
	}
	LogSequentialNumber getEndLsn() {
		return request_.endLsn_;
	}
	bool isStartLsnNext() {
		return request_.startLsnNext_;
	}
	bool isEndLsnNext() {
		return request_.endLsnNext_;
	}

private:
	/*!
		@brief Represents the information for search condition
	*/
	struct SearchCondition {
		SearchCondition() :
			logSize_(0), chunkSize_(0), chunkNum_(0) {}

		uint32_t logSize_;
		uint32_t chunkSize_;
		uint32_t chunkNum_;
	};

	Request request_;
	SearchCondition condition_;
	util::XArray<uint8_t> binaryLogRecords_;
	util::XArray<uint8_t*> chunks_;
	EventEngine* ee_;
};

/*!
	@brief Represents the information for sync response
*/
class SyncResponseInfo : public SyncManagerInfo {
public:
	SyncResponseInfo(
		util::StackAllocator& alloc, EventType eventType,
		SyncManager* syncMgr, PartitionId pId,
		SyncMode mode, LogSequentialNumber lsn)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		response_(mode), lsn_(lsn), binaryLogRecords_(alloc) {
		response_.responseInfo_ = this;
	};

	void set(EventType type, SyncRequestInfo& syncRequestInfo,
		LogSequentialNumber lsn, SyncContext* context) {
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

	void encode(EventByteOutStream& out);

	void decode(
		EventByteInStream& in, const char8_t* bodyBuffer);

	std::string dump();

	/*!
		@brief Encodes/Decodes the message for sync response
	*/
	struct Response {
		Response(int32_t mode) : syncMode_(mode), targetLsn_(UNDEF_LSN) {}
		~Response() {};

		void encode(EventByteOutStream& out);

		void decode(EventByteInStream& in);

		std::string dump();

		int32_t syncMode_;
		PartitionRevision ptRev_;
		LogSequentialNumber targetLsn_;
		SyncResponseInfo* responseInfo_;
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
	SyncCheckEndInfo(
		util::StackAllocator& alloc, EventType eventType,
		SyncManager* syncMgr, PartitionId pId,
		SyncMode mode, SyncContext* context)
		: SyncManagerInfo(alloc, eventType, syncMgr, pId),
		syncMode_(mode), context_(context) {
		if (context != NULL) {
			context->getSyncId(syncId_);
			ptRev_ = context->getPartitionRevision();
		}
	};

	void encode(EventByteOutStream& out);
	void decode(
		EventByteInStream& in, const char8_t* bodyBuffer);

	PartitionRevision& getRevision() {
		return ptRev_;
	}

	PartitionRevisionNo getRevisionNo() {
		return ptRev_.getRevisionNo();
	}

	SyncMode getMode() {
		return syncMode_;
	}
	std::string dump();

private:
	SyncMode syncMode_;
	SyncContext* context_;
	PartitionRevision ptRev_;
};

/*!
	@brief Represents the information for drop partition
*/
class DropPartitionInfo : public SyncManagerInfo {
public:
	DropPartitionInfo(
		util::StackAllocator& alloc, EventType eventType,
		SyncManager* syncMgr, PartitionId pId,
		bool forceFlag, PartitionRevisionNo revision = 0) :
		SyncManagerInfo(
			alloc, eventType, syncMgr, pId),
		forceFlag_(forceFlag),
		revision_(revision) {
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
	MSGPACK_DEFINE(forceFlag_, revision_);

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

	void initialize(const ManagerSet& mgrSet);

	void operator()(EventContext&, EventEngine::Event&) {};

	void removePartition(PartitionId pId);


	PartitionTable* pt_;
	ClusterManager* clsMgr_;
	ClusterService* clsSvc_;

	TransactionService* txnSvc_;
	TransactionManager* txnMgr_;
	EventEngine* txnEE_;

	SyncService* syncSvc_;
	SyncManager* syncMgr_;
	EventEngine* syncEE_;

	RecoveryManager* recoveryMgr_;
	PartitionList* partitionList_;
	CheckpointService* cpSvc_;

protected:

	void sendResponse(
		EventContext& ec, EventType eventType,
		PartitionId pId, SyncRequestInfo& requestInfo,
		SyncResponseInfo& responseInfo,
		SyncContext* context, NodeId senderNodeId);

	void sendEvent(
		EventContext& ec, EventEngine& ee, NodeId targetNodeId,
		Event& ev, int32_t delayTime = UNDEF_DELAY_TIME);

	void sendRequest(
		EventContext& ec, EventEngine& ee, EventType eventType,
		PartitionId pId, SyncRequestInfo& requestInfo,
		NodeId senderNodeId, int32_t waitTime = 0);

	template <typename T>
	void sendSyncRequest(
		EventContext& ec, EventEngine& ee, EventType eventType,
		PartitionId pId, T& requestInfo,
		NodeId senderNodeId, int32_t waitTime);

	template <typename T>
	void sendMultiRequest(
		EventContext& ec, EventEngine& ee, EventType eventType,
		PartitionId pId, SyncRequestInfo& requestInfo,
		T& t, SyncContext* context);

	void addCheckEndEvent(
		EventContext& ec, PartitionId pId,
		util::StackAllocator& alloc,
		SyncMode mode, SyncContext* context,
		bool isImmediate);

	void addLongtermSyncCheckEvent(
		EventContext& ec, PartitionId pId,
		util::StackAllocator& alloc, SyncContext* context,
		int32_t longtermSyncType);
};

/*!
	@brief Handles short-term Sync
*/
class ShortTermSyncHandler : public SyncHandler {
public:
	ShortTermSyncHandler() {};

	void operator()(
		EventContext& ec, EventEngine::Event& ev);

private:
	void decodeLongSyncRequestInfo(
		PartitionId pId, EventByteInStream& in,
		SyncId& longSyncId, SyncContext*& longSyncContext);

	int64_t getTargetSSN(PartitionId pId, SyncContext* context);

	void undoPartition(
		util::StackAllocator& alloc,
		SyncContext* context, PartitionId pId);

	void executeSyncRequest(EventContext& ec, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo);

	void executeSyncStart(EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncStartAck(EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncLog(EventContext& ec, Event& ev, PartitionId pId, SyncContext* context,
		SyncRequestInfo& syncRequestInfo, SyncResponseInfo& syncResponseInfo);

	void executeSyncLogAck(EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncEnd(EventContext& ec, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncEndAck(EventContext& ec, PartitionId pId,
		SyncContext* context,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void checkOwner(LogSequentialNumber ownerLsn,
		SyncContext* context, bool isOwner = true);

	void checkBackup(NodeId backupNodeId,
		LogSequentialNumber backupLsn, SyncContext* context);
};



/*!
	@brief Handles long-term Sync
*/
class LongTermSyncHandler : public SyncHandler {
public:
	LongTermSyncHandler() {};

	void operator() (EventContext& ec, EventEngine::Event& ev);

	void requestShortTermSync(
		EventContext& ec, PartitionId pId, SyncContext* context);

	void executeSyncRequest(
		EventContext& ec, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo);

	void executeSyncStart(
		EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncStartAck(
		EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncChunk(
		EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncChunkAck(
		EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);

	void executeSyncLog(EventContext& ec, Event& ev, PartitionId pId, SyncContext* context,
		SyncRequestInfo& syncRequestInfo, SyncResponseInfo& syncResponseInfo);

	void executeSyncLogAck(
		EventContext& ec, Event& ev, PartitionId pId,
		SyncContext* context, SyncRequestInfo& syncRequestInfo,
		SyncResponseInfo& syncResponseInfo, NodeId senderNodeId);
};


/*!
	@brief Handles Sync Timeout
*/
class SyncCheckEndHandler : public SyncHandler {
public:
	SyncCheckEndHandler() {};
	void operator() (
		EventContext& ec, EventEngine::Event& ev);
};

/*!
	@brief Handles DropPartition
*/
class DropPartitionHandler : public SyncHandler {
public:
	DropPartitionHandler() {};
	void operator() (
		EventContext& ec, EventEngine::Event& ev);
};

/*!
	@brief Handles receiving Sync message
*/
class RecvSyncMessageHandler : public SyncHandler {
public:
	RecvSyncMessageHandler() {};
	void operator()(
		EventContext& ec, EventEngine::Event& ev);
};

/*!
	@brief Handles unknown Sync event
*/
class UnknownSyncEventHandler : public SyncHandler {
public:
	UnknownSyncEventHandler() {};
	void operator() (
		EventContext& ec, EventEngine::Event& ev);
};

/*!
	@brief RedoManager
*/
class RedoManager {
	struct RedoInfo;
public:
	typedef int64_t RequestId;
	static const RequestId UNDEF_SYNC_REQUEST_ID = -1;

	struct RedoStatus {
		class Coder;
		enum Status {
			WAIT,
			RUNNING,
			FINISH,
			WARNING,
			FAIL
		};
	};

	class RedoStatus::Coder {
	public:
		const char8_t* operator()(Status status, const char8_t* name = NULL) const;
		static const char8_t* getStatusName(Status status);
	};

	struct RedoContext {

		RedoContext(GlobalVariableSizeAllocator& varAlloc, PartitionId pId, std::string& uuid, NodeId senderNodeId) :
			varAlloc_(varAlloc), readOffset_(0), currentLsn_(UNDEF_LSN), orgStartLsn_(UNDEF_LSN), orgEndLsn_(UNDEF_LSN),
			startLsn_(UNDEF_LSN), endLsn_(UNDEF_LSN), prevReadLsn_(UNDEF_LSN),
			status_(RedoStatus::Status::WAIT), errorCode_(0), partitionId_(pId), uuid_(uuid), requestId_(UNDEF_SYNC_REQUEST_ID),
			start_(0), end_(0), checkAckCount_(0), ackCount_(0), sequenceNo_(0), isContinue_(false), isOwner_(false),
			senderNodeId_(senderNodeId), logMgrStats_(NULL), readfileSize_(-1), skipReadLog_(true), executionStatusValue_(ExecutionStatus::INIT),
			backupErrorList_(varAlloc_), backupErrorMap_(varAlloc_), counter_(0), retryCount_(0) {
			start_ = util::DateTime::now(false).getUnixTime();
			startTime_ = CommonUtility::getTimeStr(start_);
		}

		RedoContext(util::VariableSizeAllocator<util::Mutex>& varAlloc, std::string& logFilePath, std::string& logFileName, LogSequentialNumber startLsn, LogSequentialNumber endLsn,
			PartitionId pId, RequestId requestId, std::string& uuid, bool forceApply);

		~RedoContext();

		void begin();

		void end();

		void setException(std::exception& exception, const char* str = NULL);

		void appendBackupError(NodeId nodeId, std::string& node, int32_t errorCode, std::string& errorName, LogSequentialNumber lsn);
		GlobalVariableSizeAllocator& varAlloc_;
		std::string logFileDir_;
		std::string logFileName_;
		std::string logFilePathName_;
		int64_t readOffset_;
		LogSequentialNumber currentLsn_;
		LogSequentialNumber orgStartLsn_;
		LogSequentialNumber orgEndLsn_;
		LogSequentialNumber startLsn_;
		LogSequentialNumber endLsn_;
		LogSequentialNumber prevReadLsn_;
		RedoStatus::Status status_;
		int32_t errorCode_;
		std::string errorName_;
		PartitionId partitionId_;
		std::string& uuid_;
		RequestId requestId_;
		int64_t start_;
		int64_t end_;
		std::string startTime_;
		std::string endTime_;
		PartitionRole role_;
		int32_t checkAckCount_;
		int32_t ackCount_;
		int64_t sequenceNo_;
		util::Stopwatch watch_;
		bool isContinue_;
		bool isOwner_;
		NodeId senderNodeId_;
		LogManagerStats logMgrStats_;
		int64_t readfileSize_;
		bool skipReadLog_;
		enum ExecutionStatus {
			INIT,
			PREPARE_EXECUTION,
			CHECK_STANDBY,
			CHECK_CLUSTER,
			READ_LOG,
			REDO,
			REPLICATION,
			POST_EXECUTION,
			FINISH
		} executionStatusValue_;
		std::string executionStatus_;

		const char* getExecutionStatus() {
			switch (executionStatusValue_) {
				case INIT: return "INIT";
				case PREPARE_EXECUTION: return "PREPARE_EXECUTION";
				case CHECK_STANDBY: return "CHECK_STANDBY";
				case CHECK_CLUSTER: return "CHECK_CLUSTER";
				case READ_LOG: return "READ_LOG";
				case REDO: return "REDO";
				case REPLICATION: return "REPLICATION";
				case POST_EXECUTION: return "POST_EXECUTION";
				case FINISH: return "FINISH";
				default: return "UNDEF";
			}
		}

		struct ErrorInfo {
			ErrorInfo(GlobalVariableSizeAllocator& varAlloc, std::string &node, int32_t errorCode, std::string& errorName, LogSequentialNumber  lsn) :
				varAlloc_(varAlloc), node_(node), lastErrorCode_(errorCode), lastErrorName_(errorName), lsn_(lsn), errorCount_(0){}
			void update(int32_t errorCode, std::string& errorName, LogSequentialNumber  lsn) {
				lastErrorCode_ = errorCode;
				lastErrorName_ = errorName;
				lsn_ = lsn;
				errorCount_++;
			}
			GlobalVariableSizeAllocator& varAlloc_;
			std::string node_;
			int32_t lastErrorCode_;
			std::string lastErrorName_;
			LogSequentialNumber  lsn_;
			int32_t errorCount_;
			UTIL_OBJECT_CODER_MEMBERS(node_, lastErrorCode_, lastErrorName_, lsn_, errorCount_);
		};
		util::AllocVector<ErrorInfo*> backupErrorList_;
		util::AllocMap<NodeId, ErrorInfo*> backupErrorMap_;
		int64_t counter_;
		int32_t retryCount_;
		bool checkLsn();

		void dump(util::NormalOStringStream& oss) {
			oss << "Context (";
			oss << "role=" << (isOwner_ ? "owner" : "backup");
			oss << ", pId=" << partitionId_ << ", uuid=" << uuid_ << ", requestId=" << requestId_ << ", status=" << RedoStatus::Coder::getStatusName(status_) << ", phase=" << getExecutionStatus();
			if (isOwner_) {
				oss << ", count=" << counter_;
			}
			if (errorCode_ != 0) {
				oss << ", errorName=" << errorName_;
			}
			if (!backupErrorList_.empty()) {
				oss << ", backupError=";
				std::vector<std::string> nodeList;
				for (size_t pos = 0; pos < backupErrorList_.size(); pos++) {
					nodeList.push_back(backupErrorList_[pos]->node_);
				}
				CommonUtility::dumpValueList(oss, nodeList);
			}
		}

		UTIL_OBJECT_CODER_MEMBERS(
			logFileDir_, logFileName_,
			UTIL_OBJECT_CODER_OPTIONAL(startLsn_, UNDEF_LSN),
			UTIL_OBJECT_CODER_OPTIONAL(endLsn_, UNDEF_LSN),
			partitionId_, uuid_, requestId_,
			UTIL_OBJECT_CODER_OPTIONAL(errorCode_, 0),
			UTIL_OBJECT_CODER_OPTIONAL(errorName_, ""),
			UTIL_OBJECT_CODER_ENUM(status_, RedoStatus::Coder()),
			UTIL_OBJECT_CODER_OPTIONAL(startTime_, ""),
			UTIL_OBJECT_CODER_OPTIONAL(endTime_, ""),
			backupErrorList_,counter_,
			UTIL_OBJECT_CODER_OPTIONAL(executionStatus_, "")
		);
	};

	struct RedoContextList {
		void append(RedoContext* context) {
			requestList_.push_back(context);
		}
		std::vector<RedoContext*> requestList_;
		UTIL_OBJECT_CODER_MEMBERS(requestList_);
	};

	RedoManager(const ConfigTable& config, GlobalVariableSizeAllocator& varAlloc, SyncService* syncSvc);
	~RedoManager();
	void initialize(ManagerSet& mgrSet);
	GlobalVariableSizeAllocator& getAllocator() {
		return varAlloc_;
	}
	std::string& getUuid() {
		return uuidString_;
	}

	bool getStatus(PartitionId pId, std::string& uuid, RequestId requestId, RestContext& restCxt);
	bool put(const Event::Source& eventSource, util::StackAllocator& alloc, PartitionId pId, std::string& redoFilePath, std::string& redoFileName,
		LogSequentialNumber startLsn, LogSequentialNumber endLsn, std::string& uuidString, RequestId& requestId, bool forceApply, RestContext& restCxt);
	bool cancel(const Event::Source& eventSource, util::StackAllocator& alloc,
		PartitionId pId, std::string& uuid, RestContext& restCxt, RequestId requestId = UNDEF_SYNC_REQUEST_ID);

	RedoContext* get(PartitionId pId, RequestId RequestId);
	bool remove(PartitionId pId, RequestId RequestId = UNDEF_SYNC_REQUEST_ID);
	int64_t getTotalContextNum();
	int64_t getContextNum(PartitionId pId);

private:

	struct RedoInfo {
	public:
		RedoInfo(const RedoInfo& another) : counter_(another.counter_) {}
		RedoInfo() : counter_(0) {}

	private:
		RedoInfo& operator=(const RedoInfo& another);

	public:
		int64_t counter_;
		util::Mutex lock_;
	};

	RequestId reserveId(PartitionId pId);
	bool checkRange(PartitionId pId, RestContext& cxt);
	bool checkStandby(RestContext& cxt);
	bool checkExecutable(PartitionId pId, RestContext& cxt);

	bool checkUuid(std::string& uuidString, RestContext& cxt, bool check);
	void sendRequest(const Event::Source& eventSource, util::StackAllocator& alloc, PartitionId pId, RequestId requestId, EventType eventType);

	util::Mutex lock_;
	typedef  util::AllocMap<RequestId, RedoContext*> ContextMap;
	uint32_t partitionNum_;
	GlobalVariableSizeAllocator& varAlloc_;
	UUIDValue uuid_;
	std::string uuidString_;
	std::vector<ContextMap> contextMapList_;
	std::vector<RedoInfo> contextInfoList_;
	SyncService* syncSvc_;
	TransactionService* txnSvc_;
	ClusterManager* clsMgr_;
	PartitionTable* pt_;
};

typedef RedoManager::RequestId RequestId;
typedef RedoManager::RedoContext RedoContext;

/*!
	@brief SyncService
*/
class SyncService {
public:
	SyncService(
		const ConfigTable& config,
		const EventEngine::Config& eeConfig,
		EventEngine::Source source,
		const char8_t* name,
		SyncManager& syncMgr,
		ClusterVersionId versionId,
		ServiceThreadErrorHandler& serviceThreadErrorHandler);

	~SyncService();

	static EventEngine::Config createEEConfig(
		const ConfigTable& config, const EventEngine::Config& src);

	void initialize(ManagerSet& mgrSet);
	void start();
	void shutdown();
	void waitForShutdown();

	EventEngine* getEE() {
		return &ee_;
	}

	template <class T>
	void decode(
		util::StackAllocator& alloc,
		EventEngine::Event& ev,
		T& t, EventByteInStream& in);

	template <class T>
	void encode(
		EventEngine::Event& ev,
		T& t,
		EventByteOutStream& out);

	void requestDrop(const Event::Source& eventSource,
		util::StackAllocator& alloc,
		PartitionId pId, bool isForce,
		PartitionRevisionNo revision = 0);

	void notifyCheckpointLongSyncReady(EventContext& ec, PartitionId pId, LongtermSyncInfo* syncInfo);

	void notifySyncCheckpointEnd(EventContext& ec, PartitionId pId, LongtermSyncInfo* syncInfo);

	void notifySyncCheckpointError(EventContext& ec, PartitionId pId, LongtermSyncInfo* syncInfo);
	
	SyncManager* getManager() {
		return syncMgr_;
	}

	RedoManager& getRedoManager() {
		return redoMgr_;
	}
private:
	void checkVersion(ClusterVersionId decodedVersion);
	uint16_t getLogVersion(PartitionId pId);
	void setClusterHandler();

	util::VariableSizeAllocator<util::Mutex> varAlloc_;
	EventEngine ee_;
	SyncManager* syncMgr_;
	RedoManager redoMgr_; 
	TransactionService* txnSvc_;
	ClusterService* clsSvc_;
	PartitionList* partitionList_;

	const uint8_t versionId_;
	RecvSyncMessageHandler recvSyncMessageHandler_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;
	UnknownSyncEventHandler unknownSyncEventHandler_;
	bool initialized_;
};

/*!
	@brief SyncRedoRequestInfo
*/
struct SyncRedoRequestInfo {

	SyncRedoRequestInfo(util::StackAllocator& alloc);
	void encode(EventByteOutStream& out);
	void decode(EventByteInStream& in, const char8_t*data);
	void initLog();
	std::string dump();

	int64_t requestId_;
	int64_t sequenceNo_;
	LogSequentialNumber startLsn_;
	LogSequentialNumber endLsn_;
	int32_t errorCode_;
	std::string errorName_;
	int64_t binaryLogRecordsSize_;
	util::XArray<uint8_t> binaryLogRecords_;
};

/*!
	@brief RedoLogHandler
*/
class RedoLogHandler : public SyncHandler {
public:
	static const size_t LOG_MEM_POOL_SIZE_BITS = 12; 
	struct MemoryBlock {
		MemoryBlock() : memoryPool_(util::AllocatorInfo(ALLOCATOR_GROUP_SYNC, "syncFixed"), 1 << LOG_MEM_POOL_SIZE_BITS),
			stackAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_SYNC, "syncFixed"), &memoryPool_) {
		}
		util::FixedSizeAllocator<util::Mutex> memoryPool_;
		util::StackAllocator stackAlloc_;
	};

	RedoLogHandler() {}

	void initialize(const ManagerSet& mgrSet) {
		SyncHandler::initialize(mgrSet);
		for (int32_t pId = 0; pId < pt_->getPartitionNum(); pId++) {
			memoryBlockList_.push_back(UTIL_NEW MemoryBlock);
		}
	}

	~RedoLogHandler() {
		for (int32_t pId = 0; pId < pt_->getPartitionNum(); pId++) {
			delete memoryBlockList_[pId];
		}
	}

	void operator()(EventContext& ec, EventEngine::Event& ev);
	util::StackAllocator& getLogStackAllocator(PartitionId pId) {
		return memoryBlockList_[pId]->stackAlloc_;
	}

private:
	std::vector<MemoryBlock*> memoryBlockList_;

	bool isSemiSyncReplication();
	bool checkStandby(RedoContext& cxt);
	bool prepareExecution(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool checkAck(RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool checkExecutable(RedoContext& cxt);
	bool readLogFile(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool redoLog(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool executeReplication(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool postExecution(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool executeReplicationAck(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
	bool checkReplicationTimeout(RedoContext& cxt);
	bool checkDisplayErrorStatus(EventContext& ec, RedoContext& cxt);
	void complete(RedoContext& cxt);
	void checkBackupError(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo);
};


#endif
