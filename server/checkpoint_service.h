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
	@brief Definition of CheckpointService
*/
#ifndef CHECKPOINT_SERVICE_H_
#define CHECKPOINT_SERVICE_H_

#include "util/container.h"
#include "bit_array.h"
#include "cluster_event_type.h"  
#include "data_type.h"
#include "event_engine.h"
#include "gs_error.h"
#include "recovery_manager.h"  
#include "system_service.h"	



class CheckpointServiceMainHandler;  
class CheckpointServiceGroupHandler;  
class CheckpointOperationHandler;   
class LogFlushPeriodicallyHandler;  
class ConfigTable;

/*!
	@brief Locks a Partition group.
*/
class PartitionGroupLock {
public:
	PartitionGroupLock(TransactionManager &txnManager, PartitionGroupId pgId);
	~PartitionGroupLock();

private:
	TransactionManager &transactionManager_;
	PartitionGroupId pgId_;
};

/*!
	@brief Locks a Partition.
*/
class PartitionLock {
public:
	PartitionLock(TransactionManager &txnManager, PartitionId pId);
	~PartitionLock();

private:
	TransactionManager &transactionManager_;
	PartitionId pId_;
};

static const uint32_t CP_HANDLER_PARTITION_ID = 0;
static const uint32_t CP_SERIALIZED_PARTITION_ID = 0;

static const int32_t secondLimit = INT32_MAX / 1000;

static const int32_t CP_SYNC_DELAY_INTERVAL = 30 * 1000;

/*!
	@brief Converts time unit from second to millisecond.
*/
static inline const int32_t changeTimeSecondToMilliSecond(int32_t second) {
	if (second > secondLimit) {
		return INT32_MAX;
	}
	else {
		return second * 1000;
	}
}

class LogManager;
class RecoveryManager;
class TransactionManager;
class TransactionService;
class CheckpointManager;
struct ManagerSet;

/*!
	@brief Operates Checkpoint events.
*/
class CheckpointHandler : public EventHandler {
public:
	CheckpointHandler(){};
	~CheckpointHandler(){};

	void initialize(const ManagerSet &mgrSet);

	void operator()(EventContext &ec, Event &ev){};

	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	CheckpointService *checkpointService_;
	ClusterService *clusterService_;

	LogManager *logManager_;
	DataStore *dataStore_;
	ChunkManager *chunkManager_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	ConfigTable *config_;

private:
};

/*!
	@brief Operates main Checkpoint events.
*/
class CheckpointServiceMainHandler : public CheckpointHandler {
public:
	CheckpointServiceMainHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Operates Checkpoint events for each group of CheckpointService.
*/
class CheckpointServiceGroupHandler : public CheckpointHandler {
public:
	CheckpointServiceGroupHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Operates Checkpoint events for Transaction Service.
*/
class CheckpointOperationHandler : public CheckpointHandler {
public:
	static const int32_t CHUNK_META_DATA_LOG_MAX_NUM = 10000;

	CheckpointOperationHandler(){};
	void operator()(EventContext &ec, Event &ev);
	void writeCheckpointStartLog(util::StackAllocator &alloc, int32_t mode,
		PartitionGroupId pgId, PartitionId pId, CheckpointId cpId);

	void writeChunkMetaDataLog(util::StackAllocator &alloc, int32_t mode,
		PartitionGroupId pgId, PartitionId pId, CheckpointId cpId,
		bool isRestored);

};

/*!
	@brief Operates intermittent log flush.
*/
class FlushLogPeriodicallyHandler : public CheckpointHandler {
public:
	FlushLogPeriodicallyHandler(){};
	void operator()(EventContext &ec);
};

/*!
	@brief Operates Checkpoint events.
*/
class CheckpointService {
public:
	friend class CheckpointOperationHandler;
	friend class CheckpointServiceMainHandler;
	friend class CheckpointServiceGroupHandler;

	static const uint32_t CP_CHUNK_COPY_NUM = 10;  
	static const uint32_t ALL_GROUP_CHECKPOINT_END_CHECK_INTERVAL =
		1000;  
	static const std::string PID_LSN_INFO_FILE_NAME;
	static const uint32_t PID_LSN_INFO_FILE_VERSION = 0x1;  


	/*!
		@brief Types of events
	*/
	enum EventType {
		CP_REQUEST_CHECKPOINT,
		CP_TIMER_LOG_FLUSH,
		CP_REQUEST_GROUP_CHECKPOINT  
	};

	/*!
		@brief Status of checkpoint for each partitionGroup
	*/
	enum GroupCheckpointStatus { GROUP_CP_COMPLETED = 0, GROUP_CP_RUNNING };


	CheckpointService(const ConfigTable &config, EventEngine::Config eeConfig,
		EventEngine::Source source, const char8_t *name,
		ServiceThreadErrorHandler &serviceThreadErrorHandler);

	~CheckpointService();

	void initialize(ManagerSet &mgrSet);


	void start(const Event::Source &eventSource);

	void shutdown();

	void waitForShutdown();

	EventEngine *getEE();

	void executeRecoveryCheckpoint(const Event::Source &eventSource);

	void requestNormalCheckpoint(const Event::Source &eventSource);


	void requestShutdownCheckpoint(const Event::Source &eventSource);

	void executeOnTransactionService(EventContext &ec, GSEventType eventType,
		int32_t mode, PartitionId pId, CheckpointId cpId, bool executeAndWait);
	CheckpointId getLastCompletedCheckpointId(PartitionGroupId pgId) const;

	CheckpointId getCurrentCheckpointId(PartitionGroupId pgId) const;

	const PartitionGroupConfig &getPGConfig() const {
		return pgConfig_;
	}

	static const char8_t *checkpointModeToString(int32_t mode);

	void changeParam(std::string &paramName, std::string &value);

	int32_t getLastMode() const {
		return lastMode_;
	}

	int64_t getLastDuration(const util::DateTime &now) const {
		const int64_t startTime = startTime_;
		const int64_t endTime = endTime_;
		if (startTime == 0) {
			return 0;
		}
		else if (endTime > 0 && startTime <= endTime) {
			return endTime - startTime;
		}
		else {
			return (now.getUnixTime() - startTime);
		}
	}

	int64_t getLastStartTime() const {
		return startTime_;
	}

	int64_t getLastEndTime() const {
		return endTime_;
	}

	int32_t getPendingPartitionCount() const {
		return pendingPartitionCount_;
	}

	int64_t getTotalNormalCpOperation() const {
		return totalNormalCpOperation_;
	}

	int64_t getTotalRequestedCpOperation() const {
		return totalRequestedCpOperation_;
	}


	void setCurrentCpGrpId(PartitionGroupId pgId) {
		currentCpGrpId_ = pgId;
	}

	PartitionGroupId getCurrentCpGroupId() {
		return currentCpGrpId_;
	}

	void setCurrentCpPId(PartitionId pId) {
		currentCpPId_ = pId;
	}

	PartitionGroupId getCurrentCpPId() {
		return currentCpPId_;
	}


	PartitionTable *getPartitionTable() {
		return partitionTable_;
	}

	void setCurrentCheckpointId(PartitionId pId, CheckpointId cpId) {
		currentCheckpointIdList_[pId] = cpId;
	}

	CheckpointId getCurrentCheckpointId(PartitionId pId) {
		return currentCheckpointIdList_[pId];
	}

private:
	/*!
		@brief Manages LSNs for each Partition.
	*/
	class PIdLsnInfo {
	public:
		PIdLsnInfo();
		~PIdLsnInfo();

		void setConfigValue(CheckpointService *cpService, uint32_t partitionNum,
			uint32_t partitionGroupNum, const std::string &path);

		void startCheckpoint();  

		void setLsn(PartitionId pId, LogSequentialNumber lsn);

		void endCheckpoint();  

	private:
		void writeFile();

		CheckpointService *checkpointService_;
		util::Mutex mutex_;
		uint32_t partitionNum_;
		uint32_t partitionGroupNum_;

		std::string path_;
		std::vector<LogSequentialNumber> lsnList_;
	};

	static EventEngine::Config &createEEConfig(
		const ConfigTable &config, EventEngine::Config &eeConfig);

	void runCheckpoint(EventContext &ec, int32_t mode, uint32_t flag);

	void runGroupCheckpoint(
		PartitionGroupId pgId, EventContext &ec, int32_t mode, uint32_t flag);

	void waitAllGroupCheckpointEnd();



	inline int32_t getCheckpointInterval() const {
		return cpInterval_;
	}

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		CheckpointService *service_;
	} statUpdator_;

	CheckpointServiceMainHandler checkpointServiceMainHandler_;
	CheckpointServiceGroupHandler checkpointServiceGroupHandler_;
	FlushLogPeriodicallyHandler flushLogPeriodicallyHandler_;

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	EventEngine ee_;

	ClusterService *clusterService_;
	EventEngine *clsEE_;
	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	EventEngine *txnEE_;
	SystemService *systemService_;
	LogManager *logManager_;
	DataStore *dataStore_;
	ChunkManager *chunkManager_;
	PartitionTable *partitionTable_;
	bool initailized_;

	GlobalFixedSizeAllocator *fixedSizeAlloc_;

	util::Atomic<bool> requestedShutdownCheckpoint_;

	const PartitionGroupConfig pgConfig_;

	const int32_t cpInterval_;	
	const int32_t logWriteMode_;  
	util::Atomic<int32_t>
		chunkCopyIntervalMillis_;  

	int32_t delaySleepTime_;


	std::string lastBackupPath_;
	bool currentDuplicateLogMode_;

	PartitionGroupId currentCpGrpId_;
	PartitionId currentCpPId_;


	std::vector<uint8_t> groupCheckpointStatus_;
	bool parallelCheckpoint_;
	util::Atomic<bool> errorOccured_;

	PIdLsnInfo lsnInfo_;
	bool enableLsnInfoFile_;

	std::vector<CheckpointId> lastArchivedCpIdList_;


	util::Atomic<int32_t> lastMode_;
	util::Atomic<int64_t> startTime_;
	util::Atomic<int64_t> endTime_;
	util::Atomic<int32_t> pendingPartitionCount_;
	util::Atomic<int64_t> totalNormalCpOperation_;
	util::Atomic<int64_t> totalRequestedCpOperation_;

	std::vector<CheckpointId> currentCheckpointIdList_;


};


#endif  
