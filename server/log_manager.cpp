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
    @brief Implementation of LogManager
*/

#include "log_manager.h"
#include "gs_error.h"
#include "util/trace.h"
#include "util/time.h"
#include "chunk_manager.h"
#include "partition_table.h"

UTIL_TRACER_DECLARE(LOG_MANAGER);
UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(CLUSTER_SERVICE);


const char *const LogManager::LOG_FILE_BASE_NAME = "gs_log_";
const char *const LogManager::LOG_FILE_SEPARATOR = "_";
const char *const LogManager::LOG_FILE_EXTENSION = ".log";

const char *const LogManager::LOG_INDEX_FILE_BASE_NAME = "gs_log_index_";
const char *const LogManager::LOG_INDEX_FILE_SEPARATOR = "_";
const char *const LogManager::LOG_INDEX_FILE_EXTENSION = ".idx";

const char *const LogManager::LOG_DUMP_FILE_BASE_NAME = "gs_dump_log_";
const char *const LogManager::LOG_DUMP_FILE_EXTENSION = ".txt";

/*!
    @brief Constructor of LogManager
*/
LogManager::LogManager(const Config &config) :
	config_(config) 
{
	if (config_.getBlockSize() <=
			LogBlocksInfo::LOGBLOCK_HEADER_SIZE +
			LogRecord::LOG_HEADER_SIZE) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Too small block size (blockSize=" << config_.getBlockSize() << ")");
	}

	if (config_.pgConfig_.getPartitionCount() <= 0 ||
			config_.pgConfig_.getPartitionGroupCount() <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Non positive partition or partition group count");
	}

	partitionInfoList_.resize(config.pgConfig_.getPartitionCount());
	pgManagerList_.resize(config.pgConfig_.getPartitionGroupCount());

	try {
		for (size_t i = 0; i < pgManagerList_.size(); i++) {
			pgManagerList_[i] = UTIL_NEW PartitionGroupManager();
		}
	}
	catch (...) {
		for (size_t i = 0; i < pgManagerList_.size(); i++) {
			delete pgManagerList_[i];
		}
	}

	config_.setUpConfigHandler();
}

LogManager::~LogManager() {
	for (size_t i = 0; i < pgManagerList_.size(); i++) {
		delete pgManagerList_[i];
	}
}

namespace {
/*!
    @brief Checks digit.
*/
struct NoDigitChecker {
	inline bool operator()(char8_t ch) { return !('0' <= ch && ch <= '9'); }
};
}

/*!
    @brief Opens Log file of all Partition groups.
*/
void LogManager::open(bool checkOnly, bool forceRecoveryFromExistingFiles) {
	typedef std::vector<CheckpointId> CheckpointIdList;
	typedef std::map<PartitionGroupId, CheckpointIdList> CheckpointIdsMap;

	try {
		CheckpointIdsMap cpIdsMap;

		const char8_t *dirPath =
				config_.logDirectory_.empty() ? "." : config_.logDirectory_.c_str();

		if (!util::FileSystem::isDirectory(dirPath)) {
			if (checkOnly) {
				GS_THROW_USER_ERROR(
						GS_ERROR_LM_LOG_FILES_CONFIGURATION_UNMATCHED,
						"Directory not found despite check only (path=" << dirPath << ")");
			}
			util::FileSystem::createDirectoryTree(dirPath);
		}

		util::Directory dir(dirPath);

		for (u8string fileName; dir.nextEntry(fileName);) {
			if (fileName.find(LOG_FILE_BASE_NAME) != 0) {
				continue;
			}

			const u8string::iterator beginIt = fileName.begin();
			u8string::iterator lastIt = beginIt + strlen(LOG_FILE_BASE_NAME);

			u8string::iterator it = std::find_if(lastIt, fileName.end(), NoDigitChecker());
			if (it == fileName.end() || it  == lastIt) {
				continue;
			}
			const PartitionGroupId pgId =
					util::LexicalConverter<uint32_t>()(std::string(lastIt, it));
			lastIt = it;

			size_t lastOff = lastIt - beginIt;
			if (fileName.find(LOG_FILE_SEPARATOR, lastOff) != lastOff) {
				continue;
			}
			lastIt += strlen(LOG_FILE_SEPARATOR);

			it = std::find_if(lastIt, fileName.end(), NoDigitChecker());
			if (it == fileName.end() || it  == lastIt) {
				continue;
			}
			const CheckpointId cpId =
					util::LexicalConverter<uint64_t>()(std::string(lastIt, it));
			lastIt = it;

			lastOff = lastIt - beginIt;
			if (fileName.find(LOG_FILE_EXTENSION, lastOff) != lastOff ||
					lastIt + strlen(LOG_FILE_EXTENSION) != fileName.end()) {
				continue;
			}

			cpIdsMap[pgId].push_back(cpId);
		}

		if (!cpIdsMap.empty()) {
			if (forceRecoveryFromExistingFiles) {
				for (PartitionGroupId pgId = 0; pgId < config_.pgConfig_.getPartitionGroupCount(); ++pgId) {
					if (cpIdsMap.find(pgId) == cpIdsMap.end()) {
						GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_OPEN_SKIP_PARTITIONGROUP,
									  "forceRecoveryFromExistingFiles=true: skip pgId=" << pgId);
						const CheckpointId cpId = 0;

						LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);
						pgManagerList_[pgId]->open(
							pgId, config_, cpId, cpId,
							partitionInfoList_, config_.emptyFileAppendable_, checkOnly);
					} else {
						LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);
						
						CheckpointIdList &list = cpIdsMap[pgId];
						std::sort(list.begin(), list.end());
						
						if (list.size() > 1 && list.begin()[0] == 0 && list.begin()[1] > 1) {
							list.erase(list.begin());
						}
						
						CheckpointId first = list.front();
						pgManagerList_[pgId]->open(
							pgId, config_, first, list.back() + 1,
							partitionInfoList_, config_.emptyFileAppendable_, checkOnly);
					}
				}
			} else {
				const PartitionGroupId minPgId = cpIdsMap.begin()->first;
				const PartitionGroupId maxPgId = (--cpIdsMap.end())->first;
				if (minPgId != 0 || maxPgId + 1 !=
					config_.pgConfig_.getPartitionGroupCount() ||
					cpIdsMap.size() != config_.pgConfig_.getPartitionGroupCount()) {
					GS_THROW_USER_ERROR(
						GS_ERROR_LM_LOG_FILES_CONFIGURATION_UNMATCHED,
						"Existing log files do not match the server configuration"
						" (expectedPartitionGroupNum=" <<
						config_.pgConfig_.getPartitionGroupCount() <<
						", actualPartitionGroupNum=" << cpIdsMap.size() <<
						", actualMinPartitionGroupId=" << minPgId <<
						", actualMaxPartitionGroupId=" << maxPgId << ")");
				}
				for (CheckpointIdsMap::iterator it = cpIdsMap.begin();
					 it != cpIdsMap.end(); ++it) {
					const PartitionGroupId pgId = it->first;
					LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);
					
					CheckpointIdList &list = it->second;
					std::sort(list.begin(), list.end());
					
					if (list.size() > 1 && list.begin()[0] == 0 && list.begin()[1] > 1) {
						list.erase(list.begin());
					}

					CheckpointId first = list.front();
					pgManagerList_[pgId]->open(
						pgId, config_, first, list.back() + 1,
						partitionInfoList_, config_.emptyFileAppendable_, checkOnly);
				}
			}
		}
		else {
			if (checkOnly) {
				GS_THROW_USER_ERROR(
						GS_ERROR_LM_LOG_FILES_CONFIGURATION_UNMATCHED,
						"Log file not found despite check only");
			}

			for (PartitionGroupId pgId = 0;
					pgId < config_.pgConfig_.getPartitionGroupCount(); pgId++) {
				LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

				const CheckpointId cpId = 0;
				pgManagerList_[pgId]->open(
						pgId, config_, cpId, cpId,
						partitionInfoList_, config_.emptyFileAppendable_, checkOnly);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Open logfile failed. (reason="
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Open Log file of specified Partition group.
*/
void LogManager::openPartial(PartitionGroupId pgId, CheckpointId cpId, const char8_t *suffix) {
	try {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		const bool appendable = false;
		const bool checkOnly = false;
		pgManagerList_[pgId]->open(
				pgId, config_, cpId, cpId + 1, partitionInfoList_,
				appendable, checkOnly, suffix);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Open logfile failed. (reason="
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Create Log files of all Partition groups.
*/
void LogManager::create(PartitionGroupId pgId, CheckpointId cpId,
		const char8_t *suffix) {
	try {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		const bool appendable = true;
		const bool checkOnly = false;
		pgManagerList_[pgId]->open(
				pgId, config_, cpId, cpId, partitionInfoList_,
				appendable, checkOnly, suffix);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Create logfile failed. (reason="
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Closes Log files of all Partition groups.
*/
void LogManager::close() {
	for (PartitionGroupId pgId = 0;
			pgId < config_.pgConfig_.getPartitionGroupCount(); pgId++) {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		pgManagerList_[pgId]->close();
	}
}

/*!
    @brief Returns Checkpoint ID of the first checkpoint.
*/
CheckpointId LogManager::getFirstCheckpointId(PartitionGroupId pgId) const {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	return pgManagerList_[pgId]->getFirstCheckpointId();
}

/*!
    @brief Returns Checkpoint ID of the last checkpoint.
*/
CheckpointId LogManager::getLastCheckpointId(PartitionGroupId pgId) const {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	return pgManagerList_[pgId]->getLastCheckpointId();
}

/*!
    @brief Returns Checkpoint ID of the last completed checkpoint.
*/
CheckpointId LogManager::getLastCompletedCheckpointId(
		PartitionGroupId pgId) const {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	return pgManagerList_[pgId]->getCompletedCheckpointId();
}

/*!
    @brief Finds the Log of the specified Partition and LSN.
*/
bool LogManager::findLog(
		LogCursor &cursor, PartitionId pId, LogSequentialNumber lsn) {
	const PartitionGroupId pgId = calcPartitionGroupId(pId);
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	cursor.clear();

	if (pgManagerList_[pgId]->findLog(
			cursor.blocksLatch_, cursor.offset_, pId, lsn)) {
		cursor.pgId_ = pgId;
		return true;
	}

	cursor.clear();
	return false;
}

/*!
    @brief Finds the CheckpointEndLog of the specified Partition group and Checkpoint ID.
*/
bool LogManager::findCheckpointStartLog(
		LogCursor &cursor, PartitionGroupId pgId, CheckpointId cpId) {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	cursor.clear();

	if (pgManagerList_[pgId]->findCheckpointStartLog(
			cursor.blocksLatch_, cursor.offset_, cpId)) {
		cursor.pgId_ = pgId;
		return true;
	}

	cursor.clear();
	return false;
}

/*!
    @brief Finds the end of the CheckpointEndLog of the specified Partition group and Checkpoint ID.
*/
bool LogManager::findCheckpointEndLog(
		LogCursor &cursor, PartitionGroupId pgId, CheckpointId cpId) {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	cursor.clear();

	if (pgManagerList_[pgId]->findCheckpointEndLog(
			cursor.blocksLatch_, cursor.offset_, cpId)) {
		cursor.pgId_ = pgId;
		return true;
	}

	cursor.clear();
	return false;
}



/*!
    @brief Output a Log of a Replication.
*/
void LogManager::putReplicationLog(PartitionId pId, LogSequentialNumber lsn,
								   uint32_t len, uint8_t *logRecord)
{
	putLog(pId, lsn, logRecord, len, false);
}

/*!
    @brief Output a Log of the start of a Checkpoint.
*/
LogSequentialNumber LogManager::putCheckpointStartLog(util::XArray<uint8_t> &binaryLogBuf,
													  PartitionId pId,
													  TransactionId maxTxnId,
													  LogSequentialNumber cpLsn,
													  const util::XArray<ClientId> &clientIds,
													  const util::XArray<TransactionId> &activeTxnIds,
													  const util::XArray<ContainerId> &refContainerIds,
													  const util::XArray<StatementId> &lastExecStmtIds,
													  const util::XArray<int32_t> &txnTimeoutIntervalSec)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		const uint32_t partitionCount = config_.pgConfig_.getPartitionCount();
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_CHECKPOINT_START,
			pId, cpLsn, maxTxnId, partitionCount);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 2];
		uint8_t *addr = tmp;
		uint32_t txnNum = static_cast<uint32_t>(activeTxnIds.size());
		addr += util::varIntEncode32(addr, txnNum);
		uint32_t numContainerRefCount = 0;
		addr += util::varIntEncode32(addr, numContainerRefCount);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(clientIds.data()),
			clientIds.size() * sizeof(ClientId));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(activeTxnIds.data()),
			activeTxnIds.size() * sizeof(TransactionId));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(refContainerIds.data()),
			refContainerIds.size() * sizeof(ContainerId));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(lastExecStmtIds.data()),
			lastExecStmtIds.size() * sizeof(StatementId));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(txnTimeoutIntervalSec.data()),
			txnTimeoutIntervalSec.size() * sizeof(uint32_t));
		assert(clientIds.size() == activeTxnIds.size()
			   && activeTxnIds.size() == refContainerIds.size()
			   && refContainerIds.size() == lastExecStmtIds.size()
			   && lastExecStmtIds.size() == txnTimeoutIntervalSec.size()
			   && txnTimeoutIntervalSec.size() == static_cast<size_t>(txnNum));

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);


	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of committing a transaction.
*/
LogSequentialNumber LogManager::putCommitTransactionLog(util::XArray<uint8_t> &binaryLogBuf,
														PartitionId pId,
														const ClientId &clientId,
														TransactionId txnId,
														ContainerId containerId,
														StatementId stmtId)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_COMMIT,
            pId, clientId.sessionId_, txnId, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;
		addr += util::varIntEncode64(addr, stmtId);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of aborting a transaction.
*/
LogSequentialNumber LogManager::putAbortTransactionLog(util::XArray<uint8_t> &binaryLogBuf,
													   PartitionId pId,
													   const ClientId &clientId,
													   TransactionId txnId,
													   ContainerId containerId,
													   StatementId stmtId)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_ABORT,
            pId, clientId.sessionId_, txnId, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;
		addr += util::varIntEncode64(addr, stmtId);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of removing a partition.
*/
LogSequentialNumber LogManager::putDropPartitionLog(util::XArray<uint8_t> &binaryLogBuf,
													PartitionId pId)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_DROP_PARTITION,
			pId, 0, 0, 0);


		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);
	} catch(util::Exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of putting a Container.
*/
LogSequentialNumber LogManager::putPutContainerLog(util::XArray<uint8_t> &binaryLogBuf,
												   PartitionId pId,
												   ContainerId containerId,
												   uint32_t containerNameLen, const char *containerName,
												   const util::XArray<uint8_t> &containerInfo,
												   int32_t containerType)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_PUT_CONTAINER,
			pId, 0, 0, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 3];
		uint8_t *addr = tmp;
		addr += util::varIntEncode32(addr, containerType);
		addr += util::varIntEncode32(addr, containerNameLen + 1);
		uint32_t dataLen = static_cast<uint32_t>(containerInfo.size());
		addr += util::varIntEncode32(addr, dataLen);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(containerName), containerNameLen + 1);
		binaryLogBuf.push_back(containerInfo.data(), dataLen);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of removing a Container.
*/
LogSequentialNumber LogManager::putDropContainerLog(util::XArray<uint8_t> &binaryLogBuf,
													PartitionId pId,
													ContainerId containerId,
													uint32_t containerNameLen, const char *containerName)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_DROP_CONTAINER,
			pId, 0, 0, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;
		addr += util::varIntEncode32(addr, containerNameLen + 1);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(containerName), containerNameLen + 1);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of creating an Index.
*/
LogSequentialNumber LogManager::putCreateIndexLog(util::XArray<uint8_t> &binaryLogBuf,
												  PartitionId pId,
												  ContainerId containerId,
												  ColumnId columnId, int32_t mapType)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_CREATE_INDEX,
			pId, 0, 0, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 2];
		uint8_t *addr = tmp;
		addr += util::varIntEncode32(addr, columnId);
		addr += util::varIntEncode32(addr, mapType);
		binaryLogBuf.push_back(tmp, (addr - tmp));

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of removing an Index.
*/
LogSequentialNumber LogManager::putDropIndexLog(util::XArray<uint8_t> &binaryLogBuf,
												PartitionId pId,
												ContainerId containerId,
												ColumnId columnId, int32_t mapType)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_DROP_INDEX,
			pId, 0, 0, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 2];
		uint8_t *addr = tmp;
		addr += util::varIntEncode32(addr, columnId);
		addr += util::varIntEncode32(addr, mapType);
		binaryLogBuf.push_back(tmp, (addr - tmp));

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of creating a Trigger.
*/
LogSequentialNumber LogManager::putCreateTriggerLog(util::XArray<uint8_t> &binaryLogBuf,
													PartitionId pId,
													ContainerId containerId,
													uint32_t nameLen, const char *name,
													const util::XArray<uint8_t> &triggerInfo)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_CREATE_TRIGGER,
			pId, 0, 0, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 2];
		uint8_t *addr = tmp;
		addr += util::varIntEncode32(addr, nameLen + 1);
		uint32_t dataLen = static_cast<uint32_t>(triggerInfo.size());
		addr += util::varIntEncode32(addr, dataLen);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(name), nameLen + 1);
		binaryLogBuf.push_back(triggerInfo.data(), dataLen);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of removing a Trigger.
*/
LogSequentialNumber LogManager::putDropTriggerLog(util::XArray<uint8_t> &binaryLogBuf,
												  PartitionId pId,
												  ContainerId containerId,
												  uint32_t nameLen,
												  const char *name)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_DROP_TRIGGER,
			pId, 0, 0, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;
		addr += util::varIntEncode32(addr, nameLen + 1);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(reinterpret_cast<const uint8_t*>(name), nameLen + 1);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		flushByCommit(pId);

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of putting a Row.
*/
LogSequentialNumber LogManager::putPutRowLog(util::XArray<uint8_t> &binaryLogBuf,
											 PartitionId pId,
											 const ClientId &clientId,
											 TransactionId txnId,
											 ContainerId containerId,
											 StatementId stmtId,
											 uint64_t numRowId, const util::XArray<RowId> &rowIds,
											 uint64_t numRow, const util::XArray<uint8_t> &rowData,
											 int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
											 bool withBegin, bool isAutoCommit)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	return putPutOrUpdateRowLog(LOG_TYPE_PUT_ROW, binaryLogBuf, pId,
		clientId, txnId, containerId, stmtId,
		numRowId, rowIds, numRow, rowData,
		txnTimeoutInterval, txnContextCreateMode,
		withBegin, isAutoCommit);
}

/*!
    @brief Output a Log of updating a Row.
*/
LogSequentialNumber LogManager::putUpdateRowLog(util::XArray<uint8_t> &binaryLogBuf,
												PartitionId pId,
												const ClientId &clientId,
												TransactionId txnId,
												ContainerId containerId,
												StatementId stmtId,
												uint64_t numRowId, const util::XArray<RowId> &rowIds,
												uint64_t numRow, const util::XArray<uint8_t> &rowData,
												int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
												bool withBegin, bool isAutoCommit)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");

	return putPutOrUpdateRowLog(LOG_TYPE_UPDATE_ROW, binaryLogBuf, pId,
		clientId, txnId, containerId, stmtId,
		numRowId, rowIds, numRow, rowData,
		txnTimeoutInterval, txnContextCreateMode,
		withBegin, isAutoCommit);
}

/*!
    @brief Output a Log of removing a Row.
*/
LogSequentialNumber LogManager::putRemoveRowLog(util::XArray<uint8_t> &binaryLogBuf,
												PartitionId pId,
												const ClientId &clientId,
												TransactionId txnId,
												ContainerId containerId,
												StatementId stmtId,
												uint64_t numRowId, const util::XArray<RowId> &rowIds,
												int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
												bool withBegin, bool isAutoCommit)
{
	assert(!(withBegin && isAutoCommit));

	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		uint8_t beginFlag = withBegin ? LOG_TYPE_WITH_BEGIN : 0;
		uint8_t autoCommitFlag = isAutoCommit ? LOG_TYPE_IS_AUTO_COMMIT : 0;
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_REMOVE_ROW | beginFlag | autoCommitFlag,
			pId, clientId.sessionId_, txnId, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 4];
		uint8_t *addr = tmp;
		addr += util::varIntEncode64(addr, stmtId);
		addr += util::varIntEncode32(addr, txnContextCreateMode);
		addr += util::varIntEncode32(addr, txnTimeoutInterval);
		addr += util::varIntEncode64(addr, numRowId);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(rowIds.data()), numRowId * sizeof(RowId));

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		if (isAutoCommit) {
			flushByCommit(pId);
		}
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of locking a Row.
*/
LogSequentialNumber LogManager::putLockRowLog(util::XArray<uint8_t> &binaryLogBuf,
											  PartitionId pId,
											  const ClientId &clientId,
											  TransactionId txnId,
											  ContainerId containerId,
											  StatementId stmtId,
											  uint64_t numRowId, const util::XArray<RowId> &rowIds,
											  int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
											  bool withBegin, bool isAutoCommit)
{
	assert(!(withBegin && isAutoCommit));

	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		uint8_t beginFlag = withBegin ? LOG_TYPE_WITH_BEGIN : 0;
		uint8_t autoCommitFlag = isAutoCommit ? LOG_TYPE_IS_AUTO_COMMIT : 0;
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_LOCK_ROW | beginFlag | autoCommitFlag,
			pId, clientId.sessionId_, txnId, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 4];
		uint8_t *addr = tmp;
		addr += util::varIntEncode64(addr, stmtId);
		addr += util::varIntEncode32(addr, txnContextCreateMode);
		addr += util::varIntEncode32(addr, txnTimeoutInterval);
		addr += util::varIntEncode64(addr, numRowId);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(rowIds.data()), numRowId * sizeof(RowId));

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of the end of Checkpoint.
*/
LogSequentialNumber LogManager::putCheckpointEndLog(util::XArray<uint8_t> &binaryLogBuf,
													PartitionId pId,
													BitArray &validBlockInfo)
{
	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_START, "start");
	LogSequentialNumber lsn = 0;
	try {
		const uint32_t partitionCount = config_.pgConfig_.getPartitionCount();
		const uint32_t partitionGroupCount = config_.pgConfig_.getPartitionGroupCount();
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_CHECKPOINT_END,
			pId, partitionCount, partitionGroupCount, 1);

		GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
				"writeCPFileInfoLog: lsn=" << lsn << ", pId=" << pId);

		uint64_t validBlockInfoBitsSize = validBlockInfo.length();
		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;
		addr += util::varIntEncode64(addr, validBlockInfoBitsSize);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		validBlockInfo.copyAll(binaryLogBuf);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size(), true);

		putBlockTailNoopLog(binaryLogBuf, pId);

		GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
				"cpFileInfo: num=" << validBlockInfoBitsSize << ", data=");
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of metadata of Chunk data.
*/
LogSequentialNumber LogManager::putChunkMetaDataLog(util::XArray<uint8_t> &binaryLogBuf,
													PartitionId pId, ChunkCategoryId categoryId,
													ChunkId startChunkId, int32_t chunkNum,
													const util::XArray<uint8_t> *chunkMetaDataList,
													bool sentinel) {
	LogSequentialNumber lsn = 0;
	try {
		const ContainerId cId = sentinel ? UNDEF_CONTAINERID : 0;
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_CHUNK_META_DATA,
			pId, 0, 0, cId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 4];
		uint8_t *addr = tmp;

		tmp[0] = static_cast<uint8_t>(categoryId);  
		++addr;
		addr += util::varIntEncode32(addr, startChunkId);
		addr += util::varIntEncode32(addr, chunkNum);
		uint32_t dataLen = 0;
		if (chunkMetaDataList) {
			dataLen = static_cast<uint32_t>(chunkMetaDataList->size());
			addr += util::varIntEncode32(addr, dataLen);
			binaryLogBuf.push_back(tmp, (addr - tmp));
			binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(chunkMetaDataList->data()), dataLen);

			putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size(), true);
			GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
							"writeChunkMetaDataLog: lsn," << lsn << ",pId," << pId
							<< ",logSize," << binaryLogBuf.size());
		} else {
			assert(sentinel);
			addr += util::varIntEncode32(addr, dataLen);
			binaryLogBuf.push_back(tmp, (addr - tmp));
			
			putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size(), true);
			GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
							"writeChunkMetaDataLog(sentinel): lsn," << lsn << ",pId," << pId
							<< ",logSize,0");
		}

	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of the start of Chunk data.
*/
LogSequentialNumber LogManager::putChunkStartLog(util::XArray<uint8_t> &binaryLogBuf,
												 PartitionId pId, uint64_t putChunkId,
												 uint64_t chunkNum) {

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_PUT_CHUNK_START,
			pId, 0, 0, putChunkId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;

		addr += util::varIntEncode64(addr, chunkNum);
		binaryLogBuf.push_back(tmp, (addr - tmp));

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size(), true);
		GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
						"writePutChunkStartLog: lsn," << lsn << ",pId," << pId
						<< ",putChunkId," << putChunkId
						<< ",logSize," << binaryLogBuf.size());
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of a Chunk data.
*/
LogSequentialNumber LogManager::putChunkDataLog(util::XArray<uint8_t> &binaryLogBuf,
												PartitionId pId, uint64_t putChunkId,
												uint32_t chunkSize,
												const uint8_t* chunkImage) {
	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_PUT_CHUNK_DATA,
			pId, 0, 0, putChunkId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 1];
		uint8_t *addr = tmp;

		addr += util::varIntEncode32(addr, chunkSize);
		assert(chunkImage);

		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(chunkImage, chunkSize);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size(), true);
		GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
					  "writePutChunkDataLog: lsn," << lsn << ",pId," << pId
					  << ",putChunkId," << putChunkId
					  << ",logSize," << binaryLogBuf.size());
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Output a Log of the end of Chunk data.
*/
LogSequentialNumber LogManager::putChunkEndLog(util::XArray<uint8_t> &binaryLogBuf,
											   PartitionId pId, uint64_t putChunkId) {

	LogSequentialNumber lsn = 0;
	try {
		lsn = serializeLogCommonPart(
			binaryLogBuf, LOG_TYPE_PUT_CHUNK_END,
			pId, 0, 0, putChunkId);


		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size(), true);
		GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
					  "writePutChunkEndLog: lsn," << lsn << ",pId," << pId
					  << ",putChunkId," << putChunkId
					  << ",logSize," << binaryLogBuf.size());
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

/*!
    @brief Preparation of a checkpoint.
*/
void LogManager::prepareCheckpoint(PartitionGroupId pgId, CheckpointId cpId) {
	try {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		if (cpId != pgManagerList_[pgId]->getLastCheckpointId() + 1) {
			assert(false);
			GS_THROW_SYSTEM_ERROR(
					GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT,
					"Illegal checkpoint ID");
		}
		pgManagerList_[pgId]->prepareCheckpoint();
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Prepare checkpoint failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Post operations after a checkpoint finishes.
*/
void LogManager::postCheckpoint(PartitionGroupId pgId) {
	try {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		pgManagerList_[pgId]->cleanupLogFiles();
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Post checkpoint failed. reason=("
								  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Flushes Logs on the buffer to the Log file.
*/
void LogManager::writeBuffer(PartitionGroupId pgId) {
	try {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		pgManagerList_[pgId]->writeBuffer();
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Flush log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Flushes the Log file.
*/
void LogManager::flushFile(PartitionGroupId pgId) {
	try {
		pgManagerList_[pgId]->flushFile();
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Flush log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Returns of version of the Log.
*/
uint16_t LogManager::getVersion() {
	return LogBlocksInfo::LOGBLOCK_VERSION;
};

/*!
    @brief Copies Log files.
*/
uint64_t LogManager::copyLogFile(PartitionGroupId pgId, const char8_t *dirPath) {
	try {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

		return pgManagerList_[pgId]->copyLogFile(pgId, dirPath);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Copy logfile failed. reason=("
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Parses header data of Log and set to a LogRecord.
*/
uint32_t LogManager::parseLogHeader(const uint8_t *&addr, LogRecord &record) {
	const uint8_t *topAddr = addr;
    LogRecord::LogType logType;
	memcpy(&logType, addr, sizeof(uint8_t));
	record.withBegin_ = (logType & LOG_TYPE_WITH_BEGIN) > 0;
	record.isAutoCommit_ = (logType & LOG_TYPE_IS_AUTO_COMMIT) > 0;
	record.type_ = static_cast<LogType>(logType & LOG_TYPE_CLEAR_FLAGS);
	addr += sizeof(uint8_t);
	memcpy(&record.lsn_, addr, sizeof(LogSequentialNumber));
	addr += sizeof(LogSequentialNumber);
	memcpy(&record.bodyLen_, addr, sizeof(uint32_t));
	addr += sizeof(uint32_t);
	memcpy(&record.partitionId_, addr, sizeof(PartitionId));
	addr += sizeof(PartitionId);
	return static_cast<uint32_t>(addr - topAddr);
}

/*!
    @brief Parses body data of Log and sets to a LogRecord.
*/
uint32_t LogManager::parseLogBody(const uint8_t *&addr, LogRecord &record) {
	const uint8_t *topAddr = addr;


	addr += util::varIntDecode64(addr, record.clientId_.sessionId_);
	addr += util::varIntDecode64(addr, record.txnId_);
	addr += util::varIntDecode64(addr, record.containerId_);

	uint32_t flagValue = 0;
	switch(record.type_) {
	case LOG_TYPE_COMMIT:
		/* FALLTHROUGH */
	case LOG_TYPE_ABORT:
		addr += util::varIntDecode64(addr, record.stmtId_);
		memcpy(record.clientId_.uuid_, addr, TXN_CLIENT_UUID_BYTE_SIZE);
		addr += TXN_CLIENT_UUID_BYTE_SIZE;
		break;

	case LOG_TYPE_PUT_ROW:
		/* FALLTHROUGH */
	case LOG_TYPE_UPDATE_ROW:
		addr += util::varIntDecode64(addr, record.stmtId_);
		addr += util::varIntDecode32(addr, record.txnContextCreationMode_);
		addr += util::varIntDecode32(addr, record.txnTimeout_);
		addr += util::varIntDecode64(addr, record.numRowId_);
		addr += util::varIntDecode64(addr, record.numRow_);
		addr += util::varIntDecode32(addr, record.dataLen_);
		memcpy(record.clientId_.uuid_, addr, TXN_CLIENT_UUID_BYTE_SIZE);
		addr += TXN_CLIENT_UUID_BYTE_SIZE;
		record.rowIds_ = addr;
		addr += sizeof(RowId)*record.numRowId_;
		record.rowData_ = addr;
		addr += record.dataLen_;
		break;

	case LOG_TYPE_REMOVE_ROW:
		/* FALLTHROUGH */
	case LOG_TYPE_LOCK_ROW:
		addr += util::varIntDecode64(addr, record.stmtId_);
		addr += util::varIntDecode32(addr, record.txnContextCreationMode_);
		addr += util::varIntDecode32(addr, record.txnTimeout_);
		addr += util::varIntDecode64(addr, record.numRowId_);
		record.numRow_ = record.numRowId_;
		memcpy(record.clientId_.uuid_, addr, TXN_CLIENT_UUID_BYTE_SIZE);
		addr += TXN_CLIENT_UUID_BYTE_SIZE;
		record.rowIds_ = addr;
		addr += sizeof(RowId)*record.numRowId_;
		break;

	case LOG_TYPE_PUT_CONTAINER:
		addr += util::varIntDecode32(addr, flagValue);
		addr += util::varIntDecode32(addr, record.containerNameLen_);
		addr += util::varIntDecode32(addr, record.containerInfoLen_);
		record.containerType_ = flagValue;
		record.containerName_ = reinterpret_cast<const char*>(addr);
		addr += record.containerNameLen_;
		record.containerInfo_ = addr;
		addr += record.containerInfoLen_;
		record.isAutoCommit_ = true;
		break;

	case LOG_TYPE_DROP_CONTAINER:
		addr += util::varIntDecode32(addr, record.containerNameLen_);
		record.containerName_ = reinterpret_cast<const char*>(addr);
		addr += record.containerNameLen_;
		record.isAutoCommit_ = true;
		break;

	case LOG_TYPE_CREATE_INDEX:
		/* FALLTHROUGH */
	case LOG_TYPE_DROP_INDEX:
		addr += util::varIntDecode32(addr, record.columnId_);
		addr += util::varIntDecode32(addr, record.mapType_);
		record.isAutoCommit_ = true;
		break;

	case LOG_TYPE_CREATE_TRIGGER:
		addr += util::varIntDecode32(addr, record.containerNameLen_);
		addr += util::varIntDecode32(addr, record.containerInfoLen_);
		record.containerName_ = reinterpret_cast<const char*>(addr);
		addr += record.containerNameLen_;
		record.containerInfo_ = addr;
		addr += record.containerInfoLen_;
		record.isAutoCommit_ = true;
		break;

	case LOG_TYPE_DROP_TRIGGER:
		addr += util::varIntDecode32(addr, record.containerNameLen_);
		record.containerName_ = reinterpret_cast<const char*>(addr);
		addr += record.containerNameLen_;
		record.isAutoCommit_ = true;
		break;

	case LOG_TYPE_DROP_PARTITION:
		break;

	case LOG_TYPE_CHECKPOINT_START:
		{
			record.cpLsn_ = record.clientId_.sessionId_;
			addr += util::varIntDecode32(addr, record.txnContextNum_);
			addr += util::varIntDecode32(addr, record.containerNum_);
			record.clientIds_ = addr;
			addr += record.txnContextNum_ * sizeof(ClientId);
			record.activeTxnIds_ = addr;
			addr += record.txnContextNum_ * sizeof(TransactionId);
			record.refContainerIds_ = addr;
			addr += record.txnContextNum_ * sizeof(ContainerId);
			record.lastExecStmtIds_ = addr;
			addr += record.txnContextNum_ * sizeof(StatementId);
			record.txnTimeouts_ = addr;
			addr += record.txnContextNum_ * sizeof(uint32_t);
		}
		break;

	case LOG_TYPE_CHECKPOINT_END:
		{
			const uint8_t *endAddr = topAddr + record.bodyLen_;
			addr += util::varIntDecode64(addr, record.numRow_);
			record.dataLen_ = static_cast<uint32_t>(endAddr - addr);
			record.rowData_ = addr;
			addr = endAddr;
		}
		break;

	case LOG_TYPE_CHUNK_META_DATA:
		{
			record.chunkCategoryId_ = *addr;  
			++addr;
			uint32_t tmpData;
			addr += util::varIntDecode32(addr, tmpData);
			record.startChunkId_ = static_cast<ChunkId>(tmpData);
			addr += util::varIntDecode32(addr, tmpData);
			record.chunkNum_ = static_cast<int32_t>(tmpData);
			addr += util::varIntDecode32(addr, record.dataLen_);
			record.rowData_ = addr;
			addr += record.dataLen_;
		}
		break;

	case LOG_TYPE_PUT_CHUNK_START:
		{
			uint64_t tmpData;
			addr += util::varIntDecode64(addr, tmpData);
			record.numRow_ = tmpData;
		}
		break;

	case LOG_TYPE_PUT_CHUNK_DATA:
		{
			uint32_t tmpData;
			addr += util::varIntDecode32(addr, tmpData);
			record.dataLen_ = tmpData;
			record.rowData_ = addr;
			addr += record.dataLen_;
		}
		break;

	case LOG_TYPE_PUT_CHUNK_END:
		{
		}
		break;

	case LOG_TYPE_SHUTDOWN:
		{
			const uint8_t *endAddr = topAddr + record.bodyLen_;
			record.dataLen_ = static_cast<uint32_t>(endAddr - addr);
			record.rowData_ = addr;
			addr = endAddr;
		}
		break;

	case LOG_TYPE_NOOP:
		GS_TRACE_WARNING(LOG_MANAGER, GS_TRACE_LM_UNSUPPORTED_LOGTYPE,
						 "Unsupported logType=" << record.type_);
		addr = topAddr + record.bodyLen_;
		break;

	default:
		GS_TRACE_WARNING(LOG_MANAGER, GS_TRACE_LM_UNKNOWN_LOGTYPE,
						 "Unknown logType=" << record.type_);
		addr = topAddr + record.bodyLen_;
		break;
	}

	return static_cast<uint32_t>(addr - topAddr);
}

/*!
    @brief Returns size of a Log file.
*/
uint64_t LogManager::getLogSize(PartitionGroupId pgId) {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);

	PartitionGroupManager *pgManager = pgManagerList_[pgId];

	LogFileLatch fileLatch;
	fileLatch.set(*pgManager, pgManager->getLastCheckpointId(), true);

	LogBlocksLatch blocksLatch;
	return pgManager->getAvailableEndOffset(fileLatch, blocksLatch);
}

/*!
    @brief Checks if LogType is assignable.
*/
bool LogManager::isLsnAssignable(uint8_t logType) {
	switch (logType) {
	case LOG_TYPE_CHECKPOINT_START:
		/* FALLTHROUGH */
	case LOG_TYPE_CREATE_PARTITION:
		/* FALLTHROUGH */
	case LOG_TYPE_DROP_PARTITION:
		/* FALLTHROUGH */
	case LOG_TYPE_NOOP:
		/* FALLTHROUGH */
	case LOG_TYPE_CHECKPOINT_END:
		/* FALLTHROUGH */
	case LOG_TYPE_SHUTDOWN:
		/* FALLTHROUGH */
	case LOG_TYPE_CHUNK_META_DATA:
		/* FALLTHROUGH */
	case LOG_TYPE_PUT_CHUNK_START:
		/* FALLTHROUGH */
	case LOG_TYPE_PUT_CHUNK_DATA:
		/* FALLTHROUGH */
	case LOG_TYPE_PUT_CHUNK_END:
		return false;
	default:
		return true;
	}
}


/*!
    @brief Converts LogType to a string.
*/
const char* LogManager::logTypeToString(LogType type) {
	switch (type) {
	case LOG_TYPE_CHECKPOINT_START:
		return "CHECKPOINT_START";
		break;

	case LOG_TYPE_COMMIT:
		return "COMMIT";
		break;

	case LOG_TYPE_ABORT:
		return "ABORT";
		break;

	case LOG_TYPE_CREATE_PARTITION:
		return "CREATE_PARTITION";
		break;

	case LOG_TYPE_DROP_PARTITION:
		return "DROP_PARTITION";
		break;

	case LOG_TYPE_PUT_CONTAINER:
		return "PUT_CONTAINER";
		break;

	case LOG_TYPE_DROP_CONTAINER:
		return "DROP_CONTAINER";
		break;

	case LOG_TYPE_CREATE_INDEX:
		return "CREATE_INDEX";
		break;

	case LOG_TYPE_DROP_INDEX:
		return "DROP_INDEX";
		break;

	case LOG_TYPE_CREATE_TRIGGER:
		return "CREATE_TRIGGER";
		break;

	case LOG_TYPE_DROP_TRIGGER:
		return "DROP_TRIGGER";
		break;

	case LOG_TYPE_PUT_ROW:
		return "PUT_ROW";
		break;

	case LOG_TYPE_REMOVE_ROW:
		return "REMOVE_ROW";
		break;

	case LOG_TYPE_LOCK_ROW:
		return "LOCK_ROW";
		break;

	case LOG_TYPE_CHECKPOINT_END:
		return "CHECKPOINT_END";
		break;

	case LOG_TYPE_SHUTDOWN:
		return "SHUTDOWN";
		break;

	case LOG_TYPE_CHUNK_META_DATA:
		return "CHUNK_META_DATA";
		break;

	case LOG_TYPE_UPDATE_ROW:
		return "UPDATE_ROW";
		break;

	case LOG_TYPE_PUT_CHUNK_START:
		return "PUT_CHUNK_START";
		break;

	case LOG_TYPE_PUT_CHUNK_DATA:
		return "PUT_CHUNK_DATA";
		break;

	case LOG_TYPE_PUT_CHUNK_END:
		return "PUT_CHUNK_END";
		break;

	case LOG_TYPE_UNDEF:
	case LOG_TYPE_WITH_BEGIN:
	case LOG_TYPE_IS_AUTO_COMMIT:
	case LOG_TYPE_CLEAR_FLAGS:
	default:
		return "UNDEF";
	}
}

template<typename Alloc>
LogSequentialNumber LogManager::serializeLogCommonPart(
		util::XArray<uint8_t, Alloc> &logBuf,
		uint8_t type, PartitionId pId, SessionId sessionId,
		TransactionId txnId, ContainerId containerId) {

	static const uint32_t HEADER_RESERVE_LEN = HEADER_RESERVE_LEN * 5; 
	LogSequentialNumber lsn;
	if (isLsnAssignable(type)) {
		lsn = getLSN(pId) + 1;
	}
	else {
		lsn = getLSN(pId);
	}

	uint32_t len = 0; 

	logBuf.reserve(HEADER_RESERVE_LEN);
	logBuf.push_back(&type, sizeof(uint8_t));
	logBuf.push_back(reinterpret_cast<uint8_t*>(&lsn), sizeof(LogSequentialNumber));
	logBuf.push_back(reinterpret_cast<uint8_t*>(&len), sizeof(uint32_t));
	logBuf.push_back(reinterpret_cast<uint8_t*>(&pId), sizeof(pId));

	uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 3];
	uint8_t *addr = tmp;
	addr += util::varIntEncode64(addr, sessionId);
	addr += util::varIntEncode64(addr, txnId);
	addr += util::varIntEncode64(addr, containerId);

	logBuf.push_back(tmp, (addr - tmp));

	return lsn;
}

template<typename Alloc>
void LogManager::putBlockTailNoopLog(
		util::XArray<uint8_t, Alloc> &logBuf, PartitionId pId) {
	const PartitionGroupId pgId = calcPartitionGroupId(pId);
	const uint64_t lastLogSize = getLogSize(pgId);

	const uint32_t blockTailOffset = getBlockOffset(config_, lastLogSize);
	if (blockTailOffset == 0) {
		return;
	}

	const size_t orgBufSize = logBuf.size();
	try {
		const LogSequentialNumber lsn =
				serializeLogCommonPart(logBuf, LOG_TYPE_NOOP, pId, 0, 0, 0);

		const uint32_t blockRemaining = config_.getBlockSize() - blockTailOffset;

		const size_t logRecordSize =
				std::max<size_t>(blockRemaining, logBuf.size() - orgBufSize);

		logBuf.resize(
				orgBufSize + logRecordSize, LOG_RECORD_PADDING_ELEMENT);
		putLog(pId, lsn, logBuf.data() + orgBufSize, logRecordSize, true);
	}
	catch (...) {
		logBuf.resize(orgBufSize);
		throw;
	}
	logBuf.resize(orgBufSize);

	assert(getBlockIndex(config_, getLogSize(pgId)) ==
			getBlockIndex(config_, lastLogSize) + 1);
}

void LogManager::putLog(PartitionId pId, LogSequentialNumber lsn,
		uint8_t *serializedLogRecord, size_t logRecordLen,
		bool needUpdateLength) {


	const size_t lenOffset = sizeof(uint8_t) + sizeof(LogSequentialNumber);
	const uint32_t bodyLen =
			static_cast<uint32_t>(logRecordLen) - LogRecord::LOG_HEADER_SIZE;
	if (needUpdateLength) {
		memcpy(serializedLogRecord + lenOffset, &bodyLen, sizeof(bodyLen));
	}
	else {
		uint32_t actualBodyLen;
		memcpy(&actualBodyLen,
				serializedLogRecord + lenOffset, sizeof(actualBodyLen));

		if (actualBodyLen != bodyLen) {
			GS_THROW_USER_ERROR(
					GS_ERROR_LM_INVALID_LOG_RECORD,
					"Invalid log record body size (expected=" << bodyLen <<
					", actual=" << actualBodyLen << ")");
		}
	}

	const PartitionGroupId pgId = calcPartitionGroupId(pId);

	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, true);
	pgManagerList_[pgId]->putLog(
			serializedLogRecord, static_cast<uint32_t>(logRecordLen));

	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, false);
	setLSN(pId, lsn);
}

void LogManager::flushByCommit(PartitionId pId) {
	if (config_.alwaysFlushOnTxnEnd_) {
		const PartitionGroupId pgId = calcPartitionGroupId(pId);
		writeBuffer(pgId);
		flushFile(pgId);
	}
}

LogSequentialNumber LogManager::putPutOrUpdateRowLog(LogType logType,
													 util::XArray<uint8_t> &binaryLogBuf,
													 PartitionId pId,
													 const ClientId &clientId,
													 TransactionId txnId,
													 ContainerId containerId,
													 StatementId stmtId,
													 uint64_t numRowId, const util::XArray<RowId> &rowIds,
													 uint64_t numRow, const util::XArray<uint8_t> &rowData,
													 int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
													 bool withBegin, bool isAutoCommit)
{
	assert(!(withBegin && isAutoCommit));

	LogSequentialNumber lsn = 0;
	try {
		uint8_t beginFlag = withBegin ? LOG_TYPE_WITH_BEGIN : 0;
		uint8_t autoCommitFlag = isAutoCommit ? LOG_TYPE_IS_AUTO_COMMIT : 0;
		lsn = serializeLogCommonPart(
			binaryLogBuf, static_cast<uint8_t>(logType) | beginFlag | autoCommitFlag,
			pId, clientId.sessionId_, txnId, containerId);

		uint8_t tmp[LOGMGR_VARINT_MAX_LEN * 6];
		uint8_t *addr = tmp;
		addr += util::varIntEncode64(addr, stmtId);
		addr += util::varIntEncode32(addr, txnContextCreateMode);
		addr += util::varIntEncode32(addr, txnTimeoutInterval);
		addr += util::varIntEncode64(addr, numRowId);
		addr += util::varIntEncode64(addr, numRow);
		uint32_t dataLen = static_cast<uint32_t>(rowData.size());
		addr += util::varIntEncode32(addr, dataLen);
		binaryLogBuf.push_back(tmp, (addr - tmp));
		binaryLogBuf.push_back(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		binaryLogBuf.push_back(
			reinterpret_cast<const uint8_t*>(rowIds.data()), numRowId * sizeof(RowId));
		binaryLogBuf.push_back(
			reinterpret_cast<const uint8_t*>(rowData.data()), dataLen);

		putLog(pId, lsn, binaryLogBuf.data(), binaryLogBuf.size());
		if (isAutoCommit) {
			flushByCommit(pId);
		}
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Write log failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	return lsn;
}

const char* LogManager::maskDirectoryPath(const char *dirPath) {
	if (strlen(dirPath) == 0) {
		return ".";
	}
	else {
		return dirPath;
	}
}

inline uint64_t LogManager::getBlockIndex(
		const Config &config, uint64_t fileOffset) {
	return fileOffset >> config.blockSizeBits_;
}

inline uint32_t LogManager::getBlockOffset(
		const Config &config, uint64_t fileOffset) {
	return static_cast<uint32_t>(
			fileOffset & ((1 << config.blockSizeBits_) - 1));
}

inline uint64_t LogManager::getFileOffset(
		const Config &config, uint64_t blockIndex, uint32_t blockOffset) {
	return ((blockIndex << config.blockSizeBits_) + blockOffset);
}

inline uint32_t LogManager::getBlockRemaining(
		const Config &config, uint64_t fileOffset) {
	return ((1 << config.blockSizeBits_) - getBlockOffset(config, fileOffset));
}

LogManager::ConfigSetUpHandler LogManager::configSetUpHandler_;

void LogManager::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_DS, "dataStore");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_DS_PERSISTENCY_MODE, INT32).
			setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM).
			addEnum(LogManager::PERSISTENCY_NORMAL, "NORMAL").
			addEnum(LogManager::PERSISTENCY_KEEP_ALL_LOG, "KEEP_ALL_LOG").
			setDefault(LogManager::PERSISTENCY_NORMAL);
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_DS_RETAINED_FILE_COUNT, INT32).
			setMin(2).
			setDefault(2);
}


/*!
    @brief Returns the version of Log file.
*/
uint16_t LogManager::getFileVersion(PartitionGroupId pgId, CheckpointId cpId) { 
	if (pgId >= pgManagerList_.size()) {
		GS_THROW_USER_ERROR(
			GS_ERROR_LM_LOG_FILES_CONFIGURATION_UNMATCHED,
			"Existing log files do not match the server configuration"
			" (expectedPartitionGroupNum=" <<
			config_.pgConfig_.getPartitionGroupCount() <<
			", requestedLogFilePartitionGroupNum=" << pgId << ")");
		return 0;
	}
	LogFileLatch fileLatch;
	fileLatch.set(*pgManagerList_[pgId], cpId, true);

	LogBlocksLatch blocksLatch;
	blocksLatch.setForScan(fileLatch, 0);

	return blocksLatch.get()->getVersion();
}






LogManager::Config::Config(ConfigTable &configTable) :
		configTable_(&configTable),
		pgConfig_(configTable),
		persistencyMode_(configTable.get<int32_t>(
				CONFIG_TABLE_DS_PERSISTENCY_MODE)),
		logWriteMode_(configTable.get<int32_t>(
				CONFIG_TABLE_DS_LOG_WRITE_MODE)),
		alwaysFlushOnTxnEnd_(false),
		logDirectory_(configTable.get<const char8_t*>(
				CONFIG_TABLE_DS_DB_PATH)),
		blockSizeBits_(18),
		blockBufferCountBits_(3),
		lockRetryCount_(10),
		lockRetryIntervalMillis_(500),
		indexEnabled_(false),
		indexBufferSizeBits_(17),
		primaryCheckpointAssumed_(false),
		emptyFileAppendable_(false),
		ioWarningThresholdMillis_(configTable.getUInt32(
				CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_TIME)),
		retainedFileCount_(configTable.getUInt32(
				CONFIG_TABLE_DS_RETAINED_FILE_COUNT)),
		lsnBaseFileRetention_(false) {
	if (logWriteMode_ == 0 || logWriteMode_ == -1) {
		alwaysFlushOnTxnEnd_ = true;
	}
}

void LogManager::Config::setUpConfigHandler() {
	if (!configTable_->isSet(CONFIG_TABLE_DS_RETAINED_FILE_COUNT)) {
		configTable_->setParamHandler(CONFIG_TABLE_DS_RETAINED_FILE_COUNT, *this);
	}
}

void LogManager::Config::operator()(
		ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_DS_RETAINED_FILE_COUNT:
		retainedFileCount_ = value.get<int32_t>();
		break;
	}
}

uint32_t LogManager::Config::getBlockSize() const {
	return (1 << blockSizeBits_);
}

uint32_t LogManager::Config::getBlockBufferCount() const {
	return (1 << blockBufferCountBits_);
}

uint32_t LogManager::Config::getIndexBufferSize() const {
	return (1 << indexBufferSizeBits_);
}


LogManager::ReferenceCounter::ReferenceCounter() :
		count_(0) {
}

size_t LogManager::ReferenceCounter::getCount() const {
	return count_;
}

size_t LogManager::ReferenceCounter::increment() {
	return ++count_;
}

size_t LogManager::ReferenceCounter::decrement() {
	assert(count_ > 0);

	return --count_;
}


LogManager::LogFileInfo::LogFileInfo() :
		cpId_(UNDEF_CHECKPOINT_ID),
		file_(NULL),
		fileSize_(0),
		availableFileSize_(0),
		config_(NULL)
{
}

void LogManager::LogFileInfo::open(const Config &config,
		PartitionGroupId pgId, CheckpointId cpId, bool expectExisting,
		bool checkOnly, const char8_t *suffix) {
	close();

	const std::string filePath = createLogFilePath(
			config.logDirectory_.c_str(), pgId, cpId, suffix);
	try {
		UTIL_UNIQUE_PTR<util::NamedFile> file(UTIL_NEW util::NamedFile());

		util::FileFlag flags = (checkOnly ?
				util::FileFlag::TYPE_READ_ONLY : util::FileFlag::TYPE_READ_WRITE);
		if (!expectExisting) {
			flags = flags | util::FileFlag::TYPE_CREATE | util::FileFlag::TYPE_EXCLUSIVE;
		}
		file->open(filePath.c_str(), flags);

		cpId_ = cpId;
		file_ = file.release();
		config_ = &config;


		if (!checkOnly) {
			lock();
		}

		util::FileStatus status;
		file_->getStatus(&status);
		fileSize_ = status.getSize();
		if (expectExisting) {
			availableFileSize_ = std::numeric_limits<uint64_t>::max();
		}
		else {
			availableFileSize_ = 0;
		}

		if (!expectExisting && fileSize_ != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_LM_CREATE_LOG_FILE_FAILED,
					"Created file is not empty");
		}
	}
	catch(std::exception &e) {
		try {
			close();
		}
		catch (...) {
		}

		GS_RETHROW_USER_ERROR(
			e, "Failed to open (path=" << filePath <<
			", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void LogManager::LogFileInfo::close() {
	if (isClosed()) {
		return;
	}

	UTIL_UNIQUE_PTR<util::File> file(file_);

	cpId_ = UNDEF_CHECKPOINT_ID;
	file_ = NULL;
	fileSize_ = 0;
	availableFileSize_ = 0;
	config_ = NULL;

	try {
		file->close();
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(
			e, "Close log file failed. reason=("
			<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

bool LogManager::LogFileInfo::isClosed() const {
	return (file_ == NULL);
}

std::string LogManager::LogFileInfo::createLogFilePath(
		const char8_t *dirPath, PartitionGroupId pgId, CheckpointId cpId,
		const char8_t *suffix) {
	util::NormalOStringStream oss;

	oss << maskDirectoryPath(dirPath) << "/" <<
			LOG_FILE_BASE_NAME << pgId <<
			LOG_FILE_SEPARATOR << cpId <<
			(suffix ? suffix : "") << LOG_FILE_EXTENSION;

	return oss.str();
}

LogManager::ReferenceCounter& LogManager::LogFileInfo::getReferenceCounter() {
	return referenceCounter_;
}

CheckpointId LogManager::LogFileInfo::getCheckpointId() const {
	assert(!isClosed());

	return cpId_;
}

uint64_t LogManager::LogFileInfo::getFileSize() const {
	assert(!isClosed());

	return fileSize_;
}

void LogManager::LogFileInfo::setAvailableFileSize(uint64_t size) {
	assert(!isClosed());
	assert(size <= fileSize_);

	availableFileSize_ = size;
}

bool LogManager::LogFileInfo::isAvailableFileSizeUpdated() const {
	return (availableFileSize_ != std::numeric_limits<uint64_t>::max());
}

uint64_t LogManager::LogFileInfo::getTotalBlockCount(
		bool ignoreIncompleteTail) const {
	assert(!isClosed());

	if (ignoreIncompleteTail) {
		assert(isAvailableFileSizeUpdated());
		return getBlockIndex(*config_, availableFileSize_);
	}
	else {
		return getBlockIndex(*config_, fileSize_ + config_->getBlockSize() - 1);
	}
}

uint32_t LogManager::LogFileInfo::readBlock(
		void *buffer, uint64_t startIndex, uint32_t count) {
	assert(!isClosed());

	const uint64_t startOffset = getFileOffset(*config_, startIndex);
	const uint64_t blockEndOffset = getFileOffset(*config_, startIndex + count);

	uint64_t endOffset = blockEndOffset;
	if (endOffset > fileSize_) {
		if (startOffset >= fileSize_ ||
				startIndex + count != getBlockIndex(*config_, fileSize_)) {
			GS_THROW_USER_ERROR(GS_ERROR_LM_READ_LOG_BLOCK_FAILED,
					"Illegal file size (path=" << file_->getName() << ")");
		}
		endOffset = fileSize_;
	}

	const uint32_t requestSize = static_cast<uint32_t>(endOffset - startOffset);
	try {
		util::Stopwatch watch(util::Stopwatch::STATUS_STARTED);
		const int64_t resultSize = file_->read(buffer, requestSize, startOffset);
		const uint32_t lap = watch.elapsedMillis();

		if (resultSize != static_cast<int64_t>(requestSize)) {
			GS_THROW_USER_ERROR(GS_ERROR_LM_READ_LOG_BLOCK_FAILED,
					"Result size unmatched (path=" << file_->getName() << ")");
		}
		if (lap > config_->ioWarningThresholdMillis_) { 
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_IO,
					"[LONG I/O] read time," << lap <<
				   ",fileName," << file_->getName() <<
				   ",startIndex," << startIndex << ",count," << count); 
		}
	}
	catch (std::exception &e) {
		try {
			file_->close();
		}
		catch (...) {
		}

		GS_RETHROW_USER_ERROR(e, "Read log block failed. reason=("
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}

	return requestSize;
}

void LogManager::LogFileInfo::writeBlock(
		const void *buffer, uint64_t startIndex, uint32_t count) {
	assert(!isClosed());

	const uint64_t startOffset = getFileOffset(*config_, startIndex);
	const uint64_t endOffset = getFileOffset(*config_, startIndex + count);

	if (startOffset > fileSize_) {
		GS_THROW_USER_ERROR(GS_ERROR_LM_WRITE_LOG_BLOCK_FAILED,
				"Illegal file size (path=" << file_->getName() << ")");
	}

	const uint32_t requestSize = static_cast<uint32_t>(endOffset - startOffset);
	try {
		util::Stopwatch watch(util::Stopwatch::STATUS_STARTED);
		const int64_t resultSize = file_->write(buffer, requestSize, startOffset);
		const uint32_t lap = watch.elapsedMillis();

		if (resultSize != static_cast<int64_t>(requestSize)) {
			GS_THROW_USER_ERROR(GS_ERROR_LM_WRITE_LOG_BLOCK_FAILED,
					"Result size unmatched (path=" << file_->getName() << ")");
		}
		if (lap > config_->ioWarningThresholdMillis_) { 
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_IO,
					"[LONG I/O] write time," << lap <<
				   ",fileName," << file_->getName() <<
				   ",startIndex," << startIndex << ",count," << count); 
		}

		fileSize_ = std::max(fileSize_, endOffset);
		availableFileSize_ = std::max(fileSize_, endOffset);
	}
	catch (std::exception &e) {
		try {
			file_->close();
		}
		catch (...) {
		}

		GS_RETHROW_USER_ERROR(e, "Write log block failed. reason=("
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void LogManager::LogFileInfo::flush() {
	assert(!isClosed());
	flush(*file_, *config_, true);
}

void LogManager::LogFileInfo::flush(
		util::NamedFile &file, const Config &config, bool failOnClose) {
	try {
		util::Stopwatch watch(util::Stopwatch::STATUS_STARTED);
		file.sync();
		const uint32_t lap = watch.elapsedMillis();
		if (lap > config.ioWarningThresholdMillis_) { 
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_IO,
					"[LONG I/O] sync time," << lap <<
					",fileName," << file.getName());
		}
	}
	catch (std::exception &e) {
		if (failOnClose) {
			try {
				file.close();
			}
			catch (...) {
			}
		}

		GS_RETHROW_USER_ERROR(e, "Sync log file failed. reason=("
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void LogManager::LogFileInfo::lock() {
	assert(!isClosed());

	try {
		for (uint32_t i = 0;; i++) {
			if (file_->lock()) {
				break;
			}

			if (i >= config_->lockRetryCount_) {
				GS_THROW_USER_ERROR(GS_ERROR_LM_LOCK_LOG_FILE_FAILED,
						"Lock failed (path=" << file_->getName() <<
						", retryCount=" << i << ")");
			}

			util::Thread::sleep(config_->lockRetryIntervalMillis_);
		}
	}
	catch (std::exception &e) {
		try {
			file_->close();
		}
		catch (...) {
		}

		GS_RETHROW_USER_ERROR(e, "Lock log file failed. reason=("
							  << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}



const uint16_t LogManager::LogBlocksInfo::LOGBLOCK_VERSION = 0x8;

const uint16_t
LogManager::LogBlocksInfo::LOGBLOCK_ACCEPTABLE_VERSIONS[] = {
		LOGBLOCK_VERSION,
		0 /* sentinel */
};

LogManager::LogBlocksInfo::LogBlocksInfo() :
		blocks_(NULL),
		startIndex_(0),
		blockCount_(0),
		config_(NULL) {
}

void LogManager::LogBlocksInfo::initialize(
		const Config &config, BlockPool &blocksPool,
		uint64_t startIndex, bool multiple) {
	if (isEmpty()) {
		clear(blocksPool);
	}

	try {
		blocks_ = static_cast<uint8_t*>(blocksPool.allocate());
		assert(blocks_ != NULL);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Initialize failed. reason=("
								<< GS_EXCEPTION_MESSAGE(e) << ")");
	}

	startIndex_ = startIndex;
	blockCount_= (multiple ? (1 << config.blockBufferCountBits_) : 1);
	config_ = &config;

	assert(blockCount_ > 0);

	memset(blocks_, 0, config.getBlockSize() * blockCount_);
}

void LogManager::LogBlocksInfo::clear(BlockPool &blocksPool) {
	uint8_t *blocks = blocks_;

	blocks_ = NULL;
	startIndex_ = 0;
	blockCount_ = 0;
	config_ = NULL;

	if (blocks != NULL) {
		blocksPool.deallocate(blocks);
	}
}

bool LogManager::LogBlocksInfo::isEmpty() const {
	return (blocks_ == NULL);
}

LogManager::ReferenceCounter&
LogManager::LogBlocksInfo::getReferenceCounter() {
	return referenceCounter_;
}

uint32_t LogManager::LogBlocksInfo::getBlockCount() const {
	return blockCount_;
}

uint64_t LogManager::LogBlocksInfo::getStartIndex() const {
	return startIndex_;
}

uint64_t LogManager::LogBlocksInfo::getFileEndOffset() const {
	return getFileOffset(*config_, startIndex_ + blockCount_);
}

bool LogManager::LogBlocksInfo::prepareNextBodyOffset(
		uint64_t &totalOffset) const {
	if (totalOffset >= getFileEndOffset()) {
		return false;
	}

	const uint32_t blockOffset = getBlockOffset(*config_, totalOffset);
	if (blockOffset < LOGBLOCK_HEADER_SIZE) {
		assert(blockOffset == 0);

		const uint64_t blockIndex = getBlockIndex(*config_, totalOffset);
		totalOffset = getFileOffset(*config_, blockIndex, LOGBLOCK_HEADER_SIZE);
	}

	return true;
}

uint32_t LogManager::LogBlocksInfo::getBodyPart(
		uint8_t *buffer, uint64_t index, uint32_t offset, uint32_t size) const {
	assert(!isEmpty());

	assert(offset >= LOGBLOCK_HEADER_SIZE);
	assert(offset <= config_->getBlockSize());

	const uint8_t *src = blocks_ + getBlockHeadOffset(index) + offset;
	const uint32_t copySize = static_cast<uint32_t>(
			std::min(size, config_->getBlockSize() - offset));

	memcpy(buffer, src, copySize);

	return copySize;
}

uint32_t LogManager::LogBlocksInfo::putBodyPart(
		const uint8_t *buffer, uint64_t index, uint32_t offset, uint32_t size) {
	assert(!isEmpty());

	assert(offset >= LOGBLOCK_HEADER_SIZE);
	assert(offset <= config_->getBlockSize());

	uint8_t *dest = blocks_ + getBlockHeadOffset(index) + offset;
	const uint32_t copySize = static_cast<uint32_t>(
			std::min(size, config_->getBlockSize() - offset));

	memcpy(dest, buffer, copySize);

	return copySize;
}

void LogManager::LogBlocksInfo::fillBodyRemaining(
		uint64_t index, uint32_t offset, uint8_t value) {
	assert(!isEmpty());

	if (offset == 0) {
		return;
	}

	assert(offset >= LOGBLOCK_HEADER_SIZE);
	assert(offset <= config_->getBlockSize());

	uint8_t *begin = blocks_ + getBlockHeadOffset(index) + offset;
	uint8_t *end = blocks_ + getBlockHeadOffset(index) + config_->getBlockSize();

	std::fill(begin, end, value);
}

uint32_t LogManager::LogBlocksInfo::copyAllFromFile(LogFileInfo &fileInfo) {
	assert(!isEmpty());

	const uint64_t totalBlockCount =
			getBlockIndex(*config_, fileInfo.getFileSize());
	const uint64_t endIndex =
			std::min(startIndex_ + blockCount_, totalBlockCount);

	if (startIndex_ >= endIndex) {
		if (startIndex_ > endIndex) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_LM_INTERNAL_ERROR_OTHER, "");
		}

		if (!fileInfo.isAvailableFileSizeUpdated()) {
			const uint64_t availableSize =
					getFileOffset(*config_, totalBlockCount);
			const uint64_t totalSize = fileInfo.getFileSize();

			if (availableSize != totalSize) {
				GS_TRACE_WARNING(LOG_MANAGER, GS_TRACE_LM_INCOMPLETE_TAIL_BLOCK,
						"Tail block is incomplete (checkpointId=" <<
						fileInfo.getCheckpointId() <<
						", blockIndex=" << totalBlockCount <<
						", availableSize=" << availableSize <<
						", totalSize=" << totalSize <<
						", incompleteSize=" << (totalSize - availableSize) << ")");
			}

			fileInfo.setAvailableFileSize(availableSize);
		}

		return 0;
	}

	const uint32_t copyBlockCount =
			static_cast<uint32_t>(endIndex - startIndex_);
	const uint32_t copySize = fileInfo.readBlock(
			blocks_, startIndex_, copyBlockCount);
	assert(copySize == copyBlockCount * config_->getBlockSize());

	uint32_t validBlockCount = 0;
	for (uint64_t index = startIndex_; index < endIndex; index++) {
		if (index + 1 < totalBlockCount) {
			validate(index);
			validBlockCount++;
		}
		else {
			uint64_t availableBlockCount;
			try {
				validate(index);
				validBlockCount++;

				availableBlockCount = index + 1;
			}
			catch (UserException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(
						LOG_MANAGER, e, "Tail block is corrupted (checkpointId=" <<
						fileInfo.getCheckpointId() << ", blockIndex=" << index << ")");

				availableBlockCount = index;
			}
			fileInfo.setAvailableFileSize(
					getFileOffset(*config_, availableBlockCount));
		}
	}

	memset(blocks_ + (validBlockCount << config_->blockSizeBits_), 0,
			(blockCount_ - validBlockCount) << config_->blockSizeBits_);

	return copySize;
}

void LogManager::LogBlocksInfo::copyToFile(
		LogFileInfo &fileInfo, uint64_t index) {
	assert(!isEmpty());

	updateChecksum(index);

	const uint8_t *src = blocks_ + getBlockHeadOffset(index);
	fileInfo.writeBlock(src, index, 1);
}

void LogManager::LogBlocksInfo::initializeBlockHeader(uint64_t index) {
	memset(blocks_ + getBlockHeadOffset(index), 0, LOGBLOCK_HEADER_SIZE);
	setHeaderFileld(index, LOGBLOCK_VERSION_OFFSET, LOGBLOCK_VERSION);
	setHeaderFileld(index, LOGBLOCK_MAGIC_OFFSET, LOGBLOCK_MAGIC_NUMBER);
	setHeaderFileld<uint32_t>(index, LOGBLOCK_LSN_INFO_OFFSET, 0);
}

uint64_t LogManager::LogBlocksInfo::getLastCheckpointId(
		uint64_t index) const {
	return getHeaderFileld<uint64_t>(
			index, LOGBLOCK_LAST_CHECKPOINT_ID_OFFSET);
}

void LogManager::LogBlocksInfo::setLastCheckpointId(
		uint64_t index, uint64_t cpId) {
	setHeaderFileld(index, LOGBLOCK_LAST_CHECKPOINT_ID_OFFSET, cpId);
}

uint64_t LogManager::LogBlocksInfo::getCheckpointFileInfoOffset(
		uint64_t index) const {
	const uint64_t maskedOffset = getHeaderFileld<uint64_t>(
			index, LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET);
	const uint64_t offset =
			maskedOffset & ~LOGBLOCK_NORMAL_SHUTDOWN_FLAG;

	if (offset != 0 && !isValidRecordOffset(*config_, offset)) {
		GS_THROW_USER_ERROR(GS_ERROR_LM_INVALID_LOG_BLOCK, "");
	}

	return offset;
}

void LogManager::LogBlocksInfo::setCheckpointFileInfoOffset(
		uint64_t index, uint64_t offset) {
	if (offset != 0 && !isValidRecordOffset(*config_, offset)) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_LM_INVALID_LOG_BLOCK, "");
	}

	const uint64_t maskedOffset = offset | (
			isNormalShutdownCompleted(index) ?
					LOGBLOCK_NORMAL_SHUTDOWN_FLAG : 0);

	setHeaderFileld(index,
			LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET, maskedOffset);
}

uint32_t LogManager::LogBlocksInfo::getFirstRecordOffest(
		uint64_t index) const {
	const uint32_t offset = getHeaderFileld<uint32_t>(
			index, LOGBLOCK_FIRSTRECORD_OFFSET);

	if (offset != 0 && !(isValidRecordOffset(*config_, offset) &&
			offset < config_->getBlockSize())) {
		GS_THROW_USER_ERROR(GS_ERROR_LM_INVALID_LOG_BLOCK, "");
	}

	return offset;
}

void LogManager::LogBlocksInfo::setFirstRecordOffest(
		uint64_t index, uint32_t offset) {
	if (offset != 0 && !(isValidRecordOffset(*config_, offset) &&
			offset < config_->getBlockSize())) {
		GS_THROW_SYSTEM_ERROR(
				GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
	}

	setHeaderFileld(index, LOGBLOCK_FIRSTRECORD_OFFSET, offset);
}

bool LogManager::LogBlocksInfo::isNormalShutdownCompleted(
		uint64_t index) const {
	const uint64_t maskedOffset = getHeaderFileld<uint64_t>(
			index, LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET);

	return ((maskedOffset & LOGBLOCK_NORMAL_SHUTDOWN_FLAG) != 0);
}

void LogManager::LogBlocksInfo::setNormalShutdownCompleted(
		uint64_t index, bool completed) {
	uint64_t maskedOffset = getHeaderFileld<uint64_t>(
			index, LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET);
	maskedOffset |=
			(completed ? LOGBLOCK_NORMAL_SHUTDOWN_FLAG : 0);

	setHeaderFileld(
			index, LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET,
			maskedOffset);
}

uint16_t LogManager::LogBlocksInfo::getVersion() const {
	assert(blockCount_ > 0);

	return getHeaderFileld<uint16_t>(startIndex_, LOGBLOCK_VERSION_OFFSET);
}

bool LogManager::LogBlocksInfo::isValidRecordOffset(
		const Config &config, uint64_t offset) {
	const uint32_t blockOffset = getBlockOffset(config, offset);

	return (LOGBLOCK_HEADER_SIZE <= blockOffset &&
			blockOffset <= config.getBlockSize() - LogRecord::LOG_HEADER_SIZE);
}

const uint8_t* LogManager::LogBlocksInfo::data() const {
	return blocks_;
}

void LogManager::LogBlocksInfo::validate(uint64_t index) const {
	if (getHeaderFileld<uint32_t>(index, LOGBLOCK_CHECKSUM_OFFSET) !=
			getChecksum(index)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_LM_INVALID_LOG_BLOCK,
				"Log file corrupted (Check sum error)");
	}

	const uint16_t version =
			getHeaderFileld<uint16_t>(index, LOGBLOCK_VERSION_OFFSET);

	for (const uint16_t *acceptableVersions = LOGBLOCK_ACCEPTABLE_VERSIONS;;
			acceptableVersions++) {

		if (version == *acceptableVersions) {
			break;
		}
		else if (*acceptableVersions == 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_LM_LOG_FILE_VERSION_UNMATCHED,
					"Version unmatched (version=" << version << ")");
		}

	}

	if (getHeaderFileld<uint16_t>(index, LOGBLOCK_MAGIC_OFFSET) !=
			LOGBLOCK_MAGIC_NUMBER) {
		GS_THROW_USER_ERROR(
				GS_ERROR_LM_INVALID_LOG_BLOCK,
				"Log file corrupted (Invalid magic number)");
	}
}

void LogManager::LogBlocksInfo::updateChecksum(uint64_t index) {
	const uint32_t checksum = getChecksum(index);

	setHeaderFileld(index, LOGBLOCK_CHECKSUM_OFFSET, checksum);
}

uint32_t LogManager::LogBlocksInfo::getChecksum(uint64_t index) const {
	return util::fletcher32(
			blocks_ + getBlockHeadOffset(index) + LOGBLOCK_HEADER_SIZE,
			config_->getBlockSize() - LOGBLOCK_HEADER_SIZE);
}

template<typename T>
T LogManager::LogBlocksInfo::getHeaderFileld(
		uint64_t index, uint32_t offset) const {
	T value;
	memcpy(&value, blocks_ + getBlockHeadOffset(index) + offset, sizeof(T));

	return value;
}

template<typename T>
void LogManager::LogBlocksInfo::setHeaderFileld(
		uint64_t index, uint32_t offset, T value) {
	memcpy(blocks_ + getBlockHeadOffset(index) + offset, &value, sizeof(T));
}

uint32_t LogManager::LogBlocksInfo::getBlockHeadOffset(uint64_t index) const {
	assert(startIndex_ <= index);
	assert(index < startIndex_ + blockCount_);

	return static_cast<uint32_t>(
			(index - startIndex_) << config_->blockSizeBits_);
}


LogManager::LogFileLatch::LogFileLatch() :
		manager_(NULL),
		fileInfo_(NULL) {
}

LogManager::LogFileLatch::~LogFileLatch() {
	try {
		clear();
	}
	catch (...) {
	}
}

void LogManager::LogFileLatch::set(
		PartitionGroupManager &manager,
		CheckpointId cpId, bool expectExisting,
		const char8_t *suffix) {
	LogFileLatch oldLatch;
	if (get() != NULL) {
		duplicate(oldLatch);
	}

	clear();

	fileInfo_ = manager.latchFile(cpId, expectExisting, suffix);
	manager_ = &manager;
}

void LogManager::LogFileLatch::clear() {
	if (fileInfo_ != NULL) {
		manager_->unlatchFile(fileInfo_);
	}

	fileInfo_ = NULL;
	manager_ = NULL;
}

LogManager::PartitionGroupManager* LogManager::LogFileLatch::getManager() {
	return manager_;
}

LogManager::LogFileInfo* LogManager::LogFileLatch::get() {
	return fileInfo_;
}

LogManager::LogFileInfo* LogManager::LogFileLatch::get() const {
	return fileInfo_;
}

void LogManager::LogFileLatch::duplicate(LogFileLatch &dest) {
	assert(get() != NULL);

	dest.clear();

	fileInfo_->getReferenceCounter().increment();

	dest.manager_ = manager_;
	dest.fileInfo_ = fileInfo_;
}


LogManager::LogBlocksLatch::LogBlocksLatch() :
		blocksInfo_(NULL) {
}

LogManager::LogBlocksLatch::~LogBlocksLatch() {
	try {
		clear();
	}
	catch (...) {
	}
}

void LogManager::LogBlocksLatch::setForScan(
		LogFileLatch &fileLatch, uint64_t startIndex) {
	LogBlocksLatch oldLatch;
	if (get() != NULL) {
		duplicate(oldLatch);
	}

	const bool blocksMultiple = true;
	set(fileLatch, startIndex, blocksMultiple);

	if (blocksInfo_->getReferenceCounter().getCount() == 1) {
		blocksInfo_->copyAllFromFile(*fileLatch_.get());
	}
}

void LogManager::LogBlocksLatch::setForAppend(LogFileLatch &fileLatch) {
	assert(fileLatch.get() != NULL);

	LogBlocksLatch oldLatch;
	if (get() != NULL) {
		duplicate(oldLatch);
	}

	const bool ignoreIncompleteTail = true;
	const uint64_t totalBlockCount =
			fileLatch.get()->getTotalBlockCount(ignoreIncompleteTail);

	const bool blocksMultiple = true;
	set(fileLatch, totalBlockCount, blocksMultiple);
}

void LogManager::LogBlocksLatch::clear() {
	if (blocksInfo_ == NULL) {
		return;
	}

	try {
		fileLatch_.getManager()->unlatchBlocks(fileLatch_, blocksInfo_);
		fileLatch_.clear();
	}
	catch (...) {
		blocksInfo_ = NULL;
		try {
			fileLatch_.clear();
		}
		catch (...) {
		}

		throw;
	}

	blocksInfo_ = NULL;
}

LogManager::PartitionGroupManager* LogManager::LogBlocksLatch::getManager() {
	return fileLatch_.getManager();
}

LogManager::LogFileInfo* LogManager::LogBlocksLatch::getFileInfo() {
	return fileLatch_.get();
}

LogManager::LogFileInfo* LogManager::LogBlocksLatch::getFileInfo() const {
	return fileLatch_.get();
}

LogManager::LogBlocksInfo* LogManager::LogBlocksLatch::get() {
	return blocksInfo_;
}

LogManager::LogBlocksInfo* LogManager::LogBlocksLatch::get() const {
	return blocksInfo_;
}

void LogManager::LogBlocksLatch::duplicate(LogBlocksLatch &dest) {
	assert(get() != NULL);

	dest.clear();

	fileLatch_.duplicate(dest.fileLatch_);

	try {
		blocksInfo_->getReferenceCounter().increment();
	}
	catch (...) {
		dest.clear();
		throw;
	}

	dest.blocksInfo_ = blocksInfo_;
}

void LogManager::LogBlocksLatch::set(
		LogFileLatch &fileLatch, uint64_t startIndex, bool multiple) {
	clear();

	try {
		fileLatch.duplicate(fileLatch_);
		blocksInfo_ = fileLatch_.getManager()->latchBlocks(
				fileLatch_, startIndex, multiple);
	}
	catch (...) {
		try {
			clear();
		}
		catch (...) {
		}

		throw;
	}
}


LogManager::LogFileIndex::LogFileIndex() :
		config_(NULL),
		pgId_(UNDEF_PARTITIONGROUPID),
		cpId_(UNDEF_CHECKPOINT_ID),
		partitionInfoList_(NULL),
		buffer_(NULL),
		bufferValueId_(0),
		valueCount_(0),
		dirty_(false),
		invalid_(false) {
}

LogManager::LogFileIndex::~LogFileIndex() {
	try {
		clear();
	}
	catch (...) {
	}
}

void LogManager::LogFileIndex::initialize(const Config &config,
		PartitionGroupId pgId, CheckpointId cpId, uint64_t logBlockCount,
		const std::vector<PartitionInfo> &partitionInfoList) {
	clear();

	try {
		config_ = &config;
		pgId_ = pgId;
		cpId_ = cpId;
		partitionInfoList_ = &partitionInfoList;
		buffer_ = UTIL_NEW uint64_t[getBufferValueCount()];
		valueCount_ = (logBlockCount == 0 ?
				0 : HEADER_FIELD_MAX + logBlockCount * getPartitionCount());
	}
	catch (...) {
		try {
			clear();
		}
		catch (...) {
		}
		throw;
	}
}

void LogManager::LogFileIndex::clear() {
	try {
		updateFile();

		delete[] buffer_;

		config_ = NULL;
		pgId_ = UNDEF_PARTITIONGROUPID;
		cpId_ = UNDEF_CHECKPOINT_ID;
		partitionInfoList_ = NULL;
		buffer_ = NULL;
		bufferValueId_ = 0;
		valueCount_ = 0;
		dirty_ = false;

		file_.close();
	}
	catch (...) {
		try {
			clear();
		}
		catch (...) {
		}
		throw;
	}
}

bool LogManager::LogFileIndex::isEmpty() {
	return (config_ == NULL);
}

bool LogManager::LogFileIndex::tryFind(
		PartitionId pId, LogSequentialNumber startLsn,
		uint64_t &blockIndex, int32_t &direction, bool &empty) {
	if (!isEmpty() && !invalid_) {
		try {
			return find(pId, startLsn, blockIndex, direction, empty);
		}
		catch (...) {
			try {
				invalidate();
			}
			catch (...) {
			}
		}
	}

	direction = 0;
	empty = false;
	return false;
}

void LogManager::LogFileIndex::trySetLastEntry(uint64_t blockIndex,
		PartitionId activePId, LogSequentialNumber activeLsn) {
	if (!isEmpty() && !invalid_) {
		try {
			setLastEntry(blockIndex, activePId, activeLsn);
		}
		catch (...) {
			try {
				invalidate();
			}
			catch (...) {
			}
		}
	}
}

void LogManager::LogFileIndex::prepare(PartitionGroupManager &manager) {
	if (valueCount_ == 0) {
		invalid_ = true;
	}
	else {
		try {
			prepareBuffer(HEADER_FIELD_MAX);
		}
		catch (...) {
			invalid_ = true;
		}
	}

	if (invalid_) {
		try {
			rebuild(manager);
		}
		catch (...) {
			try {
				invalidate();
			}
			catch (...) {
			}
			throw;
		}
	}
}

void LogManager::LogFileIndex::rebuild(PartitionGroupManager &manager) {
	assert(!isEmpty());
	invalidate();

	LogFileLatch fileLatch;
	fileLatch.set(manager, cpId_, true);

	LogBlocksLatch blocksLatch;

	const uint64_t endOffset =
			manager.getAvailableEndOffset(fileLatch, blocksLatch);
	if (endOffset <= 0) {
		invalid_ = false;
		return;
	}

	blocksLatch.setForScan(fileLatch, 0);

	const uint32_t partitionCount = getPartitionCount();
	std::vector<LogSequentialNumber> lastLsnList;
	if (cpId_ <= 0) {
		lastLsnList.resize(partitionCount, 0);
	}
	else {
		lastLsnList.resize(partitionCount, UNDEF_LSN);
	}

	const PartitionId beginPId = config_->pgConfig_.getGroupBeginPartitionId(pgId_);
	const PartitionId endPId = config_->pgConfig_.getGroupEndPartitionId(pgId_);
	uint64_t offset = blocksLatch.get()->getFirstRecordOffest(0);
	LogRecord logRecord;
	uint64_t lastBlockIndex = 0;
	bool empty = true;
	for (;;) {
		const bool found = (offset > 0 ?
				manager.getLogHeader(blocksLatch, offset, logRecord, NULL, true) :
				false);

		const uint64_t blockIndex = (found ?
				getBlockIndex(*config_, offset - LogRecord::LOG_HEADER_SIZE) :
				getBlockIndex(*config_, endOffset));

		if (blockIndex != lastBlockIndex && !empty) {

			for (uint64_t index = lastBlockIndex; index < blockIndex; index++) {
				const uint64_t baseValueId =
						HEADER_FIELD_MAX + getPartitionCount() * index;

				for (uint32_t i = 0; i < partitionCount; i++) {
					setValue(baseValueId + i, lastLsnList[i]);
				}
			}
		}

		if (!found) {
			break;
		}

		if (!manager.skipLogBody(blocksLatch, offset, logRecord.bodyLen_)) {
			continue;
		}

		if (beginPId <= logRecord.partitionId_ &&
				logRecord.partitionId_ < endPId) {
			lastLsnList[logRecord.partitionId_ - beginPId] = logRecord.lsn_;
		}

		lastBlockIndex = blockIndex;
		empty = false;
	}
	updateFile();
	if (!file_.isClosed()) {
		file_.sync();
	}
	invalid_ = false;
}

bool LogManager::LogFileIndex::find(
		PartitionId pId, LogSequentialNumber startLsn, uint64_t &blockIndex,
		int32_t &direction, bool &empty) {
	blockIndex = 0;
	empty = false;
	direction = 0;

	if (isEmpty() || invalid_) {
		GS_THROW_USER_ERROR(GS_ERROR_LM_INTERNAL_ERROR_OTHER, "");
	}

	if (valueCount_ < HEADER_FIELD_MAX) {
		empty = true;
		return false;
	}

	const PartitionId beginPId =
			config_->pgConfig_.getGroupBeginPartitionId(pgId_);
	const PartitionId endPId =
			config_->pgConfig_.getGroupEndPartitionId(pgId_);
	if (!(beginPId <= pId && pId < endPId)) {
		GS_THROW_SYSTEM_ERROR(
				GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
	}

	const uint32_t partitionCount = getPartitionCount();
	const uint32_t baseValueId = HEADER_FIELD_MAX + (pId - beginPId);
	const uint64_t blockCount =
			(valueCount_ - HEADER_FIELD_MAX) / partitionCount;

	if (blockCount <= 0) {
		empty = true;
		return false;
	}
	else {
		const LogSequentialNumber blockLastLsn =
				getValue(baseValueId + (blockCount - 1) * partitionCount);

		if (blockLastLsn == UNDEF_LSN) {
			empty = true;
			return false;
		}

		if (startLsn > blockLastLsn) {
			direction = 1;
			return false;
		}
	}

	bool prevExisting = false;
	for (uint64_t i = 0; i < blockCount; i++) {
		const LogSequentialNumber blockLastLsn =
				getValue(baseValueId + i * partitionCount);
		if (blockLastLsn == UNDEF_LSN) {
			continue;
		}

		if (startLsn <= blockLastLsn) {
			if (prevExisting) {
				blockIndex = i;
				return true;
			}
			return false;
		}
		prevExisting = true;
	}

	return false;
}

void LogManager::LogFileIndex::setLastEntry(
		uint64_t blockIndex, PartitionId activePId, LogSequentialNumber activeLsn) {
	if (isEmpty() || invalid_) {
		GS_THROW_USER_ERROR(GS_ERROR_LM_INTERNAL_ERROR_OTHER, "");
	}

	const uint32_t partitionCount = getPartitionCount();
	const PartitionId beginPId =
			config_->pgConfig_.getGroupBeginPartitionId(pgId_);

	const uint64_t baseValueId =
			HEADER_FIELD_MAX + getPartitionCount() * blockIndex;

	for (uint32_t i = 0; i < partitionCount; i++) {
		const PartitionId pId = beginPId + i;
		const LogSequentialNumber lsn =
				(pId == activePId ? activeLsn : (*partitionInfoList_)[pId].lsn_);
		setValue(baseValueId + i, lsn);
	}
}

uint64_t LogManager::LogFileIndex::getValue(uint64_t valueId) {
	if (valueId >= valueCount_) {
		GS_THROW_USER_ERROR(
				GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
	}

	prepareBuffer(valueId);

	return buffer_[valueId - bufferValueId_];
}

void LogManager::LogFileIndex::setValue(uint64_t valueId, uint64_t value) {
	if (valueId > valueCount_ &&
			(valueCount_ == 0 && valueId != HEADER_FIELD_MAX)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
	}

	prepareBuffer(valueId);

	buffer_[valueId - bufferValueId_] = value;

	if (valueId == valueCount_) {
		valueCount_++;
	}
	dirty_ = true;
}

void LogManager::LogFileIndex::prepareBuffer(uint64_t valueId) {
	if (file_.isClosed()) {
		file_.open(getFileName().c_str(),
				util::FileFlag::TYPE_READ_WRITE |
				(valueCount_ == 0 ? util::FileFlag::TYPE_CREATE : 0));

		util::FileStatus status;
		file_.getStatus(&status);

		bufferValueId_ = std::numeric_limits<uint64_t>::max();

		const uint64_t fileSize = status.getSize();
		if (fileSize == 0 && valueCount_ == 0) {
			setValue(HEADER_FIELD_VERSION, VERSION_NUMBER);
			setValue(HEADER_FIELD_MAGIC, MAGIC_NUMBER);
			setValue(HEADER_FIELD_PARTITION_GROUP_ID, pgId_);
			setValue(HEADER_FIELD_CHECKPOINT_ID, cpId_);
			setValue(HEADER_FIELD_PARTITION_COUNT,
					config_->pgConfig_.getPartitionCount());
		}
		else {
			if (fileSize != getFileOffset(valueCount_)) {
				GS_THROW_USER_ERROR(
						GS_ERROR_LM_INVALID_LOG_INDEX, "");
			}

			if (getValue(HEADER_FIELD_VERSION) != VERSION_NUMBER ||
					getValue(HEADER_FIELD_MAGIC) != MAGIC_NUMBER ||
					getValue(HEADER_FIELD_PARTITION_GROUP_ID) != pgId_ ||
					getValue(HEADER_FIELD_CHECKPOINT_ID) != cpId_ ||
					getValue(HEADER_FIELD_PARTITION_COUNT) !=
							config_->pgConfig_.getPartitionCount()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_LM_INVALID_LOG_INDEX, "");
			}
		}
	}

	if (!(bufferValueId_ <= valueId &&
			valueId < bufferValueId_ + getBufferValueCount())) {
		updateFile();

		const uint64_t bufferValueId =
				valueId / getBufferValueCount() * getBufferValueCount();

		const uint64_t startOffset = getFileOffset(bufferValueId);
		const uint64_t endOffset = getFileOffset(
				std::min(bufferValueId + getBufferValueCount(), valueCount_));

		if (startOffset < endOffset) {
			const size_t size = static_cast<size_t>(endOffset - startOffset);
			file_.read(buffer_, size, startOffset);
		}

		bufferValueId_ = bufferValueId;
	}
}

void LogManager::LogFileIndex::updateFile() {
	if (!dirty_ || file_.isClosed()) {
		return;
	}

	const uint64_t startOffset = getFileOffset(bufferValueId_);
	const uint64_t endOffset = getFileOffset(
			std::min(bufferValueId_ + getBufferValueCount(), valueCount_));

	const size_t size = static_cast<size_t>(endOffset - startOffset);
	file_.write(buffer_, size, startOffset);
}

void LogManager::LogFileIndex::invalidate() {
	invalid_ = true;
	dirty_ = false;
	valueCount_ = 0;
	file_.close();
	if (util::FileSystem::exists(getFileName().c_str())) {
		util::FileSystem::removeFile(getFileName().c_str());
	}
}

uint64_t LogManager::LogFileIndex::getFileOffset(uint64_t valueId) {
	return sizeof(uint64_t) * valueId;
}

uint32_t LogManager::LogFileIndex::getBufferValueCount() const {
	return config_->getIndexBufferSize() / sizeof(uint64_t);
}

uint32_t LogManager::LogFileIndex::getPartitionCount() const {
	return config_->pgConfig_.getGroupPartitonCount(pgId_);
}

std::string LogManager::LogFileIndex::getFileName() const {
	util::NormalOStringStream oss;
	oss << maskDirectoryPath(config_->logDirectory_.c_str()) << "/" <<
			LOG_INDEX_FILE_BASE_NAME << pgId_ <<
			LOG_INDEX_FILE_SEPARATOR << cpId_ << LOG_INDEX_FILE_EXTENSION;

	return oss.str();
}


LogManager::PartitionGroupManager::PartitionGroupManager() :
		pgId_(UNDEF_PARTITIONGROUPID),
		firstCheckpointId_(UNDEF_CHECKPOINT_ID),
		lastCheckpointId_(UNDEF_CHECKPOINT_ID),
		completedCheckpointId_(UNDEF_CHECKPOINT_ID),
		tailOffset_(0),
		checkpointFileInfoOffset_(0),
		dirty_(false),
		flushRequired_(false),
		normalShutdownCompleted_(false),
		tailReadOnly_(false),
		config_(NULL),
		partitionInfoList_(NULL) {
}

LogManager::PartitionGroupManager::~PartitionGroupManager() {
	try {
		close();
	}
	catch (...) {
	}
}

void LogManager::PartitionGroupManager::open(
		PartitionGroupId pgId, const Config &config,
		CheckpointId firstCpId, CheckpointId endCpId,
		std::vector<PartitionInfo> &partitionInfoList,
		bool emptyFileAppendable, bool checkOnly,
		const char8_t *suffix) {
	assert(isClosed());

	pgId_ = pgId;
	config_ = &config;

	blocksPool_.reset(UTIL_NEW BlockPool(
			util::AllocatorInfo(ALLOCATOR_GROUP_LOG, "logBlock"),
			1 << (config.blockSizeBits_ + config.blockBufferCountBits_)));
	blocksPool_->setFreeElementLimit(config.pgConfig_.getPartitionGroupCount());

	partitionInfoList_ = &partitionInfoList;
	checkOnly_ = checkOnly;

	try {
		LogFileLatch fileLatch;
		LogBlocksLatch blocksLatch;
		scanExistingFiles(
				fileLatch, blocksLatch, firstCpId, endCpId, tailFileIndex_, suffix);

		if (lastCheckpointId_ == UNDEF_CHECKPOINT_ID) {
			const bool expectExisting = false;
			fileLatch.set(*this, firstCpId, expectExisting, suffix);

			firstCheckpointId_ = firstCpId;
			lastCheckpointId_ = firstCpId;

			if (config_->indexEnabled_) {
				tailFileIndex_.initialize(
						*config_, pgId_, lastCheckpointId_, 0, *partitionInfoList_);
			}
		}
		else {
			const bool expectExisting = true;
			fileLatch.set(*this, lastCheckpointId_, expectExisting, suffix);
		}

		if (emptyFileAppendable && fileLatch.get()->getFileSize() == 0) {
			tailReadOnly_ = false;
		}
		else {
			tailReadOnly_ = true;
		}

		if (tailOffset_ > 0) {
			tailBlocksLatch_.setForScan(
					fileLatch, getBlockIndex(*config_, tailOffset_ - 1));
		}
		else {
			tailBlocksLatch_.setForAppend(fileLatch);
		}

		lastLogFile_ = tailBlocksLatch_.getFileInfo()->getFileDirect();

	}
	catch (...) {
		try {
			close();
		}
		catch (...) {
		}
		throw;
	}
}

void LogManager::PartitionGroupManager::close() {
	if (isClosed()) {
		return;
	}

	try {
		tailFileIndex_.clear();
		tailBlocksLatch_.clear();

		if (!blocksInfoMap_.empty() || !fileInfoMap_.empty()) {
			for (BlocksInfoMap::iterator it = blocksInfoMap_.begin();
					it != blocksInfoMap_.end(); ++it) {
				if (it->second != NULL) {
					it->second->clear(*blocksPool_);
					delete it->second;
					it->second = NULL;
				}
			}
			blocksInfoMap_.clear();

			for (FileInfoMap::iterator it = fileInfoMap_.begin();
					it != fileInfoMap_.end(); ++it) {
				if (it->second != NULL) {
					it->second->close();
					delete it->second;
					it->second = NULL;
				}
			}
			fileInfoMap_.clear();

			GS_THROW_SYSTEM_ERROR(
					GS_ERROR_LM_INTERNAL_ERROR_OTHER, "");
		}

		pgId_ = UNDEF_PARTITIONGROUPID;
		firstCheckpointId_ = UNDEF_CHECKPOINT_ID;
		lastCheckpointId_ = UNDEF_CHECKPOINT_ID;
		completedCheckpointId_ = UNDEF_CHECKPOINT_ID;
		tailOffset_ = 0;
		checkpointFileInfoOffset_ = 0;
		dirty_ = false;
		flushRequired_ = false;
		normalShutdownCompleted_ = false;
		tailReadOnly_ = false;
		checkOnly_ = false;
		config_ = NULL;
		blocksPool_.reset();
		partitionInfoList_ = NULL;
		lastLogFile_ = NULL;
	}
	catch (...) {
		try {
			close();
		}
		catch (...) {
		}
		throw;
	}
}

bool LogManager::PartitionGroupManager::isClosed() const {
	return (config_ == NULL);
}

CheckpointId
LogManager::PartitionGroupManager::getFirstCheckpointId() const {
	assert(!isClosed());

	return firstCheckpointId_;
}

CheckpointId
LogManager::PartitionGroupManager::getLastCheckpointId() const {
	assert(!isClosed());

	return lastCheckpointId_;
}

CheckpointId
LogManager::PartitionGroupManager::getCompletedCheckpointId() const {
	assert(!isClosed());

	return completedCheckpointId_;
}

bool LogManager::PartitionGroupManager::findLog(
		LogBlocksLatch &blocksLatch, uint64_t &offset,
		PartitionId pId, LogSequentialNumber lsn) {
	assert(!isClosed());

	BlockPosition start;
	BlockPosition end;
	getAllBlockRange(start, end);


	if (end.second <= 0) {
		return false;
	}

	int32_t direction;
	const uint64_t tailStartIndex = tailBlocksLatch_.get()->getStartIndex();
	BlockPosition tail(end.first, tailStartIndex);
	if (findLogByScan(blocksLatch, offset, pId, lsn, tail, end, direction)) {
		return true;
	}

	end.second = tailStartIndex;
	if (config_->indexEnabled_) {
		return findLogByIndex(blocksLatch, offset, pId, lsn, start, end, direction);
	}
	else {
		return findLogByScan(blocksLatch, offset, pId, lsn, start, end, direction);
	}
}

bool LogManager::PartitionGroupManager::findLogByIndex(
		LogBlocksLatch &blocksLatch, uint64_t &offset,
		PartitionId pId, LogSequentialNumber lsn,
		const BlockPosition &start, const BlockPosition &end,
		int32_t &direction) {
	assert(!isClosed());

	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_FIND_LOG_INFO,
			"Finding log by index (pgId=" << pgId_ <<
			", pId=" << pId << ", lsn=" << lsn <<
			", range=<" << start.first << "," << start.second << ">-<" <<
			end.first << "," << end.second << ">)");

	direction = 0;

	if (isRangeEmpty(start, end)) {
		return false;
	}

	if (start.first != end.first) {
		LogFileLatch fileLatch;
		const BlockPosition middle = splitBlockRange(start, end, fileLatch, blocksLatch);
		BlockPosition middleEnd;
		if (middle.first == end.first) {
			middleEnd = end;
		}
		else {
			fileLatch.set(*this, middle.first, true);
			middleEnd = BlockPosition(middle.first,
					getBlockIndex(*config_, getAvailableEndOffset(fileLatch, blocksLatch)));
		}

		int32_t middleDirection;
		if (findLogByIndex(
				blocksLatch, offset, pId, lsn, middle, middleEnd, middleDirection)) {
			return true;
		}

		int32_t subDirection = 0;

		if (middleDirection <= 0 && findLogByIndex(
				blocksLatch, offset, pId, lsn, start, middle, subDirection)) {
			return true;
		}

		const BlockPosition middleNext = (middleEnd == start ?
				BlockPosition(start.first + 1, 0) : middleEnd);
		if (middleDirection >= 0 && findLogByIndex(
				blocksLatch, offset, pId, lsn, middleNext, end, subDirection)) {
			return true;
		}

		direction = (middleDirection < 0 && subDirection < 0 ? -1 :
				(middleDirection > 0 && subDirection > 0 ? 1 : 0));
		return false;
	}

	{
		LogFileLatch fileLatch;

		const CheckpointId cpId = start.first;
		LogFileIndex fileIndex;
		LogFileIndex *fileIndexPtr;
		if (cpId == lastCheckpointId_) {
			fileIndexPtr = &tailFileIndex_;
		}
		else {
			fileLatch.set(*this, cpId, true);

			fileIndexPtr = &fileIndex;
			fileIndexPtr->initialize(*config_, pgId_, cpId,
					fileLatch.get()->getTotalBlockCount(true), *partitionInfoList_);
		}

		uint64_t blockIndex;
		bool empty;

		if (fileIndexPtr->tryFind(pId, lsn, blockIndex, direction, empty)) {
			if (blockIndex < start.second) {
				direction = -1;
				return false;
			}
			else if (blockIndex >= end.second) {
				direction = 1;
				return false;
			}

			return findLogByScan(blocksLatch, offset, pId, lsn,
					BlockPosition(cpId, blockIndex),
					BlockPosition(cpId, blockIndex + 1),
					direction);
		}

		if (empty || direction != 0) {
			return false;
		}

		return findLogByScan(
				blocksLatch, offset, pId, lsn, start, end, direction);
	}
}

bool LogManager::PartitionGroupManager::findLogByScan(
		LogBlocksLatch &blocksLatch, uint64_t &offset,
		PartitionId pId, LogSequentialNumber lsn,
		const BlockPosition &start, const BlockPosition &end,
		int32_t &direction) {
	assert(!isClosed());

	GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_FIND_LOG_INFO,
			"Finding log by scan (pgId=" << pgId_ <<
			", pId=" << pId << ", lsn=" << lsn <<
			", range=<" << start.first << "," << start.second << ">-<" <<
			end.first << "," << end.second << ">)");

	direction = 0;

	if (isRangeEmpty(start, end)) {
		return false;
	}

	do {
		const BlockPosition scanEnd = getMinBlockScanEnd(start, end);
		if (scanEnd == end) {
			break;
		}

		LogFileLatch fileLatch;
		const BlockPosition middle = splitBlockRange(start, end, fileLatch, blocksLatch);
		if (middle == end) {
			break;
		}

		const BlockPosition middleEnd = getMinBlockScanEnd(middle, end);

		int32_t middleDirection;
		if (findLogByScan(
				blocksLatch, offset, pId, lsn, middle, middleEnd, middleDirection)) {
			return true;
		}

		int32_t subDirection = 0;

		if (middleDirection <= 0 && findLogByScan(
				blocksLatch, offset, pId, lsn, start, middle, subDirection)) {
			return true;
		}

		const BlockPosition middleNext = (middleEnd == start ?
				BlockPosition(start.first + 1, 0) : middleEnd);
		if (middleDirection >= 0 && findLogByScan(
				blocksLatch, offset, pId, lsn, middleNext, end, subDirection)) {
			return true;
		}

		direction = (middleDirection < 0 && subDirection < 0 ? -1 :
				(middleDirection > 0 && subDirection > 0 ? 1 : 0));
		return false;
	}
	while (false);

	do {
		LogFileLatch fileLatch;
		fileLatch.set(*this, start.first, true);

		uint32_t blockOffset = 0;
		uint64_t index = start.second;
		const uint64_t endIndex = std::min(
				getAvailableEndBlock(fileLatch, blocksLatch), end.second);
		for (; index < endIndex; index++) {
			blocksLatch.setForScan(fileLatch, index);
			blockOffset = blocksLatch.get()->getFirstRecordOffest(index);
			if (blockOffset > 0) {
				break;
			}
		}
		if (blockOffset <= 0) {
			break;
		}

		LogSequentialNumber lastLsn = UNDEF_LSN;
		offset = getFileOffset(*config_, index, blockOffset);
		for (;;) {
			LogRecord logRecord;
			if (!getLogHeader(blocksLatch, offset, logRecord, NULL, true, &end)) {
				if (lastLsn != UNDEF_LSN) {
					assert(lsn >= lastLsn);
					direction = 1;
				}
				break;
			}

			if (pId != UNDEF_PARTITIONID && lsn != UNDEF_LSN) {
				if (logRecord.partitionId_ == pId) {
					if (lsn < logRecord.lsn_) {
						direction = -1;
						break;
					}
					lastLsn = logRecord.lsn_;
				}
			}

			if ((lsn == UNDEF_LSN || logRecord.lsn_ == lsn) &&
					(pId == UNDEF_PARTITIONID || logRecord.partitionId_ == pId)) {

				if (!isLsnAssignable(static_cast<uint8_t>(logRecord.type_))) {
					direction = -1;
					break;
				}

				offset -= LogRecord::LOG_HEADER_SIZE;

				return true;
			}

			skipLogBody(blocksLatch, offset, logRecord.bodyLen_);
		}
	}
	while (false);

	blocksLatch.clear();
	offset = 0;
	return false;
}

bool LogManager::PartitionGroupManager::findCheckpointStartLog(
		LogBlocksLatch &blocksLatch, uint64_t &offset, CheckpointId cpId) {
	assert(!isClosed());

	LogFileLatch fileLatch;
	fileLatch.set(*this, cpId, true);

	const uint32_t startIndex = 0;
	blocksLatch.setForScan(fileLatch, startIndex);
	const uint64_t orgOffset = getFileOffset(
			*config_, 0, blocksLatch.get()->getFirstRecordOffest(0));
	offset = orgOffset;

	LogRecord logRecord;
	if (offset > 0 &&
			getLogHeader(blocksLatch, offset, logRecord, NULL, true)) {
		offset = orgOffset;
		return true;
	}

	blocksLatch.clear();
	offset = 0;
	return false;
}

bool LogManager::PartitionGroupManager::findCheckpointEndLog(
		LogBlocksLatch &blocksLatch, uint64_t &offset, CheckpointId cpId) {
	assert(!isClosed());

	LogFileLatch fileLatch;
	fileLatch.set(*this, cpId, true);

	do {
		uint64_t orgOffset;
		if (cpId == lastCheckpointId_) {
			orgOffset = checkpointFileInfoOffset_;
		}
		else {
			const uint64_t endOffset = getAvailableEndOffset(fileLatch, blocksLatch);
			if (endOffset <= 0) {
				break;
			}

			const uint64_t index = getBlockIndex(*config_, endOffset  - 1);
			blocksLatch.setForScan(fileLatch, index);
			orgOffset = blocksLatch.get()->getCheckpointFileInfoOffset(index);
		}

		offset = orgOffset;
		if (offset <= 0) {
			break;
		}

		blocksLatch.setForScan(fileLatch, getBlockIndex(*config_, offset));

		LogRecord logRecord;
		if (!getLogHeader(blocksLatch, offset, logRecord, NULL, true)) {
			break;
		}

		if (logRecord.type_ != LOG_TYPE_CHECKPOINT_END) {
			break;
		}

		LogBlocksLatch blocksLatchCopy;
		blocksLatch.duplicate(blocksLatchCopy);
		if (!skipLogBody(blocksLatchCopy, offset, logRecord.bodyLen_)) {
			break;
		}

		offset = orgOffset;
		return true;
	}
	while (false);

	blocksLatch.clear();
	offset = 0;
	return false;
}

bool LogManager::PartitionGroupManager::getLogHeader(
		LogBlocksLatch &blocksLatch, uint64_t &offset, LogRecord &logRecord,
		util::XArray<uint8_t> *data, bool sameFileOnly,
		const BlockPosition *endPosition) {
	assert(!isClosed());

	if (!prepareBlockForScan(
			blocksLatch, offset, sameFileOnly, endPosition)) {
		return false;
	}

	for (;;) {
		const uint32_t lastBlockRemaining = getBlockRemaining(*config_, offset);
		if (lastBlockRemaining < LogRecord::LOG_HEADER_SIZE) {
			offset += lastBlockRemaining;
			if (!prepareBlockForScan(
					blocksLatch, offset, sameFileOnly, endPosition)) {
				return false;
			}
		}

		size_t orgDataSize;
		uint8_t *recordHeader;
		uint8_t recordHeaderStorage[LogRecord::LOG_HEADER_SIZE];
		if (data == NULL) {
			orgDataSize = 0;
			recordHeader = recordHeaderStorage;
		}
		else {
			orgDataSize = data->size();
			data->resize(orgDataSize + LogRecord::LOG_HEADER_SIZE);
			recordHeader = data->data() + orgDataSize;
		}

		const uint32_t copySize = blocksLatch.get()->getBodyPart(
				recordHeader,
				getBlockIndex(*config_, offset),
				getBlockOffset(*config_, offset),
				LogRecord::LOG_HEADER_SIZE);
		assert(copySize == LogRecord::LOG_HEADER_SIZE);
		(void) copySize;

		const uint8_t *recordHeaderPtr = recordHeader;
		const uint32_t parsedSize = parseLogHeader(recordHeaderPtr, logRecord);
		assert(parsedSize == LogRecord::LOG_HEADER_SIZE);
		(void) parsedSize;

		if (logRecord.type_ != LOG_TYPE_UNDEF) {
			logRecord.fileVersion_ = blocksLatch.get()->getVersion();

			offset += LogRecord::LOG_HEADER_SIZE;
			return true;
		}

		if (data != NULL) {
			data->resize(orgDataSize);
		}

		const CheckpointId nextCpId =
				blocksLatch.getFileInfo()->getCheckpointId() + 1;
		uint64_t nextOffset = std::numeric_limits<uint64_t>::max();

		if (!prepareBlockForScan(
				blocksLatch, nextOffset, sameFileOnly, endPosition)) {
			return false;
		}

		if (blocksLatch.getFileInfo()->getCheckpointId() != nextCpId) {
			return false;
		}
		offset = nextOffset;
	}
}

bool LogManager::PartitionGroupManager::getLogBody(
		LogBlocksLatch &blocksLatch, uint64_t &offset,
		util::XArray<uint8_t> &data, uint32_t bodyLen) {
	assert(!isClosed());

	uint32_t remainingLen = bodyLen;

	const size_t orgDataSize = data.size();
	data.resize(data.size() + remainingLen);
	uint8_t *nextRecordPart = data.data() + orgDataSize;

	while (remainingLen > 0) {
		if (!prepareBlockForScan(blocksLatch, offset, true)) {
			offset = getFileOffset(*config_, getBlockIndex(*config_, offset));
			data.resize(orgDataSize);
			return false;
		}

		const uint32_t copySize = blocksLatch.get()->getBodyPart(
				nextRecordPart,
				getBlockIndex(*config_, offset),
				getBlockOffset(*config_, offset),
				remainingLen);
		assert(copySize > 0);
		assert(copySize <= remainingLen);

		nextRecordPart += copySize;
		offset += copySize;
		remainingLen -= copySize;
	}

	return true;
}

bool LogManager::PartitionGroupManager::skipLogBody(
		LogBlocksLatch &blocksLatch, uint64_t &offset, uint32_t bodyLen) {
	assert(!isClosed());

	uint32_t remainingLen = bodyLen;

	while (remainingLen > 0) {
		if (!prepareBlockForScan(blocksLatch, offset, true)) {
			offset = getFileOffset(*config_, getBlockIndex(*config_, offset));
			return false;
		}

		const uint32_t skipSize = static_cast<uint32_t>(
				std::min(getBlockRemaining(*config_, offset), remainingLen));
		assert(skipSize > 0);

		offset += skipSize;
		remainingLen -= skipSize;
	}

	return true;
}

void LogManager::PartitionGroupManager::putLog(
		const uint8_t *data, uint32_t size) {
	assert(!isClosed());

	LogRecord logRecord;
	const uint8_t *bodyAddr = data;
	LogManager::parseLogHeader(bodyAddr, logRecord);

	if (tailReadOnly_) {
		GS_THROW_SYSTEM_ERROR(
				GS_ERROR_LM_INTERNAL_ERROR_OTHER,
				"Read only log file");
	}



	prepareBlockForAppend(UNDEF_PARTITIONID, UNDEF_LSN);

	const uint32_t lastBlockRemaining = getBlockRemaining(*config_, tailOffset_);
	if (lastBlockRemaining < LogRecord::LOG_HEADER_SIZE) {
		uint8_t padding[LogRecord::LOG_HEADER_SIZE];
		memset(padding, LOG_RECORD_PADDING_ELEMENT, sizeof(padding));

		tailBlocksLatch_.get()->putBodyPart(
				padding,
				getBlockIndex(*config_, tailOffset_),
				getBlockOffset(*config_, tailOffset_),
				lastBlockRemaining);

		tailOffset_ += lastBlockRemaining;
		dirty_ = true;

		prepareBlockForAppend(UNDEF_PARTITIONID, UNDEF_LSN);
	}

	if (logRecord.type_ == LOG_TYPE_CHECKPOINT_END) {
		checkpointFileInfoOffset_ = tailOffset_;
		completedCheckpointId_ = lastCheckpointId_;
	}
	else if (logRecord.type_ == LOG_TYPE_SHUTDOWN) {
		normalShutdownCompleted_ = true;
	}

	if (tailBlocksLatch_.get()->getFirstRecordOffest(
			getBlockIndex(*config_, tailOffset_)) == 0) {
		tailBlocksLatch_.get()->setFirstRecordOffest(
				getBlockIndex(*config_, tailOffset_),
				getBlockOffset(*config_, tailOffset_));
	}


	const uint8_t *nextRecordPart = data;
	for (uint32_t remainingLen = size;;) {
		const uint32_t copySize = tailBlocksLatch_.get()->putBodyPart(
				nextRecordPart,
				getBlockIndex(*config_, tailOffset_),
				getBlockOffset(*config_, tailOffset_),
				remainingLen);
		assert(copySize <= remainingLen);

		nextRecordPart += copySize;
		tailOffset_ += copySize;
		remainingLen -= copySize;
		dirty_ = (copySize > 0);

		if (remainingLen <= 0) {
			break;
		}

		prepareBlockForAppend(logRecord.partitionId_, logRecord.lsn_);
	}
}

void LogManager::PartitionGroupManager::writeBuffer() {
	assert(!isClosed());

	updateTailBlock(UNDEF_PARTITIONID, UNDEF_LSN);
}

void LogManager::PartitionGroupManager::flushFile() {
	assert(!isClosed());

	util::NamedFile *file = lastLogFile_;

	if (file != NULL) {

		bool requiredState = true;
		const bool nextState = false;

		if (flushRequired_.compareExchange(requiredState, nextState)) {
			LogFileInfo::flush(*file, *config_, false);
		}
	}
}

uint64_t LogManager::PartitionGroupManager::copyLogFile(
		PartitionGroupId pgId, const char8_t *dirPath) {

	assert(!isClosed());

	if (!util::FileSystem::isDirectory(dirPath)) {
		GS_THROW_USER_ERROR(GS_ERROR_LM_COPY_LOG_FILE_FAILED,
				"Path is not an existing directory (path=" << dirPath << ")");
	}

	const std::string targetPath =
			LogFileInfo::createLogFilePath(dirPath, pgId, lastCheckpointId_);

	UTIL_UNIQUE_PTR<util::NamedFile> destFile(UTIL_NEW util::NamedFile());
	destFile->open(targetPath.c_str(),
			util::FileFlag::TYPE_READ_WRITE | util::FileFlag::TYPE_CREATE);

	try {
		util::File *srcFile = lastLogFile_;

		util::FileStatus status;
		srcFile->getStatus(&status);
		const uint64_t totalBlockCount = LogManager::getBlockIndex(
				*config_, static_cast<uint64_t>(status.getSize()));

		const uint32_t bufferBlockCount =
				static_cast<uint32_t>(1) << config_->blockBufferCountBits_;
		util::NormalXArray<uint8_t> buffer;
		buffer.resize(config_->getBlockSize() * bufferBlockCount);

		for (uint64_t i = 0; i < totalBlockCount;) {
			const uint32_t blockCount = static_cast<uint32_t>(std::min(
					bufferBlockCount,
					static_cast<uint32_t>(totalBlockCount - i)));
			const uint32_t copySize = config_->getBlockSize() * blockCount;

			const uint64_t copyOffset = config_->getBlockSize() * i;
			srcFile->read(buffer.data(), copySize, copyOffset);
			destFile->write(buffer.data(), copySize, copyOffset);

			i += blockCount;
		}

		destFile->close();
		return totalBlockCount * config_->getBlockSize();
	}
	catch (...) {
		try {
			util::FileSystem::removeFile(targetPath.c_str());
		}
		catch (...) {
		}
	}
	return 0;
}

void LogManager::PartitionGroupManager::prepareCheckpoint() {
	assert(!isClosed());

	writeBuffer();
	flushFile();

	tailFileIndex_.clear();

	tailOffset_ = 0;
	checkpointFileInfoOffset_ = 0;
	lastCheckpointId_++;
	tailReadOnly_ = false;

	LogFileLatch fileLatch;
	fileLatch.set(*this, lastCheckpointId_, false);
	tailBlocksLatch_.setForAppend(fileLatch);
	lastLogFile_ = tailBlocksLatch_.getFileInfo()->getFileDirect();

	if (config_->indexEnabled_) {
		try {
			tailFileIndex_.initialize(
					*config_, pgId_, lastCheckpointId_, 0, *partitionInfoList_);
			tailFileIndex_.prepare(*this);
		}
		catch (...) {
		}
	}
}

uint64_t LogManager::PartitionGroupManager::getAvailableEndOffset(
		LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch) {
	assert(!isClosed());
	assert(fileLatch.get() != NULL);

	const CheckpointId cpId = fileLatch.get()->getCheckpointId();

	if (!fileLatch.get()->isAvailableFileSizeUpdated()) {
		const uint64_t totalBlockCount = fileLatch.get()->getTotalBlockCount(false);
		if (totalBlockCount > 0) {
			blocksLatch.setForScan(fileLatch, totalBlockCount - 1);
		}
		else {
			fileLatch.get()->setAvailableFileSize(0);
		}

		if (!fileLatch.get()->isAvailableFileSizeUpdated()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_LM_INTERNAL_ERROR_OTHER, "");
		}
	}

	if (cpId == lastCheckpointId_) {
		return tailOffset_;
	}
	else {
		return getFileOffset(
				*config_, fileLatch.get()->getTotalBlockCount(true));
	}
}

uint64_t LogManager::PartitionGroupManager::getAvailableEndBlock(
		LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch) {
	return getBlockIndex(
			*config_, getAvailableEndOffset(fileLatch, blocksLatch) +
					config_->getBlockSize() - 1);
}

LogManager::LogBlocksInfo* LogManager::PartitionGroupManager::latchBlocks(
		LogFileLatch &fileLatch, uint64_t startIndex, bool multiple) {
	assert(!isClosed());

	const uint64_t bufferIndex =
			(startIndex >> config_->blockBufferCountBits_ <<
			config_->blockBufferCountBits_);
	const BlockPosition position(
			fileLatch.get()->getCheckpointId(), bufferIndex);

	LogBlocksInfo *blocksInfo;
	BlocksInfoMap::iterator it = blocksInfoMap_.find(position);

	if (it == blocksInfoMap_.end()) {
		UTIL_UNIQUE_PTR<LogBlocksInfo> blocksInfoPtr(UTIL_NEW LogBlocksInfo());
		const uint64_t targetIndex = (multiple ? bufferIndex : startIndex);
		blocksInfoPtr->initialize(
				*config_, *blocksPool_, targetIndex, multiple);

		if (config_->blockBufferCountBits_ == 0 ||
				blocksInfoPtr->getBlockCount() > 1) {
			blocksInfoMap_.insert(
					std::make_pair(position, blocksInfoPtr.get()));
		}

		blocksInfo = blocksInfoPtr.release();
	}
	else {
		blocksInfo = it->second;
	}

	blocksInfo->getReferenceCounter().increment();

	return blocksInfo;
}

void LogManager::PartitionGroupManager::unlatchBlocks(
		LogFileLatch &fileLatch, LogBlocksInfo *&blocksInfo) {
	assert(!isClosed());

	const BlockPosition position(
			fileLatch.get()->getCheckpointId(), blocksInfo->getStartIndex());

	if (config_->blockBufferCountBits_ == 0 ||
			blocksInfo->getBlockCount() > 1) {
		BlocksInfoMap::iterator it = blocksInfoMap_.find(position);
		if (it == blocksInfoMap_.end() || it->second != blocksInfo) {
			assert(false);
			return;
		}

		if (blocksInfo->getReferenceCounter().decrement() == 0) {
			blocksInfoMap_.erase(it);
			try {
				blocksInfo->clear(*blocksPool_);
			}
			catch (...) {
			}
			delete blocksInfo;
		}

		blocksInfo = NULL;
	}
	else {
		assert(blocksInfo->getReferenceCounter().decrement() == 0);
		try {
			blocksInfo->clear(*blocksPool_);
		}
		catch (...) {
		}
		delete blocksInfo;
		blocksInfo = NULL;
	}
}

LogManager::LogFileInfo* LogManager::PartitionGroupManager::latchFile(
		CheckpointId cpId, bool expectExisting, const char8_t *suffix) {
	assert(!isClosed());

	LogFileInfo *fileInfo;

	FileInfoMap::iterator it = fileInfoMap_.find(cpId);
	if (it == fileInfoMap_.end()) {
		UTIL_UNIQUE_PTR<LogFileInfo> fileInfoPtr(UTIL_NEW LogFileInfo());
		fileInfoPtr->open(*config_, pgId_, cpId, expectExisting, checkOnly_, suffix);
		fileInfoMap_.insert(std::make_pair(cpId, fileInfoPtr.get()));

		fileInfo = fileInfoPtr.release();
	}
	else {
		fileInfo = it->second;
	}

	fileInfo->getReferenceCounter().increment();

	return fileInfo;
}

void LogManager::PartitionGroupManager::unlatchFile(LogFileInfo *&fileInfo) {
	assert(!isClosed());

	FileInfoMap::iterator it = fileInfoMap_.find(fileInfo->getCheckpointId());
	if (it == fileInfoMap_.end() || it->second != fileInfo) {
		assert(false);
		return;
	}

	if (fileInfo->getReferenceCounter().decrement() == 0) {
		fileInfoMap_.erase(it);
		try {
			fileInfo->close();
		}
		catch (...) {
		}
		delete fileInfo;
	}

	fileInfo = NULL;
}

void LogManager::PartitionGroupManager::getAllBlockRange(
		BlockPosition &start, BlockPosition &end) const {
	start.first = firstCheckpointId_;
	start.second = 0;

	end.first = lastCheckpointId_;
	end.second =
			getBlockIndex(*config_, tailOffset_ + config_->getBlockSize() - 1);
}

LogManager::BlockPosition LogManager::PartitionGroupManager::splitBlockRange(
		const BlockPosition &start, const BlockPosition &end,
		LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch) {

	if (start.first < end.first) {
		const CheckpointId cpId = start.first + (end.first - start.first) / 2;

		uint64_t blockIndex;
		if (cpId == start.first) {
			fileLatch.set(*this, cpId, true);
			blockIndex = getAvailableEndBlock(fileLatch, blocksLatch);
		}
		else {
			blockIndex = 0;
		}

		return BlockPosition(cpId, blockIndex);
	}
	else if (start.first == end.first && start.second < end.second) {
		const uint64_t startBlockCount =
				((end.second - start.second) / 2 +
					config_->getBlockBufferCount() - 1) >>
				config_->blockBufferCountBits_ <<
				config_->blockBufferCountBits_;

		return BlockPosition(start.first,
				std::min(start.second + startBlockCount, end.second));
	}

	return end;
}

LogManager::BlockPosition
LogManager::PartitionGroupManager::getMinBlockScanEnd(
		const BlockPosition &start, const BlockPosition &end) {

	if (start.first < end.first) {
		return BlockPosition(start.first,
				start.second + config_->getBlockBufferCount());
	}
	else if (start.first == end.first) {
		return BlockPosition(start.first,
				std::min(start.second + config_->getBlockBufferCount(),
				end.second));
	}

	return end;
}

bool LogManager::PartitionGroupManager::isRangeEmpty(
		const BlockPosition &start, const BlockPosition &end) {
	if (start.first > end.first ||
			(start.first == end.first && start.second >= end.second)) {
		return true;
	}

	return false;
}

void LogManager::PartitionGroupManager::updateTailBlock(
		PartitionId activePId, LogSequentialNumber activeLsn) {
	if (dirty_) {
		assert(tailOffset_ > 0);

		const uint64_t tailIndex = getBlockIndex(*config_, tailOffset_ - 1);
		LogBlocksInfo *blocksInfo = tailBlocksLatch_.get();

		blocksInfo->setLastCheckpointId(
				tailIndex, completedCheckpointId_);
		blocksInfo->setCheckpointFileInfoOffset(
				tailIndex, checkpointFileInfoOffset_);
		blocksInfo->setNormalShutdownCompleted(
				tailIndex, normalShutdownCompleted_);

		blocksInfo->copyToFile(*tailBlocksLatch_.getFileInfo(), tailIndex);

		dirty_ = false;
		flushRequired_ = true;

		tailFileIndex_.trySetLastEntry(tailIndex, activePId, activeLsn);
	}
}

bool LogManager::PartitionGroupManager::prepareBlockForScan(
		LogBlocksLatch &blocksLatch, uint64_t &offset,
		bool sameFileOnly, const BlockPosition *endPosition) {
	for (;;) {
		const uint64_t orgOffset = offset;
		if (blocksLatch.get()->prepareNextBodyOffset(offset) &&
				offset == orgOffset) {
			return true;
		}

		const CheckpointId currentCpId =
				blocksLatch.getFileInfo()->getCheckpointId();

		LogFileLatch fileLatch;
		fileLatch.set(*this, currentCpId, true);

		CheckpointId nextCpId;
		uint64_t nextIndex;
		const uint64_t currentEndOffset =
				getAvailableEndOffset(fileLatch, blocksLatch);
		if (offset < currentEndOffset) {
			nextCpId = currentCpId;
			nextIndex = getBlockIndex(*config_, offset);
		}
		else if (!sameFileOnly && currentCpId < lastCheckpointId_) {
			nextCpId = currentCpId + 1;
			nextIndex = 0;
			offset = 0;
		}
		else {
			offset = orgOffset;
			return false;
		}

		if (endPosition != NULL) {
			if (nextCpId > endPosition->first) {
				return false;
			}
			else if (nextCpId == endPosition->first &&
					nextIndex >= endPosition->second) {
				return false;
			}
		}

		const bool expectExisting = true;
		fileLatch.set(*this, nextCpId, expectExisting);

		blocksLatch.setForScan(fileLatch, nextIndex);

	}
}

void LogManager::PartitionGroupManager::prepareBlockForAppend(
		PartitionId activePId, LogSequentialNumber activeLsn) {
	uint64_t nextOffset = tailOffset_;

	const bool foundInBuffer =
			tailBlocksLatch_.get()->prepareNextBodyOffset(nextOffset);

	if (foundInBuffer && nextOffset == tailOffset_) {
		return;
	}

	updateTailBlock(activePId, activeLsn);

	tailOffset_ = nextOffset;

	if (!foundInBuffer) {
		LogFileLatch fileLatch;
		fileLatch.set(*this, lastCheckpointId_, true);

		tailBlocksLatch_.setForAppend(fileLatch);

		if (!tailBlocksLatch_.get()->prepareNextBodyOffset(tailOffset_)) {
			assert(false);
			GS_THROW_SYSTEM_ERROR(
					GS_ERROR_LM_INTERNAL_ERROR_OTHER, "");
		}
	}

	tailBlocksLatch_.get()->initializeBlockHeader(
			getBlockIndex(*config_, tailOffset_));
}

void LogManager::PartitionGroupManager::scanExistingFiles(
		LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch,
		CheckpointId firstCpId, CheckpointId endCpId,
		LogFileIndex &fileIndex, const char8_t *suffix) {
	assert (firstCpId <= endCpId);

	CheckpointId lastCompletedCpId = UNDEF_CHECKPOINT_ID;
	uint64_t cpFileInfoOffset = 0;
	uint64_t tailOffset = 0;
	for (CheckpointId cpId = firstCpId;; cpId++) {
		if (cpId >= endCpId) {
			break;
		}

		const bool expectExisting = true;
		fileLatch.set(*this, cpId, expectExisting, suffix);

		if (config_->primaryCheckpointAssumed_ && cpId == 0) {
			if (fileLatch.get()->getFileSize() != 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_LM_OPEN_LOG_FILE_FAILED,
						"First log file must be empty (pgId=" << pgId_ <<
						", cpId=" << cpId << ")");
			}
		}

		const uint64_t availabeEndOffset =
				getAvailableEndOffset(fileLatch, blocksLatch);

		const uint64_t totalBlockCount =
				getBlockIndex(*config_, availabeEndOffset);

		if (!checkOnly_ && config_->indexEnabled_) {
			try {
				fileIndex.initialize(*config_,
						pgId_, cpId, totalBlockCount, *partitionInfoList_);
				fileIndex.prepare(*this);
			}
			catch (...) {
			}
		}

		if (totalBlockCount > 0) {
			const uint64_t lastBlockIndex = totalBlockCount - 1;

			blocksLatch.setForScan(fileLatch, lastBlockIndex);

			const bool tailReached = (cpId + 1 == endCpId);

			const uint64_t blockCpFileInfoOffset =
					blocksLatch.get()->getCheckpointFileInfoOffset(lastBlockIndex);

			if (blockCpFileInfoOffset > 0 &&
					blocksLatch.get()->getLastCheckpointId(lastBlockIndex) == cpId) {

				LogRecord record;
				uint64_t offset = blockCpFileInfoOffset;
				do {
					blocksLatch.setForScan(fileLatch, getBlockIndex(*config_, offset));
					if (!getLogHeader(
							blocksLatch, offset, record, NULL, true)) {
						break;
					}

					if (record.type_ != LOG_TYPE_CHECKPOINT_END) {
						break;
					}

					if (!skipLogBody(blocksLatch, offset, record.bodyLen_)) {
						break;
					}

					lastCompletedCpId = cpId;
					if (tailReached) {
						cpFileInfoOffset = blockCpFileInfoOffset;
					}
				}
				while (false);
			}

			if (tailReached) {
				tailOffset = availabeEndOffset;
			}

			GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
					"Scanning log file (pgId=" << pgId_ << ", cpId=" << cpId <<
					", fileSize=" << fileLatch.get()->getFileSize() <<
					", availableFileSize=" << availabeEndOffset <<
					", completedCpId=" << lastCompletedCpId <<
					", cpFileInfoOffset=" << blockCpFileInfoOffset << ")");
		}
		else {
			GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_PUT_LOG_INFO,
					"Scanning log file (pgId=" << pgId_ << ", cpId=" << cpId <<
					", fileSize=" << fileLatch.get()->getFileSize() <<
					", availableFileSize=" << availabeEndOffset << ")");
		}
	}

	if (firstCpId < endCpId) {
		firstCheckpointId_ = firstCpId;
		lastCheckpointId_ = endCpId - 1;
		completedCheckpointId_ = lastCompletedCpId;

		checkpointFileInfoOffset_ = cpFileInfoOffset;
		tailOffset_ = tailOffset;
	}
}

void LogManager::PartitionGroupManager::cleanupLogFiles() {
	assert(firstCheckpointId_ != UNDEF_CHECKPOINT_ID);
	assert(lastCheckpointId_ != UNDEF_CHECKPOINT_ID);

	if (completedCheckpointId_ == UNDEF_CHECKPOINT_ID ||
			config_->persistencyMode_ == PERSISTENCY_KEEP_ALL_LOG) {
		return;
	}


	try {
		CheckpointId requiredCpId;
		if (config_->lsnBaseFileRetention_) {
			requiredCpId = firstCheckpointId_;

			LogBlocksLatch blocksLatch;

			const PartitionId beginPId =
					config_->pgConfig_.getGroupBeginPartitionId(pgId_);
			const PartitionId endPId =
					config_->pgConfig_.getGroupEndPartitionId(pgId_);

			for (PartitionId pId = beginPId; pId < endPId; pId++) {

				const LogSequentialNumber startLsn =
						(*partitionInfoList_)[pId].availableStartLsn_;
				if (startLsn == 0) {
					continue;
				}

				uint64_t offset;
				if (findLog(blocksLatch, offset, pId, startLsn)) {
					const CheckpointId cpId =
							blocksLatch.getFileInfo()->getCheckpointId();
					requiredCpId = std::max(requiredCpId, cpId);
				}
			}

			requiredCpId = std::min(requiredCpId, completedCheckpointId_);
		}
		else {
			requiredCpId = completedCheckpointId_;
		}

		requiredCpId = std::min(requiredCpId,
								lastCheckpointId_ -
								std::min(config_->retainedFileCount_, lastCheckpointId_));


		while (firstCheckpointId_ < requiredCpId) {
			if (fileInfoMap_.find(firstCheckpointId_) != fileInfoMap_.end()) {
				break;
			}

			const std::string filePath = LogFileInfo::createLogFilePath(
					config_->logDirectory_.c_str(), pgId_, firstCheckpointId_);

			try {
				util::FileSystem::removeFile(filePath.c_str());
			}
			catch (std::exception &e) {
				GS_RETHROW_USER_ERROR(
					e, "Remove failed (path=" << filePath <<
					"reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}

			GS_TRACE_INFO(LOG_MANAGER, GS_TRACE_LM_CLEANUP_LOG_FILES,
					"File successfully removed (path=" << filePath << ")");

			firstCheckpointId_++;
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(LOG_MANAGER, e, "Ignore exception. (message="
						   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
    @brief Updates an available start LSN.
*/
void LogManager::updateAvailableStartLsn(PartitionTable *pt, PartitionGroupId pgId, 
										 util::XArray<uint8_t> &binaryLogRecords, CheckpointId cpId) 
{
	const PartitionId startPId =  config_.pgConfig_.getGroupBeginPartitionId(pgId);
	const PartitionId endPId =  config_.pgConfig_.getGroupEndPartitionId(pgId);
	CheckpointId lastCpId = getLastCheckpointId(pgId);

	for (PartitionId pId = startPId; pId < endPId; pId++) {
		std::map<CheckpointId, LogSequentialNumber> &startLsnMap =  partitionInfoList_[pId].startLsnMap_;
		std::map<CheckpointId, LogSequentialNumber>::iterator itr = startLsnMap.begin();
		LogSequentialNumber startLsn = UINT64_MAX;

		if(startLsnMap.size() == 0) {
			try {
				LogCursor cursor;
				if (findCheckpointStartLog(cursor, pgId, cpId)) {
					LogRecord logRecord;
					if (cursor.nextLog(logRecord, binaryLogRecords,
									   pId, LogManager::LOG_TYPE_CHECKPOINT_START)) {
						GS_TRACE_INFO(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
							"pgId:" << pgId << ", pId:" << pId
							<< ", cpId:" << cpId << ", lsn:" << logRecord.lsn_);
						pt->setStartLSN(pId, logRecord.lsn_);
					}
					else {
						GS_TRACE_INFO(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
							"pgId:" << pgId << ", pId:"
							<< pId << ", cpId:" << cpId << ", not found1");
					}
				}
				else {
					GS_TRACE_INFO(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
						"pgId:" << pgId << ", pId:" << pId
						<< ", cpId:" << cpId << ", not found2");
				}
			} catch(std::exception &) {
				GS_TRACE_INFO(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
					"pgId:" << pgId << ", pId:" << pId
					<< ", cpId:" << cpId << ", not found2");
			}
		}
		else {
			while (itr != startLsnMap.end()) {
				CheckpointId currentCPId = (*itr).first;
				LogSequentialNumber currentLsn = (*itr).second;
				if(currentCPId < cpId) {
					GS_TRACE_WARNING(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
						"erase start lsn, pId:" << pId << ", currentCpId:" << currentCPId 
						<< ", cpId:" << cpId << ", lsn:" << currentLsn);
					startLsnMap.erase(itr++);
				}
				else {
					LogSequentialNumber currentLsn = (*itr).second;
					if(startLsn > currentLsn && currentLsn <= getLSN(pId)) {
						pt->setStartLSN(pId, currentLsn);
						GS_TRACE_WARNING(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
							"update start lsn, pId:" << pId << ", currentCpId:" << currentCPId 
							<< ", cpId:" << cpId << ", lsn:" << currentLsn);
						startLsn = currentLsn;
					}
					itr++;
				}
			}
		}
		setAvailableStartLSN(pId, lastCpId);
		GS_TRACE_WARNING(CLUSTER_SERVICE, GS_TRACE_CS_UPDATE_START_LSN,
						 "update new cpId, pId:" << pId << ", cpId:" << lastCpId 
						 << ", lsn:" << pt->getLSN(pId));
	}
}


/*!
    @brief Checks if fileName is of Log.
*/
bool LogManager::checkFileName(const std::string &fileName,
							  PartitionGroupId &pgId, CheckpointId &cpId) {
	pgId = UNDEF_PARTITIONGROUPID;
	cpId = UNDEF_CHECKPOINT_ID;

	std::string::size_type pos = fileName.find(LOG_FILE_BASE_NAME);
	if (0 != pos) {
		return false;
	}
	pos = fileName.rfind(LOG_FILE_EXTENSION);
	if (std::string::npos == pos) {
		return false;
	} else if ((pos + strlen(LOG_FILE_EXTENSION)) != fileName.length()) {
		return false;
	}

	util::NormalIStringStream iss(fileName);
	iss.seekg(static_cast<std::streamoff>(strlen(LOG_FILE_BASE_NAME)));
	uint32_t num32;
	uint64_t num64;
	char c;
	iss >> num32 >> c >> num64;

	if (iss.fail()) {
		return false;
	}
	if (c != '_') {
		return false;
	}
	if (static_cast<std::string::size_type>(iss.tellg()) != pos) {
		return false;
	}
	pgId = num32; 
	cpId = num64;
	return true;
}


const char *const LogRecord::GS_LOG_TYPE_NAME_CHECKPOINT_START = "[CHECKPOINT_START]";

const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_SESSION = "[CREATE_SESSION]";
const char *const LogRecord::GS_LOG_TYPE_NAME_CLOSE_SESSION = "[CLOSE_SESSION]";
const char *const LogRecord::GS_LOG_TYPE_NAME_BEGIN = "[TXN_BEGIN]";
const char *const LogRecord::GS_LOG_TYPE_NAME_COMMIT = "[TXN_COMMIT]";
const char *const LogRecord::GS_LOG_TYPE_NAME_ABORT = "[TXN_ABORT]";

const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_PARTITION = "[CREATE_PARTITION]";
const char *const LogRecord::GS_LOG_TYPE_NAME_DROP_PARTITION = "[DROP_PARTITION]";
const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_CONTAINER = "[CREATE_CONTAINER]";
const char *const LogRecord::GS_LOG_TYPE_NAME_DROP_CONTAINER = "[DROP_CONTAINER]";
const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_INDEX = "[CREATE_INDEX]";
const char *const LogRecord::GS_LOG_TYPE_NAME_DROP_INDEX = "[DROP_INDEX]";
const char *const LogRecord::GS_LOG_TYPE_NAME_MIGRATE_SCHEMA = "[MIGRATE_SCHEMA]";
const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_EVENT_NOTIFICATION = "[CREATE_EVENT_NOTIFICATION]";
const char *const LogRecord::GS_LOG_TYPE_NAME_DELETE_EVENT_NOTIFICATION = "[DELETE_EVENT_NOTIFICATION]";

const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_COLLECTION_ROW = "[CREATE_COLLECTION_ROW]";
const char *const LogRecord::GS_LOG_TYPE_NAME_UPDATE_COLLECTION_ROW = "[UPDATE_COLLECTION_ROW]";
const char *const LogRecord::GS_LOG_TYPE_NAME_DELETE_COLLECTION_ROW = "[DELETE_COLLECTION_ROW]";
const char *const LogRecord::GS_LOG_TYPE_NAME_PUT_COLLECTION_MULTIPLE_ROWS = "[PUT_COLLECTION_MULTIPLE_ROWS]";
const char *const LogRecord::GS_LOG_TYPE_NAME_LOCK_COLLECTION_ROW = "[LOCK_COLLECTION_ROW]";

const char *const LogRecord::GS_LOG_TYPE_NAME_CREATE_TIME_SERIES_ROW = "[CREATE_TIME_SERIES_ROW]";
const char *const LogRecord::GS_LOG_TYPE_NAME_UPDATE_TIME_SERIES_ROW = "[UPDATE_TIME_SERIES_ROW]";
const char *const LogRecord::GS_LOG_TYPE_NAME_DELETE_TIME_SERIES_ROW = "[DELETE_TIME_SERIES_ROW]";
const char *const LogRecord::GS_LOG_TYPE_NAME_PUT_TIME_SERIES_MULTIPLE_ROWS = "[PUT_TIME_SERIES_MULTIPLE_ROWS]";
const char *const LogRecord::GS_LOG_TYPE_NAME_LOCK_TIME_SERIES_ROW = "[LOCK_TIME_SERIES_ROW]";

const char *const LogRecord::GS_LOG_TYPE_NAME_CHECKPOINT_FILE_INFO = "[CHECKPOINT_FILE_INFO]";
const char *const LogRecord::GS_LOG_TYPE_NAME_CHUNK_TABLE_INFO = "[CHUNK_TABLE_INFO]";
const char *const LogRecord::GS_LOG_TYPE_NAME_CPFILE_LSN_INFO = "[CPFILE_LSN_INFO]";

const char *const LogRecord::GS_LOG_TYPE_NAME_UNDEF = "[UNDEF]";

void LogRecord::reset() {
	lsn_ = 0;
	type_ = LogManager::LOG_TYPE_UNDEF;
	partitionId_ = 0;
	txnId_ = 0;
	containerId_ = 0;

	bodyLen_ = 0;

	clientId_ = TXN_EMPTY_CLIENTID;

	stmtId_ = 0;
	txnContextCreationMode_ = 0;
	txnTimeout_ = 0;
	numRowId_ = 0;
	numRow_ = 0;
	dataLen_ = 0;
	rowIds_ = NULL;
	rowData_ = NULL;

	containerType_ = 0;
	containerNameLen_ = 0;
	containerInfoLen_ = 0;
	containerName_ = NULL;
	containerInfo_ = NULL;

	columnId_ = 0;
	mapType_ = 0;

	cpLsn_ = 0;
	txnContextNum_ = 0;
	containerNum_ = 0;
	clientIds_ = NULL;
	activeTxnIds_ = NULL;
	refContainerIds_ = NULL;
	lastExecStmtIds_ = NULL;
	txnTimeouts_ = NULL;

	withBegin_ = false;
	isAutoCommit_ = false;

	fileVersion_ = 0;
}


LogCursor::LogCursor() :
		pgId_(UNDEF_PARTITIONGROUPID),
		offset_(0) {
}

LogCursor::~LogCursor() {
}

bool LogCursor::isEmpty() const {
	return (blocksLatch_.get() == NULL);
}

void LogCursor::clear() {
	pgId_ = UNDEF_PARTITIONGROUPID;
	offset_ = 0;

	blocksLatch_.clear();
}

bool LogCursor::nextLog(
		LogRecord &logRecord,
		util::XArray<uint8_t> &binaryLogRecord,
		PartitionId pId,
		uint8_t targetLogType) {
	logRecord.reset();

	if (isEmpty()) {
		return false;
	}

	LogManager::PartitionGroupManager *pgManager =
			blocksLatch_.getManager();

	const size_t orgSize = binaryLogRecord.size();
	while (pgManager->getLogHeader(
			blocksLatch_, offset_, logRecord, &binaryLogRecord, false)) {

		if ((pId == UNDEF_PARTITIONID ||
				pId == logRecord.partitionId_) &&
				(targetLogType == LogManager::LOG_TYPE_UNDEF ||
				targetLogType == logRecord.type_) &&
				logRecord.type_ != LogManager::LOG_TYPE_NOOP) {

			if (pgManager->getLogBody(
					blocksLatch_, offset_, binaryLogRecord, logRecord.bodyLen_)) {
				const uint8_t *addr = binaryLogRecord.data() +
						orgSize + LogRecord::LOG_HEADER_SIZE;
				LogManager::parseLogBody(addr, logRecord);
				return true;
			}

		}
		else {
			pgManager->skipLogBody(blocksLatch_, offset_, logRecord.bodyLen_);
		}

		binaryLogRecord.resize(orgSize);
	}

	logRecord.reset();
	return false;
}

LogCursor::Progress LogCursor::getProgress() const {
	Progress progress;

	if (!isEmpty()) {
		progress.pgId_ = pgId_;
		progress.cpId_ = blocksLatch_.getFileInfo()->getCheckpointId();
		progress.fileSize_ = blocksLatch_.getFileInfo()->getFileSize();
		progress.offset_ = offset_;
	}

	return progress;
}

void LogCursor::exportPosition(
		CheckpointId &cpId, uint64_t &offset,
		PartitionId &pId, LogSequentialNumber &lsn) {

	if (!isEmpty()) {
		LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE_CUSTOM(
				blocksLatch_.getManager()->conflictionDetector_, true);

		LogManager::LogBlocksLatch blocksLatch;
		blocksLatch_.duplicate(blocksLatch);

		LogRecord logRecord;
		uint64_t nextOffset = offset_;
		for (;;) {
			if (!blocksLatch.getManager()->getLogHeader(
					blocksLatch, nextOffset, logRecord, NULL, false)) {
				break;
			}

			cpId = blocksLatch.getFileInfo()->getCheckpointId();
			offset = nextOffset - LogRecord::LOG_HEADER_SIZE;
			pId = logRecord.partitionId_;
			lsn = logRecord.lsn_;

			if (blocksLatch.getManager()->skipLogBody(
					blocksLatch, nextOffset, logRecord.bodyLen_)) {
				return;
			}

		}
	}

	cpId = UNDEF_CHECKPOINT_ID;
	offset = 0;
	pId = UNDEF_PARTITIONID;
	lsn = UNDEF_LSN;
}

void LogCursor::importPosition(LogManager &logManager,
		CheckpointId cpId, uint64_t offset,
		PartitionId pId, LogSequentialNumber lsn) {
	clear();

	if (cpId == UNDEF_CHECKPOINT_ID && offset == 0 &&
			pId == UNDEF_PARTITIONID && lsn == UNDEF_LSN) {
		return;
	}


	if (pId >= logManager.config_.pgConfig_.getPartitionCount()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
	}

	const PartitionGroupId pgId = logManager.calcPartitionGroupId(pId);
	LogManager::PartitionGroupManager &pgManager =
			*logManager.pgManagerList_[pgId];

	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE_CUSTOM(
			pgManager.conflictionDetector_, true);

	LogManager::LogFileLatch fileLatch;
	fileLatch.set(pgManager, cpId, true);

	LogManager::LogBlocksLatch blocksLatch;

	if (offset <= 0 ||
			offset > pgManager.getAvailableEndOffset(fileLatch, blocksLatch)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
	}

	const uint64_t targetBlockIndex =
			LogManager::getBlockIndex(logManager.config_, offset - 1);
	uint64_t blockIndex = targetBlockIndex;
	blocksLatch.setForScan(fileLatch, blockIndex);

	const uint32_t firstRecordOffset =
			blocksLatch.get()->getFirstRecordOffest(blockIndex);
	while (firstRecordOffset <= 0) {
		if (blockIndex <= 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
		}
		blockIndex--;
	}

	LogRecord logRecord;
	const LogManager::BlockPosition endPosition =
			LogManager::BlockPosition(cpId, blockIndex + 1);

	LogManager::LogBlocksLatch checkingBlocksLatch;
	blocksLatch.duplicate(checkingBlocksLatch);
	uint64_t offsetByBlock = LogManager::getFileOffset(
			logManager.config_, blockIndex, firstRecordOffset);

	for (;;) {
		const uint64_t lastOffeset = offsetByBlock;

		if (!pgManager.getLogHeader(checkingBlocksLatch, offsetByBlock,
				logRecord, NULL, true, &endPosition)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
		}

		if (!pgManager.skipLogBody(
				checkingBlocksLatch, offsetByBlock, logRecord.bodyLen_)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
		}

		if (lastOffeset == offset &&
				pId == logRecord.partitionId_ && lsn == logRecord.lsn_) {
			break;
		}

		if (lastOffeset >= offset) {
			GS_THROW_USER_ERROR(
					GS_ERROR_LM_INTERNAL_INVALID_ARGUMENT, "");
		}
	}

	blocksLatch.duplicate(blocksLatch_);
	pgId_ = pgId;
	offset_ = offset;
}

LogCursor::Progress::Progress() :
		pgId_(UNDEF_PARTITIONGROUPID),
		cpId_(UNDEF_CHECKPOINT_ID),
		fileSize_(0),
		offset_(0) {
}
