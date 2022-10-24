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
	@brief Definition of LogManager
*/
#ifndef LOG_MANAGER_H_
#define LOG_MANAGER_H_

#include "data_type.h"
#include "utility_v5.h"
#include "config_table.h"
#include "partition_file.h"
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>


UTIL_TRACER_DECLARE(LOG_MANAGER);

/*!
	@brief ログの種類
*/
enum LogType {
	CPLog,  
	CPULog, 
	OBJLog, 


	TXNLog, 
	PutChunkStartLog, 
	PutChunkDataLog,  
	PutChunkEndLog,   
	NOPLog, 
	EOFLog, 
	CheckpointEndLog  
};

enum LogFlushMode{
	LOG_WRITE_WAL_BUFFER,
	LOG_FLUSH_FILE,
};

struct LogManagerStats  {
	enum Param {

		LOG_STAT_FLUSH_COUNT,
		LOG_STAT_FLUSH_TIME,

		LOG_STAT_FLUSH_COUNT_ON_CP,
		LOG_STAT_FLUSH_TIME_ON_CP,

		LOG_STAT_DUPLICATE_LOG_MODE,

		LOG_STAT_END
	};

	typedef TimeStatTable<Param, LOG_STAT_END> TimeTable;
	typedef TimeTable::BaseTable Table;
	typedef Table::Mapper Mapper;

	explicit LogManagerStats(const Mapper *mapper);

	static void setUpMapper(Mapper &mapper);
	void merge(const Table &src);

	Table table_;
	TimeTable timeTable_;
};

/*!
	@brief ログクラス
*/
class Log {
private:
	static const int64_t serialVersionUID = 1L;

	LogType type_;
	uint16_t checksum_;
	uint16_t integrityCheckVal_;
	int16_t version_;
	LogSequentialNumber lsn_;
	int64_t chunkId_;
	int64_t groupId_;
	int64_t offset_;
	int8_t vacancy_;

	int64_t dataSize_;
	const void* data_;

public:
	Log(LogType cplog)
	: type_(cplog), checksum_(0), integrityCheckVal_(0),
	  version_(0), lsn_(UINT64_MAX),
	  chunkId_(-1), groupId_(-1), offset_(-1), vacancy_(-1),
	  dataSize_(0), data_(NULL)
	{ }

	LogType getType() const {
		return type_;
	}

	Log& setCheckSum();
	uint16_t getCheckSum() const;

	bool checkSum() const;

	Log& setIntegrityCheckVal(uint16_t checkVal);
	uint16_t getIntegrityCheckVal() const;

	Log& setLsn(LogSequentialNumber lsn);
	LogSequentialNumber getLsn() const;

	template <typename S>
	void encode(S& out);

	template <typename S>
	void decode(S& in, util::StackAllocator& alloc);

	void encode(util::StackAllocator& alloc, util::XArray<uint8_t>& logRecoreds);

	Log& setChunkId(int64_t chunkid);

	int64_t getChunkId() const;

	Log& setGroupId(int64_t groupid);

	int64_t getGroupId() const;

	Log& setOffset(int64_t offset);
	int64_t getOffset() const;

	Log& setVacancy(int8_t vacancy);
	int8_t getVacancy() const;

	Log& setVersion(int16_t ver);
	int16_t getVersion() const;

	Log& setDataSize(int64_t dataLen);
	Log& setData(const void* data);

	const void* getData() const;
	int64_t getDataSize() const;

	std::string toString() const;
};

struct DuplicateLogMode {
	static const LocalStatTypes::MergeMode STATUS_MERGE_MODE =
			LocalStatTypes::MERGE_MAX;

	enum Status {
		DUPLICATE_LOG_DISABLE,
		DUPLICATE_LOG_ENABLE,
		DUPLICATE_LOG_ERROR,
	};

	DuplicateLogMode() :
			status_(DUPLICATE_LOG_DISABLE),
			stopOnDuplicateErrorFlag_(false) {
	}

	static void applyDuplicateStatus(
			Status srcStatus, LogManagerStats &stats, bool force);
	static void applyDuplicateStatus(Status src, Status &dest, bool force);

	std::string duplicateLogPath_;
	Status status_;
	bool stopOnDuplicateErrorFlag_;
};


template <class L>
class LogManager;
class SimpleFile;
/*!
	@brief ログイテレータ
*/
template <class L>
class LogIterator {
private:
	static const size_t DEFAULT_LOG_READ_BUFFER_SIZE = 4 * 1024;

	LogManager<L>* logManager_;
	util::StackAllocator* alloc_;

	std::vector<std::string> fileNames_;
	int32_t nextFile_;
	BufferedReader* logFile_;
	int64_t logversion_;
	int64_t offset_;
	int64_t lastLsn_;
	int64_t checkLsn_;
	bool isXLog_;
	std::vector<uint8_t> buffer_;

public:
	LogIterator(LogManager<L>* logmanager, int64_t logVersion)
		: logManager_(logmanager), alloc_(&logmanager->getAllocator()),
		  nextFile_(0), logFile_(NULL), logversion_(logVersion),
		  offset_(0), lastLsn_(0), checkLsn_(-1), isXLog_(false) {
	};

	LogIterator(LogManager<L>* logmanager, int64_t logVersion, uint64_t offset)
		: logManager_(logmanager), alloc_(&logmanager->getAllocator()),
		  nextFile_(0), logFile_(NULL), logversion_(logVersion),
		  offset_(offset), lastLsn_(0), checkLsn_(-1), isXLog_(false) {
	};

	void add(const std::string& filename);

	bool checkExists(LogSequentialNumber offset);

	std::unique_ptr<Log> next(bool forRecovery);
	void fin();

	void removeLogFiles();

	void markXLog() {
		isXLog_ = true;
	}

	std::string toString() const;
};


/*!
	@brief WALバッファ
*/
class WALBuffer : public Interchangeable {
private:
	PartitionId pId_;
	int32_t bufferSize_;
	SimpleFile* logFile_;
	SimpleFile* duplicateFile_;
	bool stopOnDuplicateError_;
	DuplicateLogMode::Status duplicateStatus_;
	std::unique_ptr<uint8_t[]> buffer_;
	int32_t position_;
	util::Atomic<bool> needFlush_;
	LogManagerStats &stats_;

public:
	WALBuffer(PartitionId pId, LogManagerStats &stats);
	~WALBuffer();

	WALBuffer& init(SimpleFile* logfile);

	uint64_t append(const uint8_t* data, size_t dataLength);

	bool flush(LogFlushMode x, bool byCP);

	static void flush(
			SimpleFile &logfile, bool byCP, LogManagerStats &stats);

	void fin();

	void setDuplicateFile(SimpleFile* file);

	void setStopOnDuplicateError(bool stopOnDuplicateError);
	bool getStopOnDuplicateError();

	void initDuplicateStatus();
	DuplicateLogMode::Status getDuplicateStatus();

	SimpleFile* initDuplicateLogFile(const char* path);

	double getActivity() const;

	bool resize(int32_t capacity);

	PartitionId getPId() { return pId_; }

private:
	void handleDuplicateError(std::exception &e);
	void setDuplicateStatus(
			DuplicateLogMode::Status status, bool force = false);
};


struct LogManagerConst {
	static const char* CPLOG_FILE_SUFFIX;
	static const char* XLOG_FILE_SUFFIX;
	static const char* INCREMENTAL_FILE_SUFFIX;
};

template <class L>
/*!
	@brief ログマネージャ
*/
class LogManager {
public:
	static const size_t LOG_DATA_LIMIT_SIZE = INT32_MAX - 100;

	LogManager(
			ConfigTable& configTable, std::unique_ptr<L>&& locker,
			util::StackAllocator& alloc, WALBuffer& xWALbuffer,
			WALBuffer& cpWALBuffer, int32_t checkpointrange,
			const char* cpLogPath, const char* xLogPath, PartitionId pId,
			LogManagerStats &stats);
	~LogManager();

	enum PersistencyMode {
		PERSISTENCY_NORMAL = 1,
		PERSISTENCY_KEEP_ALL_LOG = 2
	};
	/*!
		@brief Configuration of LogManager
	*/
	struct Config : public ConfigTable::ParamHandler {
		Config(ConfigTable &configTable);

		void setUpConfigHandler();
		virtual void operator()(
				ConfigTable::ParamId id, const ParamValue &value);

		uint32_t getBlockSize() const;
		uint32_t getBlockBufferCount() const;
		uint32_t getIndexBufferSize() const;

		ConfigTable *configTable_;
		PartitionGroupConfig pgConfig_;

		int32_t persistencyMode_;
		int32_t logWriteMode_;
		bool alwaysFlushOnTxnEnd_;

		std::string logDirectory_;
		uint32_t blockSizeBits_;
		uint32_t blockBufferCountBits_;
		uint32_t lockRetryCount_;
		uint32_t lockRetryIntervalMillis_;

		bool indexEnabled_;
		uint32_t indexBufferSizeBits_;

		bool primaryCheckpointAssumed_;
		bool emptyFileAppendable_;

		uint32_t ioWarningThresholdMillis_;

		uint64_t retainedFileCount_;
		uint32_t logFileClearCacheInterval_;
		bool lsnBaseFileRetention_;
	};

	Config& getConfig() { return config_; }

	std::tuple<LogIterator<L>, LogIterator<L> > init(bool byCP);

	void init(int64_t logversion, bool withFlush, bool byCP);

	inline int64_t getLogVersion() { return logversion_; };

	void startCheckpoint0(bool withFlush, bool byCP);

	void startCheckpoint(bool withFlush, bool byCP);

	LogSequentialNumber appendXLog(
			LogType type, void* data, size_t dataLen, util::XArray<uint8_t>* logBinary);

	void copyXLog(
		LogType type, LogSequentialNumber lsn, const void* data, size_t len);

	void appendCpLog(Log& log);

	void flushCpLog(LogFlushMode x, bool byCP);

	void flushXLog(LogFlushMode x, bool byCP);

	void flushXLogByCommit(bool byCP);

	void removeLogFiles(int64_t baseLogVer, int64_t checkpointRange);

	LogSequentialNumber getStartLSN();

	int64_t getStartLogVersion();

	void closeFiles(bool withFlush, bool byCP);

	void removeAllLogFiles(bool byCP);

	void addPendingFiles(SimpleFile *file);
	void closePendingFiles(bool force, bool byCP);

	void fin(bool byCP);

	inline L* locker() {
		return locker_.get();
	}

	inline util::StackAllocator& getAllocator() {
		return alloc_;
	}

	LogIterator<L> createLogIterator(int64_t logversion);

	LogIterator<L> createXLogIterator(LogSequentialNumber lsn);

	LogIterator<L> createCPLogIterator(LogSequentialNumber lsn);

	LogIterator<L> createChunkDataLogIterator();

	void copyCpLog(const char* path, uint8_t* buffer, size_t bufferSize);
	void copyXLog(const char* path, uint8_t* buffer, size_t bufferSize);
	bool isLongtermSyncLogAvailable() const;
	void setLongtermSyncLogError(const std::string &message);
	bool getLongtermSyncLogErrorFlag() const;
	const std::string& getLongtermSyncLogErrorMessage() const;

	void setKeepXLogVersion(int64_t logVersion) {
		keepXLogVersion_ = logVersion;
	}
	int64_t getXKeepLogVersion() { return keepXLogVersion_; }

	LogSequentialNumber getLSN();
	void setLSN(LogSequentialNumber lsn);
	LogSequentialNumber incrementLSN();
	void setSuffix(const char* suffix);

	void setChild(std::unique_ptr<LogManager> child);

	DuplicateLogMode& getDuplicateLogMode();
	void setDuplicateLogMode(const DuplicateLogMode& mode);
	void setDuplicateFile(SimpleFile* file);
	void setStopOnDuplicateError(bool stopOnDuplicateError);
	bool getStopOnDuplicateError();

private:
	void setDuplicateStatus(
			DuplicateLogMode::Status status, bool force = false);

	Config config_;
	std::unique_ptr<L> locker_;
	util::StackAllocator& alloc_;
	WALBuffer& xWALBuffer_;
	WALBuffer& cpWALBuffer_;
	int32_t checkpointrange_;
	PartitionId pId_;
	std::string cpLogPath_;
	std::string xLogPath_;
	int64_t logversion_;
	SimpleFile* cpFile_;
	SimpleFile* xFile_;
	SimpleFile* duplicateFile_;
	const char* suffix_;
	uint64_t xLogFileOffset_; 
	util::Atomic<uint64_t> xLogCount_;

	bool longtermLogAvailable_;
	std::string longtermSyncLogErrorMessage_;
	bool longtermSyncLogErrorFlag_;
	int64_t keepXLogVersion_;

	typedef std::map<int64_t, LogSequentialNumber> ExistLsnMap;
	ExistLsnMap existLsnMap_; 
	LogManager<L>* child_;
	DuplicateLogMode duplicateLogMode_;
	std::vector<SimpleFile*> pendingFiles_;
	LogManagerStats &stats_;
};



/*!
	@brief ログファイルの追加
*/
template<class L>
void LogIterator<L>::add(const std::string& filename) {
	fileNames_.push_back(filename);
}

/*!
	@brief 存在チェック
*/
template<class L>
bool LogIterator<L>::checkExists(LogSequentialNumber lsn) {
	int64_t offset = 0;

	util::LockGuard<Locker>(*logManager_->locker());
	if (lsn < logManager_->getStartLSN()) {
		return false;
	}
	else if (lsn > logManager_->getLSN()) {
		return false;
	}
	assert(logFile_ == NULL);
	std::unique_ptr<Log> log;
	bool found = false;
	try {
		while (true) {
			util::StackAllocator::Scope scope(logManager_->getAllocator());
			log = next(false);
			if (log == nullptr) {
				break;
			}
			if (log->getLsn() == lsn) {
				found = true;
				break;
			}
			offset = offset_;
		}
		offset_ = offset; 

		return found;
	}
	catch (std::exception &e) {
		throw e;
	}
}

/*!
	@brief 次のログ取得
*/
template <class L>
std::unique_ptr<Log> LogIterator<L>::next(bool forRecovery) {
	util::LockGuard<Locker>(*logManager_->locker());
	try {
		while (true) {
			if (logFile_ == NULL) {
				if (nextFile_ < static_cast<int32_t>(fileNames_.size())) {
					logFile_ = new BufferedReader(
						fileNames_.at(nextFile_), DEFAULT_LOG_READ_BUFFER_SIZE);
					logFile_->open();
					logversion_ = nextFile_;
					nextFile_++;
					offset_ = 0;
					if (logFile_->getSize() == 0) {
						logFile_->close();
						delete logFile_;
						logFile_ = NULL;
						continue;
					}
				} else {
					return NULL;
				}
			}
			try {
				uint64_t fileSize = logFile_->getSize();
				if (offset_ + sizeof(int64_t) > fileSize) {
					if (isXLog_) {
						logManager_->flushXLog(LOG_WRITE_WAL_BUFFER, false);
						logManager_->flushXLog(LOG_FLUSH_FILE, false);
						fileSize = logFile_->getSize();
						if (offset_ + sizeof(int64_t) > fileSize) {
							logFile_->close();
							delete logFile_;
							logFile_ = NULL;
							continue;
						}
					} else {
						logFile_->close();
						delete logFile_;
						logFile_ = NULL;
						continue;
					}
				}
				int64_t len = logFile_->readLong(offset_);
				if (len <= 0) {
					logFile_->close();
					delete logFile_;
					logFile_ = NULL;
					continue;
				}
				if (buffer_.size() < static_cast<size_t>(len)) {
					const uint8_t dummy = 0;
					buffer_.assign(len, dummy);
				}
				if ( static_cast<uint64_t>(offset_ + len) > fileSize) {
					GS_TRACE_INFO(
						LOG_MANAGER, GS_TRACE_LM_INCOMPLETE_LOG_RECORD,
						"Log record is incomplete: recordLength=" << len <<
						",fileName=" << logFile_->getFileName() <<
						",offset=" << offset_ << ",fileSize=" << fileSize <<
						",lastLsn=" << lastLsn_);
					logFile_->close();
					delete logFile_;
					logFile_ = NULL;
					continue;
				}
				int64_t readSize;
				logFile_->read(len, buffer_.data(), offset_, readSize);
				std::unique_ptr<Log> log(UTIL_NEW Log(LogType::NOPLog));
				util::ArrayByteInStream in = util::ArrayByteInStream(
						util::ArrayInStream(buffer_.data(), len));
				log->decode<util::ArrayByteInStream>(in, *alloc_);
				lastLsn_ = log->getLsn();
				if (log->getType() == LogType::TXNLog
						&& forRecovery
						&& nextFile_ < static_cast<int32_t>(fileNames_.size())) {
					GS_TRACE_INFO(
						LOG_MANAGER, GS_TRACE_LM_SCAN_LOG_INFO,
						"TXNLog is skipped. nextFile_=" << nextFile_ <<
						",fileNames_.size()=" << fileNames_.size() <<
						",fileName=" << logFile_->getFileName());
					continue;
				}
				if (log->getType() == LogType::EOFLog) {
					GS_TRACE_INFO(
						LOG_MANAGER, GS_TRACE_LM_SCAN_LOG_INFO,
						"EOFLog: fileName=" << logFile_->getFileName());
					logFile_->close();
					delete logFile_;
					logFile_ = NULL;
					continue;
				}
				if (log->getType() == LogType::CheckpointEndLog
						&& forRecovery
						&& nextFile_ >= static_cast<int32_t>(fileNames_.size())) {
					GS_TRACE_INFO(
						LOG_MANAGER, GS_TRACE_LM_SCAN_LOG_INFO,
						"CheckpointEndLog(redo CPLog last): fileName=" <<
						logFile_->getFileName() <<
						",offset=" << offset_);
					logFile_->close();
					delete logFile_;
					logFile_ = NULL;
				}
				return log;
			}
			catch (std::exception &e) {
				logFile_->close();
				delete logFile_;
				logFile_ = NULL;
				throw e;
			}
		}
	}
	catch (std::exception &e) {
		throw e;
	}
}

/*!
	@brief 終了
*/
template<class L>
void LogIterator<L>::fin() {
	if (logFile_ != NULL) {
		logFile_->close();
		delete logFile_;
		logFile_ = NULL;
	}
}

template<class L>
std::string LogIterator<L>::toString() const {
	util::NormalOStringStream oss;
	for (const auto& name : fileNames_) {
		oss << name.c_str() << ",";
	}
	return oss.str();
}


/*!
	@brief コンストラクタ
*/
template<class L>
LogManager<L>::LogManager(
		ConfigTable& configTable,
		std::unique_ptr<L>&& locker, util::StackAllocator& alloc,
		WALBuffer& xWALBuffer, WALBuffer& cpWALBuffer,
		int32_t checkpointrange, const char* cpLogPath, const char* xLogPath,
		PartitionId pId, LogManagerStats &stats) :
		config_(configTable), locker_(std::move(locker)), alloc_(alloc),
		xWALBuffer_(xWALBuffer), cpWALBuffer_(cpWALBuffer),
		checkpointrange_(checkpointrange), pId_(pId),
		cpLogPath_(cpLogPath), xLogPath_(xLogPath),
		logversion_(-1), cpFile_(nullptr), xFile_(nullptr), duplicateFile_(nullptr),
		suffix_(nullptr), xLogFileOffset_(0), xLogCount_(0),
		longtermLogAvailable_(false), longtermSyncLogErrorFlag_(false),
		keepXLogVersion_(-1), child_(NULL),
		stats_(stats)
{
	xWALBuffer_.use();
	cpWALBuffer_.use();
}

template<class L>
LogManager<L>::~LogManager() {
	const bool byCP = false;
	closePendingFiles(true, byCP);
}

/*!
	@brief LSN取得
*/
template <class L> uint64_t LogManager<L>::getLSN() {
	return xLogCount_;
}

template <class L> void LogManager<L>::setLSN(uint64_t lsn) {
	xLogCount_ = lsn;
}

template <class L> LogSequentialNumber LogManager<L>::incrementLSN() {
	++xLogCount_;
	return xLogCount_;
}

template <class L> void LogManager<L>::setSuffix(const char* suffix) {
	suffix_ = suffix;
}

/*!
	@brief ディスクからログ開始ポイントを決定
*/
template <class L>
std::tuple<LogIterator<L>, LogIterator<L> > LogManager<L>::init(bool byCP) {
	int32_t latestcplogver = -1;
	int32_t latestxlogver = -1;

	closeFiles(true, byCP);

	std::vector<int32_t> cplogs;
	UtilFile::getFileNumbers(
			cpLogPath_.c_str(), LogManagerConst::CPLOG_FILE_SUFFIX,
			LogManagerConst::INCREMENTAL_FILE_SUFFIX, cplogs);
	std::vector<int32_t> x;
	for (int32_t cplogver : cplogs) {
		LogIterator<L> cpit = createLogIterator(cplogver);

		std::unique_ptr<Log> log;
		while ((log = cpit.next(true)) != NULL) { 
			if (log->getType() == LogType::CheckpointEndLog) {
				x.push_back(cplogver);
				break;
			}
		}
		cpit.fin();
	}
	cplogs = x;
	std::vector<int32_t> xlogs;
	UtilFile::getFileNumbers(
			xLogPath_.c_str(), LogManagerConst::XLOG_FILE_SUFFIX, NULL, xlogs);

	if (cplogs.size() > 0) {
		latestcplogver = *std::max_element(cplogs.begin(), cplogs.end());
	}
	if (xlogs.size() > 0) {
		latestxlogver = *std::max_element(xlogs.begin(), xlogs.end());
		if (latestcplogver > latestxlogver) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
					"log version " << latestcplogver << " vs " << latestxlogver);
		}
	}
	LogIterator<L> cpit(this, latestcplogver);
	int32_t mincplogver = INT32_MAX;
	for (int32_t ver = std::max(0, latestcplogver - checkpointrange_ + 1);
			ver <= latestcplogver; ver++) {
		auto result = std::find(cplogs.begin(), cplogs.end(), ver);
		if (result != cplogs.end()) {
			mincplogver = std::min(mincplogver, ver);
		} else {
			mincplogver = ver + 1;
		}
	}
	for (int32_t ver = mincplogver; ver <= latestcplogver; ver++) {
		util::NormalOStringStream oss;
		oss << cpLogPath_ << "/" << pId_ << "_" << ver <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
		cpit.add(oss.str());
	}
	LogIterator<L> xit(this, latestcplogver);
	for (int32_t ver = latestcplogver; ver <= latestxlogver; ver++) {
		auto result = std::find(xlogs.begin(), xlogs.end(), ver);
		if (result != xlogs.end()) {
			util::NormalOStringStream oss;
			oss << xLogPath_ << "/" << pId_ << "_" << ver <<
					LogManagerConst::XLOG_FILE_SUFFIX;
			xit.add(oss.str());
		}
	}
	logversion_ = latestcplogver;
	return std::forward_as_tuple(cpit, xit);
}

/*!
	@brief ディスク無し、サイクル性あり
*/
template <class L>
void LogManager<L>::init(int64_t logversion, bool withFlush, bool byCP) {
	closeFiles(withFlush, byCP);

	logversion_ = logversion;

	util::NormalOStringStream oss1;
	if (!suffix_) {
		oss1 << cpLogPath_ << "/" << pId_ << "_" << logversion_ <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
	}
	else {
		oss1 << cpLogPath_ << "/" << pId_ << "_" << logversion_ << suffix_ <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
	}
	cpFile_ = UTIL_NEW SimpleFile(oss1.str());
	cpFile_->open();

	util::NormalOStringStream oss2;
	oss2 << xLogPath_ << "/" << pId_ << "_" << logversion_ <<
			LogManagerConst::XLOG_FILE_SUFFIX;
	xFile_ = UTIL_NEW SimpleFile(oss2.str());
	xFile_->open();
	xLogFileOffset_ = 0;

	xWALBuffer_.init(xFile_);

	if (duplicateLogMode_.status_ == DuplicateLogMode::DUPLICATE_LOG_ENABLE) {
		if (xWALBuffer_.getDuplicateStatus() !=
				DuplicateLogMode::DUPLICATE_LOG_ERROR) {
			util::NormalOStringStream oss3;
			oss3 << duplicateLogMode_.duplicateLogPath_.c_str() << "/" << pId_ <<
					"_" << logversion_ << LogManagerConst::XLOG_FILE_SUFFIX;

			xWALBuffer_.setStopOnDuplicateError(
					duplicateLogMode_.stopOnDuplicateErrorFlag_);
			duplicateFile_ = xWALBuffer_.initDuplicateLogFile(oss3.str().c_str());
			if (!duplicateFile_) {
				setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_ERROR);
			}
		}
		else {
			setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_ERROR);
		}
	}
	else {
		xWALBuffer_.setDuplicateFile(NULL);
	}
	cpWALBuffer_.init(cpFile_);
	existLsnMap_[logversion] = getLSN() + 1; 

	appendXLog(LogType::NOPLog, NULL, 0, NULL); 
}

/*!
	@brief リカバリ後のチェックポイント
*/
template <class L>
void LogManager<L>::startCheckpoint0(bool withFlush, bool byCP) {
	std::vector<int32_t> _cplogs;
	UtilFile::getFileNumbers(
			cpLogPath_.c_str(), LogManagerConst::CPLOG_FILE_SUFFIX,
			LogManagerConst::INCREMENTAL_FILE_SUFFIX, _cplogs);
	if (_cplogs.size() < 0) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "startCheckpoint");
	}
	if (_cplogs.size() > 0) {
		init(
				*std::max_element(_cplogs.begin(), _cplogs.end()) + 1,
				withFlush, byCP);
	}
	else {
		init(0, withFlush, byCP);
	} 
}

/*!
	@brief 通常のチェックポイント
*/
template <class L>
void LogManager<L>::startCheckpoint(bool withFlush, bool byCP) {
	init(logversion_ + 1, withFlush, byCP);
}

/*!
	@brief Xログ追加

	@note XログはWALバッファを経由してXログファイルに書き込む
	@note V5.0では暫定的に扱えるログ内バイナリデータ超の上限は(2GB-100)とする
*/
template <class L>
LogSequentialNumber LogManager<L>::appendXLog(
		LogType type, void* data, size_t len, util::XArray<uint8_t>* logBinary) {
	try {
		util::StackAllocator::Scope scope(alloc_);
		util::XArray<uint8_t> binary(alloc_);
		typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
		util::XArrayOutStream<> arrayOut(binary);
		OutStream out(arrayOut);

		uint8_t* addr = static_cast<uint8_t*>(data);
		Log log(type);
		int64_t dataLen = 0;
		if (len > 0 && data) {
			if (len > LOG_DATA_LIMIT_SIZE) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"Too large data size (size=" << len << ")");
			}
			log.setData(addr);
			dataLen = static_cast<int64_t>(len);
		}
		log.setDataSize(dataLen);

		LogSequentialNumber lsn = 0;
		{
			util::LockGuard<Locker>(*locker());
			lsn = getLSN();
			if (type == LogType::OBJLog) {
				++lsn;
			}
			log.setLsn(lsn);
			log.setCheckSum();
			log.encode(out);
			xLogFileOffset_ += xWALBuffer_.append(binary.data(), binary.size());
			if (type == LogType::OBJLog) {
				setLSN(lsn);
			}
		}
		if (child_) {
			child_->copyXLog(type, lsn, data, len);
		}
		if (logBinary) {
			logBinary->clear();
			logBinary->push_back(binary.data(), binary.size());
		}
		return lsn;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Write XLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief レプリケーション用Xログ追加

		XログはWALバッファを経由してXログファイルに書き込む
*/
template <class L>
void LogManager<L>::copyXLog(
		LogType type, LogSequentialNumber lsn, const void* data, size_t len) {
	static_cast<void>(type);
	if (!data) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "data must be NOT NULL");
	}
	try {
		util::LockGuard<Locker>(*locker());
		xLogFileOffset_ += xWALBuffer_.append(static_cast<const uint8_t*>(data), len);
		setLSN(lsn);
	}
	catch (std::exception& e) { 
		GS_RETHROW_SYSTEM_ERROR(
				e, "Copy XLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief CPログ追加
*/
template <class L>
void LogManager<L>::appendCpLog(Log& log) {
	try {
		util::StackAllocator::Scope scope(alloc_);
		util::LockGuard<Locker>(*locker());

		util::XArray<uint8_t> binary(alloc_);
		typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
		util::XArrayOutStream<> arrayOut(binary);
		OutStream out(arrayOut);

		LogSequentialNumber lsn = getLSN();
		log.setLsn(lsn);
		log.setCheckSum();
		log.encode(out);

		cpWALBuffer_.append(binary.data(), binary.size());

	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Write CPLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief CPログフラッシュ
*/
template <class L>
void LogManager<L>::flushCpLog(LogFlushMode x, bool byCP) {
	if (!cpFile_) {
		return;
	}
	try {
		util::LockGuard<Locker>(locker());
		cpWALBuffer_.flush(x, byCP);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Flush CPLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Xログバッファ出力＆ファイルフラッシュ
*/
template <class L>
void LogManager<L>::flushXLog(LogFlushMode x, bool byCP) {
	if (!xFile_) {
		return;
	}
	try {
		util::LockGuard<Locker>(locker());
		xWALBuffer_.flush(x, byCP);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Flush XLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief コミット時Xログバッファ出力＆ファイルフラッシュ
	@note configでlogWriteMode=0指定時
*/
template <class L>
void LogManager<L>::flushXLogByCommit(bool byCP) {
	if (!xFile_ || !config_.alwaysFlushOnTxnEnd_) {
		return;
	}
	try {
		util::LockGuard<Locker>(locker());
		xWALBuffer_.flush(LOG_WRITE_WAL_BUFFER, byCP);
		xWALBuffer_.flush(LOG_FLUSH_FILE, byCP);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Flush XLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief 保持している最小のLSNを返す

	@note 主にクラスタで使用する。このLSNはCP実行時点(TXNLog)のLSNと一致する
	@note OBJLogはこのLSN+1以降になる(存在するとは限らない)
*/
template <class L>
LogSequentialNumber LogManager<L>::getStartLSN() {
	assert(!existLsnMap_.empty());
	const auto& itr = existLsnMap_.begin();
	return itr->second - 1;
}

/*!
	@brief 保持している最小のlogversionを返す
*/
template <class L>
int64_t LogManager<L>::getStartLogVersion() {
	assert(!existLsnMap_.empty());
	const auto& itr = existLsnMap_.begin();
	return itr->first;
}

/*!
	@brief 指定ログバージョン以前のファイル削除

	@note XログはV4同様にカレント以外にパラメータretainedFileCountの数残す
	@note CPログは最低checkpointRange数の世代は残す
*/
template <class L>
void LogManager<L>::removeLogFiles(int64_t baseLogVer, int64_t checkpointRange) {
	if (config_.persistencyMode_ == PERSISTENCY_KEEP_ALL_LOG) {
		return;
	}
	const int64_t retainedFileCount =
			static_cast<int64_t>(config_.retainedFileCount_);
	const int64_t targetCpLogVer =
			(checkpointRange > retainedFileCount + 1)
					? (baseLogVer - checkpointRange)
					  : (baseLogVer - retainedFileCount - 1);
	std::vector<int32_t> logs;
	{
		util::NormalOStringStream oss1;
		oss1 << LogManagerConst::INCREMENTAL_FILE_SUFFIX <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
		UtilFile::getFileNumbers(
				cpLogPath_.c_str(), oss1.str().c_str(), NULL, logs);
		for (int32_t ver : logs) {
			util::NormalOStringStream oss;
			oss << cpLogPath_ << "/" << pId_ << "_" << ver << oss1.str();
			UtilFile::removeFile(oss.str().c_str());
		}
	}
	logs.clear();
	UtilFile::getFileNumbers(
			cpLogPath_.c_str(), LogManagerConst::CPLOG_FILE_SUFFIX, NULL, logs);
	for (int32_t cpver : logs) {
		if (cpver <= targetCpLogVer) {
			util::NormalOStringStream oss;
			oss << cpLogPath_ << "/" << pId_ << "_" << cpver <<
					LogManagerConst::CPLOG_FILE_SUFFIX;
			UtilFile::removeFile(oss.str().c_str());
		}
	}
	logs.clear();
	int64_t targetXLogVer = baseLogVer - retainedFileCount - 1;
	if (keepXLogVersion_ != -1 && targetXLogVer >= keepXLogVersion_) {
		targetXLogVer = keepXLogVersion_ - 1;
	}
	UtilFile::getFileNumbers(
			xLogPath_.c_str(), LogManagerConst::XLOG_FILE_SUFFIX, NULL, logs);
	for (int32_t xver : logs) {
		if (xver <= targetXLogVer) {
			util::NormalOStringStream oss;
			oss << xLogPath_ << "/" << pId_ << "_" << xver <<
					LogManagerConst::XLOG_FILE_SUFFIX;
			UtilFile::removeFile(oss.str().c_str());
			existLsnMap_.erase(xver);
		}
	}
}


/*!
	@brief すべてのファイル削除(dropPartition用)
*/
template <class L>
void LogManager<L>::removeAllLogFiles(bool byCP) {
	closeFiles(true, byCP);

	std::vector<int32_t> cplogs;
	UtilFile::getFileNumbers(
			cpLogPath_.c_str(), LogManagerConst::CPLOG_FILE_SUFFIX,
			LogManagerConst::INCREMENTAL_FILE_SUFFIX, cplogs);

	for (int32_t ver : cplogs) {
		util::NormalOStringStream oss;
		oss << cpLogPath_ << "/" << pId_ << "_" << ver <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
		UtilFile::removeFile(oss.str().c_str());
	}

	std::vector<int32_t> xlogs;
	UtilFile::getFileNumbers(
			xLogPath_.c_str(), LogManagerConst::XLOG_FILE_SUFFIX, NULL, xlogs);

	for (int32_t ver : xlogs) {
		util::NormalOStringStream oss;
		oss << xLogPath_ << "/" << pId_ << "_" << ver <<
				LogManagerConst::XLOG_FILE_SUFFIX;
		UtilFile::removeFile(oss.str().c_str());
	}
}

/*!
	@brief ログファイルクローズ
*/
template <class L>
void LogManager<L>::closeFiles(bool withFlush, bool byCP) {
	if (cpFile_ != NULL) {
		try {
			cpWALBuffer_.flush(LOG_WRITE_WAL_BUFFER, byCP);
			if (withFlush) {
				cpWALBuffer_.flush(LOG_FLUSH_FILE, byCP);
				cpFile_->clearFileCache();
				cpFile_->close();
				delete cpFile_;
			}
			else {
				addPendingFiles(cpFile_);
			}
			cpFile_ = NULL;
		}
		catch (std::exception& e) {
			GS_RETHROW_SYSTEM_ERROR(
					e, "Close CPLog failed. reason=(" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	if (xFile_ != NULL) {
		try {
			appendXLog(LogType::EOFLog, NULL, 0, NULL);

			xWALBuffer_.flush(LOG_WRITE_WAL_BUFFER, byCP);
			if (withFlush) {
				xWALBuffer_.flush(LOG_FLUSH_FILE, byCP);
				xFile_->clearFileCache();
				xFile_->close();
				if (duplicateFile_) {
					duplicateFile_->clearFileCache();
					duplicateFile_->close();
					delete duplicateFile_;
				}
				delete xFile_;
			}
			else {
				addPendingFiles(xFile_);
				if (duplicateFile_) {
					addPendingFiles(duplicateFile_);
				}
				
			}
			xFile_ = NULL;
			duplicateFile_ = NULL;
		}
		catch (std::exception& e) {
			GS_RETHROW_SYSTEM_ERROR(
					e, "Close XLog failed. reason=(" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
}

template <class L>
void LogManager<L>::addPendingFiles(SimpleFile *file) {
	std::unique_ptr<SimpleFile> ptr(file);
	pendingFiles_.push_back(ptr.get());
	ptr.release();
}

template <class L>
void LogManager<L>::closePendingFiles(bool force, bool byCP) {
	while (!pendingFiles_.empty()) {
		std::unique_ptr<SimpleFile> file(pendingFiles_.back());
		pendingFiles_.pop_back();

		if (!force) {
			WALBuffer::flush(*file, byCP, stats_);
			file->clearFileCache();
			file->close();
		}
	}
}

/*!
	@brief 終了処理

		エリア削除など
*/
template <class L>
void LogManager<L>::fin(bool byCP) {
	closeFiles(true, byCP);
	xWALBuffer_.unuse();
	cpWALBuffer_.unuse();
}

/*!
	@brief 過去CPログ参照
*/
template <class L>
LogIterator<L> LogManager<L>::createLogIterator(int64_t logversion) {
	LogIterator<L> cpit(this, logversion);
	util::NormalOStringStream oss;
	oss << cpLogPath_ << "/" << pId_ << "_" << logversion <<
			LogManagerConst::CPLOG_FILE_SUFFIX;
	cpit.add(oss.str());
	return cpit;
}

/*!
	@brief 過去CPログ参照(LSN指定)
*/
template <class L>
LogIterator<L> LogManager<L>::createCPLogIterator(LogSequentialNumber lsn) {
	ExistLsnMap::reverse_iterator itr = existLsnMap_.rbegin();
	int64_t logversion = itr->first; 
	for (; itr != existLsnMap_.rend(); ++itr) {
		if (itr->second <= lsn) {
			logversion = itr->first;
			break;
		}
	}
	LogIterator<L> cpLogIt(this, logversion);
	util::NormalOStringStream oss;
	oss << cpLogPath_ << "/" << pId_ << "_" << logversion <<
			LogManagerConst::CPLOG_FILE_SUFFIX;
	cpLogIt.add(oss.str());

	return cpLogIt;
}
/*!
	@brief 過去Xログ参照
*/
template <class L>
LogIterator<L> LogManager<L>::createXLogIterator(LogSequentialNumber lsn) {
	ExistLsnMap::reverse_iterator itr = existLsnMap_.rbegin();
	int64_t logversion = itr->first; 
	for (; itr != existLsnMap_.rend(); ++itr) {
		if (itr->second <= lsn) {
			logversion = itr->first;
			break;
		}
	}
	LogIterator<L> xLogIt(this, logversion);
	util::NormalOStringStream oss;
	oss << xLogPath_ << "/" << pId_ << "_" << logversion <<
			LogManagerConst::XLOG_FILE_SUFFIX;
	xLogIt.add(oss.str());
	xLogIt.markXLog();
	flushXLog(LOG_WRITE_WAL_BUFFER, false);
	flushXLog(LOG_FLUSH_FILE, false);
	return xLogIt;
}

template <class L>
LogIterator<L> LogManager<L>::createChunkDataLogIterator() {
	std::vector<int32_t> chunkLogs;
	util::NormalOStringStream oss1;
	oss1 << LogManagerConst::INCREMENTAL_FILE_SUFFIX <<
			LogManagerConst::CPLOG_FILE_SUFFIX;
	UtilFile::getFileNumbers(
			cpLogPath_.c_str(), oss1.str().c_str(), NULL, chunkLogs);

	LogIterator<L> chunkIt(this, 0);
	for (const auto& ver : chunkLogs) {
		util::NormalOStringStream oss2;
		oss2 << cpLogPath_ << "/" << pId_ << "_" << ver <<
				LogManagerConst::INCREMENTAL_FILE_SUFFIX <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
		chunkIt.add(oss2.str());
	}
	return chunkIt;
}

template <class L>
void LogManager<L>::copyCpLog(
		const char* dirPath, uint8_t* buffer, size_t bufferSize) {
	if (!cpFile_) {
		return;
	}
	try {
		flushCpLog(LOG_WRITE_WAL_BUFFER, false);
		flushCpLog(LOG_FLUSH_FILE, false);
		util::NormalOStringStream oss;
		oss << dirPath << "/" << pId_ << "_" << logversion_ <<
				LogManagerConst::CPLOG_FILE_SUFFIX;
		cpFile_->copyAll(oss.str().c_str(), buffer, bufferSize);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Copy CPLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

template <class L>
void LogManager<L>::copyXLog(
		const char* path, uint8_t* buffer, size_t bufferSize) {
	if (!xFile_) {
		return;
	}
	try {
		flushXLog(LOG_WRITE_WAL_BUFFER, false);
		flushXLog(LOG_FLUSH_FILE, false);
		util::NormalOStringStream oss;
		oss << path << "/" << pId_ << "_" << logversion_ <<
				LogManagerConst::XLOG_FILE_SUFFIX;
		xFile_->copyAll(oss.str().c_str(), buffer, bufferSize);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Copy XLog failed. reason=(" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

template <class L>
bool LogManager<L>::isLongtermSyncLogAvailable() const {
	return !longtermSyncLogErrorFlag_;
}

template <class L>
void LogManager<L>::setLongtermSyncLogError(const std::string &message) {
	longtermSyncLogErrorFlag_ = true;
	longtermSyncLogErrorMessage_ = message;
}

template <class L>
bool LogManager<L>::getLongtermSyncLogErrorFlag() const {
	return longtermSyncLogErrorFlag_;
}

template <class L>
const std::string& LogManager<L>::getLongtermSyncLogErrorMessage() const {
	return longtermSyncLogErrorMessage_;
}

template <class L>
void LogManager<L>::setChild(std::unique_ptr<LogManager> child) {
	child_.reset(child);
}

template <class L>
void LogManager<L>::setDuplicateLogMode(const DuplicateLogMode& mode) {
	xWALBuffer_.initDuplicateStatus();

	const bool force = true;
	setDuplicateStatus(mode.status_, force);

	DuplicateLogMode actualMode = mode;
	actualMode.status_ = duplicateLogMode_.status_;

	duplicateLogMode_ = actualMode;
}

template <class L>
DuplicateLogMode& LogManager<L>::getDuplicateLogMode(){
	return duplicateLogMode_;
}

template <class L>
void LogManager<L>::setDuplicateFile(SimpleFile* file) {
	xWALBuffer_.setDuplicateFile(file);
}

template <class L>
void LogManager<L>::setStopOnDuplicateError(bool stopOnDuplicateError) {
	xWALBuffer_.setStopOnDuplicateError(stopOnDuplicateError);
}

template <class L>
bool LogManager<L>::getStopOnDuplicateError() {
	return xWALBuffer_.getStopOnDuplicateError();
}

template <class L>
void LogManager<L>::setDuplicateStatus(
		DuplicateLogMode::Status status, bool force) {
	DuplicateLogMode::applyDuplicateStatus(
			status, duplicateLogMode_.status_, force);
	DuplicateLogMode::applyDuplicateStatus(status, stats_, force);
}

inline Log& Log::setCheckSum() {
	checksum_ = static_cast<uint16_t>(
			static_cast<uint32_t>(type_) + static_cast<uint64_t>(chunkId_) +
			static_cast<uint64_t>(groupId_) + static_cast<uint64_t>(lsn_));
	if (dataSize_ > 0) {
		assert(data_);
		checksum_ = static_cast<uint16_t>(
				checksum_ + util::CRC16::calculate(data_, dataSize_));
	}
	return *this;
}
inline uint16_t Log::getCheckSum() const {
	uint16_t checksum = static_cast<uint16_t>(
			static_cast<uint32_t>(type_) + static_cast<uint64_t>(chunkId_) +
			static_cast<uint64_t>(groupId_) + static_cast<uint64_t>(lsn_));
	if (dataSize_ > 0) {
		assert(data_);
		checksum = static_cast<uint16_t>(
				checksum + util::CRC16::calculate(data_, dataSize_));
	}
	return checksum;
}
inline bool Log::checkSum() const {
	if (getCheckSum() == checksum_) {
		return true;
	} else {
		return false;
	}
}

inline Log& Log::setLsn(uint64_t lsn) {
	lsn_ = lsn;
	return *this;
}

inline LogSequentialNumber Log::getLsn() const {
	return lsn_;
}

/*!
	@brief ログエンコード (ストリームへ出力)
*/
template <typename S>
void Log::encode(S& out) {
	out << static_cast<int8_t>(type_);
	out << checksum_;
	out << integrityCheckVal_;
	out << version_;
	out << lsn_;
	out << chunkId_;
	out << groupId_;
	out << offset_;
	out << vacancy_;
	out << dataSize_;
	if (dataSize_ > 0) {
		out.writeAll(data_, dataSize_);
	}
}

/*!
	@brief ログデコード (ストリームから入力)
*/
template <typename S>
void Log::decode(S& in, util::StackAllocator& alloc) {
	int8_t i8val;
	in >> i8val;
	type_ = static_cast<LogType>(i8val);
	in >> checksum_;
	in >> integrityCheckVal_;
	in >> version_;
	in >> lsn_;
	in >> chunkId_;
	in >> groupId_;
	in >> offset_;
	in >> vacancy_;
	in >> dataSize_;
	if (dataSize_ > 0) {
		uint8_t* bytes = static_cast<uint8_t*>(alloc.allocate(dataSize_));
		in.readAll(bytes, dataSize_);
		data_ = bytes;
	}
	else {
		data_ = NULL;
	}
}

/*!
	@brief 連続性確認
*/
inline Log& Log::setIntegrityCheckVal(uint16_t checkVal) {
	integrityCheckVal_ = checkVal;
	return *this;
}
inline uint16_t Log::getIntegrityCheckVal() const {
	return integrityCheckVal_;
}
inline Log& Log::setChunkId(int64_t chunkid) {
	chunkId_ = chunkid;
	return *this;
}

inline int64_t Log::getChunkId() const {
	return chunkId_;
}

inline Log& Log::setGroupId(int64_t groupid) {
	groupId_ = groupid;
	return *this;
}

inline int64_t Log::getGroupId() const{
	return groupId_;
}

inline Log& Log::setOffset(int64_t offset) {
	offset_ = offset;
	return *this;
}

inline int64_t Log::getOffset() const {
	return offset_;
}

inline Log& Log::setVacancy(int8_t vacancy) {
	vacancy_ = vacancy;
	return *this;
}

inline int8_t Log::getVacancy() const {
	return vacancy_;
}

inline Log& Log::setDataSize(int64_t dataSize) {
	dataSize_ = dataSize;
	return *this;
}
inline Log& Log::setData(const void* data) {
	data_ = data;
	return *this;
}

inline int64_t Log::getDataSize() const {
	return dataSize_;
}
inline const void* Log::getData() const {
	return data_;
}

/*!
	@brief ログマネージャ用config
*/
template <class L>
LogManager<L>::Config::Config(ConfigTable &configTable) :
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
		logFileClearCacheInterval_(configTable.getUInt32(
				CONFIG_TABLE_DS_LOG_FILE_CLEAR_CACHE_INTERVAL)),
		lsnBaseFileRetention_(false) {
	if (logWriteMode_ == 0 || logWriteMode_ == -1) {
		alwaysFlushOnTxnEnd_ = true;
	}
}

template <class L>
void LogManager<L>::Config::setUpConfigHandler() {
}

template <class L>
void LogManager<L>::Config::operator()(
		ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_DS_RETAINED_FILE_COUNT:
		retainedFileCount_ = value.get<int32_t>();
		break;
	case CONFIG_TABLE_DS_LOG_FILE_CLEAR_CACHE_INTERVAL:
		logFileClearCacheInterval_ = value.get<int32_t>();
		break;
	}
}

template <class L>
uint32_t LogManager<L>::Config::getBlockSize() const {
	return (1 << blockSizeBits_);
}

template <class L>
uint32_t LogManager<L>::Config::getBlockBufferCount() const {
	return (1 << blockBufferCountBits_);
}

template <class L>
uint32_t LogManager<L>::Config::getIndexBufferSize() const {
	return (1 << indexBufferSizeBits_);
}

#endif 
