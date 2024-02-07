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
	@brief Implementation of LogManager
*/
#include "log_manager.h"
#include "util/container.h"
#include "util/trace.h"
#include "gs_error.h"
#include <fstream>

const char* LogManagerConst::CPLOG_FILE_SUFFIX = ".cplog";
const char* LogManagerConst::XLOG_FILE_SUFFIX = ".xlog";
const char* LogManagerConst::INCREMENTAL_FILE_SUFFIX = "_incremental";
const uint32_t Log::COMMON_HEADER_SIZE = Log::getCommonHeaderSize();

const uint16_t LogManagerConst::LOGFORMAT_OLD_VERSION = 0x0;
const uint16_t LogManagerConst::LOGFORMAT_BASE_VERSION = 0x1;
const uint16_t LogManagerConst::LOGFORMAT_LATEST_VERSION = 0x1; 

const uint16_t
LogManagerConst::LOGFORMAT_ACCEPTABLE_VERSIONS[] = {
		LOGFORMAT_OLD_VERSION,      
		LOGFORMAT_BASE_VERSION,
		UINT16_MAX /* sentinel */
};

uint32_t LogManagerConst::getLogFormatVersionScore(uint16_t version) {
	switch(version) {
	case LOGFORMAT_BASE_VERSION:
		return 1;
	default:
		return 0;
	}
}

LogManagerStats::LogManagerStats(const Mapper *mapper) :
		table_(mapper),
		timeTable_(table_) {
}

void LogManagerStats::setUpMapper(Mapper &mapper) {
	const ConfigTable::ValueUnit timeUnit = ConfigTable::VALUE_UNIT_DURATION_MS;

	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);

	mapper.bind(LOG_STAT_FLUSH_COUNT, STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_COUNT);
	mapper.bind(LOG_STAT_FLUSH_TIME, STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_TIME)
			.setTimeUnit(timeUnit);

	{
		const ParamValue statErrorValue(-1);
		const uint64_t errorValue = DuplicateLogMode::DUPLICATE_LOG_ERROR;

		mapper.bind(LOG_STAT_DUPLICATE_LOG_MODE, STAT_TABLE_CP_DUPLICATE_LOG)
				.setDefaultValue(statErrorValue, &errorValue);
	}
}

void LogManagerStats::merge(const Table &src) {
	typedef DuplicateLogMode::Status Status;
	const Param statusParam = LOG_STAT_DUPLICATE_LOG_MODE;
	Status status = static_cast<Status>(table_(statusParam).get());

	table_.merge(src);


	table_(LOG_STAT_FLUSH_COUNT).add(src(LOG_STAT_FLUSH_COUNT_ON_CP).get());
	table_(LOG_STAT_FLUSH_COUNT_ON_CP).set(0);

	table_(LOG_STAT_FLUSH_TIME).add(src(LOG_STAT_FLUSH_TIME_ON_CP).get());
	table_(LOG_STAT_FLUSH_TIME_ON_CP).set(0);

	const bool force = false;
	DuplicateLogMode::applyDuplicateStatus(
			static_cast<Status>(src(statusParam).get()), status, force);
	table_(statusParam).set(status);
}

std::string Log::toString() const {
	util::NormalOStringStream oss;
	switch (type_) {
	case CPLog:
	case CPULog:
	case PutChunkDataLog:
		oss << "<" << (int32_t)type_ << ",L" << lsn_ << ",C" << chunkId_ << ",G" << groupId_ <<
		  ",O" << offset_ << ",V" << (int32_t)vacancy_ << ",S" << dataSize_ << ">";
		return oss.str();
	case OBJLog:
		oss << "<" << (int32_t)type_ << ",L" << lsn_ << ",S" << dataSize_ << ">";
		return oss.str();
	default:
		oss << "<" << (int32_t)type_ << ",L" << lsn_ << ",S" << dataSize_ << ">";
		return oss.str();
	}
}


void Log::encode(util::StackAllocator& alloc, util::XArray<uint8_t>& logRecoreds) {
	int64_t dataSize = getDataSize();
	size_t startPos = logRecoreds.size();
	util::XArray<uint8_t> binary(alloc);
	typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
	util::XArrayOutStream<> arrayOut(binary);
	OutStream out(arrayOut);
	encode(out);
	logRecoreds.resize(logRecoreds.size() + sizeof(uint64_t) + binary.size());
	uint64_t logAllSize = binary.size();
	uint8_t* sizeAddr = logRecoreds.data() + startPos;
	uint8_t* addr = sizeAddr + sizeof(uint64_t);
	memcpy(sizeAddr, &logAllSize, sizeof(uint64_t));
	memcpy(addr, binary.data(), binary.size());
}


void DuplicateLogMode::applyDuplicateStatus(
		Status srcStatus, LogManagerStats &stats, bool force) {
	LocalStatValue &destValue =
			stats.table_(LogManagerStats::LOG_STAT_DUPLICATE_LOG_MODE);

	Status destStatus = static_cast<Status>(destValue.get());
	applyDuplicateStatus(srcStatus, destStatus, force);
	destValue.set(destStatus);
}

void DuplicateLogMode::applyDuplicateStatus(
		Status src, Status &dest, bool force) {
	if (dest != DUPLICATE_LOG_ERROR || force) {
		dest = src;
	}
}

/*!
	@brief コンストラクタ
*/
WALBuffer::WALBuffer(PartitionId pId, LogManagerStats &stats) :
		pId_(pId), bufferSize_(1000), logFile_(NULL), duplicateFile_(NULL),
		stopOnDuplicateError_(false),
		duplicateStatus_(DuplicateLogMode::DUPLICATE_LOG_DISABLE),
		position_(0),
		needFlush_(false), stats_(stats) {
	buffer_.reset(UTIL_NEW uint8_t[bufferSize_]);
}

WALBuffer::~WALBuffer() {
}

/*!
	@brief 初期化

		エリア作成など
*/
WALBuffer& WALBuffer::init(SimpleFile* logfile) {
	assert(logfile);
	logFile_ = logfile;
	logFile_->setSize(0);

	buffer_.reset(UTIL_NEW uint8_t[bufferSize_]);
	position_ = 0;
	return *this;
}

void WALBuffer::setDuplicateFile(SimpleFile* file) {
	duplicateFile_ = file;
	if (file) {
		setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_ENABLE);
	}
	else {
		setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_DISABLE);
	}
}

void WALBuffer::setStopOnDuplicateError(bool flag) {
	stopOnDuplicateError_ = flag;
}
bool WALBuffer::getStopOnDuplicateError() {
	return stopOnDuplicateError_;
}

void WALBuffer::initDuplicateStatus() {
	const bool force = true;
	setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_DISABLE, force);
}

DuplicateLogMode::Status WALBuffer::getDuplicateStatus() {
	return duplicateStatus_;
}

SimpleFile* WALBuffer::initDuplicateLogFile(const char* path) {
	if (util::FileSystem::exists(path)) {
		duplicateFile_ = NULL;
		setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_ERROR);
		if (stopOnDuplicateError_) {
			GS_THROW_SYSTEM_ERROR(
					GS_ERROR_LM_CREATE_LOG_FILE_FAILED,
					"Duplicate log file already exists.");
		}
		else {
			GS_TRACE_ERROR(LOG_MANAGER,
					GS_TRACE_LM_WRITE_DUPLICATE_LOG_FAILED,
					"Duplicate log file already exists(continue). ");
			return NULL;
		}
	}
	try {
		duplicateFile_ = UTIL_NEW SimpleFile(path);
		duplicateFile_->open();
	} catch(std::exception &e) {
		handleDuplicateError(e);
		return NULL;
	}
	return duplicateFile_;
}

void WALBuffer::handleDuplicateError(std::exception &e) {
	setDuplicateStatus(DuplicateLogMode::DUPLICATE_LOG_ERROR);
	if (stopOnDuplicateError_) {
		if (duplicateFile_) {
			try {
				duplicateFile_->close();
			}
			catch (...) {
				;
			}
			delete duplicateFile_;
			duplicateFile_ = NULL;
		}
		GS_RETHROW_SYSTEM_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e, "Write duplicate log failed."));
	} else {
		GS_TRACE_ERROR(LOG_MANAGER,
				GS_TRACE_LM_WRITE_DUPLICATE_LOG_FAILED,
				"Write duplicate log failed(continue). " <<
				GS_EXCEPTION_MERGE_MESSAGE(e, "") );
		if (duplicateFile_) {
			try {
				duplicateFile_->close();
			}
			catch (...) {
				;
			}
			delete duplicateFile_;
			duplicateFile_ = NULL;
		}
	}
}

/*!
	@brief ログ追加
	@return (実際にファイルに書込んだかどうかにかかわらず)このログで書込むサイズ
*/
uint64_t WALBuffer::append(const uint8_t* data, size_t dataLength) {
	if (!logFile_) {
		return sizeof(uint64_t) + dataLength;
	}
	int64_t count;
	for (count = 0; count < 2; count++) {
		if (bufferSize_ - position_ >= static_cast<int32_t>(sizeof(uint64_t))) {
			uint64_t length = static_cast<uint64_t>(dataLength);
			memcpy(buffer_.get() + position_, &length, sizeof(uint64_t));
			position_ += static_cast<int32_t>(sizeof(uint64_t));
			break;
		}
		assert(logFile_ != NULL);
		logFile_->write(position_, buffer_.get());
		if (duplicateFile_) {
			try {
				duplicateFile_->write(position_, buffer_.get());
			} catch (std::exception &e) {
				handleDuplicateError(e);
			}
		}
		position_ = 0;
	}
	if (count == 2) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
			"WALBuffer append");
	}
	for (count = 0; count < 2; count++) {
		if (bufferSize_ - position_ >= static_cast<int32_t>(dataLength)) {
			memcpy(buffer_.get() + position_, data, dataLength);
			position_ += static_cast<int32_t>(dataLength);
			break;
		}
		assert(logFile_ != NULL);
		logFile_->write(position_, buffer_.get());
		if (duplicateFile_) {
			try {
				duplicateFile_->write(position_, buffer_.get());
			} catch (std::exception &e) {
				handleDuplicateError(e);
			}
		}
		position_ = 0;
	}
	if (count == 2) {
		assert(position_ == 0);
		assert(logFile_ != NULL);
		logFile_->write(dataLength, data);
		if (duplicateFile_) {
			try {
				duplicateFile_->write(dataLength, data);
			} catch (std::exception &e) {
				handleDuplicateError(e);
			}
		}
	}
	needFlush_ = true;
	return sizeof(uint64_t) + dataLength;
}

/*!
	@brief フラッシュの必要性をチェック
*/
bool WALBuffer::checkNeedFlush() {
	return ( needFlush_ || (position_ > 0) );
}

/*!
	@brief バッファ書き込み＆フラッシュ
	@note 物理ファイルsync中は排他を外す
*/
bool WALBuffer::flush(LockGuardVariant<Locker>* guard, LogFlushMode x, bool byCP) {
	bool flushed = false;
	if (logFile_ != NULL) {
		switch (x) {
		case LOG_WRITE_WAL_BUFFER:
			if (position_ > 0) {
				logFile_->write(position_, buffer_.get());
				if (duplicateFile_) {
					try {
						duplicateFile_->write(position_, buffer_.get());
					} catch (std::exception &e) {
						handleDuplicateError(e);
					}
				}
				position_ = 0;
				needFlush_ = true;
			}
			break;
		case LOG_FLUSH_FILE:
			{
				bool doFlush = needFlush_.exchange(false);
				if (doFlush) {
					if (guard) { 
						guard->release();
					}
					flush(*logFile_, byCP, stats_);
					if (duplicateFile_) {
						try {
							flush(*duplicateFile_, byCP, stats_);
						} catch (std::exception &e) {
							handleDuplicateError(e);
						}
					}
					flushed = true;
					if (guard) {
						guard->acquire();
					}
				}
				break;
			}
		}
	}
	return flushed;
}

void WALBuffer::flush(
		SimpleFile &logfile, bool byCP, LogManagerStats &stats) {
	StatStopwatch &watch = stats.timeTable_(byCP ?
			LogManagerStats::LOG_STAT_FLUSH_TIME_ON_CP :
			LogManagerStats::LOG_STAT_FLUSH_TIME);

	watch.start();
	logfile.flush();
	watch.stop();

	stats.table_(byCP ?
			LogManagerStats::LOG_STAT_FLUSH_COUNT_ON_CP :
			LogManagerStats::LOG_STAT_FLUSH_COUNT)
			.increment();
}

/*!
	@brief 終了処理

		エリア削除など
*/
void WALBuffer::fin() {
	buffer_.reset();
}

/*!
	@brief アクティビティ取得
*/
double WALBuffer::getActivity() const {
	return 1.0;
}

/*!
	@brief リサイズ
*/
bool WALBuffer::resize(int32_t capacity) {
	const bool byCP = false;

	flush(NULL, LOG_WRITE_WAL_BUFFER, byCP);
	flush(NULL, LOG_FLUSH_FILE, byCP);

	bufferSize_ = (int32_t)capacity;
	buffer_.reset(UTIL_NEW uint8_t[bufferSize_]);
	position_ = 0;
	return true;
}

void WALBuffer::setDuplicateStatus(
		DuplicateLogMode::Status status, bool force) {
	DuplicateLogMode::applyDuplicateStatus(status, duplicateStatus_, force);
	DuplicateLogMode::applyDuplicateStatus(status, stats_, force);
}
