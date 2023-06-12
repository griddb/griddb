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
#include "util/trace.h"

#include "util/container.h"
#include "util/time.h"
#include "util/net.h"
#include "util/file.h"
#include "util/thread.h"
#include "util/os.h"
#include <iomanip>
#include <functional> 

#define NEW_LOG_FORMAT 

#ifdef NEW_LOG_FORMAT
#include "util/code.h"
#ifndef _WIN32
#include <limits.h>
#endif
#endif

namespace util {

TraceHandler::~TraceHandler() {
}

TraceWriter::~TraceWriter() {
}

TraceRecord::TraceRecord() :
		hostName_(NULL),
		tracerName_(NULL),
		level_(0),
		message_(NULL),
		fileNameLiteral_(NULL),
		functionNameLiteral_(NULL),
		lineNumber_(0),
		cause_(NULL),
		causeInHandling_(NULL) {
}

TraceFormatter::~TraceFormatter() {
}

void TraceFormatter::format(std::ostream &stream, TraceRecord &record) {
	u8string hostName;
	Exception cause;

	if (record.hostName_ == NULL) {
		util::SocketAddress::getHostName(hostName);
		record.hostName_ = hostName.c_str();
	}

	if (record.cause_ == NULL && record.causeInHandling_ != NULL) {
		cause.assign(Exception(
				0, NULL, NULL, NULL, -1, record.causeInHandling_), 1);
		record.cause_ = &cause;
	}

	if (record.namedErrorCode_.isEmpty() && record.cause_ != NULL) {
		record.namedErrorCode_ = record.cause_->getNamedErrorCode();
	}

	formatFiltered(stream, record);
	record.cause_ = NULL;
}

void TraceFormatter::handleTraceFailure(const char8_t *formattingString) {
	try {
		std::cerr << "[CRITICAL] Failed to write trace" << std::endl;
	}
	catch (...) {
	}

	try {
		if (formattingString != NULL && *formattingString != '\0') {
			std::cerr << formattingString << std::endl;
		}
	}
	catch (...) {
	}

	try {
		std::exception e;
		Exception(0, NULL, NULL, NULL, 0, &e, NULL).format(std::cerr);
	}
	catch (...) {
	}
}

void TraceFormatter::escapeControlChars(NormalOStringStream &oss) {
	bool found = false;
	{
		const u8string &str = oss.str();
		for (u8string::const_iterator it = str.begin(); it != str.end(); ++it) {
			if (isControlChar(*it)) {
				found = true;
				break;
			}
		}
	}

	if (found) {
		const u8string str = oss.str();
		oss.str("");

		oss.fill('0');
		oss << std::hex;

		for (u8string::const_iterator it = str.begin(); it != str.end(); ++it) {
			if (isControlChar(*it)) {
				oss << "\\x" << std::setw(2) << static_cast<uint32_t>(*it);
			}
			else {
				oss.put(*it);
			}
		}
	}
}

bool TraceFormatter::isControlChar(char8_t ch) {
	return ((0x0 <= ch && ch <= 0x1f) || ch == 0x7f);
}

void TraceFormatter::formatFiltered(
		std::ostream &stream, const TraceRecord &record) {

	stream << record.dateTime_ << " ";

	if (record.hostName_ != NULL) {
		stream << record.hostName_ << " ";
	}

	stream << Thread::getSelfId() << " ";

	formatLevel(stream, record);

	if (record.tracerName_ != NULL) {
		stream << " " << record.tracerName_;
	}

	if (formatMainErrorCode(stream, record, false, " ")) {
		formatMainErrorCode(stream, record, true, ":");
	}

	formatMainLocation(stream, record, " ");

	if (record.message_ != NULL && *record.message_ != '\0') {
		stream << " : " << record.message_;
	}

	formatCause(stream, record, " ");
}

void TraceFormatter::formatLevel(
		std::ostream &stream, const TraceRecord &record) {
	switch (record.level_) {
	case TraceOption::LEVEL_CRITICAL:
		stream << "CRITICAL";
		break;
	case TraceOption::LEVEL_ERROR:
		stream << "ERROR";
		break;
	case TraceOption::LEVEL_WARNING:
		stream << "WARNING";
		break;
	case TraceOption::LEVEL_INFO:
		stream << "INFO";
		break;
	case TraceOption::LEVEL_DEBUG:
		stream << "DEBUG";
		break;
	default:
		stream << "UNKNOWN_LEVEL(" << record.level_ << ")";
		break;
	}
}

bool TraceFormatter::formatMainErrorCode(
		std::ostream &stream, const TraceRecord &record, bool asSymbol,
		const char8_t *separator) {
	if (asSymbol) {
		if (record.namedErrorCode_.getName() != NULL) {
			stream << separator << record.namedErrorCode_.getName();
			return true;
		}
	}
	else {
		if (!record.namedErrorCode_.isEmpty() || record.cause_ != NULL) {
			stream << separator << record.namedErrorCode_.getCode();
			return true;
		}
	}

	return false;
}

bool TraceFormatter::formatMainLocation(
		std::ostream &stream, const TraceRecord &record,
		const char8_t *separator) {
	bool found = false;

	if (record.fileNameLiteral_ != NULL) {
		stream << separator << record.fileNameLiteral_;
		found = true;
	}

	if (record.functionNameLiteral_ != NULL) {
		stream << (found ? " " : separator) << record.functionNameLiteral_;
		found = true;
	}

	if (record.lineNumber_ > 0) {
		stream << (found ? " " : separator) << "line=" << record.lineNumber_;
		found = true;
	}

	return found;
}

bool TraceFormatter::formatCause(
		std::ostream &stream, const TraceRecord &record,
		const char8_t *separator) {
	const Exception *cause = record.cause_;

	if (cause == NULL) {
		return false;
	}

	stream << separator;

	for (size_t i = 0; i <= cause->getMaxDepth(); i++) {
		if (i > 0) {
			stream << " ";
		}
		stream << "by ";
		cause->formatEntry(stream, i);
	}

	return true;
}

namespace detail {

namespace {
struct NoDigitChecker {
	inline bool operator()(char8_t ch) { return !('0' <= ch && ch <= '9'); }
};
}

FileTraceWriter::FileTraceWriter(
		const char8_t *name, const char8_t *baseDir,
		uint64_t maxFileSize, uint32_t maxFileCount,
		TraceOption::RotationMode rotationMode,
		TraceHandler &tracerHandler, bool prepareFileFirst) :
		name_(strlen(name) == 0 ? "log" : name),
		baseDir_(baseDir),
		mutex_(UTIL_MUTEX_RECURSIVE),
		maxFileSize_(maxFileSize),
		maxFileCount_(maxFileCount),
		rotationMode_(rotationMode),
		traceHandler_(tracerHandler),
		reentrantCount_(0) {
	if (prepareFileFirst) {
		prepareFile(TraceOption::LEVEL_INFO, DateTime::now(false), 0);
		try {
			traceHandler_.startStream();
		}
		catch (...) {
		}
	}
}

FileTraceWriter::~FileTraceWriter() try {
	lastFile_.close();
}
catch (...) {
}

void FileTraceWriter::write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size) {

	LockGuard<Mutex> guard(mutex_);

	if (reentrantCount_ == 0 && prepareFile(level, dateTime, size)) {
		struct ScopedCounter {
			explicit ScopedCounter(size_t &count) : count_(count) { count_++; }
			~ScopedCounter() { count_--; }
			size_t &count_;
		} counter(reentrantCount_);

		try {
			traceHandler_.startStream();
		}
		catch (...) {
			try {
				lastFile_.write(data, size);
			}
			catch (...) {
			}

			try {
				throw;
			}
			catch (std::exception &e) {
				UTIL_RETHROW_UTIL_ERROR(CODE_DEFAULT, e, "");
			}
			throw;
		}
	}

	lastFile_.write(data, size);
}

void FileTraceWriter::getHistory(std::vector<u8string> &history) {
	LockGuard<Mutex> guard(mutex_);
	if (!lastFile_.isClosed()) {
		uint64_t fileSize = lastFile_.tell();
		char8_t buf[2048];

		history.push_back("");
		for (uint64_t pos = 0; pos < fileSize; pos += sizeof(buf)) {
			const int64_t readSize = lastFile_.read(&buf, sizeof(buf), pos);
			const char8_t *it = buf;
			const char8_t *end = it + static_cast<size_t>(readSize);

			while (it < end) {
				const char8_t *sepIt = std::find(it, end, '\n');
				history.back() = history.back() + u8string(it, sepIt);
				if (sepIt == end) {
					break;
				}
				history.push_back("");
				it = sepIt + 1;
			}
		}

		if (history.back().empty()) {
			history.pop_back();
		}
	}
}

void FileTraceWriter::flush() {
	LockGuard<Mutex> guard(mutex_);
	if (!lastFile_.isClosed()) {
		lastFile_.sync();
	}
}

void FileTraceWriter::close() {
	LockGuard<Mutex> guard(mutex_);
	lastFile_.close();
}

bool FileTraceWriter::prepareFile(
		int32_t level, const util::DateTime &dateTime, size_t appendingSize) {
	(void) level;

	if (!lastFile_.isClosed() &&
			static_cast<uint64_t>(lastFile_.tell()) + appendingSize <= maxFileSize_ &&
			lastDate_ != util::DateTime::INITIAL_UNIX_TIME &&
			dateTime.getDifference(
					lastDate_, util::DateTime::FIELD_DAY_OF_MONTH) <= 0) {
		return false;
	}

	const char8_t *const suffix = ".log";

	if (lastFile_.isClosed()) {
		FileSystem::createDirectoryTree(baseDir_.c_str());
	}
	else {
		lastFile_.close();
	}

	u8string lastPath;
	if (rotationMode_ == TraceOption::ROTATION_DAILY) {
		u8string baseName;
		{
			int32_t year, month, monthDay, hour, minute, second, milliSecond;
			const bool asLocalTimeZone = true;
			dateTime.getFields(
					year, month, monthDay, hour, minute, second, milliSecond,
					asLocalTimeZone);
			NormalOStringStream oss;
			oss.fill('0');
			oss <<
					name_ << "-" <<
					std::setw(4) << year <<
					std::setw(2) << month <<
					std::setw(2) << monthDay;
			lastDate_.setFields(year, month, monthDay, 0, 0, 0, 0, asLocalTimeZone);
			baseName = oss.str();
		}
		const size_t dateSize = 8;

		typedef std::pair<uint32_t, uint32_t> Info;
		util::NormalSortedList< Info, std::greater<Info> > list;

		uint32_t nextSubNum = 0;
		Directory directory(baseDir_.c_str());
		for (u8string fileName; directory.nextEntry(fileName);) {

			if (fileName.find(name_) != 0) {
				continue;
			}

			const u8string::iterator beginIt = fileName.begin();
			u8string::iterator lastIt = beginIt + name_.size();
			if (lastIt == fileName.end() || *lastIt != '-') {
				continue;
			}
			++lastIt;

			u8string::iterator it = std::find_if(lastIt, fileName.end(), NoDigitChecker());
			u8string::iterator dateTopIt = lastIt;
			if (it == fileName.end() ||
					(it - lastIt) != static_cast<ptrdiff_t>(dateSize)) {
				continue;
			}
			lastIt += dateSize;
			Info info(util::LexicalConverter<uint32_t>()(
					std::string(dateTopIt, lastIt)), 0);

			if (*lastIt == '-') {
				++lastIt;
				if (*lastIt == '0') {
					continue;
				}

				it = std::find_if(lastIt, fileName.end(), NoDigitChecker());
				const uint32_t subNumMaxDigit = 9;
				if (it == fileName.end() || (it == lastIt) || (it - lastIt) > subNumMaxDigit) {
					continue;
				}

				info.second =
						util::LexicalConverter<uint32_t>()(std::string(lastIt, it));
				if (fileName.find(baseName) == 0) {
					nextSubNum = std::max(nextSubNum, info.second + 1);
				}
				lastIt = it;
			}
			else if (fileName.find(baseName) == 0) {
				nextSubNum = std::max<uint32_t>(nextSubNum, 1);
			}

			if (beginIt + fileName.find(suffix, lastIt - beginIt) != lastIt) {
				continue;
			}

			list.insert(info);
		}

		while (!list.empty() && list.size() >= maxFileCount_ && maxFileCount_ > 0) {
			const Info &info = *(list.end() - 1);
			NormalOStringStream oss;
			oss.fill('0');
			oss << name_ << "-" << std::setw(dateSize) << info.first;
			if (info.second > 0) {
				oss << "-" << info.second;
			}
			oss << suffix;

			u8string path;
			FileSystem::createPath(
					baseDir_.c_str(), oss.str().c_str(), path);
			try {
				FileSystem::removeFile(path.c_str());
			}
			catch (...) {
			}
			list.erase(list.end() - 1);
		}

		{
			NormalOStringStream oss;
			oss << baseName;
			if (nextSubNum > 0) {
				oss << "-" << nextSubNum;
			}
			oss << suffix;
			FileSystem::createPath(
					baseDir_.c_str(), oss.str().c_str(), lastPath);
		}
	}
	else {
		FileSystem::createPath(
				baseDir_.c_str(), (name_ + suffix).c_str(), lastPath);

		bool rotate;
		if (FileSystem::exists(lastPath.c_str())) {
			rotate = true;
		}
		else {
			rotate = false;
		}

		if (rotate) {
			u8string oldPath;
			u8string newPath;
			for (uint32_t i = maxFileCount_; i > 0; i--) {
				oldPath.swap(newPath);
				if (i > 1) {
					NormalOStringStream oss;
					oss << name_ << "-" << (i - 1) << suffix;
					FileSystem::createPath(
							baseDir_.c_str(), oss.str().c_str(), newPath);
				}
				else {
					newPath = lastPath;
				}
				if (!oldPath.empty() && FileSystem::exists(newPath.c_str())) {
					FileSystem::move(newPath.c_str(), oldPath.c_str());
				}
			}
		}
	}

	lastFile_.open(lastPath.c_str(),
			FileFlag::TYPE_READ_WRITE | FileFlag::TYPE_CREATE);

	return true;
}

StdTraceWriter::~StdTraceWriter() {
}

void StdTraceWriter::write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size) {
	(void) level;
	(void) dateTime;
	fwrite(data, sizeof(char8_t), size, file_);
}

void StdTraceWriter::getHistory(std::vector<u8string>&) {
}

void StdTraceWriter::flush() {
	fflush(file_);
}

void StdTraceWriter::close() {
	flush();
}

StdTraceWriter::StdTraceWriter(FILE *file) : file_(file) {
}

StdTraceWriter& StdTraceWriter::getForStdOut() {
	static StdTraceWriter instance(stdout);
	return instance;
}

StdTraceWriter& StdTraceWriter::getForStdErr() {
	static StdTraceWriter instance(stderr);
	return instance;
}

ChainTraceWriter::ChainTraceWriter(
		UTIL_UNIQUE_PTR<TraceWriter> writer1,
		UTIL_UNIQUE_PTR<TraceWriter> writer2) :
		writer1_(writer1.release()),
		writer2_(writer2.release()) {
}

ChainTraceWriter::~ChainTraceWriter() {
}

void ChainTraceWriter::write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size) {
	writer1_->write(level, dateTime, data, size);
	writer2_->write(level, dateTime, data, size);
}

void ChainTraceWriter::getHistory(std::vector<u8string>&) {
}

void ChainTraceWriter::flush() {
	writer1_->flush();
	writer2_->flush();
}

void ChainTraceWriter::close() {
	writer1_->close();
	writer2_->close();
}

ChainTraceWriter& ChainTraceWriter::getForStdOutAndStdErr() {
	static ChainTraceWriter instance(
			UTIL_UNIQUE_PTR<TraceWriter>(UTIL_NEW StdTraceWriter(stdout)),
			UTIL_UNIQUE_PTR<TraceWriter>(UTIL_NEW StdTraceWriter(stderr)));
	return instance;
}

ProxyTraceHandler::ProxyTraceHandler() : proxy_(NULL) {
}

ProxyTraceHandler::~ProxyTraceHandler() {
}

void ProxyTraceHandler::setProxy(TraceHandler *proxy) {
	LockGuard<RWLock::WriteLock> guard(rwLock_.getWriteLock());
	proxy_ = proxy;
}

void ProxyTraceHandler::startStream() {
	LockGuard<RWLock::ReadLock> guard(rwLock_.getReadLock());
	if (proxy_ != NULL) {
		proxy_->startStream();
	}
}

} 

void Tracer::put(
		int32_t level,
		const char8_t *message,
		const SourceSymbolChar *fileNameLiteral,
		const SourceSymbolChar *functionNameLiteral,
		int32_t lineNumber,
		const std::exception *causeInHandling,
		const Exception::NamedErrorCode &namedErrorCode) throw() try {


	if (level < minOutputLevel_) {
		return;
	}

	TraceFormatter *formatter = formatter_;
	assert(formatter != NULL);

	UTIL_UNIQUE_PTR<u8string> pendingData;
	try {
		TraceRecord record;

		record.dateTime_ = DateTime::now(false);
		record.tracerName_ = name_.c_str();
		record.level_ = level;
		record.message_ = message;
		record.fileNameLiteral_ = fileNameLiteral;
		record.functionNameLiteral_ = functionNameLiteral;
		record.lineNumber_ = lineNumber;
		record.causeInHandling_ = causeInHandling;
		record.namedErrorCode_ = namedErrorCode;

		NormalOStringStream oss;
		formatter->format(oss, record);
		formatter->escapeControlChars(oss);
		oss << std::endl;

		TraceWriter *writer = writer_;
		if (writer != NULL) {
			try {
				writer->write(record.level_, record.dateTime_,
						oss.str().c_str(), oss.str().size());
			}
			catch (...) {
				try {
					pendingData.reset(UTIL_NEW u8string(oss.str()));
				}
				catch (...) {
				}
				throw;
			}
		}
	}
	catch (...) {
		try {
			const char8_t *pendingDataPtr = NULL;
			try {
				if (pendingData.get() != NULL) {
					pendingDataPtr = pendingData->c_str();
				}
			}
			catch (...) {
			}

			formatter->handleTraceFailure(pendingDataPtr);
		}
		catch (...) {
		}
	}
}
catch (...) {
	assert(false);
}

void Tracer::setMinOutputLevel(int32_t minOutputLevel) {
	minOutputLevel_ = minOutputLevel;
}

Tracer::Tracer(
		const char8_t *name, TraceWriter *writer, TraceFormatter &formatter) :
		name_(name), writer_(writer), formatter_(&formatter),
		minOutputLevel_(0) {
}

Tracer& TraceManager::resolveTracer(const char8_t *name) {

	LockGuard<Mutex> guard(mutex_);
	TracerMap::iterator it = tracerMap_.find(name);
	if (it != tracerMap_.end()) {
		return *it->second;
	}

	Tracer *tracer = UTIL_NEW Tracer(
			name, getDefaultWriter(outputType_), defaultFormatter_);
	tracer->formatter_ = formatter_;
	tracer->setMinOutputLevel(minOutputLevel_);
	try {
		tracerMap_.insert(std::make_pair(name, tracer));
	}
	catch (...) {
		delete tracer;
		throw;
	}

	return *tracer;
}

Tracer* TraceManager::getTracer(const char8_t *name) {
	LockGuard<Mutex> guard(mutex_);
	TracerMap::iterator it = tracerMap_.find(name);
	if (it != tracerMap_.end()) {
		return &(*it->second);
	} else {
		return NULL;
	}
}

void TraceManager::getAllTracers(std::vector<Tracer*> &tracerList) {
	tracerList.clear();
	LockGuard<Mutex> guard(mutex_);
	tracerList.reserve(tracerMap_.size());
	TracerMap::iterator it = tracerMap_.begin();
	for(; it != tracerMap_.end(); ++it) {
		tracerList.push_back(&(*it->second));
	}
}

void TraceManager::setOutputType(TraceOption::OutputType outputType) {
	LockGuard<Mutex> guard(mutex_);
	TraceWriter *oldWriter = getDefaultWriter(outputType_);
	if (outputType == TraceOption::OUTPUT_ROTATION_FILES &&
			filesWriter_ == NULL) {
		filesWriter_ = UTIL_NEW detail::FileTraceWriter(
				rotationFileName_.c_str(), rotationFilesDirectory_.c_str(),
				maxRotationFileSize_, maxRotationFileCount_, rotationMode_,
				*proxyTraceHandler_, false);
	}

	TraceWriter *newWriter = getDefaultWriter(outputType);
	if (oldWriter != NULL && oldWriter != newWriter) {
		oldWriter->close();
	}

	for (TracerMap::iterator i = tracerMap_.begin();
			i != tracerMap_.end(); ++i) {
		i->second->writer_ = newWriter;
	}

	outputType_ = outputType;
}

void TraceManager::resetFileWriter() {
	delete filesWriter_;
	filesWriter_ = NULL;
}

void TraceManager::setFormatter(TraceFormatter *formatter) {
	LockGuard<Mutex> guard(mutex_);
	formatter_ = (formatter == NULL ? &defaultFormatter_ : formatter);

	for (TracerMap::iterator i = tracerMap_.begin();
			i != tracerMap_.end(); ++i) {
		i->second->formatter_ = formatter_;
	}
}

void TraceManager::setMinOutputLevel(int32_t minOutputLevel) {
	LockGuard<Mutex> guard(mutex_);
	minOutputLevel_ = minOutputLevel;

	for (TracerMap::iterator i = tracerMap_.begin();
			i != tracerMap_.end(); ++i) {
		i->second->setMinOutputLevel(minOutputLevel_);
	}
}

void TraceManager::setRotationFilesDirectory(const char8_t *directory) {
	LockGuard<Mutex> guard(mutex_);
	if (filesWriter_ != NULL) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Rotation file writer has already been created");
	}
	rotationFilesDirectory_ = directory;
}

const char8_t* TraceManager::getRotationFilesDirectory() {
	return rotationFilesDirectory_.c_str();
}

void TraceManager::setRotationFileName(const char8_t *name) {
	LockGuard<Mutex> guard(mutex_);
	if (filesWriter_ != NULL) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Rotation file writer has already been created");
	}
	rotationFileName_ = name;
}

void TraceManager::setMaxRotationFileSize(int32_t maxRotationFileSize) {
	LockGuard<Mutex> guard(mutex_);
	if (filesWriter_ != NULL) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Rotation file writer has already been created");
	}
	maxRotationFileSize_ = maxRotationFileSize;
}

void TraceManager::setMaxRotationFileCount(int32_t maxRotationFileCount) {
	LockGuard<Mutex> guard(mutex_);
	if (filesWriter_ != NULL) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Rotation file writer has already been created");
	}
	maxRotationFileCount_ = maxRotationFileCount;
}

void TraceManager::setRotationMode(
		TraceOption::RotationMode rotationMode) {
	LockGuard<Mutex> guard(mutex_);
	if (filesWriter_ != NULL) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Rotation file writer has already been created");
	}
	rotationMode_ = rotationMode;
}

void TraceManager::setTraceHandler(TraceHandler *traceHandler) {
	proxyTraceHandler_->setProxy(traceHandler);
}

void TraceManager::getHistory(std::vector<u8string> &history) {
	LockGuard<Mutex> guard(mutex_);
	TraceWriter *writer = getDefaultWriter(outputType_);
	if (writer != NULL) {
		writer->getHistory(history);
	}
}

void TraceManager::flushAll() {
	LockGuard<Mutex> guard(mutex_);
	TraceWriter *writer = getDefaultWriter(outputType_);
	if (writer != NULL) {
		writer->flush();
	}
}

TraceManager& TraceManager::getInstance() {
	static TraceManager instance;
	return instance;
}

TraceManager::TraceManager() :
		filesWriter_(NULL),
		outputType_(TraceOption::OUTPUT_STDERR),
		formatter_(&defaultFormatter_),
		minOutputLevel_(0),
		rotationFilesDirectory_("log"),
		maxRotationFileSize_(1024 * 1024),
		maxRotationFileCount_(10),
		rotationMode_(TraceOption::ROTATION_SEQUENTIAL),
		proxyTraceHandler_(UTIL_NEW detail::ProxyTraceHandler()) {
}

TraceManager::~TraceManager() try {
	delete filesWriter_;
	for (TracerMap::iterator i = tracerMap_.begin();
			i != tracerMap_.end(); ++i) {
		delete i->second;
	}
}
catch (...) {
}

TraceWriter* TraceManager::getDefaultWriter(TraceOption::OutputType type) {
	switch (type) {
	case TraceOption::OUTPUT_ROTATION_FILES:
		return filesWriter_;
	case TraceOption::OUTPUT_STDOUT:
		return &detail::StdTraceWriter::getForStdOut();
	case TraceOption::OUTPUT_STDERR:
		return &detail::StdTraceWriter::getForStdErr();
	case TraceOption::OUTPUT_STDOUT_AND_STDERR:
		return &detail::ChainTraceWriter::getForStdOutAndStdErr();
	case TraceOption::OUTPUT_NONE:
		return NULL;
	default:
		return NULL;
	}
}

const char8_t* TraceManager::outputLevelToString(int32_t minOutputLevel) {
	switch(minOutputLevel) {
	case util::TraceOption::LEVEL_DEBUG:
        return "DEBUG";
		break;
	case util::TraceOption::LEVEL_INFO:
        return "INFO";
		break;
	case util::TraceOption::LEVEL_WARNING:
        return "WARNING";
		break;
	case util::TraceOption::LEVEL_ERROR:
        return "ERROR";
		break;
	}
    return "UNKNOWN";
}

bool TraceManager::stringToOutputLevel(const std::string &level, int32_t &outputLevel) {
	outputLevel = 0;
	if (level == "DEBUG") {
		outputLevel = util::TraceOption::LEVEL_DEBUG;
	} else if (level == "INFO") {
		outputLevel = util::TraceOption::LEVEL_INFO;
	} else if (level == "WARNING") {
		outputLevel = util::TraceOption::LEVEL_WARNING;
	} else if (level == "ERROR") {
		outputLevel = util::TraceOption::LEVEL_ERROR;
	} else {
		return false;
	}
	return true;
}

namespace detail {
Tracer* TraceUtil::awaitTracerInitialization(Tracer *volatile *tracer) {
	for (;;) {
		util::Tracer *current = *tracer;
		if (current != NULL) {
			return current;
		}
		Thread::yield();
	}
}
} 

} 
