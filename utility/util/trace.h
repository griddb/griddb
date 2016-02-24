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
    @brief Definition of Utility of tracers
*/
#ifndef UTIL_TRACE_H_
#define UTIL_TRACE_H_

#include "util/thread.h"
#include <map>
#include <vector>

namespace util {

/*!
    @brief Options for traces.
*/



class TraceOption {
public:




	enum Level {
		LEVEL_DEBUG = 10,
		LEVEL_INFO = 20,
		LEVEL_WARNING = 30,
		LEVEL_ERROR = 40,
		LEVEL_CRITICAL = 50
	};




	enum OutputType {
		OUTPUT_NONE,
		OUTPUT_ROTATION_FILES,
		OUTPUT_STDOUT,
		OUTPUT_STDERR,
		OUTPUT_STDOUT_AND_STDERR
	};

	enum RotationMode {
		ROTATION_SEQUENTIAL,
		ROTATION_DALILY
	};
};

/*!
    @brief Handlers for traces.
*/



class TraceHandler {
public:
	virtual ~TraceHandler();





	virtual void startStream() = 0;
};

/*!
    @brief Writes traces.
*/



class TraceWriter {
public:
	virtual ~TraceWriter();
	virtual void write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size) = 0;
	virtual void getHistory(std::vector<u8string> &history) = 0;
	virtual void flush() = 0;
	virtual void close() = 0;
};

struct TraceRecord {
	TraceRecord();

	DateTime dateTime_;
	const char8_t *hostName_;
	const char8_t *tracerName_;
	int32_t level_;
	Exception::NamedErrorCode namedErrorCode_;
	const char8_t *message_;
	const char8_t *fileNameLiteral_;
	const char8_t *functionNameLiteral_;
	int32_t lineNumber_;
	const Exception *cause_;
	const std::exception *causeInHandling_;
};

class TraceFormatter {
public:
	virtual ~TraceFormatter();

	virtual void format(std::ostream &stream, TraceRecord &record);
	virtual void handleTraceFailure(const char8_t *formattingString);

protected:
	virtual void formatFiltered(
			std::ostream &stream, const TraceRecord &record);

	virtual void formatLevel(std::ostream &stream, const TraceRecord &record);
	virtual bool formatMainErrorCode(
			std::ostream &stream, const TraceRecord &record, bool asSymbol,
			const char8_t *separator);
	virtual bool formatMainLocation(
			std::ostream &stream, const TraceRecord &record,
			const char8_t *separator);
	virtual bool formatCause(std::ostream &stream, const TraceRecord &record,
			const char8_t *separator);
};

class TraceManager;

/*!
    @brief Operates traces.
*/



class Tracer {
public:
	typedef char8_t SourceSymbolChar;





	void put(
			int32_t level,
			const char8_t *message,
			const SourceSymbolChar *fileNameLiteral = NULL,
			const SourceSymbolChar *functionNameLiteral = NULL,
			int32_t lineNumber = -1,
			const std::exception *causeInHandling = NULL,
			const Exception::NamedErrorCode &namedErrorCode =
					Exception::NamedErrorCode()) throw();






	void setMinOutputLevel(int32_t minOutputLevel);

	inline int32_t getMinOutputLevel() const { return minOutputLevel_; }

	inline const char8_t* getName() const { return name_.c_str(); }

private:
	friend class TraceManager;

	Tracer(const char8_t *name,
			TraceWriter *writer, TraceFormatter &formatter);

	const u8string name_;
	Atomic<TraceWriter*> writer_;
	Atomic<TraceFormatter*> formatter_;
	int32_t minOutputLevel_;
};

namespace detail {
class ProxyTraceHandler;
}

/*!
    @brief Manages all Traces.
*/



class TraceManager {
public:






	Tracer& resolveTracer(const char8_t *name);






	Tracer* getTracer(const char8_t *name);




	void getAllTracers(std::vector<Tracer*> &tracerList);





	void setOutputType(TraceOption::OutputType outputType);

	void setFormatter(TraceFormatter *formatter);

	void setMinOutputLevel(int32_t minOutputLevel);






	void setRotationFilesDirectory(const char8_t *directory);

	const char8_t* getRotationFilesDirectory();

	void setRotationFileName(const char8_t *name);






	void setMaxRotationFileSize(int32_t maxRotationFileSize);






	void setMaxRotationFileCount(int32_t maxRotationFileCount);

	void setRotationMode(TraceOption::RotationMode rotationMode);









	void setTraceHandler(TraceHandler *traceHandler);

	void getHistory(std::vector<u8string> &history);

	void flushAll();

	static TraceManager& getInstance();

	static const char8_t* outputLevelToString(int32_t minOutputLevel);

	static bool stringToOutputLevel(const std::string &str, int32_t &outputLevel);

private:
	typedef std::map<u8string, Tracer*> TracerMap;

	TraceManager();
	~TraceManager();

	TraceManager(const TraceManager&);
	TraceManager& operator=(const TraceManager&);

	TraceWriter* getDefaultWriter(TraceOption::OutputType type);

	static detail::ProxyTraceHandler& getProxyTraceHandler();

	Mutex mutex_;

	TraceWriter *filesWriter_;
	TraceFormatter defaultFormatter_;
	TracerMap tracerMap_;

	TraceOption::OutputType outputType_;
	TraceFormatter *formatter_;
	int32_t minOutputLevel_;
	u8string rotationFilesDirectory_;
	u8string rotationFileName_;
	uint64_t maxRotationFileSize_;
	uint32_t maxRotationFileCount_;
	TraceOption::RotationMode rotationMode_;
	detail::ProxyTraceHandler &proxyTraceHandler_;
};

namespace detail {
struct TraceUtil {
	static Tracer* awaitTracerInitialization(Tracer *volatile *tracer);
};

class FileTraceWriter : public TraceWriter {
public:
	FileTraceWriter(
			const char8_t *name, const char8_t *baseDir,
			uint64_t maxFileSize, uint32_t maxFileCount,
			TraceOption::RotationMode rotationMode,
			TraceHandler &tracerHandler, bool prepareFileFirst = true);
	virtual ~FileTraceWriter();
	virtual void write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size);
	virtual void getHistory(std::vector<u8string> &history);
	virtual void flush();
	virtual void close();

private:
	bool prepareFile(
			int32_t level, const util::DateTime &dateTime, size_t appendingSize);

	u8string name_;
	u8string baseDir_;

	Mutex mutex_;
	NamedFile lastFile_;
	util::DateTime lastDate_;

	uint64_t maxFileSize_;
	uint32_t maxFileCount_;
	TraceOption::RotationMode rotationMode_;

	TraceHandler &traceHandler_;
	size_t reentrantCount_;
};


class StdTraceWriter : public TraceWriter {
public:
	StdTraceWriter(FILE *file);
	virtual ~StdTraceWriter();
	virtual void write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size);
	virtual void getHistory(std::vector<u8string> &history);
	virtual void flush();
	virtual void close();

	static StdTraceWriter& getForStdOut();
	static StdTraceWriter& getForStdErr();

private:
	StdTraceWriter(const StdTraceWriter&);
	StdTraceWriter& operator=(const StdTraceWriter&);
	FILE *file_;
};


class ChainTraceWriter : public TraceWriter {
public:
	ChainTraceWriter(
			UTIL_UNIQUE_PTR<TraceWriter> writer1,
			UTIL_UNIQUE_PTR<TraceWriter> writer2);
	virtual ~ChainTraceWriter();
	virtual void write(
			int32_t level, const util::DateTime &dateTime,
			const char8_t *data, size_t size);
	virtual void getHistory(std::vector<u8string> &history);
	virtual void flush();
	virtual void close();

	static ChainTraceWriter& getForStdOutAndStdErr();

private:
	ChainTraceWriter(const ChainTraceWriter&);
	ChainTraceWriter& operator=(const ChainTraceWriter&);
	UTIL_UNIQUE_PTR<TraceWriter> writer1_;
	UTIL_UNIQUE_PTR<TraceWriter> writer2_;
};

class ProxyTraceHandler : public TraceHandler {
public:
	ProxyTraceHandler();
	virtual ~ProxyTraceHandler();

	void setProxy(TraceHandler *proxy);

	virtual void startStream();

private:
	RWLock rwLock_;
	TraceHandler *proxy_;
};
} 

} 

#define UTIL_TRACER_INSTANCE_GETTER(name) \
		util_GetTracerOf_##name

/*!
    @brief Macro of declaration of a tracer.
*/



#define UTIL_TRACER_DECLARE(name) \
	inline util::Tracer& UTIL_TRACER_INSTANCE_GETTER(name) () { \
		static util::Tracer *tracer( \
				&util::TraceManager::getInstance().resolveTracer(#name)); \
		if (tracer == NULL) \
			return *util::detail::TraceUtil::awaitTracerInitialization(&tracer); \
		return *tracer; \
	}

/*!
    @brief Returns tracer of specified name.
*/




#define UTIL_TRACER_RESOLVE(name) \
		UTIL_TRACER_INSTANCE_GETTER(name)()

#define UTIL_TRACER_PUT_BASE_CODED(tracer, level, code, message, cause) \
		if (UTIL_TRACER_RESOLVE(tracer).getMinOutputLevel() <= \
				level) { \
			UTIL_TRACER_RESOLVE(tracer).put( \
				level, \
					UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(message), \
					UTIL_EXCEPTION_POSITION_ARGS, (cause), (code)); \
		} \
		do {} while (false)
#define UTIL_TRACER_PUT_BASE(tracer, level, message, cause) \
		UTIL_TRACER_PUT_BASE_CODED( \
				tracer, level, util::Exception::NamedErrorCode(), message, cause)

#define UTIL_TRACER_PUT_CODED(tracer, level, code, message, cause) \
		UTIL_TRACER_PUT_BASE_CODED( \
				tracer, util::TraceOption::level, code, message, cause)
#define UTIL_TRACER_PUT(tracer, level, message, cause) \
		UTIL_TRACER_PUT_BASE(tracer, util::TraceOption::level, message, cause)

#define UTIL_TRACE_ERROR(tracer, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_ERROR, message, NULL)
#define UTIL_TRACE_WARNING(tracer, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_WARNING, message, NULL)
#define UTIL_TRACE_INFO(tracer, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_INFO, message, NULL)
#define UTIL_TRACE_DEBUG(tracer, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_DEBUG, message, NULL)
#define UTIL_TRACE(tracer, message) \
		UTIL_TRACE_INFO(tracer, message)

#define UTIL_TRACE_EXCEPTION(tracer, cause, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_ERROR, message, &(cause))
#define UTIL_TRACE_EXCEPTION_WARNING(tracer, cause, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_WARNING, message, &(cause))
#define UTIL_TRACE_EXCEPTION_INFO(tracer, cause, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_INFO, message, &(cause))
#define UTIL_TRACE_EXCEPTION_DEBUG(tracer, cause, message) \
		UTIL_TRACER_PUT(tracer, LEVEL_DEBUG, message, &(cause))

UTIL_TRACER_DECLARE(DEFAULT);

#endif
