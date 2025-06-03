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
	@brief Definition of EventEngine
*/
#ifndef EVENT_ENGINE_H_
#define EVENT_ENGINE_H_

#include "data_type.h"
#include "gs_error.h"
#include "socket_wrapper.h"
#include "util/container.h"




class PartitionGroupConfig;
struct NodeDescriptor;
/*!
	@brief EventEngine
*/
class EventEngine {
public:
	friend struct NodeDescriptor;

	struct Config;
	struct Source;
	struct EventRequestOption;
	class BufferManager;
	class ExternalBuffer;
	class Buffer;
	class Event;
	class EventContext;
	class EventHandler;
	class EventCoder;
	class LocalBuffer;
	class LocalEvent;
	class ThreadErrorHandler;
	class Stats;
	struct SocketInfo;
	struct SocketStats;
	struct Tool;

	typedef int32_t EventType;
	typedef int64_t EventMonotonicTime;

	typedef util::FixedSizeAllocator<util::Mutex> FixedSizeAllocator;
	typedef util::VariableSizeAllocator<> VariableSizeAllocator;
	typedef util::XArrayOutStream< util::StdAllocator<
			uint8_t, VariableSizeAllocator> > EventOutStream;
	typedef util::ArrayByteInStream EventByteInStream;
	typedef util::ByteStream<EventOutStream> EventByteOutStream;
	typedef std::pair<uint64_t, uint64_t> BufferId;
	typedef std::pair<SocketInfo, SocketStats> SocketStatsWithInfo;

	/*!
		@brief Event handling mode
	*/
	enum EventHandlingMode {
		HANDLING_PARTITION_SERIALIZED,
		HANDLING_IMMEDIATE,
		HANDLING_QUEUEING
	};

	/*!
		@brief Local event type
	*/
	enum LocalEventType {
		LOCAL_EVENT_WORKER_STARTED,
		LOCAL_EVENT_WORKER_TERMINATING,
		LOCAL_IO_WORKER_STARTED,
		LOCAL_IO_WORKER_TERMINATING
	};

	/*!
		@brief Resume condition at pending
	*/
	enum PendingResumeCondition {
		RESUME_ALWAYS,
		RESUME_AT_PARTITION_CHANGE
	};

	EventEngine(
			const Config &config, const Source &source, const char8_t *name);
	~EventEngine();

	void start();
	void shutdown();
	void waitForShutdown();

	const char8_t* getName() const;

	NodeDescriptor getServerND(NodeDescriptorId ndId);
	NodeDescriptor getServerND(const util::SocketAddress &address);

	NodeDescriptor resolveServerND(const util::SocketAddress &address);
	bool setServerNodeId(
			const util::SocketAddress &address, NodeDescriptorId ndId,
			bool modifiable);

	NodeDescriptor getSelfServerND();
	NodeDescriptor getMulticastND();
	template<typename T> void setUserDataType();

	void setLocalHandler(EventHandler &handler);
	void setCloseEventHandler(EventType type, EventHandler &handler);
	void setDisconnectHandler(EventHandler &handler);
	void setScanEventHandler(EventHandler &handler);
	void setEventCoder(EventCoder &coder);

	 bool send(Event &ev, const NodeDescriptor &nd,
			const EventRequestOption *option = NULL);

	void resetConnection(const NodeDescriptor &nd);
	void acceptConnectionControlInfo(
			const NodeDescriptor &nd, EventByteInStream &in);
	void exportConnectionControlInfo(
			const NodeDescriptor &nd, EventByteOutStream &out);

	void setHandler(EventType type, EventHandler &handler);
	void setHandlingMode(EventType type, EventHandlingMode mode);
	void setUnknownEventHandler(EventHandler &handler);
	void setThreadErrorHandler(ThreadErrorHandler &handler);

	void add(const Event &ev);
	void addTimer(const Event &ev, int32_t timeoutMillis);
	void addPeriodicTimer(const Event &ev, int32_t intervalMillis);
	void addPending(const Event &ev, PendingResumeCondition condition);
	void addScanner(const Event &ev);
	void executeAndWait(EventContext &ec, Event &ev);

	void getStats(Stats &stats);
	bool getStats(PartitionGroupId pgId, Stats &stats);
	void getSocketStats(util::Vector<SocketStatsWithInfo> &statsList);

	EventMonotonicTime getMonotonicTime();


private:
	template<typename T> struct UserDataTraits;

	class NDPool;
	class SocketPool;
	class Dispatcher;
	class Limitter;
	class LocalLimitter;
	class DefaultLimitter;
	struct Manipulator;

	class ClockGenerator;
	class Listener;
	class IOWorker;
	class EventWorker;

	class EventSocket;
	struct SocketReference;
	struct EventProgressWatcher;

	class DefaultEventCoder;

	typedef void* (UserDataConstructor)(void*);
	typedef void (UserDataDestructor)(void*);

	typedef util::LockGuard<util::Mutex> LockGuard;

	typedef std::vector< Event*, util::StdAllocator<
			Event*, VariableSizeAllocator> > EventList;

	typedef std::vector< const Event*, util::StdAllocator<
			const Event*, VariableSizeAllocator> > EventRefList;
	typedef std::pair<EventRefList, int64_t> EventRefCheckList;

	EventEngine(const EventEngine&);
	EventEngine& operator=(const EventEngine&);

	void cleanWorkers();

	void setUserDataType(
			UserDataConstructor &constructor, UserDataDestructor &destructor,
			size_t dataSize);

	VariableSizeAllocator::TraitsType getAllocatorTraits();

	FixedSizeAllocator *fixedAllocator_;
	util::VariableSizeAllocatorPool varAllocPool_;

	const std::string name_;
	UTIL_UNIQUE_PTR<const Config> config_;
	UTIL_UNIQUE_PTR<const PartitionGroupConfig> pgConfig_;
	UTIL_UNIQUE_PTR<Stats> stats_;

	UTIL_UNIQUE_PTR<NDPool> ndPool_;
	UTIL_UNIQUE_PTR<SocketPool> socketPool_;
	UTIL_UNIQUE_PTR<Dispatcher> dispatcher_;
	UTIL_UNIQUE_PTR<DefaultLimitter> limitter_;
	util::SocketAddress selfAddress_;

	UTIL_UNIQUE_PTR<ClockGenerator> clockGenerator_;
	UTIL_UNIQUE_PTR<Listener> listener_;
	IOWorker *ioWorkerList_;
	EventWorker *eventWorkerList_;
	BufferManager *bufferManager_;
};

typedef EventEngine::EventRequestOption EventRequestOption;
typedef EventEngine::Event Event;
typedef EventEngine::EventContext EventContext;
typedef EventEngine::EventHandler EventHandler;
typedef EventEngine::EventCoder EventCoder;
typedef EventEngine::EventType EventType;
typedef EventEngine::EventMonotonicTime EventMonotonicTime;

typedef EventEngine::EventByteInStream EventByteInStream;
typedef EventEngine::EventByteOutStream EventByteOutStream;

/*!
	@brief Represents node descriptor
*/
struct NodeDescriptor {
public:
	friend struct EventEngine::Manipulator;

	struct Body;

	/*!
		@brief ND type
	*/	
	enum Type {
		ND_TYPE_CLIENT,
		ND_TYPE_SERVER,
		ND_TYPE_MULTICAST
	};

	static const NodeDescriptor EMPTY_ND;

	explicit NodeDescriptor(Body &body);
	NodeDescriptor();
	NodeDescriptor(const NodeDescriptor &nd);
	NodeDescriptor& operator=(const NodeDescriptor &nd);
	~NodeDescriptor();

	bool isEmpty() const;
	bool isSelf() const;
	Type getType() const;
	NodeDescriptorId getId() const;
	const util::SocketAddress& getAddress() const;
	template<typename T> T& getUserData() const;

	static const char8_t* typeToString(Type type);
	std::ostream& format(std::ostream &stream) const;

private:
	typedef EventEngine::UserDataConstructor UserDataConstructor;

	void* resolveUserData(UserDataConstructor &constructor) const;

	Body *body_;
};

std::ostream& operator<<(std::ostream &stream, const NodeDescriptor &nd);

/*!
	@brief Represents config of EventEngine
*/
struct EventEngine::Config {
public:
	enum {
		VAR_ALLOC_BASE_COUNT =
				VariableSizeAllocator::TraitsType::FIXED_ALLOCATOR_COUNT
	};

	struct IOMode {
		explicit IOMode(bool serverAcceptable = false, bool clusterAcceptable = false) :
			serverAcceptable_(serverAcceptable), clusterAcceptable_(clusterAcceptable) {
		}

		void set(bool serverAcceptable, bool clusterAcceptable) {
			serverAcceptable_ = serverAcceptable;
			clusterAcceptable_ = clusterAcceptable;
		}
		bool isSomeAcceptable() const {
			if (serverAcceptable_ || clusterAcceptable_) {
				return true;
			} else {
				return false;
			}
		}

		bool serverAcceptable_;
		bool clusterAcceptable_;
	};

	struct ElementValue {
		ElementValue() : value_(-1) {}
		int64_t value_;
	};

	Config();

	Config& setClientNDEnabled(bool enabled);
	Config& setServerNDAutoNumbering(bool enabled);
	Config& setListenAddress(const char8_t *address, uint16_t port);
	Config& setMulticastAddress(const char8_t *address, uint16_t port);
	Config& setServerAddress(const char8_t *address, uint16_t port);
	Config& setConcurrency(uint32_t concurrency);
	Config& setPartitionCount(uint32_t partitionCount);
	Config& setAllAllocatorGroup(util::AllocatorGroupId id);
	Config& setVarAllocFreeSizeLimit(size_t index, int64_t value);
	void applyFreeSizeLimits(VariableSizeAllocator &varAlloc) const;

	bool clientNDEnabled_;
	bool serverNDAutoNumbering_;

	util::SocketAddress listenAddress_;
	util::SocketAddress multicastAddress_;
	Config& setMulticastInterfaceAddress(const char8_t *address, uint16_t port);
	util::SocketAddress multicastInterfaceAddress_;

	util::SocketAddress serverAddress_;

	double ioConcurrencyRate_;
	bool secondaryIOEnabled_;
	bool ioNegotiationMinimum_;
	IOMode plainIOMode_;
	IOMode secureIOMode_;

	bool keepaliveEnabled_;
	int32_t keepaliveIdle_;
	int32_t keepaliveInterval_;
	int32_t keepaliveCount_;
	bool tcpNoDelay_;
	uint32_t backlogSize_;
	int32_t multicastTTL_;
	uint32_t ioBufferSize_;
	int32_t reconnectIntervalMillis_;
	int32_t connectTimeoutMillis_;
	PartitionId filteredPartitionId_;

	int64_t connectionCountLimit_;
	int64_t sendBufferSizeLimit_;

	uint32_t concurrency_;
	uint32_t partitionCount_;

	int64_t eventBufferSizeLimit_;
	int64_t eventBatchSizeLimit_;
	int64_t ioBatchSizeLimit_;

	uint32_t clockIntervalMillis_;
	uint32_t clockCorrectionMillis_;
	uint32_t clockCorrectionMaxTrial_;

	util::AllocatorGroupId allocatorGroupId_;
	util::AllocatorGroupId workAllocatorGroupId_;

	ElementValue varAllocFreeSizeLimits_[VAR_ALLOC_BASE_COUNT];
};

/*!
	@brief EventEngine::Source
*/
struct EventEngine::Source {
	Source(
			VariableSizeAllocator &varAllocator,
			FixedSizeAllocator &fixedAllocator);

	VariableSizeAllocator& resolveVariableSizeAllocator() const;
	FixedSizeAllocator& resolveFixedSizeAllocator() const;

	static SocketFactory& getDefaultSocketFactory();

	VariableSizeAllocator *varAllocator_;
	FixedSizeAllocator *fixedAllocator_;
	BufferManager *bufferManager_;

	SocketFactory *socketFactory_;
	std::pair<SocketFactory*, SocketFactory*> secureSocketFactories_;

private:
	struct Defaults {
		static SocketFactory defaultSocketFactory_;
	};
};

/*!
	@brief Represents the option of event request
*/
struct EventEngine::EventRequestOption {
	EventRequestOption();

	bool isSameEncodingOption(const EventRequestOption &option) const;

	int32_t timeoutMillis_;
	bool onSecondary_;

	Event *eventOnSent_;
};

class EventEngine::BufferManager {
public:
	virtual ~BufferManager();

	virtual size_t getUnitSize() = 0;

	virtual std::pair<BufferId, void*> allocate() = 0;
	virtual void deallocate(const BufferId &id) = 0;

	virtual void* latch(const BufferId &id) = 0;
	virtual void unlatch(const BufferId &id) = 0;
};

class EventEngine::ExternalBuffer {
public:
	class Reader;
	class Writer;

	explicit ExternalBuffer(BufferManager *manager = NULL);
	~ExternalBuffer();

	void clear();
	void swap(ExternalBuffer &another);

	bool isEmpty() const;
	size_t getSize() const;
	size_t getCapacity() const;

private:
	ExternalBuffer(const ExternalBuffer&);
	ExternalBuffer& operator=(const ExternalBuffer&);

	BufferManager *manager_;
	BufferId id_;
	size_t size_;
	size_t capacity_;
};

class EventEngine::ExternalBuffer::Reader {
public:
	explicit Reader(const ExternalBuffer &buffer);
	~Reader();

	const void* data();

private:
	Reader(const Reader&);
	Reader& operator=(const Reader&);

	BufferManager *manager_;
	BufferId id_;
	const void *data_;
};

class EventEngine::ExternalBuffer::Writer {
public:
	explicit Writer(ExternalBuffer &buffer);
	~Writer();

	size_t tryAppend(const void *data, size_t size);

private:
	Writer(const Writer&);
	Writer& operator=(const Writer&);

	ExternalBuffer &bufferRef_;
	ExternalBuffer buffer_;
	void *data_;
};

/*!
	@brief Handles buffer in EventEngine
*/
class EventEngine::Buffer {
public:
	typedef util::XArray< uint8_t,
			util::StdAllocator<uint8_t, VariableSizeAllocator> > XArray;

	explicit Buffer(VariableSizeAllocator &allocator);
	Buffer();
	~Buffer();
	Buffer(const Buffer &another);
	Buffer& operator=(const Buffer &another);

	EventByteInStream getInStream() const;
	EventByteOutStream getOutStream();
	const XArray& getXArray() const;
	XArray& getWritableXArray();

	bool isEmpty() const;
	void clear();

	size_t getSize() const;

	size_t getOffset() const;
	void setOffset(size_t offset);

	size_t getCapacity() const;
	VariableSizeAllocator* getAllocator();

	void transferTo(Buffer &another) const;

	ExternalBuffer& prepareExternal(BufferManager *manager);
	const ExternalBuffer* getExternal() const;
	size_t getExternalOffset() const;
	void setExternalOffset(size_t offset);

private:
	struct Body;

	mutable Body *body_;
	size_t offset_;
	size_t extOffset_;
};

/*!
	@brief Represents event
*/
class EventEngine::Event {
public:
	struct Source;

	/*!
		@brief Number of max extra message
	*/
	enum {
		MAX_EXTRA_MESSAGE_COUNT = 2
	};

	Event(const Source &source, EventType type, PartitionId partitionId);
	Event();
	~Event();
	Event(VariableSizeAllocator &varAllocator, const Event &ev);
	Event(const Event &ev);
	Event& operator=(const Event &ev);

	void resetAttributes(EventType type);

	void resetAttributes(EventType type, PartitionId partitionId,
			const NodeDescriptor &senderND);

	bool isEmpty() const;
	EventType getType() const;
	PartitionId getPartitionId() const;
	const NodeDescriptor& getSenderND() const;
	void setSenderND(const NodeDescriptor &nd);

	EventMonotonicTime getQueuedMonotonicTime() const;
	void setQueuedMonotonicTime(EventMonotonicTime time);
	uint32_t getQueueingCount() const;
	void incrementQueueingCount();

	EventByteInStream getInStream() const;
	EventByteOutStream getOutStream();

	bool isMessageSpecified() const;
	const Buffer& getMessageBuffer() const;
	Buffer& getMessageBuffer();
	void setMessageBuffer(const Buffer &buffer);
	void transferMessageBuffer(const Buffer &src);

	size_t getExtraMessageCount() const;
	void getExtraMessage(size_t index, const void *&ptr, size_t &size) const;
	void addExtraMessage(const void *ptr, size_t size);

	Source getSource();

	uint16_t getTailMetaMessageSize() const;
	void setTailMetaMessageSize(uint16_t size);
	void clearTailMetaMessage();

	bool isPartitionIdSpecified() const;
	void setPartitionIdSpecified(bool specified);
	void setPartitionId(PartitionId pId);

	EventProgressWatcher* getProgressWatcher() const;
	void setProgressWatcher(EventProgressWatcher *progressWatcher);

private:
	typedef std::pair<const void *, size_t> ExtraMessage;

	void mergeExtraMessages();

	VariableSizeAllocator *allocator_;

	EventType type_;
	PartitionId partitionId_;
	NodeDescriptor senderND_;

	EventMonotonicTime queuedMonotonicTime_;
	uint32_t queueingCount_;

	uint16_t tailMetaMessageSize_;
	bool partitionIdSpecified_;

	Buffer messageBuffer_;
	size_t extraMessageCount_;
	ExtraMessage extraMessageList_[MAX_EXTRA_MESSAGE_COUNT];

	EventProgressWatcher *progressWatcher_;
};

/*!
	@brief Event::Source
*/
struct EventEngine::Event::Source {
	Source(const EventEngine::Source &source);
	explicit Source(VariableSizeAllocator &allocator);

	VariableSizeAllocator *allocator_;
};

/*!
	@brief Represents contextual information around the current event
*/
class EventEngine::EventContext {
public:
	friend struct Manipulator;

	struct Source;

	typedef std::vector<bool> PendingPartitionCheckList;

	explicit EventContext(const Source &source);

	operator Event::Source&();

	EventEngine& getEngine();
	VariableSizeAllocator& getVariableSizeAllocator();
	util::StackAllocator& getAllocator();

	const util::DateTime& getHandlerStartTime() const;
	void setHandlerStartTime(const util::DateTime &time);

	EventMonotonicTime getHandlerStartMonotonicTime() const;
	void setHandlerStartMonotonicTime(EventMonotonicTime time);

	bool isPendingPartitionChanged(PartitionId partitionId) const;
	void setPendingPartitionChanged(PartitionId partitionId);
	bool resetPendingPartitionStatus();

	EventProgressWatcher& getProgressWatcher();

	uint32_t getWorkerId() const;
	bool isOnIOWorker() const;
	bool isOnSecondaryIOWorker() const;

	bool isScanningEventAccepted() const;
	void acceptScanningEvent();
	Event* getScannerEvent() const;

	int64_t getLastEventCycle() const;

private:
	EventContext(const EventContext&);
	EventContext& operator=(const EventContext&);

	size_t getCheckListIndex(PartitionId partitionId) const;

	EventEngine *ee_;
	VariableSizeAllocator &varAllocator_;
	util::StackAllocator &allocator_;
	util::DateTime handlerStartTime_;
	EventMonotonicTime handlerStartMonotonicTime_;

	const PartitionId beginPartitionId_;
	PendingPartitionCheckList *pendingPartitionCheckList_;
	bool pendingPartitionChanged_;

	EventProgressWatcher *progressWatcher_;

	Event::Source eventSource_;

	Stats &workerStats_;

	uint32_t workerId_;
	bool onIOWorker_;
	bool onSecondaryIOWorker_;

	const EventList *eventList_;
	const EventRefCheckList *periodicEventList_;

	bool scanningEventAccepted_;
	Event *scannerEvent_;
};

/*!
	@brief EventContext::Source
*/
struct EventEngine::EventContext::Source {
	explicit Source(
			VariableSizeAllocator &varAllocator, util::StackAllocator &allocator,
			Stats &workerStats);

	EventEngine *ee_;
	VariableSizeAllocator *varAllocator_;
	util::StackAllocator *allocator_;
	PartitionId beginPartitionId_;
	PendingPartitionCheckList *pendingPartitionCheckList_;
	EventProgressWatcher *progressWatcher_;
	Stats *workerStats_;
	uint32_t workerId_;
	bool onIOWorker_;
	bool onSecondaryIOWorker_;
	const EventList *eventList_;
	const EventRefCheckList *periodicEventList_;
};

/*!
	@brief Handles event
*/
class EventEngine::EventHandler {
public:
	virtual ~EventHandler();
	virtual void operator()(EventContext &ec, Event &ev) = 0;
};

/*!
	@brief Encodes/Decodes event
*/
class EventEngine::EventCoder {
public:
	virtual ~EventCoder();

	virtual size_t decode(
			EventEngine &ee, LocalBuffer &buffer, Event &ev,
			const NodeDescriptor &nd,
			const util::SocketAddress &socketAddress,
			EventRequestOption &option) = 0;

	virtual int32_t encode(
			EventEngine &ee, Event &ev,
			const void *&ptr, size_t &size, int32_t index,
			const EventRequestOption *option,
			bool followingEventEncoding) = 0;
};

class EventEngine::LocalBuffer {
public:
	LocalBuffer();

	bool isAttached() const;
	void attach(Buffer &base);
	void reset();
	void eraseAll();

	size_t getWritableSize() const;
	void* getWritableAddress();
	bool isWriting() const;

	void startWriting(size_t minSize);
	void finishWriting(size_t size);

	size_t getPendingSize() const;
	EventByteInStream getPendingInStream() const;
	void assignPending(const Buffer &buffer);

	const Buffer& share() const;
	void assignReadable(size_t offset, size_t size);
	void eraseReadable();
	void expandReadable();

	size_t getCapacity() const;
	void adjustCapacity(size_t minSize);

private:
	LocalBuffer(const LocalBuffer&);
	LocalBuffer& operator=(const LocalBuffer&);

	void growCapacity(size_t extraSize);

	size_t getReadableTailPosition() const;
	size_t getPendingHeadPosition() const;
	size_t getPendingTailPosition() const;

	const uint8_t* getData() const;
	uint8_t* getData();

	void errorNoAttach();
	void errorWritingStart();
	void errorWritingFinish(size_t size);
	void errorReadableRange(size_t offset, size_t size);

	Buffer base_;
	Buffer::XArray *storage_;
	size_t pendingOffset_;
	size_t pendingSize_;
	bool writing_;
};

class EventEngine::LocalEvent {
public:
	LocalEvent();

	void reset();
	Event& prepareLocal(const Event::Source &souce);
	Event& getLocal();

	void swap(LocalEvent &ev);
	void transferTo(const Event::Source &souce, LocalEvent &ev) const;

	bool isEmpty() const;
	EventType getType() const;

private:
	LocalEvent(const LocalEvent&);
	LocalEvent& operator=(const LocalEvent&);

	Event& errorEmptyAccess();

	Event base_;
};

/*!
	@brief Handles thread error
*/
class EventEngine::ThreadErrorHandler {
public:
	virtual ~ThreadErrorHandler();
	virtual void operator()(EventContext &ec, std::exception &e) = 0;
};

/*!
	@brief Represents the statistics of EventEngine
*/
class EventEngine::Stats {
public:
	/*!
		@brief Statistics type of EventEngine
	*/
	enum Type {
		MAIN_LISTENER_COUNT,
		MAIN_IO_WORKER_COUNT,
		MAIN_EVENT_WORKER_COUNT,
		MAIN_THREAD_ERROR_COUNT,
		MAIN_TOTAL_ALLOC_SIZE,
		MAIN_FREE_ALLOC_SIZE,
		MAIN_HUGE_ALLOC_COUNT,
		MAIN_HUGE_ALLOC_SIZE,

		ND_SERVER_CREATE_COUNT,
		ND_SERVER_REMOVE_COUNT,
		ND_CLIENT_CREATE_COUNT,
		ND_CLIENT_REMOVE_COUNT,
		ND_MCAST_CREATE_COUNT,
		ND_MCAST_REMOVE_COUNT,
		ND_MODIFY_COUNT,
		ND_MODIFY_ERROR_COUNT,
		ND_USER_DATA_CREATE_COUNT,
		ND_USER_DATA_REMOVE_COUNT,

		SOCKET_CREATE_COUNT,
		SOCKET_REMOVE_COUNT,

		CLOCK_INITIALIZE_COUNT,
		CLOCK_INITIALIZE_ERROR_COUNT,
		CLOCK_UPDATE_ERROR_COUNT,

		LISTENER_BIND_COUNT,
		LISTENER_BIND_ERROR_COUNT,

		WORKER_DISPATCH_COUNT,
		WORKER_TIMED_DISPATCH_COUNT,
		WORKER_DISPATCH_TIMEOUT_MAX,
		WORKER_DISPATCH_TIMEOUT_MIN,
		WORKER_IMMEDIATE_DISPATCH_COUNT,
		WORKER_CLOSING_DISPATCH_COUNT,
		WORKER_WAIT_START_COUNT,
		WORKER_WAIT_ERROR_COUNT,
		WORKER_WAIT_TIME_MAX,
		WORKER_WAIT_TIME_TOTAL,
		WORKER_HANDLED_EVENT_COUNT,
		WORKER_LOCAL_EVENT_COUNT,
		WORKER_UNKNOWN_EVENT_COUNT,
		WORKER_STOPPED_THREAD_COUNT,
		WORKER_HANDLING_THREAD_COUNT,
		WORKER_IDLE_THREAD_COUNT,
		WORKER_WAITING_THREAD_COUNT,

		IO_CYCLE_COUNT,
		IO_MANAGED_SOCKET_COUNT,
		IO_POLLING_SOCKET_COUNT,
		IO_CONNECTING_SOCKET_COUNT,
		IO_SUSPENDING_SOCKET_COUNT,
		IO_REQUEST_CANCEL_COUNT,
		IO_REQUEST_CONNECT_COUNT,
		IO_REQUEST_ADD_COUNT,
		IO_REQUEST_MOVE_COUNT,
		IO_REQUEST_REMOVE_COUNT,
		IO_REQUEST_MODIFY_COUNT,
		IO_REQUEST_SUSPEND_COUNT,
		IO_RECEIVE_BUFFER_SIZE_CURRENT,
		IO_RECEIVE_BUFFER_SIZE_MAX,
		IO_SEND_BUFFER_SIZE_CURRENT,
		IO_SEND_BUFFER_SIZE_MAX,
		IO_ERROR_OPEN_COUNT,
		IO_ERROR_CONNECT_COUNT,
		IO_ERROR_RECEIVE_COUNT,
		IO_ERROR_SEND_COUNT,
		IO_ERROR_OTHER_COUNT,

		EVENT_CYCLE_COUNT,
		EVENT_ACTIVE_ADD_COUNT,
		EVENT_PENDING_ADD_COUNT,
		EVENT_CANCELED_ADD_COUNT,
		EVENT_ACTIVE_QUEUE_SIZE_CURRENT,
		EVENT_ACTIVE_QUEUE_SIZE_MAX,
		EVENT_ACTIVE_BUFFER_SIZE_CURRENT,
		EVENT_ACTIVE_BUFFER_SIZE_MAX,
		EVENT_PENDING_QUEUE_SIZE_CURRENT,
		EVENT_PENDING_QUEUE_SIZE_MAX,

		STATS_LIVE_TYPE_START,

		EVENT_ACTIVE_EXECUTABLE_COUNT =
				STATS_LIVE_TYPE_START, 
		EVENT_ACTIVE_EXECUTABLE_ONE_SHOT_COUNT, 
		EVENT_CYCLE_HANDLING_COUNT, 
		EVENT_CYCLE_HANDLING_AFTER_COUNT, 
		EVENT_CYCLE_HANDLING_AFTER_ONE_SHOT_COUNT, 

		STATS_TYPE_MAX
	};

	Stats();

	int64_t get(Type type) const;

	void set(Type type, int64_t value);

	void updateMax(Type type, int64_t value);
	void updateMin(Type type, int64_t value);

	void increment(Type type);
	void decrement(Type type);

	void merge(Type type, int64_t value, bool sumOnly = false);
	void mergeAll(const Stats &stats, bool sumOnly = false);

	static const char8_t* typeToString(Type type);
	void dump(std::ostream &out) const;

private:
	static const char8_t *const STATS_TYPE_NAMES[STATS_TYPE_MAX];

	static const Type STATS_MAX_INITIAL_VALUE_TYPES[];
	static const Type STATS_MIN_INITIAL_VALUE_TYPES[];

	static const Type STATS_MAX_TYPES[];
	static const Type STATS_MIN_TYPES[];

	template<size_t N>
	static bool findType(const Type (&typeArray)[N], Type type);

	int64_t valueList_[STATS_TYPE_MAX];
};

struct EventEngine::SocketInfo {
	SocketInfo();

	NodeDescriptor nd_;
	util::SocketAddress localAddress_;
	util::SocketAddress remoteAddress_;
	bool multicast_;
};

struct EventEngine::SocketStats {
	SocketStats();

	int64_t dispatchingEventCount_;
	int64_t sendingEventCount_;
	util::DateTime initialTime_;
};

struct EventEngine::Tool {
	static bool getLiveStats(
			EventEngine &ee, PartitionGroupId pgId, Stats::Type type,
			int64_t &value, const EventContext *ec, const Event *ev,
			const EventMonotonicTime *now = NULL);
};


struct NodeDescriptor::Body {
public:
	friend struct NodeDescriptor;
	friend class EventEngine::NDPool;

	/*!
		@brief Socket type
	*/
	enum SocketType {
		ND_SOCKET_SENDER,
		ND_SOCKET_RECEIVER,
		ND_SOCKET_MAX
	};

	enum SocketMode {
		SOCKET_MODE_SINGLE,
		SOCKET_MODE_RESOLVING,
		SOCKET_MODE_FULL,
		SOCKET_MODE_MIXED
	};

	typedef EventEngine::EventSocket EventSocket;
	typedef EventEngine::NDPool NDPool;
	typedef EventEngine::LockGuard LockGuard;
	typedef EventEngine::VariableSizeAllocator VariableSizeAllocator;

	Body(
			NodeDescriptorId id, Type type, bool self, bool onSecondary,
			EventEngine &ee);
	~Body();

	EventEngine& getEngine();
	util::Mutex& getLock();

	Body* findPrimary();
	Body* findSecondary();

	void setSocketMode(LockGuard &ndGuard, SocketMode mode);
	SocketMode getSocketMode(LockGuard &ndGuard);

	bool findSocketType(
			LockGuard &ndGuard, EventSocket *socket, SocketType &type);

	void setSocket(LockGuard &ndGuard, EventSocket *socket, SocketType type);

	EventSocket* getSocket(LockGuard &ndGuard);

	bool removeSocket(LockGuard &ndGuard, EventSocket *socket);

	int32_t acceptEventTimeout(
			LockGuard &ndGuard, EventType type, int32_t timeoutMillis);

private:
	typedef std::pair<EventType, int64_t> TimeMapEntry;

	typedef std::map<
			EventType, int64_t, std::map<EventType, int64_t>::key_compare,
			util::StdAllocator<TimeMapEntry, VariableSizeAllocator> > TimeMap;

	Body(const Body&);
	Body& operator=(const Body&);

	void* resolveUserData(UserDataConstructor &constructor);

	static Body* duplicateReference(Body *body);
	static void removeReference(Body *&body);

	const NodeDescriptorId id_;
	const Type type_;
	const bool self_;
	const bool onSecondary_;
	SocketMode mode_;
	util::Atomic<const util::SocketAddress*> address_;
	EventEngine &ee_;

	util::Atomic<uint64_t> refCount_;
	util::Atomic<void*> userData_;
	Body *freeLink_;

	util::Mutex mutex_;
	TimeMap *eventTimeMap_;
	EventSocket *socketList_[ND_SOCKET_MAX];

	Body *another_;
};

struct EventEngine::Buffer::Body {
	static Body* newInstance(VariableSizeAllocator &allocator);
	static Body* duplicateReference(Body *body);
	static void removeReference(Body *&body);

	explicit Body(VariableSizeAllocator &allocator);

	VariableSizeAllocator& getAllocator();
	ExternalBuffer& prepareExternal(BufferManager *manager);

	Body(const Body&);
	Body& operator=(const Body&);
	uint64_t refCount_;
	XArray storage_;
	ExternalBuffer ext_;
};

class EventEngine::NDPool {
public:
	friend struct NodeDescriptor::Body;

	typedef std::pair<EventType, int64_t> TimeMapEntry;

	typedef std::map<
			EventType, int64_t, std::map<EventType, int64_t>::key_compare,
			util::StdAllocator<TimeMapEntry, VariableSizeAllocator> > TimeMap;

	explicit NDPool(EventEngine &ee);
	~NDPool();

	NodeDescriptor getServerND(NodeDescriptorId ndId);
	NodeDescriptor getServerND(const util::SocketAddress &address);

	NodeDescriptor resolveServerND(const util::SocketAddress &address);

	bool setServerNodeId(
			const util::SocketAddress &address, NodeDescriptorId ndId,
			bool modifiable);

	NodeDescriptor getSelfServerND();
	NodeDescriptor getMulticastND();

	void setUserDataType(
			UserDataConstructor &constructor, UserDataDestructor &destructor,
			size_t dataSize);

	NodeDescriptor allocateClientND();

	int64_t& prepareLastEventTime(TimeMap *&map, EventType type);
	void releaseTimeMap(TimeMap *&map);

	struct SocketAddressLess :
			public std::binary_function<util::SocketAddress, util::SocketAddress, bool> {
		bool operator()(
				const util::SocketAddress &left,
				const util::SocketAddress &right) const;
	};

private:

	struct NDMapValue {
		NDMapValue();

		NodeDescriptor nd_;
		util::SocketAddress *address_;
	};

	typedef std::vector< NodeDescriptor,
			util::StdAllocator<NodeDescriptor, VariableSizeAllocator> > NDList;

	typedef std::pair<util::SocketAddress, NDMapValue> NDMapPair;

	typedef std::map<
			util::SocketAddress,
			NDMapValue,
			SocketAddressLess,
			util::StdAllocator<NDMapPair, VariableSizeAllocator> > NDMap;

	NDPool(const NDPool&);
	NDPool& operator=(const NDPool&);

	NodeDescriptor putServerND(
			const util::SocketAddress &address, NodeDescriptorId ndId,
			bool modifiable, bool &found);

	NodeDescriptor::Body& allocateBody(
			NodeDescriptorId ndId, NodeDescriptor::Type type, bool self);

	void deallocateBody(NodeDescriptor::Body *body);

	void* allocateUserData(UserDataConstructor *constructor);
	void deallocateUserData(void *userData);

	EventEngine &ee_;
	const Config &config_;

	util::Mutex mutex_;
	util::Mutex poolMutex_;
	util::Mutex userDataMutex_;

	VariableSizeAllocator varAllocator_;

	UTIL_UNIQUE_PTR< util::FixedSizeAllocator<> > userDataPool_;
	UserDataConstructor *userDataConstructor_;
	UserDataDestructor *userDataDestructor_;

	util::ObjectPool<TimeMap> timeMapPool_;
	util::ObjectPool<NodeDescriptor::Body> bodyPool_;
	util::ObjectPool<util::SocketAddress> addressPool_;

	NodeDescriptor::Body *bodyFreeLink_;

	NDList serverNDList_;
	NDMap serverNDMap_;

	NodeDescriptor selfServerND_;
	NodeDescriptor multicastND_;

	NodeDescriptorId lastClientNDId_;
};

class EventEngine::SocketPool {
public:
	SocketPool(EventEngine &ee, const Source &eeSource);
	~SocketPool();

	EventSocket* allocate(bool onSecondary);
	void deallocate(EventSocket *socket, bool workerAlive, LockGuard *ndGuard);

	void getSocketStats(util::Vector<SocketStatsWithInfo> &statsList);

	SocketFactory& getFactory(bool secure, bool clientMode);

private:
	typedef std::set<
			EventSocket*, std::set<EventSocket*>::key_compare,
			util::StdAllocator<EventSocket*, VariableSizeAllocator> >
			SocketSet;

	SocketPool(const SocketPool&);
	SocketPool& operator=(const SocketPool&);

	static uint64_t getCurrentUsage(util::ObjectPool<EventSocket> &base);

	EventEngine &ee_;
	const size_t ioConcurrency_;
	const size_t primaryIOConcurrency_;
	const size_t secondaryIOConcurrency_;

	util::Mutex mutex_;
	util::ObjectPool<EventSocket> base_;
	size_t primaryWorkerSeed_;
	size_t secondaryWorkerSeed_;

	VariableSizeAllocator varAlloc_;
	SocketSet socketSet_;

	SocketFactory *factory_;
	std::pair<SocketFactory*, SocketFactory*> secureFactories_;

};

class EventEngine::Dispatcher {
public:
	/*!
		@brief Result type of dispatcher
	*/	
	enum DispatchResult {
		DISPATCH_RESULT_DONE,
		DISPATCH_RESULT_CANCELED,
		DISPATCH_RESULT_CLOSED
	};

	enum ControlEventType {
		CONTROL_PRIMARY = -1,
		CONTROL_SECONDARY = -2,
		CONTROL_MIXED = -3,
		CONTROL_TYPE_END = -4
	};

	explicit Dispatcher(EventEngine &ee);
	~Dispatcher();

	void setHandler(EventType type, EventHandler &handler);
	void setHandlingMode(EventType type, EventHandlingMode mode);
	void setUnknownEventHandler(EventHandler &handler);
	void setThreadErrorHandler(ThreadErrorHandler &handler);

	void setLocalHandler(EventHandler &handler);
	void setCloseEventHandler(EventType type, EventHandler &handler);
	void setDisconnectHandler(EventHandler &handler);
	void setScanEventHandler(EventHandler &handler);
	void setEventCoder(EventCoder &coder);

	bool isScannerAvailable();

	DispatchResult dispatch(
			EventContext &ec, Event &ev, bool queueingSuspended,
			const EventRequestOption &option);

	void requestAndWait(EventContext &ec, Event &ev);
	bool scanOrExecute(EventContext &ec, Event &scannerEv, Event &ev);

	void execute(EventContext &ec, Event &ev);

	EventWorker& selectWorker(const Event &ev, const EventHandlingMode *mode);

	void handleLocalEvent(EventContext &ec, LocalEventType eventType);
	void handleDisconnection(EventContext &ec, Event &ev);

	void handleThreadError(
			EventContext &ec, std::exception &e, bool internalError) throw();
	void handleUnexpectedShutdownError(
			std::exception &e, const NodeDescriptor &nd);

	EventCoder& getEventCoder();


	static bool isSpecialEventType(EventType type);

private:
	struct HandlerEntry {
		HandlerEntry();

		EventHandler *handler_;
		EventHandlingMode mode_;
		bool closeMode_;

	};

	typedef std::vector<HandlerEntry> HandlerTable;

	Dispatcher(const Dispatcher&);
	Dispatcher& operator=(const Dispatcher&);

	void handleEvent(EventContext &ec, Event &ev, HandlerEntry &entry);
	void handleUnknownOrControlEvent(EventContext &ec, Event &ev);

	void setHandlerEntry(EventType type, HandlerEntry entry);
	HandlerEntry* findHandlerEntry(EventType type, bool handlerRequired);
	void filterImmediatePartitionId(Event &ev);

	static void resolveEventTimeout(
			EventContext &ec, Event &ev, int32_t &timeoutMillis);

	EventEngine &ee_;

	HandlerTable handlerTable_;
	EventHandler *localHandler_;
	EventHandler *disconnectHandler_;
	EventHandler *scanEventHandler_;
	EventHandler *unknownEventHandler_;
	ThreadErrorHandler *threadErrorHandler_;

	EventCoder *eventCoder_;

	size_t closeHandlerCount_;

	util::Atomic<uint32_t> workerSelectionSeed_;

	util::Atomic<int64_t> threadErrorCount_;

};

class EventEngine::Limitter {
public:
	struct Info;
	typedef std::vector<Info> InfoList;
	typedef uint32_t LimitId;

	virtual ~Limitter();

	const Info& getInfo(LimitId id) const;
	void reportDiff(LimitId id, int64_t diff);

protected:
	explicit Limitter(const InfoList &infoList);

	virtual void handleChange(bool exceeded) = 0;

private:
	struct Entry;
	typedef std::vector<Entry> EntryList;

	static EntryList makeEntryList(const InfoList &infoList);

	bool checkLimit() const;
	size_t getEntryIndex(LimitId id) const;

	util::Mutex mutex_;
	EntryList entryList_;
};

struct EventEngine::Limitter::Info {
	Info();

	int64_t limit_;
	uint32_t localCount_;
};

struct EventEngine::Limitter::Entry {
	Entry();

	Info info_;
	int64_t value_;
};

class EventEngine::LocalLimitter {
public:
	typedef Limitter::LimitId LimitId;

	LocalLimitter();

	void initialize(Limitter &limitter, LimitId id);

	void reportDiff(int64_t diff);
	int64_t getValue() const;

private:
	LocalLimitter(const LocalLimitter&);
	LocalLimitter& operator=(const LocalLimitter&);

	static int64_t resolveLocalLimit(const Limitter::Info &info);

	Limitter *limitter_;
	LimitId id_;
	int64_t value_;
	int64_t localLimit_;
};

class EventEngine::DefaultLimitter : public Limitter {
public:
	enum DefaultId {
		LIMIT_ID_EVENT_BUFFER,
		LIMIT_ID_SEND_BUFFER,

		LIMIT_ID_MAX
	};

	explicit DefaultLimitter(EventEngine &ee);
	virtual ~DefaultLimitter();

	void setUpLocal(DefaultId id, LocalLimitter &localLimitter);

protected:
	virtual void handleChange(bool exceeded);

private:
	static InfoList makeInfoList(const Config &config);

	EventEngine &ee_;
};

struct EventEngine::Manipulator {
	static NodeDescriptor::Body& getNDBody(
			const NodeDescriptor &nd, bool onSecondary);
	static NodeDescriptor::Body* findNDBody(
			const NodeDescriptor &nd, bool onSecondary);
	static util::SocketAddress resolveListenAddress(const Config &config);
	static bool isListenerEnabled(const Config &config);
	static uint32_t getIOConcurrency(const Config &config, bool withSecondary);
	static size_t resolveBatchSizeLimit(const Config &config, bool forIO);
	static Stats& getStats(const EventContext &ec);
	static void setScanner(EventContext &ec, Event *ev);

	static void mergeAllocatorStats(Stats &stats, VariableSizeAllocator &alloc);
	static void mergeAllocatorStats(Stats &stats, util::StackAllocator &alloc);

	static void prepareMulticastSocket(
			const Config &config, NDPool &ndPool, SocketPool &socketPool);

	static size_t getHandlingEventCount(
			const EventContext &ec, const Event *ev, bool includesStarted,
			bool oneShotOnly);
};

class EventEngine::ClockGenerator : public util::ThreadRunner {
public:
	explicit ClockGenerator(EventEngine &ee);
	virtual ~ClockGenerator();
	virtual void run();

	void start();
	void shutdown();
	void waitForShutdown();

	bool isShutdownRequested();

	util::DateTime getCurrentApproximately();

	util::DateTime getCurrent();
	EventMonotonicTime getMonotonicTime();

private:
	ClockGenerator(const ClockGenerator&);
	ClockGenerator& operator=(const ClockGenerator&);

	void generateCorrectedTime();

	EventEngine &ee_;
	util::Thread thread_;

	util::Condition condition_;
	util::Atomic<int64_t> correctedTime_;
	util::Atomic<EventMonotonicTime> monotonicTime_;
	bool started_;
	util::Atomic<bool> runnable_;
};

class EventEngine::Listener : public util::ThreadRunner, util::IOPollHandler {
public:
	explicit Listener(EventEngine &ee);
	virtual ~Listener();
	virtual void run();
	virtual void handlePollEvent(util::IOPollBase *io, util::IOPollEvent ev);
	virtual util::File& getFile();

	void start();
	void shutdown();
	void waitForShutdown();

private:
	Listener(const Listener&);
	Listener& operator=(const Listener&);

	EventEngine &ee_;
	util::Thread thread_;
	util::Atomic<bool> runnable_;
	util::Socket listenerSocket_;

	util::IOPollEPoll poll_;
};

class EventEngine::IOWorker : public util::ThreadRunner {
public:
	typedef std::deque< Buffer,
			util::StdAllocator<Buffer, VariableSizeAllocator> > BufferQueue;

	typedef std::pair< Event*, std::pair<uint64_t, uint64_t> > IOEventEntry;

	typedef std::deque< IOEventEntry, util::StdAllocator<
			IOEventEntry, VariableSizeAllocator> > IOEventSubQueue;
	typedef std::deque< IOEventSubQueue*, util::StdAllocator<
			IOEventSubQueue*, VariableSizeAllocator> > IOEventQueue;

	IOWorker();
	virtual ~IOWorker();
	void initialize(EventEngine &ee, uint32_t id, bool secondary);
	virtual void run();

	void start();
	void shutdown();
	void waitForShutdown();

	void suspend(bool enabled);

	bool isShutdown();
	bool isSuspendedLocal();
	EventContext& getLocalEventContext();
	VariableSizeAllocator& getVariableSizeAllocator();

	Buffer& pushBufferQueue(
			BufferQueue *&queue, size_t size, bool toFront = false);
	void popBufferQueue(BufferQueue &queue);
	void releaseBufferQueue(BufferQueue *&queue);

	void pushIOEventQueue(
			IOEventQueue *&queue, BufferQueue *&bufQueue,
			const Event *ev, bool toFront = false);
	void transferIOEventQueue(
			const IOEventQueue &src, IOEventQueue *&dest);
	void popIOEventQueue(
			IOEventQueue &queue, const BufferQueue &bufQueue,
			IOEventSubQueue *&subQueue);
	void releaseIOEventQueue(IOEventQueue *&queue);

	void popIOEventSubQueue(
			IOEventQueue &queue, const Buffer *buf,
			IOEventSubQueue *&subQueue);
	void releaseIOEventSubQueue(IOEventSubQueue *&subQueue);

	bool reconnectSocket(EventSocket *socket, util::IOPollEvent pollEvent);
	bool addSocket(EventSocket *socket, util::IOPollEvent pollEvent);
	void removeSocket(EventSocket *socket);
	void moveSocket(EventSocket *socket);
	void modifySocket(EventSocket *socket, util::IOPollEvent pollEvent);
	void suspendSocket(EventSocket *socket, util::IOPollEvent pollEvent);

	size_t getBatchSizeLimit() const;
	const Stats& getStats() const;
	void mergeExtraStats(Stats &stats);

	void updateReceiveBufferSize(const Buffer &buffer, bool adding);
	void updateSendBufferSize(const Buffer &buffer, bool adding);
	void incrementErrorCount(Stats::Type type);

private:
	enum SocketOperation {
		SOCKET_OPERATION_NONE,
		SOCKET_OPERATION_RECONNECT,
		SOCKET_OPERATION_ADD,
		SOCKET_OPERATION_MOVE,
		SOCKET_OPERATION_REMOVE,
		SOCKET_OPERATION_MODIFY,
		SOCKET_OPERATION_SUSPEND
	};

	enum StatsErrorType {
		STATS_IO_ERROR_BEGIN = Stats::IO_ERROR_OPEN_COUNT,
		STATS_IO_ERROR_END = (Stats::IO_ERROR_OTHER_COUNT + 1)
	};

	typedef std::pair<SocketOperation, util::IOPollEvent> SocketOperationEntry;
	typedef std::pair<EventSocket*, SocketOperationEntry> OperationMapEntry;

	typedef std::map<
			EventSocket*, SocketOperationEntry,
			std::map<EventSocket*, SocketOperationEntry>::key_compare,
			util::StdAllocator<SocketOperationEntry, VariableSizeAllocator> >
			OperationMap;

	typedef std::pair<EventSocket*, bool> SocketMapEntry;

	typedef std::map<
			EventSocket*, bool, std::map<EventSocket*, bool>::key_compare,
			util::StdAllocator<SocketMapEntry, VariableSizeAllocator> >
			SocketMap;

	typedef std::vector< EventSocket*,
			util::StdAllocator<SocketMapEntry, VariableSizeAllocator> >
			SocketList;

	IOWorker(const IOWorker&);
	IOWorker& operator=(const IOWorker&);

	void applyPolling(EventSocket *socket, util::IOPollEvent pollEvent);
	void discardAllSockets();
	void discardSocket(EventSocket *&socket, bool polled);

	util::Mutex mutex_;
	util::Thread thread_;

	EventEngine *ee_;
	uint32_t id_;
	bool secondary_;

	bool runnable_;
	bool suspended_;

	bool suspendedLocal_;
	UTIL_UNIQUE_PTR<VariableSizeAllocator> sharedVarAllocator_;
	UTIL_UNIQUE_PTR<VariableSizeAllocator> localVarAllocator_;
	UTIL_UNIQUE_PTR<util::StackAllocator> allocator_;
	UTIL_UNIQUE_PTR<EventContext> localEventContext_;

	UTIL_UNIQUE_PTR<OperationMap> operationMap_;
	UTIL_UNIQUE_PTR<OperationMap> workingOperationMap_;
	UTIL_UNIQUE_PTR<SocketMap> socketMap_;
	UTIL_UNIQUE_PTR< util::ObjectPool<BufferQueue> > bufferQueuePool_;

	util::IOPollEPoll poll_;

	bool sendSizeLimited_;
	size_t batchSizeLimit_;
	Stats stats_;
	util::Atomic<int64_t> sendBufferSize_;
	util::Atomic<int64_t> errorCount_[
			STATS_IO_ERROR_END - STATS_IO_ERROR_BEGIN];
};

/*!
	@brief Handles worker thread processing event
*/
class EventEngine::EventWorker : public util::ThreadRunner {
public:
	EventWorker();
	virtual ~EventWorker();
	void initialize(EventEngine &ee, uint32_t id);
	virtual void run();

	void start();
	void shutdown();
	void waitForShutdown();

	void add(const Event &ev, int32_t timeoutMillis, int32_t periodicInterval);
	void addPending(const Event &ev, PendingResumeCondition resumeCondition);
	void addScanner(const Event &ev);

	bool removeWatcher(EventProgressWatcher &watcher);

	const Stats& getStats() const;
	void mergeExtraStats(Stats &stats);

	bool getLiveStats(
			Stats::Type type, EventMonotonicTime now, int64_t &value);

private:
	struct ActiveEntry {
		ActiveEntry(int64_t time, int32_t periodicInterval);

		int64_t time_;
		int32_t periodicInterval_;
		Event *ev_;
	};

	struct ActiveEntryLess :
			public std::binary_function<ActiveEntry, ActiveEntry, bool> {
		bool operator()(
				const ActiveEntry &left, const ActiveEntry &right) const;
	};

	template<typename Events>
	struct EventsCleaner {
		EventsCleaner(VariableSizeAllocator &varAllocator, Events &events) :
				varAllocator_(varAllocator), events_(events) {
		}

		~EventsCleaner() {
			try {
				clearEvents(varAllocator_, events_);
			}
			catch (...) {
			}
		}

		VariableSizeAllocator &varAllocator_;
		Events &events_;
	};

	typedef std::deque<
			ActiveEntry, util::StdAllocator<
					ActiveEntry, VariableSizeAllocator> >
			ActiveQueue;

	typedef std::deque< Event*, util::StdAllocator<
			Event*, VariableSizeAllocator> > EventQueue;

	typedef std::vector<
			EventProgressWatcher*,
			util::StdAllocator<
					EventProgressWatcher*, VariableSizeAllocator> > WatcherList;

	typedef EventContext::PendingPartitionCheckList PendingPartitionCheckList;

	EventWorker(const EventWorker&);
	EventWorker& operator=(const EventWorker&);

	EventContext::Source createContextSource(
			util::StackAllocator &allocator, const EventList *eventList
			,
			const EventRefCheckList *periodicEventList
			);

	void scanEvents(EventContext &ec, util::LockGuard<util::Condition> &guard);
	void scanEvents(EventContext &ec, Event &scannerEv,
			util::LockGuard<util::Condition> &guard);

	bool popActiveEvents(
			EventList &eventList, int64_t &nextTimeDiff
			,
			EventRefCheckList &periodicEventList
			);
	bool popPendingEvents(EventContext &ec, EventList &eventList,
			size_t &lastSize, bool checkFirst);

	void addPeriodicEventsAgain(
			int64_t monotonicTime, const Event *ev, int32_t periodicInterval,
			const EventList &eventList, EventRefCheckList &periodicEventList);

	void addDirect(
			int64_t monotonicTime, const Event &ev, int32_t periodicInterval,
			const EventMonotonicTime *queuedTime);

	template<bool Adding> void updateBufferSize(const Event &ev);
	static size_t getEventBufferSize(const Event &ev);

	template<typename EventContainer>
	static void addEvent(
			VariableSizeAllocator &allocator, EventContainer &eventContainer,
			const Event &ev);

	template<typename EventContainer>
	static void clearEvents(
			VariableSizeAllocator &allocator, EventContainer &eventContainer);
	static void clearEvents(
			VariableSizeAllocator &allocator, ActiveQueue &activeQueue);

	size_t getExecutableActiveEventCount(
			EventMonotonicTime now, bool oneShotOnly,
			const util::LockGuard<util::Condition>&);

	EventEngine *ee_;
	uint32_t id_;
	util::Thread thread_;

	UTIL_UNIQUE_PTR<util::StackAllocator> allocator_;

	util::Condition condition_;
	bool started_;
	bool runnable_;

	UTIL_UNIQUE_PTR<VariableSizeAllocator> sharedVarAllocator_;
	UTIL_UNIQUE_PTR<VariableSizeAllocator> localVarAllocator_;
	UTIL_UNIQUE_PTR<ActiveQueue> activeQueue_;
	UTIL_UNIQUE_PTR<EventQueue> pendingQueue_;
	UTIL_UNIQUE_PTR<EventQueue> conditionalQueue_;
	UTIL_UNIQUE_PTR<EventQueue> scannerQueue_;
	UTIL_UNIQUE_PTR<EventProgressWatcher> progressWatcher_;
	UTIL_UNIQUE_PTR<WatcherList> watcherList_;

	PendingPartitionCheckList pendingPartitionCheckList_;

	LocalLimitter bufferSizeLimitter_;
	int64_t bufferSizeSoftLimit_;
	size_t batchSizeLimit_;
	Stats stats_;
};

/*!
	@brief Handles event socket
*/
class EventEngine::EventSocket : public util::IOPollHandler {
public:
	friend struct NodeDescriptor::Body;
	friend class SocketPool;

	EventSocket(EventEngine &ee, IOWorker &parentWorker);
	virtual ~EventSocket();

	virtual void handlePollEvent(util::IOPollBase *io, util::IOPollEvent ev);
	virtual util::File& getFile();

	SocketPool& getParentPool();

	bool isConnectionPending(LockGuard &ndGuard);

	bool openAsClient(LockGuard &ndGuard, const util::SocketAddress &address);
	bool openAsServer(
			util::Socket &acceptedSocket,
			const util::SocketAddress &acceptedAddress);

	bool openAsMulticast(const util::SocketAddress &address,
			const util::SocketAddress *interfaceAddr = NULL);

	void sendNegotiationEvent(
			LockGuard &ndGuard, const Event::Source &eventSource);
	void send(LockGuard &ndGuard, Event &ev, const EventRequestOption *option);

	void shutdown(LockGuard &ndGuard);

	bool reconnectLocal(bool polling);
	void resumeLocal();

	void handleDisconnectionLocal();

	bool addLocal();
	void moveLocal();

	void closeLocal(bool workerAlive, LockGuard *ndGuard) throw();

	SocketInfo getInfo() const;
	SocketStats getStats() const;

	static Event::Source eventToSource(Event &ev);

	void acceptControlInfo(
			LockGuard &ndGuard, const NodeDescriptor &nd,
			EventByteInStream &in);
	void exportControlInfo(LockGuard &ndGuard, EventByteOutStream &out);

private:
	class SizeUpdator;

	typedef IOWorker::BufferQueue BufferQueue;

	typedef IOWorker::IOEventSubQueue IOEventSubQueue;
	typedef IOWorker::IOEventQueue IOEventQueue;

	static const size_t MULTICAST_MAX_PACKET_SIZE;

	EventSocket(const EventSocket&);
	EventSocket& operator=(const EventSocket&);

	static bool isAcceptableIOType(
			const Config &config, const NodeDescriptor &nd, bool secure);

	bool acceptInitialEvent(const Event &ev);
	void initializeND(LockGuard &ndGuard, const NodeDescriptor &nd);
	void dispatchEvent(LocalEvent &ev, const EventRequestOption &option);
	void dispatchEventsOnSent(IOEventSubQueue *&queue);

	bool connect(const util::SocketAddress *address, bool polling);
	void setConnected(bool pending);
	bool receiveLocal(LocalEvent &ev, EventRequestOption &option);
	void finishLocalReceive();
	void sendLocal(IOEventSubQueue *&queue);
	void suspendLocal(bool enabled, util::IOPollEvent extraPollEvent);
	void requestShutdown();

	bool handleIOError(std::exception &e) throw();

	util::IOPollEvent filterPollEvent(util::IOPollEvent ev);
	void setNextPollEvent(
			LockGuard *ndGuard, bool lastActionPending, bool forRead);

	void changeTransportMethod(LockGuard &ndGuard);
	void transferSendBuffer(EventSocket &dest) const;
	void transferIOEventQueue(EventSocket &dest) const;
	void appendToSendBuffer(
			const void *data, size_t size, size_t offset);
	bool adjustExternalSendBuffer(LockGuard &ndGuard, Buffer *&buffer);
	static size_t getSendBufferThreshold();


	EventEngine &ee_;
	IOWorker &parentWorker_;
	Event::Source eventSource_;
	EventCoder &eventCoder_;

	AbstractSocket base_;
	util::SocketAddress address_;
	bool multicast_;
	bool openedAsClient_;
	const bool onSecondary_;

	bool firstEventSent_;
	bool firstEventReceived_;
	bool negotiationEventDone_;
	bool negotiationRejected_;

	bool methodNegotiated_;
	bool methodChanging_;
	bool extraGuardRequired_;
	AbstractSocket::SocketAction lastPendingAction_;

	LocalBuffer receiveBuffer_;
	BufferQueue *receiveBufferQueue_;
	BufferQueue *sendBufferQueue_;
	IOEventQueue *eventQueueOnSent_;

	size_t firstEventQueueSize_;
	int64_t connectStartTime_;
	int64_t lastConnectTime_;
	util::Atomic<bool> shutdownRequested_;
	LocalEvent pendingEvent_;
	EventRequestOption pendingOption_;

	NodeDescriptor nd_;
	NodeDescriptor::Body::SocketType ndSocketPendingType_;

	util::SocketAddress localAddress_;
	SocketStats stats_;
};

class EventEngine::EventSocket::SizeUpdator {
public:
	explicit SizeUpdator(EventSocket &socket);
	~SizeUpdator();

private:
	SizeUpdator(const SizeUpdator&);
	SizeUpdator& operator=(const SizeUpdator&);

	EventSocket &socket_;
};

/*!
	@brief Handles socket reference
*/
struct EventEngine::SocketReference {
public:
	SocketReference(EventSocket *socket, LockGuard *ndGuard);
	~SocketReference();

	SocketReference(SocketReference &another);
	SocketReference& operator=(SocketReference &another);

	EventSocket* get();
	EventSocket* release();
	void reset();

private:
	EventSocket *socket_;
	LockGuard *ndGuard_;
};

/*!
	@brief Watches event progress
*/
struct EventEngine::EventProgressWatcher {
public:
	friend class Dispatcher;

	explicit EventProgressWatcher();
	~EventProgressWatcher();

	static void setLastException(
			EventProgressWatcher *watcher, std::exception &e);

private:
	EventProgressWatcher(const EventProgressWatcher&);
	EventProgressWatcher& operator=(const EventProgressWatcher&);

	void clear();

	static EventProgressWatcher* release(Event &ev);

	static void setHandlerStartTime(
			EventProgressWatcher *watcher, EventMonotonicTime time);
	static void setCompleted(EventProgressWatcher *watcher);

	util::Condition condition_;
	EventMonotonicTime handlerStartTime_;
	bool completed_;
	util::Exception *lastException_;
	uint8_t lastExceptionStorage_[sizeof(util::Exception)];
};

/*!
	@brief Default EventCoder
*/
class EventEngine::DefaultEventCoder : public EventCoder {
public:
	virtual ~DefaultEventCoder();

	virtual size_t decode(
			EventEngine &ee, LocalBuffer &buffer, Event &ev,
			const NodeDescriptor &nd,
			const util::SocketAddress &socketAddress,
			EventRequestOption &option);

	virtual int32_t encode(
			EventEngine &ee, Event &ev,
			const void *&ptr, size_t &size, int32_t index,
			const EventRequestOption *option,
			bool followingEventEncoding);

	static void initializeMessageBuffer(Buffer &buffer);

private:
	typedef uint32_t CheckCode;
	typedef int32_t PortNumber;
	typedef int32_t HeaderType;
	typedef uint32_t BodySize;
	typedef uint16_t OptionType;

	static const CheckCode CODER_CHECK_CODE_VALUE;

	static const HeaderType CODER_NODE_TYPE_SERVER;
	static const HeaderType CODER_NODE_TYPE_CLIENT;
	static const HeaderType CODER_NODE_TYPE_MASK;

	static const HeaderType CODER_OPTION_CODE_MASK;
	static const HeaderType CODER_OPTION_CODE_VALUE;
	static const size_t CODER_OPTION_SIZE_BASE_BITS;

	static const OptionType CODER_OPTION_TYPE_TIMEOUT;

	static const BodySize CODER_MAX_BODY_SIZE;

	static const size_t CODER_BODY_COMMON_PART_SIZE;
	static const size_t CODER_FIXED_COMMON_PART_SIZE;

	static size_t getMaxCommonPartSize();
	static size_t getCommonPartSize(
			const util::SocketAddress &socketAddress, bool pIdSpecified);
	static size_t getAddressSize(const util::SocketAddress &socketAddress);

	static void decodeCheckCode(EventByteInStream &in);
	static BodySize decodeNonCommonBodySize(EventByteInStream &in);

	static HeaderType decodeND(
			EventEngine &ee, EventByteInStream &in, NodeDescriptor *nd,
			util::SocketAddress &ndAddress,
			const util::SocketAddress &baseAddress);

	template<bool PIdSpecified, bool FollowingEncoding>
	static size_t encodeCommonPart(
			Event &ev, Buffer &buffer,
			const util::SocketAddress &selfAddress);

	static void decodeRequestOption(
			EventByteInStream &in, HeaderType headerType,
			EventRequestOption &option, size_t &optionSize);

	static void encodeRequestOption(
			Event &ev, const EventRequestOption *option);

	template<typename T>
	static void putValueUnchecked(uint8_t *&out, const T &value);
};


template<typename T>
struct EventEngine::UserDataTraits {
	inline static void* construct(void *ptr) {
		return new(ptr) T();
	}

	inline static void destruct(void *ptr) {
		static_cast<T*>(ptr)->~T();
	}
};

template<typename T>
inline void EventEngine::setUserDataType() {
	setUserDataType(
			*UserDataTraits<T>::construct, *UserDataTraits<T>::destruct,
			sizeof(T));
}

template<typename T>
inline T& NodeDescriptor::getUserData() const {
	typedef EventEngine::UserDataTraits<T> UserDataTraits;

	return *static_cast<T*>(resolveUserData(*UserDataTraits::construct));
}


inline const char8_t* EventEngine::getName() const {
	return name_.c_str();
}

inline NodeDescriptor EventEngine::getServerND(NodeDescriptorId ndId) {
	return ndPool_->getServerND(ndId);
}

inline NodeDescriptor EventEngine::getServerND(
		const util::SocketAddress &address) {
	return ndPool_->getServerND(address);
}

inline NodeDescriptor EventEngine::resolveServerND(
		const util::SocketAddress &address) {
	return ndPool_->resolveServerND(address);
}

inline bool EventEngine::setServerNodeId(
		const util::SocketAddress &address, NodeDescriptorId ndId,
		bool modifiable) {
	return ndPool_->setServerNodeId(address, ndId, modifiable);
}

inline NodeDescriptor EventEngine::getSelfServerND() {
	return ndPool_->getSelfServerND();
}

inline NodeDescriptor EventEngine::getMulticastND() {
	return ndPool_->getMulticastND();
}

inline void EventEngine::setLocalHandler(EventHandler &handler) {
	dispatcher_->setLocalHandler(handler);
}

inline void EventEngine::setCloseEventHandler(
		EventType type, EventHandler &handler) {
	dispatcher_->setCloseEventHandler(type, handler);
}

inline void EventEngine::setDisconnectHandler(EventHandler &handler) {
	dispatcher_->setDisconnectHandler(handler);
}

inline void EventEngine::setScanEventHandler(EventHandler &handler) {
	dispatcher_->setScanEventHandler(handler);
}

inline void EventEngine::setEventCoder(EventCoder &coder) {
	dispatcher_->setEventCoder(coder);
}

inline void EventEngine::resetConnection(const NodeDescriptor &nd) {
	if (nd.getType() == NodeDescriptor::ND_TYPE_MULTICAST) {
		return;
	}

	for (size_t i = 0; i < 2; i++) {
		const bool onSecondary = (i > 0);
		NodeDescriptor::Body *body = Manipulator::findNDBody(nd, onSecondary);
		if (body == NULL) {
			continue;
		}
		LockGuard guard(body->getLock());

		body->setSocket(guard, NULL, NodeDescriptor::Body::ND_SOCKET_RECEIVER);
		body->setSocket(guard, NULL, NodeDescriptor::Body::ND_SOCKET_SENDER);
	}
}

inline void EventEngine::acceptConnectionControlInfo(
		const NodeDescriptor &nd, EventByteInStream &in) {
	if (nd.getType() != NodeDescriptor::ND_TYPE_CLIENT) {
		assert(false);
		return;
	}

	const bool onSecondary = false;
	NodeDescriptor::Body *body = Manipulator::findNDBody(nd, onSecondary);
	LockGuard guard(body->getLock());

	EventSocket *socket = body->getSocket(guard);
	if (socket != NULL) {
		socket->acceptControlInfo(guard, nd, in);
	}
}

inline void EventEngine::exportConnectionControlInfo(
		const NodeDescriptor &nd, EventByteOutStream &out) {
	if (nd.getType() != NodeDescriptor::ND_TYPE_CLIENT) {
		assert(false);
		return;
	}

	const bool onSecondary = false;
	NodeDescriptor::Body *body = Manipulator::findNDBody(nd, onSecondary);
	LockGuard guard(body->getLock());

	EventSocket *socket = body->getSocket(guard);
	if (socket != NULL) {
		socket->exportControlInfo(guard, out);
	}
}

inline void EventEngine::setHandler(EventType type, EventHandler &handler) {
	dispatcher_->setHandler(type, handler);
}

inline void EventEngine::setHandlingMode(
		EventType type, EventHandlingMode mode) {
	dispatcher_->setHandlingMode(type, mode);
}

inline void EventEngine::setUnknownEventHandler(EventHandler &handler) {
	dispatcher_->setUnknownEventHandler(handler);
}

inline void EventEngine::setThreadErrorHandler(ThreadErrorHandler &handler) {
	dispatcher_->setThreadErrorHandler(handler);
}

inline void EventEngine::add(const Event &ev) {
	dispatcher_->selectWorker(ev, NULL).add(ev, 0, 0);
}

inline void EventEngine::addTimer(const Event &ev, int32_t timeoutMillis) {
	dispatcher_->selectWorker(ev, NULL).add(ev, timeoutMillis, 0);
}

inline void EventEngine::addPeriodicTimer(
		const Event &ev, int32_t intervalMillis) {
	if (intervalMillis <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Invalid periodic interval (value=" << intervalMillis << ")");
	}

	dispatcher_->selectWorker(ev, NULL).add(
			ev, intervalMillis, intervalMillis);
}

inline void EventEngine::addPending(
		const Event &ev, PendingResumeCondition condition) {
	dispatcher_->selectWorker(ev, NULL).addPending(ev, condition);
}

inline void EventEngine::addScanner(const Event &ev) {
	const EventHandlingMode mode = HANDLING_PARTITION_SERIALIZED;
	dispatcher_->selectWorker(ev, &mode).addScanner(ev);
}

inline void EventEngine::executeAndWait(EventContext &ec, Event &ev) {
	dispatcher_->requestAndWait(ec, ev);
}

inline void EventEngine::getStats(Stats &stats) {
	stats = *stats_;

	for (uint32_t i = 0; i < config_->concurrency_; i++) {
		stats.mergeAll(eventWorkerList_[i].getStats(), true);
		eventWorkerList_[i].mergeExtraStats(stats);
	}

	const uint32_t ioConcurrency =
			Manipulator::getIOConcurrency(*config_, true);
	for (uint32_t i = 0; i < ioConcurrency; i++) {
		stats.mergeAll(ioWorkerList_[i].getStats(), true);
		ioWorkerList_[i].mergeExtraStats(stats);
	}
}

inline bool EventEngine::getStats(PartitionGroupId pgId, Stats &stats) {
	if(pgId >= config_->concurrency_) {
		return false;
	}
	stats = eventWorkerList_[pgId].getStats();
	return true;
}

inline void EventEngine::getSocketStats(
		util::Vector<SocketStatsWithInfo> &statsList) {
	socketPool_->getSocketStats(statsList);
}


inline void EventEngine::cleanWorkers() {
	delete[] eventWorkerList_;
	eventWorkerList_ = NULL;

	delete[] ioWorkerList_;
	ioWorkerList_ = NULL;
}

inline void EventEngine::setUserDataType(
		UserDataConstructor &constructor, UserDataDestructor &destructor,
		size_t dataSize) {
	ndPool_->setUserDataType(constructor, destructor, dataSize);
}


inline NodeDescriptor::NodeDescriptor(Body &body) :
		body_(Body::duplicateReference(&body)) {
}

inline NodeDescriptor::NodeDescriptor() : body_(NULL) {
}

inline NodeDescriptor::NodeDescriptor(const NodeDescriptor &nd) :
		body_(Body::duplicateReference(nd.body_)) {
}

inline NodeDescriptor& NodeDescriptor::operator=(const NodeDescriptor &nd) {
	if (this != &nd && body_ != nd.body_) {
		Body::removeReference(body_);
		body_ = Body::duplicateReference(nd.body_);
	}

	return *this;
}

inline NodeDescriptor::~NodeDescriptor() try {
	Body::removeReference(body_);
}
catch (...) {
}

inline bool NodeDescriptor::isEmpty() const {
	return (body_ == NULL);
}

inline bool NodeDescriptor::isSelf() const {
	assert(!isEmpty());
	return body_->self_;
}

inline NodeDescriptor::Type NodeDescriptor::getType() const {
	assert(!isEmpty());
	return body_->type_;
}

inline NodeDescriptorId NodeDescriptor::getId() const {
	assert(!isEmpty());
	return body_->id_;
}

inline const util::SocketAddress& NodeDescriptor::getAddress() const {
	assert(!isEmpty());
	return *(body_->address_.load());
}

inline const char8_t* NodeDescriptor::typeToString(Type type) {
	switch (type) {
	case ND_TYPE_CLIENT:
		return "client";
	case ND_TYPE_SERVER:
		return "server";
	case ND_TYPE_MULTICAST:
		return "multicast";
	default:
		return "(unknown)";
	}
}

inline void* NodeDescriptor::resolveUserData(
		UserDataConstructor &constructor) const {
	assert(!isEmpty());
	return body_->resolveUserData(constructor);
}


inline EventEngine::Source::Source(
		VariableSizeAllocator &varAllocator,
		FixedSizeAllocator &fixedAllocator) :
		varAllocator_(&varAllocator),
		fixedAllocator_(&fixedAllocator),
		bufferManager_(NULL),
		socketFactory_(&getDefaultSocketFactory()),
		secureSocketFactories_() {
}

inline EventEngine::VariableSizeAllocator&
EventEngine::Source::resolveVariableSizeAllocator() const {
	if (varAllocator_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Allocator is empty");
	}

	return *varAllocator_;
}

inline EventEngine::FixedSizeAllocator&
EventEngine::Source::resolveFixedSizeAllocator() const {
	if (fixedAllocator_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Block pool is empty");
	}

	return *fixedAllocator_;
}

inline SocketFactory& EventEngine::Source::getDefaultSocketFactory() {
	return Defaults::defaultSocketFactory_;
}


inline EventEngine::EventRequestOption::EventRequestOption() :
		timeoutMillis_(0),
		onSecondary_(false),
		eventOnSent_(NULL) {
}

inline bool EventEngine::EventRequestOption::isSameEncodingOption(
		const EventRequestOption &option) const {
	return (timeoutMillis_ == option.timeoutMillis_);
}


inline EventEngine::Buffer::Buffer(VariableSizeAllocator &allocator) :
		body_(Body::newInstance(allocator)),
		offset_(0),
		extOffset_(0) {
}

inline EventEngine::Buffer::Buffer() :
		body_(NULL),
		offset_(0),
		extOffset_(0) {
}

inline EventEngine::Buffer::~Buffer() try {
	Body::removeReference(body_);
}
catch (...) {
}

inline EventEngine::Buffer::Buffer(const Buffer &another) :
		body_(Body::duplicateReference(another.body_)),
		offset_(another.offset_),
		extOffset_(another.extOffset_) {
}

inline EventEngine::Buffer& EventEngine::Buffer::operator=(
		const Buffer &another) {
	if (this == &another) {
		return *this;
	}

	Body::removeReference(body_);

	body_ = Body::duplicateReference(another.body_);
	offset_ = another.offset_;
	extOffset_ = another.extOffset_;

	return *this;
}

inline EventByteInStream EventEngine::Buffer::getInStream() const {
	if (body_ == NULL) {
		return EventByteInStream(util::ArrayInStream(NULL, 0));
	}

	const size_t storageSize = body_->storage_.size();
	const size_t offset = std::min(offset_, storageSize);

	return EventByteInStream(util::ArrayInStream(
			body_->storage_.data() + offset, storageSize - offset));
}

inline EventByteOutStream EventEngine::Buffer::getOutStream() {
	XArray &storage = getWritableXArray();
	return EventByteOutStream(EventOutStream(storage));
}

inline const EventEngine::Buffer::XArray&
EventEngine::Buffer::getXArray() const {
	if (body_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Storage not found");
	}

	return body_->storage_;
}

inline EventEngine::Buffer::XArray& EventEngine::Buffer::getWritableXArray() {
	if (body_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Storage not found");
	}
	else if (body_->refCount_ > 1) {

		Buffer another(body_->getAllocator());

		XArray &anotherStorage = another.body_->storage_;
		const size_t size = body_->storage_.size();

		anotherStorage.resize(size);
		memcpy(anotherStorage.data(), body_->storage_.data(), size);

		std::swap(another.body_, body_);
	}

	return body_->storage_;
}

inline bool EventEngine::Buffer::isEmpty() const {
	return (body_ == NULL || body_->storage_.size() <= offset_);
}

inline void EventEngine::Buffer::clear() {
	offset_ = 0;
	extOffset_ = 0;

	if (body_ == NULL) {
		return;
	}
	else if (body_->refCount_ > 1) {
		Buffer another(body_->getAllocator());
		std::swap(another.body_, body_);
	}
	else {
		body_->storage_.clear();
	}
}

inline size_t EventEngine::Buffer::getSize() const {
	if (body_ == NULL) {
		return 0;
	}

	return body_->storage_.size() -
			std::min(offset_, body_->storage_.size());
}

inline size_t EventEngine::Buffer::getOffset() const {
	if (body_ == NULL) {
		return 0;
	}

	return std::min(offset_, body_->storage_.size());
}

inline size_t EventEngine::Buffer::getCapacity() const {
	if (body_ == NULL) {
		return 0;
	}

	return body_->storage_.capacity();
}

inline EventEngine::VariableSizeAllocator* EventEngine::Buffer::getAllocator() {
	if (body_ == NULL) {
		return NULL;
	}

	return &body_->getAllocator();
}

inline void EventEngine::Buffer::transferTo(Buffer &another) const {
	if (body_ == NULL) {
		if (another.body_ != NULL) {
			another.getWritableXArray().clear();
		}
	}
	else {
		const size_t storageSize = body_->storage_.size();

		XArray &anotherStorage = another.getWritableXArray();
		anotherStorage.resize(storageSize);

		memcpy(anotherStorage.data(), body_->storage_.data(), storageSize);
	}
	another.offset_ = offset_;
}

inline void EventEngine::Buffer::setOffset(size_t offset) {
	const size_t storageSize =
			(body_ == NULL ? 0 : body_->storage_.size());
	if (offset > storageSize) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Offset out of range (specified=" << offset <<
				", max=" << storageSize << ")");
	}

	offset_ = offset;
}

inline EventEngine::ExternalBuffer& EventEngine::Buffer::prepareExternal(
		BufferManager *manager) {
	assert(body_ != NULL);
	return body_->prepareExternal(manager);
}

inline const EventEngine::ExternalBuffer*
EventEngine::Buffer::getExternal() const {
	assert(body_ != NULL);
	return (body_->ext_.isEmpty() ? NULL : &body_->ext_);
}

inline size_t EventEngine::Buffer::getExternalOffset() const {
	return extOffset_;
}

inline void EventEngine::Buffer::setExternalOffset(size_t offset) {
	extOffset_ = offset;
}


inline EventEngine::Event::Event(
		const Source &source, EventType type, PartitionId partitionId) :
		allocator_(source.allocator_),
		type_(type),
		partitionId_(partitionId),
		queuedMonotonicTime_(0),
		queueingCount_(0),
		tailMetaMessageSize_(0),
		partitionIdSpecified_(true),
		messageBuffer_(*source.allocator_),
		extraMessageCount_(0),
		progressWatcher_(NULL) {
}

inline EventEngine::Event::Event() :
		allocator_(NULL),
		type_(EventType()),
		partitionId_(PartitionId()),
		queuedMonotonicTime_(0),
		queueingCount_(0),
		tailMetaMessageSize_(0),
		partitionIdSpecified_(false),
		extraMessageCount_(0),
		progressWatcher_(NULL) {
}

inline EventEngine::Event::Event(
		VariableSizeAllocator &varAllocator, const Event &ev) :
		allocator_(&varAllocator),
		type_(ev.type_),
		partitionId_(ev.partitionId_),
		senderND_(ev.senderND_),
		queuedMonotonicTime_(ev.queuedMonotonicTime_),
		queueingCount_(ev.queueingCount_),
		tailMetaMessageSize_(ev.tailMetaMessageSize_),
		partitionIdSpecified_(ev.partitionIdSpecified_),
		messageBuffer_(varAllocator),
		extraMessageCount_(ev.extraMessageCount_),
		progressWatcher_(ev.progressWatcher_) {
	ev.messageBuffer_.transferTo(messageBuffer_);
}

inline EventEngine::Event::~Event() {
}

inline EventEngine::Event::Event(const Event &ev) :
		allocator_(ev.allocator_),
		type_(ev.type_),
		partitionId_(ev.partitionId_),
		senderND_(ev.senderND_),
		queuedMonotonicTime_(ev.queuedMonotonicTime_),
		queueingCount_(ev.queueingCount_),
		tailMetaMessageSize_(ev.tailMetaMessageSize_),
		partitionIdSpecified_(ev.partitionIdSpecified_),
		messageBuffer_(ev.messageBuffer_),
		extraMessageCount_(ev.extraMessageCount_),
		progressWatcher_(ev.progressWatcher_) {

	for (size_t i = 0; i < extraMessageCount_; i++) {
		extraMessageList_[i] = ev.extraMessageList_[i];
	}
	mergeExtraMessages();
}

inline Event& EventEngine::Event::operator=(const Event &ev) {
	if (this == &ev) {
		return *this;
	}

	allocator_ = ev.allocator_;
	type_ = ev.type_;
	partitionId_ = ev.partitionId_;
	senderND_ = ev.senderND_;
	queuedMonotonicTime_ = ev.queuedMonotonicTime_;
	queueingCount_ = ev.queueingCount_;
	tailMetaMessageSize_ = ev.tailMetaMessageSize_;
	partitionIdSpecified_ = ev.partitionIdSpecified_;
	messageBuffer_ = ev.messageBuffer_;
	extraMessageCount_ = ev.extraMessageCount_;
	progressWatcher_ = ev.progressWatcher_;

	for (size_t i = 0; i < extraMessageCount_; i++) {
		extraMessageList_[i] = ev.extraMessageList_[i];
	}
	mergeExtraMessages();

	return *this;
}

inline void EventEngine::Event::resetAttributes(EventType type) {
	type_ = type;
}

inline void EventEngine::Event::resetAttributes(
		EventType type, PartitionId partitionId,
		const NodeDescriptor &senderND) {
	type_ = type;
	partitionId_ = partitionId;
	senderND_ = senderND;
	queueingCount_ = 0;
}

inline bool EventEngine::Event::isEmpty() const {
	return (allocator_ == NULL);
}

inline EventType EventEngine::Event::getType() const {
	return type_;
}

inline PartitionId EventEngine::Event::getPartitionId() const {
	return partitionId_;
}

inline const NodeDescriptor& EventEngine::Event::getSenderND() const {
	return senderND_;
}

inline void EventEngine::Event::setSenderND(const NodeDescriptor &nd) {
	senderND_ = nd;
}

inline EventMonotonicTime EventEngine::Event::getQueuedMonotonicTime() const {
	return queuedMonotonicTime_;
}

inline void EventEngine::Event::setQueuedMonotonicTime(
		EventMonotonicTime time) {
	queuedMonotonicTime_ = time;
}

inline uint32_t EventEngine::Event::getQueueingCount() const {
	return queueingCount_;
}

inline void EventEngine::Event::incrementQueueingCount() {
	queueingCount_++;
}

inline EventByteInStream EventEngine::Event::getInStream() const {
	if (tailMetaMessageSize_ == 0 || getTailMetaMessageSize() == 0) {
		return messageBuffer_.getInStream();
	}

	const Buffer::XArray &storage = messageBuffer_.getXArray();
	const size_t offset = messageBuffer_.getOffset();

	return EventByteInStream(util::ArrayInStream(
			storage.data() + offset,
			storage.size() - offset - getTailMetaMessageSize()));
}

inline EventByteOutStream EventEngine::Event::getOutStream() {
	clearTailMetaMessage();
	return getMessageBuffer().getOutStream();
}

inline bool EventEngine::Event::isMessageSpecified() const {
	return (messageBuffer_.getOffset() > 0);
}

inline const EventEngine::Buffer&
EventEngine::Event::getMessageBuffer() const {
	return messageBuffer_;
}

inline EventEngine::Buffer& EventEngine::Event::getMessageBuffer() {
	assert(!isEmpty());

	if (messageBuffer_.getOffset() == 0) {
		DefaultEventCoder::initializeMessageBuffer(messageBuffer_);
	}

	return messageBuffer_;
}

inline void EventEngine::Event::setMessageBuffer(const Buffer &buffer) {
	messageBuffer_ = buffer;
}

inline void EventEngine::Event::transferMessageBuffer(const Buffer &src) {
	assert(!isEmpty());

	src.transferTo(messageBuffer_);
}

inline size_t EventEngine::Event::getExtraMessageCount() const {
	return extraMessageCount_;
}

inline void EventEngine::Event::getExtraMessage(
		size_t index, const void *&ptr, size_t &size) const {
	if (index >= extraMessageCount_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Index out of range (specified=" << index <<
				", count=" << extraMessageCount_ << ")");
	}

	ptr = extraMessageList_[index].first;
	size = extraMessageList_[index].second;
}

inline void EventEngine::Event::addExtraMessage(const void *ptr, size_t size) {
	if (extraMessageCount_ >= MAX_EXTRA_MESSAGE_COUNT) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Too many extra messages");
	}

	clearTailMetaMessage();

	extraMessageList_[extraMessageCount_].first = ptr;
	extraMessageList_[extraMessageCount_].second = size;

	extraMessageCount_++;
}

inline Event::Source EventEngine::Event::getSource() {
	VariableSizeAllocator *allocator = messageBuffer_.getAllocator();
	assert(allocator != NULL);

	return Event::Source(*allocator);
}

inline uint16_t EventEngine::Event::getTailMetaMessageSize() const {
	return static_cast<uint16_t>(
			std::min<size_t>(messageBuffer_.getSize(), tailMetaMessageSize_));
}

inline void EventEngine::Event::setTailMetaMessageSize(uint16_t size) {
	if (size > messageBuffer_.getSize()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Out of range");
	}

	tailMetaMessageSize_ = size;
}

inline void EventEngine::Event::clearTailMetaMessage() {
	if (tailMetaMessageSize_ == 0) {
		return;
	}

	tailMetaMessageSize_ = getTailMetaMessageSize();
	Buffer::XArray &storage = messageBuffer_.getWritableXArray();
	storage.resize(storage.size() - tailMetaMessageSize_);
	tailMetaMessageSize_ = 0;
}

inline bool EventEngine::Event::isPartitionIdSpecified() const {
	return partitionIdSpecified_;
}

inline void EventEngine::Event::setPartitionIdSpecified(bool specified) {
	partitionIdSpecified_ = specified;
	if (!specified) {
		partitionId_ = UNDEF_PARTITIONID;
	}
}

inline void EventEngine::Event::setPartitionId(PartitionId pId) {
	partitionIdSpecified_ = true;
	partitionId_ = pId;
}

inline EventEngine::EventProgressWatcher*
EventEngine::Event::getProgressWatcher() const {
	return progressWatcher_;
}

inline void EventEngine::Event::setProgressWatcher(
		EventEngine::EventProgressWatcher *progressWatcher) {
	progressWatcher_ = progressWatcher;
}

inline void EventEngine::Event::mergeExtraMessages() {
	if (extraMessageCount_ == 0) {
		return;
	}

	clearTailMetaMessage();

	size_t extraSize = 0;
	for (size_t i = 0; i < extraMessageCount_; i++) {
		extraSize += extraMessageList_[i].second;
	}

	EventByteOutStream out = getOutStream();

	const size_t orgSize = out.base().position();
	out.base().position(orgSize + extraSize);
	out.base().position(orgSize);

	for (size_t i = 0; i < extraMessageCount_; i++) {
		ExtraMessage &message = extraMessageList_[i];
		out.writeAll(message.first, message.second);
		message = ExtraMessage();
	}

	extraMessageCount_ = 0;
}


inline EventEngine::Event::Source::Source(const EventEngine::Source &source) :
		allocator_(&source.resolveVariableSizeAllocator()) {
}

inline EventEngine::Event::Source::Source(VariableSizeAllocator &allocator) :
		allocator_(&allocator) {
}


inline EventEngine::EventContext::EventContext(const Source &source) :
		ee_(source.ee_),
		varAllocator_(*source.varAllocator_),
		allocator_(*source.allocator_),
		handlerStartMonotonicTime_(0),
		beginPartitionId_(source.beginPartitionId_),
		pendingPartitionCheckList_(source.pendingPartitionCheckList_),
		pendingPartitionChanged_(false),
		progressWatcher_(source.progressWatcher_),
		eventSource_(*source.varAllocator_),
		workerStats_(*source.workerStats_),
		workerId_(source.workerId_),
		onIOWorker_(source.onIOWorker_),
		onSecondaryIOWorker_(source.onSecondaryIOWorker_),
		eventList_(source.eventList_)
		,
		periodicEventList_(source.periodicEventList_)
		,
		scanningEventAccepted_(false),
		scannerEvent_(NULL)
{

	if (source.varAllocator_ == NULL || source.allocator_ == NULL ||
			source.workerStats_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Pool allocator or stack allocator or stats is empty");
	}
}

inline EventContext::operator EventEngine::Event::Source&() {
	return eventSource_;
}

inline EventEngine& EventEngine::EventContext::getEngine() {
	if (ee_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Engine is empty");
	}

	return *ee_;
}

inline EventEngine::VariableSizeAllocator&
EventEngine::EventContext::getVariableSizeAllocator() {
	return varAllocator_;
}

inline util::StackAllocator& EventEngine::EventContext::getAllocator() {
	return allocator_;
}

inline const util::DateTime&
EventEngine::EventContext::getHandlerStartTime() const {
	return handlerStartTime_;
}

inline void EventEngine::EventContext::setHandlerStartTime(
		const util::DateTime &time) {
	handlerStartTime_ = time;
}

inline EventMonotonicTime
EventEngine::EventContext::getHandlerStartMonotonicTime() const {
	return handlerStartMonotonicTime_;
}

inline void EventEngine::EventContext::setHandlerStartMonotonicTime(
		EventMonotonicTime time) {
	handlerStartMonotonicTime_ = time;
}

inline bool EventEngine::EventContext::isPendingPartitionChanged(
		PartitionId partitionId) const {
	return (*pendingPartitionCheckList_)[getCheckListIndex(partitionId)];
}

inline void EventEngine::EventContext::setPendingPartitionChanged(
		PartitionId partitionId) {
	(*pendingPartitionCheckList_)[getCheckListIndex(partitionId)] = true;
	pendingPartitionChanged_ = true;
}

inline bool EventEngine::EventContext::resetPendingPartitionStatus() {
	if (pendingPartitionChanged_) {

		pendingPartitionCheckList_->assign(
				pendingPartitionCheckList_->size(), false);
		return true;
	}

	return false;
}

inline EventEngine::EventProgressWatcher&
EventEngine::EventContext::getProgressWatcher() {
	if (progressWatcher_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Watcher is empty");
	}

	return *progressWatcher_;
}

inline uint32_t EventEngine::EventContext::getWorkerId() const {
	return workerId_;
}

inline bool EventEngine::EventContext::isOnIOWorker() const {
	return onIOWorker_;
}

inline bool EventEngine::EventContext::isOnSecondaryIOWorker() const {
	return onSecondaryIOWorker_;
}

inline bool EventEngine::EventContext::isScanningEventAccepted() const {
	return scanningEventAccepted_;
}

inline void EventEngine::EventContext::acceptScanningEvent() {
	if (scannerEvent_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Event not scanning");
	}

	scanningEventAccepted_ = true;
}

inline Event* EventEngine::EventContext::getScannerEvent() const {
	return scannerEvent_;
}

inline int64_t EventEngine::EventContext::getLastEventCycle() const {
	return workerStats_.get(Stats::EVENT_CYCLE_COUNT);
}

inline size_t EventEngine::EventContext::getCheckListIndex(
		PartitionId partitionId) const {

	if (pendingPartitionCheckList_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Check list is empty");
	}

	if (partitionId < beginPartitionId_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Partition ID out of range (specified=" << partitionId <<
				", begin=" << beginPartitionId_ << ")");
	}

	const size_t index = partitionId - beginPartitionId_;
	if (index >= pendingPartitionCheckList_->size()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Partition ID out of range (specified=" << partitionId <<
				", end=" << (beginPartitionId_ +
						pendingPartitionCheckList_->size()) << ")");
	}

	return index;
}


inline EventEngine::EventContext::Source::Source(
		VariableSizeAllocator &varAllocator, util::StackAllocator &allocator,
		Stats &workerStats) :
		ee_(NULL),
		varAllocator_(&varAllocator),
		allocator_(&allocator),
		beginPartitionId_(0),
		pendingPartitionCheckList_(NULL),
		progressWatcher_(NULL),
		workerStats_(&workerStats),
		workerId_(0),
		onIOWorker_(false),
		onSecondaryIOWorker_(false),
		eventList_(NULL)
		,
		periodicEventList_(NULL)
{
}


inline NodeDescriptor::Body::Body(
		NodeDescriptorId id, Type type, bool self, bool onSecondary,
		EventEngine &ee) :
		id_(id),
		type_(type),
		self_(self),
		onSecondary_(onSecondary),
		mode_(SOCKET_MODE_SINGLE),
		ee_(ee),
		freeLink_(NULL),
		eventTimeMap_(NULL),
		another_(NULL) {
	std::fill(socketList_, socketList_ + ND_SOCKET_MAX,
			static_cast<EventSocket*>(NULL));
}

inline NodeDescriptor::Body::~Body() try {
	ee_.ndPool_->deallocateUserData(userData_);
	ee_.ndPool_->releaseTimeMap(eventTimeMap_);
}
catch (...) {
}

inline EventEngine& NodeDescriptor::Body::getEngine() {
	return ee_;
}

inline util::Mutex& NodeDescriptor::Body::getLock() {
	return mutex_;
}

inline NodeDescriptor::Body* NodeDescriptor::Body::findPrimary() {
	return (onSecondary_ ? another_ : this);
}

inline NodeDescriptor::Body* NodeDescriptor::Body::findSecondary() {
	return (onSecondary_ ? this : another_);
}

inline void NodeDescriptor::Body::setSocket(
		LockGuard &ndGuard, EventSocket *socket, SocketType type) {

	EventSocket *&storedSocket = socketList_[type];

	if (storedSocket != NULL) {
		storedSocket->shutdown(ndGuard);
		storedSocket = NULL;
	}

	if (socket != NULL) {
		assert(!onSecondary_ == !socket->onSecondary_);
		socket->initializeND(ndGuard, NodeDescriptor(*findPrimary()));
		storedSocket = socket;
	}
}

inline NodeDescriptor::Body::EventSocket* NodeDescriptor::Body::getSocket(
		LockGuard &ndGuard) {
	(void) ndGuard;

	for (size_t i = 0; i < ND_SOCKET_MAX; i++) {
		if (socketList_[i] != NULL) {
			return socketList_[i];
		}
	}

	return NULL;
}

inline bool NodeDescriptor::Body::removeSocket(
		LockGuard &ndGuard, EventSocket *socket) {

	for (size_t i = 0; i < ND_SOCKET_MAX; i++) {
		if (socketList_[i] == socket) {
			socket->shutdown(ndGuard);
			socketList_[i] = NULL;
			return true;
		}
	}

	return false;
}

inline int32_t NodeDescriptor::Body::acceptEventTimeout(
		LockGuard &ndGuard, EventType type, int32_t timeoutMillis) {
	(void) ndGuard;

	if (timeoutMillis == 0) {
		return 0;
	}

	const int64_t current = ee_.clockGenerator_->getMonotonicTime();

	int64_t &next =
			ee_.ndPool_->prepareLastEventTime(eventTimeMap_, type);
	next = std::max(next, current + timeoutMillis);

	int64_t diff = next - current;
	diff = std::max<int64_t>(diff, std::numeric_limits<int32_t>::min());
	diff = std::min<int64_t>(diff, std::numeric_limits<int32_t>::max());

	next = current + diff;

	return static_cast<int32_t>(diff);
}

inline void* NodeDescriptor::Body::resolveUserData(
		UserDataConstructor &constructor) {
	void *userData = userData_;

	if (userData == NULL) {
		void *newUserData = ee_.ndPool_->allocateUserData(&constructor);

		if (userData_.compareExchange(userData, newUserData)) {
			userData = newUserData;
		}
		else {
			assert(userData != NULL);
			ee_.ndPool_->deallocateUserData(newUserData);
		}
	}

	return userData;
}

inline NodeDescriptor::Body* NodeDescriptor::Body::duplicateReference(
		Body *body) {
	if (body != NULL) {
		assert(!body->onSecondary_);
		++body->refCount_;
	}

	return body;
}

inline void NodeDescriptor::Body::removeReference(Body *&body) {
	if (body != NULL && --body->refCount_ == 0) {
		body->ee_.ndPool_->deallocateBody(body);
		body = NULL;
	}
}

inline std::ostream& operator<<(
		std::ostream &stream, const NodeDescriptor &nd) {
	return nd.format(stream);
}


inline EventEngine::Buffer::Body* EventEngine::Buffer::Body::newInstance(
		VariableSizeAllocator &allocator) {
	Body *body = ALLOC_VAR_SIZE_NEW(allocator) Body(allocator);

	++body->refCount_;
	return body;
}

inline EventEngine::Buffer::Body*
EventEngine::Buffer::Body::duplicateReference(Body *body) {
	if (body == NULL) {
		return NULL;
	}

	++body->refCount_;
	return body;
}

inline void EventEngine::Buffer::Body::removeReference(Body *&body) {
	if (body == NULL) {
		return;
	}

	if (--body->refCount_ == 0) {
		ALLOC_VAR_SIZE_DELETE(body->getAllocator(), body);
	}
	body = NULL;
}

inline EventEngine::Buffer::Body::Body(VariableSizeAllocator &allocator) :
		refCount_(0),
		storage_(allocator),
		ext_(NULL) {
}

inline EventEngine::VariableSizeAllocator&
EventEngine::Buffer::Body::getAllocator() {
	return *storage_.get_allocator().base();
}

inline EventEngine::ExternalBuffer&
EventEngine::Buffer::Body::prepareExternal(BufferManager *manager) {
	if (ext_.isEmpty()) {
		ExternalBuffer ext(manager);
		ext_.swap(ext);
	}
	return ext_;
}

/*!
	@brief Deallocates body objects
*/
inline void EventEngine::NDPool::deallocateBody(NodeDescriptor::Body *body) {
	if (body == NULL) {
		return;
	}

	LockGuard guard(poolMutex_);

	switch (body->type_) {
	case NodeDescriptor::ND_TYPE_CLIENT:
		ee_.stats_->increment(Stats::ND_CLIENT_REMOVE_COUNT);
		break;
	case NodeDescriptor::ND_TYPE_SERVER:
		ee_.stats_->increment(Stats::ND_SERVER_REMOVE_COUNT);
		break;
	case NodeDescriptor::ND_TYPE_MULTICAST:
		ee_.stats_->increment(Stats::ND_MCAST_REMOVE_COUNT);
		break;
	default:
		assert(false);
	}

	if (!body->onSecondary_ && body->another_ != NULL) {
		UTIL_OBJECT_POOL_DELETE(bodyPool_, body->another_);
	}
	body->another_ = NULL;

	if (body->type_ == NodeDescriptor::ND_TYPE_CLIENT) {
		body->freeLink_ = bodyFreeLink_;
		bodyFreeLink_ = body;
	}
	else {
		UTIL_OBJECT_POOL_DELETE(bodyPool_, body);
	}
}

/*!
	@brief Deallocates user data
*/
inline void EventEngine::NDPool::deallocateUserData(void *userData) {
	if (userData == NULL) {
		return;
	}

	LockGuard guard(userDataMutex_);

	if (userDataPool_.get() == NULL) {
		return;
	}

	try {
		(*userDataDestructor_)(userData);
	}
	catch (...) {
	}

	userDataPool_->deallocate(userData);

	ee_.stats_->increment(Stats::ND_USER_DATA_REMOVE_COUNT);
}

inline void EventEngine::Stats::increment(Type type) {
	assert(0 <= type && type < STATS_TYPE_MAX);
	valueList_[type]++;
}

inline void EventEngine::Stats::decrement(Type type) {
	assert(0 <= type && type < STATS_TYPE_MAX);
	valueList_[type]--;
}

inline void EventEngine::NDPool::releaseTimeMap(TimeMap *&map) {
	if (map != NULL) {
		LockGuard guard(poolMutex_);
		UTIL_OBJECT_POOL_DELETE(timeMapPool_, map);
		map = NULL;
	}
}


inline EventEngine::LocalBuffer::LocalBuffer() :
		storage_(NULL),
		pendingOffset_(0),
		pendingSize_(0),
		writing_(false) {
}

inline bool EventEngine::LocalBuffer::isAttached() const {
	return (storage_ != NULL);
}

inline void EventEngine::LocalBuffer::attach(Buffer &base) {
	base_ = base;
	base = Buffer();

	storage_ = &base_.getWritableXArray();
	eraseAll();
}

inline void EventEngine::LocalBuffer::reset() {
	storage_ = NULL;
	base_ = Buffer();
	eraseAll();
}

inline void EventEngine::LocalBuffer::eraseAll() {
	pendingOffset_ = 0;
	pendingSize_ = 0;
	writing_ = false;

	base_.setOffset(0);
	if (storage_ != NULL) {
		storage_->clear();
	}
}

inline size_t EventEngine::LocalBuffer::getWritableSize() const {
	if (!writing_) {
		return 0;
	}
	return storage_->capacity() - getPendingTailPosition();
}

inline void* EventEngine::LocalBuffer::getWritableAddress() {
	if (!writing_) {
		return NULL;
	}
	return getData() + getPendingTailPosition();
}

inline bool EventEngine::LocalBuffer::isWriting() const {
	return writing_;
}

inline void EventEngine::LocalBuffer::startWriting(size_t minSize) {
	if (writing_ || !isAttached()) {
		return errorWritingStart();
	}

	const size_t positiveMinSize =
			static_cast<size_t>(std::max<uint64_t>(minSize, 1));
	const size_t writableSize =
			storage_->capacity() - getPendingTailPosition();
	if (positiveMinSize > writableSize) {
		growCapacity(positiveMinSize - writableSize);
	}

	writing_ = true;
}

inline void EventEngine::LocalBuffer::finishWriting(size_t size) {
	if (!writing_ || size > getWritableSize()) {
		return errorWritingFinish(size);
	}

	writing_ = false;
	pendingSize_ += size;
}

inline size_t EventEngine::LocalBuffer::getPendingSize() const {
	return pendingSize_;
}

inline EventByteInStream EventEngine::LocalBuffer::getPendingInStream() const {
	if (!isAttached() || pendingSize_ <= 0) {
		return EventByteInStream(util::ArrayInStream(NULL, 0));
	}
	return EventByteInStream(util::ArrayInStream(
			getData() + getPendingHeadPosition(), pendingSize_));
}

inline const EventEngine::Buffer& EventEngine::LocalBuffer::share() const {
	return base_;
}

inline size_t EventEngine::LocalBuffer::getCapacity() const {
	if (!isAttached()) {
		return 0;
	}
	return storage_->capacity();
}

inline size_t EventEngine::LocalBuffer::getReadableTailPosition() const {
	assert(storage_ != NULL);
	return storage_->size();
}

inline size_t EventEngine::LocalBuffer::getPendingHeadPosition() const {
	assert(storage_ != NULL);
	return getReadableTailPosition() + pendingOffset_;
}

inline size_t EventEngine::LocalBuffer::getPendingTailPosition() const {
	assert(storage_ != NULL);
	return getPendingHeadPosition() + pendingSize_;
}

inline const uint8_t* EventEngine::LocalBuffer::getData() const {
	assert(storage_ != NULL);
	return storage_->data();
}

inline uint8_t* EventEngine::LocalBuffer::getData() {
	assert(storage_ != NULL);
	return storage_->data();
}


inline EventEngine::LocalEvent::LocalEvent() {
}

inline void EventEngine::LocalEvent::reset() {
	base_ = Event();
}

inline Event& EventEngine::LocalEvent::prepareLocal(
		const Event::Source &souce) {
	if (isEmpty()) {
		base_ = Event(souce, 0, 0);
	}
	return base_;
}

inline Event& EventEngine::LocalEvent::getLocal() {
	if (isEmpty()) {
		return errorEmptyAccess();
	}
	return base_;
}

inline void EventEngine::LocalEvent::swap(LocalEvent &ev) {
	std::swap(base_, ev.base_);
}

inline void EventEngine::LocalEvent::transferTo(
		const Event::Source &souce, LocalEvent &ev) const {
	assert(souce.allocator_ != NULL);
	ev.base_ = Event(*souce.allocator_, base_);
}

inline bool EventEngine::LocalEvent::isEmpty() const {
	return base_.isEmpty();
}

inline EventType EventEngine::LocalEvent::getType() const {
	return base_.getType();
}


inline bool EventEngine::Dispatcher::isSpecialEventType(EventType type) {
	return type < 0;
}


inline util::DateTime EventEngine::ClockGenerator::getCurrentApproximately() {
	util::DateTime time = correctedTime_.load();

	if (time == util::DateTime()) {
		const bool trimMilliseconds = false;
		time = util::DateTime::now(trimMilliseconds);
	}

	return time;
}

#endif
