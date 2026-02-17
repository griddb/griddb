/*
	Copyright (c) 2024 TOSHIBA Digital Solutions Corporation

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
	@brief 優先度制御を伴うジョブ制御機構の定義
*/
#ifndef SQL_JOB_MANAGER_PRIORITY_H_
#define SQL_JOB_MANAGER_PRIORITY_H_

#include "sql_job_manager.h"

#include "sql_processor.h"
#include "uuid_utils.h"

struct DataStoreConfig;
class PartitionList;
class SQLExecutionManager;
class DDLProcessor;
class NoSQLContainer;
struct ChunkBufferStats;

class JobBufferedBlockPool;
class JobNodeBodyBase;
class JobNode;

class PriorityJob;
class PriorityTask;
class PriorityJobWorker;
class PriorityJobWorkerTable;
class PriorityJobController;

class PriorityJobBodyBase;
class PriorityTaskBodyBase;

class PriorityJobEnvironment;
class PriorityJobContext;
struct PriorityJobContextSource;

typedef util::VariableSizeAllocator<> PriorityJobAllocator;

/**
 * ジョブ制御機構の共通設定
 */
struct PriorityJobConfig {
	PriorityJobConfig();

	uint32_t resolveBlockPoolSize() const;

	bool priorityControlActivated_;

	uint64_t initialNodeIOCapacity_;
	uint64_t minNodeIOCache_;

	uint64_t initialTaskIOCapacity_;
	uint64_t minTaskIOCache_;

	uint32_t concurrency_;
	uint32_t partitionCount_;

	uint32_t txnConcurrency_;
	uint32_t txnPartitionCount_;

	uint32_t concurrencyPctOfPriorityUpdates_;

	uint64_t storeMemoryLimit_;
	uint64_t workMemoryLimit_;
	int64_t totalMemoryLimit_;

	double workMemoryLimitRate_;
	double workMemoryReservationRate_;
	double storeMemoryReservationRate_;

	bool failOnTotalMemoryLimit_;

	uint64_t workMemoryCache_;
	uint32_t allocatorBlockExpSize_;

	int64_t outputValidationInterval_;
	int64_t nodeValidationInterval_;
	int64_t nodeValidationTimeout_;

	int64_t traceInterval_;
	int32_t traceLimit_;

	int64_t networkTimeInterval_;
};

/**
 * ジョブ・タスクごとの個別設定
 */
struct PriorityTaskConfig {
	explicit PriorityTaskConfig(PriorityJobAllocator &alloc);

	void getStatProfile(StatJobProfilerInfo::Config &dest) const;

	double storeMemoryAgingSwapRate_;
	util::TimeZone timeZone_;

	bool startTimeRequired_;
	int64_t startTime_;
	int64_t startMonotonicTime_;

	bool forAdministrator_;
	bool forAnalysis_;
	bool forResultSet_;
	ContainerType containerType_;

	size_t taskCount_;

	util::AllocString clientAddress_;
	uint16_t clientPort_;

	DatabaseId dbId_;
	util::AllocString dbName_;
	util::AllocString userName_;
	util::AllocString applicationName_;
	util::AllocString statement_;
	int64_t statementTimeout_;
};

/**
 * ジョブ制御機構でプールされる個々のリソースのRAII管理用ホルダー
 */
struct PriorityJobHolders {
	class Pool;

	class Allocator;
	class VarAllocator;
	class Lock;
};

class PriorityJobHolders::Pool {
public:
	explicit Pool(const PriorityJobConfig &config);
	~Pool();

	PriorityJobAllocator& allocateAllocator(
			util::AllocatorLimitter *limitter, bool forTask);
	void releaseAllocator(PriorityJobAllocator *alloc);

	util::StackAllocator& allocateStackAllocator(
			util::AllocatorLimitter *limitter);
	void releaseStackAllocator(util::StackAllocator *alloc);

	util::Mutex& allocateLock();
	void releaseLock(util::Mutex *lock);

private:
	void clear();

	PriorityJobAllocator alloc_;
	util::FixedSizeAllocator<util::Mutex> fixedAllocator_;

	util::Mutex lock_;

	util::AllocDeque<util::Mutex*> mutexList_;
	PriorityJobConfig config_;
};

/**
 * ジョブ制御機構でプールされるスタックアロケータのRAII管理用ホルダー
 */
class PriorityJobHolders::Allocator {
public:
	Allocator(Pool &pool, util::AllocatorLimitter *limitter);
	~Allocator();

	util::StackAllocator& get();

private:
	Allocator(const Allocator&);
	Allocator& operator=(const Allocator&);

	Pool &pool_;
	util::StackAllocator *alloc_;
};

/**
 * ジョブ制御機構でプールされる可変長アロケータのRAII管理用ホルダー
 */
class PriorityJobHolders::VarAllocator {
public:
	VarAllocator(Pool &pool, util::AllocatorLimitter *limitter, bool forTask);
	~VarAllocator();

	PriorityJobAllocator& get();

private:
	VarAllocator(const VarAllocator&);
	VarAllocator& operator=(const VarAllocator&);

	Pool &pool_;
	PriorityJobAllocator *alloc_;
};

/**
 * ジョブ制御機構でプールされるロックのRAII管理用ホルダー
 */
class PriorityJobHolders::Lock {
public:
	explicit Lock(Pool &pool);
	~Lock();

	util::Mutex& get();

private:
	Lock(const Lock&);
	Lock& operator=(const Lock&);

	Pool &pool_;
	util::Mutex *lock_;
};

/**
 * ジョブ制御機構における中間ストアのインメモリブロックプール
 */
class JobBufferedBlockPool {
public:
	JobBufferedBlockPool(LocalTempStore &store, uint32_t freeBlockCount);

	TupleList::Block takeBufferedBlock();
	void addBufferedBlock();

	LocalTempStore& getStore();

	void resetPooledBlocks();

private:
	PriorityJobAllocator alloc_;
	util::Mutex lock_;

	TupleList::Block allocateBufferedBlock();

	util::AllocDeque<TupleList::Block> blockList_;
	LocalTempStore &store_;

	uint32_t freeBlockCount_;
};

/**
 * ジョブ制御機構における中間ストアの非インメモリブロック
 */
class JobUnbufferedBlock {
public:
	typedef LocalTempStore::BlockInfo BlockInfo;
	typedef LocalTempStore::BlockId BlockId;
	typedef TupleList::Block Block;

	explicit JobUnbufferedBlock(Block src = Block());
	~JobUnbufferedBlock();

	JobUnbufferedBlock(const JobUnbufferedBlock &another);
	JobUnbufferedBlock& operator=(const JobUnbufferedBlock &another);

	Block toBufferedBlock() const;
	bool isEmpty() const;

private:
	void assign(LocalTempStore *store, BlockId blockId);
	void clear();

	LocalTempStore *store_;
	BlockId blockId_;
};

struct PriorityJobUtils {
	template<typename T>
	static bool pointerLessNullsFirst(const T *p1, const T *p2);
};

struct PriorityJobStatValue {
	PriorityJobStatValue();

	int64_t value_;
};

struct PriorityJobStats {
	enum Type {
		TYPE_LEAD_TIME,
		TYPE_ACTUAL_TIME,
		TYPE_MEMORY_USE,
		TYPE_SQL_STORE_USE_LAST,
		TYPE_SQL_STORE_USE_PEAK,
		TYPE_DATA_STORE_ACCESS,
		TYPE_TRANS_SIZE,
		TYPE_TRANS_ACTIVE_TIME,
		TYPE_TRANS_BUSY_TIME,
		END_TYPE
	};

	int64_t getValueAt(Type type) const;

	void setValueAt(Type type, int64_t value);
	void setPeakValueAt(Type type, int64_t value);
	void addValueAt(Type type, int64_t value);

	void subtract(const PriorityJobStats &base);
	void add(const PriorityJobStats &base);

	PriorityJobStatValue valueList_[END_TYPE];
};

class PriorityJobTracer {
public:
	struct ResourceUsePlanInfo;

	struct ResourceUseStatsFormatter;
	struct ResourceUsePlanFormatter;

	PriorityJobTracer();
	virtual ~PriorityJobTracer();

	virtual void traceEventHandlingError(
			std::exception &e, const char8_t *situation);

	virtual void traceValidationTimeout(
			const JobNode &node, int64_t currentTime, int64_t lastAliveTime,
			int64_t validationRequestTime, const PriorityJobConfig &config);

	virtual void traceResourceUse(
			PriorityJobContext &cxt, PriorityJobEnvironment &env,
			PriorityJob &job, const JobResourceInfo &info, bool withPlan,
			uint32_t planLimit);

	virtual void traceNodeOutputValidation(
			const JobNode &node, int64_t currentTime, int64_t sendingTime,
			int64_t validatingTime, bool validating, bool started, bool fixed);
};

struct PriorityJobTracer::ResourceUsePlanInfo {
	explicit ResourceUsePlanInfo(util::StackAllocator &alloc);

	void assign(const JobResourceInfo &baseInfo);

	static u8string formatTime(const util::DateTime &value);

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;

	UTIL_OBJECT_CODER_MEMBERS(
			dbName_,
			requestId_,
			nodeAddress_,
			nodePort_,
			connectionAddress_,
			connectionPort_,
			userName_,
			applicationName_,
			statementType_,
			startTime_,
			statementType_,
			actualTime_,
			memoryUse_,
			sqlStoreUse_,
			dataStoreAccess_,
			networkTransferSize_,
			networkTime_,
			availableConcurrency_,
			resourceRestrictions_,
			statement_);

	util::String dbName_;

	util::String requestId_;
	util::String nodeAddress_;
	int32_t nodePort_;

	util::String connectionAddress_;
	int32_t connectionPort_;

	util::String userName_;
	util::String applicationName_;
	util::String statementType_;

	util::String startTime_;
	int64_t actualTime_;
	int64_t memoryUse_;
	int64_t sqlStoreUse_;
	int64_t dataStoreAccess_;
	int64_t networkTransferSize_;
	int64_t networkTime_;
	int64_t availableConcurrency_;

	util::String resourceRestrictions_;
	util::String statement_;
};

struct PriorityJobTracer::ResourceUseStatsFormatter {
public:
	ResourceUseStatsFormatter(
			PriorityJobEnvironment &env, const JobResourceInfo &info);
	std::ostream& format(std::ostream &os) const;

private:
	PriorityJobEnvironment &env_;
	const JobResourceInfo &info_;
};

std::ostream& operator<<(
		std::ostream &os,
		const PriorityJobTracer::ResourceUseStatsFormatter &formatter);

struct PriorityJobTracer::ResourceUsePlanFormatter {
public:
	ResourceUsePlanFormatter(
			PriorityJobContext &cxt, PriorityJob &job,
			const JobResourceInfo &info, uint32_t planLimit);
	std::ostream& format(std::ostream &os) const;

private:
	PriorityJobContext &cxt_;
	PriorityJob &job_;
	const JobResourceInfo &info_;
	uint32_t planLimit_;
};

std::ostream& operator<<(
		std::ostream &os,
		const PriorityJobTracer::ResourceUsePlanFormatter &formatter);

struct PriorityJobInfo {
	typedef JobManager::JobInfo Base;

	struct Node;

	PriorityJobInfo();

	static PriorityJobInfo* ofBase(
			util::StackAllocator &alloc, const Base *base,
			SQLExecution &execution);

	static void resolveClientAddress(
			util::StackAllocator &alloc, SQLExecution &execution,
			const util::String *&address, uint16_t &port);

	static util::String* makeString(
			util::StackAllocator &alloc, const char8_t *src);
	static util::AllocString makeString(
			PriorityJobAllocator &alloc, const util::String *src);

	static util::Vector<Node>* makeNodeList(
			util::StackAllocator &alloc, const Base *base);

	UTIL_OBJECT_CODER_MEMBERS(
			base_,
			nodeList_,
			onCoordinator_,
			startTimeRequired_,
			forAdministrator_,
			forAnalysis_,
			forResultSet_,
			containerType_,
			clientAddress_,
			clientPort_,
			dbId_,
			dbName_,
			userName_,
			applicationName_,
			statement_,
			startTime_,
			statementTimeout_);

	const Base *base_;
	const util::Vector<Node> *nodeList_;
	bool onCoordinator_;

	bool startTimeRequired_;
	bool forAdministrator_;
	bool forAnalysis_;
	bool forResultSet_;
	ContainerType containerType_;

	const util::String *clientAddress_;
	uint16_t clientPort_;

	DatabaseId dbId_;
	const util::String *dbName_;
	const util::String *userName_;
	const util::String *applicationName_;
	const util::String *statement_;
	int64_t startTime_;
	int64_t statementTimeout_;

	const util::XArray<uint8_t> *processorData_;
};

struct PriorityJobInfo::Node {
	Node();

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_ENUM(type_, SQLType::Coder()));

	SQLType::Id type_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ
 */
struct JobMessages {
	typedef util::VariableSizeAllocator<> VariableSizeAllocator;
	typedef util::XArrayOutStream< util::StdAllocator<
			uint8_t, VariableSizeAllocator> > OutStream;

	typedef util::ArrayByteInStream ByteInStream;
	typedef util::ByteStream<OutStream> ByteOutStream;

	typedef util::ByteStream< util::XArrayOutStream<> > StackOutStream;
	typedef util::XArray<
			uint8_t,
			util::StdAllocator<uint8_t, VariableSizeAllocator > > VarByteXArray;

	typedef int32_t Type;
	typedef JobManager::JobId JobId;

	typedef std::pair<uint32_t, uint32_t> TaskPriority;
	typedef util::Vector<TaskPriority> TaskPriorityList;

	typedef std::pair<JobId, bool> JobActivityEntry;
	typedef util::Vector<JobActivityEntry> JobActivityList;

	enum PriorityLevel {
		LEVEL_NORMAL_WORKING,
		LEVEL_NORMAL_CLOSING,
		LEVEL_ERROR
	};

	struct CompatibleHeader;
	struct Header;
	struct Base;
	struct TaskIOInfo;

	struct Deploy;
	struct ReadyOutput;
	struct Pipe;
	struct Finish;
	struct Cancel;
	struct Complete;

	struct ChangeTaskPriority;
	struct ApplyTaskPathPriority;
	struct GrowTaskOutput;
	struct GrowNodeOutput;

	struct ReportSent;
	struct ValidateNode;

	struct Utils;
};

struct JobMessages::CompatibleHeader {
	CompatibleHeader();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	bool priorityControlActivated_;
};

struct JobMessages::Header {
	Header();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	bool compatible_;
	CompatibleHeader compatiblePart_;

	UUIDValue uuid_;
	uint64_t messageId_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(共通)
 */
struct JobMessages::Base {
	Base();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	JobId jobId_;
};

struct JobMessages::TaskIOInfo {
	TaskIOInfo();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	uint32_t inputCount_;
	uint32_t outputCount_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(デプロイ、CNODE->PNODE)
 */
struct JobMessages::Deploy {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	Deploy();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	static void encodeBinaryData(
			ByteOutStream &out, const util::XArray<uint8_t> *data);
	static util::XArray<uint8_t>& decodeBinaryData(
			ByteInStream &in, util::StackAllocator &alloc);

	Base base_;
	const PriorityJobInfo *jobInfo_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(出力準備完了、PNODE->CNODE)
 */
struct JobMessages::ReadyOutput {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	ReadyOutput();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	TaskId taskId_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(タスク間ブロック転送)
 */
struct JobMessages::Pipe {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	Pipe();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	TaskId taskId_;
	InputId input_;
	TaskIOInfo ioInfo_;
	TupleList::Block block_;

	uint64_t *encodedBlockSize_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(タスク間データ転送完了)
 */
struct JobMessages::Finish {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	Finish();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	TaskId taskId_;
	InputId input_;
	TaskIOInfo ioInfo_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(ジョブキャンセル)
 */
struct JobMessages::Cancel {
	enum { PRIORITY_LEVEL = LEVEL_ERROR };

	Cancel();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	util::LocalUniquePtr<util::Exception> exception_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(特定ノード内のジョブ処理完了)
 */
struct JobMessages::Complete {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_CLOSING };

	Complete();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	JobProfilerInfo *profilerInfo_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ
 * (タスク入力優先度変更通知、PNODE->CNODE)
 */
struct JobMessages::ChangeTaskPriority {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	ChangeTaskPriority();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	TaskId taskId_;
	uint32_t priorInput_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ
 * (タスクパス優先度変更確認・変更適用、PNODE<->CNODE)
 */
struct JobMessages::ApplyTaskPathPriority {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	ApplyTaskPathPriority();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	const TaskPriorityList *priorityList_;
	bool checking_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(タスク出力容量拡張)
 */
struct JobMessages::GrowTaskOutput {
	enum { PRIORITY_LEVEL = LEVEL_NORMAL_WORKING };

	GrowTaskOutput();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	TaskId taskId_;
	uint32_t output_;
	TaskIOInfo ioInfo_;
	uint64_t capacity_;
};

/**
 * 各ノードのジョブ制御機構間でやり取りするメッセージ(ノード出力容量拡張)
 */
struct JobMessages::GrowNodeOutput {
	GrowNodeOutput();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	uint64_t capacity_;
};

/**
 * 他ノードへの送信の完了通知を示すメッセージ
 */
struct JobMessages::ReportSent {
	ReportSent();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	Base base_;
	TaskId taskId_;
	bool networkBusy_;
	uint64_t sequenceNum_;
	uint64_t size_;
};

struct JobMessages::ValidateNode {
	ValidateNode();

	void encode(ByteOutStream &out) const;
	void decode(PriorityJobContext &cxt, ByteInStream &in);

	bool ackRequired_;
	const JobActivityList *jobActivityList_;
	uint64_t validationCode_;
};

struct JobMessages::Utils {
	template<typename M> static bool isHeaderCompatible();
	template<typename M> static JobMessages::Type getEventType();

	static void encodeBool(ByteOutStream &out, bool value);
	static bool decodeBool(ByteInStream &in);
};

/**
 * ジョブ制御機構におけるノード表現
 */
class JobNode {
public:
	struct Info;
	struct Source;

	explicit JobNode(const JobNodeBodyBase *base = NULL);
	~JobNode();

	JobNode(const JobNode &another);
	JobNode& operator=(const JobNode &another);

	bool operator==(const JobNode &another) const;
	bool operator<(const JobNode &another) const;

	const Info& getInfo() const;
	const Info& getInfoOptional() const;

	bool isEmpty() const;
	bool isSelf() const;

	bool isClosed() const;
	void close();

	bool acceptBlockInput(uint64_t &nextCapacity);
	void acceptBlockOutput();

	void growOutputCapacity(uint64_t minCapacity);
	bool isOutputRestricted();

	void getStatProfileOptional(StatWorkerProfilerInfo::NodeEntry &dest) const;

private:
	struct Body;

	void addReference() const;
	void removeReference();

	Body& getBody();
	const Body& getBody() const;

	bool updateInputCapacity(Body &body, uint64_t &nextCapacity);

	static Body& errorEmptyBody();

	Body *body_;
};

/**
 * ジョブ制御機構におけるノード識別用情報
 */
struct JobNode::Info {
public:
	Info(
			NodeDescriptorId id, const util::SocketAddress &address,
			const NodeDescriptor &nd, bool self);

	static const Info& emptyInfo();

	util::String* toAddressString(util::StackAllocator &alloc) const;
	void getAddressString(util::String &str, bool appending) const;

	void getHostString(util::String &str) const;
	static void getHostString(
			const util::SocketAddress &address, util::String &str);

	uint16_t getPort() const;

	NodeDescriptorId id_;
	util::SocketAddress address_;
	NodeDescriptor nd_;
	bool self_;

private:
	static Info EMPTY_INFO;
};

struct JobNode::Source {
	Source(
			const Info &info, const PriorityJobConfig &config,
			PriorityJobAllocator &alloc, util::Mutex &totalLock);

	Info info_;
	PriorityJobConfig config_;

	PriorityJobAllocator &alloc_;
	util::Mutex &totalLock_;
};

struct JobNode::Body {
	explicit Body(const Source &source);

	util::Atomic<uint64_t> refCount_;
	util::Mutex nodeLock_;

	PriorityJobAllocator &alloc_;
	util::Mutex &totalLock_;

	Info info_;

	util::Atomic<bool> closed_;

	uint64_t initialIOCapacity_;
	uint64_t minIOCache_;

	uint64_t inputSize_;
	uint64_t inputCapacity_;

	uint64_t outputSize_;
	uint64_t outputCapacity_;
};

/**
 * ジョブ制御機構におけるノード間メッセージ転送機構
 */
class JobTransporter {
public:
	class Engine;
	class DefaultEngine;

	struct EngineRequestOption;
	struct RequestOption;

	struct EntryBodyKey;

	template<typename M> struct HandlerBase {
		typedef void (PriorityJobController::*ReceiverFunc)(
				PriorityJobContext&, const M&);
	};

	JobTransporter(Engine &engine, const PriorityJobConfig &config);
	~JobTransporter();

	void setUpHandlers(PriorityJobController &controller);

	template<typename M>
	void setReceiverHandler(
			PriorityJobController &controller,
			typename HandlerBase<M>::ReceiverFunc func);

	void prepare(PriorityJobContext &cxt);

	template<typename M>
	bool send(
			PriorityJobContext &cxt, const JobNode &node, const M &msg,
			const RequestOption *option = NULL);

	const JobNode& getSelfNode();
	JobNode resolveNode(const util::SocketAddress &address);

	void validateNodes(
			PriorityJobContext &cxt, PriorityJobController &controller);
	uint64_t requestNodeValidation(
			PriorityJobContext &cxt, const JobNode &node);

	void invalidateRandomNode(
			PriorityJobContext& cxt, PriorityJobController &controller,
			int32_t mode);

	void ackNodeValidation(
			PriorityJobContext &cxt, const JobNode &node,
			uint64_t validationCode);
	void accpetNodeValidationResult(
			const JobNode &node, uint64_t validationCode);
	void accpetSelfValidationResult(PriorityJobContext &cxt);

	void activateControl(bool activated);
	void setNodeValidationTimeout(int64_t timeoutMillis);

	void getStatProfile(
			util::StackAllocator &alloc, StatJobProfiler &profiler, bool empty);

private:
	class BaseReceiverHandler;
	class BaseSenderHandler;

	template<typename M> class ReceiverHandler;
	template<typename M> class SenderHandler;

	class ReportSentSenderHandler;
	class PeriodicHandler;

	struct NodeEntry;
	struct EntryBodyKeyBase;

	typedef util::AllocMap<
			JobMessages::Type, BaseReceiverHandler*> ReceiverHandlerMap;
	typedef util::AllocMap<util::SocketAddress, NodeEntry*> NodeMap;

	void clear();

	static void clearReceiverHandlerMap(
			PriorityJobAllocator &alloc, ReceiverHandlerMap &map);
	static void clearNodeMap(PriorityJobAllocator &alloc, NodeMap &map);

	void executePeriodicOperations(
			PriorityJobContext &cxt, PriorityJobController &controller);

	void validateWorkers(
			PriorityJobContext &cxt, PriorityJobController &controller);
	void cleanUpPools(PriorityJobController &controller);

	const JobMessages::JobActivityList* checkJobActivities(
			PriorityJobContext &cxt, PriorityJobController &controller);

	void validateNodes(
			PriorityJobContext &cxt, PriorityJobController &controller,
			const JobMessages::JobActivityList *activityList);
	void validateNodeAt(
			PriorityJobContext &cxt, PriorityJobController &controller,
			const JobMessages::JobActivityList *activityList,
			NodeEntry &entry);

	int64_t getPrevValidatingTime();
	uint64_t requestNodeValidationInternal(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			NodeEntry &entry, const JobMessages::JobActivityList *activityList,
			bool ackRequired, const uint64_t *validationCode);

	template<typename M>
	bool sendInternal(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			NodeEntry &entry, const M &msg, const RequestOption *option);

	void prepareValidationTimer(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt);

	EngineRequestOption makeRequestOption(
			util::LocalUniquePtr<ReportSentSenderHandler> &sentHandler,
			const RequestOption *baseOption);

	JobMessages::Header makeRequestHeader(
			bool compatible, uint64_t messageId);
	bool acceptHeader(
			PriorityJobContext &cxt, PriorityJobController &controller,
			const JobNode &node, const JobMessages::Header &header);
	static void assignInitialHeader(
			NodeEntry &entry, const JobMessages::Header &header);

	bool checkResourceControlLevel(const JobNode &node, bool activated);

	JobNode invalidateNodeEntry(
			util::LockGuard<util::Mutex>&, PriorityJobController &controller,
			NodeEntry &entry);

	NodeEntry& resolveNodeEntry(const JobNode &node);
	NodeEntry& resolveNodeEntryDetail(
			util::LockGuard<util::Mutex>&, const util::SocketAddress &address,
			bool selfAllowed);

	NodeEntry* findNodeEntry(
			util::LockGuard<util::Mutex>&, const util::SocketAddress &address);
	NodeEntry* nextNodeEntry(util::SocketAddress &lastAddress);

	static util::AllocUniquePtr<NodeEntry>::ReturnType createNodeEntry(
			PriorityJobAllocator &alloc, const JobNode &node);

	static JobNode createNode(
			PriorityJobAllocator &nodeSetAlloc, util::Mutex &nodeSetLock,
			Engine &engine, const PriorityJobConfig &config,
			const util::SocketAddress *address);

	static void errorDuplicateHandler();
	static NodeEntry& errorSelfNode(const util::SocketAddress &address);

	static void errorResourceControlLevel(
			const JobNode &node, bool local, bool remote);
	static void errorInvalidHeader(
			const JobNode &node, bool uuidMatch, uint64_t expectedMessageId,
			uint64_t headerMessageId);
	static NodeEntry& errorInvalidNode(const JobNode &node);
	void errorNodeTimeout(
			const JobNode &node, int64_t currentTime, int64_t lastAliveTime,
			int64_t validationRequestTime, const PriorityJobConfig &config);

	static JobNode errorNodeAddress(const util::SocketAddress &address);

	PriorityJobAllocator alloc_;
	util::Mutex lock_;

	PriorityJobAllocator nodeSetAlloc_;
	util::Mutex nodeSetLock_;

	Engine &engine_;
	PriorityJobConfig config_;
	util::Atomic<bool> priorityControlActivated_;
	util::Atomic<bool> eventsPrepared_;

	ReceiverHandlerMap receiverHandlerMap_;
	util::AllocUniquePtr<PeriodicHandler> periodicHandler_;

	util::AllocUniquePtr<NodeEntry> selfNodeEntry_;
	NodeMap nodeMap_;

	int64_t activityCheckTime_;
	int64_t activityCheckTimestamp_;

	util::Atomic<int64_t> nodeValidationTimeout_;
};

class JobTransporter::Engine {
public:
	virtual ~Engine();

	virtual void setReceiverHandlerDirect(
			JobMessages::Type type, BaseReceiverHandler &handler) = 0;
	virtual void setPeriodicHandlerDirect(
			JobMessages::Type type, PeriodicHandler &handler) = 0;

	virtual bool sendDirect(
			PriorityJobContext &cxt, const JobNode &node,
			JobMessages::Type type, const JobMessages::Header &header,
			const BaseSenderHandler &handler,
			const EngineRequestOption &option) = 0;
	virtual void addPeriodicTimer(
			PriorityJobContext &cxt, JobMessages::Type type,
			int32_t intervalMillis) = 0;

	virtual JobNode::Info getSelfNodeInfo() = 0;
	virtual bool getNodeInfo(
			const util::SocketAddress &address, JobNode::Info &info) = 0;
};

class JobTransporter::DefaultEngine : public JobTransporter::Engine {
public:
	explicit DefaultEngine(EventEngine &base);
	virtual ~DefaultEngine();

	virtual void setReceiverHandlerDirect(
			JobMessages::Type type, BaseReceiverHandler &handler);
	virtual void setPeriodicHandlerDirect(
			JobMessages::Type type, PeriodicHandler &handler);

	virtual bool sendDirect(
			PriorityJobContext &cxt, const JobNode &node,
			JobMessages::Type type, const JobMessages::Header &header,
			const BaseSenderHandler &handler,
			const EngineRequestOption &option);
	virtual void addPeriodicTimer(
			PriorityJobContext &cxt, JobMessages::Type type,
			int32_t intervalMillis);

	virtual JobNode::Info getSelfNodeInfo();
	virtual bool getNodeInfo(
			const util::SocketAddress &address, JobNode::Info &info);

private:
	static EventRequestOption* makeDefaultRequestOption(
			PriorityJobContext &cxt, const EngineRequestOption &option,
			const JobMessages::Header &header,
			util::LocalUniquePtr<Event> &eventOnSent,
			util::LocalUniquePtr<EventRequestOption> &requestOption);
	static JobNode::Info makeNodeInfo(const NodeDescriptor &nd, bool self);

	EventEngine &base_;
};

struct JobTransporter::EngineRequestOption {
	EngineRequestOption();

	JobMessages::Type typeOnSent_;
	const BaseSenderHandler *handlerOnSent_;
};

struct JobTransporter::RequestOption {
	RequestOption();

	JobId jobId_;
	TaskId taskId_;
	bool networkBusy_;
	uint64_t sequenceNum_;
	uint64_t *sendingSize_;
};

struct JobTransporter::EntryBodyKey {
	explicit EntryBodyKey(const EntryBodyKeyBase&);
};

class JobTransporter::BaseReceiverHandler : public EventHandler {
public:
	BaseReceiverHandler(
			PriorityJobController &controller, JobTransporter &transporter,
			bool headerCompatible);

	virtual ~BaseReceiverHandler();
	virtual void operator()(
			PriorityJobContext &cxt, JobMessages::ByteInStream &in) = 0;

	virtual void operator()(EventContext &ec, Event &ev);

protected:
	PriorityJobController& getController();

private:
	PriorityJobContextSource makeContextSource(EventContext &ec, Event &ev);

	PriorityJobController &controller_;
	JobTransporter &transporter_;
	bool headerCompatible_;
};

class JobTransporter::BaseSenderHandler {
public:
	virtual ~BaseSenderHandler();
	virtual void operator()(
			PriorityJobContext &cxt, JobMessages::ByteOutStream &out) const = 0;
};

template<typename M>
class JobTransporter::ReceiverHandler :
	public JobTransporter::BaseReceiverHandler {
public:
	ReceiverHandler(
			PriorityJobController &controller, JobTransporter &transporter,
			typename HandlerBase<M>::ReceiverFunc func);
	virtual ~ReceiverHandler();

	virtual void operator()(
			PriorityJobContext &cxt, JobMessages::ByteInStream &in);
	virtual void operator()(EventContext &ec, Event &ev);

private:
	typename HandlerBase<M>::ReceiverFunc func_;
};

template<typename M>
class JobTransporter::SenderHandler : public JobTransporter::BaseSenderHandler {
public:
	explicit SenderHandler(const M *msg);
	virtual ~SenderHandler();

	virtual void operator()(
			PriorityJobContext &cxt, JobMessages::ByteOutStream &out) const;

private:
	const M *msg_;
};

class JobTransporter::ReportSentSenderHandler :
	public JobTransporter::BaseSenderHandler {
public:
	ReportSentSenderHandler(const RequestOption &option);
	virtual ~ReportSentSenderHandler();

	virtual void operator()(
			PriorityJobContext &cxt, JobMessages::ByteOutStream &out) const;

private:
	RequestOption option_;
};

class JobTransporter::PeriodicHandler : public EventHandler {
public:
	PeriodicHandler(
			PriorityJobController &controller, JobTransporter &transporter);
	virtual ~PeriodicHandler();

	virtual void operator()(EventContext &ec, Event &ev);

	static JobMessages::Type getType();

private:
	PriorityJobController &controller_;
	JobTransporter &transporter_;
};

struct JobTransporter::NodeEntry {
	explicit NodeEntry(const JobNode &node);

	void resetEntry(const JobNode &node);

	util::Mutex lock_;

	JobNode node_;

	bool uuidAssigned_;
	bool validationStarted_;
	UUIDValue uuid_;

	uint64_t lastInMessageId_;
	uint64_t lastOutMessageId_;

	int64_t lastAliveTime_;
	int64_t lastValidatingTime_;
	int64_t prevValidatingTime_;
	int64_t validationRequestTime_;

	uint64_t nextValidationCode_;
	uint64_t workingValidationCode_;
};

struct JobTransporter::EntryBodyKeyBase {
};

class JobNodeBodyBase {
public:
	JobNodeBodyBase(
			const JobTransporter::EntryBodyKey&,
			const JobNode::Source &source);

	const JobNode::Source& getSource() const;

private:
	JobNode::Source source_;
};

/**
 * ジョブ制御機構におけるジョブ表現
 */
class PriorityJob {
public:
	struct Source;
	struct Types;

	typedef JobManager::JobInfo BaseJobInfo;
	typedef JobManager::JobId JobId;

	typedef util::AllocVector<JobNode> NodeList;
	typedef util::AllocSet<JobNode> NodeSet;

	typedef std::pair<uint32_t, uint32_t> TaskPriority;
	typedef util::Vector<TaskPriority> LocalPriorityList;

	explicit PriorityJob(const PriorityJobBodyBase *base = NULL);
	~PriorityJob();

	PriorityJob(const PriorityJob &another);
	PriorityJob& operator=(const PriorityJob &another);

	bool operator==(const PriorityJob &another) const;
	bool operator<(const PriorityJob &another) const;

	void close();
	bool isClosed() const;
	bool isEmpty() const;
	const JobId& getId() const;

	TaskId getTaskCount() const;
	bool findTask(TaskId taskId, bool byPre, PriorityTask &task) const;
	PriorityTask getTask(TaskId taskId) const;

	bool findTaskDirect(
			util::LockGuard<util::Mutex>&, TaskId taskId,
			PriorityTask &task) const;
	const PriorityTask& getTaskDirect(
			util::LockGuard<util::Mutex>&, TaskId taskId) const;

	bool findPreInputTask(TaskId taskId, PriorityTask &task) const;
	bool findPreInputTaskDirect(
			util::LockGuard<util::Mutex>&, TaskId taskId,
			PriorityTask &task) const;
	bool prepareTask(
			PriorityJobContext &cxt, TaskId taskId, PriorityTask &task,
			uint32_t inputCount, uint32_t outputCount);

	bool findPriorTask(
			const PriorityTask &baseTask, PriorityTask &priorTask) const;
	void setTaskProgressDirect(
			util::LockGuard<util::Mutex>&, const PriorityTask &task,
			const TaskProgressKey &key, const TaskProgressValue *prevValue,
			const TaskProgressValue *nextValue);

	void deploy(PriorityJobContext &cxt, const PriorityJobInfo &jobInfo);
	void readyOutput(PriorityJobContext &cxt, const PriorityTask &task);
	void finishTask(
			PriorityJobContext &cxt, const PriorityTask &task,
			bool procFinished);

	void changeTaskPriority(
			PriorityJobContext &cxt, const PriorityTask &task,
			uint32_t priorInput);
	void applyTaskPathPriority(
			PriorityJobContext &cxt, const LocalPriorityList *priorityList,
			bool checkOnly);

	void forceCancel(PriorityJobContext &cxt, const util::Exception &exception);
	void cancel(
			PriorityJobContext &cxt, const util::Exception &exception,
			const JobNode *requesterNode);

	void complete(PriorityJobContext &cxt, const JobProfilerInfo *profilerInfo);
	void cleanUp(PriorityJobContext &cxt);

	bool isResultOutputReady() const;
	void checkCancel();

	bool replyResult(PriorityJobContext &cxt, bool fetchFinished);
	bool fetch(
			PriorityJobContext &cxt, SQLExecution &execution, bool taskInside);
	bool getFetchContext(
			PriorityJobContext &cxt, SQLFetchContext &fetchContext);

	void setDdlAckStatus(
			PriorityJobContext &cxt, int32_t pos, int32_t status);
	void setResultCompleted();

	bool isNodeRelated(const JobNode &node) const;

	util::AllocatorLimitter& getAllocatorLimitter() const;
	const PriorityJobConfig& getConfig() const;
	const PriorityTaskConfig& getTaskConfig() const;

	uint32_t getConcurrency() const;
	uint32_t getRestrictionFlags() const;

	bool isProfiling() const;
	void getProfile(util::StackAllocator &alloc, JobProfilerInfo &info) const;
	bool getStatProfile(
			util::StackAllocator &alloc, StatJobProfilerInfo &info,
			DatabaseId &dbId) const;
	void getResourceProfile(
			util::StackAllocator &alloc, util::Vector<JobResourceInfo> &infoList,
			bool byTask, bool limited) const;
	void setUpProfilerInfo(SQLProfilerInfo &profilerInfo) const;

	bool getStats(PriorityJobStats &stats, uint32_t activeFlags) const;
	void mergeTaskStats(PriorityJobStats::Type type, int64_t valueDiff);

	void startTaskOperation();
	void finishTaskOperation(
			PriorityJobContext &cxt, PriorityJobStats &prev,
			PriorityJobStats &next);
	void finishEmptyTaskOperation();

	void updateActivity(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt);
	void updateRemoteActivity(
			PriorityJobContext &cxt, const JobNode &node, bool active);
	bool checkTotalActivity(PriorityJobContext &cxt, int64_t prevCheckTime);

	template<typename M>
	bool sendMessage(
			PriorityJobContext &cxt, const JobNode &node, const M &msg,
			JobTransporter::RequestOption *option = NULL);

private:
	class State;
	struct Body;

	enum ActivityFlag {
		ACTIVITY_DISCONNECTED = (1 << 0),
		ACTIVITY_RESPONDED = (1 << 1),
		ACTIVITY_SELF_IDLE = (1 << 2),
		ACTIVITY_SELF_WORKER_BUSY = (1 << 3),
		ACTIVITY_REMOTE_IDLE = (1 << 4)
	};

	enum ReplyType {
		REPLY_ERROR_BEGIN,
		REPLY_ERROR_PENDING,
		REPLY_ERROR_END,
		REPLY_PROFILE_BEGIN,
		REPLY_PROFILE_PENDING,
		REPLY_PROFILE_END,
		REPLY_FETCH_BEGIN,
		REPLY_FETCH_PENDING,
		REPLY_FETCH_PARTIAL,
		REPLY_FETCH_COMPLETE,
		END_REPLY
	};

	typedef util::AllocatorLimitter::ConfigScope LimitterConfigScope;
	typedef util::LocalUniquePtr<LimitterConfigScope> LimitterConfigScopePtr;

	typedef util::AllocVector<PriorityTask> TaskList;
	typedef util::AllocSet<PriorityTask> TaskSet;

	typedef util::AllocVector<TaskPriority> PriorityList;
	typedef std::pair<PriorityTask, bool> PriorityTaskValue;

	typedef util::AllocMap<PriorityTask, uint32_t> PriorInputMap;
	typedef util::AllocMap<TaskPriority, PriorityTaskValue> PriorityTaskMap;
	typedef util::AllocMap<JobNode, uint32_t> NodePriorityMap;
	typedef util::AllocMap<JobNode, int64_t> NodeTimeMap;
	typedef util::AllocSet<PriorityJobWorker*> WorkerSet;

	typedef std::pair<PriorityJobWorker*, int32_t> TypedWorker;
	typedef std::pair<TypedWorker, TaskProgressKey> FullTaskProgressKey;
	typedef std::pair<TaskProgressValue, TaskId> FullTaskProgressValue;
	typedef std::pair<
			FullTaskProgressKey, FullTaskProgressValue> FullTaskProgressEntry;
	typedef util::AllocSet<FullTaskProgressEntry> TaskProgressSet;

	explicit PriorityJob(Body &body);

	void addReference() const;
	void removeReference();

	Body& getBody() const;

	static void closeInternal(PriorityJobContext *cxt, Body &body);
	static void clearOwnedTaskList(Body &body, TaskList &taskList);

	static void prepareLimitterConfigScope(
			Body &body, LimitterConfigScopePtr &configScope,
			bool onCoordinator);

	void assignCoordinator(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			bool onCoordinator);
	void assignTaskList(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			const PriorityJobInfo &jobInfo);
	PriorityTask createPreTask(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			TaskId mainId, uint32_t inputCount, uint32_t outputCount);

	static void assignTaskIO(util::LockGuard<util::Mutex>&, Body &body);
	static void assignWorkingMap(
			util::LockGuard<util::Mutex>&, Body &body,
			const PriorityList *nextPriorityList);
	static void assignParticipants(util::LockGuard<util::Mutex>&, Body &body);
	static void assignRelatedNodes(
			util::LockGuard<util::Mutex> &guard, Body &body,
			const PriorityJobInfo &jobInfo);

	static void deployToParticipants(
			PriorityJobContext &cxt, Body &body,
			const PriorityJobInfo *destJobInfo);

	static PriorityJobInfo* makeParticipantJobInfo(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			const PriorityJobInfo &jobInfo);
	static void makeJobInfo(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			const PriorityJobInfo *src, PriorityJobInfo *&dest);
	static PriorityTaskConfig makeTaskConfig(
			PriorityJobContext &cxt, PriorityJobAllocator &alloc,
			const PriorityJobInfo &info);

	static void activateNextTasks(
			PriorityJobContext &cxt, Body &body, bool deltaOnly, bool initial);
	static void reportCompletion(PriorityJobContext &cxt, Body &body);

	static bool updateActiveTasks(
			util::LockGuard<util::Mutex>&, Body &body, bool deltaOnly,
			TaskSet &targetSet, TaskSet *checkOnlySet);
	static bool getCompletionReport(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			NodeSet &nodeSet, JobProfilerInfo *&profilerInfo);

	static void updateTaskPathPriority(PriorityJobContext &cxt, Body &body);
	static bool checkTaskPathPriority(
			PriorityJobContext &cxt, Body &body,
			const LocalPriorityList *priorityList);
	static bool replyTaskPathPriority(
			PriorityJobContext &cxt, Body &body,
			const LocalPriorityList *priorityList, bool acceptable);
	static void applyTaskPathPriorityLocal(
			PriorityJobContext &cxt, Body &body,
			const LocalPriorityList *priorityList);

	static void makeTaskPathPriority(
			util::LockGuard<util::Mutex>&, Body &body,
			PriorityList &priorityList);
	static TaskPriority getTaskPathPriority(
			util::LockGuard<util::Mutex>&, Body &body, TaskId taskId);
	static TaskPriority getTaskPathPriorityDetail(
			util::LockGuard<util::Mutex>&, const PriorityList &priorityList,
			TaskId taskId);

	static uint32_t concurrencyOfNewPriority(
			util::LockGuard<util::Mutex>&, Body &body,
			const LocalPriorityList &priorityList, uint32_t currentConcurrency);
	static bool isAllowedConcurrencyForNewPriority(
			util::LockGuard<util::Mutex>&, Body &body,
			uint32_t currentConcurrency, uint32_t nextConcurrency);

	static void removeTaskProgress(
			util::LockGuard<util::Mutex>&, Body &body,
			const PriorityTask &task);

	static void makeWorkingMap(
			util::LockGuard<util::Mutex>&, Body &body,
			const PriorityList &priorityList, PriorityTaskMap &workingMap);
	static bool removeWorkingTask(
			util::LockGuard<util::Mutex>&, Body &body, const PriorityTask &task,
			bool &taskPathDone);
	static PriorityTaskValue resolvePriorityTaskValue(
			util::LockGuard<util::Mutex>&, const PriorityList &priorityList,
			const PriorityTask &task);

	static void closeAllTaskIO(
			PriorityJobContext &cxt, Body &body, bool withOutput,
			bool withResult);
	static void closeRemainingTasks(
			PriorityJobContext &cxt, Body &body, bool withJob,
			bool withJobForce);
	static void activateClosingTask(
			PriorityJobContext &cxt, Body &body, bool force,
			const util::Exception *exception);

	static void processClosing(
			PriorityJobContext &cxt, Body &body, bool taskIgnorable,
			const util::Exception *exception, const JobNode *requesterNode);

	static void getAllWorkingTasks(
			util::LockGuard<util::Mutex>&, Body &body, bool withResult,
			TaskList &targetList);

	static bool isAllProfiled(util::LockGuard<util::Mutex>&, Body &body);

	static void mergeProfile(
			util::LockGuard<util::Mutex>&, Body &body,
			const JobProfilerInfo &profilerInfo);
	static void getProfileAt(
			util::LockGuard<util::Mutex>&, util::StackAllocator &alloc,
			Body &body, JobProfilerInfo &info);

	static bool profileReply(
			PriorityJobContext &cxt, Body &body, SQLExecution *execution,
			ReplyType type);
	static bool profileReplyEnd(
			PriorityJobContext &cxt, Body &body, SQLExecution *execution,
			bool respondedOnBegin, ReplyType pendingType, ReplyType replyType);

	static void traceLongQuery(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			SQLExecution *execution);

	static uint32_t resolveConcurrency(
			util::LockGuard<util::Mutex>&, Body &body);
	static uint32_t updateConcurrency(
			util::LockGuard<util::Mutex>&, Body &body);

	static bool acceptSendingMessagePriority(
			util::LockGuard<util::Mutex>&, Body &body, const JobNode &node,
			uint32_t level);

	static bool findTaskDirectDetail(
			util::LockGuard<util::Mutex> &guard, Body &body, TaskId taskId,
			bool byPre, PriorityTask &task);
	static PriorityTask& getTaskDirectDetail(
			util::LockGuard<util::Mutex>&, Body &body, TaskId taskId);

	static void updateActivityDetail(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body);
	static void cancelByActivity(
			PriorityJobContext &cxt, Body &body, bool connected,
			int64_t selfIdleTime, int64_t remoteIdleTime);
	static void checkStatementTimeout(
			PriorityJobContext &cxt, Body &body);

	static bool checkClientConnection(
			util::LockGuard<util::Mutex>&, Body &body, bool &responded);
	bool checkSelfActivity(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			int64_t prevCheckTime, bool responded, bool &selfWorkerBusy,
			int64_t &idleTime);
	static bool checkRemoteActivity(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
			int64_t prevCheckTime, int64_t &idleTime);

	static bool checkSpecialTaskActivity(
			util::LockGuard<util::Mutex> &guard, Body &body, bool responded,
			bool selfBusyBase);

	static void activityFlagsToStatProfile(
			uint32_t flags, StatJobProfilerInfo::State &info);
	static uint32_t makeActivityFlags(
			bool connected, bool responded, bool selfWorked,
			bool selfWorkerBusy, bool remoteWorked);

	static bool isOnCoordinator(util::LockGuard<util::Mutex>&, Body &body);

	template<typename M>
	static bool sendMessageDetail(
			PriorityJobContext &cxt, Body &body, const JobNode &node,
			const M &msg, JobTransporter::RequestOption *option = NULL);

	static uint32_t checkTaskId(
			util::LockGuard<util::Mutex>&, TaskId taskId, const TaskList *list);

	static Body& errorEmptyBody();
	static uint32_t errorTaskId();
	static TaskPriority errorTaskIdForPriority();
	static PriorityTask& errorTaskEmpty(bool closed);
	static void errorResultTaskMulti();
	static void errorResultTaskEmpty();
	static void errorTaskPath();

	Body *body_;
};

struct PriorityJob::Source {
	Source(PriorityJobController &controller, JobId id);

	PriorityJobController &controller_;
	JobId id_;
};

struct PriorityJob::Types {
	enum ResourceRestriction {
		RES_SCALE_DOWN_BY_SELF_MEMORY,
		RES_SCALE_DOWN_BY_TOTAL_MEMORY,
		RES_PIPELINE_DOWN_BY_SELF_MEMORY,
		RES_PIPELINE_DOWN_BY_TOTAL_MEMORY,
		END_RES
	};

	enum TaskStatus {
		TASK_INACTIVE,
		TASK_INPUT_WAITING,
		TASK_INPUT_BLOCK_PENDING,
		TASK_INPUT_FINISH_PENDING,
		TASK_INPUT_BUSY,
		TASK_INPUT_CLOSED,
		TASK_OUTPUT_BUSY,
		TASK_OUTPUT_CLOSED,
		TASK_OPERATING,
		TASK_CLOSED,
		TASK_ERROR,
		END_TASK
	};

	typedef util::NameCoderEntry<ResourceRestriction> ResEntry;
	typedef util::NameCoder<ResourceRestriction, END_RES> ResCoder;

	typedef util::NameCoderEntry<ReplyType> ReplyEntry;
	typedef util::NameCoder<ReplyType, END_REPLY> ReplyCoder;

	typedef util::NameCoderEntry<TaskStatus> TaskStatusEntry;
	typedef util::NameCoder<TaskStatus, END_TASK> TaskStatusCoder;

	static const ResEntry RES_LIST[];
	static const ReplyEntry REPLY_LIST[];
	static const TaskStatusEntry TASK_STATUS_LIST[];

	static const ResCoder RES_CODER;
	static const ReplyCoder REPLY_CODER;
	static const TaskStatusCoder TASK_STATUS_CODER;

	static void restrictionFlagsToString(uint32_t flags, util::String &str);
	static void replyFlagsToString(uint32_t flags, util::String &str);
	static void taskStatusFlagsToString(uint32_t flags, util::String &str);

	template<typename T, size_t N>
	static void flagsToString(
			uint32_t flags, const util::NameCoder<T, N> &coder,
			util::String &str);
};

class PriorityJob::State {
public:
	typedef util::LockGuard<util::Mutex> Guard;

	explicit State(PriorityJobAllocator &alloc);


	bool isStateClosedDirect() const;
	bool isStateClosed(Guard&) const;
	bool isCloseable(Guard&) const;
	void closeState(Guard&);


	bool isDeployed(Guard&) const;
	bool isJobDeployable(Guard&) const;
	bool isPreTaskDeployable(Guard&) const;
	void setDeployed(Guard&);


	bool isOperatable(Guard&) const;


	void applyResultCompleted(Guard&, SQLFetchContext &fetchContext) const;
	void setResultCompletedState(Guard&);


	bool isRemoteWorking(Guard&) const;
	void getWorkingNodeSet(Guard&, NodeSet &nodeSet) const;
	void addWorkingNode(Guard&, const JobNode &node);
	void clearWorkingNodeSet(Guard&);


	bool isAllRemoteProfiled(Guard&) const;
	void addProfilingNode(Guard&, const JobNode &node, bool enabled);
	void removeProfilingNode(Guard&, const JobNode &node);


	bool isAllRemoteClosed(Guard&) const;
	bool isRemoteCoordinatorClosing(Guard&, const JobNode &coordinator) const;
	void addClosingNodeIfEffective(Guard&, const JobNode &node);
	void removeClosingNode(
			Guard&, const JobNode &node, bool &allRemovedByLastOp);
	void clearClosingNodeSet(Guard&);

	void addDisabledNodeOptional(Guard&, const JobNode *node);


	bool isTaskWorking(Guard&) const;
	PriorityTaskMap& getWorkingTaskMap(Guard&);
	void clearWorkingTasks(Guard&);


	bool isTaskOperating();
	void addOperatingTasks();
	void removeOperatingTasks();


	bool isOutputPending(Guard&) const;
	bool findOutputPendingTask(Guard&, const PriorityTask &task) const;
	void addOutputPendingTask(Guard&, const PriorityTask &task);
	bool tryRemoveOutputPendingTask(Guard&, const PriorityTask &task);


	bool findOnceActivatedTask(Guard&, const PriorityTask &task) const;
	void addOnceActivatedTask(Guard&, const PriorityTask &task);


	bool isCancellable(Guard&) const;
	bool isExceptionAssigned(Guard&) const;
	bool findException(
			Guard&, util::LocalUniquePtr<util::Exception> &dest) const;
	bool trySetException(Guard&, const util::Exception &src);


	void stateToStatProfile(
			Guard&, util::StackAllocator &alloc,
			StatJobProfilerInfo::State &info) const;

	static void exceptionsToStringList(
			Guard&, util::StackAllocator &alloc,
			const util::Exception *exception, util::Vector<util::String> &list);
	static void exceptionToString(
			Guard&, const util::Exception &exception, util::String &str);

	static void nodeSetToStringList(
			Guard&, util::StackAllocator &alloc, const NodeSet &nodeSet,
			util::Vector<util::String> &list);
	static void taskMapToIdList(
			Guard&, const PriorityTaskMap &taskMap,
			util::Vector<int32_t> &taskList);

	static util::Vector<int32_t>* taskSetToIdListOptional(
			Guard&, util::StackAllocator &alloc, const TaskSet &taskSet);
	static void taskSetToIdList(
			Guard&, const TaskSet &taskSet, util::Vector<int32_t> &taskList);

private:
	util::Atomic<bool> closed_;
	bool deployed_;
	bool resultCompleted_;

	NodeSet workingNodeSet_;
	NodeSet profilingNodeSet_;
	NodeSet closingNodeSet_;
	NodeSet disabledNodeSet_;

	PriorityTaskMap workingTaskMap_;
	util::Atomic<int64_t> operatingTaskCount_;

	TaskSet outputPendingTaskSet_;
	TaskSet onceActivatedTaskSet_;

	util::LocalUniquePtr<util::Exception> exception_;
};

struct PriorityJob::Body {
	Body(
			const Source &source, PriorityJobAllocator &baseAlloc,
			util::Mutex &baseLock);

	PriorityJobAllocator &baseAlloc_;
	util::Mutex &baseLock_;

	util::Atomic<uint64_t> refCount_;
	PriorityJobController &controller_;

	util::AllocatorLimitter allocLimitter_;

	PriorityJobHolders::VarAllocator alloc_;
	PriorityJobHolders::Lock lock_;
	PriorityJobHolders::Lock senderLock_;

	JobId id_;
	JobNode coordinator_;
	bool profiling_;

	TaskList taskList_;
	TaskList preTaskList_;

	util::AllocUniquePtr<PriorityTask> resultTask_;
	TaskSet selfTaskSet_;
	TaskList ddlTaskList_;

	NodeSet participantNodeSet_;
	NodeSet relatedNodeSet_;

	State state_;

	uint32_t concurrency_;
	uint32_t restrictionFlags_;
	util::LocalUniquePtr<uint32_t> nextPriority_;

	PriorInputMap priorInputMap_;
	PriorityList priorityList_;

	NodeSet priorityPendingNodeSet_;
	NodePriorityMap sendingPriorityMap_;

	TaskProgressSet progressSet_;

	WorkerSet workerSet_;

	JobMessages::VarByteXArray processorData_;
	PriorityJobConfig config_;
	util::LocalUniquePtr<PriorityTaskConfig> taskConfig_;

	PriorityJobStats lastStats_;
	NodeTimeMap lastRemoteWorkTime_;

	int64_t lastJobWorkTime_;
	int64_t lastActivityCheckTime_;
	uint32_t lastActivityFlags_;

	uint32_t replyFlags_;
	int64_t replyTimestamp_;
};

/**
 * ジョブ制御機構におけるタスク表現
 */
class PriorityTask {
public:
	enum OperationType {
		OP_NONE,
		OP_PIPE,
		OP_FINISH,
		OP_NEXT
	};

	struct Source;
	struct InputOption;

	typedef PriorityJob::NodeSet NodeSet;
	typedef NodeSet::const_iterator NodeSetIterator;
	typedef std::pair<NodeSetIterator, NodeSetIterator> NodeSetRange;
	typedef std::pair<int32_t, int32_t> DdlAckStatus;

	explicit PriorityTask(const PriorityTaskBodyBase *base = NULL);
	~PriorityTask();

	PriorityTask(const PriorityTask &another);
	PriorityTask& operator=(const PriorityTask &another);

	bool operator==(const PriorityTask &another) const;
	bool operator<(const PriorityTask &another) const;

	void setUp(util::LockGuard<util::Mutex>&, const PriorityJobConfig &config);
	void setUpForPre(
			util::LockGuard<util::Mutex>&, const PriorityJobConfig &config,
			uint32_t outputCount);

	bool isEmpty() const;
	TaskId getId() const;
	SQLType::Id getType() const;

	PriorityJob getJob() const;
	JobNode getNode() const;
	PriorityJobWorker& getWorker() const;
	uint32_t getLoadBalance() const;

	uint32_t getInputCount() const;
	bool findInputTask(uint32_t input, PriorityTask &task) const;
	PriorityTask getInputTask(uint32_t input) const;
	const PriorityTask& getInputTaskDirect(
			util::LockGuard<util::Mutex>&, uint32_t input) const;

	uint32_t getOutputCount() const;
	bool findOutputTask(uint32_t output, PriorityTask &task) const;
	PriorityTask getOutputTask(uint32_t output) const;
	const PriorityTask& getOutputTaskDirect(
			util::LockGuard<util::Mutex>&, uint32_t output) const;

	uint32_t getOutputForInputTask(uint32_t input) const;
	uint32_t getInputForOutputTask(uint32_t output) const;

	NodeSetRange getOutputNodeSet() const;

	bool findPriorTask(PriorityTask &task) const;
	bool isPriorTaskPossible() const;
	bool findProgressDirect(
			util::LockGuard<util::Mutex>&, TaskProgressKey &key,
			TaskProgressValue &value) const;
	void setProgress(
			const TaskProgressKey &key, const TaskProgressValue *value);

	void setInputPriority(uint32_t priorInput);

	bool addInput(
			PriorityJobContext &cxt, uint32_t input, const InputOption &option,
			const TupleList::Block &block);
	bool addInputUnbuffered(
			PriorityJobContext &cxt, uint32_t input, const InputOption &option,
			const JobUnbufferedBlock &block);
	void finishInput(PriorityJobContext &cxt, uint32_t input);

	bool addInputDirect(
			PriorityJobContext &cxt, uint32_t input, const JobUnbufferedBlock &block);
	bool finishInputDirect(
			PriorityJobContext &cxt, uint32_t input);

	void addOutput(PriorityJobContext &cxt, const TupleList::Block &block);
	void finishOutput(PriorityJobContext &cxt);

	void acceptBlockOutput(uint32_t output);
	void acceptFinishOutput(PriorityJobContext &cxt);
	void reportOutputSent(bool networkBusy, uint64_t transSize);

	void growOutputCapacity(
			PriorityJobContext &cxt, uint32_t output, uint64_t minCapacity);
	void acceptPreOutputCapacity(util::LockGuard<util::Mutex> &guard);
	bool isOutputRestricted(uint32_t output);

	void setActive(PriorityJobContext &cxt, bool active);
	bool isActive();

	void setSchemaCheckMode(
			util::LockGuard<util::Mutex>&, bool checkOnly, bool pending);
	bool isSchemaCheckRequired();

	void closeInput(PriorityJobContext &cxt);
	void closeOutput(PriorityJobContext &cxt);

	OperationType prepareOperation(
			PriorityJobContext &cxt, SQLProcessor *&proc,
			SQLProcessor::Context *&procCxt, uint32_t &input,
			TupleList::Block &block, bool &withFinish);

	void acceptProcessorResult(
			PriorityJobContext &cxt, SQLProcessor *proc,
			SQLProcessor::Context *procCxt, OperationType op, bool remaining,
			bool &continuable);
	void handleProcessorException(
			PriorityJobContext &cxt, std::exception &exception);

	void encodeProcessor(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			JobMessages::StackOutStream &out);
	bool getFetchContext(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			SQLFetchContext &fetchContext);
	void setDdlAckStatus(
			PriorityJobContext &cxt, const DdlAckStatus &ackStatus);

	TaskProfiler getProfile(util::StackAllocator &alloc);
	void getStatProfile(
			util::LockGuard<util::Mutex>&, StatTaskProfilerInfo &info);
	void getResourceProfile(
			util::LockGuard<util::Mutex>&, JobResourceInfo &info, bool byTask,
			bool limited);

	TaskProfilerInfo& getProfileInfo(
			util::LockGuard<util::Mutex>&, util::StackAllocator &alloc);
	void setProfileInfo(
			util::LockGuard<util::Mutex>&, const TaskProfilerInfo &info);

	void updateOutputProfile(bool increased) const;

	void getPlanString(util::String &str);
	void getPlanJson(
			util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
			picojson::value &value, bool withProfile, const size_t *limit);

	static void writeTimeStrOptional(int64_t timestamp, util::String &str);

	static const char8_t* getOperationTypeName(OperationType op);

private:
	class InterruptionHandler;
	class Processor;
	class Profiler;
	class InputProfiler;

	struct Body;

	struct InputEntry;
	struct OutputEntry;

	struct Constants;

	typedef JobManager::JobId JobId;
	typedef JobManager::TaskInfo TaskInfo;

	typedef util::AllocDeque<JobUnbufferedBlock> BlockList;
	typedef util::AllocVector<InputEntry> InputList;
	typedef util::AllocVector<OutputEntry> OutputList;

	typedef util::AllocSet<uint32_t> InputIdSet;

	typedef SQLProcessor::TupleInfo TupleInfo;
	typedef SQLProcessor::TupleInfoList TupleInfoList;

	typedef util::AllocVector<TupleList::TupleColumnType> AllocTupleInfo;
	typedef util::AllocVector<AllocTupleInfo> AllocTupleInfoList;

	typedef util::AllocVector<JobNode> NodeList;

	typedef std::pair<TaskProgressKey, TaskProgressValue> TaskProgressEntry;

	void addReference() const;
	void removeReference();

	Body& getBody();
	const Body& getBody() const;

	static bool preparePipe(
			PriorityJobContext &cxt, Body &body, uint32_t &input,
			TupleList::Block &block, bool &busy, bool &withFinish);
	static bool prepareFinish(
			PriorityJobContext &cxt, Body &body, uint32_t &input,
			bool &blockFound);
	static bool prepareNext(Body &body);

	static void acceptInputFinish(
			PriorityJobContext &cxt, Body &body, uint32_t input);

	static void startOperation(
			PriorityJobContext &cxt, Body &body, SQLProcessor &proc,
			PriorityTask &task);
	static void finishOperation(PriorityJobContext &cxt, Body &body);
	static void closeOperation(Body &body);

	static bool checkOperatable(Body &body, bool withActivity);
	static bool checkCancelSilently(Body &body);

	static void handleProcessorExceptionDetail(
			PriorityJobContext &cxt, Body &body, std::exception &exception);

	static void setProgressDetail(
			util::LockGuard<util::Mutex>&, PriorityTask &task,
			const TaskProgressKey &key, const TaskProgressValue *value);

	static void acceptBlockInput(
			PriorityJobContext &cxt, Body &body, uint32_t input, bool forPre);
	static void updateFinishState(PriorityJobContext &cxt, PriorityTask &task);

	static bool isInputFinished(Body &body);
	static bool isOutputFinished(util::LockGuard<util::Mutex>&, Body &body);

	static bool popPendingBlockInput(
			Body &body, uint32_t &input, TupleList::Block &block,
			bool &withFinish);
	static bool popPendingFinalInput(
			Body &body, uint32_t &input, bool &blockFound);

	static void checkInputAcceptable(InputEntry &entry);
	static bool updateInputCapacity(Body &body, InputEntry &entry);

	static void applyOutputRestriction(
			util::LockGuard<util::Mutex> &guard, Body &body,
			const OutputEntry &entry, bool prev);
	static bool resolveOutputRestriction(
			util::LockGuard<util::Mutex>&, const OutputEntry &entry);
	static bool isAllOutputRestricted(
			util::LockGuard<util::Mutex>&, const Body &body);

	static void addPreInputRequest(util::LockGuard<util::Mutex>&, Body &body);
	static void acceptPreInputRequest(Body &body, PriorityJobContext &cxt);
	static bool findPreInputTask(
			Body &body, PriorityTask &task, bool &pending);
	static bool isPreInputTaskPending(Body &body);

	static PriorityJobWorker& getOutputWorker(Body &body);

	static bool isInputEmpty(const Body &body);
	static JobMessages::TaskIOInfo getIOInfo(const Body &body);

	static InputEntry& getInputEntry(Body &body, uint32_t input);
	static const InputEntry& getInputEntry(const Body &body, uint32_t input);

	static OutputEntry& getOutputEntry(Body &body, uint32_t output);
	static const OutputEntry& getOutputEntry(const Body &body, uint32_t output);

	static bool isInputAllowed(util::LockGuard<util::Mutex>&, const Body &body);
	static bool isOutputAllowed(util::LockGuard<util::Mutex>&, const Body &body);

	static Body& errorEmptyBody();

	static bool errorInputSequence();
	static InputEntry& errorInput(uint32_t input);
	static OutputEntry& errorOutput(uint32_t output);

	static TaskId errorTaskId();
	static const TaskInfo& errorTaskInfo();
	static const JobNode& errorNode();
	static SQLProcessor& errorProcessorData();
	static JobMessages::ByteInStream errorProcessorDataAsStream();
	static void errorProcessorClosed();

	Body *body_;
};

struct PriorityTask::Source {
	Source(
			util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
			PriorityJobController &controller, PriorityJobAllocator &alloc,
			util::Mutex &lock, PriorityJob &job, bool onCoordinator);

	TaskId resolveId() const;
	TaskId resolveMainId() const;

	JobNode resolveNode() const;

	PriorityJobWorker& resolveWorker() const;
	PriorityJobWorker& resolveOutputWorker() const;
	int32_t resolveLoadBalance() const;

	InputList makeInitialInputList() const;

	SQLType::Id resolveType() const;
	const TaskInfo& resolveInfo() const;

	size_t resolveReservationMemorySize() const;

	util::LockGuard<util::Mutex> &guard_;
	PriorityJobContext &cxt_;
	PriorityJobController &controller_;
	PriorityJobAllocator &alloc_;
	util::Mutex &lock_;
	PriorityJob &job_;

	TaskId baseId_;
	const PriorityJobInfo *jobInfo_;
	const TaskInfo *info_;
	const NodeList *nodeList_;
	NoSQLSyncContext *syncContext_;

	bool onCoordinator_;
	bool forPreInput_;
	bool profiling_;
	bool failOnMemoryExcess_;
	TaskOption option_;
	std::pair<uint32_t, const void*> processorData_;
};

struct PriorityTask::InputOption {
	InputOption(bool networkBusy, uint64_t sequenceNum);

	static InputOption empty();

	bool networkBusy_;
	uint64_t sequenceNum_;
};

class PriorityTask::InterruptionHandler :
		public InterruptionChecker::CheckHandler {
public:
	InterruptionHandler(SQLContext &procCxt, const PriorityJob &job);
	virtual ~InterruptionHandler();

	virtual bool operator()(InterruptionChecker &checker, int32_t flags);

private:
	SQLContext &procCxt_;
	PriorityJob job_;
	EventMonotonicTime intervalMillis_;
};

class PriorityTask::Processor {
public:
	explicit Processor(const Source &source);
	~Processor();

	void close(util::LockGuard<util::Mutex>*);
	bool isClosed() const;

	SQLProcessor& prepare(PriorityJobContext &cxt, const PriorityJob &job);
	SQLContext& prepareContext(const PriorityJob &job);

	void applyContext(util::LockGuard<util::Mutex>&, PriorityTask &task);

	void encode(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const PriorityJob &job, JobMessages::StackOutStream &out);

	void getPlanString(
			PriorityJobAllocator &varAlloc, util::String &str,
			bool limited) const;
	void getPlanJson(
			util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
			picojson::value &value, const size_t *limit) const;
	void formatStatement(
			const char8_t *src, util::String &dest, bool limited) const;

	SQLType::Id getType() const;

	void startProcessorOperation(PriorityJobContext &cxt, PriorityTask &task);
	void finishProcessorOperation();

	bool checkOperationContinuable(PriorityJobContext &cxt);

	void applyStats(Profiler &profiler);
	void applyStoreGroup(
			util::LockGuard<util::Mutex>&, const TupleList::Block &block);

	bool checkInputBusy(
			PriorityJobContext &cxt, const PriorityJob &job, Profiler &profiler);

	bool getFetchContext(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const PriorityJob &job, SQLFetchContext &fetchContext);
	void setDdlAckStatus(const DdlAckStatus &ackStatus);

	static bool isSchemaCheckRequiredType(
			SQLType::Id type, uint8_t serviceType);
	static bool isResultType(SQLType::Id type);
	static bool isDdlType(SQLType::Id type);

private:
	typedef util::AllocVector<DdlAckStatus> DdlAckStatusList;

	util::StackAllocator& prepareStackAllocator(
			util::AllocatorLimitter &allocLimitter);
	PriorityJobAllocator& prepareVarAllocator(
			util::AllocatorLimitter &allocLimitter);

	static bool isFailOnTotalMemoryLimit(
			const Source &source, SQLType::Id type);

	bool isBaseRequired(const Source &source) const;
	bool isPlanDataRequired(const Source &source) const;

	SQLProcessor& prepareDetail(
			PriorityJobContext &cxt, const PriorityJob &job,
			const Source *source);
	SQLProcessor& createBase(
			PriorityJobContext &cxt, SQLContext &procCxt, const Source *source,
			util::AllocatorLimitter &allocLimitter);

	void setUpBase(SQLProcessor &base, const PriorityJob &job);
	void setUpContext(
			SQLContext &procCxt, const PriorityJob &job,
			util::AllocatorLimitter &allocLimitter);

	void setUpContextEnvironment(
			SQLContext &procCxt, PriorityJobEnvironment &env);
	void setUpContextTaskInfo(SQLContext &procCxt, const PriorityJob &job);

	InterruptionHandler& prepareInterruptionHandler(
			SQLContext &procCxt, const PriorityJob &job);
	TupleInfoList& prepareInputInfo(util::AllocatorLimitter &allocLimitter);
	AllocTupleInfoList makeBaseInputInfo(const Source &source);

	util::AllocatorLimitter& prepareAllocatorLimitter(const PriorityJob &job);

	void preparePlanData(const Source &source);

	void setDdlAckStatusDetail(
			util::LockGuard<util::Mutex>&, const DdlAckStatus &ackStatus);
	bool applyDdlAckStatus(DDLProcessor &proc);

	JobMessages::ByteInStream getBaseInStream() const;
	JobMessages::ByteInStream getPlanInStream() const;
	JobMessages::ByteInStream getTotalInStream() const;

	void encodeBase(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const PriorityJob &job, JobMessages::StackOutStream &out);
	void encodePlan(JobMessages::StackOutStream &out);

	void applyMemoryStats(Profiler &profiler);
	void applyTempStoreStats(Profiler &profiler);

	uint32_t getColumnValueLimit() const;

	void checkNotClosed();

	util::Mutex &lock_;

	PriorityJobController &controller_;
	util::LocalUniquePtr<util::AllocatorLimitter> allocLimitter_;

	SQLType::Id type_;
	AllocTupleInfoList baseInputInfo_;
	std::pair<uint32_t, const void*> data_;
	util::AllocXArray<uint8_t> planData_;

	size_t reservationMemorySize_;
	bool failOnTotalMemoryLimit_;
	bool closed_;

	Event event_;
	NoSQLSyncContext *syncContext_;
	util::LocalUniquePtr<DdlAckStatusList> ddlAckStatusList_;

	util::LocalUniquePtr<PriorityJobHolders::Allocator> stackAlloc_;
	util::LocalUniquePtr<PriorityJobHolders::VarAllocator> varAlloc_;
	util::LocalUniquePtr<util::StackAllocator::Scope> allocScope_;

	util::LocalUniquePtr<InterruptionHandler> interruptionHandler_;
	util::AllocUniquePtr<SQLContext> procCxt_;
	util::LocalUniquePtr<SQLProcessor::Holder> base_;
	util::LocalUniquePtr<LocalTempStore::Group> localGroup_;
	util::LocalUniquePtr<LocalTempStore::GroupId> groupId_;
	util::LocalUniquePtr<TupleInfoList> inputInfo_;

	JobId localJobId_;
};

class PriorityTask::Profiler {
public:
	explicit Profiler(const Source &source);

	void finishProcessor(const Body &body, SQLProcessor &proc);

	void startProfilerOperation(PriorityJobContext &cxt, SQLProcessor &proc);
	void finishProfilerOperation(PriorityJobContext &cxt);

	void acceptOperationCheck(PriorityJobContext &cxt);

	void acceptError();
	void acceptInputBusy(bool inputBusy);

	void startTransfer(util::LockGuard<util::Mutex>&, bool networkBusy);
	void finishTransfer(
			util::LockGuard<util::Mutex>&, bool networkBusy,
			uint64_t transSize);

	void flipStats(PriorityJobStats &prev, PriorityJobStats &next);

	void acceptMemoryStats(int64_t usage);
	void acceptTempStoreStats(
			const LocalTempStore::GroupStats &stats, uint32_t blockSize);

	void updateInputProfile(bool increased);
	void updateOutputProfile(bool increased);

	static void getStatProfile(
			util::LockGuard<util::Mutex>&, const PriorityTask &task,
			const Body &body, StatTaskProfilerInfo &info);
	static void getResourceProfile(
			util::LockGuard<util::Mutex>&, const Body &body,
			JobResourceInfo &info, bool byTask, bool limited);

	static TaskProfilerInfo& getProfile(
			util::LockGuard<util::Mutex>&, util::StackAllocator &alloc,
			const Body &body);
	static void setProfile(
			util::LockGuard<util::Mutex>&, Body &body,
			const TaskProfilerInfo &info);

private:
	void startProcessor(PriorityJobContext &cxt, SQLProcessor &proc);

	void enableCustomProfile(PriorityJobContext &cxt, SQLProcessor &proc);
	void acceptCustomProfile(const Body &body, SQLProcessor &proc);

	static void jobIdToString(const JobId &jobId, util::String &str);

	util::XArray<uint8_t>* getCustomProfile(
			util::LockGuard<util::Mutex>&, util::StackAllocator &alloc) const;
	template<typename Alloc>
	void setCustomProfile(
			util::LockGuard<util::Mutex>&,
			const util::XArray<uint8_t, Alloc> &src);

	static const PartitionListStats* preparePartitionListStats(
			const Source &source);
	static uint32_t prepareChunkSize(
			const PartitionListStats *plStats, const Source &source);
	static util::AllocUniquePtr<ChunkBufferStats>::ReturnType
	prepareChunkBufferStats(
			const PartitionListStats *plStats, const Source &source);

	void startChunkBufferOperation(
			PriorityJobContext &cxt, const PartitionListStats &plStats);
	void finishChunkBufferOperation(
			PriorityJobContext &cxt, const PartitionListStats &plStats);

	template<typename P>
	static uint64_t getChunkBufferStatsDiff(
			const ChunkBufferStats &prev, const ChunkBufferStats &next,
			P param);

	static bool isNetworkTransferPossible(
			util::LockGuard<util::Mutex>&, const Body &body);

	uint32_t resolveStatusFlags(
			util::LockGuard<util::Mutex>&, const Body &body) const;

	static util::Vector<TaskId>* getInputTaskList(
			util::LockGuard<util::Mutex>&, const Body &body,
			util::StackAllocator &alloc, bool workingOnly);
	static util::Vector<TaskId>* getOutputTaskList(
			util::LockGuard<util::Mutex>&, const Body &body,
			util::StackAllocator &alloc, bool workingOnly, bool inactiveOnly);

	bool enabled_;
	bool inputBusy_;
	bool opWorking_;
	bool errorOccurred_;

	util::Stopwatch procWatch_;
	util::Stopwatch opWatch_;
	util::Stopwatch transWatch_;
	util::Stopwatch busyTransWatch_;

	int64_t leadTime_;
	int64_t actualTime_;
	int64_t executionCount_;
	util::AllocXArray<uint8_t> customData_;

	int64_t procStartTimestamp_;
	int64_t opCheckTimestamp_;

	uint64_t transCount_;
	uint64_t busyTransCount_;

	int64_t lastMemoryUsage_;
	int64_t peakMemoryUsage_;

	uint64_t activeBlockCount_;
	uint64_t swapInCount_;
	uint64_t swapOutCount_;

	util::Atomic<int64_t> inputBlockCount_;
	util::Atomic<int64_t> outputBlockCount_;

	PriorityJobStats prevStats_;
	PriorityJobStats stats_;

	const PartitionListStats *plStats_;
	uint32_t chunkSize_;
	util::AllocUniquePtr<ChunkBufferStats> bufStats_;
};

class PriorityTask::InputProfiler {
public:
	InputProfiler();

	void acceptBlock(const TupleList::Block &block);

	int64_t getRowCount() const;
	void setRowCount(int64_t count);

private:
	int64_t rowCount_;
};

struct PriorityTask::Body {
	explicit Body(const Source &source);

	util::Atomic<uint64_t> refCount_;
	PriorityJobAllocator &alloc_;
	util::Mutex &lock_;

	PriorityJob job_;

	TaskId id_;
	TaskId mainId_;

	JobNode node_;
	NodeSet outputNodeSet_;

	PriorityJobWorker &worker_;
	PriorityJobWorker &outputWorker_;
	uint32_t loadBalance_;

	InputList inputList_;
	OutputList outputList_;

	InputIdSet pendingBlockInputs_;
	InputIdSet pendingFinalInputs_;
	uint32_t finishedInputCount_;
	uint32_t finishedOutputCount_;
	uint32_t restrictedOutputCount_;
	uint32_t activatedInputCount_;
	util::LocalUniquePtr<uint32_t> priorInput_;

	bool inputClosed_;
	bool outputClosed_;
	bool processorFinished_;

	bool active_;
	bool nextRemaining_;

	bool schemaCheckOnly_;
	bool schemaCheckPending_;
	bool forPreInput_;
	uint64_t requestedPreInputCount_;
	uint64_t acceptedPreInputCount_;

	bool progressAssigned_;
	TaskProgressEntry progress_;

	Processor processor_;
	Profiler profiler_;
};

struct PriorityTask::InputEntry {
	InputEntry(
			PriorityJobAllocator &alloc, const PriorityJobConfig &config,
			TaskId taskId);

	TaskId taskId_;
	uint32_t output_;

	BlockList blockList_;
	bool finishPending_;
	bool finishPrepared_;

	uint64_t capacity_;
	uint64_t acceptedSize_;
	uint64_t preAcceptedSize_;

	InputProfiler profiler_;
};

struct PriorityTask::OutputEntry {
	explicit OutputEntry(uint64_t initialCapacity);

	static OutputEntry ofConfig(const PriorityJobConfig &config);

	TaskId taskId_;
	uint32_t input_;

	uint64_t capacity_;
	uint64_t acceptedSize_;
	bool entryClosed_;
};

struct PriorityTask::Constants {
	static const char8_t *const PLAN_STRING_KEYS[];
};

class PriorityJobTable {
public:
	struct EntryBodyKey;

	PriorityJobTable();
	~PriorityJobTable();

	void close();

	PriorityJob resolveJob(const PriorityJob::Source &source);
	bool findJob(const JobId &id, PriorityJob &job);
	void removeJob(const JobId &id);

	PriorityTask createTask(const PriorityTask::Source &source);

	void listJobIds(util::Vector<JobId> &idList);
	void listJobs(util::Vector<PriorityJob> &jobList);
	size_t getJobCount();

private:
	struct EntryBodyKeyBase;

	typedef util::AllocMap<JobId, PriorityJob> JobMap;

	void removeAll();

	static PriorityJob errorClosed();
	static PriorityJob errorJobId();

	PriorityJobAllocator alloc_;
	util::Mutex lock_;

	static JobMap& makeJobMap(
			PriorityJobAllocator &alloc,
			util::LocalUniquePtr<JobMap> &jobMapBase) {
		jobMapBase = UTIL_MAKE_LOCAL_UNIQUE(jobMapBase, JobMap, alloc);
		return *jobMapBase;
	}

	bool closed_;
	util::LocalUniquePtr<JobMap> jobMapBase_;
	JobMap &jobMap_;
};

struct PriorityJobTable::EntryBodyKey {
	explicit EntryBodyKey(const EntryBodyKeyBase&);
};

struct PriorityJobTable::EntryBodyKeyBase {
};

class PriorityJobBodyBase {
public:
	PriorityJobBodyBase(
			const PriorityJobTable::EntryBodyKey&,
			const PriorityJob::Source &source, PriorityJobAllocator &alloc,
			util::Mutex &lock);

	const PriorityJob::Source& getSource() const;
	PriorityJobAllocator& getAllocator() const;
	util::Mutex& getLock() const;

private:
	PriorityJob::Source source_;
	PriorityJobAllocator &alloc_;
	util::Mutex &lock_;
};

class PriorityTaskBodyBase {
public:
	PriorityTaskBodyBase(
			const PriorityJobTable::EntryBodyKey&,
			const PriorityTask::Source &source);

	const PriorityTask::Source& getSource() const;

private:
	PriorityTask::Source source_;
};

/**
 * ジョブ制御機構における、タスク実行・タスク間IO処理を担うワーカ
 */
class PriorityJobWorker {
public:
	class Engine;

	class DefaultHandler;
	class DefaultEngine;

	struct Info;
	struct Source;

	explicit PriorityJobWorker(const Source &source);

	const Info& getInfo() const;

	void execute(PriorityJobContext &cxt);

	void removeJob(const PriorityJob &job);
	void removeTask(const PriorityTask &task);

	void putTask(PriorityJobContext &cxt, const PriorityTask &task);

	void addTaskInput(
			PriorityJobContext &cxt, const PriorityTask &task, uint32_t input,
			const TupleList::Block &block);
	void addTaskInputUnbuffered(
			PriorityJobContext &cxt, const PriorityTask &task, uint32_t input,
			const JobUnbufferedBlock &block);
	void finishTaskInput(
			PriorityJobContext &cxt, const PriorityTask &task, uint32_t input);
	void removeTaskInput(const PriorityTask &task);

	void addTaskOutput(
			PriorityJobContext &cxt, const PriorityTask &task,
			const TupleList::Block &block);
	void finishTaskOutput(
			PriorityJobContext &cxt, const PriorityTask &task);
	void removeTaskOutput(const PriorityTask &task);

	void resumeNodeOutput(PriorityJobContext &cxt, const JobNode &node);

	void reportSent(PriorityJobContext &cxt, uint64_t seq);

	void validateNodes(PriorityJobContext &cxt);
	void invalidateNode(PriorityJobContext &cxt, const JobNode &node);

	void accpetNodeValidationAck(
			PriorityJobContext &cxt, const JobNode &node,
			uint64_t validationCode);

	bool checkJobBusy(const PriorityJob &job, int64_t prevCheckTime);
	uint64_t getOutputWorkingCount(const PriorityTask &task);

	void getStatProfile(
			PriorityJobContext &cxt, StatWorkerProfilerInfo &info);

private:
	enum QueueTag {
		TAG_INPUT,
		TAG_OUTPUT,
		TAG_NORMAL_TASK
	};

	struct InputEntry;
	struct OutputEntry;

	struct TaskEntry;
	struct JobEntry;
	struct JobQueue;

	struct OutputNodeState;

	class BlockSendingScope;
	class Profiler;

	typedef LocalTempStore::BlockId BlockId;

	typedef std::pair<uint32_t, OutputEntry> SubOutputEntry;

	typedef util::AllocDeque<InputEntry> InputList;
	typedef util::AllocDeque<OutputEntry> OutputList;
	typedef util::AllocMap<uint32_t, OutputList> OutputMap;

	typedef util::StdAllocator<TaskEntry, void> TaskAlloc;
	typedef std::list<TaskEntry, TaskAlloc> TaskList;

	typedef TaskList::iterator TaskIterator;
	typedef util::AllocMap<PriorityTask, TaskIterator> TaskIteratorMap;

	typedef util::StdAllocator<JobEntry, void> JobAlloc;
	typedef std::list<JobEntry, JobAlloc> JobList;

	typedef JobList::iterator JobIterator;
	typedef util::AllocMap<PriorityJob, JobIterator> JobIteratorMap;

	typedef std::pair<QueueTag, JobNode> QueueKey;
	typedef util::AllocMap<QueueKey, JobQueue> PriorityQueue;

	typedef util::AllocSet<QueueKey> QueueKeySet;
	typedef util::AllocMap<PriorityJob, QueueKeySet> QueueKeyMap;

	typedef util::AllocMap<uint64_t, JobNode> SequenceNodeMap;
	typedef util::AllocMap<JobNode, OutputNodeState> NodeSequenceMap;
	typedef util::AllocSet<JobNode> NodeSet;

	void addInputEntry(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const PriorityTask &task, const InputEntry &entry);
	void addOutputEntry(
			PriorityJobContext &cxt, const PriorityTask &task,
			const OutputEntry &entry);

	bool acceptIO(PriorityJobContext &cxt);
	bool acceptInput(PriorityJobContext &cxt);
	bool acceptOutput(PriorityJobContext &cxt);

	void poolBufferedBlock(PriorityJobContext &cxt);

	bool popNormalTask(
			PriorityJobContext &cxt, bool ioAccepted, PriorityTask &task);
	bool popNormalTaskAt(
			PriorityJobContext &cxt, bool ioAccepted,
			const PriorityTask *baseTask, bool &withExtraCheck,
			PriorityTask &task);

	bool popInputEntry(
			util::LockGuard<util::Mutex>&, PriorityTask &task,
			InputEntry &entry);
	bool popOutputEntry(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			PriorityTask &task, std::pair<uint32_t, OutputEntry> &entry,
			bool &busy);

	bool checkNodeOutputPending(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const JobNode &node);
	void prepareNodeOutputValidation(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const JobNode &node, OutputNodeState &state);

	void putTaskAt(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const QueueKey &key, const PriorityTask &task,
			const InputEntry *inEntry, const SubOutputEntry *outEntry);

	TaskEntry* putTaskEntry(
			util::LockGuard<util::Mutex>&, const QueueKey &key,
			const PriorityTask &task);
	void setUpTaskEntry(
			util::LockGuard<util::Mutex>&, TaskEntry &taskEntry,
			const InputEntry *inEntry, const SubOutputEntry *outEntry);

	void removeJobAt(
			util::LockGuard<util::Mutex>&, const PriorityJob &job);

	void rotateJobList(
			util::LockGuard<util::Mutex>&, const QueueKey &key);

	void removeTaskEntry(
			util::LockGuard<util::Mutex>&, const QueueKey &key,
			const PriorityTask &task, bool jobRotating);
	void removeTaskSubAt(
			util::LockGuard<util::Mutex>&, JobEntry &jobEntry,
			const PriorityTask &task);

	bool searchOtherJob(
			util::LockGuard<util::Mutex>&, const QueueKey &key,
			const PriorityJob &job);
	bool searchTask(
			util::LockGuard<util::Mutex>&, const QueueKey &key,
			const PriorityTask &task);

	bool findFrontTaskEntry(
			util::LockGuard<util::Mutex>&, const QueueKey &key,
			TaskEntry *&entry);
	bool findTaskEntry(
			util::LockGuard<util::Mutex>&, const QueueKey &key,
			const PriorityTask &task, TaskEntry *&entry);

	void putQueueKey(const PriorityJob &job, const QueueKey &key);
	void removeQueueKey(const PriorityJob &job, const QueueKey &key);

	uint64_t startBlockSending(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			const JobNode &node);
	bool finishBlockSending(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			uint64_t seq, JobNode &node);

	static bool findJobQueue(
			PriorityQueue &totalQueue, const QueueKey &key,
			PriorityQueue::iterator &it);
	static bool findJob(
			PriorityQueue &totalQueue, const QueueKey &key,
			const PriorityJob &job, JobIterator &it);

	bool findNextJob(JobQueue &queue, JobIterator &it);
	static bool findNextTask(JobEntry &entry, TaskIterator &it);
	static bool findNextOutput(OutputMap &map, OutputMap::iterator &it);

	void reserveStoreMemory();

	void releaseBlocks(JobEntry &jobEntry);
	void releaseBlocks(TaskEntry &taskEntry);

	void requestExecution(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt);

	static QueueKey inputKey();
	static QueueKey outputKey(const JobNode &node);
	static QueueKey normalTaskKey();

	static void errorOnlyForBackendIO();
	static uint64_t errorOutputPending();
	static uint64_t errorOutputClosed(const JobNode &node);

	PriorityJobAllocator alloc_;
	util::Mutex lock_;

	util::AllocUniquePtr<Info> info_;

	Engine &engine_;
	const PriorityJobConfig config_;
	JobTransporter &transporter_;
	PriorityJobTracer &tracer_;

	uint32_t partitionId_;
	bool forBackendIO_;

	PriorityQueue queue_;
	QueueKeyMap keyMap_;

	NodeSet restrictedNodeSet_;

	SequenceNodeMap seqNodeMap_;
	NodeSequenceMap nodeSeqMap_;
	uint64_t outputSequence_;

	JobBufferedBlockPool &blockPool_;
	bool executionRequested_;

	int64_t lastEventTime_;
};

class PriorityJobWorker::Engine {
public:
	virtual ~Engine();

	virtual void setHandler(JobMessages::Type type, EventHandler &handler) = 0;
	virtual void request(PriorityJobContext &cxt, uint32_t partitionId) = 0;
};

class PriorityJobWorker::DefaultHandler : public EventHandler {
public:
	DefaultHandler(
			PriorityJobWorkerTable &table, uint8_t serviceType, bool forBackend,
			JobBufferedBlockPool &blockPool, PriorityJobTracer &tracer);
	virtual ~DefaultHandler();

	virtual void operator()(EventContext &ec, Event &ev);

private:
	PriorityJobWorkerTable &table_;
	uint8_t serviceType_;
	bool forBackend_;
	JobBufferedBlockPool &blockPool_;
	PriorityJobTracer &tracer_;
};

class PriorityJobWorker::DefaultEngine : public PriorityJobWorker::Engine {
public:
	explicit DefaultEngine(EventEngine &base);
	virtual ~DefaultEngine();

	virtual void setHandler(JobMessages::Type type, EventHandler &handler);
	virtual void request(PriorityJobContext &cxt, uint32_t partitionId);

private:
	EventEngine &base_;
};

struct PriorityJobWorker::Info {
	Info(
			uint8_t serviceType, bool forBackend, uint32_t totalId,
			uint32_t subId);

	uint8_t serviceType_;
	bool forBackend_;

	uint32_t totalId_;
	uint32_t subId_;
};

struct PriorityJobWorker::Source {
	Source(
			Engine &engine, const PriorityJobConfig &config,
			uint32_t partitionId, bool forBackendIO, const Info &info,
			JobBufferedBlockPool &blockPool, JobTransporter &transporter,
			PriorityJobTracer &tracer);

	Engine &engine_;
	PriorityJobConfig config_;

	uint32_t partitionId_;
	bool forBackendIO_;

	Info info_;
	JobBufferedBlockPool &blockPool_;
	JobTransporter &transporter_;
	PriorityJobTracer &tracer_;
};

struct PriorityJobWorker::InputEntry {
	InputEntry(bool forFinish, uint32_t input);

	bool forFinish_;
	uint32_t input_;
	TupleList::Block bufferedBlock_;
	JobUnbufferedBlock block_;
};

struct PriorityJobWorker::OutputEntry {
	explicit OutputEntry(bool forFinish);

	bool forFinish_;
	JobUnbufferedBlock block_;
};

struct PriorityJobWorker::TaskEntry {
	TaskEntry(PriorityJobAllocator &alloc);

	PriorityTask task_;
	InputList inputList_;
	OutputMap outputMap_;
};

struct PriorityJobWorker::JobEntry {
	JobEntry(PriorityJobAllocator &alloc);

	PriorityJob job_;
	TaskList taskList_;
	TaskIterator nextIt_;
	TaskIteratorMap iteratorMap_;
};

struct PriorityJobWorker::JobQueue {
	JobQueue(PriorityJobAllocator &alloc);

	JobList jobList_;
	JobIterator nextIt_;
	JobIteratorMap iteratorMap_;

	int64_t lastJobRotationTime_;
	int64_t prevJobRotationTime_;
};

struct PriorityJobWorker::OutputNodeState {
	OutputNodeState(uint64_t sequence, int64_t sendingTime);

	uint64_t sequence_;
	int64_t sendingTime_;
	int64_t validatingTime_;
	uint64_t validationCode_;
	bool validating_;
};

class PriorityJobWorker::BlockSendingScope {
public:
	BlockSendingScope(
			PriorityJobContext &cxt, PriorityJobWorker &worker, uint64_t seq,
			bool sending);
	~BlockSendingScope();

	void accept();

private:
	void cancel();

	PriorityJobContext &cxt_;
	PriorityJobWorker &worker_;
	uint64_t seq_;
	bool sending_;
	bool accepted_;
};

class PriorityJobWorker::Profiler {
public:
	explicit Profiler(const PriorityJobWorker &worker);

	void getStatProfileAt(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			StatWorkerProfilerInfo &info);

private:
	void getStatBaseProfile(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			StatWorkerProfilerInfo &info);
	void getStatQueueProfile(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			util::Vector<StatWorkerProfilerInfo::QueueEntry> &dest);
	void getStatNodeProfile(
			util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
			util::Vector<StatWorkerProfilerInfo::NodeEntry> &dest);

	static void getStatJobProfile(
			util::StackAllocator &alloc, const JobList &src,
			util::Vector<StatWorkerProfilerInfo::JobEntry> &dest,
			JobList::const_iterator nextIt, int64_t &nextOrdinal);
	static void getStatTaskProfile(
			util::StackAllocator &alloc, const TaskList &src,
			util::Vector<StatWorkerProfilerInfo::TaskEntry> &dest,
			TaskList::const_iterator nextIt, int64_t &nextOrdinal);

	static void getStatInputProfile(
			util::StackAllocator &alloc, const InputList &src,
			util::Vector<StatWorkerProfilerInfo::InputEntry> *&dest);
	static void getStatOutputProfile(
			util::StackAllocator &alloc, const OutputMap &src,
			util::Vector<StatWorkerProfilerInfo::OutputEntry> *&dest);

	static util::String* newStringOptional(
			util::StackAllocator &alloc, const char8_t *src);
	static int64_t toElapsedTime(
			PriorityJobContext &cxt, int64_t monotonicTime);
	static const char8_t* tagToStringOptional(QueueTag tag);

	const PriorityJobWorker &worker_;
};

class PriorityJobWorkerTable {
public:
	struct Source;

	explicit PriorityJobWorkerTable(const Source &source);
	~PriorityJobWorkerTable();

	void setUpHandlers();

	PriorityJobWorker& getTaskWorker(
			uint8_t serviceType, int32_t loadBalance, bool forBackend);
	PriorityJobWorker& getBackendIOWorker();

	PriorityJobWorker& getWorkerDirect(
			uint8_t serviceType, uint32_t ordinal, bool forBackend);

	uint32_t resolveWorkerOrdinal(
			uint8_t serviceType, uint32_t partitionId, bool forBackend);

	void validateNodes(PriorityJobContext &cxt);
	void invalidateNode(PriorityJobContext &cxt, const JobNode &node);

	util::Vector<StatWorkerProfilerInfo>* getStatProfile(
			PriorityJobContext &cxt);

private:
	typedef util::AllocVector<PriorityJobWorker*> WorkerList;

	enum EntryType {
		TYPE_SQL_FRONT,
		TYPE_SQL_BACK,
		TYPE_TRANSACTION,
		END_TYPE
	};

	struct Entry {
		Entry();

		PriorityJobWorker::Engine *engine_;
		util::LocalUniquePtr<PriorityJobWorker::DefaultHandler> handler_;
		util::LocalUniquePtr<WorkerList> workerList_;
		util::LocalUniquePtr<PartitionGroupConfig> pgConfig_;
	};

	void clear();

	void setUpSqlFront(const Source &source);
	void setUpSqlBack(const Source &source);
	void setUpTransaction(const Source &source);

	void setUpEntry(
			EntryType type, PriorityJobWorker::Engine &engine, uint8_t serviceType,
			bool forBackend, uint32_t concurrency, uint32_t partitionCount,
			const Source &source);

	uint32_t resolveWorkerPartition(
			uint8_t serviceType, int32_t loadBalance, bool forBackend,
			bool forIO);
	uint32_t resolveLoadBalance(uint8_t serviceType, int32_t loadBalance);

	static EntryType getEntryType(uint8_t serviceType, bool forBackend);

	PriorityJobAllocator alloc_;
	Entry entryList_[END_TYPE];
	util::Atomic<uint64_t> loadBalanceCounter_;
};

struct PriorityJobWorkerTable::Source {
	Source(
			PriorityJobWorker::Engine &sqlFrontEngine,
			PriorityJobWorker::Engine &sqlBackEngine,
			PriorityJobWorker::Engine &transactionEngine,
			JobBufferedBlockPool &blockPool,
			JobTransporter &transporter,
			PriorityJobTracer &tracer,
			const PriorityJobConfig &config);

	PriorityJobWorker::Engine &sqlFrontEngine_;
	PriorityJobWorker::Engine &sqlBackEngine_;
	PriorityJobWorker::Engine &transactionEngine_;

	JobBufferedBlockPool &blockPool_;
	JobTransporter &transporter_;
	PriorityJobTracer &tracer_;
	PriorityJobConfig config_;
};

class PriorityJobMonitor {
public:
	struct StatsActions;

	PriorityJobMonitor(
			PriorityJobEnvironment &env, PriorityJobTracer &tracer,
			PriorityJobTable &jobTable);
	~PriorityJobMonitor();

	void removeStats(const JobId &jobId, const PriorityJobStats &prev);
	StatsActions updateStats(
			PriorityJobContext &cxt, const JobId &jobId,
			const PriorityJobStats &prev, const PriorityJobStats &next);
	void acceptStatsActions(
			PriorityJobContext &cxt, PriorityJob &job,
			const StatsActions &actions);

	void setReportInterval(int64_t interval);
	void setReportLimit(int64_t limit);
	void setNetworkTimeInterval(int64_t interval);

	void setPlanTraceEnabled(bool enabled);
	void setPlanSizeLimit(uint32_t limit);

	void setMonitoringTotal(PriorityJobStats::Type type, int64_t total);
	void setMonitoringRate(PriorityJobStats::Type type, double rate);
	void setLimitRate(PriorityJobStats::Type type, double rate);
	void setFailOnExcess(PriorityJobStats::Type type, bool enabled);
	void setEssentialLimit(PriorityJobStats::Type type, int64_t value);

	int64_t getPeriodicMonitoringTime();
	bool isFailOnExcess(PriorityJobStats::Type type);

	void applyJobMemoryLimits(
			util::AllocatorLimitter &limitter_, bool &failOnExcess);

private:
	enum {
		ENTRY_COUNT = PriorityJobStats::END_TYPE
	};

	typedef util::AllocMap<JobId, int64_t> JobValueMap;

	struct Entry {
		Entry();

		int64_t current_;
		int64_t total_;

		int64_t threshold_;
		int64_t limitThreshold_;
		int64_t essentialThreshold_;

		double rate_;
		double limitRate_;

		bool failOnExcess_;

		util::LocalUniquePtr<JobValueMap> valueMap_;
	};

	struct HistoryElement {
		HistoryElement(int64_t reportedTime, const JobId &jobId);

		int64_t reportedTime_;
		JobId jobId_;
	};

	typedef util::AllocDeque<HistoryElement> History;
	typedef util::AllocSet<JobId> JobIdSet;
	typedef util::Set<JobId> LocalJobIdSet;
	typedef util::AllocSet<PriorityJobStats::Type> StatTypeSet;

	void updateThreshold(
			util::LockGuard<util::Mutex>&, PriorityJobStats::Type type,
			bool forLimit);

	void acceptStats(
			util::LockGuard<util::Mutex>&, const JobId &jobId,
			const PriorityJobStats &prev, const PriorityJobStats *next,
			uint32_t &reportableTypes, uint32_t &exceededTypes,
			uint32_t &selfExceededTypes);
	bool acceptReport(
			util::LockGuard<util::Mutex>&, const JobId &jobId,
			int64_t currentTime, uint32_t reportableTypes);

	void adjustJobs(
			PriorityJobContext &cxt, uint32_t exceededTypes,
			const JobId *targetJobId);
	void adjustJobAt(
			PriorityJobContext &cxt, PriorityJobStats::Type type,
			PriorityJob &job);

	bool findExceededJob(
			JobId &jobId, PriorityJobStats::Type type,
			LocalJobIdSet &visitedJobs, PriorityJobStatValue &reducibleValue);

	void reportStats(PriorityJobContext &cxt, PriorityJob &job);

	void updatePeriodicMonitoringTime(int64_t currentTime);

	Entry& getEntry(PriorityJobStats::Type type);

	PriorityJobAllocator alloc_;
	util::Mutex lock_;

	PriorityJobEnvironment &env_;
	PriorityJobTracer &tracer_;
	PriorityJobTable &jobTable_;

	History reportHistory_;
	JobIdSet reportedJobs_;

	Entry entryList_[ENTRY_COUNT];

	int64_t reportInterval_;
	int64_t reportLimit_;
	int64_t networkTimeInterval_;

	util::Atomic<bool> planTraceEnabled_;
	util::Atomic<uint32_t> planSizeLimit_;

	int64_t lastIntervalTime_;
	int64_t intervalReportedCount_;

	util::Atomic<int64_t> periodicMonitoringTime_;
};

struct PriorityJobMonitor::StatsActions {
	StatsActions();

	uint32_t exceededTypes_;
	uint32_t selfExceededTypes_;
	bool reportable_;
};

/**
 * ジョブ制御機構全体の実行環境
 *
 * ジョブ制御機構外部との接点となる。
 */
class PriorityJobEnvironment {
public:
	struct Info {
		explicit Info(const PriorityJobConfig &jobConfig);

		PriorityJobConfig jobConfig_;

		SQLVariableSizeGlobalAllocator *globalVarAlloc_;
		util::AllocatorLimitter *totalAllocLimitter_;

		LocalTempStore *store_;
		const SQLProcessorConfig *procConfig_;

		ClusterService *clusterService_;
		PartitionTable *partitionTable_;
		TransactionService *transactionService_;
		TransactionManager *transactionManager_;
		DataStoreConfig *dataStoreConfig_;
		PartitionList *partitionList_;
		SQLExecutionManager *executionManager_;
		JobManager *jobManager_;

		EventEngine::VariableSizeAllocator *eeVarAlloc_;
		EventEngine::FixedSizeAllocator *eeFixedAlloc_;
		PriorityJobTracer *customTracer_;
	};

	explicit PriorityJobEnvironment(const Info &info);

	const PriorityJobConfig& getConfig();
	SQLVariableSizeGlobalAllocator* getGlobalVarAllocator();
	util::AllocatorLimitter* getTotalAllocatorLimitter();

	LocalTempStore* getStore();
	LocalTempStore::Group& getDefaultStoreGroup();
	const SQLProcessorConfig* getProcessorConfig();

	ClusterService* getClusterService();
	PartitionTable* getPartitionTable();
	TransactionService* getTransactionService();
	TransactionManager* getTransactionManager();
	DataStoreConfig* getDataStoreConfig();
	PartitionList* getPartitionList();
	SQLExecutionManager* getExecutionManager();
	JobManager* getJobManager();

	JobTransporter::Engine& getTransporterEngine();
	PriorityJobWorker::Engine& getWorkerEngine(bool forSql, bool forBackend);

	EventEngine& getBackendEngineBase();

	bool isSQLServiceProfiling();
	bool isDataScanProfiling();

	int64_t getMonotonicTime();
	void handleLegacyJobContol(EventContext &ec, Event &ev);

	bool checkEventContinuable(EventContext *ec, Event *ev);

	PriorityJobTracer& prepareTracer(util::LocalUniquePtr<PriorityJobTracer> &base);

private:
	Info info_;
	LocalTempStore::Group defaultStoreGroup_;

	util::LocalUniquePtr<EventEngine> sqlBackEngineBase_;

	util::LocalUniquePtr<JobTransporter::DefaultEngine> transporterEngine_;

	util::LocalUniquePtr<
			PriorityJobWorker::DefaultEngine> workerSqlFrontEngine_;
	util::LocalUniquePtr<
			PriorityJobWorker::DefaultEngine> workerSqlBackEngine_;
	util::LocalUniquePtr<
			PriorityJobWorker::DefaultEngine> workerTransactionEngine_;
};

/**
 * ジョブ制御機構の個別処理に関する実行コンテキスト
 */
class PriorityJobContext {
public:
	typedef PriorityJobContextSource Source;

	explicit PriorityJobContext(const Source &source);

	util::StackAllocator& getAllocator();
	PriorityJobAllocator& getVarAllocator();

	JobBufferedBlockPool& getBlockPool();
	const JobNode& getSenderNode();

	int64_t resolveEventMonotonicTime();

	EventContext& resolveEventContext();
	EventContext* getEventContext();

	Event* getSourceEvent();

private:
	static JobBufferedBlockPool& errorBlockPool();
	static EventContext& errorEventContext();

	PriorityJobContext(const PriorityJobContext&);
	PriorityJobContext& operator=(const PriorityJobContext&);

	util::StackAllocator &alloc_;
	PriorityJobAllocator &varAlloc_;
	JobBufferedBlockPool *blockPool_;
	JobNode senderNode_;
	EventContext *eventContext_;
	Event *sourceEvent_;
	PriorityJobEnvironment *env_;

	int64_t monotonicTime_;
	bool monotonicTimeResolved_;
};

struct PriorityJobContextSource {
	PriorityJobContextSource(
			util::StackAllocator &alloc, PriorityJobAllocator &varAlloc);

	static PriorityJobContextSource ofEventContext(EventContext &ec);
	static PriorityJobContextSource ofEvent(
			EventContext &ec, Event &sourceEvent);
	static PriorityJobContextSource ofEnvironment(
			util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
			PriorityJobEnvironment &env);

	util::StackAllocator &alloc_;
	PriorityJobAllocator &varAlloc_;
	JobBufferedBlockPool *blockPool_;
	JobNode senderNode_;
	EventContext *eventContext_;
	Event *sourceEvent_;
	PriorityJobEnvironment *env_;
};

/**
 * ジョブ制御機構のメイン
 */
class PriorityJobController {
public:
	typedef JobManager::JobId JobId;

	struct Source;

	explicit PriorityJobController(const Source &source);

	void deploy(
			PriorityJobContext &cxt, const JobMessages::Deploy &msg);
	void readyOutput(
			PriorityJobContext &cxt, const JobMessages::ReadyOutput &msg);
	void pipe(
			PriorityJobContext &cxt, const JobMessages::Pipe &msg);
	void finish(
			PriorityJobContext &cxt, const JobMessages::Finish &msg);
	void cancel(
			PriorityJobContext &cxt, const JobMessages::Cancel &msg);
	void complete(
			PriorityJobContext &cxt, const JobMessages::Complete &msg);
	void changeTaskPriority(
			PriorityJobContext &cxt,
			const JobMessages::ChangeTaskPriority &msg);
	void applyTaskPathPriority(
			PriorityJobContext &cxt,
			const JobMessages::ApplyTaskPathPriority &msg);
	void growTaskOutput(
			PriorityJobContext &cxt, const JobMessages::GrowTaskOutput &msg);
	void growNodeOutput(
			PriorityJobContext &cxt, const JobMessages::GrowNodeOutput &msg);

	void reportSent(
			PriorityJobContext &cxt, const JobMessages::ReportSent &msg);
	void validateNode(
			PriorityJobContext &cxt, const JobMessages::ValidateNode &msg);

	void invalidateNode(
			PriorityJobContext &cxt, const JobNode &node,
			std::exception &exception);

	void getStatProfile(
			util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
			const JobId *targetJobId, StatJobProfiler &profiler);
	void getResourceProfile(
			util::StackAllocator &alloc,
			util::Vector<JobResourceInfo> &infoList, bool byTask, bool limited);

	JobMessages::JobActivityList* checkJobActivities(
			PriorityJobContext &cxt, int64_t prevCheckTime);

	void getJobInfoList(util::StackAllocator& alloc, StatJobIdList &infoList);
	void getJobIdList(util::Vector<JobId> &idList);
	size_t getJobCount();

	void activateControl(bool activated);
	void resetCache();

	const PriorityJobConfig& getConfig();
	PriorityJobEnvironment& getEnvironment();

	PriorityJobHolders::Pool& getHoldersPool();
	JobBufferedBlockPool& getBlockPool();

	PriorityJobTracer& getTracer();
	JobTransporter& getTransporter();
	PriorityJobTable& getJobTable();
	PriorityJobWorkerTable& getWorkerTable();
	PriorityJobMonitor& getMonitor();

private:
	PriorityJob prepareJob(const JobId &jobId);
	bool prepareTask(
			PriorityJobContext &cxt, const JobId &jobId, TaskId taskId,
			const JobMessages::TaskIOInfo &ioInfo, PriorityTask &task);

	bool findTask(const JobId &jobId, TaskId taskId, PriorityTask &task);
	bool findTaskOptional(
			const JobId &jobId, TaskId taskId, bool optional,
			PriorityTask &task);

	void acceptNodeInput(PriorityJobContext &cxt, JobNode &node);

	void acceptJobActivities(
			PriorityJobContext &cxt, const JobNode &node,
			const JobMessages::JobActivityList *activityList);

	static PriorityJobWorkerTable::Source makeWorkerTableSource(
			const PriorityJobConfig &config, PriorityJobEnvironment &env,
			JobBufferedBlockPool &blockPool, JobTransporter &transporter,
			PriorityJobTracer &tracer);

	PriorityJobEnvironment &env_;

	PriorityJobHolders::Pool holdersPool_;
	JobBufferedBlockPool blockPool_;

	util::LocalUniquePtr<PriorityJobTracer> baseTracer_;
	PriorityJobTracer &tracer_;

	JobTransporter transporter_;
	PriorityJobTable jobTable_;
	PriorityJobWorkerTable workerTable_;
	PriorityJobMonitor monitor_;
};

struct PriorityJobController::Source {
	explicit Source(PriorityJobEnvironment &env);

	PriorityJobEnvironment &env_;
};

#endif
