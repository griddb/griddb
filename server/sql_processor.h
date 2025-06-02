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
	@brief Definition of SQL processor
*/

#ifndef SQL_PROCESSOR_H_
#define SQL_PROCESSOR_H_

#include "sql_tuple.h"
#include "sql_type.h"
#include "event_engine.h"

#include "sql_job_manager.h"

#define SQL_PROCESSOR_CONFIG_MANAGER_ENABLED 1

#define SQL_PROCESSOR_PROFILER_MANAGER_ENABLED 1

struct DataStoreConfig;
class DataStoreV4;
class ClusterService;
class TransactionManager;
class TransactionService;
class PartitionTable;
class SQLExecution;
class SQLExecutionManager;
class SQLTableInfoList;
class TransactionContext;
class TableLatch;
class PartitionList;

struct SQLProcessorConfig {
public:
	class Manager;
	struct PartialOption;
	struct PartialStatus;

	static const int32_t DEFAULT_WORK_MEMORY_MB;

	SQLProcessorConfig();

	const SQLProcessorConfig& getDefault();

	bool merge(const SQLProcessorConfig *base, bool withDefaults);

	int64_t workMemoryLimitBytes_;
	int64_t workMemoryCacheBytes_;
	int64_t hashBucketSmallSize_;
	int64_t hashBucketLargeSize_;
	int64_t hashSeed_;
	int64_t hashMapLimitRate_;
	int64_t hashMapLimit_;
	int64_t hashJoinSingleLimitBytes_;
	int64_t hashJoinBucketLimitBytes_;
	int64_t hashJoinBucketMaxCount_;
	int64_t hashJoinBucketMinCount_;
	int64_t aggregationListLimit_;
	int64_t aggregationSetLimit_;
	int64_t scanBatchThreshold_;
	int64_t scanBatchSetSizeThreshold_;
	int64_t scanBatchUnit_;
	int64_t scanCheckerLimitBytes_;
	int64_t scanUpdateListLimitBytes_;
	int64_t scanUpdateListThreshold_;
	int64_t sortPartialUnitMaxCount_;
	int64_t sortMergeUnitMaxCount_;
	int64_t sortTopNThreshold_;
	int64_t sortTopNBatchThreshold_;
	int64_t sortTopNBatchSize_;
	int64_t interruptionProjectionCount_;
	int64_t interruptionScanCount_;
	int64_t scanCountBased_;

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(workMemoryLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(workMemoryCacheBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashBucketSmallSize_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashBucketLargeSize_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashSeed_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashMapLimitRate_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashMapLimit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinSingleLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinBucketLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinBucketMaxCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinBucketMinCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(aggregationListLimit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(aggregationSetLimit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanBatchThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanBatchSetSizeThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanBatchUnit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanCheckerLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanUpdateListLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanUpdateListThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortPartialUnitMaxCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortMergeUnitMaxCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortTopNThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortTopNBatchThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortTopNBatchSize_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(interruptionProjectionCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(interruptionScanCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanCountBased_, -1));

private:
	static const SQLProcessorConfig DEFAULT_CONFIG;

	struct DefaultTag {};
	SQLProcessorConfig(const DefaultTag&);

	bool merge(const SQLProcessorConfig &base);
	bool mergeValue(const SQLProcessorConfig &base, const int64_t &baseValue);
};

struct SQLProcessorConfig::PartialOption {
	PartialOption();

	int32_t followingCount_;
	int64_t submissionCode_;
	bool activated_;

	int32_t trialCount_;
};

struct SQLProcessorConfig::PartialStatus {
	PartialStatus();

	int32_t followingCount_;
	int64_t submissionCode_;
	int64_t execCount_;
	int32_t followingProgress_;
	bool activated_;

	int32_t trialCount_;
	int32_t trialProgress_;
	int32_t opPhase_;

	UTIL_OBJECT_CODER_MEMBERS(
			followingCount_,
			submissionCode_,
			execCount_,
			followingProgress_,
			activated_,
			trialCount_,
			trialProgress_,
			opPhase_);
};

class SQLProcessorConfig::Manager {
public:
	struct SimulationEntry;
	struct PartialId;

	Manager();

	static bool isEnabled();
	static Manager& getInstance();

	void apply(const SQLProcessorConfig &config, bool overrideDefaults);
	void mergeTo(SQLProcessorConfig &config);

	bool isSimulating();
	void setSimulation(
			const util::Vector<SimulationEntry> &simulationList,
			SQLType::Id type, int64_t inputCount);
	bool getSimulation(
			util::Vector<SimulationEntry> &simulationList,
			SQLType::Id &type, int64_t &inputCount);
	void clearSimulation();

	bool isPartialMonitoring();
	bool getPartialStatus(PartialStatus &status);
	bool monitorPartial(const PartialOption &option);

	bool initPartial(const PartialId &id, int64_t submissionCode);
	bool checkPartial(const PartialId &id, PartialStatus &status);
	void setPartialOpPhase(const PartialId &id, int32_t opPhase);
	void closePartial(const PartialId &id);

private:
	struct PartialEntry;

	typedef std::vector<SimulationEntry> SimulationList;
	typedef std::vector<PartialEntry> PartialList;

	static Manager instance_;

	util::Atomic<bool> activated_;
	util::Atomic<bool> simulating_;
	util::Atomic<bool> partialMonitoring_;
	util::Mutex mutex_;

	SQLProcessorConfig config_;

	SimulationList simulationList_;
	SQLType::Id simulatingType_;
	int64_t simulatingInputCount_;

	PartialStatus partialStatus_;
	PartialList partialList_;
};

struct SQLProcessorConfig::Manager::SimulationEntry {
	SimulationEntry();

	void set(const picojson::value &value);
	void get(picojson::value &value) const;

	int32_t point_;
	int32_t action_;
	int64_t param_;
};

struct SQLProcessorConfig::Manager::PartialId {
	PartialId();
	PartialId(TaskContext &cxt, int32_t index);

	bool isEmpty() const;
	bool matchesId(const PartialId &id) const;

	JobId jobId_;
	TaskId taskId_;
	int32_t index_;
};

struct SQLProcessorConfig::Manager::PartialEntry {
	PartialEntry();

	PartialId id_;
	int64_t execCount_;
	bool execStarted_;
};

struct TableSchemaInfo;
class SQLContext : public TaskContext {
public:
	typedef StatementId ExecId;

	SQLContext(
			SQLVariableSizeGlobalAllocator *valloc,
			Task *task,
			SQLAllocator *alloc,
			SQLVarSizeAllocator *varAlloc,
			LocalTempStore &store,
			const SQLProcessorConfig *config);

	SQLContext(
			SQLAllocator *alloc,
			SQLVarSizeAllocator *varAlloc,
			LocalTempStore &store,
			const SQLProcessorConfig *config);

	bool isFinished() const;

	SQLAllocator& getAllocator() { return TaskContext::getAllocator(); }

	SQLAllocator& getEventAllocator() const;

	SQLVarSizeAllocator& getVarAllocator();

	LocalTempStore& getStore();

	void setLocalDestination(TupleList &localDest);

	InterruptionChecker::CheckHandler* getInterruptionHandler() const;
	void setInterruptionHandler(InterruptionChecker::CheckHandler *handler);

	const Event* getEvent() const;
	void setEvent(const Event *ev);

	void setDataStoreConfig(const DataStoreConfig *dsConfig);
	const DataStoreConfig* getDataStoreConfig() const;

	void setPartitionList(PartitionList *partitionList);
	PartitionList* getPartitionList() const;

	ClusterService* getClusterService() const;
	void setClusterService(ClusterService *clusterService);

	TransactionManager* getTransactionManager() const;
	void setTransactionManager(TransactionManager *transactionManager);

	TransactionService* getTransactionService() const;
	void setTransactionService(TransactionService *transactionService);

	PartitionTable* getPartitionTable() const;
	void setPartitionTable(PartitionTable *partitionTable);

	SQLExecution* getExecution() const;
	void setExecution(SQLExecution *execution);

	SQLExecutionManager* getExecutionManager() const;
	void setExecutionManager(SQLExecutionManager *executionManager);

	ClientId* getClientId() const;
	void setClientId(ClientId *clientId);

	void setExecId(ExecId execId) {
		execId_ = execId;
	}
	ExecId getExecId() {
		return execId_;
	}

	void setVersionId(uint8_t versionId) {
		versionId_ = versionId;
	}
	uint8_t getVersionId() {
		return versionId_;
	}

	StatementId getStatementId() const;
	void setStatementId(StatementId stmtId);

	Timestamp getJobStartTime() const;
	void setJobStartTime(Timestamp jobStartTime);

	void setAdministrator(bool isAdministrator) {
		isAdministrator_ = isAdministrator;
	}
	bool isAdministrator() const {
		return isAdministrator_;
	}

	void setProcessorGroup(TupleList::Group *group) {
		setGroup_ = true;
		groupId_ = group->getId();
	}

	LocalTempStore::GroupId getProcessorGroupId() {
		return groupId_;
	}

	bool isSetGroup() {
		return setGroup_;
	}


	void finish();

	bool isFetchComplete() {
		return fetchComplete_;
	}

	void setFetchComplete() {
		fetchComplete_ = true;
	}

	void setDMLTimeSeries() {
		isDmlTimeSeries_ = true;
	}
	bool isDMLTimeSeries() {
		return isDmlTimeSeries_;
	}

	void setJobCheckComplete(uint8_t status) {
		if (status ==  1) {
			jobCheckComplete_ = true;
		}
	}

	bool getJobCheckComplete() {
		return jobCheckComplete_;
	}
	const SQLProcessorConfig* getConfig() const;

	void setTableSchema(TableSchemaInfo *schema, TableLatch *latch) {
		tableSchema_ = schema;
		tableLatch_ = latch;
	}

	TableSchemaInfo *getTableSchema() {
		return tableSchema_;
	}

	TableLatch *getTableLatch() {
		return tableLatch_;
	}

	bool isPartialMonitorRestricted() const;
	void setPartialMonitorRestricted(bool restricted);

	double getStoreMemoryAgingSwapRate() const;
	void setStoreMemoryAgingSwapRate(double storeMemoryAgingSwapRate);

	util::TimeZone getTimeZone() const;
	void setTimeZone(const util::TimeZone &zone);

	util::AllocatorLimitter* getAllocatorLimitter();
	void setAllocatorLimitter(util::AllocatorLimitter *allocLimitter);

private:

	SQLContext(const SQLContext&);
	SQLContext& operator=(const SQLContext&);

	SQLVarSizeAllocator *varAlloc_;
	LocalTempStore &store_;

	TupleList *localDest_;
	InterruptionChecker::CheckHandler *interruptionHandler_;

	const Event *event_;
	const DataStoreConfig *dsConfig_;
	PartitionList *partitionList_;
	ClusterService *clusterService_;
	TransactionManager *transactionManager_;
	TransactionService *transactionService_;
	PartitionTable *partitionTable_;

	SQLExecution *execution_;
	SQLExecutionManager *executionManager_;
	ClientId *clientId_;
	StatementId stmtId_;
	Timestamp jobStartTime_;
	TableSchemaInfo *tableSchema_;
	TableLatch *tableLatch_;

	bool finished_;
	bool fetchComplete_;
	ExecId execId_;
	uint8_t versionId_;
	const SQLProcessorConfig *config_;
	bool isDmlTimeSeries_;
	bool jobCheckComplete_;

	int32_t queryTimeout_;
	bool partialMonitorRestricted_;
	double storeMemoryAgingSwapRate_;
	util::TimeZone timeZone_;
	bool setGroup_;
	LocalTempStore::GroupId groupId_;
	bool isAdministrator_;
	util::AllocatorLimitter *allocLimitter_;
};

/**
	@brief SQLプロセッサタスクの実行管理
	@note
		原則、以下の処理はSQLプロセッサを通じて透過的に行われるため、このクラスでは処理しない
			- 通信・デコード処理
			- 入力の依存関係の管理
			- TupleListリソースの生存期間管理
			- パイプライン制御
			- エラー処理(クライアントへの返却など特殊なカスタマイズが必要な場合のみ)
			- キャンセル処理
			- 再実行・回復処理
		同一インスタンスに対する呼び出しは同時に複数スレッドから呼び出されることはない
 */
class SQLProcessor : public TaskProcessor {
public:
	typedef util::Vector<TupleList::TupleColumnType> TupleInfo;
	typedef util::Vector<TupleInfo> TupleInfoList;
	typedef LocalTempStore::Block Block;
	typedef TaskOption Option;
	typedef SQLContext Context;
	typedef SQLPreparedPlan Plan;

	class Factory;
	class Holder;
	class Profiler;
	class ProfilerManager;
	struct ValueUtils;
	struct OutOption;
	template<typename Base> class Coder;
	struct TypeInfo;
	struct DQLTool;

	SQLProcessor(Context &cxt, const TypeInfo &typeInfo);
	virtual ~SQLProcessor();

	virtual void cleanUp(Context &cxt);

	SQLType::Id getType() const;

	virtual bool pipe(Context &cxt, InputId inputId, const Block &block) = 0;

	virtual bool finish(Context &cxt, InputId inputId) = 0;

	virtual bool next(Context &cxt);

	virtual bool applyInfo(
			Context &cxt, const Option &option,
			const TupleInfoList &inputInfo, TupleInfo &outputInfo) = 0;

	virtual const Profiler& getProfiler();

	virtual void setProfiler(const Profiler &profiler);

	virtual void exportTo(Context &cxt, const OutOption &option) const;

	static void planToInputInfo(
			const Plan &plan, uint32_t planNodeId,
			const SQLTableInfoList &tableInfoList, TupleInfoList &inputInfo);

	static void planToInputInfoForDml(
			const Plan &plan, uint32_t planNodeId,
			const SQLTableInfoList &tableInfoList, TupleInfoList &inputInfo);

	static void planToOutputInfo(
			const Plan &plan, uint32_t planNodeId, TupleInfoList &outputInfo);

	template<typename Base>
	static Coder<Base> makeCoder(
			const Base &base, Context &cxt, Factory *factory,
			const TupleInfoList *inputInfo);

	struct TypeInfo {
		TypeInfo();

		UTIL_OBJECT_CODER_PARTIAL_OBJECT;
		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_ENUM(type_, SQLType::Coder()));

		SQLType::Id type_;
	};

protected:
	static void getResultCountBlock(
			Context &cxt, int64_t resultNum, TupleList::Block &block);

	static void transcodeProfilerSpecific(
			util::StackAllocator &alloc, const TypeInfo &typeInfo,
			util::AbstractObjectInStream &in,
			util::AbstractObjectOutStream &out);

private:
	const TypeInfo typeInfo_;
	SQLVarSizeAllocator &varAlloc_;
};

/**
	@brief SQLProcessorの構築・破棄を取り扱うファクトリ
	@note
		構築時にcxtを通じて提供する構築時の可変サイズアロケータ(SQLVarSizeAllocator)は、
		pipe、finish時の可変サイズアロケータとは別インスタンスでもよい
 */
class SQLProcessor::Factory {
public:
	template<typename T> class Registrar;

	Factory();

	/**
		@brief SQLProcessorを構築する(種別指定あり)
		@param cxt 構築時に用いるコンテキスト情報
		@param type SQLProcessorの種別
			- SQLProcessorの派生クラスの定義に基づく
		@param option SQLProcessorの派生クラスの定義に基づくオプション情報
			- 形式: { (SQLProcessorオプション), ... }
		@param inputInfo 各入力タプルリストのカラム情報
		@return 構築されたSQLProcessor
			- 該当する名前のSQLProcessorが存在しない場合はNULL
	 */
	SQLProcessor* create(
			Context &cxt, SQLType::Id type, const Option &option,
			const TupleInfoList &inputInfo, TupleInfo *outputInfo = NULL);

	/**
		@brief SQLProcessorを構築する(種別指定なし)
	 */
	SQLProcessor* create(
			Context &cxt, const Option &option,
			const TupleInfoList &inputInfo, TupleInfo *outputInfo = NULL);

	static void destroy(SQLProcessor *processor);

	static void transcodeProfiler(
			util::StackAllocator &alloc, const TypeInfo &typeInfo,
			util::AbstractObjectInStream &in,
			util::AbstractObjectOutStream &out);

private:
	typedef SQLProcessor* (*FactoryFunc)(Context&, const TypeInfo&);
	typedef void (*ProfilerTransFunc)(
			util::StackAllocator&, const TypeInfo&,
			util::AbstractObjectInStream&, util::AbstractObjectOutStream&);

	static FactoryFunc& getFactoryFuncRef(const TypeInfo &typeInfo);
	static ProfilerTransFunc& getProfilerTransFuncRef(
			const TypeInfo &typeInfo);

	void check(
			SQLProcessor *&orgProcessor, FactoryFunc factoryFunc,
			Context &cxt, SQLType::Id type, const TupleInfoList &inputInfo);
};

template<typename T>
class SQLProcessor::Factory::Registrar {
public:
	explicit Registrar(SQLType::Id type);

private:
	static SQLProcessor* create(Context &cxt, const TypeInfo &typeInfo);
};

class SQLProcessor::Profiler {
public:
	class CoderBase;

	typedef util::XArray<
			uint8_t, util::StdAllocator<uint8_t, void> > StreamData;

	explicit Profiler(const util::StdAllocator<void, void> &alloc);
	~Profiler();

	picojson::value& getOption();
	const picojson::value& getOption() const;

	picojson::value& getResult();
	const picojson::value& getResult() const;

	StreamData& prepareStreamData();
	const StreamData* getStreamData() const;

	void setForAnalysis(bool forAnalysis);
	bool isForAnalysis() const;

	static const Profiler& getEmptyProfiler();

	static void mergeResultToPlan(
			util::StackAllocator &alloc, TaskProfiler &profiler,
			picojson::value &plan);

	template<typename Base>
	static typename util::CustomObjectCoder<
			Base, TaskProfiler, CoderBase> makeProfilerCoder(
			util::StackAllocator &alloc, const Base &base);

	template<typename C, typename T>
	static void makeStreamData(
			const C &coder, const TypeInfo &typeInfo, const T &value,
			StreamData &streamData);

private:
	Profiler(const Profiler&);
	Profiler& operator=(const Profiler&);

	Profiler(
			const util::StdAllocator<void, void> &alloc,
			const util::FalseType&);

	util::StdAllocator<void, void> alloc_;
	util::AllocUniquePtr<picojson::value> option_;
	util::AllocUniquePtr<picojson::value> result_;
	util::AllocUniquePtr<StreamData> streamData_;
	bool forAnalysis_;
};

class SQLProcessor::Holder {
public:
	explicit Holder(SQLProcessor &processor);
	~Holder();

	SQLProcessor& get();

private:
	Holder(const Holder&);
	Holder& operator=(const Holder&);

	SQLProcessor &processor_;
};

class SQLProcessor::Profiler::CoderBase {
public:
	typedef util::ObjectCoder::Attribute Attribute;

	explicit CoderBase(util::StackAllocator &alloc);

	template<typename C, typename Org, typename S, typename Traits>
	void encodeBy(
			const C&, Org &orgCoder, S &stream, const TaskProfiler &value,
			const Attribute &attr, const Traits&) const;

	template<typename C, typename Org, typename S, typename Traits>
	void decodeBy(
			const C&, Org &orgCoder, S &stream, TaskProfiler &value,
			const Attribute &attr, const Traits&) const;

private:
	void encodeCustom(
			const TaskProfiler &profiler,
			util::AbstractObjectOutStream &out) const;

	util::StackAllocator &alloc_;
};

class SQLProcessor::ProfilerManager {
public:
	typedef int64_t ProfilerId;

	enum ProfilerType {
		PROFILE_CORE,
		PROFILE_MEMORY,
		PROFILE_INDEX,
		PROFILE_INTERRUPTION,
		PROFILE_ERROR,
		PROFILE_PARTIAL
	};

	ProfilerManager();

	static bool isEnabled();
	static ProfilerManager& getInstance();

	int32_t getActivatedFlags();

	bool isActivated(ProfilerType type);
	void setActivated(ProfilerType type, bool activated);

	void listIds(util::Vector<ProfilerId> &idList);

	bool getProfiler(ProfilerId id, Profiler &profiler);
	void addProfiler(const Profiler &profiler);

	void incrementErrorCount(int32_t code);

	void getGlobalProfile(picojson::value &value);

private:
	typedef std::map<std::string, ProfilerId> IdMap;
	typedef std::deque<picojson::value> EntryList;

	typedef std::map<int32_t, int64_t> ErrorCountMap;

	static ProfilerManager instance_;

	ProfilerManager(const ProfilerManager&);
	ProfilerManager& operator=(const ProfilerManager&);

	util::Atomic<int32_t> flags_;
	util::Mutex mutex_;

	size_t maxListSize_;
	ProfilerId startId_;
	IdMap idMap_;
	UTIL_UNIQUE_PTR<EntryList> entryList_;
	UTIL_UNIQUE_PTR<ErrorCountMap> errorCountMap_;
};

struct SQLProcessor::ValueUtils {
public:
	static size_t toString(int64_t value, char8_t *buf, size_t size);
	static size_t toString(double value, char8_t *buf, size_t size);

	static util::String toString(util::StackAllocator &alloc, int64_t value);
	static util::String toString(util::StackAllocator &alloc, double value);
	static util::String toString(util::StackAllocator &alloc, const TupleValue &value);

	static bool toLong(const char8_t *buf, size_t size, int64_t &result);
	static bool toDouble(const char8_t *buf, size_t size, double &result);

	static int32_t strICmp(const char8_t *str1, const char8_t *str2);

	static void toLower(char8_t *buf, size_t size);
	static void toUpper(char8_t *buf, size_t size);

	static uint32_t hashValue(
			const TupleValue &value, const uint32_t *base = NULL);
	static uint32_t hashString(
			const char8_t *value, const uint32_t *base = NULL);

	static int32_t orderValue(
			const TupleValue &value1, const TupleValue &value2,
			bool strict = false);
	static int32_t orderString(
			const char8_t *value1, const char8_t *value2);

	static bool isTrueValue(const TupleValue &value);

	static int64_t intervalToRaw(int64_t interval);
	static int64_t intervalFromRaw(int64_t rawInterval);

	static int64_t rawIntervalFromValue(const TupleValue &value, int64_t unit);
	static int64_t rawIntervalFromValue(int64_t longValue, int64_t unit);
	static int64_t intervalValueToLong(const TupleValue &value);

	static int64_t intervalToAffinity(
			uint8_t partitioningType,
			uint32_t partitioningCount, uint32_t clusterPartitionCount,
			const util::Vector<int64_t> &affinityList,
			int64_t interval, uint32_t hash);

	static TupleValue duplicateValue(
			const util::StdAllocator<void, void> &alloc, const TupleValue &src);
	static void destroyValue(
			const util::StdAllocator<void, void> &alloc, TupleValue &value);

	static const void* getValueBody(const TupleValue &value, size_t &size);
};

struct SQLProcessor::OutOption {
public:
	template<typename S> struct Builder;

	OutOption();

	TupleValue::VarContext *varCxt_;
	SQLPreparedPlan *plan_;
	uint32_t planNodeId_;

	util::ObjectOutStream<ByteOutStream> *byteOutStream_;
	util::AbstractObjectOutStream *outStream_;
};

template<typename Base>
class SQLProcessor::Coder : public util::ObjectCoderBase<
		SQLProcessor::Coder<Base>, typename Base::Allocator, Base> {
public:
	typedef Base BaseCoder;
	typedef typename Base::Allocator Allocator;
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoderBase<
			SQLProcessor::Coder<Base>, typename Base::Allocator, Base> BaseType;

	Coder(
			const Base &base, Context &cxt, Factory *factory,
			const TupleInfoList *inputInfo);

	template<typename Org, typename S, typename T, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::encodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename T, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::decodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename Traits>
	void encodeBy(
			Org&, S &stream, const SQLProcessor &value,
			const Attribute&, const Traits&) const {
		typedef util::AbstractObjectCoder::Out::Selector<
				util::ObjectOutStream<ByteOutStream> > Selector;

		this->encode(stream, value.typeInfo_);
		value.exportTo(cxt_, makeOption(Selector()(stream)()));
	}

	template<typename Org, typename S, typename Traits>
	void decodeBy(
			Org&, S &stream, SQLProcessor *&value,
			const Attribute &attr, const Traits&) const {
		typedef util::AbstractObjectCoder::In::Selector<
				util::ObjectOutStream<ByteInStream> > Selector;

		decodeValue(value, Selector()(stream)(), attr);
	}

private:
	template<typename S>
	void decodeValue(
			SQLProcessor *&value, S &stream, const Attribute &attr) const;

	Option makeOption(util::ObjectInStream<ByteInStream> &stream) const;
	Option makeOption(util::AbstractObjectInStream &stream) const;

	OutOption makeOption(util::ObjectOutStream<ByteOutStream> &stream) const;
	OutOption makeOption(util::AbstractObjectOutStream &stream) const;

	Context &cxt_;
	Factory *factory_;
	const TupleInfoList *inputInfo_;
};

struct SQLProcessor::DQLTool {
public:
	static bool isDQL(SQLType::Id type);

private:
	typedef SQLProcessorConfig::Manager::SimulationEntry SimulationEntry;

	friend struct SQLProcessorConfig;
	friend struct SQLProcessorConfig::Manager::SimulationEntry;
	friend class Factory;
	friend class ProfilerManager;

	static void decodeSimulationEntry(
			const picojson::value &value, SimulationEntry &entry);
	static void encodeSimulationEntry(
			const SimulationEntry &entry, picojson::value &value);

	static void getInterruptionProfile(picojson::value &value);

	static void customizeDefaultConfig(SQLProcessorConfig &config);
};

template<typename T>
SQLProcessor::Factory::Registrar<T>::Registrar(SQLType::Id type) {
	TypeInfo typeInfo;
	typeInfo.type_ = type;
	Factory::getFactoryFuncRef(typeInfo) = &create;
	Factory::getProfilerTransFuncRef(typeInfo) = &T::transcodeProfilerSpecific;
}

template<typename T>
SQLProcessor* SQLProcessor::Factory::Registrar<T>::create(
		Context &cxt, const TypeInfo &typeInfo) {
	return ALLOC_VAR_SIZE_NEW(cxt.getVarAllocator()) T(cxt, typeInfo);
}

template<typename Base>
typename util::CustomObjectCoder<
		Base, TaskProfiler, SQLProcessor::Profiler::CoderBase>
SQLProcessor::Profiler::makeProfilerCoder(
		util::StackAllocator &alloc, const Base &base) {
	return typename util::CustomObjectCoder<
			Base, TaskProfiler, CoderBase>(base, CoderBase(alloc));
}

template<typename C, typename T>
void SQLProcessor::Profiler::makeStreamData(
		const C &coder, const TypeInfo &typeInfo, const T &value,
		StreamData &streamData) {
	typedef StreamData::allocator_type Alloc;
	typedef util::XArrayOutStream<Alloc> ProfilerOutStream;
	typedef util::ByteStream<ProfilerOutStream> ProfilerByteOutStream;

	ProfilerByteOutStream out((ProfilerOutStream(streamData)));
	coder.encode(out, typeInfo);
	coder.encode(out, value);
}

template<typename C, typename Org, typename S, typename Traits>
void SQLProcessor::Profiler::CoderBase::encodeBy(
		const C&, Org &orgCoder, S &stream, const TaskProfiler &value,
		const Attribute &attr, const Traits&) const {
	typename S::ObjectScope scope(stream, attr);

	util::PartialCodableObject<TaskProfiler> withoutCustom(value);
	withoutCustom.customData_ = NULL;
	static_cast<const typename C::BaseType&>(orgCoder).encodeBy(
			orgCoder, scope.stream(), withoutCustom, attr, Traits());

	typedef util::AbstractObjectCoder::Out::Selector<> Selector;
	encodeCustom(value, Selector()(scope.stream())());
}

template<typename C, typename Org, typename S, typename Traits>
void SQLProcessor::Profiler::CoderBase::decodeBy(
		const C&, Org &orgCoder, S &stream, TaskProfiler &value,
		const Attribute &attr, const Traits&) const {
	static_cast<void>(orgCoder);
	static_cast<void>(stream);
	static_cast<void>(value);
	static_cast<void>(attr);
	UTIL_STATIC_ASSERT(sizeof(C) <= 0);
}

template<typename Base>
SQLProcessor::Coder<Base> SQLProcessor::makeCoder(
		const Base &base, Context &cxt, Factory *factory,
		const TupleInfoList *inputInfo) {
	return Coder<Base>(base, cxt, factory, inputInfo);
}

template<typename Base>
SQLProcessor::Coder<Base>::Coder(
		const Base &base, Context &cxt, Factory *factory,
		const TupleInfoList *inputInfo) :
		BaseType(base.getAllocator(), base),
		cxt_(cxt),
		factory_(factory),
		inputInfo_(inputInfo) {
}

template<typename Base>
template<typename S>
void SQLProcessor::Coder<Base>::decodeValue(
		SQLProcessor *&value, S &stream, const Attribute &attr) const {
	assert(factory_ != NULL);
	assert(inputInfo_ != NULL);

	factory_->destroy(value);
	value = NULL;

	util::ObjectCoder::Type valueType;
	typename S::ValueScope valueScope(stream, valueType, attr);

	if (valueType == util::ObjectCoder::TYPE_NULL) {
		value = NULL;
	}
	else {
		TypeInfo typeInfo;
		this->decode(stream, typeInfo);
		value = factory_->create(
				cxt_, typeInfo.type_, makeOption(stream), *inputInfo_, NULL);
	}
}

template<typename Base>
SQLProcessor::Option SQLProcessor::Coder<Base>::makeOption(
		util::ObjectInStream<ByteInStream> &stream) const {
	Option option(cxt_.getAllocator());
	option.byteInStream_ = &stream;
	return option;
}

template<typename Base>
SQLProcessor::Option SQLProcessor::Coder<Base>::makeOption(
		util::AbstractObjectInStream &stream) const {
	Option option(cxt_.getAllocator());
	option.inStream_ = &stream;
	return option;
}

template<typename Base>
SQLProcessor::OutOption SQLProcessor::Coder<Base>::makeOption(
		util::ObjectOutStream<ByteOutStream> &stream) const {
	OutOption option;
	option.byteOutStream_ = &stream;
	return option;
}

template<typename Base>
SQLProcessor::OutOption SQLProcessor::Coder<Base>::makeOption(
		util::AbstractObjectOutStream &stream) const {
	OutOption option;
	option.outStream_ = &stream;
	return option;
}

#endif
