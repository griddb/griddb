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

#include "resource_set.h"
#include "sql_job_manager.h"
#include "sql_task_context.h"
#include "sql_task_processor.h"

#include "sql_processor_config.h"

#define SQL_PROCESSOR_CONFIG_MANAGER_ENABLED 0

#define SQL_PROCESSOR_PROFILER_MANAGER_ENABLED 0

class DataStore;
class ClusterService;
class TransactionManager;
class TransactionService;
class PartitionTable;
class SQLExecution;
class SQLExecutionManager;
class SQLTableInfoList;
class TransactionContext;


struct TableSchemaInfo;
class ResourceSet;

class SQLContext : public TaskContext {

public:
	typedef StatementId ExecId;

	SQLContext(
			const ResourceSet *resourceSet,
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

	DataStore* getDataStore() const;

	ClusterService* getClusterService() const;

	TransactionManager* getTransactionManager() const;

	TransactionService* getTransactionService() const;

	PartitionTable* getPartitionTable() const;

	SQLExecution* getExecution() const;
	void setExecution(SQLExecution *execution);

	SQLExecutionManager* getExecutionManager() const;

	ClientId* getClientId() const;

	void setClientId(ClientId *clientId);

	void setExecId(JobExecutionId execId) {
			execId_ = execId;
	}

	JobExecutionId getExecId() {
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

	void setTableSchema(TableSchemaInfo *schema) {
		tableSchema_ = schema;
	}

	TableSchemaInfo *getTableSchema() {
		return tableSchema_;
	}

	bool isPartialMonitorRestricted() const;
	void setPartialMonitorRestricted(bool restricted);

	double getStoreMemoryAgingSwapRate() const;
	void setStoreMemoryAgingSwapRate(double storeMemoryAgingSwapRate);

	util::TimeZone getTimeZone() const;
	void setTimeZone(const util::TimeZone &zone);

private:

	SQLContext(const SQLContext&);
	SQLContext& operator=(const SQLContext&);

	SQLVarSizeAllocator *varAlloc_;
	LocalTempStore &store_;

	TupleList *localDest_;
	InterruptionChecker::CheckHandler *interruptionHandler_;

	const Event *event_;

	SQLExecution *execution_;
	ClientId *clientId_;
	StatementId stmtId_;
	Timestamp jobStartTime_;
	TableSchemaInfo *tableSchema_;

	bool finished_;
	bool fetchComplete_;
	JobExecutionId execId_;
	uint8_t versionId_;
	const SQLProcessorConfig *config_;
	bool isDmlTimeSeries_;
	bool jobCheckComplete_;

	bool partialMonitorRestricted_;
	double storeMemoryAgingSwapRate_;
	util::TimeZone timeZone_;
};

class SQLProcessor : public TaskProcessor {

public:
	
	typedef util::Vector<TupleList::TupleColumnType> TupleInfo;
	typedef util::Vector<TupleInfo> TupleInfoList;
	typedef LocalTempStore::Block Block;
	typedef TaskOption Option;
	typedef SQLContext Context;
	typedef SQLPreparedPlan Plan;

	class Factory;
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
	typedef TupleList::TupleColumnType TupleColumnType;
	typedef TupleColumnTypeUtils ColumnTypeUtils;

	const TypeInfo typeInfo_;
	SQLVarSizeAllocator &varAlloc_;
};

class SQLProcessor::Factory {
public:
	template<typename T> class Registrar;

	Factory();

	SQLProcessor* create(
			Context &cxt, SQLType::Id type, const Option &option,
			const TupleInfoList &inputInfo, TupleInfo *outputInfo = NULL);

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

	static bool isDQL(SQLType::Id type);

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
