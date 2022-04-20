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
	@brief Implementation of SQL processor
*/

#include "sql_processor.h"
#include "sql_compiler.h"

#include "json.h"
#include "picojson.h"
#include "partition.h"

const int32_t SQLProcessorConfig::DEFAULT_WORK_MEMORY_MB = 32;
const SQLProcessorConfig SQLProcessorConfig::DEFAULT_CONFIG((DefaultTag()));

SQLProcessorConfig::SQLProcessorConfig() :
		workMemoryLimitBytes_(-1),
		workMemoryCacheBytes_(-1),
		hashBucketSmallSize_(-1),
		hashBucketLargeSize_(-1),
		hashSeed_(-1),
		hashMapLimitRate_(-1),
		hashMapLimit_(-1),
		hashJoinSingleLimitBytes_(-1),
		hashJoinBucketLimitBytes_(-1),
		hashJoinBucketMaxCount_(-1),
		hashJoinBucketMinCount_(-1),
		aggregationListLimit_(-1),
		aggregationSetLimit_(-1),
		scanBatchThreshold_(-1),
		scanBatchSetSizeThreshold_(-1),
		scanBatchUnit_(-1),
		scanCheckerLimitBytes_(-1),
		scanUpdateListLimitBytes_(-1),
		scanUpdateListThreshold_(-1),
		sortPartialUnitMaxCount_(-1),
		sortMergeUnitMaxCount_(-1),
		sortTopNThreshold_(-1),
		sortTopNBatchThreshold_(-1),
		sortTopNBatchSize_(-1),
		interruptionProjectionCount_(-1),
		interruptionScanCount_(-1) {
}

const SQLProcessorConfig& SQLProcessorConfig::getDefault() {
	return DEFAULT_CONFIG;
}

bool SQLProcessorConfig::merge(
		const SQLProcessorConfig *base, bool withDefaults) {
	bool merged = false;


	if (base != NULL) {
		merged |= merge(*base);
	}

	if (withDefaults) {
		merged |= merge(DEFAULT_CONFIG);
	}

	return merged;
}

SQLProcessorConfig::SQLProcessorConfig(const DefaultTag&) :
		workMemoryLimitBytes_(ConfigTable::megaBytesToBytes(
				DEFAULT_WORK_MEMORY_MB)),
		workMemoryCacheBytes_(-1),
		hashBucketSmallSize_(102400U - 1),
		hashBucketLargeSize_(409600U - 1),
		hashSeed_(0),
		hashMapLimitRate_(-1),
		hashMapLimit_(-1),
		hashJoinSingleLimitBytes_(24 * 1024 * 1024),
		hashJoinBucketLimitBytes_(50 * 1024 * 1024),
		hashJoinBucketMaxCount_((1 << 10) - 1),
		hashJoinBucketMinCount_((1 << 1) - 1),
		aggregationListLimit_(-1),
		aggregationSetLimit_(-1),
		scanBatchThreshold_(1000),
		scanBatchSetSizeThreshold_(8192 - 1),
		scanBatchUnit_(10000),
		scanCheckerLimitBytes_(-1),
		scanUpdateListLimitBytes_(-1),
		scanUpdateListThreshold_(-1),
		sortPartialUnitMaxCount_(-1),
		sortMergeUnitMaxCount_(-1),
		sortTopNThreshold_(100 * 1024),
		sortTopNBatchThreshold_(6),
		sortTopNBatchSize_(-1),
		interruptionProjectionCount_(100000),
		interruptionScanCount_(10000) {
	SQLProcessor::DQLTool::customizeDefaultConfig(*this);
}

bool SQLProcessorConfig::merge(const SQLProcessorConfig &base) {
	bool merged = false;
	merged |= mergeValue(base, base.workMemoryLimitBytes_);
	merged |= mergeValue(base, base.workMemoryCacheBytes_);
	merged |= mergeValue(base, base.hashBucketSmallSize_);
	merged |= mergeValue(base, base.hashBucketLargeSize_);
	merged |= mergeValue(base, base.hashSeed_);
	merged |= mergeValue(base, base.hashMapLimitRate_);
	merged |= mergeValue(base, base.hashMapLimit_);
	merged |= mergeValue(base, base.hashJoinSingleLimitBytes_);
	merged |= mergeValue(base, base.hashJoinBucketLimitBytes_);
	merged |= mergeValue(base, base.hashJoinBucketMaxCount_);
	merged |= mergeValue(base, base.hashJoinBucketMinCount_);
	merged |= mergeValue(base, base.aggregationListLimit_);
	merged |= mergeValue(base, base.aggregationSetLimit_);
	merged |= mergeValue(base, base.scanBatchThreshold_);
	merged |= mergeValue(base, base.scanBatchSetSizeThreshold_);
	merged |= mergeValue(base, base.scanBatchUnit_);
	merged |= mergeValue(base, base.scanCheckerLimitBytes_);
	merged |= mergeValue(base, base.scanUpdateListThreshold_);
	merged |= mergeValue(base, base.scanUpdateListLimitBytes_);
	merged |= mergeValue(base, base.sortPartialUnitMaxCount_);
	merged |= mergeValue(base, base.sortMergeUnitMaxCount_);
	merged |= mergeValue(base, base.sortTopNThreshold_);
	merged |= mergeValue(base, base.sortTopNBatchThreshold_);
	merged |= mergeValue(base, base.sortTopNBatchSize_);
	merged |= mergeValue(base, base.interruptionProjectionCount_);
	merged |= mergeValue(base, base.interruptionScanCount_);
	return merged;
}

bool SQLProcessorConfig::mergeValue(
		const SQLProcessorConfig &base, const int64_t &baseValue) {
	if (baseValue < 0) {
		return false;
	}

	const void *baseAddr = &base;
	const void *baseValueAddr = &baseValue;

	void *thisAddr = this;
	void *thisValueAddr = static_cast<uint8_t*>(thisAddr) +
			(static_cast<const uint8_t*>(baseValueAddr) -
					static_cast<const uint8_t*>(baseAddr));

	int64_t &thisValue = *static_cast<int64_t*>(thisValueAddr);
	if (thisValue < 0) {
		thisValue = baseValue;
		return true;
	}

	return false;
}

SQLProcessorConfig::PartialOption::PartialOption() :
		followingCount_(0),
		submissionCode_(0),
		activated_(false),
		trialCount_(1) {
}

SQLProcessorConfig::PartialStatus::PartialStatus() :
		followingCount_(0),
		submissionCode_(0),
		execCount_(0),
		followingProgress_(0),
		activated_(false),
		trialCount_(1),
		trialProgress_(1),
		opPhase_(0) {
}

SQLProcessorConfig::Manager SQLProcessorConfig::Manager::instance_;

SQLProcessorConfig::Manager::Manager() :
		simulatingType_(SQLType::START_EXEC),
		simulatingInputCount_(-1) {
}

bool SQLProcessorConfig::Manager::isEnabled() {
	return SQL_PROCESSOR_CONFIG_MANAGER_ENABLED;
}

SQLProcessorConfig::Manager& SQLProcessorConfig::Manager::getInstance() {
	return instance_;
}

void SQLProcessorConfig::Manager::apply(
		const SQLProcessorConfig &config, bool overrideDefaults) {
	util::LockGuard<util::Mutex> guard(mutex_);
	activated_ = true;

	if (overrideDefaults) {
		config_ = config;
	}
	else {
		config_.merge(config);
	}
}

void SQLProcessorConfig::Manager::mergeTo(SQLProcessorConfig &config) {
	if (!activated_) {
		return;
	}

	util::LockGuard<util::Mutex> guard(mutex_);
	config.merge(config_);
}

bool SQLProcessorConfig::Manager::isSimulating() {
	return simulating_;
}

void SQLProcessorConfig::Manager::setSimulation(
		const util::Vector<SimulationEntry> &simulationList,
		SQLType::Id type, int64_t inputCount) {
	util::LockGuard<util::Mutex> guard(mutex_);

	simulationList_.reserve(simulationList.size());

	simulating_ = true;
	simulationList_.assign(simulationList.begin(), simulationList.end());
	simulatingType_ = type;
	simulatingInputCount_ = inputCount;
}

bool SQLProcessorConfig::Manager::getSimulation(
		util::Vector<SimulationEntry> &simulationList,
		SQLType::Id &type, int64_t &inputCount) {
	util::LockGuard<util::Mutex> guard(mutex_);

	simulationList.assign(simulationList_.begin(), simulationList_.end());
	type = simulatingType_;
	inputCount = simulatingInputCount_;

	return simulating_;
}

void SQLProcessorConfig::Manager::clearSimulation() {
	util::LockGuard<util::Mutex> guard(mutex_);

	simulating_ = false;
	simulationList_.clear();
	simulatingType_ = SQLType::START_EXEC;
	simulatingInputCount_ = -1;
}

bool SQLProcessorConfig::Manager::isPartialMonitoring() {
	return partialMonitoring_;
}

bool SQLProcessorConfig::Manager::getPartialStatus(PartialStatus &status) {
	status = PartialStatus();

	if (!partialMonitoring_) {
		return false;
	}

	util::LockGuard<util::Mutex> guard(mutex_);
	status = partialStatus_;

	if (partialList_.empty()) {
		status.followingProgress_ = -1;
		return true;
	}

	status.execCount_ = partialList_.front().execCount_;

	for (PartialList::iterator it = partialList_.begin();
			++it != partialList_.end();) {
		if (!it->id_.isEmpty() && it->execCount_ >= status.execCount_) {
			status.followingProgress_++;
		}
	}

	return true;
}

bool SQLProcessorConfig::Manager::monitorPartial(const PartialOption &option) {
	if (!isEnabled()) {
		return false;
	}

	util::LockGuard<util::Mutex> guard(mutex_);

	if (option.followingCount_ < 0 || option.activated_) {
		partialList_.clear();
		partialMonitoring_ = option.activated_;
		partialStatus_.activated_ = option.activated_;
		return true;
	}

	if (option.trialCount_ <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	const size_t count = static_cast<size_t>(option.followingCount_) + 1;
	partialList_.reserve(count);

	partialStatus_ = PartialStatus();
	partialStatus_.followingCount_ = static_cast<int32_t>(count) - 1;
	partialStatus_.submissionCode_ = option.submissionCode_;
	partialStatus_.activated_ = true;
	partialStatus_.trialCount_ = option.trialCount_;

	PartialEntry entry;
	entry.execCount_ = -1;
	partialList_.assign(count, entry);
	partialList_.front().execCount_ = 0;
	partialMonitoring_ = true;

	return true;
}

bool SQLProcessorConfig::Manager::initPartial(
		const PartialId &id, int64_t submissionCode) {
	assert(!id.isEmpty());
	util::LockGuard<util::Mutex> guard(mutex_);

	if (partialList_.empty() || (id.index_ > 0 &&
			submissionCode != partialStatus_.submissionCode_)) {
		return false;
	}

	if (id.index_ < 0 || static_cast<size_t>(id.index_) >= partialList_.size()) {
		return false;
	}

	PartialEntry &entry = partialList_[static_cast<size_t>(id.index_)];
	if (id.index_== 0 && !entry.id_.isEmpty()) {
		return false;
	}

	entry.id_ = id;
	return true;
}

bool SQLProcessorConfig::Manager::checkPartial(
		const PartialId &id, PartialStatus &status) {
	status = PartialStatus();

	assert(!id.isEmpty());
	{
		util::LockGuard<util::Mutex> guard(mutex_);
		status = partialStatus_;

		if (id.index_ < 0 || static_cast<size_t>(id.index_) >= partialList_.size()) {
			return false;
		}

		PartialEntry &entry = partialList_[static_cast<size_t>(id.index_)];
		if (!entry.id_.matchesId(id)) {
			return false;
		}

		if (entry.id_.index_ == 0) {
			bool pending = false;
			for (PartialList::iterator it = partialList_.begin();
					++it != partialList_.end();) {
				if (entry.execCount_ > it->execCount_ || it->id_.isEmpty()) {
					pending = true;
				}
			}
			if (!pending) {
				entry.execCount_++;
				return false;
			}
		}
		else {
			const PartialEntry &frontEntry = partialList_.front();
			if (entry.execCount_ < frontEntry.execCount_) {
				if (entry.execStarted_) {
					entry.execStarted_ = false;
					entry.execCount_++;
				}
				else {
					entry.execStarted_ = true;
				}
				return false;
			}
		}
	}

	util::Thread::sleep(10);
	return true;
}

void SQLProcessorConfig::Manager::setPartialOpPhase(
		const PartialId &id, int32_t opPhase) {
	if (id.isEmpty() || id.index_ != 0) {
		return;
	}

	util::LockGuard<util::Mutex> guard(mutex_);

	if (partialList_.empty()) {
		return;
	}

	PartialEntry &entry = partialList_.front();
	if (!entry.id_.matchesId(id)) {
		return;
	}

	partialStatus_.opPhase_ = opPhase;
}

void SQLProcessorConfig::Manager::closePartial(const PartialId &id) {
	assert(!id.isEmpty());
	util::LockGuard<util::Mutex> guard(mutex_);

	if (id.index_ != 0 || partialList_.empty()) {
		return;
	}

	PartialEntry &entry = partialList_.front();
	if (!entry.id_.matchesId(id)) {
		return;
	}

	if (++partialStatus_.trialProgress_ <= partialStatus_.trialCount_) {
		entry.id_ = PartialId();
		return;
	}

	partialStatus_.execCount_ = entry.execCount_;
	partialList_.clear();
}


SQLProcessorConfig::Manager::SimulationEntry::SimulationEntry() :
		point_(0),
		action_(0),
		param_(0) {
}

void SQLProcessorConfig::Manager::SimulationEntry::set(
		const picojson::value &value) {
	SQLProcessor::DQLTool::decodeSimulationEntry(value, *this);
}

void SQLProcessorConfig::Manager::SimulationEntry::get(
		picojson::value &value) const {
	SQLProcessor::DQLTool::encodeSimulationEntry(*this, value);
}

SQLProcessorConfig::Manager::PartialId::PartialId() :
		taskId_(0),
		index_(-1) {
}

SQLProcessorConfig::Manager::PartialId::PartialId(
		TaskContext &cxt, int32_t index) :
		jobId_(cxt.getTask() == NULL || cxt.getTask()->getJob() == NULL ?
				JobId() : cxt.getTask()->getJobId()),
		taskId_(cxt.getTask() == NULL ? TaskId() : cxt.getTask()->getTaskId()),
		index_(index) {
}

bool SQLProcessorConfig::Manager::PartialId::isEmpty() const {
	return index_ < 0;
}

bool SQLProcessorConfig::Manager::PartialId::matchesId(
		const PartialId &id) const {
	return (
			jobId_.clientId_ == id.jobId_.clientId_&&
			jobId_.execId_ == id.jobId_.execId_ &&
			taskId_ == id.taskId_ &&
			index_ == id.index_);
}

SQLProcessorConfig::Manager::PartialEntry::PartialEntry() :
		execCount_(0),
		execStarted_(false) {
}

SQLProcessor::SQLProcessor(Context &cxt, const TypeInfo &typeInfo) :
		typeInfo_(typeInfo),
		varAlloc_(cxt.getVarAllocator()) {
}

SQLProcessor::~SQLProcessor() {
}

void SQLProcessor::cleanUp(Context &cxt) {
	static_cast<void>(cxt);
}

SQLType::Id SQLProcessor::getType() const {
	return typeInfo_.type_;
}

bool SQLProcessor::next(Context &cxt) {
	static_cast<void>(cxt);
	return false;
}

const SQLProcessor::Profiler& SQLProcessor::getProfiler() {
	return Profiler::getEmptyProfiler();
}

void SQLProcessor::setProfiler(const Profiler &profiler) {
	static_cast<void>(profiler);
}

void SQLProcessor::exportTo(Context &cxt, const OutOption &option) const {
	static_cast<void>(cxt);
	static_cast<void>(option);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

void SQLProcessor::planToInputInfo(
		const Plan &plan, uint32_t planNodeId,
		const SQLTableInfoList &tableInfoList, TupleInfoList &inputInfo) {
	typedef SQLCompiler::ProcessorUtils ProcessorUtils;

	const Plan::Node &node = plan.nodeList_[planNodeId];

	if (!node.tableIdInfo_.isEmpty()) {
		const SQLTableInfo &tableInfo =
				tableInfoList.resolve(node.tableIdInfo_.id_);

		typedef SQLTableInfo::SQLColumnInfoList ColumnList;
		const ColumnList &columnList = tableInfo.columnInfoList_;

		inputInfo.push_back(TupleInfo(inputInfo.get_allocator()));
		TupleInfo &tupleInfo = inputInfo.back();

		for (ColumnList::const_iterator it = columnList.begin();
				it != columnList.end(); ++it) {
			assert(it->first != TupleList::TYPE_NULL);
			const ColumnId columnId =
					static_cast<ColumnId>(it - columnList.begin());
			tupleInfo.push_back(ProcessorUtils::getTableColumnType(
					tableInfo, columnId, NULL));
		}
	}

	typedef Plan::Node::IdList InputList;
	const InputList &inputList = node.inputList_;

	for (InputList::const_iterator it = inputList.begin();
			it != inputList.end(); ++it) {
		planToOutputInfo(plan, *it, inputInfo);
	}
}

void SQLProcessor::planToInputInfoForDml(
		const Plan &plan, uint32_t planNodeId,
		const SQLTableInfoList &tableInfoList, TupleInfoList &inputInfo) {
	static_cast<void>(tableInfoList);
	const Plan::Node &node = plan.nodeList_[planNodeId];

	typedef Plan::Node::IdList InputList;
	const InputList &inputList = node.inputList_;

	for (InputList::const_iterator it = inputList.begin();
			it != inputList.end(); ++it) {
		planToOutputInfo(plan, *it, inputInfo);
	}
}

void SQLProcessor::planToOutputInfo(
		const Plan &plan, uint32_t planNodeId, TupleInfoList &outputInfo) {
	const Plan::Node &node = plan.nodeList_[planNodeId];

	outputInfo.push_back(TupleInfo(outputInfo.get_allocator()));
	TupleInfo &tupleInfo = outputInfo.back();

	typedef Plan::Node::ExprList ColumnList;
	const ColumnList &columnList = node.outputList_;

	for (ColumnList::const_iterator it = columnList.begin();
			it != columnList.end(); ++it) {
		const TupleList::TupleColumnType type = it->columnType_;
		assert(TupleColumnTypeUtils::isAny(type) ||
				TupleColumnTypeUtils::isDeclarable(type) ||
				TupleColumnTypeUtils::isNullable(type));

		tupleInfo.push_back(type);
	}
}

void SQLProcessor::transcodeProfilerSpecific(
		util::StackAllocator &alloc, const TypeInfo &typeInfo,
		util::AbstractObjectInStream &in, util::AbstractObjectOutStream &out) {
	static_cast<void>(alloc);
	static_cast<void>(typeInfo);
	static_cast<void>(in);
	static_cast<void>(out);
}

SQLProcessor::Factory::Factory() {
}

SQLProcessor* SQLProcessor::Factory::create(
		Context &cxt, SQLType::Id type, const Option &option,
		const TupleInfoList &inputInfo, TupleInfo *outputInfo) {

	SQLVarSizeAllocator &varAlloc = cxt.getVarAllocator();

	SQLProcessor::TypeInfo typeInfo;
	typeInfo.type_ = type;
	FactoryFunc funcRef = getFactoryFuncRef(typeInfo);
	if (funcRef == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION,
				"Unacceptable type as processor (type=" <<
				SQLType::Coder()(type, "") << ")");
	}

	SQLProcessor *processor = (**funcRef)(cxt, typeInfo);

	try {


		TupleInfo outputInfoStorage(cxt.getAllocator());
		TupleInfo &filteredOutputInfo =
				(outputInfo == NULL ? outputInfoStorage : *outputInfo);
		processor->applyInfo(cxt, option, inputInfo, filteredOutputInfo);

	}
	catch (...) {
		ALLOC_VAR_SIZE_DELETE(varAlloc, processor);
		throw;
	}

	return processor;
}

SQLProcessor* SQLProcessor::Factory::create(
		Context &cxt, const Option &option,
		const TupleInfoList &inputInfo, TupleInfo *outputInfo) {
	TypeInfo typeInfo;
	if (option.plan_ != NULL) {
		typeInfo.type_ = option.plan_->nodeList_[option.planNodeId_].type_;
	}
	else if (option.byteInStream_ != NULL) {
		util::ObjectCoder().decode(*option.byteInStream_, typeInfo);
	}
	else if (option.inStream_ != NULL) {
		util::ObjectCoder().decode(*option.inStream_, typeInfo);
	}
	else if (option.jsonValue_ != NULL) {
		JsonUtils::InStream jsonStream(*option.jsonValue_);

		typedef JsonUtils::InStream::ObjectScope ObjectScope;
		ObjectScope objectScope(jsonStream, util::ObjectCoder::Attribute());

		util::ObjectCoder().decode(objectScope.stream(), typeInfo);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}

	return create(cxt, typeInfo.type_, option, inputInfo, outputInfo);
}

void SQLProcessor::Factory::destroy(SQLProcessor *processor) {
	if (processor == NULL) {
		return;
	}

	ProfilerManager &profilerManager = ProfilerManager::getInstance();
	if (profilerManager.getActivatedFlags() != 0 &&
			DQLTool::isDQL(processor->typeInfo_.type_)) {
		profilerManager.addProfiler(processor->getProfiler());
	}

	SQLVarSizeAllocator &varAlloc = processor->varAlloc_;
	ALLOC_VAR_SIZE_DELETE(varAlloc, processor);
}

void SQLProcessor::Factory::transcodeProfiler(
		util::StackAllocator &alloc, const TypeInfo &typeInfo,
		util::AbstractObjectInStream &in, util::AbstractObjectOutStream &out) {
	getProfilerTransFuncRef(typeInfo)(alloc, typeInfo, in, out);
}

SQLProcessor::Factory::FactoryFunc& SQLProcessor::Factory::getFactoryFuncRef(
		const TypeInfo &typeInfo) {
	const SQLType::Id type = typeInfo.type_;
	const ptrdiff_t start = SQLType::START_EXEC;
	const ptrdiff_t end = SQLType::END_EXEC;

	if (!(start <= type && type < end)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	static FactoryFunc funcTable[end - start] = { NULL };
	return funcTable[type - start];
}

SQLProcessor::Factory::ProfilerTransFunc&
SQLProcessor::Factory::getProfilerTransFuncRef(const TypeInfo &typeInfo) {
	const SQLType::Id type = typeInfo.type_;
	const ptrdiff_t start = SQLType::START_EXEC;
	const ptrdiff_t end = SQLType::END_EXEC;

	if (!(start <= type && type < end)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	static ProfilerTransFunc funcTable[end - start] = { NULL };
	return funcTable[type - start];
}

void SQLProcessor::Factory::check(
		SQLProcessor *&orgProcessor, FactoryFunc factoryFunc,
		Context &cxt, SQLType::Id type, const TupleInfoList &inputInfo) {
	if (!DQLTool::isDQL(type)) {
		return;
	}

	SQLVarSizeAllocator &varAlloc = cxt.getVarAllocator();
	util::StackAllocator &alloc = cxt.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	util::XArray<uint8_t> buffer1(alloc);
	{
		util::XArrayOutStream<> outStream(buffer1);
		makeCoder(util::ObjectCoder(), cxt, NULL, NULL).encode(
				outStream, *orgProcessor);
	}

	SQLProcessor *modProcessor2 = NULL;
	SQLProcessor *modProcessor3 = NULL;
	try {
		util::XArray<uint8_t> buffer2(alloc);
		{
			LocalTempStore::Group group(cxt.getStore());
			TupleValue::VarContext varCxt;
			varCxt.setVarAllocator(&cxt.getVarAllocator());
			varCxt.setGroup(&group);

			OutOption outOption;
			outOption.varCxt_ = &varCxt;
			outOption.plan_ = ALLOC_NEW(alloc) SQLPreparedPlan(alloc);
			outOption.planNodeId_ = 0;

			orgProcessor->exportTo(cxt, outOption);

			TupleInfo outputInfo(alloc);
			Option inOption(alloc);
			inOption.plan_ = outOption.plan_;
			inOption.planNodeId_ = 0;

			SQLProcessor::TypeInfo typeInfo;
			typeInfo.type_ = type;

			modProcessor2 = factoryFunc(cxt, typeInfo);
			modProcessor2->applyInfo(cxt, inOption, inputInfo, outputInfo);

			util::XArrayOutStream<> outStream(buffer2);
			makeCoder(util::ObjectCoder(), cxt, NULL, NULL).encode(
					outStream, *modProcessor2);
		}

		util::XArray<uint8_t> buffer3(cxt.getAllocator());
		{
			util::ArrayInStream inStream(buffer1.data(), buffer1.size());
			util::ArrayByteInStream byteInStream(inStream);
			util::ObjectInStream<ByteInStream> objInStream(byteInStream);

			TypeInfo typeInfo;
			util::ObjectCoder().decode(objInStream, typeInfo);

			TupleInfo outputInfo(alloc);
			Option inOption(alloc);
			inOption.byteInStream_ = &objInStream;

			cxt.setPartialMonitorRestricted(false);
			modProcessor3 = factoryFunc(cxt, typeInfo);
			modProcessor3->applyInfo(cxt, inOption, inputInfo, outputInfo);

			util::XArrayOutStream<> outStream(buffer3);
			makeCoder(util::ObjectCoder(), cxt, NULL, NULL).encode(
					outStream, *modProcessor3);
		}

		util::Vector<uint8_t> vec1(buffer1.begin(),  buffer1.end(), alloc);
		util::Vector<uint8_t> vec2(buffer2.begin(),  buffer2.end(), alloc);
		util::Vector<uint8_t> vec3(buffer3.begin(),  buffer3.end(), alloc);
		if (vec1 != vec3 || vec2 != vec3) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		std::swap(orgProcessor, modProcessor3);
		assert(orgProcessor != NULL);
	}
	catch (...) {
		ALLOC_VAR_SIZE_DELETE(varAlloc, modProcessor2);
		ALLOC_VAR_SIZE_DELETE(varAlloc, modProcessor3);
		throw;
	}
	ALLOC_VAR_SIZE_DELETE(varAlloc, modProcessor2);
	ALLOC_VAR_SIZE_DELETE(varAlloc, modProcessor3);
}

SQLContext::SQLContext(
		SQLVariableSizeGlobalAllocator *valloc,
		Task *task,
		SQLAllocator *alloc,
		SQLVarSizeAllocator *varAlloc,
		LocalTempStore &store,
		const SQLProcessorConfig *config) :
		TaskContext(),
		varAlloc_(varAlloc),
		store_(store),
		localDest_(NULL),
		interruptionHandler_(NULL),
		event_(NULL),
		partitionList_(NULL),
		clusterService_(NULL),
		transactionManager_(NULL),
		transactionService_(NULL),
		partitionTable_(NULL),
		execution_(NULL),
		executionManager_(NULL),
		clientId_(NULL),
		stmtId_(UNDEF_STATEMENTID),
		jobStartTime_(-1),
		finished_(false),
		fetchComplete_(false),
		execId_(-1),
		config_(config),
		tableSchema_(NULL),
		tableLatch_(NULL),
		isDmlTimeSeries_(false),
		jobCheckComplete_(false),
		partialMonitorRestricted_(false),
		storeMemoryAgingSwapRate_(-1),
		timeZone_(util::TimeZone())
		,
		setGroup_(false),
		groupId_(LocalTempStore::UNDEF_GROUP_ID)
{
	setAllocator(alloc);
}

SQLContext::SQLContext(
		SQLAllocator *alloc,
		SQLVarSizeAllocator *varAlloc,
		LocalTempStore &store,
		const SQLProcessorConfig *config) :
		varAlloc_(varAlloc),
		store_(store),
		localDest_(NULL),
		interruptionHandler_(NULL),
		event_(NULL),
		partitionList_(NULL),
		clusterService_(NULL),
		transactionManager_(NULL),
		transactionService_(NULL),
		partitionTable_(NULL),
		execution_(NULL),
		executionManager_(NULL),
		clientId_(NULL),
		stmtId_(UNDEF_STATEMENTID),
		jobStartTime_(-1),
		finished_(false),
		fetchComplete_(false),
		execId_(-1),
		config_(config),
		tableSchema_(NULL),
		tableLatch_(NULL),
		isDmlTimeSeries_(false),
		jobCheckComplete_(false),
		partialMonitorRestricted_(false),
		storeMemoryAgingSwapRate_(-1),
		timeZone_(util::TimeZone())
		,
		setGroup_(false),
		groupId_(LocalTempStore::UNDEF_GROUP_ID)
{
	setAllocator(alloc);
}


void SQLContext::finish() {
	TaskContext::finish();
	finished_ = true;
}

bool SQLContext::isFinished() const {
	return finished_;
}

SQLVarSizeAllocator& SQLContext::getVarAllocator() {
	return *varAlloc_;
}

LocalTempStore& SQLContext::getStore() {
	return store_;
}

void SQLContext::setLocalDestination(TupleList &localDest) {
	localDest_ = &localDest;
}

InterruptionChecker::CheckHandler* SQLContext::getInterruptionHandler() const {
	return interruptionHandler_;
}

void SQLContext::setInterruptionHandler(
		InterruptionChecker::CheckHandler *handler) {
	interruptionHandler_ = handler;
}

const Event* SQLContext::getEvent() const {
	return event_;
}

void SQLContext::setEvent(const Event *ev) {
	event_ = ev;
}

util::StackAllocator& SQLContext::getEventAllocator() const {
	EventContext *eventContext = getEventContext();
	if (eventContext == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return eventContext->getAllocator();
}

DataStoreV4* SQLContext::getDataStore(PartitionId pId) const {
	DataStoreBase& dsBase =
		partitionList_->partition(pId).dataStore();
	return reinterpret_cast<DataStoreV4*>(&dsBase);
}

void SQLContext::setDataStore(PartitionList *partitionList) {
	partitionList_ = partitionList;
}

PartitionList* SQLContext::getPartitionList() const {
	return partitionList_;
}


ClusterService* SQLContext::getClusterService() const {
	return clusterService_;
}

void SQLContext::setClusterService(ClusterService *clusterService) {
	clusterService_ = clusterService;
}

TransactionManager* SQLContext::getTransactionManager() const {
	return transactionManager_;
}

void SQLContext::setTransactionManager(
		TransactionManager *transactionManager) {
	transactionManager_ = transactionManager;
}

TransactionService* SQLContext::getTransactionService() const {
	return transactionService_;
}

void SQLContext::setTransactionService(
		TransactionService *transactionService) {
	transactionService_ = transactionService;
}

PartitionTable* SQLContext::getPartitionTable() const {
	return partitionTable_;
}

void SQLContext::setPartitionTable(PartitionTable *partitionTable) {
	partitionTable_ = partitionTable;
}

SQLExecution* SQLContext::getExecution() const {
	return execution_;
}

void SQLContext::setExecution(SQLExecution *execution) {
	execution_ = execution;
}

SQLExecutionManager* SQLContext::getExecutionManager() const {
	return executionManager_;
}

void SQLContext::setExecutionManager(SQLExecutionManager *executionManager) {
	executionManager_ = executionManager;
}

ClientId* SQLContext::getClientId() const {
	return clientId_;
}

void SQLContext::setClientId(ClientId *clientId) {
	clientId_ = clientId;
}

StatementId SQLContext::getStatementId() const {
	return stmtId_;
}

void SQLContext::setStatementId(StatementId stmtId) {
	stmtId_ = stmtId;
}

Timestamp SQLContext::getJobStartTime() const {
	return jobStartTime_;
}

void SQLContext::setJobStartTime(Timestamp jobStartTime) {
	jobStartTime_ = jobStartTime;
}

const SQLProcessorConfig* SQLContext::getConfig() const {
	return config_;
}

bool SQLContext::isPartialMonitorRestricted() const {
	return partialMonitorRestricted_;
}

void SQLContext::setPartialMonitorRestricted(bool restricted) {
	partialMonitorRestricted_ = restricted;
}

double SQLContext::getStoreMemoryAgingSwapRate() const {
	return storeMemoryAgingSwapRate_;
}

void SQLContext::setStoreMemoryAgingSwapRate(double storeMemoryAgingSwapRate) {
	storeMemoryAgingSwapRate_ = storeMemoryAgingSwapRate;
}

util::TimeZone SQLContext::getTimeZone() const {
	return timeZone_;
}

void SQLContext::setTimeZone(const util::TimeZone &zone) {
	timeZone_ = zone;
}

SQLProcessor::Profiler::Profiler(const util::StdAllocator<void, void> &alloc) :
		alloc_(alloc),
		option_(NULL, alloc),
		result_(NULL, alloc),
		streamData_(NULL, alloc),
		forAnalysis_(false) {
	util::StdAllocator<picojson::value, void> jsonAlloc = alloc_;
	option_.reset(new (jsonAlloc.allocate(1)) picojson::value());
	result_.reset(new (jsonAlloc.allocate(1)) picojson::value());
}

SQLProcessor::Profiler::~Profiler() {
}

picojson::value& SQLProcessor::Profiler::getOption() {
	if (option_.get() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *option_;
}

const picojson::value& SQLProcessor::Profiler::getOption() const {
	if (option_.get() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *option_;
}

picojson::value& SQLProcessor::Profiler::getResult() {
	if (result_.get() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *result_;
}

const picojson::value& SQLProcessor::Profiler::getResult() const {
	if (result_.get() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *result_;
}

SQLProcessor::Profiler::StreamData&
SQLProcessor::Profiler::prepareStreamData() {
	if (streamData_.get() == NULL) {
		util::StdAllocator<StreamData, void> streamAlloc = alloc_;
		streamData_.reset(new (streamAlloc.allocate(1)) StreamData(alloc_));
	}
	return *streamData_.get();
}

const SQLProcessor::Profiler::StreamData*
SQLProcessor::Profiler::getStreamData() const {
	return streamData_.get();
}

void SQLProcessor::Profiler::setForAnalysis(bool forAnalysis) {
	forAnalysis_ = forAnalysis;
}

bool SQLProcessor::Profiler::isForAnalysis() const {
	return forAnalysis_;
}

const SQLProcessor::Profiler& SQLProcessor::Profiler::getEmptyProfiler() {
	static std::allocator<char> alloc;
	static const Profiler profiler(alloc);
	return profiler;
}

SQLProcessor::Profiler::Profiler(
		const util::StdAllocator<void, void> &alloc, const util::FalseType&) :
		alloc_(alloc),
		option_(NULL, alloc),
		result_(NULL, alloc),
		streamData_(NULL, alloc),
		forAnalysis_(false) {
}

SQLProcessor::Profiler::CoderBase::CoderBase(util::StackAllocator &alloc) :
		alloc_(alloc) {
}

void SQLProcessor::Profiler::CoderBase::encodeCustom(
		const TaskProfiler &profiler,
		util::AbstractObjectOutStream &out) const {

	const util::XArray<uint8_t> *customData = profiler.customData_;
	if (customData == NULL || customData->empty()) {
		return;
	}

	ByteInStream byteIn(
			util::ArrayInStream(customData->data(), customData->size()));
	util::ObjectInStream<ByteInStream> objIn(byteIn);

	TypeInfo typeInfo;
	util::ObjectCoder().decode(objIn, typeInfo);

	typedef util::AbstractObjectCoder::In::Selector<> Selector;
	Factory::transcodeProfiler(alloc_, typeInfo, Selector()(objIn)(), out);
}

SQLProcessor::ProfilerManager SQLProcessor::ProfilerManager::instance_;

SQLProcessor::ProfilerManager::ProfilerManager() :
		maxListSize_(10),
		entryList_(UTIL_NEW EntryList()) {
}

bool SQLProcessor::ProfilerManager::isEnabled() {
	return SQL_PROCESSOR_PROFILER_MANAGER_ENABLED;
}

SQLProcessor::ProfilerManager& SQLProcessor::ProfilerManager::getInstance() {
	return instance_;
}

int32_t SQLProcessor::ProfilerManager::getActivatedFlags() {
	return 0;
}

bool SQLProcessor::ProfilerManager::isActivated(ProfilerType type) {
	static_cast<void>(type);
	return false;
}

void SQLProcessor::ProfilerManager::setActivated(ProfilerType type, bool activated) {
	if (activated) {
		flags_ |= static_cast<int32_t>(1 << type);
	}
	else {
		flags_ &= ~static_cast<int32_t>(1 << type);
	}
}

void SQLProcessor::ProfilerManager::listIds(util::Vector<ProfilerId> &idList) {
	util::LockGuard<util::Mutex> guard(mutex_);

	idList.clear();
	const size_t count = entryList_->size();
	for (size_t i = 0; i < count; i++) {
		idList.push_back(startId_ + i);
	}
}

bool SQLProcessor::ProfilerManager::getProfiler(
		ProfilerId id, Profiler &profiler) {
	util::LockGuard<util::Mutex> guard(mutex_);
	profiler.getResult() = picojson::value();

	if (id < startId_) {
		return false;
	}

	const size_t index = static_cast<size_t>(id - startId_);
	if (index >= entryList_->size()) {
		return false;
	}

	profiler.getResult() = (*entryList_)[index];
	return true;
}

void SQLProcessor::ProfilerManager::addProfiler(const Profiler &profiler) {
	util::LockGuard<util::Mutex> guard(mutex_);

	const std::string &nameKey = "jobId";
	const picojson::value &srcValue = profiler.getResult();
	const std::string *name = JsonUtils::find<std::string>(srcValue, nameKey);
	if (name == NULL) {
		return;
	}

	picojson::object *destObj;
	{
		IdMap::const_iterator it = idMap_.find(*name);
		if (it == idMap_.end()) {
			while (!entryList_->empty() &&
					entryList_->size() >= maxListSize_) {
				idMap_.erase(JsonUtils::as<std::string>(
						entryList_->front(), nameKey));
				entryList_->pop_front();
				startId_++;
			}

			const ProfilerId id = startId_ + entryList_->size();
			idMap_.insert(std::make_pair(*name, id));
			entryList_->push_back(picojson::value(picojson::object()));

			picojson::value &destValue = entryList_->back();
			destObj = &destValue.get<picojson::object>();

			(*destObj)[nameKey] = picojson::value(*name);
		}
		else {
			const ProfilerId id = it->second;
			if (id < startId_) {
				return;
			}

			const size_t index = static_cast<size_t>(id - startId_);
			if (index >= entryList_->size()) {
				return;
			}

			destObj = &JsonUtils::as<picojson::object>((*entryList_)[index]);
		}
	}

	picojson::value &taskValue = (*destObj)["task"];
	if (!taskValue.is<picojson::array>()) {
		taskValue = picojson::value(picojson::array());
	}

	picojson::array &taskArray = taskValue.get<picojson::array>();
	taskArray.push_back(srcValue);
	JsonUtils::as<picojson::object>(taskArray.back()).erase(nameKey);
}

void SQLProcessor::ProfilerManager::incrementErrorCount(int32_t code) {
	if ((getActivatedFlags() & (1 << PROFILE_ERROR)) == 0) {
		return;
	}

	util::LockGuard<util::Mutex> guard(mutex_);
	if (errorCountMap_.get() == NULL) {
		errorCountMap_.reset(UTIL_NEW ErrorCountMap);
	}
	(*errorCountMap_)[code]++;
}

void SQLProcessor::ProfilerManager::getGlobalProfile(picojson::value &value) {
	value = picojson::value(picojson::object());
	picojson::object &valueObj = value.get<picojson::object>();

	if ((getActivatedFlags() & (1 << PROFILE_INTERRUPTION)) != 0) {
		DQLTool::getInterruptionProfile(valueObj["interruption"]);
	}

	if ((getActivatedFlags() & (1 << PROFILE_ERROR)) != 0) {
		util::LockGuard<util::Mutex> guard(mutex_);
		picojson::value &value = valueObj["error"];
		value = picojson::value(picojson::array());

		if (errorCountMap_.get() != NULL) {
			picojson::array &entryList = JsonUtils::as<picojson::array>(value);
			for (ErrorCountMap::const_iterator it = errorCountMap_->begin();
					it != errorCountMap_->end(); ++it) {
				picojson::object entry;
				entry["code"] =
						picojson::value(static_cast<double>(it->first));
				entry["count"] =
						picojson::value(static_cast<double>(it->second));
				entryList.push_back(picojson::value(entry));
			}
		}
	}
}

SQLProcessor::OutOption::OutOption() :
		varCxt_(NULL),
		plan_(NULL),
		planNodeId_(0),
		byteOutStream_(NULL),
		outStream_(NULL) {
}

SQLProcessor::TypeInfo::TypeInfo() : type_(SQLType::Id()) {
}
