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

#include "sql_processor_dql.h"

#include "sql_utils_container.h" 
#include "sql_utils_vdbe.h" 
#include "sql_utils_container.h" 

#include "sql_execution.h"
#include "transaction_manager.h"


const DQLProcessor::ProcRegistrar DQLProcessor::REGISTRAR_LIST[] = {
	ProcRegistrar::of<DQLProcs::GroupOption>(SQLType::EXEC_GROUP),
	ProcRegistrar::of<DQLProcs::JoinOption>(SQLType::EXEC_JOIN),
	ProcRegistrar::of<DQLProcs::LimitOption>(SQLType::EXEC_LIMIT),
	ProcRegistrar::of<DQLProcs::ScanOption>(SQLType::EXEC_SCAN),
	ProcRegistrar::of<DQLProcs::SelectOption>(SQLType::EXEC_SELECT),
	ProcRegistrar::of<DQLProcs::SortOption>(SQLType::EXEC_SORT),
	ProcRegistrar::of<DQLProcs::UnionOption>(SQLType::EXEC_UNION)
};

const DQLProcessor::ProcRegistrarTable DQLProcessor::REGISTRAR_TABLE(
		REGISTRAR_LIST, sizeof(REGISTRAR_LIST) / sizeof(*REGISTRAR_LIST));

DQLProcessor::DQLProcessor(Context &cxt, const TypeInfo &typeInfo) :
		SQLProcessor(cxt, typeInfo),
		inputIdOffset_(0),
		profiler_(cxt.getVarAllocator()) {
}

DQLProcessor::~DQLProcessor() {
	if (store_.get() != NULL) {
		store_->closeAllLatchResources();
	}
}

bool DQLProcessor::pipe(Context &cxt, InputId inputId, const Block &block) {
	OpStore::Entry *entry = findInputEntry(cxt, inputId);
	if (entry != NULL) {
		entry->appendBlock(0, block);
	}

	OpCursor c(getCursorSource(cxt));
	for (; c.exists(); c.next()) {
		c.get().execute(c.getContext());
	}
	return c.release();
}

bool DQLProcessor::finish(Context &cxt, InputId inputId) {
	getInputEntry(cxt, inputId).setCompleted();

	OpCursor c(getCursorSource(cxt));
	for (; c.exists(); c.next()) {
		c.get().execute(c.getContext());
	}
	return c.release();
}

bool DQLProcessor::next(Context &cxt) {
	OpCursor c(getCursorSource(cxt));
	for (; c.exists(); c.next()) {
		c.get().execute(c.getContext());
	}
	return c.release();
}

bool DQLProcessor::applyInfo(
		Context &cxt, const Option &option,
		const TupleInfoList &inputInfo, TupleInfo &outputInfo) {
	if (infoEntry_.get() == NULL) {
		SQLValues::VarAllocator &varAlloc = cxt.getVarAllocator();

		util::AllocUniquePtr<InfoEntry> infoEntry;
		infoEntry = ALLOC_UNIQUE(varAlloc, InfoEntry, varAlloc);

		infoEntry->storeGroup_ = UTIL_MAKE_LOCAL_UNIQUE(
				infoEntry->storeGroup_, TupleList::Group, cxt.getStore());

		util::LocalUniquePtr<SQLValues::VarContext> varCxt;
		SQLValues::ValueContext valueCxt(getValueContextSource(
				cxt.getAllocator(), varAlloc, *infoEntry->storeGroup_, varCxt));

		DQLProcs::Option opOption;
		{
			DQLProcs::OptionInput in(
					valueCxt, getRegistrarTable(), getType(), &option);
			opOption.importFrom(in);

			typedef util::XArrayOutStream<
					util::StdAllocator<uint8_t, void> > ByteOutStream;
			typedef util::ObjectOutStream<ByteOutStream> OutStream;

			ByteOutStream byteOutStream(infoEntry->optionImage_);
			OutStream outStream(byteOutStream);
			util::AbstractObjectOutStream::Wrapper<
					OutStream> wrappedStream(outStream);

			DQLProcs::ProcOutOption procOutOption;
			procOutOption.outStream_ = &wrappedStream;
			DQLProcs::OptionOutput out(&procOutOption, getType());
			opOption.exportTo(out);
		}

		{
			AllocTupleInfoList &dest = infoEntry->inputInfo_;
			for (TupleInfoList::const_iterator it = inputInfo.begin();
					it != inputInfo.end(); ++it) {
				dest.push_back(AllocTupleInfo(varAlloc));
				dest.back().assign(it->begin(), it->end());
			}
		}

		{
			const SQLOps::OpCode &code = createOpCode(
					cxt.getAllocator(), opOption, infoEntry->inputInfo_);
			TupleInfo dest(cxt.getAllocator());
			getOutputInfo(code, dest);
			infoEntry->outputInfo_.assign(dest.begin(), dest.end());
		}

		infoEntry_.swap(infoEntry);
	}

	outputInfo.assign(
			infoEntry_->outputInfo_.begin(), infoEntry_->outputInfo_.end());
	return true;
}

const SQLProcessor::Profiler& DQLProcessor::getProfiler() {
	if (store_.get() == NULL) {
		return profiler_;
	}

	TypeInfo typeInfo;
	typeInfo.type_ = getType();

	SQLOpUtils::AnalysisInfo mainInfo;
	SQLOps::OpProfiler *opProfiler = store_->getProfiler();
	if (opProfiler != NULL) {
		mainInfo = opProfiler->getAnalysisInfo(NULL);
	}

	if (matchProfilerResultType(profiler_, true)) {
		Profiler::makeStreamData(
				util::ObjectCoder(), typeInfo, mainInfo.toRoot(),
				profiler_.prepareStreamData());
	}
	else if (matchProfilerResultType(profiler_, false)) {
		JsonUtils::OutStream stream(profiler_.getResult());
		util::AbstractObjectOutStream::Wrapper<
				JsonUtils::OutStream> wrappedStream(stream);
		util::ObjectCoder().encode(wrappedStream, mainInfo);
	}

	return profiler_;
}

void DQLProcessor::setProfiler(const Profiler &profiler) {
	profiler_.getOption() = profiler.getOption();
	profiler_.setForAnalysis(profiler.isForAnalysis());

	if (store_.get() == NULL) {
		return;
	}

	if (isProfilerRequested(profiler_)) {
		store_->activateProfiler();
	}
}

void DQLProcessor::exportTo(Context &cxt, const OutOption &option) const {
	if (infoEntry_.get() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	util::LocalUniquePtr<SQLValues::VarContext> varCxt;
	SQLValues::ValueContext valueCxt(getValueContextSource(
			cxt.getAllocator(), cxt.getVarAllocator(),
			*infoEntry_->storeGroup_, varCxt));

	DQLProcs::Option opOption;
	loadOption(valueCxt, infoEntry_->optionImage_, opOption);

	DQLProcs::OptionOutput out(&option, getType());
	opOption.exportTo(out);
}

void DQLProcessor::transcodeProfilerSpecific(
		util::StackAllocator &alloc, const TypeInfo &typeInfo,
		util::AbstractObjectInStream &in,
		util::AbstractObjectOutStream &out) {
	if (!isDQL(typeInfo.type_)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	SQLOpUtils::AnalysisRootInfo value;
	util::ObjectCoder::withAllocator(alloc).decode(in, value);
	util::ObjectCoder::withAllocator(alloc).encode(out, value);
}

bool DQLProcessor::isDQL(SQLType::Id type) {
	return (getRegistrarTable().find(type) != NULL);
}

const DQLProcs::ProcRegistrarTable& DQLProcessor::getRegistrarTable() {
	return REGISTRAR_TABLE;
}

SQLOps::OpStore::Entry& DQLProcessor::getInputEntry(
		Context &cxt, InputId inputId) {
	OpStore::Entry *entry = findInputEntry(cxt, inputId);
	if (entry == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Ignorable ID specified");
	}
	return *entry;
}

SQLOps::OpStore::Entry* DQLProcessor::findInputEntry(
		Context &cxt, InputId inputId) {
	OpStore &store = prepareStore(cxt);
	OpStore::Entry &rootEntry = store.getEntry(getStoreRootId());
	const uint32_t inCount = rootEntry.getInputCount();

	const bool ignorable = (inputIdOffset_ != 0 && inCount <= 1);
	if (ignorable && inputId != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Invalid input ID as ignorable input");
	}

	if (ignorable) {
		return NULL;
	}

	const InputId inputCount = static_cast<InputId>(inCount);
	if (inputId < 0 || inputId >= inputCount - inputIdOffset_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Invalid input ID");
	}

	const uint32_t inIndex = static_cast<uint32_t>(inputIdOffset_ + inputId);
	const OpStoreId &storeInId = rootEntry.getLink(inIndex, true);
	OpStore::Entry &inEntry = store.getEntry(storeInId);
	if (inEntry.isCompleted()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Input already completed");
	}

	return &inEntry;
}

SQLOps::OpCursor::Source DQLProcessor::getCursorSource(Context &cxt) {
	OpStore &store = prepareStore(cxt);
	OpCursor::Source source(getStoreRootId(), store, NULL);
	source.setExtContext(&getExtOpContext(cxt));

	return source;
}

SQLOps::OpStore& DQLProcessor::prepareStore(Context &cxt) {
	if (store_.get() == NULL) {
		if (infoEntry_.get() == NULL || !storeRootId_.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
		}

		SQLOps::ExtOpContext &extCxt = getExtOpContext(cxt);
		allocManager_ = UTIL_MAKE_LOCAL_UNIQUE(
				allocManager_, OpAllocatorManager,
				cxt.getVarAllocator(), false,
				&OpAllocatorManager::getDefault().getSubManager(extCxt),
				&cxt.getVarAllocator());
		store_ = UTIL_MAKE_LOCAL_UNIQUE(store_, OpStore, *allocManager_);

		store_->setStackAllocatorBase(&cxt.getAllocator().base());

		const OpStoreId &id = store_->allocateEntry();
		OpStore::Entry &entry = store_->getEntry(id);

		cxt.setProcessorGroup(infoEntry_->storeGroup_.get());
		store_->setTempStoreGroup(*infoEntry_->storeGroup_);

		SQLValues::ValueContext valueCxt(entry.getValueContextSource(NULL));
		DQLProcs::Option option;
		loadOption(valueCxt, infoEntry_->optionImage_, option);

		const AllocTupleInfoList &inputInfo = infoEntry_->inputInfo_;
		setUpConfig(*store_, cxt.getConfig(), false);
		setUpRoot(*store_, entry, option, inputInfo);
		setUpInput(getRootCode(*store_, id), inputInfo.size(), inputIdOffset_);

		if (isProfilerRequested(profiler_)) {
			store_->activateProfiler();
		}

		storeRootId_ = id;
	}

	return *store_;
}

SQLOps::ExtOpContext& DQLProcessor::getExtOpContext(Context &cxt) {
	if (extCxt_.get() == NULL) {
		extCxt_ =
				ALLOC_UNIQUE(cxt.getVarAllocator(), DQLProcs::ExtProcContext);
		extCxt_->setBase(cxt);
	}
	return *extCxt_;
}

void DQLProcessor::loadOption(
		SQLValues::ValueContext &valueCxt, const OptionImage &image,
		DQLProcs::Option &option) const {

	util::ArrayByteInStream byteInStream(
			(util::ArrayInStream(image.data(), image.size())));
	util::ObjectInStream<util::ArrayByteInStream> inStream(byteInStream);

	DQLProcs::ProcInOption procInOption(valueCxt.getAllocator());
	procInOption.byteInStream_ = &inStream;

	DQLProcs::OptionInput in(
			valueCxt, getRegistrarTable(), getType(), &procInOption);
	option.importFrom(in);
}

SQLValues::ValueContext::Source DQLProcessor::getValueContextSource(
		util::StackAllocator &alloc, SQLValues::VarAllocator &varAlloc,
		TupleList::Group &storeGroup,
		util::LocalUniquePtr<SQLValues::VarContext> &varCxt) {
	if (varCxt.get()  == NULL) {
		varCxt = UTIL_MAKE_LOCAL_UNIQUE(varCxt, SQLValues::VarContext);
		varCxt->setVarAllocator(&varAlloc);
		varCxt->setGroup(&storeGroup);
	}
	return SQLValues::ValueContext::Source(&alloc, varCxt.get(), NULL);
}

const SQLOps::OpStoreId& DQLProcessor::getStoreRootId() const {
	if (storeRootId_.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT,
				"Not initialized");
	}
	return storeRootId_;
}

const SQLOps::OpCode& DQLProcessor::getRootCode(
		OpStore &store, const OpStoreId &rootId) {
	SQLOps::OpPlan &plan = store.getEntry(rootId).getPlan();
	return plan.getParentOutput(0).getInput(0).getCode();
}

void DQLProcessor::setUpConfig(
		OpStore &store, const SQLProcessorConfig *config, bool merged) {
	if (!merged) {
		SQLProcessorConfig localConfig;
		localConfig.merge(config, true);
		setUpConfig(store, &localConfig, true);
		return;
	}

	SQLOps::OpConfig dest;
	{
		int64_t limitBytes = config->workMemoryLimitBytes_;
		if (limitBytes < 0) {
			limitBytes = SQLProcessorConfig().getDefault().workMemoryLimitBytes_;
		}

		if (limitBytes < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
		}

		dest.set(SQLOpTypes::CONF_WORK_MEMORY_LIMIT, limitBytes);
	}

	dest.set(
			SQLOpTypes::CONF_INTERRUPTION_PROJECTION_COUNT,
			config->interruptionProjectionCount_);
	dest.set(
			SQLOpTypes::CONF_INTERRUPTION_SCAN_COUNT,
			config->interruptionScanCount_);
	store.setConfig(dest);
}

void DQLProcessor::setUpRoot(
		OpStore &store, OpStore::Entry &rootEntry,
		const DQLProcs::Option &option, const AllocTupleInfoList &inputInfo) {
	util::StackAllocator &alloc = rootEntry.getStackAllocator();
	const SQLOps::OpCode &code = createOpCode(alloc, option, inputInfo);
	const bool inputIgnorable = (code.findContainerLocation() != NULL);

	for (AllocTupleInfoList::const_iterator inIt = inputInfo.begin();
			inIt != inputInfo.end(); ++inIt) {
		const SQLOps::OpStoreId &id = store.allocateEntry();

		const TupleList::TupleColumnType *list =
				(inIt->empty() ? NULL : &(*inIt)[0]);

		OpStore::Entry &entry = store.getEntry(id);
		entry.setColumnTypeList(0, list, inIt->size(), false, false);
		entry.createTupleList(0);
		entry.setPipelined();

		if (inputIgnorable && inIt == inputInfo.begin()) {
			entry.setCompleted();
		}

		const uint32_t index = static_cast<uint32_t>(inIt - inputInfo.begin());
		rootEntry.setLink(index, true, id, 0);
	}

	{
		const SQLOps::OpStoreId &id = store.allocateEntry();

		TupleInfo outputInfo(alloc);
		getOutputInfo(code, outputInfo);

		OpStore::Entry &entry = store.getEntry(id);
		entry.setColumnTypeList(0, outputInfo, false, false);
		entry.createTupleList(0);

		const uint32_t index = 0;
		rootEntry.setLink(index, false, id, 0);
	}

	SQLOps::OpPlan &plan = rootEntry.getPlan();
	SQLOps::OpNode &node = plan.createNode(code.getType());
	node.setCode(code);

	for (AllocTupleInfoList::const_iterator inIt = inputInfo.begin();
			inIt != inputInfo.end(); ++inIt) {
		const uint32_t index = static_cast<uint32_t>(inIt - inputInfo.begin());
		node.addInput(plan.getParentInput(index));
	}

	{
		const uint32_t index = 0;
		plan.getParentOutput(index).addInput(node);
	}

	plan.setCompleted();
}

void DQLProcessor::setUpInput(
		const SQLOps::OpCode &code, size_t inputCount,
		InputId &inputIdOffset) {
	const bool inputIgnorable = (code.findContainerLocation() != NULL);
	const bool inputEmpty = (inputCount <= 0);

	InputId offset;
	if (inputIgnorable || inputEmpty) {
		offset = 1;
	}
	else {
		offset = 0;
	}

	inputIdOffset = offset;
}

SQLOps::OpCode DQLProcessor::createOpCode(
		util::StackAllocator &alloc, const DQLProcs::Option &option,
		const AllocTupleInfoList &inputInfo) {
	SQLOps::OpCodeBuilder builder(SQLOps::OpCodeBuilder::ofAllocator(alloc));

	SQLExprs::ExprFactoryContext &cxt = builder.getExprFactoryContext();

	for (AllocTupleInfoList::const_iterator inIt = inputInfo.begin();
			inIt != inputInfo.end(); ++inIt) {
		const uint32_t index = static_cast<uint32_t>(inIt - inputInfo.begin());
		for (AllocTupleInfo::const_iterator it = inIt->begin();
				it != inIt->end(); ++it) {
			const uint32_t pos = static_cast<uint32_t>(it - inIt->begin());
			cxt.setInputType(index, pos, *it);
			cxt.setInputNullable(index, false);
		}
	}

	return option.toCode(builder);
}

void DQLProcessor::getOutputInfo(
		const SQLOps::OpCode &code, TupleInfo &outputInfo) {
	outputInfo.clear();
	SQLOps::OpCodeBuilder::resolveColumnTypeList(code, outputInfo, NULL);
}

bool DQLProcessor::isProfilerRequested(const Profiler &profiler) {
	return (matchProfilerResultType(profiler, true) ||
			matchProfilerResultType(profiler, false));
}

bool DQLProcessor::matchProfilerResultType(
		const Profiler &profiler, bool asStream) {
	if (asStream) {
		return profiler.isForAnalysis();
	}
	else {
		const picojson::value &option = profiler.getOption();
		const bool *enabled = (option.is<picojson::object>() ?
				JsonUtils::find<bool>(option, "op") : NULL);
		return (enabled != NULL && *enabled);
	}
}


DQLProcessor::InfoEntry::InfoEntry(
		const util::StdAllocator<void, void> &alloc) :
		optionImage_(alloc),
		inputInfo_(alloc),
		outputInfo_(alloc) {
}


template<typename T>
DQLProcs::ProcRegistrar DQLProcs::ProcRegistrar::of(SQLType::Id type) {
	return ProcRegistrar(type, &createSubOption<T>);
}

SQLType::Id DQLProcs::ProcRegistrar::getType() const {
	return type_;
}

DQLProcs::ProcRegistrar::SubOptionFactoryFunc
DQLProcs::ProcRegistrar::getSubOptionFactory() const {
	return subOptionFactory_;
}

DQLProcs::ProcRegistrar::ProcRegistrar(
		SQLType::Id type, SubOptionFactoryFunc subOptionFactory) :
		base_(type),
		type_(type),
		subOptionFactory_(subOptionFactory) {
}

template<typename T>
DQLProcs::SubOption* DQLProcs::ProcRegistrar::createSubOption(
		util::StackAllocator &alloc) {
	return ALLOC_NEW(alloc) BasicSubOption<T>(alloc);
}


DQLProcs::ProcRegistrarTable::ProcRegistrarTable(
		const ProcRegistrar *list, size_t size) :
		list_(list),
		size_(size) {
}

const DQLProcs::ProcRegistrar& DQLProcs::ProcRegistrarTable::resolve(
		SQLType::Id type) const {
	const ProcRegistrar *registrar = find(type);
	if (registrar == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}
	return *registrar;
}

const DQLProcs::ProcRegistrar* DQLProcs::ProcRegistrarTable::find(
		SQLType::Id type) const {
	const ProcRegistrar *it = list_;
	const ProcRegistrar *end = it + size_;
	for (; it != end; ++it) {
		if (it->getType() == type) {
			return it;
		}
	}
	return NULL;
}


DQLProcs::ProcResourceSet::ProcResourceSet() :
		baseCxt_(NULL) {
}

void DQLProcs::ProcResourceSet::setBase(SQLContext &baseCxt) {
	baseCxt_ = &baseCxt;
}

SQLService* DQLProcs::ProcResourceSet::getSQLService() const {
	return baseCxt_->getExecutionManager()->getSQLService();
}

TransactionManager* DQLProcs::ProcResourceSet::getTransactionManager() const {
	return baseCxt_->getTransactionManager();
}

TransactionService* DQLProcs::ProcResourceSet::getTransactionService() const {
	return baseCxt_->getTransactionService();
}

ClusterService* DQLProcs::ProcResourceSet::getClusterService() const {
	return baseCxt_->getClusterService();
}

PartitionTable* DQLProcs::ProcResourceSet::getPartitionTable() const {
	return baseCxt_->getPartitionTable();
}

DataStoreV4* DQLProcs::ProcResourceSet::getDataStore() const {
	return baseCxt_->getDataStore();
}

PartitionList* DQLProcs::ProcResourceSet::getPartitionList() const {
	return baseCxt_->getPartitionList();
}


DQLProcs::ExtProcContext::ExtProcContext() :
		baseCxt_(NULL) {
}

void DQLProcs::ExtProcContext::setBase(SQLContext &baseCxt) {
	baseCxt_ = &baseCxt;
	resourceSet_.setBase(baseCxt);
}

size_t DQLProcs::ExtProcContext::findMaxStringLength() {
	return SQLContainerUtils::ContainerUtils::findMaxStringLength(
			getResourceSet());
}

int64_t DQLProcs::ExtProcContext::getCurrentTimeMillis() {
	return getBase().getJobStartTime();
}

util::TimeZone DQLProcs::ExtProcContext::getTimeZone() {
	return getBase().getTimeZone();
}

void DQLProcs::ExtProcContext::transfer(TupleList::Block &block, uint32_t id) {
	if (id != 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	getBase().transfer(block);
}

void DQLProcs::ExtProcContext::checkCancelRequest() {
	getBase().checkCancelRequest();
}

void DQLProcs::ExtProcContext::finishRootOp() {
	getBase().finish();
}

const Event* DQLProcs::ExtProcContext::getEvent() {
	return getBase().getEvent();
}

EventContext* DQLProcs::ExtProcContext::getEventContext() {
	return getBase().getEventContext();
}

SQLExecutionManager* DQLProcs::ExtProcContext::getExecutionManager() {
	return getBase().getExecutionManager();
}

const ResourceSet* DQLProcs::ExtProcContext::getResourceSet() {
	return &resourceSet_;
}

bool DQLProcs::ExtProcContext::isOnTransactionService() {
	return getBase().isTransactionService();
}

double DQLProcs::ExtProcContext::getStoreMemoryAgingSwapRate() {
	return getBase().getStoreMemoryAgingSwapRate();
}

uint32_t DQLProcs::ExtProcContext::getTotalWorkerId() {
	EventContext *ec = getEventContext();
	if (ec->isOnIOWorker()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const uint32_t workerId = ec->getWorkerId();
	if (isOnTransactionService()) {
		return workerId;
	}
	else {
		TransactionManager *txnMgr = getResourceSet()->getTransactionManager();
		const uint32_t txnWorkerCount =
				txnMgr->getPartitionGroupConfig().getPartitionGroupCount();
		return txnWorkerCount + workerId;
	}
}

SQLContext& DQLProcs::ExtProcContext::getBase() {
	assert(baseCxt_ != NULL);
	return *baseCxt_;
}


DQLProcs::OptionInput::OptionInput(
		SQLValues::ValueContext &valueCxt, const ProcRegistrarTable &table,
		SQLType::Id type, const ProcInOption *procOption) :
		valueCxt_(valueCxt),
		table_(table),
		type_(type),
		procOption_(procOption),
		stream_(NULL) {
	setUpStream();
}

util::StackAllocator& DQLProcs::OptionInput::getAllocator() {
	return valueCxt_.getAllocator();
}

const DQLProcs::ProcRegistrarTable&
DQLProcs::OptionInput::getRegistrarTable() {
	return table_;
}

SQLType::Id DQLProcs::OptionInput::getType() {
	return type_;
}

template<typename T>
void DQLProcs::OptionInput::decode(T &target) {
	SQLExprs::ExprFactoryContext cxt(getAllocator());
	if (stream_ != NULL) {
		makeCoder(cxt).decode(*stream_, target);
	}
	else if (procOption_->byteInStream_ != NULL) {
		makeCoder(cxt).decode(*procOption_->byteInStream_, target);
	}
	else {
		assert(false);
	}
}

DQLProcs::ProcPlan::Node::Id DQLProcs::OptionInput::resolvePlanNodeId() const {
	do {
		const ProcPlan *plan = procOption_->plan_;
		if (plan == NULL) {
			break;
		}

		const ProcPlan::Node::Id id = procOption_->planNodeId_;
		if (id >= plan->nodeList_.size()) {
			break;
		}

		return id;
	}
	while (false);

	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
}

const DQLProcs::ProcPlan::Node&
DQLProcs::OptionInput::resolvePlanNode() const {
	const ProcPlan::Node::Id id = resolvePlanNodeId();
	return procOption_->plan_->nodeList_[id];
}

const DQLProcs::ProcPlan::Node* DQLProcs::OptionInput::findPlanNode() const {
	const ProcPlan *plan = procOption_->plan_;
	if (plan == NULL) {
		return NULL;
	}

	const ProcPlan::Node::Id id = resolvePlanNodeId();
	return &plan->nodeList_[id];
}

SQLExprs::SyntaxExprRewriter DQLProcs::OptionInput::createPlanRewriter() {
	assert(procOption_ != NULL);
	assert(procOption_->plan_ != NULL);
	return SQLExprs::SyntaxExprRewriter(
			valueCxt_, &procOption_->plan_->parameterList_);
}

void DQLProcs::OptionInput::setUpStream() {
	if (findPlanNode() != NULL || procOption_->byteInStream_ != NULL) {
		return;
	}

	util::AbstractObjectInStream *stream;
	if (procOption_->inStream_ != NULL) {
		stream = procOption_->inStream_;
	}
	else if (procOption_->jsonValue_ != NULL) {
		jsonStream_ = UTIL_MAKE_LOCAL_UNIQUE(
				jsonStream_, JsonStream, *procOption_->jsonValue_);
		stream = &jsonStream_->wrappedStream_;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}
	stream_ = stream;
}

DQLProcs::OptionInput::Coder DQLProcs::OptionInput::makeCoder(
		SQLExprs::ExprFactoryContext &cxt) {
	return Coder(
			util::ObjectCoder::withAllocator(getAllocator()),
			&cxt, NULL, &valueCxt_);
}

DQLProcs::OptionInput::JsonStream::JsonStream(const picojson::value &value) :
		stream_(value),
		objectScope_(stream_, util::ObjectCoder::Attribute()),
		wrappedStream_(objectScope_.stream()) {
}


DQLProcs::OptionOutput::OptionOutput(
		const ProcOutOption *procOption, SQLType::Id type) :
		procOption_(procOption),
		type_(type) {
}

util::StackAllocator& DQLProcs::OptionOutput::getAllocator() {
	ProcPlan *plan = procOption_->plan_;
	assert(plan != NULL);

	return plan->getAllocator();
}

SQLType::Id DQLProcs::OptionOutput::getType() {
	return type_;
}

template<typename T>
void DQLProcs::OptionOutput::encode(const T &target) {
	if (procOption_->outStream_ != NULL) {
		makeCoder().encode(*procOption_->outStream_ , target);
	}
	else if (procOption_->byteOutStream_ != NULL) {
		makeCoder().encode(*procOption_->byteOutStream_, target);
	}
	else {
		assert(false);
	}
}

DQLProcs::ProcPlan::Node& DQLProcs::OptionOutput::resolvePlanNode() {
	ProcPlan::Node *node = findPlanNode();
	if (node == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *node;
}

DQLProcs::ProcPlan::Node* DQLProcs::OptionOutput::findPlanNode() {
	ProcPlan *plan = procOption_->plan_;
	if (plan == NULL) {
		return NULL;
	}

	const ProcPlan::Node::Id id = procOption_->planNodeId_;
	util::StackAllocator &alloc = plan->getAllocator();

	while (id >= plan->nodeList_.size()) {
		plan->nodeList_.push_back(ProcPlan::Node(alloc));
	}

	return &plan->nodeList_[id];
}

SQLExprs::SyntaxExprRewriter DQLProcs::OptionOutput::createPlanRewriter() {
	if (valueCxt_.get() == NULL) {
		valueCxt_ = UTIL_MAKE_LOCAL_UNIQUE(
				valueCxt_, SQLValues::ValueContext,
				SQLValues::ValueContext::ofAllocator(getAllocator()));
	}
	return SQLExprs::SyntaxExprRewriter(*valueCxt_, NULL);
}

DQLProcs::OptionOutput::Coder DQLProcs::OptionOutput::makeCoder() {
	return Coder(
			util::ObjectCoder(), NULL,
			&SQLExprs::ExprFactory::getDefaultFactory(), NULL);
}


DQLProcs::Option::Option() :
		common_(NULL),
		specific_(NULL) {
}

void DQLProcs::Option::importFrom(OptionInput &in) {
	util::StackAllocator &alloc = in.getAllocator();

	common_ = ALLOC_NEW(alloc) Common(alloc);

	ProcRegistrar::SubOptionFactoryFunc factory =
			in.getRegistrarTable().resolve(in.getType()).getSubOptionFactory();
	specific_ = factory(alloc);

	common_->importFrom(in);
	specific_->importFrom(in);
}

void DQLProcs::Option::exportTo(OptionOutput &out) {
	assert(common_ != NULL && specific_ != NULL);

	common_->exportTo(out);
	specific_->exportTo(out);
}

SQLOps::OpCode DQLProcs::Option::toCode(
		SQLOps::OpCodeBuilder &builder) const {
	assert(common_ != NULL && specific_ != NULL);

	SQLOps::OpCode code;
	common_->toCode(builder, code);
	specific_->toCode(builder, code);

	if (code.getPipeProjection() == NULL) {
		code.setPipeProjection(&builder.createIdenticalProjection(
				SQLOps::OpCodeBuilder::isInputUnified(code), 0));
	}

	return code;
}


template<typename T>
DQLProcs::BasicSubOption<T>::BasicSubOption(util::StackAllocator &alloc) :
		optionValue_(alloc) {
}

template<typename T>
void DQLProcs::BasicSubOption<T>::importFrom(OptionInput &in) {
	if (in.findPlanNode() == NULL) {
		in.decode(optionValue_);
	}
	else {
		optionValue_.fromPlanNode(in);
	}
}

template<typename T>
void DQLProcs::BasicSubOption<T>::exportTo(OptionOutput &out) const {
	if (out.findPlanNode() == NULL) {
		out.encode(optionValue_);
	}
	else {
		optionValue_.toPlanNode(out);
	}
}

template<typename T>
void DQLProcs::BasicSubOption<T>::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	optionValue_.toCode(builder, code);
}


const int64_t DQLProcs::CommonOption::DEFAULT_TUPLE_LIMIT = -1;

DQLProcs::CommonOption::CommonOption(util::StackAllocator&) :
		id_(0),
		limit_(DEFAULT_TUPLE_LIMIT),
		phase_(SQLType::AGG_PHASE_ADVANCE_PIPE),
		outputInfo_(NULL) {
}

void DQLProcs::CommonOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();
	util::StackAllocator &alloc = in.getAllocator();

	id_ = in.resolvePlanNodeId();
	limit_ = node.limit_;
	phase_ = node.aggPhase_;

	outputInfo_ = ALLOC_NEW(alloc) TupleInfo(alloc);
	for (ProcPlan::Node::ExprList::const_iterator it = node.outputList_.begin();
			it != node.outputList_.end(); ++it) {
		outputInfo_->push_back(it->columnType_);
	}
}

void DQLProcs::CommonOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	node.limit_ = limit_;
	node.aggPhase_ = phase_;
	node.type_ = out.getType();
}

void DQLProcs::CommonOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	static_cast<void>(builder);
	code.setLimit(limit_);
	code.setAggregationPhase(phase_);
}


DQLProcs::GroupOption::GroupOption(util::StackAllocator &alloc) :
		columnList_(NULL),
		groupColumns_(alloc),
		nestLevel_(0) {
}

void DQLProcs::GroupOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();
	SQLExprs::SyntaxExprRewriter rewriter = in.createPlanRewriter();

	columnList_ = &rewriter.toProjection(node.outputList_);

	Expression &list = rewriter.toColumnList(node.predList_, true);
	for (Expression::Iterator it(list); it.exists(); it.next()) {
		groupColumns_.push_back(it.get().getCode().getColumnPos());
	}
}

void DQLProcs::GroupOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	static_cast<void>(node);
}

void DQLProcs::GroupOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	code.setType(SQLOpTypes::OP_GROUP);

	bool forWindow = false;
	code.setPipeProjection(&builder.toGroupingProjectionByExpr(
			columnList_, code.getAggregationPhase(), forWindow));

	SQLValues::CompColumnList &list = builder.createCompColumnList();
	for (ColumnPosList::const_iterator it = groupColumns_.begin();
			it != groupColumns_.end(); ++it) {
		SQLValues::CompColumn column;
		column.setColumnPos(*it, true);
		column.setOrdering(false);
		list.push_back(column);
	}
	code.setKeyColumnList(&list);
}


DQLProcs::JoinOption::JoinOption(util::StackAllocator &alloc) :
		columnList_(NULL),
		joinType_(SQLType::JoinType()),
		joinOp_(SQLType::START_EXPR),
		joinColumns_(alloc),
		pred_(NULL),
		joinCond_(NULL) {
}

void DQLProcs::JoinOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();
	SQLExprs::SyntaxExprRewriter rewriter = in.createPlanRewriter();

	columnList_ = &rewriter.toProjection(node.outputList_);
	joinType_ = node.joinType_;

	if (node.predList_.size() > 0) {
		Expression &list = rewriter.toColumnList(&node.predList_[0], false);
		for (Expression::Iterator it(list); it.exists(); it.next()) {
			const SQLExprs::ExprType subType = it.get().getCode().getType();
			if (subType == SQLType::EXPR_CONSTANT) {
				break;
			}
			joinOp_ = subType;

			const Expression &left = it.get().child();
			joinColumns_.push_back(left.getCode().getColumnPos());

			const Expression &right = left.next();
			joinColumns_.push_back(right.getCode().getColumnPos());
		}
	}

	if (node.predList_.size() > 1) {
		pred_ = rewriter.toExpr(&node.predList_[1]);
	}

	if (node.predList_.size() > 2) {
		joinCond_ = rewriter.toExpr(&node.predList_[2]);
	}
}

void DQLProcs::JoinOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	static_cast<void>(node);
}

void DQLProcs::JoinOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	code.setType(SQLOpTypes::OP_JOIN);

	builder.applyJoinType(joinType_);

	code.setPipeProjection(&builder.toNormalProjectionByExpr(
			columnList_, code.getAggregationPhase()));
	code.setJoinType(joinType_);

	SQLValues::CompColumnList &list = builder.createCompColumnList();
	for (ColumnPosList::const_iterator it = joinColumns_.begin();
			it != joinColumns_.end();) {
		const uint32_t pos1 = *it;
		if (++it == joinColumns_.end()) {
			assert(false);
			break;
		}

		const uint32_t pos2 = *it;
		const bool last = (++it == joinColumns_.end());

		list.push_back(SQLExprs::ExprTypeUtils::toCompColumn(
				joinOp_, pos1, pos2, last));
	}
	if (!list.empty()) {
		code.setKeyColumnList(&list);
	}

	code.setFilterPredicate(pred_);
	code.setJoinPredicate(joinCond_);
}


DQLProcs::LimitOption::LimitOption(util::StackAllocator&) :
		offset_(-1) {
}

void DQLProcs::LimitOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();

	offset_ = node.offset_;
}

void DQLProcs::LimitOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	node.offset_ = offset_;
}

void DQLProcs::LimitOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	static_cast<void>(builder);
	code.setType(SQLOpTypes::OP_LIMIT);
	code.setOffset(offset_);
}


DQLProcs::ScanOption::ScanOption(util::StackAllocator &alloc) :
		tableType_(SQLType::TABLE_CONTAINER),
		databaseVersionId_(-1),
		containerId_(std::numeric_limits<uint64_t>::max()),
		schemaVersionId_(std::numeric_limits<uint32_t>::max()),
		partitioningVersionId_(-1),
		columnList_(NULL),
		pred_(NULL),
		containerExpirable_(false),
		indexActivated_(false),
		multiIndexActivated_(false),
		indexList_(NULL) {
	static_cast<void>(alloc);
}

void DQLProcs::ScanOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();
	SQLExprs::SyntaxExprRewriter rewriter = in.createPlanRewriter();

	tableType_ = node.tableIdInfo_.type_;
	databaseVersionId_ = node.tableIdInfo_.dbId_;
	containerId_ = node.tableIdInfo_.containerId_;
	schemaVersionId_ = node.tableIdInfo_.schemaVersionId_;
	partitioningVersionId_ = node.tableIdInfo_.partitioningVersionId_;

	columnList_ = &rewriter.toProjection(node.outputList_);

	if (node.predList_.size() > 0) {
		pred_ = rewriter.toExpr(&node.predList_[0]);
	}

	containerExpirable_ = ((node.cmdOptionFlag_ &
			ProcPlan::Node::Config::CMD_OPT_SCAN_EXPIRABLE) != 0);
	indexActivated_ = ((node.cmdOptionFlag_ &
			ProcPlan::Node::Config::CMD_OPT_SCAN_INDEX) != 0);
	multiIndexActivated_ = ((node.cmdOptionFlag_ &
			ProcPlan::Node::Config::CMD_OPT_SCAN_MULTI_INDEX) != 0);

	if (node.indexInfoList_ != NULL && !node.indexInfoList_->empty()) {
		indexList_ = ALLOC_NEW(in.getAllocator()) util::Vector<int32_t>(
				node.indexInfoList_->begin(),
				node.indexInfoList_->end(),
				in.getAllocator());
	}
}

void DQLProcs::ScanOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	static_cast<void>(node);
}

void DQLProcs::ScanOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	code.setType(SQLOpTypes::OP_SCAN);

	SQLOps::ContainerLocation &location = builder.createContainerLocation();

	location.type_ = tableType_;
	location.dbVersionId_ = databaseVersionId_;
	location.id_ = containerId_;
	location.schemaVersionId_ = schemaVersionId_;
	location.partitioningVersionId_= partitioningVersionId_;
	location.schemaVersionSpecified_ = true;
	location.expirable_ = containerExpirable_;
	location.indexActivated_ = indexActivated_;
	location.multiIndexActivated_ = multiIndexActivated_;

	code.setContainerLocation(&location);

	code.setPipeProjection(&builder.toNormalProjectionByExpr(
			columnList_, code.getAggregationPhase()));
	code.setFilterPredicate(pred_);
}


DQLProcs::SelectOption::SelectOption(util::StackAllocator&) :
		output_(NULL) {
}

void DQLProcs::SelectOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();
	SQLExprs::SyntaxExprRewriter rewriter = in.createPlanRewriter();

	output_ = &rewriter.toSelection(
			node.outputList_,
			(node.predList_.empty() ? NULL : &node.predList_.front()));
}

void DQLProcs::SelectOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	static_cast<void>(node);
}

void DQLProcs::SelectOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	code.setType(SQLOpTypes::OP_SELECT);
	assert(output_ != NULL);

	code.setFilterPredicate(
			ExprRewriter::findPredicateBySelection(*output_));
	code.setPipeProjection(&builder.toNormalProjectionByExpr(
			&ExprRewriter::getProjectionBySelection(*output_),
			code.getAggregationPhase()));
}


DQLProcs::SortOption::SortOption(util::StackAllocator &alloc) :
		subOffset_(0),
		subLimit_(-1),
		orderColumns_(alloc),
		output_(NULL),
		forWindow_(false) {
}

void DQLProcs::SortOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();
	SQLExprs::SyntaxExprRewriter rewriter = in.createPlanRewriter();

	subOffset_ = node.subOffset_;
	subLimit_ = node.subLimit_;

	for (ProcPlan::Node::ExprList::const_iterator it = node.predList_.begin();
			it != node.predList_.end(); ++it) {
		SortColumn column;

		column.column_ = it->columnId_;
		column.order_ = (it->sortAscending_ ?
				SQLType::DIRECTION_ASC : SQLType::DIRECTION_DESC);

		orderColumns_.push_back(column);
	}

	output_ = &rewriter.toProjection(node.outputList_);
	forWindow_ = ((node.cmdOptionFlag_ &
			ProcPlan::Node::Config::CMD_OPT_WINDOW_SORTED) != 0);
}

void DQLProcs::SortOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	static_cast<void>(node);
}

void DQLProcs::SortOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	if (forWindow_) {
		code.setType(SQLOpTypes::OP_WINDOW);
	}
	else {
		code.setType(SQLOpTypes::OP_SORT);
	}

	if (subLimit_ >= 0) {
		code.setSubOffset((subOffset_ >= 0 ? subOffset_ : 0));
		code.setSubLimit(subLimit_);
	}

	SQLValues::CompColumnList &list = builder.createCompColumnList();
	for (SortColumnList::const_iterator it = orderColumns_.begin();
			it != orderColumns_.end(); ++it) {
		SQLValues::CompColumn column;
		column.setColumnPos(it->column_, true);
		column.setAscending((it->order_ == SQLType::DIRECTION_ASC));
		list.push_back(column);
	}
	code.setKeyColumnList(&list);

	if (forWindow_) {
		code.setPipeProjection(&builder.toGroupingProjectionByExpr(
				output_, code.getAggregationPhase(), forWindow_));
	}
	else {
		code.setPipeProjection(&builder.toNormalProjectionByExpr(
				output_, code.getAggregationPhase()));
	}
}


DQLProcs::UnionOption::UnionOption(util::StackAllocator&) :
		opType_(SQLType::END_UNION) {
}

void DQLProcs::UnionOption::fromPlanNode(OptionInput &in) {
	const ProcPlan::Node &node = in.resolvePlanNode();

	opType_ = node.unionType_;
}

void DQLProcs::UnionOption::toPlanNode(OptionOutput &out) const {
	ProcPlan::Node &node = out.resolvePlanNode();
	static_cast<void>(node);
}

void DQLProcs::UnionOption::toCode(
		SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const {
	static_cast<void>(builder);
	code.setType(SQLOpTypes::OP_UNION);
	code.setUnionType(opType_);
}


DQLProcs::SortColumn::SortColumn() :
		column_(std::numeric_limits<uint32_t>::max()),
		order_(SQLType::DIRECTION_ASC) {
}


SQLExprs::PlanPartitioningInfo*
DQLProcs::CompilerUtils::toPlanPartitioningInfo(
		const SQLTableInfo::PartitioningInfo *src,
		SQLExprs::PlanPartitioningInfo &dest) {
	if (src == NULL) {
		return NULL;
	}

	dest = toPlanPartitioningInfo(*src);
	return &dest;
}

SQLExprs::PlanPartitioningInfo DQLProcs::CompilerUtils::toPlanPartitioningInfo(
		const SQLTableInfo::PartitioningInfo &src) {
	SQLExprs::PlanPartitioningInfo dest;

	dest.partitioningType_ = static_cast<SyntaxTree::TablePartitionType>(
			src.partitioningType_);
	dest.partitioningColumnId_ = src.partitioningColumnId_;
	dest.subPartitioningColumnId_ = src.subPartitioningColumnId_;
	dest.partitioningCount_ = src.partitioningCount_;
	dest.clusterPartitionCount_ = src.clusterPartitionCount_;
	dest.intervalValue_ = src.intervalValue_;
	dest.nodeAffinityList_ = &src.nodeAffinityList_;
	dest.availableList_ = &src.availableList_;

	return dest;
}

SQLCompiler::NarrowingKey DQLProcs::CompilerUtils::toCompilerNarrowingKey(
		const SQLExprs::PlanNarrowingKey &src) {
	SQLCompiler::NarrowingKey dest;
	dest.setRange(src.longRange_);
	dest.setHash(src.hashIndex_, src.hashCount_);
	return dest;
}

void DQLProcs::CompilerUtils::applyCompileOption(
		SQLExprs::ExprContext &cxt,
		const SQLCompiler::CompileOption &option) {
	applyCompileOption(cxt.getValueContext(), option);
}

void DQLProcs::CompilerUtils::applyCompileOption(
		SQLValues::ValueContext &cxt,
		const SQLCompiler::CompileOption &option) {
	cxt.setTimeZone(&option.getTimeZone());
}


size_t SQLProcessor::ValueUtils::toString(
		int64_t value, char8_t *buf, size_t size) {
	return SQLVdbeUtils::VdbeUtils::numericToString(
			TupleValue(value), buf, size);
}

size_t SQLProcessor::ValueUtils::toString(
		double value, char8_t *buf, size_t size) {
	return SQLVdbeUtils::VdbeUtils::numericToString(
			TupleValue(value), buf, size);
}

util::String SQLProcessor::ValueUtils::toString(
		util::StackAllocator &alloc, int64_t value) {
	SQLValues::StringBuilder builder(alloc);
	SQLVdbeUtils::VdbeUtils::numericToString(builder, TupleValue(value));
	return util::String(builder.data(), builder.size(), alloc);
}

util::String SQLProcessor::ValueUtils::toString(
		util::StackAllocator &alloc, double value) {
	SQLValues::StringBuilder builder(alloc);
	SQLVdbeUtils::VdbeUtils::numericToString(builder, TupleValue(value));
	return util::String(builder.data(), builder.size(), alloc);
}

util::String SQLProcessor::ValueUtils::toString(
		util::StackAllocator &alloc, const TupleValue &value) {
	SQLValues::ValueContext cxt(SQLValues::ValueContext::ofAllocator(alloc));

	const util::TimeZone &zone = util::TimeZone::getUTCTimeZone();
	cxt.setTimeZone(&zone);

	const TupleString::BufferInfo &buf =
			TupleString(SQLValues::ValueUtils::toString(cxt, value)).getBuffer();
	return util::String(buf.first, buf.second, alloc);
}

bool SQLProcessor::ValueUtils::toLong(
		const char8_t *buf, size_t size, int64_t &result) {
	return SQLVdbeUtils::VdbeUtils::toLong(buf, size, result);
}

bool SQLProcessor::ValueUtils::toDouble(
		const char8_t *buf, size_t size, double &result) {
	return SQLVdbeUtils::VdbeUtils::toDouble(buf, size, result);
}

int32_t SQLProcessor::ValueUtils::strICmp(
		const char8_t *str1, const char8_t *str2) {
	return SQLVdbeUtils::VdbeUtils::strICmp(str1, str2);
}

void SQLProcessor::ValueUtils::toLower(char8_t *buf, size_t size) {
	SQLVdbeUtils::VdbeUtils::toLower(buf, size);
}

void SQLProcessor::ValueUtils::toUpper(char8_t *buf, size_t size) {
	SQLVdbeUtils::VdbeUtils::toUpper(buf, size);
}

uint32_t SQLProcessor::ValueUtils::hashValue(
		const TupleValue &value, const uint32_t *base) {
	const int64_t seed =
			(base == NULL ? SQLValues::ValueUtils::fnv1aHashInit() : *base);
	return SQLValues::ValueFnv1aHasher::ofValue(value, seed)(value);
}

uint32_t SQLProcessor::ValueUtils::hashString(
		const char8_t *value, const uint32_t *base) {
	return SQLValues::ValueUtils::fnv1aHashSequence(
			(base == NULL ? SQLValues::ValueUtils::fnv1aHashInit() : *base),
			*SQLValues::ValueUtils::toStringReader(value));
}

int32_t SQLProcessor::ValueUtils::orderValue(
		const TupleValue &value1, const TupleValue &value2, bool strict) {
	return SQLValues::ValueComparator::ofValues(
			value1, value2, strict, true)(value1, value2);
}

int32_t SQLProcessor::ValueUtils::orderString(
		const char8_t *value1, const char8_t *value2) {
	return SQLValues::ValueUtils::compareSequence(
			*SQLValues::ValueUtils::toStringReader(value1),
			*SQLValues::ValueUtils::toStringReader(value2));
}

bool SQLProcessor::ValueUtils::isTrueValue(const TupleValue &value) {
	return SQLValues::ValueUtils::isTrue(value);
}

int64_t SQLProcessor::ValueUtils::intervalToRaw(int64_t interval) {
	return SQLExprs::DataPartitionUtils::intervalToRaw(interval);
}

int64_t SQLProcessor::ValueUtils::intervalFromRaw(int64_t rawInterval) {
	return SQLExprs::DataPartitionUtils::intervalFromRaw(rawInterval);
}

int64_t SQLProcessor::ValueUtils::rawIntervalFromValue(
		const TupleValue &value, int64_t unit) {
	return SQLExprs::DataPartitionUtils::rawIntervalFromValue(value, unit);
}

int64_t SQLProcessor::ValueUtils::rawIntervalFromValue(
		int64_t longValue, int64_t unit) {
	return SQLExprs::DataPartitionUtils::rawIntervalFromValue(longValue, unit);
}

int64_t SQLProcessor::ValueUtils::intervalValueToLong(
		const TupleValue &value) {
	return SQLExprs::DataPartitionUtils::intervalValueToLong(value);
}

int64_t SQLProcessor::ValueUtils::intervalToAffinity(
		uint8_t partitioningType,
		uint32_t partitioningCount, uint32_t clusterPartitionCount,
		const util::Vector<int64_t> &affinityList,
		int64_t interval, uint32_t hash) {
	return SQLExprs::DataPartitionUtils::intervalToAffinity(
			static_cast<SyntaxTree::TablePartitionType>(partitioningType),
			partitioningCount, clusterPartitionCount,
			affinityList, interval, hash);
}


void SQLProcessor::DQLTool::decodeSimulationEntry(
		const picojson::value &value, SimulationEntry &entry) {
	static_cast<void>(value);
	static_cast<void>(entry);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

void SQLProcessor::DQLTool::encodeSimulationEntry(
		const SimulationEntry &entry, picojson::value &value) {
	static_cast<void>(entry);
	static_cast<void>(value);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

void SQLProcessor::DQLTool::getInterruptionProfile(picojson::value &value) {
	static_cast<void>(value);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

bool SQLProcessor::DQLTool::isDQL(SQLType::Id type) {
	return DQLProcessor::isDQL(type);
}

void SQLProcessor::DQLTool::customizeDefaultConfig(SQLProcessorConfig &config) {
	static_cast<void>(config);
}


bool SQLCompiler::ProcessorUtils::predicateToMetaTarget(
		TupleValue::VarContext &varCxt, const Expr &expr,
		uint32_t partitionIdColumn, uint32_t containerNameColumn,
		uint32_t containerIdColumn, PartitionId partitionCount,
		PartitionId &partitionId, const Plan::ValueList *parameterList,
		bool &placeholderAffected) {
	partitionId = UNDEF_PARTITIONID;
	placeholderAffected = false;

	SQLValues::ValueContext cxt(SQLValues::ValueContext::ofVarContext(varCxt));

	SQLExprs::Expression::InOption option(cxt);
	option.syntaxExpr_ = &expr;

	bool found = false;
	for (size_t i = 0; i < 2; i++) {
		const bool withParameterList = (i != 0);

		option.parameterList_ = (withParameterList ? parameterList : NULL);

		SQLExprs::ExprFactoryContext factoryCxt(cxt.getAllocator());
		SQLExprs::Expression &procExpr =
				SQLExprs::Expression::importFrom(factoryCxt, option);

		bool placeholderAffectedLocal;
		found = SQLContainerUtils::ContainerUtils::predicateToMetaTarget(
				cxt, &procExpr, partitionIdColumn, containerNameColumn,
				containerIdColumn, partitionCount, partitionId,
				placeholderAffectedLocal);

		if (!withParameterList) {
			placeholderAffected = placeholderAffectedLocal;
			if (parameterList == NULL) {
				break;
			}
		}
	}

	return found;
}

SQLCompiler::Expr SQLCompiler::ProcessorUtils::tqlToPredExpr(
		util::StackAllocator &alloc, const Query &query) {
	return SQLContainerUtils::ContainerUtils::tqlToPredExpr(alloc, query);
}

bool SQLCompiler::ProcessorUtils::reducePartitionedTarget(
		TupleValue::VarContext &varCxt, const TableInfo &tableInfo,
		const Expr &expr, bool &uncovered, util::Vector<int64_t> *affinityList,
		util::Vector<uint32_t> *subList,
		const Plan::ValueList *parameterList, bool &placeholderAffected) {
	typedef SQLTableInfo::SubInfoList SubInfoList;
	typedef SQLTableInfo::SQLColumnInfoList ColumnInfoList;

	assert(varCxt.getStackAllocator() != NULL);
	util::StackAllocator &alloc = *varCxt.getStackAllocator();
	SQLValues::ValueContext cxt(SQLValues::ValueContext::ofAllocator(alloc));

	const bool forContainer =
			(tableInfo.idInfo_.type_ == SQLType::TABLE_CONTAINER);

	SQLExprs::PlanPartitioningInfo basePartitioning;
	SQLExprs::PlanPartitioningInfo *partitioning =
			DQLProcs::CompilerUtils::toPlanPartitioningInfo(
					tableInfo.partitioning_, basePartitioning);

	const ColumnInfoList &columnInfoList = tableInfo.columnInfoList_;
	util::Vector<ColumnType> columnTypeList(alloc);
	for (ColumnInfoList::const_iterator it = columnInfoList.begin();
			it != columnInfoList.end(); ++it) {
		columnTypeList.push_back(it->first);
	}

	util::Set<int64_t> affinitySet(alloc);

	const bool reduced = SQLExprs::DataPartitionUtils::reducePartitionedTarget(
			cxt, forContainer, partitioning, columnTypeList, expr, uncovered,
			affinityList, affinitySet, parameterList, placeholderAffected);

	if (subList != NULL) {
		const SubInfoList &subInfoList = tableInfo.partitioning_->subInfoList_;
		for (SubInfoList::const_iterator it = subInfoList.begin();
				it != subInfoList.end(); ++it) {
			if (forContainer) {
				const int64_t affinity = it->nodeAffinity_;
				assert(affinity >= 0);

				if (affinitySet.find(affinity) == affinitySet.end()) {
					continue;
				}
			}
			const uint32_t index =
					static_cast<uint32_t>(it - subInfoList.begin());
			subList->push_back(index);
		}
	}

	return reduced;
}

bool SQLCompiler::ProcessorUtils::getTablePartitionKeyList(
		const TableInfo &tableInfo,
		const util::Vector<uint32_t> &affinityRevList,
		util::Vector<NarrowingKey> &keyList,
		util::Vector<NarrowingKey> &subKeyList) {
	typedef SQLTableInfo::SubInfoList SubInfoList;

	keyList.clear();
	subKeyList.clear();

	if (tableInfo.partitioning_ == NULL ||
			tableInfo.idInfo_.type_ != SQLType::TABLE_CONTAINER) {
		return false;
	}

	const SQLExprs::PlanPartitioningInfo &partitioning =
			DQLProcs::CompilerUtils::toPlanPartitioningInfo(
					*tableInfo.partitioning_);

	const SubInfoList &subInfoList = tableInfo.partitioning_->subInfoList_;
	for (SubInfoList::const_iterator it = subInfoList.begin();
			it != subInfoList.end(); ++it) {
		typedef DQLProcs::CompilerUtils CompilerUtils;

		SQLExprs::PlanNarrowingKey key;
		SQLExprs::PlanNarrowingKey subKey;
		bool subFound;
		SQLExprs::DataPartitionUtils::getTablePartitionKey(
				partitioning, affinityRevList, it->nodeAffinity_, key, subKey,
				subFound);

		keyList.push_back(CompilerUtils::toCompilerNarrowingKey(key));
		if (!subFound) {
			continue;
		}
		subKeyList.push_back(CompilerUtils::toCompilerNarrowingKey(subKey));
	}

	return true;
}

void SQLCompiler::ProcessorUtils::getTablePartitionAffinityRevList(
		const TableInfo &tableInfo, util::Vector<uint32_t> &affinityRevList) {
	affinityRevList.clear();

	if (tableInfo.partitioning_ == NULL ||
			tableInfo.idInfo_.type_ != SQLType::TABLE_CONTAINER) {
		return;
	}

	const SQLExprs::PlanPartitioningInfo &partitioning =
			DQLProcs::CompilerUtils::toPlanPartitioningInfo(
					*tableInfo.partitioning_);
	SQLExprs::DataPartitionUtils::makeAffinityRevList(
			partitioning, affinityRevList);
}

TupleValue SQLCompiler::ProcessorUtils::evalConstExpr(
		TupleValue::VarContext &varCxt, const Expr &expr,
		const CompileOption &option) {
	SQLExprs::ExprContext cxt(SQLValues::ValueContext::ofVarContext(varCxt));
	DQLProcs::CompilerUtils::applyCompileOption(cxt, option);

	return SQLExprs::SyntaxExprRewriter::evalConstExpr(
			cxt, SQLExprs::ExprFactory::getDefaultFactory(), expr);
}

bool SQLCompiler::ProcessorUtils::checkArgCount(
		Type exprType, size_t argCount, AggregationPhase phase) {
	const bool throwOnError = false;
	return SQLExprs::ExprRewriter::checkArgCount(
			SQLExprs::ExprFactory::getDefaultFactory(),
			exprType, argCount, phase, throwOnError);
}

bool SQLCompiler::ProcessorUtils::isConstEvaluable(Type exprType) {
	return SQLExprs::ExprRewriter::isConstEvaluable(
			SQLExprs::ExprFactory::getDefaultFactory(), exprType);
}

bool SQLCompiler::ProcessorUtils::isInternalFunction(Type exprType) {
	typedef SQLExprs::ExprSpec ExprSpec;
	const ExprSpec &spec =
			SQLExprs::ExprFactory::getDefaultFactory().getSpec(exprType);

	assert(
			SQLExprs::ExprTypeUtils::isAggregation(exprType) ||
			SQLExprs::ExprTypeUtils::isFunction(exprType));

	return ((spec.flags_ & ExprSpec::FLAG_INTERNAL) != 0);
}

bool SQLCompiler::ProcessorUtils::isExperimentalFunction(Type exprType) {
	typedef SQLExprs::ExprSpec ExprSpec;
	const ExprSpec &spec =
			SQLExprs::ExprFactory::getDefaultFactory().getSpec(exprType);

	assert(
			SQLExprs::ExprTypeUtils::isAggregation(exprType) ||
			SQLExprs::ExprTypeUtils::isFunction(exprType));

	return ((spec.flags_ & ExprSpec::FLAG_EXPERIMENTAL) != 0);
}

bool SQLCompiler::ProcessorUtils::isWindowExprType(
		Type type, bool &windowOnly, bool &pseudoWindow) {
	typedef SQLExprs::ExprSpec ExprSpec;
	const ExprSpec &spec =
			SQLExprs::ExprFactory::getDefaultFactory().getSpec(type);
	const int32_t flags = spec.flags_;

	const bool windowType = ((flags & (
			ExprSpec::FLAG_WINDOW |
			ExprSpec::FLAG_WINDOW_ONLY)) != 0);

	windowOnly = ((flags & ExprSpec::FLAG_WINDOW_ONLY) != 0);
	pseudoWindow = ((flags & ExprSpec::FLAG_PSEUDO_WINDOW) != 0);
	return windowType;
}

SQLCompiler::ColumnType SQLCompiler::ProcessorUtils::getResultType(
		Type exprType, size_t index, AggregationPhase phase,
		const util::Vector<ColumnType> &argTypeList, bool grouping) {
	SQLExprs::TypeResolver resolver(
			SQLExprs::ExprFactory::getDefaultFactory().getSpec(exprType),
			phase, grouping);

	for (util::Vector<ColumnType>::const_iterator it = argTypeList.begin();
			it != argTypeList.end(); ++it) {
		resolver.next(*it);
	}

	const bool checking = false;
	const SQLExprs::TypeResolver::ResultInfo &resultInfo =
			resolver.complete(checking);

	if (index >= SQLExprs::TypeResolver::RET_TYPE_LIST_SIZE) {
		assert(false);
		return TupleTypes::TYPE_NULL;
	}

	return resultInfo.typeList_[index];
}

size_t SQLCompiler::ProcessorUtils::getResultCount(
		Type exprType, AggregationPhase phase) {
	return SQLExprs::ExprRewriter::getResultCount(
			SQLExprs::ExprFactory::getDefaultFactory(), exprType, phase);
}

SQLCompiler::ColumnType SQLCompiler::ProcessorUtils::filterColumnType(
		ColumnType type) {
	return SQLValues::TypeUtils::filterColumnType(type);
}

bool SQLCompiler::ProcessorUtils::updateArgType(
		Type exprType, util::Vector<ColumnType> &argTypeList) {
	static_cast<void>(exprType);
	static_cast<void>(argTypeList);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

TupleValue SQLCompiler::ProcessorUtils::convertType(
		TupleValue::VarContext &varCxt, const TupleValue &src,
		ColumnType type, bool implicit) {
	const ColumnType srcType = SQLValues::ValueUtils::toColumnType(src);

	const bool anyAsNull = true;
	const ColumnType convType = SQLValues::TypeUtils::resolveConversionType(
			srcType, type, implicit, anyAsNull);

	SQLValues::ValueContext cxt(SQLValues::ValueContext::ofVarContext(varCxt));
	return SQLValues::ValueConverter(convType)(cxt, src);
}

SQLCompiler::ColumnType SQLCompiler::ProcessorUtils::findPromotionType(
		ColumnType type1, ColumnType type2) {
	const bool anyAsNull = true;
	return SQLValues::TypeUtils::findPromotionType(type1, type2, anyAsNull);
}

SQLCompiler::ColumnType SQLCompiler::ProcessorUtils::findConversionType(
		ColumnType src, ColumnType desired,
		bool implicit, bool evaluating) {
	const bool anyAsNull = true;
	return SQLValues::TypeUtils::findEvaluatingConversionType(
			src, desired, implicit, anyAsNull, evaluating);
}

SQLCompiler::ColumnType SQLCompiler::ProcessorUtils::getTableColumnType(
		const TableInfo &info, const ColumnId columnId,
		const bool *withNullsStatsRef) {
	const ColumnType type = info.columnInfoList_[columnId].first;
	const bool withNullsStats = (withNullsStatsRef == NULL ?
			isInputNullsStatsEnabled() : *withNullsStatsRef);

	if (!withNullsStats || info.idInfo_.type_ != SQLType::TABLE_CONTAINER) {
		return type;
	}

	const bool nullable = (!info.nullsStats_.empty() &&
			(info.nullsStats_[columnId / CHAR_BIT] &
			(1 << (columnId % CHAR_BIT))) != 0);

	return SQLValues::TypeUtils::setNullable(type, nullable);
}

bool SQLCompiler::ProcessorUtils::isInputNullsStatsEnabled() {
	return false;
}

bool SQLCompiler::ProcessorUtils::toTupleColumnType(
		uint8_t src, bool nullable, ColumnType &dest, bool failOnUnknown) {
	return SQLContainerUtils::ContainerUtils::toTupleColumnType(
			src, nullable, dest, failOnUnknown);
}

int32_t SQLCompiler::ProcessorUtils::toSQLColumnType(ColumnType type) {
	return SQLContainerUtils::ContainerUtils::toSQLColumnType(type);
}

SQLCompiler::Type SQLCompiler::ProcessorUtils::swapCompOp(Type type) {
	return SQLExprs::ExprTypeUtils::swapCompOp(type);
}

SQLCompiler::Type SQLCompiler::ProcessorUtils::negateCompOp(Type type) {
	return SQLExprs::ExprTypeUtils::negateCompOp(type);
}

SQLCompiler::Type SQLCompiler::ProcessorUtils::getLogicalOp(
		Type type, bool negative) {
	return SQLExprs::ExprTypeUtils::getLogicalOp(type, negative);
}
