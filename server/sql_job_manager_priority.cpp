/*
	Copyright (c) 2024 TOSHIBA Digital Solutions Corporation

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*!
	@file
	@brief 優先度制御を伴うジョブ制御機構の実装
*/

#include "sql_job_manager_priority.h"

#include "sql_service.h"
#include "sql_processor_ddl.h"
#include "sql_processor_result.h"
#include "transaction_service.h"
#include "key_data_store.h"
#include "chunk_buffer.h"
#include "database_manager.h"
#include "json.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(RESOURCE_MONITOR);
UTIL_TRACER_DECLARE(RESOURCE_MONITOR_PLAN);

UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK_DETAIL);
#define TRACE_TASK_EXECUTE(str, task, cxt, proc, inputId, controlType, count) \
	if (op != PriorityTask::OP_NONE) GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			task.getJob().getId() <<  \
			" " << procCxt->getExecutionManager()->getJobManager()->getHostName() << " " << \
			"TASK " << task.getId() << " " << str << " IN " << inputId << " " \
			<< SQLType::Coder()(task.getType()) \
			<< " " << task.getOperationTypeName(controlType) << " " << count);


PriorityJobConfig::PriorityJobConfig() :
		priorityControlActivated_(false),
		initialNodeIOCapacity_(5),
		minNodeIOCache_(2),
		initialTaskIOCapacity_(5),
		minTaskIOCache_(2),
		concurrency_(4),
		partitionCount_(KeyDataStore::MAX_PARTITION_NUM),
		txnConcurrency_(4),
		txnPartitionCount_(128),
		concurrencyPctOfPriorityUpdates_(200),
		storeMemoryLimit_(1024 * 1024 * 1024),
		workMemoryLimit_(32 * 1024 * 1024),
		totalMemoryLimit_(-1),
		workMemoryLimitRate_(4),
		workMemoryReservationRate_(2),
		storeMemoryReservationRate_(0.05),
		failOnTotalMemoryLimit_(false),
		workMemoryCache_(128 * 1024 * 1024),
		allocatorBlockExpSize_(20),
		outputValidationInterval_(15 * 1000),
		nodeValidationInterval_(3 * 60 * 1000),
		nodeValidationTimeout_(5 * 60 * 1000),
		traceInterval_(0),
		traceLimit_(0),
		networkTimeInterval_(0) {
}

uint32_t PriorityJobConfig::resolveBlockPoolSize() const {
	return static_cast<uint32_t>(
			(concurrency_ * 2 + txnConcurrency_) * minNodeIOCache_);
}


PriorityTaskConfig::PriorityTaskConfig(PriorityJobAllocator &alloc) :
		storeMemoryAgingSwapRate_(0),
		timeZone_(util::TimeZone()),
		startTimeRequired_(false),
		startTime_(0),
		startMonotonicTime_(0),
		forAdministrator_(false),
		forAnalysis_(false),
		forResultSet_(false),
		containerType_(UNDEF_CONTAINER),
		taskCount_(0),
		clientAddress_(alloc),
		clientPort_(0),
		dbId_(UNDEF_DBID),
		dbName_(alloc),
		userName_(alloc),
		applicationName_(alloc),
		statement_(alloc),
		statementTimeout_(-1) {
}

void PriorityTaskConfig::getStatProfile(
		StatJobProfilerInfo::Config &dest) const {
	dest.statementTimeout_ = statementTimeout_;
}

PriorityJobHolders::Pool::Pool(const PriorityJobConfig &config) :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_WORK, "priorityJobHoldersPool")),
		fixedAllocator_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_WORK, "priorityJobFixed"),
				1 << config.allocatorBlockExpSize_),
		mutexList_(alloc_),
		config_(config) {
	fixedAllocator_.setFreeElementLimit(static_cast<size_t>(
			config_.workMemoryCache_ / fixedAllocator_.getElementSize()));
}

PriorityJobHolders::Pool::~Pool() {
	clear();
}

PriorityJobAllocator& PriorityJobHolders::Pool::allocateAllocator(
		util::AllocatorLimitter *limitter, bool forTask) {
	util::LockGuard<util::Mutex> guard(lock_);

	const char8_t *name = (forTask ? "priorityTaskVar" : "priorityJobVar");
	util::AllocUniquePtr<PriorityJobAllocator> ptr(ALLOC_UNIQUE(
			alloc_, PriorityJobAllocator,
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, name)));
	if (limitter != NULL) {
		ptr->setLimit(util::AllocatorStats::STAT_GROUP_TOTAL_LIMIT, limitter);
	}
	return *ptr.release();
}

void PriorityJobHolders::Pool::releaseAllocator(PriorityJobAllocator *alloc) {
	util::LockGuard<util::Mutex> guard(lock_);

	util::AllocUniquePtr<PriorityJobAllocator> ptr(alloc, alloc_);
	ptr.reset();
}

util::StackAllocator& PriorityJobHolders::Pool::allocateStackAllocator(
		util::AllocatorLimitter *limitter) {
	util::LockGuard<util::Mutex> guard(lock_);

	util::AllocUniquePtr<util::StackAllocator> ptr(ALLOC_UNIQUE(
			alloc_, util::StackAllocator,
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, "priorityJobStack"),
			&fixedAllocator_));
	if (limitter != NULL) {
		ptr->setLimit(util::AllocatorStats::STAT_GROUP_TOTAL_LIMIT, limitter);
	}
	return *ptr.release();
}

void PriorityJobHolders::Pool::releaseStackAllocator(util::StackAllocator *alloc) {
	util::LockGuard<util::Mutex> guard(lock_);

	util::AllocUniquePtr<util::StackAllocator> ptr(alloc, alloc_);
	ptr.reset();
}

util::Mutex& PriorityJobHolders::Pool::allocateLock() {
	util::LockGuard<util::Mutex> guard(lock_);

	if (!mutexList_.empty()) {
		util::Mutex *mutex = mutexList_.back();
		mutexList_.pop_back();
		return *mutex;
	}

	util::AllocUniquePtr<util::Mutex> ptr(ALLOC_UNIQUE(alloc_, util::Mutex));
	return *ptr.release();
}

void PriorityJobHolders::Pool::releaseLock(util::Mutex *lock) {
	util::LockGuard<util::Mutex> guard(lock_);
	mutexList_.push_back(lock);
}

void PriorityJobHolders::Pool::clear() {
	while (!mutexList_.empty()) {
		util::AllocUniquePtr<util::Mutex> ptr(mutexList_.back(), alloc_);
		ptr.reset();
		mutexList_.pop_back();
	}
}


PriorityJobHolders::Allocator::Allocator(
		Pool &pool, util::AllocatorLimitter *limitter) :
		pool_(pool),
		alloc_(&pool_.allocateStackAllocator(limitter)) {
}

PriorityJobHolders::Allocator::~Allocator() {
	util::StackAllocator::Tool::forceReset(*alloc_);
	alloc_->setFreeSizeLimit(0);
	alloc_->trim();

	pool_.releaseStackAllocator(alloc_);
}

util::StackAllocator& PriorityJobHolders::Allocator::get() {
	return *alloc_;
}


PriorityJobHolders::VarAllocator::VarAllocator(
		Pool &pool, util::AllocatorLimitter *limitter, bool forTask) :
		pool_(pool),
		alloc_(&pool_.allocateAllocator(limitter, forTask)) {
}

PriorityJobHolders::VarAllocator::~VarAllocator() {
	pool_.releaseAllocator(alloc_);
}

PriorityJobAllocator& PriorityJobHolders::VarAllocator::get() {
	return *alloc_;
}


PriorityJobHolders::Lock::Lock(Pool &pool) :
		pool_(pool),
		lock_(&pool_.allocateLock()) {
}

PriorityJobHolders::Lock::~Lock() {
	pool_.releaseLock(lock_);
}

util::Mutex& PriorityJobHolders::Lock::get() {
	return *lock_;
}


JobBufferedBlockPool::JobBufferedBlockPool(
		LocalTempStore &store, uint32_t freeBlockCount) :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "jobBufferedBlockPool")),
		blockList_(alloc_),
		store_(store),
		freeBlockCount_(freeBlockCount) {
}

TupleList::Block JobBufferedBlockPool::takeBufferedBlock() {
	do {
		util::LockGuard<util::Mutex> guard(lock_);
		if (blockList_.empty()) {
			break;
		}
		TupleList::Block block = blockList_.back();
		blockList_.pop_back();
	}
	while (false);
	return allocateBufferedBlock();
}

void JobBufferedBlockPool::addBufferedBlock() {
	{
		util::LockGuard<util::Mutex> guard(lock_);
		if (blockList_.size() >= freeBlockCount_) {
			return;
		}
	}

	const TupleList::Block &block = allocateBufferedBlock();

	util::LockGuard<util::Mutex> guard(lock_);
	blockList_.push_back(block);
}

LocalTempStore& JobBufferedBlockPool::getStore() {
	return store_;
}

void JobBufferedBlockPool::resetPooledBlocks() {
	util::LockGuard<util::Mutex> guard(lock_);
	blockList_.clear();
}

TupleList::Block JobBufferedBlockPool::allocateBufferedBlock() {
	return TupleList::Block(
			store_, store_.getDefaultBlockSize(),
			LocalTempStore::UNDEF_GROUP_ID);
}


JobUnbufferedBlock::JobUnbufferedBlock(Block src) :
		store_(NULL),
		blockId_(LocalTempStore::UNDEF_BLOCKID) {
	BlockInfo *info = src.getBlockInfo();
	if (info != NULL) {
		store_ = &info->getStore();
		blockId_ = info->getBlockId();
		store_->getBufferManager().addAssignment(blockId_);
	}
}

JobUnbufferedBlock::~JobUnbufferedBlock() {
	clear();
}

JobUnbufferedBlock::JobUnbufferedBlock(const JobUnbufferedBlock &another) :
		store_(NULL),
		blockId_(LocalTempStore::UNDEF_BLOCKID) {
	(*this) = another;
}

JobUnbufferedBlock& JobUnbufferedBlock::operator=(
		const JobUnbufferedBlock &another) {
	if (this != &another) {
		assign(another.store_, another.blockId_);
	}
	return *this;
}

TupleList::Block JobUnbufferedBlock::toBufferedBlock() const {
	if (store_ == NULL) {
		return Block();
	}
	return Block(*store_, blockId_);
}

bool JobUnbufferedBlock::isEmpty() const {
	return (store_ == NULL);
}

void JobUnbufferedBlock::assign(LocalTempStore *store, BlockId blockId) {
	clear();

	if (store == NULL) {
		return;
	}

	store_ = store;
	blockId_ = blockId;
	store_->getBufferManager().addAssignment(blockId_);
}

void JobUnbufferedBlock::clear() {
	if (store_ != NULL) {
		store_->getBufferManager().removeAssignment(blockId_);
	}
	blockId_ = LocalTempStore::UNDEF_BLOCKID;
	store_ = NULL;
}


template<typename T>
bool PriorityJobUtils::pointerLessNullsFirst(const T *p1, const T *p2) {
	const bool null1 = (p1 == NULL);
	const bool null2 = (p2 == NULL);
	if (null1 || null2) {
		return (null1 && !null2);
	}
	else {
		return p1 < p2;
	}
}


PriorityJobStatValue::PriorityJobStatValue() :
		value_(0) {
}


int64_t PriorityJobStats::getValueAt(Type type) const {
	return valueList_[type].value_;
}

void PriorityJobStats::setValueAt(Type type, int64_t value) {
	valueList_[type].value_ = value;
}

void PriorityJobStats::setPeakValueAt(Type type, int64_t value) {
	setValueAt(type, std::max(getValueAt(type), value));
}

void PriorityJobStats::addValueAt(Type type, int64_t value) {
	valueList_[type].value_ += value;
}

void PriorityJobStats::subtract(const PriorityJobStats &base) {
	for (size_t i = 0; i < END_TYPE; i++) {
		valueList_[i].value_ -= base.valueList_[i].value_;
	}
}

void PriorityJobStats::add(const PriorityJobStats &base) {
	for (size_t i = 0; i < END_TYPE; i++) {
		valueList_[i].value_ += base.valueList_[i].value_;
	}
}


PriorityJobTracer::PriorityJobTracer() {
}

PriorityJobTracer::~PriorityJobTracer() {
}

void PriorityJobTracer::traceEventHandlingError(
		std::exception &e, const char8_t *situation) {
	UTIL_TRACE_EXCEPTION(
			SQL_SERVICE, e, GS_EXCEPTION_MESSAGE_ON(e, situation));
}

void PriorityJobTracer::traceValidationTimeout(
		const JobNode &node, int64_t currentTime, int64_t lastAliveTime,
		int64_t validationRequestTime, const PriorityJobConfig &config) {
	const int64_t aliveElapsed =
			(lastAliveTime <= 0 ? -1 : currentTime - lastAliveTime);
	const int64_t requestElapsed = currentTime - validationRequestTime;

	GS_TRACE_ERROR(
			SQL_SERVICE, GS_TRACE_SQL_NODE_TIMEOUT,
			"Node timeout detected for sql job messages ("
			"address=" << node.getInfo().address_ << ", "
			"lastAliveElapsed=" << aliveElapsed << ", "
			"validationRequestElapsed=" << requestElapsed << ", "
			"interval=" << config.nodeValidationInterval_ << ", "
			"timeout=" << config.nodeValidationTimeout_ << ")");
}

void PriorityJobTracer::traceResourceUse(
		PriorityJobContext &cxt, PriorityJobEnvironment &env, PriorityJob &job,
		const JobResourceInfo &info, bool withPlan, uint32_t planLimit) {
	GS_TRACE_WARNING(
			RESOURCE_MONITOR, GS_TRACE_SQL_TOO_MUCH_RESOURCE_USE,
			ResourceUseStatsFormatter(env, info));

	if (withPlan) {
		GS_TRACE_WARNING(
				RESOURCE_MONITOR_PLAN, GS_TRACE_SQL_TOO_MUCH_RESOURCE_USE,
				ResourceUsePlanFormatter(cxt, job, info, planLimit));
	}
}

void PriorityJobTracer::traceNodeOutputValidation(
		const JobNode &node, int64_t currentTime, int64_t sendingTime,
		int64_t validatingTime, bool validating, bool started, bool fixed) {
	const int64_t sendingElapsed =
			std::max<int64_t>(currentTime - sendingTime, 0);
	const int64_t validatingElapsed = (validating ?
			std::max<int64_t>(currentTime - validatingTime, 0) : -1);

	GS_TRACE_WARNING(
			SQL_SERVICE, GS_TRACE_CM_LONG_IO,
			"Too long sql block output for remote node ("
			"address=" << node.getInfo().address_ << ", "
			"sendingElapsed=" << sendingElapsed << ", "
			"validatingElapsed=" << validatingElapsed << ", "
			"validating=" << (validating ? "true" : "false") << ", "
			"operation=" << (started ? "START_VALIDATION" :
					(fixed ? "FIX_BY_VALIDATION" : "FINISH_WITH_DELAY")) << ")");
}


PriorityJobTracer::ResourceUsePlanInfo::ResourceUsePlanInfo(
		util::StackAllocator &alloc) :
		dbName_(alloc),
		requestId_(alloc),
		nodeAddress_(alloc),
		nodePort_(0),
		connectionAddress_(alloc),
		connectionPort_(0),
		userName_(alloc),
		applicationName_(alloc),
		statementType_(alloc),
		startTime_(alloc),
		actualTime_(0),
		memoryUse_(0),
		sqlStoreUse_(0),
		dataStoreAccess_(0),
		networkTransferSize_(0),
		networkTime_(0),
		availableConcurrency_(0),
		resourceRestrictions_(alloc),
		statement_(alloc) {
}

void PriorityJobTracer::ResourceUsePlanInfo::assign(
		const JobResourceInfo &baseInfo) {
	dbName_ = baseInfo.dbName_;
	requestId_ = baseInfo.requestId_;
	nodeAddress_ = baseInfo.nodeAddress_;
	nodePort_ = baseInfo.nodePort_;
	connectionAddress_ = baseInfo.connectionAddress_;
	connectionPort_ = baseInfo.connectionPort_;
	userName_ = baseInfo.userName_;
	applicationName_ = baseInfo.applicationName_;
	statementType_ = baseInfo.statementType_;
	startTime_ = formatTime(util::DateTime(baseInfo.startTime_)).c_str();
	statementType_ = baseInfo.statementType_;
	actualTime_ = baseInfo.actualTime_;
	memoryUse_ = baseInfo.memoryUse_;
	sqlStoreUse_ = baseInfo.sqlStoreUse_;
	dataStoreAccess_ = baseInfo.dataStoreAccess_;
	networkTransferSize_ = baseInfo.networkTransferSize_;
	networkTime_ = baseInfo.networkTime_;
	availableConcurrency_ = baseInfo.availableConcurrency_;
	resourceRestrictions_ = baseInfo.resourceRestrictions_;
	statement_ = baseInfo.statement_;
}

u8string PriorityJobTracer::ResourceUsePlanInfo::formatTime(
		const util::DateTime &value) {
	util::NormalOStringStream oss;
	oss << util::DateTime(value);
	return oss.str();
}


PriorityJobTracer::ResourceUseStatsFormatter::ResourceUseStatsFormatter(
		PriorityJobEnvironment &env, const JobResourceInfo &info) :
		env_(env),
		info_(info) {
}

std::ostream& PriorityJobTracer::ResourceUseStatsFormatter::format(
		std::ostream &os) const {
	os << "Too much resource use detected (";
	os << "requestId=" << info_.requestId_ << ", ";
	os << "nodeAddress=" <<
			info_.nodeAddress_ << ":" << info_.nodePort_ << ", ";
	os << "connectionAddress=" <<
			info_.connectionAddress_ << ":" << info_.connectionPort_ << ", ";
	os << "userName=" << info_.userName_ << ", ";
	os << "dbName=" << info_.dbName_ << ", ";
	os << "applicationName=" << info_.applicationName_ << ", ";
	os << "statementType=" << info_.statementType_ << ", ";
	os << "startTime=" << util::DateTime(info_.startTime_) << ", ";
	os << "actualTime=" << info_.actualTime_ << ", ";
	os << "memoryUse=" << info_.memoryUse_ << ", ";
	os << "sqlStoreUse=" << info_.sqlStoreUse_ << ", ";
	os << "dataStoreAccess=" << info_.dataStoreAccess_ << ", ";
	os << "networkTransferSize=" << info_.networkTransferSize_ << ", ";
	os << "networkTime=" << info_.networkTime_ << ", ";
	os << "availableConcurrency=" << info_.availableConcurrency_ << ", ";
	os << "resourceRestrictions=" << info_.resourceRestrictions_ << ", ";
	os << "statement=" <<
			env_.getJobManager()->getSQLService()->getTraceLimitQueryFormatter(
					info_.statement_.c_str()) << ")";

	return os;
}

std::ostream& operator<<(
		std::ostream &os,
		const PriorityJobTracer::ResourceUseStatsFormatter &formatter) {
	return formatter.format(os);
}


PriorityJobTracer::ResourceUsePlanFormatter::ResourceUsePlanFormatter(
		PriorityJobContext &cxt, PriorityJob &job,
		const JobResourceInfo &info, uint32_t planLimit) :
		cxt_(cxt),
		job_(job),
		info_(info),
		planLimit_(planLimit) {
}

std::ostream& PriorityJobTracer::ResourceUsePlanFormatter::format(
		std::ostream &os) const {
	util::StackAllocator &alloc = cxt_.getAllocator();
	PriorityJobAllocator &varAlloc = cxt_.getVarAllocator();

	ResourceUsePlanInfo planInfo(alloc);
	planInfo.assign(info_);

	picojson::value jsonOutValue;
	JsonUtils::OutStream out(jsonOutValue);
	util::ObjectCoder().encode(out, planInfo);

	picojson::value &plan = jsonOutValue.get<picojson::object>()["plan"];
	plan = picojson::value(picojson::array());

	picojson::array &planArray = plan.get<picojson::array>();

	const TaskId count = job_.getTaskCount();
	const size_t taskPlanLimit = planLimit_ / static_cast<size_t>(count);

	for (TaskId i = 0; i < count; i++) {
		planArray.push_back(picojson::value());
		picojson::value &plan = planArray.back();
		try {
			PriorityTask task;
			if (job_.findTask(i, true, task)) {
				task.getPlanJson(
						alloc, varAlloc, plan, true, &taskPlanLimit);
			}
		}
		catch (...) {
		}
	}

	os << jsonOutValue.serialize().c_str();

	return os;
}

std::ostream& operator<<(
		std::ostream &os,
		const PriorityJobTracer::ResourceUsePlanFormatter &formatter) {
	return formatter.format(os);
}


PriorityJobInfo::PriorityJobInfo() :
		base_(NULL),
		nodeList_(NULL),
		onCoordinator_(false),
		startTimeRequired_(false),
		forAdministrator_(false),
		forAnalysis_(false),
		forResultSet_(false),
		containerType_(UNDEF_CONTAINER),
		clientAddress_(NULL),
		clientPort_(0),
		dbId_(UNDEF_DBID),
		dbName_(NULL),
		userName_(NULL),
		applicationName_(NULL),
		statement_(NULL),
		startTime_(0),
		statementTimeout_(-1),
		processorData_(NULL) {
}

PriorityJobInfo* PriorityJobInfo::ofBase(
		util::StackAllocator &alloc, const Base *base,
		SQLExecution &execution) {
	PriorityJobInfo *info = ALLOC_NEW(alloc) PriorityJobInfo();
	info->base_ = base;
	info->nodeList_ = makeNodeList(alloc, base);
	info->onCoordinator_ = base->coordinator_;

	info->startTimeRequired_ = base->isSetJobTime_;
	info->forAdministrator_ = base->isAdministrator_;
	info->forAnalysis_ = base->isExplainAnalyze_;
	info->forResultSet_ = base->isSQL_;
	info->containerType_ = base->containerType_;

	resolveClientAddress(
			alloc, execution, info->clientAddress_, info->clientPort_);
	info->dbId_ = base->dbId_;
	info->dbName_ = makeString(alloc, base->dbName_);
	info->userName_ = makeString(alloc, base->userName_);
	info->applicationName_ = makeString(alloc, base->appName_);
	info->statement_ = makeString(alloc, execution.getContext().getQuery());
	info->startTime_ = execution.getContext().getStartTime();
	info->statementTimeout_ = base->resolveStatementTimeout();

	info->processorData_ = (base == NULL ? NULL : &base->outBuffer_);
	return info;
}

void PriorityJobInfo::resolveClientAddress(
	util::StackAllocator& alloc, SQLExecution& execution,
	const util::String*& address, uint16_t& port) {

	std::string addressStr;
	execution.getContext().getClientAddress(addressStr, port);
	address = makeString(alloc, addressStr.c_str());
}

util::String* PriorityJobInfo::makeString(
		util::StackAllocator &alloc, const char8_t *src) {
	if (src == NULL) {
		return NULL;
	}

	return ALLOC_NEW(alloc) util::String(src, alloc);
}

util::AllocString PriorityJobInfo::makeString(
		PriorityJobAllocator &alloc, const util::String *src) {
	util::AllocString dest(alloc);
	if (src != NULL) {
		dest = src->c_str();
	}
	return dest;
}

util::Vector<PriorityJobInfo::Node>* PriorityJobInfo::makeNodeList(
		util::StackAllocator &alloc, const Base *base) {
	if (base == NULL) {
		return NULL;
	}

	const SQLPreparedPlan *plan = base->option_.plan_;
	if (plan == NULL) {
		return NULL;
	}

	util::Vector<Node> *dest = ALLOC_NEW(alloc) util::Vector<Node>(alloc);
	const SQLPreparedPlan::NodeList &src = plan->nodeList_;
	for (SQLPreparedPlan::NodeList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		dest->push_back(Node());
		Node &node = dest->back();
		node.type_ = it->type_;
	}

	return dest;
}


PriorityJobInfo::Node::Node() :
		type_(SQLType::START_EXEC) {
}


JobMessages::CompatibleHeader::CompatibleHeader() :
		priorityControlActivated_(false) {
}

void JobMessages::CompatibleHeader::encode(ByteOutStream &out) const {
	JobId jobId;
	JobManager::ControlType controlType = JobManager::FW_CONTROL_UNDEF;
	SQLJobHandler::encodeRequestInfo(out, jobId, controlType);

	Utils::encodeBool(out, priorityControlActivated_);
}

void JobMessages::CompatibleHeader::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	UNUSED_VARIABLE(cxt);

	JobId jobId;
	JobManager::ControlType controlType;
	int32_t sqlVersionId;
	SQLJobHandler::decodeRequestInfo(in, jobId, controlType, sqlVersionId);

	if (in.base().remaining() > 0) {
		priorityControlActivated_ = Utils::decodeBool(in);
	}
}


JobMessages::Header::Header() :
		compatible_(false),
		messageId_(0) {
	memset(uuid_, 0, sizeof(UUIDValue));
}

void JobMessages::Header::encode(ByteOutStream &out) const {
	if (compatible_) {
		compatiblePart_.encode(out);
	}

	out << std::pair<const uint8_t*, size_t>(uuid_, sizeof(UUIDValue));
	out << messageId_;
}

void JobMessages::Header::decode(PriorityJobContext &cxt, ByteInStream &in) {
	if (compatible_) {
		compatiblePart_.decode(cxt, in);

		if (!compatiblePart_.priorityControlActivated_) {
			return;
		}
	}

	in.readAll(uuid_, sizeof(UUIDValue));
	in >> messageId_;
}


JobMessages::Base::Base() {
}

void JobMessages::Base::encode(ByteOutStream &out) const {
	out << std::pair<const uint8_t*, size_t>(
			jobId_.clientId_.uuid_, sizeof(UUIDValue));
	out << jobId_.clientId_.sessionId_;
	out << jobId_.execId_;
	out << jobId_.versionId_;
}

void JobMessages::Base::decode(PriorityJobContext &cxt, ByteInStream &in) {
	UNUSED_VARIABLE(cxt);

	in.readAll(jobId_.clientId_.uuid_, sizeof(UUIDValue));
	in >> jobId_.clientId_.sessionId_;
	in >> jobId_.execId_;
	in >> jobId_.versionId_;
}


JobMessages::TaskIOInfo::TaskIOInfo() :
		inputCount_(0),
		outputCount_(0) {
}

void JobMessages::TaskIOInfo::encode(ByteOutStream &out) const {
	out << inputCount_;
	out << outputCount_;
}

void JobMessages::TaskIOInfo::decode(PriorityJobContext &cxt, ByteInStream &in) {
	UNUSED_VARIABLE(cxt);

	in >> inputCount_;
	in >> outputCount_;
}


JobMessages::Deploy::Deploy() :
		jobInfo_(NULL) {
}

void JobMessages::Deploy::encode(ByteOutStream &out) const {
	base_.encode(out);
	util::ObjectCoder().encode(out, jobInfo_);

	encodeBinaryData(out, jobInfo_->processorData_);
}

void JobMessages::Deploy::decode(PriorityJobContext &cxt, ByteInStream &in) {
	util::StackAllocator &alloc = cxt.getAllocator();

	base_.decode(cxt, in);

	PriorityJobInfo *info = NULL;
	util::ObjectCoder().withAllocator(alloc).decode(in, info);
	info->processorData_ = &decodeBinaryData(in, alloc);

	jobInfo_ = info;
}

void JobMessages::Deploy::encodeBinaryData(
		ByteOutStream &out, const util::XArray<uint8_t> *data) {
	const uint32_t dataSize =
			static_cast<uint32_t>(data == NULL ? 0 : data->size());
	const uint8_t *addr = (data == NULL ? NULL : data->data());

	out << dataSize;
	out << std::make_pair(addr, dataSize);
}

util::XArray<uint8_t>& JobMessages::Deploy::decodeBinaryData(
		ByteInStream &in, util::StackAllocator &alloc) {
	uint32_t dataSize;
	in >> dataSize;

	if (dataSize > in.base().remaining()) {
		GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid data size");
	}

	util::XArray<uint8_t> *data = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
	data->resize(dataSize);
	in >> std::make_pair(data->data(), dataSize);

	return *data;
}


JobMessages::ReadyOutput::ReadyOutput() :
		taskId_(0) {
}

void JobMessages::ReadyOutput::encode(ByteOutStream &out) const {
	base_.encode(out);
	out << taskId_;
}

void JobMessages::ReadyOutput::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);
	in >> taskId_;
}


JobMessages::Pipe::Pipe() :
		taskId_(0),
		input_(0),
		encodedBlockSize_(NULL) {
}

void JobMessages::Pipe::encode(ByteOutStream &out) const {
	base_.encode(out);
	out << taskId_;
	out << input_;
	ioInfo_.encode(out);

	const size_t startPos = out.base().position();
	TupleList::Block block = block_;
	block.encode(out);
	const size_t endPos = out.base().position();

	if (encodedBlockSize_ != NULL) {
		*encodedBlockSize_ += (endPos - startPos);
	}
}

void JobMessages::Pipe::decode(PriorityJobContext &cxt, ByteInStream &in) {
	util::StackAllocator &alloc = cxt.getAllocator();

	base_.decode(cxt, in);
	in >> taskId_;
	in >> input_;
	ioInfo_.decode(cxt, in);

	JobBufferedBlockPool &pool = cxt.getBlockPool();
	block_ = pool.takeBufferedBlock();
	block_.decode(alloc, in);
}


JobMessages::Finish::Finish() :
		taskId_(0),
		input_(0) {
}

void JobMessages::Finish::encode(ByteOutStream &out) const {
	base_.encode(out);
	out << taskId_;
	out << input_;
	ioInfo_.encode(out);
}

void JobMessages::Finish::decode(PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);
	in >> taskId_;
	in >> input_;
	ioInfo_.decode(cxt, in);
}


JobMessages::Cancel::Cancel() {
}

void JobMessages::Cancel::encode(ByteOutStream &out) const {
	base_.encode(out);

	const bool withException = (exception_.get() != NULL);
	out << static_cast<uint8_t>(withException);

	if (withException) {
		try {
			throw *exception_;
		}
		catch (...) {
			std::exception e;
			StatementHandler::encodeException(out, e, 0);
		}
	}
}

void JobMessages::Cancel::decode(PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);

	uint8_t withException;
	in >> withException;

	if (withException != 0) {
		util::StackAllocator &alloc = cxt.getAllocator();
		exception_ = UTIL_MAKE_LOCAL_UNIQUE(exception_, util::Exception);
		SQLErrorUtils::decodeException(alloc, in, *exception_);
	}
}


JobMessages::Complete::Complete() :
		profilerInfo_(NULL) {
}

void JobMessages::Complete::encode(ByteOutStream &out) const {
	base_.encode(out);
	util::ObjectCoder().encode(out, profilerInfo_);
}

void JobMessages::Complete::decode(PriorityJobContext &cxt, ByteInStream &in) {
	util::StackAllocator &alloc = cxt.getAllocator();

	base_.decode(cxt, in);
	util::ObjectCoder().withAllocator(alloc).decode(in, profilerInfo_);
}


JobMessages::ChangeTaskPriority::ChangeTaskPriority() :
		taskId_(0),
		priorInput_(0) {
}

void JobMessages::ChangeTaskPriority::encode(ByteOutStream &out) const {
	base_.encode(out);
	out << taskId_;
	out << priorInput_;
}

void JobMessages::ChangeTaskPriority::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);
	in >> taskId_;
	in >> priorInput_;
}


JobMessages::ApplyTaskPathPriority::ApplyTaskPathPriority() :
		priorityList_(NULL),
		checking_(false) {
}

void JobMessages::ApplyTaskPathPriority::encode(ByteOutStream &out) const {
	base_.encode(out);

	const uint32_t size =
			static_cast<uint32_t>(priorityList_ == NULL ? 0 : priorityList_->size());
	out << size;

	for (uint32_t i = 0; i < size; i++) {
		const TaskPriority &priority = (*priorityList_)[i];
		out << priority.first;
		out << priority.second;
	}

	Utils::encodeBool(out, checking_);
}

void JobMessages::ApplyTaskPathPriority::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);

	uint32_t size;
	in >> size;

	util::StackAllocator &alloc = cxt.getAllocator();
	TaskPriorityList *priorityList = ALLOC_NEW(alloc) TaskPriorityList(alloc);
	for (uint32_t i = 0; i < size; i++) {
		TaskPriority priority;
		in >> priority.first;
		in >> priority.second;
		priorityList->push_back(priority);
	}
	priorityList_ = priorityList;

	checking_ = Utils::decodeBool(in);
}


JobMessages::GrowTaskOutput::GrowTaskOutput() :
		taskId_(0),
		output_(0),
		capacity_(0) {
}

void JobMessages::GrowTaskOutput::encode(ByteOutStream &out) const {
	base_.encode(out);
	out << taskId_;
	out << output_;
	ioInfo_.encode(out);
	out << capacity_;
}

void JobMessages::GrowTaskOutput::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);
	in >> taskId_;
	in >> output_;
	ioInfo_.decode(cxt, in);
	in >> capacity_;
}


JobMessages::GrowNodeOutput::GrowNodeOutput() :
		capacity_(0) {
}

void JobMessages::GrowNodeOutput::encode(ByteOutStream &out) const {
	out << capacity_;
}

void JobMessages::GrowNodeOutput::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	UNUSED_VARIABLE(cxt);
	in >> capacity_;
}


JobMessages::ReportSent::ReportSent() :
		taskId_(0),
		networkBusy_(false),
		sequenceNum_(0),
		size_(0) {
}

void JobMessages::ReportSent::encode(ByteOutStream &out) const {
	base_.encode(out);
	out << taskId_;
	Utils::encodeBool(out, networkBusy_);
	out << sequenceNum_;
	out << size_;
}

void JobMessages::ReportSent::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	base_.decode(cxt, in);
	in >> taskId_;
	networkBusy_ = Utils::decodeBool(in);
	in >> sequenceNum_;
	in >> size_;
}


JobMessages::ValidateNode::ValidateNode() :
		ackRequired_(false),
		jobActivityList_(NULL),
		validationCode_(0) {
}

void JobMessages::ValidateNode::encode(ByteOutStream &out) const {
	Utils::encodeBool(out, ackRequired_);

	const bool withList = (jobActivityList_ != NULL);
	Utils::encodeBool(out, withList);

	if (withList) {
		const uint32_t size = static_cast<uint32_t>(jobActivityList_->size());
		out << size;

		for (uint32_t i = 0; i < size; i++) {
			const JobActivityEntry &entry = (*jobActivityList_)[i];

			Base base;
			base.jobId_ = entry.first;
			base.encode(out);

			Utils::encodeBool(out, entry.second);
		}
	}

	out << validationCode_;
}

void JobMessages::ValidateNode::decode(
		PriorityJobContext &cxt, ByteInStream &in) {
	UNUSED_VARIABLE(cxt);

	ackRequired_ = Utils::decodeBool(in);
	JobActivityList *activityList = NULL;

	const bool withList = Utils::decodeBool(in);
	if (withList) {
		util::StackAllocator &alloc = cxt.getAllocator();

		uint32_t size;
		in >> size;

		activityList = ALLOC_NEW(alloc) JobActivityList(alloc);
		for (uint32_t i = 0; i < size; i++) {
			Base base;
			base.decode(cxt, in);

			JobActivityEntry entry;
			entry.first = base.jobId_;
			entry.second = Utils::decodeBool(in);

			activityList->push_back(entry);
		}
	}
	jobActivityList_ = activityList;

	in >> validationCode_;
}


template<typename M>
bool JobMessages::Utils::isHeaderCompatible() {
	return (util::IsSame<M, Deploy>::VALUE);
}

template<typename M>
JobMessages::Type JobMessages::Utils::getEventType() {
	const Type type = (
			util::IsSame<M, Deploy>::VALUE ?
					PRIORITY_JOB_DEPLOY :
			util::IsSame<M, ReadyOutput>::VALUE ?
					PRIORITY_JOB_READY_OUTPUT :
			util::IsSame<M, Pipe>::VALUE ?
					PRIORITY_JOB_PIPE :
			util::IsSame<M, Finish>::VALUE ?
					PRIORITY_JOB_FINISH :
			util::IsSame<M, Cancel>::VALUE ?
					PRIORITY_JOB_CANCEL :
			util::IsSame<M, Complete>::VALUE ?
					PRIORITY_JOB_COMPLETE :
			util::IsSame<M, ChangeTaskPriority>::VALUE ?
					PRIORITY_JOB_CHANGE_JOB_PRIORITY :
			util::IsSame<M, ApplyTaskPathPriority>::VALUE ?
					PRIORITY_JOB_APPLY_TASK_PRIORITY :
			util::IsSame<M, GrowTaskOutput>::VALUE ?
					PRIORITY_JOB_GROW_TASK_OUTPUT :
			util::IsSame<M, GrowNodeOutput>::VALUE ?
					PRIORITY_JOB_GROW_NODE_OUTPUT :
			util::IsSame<M, ReportSent>::VALUE ?
					PRIORITY_JOB_REPORT_SENT :
			util::IsSame<M, ValidateNode>::VALUE ?
					PRIORITY_JOB_VALIDATE_NODE : -1);
	UTIL_STATIC_ASSERT((type > 0));
	return type;
}

void JobMessages::Utils::encodeBool(ByteOutStream &out, bool value) {
	const uint8_t intValue = (value ? 1 : 0);
	out << intValue;
}

bool JobMessages::Utils::decodeBool(ByteInStream &in) {
	uint8_t intValue;
	in >> intValue;
	return !!intValue;
}


JobNode::JobNode(const JobNodeBodyBase *base) :
		body_(NULL) {
	if (base != NULL) {
		const Source &source = base->getSource();
		{
			util::LockGuard<util::Mutex> guard(source.totalLock_);
			body_ = ALLOC_NEW(source.alloc_) Body(source);
		}
		addReference();
	}
}

JobNode::~JobNode() {
	removeReference();
}

JobNode::JobNode(const JobNode &another) :
		body_(NULL) {
	*this = another;
}

JobNode& JobNode::operator=(const JobNode &another) {
	if (this != &another) {
		removeReference();
		another.addReference();
		body_ = another.body_;
	}
	return *this;
}

bool JobNode::operator==(const JobNode &another) const {
	return (body_ == another.body_);
}

bool JobNode::operator<(const JobNode &another) const {
	return PriorityJobUtils::pointerLessNullsFirst(body_, another.body_);
}

const JobNode::Info& JobNode::getInfo() const {
	return getBody().info_;
}

const JobNode::Info& JobNode::getInfoOptional() const {
	if (isEmpty()) {
		return Info::emptyInfo();
	}
	return getInfo();
}

bool JobNode::isEmpty() const {
	return (body_ == NULL);
}

bool JobNode::isSelf() const {
	return getBody().info_.self_;
}

bool JobNode::isClosed() const {
	return getBody().closed_;
}

void JobNode::close() {
	getBody().closed_ = true;
}

bool JobNode::acceptBlockInput(uint64_t &nextCapacity) {
	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.nodeLock_);

	body.inputSize_++;
	return updateInputCapacity(body, nextCapacity);
}

void JobNode::acceptBlockOutput() {
	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.nodeLock_);

	body.outputSize_++;
}

void JobNode::growOutputCapacity(uint64_t minCapacity) {
	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.nodeLock_);

	body.outputCapacity_ = std::max(minCapacity, body.outputCapacity_);
}

bool JobNode::isOutputRestricted() {
	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.nodeLock_);

	return body.outputSize_ >= body.outputCapacity_;
}

void JobNode::getStatProfileOptional(
		StatWorkerProfilerInfo::NodeEntry &dest) const {
	if (body_ == NULL) {
		return;
	}

	Body &body = *body_;
	body.info_.getAddressString(dest.address_, false);

	util::LockGuard<util::Mutex> guard(body.nodeLock_);
	dest.inputSize_ = body.inputSize_;
	dest.inputCapacity_ = body.inputCapacity_;
	dest.outputSize_ = body.outputSize_;
	dest.outputCapacity_ = body.outputCapacity_;
}

void JobNode::addReference() const {
	if (body_ != NULL) {
		++body_->refCount_;
	}
}

void JobNode::removeReference() {
	if (body_ != NULL) {
		if (--body_->refCount_ == 0) {
			util::LockGuard<util::Mutex> guard(body_->totalLock_);
			ALLOC_DELETE(body_->alloc_, body_);
		}
		body_ = NULL;
	}
}

JobNode::Body& JobNode::getBody() {
	if (body_ == NULL) {
		return errorEmptyBody();
	}
	return *body_;
}

const JobNode::Body& JobNode::getBody() const {
	if (body_ == NULL) {
		return errorEmptyBody();
	}
	return *body_;
}

bool JobNode::updateInputCapacity(Body &body, uint64_t &nextCapacity) {
	bool updated;
	do {
		const uint64_t desired = body.inputSize_ + body.minIOCache_;
		if (desired < body.inputCapacity_) {
			updated = false;
			break;
		}
		body.inputCapacity_ = body.inputSize_ + body.initialIOCapacity_;
		updated = true;
	}
	while (false);

	nextCapacity = body.inputCapacity_;
	return updated;
}

JobNode::Body& JobNode::errorEmptyBody() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Empty node body");
}


JobNode::Info JobNode::Info::EMPTY_INFO(
		-1, util::SocketAddress(), NodeDescriptor(), false);

JobNode::Info::Info(
		NodeDescriptorId id, const util::SocketAddress &address,
		const NodeDescriptor &nd, bool self) :
		id_(id),
		address_(address),
		nd_(nd),
		self_(self) {
}

util::String* JobNode::Info::toAddressString(
		util::StackAllocator &alloc) const {
	if (address_.isEmpty()) {
		return NULL;
	}

	util::String *str = ALLOC_NEW(alloc) util::String(alloc);
	getAddressString(*str, false);
	return str;
}

void JobNode::Info::getAddressString(util::String &str, bool appending) const {
	if (!appending) {
		str.clear();
	}

	if (address_.isEmpty()) {
		return;
	}

	util::StackAllocator *alloc = str.get_allocator().base();
	getHostString(str);
	str += ":";
	str += util::TinyLexicalIntConverter().toString<util::String>(
			address_.getPort(), *alloc);
}

const JobNode::Info& JobNode::Info::emptyInfo() {
	return EMPTY_INFO;
}

void JobNode::Info::getHostString(util::String &str) const {
	getHostString(address_, str);
}

void JobNode::Info::getHostString(
		const util::SocketAddress &address, util::String &str) {
	u8string base;
	address.getIP(&base, NULL);
	str = base.c_str();
}

uint16_t JobNode::Info::getPort() const {
	return address_.getPort();
}


JobNode::Source::Source(
		const Info &info, const PriorityJobConfig &config,
		PriorityJobAllocator &alloc, util::Mutex &totalLock) :
		info_(info),
		config_(config),
		alloc_(alloc),
		totalLock_(totalLock) {
}


JobNode::Body::Body(const Source &source) :
		alloc_(source.alloc_),
		totalLock_(source.totalLock_),
		info_(source.info_),
		initialIOCapacity_(source.config_.initialNodeIOCapacity_),
		minIOCache_(source.config_.minNodeIOCache_),
		inputSize_(0),
		inputCapacity_(initialIOCapacity_),
		outputSize_(0),
		outputCapacity_(initialIOCapacity_) {
}


JobTransporter::JobTransporter(
		Engine &engine, const PriorityJobConfig &config) :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "jobTransporter")),
		nodeSetAlloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "jobNodeSet")),
		engine_(engine),
		config_(config),
		priorityControlActivated_(config.priorityControlActivated_),
		receiverHandlerMap_(alloc_),
		selfNodeEntry_(createNodeEntry(
				alloc_,
				createNode(
						nodeSetAlloc_, nodeSetLock_, engine_, config_, NULL))),
		nodeMap_(alloc_),
		activityCheckTime_(0),
		activityCheckTimestamp_(-1),
		nodeValidationTimeout_(config_.nodeValidationTimeout_) {
}

JobTransporter::~JobTransporter() {
	clear();
}

void JobTransporter::setUpHandlers(PriorityJobController &controller) {
	typedef PriorityJobController Controller;
	setReceiverHandler<JobMessages::Deploy>(
			controller, &Controller::deploy);
	setReceiverHandler<JobMessages::ReadyOutput>(
			controller, &Controller::readyOutput);
	setReceiverHandler<JobMessages::Pipe>(
			controller, &Controller::pipe);
	setReceiverHandler<JobMessages::Finish>(
			controller, &Controller::finish);
	setReceiverHandler<JobMessages::Cancel>(
			controller, &Controller::cancel);
	setReceiverHandler<JobMessages::Complete>(
			controller, &Controller::complete);
	setReceiverHandler<JobMessages::ChangeTaskPriority>(
			controller, &Controller::changeTaskPriority);
	setReceiverHandler<JobMessages::ApplyTaskPathPriority>(
			controller, &Controller::applyTaskPathPriority);
	setReceiverHandler<JobMessages::GrowTaskOutput>(
			controller, &Controller::growTaskOutput);
	setReceiverHandler<JobMessages::GrowNodeOutput>(
			controller, &Controller::growNodeOutput);
	setReceiverHandler<JobMessages::ReportSent>(
			controller, &Controller::reportSent);
	setReceiverHandler<JobMessages::ValidateNode>(
			controller, &Controller::validateNode);

	periodicHandler_ = ALLOC_UNIQUE(alloc_, PeriodicHandler, controller, *this);
	engine_.setPeriodicHandlerDirect(
			PeriodicHandler::getType(), *periodicHandler_);
}

template<typename M>
void JobTransporter::setReceiverHandler(
		PriorityJobController &controller,
		typename HandlerBase<M>::ReceiverFunc func) {
	const JobMessages::Type type = JobMessages::Utils::getEventType<M>();
	ReceiverHandler<M> *handler;
	{
		util::AllocUniquePtr< ReceiverHandler<M> > handlerPtr(ALLOC_UNIQUE(
				alloc_, ReceiverHandler<M>, controller, *this, func));
		if (receiverHandlerMap_.find(type) != receiverHandlerMap_.end()) {
			return errorDuplicateHandler();
		}
		receiverHandlerMap_.insert(std::make_pair(type, handlerPtr.get()));
		handler = handlerPtr.release();
	}
	engine_.setReceiverHandlerDirect(type, *handler);
}

void JobTransporter::prepare(PriorityJobContext &cxt) {
	if (!eventsPrepared_) {
		util::LockGuard<util::Mutex> guard(lock_);
		prepareValidationTimer(guard, cxt);
	}
}

template<typename M>
bool JobTransporter::send(
		PriorityJobContext &cxt, const JobNode &node, const M &msg,
		const RequestOption *option) {
	NodeEntry &entry = resolveNodeEntry(node);

	util::LockGuard<util::Mutex> guard(entry.lock_);
	return sendInternal(guard, cxt, entry, msg, option);
}

const JobNode& JobTransporter::getSelfNode() {
	return selfNodeEntry_->node_;
}

JobNode JobTransporter::resolveNode(const util::SocketAddress &address) {
	util::LockGuard<util::Mutex> guard(lock_);
	return resolveNodeEntryDetail(guard, address, true).node_;
}

uint64_t JobTransporter::requestNodeValidation(
		PriorityJobContext &cxt, const JobNode &node) {
	NodeEntry &entry = resolveNodeEntry(node);

	util::LockGuard<util::Mutex> guard(entry.lock_);
	return requestNodeValidationInternal(guard, cxt, entry, NULL, true, NULL);
}

void JobTransporter::invalidateRandomNode(
		PriorityJobContext& cxt, PriorityJobController &controller,
		int32_t mode) {
	NodeEntry *entry = NULL;
	{
		util::LockGuard<util::Mutex> guard(lock_);

		if (nodeMap_.empty()) {
			return;
		}

		const size_t size = nodeMap_.size();
		const size_t idx = static_cast<size_t>(rand()) % size;

		size_t i = 0;
		for (NodeMap::iterator it = nodeMap_.begin(); it != nodeMap_.end(); ++it) {
			if (i == idx) {
				entry = it->second;
				break;
			}
			++i;
		}

		if (entry == NULL) {
			return;
		}
	}

	if (mode == 0) {
		int64_t aliveTime;
		int64_t requestTime;
		JobNode node;
		{
			util::LockGuard<util::Mutex> guard(entry->lock_);

			aliveTime = entry->lastAliveTime_;
			requestTime = entry->validationRequestTime_;

			node = invalidateNodeEntry(guard, controller, *entry);
		}

		const int64_t currentTime = cxt.resolveEventMonotonicTime();
		try {
			errorNodeTimeout(node, currentTime, aliveTime, requestTime, config_);
		}
		catch (...) {
			std::exception e;
			controller.invalidateNode(cxt, node, e);
		}
	}
	else if (mode == 1) {
		util::LockGuard<util::Mutex> guard(entry->lock_);
		entry->lastOutMessageId_++;
	}
}

void JobTransporter::ackNodeValidation(
		PriorityJobContext &cxt, const JobNode &node,
		uint64_t validationCode) {
	NodeEntry &entry = resolveNodeEntry(node);
	const bool ackRequired = false;

	util::LockGuard<util::Mutex> guard(entry.lock_);
	requestNodeValidationInternal(
			guard, cxt, entry, NULL, ackRequired, &validationCode);
}

void JobTransporter::accpetNodeValidationResult(
		const JobNode &node, uint64_t validationCode) {
	NodeEntry &entry = resolveNodeEntry(node);
	util::LockGuard<util::Mutex> guard(entry.lock_);

	if (!entry.validationStarted_ ||
			entry.workingValidationCode_ != validationCode) {
		return;
	}

	entry.prevValidatingTime_ = entry.lastValidatingTime_;
	entry.lastValidatingTime_ = entry.validationRequestTime_;
	entry.validationRequestTime_ = 0;
	entry.workingValidationCode_ = 0;
	entry.validationStarted_ = false;
}

void JobTransporter::accpetSelfValidationResult(PriorityJobContext &cxt) {
	NodeEntry &entry = *selfNodeEntry_;
	const int64_t currentTime = cxt.resolveEventMonotonicTime();

	util::LockGuard<util::Mutex> guard(entry.lock_);
	entry.prevValidatingTime_ = entry.lastValidatingTime_;
	entry.lastValidatingTime_ = currentTime;
}

void JobTransporter::activateControl(bool activated) {
	priorityControlActivated_ = activated;
}

void JobTransporter::setNodeValidationTimeout(int64_t timeoutMillis) {
	nodeValidationTimeout_ = (timeoutMillis > 0 ?
			timeoutMillis : config_.nodeValidationTimeout_);
}

void JobTransporter::getStatProfile(
		util::StackAllocator &alloc, StatJobProfiler &profiler, bool empty) {

	if (!empty && activityCheckTimestamp_ >= 0) {
		util::String *timeStr = ALLOC_NEW(alloc) util::String(alloc);
		CommonUtility::writeTimeStr(
				util::DateTime(activityCheckTimestamp_), *timeStr);
		profiler.activityCheckTime_ = timeStr;
	}
}

void JobTransporter::clear() {
	clearReceiverHandlerMap(alloc_, receiverHandlerMap_);
	clearNodeMap(alloc_, nodeMap_);
}

void JobTransporter::clearReceiverHandlerMap(
		PriorityJobAllocator &alloc, ReceiverHandlerMap &map) {
	for (ReceiverHandlerMap::iterator it = map.begin(); it != map.end(); ++it) {
		util::AllocUniquePtr<BaseReceiverHandler> ptr(it->second, alloc);
		ptr.reset();
	}
	map.clear();
}

void JobTransporter::clearNodeMap(PriorityJobAllocator &alloc, NodeMap &map) {
	for (NodeMap::iterator it = map.begin(); it != map.end(); ++it) {
		util::AllocUniquePtr<NodeEntry> ptr(it->second, alloc);
		ptr.reset();
	}
	map.clear();
}

void JobTransporter::executePeriodicOperations(
		PriorityJobContext &cxt, PriorityJobController &controller) {
	validateWorkers(cxt, controller);

	cleanUpPools(controller);

	const JobMessages::JobActivityList *activityList =
			checkJobActivities(cxt, controller);

	validateNodes(cxt, controller, activityList);
}

void JobTransporter::validateWorkers(
		PriorityJobContext &cxt, PriorityJobController &controller) {
	try {
		controller.getWorkerTable().validateNodes(cxt);
	}
	catch (...) {
		std::exception e;
		controller.getTracer().traceEventHandlingError(
				e, "validating nodes of workers");
	}
}

void JobTransporter::cleanUpPools(PriorityJobController &controller) {
	try {
		const size_t jobCount = controller.getJobTable().getJobCount();
		if (jobCount == 0) {
			controller.getBlockPool().resetPooledBlocks();
		}
	}
	catch (...) {
		std::exception e;
		controller.getTracer().traceEventHandlingError(
				e, "cleaning up pools");
	}
}

const JobMessages::JobActivityList* JobTransporter::checkJobActivities(
		PriorityJobContext &cxt, PriorityJobController &controller) {
	const JobMessages::JobActivityList *activityList = NULL;
	try {
		const int64_t currentTime = cxt.resolveEventMonotonicTime();

		if (currentTime - activityCheckTime_ >= config_.nodeValidationInterval_) {
			activityList =
					controller.checkJobActivities(cxt, getPrevValidatingTime());
			activityCheckTime_ = currentTime;
			activityCheckTimestamp_ =
					cxt.resolveEventContext().getHandlerStartTime().getUnixTime();
			accpetSelfValidationResult(cxt);
		}
	}
	catch (...) {
		std::exception e;
		controller.getTracer().traceEventHandlingError(
				e, "checking activities of jobs");
	}
	return activityList;
}

void JobTransporter::validateNodes(
		PriorityJobContext &cxt, PriorityJobController &controller,
		const JobMessages::JobActivityList *activityList) {
	try {
		util::SocketAddress address;
		for (NodeEntry *entry; (entry = nextNodeEntry(address)) != NULL;) {
			validateNodeAt(cxt, controller, activityList, *entry);
		}
	}
	catch (...) {
		std::exception e;
		controller.getTracer().traceEventHandlingError(
				e, "validating all nodes of transporter");
		return;
	}
}

void JobTransporter::validateNodeAt(
		PriorityJobContext &cxt, PriorityJobController &controller,
		const JobMessages::JobActivityList *activityList,
		NodeEntry &entry) {
	const int64_t currentTime = cxt.resolveEventMonotonicTime();

	int64_t aliveTime;
	int64_t requestTime;
	JobNode node;
	try {
		util::LockGuard<util::Mutex> guard(entry.lock_);
		requestNodeValidationInternal(
				guard, cxt, entry, activityList, true, NULL);

		aliveTime = entry.lastAliveTime_;
		requestTime = entry.validationRequestTime_;

		const int64_t aliveElapsed = currentTime - aliveTime;
		const int64_t requestElapsed = currentTime - requestTime;

		const int64_t timeout = nodeValidationTimeout_;
		if (aliveElapsed <= timeout || requestElapsed <= timeout) {
			return;
		}

		controller.getTracer().traceValidationTimeout(
				entry.node_, currentTime, aliveTime, requestTime,
				config_);

		node = invalidateNodeEntry(guard, controller, entry);
	}
	catch (...) {
		std::exception e;
		controller.getTracer().traceEventHandlingError(
				e, "validating node of transporter");
		return;
	}

	try {
		errorNodeTimeout(
				node, currentTime, aliveTime, requestTime, config_);
	}
	catch (...) {
		std::exception e;
		controller.invalidateNode(cxt, node, e);
	}
}

int64_t JobTransporter::getPrevValidatingTime() {
	int64_t time = 0;
	util::SocketAddress address;
	for (;;) {
		NodeEntry *baseEntry = nextNodeEntry(address);
		NodeEntry *entry =
				(baseEntry == NULL ? selfNodeEntry_.get() : baseEntry);

		int64_t subTime;
		{
			util::LockGuard<util::Mutex> guard(entry->lock_);
			subTime = entry->prevValidatingTime_;
		}

		if (subTime > 0) {
			if (time <= 0 || time > subTime) {
				time = subTime;
			}
		}

		if (baseEntry == NULL) {
			break;
		}
	}

	return time;
}

uint64_t JobTransporter::requestNodeValidationInternal(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		NodeEntry &entry, const JobMessages::JobActivityList *activityList,
		bool ackRequired, const uint64_t *validationCode) {
	const uint64_t resolvedCode = (validationCode == NULL ?
			++entry.nextValidationCode_ : *validationCode);

	JobMessages::ValidateNode msg;
	msg.ackRequired_ = ackRequired;
	msg.jobActivityList_ = activityList;
	msg.validationCode_ = resolvedCode;

	sendInternal(guard, cxt, entry, msg, NULL);

	if (ackRequired && activityList != NULL && !entry.validationStarted_) {
		entry.workingValidationCode_ = resolvedCode;
		entry.validationRequestTime_ = cxt.resolveEventMonotonicTime();
		entry.validationStarted_ = true;
	}

	return msg.validationCode_;
}

template<typename M>
bool JobTransporter::sendInternal(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		NodeEntry &entry, const M &msg, const RequestOption *option) {
	assert(eventsPrepared_);

	util::LocalUniquePtr<ReportSentSenderHandler> sentHandler;

	const JobMessages::Type type = JobMessages::Utils::getEventType<M>();
	const bool compatible = JobMessages::Utils::isHeaderCompatible<M>();

	const uint64_t messageId = entry.lastOutMessageId_ + 1;
	const JobMessages::Header &header =
			makeRequestHeader(compatible, messageId);
	SenderHandler<M> handler(&msg);

	if (!engine_.sendDirect(
			cxt, entry.node_, type, header, handler,
			makeRequestOption(sentHandler, option))) {
		return false;
	}

	entry.lastOutMessageId_ = messageId;
	return true;
}

void JobTransporter::prepareValidationTimer(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt) {
	if (eventsPrepared_) {
		return;
	}

	const int32_t interval = static_cast<int32_t>(std::min(
			config_.outputValidationInterval_,
			config_.nodeValidationInterval_));
	engine_.addPeriodicTimer(cxt, PeriodicHandler::getType(), interval);
	eventsPrepared_ = true;
}

JobTransporter::EngineRequestOption JobTransporter::makeRequestOption(
		util::LocalUniquePtr<ReportSentSenderHandler> &sentHandler,
		const RequestOption *baseOption) {
	EngineRequestOption option;
	if (baseOption != NULL) {
		sentHandler = UTIL_MAKE_LOCAL_UNIQUE(
				sentHandler, ReportSentSenderHandler, *baseOption);
		option.typeOnSent_ = PRIORITY_JOB_REPORT_SENT;
		option.handlerOnSent_ = sentHandler.get();
	}
	return option;
}

JobMessages::Header JobTransporter::makeRequestHeader(
		bool compatible, uint64_t messageId) {
	JobMessages::Header header;

	header.compatible_ = compatible;
	memcpy(header.uuid_, selfNodeEntry_->uuid_, sizeof(UUIDValue));
	header.messageId_ = messageId;

	if (compatible) {
		const bool activated = true;
		header.compatiblePart_.priorityControlActivated_ = activated;
	}

	return header;
}

bool JobTransporter::acceptHeader(
		PriorityJobContext &cxt, PriorityJobController &controller,
		const JobNode &node, const JobMessages::Header &header) {
	if (header.compatible_) {
		const bool activated = checkResourceControlLevel(
				node, header.compatiblePart_.priorityControlActivated_);
		if (!activated) {
			return false;
		}
	}

	if (node.isEmpty()) {
		return true;
	}

	NodeEntry &entry = resolveNodeEntry(node);

	bool uuidMatch;
	uint64_t expectedMessageId;
	do {
		util::LockGuard<util::Mutex> guard(entry.lock_);
		entry.lastAliveTime_ = controller.getEnvironment().getMonotonicTime();

		if (entry.uuidAssigned_) {
			uuidMatch = (
					memcmp(header.uuid_, entry.uuid_, sizeof(UUIDValue)) == 0);
			expectedMessageId = ++entry.lastInMessageId_;
			if (!uuidMatch || expectedMessageId != header.messageId_) {
				invalidateNodeEntry(guard, controller, entry);
				assignInitialHeader(entry, header);
				break;
			}
		}
		else {
			assignInitialHeader(entry, header);
		}

		return true;
	}
	while (false);

	try {
		errorInvalidHeader(
				node, uuidMatch, expectedMessageId, header.messageId_);
		return true;
	}
	catch (...) {
		std::exception e;
		controller.invalidateNode(cxt, node, e);
		throw;
	}
}

void JobTransporter::assignInitialHeader(
		NodeEntry &entry, const JobMessages::Header &header) {
	memcpy(entry.uuid_, header.uuid_, sizeof(UUIDValue));
	entry.uuidAssigned_ = true;
	entry.lastInMessageId_ = header.messageId_;
}

bool JobTransporter::checkResourceControlLevel(
		const JobNode &node, bool activated) {
	const bool localActivated = priorityControlActivated_;
	if (!activated != !localActivated) {
		errorResourceControlLevel(node, localActivated, activated);
	}
	return localActivated;
}

JobNode JobTransporter::invalidateNodeEntry(
		util::LockGuard<util::Mutex>&, PriorityJobController &controller,
		NodeEntry &entry) {
	JobNode invalidNode = entry.node_;

	JobNode nextNode;
	try {
		nextNode = createNode(
				nodeSetAlloc_, nodeSetLock_, engine_, config_,
				&invalidNode.getInfo().address_);
	}
	catch (...) {
		std::exception e;
		controller.getTracer().traceEventHandlingError(e, "node resolving");
	}

	{
		util::LockGuard<util::Mutex> subGuard(lock_);
		invalidNode.close();
		entry.resetEntry(nextNode);
	}

	return invalidNode;
}

JobTransporter::NodeEntry& JobTransporter::resolveNodeEntry(
		const JobNode &node) {
	util::LockGuard<util::Mutex> guard(lock_);
	bool selfAllowed = false;
	NodeEntry &entry =
			resolveNodeEntryDetail(guard, node.getInfo().address_, selfAllowed);
	if (!(entry.node_ == node)) {
		return errorInvalidNode(node);
	}
	return entry;
}

JobTransporter::NodeEntry& JobTransporter::resolveNodeEntryDetail(
		util::LockGuard<util::Mutex> &guard, const util::SocketAddress &address,
		bool selfAllowed) {
	NodeEntry *entry = findNodeEntry(guard, address);
	if (entry == NULL || entry->node_.isEmpty()) {
		const JobNode &node = createNode(
				nodeSetAlloc_, nodeSetLock_, engine_, config_, &address);
		if (entry == NULL) {
			util::AllocUniquePtr<NodeEntry> entryPtr(
					createNodeEntry(alloc_, node));
			nodeMap_.insert(std::make_pair(address, entryPtr.get()));
			entry = entryPtr.release();
		}
		else {
			entry->node_ = node;
		}
	}

	if (entry->node_.isSelf() && !selfAllowed) {
		return errorSelfNode(address);
	}

	return *entry;
}

JobTransporter::NodeEntry* JobTransporter::findNodeEntry(
		util::LockGuard<util::Mutex>&, const util::SocketAddress &address) {
	NodeMap::iterator it = nodeMap_.find(address);
	if (it == nodeMap_.end()) {
		if (selfNodeEntry_->node_.getInfo().address_ == address) {
			return selfNodeEntry_.get();
		}
		return NULL;
	}
	return it->second;
}

JobTransporter::NodeEntry* JobTransporter::nextNodeEntry(
		util::SocketAddress &lastAddress) {
	util::LockGuard<util::Mutex> guard(lock_);

	NodeEntry *entry;
	if (nodeMap_.empty()) {
		entry = NULL;
	}
	else if (lastAddress.isEmpty()) {
		entry = nodeMap_.begin()->second;
	}
	else {
		NodeMap::iterator it = nodeMap_.upper_bound(lastAddress);
		if (it == nodeMap_.end()) {
			entry = NULL;
		}
		else {
			entry = it->second;
		}
	}

	util::SocketAddress nextAddress;
	if (entry != NULL) {
		nextAddress = entry->node_.getInfo().address_;
	}

	lastAddress = nextAddress;
	return entry;
}

util::AllocUniquePtr<JobTransporter::NodeEntry>::ReturnType
JobTransporter::createNodeEntry(
		PriorityJobAllocator &alloc, const JobNode &node) {
	util::AllocUniquePtr<JobTransporter::NodeEntry> entry(
			ALLOC_UNIQUE(alloc, NodeEntry, node));

	if (node.isSelf()) {
		UUIDUtils::generate(entry->uuid_);
		entry->uuidAssigned_ = true;
	}

	return entry;
}

JobNode JobTransporter::createNode(
		PriorityJobAllocator &nodeSetAlloc, util::Mutex &nodeSetLock,
		Engine &engine, const PriorityJobConfig &config,
		const util::SocketAddress *address) {
	JobNode::Info info = JobNode::Info::emptyInfo();
	if (address == NULL) {
		info = engine.getSelfNodeInfo();
	}
	else if (!engine.getNodeInfo(*address, info) || info.self_) {
		return errorNodeAddress(*address);
	}

	JobNode::Source source(info, config, nodeSetAlloc, nodeSetLock);
	JobNodeBodyBase base(EntryBodyKey(EntryBodyKeyBase()), source);
	return JobNode(&base);
}

void JobTransporter::errorDuplicateHandler() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Duplicate job handler");
}

JobTransporter::NodeEntry& JobTransporter::errorSelfNode(
		const util::SocketAddress &address) {
	GS_THROW_USER_ERROR_TRACE(
			GS_ERROR_JOB_INTERNAL,
			"Self node specified (address=" << address << ")");
}

void JobTransporter::errorResourceControlLevel(
		const JobNode &node, bool local, bool remote) {
	GS_THROW_USER_ERROR(
			GS_ERROR_JOB_INVALID_CONTROL_INFO,
			"Resource control level unmatch ("
			"address=" << node.getInfo().address_ <<
			", localActivated=" << local <<
			", remoteActivated=" << remote << ")");
}

void JobTransporter::errorInvalidHeader(
		const JobNode &node, bool uuidMatch, uint64_t expectedMessageId,
		uint64_t headerMessageId) {
	GS_THROW_USER_ERROR(
			GS_ERROR_JOB_INVALID_CONTROL_INFO,
			"Invalid job message header ("
			"address=" << node.getInfo().address_ << ", "
			"uuidMatch=" << uuidMatch << ", "
			"expectedMessageId=" << expectedMessageId << ", "
			"headerMessageId=" << headerMessageId << ")");
}

JobTransporter::NodeEntry& JobTransporter::errorInvalidNode(
		const JobNode &node) {
	GS_THROW_USER_ERROR(
			GS_ERROR_JOB_RESOLVE_NODE_FAILED,
			"Depending node is not valid ("
			"address=" << node.getInfo().address_ << ")");
}

void JobTransporter::errorNodeTimeout(
		const JobNode &node, int64_t currentTime, int64_t lastAliveTime,
		int64_t validationRequestTime, const PriorityJobConfig &config) {
	const int64_t aliveElapsed =
			(lastAliveTime <= 0 ? -1 : currentTime - lastAliveTime);
	const int64_t requestElapsed = currentTime - validationRequestTime;

	GS_THROW_USER_ERROR(
			GS_ERROR_JOB_NODE_FAILURE,
			"Depending node is not responding ("
			"address=" << node.getInfo().address_ << ", "
			"lastAliveElapsed=" << aliveElapsed << ", "
			"validationRequestElapsed=" << requestElapsed << ", "
			"interval=" << config.nodeValidationInterval_ << ", "
			"timeout=" << nodeValidationTimeout_ << ")");
}

JobNode JobTransporter::errorNodeAddress(const util::SocketAddress &address) {
	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
			"Unknown node address (address=" << address << ")");
}


JobTransporter::Engine::~Engine() {
}


JobTransporter::DefaultEngine::DefaultEngine(EventEngine &base) :
		base_(base) {
}

JobTransporter::DefaultEngine::~DefaultEngine() {
}

void JobTransporter::DefaultEngine::setReceiverHandlerDirect(
		JobMessages::Type type, BaseReceiverHandler &handler) {
	base_.setHandler(type, handler);
	base_.setHandlingMode(type, EventEngine::HANDLING_IMMEDIATE);
}

void JobTransporter::DefaultEngine::setPeriodicHandlerDirect(
		JobMessages::Type type, PeriodicHandler &handler) {
	base_.setHandler(type, handler);
	base_.setHandlingMode(type, EventEngine::HANDLING_IMMEDIATE);
}

bool JobTransporter::DefaultEngine::sendDirect(
		PriorityJobContext &cxt, const JobNode &node,
		JobMessages::Type type, const JobMessages::Header &header,
		const BaseSenderHandler &handler,
		const EngineRequestOption &option) {
	Event ev(Event::Source(cxt.getVarAllocator()), type, 0);

	util::LocalUniquePtr<Event> eventOnSent;
	util::LocalUniquePtr<EventRequestOption> requestOption;

	JobMessages::ByteOutStream out = ev.getOutStream();
	header.encode(out);
	handler(cxt, out);
	return base_.send(
			ev, node.getInfo().nd_,
			makeDefaultRequestOption(
					cxt, option, header, eventOnSent, requestOption));
}

void JobTransporter::DefaultEngine::addPeriodicTimer(
		PriorityJobContext &cxt, JobMessages::Type type,
		int32_t intervalMillis) {
	Event ev(Event::Source(cxt.getVarAllocator()), type, 0);
	base_.addPeriodicTimer(ev, intervalMillis);
}

JobNode::Info JobTransporter::DefaultEngine::getSelfNodeInfo() {
	const bool self = true;
	return makeNodeInfo(base_.getSelfServerND(), self);
}

bool JobTransporter::DefaultEngine::getNodeInfo(
		const util::SocketAddress &address, JobNode::Info &info) {
	const NodeDescriptor &nd = base_.getServerND(address);
	const bool found = !nd.isEmpty();
	const bool self = (found && nd.isSelf());
	info = makeNodeInfo(base_.getServerND(address), self);
	return found;
}

EventRequestOption* JobTransporter::DefaultEngine::makeDefaultRequestOption(
		PriorityJobContext &cxt, const EngineRequestOption &option,
		const JobMessages::Header &header,
		util::LocalUniquePtr<Event> &eventOnSent,
		util::LocalUniquePtr<EventRequestOption> &requestOption) {
	if (option.handlerOnSent_ == NULL) {
		return NULL;
	}

	eventOnSent = UTIL_MAKE_LOCAL_UNIQUE(
			eventOnSent, Event,
			Event::Source(cxt.getVarAllocator()), option.typeOnSent_, 0);

	JobMessages::ByteOutStream out = eventOnSent->getOutStream();
	header.encode(out);
	(*option.handlerOnSent_)(cxt, out);

	requestOption = UTIL_MAKE_LOCAL_UNIQUE(requestOption, EventRequestOption);
	requestOption->eventOnSent_ = eventOnSent.get();

	return requestOption.get();
}

JobNode::Info JobTransporter::DefaultEngine::makeNodeInfo(
		const NodeDescriptor &nd, bool self) {
	if (nd.isEmpty()) {
		return JobNode::Info::emptyInfo();
	}
	else {
		return JobNode::Info(nd.getId(), nd.getAddress(), nd, self);
	}
}


JobTransporter::EngineRequestOption::EngineRequestOption() :
		typeOnSent_(-1),
		handlerOnSent_(NULL) {
}


JobTransporter::RequestOption::RequestOption() :
		taskId_(-1),
		networkBusy_(false),
		sequenceNum_(0),
		sendingSize_(NULL) {
}


JobTransporter::EntryBodyKey::EntryBodyKey(const EntryBodyKeyBase&) {
}


JobTransporter::BaseReceiverHandler::BaseReceiverHandler(
		PriorityJobController &controller, JobTransporter &transporter,
		bool headerCompatible) :
		controller_(controller),
		transporter_(transporter),
		headerCompatible_(headerCompatible) {
}

JobTransporter::BaseReceiverHandler::~BaseReceiverHandler() {
}

void JobTransporter::BaseReceiverHandler::operator()(
		EventContext &ec, Event &ev) {
	try {
		PriorityJobContext cxt(makeContextSource(ec, ev));
		JobMessages::ByteInStream in = ev.getInStream();

		transporter_.prepare(cxt);

		{
			JobMessages::Header header;
			header.compatible_ = headerCompatible_;
			header.decode(cxt, in);
			const bool activated = transporter_.acceptHeader(
					cxt, controller_, cxt.getSenderNode(), header);

			if (!activated) {
				controller_.getEnvironment().handleLegacyJobContol(ec, ev);
				return;
			}
		}

		(*this)(cxt, in);
	}
	catch (...) {
		std::exception e;
		controller_.getTracer().traceEventHandlingError(
				e, "receiving sql message");
	}
}

PriorityJobController& JobTransporter::BaseReceiverHandler::getController() {
	return controller_;
}

PriorityJobContextSource
JobTransporter::BaseReceiverHandler::makeContextSource(
		EventContext &ec, Event &ev) {
	PriorityJobContextSource source = PriorityJobContextSource::ofEvent(ec, ev);
	source.blockPool_ = &controller_.getBlockPool();
	const NodeDescriptor &nd = ev.getSenderND();
	if (!nd.isEmpty()) {
		source.senderNode_ =
				transporter_.resolveNode(ev.getSenderND().getAddress());
	}
	return source;
}


JobTransporter::BaseSenderHandler::~BaseSenderHandler() {
}


template<typename M>
JobTransporter::ReceiverHandler<M>::ReceiverHandler(
		PriorityJobController &controller, JobTransporter &transporter,
		typename HandlerBase<M>::ReceiverFunc func) :
		BaseReceiverHandler(
				controller, transporter,
				JobMessages::Utils::isHeaderCompatible<M>()),
		func_(func) {
}

template<typename M>
JobTransporter::ReceiverHandler<M>::~ReceiverHandler() {
}

template<typename M>
void JobTransporter::ReceiverHandler<M>::operator()(
		PriorityJobContext &cxt, JobMessages::ByteInStream &in) {
	M msg;
	msg.decode(cxt, in);
	(getController().*func_)(cxt, msg);
}

template<typename M>
void JobTransporter::ReceiverHandler<M>::operator()(
		EventContext &ec, Event &ev) {
	BaseReceiverHandler::operator()(ec, ev);
}


template<typename M>
JobTransporter::SenderHandler<M>::SenderHandler(const M *msg) :
		msg_(msg) {
}

template<typename M>
JobTransporter::SenderHandler<M>::~SenderHandler() {
}

template<typename M>
void JobTransporter::SenderHandler<M>::operator()(
		PriorityJobContext &cxt, JobMessages::ByteOutStream &out) const {
	UNUSED_VARIABLE(cxt);

	assert(msg_ != NULL);
	msg_->encode(out);
}


JobTransporter::ReportSentSenderHandler::ReportSentSenderHandler(
		const RequestOption &option) :
		option_(option) {
}

JobTransporter::ReportSentSenderHandler::~ReportSentSenderHandler() {
}

void JobTransporter::ReportSentSenderHandler::operator()(
		PriorityJobContext &cxt, JobMessages::ByteOutStream &out) const {
	UNUSED_VARIABLE(cxt);

	assert(option_.sendingSize_ != NULL);

	JobMessages::ReportSent msg;
	msg.base_.jobId_ = option_.jobId_;
	msg.taskId_ = option_.taskId_;
	msg.networkBusy_ = option_.networkBusy_;
	msg.sequenceNum_ = option_.sequenceNum_;
	msg.size_ = *option_.sendingSize_;

	msg.encode(out);
}


JobTransporter::PeriodicHandler::PeriodicHandler(
		PriorityJobController &controller, JobTransporter &transporter) :
		controller_(controller),
		transporter_(transporter) {
}

JobTransporter::PeriodicHandler::~PeriodicHandler() {
}

void JobTransporter::PeriodicHandler::operator()(EventContext &ec, Event &ev) {
	try {
		PriorityJobContext cxt(PriorityJobContextSource::ofEvent(ec, ev));
		transporter_.executePeriodicOperations(cxt, controller_);
	}
	catch (...) {
		std::exception e;
		controller_.getTracer().traceEventHandlingError(
				e, "handling periodic sql event");
	}
}

JobMessages::Type JobTransporter::PeriodicHandler::getType() {
	return PRIORITY_JOB_CHECK_PERIODICALLY;
}


JobTransporter::NodeEntry::NodeEntry(const JobNode &node) :
		node_(node),
		uuidAssigned_(false),
		validationStarted_(false),
		lastInMessageId_(0),
		lastOutMessageId_(0),
		lastAliveTime_(0),
		lastValidatingTime_(0),
		prevValidatingTime_(0),
		validationRequestTime_(0),
		nextValidationCode_(0),
		workingValidationCode_(0) {
	memset(uuid_, 0, sizeof(UUIDValue));
}

void JobTransporter::NodeEntry::resetEntry(const JobNode &node) {
	node_ = node;

	validationStarted_ = false;
	lastValidatingTime_ = 0;
	prevValidatingTime_ = 0;
	validationRequestTime_ = 0;
	workingValidationCode_ = 0;
}


JobNodeBodyBase::JobNodeBodyBase(
		const JobTransporter::EntryBodyKey&,
		const JobNode::Source &source) :
		source_(source) {
}

const JobNode::Source& JobNodeBodyBase::getSource() const {
	return source_;
}


PriorityJob::PriorityJob(const PriorityJobBodyBase *base) :
		body_(NULL) {
	if (base != NULL) {
		const Source &source = base->getSource();
		body_ = ALLOC_NEW(base->getAllocator()) Body(
				source, base->getAllocator(), base->getLock());
		addReference();
	}
}

PriorityJob::~PriorityJob() {
	removeReference();
}

PriorityJob::PriorityJob(const PriorityJob &another) :
		body_(NULL) {
	*this = another;
}

PriorityJob& PriorityJob::operator=(const PriorityJob &another) {
	if (this != &another) {
		removeReference();
		another.addReference();
		body_ = another.body_;
	}
	return *this;
}

bool PriorityJob::operator==(const PriorityJob &another) const {
	return (body_ == another.body_);
}

bool PriorityJob::operator<(const PriorityJob &another) const {
	return PriorityJobUtils::pointerLessNullsFirst(body_, another.body_);
}

bool PriorityJob::isClosed() const {
	return getBody().state_.isStateClosedDirect();
}

void PriorityJob::close() {
	if (body_ != NULL) {
		closeInternal(NULL, *body_);
	}
}

bool PriorityJob::isEmpty() const {
	return (body_ == NULL);
}

const JobId& PriorityJob::getId() const {
	return getBody().id_;
}

TaskId PriorityJob::getTaskCount() const {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());
	return static_cast<TaskId>(body.taskList_.size());
}

bool PriorityJob::findTask(
		TaskId taskId, bool byPre, PriorityTask &task) const {
	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.lock_.get());
	return findTaskDirectDetail(guard, body, taskId, byPre, task);
}

PriorityTask PriorityJob::getTask(TaskId taskId) const {
	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.lock_.get());
	return getTaskDirectDetail(guard, body, taskId);
}

bool PriorityJob::findTaskDirect(
		util::LockGuard<util::Mutex> &guard, TaskId taskId,
		PriorityTask &task) const {
	Body &body = getBody();
	return findTaskDirectDetail(guard, body, taskId, true, task);
}

const PriorityTask& PriorityJob::getTaskDirect(
		util::LockGuard<util::Mutex> &guard, TaskId taskId) const {
	Body &body = getBody();
	return getTaskDirectDetail(guard, body, taskId);
}

bool PriorityJob::findPreInputTask(TaskId taskId, PriorityTask &task) const {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());
	checkTaskId(guard, taskId, NULL);

	if (static_cast<size_t>(taskId) >= body.preTaskList_.size()) {
		return false;
	}
	task = body.preTaskList_[taskId];

	return !task.isEmpty();
}

bool PriorityJob::findPreInputTaskDirect(
		util::LockGuard<util::Mutex> &guard, TaskId taskId,
		PriorityTask &task) const {
	Body &body = getBody();

	checkTaskId(guard, taskId, NULL);

	if (static_cast<size_t>(taskId) >= body.preTaskList_.size()) {
		return false;
	}
	task = body.preTaskList_[taskId];

	return !task.isEmpty();
}

bool PriorityJob::prepareTask(
		PriorityJobContext &cxt, TaskId taskId, PriorityTask &task,
		uint32_t inputCount, uint32_t outputCount) {
	Body &body = getBody();
	do {
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (body.state_.isOperatable(guard)) {
			task = getTaskDirectDetail(guard, body, taskId);
			break;
		}
		else if (!body.state_.isPreTaskDeployable(guard)) {
			return false;
		}

		TaskList &preTaskList = body.preTaskList_;
		const uint32_t index = checkTaskId(guard, taskId, NULL);

		if (index >= preTaskList.size()) {
			preTaskList.resize(index + 1);
		}

		TaskList::iterator listIt = preTaskList.begin() + taskId;

		if (listIt->isEmpty()) {
			*listIt = createPreTask(
					guard, cxt, body, taskId, inputCount, outputCount);
		}

		task = *listIt;
	}
	while (false);
	return true;
}

bool PriorityJob::findPriorTask(
		const PriorityTask &baseTask, PriorityTask &priorTask) const {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());

	TaskProgressKey key;
	TaskProgressValue value;
	if (!baseTask.findProgressDirect(guard, key, value)) {
		return false;
	}

	const FullTaskProgressKey fullKey(TypedWorker(
			&baseTask.getWorker(), baseTask.getType()), key);

	TaskProgressSet::const_iterator it = body.progressSet_.lower_bound(
			std::make_pair(fullKey, FullTaskProgressValue()));
	if (it == body.progressSet_.end() || it->first != fullKey) {
		return false;
	}

	const TaskProgressValue &priorValue = it->second.first;
	const TaskId &priorTaskId = it->second.second;
	if (priorValue == value || priorTaskId == baseTask.getId()) {
		return false;
	}

	return findTaskDirectDetail(guard, body, priorTaskId, true, priorTask);
}

void PriorityJob::setTaskProgressDirect(
		util::LockGuard<util::Mutex>&, const PriorityTask &task,
		const TaskProgressKey &key, const TaskProgressValue *prevValue,
		const TaskProgressValue *nextValue) {
	Body &body = getBody();

	const FullTaskProgressKey fullKey(TypedWorker(
			&task.getWorker(), task.getType()), key);

	if (prevValue != NULL) {
		const FullTaskProgressValue fullValue(*prevValue, task.getId());
		body.progressSet_.erase(std::make_pair(fullKey, fullValue));
	}

	if (nextValue != NULL) {
		const FullTaskProgressValue fullValue(*nextValue, task.getId());
		body.progressSet_.insert(std::make_pair(fullKey, fullValue));
	}
}

void PriorityJob::deploy(
		PriorityJobContext &cxt, const PriorityJobInfo &jobInfo) {
	Body &body = getBody();

	bool operatable;
	PriorityJobInfo *destJobInfo;
	try {
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isJobDeployable(guard)) {
			return;
		}

		LimitterConfigScopePtr configScope;
		prepareLimitterConfigScope(body, configScope, jobInfo.onCoordinator_);

		assignCoordinator(guard, cxt, body, jobInfo.onCoordinator_);
		assignTaskList(guard, cxt, body, jobInfo);
		assignTaskIO(guard, body);
		assignWorkingMap(guard, body, NULL);
		assignParticipants(guard, body);
		assignRelatedNodes(guard, body, jobInfo);

		destJobInfo = makeParticipantJobInfo(guard, cxt, body, jobInfo);

		body.state_.setDeployed(guard);
		operatable = body.state_.isOperatable(guard);
	}
	catch (...) {
		closeInternal(&cxt, body);
		throw;
	}

	if (!operatable) {
		processClosing(cxt, body, true, NULL, NULL);
		return;
	}

	deployToParticipants(cxt, body, destJobInfo);
	activateNextTasks(cxt, body, false, true);
}

void PriorityJob::readyOutput(
		PriorityJobContext &cxt, const PriorityTask &task) {
	Body &body = getBody();

	PriorityTask resultTask;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		if (!body.state_.tryRemoveOutputPendingTask(guard, task)) {
			return;
		}

		if (body.resultTask_.get() == NULL) {
			assert(false);
			return;
		}
		resultTask = *body.resultTask_;
	}

	if (!resultTask.getNode().isSelf()) {
		JobMessages::ReadyOutput msg;
		msg.base_.jobId_ = body.id_;
		msg.taskId_ = task.getId();
		sendMessage(cxt, resultTask.getNode(), msg);
	}
	else {
		resultTask.setActive(cxt, true);
	}
}

void PriorityJob::finishTask(
		PriorityJobContext &cxt, const PriorityTask &task,
		bool procClosed) {
	Body &body = getBody();

	bool procClosedActual = false;
	bool taskPathDone = false;
	bool outputPending = false;
	PriorityTask resultTask;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		if (procClosed) {
			procClosedActual =
					removeWorkingTask(guard, body, task, taskPathDone);
			removeTaskProgress(guard, body, task);
		}

		outputPending = body.state_.findOutputPendingTask(guard, task);

		if (body.profiling_ &&
				body.resultTask_.get() != NULL && isAllProfiled(guard, body)) {
			resultTask = *body.resultTask_;
		}
	}

	if (outputPending) {
		readyOutput(cxt, task);
	}

	if (procClosedActual &&
			(taskPathDone || body.nextPriority_.get() == NULL)) {
		activateNextTasks(cxt, body, true, false);
	}

	if (!resultTask.isEmpty()) {
		resultTask.setActive(cxt, true);
	}
}

void PriorityJob::changeTaskPriority(
		PriorityJobContext &cxt, const PriorityTask &task,
		uint32_t priorInput) {
	Body &body = getBody();

	if (!body.coordinator_.isSelf()) {
		return;
	}

	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		body.priorInputMap_.insert(std::make_pair(task, priorInput));
	}

	updateTaskPathPriority(cxt, body);
}

void PriorityJob::applyTaskPathPriority(
		PriorityJobContext &cxt, const LocalPriorityList *priorityList,
		bool checking) {
	Body &body = getBody();

	if (checking) {
		const bool acceptable = checkTaskPathPriority(cxt, body, priorityList);
		if (replyTaskPathPriority(cxt, body, priorityList, acceptable)) {
			return;
		}
	}

	applyTaskPathPriorityLocal(cxt, body, priorityList);
}

void PriorityJob::forceCancel(
		PriorityJobContext &cxt, const util::Exception &exception) {
	Body &body = getBody();

	bool forceAllowed = true;
	do {
		util::LockGuard<util::Mutex> guard(body.lock_.get());

		bool responded;
		if (isOnCoordinator(guard, body) &&
				checkClientConnection(guard, body, responded)) {
			forceAllowed = false;
			break;
		}

		body.state_.trySetException(guard, exception);
		body.state_.clearWorkingNodeSet(guard);
		body.state_.clearClosingNodeSet(guard);
	}
	while (false);

	if (!forceAllowed) {
		cancel(cxt, exception, NULL);
		return;
	}

	closeAllTaskIO(cxt, body, true, true);
	body.controller_.getWorkerTable().getBackendIOWorker().removeJob(*this);
	closeRemainingTasks(cxt, body, true, true);
}

void PriorityJob::cancel(
		PriorityJobContext &cxt, const util::Exception &exception,
		const JobNode *requesterNode) {
	Body &body = getBody();
	processClosing(cxt, body, false, &exception, requesterNode);
}

void PriorityJob::complete(
		PriorityJobContext &cxt, const JobProfilerInfo *profilerInfo) {
	Body &body = getBody();
	const JobNode &senderNode = cxt.getSenderNode();

	PriorityTask activatingTask;
	do {
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (body.state_.isStateClosed(guard)) {
			break;
		}

		if (profilerInfo != NULL && body.state_.isOperatable(guard)) {
			mergeProfile(guard, body, *profilerInfo);
			body.state_.removeProfilingNode(guard, senderNode);
			if (body.resultTask_.get() != NULL) {
				activatingTask = *body.resultTask_;
			}
			break;
		}
	}
	while (false);

	if (profilerInfo != NULL) {
		if (!activatingTask.isEmpty()) {
			activatingTask.setActive(cxt, true);
		}
		return;
	}

	processClosing(cxt, body, false, NULL, &senderNode);
}

void PriorityJob::cleanUp(PriorityJobContext &cxt) {
	Body &body = getBody();
	processClosing(cxt, body, true, NULL, NULL);
}

bool PriorityJob::isResultOutputReady() const {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());
	if (!body.state_.isOperatable(guard)) {
		return false;
	}

	return (body.coordinator_.isSelf() && !body.state_.isOutputPending(guard));
}

void PriorityJob::checkCancel() {
	Body &body = getBody();

	try {
		util::LocalUniquePtr<util::Exception> lastException;
		{
			util::LockGuard<util::Mutex> guard(body.lock_.get());
			if (!body.state_.findException(guard, lastException)) {
				return;
			}
		}
		throw *lastException;
	}
	catch (...) {
		std::exception e;
		GS_RETHROW_USER_OR_SYSTEM(
				e, GS_EXCEPTION_MESSAGE_ON(e, "checking cancel"));
	}
}

bool PriorityJob::replyResult(PriorityJobContext &cxt, bool fetchFinished) {
	Body &body = getBody();

	util::LocalUniquePtr<util::Exception> lastException;
	bool profiling;
	bool allProfiled;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		body.state_.findException(guard, lastException);
		profiling = body.profiling_;
		allProfiled = (profiling && isAllProfiled(guard, body));
	}

	SQLExecutionManager *execMgr =
			body.controller_.getEnvironment().getExecutionManager();

	SQLExecutionManager::Latch latch(body.id_.clientId_, execMgr);
	SQLExecution *execution = latch.get();
	if (execution == NULL) {
		closeAllTaskIO(cxt, body, false, true);
		closeRemainingTasks(cxt, body, true, true);
		return true;
	}

	EventContext &ec = cxt.resolveEventContext();
	JobId jobId = body.id_;
	if (lastException.get() != NULL) {
		RequestInfo request(cxt.getAllocator(), false);
		bool replyDone = false;
		try {
			try {
				throw *lastException;
			}
			catch (...) {
				std::exception e;
				GS_RETHROW_USER_ERROR(e, "");
			}
		}
		catch (...) {
			std::exception e;
			const bool respondedOnBegin = profileReply(
					cxt, body, execution, REPLY_ERROR_BEGIN);
			execution->execute(ec, request, true, &e, body.id_.versionId_, &jobId);
			replyDone = profileReplyEnd(
					cxt, body, execution, respondedOnBegin,
					REPLY_ERROR_PENDING, REPLY_ERROR_END);
		}

		const bool pendingException =
				execution->getContext().checkPendingException(false);
		if (!pendingException) {
			body.controller_.getEnvironment().getJobManager()->removeExecution(
					&ec, jobId, execution, true);
		}

		closeAllTaskIO(cxt, body, false, true);
		closeRemainingTasks(cxt, body, true, true);
		return replyDone;
	}
	else if (profiling) {
		if (!allProfiled) {
			return false;
		}

		util::StackAllocator &alloc = ec.getAllocator();

		SQLExecution::SQLReplyContext replyCxt;
		util::Vector<TaskProfiler> taskProfileList(
				static_cast<size_t>(getTaskCount()), TaskProfiler(), alloc);
		for (util::Vector<TaskProfiler>::iterator it = taskProfileList.begin();
				it != taskProfileList.end(); ++it) {
			PriorityTask task =
					getTask(static_cast<TaskId>(it - taskProfileList.begin()));
			*it = task.getProfile(alloc);
		}

		replyCxt.setExplainAnalyze(
				&ec, jobId.versionId_, &jobId, &taskProfileList);

		const bool respondedOnBegin = profileReply(
				cxt, body, execution, REPLY_PROFILE_BEGIN);
		execution->replyClient(replyCxt);
		const bool replyDone = profileReplyEnd(
				cxt, body, execution, respondedOnBegin,
				REPLY_PROFILE_PENDING, REPLY_PROFILE_END);

		closeAllTaskIO(cxt, body, false, true);
		closeRemainingTasks(cxt, body, true, true);
		return replyDone;
	}
	else {
		if (fetchFinished ||
				(execution->isBatch() && !execution->isBatchComplete())) {
			closeAllTaskIO(cxt, body, true, true);
			closeRemainingTasks(cxt, body, true, true);
			if (!execution->getContext().isPreparedStatement()) {
				body.controller_.getEnvironment().getJobManager()->removeExecution(
					&cxt.resolveEventContext(), body.id_, execution, true);
			}
			return true;
		}
		else {
			closeAllTaskIO(cxt, body, false, false);
			closeRemainingTasks(cxt, body, false, false);
			return false;
		}
	}
}

bool PriorityJob::fetch(
		PriorityJobContext &cxt, SQLExecution &execution, bool taskInside) {
	SQLFetchContext fetchContext;
	if (!getFetchContext(cxt, fetchContext)) {
		return true;
	}

	if (!isResultOutputReady()) {
		return false;
	}

	Body &body = getBody();
	EventContext &ec = cxt.resolveEventContext();

	const bool respondedOnBegin = profileReply(
			cxt, body, &execution, REPLY_FETCH_BEGIN);
	const bool completed = execution.fetch(
			ec, fetchContext,
			execution.getContext().getExecId(), body.id_.versionId_);
	profileReplyEnd(
			cxt, body, &execution, respondedOnBegin, REPLY_FETCH_PENDING,
			(completed ? REPLY_FETCH_COMPLETE : REPLY_FETCH_PARTIAL));

	if (!taskInside) {
		if (completed) {
			closeAllTaskIO(cxt, body, false, true);
			closeRemainingTasks(cxt, body, true, true);

			if (!execution.getContext().isPreparedStatement()) {
				body.controller_.getEnvironment().getJobManager()->removeExecution(
						&ec, body.id_, &execution, true);
			}
		}
		else {
			PriorityTask task;
			{
				util::LockGuard<util::Mutex> guard(body.lock_.get());
				if (body.state_.isCancellable(guard) &&
						body.resultTask_.get() != NULL) {
					task = *body.resultTask_;
				}
			}

			if (!task.isEmpty()) {
				task.setActive(cxt, true);
			}
		}
	}

	return completed;
}

bool PriorityJob::getFetchContext(
		PriorityJobContext &cxt, SQLFetchContext &fetchContext) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());

	if (!body.coordinator_.isSelf() || body.resultTask_.get() == NULL) {
		return false;
	}

	if (!body.resultTask_->getFetchContext(guard, cxt, fetchContext)) {
		return false;
	}

	body.state_.applyResultCompleted(guard, fetchContext);
	return true;
}

void PriorityJob::setDdlAckStatus(
		PriorityJobContext &cxt, int32_t pos, int32_t status) {
	Body &body = getBody();

	for (size_t i = 0;; i++) {
		PriorityTask task;
		{
			util::LockGuard<util::Mutex> guard(body.lock_.get());
			if (i >= body.ddlTaskList_.size()) {
				break;
			}
			task = body.ddlTaskList_[i];
		}
		task.setDdlAckStatus(cxt, PriorityTask::DdlAckStatus(pos, status));
	}
}

void PriorityJob::setResultCompleted() {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());
	body.state_.setResultCompletedState(guard);
}

bool PriorityJob::isNodeRelated(const JobNode &node) const {
	if (node.isSelf()) {
		return true;
	}

	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.lock_.get());

	const NodeSet &nodeSet = body.relatedNodeSet_;
	return nodeSet.find(node) != nodeSet.end();
}

util::AllocatorLimitter& PriorityJob::getAllocatorLimitter() const {
	return getBody().allocLimitter_;
}

const PriorityJobConfig& PriorityJob::getConfig() const {
	return getBody().config_;
}

const PriorityTaskConfig& PriorityJob::getTaskConfig() const {
	return *getBody().taskConfig_;
}

uint32_t PriorityJob::getConcurrency() const {
	return getBody().concurrency_;
}

uint32_t PriorityJob::getRestrictionFlags() const {
	return getBody().restrictionFlags_;
}

void PriorityJob::getProfile(util::StackAllocator &alloc, JobProfilerInfo &info) const {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());
	if (body.state_.isStateClosed(guard)) {
		return;
	}

	getProfileAt(guard, alloc, body, info);
}

bool PriorityJob::getStatProfile(
		util::StackAllocator &alloc, StatJobProfilerInfo &info,
		DatabaseId &dbId) const {
	dbId = UNDEF_DBID;

	Body &body = getBody();

	const int64_t curTime = body.controller_.getEnvironment().getMonotonicTime();

	util::LockGuard<util::Mutex> guard(body.lock_.get());

	const TaskId taskCount = (body.state_.isDeployed(guard) ?
			static_cast<TaskId>(body.taskList_.size()) : 0);
	for (TaskId i = 0; i < taskCount; i++) {
		PriorityTask task;
		if (!findTaskDirectDetail(guard, body, i, true, task) ||
				!task.getNode().isSelf()) {
			continue;
		}

		info.taskProfs_.push_back(StatTaskProfilerInfo(alloc));
		StatTaskProfilerInfo &taskInfo = info.taskProfs_.back();

		task.getStatProfile(guard, taskInfo);
	}

	if (body.taskConfig_.get() != NULL) {
		dbId = body.taskConfig_->dbId_;
		CommonUtility::writeTimeStr(
				util::DateTime(body.taskConfig_->startTime_), info.startTime_);

		StatJobProfilerInfo::Config *config =
				ALLOC_NEW(alloc) StatJobProfilerInfo::Config();
		body.taskConfig_->getStatProfile(*config);
		info.config_ = config;
	}

	PriorityTask::writeTimeStrOptional(
			body.replyTimestamp_, info.lastReplyTime_);
	Types::replyFlagsToString(body.replyFlags_, info.replyTypes_);

	info.allocateMemory_ =
			static_cast<int64_t>(body.allocLimitter_.getUsageSize());

	if (body.lastJobWorkTime_ > 0) {
		info.idleTime_ =
				std::max<int64_t>(curTime - body.lastJobWorkTime_, 0);
	}
	if (body.lastActivityCheckTime_ > 0) {
		info.activityCheckElapsedTime_ =
				std::max<int64_t>(curTime - body.lastActivityCheckTime_, 0);
	}

	{
		StatJobProfilerInfo::State *state =
				ALLOC_NEW(alloc) StatJobProfilerInfo::State(alloc);

		body.coordinator_.getInfoOptional().getAddressString(
				state->coordinator_, false);
		State::nodeSetToStringList(
				guard, alloc, body.participantNodeSet_,
				state->participantNodes_);

		body.state_.stateToStatProfile(guard, alloc, *state);
		activityFlagsToStatProfile(body.lastActivityFlags_, *state);
		info.state_ = state;
	}
	return true;
}

void PriorityJob::getResourceProfile(
		util::StackAllocator &alloc, util::Vector<JobResourceInfo> &infoList,
		bool byTask, bool limited) const {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_.get());
	const TaskId taskCount = static_cast<TaskId>(body.taskList_.size());
	if (!body.state_.isDeployed(guard) || taskCount <= 0) {
		return;
	}

	if (!byTask) {
		infoList.push_back(JobResourceInfo(alloc));
	}

	for (TaskId i = 0; i < taskCount; i++) {
		PriorityTask task;
		if (!findTaskDirectDetail(guard, body, i, true, task) ||
				!task.getNode().isSelf()) {
			continue;
		}

		if (byTask) {
			infoList.push_back(JobResourceInfo(alloc));
		}
		task.getResourceProfile(guard, infoList.back(), byTask, limited);
	}
}

void PriorityJob::setUpProfilerInfo(SQLProfilerInfo &profilerInfo) const {
	profilerInfo.deployCompletedCount_ = 0;
	profilerInfo.executeCompletedCount_ = 0;
}

void PriorityJob::startTaskOperation() {
	Body &body = getBody();
	body.state_.addOperatingTasks();
}

void PriorityJob::finishTaskOperation(
		PriorityJobContext &cxt, PriorityJobStats &prev,
		PriorityJobStats &next) {
	Body &body = getBody();
	body.state_.removeOperatingTasks();

	PriorityJobMonitor &monitor = body.controller_.getMonitor();

	PriorityJobMonitor::StatsActions actions;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (body.state_.isStateClosed(guard)) {
			return;
		}

		updateActivityDetail(guard, cxt, body);

		PriorityJobStats &totalNext = body.lastStats_;
		PriorityJobStats totalPrev = totalNext;

		totalNext.subtract(prev);
		totalNext.add(next);

		totalNext.setValueAt(
				PriorityJobStats::TYPE_MEMORY_USE,
				body.allocLimitter_.getUsageSize());

		actions = monitor.updateStats(cxt, body.id_, totalPrev, totalNext);
	}

	monitor.acceptStatsActions(cxt, *this, actions);
}

void PriorityJob::finishEmptyTaskOperation() {
	Body &body = getBody();
	body.state_.removeOperatingTasks();
}

void PriorityJob::updateActivity(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt) {
	Body &body = getBody();
	updateActivityDetail(guard, cxt, body);
}

void PriorityJob::updateRemoteActivity(
		PriorityJobContext &cxt, const JobNode &node, bool active) {
	if (!active) {
		return;
	}

	Body &body = getBody();
	util::LockGuard<util::Mutex> guard(body.lock_.get());

	const int64_t workTime = cxt.resolveEventMonotonicTime();
	body.lastRemoteWorkTime_[node] = workTime;
}

bool PriorityJob::checkTotalActivity(
		PriorityJobContext &cxt, int64_t prevCheckTime) {
	Body &body = getBody();

	checkStatementTimeout(cxt, body);

	bool connected;
	bool responded;

	bool selfWorked;
	bool selfWorkerBusy;
	int64_t selfIdleTime;

	bool remoteWorked;
	int64_t remoteIdleTime;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (body.state_.isStateClosed(guard)) {
			return false;
		}

		connected = checkClientConnection(guard, body, responded);

		selfWorked = checkSelfActivity(
				guard, cxt, body, prevCheckTime, responded, selfWorkerBusy,
				selfIdleTime);
		remoteWorked = checkRemoteActivity(
				guard, cxt, body, prevCheckTime, remoteIdleTime);

		body.lastActivityCheckTime_ = cxt.resolveEventMonotonicTime();
		body.lastActivityFlags_ = makeActivityFlags(
				connected, responded, selfWorked, selfWorkerBusy, remoteWorked);
	}

	if (!connected || (!selfWorked && !remoteWorked)) {
		cancelByActivity(cxt, body, connected, selfIdleTime, remoteIdleTime);
		return false;
	}

	return (connected && selfWorked);
}

template<typename M>
bool PriorityJob::sendMessage(
		PriorityJobContext &cxt, const JobNode &node, const M &msg,
		JobTransporter::RequestOption *option) {
	Body &body = getBody();
	return sendMessageDetail(cxt, body, node, msg, option);
}

PriorityJob::PriorityJob(Body &body) :
		body_(&body) {
	addReference();
}

void PriorityJob::addReference() const {
	if (body_ != NULL) {
		++body_->refCount_;
	}
}

void PriorityJob::removeReference() {
	if (body_ != NULL) {
		if (--body_->refCount_ == 0) {
			PriorityJobAllocator &alloc = body_->baseAlloc_;
			util::Mutex &lock = body_->baseLock_;

			util::LockGuard<util::Mutex> guard(lock);
			ALLOC_DELETE(alloc, body_);
		}
		body_ = NULL;
	}
}

PriorityJob::Body& PriorityJob::getBody() const {
	if (body_ == NULL) {
		return errorEmptyBody();
	}
	return *body_;
}

void PriorityJob::closeInternal(PriorityJobContext *cxt, Body &body) {
	SQLExecutionManager *execMgr =
			body.controller_.getEnvironment().getExecutionManager();

	SQLExecutionManager::Latch latch(body.id_.clientId_, execMgr);
	SQLExecution *execution = latch.get();

	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (body.state_.isStateClosed(guard)) {
			return;
		}

		if (cxt != NULL) {
			traceLongQuery(guard, *cxt, body, execution);
		}

		body.state_.closeState(guard);
		body.selfTaskSet_.clear();
		body.ddlTaskList_.clear();
		body.resultTask_.reset();
	}

	clearOwnedTaskList(body, body.taskList_);
	clearOwnedTaskList(body, body.preTaskList_);

	PriorityJob job(body);
	WorkerSet &workerSet = body.workerSet_;
	for (WorkerSet::iterator it = workerSet.begin();
			it != workerSet.end(); ++it) {
		(*it)->removeJob(job);
	}

	body.controller_.getJobTable().removeJob(body.id_);
	body.controller_.getMonitor().removeStats(body.id_, body.lastStats_);
}

void PriorityJob::clearOwnedTaskList(Body &body, TaskList &taskList) {
	for (TaskList::iterator it = taskList.begin(); it != taskList.end(); ++it) {
		PriorityTask task;
		{
			util::LockGuard<util::Mutex> guard(body.lock_.get());
			task = *it;
			*it = PriorityTask();
		}
		task = PriorityTask();
	}
}

void PriorityJob::prepareLimitterConfigScope(
		Body &body, LimitterConfigScopePtr &limitterScope,
		bool onCoordinator) {
	bool failOnExcess;
	body.controller_.getMonitor().applyJobMemoryLimits(
			body.allocLimitter_, failOnExcess);

	if (!onCoordinator || !failOnExcess) {
		return;
	}

	limitterScope = UTIL_MAKE_LOCAL_UNIQUE(
			limitterScope, LimitterConfigScope, body.allocLimitter_);
	limitterScope->enableFailOnExcess();
}

void PriorityJob::assignCoordinator(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
		bool onCoordinator) {
	body.coordinator_ = (onCoordinator ?
			body.controller_.getTransporter().getSelfNode() :
			cxt.getSenderNode());
}

void PriorityJob::assignTaskList(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		Body &body, const PriorityJobInfo &jobInfo) {
	assert(jobInfo.base_ != NULL);
	assert(jobInfo.processorData_ != NULL);

	const size_t processorDataSize = jobInfo.processorData_->size();
	body.processorData_.resize(processorDataSize);
	memcpy(
			body.processorData_.data(), jobInfo.processorData_->data(),
			processorDataSize);
	JobMessages::ByteInStream in((util::ArrayInStream(
			body.processorData_.data(), processorDataSize)));

	const BaseJobInfo::GsNodeInfoList &nodeInfoList =
			jobInfo.base_->gsNodeInfoList_;
	NodeList nodeList(cxt.getVarAllocator());
	for (
			BaseJobInfo::GsNodeInfoList::const_iterator it =
					nodeInfoList.begin();
			it != nodeInfoList.end(); ++it) {
		if ((*it)->address_.empty()) {
			nodeList.push_back(
					body.controller_.getTransporter().getSelfNode());
			continue;
		}
		util::SocketAddress address;
		address.assign((*it)->address_.c_str(), (*it)->port_);
		nodeList.push_back(
				body.controller_.getTransporter().resolveNode(address));
	}

	const bool profiling = jobInfo.base_->isExplainAnalyze_;
	const bool onCordinator = body.coordinator_.isSelf();

	body.taskConfig_ = UTIL_MAKE_LOCAL_UNIQUE(
			body.taskConfig_, PriorityTaskConfig,
			makeTaskConfig(cxt, body.alloc_.get(), jobInfo));
	body.profiling_ = profiling;

	PriorityTask::Source source(
			guard, cxt, body.controller_, body.alloc_.get(), body.lock_.get(),
			*this, onCordinator);
	source.jobInfo_ = &jobInfo;
	source.profiling_ = profiling;
	source.failOnMemoryExcess_ = body.controller_.getMonitor().isFailOnExcess(
			PriorityJobStats::TYPE_MEMORY_USE);
	source.option_ = jobInfo.base_->option_;
	source.nodeList_ = &nodeList;
	source.syncContext_ = jobInfo.base_->syncContext_;

	TaskList &taskList = body.taskList_;

	const BaseJobInfo::TaskInfoList &infoList = jobInfo.base_->taskInfoList_;
	for (BaseJobInfo::TaskInfoList::const_iterator it = infoList.begin();
			it != infoList.end(); ++it) {
		source.baseId_ = static_cast<TaskId>(it - infoList.begin());
		source.info_ = *it;
		if (processorDataSize > 0) {
			uint32_t size;
			in >> size;
			source.processorData_.first = size;
			source.processorData_.second =
					body.processorData_.data() + in.base().position();
			in.base().position(in.base().position() + size);
		}

		taskList.push_back(PriorityTask());

		PriorityTask &task = taskList.back();
		task = body.controller_.getJobTable().createTask(source);

		if (task.getType() == SQLType::EXEC_DDL) {
			body.ddlTaskList_.push_back(task);
		}
		body.workerSet_.insert(&task.getWorker());
	}
	body.workerSet_.insert(&source.resolveOutputWorker());
}

PriorityTask PriorityJob::createPreTask(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		Body &body, TaskId mainId, uint32_t inputCount, uint32_t outputCount) {
	TaskInfo info(cxt.getAllocator());
	info.inputList_.resize(inputCount, JobManager::UNDEF_TASKID);

	const bool onCordinator = false;
	PriorityTask::Source source(
			guard, cxt, body.controller_, body.alloc_.get(), body.lock_.get(),
			*this, onCordinator);
	source.baseId_ = mainId;
	source.info_ = &info;
	source.forPreInput_ = true;

	PriorityTask task = body.controller_.getJobTable().createTask(source);
	task.setUpForPre(guard, body.config_, outputCount);
	return task;
}

void PriorityJob::assignTaskIO(
		util::LockGuard<util::Mutex> &guard, Body &body) {
	TaskSet &selfTaskSet = body.selfTaskSet_;

	for (TaskList::iterator it = body.taskList_.begin();
			it != body.taskList_.end(); ++it) {
		it->setUp(guard, body.config_);

		if (it->getNode().isSelf()) {
			selfTaskSet.insert(*it);
		}
	}

	if (!body.preTaskList_.empty()) {
		for (TaskList::iterator it = body.taskList_.begin();
				it != body.taskList_.end(); ++it) {
			it->acceptPreOutputCapacity(guard);
		}
	}

	PriorityTask resultTask;
	for (TaskList::iterator it = body.taskList_.begin();
			it != body.taskList_.end(); ++it) {
		if (it->getOutputCount() > 0) {
			continue;
		}
		if (!resultTask.isEmpty()) {
			errorResultTaskMulti();
		}
		resultTask = *it;
	}

	if (resultTask.isEmpty()) {
		errorResultTaskEmpty();
	}

	body.resultTask_ = ALLOC_UNIQUE(body.alloc_.get(), PriorityTask, resultTask);
}

void PriorityJob::assignWorkingMap(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const PriorityList *nextPriorityList) {
	PriorityList &priorityList = body.priorityList_;
	if (nextPriorityList == NULL) {
		makeTaskPathPriority(guard, body, priorityList);
	}
	else {
		priorityList = *nextPriorityList;
	}

	makeWorkingMap(
			guard, body, priorityList, body.state_.getWorkingTaskMap(guard));
}

void PriorityJob::assignParticipants(
		util::LockGuard<util::Mutex>&, Body &body) {
	if (!body.coordinator_.isSelf()) {
		return;
	}

	TaskList &taskList = body.taskList_;
	for (TaskList::iterator it = taskList.begin();
			it != taskList.end(); ++it) {
		const JobNode &node = it->getNode();
		if (node.isSelf()) {
			continue;
		}

		body.participantNodeSet_.insert(node);
	}
}

void PriorityJob::assignRelatedNodes(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const PriorityJobInfo &jobInfo) {
	const bool profiling = (
			jobInfo.onCoordinator_ && jobInfo.base_->isExplainAnalyze_);

	TaskList &taskList = body.taskList_;
	for (TaskList::iterator it = taskList.begin();
			it != taskList.end(); ++it) {
		const JobNode &node = it->getNode();
		if (node.isSelf()) {
			continue;
		}

		if (!body.relatedNodeSet_.insert(node).second) {
			continue;
		}

		body.state_.addWorkingNode(guard, node);
		body.state_.addProfilingNode(guard, node, profiling);
		body.state_.addClosingNodeIfEffective(guard, node);
	}
}

void PriorityJob::deployToParticipants(
		PriorityJobContext &cxt, Body &body,
		const PriorityJobInfo *destJobInfo) {
	if (destJobInfo == NULL) {
		return;
	}

	NodeSet nodeSet(cxt.getVarAllocator());
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		nodeSet = body.participantNodeSet_;
	}

	util::AllocatorLimitter::Scope varLimitterScope(
			body.allocLimitter_, cxt.getVarAllocator());

	for (NodeSet::iterator it = nodeSet.begin(); it != nodeSet.end(); ++it) {
		JobMessages::Deploy msg;
		msg.base_.jobId_ = body.id_;
		msg.jobInfo_ = destJobInfo;

		sendMessageDetail(cxt, body, *it, msg);
	}
}

PriorityJobInfo* PriorityJob::makeParticipantJobInfo(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt, Body &body,
		const PriorityJobInfo &jobInfo) {
	if (!jobInfo.onCoordinator_ || body.participantNodeSet_.empty()) {
		return NULL;
	}

	PriorityJobInfo *destJobInfo;
	makeJobInfo(guard, cxt, body, &jobInfo, destJobInfo);
	return destJobInfo;
}

void PriorityJob::makeJobInfo(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		Body &body, const PriorityJobInfo *src, PriorityJobInfo *&dest) {

	util::StackAllocator &alloc = cxt.getAllocator();

	JobMessages::VarByteXArray data(cxt.getVarAllocator());
	{
		JobMessages::ByteOutStream out((JobMessages::OutStream(data)));
		util::ObjectCoder().encode(out, src);

		JobMessages::ByteInStream in(
				(util::ArrayInStream(data.data(), data.size())));
		dest = NULL;
		util::ObjectCoder().withAllocator(alloc).decode(in, dest);
		dest->onCoordinator_ = false;
	}

	{
		util::XArray<uint8_t> *data =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		JobMessages::StackOutStream out((util::XArrayOutStream<>(*data)));

		for (TaskList::iterator it = body.taskList_.begin();
				it != body.taskList_.end(); ++it) {
			it->encodeProcessor(guard, cxt, out);
		}

		dest->processorData_ = data;
	}
}

PriorityTaskConfig PriorityJob::makeTaskConfig(
		PriorityJobContext &cxt, PriorityJobAllocator &alloc,
		const PriorityJobInfo &info) {
	const BaseJobInfo *base = info.base_;

	PriorityTaskConfig config(alloc);
	config.storeMemoryAgingSwapRate_ = base->storeMemoryAgingSwapRate_;
	config.timeZone_ = base->timezone_;

	config.startTimeRequired_ = info.startTimeRequired_;
	config.startTime_ = info.startTime_;
	config.startMonotonicTime_ = cxt.resolveEventMonotonicTime();

	config.forAdministrator_ = info.forAdministrator_;
	config.forAnalysis_ = info.forAnalysis_;
	config.forResultSet_ = info.forResultSet_;
	config.containerType_ = info.containerType_;
	config.taskCount_ = base->taskInfoList_.size();

	config.clientAddress_ =
			PriorityJobInfo::makeString(alloc, info.clientAddress_);
	config.clientPort_ = info.clientPort_;

	config.dbId_ = info.dbId_;
	config.dbName_ = PriorityJobInfo::makeString(alloc, info.dbName_);
	config.userName_ = PriorityJobInfo::makeString(alloc, info.userName_);
	config.applicationName_ =
			PriorityJobInfo::makeString(alloc, info.applicationName_);
	config.statement_ = PriorityJobInfo::makeString(alloc, info.statement_);
	config.statementTimeout_ = info.statementTimeout_;
	return config;
}

void PriorityJob::activateNextTasks(
		PriorityJobContext &cxt, Body &body, bool deltaOnly, bool initial) {
	TaskSet targetSet(cxt.getVarAllocator());

	util::LocalUniquePtr<TaskSet> checkOnlySet;
	if (initial) {
		checkOnlySet = UTIL_MAKE_LOCAL_UNIQUE(
				checkOnlySet, TaskSet, cxt.getVarAllocator());
	}

	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		if (!updateActiveTasks(
				guard, body, deltaOnly, targetSet, checkOnlySet.get())) {
			return;
		}
	}

	for (TaskSet::iterator it = targetSet.begin();
			it != targetSet.end(); ++it) {
		PriorityTask task = *it;
		task.setActive(cxt, true);
	}

	if (checkOnlySet.get() != NULL) {
		for (TaskSet::iterator it = checkOnlySet->begin();
				it != checkOnlySet->end(); ++it) {
			PriorityTask task = *it;
			task.getWorker().putTask(cxt, task);
		}
	}

	if (targetSet.empty()) {
		reportCompletion(cxt, body);
		closeRemainingTasks(cxt, body, true, true);
	}
}

void PriorityJob::reportCompletion(PriorityJobContext &cxt, Body &body) {
	NodeSet nodeSet(cxt.getVarAllocator());
	JobProfilerInfo *profilerInfo;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		if (!getCompletionReport(guard, cxt, body, nodeSet, profilerInfo)) {
			return;
		}
	}

	for (NodeSet::iterator it = nodeSet.begin(); it != nodeSet.end(); ++it) {
		JobMessages::Complete msg;
		msg.base_.jobId_ = body.id_;
		msg.profilerInfo_ = profilerInfo;

		sendMessageDetail(cxt, body, *it, msg);
	}
}

bool PriorityJob::updateActiveTasks(
		util::LockGuard<util::Mutex> &guard, Body &body, bool deltaOnly,
		TaskSet &targetSet, TaskSet *checkOnlySet) {
	const bool deltaOnlyActual =
			(deltaOnly && body.nextPriority_.get() != NULL);
	const uint32_t concurrency =
			(deltaOnlyActual ? 1 : updateConcurrency(guard, body));

	uint32_t lastPriority = std::numeric_limits<uint32_t>::max();
	uint32_t lastConcurrency = 0;
	bool lastBranching = false;
	bool exceeded = false;

	PriorityTaskMap &map = body.state_.getWorkingTaskMap(guard);

	PriorityTaskMap::iterator it = (deltaOnlyActual ?
			map.lower_bound(TaskPriority(*body.nextPriority_, 0)) :
			map.begin());
	for (; it != map.end(); ++it) {
		const uint32_t priority = it->first.first;
		if (it != map.begin() && priority != lastPriority && !lastBranching) {
			if (lastConcurrency >= concurrency) {
				if (checkOnlySet == NULL) {
					break;
				}
				exceeded = true;
			}
			lastConcurrency++;
		}

		PriorityTask &task = it->second.first;

		if (task.isSchemaCheckRequired()) {

			if (!body.state_.findOnceActivatedTask(guard, task)) {
				const bool checkOnly = exceeded;
				task.setSchemaCheckMode(guard, checkOnly, true);

				if (checkOnly && checkOnlySet != NULL) {
					checkOnlySet->insert(task);
				}
			}

			if (checkOnlySet != NULL) {
				body.state_.addOutputPendingTask(guard, task);
			}
		}

		if (!exceeded) {
			targetSet.insert(task);
			body.state_.addOnceActivatedTask(guard, task);
			body.nextPriority_ = UTIL_MAKE_LOCAL_UNIQUE(
					body.nextPriority_, uint32_t, priority + 1);
		}

		lastPriority = priority;
		lastBranching = it->second.second;
	}

	if (targetSet.empty() && !map.empty()) {
		return false;
	}

	return true;
}

bool PriorityJob::getCompletionReport(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		Body &body, NodeSet &nodeSet, JobProfilerInfo *&profilerInfo) {
	profilerInfo = NULL;

	if (!body.profiling_ || body.coordinator_.isSelf()) {
		return false;
	}

	util::StackAllocator &alloc = cxt.getAllocator();
	profilerInfo = ALLOC_NEW(alloc) JobProfilerInfo(alloc);
	getProfileAt(guard, alloc, body, *profilerInfo);

	nodeSet.insert(body.coordinator_);
	return true;
}

void PriorityJob::updateTaskPathPriority(PriorityJobContext &cxt, Body &body) {
	if (!body.coordinator_.isSelf()) {
		return;
	}

	LocalPriorityList localPriorityList(cxt.getAllocator());
	NodeSet nodeSet(cxt.getVarAllocator());
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		body.priorityPendingNodeSet_ = body.participantNodeSet_;

		PriorityList priorityList(cxt.getVarAllocator());
		makeTaskPathPriority(guard, body, priorityList);

		localPriorityList.assign(priorityList.begin(), priorityList.end());
		nodeSet = body.priorityPendingNodeSet_;
	}

	if (nodeSet.empty()) {
		applyTaskPathPriorityLocal(cxt, body, &localPriorityList);
		return;
	}

	for (NodeSet::iterator it = nodeSet.begin(); it != nodeSet.end(); ++it) {
		JobMessages::ApplyTaskPathPriority msg;
		msg.base_.jobId_ = body.id_;
		msg.priorityList_ = &localPriorityList;
		msg.checking_ = true;

		sendMessageDetail(cxt, body, *it, msg);
	}
}

bool PriorityJob::checkTaskPathPriority(
		PriorityJobContext &cxt, Body &body,
		const LocalPriorityList *priorityList) {
	util::LockGuard<util::Mutex> guard(body.lock_.get());
	if (!body.state_.isOperatable(guard)) {
		return false;
	}

	if (body.coordinator_.isSelf()) {
		PriorityList localPriorityList(cxt.getVarAllocator());
		makeTaskPathPriority(guard, body, localPriorityList);

		if (priorityList == NULL) {
			return false;
		}

		PriorityList specifiedPriorityList(
				priorityList->begin(), priorityList->end(),
				cxt.getVarAllocator());
		if (specifiedPriorityList != localPriorityList) {
			return false;
		}

		NodeSet &nodeSet = body.priorityPendingNodeSet_;
		nodeSet.erase(cxt.getSenderNode());
	}
	else {
		if (priorityList == NULL ||
				priorityList->size() != body.taskList_.size()) {
			return false;
		}
	}

	const uint32_t currentConcurrency = resolveConcurrency(guard, body);
	const uint32_t nextConcurrency = concurrencyOfNewPriority(
			guard, body, *priorityList, currentConcurrency);
	return isAllowedConcurrencyForNewPriority(
			guard, body, currentConcurrency, nextConcurrency);
}

bool PriorityJob::replyTaskPathPriority(
		PriorityJobContext &cxt, Body &body,
		const LocalPriorityList *priorityList, bool acceptable) {
	bool replyOnly = false;

	NodeSet nodeSet(cxt.getVarAllocator());
	if (body.coordinator_.isSelf()) {
		if (acceptable) {
			util::LockGuard<util::Mutex> guard(body.lock_.get());
			nodeSet = body.participantNodeSet_;
		}
	}
	else {
		nodeSet.insert(body.coordinator_);
		replyOnly = true;
	}

	for (NodeSet::iterator it = nodeSet.begin(); it != nodeSet.end(); ++it) {
		JobMessages::ApplyTaskPathPriority msg;
		msg.base_.jobId_ = body.id_;
		msg.priorityList_ = priorityList;
		msg.checking_ = true;

		sendMessageDetail(cxt, body, *it, msg);
	}

	return replyOnly;
}

void PriorityJob::applyTaskPathPriorityLocal(
		PriorityJobContext &cxt, Body &body,
		const LocalPriorityList *priorityList) {
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!body.state_.isOperatable(guard)) {
			return;
		}

		if (priorityList == NULL ||
				priorityList->size() != body.taskList_.size()) {
			return;
		}

		PriorityList localPriorityList(cxt.getVarAllocator());
		localPriorityList.assign(priorityList->begin(), priorityList->end());
		assignWorkingMap(guard, body, &localPriorityList);
	}

	activateNextTasks(cxt, body, false, false);
}

void PriorityJob::makeTaskPathPriority(
		util::LockGuard<util::Mutex> &guard, Body &body,
		PriorityList &priorityList) {
	assert(body.resultTask_.get() != NULL);
	priorityList.assign(body.taskList_.size(), TaskPriority());

	const PriorInputMap &priorInputMap = body.priorInputMap_;

	typedef std::pair<const PriorityTask*, uint32_t> Point;
	typedef util::AllocVector<Point> PointList;

	uint32_t nextPriority = 1;
	uint32_t assignedCount = 1;
	bool newPriorityPending = false;

	util::AllocMap<uint32_t, uint32_t> depthMap(body.alloc_.get());
	PointList pointList(body.alloc_.get());

	pointList.push_back(Point(&(*body.resultTask_), 0));
	priorityList[pointList.back().first->getId()] =
			TaskPriority(nextPriority, depthMap[nextPriority]);
	while (!pointList.empty()) {
		const Point point = pointList.back();
		pointList.pop_back();

		bool found = false;
		const uint32_t inCount = point.first->getInputCount();
		for (uint32_t i = 0; i < inCount; i++) {
			const uint32_t in = (point.second + i) % inCount;
			const PriorityTask &inTask =
					point.first->getInputTaskDirect(guard, in);

			TaskPriority &dest = priorityList[inTask.getId()];
			if (dest.first != 0) {
				for (PointList::iterator it = pointList.begin();; ++it) {
					const Point &p = (it == pointList.end() ? point : *it);
					TaskPriority &sub = priorityList[p.first->getId()];
					if (sub.first > dest.first) {
						sub = TaskPriority(dest.first, ++depthMap[dest.first]);
					}
					if (it == pointList.end()) {
						break;
					}
				}
				continue;
			}

			uint32_t nextIn = 0;
			PriorInputMap::const_iterator it = priorInputMap.find(inTask);
			if (it != priorInputMap.end() && it->second < inTask.getInputCount()) {
				nextIn = it->second;
			}

			if (newPriorityPending && point.first->getType() != SQLType::EXEC_JOIN) {
				nextPriority++;
				newPriorityPending = false;
			}

			dest = TaskPriority(nextPriority, ++depthMap[nextPriority]);
			pointList.push_back(Point(point.first, in + 1));
			pointList.push_back(Point(&inTask, nextIn));
			assignedCount++;
			found = true;
			break;
		}

		newPriorityPending = !found;
	}

	if (assignedCount != priorityList.size()) {
		errorTaskPath();
	}
}

PriorityJob::TaskPriority PriorityJob::getTaskPathPriority(
		util::LockGuard<util::Mutex> &guard, Body &body, TaskId taskId) {
	return getTaskPathPriorityDetail(guard, body.priorityList_, taskId);
}

PriorityJob::TaskPriority PriorityJob::getTaskPathPriorityDetail(
		util::LockGuard<util::Mutex>&, const PriorityList &priorityList,
		TaskId taskId) {
	if (taskId < 0 || static_cast<size_t>(taskId) > priorityList.size()) {
		return errorTaskIdForPriority();
	}
	return priorityList[taskId];
}

uint32_t PriorityJob::concurrencyOfNewPriority(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const LocalPriorityList &priorityList, uint32_t currentConcurrency) {
	PriorityList localPriorityList(
			priorityList.begin(), priorityList.end(), body.alloc_.get());

	PriorityTaskMap localWorkingMap(body.alloc_.get());
	makeWorkingMap(guard, body, localPriorityList, localWorkingMap);

	uint32_t lastPriority = std::numeric_limits<uint32_t>::max();
	uint32_t lastConcurrency = 0;
	uint32_t newConcurrency = 0;
	bool lastBranching = false;
	bool exceeded = false;

	PriorityTaskMap &map = body.state_.getWorkingTaskMap(guard);
	for (PriorityTaskMap::iterator it = map.begin(); it != map.end(); ++it) {
		bool updated = false;

		const uint32_t priority = it->first.first;
		if (priority != lastPriority && !lastBranching) {
			if (lastConcurrency >= currentConcurrency) {
				exceeded = true;
			}
			lastConcurrency++;
			updated = true;
		}

		PriorityTask &task = it->second.first;
		if (updated && (!exceeded ||
				body.state_.findOnceActivatedTask(guard, task))) {
			newConcurrency++;
		}

		lastPriority = priority;
		lastBranching = it->second.second;
	}

	return newConcurrency;
}

bool PriorityJob::isAllowedConcurrencyForNewPriority(
		util::LockGuard<util::Mutex>&, Body &body,
		uint32_t currentConcurrency, uint32_t nextConcurrency) {
	const uint32_t limit = (
			currentConcurrency *
			body.config_.concurrencyPctOfPriorityUpdates_ / 100);
	return (nextConcurrency <= limit);
}

void PriorityJob::removeTaskProgress(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const PriorityTask &task) {
	TaskProgressKey key;
	TaskProgressValue value;
	if (!task.findProgressDirect(guard, key, value)) {
		return;
	}

	PriorityJob job(body);
	job.setTaskProgressDirect(guard, task, key, &value, NULL);
}

void PriorityJob::makeWorkingMap(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const PriorityList &priorityList, PriorityTaskMap &workingMap) {
	TaskSet &taskSet = body.selfTaskSet_;
	for (TaskSet::iterator it = taskSet.begin(); it != taskSet.end(); ++it) {
		const TaskPriority &priority =
				getTaskPathPriorityDetail(guard, priorityList, it->getId());
		const PriorityTaskValue &value =
				resolvePriorityTaskValue(guard, priorityList, *it);
		workingMap.insert(std::make_pair(priority, value));
	}
}

bool PriorityJob::removeWorkingTask(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const PriorityTask &task, bool &taskPathDone) {
	taskPathDone = false;
	PriorityTaskMap &map = body.state_.getWorkingTaskMap(guard);

	const TaskPriority &key = getTaskPathPriority(guard, body, task.getId());
	PriorityTaskMap::iterator it = map.find(key);
	if (it == map.end()) {
		it = map.begin();
		for (; it != map.end(); ++it) {
			if (task == it->second.first) {
				break;
			}
		}
		return false;
	}

	taskPathDone = it->second.second;
	do {
		if (it != map.begin()) {
			PriorityTaskMap::iterator prevIt = it;
			--prevIt;
			if (prevIt->first.first == key.first) {
				break;
			}
		}

		{
			PriorityTaskMap::iterator nextIt = it;
			++nextIt;
			if (nextIt != map.end() && nextIt->first.first == key.first) {
				break;
			}
		}

		taskPathDone = true;
	}
	while (false);
	map.erase(it);
	return true;
}

PriorityJob::PriorityTaskValue PriorityJob::resolvePriorityTaskValue(
		util::LockGuard<util::Mutex> &guard, const PriorityList &priorityList,
		const PriorityTask &task) {
	bool afterBranching = false;

	const uint32_t selfPriority =
			getTaskPathPriorityDetail(guard, priorityList, task.getId()).first;
	const uint32_t count = task.getInputCount();

	for (uint32_t i = 0; i < count; i++) {
		const uint32_t priority = getTaskPathPriorityDetail(
				guard, priorityList,
				task.getInputTaskDirect(guard, i).getId()).first;
		if (priority > selfPriority) {
			afterBranching = true;
			break;
		}
	}

	return PriorityTaskValue(task, afterBranching);
}

void PriorityJob::closeAllTaskIO(
		PriorityJobContext &cxt, Body &body, bool withOutput,
		bool withResult) {
	TaskList targetList(cxt.getVarAllocator());
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		getAllWorkingTasks(guard, body, true, targetList);
	}

	for (TaskList::iterator it = targetList.begin();
			it != targetList.end(); ++it) {
		if (!withResult && *it == *body.resultTask_) {
			continue;
		}

		it->closeInput(cxt);

		if (withOutput) {
			it->closeOutput(cxt);
		}
	}
}

void PriorityJob::closeRemainingTasks(
		PriorityJobContext &cxt, Body &body, bool withJob,
		bool withJobForce) {
	TaskList targetList(cxt.getVarAllocator());
	bool taskWorking;
	bool nodesWorking;
	bool closeable;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (withJob) {
			getAllWorkingTasks(guard, body, false, targetList);
		}
		taskWorking = body.state_.isTaskWorking(guard);
		nodesWorking = body.state_.isRemoteWorking(guard);
		closeable = body.state_.isAllRemoteClosed(guard);
	}

	if (withJob && nodesWorking) {
		processClosing(cxt, body, withJobForce, NULL, NULL);
	}

	for (TaskList::iterator it = targetList.begin();
			it != targetList.end(); ++it) {
		it->setActive(cxt, false);
		it->getWorker().removeTask(*it);
	}

	if (withJob) {
		activateClosingTask(cxt, body, true, NULL);
	}

	if (closeable && ((withJob && !taskWorking) || withJobForce)) {
		closeInternal(&cxt, body);
	}
}

void PriorityJob::activateClosingTask(
		PriorityJobContext &cxt, Body &body, bool force,
		const util::Exception *exception) {
	PriorityTask activatingTask;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());

		const bool required = (
				force ||
				exception != NULL ||
				body.state_.isExceptionAssigned(guard));

		if (required &&
				isOnCoordinator(guard, body) &&
				!body.state_.isStateClosed(guard) &&
				body.resultTask_.get() != NULL) {
			activatingTask = *body.resultTask_;
		}
	}

	if (!activatingTask.isEmpty()) {
		activatingTask.setActive(cxt, true);
	}
}

void PriorityJob::processClosing(
		PriorityJobContext &cxt, Body &body, bool taskIgnorable,
		const util::Exception *exception, const JobNode *requesterNode) {
	activateClosingTask(cxt, body, false, exception);

	NodeSet nodeSet(cxt.getVarAllocator());
	bool withError;
	bool force;
	util::LocalUniquePtr<util::Exception> localException;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());

		if (exception != NULL) {
			body.state_.trySetException(guard, *exception);
		}

		if (!body.state_.isDeployed(guard)) {
			body.state_.addDisabledNodeOptional(guard, requesterNode);
			return;
		}

		body.state_.getWorkingNodeSet(guard, nodeSet);

		withError = body.state_.isExceptionAssigned(guard);
		force = true;
		bool sending = true;
		bool byCoordinator = false;

		if (requesterNode != NULL) {
			bool allRemovedByLastOp;
			body.state_.removeClosingNode(
					guard, *requesterNode, allRemovedByLastOp);

			byCoordinator = (*requesterNode == body.coordinator_);
			if (!withError && !byCoordinator) {
				if (!allRemovedByLastOp) {
					force = false;
				}

				if (body.coordinator_.isSelf() && !allRemovedByLastOp) {
					sending = false;
				}
			}
		}

		if (taskIgnorable || byCoordinator) {
			body.state_.clearWorkingTasks(guard);
		}

		if (!withError && !taskIgnorable && body.state_.isTaskWorking(guard)) {
			force = false;
			sending = false;
		}

		if (body.state_.isRemoteCoordinatorClosing(guard, body.coordinator_) &&
				!body.state_.isTaskWorking(guard) &&
				body.state_.isAllRemoteClosed(guard)) {
			force = true;
		}

		if (sending) {
			body.state_.clearWorkingNodeSet(guard);
		}
		else {
			nodeSet.clear();
		}

		if (withError) {
			body.state_.findException(guard, localException);
		}
	}

	for (NodeSet::iterator it = nodeSet.begin(); it != nodeSet.end(); ++it) {
		if (withError) {
			JobMessages::Cancel msg;
			msg.base_.jobId_ = body.id_;
			msg.exception_ = UTIL_MAKE_LOCAL_UNIQUE(
					msg.exception_, util::Exception, *localException);
			sendMessageDetail(cxt, body, *it, msg);
		}
		else {
			JobMessages::Complete msg;
			msg.base_.jobId_ = body.id_;
			sendMessageDetail(cxt, body, *it, msg);
		}
	}

	if (!force) {
		return;
	}

	PriorityJob job(body);
	closeAllTaskIO(cxt, body, true, true);
	body.controller_.getWorkerTable().getBackendIOWorker().removeJob(job);
	closeRemainingTasks(cxt, body, true, false);
}

void PriorityJob::getAllWorkingTasks(
		util::LockGuard<util::Mutex> &guard, Body &body, bool withResult,
		TaskList &targetList) {
	PriorityTaskMap &taskMap = body.state_.getWorkingTaskMap(guard);

	for (PriorityTaskMap::iterator it = taskMap.begin();
			it != taskMap.end(); ++it) {
		const PriorityTask &task = it->second.first;
		if (withResult ||
				(body.resultTask_.get() != NULL &&
				!(task == *body.resultTask_))) {
			targetList.push_back(task);
		}
	}
}

bool PriorityJob::isAllProfiled(
		util::LockGuard<util::Mutex> &guard, Body &body) {
	if (!body.state_.isAllRemoteProfiled(guard) ||
			body.resultTask_.get() == NULL) {
		return false;
	}

	PriorityTaskMap &map = body.state_.getWorkingTaskMap(guard);
	if (map.size() > 1 || map.empty()) {
		return false;
	}

	return (map.begin()->second.first == *body.resultTask_);
}

void PriorityJob::mergeProfile(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const JobProfilerInfo &profilerInfo) {
	typedef util::Vector<TaskProfilerInfo*> ProfilerList;
	const ProfilerList &profilerList = profilerInfo.profilerList_;

	for (ProfilerList::const_iterator it = profilerList.begin();
			it != profilerList.end(); ++it) {
		PriorityTask &task = getTaskDirectDetail(guard, body, (*it)->taskId_);
		task.setProfileInfo(guard, **it);
	}
}

void PriorityJob::getProfileAt(
		util::LockGuard<util::Mutex> &guard, util::StackAllocator &alloc,
		Body &body, JobProfilerInfo &info) {
	typedef util::Vector<TaskProfilerInfo*> ProfilerList;
	ProfilerList &profilerList = info.profilerList_;

	const TaskId taskCount = static_cast<TaskId>(body.taskList_.size());
	for (TaskId i = 0; i < taskCount; i++) {
		PriorityTask &task = getTaskDirectDetail(guard, body, i);
		if (task.getNode().isSelf()) {
			profilerList.push_back(&task.getProfileInfo(guard, alloc));
		}
	}
}

bool PriorityJob::profileReply(
		PriorityJobContext &cxt, Body &body, SQLExecution *execution,
		ReplyType type) {
	body.replyFlags_ |= (1U << type);
	body.replyTimestamp_ =
			cxt.resolveEventContext().getHandlerStartTime().getUnixTime();

	return execution->getContext().isResponded();
}

bool PriorityJob::profileReplyEnd(
		PriorityJobContext &cxt, Body &body, SQLExecution *execution,
		bool respondedOnBegin, ReplyType pendingType, ReplyType replyType) {
	const bool responded = (
			!respondedOnBegin && execution->getContext().isResponded());
	profileReply(cxt, body, execution, (responded ? replyType : pendingType));
	return responded;
}

void PriorityJob::traceLongQuery(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
		SQLExecution *execution) {

	if (body.taskConfig_.get() == NULL ||
			body.coordinator_.isEmpty() || !body.coordinator_.isSelf()) {
		return;
	}

	const PriorityTaskConfig &config = *body.taskConfig_;
	const int64_t execTime = cxt.resolveEventMonotonicTime() -
			config.startMonotonicTime_;

	JobManager *jobMgr = body.controller_.getEnvironment().getJobManager();
	SQLService *sqlSvc = jobMgr->getSQLService();

	if (!JobManager::checkLongQuery(*sqlSvc, execTime)) {
		return;
	}

	JobResourceInfo info(cxt.getAllocator());
	info.startTime_ = config.startTime_;
	info.leadTime_ = execTime;
	info.statement_ = config.statement_.c_str();
	info.dbName_ = config.dbName_.c_str();
	info.applicationName_ = config.applicationName_.c_str();
	SQLExecution::traceLongQueryCore(info, execution, *sqlSvc);
}

uint32_t PriorityJob::resolveConcurrency(
		util::LockGuard<util::Mutex> &guard, Body &body) {
	if (body.concurrency_ == 0) {
		return updateConcurrency(guard, body);
	}
	return body.concurrency_;
}

uint32_t PriorityJob::updateConcurrency(
		util::LockGuard<util::Mutex>&, Body &body) {
	const PriorityJobConfig &config = body.config_;
	const size_t available = body.allocLimitter_.getAvailableSize();
	const uint64_t unit = config.workMemoryLimit_;

	const uint32_t base = static_cast<uint32_t>(available / unit);
	body.concurrency_ = std::max(std::min(base, config.concurrency_), 1U);
	return body.concurrency_;
}

bool PriorityJob::acceptSendingMessagePriority(
		util::LockGuard<util::Mutex>&, Body &body, const JobNode &node,
		uint32_t level) {
	NodePriorityMap &map = body.sendingPriorityMap_;
	NodePriorityMap::iterator it = map.find(node);

	if (it != map.end() && it->second > level) {
		return false;
	}

	map[node] = level;
	return true;
}

bool PriorityJob::findTaskDirectDetail(
		util::LockGuard<util::Mutex> &guard, Body &body, TaskId taskId,
		bool byPre, PriorityTask &task) {
	if (body.state_.isStateClosed(guard) ||
			(byPre && !body.state_.isDeployed(guard))) {
		return false;
	}

	TaskList &taskList = body.taskList_;
	task = taskList[checkTaskId(guard, taskId, &taskList)];

	if (task.isEmpty()) {
		errorTaskEmpty(false);
		return false;
	}

	return true;
}

PriorityTask& PriorityJob::getTaskDirectDetail(
		util::LockGuard<util::Mutex> &guard, Body &body, TaskId taskId) {
	if (body.state_.isStateClosed(guard)) {
		return errorTaskEmpty(true);
	}

	TaskList &taskList = body.taskList_;
	PriorityTask &task = taskList[checkTaskId(guard, taskId, &taskList)];

	if (task.isEmpty()) {
		return errorTaskEmpty(false);
	}

	return task;
}

void PriorityJob::updateActivityDetail(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		Body &body) {
	int64_t curTime = cxt.resolveEventMonotonicTime();
	int64_t &workTime = body.lastJobWorkTime_;

	workTime = std::max(workTime, curTime);
}

void PriorityJob::cancelByActivity(
		PriorityJobContext &cxt, Body &body, bool connected,
		int64_t selfIdleTime, int64_t remoteIdleTime) {
	PriorityJob job(body);
	try {
		if (connected) {
			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_CANCELLED,
					"SQL Job cancelled by too long idle time ("
					"selfIdleMillis=" << selfIdleTime << ", "
					"remoteIdleMillis=" << remoteIdleTime << ")");
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_CANCELLED,
					"SQL Job cancelled by client connection lost ("
					"selfIdleMillis=" << selfIdleTime << ", "
					"remoteIdleMillis=" << remoteIdleTime);
		}
	}
	catch (...) {
		std::exception e;
		const util::Exception localException = GS_EXCEPTION_CONVERT(e, "");
		job.forceCancel(cxt, localException);
	}
}

void PriorityJob::checkStatementTimeout(
		PriorityJobContext &cxt, Body &body) {
	int64_t elapsed;
	int64_t timeout;
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (body.state_.isStateClosed(guard)) {
			return;
		}

		if (!(!body.state_.isExceptionAssigned(guard) &&
				body.taskConfig_.get() != NULL &&
				body.taskConfig_->statementTimeout_ >= 0)) {
			return;
		}

		elapsed = (
				cxt.resolveEventMonotonicTime() -
				body.taskConfig_->startMonotonicTime_);

		timeout = body.taskConfig_->statementTimeout_;
		const int64_t slackTime = body.config_.outputValidationInterval_;

		if (elapsed - slackTime <= timeout) {
			return;
		}
	}

	try {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_CANCELLED,
				"SQL Job cancelled by statement timeout ("
				"elapsed=" << elapsed << ", timeout=" << timeout << ")");
	}
	catch (...) {
		std::exception e;
		const util::Exception localException = GS_EXCEPTION_CONVERT(e, "");

		PriorityJob job(body);
		job.cancel(cxt, localException, NULL);
	}
}

bool PriorityJob::checkClientConnection(
		util::LockGuard<util::Mutex> &guard, Body &body, bool &responded) {
	bool connected = true;
	responded = false;

	if (isOnCoordinator(guard, body)) {
		SQLExecutionManager *execMgr =
				body.controller_.getEnvironment().getExecutionManager();

		SQLExecutionManager::Latch latch(body.id_.clientId_, execMgr);
		SQLExecution *execution = latch.get();
		if (execution == NULL) {
			connected = false;
		}
		else {
			connected = (body.id_.versionId_ ==
					execution->getContext().getCurrentJobVersionId());
			responded = execution->getContext().isResponded();
		}
	}

	return connected;
}

bool PriorityJob::checkSelfActivity(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		Body &body, int64_t prevCheckTime, bool responded, bool &selfWorkerBusy,
		int64_t &idleTime) {
	idleTime = 0;
	selfWorkerBusy = false;

	const bool taskOperating = body.state_.isTaskOperating();

	if (!taskOperating && body.lastJobWorkTime_ > 0) {
		idleTime = (cxt.resolveEventMonotonicTime() - body.lastJobWorkTime_);
	}

	if (body.lastJobWorkTime_ <= 0) {
		body.lastJobWorkTime_ = cxt.resolveEventMonotonicTime();
	}

	PriorityJobWorker &ioWorker =
			body.controller_.getWorkerTable().getBackendIOWorker();

	selfWorkerBusy = ioWorker.checkJobBusy(*this, prevCheckTime);
	if (!selfWorkerBusy) {
		for (WorkerSet::iterator it = body.workerSet_.begin();
				it != body.workerSet_.end(); ++it) {
			if ((*it)->checkJobBusy(*this, prevCheckTime)) {
				selfWorkerBusy = true;
				break;
			}
		}
	}

	if (body.state_.isExceptionAssigned(guard)) {
		return false;
	}

	const bool workedAfterCheck =
			(body.lastJobWorkTime_ >= prevCheckTime ||
					idleTime < body.config_.nodeValidationTimeout_);
	const bool selfBusyBase =
			(taskOperating || workedAfterCheck || selfWorkerBusy);
	const bool selfBusy =
			checkSpecialTaskActivity(guard, body, responded, selfBusyBase);

	return selfBusy;
}

bool PriorityJob::checkRemoteActivity(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, Body &body,
		int64_t prevCheckTime, int64_t &idleTime) {
	idleTime = 0;
	bool worked = false;
	for (NodeSet::iterator it = body.relatedNodeSet_.begin();
			it != body.relatedNodeSet_.end(); ++it) {
		NodeTimeMap::iterator timeIt = body.lastRemoteWorkTime_.find(*it);

		if (timeIt == body.lastRemoteWorkTime_.end()) {
			body.lastRemoteWorkTime_[*it] = cxt.resolveEventMonotonicTime();
			worked = true;
			continue;
		}
		else if (prevCheckTime <= 0) {
			worked = true;
			continue;
		}

		const int64_t diff = (prevCheckTime - timeIt->second);
		if (diff <= 0) {
			worked = true;
		}
		idleTime = std::max(idleTime, diff);
	}
	return worked;
}

bool PriorityJob::checkSpecialTaskActivity(
		util::LockGuard<util::Mutex> &guard, Body &body, bool responded,
		bool selfBusyBase) {
	if (selfBusyBase) {
		return true;
	}

	if (isOnCoordinator(guard, body) && responded) {
		return true;
	}

	const PriorityTaskMap &map = body.state_.getWorkingTaskMap(guard);
	for (PriorityTaskMap::const_iterator it = map.begin();
			it != map.end(); ++it) {
		if (it->second.first.getType() == SQLType::EXEC_DDL) {
			return true;
		}
	}

	return false;
}

void PriorityJob::activityFlagsToStatProfile(
		uint32_t flags, StatJobProfilerInfo::State &info) {
	info.disconnected_ = ((flags & ACTIVITY_DISCONNECTED) != 0);
	info.responded_ = ((flags & ACTIVITY_RESPONDED) != 0);
	info.selfIdle_ = ((flags & ACTIVITY_SELF_IDLE) != 0);
	info.selfWorkerBusy_ = ((flags & ACTIVITY_SELF_WORKER_BUSY) != 0);
	info.remoteIdle_ = ((flags & ACTIVITY_REMOTE_IDLE) != 0);
}

uint32_t PriorityJob::makeActivityFlags(
		bool connected, bool responded, bool selfWorked,
		bool selfWorkerBusy, bool remoteWorked) {
	return (
			(!connected ? ACTIVITY_DISCONNECTED : 0) |
			(responded ? ACTIVITY_RESPONDED : 0) |
			(!selfWorked ? ACTIVITY_SELF_IDLE : 0) |
			(selfWorkerBusy ? ACTIVITY_SELF_WORKER_BUSY : 0) |
			(!remoteWorked ? ACTIVITY_REMOTE_IDLE : 0));
}

bool PriorityJob::isOnCoordinator(util::LockGuard<util::Mutex>&, Body &body) {
	return (!body.coordinator_.isEmpty() && body.coordinator_.isSelf());
}

template<typename M>
bool PriorityJob::sendMessageDetail(
		PriorityJobContext &cxt, Body &body, const JobNode &node,
		const M &msg, JobTransporter::RequestOption *option) {
	util::LockGuard<util::Mutex> senderGuard(body.senderLock_.get());
	{
		util::LockGuard<util::Mutex> guard(body.lock_.get());
		if (!acceptSendingMessagePriority(
				guard, body, node, M::PRIORITY_LEVEL)) {
			return false;
		}
		updateActivityDetail(guard, cxt, body);
	}
	return body.controller_.getTransporter().send(cxt, node, msg, option);
}

uint32_t PriorityJob::checkTaskId(
		util::LockGuard<util::Mutex>&, TaskId taskId, const TaskList *list) {
	if (taskId < 0) {
		return errorTaskId();
	}

	const uint32_t index = static_cast<uint32_t>(taskId);
	if (list != NULL && index >= list->size()) {
		return errorTaskId();
	}

	return index;
}

PriorityJob::Body& PriorityJob::errorEmptyBody() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Empty job body");
}

uint32_t PriorityJob::errorTaskId() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task ID");
}

PriorityJob::TaskPriority PriorityJob::errorTaskIdForPriority() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task ID");
}

PriorityTask& PriorityJob::errorTaskEmpty(bool closed) {
	GS_THROW_USER_ERROR_TRACE(
			GS_ERROR_JOB_INTERNAL,
			"Empty task accessing (closed=" << closed << ")");
}

void PriorityJob::errorResultTaskMulti() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Multiple result task found");
}

void PriorityJob::errorResultTaskEmpty() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Result task not found");
}

void PriorityJob::errorTaskPath() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task path");
}


PriorityJob::Source::Source(PriorityJobController &controller, JobId id) :
		controller_(controller),
		id_(id) {
}


const PriorityJob::Types::ResEntry PriorityJob::Types::RES_LIST[] = {
	UTIL_NAME_CODER_ENTRY(RES_SCALE_DOWN_BY_SELF_MEMORY),
	UTIL_NAME_CODER_ENTRY(RES_SCALE_DOWN_BY_TOTAL_MEMORY),
	UTIL_NAME_CODER_ENTRY(RES_PIPELINE_DOWN_BY_SELF_MEMORY),
	UTIL_NAME_CODER_ENTRY(RES_PIPELINE_DOWN_BY_TOTAL_MEMORY)
};

const PriorityJob::Types::ReplyEntry PriorityJob::Types::REPLY_LIST[] = {
	UTIL_NAME_CODER_ENTRY(REPLY_ERROR_BEGIN),
	UTIL_NAME_CODER_ENTRY(REPLY_ERROR_PENDING),
	UTIL_NAME_CODER_ENTRY(REPLY_ERROR_END),
	UTIL_NAME_CODER_ENTRY(REPLY_PROFILE_BEGIN),
	UTIL_NAME_CODER_ENTRY(REPLY_PROFILE_PENDING),
	UTIL_NAME_CODER_ENTRY(REPLY_PROFILE_END),
	UTIL_NAME_CODER_ENTRY(REPLY_FETCH_BEGIN),
	UTIL_NAME_CODER_ENTRY(REPLY_FETCH_PENDING),
	UTIL_NAME_CODER_ENTRY(REPLY_FETCH_PARTIAL),
	UTIL_NAME_CODER_ENTRY(REPLY_FETCH_COMPLETE)
};

const PriorityJob::Types::TaskStatusEntry
PriorityJob::Types::TASK_STATUS_LIST[] = {
	UTIL_NAME_CODER_ENTRY(TASK_INACTIVE),
	UTIL_NAME_CODER_ENTRY(TASK_INPUT_WAITING),
	UTIL_NAME_CODER_ENTRY(TASK_INPUT_BLOCK_PENDING),
	UTIL_NAME_CODER_ENTRY(TASK_INPUT_FINISH_PENDING),
	UTIL_NAME_CODER_ENTRY(TASK_INPUT_BUSY),
	UTIL_NAME_CODER_ENTRY(TASK_INPUT_CLOSED),
	UTIL_NAME_CODER_ENTRY(TASK_OUTPUT_BUSY),
	UTIL_NAME_CODER_ENTRY(TASK_OUTPUT_CLOSED),
	UTIL_NAME_CODER_ENTRY(TASK_OPERATING),
	UTIL_NAME_CODER_ENTRY(TASK_CLOSED),
	UTIL_NAME_CODER_ENTRY(TASK_ERROR)
};

const PriorityJob::Types::ResCoder PriorityJob::Types::RES_CODER(RES_LIST, 1);
const PriorityJob::Types::ReplyCoder PriorityJob::Types::REPLY_CODER(
		REPLY_LIST, 1);
const PriorityJob::Types::TaskStatusCoder PriorityJob::Types::TASK_STATUS_CODER(
		TASK_STATUS_LIST, 1);

void PriorityJob::Types::restrictionFlagsToString(
		uint32_t flags, util::String &str) {
	flagsToString(flags, RES_CODER, str);
}

void PriorityJob::Types::replyFlagsToString(uint32_t flags, util::String &str) {
	flagsToString(flags, REPLY_CODER, str);
}

void PriorityJob::Types::taskStatusFlagsToString(uint32_t flags, util::String &str) {
	flagsToString(flags, TASK_STATUS_CODER, str);
}

template<typename T, size_t N>
void PriorityJob::Types::flagsToString(
		uint32_t flags, const util::NameCoder<T, N> &coder,
		util::String &str) {
	if (flags == 0) {
		return;
	}

	const uint32_t count = N;
	bool found = false;
	for (uint32_t i = 0; i < count; i++) {
		if ((flags & (1U << i)) != 0) {
			if (found) {
				str += ",";
			}
			str += coder(static_cast<T>(i), "");
			found = true;
		}
	}
}


PriorityJob::State::State(PriorityJobAllocator &alloc) :
		closed_(false),
		deployed_(false),
		resultCompleted_(false),
		workingNodeSet_(alloc),
		profilingNodeSet_(alloc),
		closingNodeSet_(alloc),
		disabledNodeSet_(alloc),
		workingTaskMap_(alloc),
		outputPendingTaskSet_(alloc),
		onceActivatedTaskSet_(alloc) {
}


bool PriorityJob::State::isStateClosedDirect() const {
	return closed_;
}

bool PriorityJob::State::isStateClosed(Guard&) const {
	return closed_;
}

void PriorityJob::State::closeState(Guard &guard) {
	closed_ = true;

	clearWorkingTasks(guard);
	outputPendingTaskSet_.clear();
	onceActivatedTaskSet_.clear();
}


bool PriorityJob::State::isTaskOperating() {
	return operatingTaskCount_ > 0;
}

void PriorityJob::State::addOperatingTasks() {
	++operatingTaskCount_;
}

void PriorityJob::State::removeOperatingTasks() {
	--operatingTaskCount_;
}


bool PriorityJob::State::isDeployed(Guard&) const {
	return deployed_;
}

bool PriorityJob::State::isJobDeployable(Guard &guard) const {
	return (!isStateClosed(guard) && !isDeployed(guard));
}

bool PriorityJob::State::isPreTaskDeployable(Guard &guard) const {
	return (isCancellable(guard) && !isDeployed(guard));
}

void PriorityJob::State::setDeployed(Guard&) {
	assert(!deployed_);
	deployed_ = true;
}


bool PriorityJob::State::isOperatable(Guard &guard) const {
	return (isCancellable(guard) && isDeployed(guard));
}


void PriorityJob::State::applyResultCompleted(
		Guard&, SQLFetchContext &fetchContext) const {
	fetchContext.isCompleted_ = resultCompleted_;
}

void PriorityJob::State::setResultCompletedState(Guard&) {
	resultCompleted_ = true;
}


bool PriorityJob::State::isRemoteWorking(Guard&) const {
	return !workingNodeSet_.empty();
}

void PriorityJob::State::getWorkingNodeSet(
		Guard&, NodeSet &nodeSet) const {
	nodeSet.insert(workingNodeSet_.begin(), workingNodeSet_.end());
}

void PriorityJob::State::addWorkingNode(Guard&, const JobNode &node) {
	workingNodeSet_.insert(node);
}

void PriorityJob::State::clearWorkingNodeSet(Guard&) {
	workingNodeSet_.clear();
}


bool PriorityJob::State::isAllRemoteProfiled(Guard&) const {
	return profilingNodeSet_.empty();
}

void PriorityJob::State::addProfilingNode(
		Guard&, const JobNode &node, bool enabled) {
	if (enabled) {
		profilingNodeSet_.insert(node);
	}
}

void PriorityJob::State::removeProfilingNode(Guard&, const JobNode &node) {
	profilingNodeSet_.erase(node);
}


bool PriorityJob::State::isAllRemoteClosed(Guard&) const {
	return closingNodeSet_.empty();
}

bool PriorityJob::State::isRemoteCoordinatorClosing(
		Guard&, const JobNode &coordinator) const {
	assert(!coordinator.isEmpty());
	return (!coordinator.isSelf() &&
			closingNodeSet_.find(coordinator) != closingNodeSet_.end());
}

void PriorityJob::State::addClosingNodeIfEffective(
		Guard&, const JobNode &node) {
	if (disabledNodeSet_.find(node) == disabledNodeSet_.end()) {
		closingNodeSet_.insert(node);
	}
}

void PriorityJob::State::removeClosingNode(
		Guard&, const JobNode &node, bool &allRemovedByLastOp) {
	assert(!node.isEmpty());
	const bool found = !closingNodeSet_.empty();
	closingNodeSet_.erase(node);
	allRemovedByLastOp = (found && closingNodeSet_.empty());
}

void PriorityJob::State::clearClosingNodeSet(Guard&) {
	closingNodeSet_.clear();
}

void PriorityJob::State::addDisabledNodeOptional(Guard&, const JobNode *node) {
	if (node != NULL && !node->isEmpty()) {
		disabledNodeSet_.insert(*node);
	}
}


bool PriorityJob::State::isTaskWorking(Guard&) const {
	return !workingTaskMap_.empty();
}

PriorityJob::PriorityTaskMap& PriorityJob::State::getWorkingTaskMap(Guard&) {
	return workingTaskMap_;
}

void PriorityJob::State::clearWorkingTasks(Guard&) {
	workingTaskMap_.clear();
}


bool PriorityJob::State::isOutputPending(Guard&) const {
	return !outputPendingTaskSet_.empty();
}

bool PriorityJob::State::findOutputPendingTask(
		Guard&, const PriorityTask &task) const {
	return (outputPendingTaskSet_.find(task) != outputPendingTaskSet_.end());
}

void PriorityJob::State::addOutputPendingTask(
		Guard&, const PriorityTask &task) {
	outputPendingTaskSet_.insert(task);
}

bool PriorityJob::State::tryRemoveOutputPendingTask(
		Guard&, const PriorityTask &task) {
	TaskSet &taskSet = outputPendingTaskSet_;
	if (taskSet.empty()) {
		return false;
	}
	taskSet.erase(task);

	if (!taskSet.empty()) {
		return false;
	}

	return true;
}


bool PriorityJob::State::findOnceActivatedTask(
		Guard&, const PriorityTask &task) const {
	return (onceActivatedTaskSet_.find(task) != onceActivatedTaskSet_.end());
}

void PriorityJob::State::addOnceActivatedTask(
		Guard&, const PriorityTask &task) {
	onceActivatedTaskSet_.insert(task);
}


bool PriorityJob::State::isCancellable(Guard &guard) const {
	return (!isStateClosed(guard) && exception_.get() == NULL);
}

bool PriorityJob::State::isExceptionAssigned(Guard&) const {
	return (exception_.get() != NULL);
}

bool PriorityJob::State::findException(
		Guard&, util::LocalUniquePtr<util::Exception> &dest) const {
	if (exception_.get() == NULL) {
		return false;
	}

	dest = UTIL_MAKE_LOCAL_UNIQUE(dest, util::Exception, *exception_);
	return true;
}

bool PriorityJob::State::trySetException(
		Guard &guard, const util::Exception &src) {
	if (!isCancellable(guard)) {
		return false;
	}

	exception_ = UTIL_MAKE_LOCAL_UNIQUE(exception_, util::Exception, src);
	return true;
}


void PriorityJob::State::stateToStatProfile(
		Guard &guard, util::StackAllocator &alloc,
		StatJobProfilerInfo::State &info) const {
	info.closed_ = closed_;
	info.deployed_ = deployed_;
	info.resultCompleted_ = resultCompleted_;
	exceptionsToStringList(guard, alloc, exception_.get(), info.exceptions_);
	nodeSetToStringList(guard, alloc, workingNodeSet_, info.workingNodes_);
	nodeSetToStringList(guard, alloc, profilingNodeSet_, info.profilingNodes_);
	nodeSetToStringList(guard, alloc, closingNodeSet_, info.closingNodes_);
	nodeSetToStringList(guard, alloc, disabledNodeSet_, info.disabledNodes_);
	taskMapToIdList(guard, workingTaskMap_, info.workingTasks_);
	info.outputPendingTasks_ =
			taskSetToIdListOptional(guard, alloc, outputPendingTaskSet_);
	info.operatingTaskCount_ = operatingTaskCount_;
}

void PriorityJob::State::exceptionsToStringList(
		Guard &guard, util::StackAllocator &alloc,
		const util::Exception *exception, util::Vector<util::String> &list) {
	if (exception == NULL) {
		return;
	}

	list.push_back(util::String(alloc));
	exceptionToString(guard, *exception, list.back());
}

void PriorityJob::State::exceptionToString(
		Guard&, const util::Exception &exception, util::String &str) {
	str += "[";
	if (exception.hasErrorCode()) {
		if (!exception.hasErrorCodeName()) {
			str += "Code:";
		}

		str += util::TinyLexicalIntConverter().toString<util::String>(
				exception.getErrorCode(), str.get_allocator());

		const char8_t *name = exception.getNamedErrorCode().getName();
		if (name != NULL) {
			str += ":";
			str += name;
		}
	}
	str += "]";

	if (exception.hasMessage()) {
		try {
			util::NormalOStringStream oss;
			oss << " ";
			exception.formatMessage(oss);
			str += oss.str().c_str();
		}
		catch (...) {
		}
	}
}

void PriorityJob::State::nodeSetToStringList(
		Guard&, util::StackAllocator &alloc, const NodeSet &nodeSet,
		util::Vector<util::String> &list) {
	for (NodeSet::const_iterator it = nodeSet.begin();
			it != nodeSet.end(); ++it) {
		list.push_back(util::String(alloc));
		it->getInfoOptional().getAddressString(list.back(), false);
	}
}

void PriorityJob::State::taskMapToIdList(
		Guard&, const PriorityTaskMap &taskMap,
		util::Vector<int32_t> &taskList) {
	for (PriorityTaskMap::const_iterator it = taskMap.begin();
			it != taskMap.end(); ++it) {
		const PriorityTask &task = it->second.first;
		taskList.push_back(task.getId());
	}
}

util::Vector<int32_t>* PriorityJob::State::taskSetToIdListOptional(
		Guard &guard, util::StackAllocator &alloc, const TaskSet &taskSet) {
	if (taskSet.empty()) {
		return NULL;
	}

	util::Vector<int32_t> *taskList =
			ALLOC_NEW(alloc) util::Vector<int32_t>(alloc);
	taskSetToIdList(guard, taskSet, *taskList);
	return taskList;
}

void PriorityJob::State::taskSetToIdList(
		Guard&, const TaskSet &taskSet, util::Vector<int32_t> &taskList) {
	for (TaskSet::const_iterator it = taskSet.begin();
			it != taskSet.end(); ++it) {
		const PriorityTask &task = *it;
		taskList.push_back(task.getId());
	}
}


PriorityJob::Body::Body(
		const Source &source, PriorityJobAllocator &baseAlloc,
		util::Mutex &baseLock) :
		baseAlloc_(baseAlloc),
		baseLock_(baseLock),
		controller_(source.controller_),
		allocLimitter_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_WORK, "priorityJob"),
				controller_.getEnvironment().getTotalAllocatorLimitter()),
		alloc_(controller_.getHoldersPool(), &allocLimitter_, false),
		lock_(controller_.getHoldersPool()),
		senderLock_(controller_.getHoldersPool()),
		id_(source.id_),
		profiling_(false),
		taskList_(alloc_.get()),
		preTaskList_(alloc_.get()),
		selfTaskSet_(alloc_.get()),
		ddlTaskList_(alloc_.get()),
		participantNodeSet_(alloc_.get()),
		relatedNodeSet_(alloc_.get()),
		state_(alloc_.get()),
		concurrency_(controller_.getConfig().concurrency_),
		restrictionFlags_(0),
		priorInputMap_(alloc_.get()),
		priorityList_(alloc_.get()),
		priorityPendingNodeSet_(alloc_.get()),
		sendingPriorityMap_(alloc_.get()),
		progressSet_(alloc_.get()),
		workerSet_(alloc_.get()),
		processorData_(alloc_.get()),
		config_(controller_.getConfig()),
		lastRemoteWorkTime_(alloc_.get()),
		lastJobWorkTime_(0),
		lastActivityCheckTime_(0),
		lastActivityFlags_(0),
		replyFlags_(0),
		replyTimestamp_(-1) {
}


PriorityTask::PriorityTask(const PriorityTaskBodyBase *base) :
		body_(NULL) {
	if (base != NULL) {
		const Source &source = base->getSource();
		body_ = ALLOC_NEW(source.alloc_) Body(source);
		addReference();
	}
}

PriorityTask::~PriorityTask() {
	removeReference();
}

PriorityTask::PriorityTask(const PriorityTask &another) :
		body_(NULL) {
	*this = another;
}

PriorityTask& PriorityTask::operator=(const PriorityTask &another) {
	if (this != &another) {
		removeReference();
		another.addReference();
		body_ = another.body_;
	}
	return *this;
}

bool PriorityTask::operator==(const PriorityTask &another) const {
	return (body_ == another.body_);
}

bool PriorityTask::operator<(const PriorityTask &another) const {
	return PriorityJobUtils::pointerLessNullsFirst(body_, another.body_);
}

void PriorityTask::setUp(
		util::LockGuard<util::Mutex> &guard, const PriorityJobConfig &config) {
	Body &body = getBody();
	const uint32_t limit = body.job_.getConcurrency();
	if (!body.forPreInput_) {
		const uint32_t count = getInputCount();
		for (uint32_t i = 0; i < count; i++) {
			InputEntry &inEntry = getInputEntry(body, i);

			PriorityTask inTask =
					body.job_.getTaskDirect(guard, inEntry.taskId_);
			Body &inBody = inTask.getBody();

			OutputList &outList = inBody.outputList_;

			inEntry.output_ = static_cast<uint32_t>(outList.size());
			outList.push_back(OutputEntry::ofConfig(config));
			inBody.outputNodeSet_.insert(body.node_);

			OutputEntry &outEntry = outList.back();
			outEntry.taskId_ = body.id_;
			outEntry.input_ = i;

			if (i >= limit) {
				inEntry.capacity_ = 0;
				outEntry.capacity_ = 0;
				inBody.restrictedOutputCount_++;
			}
			else {
				body.activatedInputCount_++;
			}
		}

		body.processor_.applyContext(guard, *this);
	}
}

void PriorityTask::setUpForPre(
		util::LockGuard<util::Mutex>&, const PriorityJobConfig &config,
		uint32_t outputCount) {
	Body &body = getBody();

	assert(body.forPreInput_);
	body.outputList_.resize(outputCount, OutputEntry::ofConfig(config));
}

bool PriorityTask::isEmpty() const {
	return (body_ == NULL);
}

TaskId PriorityTask::getId() const {
	return getBody().id_;
}

SQLType::Id PriorityTask::getType() const {
	return getBody().processor_.getType();
}

PriorityJob PriorityTask::getJob() const {
	return getBody().job_;
}

JobNode PriorityTask::getNode() const {
	return getBody().node_;
}

PriorityJobWorker& PriorityTask::getWorker() const {
	return getBody().worker_;
}

uint32_t PriorityTask::getLoadBalance() const {
	return getBody().loadBalance_;
}

uint32_t PriorityTask::getInputCount() const {
	return static_cast<uint32_t>(getBody().inputList_.size());
}

bool PriorityTask::findInputTask(uint32_t input, PriorityTask &task) const {
	const Body &body = getBody();
	return body.job_.findTask(
			getInputEntry(body, input).taskId_, false, task);
}

PriorityTask PriorityTask::getInputTask(uint32_t input) const {
	const Body &body = getBody();
	return body.job_.getTask(getInputEntry(body, input).taskId_);
}

const PriorityTask& PriorityTask::getInputTaskDirect(
		util::LockGuard<util::Mutex> &guard, uint32_t input) const {
	const Body &body = getBody();
	return body.job_.getTaskDirect(
			guard, getInputEntry(body, input).taskId_);
}

uint32_t PriorityTask::getOutputCount() const {
	const Body &body = getBody();
	return static_cast<uint32_t>(body.outputList_.size());
}

uint32_t PriorityTask::getOutputForInputTask(uint32_t input) const {
	const Body &body = getBody();
	return getInputEntry(body, input).output_;
}

uint32_t PriorityTask::getInputForOutputTask(uint32_t output) const {
	const Body &body = getBody();
	return getOutputEntry(body, output).input_;
}

PriorityTask::NodeSetRange PriorityTask::getOutputNodeSet() const {
	const Body &body = getBody();
	const NodeSet &set = body.outputNodeSet_;
	return NodeSetRange(set.begin(), set.end());
}

bool PriorityTask::findPriorTask(PriorityTask &task) const {
	const Body &body = getBody();

	if (body.progressAssigned_) {
		return body.job_.findPriorTask(*this, task);
	}

	return false;
}

bool PriorityTask::isPriorTaskPossible() const {
	const Body &body = getBody();
	return body.progressAssigned_;
}

bool PriorityTask::findProgressDirect(
		util::LockGuard<util::Mutex>&, TaskProgressKey &key,
		TaskProgressValue &value) const {
	const Body &body = getBody();
	key = body.progress_.first;
	value = body.progress_.second;
	return body.progressAssigned_;
}

void PriorityTask::setProgress(
		const TaskProgressKey &key, const TaskProgressValue *value) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);
	setProgressDetail(guard, *this, key, value);
}

void PriorityTask::setInputPriority(uint32_t priorInput) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);
	body.priorInput_ =
			UTIL_MAKE_LOCAL_UNIQUE(body.priorInput_, uint32_t, priorInput);
}

bool PriorityTask::findOutputTask(uint32_t output, PriorityTask &task) const {
	const Body &body = getBody();
	return body.job_.findTask(
			getOutputEntry(body, output).taskId_, false, task);
}

PriorityTask PriorityTask::getOutputTask(uint32_t output) const {
	const Body &body = getBody();
	return body.job_.getTask(getOutputEntry(body, output).taskId_);
}

const PriorityTask& PriorityTask::getOutputTaskDirect(
		util::LockGuard<util::Mutex> &guard, uint32_t output) const {
	const Body &body = getBody();
	return body.job_.getTaskDirect(
			guard, getOutputEntry(body, output).taskId_);
}

bool PriorityTask::addInput(
		PriorityJobContext &cxt, uint32_t input, const InputOption &option,
		const TupleList::Block &block) {
	Body &body = getBody();
	if (body.node_.isSelf()) {
		{
			util::LockGuard<util::Mutex> guard(body.lock_);
			body.job_.updateActivity(guard, cxt);
			if (!isInputAllowed(guard, body)) {
				return false;
			}
			addPreInputRequest(guard, body);
			body.processor_.applyStoreGroup(guard, block);
		}
		body.worker_.addTaskInput(cxt, *this, input, block);
	}
	else {
		uint64_t encodedBlockSize = 0;

		JobMessages::Pipe msg;
		msg.base_.jobId_ = body.job_.getId();
		msg.taskId_ = body.id_;
		msg.input_ = input;
		msg.ioInfo_ = getIOInfo(body);
		msg.block_ = block;
		msg.encodedBlockSize_ = &encodedBlockSize;

		PriorityTask inTask;
		if (!findInputTask(input, inTask)) {
			return false;
		}

		JobTransporter::RequestOption reqOpt;
		reqOpt.jobId_ = msg.base_.jobId_;
		reqOpt.taskId_ = inTask.getId();
		reqOpt.networkBusy_ = option.networkBusy_;
		reqOpt.sequenceNum_ = option.sequenceNum_;
		reqOpt.sendingSize_ = &encodedBlockSize;

		if (!body.job_.sendMessage(cxt, body.node_, msg, &reqOpt)) {
			return false;
		}

		util::LockGuard<util::Mutex> guard(inTask.getBody().lock_);
		inTask.getBody().profiler_.startTransfer(guard, option.networkBusy_);
	}
	return true;
}

bool PriorityTask::addInputUnbuffered(
		PriorityJobContext &cxt, uint32_t input, const InputOption &option,
		const JobUnbufferedBlock &block) {
	Body &body = getBody();
	if (body.node_.isSelf()) {
		{
			util::LockGuard<util::Mutex> guard(body.lock_);
			body.job_.updateActivity(guard, cxt);
			if (!isInputAllowed(guard, body)) {
				return false;
			}
		}
		body.worker_.addTaskInputUnbuffered(cxt, *this, input, block);
		return true;
	}
	else {
		return addInput(cxt, input, option, block.toBufferedBlock());
	}
}

void PriorityTask::finishInput(PriorityJobContext &cxt, uint32_t input) {
	Body &body = getBody();
	if (body.node_.isSelf()) {
		{
			util::LockGuard<util::Mutex> guard(body.lock_);
			body.job_.updateActivity(guard, cxt);
			if (!isInputAllowed(guard, body)) {
				return;
			}
			addPreInputRequest(guard, body);
		}
		body.worker_.finishTaskInput(cxt, *this, input);
	}
	else {
		JobMessages::Finish msg;
		msg.base_.jobId_ = body.job_.getId();
		msg.taskId_ = body.id_;
		msg.input_ = input;
		msg.ioInfo_ = getIOInfo(body);
		body.job_.sendMessage(cxt, body.node_, msg);
	}
}

bool PriorityTask::addInputDirect(
		PriorityJobContext &cxt, uint32_t input,
		const JobUnbufferedBlock &block) {
	Body &body = getBody();
	bool active;
	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		body.job_.updateActivity(guard, cxt);
		if (!isInputAllowed(guard, body)) {
			return false;
		}

		InputEntry &entry = getInputEntry(body, input);
		checkInputAcceptable(entry);

		entry.blockList_.push_back(block);
		body.pendingBlockInputs_.insert(input);
		body.profiler_.updateInputProfile(true);

		active = body.active_;
	}

	acceptPreInputRequest(body, cxt);

	if (!body.forPreInput_) {
		if (active) {
			body.worker_.putTask(cxt, *this);
		}
		else {
			acceptBlockInput(cxt, body, input, true);
		}
	}
	return true;
}

bool PriorityTask::finishInputDirect(PriorityJobContext &cxt, uint32_t input) {
	Body &body = getBody();
	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		body.job_.updateActivity(guard, cxt);
		if (!isInputAllowed(guard, body)) {
			return false;
		}

		InputEntry &entry = getInputEntry(body, input);
		checkInputAcceptable(entry);

		entry.finishPending_ = true;
		body.pendingFinalInputs_.insert(input);
	}
	acceptPreInputRequest(body, cxt);
	body.worker_.putTask(cxt, *this);
	return true;
}

void PriorityTask::addOutput(
		PriorityJobContext &cxt, const TupleList::Block &block) {
	Body &body = getBody();
	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		if (!isOutputAllowed(guard, body)) {
			return;
		}
	}

	bool selfOnly = true;
	const uint32_t outCount = getOutputCount();
	for (uint32_t i = 0; i < outCount; i++) {
		PriorityTask outTask;
		if (!findOutputTask(i, outTask)) {
			continue;
		}

		acceptBlockOutput(i);

		const JobNode &node = outTask.getNode();
		if (!node.isSelf()) {
			selfOnly = false;
			continue;
		}

		const uint32_t input = getInputForOutputTask(i);
		outTask.addInputDirect(cxt, input, JobUnbufferedBlock(block));
	}

	if (!selfOnly) {
		getOutputWorker(getBody()).addTaskOutput(cxt, *this, block);
	}
}

void PriorityTask::finishOutput(PriorityJobContext &cxt) {
	Body &body = getBody();
	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		if (!isOutputAllowed(guard, body)) {
			return;
		}
		if (!body.outputList_.empty()) {
			body.processorFinished_ = true;
		}
	}

	bool selfOnly = true;
	const uint32_t outCount = getOutputCount();
	for (uint32_t i = 0; i < outCount; i++) {
		PriorityTask outTask;
		if (!findOutputTask(i, outTask)) {
			continue;
		}

		const JobNode &node = outTask.getNode();
		if (!node.isSelf()) {
			selfOnly = false;
			continue;
		}

		const uint32_t input = getInputForOutputTask(i);
		outTask.finishInputDirect(cxt, input);
		acceptFinishOutput(cxt);
	}

	if (!selfOnly) {
		getOutputWorker(getBody()).finishTaskOutput(cxt, *this);
	}
}

void PriorityTask::acceptBlockOutput(uint32_t output) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);

	OutputEntry &entry = getOutputEntry(getBody(), output);
	const bool prevRestricted = resolveOutputRestriction(guard, entry);

	++entry.acceptedSize_;
	applyOutputRestriction(guard, body, entry, prevRestricted);
}

void PriorityTask::acceptFinishOutput(PriorityJobContext &cxt) {
	Body &body = getBody();

	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		body.finishedOutputCount_++;
	}

	updateFinishState(cxt, *this);
}

void PriorityTask::reportOutputSent(bool networkBusy, uint64_t transSize) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);
	body.profiler_.finishTransfer(guard, networkBusy, transSize);
}

void PriorityTask::growOutputCapacity(
		PriorityJobContext &cxt, uint32_t output, uint64_t minCapacity) {
	Body &body = getBody();
	if (body.node_.isSelf()) {
		{
			util::LockGuard<util::Mutex> guard(body.lock_);
			if (!isOutputAllowed(guard, body)) {
				return;
			}

			OutputEntry &entry = getOutputEntry(body, output);
			const bool prevRestricted = resolveOutputRestriction(guard, entry);

			entry.capacity_ = ((entry.entryClosed_ || minCapacity == 0) ?
					0 : std::max(entry.capacity_, minCapacity));
			entry.entryClosed_ = (entry.capacity_ == 0);
			applyOutputRestriction(guard, body, entry, prevRestricted);
		}

		if (body.forPreInput_) {
			return;
		}

		if (minCapacity == 0) {
			updateFinishState(cxt, *this);
		}

		body.worker_.putTask(cxt, *this);
	}
	else {
		JobMessages::GrowTaskOutput msg;
		msg.base_.jobId_ = body.job_.getId();
		msg.taskId_ = body.id_;
		msg.output_ = output;
		msg.ioInfo_ = getIOInfo(body);
		msg.capacity_ = minCapacity;
		body.job_.sendMessage(cxt, body.node_, msg);
	}
}

void PriorityTask::acceptPreOutputCapacity(
		util::LockGuard<util::Mutex> &guard) {
	Body &body = getBody();

	PriorityTask preTask;
	if (!body.job_.findPreInputTaskDirect(guard, body.id_, preTask)) {
		return;
	}

	const uint32_t outCount =
			std::min(getOutputCount(), preTask.getOutputCount());

	for (uint32_t i = 0; i < outCount; i++) {
		OutputEntry &src = getOutputEntry(preTask.getBody(), i);
		OutputEntry &dest = getOutputEntry(body, i);

		if (dest.capacity_ == src.capacity_ &&
				!dest.entryClosed_ == !src.entryClosed_) {
			continue;
		}

		const bool prevRestricted = resolveOutputRestriction(guard, dest);

		dest.capacity_ = src.capacity_;
		dest.entryClosed_ = src.entryClosed_;

		applyOutputRestriction(guard, body, dest, prevRestricted);
	}
}

bool PriorityTask::isOutputRestricted(uint32_t output) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);

	OutputEntry &entry = getOutputEntry(body, output);
	return resolveOutputRestriction(guard, entry);
}

void PriorityTask::setActive(PriorityJobContext &cxt, bool active) {
	Body &body = getBody();

	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		body.active_ = active;
	}

	if (active) {
		body.worker_.putTask(cxt, *this);
	}
}

bool PriorityTask::isActive() {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);
	return body.active_;
}

void PriorityTask::setSchemaCheckMode(
		util::LockGuard<util::Mutex>&, bool checkOnly, bool pending) {
	Body &body = getBody();

	if (checkOnly) {
		body.schemaCheckOnly_ = true;
	}

	if (pending) {
		body.schemaCheckPending_ = true;
	}
}

bool PriorityTask::isSchemaCheckRequired() {
	Body &body = getBody();
	return Processor::isSchemaCheckRequiredType(
			body.processor_.getType(), body.worker_.getInfo().serviceType_);
}

void PriorityTask::closeInput(PriorityJobContext &cxt) {
	UNUSED_VARIABLE(cxt);

	Body &body = getBody();

	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		body.inputClosed_ = true;
	}
	body.worker_.removeTaskInput(*this);
}

void PriorityTask::closeOutput(PriorityJobContext &cxt) {
	UNUSED_VARIABLE(cxt);

	Body &body = getBody();

	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		body.outputClosed_ = true;
	}
	body.outputWorker_.removeTaskOutput(*this);
}

PriorityTask::OperationType PriorityTask::prepareOperation(
		PriorityJobContext &cxt, SQLProcessor *&proc,
		SQLProcessor::Context *&procCxt, uint32_t &input,
		TupleList::Block &block, bool &withFinish) {
	proc = NULL;
	procCxt = NULL;
	withFinish = false;

	Body &body = getBody();
	body.profiler_.acceptOperationCheck(cxt);

	OperationType op;
	uint32_t localInput = std::numeric_limits<uint32_t>::max();
	for (;;) {
		if (checkOperatable(body, true)) {
			bool busy;
			if (preparePipe(cxt, body, localInput, block, busy, withFinish)) {
				op = OP_PIPE;
				break;
			}

			if (!busy) {
				bool blockFound;
				if (prepareFinish(cxt, body, localInput, blockFound)) {
					op = OP_FINISH;
					break;
				}
				else if (blockFound) {
					continue;
				}

				if (prepareNext(body)) {
					op = OP_NEXT;
					break;
				}
			}
		}
		op = OP_NONE;
		break;
	}

	if (op != OP_NONE) {
		PriorityTask::Processor &taskProc = body.processor_;
		proc = &taskProc.prepare(cxt, body.job_);
		procCxt = &taskProc.prepareContext(body.job_);
		startOperation(cxt, body, *proc, *this);
	}

	input = localInput;
	return op;
}

void PriorityTask::acceptProcessorResult(
		PriorityJobContext &cxt, SQLProcessor *proc,
		SQLProcessor::Context *procCxt, OperationType op, bool remaining,
		bool &continuable) {
	continuable = false;

	Body &body = getBody();
	PriorityTask::Processor &taskProc = body.processor_;

	const bool operated = (op != OP_NONE);

	bool procFinished;
	bool fetchFinished;
	if (operated) {
		finishOperation(cxt, body);

		procFinished = (isInputFinished(body) && !remaining);
		fetchFinished = (procCxt != NULL && procCxt->isFetchComplete());

		body.nextRemaining_ = (remaining && !isInputEmpty(body));
	}
	else {
		body.job_.finishEmptyTaskOperation();

		procFinished = !checkOperatable(body, false);
		fetchFinished = false;
	}

	do {
		if (procFinished || fetchFinished || body.processorFinished_) {
			if (proc != NULL) {
				body.profiler_.finishProcessor(body, *proc);
			}

			bool replyDone = false;
			if (body.outputList_.empty() && !body.processorFinished_) {
				replyDone = body.job_.replyResult(cxt, fetchFinished);
			}

			if (!body.outputList_.empty() || replyDone) {
				body.processorFinished_ = true;
				closeOperation(body);
				setActive(cxt, false);
			}

			updateFinishState(cxt, *this);
			break;
		}

		bool outputReady;
		bool restricted;
		do {
			util::LockGuard<util::Mutex> guard(body.lock_);
			outputReady = body.schemaCheckPending_;
			if (outputReady) {
				body.schemaCheckPending_ = false;
				if (body.schemaCheckOnly_) {
					body.schemaCheckOnly_ = false;
					if (!body.active_) {
						restricted = true;
						break;
					}
				}
			}
			restricted = isAllOutputRestricted(guard, body);
		}
		while (false);

		if (outputReady) {
			body.job_.readyOutput(cxt, *this);
		}

		continuable = (!restricted && taskProc.checkOperationContinuable(cxt));

		if ((remaining || operated) && !continuable && !restricted) {
			body.worker_.putTask(cxt, *this);
		}
	}
	while (false);
}

void PriorityTask::handleProcessorException(
		PriorityJobContext &cxt, std::exception &exception) {
	Body &body = getBody();
	handleProcessorExceptionDetail(cxt, body, exception);
}

void PriorityTask::encodeProcessor(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		JobMessages::StackOutStream &out) {
	Body &body = getBody();
	body.processor_.encode(guard, cxt, body.job_, out);
}

bool PriorityTask::getFetchContext(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		SQLFetchContext &fetchContext) {
	Body &body = getBody();
	return body.processor_.getFetchContext(guard, cxt, body.job_, fetchContext);
}

void PriorityTask::setDdlAckStatus(
		PriorityJobContext &cxt, const DdlAckStatus &ackStatus) {
	Body &body = getBody();
	body.processor_.setDdlAckStatus(ackStatus);
	body.worker_.putTask(cxt, *this);
}

TaskProfiler PriorityTask::getProfile(util::StackAllocator &alloc) {
	Body &body = getBody();

	util::LockGuard<util::Mutex> guard(body.lock_);
	return Profiler::getProfile(guard, alloc, body).profiler_;
}

void PriorityTask::getStatProfile(
		util::LockGuard<util::Mutex> &guard, StatTaskProfilerInfo &info) {
	Body &body = getBody();
	Profiler::getStatProfile(guard, *this, body, info);
}

void PriorityTask::getResourceProfile(
		util::LockGuard<util::Mutex> &guard, JobResourceInfo &info,
		bool byTask, bool limited) {
	Body &body = getBody();
	Profiler::getResourceProfile(guard, body, info, byTask, limited);
}

TaskProfilerInfo& PriorityTask::getProfileInfo(
		util::LockGuard<util::Mutex> &guard, util::StackAllocator &alloc) {
	Body &body = getBody();
	return Profiler::getProfile(guard, alloc, body);
}

void PriorityTask::setProfileInfo(
		util::LockGuard<util::Mutex> &guard, const TaskProfilerInfo &info) {
	Body &body = getBody();
	Profiler::setProfile(guard, body, info);
}

void PriorityTask::updateOutputProfile(bool increased) const {
	if (body_ != NULL) {
		body_->profiler_.updateOutputProfile(increased);
	}
}

void PriorityTask::getPlanJson(
		util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
		picojson::value &value, bool withProfile, const size_t *limit) {
	Body &body = getBody();

	body.processor_.getPlanJson(alloc, varAlloc, value, limit);

	if (withProfile) {
		TaskProfiler *profile;
		{
			util::LockGuard<util::Mutex> guard(body.lock_);
			profile = &Profiler::getProfile(guard, alloc, body).profiler_;
		}
		SQLProcessor::Profiler::mergeResultToPlan(alloc, *profile, value);
	}
}

void PriorityTask::writeTimeStrOptional(int64_t timestamp, util::String &str) {
	if (timestamp >= 0) {
		CommonUtility::writeTimeStr(util::DateTime(timestamp), str);
	}
	else {
		str.clear();
	}
}

const char8_t* PriorityTask::getOperationTypeName(OperationType op) {
	switch (op) {
	case OP_NONE:
		return "NONE";
	case OP_PIPE:
		return "PIPE";
	case OP_FINISH:
		return "FINISH";
	case OP_NEXT:
		return "NEXT";
	default:
		return "UNKNOWN";
	}
}

void PriorityTask::addReference() const {
	if (body_ != NULL) {
		++body_->refCount_;
	}
}

void PriorityTask::removeReference() {
	if (body_ != NULL) {
		if (--body_->refCount_ == 0) {
			PriorityJob job = body_->job_;
			{
				util::LockGuard<util::Mutex> guard(body_->lock_);
				ALLOC_DELETE(body_->alloc_, body_);
			}
		}
		body_ = NULL;
	}
}

PriorityTask::Body& PriorityTask::getBody() {
	if (body_ == NULL) {
		return errorEmptyBody();
	}
	return *body_;
}

const PriorityTask::Body& PriorityTask::getBody() const {
	if (body_ == NULL) {
		return errorEmptyBody();
	}
	return *body_;
}

bool PriorityTask::preparePipe(
		PriorityJobContext &cxt, Body &body, uint32_t &input,
		TupleList::Block &block, bool &busy, bool &withFinish) {
	input = std::numeric_limits<uint32_t>::max();
	busy = false;
	withFinish = false;
	do {
		{
			util::LockGuard<util::Mutex> guard(body.lock_);
			if (isAllOutputRestricted(guard, body) && !body.schemaCheckOnly_) {
				busy = true;
				return false;
			}
		}

		if (body.nextRemaining_) {
			return false;
		}

		if (body.processor_.checkInputBusy(cxt, body.job_, body.profiler_)) {
			busy = true;
			return false;
		}

		if (isInputEmpty(body)) {
			input = 0;
			return true;
		}

		PriorityTask inTask;
		bool inPending;
		if (findPreInputTask(body, inTask, inPending)) {
			if (popPendingBlockInput(inTask.getBody(), input, block, withFinish)) {
				break;
			}
			else if (inPending) {
				return false;
			}
		}

		if (!popPendingBlockInput(body, input, block, withFinish)) {
			return false;
		}
	}
	while (false);

	acceptBlockInput(cxt, body, input, false);
	getInputEntry(body, input).profiler_.acceptBlock(block);

	if (withFinish) {
		acceptInputFinish(cxt, body, input);
	}
	return true;
}

bool PriorityTask::prepareFinish(
		PriorityJobContext &cxt, Body &body, uint32_t &input, bool &blockFound) {
	input = std::numeric_limits<uint32_t>::max();
	blockFound = false;

	if (body.nextRemaining_) {
		return false;
	}

	do {
		PriorityTask inTask;
		bool inPending;
		if (findPreInputTask(body, inTask, inPending)) {
			if (popPendingFinalInput(inTask.getBody(), input, blockFound)) {
				break;
			}
			else if (inPending) {
				return false;
			}
		}

		if (!popPendingFinalInput(body, input, blockFound)) {
			return false;
		}
	}
	while (false);

	acceptInputFinish(cxt, body, input);
	return true;
}

bool PriorityTask::prepareNext(Body &body) {
	return body.nextRemaining_;
}

void PriorityTask::acceptInputFinish(
		PriorityJobContext &cxt, Body &body, uint32_t input) {
	getInputEntry(body, input).finishPrepared_ = true;
	body.finishedInputCount_++;

	if (body.activatedInputCount_ < body.inputList_.size()) {
		InputEntry &entry = getInputEntry(body, body.activatedInputCount_);

		if (updateInputCapacity(body, entry)) {
			PriorityTask inTask = body.job_.getTask(entry.taskId_);
			const uint32_t output = entry.output_;
			inTask.growOutputCapacity(cxt, output, entry.capacity_);
		}

		body.activatedInputCount_++;
	}
}

void PriorityTask::startOperation(
		PriorityJobContext &cxt, Body &body, SQLProcessor &proc,
		PriorityTask &task) {
	body.processor_.startProcessorOperation(cxt, task);
	body.profiler_.startProfilerOperation(cxt, proc);
}

void PriorityTask::finishOperation(PriorityJobContext &cxt, Body &body) {
	body.profiler_.finishProfilerOperation(cxt);
	body.processor_.finishProcessorOperation();

	body.processor_.applyStats(body.profiler_);

	PriorityJobStats prevStats;
	PriorityJobStats nextStats;
	body.profiler_.flipStats(prevStats, nextStats);
	body.job_.finishTaskOperation(cxt, prevStats, nextStats);
}

void PriorityTask::closeOperation(Body &body) {
	util::LockGuard<util::Mutex> guard(body.lock_);
	body.processor_.close(&guard);
	body.pendingBlockInputs_.clear();
	body.pendingFinalInputs_.clear();
}

bool PriorityTask::checkOperatable(Body &body, bool withActivity) {
	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		if (!isInputAllowed(guard, body) ||
				!isOutputAllowed(guard, body)) {
			return false;
		}

		if (withActivity && !body.active_ && !body.schemaCheckOnly_) {
			return false;
		}
	}

	if (body.processor_.isClosed()) {
		return false;
	}

	if (checkCancelSilently(body)) {
		return false;
	}

	return true;
}

bool PriorityTask::checkCancelSilently(Body &body) {
	try {
		body.job_.checkCancel();
		return false;
	}
	catch (...) {
	}
	return true;
}

void PriorityTask::handleProcessorExceptionDetail(
		PriorityJobContext &cxt, Body &body, std::exception &exception) {
	const util::Exception localException = GS_EXCEPTION_CONVERT(exception, "");
	body.job_.cancel(cxt, localException, NULL);

	body.profiler_.acceptError();
	finishOperation(cxt, body);
	closeOperation(body);
}

void PriorityTask::setProgressDetail(
		util::LockGuard<util::Mutex> &guard, PriorityTask &task,
		const TaskProgressKey &key, const TaskProgressValue *value) {
	Body &body = task.getBody();

	TaskProgressKey &curKey = body.progress_.first;
	TaskProgressValue &curValue = body.progress_.second;
	bool &assigned = body.progressAssigned_;

	const TaskProgressValue prevValueBase = curValue;
	const TaskProgressValue *prevValue = (assigned ? &prevValueBase : NULL);

	body.job_.setTaskProgressDirect(guard, task, curKey, prevValue, NULL);
	body.job_.setTaskProgressDirect(guard, task, key, NULL, value);

	if (value == NULL) {
		body.progress_ = TaskProgressEntry();
		assigned = false;
	}
	else {
		curKey = key;
		curValue = *value;
		assigned = true;
	}
}

void PriorityTask::acceptBlockInput(
		PriorityJobContext &cxt, Body &body, uint32_t input, bool forPre) {
	InputEntry &entry = getInputEntry(body, input);

	if (forPre) {
		++entry.preAcceptedSize_;
	}
	else if (entry.preAcceptedSize_ > 0) {
		--entry.preAcceptedSize_;
		return;
	}

	++entry.acceptedSize_;

	if (updateInputCapacity(body, entry)) {
		PriorityTask inTask = body.job_.getTask(entry.taskId_);
		const uint32_t output = entry.output_;
		inTask.growOutputCapacity(cxt, output, entry.capacity_);
	}
}

void PriorityTask::updateFinishState(
		PriorityJobContext &cxt, PriorityTask &task) {
	Body &body = task.getBody();

	bool ioAllowed;
	bool inputFinished;
	bool procClosed;
	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		ioAllowed = (
				isInputAllowed(guard, body) &&
				isOutputAllowed(guard, body));

		if (ioAllowed && !isOutputFinished(guard, body)) {
			return;
		}

		inputFinished = isInputFinished(body);
		procClosed = body.processor_.isClosed();
	}

	if (ioAllowed && !inputFinished && !body.outputList_.empty()) {
		const uint32_t inCount =
				static_cast<uint32_t>(body.inputList_.size());

		for (uint32_t i = 0; i < inCount; i++) {
			InputEntry &entry = getInputEntry(body, i);
			if (entry.finishPending_) {
				continue;
			}

			const uint64_t newCapacity = 0;
			{
				util::LockGuard<util::Mutex> guard(body.lock_);
				if (entry.finishPending_) {
					continue;
				}
				entry.capacity_ = newCapacity;
			}

			PriorityTask inTask = body.job_.getTask(entry.taskId_);
			const uint32_t output = entry.output_;
			inTask.growOutputCapacity(cxt, output, newCapacity);
		}
	}

	body.job_.finishTask(cxt, task, procClosed);
	task.closeOutput(cxt);

	if (!procClosed) {
		task.getWorker().putTask(cxt, task);
	}
}

bool PriorityTask::isInputFinished(Body &body) {
	return (
			!body.inputList_.empty() &&
			body.inputList_.size() == body.finishedInputCount_);
}

bool PriorityTask::isOutputFinished(util::LockGuard<util::Mutex>&, Body &body) {
	if (body.outputList_.size() == body.finishedOutputCount_) {
		return true;
	}

	const uint32_t outCount = static_cast<uint32_t>(body.outputList_.size());
	for (uint32_t i = 0; i < outCount; i++) {
		OutputEntry &entry = getOutputEntry(body, i);
		if (!entry.entryClosed_ || entry.capacity_ != 0) {
			return false;
		}
	}

	return true;
}

bool PriorityTask::popPendingBlockInput(
		Body &body, uint32_t &input, TupleList::Block &block,
		bool &withFinish) {
	util::LockGuard<util::Mutex> guard(body.lock_);
	withFinish = false;

	InputIdSet &idSet = body.pendingBlockInputs_;
	if (idSet.empty()) {
		return false;
	}

	InputIdSet::iterator it;
	if (body.priorInput_.get() == NULL ||
			(it = idSet.find(*body.priorInput_)) == idSet.end()) {
		it = idSet.begin();
	}
	input = *it;

	BlockList &blockList = getInputEntry(body, input).blockList_;
	block = blockList.front().toBufferedBlock();
	blockList.pop_front();
	body.profiler_.updateInputProfile(false);

	if (blockList.empty()) {
		idSet.erase(it);
		withFinish = (body.pendingFinalInputs_.erase(input) > 0);
	}

	return true;
}

bool PriorityTask::popPendingFinalInput(
		Body &body, uint32_t &input, bool &blockFound) {
	util::LockGuard<util::Mutex> guard(body.lock_);
	blockFound = false;

	InputIdSet &idSet = body.pendingFinalInputs_;
	if (idSet.empty()) {
		return false;
	}

	InputIdSet::iterator it = idSet.begin();
	input = *it;

	BlockList &blockList = getInputEntry(body, input).blockList_;
	if (!blockList.empty()) {
		blockFound = true;
		return false;
	}

	idSet.erase(it);
	return true;
}

void PriorityTask::checkInputAcceptable(InputEntry &entry) {
	if (entry.finishPending_ || entry.finishPrepared_) {
		errorInputSequence();
	}
}

bool PriorityTask::updateInputCapacity(Body &body, InputEntry &entry) {
	const PriorityJobConfig &config = body.job_.getConfig();
	const uint64_t desired = entry.acceptedSize_ + config.minTaskIOCache_;
	if (desired <= entry.capacity_) {
		return false;
	}
	entry.capacity_ = entry.acceptedSize_ + config.initialTaskIOCapacity_;
	return true;
}

void PriorityTask::applyOutputRestriction(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const OutputEntry &entry, bool prev) {
	const bool next = resolveOutputRestriction(guard, entry);
	if (!prev == !next) {
		return;
	}

	if (prev) {
		assert(body.restrictedOutputCount_ > 0);
		body.restrictedOutputCount_--;
	}

	if (next) {
		assert(body.restrictedOutputCount_ < body.outputList_.size());
		body.restrictedOutputCount_++;
	}
}

bool PriorityTask::resolveOutputRestriction(
		util::LockGuard<util::Mutex>&, const OutputEntry &entry) {
	return (entry.acceptedSize_ >= entry.capacity_);
}

bool PriorityTask::isAllOutputRestricted(
		util::LockGuard<util::Mutex>&, const Body &body) {
	return (body.restrictedOutputCount_ > 0 &&
			body.restrictedOutputCount_ >= body.outputList_.size());
}

void PriorityTask::addPreInputRequest(
		util::LockGuard<util::Mutex>&, Body &body) {
	if (!body.forPreInput_) {
		return;
	}

	body.requestedPreInputCount_++;
}

void PriorityTask::acceptPreInputRequest(
		Body &body, PriorityJobContext &cxt) {
	if (!body.forPreInput_) {
		return;
	}

	{
		util::LockGuard<util::Mutex> guard(body.lock_);
		if (++body.acceptedPreInputCount_ < body.requestedPreInputCount_) {
			return;
		}
	}

	PriorityTask mainTask;
	if (body.job_.findTask(body.mainId_, true, mainTask)) {
		mainTask.getWorker().putTask(cxt, mainTask);
	}
}

bool PriorityTask::findPreInputTask(
		Body &body, PriorityTask &task, bool &pending) {
	pending = false;

	if (!body.job_.findPreInputTask(body.id_, task)) {
		return false;
	}

	if (task.isPreInputTaskPending(task.getBody())) {
		pending = true;
	}
	return true;
}

bool PriorityTask::isPreInputTaskPending(Body &body) {
	util::LockGuard<util::Mutex> guard(body.lock_);

	if (body.forPreInput_) {
		if (body.acceptedPreInputCount_ < body.requestedPreInputCount_) {
			return true;
		}

		if (!body.pendingBlockInputs_.empty() ||
				!body.pendingFinalInputs_.empty()) {
			return true;
		}
	}

	return false;
}

PriorityJobWorker& PriorityTask::getOutputWorker(Body &body) {
	return body.outputWorker_;
}

bool PriorityTask::isInputEmpty(const Body &body) {
	return body.inputList_.empty();
}

JobMessages::TaskIOInfo PriorityTask::getIOInfo(const Body &body) {
	JobMessages::TaskIOInfo info;
	info.inputCount_ = static_cast<uint32_t>(body.inputList_.size());
	info.outputCount_ = static_cast<uint32_t>(body.outputList_.size());
	return info;
}

PriorityTask::InputEntry& PriorityTask::getInputEntry(
		Body &body, uint32_t input) {
	if (input >= body.inputList_.size()) {
		return errorInput(input);
	}
	return body.inputList_[input];
}

const PriorityTask::InputEntry& PriorityTask::getInputEntry(
		const Body &body, uint32_t input) {
	if (input >= body.inputList_.size()) {
		return errorInput(input);
	}
	return body.inputList_[input];
}

PriorityTask::OutputEntry& PriorityTask::getOutputEntry(
		Body &body, uint32_t output) {
	if (output >= body.outputList_.size()) {
		return errorOutput(output);
	}
	return body.outputList_[output];
}

const PriorityTask::OutputEntry& PriorityTask::getOutputEntry(
		const Body &body, uint32_t output) {
	if (output >= body.outputList_.size()) {
		return errorOutput(output);
	}
	return body.outputList_[output];
}

bool PriorityTask::isInputAllowed(
		util::LockGuard<util::Mutex>&, const Body &body) {
	return !body.inputClosed_;
}

bool PriorityTask::isOutputAllowed(
		util::LockGuard<util::Mutex>&, const Body &body) {
	return !body.outputClosed_;
}

PriorityTask::Body& PriorityTask::errorEmptyBody() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Empty task body");
}

bool PriorityTask::errorInputSequence() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INVALID_RECV_BLOCK_NO, "");
}

PriorityTask::InputEntry& PriorityTask::errorInput(uint32_t input) {
	GS_THROW_USER_ERROR_TRACE(
			GS_ERROR_JOB_INTERNAL,
			"Task input out of range (input=" << input << ")");
}

PriorityTask::OutputEntry& PriorityTask::errorOutput(uint32_t output) {
	GS_THROW_USER_ERROR_TRACE(
			GS_ERROR_JOB_INTERNAL,
			"Task output out of range (input=" << output << ")");
}

TaskId PriorityTask::errorTaskId() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task ID");
}

const TaskInfo& PriorityTask::errorTaskInfo() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task info");
}

const JobNode& PriorityTask::errorNode() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid node info");
}

SQLProcessor& PriorityTask::errorProcessorData() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task processor data");
}

JobMessages::ByteInStream PriorityTask::errorProcessorDataAsStream() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Invalid task processor data");
}

void PriorityTask::errorProcessorClosed() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Task processor already closed");
}


PriorityTask::Source::Source(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		PriorityJobController &controller, PriorityJobAllocator &alloc,
		util::Mutex &lock, PriorityJob &job, bool onCoordinator) :
		guard_(guard),
		cxt_(cxt),
		controller_(controller),
		alloc_(alloc),
		lock_(lock),
		job_(job),

		baseId_(JobManager::UNDEF_TASKID),
		jobInfo_(NULL),
		info_(NULL),
		nodeList_(NULL),
		syncContext_(NULL),

		onCoordinator_(onCoordinator),
		forPreInput_(false),
		profiling_(false),
		failOnMemoryExcess_(false),
		option_(cxt.getAllocator()) {
}

TaskId PriorityTask::Source::resolveId() const {
	if (forPreInput_) {
		return JobManager::UNDEF_TASKID;
	}
	else {
		if (baseId_ < 0) {
			return errorTaskId();
		}
		return baseId_;
	}
}

TaskId PriorityTask::Source::resolveMainId() const {
	if (forPreInput_) {
		if (baseId_ < 0) {
			return errorTaskId();
		}
		return baseId_;
	}
	else {
		return JobManager::UNDEF_TASKID;
	}
}

JobNode PriorityTask::Source::resolveNode() const {
	if (forPreInput_) {
		return controller_.getTransporter().getSelfNode();
	}

	const TaskInfo &info = resolveInfo();
	const int32_t nodePos = info.nodePos_;

	if (nodeList_ == NULL || nodePos < 0 ||
			static_cast<size_t>(nodePos) >= nodeList_->size()) {
		return errorNode();
	}

	return (*nodeList_)[nodePos];
}

PriorityJobWorker& PriorityTask::Source::resolveWorker() const {
	if (forPreInput_) {
		return resolveOutputWorker();
	}

	const TaskInfo &info = resolveInfo();
	const bool forBackend = (
			SQLProcessor::DQLTool::isDQL(info.sqlType_) &&
			info.sqlType_ != SQLType::EXEC_SCAN);
	return controller_.getWorkerTable().getTaskWorker(
			info.type_, resolveLoadBalance(), forBackend);
}

PriorityJobWorker& PriorityTask::Source::resolveOutputWorker() const {
	return controller_.getWorkerTable().getBackendIOWorker();
}

int32_t PriorityTask::Source::resolveLoadBalance() const {
	const TaskInfo &info = resolveInfo();
	return info.loadBalance_;
}

PriorityTask::InputList PriorityTask::Source::makeInitialInputList() const {
	const PriorityJobConfig &config = job_.getConfig();

	const TaskInfo::ResourceList &src = resolveInfo().inputList_;
	const uint32_t count = static_cast<uint32_t>(src.size());

	InputList dest(alloc_);
	for (uint32_t i = 0; i < count; i++) {
		const TaskId taskId = static_cast<TaskId>(src[i]);
		dest.push_back(InputEntry(alloc_, config, taskId));
	}

	return dest;
}

SQLType::Id PriorityTask::Source::resolveType() const {
	const TaskId id = resolveId();
	if (id == JobManager::UNDEF_TASKID) {
		return SQLType::START_EXEC;
	}

	assert(jobInfo_ != NULL && jobInfo_->nodeList_ != NULL);
	return (*jobInfo_->nodeList_)[id].type_;
}

const TaskInfo& PriorityTask::Source::resolveInfo() const {
	if (info_ == NULL) {
		return errorTaskInfo();
	}
	return *info_;
}

size_t PriorityTask::Source::resolveReservationMemorySize() const {
	const PriorityJobConfig &config = job_.getConfig();

	const uint64_t baseSize = static_cast<uint64_t>(
			static_cast<double>(config.workMemoryLimit_) *
			std::max(config.workMemoryReservationRate_, 0.0));

	return static_cast<size_t>(std::min(
			baseSize,
			static_cast<uint64_t>(std::numeric_limits<int32_t>::max())));
}


PriorityTask::InputOption::InputOption(bool networkBusy, uint64_t sequenceNum) :
		networkBusy_(networkBusy),
		sequenceNum_(sequenceNum) {
}

PriorityTask::InputOption PriorityTask::InputOption::empty() {
	return InputOption(false, 0);
}


PriorityTask::InterruptionHandler::InterruptionHandler(
		SQLContext &procCxt, const PriorityJob &job) :
		procCxt_(procCxt),
		job_(job),
		intervalMillis_(JobManager::DEFAULT_TASK_INTERRUPTION_INTERVAL) {
}

PriorityTask::InterruptionHandler::~InterruptionHandler() {
}

bool PriorityTask::InterruptionHandler::operator()(
		InterruptionChecker &checker, int32_t flags) {
	UNUSED_VARIABLE(checker);
	UNUSED_VARIABLE(flags);

	EventContext *ec = procCxt_.getEventContext();
	job_.checkCancel();

	return ec->getEngine().getMonotonicTime() -
			ec->getHandlerStartMonotonicTime() > intervalMillis_;
}


PriorityTask::Processor::Processor(const Source &source) :
		lock_(source.lock_),
		controller_(source.controller_),
		type_(source.resolveType()),
		baseInputInfo_(makeBaseInputInfo(source)),
		data_(source.processorData_),
		planData_(source.alloc_),
		reservationMemorySize_(source.resolveReservationMemorySize()),
		failOnTotalMemoryLimit_(isFailOnTotalMemoryLimit(source, type_)),
		closed_(false),
		event_(
				Event::Source(source.alloc_), PRIORITY_JOB_EXECUTE,
				source.resolveLoadBalance()),
		syncContext_(source.syncContext_),
		localJobId_(source.job_.getId()) {
	if (isBaseRequired(source)) {
		prepareDetail(source.cxt_, source.job_, &source);
	}
	if (isPlanDataRequired(source)) {
		preparePlanData(source);
	}
}

PriorityTask::Processor::~Processor() {
	close(NULL);
}

void PriorityTask::Processor::close(util::LockGuard<util::Mutex>*) {
	closed_ = true;

	inputInfo_.reset();
	groupId_.reset();
	localGroup_.reset();
	base_.reset();
	procCxt_.reset();
	interruptionHandler_.reset();

	allocScope_.reset();
	varAlloc_.reset();
	stackAlloc_.reset();
}

bool PriorityTask::Processor::isClosed() const {
	return closed_;
}

SQLProcessor& PriorityTask::Processor::prepare(
		PriorityJobContext &cxt, const PriorityJob &job) {
	return prepareDetail(cxt, job, NULL);
}

SQLContext& PriorityTask::Processor::prepareContext(const PriorityJob &job) {
	if (procCxt_.get() == NULL) {
		checkNotClosed();
		PriorityJobEnvironment &env = controller_.getEnvironment();
		util::AllocatorLimitter &allocLimitter = prepareAllocatorLimitter(job);
		procCxt_ = ALLOC_UNIQUE(
				prepareVarAllocator(allocLimitter), SQLContext,
				&prepareStackAllocator(allocLimitter),
				&prepareVarAllocator(allocLimitter),
				*env.getStore(), env.getProcessorConfig());
		setUpContext(*procCxt_, job, allocLimitter);
	}
	return *procCxt_;
}

void PriorityTask::Processor::applyContext(
		util::LockGuard<util::Mutex> &guard, PriorityTask &task) {
	if (procCxt_.get() == NULL) {
		return;
	}

	TaskProgressKey key;
	if (procCxt_->findInitialProgress(key)) {
		const TaskProgressValue value = 0;
		setProgressDetail(guard, task, key, &value);
	}
}

void PriorityTask::Processor::encode(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		const PriorityJob &job, JobMessages::StackOutStream &out) {
	const size_t startPos = out.base().position();
	out << static_cast<uint32_t>(0);
	const size_t bodyPos = out.base().position();

	encodeBase(guard, cxt, job, out);
	encodePlan(out);

	const size_t endPos = out.base().position();
	const uint32_t bodySize = static_cast<uint32_t>(endPos - bodyPos);

	out.base().position(startPos);
	out << bodySize;
	out.base().position(endPos);
}

void PriorityTask::Processor::getPlanString(
		PriorityJobAllocator &varAlloc, util::String &str, bool limited) const {
	util::StackAllocator &alloc = *str.get_allocator().base();
	picojson::value value;
	getPlanJson(alloc, varAlloc, value, NULL);

	util::String fullStr(alloc);
	const size_t baseLimit = getColumnValueLimit();
	const size_t *limit = (limited ? &baseLimit : NULL);
	JsonUtils::serializeWithLimit(
			value, fullStr, std::back_inserter(str), limit, NULL,
			Constants::PLAN_STRING_KEYS);
}

void PriorityTask::Processor::getPlanJson(
		util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
		picojson::value &value, const size_t *limit) const {
	JobMessages::ByteInStream in = getPlanInStream();
	LocalTempStore::Group &group =
			controller_.getEnvironment().getDefaultStoreGroup();

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);
	varCxt.setVarAllocator(&varAlloc);
	varCxt.setGroup(&group);

	TupleValue::VarContext::Scope varScope(varCxt);

	SQLPreparedPlan::Node *node = NULL;
	TupleValue::coder(util::ObjectCoder().withAllocator(alloc), &varCxt).decode(
			in, node);

	JsonUtils::OutStream out(value);
	TupleValue::coder(util::ObjectCoder(), &varCxt).encode(out, node);

	if (limit != NULL) {
		value = JsonUtils::limitedValueForSerialize(
				value, *limit, NULL, Constants::PLAN_STRING_KEYS);
	}
}

void PriorityTask::Processor::formatStatement(
		const char8_t *src, util::String &dest, bool limited) const {
	if (limited) {
		util::NormalOStringStream oss;
		oss << LimitedQueryStringFormatter(src, getColumnValueLimit());
		dest = oss.str().c_str();
	}
	else {
		dest = src;
	}
}

SQLType::Id PriorityTask::Processor::getType() const {
	return type_;
}

void PriorityTask::Processor::startProcessorOperation(
		PriorityJobContext &cxt, PriorityTask &task) {
	assert(!closed_);
	procCxt_->setEventContext(cxt.getEventContext());
	procCxt_->setPriorityTask(&task);

	allocLimitter_->growReservation(reservationMemorySize_);
	if (failOnTotalMemoryLimit_) {
		allocLimitter_->setFailOnExcess(true);
	}
}

void PriorityTask::Processor::finishProcessorOperation() {
	if (allocLimitter_.get() == NULL) {
		return;
	}

	if (failOnTotalMemoryLimit_) {
		allocLimitter_->setFailOnExcess(false);
	}
	allocLimitter_->shrinkReservation(reservationMemorySize_);
}

bool PriorityTask::Processor::checkOperationContinuable(
		PriorityJobContext &cxt) {
	PriorityJobEnvironment &env = controller_.getEnvironment();
	return env.checkEventContinuable(
			cxt.getEventContext(), cxt.getSourceEvent());
}

void PriorityTask::Processor::applyStats(Profiler &profiler) {
	applyMemoryStats(profiler);
	applyTempStoreStats(profiler);
}

void PriorityTask::Processor::applyStoreGroup(
		util::LockGuard<util::Mutex>&, const TupleList::Block &block) {
	if (groupId_.get() != NULL) {
		block.setGroupId(*groupId_);
	}
}

void PriorityTask::Processor::setDdlAckStatus(const DdlAckStatus &ackStatus) {
	util::LockGuard<util::Mutex> guard(lock_);
	if (ddlAckStatusList_.get() != NULL) {
		ddlAckStatusList_->push_back(ackStatus);
	}
}

bool PriorityTask::Processor::checkInputBusy(
		PriorityJobContext &cxt, const PriorityJob &job, Profiler &profiler) {
	if (isClosed()) {
		return false;
	}

	if (isResultType(getType())) {
		if (!job.isResultOutputReady()) {
			profiler.acceptInputBusy(true);
			return true;
		}

		SQLProcessor &proc = prepareDetail(cxt, job, NULL);
		ResultProcessor &resultProc = static_cast<ResultProcessor&>(proc);
		TupleList::Reader *reader = resultProc.getReader();

		const bool inputBusy = (
				reader->exists() &&
				reader->getFollowingBlockCount() >
						controller_.getConfig().initialTaskIOCapacity_);

		profiler.acceptInputBusy(inputBusy);
		return inputBusy;
	}
	else if (isDdlType(getType())) {
		SQLProcessor &proc = prepareDetail(cxt, job, NULL);

		DDLProcessor &ddlProc = static_cast<DDLProcessor&>(proc);
		const bool inputBusy = !applyDdlAckStatus(ddlProc);

		return inputBusy;
	}
	else {
		return false;
	}
}

bool PriorityTask::Processor::getFetchContext(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		const PriorityJob &job, SQLFetchContext &fetchContext) {
	if (!isResultType(getType())) {
		assert(false);
		return false;
	}

	if (isClosed()) {
		return false;
	}

	SQLProcessor &proc = prepareDetail(cxt, job, NULL);
	ResultProcessor &resultProc = static_cast<ResultProcessor&>(proc);
	fetchContext.reader_ = resultProc.getReader();
	fetchContext.columnList_ = resultProc.getColumnInfoList();
	fetchContext.columnSize_ = resultProc.getColumnSize();
	fetchContext.isCompleted_ = false;
	return true;
}

bool PriorityTask::Processor::isSchemaCheckRequiredType(
		SQLType::Id type, uint8_t serviceType) {
	return (type == SQLType::EXEC_SCAN && serviceType == TRANSACTION_SERVICE);
}

bool PriorityTask::Processor::isResultType(SQLType::Id type) {
	return (type == SQLType::EXEC_RESULT);
}

bool PriorityTask::Processor::isDdlType(SQLType::Id type) {
	return (type == SQLType::EXEC_DDL);
}

util::StackAllocator& PriorityTask::Processor::prepareStackAllocator(
		util::AllocatorLimitter &allocLimitter) {
	if (stackAlloc_.get() == NULL) {
		stackAlloc_ = UTIL_MAKE_LOCAL_UNIQUE(
				stackAlloc_, PriorityJobHolders::Allocator,
				controller_.getHoldersPool(), &allocLimitter);
		allocScope_ = UTIL_MAKE_LOCAL_UNIQUE(
				allocScope_, util::StackAllocator::Scope, (*stackAlloc_).get());
	}
	return (*stackAlloc_).get();
}

PriorityJobAllocator& PriorityTask::Processor::prepareVarAllocator(
		util::AllocatorLimitter &allocLimitter) {
	if (varAlloc_.get() == NULL) {
		varAlloc_ = UTIL_MAKE_LOCAL_UNIQUE(
				varAlloc_, PriorityJobHolders::VarAllocator,
				controller_.getHoldersPool(), &allocLimitter, true);
	}
	return (*varAlloc_).get();
}

bool PriorityTask::Processor::isFailOnTotalMemoryLimit(
		const Source &source, SQLType::Id type) {
	return (!isResultType(type) && source.failOnMemoryExcess_);
}

bool PriorityTask::Processor::isBaseRequired(const Source &source) const {
	return (!source.forPreInput_ && (source.onCoordinator_ ||
			(type_ == SQLType::EXEC_SCAN && source.resolveNode().isSelf())));
}

bool PriorityTask::Processor::isPlanDataRequired(const Source &source) const {
	return (!source.forPreInput_ && source.onCoordinator_);
}

SQLProcessor& PriorityTask::Processor::prepareDetail(
		PriorityJobContext &cxt, const PriorityJob &job,
		const Source *source) {
	if (base_.get() == NULL) {
		checkNotClosed();
		util::AllocatorLimitter &allocLimitter = prepareAllocatorLimitter(job);
		SQLContext &procCxt = prepareContext(job);
		base_ = UTIL_MAKE_LOCAL_UNIQUE(
				base_, SQLProcessor::Holder,
				createBase(cxt, procCxt, source, allocLimitter));
		setUpBase(base_->get(), job);
	}

	SQLProcessor &proc = base_->get();
	if (isResultType(type_)) {
		ResultProcessor &resultProc = static_cast<ResultProcessor&>(proc);
		TupleList::Reader *reader = resultProc.getReader();
		reader->exists();
	}

	return proc;
}

SQLProcessor& PriorityTask::Processor::createBase(
		PriorityJobContext &cxt, SQLContext &procCxt, const Source *source,
		util::AllocatorLimitter &allocLimitter) {
	SQLProcessor::Factory factory;
	SQLProcessor *base;
	TupleInfoList &inputInfo = prepareInputInfo(allocLimitter);

	procCxt.setEventContext(cxt.getEventContext());
	if (source != NULL && source->option_.plan_ != NULL) {
		TaskOption option = source->option_;
		option.planNodeId_ = static_cast<uint32_t>(source->resolveId());
		base = factory.create(procCxt, option, inputInfo);
	}
	else {
		if (data_.second == NULL) {
			return errorProcessorData();
		}
		JobMessages::ByteInStream in = getBaseInStream();
		base = NULL;
		SQLProcessor::makeCoder(
				util::ObjectCoder().withAllocator(
						prepareStackAllocator(allocLimitter)),
				procCxt, &factory, &inputInfo).decode(in, base);
		if (base == NULL) {
			return errorProcessorData();
		}
	}
	procCxt.setEventContext(NULL);

	return *base;
}

void PriorityTask::Processor::setUpBase(
		SQLProcessor &base, const PriorityJob &job) {
	if (isResultType(type_)) {
		const PriorityTaskConfig &taskConfig = job.getTaskConfig();
		static_cast<ResultProcessor&>(base).getProfs().taskNum_ =
				static_cast<int32_t>(taskConfig.taskCount_);
	}
}

void PriorityTask::Processor::setUpContext(
		SQLContext &procCxt, const PriorityJob &job,
		util::AllocatorLimitter &allocLimitter) {

	procCxt.setInterruptionHandler(
			&prepareInterruptionHandler(procCxt, job));
	procCxt.setAllocatorLimitter(&allocLimitter);

	setUpContextEnvironment(procCxt, controller_.getEnvironment());
	setUpContextTaskInfo(procCxt, job);
}

void PriorityTask::Processor::setUpContextEnvironment(
		SQLContext &procCxt, PriorityJobEnvironment &env) {
	procCxt.setClusterService(env.getClusterService());
	procCxt.setPartitionTable(env.getPartitionTable());

	procCxt.setTransactionService(env.getTransactionService());
	procCxt.setTransactionManager(env.getTransactionManager());
	procCxt.setDataStoreConfig(env.getDataStoreConfig());

	procCxt.setPartitionList(env.getPartitionList());
	procCxt.setExecutionManager(env.getExecutionManager());
	procCxt.setJobManager(env.getJobManager());
}

void PriorityTask::Processor::setUpContextTaskInfo(
		SQLContext &procCxt, const PriorityJob &job) {
	const PriorityTaskConfig &taskConfig = job.getTaskConfig();

	procCxt.setClientId(&localJobId_.clientId_);
	procCxt.setExecId(localJobId_.execId_);
	procCxt.setVersionId(localJobId_.versionId_);
	procCxt.setEvent(&event_);
	procCxt.setSyncContext(syncContext_);

	procCxt.setStoreMemoryAgingSwapRate(
			taskConfig.storeMemoryAgingSwapRate_);

	procCxt.setTimeZone(taskConfig.timeZone_);

	if (taskConfig.startTimeRequired_) {
		procCxt.setJobStartTime(taskConfig.startTime_);
	}
	procCxt.setAdministrator(taskConfig.forAdministrator_);

	procCxt.setForResultSet(taskConfig.forResultSet_);
	procCxt.setProfiling(taskConfig.forAnalysis_);

	if (JobManager::TaskInfo::isDml(type_) &&
			taskConfig.containerType_ == TIME_SERIES_CONTAINER) {
		procCxt.setDMLTimeSeries();
	}
}

PriorityTask::InterruptionHandler&
PriorityTask::Processor::prepareInterruptionHandler(
		SQLContext &procCxt, const PriorityJob &job) {
	if (interruptionHandler_.get() == NULL) {
		interruptionHandler_ = UTIL_MAKE_LOCAL_UNIQUE(
				interruptionHandler_, InterruptionHandler, procCxt, job);
	}
	return *interruptionHandler_;
}

PriorityTask::TupleInfoList& PriorityTask::Processor::prepareInputInfo(
		util::AllocatorLimitter &allocLimitter) {
	if (inputInfo_.get() == NULL) {
		util::StackAllocator &stackAlloc = prepareStackAllocator(allocLimitter);
		inputInfo_ = UTIL_MAKE_LOCAL_UNIQUE(
				inputInfo_, TupleInfoList, stackAlloc);
		for (AllocTupleInfoList::iterator it = baseInputInfo_.begin();
				it != baseInputInfo_.end(); ++it) {
			inputInfo_->push_back(
					TupleInfo(it->begin(), it->end(), stackAlloc));
		}
	}
	return *inputInfo_;
}

PriorityTask::AllocTupleInfoList PriorityTask::Processor::makeBaseInputInfo(
		const Source &source) {
	PriorityJobAllocator &alloc = source.alloc_;
	AllocTupleInfoList list(alloc);

	const TupleInfoList &infoList = source.info_->inputTypeList_;
	for (TupleInfoList::const_iterator it = infoList.begin();
			it != infoList.end(); ++it) {
		list.push_back(AllocTupleInfo(it->begin(), it->end(), alloc));
	}

	return list;
}

util::AllocatorLimitter& PriorityTask::Processor::prepareAllocatorLimitter(
		const PriorityJob &job) {
	if (allocLimitter_.get() == NULL) {
		util::AllocatorLimitter &base = job.getAllocatorLimitter();
		allocLimitter_ = UTIL_MAKE_LOCAL_UNIQUE(
				allocLimitter_, util::AllocatorLimitter,
				util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, "priorityTask"),
				&base);
	}
	return *allocLimitter_;
}

void PriorityTask::Processor::preparePlanData(const Source &source) {
	util::StackAllocator &alloc = source.cxt_.getAllocator();
	LocalTempStore::Group &group =
			controller_.getEnvironment().getDefaultStoreGroup();

	typedef util::XArrayOutStream<
			util::StdAllocator<uint8_t, void> > OutStream;
	util::ByteStream<OutStream> out((OutStream(planData_)));

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);
	varCxt.setVarAllocator(&source.alloc_);
	varCxt.setGroup(&group);

	TupleValue::VarContext::Scope varScope(varCxt);

	const SQLPreparedPlan *plan = source.option_.plan_;
	const SQLPreparedPlan::Node *node = NULL;
	const uint32_t index = static_cast<uint32_t>(source.baseId_);
	if (plan != NULL && index < plan->nodeList_.size()) {
		node = &plan->nodeList_[index];
	}
	TupleValue::coder(util::ObjectCoder(), &varCxt).encode(out, node);
}

bool PriorityTask::Processor::applyDdlAckStatus(DDLProcessor &proc) {
	util::LockGuard<util::Mutex> localGuard(lock_);

	if (ddlAckStatusList_.get() == NULL) {
		const util::StdAllocator<void, void> &alloc =
				baseInputInfo_.get_allocator();
		ddlAckStatusList_ = UTIL_MAKE_LOCAL_UNIQUE(
				ddlAckStatusList_, DdlAckStatusList, alloc);
		return true;
	}
	else {
		if (ddlAckStatusList_->empty()) {
			return false;
		}

		const DdlAckStatus &elem = ddlAckStatusList_->back();
		{
			NoSQLContainer *containerInfo = NULL;
			const int32_t pos = elem.first;
			const int32_t status = elem.second;
			proc.setAckStatus(containerInfo, pos, status);
		}
		ddlAckStatusList_->pop_back();
		return true;
	}
}

JobMessages::ByteInStream PriorityTask::Processor::getBaseInStream() const {
	JobMessages::ByteInStream in = getTotalInStream();

	uint32_t bodySize;
	in >> bodySize;

	const size_t bodyPos = in.base().position();
	const uint8_t *addr = static_cast<const uint8_t*>(data_.second) + bodyPos;

	return JobMessages::ByteInStream(util::ArrayInStream(addr, bodySize));
}

JobMessages::ByteInStream PriorityTask::Processor::getPlanInStream() const {
	if (!planData_.empty()) {
		return JobMessages::ByteInStream(
				util::ArrayInStream(planData_.data(), planData_.size()));
	}

	JobMessages::ByteInStream baseIn = getBaseInStream();

	JobMessages::ByteInStream in = getTotalInStream();
	in.base().position(sizeof(uint32_t) + baseIn.base().remaining());

	uint32_t bodySize;
	in >> bodySize;

	const size_t bodyPos = in.base().position();
	const uint8_t *addr = static_cast<const uint8_t*>(data_.second) + bodyPos;

	return JobMessages::ByteInStream(util::ArrayInStream(addr, bodySize));
}

JobMessages::ByteInStream PriorityTask::Processor::getTotalInStream() const {
	if (data_.second == NULL) {
		return errorProcessorDataAsStream();
	}

	return JobMessages::ByteInStream(
			(util::ArrayInStream(data_.second, data_.first)));
}

void PriorityTask::Processor::encodeBase(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		const PriorityJob &job, JobMessages::StackOutStream &out) {
	const size_t startPos = out.base().position();
	out << static_cast<uint32_t>(0);
	const size_t bodyPos = out.base().position();

	SQLProcessor *proc = &prepareDetail(cxt, job, NULL);
	SQLProcessor::makeCoder(
			util::ObjectCoder(), prepareContext(job), NULL, NULL).encode(
					out, proc);

	const size_t endPos = out.base().position();
	const uint32_t bodySize = static_cast<uint32_t>(endPos - bodyPos);

	out.base().position(startPos);
	out << bodySize;
	out.base().position(endPos);
}

void PriorityTask::Processor::encodePlan(JobMessages::StackOutStream &out) {
	const uint32_t bodySize = static_cast<uint32_t>(planData_.size());
	out << bodySize;
	out << std::make_pair(planData_.data(), bodySize);
}

void PriorityTask::Processor::applyMemoryStats(Profiler &profiler) {
	const int64_t usage = (allocLimitter_.get() == NULL ?
			0 : static_cast<int64_t>(allocLimitter_->getUsageSize()));
	profiler.acceptMemoryStats(usage);
}

void PriorityTask::Processor::applyTempStoreStats(Profiler &profiler) {
	if (groupId_.get() == NULL) {
		SQLProcessor::Context *procCxt = procCxt_.get();
		if (closed_ || procCxt == NULL) {
			return;
		}

		util::LockGuard<util::Mutex> guard(lock_);
		if (procCxt->isSetGroup()) {
			groupId_ = UTIL_MAKE_LOCAL_UNIQUE(
					groupId_, LocalTempStore::GroupId,
					procCxt->getProcessorGroupId());
		}
		else {
			localGroup_ = UTIL_MAKE_LOCAL_UNIQUE(
					localGroup_, LocalTempStore::Group,
					*controller_.getEnvironment().getStore());
			groupId_ = UTIL_MAKE_LOCAL_UNIQUE(
					groupId_, LocalTempStore::GroupId, localGroup_->getId());
		}
	}

	const LocalTempStore::GroupId groupId = *groupId_;
	LocalTempStore *store = controller_.getEnvironment().getStore();

	util::LockGuard<util::Mutex> guard(store->getGroupStatsMapMutex());
	const LocalTempStore::GroupStatsMap &map = store->getGroupStatsMap();

	LocalTempStore::GroupStatsMap::const_iterator it = map.find(groupId);
	if (it == map.end()) {
		return;
	}

	const uint32_t blockSize = store->getDefaultBlockSize();
	profiler.acceptTempStoreStats(it->second, blockSize);
}

uint32_t PriorityTask::Processor::getColumnValueLimit() const {
	SQLExecutionManager *execMgr =
			controller_.getEnvironment().getExecutionManager();
	return execMgr->getManagerSet()->dsConfig_->getLimitSmallSize();
}

void PriorityTask::Processor::checkNotClosed() {
	if (closed_) {
		errorProcessorClosed();
	}
}


PriorityTask::Profiler::Profiler(const Source &source) :
		enabled_(source.profiling_),
		inputBusy_(false),
		opWorking_(false),
		errorOccurred_(false),
		leadTime_(0),
		actualTime_(0),
		executionCount_(0),
		customData_(source.alloc_),
		procStartTimestamp_(-1),
		opCheckTimestamp_(-1),
		transCount_(0),
		busyTransCount_(0),
		lastMemoryUsage_(0),
		peakMemoryUsage_(0),
		activeBlockCount_(0),
		swapInCount_(0),
		swapOutCount_(0),
		plStats_(preparePartitionListStats(source)),
		chunkSize_(prepareChunkSize(plStats_, source)),
		bufStats_(prepareChunkBufferStats(plStats_, source)) {
}

void PriorityTask::Profiler::finishProcessor(
		const Body &body, SQLProcessor &proc) {
	if (enabled_) {
		acceptCustomProfile(body, proc);
	}

	procWatch_.stop();
}

void PriorityTask::Profiler::startProfilerOperation(
		PriorityJobContext &cxt, SQLProcessor &proc) {
	if (executionCount_ == 0) {
		startProcessor(cxt, proc);
	}
	++executionCount_;
	opWorking_ = true;
	opWatch_.start();

	if (plStats_ != NULL) {
		startChunkBufferOperation(cxt, *plStats_);
	}
}

void PriorityTask::Profiler::finishProfilerOperation(PriorityJobContext &cxt) {
	opWatch_.stop();
	opWorking_ = false;

	stats_.setValueAt(
			PriorityJobStats::TYPE_LEAD_TIME,
			static_cast<int64_t>(procWatch_.elapsedMillis()));
	stats_.setValueAt(
			PriorityJobStats::TYPE_ACTUAL_TIME,
			static_cast<int64_t>(opWatch_.elapsedMillis()));

	if (plStats_ != NULL) {
		finishChunkBufferOperation(cxt, *plStats_);
	}
}

void PriorityTask::Profiler::acceptError() {
	errorOccurred_ = true;
}

void PriorityTask::Profiler::acceptInputBusy(bool inputBusy) {
	inputBusy_ = inputBusy;
}

void PriorityTask::Profiler::acceptOperationCheck(PriorityJobContext &cxt) {
	opCheckTimestamp_ =
			cxt.resolveEventContext().getHandlerStartTime().getUnixTime();
}

void PriorityTask::Profiler::flipStats(
		PriorityJobStats &prev, PriorityJobStats &next) {
	prev = prevStats_;
	next = stats_;

	prevStats_ = stats_;
}

void PriorityTask::Profiler::startTransfer(
		util::LockGuard<util::Mutex>&, bool networkBusy) {
	transCount_++;
	transWatch_.start();

	if (networkBusy) {
		busyTransCount_++;
		busyTransWatch_.start();
	}
}

void PriorityTask::Profiler::finishTransfer(
		util::LockGuard<util::Mutex>&, bool networkBusy, uint64_t transSize) {
	if (--transCount_ <= 0) {
		transWatch_.stop();
	}

	if (networkBusy && --busyTransCount_ <= 0) {
		busyTransWatch_.stop();
	}

	stats_.setValueAt(
			PriorityJobStats::TYPE_TRANS_ACTIVE_TIME,
			transWatch_.elapsedMillis());
	stats_.setValueAt(
			PriorityJobStats::TYPE_TRANS_BUSY_TIME,
			busyTransWatch_.elapsedMillis());

	stats_.addValueAt(PriorityJobStats::TYPE_TRANS_SIZE, transSize);
}

void PriorityTask::Profiler::acceptMemoryStats(int64_t usage) {
	stats_.setValueAt(PriorityJobStats::TYPE_MEMORY_USE, usage);
}

void PriorityTask::Profiler::acceptTempStoreStats(
		const LocalTempStore::GroupStats &stats, uint32_t blockSize) {
	activeBlockCount_ = stats.activeBlockCount_;
	swapInCount_ = stats.swapInCount_;
	swapOutCount_ = stats.swapOutCount_;

	const int64_t inCount = std::max<int64_t>(inputBlockCount_, 0);
	const int64_t outCount = std::max<int64_t>(outputBlockCount_, 0);

	const int64_t lastCount = stats.activeBlockCount_ + inCount + outCount;
	const int64_t peakCount =
			std::max<int64_t>(stats.maxActiveBlockCount_, lastCount);

	const int64_t lastUse = lastCount * blockSize;
	const int64_t peakUse = peakCount * blockSize;

	stats_.setValueAt(PriorityJobStats::TYPE_SQL_STORE_USE_LAST, lastUse);
	stats_.setPeakValueAt(PriorityJobStats::TYPE_SQL_STORE_USE_PEAK, peakUse);
}

void PriorityTask::Profiler::updateInputProfile(bool increased) {
	if (increased) {
		++inputBlockCount_;
	}
	else {
		--inputBlockCount_;
	}
}

void PriorityTask::Profiler::updateOutputProfile(bool increased) {
	if (increased) {
		++outputBlockCount_;
	}
	else {
		--outputBlockCount_;
	}
}

void PriorityTask::Profiler::getStatProfile(
		util::LockGuard<util::Mutex> &guard, const PriorityTask &task,
		const Body &body, StatTaskProfilerInfo &info) {
	const Profiler &profiler = body.profiler_;
	util::StackAllocator *alloc = info.name_.get_allocator().base();

	info.id_ = body.id_;
	info.inputId_ = -1;
	info.name_ = SQLType::Coder()(body.processor_.getType(), "");
	PriorityJob::Types::taskStatusFlagsToString(
			body.profiler_.resolveStatusFlags(guard, body), info.status_);
	info.counter_ = profiler.executionCount_;
	info.sendPendingCount_ = -1;
	info.dispatchCount_ = -1;
	info.sendPendingCount_ = static_cast<int64_t>(
			body.outputWorker_.getOutputWorkingCount(task));
	info.leadTime_ =
			profiler.stats_.getValueAt(PriorityJobStats::TYPE_LEAD_TIME);
	info.actualTime_ =
			profiler.stats_.getValueAt(PriorityJobStats::TYPE_ACTUAL_TIME);
	writeTimeStrOptional(profiler.procStartTimestamp_, info.startTime_);
	writeTimeStrOptional(profiler.opCheckTimestamp_, info.operationCheckTime_);
	info.allocateMemory_ =
			profiler.stats_.getValueAt(PriorityJobStats::TYPE_MEMORY_USE);
	info.inputSwapRead_ = profiler.swapInCount_;
	info.inputSwapWrite_ = profiler.swapOutCount_;
	info.inputActiveBlockCount_ = profiler.activeBlockCount_;
	info.worker_ = static_cast<int64_t>(body.worker_.getInfo().totalId_);

	info.inputList_ = getInputTaskList(guard, body, *alloc, false);
	info.outputList_ = getOutputTaskList(guard, body, *alloc, false, false);

	info.workingInputList_ = getInputTaskList(guard, body, *alloc, true);
	info.workingOutputList_ =
			getOutputTaskList(guard, body, *alloc, true, false);
	info.inactiveOutputList_ =
			getOutputTaskList(guard, body, *alloc, false, true);
}

void PriorityTask::Profiler::getResourceProfile(
		util::LockGuard<util::Mutex>&, const Body &body,
		JobResourceInfo &info, bool byTask, bool limited) {
	const Profiler &profiler = body.profiler_;
	const JobId &jobId = body.job_.getId();
	const PriorityTaskConfig &taskConfig = body.job_.getTaskConfig();

	if (byTask) {
		info.taskOrdinal_ = body.id_;
		info.taskType_ = SQLType::Coder()(body.processor_.getType(), "");

		body.processor_.getPlanString(body.alloc_, info.plan_, limited);
	}
	else {
		info.startTime_ = taskConfig.startTime_;
		body.processor_.formatStatement(
				taskConfig.statement_.c_str(), info.statement_, limited);

		PriorityJob::Types::restrictionFlagsToString(
				body.job_.getRestrictionFlags(),
				info.resourceRestrictions_);
	}

	info.dbId_ = taskConfig.dbId_;
	info.dbName_ = taskConfig.dbName_.c_str();
	jobIdToString(jobId, info.requestId_);
	info.jobOrdinal_ = static_cast<int64_t>(jobId.execId_);

	body.node_.getInfo().getHostString(info.nodeAddress_);
	info.nodePort_ = static_cast<int32_t>(body.node_.getInfo().getPort());

	info.connectionAddress_ = taskConfig.clientAddress_.c_str();
	info.connectionPort_ = static_cast<int32_t>(taskConfig.clientPort_);

	info.userName_ = taskConfig.userName_.c_str();
	info.applicationName_ = taskConfig.applicationName_.c_str();

	info.statementType_ = "SQL";
	info.leadTime_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_LEAD_TIME);
	info.actualTime_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_ACTUAL_TIME);
	info.memoryUse_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_MEMORY_USE);
	info.sqlStoreUse_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_SQL_STORE_USE_LAST);
	info.dataStoreAccess_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_DATA_STORE_ACCESS);
	info.networkTransferSize_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_TRANS_SIZE);
	info.networkTime_ += profiler.stats_.getValueAt(PriorityJobStats::TYPE_TRANS_ACTIVE_TIME);
	info.availableConcurrency_ = static_cast<int64_t>(body.job_.getConcurrency());
}

TaskProfilerInfo& PriorityTask::Profiler::getProfile(
		util::LockGuard<util::Mutex> &guard, util::StackAllocator &alloc,
		const Body &body) {
	const Profiler &profiler = body.profiler_;

	TaskProfilerInfo *info = ALLOC_NEW(alloc) TaskProfilerInfo();
	info->taskId_ = body.id_;

	TaskProfiler &sub = info->profiler_;
	sub.leadTime_ = profiler.stats_.getValueAt(PriorityJobStats::TYPE_LEAD_TIME);
	sub.actualTime_ = profiler.stats_.getValueAt(PriorityJobStats::TYPE_ACTUAL_TIME);
	sub.executionCount_ = profiler.executionCount_;
	sub.worker_ = static_cast<int32_t>(body.worker_.getInfo().totalId_);

	util::Vector<int64_t> *&rows = sub.rows_;
	rows = ALLOC_NEW(alloc) util::Vector<int64_t>(alloc);

	sub.address_ = body.node_.getInfo().toAddressString(alloc);

	const InputList &inList = body.inputList_;
	for (InputList::const_iterator it = inList.begin();
			it != inList.end(); ++it) {
		rows->push_back(it->profiler_.getRowCount());
	}

	sub.customData_ = profiler.getCustomProfile(guard, alloc);

	const bool transPossible = isNetworkTransferPossible(guard, body);
	sub.memoryUse_ = profiler.stats_.getValueAt(PriorityJobStats::TYPE_MEMORY_USE);
	sub.sqlStoreUse_ = profiler.stats_.getValueAt(PriorityJobStats::TYPE_SQL_STORE_USE_PEAK);
	sub.dataStoreAccess_ = (body.processor_.getType() == SQLType::EXEC_SCAN ?
			profiler.stats_.getValueAt(PriorityJobStats::TYPE_DATA_STORE_ACCESS) : -1);
	sub.networkTransferSize_ = (transPossible ?
			profiler.stats_.getValueAt(PriorityJobStats::TYPE_TRANS_SIZE ) : -1);
	sub.networkTime_ = (transPossible ?
			profiler.stats_.getValueAt(PriorityJobStats::TYPE_TRANS_ACTIVE_TIME) : -1);

	return *info;
}

void PriorityTask::Profiler::setProfile(
		util::LockGuard<util::Mutex> &guard, Body &body,
		const TaskProfilerInfo &info) {
	Profiler &profiler = body.profiler_;

	const TaskProfiler &sub = info.profiler_;
	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_LEAD_TIME, sub.leadTime_);
	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_ACTUAL_TIME, sub.actualTime_);
	profiler.executionCount_ = sub.executionCount_;

	const util::Vector<int64_t> *rows = sub.rows_;
	if (rows != NULL) {
		util::Vector<int64_t>::const_iterator srcIt = rows->begin();

		InputList &inList = body.inputList_;
		InputList::iterator it = inList.begin();
		for (; it != inList.end() && srcIt != rows->end(); ++it, ++srcIt) {
			it->profiler_.setRowCount(*srcIt);
		}
	}

	if (sub.customData_ != NULL) {
		profiler.setCustomProfile(guard, *sub.customData_);
	}

	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_MEMORY_USE, sub.memoryUse_);
	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_SQL_STORE_USE_PEAK, sub.sqlStoreUse_);
	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_DATA_STORE_ACCESS, sub.dataStoreAccess_);
	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_TRANS_SIZE, sub.networkTransferSize_);
	profiler.stats_.setValueAt(
			PriorityJobStats::TYPE_TRANS_ACTIVE_TIME, sub.networkTime_);
}

void PriorityTask::Profiler::startProcessor(
		PriorityJobContext &cxt, SQLProcessor &proc) {
	procStartTimestamp_ =
			cxt.resolveEventContext().getHandlerStartTime().getUnixTime();
	enableCustomProfile(cxt, proc);
	procWatch_.start();
}

void PriorityTask::Profiler::enableCustomProfile(
		PriorityJobContext &cxt, SQLProcessor &proc) {
	SQLProcessor::Profiler profiler(cxt.getAllocator());
	profiler.setForAnalysis(true);
	proc.setProfiler(profiler);
}

void PriorityTask::Profiler::acceptCustomProfile(
		const Body &body, SQLProcessor &proc) {
	const SQLProcessor::Profiler::StreamData *src =
			proc.getProfiler().getStreamData();
	if (src != NULL) {
		util::LockGuard<util::Mutex> guard(body.lock_);
		setCustomProfile(guard, *src);
	}
}

void PriorityTask::Profiler::jobIdToString(const JobId &jobId, util::String &str) {
	util::StackAllocator &alloc = *str.get_allocator().base();
	jobId.toString(alloc, str, false);
}

util::XArray<uint8_t>* PriorityTask::Profiler::getCustomProfile(
		util::LockGuard<util::Mutex>&, util::StackAllocator &alloc) const {
	const util::AllocXArray<uint8_t> &src = customData_;
	util::XArray<uint8_t> *dest = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);

	dest->resize(src.size());
	memcpy(dest->data(), src.data(), src.size());

	return dest;
}

template<typename Alloc>
void PriorityTask::Profiler::setCustomProfile(
		util::LockGuard<util::Mutex>&,
		const util::XArray<uint8_t, Alloc> &src) {
	util::AllocXArray<uint8_t> &dest = customData_;

	dest.resize(src.size());
	memcpy(dest.data(), src.data(), src.size());
}

const PartitionListStats* PriorityTask::Profiler::preparePartitionListStats(
		const Source &source) {
	if (source.resolveType() != SQLType::EXEC_SCAN) {
		return NULL;
	}

	if (source.resolveWorker().getInfo().serviceType_ != TRANSACTION_SERVICE) {
		return NULL;
	}

	SQLExecutionManager *execMgr =
			source.controller_.getEnvironment().getExecutionManager();

	if (!source.profiling_) {
		DatabaseManager &dbMgr =
				execMgr->getManagerSet()->txnMgr_->getDatabaseManager();
		if (!dbMgr.isScanStats()) {
			return NULL;
		}
	}

	return &execMgr->getManagerSet()->partitionList_->getStats();
}

uint32_t PriorityTask::Profiler::prepareChunkSize(
		const PartitionListStats *plStats, const Source &source) {
	if (plStats == NULL) {
		return 0;
	}

	SQLExecutionManager *execMgr =
			source.controller_.getEnvironment().getExecutionManager();
	return PartitionList::Config::getChunkSize(
			*execMgr->getManagerSet()->config_);
}

util::AllocUniquePtr<ChunkBufferStats>::ReturnType
PriorityTask::Profiler::prepareChunkBufferStats(
		const PartitionListStats *plStats, const Source &source) {
	util::AllocUniquePtr<ChunkBufferStats> ptr;

	if (plStats != NULL) {
		ptr = ALLOC_UNIQUE(source.alloc_, ChunkBufferStats, NULL);
	}

	return ptr;
}

void PriorityTask::Profiler::startChunkBufferOperation(
		PriorityJobContext &cxt, const PartitionListStats &plStats) {
	EventContext *ec = cxt.getEventContext();
	plStats.getPGChunkBufferStats(ec->getWorkerId(), *bufStats_);
}

void PriorityTask::Profiler::finishChunkBufferOperation(
		PriorityJobContext &cxt, const PartitionListStats &plStats) {
	ChunkBufferStats curStats(NULL);
	EventContext *ec = cxt.getEventContext();
	plStats.getPGChunkBufferStats(ec->getWorkerId(), curStats);

	const uint64_t accessCount =
			getChunkBufferStatsDiff(
					*bufStats_, curStats,
					ChunkBufferStats::BUF_STAT_HIT_COUNT) +
			getChunkBufferStatsDiff(
					*bufStats_, curStats,
					ChunkBufferStats::BUF_STAT_ALLOCATE_COUNT) +
			getChunkBufferStatsDiff(
					*bufStats_, curStats,
					ChunkBufferStats::BUF_STAT_MISS_HIT_COUNT);

	stats_.addValueAt(
			PriorityJobStats::TYPE_DATA_STORE_ACCESS,
			chunkSize_ * accessCount);
}

template<typename P>
uint64_t PriorityTask::Profiler::getChunkBufferStatsDiff(
		const ChunkBufferStats &prev, const ChunkBufferStats &next, P param) {
	return next.table_(param).get() - prev.table_(param).get();
}

bool PriorityTask::Profiler::isNetworkTransferPossible(
		util::LockGuard<util::Mutex> &guard, const Body &body) {
	for (OutputList::const_iterator it = body.outputList_.begin();
			it != body.outputList_.end(); ++it) {
		PriorityTask task;
		if (body.job_.findTaskDirect(guard, it->taskId_, task) &&
				!(body.node_ == task.getNode())) {
			return true;
		}
	}
	return false;
}

uint32_t PriorityTask::Profiler::resolveStatusFlags(
		util::LockGuard<util::Mutex> &guard, const Body &body) const {
	typedef PriorityJob::Types Types;

	const bool blockInputsPending = !body.pendingBlockInputs_.empty();
	const bool finalInputsPending = !body.pendingFinalInputs_.empty();
	const bool inputWaiting = (
			!body.inputList_.empty() &&
			!blockInputsPending && !finalInputsPending);
	const bool normallyClosed = (
			!errorOccurred_ && body.processor_.isClosed());

	uint32_t flags = 0;
	flags |= (!body.active_ ? 1U << Types::TASK_INACTIVE : 0);
	flags |= (inputWaiting ? 1U << Types::TASK_INPUT_WAITING : 0);
	flags |= (blockInputsPending ? 1U << Types::TASK_INPUT_BLOCK_PENDING : 0);
	flags |= (finalInputsPending ? 1U << Types::TASK_INPUT_FINISH_PENDING : 0);
	flags |= (inputBusy_ ? 1U << Types::TASK_INPUT_BUSY : 0);
	flags |= (!isInputAllowed(
			guard, body) ? 1U << Types::TASK_INPUT_CLOSED : 0);
	flags |= (isAllOutputRestricted(
			guard, body) ? 1U << Types::TASK_OUTPUT_BUSY : 0);
	flags |= (!isOutputAllowed(
			guard, body) ? 1U << Types::TASK_OUTPUT_CLOSED : 0);
	flags |= (opWorking_ ? 1U << Types::TASK_OPERATING : 0);
	flags |= (normallyClosed ? 1U << Types::TASK_CLOSED : 0);
	flags |= (errorOccurred_ ? 1U << Types::TASK_ERROR : 0);
	return flags;
}

util::Vector<TaskId>* PriorityTask::Profiler::getInputTaskList(
		util::LockGuard<util::Mutex>&, const Body &body,
		util::StackAllocator &alloc, bool workingOnly) {
	const InputList &src = body.inputList_;
	if (src.empty()) {
		return NULL;
	}

	util::Vector<TaskId> *dest = ALLOC_NEW(alloc) util::Vector<TaskId>(alloc);
	for (InputList::const_iterator it = src.begin(); it != src.end(); ++it) {
		if (workingOnly && it->finishPrepared_) {
			continue;
		}
		dest->push_back(it->taskId_);
	}

	if (dest->empty()) {
		return NULL;
	}
	return dest;
}

util::Vector<TaskId>* PriorityTask::Profiler::getOutputTaskList(
		util::LockGuard<util::Mutex>&, const Body &body,
		util::StackAllocator &alloc, bool workingOnly, bool inactiveOnly) {
	const OutputList &src = body.outputList_;
	if (src.empty()) {
		return NULL;
	}

	util::Vector<TaskId> *dest = ALLOC_NEW(alloc) util::Vector<TaskId>(alloc);
	for (OutputList::const_iterator it = src.begin(); it != src.end(); ++it) {
		if (workingOnly && it->entryClosed_) {
			continue;
		}
		if (inactiveOnly && (it->entryClosed_ || it->capacity_ > 0)) {
			continue;
		}
		dest->push_back(it->taskId_);
	}

	if (dest->empty()) {
		return NULL;
	}
	return dest;
}


PriorityTask::InputProfiler::InputProfiler() :
		rowCount_(0) {
}

void PriorityTask::InputProfiler::acceptBlock(const TupleList::Block &block) {
	rowCount_ += static_cast<int64_t>(TupleList::tupleCount(block));
}

int64_t PriorityTask::InputProfiler::getRowCount() const {
	return rowCount_;
}

void PriorityTask::InputProfiler::setRowCount(int64_t count) {
	rowCount_ = count;
}


PriorityTask::Body::Body(const Source &source) :
		alloc_(source.alloc_),
		lock_(source.lock_),
		job_(source.job_),
		id_(source.resolveId()),
		mainId_(source.resolveMainId()),
		node_(source.resolveNode()),
		outputNodeSet_(alloc_),
		worker_(source.resolveWorker()),
		outputWorker_(source.resolveOutputWorker()),
		loadBalance_(source.resolveLoadBalance()),
		inputList_(source.makeInitialInputList()),
		outputList_(alloc_),
		pendingBlockInputs_(alloc_),
		pendingFinalInputs_(alloc_),
		finishedInputCount_(0),
		finishedOutputCount_(0),
		restrictedOutputCount_(0),
		activatedInputCount_(0),
		inputClosed_(false),
		outputClosed_(false),
		processorFinished_(false),
		active_(false),
		nextRemaining_(false),
		schemaCheckOnly_(false),
		schemaCheckPending_(false),
		forPreInput_(source.forPreInput_),
		requestedPreInputCount_(0),
		acceptedPreInputCount_(0),
		progressAssigned_(false),
		processor_(source),
		profiler_(source) {
}


PriorityTask::InputEntry::InputEntry(
		PriorityJobAllocator &alloc, const PriorityJobConfig &config,
		TaskId taskId) :
		taskId_(taskId),
		output_(std::numeric_limits<uint32_t>::max()),
		blockList_(alloc),
		finishPending_(false),
		finishPrepared_(false),
		capacity_(config.initialTaskIOCapacity_),
		acceptedSize_(0),
		preAcceptedSize_(0) {
}


PriorityTask::OutputEntry::OutputEntry(uint64_t initialCapacity) :
		taskId_(JobManager::UNDEF_TASKID),
		input_(std::numeric_limits<uint32_t>::max()),
		capacity_(initialCapacity),
		acceptedSize_(0),
		entryClosed_(false) {
}

PriorityTask::OutputEntry PriorityTask::OutputEntry::ofConfig(
		const PriorityJobConfig &config) {
	return OutputEntry(config.initialTaskIOCapacity_);
}


const char8_t *const PriorityTask::Constants::PLAN_STRING_KEYS[] = {
		"id",
		"type",
		"joinType",
		"unionType",
		"inputList",
		"limit",
		"offset",
		"subLimit",
		"subOffset",
		"tableIdInfo",
		"qName",
		"profile",
		"outputList",
		"predList",
		NULL
};


PriorityJobTable::PriorityJobTable() :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "priorityJobTable")),
		closed_(false),
		jobMap_(makeJobMap(alloc_, jobMapBase_)) {
}

PriorityJobTable::~PriorityJobTable() {
	close();
	jobMapBase_.reset();
}

void PriorityJobTable::close() {
	{
		util::LockGuard<util::Mutex> guard(lock_);
		closed_ = true;
	}
	removeAll();
}

PriorityJob PriorityJobTable::resolveJob(const PriorityJob::Source &source) {
	const JobId &id = source.id_;
	util::LockGuard<util::Mutex> guard(lock_);

	if (closed_) {
		return errorClosed();
	}

	JobMap::iterator it = jobMap_.find(id);
	if (it != jobMap_.end()) {
		return it->second;
	}

	PriorityJobBodyBase base(
			EntryBodyKey(EntryBodyKeyBase()), source, alloc_, lock_);
	PriorityJob job(&base);
	jobMap_.insert(std::make_pair(id, job));

	return job;
}

bool PriorityJobTable::findJob(const JobId &id, PriorityJob &job) {
	util::LockGuard<util::Mutex> guard(lock_);

	JobMap::iterator it = jobMap_.find(id);
	if (it == jobMap_.end()) {
		job = PriorityJob();
		return false;
	}

	job = it->second;
	return true;
}

void PriorityJobTable::removeJob(const JobId &id) {
	PriorityJob job;
	{
		util::LockGuard<util::Mutex> guard(lock_);

		JobMap::iterator it = jobMap_.find(id);
		if (it == jobMap_.end()) {
			return;
		}

		job = it->second;
		jobMap_.erase(it);
	}
}

PriorityTask PriorityJobTable::createTask(const PriorityTask::Source &source) {
	PriorityTaskBodyBase base(EntryBodyKey(EntryBodyKeyBase()), source);
	return PriorityTask(&base);
}

void PriorityJobTable::listJobIds(util::Vector<JobId> &idList) {
	util::LockGuard<util::Mutex> guard(lock_);
	for (JobMap::iterator it = jobMap_.begin(); it != jobMap_.end(); ++it) {
		idList.push_back(it->first);
	}
}

void PriorityJobTable::listJobs(util::Vector<PriorityJob> &jobList) {
	util::LockGuard<util::Mutex> guard(lock_);
	for (JobMap::iterator it = jobMap_.begin(); it != jobMap_.end(); ++it) {
		jobList.push_back(it->second);
	}
}

size_t PriorityJobTable::getJobCount() {
	util::LockGuard<util::Mutex> guard(lock_);
	return jobMap_.size();
}

void PriorityJobTable::removeAll() {
	for (JobMap::iterator it = jobMap_.begin(); it != jobMap_.end();) {
		JobMap::iterator next = it;
		++next;
		{
			PriorityJob job = it->second;
			job.close();
		}
		it = next;
	}
	assert(jobMap_.empty());
}

PriorityJob PriorityJobTable::errorClosed() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Job table already closed");
}

PriorityJob PriorityJobTable::errorJobId() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Job ID conflicted");
}


PriorityJobTable::EntryBodyKey::EntryBodyKey(const EntryBodyKeyBase&) {
}


PriorityJobBodyBase::PriorityJobBodyBase(
		const PriorityJobTable::EntryBodyKey&,
		const PriorityJob::Source &source, PriorityJobAllocator &alloc,
		util::Mutex &lock) :
		source_(source),
		alloc_(alloc),
		lock_(lock) {
}

const PriorityJob::Source& PriorityJobBodyBase::getSource() const {
	return source_;
}

PriorityJobAllocator& PriorityJobBodyBase::getAllocator() const {
	return alloc_;
}

util::Mutex& PriorityJobBodyBase::getLock() const {
	return lock_;
}


PriorityTaskBodyBase::PriorityTaskBodyBase(
		const PriorityJobTable::EntryBodyKey&,
		const PriorityTask::Source &source) :
		source_(source) {
}

const PriorityTask::Source& PriorityTaskBodyBase::getSource() const {
	return source_;
}


PriorityJobWorker::PriorityJobWorker(const Source &source) :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "priorityJobWorker")),
		info_(ALLOC_UNIQUE(alloc_, Info, source.info_)),
		engine_(source.engine_),
		config_(source.config_),
		transporter_(source.transporter_),
		tracer_(source.tracer_),
		partitionId_(source.partitionId_),
		forBackendIO_(source.forBackendIO_),
		queue_(alloc_),
		keyMap_(alloc_),
		restrictedNodeSet_(alloc_),
		seqNodeMap_(alloc_),
		nodeSeqMap_(alloc_),
		outputSequence_(0),
		blockPool_(source.blockPool_),
		executionRequested_(false),
		lastEventTime_(0) {
}

const PriorityJobWorker::Info& PriorityJobWorker::getInfo() const {
	return *info_;
}

void PriorityJobWorker::execute(PriorityJobContext &cxt) {
	{
		util::LockGuard<util::Mutex> guard(lock_);
		executionRequested_ = false;
		lastEventTime_ = cxt.resolveEventMonotonicTime();
	}

	PriorityTask task;
	const bool ioAccepted = acceptIO(cxt);
	const bool taskAccepted = popNormalTask(cxt, ioAccepted, task);

	if (!taskAccepted) {
		return;
	}

	int32_t continuousCount = 0;
	for (;;) {
		try {
			SQLProcessor *proc;
			SQLProcessor::Context *procCxt;

			uint32_t input;
			TupleList::Block block;
			bool withFinish;
			const PriorityTask::OperationType op =
					task.prepareOperation(cxt, proc, procCxt, input, block, withFinish);

			TRACE_TASK_EXECUTE("START", task, procCxt, proc, input, op, continuousCount);
			bool remaining;
			switch (op) {
			case PriorityTask::OP_PIPE:
				remaining = proc->pipe(*procCxt, input, block);
				if (withFinish) {
					remaining = proc->finish(*procCxt, input);
				}
				break;
			case PriorityTask::OP_FINISH:
				remaining = proc->finish(*procCxt, input);
				break;
			case PriorityTask::OP_NEXT:
				remaining = proc->next(*procCxt);
				break;
			default:
				assert(op == PriorityTask::OP_NONE);
				remaining = false;
				break;
			}
			TRACE_TASK_EXECUTE("END", task, procCxt, proc, input, op, continuousCount);
			continuousCount++;

			bool continuable;
			task.acceptProcessorResult(
					cxt, proc, procCxt, op, remaining, continuable);
			if (!continuable) {
				break;
			}
		}
		catch (...) {
			std::exception e;
			task.handleProcessorException(cxt, e);
			break;
		}

		acceptIO(cxt);
	}
}

void PriorityJobWorker::removeJob(const PriorityJob &job) {
	util::LockGuard<util::Mutex> guard(lock_);
	removeJobAt(guard, job);
}

void PriorityJobWorker::removeTask(const PriorityTask &task) {
	util::LockGuard<util::Mutex> guard(lock_);
	removeTaskEntry(guard, normalTaskKey(), task, false);
}

void PriorityJobWorker::putTask(
		PriorityJobContext &cxt, const PriorityTask &task) {
	util::LockGuard<util::Mutex> guard(lock_);
	putTaskAt(guard, cxt, normalTaskKey(), task, NULL, NULL);
}

void PriorityJobWorker::addTaskInput(
		PriorityJobContext &cxt, const PriorityTask &task, uint32_t input,
		const TupleList::Block &block) {
	InputEntry entry(false, input);
	entry.bufferedBlock_ = block;

	util::LockGuard<util::Mutex> guard(lock_);
	addInputEntry(guard, cxt, task, entry);
}

void PriorityJobWorker::addTaskInputUnbuffered(
		PriorityJobContext &cxt, const PriorityTask &task, uint32_t input,
		const JobUnbufferedBlock &block) {
	InputEntry entry(false, input);
	entry.block_ = block;

	util::LockGuard<util::Mutex> guard(lock_);
	addInputEntry(guard, cxt, task, entry);
}

void PriorityJobWorker::finishTaskInput(
		PriorityJobContext &cxt, const PriorityTask &task, uint32_t input) {
	InputEntry entry(true, input);

	util::LockGuard<util::Mutex> guard(lock_);
	addInputEntry(guard, cxt, task, entry);
}

void PriorityJobWorker::removeTaskInput(const PriorityTask &task) {
	util::LockGuard<util::Mutex> guard(lock_);
	removeTaskEntry(guard, inputKey(), task, false);
}

void PriorityJobWorker::addTaskOutput(
		PriorityJobContext &cxt, const PriorityTask &task,
		const TupleList::Block &block) {
	OutputEntry entry(false);
	entry.block_ = JobUnbufferedBlock(block);

	addOutputEntry(cxt, task, entry);
}

void PriorityJobWorker::finishTaskOutput(
		PriorityJobContext &cxt, const PriorityTask &task) {
	OutputEntry entry(true);

	addOutputEntry(cxt, task, entry);
}

void PriorityJobWorker::removeTaskOutput(const PriorityTask &task) {
	util::LockGuard<util::Mutex> guard(lock_);

	const PriorityTask::NodeSetRange &nodesRange = task.getOutputNodeSet();
	for (NodeSet::const_iterator nodeIt = nodesRange.first;
			nodeIt != nodesRange.second; ++nodeIt) {
		removeTaskEntry(guard, outputKey(*nodeIt), task, false);
	}
}

void PriorityJobWorker::resumeNodeOutput(
		PriorityJobContext &cxt, const JobNode &node) {
	util::LockGuard<util::Mutex> guard(lock_);

	JobNode localNode = node;
	if (localNode.isOutputRestricted()) {
		return;
	}

	restrictedNodeSet_.erase(node);

	if (checkNodeOutputPending(guard, cxt, node)) {
		return;
	}

	requestExecution(guard, cxt);
}

bool PriorityJobWorker::checkJobBusy(
		const PriorityJob &job, int64_t prevCheckTime) {
	if (prevCheckTime <= 0) {
		return true;
	}

	util::LockGuard<util::Mutex> guard(lock_);
	QueueKeyMap::const_iterator keyIt = keyMap_.find(job);
	if (keyIt == keyMap_.end()) {
		return false;
	}

	const QueueKeySet &keySet = keyIt->second;
	for (QueueKeySet::const_iterator setIt = keySet.begin();
			setIt != keySet.end(); ++setIt) {
		PriorityQueue::const_iterator queueIt = queue_.find(*setIt);
		if (queueIt == queue_.end()) {
			continue;
		}

		const JobQueue &queue = queueIt->second;
		if (queue.prevJobRotationTime_ < prevCheckTime) {
			return true;
		}
	}

	return false;
}

uint64_t PriorityJobWorker::getOutputWorkingCount(const PriorityTask &task) {
	util::LockGuard<util::Mutex> guard(lock_);

	uint64_t count = 0;
	const PriorityTask::NodeSetRange &range = task.getOutputNodeSet();
	for (PriorityTask::NodeSetIterator nodeIt = range.first;
			nodeIt != range.second; ++nodeIt) {
		if (nodeIt->isSelf()) {
			continue;
		}

		TaskEntry *entry;
		if (!findTaskEntry(guard, outputKey(*nodeIt), task, entry)) {
			continue;
		}

		const OutputMap &outMap = entry->outputMap_;
		for (OutputMap::const_iterator it = outMap.begin();
				it != outMap.end(); ++it) {
			count += it->second.size();
		}
	}

	return count;
}

void PriorityJobWorker::getStatProfile(
		PriorityJobContext &cxt, StatWorkerProfilerInfo &info) {
	Profiler profiler(*this);

	util::LockGuard<util::Mutex> guard(lock_);
	profiler.getStatProfileAt(guard, cxt, info);
}

void PriorityJobWorker::reportSent(PriorityJobContext &cxt, uint64_t seq) {
	util::LockGuard<util::Mutex> guard(lock_);

	JobNode node;
	finishBlockSending(guard, cxt, seq, node);

	if (checkNodeOutputPending(guard, cxt, node)) {
		return;
	}

	requestExecution(guard, cxt);
}

void PriorityJobWorker::validateNodes(PriorityJobContext &cxt) {
	util::LockGuard<util::Mutex> guard(lock_);

	if (!queue_.empty()) {
		requestExecution(guard, cxt);
	}

	for (NodeSequenceMap::iterator it = nodeSeqMap_.begin();
			it != nodeSeqMap_.end(); ++it) {
		checkNodeOutputPending(guard, cxt, it->first);
	}
}

void PriorityJobWorker::invalidateNode(
		PriorityJobContext &cxt, const JobNode &node) {
	util::LockGuard<util::Mutex> guard(lock_);

	NodeSequenceMap::iterator it = nodeSeqMap_.find(node);
	if (it == nodeSeqMap_.end()) {
		return;
	}

	const OutputNodeState &state = it->second;

	seqNodeMap_.erase(state.sequence_);
	nodeSeqMap_.erase(node);

	requestExecution(guard, cxt);
}

void PriorityJobWorker::accpetNodeValidationAck(
		PriorityJobContext &cxt, const JobNode &node,
		uint64_t validationCode) {
	util::LockGuard<util::Mutex> guard(lock_);

	NodeSequenceMap::iterator it = nodeSeqMap_.find(node);
	if (it == nodeSeqMap_.end()) {
		return;
	}

	{
		const OutputNodeState &state = it->second;
		if (!state.validating_ || state.validationCode_ != validationCode) {
			return;
		}

		const int64_t currentTime = cxt.resolveEventMonotonicTime();
		tracer_.traceNodeOutputValidation(
				node, currentTime, state.sendingTime_, state.validatingTime_,
				state.validating_, false, true);

		seqNodeMap_.erase(state.sequence_);
		nodeSeqMap_.erase(node);
	}

	requestExecution(guard, cxt);
}

void PriorityJobWorker::addInputEntry(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		const PriorityTask &task, const InputEntry &entry) {
	putTaskAt(guard, cxt, inputKey(), task, &entry, NULL);
}

void PriorityJobWorker::addOutputEntry(
		PriorityJobContext &cxt, const PriorityTask &task,
		const OutputEntry &entry) {
	if (!forBackendIO_) {
		errorOnlyForBackendIO();
	}

	PriorityTask localTask = task;
	const uint32_t outCount = task.getOutputCount();

	for (uint32_t i = 0; i < outCount; i++) {
		const SubOutputEntry subEntry(i, entry);

		PriorityTask outTask;
		if (!task.findOutputTask(i, outTask)) {
			continue;
		}

		const JobNode &node = outTask.getNode();
		if (node.isSelf()) {
			continue;
		}

		util::LockGuard<util::Mutex> guard(lock_);
		putTaskAt(guard, cxt, outputKey(node), task, NULL, &subEntry);
		task.updateOutputProfile(true);
	}
}

bool PriorityJobWorker::acceptIO(PriorityJobContext &cxt) {
	const bool inputAccepted = acceptInput(cxt);
	const bool outputAccepted = acceptOutput(cxt);

	if (forBackendIO_) {
		reserveStoreMemory();
	}

	return (inputAccepted || outputAccepted);
}

bool PriorityJobWorker::acceptInput(PriorityJobContext &cxt) {
	bool found = false;
	for (;;) {
		PriorityTask task;
		InputEntry entry(false, 0);
		{
			util::LockGuard<util::Mutex> guard(lock_);
			if (!popInputEntry(guard, task, entry)) {
				break;
			}
		}

		found = true;

		bool done;
		if (entry.forFinish_) {
			done = task.finishInputDirect(cxt, entry.input_);
		}
		else {
			const bool buffered = entry.block_.isEmpty();
			{
				JobUnbufferedBlock block = (buffered ?
						JobUnbufferedBlock(entry.bufferedBlock_) :
						entry.block_);
				done = task.addInputDirect(cxt, entry.input_, block);
			}

			if (done && buffered) {
				blockPool_.addBufferedBlock();
			}
		}
		if (done) {
			putTask(cxt, task);
		}
	}
	return found;
}

bool PriorityJobWorker::acceptOutput(PriorityJobContext &cxt) {
	bool found = false;
	for (;;) {
		PriorityTask task;
		std::pair<uint32_t, OutputEntry> entry(0, OutputEntry(false));
		bool busy;
		{
			util::LockGuard<util::Mutex> guard(lock_);
			if (!popOutputEntry(guard, cxt, task, entry, busy)) {
				break;
			}
		}

		found = true;

		PriorityTask outTask;
		if (!task.findOutputTask(entry.first, outTask)) {
			continue;
		}

		const uint32_t input = task.getInputForOutputTask(entry.first);
		if (entry.second.forFinish_) {
			outTask.finishInput(cxt, input);
			task.acceptFinishOutput(cxt);
		}
		else {
			JobNode node = outTask.getNode();
			const bool sending = !node.isSelf();

			uint64_t seq = 0;
			if (sending) {
				util::LockGuard<util::Mutex> guard(lock_);
				seq = startBlockSending(guard, cxt, node);
			}

			BlockSendingScope scope(cxt, *this, seq, sending);
			const PriorityTask::InputOption option(busy, seq);

			if (outTask.addInputUnbuffered(
					cxt, input, option, entry.second.block_)) {
				scope.accept();
			}
		}
	}
	return found;
}

bool PriorityJobWorker::popNormalTask(
		PriorityJobContext &cxt, bool ioAccepted, PriorityTask &task) {
	bool withExtraCheck;
	if (popNormalTaskAt(cxt, ioAccepted, NULL, withExtraCheck, task)) {
		return true;
	}

	if (withExtraCheck) {
		PriorityTask baseTask = task;
		if (popNormalTaskAt(
				cxt, ioAccepted, &baseTask, withExtraCheck, task)) {
			return true;
		}
	}

	return false;
}

bool PriorityJobWorker::popNormalTaskAt(
		PriorityJobContext &cxt, bool ioAccepted,
		const PriorityTask *baseTask, bool &withExtraCheck,
		PriorityTask &task) {
	withExtraCheck = false;

	bool priorTaskFound = false;
	PriorityTask priorTask;
	if (baseTask != NULL) {
		priorTaskFound = baseTask->findPriorTask(priorTask);
	}

	util::LockGuard<util::Mutex> guard(lock_);
	const QueueKey &key = normalTaskKey();
	bool taskAccepted = false;
	do {
		TaskEntry *entry;
		if (priorTaskFound && findTaskEntry(guard, key, priorTask, entry)) {
		}
		else if (!findFrontTaskEntry(guard, key, entry)) {
			break;
		}

		PriorityTask foundTask = entry->task_;

		if (baseTask == NULL && foundTask.isPriorTaskPossible()) {
			task = foundTask;
			withExtraCheck = true;
			return false;
		}

		removeTaskEntry(guard, key, foundTask, true);
		task = foundTask;
		task.getJob().startTaskOperation();
		taskAccepted = true;
		break;
	}
	while (false);

	if (ioAccepted || taskAccepted) {
		requestExecution(guard, cxt);
	}

	return taskAccepted;
}

bool PriorityJobWorker::popInputEntry(
		util::LockGuard<util::Mutex> &guard, PriorityTask &task,
		InputEntry &entry) {
	bool found = false;
	const QueueKey &key = inputKey();
	do {
		TaskEntry *taskEntry;
		if (!findFrontTaskEntry(guard, key, taskEntry)) {
			break;
		}

		InputList &inputList = taskEntry->inputList_;
		if (!inputList.empty()) {
			task = taskEntry->task_;
			entry = inputList.front();
			inputList.pop_front();
			found = true;
		}

		if (inputList.empty()) {
			removeTaskEntry(guard, key, taskEntry->task_, false);
		}
		else {
			rotateJobList(guard, key);
		}
	}
	while (!found);
	return found;
}

bool PriorityJobWorker::popOutputEntry(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		PriorityTask &task, std::pair<uint32_t, OutputEntry> &entry,
		bool &busy) {
	busy = false;
	const QueueKey &startKey = outputKey(JobNode());

	for (PriorityQueue::iterator queueIt = queue_.lower_bound(startKey);
			queueIt != queue_.end() && queueIt->first.first == startKey.first;
			++queueIt) {
		JobNode node = queueIt->first.second;
		if (checkNodeOutputPending(guard, cxt, node)) {
			continue;
		}
		else if (node.isOutputRestricted()) {
			restrictedNodeSet_.insert(node);
			continue;
		}

		const QueueKey &key = outputKey(node);

		TaskEntry *taskEntry;
		if (!findFrontTaskEntry(guard, key, taskEntry)) {
			continue;
		}

		OutputMap &outputMap = taskEntry->outputMap_;
		OutputMap::iterator targetIt;
		if (!findNextOutput(outputMap, targetIt)) {
			continue;
		}

		task = taskEntry->task_;
		entry.first = targetIt->first;
		entry.second = targetIt->second.front();

		targetIt->second.pop_front();
		if (targetIt->second.empty()) {
			outputMap.erase(targetIt);
		}

		if (outputMap.empty()) {
			PriorityTask entrytask = taskEntry->task_;
			removeTaskEntry(guard, key, entrytask, true);
		}
		else {
			rotateJobList(guard, key);
		}

		task.updateOutputProfile(false);
		busy = searchOtherJob(guard, key, task.getJob());
		return true;
	}
	return false;
}

bool PriorityJobWorker::checkNodeOutputPending(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		const JobNode &node) {

	NodeSequenceMap::iterator seqIt = nodeSeqMap_.find(node);
	if (seqIt != nodeSeqMap_.end()) {
		prepareNodeOutputValidation(guard, cxt, node, seqIt->second);
		return true;
	}

	if (restrictedNodeSet_.find(node) != restrictedNodeSet_.end()) {
		return true;
	}

	return false;
}

void PriorityJobWorker::prepareNodeOutputValidation(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		const JobNode &node, OutputNodeState &state) {
	const int64_t currentTime = cxt.resolveEventMonotonicTime();
	const int64_t thresholdTime =
			(currentTime - config_.outputValidationInterval_);

	if (state.sendingTime_ >= thresholdTime) {
		return;
	}

	if (state.validating_ && state.validatingTime_ >= thresholdTime) {
		return;
	}

	tracer_.traceNodeOutputValidation(
			node, currentTime, state.sendingTime_, state.validatingTime_,
			state.validating_, true, false);

	state.validatingTime_ = currentTime;
	state.validating_ = true;
	state.validationCode_ = transporter_.requestNodeValidation(cxt, node);
}

void PriorityJobWorker::putTaskAt(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		const QueueKey &key, const PriorityTask &task,
		const InputEntry *inEntry, const SubOutputEntry *outEntry) {
	TaskEntry *taskEntry = putTaskEntry(guard, key, task);
	if (taskEntry == NULL) {
		return;
	}

	setUpTaskEntry(guard, *taskEntry, inEntry, outEntry);

	requestExecution(guard, cxt);
}

PriorityJobWorker::TaskEntry* PriorityJobWorker::putTaskEntry(
		util::LockGuard<util::Mutex>&, const QueueKey &key,
		const PriorityTask &task) {
	const PriorityJob &job = task.getJob();
	if (job.isClosed()) {
		return NULL;
	}

	putQueueKey(job, key);

	PriorityQueue::iterator queueIt = queue_.find(key);
	if (queueIt == queue_.end()) {
		queueIt = queue_.insert(std::make_pair(key, JobQueue(alloc_))).first;
	}

	JobQueue &jobQueue = queueIt->second;
	JobIteratorMap &jobMap = jobQueue.iteratorMap_;

	JobIteratorMap::iterator jobMapIt = jobMap.find(job);
	if (jobMapIt == jobMap.end()) {
		jobQueue.jobList_.push_back(JobEntry(alloc_));
		jobQueue.jobList_.back().job_ = job;

		JobList::iterator jobIt = jobQueue.jobList_.end();
		--jobIt;

		if (jobMap.empty()) {
			jobQueue.nextIt_ = jobIt;
		}
		jobMapIt = jobMap.insert(std::make_pair(job, jobIt)).first;
	}

	JobEntry &jobEntry = *jobMapIt->second;
	TaskIteratorMap &taskMap = jobEntry.iteratorMap_;

	TaskIteratorMap::iterator taskMapIt = taskMap.find(task);
	if (taskMapIt == taskMap.end()) {
		jobEntry.taskList_.push_back(TaskEntry(alloc_));
		jobEntry.taskList_.back().task_ = task;

		TaskList::iterator taskIt = jobEntry.taskList_.end();
		--taskIt;

		jobEntry.nextIt_ = taskIt;
		taskMapIt = taskMap.insert(std::make_pair(task, taskIt)).first;
	}

	return &(*taskMapIt->second);
}

void PriorityJobWorker::setUpTaskEntry(
		util::LockGuard<util::Mutex>&, TaskEntry &taskEntry,
		const InputEntry *inEntry, const SubOutputEntry *outEntry) {
	if (inEntry != NULL) {
		taskEntry.inputList_.push_back(*inEntry);
	}

	if (outEntry != NULL) {
		const uint32_t outIndex = outEntry->first;
		OutputMap &outMap = taskEntry.outputMap_;

		OutputMap::iterator outIt = outMap.find(outIndex);
		if (outIt == outMap.end()) {
			outIt = outMap.insert(
					std::make_pair(outIndex, OutputList(alloc_))).first;
		}
		outIt->second.push_back(outEntry->second);
	}
}

void PriorityJobWorker::removeJobAt(
		util::LockGuard<util::Mutex>&, const PriorityJob &job) {
	QueueKeyMap::iterator mapIt = keyMap_.find(job);
	if (mapIt == keyMap_.end()) {
		return;
	}

	QueueKeySet &set = mapIt->second;
	for (QueueKeySet::iterator setIt = set.begin();
			setIt != set.end(); ++setIt) {
		PriorityQueue::iterator queueIt;
		if (!findJobQueue(queue_, *setIt, queueIt)) {
			continue;
		}

		JobQueue &jobQueue = queueIt->second;

		JobIteratorMap::iterator it = jobQueue.iteratorMap_.find(job);
		if (it == jobQueue.iteratorMap_.end()) {
			continue;
		}

		releaseBlocks(*it->second);

		if (jobQueue.nextIt_ == it->second) {
			++jobQueue.nextIt_;
		}
		jobQueue.jobList_.erase(it->second);
		jobQueue.iteratorMap_.erase(it);

		if (jobQueue.jobList_.empty()) {
			queue_.erase(queueIt);
		}
	}

	keyMap_.erase(mapIt);
}

void PriorityJobWorker::rotateJobList(
		util::LockGuard<util::Mutex>&, const QueueKey &key) {
	PriorityQueue::iterator queueIt;
	if (!findJobQueue(queue_, key, queueIt)) {
		return;
	}

	JobQueue &jobQueue = queueIt->second;

	JobIterator jobIt;
	if (!findNextJob(jobQueue, jobIt)) {
		return;
	}

	++jobQueue.nextIt_;
}

void PriorityJobWorker::removeTaskEntry(
		util::LockGuard<util::Mutex> &guard, const QueueKey &key,
		const PriorityTask &task, bool jobRotating) {
	PriorityQueue::iterator queueIt;
	if (!findJobQueue(queue_, key, queueIt)) {
		return;
	}

	PriorityJob job = task.getJob();
	JobQueue &jobQueue = queueIt->second;

	JobList &jobList = jobQueue.jobList_;
	JobIterator &nextIt = jobQueue.nextIt_;
	JobIteratorMap &iteratorMap = jobQueue.iteratorMap_;

	JobIteratorMap::iterator mapIt = iteratorMap.find(job);
	if (mapIt != iteratorMap.end()) {
		JobEntry &jobEntry = *mapIt->second;
		removeTaskSubAt(guard, jobEntry, task);

		if (jobEntry.taskList_.empty()) {
			JobIterator it = mapIt->second;
			if (it == nextIt) {
				++nextIt;
			}
			jobList.erase(it);
			iteratorMap.erase(mapIt);
			removeQueueKey(job, key);
		}
		else if (jobRotating) {
			rotateJobList(guard, key);
		}
	}

	if (jobList.empty()) {
		queue_.erase(queueIt);
	}
}

void PriorityJobWorker::removeTaskSubAt(
		util::LockGuard<util::Mutex>&, JobEntry &jobEntry,
		const PriorityTask &task) {
	TaskList &taskList = jobEntry.taskList_;
	TaskIterator &nextIt = jobEntry.nextIt_;
	TaskIteratorMap &iteratorMap = jobEntry.iteratorMap_;

	TaskIteratorMap::iterator mapIt = iteratorMap.find(task);
	if (mapIt != iteratorMap.end()) {
		TaskIterator it = mapIt->second;
		if (it == nextIt) {
			++nextIt;
		}
		releaseBlocks(*it);
		taskList.erase(it);
		iteratorMap.erase(mapIt);
	}
}

bool PriorityJobWorker::searchOtherJob(
		util::LockGuard<util::Mutex>&, const QueueKey &key,
		const PriorityJob &job) {
	PriorityQueue::iterator queueIt;
	if (!findJobQueue(queue_, key, queueIt)) {
		return false;
	}

	const JobList &jobList = queueIt->second.jobList_;
	if (jobList.empty()) {
		return false;
	}
	else if (jobList.size() == 1 && jobList.front().job_ == job) {
		return false;
	}

	return true;
}

bool PriorityJobWorker::searchTask(
		util::LockGuard<util::Mutex> &guard, const QueueKey &key,
		const PriorityTask &task) {
	TaskEntry *entry;
	return findTaskEntry(guard, key, task, entry);
}

bool PriorityJobWorker::findFrontTaskEntry(
		util::LockGuard<util::Mutex>&, const QueueKey &key, TaskEntry *&entry) {
	entry = NULL;

	PriorityQueue::iterator queueIt;
	if (!findJobQueue(queue_, key, queueIt)) {
		return false;
	}

	JobIterator jobIt;
	if (!findNextJob(queueIt->second, jobIt)) {
		return false;
	}

	TaskIterator taskIt;
	if (!findNextTask(*jobIt, taskIt)) {
		return false;
	}

	entry = &(*taskIt);
	return true;
}

bool PriorityJobWorker::findTaskEntry(
		util::LockGuard<util::Mutex>&, const QueueKey &key,
		const PriorityTask &task, TaskEntry *&entry) {
	entry = NULL;

	JobIterator jobIt;
	if (!findJob(queue_, key, task.getJob(), jobIt)) {
		return false;
	}

	const TaskIteratorMap &map = jobIt->iteratorMap_;
	TaskIteratorMap::const_iterator mapIt = map.find(task);
	if (mapIt == map.end()) {
		return false;
	}

	entry = &(*mapIt->second);
	return true;
}

void PriorityJobWorker::putQueueKey(
		const PriorityJob &job, const QueueKey &key) {
	QueueKeyMap::iterator keyIt = keyMap_.find(job);
	if (keyIt == keyMap_.end()) {
		keyIt = keyMap_.insert(std::make_pair(job, QueueKeySet(alloc_))).first;
	}

	keyIt->second.insert(key);
}

void PriorityJobWorker::removeQueueKey(
		const PriorityJob &job, const QueueKey &key) {
	QueueKeyMap::iterator keyIt = keyMap_.find(job);
	if (keyIt == keyMap_.end()) {
		return;
	}
	keyIt->second.erase(key);

	if (keyIt->second.empty()) {
		keyMap_.erase(keyIt);
	}
}

uint64_t PriorityJobWorker::startBlockSending(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		const JobNode &node) {
	if (node.isSelf() || nodeSeqMap_.find(node) != nodeSeqMap_.end()) {
		return errorOutputPending();
	}
	else if (node.isClosed()) {
		return errorOutputClosed(node);
	}

	const uint64_t seq = ++outputSequence_;
	const int64_t currentTime = cxt.resolveEventMonotonicTime();

	seqNodeMap_.insert(std::make_pair(seq, node));
	nodeSeqMap_.insert(std::make_pair(node, OutputNodeState(seq, currentTime)));

	return seq;
}

bool PriorityJobWorker::finishBlockSending(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt, uint64_t seq,
		JobNode &node) {
	SequenceNodeMap::iterator it = seqNodeMap_.find(seq);
	if (it == seqNodeMap_.end()) {
		return false;
	}

	node = it->second;
	seqNodeMap_.erase(it);

	NodeSequenceMap::iterator seqIt = nodeSeqMap_.find(node);
	if (seqIt != nodeSeqMap_.end()) {
		const OutputNodeState &state = seqIt->second;

		try {
			const int64_t currentTime = cxt.resolveEventMonotonicTime();
			if (state.validating_) {
				tracer_.traceNodeOutputValidation(
						node, currentTime, state.sendingTime_,
						state.validatingTime_, state.validating_, false, false);
			}
		}
		catch (...) {
		}

		nodeSeqMap_.erase(seqIt);
	}

	return true;
}

bool PriorityJobWorker::findJobQueue(
		PriorityQueue &totalQueue, const QueueKey &key,
		PriorityQueue::iterator &it) {
	it = totalQueue.find(key);
	return (it != totalQueue.end());
}

bool PriorityJobWorker::findJob(
		PriorityQueue &totalQueue, const QueueKey &key,
		const PriorityJob &job, JobIterator &it) {
	PriorityQueue::iterator queueIt;
	if (!findJobQueue(totalQueue, key, queueIt)) {
		return false;
	}

	JobIteratorMap &iteratorMap = queueIt->second.iteratorMap_;

	JobIteratorMap::iterator mapIt = iteratorMap.find(job);
	if (mapIt == iteratorMap.end()) {
		return false;
	}

	it = mapIt->second;
	return true;
}

bool PriorityJobWorker::findNextJob(JobQueue &queue, JobIterator &it) {
	JobList &list = queue.jobList_;
	JobIterator &nextIt = queue.nextIt_;

	if (nextIt == list.end()) {
		nextIt = list.begin();

		if (nextIt == list.end()) {
			queue.prevJobRotationTime_ = 0;
			queue.lastJobRotationTime_ = 0;
			return false;
		}
	}

	if (nextIt == list.begin()) {
		if (lastEventTime_ != queue.lastJobRotationTime_) {
			queue.prevJobRotationTime_ = queue.lastJobRotationTime_;
			queue.lastJobRotationTime_ = lastEventTime_;
		}
	}

	it = nextIt;
	return true;
}

bool PriorityJobWorker::findNextTask(JobEntry &entry, TaskIterator &it) {
	TaskList &list = entry.taskList_;
	TaskIterator &nextIt = entry.nextIt_;

	if (nextIt == list.end()) {
		nextIt = list.begin();
		if (nextIt == list.end()) {
			return false;
		}
	}

	it = nextIt;
	return true;
}

bool PriorityJobWorker::findNextOutput(
		OutputMap &map, OutputMap::iterator &it) {
	it = map.end();

	size_t maxSize = 0;
	for (OutputMap::iterator mapIt = map.begin(); mapIt != map.end(); ++mapIt) {
		if (mapIt->second.size() <= maxSize) {
			continue;
		}

		maxSize = mapIt->second.size();
		it = mapIt;
	}

	return (it != map.end());
}

void PriorityJobWorker::reserveStoreMemory() {
}

void PriorityJobWorker::releaseBlocks(JobEntry &jobEntry) {
	TaskList &taskList = jobEntry.taskList_;
	for (TaskList::iterator it = taskList.begin();
			it != taskList.end(); ++it) {
		releaseBlocks(*it);
	}
}

void PriorityJobWorker::releaseBlocks(TaskEntry &taskEntry) {
	InputList &inputList = taskEntry.inputList_;
	for (InputList::iterator it = inputList.begin();
			it != inputList.end(); ++it) {
		it->bufferedBlock_ = TupleList::Block();
		blockPool_.addBufferedBlock();
	}
}

void PriorityJobWorker::requestExecution(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt) {
	if (executionRequested_) {
		return;
	}

	engine_.request(cxt, partitionId_);
	executionRequested_ = true;
}

PriorityJobWorker::QueueKey PriorityJobWorker::inputKey() {
	return QueueKey(TAG_INPUT, JobNode());
}

PriorityJobWorker::QueueKey PriorityJobWorker::outputKey(
		const JobNode &node) {
	return QueueKey(TAG_OUTPUT, node);
}

PriorityJobWorker::QueueKey PriorityJobWorker::normalTaskKey() {
	return QueueKey(TAG_NORMAL_TASK, JobNode());
}

void PriorityJobWorker::errorOnlyForBackendIO() {
	GS_THROW_USER_ERROR_TRACE(
			GS_ERROR_JOB_INTERNAL,
			"Internal error by unexpected output request only for backend I/O "
			"worker");
}

uint64_t PriorityJobWorker::errorOutputPending() {
	GS_THROW_USER_ERROR_TRACE(
			GS_ERROR_JOB_INTERNAL,
			"Internal error by unexpected output request in pending state");
}

uint64_t PriorityJobWorker::errorOutputClosed(const JobNode &node) {
	GS_THROW_USER_ERROR(
			GS_ERROR_JOB_RESOLVE_NODE_FAILED,
			"Output node is closed by validation error ("
			"address=" << node.getInfo().address_ << ")");
}


PriorityJobWorker::Engine::~Engine() {
}


PriorityJobWorker::DefaultHandler::DefaultHandler(
		PriorityJobWorkerTable &table, uint8_t serviceType, bool forBackend,
		JobBufferedBlockPool &blockPool, PriorityJobTracer &tracer) :
		table_(table),
		serviceType_(serviceType),
		forBackend_(forBackend),
		blockPool_(blockPool),
		tracer_(tracer) {
}

PriorityJobWorker::DefaultHandler::~DefaultHandler() {
}

void PriorityJobWorker::DefaultHandler::operator()(
		EventContext &ec, Event &ev) {
	try {
		const uint32_t ordinal = table_.resolveWorkerOrdinal(
				serviceType_, ev.getPartitionId(), forBackend_);
		PriorityJobWorker &worker =
				table_.getWorkerDirect(serviceType_, ordinal, forBackend_);

		PriorityJobContextSource source = PriorityJobContextSource::ofEvent(ec, ev);
		source.blockPool_ = &blockPool_;

		PriorityJobContext cxt(source);
		try {
			worker.execute(cxt);
		}
		catch (...) {
			util::LockGuard<util::Mutex> guard(worker.lock_);
			worker.requestExecution(guard, cxt);

			std::exception e;
			tracer_.traceEventHandlingError(e, "working sql tasks");
		}
	}
	catch (...) {
		std::exception e;
		tracer_.traceEventHandlingError(e, "working sql task managements");
	}
}


PriorityJobWorker::DefaultEngine::DefaultEngine(EventEngine &base) :
		base_(base) {
}

PriorityJobWorker::DefaultEngine::~DefaultEngine() {
}

void PriorityJobWorker::DefaultEngine::setHandler(
		JobMessages::Type type, EventHandler &handler) {
	base_.setHandler(type, handler);
}

void PriorityJobWorker::DefaultEngine::request(
		PriorityJobContext &cxt, uint32_t partitionId) {
	Event ev(
			Event::Source(cxt.getVarAllocator()), PRIORITY_JOB_EXECUTE,
			partitionId);
	base_.add(ev);
}


PriorityJobWorker::Info::Info(
		uint8_t serviceType, bool forBackend, uint32_t totalId,
		uint32_t subId) :
		serviceType_(serviceType),
		forBackend_(forBackend),
		totalId_(totalId),
		subId_(subId) {
}


PriorityJobWorker::Source::Source(
		Engine &engine, const PriorityJobConfig &config,
		uint32_t partitionId, bool forBackendIO, const Info &info,
		JobBufferedBlockPool &blockPool, JobTransporter &transporter,
		PriorityJobTracer &tracer) :
		engine_(engine),
		config_(config),
		partitionId_(partitionId),
		forBackendIO_(forBackendIO),
		info_(info),
		blockPool_(blockPool),
		transporter_(transporter),
		tracer_(tracer) {
}


PriorityJobWorker::InputEntry::InputEntry(bool forFinish, uint32_t input) :
		forFinish_(forFinish),
		input_(input) {
}


PriorityJobWorker::OutputEntry::OutputEntry(bool forFinish) :
		forFinish_(forFinish) {
}


PriorityJobWorker::TaskEntry::TaskEntry(PriorityJobAllocator &alloc) :
		inputList_(alloc),
		outputMap_(alloc) {
}


PriorityJobWorker::JobEntry::JobEntry(PriorityJobAllocator &alloc) :
		taskList_(alloc),
		nextIt_(),
		iteratorMap_(alloc) {
}


PriorityJobWorker::JobQueue::JobQueue(PriorityJobAllocator &alloc) :
		jobList_(alloc),
		nextIt_(),
		iteratorMap_(alloc),
		lastJobRotationTime_(0),
		prevJobRotationTime_(0) {
}


PriorityJobWorker::OutputNodeState::OutputNodeState(
		uint64_t sequence, int64_t sendingTime) :
		sequence_(sequence),
		sendingTime_(sendingTime),
		validatingTime_(0),
		validationCode_(0),
		validating_(false) {
}


PriorityJobWorker::BlockSendingScope::BlockSendingScope(
		PriorityJobContext &cxt, PriorityJobWorker &worker, uint64_t seq,
		bool sending) :
		cxt_(cxt),
		worker_(worker),
		seq_(seq),
		sending_(sending),
		accepted_(false) {
}

PriorityJobWorker::BlockSendingScope::~BlockSendingScope() {
	try {
		cancel();
	}
	catch (...) {
	}
}

void PriorityJobWorker::BlockSendingScope::accept() {
	accepted_ = true;
}

void PriorityJobWorker::BlockSendingScope::cancel() {
	if (sending_ && !accepted_) {
		util::LockGuard<util::Mutex> guard(worker_.lock_);
		JobNode node;
		worker_.finishBlockSending(guard, cxt_, seq_, node);
	}
}


PriorityJobWorker::Profiler::Profiler(const PriorityJobWorker &worker) :
		worker_(worker) {
}

void PriorityJobWorker::Profiler::getStatProfileAt(
		util::LockGuard<util::Mutex> &guard, PriorityJobContext &cxt,
		StatWorkerProfilerInfo &info) {
	getStatBaseProfile(guard, cxt, info);
	getStatQueueProfile(guard, cxt, info.queueList_);
	getStatNodeProfile(guard, cxt, info.nodeList_);
}

void PriorityJobWorker::Profiler::getStatBaseProfile(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		StatWorkerProfilerInfo &info) {
	const Info &src = *worker_.info_;

	info.forTransaction_ = (src.serviceType_ == TRANSACTION_SERVICE);
	info.forBackend_ = src.forBackend_;
	info.totalId_ = src.totalId_;
	info.subId_ = src.subId_;
	info.lastEventElapsed_ = toElapsedTime(cxt, worker_.lastEventTime_);
}

void PriorityJobWorker::Profiler::getStatQueueProfile(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		util::Vector<StatWorkerProfilerInfo::QueueEntry> &dest) {
	util::StackAllocator &alloc = cxt.getAllocator();

	for (PriorityQueue::const_iterator it = worker_.queue_.begin();
			it != worker_.queue_.end(); ++it) {
		const QueueKey &key = it->first;
		const JobQueue &jobQueue = it->second;

		dest.push_back(StatWorkerProfilerInfo::QueueEntry(alloc));
		StatWorkerProfilerInfo::QueueEntry &entry = dest.back();

		entry.type_ = newStringOptional(alloc, tagToStringOptional(key.first));
		entry.outputNode_ = key.second.getInfoOptional().toAddressString(alloc);
		getStatJobProfile(
				alloc, jobQueue.jobList_, entry.jobList_, jobQueue.nextIt_,
				entry.nextJobOrdinal_);
		entry.prevRotationElapsed_ =
				toElapsedTime(cxt, jobQueue.prevJobRotationTime_);
		entry.lastRotationElapsed_ =
				toElapsedTime(cxt, jobQueue.lastJobRotationTime_);
	}
}

void PriorityJobWorker::Profiler::getStatNodeProfile(
		util::LockGuard<util::Mutex>&, PriorityJobContext &cxt,
		util::Vector<StatWorkerProfilerInfo::NodeEntry> &dest) {
	typedef StatWorkerProfilerInfo::NodeEntry Entry;
	typedef util::Map<JobNode, StatWorkerProfilerInfo::NodeEntry> Map;

	util::StackAllocator &alloc = cxt.getAllocator();
	Map map(alloc);

	for (NodeSet::const_iterator it = worker_.restrictedNodeSet_.begin();
			it != worker_.restrictedNodeSet_.end(); ++it) {
		Entry &entry = map.insert(
				std::make_pair(*it, Entry(alloc))).first->second;
		entry.outputRestricted_ = true;
	}

	for (SequenceNodeMap::const_iterator it = worker_.seqNodeMap_.begin();
			it != worker_.seqNodeMap_.end(); ++it) {
		Entry &entry = map.insert(
				std::make_pair(it->second, Entry(alloc))).first->second;
		entry.outputSendingKey_ = it->first;
	}

	for (NodeSequenceMap::const_iterator it = worker_.nodeSeqMap_.begin();
			it != worker_.nodeSeqMap_.end(); ++it) {
		Entry &entry = map.insert(
				std::make_pair(it->first, Entry(alloc))).first->second;
		entry.outputSendingElapsed_ = toElapsedTime(cxt, it->second.sendingTime_);
		entry.outputSendingValue_ = it->second.sequence_;
	}

	for (Map::iterator it = map.begin(); it != map.end(); ++it) {
		dest.push_back(it->second);
		StatWorkerProfilerInfo::NodeEntry &entry = dest.back();
		it->first.getStatProfileOptional(entry);
	}
}

void PriorityJobWorker::Profiler::getStatJobProfile(
		util::StackAllocator &alloc, const JobList &src,
		util::Vector<StatWorkerProfilerInfo::JobEntry> &dest,
		JobList::const_iterator nextIt, int64_t &nextOrdinal) {
	nextOrdinal = (nextIt == src.end() ? 0 : -1);

	int64_t ordinal = 0;
	for (JobList::const_iterator it = src.begin(); it != src.end(); ++it) {
		const JobEntry &srcEntry = *it;

		dest.push_back(StatWorkerProfilerInfo::JobEntry(alloc));
		StatWorkerProfilerInfo::JobEntry &entry = dest.back();

		srcEntry.job_.getId().toString(alloc, entry.jobId_);
		getStatTaskProfile(
				alloc, srcEntry.taskList_, entry.taskList_, srcEntry.nextIt_,
				entry.nextTaskOrdinal_);

		if (it == nextIt) {
			nextOrdinal = ordinal;
		}
		ordinal++;
	}
}

void PriorityJobWorker::Profiler::getStatTaskProfile(
		util::StackAllocator &alloc, const TaskList &src,
		util::Vector<StatWorkerProfilerInfo::TaskEntry> &dest,
		TaskList::const_iterator nextIt, int64_t &nextOrdinal) {
	nextOrdinal = (nextIt == src.end() ? 0 : -1);

	int64_t ordinal = 0;
	for (TaskList::const_iterator it = src.begin(); it != src.end(); ++it) {
		const TaskEntry &srcEntry = *it;

		dest.push_back(StatWorkerProfilerInfo::TaskEntry());
		StatWorkerProfilerInfo::TaskEntry &entry = dest.back();

		entry.taskId_ = srcEntry.task_.getId();
		entry.type_ = newStringOptional(
				alloc, SQLType::Coder()(srcEntry.task_.getType(), ""));
		getStatInputProfile(alloc, srcEntry.inputList_, entry.inputList_);
		getStatOutputProfile(alloc, srcEntry.outputMap_, entry.outputList_);

		if (it == nextIt) {
			nextOrdinal = ordinal;
		}
		ordinal++;
	}
}

void PriorityJobWorker::Profiler::getStatInputProfile(
		util::StackAllocator &alloc, const InputList &src,
		util::Vector<StatWorkerProfilerInfo::InputEntry> *&dest) {
	if (src.empty()) {
		dest = NULL;
		return;
	}

	dest = ALLOC_NEW(alloc) util::Vector<StatWorkerProfilerInfo::InputEntry>(
			alloc);
	for (InputList::const_iterator it = src.begin(); it != src.end(); ++it) {
		const InputEntry &srcEntry = *it;

		dest->push_back(StatWorkerProfilerInfo::InputEntry());
		StatWorkerProfilerInfo::InputEntry &entry = dest->back();

		entry.index_ = srcEntry.input_;
		entry.blockCount_ = (srcEntry.forFinish_ ? 0 : 1);
		entry.forFinish_ = srcEntry.forFinish_;

		if (dest->size() > 1) {
			StatWorkerProfilerInfo::InputEntry &prev = *(dest->end() - 2);
			if (!prev.index_ == !entry.index_ &&
					!prev.forFinish_ && !entry.forFinish_) {
				prev.blockCount_ += entry.blockCount_;
				dest->pop_back();
			}
		}
	}
}

void PriorityJobWorker::Profiler::getStatOutputProfile(
		util::StackAllocator &alloc, const OutputMap &src,
		util::Vector<StatWorkerProfilerInfo::OutputEntry> *&dest) {
	if (src.empty()) {
		dest = NULL;
		return;
	}

	dest = ALLOC_NEW(alloc) util::Vector<StatWorkerProfilerInfo::OutputEntry>(
			alloc);
	for (OutputMap::const_iterator mapIt = src.begin();
			mapIt != src.end(); ++mapIt) {
		const OutputList &srcList = mapIt->second;
		for (OutputList::const_iterator it = srcList.begin();
				it != srcList.end(); ++it) {
			const OutputEntry &srcEntry = *it;

			dest->push_back(StatWorkerProfilerInfo::OutputEntry());
			StatWorkerProfilerInfo::OutputEntry &entry = dest->back();

			entry.index_ = mapIt->first;
			entry.blockCount_ = (srcEntry.forFinish_ ? 0 : 1);
			entry.forFinish_ = srcEntry.forFinish_;

			if (dest->size() > 1) {
				StatWorkerProfilerInfo::OutputEntry &prev = *(dest->end() - 2);
				if (!prev.index_ == !entry.index_ &&
						!prev.forFinish_ && !entry.forFinish_) {
					prev.blockCount_ += entry.blockCount_;
					dest->pop_back();
				}
			}
		}
	}
}

util::String* PriorityJobWorker::Profiler::newStringOptional(
		util::StackAllocator &alloc, const char8_t *src) {
	if (strlen(src) == 0) {
		return NULL;
	}

	return ALLOC_NEW(alloc) util::String(src, alloc);
}

int64_t PriorityJobWorker::Profiler::toElapsedTime(
		PriorityJobContext &cxt, int64_t monotonicTime) {
	if (monotonicTime <= 0) {
		return -1;
	}

	const int64_t currentTime = cxt.resolveEventMonotonicTime();
	return std::max<int64_t>(currentTime - monotonicTime, 0);
}

const char8_t* PriorityJobWorker::Profiler::tagToStringOptional(QueueTag tag) {
	switch (tag) {
	case TAG_INPUT:
		return "INPUT";
	case TAG_OUTPUT:
		return "OUTPUT";
	default:
		assert(tag == TAG_NORMAL_TASK);
		return "";
	}
}


PriorityJobWorkerTable::PriorityJobWorkerTable(const Source &source) :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "priorityJobWorkerTable")) {
	setUpSqlFront(source);
	setUpSqlBack(source);
	setUpTransaction(source);
}

PriorityJobWorkerTable::~PriorityJobWorkerTable() {
	clear();
}

void PriorityJobWorkerTable::setUpHandlers() {
	for (size_t i = 0; i < END_TYPE; i++) {
		Entry &entry = entryList_[i];
		assert(entry.engine_ != NULL);
		entry.engine_->setHandler(PRIORITY_JOB_EXECUTE, *entry.handler_);
	}
}

PriorityJobWorker& PriorityJobWorkerTable::getTaskWorker(
		uint8_t serviceType, int32_t loadBalance, bool forBackend) {
	const uint32_t partitionId =
			resolveWorkerPartition(serviceType, loadBalance, forBackend, false);
	const uint32_t ordinal =
			resolveWorkerOrdinal(serviceType, partitionId, forBackend);
	return getWorkerDirect(serviceType, ordinal, forBackend);
}

PriorityJobWorker& PriorityJobWorkerTable::getBackendIOWorker() {
	const uint8_t serviceType = SQL_SERVICE;
	const bool forBackend = true;
	const uint32_t partitionId =
			resolveWorkerPartition(serviceType, 0, forBackend, true);
	const uint32_t ordinal =
			resolveWorkerOrdinal(serviceType, partitionId, forBackend);
	return getWorkerDirect(serviceType, ordinal, forBackend);
}

PriorityJobWorker& PriorityJobWorkerTable::getWorkerDirect(
		uint8_t serviceType, uint32_t ordinal, bool forBackend) {
	const EntryType type = getEntryType(serviceType, forBackend);
	Entry &entry = entryList_[type];

	assert(ordinal < entry.workerList_->size());
	return *(*entry.workerList_)[ordinal];
}

uint32_t PriorityJobWorkerTable::resolveWorkerOrdinal(
		uint8_t serviceType, uint32_t partitionId, bool forBackend) {
	const EntryType type = getEntryType(serviceType, forBackend);
	Entry &entry = entryList_[type];

	return entry.pgConfig_->getPartitionGroupId(partitionId);
}

util::Vector<StatWorkerProfilerInfo>* PriorityJobWorkerTable::getStatProfile(
		PriorityJobContext &cxt) {
	util::StackAllocator &alloc = cxt.getAllocator();

	util::Vector<StatWorkerProfilerInfo> *dest =
			ALLOC_NEW(alloc) util::Vector<StatWorkerProfilerInfo>(alloc);
	for (size_t i = 0; i < END_TYPE; i++) {
		WorkerList &workerList = *entryList_[i].workerList_;
		for (WorkerList::iterator it = workerList.begin();
				it != workerList.end(); ++it) {
			dest->push_back(StatWorkerProfilerInfo(alloc));
			StatWorkerProfilerInfo &info = dest->back();

			(*it)->getStatProfile(cxt, info);

			if (info.isEmpty()) {
				dest->pop_back();
			}
		}
	}

	if (dest->empty()) {
		dest = NULL;
	}
	return dest;
}

void PriorityJobWorkerTable::validateNodes(PriorityJobContext &cxt) {
	for (size_t i = 0; i < END_TYPE; i++) {
		WorkerList &workerList = *entryList_[i].workerList_;
		for (WorkerList::iterator it = workerList.begin();
				it != workerList.end(); ++it) {
			(*it)->validateNodes(cxt);
		}
	}
}

void PriorityJobWorkerTable::invalidateNode(
		PriorityJobContext &cxt, const JobNode &node) {
	for (size_t i = 0; i < END_TYPE; i++) {
		WorkerList &workerList = *entryList_[i].workerList_;
		for (WorkerList::iterator it = workerList.begin();
				it != workerList.end(); ++it) {
			(*it)->invalidateNode(cxt, node);
		}
	}
}

void PriorityJobWorkerTable::clear() {
	for (size_t i = 0; i < END_TYPE; i++) {
		WorkerList &workerList = *entryList_[i].workerList_;
		for (WorkerList::iterator it = workerList.begin();
				it != workerList.end(); ++it) {
			util::AllocUniquePtr<PriorityJobWorker> ptr(*it, alloc_);
			ptr.reset();
		}
		workerList.clear();
	}
}

void PriorityJobWorkerTable::setUpSqlFront(const Source &source) {
	const PriorityJobConfig &config = source.config_;
	setUpEntry(
			TYPE_SQL_FRONT, source.sqlFrontEngine_, SQL_SERVICE,
			false, config.concurrency_, config.partitionCount_, source);
}

void PriorityJobWorkerTable::setUpSqlBack(const Source &source) {
	const PriorityJobConfig &config = source.config_;
	const uint32_t mappedConcurrency = config.concurrency_ + 1;
	setUpEntry(
			TYPE_SQL_BACK, source.sqlBackEngine_, SQL_SERVICE,
			true, mappedConcurrency, mappedConcurrency, source);
}

void PriorityJobWorkerTable::setUpTransaction(const Source &source) {
	const PriorityJobConfig &config = source.config_;
	setUpEntry(
			TYPE_TRANSACTION, source.transactionEngine_, TRANSACTION_SERVICE,
			false, config.txnConcurrency_, config.txnPartitionCount_, source);
}

void PriorityJobWorkerTable::setUpEntry(
		EntryType type, PriorityJobWorker::Engine &engine, uint8_t serviceType,
		bool forBackend, uint32_t concurrency, uint32_t partitionCount,
		const Source &source) {
	Entry &entry = entryList_[type];

	entry.engine_ = &engine;
	entry.handler_ = UTIL_MAKE_LOCAL_UNIQUE(
			entry.handler_, PriorityJobWorker::DefaultHandler,
			*this, serviceType, forBackend, source.blockPool_,
			source.tracer_);
	entry.workerList_ = UTIL_MAKE_LOCAL_UNIQUE(
			entry.workerList_, WorkerList, alloc_);
	entry.pgConfig_ = UTIL_MAKE_LOCAL_UNIQUE(
			entry.pgConfig_, PartitionGroupConfig, partitionCount, concurrency);

	const uint32_t baseId = (serviceType == TRANSACTION_SERVICE ? 0 :
			source.config_.txnConcurrency_ +
					(forBackend ? source.config_.txnConcurrency_ : 0));

	const uint32_t backendIOPartition = (
			forBackend && serviceType == SQL_SERVICE ?
					resolveWorkerPartition(serviceType, 0, forBackend, true) :
					std::numeric_limits<uint32_t>::max());

	WorkerList &workerList = *entry.workerList_;
	for (uint32_t i = 0; i < concurrency; i++) {
		const uint32_t partitionId =
				entry.pgConfig_->getGroupBeginPartitionId(i);
		const bool forBackendIO = (i == backendIOPartition);

		PriorityJobWorker::Info info(serviceType, forBackend, baseId + i, i);
		PriorityJobWorker::Source workerSource(
				engine, source.config_, partitionId, forBackendIO, info,
				source.blockPool_, source.transporter_, source.tracer_);

		util::AllocUniquePtr<PriorityJobWorker> ptr(ALLOC_UNIQUE(
				alloc_, PriorityJobWorker, workerSource));
		workerList.push_back(ptr.get());
		ptr.release();
	}
}

uint32_t PriorityJobWorkerTable::resolveWorkerPartition(
		uint8_t serviceType, int32_t loadBalance, bool forBackend,
		bool forIO) {
	if (forIO) {
		const PartitionGroupConfig &config =
				*entryList_[TYPE_SQL_BACK].pgConfig_;
		return config.getPartitionGroupCount() - 1;
	}
	else if (serviceType == SQL_SERVICE && forBackend) {
		const uint32_t partition = resolveLoadBalance(serviceType, loadBalance);
		const PartitionGroupConfig &config =
				*entryList_[TYPE_SQL_FRONT].pgConfig_;
		return config.getPartitionGroupId(partition);
	}
	else {
		return resolveLoadBalance(serviceType, loadBalance);
	}
}

uint32_t PriorityJobWorkerTable::resolveLoadBalance(
		uint8_t serviceType, int32_t loadBalance) {
	if (loadBalance >= 0) {
		return static_cast<uint32_t>(loadBalance);
	}

	if (serviceType != SQL_SERVICE) {
		GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "");
	}

	const PartitionGroupConfig &config = *entryList_[TYPE_SQL_FRONT].pgConfig_;
	const uint32_t concurrency = config.getPartitionGroupCount();
	const uint32_t group =
			static_cast<uint32_t>(++loadBalanceCounter_ % concurrency);
	return config.getGroupBeginPartitionId(group);
}

PriorityJobWorkerTable::EntryType PriorityJobWorkerTable::getEntryType(
		uint8_t serviceType, bool forBackend) {
	if (serviceType == SQL_SERVICE) {
		return (forBackend ? TYPE_SQL_BACK : TYPE_SQL_FRONT);
	}
	else {
		assert(serviceType == TRANSACTION_SERVICE);
		return TYPE_TRANSACTION;
	}
}


PriorityJobWorkerTable::Source::Source(
		PriorityJobWorker::Engine &sqlFrontEngine,
		PriorityJobWorker::Engine &sqlBackEngine,
		PriorityJobWorker::Engine &transactionEngine,
		JobBufferedBlockPool &blockPool,
		JobTransporter &transporter,
		PriorityJobTracer &tracer,
		const PriorityJobConfig &config) :
		sqlFrontEngine_(sqlFrontEngine),
		sqlBackEngine_(sqlBackEngine),
		transactionEngine_(transactionEngine),
		blockPool_(blockPool),
		transporter_(transporter),
		tracer_(tracer),
		config_(config) {
}


PriorityJobWorkerTable::Entry::Entry() :
		engine_(NULL) {
}


PriorityJobMonitor::PriorityJobMonitor(
		PriorityJobEnvironment &env, PriorityJobTracer &tracer,
		PriorityJobTable &jobTable) :
		alloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "priorityJobMonitor")),
		env_(env),
		tracer_(tracer),
		jobTable_(jobTable),
		reportHistory_(alloc_),
		reportedJobs_(alloc_),
		reportInterval_(0),
		reportLimit_(0),
		networkTimeInterval_(0),
		periodicMonitoringTime_(0) {
}

PriorityJobMonitor::~PriorityJobMonitor() {
	jobTable_.close();
}

void PriorityJobMonitor::removeStats(
		const JobId &jobId, const PriorityJobStats &prev) {
	uint32_t reportableTypes;
	uint32_t exceededTypes;
	uint32_t selfExceededTypes;
	util::LockGuard<util::Mutex> guard(lock_);

	acceptStats(
			guard, jobId, prev, NULL, reportableTypes, exceededTypes,
			selfExceededTypes);
}

PriorityJobMonitor::StatsActions PriorityJobMonitor::updateStats(
		PriorityJobContext &cxt, const JobId &jobId,
		const PriorityJobStats &prev, const PriorityJobStats &next) {
	const int64_t currentTime = cxt.resolveEventMonotonicTime();
	updatePeriodicMonitoringTime(currentTime);

	StatsActions actions;
	{
		util::LockGuard<util::Mutex> guard(lock_);

		uint32_t reportableTypes;
		acceptStats(
				guard, jobId, prev, &next, reportableTypes,
				actions.exceededTypes_, actions.selfExceededTypes_);
		actions.reportable_ =
				acceptReport(guard, jobId, currentTime, reportableTypes);
	}

	return actions;
}

void PriorityJobMonitor::acceptStatsActions(
		PriorityJobContext &cxt, PriorityJob &job,
		const StatsActions &actions) {
	adjustJobs(cxt, actions.selfExceededTypes_, &job.getId());
	adjustJobs(cxt, actions.exceededTypes_, NULL);

	if (actions.reportable_) {
		reportStats(cxt, job);
	}
}

void PriorityJobMonitor::setReportInterval(int64_t interval) {
	util::LockGuard<util::Mutex> guard(lock_);
	reportInterval_ = interval;
}

void PriorityJobMonitor::setReportLimit(int64_t limit) {
	util::LockGuard<util::Mutex> guard(lock_);
	reportLimit_ = limit;
}

void PriorityJobMonitor::setNetworkTimeInterval(int64_t interval) {
	util::LockGuard<util::Mutex> guard(lock_);
	networkTimeInterval_ = interval;
}

void PriorityJobMonitor::setPlanTraceEnabled(bool enabled) {
	planTraceEnabled_ = enabled;
}

void PriorityJobMonitor::setPlanSizeLimit(uint32_t limit) {
	planSizeLimit_ = limit;
}

void PriorityJobMonitor::setMonitoringTotal(
		PriorityJobStats::Type type, int64_t total) {
	util::LockGuard<util::Mutex> guard(lock_);

	getEntry(type).total_ = total;
	updateThreshold(guard, type, false);
	updateThreshold(guard, type, true);
}

void PriorityJobMonitor::setMonitoringRate(
		PriorityJobStats::Type type, double rate) {
	util::LockGuard<util::Mutex> guard(lock_);

	getEntry(type).rate_ = rate;
	updateThreshold(guard, type, false);
}

void PriorityJobMonitor::setLimitRate(
		PriorityJobStats::Type type, double rate) {
	util::LockGuard<util::Mutex> guard(lock_);

	getEntry(type).limitRate_ = rate;
	updateThreshold(guard, type, true);
}

void PriorityJobMonitor::setFailOnExcess(
		PriorityJobStats::Type type, bool enabled) {
	util::LockGuard<util::Mutex> guard(lock_);

	getEntry(type).failOnExcess_ = enabled;
}

void PriorityJobMonitor::setEssentialLimit(
		PriorityJobStats::Type type, int64_t value) {
	util::LockGuard<util::Mutex> guard(lock_);

	getEntry(type).essentialThreshold_ = value;
}

int64_t PriorityJobMonitor::getPeriodicMonitoringTime() {
	return periodicMonitoringTime_;
}

bool PriorityJobMonitor::isFailOnExcess(PriorityJobStats::Type type) {
	util::LockGuard<util::Mutex> guard(lock_);
	return getEntry(type).failOnExcess_;
}

void PriorityJobMonitor::applyJobMemoryLimits(
		util::AllocatorLimitter &limitter_, bool &failOnExcess) {
	util::LockGuard<util::Mutex> guard(lock_);
	Entry &entry = getEntry(PriorityJobStats::TYPE_MEMORY_USE);

	if (entry.essentialThreshold_ > 0) {
		const size_t essential = static_cast<size_t>(std::min<uint64_t>(
				static_cast<uint64_t>(
						std::max<int64_t>(entry.essentialThreshold_, 0)),
				std::numeric_limits<size_t>::max()));
		limitter_.setEssential(essential);
	}

	failOnExcess = entry.failOnExcess_;
}

void PriorityJobMonitor::updateThreshold(
		util::LockGuard<util::Mutex>&, PriorityJobStats::Type type,
		bool forLimit) {
	Entry &entry = getEntry(type);

	int64_t &threshold = (forLimit ? entry.limitThreshold_ : entry.threshold_);

	const double total = static_cast<double>(entry.total_);
	const double rate =
			static_cast<double>(forLimit ? entry.limitRate_ : entry.rate_);

	threshold = static_cast<int64_t>(total * rate);
}

void PriorityJobMonitor::acceptStats(
		util::LockGuard<util::Mutex>&, const JobId &jobId,
		const PriorityJobStats &prev, const PriorityJobStats *next,
		uint32_t &reportableTypes, uint32_t &exceededTypes,
		uint32_t &selfExceededTypes) {
	reportableTypes = 0;
	exceededTypes = 0;
	selfExceededTypes = 0;

	for (uint32_t i = 0; i < ENTRY_COUNT; i++) {
		Entry &entry = entryList_[i];
		entry.current_ -= prev.valueList_[i].value_;

		if (next == NULL) {
			if (entry.valueMap_.get() != NULL) {
				entry.valueMap_->erase(jobId);
			}
			continue;
		}

		const int64_t nextValue = next->valueList_[i].value_;
		entry.current_ += nextValue;

		if (entry.valueMap_.get() != NULL) {
			entry.valueMap_->insert(std::make_pair(jobId, nextValue));
		}

		if (entry.threshold_ > 0 && nextValue > entry.threshold_) {
			reportableTypes |= (1 << i);
		}

		if (entry.failOnExcess_ && entry.current_ > entry.total_) {
			exceededTypes |= (1 << i);
		}

		if (entry.limitThreshold_ > 0 &&
				nextValue > entry.limitThreshold_ &&
				nextValue > entry.essentialThreshold_) {
			selfExceededTypes |= (1 << i);
		}
	}
}

bool PriorityJobMonitor::acceptReport(
		util::LockGuard<util::Mutex>&, const JobId &jobId,
		int64_t currentTime, uint32_t reportableTypes) {
	if (reportableTypes == 0) {
		return false;
	}

	while (!reportHistory_.empty()) {
		const HistoryElement &elem = reportHistory_.front();

		if (elem.reportedTime_ >= currentTime - reportInterval_) {
			break;
		}

		reportedJobs_.erase(elem.jobId_);
		reportHistory_.pop_front();
	}

	if (reportedJobs_.find(jobId) != reportedJobs_.end()) {
		return false;
	}

	if (reportHistory_.size() >= static_cast<size_t>(reportLimit_)) {
		return false;
	}

	reportedJobs_.insert(jobId);
	reportHistory_.push_back(HistoryElement(currentTime, jobId));
	return true;
}

void PriorityJobMonitor::adjustJobs(
		PriorityJobContext &cxt, uint32_t exceededTypes,
		const JobId *targetJobId) {
	if (exceededTypes == 0) {
		return;
	}

	LocalJobIdSet visitedJobs(cxt.getAllocator());
	for (uint32_t i = 0; i < ENTRY_COUNT; i++) {
		if ((exceededTypes & (1 << i)) == 0) {
			continue;
		}

		const PriorityJobStats::Type type =
				static_cast<PriorityJobStats::Type>(i);

		if (targetJobId != NULL) {
			PriorityJob job;
			if (jobTable_.findJob(*targetJobId, job)) {
				adjustJobAt(cxt, type, job);
			}
			break;
		}

		PriorityJobStatValue reducibleValue;
		for (JobId jobId; findExceededJob(
				jobId, type, visitedJobs, reducibleValue);) {
			PriorityJob job;
			if (jobTable_.findJob(jobId, job)) {
				adjustJobAt(cxt, type, job);
			}
		}
	}
}

void PriorityJobMonitor::adjustJobAt(
		PriorityJobContext &cxt, PriorityJobStats::Type type,
		PriorityJob &job) {
	try {
		if (type == PriorityJobStats::TYPE_SQL_STORE_USE_LAST) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_STORE_USE_EXCEEDED, "");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_TOTAL_MEMORY_EXCEEDED, "");
		}
	}
	catch (...) {
		std::exception e;
		const util::Exception localException = GS_EXCEPTION_CONVERT(e, "");
		job.cancel(cxt, localException, NULL);
	}
}

bool PriorityJobMonitor::findExceededJob(
		JobId &jobId, PriorityJobStats::Type type,
		LocalJobIdSet &visitedJobs, PriorityJobStatValue &reducibleValue) {
	jobId = JobId();
	util::LockGuard<util::Mutex> guard(lock_);

	Entry &entry = getEntry(type);
	if (entry.valueMap_.get() == NULL) {
		return false;
	}

	if (!entry.failOnExcess_ ||
			entry.current_ - reducibleValue.value_ < entry.total_) {
		return false;
	}

	int64_t foundValue = 0;
	JobId foundJobId;
	for (JobValueMap::iterator it = entry.valueMap_->begin();
			it != entry.valueMap_->end(); ++it) {
		if (visitedJobs.find(it->first) != visitedJobs.end()) {
			continue;
		}
		if (it->second > foundValue) {
			foundJobId = it->first;
			foundValue = it->second;
		}
	}

	if (foundValue > 0 && foundValue > entry.essentialThreshold_) {
		visitedJobs.insert(foundJobId);
		reducibleValue.value_ += foundValue;

		jobId = foundJobId;
		return true;
	}

	return false;
}

void PriorityJobMonitor::reportStats(
		PriorityJobContext &cxt, PriorityJob &job) {
	util::StackAllocator &alloc = cxt.getAllocator();
	util::Vector<JobResourceInfo> infoList(alloc);
	job.getResourceProfile(alloc, infoList, false, false);

	if (!infoList.empty()) {
		tracer_.traceResourceUse(
				cxt, env_, job, infoList.front(), planTraceEnabled_,
				planSizeLimit_);
	}
}

void PriorityJobMonitor::updatePeriodicMonitoringTime(int64_t currentTime) {
	if (reportInterval_ <= 0 ||
			periodicMonitoringTime_ + reportInterval_ > currentTime) {
		return;
	}

	util::LockGuard<util::Mutex> guard(lock_);
	const int64_t next = (currentTime + reportInterval_ - 1) /
			reportInterval_ * reportInterval_;

	if (next < periodicMonitoringTime_) {
		return;
	}
	periodicMonitoringTime_ = next;
}

PriorityJobMonitor::Entry& PriorityJobMonitor::getEntry(
		PriorityJobStats::Type type) {
	assert(static_cast<size_t>(type) < ENTRY_COUNT);
	return entryList_[type];
}


PriorityJobMonitor::Entry::Entry() :
		current_(0),
		total_(0),
		threshold_(0),
		limitThreshold_(0),
		essentialThreshold_(0),
		rate_(0),
		limitRate_(0),
		failOnExcess_(false) {
}


PriorityJobMonitor::HistoryElement::HistoryElement(
		int64_t reportedTime, const JobId &jobId) :
		reportedTime_(reportedTime),
		jobId_(jobId) {
}


PriorityJobMonitor::StatsActions::StatsActions() :
		exceededTypes_(0),
		selfExceededTypes_(0),
		reportable_(false) {
}


PriorityJobEnvironment::PriorityJobEnvironment(const Info &info) :
		info_(info),
		defaultStoreGroup_(*info_.store_) {
}

const PriorityJobConfig& PriorityJobEnvironment::getConfig() {
	return info_.jobConfig_;
}

SQLVariableSizeGlobalAllocator* PriorityJobEnvironment::getGlobalVarAllocator() {
	return info_.globalVarAlloc_;
}

util::AllocatorLimitter* PriorityJobEnvironment::getTotalAllocatorLimitter() {
	return info_.totalAllocLimitter_;
}

LocalTempStore* PriorityJobEnvironment::getStore() {
	return info_.store_;
}

LocalTempStore::Group& PriorityJobEnvironment::getDefaultStoreGroup() {
	return defaultStoreGroup_;
}

const SQLProcessorConfig* PriorityJobEnvironment::getProcessorConfig() {
	return info_.procConfig_;
}

ClusterService* PriorityJobEnvironment::getClusterService() {
	return info_.clusterService_;
}

PartitionTable* PriorityJobEnvironment::getPartitionTable() {
	return info_.partitionTable_;
}

TransactionService* PriorityJobEnvironment::getTransactionService() {
	return info_.transactionService_;
}

TransactionManager* PriorityJobEnvironment::getTransactionManager() {
	return info_.transactionManager_;
}

DataStoreConfig* PriorityJobEnvironment::getDataStoreConfig() {
	return info_.dataStoreConfig_;
}

PartitionList* PriorityJobEnvironment::getPartitionList() {
	return info_.partitionList_;
}

SQLExecutionManager* PriorityJobEnvironment::getExecutionManager() {
	return info_.executionManager_;
}

JobManager* PriorityJobEnvironment::getJobManager() {
	return info_.jobManager_;
}

JobTransporter::Engine& PriorityJobEnvironment::getTransporterEngine() {
	if (transporterEngine_.get() == NULL) {
		transporterEngine_ = UTIL_MAKE_LOCAL_UNIQUE(
				transporterEngine_, JobTransporter::DefaultEngine,
				*getJobManager()->getSQLService()->getEE());
	}
	return *transporterEngine_;
}

PriorityJobWorker::Engine& PriorityJobEnvironment::getWorkerEngine(
		bool forSql, bool forBackend) {
	util::LocalUniquePtr<PriorityJobWorker::DefaultEngine> &engine = (forSql ?
			(forBackend ? workerSqlBackEngine_ : workerSqlFrontEngine_) :
			workerTransactionEngine_);
	if (engine.get() == NULL) {
		EventEngine *base = (forSql ?
				(forBackend ?
						&getBackendEngineBase() :
						getJobManager()->getSQLService()->getEE()) :
				getTransactionService()->getEE());
		assert(base != NULL);
		engine = UTIL_MAKE_LOCAL_UNIQUE(
				engine, PriorityJobWorker::DefaultEngine, *base);
	}
	return *engine;
}

EventEngine& PriorityJobEnvironment::getBackendEngineBase() {
	if (sqlBackEngineBase_.get() == NULL) {
		EventEngine::Config config;
		config.concurrency_ = info_.jobConfig_.concurrency_ + 1;
		config.partitionCount_ = info_.jobConfig_.concurrency_ + 1;

		EventEngine::Source source(*info_.eeVarAlloc_, *info_.eeFixedAlloc_);
		sqlBackEngineBase_ = UTIL_MAKE_LOCAL_UNIQUE(
				sqlBackEngineBase_, EventEngine, config, source, "sqlBackend");
	}
	return *sqlBackEngineBase_;
}

int64_t PriorityJobEnvironment::getMonotonicTime() {
	return sqlBackEngineBase_->getMonotonicTime();
}

void PriorityJobEnvironment::handleLegacyJobContol(
		EventContext &ec, Event &ev) {
	getJobManager()->getSQLService()->handleLegacyJobContol(ec, ev);
}

bool PriorityJobEnvironment::checkEventContinuable(
		EventContext *ec, Event *ev) {
	if (ec == NULL || ev == NULL) {
		assert(false);
		return false;
	}

	const int64_t startTime = ec->getHandlerStartMonotonicTime();
	const int64_t limitInterval =
			JobManager::DEFAULT_TASK_INTERRUPTION_INTERVAL;
	const bool orgPending = true;
	const bool pending = getJobManager()->checkExecutableTask(
			*ec, *ev, startTime, limitInterval, orgPending);

	return !pending;
}

PriorityJobTracer& PriorityJobEnvironment::prepareTracer(
		util::LocalUniquePtr<PriorityJobTracer> &base) {
	if (info_.customTracer_ != NULL) {
		return *info_.customTracer_;
	}

	base = UTIL_MAKE_LOCAL_UNIQUE(base, PriorityJobTracer);
	return *base;
}


PriorityJobEnvironment::Info::Info(const PriorityJobConfig &jobConfig) :
		jobConfig_(jobConfig),
		globalVarAlloc_(NULL),
		totalAllocLimitter_(NULL),
		store_(NULL),
		procConfig_(NULL),
		clusterService_(NULL),
		partitionTable_(NULL),
		transactionService_(NULL),
		transactionManager_(NULL),
		dataStoreConfig_(NULL),
		partitionList_(NULL),
		executionManager_(NULL),
		jobManager_(NULL),
		eeVarAlloc_(NULL),
		eeFixedAlloc_(NULL),
		customTracer_(NULL) {
}


PriorityJobContext::PriorityJobContext(const Source &source) :
		alloc_(source.alloc_),
		varAlloc_(source.varAlloc_),
		blockPool_(source.blockPool_),
		senderNode_(source.senderNode_),
		eventContext_(source.eventContext_),
		sourceEvent_(source.sourceEvent_),
		env_(source.env_),
		monotonicTime_(0),
		monotonicTimeResolved_(false) {
}

util::StackAllocator& PriorityJobContext::getAllocator() {
	return alloc_;
}

PriorityJobAllocator& PriorityJobContext::getVarAllocator() {
	return varAlloc_;
}

JobBufferedBlockPool& PriorityJobContext::getBlockPool() {
	if (blockPool_ == NULL) {
		return errorBlockPool();
	}
	return *blockPool_;
}

const JobNode& PriorityJobContext::getSenderNode() {
	return senderNode_;
}

int64_t PriorityJobContext::resolveEventMonotonicTime() {
	if (!monotonicTimeResolved_) {
		if (env_ == NULL) {
			monotonicTime_ =
					resolveEventContext().getHandlerStartMonotonicTime();
		}
		else {
			monotonicTime_ = env_->getMonotonicTime();
		}
		monotonicTimeResolved_ = true;
	}
	return monotonicTime_;
}

EventContext& PriorityJobContext::resolveEventContext() {
	if (eventContext_ == NULL) {
		return errorEventContext();
	}
	return *eventContext_;
}

EventContext* PriorityJobContext::getEventContext() {
	return eventContext_;
}

Event* PriorityJobContext::getSourceEvent() {
	return sourceEvent_;
}

JobBufferedBlockPool& PriorityJobContext::errorBlockPool() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Block pool not assigned");
}

EventContext& PriorityJobContext::errorEventContext() {
	GS_THROW_USER_ERROR_TRACE(GS_ERROR_JOB_INTERNAL, "Event context not assigned");
}


PriorityJobContextSource::PriorityJobContextSource(
		util::StackAllocator &alloc, PriorityJobAllocator &varAlloc) :
		alloc_(alloc),
		varAlloc_(varAlloc),
		blockPool_(NULL),
		eventContext_(NULL),
		sourceEvent_(NULL),
		env_(NULL) {
}

PriorityJobContextSource PriorityJobContextSource::ofEventContext(
		EventContext &ec) {
	PriorityJobContextSource source(
			ec.getAllocator(), ec.getVariableSizeAllocator());
	source.eventContext_ = &ec;
	return source;
}

PriorityJobContextSource PriorityJobContextSource::ofEvent(
		EventContext &ec, Event &sourceEvent) {
	PriorityJobContextSource source(
			ec.getAllocator(), ec.getVariableSizeAllocator());
	source.eventContext_ = &ec;
	source.sourceEvent_ = &sourceEvent;
	return source;
}

PriorityJobContextSource PriorityJobContextSource::ofEnvironment(
		util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
		PriorityJobEnvironment &env) {
	PriorityJobContextSource source(alloc, varAlloc);
	source.env_ = &env;
	return source;
}


PriorityJobController::PriorityJobController(const Source &source) :
		env_(source.env_),
		holdersPool_(env_.getConfig()),
		blockPool_(*env_.getStore(), env_.getConfig().resolveBlockPoolSize()),
		tracer_(env_.prepareTracer(baseTracer_)),
		transporter_(env_.getTransporterEngine(), env_.getConfig()),
		workerTable_(makeWorkerTableSource(
				env_.getConfig(), env_, blockPool_, transporter_, tracer_)),
		monitor_(env_, tracer_, jobTable_) {
	transporter_.setUpHandlers(*this);
	workerTable_.setUpHandlers();
}

void PriorityJobController::deploy(
		PriorityJobContext &cxt, const JobMessages::Deploy &msg) {
	transporter_.prepare(cxt);

	PriorityJob::Source source(*this, msg.base_.jobId_);
	PriorityJob job = getJobTable().resolveJob(source);

	assert(msg.jobInfo_ != NULL);
	job.deploy(cxt, *msg.jobInfo_);
}

void PriorityJobController::readyOutput(
		PriorityJobContext &cxt, const JobMessages::ReadyOutput &msg) {
	PriorityTask task;
	if (!findTask(msg.base_.jobId_, msg.taskId_, task)) {
		return;
	}

	PriorityJob job = task.getJob();
	job.readyOutput(cxt, task);
}

void PriorityJobController::pipe(
		PriorityJobContext &cxt, const JobMessages::Pipe &msg) {
	JobNode node = cxt.getSenderNode();
	acceptNodeInput(cxt, node);

	PriorityTask task;
	if (!prepareTask(
			cxt, msg.base_.jobId_, msg.taskId_, msg.ioInfo_, task)) {
		return;
	}
	task.addInput(
			cxt, msg.input_, PriorityTask::InputOption::empty(), msg.block_);
}

void PriorityJobController::finish(
		PriorityJobContext &cxt, const JobMessages::Finish &msg) {
	PriorityTask task;
	if (!prepareTask(
			cxt, msg.base_.jobId_, msg.taskId_, msg.ioInfo_, task)) {
		return;
	}
	task.finishInput(cxt, msg.input_);
}

void PriorityJobController::cancel(
		PriorityJobContext &cxt, const JobMessages::Cancel &msg) {
	PriorityJob job = prepareJob(msg.base_.jobId_);
	job.cancel(cxt, *msg.exception_, &cxt.getSenderNode());
}

void PriorityJobController::complete(
		PriorityJobContext &cxt, const JobMessages::Complete &msg) {
	PriorityJob job = prepareJob(msg.base_.jobId_);
	job.complete(cxt, msg.profilerInfo_);
}

void PriorityJobController::changeTaskPriority(
		PriorityJobContext &cxt, const JobMessages::ChangeTaskPriority &msg) {
	PriorityTask task;
	if (!findTask(msg.base_.jobId_, msg.taskId_, task)) {
		return;
	}
	task.getJob().changeTaskPriority(cxt, task, msg.priorInput_);
}

void PriorityJobController::applyTaskPathPriority(
		PriorityJobContext &cxt,
		const JobMessages::ApplyTaskPathPriority &msg) {
	PriorityJob job;
	if (!getJobTable().findJob(msg.base_.jobId_, job)) {
		return;
	}
	job.applyTaskPathPriority(cxt, msg.priorityList_, msg.checking_);
}

void PriorityJobController::growTaskOutput(
		PriorityJobContext &cxt, const JobMessages::GrowTaskOutput &msg) {
	PriorityTask task;
	if (!prepareTask(
			cxt, msg.base_.jobId_, msg.taskId_, msg.ioInfo_, task)) {
		return;
	}
	task.growOutputCapacity(cxt, msg.output_, msg.capacity_);
}

void PriorityJobController::growNodeOutput(
		PriorityJobContext &cxt, const JobMessages::GrowNodeOutput &msg) {
	JobNode node = cxt.getSenderNode();
	node.growOutputCapacity(msg.capacity_);
	getWorkerTable().getBackendIOWorker().resumeNodeOutput(cxt, node);
}

void PriorityJobController::reportSent(
		PriorityJobContext &cxt, const JobMessages::ReportSent &msg) {
	getWorkerTable().getBackendIOWorker().reportSent(cxt, msg.sequenceNum_);

	PriorityTask task;
	if (findTaskOptional(msg.base_.jobId_, msg.taskId_, true, task)) {
		task.reportOutputSent(msg.networkBusy_, msg.size_);
	}
}

void PriorityJobController::validateNode(
		PriorityJobContext &cxt, const JobMessages::ValidateNode &msg) {
	const JobNode &node = cxt.getSenderNode();
	const uint64_t validationCode = msg.validationCode_;

	if (msg.ackRequired_) {
		getTransporter().ackNodeValidation(cxt, node, validationCode);
	}
	else {
		getTransporter().accpetNodeValidationResult(node, validationCode);
		getWorkerTable().getBackendIOWorker().accpetNodeValidationAck(
				cxt, node, validationCode);
	}

	acceptJobActivities(cxt, node, msg.jobActivityList_);
}

void PriorityJobController::invalidateNode(
		PriorityJobContext &cxt, const JobNode &node,
		std::exception &exception) {
	util::StackAllocator &alloc = cxt.getAllocator();

	util::Vector<PriorityJob> jobList(alloc);
	jobTable_.listJobs(jobList);

	const util::Exception localException = GS_EXCEPTION_CONVERT(exception, "");
	for (util::Vector<PriorityJob>::iterator it = jobList.begin();
			it != jobList.end(); ++it) {
		if (it->isNodeRelated(node)) {
			it->forceCancel(cxt, localException);
		}
	}

	getWorkerTable().invalidateNode(cxt, node);
}

void PriorityJobController::getStatProfile(
		util::StackAllocator &alloc, PriorityJobAllocator &varAlloc,
		const JobId *targetJobId, StatJobProfiler &profiler) {
	util::Vector<JobId> jobIdList(alloc);

	if (targetJobId == NULL) {
		getJobIdList(jobIdList);
	}
	else {
		jobIdList.push_back(*targetJobId);
	}

	for (util::Vector<JobId>::iterator it = jobIdList.begin();
			it != jobIdList.end(); ++it) {
		PriorityJob job;
		if (getJobTable().findJob(*it, job)) {
			StatJobProfilerInfo jobProf(alloc, *it);
			DatabaseId dbId;
			if (job.getStatProfile(alloc, jobProf, dbId)) {
				profiler.jobProfs_.push_back(jobProf);
			}
		}
	}

	PriorityJobContext cxt(
			PriorityJobContextSource::ofEnvironment(alloc, varAlloc, env_));
	profiler.workerProfs_ = getWorkerTable().getStatProfile(cxt);

	const bool empty =
			(profiler.jobProfs_.empty() && profiler.workerProfs_ == NULL);
	getTransporter().getStatProfile(alloc, profiler, empty);
}

void PriorityJobController::getResourceProfile(
		util::StackAllocator &alloc, util::Vector<JobResourceInfo> &infoList,
		bool byTask, bool limited) {
	util::Vector<PriorityJob> jobList(alloc);
	jobTable_.listJobs(jobList);

	for (util::Vector<PriorityJob>::iterator it = jobList.begin();
			it != jobList.end(); ++it) {
		it->getResourceProfile(alloc, infoList, byTask, limited);
	}
}

JobMessages::JobActivityList* PriorityJobController::checkJobActivities(
		PriorityJobContext &cxt, int64_t prevCheckTime) {
	util::StackAllocator &alloc = cxt.getAllocator();

	JobMessages::JobActivityList *activityList =
			ALLOC_NEW(alloc) JobMessages::JobActivityList(alloc);

	util::Vector<PriorityJob> jobList(alloc);
	jobTable_.listJobs(jobList);

	for (util::Vector<PriorityJob>::iterator it = jobList.begin();
			it != jobList.end(); ++it) {
		activityList->push_back(JobMessages::JobActivityEntry(
				it->getId(), it->checkTotalActivity(cxt, prevCheckTime)));
	}

	return activityList;
}

void PriorityJobController::getJobInfoList(
		util::StackAllocator& alloc, StatJobIdList &infoList) {
	util::Vector<JobResourceInfo> resInfoList(alloc);
	getResourceProfile(alloc, resInfoList, false, false);

	for (util::Vector<JobResourceInfo>::iterator it = resInfoList.begin();
			it != resInfoList.end(); ++it) {
		infoList.infoList_.push_back(StatJobIdListInfo(alloc));
		StatJobIdListInfo &info = infoList.infoList_.back();

		CommonUtility::writeTimeStr(
				util::DateTime(it->startTime_), info.startTime_);
		info.jobId_ = it->requestId_;
	}
}

void PriorityJobController::getJobIdList(util::Vector<JobId> &idList) {
	jobTable_.listJobIds(idList);
}

size_t PriorityJobController::getJobCount() {
	return jobTable_.getJobCount();
}

void PriorityJobController::activateControl(bool activated) {
	getTransporter().activateControl(activated);
}

void PriorityJobController::resetCache() {
	blockPool_.getStore().resetCache();
	blockPool_.resetPooledBlocks();
}

const PriorityJobConfig& PriorityJobController::getConfig() {
	return env_.getConfig();
}

PriorityJobEnvironment& PriorityJobController::getEnvironment() {
	return env_;
}

PriorityJobHolders::Pool& PriorityJobController::getHoldersPool() {
	return holdersPool_;
}

JobBufferedBlockPool& PriorityJobController::getBlockPool() {
	return blockPool_;
}

PriorityJobTracer& PriorityJobController::getTracer() {
	return tracer_;
}

JobTransporter& PriorityJobController::getTransporter() {
	return transporter_;
}

PriorityJobTable& PriorityJobController::getJobTable() {
	return jobTable_;
}

PriorityJobWorkerTable& PriorityJobController::getWorkerTable() {
	return workerTable_;
}

PriorityJobMonitor& PriorityJobController::getMonitor() {
	return monitor_;
}

PriorityJob PriorityJobController::prepareJob(const JobId &jobId) {
	PriorityJob job;
	if (!getJobTable().findJob(jobId, job)) {
		PriorityJob::Source source(*this, jobId);
		job = getJobTable().resolveJob(source);
	}
	return job;
}

bool PriorityJobController::prepareTask(
		PriorityJobContext &cxt, const JobId &jobId, TaskId taskId,
		const JobMessages::TaskIOInfo &ioInfo, PriorityTask &task) {
	PriorityJob job = prepareJob(jobId);
	return job.prepareTask(
			cxt, taskId, task, ioInfo.inputCount_, ioInfo.outputCount_);
}

bool PriorityJobController::findTask(
		const JobId &jobId, TaskId taskId, PriorityTask &task) {
	const bool optional = false;
	return findTaskOptional(jobId, taskId, optional, task);
}

bool PriorityJobController::findTaskOptional(
		const JobId &jobId, TaskId taskId, bool optional, PriorityTask &task) {
	PriorityJob job;
	if (!getJobTable().findJob(jobId, job)) {
		return false;
	}

	if (optional) {
		return job.findTask(taskId, true, task);
	}

	task = job.getTask(taskId);
	return true;
}

void PriorityJobController::acceptNodeInput(
		PriorityJobContext &cxt, JobNode &node) {
	uint64_t nextCapacity;
	if (node.acceptBlockInput(nextCapacity)) {
		JobMessages::GrowNodeOutput msg;
		msg.capacity_ = nextCapacity;
		getTransporter().send(cxt, node, msg);
	}
}

void PriorityJobController::acceptJobActivities(
		PriorityJobContext &cxt, const JobNode &node,
		const JobMessages::JobActivityList *activityList) {
	if (activityList == NULL) {
		return;
	}

	for (JobMessages::JobActivityList::const_iterator it =
			activityList->begin(); it != activityList->end(); ++it) {
		PriorityJob job;
		if (jobTable_.findJob(it->first, job)) {
			job.updateRemoteActivity(cxt, node, it->second);
		}
	}
}

PriorityJobWorkerTable::Source PriorityJobController::makeWorkerTableSource(
		const PriorityJobConfig &config, PriorityJobEnvironment &env,
		JobBufferedBlockPool &blockPool, JobTransporter &transporter,
		PriorityJobTracer &tracer) {
	return PriorityJobWorkerTable::Source(
			env.getWorkerEngine(true, false),
			env.getWorkerEngine(true, true),
			env.getWorkerEngine(false, false),
			blockPool,
			transporter,
			tracer,
			config);
}


PriorityJobController::Source::Source(PriorityJobEnvironment &env) :
		env_(env) {
}
