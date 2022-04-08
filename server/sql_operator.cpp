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

#include "sql_operator_utils.h"



SQLOps::OpConfig::OpConfig() {
	std::fill(valueList_, valueList_ + VALUE_COUNT, -1);
}

SQLOps::OpConfig::Value SQLOps::OpConfig::get(Type type) const {
	return valueList_[getIndex(type)];
}

void SQLOps::OpConfig::set(Type type, Value value) {
	valueList_[getIndex(type)] = value;
}

bool SQLOps::OpConfig::isSame(const OpConfig &another) const {
	if (this == &another) {
		return true;
	}

	for (size_t i = 0; i < VALUE_COUNT; i++) {
		const Type type = static_cast<Type>(i);
		if (get(type) != another.get(type)) {
			return false;
		}
	}

	return true;
}

SQLOps::OpConfig SQLOps::OpConfig::resolveAll(const OpConfig *base) const {
	OpConfig dest;

	for (size_t i = 0; i < VALUE_COUNT; i++) {
		const Type type = static_cast<Type>(i);
		Value value = get(type);
		if (value < 0) {
			value = resolve(type, base);
		}
		dest.set(type, value);
	}

	return dest;
}

SQLOps::OpConfig::Value SQLOps::OpConfig::resolve(
		Type type, const OpConfig *base) {
	if (base == NULL) {
		return -1;
	}
	return base->get(type);
}

size_t SQLOps::OpConfig::getIndex(Type type) const {
	assert(0 <= type);
	const size_t index = static_cast<size_t>(type);

	assert(index < VALUE_COUNT);
	return index;
}


SQLOps::ContainerLocation::ContainerLocation() :
		type_(SQLType::END_TABLE),
		dbVersionId_(0),
		id_(0),
		schemaVersionId_(0),
		partitioningVersionId_(-1),
		schemaVersionSpecified_(false),
		expirable_(false),
		indexActivated_(false),
		multiIndexActivated_(false) {
}


SQLOps::OpCode::OpCode(SQLOpTypes::Type type) :
		type_(type),
		config_(NULL),
		inputCount_(0),
		outputCount_(0),
		drivingInput_(-1),
		containerLocation_(NULL),
		keyColumnList_(NULL),
		middleKeyColumnList_(NULL),
		sideKeyColumnList_(NULL),
		joinPredicate_(NULL),
		filterPredicate_(NULL),
		indexConditionList_(NULL),
		pipeProjection_(NULL),
		middleProjection_(NULL),
		finishProjection_(NULL),
		aggrPhase_(SQLType::END_AGG_PHASE),
		joinType_(SQLType::END_JOIN),
		unionType_(SQLType::END_UNION),
		limit_(-1),
		offset_(-1),
		subLimit_(-1),
		subOffset_(-1) {
}

SQLOpTypes::Type SQLOps::OpCode::getType() const {
	return type_;
}

const SQLOps::OpConfig* SQLOps::OpCode::getConfig() const {
	return config_;
}

uint32_t SQLOps::OpCode::getInputCount() const {
	return inputCount_;
}

uint32_t SQLOps::OpCode::getOutputCount() const {
	return outputCount_;
}

int32_t SQLOps::OpCode::getDrivingInput() const {
	return drivingInput_;
}

const SQLOps::ContainerLocation&
SQLOps::OpCode::getContainerLocation() const {
	assert(containerLocation_ != NULL);
	return *containerLocation_;
}

const SQLOps::ContainerLocation*
SQLOps::OpCode::findContainerLocation() const {
	return containerLocation_;
}

const SQLValues::CompColumnList& SQLOps::OpCode::getKeyColumnList(
		SQLOpTypes::OpCodeKey key) const {
	const SQLValues::CompColumnList *list = findKeyColumnList(key);
	assert(list != NULL);
	return *list;
}

const SQLValues::CompColumnList& SQLOps::OpCode::getKeyColumnList() const {
	const SQLValues::CompColumnList *list = findKeyColumnList();
	assert(list != NULL);
	return *list;
}

const SQLValues::CompColumnList&
SQLOps::OpCode::getMiddleKeyColumnList() const {
	const SQLValues::CompColumnList *list = findMiddleKeyColumnList();
	assert(list != NULL);
	return *list;
}

const SQLValues::CompColumnList* SQLOps::OpCode::findKeyColumnList(
		SQLOpTypes::OpCodeKey key) const {
	switch (key) {
	case SQLOpTypes::CODE_KEY_COLUMNS:
		return findKeyColumnList();
	case SQLOpTypes::CODE_MIDDLE_KEY_COLUMNS:
		return findMiddleKeyColumnList();
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

const SQLValues::CompColumnList* SQLOps::OpCode::findKeyColumnList() const {
	return keyColumnList_;
}

const SQLValues::CompColumnList*
SQLOps::OpCode::findMiddleKeyColumnList() const {
	return middleKeyColumnList_;
}

const SQLOps::CompColumnListPair*
SQLOps::OpCode::findSideKeyColumnList() const {
	return sideKeyColumnList_;
}

const SQLOps::Expression* SQLOps::OpCode::getJoinPredicate() const {
	return joinPredicate_;
}

const SQLOps::Expression* SQLOps::OpCode::getFilterPredicate() const {
	return filterPredicate_;
}

const SQLExprs::IndexConditionList&
SQLOps::OpCode::getIndexConditionList() const {
	assert(indexConditionList_ != NULL);
	return *indexConditionList_;
}

const SQLExprs::IndexConditionList*
SQLOps::OpCode::findIndexConditionList() const {
	return indexConditionList_;
}

const SQLOps::Projection* SQLOps::OpCode::getProjection(
		SQLOpTypes::OpCodeKey key) const {
	switch (key) {
	case SQLOpTypes::CODE_PIPE_PROJECTION:
		return getPipeProjection();
	case SQLOpTypes::CODE_MIDDLE_PROJECTION:
		return getMiddleProjection();
	case SQLOpTypes::CODE_FINISH_PROJECTION:
		return getFinishProjection();
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

const SQLOps::Projection* SQLOps::OpCode::getPipeProjection() const {
	return pipeProjection_;
}

const SQLOps::Projection* SQLOps::OpCode::getMiddleProjection() const {
	return middleProjection_;
}

const SQLOps::Projection* SQLOps::OpCode::getFinishProjection() const {
	return finishProjection_;
}

SQLType::AggregationPhase SQLOps::OpCode::getAggregationPhase() const {
	return aggrPhase_;
}

SQLType::JoinType SQLOps::OpCode::getJoinType() const {
	return joinType_;
}

SQLType::UnionType SQLOps::OpCode::getUnionType() const {
	return unionType_;
}

int64_t SQLOps::OpCode::getLimit() const {
	return limit_;
}

int64_t SQLOps::OpCode::getOffset() const {
	return offset_;
}

int64_t SQLOps::OpCode::getSubLimit() const {
	return subLimit_;
}

int64_t SQLOps::OpCode::getSubOffset() const {
	return subOffset_;
}

void SQLOps::OpCode::setType(SQLOpTypes::Type type) {
	type_ = type;
}

void SQLOps::OpCode::setConfig(const OpConfig *config) {
	config_ = config;
}

void SQLOps::OpCode::setInputCount(uint32_t count) {
	inputCount_ = count;
}

void SQLOps::OpCode::setOutputCount(uint32_t count) {
	outputCount_ = count;
}

void SQLOps::OpCode::setDrivingInput(int32_t input) {
	drivingInput_ = input;
}

void SQLOps::OpCode::setContainerLocation(const ContainerLocation *location) {
	containerLocation_ = location;
}

void SQLOps::OpCode::setKeyColumnList(const SQLValues::CompColumnList *list) {
	keyColumnList_ = list;
}

void SQLOps::OpCode::setMiddleKeyColumnList(
		const SQLValues::CompColumnList *list) {
	middleKeyColumnList_ = list;
}

void SQLOps::OpCode::setSideKeyColumnList(const CompColumnListPair *list) {
	sideKeyColumnList_ = list;
}

void SQLOps::OpCode::setJoinPredicate(const Expression *expr) {
	joinPredicate_ = expr;
}

void SQLOps::OpCode::setFilterPredicate(const Expression *expr) {
	filterPredicate_ = expr;
}

void SQLOps::OpCode::setIndexConditionList(
		const SQLExprs::IndexConditionList *list) {
	indexConditionList_ = list;
}

void SQLOps::OpCode::setPipeProjection(const Projection *proj) {
	pipeProjection_ = proj;
}

void SQLOps::OpCode::setMiddleProjection(const Projection *proj) {
	middleProjection_ = proj;
}

void SQLOps::OpCode::setFinishProjection(const Projection *proj) {
	finishProjection_ = proj;
}

void SQLOps::OpCode::setAggregationPhase(
		SQLType::AggregationPhase aggrPhase) {
	aggrPhase_ = aggrPhase;
}

void SQLOps::OpCode::setJoinType(SQLType::JoinType joinType) {
	joinType_ = joinType;
}

void SQLOps::OpCode::setUnionType(SQLType::UnionType unionType) {
	unionType_ = unionType;
}

void SQLOps::OpCode::setLimit(int64_t limit) {
	limit_ = limit;
}

void SQLOps::OpCode::setOffset(int64_t offset) {
	offset_ = offset;
}

void SQLOps::OpCode::setSubLimit(int64_t limit) {
	subLimit_ = limit;
}

void SQLOps::OpCode::setSubOffset(int64_t offset) {
	subOffset_ = offset;
}


const SQLOpTypes::OpCodeKey SQLOps::OpCode::ProjectionIterator::KEY_LIST[] = {
	SQLOpTypes::CODE_PIPE_PROJECTION,
	SQLOpTypes::CODE_MIDDLE_PROJECTION,
	SQLOpTypes::CODE_FINISH_PROJECTION
};

const SQLOpTypes::OpCodeKey *const
SQLOps::OpCode::ProjectionIterator::KEY_LIST_END =
		(KEY_LIST + sizeof(KEY_LIST) / sizeof(*KEY_LIST));

SQLOps::OpCode::ProjectionIterator::ProjectionIterator(const OpCode *code) :
		keyIt_(KEY_LIST),
		code_(code),
		proj_(NULL) {
	next();
}

const SQLOps::Projection& SQLOps::OpCode::ProjectionIterator::get() const {
	assert(exists());
	return *proj_;
}

bool SQLOps::OpCode::ProjectionIterator::exists() const {
	return (proj_ != NULL);
}

void SQLOps::OpCode::ProjectionIterator::next() {
	if (code_ == NULL || keyIt_ == NULL) {
		assert(false);
		return;
	}

	const Projection *proj = NULL;
	for (; proj == NULL; ++keyIt_) {
		if (keyIt_ == KEY_LIST_END) {
			keyIt_ = NULL;
			break;
		}

		proj = code_->getProjection(*keyIt_);
	}
	proj_ = proj;
}


SQLOps::Operator::Operator() {
}

SQLOps::Operator::~Operator() {
}

void SQLOps::Operator::execute(OpContext &cxt) const {
	OpCursor cursor(cxt.getCursorSource());
	for (; cursor.exists(); cursor.next()) {
		cursor.get().execute(cursor.getContext());
	}
	cursor.release();
}

void SQLOps::Operator::compile(OpContext &cxt) const {
	cxt.getPlan().setCompleted();
}

const SQLOps::OpCode& SQLOps::Operator::getCode() const {
	return code_;
}


const SQLOps::OpFactory &SQLOps::OpFactory::defaultFactory_ =
		getFactoryForRegistrar();

SQLOps::OpFactory::~OpFactory() {
}

util::AllocUniquePtr<SQLOps::Operator>::ReturnType SQLOps::OpFactory::create(
		util::StdAllocator<void, void> &alloc, const OpCode &code) const {
	const SQLOpTypes::Type type = code.getType();
	assert(0 <= type && type < SQLOpTypes::END_OP);

	assert(funcTable_ != NULL);
	FactoryFunc func = (*funcTable_)[type];

	assert(func != NULL);
	util::AllocUniquePtr<Operator> op(func(alloc, code));
	op->code_ = code;

	return op;
}

void SQLOps::OpFactory::addDefaultEntry(
		SQLOpTypes::Type type, FactoryFunc func) {
	assert(0 <= type && type < SQLOpTypes::END_OP);

	assert(funcTable_ != NULL);
	FactoryFunc &dest = (*funcTable_)[type];

	assert(dest == NULL);
	dest = func;
}

const SQLOps::OpFactory& SQLOps::OpFactory::getDefaultFactory() {
	return defaultFactory_;
}

SQLOps::OpFactory& SQLOps::OpFactory::getFactoryForRegistrar() {
	static FactoryFunc funcTable[ENTRY_COUNT] = { NULL };
	static OpFactory factory(&funcTable);
	return factory;
}

SQLOps::OpFactory::OpFactory() :
		funcTable_(NULL) {
}

SQLOps::OpFactory::OpFactory(FactoryFunc (*funcTable)[ENTRY_COUNT]) :
		funcTable_(funcTable) {
}


SQLOps::OpRegistrar::OpRegistrar() throw() :
		factory_(&OpFactory::getFactoryForRegistrar()) {
}

SQLOps::OpRegistrar::OpRegistrar(const OpRegistrar &sub) throw() :
		factory_(NULL) {
	try {
		sub();
	}
	catch (...) {
		assert(false);
	}
}

void SQLOps::OpRegistrar::operator()() const {
	assert(false);
}


bool SQLOps::OpNodeId::isEmpty() const {
	return id_.isEmpty();
}

bool SQLOps::OpNodeId::operator==(const OpNodeId &another) const {
	return (id_ == another.id_);
}

bool SQLOps::OpNodeId::operator<(const OpNodeId &another) const {
	return (id_ < another.id_);
}


SQLOps::OpNode::OpNode(
		util::StackAllocator &alloc, const OpNodeId &id, OpPlan &plan,
		SQLOpTypes::Type type, uint32_t parentIndex) :
		id_(id),
		plan_(plan),
		inputIdList_(alloc),
		code_(type),
		parentIndex_(parentIndex) {
}

const SQLOps::OpNodeId& SQLOps::OpNode::getId() const {
	return id_;
}

const SQLOps::OpNode& SQLOps::OpNode::getInput(uint32_t index) const {
	assert(index < inputIdList_.size());
	return plan_.getNode(inputIdList_[index].first);
}

SQLOps::OpNode& SQLOps::OpNode::getInput(uint32_t index) {
	assert(index < inputIdList_.size());
	return plan_.getNode(inputIdList_[index].first);
}

uint32_t SQLOps::OpNode::getInputPos(uint32_t index) const {
	assert(index < inputIdList_.size());
	return inputIdList_[index].second;
}

void SQLOps::OpNode::setInput(
		uint32_t index, const OpNode &node, uint32_t pos) {
	plan_.prepareModification();
	if (index >= inputIdList_.size()) {
		inputIdList_.resize(index + 1, IdRefEntry());
	}
	inputIdList_[index] = IdRefEntry(node.id_, pos);
}

void SQLOps::OpNode::addInput(const OpNode &node, uint32_t pos) {
	plan_.prepareModification();
	inputIdList_.push_back(IdRefEntry(node.id_, pos));
}

uint32_t SQLOps::OpNode::getInputCount() const {
	return static_cast<uint32_t>(inputIdList_.size());
}

const SQLOps::OpCode& SQLOps::OpNode::getCode() const {
	return code_;
}

void SQLOps::OpNode::setCode(const OpCode &code) {
	plan_.prepareModification();
	const SQLOpTypes::Type orgType = code_.getType();
	code_ = code;
	code_.setType(orgType);
}

uint32_t SQLOps::OpNode::getParentIndex() const {
	return parentIndex_;
}


void SQLOps::OpNode::PlanTool::reset(
		OpNode &node, const OpNodeId &id, SQLOpTypes::Type type,
		uint32_t parentIndex) {
	node.id_ = id;
	node.inputIdList_.clear();
	node.code_ = OpCode(type);
	node.parentIndex_ = parentIndex;
}


SQLOps::OpPlan::OpPlan(
		util::StackAllocator &alloc,
		uint32_t parentInCount, uint32_t parentOutCount) :
		alloc_(alloc),
		nodeList_(alloc),
		parentInList_(alloc),
		parentOutList_(alloc),
		normalNodeList_(alloc),
		refCountListBase_(alloc),
		refCountList_(&refCountListBase_),
		outCountsCache_(alloc),
		idManager_(alloc),
		completed_(false) {
	for (uint32_t i = 0; i < parentInCount; i++) {
		parentInList_.push_back(
				newNode(SQLOpTypes::OP_PARENT_INPUT, i)->getId());
	}
	for (uint32_t i = 0; i < parentOutCount; i++) {
		parentOutList_.push_back(
				newNode(SQLOpTypes::OP_PARENT_OUTPUT, i)->getId());
	}
}

SQLOps::OpPlan::~OpPlan() {
	parentInList_.clear();
	parentOutList_.clear();
	normalNodeList_.clear();

	for (NodeList::const_iterator it = nodeList_.begin();
			it != nodeList_.end(); ++it) {
		ALLOC_DELETE(alloc_, *it);
	}
	nodeList_.clear();
}

SQLOps::OpNode& SQLOps::OpPlan::createNode(SQLOpTypes::Type opType) {
	prepareModification();
	const uint32_t parentIndex = std::numeric_limits<uint32_t>::max();
	OpNode *node = newNode(opType, parentIndex);
	normalNodeList_.push_back(node->getId());
	return *node;
}

const SQLOps::OpNode& SQLOps::OpPlan::getNode(const OpNodeId &id) const {
	return *nodeList_[id.id_.getIndex(idManager_)];
}

SQLOps::OpNode& SQLOps::OpPlan::getNode(const OpNodeId &id) {
	return *nodeList_[id.id_.getIndex(idManager_)];
}

const SQLOps::OpNode& SQLOps::OpPlan::getParentInput(uint32_t index) const {
	return getNode(parentInList_[index]);
}

uint32_t SQLOps::OpPlan::getParentInputCount() const {
	return static_cast<uint32_t>(parentInList_.size());
}

const SQLOps::OpNode& SQLOps::OpPlan::getParentOutput(uint32_t index) const {
	return getNode(parentOutList_[index]);
}

SQLOps::OpNode& SQLOps::OpPlan::getParentOutput(uint32_t index) {
	return getNode(parentOutList_[index]);
}

uint32_t SQLOps::OpPlan::getParentOutputCount() const {
	return static_cast<uint32_t>(parentOutList_.size());
}

void SQLOps::OpPlan::linkToAllParentOutput(const OpNode &node) {
	const uint32_t count = getParentOutputCount();
	for (uint32_t i = 0; i < count; i++) {
		getParentOutput(i).addInput(node, i);
	}
}

uint32_t SQLOps::OpPlan::findNextNodeIndex(const OpNodeId &id) const {
	if (id.isEmpty()) {
		return 0;
	}

	NodeIdList::const_iterator it =
			std::find(normalNodeList_.begin(), normalNodeList_.end(), id);
	if (it == normalNodeList_.end()) {
		return 0;
	}

	return static_cast<uint32_t>(it - normalNodeList_.begin() + 1);
}

const SQLOps::OpNode* SQLOps::OpPlan::findParentOutput(
		const OpNodeId &id, uint32_t pos) const {
	const uint32_t parentCount = getParentOutputCount();
	for (uint32_t i = 0; i < parentCount; i++) {
		const OpNode &parentNode = getParentOutput(i);
		const uint32_t inCount = parentNode.getInputCount();
		for (uint32_t j = 0; j < inCount; j++) {
			if (parentNode.getInput(j).getId() == id &&
					parentNode.getInputPos(j) == pos) {
				return &parentNode;
			}
		}
	}
	return NULL;
}

const SQLOps::OpNode* SQLOps::OpPlan::findNextNode(uint32_t startIndex) const {
	for (uint32_t i = startIndex; i < normalNodeList_.size(); i++) {
		const OpNodeId &id = normalNodeList_[i];
		const OpNode *node = nodeList_[id.id_.getIndex(idManager_)];
		if (node != NULL && !node->getId().isEmpty()) {
			return node;
		}
	}
	return NULL;
}

uint32_t SQLOps::OpPlan::getNodeOutputCount(const OpNodeId &id) const {
	const OpNode &node = getNode(id);

	const uint64_t byNodes =
			resolveReferenceCounts()[id.id_.getIndex(idManager_)].size();
	const uint64_t byCode =
			OpCodeBuilder::resolveOutputCount(node.getCode());
	return static_cast<uint32_t>(std::max(byNodes, byCode));
}

bool SQLOps::OpPlan::isMultiReferenced(
		const OpNodeId &id, uint32_t pos) const {
	const RefCountEntry &entry =
			resolveReferenceCounts()[id.id_.getIndex(idManager_)];
	if (pos >= entry.size()) {
		if (!completed_) {
			return false;
		}
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return entry[pos] > 1;
}

void SQLOps::OpPlan::prepareModification() {
	if (isCompleted()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	refCountList_->clear();
}

bool SQLOps::OpPlan::isCompleted() const {
	return completed_;
}

void SQLOps::OpPlan::setCompleted() {
	assert(!completed_);
	const RefCountEntry &outCounts = getAllNodeOutputCounts();
	for (NodeList::iterator it = nodeList_.begin();
			it != nodeList_.end(); ++it) {
		OpNode *node = *it;
		if (node == NULL || node->getId().isEmpty()) {
			continue;
		}

		const uint32_t inCount = node->getInputCount();
		{
			OpCode code = node->getCode();
			code.setInputCount(inCount);
			node->setCode(code);
		}
		for (uint32_t i = 0; i < inCount; i++) {
			OpNode& inNode = node->getInput(i);
			OpCode code = inNode.getCode();
			code.setOutputCount(outCounts[it - nodeList_.begin()]);
			inNode.setCode(code);
		}
	}
	completed_ = true;
}

bool SQLOps::OpPlan::isEmpty() const {
	return (nodeList_.size() == parentInList_.size() + parentOutList_.size());
}

SQLOps::OpNode* SQLOps::OpPlan::newNode(
		SQLOpTypes::Type opType, uint32_t parentIndex) {
	OpNodeId id;
	id.id_.assign(idManager_);
	const size_t index = id.id_.getIndex(idManager_);

	if (index >= nodeList_.size()) {
		nodeList_.resize(index + 1, NULL);
	}

	OpNode *&node = nodeList_[index];
	if (node == NULL) {
		node = ALLOC_NEW(alloc_) OpNode(
				alloc_, id, *this, opType, parentIndex);
	}
	else {
		OpNode::PlanTool::reset(*node, id, opType, parentIndex);
	}

	return node;
}

const SQLOps::OpPlan::RefCountEntry& SQLOps::OpPlan::getAllNodeOutputCounts() {
	RefCountEntry &outCounts = outCountsCache_;
	outCounts.assign(nodeList_.size(), 0);

	for (NodeList::iterator it = nodeList_.begin();
			it != nodeList_.end(); ++it) {
		OpNode *node = *it;
		if (node == NULL || node->getId().isEmpty()) {
			continue;
		}

		outCounts[it - nodeList_.begin()] = getNodeOutputCount(node->getId());
	}

	return outCounts;
}

const SQLOps::OpPlan::RefCountList&
SQLOps::OpPlan::resolveReferenceCounts() const {
	if (refCountList_->empty()) {
		refCountList_->assign(nodeList_.size(), RefCountEntry(alloc_));

		for (NodeList::const_iterator it = nodeList_.begin();
				it != nodeList_.end(); ++it) {
			if (*it == NULL) {
				continue;
			}
			const uint32_t count = (*it)->getInputCount();
			for (uint32_t i = 0; i < count; i++) {
				const OpNodeId &inId = (*it)->getInput(i).getId();
				const uint32_t inPos = (*it)->getInputPos(i);

				RefCountEntry &entry =
						(*refCountList_)[inId.id_.getIndex(idManager_)];
				if (inPos >= entry.size()) {
					entry.resize(inPos + 1, 0);
				}
				entry[inPos]++;
			}
		}
	}

	return *refCountList_;
}


bool SQLOps::OpStoreId::isEmpty() const {
	return id_.isEmpty();
}


SQLOps::OpCursor::OpCursor(const Source &source) :
		storeId_(source.id_),
		store_(source.store_),
		parentCxt_(source.parentCxt_),
		extCxt_(source.extCxt_),
		alloc_(*store_.getEntry(storeId_).getVarContext().getVarAllocator()),
		state_(store_.getEntry(storeId_).getCursorState()),
		plan_(store_.getEntry(storeId_).getPlan()),
		executableCode_(NULL),
		executableProfiler_(NULL),
		planPendingCount_(0),
		completedNodeFound_(false),
		executing_(false),
		compiling_(false),
		suspended_(false),
		released_(false) {
	if (plan_.isEmpty() || state_.problemOccurred_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

SQLOps::OpCursor::~OpCursor() {
	if (!released_) {
		state_.problemOccurred_ = true;
		store_.closeAllLatchHolders();
	}
	clearOp();
}

bool SQLOps::OpCursor::exists() {
	if (released_ || executing_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	if (executableOp_.get() == NULL) {
		prepareOp();
		if (executableOp_.get() == NULL) {
			return false;
		}
	}
	return true;
}

void SQLOps::OpCursor::next() {
	if (released_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	finishExec();
}

SQLOps::Operator& SQLOps::OpCursor::get() {
	if (released_ || executableOp_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	startExec();
	return *executableOp_;
}

SQLOps::OpContext& SQLOps::OpCursor::getContext() {
	if (released_ || executableCxt_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *executableCxt_;
}

bool SQLOps::OpCursor::release() {
	if (released_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	state_.executedNodeSet_.clear();

	if (suspended_ && parentCxt_ != NULL) {
		parentCxt_->setSuspended();
	}

	if (isRootOp()) {
		store_.detach();

	}

	released_ = true;
	return suspended_;
}

void SQLOps::OpCursor::clearOp() throw() {
	executableOp_.reset();

	if (executableCxt_.get() != NULL) {
		if (!state_.problemOccurred_) {
			executableCxt_->release();
		}
	}
	executableCxt_.reset();

	executableProfiler_ = NULL;

	state_.currentWorkingNode_ = OpNodeId();
}

void SQLOps::OpCursor::prepareOp() {
	if (executableOp_.get() != NULL || suspended_) {
		return;
	}

	for (;;) {
		const OpNode *node = findNextNode();
		if (node == NULL) {
			return;
		}

		const OpStoreId subStoreId = prepareSubStore(*node);
		OpStore::Entry &subEntry = store_.getEntry(subStoreId);

		const OpFactory &factory = store_.getOpFactory();
		util::AllocUniquePtr<Operator> op;
		util::AllocUniquePtr<OpContext> cxt(ALLOC_UNIQUE(
				alloc_, OpContext,
				OpContext::Source(subStoreId, store_, extCxt_)));

		const OpCode &plannedCode =
				subEntry.resolvePlannedCode(node->getCode(), storeId_);
		const OpCode *code = subEntry.findExecutableCode();

		bool initial = false;
		if (code == NULL && !checkSubPlanReady(subStoreId)) {
			op = factory.create(alloc_, plannedCode);
			if (!trySetUpDefaultSubPlan(*cxt, plannedCode)) {
				op->compile(*cxt);
			}
			if (!finishSubPlan(*node, *cxt, code)) {
				continue;
			}
			initial = (code != NULL);
		}
		op = factory.create(alloc_, (code == NULL ? plannedCode : *code));

		executableOp_.swap(op);
		executableCxt_.swap(cxt);
		executableCode_ = code;
		executableProfiler_ = subEntry.getProfiler();

		if (initial) {
			initializeOp();
		}
		break;
	}
}

void SQLOps::OpCursor::initializeOp() {
	const OpCode *code = executableCode_;
	if (code == NULL) {
		return;
	}

	OpContext &cxt = *executableCxt_;

	for (OpCode::ProjectionIterator it(code); it.exists(); it.next()) {
		it.get().initializeProjection(cxt);
	}
}

void SQLOps::OpCursor::finishOp() {
	OpContext &cxt = *executableCxt_;
	if (!cxt.isCompleted() && !cxt.isInvalidated()) {
		if (executableCode_ != NULL) {
			const Projection *proj = executableCode_->getFinishProjection();
			if (proj != NULL) {
				cxt.setUpExprContext();
				proj->project(cxt);
			}
		}

		cxt.setCompleted();
	}

	OpStore::Entry &entry = store_.getEntry(
			state_.entryMap_.find(state_.currentWorkingNode_)->second);
	entry.closeAllInputReader();
	entry.closeAllWriter(false);
	entry.closeLatchHolder();

	if (executableProfiler_ != NULL) {
		OpProfiler *profiler = store_.getProfiler();
		assert(profiler != NULL);

		profiler->addOperatorProfile(
				executableProfiler_->getId(), executableProfiler_->get());
	}

	state_.workingNodeSet_.erase(state_.currentWorkingNode_);
	completedNodeFound_ = true;
}

void SQLOps::OpCursor::finishTotalOps() {
	OpStore::Entry &topEntry = store_.getEntry(storeId_);
	topEntry.setCompleted();
	topEntry.closeLatchResource();

	if (isRootOp() && extCxt_ != NULL) {
		extCxt_->finishRootOp();
	}
	cleanUpSubStore();
}

void SQLOps::OpCursor::invalidateTotalOps() {
	if (plan_.isCompleted()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	finishOp();
	state_.workingNodeSet_.clear();
	cleanUpSubStore();

	OpStore::Entry &topEntry = store_.getEntry(storeId_);
	topEntry.setSubPlanInvalidated();
}

const SQLOps::OpNode* SQLOps::OpCursor::findNextNode() {
	OpStore::Entry &topEntry = store_.getEntry(storeId_);
	if (topEntry.isCompleted()) {
		return NULL;
	}

	OpNodeId nodeId = state_.currentWorkingNode_;
	for (;;) {
		if (!nodeId.isEmpty()) {
			break;
		}

		if (completedNodeFound_) {
			state_.executedNodeSet_.clear();
			completedNodeFound_ = false;
		}

		for (State::NodeIdSet::iterator it = state_.workingNodeSet_.begin();
				it != state_.workingNodeSet_.end(); ++it) {
			if ((state_.executedNodeSet_.find(*it) ==
					state_.executedNodeSet_.end() ||
					!checkSubPlanReady(state_.entryMap_.find(*it)->second)) &&
					checkSubStoreReady(*it)) {
				nodeId = *it;
				break;
			}
		}
		if (planPendingCount_ >= state_.workingNodeSet_.size()) {
			nodeId = OpNodeId();
		}
		if (!nodeId.isEmpty()) {
			break;
		}

		bool newNodeFound = false;
		uint32_t index = plan_.findNextNodeIndex(state_.lastPlannedNode_);
		for (;; ++index) {
			const OpNode *node = plan_.findNextNode(index);
			if (node == NULL) {
				break;
			}
			state_.lastPlannedNode_ = node->getId();
			state_.workingNodeSet_.insert(node->getId());
			newNodeFound = true;
			planPendingCount_ = 0;
		}
		if (state_.workingNodeSet_.empty()) {
			if (plan_.isCompleted()) {
				finishTotalOps();
			}
			return NULL;
		}
		else if (!newNodeFound) {
			return NULL;
		}
	}

	state_.currentWorkingNode_ = nodeId;
	return &plan_.getNode(nodeId);
}

void SQLOps::OpCursor::prepareRootStore() {
	if (!isRootOp()) {
		return;
	}

	OpStore::Entry &topEntry = store_.getEntry(storeId_);
	topEntry.activateTransfer(extCxt_);
}

SQLOps::OpStoreId SQLOps::OpCursor::prepareSubStore(const OpNode &node) {
	prepareRootStore();

	const OpNodeId &nodeId = node.getId();

	State::EntryMap::iterator entryIt = state_.entryMap_.find(nodeId);
	if (entryIt == state_.entryMap_.end()) {
		const OpStoreId subStoreId = store_.allocateEntry();
		entryIt = state_.entryMap_.insert(
				std::make_pair(nodeId, subStoreId)).first;
		setUpSubStore(node, subStoreId);
	}

	return entryIt->second;
}

bool SQLOps::OpCursor::checkSubStoreReady(const OpNodeId &nodeId) {
	if (state_.entryMap_.find(nodeId) != state_.entryMap_.end()) {
		return true;
	}

	OpStore::Entry &topEntry = store_.getEntry(storeId_);

	const OpNode &node = plan_.getNode(nodeId);
	const uint32_t inCount = node.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		const OpNode &inNode = node.getInput(i);
		const uint32_t parentIndex = inNode.getParentIndex();

		OpStore::Entry *inEntry;
		if (parentIndex != std::numeric_limits<uint32_t>::max()) {
			inEntry = &store_.getEntry(topEntry.getLink(parentIndex, true));
		}
		else {
			State::EntryMap &entryMap = state_.entryMap_;
			State::EntryMap::iterator mapIt = entryMap.find(inNode.getId());
			if (mapIt == entryMap.end()) {
				return false;
			}
			inEntry = &store_.getEntry(mapIt->second);
		}
		if (!inEntry->isPipelined() && !inEntry->isCompleted()) {
			return false;
		}
	}

	return true;
}

void SQLOps::OpCursor::setUpSubStore(
		const OpNode &node, const OpStoreId &subStoreId) {
	OpStore::Entry &topEntry = store_.getEntry(storeId_);
	OpStore::Entry &subEntry = store_.getEntry(subStoreId);

	const uint32_t inCount = node.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		const OpNode &inNode = node.getInput(i);
		const uint32_t parentIndex = inNode.getParentIndex();

		const OpStoreId *inStoreId;
		uint32_t inPos;
		if (parentIndex != std::numeric_limits<uint32_t>::max()) {
			inStoreId = &topEntry.getLink(parentIndex, true);
			inPos = topEntry.getLinkPos(parentIndex, true);
		}
		else {
			State::EntryMap &entryMap = state_.entryMap_;
			State::EntryMap::iterator mapIt = entryMap.find(inNode.getId());
			assert(mapIt != entryMap.end());
			inStoreId = &mapIt->second;
			inPos = node.getInputPos(i);
		}
		subEntry.setLink(i, true, *inStoreId, inPos);
	}

	const uint32_t outCount = plan_.getNodeOutputCount(node.getId());
	for (uint32_t i = 0; i < outCount; i++) {
		const OpNode *parentNode = plan_.findParentOutput(node.getId(), i);
		if (parentNode != NULL) {
			const uint32_t parentIndex = parentNode->getParentIndex();
			const OpStoreId *outStoreId = &topEntry.getLink(parentIndex, false);
			if (outStoreId->isEmpty()) {
				outStoreId = &storeId_;
			}
			subEntry.setLink(i, false, *outStoreId, parentIndex);
			if (parentNode->getInputCount() > 1 ||
					topEntry.findUnifiedOutput(parentIndex)) {
				subEntry.setUnifiedOutput(i, storeId_, parentIndex);
			}
			continue;
		}

		ColumnTypeList typeList(subEntry.getStackAllocator());
		SQLOps::OpCodeBuilder::resolveColumnTypeList(
				node.getCode(), typeList, &i);
		subEntry.setColumnTypeList(i, typeList, false, false);
		subEntry.createTupleList(i);

		if (plan_.isMultiReferenced(node.getId(), i)) {
			subEntry.setMultiReferenced(i);
		}
	}

	OpProfiler *profiler = store_.getProfiler();
	if (profiler != NULL) {
		OpProfilerEntry *topProfilerEntry = topEntry.getProfiler();
		const OpProfilerId *baseId =
				(topProfilerEntry == NULL ? NULL : &topProfilerEntry->getId());

		const OpProfilerId &subId =
				profiler->getSubId(baseId, node.getCode().getType());
		subEntry.activateProfiler(subId);
	}
}

void SQLOps::OpCursor::cleanUpSubStore() {
	typedef util::AllocMap<OpNodeId, OpStoreId> EntryMap;
	EntryMap &map = state_.entryMap_;

	for (EntryMap::iterator it = map.begin(); it != map.end(); ++it) {
		OpStoreId &storeId = it->second;
		if (!storeId.isEmpty()) {
			store_.getEntry(storeId).closeAllWriter(false);
		}
	}

	for (EntryMap::iterator it = map.begin(); it != map.end(); ++it) {
		OpStoreId &storeId = it->second;
		if (!storeId.isEmpty()) {
			store_.getEntry(storeId).close(true, true);
		}
	}

	for (EntryMap::iterator it = map.begin(); it != map.end(); ++it) {
		OpStoreId &storeId = it->second;
		if (!storeId.isEmpty()) {
			store_.releaseEntry(storeId);
			storeId = OpStoreId();
		}
	}
}

bool SQLOps::OpCursor::checkSubPlanReady(const OpStoreId &subStoreId) {
	OpStore::Entry &subEntry = store_.getEntry(subStoreId);
	return subEntry.getPlan().isCompleted() ||
			!subEntry.getCursorState().workingNodeSet_.empty();
}

bool SQLOps::OpCursor::trySetUpDefaultSubPlan(
		OpContext &cxt, const OpCode &code) {
	if (!SQLOps::OpCodeBuilder::isNoopAlways(code) ||
			cxt.getInputCount() > 2 || cxt.getOutputCount() != 1) {
		return false;
	}

	OpPlan &subPlan = cxt.getPlan();
	if (!subPlan.isEmpty()) {
		return false;
	}

	ColumnTypeList outTypeList(cxt.getAllocator());
	OpCodeBuilder::getColumnTypeListByColumns(
			cxt.getOutputColumnList(0), outTypeList);

	OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
	builder.setUpNoopNodes(subPlan, outTypeList);

	return true;
}

bool SQLOps::OpCursor::finishSubPlan(
		const OpNode &node, OpContext &cxt, const OpCode *&code) {
	assert(code == NULL);

	OpStore::Entry &subEntry =
			store_.getEntry(state_.entryMap_.find(node.getId())->second);
	OpPlan &subPlan = subEntry.getPlan();

	const size_t lastPendingCount = planPendingCount_;
	planPendingCount_ = 0;

	if (subPlan.isCompleted()) {
		if (subPlan.isEmpty()) {
			OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
			subEntry.setUpProjectionFactoryContext(
					builder.getProjectionFactoryContext(), NULL);
			code = &subEntry.setExecutableCode(
					builder.toExecutable(node.getCode()));
		}
		return true;
	}

	if (subPlan.getParentOutput(0).getInputCount() > 0) {
		subPlan.setCompleted();

		const uint32_t inCount = subPlan.getParentInputCount();
		for (uint32_t i = 0; i < inCount; i++) {
			if (!subPlan.isMultiReferenced(
					subPlan.getParentInput(i).getId(), 0)) {
				continue;
			}
			const OpStoreId &inStoreId = subEntry.getLink(i, true);
			const uint32_t inPos = subEntry.getLinkPos(i, true);
			store_.getEntry(inStoreId).setMultiReferenced(inPos);
		}
		return true;
	}

	if (!cxt.isPlanPending()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (subPlan.isEmpty()) {
		state_.currentWorkingNode_ = OpNodeId();
		planPendingCount_ = lastPendingCount + 1;
		return false;
	}

	{
		const uint32_t inCount = node.getInputCount();
		for (uint32_t i = 0; i < inCount; i++) {
			const OpStoreId &inStoreId = subEntry.getLink(i, true);
			const uint32_t pos = subEntry.getLinkPos(i, true);
			store_.getEntry(inStoreId).setMultiReferenced(pos);
		}
	}

	return true;
}

void SQLOps::OpCursor::startExec() {
	updateContext(true);
	executing_ = true;

	if (executableProfiler_ != NULL) {
		executableProfiler_->startExec();
	}
}

void SQLOps::OpCursor::finishExec() {
	if (executableProfiler_ != NULL) {
		executableProfiler_->finishExec();
	}

	if (!executing_ || executableCxt_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	executing_ = false;
	updateContext(false);

	if (executableCxt_->isInvalidated()) {
		invalidateTotalOps();
	}
	else if (executableCxt_->isCompleted() ||
			(!executableCxt_->isSuspended() &&
			executableCxt_->isAllInputCompleted() &&
			executableCxt_->getPlan().isCompleted())) {
		finishOp();
	}
	else if (executableCxt_->isSuspended()) {
		suspended_ = true;
	}
	else {
		state_.executedNodeSet_.insert(state_.currentWorkingNode_);
	}

	clearOp();
}

void SQLOps::OpCursor::updateContext(bool starting) {
	OpContext *cxt = executableCxt_.get();
	const OpCode *code = executableCode_;

	if (cxt != NULL && code != NULL) {
		for (OpCode::ProjectionIterator it(code); it.exists(); it.next()) {
			it.get().updateProjectionContext(*cxt);
		}

		if (starting && cxt->isExprContextAutoSetUp()) {
			cxt->setUpExprContext();
		}
	}
}

bool SQLOps::OpCursor::isRootOp() {
	return (parentCxt_ == NULL);
}


SQLOps::OpCursor::Source::Source(
		const OpStoreId &id, OpStore &store, OpContext *parentCxt) :
		id_(id),
		store_(store),
		parentCxt_(parentCxt),
		extCxt_((parentCxt == NULL ? NULL : parentCxt->findExtContext())) {
}

void SQLOps::OpCursor::Source::setExtContext(ExtOpContext *extCxt) {
	extCxt_ = extCxt;
}


SQLOps::OpCursor::State::State(const util::StdAllocator<void, void> &alloc) :
		entryMap_(alloc),
		workingNodeSet_(alloc),
		executedNodeSet_(alloc),
		problemOccurred_(false) {
}


SQLOps::OpStore::OpStore(OpAllocatorManager &allocManager) :
		alloc_(allocManager.getLocalAllocator()),
		entryList_(alloc_),
		idManager_(alloc_),
		resourceIdManager_(alloc_),
		attachedReaderList_(alloc_),
		attachedWriterList_(alloc_),
		usedLatchHolderList_(alloc_),
		tempStoreGroup_(NULL),
		varAlloc_(allocManager.getLocalVarAllocator()),
		allocBase_(NULL),
		opFactory_(&OpFactory::getDefaultFactory()),
		allocManager_(allocManager),
		latchKeeperManager_(NULL),
		lastTempStoreUsage_(0, SQLOpTypes::END_OP) {
}

SQLOps::OpStore::~OpStore() {
	close();
}

void SQLOps::OpStore::close() {
	assert(usedLatchHolderList_.empty());

	attachedReaderList_.clear();
	attachedWriterList_.clear();
	usedLatchHolderList_.clear();

	for (IdEntryList::iterator it = entryList_.begin();
			it != entryList_.end(); ++it) {
		if (it->second != NULL) {
			try {
				it->second->closeAllWriter(true);
			}
			catch (...) {
			}
		}
	}

	for (IdEntryList::iterator it = entryList_.begin();
			it != entryList_.end(); ++it) {
		if (it->second != NULL) {
			try {
				it->second->close(true, false);
			}
			catch (...) {
			}
		}
	}

	while (!entryList_.empty()) {
		util::AllocUniquePtr<Entry> entryPtr(entryList_.back().second, alloc_);
		entryList_.pop_back();
	}

}

SQLOps::OpStoreId SQLOps::OpStore::allocateEntry() {
	OpStoreId id;
	id.id_.assign(idManager_);
	const size_t index = id.id_.getIndex(idManager_);

	if (index >= entryList_.size()) {
		entryList_.resize(index + 1);
	}

	IdEntry &idEntry = entryList_[index];
	Entry *&entryRef = idEntry.second;

	util::AllocUniquePtr<Entry> entryPtr(entryRef, alloc_);
	entryRef = NULL;

	entryPtr = ALLOC_UNIQUE(alloc_, Entry, *this, id);

	idEntry.first = id;
	entryRef = entryPtr.release();

	return id;
}

void SQLOps::OpStore::releaseEntry(const OpStoreId &id) {
	const size_t index = id.id_.getIndex(idManager_);
	IdEntry &idEntry = entryList_[index];
	Entry *&entryRef = idEntry.second;
	if (idEntry.first.isEmpty() || entryRef == NULL) {
		return;
	}
	idEntry.first = OpStoreId();
	entryRef->close(false, true);

	util::AllocUniquePtr<Entry> entryPtr(entryRef, alloc_);
	entryRef = NULL;
}

inline SQLOps::OpStore::Entry& SQLOps::OpStore::getEntry(const OpStoreId &id) {
	Entry *entry = findEntry(id);
	if (entry == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *entry;
}

inline SQLOps::OpStore::Entry* SQLOps::OpStore::findEntry(const OpStoreId &id) {
	const size_t index = id.id_.getIndex(idManager_);
	IdEntry &idEntry = entryList_[index];
	if (idEntry.first.isEmpty()) {
		return NULL;
	}
	return idEntry.second;
}

void SQLOps::OpStore::detach() {
	{
		ElementRefList &list = attachedReaderList_;
		if (!list.empty()) {
			ElementRefList::iterator it = list.begin();
			do {
				Entry::detachReaders(findEntry(it->first), it->second);
			}
			while (++it != list.end());
			list.clear();
		}
	}

	{
		ElementRefList &list = attachedWriterList_;
		if (!list.empty()) {
			ElementRefList::iterator it = list.begin();
			do {
				Entry::detachWriter(findEntry(it->first), it->second);
			}
			while (++it != list.end());
			list.clear();
		}
	}

	{
		ElementRefList &list = usedLatchHolderList_;
		for (ElementRefList::iterator it = list.end(); it != list.begin();) {
			Entry *entry = findEntry((--it)->first);
			if (entry == NULL) {
				it = list.erase(it);
			}
			else {
				entry->resetLatchHolder();
			}
		}
	}
}

void SQLOps::OpStore::closeAllLatchHolders() throw() {
	ElementRefList &list = usedLatchHolderList_;
	for (ElementRefList::iterator it = list.end(); it != list.begin();) {
		try {
			Entry *entry = findEntry((--it)->first);
			if (entry == NULL) {
				it = list.erase(it);
			}
			else {
				entry->closeLatchHolder();
			}
		}
		catch (...) {
			assert(false);
		}
	}
}

void SQLOps::OpStore::closeAllLatchResources() throw() {
	ElementRefList &list = usedLatchHolderList_;
	for (ElementRefList::iterator it = list.begin(); it != list.end(); ++it) {
		try {
			Entry *entry = findEntry(it->first);
			if (entry != NULL) {
				entry->closeLatchResource();
			}
		}
		catch (...) {
			assert(false);
		}
	}
	list.clear();
}

TupleList::Group& SQLOps::OpStore::getTempStoreGroup() {
	if (tempStoreGroup_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *tempStoreGroup_;
}

void SQLOps::OpStore::setTempStoreGroup(TupleList::Group &group) {
	tempStoreGroup_ = &group;
}

SQLValues::VarAllocator& SQLOps::OpStore::getVarAllocator() {
	return varAlloc_;
}

util::StackAllocator::BaseAllocator& SQLOps::OpStore::getStackAllocatorBase() {
	if (allocBase_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *allocBase_;
}

void SQLOps::OpStore::setStackAllocatorBase(
		util::StackAllocator::BaseAllocator *allocBase) {
	allocBase_ = allocBase;
}

const SQLOps::OpFactory& SQLOps::OpStore::getOpFactory() {
	assert(opFactory_ != NULL);
	return *opFactory_;
}

const SQLOps::OpConfig& SQLOps::OpStore::getConfig() {
	return config_;
}

void SQLOps::OpStore::setConfig(const OpConfig &config) {
	config_ = config;
}

SQLOps::OpAllocatorManager& SQLOps::OpStore::getAllocatorManager() {
	return allocManager_;
}

SQLOps::OpLatchKeeperManager& SQLOps::OpStore::getLatchKeeperManager(
		ExtOpContext &cxt) {
	if (latchKeeperManager_ == NULL) {
		util::StdAllocator<void, void> &alloc =
				getAllocatorManager().getLocalAllocator();
		latchKeeperManager_ =
				&OpLatchKeeperManager::getDefault().getSubManager(cxt, alloc);
	}
	return *latchKeeperManager_;
}

SQLOps::OpProfiler* SQLOps::OpStore::getProfiler() {
	return profiler_.get();
}

void SQLOps::OpStore::activateProfiler() {
	if (profiler_.get() == NULL) {
		profiler_ = ALLOC_UNIQUE(alloc_, OpProfiler, alloc_);
	}
}

void SQLOps::OpStore::dumpTotalTempStoreUsage(
		std::ostream &out, TotalStats &stats, const OpStoreId &id,
		bool asLine) {
	if (asLine) {
		util::NormalOStringStream oss;
		dumpTotalTempStoreUsage(oss, stats, id, false);
		oss << std::endl;
		out << oss.str();
		out.flush();
		return;
	}

	TotalStatsElement last;
	const TotalStatsElement cur = profileTotalTempStoreUsage(id, last);
	const int64_t diff =
			static_cast<int64_t>(cur.first) -
			static_cast<int64_t>(last.first);

	stats.updateValue(last.second, last.first, 0);
	stats.updateValue(cur.second, 0, cur.first);

	const char8_t *unknownName = "UNKNOWN";

	out << "[TempStoreUsage] ";
	out << "" << stats.getTotalValue();
	out << " (";
	bool found = false;
	for (uint32_t i = 0; i <= SQLOpTypes::END_OP; i++) {
		const SQLOpTypes::Type opType = static_cast<SQLOpTypes::Type>(i);
		const uint64_t value = stats.getValue(opType);
		if (value == 0) {
			continue;
		}
		if (found) {
			out << ", ";
		}
		out << SQLOpUtils::AnalysisInfo::OP_TYPE_CODER(opType, unknownName);
		out << ":" << value;
		found = true;
	}
	out << "), ";
	out << SQLOpUtils::AnalysisInfo::OP_TYPE_CODER(cur.second, unknownName);
	out << ":" << cur.first;

	out << " (";
	if (diff >= 0) {
		out << "+";
	}
	out << diff << ")";
}

void SQLOps::OpStore::clearTotalTempStoreUsage(TotalStats &stats) {
	const TotalStatsElement last = lastTempStoreUsage_;
	stats.updateValue(last.second, last.first, 0);
	lastTempStoreUsage_ = TotalStatsElement(0, SQLOpTypes::END_OP);
}

SQLOps::OpStore::TotalStatsElement SQLOps::OpStore::profileTotalTempStoreUsage(
		const OpStoreId &id, TotalStatsElement &last) {
	Entry *entry = findEntry(id);
	SQLOpTypes::Type opType = SQLOpTypes::END_OP;
	if (entry != NULL) {
		opType = entry->findFirstOpType();
	}

	uint64_t usage = 0;
	for (IdEntryList::iterator it = entryList_.begin();
			it != entryList_.end(); ++it) {
		if (it->second == NULL) {
			continue;
		}
		usage += it->second->profileTempStoreUsage();
	}

	const TotalStatsElement cur = TotalStatsElement(usage, opType);
	last = lastTempStoreUsage_;
	lastTempStoreUsage_ = cur;
	return cur;
}


SQLOps::OpStore::ResourceRefData::ResourceRefData() :
		store_(NULL),
		index_(std::numeric_limits<uint32_t>::max()),
		forLatch_(false) {
}


SQLOps::OpStore::AllocatorRef::AllocatorRef(
		OpAllocatorManager &manager,
		util::StackAllocator::BaseAllocator &base) :
		alloc_(manager.create(base)),
		manager_(manager) {
}

SQLOps::OpStore::AllocatorRef::~AllocatorRef() {
	try {
		manager_.release(&alloc_);
	}
	catch (...) {
	}
}

util::StackAllocator& SQLOps::OpStore::AllocatorRef::get() {
	return alloc_;
}



uint64_t SQLOps::OpStore::TotalStats::getTotalValue() {
	return totalValue_;
}

uint64_t SQLOps::OpStore::TotalStats::getValue(SQLOpTypes::Type opType) {
	assert(opType <= SQLOpTypes::END_OP);
	return values_[opType];
}

void SQLOps::OpStore::TotalStats::updateValue(
		SQLOpTypes::Type opType, uint64_t last, uint64_t cur) {
	assert(opType <= SQLOpTypes::END_OP);

	if (last > 0) {
		assert(last <= totalValue_);
		assert(last <= values_[opType]);
		totalValue_ -= last;
		values_[opType] -= last;
	}

	if (cur > 0) {
		totalValue_ += cur;
		values_[opType] += cur;
	}
}


SQLOps::OpStore::ResourceRef::ResourceRef(const ResourceRefData &data) :
		data_(data) {
}

const util::AllocUniquePtr<void>&
SQLOps::OpStore::ResourceRef::resolve() const {
	const util::AllocUniquePtr<void> *ptr = get();
	if (ptr == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *ptr;
}

const util::AllocUniquePtr<void>* SQLOps::OpStore::ResourceRef::get() const {
	if (data_.store_ == NULL) {
		return NULL;
	}

	Entry &entry = data_.store_->getEntry(data_.id_);
	return entry.findRefResource(
			data_.index_, data_.forLatch_, data_.resourceId_);
}


SQLOps::OpStore::Entry::Entry(OpStore &store, const OpStoreId &id) :
		id_(id),
		allocRef_(store.getAllocatorManager(), store.getStackAllocatorBase()),
		alloc_(allocRef_.get()),
		store_(store),
		elemList_(alloc_),
		inCount_(0),
		outCount_(0),
		localCount_(0),
		completed_(false),
		subPlanInvalidated_(false),
		pipelined_(false),
		completedInCount_(0),
		multiStageInCount_(0),
		readerLatchDelayCount_(0),
		remainingInList_(alloc_) {
}

SQLOps::OpStore::Entry::~Entry() {
	try {
		close(false, false);
	}
	catch (...) {
	}

	for (ElementList::iterator it = elemList_.begin();
			it != elemList_.end(); ++it) {
		ALLOC_DELETE(alloc_, *it);
	}
}

void SQLOps::OpStore::Entry::close(bool cursorOnly, bool withLatchHolder) {
	closeAllInputReader();
	closeAllWriter(false);

	const uint32_t elemCount = static_cast<uint32_t>(elemList_.size());
	for (uint32_t i = 0; i < elemCount; i++) {
		EntryElement *elem = elemList_[i];
		if (elem == NULL) {
			continue;
		}

		if (!cursorOnly) {
			closeTupleList(i);
		}

		elem->resource_.reset();

		for (ProjectionResourceList &list = elem->projResourceList_;
				!list.empty();) {
			ALLOC_DELETE(alloc_, list.back().second);
			list.pop_back();
		}

		elem->referredIdList_.clear();
	}

	if (withLatchHolder) {
		closeLatchHolder();
	}
}

void SQLOps::OpStore::Entry::closeAllInputReader() {
	const uint32_t elemCount = static_cast<uint32_t>(elemList_.size());
	for (uint32_t i = 0; i < elemCount; i++) {
		EntryElement *elem = elemList_[i];
		if (elem == NULL) {
			continue;
		}

		const uint32_t readerCount =
				static_cast<uint32_t>(elem->readerList_.size());
		for (uint32_t j = 0; j < readerCount; j++) {
			closeReader(i, j);
		}
	}
}

void SQLOps::OpStore::Entry::closeAllWriter(bool force) {
	const uint32_t elemCount = static_cast<uint32_t>(elemList_.size());
	for (uint32_t i = 0; i < elemCount; i++) {
		closeWriter(i, force);
	}
}

void SQLOps::OpStore::Entry::detachReaders(Entry *entry, uint32_t index) {
	if (entry == NULL) {
		return;
	}

	const bool withLocalRef = false;
	EntryElement *elem = entry->findElement(index, withLocalRef);
	if (elem == NULL) {
		return;
	}

	entry->detachReadersDetail(*elem);
}

void SQLOps::OpStore::Entry::detachWriter(Entry *entry, uint32_t index) {
	if (entry == NULL) {
		return;
	}

	const bool withLocalRef = false;
	EntryElement *elem = entry->findElement(index, withLocalRef);
	if (elem == NULL) {
		return;
	}

	BlockHandler *blockHandler = elem->blockHandler_.get();
	if (blockHandler != NULL) {
		blockHandler->detach(entry->getStackAllocator());
	}

	entry->detachWriterDetail(*elem, index);
}

void SQLOps::OpStore::Entry::resetLatchHolder() {
	if (latchHolderIndex_.get() == NULL) {
		return;
	}

	EntryElement &elem =
			getInput(*latchHolderIndex_).first->getElement(0, false);

	SQLValues::LatchHolder holder;
	holder.reset(static_cast<SQLValues::BaseLatchTarget*>(
			elem.latchResource_.get()));
}

void SQLOps::OpStore::Entry::closeLatchHolder() {
	if (latchHolderIndex_.get() == NULL) {
		return;
	}

	EntryElement &elem =
			getInput(*latchHolderIndex_).first->getElement(0, false);

	SQLValues::LatchHolder holder;
	holder.reset(static_cast<SQLValues::BaseLatchTarget*>(
			elem.latchResource_.get()));
	holder.close();
}

util::StackAllocator& SQLOps::OpStore::Entry::getStackAllocator() {
	return alloc_;
}

SQLValues::VarContext& SQLOps::OpStore::Entry::getVarContext() {
	if (varCxt_.get()  == NULL) {
		varCxt_ = UTIL_MAKE_LOCAL_UNIQUE(varCxt_, SQLValues::VarContext);
		varCxt_->setVarAllocator(&store_.getVarAllocator());
		varCxt_->setGroup(&store_.getTempStoreGroup());
	}
	return *varCxt_;
}

SQLValues::ValueContext::Source
SQLOps::OpStore::Entry::getValueContextSource(
		SQLValues::ExtValueContext *extCxt) {
	return SQLValues::ValueContext::Source(&alloc_, &getVarContext(), extCxt);
}

bool SQLOps::OpStore::Entry::isCompleted() {
	return completed_;
}

void SQLOps::OpStore::Entry::setCompleted() {
	if (completed_) {
		return;
	}

	completed_ = true;

	for (ElementList::const_iterator elemIt = elemList_.begin();
			elemIt != elemList_.end(); ++elemIt) {
		if (static_cast<size_t>((elemIt - elemList_.begin())) >= outCount_) {
			break;
		}

		EntryElement::ReferredIdList &idList = (*elemIt)->referredIdList_;
		for (EntryElement::ReferredIdList::iterator it = idList.begin();
				it != idList.end(); ++it) {
			Entry *target = store_.findEntry(it->first);
			if (target == NULL) {
				continue;
			}
			assert(target->completedInCount_ < target->inCount_);
			target->completedInCount_++;
		}
		idList.clear();
	}
}

bool SQLOps::OpStore::Entry::isSubPlanInvalidated() {
	return subPlanInvalidated_;
}

void SQLOps::OpStore::Entry::setSubPlanInvalidated() {
	subPlanInvalidated_ = true;
}

bool SQLOps::OpStore::Entry::isPipelined() {
	return pipelined_;
}

void SQLOps::OpStore::Entry::setPipelined() {
	pipelined_ = true;
}

bool SQLOps::OpStore::Entry::isInputCompleted(uint32_t index) {
	EntryElement &elem = getElement(index, false);
	return store_.getEntry(elem.inId_).isCompleted();
}

bool SQLOps::OpStore::Entry::isAllInputCompleted() {
	assert(completedInCount_ <= inCount_);
	return completedInCount_ >= inCount_;
}

SQLOps::OpPlan& SQLOps::OpStore::Entry::getPlan() {
	if (plan_.get() == NULL) {
		plan_ = ALLOC_UNIQUE(alloc_, OpPlan, alloc_, inCount_, outCount_);
	}
	return *plan_;
}

SQLOpTypes::Type SQLOps::OpStore::Entry::findFirstOpType() {
	if (plan_.get() != NULL) {
		const OpNode *node = plan_->findNextNode(0);
		if (node != NULL) {
			return node->getCode().getType();
		}
	}
	return SQLOpTypes::END_OP;
}

SQLOps::OpCursor::State& SQLOps::OpStore::Entry::getCursorState() {
	if (cursorState_.get() == NULL) {
		cursorState_ = ALLOC_UNIQUE(alloc_, OpCursor::State, store_.alloc_);
	}
	return *cursorState_;
}

const SQLOps::OpCode&
SQLOps::OpStore::Entry::resolvePlannedCode(
		const OpCode &src, const OpStoreId &parentId) {
	if (plannedCode_.get() == NULL) {
		plannedCode_ = ALLOC_UNIQUE(alloc_, OpCode, src);
		setUpCode(*plannedCode_, parentId, true);
	}
	return *plannedCode_;
}

const SQLOps::OpCode* SQLOps::OpStore::Entry::findExecutableCode() {
	return executableCode_.get();
}

const SQLOps::OpCode& SQLOps::OpStore::Entry::setExecutableCode(
		const OpCode &code) {
	assert(executableCode_.isEmpty());
	executableCode_ = ALLOC_UNIQUE(alloc_, OpCode, code);
	setUpCode(*executableCode_, OpStoreId(), false);
	return *executableCode_;
}

void SQLOps::OpStore::Entry::setUpProjectionFactoryContext(
		ProjectionFactoryContext &cxt, const uint32_t *mappedInput) {
	{
		const uint32_t count = (mappedInput == NULL ? getInputCount() : 1);
		for (uint32_t i = 0; i < count; i++) {
			const uint32_t srcIndex = (mappedInput == NULL ? i : *mappedInput);
			EntryElement &elem = getElement(srcIndex, false);

			cxt.initializeReaderRefList(i, &elem.readerRefList_);

			InputSourceType type = elem.inputSourceType_;
			if (type == SQLExprs::ExprCode::END_INPUT) {
				type = SQLExprs::ExprCode::INPUT_READER;
			}

			cxt.getExprFactoryContext().setInputSourceType(i, type);
			cxt.getExprFactoryContext().setReadableTupleRef(
					i, &elem.readableTuple_);
			cxt.getExprFactoryContext().setSummaryTupleRef(
					i, &elem.summaryTuple_);
			cxt.getExprFactoryContext().setSummaryColumnListRef(
					i, &elem.summaryColumnList_);
		}
	}

	{
		const uint32_t count = getOutputCount();
		for (uint32_t i = 0; i < count; i++) {
			EntryElement &elem = getElement(i, false);
			cxt.initializeWriterRefList(i, &elem.writerRefList_);
		}
	}

	cxt.getExprFactoryContext().setActiveReaderRef(getActiveReaderRef());
	cxt.getExprFactoryContext().setAggregationTupleRef(
			getAggregationTupleRef());
	cxt.getExprFactoryContext().setAggregationTupleSet(
			getAggregationTupleSet());

	OpProfilerEntry *profilerEntry = getProfiler();
	if (profilerEntry != NULL) {
		SQLOps::OpProfilerOptimizationEntry &optProfiler =
				profilerEntry->getOptimizationEntry();

		cxt.setProfiler(&optProfiler);
		cxt.getExprFactoryContext().setProfile(
				OpProfilerOptimizationEntry::getExprProfile(&optProfiler));
	}
}

const SQLOps::OpStoreId& SQLOps::OpStore::Entry::getLink(
		uint32_t index, bool fromInput) {
	EntryElement &elem = getElement(index, false);
	if (fromInput) {
		return elem.inId_;
	}
	else {
		return elem.outId_;
	}
}

void SQLOps::OpStore::Entry::setLink(
		uint32_t index, bool fromInput, const OpStoreId &targetId,
		uint32_t targetPos) {
	const bool forLocal = false;
	EntryElement &elem = prepareElement(index, fromInput, forLocal);

	OpStoreId &refId = (fromInput ? elem.inId_ : elem.outId_);
	if (!refId.isEmpty()) {
		if (refId.id_ == targetId.id_) {
			return;
		}
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	refId = targetId;

	uint32_t &refPos = (fromInput ? elem.inPos_ : elem.outPos_);
	refPos = targetPos;

	Entry &target = store_.getEntry(targetId);
	EntryElement &targetElem = target.getElement(targetPos, false);

	if (fromInput) {
		if (target.isCompleted()) {
			assert(completedInCount_ < inCount_);
			completedInCount_++;
		}
		else {
			assert(targetPos < target.outCount_);
			targetElem.referredIdList_.push_back(
					EntryElement::ReferredIdEntry(id_, index));
		}

		TupleList *tupleList = targetElem.tupleList_.get();
		if (tupleList != NULL && tupleList->getBlockCount() > 0) {
			pushRemainingInput(index, NULL);
		}
	}

	const ColumnTypeList &columnTypeList = targetElem.columnTypeList_;
	setColumnTypeList(index, columnTypeList, fromInput, forLocal);
}

uint32_t SQLOps::OpStore::Entry::getLinkPos(uint32_t index, bool fromInput) {
	EntryElement &elem = getElement(index, false);
	return (fromInput ? elem.inPos_ : elem.outPos_);
}

void SQLOps::OpStore::Entry::setMultiReferenced(uint32_t index) {
	getElement(index, false).multiRef_ = true;
}

bool SQLOps::OpStore::Entry::findUnifiedOutput(uint32_t index) {
	return !getElement(index, false).unifiedOutId_.isEmpty();
}

void SQLOps::OpStore::Entry::setUnifiedOutput(
		uint32_t index, const OpStoreId &targetId, uint32_t targetPos) {
	EntryElement &elem = getElement(index, false);

	OpStoreId &refId = elem.unifiedOutId_;
	if (!refId.isEmpty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	refId = targetId;
	elem.unifiedOutPos_ = targetPos;
}

uint32_t SQLOps::OpStore::Entry::prepareLocal(uint32_t localOrdinal) {
	while (localOrdinal >= localCount_) {
		createLocal(NULL);
	}
	return outCount_ + localOrdinal;
}

uint32_t SQLOps::OpStore::Entry::createLocal(const ColumnTypeList *list) {
	const uint32_t index = outCount_ + localCount_;
	const bool fromInput = false;
	const bool forLocal = true;

	const EntryRef &target = getInput(0);
	const ColumnTypeList &columnTypeList = (list == NULL ?
			target.first->getElement(target.second, false).columnTypeList_ :
			*list);
	setColumnTypeList(index, columnTypeList, fromInput, forLocal);

	++localCount_;
	return index;
}

void SQLOps::OpStore::Entry::setLocalLink(
		uint32_t srcIndex, uint32_t destIndex) {
	EntryElement &src = getElement(srcIndex, false);
	getElement(destIndex, false);

	src.localRef_ = destIndex;

	updateAllWriterRefList();
}

uint32_t SQLOps::OpStore::Entry::getInputCount() {
	return inCount_;
}

uint32_t SQLOps::OpStore::Entry::getOutputCount() {
	return outCount_;
}

bool SQLOps::OpStore::Entry::isLocal(uint32_t index) {
	return (outCount_ <= index && index < outCount_ + localCount_);
}

void SQLOps::OpStore::Entry::checkLocal(uint32_t index) {
	if (!isLocal(index)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

void SQLOps::OpStore::Entry::setColumnTypeList(
		uint32_t index, const ColumnTypeList &list, bool fromInput,
		bool forLocal) {
	const TupleColumnType *listData = (list.empty() ? NULL : &list[0]);
	setColumnTypeList(index, listData, list.size(), fromInput, forLocal);
}

void SQLOps::OpStore::Entry::setColumnTypeList(
		uint32_t index, const TupleColumnType *list, size_t columnCount,
		bool fromInput, bool forLocal) {
	EntryElement &elem = prepareElement(index, fromInput, forLocal);
	elem.columnTypeList_.assign(list, list + columnCount);

	elem.columnList_.clear();
	if (columnCount <= 0) {
		return;
	}

	TupleList::Info info;
	SQLValues::TypeUtils::setUpTupleInfo(
			info, &elem.columnTypeList_[0], elem.columnTypeList_.size());

	elem.columnList_.resize(info.columnCount_);
	info.getColumns(&elem.columnList_[0], elem.columnList_.size());
}

const SQLOps::TupleColumnList& SQLOps::OpStore::Entry::getInputColumnList(
		uint32_t index) {
	EntryElement &elem = getElement(index, false);
	return store_.getEntry(elem.inId_).getColumnList(elem.inPos_, false);
}

const SQLOps::TupleColumnList& SQLOps::OpStore::Entry::getColumnList(
		uint32_t index, bool withLocalRef) {
	EntryElement &elem = getElement(index, withLocalRef);
	return elem.columnList_;
}

SQLExprs::ExprCode::InputSourceType
SQLOps::OpStore::Entry::getInputSourceType(uint32_t index) {
	EntryElement &elem = getElement(index, false);
	return elem.inputSourceType_;
}

void SQLOps::OpStore::Entry::setInputSourceType(
		uint32_t index, InputSourceType type) {
	if (getPlan().isCompleted()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	EntryElement &elem = getElement(index, false);
	InputSourceType &destType = elem.inputSourceType_;
	if (destType == type) {
		return;
	}

	{
		const InputSourceType multiStageType =
				SQLExprs::ExprCode::INPUT_MULTI_STAGE;
		uint32_t &count = multiStageInCount_;
		if (type == multiStageType) {
			assert(count < inCount_);
			count++;
		}
		else if (destType == multiStageType) {
			assert(count > 0);
			count--;
		}
	}

	destType = type;
}

bool SQLOps::OpStore::Entry::isExprContextAutoSetUp() {
	return (multiStageInCount_ <= 0);
}

TupleList& SQLOps::OpStore::Entry::createTupleList(uint32_t index) {
	EntryElement &elem = getElement(index, false);
	util::LocalUniquePtr<TupleList> &tupleList = elem.tupleList_;

	if (tupleList.get() != NULL || !elem.unifiedOutId_.isEmpty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	tupleList = UTIL_MAKE_LOCAL_UNIQUE(
			tupleList, TupleList, store_.getTempStoreGroup(), getTupleInfo(elem));

	return *tupleList;

}

TupleList& SQLOps::OpStore::Entry::getTupleList(
		uint32_t index, bool fromInput, bool withLocalRef) {
	if (fromInput) {
		return getInputTupleList(index);
	}

	EntryElement &elem = getElement(index, withLocalRef);
	if (!elem.outId_.isEmpty()) {
		const EntryRef &ref = getOutput(index);
		return ref.first->getTupleList(ref.second, false, false);
	}

	TupleList *tupleList = elem.tupleList_.get();
	if (tupleList == NULL) {
		if (!fromInput && !withLocalRef) {
			return createTupleList(index);
		}
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *tupleList;
}

TupleList& SQLOps::OpStore::Entry::getInputTupleList(uint32_t index) {
	const bool withLocalRef = false;

	EntryElement &elem = getElement(index, withLocalRef);
	if (elem.inTupleList_.get() != NULL) {
		return *elem.inTupleList_;
	}

	const EntryRef &inRef = getInput(index);
	Entry &inEntry = *inRef.first;
	const uint32_t inPos = inRef.second;
	TupleList *srcTupleList =
			&inEntry.getTupleList(inPos, false, withLocalRef);

	EntryElement &inElem = inEntry.getElement(inPos, withLocalRef);
	if (!inElem.multiRef_) {
		return *srcTupleList;
	}

	TupleList::Group &group = store_.getTempStoreGroup();
	const TupleList::Info &info = getTupleInfo(inElem);

	elem.inTupleList_ = UTIL_MAKE_LOCAL_UNIQUE(
			elem.inTupleList_, TupleList, group, info);

	assert(srcTupleList->getActiveTopBlockNth() == 0);
	LocalTempStore &tempStore = srcTupleList->getStore();

	const uint64_t blockCount = srcTupleList->getBlockCount();
	for (uint64_t i = 0; i < blockCount; i++) {
		elem.inTupleList_->append(LocalTempStore::Block(
				tempStore, srcTupleList->getBlockId(i)));
	}

	return *elem.inTupleList_;
}

void SQLOps::OpStore::Entry::closeTupleList(uint32_t index) {
	EntryElement *elem = findElement(index, false);
	if (elem == NULL) {
		return;
	}

	const uint32_t readerCount =
			static_cast<uint32_t>(elem->readerList_.size());
	for (uint32_t i = 0; i < readerCount; i++) {
		closeReader(index, i);
	}
	closeWriter(index, false);

	elem->tupleList_.reset();
	elem->inTupleList_.reset();
}

uint64_t SQLOps::OpStore::Entry::getInputTupleCount(uint32_t index) {
	const bool fromInput = true;
	const bool withLocalRef = false;

	assert(getElement(index, withLocalRef).readerList_.empty());

	const EntryRef &ref = getInput(index);
	EntryElement &elem = ref.first->getElement(ref.second, withLocalRef);
	if (elem.blockTupleCount_ > 0 || ref.first->pipelined_) {
		return elem.blockTupleCount_;
	}

	TupleList &tupleList = getTupleList(index, fromInput, withLocalRef);

	uint64_t count = 0;
	TupleList::BlockReader reader(tupleList);
	for (;;) {
		TupleList::Block block;
		if (!reader.next(block)) {
			break;
		}
		count += TupleList::tupleCount(block);
	}

	if (ref.first->isCompleted()) {
		elem.blockTupleCount_ = count;
	}

	return count;
}

SQLOps::TupleListReader& SQLOps::OpStore::Entry::getReader(
		uint32_t index, uint32_t sub, bool fromInput) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);

	while (sub >= elem.readerList_.size()) {
		elem.readerList_.push_back(
				ALLOC_NEW(alloc_) util::LocalUniquePtr<TupleListReader>());
	}

	const TupleListReader::AccessOrder order = (elem.readerRandomAccessing_ ?
			TupleListReader::ORDER_RANDOM : TupleListReader::ORDER_SEQUENTIAL);

	util::LocalUniquePtr<TupleListReader> &reader = *elem.readerList_[sub];
	if (reader.get() == NULL) {
		TupleList &tupleList = getTupleList(index, fromInput, withLocalRef);

		while (sub >= elem.readerImageList_.size()) {
			elem.readerImageList_.push_back(
					ALLOC_NEW(alloc_) util::LocalUniquePtr<
							TupleListReader::Image>());
		}

		util::LocalUniquePtr<TupleListReader::Image> &readerImage =
				*elem.readerImageList_[sub];
		if (readerImage.get() == NULL) {
			reader = UTIL_MAKE_LOCAL_UNIQUE(
					reader, TupleListReader, tupleList, order);
		}
		else {
			reader = UTIL_MAKE_LOCAL_UNIQUE(
					reader, TupleListReader, tupleList, *readerImage);
			readerImage.reset();
		}

		reader->setVarContext(getVarContext());

		setReaderAttached(elem, index, withLocalRef);
		updateReaderRefListDetail(elem);
	}

	assert(reader->getAccessOrder() == order);

	return *reader;
}

void SQLOps::OpStore::Entry::closeReader(uint32_t index, uint32_t sub) {
	EntryElement *elem = findElement(index, false);
	if (elem == NULL || sub >= elem->readerList_.size()) {
		return;
	}

	elem->readerImageList_[sub]->reset();
	elem->readerList_[sub]->reset();
	elem->inputConsumedBlockCount_ = 0;

	updateReaderRefListDetail(*elem);
}

SQLOps::TupleListWriter& SQLOps::OpStore::Entry::getWriter(
		uint32_t index, bool withLocalRef) {
	EntryElement &elem = getElement(index, withLocalRef);
	if (!elem.unifiedOutId_.isEmpty()) {
		const EntryRef &ref = getUnifiedOutputElement(elem, index);
		SQLOps::TupleListWriter &writer =
				ref.first->getWriter(ref.second, withLocalRef);
		updateWriterRefListDetail(getElement(index, false), index);
		return writer;
	}

	util::LocalUniquePtr<TupleListWriter> &writer = elem.writer_;
	if (writer.get() == NULL) {
		TupleList &tupleList = getTupleList(index, false, withLocalRef);

		if (elem.writerImage_.get() == NULL) {
			writer = UTIL_MAKE_LOCAL_UNIQUE(
					writer, TupleListWriter, tupleList);
		}
		else {
			writer = UTIL_MAKE_LOCAL_UNIQUE(
					writer, TupleListWriter, tupleList, *elem.writerImage_);
			elem.writerImage_.reset();
		}
		writer->setVarContext(getVarContext());

		EntryElement &handlerElem = (elem.outId_.isEmpty() ?
				elem : getOutputElement(index, false));

		BlockHandler *blockHandler = handlerElem.blockHandler_.get();
		if (blockHandler != NULL) {
			blockHandler->bindWriter(*writer);

			const EntryRef &ref = (elem.outId_.isEmpty() ?
					EntryRef(this, index) : getOutput(index));
			ref.first->setWriterAttached(handlerElem, ref.second, false);
		}

		setWriterAttached(elem, index, withLocalRef);
		updateWriterRefListDetail(getElement(index, false), index);
	}
	return *writer;
}

void SQLOps::OpStore::Entry::closeWriter(uint32_t index, bool force) {
	EntryElement *elem = findElement(index, false);
	if (elem == NULL) {
		return;
	}

	if (!force && elem->writerImage_.get() != NULL) {
		getWriter(index, false);
	}

	util::LocalUniquePtr<TupleListWriter> &writer = elem->writer_;
	if (writer.get() != NULL) {
		if (force) {
			BlockHandler::unbindWriter(*elem->writer_);
		}

		elem->writer_->close();

		EntryElement &handlerElem = (elem->outId_.isEmpty() ?
				*elem : getOutputElement(index, false));

		BlockHandler *blockHandler = handlerElem.blockHandler_.get();
		if (blockHandler != NULL) {
			blockHandler->close();
		}
	}

	elem->blockHandler_.reset();
	elem->writerImage_.reset();
	elem->writer_.reset();

	updateWriterRefListDetail(*elem, index);
}

int64_t SQLOps::OpStore::Entry::getOutputTupleCount(
		uint32_t index, bool withLocalRef) {
	EntryElement &elem = getElement(index, withLocalRef);
	if (!elem.unifiedOutId_.isEmpty()) {
		const EntryRef &ref = getUnifiedOutputElement(elem, index);
		return ref.first->getOutputTupleCount(ref.second, withLocalRef);
	}
	return elem.outputTupleCount_;
}

void SQLOps::OpStore::Entry::addOutputTupleCount(
		uint32_t index, bool withLocalRef, int64_t count) {
	EntryElement &elem = getElement(index, withLocalRef);
	if (!elem.unifiedOutId_.isEmpty()) {
		const EntryRef &ref = getUnifiedOutputElement(elem, index);
		ref.first->addOutputTupleCount(ref.second, withLocalRef, count);
		return;
	}

	assert(count >= 0);
	elem.outputTupleCount_ += count;
}

void SQLOps::OpStore::Entry::updateAllWriterRefList() {
	for (uint32_t i = 0; i < outCount_; i++) {
		updateWriterRefList(i);
	}
}

void SQLOps::OpStore::Entry::updateReaderRefList(uint32_t index) {
	const bool withLocalRef = false;
	updateReaderRefListDetail(getElement(index, withLocalRef));
}

void SQLOps::OpStore::Entry::updateWriterRefList(uint32_t index) {
	const bool withLocalRef = false;
	updateWriterRefListDetail(getElement(index, withLocalRef), index);
}

SQLOps::TupleListReader**
SQLOps::OpStore::Entry::getActiveReaderRef() {
	EntryElement *elem = findElement(0, false);
	if (elem == NULL) {
		return NULL;
	}

	return &elem->activeReaderRef_;
}

SQLOps::ReadableTuple*
SQLOps::OpStore::Entry::getReadableTupleRef(uint32_t index) {
	EntryElement *elem = findElement(index, false);
	if (elem == NULL) {
		return NULL;
	}

	return &elem->readableTuple_;
}

SQLOps::SummaryTuple*
SQLOps::OpStore::Entry::getSummaryTupleRef(uint32_t index) {
	EntryElement *elem = findElement(index, false);
	if (elem == NULL) {
		return NULL;
	}

	return &elem->summaryTuple_;
}

SQLOps::SummaryColumnList*
SQLOps::OpStore::Entry::getSummaryColumnListRef(uint32_t index) {
	EntryElement *elem = findElement(index, false);
	if (elem == NULL) {
		return NULL;
	}

	return &elem->summaryColumnList_;
}

bool SQLOps::OpStore::Entry::setUpAggregationTuple() {
	EntryElement &elem = getElement(0, false);
	util::LocalUniquePtr<SummaryTuple> &tuple = elem.defaultAggrTuple_;

	if (tuple.get() == NULL) {
		SummaryTupleSet *tupleSet = elem.aggrTupleSet_.get();

		if (tupleSet == NULL || !tupleSet->isColumnsCompleted()) {
			return false;
		}

		tuple = UTIL_MAKE_LOCAL_UNIQUE(
				tuple, SummaryTuple, SummaryTuple::create(*tupleSet));
	}
	elem.aggrTuple_ = *tuple;
	return true;
}

SQLOps::SummaryTuple* SQLOps::OpStore::Entry::getAggregationTupleRef() {
	EntryElement &elem = getElement(0, false);
	return &elem.aggrTuple_;
}

SQLOps::SummaryTupleSet* SQLOps::OpStore::Entry::getAggregationTupleSet() {
	EntryElement &elem = getElement(0, false);
	util::AllocUniquePtr<SummaryTupleSet> &tupleSet = elem.aggrTupleSet_;

	if (tupleSet.get() == NULL) {
		SQLValues::ValueContext cxt(getValueContextSource(NULL));
		tupleSet = ALLOC_UNIQUE(alloc_, SummaryTupleSet, cxt, NULL);
	}

	return tupleSet.get();
}

void SQLOps::OpStore::Entry::activateTransfer(ExtOpContext *cxt) {
	if (cxt == NULL) {
		return;
	}

	const uint32_t count = getOutputCount();
	for (uint32_t i = 0; i < count; i++) {
		getBlockHandler(i).activate(*cxt);
	}
}

SQLOps::OpStore::BlockHandler& SQLOps::OpStore::Entry::getBlockHandler(
		uint32_t index) {
	const bool withLocalRef = false;
	const EntryRef &ref = getOutput(index);
	Entry &entry = *ref.first;
	EntryElement &elem = entry.getElement(ref.second, withLocalRef);
	if (elem.blockHandler_.get() == NULL) {
		elem.blockHandler_ = ALLOC_UNIQUE(entry.alloc_, BlockHandler, index);
	}
	return *elem.blockHandler_;
}

void SQLOps::OpStore::Entry::appendBlock(
		uint32_t index, const TupleList::Block &block) {
	assert(pipelined_);
	getTupleList(index, false, false).append(block);

	EntryElement &elem = getElement(index, false);
	elem.blockTupleCount_ += TupleList::tupleCount(block);

	EntryElement::ReferredIdList &idList = elem.referredIdList_;
	for (EntryElement::ReferredIdList::iterator it = idList.begin();
			it != idList.end(); ++it) {
		Entry *target = store_.findEntry(it->first);
		if (target != NULL) {
			target->pushRemainingInput(it->second, &block);
		}
	}
}

bool SQLOps::OpStore::Entry::findRemainingInput(uint32_t *index) {
	if (remainingInList_.empty()) {
		*index = std::numeric_limits<uint32_t>::max();
		return false;
	}

	*index = remainingInList_.back();
	return true;
}

void SQLOps::OpStore::Entry::pushRemainingInput(
		uint32_t index, const TupleList::Block *block) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);

	if (block != NULL && elem.inTupleList_.get() != NULL) {
		elem.inTupleList_->append(*block);
	}

	if (elem.inputRemaining_) {
		return;
	}

	remainingInList_.push_back(index);
	elem.inputRemaining_ = true;
}

void SQLOps::OpStore::Entry::popRemainingInput() {
	if (remainingInList_.empty()) {
		return;
	}

	const uint32_t index = remainingInList_.back();
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);

	remainingInList_.pop_back();
	elem.inputRemaining_ = false;
}

void SQLOps::OpStore::Entry::setReaderRandomAccess(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	elem.readerRandomAccessing_ = true;
}

void SQLOps::OpStore::Entry::keepReaderLatch(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	elem.readerLatchKeeping_ = true;
}

void SQLOps::OpStore::Entry::releaseReaderLatch(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);

	if (elem.readerRandomAccessing_) {
		EntryElement::ReaderList &readerList = elem.readerList_;
		for (EntryElement::ReaderList::iterator it = readerList.begin();
				it != readerList.end(); ++it) {
			util::LocalUniquePtr<TupleListReader> *reader = *it;
			if (reader == NULL || reader->get() == NULL) {
				continue;
			}
			(**reader).discard();
		}
	}

	elem.readerLatchKeeping_ = false;
	detachReadersDetail(elem);
}

void SQLOps::OpStore::Entry::releaseWriterLatch(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	detachWriterDetail(elem, index);
}

bool SQLOps::OpStore::Entry::isAllReaderLatchDelayed() {
	assert(readerLatchDelayCount_ <= inCount_);
	return (readerLatchDelayCount_ >= inCount_);
}

bool SQLOps::OpStore::Entry::isReaderLatchDelayed(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	return elem.readerLatchDelayed_;
}

void SQLOps::OpStore::Entry::setReaderLatchDelayed(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	bool &enabled = elem.readerLatchDelayed_;
	if (enabled) {
		return;
	}

	assert(readerLatchDelayCount_ < inCount_);
	readerLatchDelayCount_++;
	enabled = true;
}

SQLOps::OpStore::ResourceRef SQLOps::OpStore::Entry::getResourceRef(
		uint32_t index, bool forLatch) {
	ResourceRefData data;
	data.store_ = &store_;
	data.id_ = id_;
	if (forLatch) {
		EntryElement &elem = getInput(index).first->getElement(0, false);
		data.resourceId_ = elem.latchResourceId_;
		data.index_ = index;
	}
	else {
		data.resourceId_ = SQLValues::SharedId();
		data.index_ = index;
	}
	data.forLatch_ = forLatch;
	return ResourceRef(data);
}

const util::AllocUniquePtr<void>* SQLOps::OpStore::Entry::findRefResource(
		uint32_t index, bool forLatch, const SQLValues::SharedId &resourceId) {
	const util::AllocUniquePtr<void> *ptr;

	if (forLatch) {
		EntryElement &elem = getInput(index).first->getElement(0, false);
		if (!(elem.latchResourceId_ == resourceId)) {
			return NULL;
		}
		ptr = &elem.latchResource_;
	}
	else {
		EntryElement &elem = getElement(index, false);
		ptr = &elem.resource_;
	}

	if (ptr->get() == NULL) {
		return NULL;
	}

	return ptr;
}

util::AllocUniquePtr<void>& SQLOps::OpStore::Entry::getResource(
		uint32_t index, SQLOpTypes::ProjectionType projType) {
	EntryElement &elem = getElement(index, false);

	if (projType == SQLOpTypes::END_PROJ) {
		return elem.resource_;
	}
	else {
		for (ProjectionResourceList::iterator it =
				elem.projResourceList_.begin();
				it != elem.projResourceList_.end(); ++it) {
			if (it->first == projType) {
				return *it->second;
			}
		}

		elem.projResourceList_.push_back(std::make_pair(
				projType, ALLOC_NEW(alloc_) util::AllocUniquePtr<void>()));
		return *elem.projResourceList_.back().second;
	}
}

void SQLOps::OpStore::Entry::setLatchResource(
		uint32_t index, util::AllocUniquePtr<void> &resource,
		SQLValues::BaseLatchTarget &latchTarget) {
	if (resource.get() != &latchTarget || latchHolderIndex_.get() != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	EntryElement &elem = getInput(index).first->getElement(0, false);

	store_.usedLatchHolderList_.push_back(ElementRef(id_, index));

	latchHolderIndex_ = UTIL_MAKE_LOCAL_UNIQUE(
			latchHolderIndex_, uint32_t, index);
	elem.latchResource_.reset();

	if (resource.get() != NULL) {
		elem.latchResource_.swap(resource);
		elem.latchResourceId_.assign(store_.resourceIdManager_);
	}
}

void SQLOps::OpStore::Entry::closeLatchResource() {
	if (latchHolderIndex_.get() == NULL) {
		return;
	}

	EntryElement &elem =
			getInput(*latchHolderIndex_).first->getElement(0, false);

	elem.latchResource_.reset();
	latchHolderIndex_.reset();
}

int64_t SQLOps::OpStore::Entry::getLastTupleId(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	return elem.lastTupleId_;
}

void SQLOps::OpStore::Entry::setLastTupleId(uint32_t index, int64_t id) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	elem.lastTupleId_ = id;
}

SQLOps::OpAllocatorManager& SQLOps::OpStore::Entry::getAllocatorManager() {
	return store_.getAllocatorManager();
}

bool SQLOps::OpStore::Entry::tryLatch(
		ExtOpContext &cxt, uint64_t maxSize, bool hot) {
	if (latchKeeper_.get() == NULL) {
		OpLatchKeeperManager &manager = store_.getLatchKeeperManager(cxt);
		latchKeeper_ =
				ALLOC_UNIQUE(alloc_, OpLatchKeeper, manager, maxSize, hot);
	}
	return latchKeeper_->tryAcquire();
}

void SQLOps::OpStore::Entry::adjustLatch(uint64_t size) {
	if (latchKeeper_.get() == NULL) {
		assert(false);
		return;
	}
	latchKeeper_->adjust(size);
}

SQLOps::OpProfilerEntry* SQLOps::OpStore::Entry::getProfiler() {
	return profiler_.get();
}

void SQLOps::OpStore::Entry::activateProfiler(const OpProfilerId &id) {
	if (profiler_.get() != NULL) {
		return;
	}

	profiler_ =
			ALLOC_UNIQUE(store_.alloc_, OpProfilerEntry, store_.alloc_, id);
}

uint64_t SQLOps::OpStore::Entry::profileTempStoreUsage() {
	uint64_t totalCount = 0;
	for (ElementList::iterator elemIt = elemList_.begin();
			elemIt != elemList_.end(); ++elemIt) {
		EntryElement *elem = *elemIt;
		if (elem == NULL) {
			continue;
		}
		for (uint32_t i = 0; i < 2; i++) {
			TupleList *tupleList =
					(i == 0 ? elem->tupleList_ : elem->inTupleList_).get();
			if (tupleList == NULL) {
				continue;
			}
			const uint64_t count = tupleList->getBlockCount();
			for (uint64_t j = 0; j < count; j++) {
				if (tupleList->getBlockId(j) != LocalTempStore::UNDEF_BLOCKID) {
					totalCount++;
				}
			}
		}
	}
	return totalCount;
}

TupleList::Info SQLOps::OpStore::Entry::getTupleInfo(
		const EntryElement &elem) {
	assert(!elem.columnTypeList_.empty());
	TupleList::Info info;
	SQLValues::TypeUtils::setUpTupleInfo(
			info, &elem.columnTypeList_[0], elem.columnTypeList_.size());
	return info;
}

void SQLOps::OpStore::Entry::setUpCode(
		OpCode &code, const OpStoreId &parentId, bool planning) {
	static_cast<void>(planning);

	const OpConfig &baseConfig = store_.getConfig();
	const OpConfig *srcConfig = code.getConfig();

	const int64_t limitInheritance =
			OpConfig::resolve(SQLOpTypes::CONF_LIMIT_INHERITANCE, srcConfig);

	if ((srcConfig == NULL || baseConfig.isSame(*srcConfig)) &&
			limitInheritance < 0) {
		code.setConfig(&baseConfig);
	}
	else {
		OpConfig *destConfig = ALLOC_NEW(alloc_) OpConfig(
				srcConfig->resolveAll(&baseConfig));
		if (limitInheritance >= 0) {
			assert(planning);
			destConfig->set(SQLOpTypes::CONF_LIMIT_INHERITANCE, -1);
			code.setLimit(inheritLimit(code, parentId, 0));
		}
		code.setConfig(destConfig);
	}
}

int64_t SQLOps::OpStore::Entry::inheritLimit(
		const OpCode &code, const OpStoreId &parentId, uint32_t index) {
	const bool withLocalRef = true;
	const int64_t count =
			store_.getEntry(parentId).getOutputTupleCount(index, withLocalRef);
	return std::max(code.getLimit(), count) - count;
}

void SQLOps::OpStore::Entry::setReaderAttached(
		EntryElement &elem, uint32_t index, bool withLocalRef) {
	if (elem.readerAttached_ || elem.readerLatchKeeping_) {
		return;
	}
	store_.attachedReaderList_.push_back(
			ElementRef(id_, getElementIndex(index, withLocalRef)));
	elem.readerAttached_ = true;
}

void SQLOps::OpStore::Entry::setWriterAttached(
		EntryElement &elem, uint32_t index, bool withLocalRef) {
	if (elem.writerAttached_) {
		return;
	}
	store_.attachedWriterList_.push_back(
			ElementRef(id_, getElementIndex(index, withLocalRef)));
	elem.writerAttached_ = true;
}

void SQLOps::OpStore::Entry::detachReadersDetail(EntryElement &elem) {
	elem.readerAttached_ = false;

	if (elem.readerLatchKeeping_) {
		return;
	}

	EntryElement::ReaderList::iterator readerIt = elem.readerList_.begin();
	EntryElement::ReaderImageList::iterator imageIt =
			elem.readerImageList_.begin();

	for (; readerIt != elem.readerList_.end(); ++readerIt, ++imageIt) {
		if (imageIt == elem.readerImageList_.end() ||
				*readerIt == NULL || *imageIt == NULL ||
				(*readerIt)->get() == NULL) {
			continue;
		}

		util::LocalUniquePtr<TupleListReader> &reader = **readerIt;
		util::LocalUniquePtr<TupleListReader::Image> &image = **imageIt;

		image = UTIL_MAKE_LOCAL_UNIQUE(
				image, TupleListReader::Image, getStackAllocator());
		reader->detach(*image);
		reader.reset();
	}

	updateReaderRefListDetail(elem);
}

void SQLOps::OpStore::Entry::detachWriterDetail(
		EntryElement &elem, uint32_t index) {
	elem.writerAttached_ = false;

	util::LocalUniquePtr<TupleListWriter> &writer = elem.writer_;
	if (writer.get() == NULL) {
		return;
	}

	util::LocalUniquePtr<TupleListWriter::Image> &image = elem.writerImage_;
	image = UTIL_MAKE_LOCAL_UNIQUE(
			image, TupleListWriter::Image, getStackAllocator());
	writer->detach(*image);
	writer.reset();

	updateWriterRefListDetail(elem, index);
}

void SQLOps::OpStore::Entry::updateReaderRefListDetail(EntryElement &elem) {
	EntryElement::ReaderList &readerList = elem.readerList_;
	TupleListReader *reader =
			(readerList.empty() || readerList.front() == NULL ?
					NULL : readerList.front()->get());

	for (ReaderRefList::iterator it = elem.readerRefList_.begin();
			it != elem.readerRefList_.end(); ++it) {
		**it = reader;
	}
}

void SQLOps::OpStore::Entry::updateWriterRefListDetail(
		EntryElement &elem, uint32_t index) {
	const bool withLocalRef = true;
	TupleListWriter *writer = findWriter(index, withLocalRef);

	for (WriterRefList::iterator it = elem.writerRefList_.begin();
			it != elem.writerRefList_.end(); ++it) {
		**it = writer;
	}
}

SQLOps::TupleListWriter* SQLOps::OpStore::Entry::findWriter(
		uint32_t index, bool withLocalRef) {
	EntryElement &elem = getElement(index, withLocalRef);
	if (!elem.unifiedOutId_.isEmpty()) {
		const EntryRef &ref = findUnifiedOutputElement(elem, index);
		if (ref.first == NULL) {
			return NULL;
		}
		return ref.first->findWriter(ref.second, withLocalRef);
	}

	return elem.writer_.get();
}

SQLOps::OpStore::EntryElement& SQLOps::OpStore::Entry::getOutputElement(
		uint32_t index, bool withLocalRef) {
	const EntryRef &ref = getOutput(index);
	return ref.first->getElement(ref.second, withLocalRef);
}

SQLOps::OpStore::Entry::EntryRef
SQLOps::OpStore::Entry::getUnifiedOutputElement(
		EntryElement &elem, uint32_t index) {
	const EntryRef &ref = findUnifiedOutputElement(elem, index);
	if (ref.first == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return ref;
}

SQLOps::OpStore::Entry::EntryRef
SQLOps::OpStore::Entry::findUnifiedOutputElement(
		EntryElement &elem, uint32_t index) {
	if (!elem.unifiedOutId_.isEmpty()) {
		const bool withLocalRef = false;
		assert(!isLocal(index));
		Entry *entry = store_.findEntry(elem.unifiedOutId_);
		if (entry == NULL) {
			return EntryRef();
		}
		return entry->findUnifiedOutputElement(
				entry->getElement(elem.unifiedOutPos_, withLocalRef),
				elem.unifiedOutPos_);
	}
	return EntryRef(this, index);
}

SQLOps::OpStore::EntryElement& SQLOps::OpStore::Entry::prepareElement(
		uint32_t index, bool fromInput, bool forLocal) {
	if (!forLocal && plan_.get() != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (index >= elemList_.size()) {
		elemList_.resize(index + 1);
	}

	EntryElement *&elem = elemList_[index];
	if (elem == NULL) {
		elem = ALLOC_NEW(alloc_) EntryElement(alloc_);
	}

	if (!forLocal) {
		uint32_t &inOutCount = (fromInput ? inCount_ : outCount_);
		inOutCount = std::max<uint32_t>(index + 1, inOutCount);
	}

	return *elem;
}

inline SQLOps::OpStore::EntryElement& SQLOps::OpStore::Entry::getElement(
		uint32_t index, bool withLocalRef) {
	EntryElement *elem = findElement(index, withLocalRef);
	if (elem == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *elem;
}

inline SQLOps::OpStore::EntryElement* SQLOps::OpStore::Entry::findElement(
		uint32_t index, bool withLocalRef) {
	if (index >= elemList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	EntryElement *elem = elemList_[index];

	if (withLocalRef && isLocalRefAssigned(elem)) {
		return findElement(elem->localRef_, false);
	}

	return elem;
}

inline uint32_t SQLOps::OpStore::Entry::getElementIndex(
		uint32_t index, bool withLocalRef) {
	if (withLocalRef) {
		EntryElement *elem = elemList_[index];
		if (isLocalRefAssigned(elem)) {
			return elem->localRef_;
		}
	}
	return index;
}

inline bool SQLOps::OpStore::Entry::isLocalRefAssigned(
		const EntryElement *elem) {
	return (elem != NULL &&
			elem->localRef_ != std::numeric_limits<uint32_t>::max());
}

SQLOps::OpStore::Entry::EntryRef SQLOps::OpStore::Entry::getInput(
		uint32_t index) {
	EntryElement &elem = getElement(index, false);
	if (elem.inId_.isEmpty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return EntryRef(&store_.getEntry(elem.inId_), elem.inPos_);
}

SQLOps::OpStore::Entry::EntryRef SQLOps::OpStore::Entry::getOutput(
		uint32_t index) {
	EntryElement *elem = &getElement(index, false);
	if (elem->outId_.isEmpty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	for (;;) {
		Entry &entry = store_.getEntry(elem->outId_);
		EntryElement &nextElem = entry.getElement(elem->outPos_, false);
		if (nextElem.outId_.isEmpty()) {
			return EntryRef(&entry, elem->outPos_);
		}
		elem = &nextElem;
	}
}


SQLOps::OpStore::EntryElement::EntryElement(util::StackAllocator &alloc) :
		inPos_(std::numeric_limits<uint32_t>::max()),
		outPos_(std::numeric_limits<uint32_t>::max()),
		localRef_(std::numeric_limits<uint32_t>::max()),
		unifiedOutPos_(std::numeric_limits<uint32_t>::max()),
		multiRef_(false),
		readerList_(alloc),
		activeReaderRef_(NULL),
		readableTuple_(util::FalseType()),
		summaryColumnList_(alloc),
		columnTypeList_(alloc),
		columnList_(alloc),
		projResourceList_(alloc),
		lastTupleId_(0),
		readerImageList_(alloc),
		inputConsumedBlockCount_(0),
		blockTupleCount_(0),
		outputTupleCount_(0),
		readerRandomAccessing_(false),
		readerLatchKeeping_(false),
		readerLatchDelayed_(false),
		inputRemaining_(false),
		readerAttached_(false),
		writerAttached_(false),
		readerRefList_(alloc),
		writerRefList_(alloc),
		inputSourceType_(SQLExprs::ExprCode::END_INPUT),
		referredIdList_(alloc) {
}


SQLOps::OpStore::BlockHandler SQLOps::OpStore::BlockHandler::emptyHandler_(
		std::numeric_limits<uint32_t>::max());

SQLOps::OpStore::BlockHandler::BlockHandler(uint32_t id) :
		id_(id),
		extCxt_(NULL),
		closed_(false) {
}

void SQLOps::OpStore::BlockHandler::activate(ExtOpContext &extCxt) {
	if (closed_) {
		return;
	}
	extCxt_ = &extCxt;
}

void SQLOps::OpStore::BlockHandler::close() {
	closed_ = true;
	extCxt_ = NULL;
	readerImage_.reset();
	reader_.reset();
}

void SQLOps::OpStore::BlockHandler::detach(util::StackAllocator &alloc) {
	if (reader_.get() == NULL) {
		return;
	}

	readerImage_ = UTIL_MAKE_LOCAL_UNIQUE(
			readerImage_, TupleList::BlockReader::Image, alloc);
	reader_->detach(*readerImage_);
	reader_.reset();
}

void SQLOps::OpStore::BlockHandler::bindWriter(TupleListWriter &writer) {
	if (closed_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (readerImage_.get() == NULL) {
		reader_ = UTIL_MAKE_LOCAL_UNIQUE(
				reader_, TupleList::BlockReader, writer.getTupleList());
		reader_->setOnce();
	}
	else {
		assert(reader_.get() == NULL);
		reader_ = UTIL_MAKE_LOCAL_UNIQUE(
				reader_, TupleList::BlockReader, writer.getTupleList(),
				*readerImage_);
		readerImage_.reset();
	}
	writer.setBlockHandler(*this);
}

void SQLOps::OpStore::BlockHandler::operator()() {
	if (closed_ || extCxt_ == NULL || reader_.get() == NULL) {
		if (this == &emptyHandler_) {
			return;
		}
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	TupleList::Block block;
	while (reader_->next(block)) {
		extCxt_->transfer(block, id_);
	}
}

void SQLOps::OpStore::BlockHandler::unbindWriter(TupleListWriter &writer) {
	writer.setBlockHandler(emptyHandler_);
}


SQLOps::OpContext::OpContext(const Source &source) :
		storeId_(source.id_),
		store_(source.store_),
		storeEntry_(store_.getEntry(storeId_)),
		exprCxt_(storeEntry_.getValueContextSource(source.extCxt_)),
		extCxt_(source.extCxt_),
		interruptionCheckRemaining_(getInitialInterruptionCheckCount(store_)),
		invalidated_(false),
		suspended_(false),
		planPending_(false),
		exprCxtAvailable_(false) {
}

void SQLOps::OpContext::release() {
	if (exprCxtAvailable_ && !invalidated_) {
		saveTupleId();
	}
}

SQLOps::ExtOpContext& SQLOps::OpContext::getExtContext() {
	if (extCxt_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *extCxt_;
}

SQLOps::ExtOpContext* SQLOps::OpContext::findExtContext() {
	return extCxt_;
}

util::StackAllocator& SQLOps::OpContext::getAllocator() {
	return storeEntry_.getStackAllocator();
}

bool SQLOps::OpContext::isExprContextAutoSetUp() {
	return storeEntry_.isExprContextAutoSetUp();
}

void SQLOps::OpContext::setUpExprContext() {
	if (!storeEntry_.isAllReaderLatchDelayed()) {
		const uint32_t inCount = storeEntry_.getInputCount();
		for (uint32_t i = 0; i < inCount; i++) {
			if (!storeEntry_.isReaderLatchDelayed(i)) {
				setUpExprInputContext(i);
			}
		}
	}

	const uint32_t outCount = storeEntry_.getOutputCount();
	for (uint32_t i = 0; i < outCount; i++) {
		getWriter(i);
		storeEntry_.updateWriterRefList(i);
	}

	exprCxt_.setActiveReaderRef(storeEntry_.getActiveReaderRef());
	*exprCxt_.getActiveReaderRef() = NULL;

	if (storeEntry_.setUpAggregationTuple()) {
		exprCxt_.setAggregationTupleRef(
				storeEntry_.getAggregationTupleSet(),
				storeEntry_.getAggregationTupleRef());
	}

	loadTupleId();
	exprCxtAvailable_ = true;
}

void SQLOps::OpContext::setUpExprInputContext(uint32_t index) {
	exprCxt_.setReader(
			index, &getReader(index), &storeEntry_.getInputColumnList(index));

	exprCxt_.setReadableTupleRef(
			index, storeEntry_.getReadableTupleRef(index));
	exprCxt_.setReadableTuple(index, ReadableTuple(util::FalseType()));

	exprCxt_.setSummaryTupleRef(index, storeEntry_.getSummaryTupleRef(index));
	exprCxt_.setSummaryTuple(index, SummaryTuple());

	exprCxt_.setSummaryColumnListRef(
			index, storeEntry_.getSummaryColumnListRef(index));
	exprCxt_.setInputSourceType(
			index, storeEntry_.getInputSourceType(index));

	storeEntry_.updateReaderRefList(index);
}

bool SQLOps::OpContext::isCompleted() {
	return storeEntry_.isCompleted();
}

void SQLOps::OpContext::setCompleted() {
	storeEntry_.setCompleted();
	interruptionCheckRemaining_ = 1;
}

bool SQLOps::OpContext::isInputCompleted(uint32_t index) {
	return storeEntry_.isInputCompleted(index);
}

bool SQLOps::OpContext::isAllInputCompleted() {
	return storeEntry_.isAllInputCompleted();
}

void SQLOps::OpContext::invalidate() {
	invalidated_ = true;
}

bool SQLOps::OpContext::isInvalidated() {
	return invalidated_;
}

bool SQLOps::OpContext::isSubPlanInvalidated() {
	return storeEntry_.isSubPlanInvalidated();
}

bool SQLOps::OpContext::checkSuspendedAlways(bool suspendable) {
	interruptionCheckRemaining_ = 1;
	if (!suspendable) {
		if (extCxt_ != NULL) {
			extCxt_->checkCancelRequest();
		}
		return false;
	}
	return checkSuspendedDetail();
}

void SQLOps::OpContext::setSuspended() {
	interruptionCheckRemaining_ = 1;
	suspended_ = true;
}

bool SQLOps::OpContext::isSuspended() {
	return suspended_;
}

uint64_t SQLOps::OpContext::getNextCountForSuspend() {
	return interruptionCheckRemaining_;
}

void SQLOps::OpContext::setNextCountForSuspend(uint64_t count) {
	interruptionCheckRemaining_ = std::max<uint64_t>(count, 1);
}

void SQLOps::OpContext::resetNextCountForSuspend() {
	interruptionCheckRemaining_ = getInitialInterruptionCheckCount(store_);
}

SQLOps::OpPlan& SQLOps::OpContext::getPlan() {
	return storeEntry_.getPlan();
}

SQLOps::OpContext::Source SQLOps::OpContext::getSource() {
	return Source(storeId_, store_, NULL);
}

SQLOps::OpCursor::Source SQLOps::OpContext::getCursorSource() {
	OpCursor::Source source(storeId_, store_, this);
	source.setExtContext(extCxt_);
	return source;
}

SQLOps::OpAllocatorManager& SQLOps::OpContext::getAllocatorManager() {
	return storeEntry_.getAllocatorManager();
}

bool SQLOps::OpContext::tryLatch(uint64_t maxSize, bool hot) {
	return storeEntry_.tryLatch(getExtContext(), maxSize, hot);
}

void SQLOps::OpContext::adjustLatch(uint64_t size) {
	return storeEntry_.adjustLatch(size);
}

SQLValues::ValueProfile* SQLOps::OpContext::getValueProfile() {
	return SQLExprs::ExprProfile::getValueProfile(getExprProfile());
}

SQLExprs::ExprProfile* SQLOps::OpContext::getExprProfile() {
	return OpProfilerOptimizationEntry::getExprProfile(getOptimizationProfiler());
}

SQLOps::OpProfilerOptimizationEntry*
SQLOps::OpContext::getOptimizationProfiler() {
	OpProfilerEntry *profilerEntry = storeEntry_.getProfiler();
	if (profilerEntry == NULL) {
		return NULL;
	}
	return &profilerEntry->getOptimizationEntry();
}

SQLOps::OpProfilerIndexEntry* SQLOps::OpContext::getIndexProfiler() {
	OpProfilerEntry *profilerEntry = storeEntry_.getProfiler();
	if (profilerEntry == NULL) {
		return NULL;
	}
	return &profilerEntry->getIndexEntry();
}

uint32_t SQLOps::OpContext::getInputCount() {
	return storeEntry_.getInputCount();
}

uint32_t SQLOps::OpContext::getOutputCount() {
	return storeEntry_.getOutputCount();
}

uint32_t SQLOps::OpContext::getColumnCount(uint32_t index) {
	return static_cast<uint32_t>(storeEntry_.getInputColumnList(index).size());
}

const SQLOps::TupleColumnList&
SQLOps::OpContext::getInputColumnList(uint32_t index) {
	return storeEntry_.getInputColumnList(index);
}

const SQLOps::TupleColumnList&
SQLOps::OpContext::getOutputColumnList(uint32_t index) {
	const bool withLocalRef = true;
	return storeEntry_.getColumnList(index, withLocalRef);
}

const SQLOps::TupleColumn&
SQLOps::OpContext::getReaderColumn(uint32_t index, uint32_t pos) {
	return storeEntry_.getInputColumnList(index)[pos];
}

const SQLOps::TupleColumn&
SQLOps::OpContext::getWriterColumn(uint32_t index, uint32_t pos) {
	const bool withLocalRef = true;
	return storeEntry_.getColumnList(index, withLocalRef)[pos];
}

uint64_t SQLOps::OpContext::getInputSize(uint32_t index) {
	const bool fromInput = true;
	const bool withLocalRef = false;
	TupleList &tupleList =
			storeEntry_.getTupleList(index, fromInput, withLocalRef);
	return tupleList.getBlockSize() * tupleList.getBlockCount();
}

uint64_t SQLOps::OpContext::getInputTupleCount(uint32_t index) {
	return storeEntry_.getInputTupleCount(index);
}

uint64_t SQLOps::OpContext::getInputBlockCount(uint32_t index) {
	const bool fromInput = true;
	const bool withLocalRef = false;
	TupleList &tupleList =
			storeEntry_.getTupleList(index, fromInput, withLocalRef);
	return tupleList.getBlockCount();
}

uint32_t SQLOps::OpContext::getInputBlockSize(uint32_t index) {
	const bool fromInput = true;
	const bool withLocalRef = false;
	TupleList &tupleList =
			storeEntry_.getTupleList(index, fromInput, withLocalRef);
	return tupleList.getBlockSize();
}

SQLOps::TupleListReader& SQLOps::OpContext::getReader(
		uint32_t index, uint32_t sub) {
	const bool fromInput = true;
	return storeEntry_.getReader(index, sub, fromInput);
}

SQLOps::TupleListReader& SQLOps::OpContext::getLocalReader(
		uint32_t index, uint32_t sub) {
	const bool fromInput = false;
	return storeEntry_.getReader(index, sub, fromInput);
}

SQLOps::TupleListWriter& SQLOps::OpContext::getWriter(uint32_t index) {
	const bool withLocalRef = true;
	return storeEntry_.getWriter(index, withLocalRef);
}

void SQLOps::OpContext::addOutputTupleCount(uint32_t index, int64_t count) {
	const bool withLocalRef = true;
	return storeEntry_.addOutputTupleCount(index, withLocalRef, count);
}

SQLOps::TupleListReader** SQLOps::OpContext::getActiveReaderRef() {
	return storeEntry_.getActiveReaderRef();
}

SQLOps::ReadableTuple* SQLOps::OpContext::getReadableTupleRef(uint32_t index) {
	return storeEntry_.getReadableTupleRef(index);
}

SQLOps::SummaryTuple* SQLOps::OpContext::getSummaryTupleRef(uint32_t index) {
	return storeEntry_.getSummaryTupleRef(index);
}

SQLOps::SummaryColumnList* SQLOps::OpContext::getSummaryColumnListRef(
		uint32_t index) {
	return storeEntry_.getSummaryColumnListRef(index);
}

SQLOps::SummaryTuple* SQLOps::OpContext::getDefaultAggregationTupleRef() {
	return storeEntry_.getAggregationTupleRef();
}

SQLOps::SummaryTupleSet* SQLOps::OpContext::getDefaultAggregationTupleSet() {
	return storeEntry_.getAggregationTupleSet();
}

bool SQLOps::OpContext::findRemainingInput(uint32_t *index) {
	return storeEntry_.findRemainingInput(index);
}

void SQLOps::OpContext::popRemainingInput() {
	storeEntry_.popRemainingInput();
}

void SQLOps::OpContext::setReaderRandomAccess(uint32_t index) {
	storeEntry_.setReaderRandomAccess(index);
}

void SQLOps::OpContext::keepReaderLatch(uint32_t index) {
	storeEntry_.keepReaderLatch(index);
}

void SQLOps::OpContext::releaseReaderLatch(uint32_t index) {
	storeEntry_.releaseReaderLatch(index);
}

void SQLOps::OpContext::releaseWriterLatch(uint32_t index) {
	storeEntry_.releaseWriterLatch(index);
}

void SQLOps::OpContext::setReaderLatchDelayed(uint32_t index) {
	storeEntry_.setReaderLatchDelayed(index);
}

SQLOps::OpStore::ResourceRef SQLOps::OpContext::getResourceRef(
		uint32_t index, bool forLatch) {
	return storeEntry_.getResourceRef(index, forLatch);
}

util::AllocUniquePtr<void>& SQLOps::OpContext::getResource(
		uint32_t index, SQLOpTypes::ProjectionType projType) {
	return storeEntry_.getResource(index, projType);
}

uint32_t SQLOps::OpContext::prepareLocal(uint32_t localOrdinal) {
	return storeEntry_.prepareLocal(localOrdinal);
}

uint32_t SQLOps::OpContext::createLocal(const ColumnTypeList *list) {
	return storeEntry_.createLocal(list);
}

void SQLOps::OpContext::setLocalLink(uint32_t srcIndex, uint32_t destIndex) {
	storeEntry_.setLocalLink(srcIndex, destIndex);
}

TupleList& SQLOps::OpContext::createLocalTupleList(uint32_t index) {
	storeEntry_.checkLocal(index);
	return storeEntry_.createTupleList(index);
}

void SQLOps::OpContext::closeLocalTupleList(uint32_t index) {
	storeEntry_.checkLocal(index);
	storeEntry_.closeTupleList(index);
}

void SQLOps::OpContext::closeLocalWriter(uint32_t index) {
	storeEntry_.checkLocal(index);
	storeEntry_.closeWriter(index, false);
}

bool SQLOps::OpContext::isPlanPending() const {
	return planPending_;
}

void SQLOps::OpContext::setPlanPending() {
	planPending_ = true;
}

void SQLOps::OpContext::setInputSourceType(
		uint32_t index, InputSourceType type) {
	storeEntry_.setInputSourceType(index, type);
}

void SQLOps::OpContext::setAllInputSourceType(InputSourceType type) {
	const uint32_t inputCount = getInputCount();
	for (uint32_t i = 0; i < inputCount; i++) {
		storeEntry_.setInputSourceType(i, type);
	}
}

void SQLOps::OpContext::setUpProjectionFactoryContext(
		ProjectionFactoryContext &cxt, const uint32_t *mappedInput) {
	storeEntry_.setUpProjectionFactoryContext(cxt, mappedInput);
}

int64_t SQLOps::OpContext::getInitialInterruptionCheckCount(OpStore &store) {
	const OpConfig &config = store.getConfig();
	int64_t count = config.get(SQLOpTypes::CONF_INTERRUPTION_PROJECTION_COUNT);
	if (count < 0) {
		count = 100000;
	}
	count = std::max<int64_t>(count, 1);
	return count;
}

bool SQLOps::OpContext::checkSuspendedDetail() {
	assert(interruptionCheckRemaining_ <= 0);
	if (extCxt_ != NULL) {
		extCxt_->checkCancelRequest();
	}
	setSuspended();
	return true;
}

void SQLOps::OpContext::loadTupleId() {
	const uint32_t count = storeEntry_.getOutputCount();
	for (uint32_t i = 0; i < count; i++) {
		exprCxt_.setLastTupleId(i, storeEntry_.getLastTupleId(i));
	}
}

void SQLOps::OpContext::saveTupleId() {
	const uint32_t count = storeEntry_.getOutputCount();
	for (uint32_t i = 0; i < count; i++) {
		storeEntry_.setLastTupleId(i, exprCxt_.getLastTupleId(i));
	}
}


SQLOps::OpContext::Source::Source(
		const OpStoreId &id, OpStore &store, ExtOpContext *extCxt) :
		id_(id),
		store_(store),
		extCxt_(extCxt) {
}


SQLOps::ProjectionCode::ProjectionCode() :
		type_(SQLOpTypes::END_PROJ),
		keyList_(NULL),
		typeList_(NULL),
		aggrPhase_(SQLType::END_AGG_PHASE) {
}

SQLOpTypes::ProjectionType SQLOps::ProjectionCode::getType() const {
	return type_;
}

void SQLOps::ProjectionCode::setType(SQLOpTypes::ProjectionType type) {
	type_ = type;
}

const SQLExprs::ExprCode& SQLOps::ProjectionCode::getExprCode() const {
	return exprCode_;
}

SQLExprs::ExprCode& SQLOps::ProjectionCode::getExprCode() {
	return exprCode_;
}

const SQLValues::CompColumnList* SQLOps::ProjectionCode::getKeyList() const {
	return keyList_;
}

void SQLOps::ProjectionCode::setKeyList(
		const SQLValues::CompColumnList *keyList) {
	keyList_ = keyList;
}

const SQLOps::ColumnTypeList*
SQLOps::ProjectionCode::getColumnTypeList() const {
	return typeList_;
}

void SQLOps::ProjectionCode::setColumnTypeList(const ColumnTypeList *typeList) {
	typeList_ = typeList;
}

SQLType::AggregationPhase SQLOps::ProjectionCode::getAggregationPhase() const {
	return aggrPhase_;
}

void SQLOps::ProjectionCode::setAggregationPhase(
		SQLType::AggregationPhase aggrPhase) {
	aggrPhase_ = aggrPhase;
}


SQLOps::Projection::Projection() {
	std::fill(
			chainList_, chainList_ + MAX_CHAIN_COUNT,
			static_cast<Projection*>(NULL));
	setPlanningCode(&code_.getExprCode());
}

void SQLOps::Projection::initializeProjection(OpContext &cxt) const {
	initializeProjectionAt(cxt);
	for (ChainIterator it(*this); it.exists(); it.next()) {
		it.get().initializeProjection(cxt);
	}
}

void SQLOps::Projection::updateProjectionContext(OpContext &cxt) const {
	updateProjectionContextAt(cxt);
	for (ChainIterator it(*this); it.exists(); it.next()) {
		it.get().updateProjectionContext(cxt);
	}
}

void SQLOps::Projection::initializeProjectionAt(OpContext &cxt) const {
	static_cast<void>(cxt);
}

void SQLOps::Projection::updateProjectionContextAt(OpContext &cxt) const {
	static_cast<void>(cxt);
}

void SQLOps::Projection::clearProjection(OpContext &cxt) const {
	static_cast<void>(cxt);
}

void SQLOps::Projection::projectBy(
		OpContext &cxt, const ReadableTuple &tuple) const {
	cxt.getExprContext().setReadableTuple(0, tuple);
	project(cxt);
}

void SQLOps::Projection::projectBy(
		OpContext &cxt, const DigestTupleListReader &reader) const {
	*cxt.getExprContext().getActiveReaderRef() = reader.getReader();
	project(cxt);
}

void SQLOps::Projection::projectBy(
		OpContext &cxt, const SummaryTuple &tuple) const {
	cxt.getExprContext().setSummaryTuple(0, tuple);
	project(cxt);
}

void SQLOps::Projection::projectBy(
		OpContext &cxt, TupleListReader &reader, size_t index) const {
	cxt.getExprContext().setActiveInput(static_cast<uint32_t>(index));
	cxt.getExprContext().setReader(0, &reader, NULL);
	project(cxt);
}

size_t SQLOps::Projection::getChainCount() const {
	return std::find(
			chainList_, chainList_ + MAX_CHAIN_COUNT,
			static_cast<Projection*>(NULL)) - chainList_;
}

void SQLOps::Projection::addChain(Projection &projection) {
	const size_t index = getChainCount();
	if (index >= MAX_CHAIN_COUNT) {
		return;
	}
	chainList_[index] = &projection;
}

const SQLOps::ProjectionCode& SQLOps::Projection::getProjectionCode() const {
	return code_;
}

SQLOps::ProjectionCode& SQLOps::Projection::getProjectionCode() {
	return code_;
}

TupleValue SQLOps::Projection::eval(SQLExprs::ExprContext &cxt) const {
	static_cast<void>(cxt);
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}


SQLOps::Projection::ChainIterator::ChainIterator(const Projection &proj) :
		proj_(proj),
		index_(0) {
}

const SQLOps::Projection& SQLOps::Projection::ChainIterator::get() const {
	assert(exists());
	return *proj_.chainList_[index_];
}

bool SQLOps::Projection::ChainIterator::exists() const {
	return index_ < MAX_CHAIN_COUNT && proj_.chainList_[index_] != NULL;
}

void SQLOps::Projection::ChainIterator::next() {
	if (exists()) {
		++index_;
	}
	else {
		assert(false);
	}
}


SQLOps::Projection::ChainModIterator::ChainModIterator(Projection &proj) :
		proj_(proj),
		index_(0) {
}

SQLOps::Projection& SQLOps::Projection::ChainModIterator::get() const {
	assert(exists());
	return *proj_.chainList_[index_];
}

bool SQLOps::Projection::ChainModIterator::exists() const {
	return index_ < MAX_CHAIN_COUNT && proj_.chainList_[index_] != NULL;
}

void SQLOps::Projection::ChainModIterator::next() {
	if (exists()) {
		++index_;
	}
	else {
		assert(false);
	}
}


const SQLOps::ProjectionFactory &SQLOps::ProjectionFactory::defaultFactory_ =
		getFactoryForRegistrar();

SQLOps::ProjectionFactory::~ProjectionFactory() {
}

SQLOps::Projection& SQLOps::ProjectionFactory::create(
		ProjectionFactoryContext &cxt, SQLOpTypes::ProjectionType type) const {
	ProjectionCode code;
	code.setType(type);
	code.getExprCode().setType(SQLType::EXPR_PROJECTION);
	return create(cxt, code);
}

SQLOps::Projection& SQLOps::ProjectionFactory::create(
		ProjectionFactoryContext &cxt, const ProjectionCode &code) const {
	if (code.getExprCode().getType() == SQLType::START_EXPR) {
		ProjectionCode modCode = code;
		modCode.getExprCode().setType(SQLType::EXPR_PROJECTION);
		return create(cxt, modCode);
	}

	const SQLOpTypes::ProjectionType type = code.getType();
	assert(0 <= type && type < SQLOpTypes::END_PROJ);

	assert(funcTable_ != NULL);
	FactoryFunc func = (*funcTable_)[type];

	assert(func != NULL);
	Projection &proj = func(cxt, code);
	proj.getProjectionCode() = code;
	proj.initialize(cxt.getExprFactoryContext(), code.getExprCode());

	return proj;
}

void SQLOps::ProjectionFactory::addDefaultEntry(
		SQLOpTypes::ProjectionType type, FactoryFunc func) {
	assert(0 <= type && type < SQLOpTypes::END_PROJ);

	assert(funcTable_ != NULL);
	FactoryFunc &dest = (*funcTable_)[type];

	assert(dest == NULL);
	dest = func;
}

const SQLOps::ProjectionFactory&
SQLOps::ProjectionFactory::getDefaultFactory() {
	return defaultFactory_;
}

SQLOps::ProjectionFactory&
SQLOps::ProjectionFactory::getFactoryForRegistrar() {
	static FactoryFunc funcTable[ENTRY_COUNT] = { NULL };
	static ProjectionFactory factory(&funcTable);
	return factory;
}

SQLOps::ProjectionFactory::ProjectionFactory() :
		funcTable_(NULL) {
}

SQLOps::ProjectionFactory::ProjectionFactory(
		FactoryFunc (*funcTable)[ENTRY_COUNT]) :
		funcTable_(funcTable) {
}


SQLOps::ProjectionFactoryContext::ProjectionFactoryContext(util::StackAllocator &alloc) :
		factory_(&ProjectionFactory::getDefaultFactory()),
		exprCxt_(alloc),
		allWriterRefList_(alloc),
		optProfiler_(NULL) {
}

util::StackAllocator& SQLOps::ProjectionFactoryContext::getAllocator() {
	return exprCxt_.getAllocator();
}

const SQLOps::ProjectionFactory& SQLOps::ProjectionFactoryContext::getFactory() {
	return *factory_;
}

SQLExprs::ExprFactoryContext&
SQLOps::ProjectionFactoryContext::getExprFactoryContext() {
	return exprCxt_;
}

const SQLExprs::ExprFactory& SQLOps::ProjectionFactoryContext::getExprFactory() {
	return exprCxt_.getFactory();
}

void SQLOps::ProjectionFactoryContext::initializeReaderRefList(
		uint32_t index, ReaderRefList *list) {
	exprCxt_.initializeReaderRefList(index, list);
}

void SQLOps::ProjectionFactoryContext::initializeWriterRefList(
		uint32_t index, WriterRefList *list) {
	while (index >= allWriterRefList_.size()) {
		allWriterRefList_.push_back(NULL);
	}

	WriterRefList *&destList = allWriterRefList_[index];
	if (destList != NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	destList = list;
}

void SQLOps::ProjectionFactoryContext::addReaderRef(
		uint32_t index, TupleListReader **readerRef) {
	exprCxt_.addReaderRef(index, readerRef);
}

void SQLOps::ProjectionFactoryContext::addWriterRef(
		uint32_t index, TupleListWriter **writerRef) {
	if (index >= allWriterRefList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	allWriterRefList_[index]->push_back(writerRef);
}

SQLOps::OpProfilerOptimizationEntry*
SQLOps::ProjectionFactoryContext::getProfiler() {
	return optProfiler_;
}

void SQLOps::ProjectionFactoryContext::setProfiler(
		OpProfilerOptimizationEntry *optProfiler) {
	optProfiler_ = optProfiler;
}


SQLOps::OpProjectionRegistrar::OpProjectionRegistrar() throw() :
		factory_(&ProjectionFactory::getFactoryForRegistrar()) {
}

SQLOps::OpProjectionRegistrar::OpProjectionRegistrar(
		const OpProjectionRegistrar &sub) throw() :
		OpRegistrar(sub),
		factory_(NULL) {
}

void SQLOps::OpProjectionRegistrar::operator()() const {
	assert(false);
}

void SQLOps::OpProjectionRegistrar::addProjectionDirect(
		SQLOpTypes::ProjectionType type,
		ProjectionFactory::FactoryFunc func) const {
	assert(factory_ != NULL);
	factory_->addDefaultEntry(type, func);
}
