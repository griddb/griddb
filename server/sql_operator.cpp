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
#include "sql_common.h"


SQLOps::OpConfig::OpConfig() {
	std::fill(valueList_, valueList_ + sizeof(valueList_) / sizeof(*valueList_), -1);
}

SQLOps::OpConfig::Value SQLOps::OpConfig::get(Type type) const {
	return valueList_[getIndex(type)];
}

void SQLOps::OpConfig::set(Type type, Value value) {
	valueList_[getIndex(type)] = value;
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

	assert(index < sizeof(valueList_) / sizeof(*valueList_));
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
		containerLocation_(NULL),
		keyColumnList_(NULL),
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

const SQLOps::ContainerLocation&
SQLOps::OpCode::getContainerLocation() const {
	assert(containerLocation_ != NULL);
	return *containerLocation_;
}

const SQLOps::ContainerLocation*
SQLOps::OpCode::findContainerLocation() const {
	return containerLocation_;
}

const SQLValues::CompColumnList& SQLOps::OpCode::getKeyColumnList() const {
	assert(keyColumnList_ != NULL);
	return *keyColumnList_;
}

const SQLValues::CompColumnList* SQLOps::OpCode::findKeyColumnList() const {
	return keyColumnList_;
}

SQLValues::TupleEq SQLOps::OpCode::getEqPredicate() const {
	return SQLValues::TupleEq(getKeyColumnList());
}

SQLValues::TupleLess SQLOps::OpCode::getLessPredicate() const {
	return SQLValues::TupleLess(getKeyColumnList());
}

SQLValues::TupleGreater SQLOps::OpCode::getGreaterPredicate() const {
	return SQLValues::TupleGreater(getKeyColumnList());
}

SQLValues::TupleComparator SQLOps::OpCode::getCompPredicate(
		bool sensitive) const {
	return SQLValues::TupleComparator(getKeyColumnList(), sensitive);
}

SQLValues::TupleRangeComparator SQLOps::OpCode::getJoinRangePredicate() const {
	return SQLValues::TupleRangeComparator(getKeyColumnList());
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

void SQLOps::OpCode::setContainerLocation(const ContainerLocation *location) {
	containerLocation_ = location;
}

void SQLOps::OpCode::setKeyColumnList(const SQLValues::CompColumnList *list) {
	keyColumnList_ = list;
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
	return plan_.getNode(inputIdList_[index]);
}

SQLOps::OpNode& SQLOps::OpNode::getInput(uint32_t index) {
	assert(index < inputIdList_.size());
	return plan_.getNode(inputIdList_[index]);
}

void SQLOps::OpNode::setInput(uint32_t index, const OpNode &node) {
	plan_.prepareModification();
	if (index >= inputIdList_.size()) {
		inputIdList_.resize(index + 1, OpNodeId());
	}
	inputIdList_[index] = node.id_;
}

void SQLOps::OpNode::addInput(const OpNode &node) {
	plan_.prepareModification();
	inputIdList_.push_back(node.id_);
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

bool SQLOps::OpPlan::isMultiReferenced(const OpNodeId &id) const {
	if (refCountList_->empty()) {
		refCountList_->assign(nodeList_.size(), 0);

		for (NodeList::const_iterator it = nodeList_.begin();
				it != nodeList_.end(); ++it) {
			if (*it == NULL) {
				continue;
			}
			const uint32_t count = (*it)->getInputCount();
			for (uint32_t i = 0; i < count; i++) {
				const OpNodeId &inId = (*it)->getInput(i).getId();
				(*refCountList_)[inId.id_.getIndex(idManager_)]++;
			}
		}
	}

	return (*refCountList_)[id.id_.getIndex(idManager_)] > 1;
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
			code.setOutputCount(1); 
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


bool SQLOps::OpStoreId::isEmpty() const {
	return id_.isEmpty();
}


SQLOps::OpCursor::OpCursor(const Source &source) :
		storeId_(source.id_),
		store_(source.store_),
		parentCxt_(source.parentCxt_),
		extCxt_(source.extCxt_),
		alloc_(store_.getEntry(storeId_).getStackAllocator()),
		state_(store_.getEntry(storeId_).getCursorState()),
		plan_(store_.getEntry(storeId_).getPlan()),
		executableCode_(NULL),
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
	}

	released_ = true;
	return suspended_;
}

void SQLOps::OpCursor::clearOp() throw() {
	executableOp_.reset();

	if (executableCxt_.get() != NULL) {
		if (state_.problemOccurred_) {
			executableCxt_->close();
		}
		else {
			executableCxt_->release();
		}
	}
	executableCxt_.reset();

	state_.currentWorkingNode_ = OpNodeId();
}

void SQLOps::OpCursor::prepareOp() {
	if (executableOp_.get() != NULL || suspended_) {
		return;
	}

	util::StdAllocator<void, void> opAlloc = alloc_;

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

		const OpCode &plannedCode = node->getCode();
		const OpCode *code = subEntry.findExecutableCode();

		bool initial = false;
		if (code == NULL && !checkSubPlanReady(subStoreId)) {
			op = factory.create(opAlloc, plannedCode);
			op->compile(*cxt);
			if (!finishSubPlan(*node, *cxt, code)) {
				continue;
			}
			initial = (code != NULL);
		}
		op = factory.create(opAlloc, (code == NULL ? plannedCode : *code));

		executableOp_.swap(op);
		executableCxt_.swap(cxt);
		executableCode_ = code;

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

	for (size_t i = 0; i < 3; i++) {
		const Projection *proj = (
				i == 0 ? code->getMiddleProjection() :
				i == 1 ? code->getPipeProjection() :
				code->getFinishProjection());

		if (proj != NULL) {
			proj->initializeProjection(cxt);
		}
	}
}

void SQLOps::OpCursor::finishOp() {
	OpContext &cxt = *executableCxt_;
	if (!cxt.isCompleted() && !cxt.isInvalidated()) {
		if (executableCode_ != NULL) {
			const Projection *proj = executableCode_->getFinishProjection();
			if (proj != NULL) {
				proj->project(cxt);
			}
		}

		cxt.setCompleted();
	}
	cxt.close();

	OpStore::Entry &entry = store_.getEntry(
			state_.entryMap_.find(state_.currentWorkingNode_)->second);
	entry.closeAllInputReader();
	entry.closeAllWriter(false);

	state_.workingNodeSet_.erase(state_.currentWorkingNode_);
	completedNodeFound_ = true;
}

void SQLOps::OpCursor::finishTotalOps() {
	OpStore::Entry &topEntry = store_.getEntry(storeId_);
	topEntry.setCompleted();

	if (isRootOp() && extCxt_ != NULL) {
		extCxt_->finishRootOp();
	}
}

void SQLOps::OpCursor::invalidateTotalOps() {
	if (plan_.isCompleted()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	finishOp();
	state_.workingNodeSet_.clear();
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
		if (parentIndex != std::numeric_limits<uint32_t>::max()) {
			inStoreId = &topEntry.getLink(parentIndex, true);
		}
		else {
			State::EntryMap &entryMap = state_.entryMap_;
			State::EntryMap::iterator mapIt = entryMap.find(inNode.getId());
			assert(mapIt != entryMap.end());
			inStoreId = &mapIt->second;
		}
		subEntry.setLink(i, true, *inStoreId);
	}

	do {
		if (plan_.getParentOutputCount() > 0) {
			const OpNode &parentNode = plan_.getParentOutput(0);
			if (parentNode.getInputCount() > 0 &&
					parentNode.getInput(0).getId() == node.getId()) {
				const OpStoreId *outStoreId = &topEntry.getLink(0, false);
				if (outStoreId->isEmpty()) {
					outStoreId = &storeId_;
				}
				subEntry.setLink(0, false, *outStoreId);
				break;
			}
		}

		ColumnTypeList typeList(subEntry.getStackAllocator());
		SQLOps::OpCodeBuilder::resolveColumnTypeList(node.getCode(), typeList);
		subEntry.setColumnTypeList(0, typeList, false, false);
		subEntry.createTupleList(0);
	}
	while (false);

	if (plan_.isMultiReferenced(node.getId())) {
		subEntry.setMultiReferenced(0);
	}
}

bool SQLOps::OpCursor::checkSubPlanReady(const OpStoreId &subStoreId) {
	OpStore::Entry &subEntry = store_.getEntry(subStoreId);
	return subEntry.getPlan().isCompleted() ||
			!subEntry.getCursorState().workingNodeSet_.empty();
}

bool SQLOps::OpCursor::finishSubPlan(
		const OpNode &node, OpContext &cxt, const OpCode *&code) {
	assert(code == NULL);

	OpStore::Entry &subEntry =
			store_.getEntry(state_.entryMap_.find(node.getId())->second);
	OpPlan &subPlan = subEntry.getPlan();

	if (subPlan.isCompleted()) {
		if (subPlan.isEmpty()) {
			OpCodeBuilder builder(OpCodeBuilder::ofContext(cxt));
			code = &subEntry.setExecutableCode(
					builder.toExecutable(node.getCode()));
		}
		return true;
	}

	if (subPlan.getParentOutput(0).getInputCount() > 0) {
		subPlan.setCompleted();
		return true;
	}

	if (!cxt.isPlanPending()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (subPlan.isEmpty()) {
		return false;
	}

	const uint32_t inCount = node.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		const OpStoreId &inStoreId = subEntry.getLink(i, true);
		store_.getEntry(inStoreId).setMultiReferenced(0);
	}

	return true;
}

void SQLOps::OpCursor::startExec() {
	if (executing_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	executing_ = true;
}

void SQLOps::OpCursor::finishExec() {
	if (!executing_ || executableCxt_.get() == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	executing_ = false;

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


SQLOps::OpCursor::State::State(util::StackAllocator &alloc) :
		entryMap_(alloc),
		workingNodeSet_(alloc),
		executedNodeSet_(alloc),
		problemOccurred_(false) {
}


SQLOps::OpStore::OpStore(const util::StdAllocator<void, void> &alloc) :
		alloc_(alloc),
		entryList_(alloc),
		idManager_(alloc),
		tempStoreGroup_(NULL),
		varAlloc_(NULL),
		allocBase_(NULL),
		opFactory_(&OpFactory::getDefaultFactory()) {
}

SQLOps::OpStore::~OpStore() {
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
				it->second->close(true);
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

	util::AllocUniquePtr<Entry> entryPtr(idEntry.second, alloc_);
	idEntry.second = NULL;

	entryPtr = ALLOC_UNIQUE(alloc_, Entry, alloc_, *this);

	idEntry.first = id;
	idEntry.second = entryPtr.release();

	return id;
}

void SQLOps::OpStore::releaseEntry(const OpStoreId &id) {
	const size_t index = id.id_.getIndex(idManager_);
	IdEntry &idEntry = entryList_[index];
	if (idEntry.first.isEmpty() || idEntry.second == NULL) {
		return;
	}
	idEntry.first = OpStoreId();
	idEntry.second->close(false);
}

SQLOps::OpStore::Entry& SQLOps::OpStore::getEntry(const OpStoreId &id) {
	const size_t index = id.id_.getIndex(idManager_);
	IdEntry &idEntry = entryList_[index];
	if (idEntry.first.isEmpty() || idEntry.second == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *idEntry.second;
}

void SQLOps::OpStore::detach() {
	for (IdEntryList::iterator it = entryList_.begin();
			it != entryList_.end(); ++it) {
		Entry *entry = it->second;
		if (entry != NULL) {
			entry->detach();
		}
	}
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
	if (varAlloc_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *varAlloc_;
}

void SQLOps::OpStore::setVarAllocator(SQLValues::VarAllocator *varAlloc) {
	varAlloc_ = varAlloc;
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


SQLOps::OpStore::Entry::Entry(
		const util::StdAllocator<void, void> &alloc, OpStore &store) :
		alloc_(util::AllocatorInfo(0, "sqlOp"), &store.getStackAllocatorBase()),
		store_(store),
		elemList_(alloc_),
		inCount_(0),
		outCount_(0),
		localCount_(0),
		completed_(false),
		pipelined_(false) {
	static_cast<void>(alloc);
}

SQLOps::OpStore::Entry::~Entry() {
	try {
		close(false);
	}
	catch (...) {
	}

	for (ElementList::iterator it = elemList_.begin();
			it != elemList_.end(); ++it) {
		ALLOC_DELETE(alloc_, *it);
	}
}

void SQLOps::OpStore::Entry::close(bool cursorOnly) {
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

		for (ProjectionResourceList::iterator it =
				elem->projResourceList_.begin();
				it != elem->projResourceList_.end(); ++it) {
			ALLOC_DELETE(alloc_, it->second);
		}
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

void SQLOps::OpStore::Entry::detach() {
	for (ElementList::iterator it = elemList_.begin();
			it != elemList_.end(); ++it) {
		EntryElement *elem = *it;
		if (elem == NULL) {
			continue;
		}

		if (elem->blockHandler_.get() != NULL) {
			elem->blockHandler_->detach(getStackAllocator());
		}

		detachWriter(*elem);
		detachReaders(*elem);
	}
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
	completed_ = true;
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
	for (uint32_t i = 0; i < inCount_; i++) {
		if (!isInputCompleted(i)) {
			return false;
		}
	}
	return true;
}

SQLOps::OpPlan& SQLOps::OpStore::Entry::getPlan() {
	if (plan_.get() == NULL) {
		plan_ = ALLOC_UNIQUE(alloc_, OpPlan, alloc_, inCount_, outCount_);
	}
	return *plan_;
}

SQLOps::OpCursor::State& SQLOps::OpStore::Entry::getCursorState() {
	if (cursorState_.get() == NULL) {
		cursorState_ = ALLOC_UNIQUE(alloc_, OpCursor::State, alloc_);
	}
	return *cursorState_;
}

const SQLOps::OpCode* SQLOps::OpStore::Entry::findExecutableCode() {
	return executableCode_.get();
}

const SQLOps::OpCode& SQLOps::OpStore::Entry::setExecutableCode(
		const OpCode &code) {
	assert(executableCode_.isEmpty());
	executableCode_ = ALLOC_UNIQUE(alloc_, OpCode, code);
	return *executableCode_;
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
		uint32_t index, bool fromInput, const OpStoreId &targetId) {
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

	Entry &target = store_.getEntry(targetId);
	EntryElement &targetElem = target.getElement(0, false);

	const ColumnTypeList &columnTypeList = targetElem.columnTypeList_;
	setColumnTypeList(index, columnTypeList, fromInput, forLocal);
}

void SQLOps::OpStore::Entry::setMultiReferenced(uint32_t index) {
	getElement(index, false).multiRef_ = true;
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

	Entry &target = getInput(0);
	const ColumnTypeList &columnTypeList =
			(list == NULL ? target.getElement(0, false).columnTypeList_ : *list);
	setColumnTypeList(index, columnTypeList, fromInput, forLocal);

	++localCount_;
	return index;
}

void SQLOps::OpStore::Entry::setLocalLink(
		uint32_t srcIndex, uint32_t destIndex) {
	EntryElement &src = getElement(srcIndex, false);
	getElement(destIndex, false);

	src.localRef_ = destIndex;
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
	EntryElement &elem = prepareElement(index, fromInput, forLocal);
	elem.columnTypeList_.assign(list.begin(), list.end());

	elem.columnList_.clear();
	if (list.empty()) {
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
	return store_.getEntry(elem.inId_).getColumnList(0, false);
}

const SQLOps::TupleColumnList& SQLOps::OpStore::Entry::getColumnList(
		uint32_t index, bool withLocalRef) {
	EntryElement &elem = getElement(index, withLocalRef);
	return elem.columnList_;
}

TupleList& SQLOps::OpStore::Entry::createTupleList(uint32_t index) {
	EntryElement &elem = getElement(index, false);
	util::LocalUniquePtr<TupleList> &tupleList = elem.tupleList_;

	if (tupleList.get() != NULL) {
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
		return getOutput(index).getTupleList(0, false, false);
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

	Entry &inEntry = getInput(index);
	TupleList *srcTupleList = &inEntry.getTupleList(0, false, withLocalRef);

	EntryElement &inElem = inEntry.getElement(0, withLocalRef);
	if (!inElem.multiRef_) {
		return *srcTupleList;
	}

	TupleList::Group &group = store_.getTempStoreGroup();
	const TupleList::Info &info = getTupleInfo(inElem);

	TupleList tmpTupleList(group, info);
	{
		TupleList::BlockReader reader(*srcTupleList);
		for (TupleList::Block block; reader.next(block);) {
			tmpTupleList.append(block);
		}
		reader.close();
	}

	inEntry.closeTupleList(0);
	srcTupleList = &inEntry.createTupleList(0);

	elem.inTupleList_ = UTIL_MAKE_LOCAL_UNIQUE(
			elem.inTupleList_, TupleList, group, info);

	{
		TupleList::BlockReader reader(tmpTupleList);
		for (TupleList::Block block; reader.next(block);) {
			srcTupleList->append(block);
			elem.inTupleList_->append(block);
		}
		reader.close();
	}
	tmpTupleList.close();

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
}

SQLOps::TupleListWriter& SQLOps::OpStore::Entry::getWriter(
		uint32_t index, bool withLocalRef) {
	EntryElement &elem = getElement(index, withLocalRef);
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
				elem : getOutput(index).getElement(0, false));

		BlockHandler *blockHandler = handlerElem.blockHandler_.get();
		if (blockHandler != NULL) {
			blockHandler->bindWriter(*writer);
		}
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
				*elem : getOutput(index).getElement(0, false));

		BlockHandler *blockHandler = handlerElem.blockHandler_.get();
		if (blockHandler != NULL) {
			blockHandler->close();
		}
	}

	elem->blockHandler_.reset();
	elem->writerImage_.reset();
	elem->writer_.reset();
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
	Entry &entry = getOutput(index);
	EntryElement &elem = entry.getElement(0, withLocalRef);
	if (elem.blockHandler_.get() == NULL) {
		elem.blockHandler_ = ALLOC_UNIQUE(entry.alloc_, BlockHandler, index);
	}
	return *elem.blockHandler_;
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
	elem.readerLatchKeeping_ = false;
	detachReaders(elem);
}

void SQLOps::OpStore::Entry::releaseWriterLatch(uint32_t index) {
	const bool withLocalRef = false;
	EntryElement &elem = getElement(index, withLocalRef);
	detachWriter(elem);
}

util::AllocUniquePtr<void>&
SQLOps::OpStore::Entry::getInputResource(uint32_t index) {
	return getInput(index).getResource(index, SQLOpTypes::END_PROJ);
}

util::AllocUniquePtr<void>& 
SQLOps::OpStore::Entry::getResource(
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

TupleList::Info SQLOps::OpStore::Entry::getTupleInfo(
		const EntryElement &elem) {
	assert(!elem.columnTypeList_.empty());
	TupleList::Info info;
	SQLValues::TypeUtils::setUpTupleInfo(
			info, &elem.columnTypeList_[0], elem.columnTypeList_.size());
	return info;
}

void SQLOps::OpStore::Entry::detachReaders(EntryElement &elem) {
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
}

void SQLOps::OpStore::Entry::detachWriter(EntryElement &elem) {
	util::LocalUniquePtr<TupleListWriter> &writer = elem.writer_;
	if (writer.get() == NULL) {
		return;
	}

	util::LocalUniquePtr<TupleListWriter::Image> &image = elem.writerImage_;
	image = UTIL_MAKE_LOCAL_UNIQUE(
			image, TupleListWriter::Image, getStackAllocator());
	writer->detach(*image);
	writer.reset();
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

SQLOps::OpStore::EntryElement& SQLOps::OpStore::Entry::getElement(
		uint32_t index, bool withLocalRef) {
	EntryElement *elem = findElement(index, withLocalRef);
	if (elem == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *elem;
}

SQLOps::OpStore::EntryElement* SQLOps::OpStore::Entry::findElement(
		uint32_t index, bool withLocalRef) {
	if (index >= elemList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	EntryElement *elem = elemList_[index];

	if (withLocalRef && elem != NULL &&
			elem->localRef_ != std::numeric_limits<uint32_t>::max()) {
		return findElement(elem->localRef_, false);
	}

	return elem;
}

SQLOps::OpStore::Entry& SQLOps::OpStore::Entry::getInput(uint32_t index) {
	EntryElement &elem = getElement(index, false);
	if (elem.inId_.isEmpty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return store_.getEntry(elem.inId_);
}

SQLOps::OpStore::Entry& SQLOps::OpStore::Entry::getOutput(uint32_t index) {
	EntryElement *elem = &getElement(index, false);
	if (elem->outId_.isEmpty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	for (;;) {
		Entry &entry = store_.getEntry(elem->outId_);
		EntryElement &nextElem = entry.getElement(0, false);
		if (nextElem.outId_.isEmpty()) {
			return entry;
		}
		elem = &nextElem;
	}
}


SQLOps::OpStore::EntryElement::EntryElement(util::StackAllocator &alloc) :
		localRef_(std::numeric_limits<uint32_t>::max()),
		multiRef_(false),
		readerList_(alloc),
		columnTypeList_(alloc),
		columnList_(alloc),
		projResourceList_(alloc),
		lastTupleId_(0),
		readerImageList_(alloc),
		readerRandomAccessing_(false),
		readerLatchKeeping_(false) {
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
		interruptionCheckRemaining_(getInitialInterruptionCheckCount(extCxt_)),
		invalidated_(false),
		suspended_(false),
		planPending_(false),
		exprCxtAvailable_(false) {
}

void SQLOps::OpContext::close() {
	latchHolder_.close();
}

void SQLOps::OpContext::release() {
	if (exprCxtAvailable_) {
		saveTupleId();
	}
}

SQLExprs::ExprContext& SQLOps::OpContext::getExprContext() {
	if (!exprCxtAvailable_) {
		setUpExprContext();
	}
	return exprCxt_;
}

SQLValues::ValueContext& SQLOps::OpContext::getValueContext() {
	return exprCxt_.getValueContext();
}

SQLValues::VarContext& SQLOps::OpContext::getVarContext() {
	return getValueContext().getVarContext();
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

bool SQLOps::OpContext::isCompleted() {
	return storeEntry_.isCompleted();
}

void SQLOps::OpContext::setCompleted() {
	storeEntry_.setCompleted();
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

bool SQLOps::OpContext::checkSuspended() {
	assert(interruptionCheckRemaining_ > 0);
	if (--interruptionCheckRemaining_ <= 0) {
		if (extCxt_ != NULL) {
			extCxt_->checkCancelRequest();
		}
		setSuspended();
		return true;
	}
	return false;
}

bool SQLOps::OpContext::checkSuspendedAlways() {
	interruptionCheckRemaining_ = 1;
	return checkSuspended();
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

SQLValues::LatchHolder& SQLOps::OpContext::getLatchHolder() {
	return latchHolder_;
}

uint32_t SQLOps::OpContext::getInputCount() {
	return storeEntry_.getInputCount();
}

uint32_t SQLOps::OpContext::getColumnCount(uint32_t index) {
	return static_cast<uint32_t>(storeEntry_.getInputColumnList(index).size());
}
const SQLOps::TupleColumnList&
SQLOps::OpContext::getInputColumnList(uint32_t index) {
	return storeEntry_.getInputColumnList(index);
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

util::AllocUniquePtr<void>& SQLOps::OpContext::getInputResource(
		uint32_t index) {
	return storeEntry_.getInputResource(index);
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

int64_t SQLOps::OpContext::getInitialInterruptionCheckCount(
		ExtOpContext *extCxt) {
	int64_t count = 10000;
	if (extCxt != NULL) {
		OpConfig config;
		extCxt->getConfig(config);
		count = config.get(SQLOpTypes::CONF_INTERRUPTION_PROJECTION_COUNT);
		if (count <= 1) {
			count = 1;
		}
	}
	return count;
}

void SQLOps::OpContext::setUpExprContext() {
	const uint32_t count = storeEntry_.getInputCount();
	for (uint32_t i = 0; i < count; i++) {
		exprCxt_.setReader(
				i, getReader(i), &storeEntry_.getInputColumnList(i));
	}
	loadTupleId();
	exprCxtAvailable_ = true;
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

void SQLOps::Projection::initializeProjectionAt(OpContext &cxt) const {
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
		OpContext &cxt, TupleListReader &reader, size_t index) const {
	cxt.getExprContext().setActiveInput(static_cast<uint32_t>(index));
	cxt.getExprContext().setReader(0, reader, NULL);
	project(cxt);
}

size_t SQLOps::Projection::getChainCount() const {
	return std::find(
			chainList_, chainList_ + MAX_CHAIN_COUNT,
			static_cast<Projection*>(NULL)) - chainList_;
}

const SQLOps::Projection& SQLOps::Projection::chainAt(uint32_t index) const {
	assert(index < getChainCount());
	return *chainList_[index];
}

SQLOps::Projection& SQLOps::Projection::chainAt(uint32_t index) {
	assert(index < getChainCount());
	return *chainList_[index];
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
		exprCxt_(alloc) {
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
