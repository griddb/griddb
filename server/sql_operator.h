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

#ifndef SQL_OPERATOR_H_
#define SQL_OPERATOR_H_

#include "sql_expression.h"
#include "sql_value.h"
#include "sql_operator_type.h"

class SQLContext;

struct SQLOps {
	typedef TupleList::Reader TupleListReader;
	typedef TupleList::Writer TupleListWriter;

	typedef TupleList::ReadableTuple ReadableTuple;
	typedef TupleList::WritableTuple WritableTuple;
	typedef TupleList::Column TupleColumn;

	typedef SQLValues::ColumnTypeList ColumnTypeList;
	typedef SQLValues::TupleColumnList TupleColumnList;

	typedef SQLValues::SummaryColumn SummaryColumn;
	typedef SQLValues::SummaryTuple SummaryTuple;
	typedef SQLValues::SummaryTupleSet SummaryTupleSet;
	typedef SQLExprs::SummaryColumnList SummaryColumnList;

	typedef SQLValues::DigestTupleListReader DigestTupleListReader;
	typedef SQLValues::DigestReadableTuple DigestReadableTuple;

	typedef SQLExprs::ReaderRefList ReaderRefList;
	typedef util::Vector<TupleListWriter**> WriterRefList;

	typedef std::pair<
			const SQLValues::CompColumnList*,
			const SQLValues::CompColumnList*> CompColumnListPair;

	typedef SQLExprs::Expression Expression;

	struct ContainerLocation;
	class OpConfig;
	class OpCode;

	class Operator;
	class OpFactory;
	class OpRegistrar;

	class OpNodeId;
	class OpNode;
	class OpPlan;
	class OpSpec;

	class OpStoreId;
	class OpCursor;
	class OpStore;
	class OpContext;

	class ProjectionCode;
	class Projection;

	class ProjectionFactory;
	class ProjectionFactoryContext;
	class OpProjectionRegistrar;

	class ExtOpContext;
	class OpCodeBuilder;

	class OpAllocatorManager;
	class OpLatchKeeperManager;
	class OpLatchKeeper;

	class OpProfilerId;
	class OpProfiler;
	class OpProfilerEntry;
	class OpProfilerOptimizationEntry;
	class OpProfilerIndexEntry;
};

class SQLOps::OpConfig {
public:
	typedef SQLOpTypes::ConfigType Type;
	typedef int64_t Value;

	OpConfig();

	Value get(Type type) const;
	void set(Type type, Value value);

	bool isSame(const OpConfig &another) const;

	OpConfig resolveAll(const OpConfig *base) const;
	static Value resolve(Type type, const OpConfig *base);

private:
	enum {
		VALUE_COUNT = SQLOpTypes::END_CONF
	};

	size_t getIndex(Type type) const;

	Value valueList_[VALUE_COUNT];
};

struct SQLOps::ContainerLocation {
	ContainerLocation();

	SQLType::TableType type_;
	int64_t dbVersionId_;
	uint64_t id_;
	int32_t schemaVersionId_;
	int64_t partitioningVersionId_;
	bool schemaVersionSpecified_;
	bool expirable_;
	bool indexActivated_;
	bool multiIndexActivated_;
};

class SQLOps::OpCode {
public:
	class ProjectionIterator;

	explicit OpCode(SQLOpTypes::Type type = SQLOpTypes::END_OP);

	bool isAssigned(SQLOpTypes::OpCodeKey key) const;


	SQLOpTypes::Type getType() const;
	const OpConfig* getConfig() const;


	uint32_t getInputCount() const;
	uint32_t getOutputCount() const;

	int32_t getDrivingInput() const;

	const ContainerLocation& getContainerLocation() const;
	const ContainerLocation* findContainerLocation() const;


	const SQLValues::CompColumnList& getKeyColumnList(
			SQLOpTypes::OpCodeKey key) const;
	const SQLValues::CompColumnList& getKeyColumnList() const;
	const SQLValues::CompColumnList& getMiddleKeyColumnList() const;

	const SQLValues::CompColumnList* findKeyColumnList(
			SQLOpTypes::OpCodeKey key) const;
	const SQLValues::CompColumnList* findKeyColumnList() const;
	const SQLValues::CompColumnList* findMiddleKeyColumnList() const;
	const CompColumnListPair* findSideKeyColumnList() const;

	const Expression* getJoinPredicate() const;
	const Expression* getFilterPredicate() const;

	const SQLExprs::IndexConditionList& getIndexConditionList() const;
	const SQLExprs::IndexConditionList* findIndexConditionList() const;


	const Projection* getProjection(SQLOpTypes::OpCodeKey key) const;
	const Projection* getPipeProjection() const;
	const Projection* getMiddleProjection() const;
	const Projection* getFinishProjection() const;

	SQLType::AggregationPhase getAggregationPhase() const;
	SQLType::JoinType getJoinType() const;
	SQLType::UnionType getUnionType() const;

	int64_t getLimit() const;
	int64_t getOffset() const;
	int64_t getSubLimit() const;
	int64_t getSubOffset() const;


	void setType(SQLOpTypes::Type type);
	void setConfig(const OpConfig *config);

	void setInputCount(uint32_t count);
	void setOutputCount(uint32_t count);

	void setDrivingInput(int32_t input);

	void setContainerLocation(const ContainerLocation *location);

	void setKeyColumnList(const SQLValues::CompColumnList *list);
	void setMiddleKeyColumnList(const SQLValues::CompColumnList *list);
	void setSideKeyColumnList(const CompColumnListPair *list);
	void setJoinPredicate(const Expression *expr);
	void setFilterPredicate(const Expression *expr);

	void setIndexConditionList(const SQLExprs::IndexConditionList *list);

	void setPipeProjection(const Projection *proj);
	void setMiddleProjection(const Projection *proj);
	void setFinishProjection(const Projection *proj);

	void setAggregationPhase(SQLType::AggregationPhase aggrPhase);
	void setJoinType(SQLType::JoinType joinType);
	void setUnionType(SQLType::UnionType unionType);

	void setLimit(int64_t limit);
	void setOffset(int64_t offset);
	void setSubLimit(int64_t limit);
	void setSubOffset(int64_t offset);

private:
	SQLOpTypes::Type type_;
	const OpConfig *config_;

	uint32_t inputCount_;
	uint32_t outputCount_;

	int32_t drivingInput_;

	const ContainerLocation *containerLocation_;

	const SQLValues::CompColumnList *keyColumnList_;
	const SQLValues::CompColumnList *middleKeyColumnList_;
	const CompColumnListPair *sideKeyColumnList_;

	const Expression *joinPredicate_;
	const Expression *filterPredicate_;

	const SQLExprs::IndexConditionList *indexConditionList_;

	const Projection *pipeProjection_;
	const Projection *middleProjection_;
	const Projection *finishProjection_;

	SQLType::AggregationPhase aggrPhase_;
	SQLType::JoinType joinType_;
	SQLType::UnionType unionType_;

	int64_t limit_;
	int64_t offset_;
	int64_t subLimit_;
	int64_t subOffset_;
};

class SQLOps::OpCode::ProjectionIterator {
public:
	explicit ProjectionIterator(const OpCode *code);

	const Projection& get() const;

	bool exists() const;
	void next();

private:
	static const SQLOpTypes::OpCodeKey KEY_LIST[];
	static const SQLOpTypes::OpCodeKey *const KEY_LIST_END;

	const SQLOpTypes::OpCodeKey *keyIt_;
	const OpCode *code_;
	const Projection *proj_;
};

class SQLOps::Operator {
public:
	Operator();
	virtual ~Operator();

	virtual void execute(OpContext &cxt) const;

	virtual void compile(OpContext &cxt) const;

protected:
	const OpCode& getCode() const;

private:
	friend class OpFactory;

	Operator(const Operator&);
	Operator& operator=(const Operator&);

	OpCode code_;
};

class SQLOps::OpFactory {
public:
	typedef util::AllocUniquePtr<Operator>::ReturnType (*FactoryFunc)(
			util::StdAllocator<void, void>&, const OpCode&);

	virtual ~OpFactory();

	virtual util::AllocUniquePtr<Operator>::ReturnType create(
			util::StdAllocator<void, void> &alloc, const OpCode &code) const;

	virtual void addDefaultEntry(SQLOpTypes::Type type, FactoryFunc func);

	static const OpFactory& getDefaultFactory();

	static OpFactory& getFactoryForRegistrar();

private:
	enum {
		ENTRY_COUNT = SQLOpTypes::END_OP
	};

	OpFactory(const OpFactory&);
	OpFactory& operator=(const OpFactory&);

	OpFactory();

	explicit OpFactory(FactoryFunc (*funcTable)[ENTRY_COUNT]);

	static const OpFactory &defaultFactory_;

	FactoryFunc (*funcTable_)[ENTRY_COUNT];
};

class SQLOps::OpRegistrar {
public:
	OpRegistrar() throw();

	explicit OpRegistrar(const OpRegistrar &sub) throw();

	virtual void operator()() const;

protected:
	template<SQLOpTypes::Type T, typename Op> void add() const {
		assert(factory_ != NULL);
		factory_->addDefaultEntry(T, &create<Op>);
	}

private:
	template<typename Op>
	static util::AllocUniquePtr<Operator>::ReturnType create(
			util::StdAllocator<void, void> &alloc, const OpCode &code) {
		static_cast<void>(code);
		util::AllocUniquePtr<Operator> op(ALLOC_NEW(alloc) Op(), alloc);
		return op;
	}

	OpFactory *factory_;
};

class SQLOps::OpNodeId {
public:
	bool isEmpty() const;
	bool operator==(const OpNodeId &another) const;
	bool operator<(const OpNodeId &another) const;

private:
	friend class OpPlan;
	SQLValues::SharedId id_;
};

class SQLOps::OpNode {
public:
	struct PlanTool;

	OpNode(
			util::StackAllocator &alloc, const OpNodeId &id, OpPlan &plan,
			SQLOpTypes::Type type, uint32_t parentIndex);

	const OpNodeId& getId() const;

	const OpNode& getInput(uint32_t index) const;
	OpNode& getInput(uint32_t index);

	uint32_t getInputPos(uint32_t index) const;

	void setInput(uint32_t index, const OpNode &node, uint32_t pos = 0);
	void addInput(const OpNode &node, uint32_t pos = 0);

	uint32_t getInputCount() const;

	const OpCode& getCode() const;
	void setCode(const OpCode &code);

	uint32_t getParentIndex() const;

private:
	typedef std::pair<OpNodeId, uint32_t> IdRefEntry;
	typedef util::Vector<IdRefEntry> NodeIdList;

	OpNode(const OpNode&);
	OpNode& operator=(const OpNode&);

	OpNodeId id_;
	OpPlan &plan_;
	NodeIdList inputIdList_;
	OpCode code_;
	uint32_t parentIndex_;
};

struct SQLOps::OpNode::PlanTool {
private:
	friend class OpPlan;
	static void reset(
			OpNode &node, const OpNodeId &id, SQLOpTypes::Type type,
			uint32_t parentIndex);
};

class SQLOps::OpPlan {
public:
	OpPlan(
			util::StackAllocator &alloc,
			uint32_t parentInCount, uint32_t parentOutCount);
	~OpPlan();

	OpNode& createNode(SQLOpTypes::Type opType);

	const OpNode& getNode(const OpNodeId &id) const;
	OpNode& getNode(const OpNodeId &id);

	const OpNode& getParentInput(uint32_t index) const;
	uint32_t getParentInputCount() const;

	const OpNode& getParentOutput(uint32_t index) const;
	OpNode& getParentOutput(uint32_t index);
	uint32_t getParentOutputCount() const;

	void linkToAllParentOutput(const OpNode &node);

	uint32_t findNextNodeIndex(const OpNodeId &id) const;
	const OpNode* findNextNode(uint32_t startIndex) const;
	const OpNode* findParentOutput(const OpNodeId &id, uint32_t pos) const;

	uint32_t getNodeOutputCount(const OpNodeId &id) const;
	bool isMultiReferenced(const OpNodeId &id, uint32_t pos) const;

	void prepareModification();

	bool isCompleted() const;
	void setCompleted();

	bool isEmpty() const;

private:
	typedef util::Vector<OpNode*> NodeList;
	typedef util::Vector<OpNodeId> NodeIdList;

	typedef util::Vector<uint32_t> RefCountEntry;
	typedef util::Vector<RefCountEntry> RefCountList;

	OpPlan(const OpPlan&);
	OpPlan& operator=(const OpPlan&);

	OpNode* newNode(SQLOpTypes::Type opType, uint32_t parentIndex);
	const RefCountEntry& getAllNodeOutputCounts();
	const RefCountList& resolveReferenceCounts() const;

	util::StackAllocator &alloc_;

	NodeList nodeList_;

	NodeIdList parentInList_;
	NodeIdList parentOutList_;
	NodeIdList normalNodeList_;

	RefCountList refCountListBase_;
	RefCountList *refCountList_;
	RefCountEntry outCountsCache_;

	SQLValues::SharedIdManager idManager_;

	bool completed_;
};

class SQLOps::OpSpec {
public:

private:
};

class SQLOps::OpStoreId {
public:
	bool isEmpty() const;

private:
	friend class OpStore;
	SQLValues::SharedId id_;
};

class SQLOps::OpCursor {
public:
	class Source;
	struct State;

	explicit OpCursor(const Source &source);
	~OpCursor();

	bool exists();
	void next();

	Operator& get();
	OpContext& getContext();

	bool release();

private:
	OpCursor(const OpCursor&);
	OpCursor& operator=(const OpCursor&);

	void clearOp() throw();
	void prepareOp();

	void initializeOp();
	void finishOp();

	void finishTotalOps();
	void invalidateTotalOps();

	const OpNode* findNextNode();
	void prepareRootStore();
	OpStoreId prepareSubStore(const OpNode &node);
	bool checkSubStoreReady(const OpNodeId &nodeId);
	void setUpSubStore(const OpNode &node, const OpStoreId &subStoreId);
	void cleanUpSubStore();

	bool checkSubPlanReady(const OpStoreId &subStoreId);
	static bool trySetUpDefaultSubPlan(OpContext &cxt, const OpCode &code);
	bool finishSubPlan(
			const OpNode &node, OpContext &cxt, const OpCode *&code);

	void startExec();
	void finishExec();

	void updateContext(bool starting);

	bool isRootOp();

	OpStoreId storeId_;

	OpStore &store_;
	OpContext *parentCxt_;
	ExtOpContext *extCxt_;

	util::StdAllocator<void, void> alloc_;

	State &state_;
	const OpPlan &plan_;

	util::AllocUniquePtr<Operator> executableOp_;
	util::AllocUniquePtr<OpContext> executableCxt_;
	const OpCode *executableCode_;
	OpProfilerEntry *executableProfiler_;

	size_t planPendingCount_;
	bool completedNodeFound_;

	bool executing_;
	bool compiling_;
	bool suspended_;
	bool released_;
};

class SQLOps::OpCursor::Source {
public:
	Source(const OpStoreId &id, OpStore &store, OpContext *parentCxt);

	void setExtContext(ExtOpContext *extCxt);

private:
	friend class OpCursor;

	OpStoreId id_;
	OpStore &store_;
	OpContext *parentCxt_;
	ExtOpContext *extCxt_;
};

struct SQLOps::OpCursor::State {
public:
	explicit State(const util::StdAllocator<void, void> &alloc);

private:
	friend class OpCursor;

	typedef util::AllocMap<OpNodeId, OpStoreId> EntryMap;
	typedef util::AllocSet<OpNodeId> NodeIdSet;

	EntryMap entryMap_;
	NodeIdSet workingNodeSet_;

	NodeIdSet executedNodeSet_;

	OpNodeId lastPlannedNode_;
	OpNodeId currentWorkingNode_;

	bool problemOccurred_;

};

class SQLOps::OpStore {
private:
	struct ResourceRefData;

public:
	class AllocatorRef;
	class ResourceRef;
	class TotalStats;
	class Entry;
	typedef std::pair<uint64_t, SQLOpTypes::Type> TotalStatsElement;

	explicit OpStore(OpAllocatorManager &allocManager);
	~OpStore();

	void close();

	OpStoreId allocateEntry();
	void releaseEntry(const OpStoreId &id);

	Entry& getEntry(const OpStoreId &id);
	Entry* findEntry(const OpStoreId &id);

	void detach();
	void closeAllLatchHolders() throw();
	void closeAllLatchResources() throw();

	TupleList::Group& getTempStoreGroup();
	void setTempStoreGroup(TupleList::Group &group);

	SQLValues::VarAllocator& getVarAllocator();

	util::StackAllocator::BaseAllocator& getStackAllocatorBase();
	void setStackAllocatorBase(util::StackAllocator::BaseAllocator *allocBase);

	const OpFactory& getOpFactory();

	const OpConfig& getConfig();
	void setConfig(const OpConfig &config);

	OpAllocatorManager& getAllocatorManager();
	OpLatchKeeperManager& getLatchKeeperManager(ExtOpContext &cxt);

	OpProfiler* getProfiler();
	void activateProfiler();

	void dumpTotalTempStoreUsage(
			std::ostream &out, TotalStats &stats, const OpStoreId &id,
			bool asLine);
	void clearTotalTempStoreUsage(TotalStats &stats);

	TotalStatsElement profileTotalTempStoreUsage(
			const OpStoreId &id, TotalStatsElement &last);

private:
	typedef SQLExprs::ExprCode::InputSourceType InputSourceType;

	struct EntryElement;
	class BlockHandler;

	typedef util::Vector< std::pair<
			SQLOpTypes::ProjectionType,
			util::AllocUniquePtr<void>*> > ProjectionResourceList;

	typedef std::pair<OpStoreId, Entry*> IdEntry;
	typedef util::AllocVector<IdEntry> IdEntryList;

	typedef std::pair<OpStoreId, uint32_t> ElementRef;
	typedef util::AllocVector<ElementRef> ElementRefList;

	OpStore(const OpStore&);
	OpStore& operator=(const OpStore&);

	util::StdAllocator<void, void> alloc_;

	IdEntryList entryList_;
	SQLValues::SharedIdManager idManager_;
	SQLValues::SharedIdManager resourceIdManager_;

	ElementRefList attachedReaderList_;
	ElementRefList attachedWriterList_;
	ElementRefList usedLatchHolderList_;

	TupleList::Group *tempStoreGroup_;
	SQLValues::VarAllocator &varAlloc_;
	util::StackAllocator::BaseAllocator *allocBase_;
	const OpFactory *opFactory_;

	OpConfig config_;
	OpAllocatorManager &allocManager_;
	OpLatchKeeperManager *latchKeeperManager_;
	util::AllocUniquePtr<OpProfiler> profiler_;
	TotalStatsElement lastTempStoreUsage_;
};

struct SQLOps::OpStore::ResourceRefData {
	ResourceRefData();

	OpStore *store_;
	OpStoreId id_;
	SQLValues::SharedId resourceId_;
	uint32_t index_;
	bool forLatch_;
};

class SQLOps::OpStore::AllocatorRef {
public:
	AllocatorRef(
			OpAllocatorManager &manager,
			util::StackAllocator::BaseAllocator &base);
	~AllocatorRef();

	util::StackAllocator& get();

private:
	util::StackAllocator &alloc_;
	OpAllocatorManager &manager_;
};

class SQLOps::OpStore::ResourceRef {
public:
	explicit ResourceRef(const ResourceRefData &data);

	const util::AllocUniquePtr<void>& resolve() const;
	const util::AllocUniquePtr<void>* get() const;

private:
	ResourceRefData data_;
};

class SQLOps::OpStore::TotalStats {
public:
	static TotalStats& getInstance();

	uint64_t getTotalValue();

	uint64_t getValue(SQLOpTypes::Type opType);
	void updateValue(SQLOpTypes::Type opType, uint64_t last, uint64_t cur);

private:
	enum {
		OP_COUNT = SQLOpTypes::END_OP + 1
	};

	static TotalStats instance_;

	util::Atomic<uint64_t> totalValue_;
	util::Atomic<uint64_t> values_[OP_COUNT];
};

class SQLOps::OpStore::Entry {
public:
	Entry(OpStore &store, const OpStoreId &id);
	~Entry();

	void close(bool cursorOnly, bool withLatchHolder);
	void closeAllInputReader();
	void closeAllWriter(bool force);

	static void detachReaders(Entry *entry, uint32_t index);
	static void detachWriter(Entry *entry, uint32_t index);

	void resetLatchHolder();
	void closeLatchHolder();

	util::StackAllocator& getStackAllocator();
	SQLValues::VarContext& getVarContext();

	SQLValues::ValueContext::Source getValueContextSource(
			SQLValues::ExtValueContext *extCxt);

	bool isCompleted();
	void setCompleted();

	bool isSubPlanInvalidated();
	void setSubPlanInvalidated();

	bool isPipelined();
	void setPipelined();

	bool isInputCompleted(uint32_t index);
	bool isAllInputCompleted();

	OpPlan& getPlan();
	SQLOpTypes::Type findFirstOpType();
	OpCursor::State& getCursorState();

	const OpCode& resolvePlannedCode(
			const OpCode &src, const OpStoreId &parentId);
	const OpCode* findExecutableCode();
	const OpCode& setExecutableCode(const OpCode &code);
	void setUpProjectionFactoryContext(
			ProjectionFactoryContext &cxt, const uint32_t *mappedInput);

	const OpStoreId& getLink(uint32_t index, bool fromInput);
	void setLink(
			uint32_t index, bool fromInput, const OpStoreId &targetId,
			uint32_t targetPos);

	uint32_t getLinkPos(uint32_t index, bool fromInput);

	void setMultiReferenced(uint32_t index);

	bool findUnifiedOutput(uint32_t index);
	void setUnifiedOutput(
			uint32_t index, const OpStoreId &targetId,
			uint32_t targetPos);

	uint32_t prepareLocal(uint32_t localOrdinal);
	uint32_t createLocal(const ColumnTypeList *list);
	void setLocalLink(uint32_t srcIndex, uint32_t destIndex);

	uint32_t getInputCount();
	uint32_t getOutputCount();

	bool isLocal(uint32_t index);
	void checkLocal(uint32_t index);

	void setColumnTypeList(
			uint32_t index, const ColumnTypeList &list, bool fromInput,
			bool forLocal);
	void setColumnTypeList(
			uint32_t index, const TupleColumnType *list, size_t columnCount,
			bool fromInput, bool forLocal);

	const TupleColumnList& getInputColumnList(uint32_t index);
	const TupleColumnList& getColumnList(uint32_t index, bool withLocalRef);

	InputSourceType getInputSourceType(uint32_t index);
	void setInputSourceType(uint32_t index, InputSourceType type);
	bool isExprContextAutoSetUp();

	TupleList& createTupleList(uint32_t index);
	TupleList& getTupleList(
			uint32_t index, bool fromInput, bool withLocalRef);
	TupleList& getInputTupleList(uint32_t index);
	void closeTupleList(uint32_t index);

	uint64_t getInputTupleCount(uint32_t index);

	TupleListReader& getReader(uint32_t index, uint32_t sub, bool fromInput);
	void closeReader(uint32_t index, uint32_t sub);

	TupleListWriter& getWriter(uint32_t index, bool withLocalRef);
	void closeWriter(uint32_t index, bool force);

	int64_t getOutputTupleCount(uint32_t index, bool withLocalRef);
	void addOutputTupleCount(
			uint32_t index, bool withLocalRef, int64_t count);

	void updateAllWriterRefList();

	void updateReaderRefList(uint32_t index);
	void updateWriterRefList(uint32_t index);

	TupleListReader** getActiveReaderRef();
	ReadableTuple* getReadableTupleRef(uint32_t index);
	SummaryTuple* getSummaryTupleRef(uint32_t index);
	SummaryColumnList* getSummaryColumnListRef(uint32_t index);

	bool setUpAggregationTuple();
	SummaryTuple* getAggregationTupleRef();
	SummaryTupleSet* getAggregationTupleSet();

	void activateTransfer(ExtOpContext *cxt);
	BlockHandler& getBlockHandler(uint32_t index);
	void appendBlock(uint32_t index, const TupleList::Block &block);

	bool findRemainingInput(uint32_t *index);
	void pushRemainingInput(uint32_t index, const TupleList::Block *block);
	void popRemainingInput();

	void setReaderRandomAccess(uint32_t index);
	void keepReaderLatch(uint32_t index);
	void releaseReaderLatch(uint32_t index);
	void releaseWriterLatch(uint32_t index);

	bool isAllReaderLatchDelayed();
	bool isReaderLatchDelayed(uint32_t index);
	void setReaderLatchDelayed(uint32_t index);

	ResourceRef getResourceRef(uint32_t index, bool forLatch);
	const util::AllocUniquePtr<void>* findRefResource(
			uint32_t index, bool forLatch,
			const SQLValues::SharedId &resourceId);
	util::AllocUniquePtr<void>& getResource(
			uint32_t index, SQLOpTypes::ProjectionType projType);

	void setLatchResource(
			uint32_t index, util::AllocUniquePtr<void> &resource,
			SQLValues::BaseLatchTarget &latchTarget);
	void closeLatchResource();

	int64_t getLastTupleId(uint32_t index);
	void setLastTupleId(uint32_t index, int64_t id);

	OpAllocatorManager& getAllocatorManager();

	bool tryLatch(ExtOpContext &cxt, uint64_t maxSize, bool hot);
	void adjustLatch(uint64_t size);

	OpProfilerEntry* getProfiler();
	void activateProfiler(const OpProfilerId &id);

	uint64_t profileTempStoreUsage();

private:
	typedef std::pair<Entry*, uint32_t> EntryRef;
	typedef util::Vector<EntryElement*> ElementList;

	Entry(const Entry&);
	Entry& operator=(const Entry&);

	static TupleList::Info getTupleInfo(const EntryElement &elem);

	void setUpCode(OpCode &code, const OpStoreId &parentId, bool planning);
	int64_t inheritLimit(
			const OpCode &code, const OpStoreId &parentId, uint32_t index);

	void setReaderAttached(
			EntryElement &elem, uint32_t index, bool withLocalRef);
	void setWriterAttached(
			EntryElement &elem, uint32_t index, bool withLocalRef);

	void detachReadersDetail(EntryElement &elem);
	void detachWriterDetail(EntryElement &elem, uint32_t index);

	void updateReaderRefListDetail(EntryElement &elem);
	void updateWriterRefListDetail(EntryElement &elem, uint32_t index);

	TupleListWriter* findWriter(uint32_t index, bool withLocalRef);

	EntryElement& getOutputElement(uint32_t index, bool withLocalRef);

	EntryRef getUnifiedOutputElement(EntryElement &elem, uint32_t index);
	EntryRef findUnifiedOutputElement(EntryElement &elem, uint32_t index);

	EntryElement& prepareElement(
			uint32_t index, bool fromInput, bool forLocal);
	EntryElement& getElement(uint32_t index, bool withLocalRef);
	EntryElement* findElement(uint32_t index, bool withLocalRef);

	uint32_t getElementIndex(uint32_t index, bool withLocalRef);
	static bool isLocalRefAssigned(const EntryElement *elem);

	EntryRef getInput(uint32_t index);
	EntryRef getOutput(uint32_t index);

	OpStoreId id_;

	AllocatorRef allocRef_;
	util::StackAllocator &alloc_;

	util::LocalUniquePtr<SQLValues::VarContext> varCxt_;

	OpStore &store_;

	ElementList elemList_;

	util::AllocUniquePtr<OpPlan> plan_;
	util::AllocUniquePtr<OpCursor::State> cursorState_;
	util::AllocUniquePtr<OpCode> plannedCode_;
	util::AllocUniquePtr<OpCode> executableCode_;

	uint32_t inCount_;
	uint32_t outCount_;
	uint32_t localCount_;

	bool completed_;
	bool subPlanInvalidated_;
	bool pipelined_;

	uint32_t completedInCount_;
	uint32_t multiStageInCount_;
	uint32_t readerLatchDelayCount_;

	util::Vector<uint32_t> remainingInList_;

	util::LocalUniquePtr<uint32_t> latchHolderIndex_;
	util::AllocUniquePtr<OpLatchKeeper> latchKeeper_;
	util::AllocUniquePtr<OpProfilerEntry> profiler_;
};

struct SQLOps::OpStore::EntryElement {
public:
	typedef util::Vector<util::LocalUniquePtr<TupleListReader>*> ReaderList;
	typedef util::Vector<util::LocalUniquePtr<
			TupleListReader::Image>*> ReaderImageList;

	typedef std::pair<OpStoreId, uint32_t> ReferredIdEntry;
	typedef util::Vector<ReferredIdEntry> ReferredIdList;

	explicit EntryElement(util::StackAllocator &alloc);

	OpStoreId inId_;
	OpStoreId outId_;

	uint32_t inPos_;
	uint32_t outPos_;

	uint32_t localRef_;

	OpStoreId unifiedOutId_;
	uint32_t unifiedOutPos_;

	bool multiRef_;

	util::LocalUniquePtr<TupleList> tupleList_;
	util::LocalUniquePtr<TupleList> inTupleList_;

	ReaderList readerList_;
	util::LocalUniquePtr<TupleListWriter> writer_;
	util::AllocUniquePtr<BlockHandler> blockHandler_;

	TupleListReader *activeReaderRef_;
	ReadableTuple readableTuple_;
	SummaryTuple summaryTuple_;
	SummaryColumnList summaryColumnList_;

	SummaryTuple aggrTuple_;
	util::LocalUniquePtr<SummaryTuple> defaultAggrTuple_;
	util::AllocUniquePtr<SummaryTupleSet> aggrTupleSet_;

	ColumnTypeList columnTypeList_;
	TupleColumnList columnList_;

	util::AllocUniquePtr<void> resource_;
	util::AllocUniquePtr<void> latchResource_;
	SQLValues::SharedId latchResourceId_;
	ProjectionResourceList projResourceList_;

	int64_t lastTupleId_;

	ReaderImageList readerImageList_;
	util::LocalUniquePtr<TupleListWriter::Image> writerImage_;

	uint64_t inputConsumedBlockCount_;
	uint64_t blockTupleCount_;
	int64_t outputTupleCount_;

	bool readerRandomAccessing_;
	bool readerLatchKeeping_;
	bool readerLatchDelayed_;
	bool inputRemaining_;

	bool readerAttached_;
	bool writerAttached_;

	ReaderRefList readerRefList_;
	WriterRefList writerRefList_;
	InputSourceType inputSourceType_;

	ReferredIdList referredIdList_;

private:
	EntryElement(const EntryElement&);
	EntryElement& operator=(const EntryElement&);
};

class SQLOps::OpStore::BlockHandler : public TupleList::WriterHandler {
public:
	explicit BlockHandler(uint32_t id);

	void close();
	void detach(util::StackAllocator &alloc);

	void activate(ExtOpContext &extCxt);

	void bindWriter(TupleListWriter &writer);

	virtual void operator()();

	static void unbindWriter(TupleListWriter &writer);

private:
	static BlockHandler emptyHandler_;

	uint32_t id_;
	ExtOpContext *extCxt_;
	util::LocalUniquePtr<TupleList::BlockReader> reader_;
	util::LocalUniquePtr<TupleList::BlockReader::Image> readerImage_;
	bool closed_;
};

class SQLOps::OpContext {
public:
	typedef SQLExprs::ExprCode::InputSourceType InputSourceType;

	class Source;

	explicit OpContext(const Source &source);

	void release();

	SQLExprs::ExprContext& getExprContext();
	SQLValues::ValueContext& getValueContext();
	SQLValues::VarContext& getVarContext();

	ExtOpContext& getExtContext();
	ExtOpContext* findExtContext();

	util::StackAllocator& getAllocator();

	bool isExprContextAutoSetUp();
	void setUpExprContext();
	void setUpExprInputContext(uint32_t index);


	bool isCompleted();
	void setCompleted();

	bool isInputCompleted(uint32_t index);
	bool isAllInputCompleted();

	void invalidate();
	bool isInvalidated();
	bool isSubPlanInvalidated();

	bool checkSuspended();
	bool checkSuspendedAlways(bool suspendable = true);
	void setSuspended();
	bool isSuspended();

	uint64_t getNextCountForSuspend();
	void setNextCountForSuspend(uint64_t count);
	void resetNextCountForSuspend();

	OpPlan& getPlan();
	Source getSource();
	OpCursor::Source getCursorSource();

	OpAllocatorManager& getAllocatorManager();

	bool tryLatch(uint64_t maxSize, bool hot);
	void adjustLatch(uint64_t size);

	SQLValues::ValueProfile* getValueProfile();
	SQLExprs::ExprProfile* getExprProfile();

	OpProfilerOptimizationEntry* getOptimizationProfiler();
	OpProfilerIndexEntry* getIndexProfiler();


	uint32_t getInputCount();
	uint32_t getOutputCount();
	uint32_t getColumnCount(uint32_t index);

	const TupleColumnList& getInputColumnList(uint32_t index);
	const TupleColumnList& getOutputColumnList(uint32_t index);

	const TupleColumn& getReaderColumn(uint32_t index, uint32_t pos);
	const TupleColumn& getWriterColumn(uint32_t index, uint32_t pos);

	uint64_t getInputSize(uint32_t index);
	uint64_t getInputTupleCount(uint32_t index);
	uint64_t getInputBlockCount(uint32_t index);
	uint32_t getInputBlockSize(uint32_t index);

	TupleListReader& getReader(uint32_t index, uint32_t sub = 0);
	TupleListReader& getLocalReader(uint32_t index, uint32_t sub = 0);

	TupleListWriter& getWriter(uint32_t index);

	void addOutputTupleCount(uint32_t index, int64_t count);

	TupleListReader** getActiveReaderRef();
	ReadableTuple* getReadableTupleRef(uint32_t index);
	SummaryTuple* getSummaryTupleRef(uint32_t index);
	SummaryColumnList* getSummaryColumnListRef(uint32_t index);

	SummaryTuple* getDefaultAggregationTupleRef();
	SummaryTupleSet* getDefaultAggregationTupleSet();

	bool findRemainingInput(uint32_t *index);
	void popRemainingInput();

	void setReaderRandomAccess(uint32_t index);
	void keepReaderLatch(uint32_t index);
	void releaseReaderLatch(uint32_t index);
	void releaseWriterLatch(uint32_t index);

	void setReaderLatchDelayed(uint32_t index);

	OpStore::ResourceRef getResourceRef(uint32_t index, bool forLatch);
	util::AllocUniquePtr<void>& getResource(
			uint32_t index,
			SQLOpTypes::ProjectionType projType = SQLOpTypes::END_PROJ);
	template<typename T>
	void setLatchResource(
			uint32_t index, util::AllocUniquePtr<T> &resource);

	uint32_t prepareLocal(uint32_t localOrdinal);
	uint32_t createLocal(const ColumnTypeList *list = NULL);

	void setLocalLink(uint32_t srcIndex, uint32_t destIndex);
	TupleList& createLocalTupleList(uint32_t index);
	void closeLocalTupleList(uint32_t index);
	void closeLocalWriter(uint32_t index);


	bool isPlanPending() const;
	void setPlanPending();

	void setInputSourceType(uint32_t index, InputSourceType type);
	void setAllInputSourceType(InputSourceType type);

	void setUpProjectionFactoryContext(
			ProjectionFactoryContext &cxt, const uint32_t *mappedInput);

private:
	OpContext(const OpContext&);
	OpContext& operator=(const OpContext&);

	static int64_t getInitialInterruptionCheckCount(OpStore &store);

	bool checkSuspendedDetail();

	void loadTupleId();
	void saveTupleId();

	OpStoreId storeId_;
	OpStore &store_;

	OpStore::Entry &storeEntry_;
	SQLExprs::ExprContext exprCxt_;
	ExtOpContext *extCxt_;

	uint64_t interruptionCheckRemaining_;

	bool invalidated_;
	bool suspended_;
	bool planPending_;
	bool exprCxtAvailable_;
};

class SQLOps::OpContext::Source {
public:
	Source(const OpStoreId &id, OpStore &store, ExtOpContext *extCxt);

private:
	friend class OpContext;

	OpStoreId id_;
	OpStore &store_;
	ExtOpContext *extCxt_;
};

class SQLOps::ProjectionCode {
public:
	ProjectionCode();

	SQLOpTypes::ProjectionType getType() const;
	void setType(SQLOpTypes::ProjectionType type);

	const SQLExprs::ExprCode& getExprCode() const;
	SQLExprs::ExprCode& getExprCode();

	const SQLValues::CompColumnList* getKeyList() const;
	void setKeyList(const SQLValues::CompColumnList *keyList);

	const ColumnTypeList* getColumnTypeList() const;
	void setColumnTypeList(const ColumnTypeList *typeList);

	SQLType::AggregationPhase getAggregationPhase() const;
	void setAggregationPhase(SQLType::AggregationPhase aggrPhase);

private:
	SQLOpTypes::ProjectionType type_;

	SQLExprs::ExprCode exprCode_;
	const SQLValues::CompColumnList *keyList_;
	const ColumnTypeList *typeList_;
	SQLType::AggregationPhase aggrPhase_;
};

class SQLOps::Projection : public SQLExprs::Expression {
public:
	class ChainIterator;
	class ChainModIterator;

	Projection();

	void initializeProjection(OpContext &cxt) const;
	void updateProjectionContext(OpContext &cxt) const;

	virtual void initializeProjectionAt(OpContext &cxt) const;
	virtual void updateProjectionContextAt(OpContext &cxt) const;
	virtual void clearProjection(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const = 0;

	virtual void projectBy(
			OpContext &cxt, const ReadableTuple &tuple) const;
	virtual void projectBy(
			OpContext &cxt, const DigestTupleListReader &reader) const;
	virtual void projectBy(
			OpContext &cxt, const SummaryTuple &tuple) const;
	virtual void projectBy(
			OpContext &cxt, TupleListReader &reader, size_t index) const;

	size_t getChainCount() const;

	const Projection& chainAt(size_t index) const;
	Projection& chainAt(size_t index);

	void addChain(Projection &projection);

	const ProjectionCode& getProjectionCode() const;
	ProjectionCode& getProjectionCode();

protected:
	virtual TupleValue eval(SQLExprs::ExprContext &cxt) const;

private:
	enum {
		MAX_CHAIN_COUNT = 3
	};

	Projection *chainList_[MAX_CHAIN_COUNT];
	ProjectionCode code_;
};

class SQLOps::Projection::ChainIterator {
public:
	explicit ChainIterator(const Projection &proj);

	const Projection& get() const;

	bool exists() const;
	void next();

private:
	const Projection &proj_;
	uint32_t index_;
};

class SQLOps::Projection::ChainModIterator {
public:
	explicit ChainModIterator(Projection &proj);

	Projection& get() const;

	bool exists() const;
	void next();

private:
	Projection &proj_;
	uint32_t index_;
};

class SQLOps::ProjectionFactory {
public:
	typedef Projection& (*FactoryFunc)(
			ProjectionFactoryContext&, const ProjectionCode&);

	virtual ~ProjectionFactory();

	Projection& create(
			ProjectionFactoryContext &cxt,
			SQLOpTypes::ProjectionType type) const;

	virtual Projection& create(
			ProjectionFactoryContext &cxt, const ProjectionCode &code) const;

	virtual void addDefaultEntry(
			SQLOpTypes::ProjectionType type, FactoryFunc func);

	static const ProjectionFactory& getDefaultFactory();

	static ProjectionFactory& getFactoryForRegistrar();

protected:
	ProjectionFactory();

private:
	enum {
		ENTRY_COUNT = SQLOpTypes::END_PROJ
	};

	ProjectionFactory(const ProjectionFactory&);
	ProjectionFactory& operator=(const ProjectionFactory&);

	explicit ProjectionFactory(FactoryFunc (*funcTable)[ENTRY_COUNT]);

	static const ProjectionFactory &defaultFactory_;

	FactoryFunc (*funcTable_)[ENTRY_COUNT];
};

class SQLOps::ProjectionFactoryContext {
public:
	explicit ProjectionFactoryContext(util::StackAllocator &alloc);

	util::StackAllocator& getAllocator();

	const ProjectionFactory& getFactory();

	SQLExprs::ExprFactoryContext& getExprFactoryContext();
	const SQLExprs::ExprFactory& getExprFactory();

	void initializeReaderRefList(uint32_t index, ReaderRefList *list);
	void initializeWriterRefList(uint32_t index, WriterRefList *list);

	void addReaderRef(uint32_t index, TupleListReader **readerRef);
	void addWriterRef(uint32_t index, TupleListWriter **writerRef);

	OpProfilerOptimizationEntry* getProfiler();
	void setProfiler(OpProfilerOptimizationEntry *optProfiler);

private:
	typedef util::Vector<WriterRefList*> AllWriterRefList;

	const ProjectionFactory *factory_;
	SQLExprs::ExprFactoryContext exprCxt_;

	AllWriterRefList allWriterRefList_;
	OpProfilerOptimizationEntry *optProfiler_;
};

class SQLOps::OpProjectionRegistrar : public SQLOps::OpRegistrar {
public:
	OpProjectionRegistrar() throw();

	explicit OpProjectionRegistrar(const OpProjectionRegistrar &sub) throw();

	virtual void operator()() const;

protected:
	template<SQLOpTypes::ProjectionType T, typename Proj>
	void addProjection() const {
		addProjectionDirect(T, &create<Proj>);
	}

	template<SQLOpTypes::ProjectionType T, typename Proj>
	void addProjectionCustom() const {
		addProjectionDirect(T, &createCustom<Proj>);
	}

	void addProjectionDirect(
			SQLOpTypes::ProjectionType type,
			ProjectionFactory::FactoryFunc func) const;

private:
	template<typename Proj>
	static Projection& create(
			ProjectionFactoryContext &cxt, const ProjectionCode &code) {
		static_cast<void>(code);
		return *(ALLOC_NEW(cxt.getExprFactoryContext().getAllocator()) Proj());
	}

	template<typename Proj>
	static Projection& createCustom(
			ProjectionFactoryContext &cxt, const ProjectionCode &code) {
		static_cast<void>(code);
		return *(ALLOC_NEW(cxt.getExprFactoryContext().getAllocator()) Proj(
				cxt, code));
	}

	ProjectionFactory *factory_;
};



inline SQLExprs::ExprContext& SQLOps::OpContext::getExprContext() {
	assert(exprCxtAvailable_);
	return exprCxt_;
}

inline SQLValues::ValueContext& SQLOps::OpContext::getValueContext() {
	return exprCxt_.getValueContext();
}

inline SQLValues::VarContext& SQLOps::OpContext::getVarContext() {
	return getValueContext().getVarContext();
}

inline bool SQLOps::OpContext::checkSuspended() {
	assert(interruptionCheckRemaining_ > 0);
	if (--interruptionCheckRemaining_ <= 0) {
		return checkSuspendedDetail();
	}
	return false;
}

template<typename T>
void SQLOps::OpContext::setLatchResource(
		uint32_t index, util::AllocUniquePtr<T> &resource) {
	SQLValues::BaseLatchTarget &latchTarget = *resource;

	util::AllocUniquePtr<void> ptr(
			(typename util::AllocUniquePtr<T>::ReturnType(resource)));
	storeEntry_.setLatchResource(index, ptr, latchTarget);
}


inline const SQLOps::Projection& SQLOps::Projection::chainAt(
		size_t index) const {
	assert(index < getChainCount());
	return *chainList_[index];
}

inline SQLOps::Projection& SQLOps::Projection::chainAt(size_t index) {
	assert(index < getChainCount());
	return *chainList_[index];
}

#endif
