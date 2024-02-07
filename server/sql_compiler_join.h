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


#ifndef SQL_COMPILER_JOIN_H_
#define SQL_COMPILER_JOIN_H_

#include "sql_compiler_internal.h"

struct SQLCompiler::ReorderJoin {
	class Tags;
	class Id;
	class ConditionCost;
	class Cost;
	class CostComparator;
	class Tree;
	class TreeSet;
	class Reporter;
	class ExplainReporter;
	class TraceReporter;
	class SubOptimizer;
};

class SQLCompiler::ReorderJoin::Tags {
public:
	enum ComparisonType {
		COMP_COND_LEVEL,
		COMP_COND_DEGREE,
		COMP_COND_WEAKNESS,
		COMP_COND_END,

		COMP_EDGE_LEVEL,
		COMP_FILTERED_TREE_WEIGHT,
		COMP_NODE_DEGREE,
		COMP_FILTER,
		COMP_EDGE = COMP_FILTER + COMP_COND_END + 1,
		COMP_TREE_WEIGHT = COMP_EDGE + COMP_COND_END + 1,
		COMP_END
	};

	template<ComparisonType C> struct ComparatorTag;

	typedef ComparatorTag<COMP_COND_LEVEL> CondLevel;
	typedef ComparatorTag<COMP_COND_DEGREE> CondDegree;
	typedef ComparatorTag<COMP_COND_WEAKNESS> CondWeakness;

	typedef ComparatorTag<COMP_EDGE_LEVEL> EdgeLevel;
	typedef ComparatorTag<COMP_FILTERED_TREE_WEIGHT> FilteredTreeWeight;
	typedef ComparatorTag<COMP_NODE_DEGREE> NodeDegree;
	typedef ComparatorTag<COMP_FILTER> Filter;
	typedef ComparatorTag<COMP_EDGE> Edge;
	typedef ComparatorTag<COMP_TREE_WEIGHT> TreeWeight;

	typedef SQLPreparedPlan::Constants Constants;
	typedef Constants::StringConstant StringConstant;

	static ComparisonType toComparisonType(
			ComparisonType baseType, ComparisonType condType = COMP_END);
	static void dumpComparisonType(std::ostream &os, ComparisonType type);

	static StringConstant comparisonTypeToSymbol(ComparisonType type);
	static StringConstant comparisonTypeToSymbolSub(
			ComparisonType type, ComparisonType conditionBase);

	static bool isTableCostAffected(ComparisonType type);
};

template<SQLCompiler::ReorderJoin::Tags::ComparisonType C>
struct SQLCompiler::ReorderJoin::Tags::ComparatorTag {
public:
	explicit ComparatorTag(const CostComparator &comp);

	const CostComparator& comparator() const;

	static ComparisonType getType();

private:
	const CostComparator &comp_;
};

class SQLCompiler::ReorderJoin::Id {
public:
	Id(JoinNodeId base, bool grouping);

	JoinNodeId getBase() const;
	bool isGrouping() const;

	bool operator==(const Id &another) const;
	bool operator<(const Id &another) const;

private:
	JoinNodeId base_;
	bool grouping_;
};

class SQLCompiler::ReorderJoin::ConditionCost {
public:
	typedef SQLPreparedPlan::OptProfile::JoinCost CostProfile;

	ConditionCost();

	static ConditionCost create(const JoinEdge &edge);
	static ConditionCost createNatural();
	static ConditionCost createCross();

	bool isEmpty() const;

	void merge(const ConditionCost &another, bool filtering);

	uint64_t applyToTable(uint64_t tableCost) const;

	int32_t compareAt(
			const ConditionCost &another, const Tags::CondLevel&) const;
	int32_t compareAt(
			const ConditionCost &another, const Tags::CondDegree&) const;
	int32_t compareAt(
			const ConditionCost &another, const Tags::CondWeakness&) const;

	static int32_t compareCost(uint64_t v1, uint64_t v2);

	int32_t compareLevel(const ConditionCost &another) const;
	static int32_t compareLevel(uint64_t v1, uint64_t v2);

	void getProfile(CostProfile &profile, bool filtering) const;
	void dump(std::ostream &os) const;

private:
	int32_t compareDirect(const ConditionCost &another) const;

	static uint32_t typeToLevel(Type type);
	static uint32_t levelToTablePct(uint32_t level);

	uint32_t level_;

	uint32_t degree_;

	uint64_t weakness_;
};

class SQLCompiler::ReorderJoin::Cost {
public:
	typedef SQLPreparedPlan::OptProfile::JoinCost CostProfile;

	Cost();

	static Cost create(const JoinNode &node, uint64_t defaultTableCost);
	static Cost create(const JoinEdge &edge);

	static Cost createNaturalEdge();
	static Cost createCrossEdge();

	static bool isHighNodeDegreeComparing(
			const Cost &cost1, const Cost &cost2);

	int32_t compareAt(const Cost &another, const Tags::EdgeLevel &tag) const;
	int32_t compareAt(
			const Cost &another, const Tags::FilteredTreeWeight&) const;
	int32_t compareAt(const Cost &another, const Tags::NodeDegree&) const;
	int32_t compareAt(const Cost &another, const Tags::Filter &tag) const;
	int32_t compareAt(const Cost &another, const Tags::Edge &tag) const;
	int32_t compareAt(const Cost &another, const Tags::TreeWeight&) const;

	void merge(const Cost &another, bool forOp);

	void getProfile(CostProfile &profile) const;
	void dump(std::ostream &os) const;

private:
	uint64_t getFilteredTableCost() const;

	static int32_t compareCost(uint64_t v1, uint64_t v2);
	static uint64_t mergeTableCost(uint64_t v1, uint64_t v2, bool forOp);

	uint32_t nodeDegree_;

	ConditionCost edgeCost_;
	ConditionCost filteringCost_;

	uint64_t tableCost_;
};

class SQLCompiler::ReorderJoin::CostComparator {
private:
	typedef SQLHintInfo::ParsedHint ParsedHint;
	typedef util::Vector<ParsedHint> ParsedHintList;

public:
	explicit CostComparator(Reporter *reporter);

	int32_t compare(const Cost &cost1, const Cost &cost2) const;

	int32_t compareCondition(
			const ConditionCost &cost1, const ConditionCost &cost2) const;

	CostComparator asFilter() const;
	CostComparator asEdge() const;

	void applyHints(const ParsedHintList &hintList);

private:
	typedef SQLPreparedPlan::Constants Constants;
	typedef Constants::StringConstant StringConstant;

	typedef std::pair<uint32_t, uint32_t> SymbolEntry;
	struct SymbolTable;

	template<typename T>
	int32_t compareAt(const Cost &cost1, const Cost &cost2) const;

	template<typename T>
	int32_t compareConditionAt(
			const ConditionCost &cost1, const ConditionCost &cost2) const;

	int32_t filterResult(Tags::ComparisonType type, int32_t baseResult) const;

	CostComparator asConditionComparator(
			Tags::ComparisonType baseType) const;
	void setConditionOptions(
			Tags::ComparisonType baseType, const CostComparator &src);

	void setReversed(Tags::ComparisonType type, bool value);
	bool isReversed(Tags::ComparisonType type) const;

	void reportHints();
	void applyReversedHint(const ParsedHint &hint);
	static bool findHintComparisonType(
			const ParsedHint &hint, Tags::ComparisonType &type,
			size_t &lastElemPos);
	static int64_t getHintLongValue(const ParsedHint &hint, size_t argPos);

	Tags::ComparisonType baseType_;
	uint32_t reverseFlags_;
	Reporter *reporter_;
};

struct SQLCompiler::ReorderJoin::CostComparator::SymbolTable {
	static bool check(const SymbolEntry *entryList, size_t count) throw();

	static bool find(StringConstant str, Tags::ComparisonType &type);

	static SymbolEntry entry(
			StringConstant str, Tags::ComparisonType type,
			Tags::ComparisonType condType = Tags::COMP_END);

	static const SymbolEntry DEFAULT_ENTRY_LIST[];
	static const size_t DEFAULT_COUNT;
	static const bool ENTRY_LIST_CHECKED;
};

class SQLCompiler::ReorderJoin::Tree {
private:
	typedef std::pair<Id, Id> Entry;
	typedef util::Vector<Entry> EntryList;

public:
	class NodeCursor;

	typedef EntryList::iterator Iterator;

	Tree(util::StackAllocator &alloc, const Cost &cost);

	void setNode(const Id &id);
	void merge(const Tree &another);
	Cost mergeCost(
			const Tree &another, const Cost &edgeCost, bool forOp) const;

	const Id* findNode() const;
	const Cost& getCost() const;
	Tree edgeElement(bool first) const;

	Iterator begin();
	Iterator end();

	bool isSameSize(const Tree &another) const;

	void exportOperations(
			util::Vector<JoinOperation> &dest, bool tableCostAffected) const;

	void dump(std::ostream &os) const;
	void dumpNodes(std::ostream &os) const;

private:
	void dumpEntry(std::ostream &os, const Entry &entry, size_t depth) const;

	bool isNodeEmpty() const;
	static size_t countNodes(const EntryList &list);

	util::StackAllocator& getAllocator() const;

	EntryList list_;
	Id nodeId_;
	Cost cost_;
};

class SQLCompiler::ReorderJoin::Tree::NodeCursor {
public:
	explicit NodeCursor(const Tree &tree);

	bool exists();
	const Id& next();

private:
	void step();

	const Tree &tree_;
	const Id *nextNodeId_;
	int32_t mode_;
	size_t ordinal_;
};

class SQLCompiler::ReorderJoin::TreeSet {
public:
	class Entry;

private:
	typedef util::StdAllocator<Entry, util::StackAllocator> EntryAlloc;
	typedef std::list<Entry, EntryAlloc> BaseList;

public:
	typedef BaseList::iterator Iterator;

	explicit TreeSet(util::StackAllocator &alloc);

	Iterator insert(const Tree *tree);
	Iterator erase(Iterator it);
	void eraseOptional(Iterator it);

	Iterator merge(Iterator it1, Iterator it2, const Cost &edgeCost);
	void mergeCost(Iterator it, const Cost &cost);

	size_t size() const;

	Iterator begin();
	Iterator end();

	Iterator matchNodes(const Tree &tree);

	void exportOperations(
			util::Vector<JoinOperation> &dest, bool tableCostAffected);

private:
	typedef util::StdAllocator<Iterator, util::StackAllocator> IteratorAlloc;
	typedef std::list<Iterator, IteratorAlloc> SubList;
	typedef SubList::iterator SubIterator;

	typedef util::Map<const Tree*, SubIterator> SubMap;
	typedef std::pair<SubMap, SubList> SubEntry;

	typedef util::Map<Id, SubEntry> NodeMap;

	void addNodes(const Tree &tree, Iterator it);
	void removeNodes(const Tree &tree);
	Iterator findByNode(const Id &nodeId);

	util::StackAllocator& getAllocator();

	BaseList baseList_;
	NodeMap nodeMap_;
};

class SQLCompiler::ReorderJoin::TreeSet::Entry {
public:
	explicit Entry(const Tree *tree);

	const Tree& get() const;

private:
	const Tree &tree_;
};

class SQLCompiler::ReorderJoin::Reporter {
public:
	Reporter();

	virtual void onInitial();
	virtual void onFinal(
			util::Vector<JoinOperation> &destJoinOpList, bool reordered);
	virtual void onInitialTreeSet(TreeSet &treeSet);
	virtual void onHintHead(bool ordered);
	virtual void onMergeTarget(
			const Tree &key1, const Tree &key2, bool matched);
	virtual void onCandidateHead();
	virtual void onCandidate(
			TreeSet &treeSet, TreeSet::Iterator edgeIt,
			TreeSet::Iterator edgeBegin, const Cost *cost);
	virtual void onMinCost(
			TreeSet &treeSet, const Tree &tree1, const Tree &tree2,
			const Tree *edge);
	virtual void onMerged(const Tree &tree);
	virtual void onMergedByHint(const Tree &tree);
	virtual void onReversedCost(Tags::ComparisonType type);

	void clearLastCriterion();
	void setLastCriterion(Tags::ComparisonType type, bool reversed);
	void setLastMinCostUpdated(
			TreeSet::Iterator edgeIt, TreeSet::Iterator edgeBegin,
			bool updated);

	bool isTableCostAffected() const;

protected:
	typedef std::pair<Tags::ComparisonType, bool> Criterion;

	static void addChain(Reporter &parent, Reporter &chain);

	bool findLastMinCostCriterion(Criterion &criterion);
	static Criterion initialCriterion();

private:
	void initializeLastCriterion();

	void addChainDetail(Reporter &chain);

	Reporter* begin();
	static bool exists(Reporter *chain);
	static void next(Reporter *&chain);

	Reporter *chain_;
	bool asChain_;
	bool tableCostAffected_;

	Criterion lastCriterion_;
	Criterion lastMinCostCriterion_;
};

class SQLCompiler::ReorderJoin::ExplainReporter :
		public SQLCompiler::ReorderJoin::Reporter {
public:
	ExplainReporter(Reporter &parent, util::StackAllocator &alloc);

	static void tryCreate(
			util::LocalUniquePtr<ExplainReporter> &explainReporter,
			util::StackAllocator &alloc, bool enabled, Reporter &parent);

protected:
	virtual void onFinal(
			util::Vector<JoinOperation> &destJoinOpList, bool reordered);
	virtual void onInitialTreeSet(TreeSet &treeSet);
	virtual void onMergeTarget(
			const Tree &key1, const Tree &key2, bool matched);
	virtual void onCandidate(
			TreeSet &treeSet, TreeSet::Iterator edgeIt,
			TreeSet::Iterator edgeBegin, const Cost *cost);
	virtual void onMinCost(
			TreeSet &treeSet, const Tree &tree1, const Tree &tree2,
			const Tree *edge);
	virtual void onMerged(const Tree &tree);
	virtual void onMergedByHint(const Tree &tree);

private:
	typedef SQLPreparedPlan::OptProfile OptProfile;
	typedef OptProfile::Join JoinProfile;
	typedef OptProfile::JoinTree TreeProfile;
	typedef OptProfile::JoinCost CostProfile;
	typedef OptProfile::JoinOrdinal JoinOrdinal;
	typedef OptProfile::JoinOrdinalList JoinOrdinalList;

	typedef util::Vector<TreeProfile> TreeProfileList;
	typedef util::Map<const Tree*, TreeProfile*> TreeProfileMap;
	typedef util::Map<JoinOrdinal, TreeProfile*> OrdinalProfileMap;
	typedef std::pair<const Tree*, const Tree*> TreePair;

	JoinProfile* generateTotalProfile(bool reordered);
	void addTreeProfile(const Tree &tree);
	void addCandidateProfile(JoinOrdinal ordinal);
	void clearCurrentElements();

	void setMerging(const Tree &tree1, const Tree &tree2);
	void addCandidate(
			const TreeProfile *tree1, const TreeProfile *tree2,
			const CostProfile *cost);
	void setBestCandidate(
			const TreeProfile *tree1, const TreeProfile *tree2);

	static bool matchJoinOrdinalList(
			const JoinOrdinalList &ordinals1, const JoinOrdinalList &ordinals2);

	TreeProfile* findEdgeElementProfile(const Tree &edge, bool first);
	JoinOrdinal findTreeOrdinal(const TreeProfile *profile);
	JoinOrdinal findNodeOrdinal(const Tree &tree);
	TreeProfile* findTreeProfile(JoinOrdinal ordinal);
	TreeProfile* findTreeProfile(const Tree *tree);

	JoinProfile& makeProfile();
	TreeProfile& makeTreeProfile(const TreeProfile *src);
	TreeProfileList& makeTreeProfileList(const TreeProfileList *src);
	CostProfile* makeCostProfile(const CostProfile *src);
	CostProfile* makeCostProfile(const Cost *cost);
	JoinOrdinalList* makeJoinOrdinalList(
			JoinOrdinal ordinal1, JoinOrdinal ordinal2);

	util::StackAllocator &alloc_;

	TreeProfileMap treeProfileMap_;
	OrdinalProfileMap ordinalProfileMap_;
	TreeProfileList totalCandidates_;

	JoinOrdinal nextOrdinal_;
	TreeProfile *topTreeProfie_;

	TreeProfileList currentCandidates_;
	TreePair merging_;

	Criterion criterion_;
	bool hintAffected_;
};

class SQLCompiler::ReorderJoin::TraceReporter :
		public SQLCompiler::ReorderJoin::Reporter {
public:
	TraceReporter(
			Reporter &parent, std::ostream *os, std::ostream *resultOs);

	static void tryCreate(
			util::LocalUniquePtr<TraceReporter> &traceReporter,
			Reporter &parent);

protected:
	virtual void onInitial();
	virtual void onFinal(
			util::Vector<JoinOperation> &destJoinOpList, bool reordered);
	virtual void onInitialTreeSet(TreeSet &treeSet);
	virtual void onHintHead(bool ordered);
	virtual void onMergeTarget(
			const Tree &key1, const Tree &key2, bool matched);
	virtual void onCandidateHead();
	virtual void onCandidate(
			TreeSet &treeSet, TreeSet::Iterator edgeIt,
			TreeSet::Iterator edgeBegin, const Cost *cost);
	virtual void onMinCost(
			TreeSet &treeSet, const Tree &tree1, const Tree &tree2,
			const Tree *edge);
	virtual void onMerged(const Tree &tree);
	virtual void onMergedByHint(const Tree &tree);
	virtual void onReversedCost(Tags::ComparisonType type);

private:
	static void dumpResult(
			std::ostream &os,
			const util::Vector<JoinOperation> &destJoinOpList);
	static void dumpCriterion(std::ostream &os, const Criterion &criterion);

	util::NormalOStringStream emptyOs_;

	std::ostream &os_;
	std::ostream &resultOs_;
};

class SQLCompiler::ReorderJoin::SubOptimizer {
private:
	typedef SQLHintInfo::ParsedHint ParsedHint;
	typedef util::Vector<ParsedHint> ParsedHintList;
	typedef SQLHintInfo::HintTableIdList HintTableIdList;

public:
	explicit SubOptimizer(util::StackAllocator &alloc);

	bool optimize(
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinEdge> &joinEdgeList,
			const util::Vector<JoinOperation> &srcJoinOpList,
			const ParsedHintList &joinHintList, bool profiling,
			util::Vector<JoinOperation> &destJoinOpList);

private:
	bool findMinCostTrees(
			TreeSet &treeSet, TreeSet &edgeSet, Cost &edgeCost,
			TreeSet::Iterator &edgeIt, TreeSet::Iterator &it1,
			TreeSet::Iterator &it2);
	bool findTargetedTrees(
			TreeSet &treeSet, const Tree &key1, const Tree &key2,
			TreeSet::Iterator &it1, TreeSet::Iterator &it2);

	void applyHints(const ParsedHintList &joinHintList, TreeSet &treeSet);
	void applyOrderedHint(const HintTableIdList &idList, TreeSet &treeSet);
	void applyUnorderedHint(const HintTableIdList &idList, TreeSet &treeSet);

	void makeNodeSet(
			const util::Vector<JoinNode> &joinNodeList, TreeSet &dest);
	void makeEdgeSet(
			const util::Vector<JoinEdge> &joinEdgeList, TreeSet &treeSet,
			TreeSet &dest);

	static uint64_t resolveDefaultTableCost(
			const util::Vector<JoinNode> &joinNodeList);

	Tree& makeOrderedHint(const HintTableIdList &idList);
	Tree& makeUnorderedHint(const HintTableIdList &idList);

	static bool checkReordered(
			const util::Vector<JoinOperation> &src,
			const util::Vector<JoinOperation> &dest);

	Tree& makeNode(const Cost &cost, const Id &id);
	Tree& makeEdge(const Cost &cost, const Id &id1, const Id &id2);
	Tree& makeTree(const Cost &cost);

	void setUpReporter(bool profiling);

	util::StackAllocator &alloc_;
	Reporter reporter_;
	util::LocalUniquePtr<ExplainReporter> explainReporter_;
	util::LocalUniquePtr<TraceReporter> traceReporter_;
	CostComparator costComparator_;
};

#endif 
