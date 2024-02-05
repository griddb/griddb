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


#include "sql_compiler_join.h"



bool SQLCompiler::reorderJoinSub(
		util::StackAllocator &alloc,
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinEdge> &joinEdgeList,
		const util::Vector<JoinOperation> &srcJoinOpList,
		const util::Vector<SQLHintInfo::ParsedHint> &joinHintList,
		util::Vector<JoinOperation> &destJoinOpList, bool legacy,
		bool profiling) {
	if (legacy) {
		return reorderJoinSubLegacy(
				alloc, joinNodeList, joinEdgeList, srcJoinOpList, joinHintList,
				destJoinOpList, profiling);
	}

	ReorderJoin::SubOptimizer optimizer(alloc);
	return optimizer.optimize(
			joinNodeList, joinEdgeList, srcJoinOpList, joinHintList, profiling,
			destJoinOpList);
}


SQLCompiler::ReorderJoin::Tags::ComparisonType
SQLCompiler::ReorderJoin::Tags::toComparisonType(
		ComparisonType baseType, ComparisonType condType) {
	const uint32_t condOffset = (condType == COMP_END ? 0 : 1 + condType);
	assert(condOffset <= COMP_COND_END);

	const uint32_t type = baseType + condOffset;
	assert(COMP_COND_END < type || type < COMP_END);

	return static_cast<ComparisonType>(type);
}

void SQLCompiler::ReorderJoin::Tags::dumpComparisonType(
		std::ostream &os, ComparisonType type) {
	const char8_t *str = SQLPreparedPlan::Constants::CONSTANT_CODER(
			comparisonTypeToSymbol(type), NULL);
	if (str == NULL) {
		os << "(Comparison:" << static_cast<int32_t>(type) << ")";
	}
	else {
		os << str;
	}
}

SQLPreparedPlan::Constants::StringConstant
SQLCompiler::ReorderJoin::Tags::comparisonTypeToSymbol(ComparisonType type) {
	return comparisonTypeToSymbolSub(type, COMP_END);
}

SQLPreparedPlan::Constants::StringConstant
SQLCompiler::ReorderJoin::Tags::comparisonTypeToSymbolSub(
		ComparisonType type, ComparisonType conditionBase) {
	switch (type) {
	case COMP_COND_LEVEL:
		return (conditionBase == COMP_FILTER ?
				Constants::STR_FILTER_LEVEL : Constants::STR_EDGE_LEVEL);
	case COMP_COND_DEGREE:
		return (conditionBase == COMP_FILTER ?
				Constants::STR_FILTER_DEGREE : Constants::STR_EDGE_DEGREE);
	case COMP_COND_WEAKNESS:
		return (conditionBase == COMP_FILTER ?
				Constants::STR_FILTER_WEAKNESS : Constants::STR_EDGE_WEAKNESS);
	case COMP_EDGE_LEVEL:
		return Constants::STR_EDGE_LEVEL;
	case COMP_FILTERED_TREE_WEIGHT:
		return Constants::STR_FILTERED_TREE_WEIGHT;
	case COMP_NODE_DEGREE:
		return Constants::STR_NODE_DEGREE;
	case COMP_FILTER:
		return Constants::STR_FILTER;
	case COMP_EDGE:
		return Constants::STR_EDGE;
	case COMP_TREE_WEIGHT:
		return Constants::STR_TREE_WEIGHT;
	default:
		if (type > COMP_FILTER && type - COMP_FILTER <= COMP_COND_END) {
			return comparisonTypeToSymbolSub(
					static_cast<ComparisonType>(type - COMP_FILTER - 1),
					COMP_FILTER);
		}
		else if (type > COMP_EDGE && type - COMP_EDGE <= COMP_COND_END) {
			return comparisonTypeToSymbolSub(
					static_cast<ComparisonType>(type - COMP_EDGE - 1),
					COMP_EDGE);
		}
		else {
			return Constants::END_STR;
		}
	}
}

bool SQLCompiler::ReorderJoin::Tags::isTableCostAffected(ComparisonType type) {
	switch (type) {
	case COMP_FILTERED_TREE_WEIGHT:
		break;
	case COMP_TREE_WEIGHT:
		break;
	default:
		return false;
	}
	return true;
}


template<SQLCompiler::ReorderJoin::Tags::ComparisonType C>
SQLCompiler::ReorderJoin::Tags::ComparatorTag<C>::ComparatorTag(
		const CostComparator &comp) :
		comp_(comp) {
}

template<SQLCompiler::ReorderJoin::Tags::ComparisonType C>
const SQLCompiler::ReorderJoin::CostComparator&
SQLCompiler::ReorderJoin::Tags::ComparatorTag<C>::comparator() const {
	return comp_;
}

template<SQLCompiler::ReorderJoin::Tags::ComparisonType C>
SQLCompiler::ReorderJoin::Tags::ComparisonType
SQLCompiler::ReorderJoin::Tags::ComparatorTag<C>::getType() {
	return C;
}


SQLCompiler::ReorderJoin::Id::Id(JoinNodeId base, bool grouping) :
		base_(base),
		grouping_(grouping) {
}

SQLCompiler::JoinNodeId SQLCompiler::ReorderJoin::Id::getBase() const {
	return base_;
}

bool SQLCompiler::ReorderJoin::Id::isGrouping() const {
	return grouping_;
}

bool SQLCompiler::ReorderJoin::Id::operator==(const Id &another) const {
	return (
			grouping_ == another.grouping_ &&
			base_ == another.base_);
}

bool SQLCompiler::ReorderJoin::Id::operator<(const Id &another) const {
	if (grouping_ != another.grouping_) {
		return grouping_ < another.grouping_;
	}
	return base_ < another.base_;
}


SQLCompiler::ReorderJoin::ConditionCost::ConditionCost() :
		level_(0),
		degree_(0),
		weakness_(0) {
}

SQLCompiler::ReorderJoin::ConditionCost
SQLCompiler::ReorderJoin::ConditionCost::create(const JoinEdge &edge) {
	ConditionCost cost;
	cost.level_ = typeToLevel(edge.type_);
	cost.degree_ = 1;
	cost.weakness_ = edge.weakness_;
	return cost;
}

SQLCompiler::ReorderJoin::ConditionCost
SQLCompiler::ReorderJoin::ConditionCost::createNatural() {
	ConditionCost cost;
	cost.level_ = typeToLevel(SQLType::OP_EQ);
	cost.degree_ = 1;
	return cost;
}

SQLCompiler::ReorderJoin::ConditionCost
SQLCompiler::ReorderJoin::ConditionCost::createCross() {
	ConditionCost cost;
	cost.level_ = 1;
	cost.degree_ = 1;
	return cost;
}

bool SQLCompiler::ReorderJoin::ConditionCost::isEmpty() const {
	return (compareDirect(ConditionCost()) == 0);
}

void SQLCompiler::ReorderJoin::ConditionCost::merge(
		const ConditionCost &another, bool filtering) {
	if (level_ > 1 && compareLevel(another) == 0) {
		degree_ = degree_ + another.degree_;
		weakness_ = (filtering ?
				std::min(weakness_, another.weakness_) :
				std::max(weakness_, another.weakness_));
		return;
	}

	const int32_t inv = (filtering || isEmpty() || another.isEmpty() ? 1 : -1);
	const bool forFirst = (compareDirect(another) * inv < 0);
	if (!forFirst) {
		*this = another;
	}
}

uint64_t SQLCompiler::ReorderJoin::ConditionCost::applyToTable(
		uint64_t tableCost) const {
	if (tableCost == 0) {
		return std::numeric_limits<uint64_t>::max();
	}

	const uint32_t full = 100;
	if (tableCost > std::numeric_limits<uint64_t>::max() / full) {
		return tableCost;
	}

	uint32_t pct = levelToTablePct(level_);
	if (pct > 0 && weakness_ > 0) {
		if (weakness_ <= full / pct) {
			pct = static_cast<uint32_t>(
					std::min<uint64_t>(pct * weakness_, full));
		}
		else {
			pct = full;
		}
	}
	return std::max<uint64_t>(tableCost * levelToTablePct(level_) / full, 1);
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareAt(
		const ConditionCost &another, const Tags::CondLevel&) const {
	return compareLevel(level_, another.level_);
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareAt(
		const ConditionCost &another, const Tags::CondDegree&) const {
	return compareLevel(degree_, another.degree_);
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareAt(
		const ConditionCost &another, const Tags::CondWeakness&) const {
	return compareCost(weakness_, another.weakness_);
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareCost(
		uint64_t v1, uint64_t v2) {
	if (v1 != v2) {
		const int32_t inv = (v1 > 0 && v2 > 0 ? 1 : -1);
		return (v1 < v2 ? -1 : 1) * inv;
	}
	return 0;
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareLevel(
		const ConditionCost &another) const {
	return compareLevel(level_, another.level_);
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareLevel(
		uint64_t v1, uint64_t v2) {
	if (v1 != v2) {
		return (v1 > v2 ? -1 : 1);
	}
	return 0;
}

void SQLCompiler::ReorderJoin::ConditionCost::getProfile(
		CostProfile &profile, bool filtering) const {
	(filtering ? profile.filterLevel_ : profile.edgeLevel_) = level_;
	(filtering ? profile.filterWeakness_ : profile.edgeWeakness_) = weakness_;

	if (degree_ > 1) {
		(filtering ? profile.filterDegree_ : profile.edgeDegree_) = degree_;
	}
}

void SQLCompiler::ReorderJoin::ConditionCost::dump(std::ostream &os) const {
	bool found = false;

	if (level_ > 0) {
		os << "L:" << level_;
		found = true;
	}

	if (degree_ > 0) {
		os << (found ? " " : "");
		os << "D:" << degree_;
		found = true;
	}

	if (weakness_ > 0) {
		os << (found ? " " : "");
		os << "W:" << weakness_;
		found = true;
	}
}

int32_t SQLCompiler::ReorderJoin::ConditionCost::compareDirect(
		const ConditionCost &another) const {
	return CostComparator(NULL).compareCondition(*this, another);
}

uint32_t SQLCompiler::ReorderJoin::ConditionCost::typeToLevel(Type type) {
	JoinScoreOpLevel base;
	switch (type) {
	case SQLType::OP_EQ:
	case SQLType::OP_IS:
	case SQLType::OP_IS_NULL:
	case SQLType::EXPR_IN:
		base = JOIN_SCORE_OP_LV4; 
		break;
	case SQLType::EXPR_BETWEEN:
	case SQLType::FUNC_LIKE:
	case SQLType::OP_LT:
	case SQLType::OP_LE:
	case SQLType::OP_GT:
	case SQLType::OP_GE:
		base = JOIN_SCORE_OP_LV3; 
		break;
	case SQLType::OP_NE:
	case SQLType::OP_IS_NOT:
	case SQLType::OP_IS_NOT_NULL:
		base = JOIN_SCORE_OP_LV1; 
		break;
	default:
		base = JOIN_SCORE_OP_LV2; 
		break;
	}
	return base + 1;
}

uint32_t SQLCompiler::ReorderJoin::ConditionCost::levelToTablePct(
		uint32_t level) {
	switch (level) {
	case 5:
		return 10; 
	case 4:
		return 50; 
	case 3:
		return 70;
	case 2:
		return 90; 
	default:
		return 100;
	}
}


SQLCompiler::ReorderJoin::Cost::Cost() :
		nodeDegree_(0),
		tableCost_(0) {
}

SQLCompiler::ReorderJoin::Cost
SQLCompiler::ReorderJoin::Cost::create(
		const JoinNode &node, uint64_t defaultTableCost) {
	Cost cost;
	cost.nodeDegree_ = 1;

	const uint64_t baseTableCost = (node.approxSize_ >= 0 ?
			static_cast<uint64_t>(node.approxSize_) : defaultTableCost);
	cost.tableCost_ = std::max<uint64_t>(baseTableCost, 1);

	return cost;
}

SQLCompiler::ReorderJoin::Cost
SQLCompiler::ReorderJoin::Cost::create(const JoinEdge &edge) {
	Cost cost;
	ConditionCost &condCost = (edge.nodeIdList_[1] == UNDEF_JOIN_NODEID ?
			cost.filteringCost_ : cost.edgeCost_);
	condCost = ConditionCost::create(edge);
	return cost;
}

SQLCompiler::ReorderJoin::Cost
SQLCompiler::ReorderJoin::Cost::createNaturalEdge() {
	Cost cost;
	cost.edgeCost_ = ConditionCost::createNatural();
	return cost;
}

SQLCompiler::ReorderJoin::Cost
SQLCompiler::ReorderJoin::Cost::createCrossEdge() {
	Cost cost;
	cost.edgeCost_ = ConditionCost::createCross();
	return cost;
}

bool SQLCompiler::ReorderJoin::Cost::isHighNodeDegreeComparing(
		const Cost &cost1, const Cost &cost2) {
	return (cost1.nodeDegree_ > 2 && cost2.nodeDegree_ > 2);
}

int32_t SQLCompiler::ReorderJoin::Cost::compareAt(
		const Cost &another, const Tags::EdgeLevel &tag) const {
	return edgeCost_.compareAt(
			another.edgeCost_,Tags::CondLevel(tag.comparator().asEdge()));
}

int32_t SQLCompiler::ReorderJoin::Cost::compareAt(
		const Cost &another, const Tags::FilteredTreeWeight&) const {
	return compareCost(
			getFilteredTableCost(),
			another.getFilteredTableCost());
}

int32_t SQLCompiler::ReorderJoin::Cost::compareAt(
		const Cost &another, const Tags::NodeDegree&) const {
	return compareCost(nodeDegree_, another.nodeDegree_);
}

int32_t SQLCompiler::ReorderJoin::Cost::compareAt(
		const Cost &another, const Tags::Filter &tag) const {
	return tag.comparator().asFilter().compareCondition(
			filteringCost_, another.filteringCost_);
}

int32_t SQLCompiler::ReorderJoin::Cost::compareAt(
		const Cost &another, const Tags::Edge &tag) const {
	return tag.comparator().asEdge().compareCondition(
			edgeCost_, another.edgeCost_);
}

int32_t SQLCompiler::ReorderJoin::Cost::compareAt(
		const Cost &another, const Tags::TreeWeight&) const {
	return compareCost(tableCost_, another.tableCost_);
}

void SQLCompiler::ReorderJoin::Cost::merge(const Cost &another, bool forOp) {
	nodeDegree_ = std::min<uint32_t>(nodeDegree_ + another.nodeDegree_, 3);
	edgeCost_.merge(another.edgeCost_, false);
	filteringCost_.merge(another.filteringCost_, true);
	tableCost_ = mergeTableCost(tableCost_, another.tableCost_, forOp);
}

void SQLCompiler::ReorderJoin::Cost::getProfile(CostProfile &profile) const {
	edgeCost_.getProfile(profile, false);
	filteringCost_.getProfile(profile, true);
	profile.approxSize_ = static_cast<int64_t>(tableCost_);
}

void SQLCompiler::ReorderJoin::Cost::dump(std::ostream &os) const {
	bool found = false;

	if (nodeDegree_ > 0) {
		os << "N:" << nodeDegree_;
		found = true;
	}

	if (!edgeCost_.isEmpty()) {
		os << (found ? " " : "");
		os << "E:{";
		edgeCost_.dump(os);
		os << "}";
		found = true;
	}

	if (!filteringCost_.isEmpty()) {
		os << (found ? " " : "");
		os << "F:{";
		filteringCost_.dump(os);
		os << "}";
		found = true;
	}

	if (tableCost_ > 0) {
		os << (found ? " " : "");
		os << "T:" << tableCost_;
		found = true;
	}
}

uint64_t SQLCompiler::ReorderJoin::Cost::getFilteredTableCost() const {
	return filteringCost_.applyToTable(tableCost_);
}

int32_t SQLCompiler::ReorderJoin::Cost::compareCost(uint64_t v1, uint64_t v2) {
	return ConditionCost::compareCost(v1, v2);
}

uint64_t SQLCompiler::ReorderJoin::Cost::mergeTableCost(
		uint64_t v1, uint64_t v2, bool forOp) {
	if (forOp && v1 > 0 && v2 > 0) {
		if (v1 > std::numeric_limits<uint64_t>::max() / v2) {
			return std::numeric_limits<uint64_t>::max();
		}
		return v1 * v2;
	}
	else {
		return std::max(v1, v2);
	}
}


SQLCompiler::ReorderJoin::CostComparator::CostComparator(Reporter *reporter) :
		baseType_(Tags::COMP_END),
		reverseFlags_(0),
		reporter_(reporter) {
}

int32_t SQLCompiler::ReorderJoin::CostComparator::compare(
		const Cost &cost1, const Cost &cost2) const {
	if (reporter_ != NULL) {
		reporter_->clearLastCriterion();
	}
	int32_t comp;
	do {
		if (Cost::isHighNodeDegreeComparing(cost1, cost2)) {
			if ((comp = compareAt<Tags::EdgeLevel>(cost1, cost2)) != 0) {
				break;
			}

			if ((comp = compareAt<Tags::FilteredTreeWeight>(
					cost1, cost2)) != 0) {
				break;
			}
		}

		if ((comp = compareAt<Tags::NodeDegree>(cost1, cost2)) != 0) {
			break;
		}

		if ((comp = compareAt<Tags::Filter>(cost1, cost2)) != 0) {
			break;
		}

		if ((comp = compareAt<Tags::Edge>(cost1, cost2)) != 0) {
			break;
		}

		comp = compareAt<Tags::TreeWeight>(cost1, cost2);
	}
	while (false);
	return comp;
}

int32_t SQLCompiler::ReorderJoin::CostComparator::compareCondition(
		const ConditionCost &cost1, const ConditionCost &cost2) const {
	int32_t comp;
	do {
		if ((comp = compareConditionAt<Tags::CondLevel>(cost1, cost2)) != 0) {
			break;
		}

		if ((comp = compareConditionAt<Tags::CondDegree>(cost1, cost2)) != 0) {
			break;
		}

		comp = compareConditionAt<Tags::CondWeakness>(cost1, cost2);
	}
	while (false);
	return comp;
}

SQLCompiler::ReorderJoin::CostComparator
SQLCompiler::ReorderJoin::CostComparator::asFilter() const {
	return asConditionComparator(Tags::COMP_FILTER);
}

SQLCompiler::ReorderJoin::CostComparator
SQLCompiler::ReorderJoin::CostComparator::asEdge() const {
	return asConditionComparator(Tags::COMP_EDGE);
}

void SQLCompiler::ReorderJoin::CostComparator::applyHints(
		const ParsedHintList &hintList) {
	reverseFlags_ = 0;
	for (ParsedHintList::const_iterator it = hintList.begin();
			it != hintList.end(); ++it) {
		applyReversedHint(*it);
	}
	reportHints();
}

template<typename T>
int32_t SQLCompiler::ReorderJoin::CostComparator::compareAt(
		const Cost &cost1, const Cost &cost2) const {
	const int32_t baseResult = cost1.compareAt(cost2, T(*this));
	return filterResult(T::getType(), baseResult);
}

template<typename T>
int32_t SQLCompiler::ReorderJoin::CostComparator::compareConditionAt(
		const ConditionCost &cost1, const ConditionCost &cost2) const {
	const int32_t baseResult = cost1.compareAt(cost2, T(*this));
	return filterResult(T::getType(), baseResult);
}

int32_t SQLCompiler::ReorderJoin::CostComparator::filterResult(
		Tags::ComparisonType type, int32_t baseResult) const {
	const bool reversed = isReversed(type);
	if (baseResult != 0 && reporter_ != NULL) {
		const Tags::ComparisonType totalType = (baseType_ == Tags::COMP_END ?
				type : Tags::toComparisonType(baseType_, type));
		reporter_->setLastCriterion(totalType, reversed);
	}
	return baseResult * (reversed ? -1 : 1);
}

SQLCompiler::ReorderJoin::CostComparator
SQLCompiler::ReorderJoin::CostComparator::asConditionComparator(
		Tags::ComparisonType baseType) const {
	CostComparator condComp(reporter_);
	condComp.baseType_ = baseType;
	condComp.setConditionOptions(baseType, *this);
	return condComp;
}

void SQLCompiler::ReorderJoin::CostComparator::setConditionOptions(
		Tags::ComparisonType baseType, const CostComparator &src) {
	const uint32_t offset = baseType + 1;
	const uint32_t mask = (1U << Tags::COMP_COND_END) - 1;
	reverseFlags_ = ((src.reverseFlags_ >> offset) & mask);
}

void SQLCompiler::ReorderJoin::CostComparator::setReversed(
		Tags::ComparisonType condType, bool value) {
	const uint32_t mask = (1U << condType);
	reverseFlags_ = ((reverseFlags_ & ~mask) | (value ? mask : 0));
}

bool SQLCompiler::ReorderJoin::CostComparator::isReversed(
		Tags::ComparisonType condType) const {
	const uint32_t mask = (1U << condType);
	return ((reverseFlags_ & mask) != 0);
}

void SQLCompiler::ReorderJoin::CostComparator::reportHints() {
	if (reporter_ == NULL || reverseFlags_ == 0) {
		return;
	}

	for (int32_t i = Tags::COMP_COND_END; ++i < Tags::COMP_END;) {
		const Tags::ComparisonType type = static_cast<Tags::ComparisonType>(i);
		if (isReversed(type)) {
			reporter_->onReversedCost(type);
		}
	}
}

void SQLCompiler::ReorderJoin::CostComparator::applyReversedHint(
		const ParsedHint &hint) {
	if (hint.hintId_ != SQLHint::OPTIMIZER_FAILURE_POINT) {
		return;
	}

	Tags::ComparisonType type;
	size_t lastElemPos;
	if (!findHintComparisonType(hint, type, lastElemPos)) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Unsupported target of OptimizerFailurePoint hint (" <<
				"pos=" << lastElemPos <<
				", element=" << SQLPreparedPlan::Constants::CONSTANT_CODER(
						hint.symbolList_.getValue(lastElemPos), "") << ")");
		return;
	}

	const bool reversed = (getHintLongValue(hint, 1) != 0);
	setReversed(type, reversed);

	if (type == Tags::toComparisonType(
			Tags::COMP_EDGE, Tags::COMP_COND_LEVEL)) {
		setReversed(Tags::COMP_EDGE_LEVEL, reversed);
	}
}

bool SQLCompiler::ReorderJoin::CostComparator::findHintComparisonType(
		const ParsedHint &hint, Tags::ComparisonType &type,
		size_t &lastElemPos) {
	const SQLHintInfo::SymbolList &list = hint.symbolList_;

	lastElemPos = 0;
	if (list.getValue(lastElemPos) != Constants::STR_REORDER_JOIN) {
		return false;
	}

	lastElemPos = 1;
	if (list.getValue(lastElemPos) != Constants::STR_REVERSED_COST) {
		return false;
	}

	lastElemPos = 2;
	return SymbolTable::find(list.getValue(lastElemPos), type);
}

int64_t SQLCompiler::ReorderJoin::CostComparator::getHintLongValue(
		const ParsedHint &hint, size_t argPos) {
	do {
		const Expr *expr = hint.hintExpr_;
		if (expr == NULL) {
			break;
		}

		const ExprList *argList = expr->next_;
		if (argList == NULL || argList->size() <= argPos) {
			break;
		}

		const Expr *arg = (*argList)[argPos];
		if (argList == NULL) {
			break;
		}

		const TupleValue &value = arg->value_;
		if (value.getType() != TupleList::TYPE_LONG) {
			break;
		}

		return value.get<int64_t>();
	}
	while (false);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
}


const SQLCompiler::ReorderJoin::CostComparator::SymbolEntry
SQLCompiler::ReorderJoin::CostComparator::SymbolTable::DEFAULT_ENTRY_LIST[] = {
	entry(Constants::STR_TREE_WEIGHT,Tags::COMP_TREE_WEIGHT),

	entry(Constants::STR_FILTERED_TREE_WEIGHT,
			Tags::COMP_FILTERED_TREE_WEIGHT),

	entry(Constants::STR_NODE_DEGREE, Tags::COMP_NODE_DEGREE),

	entry(Constants::STR_FILTER_LEVEL,
			Tags::COMP_FILTER, Tags::COMP_COND_LEVEL),
	entry(Constants::STR_FILTER_DEGREE,
			Tags::COMP_FILTER, Tags::COMP_COND_DEGREE),
	entry(Constants::STR_FILTER_WEAKNESS,
			Tags::COMP_FILTER, Tags::COMP_COND_WEAKNESS),

	entry(Constants::STR_EDGE_LEVEL,
			Tags::COMP_EDGE, Tags::COMP_COND_LEVEL),
	entry(Constants::STR_EDGE_DEGREE,
			Tags::COMP_EDGE, Tags::COMP_COND_DEGREE),
	entry(Constants::STR_EDGE_WEAKNESS,
			Tags::COMP_EDGE, Tags::COMP_COND_WEAKNESS)
};

const size_t
SQLCompiler::ReorderJoin::CostComparator::SymbolTable::DEFAULT_COUNT =
		sizeof(DEFAULT_ENTRY_LIST) / sizeof(*DEFAULT_ENTRY_LIST);

const bool
SQLCompiler::ReorderJoin::CostComparator::SymbolTable::ENTRY_LIST_CHECKED =
		check(DEFAULT_ENTRY_LIST, DEFAULT_COUNT);

bool SQLCompiler::ReorderJoin::CostComparator::SymbolTable::check(
		const SymbolEntry *entryList, size_t count) throw() {
	const SymbolEntry *prev = NULL;
	const SymbolEntry *it = entryList;
	const SymbolEntry *const end = it + count;
	for (; it != end; ++it) {
		if (prev != NULL && !(*prev < *it)) {
			return false;
		}
		prev = it;
	}
	return true;
}

bool SQLCompiler::ReorderJoin::CostComparator::SymbolTable::find(
		StringConstant str, Tags::ComparisonType &type) {
	assert(ENTRY_LIST_CHECKED);

	type = Tags::COMP_END;

	const SymbolEntry *const begin = DEFAULT_ENTRY_LIST;
	const SymbolEntry *const end = begin + DEFAULT_COUNT;
	const SymbolEntry key(str, 0);

	const SymbolEntry *const it = std::lower_bound(begin, end, key);
	if (it == end || it->first != str) {
		return false;
	}

	type = static_cast<Tags::ComparisonType>(it->second);
	return true;
}

SQLCompiler::ReorderJoin::CostComparator::SymbolEntry
SQLCompiler::ReorderJoin::CostComparator::SymbolTable::entry(
		StringConstant str, Tags::ComparisonType type,
		Tags::ComparisonType subType) {
	return SymbolEntry(str, Tags::toComparisonType(type, subType));
}


SQLCompiler::ReorderJoin::Tree::Tree(
		util::StackAllocator &alloc, const Cost &cost) :
		list_(alloc),
		nodeId_(0, true),
		cost_(cost) {
}

void SQLCompiler::ReorderJoin::Tree::setNode(const Id &id) {
	assert(!id.isGrouping());
	assert(list_.empty());
	nodeId_ = id;
}

void SQLCompiler::ReorderJoin::Tree::merge(const Tree &another) {
	const bool nodeEmpty = isNodeEmpty();
	const JoinNodeId groupEnd1 = static_cast<JoinNodeId>(list_.size());
	for (EntryList::const_iterator it = another.list_.begin();
			it != another.list_.end(); ++it) {
		Entry dest = *it;
		for (size_t i = 0; i < 2; i++) {
			Id &id = (i == 0 ? dest.first : dest.second);
			if (id.isGrouping()) {
				id = Id(id.getBase() + groupEnd1, true);
			}
		}
		list_.push_back(dest);
	}

	const JoinNodeId groupEnd2 = static_cast<JoinNodeId>(list_.size());
	if (!nodeEmpty && !another.isNodeEmpty()) {
		assert(!nodeId_.isGrouping() || groupEnd1 > 0);
		assert(!another.nodeId_.isGrouping() || groupEnd2 > 0);

		const Id id1 = (nodeId_.isGrouping() ? Id(groupEnd1 - 1, true) : nodeId_);
		const Id id2 = (another.nodeId_.isGrouping() ?
				Id(groupEnd2 - 1, true) : another.nodeId_);

		list_.push_back(Entry(id1, id2));
		nodeId_ = Id(0, true);
	}
	else if (nodeEmpty) {
		nodeId_ = another.nodeId_;
	}

	cost_.merge(another.cost_, false);
}

SQLCompiler::ReorderJoin::Cost SQLCompiler::ReorderJoin::Tree::mergeCost(
		const Tree &another, const Cost &edgeCost, bool forOp) const {
	Cost cost = edgeCost;
	cost.merge(cost_, false);
	cost.merge(another.cost_, forOp);
	return cost;
}

const SQLCompiler::ReorderJoin::Id*
SQLCompiler::ReorderJoin::Tree::findNode() const {
	if (nodeId_.isGrouping()) {
		return NULL;
	}
	return &nodeId_;
}

const SQLCompiler::ReorderJoin::Cost&
SQLCompiler::ReorderJoin::Tree::getCost() const {
	return cost_;
}

SQLCompiler::ReorderJoin::Tree
SQLCompiler::ReorderJoin::Tree::edgeElement(bool first) const {
	assert(list_.size() == 1);
	const Entry &entry = list_.front();

	Tree elem(getAllocator(), Cost());
	elem.setNode(first ? entry.first : entry.second);
	return elem;
}

SQLCompiler::ReorderJoin::Tree::Iterator
SQLCompiler::ReorderJoin::Tree::begin() {
	return list_.begin();
}

SQLCompiler::ReorderJoin::Tree::Iterator
SQLCompiler::ReorderJoin::Tree::end() {
	return list_.end();
}

bool SQLCompiler::ReorderJoin::Tree::isSameSize(const Tree &another) const {
	if (nodeId_.isGrouping() != another.nodeId_.isGrouping()) {
		return false;
	}

	if (list_.size() != another.list_.size()) {
		return false;
	}

	if (countNodes(list_) != countNodes(another.list_)) {
		return false;
	}

	return true;
}

void SQLCompiler::ReorderJoin::Tree::exportOperations(
		util::Vector<JoinOperation> &dest, bool tableCostAffected) const {
	dest.clear();

	JoinNodeId groupOffset = 0;
	for (NodeCursor cursor(*this); cursor.exists();) {
		const Id &id = cursor.next();
		groupOffset = std::max(groupOffset, id.getBase());
	}
	++groupOffset;

	for (EntryList::const_iterator it = list_.begin();
			it != list_.end(); ++it) {
		JoinOperation op;
		for (size_t i = 0; i < 2; i++) {
			const Id &id = (i == 0 ? it->first : it->second);
			JoinNodeId &destId = op.nodeIdList_[i];

			destId = id.getBase();
			if (id.isGrouping()) {
				destId += groupOffset;
			}
		}
		op.tableCostAffected_ = tableCostAffected;
		dest.push_back(op);
	}
}

void SQLCompiler::ReorderJoin::Tree::dump(std::ostream &os) const {
	dumpNodes(os);

	os << (isNodeEmpty() ? "" : " ");
	os << "{";
	cost_.dump(os);
	os << "}";
}

void SQLCompiler::ReorderJoin::Tree::dumpNodes(std::ostream &os) const {
	bool found = false;

	if (!list_.empty()) {
		dumpEntry(os, list_.back(), 1);
		found = true;
	}

	const Id *id = findNode();
	if (id != NULL) {
		os << (found ? " " : "");
		os << "(" << id->getBase() << ")";
		found = true;
	}
}

void SQLCompiler::ReorderJoin::Tree::dumpEntry(
		std::ostream &os, const Entry &entry, size_t depth) const {
	if (depth > list_.size()) {
		os << "(error)";
		assert(false);
		return;
	}

	os << "(";
	for (size_t i = 0; i < 2; i++) {
		if (i > 0) {
			os << " ";
		}
		const Id &id = (i == 0 ? entry.first : entry.second);
		if (id.isGrouping()) {
			dumpEntry(os, list_[id.getBase()], depth + 1);
		}
		else {
			os << id.getBase();
		}
	}
	os << ")";
}

bool SQLCompiler::ReorderJoin::Tree::isNodeEmpty() const {
	return nodeId_.isGrouping() && list_.empty();
}

size_t SQLCompiler::ReorderJoin::Tree::countNodes(const EntryList &list) {
	size_t count = 0;
	for (EntryList::const_iterator it = list.begin(); it != list.end(); ++it) {
		for (size_t i = 0; i < 2; i++) {
			const Id &id = (i == 0 ? it->first : it->second);
			if (!id.isGrouping()) {
				count++;
			}
		}
	}
	return count;
}

util::StackAllocator& SQLCompiler::ReorderJoin::Tree::getAllocator() const {
	util::StackAllocator *alloc = list_.get_allocator().base();
	assert(alloc != NULL);
	return *alloc;
}


SQLCompiler::ReorderJoin::Tree::NodeCursor::NodeCursor(const Tree &tree) :
		tree_(tree),
		nextNodeId_(NULL),
		mode_(-1),
		ordinal_(0) {
	step();
}

bool SQLCompiler::ReorderJoin::Tree::NodeCursor::exists() {
	return (nextNodeId_ != NULL);
}

const SQLCompiler::ReorderJoin::Id&
SQLCompiler::ReorderJoin::Tree::NodeCursor::next() {
	assert(exists());
	const Id *id = nextNodeId_;
	step();
	return *id;
}

void SQLCompiler::ReorderJoin::Tree::NodeCursor::step() {
	nextNodeId_ = NULL;

	if (mode_ < 0) {
		assert(ordinal_ == 0);
		mode_ = 0;

		const Id *id = tree_.findNode();
		if (id != NULL) {
			nextNodeId_ = id;
			return;
		}
	}

	const size_t size = tree_.list_.size();
	while (ordinal_ < size) {
		const Entry *entry = &tree_.list_[ordinal_];
		while (mode_ < 2) {
			const Id *id = &(mode_ == 0 ? entry->first : entry->second);
			++mode_;

			if (!id->isGrouping()) {
				nextNodeId_ = id;
				return;
			}
		}
		mode_ = 0;
		++ordinal_;
	}
}


SQLCompiler::ReorderJoin::TreeSet::TreeSet(util::StackAllocator &alloc) :
		baseList_(alloc),
		nodeMap_(alloc) {
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::insert(const Tree *tree) {
	assert(tree != NULL);
	baseList_.push_back(Entry(tree));

	Iterator it = end();
	addNodes(*tree, --it);
	return it;
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::erase(Iterator it) {
	assert(it != end());
	removeNodes(it->get());
	return baseList_.erase(it);
}

void SQLCompiler::ReorderJoin::TreeSet::eraseOptional(Iterator it) {
	if (it != end()) {
		erase(it);
	}
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::merge(
		Iterator it1, Iterator it2, const Cost &edgeCost) {
	assert(it1 != it2);

	util::StackAllocator &alloc = getAllocator();
	Tree *tree = ALLOC_NEW(alloc) Tree(alloc, edgeCost);
	tree->merge(it1->get());
	tree->merge(it2->get());

	erase(it2);
	Iterator basePos = erase(it1);

	Iterator pos = baseList_.insert(basePos, Entry(tree));
	addNodes(*tree, pos);
	return pos;
}

void SQLCompiler::ReorderJoin::TreeSet::mergeCost(
		Iterator it, const Cost &cost) {
	util::StackAllocator &alloc = getAllocator();

	Iterator basePos = erase(it);
	Tree *tree = ALLOC_NEW(alloc) Tree(alloc, cost);
	tree->merge(it->get());

	Iterator pos = baseList_.insert(basePos, Entry(tree));
	addNodes(*tree, pos);
}

size_t SQLCompiler::ReorderJoin::TreeSet::size() const {
	return baseList_.size();
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::begin() {
	return baseList_.begin();
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::end() {
	return baseList_.end();
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::matchNodes(const Tree &tree) {
	for (Tree::NodeCursor cursor(tree); cursor.exists();) {
		const Id &nodeId = cursor.next();
		Iterator it = findByNode(nodeId);
		if (it != end()) {
			return it;
		}
	}
	return baseList_.end();
}

void SQLCompiler::ReorderJoin::TreeSet::exportOperations(
		util::Vector<JoinOperation> &dest, bool tableCostAffected) {
	assert(size() <= 1);
	if (begin() == end()) {
		return;
	}
	else {
		begin()->get().exportOperations(dest, tableCostAffected);
	}
}

void SQLCompiler::ReorderJoin::TreeSet::addNodes(
		const Tree &tree, Iterator it) {
	util::StackAllocator &alloc = getAllocator();
	for (Tree::NodeCursor cursor(tree); cursor.exists();) {
		const Id &nodeId = cursor.next();

		NodeMap::iterator mapIt = nodeMap_.find(nodeId);
		if (mapIt == nodeMap_.end()) {
			SubEntry entry((SubMap(alloc)), (SubList(alloc)));
			mapIt = nodeMap_.insert(std::make_pair(nodeId, entry)).first;
		}

		SubEntry &entry = mapIt->second;

		SubList &subList = entry.second;
		subList.push_back(it);
		SubIterator subIt = subList.end();

		SubMap &subMap = entry.first;
		assert(subMap.find(&tree) == subMap.end());
		subMap.insert(std::make_pair(&tree, --subIt));
	}
}

void SQLCompiler::ReorderJoin::TreeSet::removeNodes(const Tree &tree) {
	for (Tree::NodeCursor cursor(tree); cursor.exists();) {
		const Id &nodeId = cursor.next();

		NodeMap::iterator mapIt = nodeMap_.find(nodeId);
		assert(mapIt != nodeMap_.end());

		SubEntry &entry = mapIt->second;

		SubMap &subMap = entry.first;
		SubMap::iterator subMapIt = subMap.find(&tree);
		assert(mapIt != nodeMap_.end());

		SubList &subList = entry.second;
		subList.erase(subMapIt->second);

		subMap.erase(subMapIt);
	}
}

SQLCompiler::ReorderJoin::TreeSet::Iterator
SQLCompiler::ReorderJoin::TreeSet::findByNode(const Id &nodeId) {
	NodeMap::iterator mapIt = nodeMap_.find(nodeId);
	if (mapIt != nodeMap_.end()) {
		SubEntry &entry = mapIt->second;
		SubList &subList = entry.second;
		if (!subList.empty()) {
			return subList.front();
		}
	}
	return end();
}

util::StackAllocator& SQLCompiler::ReorderJoin::TreeSet::getAllocator() {
	util::StackAllocator *alloc = baseList_.get_allocator().base();
	assert(alloc != NULL);
	return *alloc;
}


SQLCompiler::ReorderJoin::TreeSet::Entry::Entry(const Tree *tree) :
		tree_(*tree) {
	assert(tree != NULL);
}

const SQLCompiler::ReorderJoin::Tree&
SQLCompiler::ReorderJoin::TreeSet::Entry::get() const {
	return tree_;
}


SQLCompiler::ReorderJoin::Reporter::Reporter() :
		chain_(NULL),
		asChain_(false),
		tableCostAffected_(false),
		lastCriterion_(initialCriterion()),
		lastMinCostCriterion_(initialCriterion()) {
}

void SQLCompiler::ReorderJoin::Reporter::onInitial() {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onInitial();
	}
}

void SQLCompiler::ReorderJoin::Reporter::onFinal(
		util::Vector<JoinOperation> &destJoinOpList, bool reordered) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onFinal(destJoinOpList, reordered);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onInitialTreeSet(
		TreeSet &treeSet) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onInitialTreeSet(treeSet);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onHintHead(bool ordered) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onHintHead(ordered);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onMergeTarget(
		const Tree &key1, const Tree &key2, bool matched) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onMergeTarget(key1, key2, matched);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onCandidateHead() {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->initializeLastCriterion();
		chain->onCandidateHead();
	}
}

void SQLCompiler::ReorderJoin::Reporter::onCandidate(
		TreeSet &treeSet, TreeSet::Iterator edgeIt, TreeSet::Iterator edgeBegin,
		const Cost *cost) {
	if (treeSet.size() <= 2) {
		return;
	}

	if (edgeIt == edgeBegin) {
		onCandidateHead();
	}

	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onCandidate(treeSet, edgeIt, edgeBegin, cost);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onMinCost(
		TreeSet &treeSet, const Tree &tree1, const Tree &tree2,
		const Tree *edge) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onMinCost(treeSet, tree1, tree2, edge);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onMerged(const Tree &tree) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onMerged(tree);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onMergedByHint(const Tree &tree) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onMergedByHint(tree);
	}
}

void SQLCompiler::ReorderJoin::Reporter::onReversedCost(
		Tags::ComparisonType type) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->onReversedCost(type);
	}
}

void SQLCompiler::ReorderJoin::Reporter::clearLastCriterion() {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->clearLastCriterion();
	}

	if (asChain_) {
		lastCriterion_ = initialCriterion();
		return;
	}
}

void SQLCompiler::ReorderJoin::Reporter::setLastCriterion(
		Tags::ComparisonType type, bool reversed) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->setLastCriterion(type, reversed);
	}

	if (asChain_) {
		if (lastCriterion_ == initialCriterion()) {
			lastCriterion_ = Criterion(type, reversed);
		}
	}

	if (Tags::isTableCostAffected(type)) {
		tableCostAffected_ = true;
	}
}

void SQLCompiler::ReorderJoin::Reporter::setLastMinCostUpdated(
		TreeSet::Iterator edgeIt, TreeSet::Iterator edgeBegin, bool updated) {
	for (Reporter *chain = begin(); exists(chain); next(chain)) {
		chain->setLastMinCostUpdated(edgeIt, edgeBegin, updated);
	}

	if (asChain_) {
		if (edgeIt == edgeBegin) {
			return;
		}
		if (--edgeIt == edgeBegin || updated) {
			lastMinCostCriterion_ = lastCriterion_;
		}
	}
}

bool SQLCompiler::ReorderJoin::Reporter::isTableCostAffected() const {
	return tableCostAffected_;
}

void SQLCompiler::ReorderJoin::Reporter::addChain(
		Reporter &parent, Reporter &chain) {
	parent.addChainDetail(chain);
}

bool SQLCompiler::ReorderJoin::Reporter::findLastMinCostCriterion(
		Criterion &criterion) {
	assert(asChain_);
	criterion = lastMinCostCriterion_;
	return (lastMinCostCriterion_ != initialCriterion());
}

SQLCompiler::ReorderJoin::Reporter::Criterion
SQLCompiler::ReorderJoin::Reporter::initialCriterion() {
	return Criterion(Tags::COMP_END, false);
}

void SQLCompiler::ReorderJoin::Reporter::initializeLastCriterion() {
	lastCriterion_ = initialCriterion();
	lastMinCostCriterion_ = initialCriterion();
}

void SQLCompiler::ReorderJoin::Reporter::addChainDetail(Reporter &chain) {
	assert(!asChain_);

	assert(!exists(chain.chain_));
	assert(!chain.asChain_);

	Reporter **prev = &chain_;
	for (; exists(*prev); prev = &(*prev)->chain_) {
	}
	*prev = &chain;

	chain.asChain_ = true;
}

SQLCompiler::ReorderJoin::Reporter* SQLCompiler::ReorderJoin::Reporter::begin() {
	return (asChain_ ? NULL : chain_);
}

bool SQLCompiler::ReorderJoin::Reporter::exists(Reporter *chain) {
	return (chain != NULL);
}

void SQLCompiler::ReorderJoin::Reporter::next(Reporter *&chain) {
	assert(exists(chain));
	chain = chain->chain_;
}


SQLCompiler::ReorderJoin::ExplainReporter::ExplainReporter(
		Reporter &parent, util::StackAllocator &alloc) :
		alloc_(alloc),
		treeProfileMap_(alloc),
		ordinalProfileMap_(alloc),
		totalCandidates_(alloc),
		nextOrdinal_(0),
		topTreeProfie_(NULL),
		currentCandidates_(alloc),
		merging_(TreePair()),
		criterion_(initialCriterion()),
		hintAffected_(false) {
	addChain(parent, *this);
}

void SQLCompiler::ReorderJoin::ExplainReporter::tryCreate(
		util::LocalUniquePtr<ExplainReporter> &explainReporter,
		util::StackAllocator &alloc, bool enabled, Reporter &parent) {
	if (!enabled) {
		return;
	}

	explainReporter = UTIL_MAKE_LOCAL_UNIQUE(
			explainReporter, ExplainReporter, parent, alloc);
}

void SQLCompiler::ReorderJoin::ExplainReporter::onFinal(
		util::Vector<JoinOperation> &destJoinOpList, bool reordered) {
	if (!destJoinOpList.empty()) {
		destJoinOpList.back().profile_ = generateTotalProfile(reordered);
	}
}

void SQLCompiler::ReorderJoin::ExplainReporter::onInitialTreeSet(
		TreeSet &treeSet) {
	for (TreeSet::Iterator it = treeSet.begin(); it != treeSet.end(); ++it) {
		const Tree &tree = it->get();
		assert(tree.findNode() != NULL);
		addTreeProfile(tree);
	}
}

void SQLCompiler::ReorderJoin::ExplainReporter::onMergeTarget(
		const Tree &key1, const Tree &key2, bool matched) {
	if (!matched) {
		return;
	}
	setMerging(key1, key2);
}

void SQLCompiler::ReorderJoin::ExplainReporter::onCandidate(
		TreeSet &treeSet, TreeSet::Iterator edgeIt,
		TreeSet::Iterator edgeBegin, const Cost *cost) {
	static_cast<void>(treeSet);
	static_cast<void>(edgeBegin);

	addCandidate(
			findEdgeElementProfile(edgeIt->get(), true),
			findEdgeElementProfile(edgeIt->get(), false),
			makeCostProfile(cost));
}

void SQLCompiler::ReorderJoin::ExplainReporter::onMinCost(
		TreeSet &treeSet, const Tree &tree1, const Tree &tree2,
		const Tree *edge) {
	static_cast<void>(treeSet);
	static_cast<void>(edge);

	setBestCandidate(findTreeProfile(&tree1), findTreeProfile(&tree2));
	setMerging(tree1, tree2);

	findLastMinCostCriterion(criterion_);
}

void SQLCompiler::ReorderJoin::ExplainReporter::onMerged(const Tree &tree) {
	addTreeProfile(tree);
}

void SQLCompiler::ReorderJoin::ExplainReporter::onMergedByHint(
		const Tree &tree) {
	hintAffected_ = true;
	addTreeProfile(tree);
}

SQLCompiler::ReorderJoin::ExplainReporter::JoinProfile*
SQLCompiler::ReorderJoin::ExplainReporter::generateTotalProfile(
		bool reordered) {
	if (topTreeProfie_ == NULL) {
		return NULL;
	}

	JoinProfile &profile = makeProfile();

	profile.tree_ = topTreeProfie_;
	if (!totalCandidates_.empty()) {
		profile.candidates_ = &makeTreeProfileList(&totalCandidates_);
	}
	profile.reordered_ = reordered;
	profile.tableCostAffected_ = isTableCostAffected();

	return &profile;
}

void SQLCompiler::ReorderJoin::ExplainReporter::addTreeProfile(
		const Tree &tree) {
	assert(tree.findNode() == NULL || findNodeOrdinal(tree) == nextOrdinal_);

	TreeProfile &profile = makeTreeProfile(NULL);

	profile.ordinal_ = nextOrdinal_;
	if (merging_ != TreePair()) {
		profile.left_ = findTreeProfile(merging_.first);
		profile.right_ = findTreeProfile(merging_.second);
	}

	profile.criterion_ = (hintAffected_ ?
			SQLPreparedPlan::Constants::STR_HINT :
			Tags::comparisonTypeToSymbol(criterion_.first));

	profile.cost_ = makeCostProfile(&tree.getCost());
	addCandidateProfile(profile.ordinal_);

	treeProfileMap_.insert(std::make_pair(&tree, &profile));
	ordinalProfileMap_.insert(std::make_pair(profile.ordinal_, &profile));

	nextOrdinal_++;
	topTreeProfie_ = &profile;

	clearCurrentElements();
}

void SQLCompiler::ReorderJoin::ExplainReporter::addCandidateProfile(
		JoinOrdinal ordinal) {
	if (currentCandidates_.size() <= 1) {
		return;
	}

	TreeProfile &best = makeTreeProfile(&currentCandidates_.back());
	TreeProfileList &other = makeTreeProfileList(&currentCandidates_);
	other.pop_back();

	totalCandidates_.push_back(TreeProfile());
	TreeProfile &profile = totalCandidates_.back();

	profile.ordinal_ = ordinal;
	profile.best_ = &best;
	profile.other_ = &other;
}

void SQLCompiler::ReorderJoin::ExplainReporter::clearCurrentElements() {
	currentCandidates_.clear();
	merging_ = TreePair();
	criterion_ = initialCriterion();
	hintAffected_ = false;
}

void SQLCompiler::ReorderJoin::ExplainReporter::setMerging(
		const Tree &tree1, const Tree &tree2) {
	merging_ = TreePair(&tree1, &tree2);
}

void SQLCompiler::ReorderJoin::ExplainReporter::addCandidate(
		const TreeProfile *tree1, const TreeProfile *tree2,
		const CostProfile *cost) {
	JoinOrdinalList *ordinals = makeJoinOrdinalList(
			findTreeOrdinal(tree1), findTreeOrdinal(tree2));

	if (ordinals == NULL) {
		assert(false);
		return;
	}

	currentCandidates_.push_back(TreeProfile());
	TreeProfile &profile = currentCandidates_.back();

	profile.target_ = ordinals;
	profile.cost_ = makeCostProfile(cost);
}

void SQLCompiler::ReorderJoin::ExplainReporter::setBestCandidate(
		const TreeProfile *tree1, const TreeProfile *tree2) {
	JoinOrdinalList *ordinals = makeJoinOrdinalList(
			findTreeOrdinal(tree1), findTreeOrdinal(tree2));

	if (ordinals == NULL) {
		assert(false);
		return;
	}

	for (TreeProfileList::iterator it = currentCandidates_.begin();
			it != currentCandidates_.end(); ++it) {
		JoinOrdinalList *target = it->target_;
		if (target != NULL &&
				matchJoinOrdinalList(*target, *ordinals)) {
			TreeProfile best = *it;
			currentCandidates_.erase(it);
			currentCandidates_.push_back(best);
			break;
		}
	}
}

bool SQLCompiler::ReorderJoin::ExplainReporter::matchJoinOrdinalList(
		const JoinOrdinalList &ordinals1, const JoinOrdinalList &ordinals2) {
	if (ordinals1.size() != 2) {
		assert(false);
		return false;
	}
	else if (ordinals1 == ordinals2) {
		return true;
	}
	else if (ordinals1[0] != ordinals2[1]) {
		return false;
	}
	else if (ordinals1[1] != ordinals2[0]) {
		return false;
	}
	return true;
}

SQLCompiler::ReorderJoin::ExplainReporter::TreeProfile*
SQLCompiler::ReorderJoin::ExplainReporter::findEdgeElementProfile(
		const Tree &edge, bool first) {
	const JoinOrdinal ordinal = findNodeOrdinal(edge.edgeElement(first));
	if (ordinal < 0) {
		return NULL;
	}

	return findTreeProfile(ordinal);
}

SQLCompiler::ReorderJoin::ExplainReporter::JoinOrdinal
SQLCompiler::ReorderJoin::ExplainReporter::findTreeOrdinal(
		const TreeProfile *profile) {
	if (profile == NULL) {
		return -1;
	}

	return profile->ordinal_;
}

SQLCompiler::ReorderJoin::ExplainReporter::JoinOrdinal
SQLCompiler::ReorderJoin::ExplainReporter::findNodeOrdinal(const Tree &tree) {
	const Id *id = tree.findNode();
	if (id == NULL) {
		return -1;
	}

	return static_cast<JoinOrdinal>(id->getBase());
}

SQLCompiler::ReorderJoin::ExplainReporter::TreeProfile*
SQLCompiler::ReorderJoin::ExplainReporter::findTreeProfile(JoinOrdinal ordinal) {
	do {
		OrdinalProfileMap::iterator it = ordinalProfileMap_.find(ordinal);
		if (it == ordinalProfileMap_.end()) {
			assert(false);
			break;
		}
		return it->second;
	}
	while (false);
	return NULL;
}

SQLCompiler::ReorderJoin::ExplainReporter::TreeProfile*
SQLCompiler::ReorderJoin::ExplainReporter::findTreeProfile(const Tree *tree) {
	do {
		if (tree == NULL) {
			break;
		}

		TreeProfileMap::iterator it = treeProfileMap_.find(tree);
		if (it == treeProfileMap_.end()) {
			assert(false);
			break;
		}
		return it->second;
	}
	while (false);
	return NULL;
}

SQLCompiler::ReorderJoin::ExplainReporter::JoinProfile&
SQLCompiler::ReorderJoin::ExplainReporter::makeProfile() {
	JoinProfile *profile = ALLOC_NEW(alloc_) JoinProfile();
	return *profile;
}

SQLCompiler::ReorderJoin::ExplainReporter::TreeProfile&
SQLCompiler::ReorderJoin::ExplainReporter::makeTreeProfile(
		const TreeProfile *src) {
	TreeProfile *dest = ALLOC_NEW(alloc_) TreeProfile();
	if (src != NULL) {
		*dest = *src;
	}
	return *dest;
}

SQLCompiler::ReorderJoin::ExplainReporter::TreeProfileList&
SQLCompiler::ReorderJoin::ExplainReporter::makeTreeProfileList(
		const TreeProfileList *src) {
	TreeProfileList *dest = ALLOC_NEW(alloc_) TreeProfileList(alloc_);
	if (src != NULL) {
		dest->assign(src->begin(), src->end());
	}
	return *dest;
}

SQLCompiler::ReorderJoin::ExplainReporter::CostProfile*
SQLCompiler::ReorderJoin::ExplainReporter::makeCostProfile(
		const Cost *cost) {
	if (cost == NULL) {
		return NULL;
	}

	CostProfile *profile = ALLOC_NEW(alloc_) CostProfile();
	cost->getProfile(*profile);
	return profile;
}

SQLCompiler::ReorderJoin::ExplainReporter::CostProfile*
SQLCompiler::ReorderJoin::ExplainReporter::makeCostProfile(
		const CostProfile *src) {
	if (src == NULL) {
		return NULL;
	}

	CostProfile *dest = ALLOC_NEW(alloc_) CostProfile();
	*dest = *src;
	return dest;
}

SQLCompiler::ReorderJoin::ExplainReporter::JoinOrdinalList*
SQLCompiler::ReorderJoin::ExplainReporter::makeJoinOrdinalList(
		JoinOrdinal ordinal1, JoinOrdinal ordinal2) {
	if (ordinal1 < 0 || ordinal2 < 0) {
		return NULL;
	}

	JoinOrdinalList *list = ALLOC_NEW(alloc_) JoinOrdinalList(alloc_);
	list->push_back(ordinal1);
	list->push_back(ordinal2);
	return list;
}


SQLCompiler::ReorderJoin::TraceReporter::TraceReporter(
		Reporter &parent, std::ostream *os, std::ostream *resultOs) :
		os_((os == NULL ? emptyOs_ : *os)),
		resultOs_((resultOs == NULL ? emptyOs_ : *resultOs)) {
	addChain(parent, *this);
}

void SQLCompiler::ReorderJoin::TraceReporter::tryCreate(
		util::LocalUniquePtr<TraceReporter> &traceReporter,
		Reporter &parent) {
	std::ostream *os = NULL;
	std::ostream *resultOs = NULL;

	if (os == NULL && resultOs == NULL) {
		return;
	}

	traceReporter = UTIL_MAKE_LOCAL_UNIQUE(
			traceReporter, TraceReporter, parent, os, resultOs);
}

void SQLCompiler::ReorderJoin::TraceReporter::onInitial() {
	os_ << "ReorderJoin: started" << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onFinal(
		util::Vector<JoinOperation> &destJoinOpList, bool reordered) {
	dumpResult(resultOs_, destJoinOpList);

	os_ << "ReorderJoin: finished (";
	os_ << (reordered ? "" : "not ") << "reordered)" << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onInitialTreeSet(
		TreeSet &treeSet) {
	os_ << "  Initial:" << std::endl;
	for (TreeSet::Iterator it = treeSet.begin(); it != treeSet.end(); ++it) {
		os_ << "    ";
		it->get().dump(os_);
		os_ << std::endl;
	}
}

void SQLCompiler::ReorderJoin::TraceReporter::onHintHead(bool ordered) {
	os_ << "  Hint: " << (ordered ? "ordered" : "unordered") << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onMergeTarget(
		const Tree &key1, const Tree &key2, bool matched) {
	os_ << "    " << (matched ? "Matched" : "Unmatched");

	os_ << ": ";
	key1.dumpNodes(os_);
	os_ << ", ";
	key2.dumpNodes(os_);
	os_ << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onCandidateHead() {
	os_ << "  Candidates:" << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onCandidate(
		TreeSet &treeSet, TreeSet::Iterator edgeIt,
		TreeSet::Iterator edgeBegin, const Cost *cost) {
	static_cast<void>(treeSet);
	static_cast<void>(edgeBegin);

	os_ << "    ";
	edgeIt->get().dump(os_);
	os_ << " -> {";
	if (cost != NULL) {
		cost->dump(os_);
	}
	os_ << "}" << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onMinCost(
		TreeSet &treeSet, const Tree &tree1, const Tree &tree2,
		const Tree *edge) {
	if (treeSet.size() <= 2) {
		return;
	}

	os_ << "  Found: ";

	if (edge == NULL) {
		os_ << "(cross)";
	}
	else {
		edge->dumpNodes(os_);
	}

	os_ << " ";
	tree1.dumpNodes(os_);
	os_ << ", ";
	tree2.dumpNodes(os_);

	Criterion criterion;
	if (findLastMinCostCriterion(criterion)) {
		os_ << " ";
		dumpCriterion(os_, criterion);
	}

	os_ << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onMerged(const Tree &tree) {
	os_ << "  Merged: ";
	tree.dump(os_);
	os_ << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onMergedByHint(
		const Tree &tree) {
	os_ << "    Merged by hint: ";
	tree.dump(os_);
	os_ << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::onReversedCost(
		Tags::ComparisonType type) {
	os_ << "  ReversedCost: ";
	Tags::dumpComparisonType(os_, type);
	os_ << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::dumpResult(
		std::ostream &os,
		const util::Vector<JoinOperation> &destJoinOpList) {
	os << "Result: destJoinOpList: start" << std::endl;
	for (util::Vector<JoinOperation>::const_iterator it = destJoinOpList.begin();
			it != destJoinOpList.end(); ++it) {
		it->dump(os);
		os << std::endl;
	}
	os << "Result: destJoinOpList: end" << std::endl;
}

void SQLCompiler::ReorderJoin::TraceReporter::dumpCriterion(
		std::ostream &os, const Criterion &criterion) {
	Tags::dumpComparisonType(os, criterion.first);

	if (criterion.second) {
		os << "(reversed)";
	}
}


SQLCompiler::ReorderJoin::SubOptimizer::SubOptimizer(
		util::StackAllocator &alloc) :
		alloc_(alloc),
		costComparator_(&reporter_) {
}

bool SQLCompiler::ReorderJoin::SubOptimizer::optimize(
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinEdge> &joinEdgeList,
		const util::Vector<JoinOperation> &srcJoinOpList,
		const ParsedHintList &joinHintList, bool profiling,
		util::Vector<JoinOperation> &destJoinOpList) {
	setUpReporter(profiling);
	reporter_.onInitial();

	TreeSet treeSet(alloc_);
	TreeSet edgeSet(alloc_);

	makeNodeSet(joinNodeList, treeSet);
	makeEdgeSet(joinEdgeList, treeSet, edgeSet);
	applyHints(joinHintList, treeSet);

	{
		Cost cost;
		TreeSet::Iterator edgeIt;
		TreeSet::Iterator it1;
		TreeSet::Iterator it2;
		while (findMinCostTrees(treeSet, edgeSet, cost, edgeIt, it1, it2)) {
			const Tree &ret = treeSet.merge(it1, it2, cost)->get();
			edgeSet.eraseOptional(edgeIt);
			reporter_.onMerged(ret);
		}
	}

	treeSet.exportOperations(destJoinOpList, reporter_.isTableCostAffected());

	const bool reordered = checkReordered(srcJoinOpList, destJoinOpList);
	reporter_.onFinal(destJoinOpList, reordered);
	return reordered;
}

bool SQLCompiler::ReorderJoin::SubOptimizer::findMinCostTrees(
		TreeSet &treeSet, TreeSet &edgeSet, Cost &edgeCost,
		TreeSet::Iterator &edgeIt, TreeSet::Iterator &it1,
		TreeSet::Iterator &it2) {
	edgeIt = edgeSet.end();
	edgeCost = Cost();
	it1 = treeSet.end();
	it2 = treeSet.end();

	Cost minCost;
	const TreeSet::Iterator edgeBegin = edgeSet.begin();
	const TreeSet::Iterator edgeEnd = edgeSet.end();
	for (TreeSet::Iterator it = edgeBegin; it != edgeEnd; ++it) {
		TreeSet::Iterator matchIt1 =
				treeSet.matchNodes(it->get().edgeElement(true));
		TreeSet::Iterator matchIt2 =
				treeSet.matchNodes(it->get().edgeElement(false));
		if (matchIt1 == treeSet.end() || matchIt2 == treeSet.end() ||
				matchIt1 == matchIt2) {
			reporter_.onCandidate(treeSet, it, edgeBegin, NULL);
			continue;
		}

		const bool forOp= true;
		const Cost cost = matchIt1->get().mergeCost(
				matchIt2->get(), it->get().getCost(), forOp);
		reporter_.onCandidate(treeSet, it, edgeBegin, &cost);

		const bool updated = (costComparator_.compare(cost, minCost) < 0);
		if (updated) {
			minCost = cost;
			edgeCost = it->get().getCost();
			edgeIt = it;
			it1 = matchIt1;
			it2 = matchIt2;
		}
		reporter_.setLastMinCostUpdated(it, edgeBegin, updated);
	}

	if (edgeIt != edgeSet.end()) {
		reporter_.onMinCost(treeSet, it1->get(), it2->get(), &edgeIt->get());
		return true;
	}
	else if (treeSet.size() > 1) {
		edgeCost = Cost::createCrossEdge();
		it1 = treeSet.begin();
		it2 = it1;
		++it2;
		reporter_.onMinCost(treeSet, it1->get(), it2->get(), NULL);
		return true;
	}
	return false;
}

bool SQLCompiler::ReorderJoin::SubOptimizer::findTargetedTrees(
		TreeSet &treeSet, const Tree &key1, const Tree &key2,
		TreeSet::Iterator &it1, TreeSet::Iterator &it2) {
	it1 = treeSet.end();
	it2 = treeSet.end();

	TreeSet::Iterator matchIt1 = treeSet.matchNodes(key1);
	TreeSet::Iterator matchIt2 = treeSet.matchNodes(key2);
	if (matchIt1 == treeSet.end() || matchIt2 == treeSet.end() ||
			matchIt1 == matchIt2 ||
			!matchIt1->get().isSameSize(key1) ||
			!matchIt2->get().isSameSize(key2)) {
		reporter_.onMergeTarget(key1, key2, false);
		return false;
	}

	it1 = matchIt1;
	it2 = matchIt2;
	reporter_.onMergeTarget(key1, key2, true);
	return true;
}

void SQLCompiler::ReorderJoin::SubOptimizer::applyHints(
		const ParsedHintList &joinHintList, TreeSet &treeSet) {
	for (ParsedHintList::const_iterator it = joinHintList.begin();
			it != joinHintList.end(); ++it) {
		if (it->hintId_ == SQLHint::OPTIMIZER_FAILURE_POINT) {
			continue;
		}

		reporter_.onHintHead(it->isTree_);
		if (it->isTree_) {
			applyOrderedHint(it->hintTableIdList_, treeSet);
		}
		else {
			applyUnorderedHint(it->hintTableIdList_, treeSet);
		}
	}

	costComparator_.applyHints(joinHintList);
}

void SQLCompiler::ReorderJoin::SubOptimizer::applyOrderedHint(
		const HintTableIdList &idList, TreeSet &treeSet) {
	typedef util::Map<Id, Tree*> HintTreeMap;
	HintTreeMap hintTreeMap(alloc_);

	util::Deque<Id> idStack(alloc_);
	JoinNodeId nextBaseGroupId = 0;

	for (HintTableIdList::const_iterator it = idList.begin();; ++it) {
		if (it != idList.end() && *it != UNDEF_PLAN_NODEID) {
			const Id id(*it, false);
			idStack.push_back(id);
			hintTreeMap.insert(std::make_pair(id, &makeNode(Cost(), id)));
			continue;
		}
		assert(idStack.size() > 1);

		HintTreeMap::iterator hintIt1 = hintTreeMap.find(idStack.back());
		idStack.pop_back();

		HintTreeMap::iterator hintIt2 = hintTreeMap.find(idStack.back());
		idStack.pop_back();

		TreeSet::Iterator it1;
		TreeSet::Iterator it2;
		if (!findTargetedTrees(
				treeSet, *hintIt1->second, *hintIt2->second, it1, it2)) {
			break;
		}

		const Cost &edgeCost = Cost::createNaturalEdge();
		const Tree &ret = treeSet.merge(it1, it2, edgeCost)->get();
		reporter_.onMergedByHint(ret);

		{
			const Id id(nextBaseGroupId, true);
			nextBaseGroupId++;

			idStack.push_back(id);
			Tree *tree = hintIt1->second;
			tree->merge(*hintIt2->second);

			hintTreeMap.insert(std::make_pair(id, tree));
			hintTreeMap.erase(hintIt1);
			hintTreeMap.erase(hintIt2);
			if (it != idList.end()) {
				continue;
			}
		}
		if (it == idList.end()) {
			assert(idStack.size() == 1);
			break;
		}
	}
}

void SQLCompiler::ReorderJoin::SubOptimizer::applyUnorderedHint(
		const HintTableIdList &idList, TreeSet &treeSet) {
	Tree lastHintTree(alloc_, Cost());
	for (HintTableIdList::const_iterator it = idList.begin();
			it != idList.end(); ++it) {
		Tree hintNode(alloc_, Cost());
		hintNode.setNode(Id(*it, false));
		if (it == idList.begin()) {
			lastHintTree.merge(hintNode);
			continue;
		}

		TreeSet::Iterator it1;
		TreeSet::Iterator it2;
		if (!findTargetedTrees(treeSet, lastHintTree, hintNode, it1, it2)) {
			break;
		}

		const Cost &edgeCost = Cost::createNaturalEdge();
		const Tree &ret = treeSet.merge(it1, it2, edgeCost)->get();
		reporter_.onMergedByHint(ret);

		lastHintTree.merge(hintNode);
	}
}

void SQLCompiler::ReorderJoin::SubOptimizer::makeNodeSet(
		const util::Vector<JoinNode> &joinNodeList, TreeSet &dest) {
	const uint64_t defaultTableCost = resolveDefaultTableCost(joinNodeList);

	for (util::Vector<JoinNode>::const_iterator it = joinNodeList.begin();
			it != joinNodeList.end(); ++it) {
		const Cost cost = Cost::create(*it, defaultTableCost);
		const Id id(static_cast<JoinNodeId>(it - joinNodeList.begin()), false);

		dest.insert(&makeNode(cost, id));
	}
}

void SQLCompiler::ReorderJoin::SubOptimizer::makeEdgeSet(
		const util::Vector<JoinEdge> &joinEdgeList, TreeSet &treeSet,
		TreeSet &dest) {
	typedef std::pair<Id, Id> Key;
	typedef util::Map<Key, Cost> CostMap;
	CostMap costMap(alloc_);

	for (util::Vector<JoinEdge>::const_iterator it = joinEdgeList.begin();
			it != joinEdgeList.end(); ++it) {
		const Cost cost = Cost::create(*it);
		Id id1(it->nodeIdList_[0], false);
		Id id2 = (it->nodeIdList_[1] == UNDEF_JOIN_NODEID ?
				Id(0, true) : Id(it->nodeIdList_[1], false));
		if (id2 < id1) {
			std::swap(id1, id2);
			assert(!id1.isGrouping());
		}
		const std::pair<CostMap::iterator, bool> ret =
				costMap.insert(std::make_pair(Key(id1, id2), cost));
		if (!ret.second) {
			ret.first->second.merge(cost, false);
		}
	}

	for (CostMap::iterator it = costMap.begin(); it != costMap.end(); ++it) {
		const Key &key = it->first;
		const Cost &cost = it->second;
		if (!key.second.isGrouping()) {
			dest.insert(&makeEdge(cost, key.first, key.second));
			continue;
		}

		Tree &tree = makeNode(cost, key.first);
		TreeSet::Iterator nodeIt = treeSet.matchNodes(tree);
		assert(nodeIt != treeSet.end());
		treeSet.mergeCost(nodeIt, cost);
	}
	reporter_.onInitialTreeSet(treeSet);
}

uint64_t SQLCompiler::ReorderJoin::SubOptimizer::resolveDefaultTableCost(
		const util::Vector<JoinNode> &joinNodeList) {
	int64_t maxApproxSize = -1;
	for (util::Vector<JoinNode>::const_iterator it = joinNodeList.begin();
			it != joinNodeList.end(); ++it) {
		maxApproxSize = std::max(maxApproxSize, it->approxSize_);
	}

	if (maxApproxSize < 0) {
		return JoinScore::DEFAULT_TABLE_ROW_APPROX_SIZE;
	}
	return static_cast<uint64_t>(maxApproxSize);
}

bool SQLCompiler::ReorderJoin::SubOptimizer::checkReordered(
		const util::Vector<JoinOperation> &src,
		const util::Vector<JoinOperation> &dest) {
	assert(src.size() == dest.size());

	util::Vector<JoinOperation>::const_iterator srcIt = src.begin();
	util::Vector<JoinOperation>::const_iterator destIt = dest.begin();
	for (; srcIt != src.end() && destIt != dest.end(); ++srcIt, ++destIt) {
		for (size_t i = 0; i != 2; i++) {
			if (srcIt->nodeIdList_[i] != destIt->nodeIdList_[i]) {
				return true;
			}
		}
	}
	return false;
}

SQLCompiler::ReorderJoin::Tree&
SQLCompiler::ReorderJoin::SubOptimizer::makeNode(
		const Cost &cost, const Id &id) {
	Tree &node = makeTree(cost);
	node.setNode(id);
	return node;
}

SQLCompiler::ReorderJoin::Tree&
SQLCompiler::ReorderJoin::SubOptimizer::makeEdge(
		const Cost &cost, const Id &id1, const Id &id2) {
	Tree &edge = makeTree(cost);
	{
		Tree node(alloc_, Cost());
		node.setNode(id1);
		edge.merge(node);
	}
	{
		Tree node(alloc_, Cost());
		node.setNode(id2);
		edge.merge(node);
	}
	return edge;
}

SQLCompiler::ReorderJoin::Tree&
SQLCompiler::ReorderJoin::SubOptimizer::makeTree(const Cost &cost) {
	return *(ALLOC_NEW(alloc_) Tree(alloc_, cost));
}

void SQLCompiler::ReorderJoin::SubOptimizer::setUpReporter(bool profiling) {
	ExplainReporter::tryCreate(explainReporter_, alloc_, profiling, reporter_);
	TraceReporter::tryCreate(traceReporter_, reporter_);
}
