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
#include "sql_processor.h" 
#include "util/numeric.h"



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

bool SQLCompiler::selectJoinDriving(Plan &plan, bool checkOnly) {
	if (isLegacyJoinDriving(plan)) {
		return selectJoinDrivingLegacy(plan, checkOnly);
	}

	SelectJoinDriving::SubOptimizer optimizer(*this, checkOnly);
	return optimizer.optimize(plan);
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


SQLCompiler::SelectJoinDriving::NodeCursor::NodeCursor(
		Plan &plan, bool modifiable) :
		plan_(plan),
		modifiable_(modifiable),
		nextPos_(0),
		someMarked_(false),
		markedAfterRestart_(false) {
}

SQLCompiler::PlanNode* SQLCompiler::SelectJoinDriving::NodeCursor::next() {
	if (nextPos_ >= plan_.nodeList_.size() && !tryRestart()) {
		return NULL;
	}

	PlanNode *node = &plan_.nodeList_[nextPos_];
	nextPos_++;
	return node;
}

void SQLCompiler::SelectJoinDriving::NodeCursor::markCurrent() {
	someMarked_ = true;
	markedAfterRestart_ = true;
}

bool SQLCompiler::SelectJoinDriving::NodeCursor::isSomeMarked() {
	return someMarked_;
}

bool SQLCompiler::SelectJoinDriving::NodeCursor::tryRestart() {
	if (plan_.nodeList_.empty() || !modifiable_ || !markedAfterRestart_) {
		return false;
	}

	assert(nextPos_ > 0);
	nextPos_ = 0;
	markedAfterRestart_ = false;
	return true;
}


SQLCompiler::SelectJoinDriving::Cost::Cost() :
		intValue_(-1),
		floatVaue_(-1),
		exatct_(true),
		factor_(FACTOR_NONE),
		indexed_(false),
		reducible_(false) {
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofMin() {
	return of(0, 0, true, FACTOR_HINT);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofMax() {
	return of(
			std::numeric_limits<int64_t>::max(),
			std::numeric_limits<double>::max(),
			false,
			FACTOR_HINT);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofExact(
		int64_t count, CostFactor factor) {
	return ofInt(count, true, factor);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofApprox(
		int64_t count, CostFactor factor) {
	return ofInt(count, false, factor);
}

bool SQLCompiler::SelectJoinDriving::Cost::isAssigned() const {
	return (intValue_ >= 0);
}

bool SQLCompiler::SelectJoinDriving::Cost::isIndexed() const {
	return indexed_;
}

bool SQLCompiler::SelectJoinDriving::Cost::isReducible() const {
	return reducible_;
}

SQLCompiler::SelectJoinDriving::CostFactor
SQLCompiler::SelectJoinDriving::Cost::getFactor() const {
	return factor_;
}

int32_t SQLCompiler::SelectJoinDriving::Cost::compareTo(
		const Cost &another) const {
	return approxCompareTo(another, 0);
}

int32_t SQLCompiler::SelectJoinDriving::Cost::approxCompareTo(
		const Cost &another, uint32_t rate) const {
	int32_t comp;
	if ((comp = compareBool(isAssigned(), another.isAssigned())) != 0) {
		const int32_t reversed = -1;
		return (comp * reversed);
	}
	else if ((comp = approxCompareFloat(
			floatVaue_, another.floatVaue_, rate)) != 0) {
		return comp;
	}
	else if ((comp = approxCompareInt(
			intValue_, another.intValue_, rate)) != 0) {
		return comp;
	}
	else if ((comp = compareBool(!exatct_, !another.exatct_)) != 0) {
		return comp;
	}
	return 0;
}

void SQLCompiler::SelectJoinDriving::Cost::mergeSum(const Cost &another) {
	if (tryMergeNonAssigned(another, true)) {
		return;
	}

	const bool exatctNext = resolveExactStatus(another, true);

	addInt(another);
	addFloat(another, intValue_);
	exatct_ = exatctNext;
	factor_ = mergeFactor(another, false);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofSumMerged(
		const Cost &cost1, const Cost &cost2) {
	Cost dest = cost1;
	dest.mergeSum(cost2);
	return dest;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofMinMerged(
		const Cost &cost1, const Cost &cost2) {
	Cost dest = cost1;
	dest.mergeMin(cost2);
	return dest;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofEqJoinMerged(
		const Cost &cost1, const Cost &cost2) {
	Cost dest = cost1;
	dest.mergeEqJoin(cost2);
	return dest;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofCrossMerged(
		const Cost &cost1, const Cost &cost2) {
	Cost dest = cost1;
	dest.mergeCross(cost2);
	return dest;
}

void SQLCompiler::SelectJoinDriving::Cost::mergeMin(const Cost &another) {
	if (tryMergeNonAssigned(another, false)) {
		return;
	}

	const bool exatctNext = resolveExactStatus(another, false);

	intValue_ = std::min(intValue_, another.intValue_);
	floatVaue_ = std::min(floatVaue_, another.floatVaue_);
	exatct_ = exatctNext;
	factor_ = mergeFactor(another, true);
}

void SQLCompiler::SelectJoinDriving::Cost::mergeEqJoin(const Cost &another) {
	if (tryMergeNonAssigned(another, true)) {
		return;
	}

	mergeSum(another);
	exatct_ = false;
}

void SQLCompiler::SelectJoinDriving::Cost::mergeCross(const Cost &another) {
	if (tryMergeNonAssigned(another, true)) {
		return;
	}

	Cost sum = *this;
	sum.mergeSum(another);

	const bool exatctNext = resolveExactStatus(another, true);

	multiplyInt(another);
	multiplyFloat(another, intValue_);
	exatct_ = exatctNext;
	factor_ = mergeFactor(another, false);

	if (compareTo(sum) < 0) {
		*this = sum;
	}
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::withAttributes(
		const bool *indexed, const bool *reducible) const {
	Cost dest = *this;
	if (indexed != NULL) {
		dest.indexed_ = *indexed;
	}
	if (reducible != NULL) {
		dest.reducible_ = *reducible;
	}
	return dest;
}

SQLPreparedPlan::OptProfile::JoinCost
SQLCompiler::SelectJoinDriving::Cost::toProfile() const {
	OptProfile::JoinCost dest;
	dest.approxSize_ = intValue_;
	return dest;
}

SQLPreparedPlan::Constants::StringConstant
SQLCompiler::SelectJoinDriving::Cost::getCriterion() const {
	switch (factor_) {
	case FACTOR_NONE:
		return Constants::STR_NONE;
	case FACTOR_TABLE:
		return Constants::STR_TABLE;
	case FACTOR_INDEX:
		return Constants::STR_INDEX;
	case FACTOR_SCHEMA:
		return Constants::STR_SCHEMA;
	case FACTOR_SYNTAX:
		return Constants::STR_SYNTAX;
	case FACTOR_HINT:
		return Constants::STR_HINT;
	default:
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
}

bool SQLCompiler::SelectJoinDriving::Cost::tryMergeNonAssigned(
		const Cost &another, bool unifying) {
	static_cast<void>(unifying);
	if (!another.isAssigned()) {
		return true;
	}
	else if (!isAssigned()) {
		*this = another;
		return true;
	}
	else {
		return false;
	}
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::Cost::ofInt(
		int64_t intValue, bool exatct, CostFactor factor) {
	const double floatVaue = static_cast<double>(intValue);
	return of(intValue, floatVaue, exatct, factor);
}

SQLCompiler::SelectJoinDriving::Cost SQLCompiler::SelectJoinDriving::Cost::of(
		int64_t intValue, double floatVaue, bool exatct, CostFactor factor) {
	Cost cost;
	cost.intValue_ = intValue;
	cost.floatVaue_ = floatVaue;
	cost.exatct_ = exatct;
	cost.factor_ = factor;
	return cost;
}

SQLCompiler::SelectJoinDriving::CostFactor
SQLCompiler::SelectJoinDriving::Cost::mergeFactor(
		const Cost &another, bool byMin) {
	const int32_t baseComp = compareTo(another);
	const int32_t comp = baseComp * (byMin ? 1 : -1);
	return (comp <= 0 ? *this : another).factor_;
}

int32_t SQLCompiler::SelectJoinDriving::Cost::approxCompareInt(
		int64_t v1, int64_t v2, uint32_t rate) {
	if (rate > 1) {
		const int64_t r = static_cast<int64_t>(rate);
		if (
				(v1 < v2 && v1 >= v2 / r) ||
				(v2 < v1 && v2 >= v1 / r)) {
			return 0;
		}
	}

	return compareInt(v1, v2);
}

int32_t SQLCompiler::SelectJoinDriving::Cost::approxCompareFloat(
		double v1, double v2, uint32_t rate) {
	if (rate > 1) {
		const double r = static_cast<double>(rate);
		if (
				(v1 < v2 && v1 >= v2 / r) ||
				(v2 < v1 && v2 >= v1 / r)) {
			return 0;
		}
	}

	return compareFloat(v1, v2);
}

int32_t SQLCompiler::SelectJoinDriving::Cost::compareBool(bool v1, bool v2) {
	return compareInt((v1 ? 1 : 0), (v2 ? 1 : 0));
}

int32_t SQLCompiler::SelectJoinDriving::Cost::compareInt(
		int64_t v1, int64_t v2) {
	if (v1 == v2) {
		return 0;
	}
	else {
		return (v1 < v2 ? -1 : 1);
	}
}

int32_t SQLCompiler::SelectJoinDriving::Cost::compareFloat(
		double v1, double v2) {
	return SQLProcessor::ValueUtils::orderValue(
			TupleValue(v1), TupleValue(v2), true);
}

void SQLCompiler::SelectJoinDriving::Cost::addInt(const Cost &another) {
	util::UtilityException::Code errorCode;
	util::ArithmeticErrorHandlers::Checked checked(errorCode);

	int64_t ret = util::NumberArithmetic::add(
			intValue_, another.intValue_, checked);
	if (checked.isError()) {
		ret = std::numeric_limits<int64_t>::max();
	}

	intValue_ = ret;
}

void SQLCompiler::SelectJoinDriving::Cost::addFloat(
		const Cost &another, int64_t intResult) {
	floatVaue_ += another.floatVaue_;
	floatVaue_ = std::max(floatVaue_, static_cast<double>(intResult));
}

void SQLCompiler::SelectJoinDriving::Cost::multiplyInt(const Cost &another) {
	util::UtilityException::Code errorCode;
	util::ArithmeticErrorHandlers::Checked checked(errorCode);

	int64_t ret = util::NumberArithmetic::multiply(
			intValue_, another.intValue_, checked);
	if (checked.isError()) {
		ret = std::numeric_limits<int64_t>::max();
	}

	intValue_ = ret;
}

void SQLCompiler::SelectJoinDriving::Cost::multiplyFloat(
		const Cost &another, int64_t intResult) {
	floatVaue_ *= another.floatVaue_;
	floatVaue_ = std::max(floatVaue_, static_cast<double>(intResult));
}

bool SQLCompiler::SelectJoinDriving::Cost::resolveExactStatus(
		const Cost &another, bool unifying) {
	if (unifying) {
		return (exatct_ || another.exatct_);
	}
	else {
		return (exatct_ && another.exatct_);
	}
}


SQLCompiler::SelectJoinDriving::CostResolver::CostResolver(
		SQLCompiler &compiler, TableSizeList &tableSizeList,
		bool withUnionScan, OptProfile::JoinDriving *profile) :
		alloc_(compiler.alloc_),
		compiler_(compiler),
		tableSizeList_(tableSizeList),
		withUnionScan_(withUnionScan),
		profile_(profile),
		inProfile_(NULL) {
}

bool SQLCompiler::SelectJoinDriving::CostResolver::isAcceptableNode(
		const PlanNode &node, bool checkOnly) {
	const size_t joinCount = JOIN_INPUT_COUNT;

	if (node.type_ != SQLType::EXEC_JOIN ||
			node.joinType_ != SQLType::JOIN_INNER ||
			node.inputList_.size() != joinCount) {
		return false;
	}

	if (checkOnly && (node.cmdOptionFlag_ & getJoinDrivingOptionMask()) != 0) {
		return false;
	}

	return true;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveCost(
		const Plan &plan, const PlanNode &node, uint32_t drivingInput) {
	prepareProfiler(plan, node, drivingInput);

	const uint32_t innerInput = getJoinAnotherSide(drivingInput);
	bool byUniqueEqJoin;
	const bool indexed = compiler_.findIndexedPredicate(
			plan, node, innerInput, withUnionScan_, &byUniqueEqJoin);

	bool forMax;
	if (findAggressiveHints(node, drivingInput, forMax)) {
		return finalizeFixedCost(
				indexed, !forMax, (forMax ? Cost::ofMax() : Cost::ofMin()));
	}

	bool byEq;
	bool byUnique;
	resolveJoinPredCategory(plan, node, byEq, byUnique);

	CostMap costMap(alloc_);

	const PlanNode &drivingNode = node.getInput(plan, drivingInput);
	const Cost &drivingCost = resolveOutputCost(plan, drivingNode, costMap);

	bool reducible = false;
	const PlanNode &innerNode = node.getInput(plan, innerInput);
	const Cost &scanCost = (indexed ?
			resolveInnerScanCost(
					plan, innerNode, costMap, drivingCost, byUnique,
					reducible) :
			resolveOutputCost(plan, drivingNode, costMap));

	return finalizeBasicCost(indexed, reducible, drivingCost, scanCost);
}

SQLPreparedPlan::OptProfile::JoinDriving*
SQLCompiler::SelectJoinDriving::CostResolver::toProfile(
		int32_t drivingInput) const {
	if (profile_ == NULL) {
		return NULL;
	}

	OptProfile::JoinDriving *dest =
			ALLOC_NEW(alloc_) OptProfile::JoinDriving(*profile_);
	if (drivingInput < 0) {
		std::swap(dest->driving_, dest->left_);
		std::swap(dest->inner_, dest->right_);
	}
	else if (drivingInput > 0) {
		std::swap(dest->driving_, dest->inner_);
	}

	return dest;
}

bool SQLCompiler::SelectJoinDriving::CostResolver::findAggressiveHints(
		const PlanNode &node, uint32_t drivingInput, bool &forMax) {
	forMax = false;

	if ((node.cmdOptionFlag_ & PlanNode::Config::CMD_OPT_JOIN_NO_INDEX) != 0) {
		forMax = true;
		return true;
	}

	const bool leadingList[] = {
			(node.cmdOptionFlag_ &
					PlanNode::Config::CMD_OPT_JOIN_LEADING_LEFT) != 0,
			(node.cmdOptionFlag_ &
					PlanNode::Config::CMD_OPT_JOIN_LEADING_RIGHT) != 0
	};

	const bool aggressiveList[] = { leadingList[0], leadingList[1] };

	const bool aggressiveSome = (aggressiveList[0] || aggressiveList[1]);
	if (!aggressiveSome) {
		return false;
	}

	forMax = !aggressiveList[drivingInput];
	return true;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveInnerScanCost(
		const Plan &plan, const PlanNode &node, CostMap &costMap,
		const Cost &drivingCost, bool byUniqueEqJoin,
		bool &reducible) const {
	const Cost innerTableCost =
			resolveScanOutputCost(plan, node, costMap, false);

	reducible = (drivingCost.approxCompareTo(
			innerTableCost, SubConstants::DEFAULT_REDUCIBLE_RATE) < 0);

	const bool byEq = true;
	return mergeJoinCost(
			drivingCost, innerTableCost, byEq, byUniqueEqJoin, node.joinType_);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveOutputCost(
		const Plan &plan, const PlanNode &node, CostMap &costMap) const {
	CostMap::iterator it = costMap.find(node.id_);
	if (it != costMap.end()) {
		return it->second;
	}

	Cost cost = resolveOutputCostByType(plan, node, costMap);

	if (node.limit_ >= 0) {
		const Cost costByLimit = Cost::ofExact(node.limit_, FACTOR_SYNTAX);
		cost.mergeMin(costByLimit);
	}

	costMap.insert(std::make_pair(node.id_, cost));
	return cost;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveOutputCostByType(
		const Plan &plan, const PlanNode &node, CostMap &costMap) const {
	switch (node.type_) {
	case SQLType::EXEC_GROUP:
		return resolveSimpleOutputCost(plan, node, costMap);
	case SQLType::EXEC_JOIN:
		return resolveJoinOutputCost(plan, node, costMap);
	case SQLType::EXEC_LIMIT:
		return resolveSimpleOutputCost(plan, node, costMap);
	case SQLType::EXEC_SCAN:
		return resolveScanOutputCost(plan, node, costMap, true);
	case SQLType::EXEC_SELECT:
		return resolveSimpleOutputCost(plan, node, costMap);
	case SQLType::EXEC_SORT:
		return resolveSimpleOutputCost(plan, node, costMap);
	case SQLType::EXEC_UNION:
		return resolveSimpleOutputCost(plan, node, costMap);
	default:
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveSimpleOutputCost(
		const Plan &plan, const PlanNode &node, CostMap &costMap) const {
	const uint32_t inCount = static_cast<uint32_t>(node.inputList_.size());

	if (inCount <= 0) {
		return Cost::ofExact(1, FACTOR_SYNTAX);
	}

	Cost totalCost;
	for (uint32_t i = 0; i < inCount; i++) {
		const Cost cost =
				resolveOutputCost(plan, node.getInput(plan, i), costMap);
		if (i == 0) {
			totalCost = cost;
			if (node.unionType_ == SQLType::UNION_EXCEPT) {
				break;
			}
			continue;
		}

		if (node.unionType_ == SQLType::UNION_INTERSECT) {
			totalCost.mergeMin(cost);
		}
		else {
			totalCost.mergeSum(cost);
		}
	}

	return totalCost;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveJoinOutputCost(
		const Plan &plan, const PlanNode &node, CostMap &costMap) const {
	assert(node.inputList_.size() == JOIN_INPUT_COUNT);
	bool byEq;
	bool byUnique;
	resolveJoinPredCategory(plan, node, byEq, byUnique);

	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	const Cost &leftCost =
			resolveOutputCost(plan, node.getInput(plan, left), costMap);
	const Cost &rightCost =
			resolveOutputCost(plan, node.getInput(plan, right), costMap);

	return mergeJoinCost(leftCost, rightCost, byEq, byUnique, node.joinType_);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveScanOutputCost(
		const Plan &plan, const PlanNode &node, CostMap &costMap,
		bool withPred) const {
	if (node.type_ != SQLType::EXEC_SCAN) {
		return resolveUnifiedScanOutputCost(plan, node, costMap, withPred);
	}

	Cost cost;

	const TableInfo *tableInfo = findTableInfo(node);
	if (tableInfo == NULL) {
		cost = Cost::ofApprox(
				SubConstants::DEFAULT_META_TABLE_SIZE, FACTOR_SYNTAX);
	}
	else {
		if (withPred && !node.predList_.empty()) {
			cost = resolveScanPredCost(
					plan, node, node.predList_.front(), *tableInfo, NULL);
		}

		if (!cost.isAssigned()) {
			cost = resolveScanTableCost(plan, node, *tableInfo);
		}

		saveAffectedTableSize(plan, node, *tableInfo, cost);
	}

	if (!node.inputList_.empty()) {
		cost = resolveScanJoinCost(plan, node, 0, cost, costMap);
	}

	return cost;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveUnifiedScanOutputCost(
		const Plan &plan, const PlanNode &node, CostMap &costMap,
		bool withPred) const {
	const uint32_t inCount = static_cast<uint32_t>(node.inputList_.size());

	if (node.type_ != SQLType::EXEC_UNION) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	Cost totalCost;
	for (uint32_t i = 0; i < inCount; i++) {
		const Cost cost = resolveScanOutputCost(
				plan, node.getInput(plan, i), costMap, withPred);
		totalCost.mergeSum(cost);
	}

	return totalCost;
}

void SQLCompiler::SelectJoinDriving::CostResolver::resolveJoinPredCategory(
		const Plan &plan, const PlanNode &node, bool &byEq,
		bool &byUnique) const {
	byEq = false;
	byUnique = false;

	std::pair<bool, bool> byFront;
	for (PlanExprList::const_iterator it = node.predList_.begin();
			it != node.predList_.end(); ++it) {
		bool byEqSub;
		std::pair<bool, bool> byFrontSub;
		resolveJoinPredCategorySub(*it, byEqSub, byFrontSub);

		byEq |= byEqSub;
		byFront.first |= byFrontSub.first;
		byFront.second |= byFrontSub.second;
	}

	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	byUnique = (
			(byFront.first && checkJoinInputUnique(plan, node, left)) &&
			(byFront.second && checkJoinInputUnique(plan, node, right)));
}

void SQLCompiler::SelectJoinDriving::CostResolver::resolveJoinPredCategorySub(
		const Expr &expr, bool &byEq, std::pair<bool, bool> &byFront) const {
	byEq = false;
	byFront = std::pair<bool, bool>();

	const Expr *left = expr.left_;
	const Expr *right = expr.right_;

	if (expr.op_ == SQLType::EXPR_AND) {
		for (size_t i = 0; i < 2; i++) {
			bool byEqSub;
			std::pair<bool, bool> byFrontSub;
			resolveJoinPredCategorySub(
					*(i == 0 ? left : right), byEqSub, byFrontSub);

			byEq |= byEqSub;
			byFront.first |= byFrontSub.first;
			byFront.second |= byFrontSub.second;
		}
	}
	else if (expr.op_ == SQLType::OP_EQ) {
		if (left->op_ != SQLType::EXPR_COLUMN ||
				right->op_ != SQLType::EXPR_COLUMN) {
			return;
		}

		if (left->inputId_ != 0) {
			std::swap(left, right);
		}

		if (left->inputId_ != 0 || right->inputId_ != 1) {
			return;
		}

		byEq = true;
		byFront = std::make_pair(
				(left->columnId_ == 0),
				(right->columnId_ == 0));
	}
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::mergeJoinCost(
		const Cost &left, const Cost &right, bool byEq, bool byUnique,
		SQLType::JoinType joinType) const {
	if (byEq) {
		Cost cost;
		if (byUnique) {
			cost = Cost::ofMinMerged(left, right);
		}
		else {
			cost = Cost::ofEqJoinMerged(left, right);;
		}

		if (joinType != SQLType::JOIN_INNER) {
			Cost outerCost;
			if (joinType == SQLType::JOIN_LEFT_OUTER) {
				outerCost = left;
			}
			else if (joinType == SQLType::JOIN_RIGHT_OUTER) {
				outerCost = right;
			}
			else {
				outerCost = Cost::ofSumMerged(left, right);
			}

			if (cost.compareTo(outerCost) < 0) {
				cost = outerCost;
			}
		}
		return cost;
	}
	else {
		return Cost::ofCrossMerged(left, right);
	}
}

bool SQLCompiler::SelectJoinDriving::CostResolver::checkJoinInputUnique(
		const Plan &plan, const PlanNode &node, uint32_t inputId) const {
	if (node.type_ == SQLType::EXEC_SCAN) {
		if (inputId == 0) {
			return checkFrontColumnUnique(plan, node);
		}
	}
	else if (node.type_ != SQLType::EXEC_JOIN) {
		return false;
	}

	return checkFrontColumnUnique(plan, node.getInput(plan, inputId));
}

bool SQLCompiler::SelectJoinDriving::CostResolver::checkFrontColumnUnique(
		const Plan &plan, const PlanNode &node) const {
	if (node.type_ == SQLType::EXEC_UNION) {
		const uint32_t inCount = static_cast<uint32_t>(node.inputList_.size());

		for (uint32_t i = 0; i < inCount; i++) {
			if (!checkFrontColumnUnique(plan, node.getInput(plan, i))) {
				return false;
			}
		}
		return true;
	}

	const TableInfo *tableInfo = findTableInfo(node);
	if (tableInfo == NULL) {
		return false;
	}

	return tableInfo->hasRowKey_;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveScanJoinCost(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		const Cost &baseCost, CostMap &costMap) const {
	const Cost drivingCost =
			resolveOutputCost(plan, node.getInput(plan, inputId), costMap);

	bool byEq;
	bool byUnique;
	resolveJoinPredCategory(plan, node, byEq, byUnique);

	const SQLType::JoinType joinType = SQLType::JOIN_INNER;
	return mergeJoinCost(drivingCost, baseCost, byEq, byUnique, joinType);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveScanTableCost(
		const Plan &plan, const PlanNode &node,
		const TableInfo &tableInfo) const {
	const bool withHint = true;

	CostFactor factor;
	const int64_t approxSize =
			resolveTableApproxSize(plan, node, tableInfo, withHint, factor);

	return Cost::ofApprox(approxSize, factor);
}

void SQLCompiler::SelectJoinDriving::CostResolver::saveAffectedTableSize(
		const Plan &plan, const PlanNode &node,
		const TableInfo &tableInfo, const Cost &cost) const {
	if (node.tableIdInfo_.approxSize_ >= 0 ||
			(node.id_ < tableSizeList_.size() &&
					tableSizeList_[node.id_] >= 0)) {
		return;
	}

	switch (cost.getFactor()) {
	case FACTOR_TABLE:
		break;
	case FACTOR_INDEX:
		break;
	default:
		return;
	}

	while (node.id_ >= tableSizeList_.size()) {
		tableSizeList_.push_back(-1);
	}

	const bool withHint = false;

	CostFactor factor;
	tableSizeList_[node.id_] =
			resolveTableApproxSize(plan, node, tableInfo, withHint, factor);
}

int64_t SQLCompiler::SelectJoinDriving::CostResolver::resolveTableApproxSize(
		const Plan &plan, const PlanNode &node,
		const TableInfo &tableInfo, bool withHint, CostFactor &factor) {
	factor = FACTOR_TABLE;

	int64_t approxSize;
	const int32_t subId = node.tableIdInfo_.subContainerId_;
	if (subId >= 0) {
		const TableInfo::PartitioningInfo *partInfo = tableInfo.partitioning_;
		assert(partInfo != NULL);

		approxSize = partInfo->subInfoList_[subId].approxSize_;
	}
	else {
		approxSize = tableInfo.idInfo_.approxSize_;
		do {
			if (!withHint) {
				break;
			}

			const SQLHintInfo *hintInfo = plan.hintInfo_;
			if (hintInfo == NULL) {
				break;
			}

			const char8_t *tableName = tableInfo.tableName_.c_str();
			const SQLHintValue *value = hintInfo->findStatisticalHint(
					SQLHint::TABLE_ROW_COUNT, tableName);
			if (value == NULL) {
				break;
			}

			approxSize = value->getInt64();
			factor = FACTOR_HINT;
		}
		while (false);
	}

	return adjustTableApproxSize(approxSize);
}

int64_t SQLCompiler::SelectJoinDriving::CostResolver::adjustTableApproxSize(
		int64_t rawSize) {
	if (rawSize < 0) {
		return 0;
	}

	return rawSize;
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveScanPredCost(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const TableInfo &tableInfo, ScanRangeMap *rangeMap) const {
	if (expr.op_ == SQLType::EXPR_OR) {
		uint64_t elemCount = 0;
		return resolveScanOrPredCost(plan, node, expr, tableInfo, elemCount);
	}
	else if (rangeMap == NULL) {
		ScanRangeMap subMap(alloc_);
		const Cost &predCost =
				resolveScanPredCost(plan, node, expr, tableInfo, &subMap);
		const Cost &rangeCost = resolveScanRangeMapCost(tableInfo, subMap);

		return Cost::ofMinMerged(predCost, rangeCost);
	}
	else if (expr.op_ == SQLType::EXPR_AND) {
		const Expr *left = expr.left_;
		const Expr *right = expr.right_;
		assert(left != NULL && right != NULL);

		const Cost &leftCost = resolveScanPredCost(
				plan, node, *left, tableInfo, rangeMap);
		const Cost &rightCost = resolveScanPredCost(
				plan, node, *right, tableInfo, rangeMap);

		return Cost::ofMinMerged(leftCost, rightCost);
	}
	else {
		util::LocalUniquePtr<ScanRangeKey> key;
		if (makeScanRangeKey(plan, node, expr, tableInfo, key)) {
			const std::pair<ScanRangeMap::iterator, bool> &ret =
					rangeMap->insert(std::make_pair(key->column_, *key));
			if (!ret.second) {
				mergeRangeKey(ret.first->second, *key);
			}
		}
		return Cost();
	}
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveScanOrPredCost(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const TableInfo &tableInfo, uint64_t &elemCount) const {
	if (expr.op_ != SQLType::EXPR_OR) {
		if (++elemCount > SubConstants::DEFAULT_CONDITION_LIMIT) {
			return Cost();
		}
		return resolveScanPredCost(plan, node, expr, tableInfo, NULL);
	}

	const Expr *left = expr.left_;
	const Expr *right = expr.right_;
	assert(left != NULL && right != NULL);

	const Cost &leftCost =
			resolveScanOrPredCost(plan, node, *left, tableInfo, elemCount);
	const Cost &rightCost =
			resolveScanOrPredCost(plan, node, *right, tableInfo, elemCount);
	if (!leftCost.isAssigned() || !rightCost.isAssigned()) {
		return Cost();
	}

	return Cost::ofSumMerged(leftCost, rightCost);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::resolveScanRangeMapCost(
		const TableInfo &tableInfo, const ScanRangeMap &rangeMap) const {
	const TableInfo::BasicIndexInfoList *basicInfo =
			tableInfo.basicIndexInfoList_;
	SQLIndexStatsCache &indexStats =
			compiler_.getTableInfoList().getIndexStats();

	Cost totalCost;
	for (ScanRangeMap::const_iterator it = rangeMap.begin();
			it != rangeMap.end(); ++it) {
		if (basicInfo == NULL || !basicInfo->isIndexed(it->first)) {
			continue;
		}

		SQLIndexStatsCache::Value statsValue;
		if (indexStats.find(it->second, statsValue)) {
			totalCost.mergeMin(
					Cost::ofApprox(statsValue.approxSize_, FACTOR_INDEX));
		}
		else {
			SQLIndexStatsCache::RequesterHolder *requester =
					compiler_.getCompileOption().getIndexStatsRequester();
			if (requester == NULL) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			if (requester->get() == NULL) {
				requester->assign(indexStats);
			}

			indexStats.putMissed(it->second, *requester->get());
		}
	}
	return totalCost;
}

bool SQLCompiler::SelectJoinDriving::CostResolver::makeScanRangeKey(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const TableInfo &tableInfo,
		util::LocalUniquePtr<ScanRangeKey> &key) const {
	const Plan::ValueList *parameterList = &plan.parameterList_;
	if (parameterList->empty()) {
		parameterList = NULL;
	}

	uint32_t columnId;
	std::pair<TupleValue, TupleValue> valuePair;
	std::pair<bool, bool> inclusivePair;

	if (!ProcessorUtils::findAdjustedRangeValues(
			alloc_, tableInfo, parameterList, expr, columnId, valuePair,
			inclusivePair)) {
		return false;
	}

	ScanRangeKey localKey(
			node.tableIdInfo_.partitionId_,
			node.tableIdInfo_.containerId_,
			columnId,
			valuePair.first, valuePair.second,
			inclusivePair.first, inclusivePair.second);
	key = UTIL_MAKE_LOCAL_UNIQUE(key, ScanRangeKey, localKey);
	return true;
}

void SQLCompiler::SelectJoinDriving::CostResolver::mergeRangeKey(
		ScanRangeKey &target, const ScanRangeKey &src) {
	assert(target.partitionId_ == src.partitionId_);
	assert(target.containerId_ == src.containerId_);
	assert(target.column_ == src.column_);

	for (size_t i = 0; i < 2; i++) {
		TupleValue &targetValue = (i == 0 ? target.lower_ : target.upper_);
		const TupleValue &srcValue = (i == 0 ? src.lower_ : src.upper_);

		bool &targetInclusive =
				(i == 0 ? target.lowerInclusive_ : target.upperInclusive_);
		const bool srcInclusive =
				(i == 0 ? src.lowerInclusive_ : src.upperInclusive_);

		int32_t cmp;
		if (ColumnTypeUtils::isNull(targetValue.getType())) {
			cmp = -1;
		}
		else if (ColumnTypeUtils::isNull(srcValue.getType())) {
			cmp = 1;
		}
		else {
			cmp = SQLProcessor::ValueUtils::orderValue(
					targetValue, srcValue, true) * (i == 0 ? 1 : -1);
		}

		if (cmp < 0) {
			targetValue = srcValue;
			targetInclusive = srcInclusive;
		}
		else if (cmp == 0 && !srcInclusive) {
			targetInclusive = false;
		}
	}
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::finalizeFixedCost(
		bool indexed, bool reducible, const Cost &baseCost) const {
	const Cost cost = baseCost.withAttributes(&indexed, &reducible);
	return finalizeCost(cost, NULL, NULL);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::finalizeBasicCost(
		bool indexed, bool reducible, const Cost &drivingCost,
		const Cost &scanCost) const {
	Cost cost = drivingCost.withAttributes(&indexed, &reducible);
	cost.mergeSum(scanCost);
	return finalizeCost(cost, &drivingCost, &scanCost);
}

SQLCompiler::SelectJoinDriving::Cost
SQLCompiler::SelectJoinDriving::CostResolver::finalizeCost(
		const Cost &cost, const Cost *drivingCost,
		const Cost *scanCost) const {
	profileCost(cost, drivingCost, scanCost);

	if (!cost.isAssigned()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return cost;
}

void SQLCompiler::SelectJoinDriving::CostResolver::profileCost(
		const Cost &cost, const Cost *tableScanCost,
		const Cost *indexScanCost) const {
	if (inProfile_ == NULL || !cost.isAssigned()) {
		return;
	}

	inProfile_->cost_ = makeCostProfile(&cost);
	inProfile_->tableScanCost_ = makeCostProfile(tableScanCost);
	inProfile_->indexScanCost_ = makeCostProfile(indexScanCost);

	inProfile_->criterion_ = cost.getCriterion();
	inProfile_->indexed_ = cost.isIndexed();
	inProfile_->reducible_ = cost.isReducible();
}

void SQLCompiler::SelectJoinDriving::CostResolver::prepareProfiler(
		const Plan &plan, const PlanNode &node, uint32_t drivingInput) {
	if (profile_ == NULL) {
		inProfile_ = NULL;
		return;
	}

	OptProfile::JoinDrivingInput *dest =
			ALLOC_NEW(alloc_) OptProfile::JoinDrivingInput();
	dest->qName_ =
			makeQualifiedName(getInnerTableName(plan, node, drivingInput));

	(drivingInput == 0 ? profile_->driving_ : profile_->inner_) = dest;
	inProfile_ = dest;
}

const char8_t* SQLCompiler::SelectJoinDriving::CostResolver::getInnerTableName(
		const Plan &plan, const PlanNode &node, uint32_t drivingInput) const {
	assert(node.type_ == SQLType::EXEC_JOIN);

	const uint32_t innerInput = getJoinAnotherSide(drivingInput);
	const PlanNode &inNode = node.getInput(plan, innerInput);

	const TableInfo *tableInfo = findTableInfo(inNode);
	if (tableInfo == NULL) {
		return NULL;
	}

	return tableInfo->tableName_.c_str();
}

const SQLTableInfo* SQLCompiler::SelectJoinDriving::CostResolver::findTableInfo(
		const PlanNode &node) const {
	if (node.type_ != SQLType::EXEC_SCAN ||
			node.tableIdInfo_.type_ != SQLType::TABLE_CONTAINER) {
		return NULL;
	}

	return &compiler_.getTableInfoList().resolve(node.tableIdInfo_.id_);
}

SQLPreparedPlan::OptProfile::JoinCost*
SQLCompiler::SelectJoinDriving::CostResolver::makeCostProfile(
		const Cost *src) const {
	if (src == NULL) {
		return NULL;
	}

	OptProfile::JoinCost *dest =
			ALLOC_NEW(alloc_) OptProfile::JoinCost(src->toProfile());
	return dest;
}

SyntaxTree::QualifiedName*
SQLCompiler::SelectJoinDriving::CostResolver::makeQualifiedName(
		const char8_t *src) const {
	if (src == NULL || strlen(src) == 0) {
		return NULL;
	}

	QualifiedName *dest = ALLOC_NEW(alloc_) QualifiedName(alloc_);
	dest->table_ = ALLOC_NEW(alloc_) util::String(src, alloc_);
	return dest;
}



SQLCompiler::SelectJoinDriving::Rewriter::Rewriter(
		SQLCompiler &compiler, TableSizeList &tableSizeList,
		bool checkOnly) :
		alloc_(compiler.alloc_),
		compiler_(compiler),
		tableSizeList_(tableSizeList),
		checkOnly_(checkOnly) {
}

void SQLCompiler::SelectJoinDriving::Rewriter::rewrite(
		Plan &plan, PlanNode &node, uint32_t drivingInput,
		OptProfile::JoinDriving *profile) {
	assert(CostResolver::isAcceptableNode(node, false));

	if (checkOnly_) {
		return;
	}

	const uint32_t smaller = drivingInput;
	const uint32_t larger = getJoinAnotherSide(smaller);

	util::Map<uint32_t, uint32_t> inputIdMap(alloc_);
	inputIdMap[smaller] = 1;

	util::Vector<uint32_t> nodeRefList(alloc_);
	compiler_.makeNodeRefList(plan, nodeRefList);

	const PlanNodeId largerId = node.inputList_[larger];
	PlanNode &largerNode = node.getInput(plan, larger);

	PlanExprList predList(alloc_);
	compiler_.dereferenceExprList(
			plan, node, larger, node.predList_, predList,
			node.aggPhase_, &inputIdMap);
	if (predList.size() >= 2) {
		if (!isTrueExpr(predList[1])) {
			Expr *predExpr;
			compiler_.mergeAndOp(
					ALLOC_NEW(alloc_) Expr(predList[0]),
					ALLOC_NEW(alloc_) Expr(predList[1]),
					plan, MODE_WHERE, predExpr);
			predList[0] = (predExpr == NULL ? compiler_.genConstExpr(
					plan, makeBoolValue(true), MODE_WHERE) : *predExpr);
		}
		while (predList.size() > 1) {
			predList.pop_back();
		}
	}

	if (!largerNode.predList_.empty() &&
			!isTrueExpr(largerNode.predList_[0])) {
		Expr *predExpr;
		compiler_.mergeAndOp(
				ALLOC_NEW(alloc_) Expr(predList[0]),
				ALLOC_NEW(alloc_) Expr(largerNode.predList_[0]),
				plan, MODE_WHERE, predExpr);
		if (predExpr != NULL) {
			predList[0] = *predExpr;
		}
	}

	PlanExprList outList(alloc_);
	compiler_.dereferenceExprList(
			plan, node, larger, node.outputList_, outList,
			node.aggPhase_, &inputIdMap);

	node.type_ = SQLType::EXEC_SCAN;
	node.inputList_.erase(node.inputList_.begin() + larger);
	node.predList_.swap(predList);
	node.outputList_.swap(outList);
	node.tableIdInfo_ = largerNode.tableIdInfo_;
	node.indexInfoList_ = largerNode.indexInfoList_;
	node.cmdOptionFlag_ |= largerNode.cmdOptionFlag_;

	compiler_.refreshColumnTypes(plan, node, node.predList_, false, false);
	compiler_.refreshColumnTypes(plan, node, node.outputList_, true, false);

	if (nodeRefList[largerId] <= 1) {
		compiler_.setDisabledNodeType(largerNode);
	}

	compiler_.addOptimizationProfile(&plan, &node, profile);
}

void SQLCompiler::SelectJoinDriving::Rewriter::rewriteNonDriving(
		Plan &plan, PlanNode &node, OptProfile::JoinDriving *profile) {
	if (checkOnly_) {
		return;
	}

	compiler_.addOptimizationProfile(&plan, &node, profile);
}

void SQLCompiler::SelectJoinDriving::Rewriter::rewriteTotal(Plan &plan) {
	if (checkOnly_) {
		return;
	}

	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		if (node.id_ >= tableSizeList_.size()) {
			break;
		}

		int64_t &dest = node.tableIdInfo_.approxSize_;
		const int64_t src = tableSizeList_[node.id_];

		if (dest < 0 && src >= 0) {
			dest = src;
		}
	}
}


SQLCompiler::SelectJoinDriving::SubOptimizer::SubOptimizer(
		SQLCompiler &compiler, bool checkOnly) :
		compiler_(compiler),
		tableSizeList_(compiler_.alloc_),
		checkOnly_(checkOnly),
		profiling_(isProfiling(compiler)) {
}

bool SQLCompiler::SelectJoinDriving::SubOptimizer::optimize(Plan &plan) {
	NodeCursor cursor = makeNodeCursor(plan);
	Rewriter rewriter = makeRewriter();

	for (PlanNode *node; (node = cursor.next()) != NULL;) {
		if (!CostResolver::isAcceptableNode(*node, checkOnly_)) {
			continue;
		}

		OptProfile::JoinDriving *profile;
		const int32_t drivingInput = resolveDrivingInput(plan, *node, profile);
		applyJoinOptions(*node, drivingInput);

		if (drivingInput >= 0) {
			rewriter.rewrite(
					plan, *node, static_cast<uint32_t>(drivingInput),
					profile);
			cursor.markCurrent();
		}
		else {
			rewriter.rewriteNonDriving(plan, *node, profile);
		}
	}

	rewriter.rewriteTotal(plan);
	return cursor.isSomeMarked();
}

int32_t SQLCompiler::SelectJoinDriving::SubOptimizer::resolveDrivingInput(
		const Plan &plan, const PlanNode &node,
		OptProfile::JoinDriving *&profile) {
	profile = NULL;

	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	CostResolver resolver = makeCostResolver();
	const Cost leftCost = resolver.resolveCost(plan, node, left);
	const Cost rightCost = resolver.resolveCost(plan, node, right);

	const int32_t comp = leftCost.compareTo(rightCost);
	int32_t drivingInput = SubConstants::JOIN_UNKNOWN_INPUT;
	if (comp < 0 && leftCost.isIndexed() && leftCost.isReducible()) {
		drivingInput = left;
	}
	else if (comp > 0 && rightCost.isIndexed() && rightCost.isReducible()) {
		drivingInput = right;
	}

	profile = resolver.toProfile(drivingInput);
	return drivingInput;
}

void SQLCompiler::SelectJoinDriving::SubOptimizer::applyJoinOptions(
		PlanNode &node, int32_t drivingInput) {
	if (!checkOnly_) {
		return;
	}

	const PlanNode::CommandOptionFlag drivingSome =
			PlanNode::Config::CMD_OPT_JOIN_DRIVING_SOME;
	const PlanNode::CommandOptionFlag noneOrLeft =
			PlanNode::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT;

	PlanNode::CommandOptionFlag drivingFlags;
	if (drivingInput >= 0) {
		if (drivingInput == JOIN_LEFT_INPUT) {
			drivingFlags = (drivingSome | noneOrLeft);
		}
		else {
			drivingFlags = drivingSome;
		}
	}
	else {
		drivingFlags = noneOrLeft;
	}

	node.cmdOptionFlag_ |= drivingFlags;
}

SQLCompiler::SelectJoinDriving::NodeCursor
SQLCompiler::SelectJoinDriving::SubOptimizer::makeNodeCursor(Plan &plan) {
	const bool modifiable = !checkOnly_;
	return NodeCursor(plan, modifiable);
}

SQLCompiler::SelectJoinDriving::CostResolver
SQLCompiler::SelectJoinDriving::SubOptimizer::makeCostResolver() {
	const bool withUnionScan = checkOnly_;
	OptProfile::JoinDriving *profile = NULL;
	if (profiling_) {
		profile = ALLOC_NEW(compiler_.alloc_) OptProfile::JoinDriving();
	}
	return CostResolver(compiler_, tableSizeList_, withUnionScan, profile);
}

SQLCompiler::SelectJoinDriving::Rewriter
SQLCompiler::SelectJoinDriving::SubOptimizer::makeRewriter() {
	return Rewriter(compiler_, tableSizeList_, checkOnly_);
}

bool SQLCompiler::SelectJoinDriving::SubOptimizer::isProfiling(
		SQLCompiler &compiler) {
	return (compiler.explainType_ != SyntaxTree::EXPLAIN_NONE);
}
